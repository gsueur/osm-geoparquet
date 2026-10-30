#!/usr/bin/env python3
"""
Publish a pipeline out/ directory to an S3/R2 remote, replacing latest/.

A build collects in _staging/<YYYY-MM-DD>/ (UTC by default) until it is complete.
Once latest/ has been validated, prune.py keeps the first build of each month as
an immutable <YYYY-MM-DD>/ snapshot and clears staging.

- Writes an ATTRIBUTION.txt at the prefix root (OSM/ODbL notice)
- Writes a snapshots.json at the prefix root with the date, object count and
  total bytes of latest/ (first entry) and of each monthly snapshot.

Stages (--stage):
  all       upload + finalize (default; what a single-machine nightly does)
  upload    copy out/ -> <remote>/_staging/<date>/ only. Additive, so many
            machines (GitHub Actions matrix jobs) can each upload their own
            regions into the same folder.
  finalize  verify the staging folder is complete (every expected region has
            a manifest, every manifest lists every theme with no failures,
            every claimed parquet exists), then sync latest/, write
            ATTRIBUTION.txt and snapshots.json and publish the Portolan
            catalog (scripts/catalog.py) under catalog/. Nothing goes live
            until the completeness check passes. Refuses a date older than
            the one already published.
  catalog   completeness check of latest/, then republish only the catalog
            (e.g. after a catalog.py change). --date defaults to the published
            one.

Usage:
  python3 scripts/publish.py
  python3 scripts/publish.py --remote parquetry:parquetry/osm --out-dir out/
  python3 scripts/publish.py --dry-run                # preview only
  python3 scripts/publish.py --stage upload --out-dir out/ --date 2026-09-14
  python3 scripts/publish.py --stage finalize --date 2026-09-14
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import catalog  # noqa: E402
from themes import THEMES  # noqa: E402

SNAPSHOT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
EXPECTED_THEMES = {t.name for t in THEMES}

# Present in the admin-region GeoJSON but out of coverage (matches validate.py).
DEFAULT_EXCLUDE = ["US-AS", "US-GU", "US-MP", "US-UM"]

ATTRIBUTION = """\
# Data attribution

This data is derived from OpenStreetMap (OSM), available under the
Open Database License (ODbL) 1.0.

  Source:       https://www.openstreetmap.org/
  License:      https://opendatacommons.org/licenses/odbl/1-0/
  Attribution:  (c) OpenStreetMap contributors

Downstream users of these files MUST:
  1. Credit OpenStreetMap contributors in any derived product.
  2. If redistributing or publishing a derivative dataset, do so under ODbL.

# Pipeline source

The continental PBF extracts that feed this pipeline come from Geofabrik:
  https://download.geofabrik.de/
Geofabrik does not require attribution, but we're glad to credit them.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def sh(cmd: list[str], *, dry_run: bool = False) -> None:
    print(f"  $ {' '.join(cmd)}")
    if dry_run:
        return
    subprocess.run(cmd, check=True)


def sh_json(cmd: list[str]) -> list | dict:
    r = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return json.loads(r.stdout)


def list_snapshot_dirs(remote: str) -> set[str]:
    return {e["Name"] for e in sh_json(["rclone", "lsjson", remote, "--dirs-only"])
            if SNAPSHOT_RE.match(e["Name"])}


# ---------- rclone progress-bar integration ----------

_SIZE_RE = re.compile(r"([\d.]+)\s*([KMGTP]?i?B)\b")
_UNITS = {
    "B": 1,
    "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4, "PiB": 1024**5,
    "kB": 1000, "KB": 1000, "MB": 10**6, "GB": 10**9, "TB": 10**12, "PB": 10**15,
}
_STATS_RE = re.compile(
    r"([\d.]+\s*[KMGTP]?i?B)\s*/\s*([\d.]+\s*[KMGTP]?i?B)"
)


def _parse_size(s: str) -> int:
    m = _SIZE_RE.search(s)
    if not m:
        return 0
    return int(float(m.group(1)) * _UNITS.get(m.group(2), 1))


def _count_bytes(root: Path, exclude_dirs: tuple[str, ...] = ("_work",)) -> int:
    total = 0
    for p in root.rglob("*"):
        if p.is_file() and not any(part in exclude_dirs for part in p.parts):
            total += p.stat().st_size
    return total


def rclone_with_progress(cmd: list[str], *, label: str, total_bytes: int,
                         dry_run: bool = False) -> None:
    """Run an rclone copy/sync with a Rich live progress bar.

    Parses rclone's one-line stats output (emitted every second at NOTICE
    level) to drive a Rich bar with bytes-transferred / total / speed / ETA.
    """
    full = cmd + ["--stats", "1s", "--stats-one-line", "--stats-log-level", "NOTICE"]
    print(f"  $ {' '.join(full)}")
    if dry_run:
        return

    from rich.console import Console
    from rich.progress import (
        Progress, BarColumn, TextColumn,
        DownloadColumn, TransferSpeedColumn, TimeRemainingColumn,
    )

    console = Console()
    with Progress(
        TextColumn("[cyan]{task.description}[/cyan]"),
        BarColumn(bar_width=30),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(label, total=total_bytes or 1)
        proc = subprocess.Popen(
            full, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
        )
        assert proc.stderr is not None
        try:
            for line in proc.stderr:
                m = _STATS_RE.search(line)
                if m:
                    done = _parse_size(m.group(1))
                    total = _parse_size(m.group(2))
                    # rclone's "total" grows as it discovers files; lift
                    # the bar's total when it exceeds our initial guess.
                    if total and total > progress.tasks[task].total:
                        progress.update(task, total=total)
                    progress.update(task, completed=done)
        finally:
            proc.wait()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, full)
        progress.update(task, completed=progress.tasks[task].total)


def write_remote_text(remote: str, name: str, text: str, *,
                      cache_control: str, dry_run: bool) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".tmp", delete=False) as f:
        f.write(text)
        local = f.name
    try:
        # --ignore-times forces the PUT even when the content is unchanged
        # (rclone would otherwise ETag-match and skip, which leaves the
        # existing object's Cache-Control unchanged — exactly what bit
        # ATTRIBUTION.txt on 2026-04-23).
        sh([
            "rclone", "copyto", "--ignore-times",
            "--header-upload", f"Cache-Control: {cache_control}",
            local, f"{remote}/{name}",
        ], dry_run=dry_run)
    finally:
        Path(local).unlink(missing_ok=True)


def ensure_attribution(remote: str, *, dry_run: bool) -> None:
    # Always (re)write so edits to ATTRIBUTION text propagate. It's tiny
    # and rclone skips the upload via ETag match when the content is
    # unchanged anyway.
    print("  [attribution] writing ATTRIBUTION.txt")
    write_remote_text(remote, "ATTRIBUTION.txt", ATTRIBUTION,
                      cache_control="public, max-age=300", dry_run=dry_run)


def load_expected_isos(geojson: Path, exclude: set[str]) -> set[str]:
    data = json.loads(geojson.read_text())
    out: set[str] = set()
    for f in data.get("features", []):
        iso = (f.get("properties") or {}).get("ISO3166-2")
        if iso and iso not in exclude:
            out.add(iso)
    if not out:
        sys.exit(f"no ISO3166-2 features in {geojson}")
    return out


STAGING = "_staging"
# Nobody reads staging, and a re-run can rewrite it. finalize sets the header
# latest/ serves.
STAGING_CACHE = "Cache-Control: no-store"
IMMUTABLE_CACHE = "public, max-age=31536000, immutable"
FRAGMENT_RE = re.compile(r"^_manifest\.([A-Za-z0-9_-]+)\.json$")


def fetch_manifests(prefix: str, dest: Path) -> None:
    """Mirror every _manifest.json, and every _manifest.<name>.json fragment,
    of a remote prefix into dest (same layout)."""
    subprocess.run(
        ["rclone", "copy", "--include", "_manifest*.json", prefix, str(dest)],
        check=True, capture_output=True,
    )


def upload_manifest_fragments(out_dir: Path, dest: str, name: str, *, dry_run: bool) -> None:
    """Each region's _manifest.json goes up as _manifest.<name>.json, beside
    the manifest the region's main job writes. The two never collide, and
    finalize folds the fragment in (merge_manifest_fragments)."""
    paths = sorted(out_dir.glob("country=*/state=*/_manifest.json"))
    for p in paths:
        rel = p.parent.relative_to(out_dir).as_posix()
        cmd = ["rclone", "copyto", "--header-upload", STAGING_CACHE,
               str(p), f"{dest}{rel}/_manifest.{name}.json"]
        if dry_run:
            print(f"  $ {' '.join(cmd)}")
        else:
            subprocess.run(cmd, check=True)
    print(f"  {len(paths)} manifests -> _manifest.{name}.json")


def merge_manifest(base: dict, fragments: dict[str, dict]) -> dict:
    """The region manifest with each fragment's themes folded in.

    A fragment is the manifest of a job that built a theme subset from another
    source (the boundaries theme from the parent extract, for a region whose
    own extract cuts its relations). Its counts and stats replace the base's
    for those themes; everything else in the base stands, including the base
    source and timestamp, which describe the other 15 themes. What came from
    where is kept under `fragments`. Pure, so the fixture test can run it."""
    merged = json.loads(json.dumps(base))
    merged.setdefault("themes", {})
    merged.setdefault("theme_stats", {})
    merged.setdefault("fragments", {})
    for name, frag in sorted(fragments.items()):
        merged["themes"].update(frag.get("themes") or {})
        merged["theme_stats"].update(frag.get("theme_stats") or {})
        for theme, count in (frag.get("themes") or {}).items():
            if count <= 0:
                merged["theme_stats"].pop(theme, None)
        merged["fragments"][name] = {
            "themes": sorted(frag.get("themes") or {}),
            "source_pbf": frag.get("source_pbf"),
            "source_timestamp": frag.get("source_timestamp"),
            "duration_s": frag.get("duration_s"),
        }
    merged["total_features"] = sum(c for c in merged["themes"].values() if c > 0)
    return merged


def merge_manifest_fragments(prefix: str, manifests: Path) -> int:
    """Fold every _manifest.<name>.json in the mirror into its region's
    _manifest.json, in the mirror and on the remote, so the gate, the catalog
    and validation read one complete manifest per region. Idempotent: a
    fragment already folded in changes nothing and nothing is re-uploaded.
    A region with fragments but no base manifest is left for the gate to
    report. Returns the number of manifests rewritten."""
    regions = sorted({p.parent for p in manifests.glob("country=*/state=*/_manifest.*.json")})
    rewritten = 0
    for region in regions:
        base_path = region / "_manifest.json"
        fragments = {m.group(1): json.loads(p.read_text())
                     for p in region.glob("_manifest.*.json")
                     if (m := FRAGMENT_RE.match(p.name))}
        if not base_path.is_file():
            print(f"  {region.name}: fragments {sorted(fragments)} but no _manifest.json")
            continue
        base = json.loads(base_path.read_text())
        merged = merge_manifest(base, fragments)
        if merged == base:
            continue
        base_path.write_text(json.dumps(merged, indent=2))
        rel = region.relative_to(manifests).as_posix()
        subprocess.run(["rclone", "copyto", "--header-upload", STAGING_CACHE,
                        str(base_path), f"{prefix}{rel}/_manifest.json"], check=True)
        rewritten += 1
    if regions:
        print(f"\n[gate] manifest fragments: {len(regions)} regions, "
              f"{rewritten} manifests rewritten")
    return rewritten


def check_build_complete(prefix: str, expected: set[str], manifests: Path) -> list[str]:
    """Return a list of problems (empty = the prefix is complete).

    Runs before anything goes live so a matrix run with one failed region
    never becomes `latest/`. `manifests` is the fetch_manifests() mirror of
    the prefix.
    """
    listing = sh_json(["rclone", "lsjson", "-R", "--files-only", prefix])
    sizes = {e["Path"]: e.get("Size", 0) for e in listing}
    if not sizes:
        return [f"{prefix} is empty or missing"]

    problems: list[str] = []
    for iso in sorted(expected):
        rel = f"country={iso.split('-')[0]}/state={iso}"
        mpath = manifests / rel / "_manifest.json"
        if not mpath.is_file():
            problems.append(f"{iso}: no _manifest.json")
            continue
        themes = (json.loads(mpath.read_text()).get("themes") or {})
        missing = EXPECTED_THEMES - set(themes)
        if missing:
            problems.append(f"{iso}: manifest missing themes {sorted(missing)}")
        for theme, count in themes.items():
            if count < 0:
                problems.append(f"{iso}/{theme}: pipeline reported failure")
            elif count > 0 and sizes.get(f"{rel}/{theme}.parquet", 0) <= 0:
                problems.append(f"{iso}/{theme}: parquet missing or empty on remote")
    return problems


def published_date(remote: str) -> str | None:
    """The date of the build in latest/, from snapshots.json. None before the
    first publish."""
    r = subprocess.run(["rclone", "cat", f"{remote}/snapshots.json"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        return None
    snapshots = json.loads(r.stdout).get("snapshots") or []
    return max((s["date"] for s in snapshots), default=None)


def build_snapshot_manifest(remote: str, date: str) -> dict:
    """latest/ first, then the monthly snapshots, newest first."""
    entries = [(date, "latest/")] + [(d, f"{d}/") for d in
                                     sorted(list_snapshot_dirs(remote), reverse=True)]
    snapshots = []
    for d, path in entries:
        size = sh_json(["rclone", "size", "--json", f"{remote}/{path}"])
        snapshots.append({"date": d, "path": path,
                          "objects": size["count"], "bytes": size["bytes"]})
    return {
        "manifest_version": "1",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "snapshots": snapshots,
    }


def write_snapshot_manifest(remote: str, date: str) -> dict:
    manifest = build_snapshot_manifest(remote, date)
    write_remote_text(remote, "snapshots.json", json.dumps(manifest, indent=2),
                      cache_control="public, max-age=300", dry_run=False)
    return manifest


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", type=Path, default=Path("out"))
    p.add_argument("--remote", default="parquetry:parquetry/osm",
                   help="rclone <remote>:<bucket>[/<prefix>]. The bucket holds "
                        "several datasets, each self-contained under its own "
                        "prefix, so everything below is relative to this one. "
                        "Default: parquetry:parquetry/osm")
    p.add_argument("--date", default=None,
                   help="Build date YYYY-MM-DD. Default: today (UTC); with "
                        "--stage catalog, the published date.")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stage", choices=["all", "upload", "finalize", "catalog"], default="all",
                   help="upload: copy out/ into the staging folder only. "
                        "finalize: verify completeness, then sync latest/, "
                        "attribution, snapshots.json and the Portolan catalog. "
                        "catalog: verify latest/ is complete, then republish only the "
                        "catalog. Default: upload + finalize.")
    p.add_argument("--states-geojson", type=Path, default=Path("data/admin_regions.geojson"),
                   help="Admin regions; defines what a complete build is (finalize).")
    p.add_argument("--exclude", nargs="*", default=DEFAULT_EXCLUDE,
                   help="ISO codes in the geojson that are out of coverage.")
    p.add_argument("--skip-completeness", action="store_true",
                   help="Finalize without the completeness gate (partial backfills).")
    p.add_argument("--manifest-fragment", default=None, metavar="NAME",
                   help="Upload stage: send each region's _manifest.json as "
                        "_manifest.<NAME>.json instead, for a job that built a theme "
                        "subset of regions another job also builds (the parent-extract "
                        "boundaries job). Finalize folds fragments into the region "
                        "manifest before the completeness gate.")
    args = p.parse_args()

    do_upload = args.stage in ("all", "upload")
    do_finalize = args.stage in ("all", "finalize")
    do_catalog = args.stage in ("all", "finalize", "catalog")

    if do_upload:
        if not args.out_dir.is_dir():
            sys.exit(f"out dir not found: {args.out_dir}")
        if not any(args.out_dir.rglob("*.parquet")):
            sys.exit(f"no .parquet files under {args.out_dir}; nothing to publish")

    published = published_date(args.remote)
    date = args.date or (published if args.stage == "catalog" else None) \
        or dt.datetime.now(dt.timezone.utc).date().isoformat()
    if not SNAPSHOT_RE.match(date):
        sys.exit(f"--date must be YYYY-MM-DD, got {date!r}")

    # A re-run of an old failed build must not put older data over latest/.
    if do_finalize and published and date < published:
        sys.exit(f"{date} is older than the published build {published}; "
                 f"not touching latest/")

    dest = (f"{args.remote}/latest/" if args.stage == "catalog"
            else f"{args.remote}/{STAGING}/{date}/")
    print(f"Stage:     {args.stage}")
    if do_upload:
        print(f"Local:     {args.out_dir}/")
    print(f"Remote:    {dest}")
    print(f"Dry-run:   {args.dry_run}")
    print()

    if do_upload:
        total_bytes = _count_bytes(args.out_dir)
        print(f"Total:     {total_bytes/1e9:.2f} GB across "
              f"{sum(1 for _ in args.out_dir.rglob('*.parquet'))} parquet files")
        print()

        print(f"[1/5] upload -> {dest}")
        excludes = ["--exclude", "_work/**"]
        if args.manifest_fragment:
            excludes += ["--exclude", "_manifest.json"]
        rclone_with_progress(
            [
                "rclone", "copy", *excludes,
                "--header-upload", STAGING_CACHE,
                f"{args.out_dir}/", dest,
            ],
            label="upload",
            total_bytes=total_bytes,
            dry_run=args.dry_run,
        )
        if args.manifest_fragment:
            upload_manifest_fragments(args.out_dir, dest, args.manifest_fragment,
                                      dry_run=args.dry_run)
        if not do_finalize:
            print("\nDone (upload only).")
            return
    else:
        print(f"[1/5] upload: skipped ({args.stage} only)")
        if args.dry_run or not do_finalize:
            total_bytes = 0
        else:
            total_bytes = sh_json(["rclone", "size", "--json", dest])["bytes"]

    with tempfile.TemporaryDirectory(prefix="manifests-") as tmp:
        manifests = Path(tmp)
        if not args.dry_run:
            fetch_manifests(dest, manifests)
            merge_manifest_fragments(dest, manifests)
        gate(args, date, dest, manifests)
        if do_finalize:
            finalize(args, date, dest, total_bytes)
        if do_catalog:
            publish_catalog(args, date, manifests)
    print("\nDone.")


def gate(args, date: str, dest: str, manifests: Path) -> None:
    if args.skip_completeness:
        print("\n[gate] completeness check skipped (--skip-completeness)")
        return
    if args.dry_run:
        print("\n[gate] completeness check skipped in --dry-run")
        return
    if not args.states_geojson.is_file():
        sys.exit(f"--states-geojson not found: {args.states_geojson} "
                 f"(needed for the completeness gate; or --skip-completeness)")
    expected = load_expected_isos(args.states_geojson, set(args.exclude))
    print(f"\n[gate] checking {dest} covers {len(expected)} regions "
          f"x {len(EXPECTED_THEMES)} themes")
    problems = check_build_complete(dest, expected, manifests)
    if problems:
        for pr in problems[:30]:
            print(f"  - {pr}")
        if len(problems) > 30:
            print(f"  ... {len(problems) - 30} more")
        sys.exit(f"build {date} is incomplete ({len(problems)} problems); "
                 f"not touching latest/, snapshots.json or the catalog")
    print("  complete")


def publish_catalog(args, date: str, manifests: Path) -> None:
    print(f"\n[5/5] Portolan catalog -> {args.remote}/{catalog.CATALOG_PREFIX}/")
    if args.dry_run:
        print("  (skipped in --dry-run)")
        return
    if args.skip_completeness:
        # Never describe a build nobody checked: the catalog advertises
        # the whole glob as one complete dataset.
        print("  skipped: --skip-completeness")
        return
    catalog.publish(manifests, date, args.remote, dry_run=False)


def finalize(args, date: str, dest: str, total_bytes: int) -> None:
    latest = f"{args.remote}/latest/"
    print(f"\n[2/5] sync latest/ -> {latest}  (server-side copy from {dest})")
    # Server-side S3 CopyObject from staging avoids re-uploading ~35 GB every
    # night. The source's Cache-Control is `no-store`, which would defeat the
    # edge cache if it leaked through, so we override it via rclone's metadata system: --metadata + --metadata-set
    # makes rclone emit x-amz-metadata-directive=REPLACE on CopyObject.
    #
    # REQUIRES rclone >= v1.65. The Debian-packaged v1.60.1 silently drops
    # the REPLACE directive on server-side copy and Cache-Control leaks
    # through. nightly.sh prepends ~/.local/bin to PATH so the locally
    # installed newer rclone takes precedence over /usr/bin/rclone.
    # validate.py asserts the resulting Cache-Control as a regression net.
    #
    # --ignore-times: a file rebuilt byte for byte (Geofabrik had not refreshed
    # its extract) would otherwise only get its modtime updated, which rewrites
    # the object without Cache-Control and skips the override. Bit us on
    # 2026-10-30. It also restores the header on any file that lost it.
    rclone_with_progress(
        [
            "rclone", "sync",
            "--ignore-times",
            "--metadata",
            "--metadata-set",
            "cache-control=public, max-age=300, s-maxage=86400, stale-while-revalidate=86400",
            dest, latest,
        ],
        label="latest/",
        total_bytes=total_bytes,
        dry_run=args.dry_run,
    )

    print(f"\n[3/5] attribution")
    ensure_attribution(args.remote, dry_run=args.dry_run)

    print(f"\n[4/5] snapshots.json")
    if args.dry_run:
        print("  (skipped in --dry-run)")
    else:
        manifest = write_snapshot_manifest(args.remote, date)
        for x in manifest["snapshots"]:
            print(f"  {x['date']}  {x['path']:<12} {x['objects']:>4} files  "
                  f"{x['bytes']/1_000_000:>7.1f} MB")


if __name__ == "__main__":
    main()
