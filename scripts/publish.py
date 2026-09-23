#!/usr/bin/env python3
"""
Publish a pipeline out/ directory to an S3/R2 remote as a dated snapshot.

- Uploads out/ -> <remote>/<YYYY-MM-DD>/ (UTC by default)
- Writes an ATTRIBUTION.txt at bucket root if missing (OSM/ODbL notice)
- Rebuilds a snapshots.json index at bucket root listing every dated snapshot
  with object count + total bytes

Stages (--stage):
  all       upload + finalize (default; what a single-machine nightly does)
  upload    copy out/ -> <remote>/<date>/ only. Additive, so many machines
            (GitHub Actions matrix jobs) can each upload their own regions
            into the same dated prefix.
  finalize  verify the dated prefix is complete (every expected region has
            a manifest, every manifest lists every theme with no failures,
            every claimed parquet exists), then sync latest/, write
            ATTRIBUTION.txt, rebuild snapshots.json and publish the Portolan
            catalog (scripts/catalog.py) under catalog/. Nothing goes live
            until the completeness check passes.
  catalog   completeness check, then republish only the catalog (e.g. after
            a catalog.py change, without re-syncing latest/).

Usage:
  python3 scripts/publish.py
  python3 scripts/publish.py --remote parquetry:parquetry/osm --out-dir out/
  python3 scripts/publish.py --date 2026-04-18        # backfill
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


def list_snapshot_dirs(remote_path: str) -> set[str]:
    """Date-named directories under a remote path. Empty when the path does
    not exist yet, which is the state before the first catalog is published."""
    r = subprocess.run(["rclone", "lsjson", remote_path, "--dirs-only"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        return set()
    return {e["Name"] for e in json.loads(r.stdout) if SNAPSHOT_RE.match(e["Name"])}


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


def count_remote_manifests(remote: str, date: str) -> int:
    """Cheap completeness probe: number of _manifest.json under <date>/."""
    entries = sh_json([
        "rclone", "lsjson", "-R", "--files-only",
        "--include", "_manifest.json", f"{remote}/{date}/",
    ])
    return len(entries)


def fetch_manifests(remote: str, date: str, dest: Path) -> None:
    """Mirror every _manifest.json of a dated prefix into dest (same layout)."""
    subprocess.run(
        ["rclone", "copy", "--include", "_manifest.json", f"{remote}/{date}/", str(dest)],
        check=True, capture_output=True,
    )


def check_snapshot_complete(remote: str, date: str, expected: set[str],
                            manifests: Path) -> list[str]:
    """Return a list of problems (empty = the dated prefix is complete).

    Runs before anything goes live so a matrix run with one failed region
    never becomes `latest/` or shows up in snapshots.json. `manifests` is the
    fetch_manifests() mirror of the prefix.
    """
    prefix = f"{remote}/{date}/"
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


def newest_snapshot(remote: str) -> str | None:
    dates = [e["Name"] for e in sh_json(["rclone", "lsjson", remote, "--dirs-only"])
             if SNAPSHOT_RE.match(e["Name"])]
    return max(dates, default=None)


def is_pinned_date(date: str) -> bool:
    """True for the snapshots prune.py keeps past the 14-day daily window: the
    first of each month, and Dec 31 as the yearly anchor. Those are the ones
    worth a catalog of their own, since they are still there in a year.
    Mirrors prune.classify(); keep the two in step."""
    d = dt.date.fromisoformat(date)
    return d.day == 1 or (d.month == 12 and d.day == 31)


def pinned_catalogs(remote: str, date: str) -> list[str]:
    """Pinned catalogs the live one should link, oldest first: those already on
    the remote that tonight's prune will keep, plus this snapshot when it earns
    one. Filtering by the retention rule keeps the live catalog from linking a
    catalog that the prune step deletes a minute later."""
    from prune import classify  # local import: prune imports this module

    today = dt.date.today()
    dates = list_snapshot_dirs(f"{remote}/{catalog.CATALOG_PREFIX}/")
    if is_pinned_date(date):
        dates.add(date)
    return sorted(d for d in dates
                  if classify(dt.date.fromisoformat(d), today) == "keep")


def build_snapshot_manifest(remote: str) -> dict:
    entries = sh_json(["rclone", "lsjson", remote, "--dirs-only"])
    snapshots: list[dict] = []
    for e in entries:
        name = e["Name"]
        if not SNAPSHOT_RE.match(name):
            continue
        size = sh_json(["rclone", "size", "--json", f"{remote}/{name}"])
        snapshots.append({
            "date": name,
            "path": f"{name}/",
            "objects": size["count"],
            "bytes": size["bytes"],
        })
    snapshots.sort(key=lambda s: s["date"], reverse=True)
    return {
        "manifest_version": "1",
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "snapshots": snapshots,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", type=Path, default=Path("out"))
    p.add_argument("--remote", default="parquetry:parquetry/osm",
                   help="rclone <remote>:<bucket>[/<prefix>]. The bucket holds "
                        "several datasets, each self-contained under its own "
                        "prefix, so everything below is relative to this one. "
                        "Default: parquetry:parquetry/osm")
    p.add_argument("--date", default=None,
                   help="Snapshot date YYYY-MM-DD. Default: today (UTC).")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--stage", choices=["all", "upload", "finalize", "catalog"], default="all",
                   help="upload: copy out/ into the dated prefix only. "
                        "finalize: verify completeness, then sync latest/, "
                        "attribution, snapshots.json and the Portolan catalog. "
                        "catalog: verify completeness, then republish only the "
                        "catalog. Default: upload + finalize.")
    p.add_argument("--states-geojson", type=Path, default=Path("data/admin_regions.geojson"),
                   help="Admin regions; defines what a complete snapshot is (finalize).")
    p.add_argument("--exclude", nargs="*", default=DEFAULT_EXCLUDE,
                   help="ISO codes in the geojson that are out of coverage.")
    p.add_argument("--skip-completeness", action="store_true",
                   help="Finalize without the completeness gate (partial backfills).")
    args = p.parse_args()

    do_upload = args.stage in ("all", "upload")
    do_finalize = args.stage in ("all", "finalize")
    do_catalog = args.stage in ("all", "finalize", "catalog")

    if do_upload:
        if not args.out_dir.is_dir():
            sys.exit(f"out dir not found: {args.out_dir}")
        if not any(args.out_dir.rglob("*.parquet")):
            sys.exit(f"no .parquet files under {args.out_dir}; nothing to publish")

    date = args.date or dt.datetime.now(dt.timezone.utc).date().isoformat()
    if not SNAPSHOT_RE.match(date):
        sys.exit(f"--date must be YYYY-MM-DD, got {date!r}")

    dest = f"{args.remote}/{date}/"
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
        rclone_with_progress(
            [
                "rclone", "copy", "--exclude", "_work/**",
                # Dated snapshots never change — tell browsers and Cloudflare's
                # edge cache they can pin the bytes for a year.
                "--header-upload",
                "Cache-Control: public, max-age=31536000, immutable",
                f"{args.out_dir}/", dest,
            ],
            label="upload",
            total_bytes=total_bytes,
            dry_run=args.dry_run,
        )
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
            fetch_manifests(args.remote, date, manifests)
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
    problems = check_snapshot_complete(args.remote, date, expected, manifests)
    if problems:
        for pr in problems[:30]:
            print(f"  - {pr}")
        if len(problems) > 30:
            print(f"  ... {len(problems) - 30} more")
        sys.exit(f"snapshot {date} is incomplete ({len(problems)} problems); "
                 f"not touching latest/, snapshots.json or the catalog")
    print("  complete")


def publish_catalog(args, date: str, manifests: Path) -> None:
    print(f"\n[5/5] Portolan catalog -> {args.remote}/{catalog.CATALOG_PREFIX}/")
    if args.dry_run:
        print("  (skipped in --dry-run)")
        return
    if args.skip_completeness:
        # Never describe a snapshot nobody checked: the catalog advertises
        # the whole glob as one complete dataset.
        print("  skipped: --skip-completeness")
        return
    newest = newest_snapshot(args.remote)
    if newest and date < newest:
        # The live catalog describes latest/, which this run did not move.
        print(f"  skipped: {date} is older than the newest snapshot {newest}")
        return
    catalog.publish(manifests, date, args.remote,
                    archives=pinned_catalogs(args.remote, date),
                    pin=is_pinned_date(date), dry_run=False)


def finalize(args, date: str, dest: str, total_bytes: int) -> None:
    latest = f"{args.remote}/latest/"
    print(f"\n[2/5] sync latest/ -> {latest}  (server-side copy from {date}/)")
    # Server-side S3 CopyObject from the dated snapshot avoids re-uploading
    # ~35 GB every night. The source's Cache-Control is `immutable`, which
    # would pin latest/ at the edge forever if it leaked through, so we
    # override it via rclone's metadata system: --metadata + --metadata-set
    # makes rclone emit x-amz-metadata-directive=REPLACE on CopyObject.
    #
    # REQUIRES rclone >= v1.65. The Debian-packaged v1.60.1 silently drops
    # the REPLACE directive on server-side copy and Cache-Control leaks
    # through. nightly.sh prepends ~/.local/bin to PATH so the locally
    # installed newer rclone takes precedence over /usr/bin/rclone.
    # validate.py asserts the resulting Cache-Control as a regression net.
    rclone_with_progress(
        [
            "rclone", "sync",
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
        manifest = build_snapshot_manifest(args.remote)
        write_remote_text(args.remote, "snapshots.json",
                          json.dumps(manifest, indent=2),
                          cache_control="public, max-age=300", dry_run=False)
        print(f"\nSnapshots on remote:")
        for s in manifest["snapshots"]:
            print(f"  {s['date']}  {s['objects']:>4} files  "
                  f"{s['bytes']/1_000_000:>7.1f} MB")


if __name__ == "__main__":
    main()
