#!/usr/bin/env python3
"""
Publish a pipeline out/ directory to an S3/R2 remote as a dated snapshot.

- Uploads out/ -> <remote>/<YYYY-MM-DD>/ (UTC by default)
- Writes an ATTRIBUTION.txt at bucket root if missing (OSM/ODbL notice)
- Rebuilds a snapshots.json index at bucket root listing every dated snapshot
  with object count + total bytes

Usage:
  python3 scripts/publish.py
  python3 scripts/publish.py --remote parquetry:parquetry --out-dir out/
  python3 scripts/publish.py --date 2026-04-18        # backfill
  python3 scripts/publish.py --dry-run                # preview only
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

SNAPSHOT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

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


def write_remote_text(remote: str, name: str, text: str, *, dry_run: bool) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".tmp", delete=False) as f:
        f.write(text)
        local = f.name
    try:
        sh(["rclone", "copyto", local, f"{remote}/{name}"], dry_run=dry_run)
    finally:
        Path(local).unlink(missing_ok=True)


def ensure_attribution(remote: str, *, dry_run: bool) -> None:
    # Always (re)write so edits to ATTRIBUTION text propagate. It's tiny
    # and rclone skips the upload via ETag match when the content is
    # unchanged anyway.
    print("  [attribution] writing ATTRIBUTION.txt")
    write_remote_text(remote, "ATTRIBUTION.txt", ATTRIBUTION, dry_run=dry_run)


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
    p.add_argument("--remote", default="parquetry:parquetry",
                   help="rclone <remote>:<bucket>. Default: parquetry:parquetry")
    p.add_argument("--date", default=None,
                   help="Snapshot date YYYY-MM-DD. Default: today (UTC).")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if not args.out_dir.is_dir():
        sys.exit(f"out dir not found: {args.out_dir}")

    any_parquet = any(args.out_dir.rglob("*.parquet"))
    if not any_parquet:
        sys.exit(f"no .parquet files under {args.out_dir}; nothing to publish")

    date = args.date or dt.datetime.now(dt.timezone.utc).date().isoformat()
    if not SNAPSHOT_RE.match(date):
        sys.exit(f"--date must be YYYY-MM-DD, got {date!r}")

    dest = f"{args.remote}/{date}/"
    print(f"Local:     {args.out_dir}/")
    print(f"Remote:    {dest}")
    print(f"Dry-run:   {args.dry_run}")
    print()

    print(f"[1/4] upload -> {dest}")
    sh(["rclone", "copy", "--progress",
        "--exclude", "_work/**",
        f"{args.out_dir}/", dest],
       dry_run=args.dry_run)

    latest = f"{args.remote}/latest/"
    print(f"\n[2/4] sync latest/ -> {latest}")
    sh(["rclone", "sync", "--progress",
        "--exclude", "_work/**",
        f"{args.out_dir}/", latest],
       dry_run=args.dry_run)

    print(f"\n[3/4] attribution")
    ensure_attribution(args.remote, dry_run=args.dry_run)

    print(f"\n[4/4] snapshots.json")
    if args.dry_run:
        print("  (skipped in --dry-run)")
    else:
        manifest = build_snapshot_manifest(args.remote)
        write_remote_text(args.remote, "snapshots.json",
                          json.dumps(manifest, indent=2), dry_run=False)
        print(f"\nSnapshots on remote:")
        for s in manifest["snapshots"]:
            print(f"  {s['date']}  {s['objects']:>4} files  "
                  f"{s['bytes']/1_000_000:>7.1f} MB")

    print("\nDone.")


if __name__ == "__main__":
    main()
