#!/usr/bin/env python3
"""
Prune dated snapshots on the R2 remote per the tiered retention policy.

Rules (UTC):
  * `latest/` and bucket-root files (snapshots.json, ATTRIBUTION.txt) are never touched.
  * Last 14 days: every daily snapshot is kept.
  * 15-365 days old: keep the 1st of each month; delete the rest.
  * >365 days old: keep Dec 31 of each year; delete the rest.

Dry-run by default. Pass --execute to actually delete.

Typical use (from nightly.sh, after remote validation passes):
  python3 scripts/prune.py --execute --remote parquetry:parquetry
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from publish import (  # noqa: E402
    SNAPSHOT_RE,
    build_snapshot_manifest,
    sh,
    sh_json,
    write_remote_text,
)


def classify(date: dt.date, now: dt.date) -> str:
    """Return 'keep' or 'delete' for a dated snapshot.

    Retention tiers are nested: a Dec 31 snapshot is a yearly anchor and must
    survive the monthly window (otherwise it gets pruned at age 15-365d and
    never reaches the yearly-anchor rule).
    """
    age_days = (now - date).days
    if age_days <= 14:
        return "keep"
    if date.month == 12 and date.day == 31:
        return "keep"
    if age_days <= 365 and date.day == 1:
        return "keep"
    return "delete"


def list_dated_prefixes(remote: str) -> list[str]:
    entries = sh_json(["rclone", "lsjson", remote, "--dirs-only"])
    return sorted(e["Name"] for e in entries if SNAPSHOT_RE.match(e["Name"]))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--remote", default="parquetry:parquetry",
                   help="rclone <remote>:<bucket>. Default: parquetry:parquetry")
    p.add_argument("--execute", action="store_true",
                   help="Actually delete. Default is dry-run.")
    p.add_argument("--now", default=None,
                   help="Override 'now' for testing (YYYY-MM-DD UTC).")
    args = p.parse_args()

    if args.now:
        if not SNAPSHOT_RE.match(args.now):
            sys.exit(f"--now must be YYYY-MM-DD, got {args.now!r}")
        now = dt.date.fromisoformat(args.now)
    else:
        now = dt.datetime.now(dt.timezone.utc).date()

    print(f"Remote:    {args.remote}")
    print(f"Now (UTC): {now.isoformat()}")
    print(f"Mode:      {'EXECUTE' if args.execute else 'dry-run'}")
    print()

    dates = list_dated_prefixes(args.remote)
    if not dates:
        print("No dated snapshots found.")
        return

    keeps, deletes = [], []
    for d in dates:
        verdict = classify(dt.date.fromisoformat(d), now)
        (keeps if verdict == "keep" else deletes).append(d)

    print(f"[inventory] {len(dates)} dated snapshots")
    print(f"  keep:   {len(keeps)}")
    print(f"  delete: {len(deletes)}")
    print()

    if deletes:
        print("[delete]")
        for d in deletes:
            print(f"  - {d}")
        print()

    if not args.execute:
        print("dry-run: no changes made. Pass --execute to delete.")
        return

    if not deletes:
        print("Nothing to delete. Remote already matches policy.")
        return

    for d in deletes:
        sh(["rclone", "purge", f"{args.remote}/{d}/"])

    print()
    print("[snapshots.json] rebuilding")
    manifest = build_snapshot_manifest(args.remote)
    write_remote_text(
        args.remote,
        "snapshots.json",
        json.dumps(manifest, indent=2),
        dry_run=False,
    )
    print(f"  remaining snapshots: {len(manifest['snapshots'])}")


if __name__ == "__main__":
    main()
