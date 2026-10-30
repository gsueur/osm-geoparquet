#!/usr/bin/env python3
"""
Keep the month's snapshot and clear the staging folder on the R2 remote, once
latest/ is validated.

A build collects in _staging/<YYYY-MM-DD>/ until publish.py's finalize copies
it to latest/. Then, in this order:

  * The first published build of each month is also copied to <YYYY-MM-DD>/,
    immutable. Going by the first published build rather than by the 1st means
    a failed run on the 1st does not cost the month its snapshot.
  * Snapshots older than RETENTION_MONTHS calendar months are deleted.
  * snapshots.json is rewritten, and all of _staging/ is deleted, including
    what a failed build left behind.

So run it only after a build went live. Dry-run by default. Pass --execute to
actually change anything.

Typical use (from nightly.yml, after remote validation passes):
  python3 scripts/prune.py --execute --remote parquetry:parquetry/osm
"""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from publish import (  # noqa: E402
    IMMUTABLE_CACHE,
    STAGING,
    list_snapshot_dirs,
    published_date,
    rclone_with_progress,
    sh,
    write_snapshot_manifest,
)

RETENTION_MONTHS = 12


def month_index(date: str) -> int:
    d = dt.date.fromisoformat(date)
    return d.year * 12 + d.month


def expired(dates: set[str], today: dt.date) -> list[str]:
    """Snapshots older than the last RETENTION_MONTHS calendar months."""
    now = month_index(today.isoformat())
    return sorted(d for d in dates if now - month_index(d) >= RETENTION_MONTHS)


def keep_snapshot(remote: str, date: str, source: str) -> None:
    """Copy a staged build to <date>/. The server-side copy needs the same
    metadata override as latest/, see publish.finalize. A copy that fails
    leaves no partial snapshot to pass for that month's."""
    dest = f"{remote}/{date}/"
    try:
        rclone_with_progress(
            ["rclone", "copy", "--ignore-times", "--metadata", "--metadata-set",
             f"cache-control={IMMUTABLE_CACHE}", source, dest],
            label=f"{date}/", total_bytes=0)
    except subprocess.CalledProcessError:
        subprocess.run(["rclone", "purge", dest])
        raise


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--remote", default="parquetry:parquetry/osm",
                   help="rclone <remote>:<bucket>[/<prefix>]. Default: "
                        "parquetry:parquetry/osm")
    p.add_argument("--execute", action="store_true",
                   help="Actually change the remote. Default is dry-run.")
    args = p.parse_args()

    date = published_date(args.remote)
    if not date:
        sys.exit(f"no published build at {args.remote}")
    staging = f"{args.remote}/{STAGING}/"
    r = subprocess.run(["rclone", "lsf", "--dirs-only", staging],
                       capture_output=True, text=True)
    builds = [b.rstrip("/") for b in r.stdout.split()] if r.returncode == 0 else []

    snapshots = list_snapshot_dirs(args.remote)
    keep = (date in builds
            and not any(month_index(d) == month_index(date) for d in snapshots))
    drop = expired(snapshots | ({date} if keep else set()),
                   dt.datetime.now(dt.timezone.utc).date())

    print(f"Published build: {date}")
    print(f"Keep as snapshot: {date if keep else '-'}")
    print(f"Expired: {' '.join(drop) or '-'}")
    print(f"Staging: {' '.join(builds) or '-'}")
    if not args.execute:
        print("dry-run: no changes made. Pass --execute to apply.")
        return

    if keep:
        keep_snapshot(args.remote, date, f"{staging}{date}/")
    for d in drop:
        sh(["rclone", "purge", f"{args.remote}/{d}/"])
    write_snapshot_manifest(args.remote, date)
    for b in builds:
        sh(["rclone", "purge", f"{staging}{b}/"])


if __name__ == "__main__":
    main()
