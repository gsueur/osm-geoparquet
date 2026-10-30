#!/usr/bin/env python3
"""
Clear the staging folder on the R2 remote once latest/ is validated.

A build collects in _staging/<YYYY-MM-DD>/ until publish.py's finalize copies
it to latest/. Every run of this script deletes all of _staging/, including
what a failed build left behind, so run it only after a build went live.

Dry-run by default. Pass --execute to actually delete.

Typical use (from nightly.yml, after remote validation passes):
  python3 scripts/prune.py --execute --remote parquetry:parquetry/osm
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from publish import STAGING, sh  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--remote", default="parquetry:parquetry/osm",
                   help="rclone <remote>:<bucket>[/<prefix>]. Default: "
                        "parquetry:parquetry/osm")
    p.add_argument("--execute", action="store_true",
                   help="Actually delete. Default is dry-run.")
    args = p.parse_args()

    staging = f"{args.remote}/{STAGING}/"
    r = subprocess.run(["rclone", "lsf", "--dirs-only", staging],
                       capture_output=True, text=True)
    builds = r.stdout.split() if r.returncode == 0 else []
    if not builds:
        print(f"Nothing under {staging}")
        return
    print(f"{staging}: {' '.join(builds)}")
    if not args.execute:
        print("dry-run: no changes made. Pass --execute to delete.")
        return
    for b in builds:
        sh(["rclone", "purge", f"{staging}{b}"])


if __name__ == "__main__":
    main()
