#!/usr/bin/env python3
"""
Edit the GAUL _manifest.json for a whole-world file published by hand.

The manifest is what the catalog reads (size, sha256, row groups per file),
so a file replaced or renamed outside gaul.py has to be reflected here or
the catalog publishes a checksum that does not match the bytes.

  --replace L1=/path/GAUL_2024_L1.parquet   measure the file and record it under
                                            the layer's entry, with its name
  --rename  L0_derived=GAUL_2024_L0_derived.parquet
                                            the same bytes under a new name

  python3 scripts/datasets/gaul_manifest.py --manifest _manifest.json \\
      --replace L1=GAUL_2024_L1.parquet --replace L2=GAUL_2024_L2.parquet \\
      --rename L0_derived=GAUL_2024_L0_derived.parquet --note "..." --out _manifest.json

The procedure followed on 2026-09-23, with rclone's `parquetry` remote:
  1. rclone copyto the new files to <remote>/2024/, with
     --header-upload "Cache-Control: public, max-age=31536000, immutable"
     and "Content-Type: application/vnd.apache.parquet". A server-side copy
     drops both headers on R2, so a rename goes through the local disk.
  2. rclone copyto the live _manifest.json down, edit it with this script,
     copyto it back with "Cache-Control: public, max-age=300".
  3. gaul_catalog.py publish --remote <remote>: rebuilds the catalog from the
     published files and the live manifest, runs rashid, uploads.
  4. Only then delete the old names.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import duckdb


def measure(path: Path) -> dict:
    con = duckdb.connect()
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
    groups, largest = con.execute(f"""
        SELECT count(*), max(b) FROM (
            SELECT row_group_id, sum(total_compressed_size) AS b
            FROM parquet_metadata('{path}') GROUP BY 1)
    """).fetchone()
    return {"file": path.name, "features": rows, "bytes": path.stat().st_size,
            "row_groups": groups, "largest_row_group_bytes": largest,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--out", type=Path, default=None, help="default: overwrite --manifest")
    p.add_argument("--replace", action="append", default=[], metavar="LAYER=PATH")
    p.add_argument("--rename", action="append", default=[], metavar="LAYER=FILENAME")
    p.add_argument("--note", default=None,
                   help="recorded as `written_by` on every replaced entry")
    args = p.parse_args()

    manifest = json.loads(args.manifest.read_text())
    entries = {e["name"]: e for e in manifest["files"]}

    for spec in args.replace:
        layer, path = spec.split("=", 1)
        path = Path(path)
        if layer not in entries:
            sys.exit(f"no layer {layer!r} in the manifest (have {sorted(entries)})")
        if not path.is_file():
            sys.exit(f"{path}: not a file")
        m = measure(path)
        if m["features"] != entries[layer]["features"]:
            sys.exit(f"{layer}: {path.name} has {m['features']:,} rows, the manifest says "
                     f"{entries[layer]['features']:,}; refusing")
        entries[layer].update(m)
        if args.note:
            entries[layer]["written_by"] = args.note
        print(f"  {layer}: {m['file']}, {m['bytes'] / 1e6:.1f} MB, {m['row_groups']} row groups, "
              f"largest {m['largest_row_group_bytes'] / 1e6:.1f} MB")

    for spec in args.rename:
        layer, name = spec.split("=", 1)
        if layer not in entries:
            sys.exit(f"no layer {layer!r} in the manifest")
        entries[layer]["file"] = name
        print(f"  {layer}: -> {name}")

    manifest["total_features"] = sum(e["features"] for e in manifest["files"])
    # `themes` names files (see gaul.py), so a rename moves its key.
    manifest["themes"] = {Path(e["file"]).stem: e["features"] for e in manifest["files"]}
    (args.out or args.manifest).write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
