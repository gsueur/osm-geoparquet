#!/usr/bin/env python3
"""
Which states vote on a different congressional map than in 2024.

Compares the 120th Congress districts (TIGER/Line 2026, cd120.parquet, the
2026 elections) with the 119th (TIGER/Line 2025, per-state cd119
shapefiles, the map of the 2024 elections). Each new district is matched to
the old district of its state it overlaps most, by intersection over union
(IoU) on geometries simplified to ~50 m. Per state:

  similarity          area-weighted mean of those best IoUs: 1.0 is the
                      same map, lower means more of the state changed
  changed_districts   new districts whose best IoU is under 0.95

The data says a map changed, not why: a legislature, a court or a ballot
measure. census.geomermaids.com reads the output, census/public/
redistricting.json, for its "Redrawn since 2024" presets. The 2024 and 2026
maps are fixed, so this runs by hand, when either TIGER release changes.

  python3 scripts/datasets/census_redistricting.py --work-dir work/cd119
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.census import STATES, download_zip

REPO = Path(__file__).resolve().parent.parent.parent
OUT = REPO / "census" / "public" / "redistricting.json"
OLD_DIR = "https://www2.census.gov/geo/tiger/TIGER2025/CD"
NEW = "https://parquetry.geomermaids.com/census/tiger/2026/legislative"
SIMPLIFY = 0.0005
CHANGED = 0.95


def old_files(work: Path) -> tuple[list[Path], list[str]]:
    """Every state's cd119 zip; the ones census.gov will not serve are
    reported, not guessed."""
    with urllib.request.urlopen(f"{OLD_DIR}/") as r:
        names = sorted(set(re.findall(r"tl_2025_\d+_cd119\.zip", r.read().decode())))
    work.mkdir(parents=True, exist_ok=True)
    files, missing = [], []
    for name in names:
        dest = work / name
        if not dest.is_file():
            try:
                download_zip(f"{OLD_DIR}/{name}", dest, attempts=2)
            except SystemExit as e:
                print(f"  skipped: {e}", flush=True)
                dest.unlink(missing_ok=True)
                missing.append(name.split("_")[2])
                continue
        files.append(dest)
    return files, missing


def compare(con, files: list[Path], new_url: str) -> list[tuple]:
    parts = " UNION ALL ".join(
        f"SELECT STATEFP, CD119FP AS cd, geom FROM ST_Read('/vsizip/{f}/{f.stem}.shp')" for f in files)
    con.execute(f"""CREATE OR REPLACE TABLE old AS
        SELECT STATEFP, cd, ST_MakeValid(ST_SimplifyPreserveTopology(geom, {SIMPLIFY})) AS g
        FROM ({parts}) WHERE cd NOT IN ('ZZ', '98')""")
    con.execute(f"""CREATE OR REPLACE TABLE new AS
        SELECT STATEFP, CD120FP AS cd,
               ST_MakeValid(ST_SimplifyPreserveTopology(geometry::GEOMETRY, {SIMPLIFY})) AS g
        FROM read_parquet('{new_url}') WHERE CD120FP NOT IN ('ZZ', '98')""")
    return con.execute(f"""
        WITH matched AS (
            SELECT n.STATEFP, n.cd,
                   max(ST_Area(ST_Intersection(n.g, o.g)) / ST_Area(ST_Union(n.g, o.g))) AS iou,
                   any_value(ST_Area(n.g)) AS a
            FROM new n JOIN old o ON n.STATEFP = o.STATEFP AND ST_Intersects(n.g, o.g)
            GROUP BY 1, 2)
        SELECT STATEFP, count(*), round(sum(iou * a) / sum(a), 3),
               count(*) FILTER (WHERE iou < {CHANGED})
        FROM matched GROUP BY 1 ORDER BY 3""").fetchall()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--work-dir", type=Path, default=Path("work/cd119"))
    p.add_argument("--new", default=f"{NEW}/cd120.parquet", help="cd120.parquet, path or URL")
    args = p.parse_args()

    files, missing = old_files(args.work_dir)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    rows = compare(con, files, args.new)
    with urllib.request.urlopen(f"{NEW}/_manifest.json") as r:
        bounds = {k: v.get("bounds") for k, v in json.load(r)["states"].items()}
    redrawn = [
        {"fips": fips, "code": STATES[fips][0], "name": STATES[fips][1], "seats": seats,
         "changed_districts": changed, "similarity": sim, "bounds": bounds.get(fips)}
        for fips, seats, sim, changed in rows if changed]
    OUT.write_text(json.dumps({
        "about": "States whose congressional map for the 2026 elections (120th Congress, "
                 "TIGER/Line 2026) differs from the 2024 one (119th, TIGER/Line 2025). "
                 "similarity: area-weighted mean IoU of each new district with its best "
                 "match; changed_districts: new districts under 0.95. Made by "
                 "scripts/datasets/census_redistricting.py.",
        "compared_states": len(rows),
        "not_compared": [{"fips": f, "name": STATES[f][1]} for f in missing if f in STATES],
        "redrawn": redrawn,
    }, indent=2) + "\n")
    for r in redrawn:
        print(f"  {r['name']:16} {r['changed_districts']:>2} of {r['seats']:>2} districts, "
              f"similarity {r['similarity']}")
    print(f"{len(redrawn)} of {len(rows)} states redrawn; not compared: {missing or 'none'}")


if __name__ == "__main__":
    main()
