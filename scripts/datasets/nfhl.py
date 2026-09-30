#!/usr/bin/env python3
"""
FEMA National Flood Hazard Layer, one file for the United States and one per state.

The NFHL is delivered by FEMA as one zipped shapefile set per county. The
flood hazard areas (layer S_FLD_HAZ_AR) of every county-wide delivery are
assembled, normalized and subdivided by the pipeline of
https://github.com/gsueur/nfhl-geoparquet-workshop, which leaves one
national file of pieces: every flood zone polygon cut into pieces of at
most 100 vertices, each carrying its zone's attributes. This script
packages that file for the bucket and writes:

  <out>/<snapshot>/flood_hazard_areas.parquet             the United States, sorted
                                                          by state, then along a
                                                          Hilbert curve
  <out>/<snapshot>/state=<XX>/flood_hazard_areas.parquet  one state, same columns
  <out>/<snapshot>/_manifest.json, state=<XX>/_manifest.json,
  <out>/index.json, <out>/snapshots.json                  the parquetry repository files
  <out>/<snapshot>/ATTRIBUTION.txt

Nothing is changed but the order: same rows, same geometries (hashed), and
the state files add up to the national file. Nothing is written to the
manifests unless every check passes.

  python3 scripts/datasets/nfhl.py --source nfhl_us.parquet \\
      --snapshot 2026-09-30 --out-dir out/nfhl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

import duckdb
import pycountry

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.infra import GEOMETRY_TYPE_NAMES

STEM = "flood_hazard_areas"
CRS = "EPSG:4269"          # NAD83, as FEMA delivers it
CRS_CODE = 4269
# 20,480 pieces are about 8 MB: a point lookup reads one group. Half that
# would double a footer that is already 8 MB for the national file.
ROW_GROUP_ROWS = 20_480
ZSTD_LEVEL = 9            # see clc.py: 15 costs 11x the time for nothing with DuckDB's writer
COLUMNS = """state county dfirm_id fema_update_date source_feature_id piece_id
             flood_zone zone_subtype sfha static_bfe dual_zone risk floodplain subzone""".split()
HILBERT = "ST_Hilbert(geometry, ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90)))"


def state_name(code: str) -> str:
    return pycountry.subdivisions.get(code=f"US-{code}").name


def projjson(con: duckdb.DuckDBPyConnection) -> dict:
    """The CRS as PROJJSON, for the `geo` footer: DuckDB's V2 writer puts it
    in the footer of a probe file."""
    with tempfile.TemporaryDirectory() as tmp:
        probe = Path(tmp) / "probe.parquet"
        con.execute(f"""COPY (SELECT ST_SetCRS(ST_Point(0, 0), '{CRS}') AS g)
                        TO '{probe}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')""")
        geo = con.execute(f"SELECT decode(value) FROM parquet_kv_metadata('{probe}') "
                          "WHERE key = 'geo'").fetchone()[0]
    crs = json.loads(geo)["columns"]["g"]["crs"]
    assert crs["id"] == {"authority": "EPSG", "code": CRS_CODE}, crs.get("id")
    return crs


def write(con: duckdb.DuckDBPyConnection, query: str, dest: Path, crs: dict) -> None:
    """One GeoParquet 2.0 file: native GEOMETRY typed EPSG:4269, a `geo`
    footer with the bbox covering, row groups of ROW_GROUP_ROWS."""
    xmin, ymin, xmax, ymax, types = con.execute(f"""
        SELECT min(bbox.xmin), min(bbox.ymin), max(bbox.xmax), max(bbox.ymax),
               list(DISTINCT ST_GeometryType(geometry)::VARCHAR)
        FROM ({query})""").fetchone()
    geo = json.dumps({
        "version": "2.0.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB",
            "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in types),
            "crs": crs,
            "bbox": [xmin, ymin, xmax, ymax],
            "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}},
        }},
    }).replace("'", "''")
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY ({query}) TO '{dest}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE',
            COMPRESSION ZSTD, COMPRESSION_LEVEL {ZSTD_LEVEL}, ROW_GROUP_SIZE {ROW_GROUP_ROWS},
            KV_METADATA {{geo: '{geo}'}})""")


def verify(con: duckdb.DuckDBPyConnection, path: Path) -> dict:
    """Fail unless the file is what its footer says; return its manifest entry."""
    geos = [v for k, v in con.execute("SELECT key::VARCHAR, decode(value) FROM parquet_kv_metadata(?)",
                                      [str(path)]).fetchall() if k == "geo"]
    assert len(geos) == 1, f"{path}: {len(geos)} geo keys"
    col = json.loads(geos[0])["columns"]["geometry"]
    assert col["crs"]["id"]["code"] == CRS_CODE, f"{path}: crs {col['crs'].get('id')}"
    logical = con.execute("SELECT logical_type FROM parquet_schema(?) WHERE name = 'geometry'",
                          [str(path)]).fetchone()[0]
    assert logical and logical.startswith("GeometryType(crs=") and str(CRS_CODE) in logical, \
        f"{path}: geometry is {str(logical)[:80]}"
    groups = [n for _, n in con.execute("""
        SELECT DISTINCT row_group_id, row_group_num_rows FROM parquet_metadata(?)
        ORDER BY row_group_id""", [str(path)]).fetchall()]
    assert all(n == ROW_GROUP_ROWS for n in groups[:-1]), f"{path}: row groups {groups[:5]}"
    rows, bad, typ = con.execute(f"""
        SELECT count(*),
               count(*) FILTER (WHERE bbox.xmin > ST_XMin(geometry) OR bbox.ymin > ST_YMin(geometry)
                                   OR bbox.xmax < ST_XMax(geometry) OR bbox.ymax < ST_YMax(geometry)),
               any_value(typeof(geometry))
        FROM read_parquet('{path}')""").fetchone()
    assert bad == 0, f"{path}: {bad} bboxes do not contain their geometry"
    assert typ.upper() == f"GEOMETRY('{CRS}')", f"{path}: reads as {typ}"
    largest = con.execute("""SELECT max(b) FROM (SELECT sum(total_compressed_size) AS b
                             FROM parquet_metadata(?) GROUP BY row_group_id)""", [str(path)]).fetchone()[0]
    return {"file": path.name, "features": rows, "bytes": path.stat().st_size,
            "row_groups": len(groups), "largest_row_group_bytes": largest,
            "sha256": sha256(path)}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 24), b""):
            h.update(chunk)
    return h.hexdigest()


def fingerprint(con: duckdb.DuckDBPyConnection, path: Path | str) -> tuple:
    """What must not change: the pieces, the zones (each has a piece 0) and the geometries."""
    return con.execute(f"""
        SELECT count(*), count(*) FILTER (WHERE piece_id = 0),
               sum(hash(ST_AsWKB(geometry))::HUGEINT)
        FROM read_parquet('{path}')""").fetchone()


def attribution(snapshot: str, pieces: int, zones: int, counties: int, states: int,
                first: str, last: str, shared: int, shared_in: int) -> str:
    return f"""\
# Data attribution

This data is derived from the National Flood Hazard Layer (NFHL) of the
Federal Emergency Management Agency (FEMA), U.S. Department of Homeland
Security.

  Source:       https://msc.fema.gov/portal/home (FEMA Flood Map Service Center)
                https://hazards.fema.gov/femaportal/NFHL/searchResult
  Product:      NFHL, layer S_FLD_HAZ_AR (flood hazard areas), the
                county-wide deliveries listed on {snapshot}
  Producer:     Federal Emergency Management Agency (FEMA)
  Licence:      U.S. federal government work, public domain. No restriction
                on use or redistribution.
  Attribution:  FEMA National Flood Hazard Layer

# Not for official use

These files are a repackaging, not a FEMA product. They are not the
effective Flood Insurance Rate Map and must not be used for flood zone
determinations, insurance rating, regulatory or legal purposes: for those,
use the FEMA Flood Map Service Center. FEMA has not reviewed, approved or
endorsed these files. FEMA updates the NFHL continuously; this is its state on
{snapshot}, and each row carries the date of the county delivery it comes
from (`fema_update_date`, from {first} to {last}).

# Coverage

{counties:,} county-wide deliveries in {states} states and territories: every
county-wide dataset FEMA listed on {snapshot} that holds a flood hazard
layer. Counties FEMA has not mapped digitally are absent, as are the
community-level deliveries (a town or city published on its own). An
absent area is unmapped here, not free of flood hazard.

# What was changed

  1. Pieces. Flood zone polygons reach hundreds of thousands of vertices.
     Each one is cut into pieces of at most 100 vertices (ST_Subdivide), so
     a point lookup tests a small polygon: {zones:,} zones make {pieces:,}
     pieces. A row is a piece, not a zone: `piece_id` numbers the pieces
     of a zone from 0, so `piece_id = 0` counts zones. `source_feature_id`
     is FEMA's FLD_AR_ID, which FEMA does not keep unique inside every
     county: {shared:,} zones in {shared_in:,} counties share theirs with another
     zone. Elsewhere (`state`, `county`, `source_feature_id`) identifies
     the zone and dissolving on it gives the original outline back.
     Invalid source geometries were made valid first.
  2. Columns. `flood_zone` (FLD_ZONE), `zone_subtype` (ZONE_SUBTY), `sfha`
     (SFHA_TF), `static_bfe` (STATIC_BFE, NULL where FEMA writes -9999),
     `dual_zone` (DUAL_ZONE), `dfirm_id` and `source_feature_id`
     (FLD_AR_ID) are FEMA's values. `state` (postal code), `county` and
     `fema_update_date` come from the delivery. Three columns are not part
     of the NFHL, they are a reading of FEMA's zone and subtype added by
     Geomermaids: `risk` (1 percent flood zone, 0.2 percent flood zone,
     minimal, undetermined, water, unmapped), `floodplain` (the same as
     `sfha`) and `subzone` (the zone of a dual or AR zone).
  3. Order. Sorted by state, then along a Hilbert curve, in row groups of
     {ROW_GROUP_ROWS:,} pieces with a covering bbox column, so a filter on
     state or on a bbox reads only the groups it touches.

Coordinates are FEMA's, NAD83 (EPSG:4269), unchanged.

The pipeline that builds these files is public:
  https://github.com/gsueur/nfhl-geoparquet-workshop

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--source", type=Path, required=True,
                   help="the national file of subdivided pieces, with its bbox column")
    p.add_argument("--snapshot", required=True, help="the day the FEMA catalog was read, YYYY-MM-DD")
    p.add_argument("--out-dir", type=Path, required=True, help="the dataset folder, e.g. out/nfhl")
    p.add_argument("--work-dir", type=Path, default=Path("work"), help="DuckDB spill space")
    p.add_argument("--memory-limit", default="12GB")
    args = p.parse_args()
    time.strptime(args.snapshot, "%Y-%m-%d")

    out = args.out_dir / args.snapshot
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    args.work_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; SET temp_directory = '{args.work_dir}'; "
                f"SET memory_limit = '{args.memory_limit}'")
    t0 = time.monotonic()

    before = fingerprint(con, args.source)
    print(f"  source: {before[0]:,} pieces of {before[1]:,} zones", flush=True)

    crs = projjson(con)
    cols = ", ".join(COLUMNS)
    us = out / f"{STEM}.parquet"
    write(con, f"""
        SELECT {cols}, ST_SetCRS(geometry, '{CRS}') AS geometry, bbox
        FROM read_parquet('{args.source}')
        ORDER BY state, {HILBERT}
    """, us, crs)
    files = [verify(con, us)]
    after = fingerprint(con, us)
    assert after == before, f"the national file differs from the source: {after} vs {before}"
    print(f"  {us.name}: {files[0]['bytes'] / 1e9:.2f} GB, {files[0]['row_groups']} row groups, "
          f"{time.monotonic() - t0:.0f} s", flush=True)

    # One row per county delivery: what the snapshot is made of.
    deliveries = con.execute(f"""
        SELECT state, county, any_value(dfirm_id), min(fema_update_date)::VARCHAR, count(*),
               count(*) FILTER (WHERE piece_id = 0), count(DISTINCT fema_update_date)
        FROM read_parquet('{us}') GROUP BY 1, 2 ORDER BY 1, 2""").fetchall()
    assert all(d[6] == 1 for d in deliveries), "a county with two delivery dates"
    by_state: dict[str, list] = {}
    for st, county, dfirm, date, n, zones, _ in deliveries:
        by_state.setdefault(st, []).append(
            {"county": county, "dfirm_id": dfirm, "fema_update_date": date, "pieces": n, "zones": zones})

    # The national file is in state, then Hilbert, order: a state file is a slice of it.
    datasets = [{"path": "", "code": "US", "name": "NFHL flood hazard areas, United States"}]
    counts: dict[str, int] = {}
    for code, counties in by_state.items():
        n = sum(c["pieces"] for c in counties)
        dest = out / f"state={code}" / f"{STEM}.parquet"
        write(con, f"""SELECT * FROM read_parquet('{us}') WHERE state = '{code}'
                       ORDER BY {HILBERT}""", dest, crs)
        entry = verify(con, dest)
        assert entry["features"] == n, f"{code}: {entry['features']} rows, expected {n}"
        counts[code] = n
        name = state_name(code)
        (dest.parent / "_manifest.json").write_text(json.dumps({
            "country": "US", "state": code, "state_name": name,
            "total_features": n, "themes": {STEM: n}, "files": [entry], "counties": counties,
        }, indent=2) + "\n")
        datasets.append({"path": f"state={code}", "code": code, "name": name})
        print(f"  {code} {name:22} {len(counties):>4} counties {n:>11,}  {entry['bytes'] / 1e6:9.1f} MB",
              flush=True)
    assert sum(counts.values()) == before[0], \
        f"state files hold {sum(counts.values()):,} rows, the source {before[0]:,}"

    dates = [d[3] for d in deliveries]
    zones = sum(d[5] for d in deliveries)
    assert zones == before[1], f"{zones:,} zones by county, {before[1]:,} in the source"
    # FEMA's FLD_AR_ID is not unique inside every county.
    shared, shared_in = con.execute(f"""
        SELECT coalesce(sum(n), 0), count(DISTINCT (state, county)) FROM (
            SELECT state, county, source_feature_id, count(*) AS n FROM read_parquet('{us}')
            WHERE piece_id = 0 GROUP BY ALL HAVING n > 1)""").fetchone()
    (out / "_manifest.json").write_text(json.dumps({
        "state_name": f"FEMA NFHL flood hazard areas, United States, as of {args.snapshot}",
        "snapshot": args.snapshot, "source": "FEMA National Flood Hazard Layer, S_FLD_HAZ_AR",
        "total_features": before[0], "zones": zones, "county_deliveries": len(deliveries),
        "zones_sharing_a_source_feature_id": int(shared),
        "fema_update_dates": [min(dates), max(dates)],
        "themes": {STEM: before[0]}, "files": files, "states": counts,
    }, indent=2) + "\n")
    (args.out_dir / "index.json").write_text(json.dumps({"datasets": datasets}, indent=2) + "\n")
    (args.out_dir / "snapshots.json").write_text(json.dumps({
        "latest": f"{args.snapshot}/",
        "snapshots": [{"date": args.snapshot, "path": f"{args.snapshot}/"}],
    }, indent=2) + "\n")
    (out / "ATTRIBUTION.txt").write_text(attribution(
        args.snapshot, before[0], zones, len(deliveries), len(by_state), min(dates), max(dates),
        int(shared), shared_in))
    print(f"  {len(by_state)} states, {len(deliveries):,} counties, done in {time.monotonic() - t0:.0f} s",
          flush=True)


if __name__ == "__main__":
    sys.exit(main())
