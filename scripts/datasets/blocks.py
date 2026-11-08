#!/usr/bin/env python3
"""
The 2020 Census tabulation blocks with their counts, as GeoParquet: one file
per state, and the counts on H3 cells with a map.

The Census Bureau publishes the 2020 blocks as TIGER/Line shapefiles, one zip
per state (TIGER2020/TABBLOCK20/tl_2020_<fips>_tabblock20.zip), and only
these carry the 2020 Census counts: POP20 (total population) and HOUSING20
(housing units), from the P.L. 94-171 redistricting data. The later
TIGER/Line blocks (the 2026 national GeoPackage, 6.2 GB) are the same 2020
blocks, maintained, without the counts. Blocks are 2020 Census geography
until 2030, so the files are 2020's: the outlines the counts were tabulated
on. This writes:

  <out>/tiger/2020/block/state=<XX>/blocks.parquet   one file per state, postal code
  <out>/tiger/2020/block/blocks_h3.parquet   population, housing units and blocks
                                             per H3 cell, resolutions 4, 6 and 8
  <out>/tiger/2020/block/blocks_h3.pmtiles   those cells as a map, one band per resolution
  <out>/tiger/2020/block/_manifest.json      per state: rows, sizes, sha256, counts; the gate
  <out>/tiger/2020/block/_source.json        the zips read (Last-Modified, size)
  <out>/tiger/2020/block/ATTRIBUTION.txt

No national file and no map of the blocks themselves: 8.2 million polygons
read best as the states together (s3://parquetry/census/tiger/2020/block/
state=*/blocks.parquet), and the cells map them at a glance.

Columns are TIGER's, values unchanged (STATEFP20 to POP20), plus a `bbox`
declared as the GeoParquet covering. The state zips come from two deliveries
with slightly different columns (REQUIRED, OPTIONAL below); every file
gets the same COLUMNS. The geometry is NAD83 (EPSG:4269) as shipped,
2D, Hilbert-sorted, in row groups of at most 4 MiB (census.py's writer). A
block is counted in the H3 cell of its internal point (INTPTLAT20,
INTPTLON20), whole: a cell's sum is exact for the blocks it holds, and a
block across a cell edge counts in one cell only.

The 50 states, DC and Puerto Rico: the four Island Areas (AS, GU, MP, VI)
are left out, as their TABBLOCK20 files have no counts (POP20 and HOUSING20
are empty on every block; their 2020 counts are in the Island Areas DHC, a
separate product).

The gate, per state: every feature of the shapefile is written, GEOID20 is
unique, no count is missing, and POP20 summed equals the state's 2020
resident population in the Census Bureau's apportionment table
(apportionment.csv).

Usage:
  python3 scripts/datasets/blocks.py upstream
  python3 scripts/datasets/blocks.py build --work-dir work --out-dir out/census [--state RI ...]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import shutil
import sys
import time
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles  # noqa: E402
from datasets.census import (  # noqa: E402
    BBOX, CRS, STATES, check_crs, download_zip, projjson, sha256, write_src,
)
from datasets.gaul import verify  # noqa: E402

YEAR = "2020"
SOURCE_DIR = "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20"
SOURCE_PAGE = ("https://www.census.gov/geographies/mapping-files/time-series/geo/"
               "tiger-line-file.2020.html")
LAYOUTS = ("https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2020/"
           "TGRSHP2020_TechDoc.pdf")
APPORTIONMENT = ("https://www2.census.gov/programs-surveys/decennial/2020/data/apportionment/"
                 "apportionment.csv")
PREFIX = f"tiger/{YEAR}/block"
GLOB = f"s3://parquetry/census/{PREFIX}/state=*/blocks.parquet"
ISLAND_AREAS = {"AS", "GU", "MP", "VI"}
# The 50 states, DC and Puerto Rico, by FIPS: what is built.
TARGETS = {fips: v for fips, v in STATES.items() if v[0] not in ISLAND_AREAS}

# TABBLOCK20's columns. The state zips on census.gov come from several
# deliveries: the first (the 2020 technical documentation, e.g. RI), and
# later ones that add GEOIDFQ20 and drop UATYPE20 (e.g. GA), some with an
# empty VINTAGE too (e.g. LA). Every delivery has REQUIRED; OPTIONAL are
# filled where a delivery lacks them; any other column fails the build.
# Every state file has COLUMNS, in this order.
REQUIRED = ["STATEFP20", "COUNTYFP20", "TRACTCE20", "BLOCKCE20", "GEOID20", "NAME20",
            "MTFCC20", "UR20", "UACE20", "FUNCSTAT20", "ALAND20", "AWATER20",
            "INTPTLAT20", "INTPTLON20", "HOUSING20", "POP20"]
# GEOIDFQ20 is the block's fully qualified GEOID, summary level 101
# ('1010000US' + GEOID20, as the later deliveries write it); UATYPE20 and
# VINTAGE stay empty where a delivery lacks them.
OPTIONAL = {"GEOIDFQ20": "'1010000US' || GEOID20", "UATYPE20": "NULL::VARCHAR",
            "VINTAGE": "NULL::VARCHAR"}
COLUMNS = ["STATEFP20", "COUNTYFP20", "TRACTCE20", "BLOCKCE20", "GEOID20", "GEOIDFQ20",
           "NAME20", "MTFCC20", "UR20", "UACE20", "UATYPE20", "FUNCSTAT20", "ALAND20", "AWATER20",
           "INTPTLAT20", "INTPTLON20", "HOUSING20", "POP20", "VINTAGE"]

H3_RESOLUTIONS = (4, 6, 8)
# (min zoom, max zoom, resolution): nad.py's bands, which fit the tile cap
# for 99 million points; blocks give fewer cells.
H3_BANDS = ((0, 4, 4), (5, 7, 6), (8, 11, 8))
CELL_PROPS = ["h3", "population", "housing_units", "blocks", "state"]


def zip_url(fips: str) -> str:
    return f"{SOURCE_DIR}/tl_{YEAR}_{fips}_tabblock20.zip"


def upstream() -> dict:
    """Last-Modified and size of each state's zip, and a fingerprint of them."""
    states = {}
    for fips, (st, _) in TARGETS.items():
        req = urllib.request.Request(zip_url(fips), method="HEAD")
        with urllib.request.urlopen(req, timeout=60) as r:
            states[st] = {"fips": fips, "url": zip_url(fips),
                          "last_modified": r.headers["Last-Modified"],
                          "bytes": int(r.headers["Content-Length"])}
    blob = json.dumps(states, sort_keys=True).encode()
    return {"fingerprint": hashlib.sha256(blob).hexdigest()[:16], "states": states}


def resident_population() -> dict[str, int]:
    """2020 resident population of the 50 states, DC and Puerto Rico, by
    postal code, from the apportionment table."""
    with urllib.request.urlopen(APPORTIONMENT, timeout=60) as r:
        rows = csv.DictReader(io.StringIO(r.read().decode("utf-8-sig")))
        by_name = {row["Name"]: int(row["Resident Population"].replace(",", ""))
                   for row in rows if row["Year"] == "2020" and row["Geography Type"] == "State"}
    out = {st: by_name[name] for st, name in TARGETS.values() if name in by_name}
    missing = sorted(st for st, _ in TARGETS.values() if st not in out)
    if missing:
        sys.exit(f"apportionment.csv: no 2020 resident population for {missing}")
    return out


def connect(work: Path, memory: str, threads: int | None) -> duckdb.DuckDBPyConnection:
    (work / "spill").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; INSTALL h3 FROM community; LOAD h3; "
                f"SET temp_directory = '{work / 'spill'}'; SET memory_limit = '{memory}'; "
                "SET preserve_insertion_order = false")
    if threads:
        con.execute(f"SET threads = {threads}")
    return con


def build_state(con, fips: str, st: str, work: Path, rel: Path, crs: dict,
                resident: dict[str, int]) -> dict:
    """One state: its file, verified and gated, and its counts per H3 cell
    in work/cells/<XX>.parquet. A state a previous run finished (its entry
    in work/entries/) is kept."""
    entry_path = work / "entries" / f"{st}.json"
    dest = rel / f"state={st}" / "blocks.parquet"
    cells = work / "cells" / f"{st}.parquet"
    if entry_path.is_file() and dest.is_file():
        if not cells.is_file():
            state_cells(con, f"read_parquet('{dest}', hive_partitioning = false)", st, cells)
        return json.loads(entry_path.read_text())
    archive = work / "zips" / f"tl_{YEAR}_{fips}_tabblock20.zip"
    archive.parent.mkdir(parents=True, exist_ok=True)
    if not archive.is_file():
        download_zip(zip_url(fips), archive)
    src = f"/vsizip/{archive}/tl_{YEAR}_{fips}_tabblock20.shp"
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ST_Read('{src}')").fetchall()]
    # GDAL adds OGC_FID, the shapefile's record number: not TIGER's.
    have = [c for c in cols if c not in ("geom", "OGC_FID")]
    missing = [c for c in REQUIRED if c not in have]
    unknown = [c for c in have if c not in COLUMNS]
    if missing or unknown:
        sys.exit(f"{st}: columns {have}: missing {missing}, unknown {unknown}")
    filled = [c for c in OPTIONAL if c not in have]
    # One type per column in every state file: VINTAGE is a number in one
    # delivery (LA) and absent in the others, so it is text everywhere.
    select = ", ".join(c if c in have and c != "VINTAGE" else
                       "VINTAGE::VARCHAR AS VINTAGE" if c in have else f"{OPTIONAL[c]} AS {c}"
                       for c in COLUMNS)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE src AS
        SELECT *, {BBOX} FROM (
            SELECT {select}, ST_SetCRS(ST_Force2D(geom), '{CRS}') AS geometry
            FROM ST_Read('{src}'))""")
    n, ids, pop, housing, empty, nogeom, nocount, badfq = con.execute("""
        SELECT count(*), count(DISTINCT GEOID20), sum(POP20), sum(HOUSING20),
               count(*) FILTER (WHERE POP20 = 0), count(*) FILTER (WHERE geometry IS NULL),
               count(*) FILTER (WHERE POP20 IS NULL OR HOUSING20 IS NULL),
               count(*) FILTER (WHERE GEOIDFQ20 IS DISTINCT FROM '1010000US' || GEOID20)
        FROM src""").fetchone()
    problems = []
    if badfq:
        problems.append(f"{badfq} GEOIDFQ20 not 1010000US + GEOID20")
    if nocount:
        problems.append(f"{nocount} blocks without POP20 or HOUSING20")
    if ids != n:
        problems.append(f"{n - ids} repeated GEOID20")
    if nogeom:
        problems.append(f"{nogeom} blocks without a geometry")
    expected = resident[st]
    if pop != expected:
        problems.append(f"POP20 sums to {pop:,}, the 2020 resident population is {expected:,}")
    if problems:
        sys.exit(f"gate, {st}: " + "; ".join(problems))
    dest.parent.mkdir(parents=True, exist_ok=True)
    e = write_src(con, dest, crs)
    verify(con, dest.parent, ["blocks"])
    check_crs(con, dest)
    written = con.execute(f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
    if written != n:
        sys.exit(f"gate, {st}: {n:,} blocks read, {written:,} written")
    state_cells(con, "src", st, cells)
    con.execute("DROP TABLE src")
    archive.unlink()
    entry = {"state": st, "fips": fips, "path": f"state={st}/blocks.parquet",
             "features": n, "bytes": e["bytes"], "row_groups": e["row_groups"],
             "largest_row_group_bytes": e["largest_row_group_bytes"], "sha256": e["sha256"],
             "source_columns": have, "filled": filled,
             "population": int(pop), "housing_units": int(housing), "unpopulated_blocks": empty,
             "resident_population_2020": expected}
    entry_path.parent.mkdir(parents=True, exist_ok=True)
    entry_path.write_text(json.dumps(entry, indent=2) + "\n")
    return entry


def state_cells(con, relation: str, st: str, dest: Path) -> None:
    """A state's population, housing units and blocks per H3 cell of each
    block's internal point, at every resolution of H3_RESOLUTIONS."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    parts = " UNION ALL ".join(
        f"SELECT {r} AS h3_res, h3_latlng_to_cell(INTPTLAT20::DOUBLE, INTPTLON20::DOUBLE, {r}) "
        f"AS cell, POP20, HOUSING20 FROM {relation}" for r in H3_RESOLUTIONS)
    con.execute(f"""COPY (SELECT h3_res, cell, sum(POP20)::BIGINT AS population,
                                 sum(HOUSING20)::BIGINT AS housing_units, count(*) AS blocks,
                                 '{st}' AS state
                          FROM ({parts}) GROUP BY ALL)
                    TO '{dest}' (FORMAT PARQUET)""")


def write_cells(con, work: Path, dest: Path) -> dict:
    """The states' cells as one file: a cell on a state line sums both
    states' blocks, and is labelled with the state holding most of its
    population."""
    con.execute(f"""
        COPY (
          WITH c AS (SELECT * FROM read_parquet('{work / 'cells'}/[A-Z][A-Z].parquet')),
          g AS (SELECT h3_res, cell, sum(population)::BIGINT AS population,
                       sum(housing_units)::BIGINT AS housing_units, sum(blocks)::BIGINT AS blocks,
                       arg_max(state, population) AS state
                FROM c GROUP BY ALL)
          SELECT h3_h3_to_string(cell) AS h3, h3_res, population, housing_units, blocks, state,
                 ST_GeomFromWKB(h3_cell_to_boundary_wkb(cell)) AS geometry
          FROM g ORDER BY h3_res, cell)
        TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    counts = dict(con.execute(f"SELECT h3_res, count(*) FROM read_parquet('{dest}') "
                              "GROUP BY 1 ORDER BY 1").fetchall())
    return {"file": dest.name, "bytes": dest.stat().st_size, "sha256": sha256(dest),
            "cells": {str(k): v for k, v in counts.items()}}


def tiles(con, cells: Path, work: Path, dest: Path) -> dict:
    """One band per resolution, joined into one archive by pmtiles.pyramid()."""
    bands = []
    for lo, hi, res in H3_BANDS:
        src = work / f"h3_r{res}.parquet"
        con.execute(f"COPY (SELECT * EXCLUDE (h3_res) FROM read_parquet('{cells}') "
                    f"WHERE h3_res = {res}) TO '{src}' (FORMAT PARQUET)")
        part = work / f"h3_r{res}.pmtiles"
        pmtiles.tile(src, part, layer=f"cells_r{res}", min_zoom=lo, max_zoom=hi,
                     properties=CELL_PROPS, verbatim=True)
        bands.append((lo, hi, part, f"cells_r{res}"))
    entry = pmtiles.pyramid(bands, dest)
    entry["bands"] = [{"min_zoom": lo, "max_zoom": hi, "h3_res": res, "layer": f"cells_r{res}"}
                      for lo, hi, res in H3_BANDS]
    for _, _, part, _ in bands:
        part.unlink()
    return entry


def attribution() -> str:
    return f"""\
2020 Census tabulation blocks, with their 2020 Census population and housing units
U.S. Census Bureau, 2020 TIGER/Line Shapefiles (TABBLOCK20) and the 2020 Census
Redistricting Data (P.L. 94-171) Summary File.
Republished as GeoParquet by Geomermaids: https://parquetry.geomermaids.com/census/

Source: {SOURCE_DIR}/ (one zip per state)
About TIGER/Line 2020: {SOURCE_PAGE}
Record layouts: {LAYOUTS}

Licence: a work of the US federal government, not subject to copyright in the
United States (17 U.S.C. 105). Credit "U.S. Census Bureau, 2020 TIGER/Line
Shapefiles". The Census Bureau makes no warranty on TIGER/Line's accuracy;
its boundaries are for statistical data collection and tabulation only.

Changes made here: columns and values unchanged; the geometry 2D, NAD83 as
shipped; added `bbox`. Rows sorted on a Hilbert curve. blocks_h3.parquet sums
POP20, HOUSING20 and the block count per H3 cell of each block's internal point.
"""


def build(work: Path, out: Path, only: list[str] | None, memory: str, threads: int | None,
          no_tiles: bool) -> None:
    t0 = time.monotonic()
    rel = out / PREFIX
    rel.mkdir(parents=True, exist_ok=True)
    src = upstream()
    resident = resident_population()
    todo = [(fips, st) for fips, (st, _) in TARGETS.items() if not only or st in only]
    unknown = sorted(set(only or []) - {st for _, st in todo})
    if unknown:
        sys.exit(f"--state: not one of the 50 states, DC or PR: {unknown}")
    con = connect(work, memory, threads)
    crs = projjson(con)
    files = []
    for i, (fips, st) in enumerate(todo, 1):
        e = build_state(con, fips, st, work, rel, crs, resident)
        files.append(e)
        print(f"  [{i}/{len(todo)}] {st}: {e['features']:,} blocks, {e['population']:,} people, "
              f"{e['housing_units']:,} housing units, {e['bytes'] / 1e6:,.1f} MB, "
              f"{e['row_groups']} groups, {time.monotonic() - t0:.0f} s", flush=True)
    complete = len(files) == len(TARGETS)
    total_pop = sum(f["population"] for f in files)
    us = sum(f["population"] for f in files if f["state"] != "PR")
    h3 = write_cells(con, work, rel / "blocks_h3.parquet")
    tiles_entry = None if no_tiles else tiles(con, rel / "blocks_h3.parquet", work,
                                              rel / "blocks_h3.pmtiles")
    con.close()
    manifest = {
        "dataset": "census-blocks", "year": YEAR, "crs": CRS,
        "complete": complete,
        "total_features": sum(f["features"] for f in files),
        "population": total_pop, "housing_units": sum(f["housing_units"] for f in files),
        "gate": {
            "compared_with": APPORTIONMENT,
            "states_compared": len(files),
            "united_states_population": us,
            "left_out": sorted(ISLAND_AREAS),
            "rule": "per state: every shapefile feature written, GEOID20 unique, no count "
                    "missing, POP20 summed equal to the 2020 resident population",
        },
        "files": sorted(files, key=lambda f: f["state"]),
        "h3": h3, "pmtiles": tiles_entry, "glob": GLOB,
        "built": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (rel / "_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (rel / "_source.json").write_text(json.dumps(src, indent=2) + "\n")
    (rel / "ATTRIBUTION.txt").write_text(attribution())
    shutil.rmtree(work / "spill", ignore_errors=True)
    print(f"  {len(files)} states, {manifest['total_features']:,} blocks, {total_pop:,} people"
          f"{' (the 50 states and DC: ' + format(us, ',') + ')' if complete else ''}, "
          f"{sum(h3['cells'].values()):,} H3 cells, {time.monotonic() - t0:.0f} s", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="Last-Modified and size of each state's zip")
    b = sub.add_parser("build", help="fetch, gate, write and tile")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help="the census/ prefix")
    b.add_argument("--state", action="append", help="these states only (a test run)")
    b.add_argument("--memory", default="6GB", help="DuckDB memory_limit")
    b.add_argument("--threads", type=int)
    b.add_argument("--no-tiles", action="store_true")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
    else:
        build(args.work_dir, args.out_dir, args.state, args.memory, args.threads, args.no_tiles)


if __name__ == "__main__":
    main()
