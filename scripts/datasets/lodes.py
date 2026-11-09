#!/usr/bin/env python3
"""
LEHD Origin-Destination Employment Statistics (LODES8), 2024, as Parquet:
jobs by workplace, by residence and from home to work, per 2020 Census
block, one file per state, and the jobs rolled up to tracts and counties
with their outlines and a map.

The Census Bureau's LEHD program publishes LODES as gzipped CSVs, per state
and table (https://lehd.ces.census.gov/data/lodes/LODES8/<st>/{wac,rac,od}/).
Every LODES8 year is enumerated on 2020 Census blocks. This reads the All
Jobs files (JT00) of the 50 states and DC and writes:

  <out>/lodes/2024/state=<XX>/wac.parquet   Workplace Area Characteristics: jobs per
                                            workplace block (S000, every segment column)
  <out>/lodes/2024/state=<XX>/rac.parquet   Residence Area Characteristics: jobs per
                                            residence block
  <out>/lodes/2024/state=<XX>/od.parquet    Origin-Destination: jobs per (workplace block,
                                            residence block), the main and aux parts
  <out>/lodes/2024/wac_tract.parquet        WAC summed per 2020 tract, with its outline
  <out>/lodes/2024/rac_tract.parquet        RAC summed per 2020 tract, with its outline
  <out>/lodes/2024/wac_county.parquet       ... per 2020 county
  <out>/lodes/2024/rac_county.parquet
  <out>/lodes/2024/wac_tract.pmtiles        jobs by workplace tract, the map
  <out>/lodes/2024/rac_tract.pmtiles        jobs by residence tract
  <out>/lodes/2024/_manifest.json           per state: year, rows, sizes, sha256, sums; the gate
  <out>/lodes/2024/_source.json             the files read (Last-Modified, size), each
                                            state's data vintage, a fingerprint
  <out>/lodes/2024/ATTRIBUTION.txt

<out> is the census/ prefix. Columns are LODES's, values unchanged
(geocodes as 15-character text, counts as integers, createdate as text),
plus `year`, the data year of the state's files, and in od.parquet
`interstate`, true for the aux rows (residence outside the state). Rows are
sorted by w_geocode (h_geocode for RAC, then h_geocode in OD), so a county
or tract prefix reads few row groups.

Not every state has 2024. LEHD has no OD or WAC data for Alaska after 2016
and for Michigan after 2021 (LODES technical document 8.5, data
availability). Their RAC files go on, but count only the jobs held in the
states that report: a Michigan resident working in Michigan is missing from
MI's 2024 RAC. So for a state without the year, all three tables come from
its latest year with OD and WAC, the same for the three, and `year` says
which. FALLBACK names the states allowed to fall back; any other state
without 2024 fails the build. A national sum mixes data years: read `year`.
The other side of the gap stays in the 2024 files: an Ohio resident working
in Michigan is in no 2024 file. Puerto Rico is not in LODES.

The firm age and size columns of the WAC (CFA01 to CFS05) are zero in every
JT00 file: LEHD publishes them for private jobs (JT02) only. They are kept
in the state files, as published, and left out of the rollups.

The rollups sum a state's blocks per 2020 tract (the first 11 characters of
the geocode) and county (5), and join the TIGER/Line 2020 outline: LODES8
blocks are 2020 blocks, so the 2020 tracts and counties hold them exactly
(Connecticut's 2020 counties, not the planning regions of TIGER/Line 2022
and later). NAD83 (EPSG:4269), Hilbert order, bbox covering (census.py's
writer).

The gate, per state: every row read is written, every geocode is 15 digits
in the state (a residence in OD aux is outside it), no workplace and no
(workplace, residence) pair repeats, and the WAC's C000 summed equals the
OD's S000 summed (main and aux), the same jobs counted at their workplace.
Every rolled-up tract and county is a TIGER/Line 2020 one. And across the
states, on a complete build: each state's RAC C000 summed equals the OD S000
summed over every state's OD for its residents (its own main part and the
other states' aux parts), for every state of YEAR, the jobs of its residents
counted at home and at work. The fallback states are left out of that
check: their residents' jobs in other states are in other years' files.

Usage:
  python3 scripts/datasets/lodes.py upstream
  python3 scripts/datasets/lodes.py build --work-dir work --out-dir out/census [--state RI ...]
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles  # noqa: E402
from datasets.census import (  # noqa: E402
    BBOX, CRS, STATES, check_crs, download_zip, projjson, sha256, write_src,
)
from datasets.gaul import verify  # noqa: E402

YEAR = "2024"
VERSION = "LODES8"
BASE = f"https://lehd.ces.census.gov/data/lodes/{VERSION}"
TECHDOC = f"{BASE}/LODESTechDoc8.5.pdf"
SOURCE_PAGE = "https://lehd.ces.census.gov/data/"
TIGER = "https://www2.census.gov/geo/tiger/TIGER2020"
COUNTY_ZIP = f"{TIGER}/COUNTY/tl_2020_us_county.zip"
PREFIX = f"lodes/{YEAR}"
GLOB = f"s3://parquetry/census/{PREFIX}/state=*/{{table}}.parquet"
UA = {"User-Agent": "geomermaids-parquetry"}
# The 50 states and DC: LODES has no Puerto Rico and no Island Areas.
TARGETS = {fips: v for fips, v in STATES.items() if int(fips) <= 56}
# The states LEHD has no OD and WAC for in recent years (technical document
# 8.5, data availability: Alaska from 2017, Michigan from 2022). They take
# their latest year with all four files; any other state must have YEAR.
FALLBACK = {"AK", "MI"}

# The files' columns, as the technical document lists them; a file whose
# header differs fails the build.
SEGMENTS = (["CA01", "CA02", "CA03", "CE01", "CE02", "CE03"]
            + [f"CNS{i:02d}" for i in range(1, 21)]
            + ["CR01", "CR02", "CR03", "CR04", "CR05", "CR07", "CT01", "CT02",
               "CD01", "CD02", "CD03", "CD04", "CS01", "CS02"])
FIRM = [f"CFA{i:02d}" for i in range(1, 6)] + [f"CFS{i:02d}" for i in range(1, 6)]
COLUMNS = {
    "wac": ["w_geocode", "C000", *SEGMENTS, *FIRM, "createdate"],
    "rac": ["h_geocode", "C000", *SEGMENTS, "createdate"],
    "od": ["w_geocode", "h_geocode", "S000", "SA01", "SA02", "SA03", "SE01", "SE02", "SE03",
           "SI01", "SI02", "SI03", "createdate"],
}
TEXT = {"w_geocode", "h_geocode", "createdate"}
# table -> (the files read, the geocode the rows are keyed and sorted by)
TABLES = {"wac": (["wac_S000"], "w_geocode"), "rac": (["rac_S000"], "h_geocode"),
          "od": (["od_main", "od_aux"], "w_geocode")}
# Plain Parquet, no geometry: rows per row group, for groups of 1 to 3 MB
# (Michigan: 524,288 OD rows are about 2.8 MB, 131,072 RAC rows about 1.8).
GROUP_ROWS = {"wac": 131_072, "rac": 131_072, "od": 524_288}

# Rollups: name -> (table, geocode prefix length, TIGER/Line 2020 layer).
ROLLUPS = {"wac_tract": ("wac", 11, "tract"), "rac_tract": ("rac", 11, "tract"),
           "wac_county": ("wac", 5, "county"), "rac_county": ("rac", 5, "county")}
# Tract tiles start where census.py's TIGER tract tiles start (z7, the
# lowest at which no tile of the outlines goes over the cap), with the
# properties the default style needs; later zooms if a tile goes over.
TILES = {"wac_tract": "lodes_wac_tract", "rac_tract": "lodes_rac_tract"}
TILE_PROPS = ["GEOID", "C000", "ALAND", "year"]
TILE_ZOOMS = (7, 8, 9)
MAX_ZOOM = 12


def file_name(st: str, part: str, year: int) -> str:
    """ri, wac_S000, 2024 -> ri_wac_S000_JT00_2024.csv.gz"""
    return f"{st.lower()}_{part}_JT00_{year}.csv.gz"


def file_url(st: str, part: str, year: int) -> str:
    return f"{BASE}/{st.lower()}/{part.split('_')[0]}/{file_name(st, part, year)}"


def get(url: str, method: str = "GET"):
    req = urllib.request.Request(url, headers=UA, method=method)
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.headers, (r.read() if method == "GET" else b"")
        except OSError as e:
            if attempt == 3:
                raise
            print(f"  {url}: {e}, retrying", flush=True)
            time.sleep(10 * attempt)


def years(st: str) -> set[int]:
    """The years a state has all four JT00 files for, from the listings."""
    have = None
    for part in ("wac_S000", "rac_S000", "od_main", "od_aux"):
        _, html = get(f"{BASE}/{st.lower()}/{part.split('_')[0]}/")
        found = {int(y) for y in re.findall(
            rf'href="{st.lower()}_{part}_JT00_(\d{{4}})\.csv\.gz"', html.decode())}
        have = found if have is None else have & found
    return have


def state_source(st: str) -> dict:
    """A state's year, data vintage and files (Last-Modified, size)."""
    have = years(st)
    if int(YEAR) in have:
        year = int(YEAR)
    elif st in FALLBACK and have:
        year = max(have)
    else:
        sys.exit(f"{st}: no {YEAR} OD, RAC and WAC in {VERSION} (latest {max(have, default=None)});"
                 f" only {sorted(FALLBACK)} may fall back")
    _, version = get(f"{BASE}/{st.lower()}/version.txt")
    vintage = re.search(r"Data Vintage:\s*(\S+)", version.decode())
    files = {}
    for part in ("wac_S000", "rac_S000", "od_main", "od_aux"):
        url = file_url(st, part, year)
        headers, _ = get(url, "HEAD")
        files[part] = {"url": url, "last_modified": headers["Last-Modified"],
                       "bytes": int(headers["Content-Length"])}
    return {"year": year, "vintage": vintage[1] if vintage else None, "files": files}


def upstream() -> dict:
    """Every state's year, vintage and files, and a fingerprint of them."""
    with ThreadPoolExecutor(8) as ex:
        states = dict(zip((st for st, _ in TARGETS.values()),
                          ex.map(state_source, (st for st, _ in TARGETS.values()))))
    blob = json.dumps(states, sort_keys=True).encode()
    return {"fingerprint": hashlib.sha256(blob).hexdigest()[:16], "version": VERSION,
            "year": int(YEAR),
            "fallback": {st: s["year"] for st, s in states.items() if s["year"] != int(YEAR)},
            "states": states}


def fetch(entry: dict, src: Path) -> Path:
    """The gzipped CSV, its size checked against the server's and its
    first bytes against gzip's magic: census.gov can answer a refused
    request with an HTML page and HTTP 200."""
    path = src / entry["url"].rsplit("/", 1)[1]
    if not (path.is_file() and path.stat().st_size == entry["bytes"]):
        src.mkdir(parents=True, exist_ok=True)
        part = path.with_suffix(".part")
        subprocess.run(["curl", "-fsSLR", "--retry", "3", "-A", UA["User-Agent"],
                        "-o", str(part), entry["url"]], check=True)
        part.rename(path)
    if path.stat().st_size != entry["bytes"]:
        sys.exit(f"{entry['url']}: got {path.stat().st_size} bytes, the server lists "
                 f"{entry['bytes']}")
    with path.open("rb") as f:
        if f.read(2) != b"\x1f\x8b":
            sys.exit(f"{entry['url']}: not a gzip file")
    return path


def read_csv(path: Path, table: str) -> str:
    """The file as a DuckDB relation, its header checked: geocodes and
    createdate as text, the counts as integers."""
    with gzip.open(path, "rt") as f:
        header = f.readline().strip().split(",")
    if header != COLUMNS[table]:
        sys.exit(f"{path.name}: columns {header}, expected {COLUMNS[table]}")
    cols = ", ".join(f"'{c}': '{'VARCHAR' if c in TEXT else 'INTEGER'}'" for c in header)
    return f"read_csv('{path}', header = true, columns = {{{cols}}}, compression = 'gzip')"


def connect(work: Path, memory: str, threads: int | None) -> duckdb.DuckDBPyConnection:
    (work / "spill").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; SET temp_directory = '{work / 'spill'}'; "
                f"SET memory_limit = '{memory}'; SET preserve_insertion_order = false")
    if threads:
        con.execute(f"SET threads = {threads}")
    return con


def write_table(con, table: str, order: str, dest: Path) -> dict:
    con.execute(f"COPY (SELECT * FROM t ORDER BY {order}) TO '{dest}' "
                f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {GROUP_ROWS[table]})")
    n, groups, largest = con.execute(f"""
        SELECT (SELECT count(*) FROM read_parquet('{dest}')), count(*), max(b)
        FROM (SELECT row_group_id, sum(total_compressed_size) AS b
              FROM parquet_metadata('{dest}') GROUP BY 1)""").fetchone()
    return {"file": dest.name, "rows": n, "bytes": dest.stat().st_size, "row_groups": groups,
            "largest_row_group_bytes": largest, "sha256": sha256(dest)}


def gate_geocodes(con, table: str, fips: str) -> list[str]:
    """Geocodes are 15 digits in the state; OD aux residences outside it."""
    checks = {"wac": [("w_geocode", True)], "rac": [("h_geocode", True)],
              "od": [("w_geocode", True)]}[table]
    problems = []
    for col, inside in checks:
        bad = con.execute(f"""SELECT count(*) FROM t WHERE NOT regexp_full_match({col}, '[0-9]{{15}}')
                              OR left({col}, 2) {'<>' if inside else '='} '{fips}'""").fetchone()[0]
        if bad:
            problems.append(f"{table}: {bad} {col} not a 15-digit block of the state")
    if table == "od":
        bad = con.execute(f"""SELECT count(*) FROM t WHERE NOT regexp_full_match(h_geocode, '[0-9]{{15}}')
                              OR (left(h_geocode, 2) = '{fips}') = interstate""").fetchone()[0]
        if bad:
            problems.append(f"od: {bad} h_geocode not 15 digits, or in main outside the state "
                            "or in aux inside it")
    return problems


def build_state(con, fips: str, st: str, source: dict, work: Path, rel: Path) -> dict:
    """One state: its three files, gated, and its rollups in work/roll/.
    A state a previous run finished (its entry in work/entries/) is kept."""
    entry_path = work / "entries" / f"{st}.json"
    folder = rel / f"state={st}"
    if entry_path.is_file() and all((folder / f"{t}.parquet").is_file() for t in TABLES):
        return json.loads(entry_path.read_text())
    year = source["year"]
    folder.mkdir(parents=True, exist_ok=True)
    entry = {"state": st, "fips": fips, "year": year, "vintage": source["vintage"]}
    problems, sums = [], {}
    for table, (parts, key) in TABLES.items():
        files = [fetch(source["files"][p], work / "src") for p in parts]
        if table == "od":
            relation = (f"SELECT *, false AS interstate FROM {read_csv(files[0], table)} "
                        f"UNION ALL SELECT *, true AS interstate FROM {read_csv(files[1], table)}")
        else:
            relation = f"SELECT * FROM {read_csv(files[0], table)}"
        con.execute(f"CREATE OR REPLACE TEMP TABLE t AS "
                    f"SELECT *, {year}::SMALLINT AS year FROM ({relation})")
        n = con.execute("SELECT count(*) FROM t").fetchone()[0]
        problems += gate_geocodes(con, table, fips)
        keys = "w_geocode, h_geocode" if table == "od" else key
        repeated = con.execute(f"SELECT count(*) - count(DISTINCT ({keys})) FROM t").fetchone()[0]
        if repeated:
            problems.append(f"{table}: {repeated} repeated {keys}")
        total = "S000" if table == "od" else "C000"
        sums[table] = con.execute(f"SELECT coalesce(sum({total}), 0) FROM t").fetchone()[0]
        e = write_table(con, table, keys, folder / f"{table}.parquet")
        if e["rows"] != n:
            problems.append(f"{table}: {n:,} rows read, {e['rows']:,} written")
        if table == "od":
            e["interstate_rows"] = con.execute(
                "SELECT count(*) FROM t WHERE interstate").fetchone()[0]
            e["interstate_jobs"] = con.execute(
                "SELECT coalesce(sum(S000), 0) FROM t WHERE interstate").fetchone()[0]
        else:
            rollup(con, table, st, work)
        entry[table] = e
        con.execute("DROP TABLE t")
    if sums["wac"] != sums["od"]:
        problems.append(f"WAC C000 sums to {sums['wac']:,}, OD S000 to {sums['od']:,}")
    if problems:
        sys.exit(f"gate, {st}: " + "; ".join(problems))
    entry["jobs"] = int(sums["wac"])
    entry["jobs_by_residence"] = int(sums["rac"])
    for p in source["files"].values():
        (work / "src" / p["url"].rsplit("/", 1)[1]).unlink(missing_ok=True)
    entry_path.parent.mkdir(parents=True, exist_ok=True)
    entry_path.write_text(json.dumps(entry, indent=2) + "\n")
    return entry


def rollup(con, table: str, st: str, work: Path) -> None:
    """The temp table t (a state's WAC or RAC) summed per tract and county,
    each in work/roll/<name>/<XX>.parquet."""
    key = TABLES[table][1]
    # The firm columns are zero in JT00: not summed.
    sums = ", ".join(f"sum({c})::BIGINT AS {c}" for c in ["C000", *SEGMENTS])
    for name, (tbl, n, _) in ROLLUPS.items():
        if tbl != table:
            continue
        dest = work / "roll" / name / f"{st}.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""COPY (SELECT left({key}, {n}) AS GEOID, '{st}' AS state,
                                     any_value(year) AS year, count(*) AS blocks, {sums}
                              FROM t GROUP BY 1)
                        TO '{dest}' (FORMAT PARQUET)""")


def gate_residents(con, rel: Path, files: list[dict]) -> dict:
    """RAC against OD across states, for the states of YEAR: a state's
    residents' jobs in its RAC are its OD main rows plus the other states'
    OD aux rows with a residence in it."""
    same = [f for f in files if f["year"] == int(YEAR)]
    ods = ", ".join(f"'{rel}/state={f['state']}/od.parquet'" for f in same)
    by_home = dict(con.execute(f"""
        SELECT left(h_geocode, 2), sum(S000) FROM read_parquet([{ods}], hive_partitioning = false)
        GROUP BY 1""").fetchall())
    off = {f["state"]: (f["jobs_by_residence"], int(by_home.get(f["fips"], 0)))
           for f in same if f["jobs_by_residence"] != by_home.get(f["fips"], 0)}
    if off:
        sys.exit("gate, residents: RAC C000 and OD S000 by residence differ: "
                 + "; ".join(f"{st} {r:,} vs {o:,}" for st, (r, o) in sorted(off.items())))
    return {"states_compared": len(same),
            "rule": f"RAC C000 summed = OD S000 summed by residence over every {YEAR} state"}


def tiger_zip(work: Path, url: str) -> Path:
    archive = work / "tiger" / url.rsplit("/", 1)[1]
    archive.parent.mkdir(parents=True, exist_ok=True)
    if not archive.is_file():
        download_zip(url, archive)
    return archive


def outlines(con, work: Path, states: list[tuple[str, str]]) -> None:
    """The TIGER/Line 2020 tracts of the states built and the counties, as
    the temp tables tiger_tract and tiger_county (GEOID, NAMELSAD, ALAND,
    AWATER, NAD83 geometry)."""
    cols = "GEOID, NAMELSAD, ALAND, AWATER, ST_SetCRS(ST_Force2D(geom), '{CRS}') AS geometry"
    parts = []
    for fips, _ in states:
        name = f"tl_2020_{fips}_tract"
        archive = tiger_zip(work, f"{TIGER}/TRACT/{name}.zip")
        parts.append(f"SELECT {cols.format(CRS=CRS)} FROM ST_Read('/vsizip/{archive}/{name}.shp')")
    con.execute(f"CREATE OR REPLACE TEMP TABLE tiger_tract AS {' UNION ALL '.join(parts)}")
    archive = tiger_zip(work, COUNTY_ZIP)
    fips = ", ".join(f"'{f}'" for f, _ in states)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tiger_county AS
        SELECT {cols.format(CRS=CRS)} FROM ST_Read('/vsizip/{archive}/tl_2020_us_county.shp')
        WHERE STATEFP IN ({fips})""")


def write_rollup(con, name: str, work: Path, rel: Path, crs: dict) -> dict:
    """A rollup of every state built, joined to its 2020 outline. Every
    tract or county must have one."""
    _, _, layer = ROLLUPS[name]
    rolled = f"read_parquet('{work / 'roll' / name}/[A-Z][A-Z].parquet')"
    missing = con.execute(f"SELECT count(*), list(GEOID)[:5] FROM {rolled} "
                          f"ANTI JOIN tiger_{layer} USING (GEOID)").fetchone()
    if missing[0]:
        sys.exit(f"gate, {name}: {missing[0]} areas not in TIGER/Line 2020 {layer}s, "
                 f"e.g. {missing[1]}")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE src AS
        SELECT *, {BBOX} FROM (
            SELECT r.GEOID, g.NAMELSAD, r.* EXCLUDE (GEOID), g.ALAND, g.AWATER, g.geometry
            FROM {rolled} r JOIN tiger_{layer} g USING (GEOID))""")
    without = con.execute(f"SELECT count(*) FROM tiger_{layer} ANTI JOIN src USING (GEOID)"
                          ).fetchone()[0]
    dest = rel / f"{name}.parquet"
    e = write_src(con, dest, crs)
    verify(con, rel, [name])
    check_crs(con, dest)
    con.execute("DROP TABLE src")
    return {"name": name, **e, "areas_without_jobs": without}


def tile(con, rel: Path, name: str) -> dict:
    """The tract rollup's PMTiles, from the first zoom of TILE_ZOOMS where
    no tile goes over the cap (pmtiles.tile() fails on one). tylertoo reads
    lon/lat: it gets a copy relabelled CRS84 (NAD83 is within 2 m of WGS84 in
    the conterminous US), the GeoParquet stays NAD83."""
    pmtiles.binary()
    lonlat = rel / f"{name}.lonlat.parquet"
    con.execute(f"""COPY (SELECT {', '.join(TILE_PROPS)},
                                 ST_SetCRS(geometry::GEOMETRY, 'OGC:CRS84') AS geometry
                          FROM read_parquet('{rel / name}.parquet'))
                    TO '{lonlat}' (FORMAT PARQUET)""")
    try:
        for min_zoom in TILE_ZOOMS:
            try:
                entry = pmtiles.tile(lonlat, rel / f"{name}.pmtiles", layer=TILES[name],
                                     min_zoom=min_zoom, max_zoom=MAX_ZOOM, properties=TILE_PROPS)
            except SystemExit as e:
                print(f"    tiles {name} from z{min_zoom}: {e}", flush=True)
                continue
            return {"name": name, **entry}
        sys.exit(f"{name}: no first zoom in {TILE_ZOOMS} fits the size cap")
    finally:
        lonlat.unlink(missing_ok=True)


def attribution(source: dict) -> str:
    fallback = ", ".join(f"{st} {y}" for st, y in sorted(source["fallback"].items())) or "none"
    return f"""\
LEHD Origin-Destination Employment Statistics (LODES), {VERSION}, {YEAR}
U.S. Census Bureau, LEHD Origin-Destination Employment Statistics ({VERSION}).
Republished as Parquet by Geomermaids: https://parquetry.geomermaids.com/census/{PREFIX}/

Source: {BASE}/<st>/{{wac,rac,od}}/ (All Jobs, JT00)
Technical document: {TECHDOC}
About LEHD: {SOURCE_PAGE}
Outlines: TIGER/Line 2020 tracts and counties, {TIGER}/

Licence: a work of the US federal government, not subject to copyright in the
United States (17 U.S.C. 105). Credit "U.S. Census Bureau, LEHD
Origin-Destination Employment Statistics ({VERSION})". LODES applies noise
infusion and synthetic data methods to protect confidentiality: small counts
are not exact.

Data years: {YEAR} for every state but those LEHD has no OD and WAC for:
{fallback}. Their three tables are all from that year, and `year` says it in
every row. Puerto Rico is not in LODES.

Changes made here: columns and values unchanged; added `year` and, in
od.parquet, `interstate` (true for the aux file's rows). The OD main and aux
parts are one file. Rows sorted by geocode. The rollups sum the blocks per
2020 tract and county (without the firm age and size columns, zero in JT00)
and join the TIGER/Line 2020 outline, NAD83.
"""


def build(work: Path, out: Path, only: list[str] | None, memory: str, threads: int | None,
          no_tiles: bool) -> None:
    t0 = time.monotonic()
    rel = out / PREFIX
    rel.mkdir(parents=True, exist_ok=True)
    src = upstream()
    todo = [(fips, st) for fips, (st, _) in TARGETS.items() if not only or st in only]
    unknown = sorted(set(only or []) - {st for _, st in todo})
    if unknown:
        sys.exit(f"--state: not one of the 50 states or DC: {unknown}")
    con = connect(work, memory, threads)
    crs = projjson(con)
    files = []
    for i, (fips, st) in enumerate(todo, 1):
        e = build_state(con, fips, st, src["states"][st], work, rel)
        files.append(e)
        print(f"  [{i}/{len(todo)}] {st} {e['year']}: {e['jobs']:,} jobs, wac {e['wac']['rows']:,}"
              f" rows, rac {e['rac']['rows']:,}, od {e['od']['rows']:,} "
              f"({e['od']['bytes'] / 1e6:,.1f} MB), {time.monotonic() - t0:.0f} s", flush=True)
    outlines(con, work, todo)
    rollups = []
    for name in ROLLUPS:
        r = write_rollup(con, name, work, rel, crs)
        rollups.append(r)
        print(f"  {name}: {r['features']:,} areas ({r['areas_without_jobs']:,} 2020 areas "
              f"without jobs), {r['bytes'] / 1e6:,.1f} MB, {time.monotonic() - t0:.0f} s",
              flush=True)
    residents = gate_residents(con, rel, files) if len(files) == len(TARGETS) else None
    tiles = [] if no_tiles else [tile(con, rel, name) for name in TILES]
    con.close()
    complete = len(files) == len(TARGETS)
    manifest = {
        "dataset": "lodes", "version": VERSION, "year": int(YEAR), "crs": CRS,
        "job_type": "JT00", "complete": complete,
        "fallback": {f["state"]: f["year"] for f in files if f["year"] != int(YEAR)},
        "jobs": sum(f["jobs"] for f in files),
        "jobs_by_residence": sum(f["jobs_by_residence"] for f in files),
        "rows": {t: sum(f[t]["rows"] for f in files) for t in TABLES},
        "gate": {
            "states": len(files),
            "residents": residents,
            "rule": "per state: every row read written, geocodes 15 digits in the state (OD aux "
                    "residences outside it), no repeated key, WAC C000 summed equal to OD S000 "
                    "summed (main and aux); every rolled-up tract and county a TIGER/Line 2020 one",
        },
        "files": sorted(files, key=lambda f: f["state"]),
        "rollups": rollups, "pmtiles": tiles,
        "globs": {t: GLOB.format(table=t) for t in TABLES},
        "built": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    (rel / "_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (rel / "_source.json").write_text(json.dumps(src, indent=2) + "\n")
    (rel / "ATTRIBUTION.txt").write_text(attribution(src))
    shutil.rmtree(work / "spill", ignore_errors=True)
    print(f"  {len(files)} states, {manifest['jobs']:,} jobs by workplace, "
          f"{manifest['jobs_by_residence']:,} by residence, fallback {manifest['fallback']}, "
          f"{time.monotonic() - t0:.0f} s", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="each state's year, vintage and files, a fingerprint")
    b = sub.add_parser("build", help="fetch, gate, write, roll up and tile")
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
