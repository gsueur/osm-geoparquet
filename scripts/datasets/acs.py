#!/usr/bin/env python3
"""
US Census Bureau American Community Survey 5-year estimates, 2020-2024, as
curated tables keyed like the TIGER/Line layers census.py publishes.

The Census Bureau publishes the ACS as a table-based Summary File
(https://www.census.gov/programs-surveys/acs/data/summary-file.html): one
pipe-delimited file per detailed table, every geography in it, plus a
geography file (Geos20245YR.txt) and the table shells (labels). No API key.
This reads 16 tables and writes, per geography:

  <out>/acs/2024/<geo>.parquet       the measures, one row per area, no geometry
  <out>/acs/2024/<geo>_geo.parquet   the same joined to the TIGER/Line 2026 outline
  <out>/acs/2024/_manifest.json      rows, sizes, sha256, column docs, gates, source
  <out>/acs/2024/ATTRIBUTION.txt
  <out>/acs/2024/_source.json        each source file's Last-Modified, a fingerprint

<out> is the census/ prefix. <geo> is state, county, place, tract, bg or
zcta520 (the TIGER codes). `GEOID` is TIGER's identifier (Geos' TL_GEO_ID),
so the tables join the census/tiger/2026/ layers as they are.

Each measure has its margin of error at 90 percent in `<name>_moe`, as
published. Rates are derived here, in percent, with their MOE from the ACS
handbook's formulas (a proportion, falling back to the ratio formula when
the radicand is negative; a sum's MOE is the root of the summed squares).
The Summary File's annotation codes are not numbers: an estimate of
-666666666 (not computable, too few sample cases) is NULL; an MOE of
-555555555 (the estimate is controlled, no sampling error) is 0; an MOE of
-333333333 (median in an open-ended interval) or -222222222 (not
computable) is NULL. Medians at the edge of their distribution are top- or
bottom-coded as published (income 250,001 for "250,000 or more" and 2,499
for "2,500 or less", home value 2,000,001 and 9,999, gross rent 3,501 and
99) and documented per column.

Block groups have no B17001 and no B08013: poverty comes from B17021
(the same universe, published down to the block group), and the mean
travel time to work is NULL for block groups.

Gates, before anything is written: every area of a geography is a TIGER
2026 area (places excepted: places change every year, the unmatched ones
are counted and left out of place_geo), and the populations add up block
group to tract to county to state, exactly (the ACS controls them), but
for the source gaps listed in KNOWN_GAPS.

Usage:
  python3 scripts/datasets/acs.py upstream
  python3 scripts/datasets/acs.py build --work-dir work --out-dir out/census
  python3 scripts/datasets/acs.py build ... --tiger out/census/tiger/2026   # local TIGER files
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.census import BBOX, YEAR as TIGER_YEAR, check_crs, projjson, sha256, write_src
from datasets.gaul import verify

YEAR = "2024"
SPAN = "2020-2024"
SF = f"https://www2.census.gov/programs-surveys/acs/summary_file/{YEAR}/table-based-SF"
DATA = f"{SF}/data/5YRData"
GEOS_URL = f"{SF}/documentation/Geos{YEAR}5YR.txt"
SHELLS_URL = f"{SF}/documentation/ACS{YEAR}5YR_Table_Shells.txt"
SOURCE_PAGE = "https://www.census.gov/programs-surveys/acs/data/summary-file.html"
HANDBOOK = ("https://www.census.gov/programs-surveys/acs/library/handbooks/general.html")
PUBLIC_TIGER = f"https://parquetry.geomermaids.com/census/tiger/{TIGER_YEAR}"
CRS = "EPSG:4269"

# geo -> (summary level, title, TIGER product/code, TIGER's identifier and
# area columns). The TIGER file names are the codes census.py publishes.
GEOS = {
    "state": ("040", "States", "substategeo/state", "GEOID", "ALAND", "AWATER"),
    "county": ("050", "Counties", "substategeo/county", "GEOID", "ALAND", "AWATER"),
    "place": ("160", "Places", "substategeo/place", "GEOID", "ALAND", "AWATER"),
    "tract": ("140", "Census tracts", "substategeo/tract", "GEOID", "ALAND", "AWATER"),
    "bg": ("150", "Block groups", "substategeo/bg", "GEOID", "ALAND", "AWATER"),
    "zcta520": ("860", "ZIP Code Tabulation Areas", "nationgeo/zcta520", "GEOID20", "ALAND20",
                "AWATER20"),
}
# The island areas TIGER covers and the ACS does not (it has Puerto Rico).
ISLANDS = ("60", "66", "69", "78")
# Known gaps in the published Summary File, by parent GEOID: the population
# gate leaves these parents out, and fails on any other. Suffolk County, NY:
# 14 tracts (36103122406, 36103122501, 3610314560[1-5], 36103145702,
# 36103146001, 36103146105, 36103146106, 36103146201, 36103146204,
# 36103201200) and 39 of their block groups are in neither Geos20245YR.txt
# nor the tables (Last-Modified 2026-01-29), though the county total counts
# their 77,579 people; tract 36103146001 is missing while one of its block
# groups is not.
KNOWN_GAPS = {
    "36103": "Suffolk County, NY: 14 tracts missing from the Summary File",
    "36103146001": "Suffolk County, NY: tract missing from the Summary File, one block group present",
}

TOP = "Top- and bottom-coded as published"
# The published columns: (name, kind, source, description). kind est: one
# ACS variable; sum: several, summed; pct: 100 * a / b of two columns above;
# ratio: a / b of two ACS variables.
MEASURES = [
    ("population", "est", "B01003_001", "Total population."),
    ("median_age", "est", "B01002_001", "Median age, years."),
    ("households", "est", "B11001_001", "Households."),
    ("median_household_income", "est", "B19013_001",
     f"Median household income in the past 12 months, in 2024 dollars. {TOP}: 250001 for "
     "250,000 or more, 2499 for 2,500 or less."),
    ("per_capita_income", "est", "B19301_001",
     "Per capita income in the past 12 months, in 2024 dollars."),
    ("poverty_universe", "est", "B17021_001",
     "Population for whom poverty status is determined."),
    ("below_poverty", "est", "B17021_002",
     "Of them, with an income in the past 12 months below the poverty level."),
    ("poverty_rate", "pct", ("below_poverty", "poverty_universe"),
     "Percent below the poverty level: below_poverty / poverty_universe."),
    ("median_home_value", "est", "B25077_001",
     f"Median value of owner-occupied housing units, dollars. {TOP}: 2000001 for 2,000,000 "
     "or more, 9999 for 10,000 or less."),
    ("median_gross_rent", "est", "B25064_001",
     f"Median gross rent of renter-occupied units paying cash rent, dollars a month. {TOP}: "
     "3501 for 3,500 or more, 99 for 100 or less."),
    ("housing_units", "est", "B25002_001", "Housing units."),
    ("vacant_units", "est", "B25002_003", "Vacant housing units."),
    ("vacancy_rate", "pct", ("vacant_units", "housing_units"),
     "Percent of housing units vacant: vacant_units / housing_units."),
    ("occupied_units", "est", "B25003_001", "Occupied housing units."),
    ("owner_occupied", "est", "B25003_002", "Owner-occupied housing units."),
    ("renter_occupied", "est", "B25003_003", "Renter-occupied housing units."),
    ("owner_occupied_rate", "pct", ("owner_occupied", "occupied_units"),
     "Percent of occupied units that are owner-occupied: owner_occupied / occupied_units."),
    ("hispanic_or_latino", "est", "B03002_012", "Hispanic or Latino, of any race."),
    ("white_not_hispanic", "est", "B03002_003", "White alone, not Hispanic or Latino."),
    ("black_not_hispanic", "est", "B03002_004",
     "Black or African American alone, not Hispanic or Latino."),
    ("aian_not_hispanic", "est", "B03002_005",
     "American Indian and Alaska Native alone, not Hispanic or Latino."),
    ("asian_not_hispanic", "est", "B03002_006", "Asian alone, not Hispanic or Latino."),
    ("nhpi_not_hispanic", "est", "B03002_007",
     "Native Hawaiian and Other Pacific Islander alone, not Hispanic or Latino."),
    ("other_race_not_hispanic", "est", "B03002_008",
     "Some other race alone, not Hispanic or Latino."),
    ("two_or_more_races_not_hispanic", "est", "B03002_009",
     "Two or more races, not Hispanic or Latino."),
    ("population_25_and_over", "est", "B15003_001", "Population 25 years and over."),
    ("bachelors_or_higher", "sum", ("B15003_022", "B15003_023", "B15003_024", "B15003_025"),
     "Of them, with a bachelor's, master's, professional school or doctorate degree."),
    ("bachelors_or_higher_rate", "pct", ("bachelors_or_higher", "population_25_and_over"),
     "Percent of the population 25 and over with a bachelor's degree or higher."),
    ("workers", "est", "B08301_001", "Workers 16 years and over."),
    ("transit_commuters", "est", "B08301_010",
     "Of them, commuting by public transportation (excluding taxicab)."),
    ("transit_share", "pct", ("transit_commuters", "workers"),
     "Percent of workers commuting by public transportation: transit_commuters / workers."),
    ("commuters_outside_home", "est", "B08303_001",
     "Workers 16 years and over who did not work from home."),
    ("mean_travel_time", "ratio", ("B08013_001", "B08303_001"),
     "Mean travel time to work, minutes, of workers who did not work from home: aggregate "
     "travel time (B08013_001) / commuters_outside_home. NULL for block groups (B08013 is "
     "not published for them)."),
]
# Median age and the rates are fractional; the rest are whole numbers.
FRACTIONAL = {"median_age"}


def variables() -> list[str]:
    """Every ACS variable read, e.g. B01003_001."""
    out = []
    for _, kind, src, _ in MEASURES:
        if kind == "est":
            out.append(src)
        elif kind in ("sum", "ratio"):
            out.extend(src)
    return list(dict.fromkeys(out))


def tables() -> list[str]:
    return sorted({v.split("_")[0] for v in variables()})


def table_url(table: str) -> str:
    return f"{DATA}/acsdt5y{YEAR}-{table.lower()}.dat"


def sources() -> dict[str, str]:
    """Every file the build reads, by a short name."""
    return {"geos": GEOS_URL, "shells": SHELLS_URL, **{t: table_url(t) for t in tables()}}


def upstream() -> dict:
    """Each source's Last-Modified and size, and a fingerprint of them all:
    a correction to any one file moves it."""
    files = {}
    for name, url in sources().items():
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=60) as r:
            files[name] = {"url": url, "last_modified": r.headers["Last-Modified"],
                           "bytes": int(r.headers["Content-Length"])}
    stamp = "".join(f"{k}{v['last_modified']}{v['bytes']}" for k, v in sorted(files.items()))
    return {"fingerprint": hashlib.sha256(stamp.encode()).hexdigest()[:16],
            "last_modified": max(files.values(), key=lambda f: time.mktime(
                time.strptime(f["last_modified"], "%a, %d %b %Y %H:%M:%S GMT")))["last_modified"],
            "files": files}


def fetch(name: str, url: str, size: int, src: Path) -> Path:
    """The file, kept in src: a copy of the same size is reused. census.gov
    can answer a rejected request with an HTML page and HTTP 200, so the
    first line is checked too."""
    path = src / url.rsplit("/", 1)[1]
    if not (path.is_file() and path.stat().st_size == size):
        src.mkdir(parents=True, exist_ok=True)
        part = path.with_suffix(".part")
        subprocess.run(["curl", "-fsSL", "--retry", "3", "-o", str(part), url], check=True)
        part.rename(path)
    if path.stat().st_size != size:
        sys.exit(f"{url}: got {path.stat().st_size} bytes, the server lists {size}")
    with path.open("rb") as f:
        head = f.readline()
    want = {"geos": b"FILEID|", "shells": b"Table ID|"}.get(name, b"GEO_ID|")
    if not head.startswith(want):
        sys.exit(f"{url}: not the expected file, starts {head[:80]!r}")
    return path


def labels(shells: Path) -> dict[str, str]:
    """ACS variable -> 'Table title: line label', from the table shells."""
    out = {}
    with shells.open(encoding="utf-8", errors="replace") as f:
        next(f)
        for line in f:
            row = line.rstrip("\n").split("|")
            if len(row) >= 6 and row[3]:
                out[row[3]] = f"{row[5]}: {row[4].rstrip(':')}"
    missing = [v for v in variables() if v not in out]
    if missing:
        sys.exit(f"table shells lack {missing}")
    return out


def column_docs(shells: dict[str, str]) -> list[dict]:
    """name, type and description of every column, with the ACS codes."""
    cols = [
        {"name": "GEOID", "type": "VARCHAR",
         "description": "Area identifier, as TIGER/Line's GEOID (GEOID20 for ZCTAs): join "
                        "census/tiger/2026/ on it."},
        {"name": "NAME", "type": "VARCHAR", "description": "Area name, as the ACS writes it."},
        {"name": "STUSAB", "type": "VARCHAR",
         "description": "State USPS code (US or PR for ZCTAs, which cross states)."},
        {"name": "ACS_GEO_ID", "type": "VARCHAR",
         "description": "The ACS's own identifier (e.g. 1400000US25025010100), as "
                        "data.census.gov uses it."},
    ]
    for name, kind, src, doc in MEASURES:
        if kind == "est":
            codes = f"ACS {src} ({shells[src]})."
        elif kind == "sum":
            codes = "ACS " + " + ".join(src) + f" ({shells[src[0]].split(':')[0]})."
        elif kind == "ratio":
            codes = f"ACS {src[0]} / {src[1]}."
        else:
            codes = "Derived here."
        typ = "DOUBLE" if kind in ("pct", "ratio") or name in FRACTIONAL else "BIGINT"
        cols.append({"name": name, "type": typ, "description": f"{doc} {codes}"})
        cols.append({"name": f"{name}_moe", "type": "DOUBLE" if kind != "est" or typ == "DOUBLE"
                     else "BIGINT",
                     "description": f"Margin of error of {name}, 90 percent"
                                    + (", as published." if kind == "est" else
                                       ", by the ACS handbook's formula.")})
    return cols


def est(col: str) -> str:
    return f"CASE WHEN TRY_CAST({col} AS DOUBLE) >= 0 THEN TRY_CAST({col} AS DOUBLE) END"


def moe(col: str) -> str:
    return (f"CASE WHEN TRY_CAST({col} AS DOUBLE) = -555555555 THEN 0 "
            f"WHEN TRY_CAST({col} AS DOUBLE) >= 0 THEN TRY_CAST({col} AS DOUBLE) END")


def load(con, files: dict[str, Path]) -> None:
    """The table `wide`: one row per area of the six geographies, every
    measure and MOE, from the Geos file and the 16 tables."""
    levels = ", ".join(f"'{g[0]}'" for g in GEOS.values())
    con.execute(f"""CREATE OR REPLACE TABLE geos AS
        SELECT GEO_ID, SUMLEVEL, STUSAB, NAME, TL_GEO_ID
        FROM read_csv('{files['geos']}', delim='|', header=true, all_varchar=true, quote='')
        WHERE COMPONENT = '00' AND SUMLEVEL IN ({levels})""")
    by_table: dict[str, list[str]] = {}
    for v in variables():
        by_table.setdefault(v.split("_")[0], []).append(v)
    for table, vs in by_table.items():
        cols = ", ".join(f"{est(e)} AS \"{v}\", {moe(m)} AS \"{v}_moe\""
                         for v in vs
                         for e, m in [(f"{table}_E{v.split('_')[1]}",
                                       f"{table}_M{v.split('_')[1]}")])
        con.execute(f"""CREATE OR REPLACE TABLE t_{table} AS
            SELECT GEO_ID, {cols}
            FROM read_csv('{files[table]}', delim='|', header=true, all_varchar=true)
            WHERE GEO_ID IN (SELECT GEO_ID FROM geos)""")
    joins = " ".join(f"LEFT JOIN t_{t} USING (GEO_ID)" for t in by_table)
    con.execute(f"CREATE OR REPLACE TABLE raw AS SELECT * FROM geos {joins}")
    # The published columns, in MEASURES order; a pct reads columns made
    # before it, so they are built up one SELECT at a time.
    sel = ["TL_GEO_ID AS GEOID", "NAME", "STUSAB", "GEO_ID AS ACS_GEO_ID", "SUMLEVEL"]
    done: dict[str, tuple[str, str]] = {}
    for name, kind, src, _ in MEASURES:
        if kind == "est":
            e, m = f'"{src}"', f'"{src}_moe"'
        elif kind == "sum":
            e = " + ".join(f'"{v}"' for v in src)
            m = "sqrt(" + " + ".join(f'"{v}_moe" ^ 2' for v in src) + ")"
        elif kind == "ratio":
            a, b = src
            r = f'("{a}" / nullif("{b}", 0))'
            e = r
            m = f'sqrt("{a}_moe" ^ 2 + {r} ^ 2 * "{b}_moe" ^ 2) / nullif("{b}", 0)'
        else:  # pct
            (a, am), (b, bm) = done[src[0]], done[src[1]]
            p = f"(({a}) / nullif({b}, 0))"
            rad = f"(({am}) ^ 2 - {p} ^ 2 * ({bm}) ^ 2)"
            e = f"100 * {p}"
            m = (f"100 * CASE WHEN {rad} >= 0 THEN sqrt({rad}) "
                 f"ELSE sqrt(({am}) ^ 2 + {p} ^ 2 * ({bm}) ^ 2) END / nullif({b}, 0)")
        done[name] = (e, m)
        whole = kind in ("est", "sum") and name not in FRACTIONAL
        sel.append(f"{'round' if whole else ''}({e})::{'BIGINT' if whole else 'DOUBLE'} AS {name}")
        sel.append(f"round({m}{'' if kind == 'est' and whole else ', 2'})"
                   f"::{'BIGINT' if kind == 'est' and whole else 'DOUBLE'} AS {name}_moe")
    con.execute(f"CREATE OR REPLACE TABLE wide AS SELECT {', '.join(sel)} FROM raw")
    # Rates and means to two decimals, the precision the ACS publishes them at.
    rounded = ", ".join(f"round({n}, 2) AS {n}" for n, k, *_ in MEASURES if k in ("pct", "ratio"))
    con.execute(f"CREATE OR REPLACE TABLE wide AS SELECT * REPLACE ({rounded}) FROM wide")


def tiger_file(tiger: str, geo: str) -> str:
    return f"{tiger}/{GEOS[geo][2]}.parquet"


def gate_areas(con, tiger: str) -> dict[str, dict]:
    """ACS areas against TIGER's. Every ACS area must be a TIGER one, but
    for places; TIGER areas not in the ACS are counted (the island areas,
    which the ACS does not cover, are the expected ones)."""
    report, problems = {}, []
    for geo, (level, _, _, gid, _, _) in GEOS.items():
        t = tiger_file(tiger, geo)
        con.execute(f"CREATE OR REPLACE TEMP TABLE tg AS SELECT {gid} AS GEOID "
                    f"FROM read_parquet('{t}')")
        acs_n, only_acs, only_tiger, islands = con.execute(f"""
            WITH a AS (SELECT GEOID FROM wide WHERE SUMLEVEL = '{level}')
            SELECT (SELECT count(*) FROM a),
                   (SELECT count(*) FROM a ANTI JOIN tg USING (GEOID)),
                   (SELECT count(*) FROM tg ANTI JOIN a USING (GEOID)),
                   (SELECT count(*) FROM tg ANTI JOIN a USING (GEOID)
                    WHERE left(GEOID, 2) IN {ISLANDS})""").fetchone()
        if geo == "zcta520":
            # ZCTAs carry no state: the island ones are 969xx (Guam, Northern
            # Marianas), 96799 (American Samoa) and 008xx (Virgin Islands).
            islands = con.execute("""
                SELECT count(*) FROM tg ANTI JOIN (SELECT GEOID FROM wide WHERE SUMLEVEL = '860')
                USING (GEOID)
                WHERE GEOID LIKE '969%' OR GEOID = '96799' OR GEOID LIKE '008%'""").fetchone()[0]
        report[geo] = {"acs": acs_n, "acs_not_in_tiger": only_acs,
                       "tiger_not_in_acs": only_tiger, "of_which_island_areas": islands}
        print(f"  {geo}: {acs_n:,} ACS areas, {only_acs} not in TIGER {TIGER_YEAR}, "
              f"{only_tiger} TIGER areas not in the ACS ({islands} in the island areas)",
              flush=True)
        if only_acs and geo != "place":
            problems.append(f"{geo}: {only_acs} ACS areas not in TIGER {TIGER_YEAR}")
    if problems:
        sys.exit("area gate: " + "; ".join(problems))
    return report


def gate_population(con) -> dict[str, int]:
    """Block groups sum to tracts, tracts to counties, counties to states,
    exactly: the ACS controls total population at these levels. Parents in
    KNOWN_GAPS are reported, not failed."""
    report, problems = {"known_gaps": KNOWN_GAPS}, []
    known = ", ".join(f"'{k}'" for k in KNOWN_GAPS)
    for child, parent, n in (("150", "140", 11), ("140", "050", 5), ("050", "040", 2)):
        off, worst = con.execute(f"""
            WITH c AS (SELECT left(GEOID, {n}) AS GEOID, sum(population) AS pop
                       FROM wide WHERE SUMLEVEL = '{child}' GROUP BY 1),
                 p AS (SELECT GEOID, population AS pop FROM wide WHERE SUMLEVEL = '{parent}')
            SELECT count(*) FILTER (WHERE coalesce(c.pop, 0) <> coalesce(p.pop, 0)),
                   max(abs(coalesce(c.pop, 0) - coalesce(p.pop, 0)))
            FROM c FULL JOIN p USING (GEOID)
            WHERE GEOID NOT IN ({known})""").fetchone()
        key = f"{child}->{parent}"
        report[key] = off
        print(f"  population {key}: {off} parents differ (largest gap {worst or 0}), "
              f"known gaps left out", flush=True)
        if off:
            problems.append(f"{key}: {off} differ, largest {worst}")
    if problems:
        sys.exit("population gate: " + "; ".join(problems))
    return report


def write_table(con, geo: str, dest: Path) -> dict:
    """The measures, no geometry: plain Parquet sorted by GEOID, so a state
    or county prefix reads few row groups."""
    level = GEOS[geo][0]
    con.execute(f"""COPY (SELECT * EXCLUDE (SUMLEVEL) FROM wide WHERE SUMLEVEL = '{level}'
                          ORDER BY GEOID)
                    TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 32768)""")
    n, groups = con.execute(f"""SELECT (SELECT count(*) FROM read_parquet('{dest}')),
        (SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{dest}'))""").fetchone()
    return {"file": dest.name, "features": n, "bytes": dest.stat().st_size,
            "row_groups": groups, "sha256": sha256(dest)}


def write_geo(con, geo: str, tiger: str, dest: Path, crs: dict) -> dict:
    """The measures joined to the TIGER outline, as census.py writes its
    layers (census.write_src): Hilbert order, bbox covering, row groups of at
    most 4 MiB of geometry."""
    level, _, _, gid, aland, awater = GEOS[geo]
    con.execute(f"""CREATE OR REPLACE TEMP TABLE src AS
        SELECT *, {BBOX} FROM (
            SELECT w.* EXCLUDE (SUMLEVEL), t.{aland} AS ALAND, t.{awater} AS AWATER,
                   ST_SetCRS(t.geometry::GEOMETRY, '{CRS}') AS geometry
            FROM wide w JOIN read_parquet('{tiger_file(tiger, geo)}') t ON w.GEOID = t.{gid}
            WHERE w.SUMLEVEL = '{level}')""")
    entry = write_src(con, dest, crs)
    verify(con, dest.parent, [dest.stem])
    check_crs(con, dest)
    return entry


def attribution(source: dict) -> str:
    return f"""\
# Data attribution

This data is from the U.S. Census Bureau's American Community Survey (ACS)
5-year estimates, {SPAN}, read from the table-based Summary File.

  Source:     {DATA}/
              (latest Last-Modified {source['last_modified']})
  Geography:  {GEOS_URL}
  Labels:     {SHELLS_URL}
  Page:       {SOURCE_PAGE}
  Outlines:   TIGER/Line {TIGER_YEAR}, as published under census/tiger/{TIGER_YEAR}/
  Producer:   U.S. Census Bureau, American Community Survey Office
  Status:     a work of the United States Government, not subject to copyright
              in the United States (17 U.S.C. 105)

Credit "U.S. Census Bureau, American Community Survey {SPAN} 5-year estimates"
when you use it. The Census Bureau asks that derived figures not be presented
as its own.

## What was changed here

Sixteen detailed tables ({', '.join(tables())}) reduced to {len(MEASURES)}
curated measures per area, for states, counties, places, census tracts, block
groups and ZCTAs. Each estimate keeps its published 90 percent margin of
error; rates and the mean travel time are derived here with the ACS handbook's
formulas ({HANDBOOK}). The Summary File's annotation codes are not kept as
numbers: an estimate of -666666666 is NULL, an MOE of -555555555 (controlled
estimate) is 0, an MOE of -333333333 or -222222222 is NULL. Medians keep their
top and bottom codes, documented per column.

<geo>.parquet holds the measures alone, keyed by GEOID as TIGER/Line writes
it. <geo>_geo.parquet joins them to the TIGER/Line {TIGER_YEAR} outline (NAD83,
EPSG:4269), with a bbox covering and rows along a Hilbert curve. The
estimates' sampling error is real: compare areas with their MOEs in mind.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def build(work: Path, out: Path, tiger: str) -> None:
    src = work / "src"
    source = upstream()
    files = {name: fetch(name, f["url"], f["bytes"], src) for name, f in source["files"].items()}
    shells = labels(files["shells"])
    work.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs; "
                f"SET temp_directory = '{work}'")
    crs = projjson(con)
    t0 = time.monotonic()
    load(con, files)
    print(f"  {con.execute('SELECT count(*) FROM wide').fetchone()[0]:,} areas loaded, "
          f"{time.monotonic() - t0:.0f} s", flush=True)
    gates = {"areas": gate_areas(con, tiger), "population": gate_population(con)}
    folder = out / "acs" / YEAR
    folder.mkdir(parents=True, exist_ok=True)
    files_out, themes, labels_out = [], {}, {}
    for geo, (_, title, *_rest) in GEOS.items():
        table = write_table(con, geo, folder / f"{geo}.parquet")
        geo_entry = write_geo(con, geo, tiger, folder / f"{geo}_geo.parquet", crs)
        files_out.append({"name": geo, "geometry": False, **table})
        files_out.append({"name": f"{geo}_geo", "geometry": True, **geo_entry,
                          "without_outline": table["features"] - geo_entry["features"]})
        themes[geo], themes[f"{geo}_geo"] = table["features"], geo_entry["features"]
        labels_out[geo] = labels_out[f"{geo}_geo"] = f"{title}, ACS {SPAN}"
        print(f"  {geo}: {table['features']:,} rows, {table['bytes'] / 1e6:.1f} MB; with "
              f"outlines {geo_entry['features']:,} rows, {geo_entry['bytes'] / 1e6:.1f} MB, "
              f"{geo_entry['row_groups']} row groups, {time.monotonic() - t0:.0f} s", flush=True)
    (folder / "ATTRIBUTION.txt").write_text(attribution(source))
    (folder / "_source.json").write_text(json.dumps(source, indent=2) + "\n")
    (folder / "_manifest.json").write_text(json.dumps({
        "state_name": f"ACS {SPAN} 5-year estimates",
        "total_features": sum(v for k, v in themes.items() if not k.endswith("_geo")),
        "themes": themes,
        "labels": labels_out,
        "crs": CRS,
        "source": {"url": f"{DATA}/", "last_modified": source["last_modified"],
                   "fingerprint": source["fingerprint"]},
        "tiger": f"census/tiger/{TIGER_YEAR}",
        "columns": column_docs(shells),
        "gates": gates,
        "files": files_out,
    }, indent=2) + "\n")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="print each source's Last-Modified and a fingerprint")
    b = sub.add_parser("build", help="fetch, gate and write the tables")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True,
                   help="the census/ prefix, e.g. out/census")
    b.add_argument("--tiger", default=PUBLIC_TIGER,
                   help=f"the TIGER/Line {TIGER_YEAR} folder, URL or path (default: published)")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
        return
    build(args.work_dir, args.out_dir, args.tiger.rstrip("/"))


if __name__ == "__main__":
    main()
