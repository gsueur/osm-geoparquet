#!/usr/bin/env python3
"""
The US Fish and Wildlife Service's National Wetlands Inventory (NWI) as
GeoParquet: one file of wetland and deepwater polygons per state.

FWS publishes the NWI twice a year (October and May) as one file
geodatabase per state or territory, 53 units in all (50 states, DC, PRVI
for Puerto Rico and the US Virgin Islands, PacTrust for Guam, the Northern
Mariana Islands, American Samoa and the Pacific remote islands), listed on
its download page (DOWNLOAD_PAGE). The bucket that serves them refuses
listings, so the page is the index. This writes:

  <out>/nwi/<release>/state=<XX>/wetlands.parquet   one file per state or
                                       territory, postal code; UM for the
                                       Pacific remote islands (Palmyra,
                                       Kingman), PAD-US's code
  <out>/nwi/<release>/wetlands.pmtiles  the polygons for web maps
  <out>/nwi/<release>/projects.parquet  the mapping projects: where each
                                       part of the inventory comes from, its
                                       imagery year and scale
  <out>/nwi/<release>/codes.parquet     FWS's NWI Code Definitions table,
                                       every ATTRIBUTE code with its
                                       Cowardin names and definitions
  <out>/nwi/<release>/_manifest.json    rows, sizes, sha256, the dedup, the gate
  <out>/nwi/<release>/ATTRIBUTION.txt
  <out>/index.json, <out>/_source.json

<release> is the page's "last updated" month (2026-10).

Columns: FWS's ATTRIBUTE (the Cowardin code), WETLAND_TYPE, ACRES and
NWI_ID, unchanged, then the code's names from FWS's NWI Code Definitions
table (CODES_URL; its column names: SYSTEM_NAME to MODIFIER2_NAME), and
`source_unit`, the download the row was taken from. Shape_Length and
Shape_Area (Albers measures) are left out; ACRES is FWS's area.

Geometry: each unit comes in its own Albers on NAD83 (contiguous US, Alaska,
and Hawaii, Puerto Rico and the Pacific, each with its own centre). GDAL
reads it, cuts curves into segments and reprojects to NAD83 lon/lat with
longitudes in [0, 360], so the Aleutians and American Samoa stay
continuous; DuckDB moves what lies east of 180 by -360 and splits the few
polygons across 180 there (padus.py's UNWRAP). Invalid polygons are
repaired with ST_MakeValid and counted. Each row gets a `bbox` declared as
the GeoParquet covering; rows are sorted on a Hilbert curve and cut into
row groups of at most 4 MiB (census.write_src). The result is EPSG:4269.

Deduplication. A unit's download covers whole map blocks around its state,
so neighbouring downloads repeat each other: 47% of Rhode Island's polygons
are also in Massachusetts' download (2026-10). The copies share NWI_ID but
not always their shape: a polygon that runs past a download's edge is cut
there, so the Housatonic River is 362 acres in Connecticut's download and
706 in Massachusetts'. Every row of every unit is assigned the state that
holds its interior point (ST_PointOnSurface, TIGER/Line 2026 states); a
point in no state (offshore, a remote island) takes the nearest of the
unit's own states within 1 degree, else UM in PacTrust. A row is a home
copy when that state is one of its unit's. Each NWI_ID is then kept once,
national: the home copy first, then the largest ACRES (the least cut), then
the unit name; it is written to the file of the state its point lies in.
An NWI_ID FWS repeats within one unit (Vermont: Lake Champlain and a
0.42-acre piece of lake 52 km away) is kept once by the same rule, the
largest ACRES then the geometry's WKB order, before the dedup; identical
copies are counted, different ones counted and listed in _manifest.json,
and more than 0.01% of a unit's rows (at least two) fails the build
(in_unit_duplicates). The gate checks that every NWI_ID read is kept
exactly once; _manifest.json counts the copies dropped,
the IDs whose kept copy came from a neighbour's download, and the IDs with
more than one home copy. Alaska is built on its own (build-ak below): no
other unit reaches it, so its IDs are deduplicated among its own rows.

The gate also checks each unit's rows against the geodatabase's feature
count, every ATTRIBUTE against the code table (an unknown code is counted
and left undecoded, failing the build above 0.1% of the release's rows,
this build's and the published parts it joins), and that the
state files add up to the IDs kept.

Memory: each unit is staged in a process of its own, streamed (GDAL's
FlatGeobuf to Parquet, the polygons never held in memory), its rows located
on their points alone through the grid pieces of the states (pieces_file).
Minnesota (2.7 million rows) stages in 5.4 GB peak with DuckDB at 4 GB.

Usage, on one machine (a test run, or Alaska):
  python3 scripts/datasets/nwi.py upstream
  python3 scripts/datasets/nwi.py build --work-dir work --out-dir out/wetlands \\
      [--unit RI --unit CT --unit MA] [--no-tiles] [--low-disk]
  # Alaska (an 11.8 GB zip): on a workstation, scripts/datasets/nwi_publish_ak.sh

The same steps one by one, as dataset-wetlands.yml runs them in separate jobs:
  nwi.py plan --groups 6 --upstream-out upstream.json      the units in groups
  nwi.py stage --upstream upstream.json --unit MN ...      per group of units
  nwi.py choose --groups 6                                 reads <unit>.index.parquet
  nwi.py write --upstream upstream.json --out-dir out --target MN ...   per group of states
  nwi.py assemble --upstream upstream.json --out-dir out [--live <prefix URL>]
  nwi.py tiles --out-dir out [--live <prefix URL>]
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles  # noqa: E402
from datasets.census import BBOX, check_crs, projjson, sha256, write_src  # noqa: E402
from datasets.gaul import rewrite, verify  # noqa: E402
from datasets.infra import GEOMETRY_TYPE_NAMES  # noqa: E402
from datasets.padus import UNWRAP, WRAP_SRS  # noqa: E402
from extent import lon_intervals  # noqa: E402

DOWNLOAD_PAGE = ("https://www.fws.gov/program/national-wetlands-inventory/"
                 "download-state-wetlands-data")
BASE = "https://documentst.ecosphere.fws.gov/wetlands/data/State-Downloads"
CODES_URL = "https://documentst.ecosphere.fws.gov/wetlands/downloads/NWI-Code-Definitions.zip"
CODES_CSV = "NWI-Code-Definitions/NWI_Code_Definitions.csv"
CODES_PAGE = "https://www.fws.gov/program/national-wetlands-inventory/classification-codes"
DISCLAIMER = "https://www.fws.gov/page/wetlands-geodatabase-disclaimer"
PUBLIC = "https://parquetry.geomermaids.com"
TIGER_STATES = f"{PUBLIC}/census/tiger/2026/substategeo/state.parquet"
CRS = "EPSG:4269"
# The states a unit covers: its own code, except these.
UNIT_STATES = {"PRVI": ("PR", "VI"), "PacTrust": ("GU", "MP", "AS", "UM")}
# Built on a workstation (nwi_publish_ak.sh): too big for a runner's disk.
WORKSTATION_UNITS = ("AK",)
# A point in no state is given the nearest of its unit's states within this
# many degrees.
NEAREST_DEGREES = 1.0
DECODED = ["SYSTEM_NAME", "SUBSYSTEM_NAME", "CLASS_NAME", "SUBCLASS_NAME", "SPLIT_CLASS_NAME",
           "SPLIT_SUBCLASS_NAME", "WATER_REGIME_NAME", "MODIFIER1_NAME", "MODIFIER2_NAME"]
COLUMNS = ["NWI_ID", "ATTRIBUTE", "WETLAND_TYPE", "ACRES", *DECODED, "source_unit"]
UNKNOWN_CODE_SHARE = 0.001
# An NWI_ID repeated with different rows within one unit: above this share
# of the unit's rows (and more than one), the build fails.
DUPLICATE_SHARE = 0.0001
# Written in each unit's counts; the workflow reuses a unit staged by an
# earlier run only if it has this version and the same zip. Bump it when a
# change alters what stage_one writes for units already staged. (Version 1:
# the first split runs; in_unit_duplicates changes only units with repeats,
# which no staged unit had.)
STAGE_VERSION = 1
PROJECT_COLUMNS = ["PROJECT_NAME", "STATUS", "SUPPMAPINFO", "FGDC_METADATA", "DATA_SOURCE",
                   "IMAGE_YR", "IMAGE_DATE", "IMAGE_SCALE", "ALL_SCALES", "EMULSION",
                   "COMMENTS", "SOURCE_TYPE"]
# The tiles, coloured on WETLAND_TYPE in the catalog's style: (properties,
# first zoom) tried in order, the first that fits the size cap kept. NWI_ID
# (50 characters a polygon) is left out: with it, Massachusetts alone puts
# tiles over the cap at z7-9 (2026-11-07); a map click finds the row in the
# state file by its location.
TILE_PROPS = ["ATTRIBUTE", "WETLAND_TYPE", "ACRES"]
TILE_MAX_ZOOM = 13
TILE_LADDER = [(TILE_PROPS, 6), (TILE_PROPS, 7), (TILE_PROPS, 8),
               (["WETLAND_TYPE"], 8), (["WETLAND_TYPE"], 9)]


# ---------- source ----------

def curl(*args: str) -> str:
    return subprocess.run(["curl", "-fsSL", "--retry", "3", "--max-time", "120", *args],
                          check=True, capture_output=True, text=True).stdout


def head(url: str) -> dict:
    """Size and ETag of a download (S3: the ETag moves with the bytes)."""
    out = subprocess.run(["curl", "-fsSI", "--retry", "3", "--max-time", "60", url],
                         check=True, capture_output=True, text=True).stdout
    h = {}
    for line in out.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            h[k.strip().lower()] = v.strip()
    length = h.get("content-length")
    return {"bytes": int(length) if length else None, "etag": h.get("etag", "").strip('"'),
            "last_modified": h.get("last-modified")}


def release_of(page: str) -> tuple[str, str]:
    """'(last updated October 2026)' -> ('2026-10', 'October 2026')."""
    m = re.search(r"last updated\s+([A-Z][a-z]+)\s+(\d{4})", page)
    if not m:
        sys.exit(f"{DOWNLOAD_PAGE}: no 'last updated <Month> <year>' on the page")
    month = time.strptime(m.group(1), "%B").tm_mon
    return f"{m.group(2)}-{month:02d}", f"{m.group(1)} {m.group(2)}"


def units_of(page: str) -> list[str]:
    units = sorted(set(re.findall(r"State-Downloads/([A-Za-z]+)_geodatabase_wetlands\.zip", page)))
    if len(units) < 50:
        sys.exit(f"{DOWNLOAD_PAGE}: {len(units)} geodatabase downloads listed, expected 53")
    return units


def upstream() -> dict:
    """The release on the download page: its month, each unit's zip (size,
    ETag) and the code table's. Last-Modified is recorded, not compared: FWS
    re-uploads unchanged zips."""
    page = curl("-A", "parquetry-nwi/1", DOWNLOAD_PAGE)
    release, label = release_of(page)
    units = units_of(page)
    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        heads = dict(zip(units, pool.map(lambda u: head(zip_url(u)), units)))
    return {"release": release, "last_updated": label, "page": DOWNLOAD_PAGE,
            "units": heads, "codes": head(CODES_URL)}


def zip_url(unit: str) -> str:
    return f"{BASE}/{unit}_geodatabase_wetlands.zip"


def fetch(url: str, dest: Path, size: int | None) -> Path:
    """A download, resumed with ranges; a copy of the right size is reused."""
    if dest.is_file() and size and dest.stat().st_size == size:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, 6):
        r = subprocess.run(["curl", "-fsSL", "--retry", "5", "--retry-delay", "15", "-C", "-",
                            "-o", str(dest), url], check=False)
        got = dest.stat().st_size if dest.exists() else 0
        if r.returncode == 0 and (size is None or got == size):
            return dest
        print(f"  fetch attempt {attempt}: {got:,} of {size} bytes", flush=True)
        time.sleep(30 * attempt)
    sys.exit(f"{url}: got {dest.stat().st_size if dest.exists() else 0:,} bytes, expected {size}")


def gdb_layers(archive: Path) -> tuple[str, dict[str, int]]:
    """The geodatabase inside the zip and its layers with their counts."""
    with zipfile.ZipFile(archive) as z:
        tops = {n.split("/")[0] for n in z.namelist() if n.split("/")[0].endswith(".gdb")}
    if len(tops) != 1:
        sys.exit(f"{archive.name}: {len(tops)} geodatabases, want 1")
    gdb = f"/vsizip/{archive}/{tops.pop()}"
    info = json.loads(subprocess.run(["ogrinfo", "-ro", "-so", "-json", gdb], check=True,
                                     capture_output=True, text=True).stdout)
    return gdb, {lyr["name"]: lyr.get("featureCount") for lyr in info["layers"]}


def pick(layers: dict[str, int], unit: str) -> tuple[str, str | None]:
    """The wetlands layer and the project metadata layer, by name: the case
    and the wording vary (DC_wetlands, PacTrust_Wetlands_Metadata).
    Riparian and historic-map layers are left out."""
    wet = [n for n in layers if n.lower() == f"{unit.lower()}_wetlands"]
    if len(wet) != 1:
        sys.exit(f"{unit}: no single <unit>_Wetlands layer in {sorted(layers)}")
    meta = [n for n in layers if n.lower().endswith("_metadata")
            and n.lower().startswith(f"{unit.lower()}_wetlands")]
    return wet[0], (meta[0] if len(meta) == 1 else None)


def to_fgb(gdb: str, layer: str, dest: Path, columns: list[str], driver: str = "FlatGeobuf") -> None:
    """One layer through GDAL to FlatGeobuf (any GDAL build writes it, the
    Parquet driver needs Arrow): curves cut into segments, 2D, NAD83 lon/lat
    with longitudes in [0, 360]. Every feature is kept. With driver="GPKG",
    a GeoPackage whose geometry column is `geom`, as ST_Read names it from
    FlatGeobuf: the project layers go that way, because GDAL's FlatGeobuf
    writer garbles an empty polygon (WKB type 0, which no reader accepts)
    and wrote 69 of Alaska's project outlines as NULL."""
    dest.unlink(missing_ok=True)
    lco = (["-lco", "SPATIAL_INDEX=NO"] if driver == "FlatGeobuf"
           else ["-lco", "GEOMETRY_NAME=geom", "-lco", "SPATIAL_INDEX=NO"])
    subprocess.run(["ogr2ogr", "-f", driver, "-t_srs", WRAP_SRS,
                    "-nlt", "CONVERT_TO_LINEAR", "-nlt", "PROMOTE_TO_MULTI", "-dim", "XY",
                    *lco, "-nln", layer,
                    "-sql", f'SELECT {", ".join(columns)} FROM "{layer}"',
                    str(dest), gdb], check=True)


# ---------- build ----------

# The states are cut into the cells of a grid this many degrees wide
# (state_pieces): a point is tested only against the pieces of its own cell.
GRID_DEG = 0.25


def connect(work: Path, memory: str, threads: int | None = None) -> duckdb.DuckDBPyConnection:
    """An in-memory DuckDB that spills to work/spill: nothing is kept in a
    database between steps, each step reads and writes Parquet."""
    (work / "spill").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs; "
                f"SET temp_directory = '{work / 'spill'}'; SET memory_limit = '{memory}'; "
                "SET preserve_insertion_order = false")
    if threads:
        con.execute(f"SET threads = {threads}")
    return con


def states_file(work: Path) -> Path:
    """TIGER/Line 2026 states (code, name, outline), fetched once per work dir."""
    dest = work / "states.parquet"
    if not dest.is_file():
        work.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
        tmp = dest.with_suffix(".part")
        con.execute(f"""COPY (SELECT STUSPS AS state, NAME AS name, geometry
                              FROM read_parquet('{TIGER_STATES}'))
                        TO '{tmp}' (FORMAT PARQUET)""")
        con.close()
        tmp.rename(dest)
    return dest


def pieces_file(work: Path) -> Path:
    """Each state cut along a GRID_DEG grid: (state, x, y, piece), the
    piece being the state's part within cell (x, y). A point then finds its
    state with an equality join on its cell and a test against a few small
    pieces, where a spatial join on the whole states (TIGER/Line, up to
    tens of thousands of vertices each) ran out of memory in DuckDB 1.5
    (Texas, 2026-11-07). Exact: the pieces of a state tile it. Built once
    per work dir, in a few seconds."""
    dest = work / "state_pieces.parquet"
    if dest.is_file():
        return dest
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    d = GRID_DEG
    tmp = dest.with_suffix(".part")
    con.execute(f"""COPY (
        WITH parts AS (SELECT state, unnest(ST_Dump(ST_SetCRS(geometry::GEOMETRY, ''))).geom AS g
                       FROM read_parquet('{states_file(work)}')),
        xs AS (SELECT state, g, unnest(range(floor(ST_XMin(g) / {d})::INT,
                                             floor(ST_XMax(g) / {d})::INT + 1)) AS x FROM parts),
        cells AS (SELECT state, g, x, unnest(range(floor(ST_YMin(g) / {d})::INT,
                                                   floor(ST_YMax(g) / {d})::INT + 1)) AS y FROM xs),
        env AS (SELECT state, g, x, y,
                       ST_MakeEnvelope(x * {d}, y * {d}, (x + 1) * {d}, (y + 1) * {d}) AS e
                FROM cells)
        SELECT state, x, y, ST_Intersection(g, e) AS piece FROM env WHERE ST_Intersects(g, e)
        ) TO '{tmp}' (FORMAT PARQUET)""")
    con.close()
    tmp.rename(dest)
    return dest


def load_states(con, work: Path) -> None:
    con.execute(f"""CREATE OR REPLACE TEMP TABLE states AS
        SELECT state, name, ST_SetCRS(geometry::GEOMETRY, '') AS g
        FROM read_parquet('{states_file(work)}')""")


def state_name(con, st: str) -> str:
    if st == "UM":
        return "U.S. Minor Outlying Islands"
    return con.execute("SELECT name FROM states WHERE state = ?", [st]).fetchone()[0]


def load_codes(con, work: Path, folder: Path, codes: dict) -> dict:
    archive = fetch(CODES_URL, work / "zips" / "NWI-Code-Definitions.zip", codes.get("bytes"))
    csv = work / "codes.csv"
    with zipfile.ZipFile(archive) as z, z.open(CODES_CSV) as src, csv.open("wb") as out:
        shutil.copyfileobj(src, out)
    con.execute(f"""CREATE OR REPLACE TABLE codes AS
        SELECT * FROM read_csv('{csv}', header = true, all_varchar = true)""")
    n, distinct = con.execute("SELECT count(*), count(DISTINCT ATTRIBUTE) FROM codes").fetchone()
    if n != distinct:
        sys.exit(f"code table: {n - distinct} repeated ATTRIBUTE codes")
    folder.mkdir(parents=True, exist_ok=True)
    dest = folder / "codes.parquet"
    con.execute(f"COPY (SELECT * FROM codes ORDER BY ATTRIBUTE) TO '{dest}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD)")
    return {"file": dest.name, "rows": n, "bytes": dest.stat().st_size, "sha256": sha256(dest),
            "source": CODES_URL, "etag": codes.get("etag")}


def stage_unit(unit: str, src: dict, work: Path, low_disk: bool, memory: str,
               threads: int | None) -> dict:
    """stage_one() for a unit in a process of its own: whatever GDAL, GEOS
    or DuckDB hold for one unit is returned to the system before the next
    (the national run that kept one process grew to 15 GB by Minnesota).
    A unit already staged (<unit>.json beside its folder) is skipped."""
    done = work / "stage" / f"{unit}.json"
    if done.is_file() and (work / "stage" / unit).is_dir():
        return json.loads(done.read_text())
    pieces_file(work)
    cmd = [sys.executable, str(Path(__file__).resolve()), "stage-one", "--unit", unit,
           "--work-dir", str(work), "--memory", memory, "--src", json.dumps(src)]
    if threads:
        cmd += ["--threads", str(threads)]
    if low_disk:
        cmd.append("--low-disk")
    subprocess.run(cmd, check=True)
    return json.loads(done.read_text())


def stage_one(unit: str, src: dict, work: Path, low_disk: bool, memory: str,
              threads: int | None) -> None:
    """One unit's wetlands to work/stage/<unit>/target=<XX>/part_*.parquet:
    repaired, unwrapped, each row with the state of its interior point and
    whether it is a home copy, partitioned by its target state so a state's
    file reads only its own rows; <unit>.index.parquet, the columns the
    dedup reads; its projects to <unit>.projects.parquet; <unit>.json, the
    unit's counts, written last (a rerun skips a unit that has it).

    Streamed, the polygons never held in memory: GDAL's FlatGeobuf through
    DuckDB to <unit>.fixed.parquet (repaired, unwrapped, with each row's
    interior point); every row located on its point alone; the polygons
    streamed once through a join on that into the partitions."""
    t0 = time.monotonic()
    stage = work / "stage" / unit
    done = work / "stage" / f"{unit}.json"
    shutil.rmtree(stage, ignore_errors=True)
    stage.parent.mkdir(parents=True, exist_ok=True)
    archive = fetch(zip_url(unit), work / "zips" / f"{unit}.zip", src.get("bytes"))
    gdb, layers = gdb_layers(archive)
    wet, meta = pick(layers, unit)
    raw = stage.parent / f"{unit}.raw.fgb"
    to_fgb(gdb, wet, raw, ["NWI_ID", "ATTRIBUTE", "WETLAND_TYPE", "ACRES"])
    con = connect(work, memory, threads)
    if meta:
        fgb = stage.parent / f"{unit}.projects.gpkg"
        to_fgb(gdb, meta, fgb, [c for c in PROJECT_COLUMNS if c != "DATA_SOURCE"]
               + (["DATA_SOURCE"] if unit == "PacTrust" else []), driver="GPKG")
        con.execute(f"""COPY (SELECT * EXCLUDE (geom), ST_AsWKB(geom) AS geom FROM ST_Read('{fgb}'))
                        TO '{stage.parent / f"{unit}.projects.parquet"}' (FORMAT PARQUET)""")
        fgb.unlink()
    if low_disk:
        archive.unlink()
    own = UNIT_STATES.get(unit, (unit,))
    own_sql = ", ".join(f"'{s}'" for s in own)
    fixed = stage.parent / f"{unit}.fixed.parquet"
    valid = ("CASE WHEN invalid THEN ST_Multi(ST_CollectionExtract(ST_MakeValid(g), 3)) "
             "ELSE g END")
    con.execute(f"""COPY (
        SELECT NWI_ID, ATTRIBUTE, WETLAND_TYPE, ACRES, invalid, across, geom,
               ST_PointOnSurface(geom) AS p
        FROM (SELECT *, ST_XMin(v) < 180 AND ST_XMax(v) > 180 AS across,
                     {UNWRAP.format(g='v')} AS geom
              FROM (SELECT *, {valid} AS v
                    FROM (SELECT NWI_ID, ATTRIBUTE, WETLAND_TYPE, ACRES, g,
                                 NOT ST_IsValid(g) AS invalid
                          FROM (SELECT *, ST_SetCRS(geom::GEOMETRY, '') AS g
                                FROM ST_Read('{raw}')))))
        ) TO '{fixed}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)""")
    raw.unlink()
    rows, ids, nulls, empty, invalid, across = con.execute(f"""
        SELECT count(*), count(DISTINCT NWI_ID), count(*) FILTER (WHERE NWI_ID IS NULL),
               count(*) FILTER (WHERE geom IS NULL OR ST_IsEmpty(geom)),
               count(*) FILTER (WHERE invalid), count(*) FILTER (WHERE across)
        FROM read_parquet('{fixed}')""").fetchone()
    expected = layers[wet]
    problems = []
    if rows != expected:
        problems.append(f"{rows:,} rows, the geodatabase has {expected:,}")
    if nulls:
        problems.append(f"{nulls} rows without NWI_ID")
    if empty:
        problems.append(f"{empty} empty geometries after repair")
    if problems:
        sys.exit(f"{unit}: gate failed: " + "; ".join(problems))
    dups = in_unit_duplicates(con, unit, fixed, rows) if ids != rows else \
        {"identical": 0, "different": 0, "dropped": []}
    rows -= dups["identical"] + dups["different"]
    # A fresh connection for the joins: the one that wrote and counted the
    # fixed file keeps memory DuckDB does not hand back (the same join that
    # fits in a new connection ran out of memory in it, Minnesota).
    con.close()
    con = connect(work, memory, threads)
    load_states(con, work)
    um = "'UM'" if "UM" in own else "m.state"
    # Located on the points alone (NWI_ID and its interior point, a few
    # dozen bytes a row): the state of each point from the state pieces of
    # its grid cell (pieces_file); a point on a border between two states
    # takes the first code. A point in no state takes the nearest of the
    # unit's own states. The polygons are not carried through these joins:
    # with them, Minnesota (2.7 million rows) ran out of memory.
    pts = f"(SELECT NWI_ID, p FROM read_parquet('{fixed}'))"
    d = GRID_DEG
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pieces AS
        SELECT * FROM read_parquet('{pieces_file(work)}')""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE hit AS
        SELECT c.NWI_ID, min(sp.state) AS state FROM {pts} c
        JOIN pieces sp ON sp.x = floor(ST_X(c.p) / {d})::INT
                      AND sp.y = floor(ST_Y(c.p) / {d})::INT
        WHERE ST_Intersects(sp.piece, c.p) GROUP BY 1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE miss AS
        SELECT c.NWI_ID, arg_min(s.state, ST_Distance(s.g, c.p)) AS state,
               min(ST_Distance(s.g, c.p)) AS d
        FROM {pts} c ANTI JOIN hit h ON h.NWI_ID = c.NWI_ID
        JOIN states s ON s.state IN ({own_sql}) GROUP BY 1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE located AS
        SELECT c.NWI_ID, h.state AS point_state,
               coalesce(h.state, CASE WHEN m.d <= {NEAREST_DEGREES} THEN m.state
                                      ELSE {um} END, 'NONE') AS target,
               (h.state IS NULL OR h.state IN ({own_sql})) AS home
        FROM (SELECT NWI_ID FROM read_parquet('{fixed}')) c
        LEFT JOIN hit h USING (NWI_ID) LEFT JOIN miss m USING (NWI_ID)""")
    con.execute("DROP TABLE hit; DROP TABLE miss; DROP TABLE pieces")
    # Then the polygons, streamed once through a join on that small table
    # into the partitions; small row groups bound what each open partition
    # holds before it is flushed.
    con.execute(f"""COPY (
        SELECT f.NWI_ID, f.ATTRIBUTE, f.WETLAND_TYPE, f.ACRES, '{unit}' AS source_unit,
               l.point_state, l.target, l.home, f.geom
        FROM read_parquet('{fixed}') f JOIN located l USING (NWI_ID))
        TO '{stage}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (target),
                      FILENAME_PATTERN 'part_{{i}}', ROW_GROUP_SIZE 20000)""")
    con.execute("DROP TABLE located")
    fixed.unlink()
    # part_<i>.parquet, not data_<i>: the globs never match the ._* files
    # macOS writes beside each file on exFAT (T9).
    parts = (f"read_parquet('{stage}/target=*/part_*.parquet', hive_partitioning = true)")
    staged_rows, outside, neighbour, untargeted = con.execute(f"""
        SELECT count(*), count(*) FILTER (WHERE point_state IS NULL),
               count(*) FILTER (WHERE NOT home), count(*) FILTER (WHERE target = 'NONE')
        FROM {parts}""").fetchone()
    if untargeted:
        sys.exit(f"{unit}: {untargeted} rows without a target state")
    if staged_rows != rows:
        sys.exit(f"{unit}: staged {staged_rows:,} rows of {rows:,}")
    con.execute(f"""COPY (SELECT NWI_ID, source_unit, target, home, ACRES FROM {parts})
                    TO '{stage.parent / f"{unit}.index.parquet"}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    con.close()
    entry = {"unit": unit, "states": list(own), "layer": wet, "projects_layer": meta,
             "rows": rows, "gdb_rows": expected, "repaired_invalid": invalid,
             "split_at_180": across, "point_outside_states": outside,
             "rows_in_a_neighbour": neighbour,
             "duplicates_identical": dups["identical"],
             "duplicates_different": dups["different"],
             "duplicates_dropped": dups["dropped"], "stage_version": STAGE_VERSION, **src}
    done.write_text(json.dumps(entry))
    print(f"  {unit}: {rows:,} rows, {neighbour:,} in a neighbour, {outside} outside "
          f"every state, {invalid} repaired, {time.monotonic() - t0:.0f} s", flush=True)


def in_unit_duplicates(con, unit: str, fixed: Path, rows: int) -> dict:
    """An NWI_ID FWS repeats within one unit's download, kept once, by the
    cross-state rule: the largest ACRES, then the geometry's WKB bytes in
    ascending order. A copy identical to the one kept (attributes and shape)
    is dropped and counted; a different one is dropped, counted and listed.
    The build fails if more than DUPLICATE_SHARE of the unit's rows are
    different copies (at least one is allowed): beyond that it is an
    upstream problem, not a stray polygon. fixed is rewritten without them.

    Vermont, 2026-10: Lake Champlain's NWI_ID also names a 0.42-acre piece
    of lake 52 km north, apart from the 221,309-acre lake; the lake is kept."""
    ranked = f"""
        WITH d AS (SELECT NWI_ID FROM read_parquet('{fixed}') GROUP BY 1 HAVING count(*) > 1),
        r AS (SELECT f.file_row_number AS r, f.NWI_ID, f.ATTRIBUTE, f.WETLAND_TYPE, f.ACRES,
                     md5(ST_AsWKB(f.geom)::BLOB) AS h,
                     row_number() OVER (PARTITION BY f.NWI_ID
                                        ORDER BY f.ACRES DESC NULLS LAST,
                                                 ST_AsWKB(f.geom)::BLOB, f.ATTRIBUTE,
                                                 f.WETLAND_TYPE) AS k
              FROM read_parquet('{fixed}', file_row_number = true) f SEMI JOIN d USING (NWI_ID))
        SELECT x.r, x.NWI_ID, x.ACRES, k.ACRES,
               (x.ATTRIBUTE IS NOT DISTINCT FROM k.ATTRIBUTE
                AND x.WETLAND_TYPE IS NOT DISTINCT FROM k.WETLAND_TYPE
                AND x.ACRES IS NOT DISTINCT FROM k.ACRES AND x.h = k.h) AS identical
        FROM r x JOIN r k ON k.NWI_ID = x.NWI_ID AND k.k = 1
        WHERE x.k > 1 ORDER BY x.NWI_ID, x.r"""
    drop = con.execute(ranked).fetchall()
    different = [d for d in drop if not d[4]]
    out = {"identical": len(drop) - len(different), "different": len(different),
           "dropped": [{"NWI_ID": d[1], "acres": d[2], "kept_acres": d[3]}
                       for d in different[:50]]}
    if len(different) > max(1, rows * DUPLICATE_SHARE):
        sys.exit(f"{unit}: gate failed: {len(different)} NWI_IDs repeated with different rows "
                 f"within the unit (over {DUPLICATE_SHARE:.2%} of {rows:,}): {out['dropped'][:10]}")
    kept = fixed.with_suffix(".dedup.parquet")
    gone = ", ".join(str(d[0]) for d in drop)
    con.execute(f"""COPY (SELECT * EXCLUDE (file_row_number)
                          FROM read_parquet('{fixed}', file_row_number = true)
                          WHERE file_row_number NOT IN ({gone}))
                    TO '{kept}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000)""")
    kept.replace(fixed)
    print(f"  {unit}: NWI_ID repeated within the unit: {out['identical']} identical and "
          f"{out['different']} different copies dropped {out['dropped']}", flush=True)
    return out


def staged(work: Path, target: str = "*") -> str:
    """The staged rows of every unit (all targets, or one) as read_parquet:
    `target` comes from the Hive folder."""
    return (f"read_parquet('{work / 'stage'}/*/target={target}/part_*.parquet', "
            "hive_partitioning = true)")


def index_files(work: Path) -> list[Path]:
    """Every staged unit's index (not the ._* files exFAT adds)."""
    return sorted(p for p in (work / "stage").glob("*.index.parquet")
                  if not p.name.startswith("."))


def choose(con, work: Path, groups: int = 1) -> dict:
    """Each NWI_ID once: the home copy, then the largest ACRES, then the
    unit name, read from the units' indexes only. Writes work/win.parquet
    (NWI_ID, source_unit, target) and work/dedup.json; returns the dedup
    counts and the target states in `groups` groups of about equal rows,
    for the write jobs."""
    files = index_files(work)
    if not files:
        sys.exit(f"{work / 'stage'}: no staged unit")
    idx = "read_parquet([" + ", ".join(f"'{p}'" for p in files) + "])"
    win = work / "win.parquet"
    con.execute(f"""COPY (
        SELECT NWI_ID, source_unit, target, home FROM (
            SELECT NWI_ID, source_unit, target, home,
                   row_number() OVER (PARTITION BY NWI_ID
                                      ORDER BY home DESC, ACRES DESC NULLS LAST,
                                               source_unit) AS k
            FROM {idx})
        WHERE k = 1) TO '{win}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    copies, ids = con.execute(f"SELECT count(*), count(DISTINCT NWI_ID) FROM {idx}").fetchone()
    kept, from_neighbour = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE NOT home) FROM read_parquet('{win}')").fetchone()
    if kept != ids:
        sys.exit(f"dedup: {kept:,} kept for {ids:,} NWI_IDs")
    multi_home = con.execute(f"""
        SELECT count(*) FROM (SELECT NWI_ID FROM {idx} WHERE home
                              GROUP BY 1 HAVING count(*) > 1)""").fetchone()[0]
    units = sorted(p.name.split(".")[0] for p in files)
    targets = dict(con.execute(f"SELECT target, count(*) FROM read_parquet('{win}') "
                               "GROUP BY 1 ORDER BY 2 DESC").fetchall())
    foreign = [t for t in targets if t in WORKSTATION_UNITS and t not in units]
    if foreign:
        sys.exit(f"rows for {foreign} from other units: that state is built on its own "
                 "(nwi_publish_ak.sh) and could not deduplicate them")
    dedup = {"units": units, "copies_read": copies, "ids": ids, "copies_dropped": copies - ids,
             "kept_from_a_neighbour": from_neighbour, "ids_with_several_home_copies": multi_home,
             "targets": targets}
    (work / "dedup.json").write_text(json.dumps(dedup, indent=2) + "\n")
    return {**dedup, "groups": balance(targets, groups)}


def balance(sizes: dict[str, int], groups: int) -> list[list[str]]:
    """Keys in `groups` groups of about equal total size, largest first."""
    bins: list[tuple[int, list[str]]] = [(0, []) for _ in range(max(1, groups))]
    for key, n in sorted(sizes.items(), key=lambda kv: -(kv[1] or 0)):
        total, members = min(bins, key=lambda b: b[0])
        bins.remove((total, members))
        bins.append((total + (n or 0), members + [key]))
    return [sorted(m) for _, m in sorted(bins, key=lambda b: -b[0]) if m]


# Rows sorted at once by write_sorted(): a bucket of the Hilbert order.
SORT_ROWS = 300_000


def write_sorted(con, dest: Path, crs: dict) -> dict:
    """census.write_src() for a state too large to sort at once: the view
    `src` (a NAD83 `geometry` and its `bbox`) to GeoParquet 2.0, Hilbert
    order, row groups capped by bytes. DuckDB 1.5 cannot spill a sort that
    carries large geometries (Minnesota, 2.7 million polygons, ran out of
    memory sorting), so the rows are streamed once with their Hilbert key,
    sorted SORT_ROWS at a time in key ranges, and the sorted buckets read
    back in order (insertion order kept, no sort) into the file
    gaul.rewrite() cuts. Same file as write_src() would write."""
    xmin, ymin, xmax, ymax, types, n = con.execute("""
        SELECT min(ST_XMin(geometry)), min(ST_YMin(geometry)), max(ST_XMax(geometry)),
               max(ST_YMax(geometry)), list(DISTINCT ST_GeometryType(geometry)::VARCHAR), count(*)
        FROM src""").fetchone()
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
    box = f"ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))"
    keyed = dest.with_suffix(".keyed.parquet")
    # Row groups of 20,000 rows in the intermediate files: DuckDB's writer
    # buffers a whole row group per thread, and the default 122,880 rows of
    # large polygons ran out of memory (Minnesota).
    con.execute(f"""COPY (SELECT *, ST_Hilbert(geometry, {box}) AS _h FROM src)
                    TO '{keyed}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 20000)""")
    k = max(1, -(-n // SORT_ROWS))
    qs = ", ".join(str(i / k) for i in range(1, k))
    bounds = con.execute(f"""SELECT quantile_disc(_h, [{qs}])
                             FROM read_parquet('{keyed}', hive_partitioning = false)
                          """).fetchone()[0] if k > 1 else []
    edges = [None, *sorted(set(bounds)), None]
    buckets = []
    for i, (lo, hi) in enumerate(zip(edges, edges[1:])):
        cond = " AND ".join(c for c in (f"_h >= {lo}" if lo is not None else "",
                                        f"_h < {hi}" if hi is not None else "") if c) or "true"
        part = dest.with_suffix(f".b{i:03d}.parquet")
        con.execute(f"""COPY (SELECT * EXCLUDE (_h) FROM read_parquet('{keyed}', hive_partitioning = false)
                              WHERE {cond}
                              ORDER BY _h)
                        TO '{part}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 20000)""")
        buckets.append(part)
    keyed.unlink()
    raw = dest.with_suffix(".raw.parquet")
    listing = "[" + ", ".join(f"'{b}'" for b in buckets) + "]"
    con.execute("SET preserve_insertion_order = true")
    try:
        # hive_partitioning off: the files sit in state=<XX>/, which DuckDB
        # would otherwise add as a column.
        con.execute(f"""COPY (SELECT * FROM read_parquet({listing}, hive_partitioning = false))
            TO '{raw}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', COMPRESSION ZSTD,
                        ROW_GROUP_SIZE 20000, KV_METADATA {{geo: '{geo}'}})""")
    finally:
        con.execute("SET preserve_insertion_order = false")
    for b in buckets:
        b.unlink()
    rewrite(raw, dest)
    raw.unlink()
    groups, largest = con.execute(f"""
        SELECT count(*), max(b) FROM (SELECT row_group_id, sum(total_compressed_size) AS b
        FROM parquet_metadata('{dest}') GROUP BY 1)""").fetchone()
    return {"file": dest.name, "features": n, "bytes": dest.stat().st_size,
            "row_groups": groups, "largest_row_group_bytes": largest, "sha256": sha256(dest)}


def write_states(con, work: Path, out: Path, rel_name: str, crs: dict, targets: list[str],
                 low_disk: bool) -> dict[str, dict]:
    """One file per target state in `targets`, from the kept rows (work/
    win.parquet) of every unit's partition for it; each partition deleted
    once written (low_disk). Each state's entry, with its rows whose
    ATTRIBUTE is not in the code table, also goes to out/entries/<XX>.json
    for assemble()."""
    win = work / "win.parquet"
    rel = out / "nwi" / rel_name
    (out / "entries").mkdir(parents=True, exist_ok=True)
    result = {}
    t0 = time.monotonic()
    # Joins as written: the kept keys (win.parquet) are the build side.
    # DuckDB estimates the staged partitions at a few thousand rows and
    # flips the semi join to build on them, geometries included: California
    # and Wyoming (3 GB of staged rows each) then ran out of memory.
    con.execute("SET disabled_optimizers = 'build_side_probe_side'")
    cols =", ".join(f'"{c}"' for c in COLUMNS)
    decoded = ", ".join(f"c.{d}" for d in DECODED)
    for i, st in enumerate(targets, 1):
        # A view, not a table: write_src's Hilbert sort then reads the
        # partitions and spills, where a table of Minnesota's 2.7 million
        # polygons beside the sort ran out of memory.
        con.execute(f"""CREATE OR REPLACE TEMP VIEW src AS
            SELECT {cols}, ST_SetCRS(geom, '{CRS}') AS geometry, {BBOX.replace('geometry', 'geom')}
            FROM (SELECT s.NWI_ID, s.ATTRIBUTE, s.WETLAND_TYPE, s.ACRES, {decoded},
                         s.source_unit, s.geom
                  FROM {staged(work, st)} s
                  SEMI JOIN (SELECT NWI_ID, source_unit FROM read_parquet('{win}')
                             WHERE target = '{st}') w
                      ON w.NWI_ID = s.NWI_ID AND w.source_unit = s.source_unit
                  LEFT JOIN codes c ON c.ATTRIBUTE = s.ATTRIBUTE)""")
        # One pass over ATTRIBUTE alone (the view reads no geometry for it).
        bad_rows, bad = con.execute(
            "SELECT count(*), coalesce(list(DISTINCT ATTRIBUTE)[:50], []) FROM src "
            "WHERE ATTRIBUTE NOT IN (SELECT ATTRIBUTE FROM codes)").fetchone()
        dest = rel / f"state={st}" / "wetlands.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        e = write_sorted(con, dest, crs)
        con.execute("DROP VIEW src")
        verify(con, dest.parent, [dest.stem])
        check_crs(con, dest)
        # The longitudes it covers, for the catalog's extent (Alaska and the
        # Pacific cross 180), measured while the file is local.
        rows = f"read_parquet('{dest}', hive_partitioning = false)"
        ymin, ymax = con.execute(f"SELECT min(bbox.ymin), max(bbox.ymax) FROM {rows}").fetchone()
        entry = {"state": st, **e, "path": f"state={st}/wetlands.parquet",
                 "name": state_name(con, st), "lon_intervals": lon_intervals(con, rows),
                 "ymin": ymin, "ymax": ymax,
                 "rows_with_unknown_code": bad_rows, "unknown_codes": bad}
        (out / "entries" / f"{st}.json").write_text(json.dumps(entry) + "\n")
        result[st] = entry
        if low_disk:
            for part in (work / "stage").glob(f"*/target={st}"):
                shutil.rmtree(part)
        print(f"  [{i}/{len(targets)}] {st}: {e['features']:,} rows, {e['bytes'] / 1e6:,.1f} MB, "
              f"{e['row_groups']} groups, {time.monotonic() - t0:.0f} s", flush=True)
    return result


def write_projects(con, work: Path, rel: Path, crs: dict, built: list[str],
                   live_projects: str | None) -> dict | None:
    """The mapping projects of every unit, a copy repeated across units kept
    once (same attributes, same shape). With live_projects (the published
    file of the same release), its projects from units this build did not
    stage are kept: the Alaska build adds Alaska's to the rest."""
    files = sorted((work / "stage").glob("[!.]*.projects.parquet"))
    if not files:
        return None
    parts = []
    for p in files:
        unit = p.name.split(".")[0]
        have = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall()]
        cols = ", ".join(f'"{c}"' if c in have else f'NULL::VARCHAR AS "{c}"'
                         for c in PROJECT_COLUMNS)
        parts.append(f"SELECT {cols}, '{unit}' AS source_unit, "
                     f"{UNWRAP.format(g='g')} AS geom FROM (SELECT *, "
                     f"ST_Multi(ST_CollectionExtract(ST_MakeValid(ST_GeomFromWKB(geom)), 3)) "
                     f"AS g FROM read_parquet('{p}'))")
    cols = ", ".join(f'"{c}"' for c in PROJECT_COLUMNS)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE src AS
        SELECT {cols}, list(source_unit ORDER BY source_unit) AS source_units,
               ST_SetCRS(any_value(geom), '{CRS}') AS geometry
        FROM (SELECT *, md5(ST_AsWKB(geom)::BLOB) AS shape_key
              FROM ({' UNION ALL '.join(parts)}) WHERE NOT ST_IsEmpty(geom))
        GROUP BY {cols}, shape_key""")
    if live_projects:
        units = "[" + ", ".join(f"'{u}'" for u in built) + "]"
        con.execute(f"""INSERT INTO src
            SELECT {cols}, source_units, geometry FROM read_parquet('{live_projects}')
            WHERE NOT list_has_any(source_units, {units})""")
    con.execute(f"CREATE OR REPLACE TEMP TABLE src AS SELECT *, {BBOX} FROM src")
    dest = rel / "projects.parquet"
    e = write_src(con, dest, crs)
    verify(con, rel, ["projects"])
    check_crs(con, dest)
    con.execute("DROP TABLE src")
    return e


def tile_all(con, sources: dict[str, str], rel: Path, work: Path) -> dict:
    """wetlands.pmtiles from every state file in rel, down TILE_LADDER: the
    first setting with no tile over the size cap. Wetlands are small and
    dense, so the coarse zooms are thinned (coverage=False); every polygon
    is in the maximum zoom. (Bands joined by pmtiles.pyramid() do not help:
    a band keeps every polygon at its own top zoom, so a z6-9 band overflows
    at z9.) tylertoo reads lon/lat, so it reads copies tagged CRS84 (NAD83
    and WGS 84 differ by less than a pixel at these zooms)."""
    pmtiles.binary()
    lonlat = work / "lonlat"
    shutil.rmtree(lonlat, ignore_errors=True)
    lonlat.mkdir(parents=True)
    props = ", ".join(f'"{p}"' for p in TILE_PROPS)
    if not sources:
        sys.exit(f"{rel}: no state files to tile")
    for st, f in sorted(sources.items()):
        con.execute(f"""COPY (SELECT {props}, ST_SetCRS(geometry::GEOMETRY, 'OGC:CRS84') AS geometry
                              FROM read_parquet('{f}', hive_partitioning = false))
                        TO '{lonlat / f"{st}.parquet"}' (FORMAT PARQUET)""")
    # A glob, not the directory: on exFAT (T9) macOS writes ._* files beside each.
    src = lonlat / "[A-Z]*.parquet"
    try:
        for props, lo in TILE_LADDER:
            try:
                entry = pmtiles.tile(src, rel / "wetlands.pmtiles", layer="wetlands",
                                     min_zoom=lo, max_zoom=TILE_MAX_ZOOM, properties=props,
                                     coverage=False)
            except SystemExit as e:
                print(f"    tiles: {len(props)} properties from z{lo}: {e}", flush=True)
                continue
            entry["states"] = sorted(sources)
            return {"name": "wetlands", **entry}
        sys.exit("wetlands.pmtiles: no tile setting fits the size cap")
    finally:
        shutil.rmtree(lonlat, ignore_errors=True)


def tiles(work: Path, out: Path, memory: str, live: str | None = None) -> None:
    """nwi_tiles_local.sh's step: wetlands.pmtiles from every state file of the
    release, and its entry in the release's _manifest.json. With `live`
    (the published wetlands/ prefix) the manifest and the state files are
    read from there, Alaska included, each file streamed into its lon/lat
    copy: a runner needs no copy of the release. Without it, from out/."""
    work.mkdir(parents=True, exist_ok=True)
    (work / "spill").mkdir(exist_ok=True)
    if live:
        index = json.loads(curl(f"{live}/index.json"))
        rel = out / index["datasets"][0]["path"]
        manifest = json.loads(curl(f"{live}/{index['datasets'][0]['path']}/_manifest.json"))
        sources = {f["state"]: f"{live}/{index['datasets'][0]['path']}/{f['path']}"
                   for f in manifest["files"]}
        rel.mkdir(parents=True, exist_ok=True)
        (out / "index.json").write_text(json.dumps(index, indent=2) + "\n")
    else:
        index = json.loads((out / "index.json").read_text())
        rel = out / index["datasets"][0]["path"]
        manifest = json.loads((rel / "_manifest.json").read_text())
        sources = {f["state"]: str(rel / f["path"]) for f in manifest["files"]}
        missing = [s for s, f in sources.items() if not Path(f).is_file()]
        if missing:
            sys.exit(f"tiles: no local file for {missing}")
    con = connect(work, memory)
    t0 = time.monotonic()
    manifest["pmtiles"] = tile_all(con, sources, rel, work)
    con.close()
    (rel / "_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    e = manifest["pmtiles"]
    print(f"  tiles: z{e['min_zoom']}-{e['max_zoom']}, {len(e['properties'])} properties, "
          f"{e['bytes'] / 1e6:,.1f} MB, {e['tiles']:,} tiles, {time.monotonic() - t0:.0f} s",
          flush=True)


def attribution(src: dict) -> str:
    return f"""\
# Data attribution

This data is the National Wetlands Inventory (NWI) of the U.S. Fish and
Wildlife Service, release of {src['last_updated']}.

  Credit:      U.S. Fish and Wildlife Service, National Wetlands Inventory
  Download:    {DOWNLOAD_PAGE}
  Codes:       {CODES_PAGE}
               ({CODES_URL})
  Disclaimer:  {DISCLAIMER}
  Status:      a work of the United States Government, not subject to copyright
               in the United States (17 U.S.C. 105); FWS states no access or
               use constraints

FWS's disclaimer, in short: the NWI describes wetlands by their biology, from
imagery and fieldwork at the scale of each mapping project. It is not a
jurisdictional delineation under the Clean Water Act or any other law, and it
does not define the limits of any government's jurisdiction. Mapping dates
and scales differ between projects (projects.parquet lists them); FWS makes
no warranty of accuracy or completeness.

## What was changed here

One GeoParquet 2.0 file per state or territory from FWS's 53 geodatabases.
Columns ATTRIBUTE, WETLAND_TYPE, ACRES and NWI_ID are FWS's, unchanged;
added are the code's names from FWS's NWI Code Definitions table
(SYSTEM_NAME to MODIFIER2_NAME, as FWS names them) and `source_unit`, the
download each row was taken from. The downloads overlap at state borders:
each NWI_ID is kept once, from the download of the state that holds it,
in that state's file (_manifest.json counts the copies dropped). The
geometry is reprojected from FWS's Albers projections (NAD83) to NAD83
lon/lat (EPSG:4269), curves cut into straight segments, polygons across the
180th meridian split there, invalid polygons repaired (ST_MakeValid). Each
row has a `bbox` column declared as the GeoParquet covering, rows sorted
along a Hilbert curve. Riparian layers are not included. The PMTiles archive
is derived for web maps.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def live_manifest(rel_name: str, live_url: str | None) -> dict | None:
    """The published manifest of this release, or None (none yet, another
    release, no --live)."""
    if not live_url:
        return None
    try:
        live = json.loads(curl(f"{live_url}/nwi/{rel_name}/_manifest.json"))
    except subprocess.CalledProcessError:
        return None
    return live if live.get("release") == rel_name else None


def merge_live(manifest: dict, live: dict | None) -> dict:
    """Keep what the published manifest of the same release has from units
    this build did not stage: their states, units and build part (Alaska,
    built on a workstation; or the rest, when this is the Alaska build). Its
    map is kept, marked stale, until nwi_tiles_local.sh redraws it."""
    if not live:
        return manifest
    built = set(manifest["parts"][0]["units"])
    states = {f["state"] for f in manifest["files"]}
    kept = [f for f in live.get("files", []) if f["state"] not in states]
    manifest["files"] = sorted(manifest["files"] + kept, key=lambda f: f["state"])
    manifest["units"] = sorted(manifest["units"] + [u for u in live.get("units", [])
                                                    if u["unit"] not in built],
                               key=lambda u: u["unit"])
    manifest["parts"] += [p for p in live.get("parts", []) if not built & set(p["units"])]
    manifest["total_features"] = sum(f["features"] for f in manifest["files"])
    if live.get("pmtiles"):
        manifest["pmtiles"] = {**live["pmtiles"], "stale": True}
    return manifest


def read_upstream(path: Path | None) -> dict:
    """The upstream read once per run (plan writes it, every job reads it),
    so all jobs build the same release; without a file, read now."""
    return json.loads(path.read_text()) if path else upstream()


def plan(groups: int, dest: Path) -> dict:
    """The release and the runner's units in `groups` groups of about equal
    zip size, for the staging jobs; the upstream written to dest."""
    src = upstream()
    dest.write_text(json.dumps(src, indent=2) + "\n")
    units = {u: (h or {}).get("bytes") or 0 for u, h in src["units"].items()
             if u not in WORKSTATION_UNITS}
    return {"release": src["release"], "last_updated": src["last_updated"],
            "groups": balance(units, groups)}


def stage(src: dict, work: Path, units: list[str], low_disk: bool, memory: str,
          threads: int | None) -> list[dict]:
    missing = sorted(set(units) - set(src["units"]))
    if missing:
        sys.exit(f"units not on the download page: {missing}")
    return [stage_unit(u, src["units"][u], work, low_disk, memory, threads) for u in units]


def write(src: dict, work: Path, out: Path, targets: list[str], low_disk: bool,
          memory: str, threads: int | None = None) -> dict[str, dict]:
    con = connect(work, memory, threads)
    crs = projjson(con)
    load_states(con, work)
    load_codes(con, work, work / "codes", src["codes"])
    files = write_states(con, work, out, src["release"], crs, targets, low_disk)
    con.close()
    return files


def assemble(src: dict, work: Path, out: Path, memory: str, live: str | None,
             threads: int | None = None) -> dict:
    """The release's manifest from the staged units (work/stage/<unit>.json),
    the dedup (work/dedup.json) and the written states (out/entries/*.json),
    with codes.parquet, projects.parquet, ATTRIBUTION.txt, index.json and
    _source.json. Gates: every kept NWI_ID is in a state file, unknown codes
    under UNKNOWN_CODE_SHARE of the release's rows: this build's and, with
    live, the published parts it joins (Alaska alone has 0.9%, its own forms
    of the codes, the release 0.09%)."""
    rel_name = src["release"]
    rel = out / "nwi" / rel_name
    rel.mkdir(parents=True, exist_ok=True)
    dedup = json.loads((work / "dedup.json").read_text())
    todo = dedup["units"]
    entries = [json.loads((work / "stage" / f"{u}.json").read_text()) for u in todo]
    files = sorted((json.loads(p.read_text()) for p in (out / "entries").glob("*.json")
                    if not p.name.startswith(".")), key=lambda f: f["state"])
    missing = sorted(set(dedup["targets"]) - {f["state"] for f in files})
    if missing:
        sys.exit(f"gate: no state file written for {missing}")
    total = sum(f["features"] for f in files)
    if total != dedup["ids"]:
        sys.exit(f"gate: the state files hold {total:,} rows, {dedup['ids']:,} NWI_IDs were kept")
    unknown = sum(f.pop("rows_with_unknown_code") for f in files)
    codes_seen = sorted({c for f in files for c in f.pop("unknown_codes")})
    live_m = live_manifest(rel_name, live)
    # The published parts this build keeps (merge_live), counted with it.
    kept = [p for p in (live_m or {}).get("parts", []) if not set(todo) & set(p["units"])]
    states = {f["state"] for f in files}
    rel_unknown = unknown + sum(p["gate"]["rows_with_unknown_code"] for p in kept)
    rel_total = total + sum(f["features"] for f in (live_m or {}).get("files", [])
                            if f["state"] not in states)
    if rel_total and rel_unknown / rel_total > UNKNOWN_CODE_SHARE:
        sys.exit(f"gate: {rel_unknown:,} of the release's {rel_total:,} rows have an ATTRIBUTE "
                 f"not in the code table ({unknown:,} of {total:,} in this build): "
                 f"{codes_seen[:50]}")
    con = connect(work, memory, threads)
    crs = projjson(con)
    codes = load_codes(con, work, rel, src["codes"])
    live_projects = (f"{live}/nwi/{rel_name}/{live_m['projects']['file']}"
                     if live_m and live_m.get("projects") else None)
    projects = write_projects(con, work, rel, crs, todo, live_projects)
    con.close()
    stats = {k: v for k, v in dedup.items() if k not in ("units", "targets")}
    manifest = {
        "dataset": "nwi", "release": rel_name, "last_updated": src["last_updated"],
        "crs": CRS, "total_features": total,
        # One part per build: the runner's (every unit but Alaska) and the
        # workstation's (Alaska), each deduplicated on its own.
        "parts": [{"units": todo, "dedup": stats,
                   "gate": {"rows_with_unknown_code": unknown, "unknown_codes": codes_seen[:50],
                            "duplicates_within_units": {
                                "identical": sum(e.get("duplicates_identical", 0) for e in entries),
                                "different": sum(e.get("duplicates_different", 0) for e in entries),
                                "dropped": {e["unit"]: e["duplicates_dropped"] for e in entries
                                            if e.get("duplicates_dropped")}}},
                   "built": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}],
        "units": entries,
        "files": files,
        "codes": codes, "projects": projects, "pmtiles": None,
        "glob": f"s3://parquetry/wetlands/nwi/{rel_name}/state=*/wetlands.parquet",
    }
    manifest = merge_live(manifest, live_m)
    (rel / "_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (rel / "ATTRIBUTION.txt").write_text(attribution(src))
    (out / "index.json").write_text(json.dumps({"datasets": [
        {"path": f"nwi/{rel_name}", "code": "nwi",
         "name": f"National Wetlands Inventory, {src['last_updated']}"}]}, indent=2) + "\n")
    # A unit built elsewhere (Alaska) keeps the zip it was built from, as
    # published for this release, or none in a new release: the weekly check
    # then still sees Alaska as to do, even when FWS did not change its zip.
    source = json.loads(json.dumps(src))
    if live:
        try:
            prev = json.loads(curl(f"{live}/_source.json")).get("nwi", {})
        except subprocess.CalledProcessError:
            prev = {}
        was = prev.get("units", {}) if prev.get("release") == rel_name else {}
        for u in set(source["units"]) - set(todo):
            source["units"][u] = was.get(u)
    (out / "_source.json").write_text(json.dumps({"nwi": source}, indent=2) + "\n")
    print(f"  assembled {rel_name}: {len(files)} state files, {total:,} rows, "
          f"{len(manifest['files'])} with the published ones", flush=True)
    return manifest


def build(work: Path, out: Path, units: list[str] | None, no_tiles: bool, low_disk: bool,
          memory: str, threads: int | None, live: str | None) -> None:
    """Every step on one machine, in order: stage, choose, write, assemble,
    tiles. The workflow runs the same steps as separate jobs."""
    t0 = time.monotonic()
    src = upstream()
    todo = units or [u for u in src["units"] if u not in WORKSTATION_UNITS]
    print(f"NWI release {src['release']} ({src['last_updated']}), {len(todo)} units", flush=True)
    stage(src, work, todo, low_disk, memory, threads)
    con = connect(work, memory, threads)
    d = choose(con, work)
    con.close()
    print(f"  dedup: {d['copies_read']:,} rows read, {d['ids']:,} NWI_IDs kept, "
          f"{d['copies_dropped']:,} copies dropped, {d['kept_from_a_neighbour']:,} "
          f"kept from a neighbour's download, {d['ids_with_several_home_copies']:,} with "
          f"several home copies", flush=True)
    write(src, work, out, sorted(d["targets"]), low_disk, memory, threads)
    assemble(src, work, out, memory, live, threads)
    if low_disk:
        shutil.rmtree(work / "stage")
    if not no_tiles:
        tiles(work, out, memory)
    print(f"  done, {time.monotonic() - t0:.0f} s", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp, out=False):
        sp.add_argument("--work-dir", type=Path, default=Path("work"))
        if out:
            sp.add_argument("--out-dir", type=Path, required=True, help="the wetlands/ prefix")
        sp.add_argument("--memory", default="6GB", help="DuckDB memory_limit")
        sp.add_argument("--upstream", type=Path, help="the upstream plan wrote (default: read now)")

    sub.add_parser("upstream", help="print the release on the FWS download page")
    pl = sub.add_parser("plan", help="the release and the units in groups, for the staging jobs")
    pl.add_argument("--groups", type=int, default=6)
    pl.add_argument("--upstream-out", type=Path, required=True)
    s = sub.add_parser("stage", help="stage units, each in a process of its own")
    common(s)
    s.add_argument("--unit", action="append", required=True)
    s.add_argument("--threads", type=int)
    s.add_argument("--low-disk", action="store_true", help="delete each zip once staged")
    one = sub.add_parser("stage-one", help=argparse.SUPPRESS)
    common(one)
    one.add_argument("--unit", required=True)
    one.add_argument("--src", required=True)
    one.add_argument("--threads", type=int)
    one.add_argument("--low-disk", action="store_true")
    c = sub.add_parser("choose", help="each NWI_ID once, from the staged units' indexes")
    common(c)
    c.add_argument("--groups", type=int, default=1)
    w = sub.add_parser("write", help="the state files of these target states")
    common(w, out=True)
    w.add_argument("--target", action="append", required=True)
    w.add_argument("--low-disk", action="store_true", help="delete each partition once written")
    a = sub.add_parser("assemble", help="the manifest, codes, projects and source files")
    common(a, out=True)
    a.add_argument("--live", help="merge the published manifest of the same release")
    a.add_argument("--threads", type=int)
    b = sub.add_parser("build", help="every step on one machine (a test run, or Alaska)")
    common(b, out=True)
    b.add_argument("--unit", action="append",
                   help="these units only (a test run, or AK on a workstation)")
    b.add_argument("--no-tiles", action="store_true")
    b.add_argument("--low-disk", action="store_true",
                   help="delete each zip once staged, the stage once written")
    b.add_argument("--threads", type=int)
    b.add_argument("--live", help="merge the published manifest of the same release "
                                  "(the wetlands/ prefix URL)")
    t = sub.add_parser("tiles", help="wetlands.pmtiles from the state files in --out-dir")
    common(t, out=True)
    t.add_argument("--live", help="read the release from here (the published wetlands/ prefix)")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
    elif args.cmd == "plan":
        print(json.dumps(plan(args.groups, args.upstream_out)))
    elif args.cmd == "stage":
        stage(read_upstream(args.upstream), args.work_dir, args.unit, args.low_disk,
              args.memory, args.threads)
    elif args.cmd == "stage-one":
        stage_one(args.unit, json.loads(args.src), args.work_dir, args.low_disk, args.memory,
                  args.threads)
    elif args.cmd == "choose":
        con = connect(args.work_dir, args.memory)
        d = choose(con, args.work_dir, args.groups)
        con.close()
        print(json.dumps({"groups": d["groups"], "ids": d["ids"],
                          "copies_dropped": d["copies_dropped"]}))
    elif args.cmd == "write":
        write(read_upstream(args.upstream), args.work_dir, args.out_dir, args.target,
              args.low_disk, args.memory)
    elif args.cmd == "assemble":
        assemble(read_upstream(args.upstream), args.work_dir, args.out_dir, args.memory,
                 args.live, args.threads)
    elif args.cmd == "tiles":
        tiles(args.work_dir, args.out_dir, args.memory, args.live)
    else:
        build(args.work_dir, args.out_dir, args.unit, args.no_tiles, args.low_disk,
              args.memory, args.threads, args.live)


if __name__ == "__main__":
    main()
