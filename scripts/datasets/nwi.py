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
The gate checks that NWI_ID is unique within each unit and that every
NWI_ID read is kept exactly once; _manifest.json counts the copies dropped,
the IDs whose kept copy came from a neighbour's download, and the IDs with
more than one home copy. Alaska is built on its own (build-ak below): no
other unit reaches it, so its IDs are deduplicated among its own rows.

The gate also checks each unit's rows against the geodatabase's feature
count, every ATTRIBUTE against the code table (an unknown code is counted
and left undecoded, failing the build above 0.1% of rows), and that the
state files add up to the IDs kept.

Usage:
  python3 scripts/datasets/nwi.py upstream
  python3 scripts/datasets/nwi.py build --work-dir work --out-dir out/wetlands \\
      [--unit RI --unit CT --unit MA] [--no-tiles] [--low-disk]
  # Alaska (an 11.8 GB zip): on a workstation, scripts/datasets/nwi_publish_ak.sh
  python3 scripts/datasets/nwi.py build --work-dir work --out-dir out/wetlands --unit AK
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
from datasets.gaul import verify  # noqa: E402
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


def to_fgb(gdb: str, layer: str, dest: Path, columns: list[str]) -> None:
    """One layer through GDAL to FlatGeobuf (any GDAL build writes it, the
    Parquet driver needs Arrow): curves cut into segments, 2D, NAD83 lon/lat
    with longitudes in [0, 360]. Every feature is kept."""
    dest.unlink(missing_ok=True)
    subprocess.run(["ogr2ogr", "-f", "FlatGeobuf", "-t_srs", WRAP_SRS,
                    "-nlt", "CONVERT_TO_LINEAR", "-nlt", "PROMOTE_TO_MULTI", "-dim", "XY",
                    "-lco", "SPATIAL_INDEX=NO", "-nln", layer,
                    "-sql", f'SELECT {", ".join(columns)} FROM "{layer}"',
                    str(dest), gdb], check=True)


# ---------- build ----------

def connect(work: Path, memory: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(work / "build.duckdb"))
    con.execute(f"INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs; "
                f"SET temp_directory = '{work / 'spill'}'; SET memory_limit = '{memory}'; "
                "SET preserve_insertion_order = false")
    return con


def load_states(con) -> None:
    con.execute(f"""CREATE OR REPLACE TABLE states AS
        SELECT STUSPS AS state, NAME AS name, ST_SetCRS(geometry::GEOMETRY, '') AS g
        FROM read_parquet('{TIGER_STATES}')""")


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
    dest = folder / "codes.parquet"
    con.execute(f"COPY (SELECT * FROM codes ORDER BY ATTRIBUTE) TO '{dest}' "
                "(FORMAT PARQUET, COMPRESSION ZSTD)")
    return {"file": dest.name, "rows": n, "bytes": dest.stat().st_size, "sha256": sha256(dest),
            "source": CODES_URL, "etag": codes.get("etag")}


def stage_unit(con, unit: str, src: dict, work: Path, low_disk: bool) -> dict:
    """One unit's wetlands to work/stage/<unit>/target=<XX>/part_0.parquet:
    repaired, unwrapped, each row with the state of its interior point,
    whether it is a home copy, partitioned by its target state so a state's
    file reads only its own rows; its projects to <unit>.projects.parquet.
    <unit>.json marks a unit done (a rerun skips it)."""
    stage = work / "stage" / unit
    done = work / "stage" / f"{unit}.json"
    if done.is_file() and stage.is_dir():
        return json.loads(done.read_text())
    t0 = time.monotonic()
    shutil.rmtree(stage, ignore_errors=True)
    stage.parent.mkdir(parents=True, exist_ok=True)
    archive = fetch(zip_url(unit), work / "zips" / f"{unit}.zip", src.get("bytes"))
    gdb, layers = gdb_layers(archive)
    wet, meta = pick(layers, unit)
    raw = work / "stage" / f"{unit}.raw.fgb"
    to_fgb(gdb, wet, raw, ["NWI_ID", "ATTRIBUTE", "WETLAND_TYPE", "ACRES"])
    if meta:
        fgb = work / "stage" / f"{unit}.projects.fgb"
        to_fgb(gdb, meta, fgb, [c for c in PROJECT_COLUMNS if c != "DATA_SOURCE"]
               + (["DATA_SOURCE"] if unit == "PacTrust" else []))
        con.execute(f"""COPY (SELECT * EXCLUDE (geom), ST_AsWKB(geom) AS geom FROM ST_Read('{fgb}'))
                        TO '{work / "stage" / f"{unit}.projects.parquet"}' (FORMAT PARQUET)""")
        fgb.unlink()
    if low_disk:
        archive.unlink()
    own = UNIT_STATES.get(unit, (unit,))
    own_sql = ", ".join(f"'{s}'" for s in own)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE raw AS
        SELECT NWI_ID, ATTRIBUTE, WETLAND_TYPE, ACRES, ST_SetCRS(geom::GEOMETRY, '') AS g,
               NOT ST_IsValid(ST_SetCRS(geom::GEOMETRY, '')) AS invalid
        FROM ST_Read('{raw}')""")
    rows, ids, nulls = con.execute(
        "SELECT count(*), count(DISTINCT NWI_ID), count(*) FILTER (WHERE NWI_ID IS NULL) "
        "FROM raw").fetchone()
    expected = layers[wet]
    problems = []
    if rows != expected:
        problems.append(f"{rows:,} rows, the geodatabase has {expected:,}")
    if nulls:
        problems.append(f"{nulls} rows without NWI_ID")
    if ids + (1 if nulls else 0) != rows:
        problems.append(f"NWI_ID: {rows - ids} repeated within the unit")
    if problems:
        sys.exit(f"{unit}: gate failed: " + "; ".join(problems))
    valid = ("CASE WHEN invalid THEN ST_Multi(ST_CollectionExtract(ST_MakeValid(g), 3)) "
             "ELSE g END")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE fixed AS
        SELECT NWI_ID, ATTRIBUTE, WETLAND_TYPE, ACRES, invalid,
               ST_XMin(v) < 180 AND ST_XMax(v) > 180 AS across,
               {UNWRAP.format(g='v')} AS geom
        FROM (SELECT *, {valid} AS v FROM raw)""")
    con.execute("DROP TABLE raw")
    empty = con.execute("SELECT count(*) FROM fixed WHERE geom IS NULL OR ST_IsEmpty(geom)"
                        ).fetchone()[0]
    if empty:
        sys.exit(f"{unit}: {empty} empty geometries after repair")
    # The state of each row's interior point; a point on a border between two
    # states takes the first code.
    con.execute("""CREATE OR REPLACE TEMP TABLE pts AS
        SELECT NWI_ID, ST_PointOnSurface(geom) AS p FROM fixed""")
    con.execute("""CREATE OR REPLACE TEMP TABLE located AS
        SELECT pts.NWI_ID, min(s.state) AS state FROM pts
        LEFT JOIN states s ON ST_Intersects(s.g, pts.p) GROUP BY 1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE nearest AS
        SELECT pts.NWI_ID, arg_min(s.state, ST_Distance(s.g, pts.p)) AS state,
               min(ST_Distance(s.g, pts.p)) AS d
        FROM pts JOIN located l USING (NWI_ID) JOIN states s ON s.state IN ({own_sql})
        WHERE l.state IS NULL GROUP BY 1""")
    um = "'UM'" if "UM" in own else "n.state"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE staged AS
        SELECT f.NWI_ID, f.ATTRIBUTE, f.WETLAND_TYPE, f.ACRES, '{unit}' AS source_unit,
               l.state AS point_state,
               CASE WHEN l.state IS NOT NULL THEN l.state
                    WHEN n.d <= {NEAREST_DEGREES} THEN n.state ELSE {um} END AS target,
               (l.state IS NULL OR l.state IN ({own_sql})) AS home,
               f.invalid, f.across, f.geom
        FROM fixed f JOIN located l USING (NWI_ID) LEFT JOIN nearest n USING (NWI_ID)""")
    stats = con.execute("""
        SELECT count(*), count(*) FILTER (WHERE invalid), count(*) FILTER (WHERE across),
               count(*) FILTER (WHERE point_state IS NULL), count(*) FILTER (WHERE NOT home),
               count(*) FILTER (WHERE target IS NULL)
        FROM staged""").fetchone()
    if stats[5]:
        sys.exit(f"{unit}: {stats[5]} rows without a target state")
    # part_<i>.parquet, not data_<i>: the globs below then never match the
    # ._* files macOS writes beside each file on exFAT (T9).
    con.execute(f"""COPY (SELECT * EXCLUDE (invalid, across) FROM staged)
        TO '{stage}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (target),
                      FILENAME_PATTERN 'part_{{i}}')""")
    con.execute("DROP TABLE staged")
    for t in ("fixed", "pts", "located", "nearest"):
        con.execute(f"DROP TABLE {t}")
    raw.unlink()
    entry = {"unit": unit, "states": list(own), "layer": wet, "projects_layer": meta,
             "rows": stats[0], "gdb_rows": expected, "repaired_invalid": stats[1],
             "split_at_180": stats[2], "point_outside_states": stats[3],
             "rows_in_a_neighbour": stats[4], **src}
    done.write_text(json.dumps(entry))
    print(f"  {unit}: {stats[0]:,} rows, {stats[4]:,} in a neighbour, {stats[3]} outside "
          f"every state, {stats[1]} repaired, {time.monotonic() - t0:.0f} s", flush=True)
    return entry


def staged(work: Path, target: str = "*") -> str:
    """The staged rows of every unit (all targets, or one) as read_parquet:
    `target` comes from the Hive folder."""
    return (f"read_parquet('{work / 'stage'}/*/target={target}/part_*.parquet', "
            "hive_partitioning = true)")


def choose(con, work: Path) -> dict:
    """Each NWI_ID once: the home copy, then the largest ACRES, then the
    unit name. Table `win` (NWI_ID, source_unit, target)."""
    rows = staged(work)
    con.execute(f"""CREATE OR REPLACE TABLE win AS
        SELECT NWI_ID, source_unit, target, home FROM (
            SELECT NWI_ID, source_unit, target, home,
                   row_number() OVER (PARTITION BY NWI_ID
                                      ORDER BY home DESC, ACRES DESC NULLS LAST,
                                               source_unit) AS k
            FROM {rows})
        WHERE k = 1""")
    copies, ids = con.execute(f"SELECT count(*), count(DISTINCT NWI_ID) FROM {rows}").fetchone()
    kept = con.execute("SELECT count(*) FROM win").fetchone()[0]
    if kept != ids:
        sys.exit(f"dedup: {kept:,} kept for {ids:,} NWI_IDs")
    from_neighbour = con.execute("SELECT count(*) FROM win WHERE NOT home").fetchone()[0]
    multi_home = con.execute(f"""
        SELECT count(*) FROM (SELECT NWI_ID FROM {rows} WHERE home
                              GROUP BY 1 HAVING count(*) > 1)""").fetchone()[0]
    return {"copies_read": copies, "ids": ids, "copies_dropped": copies - ids,
            "kept_from_a_neighbour": from_neighbour, "ids_with_several_home_copies": multi_home}


def write_states(con, work: Path, rel: Path, crs: dict,
                 low_disk: bool) -> tuple[dict[str, dict], dict]:
    """One file per target state, from the kept rows of every unit's
    partition for it; each partition deleted once written (low_disk). Also
    the code gate: rows whose ATTRIBUTE is not in the code table."""
    targets = [r[0] for r in con.execute("SELECT DISTINCT target FROM win ORDER BY 1").fetchall()]
    staged_units = {p.name for p in (work / "stage").iterdir() if p.is_dir()}
    foreign = [t for t in targets if t in WORKSTATION_UNITS and t not in staged_units]
    if foreign:
        sys.exit(f"rows for {foreign} from other units: that state is built on its own "
                 "(nwi_publish_ak.sh) and could not deduplicate them")
    out = {}
    t0 = time.monotonic()
    cols = ", ".join(f'"{c}"' for c in COLUMNS)
    decoded = ", ".join(f"c.{d}" for d in DECODED)
    unknown, codes_seen = 0, set()
    for i, st in enumerate(targets, 1):
        con.execute(f"""CREATE OR REPLACE TEMP TABLE src AS
            SELECT {cols}, ST_SetCRS(geom, '{CRS}') AS geometry, {BBOX.replace('geometry', 'geom')}
            FROM (SELECT s.NWI_ID, s.ATTRIBUTE, s.WETLAND_TYPE, s.ACRES, {decoded},
                         s.source_unit, s.geom
                  FROM {staged(work, st)} s
                  SEMI JOIN (SELECT NWI_ID, source_unit FROM win WHERE target = '{st}') w
                      ON w.NWI_ID = s.NWI_ID AND w.source_unit = s.source_unit
                  LEFT JOIN codes c ON c.ATTRIBUTE = s.ATTRIBUTE)""")
        bad = con.execute("SELECT count(*) FROM src WHERE ATTRIBUTE NOT IN "
                          "(SELECT ATTRIBUTE FROM codes)").fetchone()[0]
        unknown += bad
        codes_seen |= {r[0] for r in con.execute(
            "SELECT DISTINCT ATTRIBUTE FROM src WHERE ATTRIBUTE NOT IN "
            "(SELECT ATTRIBUTE FROM codes) LIMIT 50").fetchall()}
        dest = rel / f"state={st}" / "wetlands.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        e = write_src(con, dest, crs)
        verify(con, dest.parent, [dest.stem])
        check_crs(con, dest)
        # The longitudes it covers, for the catalog's extent (Alaska and the
        # Pacific cross 180), measured while the file is local.
        rows = f"read_parquet('{dest}', hive_partitioning = false)"
        ymin, ymax = con.execute(f"SELECT min(bbox.ymin), max(bbox.ymax) FROM {rows}").fetchone()
        out[st] = {**e, "path": f"state={st}/wetlands.parquet", "name": state_name(con, st),
                   "lon_intervals": lon_intervals(con, rows), "ymin": ymin, "ymax": ymax}
        if low_disk:
            for part in (work / "stage").glob(f"*/target={st}"):
                shutil.rmtree(part)
        print(f"  [{i}/{len(targets)}] {st}: {e['features']:,} rows, {e['bytes'] / 1e6:,.1f} MB, "
              f"{e['row_groups']} groups, {time.monotonic() - t0:.0f} s", flush=True)
    con.execute("DROP TABLE IF EXISTS src")
    total = sum(e["features"] for e in out.values())
    if total and unknown / total > UNKNOWN_CODE_SHARE:
        sys.exit(f"gate: {unknown:,} of {total:,} rows have an ATTRIBUTE not in the code table: "
                 f"{sorted(codes_seen)[:50]}")
    return out, {"rows_with_unknown_code": unknown, "unknown_codes": sorted(codes_seen)[:50]}


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
    """The tiles stage: wetlands.pmtiles from every state file of the
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
    map is kept, marked stale, until the tiles stage redraws it."""
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


def build(work: Path, out: Path, units: list[str] | None, no_tiles: bool, low_disk: bool,
          memory: str, live: str | None) -> None:
    t0 = time.monotonic()
    work.mkdir(parents=True, exist_ok=True)
    (work / "spill").mkdir(exist_ok=True)
    src = upstream()
    rel_name = src["release"]
    rel = out / "nwi" / rel_name
    rel.mkdir(parents=True, exist_ok=True)
    todo = units or [u for u in src["units"] if u not in WORKSTATION_UNITS]
    missing = sorted(set(todo) - set(src["units"]))
    if missing:
        sys.exit(f"units not on the download page: {missing}")
    print(f"NWI release {rel_name} ({src['last_updated']}), {len(todo)} units", flush=True)
    con = connect(work, memory)
    crs = projjson(con)
    load_states(con)
    codes = load_codes(con, work, rel, src["codes"])
    entries = [stage_unit(con, u, src["units"][u], work, low_disk) for u in todo]
    dedup = choose(con, work)
    print(f"  dedup: {dedup['copies_read']:,} rows read, {dedup['ids']:,} NWI_IDs kept, "
          f"{dedup['copies_dropped']:,} copies dropped, {dedup['kept_from_a_neighbour']:,} "
          f"kept from a neighbour's download, {dedup['ids_with_several_home_copies']:,} with "
          f"several home copies", flush=True)
    files, codes_gate = write_states(con, work, rel, crs, low_disk)
    total = sum(f["features"] for f in files.values())
    if total != dedup["ids"]:
        sys.exit(f"gate: the state files hold {total:,} rows, {dedup['ids']:,} NWI_IDs were kept")
    live_m = live_manifest(rel_name, live)
    live_projects = (f"{live}/nwi/{rel_name}/{live_m['projects']['file']}"
                     if live_m and live_m.get("projects") else None)
    projects = write_projects(con, work, rel, crs, todo, live_projects)
    con.close()
    if low_disk:
        shutil.rmtree(work / "stage")
    manifest = {
        "dataset": "nwi", "release": rel_name, "last_updated": src["last_updated"],
        "crs": CRS, "total_features": total,
        # One part per build: the runner's (every unit but Alaska) and the
        # workstation's (Alaska), each deduplicated on its own.
        "parts": [{"units": sorted(todo), "dedup": dedup, "gate": codes_gate,
                   "built": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}],
        "units": entries,
        "files": [{"state": s, **e} for s, e in files.items()],
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
    if live:
        try:
            prev = json.loads(curl(f"{live}/_source.json")).get("nwi", {})
        except subprocess.CalledProcessError:
            prev = {}
        was = prev.get("units", {}) if prev.get("release") == rel_name else {}
        for u in set(src["units"]) - set(todo):
            src["units"][u] = was.get(u)
    (out / "_source.json").write_text(json.dumps({"nwi": src}, indent=2) + "\n")
    if not no_tiles:
        tiles(work, out, memory)
    print(f"  done: {len(files)} state files, {total:,} rows, {time.monotonic() - t0:.0f} s",
          flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="print the release on the FWS download page")
    b = sub.add_parser("build", help="fetch, stage, deduplicate and write the state files")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help="the wetlands/ prefix")
    b.add_argument("--unit", action="append",
                   help="these units only (a test run, or AK on a workstation)")
    b.add_argument("--no-tiles", action="store_true")
    b.add_argument("--low-disk", action="store_true",
                   help="delete each zip once staged, the stage once written (a runner)")
    b.add_argument("--memory", default="6GB", help="DuckDB memory_limit")
    b.add_argument("--live", help="merge the published manifest of the same release "
                                  "(the wetlands/ prefix URL)")
    t = sub.add_parser("tiles", help="wetlands.pmtiles from the state files in --out-dir")
    t.add_argument("--work-dir", type=Path, default=Path("work"))
    t.add_argument("--out-dir", type=Path, required=True, help="the wetlands/ prefix")
    t.add_argument("--memory", default="6GB", help="DuckDB memory_limit")
    t.add_argument("--live", help="read the release from here (the published wetlands/ prefix)")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
        return
    if args.cmd == "tiles":
        tiles(args.work_dir, args.out_dir, args.memory, args.live)
        return
    build(args.work_dir, args.out_dir, args.unit, args.no_tiles, args.low_disk, args.memory,
          args.live)


if __name__ == "__main__":
    main()
