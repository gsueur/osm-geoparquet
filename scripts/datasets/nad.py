#!/usr/bin/env python3
"""
The US Department of Transportation's National Address Database (NAD) as
GeoParquet: one file of address points per state.

USDOT aggregates the address points that states, counties and cities choose
to share (participation is voluntary, so some states are missing and others
are partial) and publishes a release every few months on
data.transportation.gov: the text file (dataset fc2s-wawr, one CSV of 60
columns in a 8.5 GB zip) and a file geodatabase (yw36-suxr). This reads the
text file and writes:

  <out>/nad/r<release>/state=<XX>/addresses.parquet   one file per state, postal code
  <out>/nad/r<release>/_manifest.json     rows, sizes, sha256, gate, per state
  <out>/nad/r<release>/_coverage.json     per state: rows, counties, source agencies;
                                          the states and territories with no data
  <out>/nad/r<release>/addresses_h3.parquet   address counts on H3 cells, res 4 to 8
  <out>/nad/r<release>/addresses_h3.pmtiles   those cells as a coverage map
  <out>/nad/r<release>/ATTRIBUTION.txt
  <out>/index.json, <out>/_source.json

No national file: 99 million rows in one file serve no reader better than
the states read together, through the anonymous S3 endpoint
(s3://parquetry/addresses/nad/r<release>/state=*/addresses.parquet), as OSM's
themes are.

Columns are NAD's, names unchanged (OID_ to DataSet_ID), plus `full_address`,
a normalized one-line address for matching (normalize() says how), a
`bbox` declared as the GeoParquet covering and the point geometry. NAD gives
Longitude and Latitude in decimal degrees on WGS 84: its geodatabase stores
the points in Web Mercator (WKID 102100/3857, on WGS 84) and the text file's
degrees are those points. The geometry is written as OGC:CRS84. Rows are
sorted on a Hilbert curve within each state and cut into row groups of at
most 4 MiB of geometry and strings (geoconnex.py's rewrite).

The gate: every row of the CSV is accounted for (in a state file, or counted
as without usable coordinates), the states' rows add up to the CSV's, a
point outside its state's TIGER/Line 2026 box is counted per state (kept,
not dropped), OID_ is unique, and UUID duplicates are counted.

Usage:
  python3 scripts/datasets/nad.py upstream
  # the zip is Deflate64; a rerun reuses the extracted CSV, the loaded
  # table (work/build.duckdb) and every state file that verifies
  uv run --project scripts --with zipfile-inflate64 \\
      python scripts/datasets/nad.py build --work-dir work --out-dir out/addresses
"""

from __future__ import annotations

import argparse
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
from datasets.geoconnex import sha256, verify, write, write_json  # noqa: E402

SOCRATA = "https://data.transportation.gov"
TXT_ID = "fc2s-wawr"      # NAD Text File
GDB_ID = "yw36-suxr"      # NAD File Geodatabase: its zip name carries the release number
TXT_URL = f"{SOCRATA}/download/{TXT_ID}/application%2Fx-zip-compressed"
PAGE = "https://www.transportation.gov/gis/national-address-database"
PUBLIC = "https://parquetry.geomermaids.com"
TIGER_STATES = f"{PUBLIC}/census/tiger/2026/substategeo/state.parquet"
TIGER_COUNTIES = f"{PUBLIC}/census/tiger/2026/substategeo/county.parquet"

# The states, DC and the territories TIGER/Line 2026 has, by postal code: a
# state NAD has no rows for is listed as missing.
TERRITORIES = {"AS", "GU", "MP", "PR", "VI"}

# NAD's 60 columns in file order, and the type each is read as. Everything a
# number could mangle (ZIP codes, address numbers with prefixes) stays text.
COLUMNS = [
    ("OID_", "BIGINT"), ("AddNum_Pre", "VARCHAR"), ("Add_Number", "BIGINT"),
    ("AddNum_Suf", "VARCHAR"), ("AddNo_Full", "VARCHAR"), ("St_PreMod", "VARCHAR"),
    ("St_PreDir", "VARCHAR"), ("St_PreTyp", "VARCHAR"), ("St_PreSep", "VARCHAR"),
    ("St_Name", "VARCHAR"), ("St_PosTyp", "VARCHAR"), ("St_PosDir", "VARCHAR"),
    ("St_PosMod", "VARCHAR"), ("StNam_Full", "VARCHAR"), ("Building", "VARCHAR"),
    ("Floor", "VARCHAR"), ("Unit", "VARCHAR"), ("Room", "VARCHAR"), ("Seat", "VARCHAR"),
    ("Addtl_Loc", "VARCHAR"), ("SubAddress", "VARCHAR"), ("LandmkName", "VARCHAR"),
    ("County", "VARCHAR"), ("Inc_Muni", "VARCHAR"), ("Post_City", "VARCHAR"),
    ("Census_Plc", "VARCHAR"), ("Uninc_Comm", "VARCHAR"), ("Nbrhd_Comm", "VARCHAR"),
    ("NatAmArea", "VARCHAR"), ("NatAmSub", "VARCHAR"), ("Urbnztn_PR", "VARCHAR"),
    ("PlaceOther", "VARCHAR"), ("PlaceNmTyp", "VARCHAR"), ("State", "VARCHAR"),
    ("Zip_Code", "VARCHAR"), ("Plus_4", "VARCHAR"), ("UUID", "VARCHAR"),
    ("AddAuth", "VARCHAR"), ("AddrRefSys", "VARCHAR"), ("Longitude", "DOUBLE"),
    ("Latitude", "DOUBLE"), ("NatGrid", "VARCHAR"), ("Elevation", "DOUBLE"),
    ("Placement", "VARCHAR"), ("AddrPoint", "VARCHAR"), ("Related_ID", "VARCHAR"),
    ("RelateType", "VARCHAR"), ("ParcelSrc", "VARCHAR"), ("Parcel_ID", "VARCHAR"),
    ("AddrClass", "VARCHAR"), ("Lifecycle", "VARCHAR"), ("Effective", "TIMESTAMP"),
    ("Expire", "TIMESTAMP"), ("DateUpdate", "TIMESTAMP"), ("AnomStatus", "VARCHAR"),
    ("LocatnDesc", "VARCHAR"), ("Addr_Type", "VARCHAR"), ("DeliverTyp", "VARCHAR"),
    ("NAD_Source", "VARCHAR"), ("DataSet_ID", "VARCHAR"),
]

# USPS Publication 28 abbreviations (Appendix C1, the common street suffixes,
# and the directionals), applied to NAD's spelled-out types and directions
# in full_address. A type not listed stays spelled out, upper-cased.
SUFFIXES = {
    "ALLEY": "ALY", "AVENUE": "AVE", "BOULEVARD": "BLVD", "BRIDGE": "BRG", "BYPASS": "BYP",
    "CAUSEWAY": "CSWY", "CENTER": "CTR", "CIRCLE": "CIR", "COURT": "CT", "COVE": "CV",
    "CREEK": "CRK", "CROSSING": "XING", "DRIVE": "DR", "EXPRESSWAY": "EXPY",
    "EXTENSION": "EXT", "FREEWAY": "FWY", "GARDENS": "GDNS", "GLEN": "GLN", "GROVE": "GRV",
    "HARBOR": "HBR", "HEIGHTS": "HTS", "HIGHWAY": "HWY", "HILL": "HL", "HOLLOW": "HOLW",
    "ISLAND": "IS", "JUNCTION": "JCT", "LANDING": "LNDG", "LANE": "LN", "LOOP": "LOOP",
    "MANOR": "MNR", "MEADOWS": "MDWS", "MOUNTAIN": "MTN", "PARKWAY": "PKWY", "PIKE": "PIKE",
    "PLACE": "PL", "PLAZA": "PLZ", "POINT": "PT", "RIDGE": "RDG", "ROAD": "RD", "ROUTE": "RTE",
    "RUN": "RUN", "SQUARE": "SQ", "STREET": "ST", "TERRACE": "TER", "TRAIL": "TRL",
    "TURNPIKE": "TPKE", "VALLEY": "VLY", "VIEW": "VW", "VILLAGE": "VLG", "WAY": "WAY",
}
DIRECTIONS = {"NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W", "NORTHEAST": "NE",
              "NORTHWEST": "NW", "SOUTHEAST": "SE", "SOUTHWEST": "SW"}
H3_RESOLUTIONS = (4, 6, 8)
# Coverage map bands: (min zoom, max zoom, H3 resolution of the cells drawn).
H3_BANDS = ((0, 4, 4), (5, 7, 6), (8, 11, 8))


# ---------- source ----------

def get_json(url: str) -> dict:
    out = subprocess.run(["curl", "-fsSL", "--retry", "3", "--max-time", "120", url],
                         check=True, capture_output=True, text=True).stdout
    return json.loads(out)


def upstream() -> dict:
    """The release now on data.transportation.gov: its number (from the
    geodatabase's zip name, NAD_r24.gdb.zip), compile date (from the
    description) and the text zip's size and update time."""
    txt = get_json(f"{SOCRATA}/api/views/{TXT_ID}.json")
    gdb = get_json(f"{SOCRATA}/api/views/{GDB_ID}.json")
    m = re.search(r"NAD_r(\d+)", gdb.get("blobFilename") or "")
    compiled = re.search(r"compiled on ([A-Z][a-z]+ \d{1,2}, \d{4})", txt.get("description") or "")
    if not m:
        sys.exit(f"no release number in {GDB_ID}'s file name {gdb.get('blobFilename')!r}")
    return {
        "release": int(m.group(1)),
        "compiled": compiled.group(1) if compiled else None,
        "txt_file": txt.get("blobFilename"),
        "txt_bytes": txt.get("blobFileSize"),
        "txt_updated": txt.get("rowsUpdatedAt"),
        "gdb_file": gdb.get("blobFilename"),
    }


def fetch(work: Path, size: int) -> Path:
    """The text zip, resumed with ranges; a copy of the right size is reused."""
    dest = work / "TXT.zip"
    if dest.is_file() and dest.stat().st_size == size:
        return dest
    work.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, 6):
        r = subprocess.run(["curl", "-fsSL", "--retry", "5", "--retry-delay", "15", "-C", "-",
                            "-o", str(dest), TXT_URL])
        if r.returncode == 0 and dest.stat().st_size == size:
            return dest
        print(f"  fetch attempt {attempt}: {dest.stat().st_size if dest.exists() else 0:,} "
              f"of {size:,} bytes", flush=True)
        time.sleep(30 * attempt)
    sys.exit(f"{TXT_URL}: got {dest.stat().st_size if dest.exists() else 0:,} bytes, "
             f"the dataset lists {size:,}")


def extract(archive: Path, work: Path) -> Path:
    """The one CSV in the zip (TXT/NAD_r24.txt, 42 GB), extracted once.

    USDOT compresses it with Deflate64 (zip method 9), which neither
    Python's zipfile nor macOS unzip, bsdtar or ditto reads. The
    zipfile-inflate64 package adds the method to zipfile:
    `uv run --project scripts --with zipfile-inflate64 ...`."""
    try:
        import zipfile_inflate64  # noqa: F401  registers Deflate64 with zipfile
    except ImportError:
        sys.exit("the NAD zip is Deflate64: run with `uv run --with zipfile-inflate64`")
    with zipfile.ZipFile(archive) as z:
        members = [i for i in z.infolist() if i.filename.lower().endswith(".txt")]
        if len(members) != 1:
            sys.exit(f"{archive.name}: {len(members)} .txt members, want 1")
        info = members[0]
        dest = work / Path(info.filename).name
        if dest.is_file() and dest.stat().st_size == info.file_size:
            return dest
        tmp = dest.with_suffix(".part")
        with z.open(info) as src, tmp.open("wb") as out:
            shutil.copyfileobj(src, out, 16 << 20)
    if tmp.stat().st_size != info.file_size:
        sys.exit(f"{info.filename}: extracted {tmp.stat().st_size:,} bytes, the zip lists "
                 f"{info.file_size:,}")
    tmp.rename(dest)
    return dest


# ---------- full_address ----------

def normalize_sql(col: str) -> str:
    """Upper case, punctuation to spaces, runs of spaces to one, trimmed."""
    return (f"trim(regexp_replace(regexp_replace(upper(coalesce({col}, '')), "
            f"'[^A-Z0-9 ]', ' ', 'g'), ' +', ' ', 'g'))")


def mapped_sql(col: str, table: dict[str, str]) -> str:
    cases = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in table.items())
    norm = normalize_sql(col)
    return f"(CASE {norm} {cases} ELSE {norm} END)"


def full_address_sql() -> str:
    """`full_address`, the one-line form to match against:

      <number> <pre-modifier> <pre-dir> <pre-type> <separator> <street name>
      <post-type> <post-dir> <post-modifier> <sub-address>, <city>, <state> <zip>

    the street parts in the FGDC order of a complete street name, so 'Old
    County Road' and 'County Road' stay apart

    from NAD's parts, upper-cased, punctuation removed, street types and
    directions in their USPS abbreviations (ROAD -> RD, NORTH -> N), empty
    parts skipped. The city is Post_City, else Inc_Muni. Normalize an input
    address the same way before comparing; AGENTS.md shows how."""
    street = " || ' ' || ".join([
        normalize_sql("AddNo_Full"), normalize_sql("St_PreMod"),
        mapped_sql("St_PreDir", DIRECTIONS), mapped_sql("St_PreTyp", SUFFIXES),
        normalize_sql("St_PreSep"), normalize_sql("St_Name"),
        mapped_sql("St_PosTyp", SUFFIXES), mapped_sql("St_PosDir", DIRECTIONS),
        normalize_sql("St_PosMod"), normalize_sql("SubAddress"),
    ])
    city = (f"(CASE WHEN {normalize_sql('Post_City')} NOT IN ('', 'NOT STATED') "
            f"THEN {normalize_sql('Post_City')} ELSE {normalize_sql('Inc_Muni')} END)")
    tail = f"{normalize_sql('State')} || ' ' || {normalize_sql('Zip_Code')}"
    return (f"trim(regexp_replace(trim(regexp_replace({street}, ' +', ' ', 'g')) || ', ' || {city} "
            f"|| ', ' || {tail}, '(, )+', ', ', 'g'), ', ')")


# ---------- build ----------

def load(con, csv_path: Path) -> int:
    """The CSV into a DuckDB table, typed, with full_address and the point.
    Strict: a malformed row stops the build rather than going missing."""
    columns = "{" + ", ".join(f"'{n}': '{t}'" for n, t in COLUMNS) + "}"
    con.execute(f"""
        CREATE TABLE nad AS
        SELECT *,
               {full_address_sql()} AS full_address,
               CASE WHEN Longitude BETWEEN -180 AND 180 AND Latitude BETWEEN -90 AND 90
                         AND NOT (Longitude = 0 AND Latitude = 0)
                    THEN ST_Point(Longitude, Latitude) END AS geom
        FROM read_csv('{csv_path}', header = true, columns = {columns},
                      delim = ',', quote = '"', escape = '"', strict_mode = true,
                      parallel = true)""")
    return con.execute("SELECT count(*) FROM nad").fetchone()[0]


def csv_rows(csv_path: Path) -> int:
    """Lines in the file minus the header, counted apart from DuckDB's parse:
    the gate compares the two."""
    out = subprocess.run(["wc", "-l", str(csv_path)], check=True, capture_output=True,
                         text=True).stdout
    return int(out.split()[0]) - 1


def gate(con, total_lines: int) -> dict:
    """Account for every row; count what is odd without dropping it."""
    loaded = con.execute("SELECT count(*) FROM nad").fetchone()[0]
    if loaded != total_lines:
        sys.exit(f"gate: DuckDB read {loaded:,} rows, the file has {total_lines:,} lines of data")
    no_point = con.execute("SELECT count(*) FROM nad WHERE geom IS NULL").fetchone()[0]
    no_state = con.execute(
        "SELECT count(*) FROM nad WHERE geom IS NOT NULL AND (State IS NULL OR State !~ '^[A-Z]{2}$')"
    ).fetchone()[0]
    oid_dupes = con.execute(
        "SELECT count(*) - count(DISTINCT OID_) FROM nad").fetchone()[0]
    if oid_dupes:
        sys.exit(f"gate: {oid_dupes:,} duplicate OID_ values")
    uuid_dupes = con.execute("""
        SELECT count(*) FROM (SELECT UUID FROM nad WHERE UUID IS NOT NULL
                              GROUP BY UUID HAVING count(*) > 1)""").fetchone()[0]
    uuid_rows = con.execute("""
        SELECT coalesce(sum(n), 0) FROM (SELECT count(*) AS n FROM nad WHERE UUID IS NOT NULL
                                         GROUP BY UUID HAVING count(*) > 1)""").fetchone()[0]
    return {"csv_rows": total_lines, "without_point": no_point, "without_state": no_state,
            "oid_unique": True, "uuid_values_repeated": uuid_dupes, "uuid_rows_repeated": int(uuid_rows)}


def tiger_states(con) -> dict[str, dict]:
    con.execute(f"""
        CREATE OR REPLACE TABLE tiger AS
        SELECT STUSPS AS state, NAME AS name, bbox.xmin AS xmin, bbox.ymin AS ymin,
               bbox.xmax AS xmax, bbox.ymax AS ymax
        FROM read_parquet('{TIGER_STATES}')""")
    # Counties (and equivalents) per state, so a state's county coverage
    # reads as a share.
    con.execute(f"""
        CREATE OR REPLACE TABLE tiger_counties AS
        SELECT s.STUSPS AS state, count(*) AS counties
        FROM read_parquet('{TIGER_COUNTIES}') c
        JOIN read_parquet('{TIGER_STATES}') s ON s.STATEFP = c.STATEFP
        GROUP BY 1""")
    return {r[0]: {"name": r[1]} for r in con.execute("SELECT state, name FROM tiger").fetchall()}


def state_stats(con, compiled: str | None) -> dict[str, dict]:
    """Per state: rows, counties named (and TIGER's count), agencies, points
    outside the state's TIGER box (0.01 degree of slack for the NAD83 and
    WGS 84 difference and the box's FLOAT rounding; Alaska's box spans the
    antimeridian, so its check is loose), the latest DateUpdate up to the
    release's compile date, and the dates after it (provider typos such as
    3034 for 2034 stay in the files, counted here)."""
    cutoff = f"strptime('{compiled}', '%B %d, %Y')" if compiled else "current_timestamp"
    rows = con.execute(f"""
        SELECT n.State, count(*) AS rows,
               count(DISTINCT n.County) AS counties, any_value(tc.counties) AS tiger_counties,
               count(DISTINCT n.AddAuth) AS agencies,
               count(*) FILTER (WHERE t.state IS NOT NULL AND NOT (
                   ST_X(n.geom) BETWEEN t.xmin - 0.01 AND t.xmax + 0.01
                   AND ST_Y(n.geom) BETWEEN t.ymin - 0.01 AND t.ymax + 0.01)) AS outside,
               max(n.DateUpdate) FILTER (WHERE n.DateUpdate <= {cutoff})::DATE::VARCHAR,
               count(*) FILTER (WHERE n.DateUpdate > {cutoff}) AS future
        FROM nad n LEFT JOIN tiger t ON t.state = n.State
                   LEFT JOIN tiger_counties tc ON tc.state = n.State
        WHERE n.geom IS NOT NULL AND n.State ~ '^[A-Z]{{2}}$'
        GROUP BY n.State ORDER BY n.State""").fetchall()
    return {s: {"rows": r, "counties": c, "tiger_counties": tc, "agencies": a,
                "outside_state_box": o, "latest_update": u, "update_after_compile": f}
            for s, r, c, tc, a, o, u, f in rows}


# The columns of every state file, in order: NAD's 60, then what is added.
FILE_COLUMNS = [n for n, _ in COLUMNS] + ["full_address", "bbox", "geometry"]


def columns_of(dest: Path) -> list[str]:
    import pyarrow.parquet as pq
    return pq.ParquetFile(dest).schema_arrow.names


def check_columns(dest: Path) -> None:
    """A state file holds exactly FILE_COLUMNS: nothing a reader of the
    `state=XX/` path (pyarrow's Hive discovery) slipped in."""
    have = columns_of(dest)
    if have != FILE_COLUMNS:
        sys.exit(f"{dest}: columns {have[-4:]} ... differ from the expected "
                 f"{len(FILE_COLUMNS)}: extra {sorted(set(have) - set(FILE_COLUMNS))}, "
                 f"missing {sorted(set(FILE_COLUMNS) - set(have))}")


def written(con, dest: Path, expected: int) -> dict | None:
    """The entry of a state file a previous run finished, or None: a file is
    kept only if no temporary file sits beside it (the write ran to the end),
    it has exactly FILE_COLUMNS and it passes verify() with the expected rows."""
    if not dest.is_file() or dest.with_name(dest.stem + ".duckdb.parquet").exists():
        return None
    if columns_of(dest) != FILE_COLUMNS:
        return None
    try:
        verify(con, dest, expected)
    except SystemExit:
        return None
    groups, largest = con.execute(f"""
        SELECT count(*), max(b) FROM (SELECT row_group_id, sum(total_compressed_size) AS b
                                      FROM parquet_metadata('{dest}') GROUP BY 1)""").fetchone()
    return {"file": dest.name, "features": expected, "without_geometry": 0,
            "bytes": dest.stat().st_size, "row_groups": groups,
            "largest_row_group_bytes": largest, "sha256": sha256(dest)}


def write_states(con, rel: Path, states: dict[str, int]) -> dict[str, dict]:
    """One file per state, resuming: a state a previous run wrote is kept."""
    entries = {}
    t0 = time.monotonic()
    for i, (st, rows) in enumerate(states.items(), 1):
        dest = rel / f"state={st}" / "addresses.parquet"
        e = written(con, dest, rows)
        kept = e is not None
        if not kept:
            con.execute(f"CREATE OR REPLACE TEMP TABLE one AS SELECT * FROM nad "
                        f"WHERE State = '{st}' AND geom IS NOT NULL")
            e = write(con, "one", dest)
            check_columns(dest)
            verify(con, dest, rows)
        entries[st] = {**e, "path": f"state={st}/addresses.parquet"}
        print(f"  [{i}/{len(states)}] {st}: {e['features']:,} rows, {e['bytes'] / 1e6:,.1f} MB, "
              f"{e['row_groups']} groups{' (kept)' if kept else ''}, "
              f"{time.monotonic() - t0:.0f} s", flush=True)
    con.execute("DROP TABLE IF EXISTS one")
    return entries


def h3_cells(con, dest: Path) -> dict[int, int]:
    """Address counts per H3 cell at each resolution of H3_RESOLUTIONS, with
    the cell outline: the coverage of the release at a glance."""
    parts = " UNION ALL ".join(
        f"SELECT {r} AS h3_res, h3_latlng_to_cell(ST_Y(geom), ST_X(geom), {r}) AS cell, State "
        f"FROM nad WHERE geom IS NOT NULL" for r in H3_RESOLUTIONS)
    con.execute(f"""
        COPY (
          WITH c AS ({parts}),
          g AS (SELECT h3_res, cell, count(*) AS addresses, mode(State) AS state
                FROM c GROUP BY ALL)
          SELECT h3_h3_to_string(cell) AS h3, h3_res, addresses, state,
                 ST_GeomFromWKB(h3_cell_to_boundary_wkb(cell)) AS geometry
          FROM g ORDER BY h3_res, cell)
        TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    return dict(con.execute(f"SELECT h3_res, count(*) FROM read_parquet('{dest}') "
                            "GROUP BY 1 ORDER BY 1").fetchall())


def h3_tiles(con, cells: Path, work: Path, dest: Path) -> dict:
    """One band per resolution, joined into one archive by pmtiles.pyramid()."""
    bands = []
    for lo, hi, res in H3_BANDS:
        src = work / f"h3_r{res}.parquet"
        con.execute(f"COPY (SELECT * EXCLUDE (h3_res) FROM read_parquet('{cells}') "
                    f"WHERE h3_res = {res}) TO '{src}' (FORMAT PARQUET)")
        part = work / f"h3_r{res}.pmtiles"
        pmtiles.tile(src, part, layer=f"cells_r{res}", min_zoom=lo, max_zoom=hi,
                     properties=["h3", "addresses", "state"], verbatim=True)
        bands.append((lo, hi, part, f"cells_r{res}"))
    entry = pmtiles.pyramid(bands, dest)
    entry["bands"] = [{"min_zoom": lo, "max_zoom": hi, "h3_res": res, "layer": f"cells_r{res}"}
                      for lo, hi, res in H3_BANDS]
    return entry


def attribution(src: dict, release: int) -> str:
    return f"""\
US National Address Database (NAD), release {release}
Compiled {src.get('compiled') or 'see the source page'} by the US Department of Transportation.
Republished as GeoParquet by Geomermaids: https://parquetry.geomermaids.com/addresses/

Source: {SOCRATA}/d/{TXT_ID} ({src.get('txt_file')}, {src.get('txt_bytes'):,} bytes)
About the NAD: {PAGE}

Licence: public domain (a work of the US Government), with no restriction on
use. Credit "U.S. Department of Transportation, National Address Database,
release {release}". Each record's AddAuth, NAD_Source and DataSet_ID name the
agency that provided it.

USDOT's disclaimer: the NAD is aggregated from state data, which is in turn
aggregated from local data, and participation is voluntary. Not every state
contributes, and a contributing state's coverage can be partial
(_coverage.json lists both). USDOT makes no warranty as to the accuracy or
completeness of the data.

Changes made here: columns and values unchanged; added `full_address` (a
normalized one-line address), `bbox` and the point `geometry` built from
Longitude and Latitude (WGS 84). Rows without usable coordinates and rows
without a state code are counted in _manifest.json and not written.
"""


def connect(work: Path) -> duckdb.DuckDBPyConnection:
    """The build database, kept between runs: a rerun after a failure reuses
    the loaded table instead of reading the 42 GB CSV again."""
    con = duckdb.connect(str(work / "build.duckdb"))
    con.execute(f"INSTALL spatial; LOAD spatial; INSTALL h3 FROM community; LOAD h3; "
                f"INSTALL httpfs; LOAD httpfs; SET temp_directory = '{work / 'spill'}'; "
                # Leaves room on a 16 GB runner for the Arrow copy of the
                # largest state that geoconnex.rewrite() holds while it cuts.
                "SET memory_limit = '6GB'; SET preserve_insertion_order = false")
    return con


def build(work: Path, out: Path, states_only: list[str] | None, no_tiles: bool,
          low_disk: bool = False) -> None:
    t0 = time.monotonic()
    src = upstream()
    rel_name = f"r{src['release']}"
    rel = out / "nad" / rel_name
    print(f"NAD release {src['release']}, compiled {src['compiled']}", flush=True)
    archive = fetch(work, src["txt_bytes"])
    csv_path = extract(archive, work)
    if low_disk:
        archive.unlink()
    print(f"  {csv_path.name}: {csv_path.stat().st_size / 1e9:,.1f} GB, "
          f"{time.monotonic() - t0:.0f} s", flush=True)

    con = connect(work)
    lines = csv_rows(csv_path)
    have = con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'nad'").fetchone()[0]
    if have and con.execute("SELECT count(*) FROM nad").fetchone()[0] == lines:
        print(f"  reusing the loaded table ({lines:,} rows)", flush=True)
    else:
        con.execute("DROP TABLE IF EXISTS nad")
        loaded = load(con, csv_path)
        print(f"  loaded {loaded:,} rows, {time.monotonic() - t0:.0f} s", flush=True)
    g = gate(con, lines)
    if low_disk:
        csv_path.unlink()
    tiger = tiger_states(con)
    stats = state_stats(con, src["compiled"])
    unknown = sorted(set(stats) - set(tiger))
    if unknown:
        sys.exit(f"gate: state codes not in TIGER/Line 2026: {unknown}")
    written = sum(s["rows"] for s in stats.values())
    if written + g["without_point"] + g["without_state"] != lines:
        sys.exit(f"gate: {written:,} in state files + {g['without_point']:,} without a point + "
                 f"{g['without_state']:,} without a state != {lines:,}")
    print(f"  gate: {lines:,} rows = {written:,} in {len(stats)} states + "
          f"{g['without_point']:,} without a point + {g['without_state']:,} without a state",
          flush=True)

    todo = {s: stats[s]["rows"] for s in sorted(stats) if not states_only or s in states_only}
    files = write_states(con, rel, todo)

    cells_entry, tiles_entry = None, None
    if not states_only:
        cells = rel / "addresses_h3.parquet"
        counts = h3_cells(con, cells)
        cells_entry = {"file": cells.name, "cells": counts, "bytes": cells.stat().st_size,
                       "sha256": sha256(cells)}
        if not no_tiles:
            tiles_entry = h3_tiles(con, cells, work, rel / "addresses_h3.pmtiles")
    con.close()

    missing = sorted(set(tiger) - set(stats))
    coverage = {
        "release": src["release"], "compiled": src["compiled"],
        "states": {s: {"name": tiger[s]["name"], **stats[s]} for s in sorted(stats)},
        "missing": [{"state": s, "name": tiger[s]["name"],
                     "territory": s in TERRITORIES} for s in missing],
    }
    write_json(rel / "_coverage.json", coverage)
    write_json(rel / "_manifest.json", {
        "dataset": "nad", "release": src["release"], "compiled": src["compiled"],
        "crs": "OGC:CRS84", "total_features": sum(e["features"] for e in files.values()),
        "gate": {**g, "rows_written": written, "states": len(stats),
                 "outside_state_box": sum(s["outside_state_box"] for s in stats.values())},
        "files": [{"state": s, **e} for s, e in files.items()],
        "h3": cells_entry, "pmtiles": tiles_entry,
        "glob": f"s3://parquetry/addresses/nad/{rel_name}/state=*/addresses.parquet",
    })
    (rel / "ATTRIBUTION.txt").write_text(attribution(src, src["release"]))
    write_json(out / "index.json", {"datasets": [
        {"path": f"nad/{rel_name}", "code": "nad",
         "name": f"National Address Database, release {src['release']}"}]})
    write_json(out / "_source.json", {"nad": src})
    print(f"  done: {len(files)} state files, {time.monotonic() - t0:.0f} s", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="print the release on data.transportation.gov")
    b = sub.add_parser("build", help="fetch, load, gate and write the state files")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help="the addresses/ prefix")
    b.add_argument("--state", action="append", help="these states only (a test run)")
    b.add_argument("--no-tiles", action="store_true", help="no coverage map")
    b.add_argument("--low-disk", action="store_true",
                   help="delete the zip once extracted and the CSV once loaded (a runner)")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
        return
    build(args.work_dir, args.out_dir, args.state, args.no_tiles, args.low_disk)


if __name__ == "__main__":
    main()
