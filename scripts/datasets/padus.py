#!/usr/bin/env python3
"""
USGS PAD-US 4.1, the Protected Areas Database of the United States, as
GeoParquet and PMTiles.

USGS Gap Analysis Project publishes PAD-US as one national file geodatabase
on ScienceBase (item 652d4fc5d34e44db0e2ee45e, DOI 10.5066/P96WBCHS). The
download sits behind a ScienceBase login, so it is fetched by hand, once per
release, and this builds from the unpacked geodatabase (--gdb). It writes:

  <out>/v<version>/combined.parquet       the Combined layer, national: fee,
                                           designation, easement, marine and
                                           proclamation areas in one layer; they
                                           overlap (a designation on fee land,
                                           a proclamation boundary around both)
  <out>/v<version>/combined/state=<XX>/combined.parquet   the same, per State_Nm
  <out>/v<version>/fee.parquet, designation.parquet, easement.parquet,
                   marine.parquet, proclamation.parquet    the source layers
  <out>/v<version>/combined.pmtiles       the Combined layer for web maps
  <out>/v<version>/lookups/<table>.parquet  PAD-US's code tables
  <out>/v<version>/PADUS4_1_Metadata_XML_FGDC.xml, VersHist_PADUS.txt   USGS's
                                           metadata and version history, as shipped
  <out>/v<version>/_manifest.json         rows, sizes, sha256, geometry fixes, gate
  <out>/v<version>/ATTRIBUTION.txt

Columns and values are PAD-US's, unchanged, plus `<column>_Desc` beside
every coded column, decoded from the geodatabase's own code tables (an
unknown code fails the build). Shape_Length and Shape_Area (Albers measures)
are left out; GIS_Acres is PAD-US's area. OBJECTID is the geodatabase's
feature number: unique within the release, not stable across releases
(Source_PAID and WDPA_Cd are the lasting identifiers, where present).

Geometry: PAD-US ships USGS's contiguous-US Albers on NAD83 for every state
and territory. GDAL reads it (DuckDB stops on the geodatabase's curves),
cuts curves into segments and reprojects to NAD83 lon/lat with longitudes
in [0, 360] (+lon_wrap=180), so the Aleutians and the Pacific monuments stay
continuous; DuckDB then shifts what lies east of 180 by -360 and splits the
few features that cross 180 there. The result is EPSG:4269, like the US
Census and NFHL layers on parquetry. Invalid polygons are repaired with
ST_MakeValid and counted. Each row gets a `bbox` declared as the GeoParquet
covering, rows are sorted on a Hilbert curve and cut into row groups of at
most 4 MiB of geometry (census.write_src, gaul.rewrite). Features are not
subdivided: one row is one PAD-US area as USGS delivers it (a few Alaska
units are over 10 MB each and sit in row groups of their own). Areas overlap
across rows, in the Combined layer too: an area question takes the union of
the geometries, never a sum of rows.

The gate: each layer's rows equal the geodatabase's feature count, OBJECTID
is unique, every coded value is in its code table, no geometry spans more
than 180 degrees after the fix, and the state files add up to the national
Combined file.

Usage:
  python3 scripts/datasets/padus.py upstream
  python3 scripts/datasets/padus.py build --gdb <PADUS4_1Geodatabase.gdb> \\
      --work-dir work --out-dir out/padus
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles
from datasets.census import BBOX, check_crs, projjson, sha256, write_src
from datasets.gaul import verify

VERSION = "4.1"
RELEASED = "2025-03-31"
DOI = "https://doi.org/10.5066/P96WBCHS"
ITEM_ID = "652d4fc5d34e44db0e2ee45e"
ITEM_URL = f"https://www.sciencebase.gov/catalog/item/{ITEM_ID}"
ZIP_NAME = "PADUS4_1Geodatabase.zip"
PROGRAM_PAGE = "https://www.usgs.gov/programs/gap-analysis-project/science/protected-areas"
STANDARDS = ("https://www.usgs.gov/programs/gap-analysis-project/science/"
             "pad-us-data-standards-manual")
CRS = "EPSG:4269"
# GDAL's target: NAD83 lon/lat, longitudes in [0, 360] so nothing wraps.
WRAP_SRS = "+proj=longlat +datum=NAD83 +lon_wrap=180 +no_defs"

# name -> geodatabase layer, title.
LAYERS = {
    "combined": {"layer": "PADUS4_1Combined_Proclamation_Marine_Fee_Designation_Easement",
                 "title": "Combined (all feature classes)"},
    "fee": {"layer": "PADUS4_1Fee", "title": "Fee (ownership)"},
    "designation": {"layer": "PADUS4_1Designation", "title": "Designation"},
    "easement": {"layer": "PADUS4_1Easement", "title": "Easement"},
    "marine": {"layer": "PADUS4_1Marine", "title": "Marine"},
    "proclamation": {"layer": "PADUS4_1Proclamation",
                     "title": "Proclamation and other planning boundaries"},
}
LOOKUPS = ["Category", "Agency_Type", "Agency_Name", "Designation_Type", "GAP_Status",
           "Public_Access", "IUCN_Category", "State_Name"]
# Coded column -> its code table. FeatClass takes Category's codes.
CODED = {
    "FeatClass": "Category", "Category": "Category",
    "Own_Type": "Agency_Type", "Own_Name": "Agency_Name",
    "Mang_Type": "Agency_Type", "Mang_Name": "Agency_Name",
    # EHoldTyp (easement holder type) is not decoded: it follows the National
    # Conservation Easement Database's codes (REG, blank...), not Agency_Type.
    "Des_Tp": "Designation_Type", "GAP_Sts": "GAP_Status", "Pub_Access": "Public_Access",
    "IUCN_Cat": "IUCN_Category", "State_Nm": "State_Name",
}
DROPPED = ["OGC_FID", "SHAPE_Length", "SHAPE_Area"]
# Shipped beside the geodatabase in the zip, published beside the files.
METADATA_XML = "PADUS4_1_Metadata_XML_FGDC.xml"
HISTORY = "VersHist_PADUS.txt"

# The tiles: the Combined layer, coloured on GAP_Sts. Steps of the ladder,
# tried in order; pmtiles.tile() fails a step that puts a tile over the cap.
TILE_PROPS = ["OBJECTID", "Unit_Nm", "Category", "Mang_Type", "Mang_Name", "Mang_Name_Desc",
              "Des_Tp", "Des_Tp_Desc", "GAP_Sts", "Pub_Access", "IUCN_Cat", "State_Nm",
              "GIS_Acres"]
TILE_MIN = ["Unit_Nm", "Mang_Name", "Des_Tp", "GAP_Sts", "Pub_Access"]
TILE_MAX_ZOOM = 12


def http_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "parquetry-padus/1"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def newer_items() -> list[str] | None:
    """Titles of ScienceBase PAD-US full inventories newer than VERSION, or
    None when the search cannot be read."""
    try:
        found = http_json("https://www.sciencebase.gov/catalog/items?format=json&max=50"
                          "&fields=title&q=PAD-US%20Full%20Inventory%20Database")
    except OSError as exc:
        print(f"  ScienceBase search: {exc}", file=sys.stderr)
        return None
    def version(title: str) -> tuple[int, ...] | None:
        m = re.search(r"PAD-US (\d+)\.(\d+) Full Inventory", title)
        return tuple(int(x) for x in m.groups()) if m else None
    ours = tuple(int(x) for x in VERSION.split("."))
    return sorted(t["title"] for t in found.get("items", [])
                  if (v := version(t.get("title", ""))) and v > ours)


def upstream() -> dict:
    """The release item's title, last update and zip size (public JSON, no
    login), and any newer PAD-US full inventory ScienceBase lists. A change
    means a human downloads the zip again."""
    item = http_json(f"{ITEM_URL}?format=json")
    zips = [f for f in item.get("files", []) if f.get("name") == ZIP_NAME]
    return {
        "version": VERSION,
        "item": ITEM_URL,
        "title": item.get("title"),
        "last_updated": (item.get("provenance") or {}).get("lastUpdated"),
        "zip": {"name": ZIP_NAME, "bytes": zips[0]["size"] if zips else None},
        "newer": newer_items(),
    }


def feature_count(gdb: Path, layer: str) -> int:
    out = subprocess.run(["ogrinfo", "-ro", "-so", "-json", str(gdb), layer],
                         check=True, capture_output=True, text=True).stdout
    return json.loads(out)["layers"][0]["featureCount"]


def linearized(gdb: Path, layer: str, work: Path) -> Path:
    """The layer through GDAL to FlatGeobuf: curves cut into segments, 2D,
    NAD83 lon/lat with longitudes in [0, 360], the feature number kept as
    OBJECTID. Every feature is kept (the gate counts them)."""
    dest = work / f"{layer}.fgb"
    dest.unlink(missing_ok=True)
    subprocess.run(["ogr2ogr", "-f", "FlatGeobuf", "-nlt", "CONVERT_TO_LINEAR", "-dim", "XY",
                    "-t_srs", WRAP_SRS, "-lco", "SPATIAL_INDEX=NO", "-nln", layer,
                    "-sql", f'SELECT FID AS OBJECTID, * FROM "{layer}"',
                    str(dest), str(gdb)], check=True)
    return dest


# Longitudes in [0, 360] back to [-180, 180]: a feature east of 180 moves by
# -360, one across 180 is cut there and its eastern pieces moved. g is valid.
UNWRAP = """CASE
    WHEN ST_XMax({g}) <= 180 THEN {g}
    WHEN ST_XMin({g}) >= 180 THEN ST_Translate({g}, -360, 0)
    ELSE ST_Multi(ST_CollectionExtract(ST_Union(
        ST_Intersection({g}, ST_MakeEnvelope(-180, -90, 180, 90)),
        ST_Translate(ST_Intersection({g}, ST_MakeEnvelope(180, -90, 540, 90)), -360, 0)), 3))
    END"""


def load_lookups(con, gdb: Path, folder: Path) -> dict:
    """PAD-US's code tables to lookups/<table>.parquet and DuckDB tables
    lk_<table>(code, dom). Their column names differ in case (Code, CODE)."""
    out = {}
    (folder / "lookups").mkdir(parents=True, exist_ok=True)
    for table in LOOKUPS:
        cols = [r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM ST_Read('{gdb}', layer = '{table}')").fetchall()]
        lower = {c.lower(): c for c in cols}
        extra = [c for c in cols if c.lower() not in ("code", "dom")]
        con.execute(f"""CREATE OR REPLACE TABLE lk_{table} AS
            SELECT "{lower['code']}" AS Code, "{lower['dom']}" AS Dom
                   {''.join(f', "{c}"' for c in extra)}
            FROM ST_Read('{gdb}', layer = '{table}') ORDER BY Code""")
        dest = folder / "lookups" / f"{table}.parquet"
        con.execute(f"COPY lk_{table} TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        n = con.execute(f"SELECT count(*), count(DISTINCT Code) FROM lk_{table}").fetchone()
        if n[0] != n[1]:
            sys.exit(f"{table}: {n[0] - n[1]} repeated codes")
        out[table] = {"file": f"lookups/{dest.name}", "rows": n[0], "bytes": dest.stat().st_size,
                      "sha256": sha256(dest), "columns": ["Code", "Dom", *extra]}
    return out


def load_layer(con, fgb: Path) -> dict:
    """The FlatGeobuf into table `src`: PAD-US's columns with `<col>_Desc`
    after each coded one, the geometry repaired where invalid, unwrapped to
    [-180, 180], tagged NAD83, and its bbox. Returns the geometry fixes."""
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ST_Read('{fgb}')").fetchall()
            if r[0] != "geom"]
    select, joins = [], []
    for c in cols:
        if c in DROPPED:
            continue
        select.append(f's."{c}"')
        if c in CODED:
            select.append(f'{c}_lk.Dom AS "{c}_Desc"')
            joins.append(f"LEFT JOIN lk_{CODED[c]} AS {c}_lk ON {c}_lk.Code = s.\"{c}\"")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE raw AS
        SELECT *, NOT ST_IsValid(geom) AS invalid, ST_XMax(geom) > 180 AS east,
               ST_XMin(geom) < 180 AND ST_XMax(geom) > 180 AS across
        FROM ST_Read('{fgb}')""")
    fixes = con.execute("SELECT count(*) FILTER (WHERE invalid), count(*) FILTER (WHERE across) "
                        "FROM raw").fetchone()
    valid = ("CASE WHEN invalid THEN ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3)) "
             "ELSE geom END")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE src AS
        SELECT *, {BBOX} FROM (
            SELECT {', '.join(select)},
                   ST_SetCRS({UNWRAP.format(g='v')}, '{CRS}') AS geometry
            FROM (SELECT *, {valid} AS v FROM raw) AS s {' '.join(joins)})""")
    con.execute("DROP TABLE raw")
    return {"repaired_invalid": fixes[0], "split_at_180": fixes[1]}


def gate(con, name: str, expected: int) -> dict:
    """Rows against the geodatabase, OBJECTID unique, every code decoded,
    no geometry left empty or wider than 180 degrees."""
    rows, ids, empty = con.execute("""
        SELECT count(*), count(DISTINCT OBJECTID),
               count(*) FILTER (WHERE geometry IS NULL OR ST_IsEmpty(geometry))
        FROM src""").fetchone()
    problems = []
    if rows != expected:
        problems.append(f"{rows:,} rows, the geodatabase has {expected:,}")
    if ids != rows:
        problems.append(f"OBJECTID: {rows - ids} repeated")
    if empty:
        problems.append(f"{empty} empty geometries after repair")
    # A part wider than 180 degrees is a ring the unwrap missed.
    wrapping = con.execute("""
        WITH p AS (SELECT OBJECTID, unnest(ST_Dump(geometry)).geom AS g FROM src
                   WHERE ST_XMax(geometry) - ST_XMin(geometry) > 180)
        SELECT count(DISTINCT OBJECTID) FROM p WHERE ST_XMax(g) - ST_XMin(g) > 180""").fetchone()[0]
    if wrapping:
        problems.append(f"{wrapping} features with a part across the whole globe")
    names = [r[0] for r in con.execute("DESCRIBE src").fetchall()]
    unmapped = {}
    for c in CODED:
        if c in names:
            n = con.execute(f'SELECT count(*) FROM src WHERE "{c}" IS NOT NULL '
                            f'AND "{c}_Desc" IS NULL').fetchone()[0]
            if n:
                unmapped[c] = [r[0] for r in con.execute(
                    f'SELECT DISTINCT "{c}" FROM src WHERE "{c}" IS NOT NULL '
                    f'AND "{c}_Desc" IS NULL LIMIT 10').fetchall()]
    if unmapped:
        problems.append(f"codes not in their code table: {unmapped}")
    if problems:
        sys.exit(f"{name}: gate failed: " + "; ".join(problems))
    return {"rows": rows, "gdb_rows": expected, "objectid_unique": True}


def write(con, dest: Path, crs: dict) -> dict:
    entry = write_src(con, dest, crs)
    verify(con, dest.parent, [dest.stem])
    check_crs(con, dest)
    return entry


def write_states(con, folder: Path, crs: dict) -> list[dict]:
    """The Combined layer per State_Nm, from `src` (the national rows)."""
    con.execute("ALTER TABLE src RENAME TO national")
    out = []
    states = [r[0] for r in con.execute(
        "SELECT DISTINCT State_Nm FROM national ORDER BY 1").fetchall()]
    for state in states:
        con.execute(f"CREATE OR REPLACE TEMP TABLE src AS "
                    f"SELECT * FROM national WHERE State_Nm = '{state}'")
        dest = folder / "combined" / f"state={state}" / "combined.parquet"
        dest.parent.mkdir(parents=True, exist_ok=True)
        e = write(con, dest, crs)
        out.append({"state": state, "path": f"combined/state={state}/combined.parquet", **e})
    con.execute("DROP TABLE src")
    con.execute("ALTER TABLE national RENAME TO src")
    return out


def tile_combined(con, parquet: Path) -> dict:
    """combined.pmtiles down the ladder: TILE_PROPS from zoom 0, then
    TILE_MIN, then later first zooms. PAD-US is not a coverage (scattered
    parcels, many of them a few acres), so the tiler may thin dense small
    polygons at the small zooms (coverage=False); kept as a coverage, every
    setting up to zoom 8 had tiles over the cap (2026-11-06). The tiler reads
    lon/lat (CRS84); NAD83 differs from it by less than a tile pixel at these
    zooms."""
    pmtiles.binary()
    lonlat = parquet.with_suffix(".lonlat.parquet")
    con.execute(f"""COPY (SELECT * EXCLUDE (geometry, bbox),
                                 ST_SetCRS(geometry::GEOMETRY, 'OGC:CRS84') AS geometry
                          FROM read_parquet('{parquet}')) TO '{lonlat}' (FORMAT PARQUET)""")
    steps = [(props, z) for z in range(0, 9) for props in (TILE_PROPS, TILE_MIN)]
    try:
        for props, min_zoom in steps:
            try:
                entry = pmtiles.tile(lonlat, parquet.with_suffix(".pmtiles"), layer="combined",
                                     min_zoom=min_zoom, max_zoom=TILE_MAX_ZOOM, properties=props,
                                     coverage=False)
            except SystemExit as e:
                print(f"    tiles: {len(props)} properties from z{min_zoom}: {e}", flush=True)
                continue
            return {"name": "combined", **entry}
        sys.exit(f"{parquet.name}: no tile setting fits the size cap")
    finally:
        lonlat.unlink(missing_ok=True)


def attribution(source: dict) -> str:
    return f"""\
# Data attribution

This data is the Protected Areas Database of the United States (PAD-US),
version {VERSION}, published {RELEASED} by the U.S. Geological Survey (USGS),
Gap Analysis Project (GAP).

  Citation:   U.S. Geological Survey (USGS) Gap Analysis Project (GAP), 2024,
              Protected Areas Database of the United States (PAD-US) 4.1:
              U.S. Geological Survey data release, {DOI}
  Release:    {ITEM_URL} ({ZIP_NAME}; the download needs a ScienceBase login)
  Program:    {PROGRAM_PAGE}
  Standards:  {STANDARDS}
  Status:     a work of the United States Government, not subject to copyright
              in the United States (17 U.S.C. 105)

Credit "U.S. Geological Survey, Gap Analysis Project, PAD-US {VERSION}" when you
use it. PAD-US is a compilation of many federal, state, local and private
sources; it is not a legal survey of ownership or boundaries.

## What was changed here

One GeoParquet 2.0 file per feature class (combined, fee, designation,
easement, marine, proclamation), the Combined layer also per state, and the
geodatabase's code tables. Columns and values are PAD-US's, unchanged, plus
`<column>_Desc` beside each coded column, decoded from those code tables;
Shape_Length and Shape_Area (Albers measures) are left out, GIS_Acres is
PAD-US's area. The geometry is reprojected from USGS's contiguous-US Albers
(NAD83) to NAD83 lon/lat (EPSG:4269), curves cut into straight segments,
features crossing the 180th meridian split there, invalid polygons repaired
(ST_MakeValid); _manifest.json counts both per layer. Each row has a `bbox`
column declared as the GeoParquet covering, rows sorted along a Hilbert curve.
The PMTiles archive is derived for web maps.

Release item as read for this build: {source.get('title')}, last updated
{source.get('last_updated')}, {ZIP_NAME} {source.get('zip', {}).get('bytes')} bytes.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def field_docs(xml: Path) -> dict[str, str]:
    """Each attribute's definition from USGS's FGDC metadata (the Combined
    feature class's entity, which documents the shared columns)."""
    docs = {}
    for det in ET.parse(xml).getroot().findall(".//eainfo/detailed"):
        for a in det.findall("attr"):
            label = (a.findtext("attrlabl") or "").strip()
            text = " ".join((a.findtext("attrdef") or "").split())
            if label and text and label not in docs:
                docs[label] = text
    return docs


def shipped(gdb: Path, folder: Path) -> dict:
    """USGS's metadata XML and version history, copied as shipped."""
    out = {}
    for name in (METADATA_XML, HISTORY):
        src = gdb.parent / name
        if not src.is_file():
            sys.exit(f"{src}: not beside the geodatabase (it is in {ZIP_NAME})")
        shutil.copyfile(src, folder / name)
        out[name] = {"file": name, "bytes": src.stat().st_size, "sha256": sha256(folder / name)}
    return out


def build(gdb: Path, work: Path, out: Path, only: list[str] | None = None,
          no_tiles: bool = False) -> None:
    work.mkdir(parents=True, exist_ok=True)
    (work / "tmp").mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs; "
                f"SET temp_directory = '{work / 'tmp'}'")
    crs = projjson(con)
    source = upstream()
    folder = out / f"v{VERSION}"
    folder.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    lookups = load_lookups(con, gdb, folder)
    metadata = shipped(gdb, folder)
    fields = field_docs(folder / METADATA_XML)
    files, themes, labels, states, tiles = [], {}, {}, [], []
    for name, spec in LAYERS.items():
        if only and name not in only:
            continue
        expected = feature_count(gdb, spec["layer"])
        fgb = linearized(gdb, spec["layer"], work)
        fixes = load_layer(con, fgb)
        checked = gate(con, name, expected)
        dest = folder / f"{name}.parquet"
        entry = write(con, dest, crs)
        if name == "combined":
            states = write_states(con, folder, crs)
            total = sum(s["features"] for s in states)
            if total != entry["features"]:
                sys.exit(f"combined: the state files hold {total:,} rows, the national "
                         f"file {entry['features']:,}")
            checked["state_files"] = len(states)
        con.execute("DROP TABLE src")
        fgb.unlink()
        files.append({"name": name, "layer": spec["layer"], **entry, **fixes, "gate": checked})
        themes[name], labels[name] = entry["features"], spec["title"]
        print(f"  {name}: {entry['features']:,} rows, {entry['bytes'] / 1e6:.1f} MB, "
              f"{entry['row_groups']} row groups, {fixes['repaired_invalid']} repaired, "
              f"{fixes['split_at_180']} split at 180, {time.monotonic() - t0:.0f} s", flush=True)
    # Tiles last: every GeoParquet file is written and gated first.
    if "combined" in themes and not no_tiles:
        e = tile_combined(con, folder / "combined.parquet")
        tiles.append(e)
        print(f"  combined: tiles z{e['min_zoom']}-{e['max_zoom']}, {e['bytes'] / 1e6:.1f} MB, "
              f"{len(e['properties'])} properties, {time.monotonic() - t0:.0f} s", flush=True)
    (folder / "ATTRIBUTION.txt").write_text(attribution(source))
    (folder / "_manifest.json").write_text(json.dumps({
        "state_name": f"PAD-US {VERSION}",
        "version": VERSION,
        "released": RELEASED,
        "total_features": sum(themes.values()),
        "themes": themes,
        "labels": labels,
        "crs": CRS,
        "source": source,
        "files": files,
        "states": states,
        "lookups": lookups,
        "metadata": metadata,
        "fields": fields,
        "pmtiles": tiles,
    }, indent=2) + "\n")
    (out / "index.json").write_text(json.dumps({"datasets": [
        {"path": f"v{VERSION}", "code": f"v{VERSION}", "name": f"PAD-US {VERSION}"}]},
        indent=2) + "\n")
    (out / "snapshots.json").write_text(json.dumps({"latest": "", "snapshots": []}) + "\n")
    (out / "_source.json").write_text(json.dumps(source, indent=2) + "\n")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="the ScienceBase release item, and any newer PAD-US")
    b = sub.add_parser("build", help="convert, gate, tile and verify a downloaded geodatabase")
    b.add_argument("--gdb", type=Path, required=True, help="the unpacked PADUS4_1Geodatabase.gdb")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help="the padus/ prefix")
    b.add_argument("--layer", action="append", choices=list(LAYERS),
                   help="build only this layer (repeatable; a test run)")
    b.add_argument("--no-tiles", action="store_true", help="GeoParquet only (a test run)")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
        return
    if not args.gdb.is_dir():
        sys.exit(f"{args.gdb}: not a file geodatabase folder")
    build(args.gdb, args.work_dir, args.out_dir, args.layer, args.no_tiles)


if __name__ == "__main__":
    main()
