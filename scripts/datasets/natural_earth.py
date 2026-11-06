#!/usr/bin/env python3
"""
Natural Earth v5.1.2 as GeoParquet, with one PMTiles world basemap per scale.

Natural Earth (naturalearthdata.com) is the public domain world dataset at
three scales: 1:10m, 1:50m and 1:110m. It ships one zipped shapefile per
layer and scale on naciscdn.org. This writes each listed layer as one
GeoParquet file:

  <out>/<scale>/<layer>.parquet        e.g. 10m/admin_0_countries.parquet
  <out>/<scale>/basemap.pmtiles        every layer of the scale, one tile layer
  <out>/<scale>/basemap-style.json     a MapLibre style over it
  <out>/_manifest.json                 counts, sizes, sha256, sources, repairs
  <out>/ATTRIBUTION.txt

natural_earth_layers.csv says which layers: one row per scale and layer,
the zip's category (cultural or physical) and a title. Adding a layer is a
row. <out> is published as natural-earth/v5.1.2/.

Columns are Natural Earth's, all of them, unchanged, with their mixed case
(NAME in the countries and populated places, name elsewhere). The geometry
is lon/lat (WGS84, OGC:CRS84), with a `bbox` covering column, sorted on a
Hilbert curve, row groups cut at 4 MiB of geometry (gaul.rewrite).

The release is pinned. Each zip carries a VERSION.txt with the version its
layer last changed in (5.1.1 for the 10m countries in the 5.1.2 release);
the build records it and refuses a zip newer than VERSION, which would mean
naciscdn moved on to a release this layout does not name.

Gates, before anything is written as final: every listed zip fetched (a
failed download fails the build), its .prj is WGS84, the rows read equal the
shapefile's record count (from its .shx), and every geometry is valid.
Natural Earth has a few invalid polygons (self-touching rings in the 10m
ocean, lakes and admin units): they are repaired with ST_MakeValid, keeping
the polygonal part, and each repair is counted in the manifest. A row
without geometry (the Loire in the 50m rivers) is kept, so the row count
stays the shapefile's, and is left out of the tiles.

The tiles: tylertoo writes one layer per archive, so each scale's archive
holds every layer of the scale in one tile layer, `ne`, with an `ne_layer`
property naming the file a feature comes from, and a few properties a map
styles and labels by. A style filters on `ne_layer`. The tiles are clipped
to the Web Mercator band (85.0511 degrees); the GeoParquet is not.

Usage:
  python3 scripts/datasets/natural_earth.py upstream
  python3 scripts/datasets/natural_earth.py build --src-dir <zips> --work-dir <tmp> --out-dir out/natural-earth/v5.1.2
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles  # noqa: E402
from datasets.gaul import BBOX_STRUCT, geo_metadata, rewrite, verify  # noqa: E402

VERSION = "5.1.2"
RELEASE_DATE = "2022-05-13"
CDN = "https://naciscdn.org/naturalearth"
SITE = "https://www.naturalearthdata.com/"
TERMS = "https://www.naturalearthdata.com/about/terms-of-use/"
LAYERS_CSV = Path(__file__).with_name("natural_earth_layers.csv")
DATASET_PREFIX = "natural-earth"
PUBLIC = f"https://parquetry.geomermaids.com/{DATASET_PREFIX}/v{VERSION}"
SCALES = {"10m": "1:10 million", "50m": "1:50 million", "110m": "1:110 million"}
TILE_LAYER = "ne"
# The tile property naming the layer a feature comes from. Not `layer`:
# tylertoo's decoder keeps that name for its own provenance column.
LAYER_PROP = "ne_layer"
# Zoom range of each scale's archive: about where Natural Earth says each
# scale reads well, plus one zoom of headroom. The 10m archive starts at zoom
# 1: its one zoom 0 tile holds 8,900 features and goes over the 500 KB cap,
# and the 110m and 50m archives are the ones for a world view.
ZOOMS = {"110m": (0, 3), "50m": (0, 5), "10m": (1, 8)}
CREDIT = "Made with Natural Earth"

GEOMETRY_NAMES = {"POINT": "Point", "MULTIPOINT": "MultiPoint", "LINESTRING": "LineString",
                  "MULTILINESTRING": "MultiLineString", "POLYGON": "Polygon",
                  "MULTIPOLYGON": "MultiPolygon"}
POLYGONAL = {"Polygon", "MultiPolygon"}

# Colours, shared with the catalog's styles and thumbnails.
COLORS = {
    "admin_0_countries": "#8e6c8a", "admin_1_states_provinces": "#b392ac",
    "populated_places": "#c0392b", "urban_areas": "#e0a458", "airports": "#34495e",
    "ports": "#16a085", "time_zones": "#7f8c8d", "coastline": "#2f6f9f",
    "land": "#d9cfb8", "ocean": "#a8c8e0", "rivers_lake_centerlines": "#3b8fd4",
    "lakes": "#7fb2dc", "graticules_10": "#9aa5b1",
}


# ---------- the layer table ----------

def layers() -> list[dict]:
    with LAYERS_CSV.open(newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        if r["scale"] not in SCALES:
            sys.exit(f"{LAYERS_CSV.name}: unknown scale {r['scale']!r}")
        if r["category"] not in ("cultural", "physical"):
            sys.exit(f"{LAYERS_CSV.name}: unknown category {r['category']!r}")
        if r["layer"] not in COLORS:
            sys.exit(f"{LAYERS_CSV.name}: {r['layer']} has no colour in COLORS")
        r["id"] = f"{r['scale']}/{r['layer']}"
        r["stem"] = f"ne_{r['scale']}_{r['layer']}"
        r["url"] = f"{CDN}/{r['scale']}/{r['category']}/{r['stem']}.zip"
    ids = [r["id"] for r in rows]
    if len(ids) != len(set(ids)):
        sys.exit(f"{LAYERS_CSV.name}: duplicate rows")
    return rows


def version_tuple(v: str) -> tuple[int, ...]:
    """5.0.0-pre9 -> (5, 0, 0): a pre-release sorts with its release."""
    return tuple(int(p) for p in v.split("-")[0].split("."))


# ---------- upstream ----------

def head(url: str) -> dict:
    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=60) as r:
        return {"etag": (r.headers.get("ETag") or "").strip('"'),
                "last_modified": r.headers.get("Last-Modified"),
                "bytes": int(r.headers.get("Content-Length") or 0)}


def upstream() -> dict:
    """Each zip's ETag, Last-Modified and size on naciscdn, and a fingerprint
    of them all: a change means the CDN no longer serves what v5.1.2 named."""
    out = {r["id"]: {"url": r["url"], **head(r["url"])} for r in layers()}
    etags = "".join(out[k]["etag"] for k in sorted(out))
    return {"version": VERSION, "fingerprint": hashlib.sha256(etags.encode()).hexdigest()[:16],
            "layers": out}


def fetch(row: dict, src: Path, size: int) -> Path:
    """The zip, kept in src/<scale>/: a copy of the listed size is reused.
    Five attempts, then the build fails."""
    path = src / row["scale"] / f"{row['stem']}.zip"
    if path.is_file() and path.stat().st_size == size:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".part")
    for attempt in range(1, 6):
        try:
            with urllib.request.urlopen(row["url"], timeout=300) as r, tmp.open("wb") as f:
                while chunk := r.read(1 << 20):
                    f.write(chunk)
            break
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if attempt == 5:
                sys.exit(f"{row['url']}: {e}")
            time.sleep(5 * attempt)
    if tmp.stat().st_size != size:
        sys.exit(f"{row['url']}: got {tmp.stat().st_size} bytes, the CDN lists {size}")
    tmp.rename(path)
    return path


def inspect_zip(row: dict, path: Path) -> dict:
    """The zip's shapefile, its record count from the .shx, its VERSION.txt,
    and a check that its .prj is WGS84."""
    with zipfile.ZipFile(path) as z:
        names = z.namelist()
        need = {ext: f"{row['stem']}.{ext}" for ext in ("shp", "shx", "dbf", "prj")}
        missing = [n for n in need.values() if n not in names]
        if missing:
            sys.exit(f"{path.name}: missing {missing}")
        prj = z.read(need["prj"]).decode("latin-1")
        if "WGS_1984" not in prj and "WGS 84" not in prj:
            sys.exit(f"{path.name}: the .prj is not WGS84: {prj[:120]}")
        vfile = f"{row['stem']}.VERSION.txt"
        ne_version = z.read(vfile).decode().strip() if vfile in names else None
        # A shapefile index is a 100-byte header and 8 bytes per record.
        records = (z.getinfo(need["shx"]).file_size - 100) // 8
        # The DBF's field names, from its header: 32 bytes, then one 32-byte
        # descriptor per field, the name in its first 11 bytes.
        dbf = z.read(need["dbf"])
    header = int.from_bytes(dbf[8:10], "little")
    fields = [dbf[32 + 32 * i:43 + 32 * i].split(b"\0")[0].decode("latin-1")
              for i in range((header - 33) // 32)]
    if ne_version and version_tuple(ne_version) > version_tuple(VERSION):
        sys.exit(f"{path.name}: VERSION.txt says {ne_version}, newer than {VERSION}: "
                 f"naciscdn serves a later release; bump VERSION and the layout")
    return {"shp": f"/vsizip/{path}/{need['shp']}", "records": records,
            "ne_version": ne_version, "fields": fields}


# ---------- one layer ----------

def load(con, row: dict, shp: str, fields: list[str]) -> dict:
    """Read the shapefile into `src`, repairing invalid geometries, with the
    DBF's columns and no other: DuckDB's ST_Read adds an OGC_FID that
    Natural Earth does not have. Returns the counts the manifest records."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE raw AS SELECT * FROM ST_Read('{shp}')")
    read = [c for (c, *_) in con.execute("DESCRIBE raw").fetchall() if c != "geom"]
    extra = [c for c in read if c not in fields]
    if extra:
        if extra != ["OGC_FID"]:
            sys.exit(f"{row['id']}: columns not in the DBF: {extra}")
        con.execute('ALTER TABLE raw DROP COLUMN "OGC_FID"')
        read.remove("OGC_FID")
    if read != fields:
        sys.exit(f"{row['id']}: read columns {read[:8]}... differ from the DBF's {fields[:8]}...")
    types = {GEOMETRY_NAMES.get(t, t) for (t,) in con.execute(
        "SELECT DISTINCT ST_GeometryType(geom)::VARCHAR FROM raw WHERE geom IS NOT NULL"
    ).fetchall()}
    # Keep the polygonal part of a repaired polygon: ST_MakeValid can return
    # a collection with a stray line where a ring touched itself.
    fix = ("ST_CollectionExtract(ST_MakeValid(geom), 3)" if types <= POLYGONAL
           else "ST_MakeValid(geom)")
    repaired = con.execute(
        "SELECT count(*) FROM raw WHERE geom IS NOT NULL AND NOT ST_IsValid(geom)").fetchone()[0]
    con.execute(f"""CREATE OR REPLACE TEMP TABLE src AS
        SELECT * EXCLUDE (geom),
               CASE WHEN geom IS NULL OR ST_IsValid(geom) THEN geom ELSE {fix} END AS geom
        FROM raw""")
    con.execute("DROP TABLE raw")
    n, nulls, invalid, empty = con.execute("""SELECT count(*), count(*) FILTER (WHERE geom IS NULL),
        count(*) FILTER (WHERE geom IS NOT NULL AND NOT ST_IsValid(geom)),
        count(*) FILTER (WHERE geom IS NOT NULL AND ST_IsEmpty(geom)) FROM src""").fetchone()
    if invalid or empty:
        sys.exit(f"{row['id']}: {invalid} invalid and {empty} empty geometries after repair")
    return {"rows": n, "null_geometries": nulls, "repaired": repaired}


def write_layer(con, dest: Path) -> dict:
    """`src` as GeoParquet at dest: Hilbert-sorted, bbox covering, row groups
    capped by gaul.rewrite. Like gaul.write_layer, but a row without geometry
    is allowed and the footer names geometry types the GeoParquet way."""
    count, xmin, ymin, xmax, ymax = con.execute("""
        SELECT count(*), min(ST_XMin(geom)), min(ST_YMin(geom)),
               max(ST_XMax(geom)), max(ST_YMax(geom)) FROM src""").fetchone()
    if not count or xmin is None:
        sys.exit(f"{dest}: no features")
    types = sorted({GEOMETRY_NAMES[t] for (t,) in con.execute(
        "SELECT DISTINCT ST_GeometryType(geom)::VARCHAR FROM src WHERE geom IS NOT NULL"
    ).fetchall()})
    attrs = ", ".join(f'"{c}"' for (c, *_) in con.execute("DESCRIBE src").fetchall()
                      if c != "geom")
    box = f"ST_Extent(ST_MakeEnvelope({xmin!r}, {ymin!r}, {xmax!r}, {ymax!r}))"
    geo = geo_metadata((xmin, ymin, xmax, ymax), types)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.stem + ".duckdb.parquet")
    con.execute(f"""
        COPY (
            SELECT {attrs}, {BBOX_STRUCT}, geom AS geometry
            FROM src
            ORDER BY ST_Hilbert(geom, {box}) NULLS LAST
        ) TO '{tmp}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', COMPRESSION ZSTD,
                      COMPRESSION_LEVEL 1, ROW_GROUP_SIZE 1048576,
                      KV_METADATA {{geo: '{geo}'}})""")
    rewrite(tmp, dest)
    tmp.unlink()
    groups, largest = con.execute(f"""
        SELECT count(*), max(b) FROM (
            SELECT row_group_id, sum(total_compressed_size) AS b
            FROM parquet_metadata('{dest}') GROUP BY 1)""").fetchone()
    return {"features": count, "bytes": dest.stat().st_size, "row_groups": groups,
            "largest_row_group_bytes": largest, "geometry_types": types,
            "bbox": [xmin, ymin, xmax, ymax], "sha256": pmtiles.sha256(dest)}


# ---------- the basemap of a scale ----------

# The properties a map styles and labels by, wherever a layer has them
# (Natural Earth spells them in either case).
MERC_LAT = 85.0511
TILE_PROPS = {"name": "VARCHAR", "featurecla": "VARCHAR", "scalerank": "DOUBLE",
              "min_zoom": "DOUBLE", "pop_max": "BIGINT", "adm0_a3": "VARCHAR"}


def basemap_input(con, scale: str, rows: list[dict], out_dir: Path, dest: Path) -> None:
    """One GeoParquet with every layer of the scale, a `layer` column and the
    TILE_PROPS: the input of the scale's archive. Smallest layers last, so a
    point or a line is not hidden under a polygon in renderers that paint in
    file order."""
    order = ["ocean", "land", "time_zones", "urban_areas", "lakes", "admin_0_countries",
             "admin_1_states_provinces", "graticules_10", "coastline",
             "rivers_lake_centerlines", "airports", "ports", "populated_places"]
    parts = []
    for r in sorted(rows, key=lambda r: order.index(r["layer"])):
        f = out_dir / f"{r['id']}.parquet"
        have = {c.lower(): c for (c, *_) in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{f}')").fetchall()}
        cols = ", ".join(
            f'TRY_CAST("{have[p]}" AS {t}) AS {p}' if p in have else f"NULL::{t} AS {p}"
            for p, t in TILE_PROPS.items())
        # Clipped to the Web Mercator band: tylertoo drops a whole feature
        # with a vertex past 90 degrees (the ocean and the 110m land reach
        # 90.0000000000006), which left the 10m archive without an ocean.
        # The tiles only; the GeoParquet keeps Natural Earth's geometry.
        geom = (f"CASE WHEN ST_YMax(geometry) > {MERC_LAT} OR ST_YMin(geometry) < -{MERC_LAT} "
                f"THEN ST_Intersection(geometry, ST_MakeEnvelope(-180, -{MERC_LAT}, 180, "
                f"{MERC_LAT})) ELSE geometry END")
        parts.append(f"SELECT '{r['layer']}' AS {LAYER_PROP}, {cols}, {geom} AS geometry "
                     f"FROM read_parquet('{f}') WHERE geometry IS NOT NULL")
    con.execute(f"COPY (SELECT * FROM ({' UNION ALL '.join(parts)}) WHERE NOT ST_IsEmpty(geometry)) "
                f"TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)")


def basemap_style(scale: str, layer_names: set[str], tiles: dict) -> dict:
    """A plain world basemap over the scale's archive: water, land, urban
    areas, boundaries, rivers and places as dots (no labels: a style with
    text needs a glyph server, which this one does not assume)."""
    src = {"source": "ne", "source-layer": TILE_LAYER}

    def only(layer: str) -> list:
        return ["==", ["get", LAYER_PROP], layer]

    width = ["interpolate", ["linear"], ["zoom"], 0, 0.4, 8, 1.6]
    spec = [
        ("ocean", "fill", {"fill-color": COLORS["ocean"]}),
        ("land", "fill", {"fill-color": "#f2efe6"}),
        ("urban_areas", "fill", {"fill-color": COLORS["urban_areas"], "fill-opacity": 0.5}),
        ("lakes", "fill", {"fill-color": COLORS["lakes"]}),
        ("graticules_10", "line", {"line-color": COLORS["graticules_10"], "line-width": 0.4,
                                   "line-opacity": 0.6}),
        ("admin_1_states_provinces", "line", {"line-color": COLORS["admin_1_states_provinces"],
                                              "line-width": 0.5, "line-dasharray": [2, 2]}),
        ("admin_0_countries", "line", {"line-color": COLORS["admin_0_countries"],
                                       "line-width": width}),
        ("rivers_lake_centerlines", "line", {"line-color": COLORS["rivers_lake_centerlines"],
                                             "line-width": 0.7}),
        ("coastline", "line", {"line-color": COLORS["coastline"], "line-width": 0.6}),
        ("populated_places", "circle", {
            "circle-color": COLORS["populated_places"], "circle-stroke-color": "#ffffff",
            "circle-stroke-width": 0.5,
            "circle-radius": ["interpolate", ["linear"], ["zoom"], 0, 1.5, 8, 4]}),
    ]
    return {
        "version": 8,
        "name": f"Natural Earth {VERSION}, {scale} basemap",
        "sources": {"ne": {"type": "vector", "url": f"pmtiles://{PUBLIC}/{scale}/{tiles['file']}",
                           "attribution": CREDIT}},
        "layers": [{"id": f"ne-{layer}-{kind}", "type": kind, **src, "filter": only(layer),
                    "paint": paint}
                   for layer, kind, paint in spec if layer in layer_names],
    }


# ---------- build ----------

def attribution() -> str:
    return f"""\
# Data attribution

Natural Earth {VERSION} ({RELEASE_DATE}), free vector map data at 1:10m,
1:50m and 1:110m scales, redistributed here as GeoParquet and PMTiles.

## Licence

Public domain. Natural Earth's terms of use: {TERMS}
No permission is needed to use, modify or redistribute it. Natural Earth
asks for, but does not require, the credit "{CREDIT}".

## What was changed here

Every column and value is Natural Earth's. Each layer was converted from
its shapefile to GeoParquet, given a `bbox` column, and sorted on a Hilbert
curve. Invalid polygons (a few self-touching rings) were repaired with
ST_MakeValid, keeping their polygonal part; _manifest.json counts the
repairs per layer. The PMTiles archives are derived from the same rows.

Natural Earth's boundaries follow its de facto policy (the line of actual
control); its own documentation explains the disputed areas and the
point-of-view variants it publishes. This redistribution adds no position
of its own.

## Source

  {CDN}/<scale>/<cultural|physical>/ne_<scale>_<layer>.zip
"""


def build(src: Path, work: Path, out: Path, *, no_tiles: bool) -> None:
    rows = layers()
    work.mkdir(parents=True, exist_ok=True)
    source = upstream()["layers"]
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; SET temp_directory = '{work}'")
    print(f"Natural Earth {VERSION} -> {out}  ({len(rows)} layers)", flush=True)
    entries = []
    for r in rows:
        up = source[r["id"]]
        z = fetch(r, src, up["bytes"])
        info = inspect_zip(r, z)
        counts = load(con, r, info["shp"], info["fields"])
        if counts["rows"] != info["records"]:
            sys.exit(f"{r['id']}: read {counts['rows']} rows, the shapefile holds "
                     f"{info['records']}")
        e = write_layer(con, out / f"{r['id']}.parquet")
        if e["features"] != info["records"]:
            sys.exit(f"{r['id']}: wrote {e['features']} rows, the shapefile holds "
                     f"{info['records']}")
        entries.append({
            "name": r["id"], "scale": r["scale"], "layer": r["layer"], "title": r["title"],
            "category": r["category"], "file": f"{r['id']}.parquet", **e,
            "null_geometries": counts["null_geometries"], "repaired": counts["repaired"],
            "source": {"url": r["url"], "etag": up["etag"], "last_modified": up["last_modified"],
                       "bytes": up["bytes"], "ne_version": info["ne_version"]},
        })
        print(f"  {r['id']}: {e['features']:,} rows, {e['bytes'] / 1e6:.1f} MB, "
              f"{e['row_groups']} group(s)"
              + (f", {counts['repaired']} repaired" if counts["repaired"] else "")
              + (f", {counts['null_geometries']} without geometry"
                 if counts["null_geometries"] else ""), flush=True)
    verify(con, out, [r["id"] for r in rows])

    tiles = []
    if not no_tiles:
        for scale in SCALES:
            srows = [r for r in rows if r["scale"] == scale]
            if not srows:
                continue
            lo, hi = ZOOMS[scale]
            inp = work / f"basemap-{scale}.parquet"
            basemap_input(con, scale, srows, out, inp)
            t = pmtiles.tile(inp, out / scale / "basemap.pmtiles", layer=TILE_LAYER,
                             min_zoom=lo, max_zoom=hi, properties=[LAYER_PROP, *TILE_PROPS])
            t = {"scale": scale, **t, "file": "basemap.pmtiles", "path": f"{scale}/basemap.pmtiles",
                 "layers": [r["layer"] for r in srows]}
            style = basemap_style(scale, set(t["layers"]), t)
            spath = out / scale / "basemap-style.json"
            spath.write_text(json.dumps(style, indent=2) + "\n")
            t["style"] = {"file": f"{scale}/basemap-style.json", "bytes": spath.stat().st_size,
                          "sha256": pmtiles.sha256(spath)}
            tiles.append(t)
            inp.unlink()
            print(f"  {scale}/basemap.pmtiles: z{lo}-{hi}, {t['tiles']:,} tiles, "
                  f"{t['bytes'] / 1e6:.1f} MB", flush=True)

    (out / "ATTRIBUTION.txt").write_text(attribution())
    (out / "_manifest.json").write_text(json.dumps({
        "state_name": f"Natural Earth {VERSION}",
        "version": VERSION,
        "release_date": RELEASE_DATE,
        "licence": "public domain",
        "total_features": sum(e["features"] for e in entries),
        # Keyed by file path without .parquet, relative to this manifest: a
        # parquetry repository browser (GeoPQ Workbench) appends .parquet.
        "themes": {e["name"]: e["features"] for e in entries},
        "labels": {e["name"]: f"{e['title']} ({e['scale']})" for e in entries},
        "files": entries,
        "pmtiles": tiles,
    }, indent=2) + "\n")
    print(f"  ATTRIBUTION.txt, _manifest.json: {sum(e['features'] for e in entries):,} rows, "
          f"{sum(e['bytes'] for e in entries) / 1e6:.1f} MB of GeoParquet", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="print each zip's ETag, size and Last-Modified")
    b = sub.add_parser("build", help="fetch, convert, tile and verify every listed layer")
    b.add_argument("--src-dir", type=Path, required=True, help="where the zips are kept")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True,
                   help=f"the {DATASET_PREFIX}/v{VERSION}/ prefix")
    b.add_argument("--no-tiles", action="store_true", help="GeoParquet only (a test run)")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
        return
    build(args.src_dir, args.work_dir, args.out_dir, no_tiles=args.no_tiles)


if __name__ == "__main__":
    main()
