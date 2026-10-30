#!/usr/bin/env python3
"""
MassGIS, the Massachusetts Bureau of Geographic Information, its statewide
vector layers as GeoParquet and PMTiles.

MassGIS publishes its layers as zipped shapefiles and file geodatabases in a
public bucket (https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/),
the files behind its download pages. This writes one GeoParquet file per
layer, and its PMTiles archive, grouped by theme:

  <out>/<theme>/<layer>.parquet     e.g. parcels/l3_taxpar_poly.parquet
  <out>/<theme>/<layer>.pmtiles
  <out>/<theme>/_manifest.json      rows, sizes, sha256, titles, sources, tile settings
  <out>/<theme>/ATTRIBUTION.txt

massgis_layers.csv says which layers: one row per layer, its theme, the zip
it comes from and the shapefile or geodatabase layer inside. The current
layers only: the bucket also keeps dated copies, superseded editions,
per-town and per-tile splits of statewide layers, indexes and staff working
folders, none of which are published here. Adding a layer is a row.

Columns and values are MassGIS's, unchanged. The geometry is 2D, in the
layer's own CRS (Massachusetts State Plane, EPSG:26986, for all but a few),
with a `bbox` column declared as the GeoParquet covering; rows are sorted on
a Hilbert curve and cut into row groups of at most 4 MiB of geometry
(gaul.py's rewrite). A table without geometry (the assessor records) is
plain Parquet.

The tiles are built from a copy transformed to lon/lat (tylertoo reads
lon/lat or Web Mercator only), with every attribute when the tiles fit the
size cap, else the layer's naming columns, else from a later minimum zoom:
then with larger tiles (2 and 4 MB): tile_layer() steps down that ladder
and the manifest records where it stopped. A layer too dense to try from
zoom 0 starts at the CSV's min_zoom: land cover (22.8 million polygons) and
the tax parcels, from zoom 12. Below the first zoom of their tiles, the
OVERVIEWS layers get an overview in the same archive (H3 cells with the
dominant class or a count per km2, or the major roads), so a map zoomed out
on the state still shows them; the `overviews` command adds it to layers
already published.

A build reads the published manifests and _source.json, and rebuilds only
the layers whose zip changed upstream (or the ones named with --layer).

Usage:
  python3 scripts/datasets/massgis.py upstream
  uv run --project scripts --with matplotlib python scripts/datasets/massgis.py build --src-dir /Volumes/T9/massgis/src \\
      --work-dir /Volumes/T9/massgis/tmp --out-dir /Volumes/T9/massgis/out
  uv run --project scripts python scripts/datasets/massgis.py overviews --work-dir work --out-dir out
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import duckdb

# tylertoo logs every zoom of every layer at info level.
os.environ.setdefault("RUST_LOG", "warn")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles
from datasets.gaul import rewrite, verify
from datasets.infra import GEOMETRY_TYPE_NAMES

BUCKET = "https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/"
PUBLIC = "https://parquetry.geomermaids.com/massgis"
LAYERS_CSV = Path(__file__).with_name("massgis_layers.csv")
PAGE = "https://www.mass.gov/info-details/massgis-data-layers"
# Massachusetts State Plane, mainland zone, meters.
MASS_CRS = "EPSG:26986"

THEMES = {
    "parcels": "Property Tax Parcels",
    "buildings": "Buildings and Addresses",
    "boundaries": "Boundaries and Districts",
    "regions": "Administrative Regions",
    "census": "Census 2020",
    "transportation": "Transportation",
    "water": "Water Resources",
    "wetlands": "Wetlands",
    "habitat": "Habitat and Natural Resources",
    "open_space": "Open Space and Recreation",
    "land_cover": "Land Cover and Forest",
    "geology_soils": "Geology and Soils",
    "elevation": "Elevation and Bathymetry",
    "hazards": "Hazards",
    "regulated_sites": "Regulated Sites",
    "facilities": "Public Facilities",
    "historic": "Historic Resources",
    "utilities": "Utilities",
    "names": "Place Names",
    "new_england": "New England Reference",
}

# The tiles' maximum zoom: MassGIS's layers are mapped at 1:5,000 to
# 1:25,000, which zoom 14 (about 10 m per pixel) shows; maps overzoom past it.
MAX_ZOOM = 14
# Minimum zooms tried, in order, once the naming columns alone still go over
# the tile size cap at zoom 0.
MIN_ZOOMS = (0, 6, 8, 10, 12)
# Last resort, for points stacked on one spot (the 3.7 million address
# points stand on 2.1 million locations: a condo building's units share
# one): a larger tile size cap. tylertoo drops features from a tile over
# the cap, so the cap is raised rather than the loss accepted; MapLibre
# reads a few tiles of 2 to 4 MB without trouble.
BIG_TILES = ("2M", "4M")
# Lines the tiles keep at every zoom, however short: by default a segment under
# 2 pixels goes, 9% to 16% of Cambridge's street length at zoom 12.
EVERY_SEGMENT = ["--line-visibility", "0", "--line-thinning", "0"]
# Columns that name or identify a feature, kept in the tiles when all of the
# attributes do not fit. Matched case-insensitively, at most four.
LABEL = re.compile(r"(^|_)(name|label|town|site|title|type|class|zone|code|id|loc_id|"
                   r"map_par_id|full_str|addr_num|poly_type|covername|landuse)$", re.IGNORECASE)

BBOX = """struct_pack(
            xmin := (ST_XMin(geometry) - abs(ST_XMin(geometry)) * 1e-6 - 1e-9)::FLOAT,
            ymin := (ST_YMin(geometry) - abs(ST_YMin(geometry)) * 1e-6 - 1e-9)::FLOAT,
            xmax := (ST_XMax(geometry) + abs(ST_XMax(geometry)) * 1e-6 + 1e-9)::FLOAT,
            ymax := (ST_YMax(geometry) + abs(ST_YMax(geometry)) * 1e-6 + 1e-9)::FLOAT
        ) AS bbox"""


def layers() -> list[dict]:
    with LAYERS_CSV.open() as f:
        rows = list(csv.DictReader(f))
    unknown = {r["theme"] for r in rows} - set(THEMES)
    if unknown:
        sys.exit(f"{LAYERS_CSV.name}: themes not in THEMES: {sorted(unknown)}")
    ids = [r["id"] for r in rows]
    if len(ids) != len(set(ids)):
        sys.exit(f"{LAYERS_CSV.name}: duplicate ids")
    return rows


def zips(layer: dict, listing: dict) -> list[str]:
    """The bucket keys a layer reads: one zip, or every zip under a prefix
    for a layer MassGIS ships as tiles (contours5k/*.zip)."""
    if "*" in layer["zip"]:
        prefix = layer["zip"].split("*")[0]
        return sorted(k for k in listing if k.startswith(prefix) and k.endswith(".zip"))
    return [layer["zip"]]


def list_bucket(prefixes: set[str]) -> dict:
    """ETag, Last-Modified and size of every object under the prefixes."""
    out = {}
    ns = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}
    for prefix in sorted(prefixes):
        token = None
        while True:
            q = {"list-type": "2", "prefix": prefix}
            if token:
                q["continuation-token"] = token
            with urllib.request.urlopen(f"{BUCKET}?{urllib.parse.urlencode(q)}", timeout=120) as r:
                root = ET.fromstring(r.read())
            for c in root.findall("s:Contents", ns):
                out[c.find("s:Key", ns).text] = {
                    "etag": c.find("s:ETag", ns).text.strip('"'),
                    "last_modified": c.find("s:LastModified", ns).text,
                    "bytes": int(c.find("s:Size", ns).text)}
            t = root.find("s:NextContinuationToken", ns)
            if t is None:
                break
            token = t.text
    return out


def upstream() -> dict:
    """Each layer's source zips and a fingerprint of their ETags: a layer is
    rebuilt when its fingerprint changes."""
    rows = layers()
    listing = list_bucket({r["zip"].rsplit("/", 1)[0] + "/" for r in rows})
    out = {}
    for r in rows:
        keys = zips(r, listing)
        missing = [k for k in keys if k not in listing]
        if missing or not keys:
            sys.exit(f"{r['id']}: not in the bucket: {missing or r['zip']}")
        etags = "".join(listing[k]["etag"] for k in keys)
        out[r["id"]] = {
            "zips": keys if len(keys) < 4 else [r["zip"]],
            "files": len(keys),
            "last_modified": max(listing[k]["last_modified"] for k in keys),
            "bytes": sum(listing[k]["bytes"] for k in keys),
            "fingerprint": hashlib.sha256(etags.encode()).hexdigest()[:16],
        }
    return out


def fetch(key: str, src: Path, size: int) -> Path:
    """The zip from the bucket, kept in src: a copy of the same size is reused."""
    path = src / key
    if path.is_file() and path.stat().st_size == size:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".part")
    for attempt in range(1, 6):
        try:
            with urllib.request.urlopen(BUCKET + urllib.parse.quote(key), timeout=300) as r, \
                    tmp.open("wb") as f:
                shutil.copyfileobj(r, f, 1 << 20)
            break
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if attempt == 5:
                raise
            print(f"  {key}: {e}, retrying", flush=True)
            time.sleep(10 * attempt)
    if tmp.stat().st_size != size:
        sys.exit(f"{key}: got {tmp.stat().st_size} bytes, the bucket lists {size}")
    tmp.replace(path)
    return path


def sources(layer: dict, paths: list[Path]) -> list[tuple[str, str | None]]:
    """GDAL dataset and layer name for each zip: the shapefile the CSV names,
    every shapefile of a tiled layer, or a geodatabase layer."""
    out = []
    for path in paths:
        names = zipfile.ZipFile(path).namelist()
        member = layer["member"]
        if member.lower().endswith(".shp"):
            hit = [n for n in names if n.lower() == member.lower()]
            if not hit:
                sys.exit(f"{layer['id']}: {member} not in {path.name}")
            out.append((f"/vsizip/{path}/{hit[0]}", None))
        elif member == "*" and any(n.lower().endswith(".shp") for n in names):
            out += [(f"/vsizip/{path}/{n}", None) for n in names if n.lower().endswith(".shp")]
        else:
            gdb = sorted({n.split(".gdb/")[0] + ".gdb" for n in names if ".gdb/" in n})
            if len(gdb) != 1:
                sys.exit(f"{layer['id']}: {path.name} holds {len(gdb)} geodatabases")
            out.append((f"/vsizip/{path}/{gdb[0]}", None if member == "*" else member))
    return out


def read_sql(dataset: str, layer: str | None) -> str:
    args = f"'{dataset}'" + (f", layer = '{layer}'" if layer else "")
    return f"SELECT * FROM ST_Read({args})"


def gdb_layers(con, dataset: str) -> dict[str, bool]:
    """A geodatabase's layers, and whether each has a geometry."""
    return dict(con.execute(f"""SELECT unnest(layers).name, len(unnest(layers).geometry_fields) > 0
                                FROM ST_Read_Meta('{dataset}')""").fetchall())


def linearized(dataset: str, layer: str, work: Path) -> str:
    """A geodatabase layer copied to FlatGeobuf with GDAL, its curves
    (circular arcs in parcel and road edges) cut into straight segments and
    its geometry made 2D. DuckDB's reader stops on curved geometries, which
    MassGIS's geodatabases hold even where the layer declares plain
    polygons, so every spatial geodatabase layer takes this way.

    Polygons of zero area are left out: a degenerate arc GDAL cannot make a
    ring of (OGRCurve::CastToLinearRing failed) comes out of FlatGeobuf as a
    geometry DuckDB cannot read, and every such parcel has zero area. That is
    not enough under GDAL 3.8.4, which still fails on two parcel layers: 3.13.3
    is what this was tested with, and what the workflow runs."""
    dest = work / f"{layer}.fgb"
    dest.unlink(missing_ok=True)
    sql = (f'SELECT * FROM "{layer}" WHERE OGR_GEOM_AREA > 0 OR OGR_GEOMETRY NOT IN '
           "('POLYGON', 'MULTIPOLYGON', 'CURVEPOLYGON', 'MULTISURFACE')")
    subprocess.run(["ogr2ogr", "-f", "FlatGeobuf", "-nlt", "CONVERT_TO_LINEAR", "-dim", "XY",
                    "-lco", "SPATIAL_INDEX=NO", "-nln", layer, str(dest), dataset, "-sql", sql],
                   check=True)
    return str(dest)


_crs_cache: dict[str, dict] = {}


def projjson(con, crs: str) -> dict:
    """A CRS as PROJJSON for the `geo` footer, from a probe file DuckDB writes."""
    if crs not in _crs_cache:
        probe = Path(con.execute("SELECT current_setting('temp_directory')").fetchone()[0]) / "crs.parquet"
        con.execute(f"COPY (SELECT ST_SetCRS(ST_Point(0, 0), '{crs}') AS g) TO '{probe}' "
                    "(FORMAT PARQUET, GEOPARQUET_VERSION 'V2')")
        geo = con.execute(f"SELECT decode(value) FROM parquet_kv_metadata('{probe}') "
                          "WHERE key = 'geo'").fetchone()[0]
        probe.unlink()
        _crs_cache[crs] = json.loads(geo)["columns"]["g"]["crs"]
    return _crs_cache[crs]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load(con, srcs: list[tuple[str, str | None]], work: Path) -> str | None:
    """The layer into the temp table src, its geometry column renamed
    `geometry` and made 2D. Returns the CRS (e.g. 'EPSG:26986'), None for a
    table without geometry."""
    parts = []
    for dataset, layer in srcs:
        if ".gdb" in dataset:
            found = gdb_layers(con, dataset)
            if layer is None:
                spatial = [n for n, g in found.items() if g]
                if len(spatial) != 1:
                    sys.exit(f"{dataset}: {len(spatial)} spatial layers ({spatial}); "
                             "name one in the CSV")
                layer = spatial[0]
            if layer not in found:
                sys.exit(f"{dataset}: no layer {layer} (has {sorted(found)})")
            if found[layer]:
                dataset, layer = linearized(dataset, layer, work), None
        parts.append(read_sql(dataset, layer))
    con.execute("CREATE OR REPLACE TEMP TABLE raw AS " + " UNION ALL BY NAME ".join(parts))
    cols = con.execute("DESCRIBE raw").fetchall()
    geom = [c for c, t, *_ in cols if t.startswith("GEOMETRY")]
    if not geom:
        con.execute("CREATE OR REPLACE TEMP TABLE src AS SELECT * FROM raw")
        return None
    g = geom[0]
    crs = re.search(r"GEOMETRY\('([^']+)'\)", {c: t for c, t, *_ in cols}[g])
    if crs:
        crs, geometry = crs.group(1), f'ST_Force2D("{g}")'
    else:
        # A shapefile without its .prj (trnslna1, 1998). MassGIS's statewide
        # layers are all Mass State Plane meters: take EPSG:26986 when the
        # extent fits the state in it, refuse anything else.
        x0, y0, x1, y1 = con.execute(f"""SELECT min(ST_XMin("{g}")), min(ST_YMin("{g}")),
            max(ST_XMax("{g}")), max(ST_YMax("{g}")) FROM raw""").fetchone()
        if not (30_000 <= x0 and x1 <= 340_000 and 770_000 <= y0 and y1 <= 960_000):
            sys.exit(f"{srcs[0][0]}: the geometry has no CRS")
        crs, geometry = MASS_CRS, f"""ST_SetCRS(ST_Force2D("{g}"), '{MASS_CRS}')"""
    # Esri writes a point with no location at -DBL_MAX: as empty as ST_IsEmpty's.
    con.execute(f"""CREATE OR REPLACE TEMP TABLE src AS
        SELECT *, {BBOX} FROM (
            SELECT * EXCLUDE ("{g}"), {geometry} AS geometry FROM raw
            WHERE "{g}" IS NOT NULL AND NOT ST_IsEmpty("{g}") AND ST_XMin("{g}") > -1e300)""")
    con.execute("DROP TABLE raw")
    close_rings(con)
    return crs


def close_rings(con) -> None:
    """Close the polygon rings in src whose last point is not their first
    (three in dsga_poly's shapefile). DuckDB reads them as they are; GEOS,
    and so every reader that hands them to it, refuses them."""
    import shapely

    bad = con.execute("""SELECT rowid, ST_AsWKB(geometry) FROM src
        WHERE ST_GeometryType(geometry) IN ('POLYGON', 'MULTIPOLYGON')
          AND TRY(ST_Simplify(geometry, 0)) IS NULL""").fetchall()
    for rowid, wkb in bad:
        fixed = shapely.from_wkb(bytes(wkb), on_invalid="fix")
        if fixed is None or fixed.is_empty:
            con.execute("DELETE FROM src WHERE rowid = ?", [rowid])
            continue
        con.execute("UPDATE src SET geometry = ST_SetCRS(ST_GeomFromWKB(?), "
                    "ST_CRS(geometry)) WHERE rowid = ?", [shapely.to_wkb(fixed), rowid])
    if bad:
        print(f"  closed the rings of {len(bad)} polygons", flush=True)


def write_layer(con, dest: Path, crs: str | None) -> dict:
    """src to GeoParquet 2.0 (bbox covering, Hilbert order, byte-capped row
    groups) or, without geometry, to plain Parquet."""
    raw = dest.with_suffix(".raw.parquet")
    if crs is None:
        n = con.execute("SELECT count(*) FROM src").fetchone()[0]
        con.execute(f"COPY src TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)")
        return {"file": dest.name, "features": n, "bytes": dest.stat().st_size,
                "row_groups": None, "geometry_types": [], "crs": None, "sha256": sha256(dest)}
    xmin, ymin, xmax, ymax, types, n = con.execute("""
        SELECT min(ST_XMin(geometry)), min(ST_YMin(geometry)), max(ST_XMax(geometry)),
               max(ST_YMax(geometry)), list(DISTINCT ST_GeometryType(geometry)::VARCHAR), count(*)
        FROM src""").fetchone()
    if not n:
        sys.exit(f"{dest.name}: no features")
    geo = json.dumps({
        "version": "2.0.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB",
            "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in types),
            "crs": projjson(con, crs),
            "bbox": [xmin, ymin, xmax, ymax],
            "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}},
        }},
    }).replace("'", "''")
    con.execute(f"""
        COPY (SELECT * FROM src ORDER BY ST_Hilbert(geometry,
                  ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))))
        TO '{raw}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', COMPRESSION ZSTD,
                    KV_METADATA {{geo: '{geo}'}})""")
    rewrite(raw, dest)
    raw.unlink()
    verify(con, dest.parent, [dest.stem])
    typ = con.execute(f"SELECT any_value(typeof(geometry)) FROM read_parquet('{dest}')").fetchone()[0]
    if crs not in typ:
        sys.exit(f"{dest.name}: CRS lost in the rewrite ({typ}, want {crs})")
    groups = con.execute(f"SELECT count(DISTINCT row_group_id) FROM parquet_metadata('{dest}')").fetchone()[0]
    return {"file": dest.name, "features": n, "bytes": dest.stat().st_size, "row_groups": groups,
            "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in types), "crs": crs,
            "bbox": [xmin, ymin, xmax, ymax], "sha256": sha256(dest)}


def lonlat_copy(con, parquet: Path, crs: str) -> Path:
    """The layer in lon/lat beside it, for tylertoo and H3."""
    lonlat = parquet.with_suffix(".lonlat.parquet")
    con.execute(f"""COPY (SELECT * EXCLUDE (geometry, bbox),
                                 ST_Transform(geometry, '{crs}', 'OGC:CRS84', always_xy := true)
                                     ::GEOMETRY('OGC:CRS84') AS geometry
                          FROM read_parquet('{parquet}')) TO '{lonlat}' (FORMAT PARQUET)""")
    return lonlat


def tile_layer(con, parquet: Path, layer_id: str, crs: str, types: list[str],
               start: int = 0) -> dict:
    """PMTiles from a lon/lat copy, down the ladder: every attribute, then
    the naming columns, then later minimum zooms, then a larger tile size
    cap (BIG_TILES). pmtiles.tile() fails when
    a tile goes over the size cap; that failure moves to the next step."""
    cols = [c for c, t, *_ in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet}')").fetchall()
            if c not in ("geometry", "bbox") and not t.startswith(("BLOB", "STRUCT", "GEOMETRY"))]
    # An overview's class column first, so the polygons can be coloured
    # like their cells.
    keep = [OVERVIEWS.get(layer_id, {}).get("class")]
    labels = ([c for c in keep if c in cols] + [c for c in cols if LABEL.search(c) and c not in keep])[:4]
    lonlat = lonlat_copy(con, parquet, crs)
    # "every_segment": no thinning either, so a map's streets have no gaps where
    # its tiles start; only its overview's columns, for the lowest zoom they fit.
    spec = OVERVIEWS.get(layer_id, {})
    every_segment = spec.get("every_segment", False)
    coverage = all(t.endswith("Polygon") for t in types) or every_segment
    dest = parquet.with_suffix(".pmtiles")
    steps = ([(spec["properties"], z, "500K") for z in MIN_ZOOMS if z >= start] if every_segment
             else [(cols, start, "500K")] + [(labels, z, "500K") for z in MIN_ZOOMS if z >= start]
             + [(labels, start, cap) for cap in BIG_TILES])
    try:
        for props, min_zoom, cap in steps:
            try:
                entry = {**pmtiles.tile(lonlat, dest, layer=layer_id, min_zoom=min_zoom,
                                        max_zoom=MAX_ZOOM, properties=props, coverage=coverage,
                                        extra=["--max-tile-size", cap, *(EVERY_SEGMENT if every_segment else [])]),
                         "max_tile_size": cap,
                         "all_properties": props == cols}
            except SystemExit as e:
                print(f"    tiles {layer_id}: {len(props)} properties from z{min_zoom}, "
                      f"cap {cap}: {e}", flush=True)
                continue
            return overview(con, lonlat, layer_id, types, entry, dest)
        sys.exit(f"{layer_id}: no tile setting fits the size cap")
    finally:
        lonlat.unlink(missing_ok=True)



# ---------- overviews ----------

# A layer whose tiles start at zoom 12 vanishes from a map zoomed out past a
# town. Below that zoom it gets an overview, joined into the same archive by
# pmtiles.pyramid() as Corine's is (clc_tiles.py): H3 cells with the
# dominant class, or the count per km2, or for the roads the major ones.
# A cell is 2 to 3 pixels wide across its band at 42 degrees north;
# Massachusetts is a few pixels wide below zoom 6, where the overview starts.
OVERVIEW_BANDS = [(6, 6, 6), (7, 8, 7), (9, 9, 8), (10, 11, 9)]  # (min zoom, max zoom, H3 resolution)
# Polygons are sampled at the centre of every resolution 11 cell (about
# 0.2 ha), 49 to a resolution 9 cell; points and polygons counted at the
# finest band resolution.
SAMPLE_RES = 11
CELLS_LAYER = "cells"
OVERVIEWS = {
    "lclu2016": {"class": "COVERNAME"},
    "vcforest_poly": {"class": "GRID_CODE"},
    "primeforest_poly": {"class": "PRIME"},
    "soils_mupolygon_top20": {"class": "HYDROLGRP"},
    "soils_poly_primefarmland": {"class": "FARMLNDCL"},
    "surfgeo24k_t_ay_poly": {"class": "AQFRYLD"},
    "l3_taxpar_poly": {"count": None},
    "census2020blocks_poly": {"count": "POP20"},
    "bedrock_data_points": {"count": None},
    # NG911 road classes 1 to 3: limited access highways, numbered routes,
    # major roads (52,000 of 452,000 segments). The tiles keep every segment
    # (thinned, they showed 49% of Cambridge's street length at zoom 12) and
    # the class, to colour a map by it at every zoom.
    "massgis_roads": {"where": "ROAD_CLASS IN ('1', '2', '3')",
                      "class": "ROAD_CLASS", "every_segment": True,
                      "properties": ["ROAD_CLASS", "ST_NAME"]},
}
# Class colours, the most common class first; past ten, OTHER.
PALETTE = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948", "#b07aa1",
           "#ff9da7", "#9c755f", "#bab0ac"]
OTHER = "#d9d9d9"
# Land cover by name, as land cover maps colour it (first match wins).
LAND_COVER = [("wetland", "#5fa89a"), ("impervious", "#8c8c8c"), ("developed", "#e3a59a"),
              ("evergreen", "#1e6b3a"), ("forest", "#68a84f"), ("water", "#4a86c5"),
              ("aquatic", "#4a86c5"), ("grass", "#d8d27a"), ("pasture", "#e6dc6e"),
              ("cultivated", "#b8864b"), ("scrub", "#b5c97a"), ("shore", "#efe3b0"),
              ("bare", "#d9c9a8")]
# Density, light to dark, cut at the sextiles of the finest band.
RAMP = ["#fff5eb", "#fdd0a2", "#fdae6b", "#fd8d3c", "#f16913", "#d94801", "#8c2d04"]


def class_colors(column: str, counts: list[tuple]) -> list[list]:
    """[value, colour] per class, the most sampled first."""
    if column == "COVERNAME":
        return [[v, next((c for k, c in LAND_COVER if k in str(v).lower()), OTHER)]
                for v, _ in counts]
    return [[v, PALETTE[i] if i < len(PALETTE) else OTHER] for i, (v, _) in enumerate(counts)]


def class_cells(con, lonlat: Path, column: str, resolutions: list[int], dest: Path) -> dict:
    """Every band cell's dominant class (named like the column), its share
    and how much of the cell the layer covers, from the resolution 11 cells
    whose centre falls in a polygon (any one, where polygons overlap)."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE samples AS
        SELECT cell, any_value(cls) AS cls FROM (
            SELECT unnest(h3_polygon_wkb_to_cells(ST_AsWKB(part), {SAMPLE_RES})) AS cell, cls
            FROM (SELECT unnest(ST_Dump(geometry)).geom AS part, "{column}" AS cls
                  FROM read_parquet('{lonlat}') WHERE "{column}" IS NOT NULL))
        GROUP BY cell""")
    counts = con.execute("SELECT cls, count(*) FROM samples GROUP BY 1 ORDER BY 2 DESC").fetchall()
    con.execute(f"""COPY (
        WITH p AS (SELECT r AS h3_res, h3_cell_to_parent(cell, r) AS parent, cls
                   FROM samples, (SELECT unnest({resolutions}) AS r)),
             c AS (SELECT h3_res, parent, cls, count(*) AS n FROM p GROUP BY ALL),
             t AS (SELECT h3_res, parent, sum(n) AS total, arg_max(cls, n) AS dominant,
                          max(n) AS top FROM c GROUP BY ALL)
        SELECT h3_h3_to_string(parent) AS h3, h3_res, dominant AS "{column}",
               round(100.0 * top / total)::UTINYINT AS dominant_pct,
               least(100, round(100.0 * total / pow(7, {SAMPLE_RES} - h3_res)))::UTINYINT
                   AS coverage_pct,
               ST_GeomFromWKB(h3_cell_to_boundary_wkb(parent)) AS geometry
        FROM t ORDER BY h3_res, parent) TO '{dest}' (FORMAT PARQUET)""")
    con.execute("DROP TABLE samples")
    return {"kind": "class", "column": column, "classes": class_colors(column, counts),
            "other": OTHER, "properties": ["h3", column, "dominant_pct", "coverage_pct"]}


def count_cells(con, lonlat: Path, total: str | None, points: bool, resolutions: list[int],
                dest: Path) -> dict:
    """Every band cell's feature count (each feature at a point on its
    surface), the sum of `total` when given, and per km2 of that."""
    at = "geometry" if points else "TRY(ST_PointOnSurface(geometry))"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE samples AS
        SELECT h3_latlng_to_cell(ST_Y(p), ST_X(p), {max(resolutions)}) AS cell, v FROM (
            SELECT {at} AS p, {f'"{total}"' if total else 1} AS v FROM read_parquet('{lonlat}'))
        WHERE p IS NOT NULL""")
    sums = f', sum(v) AS "{total}"' if total else ""
    con.execute(f"""COPY (
        WITH t AS (SELECT r AS h3_res, h3_cell_to_parent(cell, r) AS parent, count(*) AS n {sums}
                   FROM samples, (SELECT unnest({resolutions}) AS r) GROUP BY ALL)
        SELECT h3_h3_to_string(parent) AS h3, h3_res, n {f', "{total}"' if total else ""},
               round({f'"{total}"' if total else "n"} / h3_cell_area(parent, 'km^2'), 1) AS per_km2,
               ST_GeomFromWKB(h3_cell_to_boundary_wkb(parent)) AS geometry
        FROM t ORDER BY h3_res, parent) TO '{dest}' (FORMAT PARQUET)""")
    con.execute("DROP TABLE samples")
    q = con.execute(f"""SELECT quantile_cont(per_km2, [1/7, 2/7, 3/7, 4/7, 5/7, 6/7])
                        FROM read_parquet('{dest}') WHERE h3_res = {max(resolutions)}""").fetchone()[0]
    breaks = sorted({float(f"{b:.2g}") for b in q if b > 0})
    return {"kind": "count", "column": total, "density": "per_km2", "breaks": breaks,
            "ramp": RAMP[:len(breaks) + 1],
            "properties": ["h3", "n", *([total] if total else []), "per_km2"]}


def overview(con, lonlat: Path, layer_id: str, types: list[str], detail: dict,
             archive: Path) -> dict:
    """The layer's archive with the overview bands below its tiles' first
    zoom, for the OVERVIEWS layers; else the archive as it is."""
    spec = OVERVIEWS.get(layer_id)
    hand = detail["min_zoom"]
    bands = [(lo, min(hi, hand - 1), res) for lo, hi, res in OVERVIEW_BANDS if lo < hand]
    if not spec or not bands:
        return detail
    con.execute("INSTALL h3 FROM community; LOAD h3")
    work = archive.parent
    archives = []
    if "where" in spec:
        lo, hi = bands[0][0], bands[-1][1]
        major = work / f"{layer_id}.major.parquet"
        con.execute(f"COPY (SELECT * FROM read_parquet('{lonlat}') WHERE {spec['where']}) "
                    f"TO '{major}' (FORMAT PARQUET)")
        band = work / f"{layer_id}.z{lo}-{hi}.pmtiles"
        try:
            pmtiles.tile(major, band, layer=layer_id, min_zoom=lo, max_zoom=hi,
                         properties=spec["properties"], coverage=False)
        finally:
            major.unlink(missing_ok=True)
        archives.append((lo, hi, band, layer_id))
        info = {"kind": "major", "filter": spec["where"], "properties": spec["properties"],
                "layer": layer_id}
    else:
        cells = work / f"{layer_id}.cells.parquet"
        resolutions = [res for *_, res in bands]
        if "class" in spec:
            info = class_cells(con, lonlat, spec["class"], resolutions, cells)
        else:
            info = count_cells(con, lonlat, spec["count"], types == ["Point"], resolutions, cells)
        info["layer"] = CELLS_LAYER
        try:
            for lo, hi, res in bands:
                src = work / f"{layer_id}.r{res}.parquet"
                con.execute(f"COPY (SELECT * EXCLUDE (h3_res) FROM read_parquet('{cells}') "
                            f"WHERE h3_res = {res}) TO '{src}' (FORMAT PARQUET)")
                band = work / f"{layer_id}.z{lo}-{hi}.pmtiles"
                try:
                    # Hexagons are drawn as they are: simplifying would only bend them.
                    pmtiles.tile(src, band, layer=CELLS_LAYER, min_zoom=lo, max_zoom=hi,
                                 properties=info["properties"], verbatim=True)
                finally:
                    src.unlink(missing_ok=True)
                archives.append((lo, hi, band, CELLS_LAYER))
        finally:
            cells.unlink(missing_ok=True)
    joined = archive.with_suffix(".joined.pmtiles")
    try:
        entry = pmtiles.pyramid([*archives, (hand, detail["max_zoom"], archive, layer_id)], joined)
    finally:
        for _, _, band, _ in archives:
            band.unlink(missing_ok=True)
    joined.replace(archive)
    entry["file"] = archive.name
    info["handover"] = hand
    if "where" in spec:
        info["bands"] = [{"min_zoom": bands[0][0], "max_zoom": bands[-1][1]}]
    else:
        info["bands"] = [{"min_zoom": lo, "max_zoom": hi, "h3_res": res} for lo, hi, res in bands]
    return {**detail, **entry, "overview": info}

def zip_metadata(path: Path, member: str) -> dict:
    """Title, abstract and use constraints from the shapefile's .shp.xml
    (FGDC or ArcGIS metadata), when MassGIS ships one."""
    if not member.lower().endswith(".shp"):
        return {}
    with zipfile.ZipFile(path) as z:
        names = {n.lower(): n for n in z.namelist()}
        x = names.get(member.lower() + ".xml")
        if not x:
            return {}
        text = z.read(x).decode("utf-8", "replace")

    def first(*tags: str) -> str | None:
        for tag in tags:
            m = re.search(rf"<{tag}[^>]*>(.*?)</{tag}>", text, re.DOTALL)
            if m:
                # ArcGIS metadata holds the text as escaped HTML: unescape,
                # then drop the tags.
                value = re.sub(r"<[^>]+>", " ", html.unescape(m.group(1)))
                value = re.sub(r"\s+", " ", html.unescape(value)).strip()
                if value:
                    return value
        return None

    return {k: v for k, v in {
        "title": first("title", "resTitle"),
        "description": first("abstract", "idAbs"),
        "use_constraints": first("useconst", "useLimit"),
    }.items() if v}


# ---------- thumbnails ----------

# The catalog shows one image per collection (PORTO-CORE-067). Each build
# renders it beside the layer, so it follows the data.
COLORS = {
    "parcels": "#b5553c", "buildings": "#8c3b2a", "boundaries": "#8e44ad", "regions": "#6c5aa8",
    "census": "#2a78d6", "transportation": "#4a4a4a", "water": "#2f7fc1", "wetlands": "#1b8a8a",
    "habitat": "#3f8a4a", "open_space": "#5aa02c", "land_cover": "#7a9a3a",
    "geology_soils": "#9a6b3a", "elevation": "#7f6a52", "hazards": "#d2553a",
    "regulated_sites": "#c0392b", "facilities": "#d35400", "historic": "#a0522d",
    "utilities": "#c9a100", "names": "#34495e", "new_england": "#6b7b8c",
}
# Fewer features than this are drawn; more, as features per pixel.
DRAW_BELOW = 60_000


def thumbnail(con, parquet: Path, crs: str | None, dest: Path, theme: str, title: str,
              outline: Path | None) -> dict:
    """A 600 x 400 PNG: the layer over the state outline, framed on its
    extent; a table without geometry gets a card with its row count."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import shapely
    from matplotlib.collections import LineCollection, PolyCollection
    from matplotlib.colors import LinearSegmentedColormap

    from thumbnails import BACKGROUND, REGION_EDGE, REGION_FILL, frame

    color = COLORS[theme]
    fig = plt.figure(figsize=(6, 4), dpi=100)
    ax = fig.add_axes((0, 0, 1, 1))
    ax.axis("off")
    fig.patch.set_facecolor(BACKGROUND)
    src = f"read_parquet('{parquet}')"
    if crs is None:
        n = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
        ax.text(0.5, 0.58, title, ha="center", va="center", fontsize=15, color="#333333",
                wrap=True)
        ax.text(0.5, 0.38, f"table, {n:,} rows", ha="center", va="center", fontsize=12,
                color=color)
    else:
        to84 = f"ST_Transform({{g}}, '{crs}', 'OGC:CRS84', always_xy := true)"
        box = con.execute(f"""SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e) FROM (
            SELECT ST_Extent_Agg({to84.format(g="ST_MakeEnvelope(bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax)")}) AS e
            FROM {src})""").fetchone()
        x0, y0, x1, y1, aspect = frame(*box)
        tol = (x1 - x0) / 1200
        if outline is not None and outline.exists():
            for (wkb,) in con.execute(f"""SELECT ST_AsWKB(ST_Simplify(
                    ST_Transform(geometry, '{MASS_CRS}', 'OGC:CRS84', always_xy := true), {tol}))
                    FROM read_parquet('{outline}')""").fetchall():
                g = shapely.from_wkb(bytes(wkb))
                for poly in getattr(g, "geoms", [g]):
                    if poly.is_empty or poly.geom_type != "Polygon":
                        continue
                    ax.add_collection(PolyCollection([list(poly.exterior.coords)],
                                                     facecolors=REGION_FILL, edgecolors=REGION_EDGE,
                                                     linewidths=0.4))
        n = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
        if n < DRAW_BELOW:
            polys, lines, pts = [], [], []
            for (wkb,) in con.execute(f"SELECT ST_AsWKB(ST_Simplify({to84.format(g='geometry')}, {tol})) "
                                      f"FROM {src}").fetchall():
                g = shapely.from_wkb(bytes(wkb))
                for part in getattr(g, "geoms", [g]):
                    if part.is_empty:
                        continue
                    if part.geom_type == "Polygon":
                        polys.append(list(part.exterior.coords))
                    elif part.geom_type == "LineString":
                        lines.append(list(part.coords))
                    elif part.geom_type == "Point":
                        pts.append((part.x, part.y))
            ax.add_collection(PolyCollection(polys, facecolors=color + "55", edgecolors=color,
                                             linewidths=0.4))
            ax.add_collection(LineCollection(lines, colors=color, linewidths=0.6))
            if pts:
                ax.scatter(*zip(*pts), s=3 if len(pts) > 3000 else 8, c=color, linewidths=0)
        else:
            nx, ny = 600, 400
            cells = con.execute(f"""
                SELECT floor((ST_X(c) - {x0}) / {(x1 - x0) / nx})::INT, floor((ST_Y(c) - {y0}) / {(y1 - y0) / ny})::INT,
                       count(*)
                FROM (SELECT {to84.format(g="ST_Point((bbox.xmin + bbox.xmax) / 2, (bbox.ymin + bbox.ymax) / 2)")} AS c
                      FROM {src}) GROUP BY 1, 2""").fetchall()
            grid = np.zeros((ny, nx))
            for i, j, k in cells:
                if 0 <= i < nx and 0 <= j < ny:
                    grid[j, i] = k
            cmap = LinearSegmentedColormap.from_list(theme, [BACKGROUND + "00", color])
            ax.imshow(np.log1p(grid), origin="lower", extent=(x0, x1, y0, y1), cmap=cmap,
                      vmin=0, vmax=max(np.log1p(grid).max(), 1) * 0.8, interpolation="nearest",
                      zorder=3)
        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
        ax.set_aspect(aspect)
    fig.savefig(dest, dpi=100, facecolor=BACKGROUND)
    plt.close(fig)
    return {"file": dest.name, "bytes": dest.stat().st_size, "sha256": sha256(dest)}


def get_json(url: str, default):
    try:
        if url.startswith(("http://", "https://")):
            with urllib.request.urlopen(url, timeout=60) as r:
                return json.load(r)
        p = Path(url)
        return json.loads(p.read_text()) if p.exists() else default
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return default
        raise


def attribution(theme: str, entries: list[dict]) -> str:
    lines = "\n".join(f"  {e['name']:<34} {e['source']['zips'][0]}" for e in entries)
    return f"""\
# Data attribution

These layers are published by MassGIS, the Massachusetts Bureau of Geographic
Information (Executive Office of Technology Services and Security), and by
the state agencies it publishes for, in its public download bucket:

  Bucket:     {BUCKET}
  Layers:     {PAGE}
  Producer:   MassGIS, Commonwealth of Massachusetts

{THEMES[theme]}: each layer and the zip it comes from (the Last-Modified of
each is in _manifest.json):

{lines}

MassGIS distributes its data free of charge and without restriction; each
layer's own use constraints, when its metadata states any, are in
_manifest.json (use_constraints). Credit MassGIS (and the agency the layer's
metadata names) when you use it. The data is provided as is, without
warranty; MassGIS's metadata describes each layer's scale, sources and
limits.

## What was changed here

One GeoParquet 2.0 file per layer. Columns and values are MassGIS's,
unchanged. The geometry is 2D, in the layer's own CRS as published, with a
`bbox` column declared as the GeoParquet covering, rows sorted along a
Hilbert curve. Tables without geometry are plain Parquet. The PMTiles
archives beside them are derived for web maps, in lon/lat.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def build_layer(con, r: dict, info: dict, src: Path, work: Path, out: Path,
                no_tiles: bool) -> dict:
    """One layer: fetch its zips, write and verify the GeoParquet, tile it,
    draw its thumbnail. Returns its manifest entry, also kept beside the
    files as <id>.entry.json."""
    folder = out / r["theme"]
    folder.mkdir(parents=True, exist_ok=True)
    listing = list_bucket({r["zip"].split("*")[0]}) if "*" in r["zip"] else {
        r["zip"]: {"bytes": info["bytes"]}}
    keys = zips(r, listing)
    paths = [fetch(k, src, listing[k]["bytes"]) for k in keys]
    try:
        crs = load(con, sources(r, paths), work)
    finally:
        for f in work.glob("*.fgb"):
            f.unlink(missing_ok=True)
    dest = folder / f"{r['id']}.parquet"
    entry = write_layer(con, dest, crs)
    meta = zip_metadata(paths[0], r["member"])
    tiles = None
    if crs and not no_tiles:
        tiles = tile_layer(con, dest, r["id"], crs, entry["geometry_types"],
                           int(r["min_zoom"] or 0))
    title = r["title"] or meta.get("title") or r["id"]
    thumb = thumbnail(con, dest, crs, folder / f"{r['id']}.png", r["theme"], title,
                      out / "boundaries" / "outline25k_poly.parquet")
    e = {"name": r["id"], "theme": r["theme"], "title": title,
         **{k: v for k, v in meta.items() if k != "title"},
         **entry, "source": info, "pmtiles": tiles, "thumbnail": thumb}
    con.execute("DROP TABLE IF EXISTS src")
    (folder / f"{r['id']}.entry.json").write_text(json.dumps(e, indent=2) + "\n")
    return e


def build(src: Path, work: Path, out: Path, *, public: str, only: list[str] | None,
          force: bool, no_tiles: bool) -> None:
    work.mkdir(parents=True, exist_ok=True)
    rows = layers()
    source = upstream()
    published = get_json(f"{public}/_source.json", {})
    con = duckdb.connect()
    # Land cover (22.8 million polygons) outgrows memory: past the limit
    # DuckDB spills to the work dir instead.
    con.execute(f"INSTALL spatial; LOAD spatial; SET temp_directory = '{work}'; "
                "SET memory_limit = '6GB'")
    t0 = time.monotonic()

    def done(r: dict) -> dict | None:
        """This out dir's entry for the layer, if built from the current zips:
        a rerun after a failure resumes instead of starting over."""
        f = out / r["theme"] / f"{r['id']}.entry.json"
        e = json.loads(f.read_text()) if f.is_file() else None
        return e if e and e["source"]["fingerprint"] == source[r["id"]]["fingerprint"] else None

    if only:
        todo = [r for r in rows if r["id"] in only]
    else:
        todo = [r for r in rows if force or published.get(r["id"], {}).get("fingerprint")
                != source[r["id"]]["fingerprint"]]
    built: dict[str, dict] = {}
    for r in list(todo):
        if not only and (e := done(r)):
            built[r["id"]] = e
            todo.remove(r)
    print(f"  {len(rows)} layers, {len(todo)} to build"
          + (f", {len(built)} already built here" if built else ""), flush=True)
    failed: dict[str, str] = {}
    for r in todo:
        try:
            built[r["id"]] = build_layer(con, r, source[r["id"]], src, work, out, no_tiles)
        except (Exception, SystemExit) as e:  # noqa: BLE001 - one layer must not stop the others
            failed[r["id"]] = f"{type(e).__name__}: {e}"[:500]
            print(f"  FAIL {r['theme']}/{r['id']}: {failed[r['id']]}", flush=True)
            # Nothing of a failed layer may reach the upload: the published
            # files stay as they are.
            for f in (out / r["theme"]).glob(f"{r['id']}.*"):
                f.unlink(missing_ok=True)
            continue
        e = built[r["id"]]
        tiles = e["pmtiles"]
        print(f"  {r['theme']}/{r['id']}: {e['features']:,} rows, {e['bytes'] / 1e6:.1f} MB"
              + (f", tiles {tiles['bytes'] / 1e6:.1f} MB from z{tiles['min_zoom']}"
                 f"{'' if tiles['all_properties'] else ' (naming columns)'}"
                 f"{'' if tiles['max_tile_size'] == '500K' else ', cap ' + tiles['max_tile_size']}"
                 if tiles else "")
              + f", {time.monotonic() - t0:.0f} s", flush=True)
    # Manifests: the published entries, with the layers just built replaced.
    by_theme: dict[str, list[dict]] = {}
    absent = []
    for r in rows:
        theme = r["theme"]
        if r["id"] in built:
            by_theme.setdefault(theme, []).append(built[r["id"]])
            continue
        prev = {f["name"]: f for f in get_json(f"{public}/{theme}/_manifest.json", {}).get("files", [])}
        if r["id"] in prev:
            by_theme.setdefault(theme, []).append(prev[r["id"]])
        else:
            absent.append(r["id"])
    if absent:
        print(f"  {len(absent)} layers neither built nor published: left out of the manifests",
              flush=True)
    for theme, entries in by_theme.items():
        folder = out / theme
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "ATTRIBUTION.txt").write_text(attribution(theme, entries))
        crs = {e["crs"] for e in entries if e.get("crs")}
        (folder / "_manifest.json").write_text(json.dumps({
            "state_name": f"MassGIS: {THEMES[theme]}",
            **({"crs": crs.pop()} if len(crs) == 1 else {}),
            "total_features": sum(e["features"] for e in entries),
            "themes": {e["name"]: e["features"] for e in entries},
            "labels": {e["name"]: e["title"] for e in entries},
            "files": entries,
            "pmtiles": [{"name": e["name"], **e["pmtiles"]} for e in entries if e.get("pmtiles")],
        }, indent=2) + "\n")
    (out / "index.json").write_text(json.dumps({"datasets": [
        {"path": theme, "code": theme, "name": f"MassGIS: {title}"}
        for theme, title in THEMES.items() if theme in by_theme]}, indent=2) + "\n")
    (out / "snapshots.json").write_text(json.dumps({"latest": "", "snapshots": []}) + "\n")
    # A layer not built keeps its published fingerprint, so the next run
    # tries it again.
    (out / "_source.json").write_text(json.dumps(
        {r["id"]: source[r["id"]] if r["id"] in built else published[r["id"]]
         for r in rows if r["id"] in built or r["id"] in published}, indent=2) + "\n")
    (out / "failed.json").write_text(json.dumps(failed, indent=2) + "\n")
    if failed:
        print(f"  {len(failed)} layers failed (failed.json): {', '.join(failed)}", flush=True)


def add_overviews(work: Path, out: Path, *, public: str, only: list[str] | None,
                  force: bool) -> None:
    """Give published layers their overview from their published files, no
    MassGIS download: <out>/<theme>/<layer>.pmtiles and the theme's
    _manifest.json, to upload in that order. A layer whose tiles lack its
    class column is tiled again first."""
    work.mkdir(parents=True, exist_ok=True)
    themes = {r["id"]: r["theme"] for r in layers()}
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; SET temp_directory = '{work}'; "
                "SET memory_limit = '6GB'")
    manifests = {}
    for layer_id in only or OVERVIEWS:
        theme = themes[layer_id]
        if theme not in manifests:
            # This out dir's, where an earlier run left one: it holds that
            # run's layers.
            local = out / theme / "_manifest.json"
            manifests[theme] = get_json(str(local) if local.is_file()
                                        else f"{public}/{theme}/_manifest.json", None)
        manifest = manifests[theme]
        e = next(f for f in manifest["files"] if f["name"] == layer_id)
        tiles = e["pmtiles"]
        if tiles.get("overview") and not force:
            print(f"  {layer_id}: has its overview", flush=True)
            continue
        t0 = time.monotonic()
        folder = out / theme
        folder.mkdir(parents=True, exist_ok=True)
        parquet, archive = work / e["file"], folder / tiles["file"]
        for f in (parquet, archive):
            fetch_public(f"{public}/{theme}/{f.name}", f)
        column = OVERVIEWS[layer_id].get("class")
        if OVERVIEWS[layer_id].get("every_segment") or (
                column and not tiles.get("all_properties") and column not in tiles["properties"]):
            tiles = tile_layer(con, parquet, layer_id, e["crs"], e["geometry_types"],
                               tiles["min_zoom"])
            tiles["file"] = archive.name
            archive.unlink()
            (work / tiles["file"]).replace(archive)
        else:
            lonlat = lonlat_copy(con, parquet, e["crs"])
            try:
                tiles = overview(con, lonlat, layer_id, e["geometry_types"], tiles, archive)
            finally:
                lonlat.unlink(missing_ok=True)
        parquet.unlink()
        e["pmtiles"] = tiles
        manifest["pmtiles"] = [{"name": f["name"], **f["pmtiles"]}
                               for f in manifest["files"] if f.get("pmtiles")]
        (folder / "_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"  {theme}/{layer_id}: {tiles['bytes'] / 1e6:.1f} MB, z{tiles['min_zoom']} to "
              f"z{tiles['max_zoom']}, {time.monotonic() - t0:.0f} s", flush=True)


def fetch_public(url: str, dest: Path) -> None:
    with urllib.request.urlopen(url, timeout=300) as r, dest.open("wb") as f:
        shutil.copyfileobj(r, f, 1 << 20)

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="print each layer's source fingerprint")
    b = sub.add_parser("build", help="fetch, convert, tile and verify the layers that changed")
    b.add_argument("--src-dir", type=Path, required=True, help="where the zips are kept")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help="the massgis/ prefix")
    b.add_argument("--public", default=PUBLIC, help="where the published manifests are read")
    b.add_argument("--layer", action="append", help="build this layer (repeatable)")
    b.add_argument("--force", action="store_true", help="build every layer")
    b.add_argument("--no-tiles", action="store_true", help="GeoParquet only (a test run)")
    o = sub.add_parser("overviews", help="add the overviews to published layers")
    o.add_argument("--work-dir", type=Path, default=Path("work"))
    o.add_argument("--out-dir", type=Path, required=True, help="the massgis/ prefix")
    o.add_argument("--public", default=PUBLIC)
    o.add_argument("--layer", action="append", help="this layer only (repeatable)")
    o.add_argument("--force", action="store_true", help="also layers that have one")
    args = p.parse_args()
    if args.cmd == "overviews":
        add_overviews(args.work_dir, args.out_dir, public=args.public, only=args.layer,
                      force=args.force)
        return
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
        return
    build(args.src_dir, args.work_dir, args.out_dir, public=args.public, only=args.layer,
          force=args.force, no_tiles=args.no_tiles)


if __name__ == "__main__":
    main()
