#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog of the gazetteers.

  gazetteer/catalog/catalog.json                       root: Gazetteers
  gazetteer/catalog/census-2026/catalog.json           the Census Gazetteer, one
  gazetteer/catalog/census-2026/<code>/collection.json   collection per layer
  gazetteer/catalog/geonames/catalog.json              GeoNames
  gazetteer/catalog/geonames/geonames/collection.json    every name, world file and
                                                         one file per country
  gazetteer/catalog/geonames/cities1000/collection.json  places of 1,000 or more,
                                                         with PMTiles for labels

One catalog for both: one URL for whoever lists it, and the two answer the
same question (a name to a point) for different footprints. gazetteer.py's
tables drive it; counts, sizes and checksums come from each part's
_manifest.json, local (--out-dir) or published. Spec: portolan-spec v0.2.0.
Validator: rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/gazetteer_catalog.py build --dest build/gazetteer-catalog [--out-dir out/gazetteer]
  python3 scripts/datasets/gazetteer_catalog.py check build/gazetteer-catalog
  python3 scripts/datasets/gazetteer_catalog.py publish --remote parquetry:parquetry/gazetteer [--out-dir out/gazetteer]
  uv run --project scripts --with matplotlib \\
      python scripts/datasets/gazetteer_catalog.py thumbnails --out-dir out/gazetteer
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.gaul_catalog import STYLE_TYPE, WEB_MAP_LINKS_EXT
from datasets.gazetteer import (
    CENSUS_LAYERS, CENSUS_PAGE, FEATURE_CLASSES, GEONAMES, GEONAMES_PAGE,
    NO_COUNTRY, SEATS, YEAR,
)
from datasets.pmtiles import PMTILES_TYPE

import catalog as shared  # scripts/catalog.py
import filedates  # noqa: E402  scripts/filedates.py
from extent import lon_intervals, wrapped_bbox
from catalog import (
    ALTERNATE_EXT, BUCKET, CONTACT_EMAIL, FILE_EXT, LOGO, PARQUET_TYPE, PARTITION_EXT,
    PORTOLAN_SCHEMA, PUBLIC_BASE, REPO, REPO_URL, S3_ENDPOINT, SITE_URL, TABLE_EXT,
    VERSION_EXT, md_link, multihash, now_rfc3339, write_json,
)

DATASET_PREFIX = "gazetteer"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
# The workflow stamps what it uploads (scripts/filedates.py): `updated` is
# when the data last changed, `checked` the last build.
STAMPS = [f"{PUBLIC_DATA}/_files/gazetteer.json"]
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "gazetteer"
TITLE = "Gazetteers: US Census and GeoNames"
THUMBS = REPO / "catalog" / "thumbnails" / "gazetteer"
PROJ_EXT = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
CENSUS_ID = f"census-{YEAR}"
CENSUS_PATH = f"census/{YEAR}"
GEONAMES_PATH = "geonames/latest"

CENSUS_ATTRIBUTION = f"U.S. Census Bureau, {YEAR} Gazetteer Files"
GEONAMES_ATTRIBUTION = "GeoNames (www.geonames.org), CC BY 4.0"
CENSUS_LICENSE = {
    "rel": "license", "href": "https://www.usa.gov/government-copyright", "type": "text/html",
    "title": "Works of the US federal government are not subject to copyright in the "
             "United States (17 U.S.C. 105)",
}
GEONAMES_LICENSE = {
    "rel": "license", "href": "https://creativecommons.org/licenses/by/4.0/",
    "type": "text/html", "title": "Creative Commons Attribution 4.0 International (CC BY 4.0)",
}
CENSUS_VIA = {"rel": "via", "href": CENSUS_PAGE, "type": "text/html",
              "title": "Gazetteer Files on census.gov, where they are published"}
GEONAMES_VIA = {"rel": "via", "href": f"{GEONAMES}/", "type": "text/html",
                "title": "The GeoNames dump files, where they are published"}
HOST = {
    "name": "Geomermaids",
    "description": "Converted the files to GeoParquet and maintains and hosts this "
                   "catalog and its data.",
    "url": SITE_URL, "email": CONTACT_EMAIL, "roles": ["processor", "host"],
}
CENSUS_PROVIDERS = [
    {"name": "U.S. Census Bureau, Geography Division",
     "description": "Publishes the Gazetteer Files with each TIGER/Line vintage. A US "
                    "government work, not subject to copyright in the United States.",
     "url": CENSUS_PAGE, "roles": ["producer", "licensor"]},
    HOST,
]
GEONAMES_PROVIDERS = [
    {"name": "GeoNames",
     "description": "Collects and edits the GeoNames database from many sources and "
                    "publishes it daily as dump files under CC BY 4.0.",
     "url": GEONAMES_PAGE, "roles": ["producer", "licensor"]},
    HOST,
]

CENSUS_COLUMNS = {
    "USPS": "Postal code of the state the area is in.",
    "GEOID": "The area's geographic identifier, as in TIGER/Line: joins the TIGER layer "
             "of the same code (GEOID20 for zcta520 and uac20) and the ACS tables.",
    "GEOIDFQ": "The fully qualified GEOID, as data.census.gov writes it.",
    "ANSICODE": "The ANSI feature code (GNIS identifier).",
    "NAME": "The area's name, with its legal or statistical description where the "
            "Census Bureau gives one.",
    "LSAD": "Legal/statistical area description code (city, town, CDP...).",
    "FUNCSTAT": "Functional status: A active government, S statistical, F fictitious...",
    "CSAFP": "The combined statistical area the CBSA belongs to, if any.",
    "CBSA_TYPE": "1 metropolitan, 2 micropolitan statistical area.",
    "LOGRADE": "Lowest grade the school district serves.",
    "HIGRADE": "Highest grade the school district serves.",
    "ALAND": "Land area, square meters.",
    "AWATER": "Water area, square meters.",
    "ALAND_SQMI": "Land area, square miles.",
    "AWATER_SQMI": "Water area, square miles.",
    "INTPTLAT": "Latitude of the internal point, NAD83 decimal degrees.",
    "INTPTLONG": "Longitude of the internal point, NAD83 decimal degrees.",
    "bbox": "The point's bounding box (float32, rounded outward): the GeoParquet "
            "covering, for filtering on a box before touching geometry.",
    "geometry": "The internal point (INTPTLONG, INTPTLAT), NAD83 (EPSG:4269). Inside the "
                "area, near its centre: a label point, not a centroid.",
}
GEONAMES_COLUMNS = {
    "geonameid": "GeoNames' identifier: https://www.geonames.org/<geonameid>.",
    "name": "The name, UTF-8.",
    "asciiname": "The name in plain ASCII.",
    "alternatenames": "Other names, comma-separated, with ASCII transliterations: the "
                      "readme's convenience copy of the alternate names table.",
    "latitude": "WGS84 decimal degrees.",
    "longitude": "WGS84 decimal degrees.",
    "feature_class": "One letter: " + ", ".join(f"{k} {v}" for k, v in FEATURE_CLASSES.items())
                     + ". https://www.geonames.org/export/codes.html",
    "feature_class_name": "What the feature class letter stands for.",
    "feature_code": "The feature code within the class: PPL populated place, PPLC capital, "
                    "ADM1 first-order division, MT mountain, LK lake...",
    "feature_name": "The feature code's name, from featureCodes_en.txt.",
    "feature_description": "The feature code's description, from featureCodes_en.txt.",
    "country_code": "ISO 3166-1 alpha-2 country code; NULL for oceans and undersea features.",
    "country_name": "The country's name, from countryInfo.txt.",
    "cc2": "Alternate country codes, comma-separated.",
    "admin1_code": "First-order division code (FIPS or ISO, see the GeoNames readme).",
    "admin1_name": "First-order division name, from admin1CodesASCII.txt.",
    "admin2_code": "Second-order division code (a county in the US).",
    "admin2_name": "Second-order division name, from admin2Codes.txt.",
    "admin3_code": "Third-order division code.",
    "admin4_code": "Fourth-order division code.",
    "population": "Population as GeoNames records it, 0 when unknown.",
    "elevation": "Elevation, meters, when recorded.",
    "dem": "Elevation from a digital elevation model (SRTM3 or GTOPO30), meters.",
    "timezone": "IANA time zone.",
    "modification_date": "When GeoNames last changed the record.",
    "bbox": CENSUS_COLUMNS["bbox"],
    "geometry": "The point (longitude, latitude), WGS84 lon/lat (OGC:CRS84).",
}


# ---------- reading ----------

def part_url(path: str) -> str:
    return f"{PUBLIC_DATA}/{path}"


def read_manifest(out_dir: Path | None, path: str) -> dict:
    local = out_dir / path / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    with urllib.request.urlopen(f"{part_url(path)}/_manifest.json") as r:
        return json.load(r)


def data_source(out_dir: Path | None, path: str, file: str) -> str:
    local = out_dir / path / file if out_dir else None
    return str(local) if local and local.is_file() else f"{part_url(path)}/{file}"


def connect():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    return con


def footer(con, source: str) -> list[tuple[str, str]]:
    return [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
            con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()]


def extent(con, source: str) -> list[float]:
    """Measured on the data; west > east when it crosses the antimeridian."""
    src = f"read_parquet('{source}')"
    y0, y1 = con.execute(f"SELECT min(bbox.ymin), max(bbox.ymax) FROM {src}").fetchone()
    b = wrapped_bbox(lon_intervals(con, src), y0, y1)
    return [round(b[0], 6), max(-90.0, round(b[1], 6)), round(b[2], 6), min(90.0, round(b[3], 6))]


def table_columns(columns: list[tuple[str, str]], docs: dict, what: str) -> list[dict]:
    undocumented = [c for c, _ in columns if c not in docs]
    if undocumented:
        sys.exit(f"{what}: no column doc for {undocumented}")
    return [{"name": n, "type": t, "description": docs[n]} for n, t in columns]


def s3(path: str) -> dict:
    return {"s3": {"href": f"s3://{BUCKET}/{path}",
                   "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}


def thumbnail_asset(name: str, title: str) -> dict:
    thumb = THUMBS / f"{name}.png"
    if not thumb.is_file():
        sys.exit(f"missing {thumb}: run the thumbnails command")
    return {"href": "./thumbnail.png", "type": "image/png", "title": title,
            "roles": ["thumbnail"], "file:size": thumb.stat().st_size,
            "file:checksum": multihash(thumb)}


def data_asset(path: str, entry: dict, title: str, proj: str | None = None) -> dict:
    asset = {
        "href": f"{PUBLIC_BASE}/{DATASET_PREFIX}/{path}",
        "type": PARQUET_TYPE, "title": title,
        "description": f"{entry['features']:,} rows in {entry['row_groups']} row group(s) "
                       f"of at most 4 MiB of geometry.",
        "roles": ["data"],
        "file:size": entry["bytes"], "file:checksum": "1220" + entry["sha256"],
        "alternate": s3(f"{DATASET_PREFIX}/{path}"),
    }
    if proj:
        asset["proj:code"] = proj
    return asset


# ---------- Census ----------

def census_collection(code: str, layer: str, title: str, entry: dict, gate: dict,
                      columns: list[tuple[str, str]], bbox: list[float], updated: str) -> dict:
    path = f"{CENSUS_PATH}/{entry['file']}"
    tiger = gate.get("tiger")
    join = (f" Its GEOID joins census/tiger/{YEAR} `{'/'.join(tiger['file'].rsplit('/', 2)[-2:])}`"
            f" on `{tiger['key']}`: "
            + ("every row here is there (the build fails otherwise)"
               if not tiger["gazetteer_not_in_tiger"] else
               f"{tiger['gazetteer_not_in_tiger']:,} rows here are not there")
            + (f"; {tiger['tiger_not_in_gazetteer']:,} TIGER areas are not here (the Gazetteer "
               "leaves out the island areas and a few others)."
               if tiger["tiger_not_in_gazetteer"] else ".")
            if tiger else " parquetry has no TIGER/Line layer of these areas.")
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT,
                            PROJ_EXT],
        "id": f"{CENSUS_ID}-{code}",
        "title": f"{title}, Census Gazetteer {YEAR}",
        "description": (
            f"The {YEAR} Census Gazetteer file `{YEAR}_Gaz_{layer}_national`: "
            f"{entry['features']:,} areas, one row each with its name, land and water area and "
            f"an internal point. Columns and values unchanged; the internal point as a NAD83 "
            f"(EPSG:4269) point geometry with a bbox covering, Hilbert-sorted, one GeoParquet "
            f"2.0 file read in place over HTTPS or the anonymous S3 endpoint {S3_ENDPOINT}."
            f"{join} Credit: {CENSUS_ATTRIBUTION}."),
        "keywords": ["gazetteer", "US Census Bureau", "place names", "geocoding", code,
                     "GeoParquet"],
        "license": "other",
        "attribution": CENSUS_ATTRIBUTION,
        "version": YEAR,
        "updated": updated,
        "providers": CENSUS_PROVIDERS,
        "extent": {"spatial": {"bbox": [bbox]},
                   "temporal": {"interval": [[f"{YEAR}-01-01T00:00:00Z",
                                              f"{YEAR}-12-31T23:59:59Z"]]}},
        "table:row_count": entry["features"],
        "table:primary_geometry": "geometry",
        "table:columns": table_columns(columns, CENSUS_COLUMNS, f"census/{code}"),
        "assets": {
            "data": data_asset(path, entry, f"{title}, {YEAR}", "EPSG:4269"),
            "thumbnail": thumbnail_asset(f"census-{code}", f"{title}, internal points"),
        },
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{title}: README"),
            md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
            CENSUS_LICENSE, CENSUS_VIA,
            {"rel": "alternate", "href": f"{part_url(CENSUS_PATH)}/", "type": "text/html",
             "title": "Browse the files"},
        ],
    }


def census_access(code: str) -> str:
    return f"""\
```sql
INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;
SELECT GEOID, NAME, INTPTLAT, INTPTLONG
FROM read_parquet('{part_url(CENSUS_PATH)}/{code}.parquet')
WHERE NAME ILIKE 'Springfield%'
ORDER BY ALAND DESC;
```"""


def census_readme(col: dict, code: str, entry: dict) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, sha256 `{entry['sha256'][:12]}...` |
| Rows | {entry['features']:,} |
| CRS | NAD83, EPSG:4269 (geographic) |

## Access

{census_access(code)}

## Schema

| Column | Type | Description |
|---|---|---|
{schema}

## License

Public domain in the United States (17 U.S.C. 105). Credit "{CENSUS_ATTRIBUTION}".
Source: {CENSUS_PAGE}. Built by [gazetteer.py]({REPO_URL}/blob/main/scripts/datasets/gazetteer.py).
"""


def census_agents(col: dict, code: str) -> str:
    return f"""\
# {col['id']}: agent guide

One row per area: `GEOID`, `NAME`, land and water area, and an internal
point (`INTPTLAT`, `INTPTLONG`, and `geometry`, NAD83). A name lookup or a
point for an area; join TIGER/Line or the ACS on `GEOID` for outlines and
figures.

{census_access(code)}

- Names repeat across states: filter on `USPS` (the state) or `GEOID`'s
  first two digits, and prefer the largest `ALAND` or a population from the
  ACS when several match.
- The internal point is inside the area, near its centre: good for a label
  or a geocode at area level, not a centroid.
- Credit: {CENSUS_ATTRIBUTION}.
"""


# ---------- GeoNames ----------

def country_assets(part: dict) -> dict:
    assets = {}
    for e in part["entries"]:
        path = f"{DATASET_PREFIX}/{GEONAMES_PATH}/country={e['country']}/geonames.parquet"
        assets[f"data-{e['country'].lower()}"] = {
            "href": f"{PUBLIC_BASE}/{path}", "type": PARQUET_TYPE,
            "title": f"{e['name'] or 'No country: oceans and undersea features'} ({e['country']})",
            "description": f"{e['features']:,} names",
            "roles": ["data"],
            "file:size": e["bytes"], "file:checksum": "1220" + e["sha256"],
            "alternate": s3(path),
        }
    return assets


def geonames_common(updated: str, manifest: dict, bbox: list[float]) -> dict:
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "license": "CC-BY-4.0",
        "attribution": GEONAMES_ATTRIBUTION,
        "version": manifest["dump_date"],
        "updated": updated,
        "providers": GEONAMES_PROVIDERS,
        "extent": {"spatial": {"bbox": [bbox]},
                   "temporal": {"interval": [[None, f"{manifest['dump_date']}T23:59:59Z"]]}},
        "table:primary_geometry": "geometry",
    }


def geonames_links(title: str) -> list[dict]:
    return [
        {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
        {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
        md_link("describedby", "./README.md", f"{title}: README"),
        md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
        GEONAMES_LICENSE, GEONAMES_VIA,
        {"rel": "alternate", "href": f"{part_url(GEONAMES_PATH)}/", "type": "text/html",
         "title": "Browse the files"},
    ]


def geonames_collection(entry: dict, part: dict, manifest: dict, columns: list[tuple[str, str]],
                        bbox: list[float], updated: str) -> dict:
    glob = f"s3://{BUCKET}/{DATASET_PREFIX}/{GEONAMES_PATH}/country=*/geonames.parquet"
    return {
        **geonames_common(updated, manifest, bbox),
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT, VERSION_EXT,
                            ALTERNATE_EXT],
        "id": "geonames",
        "title": "GeoNames, every name",
        "description": (
            f"The GeoNames database as of {manifest['dump_date']}: {entry['features']:,} named "
            f"places and features worldwide (populated places, administrative divisions, "
            f"water, terrain, buildings, roads, undersea features) with their coordinates, "
            f"feature class and code, country, divisions, population and elevation, decoded "
            f"with GeoNames' own lookup files. Published twice from the same rows: one world "
            f"file ({entry['bytes'] / 1e6:,.0f} MB, the data asset) and one file per country "
            f"under country=<ISO 3166-1 alpha-2>/ ({part['files']} files, `{NO_COUNTRY}` for "
            f"rows without a country; the data-<cc> assets and the partition glob {glob}). "
            f"WGS84 points with a bbox covering, Hilbert-sorted, read in place over HTTPS or "
            f"the anonymous S3 endpoint {S3_ENDPOINT}. Rebuilt weekly from GeoNames' daily "
            f"dump. Credit: {GEONAMES_ATTRIBUTION}."),
        "keywords": ["gazetteer", "GeoNames", "place names", "geocoding", "global",
                     "GeoParquet"],
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [{"name": "country", "type": "string",
                            "description": "The row's country_code (ISO 3166-1 alpha-2), or "
                                           f"`{NO_COUNTRY}` for rows without one."}],
        "partition:file_count": part["files"],
        "partition:glob": glob,
        "table:row_count": entry["features"],
        "table:columns": table_columns(columns, GEONAMES_COLUMNS, "geonames"),
        "assets": {
            "data": data_asset(f"{GEONAMES_PATH}/{entry['file']}", entry,
                               "GeoNames, the whole world in one file"),
            **country_assets(part),
            "thumbnail": thumbnail_asset("geonames", "Populated places, world"),
        },
        "links": geonames_links("GeoNames, every name"),
    }


def cities_style(tiles: dict) -> dict:
    layer = tiles["layer"]
    return {
        "version": 8,
        "name": "GeoNames cities1000, sized by population",
        "glyphs": "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf",
        "sources": {"data": {"type": "vector",
                             "url": f"pmtiles://{part_url(GEONAMES_PATH)}/{tiles['file']}",
                             "attribution": GEONAMES_ATTRIBUTION}},
        "layers": [
            {"id": f"{layer}-dots", "type": "circle", "source": "data", "source-layer": layer,
             "paint": {"circle-color": "#c0392b",
                       "circle-radius": ["interpolate", ["linear"],
                                         ["ln", ["max", ["get", "population"], 1]],
                                         7, 1.5, 16, 6],
                       "circle-stroke-color": "#ffffff", "circle-stroke-width": 0.5}},
            {"id": f"{layer}-labels", "type": "symbol", "source": "data", "source-layer": layer,
             "layout": {"text-field": ["get", "name"], "text-font": ["Open Sans Regular"],
                        "text-size": 11, "text-offset": [0, 0.8], "text-anchor": "top",
                        "symbol-sort-key": ["-", ["get", "population"]]},
             "paint": {"text-color": "#333333", "text-halo-color": "#ffffff",
                       "text-halo-width": 1}},
        ],
    }


def cities_collection(entry: dict, tiles: dict, manifest: dict, columns: list[tuple[str, str]],
                      bbox: list[float], updated: str) -> dict:
    tiles_path = f"{DATASET_PREFIX}/{GEONAMES_PATH}/{tiles['file']}"
    title = "GeoNames cities1000"
    return {
        **geonames_common(updated, manifest, bbox),
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT,
                            WEB_MAP_LINKS_EXT],
        "id": "cities1000",
        "title": "GeoNames populated places of 1,000 people or more",
        "description": (
            f"The GeoNames populated places (feature class P) with a population of 1,000 or "
            f"more, and the seats of government whatever their population "
            f"({', '.join(SEATS)}), as of {manifest['dump_date']}: {entry['features']:,} "
            f"places, the same columns as the full GeoNames file. A small file for city "
            f"lookups and labels, and a PMTiles archive for map labels, thinned per zoom "
            f"with the most populous place of each cell kept. Credit: {GEONAMES_ATTRIBUTION}."),
        "keywords": ["gazetteer", "GeoNames", "cities", "populated places", "labels",
                     "GeoParquet", "PMTiles"],
        "table:row_count": entry["features"],
        "table:columns": table_columns(columns, GEONAMES_COLUMNS, "cities1000"),
        "assets": {
            "data": data_asset(f"{GEONAMES_PATH}/{entry['file']}", entry, title),
            "pmtiles": {
                "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
                "title": f"{title}, vector tiles",
                "description": f"PMTiles, layer `{tiles['layer']}`, zoom {tiles['min_zoom']} to "
                               f"{tiles['max_zoom']}, {tiles['tiles']:,} tiles; properties "
                               f"{', '.join(tiles['properties'])}. Points thinned per zoom, "
                               f"the most populous kept. Built with tylertoo "
                               f"{tiles['tylertoo']}.",
                "roles": ["visual"],
                "file:size": tiles["bytes"], "file:checksum": "1220" + tiles["sha256"],
                "alternate": s3(tiles_path),
            },
            "style": {"href": "./styles/default.json", "type": STYLE_TYPE,
                      "title": "Places sized by population, with names",
                      "roles": ["style", "default"]},
            "thumbnail": thumbnail_asset("cities1000", "Populated places of 1,000 or more"),
        },
        "links": [*geonames_links(title),
                  {"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}",
                   "type": PMTILES_TYPE, "title": f"{title}, vector tiles",
                   "pmtiles:layers": [tiles["layer"]]}],
    }


def geonames_access(file: str, where: str) -> str:
    return f"""\
```sql
INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;
SELECT geonameid, name, admin1_name, country_name, population, latitude, longitude
FROM read_parquet('{part_url(GEONAMES_PATH)}/{file}')
WHERE {where}
ORDER BY population DESC LIMIT 10;
```"""


def geonames_readme(col: dict, entry: dict, extra: str) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, {entry['row_groups']} row groups |
| Rows | {entry['features']:,} |
| CRS | WGS84 lon/lat (OGC:CRS84) |
{extra}
## Access

{geonames_access(entry['file'], "name = 'Springfield' AND feature_class = 'P'")}

## Schema

| Column | Type | Description |
|---|---|---|
{schema}

## License

CC BY 4.0: credit "{GEONAMES_ATTRIBUTION}". GeoNames provides the data "as is",
without warranty of accuracy, timeliness or completeness. Source: {GEONAMES}/.
Built by [gazetteer.py]({REPO_URL}/blob/main/scripts/datasets/gazetteer.py).
"""


def geonames_agents(col: dict, entry: dict, partitioned: bool) -> str:
    part = (f"""
- One country only: `{part_url(GEONAMES_PATH)}/country=<CC>/geonames.parquet`
  (ISO 3166-1 alpha-2, `{NO_COUNTRY}` for oceans and undersea features): far
  smaller than the world file for a name search.
""" if partitioned else "")
    return f"""\
# {col['id']}: agent guide

GeoNames names with WGS84 points. Filter on the point with `bbox` first;
search names on `name`, `asciiname` or `alternatenames` (comma-separated).

{geonames_access(entry['file'], "name = 'Springfield' AND feature_class = 'P'")}
{part}
- Disambiguate a name with `country_code`, `admin1_name` and `population`
  (0 means unknown, not empty).
- `feature_class` P populated places, A divisions, H water, T terrain, S
  spots and buildings; `feature_code` refines it (PPLC capital).
- Credit: {GEONAMES_ATTRIBUTION}.
"""


# ---------- catalogs ----------

def sub_catalog(cid: str, title: str, description: str, collections: list[dict],
                license_link: dict, updated: str, version: str) -> dict:
    return {
        "type": "Catalog", "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": cid, "title": title, "description": description,
        "version": version, "updated": updated,
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json",
               "type": "application/json", "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{title}: README"),
            md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
            license_link,
        ],
    }


def build_root(subs: list[dict], updated: str) -> dict:
    return {
        "type": "Catalog", "stac_version": "1.1.0", "stac_extensions": [PORTOLAN_SCHEMA],
        "id": CATALOG_ID, "title": TITLE,
        "description": (
            f"Names to coordinates. The US Census Bureau's {YEAR} Gazetteer Files (every "
            f"state, county, place, tract, ZIP code area, district... with its internal point "
            f"and area, joining TIGER/Line on GEOID; public domain) and GeoNames (13 million "
            f"names worldwide with feature types, divisions and population; CC BY 4.0), as "
            f"GeoParquet. Each sub-catalog states its licence."),
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{s['id']}/catalog.json", "type": "application/json",
               "title": s["title"]} for s in subs),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            {"rel": "license", "href": "./README.md", "type": "text/markdown",
             "title": "Census Gazetteer: public domain; GeoNames: CC BY 4.0"},
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


def listing(collections: list[dict]) -> str:
    return "\n".join(f"- `{c['id']}`: {c['title']}, {c['table:row_count']:,} rows. "
                     f"{c['id']}/AGENTS.md" for c in collections)


def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    con = connect()
    total = 0

    # Census
    cm = read_manifest(out_dir, CENSUS_PATH)
    cdir = dest / CENSUS_ID
    census = []
    for f in cm["files"]:
        code, layer, title = f["name"], f["layer"], f["title"]
        source = data_source(out_dir, CENSUS_PATH, f["file"])
        col = census_collection(code, layer, title, f, cm["gate"][code], footer(con, source),
                                extent(con, source), updated)
        d = cdir / col["id"]
        write_json(d / "collection.json", col)
        shutil.copyfile(THUMBS / f"census-{code}.png", d / "thumbnail.png")
        (d / "README.md").write_text(census_readme(col, code, f))
        (d / "AGENTS.md").write_text(census_agents(col, code))
        census.append(col)
        total += f["features"]
    ccat = sub_catalog(CENSUS_ID, f"Census Gazetteer {YEAR}",
                       f"The US Census Bureau's {YEAR} Gazetteer Files: {len(census)} national "
                       f"files, one collection each, keyed by the TIGER/Line GEOID. Public "
                       f"domain. Credit: {CENSUS_ATTRIBUTION}.",
                       census, CENSUS_LICENSE, updated, YEAR)
    write_json(cdir / "catalog.json", ccat)
    (cdir / "README.md").write_text(f"# {ccat['title']}\n\n{ccat['description']}\n\n"
                                    f"{listing(census)}\n\nSource: {CENSUS_PAGE}\n")
    (cdir / "AGENTS.md").write_text(f"# {CENSUS_ID}: agent guide\n\n{ccat['description']}\n\n"
                                    f"{listing(census)}\n")

    # GeoNames
    gm = read_manifest(out_dir, GEONAMES_PATH)
    files = {f["name"]: f for f in gm["files"]}
    tiles = {t["name"]: t for t in gm["pmtiles"]}
    gdir = dest / "geonames"
    world = files["geonames"]
    wsrc = data_source(out_dir, GEONAMES_PATH, world["file"])
    wbox = extent(con, wsrc)
    gcol = geonames_collection(world, gm["partitions"], gm, footer(con, wsrc), wbox, updated)
    csrc = data_source(out_dir, GEONAMES_PATH, files["cities1000"]["file"])
    ccol = cities_collection(files["cities1000"], tiles["cities1000"], gm, footer(con, csrc),
                             extent(con, csrc), updated)
    for col, entry, partitioned, extra in (
            (gcol, world, True, f"| Country files | {gm['partitions']['files']}, under "
                                f"`country=<CC>/` |\n"),
            (ccol, files["cities1000"], False,
             f"| Map tiles | `{tiles['cities1000']['file']}`, "
             f"{tiles['cities1000']['bytes'] / 1e6:,.1f} MB, zoom "
             f"{tiles['cities1000']['min_zoom']} to {tiles['cities1000']['max_zoom']} |\n")):
        d = gdir / col["id"]
        write_json(d / "collection.json", col)
        shutil.copyfile(THUMBS / f"{col['id']}.png", d / "thumbnail.png")
        (d / "README.md").write_text(geonames_readme(col, entry, extra))
        (d / "AGENTS.md").write_text(geonames_agents(col, entry, partitioned))
    style = gdir / "cities1000" / "styles" / "default.json"
    write_json(style, cities_style(tiles["cities1000"]))
    a = ccol["assets"]["style"]
    a["file:size"], a["file:checksum"] = style.stat().st_size, multihash(style)
    write_json(gdir / "cities1000" / "collection.json", ccol)
    total += world["features"]
    gcat = sub_catalog("geonames", "GeoNames",
                       f"The GeoNames database as of {gm['dump_date']}, rebuilt weekly from "
                       f"GeoNames' daily dump: every name, and the populated places of 1,000 "
                       f"or more. CC BY 4.0. Credit: {GEONAMES_ATTRIBUTION}.",
                       [gcol, ccol], GEONAMES_LICENSE, updated, gm["dump_date"])
    write_json(gdir / "catalog.json", gcat)
    (gdir / "README.md").write_text(f"# {gcat['title']}\n\n{gcat['description']}\n\n"
                                    f"{listing([gcol, ccol])}\n\nSource: {GEONAMES}/\n")
    (gdir / "AGENTS.md").write_text(f"# geonames: agent guide\n\n{gcat['description']}\n\n"
                                    f"{listing([gcol, ccol])}\n")

    root = build_root([ccat, gcat], updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(f"""\
# {TITLE}

{root['description']}

| Catalog | Licence | Collections |
|---|---|---|
| [{ccat['title']}](./{CENSUS_ID}/catalog.json) | Public domain (US Government work); credit "{CENSUS_ATTRIBUTION}" | {len(census)} |
| [GeoNames](./geonames/catalog.json) | CC BY 4.0; credit "{GEONAMES_ATTRIBUTION}" | 2 |

Maintainer: Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
""")
    (dest / "AGENTS.md").write_text(f"""\
# {CATALOG_ID}: agent guide

Names to points. Use the Census Gazetteer for US areas (join TIGER/Line and
the ACS on GEOID), GeoNames for anywhere else and for features that are not
statistical areas (mountains, lakes, buildings).

- `{CENSUS_ID}`: {CENSUS_ID}/AGENTS.md
- `geonames`: geonames/AGENTS.md

Filter on `bbox` before any spatial test. Credit each source as its
catalog says: "{CENSUS_ATTRIBUTION}"; "{GEONAMES_ATTRIBUTION}".
""")
    filedates.apply(dest, filedates.load(PUBLIC_DATA, *STAMPS))
    for doc in ("README.md", "AGENTS.md"):
        path = dest / doc
        path.write_text(f"{path.read_text().rstrip()}\n\n{filedates.DATES_MD}\n")
    return total


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="gazetteer-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        total = build(out_dir, tree)
        print(f"  rendered the gazetteer catalog, {total:,} rows")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

CONUS = (-125.0, 24.0, -66.5, 49.5)
WORLD = (-180.0, -58.0, 180.0, 78.0)


def thumbnails(out_dir: Path) -> None:
    """Each file's points, flat red on the page background, framed 3:2."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from thumbnails import BACKGROUND, frame

    con = connect()
    THUMBS.mkdir(parents=True, exist_ok=True)
    jobs = [(f"census-{code}", out_dir / CENSUS_PATH / f"{code}.parquet", CONUS, "")
            for _, (code, _, _) in CENSUS_LAYERS.items()]
    jobs += [("geonames", out_dir / GEONAMES_PATH / "geonames.parquet", WORLD,
              "WHERE feature_class = 'P' USING SAMPLE 400000 ROWS"),
             ("cities1000", out_dir / GEONAMES_PATH / "cities1000.parquet", WORLD, "")]
    for name, path, win, where in jobs:
        rows = con.execute(f"SELECT ST_X(geometry::GEOMETRY), ST_Y(geometry::GEOMETRY) "
                           f"FROM read_parquet('{path}') {where}").fetchall()
        xs = [r[0] for r in rows]
        ys = [r[1] for r in rows]
        x0, y0, x1, y1, aspect = frame(*win)
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.set_facecolor(BACKGROUND)
        fig.patch.set_facecolor(BACKGROUND)
        size = 4 if len(rows) < 2_000 else 1 if len(rows) < 50_000 else 0.15
        ax.scatter(xs, ys, s=size, c="#c0392b", linewidths=0, alpha=0.8)
        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
        ax.set_aspect(aspect)
        ax.axis("off")
        fig.savefig(THUMBS / f"{name}.png", dpi=100, facecolor=BACKGROUND)
        plt.close(fig)
        print(f"  {name}: {len(rows):,} points -> {(THUMBS / f'{name}.png').relative_to(REPO)}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp, required=False):
        sp.add_argument("--out-dir", type=Path, default=None, required=required,
                        help="gazetteer.py's --out-dir (default: read the published files)")

    b = sub.add_parser("build")
    out_dir_arg(b)
    b.add_argument("--dest", type=Path, required=True)
    c = sub.add_parser("check")
    c.add_argument("tree", type=Path)
    u = sub.add_parser("publish")
    out_dir_arg(u)
    u.add_argument("--remote", required=True)
    u.add_argument("--dry-run", action="store_true")
    t = sub.add_parser("thumbnails")
    out_dir_arg(t, required=True)
    args = p.parse_args()
    if args.cmd == "build":
        print(f"wrote {args.dest}/ ({build(args.out_dir, args.dest):,} rows)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    elif args.cmd == "publish":
        publish(args.out_dir, args.remote, dry_run=args.dry_run)
    else:
        thumbnails(args.out_dir)


if __name__ == "__main__":
    main()
