#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for NCES school locations.

  schools/catalog/catalog.json                 root: NCES EDGE school locations
  schools/catalog/<name>/collection.json       public, private, postsecondary

schools.py's FILES drives it. Each collection has the file's GeoParquet
(`data`), a thumbnail and its PMTiles registered as formats.md asks (a
`rel: pmtiles` link with `pmtiles:layers`, web-map-links v1.3.0, the file as
a `visual` asset) with a MapLibre style on NCES's locale (city, suburb,
town, rural). Column descriptions follow NCES's record layouts. Counts,
sizes, checksums, school years and the gate come from latest/_manifest.json,
local (--out-dir) or published. Spec: portolan-spec v0.2.0. Validator:
rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/schools_catalog.py build --dest build/schools-catalog [--out-dir out/schools]
  python3 scripts/datasets/schools_catalog.py check build/schools-catalog
  python3 scripts/datasets/schools_catalog.py publish --remote parquetry:parquetry/schools [--out-dir out/schools]
  uv run --project scripts --with matplotlib \\
      python scripts/datasets/schools_catalog.py thumbnails --out-dir out/schools
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
from datasets.pmtiles import PMTILES_TYPE
from datasets.schools import FILES, RELEASE, SOURCE_PAGE

import catalog as shared  # scripts/catalog.py
import filedates  # noqa: E402  scripts/filedates.py
from extent import lon_intervals, wrapped_bbox
from catalog import (
    ALTERNATE_EXT,
    BUCKET,
    CONTACT_EMAIL,
    FILE_EXT,
    LOGO,
    PARQUET_TYPE,
    PORTOLAN_SCHEMA,
    PUBLIC_BASE,
    REPO,
    REPO_URL,
    S3_ENDPOINT,
    SITE_URL,
    TABLE_EXT,
    VERSION_EXT,
    md_link,
    multihash,
    now_rfc3339,
    write_json,
)

DATASET_PREFIX = "schools"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
# The workflow stamps what it uploads (scripts/filedates.py): `updated` is
# when the data last changed, `checked` the last build.
STAMPS = [f"{PUBLIC_DATA}/_files/schools.json"]
RELEASE_URL = f"{PUBLIC_DATA}/{RELEASE}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "nces-schools"
TITLE = "NCES school locations"
THUMBS = REPO / "catalog" / "thumbnails" / "schools"
ATTRIBUTION = ("U.S. Department of Education, National Center for Education Statistics, "
               "EDGE school locations")

LICENSE_LINK = {
    "rel": "license",
    "href": "https://www.usa.gov/government-copyright",
    "type": "text/html",
    "title": "Works of the US federal government are not subject to copyright in the "
             "United States (17 U.S.C. 105)",
}
VIA_LINK = {
    "rel": "via",
    "href": SOURCE_PAGE,
    "type": "text/html",
    "title": "NCES EDGE school locations, where they are published",
}
PROVIDERS = [
    {
        "name": "National Center for Education Statistics (NCES)",
        "description": "Collects the school surveys (CCD, PSS, IPEDS) and geocodes the schools "
                       "through its EDGE program. A US government work, in the public domain.",
        "url": "https://nces.ed.gov/programs/edge/",
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted the EDGE files to GeoParquet, tiled them, and maintains and "
                       "hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]
SCOPE = ("A location is NCES's estimate of the school's physical address, usually on the "
         "building or its parcel; a school without one (a virtual school) carries its "
         "administration's address.")

SINGULAR = {"public": "public elementary or secondary school", "private": "private school",
            "postsecondary": "postsecondary institution"}
KEY_DOCS = {
    "NCESSCH": "NCES school ID, 12 characters: state FIPS (2), LEA code (5), school code (5). "
               "Joins the Common Core of Data (enrollment, grades, school type).",
    "LEAID": "NCES local education agency (school district) ID, 7 characters: state FIPS and "
             "LEA code. Equals TIGER/Line's school district GEOID where the agency has a "
             "district polygon.",
    "PPIN": "Private School Survey school ID, 8 characters. Joins the PSS data files.",
    "UNITID": "IPEDS institution ID (unit ID). Joins the IPEDS data (enrollment, programs, "
              "finance).",
}
COLUMN_DOCS = {
    **KEY_DOCS,
    "NAME": "Name of the school, as reported to the survey.",
    "OPSTFIPS": "FIPS code of the state that administers the school, not always the state it "
                "is in: 59 for Bureau of Indian Education schools, 63 for Department of "
                "Defense schools.",
    "STREET": "Reported location street address; M where missing.",
    "CITY": "Reported location city.",
    "STATE": "Reported location state (USPS abbreviation).",
    "ZIP": "Reported location ZIP code.",
    "STFIP": "FIPS code of the state the point is in, assigned from the point.",
    "CNTY": "County FIPS, 5 characters (state and county), assigned from the point. Joins "
            "TIGER/Line's county GEOID.",
    "NMCNTY": "County name, with its legal description (county, parish, borough...).",
    "LOCALE": "NCES locale code, from the point: 11 to 13 city (large, midsize, small), 21 to "
              "23 suburb, 31 to 33 town (fringe, distant, remote), 41 to 43 rural (fringe, "
              "distant, remote); N outside the United States.",
    "LAT": "Latitude of the estimated location, as NCES ships it (equal to the point).",
    "LON": "Longitude of the estimated location, as NCES ships it (equal to the point).",
    "CBSA": "Core based statistical area code, from the point. Joins TIGER/Line's CBSA "
            "GEOID; N outside every CBSA.",
    "NMCBSA": "Core based statistical area name.",
    "CBSATYPE": "1 metropolitan, 2 micropolitan, 0 outside every CBSA.",
    "CSA": "Combined statistical area code, from the point; N outside every CSA.",
    "NMCSA": "Combined statistical area name.",
    "CD": "Congressional district, state FIPS and district number (00 at large, 98 for a "
          "non-voting delegate), from the point.",
    "SLDL": "State legislative district, lower chamber: state FIPS and district code, from "
            "the point; N where there is none (Nebraska, DC).",
    "SLDU": "State legislative district, upper chamber: state FIPS and district code, from "
            "the point.",
    "SCHOOLYEAR": "School year of the survey.",
    "geometry": "Estimated school location as native Parquet GEOMETRY (WKB) point, lon/lat "
                "(OGC:CRS84).",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the GeoParquet "
            "covering. Filter on it to prune row groups.",
}

# NCES's four locale types, by the code's first digit.
LOCALE_TYPES = {"1": ("City", "#c2185b"), "2": ("Suburb", "#f57c00"), "3": ("Town", "#388e3c"),
                "4": ("Rural", "#1976d2")}
NO_LOCALE = "#8a8a8a"


# ---------- measured inputs ----------

def read_manifest(out_dir: Path | None) -> dict:
    local = out_dir / RELEASE / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    with urllib.request.urlopen(f"{RELEASE_URL}/_manifest.json") as r:
        return json.load(r)


def data_source(out_dir: Path | None, file: str) -> str:
    local = out_dir / RELEASE / file if out_dir else None
    return str(local) if local and local.is_file() else f"{RELEASE_URL}/{file}"


def connect():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    return con


def layer_extent(source: str) -> list[float]:
    """Measured on the data (scripts/extent.py): the island areas put the
    public schools across the antimeridian, written with west > east."""
    con = connect()
    src = f"read_parquet('{source}')"
    y0, y1 = con.execute(f"SELECT min(bbox.ymin), max(bbox.ymax) FROM {src}").fetchone()
    b = wrapped_bbox(lon_intervals(con, src), y0, y1)
    return [round(b[0], 6), max(-90.0, round(b[1], 6)), round(b[2], 6), min(90.0, round(b[3], 6))]


def footer(source: str) -> list[tuple[str, str]]:
    return [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
            connect().execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()]


def school_year_interval(year: str) -> list[str]:
    """'2024-25' -> July 2024 to June 2025, the school year as NCES counts it."""
    start = int(year[:4])
    return [f"{start}-07-01T00:00:00Z", f"{start + 1}-06-30T23:59:59Z"]


# ---------- styles ----------

def locale_colour() -> list:
    expr = ["match", ["slice", ["coalesce", ["get", "LOCALE"], ""], 0, 1]]
    for digit, (_, colour) in LOCALE_TYPES.items():
        expr += [digit, colour]
    return [*expr, NO_LOCALE]


def style(name: str, tiles: dict) -> dict:
    return {
        "version": 8, "name": f"{name}: NCES locale (city, suburb, town, rural)",
        "sources": {"data": {"type": "vector", "url": f"pmtiles://{RELEASE_URL}/{tiles['file']}",
                             "attribution": ATTRIBUTION}},
        "layers": [
            {"id": f"{name}-points", "type": "circle", "source": "data", "source-layer": name,
             "paint": {"circle-color": locale_colour(),
                       "circle-radius": ["interpolate", ["linear"], ["zoom"], 3, 1.5, 8, 3,
                                         13, 5],
                       "circle-stroke-color": "#ffffff",
                       "circle-stroke-width": ["interpolate", ["linear"], ["zoom"], 6, 0,
                                               10, 0.8],
                       "circle-opacity": 0.85}},
        ],
    }


# ---------- STAC ----------

def asset_s3(path: str) -> dict:
    return {"s3": {"href": f"s3://{BUCKET}/{path}", "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}


def gate_text(name: str, gate: dict) -> str:
    s = gate["states"]
    text = (f"Checked against the source layer: {gate['source_rows']:,} rows, every "
            f"{gate['key']} unique, every point in range and equal to NCES's LAT and LON. "
            f"Against TIGER/Line 2026 states: {s['stfip_matches_tiger_state']:,} points in the "
            f"state their STFIP names, {s['in_another_state']} in another, "
            f"{s['outside_every_state']} outside every state")
    d = gate.get("districts")
    if d:
        text += (f". LEAID joins a TIGER/Line 2026 school district GEOID for "
                 f"{d['schools_joined']:,} schools ({d['schools_share']:.1%}; "
                 f"{d['leas_joined']:,} of {d['leas']:,} agencies): charter, state-run and "
                 f"regional agencies have no district polygon")
    return text + "."


def build_collection(name: str, entry: dict, tiles: dict, extent: list[float],
                     columns: list[tuple[str, str]], updated: str) -> dict:
    spec = FILES[name]
    path = f"{DATASET_PREFIX}/{RELEASE}/{entry['file']}"
    undocumented = [c for c, _ in columns if c not in COLUMN_DOCS]
    if undocumented:
        sys.exit(f"{name}: no column doc for {undocumented}")
    tiles_path = f"{DATASET_PREFIX}/{RELEASE}/{tiles['file']}"
    year = entry["school_year"]
    thumb = THUMBS / f"{name}.png"
    assets = {
        "data": {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": f"{spec['title']}, {year}",
            "description": f"{entry['features']:,} rows in {entry['row_groups']} row groups.",
            "roles": ["data"],
            "file:size": entry["bytes"],
            "file:checksum": "1220" + entry["sha256"],
            "alternate": asset_s3(path),
        },
        "pmtiles": {
            "href": f"{PUBLIC_BASE}/{tiles_path}",
            "type": PMTILES_TYPE,
            "title": f"{spec['title']}, vector tiles",
            "description": f"PMTiles, layer `{tiles['layer']}`, zoom {tiles['min_zoom']} to "
                           f"{tiles['max_zoom']}, {tiles['tiles']:,} tiles; points thinned below "
                           f"the maximum zoom, every school at it; properties "
                           f"{', '.join(tiles['properties'])}. Built with tylertoo "
                           f"{tiles['tylertoo']}.",
            "roles": ["visual"],
            "file:size": tiles["bytes"],
            "file:checksum": "1220" + tiles["sha256"],
            "alternate": asset_s3(tiles_path),
        },
        "style": {"href": "./styles/default.json", "type": STYLE_TYPE,
                  "title": "NCES locale: city, suburb, town, rural",
                  "roles": ["style", "default"]},
        "thumbnail": {
            "href": "./thumbnail.png",
            "type": "image/png",
            "title": f"{spec['title']}, by NCES locale",
            "roles": ["thumbnail"],
            "file:size": thumb.stat().st_size,
            "file:checksum": multihash(thumb),
        },
    }
    links = [
        {"rel": "root", "href": "../catalog.json", "type": "application/json"},
        {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
        md_link("describedby", "./README.md", f"{spec['title']}: README"),
        md_link("agents", "./AGENTS.md", f"{spec['title']}: agent guide"),
        LICENSE_LINK,
        VIA_LINK,
        {"rel": "describedby", "href": spec["doc"], "type": "application/pdf",
         "title": f"EDGE geocode technical documentation, {spec['title'].lower()}"},
        {"rel": "related", "href": f"{RELEASE_URL}/ATTRIBUTION.txt", "type": "text/plain",
         "title": "Attribution and what was changed"},
        {"rel": "alternate", "href": f"{RELEASE_URL}/", "type": "text/html",
         "title": "Browse the files"},
        {"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
         "title": f"{spec['title']}, vector tiles", "pmtiles:layers": [tiles["layer"]]},
    ]
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT,
                            WEB_MAP_LINKS_EXT],
        "id": name,
        "title": f"{spec['title']}, {year} (NCES)",
        "description": (
            f"NCES's EDGE locations of {spec['title'].lower()} for the {year} school year, "
            f"from the {spec['survey']}: {entry['features']:,} points keyed by {spec['key']}, "
            "with NCES's county, CBSA, congressional and legislative districts and locale "
            "assigned from each point. Columns as NCES's record layout names them, lon/lat "
            "points with a bbox covering, Hilbert-sorted, as one GeoParquet 2.0 file read in "
            f"place over HTTPS or the anonymous S3 endpoint {S3_ENDPOINT}, and a PMTiles "
            f"archive for web maps. {gate_text(name, entry['gate'])} {SCOPE} "
            f"Credit: {ATTRIBUTION}."),
        "keywords": ["schools", "education", "NCES", "EDGE", spec["survey"].split(" (")[0],
                     name, "critical facilities", "GeoParquet"],
        "license": "other",
        "attribution": ATTRIBUTION,
        "version": year,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {
            "spatial": {"bbox": [extent]},
            "temporal": {"interval": [school_year_interval(year)]},
        },
        "table:row_count": entry["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": COLUMN_DOCS[n]}
                          for n, t in columns],
        "assets": assets,
        "links": links,
    }


def build_root(collections: list[dict], updated: str) -> dict:
    years = ", ".join(f"{c['id']} {c['version']}" for c in collections)
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (
            "The National Center for Education Statistics' EDGE school locations: every US "
            "public school (Common Core of Data), private school (Private School Survey) and "
            "postsecondary institution (IPEDS) as a point, with NCES's ids, address, county, "
            "CBSA, congressional and legislative districts and locale. GeoParquet and "
            f"PMTiles, one collection per survey, each its latest school year ({years}). "
            "The ids join NCES's survey data; county and district codes join the US Census "
            f"TIGER/Line layers on parquetry. Public domain in the United States. {SCOPE} "
            f"Credit: {ATTRIBUTION}."),
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "related", "href": f"{RELEASE_URL}/ATTRIBUTION.txt", "type": "text/plain",
             "title": "Attribution and what was changed"},
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


# ---------- markdown ----------

LICENSE_MD = f"""\
The EDGE school locations are a work of the US federal government, in the
public domain in the United States (17 U.S.C. 105). Credit "{ATTRIBUTION}"."""

# Downtown Houston, where a 2 km radius takes in public, private and
# postsecondary schools.
EXAMPLE = (-95.3698, 29.7604)


def access_md(name: str) -> str:
    x, y = EXAMPLE
    url = f"{RELEASE_URL}/{name}.parquet"
    key = FILES[name]["key"]
    d = 0.02  # about 2 km
    return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
SELECT {key}, NAME, CITY, LOCALE,
       ST_Distance_Sphere(geometry, ST_Point({x}, {y})) AS metres
FROM read_parquet('{url}')
WHERE bbox.xmin <= {x + d} AND bbox.xmax >= {x - d}
  AND bbox.ymin <= {y + d} AND bbox.ymax >= {y - d}
  AND ST_Distance_Sphere(geometry, ST_Point({x}, {y})) < 2000
ORDER BY metres;
```

The same file at `s3://{BUCKET}/{DATASET_PREFIX}/{RELEASE}/{name}.parquet` on the
anonymous S3 endpoint `{S3_ENDPOINT}`."""


def joins(name: str) -> str:
    rows = ["`CNTY` = TIGER/Line county `GEOID` (census/tiger/2026/substategeo/county.parquet)",
            "`CBSA` = TIGER/Line CBSA `GEOID` (census/tiger/2026/nationgeo/cbsa.parquet)"]
    if name == "public":
        rows.insert(0, "`LEAID` = TIGER/Line school district `GEOID` (census/tiger/2026/school/"
                       "unsd, elsd or scsd.parquet), where the agency has a district polygon")
    rows.append({"public": "`NCESSCH` joins NCES's Common Core of Data (enrollment, grades, "
                           "school type)",
                 "private": "`PPIN` joins the Private School Survey data files",
                 "postsecondary": "`UNITID` joins IPEDS (enrollment, programs, finance)"}[name])
    return "\n".join(f"- {r}" for r in rows)


def collection_readme(col: dict, name: str, entry: dict, tiles: dict) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, {entry['row_groups']} row groups, sha256 `{entry['sha256'][:12]}...` |
| Rows | {entry['features']:,} |
| School year | {entry['school_year']} |
| Map tiles | `{tiles['file']}`, {tiles['bytes'] / 1e6:,.1f} MB, zoom {tiles['min_zoom']} to {tiles['max_zoom']} |
| CRS | lon/lat, OGC:CRS84 (NCES ships WGS 84) |

## Access

{access_md(name)}

## Joins

{joins(name)}

## Map tiles

`{RELEASE_URL}/{tiles['file']}`, tile layer `{tiles['layer']}`, properties
{', '.join(f"`{p}`" for p in tiles['properties'])}. Below zoom {tiles['max_zoom']} the points
are thinned; every school is in zoom {tiles['max_zoom']}. `styles/default.json` colours the
points on NCES's locale: city, suburb, town, rural.

## Columns

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

`{entry['source'].rsplit('/', 1)[1]}` from {entry['source'].rsplit('/', 1)[0]}/ (page
{SOURCE_PAGE}). [schools.py]({REPO_URL}/blob/main/scripts/datasets/schools.py)
reads the zip's shapefile with DuckDB, keeps NCES's columns and values, writes
the point as native GEOMETRY in lon/lat with the `bbox` covering, sorted along
a Hilbert curve; rows, keys and coordinates are checked against the shapefile,
and the footer, bbox and groups are verified before upload.

## License

{LICENSE_MD}
"""


def collection_agents(col: dict, name: str) -> str:
    spec = FILES[name]
    return f"""\
# {col['title']}: agent guide

One row per {SINGULAR[name]}, key `{spec['key']}`, lon/lat (OGC:CRS84), point
geometry column `geometry`, school year {col['version']}.

## Access

{access_md(name)}

## Query tips

- Filter on `bbox` before a distance or containment test; the file is
  Hilbert-sorted, so a small area reads one or two row groups.
- Schools in an area (a flood zone, a fire perimeter): `ST_Contains(area, geometry)`
  after a bbox prefilter on both sides.
{joins(name)}
- `LOCALE`: first digit 1 city, 2 suburb, 3 town, 4 rural; N outside the US.
- Missing values are `M`, not applicable `N`, as NCES codes them.
- Credit: {ATTRIBUTION}.
"""


def root_readme(root: dict, collections: list[dict]) -> str:
    rows = "\n".join(f"| [{c['title']}](./{c['id']}/README.md) | `{c['id']}.parquet` | "
                     f"{c['table:row_count']:,} |" for c in collections)
    return f"""\
# {root['title']}

{root['description']}

| Collection | File | Rows |
|---|---|---|
{rows}

Files: `{RELEASE_URL}/<name>.parquet` and `<name>.pmtiles`. A newer school
year replaces a file in place; `_manifest.json` names each file's year.

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}, {c['table:row_count']:,} points. "
                        f"{c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {CATALOG_ID}: agent guide

NCES's EDGE school locations, one collection per survey.

{listing}

Layout: `{RELEASE_URL}/<name>.parquet`, anonymous S3 at
`s3://{BUCKET}/{DATASET_PREFIX}/{RELEASE}/` on `{S3_ENDPOINT}`. Lon/lat points.
`CNTY` and `CBSA` join census/TIGER; public `LEAID` joins TIGER school
districts. Credit: {ATTRIBUTION}.
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    manifest = read_manifest(out_dir)
    files = {f["name"]: f for f in manifest["files"]}
    tiles = {t["name"]: t for t in manifest["pmtiles"]}
    collections, total = [], 0
    for name in FILES:
        if not (THUMBS / f"{name}.png").is_file():
            sys.exit(f"missing {THUMBS / name}.png: run the thumbnails command")
        source = data_source(out_dir, files[name]["file"])
        col = build_collection(name, files[name], tiles[name], layer_extent(source),
                               footer(source), updated)
        cdir = dest / name
        write_json(cdir / "styles" / "default.json", style(name, tiles[name]))
        asset = col["assets"]["style"]
        asset["file:size"] = (cdir / asset["href"]).stat().st_size
        asset["file:checksum"] = multihash(cdir / asset["href"])
        write_json(cdir / "collection.json", col)
        shutil.copyfile(THUMBS / f"{name}.png", cdir / "thumbnail.png")
        (cdir / "README.md").write_text(collection_readme(col, name, files[name], tiles[name]))
        (cdir / "AGENTS.md").write_text(collection_agents(col, name))
        collections.append(col)
        total += files[name]["features"]
    root = build_root(collections, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, collections))
    (dest / "AGENTS.md").write_text(root_agents(collections))
    filedates.apply(dest, filedates.load(PUBLIC_DATA, *STAMPS))
    for doc in ("README.md", "AGENTS.md"):
        path = dest / doc
        path.write_text(f"{path.read_text().rstrip()}\n\n{filedates.DATES_MD}\n")
    return total


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="schools-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        total = build(out_dir, tree)
        print(f"  rendered {len(FILES)} collections, {total:,} rows")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

CONUS = (-125.0, 24.0, -66.5, 49.5)


def thumbnails(out_dir: Path) -> None:
    """Each file's points over the conterminous US, coloured on the locale as
    the default style."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from thumbnails import BACKGROUND, frame

    con = connect()
    THUMBS.mkdir(parents=True, exist_ok=True)
    x0, y0, x1, y1, aspect = frame(*CONUS)
    for name in FILES:
        path = out_dir / RELEASE / f"{name}.parquet"
        rows = con.execute(f"""
            SELECT ST_X(geometry), ST_Y(geometry), left(LOCALE, 1)
            FROM read_parquet('{path}')
            WHERE bbox.xmax > {CONUS[0]} AND bbox.xmin < {CONUS[2]}
              AND bbox.ymax > {CONUS[1]} AND bbox.ymin < {CONUS[3]}""").fetchall()
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.set_facecolor(BACKGROUND)
        fig.patch.set_facecolor(BACKGROUND)
        size = 0.4 if len(rows) > 50_000 else 1.0 if len(rows) > 10_000 else 2.5
        ax.scatter([r[0] for r in rows], [r[1] for r in rows], s=size, linewidths=0,
                   c=[LOCALE_TYPES.get(r[2], (None, NO_LOCALE))[1] for r in rows], alpha=0.8)
        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
        ax.set_aspect(aspect)
        ax.axis("off")
        fig.savefig(THUMBS / f"{name}.png", dpi=100, facecolor=BACKGROUND)
        plt.close(fig)
        print(f"  {name}: {len(rows):,} points -> {(THUMBS / f'{name}.png').relative_to(REPO)}")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp, required=False):
        sp.add_argument("--out-dir", type=Path, default=None, required=required,
                        help="schools.py's --out-dir (default: read the published files)")

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
    elif args.cmd == "thumbnails":
        thumbnails(args.out_dir)


if __name__ == "__main__":
    main()
