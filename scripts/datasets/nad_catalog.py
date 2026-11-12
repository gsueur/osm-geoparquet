#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the US National Address
Database (NAD).

  addresses/catalog/catalog.json            root: US National Address Database
  addresses/catalog/nad/collection.json     the address points, one file per state
  addresses/catalog/coverage/collection.json  address counts on H3 cells, and the map

The `nad` collection is partitioned like the OSM themes: a `partition:glob`
over the state files through the anonymous S3 endpoint, and one `data-<xx>`
asset per state for clients that read assets. Release paths carry the
release number (r24), so each asset carries its size and checksum. Counts,
sizes, checksums, coverage and the gate come from the release's
_manifest.json and _coverage.json, local (--out-dir) or published. Spec:
portolan-spec v0.2.0. Validator: rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/nad_catalog.py build --dest build/nad-catalog [--out-dir out/addresses]
  python3 scripts/datasets/nad_catalog.py check build/nad-catalog
  python3 scripts/datasets/nad_catalog.py publish --remote parquetry:parquetry/addresses [--out-dir out/addresses]
  uv run --project scripts --with matplotlib --with shapely \\
      python scripts/datasets/nad_catalog.py thumbnails --out-dir out/addresses
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
from datasets.gaul_catalog import STYLE_TYPE, WEB_MAP_LINKS_EXT  # noqa: E402
from datasets.nad import H3_BANDS, PAGE, SOCRATA, TXT_ID  # noqa: E402
from datasets.pmtiles import PMTILES_TYPE  # noqa: E402

import catalog as shared  # noqa: E402  scripts/catalog.py
import filedates  # noqa: E402  scripts/filedates.py
from catalog import (  # noqa: E402
    ALTERNATE_EXT,
    BUCKET,
    CONTACT_EMAIL,
    FILE_EXT,
    LOGO,
    PARQUET_TYPE,
    PARTITION_EXT,
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

DATASET_PREFIX = "addresses"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
# The workflow stamps what it uploads (scripts/filedates.py): `updated` is
# when the data last changed, `checked` the last build.
STAMPS = [f"{PUBLIC_DATA}/_files/addresses.json"]
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "us-national-address-database"
TITLE = "US National Address Database"
THUMBS = REPO / "catalog" / "thumbnails" / "addresses"
SOURCE_URL = f"{SOCRATA}/d/{TXT_ID}"

LICENSE_LINK = {
    "rel": "license",
    "href": "https://www.usa.gov/government-copyright",
    "type": "text/html",
    "title": "Works of the US federal government are not subject to copyright in the "
             "United States (17 U.S.C. 105)",
}
VIA_LINK = {
    "rel": "via",
    "href": SOURCE_URL,
    "type": "text/html",
    "title": "NAD Text File on data.transportation.gov, where it is published",
}
ABOUT_LINK = {"rel": "describedby", "href": PAGE, "type": "text/html",
              "title": "About the National Address Database (USDOT)"}
PROVIDERS = [
    {
        "name": "State, county, municipal and tribal addressing authorities",
        "description": "Create and maintain the address points and share them with USDOT. "
                       "Each record's AddAuth, NAD_Source and DataSet_ID name its provider.",
        "roles": ["producer"],
    },
    {
        "name": "US Department of Transportation (USDOT)",
        "description": "Aggregates the providers' points into the National Address Database "
                       "and publishes it on data.transportation.gov. Public domain.",
        "url": PAGE,
        "roles": ["processor", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted the text file to GeoParquet per state, built the coverage "
                       "cells and map, and maintains and hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]
SCOPE = ("Participation is voluntary: some states have no rows and a contributing state's "
         "coverage can be partial (the coverage collection maps it). USDOT makes no warranty "
         "as to accuracy or completeness.")

# NAD's columns, from its schema (USDOT, NAD Schema 2023), shortened.
COLUMN_DOCS = {
    "OID_": "Record identifier in this NAD release; unique within the release, not stable "
            "across releases.",
    "AddNum_Pre": "Address number prefix.",
    "Add_Number": "Address number, the integer part.",
    "AddNum_Suf": "Address number suffix (e.g. 'B', '1/2').",
    "AddNo_Full": "Complete address number: prefix, number and suffix.",
    "St_PreMod": "Street name pre-modifier.",
    "St_PreDir": "Street name pre-directional, spelled out (e.g. 'North').",
    "St_PreTyp": "Street name pre-type (e.g. 'Avenue' in 'Avenue A').",
    "St_PreSep": "Separator between the pre-type and the name (e.g. 'of the').",
    "St_Name": "Street name.",
    "St_PosTyp": "Street name post-type, spelled out (e.g. 'Road').",
    "St_PosDir": "Street name post-directional, spelled out.",
    "St_PosMod": "Street name post-modifier.",
    "StNam_Full": "Complete street name.",
    "Building": "Building name or number.",
    "Floor": "Floor.",
    "Unit": "Unit (apartment, suite).",
    "Room": "Room.",
    "Seat": "Seat.",
    "Addtl_Loc": "Additional location information.",
    "SubAddress": "Complete subaddress (building, floor, unit, room, seat together).",
    "LandmkName": "Landmark name.",
    "County": "County name.",
    "Inc_Muni": "Incorporated municipality.",
    "Post_City": "Postal community name (the city of the mailing address).",
    "Census_Plc": "Census place.",
    "Uninc_Comm": "Unincorporated community.",
    "Nbrhd_Comm": "Neighborhood community.",
    "NatAmArea": "Native American area.",
    "NatAmSub": "Native American subdivision.",
    "Urbnztn_PR": "Urbanization (Puerto Rico).",
    "PlaceOther": "Other place name.",
    "PlaceNmTyp": "Type of PlaceOther.",
    "State": "State, DC or territory, USPS postal code; the partition key.",
    "Zip_Code": "ZIP code, five digits, as text.",
    "Plus_4": "ZIP+4 add-on code.",
    "UUID": "The provider's unique identifier for the address point.",
    "AddAuth": "Address authority: the agency that assigns the address.",
    "AddrRefSys": "Address reference system.",
    "Longitude": "Longitude, decimal degrees, WGS 84.",
    "Latitude": "Latitude, decimal degrees, WGS 84.",
    "NatGrid": "US National Grid coordinate.",
    "Elevation": "Elevation.",
    "Placement": "How the point was placed (e.g. 'Structure - Rooftop', 'Site', 'Parcel', "
                 "'Unknown').",
    "AddrPoint": "The point as 'longitude latitude' text, as NAD ships it.",
    "Related_ID": "Identifier of a related address.",
    "RelateType": "Type of the relation to Related_ID.",
    "ParcelSrc": "Source of the parcel identifier.",
    "Parcel_ID": "Parcel identifier.",
    "AddrClass": "Address class (e.g. 'Numbered Thoroughfare Address').",
    "Lifecycle": "Lifecycle status (e.g. current, proposed, retired).",
    "Effective": "Date the address became effective.",
    "Expire": "Date the address expired.",
    "DateUpdate": "Date of the provider's last update to the record.",
    "AnomStatus": "Anomaly status.",
    "LocatnDesc": "Location description.",
    "Addr_Type": "Address type (e.g. 'Residential', 'Commercial', 'Unknown').",
    "DeliverTyp": "Delivery type.",
    "NAD_Source": "The organization that provided the record to the NAD.",
    "DataSet_ID": "The provider's dataset identifier for the record.",
    "full_address": "Added here: one-line normalized address for matching, upper case, no "
                    "punctuation, USPS street type and direction abbreviations: '<number> "
                    "<street> <subaddress>, <city>, <state> <zip>', the street being every part "
                    "of the complete street name, modifiers included ('OLD COUNTY RD').",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the GeoParquet "
            "covering. Filter on it to prune row groups.",
    "geometry": "The address point as native Parquet GEOMETRY (WKB), lon/lat (OGC:CRS84), "
                "from Longitude and Latitude.",
}
CELL_DOCS = {
    "h3": "H3 cell index, as a string.",
    "h3_res": "H3 resolution of the cell (4, 6 or 8).",
    "addresses": "Address points of the release in the cell.",
    "state": "The state most of the cell's points belong to (postal code).",
    "geometry": "The cell's outline as native Parquet GEOMETRY (WKB), lon/lat (OGC:CRS84).",
}
# Address count classes per H3 resolution (cell areas differ by 49 between
# resolutions 4, 6 and 8), light to dark.
COUNT_STEPS = {4: [100, 1_000, 10_000, 100_000], 6: [10, 100, 1_000, 10_000],
               8: [1, 10, 100, 1_000]}
COLOURS = ["#edf8fb", "#b3cde3", "#8c96c6", "#8856a7", "#810f7c"]


# ---------- measured inputs ----------

def get_json(url: str):
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def release_path(out_dir: Path | None) -> str:
    """'nad/r24', from the local or the published index.json."""
    local = out_dir / "index.json" if out_dir else None
    index = json.loads(local.read_text()) if local and local.is_file() else \
        get_json(f"{PUBLIC_DATA}/index.json")
    return index["datasets"][0]["path"]


def read_json(out_dir: Path | None, rel: str, name: str) -> dict:
    local = out_dir / rel / name if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    return get_json(f"{PUBLIC_DATA}/{rel}/{name}")


def source(out_dir: Path | None, rel: str, file: str) -> str:
    local = out_dir / rel / file if out_dir else None
    return str(local) if local and local.is_file() else f"{PUBLIC_DATA}/{rel}/{file}"


def connect():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    return con


def footer(src: str) -> list[tuple[str, str]]:
    return [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
            connect().execute(f"DESCRIBE SELECT * FROM read_parquet('{src}', "
                              "hive_partitioning = false)").fetchall()]


def extent_of(con, cells_src: str) -> list[float]:
    """From the resolution-8 H3 cells, which cover every point: an outer
    bound padded by at most a cell (about a kilometre), read in seconds where
    the 99 million points would take minutes. With points on both sides of
    the antimeridian the box crosses it, written west > east from the
    measured longitude gaps."""
    from extent import lon_intervals, wrapped_bbox
    src = (f"(SELECT geometry, struct_pack(xmin := ST_XMin(geometry), ymin := ST_YMin(geometry), "
           f"xmax := ST_XMax(geometry), ymax := ST_YMax(geometry)) AS bbox "
           f"FROM read_parquet('{cells_src}') WHERE h3_res = 8)")
    y0, y1 = con.execute(f"SELECT min(ST_YMin(geometry)), max(ST_YMax(geometry)) "
                         f"FROM {src}").fetchone()
    b = wrapped_bbox(lon_intervals(con, src), y0, y1)
    return [round(b[0], 6), round(b[1], 6), round(b[2], 6), round(b[3], 6)]


# ---------- styles ----------

def coverage_style(rel: str, tiles: dict) -> dict:
    layers = []
    for lo, hi, res in H3_BANDS:
        steps = COUNT_STEPS[res]
        fill = ["step", ["get", "addresses"], COLOURS[0]]
        for threshold, colour in zip(steps, COLOURS[1:]):
            fill += [threshold, colour]
        layers.append({"id": f"cells-r{res}", "type": "fill", "source": "data",
                       "source-layer": f"cells_r{res}", "minzoom": lo, "maxzoom": hi + 1,
                       "paint": {"fill-color": fill, "fill-opacity": 0.8}})
    return {
        "version": 8,
        "name": "NAD coverage: address points per H3 cell",
        "sources": {"data": {"type": "vector",
                             "url": f"pmtiles://{PUBLIC_DATA}/{rel}/{tiles['file']}",
                             "attribution": "U.S. Department of Transportation, National Address "
                                            "Database"}},
        "layers": layers,
    }


# ---------- STAC ----------

def asset_s3(path: str) -> dict:
    return {"s3": {"href": f"s3://{BUCKET}/{path}", "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}


def state_assets(rel: str, manifest: dict, coverage: dict) -> dict:
    assets = {}
    for f in manifest["files"]:
        st = f["state"]
        path = f"{DATASET_PREFIX}/{rel}/{f['path']}"
        assets[f"data-{st.lower()}"] = {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": f"{coverage['states'][st]['name']} ({st})",
            "description": f"{f['features']:,} address points in {f['row_groups']} row groups",
            "roles": ["data"],
            "file:size": f["bytes"],
            "file:checksum": "1220" + f["sha256"],
            "alternate": asset_s3(path),
        }
    return assets


def attribution_text(manifest: dict) -> str:
    return (f"U.S. Department of Transportation, National Address Database, release "
            f"{manifest['release']}")


def gate_text(manifest: dict) -> str:
    g = manifest["gate"]
    return (f"Every row of the release's text file is accounted for: {g['csv_rows']:,} rows, "
            f"{g['rows_written']:,} written in {g['states']} state files, "
            f"{g['without_point']:,} without usable coordinates and {g['without_state']:,} "
            f"without a state code left out. {g['outside_state_box']:,} points lie outside "
            "their state's TIGER/Line box and are kept. OID_ is unique; "
            f"{g['uuid_values_repeated']:,} UUID values repeat.")


def nad_collection(rel: str, manifest: dict, coverage: dict, columns, extent, updated: str) -> dict:
    release = manifest["release"]
    glob = manifest["glob"]
    missing = ", ".join(m["state"] for m in coverage["missing"]) or "none"
    undocumented = [c for c, _ in columns if c not in COLUMN_DOCS]
    if undocumented:
        sys.exit(f"nad: no column doc for {undocumented}")
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT, VERSION_EXT,
                            ALTERNATE_EXT],
        "id": "nad",
        "title": f"National Address Database, release {release}: address points",
        "description": (
            f"The US Department of Transportation's National Address Database, release "
            f"{release} (compiled {manifest['compiled']}): {manifest['total_features']:,} "
            f"address points from {len(manifest['files'])} states and territories, one "
            "GeoParquet file per state (postal code). NAD's 60 columns unchanged, plus "
            "full_address, a normalized one-line address for matching. Read every state at "
            f"once with the partition glob {glob} through the anonymous S3 endpoint "
            f"{S3_ENDPOINT} (path-style, empty credentials), or one state over HTTPS. No "
            f"rows for: {missing}. {gate_text(manifest)} {SCOPE} Credit: "
            f"{attribution_text(manifest)}."),
        "keywords": ["National Address Database", "NAD", "addresses", "address points",
                     "geocoding", "USDOT", "United States", "GeoParquet"],
        "license": "other",
        "attribution": attribution_text(manifest),
        "version": f"r{release}",
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [extent]},
                   "temporal": {"interval": [[None, None]]}},
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [{"name": "state", "type": "string",
                            "description": "USPS postal code of the state, DC or territory. "
                                           "One file per state."}],
        "partition:file_count": len(manifest["files"]),
        "partition:glob": glob,
        "table:row_count": manifest["total_features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": COLUMN_DOCS[n]}
                          for n, t in columns],
        "assets": {
            **state_assets(rel, manifest, coverage),
            "coverage": {
                "href": f"{PUBLIC_DATA}/{rel}/_coverage.json", "type": "application/json",
                "title": "Coverage per state, and the states with no rows",
                "roles": ["metadata"]},
            "thumbnail": {
                "href": "./thumbnail.png", "type": "image/png",
                "title": "Address points per H3 cell (resolution 4)", "roles": ["thumbnail"],
                "file:size": (THUMBS / "nad.png").stat().st_size,
                "file:checksum": multihash(THUMBS / "nad.png")},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", "NAD address points: README"),
            md_link("agents", "./AGENTS.md", "NAD address points: agent guide"),
            LICENSE_LINK, VIA_LINK, ABOUT_LINK,
            {"rel": "related", "href": f"{PUBLIC_DATA}/{rel}/ATTRIBUTION.txt",
             "type": "text/plain", "title": "Attribution and what was changed"},
            {"rel": "alternate", "href": f"{PUBLIC_DATA}/{rel}/", "type": "text/html",
             "title": "Browse the files"},
        ],
    }


def coverage_collection(rel: str, manifest: dict, columns, extent, updated: str) -> dict:
    h3, tiles = manifest["h3"], manifest["pmtiles"]
    cells_path = f"{DATASET_PREFIX}/{rel}/{h3['file']}"
    tiles_path = f"{DATASET_PREFIX}/{rel}/{tiles['file']}"
    counts = ", ".join(f"{n:,} at resolution {r}" for r, n in sorted(h3["cells"].items(),
                                                                     key=lambda kv: int(kv[0])))
    bands = "; ".join(f"zoom {b['min_zoom']}-{b['max_zoom']}: resolution {b['h3_res']} "
                      f"(layer {b['layer']})" for b in tiles["bands"])
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT,
                            WEB_MAP_LINKS_EXT],
        "id": "coverage",
        "title": f"National Address Database, release {manifest['release']}: coverage on H3 cells",
        "description": (
            "Where the release has address points: the points counted on H3 cells at "
            f"resolutions 4, 6 and 8 ({counts} cells), with each cell's outline and main "
            "state, and a PMTiles map of them. A cell with no points is absent, which on a "
            "map shows the counties and states that do not contribute. Credit: "
            f"{attribution_text(manifest)}."),
        "keywords": ["National Address Database", "coverage", "H3", "address density"],
        "license": "other",
        "attribution": attribution_text(manifest),
        "version": f"r{manifest['release']}",
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [extent]},
                   "temporal": {"interval": [[None, None]]}},
        "table:row_count": sum(h3["cells"].values()),
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": CELL_DOCS[n]}
                          for n, t in columns],
        "assets": {
            "data": {
                "href": f"{PUBLIC_BASE}/{cells_path}", "type": PARQUET_TYPE,
                "title": "Address points per H3 cell, resolutions 4, 6 and 8",
                "roles": ["data"], "file:size": h3["bytes"],
                "file:checksum": "1220" + h3["sha256"], "alternate": asset_s3(cells_path)},
            "pmtiles": {
                "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
                "title": "Coverage map, vector tiles",
                "description": f"PMTiles, one band per resolution ({bands}), {tiles['tiles']:,} "
                               f"tiles; properties h3, addresses, state. Built with tylertoo "
                               f"{tiles['tylertoo']}.",
                "roles": ["visual"], "file:size": tiles["bytes"],
                "file:checksum": "1220" + tiles["sha256"], "alternate": asset_s3(tiles_path)},
            "style": {"href": "./styles/default.json", "type": STYLE_TYPE,
                      "title": "Address points per cell, light to dark",
                      "roles": ["style", "default"]},
            "thumbnail": {
                "href": "./thumbnail.png", "type": "image/png",
                "title": "Address points per H3 cell (resolution 4)", "roles": ["thumbnail"],
                "file:size": (THUMBS / "coverage.png").stat().st_size,
                "file:checksum": multihash(THUMBS / "coverage.png")},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", "NAD coverage: README"),
            md_link("agents", "./AGENTS.md", "NAD coverage: agent guide"),
            LICENSE_LINK, VIA_LINK,
            {"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
             "title": "Coverage map, vector tiles",
             "pmtiles:layers": [b["layer"] for b in tiles["bands"]]},
        ],
    }


def build_root(collections: list[dict], manifest: dict, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (
            f"The US Department of Transportation's National Address Database, release "
            f"{manifest['release']}: {manifest['total_features']:,} address points as "
            "GeoParquet, one file per state, NAD's columns unchanged with a normalized "
            "full_address for matching, and the release's coverage on H3 cells with a map. "
            f"Public domain. {SCOPE} Credit: {attribution_text(manifest)}."),
        "version": f"r{manifest['release']}",
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK, VIA_LINK, ABOUT_LINK,
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


# ---------- markdown ----------

def coverage_table(coverage: dict) -> str:
    rows = "\n".join(
        f"| {s} | {v['name']} | {v['rows']:,} | {v['counties']} of {v['tiger_counties']} "
        f"| {v['latest_update'] or ''} | {v['update_after_compile']:,} |"
        for s, v in coverage["states"].items())
    missing = "\n".join(f"- {m['state']}, {m['name']}" + (" (territory)" if m["territory"] else "")
                        for m in coverage["missing"]) or "- none"
    return f"""\
Counties: the distinct County names in the state's rows, of the counties and
equivalents TIGER/Line 2026 has. A state with all of its counties can still
miss addresses within them: the coverage collection maps the points.
`Latest update` is the latest DateUpdate up to the compile date; the last
column counts DateUpdate values after it, provider typos (3034 for 2034)
kept in the files as shipped.

| State | Name | Address points | Counties | Latest update | Dates after the compile date |
|---|---|---|---|---|---|
{rows}

No rows in this release:

{missing}"""


GEOCODE_SQL = """\
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
-- Normalize the input the way full_address is: upper case, no punctuation,
-- USPS abbreviations (ROAD -> RD, NORTH -> N), '<street>, <city>, <state> <zip>'.
SELECT full_address, Longitude, Latitude, Placement
FROM read_parquet('{url}')
WHERE full_address LIKE '1 CITY HALL SQ, BOSTON, MA%';"""


def access_md(rel: str, manifest: dict) -> str:
    url = f"{PUBLIC_DATA}/{rel}/state=MA/addresses.parquet"
    return f"""\
One state over HTTPS (DuckDB, GDAL, polars, pyarrow read it in place):

```sql
{GEOCODE_SQL.format(url=url)}
```

Every state at once, through the anonymous S3 endpoint `{S3_ENDPOINT}`:

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, ENDPOINT '{S3_ENDPOINT}', URL_STYLE 'path', KEY_ID '', SECRET '');
-- The State column already holds the partition value: turn DuckDB's Hive
-- partitioning off, or it adds a second column named state_1.
SELECT State, count(*) FROM read_parquet('{manifest['glob']}', hive_partitioning = false)
GROUP BY State ORDER BY 2 DESC;
```"""


def collection_readme(col: dict, body: str) -> str:
    return f"""\
# {col['title']}

{col['description']}

{body}

## License

Public domain: a work of the US federal government, not subject to copyright in
the United States (17 U.S.C. 105). Credit "{col['attribution']}".

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def nad_agents(rel: str, manifest: dict, coverage: dict) -> str:
    missing = ", ".join(m["state"] for m in coverage["missing"]) or "none"
    return f"""\
# nad: agent guide

US address points, release {manifest['release']} of USDOT's National Address
Database: one GeoParquet file per state at
`{PUBLIC_DATA}/{rel}/state=<XX>/addresses.parquet` (postal code), lon/lat
(OGC:CRS84), sorted on a Hilbert curve with a `bbox` covering.

## Access

{access_md(rel, manifest)}

## Query tips

- Filter on `bbox` (FLOAT bounds) before any spatial predicate; within a
  state a point lookup reads one row group.
- Match addresses on `full_address`: normalize the input first (upper case,
  punctuation to spaces, USPS abbreviations for street types and directions,
  '<number> <street> <unit>, <city>, <state> <zip>', the street with its
  modifiers: 'OLD COUNTY RD', 'MAIN ST EXT'). For fuzzy matches,
  restrict by Zip_Code or State first, then `jaro_winkler_similarity`.
- Coverage is uneven: no rows for {missing}; within a state, check
  `_coverage.json` (counties present) or the coverage collection before
  calling an address missing.
- `Placement` says how a point was placed: a rooftop point is not a parcel
  centroid.
- OID_ is unique in this release only; the provider's UUID identifies a point
  across releases ({manifest['gate']['uuid_values_repeated']:,} UUID values
  repeat in this release).
- Credit: {attribution_text(manifest)}.
"""


def coverage_agents(rel: str, manifest: dict) -> str:
    return f"""\
# coverage: agent guide

Address points of NAD release {manifest['release']} counted on H3 cells:
`{PUBLIC_DATA}/{rel}/{manifest['h3']['file']}`, one row per cell and resolution
(`h3_res` 4, 6 or 8), with `addresses`, `state` and the cell outline.

```sql
INSTALL httpfs; LOAD httpfs;
SELECT state, sum(addresses) FROM read_parquet('{PUBLIC_DATA}/{rel}/{manifest['h3']['file']}')
WHERE h3_res = 4 GROUP BY 1 ORDER BY 2 DESC;
```

A cell absent from the file has no address point in the release. Use it to
say where the NAD can answer before querying the state files.
"""


def root_readme(root: dict, coverage: dict) -> str:
    return f"""\
# {root['title']}

{root['description']}

## Coverage

{coverage_table(coverage)}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(rel: str, manifest: dict) -> str:
    return f"""\
# {CATALOG_ID}: agent guide

- `nad`: the address points, one file per state. nad/AGENTS.md
- `coverage`: address counts on H3 cells, and the map. coverage/AGENTS.md

Release {manifest['release']} at `{PUBLIC_DATA}/{rel}/`. Public domain; credit
"{attribution_text(manifest)}".
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    rel = release_path(out_dir)
    manifest = read_json(out_dir, rel, "_manifest.json")
    coverage = read_json(out_dir, rel, "_coverage.json")
    for name in ("nad", "coverage"):
        if not (THUMBS / f"{name}.png").is_file():
            sys.exit(f"missing {THUMBS / name}.png: run the thumbnails command")
    con = connect()
    state_srcs = [source(out_dir, rel, f["path"]) for f in manifest["files"]]
    cells_src = source(out_dir, rel, manifest["h3"]["file"])
    extent = extent_of(con, cells_src)
    nad = nad_collection(rel, manifest, coverage, footer(state_srcs[0]), extent, updated)
    cov = coverage_collection(rel, manifest, footer(cells_src), extent, updated)

    for col, readme, agents in (
            (nad, collection_readme(nad, "## Access\n\n" + access_md(rel, manifest)
                                    + "\n\n## Coverage\n\n" + coverage_table(coverage)),
             nad_agents(rel, manifest, coverage)),
            (cov, collection_readme(cov, "## Access\n\n```sql\n"
                                    f"SELECT * FROM read_parquet('{PUBLIC_DATA}/{rel}/"
                                    f"{manifest['h3']['file']}') WHERE h3_res = 4;\n```"),
             coverage_agents(rel, manifest))):
        cdir = dest / col["id"]
        cdir.mkdir(parents=True)
        if col["id"] == "coverage":
            write_json(cdir / "styles" / "default.json", coverage_style(rel, manifest["pmtiles"]))
            asset = col["assets"]["style"]
            asset["file:size"] = (cdir / asset["href"]).stat().st_size
            asset["file:checksum"] = multihash(cdir / asset["href"])
        write_json(cdir / "collection.json", col)
        shutil.copyfile(THUMBS / f"{col['id']}.png", cdir / "thumbnail.png")
        (cdir / "README.md").write_text(readme)
        (cdir / "AGENTS.md").write_text(agents)
    root = build_root([nad, cov], manifest, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, coverage))
    (dest / "AGENTS.md").write_text(root_agents(rel, manifest))
    filedates.apply(dest, filedates.load(PUBLIC_DATA, *STAMPS))
    for doc in ("README.md", "AGENTS.md"):
        path = dest / doc
        path.write_text(f"{path.read_text().rstrip()}\n\n{filedates.DATES_MD}\n")
    return manifest["total_features"]


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="nad-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        total = build(out_dir, tree)
        print(f"  rendered 2 collections, {total:,} address points")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

CONUS = (-125.0, 24.0, -66.5, 49.5)


def thumbnails(out_dir: Path) -> None:
    """The resolution-4 cells over the conterminous US, coloured like the
    default style: the same picture for both collections."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    from thumbnails import BACKGROUND, frame

    rel = release_path(out_dir)
    manifest = read_json(out_dir, rel, "_manifest.json")
    cells = out_dir / rel / manifest["h3"]["file"]
    con = connect()
    rows = con.execute(f"""
        SELECT addresses, ST_AsWKB(geometry) FROM read_parquet('{cells}')
        WHERE h3_res = 4""").fetchall()
    steps = COUNT_STEPS[4]
    polys, fills = [], []
    for n, wkb in rows:
        part = shapely.from_wkb(bytes(wkb))
        if part.geom_type != "Polygon":
            continue
        polys.append(shapely.get_coordinates(part.exterior))
        fills.append(COLOURS[sum(n >= t for t in steps)])
    THUMBS.mkdir(parents=True, exist_ok=True)
    x0, y0, x1, y1, aspect = frame(*CONUS)
    fig = plt.figure(figsize=(6, 4), dpi=100)
    ax = fig.add_axes((0, 0, 1, 1))
    ax.set_facecolor(BACKGROUND)
    fig.patch.set_facecolor(BACKGROUND)
    ax.add_collection(PolyCollection(polys, facecolors=fills, edgecolors="none", alpha=0.9))
    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.set_aspect(aspect)
    ax.axis("off")
    for name in ("nad", "coverage"):
        fig.savefig(THUMBS / f"{name}.png", dpi=100, facecolor=BACKGROUND)
    plt.close(fig)
    print(f"  {len(polys):,} cells -> {THUMBS.relative_to(REPO)}/nad.png, coverage.png")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp, required=False):
        sp.add_argument("--out-dir", type=Path, default=None, required=required,
                        help="nad.py's --out-dir (default: read the published files)")

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
        print(f"wrote {args.dest}/ ({build(args.out_dir, args.dest):,} address points)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    elif args.cmd == "publish":
        publish(args.out_dir, args.remote, dry_run=args.dry_run)
    elif args.cmd == "thumbnails":
        thumbnails(args.out_dir)


if __name__ == "__main__":
    main()
