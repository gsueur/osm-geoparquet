#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the US Fish and
Wildlife Service's National Wetlands Inventory (NWI).

  wetlands/catalog/catalog.json              root: National Wetlands Inventory
  wetlands/catalog/nwi/collection.json       the wetland polygons, one file per
                                             state, the PMTiles map, the code table
  wetlands/catalog/projects/collection.json  the mapping projects (imagery year, scale)

The `nwi` collection is partitioned like the NAD's: a `partition:glob` over
the state files through the anonymous S3 endpoint, and one `data-<xx>` asset
per state titled with the state's name. Release paths carry the release month
(2026-10), so each asset carries its size and checksum. Counts, sizes,
checksums, extents, the dedup and the gate come from the release's
_manifest.json, local (--out-dir) or published. Spec: portolan-spec v0.2.0.
Validator: rashid, through scripts/catalog.py.

Dates (scripts/filedates.py): each GeoParquet's `updated` is when its bytes
last changed, `checked` when it was last built. The release's files are
written by many jobs and copied server-side, so no machine holds them: the
stamp (wetlands/_files/nwi.json) is made here, at the catalog stage, from the
sha256 the manifest records, compared with the published stamp; a changed
file takes the build time of the part that wrote it, so rendering again gives
the same dates. publish uploads it after the catalog. The map is drawn on a
workstation on its own schedule: it is not stamped.

Usage:
  python3 scripts/datasets/nwi_catalog.py build --dest build/nwi-catalog [--out-dir out/wetlands]
  python3 scripts/datasets/nwi_catalog.py check build/nwi-catalog
  python3 scripts/datasets/nwi_catalog.py publish --remote parquetry:parquetry/wetlands [--out-dir out/wetlands]
  uv run --project scripts --with matplotlib --with shapely \\
      python scripts/datasets/nwi_catalog.py thumbnails --out-dir out/wetlands
"""

from __future__ import annotations

import argparse
import email.utils
import json
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import filedates
from datasets.gaul_catalog import STYLE_TYPE, WEB_MAP_LINKS_EXT  # noqa: E402
from datasets.nwi import (  # noqa: E402
    CODES_PAGE,
    CODES_URL,
    DECODED,
    DISCLAIMER,
    DOWNLOAD_PAGE,
    WORKSTATION_UNITS,
)
from datasets.pmtiles import PMTILES_TYPE  # noqa: E402
from extent import wrapped_bbox  # noqa: E402

import catalog as shared  # noqa: E402  scripts/catalog.py
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

DATASET_PREFIX = "wetlands"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "usfws-national-wetlands-inventory"
STAMP = "_files/nwi.json"
TITLE = "USFWS National Wetlands Inventory: wetlands of the United States"
THUMBS = REPO / "catalog" / "thumbnails" / "wetlands"
ATTRIBUTION = "U.S. Fish and Wildlife Service, National Wetlands Inventory"

LICENSE_LINK = {
    "rel": "license",
    "href": "https://www.usa.gov/government-copyright",
    "type": "text/html",
    "title": "Works of the US federal government are not subject to copyright in the "
             "United States (17 U.S.C. 105)",
}
VIA_LINK = {"rel": "via", "href": DOWNLOAD_PAGE, "type": "text/html",
            "title": "NWI state downloads (USFWS), where it is published"}
CODES_LINK = {"rel": "describedby", "href": CODES_PAGE, "type": "text/html",
              "title": "NWI classification codes (USFWS)"}
DISCLAIMER_LINK = {"rel": "related", "href": DISCLAIMER, "type": "text/html",
                   "title": "USFWS wetlands geodatabase disclaimer"}
PROVIDERS = [
    {
        "name": "U.S. Fish and Wildlife Service, National Wetlands Inventory",
        "description": "Maps wetlands and deepwater habitats from imagery and fieldwork, "
                       "classified to the federal Cowardin standard, and publishes them per "
                       "state twice a year. Public domain.",
        "url": DOWNLOAD_PAGE,
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted the state geodatabases to GeoParquet, removed the copies "
                       "repeated across state downloads, decoded the codes, built the map, "
                       "and maintains and hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]
SCOPE = ("Not a jurisdictional delineation: the NWI describes wetlands by their biology, "
         "and does not define the limits of any government's jurisdiction (Clean Water Act "
         "or otherwise). Mapping dates and scales differ by project (the projects "
         "collection lists them).")

COLUMN_DOCS = {
    "NWI_ID": "FWS's identifier of the polygon. The same NWI_ID can appear in several state "
              "downloads; here each is kept once, in the file of the state that holds it.",
    "ATTRIBUTE": "The polygon's Cowardin code as FWS writes it (system, subsystem, class, "
                 "subclass, water regime, modifiers: PFO1E, E2EM1P, R2UBH). The *_NAME "
                 "columns spell it out.",
    "WETLAND_TYPE": "FWS's general type: Freshwater Emergent Wetland, Freshwater "
                    "Forested/Shrub Wetland, Freshwater Pond, Estuarine and Marine Wetland, "
                    "Estuarine and Marine Deepwater, Lake, Riverine or Other.",
    "ACRES": "Area in acres, as FWS computes it.",
    "SYSTEM_NAME": "Cowardin system: Marine, Estuarine, Riverine, Lacustrine or Palustrine.",
    "SUBSYSTEM_NAME": "Cowardin subsystem (Intertidal, Subtidal, Lower Perennial, Limnetic...); "
                      "NULL for Palustrine, which has none.",
    "CLASS_NAME": "Cowardin class: Emergent, Forested, Scrub-Shrub, Unconsolidated Bottom, "
                  "Aquatic Bed, Streambed...",
    "SUBCLASS_NAME": "Cowardin subclass (Persistent, Broad-Leaved Deciduous, Needle-Leaved "
                     "Evergreen...), where coded.",
    "SPLIT_CLASS_NAME": "The second class of a mixed code (PFO1/SS1E: Forested and "
                        "Scrub-Shrub).",
    "SPLIT_SUBCLASS_NAME": "The second subclass of a mixed code.",
    "WATER_REGIME_NAME": "Water regime: how long and how often the habitat is flooded or "
                         "saturated (Seasonally Flooded, Permanently Flooded, Irregularly "
                         "Flooded...).",
    "MODIFIER1_NAME": "First special modifier (Diked/Impounded, Excavated, Beaver, Farmed, "
                      "Partially Drained/Ditched) or a water chemistry or soil modifier.",
    "MODIFIER2_NAME": "Second modifier.",
    "source_unit": "Added here: the FWS download (state or territory unit: PRVI, PacTrust) the "
                   "row was taken from.",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the GeoParquet "
            "covering. Filter on it to prune row groups.",
    "geometry": "The polygon as native Parquet GEOMETRY (WKB), NAD83 lon/lat (EPSG:4269), "
                "reprojected from FWS's Albers.",
}
PROJECT_DOCS = {
    "PROJECT_NAME": "Mapping project name.",
    "STATUS": "Project status as FWS records it (Digital...).",
    "SUPPMAPINFO": "Supplementary map information (a link or None).",
    "FGDC_METADATA": "Link to the project's FGDC metadata.",
    "DATA_SOURCE": "Data source (PacTrust only).",
    "IMAGE_YR": "Year of the imagery the project was mapped from.",
    "IMAGE_DATE": "Date of the imagery, as FWS writes it (04/77).",
    "IMAGE_SCALE": "Scale denominator of the imagery (80000 for 1:80,000).",
    "ALL_SCALES": "Every imagery scale used, as text.",
    "EMULSION": "Imagery type (BW, CIR, color...).",
    "COMMENTS": "FWS's comments on the project.",
    "SOURCE_TYPE": "Source type code.",
    "source_units": "Added here: the FWS downloads that ship this project polygon.",
    "bbox": COLUMN_DOCS["bbox"],
    "geometry": "The project's footprint as native Parquet GEOMETRY (WKB), NAD83 lon/lat "
                "(EPSG:4269).",
}
# Fill colours per WETLAND_TYPE, ours.
TYPE_COLOURS = {
    "Estuarine and Marine Deepwater": "#1f6f8b",
    "Estuarine and Marine Wetland": "#5fb3a1",
    "Freshwater Emergent Wetland": "#9bc53d",
    "Freshwater Forested/Shrub Wetland": "#2e7d32",
    "Freshwater Pond": "#5b8fd1",
    "Lake": "#1a3e8c",
    "Riverine": "#2aa9d6",
    "Other": "#b08d57",
}
# The thumbnail frame: Narragansett Bay and the Massachusetts border, where
# the downloads of three states overlap.
THUMB_BOX = (-71.65, 41.35, -70.95, 41.95)
THUMB_STATES = ("CT", "MA", "RI")


# ---------- measured inputs ----------

def get_json(url: str):
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def release_path(out_dir: Path | None) -> str:
    local = out_dir / "index.json" if out_dir else None
    index = json.loads(local.read_text()) if local and local.is_file() else \
        get_json(f"{PUBLIC_DATA}/index.json")
    return index["datasets"][0]["path"]


def read_manifest(out_dir: Path | None, rel: str) -> dict:
    local = out_dir / rel / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    return get_json(f"{PUBLIC_DATA}/{rel}/_manifest.json")


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


def extent_of(files: list[dict]) -> list[float]:
    """From the longitude intervals and latitudes nwi.py measured per state:
    with Alaska, Guam and American Samoa the box crosses the antimeridian,
    written west > east."""
    intervals = [iv for f in files for iv in f["lon_intervals"]]
    b = wrapped_bbox(intervals, min(f["ymin"] for f in files), max(f["ymax"] for f in files))
    return [round(b[0], 6), round(b[1], 6), round(b[2], 6), round(b[3], 6)]


def projects_extent(con, src: str) -> list[float]:
    from extent import lon_intervals
    rows = f"read_parquet('{src}', hive_partitioning = false)"
    y0, y1 = con.execute(f"SELECT min(bbox.ymin), max(bbox.ymax) FROM {rows}").fetchone()
    b = wrapped_bbox(lon_intervals(con, rows), y0, y1)
    return [round(b[0], 6), round(b[1], 6), round(b[2], 6), round(b[3], 6)]


# ---------- styles ----------

def wetlands_style(rel: str, tiles: dict) -> dict:
    fill = ["match", ["get", "WETLAND_TYPE"]]
    for t, c in TYPE_COLOURS.items():
        fill += [t, c]
    fill.append("#999999")
    return {
        "version": 8,
        "name": "NWI wetlands by WETLAND_TYPE",
        "sources": {"data": {"type": "vector",
                             "url": f"pmtiles://{PUBLIC_DATA}/{rel}/{tiles['file']}",
                             "attribution": ATTRIBUTION}},
        "layers": [
            {"id": "wetlands", "type": "fill", "source": "data", "source-layer": tiles["layer"],
             "paint": {"fill-color": fill, "fill-opacity": 0.75}},
            {"id": "wetlands-outline", "type": "line", "source": "data",
             "source-layer": tiles["layer"], "minzoom": 12,
             "paint": {"line-color": "#1b3a2f", "line-width": 0.4, "line-opacity": 0.6}},
        ],
    }


# ---------- STAC ----------

def asset_s3(path: str) -> dict:
    return {"s3": {"href": f"s3://{BUCKET}/{path}", "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}


def state_assets(rel: str, manifest: dict) -> dict:
    assets = {}
    for f in manifest["files"]:
        st = f["state"]
        path = f"{DATASET_PREFIX}/{rel}/{f['path']}"
        assets[f"data-{st.lower()}"] = {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": f"{f['name']} ({st})",
            "description": f"Wetlands in {f['name']}: {f['features']:,} polygons in "
                           f"{f['row_groups']} row groups.",
            "roles": ["data"],
            "file:size": f["bytes"],
            "file:checksum": "1220" + f["sha256"],
            "alternate": asset_s3(path),
        }
    return assets


def dedup_text(manifest: dict) -> str:
    read = sum(p["dedup"]["copies_read"] for p in manifest["parts"])
    dropped = sum(p["dedup"]["copies_dropped"] for p in manifest["parts"])
    return (f"FWS's state downloads overlap at the borders: of {read:,} rows read, "
            f"{dropped:,} repeated an NWI_ID and were dropped, each NWI_ID kept "
            "once in the file of the state that holds its interior point, from that state's "
            "own download where it has the polygon (the largest copy otherwise).")


def nwi_collection(rel: str, manifest: dict, columns, extent, updated: str) -> dict:
    tiles, codes = manifest["pmtiles"], manifest["codes"]
    glob = manifest["glob"]
    undocumented = [c for c, _ in columns if c not in COLUMN_DOCS]
    if undocumented:
        sys.exit(f"nwi: no column doc for {undocumented}")
    codes_path = f"{DATASET_PREFIX}/{rel}/{codes['file']}"
    assets = {
        **state_assets(rel, manifest),
        "codes": {
            "href": f"{PUBLIC_BASE}/{codes_path}", "type": PARQUET_TYPE,
            "title": "NWI Code Definitions (USFWS): every ATTRIBUTE code with its Cowardin "
                     "names and definitions",
            "description": f"{codes['rows']:,} codes, FWS's table as published ({CODES_URL}); "
                           "join on ATTRIBUTE for the definitions the *_NAME columns leave out.",
            "roles": ["metadata"], "file:size": codes["bytes"],
            "file:checksum": "1220" + codes["sha256"], "alternate": asset_s3(codes_path)},
        "thumbnail": {
            "href": "./thumbnail.png", "type": "image/png",
            "title": "Wetlands around Narragansett Bay, by type", "roles": ["thumbnail"],
            "file:size": (THUMBS / "nwi.png").stat().st_size,
            "file:checksum": multihash(THUMBS / "nwi.png")},
    }
    links = [
        {"rel": "root", "href": "../catalog.json", "type": "application/json"},
        {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
        md_link("describedby", "./README.md", "NWI wetlands: README"),
        md_link("agents", "./AGENTS.md", "NWI wetlands: agent guide"),
        LICENSE_LINK, VIA_LINK, CODES_LINK, DISCLAIMER_LINK,
        {"rel": "related", "href": f"{PUBLIC_DATA}/{rel}/ATTRIBUTION.txt",
         "type": "text/plain", "title": "Attribution and what was changed"},
        {"rel": "alternate", "href": f"{PUBLIC_DATA}/{rel}/", "type": "text/html",
         "title": "Browse the files"},
    ]
    extensions = [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT, VERSION_EXT,
                  ALTERNATE_EXT]
    if tiles:
        tiles_path = f"{DATASET_PREFIX}/{rel}/{tiles['file']}"
        assets["pmtiles"] = {
            "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
            "title": "Wetlands, vector tiles",
            "description": f"PMTiles, layer `{tiles['layer']}`, zoom {tiles['min_zoom']} to "
                           f"{tiles['max_zoom']} (thinned below {tiles['max_zoom']}), "
                           f"{tiles['tiles']:,} tiles; properties "
                           f"{', '.join(tiles['properties'])}. Built with tylertoo "
                           f"{tiles['tylertoo']}.",
            "roles": ["visual"], "file:size": tiles["bytes"],
            "file:checksum": "1220" + tiles["sha256"], "alternate": asset_s3(tiles_path)}
        assets["style"] = {"href": "./styles/default.json", "type": STYLE_TYPE,
                           "title": "Wetlands by WETLAND_TYPE", "roles": ["style", "default"]}
        links.append({"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}",
                      "type": PMTILES_TYPE, "title": "Wetlands, vector tiles",
                      "pmtiles:layers": [tiles["layer"]]})
        extensions.append(WEB_MAP_LINKS_EXT)
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": extensions,
        "id": "nwi",
        "title": f"Wetlands (USFWS National Wetlands Inventory, {manifest['last_updated']})",
        "description": (
            f"The National Wetlands Inventory of the U.S. Fish and Wildlife Service, release of "
            f"{manifest['last_updated']}: {manifest['total_features']:,} wetland and deepwater "
            f"polygons in {len(manifest['files'])} states and territories, one GeoParquet file "
            "per state (postal code; UM for the Pacific remote islands). FWS's ATTRIBUTE "
            "(Cowardin code), WETLAND_TYPE, ACRES and NWI_ID, with the code spelled out from "
            "FWS's NWI Code Definitions table. NAD83 lon/lat (EPSG:4269). Read every state at "
            f"once with the partition glob {glob} through the anonymous S3 endpoint "
            f"{S3_ENDPOINT} (path-style, empty credentials), or one state over HTTPS. "
            f"{dedup_text(manifest)} Riparian layers are not included. {SCOPE} Credit: "
            f"{ATTRIBUTION}."),
        "keywords": ["National Wetlands Inventory", "NWI", "wetlands", "Cowardin", "USFWS",
                     "deepwater habitats", "United States", "GeoParquet"],
        "license": "other",
        "attribution": ATTRIBUTION,
        "version": manifest["release"],
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [extent]},
                   "temporal": {"interval": [[None, None]]}},
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [{"name": "state", "type": "string",
                            "description": "USPS postal code of the state, DC or territory "
                                           "(UM: U.S. Minor Outlying Islands). One file per "
                                           "state."}],
        "partition:file_count": len(manifest["files"]),
        "partition:glob": glob,
        "table:row_count": manifest["total_features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": COLUMN_DOCS[n]}
                          for n, t in columns],
        "assets": assets,
        "links": links,
    }


def projects_collection(rel: str, manifest: dict, columns, extent, updated: str) -> dict:
    p = manifest["projects"]
    path = f"{DATASET_PREFIX}/{rel}/{p['file']}"
    undocumented = [c for c, _ in columns if c not in PROJECT_DOCS]
    if undocumented:
        sys.exit(f"projects: no column doc for {undocumented}")
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT],
        "id": "projects",
        "title": f"Wetlands mapping projects (USFWS NWI, {manifest['last_updated']})",
        "description": (
            f"The footprints of the {p['features']:,} NWI mapping projects of the release, "
            "from each state's project metadata layer: where each part of the inventory comes "
            "from, the year, date, scale and type of its imagery. A project shipped in several "
            "state downloads is kept once (same attributes, same shape; a project cut "
            "differently by two downloads stays twice, listed in source_units). Join a "
            "wetland to the project under it to know how old its mapping is. Credit: "
            f"{ATTRIBUTION}."),
        "keywords": ["National Wetlands Inventory", "NWI", "mapping projects", "imagery year"],
        "license": "other",
        "attribution": ATTRIBUTION,
        "version": manifest["release"],
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [extent]},
                   "temporal": {"interval": [[None, None]]}},
        "table:row_count": p["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": PROJECT_DOCS[n]}
                          for n, t in columns],
        "assets": {
            "data": {"href": f"{PUBLIC_BASE}/{path}", "type": PARQUET_TYPE,
                     "title": "NWI mapping projects", "roles": ["data"],
                     "file:size": p["bytes"], "file:checksum": "1220" + p["sha256"],
                     "alternate": asset_s3(path)},
            "thumbnail": {
                "href": "./thumbnail.png", "type": "image/png",
                "title": "Mapping projects around Narragansett Bay, by imagery year",
                "roles": ["thumbnail"],
                "file:size": (THUMBS / "projects.png").stat().st_size,
                "file:checksum": multihash(THUMBS / "projects.png")},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", "NWI mapping projects: README"),
            md_link("agents", "./AGENTS.md", "NWI mapping projects: agent guide"),
            LICENSE_LINK, VIA_LINK,
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
            f"The U.S. Fish and Wildlife Service's National Wetlands Inventory, release of "
            f"{manifest['last_updated']}: {manifest['total_features']:,} wetland and deepwater "
            "polygons as GeoParquet, one file per state, each FWS code spelled out, with a "
            f"PMTiles map and the mapping projects. Public domain. {SCOPE} Credit: "
            f"{ATTRIBUTION}."),
        "version": manifest["release"],
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK, VIA_LINK, CODES_LINK, DISCLAIMER_LINK,
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


# ---------- markdown ----------

def states_table(manifest: dict) -> str:
    rows = "\n".join(f"| {f['state']} | {f['name']} | {f['features']:,} | "
                     f"{f['bytes'] / 1e6:,.1f} MB |" for f in manifest["files"])
    return f"""\
| State | Name | Polygons | File |
|---|---|---|---|
{rows}"""


def access_md(rel: str, manifest: dict) -> str:
    url = f"{PUBLIC_DATA}/{rel}/state=MA/wetlands.parquet"
    return f"""\
One state over HTTPS (DuckDB, GDAL, polars, pyarrow read it in place):

```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
-- Wetlands touching a box (Boston): filter on bbox first, it skips row
-- groups. ACRES is each whole polygon's: Massachusetts Bay counts entire.
SELECT WETLAND_TYPE, count(*) AS polygons, round(sum(ACRES)) AS acres
FROM read_parquet('{url}', hive_partitioning = false)
WHERE bbox.xmin <= -71.0 AND bbox.xmax >= -71.2 AND bbox.ymin <= 42.4 AND bbox.ymax >= 42.3
GROUP BY 1 ORDER BY 3 DESC;
```

Every state at once, through the anonymous S3 endpoint `{S3_ENDPOINT}`:

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, ENDPOINT '{S3_ENDPOINT}', URL_STYLE 'path', KEY_ID '', SECRET '');
SELECT SYSTEM_NAME, count(*) FROM read_parquet('{glob_of(manifest)}', hive_partitioning = false)
GROUP BY 1 ORDER BY 2 DESC;
```"""


def glob_of(manifest: dict) -> str:
    return manifest["glob"]


def collection_readme(col: dict, body: str) -> str:
    return f"""\
# {col['title']}

{col['description']}

{body}

{filedates.DATES_MD}

## License

Public domain: a work of the US federal government, not subject to copyright in
the United States (17 U.S.C. 105); USFWS states no access or use constraints.
Credit "{ATTRIBUTION}". Read FWS's disclaimer: {DISCLAIMER}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def nwi_agents(rel: str, manifest: dict) -> str:
    decoded = ", ".join(DECODED)
    return f"""\
# nwi: agent guide

US wetlands and deepwater habitats, the NWI release of {manifest['last_updated']}:
one GeoParquet file per state at
`{PUBLIC_DATA}/{rel}/state=<XX>/wetlands.parquet` (postal code; UM for
Palmyra and Kingman), NAD83 lon/lat (EPSG:4269), sorted on a Hilbert curve
with a `bbox` covering.

## Access

{access_md(rel, manifest)}

## Query tips

- Filter on `bbox` (FLOAT bounds) before any spatial predicate.
- Each polygon is in one file only: the copies FWS repeats across state
  downloads were removed, so a sum over several states counts each wetland
  once. A polygon that crosses a state line sits in the file of the state
  holding its interior point: for a state's exact wetland area, clip to the
  state boundary (TIGER/Line on parquetry, census/tiger/2026/substategeo/state.parquet).
- ACRES is FWS's area; deepwater (lakes, rivers, bays: WETLAND_TYPE Lake,
  Riverine, Estuarine and Marine Deepwater) is mapped too. "Wetland area"
  usually means the other types.
- `ATTRIBUTE` is the Cowardin code; {decoded} spell it out. The
  `codes` asset (codes.parquet) holds FWS's full definitions: join on ATTRIBUTE.
- How old the mapping is: join to the projects collection (IMAGE_YR) by location.
- Not a jurisdictional delineation (Clean Water Act or otherwise): say so when
  an answer could be read as one.
- Credit: {ATTRIBUTION}.
"""


def projects_agents(rel: str, manifest: dict) -> str:
    return f"""\
# projects: agent guide

The NWI mapping projects of the {manifest['last_updated']} release:
`{PUBLIC_DATA}/{rel}/{manifest['projects']['file']}`, one row per project
footprint, with the imagery year (IMAGE_YR), date, scale and type.

```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
-- The imagery year of the mapping under a point.
SELECT PROJECT_NAME, IMAGE_YR, ALL_SCALES
FROM read_parquet('{PUBLIC_DATA}/{rel}/{manifest['projects']['file']}')
WHERE bbox.xmin <= -71.4 AND bbox.xmax >= -71.4 AND bbox.ymin <= 41.8 AND bbox.ymax >= 41.8
  AND ST_Contains(geometry, ST_Point(-71.4, 41.8));
```
"""


def root_readme(root: dict, manifest: dict) -> str:
    return f"""\
# {root['title']}

{root['description']}

## States

{states_table(manifest)}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(rel: str, manifest: dict) -> str:
    return f"""\
# {CATALOG_ID}: agent guide

- `nwi`: the wetland polygons, one file per state, the map, the code table. nwi/AGENTS.md
- `projects`: the mapping projects (imagery year and scale). projects/AGENTS.md

Release {manifest['release']} at `{PUBLIC_DATA}/{rel}/`. Public domain; credit
"{ATTRIBUTION}". Not a jurisdictional delineation.

{filedates.DATES_MD}
"""


# ---------- dates ----------

def http_date(value: str) -> str:
    """An HTTP Last-Modified as RFC 3339 UTC."""
    return email.utils.parsedate_to_datetime(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def release_files(rel: str, manifest: dict) -> dict[str, dict]:
    """The release's GeoParquet files with the sha256 and size the manifest
    records, each with the build time of the part that wrote it (a state's
    rows come from the units listing it; codes and projects from every part)."""
    built_by_unit = {u: part["built"] for part in manifest["parts"] for u in part["units"]}
    latest = max(part["built"] for part in manifest["parts"])
    files = {}
    for f in manifest["files"]:
        units = [u["unit"] for u in manifest["units"] if f["state"] in u.get("states", [])]
        files[f"{rel}/{f['path']}"] = {
            "sha256": f["sha256"], "bytes": f["bytes"],
            "built": max((built_by_unit[u] for u in units if u in built_by_unit), default=latest)}
    for key in ("codes", "projects"):
        x = manifest[key]
        files[f"{rel}/{x['file']}"] = {"sha256": x["sha256"], "bytes": x["bytes"], "built": latest}
    return files


def release_stamp(rel: str, manifest: dict) -> dict:
    """The stamp of this release against the published one. With no stamp
    published yet, a file's first date is FWS's: the newest Last-Modified of
    the release's downloads."""
    published = filedates.fetch_json(f"{PUBLIC_DATA}/{STAMP}")
    source = max(http_date(u["last_modified"]) for u in manifest["units"]
                 if u.get("last_modified"))
    latest = max(part["built"] for part in manifest["parts"])
    doc = filedates.stamp_digests(release_files(rel, manifest), "nwi", published, source, latest)
    doc.pop("upload")
    return doc


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None,
          stamp_out: Path | None = None) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    rel = release_path(out_dir)
    manifest = read_manifest(out_dir, rel)
    missing = [u for u in WORKSTATION_UNITS
               if u not in {x["unit"] for x in manifest["units"]}]
    if missing:
        print(f"  note: {missing} not in this release's manifest yet (built on a workstation)")
    for name in ("nwi", "projects"):
        if not (THUMBS / f"{name}.png").is_file():
            sys.exit(f"missing {THUMBS / name}.png: run the thumbnails command")
    con = connect()
    first = source(out_dir, rel, manifest["files"][0]["path"])
    projects_src = source(out_dir, rel, manifest["projects"]["file"])
    nwi = nwi_collection(rel, manifest, footer(first), extent_of(manifest["files"]), updated)
    proj = projects_collection(rel, manifest, footer(projects_src),
                               projects_extent(con, projects_src), updated)
    for col, readme, agents in (
            (nwi, collection_readme(nwi, "## Access\n\n" + access_md(rel, manifest)
                                    + "\n\n## States\n\n" + states_table(manifest)),
             nwi_agents(rel, manifest)),
            (proj, collection_readme(proj, "## Access\n\n```sql\n"
                                     f"SELECT * FROM read_parquet('{PUBLIC_DATA}/{rel}/"
                                     f"{manifest['projects']['file']}') LIMIT 10;\n```"),
             projects_agents(rel, manifest))):
        cdir = dest / col["id"]
        cdir.mkdir(parents=True)
        if col["id"] == "nwi" and manifest.get("pmtiles"):
            write_json(cdir / "styles" / "default.json", wetlands_style(rel, manifest["pmtiles"]))
            asset = col["assets"]["style"]
            asset["file:size"] = (cdir / asset["href"]).stat().st_size
            asset["file:checksum"] = multihash(cdir / asset["href"])
        write_json(cdir / "collection.json", col)
        shutil.copyfile(THUMBS / f"{col['id']}.png", cdir / "thumbnail.png")
        (cdir / "README.md").write_text(readme)
        (cdir / "AGENTS.md").write_text(agents)
    root = build_root([nwi, proj], manifest, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, manifest))
    (dest / "AGENTS.md").write_text(root_agents(rel, manifest))
    doc = release_stamp(rel, manifest)
    filedates.apply(dest, filedates.Dates(PUBLIC_DATA, doc["files"], [doc]))
    if stamp_out is not None:
        stamp_out.parent.mkdir(parents=True, exist_ok=True)
        stamp_out.write_text(json.dumps(doc, indent=1) + "\n")
    return manifest["total_features"]


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="nwi-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        stamp = Path(tmp) / STAMP
        total = build(out_dir, tree, stamp_out=stamp)
        print(f"  rendered 2 collections, {total:,} wetland polygons")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)
        # The stamp after the catalog. If this upload fails, the next
        # catalog stage compares with the previous stamp and gives the same
        # dates: each file's build time comes from the manifest.
        cmd = ["rclone", "copyto", "--ignore-times",
               "--header-upload", "Cache-Control: public, max-age=300",
               "--header-upload", "Content-Type: application/json",
               str(stamp), f"{remote}/{STAMP}"]
        if dry_run:
            print(f"  $ {' '.join(cmd)}")
        else:
            subprocess.run(cmd, check=True)


# ---------- thumbnails ----------

def thumbnails(out_dir: Path) -> None:
    """THUMB_BOX from the THUMB_STATES files: the wetlands coloured like the
    default style, and the projects by imagery year."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    from thumbnails import BACKGROUND, frame

    rel = release_path(out_dir)
    manifest = read_manifest(out_dir, rel)
    con = connect()
    # The frame widens THUMB_BOX to the card's 3:2: read what it shows.
    x0, y0, x1, y1, _ = frame(*THUMB_BOX)
    box = (f"bbox.xmin <= {x1} AND bbox.xmax >= {x0} AND bbox.ymin <= {y1} "
           f"AND bbox.ymax >= {y0}")

    def draw(rows, colour, name: str, edges: str = "none") -> None:
        polys, fills = [], []
        for value, wkb in rows:
            g = shapely.from_wkb(bytes(wkb))
            for part in getattr(g, "geoms", [g]):
                if part.geom_type == "Polygon":
                    polys.append(shapely.get_coordinates(part.exterior))
                    fills.append(colour(value))
        fx0, fy0, fx1, fy1, aspect = frame(*THUMB_BOX)
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.set_facecolor(BACKGROUND)
        fig.patch.set_facecolor(BACKGROUND)
        ax.add_collection(PolyCollection(polys, facecolors=fills, edgecolors=edges,
                                         linewidths=0.8, alpha=0.9))
        ax.set_xlim(fx0, fx1)
        ax.set_ylim(fy0, fy1)
        ax.set_aspect(aspect)
        ax.axis("off")
        THUMBS.mkdir(parents=True, exist_ok=True)
        fig.savefig(THUMBS / f"{name}.png", dpi=100, facecolor=BACKGROUND)
        plt.close(fig)
        print(f"  {len(polys):,} polygons -> {(THUMBS / name).relative_to(REPO)}.png")

    files = [out_dir / rel / f["path"] for f in manifest["files"] if f["state"] in THUMB_STATES]
    srcs = "[" + ", ".join(f"'{f}'" for f in files) + "]"
    rows = con.execute(f"""SELECT WETLAND_TYPE, ST_AsWKB(geometry)
        FROM read_parquet({srcs}, hive_partitioning = false) WHERE {box}""").fetchall()
    draw(rows, lambda t: TYPE_COLOURS.get(t, "#999999"), "nwi")
    proj = out_dir / rel / manifest["projects"]["file"]
    rows = con.execute(f"""SELECT IMAGE_YR, ST_AsWKB(geometry) FROM read_parquet('{proj}')
        WHERE {box} ORDER BY IMAGE_YR""").fetchall()
    years = [y for y, _ in rows if y]
    lo, hi = (min(years), max(years)) if years else (1970, 2020)
    cmap = matplotlib.colormaps["viridis"]
    draw(rows, lambda y: matplotlib.colors.to_hex(cmap((y - lo) / max(1, hi - lo))) if y
         else "#cccccc", "projects", edges="white")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp, required=False):
        sp.add_argument("--out-dir", type=Path, default=None, required=required,
                        help="nwi.py's --out-dir (default: read the published files)")

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
        print(f"wrote {args.dest}/ ({build(args.out_dir, args.dest):,} wetland polygons)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    elif args.cmd == "publish":
        publish(args.out_dir, args.remote, dry_run=args.dry_run)
    elif args.cmd == "thumbnails":
        thumbnails(args.out_dir)


if __name__ == "__main__":
    main()
