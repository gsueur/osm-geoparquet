#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for FEMA's USA Structures.

  structures/catalog/catalog.json             root: USA Structures
  structures/catalog/structures/collection.json  the footprints, one file and one map per state
  structures/catalog/cells/collection.json    their counts and population on H3 cells

Both collections are partitioned like NAD's: a `partition:glob` over the
state files through the anonymous S3 endpoint, and one asset per state for
clients that read assets, titled by state name. States carry their own
vintage (FEMA refreshes them in batches): each asset says which. The
footprints collection registers each state's PMTiles (`rel: pmtiles` with
its layers, web-map-links v1.3.0, the archive as a `visual` asset) and a
default MapLibre style over all of them. Counts, sizes, checksums and the
gate come from latest/_manifest.json, local (--out-dir) or published. Spec:
portolan-spec v0.2.0. Validator: rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/structures_catalog.py build --dest build/structures-catalog [--out-dir out/structures]
  python3 scripts/datasets/structures_catalog.py check build/structures-catalog
  python3 scripts/datasets/structures_catalog.py publish --remote parquetry:parquetry/structures [--out-dir out/structures]
  uv run --project scripts --with matplotlib --with shapely \\
      python scripts/datasets/structures_catalog.py thumbnails [--out-dir out/structures]
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
from datasets.pmtiles import PMTILES_TYPE  # noqa: E402
from datasets.structures import (  # noqa: E402
    CREDIT,
    FOOTPRINT_LAYER,
    PAGE,
    RELEASE,
)

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
    md_link,
    multihash,
    now_rfc3339,
    write_json,
)

DATASET_PREFIX = "structures"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
# The workflow stamps what it uploads (scripts/filedates.py): `updated` is
# when the data last changed, `checked` the last build.
STAMPS = [f"{PUBLIC_DATA}/_files/structures.json"]
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "usa-structures"
TITLE = "USA Structures (FEMA, ORNL): building footprints of the United States"
THUMBS = REPO / "catalog" / "thumbnails" / "structures"

LICENSE_LINK = {
    "rel": "license",
    "href": "https://www.usa.gov/government-copyright",
    "type": "text/html",
    "title": "Works of the US federal government are not subject to copyright in the "
             "United States (17 U.S.C. 105)",
}
VIA_LINK = {"rel": "via", "href": PAGE, "type": "text/html",
            "title": "USA Structures on FEMA's GIS hub, where it is published"}
PROVIDERS = [
    {
        "name": "Oak Ridge National Laboratory (ORNL)",
        "description": "Extracts the footprints from imagery and LiDAR, classifies their "
                       "occupancy and models the population per building.",
        "url": "https://www.ornl.gov/",
        "roles": ["producer"],
    },
    {
        "name": "FEMA Geospatial Response Office",
        "description": "Publishes USA Structures, one zip per state, on FEMA's public bucket "
                       "fema-femadata. No access or use constraints.",
        "url": PAGE,
        "roles": ["licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted each state's geodatabase to GeoParquet, built the H3 cells and "
                       "the maps, and maintains and hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]
SCOPE = ("Structures over 450 square feet. ORNL's population estimates are modelled residents "
         "per residential building (block group household sizes and occupancy), not a census "
         "count, and are missing on part of some states' residential buildings: each state's "
         "share is in its asset.")

# From FEMA's FGDC metadata (each zip's XX_Structures_metadata.xml), shortened.
OCC = "Locations: Building Occupancy Classification, FEMA Data Standard, 2018"
COLUMN_DOCS = {
    "BUILD_ID": "Feature identifier within the state's delivery.",
    "OCC_CLS": f"Top-level occupancy class ({OCC}): Residential, Commercial, Industrial, "
               "Government, Education, Agriculture, Assembly, Utility and Misc, Unclassified.",
    "PRIM_OCC": f"Primary use within the occupancy class ({OCC}), e.g. 'Single Family "
                "Dwelling', 'Multi - Family Dwelling'.",
    "SEC_OCC": "Range of units for Multi Family Dwelling and Temporary Lodging.",
    "PROP_ADDR": "Primary street address of the structure, where known.",
    "PROP_CITY": "City name.",
    "PROP_ST": "State name.",
    "PROP_ZIP": "ZIP code, five digits, as text.",
    "PROP_CNTY": "County name.",
    "OUTBLDG": "Set for a non-primary structure (garage, shed or other accessory building).",
    "HEIGHT": "Height of the structure, metres, from LiDAR or other sources.",
    "SQMETERS": "Footprint area, square metres.",
    "SQFEET": "Footprint area, square feet.",
    "H_ADJ_ELEV": "Highest ground elevation adjacent to the structure, metres.",
    "L_ADJ_ELEV": "Lowest ground elevation adjacent to the structure, metres.",
    "FIPS": "County FIPS code, five digits, as text.",
    "CENSUSCODE": "Census tract GEOID (state, county and tract FIPS), as text: joins the "
                  "TIGER/Line and ACS tracts.",
    "PROD_DATE": "Date the footprint was captured.",
    "SOURCE": "Contractor or agency that created the footprint.",
    "USNG": "US National Grid coordinate.",
    "LONGITUDE": "Longitude of the footprint's centroid, decimal degrees (FEMA's metadata says "
                 "millionths of a degree; the values are degrees).",
    "LATITUDE": "Latitude of the footprint's centroid, decimal degrees.",
    "IMAGE_NAME": "Imagery (Maxar catalog id, NAIP image) or LiDAR the footprint was "
                  "extracted from.",
    "IMAGE_DATE": "Date of that imagery or LiDAR.",
    "VAL_METHOD": "How the footprint was validated as a true positive.",
    "REMARKS": "Comments.",
    "UUID": "Universally unique identifier of the structure across USA Structures, "
            "8-4-4-4-12 hexadecimal (braces removed here).",
    "B_CODE": "Building code standard likely in effect when the structure was built.",
    "POP_CI95_UPPER": "Upper bound (97.5th percentile) of the 95% interval of ORNL's "
                      "residential population estimate.",
    "POP_MEDIAN": "ORNL's median estimate of the people living in the building (residential "
                  "buildings; block group household size and occupancy, county where "
                  "missing). NULL where not estimated.",
    "POP_CI95_LOWER": "Lower bound (2.5th percentile) of the 95% interval of ORNL's "
                      "residential population estimate.",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the GeoParquet "
            "covering. Filter on it to prune row groups.",
    "geometry": "The footprint as native Parquet GEOMETRY (WKB), MultiPolygon, lon/lat "
                "(OGC:CRS84).",
}
CELL_DOCS = {
    "h3": "H3 cell index, as a string.",
    "h3_res": "H3 resolution of the cell (4, 6 or 8).",
    "structures": "Footprints whose centroid is in the cell.",
    "residential": "Of them, OCC_CLS = 'Residential'.",
    "population": "Sum of POP_MEDIAN over them (0 where none is estimated).",
    "geometry": "The cell's outline as native Parquet GEOMETRY (WKB), lon/lat (OGC:CRS84).",
}
# Population classes per H3 resolution (cell areas differ by 49 between
# resolutions 4, 6 and 8), light to dark.
POP_STEPS = {4: [1_000, 10_000, 100_000, 1_000_000], 6: [100, 1_000, 10_000, 100_000],
             8: [10, 100, 1_000, 10_000]}
POP_COLOURS = ["#fff5eb", "#fdd0a2", "#fd8d3c", "#d94801", "#7f2704"]
OCC_COLOURS = {
    "Residential": "#f4a261", "Commercial": "#2a9d8f", "Industrial": "#8d6e63",
    "Government": "#457b9d", "Education": "#e9c46a", "Agriculture": "#90be6d",
    "Assembly": "#9b5de5", "Utility and Misc": "#6c757d",
}
OTHER_COLOUR = "#bdbdbd"


# ---------- measured inputs ----------

def get_json(url: str):
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def read_manifest(out_dir: Path | None) -> dict:
    local = out_dir / RELEASE / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    return get_json(f"{PUBLIC_DATA}/{RELEASE}/_manifest.json")


def source(out_dir: Path | None, file: str) -> str:
    local = out_dir / RELEASE / file if out_dir else None
    return str(local) if local and local.is_file() else f"{PUBLIC_DATA}/{RELEASE}/{file}"


def connect():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    return con


def footer(src: str) -> list[tuple[str, str]]:
    return [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
            connect().execute(f"DESCRIBE SELECT * FROM read_parquet('{src}', "
                              "hive_partitioning = false)").fetchall()]


def extent_of(con, cell_srcs: list[str]) -> list[float]:
    """From the resolution-8 cells of every state, which cover every
    footprint's centroid: an outer bound padded by about a kilometre, read in
    seconds. Guam and the Northern Marianas lie west of the antimeridian:
    the box then crosses it, written west > east from the measured gaps."""
    from extent import lon_intervals, wrapped_bbox
    files = ", ".join(f"'{s}'" for s in cell_srcs)
    src = (f"(SELECT geometry, struct_pack(xmin := ST_XMin(geometry), ymin := ST_YMin(geometry), "
           f"xmax := ST_XMax(geometry), ymax := ST_YMax(geometry)) AS bbox "
           f"FROM read_parquet([{files}], hive_partitioning = false) WHERE h3_res = 8)")
    y0, y1 = con.execute(f"SELECT min(ST_YMin(geometry)), max(ST_YMax(geometry)) "
                         f"FROM {src}").fetchone()
    b = wrapped_bbox(lon_intervals(con, src), y0, y1)
    return [round(b[0], 6), round(b[1], 6), round(b[2], 6), round(b[3], 6)]


# ---------- styles ----------

def pop_fill(res: int) -> list:
    fill = ["step", ["get", "population"], POP_COLOURS[0]]
    for threshold, colour in zip(POP_STEPS[res], POP_COLOURS[1:]):
        fill += [threshold, colour]
    return fill


def occ_fill() -> list:
    fill = ["match", ["get", "OCC_CLS"]]
    for cls, colour in OCC_COLOURS.items():
        fill += [cls, colour]
    return [*fill, OTHER_COLOUR]


def style(manifest: dict) -> dict:
    """One source per state archive: population per cell at the coarse
    zooms, footprints by occupancy class from the zoom each state's tiles
    start them."""
    sources, layers = {}, []
    for e in manifest["states"]:
        t = e.get("pmtiles")
        if not t:
            continue
        sid = e["state"].lower()
        sources[sid] = {"type": "vector",
                        "url": f"pmtiles://{PUBLIC_DATA}/{RELEASE}/{t['path']}",
                        "attribution": CREDIT}
        for b in t["bands"]:
            if b["layer"] == FOOTPRINT_LAYER:
                layers.append({"id": f"{sid}-structures", "type": "fill", "source": sid,
                               "source-layer": FOOTPRINT_LAYER, "minzoom": b["min_zoom"],
                               "paint": {"fill-color": occ_fill(), "fill-opacity": 0.85}})
            else:
                res = int(b["layer"].removeprefix("cells_r"))
                layers.append({"id": f"{sid}-{b['layer']}", "type": "fill", "source": sid,
                               "source-layer": b["layer"], "minzoom": b["min_zoom"],
                               "maxzoom": b["max_zoom"] + 1,
                               "paint": {"fill-color": pop_fill(res), "fill-opacity": 0.75}})
    return {"version": 8,
            "name": "USA Structures: population per H3 cell, then footprints by occupancy class",
            "sources": sources, "layers": layers}


# ---------- STAC ----------

def asset_s3(path: str) -> dict:
    return {"s3": {"href": f"s3://{BUCKET}/{path}", "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}


def pop_share(e: dict) -> float:
    g = e["gate"]
    return 100 * g["residential_with_population"] / g["residential"] if g["residential"] else 0.0


def state_line(e: dict) -> str:
    return (f"vintage {e['source']['vintage'] or 'undated'} ({e['source']['file']}); "
            f"population estimate on {pop_share(e):.1f}% of residential footprints")


def footprint_assets(manifest: dict) -> dict:
    assets = {}
    for e in manifest["states"]:
        st, path = e["state"], f"{DATASET_PREFIX}/{RELEASE}/{e['path']}"
        assets[f"data-{st.lower()}"] = {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": f"{e['name']} ({st})",
            "description": f"{e['features']:,} footprints in {e['row_groups']} row groups; "
                           f"{state_line(e)}.",
            "roles": ["data"],
            "file:size": e["bytes"],
            "file:checksum": "1220" + e["sha256"],
            "alternate": asset_s3(path),
        }
        t = e.get("pmtiles")
        if t:
            tpath = f"{DATASET_PREFIX}/{RELEASE}/{t['path']}"
            fp = t["footprints"]
            assets[f"tiles-{st.lower()}"] = {
                "href": f"{PUBLIC_BASE}/{tpath}",
                "type": PMTILES_TYPE,
                "title": f"{e['name']} ({st}), vector tiles",
                "description": f"PMTiles, {t['tiles']:,} tiles: population on H3 cells to zoom "
                               f"{fp['min_zoom'] - 1}, footprints (layer `{FOOTPRINT_LAYER}`, "
                               f"properties {', '.join(fp['properties'])}) from zoom "
                               f"{fp['min_zoom']} to {t['max_zoom']}. Built with tylertoo "
                               f"{t['tylertoo']}.",
                "roles": ["visual"],
                "file:size": t["bytes"],
                "file:checksum": "1220" + t["sha256"],
                "alternate": asset_s3(tpath),
            }
    return assets


def cell_assets(manifest: dict) -> dict:
    assets = {}
    for e in manifest["states"]:
        st, c = e["state"], e["cells"]
        path = f"{DATASET_PREFIX}/{RELEASE}/{c['path']}"
        counts = ", ".join(f"{n:,} at resolution {r}" for r, n in sorted(c["cells"].items(),
                                                                         key=lambda kv: int(kv[0])))
        assets[f"data-{st.lower()}"] = {
            "href": f"{PUBLIC_BASE}/{path}", "type": PARQUET_TYPE,
            "title": f"{e['name']} ({st})",
            "description": f"{counts} cells; {state_line(e)}.",
            "roles": ["data"], "file:size": c["bytes"], "file:checksum": "1220" + c["sha256"],
            "alternate": asset_s3(path),
        }
    return assets


def vintages(manifest: dict) -> str:
    by: dict[str, int] = {}
    for e in manifest["states"]:
        v = e["source"]["vintage"] or "undated"
        by[v] = by.get(v, 0) + 1
    return ", ".join(f"{v} ({n} states)" for v, n in sorted(by.items(), reverse=True))


def thumbnail_asset(name: str, title: str) -> dict:
    return {"href": "./thumbnail.png", "type": "image/png", "title": title,
            "roles": ["thumbnail"], "file:size": (THUMBS / f"{name}.png").stat().st_size,
            "file:checksum": multihash(THUMBS / f"{name}.png")}


def common_links(name: str, label: str) -> list[dict]:
    return [
        {"rel": "root", "href": "../catalog.json", "type": "application/json"},
        {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
        md_link("describedby", "./README.md", f"{label}: README"),
        md_link("agents", "./AGENTS.md", f"{label}: agent guide"),
        LICENSE_LINK, VIA_LINK,
        {"rel": "related", "href": f"{PUBLIC_DATA}/{RELEASE}/ATTRIBUTION.txt",
         "type": "text/plain", "title": "Attribution and what was changed"},
        {"rel": "alternate", "href": f"{PUBLIC_DATA}/{RELEASE}/", "type": "text/html",
         "title": "Browse the files"},
    ]


def structures_collection(manifest: dict, columns, extent, updated: str) -> dict:
    undocumented = [c for c, _ in columns if c not in COLUMN_DOCS]
    if undocumented:
        sys.exit(f"structures: no column doc for {undocumented}")
    tiled = [e for e in manifest["states"] if e.get("pmtiles")]
    links = common_links("structures", "USA Structures footprints")
    links += [{"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{DATASET_PREFIX}/{RELEASE}/"
                                         f"{e['pmtiles']['path']}",
               "type": PMTILES_TYPE, "title": f"{e['name']} ({e['state']}), vector tiles",
               "pmtiles:layers": [b["layer"] for b in e["pmtiles"]["bands"]]} for e in tiled]
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT, ALTERNATE_EXT,
                            WEB_MAP_LINKS_EXT],
        "id": "structures",
        "title": "USA Structures: building footprints, one file per state",
        "description": (
            f"{manifest['total_features']:,} building footprints of {len(manifest['states'])} "
            "states and territories from USA Structures (ORNL and FEMA), each with its "
            "occupancy class and use, height, area, address where known, census tract and "
            "ORNL's estimate of the people living in it: one GeoParquet file per state "
            "(postal code), FEMA's columns unchanged, lon/lat, and a PMTiles map per state. "
            f"States are refreshed one batch at a time; vintages: {vintages(manifest)}. Read "
            f"every state at once with the partition glob {manifest['glob']} through the "
            f"anonymous S3 endpoint {S3_ENDPOINT} (path-style, empty credentials), or one "
            f"state over HTTPS. {SCOPE} Credit: {CREDIT}."),
        "keywords": ["USA Structures", "building footprints", "buildings", "occupancy",
                     "population", "exposure", "FEMA", "ORNL", "United States", "GeoParquet"],
        "license": "other",
        "attribution": CREDIT,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [extent]}, "temporal": {"interval": [[None, None]]}},
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [{"name": "state", "type": "string",
                            "description": "USPS postal code of the state, DC or territory. "
                                           "One file per state."}],
        "partition:file_count": len(manifest["states"]),
        "partition:glob": manifest["glob"],
        "table:row_count": manifest["total_features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": COLUMN_DOCS[n]} for n, t in columns],
        "assets": {
            **footprint_assets(manifest),
            "style": {"href": "./styles/default.json", "type": STYLE_TYPE,
                      "title": "Population per H3 cell, then footprints by occupancy class",
                      "roles": ["style", "default"]},
            "thumbnail": thumbnail_asset("structures", "ORNL population per H3 cell "
                                                       "(resolution 4)"),
        },
        "links": links,
    }


def cells_collection(manifest: dict, columns, extent, updated: str) -> dict:
    total = sum(sum(e["cells"]["cells"].values()) for e in manifest["states"])
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT, ALTERNATE_EXT],
        "id": "cells",
        "title": "USA Structures: footprints and population on H3 cells, one file per state",
        "description": (
            "The footprints counted on H3 cells (by centroid) at resolutions 4, 6 and 8, with "
            "the residential count and the sum of ORNL's median population estimate, per "
            "state: the quick way to people and buildings per area without reading the "
            f"footprints. Glob: {manifest['cells_glob']}. A cell on a state line appears in "
            f"both states' files: sum them. {SCOPE} Credit: {CREDIT}."),
        "keywords": ["USA Structures", "H3", "population", "building density"],
        "license": "other",
        "attribution": CREDIT,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [extent]}, "temporal": {"interval": [[None, None]]}},
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [{"name": "state", "type": "string",
                            "description": "USPS postal code of the state, DC or territory."}],
        "partition:file_count": len(manifest["states"]),
        "partition:glob": manifest["cells_glob"],
        "table:row_count": total,
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": CELL_DOCS[n]} for n, t in columns],
        "assets": {
            **cell_assets(manifest),
            "thumbnail": thumbnail_asset("cells", "ORNL population per H3 cell (resolution 4)"),
        },
        "links": common_links("cells", "USA Structures cells"),
    }


def build_root(collections: list[dict], manifest: dict, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (
            f"FEMA's USA Structures (produced by ORNL): {manifest['total_features']:,} building "
            "footprints as GeoParquet, one file per state, FEMA's columns unchanged, with "
            "occupancy class and ORNL's population estimate per building; their counts and "
            "population on H3 cells; a map per state. Public domain. "
            f"{SCOPE} Credit: {CREDIT}."),
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", "USA Structures: README"),
            md_link("agents", "./AGENTS.md", "USA Structures: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK, VIA_LINK,
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


# ---------- markdown ----------

def states_table(manifest: dict) -> str:
    rows = "\n".join(
        f"| {e['state']} | {e['name']} | {e['features']:,} | {e['source']['vintage'] or ''} | "
        f"{pop_share(e):.1f}% | {e['gate']['population_median'] or 0:,} |"
        for e in manifest["states"])
    return f"""\
`Population` is the sum of ORNL's median estimate over the state's buildings;
`Estimated` the share of residential footprints that carry one.

| State | Name | Footprints | Vintage | Estimated | Population |
|---|---|---|---|---|---|
{rows}"""


def access_md(manifest: dict) -> str:
    url = f"{PUBLIC_DATA}/{RELEASE}/state=MA/structures.parquet"
    return f"""\
One state over HTTPS (DuckDB, GDAL, polars, pyarrow read it in place):

```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
-- Buildings and people around Boston Common, by occupancy class.
SELECT OCC_CLS, count(*) AS buildings, sum(POP_MEDIAN) AS people
FROM read_parquet('{url}')
WHERE bbox.xmin <= -71.06 AND bbox.xmax >= -71.075
  AND bbox.ymin <= 42.36 AND bbox.ymax >= 42.35
GROUP BY 1 ORDER BY 2 DESC;
```

Every state at once, through the anonymous S3 endpoint `{S3_ENDPOINT}`:

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (TYPE s3, ENDPOINT '{S3_ENDPOINT}', URL_STYLE 'path', KEY_ID '', SECRET '');
SELECT state, count(*) FROM read_parquet('{manifest['glob']}') GROUP BY 1 ORDER BY 2 DESC;
```"""


def collection_readme(col: dict, body: str) -> str:
    return f"""\
# {col['title']}

{col['description']}

{body}

## License

Public domain: a work of the US federal government, not subject to copyright in
the United States (17 U.S.C. 105); FEMA's metadata states no access or use
constraints. Credit "{CREDIT}".

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def structures_agents(manifest: dict) -> str:
    low = [f"{e['state']} ({pop_share(e):.0f}%)" for e in manifest["states"] if pop_share(e) < 90]
    return f"""\
# structures: agent guide

US building footprints from USA Structures (ORNL, FEMA): one GeoParquet file per
state at `{PUBLIC_DATA}/{RELEASE}/state=<XX>/structures.parquet` (postal code),
lon/lat (OGC:CRS84), sorted on a Hilbert curve with a `bbox` covering.

## Access

{access_md(manifest)}

## Query tips

- Filter on `bbox` (FLOAT bounds) before any spatial predicate.
- People: `sum(POP_MEDIAN)`. These are ORNL's modelled residents per building,
  not a census count. Summed per-building bounds (`POP_CI95_LOWER`,
  `POP_CI95_UPPER`) give a loose range, wider than the total's own interval.
- States where under 90% of residential footprints carry an estimate:
  {', '.join(low) or 'none'}. There the sum undercounts: use the ACS tracts
  (`CENSUSCODE` joins them) for people instead.
- Occupancy: `OCC_CLS` (Residential, Commercial, ...) and `PRIM_OCC` ('Single
  Family Dwelling', 'Multi - Family Dwelling', 'Nursing Home', ...). `OUTBLDG`
  marks sheds and garages.
- Exposure: join footprints to a hazard polygon (an NFHL flood zone, a fire
  perimeter, a PAD-US unit) with `ST_Intersects` after the `bbox` filter.
- States are of different vintages (each asset says which): compare states
  with care.
- For counts per area, the `cells` collection is much cheaper than the footprints.
- Credit: {CREDIT}.
"""


def cells_agents(manifest: dict) -> str:
    return f"""\
# cells: agent guide

Footprints of USA Structures counted on H3 cells per state:
`{PUBLIC_DATA}/{RELEASE}/state=<XX>/cells.parquet`, one row per cell and
resolution (`h3_res` 4, 6 or 8), with `structures`, `residential`, `population`
(sum of ORNL's POP_MEDIAN) and the cell outline.

```sql
INSTALL httpfs; LOAD httpfs;
SELECT sum(structures), sum(population)
FROM read_parquet('{PUBLIC_DATA}/{RELEASE}/state=MA/cells.parquet') WHERE h3_res = 4;
```

A cell on a state line is in both states' files: sum the rows of a cell across
states. Credit: {CREDIT}.
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


def root_agents() -> str:
    return f"""\
# {CATALOG_ID}: agent guide

- `structures`: the footprints, one file and one map per state. structures/AGENTS.md
- `cells`: counts and population on H3 cells, one file per state. cells/AGENTS.md

Files at `{PUBLIC_DATA}/{RELEASE}/`; `_manifest.json` there gives each state's
vintage and source zip. Public domain; credit "{CREDIT}".
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    manifest = read_manifest(out_dir)
    for name in ("structures", "cells"):
        if not (THUMBS / f"{name}.png").is_file():
            sys.exit(f"missing {THUMBS / name}.png: run the thumbnails command")
    con = connect()
    cell_srcs = [source(out_dir, e["cells"]["path"]) for e in manifest["states"]]
    extent = extent_of(con, cell_srcs)
    fp = structures_collection(manifest, footer(source(out_dir, manifest["states"][0]["path"])),
                               extent, updated)
    cells = cells_collection(manifest, footer(cell_srcs[0]), extent, updated)
    for col, readme, agents in (
            (fp, collection_readme(fp, "## Access\n\n" + access_md(manifest) + "\n\n## States\n\n"
                                   + states_table(manifest)), structures_agents(manifest)),
            (cells, collection_readme(cells, "## Access\n\n```sql\nSELECT * FROM read_parquet('"
                                      f"{PUBLIC_DATA}/{RELEASE}/state=MA/cells.parquet') "
                                      "WHERE h3_res = 4;\n```"), cells_agents(manifest))):
        cdir = dest / col["id"]
        cdir.mkdir(parents=True)
        if col["id"] == "structures":
            write_json(cdir / "styles" / "default.json", style(manifest))
            asset = col["assets"]["style"]
            asset["file:size"] = (cdir / asset["href"]).stat().st_size
            asset["file:checksum"] = multihash(cdir / asset["href"])
        write_json(cdir / "collection.json", col)
        shutil.copyfile(THUMBS / f"{col['id']}.png", cdir / "thumbnail.png")
        (cdir / "README.md").write_text(readme)
        (cdir / "AGENTS.md").write_text(agents)
    root = build_root([fp, cells], manifest, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, manifest))
    (dest / "AGENTS.md").write_text(root_agents())
    filedates.apply(dest, filedates.load(PUBLIC_DATA, *STAMPS))
    for doc in ("README.md", "AGENTS.md"):
        path = dest / doc
        path.write_text(f"{path.read_text().rstrip()}\n\n{filedates.DATES_MD}\n")
    return manifest["total_features"]


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="structures-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        total = build(out_dir, tree)
        print(f"  rendered 2 collections, {total:,} footprints")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

CONUS = (-125.0, 24.0, -66.5, 49.5)


def thumbnails(out_dir: Path | None) -> None:
    """Population per resolution-4 cell over the conterminous US, coloured
    like the default style: the same picture for both collections. Reads
    the published cells unless --out-dir has them."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    from thumbnails import BACKGROUND, frame

    manifest = read_manifest(out_dir)
    files = ", ".join(f"'{source(out_dir, e['cells']['path'])}'" for e in manifest["states"])
    con = connect()
    rows = con.execute(f"""
        SELECT sum(population), any_value(ST_AsWKB(geometry))
        FROM read_parquet([{files}], hive_partitioning = false)
        WHERE h3_res = 4 GROUP BY h3""").fetchall()
    steps = POP_STEPS[4]
    polys, fills = [], []
    for n, wkb in rows:
        part = shapely.from_wkb(bytes(wkb))
        if part.geom_type != "Polygon":
            continue
        polys.append(shapely.get_coordinates(part.exterior))
        fills.append(POP_COLOURS[sum(n >= t for t in steps)])
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
    for name in ("structures", "cells"):
        fig.savefig(THUMBS / f"{name}.png", dpi=100, facecolor=BACKGROUND)
    plt.close(fig)
    print(f"  {len(polys):,} cells from {len(manifest['states'])} states -> "
          f"{THUMBS.relative_to(REPO)}/structures.png, cells.png")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp):
        sp.add_argument("--out-dir", type=Path, default=None,
                        help="structures.py's --out-dir (default: read the published files)")

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
    out_dir_arg(t)
    args = p.parse_args()
    if args.cmd == "build":
        print(f"wrote {args.dest}/ ({build(args.out_dir, args.dest):,} footprints)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    elif args.cmd == "publish":
        publish(args.out_dir, args.remote, dry_run=args.dry_run)
    elif args.cmd == "thumbnails":
        thumbnails(args.out_dir)


if __name__ == "__main__":
    main()
