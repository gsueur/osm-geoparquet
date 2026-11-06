#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for FEMA's National Risk Index.

  nri/catalog/catalog.json                  root: FEMA National Risk Index
  nri/catalog/<level>/collection.json       tracts, counties, states, tribal_counties

nri.py's LEVELS drives it. Each collection has the level's GeoParquet
(`data`), FEMA's hazard table and field dictionary as assets, a thumbnail
and, for the tracts and counties, the PMTiles registered as formats.md asks
(a `rel: pmtiles` link with `pmtiles:layers`, web-map-links v1.3.0, the
file as a `visual` asset) with a MapLibre style on the composite rating.
The column descriptions are FEMA's field aliases from the dictionary shipped
with the data, with what the metric means. Counts, sizes, checksums and the
gate come from the release's _manifest.json, local (--out-dir) or
published. Spec: portolan-spec v0.2.0. Validator: rashid, through
scripts/catalog.py.

Usage:
  python3 scripts/datasets/nri_catalog.py build --dest build/nri-catalog [--out-dir out/nri]
  python3 scripts/datasets/nri_catalog.py check build/nri-catalog
  python3 scripts/datasets/nri_catalog.py publish --remote parquetry:parquetry/nri [--out-dir out/nri]
  uv run --project scripts --with matplotlib --with shapely \\
      python scripts/datasets/nri_catalog.py thumbnails --out-dir out/nri
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
from datasets.nri import (
    DOCS, LEVELS, NON_RATINGS, RATINGS, RELEASED, SOURCE_DIR, SOURCE_PAGE, VERSION,
)
from datasets.pmtiles import PMTILES_TYPE

import catalog as shared  # scripts/catalog.py
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

DATASET_PREFIX = "nri"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
RELEASE = f"v{VERSION}"
RELEASE_URL = f"{PUBLIC_DATA}/{RELEASE}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "fema-nri"
TITLE = "FEMA National Risk Index"
THUMBS = REPO / "catalog" / "thumbnails" / "nri"
ATTRIBUTION = f"FEMA, National Risk Index, version {VERSION}"
CSV_TYPE = "text/csv"

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
    "title": "National Risk Index data on OpenFEMA, where it is published",
}
PROVIDERS = [
    {
        "name": "Federal Emergency Management Agency (FEMA)",
        "description": "Produces the National Risk Index and publishes it on OpenFEMA. A US "
                       "government work, not subject to copyright in the United States.",
        "url": "https://www.fema.gov/flood-maps/products-tools/national-risk-index",
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted the geodatabases to GeoParquet, tiled them, and maintains "
                       "and hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]
SCOPE = (
    "Scores are national percentiles among communities of the same level and ratings "
    "five classes from that comparison: compare tracts with tracts and counties with "
    "counties. The NRI is a screening tool, not a property-level assessment.")

RATING_CLASSES = ", ".join(RATINGS)
# What each FEMA metric type means, appended to the field alias.
METRIC_NOTES = {
    "Score": "National percentile among communities of the same level, 0 to 100.",
    "Expected Annual Loss Score": "National percentile among communities of the same level, "
                                  "0 to 100.",
    "Hazard Type Risk Index Score": "National percentile among communities of the same level, "
                                    "0 to 100.",
    "Rating": f"{RATING_CLASSES}; or {', '.join(NON_RATINGS[:2])}.",
    "Expected Annual Loss Rating": f"{RATING_CLASSES}; or {', '.join(NON_RATINGS[:2])}.",
    "Hazard Type Risk Index Rating": f"{RATING_CLASSES}; or {', '.join(NON_RATINGS[:2])}.",
    "Historic Loss Ratio - Total Rating": f"{RATING_CLASSES}; or {', '.join(NON_RATINGS[:2])}.",
    "State Percentile": "Percentile within the state, 0 to 100.",
    "National Percentile": "National percentile, 0 to 100.",
    "Hazard Type Risk Index Value": "US dollars a year, adjusted for social vulnerability and "
                                    "community resilience.",
    "Expected Annual Loss - Total": "US dollars a year.",
    "Expected Annual Loss - Building Value": "US dollars a year.",
    "Expected Annual Loss - Agriculture Value": "US dollars a year.",
    "Expected Annual Loss - Population Equivalence": "US dollars a year (the population loss "
                                                     "at the value of a statistical life).",
    "Expected Annual Loss - Population": "People a year: fatalities, an injury counted as a "
                                         "tenth of one.",
    "Exposure - Total": "US dollars.",
    "Exposure - Building Value": "US dollars.",
    "Exposure - Agriculture Value": "US dollars.",
    "Exposure - Population Equivalence": "US dollars (the population at the value of a "
                                         "statistical life).",
    "Exposure - Population": "People.",
    "Annualized Frequency": "Expected occurrences a year (events or event-days; the technical "
                            "documentation says which).",
    "Number of Events": "Occurrences recorded in the period of record (hazards.parquet).",
    "Historic Loss Ratio - Buildings": "Share of the exposed value lost per occurrence.",
    "Historic Loss Ratio - Population": "Share of the exposed value lost per occurrence.",
    "Historic Loss Ratio - Agriculture": "Share of the exposed value lost per occurrence.",
    "Expected Annual Loss Rate - Building": "Share of the exposed value lost a year.",
    "Expected Annual Loss Rate - Population": "Share of the exposed value lost a year.",
    "Expected Annual Loss Rate - Agriculture": "Share of the exposed value lost a year.",
}
EXTRA_DOCS = {
    "geometry": "Area outline as native Parquet GEOMETRY (WKB), lon/lat (OGC:CRS84), "
                "reprojected from FEMA's Web Mercator.",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the GeoParquet "
            "covering. Filter on it to prune row groups.",
}


def column_doc(name: str, fields: dict) -> str | None:
    if name in EXTRA_DOCS:
        return EXTRA_DOCS[name]
    f = fields.get(name)
    if not f:
        return None
    note = METRIC_NOTES.get(f["metric"])
    if f["metric"] == "Value":
        note = ("US dollars a year." if "Expected Annual Loss" in f["alias"]
                or f["alias"].startswith("National Risk Index") else
                "Index value from the source data set.")
    return f"{f['alias']}." + (f" {note}" if note else "")


# The ratings, low to high, from blue to red; the non-ratings grey.
PALETTE = {"Very Low": "#4575b4", "Relatively Low": "#91bfdb",
           "Relatively Moderate": "#fee090", "Relatively High": "#fc8d59",
           "Very High": "#d73027"}
NO_RATING = "#c9c8c2"
# The column each level's map is coloured on: the states have no composite.
RATED = {"tracts": "RISK_RATNG", "counties": "RISK_RATNG", "states": "EAL_RATNG",
         "tribal_counties": "RISK_RATNG"}


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
    """Measured on the data (scripts/extent.py): from Guam (144.6 E) across
    the antimeridian to the Virgin Islands (64.6 W), written with west > east."""
    con = connect()
    src = f"read_parquet('{source}')"
    y0, y1 = con.execute(f"SELECT min(bbox.ymin), max(bbox.ymax) FROM {src}").fetchone()
    b = wrapped_bbox(lon_intervals(con, src), y0, y1)
    return [round(b[0], 6), max(-90.0, round(b[1], 6)), round(b[2], 6), min(90.0, round(b[3], 6))]


def footer(source: str) -> list[tuple[str, str]]:
    return [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
            connect().execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()]


# ---------- styles ----------

def rating_fill(column: str) -> list:
    fill = ["match", ["get", column]]
    for rating, colour in PALETTE.items():
        fill += [rating, colour]
    return [*fill, NO_RATING]


def style(name: str, tiles: dict) -> dict:
    column = RATED[name]
    return {
        "version": 8, "name": f"{name}: {column}, Very Low to Very High",
        "sources": {"data": {"type": "vector", "url": f"pmtiles://{RELEASE_URL}/{tiles['file']}",
                             "attribution": ATTRIBUTION}},
        "layers": [
            {"id": f"{name}-fill", "type": "fill", "source": "data", "source-layer": name,
             "paint": {"fill-color": rating_fill(column), "fill-opacity": 0.75}},
            {"id": f"{name}-outline", "type": "line", "source": "data", "source-layer": name,
             "paint": {"line-color": "#ffffff",
                       "line-width": ["interpolate", ["linear"], ["zoom"], 4, 0.2, 10, 1]}},
        ],
    }


# ---------- STAC ----------

def asset_s3(path: str) -> dict:
    return {"s3": {"href": f"s3://{BUCKET}/{path}", "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}


def gate_text(name: str, gate: dict) -> str:
    text = f"Checked against FEMA's table of the same release: {gate['table_rows']:,} rows"
    if gate.get("key"):
        text += f", every {gate['key']} unique and matching"
    tiger = gate.get("tiger")
    if tiger:
        text += (f". Against TIGER/Line 2026: {tiger['nri_only']} NRI keys not in TIGER, "
                 f"{tiger['tiger_only']} TIGER areas not in the NRI")
        if "tiger_only_water_tracts" in tiger:
            text += f" ({tiger['tiger_only_water_tracts']} of them water-only tracts)"
    if gate.get("repeated_tribe_county_pairs") is not None:
        text += (f"; no unique key (NRI_ID names the county), "
                 f"{gate['repeated_tribe_county_pairs']} repeated (AIANNHCE, STCOFIPS) pairs")
    return text + "."


def build_collection(name: str, entry: dict, tiles: dict | None, extras: dict, extent: list[float],
                     columns: list[tuple[str, str]], fields: dict, dictionary: dict,
                     updated: str) -> dict:
    spec = LEVELS[name]
    path = f"{DATASET_PREFIX}/{RELEASE}/{entry['file']}"
    undocumented = [c for c, _ in columns if column_doc(c, fields) is None]
    if undocumented:
        sys.exit(f"{name}: no column doc for {undocumented}")
    assets = {
        "data": {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": f"NRI {VERSION}, {spec['title'].lower()}",
            "description": f"{entry['features']:,} rows in {entry['row_groups']} row groups of at "
                           f"most 4 MiB of geometry.",
            "roles": ["data"],
            "file:size": entry["bytes"],
            "file:checksum": "1220" + entry["sha256"],
            "alternate": asset_s3(path),
        },
        "hazards": {
            "href": f"{RELEASE_URL}/{extras['hazards']['file']}",
            "type": PARQUET_TYPE,
            "title": "The 18 hazards: column prefix, period of record, consequence types",
            "roles": ["metadata"],
            "file:size": extras["hazards"]["bytes"],
            "file:checksum": "1220" + extras["hazards"]["sha256"],
        },
        "dictionary": {
            "href": f"{RELEASE_URL}/{dictionary['file']}",
            "type": CSV_TYPE,
            "title": "FEMA's field dictionary, as shipped with the data",
            "roles": ["metadata"],
            "file:size": dictionary["bytes"],
            "file:checksum": "1220" + dictionary["sha256"],
        },
        "thumbnail": {
            "href": "./thumbnail.png",
            "type": "image/png",
            "title": f"{spec['title']}, {RATED[name]}",
            "roles": ["thumbnail"],
            "file:size": (THUMBS / f"{name}.png").stat().st_size,
            "file:checksum": multihash(THUMBS / f"{name}.png"),
        },
    }
    links = [
        {"rel": "root", "href": "../catalog.json", "type": "application/json"},
        {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
        md_link("describedby", "./README.md", f"{spec['title']}: README"),
        md_link("agents", "./AGENTS.md", f"{spec['title']}: agent guide"),
        LICENSE_LINK,
        VIA_LINK,
        {"rel": "describedby", "href": DOCS["technical"], "type": "application/pdf",
         "title": "National Risk Index technical documentation"},
        {"rel": "related", "href": f"{RELEASE_URL}/ATTRIBUTION.txt", "type": "text/plain",
         "title": "Attribution and what was changed"},
        {"rel": "alternate", "href": f"{RELEASE_URL}/", "type": "text/html",
         "title": "Browse the files"},
    ]
    extensions = [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT]
    if tiles:
        tiles_path = f"{DATASET_PREFIX}/{RELEASE}/{tiles['file']}"
        assets["pmtiles"] = {
            "href": f"{PUBLIC_BASE}/{tiles_path}",
            "type": PMTILES_TYPE,
            "title": f"{spec['title']}, vector tiles",
            "description": f"PMTiles, layer `{tiles['layer']}`, zoom {tiles['min_zoom']} to "
                           f"{tiles['max_zoom']}, {tiles['tiles']:,} tiles; properties "
                           f"{', '.join(tiles['properties'])}. Built with tylertoo "
                           f"{tiles['tylertoo']}.",
            "roles": ["visual"],
            "file:size": tiles["bytes"],
            "file:checksum": "1220" + tiles["sha256"],
            "alternate": asset_s3(tiles_path),
        }
        assets["style"] = {"href": "./styles/default.json", "type": STYLE_TYPE,
                           "title": f"{RATED[name]}, Very Low (blue) to Very High (red)",
                           "roles": ["style", "default"]}
        links.append({"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}",
                      "type": PMTILES_TYPE, "title": f"{spec['title']}, vector tiles",
                      "pmtiles:layers": [tiles["layer"]]})
        extensions.append(WEB_MAP_LINKS_EXT)
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": extensions,
        "id": name,
        "title": f"National Risk Index {VERSION}: {spec['title'].lower()}",
        "description": (
            f"FEMA's National Risk Index, version {VERSION} ({RELEASED}), for "
            f"{spec['title'].lower()}: {entry['features']:,} rows"
            + (f" keyed by {spec['key']}" if spec["key"] else "")
            + ". The composite risk, expected annual loss, social vulnerability and community "
            "resilience, and each of the 18 hazards' values, scores and ratings, columns as "
            "FEMA names them (descriptions from its field dictionary). Lon/lat geometry with a "
            "bbox covering, Hilbert-sorted, as one GeoParquet 2.0 file read in place over HTTPS "
            f"or the anonymous S3 endpoint {S3_ENDPOINT}"
            + (", and a PMTiles archive for web maps" if tiles else "")
            + f". {gate_text(name, entry['gate'])} {SCOPE} Credit: {ATTRIBUTION}."
        ),
        "keywords": ["National Risk Index", "FEMA", "natural hazards", "risk", "expected annual "
                     "loss", "social vulnerability", "community resilience", name, "GeoParquet"],
        "license": "other",
        "attribution": ATTRIBUTION,
        "version": VERSION,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {
            "spatial": {"bbox": [extent]},
            "temporal": {"interval": [["2025-12-01T00:00:00Z", "2025-12-31T23:59:59Z"]]},
        },
        "table:row_count": entry["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": column_doc(n, fields)}
                          for n, t in columns],
        "assets": assets,
        "links": links,
    }


def build_root(collections: list[dict], manifest: dict, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (
            f"FEMA's National Risk Index, version {VERSION} ({RELEASED}), from OpenFEMA: the "
            "natural hazard risk of US census tracts, counties, states and tribal areas, as "
            "GeoParquet and PMTiles, one collection per level. Columns and values as FEMA "
            "publishes them, geometry reprojected to lon/lat. Keys join with the US Census "
            f"TIGER/Line layers on parquetry. Public domain in the United States. {SCOPE} "
            f"Credit: {ATTRIBUTION}."),
        "version": VERSION,
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
            {"rel": "describedby", "href": DOCS["technical"], "type": "application/pdf",
             "title": "National Risk Index technical documentation"},
            {"rel": "related", "href": f"{RELEASE_URL}/ATTRIBUTION.txt", "type": "text/plain",
             "title": "Attribution and what was changed"},
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


# ---------- markdown ----------

LICENSE_MD = f"""\
The National Risk Index is a work of the US federal government, not subject to
copyright in the United States (17 U.S.C. 105). Credit "{ATTRIBUTION}"."""

# A point in a tract with a Very High wildfire rating: Paradise, California.
EXAMPLE = (-121.6219, 39.7596)


def access_md(name: str) -> str:
    x, y = EXAMPLE
    url = f"{RELEASE_URL}/{name}.parquet"
    key = LEVELS[name]["key"] or "NRI_ID, AIANNHCE"
    rating = RATED[name]
    wildfire = ", WFIR_RISKR, WFIR_RISKS" if name != "states" else ", WFIR_EALR"
    return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
SELECT {key}, {rating}{wildfire}
FROM read_parquet('{url}')
WHERE bbox.xmin <= {x} AND bbox.xmax >= {x}
  AND bbox.ymin <= {y} AND bbox.ymax >= {y}
  AND ST_Contains(geometry, ST_Point({x}, {y}));
```

The same file at `s3://{BUCKET}/{DATASET_PREFIX}/{RELEASE}/{name}.parquet` on the
anonymous S3 endpoint `{S3_ENDPOINT}`."""


def collection_readme(col: dict, name: str, entry: dict, tiles: dict | None) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    tiles_row = (f"| Map tiles | `{tiles['file']}`, {tiles['bytes'] / 1e6:,.1f} MB, zoom "
                 f"{tiles['min_zoom']} to {tiles['max_zoom']} |\n" if tiles else "")
    tiles_md = (f"""
## Map tiles

`{RELEASE_URL}/{tiles['file']}`, tile layer `{tiles['layer']}`, properties
{', '.join(f"`{p}`" for p in tiles['properties'])}. `styles/default.json` colours
`{RATED[name]}` from Very Low (blue) to Very High (red), the non-ratings grey.
""" if tiles else "")
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, {entry['row_groups']} row groups, sha256 `{entry['sha256'][:12]}...` |
| Rows | {entry['features']:,} |
{tiles_row}| CRS | lon/lat, OGC:CRS84 (FEMA ships Web Mercator) |
| Hazards | `{RELEASE_URL}/hazards.parquet`: prefix (WFIR, HRCN...), hazard, period of record |

## Access

{access_md(name)}
{tiles_md}
## Columns

Each hazard has the same block of columns under its prefix (`WFIR_` wildfire,
`HRCN_` hurricane, `IFLD_` inland flooding, `CFLD_` coastal flooding...;
hazards.parquet lists the 18): events, annualized frequency, exposure,
historic loss ratio, expected annual loss (`_EALT` dollars a year, `_EALS`
score, `_EALR` rating), loss rates and the hazard's risk index (`_RISKV`,
`_RISKS`, `_RISKR`). Fewer columns exist where FEMA computes no loss for a
consequence type (drought has no building loss).

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

`NRI_GDB_{LEVELS[name]['stem']}.zip` from {SOURCE_DIR}/ (OpenFEMA, page
{SOURCE_PAGE}). [nri.py]({REPO_URL}/blob/main/scripts/datasets/nri.py) reads
the geodatabase with DuckDB, keeps every column and value except OBJECTID,
Shape_Length and Shape_Area (Web Mercator measures), reprojects the geometry
from Web Mercator to lon/lat, adds the `bbox` covering, sorts along a Hilbert
curve and cuts row groups at 4 MiB of geometry; rows and keys are checked
against FEMA's table CSV of the same release, and the footer, bbox and groups
are verified before upload.

## License

{LICENSE_MD}
"""


def collection_agents(col: dict, name: str) -> str:
    spec = LEVELS[name]
    key = (f"key `{spec['key']}`" if spec["key"] else
           "no unique key (one row per tribal land and county: `AIANNHCE` and `STCOFIPS`)")
    joins = {
        "tracts": "`TRACTFIPS` = TIGER tract `GEOID`; `STCOFIPS` = county `GEOID`",
        "counties": "`STCOFIPS` = TIGER county `GEOID` (Connecticut by planning region)",
        "states": "`STATEFIPS` = TIGER state `GEOID`",
        "tribal_counties": "`STCOFIPS` = TIGER county `GEOID`; `AIANNHCE` = TIGER AIANNH code",
    }[name]
    return f"""\
# {col['title']}: agent guide

One row per {spec['title'].lower().rstrip('s')}, {key}, lon/lat (OGC:CRS84),
geometry column `geometry`. {RATED[name]} is the composite rating to map.

## Access

{access_md(name)}

## Query tips

- Filter on `bbox` before `ST_Contains`; the file is Hilbert-sorted, so a point
  reads one row group.
- Join to census/TIGER: {joins}.
- Hazard columns share a prefix (`WFIR_` wildfire, `HRCN_` hurricane...);
  {RELEASE_URL}/hazards.parquet lists the 18. `_RISKR` is a hazard's rating,
  `_EALT` its expected annual loss in dollars a year.
- Ratings: {RATING_CLASSES}; also {', '.join(NON_RATINGS[:2])}. Scores are
  national percentiles within the level: do not compare a tract score with a
  county score.
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

Files: `{RELEASE_URL}/<level>.parquet`, with `hazards.parquet` (the 18
hazards and their column prefixes) and `NRIDataDictionary.csv` (FEMA's field
dictionary). FEMA's documentation: {DOCS['technical']}.

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {CATALOG_ID}: agent guide

FEMA's National Risk Index {VERSION}, one collection per level.

{listing}

Layout: `{RELEASE_URL}/<level>.parquet`, anonymous S3 at
`s3://{BUCKET}/{DATASET_PREFIX}/{RELEASE}/` on `{S3_ENDPOINT}`. Lon/lat. Hazard
columns share a prefix listed in `hazards.parquet`. Keys join with census/TIGER
(tract and county GEOID). Credit: {ATTRIBUTION}.
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
    for name in LEVELS:
        if not (THUMBS / f"{name}.png").is_file():
            sys.exit(f"missing {THUMBS / name}.png: run the thumbnails command")
        source = data_source(out_dir, files[name]["file"])
        col = build_collection(name, files[name], tiles.get(name), files, layer_extent(source),
                               footer(source), manifest["fields"], manifest["dictionary"],
                               updated)
        cdir = dest / name
        if name in tiles:
            write_json(cdir / "styles" / "default.json", style(name, tiles[name]))
            asset = col["assets"]["style"]
            asset["file:size"] = (cdir / asset["href"]).stat().st_size
            asset["file:checksum"] = multihash(cdir / asset["href"])
        write_json(cdir / "collection.json", col)
        shutil.copyfile(THUMBS / f"{name}.png", cdir / "thumbnail.png")
        (cdir / "README.md").write_text(collection_readme(col, name, files[name], tiles.get(name)))
        (cdir / "AGENTS.md").write_text(collection_agents(col, name))
        collections.append(col)
        total += files[name]["features"]
    root = build_root(collections, manifest, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, collections))
    (dest / "AGENTS.md").write_text(root_agents(collections))
    return total


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="nri-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        total = build(out_dir, tree)
        print(f"  rendered {len(LEVELS)} collections, {total:,} rows")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

CONUS = (-125.0, 24.0, -66.5, 49.5)


def thumbnails(out_dir: Path) -> None:
    """Each level over the conterminous US, coloured on its rating as the
    default style."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    from thumbnails import BACKGROUND, frame

    con = connect()
    THUMBS.mkdir(parents=True, exist_ok=True)
    x0, y0, x1, y1, aspect = frame(*CONUS)
    tolerance = 0.02
    for name in LEVELS:
        path = out_dir / RELEASE / f"{name}.parquet"
        rows = con.execute(f"""
            SELECT {RATED[name]},
                   ST_AsWKB(ST_SimplifyPreserveTopology(geometry, {tolerance}))
            FROM read_parquet('{path}')
            WHERE bbox.xmax > {CONUS[0]} AND bbox.xmin < {CONUS[2]}
              AND bbox.ymax > {CONUS[1]} AND bbox.ymin < {CONUS[3]}""").fetchall()
        polys, fills = [], []
        for rating, wkb in rows:
            for part in shapely.get_parts(shapely.from_wkb(bytes(wkb))):
                if part.geom_type != "Polygon" or part.is_empty:
                    continue
                polys.append(shapely.get_coordinates(part.exterior))
                fills.append(PALETTE.get(rating, NO_RATING))
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.set_facecolor(BACKGROUND)
        fig.patch.set_facecolor(BACKGROUND)
        dense = len(rows) > 20_000
        ax.add_collection(PolyCollection(
            polys, facecolors=fills, edgecolors="none" if dense else "#ffffff",
            linewidths=0 if dense else 0.2, alpha=0.9))
        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
        ax.set_aspect(aspect)
        ax.axis("off")
        fig.savefig(THUMBS / f"{name}.png", dpi=100, facecolor=BACKGROUND)
        plt.close(fig)
        print(f"  {name}: {len(rows):,} areas -> {(THUMBS / f'{name}.png').relative_to(REPO)}")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp, required=False):
        sp.add_argument("--out-dir", type=Path, default=None, required=required,
                        help="nri.py's --out-dir (default: read the published files)")

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
