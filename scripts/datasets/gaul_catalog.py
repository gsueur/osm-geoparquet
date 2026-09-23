#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for FAO GAUL 2024.

One STAC Collection per layer (L1, L2, and the L0 we derive). Each layer is
published in two layouts and the collection describes both: the whole-world
file as the `data` asset, and the per-country files under country=<iso3>/
through the partition extension (`partition:glob`) and one `data-<iso3>`
asset each. The release is static, so every asset carries a checksum. Row counts, sizes and checksums come from the
_manifest.json gaul.py writes; column types and the extent are read from the
Parquet footers, which is a range request when the source is a URL. The
prose lives here. Spec: portolan-spec v0.2.0. Validator: rashid, pinned in
pyproject.toml, shared with the OSM catalog (scripts/catalog.py), as are the
STAC constants, the rashid wrapper and the uploader.

The catalog is published beside the data, at /gaul/catalog/catalog.json, so
the dataset stays one self-contained prefix.

Usage:
  # render from a local gaul.py output dir (footers read locally)
  python3 scripts/datasets/gaul_catalog.py build --out-dir out/2024 --dest build/gaul-catalog
  # or from the published files (footers read over HTTPS)
  python3 scripts/datasets/gaul_catalog.py build --dest build/gaul-catalog
  python3 scripts/datasets/gaul_catalog.py check build/gaul-catalog
  python3 scripts/datasets/gaul_catalog.py publish --out-dir out/2024 --remote parquetry:parquetry/gaul
  # the committed thumbnails, re-rendered only when the data changes
  uv run --project scripts --with matplotlib python scripts/datasets/gaul_catalog.py thumbnails --out-dir out/2024
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.gaul import ACCESS_URL, PARTITION_KEY, SOURCE_ZIPS, VERSION

import catalog as shared  # scripts/catalog.py
from catalog import (
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

DATASET_PREFIX = "gaul"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
DATA_URL = f"{PUBLIC_DATA}/{VERSION}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "gaul-geoparquet"
THUMBS = REPO / "catalog" / "thumbnails" / "gaul"

LICENSE = "CC-BY-4.0"
LICENSE_LINK = {
    "rel": "license",
    "href": "https://creativecommons.org/licenses/by/4.0/",
    "type": "text/html",
    "title": "Creative Commons Attribution 4.0 International (CC BY 4.0)",
}
VIA_LINK = {
    "rel": "via",
    "href": ACCESS_URL,
    "type": "text/html",
    "title": "FAO Map Catalog, where GAUL 2024 is published",
}
GUIDELINES_URL = "https://doi.org/10.4060/cd4262en"

# Mirror semantics, as in the OSM catalog: producer and host differ, the host
# is listed last and once.
PROVIDERS = [
    {
        "name": "Food and Agriculture Organization of the United Nations (FAO)",
        "description": "Develops and owns the Global Administrative Unit Layers, "
                       "published under CC BY 4.0 and the GAUL 2024 Terms of Use.",
        "url": "https://data.apps.fao.org/",
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted the FAO shapefiles to GeoParquet, derived the L0 "
                       "layer, and maintains and hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]

# GAUL 2024 is a vintage: the units as FAO delineated them for that reference
# year. There is no finer timestamp to give.
INTERVAL = [f"{VERSION}-01-01T00:00:00Z", f"{VERSION}-12-31T23:59:59Z"]

# The Terms of Use disclaimers travel with every description (para 8, 9).
NOT_ENDORSED = ("FAO has not participated in, sponsored, approved or endorsed "
                "this redistribution.")
UN_DISCLAIMER = (
    "The designations employed and the presentation of material in GAUL do not "
    "imply the expression of any opinion whatsoever on the part of FAO concerning "
    "the legal status of any country, territory, city or area or of its "
    "authorities, or concerning the delimitation of its frontiers or boundaries. "
    "GAUL is not an authoritative or official representation of subnational "
    "boundaries; its purpose is to support the representation of subnational "
    "statistics.")

# Ordered as a reader meets them: countries, then down.
LAYERS = {
    "L0_derived": (
        "Countries (L0, derived)",
        ("One polygon per country, dissolved by Geomermaids from the L1 units "
        "sharing a gaul0_code. FAO publishes no GAUL 2024 country layer, so this "
        "one is NOT an FAO product: use it as a convenience for joins and maps, "
        "not as an authority on international boundaries. Every row carries a "
        "derived_by column saying so."),
    ),
    "L1": (
        "First-level administrative units (L1)",
        ("FAO's first-level units (states, provinces, regions), geometry and "
        "attributes unchanged from the GAUL 2024 release, with the country codes "
        "and names each unit belongs to."),
    ),
    "L2": (
        "Second-level administrative units (L2)",
        ("FAO's second-level units (districts, departments, counties), geometry "
        "and attributes unchanged from the GAUL 2024 release, with the L1 and "
        "country codes and names each unit belongs to."),
    ),
}

# Collection ids are lower-case (PTL-COL-003); the file stems keep FAO's case.
STEMS = {n.lower(): n for n in LAYERS}


def stem(col: dict) -> str:
    return STEMS[col["id"]]


COLUMN_DOCS = {
    "iso3_code": "ISO 3166-1 alpha-3 code of the country.",
    "map_code": "FAO map code of the country, as used across FAO statistical products.",
    "gaul0_code": "GAUL code of the country (level 0). Stable across GAUL releases; "
                  "the key to join FAO country statistics on.",
    "gaul0_name": "Name of the country, as FAO writes it.",
    "gaul1_code": "GAUL code of the first-level unit.",
    "gaul1_name": "Name of the first-level unit.",
    "gaul2_code": "GAUL code of the second-level unit.",
    "gaul2_name": "Name of the second-level unit.",
    "continent": "Continent the unit is on, as FAO assigns it.",
    "disp_en": "Display name in English, the label FAO uses on maps.",
    "derived_by": "Who derived the row and how. Constant: this layer is a "
                  "Geomermaids dissolve of FAO's L1, not an FAO product.",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax) as float32, rounded "
            "outward, declared as the GeoParquet covering. Filter on it to prune "
            "row groups before touching geometry.",
    "geometry": "Unit outline as native Parquet GEOMETRY (WKB), OGC:CRS84 lon/lat, "
                "MultiPolygon.",
}


# ---------- measured inputs ----------

def data_source(out_dir: Path | None, name: str) -> str:
    """Local file when the build reads a gaul.py output dir, else the
    published URL. Both are Parquet footers to DuckDB."""
    if out_dir is not None:
        return str(out_dir / f"{name}.parquet")
    return f"{DATA_URL}/{name}.parquet"


def read_manifest(out_dir: Path | None) -> dict:
    if out_dir is not None:
        return json.loads((out_dir / "_manifest.json").read_text())
    import urllib.request
    with urllib.request.urlopen(f"{DATA_URL}/_manifest.json") as r:
        return json.load(r)


def footer(con, source: str) -> tuple[list[tuple[str, str]], dict]:
    """(column name/type pairs, geo metadata) from one file's footer."""
    # DuckDB spells the geometry type with its whole PROJJSON CRS inline;
    # the CRS is documented in prose, so the type is kept to its name.
    columns = [(r[0], "GEOMETRY" if r[1].startswith("GEOMETRY") else r[1]) for r in
               con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()]
    row = con.execute(
        f"SELECT value FROM parquet_kv_metadata('{source}') WHERE key = 'geo'").fetchone()
    if row is None:
        sys.exit(f"{source}: no geo footer")
    geo = json.loads(bytes(row[0]).decode())
    if "covering" not in geo["columns"]["geometry"]:
        sys.exit(f"{source}: geo footer declares no bbox covering")
    return columns, geo


def connect(remote: bool):
    con = duckdb.connect()
    if remote:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    return con


# ---------- STAC ----------

def country_assets(name: str, part: dict) -> dict:
    """One asset per country file, keyed data-<iso3>, checksummed."""
    assets = {}
    for e in part["entries"]:
        path = f"{DATASET_PREFIX}/{VERSION}/{PARTITION_KEY}={e['country']}/{name}.parquet"
        assets[f"data-{e['country'].lower()}"] = {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": f"{', '.join(e['names'])} ({e['country']})",
            "description": f"{e['features']:,} features",
            "roles": ["data"],
            "file:size": e["bytes"],
            "file:checksum": "1220" + e["sha256"],
            "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                 "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
        }
    return assets


def build_collection(name: str, entry: dict, part: dict, columns: list[tuple[str, str]],
                     geo: dict, updated: str) -> dict:
    title, description = LAYERS[name]
    glob = f"s3://{BUCKET}/{DATASET_PREFIX}/{VERSION}/{PARTITION_KEY}=*/{name}.parquet"
    # The footer extent is the outward-rounded data extent, which overshoots
    # the antimeridian by a few 1e-6; a STAC bbox must stay within the CRS.
    x0, y0, x1, y1 = geo["columns"]["geometry"]["bbox"]
    bbox = [max(-180.0, round(x0, 6)), max(-90.0, round(y0, 6)),
            min(180.0, round(x1, 6)), min(90.0, round(y1, 6))]
    path = f"{DATASET_PREFIX}/{VERSION}/{name}.parquet"
    thumb = THUMBS / f"{name}.png"
    undocumented = [c for c, _ in columns if c not in COLUMN_DOCS]
    if undocumented:
        sys.exit(f"{name}: no column doc for {undocumented}")
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT,
                            VERSION_EXT, ALTERNATE_EXT],
        "id": name.lower(),
        "title": title,
        "description": (
            f"{description} Published twice from the same rows: one whole-world "
            f"GeoParquet 2.0 file ({entry['features']:,} rows, {entry['bytes'] / 1e6:,.0f} MB, "
            f"the data asset) for anyone who wants a single download, and one file per "
            f"country under {PARTITION_KEY}=<iso3>/ ({part['files']} files, the data-<iso3> "
            f"assets and the partition glob {glob}) for small reads. Both are "
            f"Hilbert-ordered with a bbox covering and readable in place over HTTPS or "
            f"through the anonymous S3 endpoint {S3_ENDPOINT}. Data (c) FAO {VERSION}, "
            f"CC BY 4.0, attribution required. {NOT_ENDORSED} {UN_DISCLAIMER}"
        ),
        "keywords": ["GAUL", "FAO", "administrative boundaries", "administrative units",
                     "GeoParquet", "global", name.split("_")[0]],
        "license": LICENSE,
        "version": VERSION,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {
            "spatial": {"bbox": [bbox]},
            "temporal": {"interval": [INTERVAL]},
        },
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [
            {"name": PARTITION_KEY, "type": "string",
             "description": "The unit's iso3_code: ISO 3166-1 alpha-3, plus FAO's "
                            "pseudo-codes for disputed areas (xAB Abyei, xJK Jammu and "
                            "Kashmir, xxx). A few codes group several GAUL units, e.g. "
                            "AUS also holds Ashmore and Cartier Islands. One file per code."},
        ],
        "partition:file_count": part["files"],
        "partition:glob": glob,
        "table:row_count": entry["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [
            {"name": n, "type": t, "description": COLUMN_DOCS[n]} for n, t in columns
        ],
        "assets": {
            "data": {
                "href": f"{PUBLIC_BASE}/{path}",
                "type": PARQUET_TYPE,
                "title": f"{title}, whole world in one file",
                "description": f"{entry['features']:,} features in {entry['row_groups']} "
                               f"row group(s). Large: a bbox filter cannot skip much, "
                               f"so prefer the per-country files for small reads.",
                "roles": ["data"],
                "file:size": entry["bytes"],
                "file:checksum": "1220" + entry["sha256"],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                     "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            **country_assets(name, part),
            "thumbnail": {
                "href": "./thumbnail.png",
                "type": "image/png",
                "title": "World preview of the layer, flat styling",
                "roles": ["thumbnail"],
                "file:size": thumb.stat().st_size,
                "file:checksum": multihash(thumb),
            },
        },
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{title}: README"),
            md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "alternate", "href": f"{DATA_URL}/", "type": "text/html",
             "title": f"Browse the {VERSION} files"},
        ],
    }


def build_root(collections: list[dict], updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID,
        "title": f"FAO GAUL {VERSION} as GeoParquet",
        "description": (
            f"The Global Administrative Unit Layers (GAUL) {VERSION}, FAO's "
            f"subnational administrative units for every country, repacked from "
            f"ESRI shapefile to GeoParquet 2.0 with a bbox covering: the L1 and L2 "
            f"layers as FAO published them, and a country layer (L0) that "
            f"Geomermaids dissolved from L1 because FAO publishes none. A static "
            f"release; the files are immutable. Data (c) FAO {VERSION}, CC BY 4.0. "
            f"{NOT_ENDORSED} {UN_DISCLAIMER}"
        ),
        "version": VERSION,
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json",
               "type": "application/json", "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", "Catalog README"),
            md_link("agents", "./AGENTS.md", "Catalog agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "related", "href": f"{DATA_URL}/ATTRIBUTION.txt", "type": "text/plain",
             "title": "Attribution, citation and Terms of Use obligations"},
            {"rel": "related", "href": f"{DATA_URL}/GAUL2024TermsOfUse.pdf",
             "type": "application/pdf", "title": f"GAUL {VERSION} Terms of Use (FAO)"},
            {"rel": "related", "href": GUIDELINES_URL, "type": "text/html",
             "title": f"GAUL {VERSION} technical guidelines (FAO, 2025)"},
            {"rel": "vcs", "href": REPO_URL, "type": "text/html",
             "title": "Converter and catalog source"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
            {"rel": "alternate", "href": SITE_URL, "type": "text/html",
             "title": "Project site"},
        ],
    }


# ---------- markdown ----------

def citation(accessed: str) -> str:
    d = dt.date.fromisoformat(accessed)
    return (f"FAO. {VERSION}. Global Administrative Unit Layers (GAUL). "
            f"[Accessed on {d.day} {d.strftime('%B %Y')}]. {ACCESS_URL}. "
            f"Licence: CC-BY-4.0")


def access_section(name: str) -> str:
    return f"""\
Two layouts of the same rows, both immutable and readable in place with
HTTP range requests. One country, a small file:

```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
SELECT gaul0_name, ST_Area(geometry) AS area
FROM read_parquet('{DATA_URL}/{PARTITION_KEY}=FRA/{name}.parquet');
```

The whole world in one download, `{DATA_URL}/{name}.parquet`. Its row groups
are large (DuckDB writes none under 2,048 rows), so a bbox filter reads most
of the file; use it when you want everything, or the per-country files when
you do not. Every country at once, through the anonymous S3 endpoint
`{S3_ENDPOINT}` (path-style, empty credentials):

```sql
SET s3_endpoint='{S3_ENDPOINT}'; SET s3_url_style='path';
SET s3_access_key_id=''; SET s3_secret_access_key='';
SELECT country, count(*) FROM read_parquet(
  's3://{BUCKET}/{DATASET_PREFIX}/{VERSION}/{PARTITION_KEY}=*/{name}.parquet', hive_partitioning = true)
GROUP BY 1;
```"""


def license_md(accessed: str) -> str:
    return f"""\
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), under the
[GAUL {VERSION} Terms of Use]({DATA_URL}/GAUL2024TermsOfUse.pdf), which prevail where
they conflict. Cite it as (Terms of Use, para 7):

> {citation(accessed)}

{NOT_ENDORSED} {UN_DISCLAIMER} GAUL may include third-party data with terms
of its own; anyone redistributing it further is responsible for checking that
(Terms of Use, para 6). Full text: [ATTRIBUTION.txt]({DATA_URL}/ATTRIBUTION.txt)."""


PROVENANCE = f"""\
FAO ships GAUL {VERSION} as ESRI shapefiles in [GAUL_2024_L1.zip]({SOURCE_ZIPS['L1']})
and [GAUL_2024_L2.zip]({SOURCE_ZIPS['L2']}). The [converter]({REPO_URL})
reads each with DuckDB, keeps every FAO attribute column under its FAO name,
adds a float32 `bbox` per row, orders rows on a Hilbert curve, and writes
GeoParquet 2.0 (zstd) with the bbox declared as the `covering`. L0 is
`ST_Union_Agg` of the L1 units per `gaul0_code`; its total area equals L1's.
Each file's footer is verified after writing, and the manifest records size
and sha256, which this catalog publishes as `file:size` and `file:checksum`."""


def collection_readme(col: dict, entry: dict, part: dict, accessed: str) -> str:
    name = stem(col)
    schema = "\n".join(
        f"| `{c['name']}` | `{c['type']}` | {c['description']} |" for c in col["table:columns"])
    return f"""\
# {col['title']}

{LAYERS[name][1]}

| | |
|---|---|
| Rows | {entry['features']:,} |
| Whole-world file | {entry['bytes'] / 1e6:,.0f} MB, {entry['row_groups']} row group(s), sha256 `{entry['sha256'][:12]}...` |
| Per-country files | {part['files']} under `{PARTITION_KEY}=<iso3>/`, {part['bytes'] / 1e6:,.0f} MB in all |
| Extent | {shared.fmt_bbox(col['extent']['spatial']['bbox'][0])} (lon/lat) |
| Vintage | GAUL {VERSION}, accessed {accessed} |
| Source | {entry['source']} |
| License | CC BY 4.0, (c) FAO |

## Access

{access_section(name)}

## Schema

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

{PROVENANCE}

## License

{license_md(accessed)}
"""


def collection_agents(col: dict, entry: dict, part: dict) -> str:
    name = stem(col)
    pruning = ("one row group, so the bbox covering cannot skip anything"
               if entry["row_groups"] == 1 else
               f"{entry['row_groups']} row groups, so a bbox filter skips at best "
               f"the ones outside the window")
    return f"""\
# {col['title']}: agent guide

Each row is one {'country' if name == 'L0_derived' else 'administrative unit'} of
GAUL {VERSION}, MultiPolygon, OGC:CRS84.

## Access

{access_section(name)}

## Query tips

- Pick the layout by the question. One country or a small window: the
  per-country file `{PARTITION_KEY}=<iso3>/{name}.parquet` ({part['files']} files, keyed
  by `iso3_code`, `hive_partitioning = true` adds a `{PARTITION_KEY}` column on a
  glob). Everything: the whole-world file, which has {pruning}.
- Join on the GAUL codes (`gaul0_code`, `gaul1_code`, `gaul2_code`), which
  are what FAO statistics carry. `iso3_code` is ISO 3166-1 alpha-3.
- Features are large (a country or a province): the whole-world file is
  {entry['bytes'] / 1e6:,.0f} MB, so project the columns you need and let the
  `bbox` filter run before any geometry function.
- {'This layer is a Geomermaids dissolve of L1, not an FAO product; say so when you cite it, and prefer L1 for anything where the boundary itself matters.' if name == 'L0_derived' else 'Geometry and attributes are exactly the FAO release.'}
- Attribution is required: cite FAO GAUL {VERSION}, CC BY 4.0, and carry the
  UN disclaimer on frontiers. Do not present FAO as endorsing your use.
"""


def root_readme(root: dict, collections: list[dict], manifest: dict) -> str:
    entries = {e["name"]: e for e in manifest["files"]}
    parts = {p["name"]: p for p in manifest["partitions"]}
    rows = "\n".join(
        f"| [{c['title']}](./{c['id']}/README.md) | `{stem(c)}.parquet` | "
        f"{entries[stem(c)]['features']:,} | {entries[stem(c)]['bytes'] / 1e6:,.0f} MB | "
        f"{parts[stem(c)]['files']} |"
        for c in collections)
    return f"""\
# {root['title']}

{root['description']}

| Collection | Whole-world file | Rows | Size | Per-country files |
|---|---|---|---|---|
{rows}

## Versions

GAUL {VERSION} is a static release: the files under `{DATA_URL}/` are
immutable, and a later GAUL vintage would be published beside it under its
own year, with its own catalog.

## Access

Each layer comes in two layouts of the same rows: one whole-world file at
`{DATA_URL}/<layer>.parquet` for a single download, and one file per country
at `{DATA_URL}/{PARTITION_KEY}=<iso3>/<layer>.parquet` for small reads (the
whole-world files have row groups of 80 to 290 MB, so a bbox filter cannot
skip much there). Both read in place with HTTP range requests, and the same
paths exist under `s3://{BUCKET}/{DATASET_PREFIX}/{VERSION}/` on the anonymous S3 endpoint
`{S3_ENDPOINT}`, where a glob over `{PARTITION_KEY}=*/` reads every country. Each
collection's README shows the queries.

## Provenance

{PROVENANCE}

## License

{license_md(manifest['accessed'])}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {CATALOG_ID}: agent guide

FAO GAUL {VERSION} as GeoParquet 2.0: one collection per level, global
coverage, each in two layouts: one whole-world file, and one file per country
under `{PARTITION_KEY}=<iso3>/`. Static release, immutable files.

## Collections

{listing}

## Access

- One country: `{DATA_URL}/{PARTITION_KEY}=<iso3>/<layer>.parquet` over HTTPS, no
  credentials, a few MB. Prefer it for anything that is not the whole world.
- The whole world: `{DATA_URL}/<layer>.parquet`, 290 to 490 MB with row groups
  too large to prune. Every country at once: the collection's
  `partition:glob` through the S3 endpoint.
- The same path under `s3://{BUCKET}/{DATASET_PREFIX}/{VERSION}/` on the anonymous S3 endpoint
  `{S3_ENDPOINT}`, path-style, empty credentials.
- Each collection documents its columns in `table:columns` and carries
  `file:size` and `file:checksum` on its data asset.

## Conventions

- Geometry is native Parquet GEOMETRY, OGC:CRS84 lon/lat, MultiPolygon.
- `bbox` is a per-row float32 box rounded outward, declared as the
  GeoParquet `covering`; filter on it before touching geometry.
- GAUL codes are the join keys to FAO statistics; `iso3_code` is ISO 3166-1
  alpha-3 plus FAO pseudo-codes for disputed areas (xAB, xJK, xxx), and is
  the partition key: a few codes group several units (AUS, PYF, xFR, xUK).
- `L0_derived` is ours, dissolved from L1; L1 and L2 are FAO's, unchanged.
- Data (c) FAO, CC BY 4.0. Attribution and the UN boundary disclaimer are
  required; FAO does not endorse this redistribution or any use of it.
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> dict:
    """Render the catalog tree into dest (replaced). Returns the manifest."""
    manifest = read_manifest(out_dir)
    entries = {e["name"]: e for e in manifest["files"]}
    parts = {p["name"]: p for p in manifest.get("partitions", [])}
    missing = (set(LAYERS) ^ set(entries)) | (set(LAYERS) ^ set(parts))
    if missing:
        sys.exit(f"LAYERS and the manifest disagree on: {sorted(missing)}")
    if any(p["key"] != PARTITION_KEY for p in parts.values()):
        sys.exit("manifest partition key differs from gaul.py's PARTITION_KEY")
    for name in LAYERS:
        if not (THUMBS / f"{name}.png").is_file():
            sys.exit(f"missing thumbnail {THUMBS / name}.png: run the thumbnails command")
    updated = updated or now_rfc3339()
    con = connect(remote=out_dir is None)

    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)

    collections = []
    for name in LAYERS:
        columns, geo = footer(con, data_source(out_dir, name))
        col = build_collection(name, entries[name], parts[name], columns, geo, updated)
        cdir = dest / col["id"]
        write_json(cdir / "collection.json", col)
        shutil.copyfile(THUMBS / f"{name}.png", cdir / "thumbnail.png")
        (cdir / "README.md").write_text(
            collection_readme(col, entries[name], parts[name], manifest["accessed"]))
        (cdir / "AGENTS.md").write_text(collection_agents(col, entries[name], parts[name]))
        collections.append(col)

    root = build_root(collections, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, collections, manifest))
    (dest / "AGENTS.md").write_text(root_agents(collections))
    return manifest


def check(tree: Path, *, remote_data: bool) -> int:
    """rashid over the tree. With remote_data the data pass range-reads the
    published parquet files too (PTL-DAT-*), which is what the local OSM
    check cannot do through its s3 glob."""
    scope = ("--data-scope", "all") if remote_data else shared.LOCAL_DATA
    return shared.check(tree, *scope)


def publish(out_dir: Path | None, remote: str, *, remote_data: bool, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="gaul-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        manifest = build(out_dir, tree)
        print(f"  rendered {len(LAYERS)} collections, {manifest['total_features']:,} rows")
        if check(tree, remote_data=remote_data):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

COLORS = {"L0_derived": "#4a6fa5", "L1": "#3f8a4a", "L2": "#b5553c"}
WORLD = (-180.0, -60.0, 180.0, 84.0)  # Antarctica cropped: it is one unit and half the height


def thumbnails(out_dir: Path, names: list[str]) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    con = connect(remote=False)
    con.execute("INSTALL spatial; LOAD spatial;")
    x0, y0, x1, y1, aspect = shared_frame(*WORLD)
    tol = (x1 - x0) / 1200
    THUMBS.mkdir(parents=True, exist_ok=True)
    for name in names:
        rows = con.execute(
            f"SELECT ST_AsWKB(ST_SimplifyPreserveTopology(geometry, {tol})) "
            f"FROM read_parquet('{data_source(out_dir, name)}')").fetchall()
        geoms = shapely.from_wkb([bytes(r[0]) for r in rows])
        rings = shapely.get_exterior_ring(shapely.get_parts(geoms))
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.set_facecolor(shared_bg())
        fig.patch.set_facecolor(shared_bg())
        ax.add_collection(PolyCollection(
            [shapely.get_coordinates(r) for r in rings],
            facecolors=COLORS[name], edgecolors="#ffffff", linewidths=0.2, alpha=0.85))
        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
        ax.set_aspect(aspect)
        ax.axis("off")
        path = THUMBS / f"{name}.png"
        fig.savefig(path, dpi=100, facecolor=shared_bg())
        plt.close(fig)
        print(f"  {name:12} {len(geoms):>7,} features -> {path.relative_to(REPO)}")


def shared_frame(*box):
    from thumbnails import frame  # matplotlib-free helper in scripts/thumbnails.py
    return frame(*box)


def shared_bg() -> str:
    from thumbnails import BACKGROUND
    return BACKGROUND


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp):
        sp.add_argument("--out-dir", type=Path, default=None,
                        help="gaul.py output dir (default: read the published files)")

    b = sub.add_parser("build", help="render the catalog tree")
    out_dir_arg(b)
    b.add_argument("--dest", type=Path, required=True)

    c = sub.add_parser("check", help="rashid pass over a rendered tree")
    c.add_argument("tree", type=Path)
    c.add_argument("--remote-data", action="store_true",
                   help="also range-read the published parquet files (PTL-DAT-*)")

    u = sub.add_parser("publish", help="build, check, upload to <remote>/catalog/")
    out_dir_arg(u)
    u.add_argument("--remote", required=True, help="rclone path of the dataset prefix, "
                                                   "e.g. parquetry:parquetry/gaul")
    u.add_argument("--remote-data", action="store_true")
    u.add_argument("--dry-run", action="store_true")

    t = sub.add_parser("thumbnails", help="render catalog/thumbnails/gaul/<layer>.png")
    t.add_argument("--out-dir", type=Path, required=True)
    t.add_argument("--layers", nargs="*", default=list(LAYERS))

    args = p.parse_args()
    if args.cmd == "build":
        m = build(args.out_dir, args.dest)
        print(f"wrote {args.dest}/ ({m['total_features']:,} rows over {len(LAYERS)} collections)")
    elif args.cmd == "check":
        sys.exit(check(args.tree, remote_data=args.remote_data))
    elif args.cmd == "publish":
        publish(args.out_dir, args.remote, remote_data=args.remote_data, dry_run=args.dry_run)
    elif args.cmd == "thumbnails":
        thumbnails(args.out_dir, args.layers)


if __name__ == "__main__":
    main()
