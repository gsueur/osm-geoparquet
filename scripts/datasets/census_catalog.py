#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the US Census data.

  census/catalog/catalog.json                     root: US Census Bureau TIGER/Line
  census/catalog/tiger-2026-legislative/catalog.json   one sub-catalog per product
  census/catalog/tiger-2026-legislative/<code>/collection.json   one per layer

census.py's PRODUCTS drives it: a product added there gets its sub-catalog
here. Each collection has the layer's GeoParquet (`data`, NAD83 declared
with the projection extension), its PMTiles registered as formats.md asks
(a `rel: pmtiles` link with `pmtiles:layers`, web-map-links v1.3.0, and the
file as a `visual` asset), two MapLibre styles under styles/ and a
thumbnail. Counts, sizes and checksums come from the product's
_manifest.json, local (--out-dir) or published. Spec: portolan-spec v0.2.0.
Validator: rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/census_catalog.py build --dest build/census-catalog [--out-dir out/census]
  python3 scripts/datasets/census_catalog.py check build/census-catalog
  python3 scripts/datasets/census_catalog.py publish --remote parquetry:parquetry/census [--out-dir out/census]
  uv run --project scripts --with matplotlib --with shapely \\
      python scripts/datasets/census_catalog.py thumbnails --out-dir out/census
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
from datasets.census import LAYOUTS, PRODUCTS, SOURCE_PAGE, YEAR, color_index
from datasets.gaul_catalog import STYLE_TYPE, WEB_MAP_LINKS_EXT
from datasets.pmtiles import PMTILES_TYPE

import catalog as shared  # scripts/catalog.py
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

DATASET_PREFIX = "census"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "us-census-tiger"
TITLE = "US Census Bureau TIGER/Line"
PAGE_URL = "https://census.geomermaids.com/"
THUMBS = REPO / "catalog" / "thumbnails" / "census"
PROJ_EXT = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
ATTRIBUTION = f"U.S. Census Bureau, TIGER/Line {YEAR}"

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
    "title": "TIGER/Line GeoPackages on census.gov, where they are published",
}
PROVIDERS = [
    {
        "name": "U.S. Census Bureau, Geography Division",
        "description": "Produces TIGER/Line from its MAF/TIGER system and publishes it as "
                       "national GeoPackages. A US government work, not subject to copyright "
                       "in the United States.",
        "url": "https://www.census.gov/geographies/mapping-files.html",
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted the GeoPackage layers to GeoParquet, tiled them, and "
                       "maintains and hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]
DISCLAIMER = (
    "The Census Bureau makes no warranty on the accuracy of TIGER/Line, and its boundaries "
    "are for statistical data collection and tabulation only: not a determination of "
    "jurisdictional authority, ownership or entitlement, and not legal land descriptions.")

# From the record layouts (section 8). The area code column differs per layer.
COLUMN_DOCS = {
    "STATEFP": "State FIPS code.",
    "CD120FP": "120th congressional district FIPS code. 00: at-large district; 98: "
               "non-voting delegate or resident commissioner; ZZ: no district defined.",
    "SLDUST": "State legislative district (upper chamber) code. ZZZ: no district defined.",
    "SLDLST": "State legislative district (lower chamber) code. ZZZ: no district defined.",
    "GEOID": "District identifier: state FIPS code followed by the district code.",
    "GEOIDFQ": "Fully qualified GEOID, used to join data tables from data.census.gov.",
    "NAMELSAD": "Name and translated legal/statistical area description.",
    "LSAD": "Legal/statistical area description code.",
    "CDSESSN": "Congressional session code (120).",
    "LSY": "Legislative session year.",
    "MTFCC": "MAF/TIGER feature class code (G5200 congressional, G5210 upper, G5220 lower).",
    "FUNCSTAT": "Functional status.",
    "ALAND": "Land area, square metres.",
    "AWATER": "Water area, square metres.",
    "INTPTLAT": "Latitude of the internal point (text, as published).",
    "INTPTLON": "Longitude of the internal point (text, as published).",
    "geometry": "District outline as native Parquet GEOMETRY (WKB), NAD83 (EPSG:4269), "
                "longitude and latitude, 2D.",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the GeoParquet "
            "covering. Filter on it to prune row groups.",
}

# The page's district palette: neighbours never share a colour (color_index).
PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
UNDEFINED = "#c9c8c2"


# ---------- measured inputs ----------

def product_url(product: str) -> str:
    return f"{PUBLIC_DATA}/tiger/{YEAR}/{product}"


def read_manifest(out_dir: Path | None, product: str) -> dict:
    local = out_dir / "tiger" / YEAR / product / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    with urllib.request.urlopen(f"{product_url(product)}/_manifest.json") as r:
        return json.load(r)


def footer(source: str) -> list[tuple[str, str]]:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    return [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
            con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()]


def data_source(out_dir: Path | None, product: str, file: str) -> str:
    local = out_dir / "tiger" / YEAR / product / file if out_dir else None
    return str(local) if local and local.is_file() else f"{product_url(product)}/{file}"


# ---------- styles ----------

def styles(code: str, key: str, tiles: dict, product: str) -> dict[str, dict]:
    source = {"data": {"type": "vector", "url": f"pmtiles://{product_url(product)}/{tiles['file']}",
                       "attribution": ATTRIBUTION}}
    # A case chain rather than a match: the colours only separate neighbours,
    # so no legend should be read off them.
    fill = ["case", ["in", "ZZ", ["get", key]], UNDEFINED]
    for i, c in enumerate(PALETTE[:-1]):
        fill += [["==", ["get", "color_index"], i], c]
    fill.append(PALETTE[-1])
    outline = {"id": f"{code}-outline", "type": "line", "source": "data", "source-layer": code,
               "paint": {"line-color": "#ffffff",
                         "line-width": ["interpolate", ["linear"], ["zoom"], 3, 0.4, 9, 1.6]}}
    return {
        "default.json": {
            "version": 8, "name": f"{code}: districts, neighbours apart", "sources": source,
            "layers": [{"id": f"{code}-fill", "type": "fill", "source": "data",
                        "source-layer": code,
                        "paint": {"fill-color": fill, "fill-opacity": 0.6}}, outline],
        },
        "outlines.json": {
            "version": 8, "name": f"{code}: outlines only", "sources": source,
            "layers": [{**outline, "paint": {"line-color": "#d2553a", "line-width": [
                "interpolate", ["linear"], ["zoom"], 3, 0.5, 9, 2]}}],
        },
    }


STYLE_ASSETS = {
    "style": ("./styles/default.json", "Districts, coloured so neighbours differ",
              ["style", "default"]),
    "style-outlines": ("./styles/outlines.json", "District outlines only, to lay over other data",
                       ["style"]),
}


# ---------- STAC ----------

def build_collection(product: str, layer: tuple, entry: dict, tiles: dict, manifest: dict,
                     columns: list[tuple[str, str]], updated: str) -> dict:
    gpkg_layer, code, title, key, _, _ = layer
    path = f"{DATASET_PREFIX}/tiger/{YEAR}/{product}/{entry['file']}"
    tiles_path = f"{DATASET_PREFIX}/tiger/{YEAR}/{product}/{tiles['file']}"
    x0, y0, x1, y1 = [round(v, 6) for v in manifest_extent(manifest, code)]
    districts = sum(entry["by_state"].values())
    undocumented = [c for c, _ in columns if c not in COLUMN_DOCS]
    if undocumented:
        sys.exit(f"{code}: no column doc for {undocumented}")
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT,
                            PROJ_EXT, WEB_MAP_LINKS_EXT],
        "id": code,
        "title": f"{title}, {YEAR}",
        "description": (
            f"The '{gpkg_layer}' layer of the {YEAR} TIGER/Line {PRODUCTS[product]['title']} "
            f"National GeoPackage: {entry['features']:,} rows, of which {districts:,} districts "
            f"in {len(entry['by_state'])} states and territories (the rest are TIGER's "
            f"placeholders, documented in table:columns). Columns and values unchanged; 2D "
            f"geometry in NAD83 (EPSG:4269) with a bbox covering, Hilbert-sorted, as one "
            f"GeoParquet 2.0 file read in place over HTTPS or the anonymous S3 endpoint "
            f"{S3_ENDPOINT}, and a PMTiles archive for web maps. {DISCLAIMER} "
            f"Credit: {ATTRIBUTION}."
        ),
        "keywords": ["TIGER/Line", "US Census Bureau", "boundaries", "elections", "districts",
                     code, "GeoParquet", "PMTiles"],
        "license": "other",
        "version": YEAR,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {
            # Districts cross the antimeridian (Alaska's Aleutians): the box
            # is the full span, as STAC wants for a crossing extent's union.
            "spatial": {"bbox": [[max(-180.0, x0), max(-90.0, y0), min(180.0, x1), min(90.0, y1)]]},
            "temporal": {"interval": [[f"{YEAR}-01-01T00:00:00Z", f"{YEAR}-12-31T23:59:59Z"]]},
        },
        "table:row_count": entry["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": COLUMN_DOCS[n]} for n, t in columns],
        "assets": {
            "data": {
                "href": f"{PUBLIC_BASE}/{path}",
                "type": PARQUET_TYPE,
                "title": f"{title}, {YEAR}",
                "description": f"{entry['features']:,} rows in {entry['row_groups']} row groups "
                               f"of at most 4 MiB of geometry.",
                "roles": ["data"],
                "proj:code": "EPSG:4269",
                "file:size": entry["bytes"],
                "file:checksum": "1220" + entry["sha256"],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                     "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            "pmtiles": {
                "href": f"{PUBLIC_BASE}/{tiles_path}",
                "type": PMTILES_TYPE,
                "title": f"{title}, vector tiles",
                "description": f"PMTiles, layer `{tiles['layer']}`, zoom {tiles['min_zoom']} to "
                               f"{tiles['max_zoom']}, {tiles['tiles']:,} tiles; properties "
                               f"{', '.join(tiles['properties'])} (color_index: a colour per "
                               f"district such that neighbours differ). Built with tylertoo "
                               f"{tiles['tylertoo']}.",
                "roles": ["visual"],
                "file:size": tiles["bytes"],
                "file:checksum": "1220" + tiles["sha256"],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{tiles_path}",
                                     "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            **{k: {"href": href, "type": STYLE_TYPE, "title": t, "roles": roles}
               for k, (href, t, roles) in STYLE_ASSETS.items()},
            "thumbnail": {
                "href": "./thumbnail.png",
                "type": "image/png",
                "title": f"{title}, conterminous United States",
                "roles": ["thumbnail"],
                "file:size": (THUMBS / f"{code}.png").stat().st_size,
                "file:checksum": multihash(THUMBS / f"{code}.png"),
            },
        },
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{title}: README"),
            md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "describedby", "href": LAYOUTS, "type": "application/pdf",
             "title": f"{YEAR} TIGER/Line GeoPackage record layouts"},
            {"rel": "alternate", "href": f"{product_url(product)}/", "type": "text/html",
             "title": "Browse the files"},
            {"rel": "alternate", "href": PAGE_URL, "type": "text/html",
             "title": "census.geomermaids.com: the districts on a map"},
            {"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
             "title": f"{title}, vector tiles", "pmtiles:layers": [tiles["layer"]]},
        ],
    }


def manifest_extent(manifest: dict, code: str) -> list[float]:
    """The layer's extent: the union of the states' boxes, which the build
    measured with the antimeridian handled; unwrapped back into -180..180."""
    boxes = [s["bounds"] for s in manifest["states"].values() if s.get("bounds")]
    x0 = min(b[0] for b in boxes)
    return [-180.0 if x0 < -180 else x0, min(b[1] for b in boxes),
            max(b[2] for b in boxes) if x0 >= -180 else 180.0, max(b[3] for b in boxes)]


def product_catalog_id(product: str) -> str:
    return f"tiger-{YEAR}-{product}"


def build_product(product: str, collections: list[dict], manifest: dict, updated: str) -> dict:
    spec = PRODUCTS[product]
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": product_catalog_id(product),
        "title": f"TIGER/Line {YEAR}: {spec['title']}",
        "description": (f"The {YEAR} TIGER/Line {spec['title']} National GeoPackage "
                        f"({spec['gpkg']}), one collection per layer. Source last modified "
                        f"{manifest['source']['last_modified']}."),
        "version": YEAR,
        "updated": updated,
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{spec['title']}: README"),
            md_link("agents", "./AGENTS.md", f"{spec['title']}: agent guide"),
            LICENSE_LINK,
            {"rel": "related", "href": f"{product_url(product)}/ATTRIBUTION.txt",
             "type": "text/plain", "title": "Attribution, disclaimer and what was changed"},
        ],
    }


def build_root(products: list[dict], updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": ("The US Census Bureau's TIGER/Line national GeoPackages, republished "
                        "as GeoParquet and PMTiles: one sub-catalog per product and vintage, "
                        "one collection per layer. Columns and codes unchanged. Public domain "
                        f"in the United States. Credit: {ATTRIBUTION}."),
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{p['id']}/catalog.json", "type": "application/json",
               "title": p["title"]} for p in products),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
            {"rel": "alternate", "href": PAGE_URL, "type": "text/html",
             "title": "census.geomermaids.com"},
        ],
    }


# ---------- markdown ----------

LICENSE_MD = f"""\
TIGER/Line is a work of the US federal government, not subject to copyright in
the United States (17 U.S.C. 105). Credit "{ATTRIBUTION}". {DISCLAIMER}"""


def access_md(product: str, code: str, key: str) -> str:
    url = f"{product_url(product)}/{code}.parquet"
    return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
SELECT GEOID, NAMELSAD
FROM read_parquet('{url}')
WHERE bbox.xmin <= -97.74 AND bbox.xmax >= -97.74
  AND bbox.ymin <= 30.27 AND bbox.ymax >= 30.27
  AND ST_Contains(geometry, ST_Point(-97.74, 30.27));
```

The same file at `s3://{BUCKET}/{DATASET_PREFIX}/tiger/{YEAR}/{product}/{code}.parquet` on the
anonymous S3 endpoint `{S3_ENDPOINT}`. Leave out TIGER's placeholders with
`WHERE {key} NOT LIKE 'ZZ%'`{" AND " + key + " <> '98'" if key == "CD120FP" else ""}."""


def collection_readme(col: dict, product: str, layer: tuple, entry: dict, tiles: dict) -> str:
    _, code, title, key, _, _ = layer
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, {entry['row_groups']} row groups, sha256 `{entry['sha256'][:12]}...` |
| Rows | {entry['features']:,} ({sum(entry['by_state'].values()):,} districts) |
| Map tiles | `{tiles['file']}`, {tiles['bytes'] / 1e6:,.1f} MB, zoom {tiles['min_zoom']} to {tiles['max_zoom']} |
| CRS | NAD83, EPSG:4269 (geographic) |
| On a map | {PAGE_URL} |

## Access

{access_md(product, code, key)}

## Map tiles

`{product_url(product)}/{tiles['file']}`, tile layer `{tiles['layer']}`, properties
{', '.join(f"`{p}`" for p in tiles['properties'])}. `color_index` (0 to 5) gives every
district a colour no neighbour shares; `styles/default.json` uses it, and
`styles/outlines.json` draws the boundaries alone. The tiles read NAD83 as
lon/lat, within 2 m in the conterminous US.

## Schema

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

The layer '{layer[0]}' of `{PRODUCTS[product]['gpkg']}`, downloaded from
{SOURCE_PAGE} (Census Bureau, Geography Division; record layouts:
{LAYOUTS}). [census.py]({REPO_URL}/blob/main/scripts/datasets/census.py) reads
it with DuckDB, keeps every column and value, drops the empty Z and M from the
geometry, keeps NAD83, adds the `bbox` covering, sorts along a Hilbert curve
and cuts row groups at 4 MiB of geometry; each file's footer, CRS, bbox and
groups are verified before upload. The PMTiles come from the same rows, with
tylertoo.

## License

{LICENSE_MD}
"""


def collection_agents(col: dict, product: str, layer: tuple) -> str:
    _, code, title, key, _, _ = layer
    return f"""\
# {col['title']}: agent guide

One row per district (plus TIGER's placeholders), NAD83 (EPSG:4269), geometry
column `geometry`, area code in `{key}`, identifier `GEOID`.

## Access

{access_md(product, code, key)}

## Query tips

- Filter on `bbox` before `ST_Contains`; the file is Hilbert-sorted, so a point
  reads one row group.
- `ALAND` and `AWATER` are in square metres, as published. For other areas,
  `ST_Area_Spheroid` works on NAD83 coordinates.
- Join to Census tables on `GEOID`, or on `GEOIDFQ` for data.census.gov.
- For a map, use the `rel: pmtiles` link (source-layer `{code}`) and the
  `default` style asset.
- Credit: {ATTRIBUTION}. Boundaries are statistical, not legal descriptions.
"""


def product_readme(cat: dict, product: str, collections: list[dict], manifest: dict) -> str:
    rows = "\n".join(f"| [{c['title']}](./{c['id']}/README.md) | `{c['id']}.parquet` | "
                     f"{c['table:row_count']:,} |" for c in collections)
    return f"""\
# {cat['title']}

{cat['description']}

| Collection | File | Rows |
|---|---|---|
{rows}

Source: {manifest['source']['url']}. On a map: {PAGE_URL}.

## License

{LICENSE_MD}
"""


def product_agents(cat: dict, collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {cat['id']}: agent guide

{cat['description']}

{listing}

All NAD83 (EPSG:4269), TIGER columns unchanged, one GeoParquet file and one
PMTiles archive per collection. Credit: {ATTRIBUTION}.
"""


def root_readme(root: dict, products: list[dict]) -> str:
    listing = "\n".join(f"- [{p['title']}](./{p['id']}/README.md)" for p in products)
    return f"""\
# {root['title']}

{root['description']}

{listing}

Files: `{PUBLIC_DATA}/tiger/<year>/<product>/<layer>.parquet`. On a map:
{PAGE_URL}.

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(products: list[dict]) -> str:
    listing = "\n".join(f"- `{p['id']}`: {p['title']}. {p['id']}/AGENTS.md" for p in products)
    return f"""\
# {CATALOG_ID}: agent guide

TIGER/Line national GeoPackages as GeoParquet, one sub-catalog per product.

{listing}

Layout: `{PUBLIC_DATA}/tiger/<year>/<product>/<layer>.parquet`, anonymous S3 at
`s3://{BUCKET}/{DATASET_PREFIX}/` on `{S3_ENDPOINT}`. NAD83 (EPSG:4269).
Credit: {ATTRIBUTION}.
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    products, total = [], 0
    for product, spec in PRODUCTS.items():
        manifest = read_manifest(out_dir, product)
        files = {f["name"]: f for f in manifest["files"]}
        tiles = {t["name"]: t for t in manifest["pmtiles"]}
        pdir = dest / product_catalog_id(product)
        collections = []
        for layer in spec["layers"]:
            code = layer[1]
            if not (THUMBS / f"{code}.png").is_file():
                sys.exit(f"missing {THUMBS / code}.png: run the thumbnails command")
            columns = footer(data_source(out_dir, product, files[code]["file"]))
            col = build_collection(product, layer, files[code], tiles[code], manifest,
                                   columns, updated)
            cdir = pdir / code
            for fname, doc in styles(code, layer[3], tiles[code], product).items():
                write_json(cdir / "styles" / fname, doc)
            for asset in col["assets"].values():
                if "style" in asset["roles"]:
                    asset["file:size"] = (cdir / asset["href"]).stat().st_size
                    asset["file:checksum"] = multihash(cdir / asset["href"])
            write_json(cdir / "collection.json", col)
            shutil.copyfile(THUMBS / f"{code}.png", cdir / "thumbnail.png")
            (cdir / "README.md").write_text(
                collection_readme(col, product, layer, files[code], tiles[code]))
            (cdir / "AGENTS.md").write_text(collection_agents(col, product, layer))
            collections.append(col)
            total += files[code]["features"]
        cat = build_product(product, collections, manifest, updated)
        write_json(pdir / "catalog.json", cat)
        (pdir / "README.md").write_text(product_readme(cat, product, collections, manifest))
        (pdir / "AGENTS.md").write_text(product_agents(cat, collections))
        products.append(cat)
    root = build_root(products, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, products))
    (dest / "AGENTS.md").write_text(root_agents(products))
    return total


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="census-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        total = build(out_dir, tree)
        print(f"  rendered {sum(len(p['layers']) for p in PRODUCTS.values())} collections, "
              f"{total:,} rows")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

CONUS = (-125.0, 24.0, -66.5, 49.5)


def thumbnails(out_dir: Path) -> None:
    """Each layer over the conterminous US, coloured as the default style."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    from thumbnails import BACKGROUND, frame

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    x0, y0, x1, y1, aspect = frame(*CONUS)
    THUMBS.mkdir(parents=True, exist_ok=True)
    for product, spec in PRODUCTS.items():
        for _, code, _, key, _, _ in spec["layers"]:
            path = out_dir / "tiger" / YEAR / product / f"{code}.parquet"
            colours = color_index(con, path)
            rows = con.execute(f"""
                SELECT GEOID, {key}, ST_AsWKB(ST_SimplifyPreserveTopology(geometry::GEOMETRY, 0.02))
                FROM read_parquet('{path}')
                WHERE bbox.xmax > {CONUS[0]} AND bbox.xmin < {CONUS[2]}
                  AND bbox.ymax > {CONUS[1]} AND bbox.ymin < {CONUS[3]}""").fetchall()
            polys, fills = [], []
            for geoid, area_code, wkb in rows:
                colour = UNDEFINED if "ZZ" in area_code else PALETTE[colours[geoid]]
                for part in shapely.get_parts(shapely.from_wkb(bytes(wkb))):
                    polys.append(shapely.get_coordinates(part.exterior))
                    fills.append(colour)
            fig = plt.figure(figsize=(6, 4), dpi=100)
            ax = fig.add_axes((0, 0, 1, 1))
            ax.set_facecolor(BACKGROUND)
            fig.patch.set_facecolor(BACKGROUND)
            ax.add_collection(PolyCollection(polys, facecolors=fills, edgecolors="#ffffff",
                                             linewidths=0.25, alpha=0.85))
            ax.set_xlim(x0, x1)
            ax.set_ylim(y0, y1)
            ax.set_aspect(aspect)
            ax.axis("off")
            fig.savefig(THUMBS / f"{code}.png", dpi=100, facecolor=BACKGROUND)
            plt.close(fig)
            print(f"  {code}: {len(rows):,} districts -> {(THUMBS / f'{code}.png').relative_to(REPO)}")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp, required=False):
        sp.add_argument("--out-dir", type=Path, default=None, required=required,
                        help="census.py's --out-dir (default: read the published files)")

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
