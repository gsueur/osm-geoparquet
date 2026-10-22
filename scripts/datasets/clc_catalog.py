#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for Corine Land Cover 2018.

One STAC Collection, clc_2018, describing the release: the Europe file (the
`data` asset), one file per country under country=<XX>/ (the partition
extension and one `data-<xx>` asset each), the H3 cell aggregates
clc_tiles.py derives (`data-h3`), and its PMTiles archive, registered as
formats.md asks: a `rel: pmtiles` link with `pmtiles:layers` (web-map-links
v1.3.0), the file as a `visual` asset, and MapLibre styles under styles/, the
default drawing the official legend from meta/clc_legend.json.

The data is EPSG:3035 and stays so (the projection extension says it on the
data assets); the collection extent is that footer box, densified and
reprojected to lon/lat. Sizes, checksums and counts come from the release's
manifests, read locally from --out-dir or from the published files. Spec:
portolan-spec v0.2.0. Validator: rashid, through scripts/catalog.py.

The catalog sits beside the data, at /clc/catalog/catalog.json.

Usage:
  python3 scripts/datasets/clc_catalog.py build --dest build/clc-catalog [--out-dir out/2018]
  python3 scripts/datasets/clc_catalog.py check build/clc-catalog
  python3 scripts/datasets/clc_catalog.py publish --remote parquetry:parquetry/clc [--out-dir out/2018]
  # the committed thumbnail, from the H3 cells clc_tiles.py writes
  uv run --project scripts --with matplotlib --with shapely \\
      python scripts/datasets/clc_catalog.py thumbnail --cells out/2018/clc_2018_h3.parquet
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
from datasets.clc import RELEASE, STEM, country_name
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

DATASET_PREFIX = "clc"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
DATA_URL = f"{PUBLIC_DATA}/{RELEASE}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "corine-land-cover-2018"
COLLECTION_ID = STEM
TITLE = "Corine Land Cover 2018"
THUMB = REPO / "catalog" / "thumbnails" / "clc" / f"{STEM}.png"
LEGEND = REPO / "meta" / "clc_legend.json"
PROJ_EXT = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
PARTITION_KEY = "country"

CLMS_URL = "https://land.copernicus.eu/en/products/corine-land-cover/clc2018"
POLICY_URL = "https://land.copernicus.eu/en/data-policy"
LICENSE_LINK = {
    "rel": "license",
    "href": POLICY_URL,
    "type": "text/html",
    "title": "Copernicus data policy: full, open and free access, attribution required "
             "(Regulation (EU) No 1159/2013)",
}
VIA_LINK = {
    "rel": "via",
    "href": CLMS_URL,
    "type": "text/html",
    "title": "CLC 2018 on the Copernicus Land Monitoring Service, where it is published",
}
ATTRIBUTION = ("(c) European Union, Copernicus Land Monitoring Service, "
               "European Environment Agency (EEA)")

PROVIDERS = [
    {
        "name": "European Environment Agency (EEA), Copernicus Land Monitoring Service",
        "description": "Produces Corine Land Cover and distributes it under the Copernicus "
                       "data policy: full, open and free access, attribution required.",
        "url": CLMS_URL,
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted CLC 2018 to GeoParquet, assigned each polygon a country, "
                       "split it per country, tiled it, and maintains and hosts this catalog "
                       "and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]

# CLC 2018 maps land cover in 2018 (imagery from 2017 to 2018).
INTERVAL = ["2017-01-01T00:00:00Z", "2018-12-31T23:59:59Z"]

COLUMN_DOCS = {
    "Shape": "Land cover polygon as native Parquet GEOMETRY (WKB), ETRS89 / LAEA Europe "
             "(EPSG:3035), metres. Minimum mapping unit 25 ha.",
    "OBJECTID": "Object id from the EEA GeoPackage.",
    "Code_18": "CLC class, the three-digit code of the CLC nomenclature (111 continuous "
               "urban fabric ... 523 sea and ocean; 999 NODATA). The legend is in the README.",
    "Remark": "EEA remark on the polygon, mostly empty.",
    "Area_Ha": "Polygon area in hectares, as the EEA computed it.",
    "ID": "EEA polygon id (EU_<n>), unique across Europe.",
    "country": "ISO 3166-1 alpha-2 country, added by Geomermaids (not part of CLC): the "
               "GAUL 2024 country the polygon overlaps most, or the nearest one. Polygons "
               "are never cut. Also the partition key.",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax) in EPSG:3035 metres, declared "
            "as the GeoParquet covering. Filter on it to prune row groups.",
}


# ---------- measured inputs ----------

def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def read_manifests(out_dir: Path | None) -> tuple[dict, dict[str, dict]]:
    """The release manifest and each country's, local or published."""
    if out_dir is not None and (out_dir / "_manifest.json").is_file():
        main = json.loads((out_dir / "_manifest.json").read_text())
    else:
        main = fetch_json(f"{DATA_URL}/_manifest.json")
    countries = {}
    for code in main["countries"]:
        local = out_dir / f"{PARTITION_KEY}={code}" / "_manifest.json" if out_dir else None
        countries[code] = (json.loads(local.read_text()) if local and local.is_file()
                           else fetch_json(f"{DATA_URL}/{PARTITION_KEY}={code}/_manifest.json"))
    return main, countries


def footer(source: str) -> tuple[list[tuple[str, str]], list[float]]:
    """(column name/type pairs, lon/lat extent) from the Europe file's footer.
    The footer box is EPSG:3035; its edges are densified before reprojecting,
    since a straight LAEA edge is a curve in lon/lat."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs; "
                "SET geometry_always_xy = true")
    columns = [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
               con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()]
    row = con.execute(f"SELECT value FROM parquet_kv_metadata('{source}') "
                      "WHERE key = 'geo'").fetchone()
    geo = json.loads(bytes(row[0]).decode())
    x0, y0, x1, y1 = geo["columns"]["Shape"]["bbox"]
    n = 64
    edge = [(x0 + (x1 - x0) * i / n, y0) for i in range(n + 1)]
    edge += [(x1, y0 + (y1 - y0) * i / n) for i in range(n + 1)]
    edge += [(x0 + (x1 - x0) * i / n, y1) for i in range(n + 1)]
    edge += [(x0, y0 + (y1 - y0) * i / n) for i in range(n + 1)]
    pts = ", ".join(f"ST_Point({x}, {y})" for x, y in edge)
    box = con.execute(f"""
        SELECT min(ST_X(p)), min(ST_Y(p)), max(ST_X(p)), max(ST_Y(p)) FROM (
          SELECT ST_Transform(unnest([{pts}]), 'EPSG:3035', 'OGC:CRS84') AS p)""").fetchone()
    bbox = [max(-180.0, round(box[0], 4)), max(-90.0, round(box[1], 4)),
            min(180.0, round(box[2], 4)), min(90.0, round(box[3], 4))]
    return columns, bbox


def europe_source(out_dir: Path | None) -> str:
    if out_dir is not None and (out_dir / f"{STEM}.parquet").is_file():
        return str(out_dir / f"{STEM}.parquet")
    return f"{DATA_URL}/{STEM}.parquet"


# ---------- style ----------

def legend() -> list[dict]:
    return json.loads(LEGEND.read_text())["classes"]


def source(tiles: dict) -> dict:
    return {"data": {
        "type": "vector",
        "url": f"pmtiles://{DATA_URL}/{tiles['file']}",
        "attribution": ATTRIBUTION,
    }}


def present_codes(source: str) -> set[str]:
    """The CLC codes the data holds: a legend lists those and no others
    (990 and 995 are in the CLC nomenclature, not in the 2018 data)."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    return {r[0] for r in con.execute(
        f"SELECT DISTINCT Code_18 FROM read_parquet('{source}')").fetchall()}


def legend_match(codes: set[str], color=None, fallback: str = "#ffffff") -> list:
    """A match on `label`, not on `Code_18`: the Portolan browser names each
    legend entry after its match key, and every feature carries both."""
    match = ["match", ["get", "label"]]
    for c in legend():
        if c["code"] in codes:
            match += [c["label"], color(c["code"]) if color else c["color"]]
    return match + [fallback]


def styles(tiles: dict, codes: set[str]) -> dict[str, dict]:
    """The MapLibre styles, by file name under styles/. The default draws the
    official legend on both layers; the others draw one CLC group's share,
    from the cells' percentage at small scales and the classes at large.
    Each layer has the zoom range its source layer covers, so the browser's
    legend describes the layer on screen."""
    cells, polys = tiles["layers"]
    handover = next(b["min_zoom"] for b in tiles["bands"] if b["h3_res"] is None)
    out = {"default.json": {
        "version": 8,
        "name": f"{TITLE}, official CLC legend",
        "sources": source(tiles),
        "layers": [
            {"id": "cells-fill", "type": "fill", "source": "data", "source-layer": cells,
             "maxzoom": handover,
             "paint": {"fill-color": legend_match(codes),
                       "fill-outline-color": "rgba(255,255,255,0.35)"}},
            {"id": "clc-fill", "type": "fill", "source": "data", "source-layer": polys,
             "minzoom": handover,
             "paint": {"fill-color": legend_match(codes), "fill-antialias": False}},
        ],
    }}
    for name, group, prefix, ramp in (
            ("artificial", "Artificial surfaces", "1",
             ["#f7f4f0", "#f4c7b8", "#e8846a", "#cc3d2e", "#8c1c1c"]),
            ("forest", "Forest and semi-natural areas", "3",
             ["#f5f7f0", "#cfe5b8", "#8fc77a", "#3f9a45", "#1b5e2a"])):
        steps = ["step", ["get", f"{name}_pct"], ramp[0]]
        for threshold, color in zip((5, 25, 50, 75), ramp[1:]):
            steps += [threshold, color]
        out[f"{name}.json"] = {
            "version": 8,
            "name": f"{TITLE}, share of {group.lower()}",
            "sources": source(tiles),
            "layers": [
                {"id": "cells-fill", "type": "fill", "source": "data", "source-layer": cells,
                 "maxzoom": handover,
                 "paint": {"fill-color": steps, "fill-outline-color": "rgba(255,255,255,0.35)"}},
                {"id": "clc-fill", "type": "fill", "source": "data", "source-layer": polys,
                 "minzoom": handover,
                 "paint": {"fill-color": legend_match(
                               {c for c in codes if c.startswith(prefix)},
                               lambda _: ramp[-1], ramp[0]),
                           "fill-antialias": False}},
            ],
        }
    return out


STYLE_TITLES = {
    "default.json": ("Official CLC legend: dominant class per H3 cell, then the polygons",
                     ["style", "default"]),
    "artificial.json": ("Share of artificial surfaces (CLC 1xx)", ["style"]),
    "forest.json": ("Share of forest and semi-natural areas (CLC 3xx)", ["style"]),
}


def bands_text(tiles: dict) -> str:
    cells = [b for b in tiles["bands"] if b["h3_res"] is not None]
    poly = next(b for b in tiles["bands"] if b["h3_res"] is None)
    zooms = ", ".join(
        f"z{b['min_zoom']}" + (f"-{b['max_zoom']}" if b["max_zoom"] != b["min_zoom"] else "")
        + f" r{b['h3_res']}" for b in cells)
    return (f"Zooms {cells[0]['min_zoom']} to {cells[-1]['max_zoom']} draw the `{cells[0]['layer']}` "
            f"layer, H3 cells with the dominant class and the share of each CLC group "
            f"({zooms}); from zoom {poly['min_zoom']} the `{poly['layer']}` layer holds the CLC "
            f"polygons themselves.")


def country_assets(countries: dict[str, dict]) -> dict:
    assets = {}
    for code, m in sorted(countries.items()):
        f = m["files"][0]
        path = f"{DATASET_PREFIX}/{RELEASE}/{PARTITION_KEY}={code}/{f['file']}"
        assets[f"data-{code.lower()}"] = {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": f"{country_name(code)} ({code})",
            "description": f"{f['features']:,} polygons",
            "roles": ["data"],
            "proj:code": "EPSG:3035",
            "file:size": f["bytes"],
            "file:checksum": "1220" + f["sha256"],
            "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                 "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
        }
    return assets


def h3_asset(h3: dict) -> dict:
    path = f"{DATASET_PREFIX}/{RELEASE}/{h3['file']}"
    counts = ", ".join(f"r{r}: {n:,}" for r, n in h3["cells"].items())
    return {
        "href": f"{PUBLIC_BASE}/{path}",
        "type": PARQUET_TYPE,
        "title": f"{TITLE} on H3 cells",
        "description": f"Land cover per H3 cell, one row per cell of resolutions "
                       f"{', '.join(str(r) for r in h3['cells'])} ({counts}): dominant class, its "
                       f"share, the share of each CLC group and the part of the cell CLC covers, "
                       f"from a sample of resolution {h3['sample_res']} cells. The z0-8 layer of "
                       f"the map tiles, and a table to join on H3. Lon/lat (OGC:CRS84).",
        "roles": ["data"],
        "file:size": h3["bytes"],
        "file:checksum": "1220" + h3["sha256"],
        "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                             "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
    }


def build_collection(main: dict, countries: dict[str, dict], tiles: dict, h3: dict,
                     columns: list[tuple[str, str]], bbox: list[float], updated: str) -> dict:
    europe = main["files"][0]
    path = f"{DATASET_PREFIX}/{RELEASE}/{europe['file']}"
    tiles_path = f"{DATASET_PREFIX}/{RELEASE}/{tiles['file']}"
    glob = f"s3://{BUCKET}/{DATASET_PREFIX}/{RELEASE}/{PARTITION_KEY}=*/{STEM}.parquet"
    undocumented = [c for c, _ in columns if c not in COLUMN_DOCS]
    if undocumented:
        sys.exit(f"no column doc for {undocumented}")
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT, VERSION_EXT,
                            ALTERNATE_EXT, PROJ_EXT, WEB_MAP_LINKS_EXT],
        "id": COLLECTION_ID,
        "title": f"{TITLE}, land cover polygons",
        "description": (
            f"Corine Land Cover 2018 (version 2020 20u1), the EEA's pan-European land cover "
            f"inventory: {europe['features']:,} polygons in 44 classes, minimum mapping unit "
            f"25 ha, ETRS89 / LAEA Europe (EPSG:3035), geometry and attributes unchanged. Three "
            f"layouts of the same polygons: the Europe file ({europe['bytes'] / 1e9:,.1f} GB, the "
            f"data asset), one file per country under {PARTITION_KEY}=<XX>/ ({len(countries)} "
            f"files, the data-<xx> assets and the partition glob {glob}), both with a bbox "
            f"covering and read in place over HTTPS or the anonymous S3 endpoint "
            f"{S3_ENDPOINT}. Derived here: land cover aggregated on H3 cells (the data-h3 "
            f"asset), and a PMTiles archive for web maps, zoom {tiles['min_zoom']} to "
            f"{tiles['max_zoom']}, H3 cells up to zoom 8 then the polygons, every feature "
            f"carrying its official legend colour. The country column is ours, assigned "
            f"from GAUL 2024. {ATTRIBUTION}."
        ),
        "keywords": ["CORINE", "CLC", "land cover", "land use", "Copernicus", "EEA", "Europe",
                     "H3", "GeoParquet", "PMTiles"],
        "license": "other",
        "attribution": ATTRIBUTION,
        "version": RELEASE,
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
             "description": "ISO 3166-1 alpha-2 country the polygon was assigned to "
                            "(Geomermaids, from GAUL 2024; Kosovo is in RS). One file per code."},
        ],
        "partition:file_count": len(countries),
        "partition:glob": glob,
        "table:row_count": europe["features"],
        "table:primary_geometry": "Shape",
        "table:columns": [
            {"name": n, "type": t, "description": COLUMN_DOCS[n]} for n, t in columns
        ],
        "assets": {
            "data": {
                "href": f"{PUBLIC_BASE}/{path}",
                "type": PARQUET_TYPE,
                "title": f"{TITLE}, Europe in one file",
                "description": f"{europe['features']:,} polygons in {europe['row_groups']} row "
                               f"groups, sorted by country then along a Hilbert curve.",
                "roles": ["data"],
                "proj:code": "EPSG:3035",
                "file:size": europe["bytes"],
                "file:checksum": "1220" + europe["sha256"],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                     "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            **country_assets(countries),
            "pmtiles": {
                "href": f"{PUBLIC_BASE}/{tiles_path}",
                "type": PMTILES_TYPE,
                "title": f"{TITLE}, vector tiles",
                "description": f"PMTiles, zoom {tiles['min_zoom']} to {tiles['max_zoom']}, "
                               f"{tiles['tiles']:,} tiles. {bands_text(tiles)} Every feature "
                               f"carries Code_18, color (the official legend hex) and label. "
                               f"Built with tylertoo {tiles['tylertoo']}.",
                "roles": ["visual"],
                "file:size": tiles["bytes"],
                "file:checksum": "1220" + tiles["sha256"],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{tiles_path}",
                                     "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            "data-h3": h3_asset(h3),
            **{("style" if f == "default.json" else f"style-{f[:-5]}"): {
                "href": f"./styles/{f}", "type": STYLE_TYPE, "title": t, "roles": roles}
               for f, (t, roles) in STYLE_TITLES.items()},
            "thumbnail": {
                "href": "./thumbnail.png",
                "type": "image/png",
                "title": "Europe, dominant class per H3 cell, official CLC legend",
                "roles": ["thumbnail"],
                "file:size": THUMB.stat().st_size,
                "file:checksum": multihash(THUMB),
            },
        },
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "alternate", "href": f"{DATA_URL}/", "type": "text/html",
             "title": f"Browse the {RELEASE} files"},
            {"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
             "title": f"{TITLE}, vector tiles", "pmtiles:layers": tiles["layers"]},
        ],
    }


def build_root(col: dict, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (
            "Corine Land Cover 2018, the EEA's pan-European land cover inventory (Copernicus "
            "Land Monitoring Service), as GeoParquet for Europe and per country, and as "
            f"PMTiles in the official colours. Republished by Geomermaids. {ATTRIBUTION}."
        ),
        "version": RELEASE,
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            {"rel": "child", "href": f"./{col['id']}/collection.json",
             "type": "application/json", "title": col["title"]},
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK,
            {"rel": "related", "href": f"{DATA_URL}/ATTRIBUTION.txt", "type": "text/plain",
             "title": "Attribution and licence notice"},
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
            {"rel": "alternate", "href": SITE_URL, "type": "text/html",
             "title": "Geomermaids open datasets"},
        ],
    }


# ---------- markdown ----------

def access_section() -> str:
    return f"""\
One country, a small file (EPSG:3035, metres):

```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
SELECT Code_18, round(sum(Area_Ha)) AS ha
FROM read_parquet('{DATA_URL}/{PARTITION_KEY}=LU/{STEM}.parquet')
GROUP BY 1 ORDER BY 2 DESC;
```

All of Europe through the anonymous S3 endpoint `{S3_ENDPOINT}` (path-style,
empty credentials), or the single file `{DATA_URL}/{STEM}.parquet`:

```sql
SET s3_endpoint='{S3_ENDPOINT}'; SET s3_url_style='path';
SET s3_access_key_id=''; SET s3_secret_access_key='';
SELECT country, count(*) FROM read_parquet(
  's3://{BUCKET}/{DATASET_PREFIX}/{RELEASE}/{PARTITION_KEY}=*/{STEM}.parquet', hive_partitioning = true)
GROUP BY 1;
```

A lon/lat window has to be reprojected first; with `SET geometry_always_xy =
true`, `ST_Transform(ST_MakeEnvelope(...), 'OGC:CRS84', 'EPSG:3035')` gives the
box to compare with `bbox`."""


def tiles_section(tiles: dict) -> str:
    return f"""\
`{DATA_URL}/{tiles['file']}` ({tiles['bytes'] / 1e9:,.2f} GB) draws CLC on a web map with
the PMTiles protocol, zoom {tiles['min_zoom']} to {tiles['max_zoom']}. {bands_text(tiles)}

Every feature of both layers carries `Code_18`, `color` (the official legend hex)
and `label` (the class name), so `["get", "color"]` is a complete fill colour in
any client. The cells also carry `dominant_pct`, the `*_pct` share of each CLC
group, `coverage_pct` and their `h3` index. Styles: `styles/default.json` (the
official legend, a `match` on `label`), `styles/artificial.json` and
`styles/forest.json` (one group's share). Use the GeoParquet for areas and
exact boundaries."""


H3_COLUMNS = [
    ("h3", "H3 cell index, hex string."),
    ("h3_res", "H3 resolution of the cell."),
    ("Code_18", "Dominant CLC class: the class of most of the cell's sampled resolution 9 cells."),
    ("color", "Official legend colour of the dominant class (hex)."),
    ("label", "Name of the dominant class."),
    ("dominant_pct", "Share of the cell's CLC area in the dominant class, %."),
    ("artificial_pct", "Share in artificial surfaces (1xx), %."),
    ("agricultural_pct", "Share in agricultural areas (2xx), %."),
    ("forest_pct", "Share in forest and semi-natural areas (3xx), %."),
    ("wetland_pct", "Share in wetlands (4xx), %."),
    ("water_pct", "Share in water bodies (5xx), %."),
    ("coverage_pct", "Part of the cell CLC covers, %: lower on coasts and at the edge of the CLC "
                     "area. Shares are of the covered part."),
    ("geometry", "Cell outline, lon/lat (OGC:CRS84)."),
]


def h3_section(h3: dict) -> str:
    rows = "\n".join(f"| `{n}` | {d} |" for n, d in H3_COLUMNS)
    return f"""\
`{DATA_URL}/{h3['file']}` ({h3['bytes'] / 1e6:,.0f} MB) holds CLC aggregated on H3,
one row per cell of resolutions {', '.join(str(r) for r in h3['cells'])}. Every
resolution {h3['sample_res']} cell (about 0.1 km2) takes the class of the polygon holding
its centre; a coarser cell counts its resolution {h3['sample_res']} children, so its shares
are shares of area. Join it to anything indexed by H3:

```sql
INSTALL h3 FROM community; LOAD h3;
SELECT h3, label, forest_pct
FROM read_parquet('{DATA_URL}/{h3['file']}')
WHERE h3_res = 6 AND h3 = h3_h3_to_string(h3_latlng_to_cell(48.85, 2.35, 6));
```

| Column | Description |
|---|---|
{rows}"""


def legend_md() -> str:
    rows = "\n".join(f"| {c['code']} | {c['label']} | `{c['color']}` |" for c in legend())
    return f"| Code_18 | Class | Colour |\n|---|---|---|\n{rows}"


LICENSE_MD = f"""\
Corine Land Cover is produced by the European Environment Agency under the
Copernicus Land Monitoring Service, with full, open and free access under the
[Copernicus data policy]({POLICY_URL}) (Regulation (EU) No 1159/2013).
Credit it as: {ATTRIBUTION}. The `country` column uses FAO GAUL 2024 (CC BY 4.0);
FAO has not endorsed this use. Full notice: [ATTRIBUTION.txt]({DATA_URL}/ATTRIBUTION.txt)."""


def collection_readme(col: dict, main: dict, countries: dict, tiles: dict, h3: dict) -> str:
    europe = main["files"][0]
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    by_country = "\n".join(
        f"| {code} | {country_name(code)} | {m['files'][0]['features']:,} | "
        f"{m['files'][0]['bytes'] / 1e6:,.1f} MB |" for code, m in sorted(countries.items()))
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| Polygons | {europe['features']:,} |
| Europe file | `{europe['file']}`, {europe['bytes'] / 1e9:,.2f} GB, {europe['row_groups']} row groups |
| Per-country files | {len(countries)} under `{PARTITION_KEY}=<XX>/` |
| H3 cells | `{h3['file']}`, {h3['bytes'] / 1e6:,.0f} MB, resolutions {', '.join(str(r) for r in h3['cells'])} |
| Map tiles | `{tiles['file']}`, {tiles['bytes'] / 1e9:,.2f} GB, zoom {tiles['min_zoom']} to {tiles['max_zoom']} |
| CRS | ETRS89 / LAEA Europe (EPSG:3035) for the GeoParquet, Web Mercator tiles |
| Extent | {shared.fmt_bbox(col['extent']['spatial']['bbox'][0])} (lon/lat) |
| Source | [CLC 2018 v2020 20u1]({CLMS_URL}) |

## Access

{access_section()}

## H3 cells

{h3_section(h3)}

## Map tiles

{tiles_section(tiles)}

## Legend

{legend_md()}

## Schema

| Column | Type | Description |
|---|---|---|
{schema}

## Countries

| Code | Country | Polygons | File |
|---|---|---|---|
{by_country}

## Provenance

The EEA GeoPackage, converted to GeoParquet 2.0 with geometry, attributes and
CRS unchanged. [clc.py]({REPO_URL}/blob/main/scripts/datasets/clc.py) adds
`country`: each polygon goes whole to the GAUL 2024 country it overlaps most,
or to the nearest one. The Europe file is sorted by country then along a
Hilbert curve; each country file is a slice of it.
[clc_tiles.py]({REPO_URL}/blob/main/scripts/datasets/clc_tiles.py) derives the
H3 cells with DuckDB's h3 extension and builds the PMTiles with tylertoo, as
described above.

## License

{LICENSE_MD}
"""


def collection_agents(col: dict, tiles: dict) -> str:
    return f"""\
# {col['title']}: agent guide

Each row is one CLC 2018 land cover polygon, EPSG:3035 (metres), geometry
column `Shape`, class code in `Code_18` (VARCHAR, three digits).

## Access

{access_section()}

## Query tips

- Pick the file by the question: one country, `{PARTITION_KEY}=<XX>/{STEM}.parquet`;
  several, the glob with `hive_partitioning = true`; all of Europe, the
  single file.
- Areas: `Area_Ha` is the EEA's; `ST_Area(Shape)` is in m2 since the CRS is
  equal-area. Do not reproject before measuring.
- Class names are not in the parquet: join on `Code_18` against the legend
  in README.md or `{REPO_URL}/blob/main/meta/clc_legend.json`.
- Filter on `bbox` (EPSG:3035 metres) before any spatial function.
- Land cover per area unit: the `data-h3` asset, `{DATA_URL}/{STEM}_h3.parquet`,
  one row per H3 cell (filter on `h3_res`), shares in `*_pct`.
- For a map, use the `rel: pmtiles` link (layers {', '.join(f"`{l}`" for l in tiles['layers'])})
  and the `default` style asset, or fill with `["get", "color"]`. {bands_text(tiles)}
- Credit: {ATTRIBUTION}.
"""


def root_readme(root: dict, col: dict) -> str:
    return f"""\
# {root['title']}

{root['description']}

One collection: [{col['title']}](./{col['id']}/README.md), with the Europe
file, one file per country and the map tiles.

## Versions

CLC 2018 is a static release: a later CLC inventory would be published beside
it under its own year.

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(col: dict) -> str:
    return f"""\
# {CATALOG_ID}: agent guide

Corine Land Cover 2018 for Europe, one collection: `{col['id']}`
({col['id']}/AGENTS.md). GeoParquet in EPSG:3035, for Europe and per
country, and PMTiles for maps. Static release.

- One country: `{DATA_URL}/{PARTITION_KEY}=<XX>/{STEM}.parquet`.
- Europe: `{DATA_URL}/{STEM}.parquet`, or the collection's `partition:glob`
  through the anonymous S3 endpoint `{S3_ENDPOINT}`.
- Map: the collection's `rel: pmtiles` link and `default` style.
- Credit: {ATTRIBUTION}.
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> dict:
    main, countries = read_manifests(out_dir)
    tiles = {t["name"]: t for t in main.get("pmtiles", [])}.get(STEM)
    h3 = main.get("h3")
    if tiles is None or h3 is None:
        sys.exit("the release manifest has no PMTiles or H3 entry: run clc_tiles.py "
                 "(the workflow's tiles stage)")
    if not THUMB.is_file():
        sys.exit(f"missing {THUMB}: run the thumbnail command")
    updated = updated or now_rfc3339()
    columns, bbox = footer(europe_source(out_dir))

    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    col = build_collection(main, countries, tiles, h3, columns, bbox, updated)
    cdir = dest / col["id"]
    for fname, doc in styles(tiles, present_codes(europe_source(out_dir))).items():
        write_json(cdir / "styles" / fname, doc)
    for asset in col["assets"].values():
        if "style" in asset["roles"]:
            path = cdir / asset["href"]
            asset["file:size"] = path.stat().st_size
            asset["file:checksum"] = multihash(path)
    write_json(cdir / "collection.json", col)
    shutil.copyfile(THUMB, cdir / "thumbnail.png")
    (cdir / "README.md").write_text(collection_readme(col, main, countries, tiles, h3))
    (cdir / "AGENTS.md").write_text(collection_agents(col, tiles))

    root = build_root(col, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, col))
    (dest / "AGENTS.md").write_text(root_agents(col))
    return main


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="clc-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        main = build(out_dir, tree)
        print(f"  rendered {COLLECTION_ID}, {main['total_features']:,} polygons")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnail ----------

EUROPE = (-25.0, 34.0, 45.0, 72.0)  # mainland and the Atlantic islands; Turkey to 45 E


THUMB_RES = 5


def thumbnail(cells: Path) -> None:
    """Europe in the official colours: the H3 cells the default style draws
    at the first zooms, one resolution finer to suit the card's size."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    from thumbnails import BACKGROUND, frame

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    rows = con.execute(f"SELECT color, ST_AsWKB(geometry) FROM read_parquet('{cells}') "
                       f"WHERE h3_res = {THUMB_RES}").fetchall()
    polys, colors = [], []
    for color, wkb in rows:
        for part in shapely.get_parts(shapely.from_wkb(bytes(wkb))):
            polys.append(shapely.get_coordinates(part.exterior))
            colors.append(color)
    x0, y0, x1, y1, aspect = frame(*EUROPE)
    fig = plt.figure(figsize=(6, 4), dpi=100)
    ax = fig.add_axes((0, 0, 1, 1))
    ax.set_facecolor(BACKGROUND)
    fig.patch.set_facecolor(BACKGROUND)
    ax.add_collection(PolyCollection(polys, facecolors=colors, edgecolors=colors, linewidths=0.2))
    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.set_aspect(aspect)
    ax.axis("off")
    THUMB.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(THUMB, dpi=100, facecolor=BACKGROUND)
    plt.close(fig)
    print(f"  {len(rows):,} polygons -> {THUMB.relative_to(REPO)}")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp):
        sp.add_argument("--out-dir", type=Path, default=None,
                        help="release folder with the manifests (default: the published ones)")

    b = sub.add_parser("build", help="render the catalog tree")
    out_dir_arg(b)
    b.add_argument("--dest", type=Path, required=True)
    c = sub.add_parser("check", help="rashid pass over a rendered tree")
    c.add_argument("tree", type=Path)
    u = sub.add_parser("publish", help="build, check, upload to <remote>/catalog/")
    out_dir_arg(u)
    u.add_argument("--remote", required=True)
    u.add_argument("--dry-run", action="store_true")
    t = sub.add_parser("thumbnail", help=f"render {THUMB.relative_to(REPO)}")
    t.add_argument("--cells", type=Path, required=True, help="clc_2018_h3.parquet")

    args = p.parse_args()
    if args.cmd == "build":
        m = build(args.out_dir, args.dest)
        print(f"wrote {args.dest}/ ({m['total_features']:,} polygons)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    elif args.cmd == "publish":
        publish(args.out_dir, args.remote, dry_run=args.dry_run)
    elif args.cmd == "thumbnail":
        thumbnail(args.cells)


if __name__ == "__main__":
    main()
