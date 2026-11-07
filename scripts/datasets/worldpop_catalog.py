#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for WorldPop on H3.

  worldpop/catalog/catalog.json                    root: WorldPop population on H3
  worldpop/catalog/worldpop_2025/collection.json   the 2025 release

One collection: the per-country files at H3 resolution 7 (the partition
extension and one `data-<iso3>` asset each), the world file at resolution 5
(`data`), and the PMTiles archive, registered as formats.md asks (a
`rel: pmtiles` link with `pmtiles:layers`, web-map-links v1.3.0, the file as
a `visual` asset), with a MapLibre style on population density. Counts,
sizes and checksums come from the release's _manifest.json, local
(--out-dir) or published. Spec: portolan-spec v0.2.0. Validator: rashid,
through scripts/catalog.py.

Usage:
  python3 scripts/datasets/worldpop_catalog.py build --dest build/worldpop-catalog [--out-dir out/worldpop]
  python3 scripts/datasets/worldpop_catalog.py check build/worldpop-catalog
  python3 scripts/datasets/worldpop_catalog.py publish --remote parquetry:parquetry/worldpop [--out-dir out/worldpop]
  uv run --project scripts --with matplotlib --with shapely \\
      python scripts/datasets/worldpop_catalog.py thumbnail --out-dir out/worldpop
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
from datasets.worldpop import (
    AGESEX_DIR, CITATION, DESCRIPTIONS, DOI, PAGE, POP_DIR, RELEASE, RES, STEM,
    TILE_LAYER, VERSION, WORLD_RES, YEAR,
)

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

DATASET_PREFIX = "worldpop"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
RELEASE_PATH = f"{RELEASE}/{YEAR}"
RELEASE_URL = f"{PUBLIC_DATA}/{RELEASE_PATH}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "worldpop-h3"
COLLECTION_ID = STEM
TITLE = "WorldPop population on H3"
THUMB = REPO / "catalog" / "thumbnails" / "worldpop" / f"{STEM}.png"
PARTITION_KEY = "country"
ATTRIBUTION = (f"WorldPop (www.worldpop.org), Global2 {RELEASE} {VERSION}, "
               "University of Southampton, CC BY 4.0, doi:10.5258/SOTON/WP00846")

LICENSE_LINK = {
    "rel": "license",
    "href": "https://creativecommons.org/licenses/by/4.0/",
    "type": "text/html",
    "title": "Creative Commons Attribution 4.0 International (CC BY 4.0)",
}
VIA_LINK = {
    "rel": "via",
    "href": DOI,
    "type": "text/html",
    "title": f"WorldPop Global2 {RELEASE} population, where it is published",
}
PROVIDERS = [
    {
        "name": "WorldPop, University of Southampton",
        "description": "Produces the Global2 gridded population estimates and their age and "
                       "sex structures, under CC BY 4.0.",
        "url": "https://www.worldpop.org/",
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Summed the 1 km rasters on H3 cells, assigned each cell a GAUL "
                       "country, tiled them, and maintains and hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]
SCOPE = (f"Estimates for {YEAR}, modelled from census data, not counts. Constrained: people "
         "are placed only where buildings are mapped.")
# A people-per-km2 ramp (ColorBrewer YlOrRd), for the default style. Under
# 1 person per km2 a cell is drawn faint.
DENSITY_STEPS = [(0, "#ffffcc"), (1, "#fed976"), (5, "#feb24c"), (25, "#fd8d3c"),
                 (100, "#fc4e2a"), (400, "#e31a1c"), (1500, "#b10026")]


# ---------- measured inputs ----------

def read_manifest(out_dir: Path | None) -> dict:
    local = out_dir / RELEASE_PATH / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    with urllib.request.urlopen(f"{RELEASE_URL}/_manifest.json") as r:
        return json.load(r)


def data_source(out_dir: Path | None, file: str) -> str:
    local = out_dir / RELEASE_PATH / file if out_dir else None
    return str(local) if local and local.is_file() else f"{RELEASE_URL}/{file}"


def footer(source: str) -> list[tuple[str, str]]:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    return [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
            con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()]


# ---------- style ----------

def style(tiles: dict) -> dict:
    fill = ["step", ["get", "density"], DENSITY_STEPS[0][1]]
    for value, colour in DENSITY_STEPS[1:]:
        fill += [value, colour]
    return {
        "version": 8, "name": "People per km2, H3 cells",
        "sources": {"data": {"type": "vector",
                             "url": f"pmtiles://{RELEASE_URL}/{tiles['file']}",
                             "attribution": ATTRIBUTION}},
        "layers": [{"id": "population", "type": "fill", "source": "data",
                    "source-layer": TILE_LAYER,
                    "paint": {"fill-color": fill, "fill-opacity": 0.8}}],
    }


# ---------- STAC ----------

def asset_s3(path: str) -> dict:
    return {"s3": {"href": f"s3://{BUCKET}/{path}", "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}


def country_assets(entries: list[dict]) -> dict:
    assets = {}
    for e in entries:
        path = f"{DATASET_PREFIX}/{RELEASE_PATH}/{e['path']}"
        assets[f"data-{e['country'].lower()}"] = {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": f"{e['name']} ({e['country']})",
            "description": f"Population of {e['name']}: {e['population']:,.0f} people, aged 65 "
                           f"and over {e['pop_65_plus']:,.0f}, on {e['features']:,} H3 "
                           f"resolution {RES} cells.",
            "roles": ["data"],
            "file:size": e["bytes"],
            "file:checksum": "1220" + e["sha256"],
            "alternate": asset_s3(path),
        }
    return assets


def column_docs(columns: list[tuple[str, str]]) -> list[dict]:
    missing = [n for n, _ in columns if n not in DESCRIPTIONS]
    if missing:
        sys.exit(f"no column doc for {missing}")
    return [{"name": n, "type": t, "description": DESCRIPTIONS[n]} for n, t in columns]


def build_collection(m: dict, columns: list[tuple[str, str]], updated: str) -> dict:
    entries = m["partitions"]["entries"]
    world, tiles = m["world"], m["pmtiles"][0]
    world_path = f"{DATASET_PREFIX}/{RELEASE_PATH}/{world['file']}"
    tiles_path = f"{DATASET_PREFIX}/{RELEASE_PATH}/{tiles['file']}"
    glob = f"s3://{BUCKET}/{DATASET_PREFIX}/{RELEASE_PATH}/{PARTITION_KEY}=*/{STEM}.parquet"
    t = m["totals"]
    x0, y0, x1, y1 = world["bbox"]
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT, VERSION_EXT,
                            ALTERNATE_EXT, WEB_MAP_LINKS_EXT],
        "id": COLLECTION_ID,
        "title": f"World population {YEAR} on H3 (WorldPop {RELEASE})",
        "description": (
            f"People per H3 cell in {YEAR}, from WorldPop's Global2 {RELEASE} constrained 1 km "
            f"estimates: {t['population']:,.0f} people, with age groups (0 to 14, 15 to 64, 65 "
            f"and over, 80 and over) and sex. One file per country at H3 resolution {RES} "
            f"(about 5 km2) under {PARTITION_KEY}=<ISO3>/ ({len(entries)} files, the data-<iso3> "
            f"assets and the partition glob {glob}), countries from GAUL 2024; one world file at "
            f"resolution {WORLD_RES} (the data asset); and a PMTiles archive for web maps. Each "
            "1 km pixel is counted in the cell holding its centre, so totals are WorldPop's. "
            f"Lon/lat with a bbox covering, read in place over HTTPS or the anonymous S3 endpoint "
            f"{S3_ENDPOINT}. Join on `h3` to anything indexed by H3, or on geometry. {SCOPE} "
            f"Credit: {ATTRIBUTION}."),
        "keywords": ["population", "WorldPop", "demographics", "age", "elderly", "H3", "people",
                     "exposure", "GeoParquet", "PMTiles"],
        "license": "CC-BY-4.0",
        "attribution": ATTRIBUTION,
        "version": f"{RELEASE} {VERSION}",
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {
            "spatial": {"bbox": [[x0, y0, x1, y1]]},
            "temporal": {"interval": [[f"{YEAR}-01-01T00:00:00Z", f"{YEAR}-12-31T23:59:59Z"]]},
        },
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [{"name": PARTITION_KEY, "type": "string",
                            "description": "GAUL 2024 iso3_code of the cell's country "
                                           "(x codes: disputed areas). One file per code."}],
        "partition:file_count": len(entries),
        "partition:glob": glob,
        "table:row_count": m["total_features"],
        "table:primary_geometry": "geometry",
        "table:columns": column_docs(columns),
        "assets": {
            "data": {
                "href": f"{PUBLIC_BASE}/{world_path}",
                "type": PARQUET_TYPE,
                "title": f"The world on H3 resolution {WORLD_RES}",
                "description": f"{world['features']:,} cells of about 250 km2, every person "
                               f"counted once; a cell across a border carries the country "
                               f"holding most of its people.",
                "roles": ["data"],
                "file:size": world["bytes"],
                "file:checksum": "1220" + world["sha256"],
                "alternate": asset_s3(world_path),
            },
            **country_assets(entries),
            "pmtiles": {
                "href": f"{PUBLIC_BASE}/{tiles_path}",
                "type": PMTILES_TYPE,
                "title": f"World population {YEAR}, vector tiles",
                "description": f"PMTiles, layer `{tiles['layer']}`, zoom {tiles['min_zoom']} to "
                               f"{tiles['max_zoom']}, {tiles['tiles']:,} tiles: H3 cells, "
                               + ", ".join(f"r{b['h3_res']} at z{b['min_zoom']}-{b['max_zoom']}"
                                           for b in tiles["bands"])
                               + f"; properties {', '.join(tiles['properties'])} (density in "
                               f"people per km2). Built with tylertoo {tiles['tylertoo']}.",
                "roles": ["visual"],
                "file:size": tiles["bytes"],
                "file:checksum": "1220" + tiles["sha256"],
                "alternate": asset_s3(tiles_path),
            },
            "style": {"href": "./styles/default.json", "type": STYLE_TYPE,
                      "title": "People per km2, yellow (1) to dark red (1,500 and more)",
                      "roles": ["style", "default"]},
            "thumbnail": {
                "href": "./thumbnail.png",
                "type": "image/png",
                "title": f"People per km2 in {YEAR}, H3 resolution {WORLD_RES}",
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
            {"rel": "related", "href": f"{RELEASE_URL}/ATTRIBUTION.txt", "type": "text/plain",
             "title": "Attribution and what was changed"},
            {"rel": "alternate", "href": f"{RELEASE_URL}/", "type": "text/html",
             "title": "Browse the files"},
            {"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
             "title": f"World population {YEAR}, vector tiles", "pmtiles:layers": [TILE_LAYER]},
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
            f"WorldPop's Global2 population estimates ({RELEASE}) summed on H3 cells, worldwide: "
            "people, age groups and sex, per country and for the world, as GeoParquet and "
            "PMTiles. Countries from GAUL 2024, so the cells join the administrative units on "
            f"parquetry. Republished by Geomermaids. {SCOPE} Credit: {ATTRIBUTION}."),
        "version": f"{RELEASE} {VERSION}",
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
WorldPop data are licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
Credit "{ATTRIBUTION}", or cite:

> {CITATION}

The `country` column uses FAO GAUL 2024 (CC BY 4.0); FAO has not endorsed this
use. Full notice: [ATTRIBUTION.txt]({RELEASE_URL}/ATTRIBUTION.txt)."""


def access_md() -> str:
    return f"""\
One country (a few MB), people aged 65 and over within 10 km of a point, Lyon:

```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
SET geometry_always_xy = true;
SELECT round(sum(pop_65_plus)) AS people_65_plus, round(sum(population)) AS people
FROM read_parquet('{RELEASE_URL}/{PARTITION_KEY}=FRA/{STEM}.parquet')
WHERE bbox.xmin <= 4.97 AND bbox.xmax >= 4.71 AND bbox.ymin <= 45.85 AND bbox.ymax >= 45.67
  AND ST_Distance_Sphere(ST_Point(lon, lat), ST_Point(4.835, 45.76)) <= 10000;
```

People per GAUL L1 unit of a country, joined on geometry (cell centres):

```sql
SELECT g.gaul1_name, round(sum(p.population)) AS people
FROM read_parquet('{RELEASE_URL}/{PARTITION_KEY}=KEN/{STEM}.parquet') p
JOIN read_parquet('{PUBLIC_BASE}/gaul/2024/{PARTITION_KEY}=KEN/L1.parquet') g
  ON ST_Contains(g.geometry, ST_Point(p.lon, p.lat))
GROUP BY 1 ORDER BY 2 DESC;
```

The world, by country, from the resolution {WORLD_RES} file, or every country file
through the anonymous S3 endpoint `{S3_ENDPOINT}` (path style, empty credentials):

```sql
SET s3_endpoint = '{S3_ENDPOINT}'; SET s3_url_style = 'path';
SET s3_access_key_id = ''; SET s3_secret_access_key = '';
SELECT country, round(sum(population)) AS people
FROM read_parquet('s3://{BUCKET}/{DATASET_PREFIX}/{RELEASE_PATH}/{PARTITION_KEY}=*/{STEM}.parquet',
                  hive_partitioning = true)
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
```

H3 joins: `h3` is the resolution {RES} cell as text; `h3_string_to_h3(h3)` gives the
index, `h3_cell_to_parent(..., r)` a coarser cell (DuckDB's community `h3`
extension)."""


def collection_readme(col: dict, m: dict) -> str:
    entries, world, tiles, t = (m["partitions"]["entries"], m["world"], m["pmtiles"][0],
                                m["totals"])
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    top = sorted(entries, key=lambda e: -e["population"])
    rows = "\n".join(f"| {e['country']} | {e['name']} | {e['population']:,.0f} | "
                     f"{e['pop_65_plus']:,.0f} | {e['features']:,} | {e['bytes'] / 1e6:,.1f} MB |"
                     for e in top)
    bands = ", ".join(f"r{b['h3_res']} at zoom {b['min_zoom']}-{b['max_zoom']}"
                      for b in tiles["bands"])
    by_ring = m["countries"].get("cells_by_search_degrees", {})
    offshore = sum(v for k, v in by_ring.items() if float(k) > 0)
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| People | {t['population']:,.0f} (0 to 14: {t['pop_0_14']:,.0f}; 15 to 64: {t['pop_15_64']:,.0f}; 65 and over: {t['pop_65_plus']:,.0f}) |
| Country files | {len(entries)} under `{PARTITION_KEY}=<ISO3>/`, {m['total_features']:,} cells, {m['partitions']['bytes'] / 1e6:,.0f} MB |
| World file | `{world['file']}`, {world['features']:,} cells, {world['bytes'] / 1e6:,.1f} MB |
| Map tiles | `{tiles['file']}`, {tiles['bytes'] / 1e6:,.0f} MB, {bands} |
| CRS | lon/lat, OGC:CRS84 |
| Source | WorldPop Global2 {RELEASE} {VERSION}, constrained, 1 km, {YEAR} ([doi]({DOI})) |

## Access

{access_md()}

## How the cells were made

Each WorldPop 1 km pixel goes whole to the H3 resolution {RES} cell holding its
centre (a cell holds about 5 pixels), so a cell's numbers are WorldPop's,
moved by at most half a cell, and every total is conserved: the build checks
each of the {len(m['bands'])} rasters' sums against the cells. The age columns
sum WorldPop's age bands (both sexes, `t_` rasters); `female` and `male` sum
the per-sex bands; neither need equal `population` exactly (separate rasters).
A cell's country is the GAUL 2024 L0 unit holding its centre; {offshore:,} cells whose
centre GAUL leaves in the sea went to the nearest unit. WorldPop's own
national totals use its own boundaries and differ at borders.

## Map tiles

`{RELEASE_URL}/{tiles['file']}`, tile layer `{tiles['layer']}`, properties
{', '.join(f"`{p}`" for p in tiles['properties'])}; `density` is people per km2.
`styles/default.json` colours density from yellow (1 person per km2) to dark
red (1,500 and more), ColorBrewer YlOrRd.

## Columns

| Column | Type | Description |
|---|---|---|
{schema}

## Countries

| ISO3 | Name | People | 65 and over | Cells | File |
|---|---|---|---|---|---|
{rows}

## Sources

Rasters from {POP_DIR}/ (total) and {AGESEX_DIR}/ (age and sex),
{len(m['bands'])} files; WorldPop's page: {PAGE}. Built by
[worldpop.py]({REPO_URL}/blob/main/scripts/datasets/worldpop.py).

## License

{LICENSE_MD}
"""


def collection_agents(col: dict) -> str:
    return f"""\
# {col['title']}: agent guide

People per H3 cell in {YEAR} (WorldPop {RELEASE}), lon/lat, geometry column
`geometry` (the hexagon), key `h3`.

## Access

{access_md()}

## Query tips

- One country: `{RELEASE_URL}/{PARTITION_KEY}=<ISO3>/{STEM}.parquet` (GAUL iso3 codes; the
  collection's data-<iso3> assets list them). Filter on `bbox` first.
- Across borders: the partition glob with `hive_partitioning = true`, or the world
  file at resolution {WORLD_RES} for coarse questions (one row per cell, people counted once).
- A radius: compare `ST_Distance_Sphere(ST_Point(lon, lat), point)` with metres
  (`SET geometry_always_xy = true`). Cells are about 2.5 km across: answers are
  good to a cell, not to a street.
- Columns: `population`, `pop_0_14`, `pop_15_64`, `pop_65_plus`, `pop_80_plus`,
  `female`, `male`, `area_km2` (density = population / area_km2), `lon`/`lat`
  (the cell centre: use it rather than ST_Centroid, which a cell cut at the
  antimeridian puts far away).
- Estimates, not counts. Credit: {ATTRIBUTION}.
"""


def root_readme(root: dict, col: dict) -> str:
    return f"""\
# {root['title']}

{root['description']}

| Collection | Files |
|---|---|
| [{col['title']}](./{col['id']}/README.md) | `{RELEASE_URL}/{PARTITION_KEY}=<ISO3>/{STEM}.parquet`, `{STEM}_world_r{WORLD_RES}.parquet`, `{STEM}.pmtiles` |

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(col: dict) -> str:
    return f"""\
# {CATALOG_ID}: agent guide

WorldPop population on H3 cells. One collection: `{col['id']}`, {col['id']}/AGENTS.md.

Layout: `{RELEASE_URL}/{PARTITION_KEY}=<ISO3>/{STEM}.parquet` (resolution {RES}) and
`{RELEASE_URL}/{STEM}_world_r{WORLD_RES}.parquet`; anonymous S3 at
`s3://{BUCKET}/{DATASET_PREFIX}/{RELEASE_PATH}/` on `{S3_ENDPOINT}`. Lon/lat. Countries
are GAUL 2024 iso3 codes, so the files join gaul/. Credit: {ATTRIBUTION}.
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    if not THUMB.is_file():
        sys.exit(f"missing {THUMB}: run the thumbnail command")
    m = read_manifest(out_dir)
    first = m["partitions"]["entries"][0]["path"]
    col = build_collection(m, footer(data_source(out_dir, first)), updated)
    cdir = dest / col["id"]
    write_json(cdir / "styles" / "default.json", style(m["pmtiles"][0]))
    asset = col["assets"]["style"]
    asset["file:size"] = (cdir / asset["href"]).stat().st_size
    asset["file:checksum"] = multihash(cdir / asset["href"])
    write_json(cdir / "collection.json", col)
    shutil.copyfile(THUMB, cdir / "thumbnail.png")
    (cdir / "README.md").write_text(collection_readme(col, m))
    (cdir / "AGENTS.md").write_text(collection_agents(col))
    root = build_root(col, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, col))
    (dest / "AGENTS.md").write_text(root_agents(col))
    return m["total_features"]


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="worldpop-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        total = build(out_dir, tree)
        print(f"  rendered 1 collection, {total:,} cells")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnail ----------

def thumbnail(out_dir: Path) -> None:
    """The world file's cells coloured on density, as the default style."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    from thumbnails import BACKGROUND, frame

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    world = out_dir / RELEASE_PATH / f"{STEM}_world_r{WORLD_RES}.parquet"
    rows = con.execute(f"SELECT population / area_km2, ST_AsWKB(geometry) "
                       f"FROM read_parquet('{world}')").fetchall()
    polys, fills = [], []
    for density, wkb in rows:
        colour = DENSITY_STEPS[0][1]
        for value, c in DENSITY_STEPS:
            if density >= value:
                colour = c
        for part in shapely.get_parts(shapely.from_wkb(bytes(wkb))):
            polys.append(shapely.get_coordinates(part.exterior))
            fills.append(colour)
    x0, y0, x1, y1, aspect = frame(-170, -56, 180, 74)
    fig = plt.figure(figsize=(6, 4), dpi=100)
    ax = fig.add_axes((0, 0, 1, 1))
    ax.set_facecolor(BACKGROUND)
    fig.patch.set_facecolor(BACKGROUND)
    # Edges in the fill colour close the hairline gaps between cells.
    ax.add_collection(PolyCollection(polys, facecolors=fills, edgecolors=fills, linewidths=0.3))
    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.set_aspect(aspect)
    ax.axis("off")
    THUMB.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(THUMB, dpi=100, facecolor=BACKGROUND)
    plt.close(fig)
    print(f"  {len(rows):,} cells -> {THUMB.relative_to(REPO)}")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp, required=False):
        sp.add_argument("--out-dir", type=Path, default=None, required=required,
                        help="worldpop.py's --out-dir (default: read the published files)")

    b = sub.add_parser("build")
    out_dir_arg(b)
    b.add_argument("--dest", type=Path, required=True)
    c = sub.add_parser("check")
    c.add_argument("tree", type=Path)
    u = sub.add_parser("publish")
    out_dir_arg(u)
    u.add_argument("--remote", required=True)
    u.add_argument("--dry-run", action="store_true")
    t = sub.add_parser("thumbnail")
    out_dir_arg(t, required=True)
    args = p.parse_args()
    if args.cmd == "build":
        print(f"wrote {args.dest}/ ({build(args.out_dir, args.dest):,} cells)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    elif args.cmd == "publish":
        publish(args.out_dir, args.remote, dry_run=args.dry_run)
    else:
        thumbnail(args.out_dir)


if __name__ == "__main__":
    main()
