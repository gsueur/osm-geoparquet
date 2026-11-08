#!/usr/bin/env python3
"""
The 2020 Census blocks sub-catalog of the census Portolan catalog
(census_catalog.py):

  census/catalog/tiger-2020-block/catalog.json                2020 Census blocks
  census/catalog/tiger-2020-block/tabblock20/collection.json     the blocks, one file per state
  census/catalog/tiger-2020-block/tabblock20-h3/collection.json  their counts on H3 cells, the map
  census/catalog/tiger-2020-block/tabblock20-h3/styles/default.json  population per cell

The `tabblock20` collection is partitioned like NAD's: a `partition:glob`
over the state files through the anonymous S3 endpoint, and one `data-<xx>`
asset per state. blocks.py's _manifest.json drives it (counts, sizes,
checksums, gate), local (--out-dir) or published. Until blocks.py's files
are published there is no manifest, and census_catalog.py renders the
catalog without this sub-catalog. Thumbnails: `census_catalog.py
thumbnails --blocks`.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.blocks import (  # noqa: E402
    APPORTIONMENT, H3_BANDS, LAYOUTS, PREFIX, SOURCE_DIR, SOURCE_PAGE, YEAR,
)
from datasets.census import STATES  # noqa: E402
from datasets.gaul_catalog import STYLE_TYPE, WEB_MAP_LINKS_EXT  # noqa: E402
from datasets.nad_catalog import extent_of  # noqa: E402
from datasets.pmtiles import PMTILES_TYPE  # noqa: E402

from catalog import (  # noqa: E402
    ALTERNATE_EXT, BUCKET, CONTACT_EMAIL, FILE_EXT, PARQUET_TYPE, PARTITION_EXT,
    PORTOLAN_SCHEMA, PUBLIC_BASE, REPO_URL, S3_ENDPOINT, SITE_URL, TABLE_EXT, VERSION_EXT,
    md_link, multihash, write_json,
)

URL = f"{PUBLIC_BASE}/census/{PREFIX}"
CATALOG_ID = f"tiger-{YEAR}-block"
TITLE = "2020 Census blocks, with their population and housing units"
ATTRIBUTION = ("U.S. Census Bureau, 2020 TIGER/Line Shapefiles (TABBLOCK20) and 2020 Census "
               "Redistricting Data (P.L. 94-171)")
PROJ_EXT = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
NAMES = {st: name for st, name in STATES.values()}
LICENSE_LINK = {
    "rel": "license",
    "href": "https://www.usa.gov/government-copyright",
    "type": "text/html",
    "title": "Works of the US federal government are not subject to copyright in the "
             "United States (17 U.S.C. 105)",
}
VIA_LINK = {"rel": "via", "href": f"{SOURCE_DIR}/", "type": "text/html",
            "title": "TABBLOCK20 shapefiles on census.gov, where they are published"}
ABOUT_LINK = {"rel": "describedby", "href": LAYOUTS, "type": "application/pdf",
              "title": "TIGER/Line 2020 technical documentation (record layouts)"}
PROVIDERS = [
    {
        "name": "U.S. Census Bureau, Geography Division",
        "description": "Delineates the 2020 Census tabulation blocks and publishes them as "
                       "TIGER/Line shapefiles with their 2020 Census counts. A US government "
                       "work, not subject to copyright in the United States.",
        "url": SOURCE_PAGE,
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted the state shapefiles to GeoParquet, summed the counts on H3 "
                       "cells and tiled them, and maintains and hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]
CAVEAT = ("Blocks are 2020 Census geography, the outlines the 2020 counts were tabulated on; "
          "they nest in the 2020 tracts and block groups (GEOID20), not in the 2026 TIGER/Line "
          "layers where those were revised. TIGER/Line boundaries are for statistical "
          "tabulation only, not legal land descriptions.")

COLUMN_DOCS = {
    "STATEFP20": "2020 Census state FIPS code.",
    "COUNTYFP20": "2020 Census county FIPS code.",
    "TRACTCE20": "2020 Census tract code.",
    "BLOCKCE20": "2020 Census tabulation block number; its first digit is the block group.",
    "GEOID20": "Block identifier: state, county, tract and block codes (15 characters). Its "
               "first 12 characters are the block group's GEOID, the first 11 the tract's.",
    "GEOIDFQ20": "Fully qualified GEOID (1010000US, the block summary level, followed by "
                 "GEOID20), used to join data tables from data.census.gov. Written from "
                 "GEOID20 for the states whose delivery lacks it (the manifest's `filled`).",
    "NAME20": "Block name (Block 1001).",
    "MTFCC20": "MAF/TIGER feature class code (G5040, tabulation block).",
    "UR20": "2020 urban/rural indicator: U urban, R rural.",
    "UACE20": "2020 urban area code, empty in a rural block.",
    "UATYPE20": "2020 urban area type, empty in a rural block, and empty for the states whose "
                "delivery lacks the column (the manifest's `filled`).",
    "VINTAGE": "Delivery vintage field of the later deliveries, empty in every state that "
               "has it and in the others.",
    "FUNCSTAT20": "Functional status (S statistical entity).",
    "ALAND20": "Land area, square metres.",
    "AWATER20": "Water area, square metres.",
    "INTPTLAT20": "Latitude of the internal point (text, as published).",
    "INTPTLON20": "Longitude of the internal point (text, as published).",
    "HOUSING20": "Housing units in the block, 2020 Census (P.L. 94-171, H1).",
    "POP20": "Total population of the block, 2020 Census (P.L. 94-171, P1). Census applied "
             "differential privacy below the state: block counts are noisy, sums over many "
             "blocks are not.",
    "geometry": "Block outline as native Parquet GEOMETRY (WKB), NAD83 (EPSG:4269), longitude "
                "and latitude, 2D.",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the GeoParquet "
            "covering. Filter on it to prune row groups.",
}
CELL_DOCS = {
    "h3": "H3 cell index, as a string.",
    "h3_res": "H3 resolution of the cell (4, 6 or 8).",
    "population": "POP20 summed over the blocks whose internal point is in the cell.",
    "housing_units": "HOUSING20 summed over the same blocks.",
    "blocks": "Blocks whose internal point is in the cell.",
    "state": "The state holding most of the cell's population (postal code).",
    "geometry": "The cell's outline as native Parquet GEOMETRY (WKB), lon/lat (OGC:CRS84).",
}
# Population classes per H3 resolution (cell areas differ by 49 between
# resolutions 4, 6 and 8), light to dark.
POP_STEPS = {4: [1_000, 10_000, 100_000, 1_000_000], 6: [100, 1_000, 10_000, 50_000],
             8: [10, 100, 1_000, 5_000]}
COLOURS = ["#ffffcc", "#a1dab4", "#41b6c4", "#2c7fb8", "#253494"]


def read_manifest(out_dir: Path | None) -> dict | None:
    local = out_dir / PREFIX / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    try:
        with urllib.request.urlopen(f"{URL}/_manifest.json") as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def source(out_dir: Path | None, file: str) -> str:
    local = out_dir / PREFIX / file if out_dir else None
    return str(local) if local and local.is_file() else f"{URL}/{file}"


def asset_s3(path: str) -> dict:
    return {"s3": {"href": f"s3://{BUCKET}/{path}", "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}


def gate_text(m: dict) -> str:
    g = m["gate"]
    return (f"Gate: every block of each state's shapefile is written, GEOID20 is unique, no "
            f"count is missing, and POP20 summed per state equals the state's 2020 resident "
            f"population in the Census Bureau's apportionment table, for all "
            f"{g['states_compared']} files. The 50 states and DC sum to "
            f"{g['united_states_population']:,}. The Island Areas "
            f"({', '.join(g['left_out'])}) are left out: their TABBLOCK20 files have no counts.")


def style(tiles: dict) -> dict:
    layers = []
    for lo, hi, res in H3_BANDS:
        fill = ["step", ["get", "population"], COLOURS[0]]
        for threshold, colour in zip(POP_STEPS[res], COLOURS[1:]):
            fill += [threshold, colour]
        layers.append({"id": f"cells-r{res}", "type": "fill", "source": "data",
                       "source-layer": f"cells_r{res}", "minzoom": lo, "maxzoom": hi + 1,
                       "paint": {"fill-color": fill, "fill-opacity": 0.8}})
    return {
        "version": 8,
        "name": "2020 Census population per H3 cell",
        "sources": {"data": {"type": "vector", "url": f"pmtiles://{URL}/{tiles['file']}",
                             "attribution": "U.S. Census Bureau, 2020 Census"}},
        "layers": layers,
    }


def blocks_collection(m: dict, columns, extent, thumbs: Path, updated: str) -> dict:
    undocumented = [c for c, _ in columns if c not in COLUMN_DOCS]
    if undocumented:
        sys.exit(f"tabblock20: no column doc for {undocumented}")
    assets = {}
    for f in m["files"]:
        path = f"census/{PREFIX}/{f['path']}"
        assets[f"data-{f['state'].lower()}"] = {
            "href": f"{PUBLIC_BASE}/{path}", "type": PARQUET_TYPE,
            "title": f"{NAMES[f['state']]} ({f['state']})",
            "description": f"{f['features']:,} blocks, {f['population']:,} people, "
                           f"{f['housing_units']:,} housing units, {f['row_groups']} row groups",
            "roles": ["data"], "file:size": f["bytes"], "file:checksum": "1220" + f["sha256"],
            "alternate": asset_s3(path)}
    thumb = thumbs / "tabblock20.png"
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT, VERSION_EXT,
                            ALTERNATE_EXT, PROJ_EXT],
        "id": "tabblock20",
        "title": "2020 Census tabulation blocks, with population and housing units",
        "description": (
            f"The {m['total_features']:,} tabulation blocks of the 2020 Census, the smallest "
            f"area the Census Bureau counts people in, with their 2020 Census total population "
            f"(POP20, {m['population']:,}) and housing units (HOUSING20, "
            f"{m['housing_units']:,}): one GeoParquet file per state, DC and Puerto Rico "
            f"(postal code). TIGER/Line's columns unchanged, NAD83 (EPSG:4269), Hilbert-sorted, "
            f"with a bbox covering. Read every state at once with the partition glob "
            f"{m['glob']} through the anonymous S3 endpoint {S3_ENDPOINT} (path-style, empty "
            f"credentials), or one state over HTTPS. {gate_text(m)} {CAVEAT} Credit: "
            f"{ATTRIBUTION}."),
        "keywords": ["Census blocks", "TABBLOCK20", "2020 Census", "population",
                     "housing units", "P.L. 94-171", "TIGER/Line", "US Census Bureau",
                     "GeoParquet"],
        "license": "other",
        "attribution": ATTRIBUTION,
        "version": YEAR,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [extent]},
                   "temporal": {"interval": [["2020-04-01T00:00:00Z", "2020-04-01T00:00:00Z"]]}},
        "proj:code": "EPSG:4269",
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [{"name": "state", "type": "string",
                            "description": "USPS postal code of the state, DC or PR. "
                                           "One file per state."}],
        "partition:file_count": len(m["files"]),
        "partition:glob": m["glob"],
        "table:row_count": m["total_features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": COLUMN_DOCS[n]}
                          for n, t in columns],
        "assets": {
            **assets,
            "thumbnail": {"href": "./thumbnail.png", "type": "image/png",
                          "title": "2020 population per H3 cell (resolution 4)",
                          "roles": ["thumbnail"], "file:size": thumb.stat().st_size,
                          "file:checksum": multihash(thumb)},
        },
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", "2020 Census blocks: README"),
            md_link("agents", "./AGENTS.md", "2020 Census blocks: agent guide"),
            LICENSE_LINK, VIA_LINK, ABOUT_LINK,
            {"rel": "related", "href": f"{URL}/ATTRIBUTION.txt", "type": "text/plain",
             "title": "Attribution and what was changed"},
            {"rel": "related", "href": APPORTIONMENT, "type": "text/csv",
             "title": "2020 resident population per state, the gate's reference"},
        ],
    }


def cells_collection(m: dict, columns, extent, thumbs: Path, updated: str) -> dict:
    h3, tiles = m["h3"], m["pmtiles"]
    cells_path = f"census/{PREFIX}/{h3['file']}"
    tiles_path = f"census/{PREFIX}/{tiles['file']}"
    counts = ", ".join(f"{n:,} at resolution {r}"
                       for r, n in sorted(h3["cells"].items(), key=lambda kv: int(kv[0])))
    bands = "; ".join(f"zoom {b['min_zoom']}-{b['max_zoom']}: resolution {b['h3_res']} "
                      f"(layer {b['layer']})" for b in tiles["bands"])
    undocumented = [c for c, _ in columns if c not in CELL_DOCS]
    if undocumented:
        sys.exit(f"tabblock20-h3: no column doc for {undocumented}")
    thumb = thumbs / "tabblock20-h3.png"
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT,
                            WEB_MAP_LINKS_EXT],
        "id": "tabblock20-h3",
        "title": "2020 Census population and housing units on H3 cells",
        "description": (
            f"The 2020 Census blocks' population and housing units summed on H3 cells at "
            f"resolutions 4, 6 and 8 ({counts} cells), each block counted whole in the cell "
            f"of its internal point, with the cell's outline and main state, and a PMTiles "
            f"map of them. A cell's sum is exact for the blocks it holds; a block across a "
            f"cell edge counts in one cell only. Credit: {ATTRIBUTION}."),
        "keywords": ["2020 Census", "population", "housing units", "H3", "population density"],
        "license": "other",
        "attribution": ATTRIBUTION,
        "version": YEAR,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [extent]},
                   "temporal": {"interval": [["2020-04-01T00:00:00Z", "2020-04-01T00:00:00Z"]]}},
        "table:row_count": sum(h3["cells"].values()),
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": CELL_DOCS[n]}
                          for n, t in columns],
        "assets": {
            "data": {"href": f"{PUBLIC_BASE}/{cells_path}", "type": PARQUET_TYPE,
                     "title": "Population and housing units per H3 cell, resolutions 4, 6 and 8",
                     "roles": ["data"], "file:size": h3["bytes"],
                     "file:checksum": "1220" + h3["sha256"], "alternate": asset_s3(cells_path)},
            "pmtiles": {"href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
                        "title": "Population map, vector tiles",
                        "description": f"PMTiles, one band per resolution ({bands}), "
                                       f"{tiles['tiles']:,} tiles; properties h3, population, "
                                       f"housing_units, blocks, state. Built with tylertoo "
                                       f"{tiles['tylertoo']}.",
                        "roles": ["visual"], "file:size": tiles["bytes"],
                        "file:checksum": "1220" + tiles["sha256"], "alternate": asset_s3(tiles_path)},
            "style": {"href": "./styles/default.json", "type": STYLE_TYPE,
                      "title": "Population per cell, light to dark", "roles": ["style", "default"]},
            "thumbnail": {"href": "./thumbnail.png", "type": "image/png",
                          "title": "2020 population per H3 cell (resolution 4)",
                          "roles": ["thumbnail"], "file:size": thumb.stat().st_size,
                          "file:checksum": multihash(thumb)},
        },
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", "2020 Census population on H3 cells: README"),
            md_link("agents", "./AGENTS.md", "2020 Census population on H3 cells: agent guide"),
            LICENSE_LINK, VIA_LINK,
            {"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
             "title": "Population map, vector tiles",
             "pmtiles:layers": [b["layer"] for b in tiles["bands"]]},
        ],
    }


def build_catalog(collections: list[dict], m: dict, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (
            f"The {m['total_features']:,} tabulation blocks of the 2020 Census with their "
            f"population ({m['population']:,}) and housing units ({m['housing_units']:,}), one "
            f"GeoParquet file per state, and the counts on H3 cells with a map. The official "
            f"counts below the block group: join them to a flood zone, a fire perimeter or a "
            f"buffer for the people and homes inside. Credit: {ATTRIBUTION}."),
        "version": YEAR,
        "updated": updated,
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            LICENSE_LINK, VIA_LINK,
        ],
    }


# ---------- markdown ----------

LICENSE_MD = (f"A work of the US federal government, not subject to copyright in the United "
              f"States (17 U.S.C. 105). Credit \"{ATTRIBUTION}\".")

INSIDE_SQL = """\
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
-- People and housing units in the blocks whose internal point is inside an area
-- (here a 1 km circle around a point in Austin, TX), from one state file.
WITH area AS (SELECT ST_Buffer(ST_Point(-97.7431, 30.2672), 0.01) AS g)
SELECT count(*) AS blocks, sum(POP20) AS people, sum(HOUSING20) AS housing_units
FROM read_parquet('{url}/state=TX/blocks.parquet') b, area
WHERE b.bbox.xmin < ST_XMax(area.g) AND b.bbox.xmax > ST_XMin(area.g)
  AND b.bbox.ymin < ST_YMax(area.g) AND b.bbox.ymax > ST_YMin(area.g)
  AND ST_Intersects(ST_Point(INTPTLON20::DOUBLE, INTPTLAT20::DOUBLE), area.g);"""


def state_table(m: dict) -> str:
    rows = "\n".join(
        f"| {f['state']} | {NAMES[f['state']]} | {f['features']:,} | {f['population']:,} | "
        f"{f['housing_units']:,} | {f['bytes'] / 1e6:,.1f} MB |" for f in m["files"])
    return ("| State | Name | Blocks | Population | Housing units | File |\n"
            "|---|---|---|---|---|---|\n" + rows)


def readme(title: str, description: str, body: str) -> str:
    return f"""\
# {title}

{description}

{body}

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def blocks_agents(m: dict) -> str:
    return f"""\
# tabblock20: agent guide

The 2020 Census tabulation blocks with POP20 (population) and HOUSING20
(housing units): one GeoParquet file per state at
`{URL}/state=<XX>/blocks.parquet` (postal code), NAD83 (EPSG:4269, lon/lat),
Hilbert-sorted with a `bbox` covering. All states at once:
`{m['glob']}` (anonymous S3 on `{S3_ENDPOINT}`, path style, empty credentials).

## Query tips

- People inside an area: sum POP20 over the blocks whose internal point
  (INTPTLON20, INTPTLAT20, text: cast to DOUBLE) is in it, or weight by the
  share of each block's area for a smaller area than a few blocks.
- Filter on `bbox` first: within a state an area lookup reads a few row groups.
- GEOID20's first 12 characters are the 2020 block group, the first 11 the
  tract: join to ACS (2020 geography) on those prefixes.
- Block counts are noisy (differential privacy below the state): report sums
  over many blocks, not one block's count.
- Credit: {ATTRIBUTION}.

```sql
{INSIDE_SQL.format(url=URL)}
```
"""


def cells_agents(m: dict) -> str:
    return f"""\
# tabblock20-h3: agent guide

2020 Census population and housing units per H3 cell:
`{URL}/{m['h3']['file']}`, one row per cell and resolution (`h3_res` 4, 6
or 8), with `population`, `housing_units`, `blocks`, `state` and the outline.

```sql
INSTALL httpfs; LOAD httpfs;
SELECT state, sum(population) FROM read_parquet('{URL}/{m['h3']['file']}')
WHERE h3_res = 4 GROUP BY 1 ORDER BY 2 DESC;
```

Each block counts whole in the cell of its internal point: a quick estimate
for an area of many cells, the block files for an exact answer.
"""


def catalog_agents(cat: dict, collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {cat['id']}: agent guide

{cat['description']}

{listing}
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, thumbs: Path, updated: str, footer) -> dict | None:
    """Write the sub-catalog under dest/tiger-2020-block/ and return it, or
    None when blocks.py's files are not published yet."""
    m = read_manifest(out_dir)
    if m is None:
        print(f"  no {URL}/_manifest.json: the blocks sub-catalog is left out", flush=True)
        return None
    if not m.get("complete"):
        sys.exit(f"{URL}/_manifest.json: a partial build (test states only); not catalogued")
    for name in ("tabblock20.png", "tabblock20-h3.png"):
        if not (thumbs / name).is_file():
            sys.exit(f"missing {thumbs / name}: run census_catalog.py thumbnails --blocks")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    cells = source(out_dir, m["h3"]["file"])
    extent = extent_of(con, cells)
    cdir = dest / CATALOG_ID
    first = source(out_dir, m["files"][0]["path"])
    # The state file's own columns: footer() would add the state= partition key.
    columns = [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
               con.execute(f"DESCRIBE SELECT * FROM read_parquet('{first}', "
                           "hive_partitioning = false)").fetchall()]
    blocks = blocks_collection(m, columns, extent, thumbs, updated)
    h3 = cells_collection(m, footer(cells), extent, thumbs, updated)
    write_json(cdir / "tabblock20-h3" / "styles" / "default.json", style(m["pmtiles"]))
    sfile = cdir / "tabblock20-h3" / "styles" / "default.json"
    h3["assets"]["style"]["file:size"] = sfile.stat().st_size
    h3["assets"]["style"]["file:checksum"] = multihash(sfile)
    for col, thumb, agents, body in (
            (blocks, "tabblock20.png", blocks_agents(m),
             f"## States\n\n{state_table(m)}\n\n## Query\n\n```sql\n{INSIDE_SQL.format(url=URL)}\n```"),
            (h3, "tabblock20-h3.png", cells_agents(m), "")):
        d = cdir / col["id"]
        write_json(d / "collection.json", col)
        (d / "thumbnail.png").write_bytes((thumbs / thumb).read_bytes())
        (d / "README.md").write_text(readme(col["title"], col["description"], body))
        (d / "AGENTS.md").write_text(agents)
    cat = build_catalog([blocks, h3], m, updated)
    write_json(cdir / "catalog.json", cat)
    listing = "\n".join(f"- [{c['title']}](./{c['id']}/README.md)" for c in (blocks, h3))
    (cdir / "README.md").write_text(readme(cat["title"], cat["description"], listing))
    (cdir / "AGENTS.md").write_text(catalog_agents(cat, [blocks, h3]))
    return cat


def thumbnails(out_dir: Path, thumbs: Path) -> None:
    """The resolution-4 cells over the conterminous US, coloured like the
    default style: the same picture for both collections."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    from thumbnails import BACKGROUND, frame

    CONUS = (-125.0, 24.0, -66.5, 49.5)
    m = read_manifest(out_dir)
    cells = source(out_dir, m["h3"]["file"])
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    rows = con.execute(f"SELECT population, ST_AsWKB(geometry) FROM read_parquet('{cells}') "
                       "WHERE h3_res = 4").fetchall()
    polys, fills = [], []
    for n, wkb in rows:
        part = shapely.from_wkb(bytes(wkb))
        if part.geom_type != "Polygon":
            continue
        polys.append(shapely.get_coordinates(part.exterior))
        fills.append(COLOURS[sum(n >= t for t in POP_STEPS[4])])
    thumbs.mkdir(parents=True, exist_ok=True)
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
    for name in ("tabblock20", "tabblock20-h3"):
        fig.savefig(thumbs / f"{name}.png", dpi=100, facecolor=BACKGROUND)
    plt.close(fig)
    print(f"  {len(polys):,} cells -> {thumbs}/tabblock20.png, tabblock20-h3.png")
