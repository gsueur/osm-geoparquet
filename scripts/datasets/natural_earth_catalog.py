#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for Natural Earth.

  natural-earth/catalog/catalog.json                       root: Natural Earth
  natural-earth/catalog/<scale>/catalog.json               one sub-catalog per scale
  natural-earth/catalog/<scale>/<layer>/collection.json    one per layer

natural_earth.py's manifest drives it (counts, sizes, checksums, sources,
repairs, the archives). Each collection has the layer's GeoParquet (`data`),
the layer's own PMTiles registered as formats.md asks (a `rel: pmtiles` link
with `pmtiles:layers`, web-map-links v1.3.0, and the file as a `visual`
asset), a default MapLibre style over it, the scale's basemap archive and
its style as further assets, and a thumbnail. The thumbnails are committed under catalog/thumbnails/natural-earth/
(the release is static) and rendered by the `thumbnails` command.
Spec: portolan-spec v0.2.0. Validator: rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/natural_earth_catalog.py thumbnails --out-dir out/natural-earth/v5.1.2
  python3 scripts/datasets/natural_earth_catalog.py build --dest build/ne-catalog [--out-dir out/natural-earth/v5.1.2]
  python3 scripts/datasets/natural_earth_catalog.py check build/ne-catalog
  python3 scripts/datasets/natural_earth_catalog.py publish --remote parquetry:parquetry/natural-earth [--out-dir ...]
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
    md_link,
    multihash,
    now_rfc3339,
    write_json,
)
from datasets.gaul import GROUP_BYTES
from datasets.gaul_catalog import STYLE_TYPE, WEB_MAP_LINKS_EXT
from datasets.natural_earth import (
    CDN,
    COLORS,
    CREDIT,
    DATASET_PREFIX,
    LAYER_PROP,
    PUBLIC,
    RELEASE_DATE,
    SCALES,
    SITE,
    TERMS,
    TILE_LAYER,
    VERSION,
)
from datasets.pmtiles import PMTILES_TYPE
from extent import lon_intervals, wrapped_bbox

PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "natural-earth"
TITLE = f"Natural Earth {VERSION}"
THUMBS = REPO / "catalog" / "thumbnails" / "natural-earth"
INTERVAL = [f"{RELEASE_DATE}T00:00:00Z", f"{RELEASE_DATE}T00:00:00Z"]

LICENSE_LINK = {"rel": "license", "href": TERMS, "type": "text/html",
                "title": "Natural Earth terms of use: public domain"}
VIA_LINK = {"rel": "via", "href": SITE, "type": "text/html",
            "title": "naturalearthdata.com, where Natural Earth is published"}
PROVIDERS = [
    {
        "name": "Natural Earth",
        "description": "Public domain map data at 1:10m, 1:50m and 1:110m, made by volunteers "
                       "and supported by NACIS, the North American Cartographic Information "
                       "Society.",
        "url": SITE,
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted the layers to GeoParquet, tiled them, and maintains and "
                       "hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]
LICENSE_MD = (
    f"Public domain ([terms of use]({TERMS})): no permission is needed to use, modify or "
    f"redistribute it. Natural Earth asks for, but does not require, the credit "
    f"\"{CREDIT}\".")

# What a reader of each layer should know, quoted in its README and agent guide.
NOTES = {
    "admin_0_countries": [
        "`ISO_A2` and `ISO_A3` are `-99` for 22 units at 10m, France and Norway among them "
        "(their overseas or Antarctic parts make them not match ISO). `ISO_A2_EH` and "
        "`ISO_A3_EH` hold the code a join on ISO wants (`FR`, `FRA`); they are `-99` only "
        "for units ISO does not code (Somaliland, N. Cyprus, Bir Tawil...).",
        "`ADM0_A3` is Natural Earth's own code and is set on every row; it differs from "
        "`ISO_A3` on 26 rows at 10m (`SDS` for South Sudan, `KOS` for Kosovo).",
        "Boundaries follow Natural Earth's de facto policy (the line of actual control). "
        "Natural Earth publishes point-of-view variants for disputed areas; they are not "
        "included here.",
        "Seven units cross the antimeridian (Russia, Fiji, the United States, New Zealand, "
        "Kiribati, Antarctica, the US Minor Outlying Islands): their `bbox` spans every "
        "longitude, so a bbox filter anywhere on Earth reads them. Results stay correct.",
    ],
    "admin_1_states_provinces": [
        "Coverage depends on the scale: every country at 10m (253), 9 large countries at 50m, "
        "and the United States only at 110m (51 rows, the states and DC).",
        "`iso_3166_2` holds the subdivision code (`US-MA`), `adm0_a3` the country.",
    ],
    "populated_places": [
        "`POP_MAX` is the metropolitan population estimate, `POP_MIN` the city proper. "
        "`SCALERANK` and `MIN_ZOOM` say how important a place is (lower is more important).",
        "`ADM0CAP` is 1 for a national capital; `FEATURECLA` tells capitals, admin-1 "
        "capitals and other places apart.",
    ],
    "rivers_lake_centerlines": [
        "At 50m one row, the Loire, has no geometry in Natural Earth's shapefile; it is "
        "kept, so the row count is Natural Earth's, and it is not in the tiles.",
    ],
    "ocean": ["One or two polygons covering the whole ocean: read it whole, there is "
              "nothing to prune."],
    "time_zones": ["Nautical time zones over the sea and the civil zones over land: "
                   "`zone` is the UTC offset in hours, `time_zone` its label."],
}


# ---------- measured inputs ----------

def read_manifest(out_dir: Path | None) -> dict:
    if out_dir is not None:
        return json.loads((out_dir / "_manifest.json").read_text())
    with urllib.request.urlopen(f"{PUBLIC}/_manifest.json") as r:
        return json.load(r)


def data_source(out_dir: Path | None, file: str) -> str:
    return str(out_dir / file) if out_dir is not None else f"{PUBLIC}/{file}"


def footer(con, source: str) -> list[tuple[str, str]]:
    return [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
            con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()]


def lonlat_extent(con, source: str, entry: dict) -> list[float]:
    """The layer's extent as STAC wants it: west > east when it crosses the
    antimeridian (extent.py). A world layer comes out [-180, ..., 180, ...]."""
    _, y0, _, y1 = entry["bbox"]
    return [round(v, 6) for v in
            wrapped_bbox(lon_intervals(con, f"read_parquet('{source}')"), y0, y1)]


# ---------- styles ----------

def layer_style(entry: dict) -> dict:
    """The layer over its own archive, tile layer named after the layer."""
    layer, color = entry["layer"], COLORS[entry["layer"]]
    types = set(entry["geometry_types"])
    src = {"source": "ne", "source-layer": entry["pmtiles"]["layer"]}
    out = []
    if types & {"Polygon", "MultiPolygon"}:
        out += [
            {"id": f"{layer}-fill", "type": "fill", **src,
             "paint": {"fill-color": color, "fill-opacity": 0.45}},
            {"id": f"{layer}-outline", "type": "line", **src,
             "paint": {"line-color": color,
                       "line-width": ["interpolate", ["linear"], ["zoom"], 0, 0.4, 8, 1.4]}},
        ]
    if types & {"LineString", "MultiLineString"}:
        out.append({"id": f"{layer}-line", "type": "line", **src,
                    "paint": {"line-color": color,
                              "line-width": ["interpolate", ["linear"], ["zoom"], 0, 0.5, 8, 1.6]}})
    if types & {"Point", "MultiPoint"}:
        out.append({"id": f"{layer}-point", "type": "circle", **src,
                    "paint": {"circle-color": color, "circle-stroke-color": "#ffffff",
                              "circle-stroke-width": 0.5,
                              "circle-radius": ["interpolate", ["linear"], ["zoom"], 0, 1.5, 8, 4]}})
    return {
        "version": 8, "name": f"{entry['title']} ({entry['scale']})",
        "sources": {"ne": {"type": "vector",
                           "url": f"pmtiles://{PUBLIC}/{entry['pmtiles']['path']}",
                           "attribution": CREDIT}},
        "layers": out,
    }


# ---------- STAC ----------

def collection_id(entry: dict) -> str:
    return f"ne_{entry['scale']}_{entry['layer']}"


def build_collection(entry: dict, tiles: dict, extent: list[float],
                     columns: list[tuple[str, str]], updated: str) -> dict:
    scale, title = entry["scale"], f"{entry['title']} ({entry['scale']})"
    path = f"{DATASET_PREFIX}/v{VERSION}/{entry['file']}"
    own = entry["pmtiles"]
    own_path = f"{DATASET_PREFIX}/v{VERSION}/{own['path']}"
    own_props = ("every column" if own["all_properties"] else
                 "the naming columns (" + ", ".join(f"`{c}`" for c in own["properties"]) +
                 "): with every column, tiles went over the size cap")
    tiles_path = f"{DATASET_PREFIX}/v{VERSION}/{tiles['path']}"
    style_path = f"{DATASET_PREFIX}/v{VERSION}/{tiles['style']['file']}"
    nulls = (f" {entry['null_geometries']} row without geometry is kept." if
             entry["null_geometries"] else "")
    repaired = (f" {entry['repaired']} invalid polygon(s) repaired with ST_MakeValid." if
                entry["repaired"] else "")
    description = (
        f"Natural Earth's {entry['title'].lower()} at {SCALES[scale]}, release {VERSION} "
        f"(this layer last changed in {entry['source']['ne_version']}): {entry['features']:,} "
        f"rows, every column as Natural Earth publishes it. One GeoParquet 2.0 file in lon/lat "
        f"with a bbox covering, Hilbert-sorted, read in place over HTTPS or the anonymous S3 "
        f"endpoint {S3_ENDPOINT}.{nulls}{repaired} Public domain; {CREDIT}.")
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, ALTERNATE_EXT,
                            WEB_MAP_LINKS_EXT],
        "id": collection_id(entry),
        "title": title,
        "description": description,
        "keywords": ["Natural Earth", entry["title"], scale, entry["category"], "world",
                     "GeoParquet", "PMTiles"],
        "license": "other",
        "attribution": CREDIT,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [extent]}, "temporal": {"interval": [INTERVAL]}},
        "table:row_count": entry["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t} for n, t in columns],
        "assets": {
            "data": {
                "href": f"{PUBLIC_BASE}/{path}",
                "type": PARQUET_TYPE,
                "title": title,
                "description": f"{entry['features']:,} rows in {entry['row_groups']} row "
                               f"group(s) of at most {GROUP_BYTES >> 20} MiB of geometry.",
                "roles": ["data"],
                "file:size": entry["bytes"],
                "file:checksum": "1220" + entry["sha256"],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                     "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            "pmtiles": {
                "href": f"{PUBLIC_BASE}/{own_path}",
                "type": PMTILES_TYPE,
                "title": f"{title}, vector tiles",
                "description": f"This layer alone in one PMTiles archive, tile layer "
                               f"`{own['layer']}`, zoom {own['min_zoom']} to "
                               f"{own['max_zoom']}, {own['tiles']:,} tiles, {own_props}. "
                               f"Clipped to the Web Mercator band"
                               + (f", which leaves out {own['outside_web_mercator']} row(s) "
                                  f"wholly outside it" if own.get("outside_web_mercator")
                                  else "")
                               + f". Built with tylertoo "
                               f"{own['tylertoo']}.",
                "roles": ["visual"],
                "file:size": own["bytes"],
                "file:checksum": "1220" + own["sha256"],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{own_path}",
                                     "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            "pmtiles-basemap": {
                "href": f"{PUBLIC_BASE}/{tiles_path}",
                "type": PMTILES_TYPE,
                "title": f"Natural Earth {scale} basemap, vector tiles",
                "description": f"Every layer of the {scale} scale in one PMTiles archive, tile "
                               f"layer `{TILE_LAYER}`, zoom {tiles['min_zoom']} to "
                               f"{tiles['max_zoom']}, {tiles['tiles']:,} tiles. This layer's "
                               f"features carry `{LAYER_PROP}` = `{entry['layer']}`. Clipped "
                               f"to the Web Mercator band. Built with tylertoo "
                               f"{tiles['tylertoo']}.",
                "roles": ["visual"],
                "file:size": tiles["bytes"],
                "file:checksum": "1220" + tiles["sha256"],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{tiles_path}",
                                     "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            "style": {"href": "./styles/default.json", "type": STYLE_TYPE,
                      "title": "This layer, over its own archive",
                      "roles": ["style", "default"]},
            "style-basemap": {
                "href": f"{PUBLIC_BASE}/{style_path}", "type": STYLE_TYPE,
                "title": f"The {scale} world basemap: water, land, boundaries, rivers, places",
                "roles": ["style"],
                "file:size": tiles["style"]["bytes"],
                "file:checksum": "1220" + tiles["style"]["sha256"],
            },
            "thumbnail": {"href": "./thumbnail.png", "type": "image/png",
                          "title": f"{title}, overview", "roles": ["thumbnail"]},
        },
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{title}: README"),
            md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "derived_from", "href": entry["source"]["url"], "type": "application/zip",
             "title": f"The Natural Earth zip this layer was converted from"},
            {"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{own_path}", "type": PMTILES_TYPE,
             "title": f"{title}, vector tiles", "pmtiles:layers": [own["layer"]]},
            {"rel": "related", "href": f"{PUBLIC}/ATTRIBUTION.txt", "type": "text/plain",
             "title": "Attribution, source and what was changed"},
        ],
    }


def build_scale(scale: str, collections: list[dict], tiles: dict, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": f"natural-earth-{scale}",
        "title": f"Natural Earth {VERSION}, {scale} ({SCALES[scale]})",
        "description": (
            f"Natural Earth at {SCALES[scale]}: {len(collections)} layers, one collection each "
            f"with its own PMTiles archive, and one PMTiles basemap of them all (zoom "
            f"{tiles['min_zoom']} to {tiles['max_zoom']}). Public domain; {CREDIT}."),
        "updated": updated,
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json",
               "type": "application/json", "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"Natural Earth {scale}: README"),
            md_link("agents", "./AGENTS.md", f"Natural Earth {scale}: agent guide"),
            LICENSE_LINK,
        ],
    }


def build_root(scales: list[dict], total: int, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (
            f"Natural Earth {VERSION}, the public domain world map data, as GeoParquet and "
            f"PMTiles: countries, states and provinces, populated places, urban areas, "
            f"airports, ports, time zones, coastline, land, ocean, rivers, lakes and "
            f"graticules at 1:10m, 1:50m and 1:110m. {total} collections in {len(scales)} "
            f"scales, every column unchanged, lon/lat, a PMTiles archive per layer and one "
            f"PMTiles world basemap per scale. {CREDIT}."),
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{s['_scale']}/catalog.json",
               "type": "application/json", "title": s["title"]} for s in scales),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


# ---------- markdown ----------

LYON = (4.8357, 45.7640)
# A box each layer has features in, at every scale (checked on the build):
# Provence for most, and where the 50m and 110m files have rows for the rest.
EXAMPLE_AREAS = {
    None: ("Provence", (3.9, 42.3, 6.9, 44.3)),
    "lakes": ("the Great Lakes", (-92.5, 41.0, -76.0, 49.5)),
    "admin_1_states_provinces": ("Massachusetts", (-73.5, 41.2, -69.9, 42.9)),
    "rivers_lake_centerlines": ("the Mississippi valley", (-95.0, 35.0, -88.0, 45.0)),
}


def access_md(entry: dict) -> str:
    url = f"{PUBLIC}/{entry['file']}"
    lon, lat = LYON
    if entry["layer"] == "populated_places":
        return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
-- the ten largest places within 200 km of Lyon
SELECT * FROM (
  SELECT NAME, ADM0NAME, POP_MAX,
         2 * 6371 * asin(sqrt(pow(sin(radians(ST_Y(geometry) - {lat}) / 2), 2)
           + cos(radians({lat})) * cos(radians(ST_Y(geometry)))
           * pow(sin(radians(ST_X(geometry) - {lon}) / 2), 2))) AS km
  FROM read_parquet('{url}')
  WHERE bbox.xmin <= {lon + 2.6} AND bbox.xmax >= {lon - 2.6}
    AND bbox.ymin <= {lat + 1.8} AND bbox.ymax >= {lat - 1.8})
WHERE km <= 200
ORDER BY POP_MAX DESC LIMIT 10;
```"""
    if entry["layer"] == "graticules_10":
        return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
-- the whole layer: {entry['features']} lines
SELECT * EXCLUDE (bbox) FROM read_parquet('{url}');
```"""
    where, (x0, y0, x1, y1) = EXAMPLE_AREAS.get(entry["layer"], EXAMPLE_AREAS[None])
    return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
-- the features that touch a box over {where}
SELECT * EXCLUDE (geometry, bbox)
FROM read_parquet('{url}')
WHERE bbox.xmin <= {x1} AND bbox.xmax >= {x0}
  AND bbox.ymin <= {y1} AND bbox.ymax >= {y0}
  AND ST_Intersects(geometry, ST_MakeEnvelope({x0}, {y0}, {x1}, {y1}));
```"""


def layer_notes(entry: dict) -> list[str]:
    """NOTES for the layer; the rivers note is about the 50m file only."""
    if entry["layer"] == "rivers_lake_centerlines" and not entry["null_geometries"]:
        return []
    return NOTES.get(entry["layer"], [])


def notes_md(entry: dict) -> str:
    notes = layer_notes(entry)
    return ("\n## Notes\n\n" + "\n".join(f"- {n}" for n in notes) + "\n") if notes else ""


def collection_readme(col: dict, entry: dict, tiles: dict) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` |" for c in col["table:columns"])
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.2f} MB, sha256 `{entry['sha256'][:12]}...` |
| Rows | {entry['features']:,} |
| Geometry | {', '.join(entry['geometry_types'])}, lon/lat (OGC:CRS84) |
| Map tiles | `{entry['pmtiles']['path']}`, {entry['pmtiles']['bytes'] / 1e6:,.2f} MB, zoom {entry['pmtiles']['min_zoom']} to {entry['pmtiles']['max_zoom']}, tile layer `{entry['pmtiles']['layer']}`, {'every column' if entry['pmtiles']['all_properties'] else 'the naming columns'} |
| Basemap | `{tiles['path']}`, every layer of the scale, zoom {tiles['min_zoom']} to {tiles['max_zoom']}, filter `{LAYER_PROP}` = `{entry['layer']}` |
| Source | `{entry['source']['url']}`, Natural Earth {entry['source']['ne_version']} |
{notes_md(entry)}
## Access

{access_md(entry)}

The same file at `s3://{BUCKET}/{DATASET_PREFIX}/v{VERSION}/{entry['file']}` on the
anonymous S3 endpoint `{S3_ENDPOINT}`.

## Schema

| Column | Type |
|---|---|
{schema}

## Provenance

Natural Earth's zip, from {CDN}.
[natural_earth.py]({REPO_URL}/blob/main/scripts/datasets/natural_earth.py) reads it
with DuckDB, checks the row count against the shapefile's index and the .prj,
keeps every column and value, repairs invalid polygons with ST_MakeValid
(counted in `_manifest.json`), adds the `bbox` covering, sorts along a
Hilbert curve and cuts row groups at 4 MiB of geometry. Each file's footer,
bbox and groups are verified before upload. The PMTiles come from the same
rows with tylertoo.

## License

{LICENSE_MD}
"""


def collection_agents(col: dict, entry: dict) -> str:
    notes = "\n".join(f"- {n}" for n in layer_notes(entry))
    return f"""\
# {col['title']}: agent guide

{entry['features']:,} rows; geometry column `geometry` in lon/lat, `bbox` covering.
Columns are Natural Earth's, case included (DuckDB matches names without case).

## Access

{access_md(entry)}

## Query tips

- Filter on `bbox` before an exact predicate; the file is Hilbert-sorted.
- Distances in lon/lat need a great-circle formula (above) or a projection.
{notes}
- Credit: "{CREDIT}" (asked, not required).
"""


def scale_readme(cat: dict, scale: str, collections: list[dict], tiles: dict) -> str:
    rows = "\n".join(f"| [{c['title']}](./{c['id']}/README.md) | `{c['id']}` | "
                     f"{c['table:row_count']:,} |" for c in collections)
    return f"""\
# {cat['title']}

{cat['description']}

| Collection | Id | Rows |
|---|---|---|
{rows}

## Basemap

`{PUBLIC}/{tiles['path']}` holds every layer above in one tile layer, `{TILE_LAYER}`;
`{LAYER_PROP}` names the layer of each feature. A MapLibre style over it:
`{PUBLIC}/{tiles['style']['file']}`. Zoom {tiles['min_zoom']} to {tiles['max_zoom']}.

## License

{LICENSE_MD}
"""


def scale_agents(cat: dict, collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {cat['id']}: agent guide

{cat['description']}

{listing}
"""


def root_readme(root: dict, scales: list[dict], manifest: dict) -> str:
    listing = "\n".join(f"- [{s['title']}](./{s['_scale']}/README.md)" for s in scales)
    return f"""\
# {root['title']}

{root['description']}

{listing}

Files: `{PUBLIC}/<scale>/<layer>.parquet`, its tiles `<scale>/<layer>.pmtiles`
(tile layer `<layer>`), and `<scale>/basemap.pmtiles` with its style
`<scale>/basemap-style.json`. {manifest['total_features']:,} rows in all.
Which scale: 110m for a world view, 50m for a continent, 10m for a country.

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(scales: list[dict]) -> str:
    listing = "\n".join(f"- `{s['_scale']}`: {s['title']}. {s['_scale']}/AGENTS.md"
                        for s in scales)
    return f"""\
# {CATALOG_ID}: agent guide

Natural Earth {VERSION} as GeoParquet, one sub-catalog per scale, one
collection per layer.

{listing}

Layout: `{PUBLIC}/<scale>/<layer>.parquet` (scale: 10m, 50m, 110m), anonymous
S3 at `s3://{BUCKET}/{DATASET_PREFIX}/v{VERSION}/` on `{S3_ENDPOINT}`. Lon/lat.
Countries: join on `ISO_A2_EH` / `ISO_A3_EH` or `ADM0_A3`, not `ISO_A2`
(-99 for France and Norway). States and provinces are worldwide at 10m only.
Public domain; credit "{CREDIT}".
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> dict:
    manifest = read_manifest(out_dir)
    tiles = {t["scale"]: t for t in manifest["pmtiles"]}
    for e in manifest["files"]:
        if e["scale"] not in tiles:
            sys.exit(f"no PMTiles for scale {e['scale']} in the manifest")
        if not e.get("pmtiles"):
            sys.exit(f"no PMTiles for {e['name']} in the manifest: rebuild with tiles")
        if not (THUMBS / f"{e['scale']}_{e['layer']}.png").is_file():
            sys.exit(f"missing thumbnail {THUMBS}/{e['scale']}_{e['layer']}.png: "
                     f"run the thumbnails command")
    updated = updated or now_rfc3339()
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    if out_dir is None:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)

    scales = []
    for scale in SCALES:
        entries = [e for e in manifest["files"] if e["scale"] == scale]
        if not entries:
            continue
        sdir, collections = dest / scale, []
        for e in entries:
            source = data_source(out_dir, e["file"])
            col = build_collection(e, tiles[scale], lonlat_extent(con, source, e),
                                   footer(con, source), updated)
            cdir = sdir / col["id"]
            write_json(cdir / "styles" / "default.json", layer_style(e))
            shutil.copyfile(THUMBS / f"{scale}_{e['layer']}.png", cdir / "thumbnail.png")
            for key in ("style", "thumbnail"):
                asset = col["assets"][key]
                asset["file:size"] = (cdir / asset["href"]).stat().st_size
                asset["file:checksum"] = multihash(cdir / asset["href"])
            write_json(cdir / "collection.json", col)
            (cdir / "README.md").write_text(collection_readme(col, e, tiles[scale]))
            (cdir / "AGENTS.md").write_text(collection_agents(col, e))
            collections.append(col)
        cat = build_scale(scale, collections, tiles[scale], updated)
        write_json(sdir / "catalog.json", cat)
        (sdir / "README.md").write_text(scale_readme(cat, scale, collections, tiles[scale]))
        (sdir / "AGENTS.md").write_text(scale_agents(cat, collections))
        scales.append({**cat, "_scale": scale})
    root = build_root(scales, len(manifest["files"]), updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, scales, manifest))
    (dest / "AGENTS.md").write_text(root_agents(scales))
    return manifest


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="ne-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        m = build(out_dir, tree)
        print(f"  rendered {len(m['files'])} collections")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

WORLD = (-180.0, -60.0, 180.0, 84.0)  # Antarctica cropped, as for GAUL


def thumbnails(out_dir: Path, only: list[str] | None = None) -> None:
    """catalog/thumbnails/natural-earth/<scale>_<layer>.png: the layer over
    the 110m land, framed on the world, 600 x 400."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import LineCollection, PathCollection
    from matplotlib.path import Path as MPath

    from thumbnails import BACKGROUND, REGION_EDGE, REGION_FILL, frame

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    x0, y0, x1, y1, aspect = frame(*WORLD)
    tol = (x1 - x0) / 1200

    def geoms(file: Path):
        return [shapely.from_wkb(bytes(w)) for (w,) in con.execute(
            f"SELECT ST_AsWKB(ST_Simplify(geometry, {tol})) FROM read_parquet('{file}') "
            f"WHERE geometry IS NOT NULL").fetchall()]

    def poly_paths(gs):
        out = []
        for g in gs:
            for p in shapely.get_parts(g):
                if p.geom_type != "Polygon" or p.is_empty:
                    continue
                rings = [p.exterior, *p.interiors]
                verts, codes = [], []
                for r in rings:
                    c = list(r.coords)
                    verts += c
                    codes += [MPath.MOVETO] + [MPath.LINETO] * (len(c) - 2) + [MPath.CLOSEPOLY]
                out.append(MPath(verts, codes))
        return out

    land = geoms(out_dir / "110m" / "land.parquet")
    manifest = read_manifest(out_dir)
    THUMBS.mkdir(parents=True, exist_ok=True)
    for e in manifest["files"]:
        key = f"{e['scale']}_{e['layer']}"
        if only and key not in only:
            continue
        color = COLORS[e["layer"]]
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.axis("off")
        fig.patch.set_facecolor(BACKGROUND)
        ax.set_facecolor(BACKGROUND)
        if e["layer"] not in ("land", "ocean"):
            ax.add_collection(PathCollection(poly_paths(land), facecolors=REGION_FILL,
                                             edgecolors=REGION_EDGE, linewidths=0.3))
        gs = geoms(out_dir / e["file"])
        polys = poly_paths(gs)
        if polys:
            ax.add_collection(PathCollection(polys, facecolors=color + "aa", edgecolors=color,
                                             linewidths=0.3))
        lines = [list(p.coords) for g in gs for p in shapely.get_parts(g)
                 if p.geom_type == "LineString" and not p.is_empty]
        if lines:
            ax.add_collection(LineCollection(lines, colors=color, linewidths=0.6))
        pts = [(p.x, p.y) for g in gs for p in shapely.get_parts(g) if p.geom_type == "Point"]
        if pts:
            ax.scatter(*zip(*pts), s=2 if len(pts) > 2000 else 5, c=color, linewidths=0)
        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
        ax.set_aspect(aspect)
        path = THUMBS / f"{key}.png"
        fig.savefig(path, dpi=100, facecolor=BACKGROUND)
        plt.close(fig)
        print(f"  {key:36} {len(gs):>7,} features -> {path.relative_to(REPO)}")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp):
        sp.add_argument("--out-dir", type=Path, default=None,
                        help="natural_earth.py output dir (default: read the published files)")

    b = sub.add_parser("build", help="render the catalog tree")
    out_dir_arg(b)
    b.add_argument("--dest", type=Path, required=True)
    c = sub.add_parser("check", help="rashid pass over a rendered tree")
    c.add_argument("tree", type=Path)
    u = sub.add_parser("publish", help="build, check, upload to <remote>/catalog/")
    out_dir_arg(u)
    u.add_argument("--remote", required=True,
                   help="rclone path of the dataset prefix, e.g. parquetry:parquetry/natural-earth")
    u.add_argument("--dry-run", action="store_true")
    t = sub.add_parser("thumbnails", help="render catalog/thumbnails/natural-earth/*.png")
    t.add_argument("--out-dir", type=Path, required=True)
    t.add_argument("--only", nargs="*", help="<scale>_<layer> keys (default: all)")
    args = p.parse_args()
    if args.cmd == "build":
        m = build(args.out_dir, args.dest)
        print(f"wrote {args.dest}/ ({len(m['files'])} collections, "
              f"{m['total_features']:,} rows)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    elif args.cmd == "publish":
        publish(args.out_dir, args.remote, dry_run=args.dry_run)
    else:
        thumbnails(args.out_dir, args.only)


if __name__ == "__main__":
    main()
