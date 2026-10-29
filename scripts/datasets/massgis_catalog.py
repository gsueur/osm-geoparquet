#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the MassGIS layers.

  massgis/catalog/catalog.json                        root: MassGIS
  massgis/catalog/<theme>/catalog.json                one sub-catalog per theme
  massgis/catalog/<theme>/<layer>/collection.json     one per layer

massgis.py's THEMES and massgis_layers.csv drive it. Each collection has the
layer's GeoParquet (`data`, its CRS declared with the projection extension),
its PMTiles registered as formats.md asks (a `rel: pmtiles` link with
`pmtiles:layers`, web-map-links v1.3.0, and the file as a `visual` asset), a
MapLibre style and the thumbnail the build rendered. A table without
geometry (the assessor records) is a collection with its Parquet file and no
tiles. Counts, sizes, checksums, titles and MassGIS's own descriptions come
from each theme's _manifest.json, local (--out-dir) or published.
Spec: portolan-spec v0.2.0. Validator: rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/massgis_catalog.py build --dest build/massgis-catalog [--out-dir out/massgis]
  python3 scripts/datasets/massgis_catalog.py check build/massgis-catalog
  python3 scripts/datasets/massgis_catalog.py publish --remote parquetry:parquetry/massgis [--out-dir out/massgis]
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import urllib.error
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
from datasets.massgis import BUCKET as MASSGIS_BUCKET
from datasets.massgis import CELLS_LAYER, COLORS, PAGE, THEMES, layers
from datasets.pmtiles import PMTILES_TYPE

DATASET_PREFIX = "massgis"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "massgis"
TITLE = "MassGIS: Massachusetts statewide GIS layers"
PROJ_EXT = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
ATTRIBUTION = "MassGIS (Bureau of Geographic Information), Commonwealth of Massachusetts"
# Massachusetts with its islands, for the tables that have no geometry.
STATE_BBOX = [-73.508, 41.187, -69.858, 42.887]

LICENSE_LINK = {
    "rel": "license",
    "href": PAGE,
    "type": "text/html",
    "title": "MassGIS data layers: free of charge; each layer's metadata states its use "
             "constraints",
}
VIA_LINK = {
    "rel": "via",
    "href": PAGE,
    "type": "text/html",
    "title": "MassGIS data layers on mass.gov, where they are published",
}
PROVIDERS = [
    {
        "name": "MassGIS, Bureau of Geographic Information",
        "description": "The Commonwealth of Massachusetts' GIS office (Executive Office of "
                       "Technology Services and Security). Maintains and publishes the "
                       "statewide layers, many for other state agencies, free of charge.",
        "url": "https://www.mass.gov/orgs/massgis-bureau-of-geographic-information",
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
    "MassGIS distributes its data free of charge. Each layer's own use constraints, when "
    "its metadata states any, are quoted in its README. Credit MassGIS, and the agency the "
    "layer's metadata names, when you use it. The data is provided as is, without warranty.")

POLYGON_TYPES = ["Polygon", "MultiPolygon"]
LINE_TYPES = ["LineString", "MultiLineString"]
POINT_TYPES = ["Point", "MultiPoint"]


# ---------- measured inputs ----------

def theme_url(theme: str) -> str:
    return f"{PUBLIC_DATA}/{theme}"


def read_manifest(out_dir: Path | None, theme: str) -> dict:
    local = out_dir / theme / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    with urllib.request.urlopen(f"{theme_url(theme)}/_manifest.json") as r:
        return json.load(r)


def data_source(out_dir: Path | None, theme: str, file: str) -> str:
    local = out_dir / theme / file if out_dir else None
    return str(local) if local and local.is_file() else f"{theme_url(theme)}/{file}"


def lonlat_extent(con, entry: dict) -> list[float]:
    """The layer's bbox, from its footer extent in its own CRS, to lon/lat."""
    if not entry.get("crs"):
        return STATE_BBOX
    x0, y0, x1, y1 = entry["bbox"]
    e = con.execute(f"""SELECT ST_XMin(g), ST_YMin(g), ST_XMax(g), ST_YMax(g) FROM (
        SELECT ST_Transform(ST_MakeEnvelope({x0}, {y0}, {x1}, {y1}), '{entry['crs']}',
                            'OGC:CRS84', always_xy := true) AS g)""").fetchone()
    return [round(v, 6) for v in e]


def footer(con, source: str) -> list[tuple[str, str]]:
    return [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
            con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()]


def thumbnail_file(out_dir: Path | None, theme: str, entry: dict, dest: Path) -> None:
    local = out_dir / theme / entry["thumbnail"]["file"] if out_dir else None
    if local and local.is_file():
        shutil.copyfile(local, dest)
        return
    with urllib.request.urlopen(f"{theme_url(theme)}/{entry['thumbnail']['file']}") as r:
        dest.write_bytes(r.read())


# ---------- styles ----------

def style(theme: str, entry: dict) -> dict:
    """One MapLibre layer per kind of geometry the file holds, after the
    overview's cells when it has some (massgis.py, OVERVIEWS): coloured by
    dominant class, more opaque the more of the cell the layer covers, or
    by count per km2. The polygons of a class overview take its colours."""
    tiles = entry["pmtiles"]
    color = COLORS[theme]
    types = set(entry["geometry_types"])
    ov = tiles.get("overview") or {}
    by_class = (["match", ["get", ov["column"]], *(x for pair in ov["classes"] for x in pair),
                 ov["other"]] if ov.get("kind") == "class" else None)

    def of(kinds: list[str]) -> list:
        return ["match", ["geometry-type"], kinds, True, False]

    base = {"source": "data", "source-layer": tiles["layer"]}
    out = []
    if ov.get("layer") == CELLS_LAYER:
        cells = {"source": "data", "source-layer": CELLS_LAYER, "maxzoom": ov["handover"]}
        if by_class:
            paint = {"fill-color": by_class,
                     "fill-opacity": ["interpolate", ["linear"], ["get", "coverage_pct"],
                                      0, 0.15, 100, 0.85]}
        else:
            paint = {"fill-color": ["step", ["get", ov["density"]], ov["ramp"][0],
                                    *(x for b, c in zip(ov["breaks"], ov["ramp"][1:]) for x in (b, c))],
                     "fill-opacity": 0.8}
        out.append({"id": f"{entry['name']}-cells", "type": "fill", **cells, "paint": paint})
        base["minzoom"] = ov["handover"]
    if types & set(POLYGON_TYPES):
        out += [
            {"id": f"{entry['name']}-fill", "type": "fill", **base, "filter": of(POLYGON_TYPES),
             "paint": {"fill-color": by_class or color, "fill-opacity": 0.6 if by_class else 0.2}},
            {"id": f"{entry['name']}-outline", "type": "line", **base,
             "filter": of(POLYGON_TYPES),
             "paint": {"line-color": color,
                       "line-width": ["interpolate", ["linear"], ["zoom"], 8, 0.3, 14, 1.2]}},
        ]
    if types & set(LINE_TYPES):
        out.append({"id": f"{entry['name']}-line", "type": "line", **base,
                    "filter": of(LINE_TYPES),
                    "paint": {"line-color": color,
                              "line-width": ["interpolate", ["linear"], ["zoom"], 8, 0.6, 14, 1.6]}})
    if types & set(POINT_TYPES):
        out.append({"id": f"{entry['name']}-point", "type": "circle", **base,
                    "filter": of(POINT_TYPES),
                    "paint": {"circle-color": color,
                              "circle-radius": ["interpolate", ["linear"], ["zoom"], 6, 1.5, 14, 4.5],
                              "circle-stroke-color": "#ffffff", "circle-stroke-width": 0.5}})
    return {
        "version": 8, "name": entry["title"],
        "sources": {"data": {"type": "vector",
                             "url": f"pmtiles://{theme_url(theme)}/{tiles['file']}",
                             "attribution": ATTRIBUTION}},
        "layers": out,
    }


# ---------- STAC ----------

def short(text: str | None, limit: int = 700) -> str:
    if not text:
        return ""
    if len(text) <= limit:
        return text
    cut = text[:limit].rsplit(". ", 1)[0]
    return (cut if len(cut) > limit // 2 else text[:limit].rsplit(" ", 1)[0]) + " (...)"


def build_collection(theme: str, entry: dict, extent: list[float],
                     columns: list[tuple[str, str]], updated: str) -> dict:
    name, title = entry["name"], entry["title"]
    path = f"{DATASET_PREFIX}/{theme}/{entry['file']}"
    lm = entry["source"]["last_modified"]
    spatial = bool(entry.get("crs"))
    tiles = entry.get("pmtiles")
    zips = entry["source"]["zips"]
    what = ("one GeoParquet 2.0 file, 2D geometry in "
            f"{entry['crs']} as published, with a bbox covering, Hilbert-sorted, and a PMTiles "
            "archive for web maps" if spatial else "one Parquet file (a table without geometry)")
    description = (
        f"{short(entry.get('description')) or title}\n\n"
        f"MassGIS's layer, {entry['features']:,} rows, from `{zips[0]}` in its download "
        f"bucket. Columns and values unchanged; {what}, read in place over HTTPS or the "
        f"anonymous S3 endpoint {S3_ENDPOINT}. Credit: {ATTRIBUTION}.")
    assets = {
        "data": {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": title,
            "description": (f"{entry['features']:,} rows in {entry['row_groups']} row groups "
                            f"of at most {GROUP_BYTES >> 20} MiB of geometry." if spatial else
                            f"{entry['features']:,} rows."),
            "roles": ["data"],
            **({"proj:code": entry["crs"]} if spatial else {}),
            "file:size": entry["bytes"],
            "file:checksum": "1220" + entry["sha256"],
            "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                 "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
        },
    }
    links = []
    if tiles:
        tiles_path = f"{DATASET_PREFIX}/{theme}/{tiles['file']}"
        props = ("every attribute" if tiles.get("all_properties") else
                 "properties " + ", ".join(tiles["properties"]) if tiles["properties"] else
                 "no attributes")
        ov = tiles.get("overview") or {}
        if ov.get("layer") == CELLS_LAYER:
            what = (f"the dominant {ov['column']}" if ov["kind"] == "class" else
                    f"the {'sum of ' + ov['column'] if ov['column'] else 'feature count'} "
                    f"per km2 ({ov['density']})")
            props += (f". Below zoom {ov['handover']}, layer `{CELLS_LAYER}` instead: H3 cells "
                      f"(resolutions {', '.join(str(b['h3_res']) for b in ov['bands'])}) with "
                      f"{what}")
        elif ov:
            props += (f". Below zoom {ov['handover']}, the major features only "
                      f"({ov['filter']}), properties {', '.join(ov['properties'])}")
        assets["pmtiles"] = {
            "href": f"{PUBLIC_BASE}/{tiles_path}",
            "type": PMTILES_TYPE,
            "title": f"{title}, vector tiles",
            "description": f"PMTiles in lon/lat, layer `{tiles['layer']}`, zoom "
                           f"{tiles['min_zoom']} to {tiles['max_zoom']}, {tiles['tiles']:,} "
                           f"tiles, {props}. Built with tylertoo {tiles['tylertoo']}.",
            "roles": ["visual"],
            "file:size": tiles["bytes"],
            "file:checksum": "1220" + tiles["sha256"],
            "alternate": {"s3": {"href": f"s3://{BUCKET}/{tiles_path}",
                                 "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
        }
        assets["style"] = {"href": "./styles/default.json", "type": STYLE_TYPE,
                           "title": "Default style", "roles": ["style", "default"]}
        links.append({"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}",
                      "type": PMTILES_TYPE, "title": f"{title}, vector tiles",
                      "pmtiles:layers": [*([CELLS_LAYER] if (tiles.get("overview") or {})
                                            .get("layer") == CELLS_LAYER else []),
                                         tiles["layer"]]})
    assets["thumbnail"] = {"href": "./thumbnail.png", "type": "image/png",
                           "title": f"{title}, overview", "roles": ["thumbnail"]}
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, ALTERNATE_EXT,
                            *([PROJ_EXT] if spatial else []),
                            *([WEB_MAP_LINKS_EXT] if tiles else [])],
        "id": name,
        "title": title,
        "description": description,
        "keywords": ["MassGIS", "Massachusetts", THEMES[theme], "GeoParquet",
                     *(["PMTiles"] if tiles else [])],
        "license": "other",
        "attribution": ATTRIBUTION,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {
            "spatial": {"bbox": [extent]},
            "temporal": {"interval": [[lm, lm]]},
        },
        "table:row_count": entry["features"],
        **({"table:primary_geometry": "geometry"} if spatial else {}),
        "table:columns": [{"name": n, "type": t} for n, t in columns],
        "assets": assets,
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{title}: README"),
            md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "derived_from", "href": MASSGIS_BUCKET + zips[0], "type": "application/zip",
             "title": f"The MassGIS zip this layer was converted from ({zips[0]})"},
            {"rel": "alternate", "href": f"{theme_url(theme)}/", "type": "text/html",
             "title": "Browse the files"},
            *links,
        ],
    }


def build_theme(theme: str, collections: list[dict], updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": f"massgis-{theme.replace('_', '-')}",
        "title": f"MassGIS: {THEMES[theme]}",
        "description": (f"MassGIS's statewide layers on {THEMES[theme].lower()}, one "
                        f"collection per layer ({len(collections)}). Credit: {ATTRIBUTION}."),
        "updated": updated,
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{THEMES[theme]}: README"),
            md_link("agents", "./AGENTS.md", f"{THEMES[theme]}: agent guide"),
            LICENSE_LINK,
            {"rel": "related", "href": f"{theme_url(theme)}/ATTRIBUTION.txt",
             "type": "text/plain", "title": "Attribution, sources and what was changed"},
        ],
    }


def build_root(themes: list[dict], total: int, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (
            f"The current statewide vector layers of MassGIS, the Massachusetts Bureau of "
            f"Geographic Information, republished as GeoParquet and PMTiles: "
            f"{total} collections in {len(themes)} "
            f"themes, from property tax parcels and assessor records to wetlands, habitats, "
            f"districts and public facilities. Columns and values unchanged, in each layer's "
            f"own CRS (Massachusetts State Plane, EPSG:26986, for nearly all). Credit: "
            f"{ATTRIBUTION}."),
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{t['_theme']}/catalog.json", "type": "application/json",
               "title": t["title"]} for t in themes),
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

EXAMPLE = (236_000, 900_600)  # Boston Common, in EPSG:26986


def access_md(theme: str, entry: dict) -> str:
    url = f"{theme_url(theme)}/{entry['file']}"
    if not entry.get("crs"):
        return f"""\
```sql
INSTALL httpfs; LOAD httpfs;
SELECT * FROM read_parquet('{url}') LIMIT 10;
```"""
    x, y = EXAMPLE
    near = ("ST_DWithin(geometry, ST_Point({x}, {y}), 2000)" if entry["crs"] == "EPSG:26986"
            else "true")
    return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
-- within 2 km of Boston Common, in metres ({entry['crs']})
SELECT * EXCLUDE (geometry, bbox)
FROM read_parquet('{url}')
WHERE bbox.xmin <= {x + 2000} AND bbox.xmax >= {x - 2000}
  AND bbox.ymin <= {y + 2000} AND bbox.ymax >= {y - 2000}
  AND {near.format(x=x, y=y)};
```

The same file at `s3://{BUCKET}/{DATASET_PREFIX}/{theme}/{entry['file']}` on the anonymous
S3 endpoint `{S3_ENDPOINT}`."""


def collection_readme(col: dict, theme: str, entry: dict) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` |" for c in col["table:columns"])
    tiles = entry.get("pmtiles")
    tiles_row = (f"| Map tiles | `{tiles['file']}`, {tiles['bytes'] / 1e6:,.1f} MB, zoom "
                 f"{tiles['min_zoom']} to {tiles['max_zoom']} |\n" if tiles else "")
    uc = entry.get("use_constraints")
    return f"""\
# {col['title']}

{entry.get('description') or col['title']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, sha256 `{entry['sha256'][:12]}...` |
| Rows | {entry['features']:,} |
{tiles_row}| CRS | {entry.get('crs') or 'none (a table)'} |
| Source | `{', '.join(entry['source']['zips'])}`, last modified {entry['source']['last_modified']} |

## Access

{access_md(theme, entry)}

## Schema

| Column | Type |
|---|---|
{schema}

## Provenance

MassGIS's zip, from its public download bucket ({MASSGIS_BUCKET}).
[massgis.py]({REPO_URL}/blob/main/scripts/datasets/massgis.py) reads it with DuckDB
(geodatabase layers through GDAL first, their curves cut into straight
segments), keeps every column and value, makes the geometry 2D, keeps its CRS,
adds the `bbox` covering, sorts along a Hilbert curve and cuts row groups at
4 MiB of geometry; each file's footer, CRS, bbox and groups are verified
before upload. The PMTiles come from the same rows, in lon/lat, with tylertoo.

## License

{LICENSE_MD}{f'''

Use constraints, from the layer's metadata: "{uc}"''' if uc and uc.lower() != 'none' else ''}
"""


def collection_agents(col: dict, theme: str, entry: dict) -> str:
    crs = entry.get("crs")
    return f"""\
# {col['title']}: agent guide

{entry['features']:,} rows; {f"geometry column `geometry` in {crs} (metres for EPSG:26986), `bbox` covering in the same units" if crs else "no geometry: a table"}.

## Access

{access_md(theme, entry)}

## Query tips

- {"Filter on `bbox` before an exact predicate; the file is Hilbert-sorted, so a small area reads few row groups. Coordinates are in the layer's CRS, not lon/lat: transform your point first, e.g. `ST_Transform(ST_Point(lon, lat), 'OGC:CRS84', '" + crs + "', always_xy := true)`." if crs else "Join to the parcels on `LOC_ID` (assessor records) or on the code columns (lookups)."}
- For a map, use the `rel: pmtiles` link and the `default` style asset when there is one.
- Credit: {ATTRIBUTION}.
"""


def theme_readme(cat: dict, collections: list[dict]) -> str:
    rows = "\n".join(f"| [{c['title']}](./{c['id']}/README.md) | `{c['id']}` | "
                     f"{c['table:row_count']:,} |" for c in collections)
    return f"""\
# {cat['title']}

{cat['description']}

| Collection | Id | Rows |
|---|---|---|
{rows}

## License

{LICENSE_MD}
"""


def theme_agents(cat: dict, collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {cat['id']}: agent guide

{cat['description']}

{listing}
"""


def root_readme(root: dict, themes: list[dict]) -> str:
    listing = "\n".join(f"- [{t['title']}](./{t['_theme']}/README.md)" for t in themes)
    return f"""\
# {root['title']}

{root['description']}

{listing}

Files: `{PUBLIC_DATA}/<theme>/<layer>.parquet` and `.pmtiles`.

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(themes: list[dict]) -> str:
    listing = "\n".join(f"- `{t['_theme']}`: {t['title']}. {t['_theme']}/AGENTS.md"
                        for t in themes)
    return f"""\
# {CATALOG_ID}: agent guide

MassGIS's statewide layers as GeoParquet, one sub-catalog per theme.

{listing}

Layout: `{PUBLIC_DATA}/<theme>/<layer>.parquet`, anonymous S3 at
`s3://{BUCKET}/{DATASET_PREFIX}/` on `{S3_ENDPOINT}`. Coordinates are in each
layer's CRS (EPSG:26986, metres, for nearly all): transform a lon/lat point
before filtering on `bbox`. Credit: {ATTRIBUTION}.
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    order = {r["id"]: i for i, r in enumerate(layers())}
    themes, count = [], 0
    for theme in THEMES:
        try:
            manifest = read_manifest(out_dir, theme)
        except (FileNotFoundError, urllib.error.HTTPError):
            continue
        tdir = dest / theme
        collections = []
        for entry in sorted(manifest["files"], key=lambda e: order.get(e["name"], 1 << 20)):
            source = data_source(out_dir, theme, entry["file"])
            col = build_collection(theme, entry, lonlat_extent(con, entry),
                                   footer(con, source), updated)
            cdir = tdir / entry["name"]
            cdir.mkdir(parents=True)
            if entry.get("pmtiles"):
                write_json(cdir / "styles" / "default.json", style(theme, entry))
            thumbnail_file(out_dir, theme, entry, cdir / "thumbnail.png")
            for asset in col["assets"].values():
                if asset["href"].startswith("./"):
                    asset["file:size"] = (cdir / asset["href"]).stat().st_size
                    asset["file:checksum"] = multihash(cdir / asset["href"])
            write_json(cdir / "collection.json", col)
            (cdir / "README.md").write_text(collection_readme(col, theme, entry))
            (cdir / "AGENTS.md").write_text(collection_agents(col, theme, entry))
            collections.append(col)
        cat = build_theme(theme, collections, updated)
        write_json(tdir / "catalog.json", cat)
        (tdir / "README.md").write_text(theme_readme(cat, collections))
        (tdir / "AGENTS.md").write_text(theme_agents(cat, collections))
        themes.append({**cat, "_theme": theme})
        count += len(collections)
    root = build_root(themes, count, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, themes))
    (dest / "AGENTS.md").write_text(root_agents(themes))
    return count


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="massgis-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        n = build(out_dir, tree)
        print(f"  rendered {n} collections")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--out-dir", type=Path, default=None,
                   help="massgis.py's --out-dir (default: read the published files)")
    b.add_argument("--dest", type=Path, required=True)
    c = sub.add_parser("check")
    c.add_argument("tree", type=Path)
    u = sub.add_parser("publish")
    u.add_argument("--out-dir", type=Path, default=None)
    u.add_argument("--remote", required=True)
    u.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if args.cmd == "build":
        print(f"wrote {args.dest}/ ({build(args.out_dir, args.dest):,} collections)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    else:
        publish(args.out_dir, args.remote, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
