#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the Maine GeoLibrary
layers.

  maine/catalog/catalog.json                        root: Maine GeoLibrary
  maine/catalog/<theme>/catalog.json                one sub-catalog per theme
  maine/catalog/<theme>/<layer>/collection.json     one per layer

maine.py's THEMES and maine_layers.csv drive it, the same way as
massgis_catalog.py, whose styles, extents and footer reading it shares. Each
collection has the layer's GeoParquet (`data`, its CRS declared with the
projection extension), its PMTiles registered as formats.md asks (a
`rel: pmtiles` link with `pmtiles:layers`, web-map-links v1.3.0, and the
file as a `visual` asset), a MapLibre style and the thumbnail the build
rendered. Counts, sizes, checksums, titles and the GeoLibrary's own
descriptions come from each theme's _manifest.json, local (--out-dir) or
published. `updated` and `checked` come from the `maine` file stamp
(scripts/filedates.py). Spec: portolan-spec v0.2.0. Validator: rashid,
through scripts/catalog.py.

Usage:
  python3 scripts/datasets/maine_catalog.py build --dest build/maine-catalog [--out-dir out/maine]
  python3 scripts/datasets/maine_catalog.py check build/maine-catalog
  python3 scripts/datasets/maine_catalog.py publish --remote parquetry:parquetry/maine [--out-dir out/maine]
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
import filedates
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
from datasets.maine import (COLORS, DEP, DEP_SITE, HUB, ORG, SERVERS, TERMS, THEMES,  # noqa: E402
                            layers, theme_title)
from datasets.massgis_catalog import PROJ_EXT, footer, lonlat_extent, short, style
from datasets.pmtiles import PMTILES_TYPE

DATASET_PREFIX = "maine"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "maine-geolibrary"
TITLE = "Maine GeoLibrary: Maine statewide GIS layers"
ATTRIBUTION = "State of Maine, Maine GeoLibrary (Maine Office of GIS) and the producing agencies"
# The file stamp's name (filedates.py), at maine/_files/maine.json.
STAMP = f"{PUBLIC_DATA}/_files/maine.json"

LICENSE_LINK = {
    "rel": "license",
    "href": HUB,
    "type": "text/html",
    "title": f'Maine GeoLibrary terms: "{TERMS}" No other licence is stated.',
}
VIA_LINK = {
    "rel": "via",
    "href": HUB,
    "type": "text/html",
    "title": "The Maine GeoLibrary data catalog, where the layers are published",
}
PROVIDERS = [
    {
        "name": "Maine Office of GIS (MEGIS) and the Maine GeoLibrary",
        "description": "The State of Maine's GIS office and its public data library. "
                       "Publishes the state agencies' layers (Marine Resources, Inland "
                       "Fisheries and Wildlife, the Maine Geological Survey, Transportation, "
                       "the E911 Bureau and others) as ArcGIS feature services.",
        "url": HUB,
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
    f'The Maine GeoLibrary\'s items state: "{TERMS}" There is no other licence statement. '
    "Credit the State of Maine, the Maine GeoLibrary and the agency each layer names when "
    "you use it. The data is provided as is, without warranty.")

# The dep theme: the Maine DEP's own map services.
DEP_ATTRIBUTION = "Maine Department of Environmental Protection"
DEP_LICENSE_LINK = {
    "rel": "license",
    "href": DEP_SITE,
    "type": "text/html",
    "title": (f'Maine DEP: no terms stated on its map services; the GeoLibrary\'s items state '
              f'"{TERMS}"'),
}
DEP_VIA_LINK = {
    "rel": "via",
    "href": DEP_SITE,
    "type": "text/html",
    "title": "Maine DEP GIS data and maps, where the layers are published",
}
DEP_PROVIDERS = [
    {
        "name": "Maine Department of Environmental Protection",
        "description": "Maine's environmental regulator. Publishes its sites, permits, "
                       "water quality, lakes, PFAS and seagrass layers as map services on "
                       "the State of Maine's ArcGIS Server.",
        "url": DEP_SITE,
        "roles": ["producer", "licensor"],
    },
    PROVIDERS[1],
]
DEP_LICENSE_MD = (
    "The Maine DEP's map services state no terms of use and no licence; their copyright text "
    "names the producer. The Maine GeoLibrary's items, including the DEP layers it lists, "
    f'state: "{TERMS}" Credit the Maine Department of Environmental Protection, and the '
    "partners a layer names, when you use it. The data is provided as is, without warranty.")


def terms(theme: str) -> dict:
    """Who publishes a theme and on what terms: the GeoLibrary, or the DEP."""
    if theme == "dep":
        return {"attribution": DEP_ATTRIBUTION, "license": DEP_LICENSE_LINK,
                "via": DEP_VIA_LINK, "providers": DEP_PROVIDERS, "license_md": DEP_LICENSE_MD,
                "keyword": "Maine DEP", "sources": DEP}
    return {"attribution": ATTRIBUTION, "license": LICENSE_LINK, "via": VIA_LINK,
            "providers": PROVIDERS, "license_md": LICENSE_MD, "keyword": "Maine GeoLibrary",
            "sources": ORG}


def folds_of(entry: dict) -> tuple[dict, dict]:
    """A folded layer's values per fold column, and each column's name in
    the file (source_<name> where the publisher already uses the name)."""
    folds = entry.get("folds") or ({"year": entry["years"]} if entry.get("years") else {})
    return folds, entry.get("fold_columns") or {c: c for c in folds}


def fold_text(entry: dict) -> str:
    """How a folded layer's sources are told apart, for its description."""
    folds, col = folds_of(entry)
    n = len(entry["source"]["urls"])
    if "from_year" in folds:
        return (f", one per pair of classifications in the `{col['from_year']}` and "
                f"`{col['to_year']}` columns, {min(folds['from_year'])} to "
                f"{max(folds['to_year'])}")
    if "year" in folds:
        return (f", one a year: {min(folds['year'])} to {max(folds['year'])}, in the "
                f"`{col['year']}` column")
    if "category" in folds:
        return f", one per category, in the `{col['category']}` column ({n} categories)"
    return ""


def fold_years(entry: dict) -> tuple[int, int] | None:
    folds, _ = folds_of(entry)
    if "from_year" in folds:
        return min(folds["from_year"]), max(folds["to_year"])
    if "year" in folds:
        return min(folds["year"]), max(folds["year"])
    return None


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


def thumbnail_file(out_dir: Path | None, theme: str, entry: dict, dest: Path) -> None:
    local = out_dir / theme / entry["thumbnail"]["file"] if out_dir else None
    if local and local.is_file():
        shutil.copyfile(local, dest)
        return
    with urllib.request.urlopen(f"{theme_url(theme)}/{entry['thumbnail']['file']}") as r:
        dest.write_bytes(r.read())


# ---------- STAC ----------

def build_collection(theme: str, entry: dict, extent: list[float],
                     columns: list[tuple[str, str]], updated: str) -> dict:
    name, title = entry["name"], entry["title"]
    path = f"{DATASET_PREFIX}/{theme}/{entry['file']}"
    src = entry["source"]
    lm = src.get("last_modified") or entry["built"]
    tiles = entry.get("pmtiles")
    years = fold_years(entry)
    t = terms(theme)
    server = SERVERS[entry.get("server") or ""]
    first_url = src["urls"][0]
    projected = [x for x in src.get("sources", []) if x.get("projected_from")]
    description = (
        f"{short(entry.get('description')) or title}\n\n"
        f"{server['publisher'][0].upper()}{server['publisher'][1:]}'s layer, "
        f"{entry['features']:,} rows, read from its {server['kind']} ({first_url}"
        + (f" and {len(src['urls']) - 1} more{fold_text(entry)}" if len(src["urls"]) > 1 else "")
        + f"). Columns and values unchanged; one GeoParquet 2.0 file, 2D geometry in "
        f"{entry['crs']} as published"
        + (f" (the server projected {len(projected)} of the sources from "
           f"{', '.join(sorted({x['projected_from'] for x in projected}))})" if projected else "")
        + f", with a bbox covering, Hilbert-sorted, and a PMTiles "
        f"archive for web maps, read in place over HTTPS or the anonymous S3 endpoint "
        f"{S3_ENDPOINT}. Credit: {t['attribution']}.")
    assets = {
        "data": {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": title,
            "description": (f"{entry['features']:,} rows in {entry['row_groups']} row groups "
                            f"of at most {GROUP_BYTES >> 20} MiB of geometry."),
            "roles": ["data"],
            "proj:code": entry["crs"],
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
                      "pmtiles:layers": [tiles["layer"]]})
    assets["thumbnail"] = {"href": "./thumbnail.png", "type": "image/png",
                           "title": f"{title}, overview", "roles": ["thumbnail"]}
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, ALTERNATE_EXT, PROJ_EXT,
                            *([WEB_MAP_LINKS_EXT] if tiles else [])],
        "id": name,
        "title": title,
        "description": description,
        "keywords": ["Maine", t["keyword"], THEMES[theme], "GeoParquet",
                     *(["PMTiles"] if tiles else [])],
        "license": "other",
        "attribution": t["attribution"],
        "updated": updated,
        "providers": t["providers"],
        "extent": {
            "spatial": {"bbox": [extent]},
            "temporal": {"interval": [[f"{years[0]}-01-01T00:00:00Z",
                                       f"{years[1]}-12-31T23:59:59Z"] if years else [lm, lm]]},
        },
        "table:row_count": entry["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t} for n, t in columns],
        "assets": assets,
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{title}: README"),
            md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
            t["license"],
            t["via"],
            *({"rel": "derived_from", "href": u, "type": "text/html",
               "title": f"The {server['kind']} layer this was read from ({u.split('/services/')[1]})"}
              for u in src["urls"]),
            {"rel": "alternate", "href": f"{theme_url(theme)}/", "type": "text/html",
             "title": "Browse the files"},
            *links,
        ],
    }


def build_theme(theme: str, collections: list[dict], updated: str) -> dict:
    t = terms(theme)
    if theme == "dep":
        about = ("The Maine Department of Environmental Protection's own layers, read from its "
                 "map services: regulated sites and spills, storage tanks, landfills, "
                 "wastewater discharges, PFAS investigation, water quality assessment and "
                 "classification, lakes, biomonitoring, seagrass surveys, dams and natural "
                 "drainageways")
    else:
        about = f"The Maine GeoLibrary's statewide layers on {THEMES[theme].lower()}"
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": f"maine-{theme}",
        "title": theme_title(theme),
        "description": (f"{about}, one collection per layer ({len(collections)}). Credit: "
                        f"{t['attribution']}."),
        "updated": updated,
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{THEMES[theme]}: README"),
            md_link("agents", "./AGENTS.md", f"{THEMES[theme]}: agent guide"),
            t["license"],
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
            f"Statewide vector layers of the State of Maine, from the Maine GeoLibrary's "
            f"feature services, republished as GeoParquet and PMTiles: {total} collections in "
            f"{len(themes)} themes, from parcels, E911 addresses and roads to conserved lands, "
            f"wildlife habitat, marine fisheries, coastal hazards and geology"
            + (", and the Maine Department of Environmental Protection's own layers (sites, "
               "spills, tanks, PFAS, water quality, lakes, seagrass) from its map services"
               if any(t["_theme"] == "dep" for t in themes) else "")
            + f". Columns and values unchanged, in each layer's own CRS (UTM zone 19N, "
            f"EPSG:26919, for most). Credit: {ATTRIBUTION}"
            + (f"; {DEP_ATTRIBUTION} for its layers" if any(t["_theme"] == "dep" for t in themes)
               else "") + "."),
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

EXAMPLE = (437_652, 4_906_290)  # the State House in Augusta, in EPSG:26919


def access_md(theme: str, entry: dict) -> str:
    url = f"{theme_url(theme)}/{entry['file']}"
    if entry["crs"] == "EPSG:26919":
        x, y = EXAMPLE
        where = (f"WHERE bbox.xmin <= {x + 2000} AND bbox.xmax >= {x - 2000}\n"
                 f"  AND bbox.ymin <= {y + 2000} AND bbox.ymax >= {y - 2000}\n"
                 f"  AND ST_DWithin(geometry, ST_Point({x}, {y}), 2000)")
        note = "-- within 2 km of the State House in Augusta, in metres (EPSG:26919)"
    else:
        where, note = "LIMIT 10", f"-- coordinates in {entry['crs']}"
    return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
{note}
SELECT * EXCLUDE (geometry, bbox)
FROM read_parquet('{url}')
{where};
```

The same file at `s3://{BUCKET}/{DATASET_PREFIX}/{theme}/{entry['file']}` on the anonymous
S3 endpoint `{S3_ENDPOINT}`."""


def collection_readme(col: dict, theme: str, entry: dict) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` |" for c in col["table:columns"])
    tiles = entry.get("pmtiles")
    tiles_row = (f"| Map tiles | `{tiles['file']}`, {tiles['bytes'] / 1e6:,.1f} MB, zoom "
                 f"{tiles['min_zoom']} to {tiles['max_zoom']} |\n" if tiles else "")
    src = entry["source"]
    dropped = entry.get("dropped_without_geometry") or 0
    return f"""\
# {col['title']}

{entry.get('description') or col['title']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, sha256 `{entry['sha256'][:12]}...` |
| Rows | {entry['features']:,}{f" ({dropped:,} without a geometry left out)" if dropped else ""} |
{tiles_row}| CRS | {entry['crs']} |
| Source | {", ".join(f"<{u}>" for u in src['urls'])}, last edited {src.get('last_modified') or 'not stated'} |

## Access

{access_md(theme, entry)}

## Schema

| Column | Type |
|---|---|
{schema}

## Provenance

{SERVERS[entry.get("server") or ""]["publisher"][0].upper()}{SERVERS[entry.get("server") or ""]["publisher"][1:]}'s {SERVERS[entry.get("server") or ""]["kind"]}, read page by page on the object id in
its own spatial reference, the rows checked against the service's count.
[maine.py]({REPO_URL}/blob/main/scripts/datasets/maine.py) keeps every column
and value, makes the geometry 2D, keeps its CRS, adds the `bbox` covering,
sorts along a Hilbert curve and cuts row groups at 4 MiB of geometry; each
file's footer, CRS, bbox and groups are verified before upload. The PMTiles
come from the same rows, in lon/lat, with tylertoo.{f'''

Copyright, from the service: "{short(entry["copyright"], 300)}"''' if entry.get("copyright") else ""}

## License

{terms(theme)["license_md"]}
"""


def collection_agents(col: dict, theme: str, entry: dict) -> str:
    crs = entry["crs"]
    return f"""\
# {col['title']}: agent guide

{entry['features']:,} rows; geometry column `geometry` in {crs}, `bbox` covering in the same units.{fold_note(entry)}

## Access

{access_md(theme, entry)}

## Query tips

- Filter on `bbox` before an exact predicate; the file is Hilbert-sorted, so a small area reads few row groups. Coordinates are in the layer's CRS, not lon/lat unless it is EPSG:4326 or EPSG:4269: transform your point first, e.g. `ST_Transform(ST_Point(lon, lat), 'OGC:CRS84', '{crs}', always_xy := true)`.
- For a map, use the `rel: pmtiles` link and the `default` style asset.
- Credit: {terms(theme)["attribution"]}.
"""


def fold_note(entry: dict) -> str:
    """The agent guide's line on a folded layer's extra columns."""
    folds, col = folds_of(entry)
    same = (" (named source_ because the publisher's own field already has the name)"
            if any(v != c for c, v in col.items()) else "")
    if "from_year" in folds:
        return (f" Columns `{col['from_year']}` and `{col['to_year']}`: the two classifications "
                f"a row's change is between; filter on them for one period{same}.")
    if "year" in folds:
        return (f" Column `{col['year']}`: the yearly source layer the row comes from; filter on "
                f"it for one year{same}.")
    if "category" in folds:
        return (f" Column `{col['category']}`: the source layer the row comes from; filter on it "
                f"for one category{same}.")
    return ""


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

{terms(cat["id"].removeprefix("maine-"))["license_md"]}
"""


def theme_agents(cat: dict, collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {cat['id']}: agent guide

{cat['description']}

{listing}
"""


def root_readme(root: dict, themes: list[dict], since: str | None) -> str:
    listing = "\n".join(f"- [{t['title']}](./{t['_theme']}/README.md)" for t in themes)
    return f"""\
# {root['title']}

{root['description']}

{listing}

Files: `{PUBLIC_DATA}/<theme>/<layer>.parquet` and `.pmtiles`.

{filedates.DATES_MD}{f'''

For these layers the record of changes starts with this republication: their
`updated` dates are tracked since {since[:10]}.''' if since else ''}

## License

{LICENSE_MD}{f"""

The `dep` theme: {DEP_LICENSE_MD}""" if any(t["_theme"] == "dep" for t in themes) else ""}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(themes: list[dict]) -> str:
    listing = "\n".join(f"- `{t['_theme']}`: {t['title']}. {t['_theme']}/AGENTS.md"
                        for t in themes)
    return f"""\
# {CATALOG_ID}: agent guide

The Maine GeoLibrary's statewide layers as GeoParquet, one sub-catalog per theme;
the `dep` theme is the Maine DEP's own layers, from its map services.

{listing}

Layout: `{PUBLIC_DATA}/<theme>/<layer>.parquet`, anonymous S3 at
`s3://{BUCKET}/{DATASET_PREFIX}/` on `{S3_ENDPOINT}`. Coordinates are in each
layer's CRS (EPSG:26919, UTM zone 19N metres, for most): transform a lon/lat
point before filtering on `bbox`. Sources: {ORG} and, for `dep`, {DEP}. Credit:
{ATTRIBUTION}; for `dep`, {DEP_ATTRIBUTION}.

{filedates.DATES_MD}
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None,
          stamp: str | Path = STAMP) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    order = {r["id"]: i for i, r in enumerate(sorted(layers(), key=lambda r: r["id"]))}
    themes, count = [], 0
    for theme in THEMES:
        try:
            manifest = read_manifest(out_dir, theme)
        except FileNotFoundError:
            continue
        except urllib.error.HTTPError as e:
            if e.code != 404:  # a 403 or 500 must not publish a catalog without the theme
                raise
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
                write_json(cdir / "styles" / "default.json",
                           style(theme, entry, color=COLORS[theme], url=theme_url(theme),
                                 attribution=terms(theme)["attribution"]))
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
    dates = filedates.load(PUBLIC_DATA, stamp)
    root = build_root(themes, count, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, themes, dates.since))
    (dest / "AGENTS.md").write_text(root_agents(themes))
    filedates.apply(dest, dates)
    return count


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="maine-catalog-") as tmp:
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
                   help="maine.py's --out-dir (default: read the published files)")
    b.add_argument("--dest", type=Path, required=True)
    b.add_argument("--stamp", default=STAMP, help="the file stamp (default: the published one)")
    c = sub.add_parser("check")
    c.add_argument("tree", type=Path)
    u = sub.add_parser("publish")
    u.add_argument("--out-dir", type=Path, default=None)
    u.add_argument("--remote", required=True)
    u.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if args.cmd == "build":
        print(f"wrote {args.dest}/ ({build(args.out_dir, args.dest, stamp=args.stamp):,} collections)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    else:
        publish(args.out_dir, args.remote, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
