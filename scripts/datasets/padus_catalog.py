#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for USGS PAD-US.

  padus/catalog/catalog.json                 root: PAD-US 4.1
  padus/catalog/<layer>/collection.json      combined, fee, designation,
                                             easement, marine, proclamation

padus.py's LAYERS drives it. Each collection has the layer's GeoParquet
(`data`), PAD-US's code tables and USGS's FGDC metadata as assets, and a
thumbnail; the Combined collection also lists its per-state files
(`data-<xx>`) and its PMTiles, registered as formats.md asks (a
`rel: pmtiles` link with `pmtiles:layers`, web-map-links v1.3.0, the file
as a `visual` asset), with a MapLibre style on the GAP status. Column
descriptions are USGS's attribute definitions from the FGDC metadata
(recorded in the manifest). Counts, sizes, checksums and the gate come from
the release's _manifest.json, local (--out-dir) or published, so the
catalog stage never needs the geodatabase. Spec: portolan-spec v0.2.0.
Validator: rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/padus_catalog.py build --dest build/padus-catalog [--out-dir out/padus]
  python3 scripts/datasets/padus_catalog.py check build/padus-catalog
  python3 scripts/datasets/padus_catalog.py publish --remote parquetry:parquetry/padus [--out-dir out/padus]
  uv run --project scripts --with matplotlib --with shapely \\
      python scripts/datasets/padus_catalog.py thumbnails --out-dir out/padus
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
from datasets.padus import (
    CODED, DOI, HISTORY, ITEM_URL, LAYERS, LOOKUPS, METADATA_XML, PROGRAM_PAGE, RELEASED,
    STANDARDS, VERSION,
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

DATASET_PREFIX = "padus"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
RELEASE = f"v{VERSION}"
RELEASE_URL = f"{PUBLIC_DATA}/{RELEASE}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "usgs-padus"
TITLE = f"USGS PAD-US {VERSION}: protected areas of the United States"
THUMBS = REPO / "catalog" / "thumbnails" / "padus"
ATTRIBUTION = f"U.S. Geological Survey, Gap Analysis Project, PAD-US {VERSION}"
XML_TYPE = "application/xml"

LICENSE_LINK = {
    "rel": "license",
    "href": "https://www.usa.gov/government-copyright",
    "type": "text/html",
    "title": "Works of the US federal government are not subject to copyright in the "
             "United States (17 U.S.C. 105)",
}
VIA_LINK = {
    "rel": "via",
    "href": ITEM_URL,
    "type": "text/html",
    "title": "PAD-US 4.1 Full Inventory Database on ScienceBase, where it is published",
}
CITE_LINK = {"rel": "cite-as", "href": DOI, "type": "text/html",
             "title": "PAD-US 4.1 data release (DOI)"}
PROVIDERS = [
    {
        "name": "U.S. Geological Survey, Gap Analysis Project (GAP)",
        "description": "Compiles PAD-US from federal, state, local and private sources and "
                       "publishes it on ScienceBase. A US government work, not subject to "
                       "copyright in the United States.",
        "url": PROGRAM_PAGE,
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted the geodatabase to GeoParquet, tiled it, and maintains and "
                       "hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]

GAP_MEANING = (
    "GAP status: 1 and 2 are managed for biodiversity (1 lets disturbance such as fire run "
    "its course, 2 suppresses it), 3 is multiple use (open to extraction such as mining or "
    "logging, or off-highway vehicles), 4 has no known mandate to protect biodiversity.")
ACCESS_MEANING = ("Public access: OA open, RA restricted (a permit or season), XA closed, "
                  "UK unknown.")
OVERLAP = (
    "Areas overlap, in the Combined layer too: a designation (wilderness, monument) lies on "
    "fee land, a proclamation boundary encloses both. Never sum GIS_Acres or areas across "
    "rows: take the union of the geometries, or keep Category 'Fee' for ownership, and leave "
    "Proclamation (an approved or extent boundary, not land held) out of area questions. In "
    "Grand County, Utah, federal rows sum to 99.8% of the county and their union is 73.7%.")
SCOPE = ("PAD-US is a compilation, not a legal survey of ownership or boundaries.")

# GAP status codes, most protective first: greens, then tan, then grey.
PALETTE = {"1": "#1a6634", "2": "#5cae5c", "3": "#d8b365", "4": "#bdbdbd"}

EXTRA_DOCS = {
    "geometry": "Area outline as native Parquet GEOMETRY (WKB), NAD83 lon/lat (EPSG:4269), "
                "reprojected from USGS's contiguous-US Albers; features crossing the 180th "
                "meridian are split there, invalid polygons repaired (ST_MakeValid).",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the GeoParquet "
            "covering. Filter on it to prune row groups.",
    "OBJECTID": "The geodatabase's feature number: unique within this release, not stable "
                "across releases (use Source_PAID or WDPA_Cd for that).",
    "Term": "Easement term, from the easement source data (see the PAD-US Standards Manual).",
    "Duration": "Easement duration (for example Permanent or Temporary), from the easement "
                "source data.",
}
MARINE_DOC = ("Marine protected area attribute, as in PAD-US's Marine feature class (see the "
              "PAD-US Standards Manual).")
MARINE_ONLY = {"Prot_Lvl", "Pri_Con_Fo", "Cons_Focus", "Permanence", "NS_Full", "Mgmt_Plan",
               "Fish_Rstr", "Prot_Foc", "Constancy", "Vessel", "Anchor", "MarPercent",
               "AreaMar", "Marine"}


def column_doc(name: str, fields: dict) -> str | None:
    if name in EXTRA_DOCS:
        return EXTRA_DOCS[name]
    if name.endswith("_Desc") and name[:-5] in CODED:
        code = name[:-5]
        return (f"{code} decoded: the description of the code in PAD-US's {CODED[code]} table "
                f"(lookups/{CODED[code]}.parquet).")
    if name in fields:
        return fields[name]
    if name in MARINE_ONLY:
        return MARINE_DOC
    return None


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


def state_names(out_dir: Path | None) -> dict[str, str]:
    """State_Nm code -> name, from PAD-US's own State_Name code table, so a
    search for "Massachusetts" finds the state's file."""
    source = data_source(out_dir, "lookups/State_Name.parquet")
    return dict(connect().execute(f"SELECT Code, Dom FROM read_parquet('{source}')").fetchall())


def connect():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    return con


def layer_extent(source: str) -> list[float]:
    """Measured on the data (scripts/extent.py): from Guam across the
    antimeridian to the Virgin Islands, written with west > east."""
    con = connect()
    src = f"read_parquet('{source}')"
    y0, y1 = con.execute(f"SELECT min(bbox.ymin), max(bbox.ymax) FROM {src}").fetchone()
    b = wrapped_bbox(lon_intervals(con, src), y0, y1)
    return [round(b[0], 6), max(-90.0, round(b[1], 6)), round(b[2], 6), min(90.0, round(b[3], 6))]


def footer(source: str) -> list[tuple[str, str]]:
    return [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
            connect().execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()]


# ---------- styles ----------

def style(tiles: dict) -> dict:
    fill = ["match", ["get", "GAP_Sts"]]
    for code, colour in PALETTE.items():
        fill += [code, colour]
    return {
        "version": 8, "name": "combined: GAP status, 1 (dark green) to 4 (grey)",
        "sources": {"data": {"type": "vector", "url": f"pmtiles://{RELEASE_URL}/{tiles['file']}",
                             "attribution": ATTRIBUTION}},
        "layers": [
            {"id": "combined-fill", "type": "fill", "source": "data",
             "source-layer": tiles["layer"],
             "paint": {"fill-color": [*fill, "#bdbdbd"], "fill-opacity": 0.7}},
            {"id": "combined-outline", "type": "line", "source": "data",
             "source-layer": tiles["layer"],
             "paint": {"line-color": "#4d4d4d",
                       "line-width": ["interpolate", ["linear"], ["zoom"], 6, 0.1, 12, 0.8]}},
        ],
    }


# ---------- STAC ----------

def asset_s3(path: str) -> dict:
    return {"s3": {"href": f"s3://{BUCKET}/{path}", "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}


def gate_text(entry: dict) -> str:
    g = entry["gate"]
    text = (f"Checked against the geodatabase: {g['gdb_rows']:,} rows, OBJECTID unique, every "
            f"coded value in its code table; {entry['repaired_invalid']:,} invalid polygons "
            f"repaired, {entry['split_at_180']:,} features split at the 180th meridian")
    if g.get("state_files"):
        text += f"; the {g['state_files']} state files add up to the national file"
    return text + "."


def shared_assets(manifest: dict) -> dict:
    assets = {}
    for table, e in manifest["lookups"].items():
        assets[f"lookup-{table.lower().replace('_', '-')}"] = {
            "href": f"{RELEASE_URL}/{e['file']}",
            "type": PARQUET_TYPE,
            "title": f"Code table {table}: Code, Dom" + (
                f", {', '.join(e['columns'][2:])}" if len(e["columns"]) > 2 else ""),
            "roles": ["metadata"],
            "file:size": e["bytes"],
            "file:checksum": "1220" + e["sha256"],
        }
    xml = manifest["metadata"][METADATA_XML]
    assets["fgdc-metadata"] = {
        "href": f"{RELEASE_URL}/{METADATA_XML}", "type": XML_TYPE,
        "title": "USGS's FGDC metadata for PAD-US 4.1, as shipped", "roles": ["metadata"],
        "file:size": xml["bytes"], "file:checksum": "1220" + xml["sha256"],
    }
    return assets


def build_collection(name: str, entry: dict, manifest: dict, extent: list[float],
                     columns: list[tuple[str, str]], updated: str,
                     states: dict[str, str]) -> dict:
    spec = LAYERS[name]
    fields = manifest["fields"]
    path = f"{DATASET_PREFIX}/{RELEASE}/{entry['file']}"
    undocumented = [c for c, _ in columns if column_doc(c, fields) is None]
    if undocumented:
        sys.exit(f"{name}: no column doc for {undocumented}")
    assets = {
        "data": {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": f"PAD-US {VERSION}, {spec['title'].lower()}, national",
            "description": f"{entry['features']:,} rows in {entry['row_groups']} row groups of at "
                           f"most 4 MiB of geometry (a single larger area gets its own).",
            "roles": ["data"],
            "file:size": entry["bytes"],
            "file:checksum": "1220" + entry["sha256"],
            "alternate": asset_s3(path),
        },
        **shared_assets(manifest),
        "thumbnail": {
            "href": "./thumbnail.png",
            "type": "image/png",
            "title": f"{spec['title']}, GAP status",
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
        CITE_LINK,
        {"rel": "describedby", "href": STANDARDS, "type": "text/html",
         "title": "PAD-US Data Standards Manual"},
        {"rel": "related", "href": f"{RELEASE_URL}/ATTRIBUTION.txt", "type": "text/plain",
         "title": "Attribution and what was changed"},
        {"rel": "alternate", "href": f"{RELEASE_URL}/", "type": "text/html",
         "title": "Browse the files"},
    ]
    extensions = [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT]
    tiles = {t["name"]: t for t in manifest["pmtiles"]}.get(name)
    if name == "combined":
        for s in manifest["states"]:
            spath = f"{DATASET_PREFIX}/{RELEASE}/{s['path']}"
            assets[f"data-{s['state'].lower()}"] = {
                "href": f"{PUBLIC_BASE}/{spath}",
                "type": PARQUET_TYPE,
                "title": f"{states[s['state']]} ({s['state']})",
                "description": (f"Protected areas in {states[s['state']]}: "
                                if s["state"] != "UNKF" else
                                "Federal protected areas with no state recorded: ")
                               + f"{s['features']:,} rows.",
                "roles": ["data"],
                "file:size": s["bytes"],
                "file:checksum": "1220" + s["sha256"],
                "alternate": asset_s3(spath),
            }
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
                           "title": "GAP status, 1 (dark green) to 4 (grey)",
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
        "title": f"Protected areas (PAD-US {VERSION}): {spec['title'].lower()}",
        "description": (
            f"The Protected Areas Database of the United States, version {VERSION} "
            f"({RELEASED}), {spec['title'].lower()} feature class: {entry['features']:,} "
            "areas with their owner, manager, designation, GAP status, public access and IUCN "
            "category, columns as PAD-US names them with each code decoded beside it "
            "(`<column>_Desc`). NAD83 lon/lat geometry (EPSG:4269) with a bbox covering, "
            "Hilbert-sorted, as one GeoParquet 2.0 file read in place over HTTPS or the "
            f"anonymous S3 endpoint {S3_ENDPOINT}"
            + (", also split per state, and a PMTiles archive for web maps"
               if name == "combined" else "")
            + f". {gate_text(entry)} "
            + (f"{OVERLAP} " if name in ("combined", "fee", "designation", "easement") else "")
            + f"{SCOPE} Credit: {ATTRIBUTION}."
        ),
        "keywords": ["PAD-US", "protected areas", "public lands", "conservation", "GAP status",
                     "USGS", name, "GeoParquet"],
        "license": "other",
        "attribution": ATTRIBUTION,
        "version": VERSION,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {
            "spatial": {"bbox": [extent]},
            "temporal": {"interval": [[f"{RELEASED}T00:00:00Z", f"{RELEASED}T23:59:59Z"]]},
        },
        "table:row_count": entry["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": column_doc(n, fields)}
                          for n, t in columns],
        "assets": assets,
        "links": links,
    }


def build_root(collections: list[dict], updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (
            f"The Protected Areas Database of the United States (PAD-US) {VERSION}, USGS Gap "
            "Analysis Project's national inventory of protected areas: federal, state, local, "
            "tribal and private conservation lands, easements, designations and marine areas, "
            "with owner, manager, GAP status and public access. One collection per feature "
            "class, as GeoParquet in NAD83 lon/lat, the Combined layer also per state and as "
            f"PMTiles. {OVERLAP} Public domain in the United States. {SCOPE} "
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
            CITE_LINK,
            {"rel": "describedby", "href": STANDARDS, "type": "text/html",
             "title": "PAD-US Data Standards Manual"},
            {"rel": "related", "href": f"{RELEASE_URL}/ATTRIBUTION.txt", "type": "text/plain",
             "title": "Attribution and what was changed"},
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


# ---------- markdown ----------

LICENSE_MD = f"""\
PAD-US is a work of the US federal government, not subject to copyright in
the United States (17 U.S.C. 105). Credit "{ATTRIBUTION}" and cite the data
release, {DOI}."""

# Moab, Utah: Arches, Canyonlands, BLM and state lands within a few km.
EXAMPLE_BOX = (-109.75, 38.45, -109.35, 38.75)


def access_md(name: str) -> str:
    x0, y0, x1, y1 = EXAMPLE_BOX
    url = f"{RELEASE_URL}/{name}.parquet"
    return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
SELECT Unit_Nm, Mang_Name_Desc, Des_Tp_Desc, GAP_Sts, Pub_Access_Desc, GIS_Acres
FROM read_parquet('{url}')
WHERE bbox.xmin <= {x1} AND bbox.xmax >= {x0}
  AND bbox.ymin <= {y1} AND bbox.ymax >= {y0}
ORDER BY GIS_Acres DESC LIMIT 20;
```

The same file at `s3://{BUCKET}/{DATASET_PREFIX}/{RELEASE}/{name}.parquet` on the
anonymous S3 endpoint `{S3_ENDPOINT}`."""


def collection_readme(col: dict, name: str, entry: dict, tiles: dict | None) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    tiles_row = (f"| Map tiles | `{tiles['file']}`, {tiles['bytes'] / 1e6:,.1f} MB, zoom "
                 f"{tiles['min_zoom']} to {tiles['max_zoom']} |\n" if tiles else "")
    states_row = ("| Per state | `combined/state=<XX>/combined.parquet`, XX the `State_Nm` "
                  "code (UNKF: unknown federal, UM: Minor Outlying Islands) |\n"
                  if name == "combined" else "")
    tiles_md = (f"""
## Map tiles

`{RELEASE_URL}/{tiles['file']}`, tile layer `{tiles['layer']}`, properties
{', '.join(f"`{p}`" for p in tiles['properties'])}. `styles/default.json`
colours `GAP_Sts`: 1 dark green, 2 green, 3 tan, 4 grey.
""" if tiles else "")
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, {entry['row_groups']} row groups, sha256 `{entry['sha256'][:12]}...` |
| Rows | {entry['features']:,} |
{states_row}{tiles_row}| CRS | NAD83 lon/lat, EPSG:4269 (PAD-US ships USGS's contiguous-US Albers) |
| Code tables | `{RELEASE_URL}/lookups/<table>.parquet`: {', '.join(LOOKUPS)} |

## Access

{access_md(name)}
{tiles_md}
## What the codes mean

- {GAP_MEANING}
- {ACCESS_MEANING}
- `Category`: Fee (ownership), Easement, Designation, Marine, Proclamation
  (approved or extent boundary), Other, Unknown. `FeatClass`, in the
  Combined layer, names the feature class a row comes from.
- Every coded column has its description beside it in `<column>_Desc`.

## Columns

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

`{ITEM_URL}` (`PADUS4_1Geodatabase.zip`, behind a ScienceBase login,
downloaded by hand). [padus.py]({REPO_URL}/blob/main/scripts/datasets/padus.py)
reads the geodatabase with GDAL (curves cut into segments) and DuckDB, keeps
every column and value except Shape_Length and Shape_Area, decodes the coded
columns from the geodatabase's code tables, reprojects to NAD83 lon/lat,
splits the features that cross the 180th meridian, repairs invalid polygons,
adds the `bbox` covering, sorts along a Hilbert curve and cuts row groups at
4 MiB of geometry; rows are checked against the geodatabase, and the footer,
bbox and groups are verified before upload.

## License

{LICENSE_MD}
"""


def collection_agents(col: dict, name: str) -> str:
    return f"""\
# {col['title']}: agent guide

One row per PAD-US area, NAD83 lon/lat (EPSG:4269), geometry column `geometry`.
`OBJECTID` is unique in this release only; `Source_PAID` and `WDPA_Cd`
identify an area across releases where present.

## Access

{access_md(name)}

## Query tips

- Filter on `bbox` before any spatial predicate: a few Alaska areas are over
  10 MB each, and the bbox keeps a query away from them.
- {OVERLAP}
- {GAP_MEANING}
- {ACCESS_MEANING}
- Area: `GIS_Acres` (PAD-US's), or measure the geometry in an equal-area CRS.
  The file is NAD83 like census/TIGER: an untagged `ST_Point` compares with it
  directly; cast with `geometry::GEOMETRY` to join it with CRS84 data.
- Per state: `{RELEASE_URL}/combined/state=<XX>/combined.parquet`.
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

Files: `{RELEASE_URL}/<layer>.parquet`, the Combined layer also at
`{RELEASE_URL}/combined/state=<XX>/combined.parquet` and as
`combined.pmtiles`; PAD-US's code tables under `lookups/`, USGS's FGDC
metadata (`{METADATA_XML}`) and version history (`{HISTORY}`). Standards:
{STANDARDS}.

## What the codes mean

- {GAP_MEANING}
- {ACCESS_MEANING}

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {CATALOG_ID}: agent guide

USGS PAD-US {VERSION}, one collection per feature class.

{listing}

Layout: `{RELEASE_URL}/<layer>.parquet`, Combined per state under
`combined/state=<XX>/`, anonymous S3 at `s3://{BUCKET}/{DATASET_PREFIX}/{RELEASE}/`
on `{S3_ENDPOINT}`. NAD83 lon/lat (EPSG:4269). Start from `combined` for
"is this protected" and area questions: {OVERLAP} Credit: {ATTRIBUTION}.
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
    states = state_names(out_dir)
    collections, total = [], 0
    for name in LAYERS:
        if not (THUMBS / f"{name}.png").is_file():
            sys.exit(f"missing {THUMBS / name}.png: run the thumbnails command")
        source = data_source(out_dir, files[name]["file"])
        col = build_collection(name, files[name], manifest, layer_extent(source), footer(source),
                               updated, states)
        cdir = dest / name
        if name in tiles:
            write_json(cdir / "styles" / "default.json", style(tiles[name]))
            asset = col["assets"]["style"]
            asset["file:size"] = (cdir / asset["href"]).stat().st_size
            asset["file:checksum"] = multihash(cdir / asset["href"])
        write_json(cdir / "collection.json", col)
        shutil.copyfile(THUMBS / f"{name}.png", cdir / "thumbnail.png")
        (cdir / "README.md").write_text(collection_readme(col, name, files[name], tiles.get(name)))
        (cdir / "AGENTS.md").write_text(collection_agents(col, name))
        collections.append(col)
        total += files[name]["features"]
    root = build_root(collections, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, collections))
    (dest / "AGENTS.md").write_text(root_agents(collections))
    return total


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="padus-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        total = build(out_dir, tree)
        print(f"  rendered {len(LAYERS)} collections, {total:,} rows")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

CONUS = (-125.0, 24.0, -66.5, 49.5)


def thumbnails(out_dir: Path) -> None:
    """Each feature class over the conterminous US, coloured on GAP status
    as the default style."""
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
    for name in LAYERS:
        path = out_dir / RELEASE / f"{name}.parquet"
        rows = con.execute(f"""
            SELECT GAP_Sts, ST_AsWKB(ST_SimplifyPreserveTopology(geometry, {tolerance}))
            FROM read_parquet('{path}')
            WHERE bbox.xmax > {CONUS[0]} AND bbox.xmin < {CONUS[2]}
              AND bbox.ymax > {CONUS[1]} AND bbox.ymin < {CONUS[3]}""").fetchall()
        polys, fills = [], []
        for gap, wkb in rows:
            for part in shapely.get_parts(shapely.from_wkb(bytes(wkb))):
                if part.geom_type != "Polygon" or part.is_empty:
                    continue
                polys.append(shapely.get_coordinates(part.exterior))
                fills.append(PALETTE.get(gap, "#bdbdbd"))
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.set_facecolor(BACKGROUND)
        fig.patch.set_facecolor(BACKGROUND)
        ax.add_collection(PolyCollection(polys, facecolors=fills, edgecolors="none",
                                         linewidths=0, alpha=0.9))
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
                        help="padus.py's --out-dir (default: read the published files)")

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
