#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for OSM infrastructure.

One STAC Collection per layer of infra.py (27), each partitioned by country
and state like the data: `partition:glob` for the bulk read through the S3
endpoint, and one `data-<country>-<state>` asset per folder so a plain STAC
client can open the files. Row counts come from the _manifest.json files,
sizes from the files, extents from the bbox columns and column types from the
footers, all read from the build's output directory. Only latest/ is
published, and the next build replaces it, so assets carry sizes but no
checksums (spec: a publisher that cannot keep one current should omit it).

The catalog is published beside the data, at /osm-infrastructure/catalog/,
so the dataset stays one self-contained prefix. Spec, validator (rashid),
STAC constants and uploader are shared with the OSM catalog (scripts/catalog.py).

Usage:
  python3 scripts/datasets/infra_catalog.py build --out-dir out/latest --dest build/infra-catalog
  python3 scripts/datasets/infra_catalog.py check build/infra-catalog
  python3 scripts/datasets/infra_catalog.py publish --out-dir out/latest \\
      --remote parquetry:parquetry/osm-infrastructure
  # the committed thumbnails, re-rendered only when the data changes a lot
  uv run --project scripts --with matplotlib python scripts/datasets/infra_catalog.py \\
      thumbnails --out-dir out/latest
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import tempfile
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import infra

import catalog as shared  # scripts/catalog.py
from catalog import (
    ALTERNATE_EXT,
    BUCKET,
    CONTACT_EMAIL,
    FILE_EXT,
    LICENSE,
    LICENSE_LINK,
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
    VIA_LINK,
    md_link,
    multihash,
    now_rfc3339,
    write_json,
)

DATASET_PREFIX = "osm-infrastructure"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
DATA_URL = f"{PUBLIC_DATA}/latest"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "osm-infrastructure-geoparquet"
THUMBS = REPO / "catalog" / "thumbnails" / "osm-infrastructure"
OIM_URL = "https://openinframap.org"

PROVIDERS = [
    {
        "name": "OpenStreetMap contributors",
        "description": "Created and maintain the data, published under the ODbL 1.0.",
        "url": "https://www.openstreetmap.org/",
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geofabrik GmbH",
        "description": "Publishes the continental OSM extracts this catalog is built from.",
        "url": "https://download.geofabrik.de/",
        "roles": ["processor"],
    },
    {
        "name": "Geomermaids",
        "description": "Extracts the infrastructure, derives the typed columns following "
                       "Open Infrastructure Map's data model, and maintains and hosts "
                       "this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]

PREVIEW = ("A preview: layer names, columns and values may still change between builds.")

# Title, description. Order and names are infra.LAYERS'.
LAYER_DOCS = {
    "power_line": ("Power lines and cables",
                   "Overhead lines, minor lines and underground or submarine cables "
                   "(power=line, minor_line, cable), with voltages in kV per circuit."),
    "power_circuit": ("Power circuits",
                      "Circuits assembled from route=power relations: the voltage, "
                      "length and substations of each circuit, across the line sections "
                      "that carry it."),
    "power_tower": ("Power towers and poles",
                    "The supports of power lines (power=tower, pole, portal), with "
                    "their design and height, and the transformer tags of pole-mounted "
                    "transformers."),
    "power_substation": ("Substations",
                         "Substations, with their voltages in kV. Site relations are "
                         "merged into one feature, and the circuits ending there are listed."),
    "power_plant": ("Power plants",
                    "Power plants, with source, method and output in MW, tagged or "
                    "estimated from their generators (and, for solar farms, their area)."),
    "power_generator": ("Generators",
                        "Individual generators (wind turbines, solar panels, hydro units), "
                        "with source, method and output in MW, tagged or estimated."),
    "power_switchgear": ("Switchgear and transformers",
                         "Switches, transformers, compensators, insulators, terminals "
                         "and converters, with their voltages."),
    "power_other": ("Other power features",
                    "Every other power=* value, kept so nothing tagged power is lost "
                    "(Open Infrastructure Map does not draw these)."),
    "telecom_cable": ("Telecom cables",
                      "Telecommunication lines and cables, submarine cables included."),
    "telecom_building": ("Telecom buildings",
                         "Data centres, telephone exchanges and telecommunication offices."),
    "telecom_location": ("Telecom locations",
                         "Other telecom=* points and areas (connection points, "
                         "distribution points)."),
    "telecom_mast": ("Masts and communication towers",
                     "Masts and towers carrying antennas, with their height and the "
                     "services they host (mobile, television, radio...)."),
    "telecom_antenna": ("Antennas", "Antennas mapped on their own (man_made=antenna)."),
    "utility_pole": ("Utility poles", "Poles for other utilities (man_made=utility_pole)."),
    "street_cabinet": ("Street cabinets",
                       "Street cabinets, by the utility they serve (telecom, power, "
                       "water...)."),
    "pipeline": ("Pipelines",
                 "Pipelines, with the substance they carry, a category (oil, gas, "
                 "water...) and their length."),
    "petroleum_site": ("Oil and gas sites",
                       "Refineries, terminals, storage, well clusters and other oil and "
                       "gas industrial sites."),
    "petroleum_well": ("Oil and gas wells", "Petroleum and oil wells."),
    "offshore_platform": ("Offshore platforms", "Offshore platforms (man_made=offshore_platform)."),
    "pipeline_feature": ("Pipeline features",
                         "Valves, compressors, pig launchers and other features on "
                         "pipelines."),
    "marker": ("Markers", "Markers of buried utilities (marker=*)."),
    "water_treatment_plant": ("Water treatment plants",
                              "Water works and desalination plants."),
    "wastewater_plant": ("Wastewater plants", "Wastewater treatment plants."),
    "pumping_station": ("Pumping stations", "Pumping stations, by the substance pumped."),
    "water_tower": ("Water towers", "Water towers, with their height."),
    "water_well": ("Water wells", "Water wells, with the pump type and whether the water is drinkable."),
    "pressurised_waterway": ("Pressurised waterways",
                             "Penstocks and other pressurised waterways feeding hydro plants."),
}

# The root lists these four; each lists its layers (27 flat children are hard
# to browse, PTL-CAT-001). Keys are infra.Layer.group.
GROUPS = {
    "power": ("Power", "Lines, circuits, towers, substations, plants, generators and "
                       "switchgear."),
    "telecoms": ("Telecoms", "Cables, data centres and exchanges, masts, antennas, poles "
                             "and street cabinets."),
    "petroleum": ("Oil and gas", "Pipelines, oil and gas sites, wells, offshore platforms, "
                                 "pipeline features and markers."),
    "water": ("Water", "Water and wastewater treatment plants, pumping stations, water "
                       "towers, wells and pressurised waterways."),
}

COMMON_DOCS = {
    "osm_id": "OSM element id. With osm_type, the key back to openstreetmap.org.",
    "osm_type": "OSM element type: node, way or relation.",
    "country": "ISO 3166-1 alpha-2 code of the country the feature is in (FAO GAUL "
               "codes for disputed areas), or _intl on the high seas. Also the "
               "partition key.",
    "state": "Slug of the FAO GAUL 2024 first-level unit the feature is in "
             "(state=texas), _offshore in a country's EEZ, _intl on the high seas. "
             "Also the partition key.",
    "type": "The feature's kind within the layer: the value of the layer's main tag "
            "(line, minor_line, cable for power_line), whatever its lifecycle.",
    "lifecycle": "active, construction, proposed, disused or abandoned, read from the "
                 "lifecycle prefixes and values of the layer's main tag.",
    "name": "The name tag.",
    "names": "Every name and name:<lang> tag, keyed by tag.",
    "operator": "The operator tag.",
    "operator_wikidata": "The operator:wikidata tag.",
    "ref": "The ref tag.",
    "wikidata": "The wikidata tag.",
    "wikipedia": "The wikipedia tag.",
    "start_date": "The start_date tag, as tagged.",
    "website": "The website tag.",
    "tags": "Every OSM tag of the element. Anything not promoted to a column is a "
            "tags['key'] lookup away.",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax, float32 rounded outward), "
            "declared as the GeoParquet covering. Filter on it to prune row groups.",
    "geometry": "Native Parquet GEOMETRY (WKB), OGC:CRS84 lon/lat. Closed ways are "
                "lines or polygons as the layer's features are, never both.",
}

DERIVED_DOCS = {
    "voltages_kv": "Voltages in kV, one entry per circuit, highest first; a single "
                   "voltage on a multi-circuit line is repeated for each circuit "
                   "(Open Infrastructure Map's rule). Non-integer entries are dropped.",
    "voltage_kv": "The highest voltage, in kV.",
    "voltage_primary_kv": "Highest voltage:primary, in kV.",
    "voltage_secondary_kv": "Highest voltage:secondary, in kV.",
    "voltage_tertiary_kv": "Highest voltage:tertiary, in kV.",
    "circuits": "The circuits tag as an integer.",
    "cables": "The cables tag as an integer.",
    "frequency_hz": "The first value of the frequency tag, in Hz.",
    "length_km": "Geodesic length, in km.",
    "area_m2": "Geodesic area, in m². NULL for points and lines.",
    "height_m": "The height tag as a number, in metres.",
    "transition": "True where location:transition=yes: the line goes underground here.",
    "source": "The first value of generator:source or plant:source.",
    "output_mw": "The tagged electrical output, converted to MW (W, kW, MW and GW "
                 "units; a value without a unit is NULL).",
    "output_mw_estimated": "The output in MW: tagged if it is, else estimated as Open "
                           "Infrastructure Map does (solar: modules x 250 W, else 150 W/m² "
                           "of panel, else 4 kW per point; a plant also sums its generators, "
                           "or 40 W/m² for a solar farm's area).",
    "output_basis": "Where output_mw_estimated comes from: tagged, generators, modules, "
                    "area or point.",
    "generator_count": "Number of generators inside the plant or its site relation.",
    "generator_output_mw": "Summed output of those generators, in MW.",
    "section_count": "Number of line sections the circuit relation holds.",
    "substation_members": "The substations the circuit relation lists, as (osm_type, osm_id).",
    "circuit_ids": "OSM ids of the circuit relations ending at the substation.",
    "services": "What the mast hosts, from its communication:* tags (mobile_phone, "
                "television...), sorted.",
    "utility": "The first value of the utility tag.",
    "category": "Pipeline category from the substance (oil, gas, water, "
                "heat, ...), Open Infrastructure Map's grouping.",
}

_TAG = re.compile(r"(?:first_semi\()?tags\['([^']+)'\]\)?")


def column_docs() -> dict[str, str]:
    """Every column of every layer described: common, derived, and plain tag
    copies from their infra.py expression."""
    docs = dict(COMMON_DOCS)
    for layer in infra.LAYERS:
        for name, expr in layer.columns:
            if name in DERIVED_DOCS:
                docs.setdefault(name, DERIVED_DOCS[name])
                continue
            m = _TAG.fullmatch(expr)
            if not m:
                sys.exit(f"{layer.name}.{name}: derived column with no doc ({expr})")
            docs.setdefault(name, f"The {m.group(1)} tag." if not expr.startswith("first_semi")
                            else f"The first value of the {m.group(1)} tag.")
    return docs


# ---------- measured inputs ----------

def read_as_of(attribution: str | None) -> list[str]:
    """The Geofabrik timestamps ATTRIBUTION.txt lists, as a STAC interval."""
    if attribution and Path(attribution).is_file():
        text = Path(attribution).read_text()
    else:
        with urllib.request.urlopen(attribution or f"{PUBLIC_DATA}/ATTRIBUTION.txt") as r:
            text = r.read().decode()
    stamps = sorted(re.findall(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", text))
    if not stamps:
        sys.exit("no source timestamp in ATTRIBUTION.txt")
    return [stamps[0], stamps[-1]]


def remote_sizes(remote: str) -> dict[str, int]:
    """Published sizes, keyed like the local paths (country=../state=../x.parquet).
    For a catalog rendered from another build of the same input: the rows are
    the same, the bytes are not (parquet encoding is not reproducible)."""
    import subprocess
    out = subprocess.run(["rclone", "lsjson", "-R", "--files-only", f"{remote}/latest/"],
                         check=True, capture_output=True, text=True).stdout
    return {e["Path"]: e["Size"] for e in json.loads(out) if e["Path"].endswith(".parquet")}


def measure(out_dir: Path, sizes: dict[str, int] | None = None) -> dict[str, dict]:
    """Per layer: its folders (with rows and size), extent and columns."""
    layers = {l.name: {"folders": [], "rows": 0, "bytes": 0} for l in infra.LAYERS}
    for mf in sorted(out_dir.glob("country=*/state=*/_manifest.json")):
        m = json.loads(mf.read_text())
        for name, rows in m["themes"].items():
            f = mf.parent / f"{name}.parquet"
            size = sizes[f.relative_to(out_dir).as_posix()] if sizes else f.stat().st_size
            layers[name]["folders"].append({
                "country": m["country"], "state": m["state"], "rows": rows, "bytes": size,
                "title": f"{m['state_name']}, {m['country_name']}"
                         if not m["state"].startswith("_") else m["state_name"]})
            layers[name]["rows"] += rows
            layers[name]["bytes"] += size
    con = duckdb.connect()
    for name, agg in layers.items():
        if not agg["folders"]:
            sys.exit(f"{name}: no files under {out_dir}")
        glob = str(out_dir / "country=*" / "state=*" / f"{name}.parquet")
        x0, y0, x1, y1 = con.execute(f"""
            SELECT min(bbox.xmin), min(bbox.ymin), max(bbox.xmax), max(bbox.ymax)
            FROM read_parquet('{glob}')""").fetchone()
        agg["bbox"] = [max(-180.0, round(x0, 6)), max(-90.0, round(y0, 6)),
                       min(180.0, round(x1, 6)), min(90.0, round(y1, 6))]
        first = out_dir / f"country={agg['folders'][0]['country']}" / \
            f"state={agg['folders'][0]['state']}" / f"{name}.parquet"
        agg["columns"] = [(r[0], "GEOMETRY" if r[1].startswith("GEOMETRY") else r[1])
                          for r in con.execute(f"DESCRIBE SELECT * FROM '{first}'").fetchall()]
        agg["countries"] = sorted({f["country"] for f in agg["folders"]})
    return layers


# ---------- STAC ----------

def folder_assets(name: str, folders: list[dict]) -> dict:
    assets = {}
    for f in folders:
        path = f"{DATASET_PREFIX}/latest/country={f['country']}/state={f['state']}/{name}.parquet"
        assets[f"data-{f['country'].lower()}-{f['state'].strip('_')}"] = {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": f["title"],
            "description": f"{f['rows']:,} features",
            "roles": ["data"],
            "file:size": f["bytes"],
            "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                 "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
        }
    return assets


def glob_for(name: str) -> str:
    return f"s3://{BUCKET}/{DATASET_PREFIX}/latest/country=*/state=*/{name}.parquet"


def build_collection(name: str, agg: dict, docs: dict, interval: list[str],
                     updated: str) -> dict:
    title, description = LAYER_DOCS[name]
    thumb = THUMBS / f"{name}.png"
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT,
                            VERSION_EXT, ALTERNATE_EXT],
        "id": name,
        "title": title,
        "description": (
            f"{description} From OpenStreetMap, worldwide, following Open "
            f"Infrastructure Map's data model. One GeoParquet file per country and "
            f"state ({len(agg['folders']):,} files, {agg['rows']:,} rows). Read every "
            f"file at once with the partition glob {glob_for(name)} through the "
            f"anonymous S3 endpoint {S3_ENDPOINT} (path-style, empty credentials), or "
            f"one file over plain HTTPS. This collection reads latest/, which each "
            f"build replaces. {PREVIEW}"
        ),
        "keywords": ["OpenStreetMap", "OSM", "infrastructure", "GeoParquet", "worldwide",
                     infra_group(name), name.replace("_", " ")],
        "license": LICENSE,
        "version": interval[-1][:10],
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [agg["bbox"]]},
                   "temporal": {"interval": [interval]}},
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [
            {"name": "country", "type": "string", "description": COMMON_DOCS["country"]},
            {"name": "state", "type": "string", "description": COMMON_DOCS["state"]},
        ],
        "partition:file_count": len(agg["folders"]),
        "partition:glob": glob_for(name),
        "table:row_count": agg["rows"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": docs[n]}
                          for n, t in agg["columns"]],
        "assets": {
            **folder_assets(name, agg["folders"]),
            "thumbnail": {
                "href": "./thumbnail.png",
                "type": "image/png",
                "title": "World density of the layer",
                "roles": ["thumbnail"],
                "file:size": thumb.stat().st_size,
                "file:checksum": multihash(thumb),
            },
        },
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{title}: README"),
            md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "alternate", "href": f"{DATA_URL}/", "type": "text/html",
             "title": "Browse the latest files"},
        ],
    }


def infra_group(name: str) -> str:
    return next(l.group for l in infra.LAYERS if l.name == name)


def build_root(collections: list[dict], interval: list[str], updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID,
        "title": "OpenStreetMap infrastructure as GeoParquet",
        "description": (
            "The world's power, telecoms, oil and gas, and water infrastructure from "
            "OpenStreetMap, as GeoParquet 2.0: the content of Open Infrastructure Map, "
            "with its data model (voltages in kV per circuit, outputs in MW, solar "
            "estimates, circuits and plants assembled from their relations) and every "
            f"OSM tag kept. {len(collections)} layers, one file per country and state. "
            f"Only the latest build is published. {PREVIEW} Data (c) OpenStreetMap "
            "contributors, ODbL 1.0. Not affiliated with Open Infrastructure Map."
        ),
        "version": interval[-1][:10],
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{g}/catalog.json",
               "type": "application/json", "title": GROUPS[g][0]} for g in GROUPS),
            md_link("describedby", "./README.md", "Catalog README"),
            md_link("agents", "./AGENTS.md", "Catalog agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "related", "href": f"{PUBLIC_DATA}/ATTRIBUTION.txt", "type": "text/plain",
             "title": "Attribution, sources and their licences"},
            {"rel": "related", "href": OIM_URL, "type": "text/html",
             "title": "Open Infrastructure Map, whose data model this follows"},
            {"rel": "vcs", "href": REPO_URL, "type": "text/html",
             "title": "Builder and catalog source"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
            {"rel": "alternate", "href": SITE_URL, "type": "text/html",
             "title": "Project site"},
        ],
    }


def build_group(group: str, collections: list[dict], interval: list[str],
                updated: str) -> dict:
    title, description = GROUPS[group]
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": f"{CATALOG_ID}-{group}",
        "title": f"{title} infrastructure",
        "description": f"{description} From OpenStreetMap, worldwide. {PREVIEW}",
        "version": interval[-1][:10],
        "updated": updated,
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json",
               "type": "application/json", "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{title} infrastructure: README"),
            md_link("agents", "./AGENTS.md", f"{title} infrastructure: agent guide"),
            LICENSE_LINK,
        ],
    }


def group_readme(cat: dict, collections: list[dict], aggs: dict) -> str:
    rows = "\n".join(
        f"| [{c['title']}](./{c['id']}/README.md) | `{c['id']}` | {aggs[c['id']]['rows']:,} | "
        f"{len(aggs[c['id']]['folders']):,} |" for c in collections)
    return f"""\
# {cat['title']}

{cat['description']}

| Collection | Layer | Rows | Files |
|---|---|---|---|
{rows}
"""


def group_agents(cat: dict, collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {cat['title']}: agent guide

{cat['description']}

## Collections

{listing}

Every layer is one file per country and state,
`{DATA_URL}/country=<ISO2>/state=<slug>/<layer>.parquet`; the catalog's root
AGENTS.md (../AGENTS.md) has the access and conventions.
"""


# ---------- markdown ----------

LICENSE_MD = """\
[ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/). Credit
"© OpenStreetMap contributors", and keep a derived database under the ODbL.
The country and state keys come from FAO GAUL 2024 (CC BY 4.0) on land and
Marine Regions' EEZ union (CC BY 4.0) offshore; the full notice, with the
source extracts' dates, is in [ATTRIBUTION.txt]({attribution})."""

PROVENANCE = f"""\
Built from Geofabrik's nine continental extracts: osmium tags-filter keeps the
infrastructure of each (about 0.8%), osmium merge joins them, and
[infra.py]({REPO_URL}/blob/main/scripts/datasets/infra.py) exports the
geometries, assembles circuits and site relations, derives the typed columns
with Open Infrastructure Map's rules, assigns each feature to the country and
state holding a point on it, and writes one Hilbert-sorted file per layer and
folder, in row groups of 32,000 rows with a bbox covering. Every file is checked
(geo footer, covering, GEOMETRY type, row groups, bboxes) before publication.
Features that touch no Geofabrik extract (mid-ocean cable nodes) are missing."""


def access_section(name: str) -> str:
    return f"""\
One file over HTTPS, no credentials:

```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
SELECT count(*) FROM read_parquet('{DATA_URL}/country=DE/state=bayern/{name}.parquet');
```

Every country and state at once, through the anonymous S3 endpoint
`{S3_ENDPOINT}` (path-style, empty credentials); the `country` and `state`
columns are inside every file:

```sql
SET s3_endpoint='{S3_ENDPOINT}'; SET s3_url_style='path';
SET s3_access_key_id=''; SET s3_secret_access_key='';
SELECT country, count(*) FROM read_parquet('{glob_for(name)}') GROUP BY 1 ORDER BY 2 DESC;
```"""


def collection_readme(col: dict, agg: dict, interval: list[str]) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    return f"""\
# {col['title']}

{LAYER_DOCS[col['id']][1]} {PREVIEW}

| | |
|---|---|
| Rows | {agg['rows']:,} |
| Files | {len(agg['folders']):,} (one per country and state), {agg['bytes'] / 1e6:,.0f} MB in all |
| Countries | {len(agg['countries'])} |
| Extent | {shared.fmt_bbox(agg['bbox'])} (lon/lat) |
| Data as of | {interval[0]} (Geofabrik extracts) |
| License | ODbL 1.0, © OpenStreetMap contributors |

## Access

{access_section(col['id'])}

## Schema

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

{PROVENANCE}

## License

{LICENSE_MD.format(attribution=f"{PUBLIC_DATA}/ATTRIBUTION.txt")}
"""


def collection_agents(col: dict, agg: dict) -> str:
    return f"""\
# {col['title']}: agent guide

{LAYER_DOCS[col['id']][1]} One row per OSM element, OGC:CRS84.

## Access

{access_section(col['id'])}

## Query tips

- One place: open its file, `{DATA_URL}/country=<ISO2>/state=<slug>/{col['id']}.parquet`.
  The data assets list every file with its title ("Texas, United States of
  America") and row count; `{PUBLIC_DATA}/index.json` maps every folder to its
  names and GAUL code.
- A region or the world: the partition glob through the S3 endpoint, with a
  `bbox` filter so row groups outside the window are skipped.
- `lifecycle` separates what exists (active) from construction, proposed,
  disused and abandoned features; filter on it for current infrastructure.
- Anything not promoted to a column is in `tags`, a MAP: `tags['key']`.
- Country and state are assigned from one point on each feature, so a long
  line or cable sits in one folder only. They are a partition key, not a
  statement on borders.
- Data (c) OpenStreetMap contributors, ODbL 1.0: credit it.
"""


def root_readme(root: dict, collections: list[dict], aggs: dict, interval: list[str]) -> str:
    rows = "\n".join(
        f"| [{c['title']}](./{infra_group(c['id'])}/{c['id']}/README.md) | `{c['id']}` | "
        f"{aggs[c['id']]['rows']:,} | {len(aggs[c['id']]['folders']):,} | "
        f"{aggs[c['id']]['bytes'] / 1e6:,.0f} MB |"
        for c in collections)
    return f"""\
# {root['title']}

{root['description']}

| Collection | Layer | Rows | Files | Size |
|---|---|---|---|---|
{rows}

## Versions

Only `latest/` is published, and each build replaces it. This build's data is
as of {interval[0]} (the Geofabrik extracts' timestamps, listed in
ATTRIBUTION.txt).

## Access

Every layer is one file per country and state:
`{DATA_URL}/country=<ISO2>/state=<slug>/<layer>.parquet`, readable in place
with HTTP range requests. `{PUBLIC_DATA}/index.json` lists the folders with
their names. The same paths exist under `s3://{BUCKET}/{DATASET_PREFIX}/latest/`
on the anonymous S3 endpoint `{S3_ENDPOINT}`, where each collection's
`partition:glob` reads the whole world. GeoPQ Workbench has the dataset built
in as a repository.

## Provenance

{PROVENANCE}

## License

{LICENSE_MD.format(attribution=f"{PUBLIC_DATA}/ATTRIBUTION.txt")}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {infra_group(c['id'])}/{c['id']}/AGENTS.md"
                        for c in collections)
    return f"""\
# {CATALOG_ID}: agent guide

OpenStreetMap infrastructure (power, telecoms, oil and gas, water) as
GeoParquet 2.0, worldwide, one collection per layer, one file per country and
state. Only the latest build is published. {PREVIEW}

## Collections

{listing}

## Access

- One place: `{DATA_URL}/country=<ISO2>/state=<slug>/<layer>.parquet` over
  HTTPS, no credentials. `{PUBLIC_DATA}/index.json` maps folders to names.
- Everything: the collection's `partition:glob` on the anonymous S3 endpoint
  `{S3_ENDPOINT}`, path-style, empty credentials.

## Conventions

- Geometry is native Parquet GEOMETRY, OGC:CRS84; `bbox` is the covering.
- `country` is ISO 3166-1 alpha-2 (`_intl` on the high seas); `state` is the
  slug of the FAO GAUL 2024 first-level unit, or `_offshore`, `_intl`.
- Voltages are in kV (`voltages_kv` per circuit, `voltage_kv` the highest),
  outputs in MW, lengths in km, areas in m², heights in m.
- `lifecycle` is active, construction, proposed, disused or abandoned.
- `tags` holds every OSM tag. Data (c) OpenStreetMap contributors, ODbL 1.0.
"""


# ---------- build ----------

def build(out_dir: Path, dest: Path, *, attribution: str | None = None,
          sizes_from: str | None = None, updated: str | None = None) -> dict:
    """Render the catalog tree into dest (replaced). Returns the measures."""
    for layer in infra.LAYERS:
        if layer.name not in LAYER_DOCS:
            sys.exit(f"{layer.name}: no entry in LAYER_DOCS")
        if not (THUMBS / f"{layer.name}.png").is_file():
            sys.exit(f"missing thumbnail {THUMBS}/{layer.name}.png: run the thumbnails command")
    docs = column_docs()
    interval = read_as_of(attribution)
    updated = updated or now_rfc3339()
    aggs = measure(out_dir, remote_sizes(sizes_from) if sizes_from else None)
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    collections = []
    for layer in infra.LAYERS:
        agg = aggs[layer.name]
        col = build_collection(layer.name, agg, docs, interval, updated)
        cdir = dest / layer.group / col["id"]
        write_json(cdir / "collection.json", col)
        shutil.copyfile(THUMBS / f"{layer.name}.png", cdir / "thumbnail.png")
        (cdir / "README.md").write_text(collection_readme(col, agg, interval))
        (cdir / "AGENTS.md").write_text(collection_agents(col, agg))
        collections.append(col)
    for group in GROUPS:
        members = [c for c in collections if infra_group(c["id"]) == group]
        cat = build_group(group, members, interval, updated)
        write_json(dest / group / "catalog.json", cat)
        (dest / group / "README.md").write_text(group_readme(cat, members, aggs))
        (dest / group / "AGENTS.md").write_text(group_agents(cat, members))
    root = build_root(collections, interval, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, collections, aggs, interval))
    (dest / "AGENTS.md").write_text(root_agents(collections))
    return aggs


def check(tree: Path) -> int:
    # Metadata pass only: the data hrefs are 41,000 remote files.
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path, remote: str, *, attribution: str | None, sizes_from: str | None,
            dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="infra-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        aggs = build(out_dir, tree, attribution=attribution, sizes_from=sizes_from)
        print(f"  rendered {len(aggs)} collections, "
              f"{sum(a['rows'] for a in aggs.values()):,} rows")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

# Open Infrastructure Map's colours, as GeoPQ Workbench draws the layers.
COLORS = {
    "power_line": "#c73030", "power_circuit": "#b54eb2", "power_tower": "#444444",
    "power_substation": "#7c4544", "power_plant": "#6b5947", "power_generator": "#726ba9",
    "power_switchgear": "#1e1e1e", "power_other": "#999999", "telecom_cable": "#61637a",
    "telecom_building": "#7d59ab", "telecom_location": "#61637a", "telecom_mast": "#61637a",
    "telecom_antenna": "#61637a", "utility_pole": "#444444", "street_cabinet": "#61637a",
    "pipeline": "#ea972d", "petroleum_site": "#6b6b6b", "petroleum_well": "#6b6b6b",
    "offshore_platform": "#6b6b6b", "pipeline_feature": "#6b6b6b", "marker": "#8a8a8a",
    "water_treatment_plant": "#7bbaac", "wastewater_plant": "#c19653",
    "pumping_station": "#7b7cba", "water_tower": "#7b7cba", "water_well": "#7b7cba",
    "pressurised_waterway": "#7b7cba",
}
WORLD = (-180.0, -60.0, 180.0, 84.0)


def thumbnails(out_dir: Path, names: list[str]) -> None:
    """A world density map per layer: the count of features per cell of a
    grid, log-scaled, in the layer's colour. Points and lines alike are
    placed by their bbox centre, so 40 M towers draw in a second."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.colors import LinearSegmentedColormap
    from thumbnails import BACKGROUND, frame

    x0, y0, x1, y1, aspect = frame(*WORLD)
    nx, ny = 600, 400
    con = duckdb.connect()
    THUMBS.mkdir(parents=True, exist_ok=True)
    for name in names:
        glob = str(out_dir / "country=*" / "state=*" / f"{name}.parquet")
        cells = con.execute(f"""
            SELECT floor(((bbox.xmin + bbox.xmax) / 2 - {x0}) / {(x1 - x0) / nx})::INT AS i,
                   floor(((bbox.ymin + bbox.ymax) / 2 - {y0}) / {(y1 - y0) / ny})::INT AS j,
                   count(*) AS n
            FROM read_parquet('{glob}') GROUP BY 1, 2""").fetchall()
        grid = np.zeros((ny, nx))
        for i, j, n in cells:
            if 0 <= i < nx and 0 <= j < ny:
                grid[j, i] = n
        cmap = LinearSegmentedColormap.from_list(name, [BACKGROUND, COLORS[name]])
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.imshow(np.log1p(grid), origin="lower", extent=(x0, x1, y0, y1), cmap=cmap,
                  vmin=0, vmax=max(np.log1p(grid).max(), 1) * 0.8, interpolation="nearest")
        ax.set_aspect(aspect)
        ax.axis("off")
        fig.patch.set_facecolor(BACKGROUND)
        path = THUMBS / f"{name}.png"
        fig.savefig(path, dpi=100, facecolor=BACKGROUND)
        plt.close(fig)
        print(f"  {name:24} {int(grid.sum()):>11,} features -> {path.relative_to(REPO)}")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="render the catalog tree")
    b.add_argument("--out-dir", type=Path, required=True, help="infra.py output (latest/)")
    b.add_argument("--dest", type=Path, required=True)
    b.add_argument("--attribution", help="ATTRIBUTION.txt path or URL (default: published)")
    b.add_argument("--remote-sizes", metavar="REMOTE",
                   help="take file sizes from this rclone dataset prefix, not the out dir")

    c = sub.add_parser("check", help="rashid pass over a rendered tree")
    c.add_argument("tree", type=Path)

    u = sub.add_parser("publish", help="build, check, upload to <remote>/catalog/")
    u.add_argument("--out-dir", type=Path, required=True)
    u.add_argument("--remote", required=True,
                   help="rclone path of the dataset prefix, e.g. parquetry:parquetry/osm-infrastructure")
    u.add_argument("--attribution")
    u.add_argument("--remote-sizes", metavar="REMOTE")
    u.add_argument("--dry-run", action="store_true")

    t = sub.add_parser("thumbnails", help="render catalog/thumbnails/osm-infrastructure/<layer>.png")
    t.add_argument("--out-dir", type=Path, required=True)
    t.add_argument("--layers", nargs="*", default=[l.name for l in infra.LAYERS])

    args = p.parse_args()
    if args.cmd == "build":
        aggs = build(args.out_dir, args.dest, attribution=args.attribution,
                     sizes_from=args.remote_sizes)
        print(f"wrote {args.dest}/ ({sum(a['rows'] for a in aggs.values()):,} rows "
              f"over {len(aggs)} collections)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    elif args.cmd == "publish":
        publish(args.out_dir, args.remote, attribution=args.attribution,
                sizes_from=args.remote_sizes, dry_run=args.dry_run)
    elif args.cmd == "thumbnails":
        thumbnails(args.out_dir, args.layers)


if __name__ == "__main__":
    main()
