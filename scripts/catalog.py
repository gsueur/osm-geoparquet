#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for one snapshot.

The catalog is a metadata layer over the bucket. It copies no data: one STAC
Collection per theme, partitioned (partition extension) with `partition:glob`
pointing at the dated snapshot through the anonymous S3 facade (api/). The
glob uses s3:// on purpose: expanding it needs a listing, which plain HTTPS
does not provide, and PORTO-FMT-020 exempts the glob from the https-only rule
for that reason.

Everything measurable (row counts, file counts, extents, schemas, source
timestamps) comes from the per-region _manifest.json files the pipeline
writes (schema >= 0.3.0), so building the catalog reads no parquet. The prose
lives here. Spec: portolan-spec v0.2.0. Validator: rashid, pinned in
pyproject.toml.

Two kinds of catalog, both written by publish.py's finalize stage after the
completeness gate:

  catalog/catalog.json          the live one. Its globs read latest/, so it
                                stays current, and its URL is what the
                                registry points at. Rebuilt nightly.
  catalog/<YYYY-MM-DD>/         a pinned copy, published only for snapshots
                                retention keeps (the first of each month, and
                                Dec 31), with globs pinned to that date. Never
                                rewritten, and pruned with its snapshot.

The live catalog links each pinned one as rel:'predecessor-version', and each
pinned one links back as rel:'latest-version'. Neither rel is structural, so a
validator does not try to resolve them inside the tree.

Usage:
  # render from local manifests (a pipeline out/ dir or a manifest mirror)
  python3 scripts/catalog.py build --manifests out/ --date 2026-09-16 --dest build/catalog
  # metadata pass (what finalize runs before uploading)
  python3 scripts/catalog.py check build/catalog
  # data pass over local partitions (what each nightly build job runs)
  python3 scripts/catalog.py check-data --out-dir out/
  # rashid must pass the fixture catalog and fail each planted violation
  python3 scripts/catalog.py selftest
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from themes import THEMES, Theme  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
ASSETS = REPO / "catalog"
FIXTURES = REPO / "tests" / "fixtures" / "manifests"
LOGO = REPO / "site" / "public" / "logo-192.png"

PUBLIC_BASE = "https://parquetry.geomermaids.com"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_BASE}/{CATALOG_PREFIX}"
BUCKET = "parquetry"
S3_ENDPOINT = "s3.geomermaids.com"
SITE_URL = "https://geoparquet.geomermaids.com/"
REPO_URL = "https://github.com/gsueur/osm-geoparquet"
CONTACT_EMAIL = "gsueur@geomermaids.com"
CATALOG_ID = "osm-geoparquet"

# Every object declares the same profile version (PORTO-CORE-006, PTL-CNF-002).
PORTOLAN_SCHEMA = "https://schemas.portolan-sdi.org/portolan/v0.2.0/schema.json"
PARTITION_EXT = "https://schemas.portolan-sdi.org/incubating/partition/v1.0.0/schema.json"
TABLE_EXT = "https://stac-extensions.github.io/table/v1.2.0/schema.json"
FILE_EXT = "https://stac-extensions.github.io/file/v2.1.0/schema.json"
VERSION_EXT = "https://stac-extensions.github.io/version/v1.2.0/schema.json"

LICENSE = "ODbL-1.0"
LICENSE_LINK = {
    "rel": "license",
    "href": "https://opendatacommons.org/licenses/odbl/1-0/",
    "type": "text/html",
    "title": "Open Database License (ODbL) 1.0",
}
VIA_LINK = {
    "rel": "via",
    "href": "https://www.openstreetmap.org/",
    "type": "text/html",
    "title": "OpenStreetMap",
}

# Mirror semantics: producer and host differ (spec: Source Provenance). The
# host is listed last and exactly once (spec: Providers).
PROVIDERS = [
    {
        "name": "OpenStreetMap contributors",
        "description": "Created and maintain the data, published under the ODbL 1.0.",
        "url": "https://www.openstreetmap.org/",
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geofabrik GmbH",
        "description": "Publishes the daily regional OSM extracts this catalog is built from.",
        "url": "https://download.geofabrik.de/",
        "roles": ["processor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converts the extracts to GeoParquet nightly, and maintains and "
                       "hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]

COUNTRY_NAMES = {"US": "United States", "CA": "Canada", "MX": "Mexico"}

# (title, one-paragraph description). Every theme in themes.py needs an entry.
THEME_DOCS: dict[str, tuple[str, str]] = {
    "buildings": ("Buildings",
        "OSM ways and multipolygon relations tagged building=*, as polygons, "
        "with address, height and level tags promoted to columns."),
    "roads": ("Roads and Paths",
        "OSM ways tagged highway=*, as lines: motorways down to residential "
        "streets, tracks, footways and cycleways."),
    "railways": ("Railways",
        "OSM nodes and ways tagged railway=*: track as lines; stations, "
        "crossings, switches and other railway features as points."),
    "waterways": ("Waterways",
        "OSM ways tagged waterway=* (rivers, streams, canals, ditches, drains), "
        "as lines."),
    "water": ("Water Bodies",
        "OSM areas tagged natural=water or water=* (lakes, ponds, reservoirs, "
        "river areas), as polygons."),
    "landuse": ("Land Use",
        "OSM areas tagged landuse=* (residential, farmland, forest, industrial "
        "and so on), as polygons."),
    "natural_areas": ("Natural Areas",
        "OSM areas tagged natural=* other than water (wood, wetland, scrub, "
        "grassland, beach and so on), as polygons. Water bodies are in the "
        "Water Bodies collection."),
    "natural_features": ("Natural Point Features",
        "OSM nodes tagged natural=* (peaks, springs, trees, cave entrances and "
        "similar), as points, with elevation and prominence where tagged."),
    "places": ("Place Names",
        "OSM nodes tagged place=* (cities, towns, villages, hamlets, suburbs, "
        "neighbourhoods), as points, with population where tagged."),
    "boundaries": ("Administrative Boundaries",
        "OSM relations tagged boundary=administrative, as polygons, at every "
        "admin_level present in the region."),
    "pois": ("Points of Interest",
        "OSM nodes tagged amenity, shop, tourism, leisure, office or healthcare, "
        "as points. The same features mapped as areas are in the Points of "
        "Interest (Areas) collection."),
    "amenities_polygons": ("Points of Interest (Areas)",
        "OSM ways and relations tagged amenity, shop, tourism, leisure, office "
        "or healthcare, as polygons: schools, parks, malls, hospitals, parking "
        "lots and similar."),
    "power": ("Power Infrastructure",
        "OSM nodes, ways and relations tagged power=*: lines and cables, towers "
        "and poles, substations, plants and generators."),
    "aeroways": ("Aeroways",
        "OSM nodes, ways and relations tagged aeroway=*: aerodromes, runways, "
        "taxiways, aprons, helipads and gates."),
    "barriers": ("Barriers",
        "OSM nodes and ways tagged barrier=*: fences, walls, hedges, gates, "
        "bollards and similar."),
    "public_transport": ("Public Transport Stops",
        "OSM nodes tagged public_transport=*, highway=bus_stop, or "
        "railway=station, halt, tram_stop or subway_entrance, as points."),
}

BASE_COLUMN_DOCS = {
    "osm_id": "OSM element id. Unique only together with osm_type, since nodes, "
              "ways and relations have separate id spaces. An area carries the id "
              "of the way or relation it was built from. A feature crossing a "
              "region border appears once per region it touches.",
    "osm_type": "OSM element type: node, way or relation.",
    "country": "ISO 3166-1 alpha-2 country code. Same value as the country= "
               "partition key.",
    "state_name": "Name of the admin region (state, province or territory) the "
                  "file was cut to.",
    "state_iso": "ISO 3166-2 code of the admin region, e.g. US-NY or CA-ON. Same "
                 "value as the state= partition key.",
    "tags": "Every OSM tag on the element, as a key to value map. The promoted "
            "columns are typed copies of single tags.",
    "bbox": "Per-feature bounding box in float32, rounded outward so it always "
            "contains the geometry (at most about 9 m of slack). Its Parquet "
            "min/max statistics let any engine skip row groups: filter on "
            "bbox.xmin/ymin/xmax/ymax before touching geometry. It is not "
            "declared as a GeoParquet covering.",
    "geometry": "Feature geometry, native Parquet GEOMETRY (GeoParquet 2.0), "
                "OGC:CRS84 longitude/latitude.",
}

GEOMETRY_TYPES = {"point": "Point", "linestring": "LineString", "polygon": "MultiPolygon"}
_TAG_RE = re.compile(r"tags\['([^']+)'\]\s+AS\s+(\w+)")
CAST_NAMES = {"VARCHAR": "text", "INT": "an integer", "DOUBLE": "a number"}

MEDIA_TYPES = {
    ".json": "application/json",
    ".md": "text/markdown; charset=utf-8",
    ".png": "image/png",
}


# ---------- measured inputs ----------

def load_manifests(root: Path) -> list[dict]:
    paths = sorted(root.glob("country=*/state=*/_manifest.json"))
    if not paths:
        sys.exit(f"no country=*/state=*/_manifest.json under {root}")
    manifests = []
    for p in paths:
        m = json.loads(p.read_text())
        if "theme_stats" not in m:
            sys.exit(f"{p}: manifest schema {m.get('schema_version')!r} has no "
                     f"theme_stats; the catalog needs pipeline schema >= 0.3.0")
        manifests.append(m)
    return manifests


def aggregate(manifests: list[dict]) -> dict[str, dict]:
    """Per theme: rows, files, regions, extent and the single shared schema.

    Fails if two partitions of a theme disagree on columns or types: every
    partition MUST share one Parquet schema so the glob reads as one table
    (formats.md, Partitioned Collections).
    """
    out: dict[str, dict] = {}
    for t in THEMES:
        rows = files = 0
        bbox = [180.0, 90.0, -180.0, -90.0]
        columns: list | None = None
        columns_from = None
        for m in manifests:
            n = m["themes"].get(t.name, 0)
            if n <= 0:
                continue
            st = m["theme_stats"][t.name]
            rows += n
            files += 1
            b = st["bbox"]
            bbox = [min(bbox[0], b[0]), min(bbox[1], b[1]),
                    max(bbox[2], b[2]), max(bbox[3], b[3])]
            if columns is None:
                columns, columns_from = st["columns"], m["state_iso"]
            elif st["columns"] != columns:
                sys.exit(f"{t.name}: schema of {m['state_iso']} differs from "
                         f"{columns_from}; partitions must share one schema")
        if files:
            out[t.name] = {"rows": rows, "files": files, "bbox": bbox, "columns": columns}
    return out


def temporal_interval(manifests: list[dict], date: str) -> list[str]:
    """The OSM replication timestamps the snapshot reflects. Falls back to the
    snapshot date when no manifest recorded one."""
    stamps = sorted(m["source_timestamp"] for m in manifests if m.get("source_timestamp"))
    if not stamps:
        return [f"{date}T00:00:00Z", f"{date}T00:00:00Z"]
    return [stamps[0], stamps[-1]]


# ---------- rendering ----------

def multihash(path: Path) -> str:
    """sha2-256 as a multihash: 0x12 (function) 0x20 (length) + digest."""
    return "1220" + hashlib.sha256(path.read_bytes()).hexdigest()


def column_doc(theme: Theme, name: str) -> str:
    if name in BASE_COLUMN_DOCS:
        doc = BASE_COLUMN_DOCS[name]
        if name == "geometry":
            kinds = [GEOMETRY_TYPES[g] for g in theme.geometry_types.split(",")]
            doc += f" Geometry types: {', '.join(kinds)}."
        return doc
    expr = dict(theme.typed_columns).get(name)
    m = _TAG_RE.search(expr or "")
    if not m:
        sys.exit(f"{theme.name}.{name}: no documentation and not a promoted tag")
    key, cast = m.group(1), m.group(2)
    doc = f"Value of the OSM {key} tag as {CAST_NAMES[cast]}"
    doc += "; NULL when absent." if cast == "VARCHAR" else \
           "; NULL when absent or not a valid number."
    return doc + f" See https://wiki.openstreetmap.org/wiki/Key:{key}"


def partition_glob(theme: str, target: str, local: bool) -> str:
    if local:
        return f"./country=*/state=*/{theme}.parquet"
    return f"s3://{BUCKET}/{target}/country=*/state=*/{theme}.parquet"


def country_list(countries: list[str]) -> str:
    names = [("the " if c == "US" else "") + COUNTRY_NAMES.get(c, c) for c in countries]
    return names[0] if len(names) == 1 else ", ".join(names[:-1]) + " and " + names[-1]


def as_of(interval: list[str]) -> str:
    return interval[0] if interval[0] == interval[1] else f"{interval[0]} to {interval[1]}"


def md_link(rel: str, href: str, title: str) -> dict:
    return {"rel": rel, "href": href, "type": "text/markdown", "title": title}


def build_collection(theme: Theme, agg: dict, date: str, updated: str,
                     interval: list[str], countries: list[str], local: bool,
                     target: str) -> dict:
    title, description = THEME_DOCS[theme.name]
    glob = partition_glob(theme.name, target, local)
    tracking = ("the nightly build, so its contents change every night"
                if target == "latest" else
                f"the immutable {target} snapshot")
    description = (
        f"{description} Cut to the admin regions (states, provinces and "
        f"territories) of {country_list(countries)}, one GeoParquet file per region. "
        f"This collection reads {tracking}. "
        f"Read every region at once with the partition glob {glob} through the "
        f"anonymous S3 endpoint {S3_ENDPOINT} (path-style, empty credentials), "
        f"or one region over plain HTTPS. The README gives both setups."
    )
    thumb = ASSETS / "thumbnails" / f"{theme.name}.png"
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT, VERSION_EXT],
        "id": theme.name,
        "title": title,
        "description": description,
        "keywords": ["OpenStreetMap", "OSM", "GeoParquet", theme.name.replace("_", " "),
                     *(COUNTRY_NAMES.get(c, c) for c in countries)],
        "license": LICENSE,
        "version": date,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {
            "spatial": {"bbox": [agg["bbox"]]},
            "temporal": {"interval": [interval]},
        },
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [
            {"name": "country", "type": "string",
             "description": "ISO 3166-1 alpha-2 country code."},
            {"name": "state", "type": "string",
             "description": "ISO 3166-2 code of the admin region (state, province "
                            "or territory), e.g. US-NY. One file per region."},
        ],
        "partition:file_count": agg["files"],
        "partition:glob": glob,
        "table:row_count": agg["rows"],
        "table:primary_geometry": "geometry",
        "table:columns": [
            {"name": n, "type": t, "description": column_doc(theme, n)}
            for n, t in agg["columns"]
        ],
        "assets": {
            "thumbnail": {
                "href": "./thumbnail.png",
                "type": "image/png",
                "title": "Preview of one region, flat styling by geometry type",
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
            {"rel": "alternate", "href": f"{PUBLIC_BASE}/{target}/", "type": "text/html",
             "title": f"Browse the {target} files"},
        ],
    }


def build_root(collections: list[dict], date: str, updated: str, countries: list[str],
               target: str, archives: list[str]) -> dict:
    """The live catalog (target 'latest') or a pinned archive (target = a date).

    The live one links each archive as a predecessor version; an archive links
    back to the live one. Those rels are not structural, so a validator does
    not try to resolve them inside the tree.
    """
    names = country_list(countries)
    live = target == "latest"
    tracks = ("Its collections read latest/, the alias the nightly build "
              f"refreshes, and it was generated from the {date} snapshot."
              if live else
              f"Its collections are pinned to the immutable {date} snapshot.")
    if live:
        version_links = [
            {"rel": "predecessor-version", "href": f"{CATALOG_URL}/{d}/catalog.json",
             "type": "application/json", "title": f"{d} snapshot, pinned"}
            for d in archives
        ]
    else:
        version_links = [
            {"rel": "latest-version", "href": f"{CATALOG_URL}/catalog.json",
             "type": "application/json", "title": "Current data (latest)"},
        ]
    self_href = (f"{CATALOG_URL}/catalog.json" if live
                 else f"{CATALOG_URL}/{date}/catalog.json")
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID if live else f"{CATALOG_ID}-{date}",
        "title": ("OpenStreetMap GeoParquet for North America" if live else
                  f"OpenStreetMap GeoParquet for North America, {date}"),
        "description": (
            f"OpenStreetMap for {names}, split into {len(collections)} thematic "
            f"collections and partitioned by country and admin region, as "
            f"Hilbert-ordered GeoParquet 2.0, rebuilt nightly from Geofabrik "
            f"extracts. {tracks} Data (c) OpenStreetMap contributors, ODbL 1.0."
        ),
        "version": date,
        "updated": updated,
        "links": [
            {"rel": "self", "href": self_href, "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json",
               "type": "application/json", "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", "Catalog README"),
            md_link("agents", "./AGENTS.md", "Catalog agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "vcs", "href": REPO_URL, "type": "text/html",
             "title": "Pipeline and catalog source"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
            {"rel": "related", "href": f"{PUBLIC_BASE}/snapshots.json",
             "type": "application/json", "title": "Index of every retained snapshot"},
            *version_links,
            {"rel": "alternate", "href": SITE_URL, "type": "text/html",
             "title": "Project site"},
        ],
    }


# ---------- markdown ----------

def fmt_bbox(b: list[float]) -> str:
    return ", ".join(f"{v:.4f}" for v in b)


def access_section(theme: str, target: str) -> str:
    switch = (
        "Replace `latest` with a snapshot date, e.g. "
        f"`{PUBLIC_BASE}/2026-09-01/...`, to pin data that does not move under "
        "you. Monthly snapshots are kept, and each has its own catalog under "
        f"{CATALOG_URL}/<date>/catalog.json."
        if target == "latest" else
        f"`{target}` is immutable. Replace it with `latest` to follow the "
        "nightly build instead.")
    return f"""\
One region over plain HTTPS, no credentials:

```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
SELECT count(*)
FROM read_parquet('{PUBLIC_BASE}/{target}/country=US/state=US-RI/{theme}.parquet');
```

Every region at once, through the anonymous S3 endpoint (plain HTTPS cannot
expand a glob because it has no listing):

```sql
CREATE SECRET parquetry (TYPE s3, KEY_ID '', SECRET '',
    ENDPOINT '{S3_ENDPOINT}', URL_STYLE 'path');
SELECT state, count(*)
FROM read_parquet('s3://{BUCKET}/{target}/country=*/state=*/{theme}.parquet')
GROUP BY state ORDER BY 2 DESC;
```

DuckDB reads `country` and `state` from the path (Hive partitioning), so a
`WHERE state = 'US-NY'` filter skips every other file without opening it.
{switch}"""


PROVENANCE = f"""\
OpenStreetMap data (c) OpenStreetMap contributors, via the daily regional
extracts published by [Geofabrik](https://download.geofabrik.de/). Each night
the [pipeline]({REPO_URL}) cuts every extract to admin-region polygons with
`osmium extract` (simple strategy: a way crossing a border keeps only its
nodes inside the region), selects each theme with `osmium tags-filter`,
exports geometries with `osmium export`, and writes GeoParquet 2.0 with
DuckDB: Hilbert-ordered rows, zstd level 15, 50,000-row row groups, bloom
filters on the promoted columns. A snapshot goes live only after every region
and theme is complete."""

LICENSE_MD = """\
[Open Database License (ODbL) 1.0](https://opendatacommons.org/licenses/odbl/1-0/).
Credit "(c) OpenStreetMap contributors" in any product built on this data, and
publish derived databases under the ODbL."""


def collection_readme(theme: Theme, col: dict, agg: dict, date: str, target: str) -> str:
    interval = col["extent"]["temporal"]["interval"][0]
    schema = "\n".join(
        f"| `{c['name']}` | `{c['type']}` | {c['description']} |" for c in col["table:columns"])
    reads = ("`latest/`, refreshed nightly" if target == "latest"
             else f"`{target}/`, immutable")
    return f"""\
# {col['title']}

{THEME_DOCS[theme.name][1]}

| | |
|---|---|
| Reads | {reads} |
| Counted on | {date} (OSM data as of {as_of(interval)}) |
| Rows | {agg['rows']:,} in {agg['files']} files, one per admin region |
| Extent | {fmt_bbox(agg['bbox'])} (lon/lat) |
| Selection | `osmium tags-filter {theme.osmium_filter}`, exported as {theme.geometry_types.replace(',', ', ')} |
| Partition glob | `{col['partition:glob']}` |
| License | ODbL 1.0, (c) OpenStreetMap contributors |

## Access

{access_section(theme.name, target)}

## Schema

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

{PROVENANCE}

## License

{LICENSE_MD}
"""


def collection_agents(theme: Theme, col: dict, target: str) -> str:
    promoted = ", ".join(f"`{n}`" for n, _ in theme.typed_columns)
    return f"""\
# {col['title']}: agent guide

Each row is one OSM element ({theme.geometry_types.replace(',', ', ')}) matching
`osmium tags-filter {theme.osmium_filter}`, cut to one admin region.

## Access

{access_section(theme.name, target)}

## Query tips

- Filter space on the `bbox` struct first, e.g. `WHERE bbox.xmin <= -73.9
  AND bbox.xmax >= -74.0 AND bbox.ymin <= 40.8 AND bbox.ymax >= 40.7`. Its
  Parquet statistics prune row groups in any engine; rows are Hilbert-ordered,
  so a small window reads a small slice of each file.
- Promoted tag columns: {promoted}. Any other tag is in the `tags` map:
  `tags['key']`, NULL when absent.
- Filter regions on the `state` path key (ISO 3166-2, e.g. `US-NY`) rather
  than the `state_iso` column: the path key skips files without opening them.
  `state_name` is the region name.
- A feature crossing a region border is in every region it touches, cut to
  the nodes inside each one. Across the glob, count features with
  `count(DISTINCT (osm_type, osm_id))`, and treat summed lengths or areas
  near borders as approximate.
- Snapshots are immutable under `/<YYYY-MM-DD>/`; `/latest/` follows the
  nightly build. Retention: {PUBLIC_BASE}/snapshots.json.
- Attribution is required: "(c) OpenStreetMap contributors", ODbL 1.0.
"""


def versions_section(date: str, target: str, archives: list[str]) -> str:
    if target != "latest":
        return (f"""\
This catalog is pinned to the immutable `{date}` snapshot, so its numbers and
its files stay as they are. [Current data]({CATALOG_URL}/catalog.json) follows
the nightly build.""")
    listing = "\n".join(
        f"- [{d}]({CATALOG_URL}/{d}/catalog.json)" for d in reversed(archives)
    ) or "- none yet"
    return f"""\
This catalog tracks `latest/`, the alias the nightly build refreshes, and was
generated from the {date} snapshot. Its row counts and extents describe that
build; the files behind it change every night.

For work that has to be reproducible, use a pinned catalog instead. Every
snapshot kept beyond the 14-day daily window (the first of each month, and
31 December as the yearly anchor) gets one, and it is removed when its
snapshot is:

{listing}"""


def root_readme(root: dict, collections: list[dict], aggs: dict, date: str,
                target: str, archives: list[str]) -> str:
    rows = "\n".join(
        f"| [{c['title']}](./{c['id']}/README.md) | `{c['id']}` | "
        f"{aggs[c['id']]['rows']:,} | {aggs[c['id']]['files']} |" for c in collections)
    return f"""\
# {root['title']}

{root['description']}

| Collection | File stem | Rows | Files |
|---|---|---|---|
{rows}

## Versions

{versions_section(date, target, archives)}

## Access

Files live at `{PUBLIC_BASE}/<YYYY-MM-DD | latest>/country=<CC>/state=<ISO>/<theme>.parquet`
and can be read in place with HTTP range requests. Each collection's README
shows how to read one region over HTTPS and every region through the
anonymous S3 endpoint `{S3_ENDPOINT}`. Retained snapshots are listed in
[snapshots.json]({PUBLIC_BASE}/snapshots.json).

## Provenance

{PROVENANCE}

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(collections: list[dict], date: str, target: str) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    reads = (f"""\
Reads `latest/`, which the nightly build refreshes, so the same query can
return different numbers tomorrow. The counts here were taken from the {date}
snapshot. For a result someone can reproduce, use a pinned catalog under
{CATALOG_URL}/<date>/catalog.json: one exists for every snapshot kept beyond
the 14-day daily window, and the live catalog links them as
rel:'predecessor-version'."""
        if target == "latest" else f"""\
Pinned to the immutable `{date}` snapshot. Current data is at
{CATALOG_URL}/catalog.json (rel:'latest-version').""")
    return f"""\
# {CATALOG_ID}: agent guide

OpenStreetMap as GeoParquet 2.0, one collection per theme, one file per admin
region (US states, Canadian provinces and territories, Mexican states).

## Version

{reads}

## Collections

{listing}

## Access

- One file: `{PUBLIC_BASE}/{target}/country=US/state=US-NY/buildings.parquet`
  over HTTPS, no credentials. DuckDB, polars, pyarrow and GDAL read it in
  place with range requests.
- All regions of a theme: the `partition:glob` of its collection,
  `s3://{BUCKET}/{target}/country=*/state=*/<theme>.parquet`, through the
  anonymous S3 endpoint `{S3_ENDPOINT}` with path-style addressing and empty
  credentials. Plain HTTPS cannot expand a glob.
- Every file of a theme shares one schema, documented in the collection's
  `table:columns`.

## Conventions

- Geometry is native Parquet GEOMETRY, OGC:CRS84 lon/lat.
- `bbox` is a per-row float32 box rounded outward; filter on it before
  touching geometry.
- `(osm_type, osm_id)` identifies an OSM element; it repeats across regions
  for features that cross a border.
- Data (c) OpenStreetMap contributors, ODbL 1.0. Attribution is required.
"""


# ---------- build ----------

def now_rfc3339() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")


def build(manifests_dir: Path, date: str, dest: Path, *, updated: str | None = None,
          local_glob: bool = False, target: str = "latest",
          archives: list[str] = ()) -> dict:
    """Render the catalog tree into dest (replaced). Returns per-theme totals.

    `target` is what the globs read: "latest" for the live catalog, or a
    snapshot date for a pinned archive. `archives` are the pinned catalogs the
    live one links as predecessor versions.

    The published catalog needs rows in every theme. The local data check
    (local_glob) runs per build job over a few regions, where a theme can be
    legitimately empty, so it just leaves that collection out."""
    missing = {t.name for t in THEMES} ^ set(THEME_DOCS)
    if missing:
        sys.exit(f"THEME_DOCS and themes.py disagree on: {sorted(missing)}")
    if target not in ("latest", date):
        # A pinned catalog takes its id, self link and version from the
        # snapshot it reads, so it can only describe its own date.
        sys.exit(f"pinned catalog target {target} does not match date {date}")
    manifests = load_manifests(manifests_dir)
    aggs = aggregate(manifests)
    empty = [t.name for t in THEMES if t.name not in aggs]
    if empty and not local_glob:
        sys.exit(f"no partition has any rows for: {', '.join(empty)}")
    interval = temporal_interval(manifests, date)
    countries = sorted({m["country"] for m in manifests})
    updated = updated or now_rfc3339()

    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)

    collections = []
    for t in THEMES:
        if t.name not in aggs:
            continue
        col = build_collection(t, aggs[t.name], date, updated, interval, countries,
                               local_glob, target)
        cdir = dest / t.name
        write_json(cdir / "collection.json", col)
        shutil.copyfile(ASSETS / "thumbnails" / f"{t.name}.png", cdir / "thumbnail.png")
        (cdir / "README.md").write_text(collection_readme(t, col, aggs[t.name], date, target))
        (cdir / "AGENTS.md").write_text(collection_agents(t, col, target))
        collections.append(col)

    root = build_root(collections, date, updated, countries, target, list(archives))
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, collections, aggs, date, target, list(archives)))
    (dest / "AGENTS.md").write_text(root_agents(collections, date, target))
    return aggs


# ---------- validation ----------

# Byte checks (size, checksum, format) on the assets inside the tree, i.e. the
# thumbnails. The partitions sit behind a remote glob that rashid does not
# expand; check_data covers them.
LOCAL_DATA = ("--data-scope", "local")


def rashid(tree: Path, *args: str) -> tuple[int, list[dict]]:
    """Run rashid over tree. Returns (error count, findings)."""
    if not shutil.which("rashid"):
        sys.exit("rashid not on PATH: run through `uv run --project scripts`")
    # --schema adds the bundled Portolan profile schema to the metadata and
    # STAC structural passes; it is opt-in in rashid.
    r = subprocess.run(["rashid", "check", str(tree), "--json", "--all", "--schema", *args],
                       capture_output=True, text=True)
    try:
        report = json.loads(r.stdout)
    except json.JSONDecodeError:
        sys.exit(f"rashid produced no JSON report (exit {r.returncode}):\n"
                 f"{r.stdout[-2000:]}{r.stderr[-2000:]}")
    return report["error_count"], report["findings"]


def print_findings(findings: list[dict]) -> None:
    for f in findings:
        print(f"  {f.get('severity', '?'):7} {f.get('rule_id', '?')}  "
              f"{f.get('path', '')}{f.get('json_pointer', '')}  {f.get('message', '')}")


def check(tree: Path, *args: str) -> int:
    errors, findings = rashid(tree, *args)
    print_findings(findings)
    print(f"rashid: {errors} error(s), {len(findings) - errors} other finding(s)")
    return 1 if errors else 0


def check_data(out_dir: Path, date: str) -> int:
    """rashid's data pass over local partitions: spatial ordering, row-group
    statistics, the 150,000-row cap, GeoParquet version and one schema per
    theme (PTL-DAT-*). Those rules only expand a local relative glob, so this
    renders a throwaway catalog whose globs point into symlinked copies of
    out_dir instead of the bucket."""
    with tempfile.TemporaryDirectory(prefix="catalog-data-") as tmp:
        tree = Path(tmp) / "catalog"
        aggs = build(out_dir, date, tree, local_glob=True)
        for name in aggs:
            for country in out_dir.glob("country=*"):
                (tree / name / country.name).symlink_to(country.resolve())
        return check(tree, "--data-scope", "all")


# ---------- selftest (negative controls) ----------

def _mutations() -> list[tuple[str, callable]]:
    """(description, mutate(tree)) pairs, each a violation rashid must catch."""
    def edit(rel: str, fn):
        def apply(tree: Path) -> None:
            p = tree / rel
            doc = json.loads(p.read_text())
            fn(doc)
            p.write_text(json.dumps(doc))
        return apply

    def drop_link(rel_name: str):
        return lambda d: d.__setitem__("links", [l for l in d["links"] if l["rel"] != rel_name])

    return [
        ("proprietary license", edit("roads/collection.json", lambda d: d.__setitem__("license", "proprietary"))),
        ("no agents link", edit("roads/collection.json", drop_link("agents"))),
        ("no host provider", edit("roads/collection.json", lambda d: d.__setitem__("providers", d["providers"][:-1]))),
        ("no thumbnail", edit("roads/collection.json", lambda d: d["assets"].pop("thumbnail"))),
        ("mirror without via", edit("roads/collection.json", drop_link("via"))),
        ("stale checksum", edit("roads/collection.json", lambda d: d["assets"]["thumbnail"].__setitem__("file:checksum", "1220" + "0" * 64))),
        ("glob without partition keys", edit("roads/collection.json", lambda d: d.pop("partition:keys"))),
        ("missing README", lambda tree: (tree / "roads" / "README.md").unlink()),
    ]


def selftest(manifests_dir: Path) -> int:
    """Both catalogs must pass; every planted violation must fail. A validator
    that has never failed on this catalog proves nothing."""
    ok = True
    with tempfile.TemporaryDirectory(prefix="catalog-selftest-") as tmp:
        clean = Path(tmp) / "clean"
        # The live catalog, linking a pinned one, and a pinned catalog:
        # different ids, self links, globs and version links.
        for target, archives in (("latest", ["2026-09-01"]), ("2026-09-15", [])):
            build(manifests_dir, "2026-09-15", clean, updated="2026-09-15T06:00:00Z",
                  target=target, archives=archives)
            errors, findings = rashid(clean, *LOCAL_DATA)
            print(f"clean fixture catalog (target {target}): {errors} error(s)")
            if errors:
                print_findings(findings)
                ok = False
        # Violations are planted in the live one; the two differ only in
        # links and prose.
        build(manifests_dir, "2026-09-15", clean, updated="2026-09-15T06:00:00Z",
              target="latest", archives=["2026-09-01"])
        for desc, mutate in _mutations():
            tree = Path(tmp) / "broken"
            if tree.exists():
                shutil.rmtree(tree)
            shutil.copytree(clean, tree)
            mutate(tree)
            errors, findings = rashid(tree, *LOCAL_DATA)
            rules = sorted({f["rule_id"] for f in findings if f.get("severity") == "error"})
            status = "caught" if errors else "MISSED"
            print(f"  {status:6}  {desc:28} {', '.join(rules)}")
            ok &= bool(errors)
    print("selftest", "passed" if ok else "FAILED")
    return 0 if ok else 1


# ---------- upload ----------

def upload(tree: Path, remote: str, prefix: str, *, dry_run: bool) -> None:
    """Copy the rendered tree to <remote>/<prefix>/, root catalog.json last so
    it never links a child that is not there yet. Nothing is deleted; a theme
    dropped from themes.py leaves its old collection files behind until
    removed by hand.

    Cache-Control is short for the live catalog, which is rewritten nightly,
    and a year for a pinned one, which never changes."""
    pinned = prefix != CATALOG_PREFIX
    cache = ("public, max-age=31536000, immutable" if pinned
             else "public, max-age=300")
    files = sorted(p for p in tree.rglob("*") if p.is_file())
    root = tree / "catalog.json"
    files = [p for p in files if p != root] + [root]
    for p in files:
        rel = p.relative_to(tree).as_posix()
        cmd = ["rclone", "copyto", "--ignore-times",
               "--header-upload", f"Cache-Control: {cache}",
               "--header-upload", f"Content-Type: {MEDIA_TYPES[p.suffix]}",
               str(p), f"{remote}/{prefix}/{rel}"]
        if dry_run:
            print(f"  $ {' '.join(cmd)}")
        else:
            subprocess.run(cmd, check=True)
    print(f"  {len(files)} files -> {remote}/{prefix}/")


def build_check_upload(manifests_dir: Path, date: str, remote: str, prefix: str,
                       *, target: str, archives: list[str], dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        aggs = build(manifests_dir, date, tree, target=target, archives=archives)
        print(f"  rendered {len(aggs)} collections, "
              f"{sum(a['rows'] for a in aggs.values()):,} rows, reading {target}/")
        if check(tree, *LOCAL_DATA):
            sys.exit("catalog failed rashid; not uploading")
        upload(tree, remote, prefix, dry_run=dry_run)


def publish(manifests_dir: Path, date: str, remote: str, *,
            archives: list[str], pin: bool, dry_run: bool) -> None:
    """Publish the live catalog, and a pinned copy when this snapshot is one
    retention keeps. Used by publish.py finalize.

    The pinned copy goes up first, so the predecessor-version link the live
    catalog carries resolves as soon as anyone can follow it."""
    if pin:
        print(f"  pinned catalog for {date}")
        build_check_upload(manifests_dir, date, remote, f"{CATALOG_PREFIX}/{date}",
                           target=date, archives=[], dry_run=dry_run)
    print("  live catalog (latest)")
    build_check_upload(manifests_dir, date, remote, CATALOG_PREFIX,
                       target="latest", archives=archives, dry_run=dry_run)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="render the catalog tree")
    b.add_argument("--manifests", type=Path, required=True,
                   help="dir holding country=*/state=*/_manifest.json")
    b.add_argument("--date", required=True, help="snapshot date YYYY-MM-DD")
    b.add_argument("--dest", type=Path, required=True)

    c = sub.add_parser("check", help="rashid metadata pass over a rendered tree")
    c.add_argument("tree", type=Path)

    d = sub.add_parser("check-data", help="rashid data pass over local partitions")
    d.add_argument("--out-dir", type=Path, required=True)
    d.add_argument("--date", default=dt.date.today().isoformat())

    s = sub.add_parser("selftest", help="fixture catalog passes, planted violations fail")
    s.add_argument("--manifests", type=Path, default=FIXTURES)

    args = p.parse_args()
    if args.cmd == "build":
        aggs = build(args.manifests, args.date, args.dest)
        for name, a in aggs.items():
            print(f"  {name:20} {a['rows']:>12,} rows  {a['files']:>3} files")
        print(f"wrote {args.dest}/")
    elif args.cmd == "check":
        sys.exit(check(args.tree, *LOCAL_DATA))
    elif args.cmd == "check-data":
        sys.exit(check_data(args.out_dir, args.date))
    elif args.cmd == "selftest":
        sys.exit(selftest(args.manifests))


if __name__ == "__main__":
    main()
