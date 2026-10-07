#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the FEMA NFHL.

One STAC Collection, the flood hazard areas: one file per FEMA county-wide
delivery under latest/state=<XX>/<DFIRM_ID>.parquet, described through the
partition extension (`partition:glob`), and the index of those files,
latest/counties.parquet, as the `index` asset. latest/ is updated in place
every day by nfhl.py, so the catalog is rendered again after each update:
counts, sizes and the index checksum come from the manifests and the index
just published, column types from one delivery's footer (a range request
when the source is a URL). The prose lives here. Spec: portolan-spec
v0.2.0. Validator: rashid, pinned in pyproject.toml, shared with the other
catalogs (scripts/catalog.py), as are the STAC constants, the rashid wrapper
and the uploader.

The catalog is published beside the data, at /nfhl/catalog/catalog.json.

Usage:
  # render from a local nfhl.py output dir (footers read locally)
  python3 scripts/datasets/nfhl_catalog.py build --out-dir out/nfhl --dest build/nfhl-catalog
  # or from the published files (footers read over HTTPS)
  python3 scripts/datasets/nfhl_catalog.py build --dest build/nfhl-catalog
  python3 scripts/datasets/nfhl_catalog.py check build/nfhl-catalog
  python3 scripts/datasets/nfhl_catalog.py publish --remote parquetry:parquetry/nfhl
  # the committed thumbnail, re-rendered only when the data changes
  uv run --project scripts --with matplotlib python scripts/datasets/nfhl_catalog.py \\
      thumbnails --out-dir out/nfhl
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
from datasets.nfhl import INDEX, LATEST, ROW_GROUP_ROWS, STEM

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

DATASET_PREFIX = "nfhl"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "fema-nfhl-geoparquet"
COLLECTION_ID = STEM.replace("_", "-")
PARTITION_KEY = "state"
THUMB = REPO / "catalog" / "thumbnails" / "nfhl" / f"{STEM}.png"
PIPELINE_URL = "https://github.com/gsueur/nfhl-geoparquet-workshop"
FEMA_NFHL_URL = "https://hazards.fema.gov/femaportal/NFHL/searchResult"
MSC_URL = "https://msc.fema.gov/portal/home"

# A work of the US federal government: no copyright in the US (17 U.S.C. 105).
# CC-PDM-1.0 is the SPDX id that labels a work as free of known copyright.
LICENSE = "CC-PDM-1.0"
LICENSE_LINK = {
    "rel": "license",
    "href": "https://www.usa.gov/government-copyright",
    "type": "text/html",
    "title": "US government works are not subject to copyright (17 U.S.C. 105)",
}
VIA_LINK = {
    "rel": "via",
    "href": FEMA_NFHL_URL,
    "type": "text/html",
    "title": "FEMA Flood Map Service Center, NFHL downloads by county",
}

PROVIDERS = [
    {
        "name": "Federal Emergency Management Agency (FEMA)",
        "description": "Produces the National Flood Hazard Layer, the digital Flood "
                       "Insurance Rate Map data, and publishes it county by county.",
        "url": MSC_URL,
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Assembled the county deliveries, cut the zones into pieces, "
                       "converted them to GeoParquet, and maintains and hosts this "
                       "catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]

NOT_OFFICIAL = (
    "Not a FEMA product and not the effective Flood Insurance Rate Map: do not use "
    "it for flood zone determinations, insurance rating, regulatory or legal "
    "purposes; use the FEMA Flood Map Service Center for those. FEMA has not "
    "reviewed, approved or endorsed this redistribution.")

TITLE = "Flood hazard areas"
ABOUT = (
    "FEMA's flood zones (NFHL layer S_FLD_HAZ_AR) for every county-wide delivery "
    "that holds one, cut into pieces of at most 100 vertices so a point lookup "
    "tests a small polygon. A row is a piece of a zone, not a zone: piece_id "
    "numbers the pieces of a zone from 0. FEMA's attributes are kept, and a "
    "risk reading of the zone and subtype is added.")

COLUMN_DOCS = {
    "state": "US postal code of the state or territory of the county delivery "
             "(LA, PR...). Also the partition key.",
    "county": "County (parish, borough...) of the delivery, title case, without the "
              "COUNTY / PARISH suffix.",
    "dfirm_id": "FEMA DFIRM_ID of the delivery: the county FIPS code and C for "
                "county-wide (22071C). Also the file name.",
    "fema_update_date": "Date of the county delivery on the FEMA portal, from its "
                        "file name. Counties are updated independently.",
    "source_feature_id": "FEMA's FLD_AR_ID of the zone the piece comes from. Not "
                         "unique inside every county: see the collection README.",
    "piece_id": "Number of the piece inside its zone, from 0. `piece_id = 0` counts "
                "zones.",
    "flood_zone": "FEMA flood zone (FLD_ZONE), upper case: A, AE, AH, AO, VE, X, D, "
                  "OPEN WATER, AREA NOT INCLUDED...",
    "zone_subtype": "FEMA zone subtype (ZONE_SUBTY), e.g. FLOODWAY, 0.2 PCT ANNUAL "
                    "CHANCE FLOOD HAZARD, AREA OF MINIMAL FLOOD HAZARD. NULL when "
                    "blank.",
    "sfha": "FEMA SFHA_TF: true inside the Special Flood Hazard Area (1 percent "
            "annual chance).",
    "static_bfe": "FEMA STATIC_BFE: base flood elevation in the zone's vertical "
                  "datum, feet. NULL where FEMA writes -9999.",
    "dual_zone": "FEMA DUAL_ZONE: true where two zones overlap (AR/A...).",
    "risk": "Geomermaids reading of flood_zone and zone_subtype, not FEMA's: 1 percent "
            "flood zone, 0.2 percent flood zone (shaded X, B), minimal, undetermined "
            "(D), water, unmapped (AREA NOT INCLUDED).",
    "floodplain": "Geomermaids: the same as sfha, under a plain name.",
    "subzone": "Geomermaids: the zone of a dual or AR zone, NULL otherwise.",
    "geometry": "Piece outline as native Parquet GEOMETRY (WKB), NAD83 lon/lat "
                "(EPSG:4269) as FEMA delivers it, Polygon, at most 100 vertices.",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the "
            "GeoParquet covering. Filter on it to prune row groups before touching "
            "geometry.",
}


# ---------- measured inputs ----------

def read_json(out_dir: Path | None, rel: str) -> dict:
    if out_dir is not None:
        return json.loads((out_dir / rel).read_text())
    with urllib.request.urlopen(f"{PUBLIC_DATA}/{rel}") as r:
        return json.load(r)


def read_inputs(out_dir: Path | None) -> tuple[dict, dict]:
    """(national manifest, one state's manifest: its first file gives the schema)."""
    manifest = read_json(out_dir, f"{LATEST}/_manifest.json")
    first = next(iter(manifest["states"]))
    return manifest, read_json(out_dir, f"{LATEST}/{PARTITION_KEY}={first}/_manifest.json")


def index_facts(out_dir: Path | None) -> tuple[int, str]:
    """(size, multihash) of counties.parquet, read whole: it is a few hundred KB."""
    if out_dir is not None:
        path = out_dir / LATEST / INDEX
        return path.stat().st_size, multihash(path)
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / INDEX
        urllib.request.urlretrieve(f"{PUBLIC_DATA}/{LATEST}/{INDEX}", path)
        return path.stat().st_size, multihash(path)


def footer(con, source: str) -> tuple[list[tuple[str, str]], dict]:
    """(column name/type pairs, geo metadata) from one file's footer."""
    columns = [(r[0], "GEOMETRY" if r[1].startswith("GEOMETRY") else r[1]) for r in
               con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()]
    row = con.execute(
        f"SELECT value FROM parquet_kv_metadata('{source}') WHERE key = 'geo'").fetchone()
    if row is None:
        sys.exit(f"{source}: no geo footer")
    geo = json.loads(bytes(row[0]).decode())
    if "covering" not in geo["columns"]["geometry"]:
        sys.exit(f"{source}: geo footer declares no bbox covering")
    return columns, geo


def connect(remote: bool):
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    if remote:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    return con


# ---------- STAC ----------

GLOB = f"s3://{BUCKET}/{DATASET_PREFIX}/{LATEST}/{PARTITION_KEY}=*/*.parquet"


def build_collection(manifest: dict, columns: list[tuple[str, str]], geo: dict,
                     index: tuple[int, str], updated: str) -> dict:
    x0, y0, x1, y1 = geo["columns"]["geometry"]["bbox"]
    first, _ = manifest["fema_update_dates"]
    day = manifest["updated"]
    undocumented = [c for c, _ in columns if c not in COLUMN_DOCS]
    if undocumented:
        sys.exit(f"no column doc for {undocumented}")
    index_path = f"{DATASET_PREFIX}/{LATEST}/{INDEX}"
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT,
                            VERSION_EXT, ALTERNATE_EXT],
        "id": COLLECTION_ID,
        "title": TITLE,
        "description": (
            f"{ABOUT} {manifest['zones']:,} zones in {manifest['total_features']:,} pieces "
            f"from {manifest['county_deliveries']:,} county deliveries, checked against "
            f"FEMA's list every day (last: {day}). One GeoParquet 2.0 file per delivery, "
            f"{PARTITION_KEY}=<XX>/<DFIRM_ID>.parquet, so a delivery FEMA republishes "
            f"replaces one file; {INDEX} (the index asset) gives each file's bbox, date, "
            f"rows and checksum, and the partition glob {GLOB} reads them all. Every file "
            f"carries a bbox covering and reads in place over HTTPS or through the "
            f"anonymous S3 endpoint {S3_ENDPOINT}. Public domain, FEMA. {NOT_OFFICIAL}"
        ),
        "keywords": ["NFHL", "FEMA", "flood", "flood zones", "FIRM", "flood hazard",
                     "GeoParquet", "United States"],
        "license": LICENSE,
        "version": day,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {
            "spatial": {"bbox": [[max(-180.0, round(x0, 6)), max(-90.0, round(y0, 6)),
                                  min(180.0, round(x1, 6)), min(90.0, round(y1, 6))]]},
            "temporal": {"interval": [[f"{first}T00:00:00Z", f"{day}T23:59:59Z"]]},
        },
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [
            {"name": PARTITION_KEY, "type": "string",
             "description": "US postal code of the state or territory: the 50 states "
                            "and PR where FEMA has county-wide deliveries. The same "
                            "value as the state column in the files. Inside a state, "
                            "one file per delivery, named by its dfirm_id."},
        ],
        "partition:file_count": manifest["county_deliveries"],
        "partition:glob": GLOB,
        "table:row_count": manifest["total_features"],
        "table:primary_geometry": "geometry",
        "table:columns": [
            {"name": n, "type": t, "description": COLUMN_DOCS[n]} for n, t in columns
        ],
        "assets": {
            "index": {
                "href": f"{PUBLIC_BASE}/{index_path}",
                "type": PARQUET_TYPE,
                "title": "Index of the delivery files",
                "description": (
                    f"{manifest['county_deliveries']:,} rows, one per file: state, county, "
                    f"dfirm_id, fema_update_date, zip_name, path (relative to {LATEST}/), "
                    f"features, zones, bytes, sha256, the file's bbox as geometry and "
                    f"bbox columns. Filter it, then read the files it names."),
                "roles": ["metadata"],
                "file:size": index[0],
                "file:checksum": index[1],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{index_path}",
                                     "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            "thumbnail": {
                "href": "./thumbnail.png",
                "type": "image/png",
                "title": "1 percent annual chance flood zones, contiguous United States",
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
            {"rel": "related", "href": f"{PUBLIC_DATA}/{LATEST}/ATTRIBUTION.txt",
             "type": "text/plain", "title": "Attribution, what was changed, and the "
                                           "not-for-official-use notice"},
            {"rel": "related", "href": f"{PUBLIC_DATA}/{LATEST}/changes.json",
             "type": "application/json", "title": "Deliveries replaced, added or skipped, by day"},
            {"rel": "alternate", "href": f"{PUBLIC_DATA}/{LATEST}/", "type": "text/html",
             "title": "Browse the files"},
        ],
    }


def build_root(collection: dict, day: str, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID,
        "title": "FEMA National Flood Hazard Layer as GeoParquet",
        "description": (
            f"The flood zones of FEMA's National Flood Hazard Layer for the United "
            f"States, assembled from the county-by-county shapefile deliveries into "
            f"GeoParquet 2.0 with a bbox covering, one file per delivery, checked against "
            f"FEMA's list every day (last: {day}): a delivery FEMA republishes replaces "
            f"its file. Public domain, FEMA. {NOT_OFFICIAL}"
        ),
        "version": day,
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            {"rel": "child", "href": f"./{collection['id']}/collection.json",
             "type": "application/json", "title": collection["title"]},
            md_link("describedby", "./README.md", "Catalog README"),
            md_link("agents", "./AGENTS.md", "Catalog agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "related", "href": f"{PUBLIC_DATA}/{LATEST}/ATTRIBUTION.txt",
             "type": "text/plain", "title": "Attribution and what was changed"},
            {"rel": "related", "href": PIPELINE_URL, "type": "text/html",
             "title": "The pipeline that assembles the county deliveries"},
            {"rel": "vcs", "href": REPO_URL, "type": "text/html",
             "title": "Packaging and catalog source"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
            {"rel": "alternate", "href": SITE_URL, "type": "text/html",
             "title": "Project site"},
        ],
    }


# ---------- markdown ----------

def access_section() -> str:
    return f"""\
Every file reads in place with HTTP range requests. A point: find the
delivery in the index, then read its file.

```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
SELECT path FROM read_parquet('{PUBLIC_DATA}/{LATEST}/{INDEX}')
WHERE bbox.xmin <= -90.07 AND bbox.xmax >= -90.07
  AND bbox.ymin <= 29.95 AND bbox.ymax >= 29.95;
-- state=LA/22071C.parquet
SELECT flood_zone, zone_subtype, risk
FROM read_parquet('{PUBLIC_DATA}/{LATEST}/{PARTITION_KEY}=LA/22071C.parquet')
WHERE bbox.xmin <= -90.07 AND bbox.xmax >= -90.07
  AND bbox.ymin <= 29.95 AND bbox.ymax >= 29.95
  AND ST_Contains(geometry, ST_Point(-90.07, 29.95));
```

A file is sorted along a Hilbert curve in row groups of {ROW_GROUP_ROWS:,} pieces,
so the second query reads the footer and one row group. A state or the
country at once, through the anonymous S3 endpoint `{S3_ENDPOINT}`
(path-style, empty credentials), where a glob lists the files:

```sql
SET s3_endpoint='{S3_ENDPOINT}'; SET s3_url_style='path';
SET s3_access_key_id=''; SET s3_secret_access_key='';
SELECT state, risk, count(*) FILTER (WHERE piece_id = 0) AS zones
FROM read_parquet('s3://{BUCKET}/{DATASET_PREFIX}/{LATEST}/{PARTITION_KEY}=*/*.parquet')
GROUP BY ALL ORDER BY ALL;
```

Over HTTPS, where there is no listing, take the paths from the index."""


def pieces_md(manifest: dict) -> str:
    return f"""\
FEMA zones reach hundreds of thousands of vertices. Each was cut into pieces
of at most 100 vertices (DuckDB `ST_Subdivide`), so {manifest['zones']:,} zones make
{manifest['total_features']:,} rows. Count zones with `piece_id = 0`. Dissolve on
(`dfirm_id`, `source_feature_id`) to get a zone's outline back, with one
caveat: FEMA does not keep FLD_AR_ID unique inside every delivery, and
{manifest['zones_sharing_a_source_feature_id']:,} zones share theirs with another zone,
so a dissolve on that key merges them."""


def provenance(manifest: dict) -> str:
    first, last = manifest["fema_update_dates"]
    return f"""\
FEMA publishes the NFHL one county at a time, as zipped shapefiles, on the
[Flood Map Service Center]({FEMA_NFHL_URL}). The
[pipeline]({PIPELINE_URL}) reads S_FLD_HAZ_AR from each
county-wide delivery's ZIP in memory, maps FEMA's fields to the columns
documented here, makes invalid geometries valid, and cuts the zones into
pieces. Every day the [packaging script]({REPO_URL}/blob/main/scripts/datasets/nfhl.py)
reads the portal's list of deliveries, runs the pipeline on those FEMA
republished since the last run, sorts each one along a Hilbert curve with a
bbox covering, checks rows and geometry hashes against the pipeline's
output, and replaces its file and its row in the index; `changes.json` lists
what each day replaced. Deliveries date from {first} to {last}; each row
carries its own in `fema_update_date`. Counties without a digital FIRM are
absent, and so are community-level deliveries; `skipped.json` names the
listed deliveries that hold no flood hazard layer. The index records each
file's size and sha256."""


LICENSE_MD = f"""\
A work of the US federal government, not subject to copyright in the United
States (17 U.S.C. 105): no restriction on use or redistribution. Credit
"FEMA National Flood Hazard Layer".

{NOT_OFFICIAL}"""


def collection_readme(col: dict, manifest: dict) -> str:
    schema = "\n".join(
        f"| `{c['name']}` | `{c['type']}` | {c['description']} |" for c in col["table:columns"])
    return f"""\
# {col['title']}

{ABOUT}

| | |
|---|---|
| Rows | {manifest['total_features']:,} pieces of {manifest['zones']:,} zones |
| Files | {manifest['county_deliveries']:,} county-wide deliveries in {len(manifest['states'])} states and territories, {manifest['bytes'] / 1e9:,.1f} GB, `{PARTITION_KEY}=<XX>/<DFIRM_ID>.parquet` |
| Index | `{LATEST}/{INDEX}`: one row per file, with its bbox, date, rows and sha256 |
| Extent | {shared.fmt_bbox(col['extent']['spatial']['bbox'][0])} (lon/lat, NAD83) |
| Updates | daily, last {manifest['updated']}; deliveries from {manifest['fema_update_dates'][0]} to {manifest['fema_update_dates'][1]} |
| License | Public domain (US federal work), FEMA |

## Access

{access_section()}

## Pieces, not zones

{pieces_md(manifest)}

## Schema

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

{provenance(manifest)}

## License

{LICENSE_MD}
"""


def collection_agents(col: dict, manifest: dict) -> str:
    return f"""\
# {col['title']}: agent guide

Each row is one piece (at most 100 vertices) of a FEMA flood zone, Polygon,
NAD83 lon/lat (EPSG:4269). One file per FEMA county-wide delivery, updated
daily (last: {manifest['updated']}).

## Access

{access_section()}

## Query tips

- Start from `{INDEX}`: filter it on `bbox` or `state`, then read only the
  files it names. Over HTTPS there is no listing; through the S3 endpoint
  the partition glob reads them all.
- Inside a file, filter on `bbox` before any geometry function: it is the
  covering, and the file is sorted along a Hilbert curve, so a bbox filter
  reads only the row groups it touches.
- A file can be replaced between two reads when FEMA republishes its
  delivery: compare `sha256` in the index when that matters.
- A point can fall in two pieces only on a shared edge; zones themselves can
  overlap where FEMA mapped them so (dual zones).
- Count zones with `count(*) FILTER (WHERE piece_id = 0)`, not `count(*)`.
- `risk`, `floodplain` and `subzone` are Geomermaids readings; `flood_zone`,
  `zone_subtype`, `sfha` and `static_bfe` are FEMA's.
- An area with no row is unmapped here, not free of flood hazard.
- Never present results as an official flood zone determination.
"""


def root_readme(root: dict, col: dict, manifest: dict) -> str:
    return f"""\
# {root['title']}

{root['description']}

| Collection | Files | Rows | Size | Index |
|---|---|---|---|---|
| [{col['title']}](./{col['id']}/README.md) | {manifest['county_deliveries']:,} | {manifest['total_features']:,} | {manifest['bytes'] / 1e9:,.1f} GB | `{LATEST}/{INDEX}` |

## Versions

`{PUBLIC_DATA}/{LATEST}/` is checked against FEMA's list every day and a
delivery FEMA republished replaces its file (`changes.json` lists them by
day). It is the only version: no frozen copies are kept. This catalog
describes `{LATEST}/`.

## Access

`{PUBLIC_DATA}/{LATEST}/{PARTITION_KEY}=<XX>/<DFIRM_ID>.parquet`, one file per
delivery, read in place with HTTP range requests; `{LATEST}/{INDEX}` lists
them with their bbox. The same paths exist under
`s3://{BUCKET}/{DATASET_PREFIX}/{LATEST}/` on the anonymous S3 endpoint
`{S3_ENDPOINT}`, where a glob reads every file. The collection's README
shows the queries.

## Pieces, not zones

{pieces_md(manifest)}

## Provenance

{provenance(manifest)}

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(col: dict, manifest: dict) -> str:
    return f"""\
# {CATALOG_ID}: agent guide

FEMA's National Flood Hazard Layer flood zones as GeoParquet 2.0, United
States: one file per FEMA county-wide delivery under
`{LATEST}/{PARTITION_KEY}=<XX>/<DFIRM_ID>.parquet`, replaced when FEMA
republishes it (checked daily, last {manifest['updated']}).

## Collections

- `{col['id']}`: {col['title']}. {col['id']}/AGENTS.md

## Access

- The index: `{PUBLIC_DATA}/{LATEST}/{INDEX}`, one row per file with its bbox,
  date, rows and sha256. Filter it, then read the files it names, over HTTPS,
  no credentials.
- Every file at once: the collection's `partition:glob` through the S3
  endpoint `{S3_ENDPOINT}`, path-style, empty credentials.
- The collection documents its columns in `table:columns`.

## Conventions

- Geometry is native Parquet GEOMETRY, NAD83 lon/lat (EPSG:4269), Polygon,
  at most 100 vertices: rows are pieces of zones, `piece_id = 0` counts zones.
- `bbox` is a per-row box declared as the GeoParquet `covering`; filter on
  it before touching geometry.
- Public domain (FEMA). Not for official flood zone determinations.
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> dict:
    """Render the catalog tree into dest (replaced). Returns the national manifest."""
    manifest, state = read_inputs(out_dir)
    if not THUMB.is_file():
        sys.exit(f"missing thumbnail {THUMB}: run the thumbnails command")
    rel = f"{LATEST}/{PARTITION_KEY}={state['state']}/{state['files'][0]['file']}"
    # A daily output dir holds only the deliveries it replaced: the schema is
    # then read from a published file.
    local = out_dir is not None and (out_dir / rel).is_file()
    source = str(out_dir / rel) if local else f"{PUBLIC_DATA}/{rel}"
    con = connect(remote=not local)
    columns, geo = footer(con, source)
    # The extent is the country's, not one delivery's: the index's own footer.
    index_source = (str(out_dir / LATEST / INDEX) if out_dir is not None
                    else f"{PUBLIC_DATA}/{LATEST}/{INDEX}")
    _, index_geo = footer(con, index_source)
    updated = updated or now_rfc3339()

    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    col = build_collection(manifest, columns, index_geo, index_facts(out_dir), updated)
    cdir = dest / col["id"]
    write_json(cdir / "collection.json", col)
    shutil.copyfile(THUMB, cdir / "thumbnail.png")
    (cdir / "README.md").write_text(collection_readme(col, manifest))
    (cdir / "AGENTS.md").write_text(collection_agents(col, manifest))

    root = build_root(col, manifest["updated"], updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, col, manifest))
    (dest / "AGENTS.md").write_text(root_agents(col, manifest))
    return manifest


def check(tree: Path, *, remote_data: bool) -> int:
    scope = ("--data-scope", "all") if remote_data else shared.LOCAL_DATA
    return shared.check(tree, *scope)


def publish(out_dir: Path | None, remote: str, *, remote_data: bool, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="nfhl-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        manifest = build(out_dir, tree)
        print(f"  rendered 1 collection, {manifest['total_features']:,} rows")
        if check(tree, remote_data=remote_data):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnail ----------

CONUS = (-125.0, 24.0, -66.5, 49.5)
COLOR = "#2f6db5"


def thumbnails(out_dir: Path) -> None:
    """The 1 percent zones of the contiguous states: pieces per pixel, counted
    at their bbox centre, read from the bbox column only."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.colors import LinearSegmentedColormap

    from thumbnails import BACKGROUND, frame

    src = out_dir / LATEST / f"{PARTITION_KEY}=*" / "*.parquet"
    con = connect(remote=False)
    x0, y0, x1, y1, aspect = frame(*CONUS)
    xy = con.execute(f"""
        SELECT (bbox.xmin + bbox.xmax) / 2, (bbox.ymin + bbox.ymax) / 2
        FROM read_parquet('{src}')
        WHERE risk = '1 percent flood zone'
          AND bbox.xmin >= {x0} AND bbox.xmax <= {x1} AND bbox.ymin >= {y0} AND bbox.ymax <= {y1}
    """).fetchnumpy()
    xs, ys = list(xy.values())
    fig = plt.figure(figsize=(6, 4), dpi=100)
    ax = fig.add_axes((0, 0, 1, 1))
    ax.set_facecolor(BACKGROUND)
    fig.patch.set_facecolor(BACKGROUND)
    # Pieces per screen pixel, log-scaled: a scatter of 33 M dots saturates.
    counts, _, _ = np.histogram2d(ys, xs, bins=(400, 600), range=((y0, y1), (x0, x1)))
    shade = np.log1p(counts) / np.log1p(counts.max())
    cmap = LinearSegmentedColormap.from_list("nfhl", [BACKGROUND, COLOR])
    ax.imshow(shade, origin="lower", extent=(x0, x1, y0, y1), cmap=cmap,
              interpolation="nearest", aspect=aspect)
    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.set_aspect(aspect)
    ax.axis("off")
    THUMB.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(THUMB, dpi=100, facecolor=BACKGROUND)
    plt.close(fig)
    print(f"  {len(xs):,} pieces -> {THUMB.relative_to(REPO)}")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp, required=False):
        sp.add_argument("--out-dir", type=Path, default=None, required=required,
                        help="nfhl.py output dir, the one holding latest/ "
                             "(default: read the published files)")

    b = sub.add_parser("build", help="render the catalog tree")
    out_dir_arg(b)
    b.add_argument("--dest", type=Path, required=True)

    c = sub.add_parser("check", help="rashid pass over a rendered tree")
    c.add_argument("tree", type=Path)
    c.add_argument("--remote-data", action="store_true",
                   help="also range-read the published parquet files (PTL-DAT-*)")

    u = sub.add_parser("publish", help="build, check, upload to <remote>/catalog/")
    out_dir_arg(u)
    u.add_argument("--remote", required=True, help="rclone path of the dataset prefix, "
                                                   "e.g. parquetry:parquetry/nfhl")
    u.add_argument("--remote-data", action="store_true")
    u.add_argument("--dry-run", action="store_true")

    t = sub.add_parser("thumbnails", help=f"render catalog/thumbnails/nfhl/{STEM}.png")
    out_dir_arg(t, required=True)

    args = p.parse_args()
    if args.cmd == "build":
        m = build(args.out_dir, args.dest)
        print(f"wrote {args.dest}/ ({m['total_features']:,} rows)")
    elif args.cmd == "check":
        sys.exit(check(args.tree, remote_data=args.remote_data))
    elif args.cmd == "publish":
        publish(args.out_dir, args.remote, remote_data=args.remote_data, dry_run=args.dry_run)
    elif args.cmd == "thumbnails":
        thumbnails(args.out_dir)


if __name__ == "__main__":
    main()
