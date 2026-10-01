#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the FEMA NFHL.

One STAC Collection, the flood hazard areas, published in two layouts that
the collection describes both: the United States file as the `data` asset,
and the per-state files under state=<XX>/ through the partition extension
(`partition:glob`) and one `data-<xx>` asset each. A snapshot is a dated
folder that never changes, so every asset carries a checksum. Row counts,
sizes and checksums come from the _manifest.json files nfhl.py writes;
column types and the extent are read from the Parquet footer, which is a
range request when the source is a URL. The prose lives here. Spec:
portolan-spec v0.2.0. Validator: rashid, pinned in pyproject.toml, shared
with the other catalogs (scripts/catalog.py), as are the STAC constants,
the rashid wrapper and the uploader.

The catalog is published beside the data, at /nfhl/catalog/catalog.json,
and describes the snapshot snapshots.json names as latest.

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
from datasets.nfhl import ROW_GROUP_ROWS, STEM

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
                "county-wide (22071C).",
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


def read_inputs(out_dir: Path | None) -> tuple[str, dict, dict[str, dict]]:
    """(snapshot, national manifest, state manifests by code)."""
    snapshot = read_json(out_dir, "snapshots.json")["latest"].strip("/")
    manifest = read_json(out_dir, f"{snapshot}/_manifest.json")
    states = {code: read_json(out_dir, f"{snapshot}/{PARTITION_KEY}={code}/_manifest.json")
              for code in manifest["states"]}
    return snapshot, manifest, states


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

def state_assets(snapshot: str, states: dict[str, dict]) -> dict:
    """One asset per state file, keyed data-<xx>, checksummed."""
    assets = {}
    for code, m in states.items():
        e = m["files"][0]
        path = f"{DATASET_PREFIX}/{snapshot}/{PARTITION_KEY}={code}/{STEM}.parquet"
        assets[f"data-{code.lower()}"] = {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": f"{m['state_name']} ({code})",
            "description": f"{e['features']:,} pieces, {len(m['counties'])} counties",
            "roles": ["data"],
            "file:size": e["bytes"],
            "file:checksum": "1220" + e["sha256"],
            "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                 "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
        }
    return assets


def build_collection(snapshot: str, manifest: dict, states: dict[str, dict],
                     columns: list[tuple[str, str]], geo: dict, updated: str) -> dict:
    entry = manifest["files"][0]
    glob = f"s3://{BUCKET}/{DATASET_PREFIX}/{snapshot}/{PARTITION_KEY}=*/{STEM}.parquet"
    x0, y0, x1, y1 = geo["columns"]["geometry"]["bbox"]
    bbox = [max(-180.0, round(x0, 6)), max(-90.0, round(y0, 6)),
            min(180.0, round(x1, 6)), min(90.0, round(y1, 6))]
    first, _ = manifest["fema_update_dates"]
    path = f"{DATASET_PREFIX}/{snapshot}/{entry['file']}"
    undocumented = [c for c, _ in columns if c not in COLUMN_DOCS]
    if undocumented:
        sys.exit(f"no column doc for {undocumented}")
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT,
                            VERSION_EXT, ALTERNATE_EXT],
        "id": COLLECTION_ID,
        "title": TITLE,
        "description": (
            f"{ABOUT} Snapshot {snapshot}: {manifest['zones']:,} zones in "
            f"{manifest['total_features']:,} pieces from {manifest['county_deliveries']:,} "
            f"county deliveries. Published twice from the same rows: one United States "
            f"GeoParquet 2.0 file ({entry['bytes'] / 1e9:,.1f} GB, the data asset), and "
            f"one file per state under {PARTITION_KEY}=<XX>/ ({len(states)} files, the "
            f"data-<xx> assets and the partition glob {glob}). Both carry a bbox covering "
            f"and read in place over HTTPS or through the anonymous S3 endpoint "
            f"{S3_ENDPOINT}. Public domain, FEMA. {NOT_OFFICIAL}"
        ),
        "keywords": ["NFHL", "FEMA", "flood", "flood zones", "FIRM", "flood hazard",
                     "GeoParquet", "United States"],
        "license": LICENSE,
        "version": snapshot,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {
            "spatial": {"bbox": [bbox]},
            "temporal": {"interval": [[f"{first}T00:00:00Z", f"{snapshot}T23:59:59Z"]]},
        },
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [
            {"name": PARTITION_KEY, "type": "string",
             "description": "US postal code of the state or territory: the 50 states "
                            "and PR where FEMA has county-wide deliveries. The same "
                            "value as the state column in the files."},
        ],
        "partition:file_count": len(states),
        "partition:glob": glob,
        "table:row_count": manifest["total_features"],
        "table:primary_geometry": "geometry",
        "table:columns": [
            {"name": n, "type": t, "description": COLUMN_DOCS[n]} for n, t in columns
        ],
        "assets": {
            "data": {
                "href": f"{PUBLIC_BASE}/{path}",
                "type": PARQUET_TYPE,
                "title": f"{TITLE}, United States in one file",
                "description": f"{entry['features']:,} pieces in {entry['row_groups']:,} "
                               f"row groups, the largest "
                               f"{entry['largest_row_group_bytes'] / 1e6:,.0f} MB.",
                "roles": ["data"],
                "file:size": entry["bytes"],
                "file:checksum": "1220" + entry["sha256"],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                     "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            **state_assets(snapshot, states),
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
            {"rel": "related", "href": f"{PUBLIC_DATA}/{snapshot}/ATTRIBUTION.txt",
             "type": "text/plain", "title": "Attribution, what was changed, and the "
                                           "not-for-official-use notice"},
            {"rel": "alternate", "href": f"{PUBLIC_DATA}/{snapshot}/", "type": "text/html",
             "title": f"Browse the {snapshot} files"},
        ],
    }


def build_root(collection: dict, snapshot: str, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID,
        "title": "FEMA National Flood Hazard Layer as GeoParquet",
        "description": (
            f"The flood zones of FEMA's National Flood Hazard Layer for the United "
            f"States, assembled from the county-by-county shapefile deliveries into "
            f"GeoParquet 2.0 with a bbox covering: one file for the country and one "
            f"per state. Snapshot {snapshot}; a snapshot is a dated folder whose files "
            f"never change. Public domain, FEMA. {NOT_OFFICIAL}"
        ),
        "version": snapshot,
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
            {"rel": "related", "href": f"{PUBLIC_DATA}/{snapshot}/ATTRIBUTION.txt",
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

def access_section(snapshot: str, entry: dict) -> str:
    return f"""\
Two layouts of the same rows, both immutable and readable in place with
HTTP range requests. One state, a smaller file:

```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
SELECT flood_zone, zone_subtype, risk
FROM read_parquet('{PUBLIC_DATA}/{snapshot}/{PARTITION_KEY}=LA/{STEM}.parquet')
WHERE bbox.xmin <= -90.07 AND bbox.xmax >= -90.07
  AND bbox.ymin <= 29.95 AND bbox.ymax >= 29.95
  AND ST_Contains(geometry, ST_Point(-90.07, 29.95));
```

The United States in one file, `{PUBLIC_DATA}/{snapshot}/{entry['file']}`,
{entry['bytes'] / 1e9:,.1f} GB in {entry['row_groups']:,} row groups of {ROW_GROUP_ROWS:,} pieces:
the same query on it reads the footer and one row group. Every state at once,
through the anonymous S3 endpoint `{S3_ENDPOINT}` (path-style, empty credentials):

```sql
SET s3_endpoint='{S3_ENDPOINT}'; SET s3_url_style='path';
SET s3_access_key_id=''; SET s3_secret_access_key='';
SELECT state, risk, count(*) FILTER (WHERE piece_id = 0) AS zones
FROM read_parquet('s3://{BUCKET}/{DATASET_PREFIX}/{snapshot}/{PARTITION_KEY}=*/{STEM}.parquet')
GROUP BY ALL ORDER BY ALL;
```"""


def pieces_md(manifest: dict) -> str:
    return f"""\
FEMA zones reach hundreds of thousands of vertices. Each was cut into pieces
of at most 100 vertices (DuckDB `ST_Subdivide`), so {manifest['zones']:,} zones make
{manifest['total_features']:,} rows. Count zones with `piece_id = 0`. Dissolve on
(`state`, `county`, `source_feature_id`) to get a zone's outline back, with one
caveat: FEMA does not keep FLD_AR_ID unique inside every county, and
{manifest['zones_sharing_a_source_feature_id']:,} zones share theirs with another zone,
so a dissolve on that key merges them."""


def provenance(manifest: dict) -> str:
    first, last = manifest["fema_update_dates"]
    return f"""\
FEMA publishes the NFHL one county at a time, as zipped shapefiles, on the
[Flood Map Service Center]({FEMA_NFHL_URL}). The
[pipeline]({PIPELINE_URL}) reads the portal's list of
county-wide deliveries on the snapshot day, reads S_FLD_HAZ_AR from each ZIP
in memory, maps FEMA's fields to the columns documented here, makes invalid
geometries valid, and cuts the zones into pieces. Deliveries date from {first}
to {last}; each row carries its own in `fema_update_date`. Counties without a
digital FIRM are absent, and so are community-level deliveries. The
[packaging script]({REPO_URL}/blob/main/scripts/datasets/nfhl.py) sorts the
pieces by state then along a Hilbert curve, writes the bbox covering and the
state files, and checks rows and geometry hashes against its input. The
manifests record each file's size and sha256, which this catalog publishes as
`file:size` and `file:checksum`."""


LICENSE_MD = f"""\
A work of the US federal government, not subject to copyright in the United
States (17 U.S.C. 105): no restriction on use or redistribution. Credit
"FEMA National Flood Hazard Layer".

{NOT_OFFICIAL}"""


def collection_readme(col: dict, snapshot: str, manifest: dict, states: dict) -> str:
    entry = manifest["files"][0]
    schema = "\n".join(
        f"| `{c['name']}` | `{c['type']}` | {c['description']} |" for c in col["table:columns"])
    state_bytes = sum(m["files"][0]["bytes"] for m in states.values())
    return f"""\
# {col['title']}

{ABOUT}

| | |
|---|---|
| Rows | {manifest['total_features']:,} pieces of {manifest['zones']:,} zones |
| Counties | {manifest['county_deliveries']:,} county-wide deliveries in {len(states)} states and territories |
| United States file | `{entry['file']}`, {entry['bytes'] / 1e9:,.1f} GB, {entry['row_groups']:,} row groups, sha256 `{entry['sha256'][:12]}...` |
| Per-state files | {len(states)} under `{PARTITION_KEY}=<XX>/`, {state_bytes / 1e9:,.1f} GB in all |
| Extent | {shared.fmt_bbox(col['extent']['spatial']['bbox'][0])} (lon/lat, NAD83) |
| Snapshot | {snapshot}, deliveries from {manifest['fema_update_dates'][0]} to {manifest['fema_update_dates'][1]} |
| License | Public domain (US federal work), FEMA |

## Access

{access_section(snapshot, entry)}

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


def collection_agents(col: dict, snapshot: str, manifest: dict) -> str:
    entry = manifest["files"][0]
    return f"""\
# {col['title']}: agent guide

Each row is one piece (at most 100 vertices) of a FEMA flood zone, Polygon,
NAD83 lon/lat (EPSG:4269). Snapshot {snapshot}.

## Access

{access_section(snapshot, entry)}

## Query tips

- Filter on `bbox` before any geometry function: it is the covering, and the
  files are sorted by state then along a Hilbert curve, so a bbox filter
  reads only the row groups it touches.
- One state or a small window: the per-state file. The whole country: the
  United States file ({entry['bytes'] / 1e9:,.1f} GB), never downloaded, always
  read in place.
- A point can fall in two pieces only on a shared edge; zones themselves can
  overlap where FEMA mapped them so (dual zones).
- Count zones with `count(*) FILTER (WHERE piece_id = 0)`, not `count(*)`.
- `risk`, `floodplain` and `subzone` are Geomermaids readings; `flood_zone`,
  `zone_subtype`, `sfha` and `static_bfe` are FEMA's.
- An area with no row is unmapped here, not free of flood hazard.
- Never present results as an official flood zone determination.
"""


def root_readme(root: dict, col: dict, snapshot: str, manifest: dict, states: dict) -> str:
    entry = manifest["files"][0]
    return f"""\
# {root['title']}

{root['description']}

| Collection | United States file | Rows | Size | Per-state files |
|---|---|---|---|---|
| [{col['title']}](./{col['id']}/README.md) | `{entry['file']}` | {entry['features']:,} | {entry['bytes'] / 1e9:,.1f} GB | {len(states)} |

## Versions

A snapshot is the NFHL as FEMA listed it on one day, under
`{PUBLIC_DATA}/<YYYY-MM-DD>/`, and never changes afterwards.
`{PUBLIC_DATA}/snapshots.json` names the latest; this catalog describes it
({snapshot}).

## Access

`{PUBLIC_DATA}/{snapshot}/{STEM}.parquet` for the country, and
`{PUBLIC_DATA}/{snapshot}/{PARTITION_KEY}=<XX>/{STEM}.parquet` per state. Both
read in place with HTTP range requests, and the same paths exist under
`s3://{BUCKET}/{DATASET_PREFIX}/{snapshot}/` on the anonymous S3 endpoint
`{S3_ENDPOINT}`, where a glob over `{PARTITION_KEY}=*/` reads every state. The
collection's README shows the queries.

## Pieces, not zones

{pieces_md(manifest)}

## Provenance

{provenance(manifest)}

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(col: dict, snapshot: str) -> str:
    return f"""\
# {CATALOG_ID}: agent guide

FEMA's National Flood Hazard Layer flood zones as GeoParquet 2.0, United
States, snapshot {snapshot}: one file for the country, one per state under
`{PARTITION_KEY}=<XX>/`. Immutable files.

## Collections

- `{col['id']}`: {col['title']}. {col['id']}/AGENTS.md

## Access

- One state: `{PUBLIC_DATA}/{snapshot}/{PARTITION_KEY}=<XX>/{STEM}.parquet` over
  HTTPS, no credentials.
- The country: `{PUBLIC_DATA}/{snapshot}/{STEM}.parquet`, read in place. Every
  state at once: the collection's `partition:glob` through the S3 endpoint
  `{S3_ENDPOINT}`, path-style, empty credentials.
- The collection documents its columns in `table:columns` and carries
  `file:size` and `file:checksum` on every data asset.

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
    snapshot, manifest, states = read_inputs(out_dir)
    if not THUMB.is_file():
        sys.exit(f"missing thumbnail {THUMB}: run the thumbnails command")
    entry = manifest["files"][0]
    source = (str(out_dir / snapshot / entry["file"]) if out_dir is not None
              else f"{PUBLIC_DATA}/{snapshot}/{entry['file']}")
    con = connect(remote=out_dir is None)
    columns, geo = footer(con, source)
    updated = updated or now_rfc3339()

    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    col = build_collection(snapshot, manifest, states, columns, geo, updated)
    cdir = dest / col["id"]
    write_json(cdir / "collection.json", col)
    shutil.copyfile(THUMB, cdir / "thumbnail.png")
    (cdir / "README.md").write_text(collection_readme(col, snapshot, manifest, states))
    (cdir / "AGENTS.md").write_text(collection_agents(col, snapshot, manifest))

    root = build_root(col, snapshot, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, col, snapshot, manifest, states))
    (dest / "AGENTS.md").write_text(root_agents(col, snapshot))
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

    snapshot, manifest, _ = read_inputs(out_dir)
    src = out_dir / snapshot / manifest["files"][0]["file"]
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
                        help="nfhl.py output dir, the one holding snapshots.json "
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
