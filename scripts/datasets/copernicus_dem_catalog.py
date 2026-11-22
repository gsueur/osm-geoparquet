#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog of the Copernicus DEM
GLO-30 tile index (copernicus_dem_index.py): the index is the dataset, the
COGs stay in the AWS Open Data bucket copernicus-dem-30m.

  copernicus-dem/catalog/catalog.json                     root
  copernicus-dem/catalog/copernicus-glo30/collection.json the index: GeoParquet
                                                          (`data`), PMTiles
                                                          (`pmtiles`) and a style

No item per tile: the GeoParquet index is the listing (one row per COG, with
its URL, grid and elevation range), read with DuckDB or GDAL's GTI driver.
Sizes, checksums and dates come from the published index.json; `updated` is
the newest tile's Last-Modified in the bucket, `checked` the index's listing
time. The thumbnail (the tiles coloured by highest elevation) is committed
under catalog/thumbnails/copernicus-dem/. Spec: portolan-spec v0.3.0.
Validator: rashid, through scripts/catalog.py.

Usage:
  uv run --project scripts --with matplotlib \\
      python3 scripts/datasets/copernicus_dem_catalog.py thumbnails
  python3 scripts/datasets/copernicus_dem_catalog.py build --dest build/copernicus-dem-catalog
  python3 scripts/datasets/copernicus_dem_catalog.py check build/copernicus-dem-catalog
  python3 scripts/datasets/copernicus_dem_catalog.py publish --remote parquetry:parquetry/copernicus-dem
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
from datasets.copernicus_dem_index import ATTRIBUTION, BUCKET as SOURCE_BUCKET, INDEX
from datasets.copernicus_dem_index import PROPERTIES, PUBLIC as INDEX_URL, SOURCE
from datasets.gaul_catalog import STYLE_TYPE, WEB_MAP_LINKS_EXT
from datasets.pmtiles import PMTILES_TYPE

import catalog as shared  # scripts/catalog.py
from catalog import (
    ALTERNATE_EXT, BUCKET, CONTACT_EMAIL, FILE_EXT, LOGO, PARQUET_TYPE, PORTOLAN_SCHEMA,
    PUBLIC_BASE, REPO, REPO_URL, S3_ENDPOINT, SITE_URL, TABLE_EXT, md_link, multihash,
    write_json,
)

DATASET_PREFIX = "copernicus-dem"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_BASE}/{DATASET_PREFIX}/{CATALOG_PREFIX}"
CATALOG_ID = "copernicus-dem"
COLLECTION = "copernicus-glo30"
TITLE = "Copernicus DEM GLO-30: tile index (linked)"
THUMB = REPO / "catalog" / "thumbnails" / DATASET_PREFIX / f"{COLLECTION}.png"
LICENCE_PDF = f"{SOURCE}/Copernicus_DSM_COG_10_N00_00_E006_00_DEM/INFO/eula_F.pdf"
DISCLAIMER = ("The organisations in charge of the Copernicus programme by law or by delegation "
              "do not incur any liability for any use of the Copernicus WorldDEM-30.")
MODIFIED = ("produced using Copernicus WorldDEM-30 © DLR e.V. 2010-2014 and © Airbus Defence "
            "and Space GmbH 2014-2018 provided under COPERNICUS by the European Union and ESA; "
            "all rights reserved")
# TanDEM-X acquisitions behind the DEM (Copernicus DEM product handbook).
ACQUIRED = ["2010-12-01T00:00:00Z", "2015-01-31T23:59:59Z"]

LICENSE_LINK = {"rel": "license", "href": LICENCE_PDF, "type": "application/pdf",
                "title": "Copernicus WorldDEM-30 licence: free use and redistribution, with "
                         "attribution"}
VIA_LINK = {"rel": "via", "href": "https://registry.opendata.aws/copernicus-dem/",
            "type": "text/html", "title": "Copernicus DEM on the Registry of Open Data on AWS"}
PROVIDERS = [
    {"name": "European Space Agency (ESA), Copernicus programme", "roles": ["licensor"],
     "description": "Provides the Copernicus DEM under the Copernicus WorldDEM-30 licence.",
     "url": "https://dataspace.copernicus.eu/explore-data/data-collections/copernicus-contributing-missions/collections-description/COP-DEM"},
    {"name": "Airbus Defence and Space and DLR", "roles": ["producer"],
     "description": "Produced the WorldDEM from the TanDEM-X mission's radar acquisitions, "
                    "2010 to 2015.",
     "url": "https://www.dlr.de/en/research-and-transfer/projects-and-missions/tandem-x"},
    {"name": "Geomermaids", "roles": ["processor"],
     "description": "Builds and hosts the tile index; hosts none of the elevation data.",
     "url": SITE_URL, "email": CONTACT_EMAIL},
    {"name": "Amazon Web Services, Open Data", "roles": ["host"],
     "description": f"Serves the COGs in the public bucket {SOURCE_BUCKET} (eu-central-1).",
     "url": "https://registry.opendata.aws/copernicus-dem/"},
]
COLUMN_DOCS = {
    "tile_id": "The COG's file name without .tif (Copernicus_DSM_COG_10_<lat>_00_<lon>_00_DEM, "
               "the cell's south-west corner).",
    "product": "Always glo30.",
    "project": "Null (a 3DEP column).",
    "acquired_start": "Null: the DEM merges TanDEM-X acquisitions of 2010 to 2015.",
    "acquired_end": "Null.",
    "published": "The COG's Last-Modified in the bucket (UTC).",
    "url": "The COG over HTTPS (no CORS on the bucket). GDAL GTI: -oo LOCATION_FIELD=url.",
    "s3_url": f"s3://{SOURCE_BUCKET}/... (anonymous, eu-central-1).",
    "thumbnail_url": "Null.",
    "file_size": "The COG's size in bytes.",
    "crs": "EPSG:4326 (heights above EGM2008).",
    "epsg": "4326.",
    "proj4": "The CRS as a PROJ string.",
    "nodata": "Null: the COGs declare no nodata.",
    "pixel_size": "East-west cell size in degrees: 1/3600 up to 50 degrees of latitude, wider "
                  "toward the poles (1/2400 to 1/360). North-south is always 1/3600.",
    "width": "Columns: 3600, 2400, 1800, 1200, 720 or 360 by latitude band.",
    "height": "Rows: 3600.",
    "min_elev": "Lowest elevation in the tile, metres above EGM2008 (the tile's ISO 19115 XML).",
    "max_elev": "Highest elevation in the tile, metres above EGM2008.",
    "native_bbox": "The grid's extent as GDAL reads it: PixelIsPoint, so half a cell west and "
                   "north of the named corner.",
    "west": "Footprint west edge, degrees.",
    "south": "Footprint south edge, degrees.",
    "east": "Footprint east edge, degrees.",
    "north": "Footprint north edge, degrees.",
    "geometry": "Footprint, lon/lat (OGC:CRS84), cut at the antimeridian and the poles.",
    "bbox": "Per-row bounding box, the GeoParquet covering: filter on it first.",
}
# Elevation ramp of the thumbnail and the style, on max_elev.
RAMP = [(0, "#3d7a4a"), (500, "#a9c27a"), (1500, "#e3cf8f"), (3000, "#b07a4f"),
        (5000, "#c9bfb5")]


def read_index() -> dict:
    with urllib.request.urlopen(f"{INDEX_URL}/index.json", timeout=60) as r:
        return json.load(r)


def measured() -> dict:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    src = f"read_parquet('{INDEX_URL}/{INDEX}.parquet')"
    x0, y0, x1, y1, newest = con.execute(
        f"SELECT min(west), min(south), max(east), max(north), "
        f"strftime(max(published) AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') FROM {src}").fetchone()
    cols = [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1])
            for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
    return {"bbox": [x0, y0, x1, y1], "updated": newest, "columns": cols}


def style() -> dict:
    ramp = ["interpolate", ["linear"], ["get", "max_elev"]]
    for v, c in RAMP:
        ramp += [v, c]
    src = f"pmtiles://{INDEX_URL}/{INDEX}.pmtiles"
    return {
        "version": 8, "name": "GLO-30 tiles by highest elevation",
        "sources": {"data": {"type": "vector", "url": src, "attribution": ATTRIBUTION}},
        "layers": [
            {"id": "tiles-fill", "type": "fill", "source": "data", "source-layer": "tiles",
             "paint": {"fill-color": ramp, "fill-opacity": 0.7}},
            {"id": "tiles-outline", "type": "line", "source": "data", "source-layer": "tiles",
             "paint": {"line-color": "#555555",
                       "line-width": ["interpolate", ["linear"], ["zoom"], 2, 0.1, 9, 1]}},
        ],
    }


def build_collection(index: dict, m: dict) -> dict:
    data, tiles = index["files"][INDEX], index["files"][f"{INDEX}-pmtiles"]
    path, tiles_path = f"{DATASET_PREFIX}/index/{data['file']}", f"{DATASET_PREFIX}/index/{tiles['file']}"
    missing = [c for c, _ in m["columns"] if c not in COLUMN_DOCS]
    if missing:
        sys.exit(f"no column doc for {missing}")
    s3 = {"s3": {"href": f"s3://{BUCKET}/{path}", "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}
    return {
        "type": "Collection", "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, ALTERNATE_EXT, WEB_MAP_LINKS_EXT],
        "id": COLLECTION,
        "title": "Copernicus DEM GLO-30 tiles (about 30 m, worldwide)",
        "description": (
            f"A tile index of the Copernicus DEM GLO-30, the world's land at 1 arc-second: "
            f"{index['tiles']:,} one-degree Cloud-Optimized GeoTIFFs "
            f"({index['bytes'] / 1e9:,.0f} GB) in the AWS Open Data bucket {SOURCE_BUCKET}, "
            f"listed weekly, none copied. One GeoParquet row per COG with its URL, grid "
            f"(EPSG:4326, heights above EGM2008), size and elevation range, and a PMTiles grid "
            f"of the footprints for map pickers. The bucket holds the 2021 release, without "
            f"Armenia and Azerbaijan; open sea has no tiles. It sends no CORS headers: browsers "
            f"read the COGs through a proxy. Credit: {ATTRIBUTION} {DISCLAIMER}"),
        "keywords": ["Copernicus DEM", "GLO-30", "DEM", "elevation", "TanDEM-X", "COG",
                     "tile index", "GeoParquet", "worldwide"],
        "license": "other",
        "attribution": ATTRIBUTION,
        "updated": m["updated"],
        "checked": index["listed"],
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [[round(v, 6) for v in m["bbox"]]]},
                   "temporal": {"interval": [ACQUIRED]}},
        "table:row_count": data["rows"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": COLUMN_DOCS[n]}
                          for n, t in m["columns"]],
        "assets": {
            "data": {"href": f"{PUBLIC_BASE}/{path}", "type": PARQUET_TYPE,
                     "title": "GLO-30 tile index", "roles": ["data"],
                     "description": f"{data['rows']:,} rows, sorted by tile_id.",
                     "file:size": data["bytes"], "file:checksum": "1220" + data["sha256"],
                     "alternate": s3},
            "pmtiles": {"href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
                        "title": "GLO-30 tile footprints, vector tiles", "roles": ["visual"],
                        "description": f"PMTiles, layer `tiles`, zoom {tiles['zooms'][0]} to "
                                       f"{tiles['zooms'][1]}; properties {', '.join(PROPERTIES)}. "
                                       f"Web Mercator: the tiles south of 85.05 S are not in it.",
                        "file:size": tiles["bytes"], "file:checksum": "1220" + tiles["sha256"],
                        "alternate": {"s3": {"href": f"s3://{BUCKET}/{tiles_path}",
                                             "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}},
            "style": {"href": "./styles/default.json", "type": STYLE_TYPE,
                      "title": "Tiles by highest elevation", "roles": ["style", "default"]},
            "thumbnail": {"href": "./thumbnail.png", "type": "image/png",
                          "title": "GLO-30 tiles by highest elevation", "roles": ["thumbnail"],
                          "file:size": THUMB.stat().st_size, "file:checksum": multihash(THUMB)},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", "GLO-30 tiles: README"),
            md_link("agents", "./AGENTS.md", "GLO-30 tiles: agent guide"),
            LICENSE_LINK, VIA_LINK,
            {"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
             "title": "GLO-30 tile footprints, vector tiles", "pmtiles:layers": ["tiles"]},
            {"rel": "related", "href": f"{INDEX_URL}/index.json", "type": "application/json",
             "title": "Listing time, counts, sizes and checksums"},
        ],
    }


def build_root(col: dict) -> dict:
    return {
        "type": "Catalog", "stac_version": "1.1.0", "stac_extensions": [PORTOLAN_SCHEMA],
        "id": CATALOG_ID, "title": TITLE,
        "description": (
            "The Copernicus DEM GLO-30 (the world's land at about 30 m) as a tile index: one "
            "GeoParquet row per Cloud-Optimized GeoTIFF of the AWS Open Data bucket "
            f"{SOURCE_BUCKET}, with a PMTiles grid of the footprints. The elevation data stays "
            f"in the bucket. Relisted weekly. Credit: {ATTRIBUTION}"),
        "updated": col["updated"], "checked": col["checked"],
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            {"rel": "child", "href": f"./{COLLECTION}/collection.json", "type": "application/json",
             "title": col["title"]},
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK, VIA_LINK,
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


# ---------- markdown ----------

EXAMPLE = (6.8652, 45.8326)   # Mont Blanc

LICENSE_MD = f"""\
Copernicus WorldDEM-30 licence ({LICENCE_PDF}): free use, copying and
redistribution, modified or not. Credit, as the licence words it:

> {ATTRIBUTION}

For data you derive (a clip, a reprojection, a hillshade): "{MODIFIED}".
Where you distribute it, add: "{DISCLAIMER}\""""


def access_md() -> str:
    x, y = EXAMPLE
    return f"""\
The tile under a point, then its elevation there:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT tile_id, url, min_elev, max_elev
FROM read_parquet('{INDEX_URL}/{INDEX}.parquet')
WHERE bbox.xmin <= {x} AND bbox.xmax >= {x} AND bbox.ymin <= {y} AND bbox.ymax >= {y};
```

```sh
gdallocationinfo -valonly -wgs84 /vsicurl/<url> {x} {y}
```

An area as one mosaic with GDAL's GTI driver (GDAL 3.10 or later):

```sh
gdalwarp -te 6.6 45.7 7.1 46.0 -oo LOCATION_FIELD=url \\
  "GTI:/vsicurl/{INDEX_URL}/{INDEX}.parquet" mont_blanc.tif
```

The COGs are also at `s3://{SOURCE_BUCKET}/` (anonymous, eu-central-1). The
bucket sends no CORS headers: a browser needs a proxy."""


def collection_readme(col: dict) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| Index | `{INDEX}.parquet`, {col['assets']['data']['file:size'] / 1e6:,.1f} MB, {col['table:row_count']:,} rows |
| Map tiles | `{INDEX}.pmtiles`, layer `tiles`, {col['assets']['pmtiles']['file:size'] / 1e6:,.1f} MB |
| Grid | EPSG:4326, 1 arc-second north-south, wider east-west toward the poles |
| Source | {SOURCE}/ (2021 release) |

## Access

{access_md()}

## Columns

The 3DEP tile index's columns (usgs-rasters), so one reader serves both; those
Copernicus has no value for are null.

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

[copernicus_dem_index.py]({REPO_URL}/blob/main/scripts/datasets/copernicus_dem_index.py)
lists the bucket each week (checked against its tileList.txt), reads each new
or replaced COG's header with one range request and its ISO 19115 XML for the
elevation range, and checks a sample of headers against gdalinfo. Nothing is
downloaded or rehosted.

## Dates

`updated` is the newest COG's Last-Modified in the bucket; `checked` the last
listing.

## License

{LICENSE_MD}
"""


def collection_agents(col: dict) -> str:
    return f"""\
# {col['title']}: agent guide

A table of tiles, not the elevations: find the COGs with SQL on the index,
then read them with GDAL (`/vsicurl/`) or rasterio, a window at a time.

## Access

{access_md()}

## Tips

- Filter on `bbox` first. A point falls in one tile; tile edges are on whole
  degrees, half a cell off (PixelIsPoint).
- `min_elev`/`max_elev` give a colour ramp before reading any COG.
- Heights are above the EGM2008 geoid, not the ellipsoid. A DSM: tree tops and
  roofs, not bare ground.
- No tile over open sea, Armenia or Azerbaijan.
- Credit: {ATTRIBUTION}
"""


def root_readme(root: dict, col: dict) -> str:
    return f"""\
# {root['title']}

{root['description']}

| Collection | Rows |
|---|---|
| [{col['title']}](./{COLLECTION}/README.md) | {col['table:row_count']:,} |

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(col: dict) -> str:
    return f"""\
# {CATALOG_ID}: agent guide

The Copernicus DEM GLO-30 tile index. One collection: `{COLLECTION}`
({COLLECTION}/AGENTS.md). Index: {INDEX_URL}/{INDEX}.parquet. COGs: {SOURCE}/.
Credit: {ATTRIBUTION}
"""


# ---------- build ----------

def build(dest: Path) -> dict:
    if not THUMB.is_file():
        sys.exit(f"missing {THUMB}: run the thumbnails command")
    if dest.exists():
        shutil.rmtree(dest)
    cdir = dest / COLLECTION
    col = build_collection(read_index(), measured())
    write_json(cdir / "styles" / "default.json", style())
    asset = col["assets"]["style"]
    asset["file:size"] = (cdir / asset["href"]).stat().st_size
    asset["file:checksum"] = multihash(cdir / asset["href"])
    write_json(cdir / "collection.json", col)
    shutil.copyfile(THUMB, cdir / "thumbnail.png")
    (cdir / "README.md").write_text(collection_readme(col))
    (cdir / "AGENTS.md").write_text(collection_agents(col))
    root = build_root(col)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, col))
    (dest / "AGENTS.md").write_text(root_agents(col))
    return col


def check(tree: Path) -> int:
    # The index's two files are checked by their checksums in index.json;
    # the COGs are not ours: no byte pass.
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="copernicus-dem-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        build(tree)
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnail ----------

def thumbnails() -> None:
    """The tiles over the world, coloured on max_elev as the default style."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap, Normalize
    from matplotlib.collections import PatchCollection
    from matplotlib.patches import Rectangle

    from thumbnails import BACKGROUND

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    rows = con.execute(f"SELECT west, south, east, north, coalesce(max_elev, 0) "
                       f"FROM read_parquet('{INDEX_URL}/{INDEX}.parquet')").fetchall()
    top = RAMP[-1][0]
    cmap = LinearSegmentedColormap.from_list("elev", [(v / top, c) for v, c in RAMP])
    fig = plt.figure(figsize=(6, 4), dpi=100)
    ax = fig.add_axes((0, 0, 1, 1))
    fig.patch.set_facecolor(BACKGROUND)
    ax.set_facecolor(BACKGROUND)
    patches = [Rectangle((w, s), e - w, n - s) for w, s, e, n, _ in rows]
    pc = PatchCollection(patches, cmap=cmap, norm=Normalize(0, top), edgecolor="none")
    pc.set_array([min(r[4], top) for r in rows])
    ax.add_collection(pc)
    ax.set_xlim(-180, 180)
    ax.set_ylim(-60, 84)    # 360 x 240 degrees would be mostly Antarctica: cut at 60 S
    ax.set_aspect("auto")   # plate carree stretched to 3:2
    ax.axis("off")
    THUMB.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(THUMB, dpi=100, facecolor=BACKGROUND)
    plt.close(fig)
    print(f"  {len(rows):,} tiles -> {THUMB.relative_to(REPO)}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--dest", type=Path, required=True)
    c = sub.add_parser("check")
    c.add_argument("tree", type=Path)
    u = sub.add_parser("publish")
    u.add_argument("--remote", required=True)
    u.add_argument("--dry-run", action="store_true")
    sub.add_parser("thumbnails")
    a = p.parse_args()
    if a.cmd == "build":
        build(a.dest)
        print(f"wrote {a.dest}/")
    elif a.cmd == "check":
        sys.exit(check(a.tree))
    elif a.cmd == "publish":
        publish(a.remote, dry_run=a.dry_run)
    else:
        thumbnails()


if __name__ == "__main__":
    main()
