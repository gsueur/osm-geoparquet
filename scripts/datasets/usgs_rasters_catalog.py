#!/usr/bin/env python3
"""
Render, validate and publish a Portolan catalog of USGS base rasters served
elsewhere as Cloud-Optimized GeoTIFFs: metadata only, nothing is copied.

  usgs-rasters/catalog/catalog.json                         root
  usgs-rasters/catalog/<collection>/collection.json         one per product
  usgs-rasters/catalog/<collection>/<item>/<item>.json      one per file

Collections:
- usgs-3dep-13: the USGS 3D Elevation Program's seamless 1/3 arc-second DEM
  (about 10 m), one item per 1-degree tile, read from the "current" folder of
  USGS's public bucket prd-tnm (https://prd-tnm.s3.amazonaws.com). The tiles
  are COGs (LZW, overviews), NAD83 (EPSG:4269), metres above NAVD88.
- annual-nlcd-*: Annual NLCD Collection 1 (USGS EROS), one collection per
  product (land cover, impervious surface and its descriptor, land cover
  change, confidence, spectral change), one item per year, CONUS mosaics at
  30 m. USGS's own bucket (usgs-landcover) is requester-pays, so the items
  point to the public Source Cooperative mirror
  (https://source.coop/kylebarron/usgs-landcover). The mirror holds C1V1,
  1985 to 2024; USGS's current release is v1.2 (1985 to 2025).

Each run lists the buckets, so the catalog follows them: a tile USGS adds or
replaces shows with its new date, one it removes leaves the catalog (the
publish is an rclone sync of a prefix holding only this generated metadata).
An item's `updated` is its file's Last-Modified; a collection's the newest of
its items, the root's the newest of its collections. The collection
thumbnails are committed under catalog/thumbnails/usgs-rasters/, rendered by
the `thumbnails` command from the files' overviews (3DEP: each tile's
smallest; Annual NLCD: the latest year's mosaic, in its own colours). Spec:
portolan-spec v0.2.0. Validator: rashid, through scripts/catalog.py.

Usage:
  uv run --project scripts --with rasterio --with matplotlib \
      python3 scripts/datasets/usgs_rasters_catalog.py thumbnails
  python3 scripts/datasets/usgs_rasters_catalog.py build --dest build/usgs-rasters-catalog
  python3 scripts/datasets/usgs_rasters_catalog.py check build/usgs-rasters-catalog
  python3 scripts/datasets/usgs_rasters_catalog.py publish --remote parquetry:parquetry/usgs-rasters
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import catalog as shared  # scripts/catalog.py
from catalog import (
    CONTACT_EMAIL,
    FILE_EXT,
    LOGO,
    PORTOLAN_SCHEMA,
    PUBLIC_BASE,
    REPO_URL,
    SITE_URL,
    md_link,
    now_rfc3339,
    write_json,
)
import filedates

DATASET_PREFIX = "usgs-rasters"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "usgs-rasters"
TITLE = "USGS base rasters: 3DEP elevation and Annual NLCD (linked)"
COG_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"
PROJ_EXT = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
ALTERNATE_EXT = shared.ALTERNATE_EXT
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

TNM = "https://prd-tnm.s3.amazonaws.com"
TNM_PREFIX = "StagedProducts/Elevation/13/TIFF/current/"
MIRROR = "https://data.source.coop"
MIRROR_PREFIX = "kylebarron/usgs-landcover/annual-nlcd/c1/v1/cu/mosaic/"
MIRROR_S3 = "s3://us-west-2.opendata.source.coop"

LICENSE_LINK = {
    "rel": "license",
    "href": "https://www.usgs.gov/information-policies-and-instructions/copyrights-and-credits",
    "type": "text/html",
    "title": "USGS products are in the US public domain; credit the USGS",
}
USGS = {"name": "U.S. Geological Survey", "roles": ["producer", "licensor"],
        "description": "Produces the 3D Elevation Program DEMs (National Geospatial Program) and "
                       "Annual NLCD (EROS Center). US public domain.",
        "url": "https://www.usgs.gov/"}
GEOMERMAIDS = {"name": "Geomermaids", "roles": ["processor"],
               "description": "Maintains this catalog of the files where they are served; it "
                              "hosts no raster data.",
               "url": SITE_URL, "email": CONTACT_EMAIL}
# Where the files are served: the USGS's own bucket for 3DEP, the Source
# Cooperative mirror for Annual NLCD.
# The host listed last (Portolan).
TNM_PROVIDERS = [GEOMERMAIDS, {**USGS, "roles": ["producer", "licensor", "host"]}]
NLCD_PROVIDERS = [USGS, GEOMERMAIDS, {"name": "Source Cooperative", "roles": ["host"],
                                      "description": "Serves the public mirror of the USGS's "
                                                     "Annual NLCD files "
                                                     "(kylebarron/usgs-landcover).",
                                      "url": "https://source.coop/kylebarron/usgs-landcover"}]
THUMBNAILS = shared.REPO / "catalog" / "thumbnails" / DATASET_PREFIX
# Conterminous US, the frame of the 3DEP thumbnail (the collection also has
# Alaska, Hawaii and the territories).
CONUS = (-125.0, 24.0, -66.0, 50.0)
LICENSE_MD = """\
USGS products are in the US public domain. Credit the USGS ("U.S. Geological
Survey, 3D Elevation Program" or "U.S. Geological Survey, Annual NLCD
Collection 1"). The files are the USGS's (3DEP) or a mirror of them (Annual
NLCD); this catalog hosts none of them."""

# Annual NLCD products: file code -> (collection id, title, what a pixel holds).
NLCD = {
    "LndCov": ("annual-nlcd-land-cover", "Annual NLCD land cover",
               "Land cover class (NLCD Anderson Level II legend, 16 classes: 11 open water, "
               "21-24 developed, 41-43 forest, 52 shrub, 71 grassland, 81-82 agriculture, "
               "90 and 95 wetlands...), 250 no data."),
    "FctImp": ("annual-nlcd-impervious", "Annual NLCD fractional impervious surface",
               "Share of the pixel covered by impervious surface, 0 to 100 percent."),
    "ImpDsc": ("annual-nlcd-impervious-descriptor", "Annual NLCD impervious descriptor",
               "What the impervious surface is (roads, urban, energy production...)."),
    "LndChg": ("annual-nlcd-land-cover-change", "Annual NLCD land cover change",
               "Land cover change from the previous year (from-to class code)."),
    "LndCnf": ("annual-nlcd-land-cover-confidence", "Annual NLCD land cover confidence",
               "Model confidence in the land cover class."),
    "SpcChg": ("annual-nlcd-spectral-change", "Annual NLCD spectral change day of year",
               "Day of year of the spectral change detected, where one was."),
}
# Measured on the 2024 land cover mosaic (gdalinfo wgs84Extent).
NLCD_BBOX = [-129.277320, 21.805095, -63.118430, 49.090269]
NLCD_DOCS = "https://www.mrlc.gov/data/project/annual-nlcd"
TNM_DOCS = "https://www.usgs.gov/3d-elevation-program"


# ---------- listings ----------

def list_bucket(base: str, prefix: str) -> list[dict]:
    """Every object under prefix: key, size, last modified (ListObjectsV2,
    paged by continuation token)."""
    out, token = [], None
    while True:
        q = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        if token:
            q["continuation-token"] = token
        url = base + "/?" + urllib.parse.urlencode(q)
        with urllib.request.urlopen(url, timeout=120) as r:
            root = ET.fromstring(r.read())
        for c in root.findall("s3:Contents", S3_NS):
            out.append({"key": c.findtext("s3:Key", namespaces=S3_NS),
                        "size": int(c.findtext("s3:Size", namespaces=S3_NS)),
                        "modified": c.findtext("s3:LastModified", namespaces=S3_NS)})
        if root.findtext("s3:IsTruncated", namespaces=S3_NS) != "true":
            return out
        token = root.findtext("s3:NextContinuationToken", namespaces=S3_NS)


def mirror_listing() -> list[dict]:
    """Source Cooperative answers ListObjectsV2 on its data host without the
    bucket in the path: the account is the first path segment."""
    account, rest = MIRROR_PREFIX.split("/", 1)
    objs = list_bucket(f"{MIRROR}/{account}", rest)
    for o in objs:
        o["key"] = f"{account}/{o['key']}" if not o["key"].startswith(account + "/") else o["key"]
    return objs


def rfc3339(s: str) -> str:
    return s.replace(".000Z", "Z")


# ---------- STAC ----------

def polygon(b: list[float]) -> dict:
    x0, y0, x1, y1 = b
    return {"type": "Polygon", "coordinates": [[[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]]}


def item(cid: str, iid: str, bbox: list[float], props: dict, href: str, s3: str, size: int,
         title: str, extensions: list[str]) -> dict:
    return {
        "type": "Feature", "stac_version": "1.1.0",
        "stac_extensions": [FILE_EXT, ALTERNATE_EXT, *extensions],
        "id": iid, "collection": cid, "bbox": bbox, "geometry": polygon(bbox),
        "properties": props,
        "assets": {"data": {"href": href, "type": COG_TYPE, "title": title, "roles": ["data"],
                            "file:size": size,
                            "alternate": {"s3": {"href": s3, "title": "S3, anonymous, us-west-2"}}}},
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../collection.json", "type": "application/json"},
            {"rel": "collection", "href": "../collection.json", "type": "application/json"},
        ],
    }


def tnm_items() -> list[dict]:
    rx = re.compile(re.escape(TNM_PREFIX) + r"([ns]\d{2}[ew]\d{3})/USGS_13_\1\.tif$")
    items = []
    for o in list_bucket(TNM, TNM_PREFIX):
        m = rx.match(o["key"])
        if not m:
            continue
        tile = m.group(1)
        lat = int(tile[1:3]) * (1 if tile[0] == "n" else -1)
        lon = int(tile[4:7]) * (-1 if tile[3] == "w" else 1)
        # The name is the tile's north-west corner.
        bbox = [float(lon), float(lat - 1), float(lon + 1), float(lat)]
        updated = rfc3339(o["modified"])
        items.append(item("usgs-3dep-13", tile, bbox,
                          {"datetime": updated, "updated": updated, "proj:code": "EPSG:4269",
                           "gsd": 10},
                          f"{TNM}/{o['key']}", f"s3://prd-tnm/{o['key']}", o["size"],
                          f"3DEP 1/3 arc-second DEM, tile {tile}", [PROJ_EXT]))
    return sorted(items, key=lambda i: i["id"])


def nlcd_items() -> dict[str, list[dict]]:
    rx = re.compile(r"Annual_NLCD_(\w+?)_(\d{4})_CU_C1V1\.tif$")
    out: dict[str, list[dict]] = {code: [] for code in NLCD}
    for o in mirror_listing():
        m = rx.search(o["key"])
        if not m or m.group(1) not in NLCD:
            continue
        code, year = m.group(1), m.group(2)
        cid, title, _ = NLCD[code]
        updated = rfc3339(o["modified"])
        out[code].append(item(cid, year, NLCD_BBOX,
                              {"datetime": None, "start_datetime": f"{year}-01-01T00:00:00Z",
                               "end_datetime": f"{year}-12-31T23:59:59Z", "updated": updated,
                               "gsd": 30},
                              f"{MIRROR}/{o['key']}", f"{MIRROR_S3}/{o['key']}", o["size"],
                              f"{title}, {year}", []))
    return {k: sorted(v, key=lambda i: i["id"]) for k, v in out.items()}


def collection(cid: str, title: str, description: str, items: list[dict], keywords: list[str],
               links: list[dict], providers: list[dict]) -> dict:
    bboxes = [i["bbox"] for i in items]
    bbox = [min(b[0] for b in bboxes), min(b[1] for b in bboxes),
            max(b[2] for b in bboxes), max(b[3] for b in bboxes)]
    starts = [i["properties"].get("start_datetime") or i["properties"]["datetime"] for i in items]
    ends = [i["properties"].get("end_datetime") or i["properties"]["datetime"] for i in items]
    return {
        "type": "Collection", "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": cid, "title": title, "description": description,
        "keywords": keywords, "license": "other",
        "updated": max(i["properties"]["updated"] for i in items),
        "providers": providers,
        "extent": {"spatial": {"bbox": [bbox]},
                   "temporal": {"interval": [[min(starts), max(ends)]]}},
        "item_assets": {"data": {"type": COG_TYPE, "roles": ["data"]}},
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{title}: README"),
            md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
            LICENSE_LINK, *links,
            *({"rel": "item", "href": f"./{i['id']}/{i['id']}.json", "type": "application/geo+json",
               "title": i["assets"]["data"]["title"]} for i in items),
        ],
    }


def tnm_collection(items: list[dict]) -> dict:
    total = sum(i["assets"]["data"]["file:size"] for i in items)
    return collection(
        "usgs-3dep-13", "3DEP seamless DEM, 1/3 arc-second (about 10 m)",
        f"The USGS 3D Elevation Program's seamless 1/3 arc-second digital elevation model: "
        f"{len(items):,} one-degree tiles ({total / 1e9:,.0f} GB), Cloud-Optimized GeoTIFFs "
        f"(LZW, overviews) in NAD83 (EPSG:4269), elevations in metres above NAVD88 over the "
        f"conterminous US, float32, no data -999999. Read in place from the USGS's public "
        f"bucket prd-tnm (the 'current' folder, which the USGS updates as new data arrives): "
        f"this collection lists the tiles, it copies none. Tile names give the north-west "
        f"corner (n43w072 spans 42 to 43 N, 72 to 71 W). Credit: U.S. Geological Survey, 3D "
        f"Elevation Program.",
        items, ["3DEP", "DEM", "elevation", "USGS", "COG", "1/3 arc-second"],
        [{"rel": "related", "href": TNM_DOCS, "type": "text/html", "title": "USGS 3D Elevation Program"},
         {"rel": "alternate", "href": f"{TNM}/index.html#{TNM_PREFIX}", "type": "text/html",
          "title": "The USGS bucket"}], TNM_PROVIDERS)


def nlcd_collection(code: str, items: list[dict]) -> dict:
    cid, title, pixel = NLCD[code]
    total = sum(i["assets"]["data"]["file:size"] for i in items)
    years = [i["id"] for i in items]
    return collection(
        cid, f"{title}, {years[0]} to {years[-1]}",
        f"{title} (USGS EROS, Annual NLCD Collection 1), one CONUS mosaic a year at 30 m, "
        f"{years[0]} to {years[-1]}: {len(items)} Cloud-Optimized GeoTIFFs ({total / 1e9:,.1f} "
        f"GB, DEFLATE, overviews) in the product's Albers equal-area projection on WGS 84. "
        f"{pixel} The USGS's own bucket is requester-pays: these items point to the public "
        f"Source Cooperative mirror of its files (C1V1). The USGS's current release, v1.2, "
        f"adds 2025. Credit: U.S. Geological Survey, Annual NLCD Collection 1.",
        items, ["Annual NLCD", "NLCD", "land cover", "USGS", "COG", "CONUS"],
        # A mirror (Source Cooperative hosts it): `via` the USGS original.
        [{"rel": "via", "href": NLCD_DOCS, "type": "text/html", "title": "Annual NLCD at MRLC"},
         {"rel": "alternate", "href": "https://source.coop/kylebarron/usgs-landcover",
          "type": "text/html", "title": "The Source Cooperative mirror"}], NLCD_PROVIDERS)


# ---------- markdown ----------

EXAMPLE_LONLAT = (-71.0589, 42.3601)  # Boston City Hall


def access_md(col: dict) -> str:
    x, y = EXAMPLE_LONLAT
    if col["id"] == "usgs-3dep-13":
        url = f"{TNM}/{TNM_PREFIX}n43w072/USGS_13_n43w072.tif"
        return f"""\
The tile holding a point is named after the whole degrees north and west of it
(Boston, {y} N {-x} W, is in `n43w072`). Elevation at a point with GDAL:

```sh
gdallocationinfo -valonly -wgs84 /vsicurl/{url} {x} {y}
```

A window with rasterio (only the blocks needed are read):

```python
import rasterio
from rasterio.windows import from_bounds
with rasterio.open("{url}") as src:
    dem = src.read(1, window=from_bounds(-71.12, 42.33, -71.00, 42.40, src.transform))
```

The items list each tile with its URL: read `./<tile>/<tile>.json` or filter
items on `bbox` to find the tiles for an area. The same files are at
`s3://prd-tnm/{TNM_PREFIX}` (anonymous, us-west-2)."""
    year = col["links"][-1]["href"].split("/")[1]
    code = next(k for k, v in NLCD.items() if v[0] == col["id"])
    url = f"{MIRROR}/{MIRROR_PREFIX}Annual_NLCD_{code}_{year}_CU_C1V1.tif"
    return f"""\
One file a year: `./<year>/<year>.json` gives its URL. The value at a point with
GDAL (the file is in Albers: `-wgs84` lets GDAL transform the lon/lat):

```sh
gdallocationinfo -valonly -wgs84 /vsicurl/{url} {x} {y}
```

A clip with GDAL, kept in the source grid (no resampling):

```sh
gdal_translate -projwin_srs EPSG:4326 -projwin -71.2 42.45 -70.95 42.25 \\
  /vsicurl/{url} boston_{year}.tif
```

The same files are at `{MIRROR_S3}/{MIRROR_PREFIX}` (anonymous)."""


def collection_readme(col: dict) -> str:
    n = sum(1 for link in col["links"] if link["rel"] == "item")
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| Items | {n:,} |
| Extent | {', '.join(f'{v:.2f}' for v in col['extent']['spatial']['bbox'][0])} (lon/lat) |
| Time | {col['extent']['temporal']['interval'][0][0][:10]} to {col['extent']['temporal']['interval'][0][1][:10]} |
| Format | Cloud-Optimized GeoTIFF, read over HTTP range requests |

## Access

{access_md(col)}

## Provenance

[usgs_rasters_catalog.py]({REPO_URL}/blob/main/scripts/datasets/usgs_rasters_catalog.py)
lists the bucket each week and writes one item per file with its URL, size and
Last-Modified date. Nothing is downloaded or rehosted.

{filedates.DATES_MD}
## License

{LICENSE_MD}
"""


def collection_agents(col: dict) -> str:
    return f"""\
# {col['title']}: agent guide

Rasters, not tables: no SQL. Read the COGs in place over HTTP with GDAL
(`/vsicurl/`), rasterio or any COG reader; each item's `data` asset is one
file. {'Pick the tile named after the whole degrees north and west of a point.' if col['id'] == 'usgs-3dep-13' else 'One file a year; the item id is the year.'}

## Access

{access_md(col)}

## Tips

- Read windows, not whole files: they are hundreds of MB to GB each, and COG
  blocks let a reader fetch only what a window needs.
- Keep the source grid when you can; reprojecting resamples.
- Credit the USGS. Public domain.
"""


def root_readme(root: dict, cols: list[dict]) -> str:
    rows = "\n".join(f"| [{c['title']}](./{c['id']}/README.md) | "
                     f"{sum(1 for link in c['links'] if link['rel'] == 'item'):,} |" for c in cols)
    return f"""\
# {root['title']}

{root['description']}

| Collection | Items |
|---|---|
{rows}

{filedates.DATES_MD}
## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(cols: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in cols)
    return f"""\
# {CATALOG_ID}: agent guide

USGS base rasters as Cloud-Optimized GeoTIFFs, listed where they are served:
the files are not on parquetry. Read them in place with GDAL (`/vsicurl/`) or
rasterio, a window at a time.

{listing}

3DEP tiles are at {TNM}/{TNM_PREFIX} (public, us-west-2). Annual NLCD files are
on the Source Cooperative mirror {MIRROR}/{MIRROR_PREFIX} (public; the USGS's
own bucket is requester-pays). Public domain; credit the USGS. NAIP imagery is
in Microsoft Planetary Computer's own STAC collection, linked from the
Geomermaids root catalog with a guide of ours.

{filedates.DATES_MD}
"""


def build_root(cols: list[dict], checked: str) -> dict:
    return {
        "type": "Catalog", "stac_version": "1.1.0", "stac_extensions": [PORTOLAN_SCHEMA],
        "id": CATALOG_ID, "title": TITLE,
        "description": (
            "USGS base rasters as Cloud-Optimized GeoTIFFs, listed where they are served, "
            "nothing copied: the 3D Elevation Program's seamless 1/3 arc-second DEM (about "
            "10 m, one item per one-degree tile, from the USGS's public bucket) and Annual NLCD "
            "Collection 1 (land cover, impervious surface and change products, one CONUS "
            "mosaic a year at 30 m since 1985, from a public mirror of the USGS's "
            "requester-pays bucket). Rebuilt weekly from the bucket listings. Public domain; "
            "credit the USGS."),
        "updated": max(c["updated"] for c in cols),
        "checked": checked,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in cols),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK,
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


# ---------- thumbnails ----------

def thumbnails() -> None:
    """catalog/thumbnails/usgs-rasters/<collection>.png, 600 x 400, from the
    files' overviews: a few MB read in all. Needs rasterio and matplotlib."""
    from concurrent.futures import ThreadPoolExecutor

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import rasterio
    from rasterio.enums import Resampling

    from thumbnails import BACKGROUND, REGION_FILL, frame

    THUMBNAILS.mkdir(parents=True, exist_ok=True)
    # No directory listing on open: each tile's folder sits in a bucket
    # listing GDAL would otherwise fetch for sidecar files.
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
    os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = ".tif"

    def save(rgba, extent: tuple[float, float, float, float], box, aspect: float, dest: Path):
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.set_facecolor(BACKGROUND)
        fig.patch.set_facecolor(BACKGROUND)
        ax.imshow(rgba, extent=extent, interpolation="antialiased", aspect=aspect)
        ax.set_xlim(box[0], box[2])
        ax.set_ylim(box[1], box[3])
        ax.set_axis_off()
        fig.savefig(dest, facecolor=BACKGROUND)
        plt.close(fig)
        print(f"  {dest.relative_to(shared.REPO)}: {dest.stat().st_size / 1e3:.0f} kB")

    # 3DEP: each conterminous tile's smallest overview, on a 0.05 degree grid.
    step = 20  # pixels per one-degree tile
    x0, y0, x1, y1 = CONUS
    grid = np.full((int(y1 - y0) * step, int(x1 - x0) * step), np.nan, dtype="float32")
    tiles = [i for i in tnm_items()
             if x0 <= i["bbox"][0] < x1 and y0 <= i["bbox"][1] < y1]

    def read_tile(i):
        with rasterio.open(i["assets"]["data"]["href"]) as src:
            a = src.read(1, out_shape=(step, step), resampling=Resampling.average,
                         masked=True).astype("float32")
        return i, a.filled(np.nan)

    with ThreadPoolExecutor(16) as ex:
        for i, a in ex.map(read_tile, tiles):
            r = int(y1 - i["bbox"][3]) * step
            c = int(i["bbox"][0] - x0) * step
            grid[r:r + step, c:c + step] = a
    cmap = plt.get_cmap("terrain")
    # From -800 m, so sea level falls on the colour map's greens, not its
    # water blues.
    norm = plt.Normalize(-800, 3500)
    rgba = cmap(norm(np.nan_to_num(grid, nan=0)))
    # The sea inside coastal tiles is 0 m, not no data.
    rgba[np.isnan(grid) | (grid <= 0)] = (0, 0, 0, 0)
    *box, aspect = frame(*CONUS)
    save(rgba, (x0, x1, y0, y1), box, aspect, THUMBNAILS / "usgs-3dep-13.png")

    # Annual NLCD: the latest year, the 64x overview (2,500 x 1,640 pixels).
    listed = nlcd_items()
    for code, (cid, _, _) in NLCD.items():
        year = max(i["id"] for i in listed[code])
        href = f"{MIRROR}/{MIRROR_PREFIX}Annual_NLCD_{code}_{year}_CU_C1V1.tif"
        with rasterio.open(href, overview_level=2) as src:
            a = src.read(1)
            nodata = src.nodata
            try:
                cm = src.colormap(1)
            except ValueError:
                cm = None
            b = src.bounds
        valid = a != nodata
        if cm:
            lut = np.zeros((max(max(cm), int(a.max())) + 1, 4), dtype="float32")
            for k, v in cm.items():
                lut[k] = [c / 255 for c in v]
            # A black 0 (no impervious surface) drawn in the land colour.
            if tuple(lut[0][:3]) == (0, 0, 0):
                lut[0] = matplotlib.colors.to_rgba(REGION_FILL)
            rgba = lut[a]
        else:
            # Spectral change: a day of year where a change was detected, 0
            # where none was (most of the map), drawn in the land colour.
            rgba = plt.get_cmap("plasma")(plt.Normalize(1, 366)(a))
            rgba[a == 0] = matplotlib.colors.to_rgba(REGION_FILL)
        rgba[~valid] = (0, 0, 0, 0)
        # Albers metres: no latitude correction.
        w, h = b.right - b.left, b.top - b.bottom
        pad = 0.04
        if w / h < 1.5:
            W, H = h * (1 + pad) * 1.5, h * (1 + pad)
        else:
            W, H = w * (1 + pad), w * (1 + pad) / 1.5
        cx, cy = (b.left + b.right) / 2, (b.bottom + b.top) / 2
        save(rgba, (b.left, b.right, b.bottom, b.top),
             (cx - W / 2, cy - H / 2, cx + W / 2, cy + H / 2), 1.0, THUMBNAILS / f"{cid}.png")


# ---------- build ----------

def write_collection(dest: Path, col: dict, items: list[dict], checked: str) -> None:
    col["checked"] = checked
    cdir = dest / col["id"]
    thumb = THUMBNAILS / f"{col['id']}.png"
    if not thumb.is_file():
        sys.exit(f"{thumb} missing: run the thumbnails command")
    cdir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(thumb, cdir / "thumbnail.png")
    col["assets"] = {"thumbnail": {
        "href": "./thumbnail.png", "type": "image/png",
        "title": f"{col['title']}: preview", "roles": ["thumbnail"],
        "file:size": thumb.stat().st_size, "file:checksum": shared.multihash(thumb)}}
    col["stac_extensions"] = [*col["stac_extensions"], FILE_EXT]
    write_json(cdir / "collection.json", col)
    (cdir / "README.md").write_text(collection_readme(col))
    (cdir / "AGENTS.md").write_text(collection_agents(col))
    for i in items:
        write_json(cdir / i["id"] / f"{i['id']}.json", i)


def build(dest: Path) -> list[dict]:
    checked = now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    cols = []
    tiles = tnm_items()
    if len(tiles) < 1000:
        sys.exit(f"only {len(tiles)} 3DEP tiles listed: the bucket listing looks wrong")
    col = tnm_collection(tiles)
    write_collection(dest, col, tiles, checked)
    cols.append(col)
    for code, items in nlcd_items().items():
        if len(items) < 30:
            sys.exit(f"only {len(items)} Annual NLCD {code} files on the mirror")
        col = nlcd_collection(code, items)
        write_collection(dest, col, items, checked)
        cols.append(col)
    root = build_root(cols, checked)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, cols))
    (dest / "AGENTS.md").write_text(root_agents(cols))
    return cols


def check(tree: Path) -> int:
    # The assets are USGS's and Source Cooperative's (terabytes of COGs):
    # metadata and schema only, no byte pass over them.
    return shared.check(tree, "--data-scope", "local")


def publish(remote: str, *, dry_run: bool) -> None:
    """rclone sync of the generated tree (the prefix holds nothing else), the
    root catalog.json last so it never links a collection not there yet."""
    with tempfile.TemporaryDirectory(prefix="usgs-rasters-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        cols = build(tree)
        print(f"  rendered {len(cols)} collections")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        dst = f"{remote}/{CATALOG_PREFIX}"
        headers = ["--header-upload", "Cache-Control: public, max-age=300"]
        dry = ["--dry-run"] if dry_run else []
        for cmd in (
            ["rclone", "sync", str(tree), dst, "--exclude", "/catalog.json", "--checksum",
             "--transfers", "32", *headers, *dry],
            ["rclone", "copyto", str(tree / "catalog.json"), f"{dst}/catalog.json",
             "--ignore-times", *headers, "--header-upload", "Content-Type: application/json", *dry],
        ):
            print("  $ " + " ".join(cmd))
            subprocess.run(cmd, check=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="render the catalog from the bucket listings, then check it")
    b.add_argument("--dest", type=Path, default=Path("build/usgs-rasters-catalog"))
    sub.add_parser("thumbnails", help="render catalog/thumbnails/usgs-rasters/*.png")
    c = sub.add_parser("check", help="rashid over a rendered catalog")
    c.add_argument("tree", type=Path)
    u = sub.add_parser("publish", help="render, check and sync to the bucket")
    u.add_argument("--remote", default=f"parquetry:parquetry/{DATASET_PREFIX}")
    u.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if args.cmd == "build":
        cols = build(args.dest)
        print(f"wrote {args.dest}/ ({len(cols)} collections)")
        sys.exit(check(args.dest))
    if args.cmd == "check":
        sys.exit(check(args.tree))
    if args.cmd == "thumbnails":
        thumbnails()
        return
    publish(args.remote, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
