#!/usr/bin/env python3
"""
GeoParquet tile index of the Copernicus DEM GLO-30 (the world at 1
arc-second, about 30 m), for map pickers (Geobox's Elevation page) and GDAL:
which COG covers a place, its grid and elevation range, without opening any
of them. Metadata only; the tiles stay in the AWS Open Data bucket
copernicus-dem-30m (public, range requests, but no CORS: a browser cannot
read them directly).

  copernicus-dem/index/copernicus-glo30-tiles.parquet   one row per tile
  copernicus-dem/index/copernicus-glo30-tiles.pmtiles   footprints (layer `tiles`, z0-9);
                                                        Web Mercator: the 1,440 tiles past
                                                        85.05 degrees (Antarctica) are not in it
  copernicus-dem/index/index.json                       counts, sizes, sha256, when listed

The same columns as the 3DEP index (usgs_dem_index.py), so one reader serves
both; the columns 3DEP fills and Copernicus has no value for are null.

Source: one 1 degree cell per tile, named for its south-west corner
(Copernicus_DSM_COG_10_N45_00_E006_00_DEM), WGS 84 (EPSG:4326, heights
EGM2008). The COGs are PixelIsPoint: cell centres fall on whole degrees, so
a tile's grid runs from half a cell west and north of its corner (GDAL's
reading, kept in native_bbox); footprints are cut at the antimeridian and
the poles, which the outer tiles pass by half a cell. Cells are 1
arc-second north-south; east-west they widen with latitude
(3600 columns up to 50 degrees, then 2400, 1800, 1200, 720 and 360 at the
poles), so pixel_size is the east-west size and width varies. Land only:
cells of open sea have no tile. The 2021 release this bucket holds leaves
out Armenia and Azerbaijan (GLO-90 only). The listing is the whole bucket
(each tile's folder also holds quality layers, previews and an ISO 19115
XML), checked against its tileList.txt.

Grid and nodata come from each COG's header (one 64 KB range request); the
elevation range from the tile's XML (gmd:verticalElement minimumValue and
maximumValue), the COGs carrying no GDAL statistics. A tile whose
Last-Modified is the one in the published index keeps its published row, so
a weekly run reads nothing when the bucket has not moved.

Columns (see usgs_dem_index.py for the full list):
  tile_id          the file name without .tif
  product          "glo30"
  published        the COG's Last-Modified in the bucket (TIMESTAMPTZ, UTC)
  url              the COG over HTTPS (GDAL GTI: -oo LOCATION_FIELD=url)
  s3_url           s3://copernicus-dem-30m/... (anonymous, eu-central-1)
  file_size        bytes
  crs, epsg, proj4 EPSG:4326
  nodata           null (the COGs declare none)
  pixel_size       east-west, degrees; width, height in cells
  min_elev, max_elev  metres above EGM2008, from the tile's XML
  native_bbox      the grid's extent, as GDAL reads it
  west, south, east, north, geometry, bbox   the footprint, within +-180 / +-90
  project, acquired_start, acquired_end, thumbnail_url   null

Attribution, from the licence (Article 6):
  © DLR e.V. 2010-2014 and © Airbus Defence and Space GmbH 2014-2018
  provided under COPERNICUS by the European Union and ESA; all rights reserved.

Usage:
  python3 scripts/datasets/copernicus_dem_index.py build --out-dir out/index [--check 25]
  python3 scripts/datasets/copernicus_dem_index.py publish --out-dir out/index --remote parquetry:parquetry/copernicus-dem
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.usgs_dem_index import (check_parser, get, publish, published_rows, read_headers,
                                     utc, write, write_footprints)
from datasets.usgs_rasters_catalog import list_bucket

BUCKET = "copernicus-dem-30m"
SOURCE = f"https://{BUCKET}.s3.amazonaws.com"
PUBLIC = "https://parquetry.geomermaids.com/copernicus-dem/index"
INDEX = "copernicus-glo30-tiles"
PRODUCT = "glo30"
TILE = re.compile(r"(Copernicus_DSM_COG_10_([NS]\d\d_00_[EW]\d{3}_00)_DEM)/\1\.tif$")
GMD = {"gmd": "http://www.isotc211.org/2005/gmd", "gco": "http://www.isotc211.org/2005/gco"}
PROPERTIES = ["tile_id", "product", "url", "epsg", "min_elev", "max_elev"]
ATTRIBUTION = ("© DLR e.V. 2010-2014 and © Airbus Defence and Space GmbH 2014-2018 provided "
               "under COPERNICUS by the European Union and ESA; all rights reserved.")


def listing() -> tuple[list[dict], dict[str, str]]:
    """The DEM COGs in the bucket, and each tile's XML key, by tile_id."""
    tifs, xmls = [], {}
    for o in list_bucket(SOURCE, "Copernicus_DSM_COG_10_"):
        if m := TILE.match(o["key"]):
            tifs.append(o | {"tile_id": m[1]})
        elif o["key"].endswith(".xml") and o["key"].count("/") == 1:
            xmls[o["key"].split("/")[0]] = o["key"]
    listed = {line.strip() for line in get(f"{SOURCE}/tileList.txt").decode().splitlines()
              if line.strip()}
    found = {o["tile_id"] for o in tifs}
    if listed != found:
        print(f"  tileList.txt and the bucket differ: {len(listed - found)} listed without a "
              f"COG {sorted(listed - found)[:5]}, {len(found - listed)} COGs not listed "
              f"{sorted(found - listed)[:5]}", flush=True)
    return tifs, xmls


def elevation_range(key: str) -> tuple[float | None, float | None]:
    doc = ET.fromstring(get(f"{SOURCE}/{key}"))
    out = []
    for name in ("minimumValue", "maximumValue"):
        v = doc.findtext(f".//gmd:verticalElement//gmd:{name}/gco:Real", namespaces=GMD)
        out.append(float(v) if v and v.strip() else None)
    return out[0], out[1]


def build(out: Path, check: int) -> None:
    out.mkdir(parents=True, exist_ok=True)
    old = published_rows(f"{PUBLIC}/{INDEX}.parquet")
    tifs, xmls = listing()
    keep, read = [], []
    for o in tifs:
        r = {"tile_id": o["tile_id"], "product": PRODUCT, "project": None,
             "acquired_start": None, "acquired_end": None, "published": utc(o["modified"]),
             "url": f"{SOURCE}/{o['key']}", "s3_url": f"s3://{BUCKET}/{o['key']}",
             "thumbnail_url": None, "file_size": o["size"]}
        prev = old.get(r["tile_id"])
        (keep.append(prev) if prev and prev["published"] == r["published"] else read.append(r))
    print(f"  {len(tifs):,} tiles, {len(read):,} to read", flush=True)
    if check and read:
        check_parser(read, check, products=(PRODUCT,))
    got, failed = read_headers(read, nodata=None)
    if failed:
        sys.exit(f"headers unread: {failed[:5]}")

    def with_range(r):
        key = xmls.get(r["tile_id"])
        if r["min_elev"] is None and key:
            try:
                r["min_elev"], r["max_elev"] = elevation_range(key)
            except Exception as e:  # noqa: BLE001
                return r, f"{r['tile_id']}: {type(e).__name__}: {e}"
        return r, None

    with cf.ThreadPoolExecutor(48) as ex:
        done = list(ex.map(with_range, got))
    got = [r for r, _ in done]
    unread = [e for _, e in done if e]
    if unread:
        print(f"  {len(unread)} XML unread (elevation range null): {unread[:3]}", flush=True)
    rows = keep + got
    if any(r["epsg"] != 4326 for r in rows):
        sys.exit("a tile not in EPSG:4326")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    files = {INDEX: write(con, rows, out / f"{INDEX}.parquet")}
    print(f"  {INDEX}.parquet: {files[INDEX]['rows']:,} rows, "
          f"{files[INDEX]['bytes'] / 1e6:.1f} MB", flush=True)
    tiles = f"{INDEX}-pmtiles"
    files[tiles] = write_footprints(con, out / f"{INDEX}.parquet", out / f"{INDEX}.pmtiles",
                                    "true", (0, 9), PROPERTIES)
    print(f"  {INDEX}.pmtiles: {files[tiles]['bytes'] / 1e6:.1f} MB", flush=True)
    no_range = con.execute(f"SELECT count(*) FILTER (WHERE min_elev IS NULL) FROM "
                           f"read_parquet('{out / INDEX}.parquet')").fetchone()[0]
    (out / "index.json").write_text(json.dumps({
        "listed": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": f"{SOURCE}/", "attribution": ATTRIBUTION,
        "tiles": len(rows), "bytes": sum(r["file_size"] for r in rows),
        "without_elevation_range": no_range, "files": files}, indent=1) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--out-dir", type=Path, required=True)
    b.add_argument("--check", type=int, default=25, help="headers checked against gdalinfo")
    p = sub.add_parser("publish")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--remote", required=True)
    a = ap.parse_args()
    if a.cmd == "build":
        build(a.out_dir, a.check)
    else:
        publish(a.out_dir, a.remote)


if __name__ == "__main__":
    main()
