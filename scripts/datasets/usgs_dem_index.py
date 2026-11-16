#!/usr/bin/env python3
"""
GeoParquet tile index of USGS 3DEP elevation, for map pickers (Geobox's
Elevation page) and GDAL: which COG covers a place, in which CRS and grid,
without opening any of them. Metadata only; the tiles stay in USGS's public
bucket prd-tnm (CORS open).

  usgs-rasters/index/3dep-tiles.parquet          one row per tile, all three products
  usgs-rasters/index/3dep-1m-tiles.pmtiles       footprints of the 1 m tiles (layer `tiles`)
  usgs-rasters/index/3dep-arcsec-tiles.pmtiles   footprints of the 1/3 and 1 arc-second tiles
  usgs-rasters/index/index.json                  counts, sizes, sha256, when listed

PMTiles features: tile_id, product, project, acquired_end, url, epsg,
min_elev, max_elev, enough to preview and add a tile from a click.

Products (`product`):
- `1m`: the 1 m DEM, one folder per lidar project under
  StagedProducts/Elevation/1m/Projects/<project>/TIFF/. A tile is the 10 km
  UTM cell whose north-west corner is (X * 10 km, Y * 10 km): 10,012 x
  10,012 cells of 1 m, 6 m of overlap on each side. Three
  name patterns (2026-11-15): USGS_1M_<zone>_x<X>y<Y>_<project>.tif (NAD83 /
  UTM <zone>, EPSG:269<zone>), and the older USGS_one_meter_x<X>y<Y>_... and
  USGS_1m_x<X>y<Y>_..., whose names lack the zone.
  Projects overlap: a spot can be in several tiles, one per project, all kept.
  Each project's acquisition dates come from one of its tiles' FGDC
  metadata (timeperd: begdate, enddate).
- `13` and `1`: the seamless 1/3 and 1 arc-second DEMs ("current"), one tile
  per 1 degree cell named for its north-west corner (n42w072), NAD83
  (EPSG:4269), 10,812 x 10,812 and 3,612 x 3,612 cells, 6 cells of overlap
  on each side (a few at the antimeridian and coasts differ: the header's
  grid is used). Mosaics of many sources: no project, no
  acquisition date.

Every tile's CRS, grid, nodata and elevation range come from its GeoTIFF
header (one 16 KB range request: the first IFD and GDAL's metadata tag, whose
STATISTICS_MINIMUM/MAXIMUM most 3DEP COGs carry; the older 1 m pattern
mostly does not, and gets null). A tile whose Last-Modified is the one in
the published index keeps its published row, so a weekly run reads headers
only for new or replaced tiles. Grids taken from names are checked against the COG
headers of a sample of tiles each build (`--check`, 25 per product, gdalinfo
over HTTPS): size, pixel size, origin, CRS and nodata must match, or the
build fails.

Columns:
  tile_id          the file name without .tif (the PMTiles `tile_id` too)
  product          "1m", "13" or "1"
  project          the lidar project (1m); null for 13 and 1
  acquired_start   first day of the project's acquisition (DATE, 1m)
  acquired_end     last day (DATE, 1m)
  published        the file's Last-Modified in the bucket (TIMESTAMPTZ, UTC)
  url              the COG over HTTPS (GDAL GTI: -oo LOCATION_FIELD=url)
  s3_url           s3://prd-tnm/... (anonymous, us-west-2)
  thumbnail_url    USGS's preview JPEG
  file_size        bytes
  crs              "EPSG:26916", "EPSG:4269"...
  epsg             the same, as an integer
  proj4            the CRS as a PROJ string, for browser reprojection
  nodata           the COG's: -999999, or -3.402823e+38 for some older 1 m tiles
  pixel_size       in CRS units: 1 (metre) or 1/10800, 1/3600 (degree)
  width, height    the COG's size in cells
  min_elev, max_elev  the tile's elevation range (metres, DOUBLE), from its
                   GDAL statistics; null when the COG has none
  native_bbox      the COG's extent in its own CRS, STRUCT(xmin, ymin, xmax, ymax)
  west, south, east, north   the footprint's extent in EPSG:4326 (DOUBLE)
  geometry, bbox   the footprint in lon/lat (OGC:CRS84), UTM edges densified
                   to 1 km before the transform, with its bbox covering

Rows are sorted by product then tile_id: a lookup by tile_id reads one row
group. NAD83 is taken as lon/lat (under 2 m apart in CONUS).

Reading:
  DuckDB:  SELECT url, crs, proj4, project, acquired_end
           FROM read_parquet('.../3dep-tiles.parquet') WHERE tile_id = '...'
  GDAL:    gdalwarp -t_srs EPSG:26916 -tr 1 1 -te <xmin ymin xmax ymax> \\
             -oo LOCATION_FIELD=url -oo SORT_FIELD=acquired_end \\
             "GTI:/vsicurl/https://.../3dep-tiles.parquet" out.tif
           (with -oo FILTER="product = '1m'" for one product)

Usage:
  python3 scripts/datasets/usgs_dem_index.py build --out-dir out/index [--check 25]
  python3 scripts/datasets/usgs_dem_index.py publish --out-dir out/index --remote parquetry:parquetry/usgs-rasters
"""

from __future__ import annotations

import argparse
import collections
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import random
import re
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles
from datasets.census import BBOX
from datasets.gaul import rewrite, verify
from datasets.usgs_rasters_catalog import S3_NS, TNM, list_bucket

PUBLIC = "https://parquetry.geomermaids.com/usgs-rasters/index"
INDEX = "3dep-tiles"
ONE_M = "StagedProducts/Elevation/1m/Projects/"
NODATA = -999999.0
CELL_M, OVERLAP = 10_000, 6
# Arc-second products: prefix, cells per degree.
ARCSEC = {"13": ("StagedProducts/Elevation/13/TIFF/current/", 10800),
          "1": ("StagedProducts/Elevation/1/TIFF/current/", 3600)}
NAME_1M = re.compile(r"USGS_1M_(\d{1,2})_x(\d+)y(\d+)_(.+)\.tif$")
NAME_1M_OLD = re.compile(r"USGS_(?:one_meter|1m)_x(\d+)y(\d+)_(.+)\.tif$")
COLUMNS = ("tile_id", "product", "project", "acquired_start", "acquired_end", "published", "url",
           "s3_url", "thumbnail_url", "file_size", "crs", "epsg", "proj4", "nodata", "pixel_size",
           "width", "height", "min_elev", "max_elev", "native_bbox", "wkt")


def utc(s: str) -> str:
    return s.replace(".000Z", "Z")


def get(url: str, headers: dict | None = None, attempts: int = 4) -> bytes:
    for i in range(attempts):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers or {}),
                                        timeout=60) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code < 500 or i == attempts - 1:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if i == attempts - 1:
                raise
        time.sleep(2 ** i)
    raise AssertionError


def prefixes(base: str, prefix: str) -> list[str]:
    """The "folders" directly under prefix (ListObjectsV2 with a delimiter)."""
    out, token = [], None
    while True:
        q = {"list-type": "2", "prefix": prefix, "delimiter": "/", "max-keys": "1000"}
        if token:
            q["continuation-token"] = token
        root = ET.fromstring(get(base + "/?" + urllib.parse.urlencode(q)))
        out += [p.findtext("s3:Prefix", namespaces=S3_NS)
                for p in root.findall("s3:CommonPrefixes", S3_NS)]
        if root.findtext("s3:IsTruncated", namespaces=S3_NS) != "true":
            return out
        token = root.findtext("s3:NextContinuationToken", namespaces=S3_NS)


# ---------- reading a tile's header and a project's metadata ----------

class Ranged:
    """A remote file read in 64 KB chunks, fetched when first touched."""
    CHUNK = 65536

    def __init__(self, url: str):
        self.url, self.chunks, self.requests = url, {}, 0

    def read(self, off: int, n: int) -> bytes:
        out = b""
        while n > 0:
            i, at = divmod(off, self.CHUNK)
            if i not in self.chunks:
                self.requests += 1
                self.chunks[i] = get(self.url, {
                    "Range": f"bytes={i * self.CHUNK}-{(i + 1) * self.CHUNK - 1}"})
            piece = self.chunks[i][at:at + n]
            if not piece:
                raise ValueError(f"read past the end at {off}")
            out, off, n = out + piece, off + len(piece), n - len(piece)
        return out


def header(url: str) -> dict:
    """CRS, grid, nodata and GDAL statistics from a GeoTIFF's first IFD. A
    COG keeps it in the first chunk (one request); a plain GeoTIFF may keep
    it anywhere (some 1/3 arc-second tiles: at the end), and its tags are
    read where they point."""
    f = Ranged(url)
    b = f.read(0, 16)
    e = "<" if b[:2] == b"II" else ">"
    big = struct.unpack(e + "H", b[2:4])[0] == 43
    if big:
        off = struct.unpack(e + "Q", b[8:16])[0]
        count, p, size = struct.unpack(e + "Q", f.read(off, 8))[0], off + 8, 20
    else:
        off = struct.unpack(e + "I", b[4:8])[0]
        count, p, size = struct.unpack(e + "H", f.read(off, 2))[0], off + 2, 12
    width = {1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 6: 1, 7: 1, 8: 2, 9: 4, 10: 8, 11: 4, 12: 8, 16: 8}
    code = {2: "s", 3: "H", 4: "I", 12: "d", 16: "Q"}
    entries = f.read(p, count * size)
    tags = {}
    for i in range(count):
        ent = entries[i * size:(i + 1) * size]
        tag, typ, c = struct.unpack(e + ("HHQ" if big else "HHI"), ent[:12 if big else 8])
        if tag not in (256, 257, 33550, 33922, 34735, 42112, 42113):
            continue
        val, ln = ent[12:20] if big else ent[8:12], width[typ] * c
        if ln > len(val):
            val = f.read(struct.unpack(e + ("Q" if big else "I"), val)[0], ln)
        val = val[:ln]
        tags[tag] = (val.rstrip(b"\0").decode() if typ == 2
                     else struct.unpack(e + code[typ] * c, val))
    gk = tags[34735]
    keys = {gk[4 + 4 * i]: gk[7 + 4 * i] for i in range(gk[3]) if gk[5 + 4 * i] == 0}
    w, h = tags[256][0], tags[257][0]
    sx, sy = tags[33550][:2]
    tp = tags[33922]
    x0, y1 = tp[3] - tp[0] * sx, tp[4] + tp[1] * sy
    if keys.get(1025) == 2:   # PixelIsPoint: GDAL puts the cell's centre on the tie point
        x0, y1 = x0 - sx / 2, y1 + sy / 2
    return {"epsg": keys.get(3072) or keys.get(2048), "size": [w, h], "pixel": sx,
            "extent": [x0, y1 - h * sy, x0 + w * sx, y1],
            "nodata": float(tags[42113]) if 42113 in tags else None,   # as GDAL reports it
            **stats(tags.get(42112, ""))}


def stats(gdal_metadata: str) -> dict:
    """STATISTICS_MINIMUM and _MAXIMUM of band 1 from GDAL's metadata tag."""
    out = {"min_elev": None, "max_elev": None}
    for key, name in (("min_elev", "STATISTICS_MINIMUM"), ("max_elev", "STATISTICS_MAXIMUM")):
        m = re.search(rf'<Item name="{name}"(?: sample="0")?[^>]*>([^<]+)</Item>', gdal_metadata)
        if m:
            out[key] = float(m[1])
    return out


def acquisition(project_prefix: str) -> tuple[str | None, str | None]:
    """A project's acquisition dates, from the FGDC metadata of its first
    tile (timeperd: rngdates begdate/enddate, or a single caldate)."""
    q = urllib.parse.urlencode({"list-type": "2", "prefix": project_prefix + "metadata/",
                                "max-keys": "5"})
    root = ET.fromstring(get(f"{TNM}/?{q}"))
    keys = [k.text for k in root.iter(f"{{{S3_NS['s3']}}}Key") if k.text.endswith(".xml")]
    if not keys:
        return None, None
    doc = ET.fromstring(get(f"{TNM}/{keys[0]}"))
    t = doc.find(".//timeperd/timeinfo")
    if t is None:
        return None, None

    def day(s):
        s = (s or "").strip()
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}" if re.fullmatch(r"\d{8}", s) else None

    if t.find("rngdates") is not None:
        return day(t.findtext("rngdates/begdate")), day(t.findtext("rngdates/enddate"))
    single = day(t.findtext("sngdate/caldate"))
    return single, single


def proj4_of(epsg: int) -> str:
    """For browser reprojection (proj4js); lossless enough for NAD83 UTM and
    NAD83 geographic, which is all 3DEP uses (pyproj warns in general)."""
    import warnings
    from pyproj import CRS
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        return CRS.from_epsg(epsg).to_proj4()


def utm_zone(epsg: int) -> int:
    from pyproj import CRS
    z = CRS.from_epsg(epsg).utm_zone
    if not z:
        raise ValueError(f"EPSG:{epsg} is not a UTM zone")
    return int(z[:-1])


# ---------- listing ----------

def published_rows(src: str = f"{PUBLIC}/{INDEX}.parquet") -> dict[str, dict]:
    """The published index's rows, by tile_id (empty when there is none)."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    try:
        cur = con.execute(f"""SELECT * EXCLUDE (geometry, bbox, west, south, east, north),
            strftime(published AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS published_s,
            ST_AsText(geometry) AS wkt FROM read_parquet('{src}')""")
    except duckdb.Error as e:
        print(f"  no published index to reuse ({str(e)[:80]})", flush=True)
        return {}
    names = [d[0] for d in cur.description]
    out = {}
    for row in cur.fetchall():
        r = dict(zip(names, row))
        r["published"] = r.pop("published_s")
        for k in ("acquired_start", "acquired_end"):
            r[k] = r[k].isoformat() if r[k] else None
        out[r["tile_id"]] = r
    return out


def base_row(product: str, key: str, o: dict, project: str | None, thumb: str) -> dict:
    return {"tile_id": key.rsplit("/", 1)[-1][:-4], "product": product, "project": project,
            "published": utc(o["modified"]), "url": f"{TNM}/{key}", "s3_url": f"s3://prd-tnm/{key}",
            "thumbnail_url": thumb, "file_size": o["size"], "nodata": NODATA}


def rows_1m(old: dict[str, dict], limit: int | None = None
            ) -> tuple[list[dict], list[dict], list[str]]:
    """The 1 m tiles: (rows kept from the published index, rows whose header
    is to be read, files left out). limit: the first n projects (tests)."""
    projects = prefixes(TNM, ONE_M)[:limit]
    with cf.ThreadPoolExecutor(16) as ex:
        listings = list(ex.map(lambda p: list_bucket(TNM, p + "TIFF/"), projects))
        dates = dict(zip(projects, ex.map(lambda p: _try(acquisition, p), projects)))
    rows, odd, read = [], [], []
    for proj, objs in zip(projects, listings):
        project = proj[len(ONE_M):].rstrip("/")
        start, end = dates[proj] if isinstance(dates[proj], tuple) else (None, None)
        for o in objs:
            key = o["key"]
            if not key.lower().endswith(".tif"):
                continue
            name = key.rsplit("/", 1)[-1]
            r = base_row("1m", key, o, project, f"{TNM}/{proj}browse/{name[:-4]}.jpg")
            r |= {"acquired_start": start, "acquired_end": end}
            prev = old.get(r["tile_id"])
            if prev and prev["published"] == r["published"]:
                rows.append(prev | {k: r[k] for k in ("acquired_start", "acquired_end",
                                                       "thumbnail_url", "file_size")})
            elif m := NAME_1M.match(name):
                zone, x, y = int(m[1]), int(m[2]), int(m[3])
                x0, y1 = x * CELL_M - OVERLAP, y * CELL_M + OVERLAP
                side = CELL_M + 2 * OVERLAP
                read.append(r | grid(26900 + zone, 1.0, [side, side],
                                     [x0, y1 - side, x0 + side, y1]))
            elif NAME_1M_OLD.match(name):
                read.append(r)
            else:
                odd.append(key)
    undated = len({r["project"] for r in rows + read if not r["acquired_end"]})
    print(f"  1m: {len(rows) + len(read):,} tiles, {len(projects):,} projects ({undated} without "
          f"acquisition dates); {len(rows):,} rows reused, {len(read):,} headers to read",
          flush=True)
    return rows, read, odd


def read_headers(rows: list[dict], *, utm: bool = False,
                 nodata: float | None = NODATA) -> tuple[list[dict], list[str]]:
    """Each row with its header's grid, nodata and elevation range. A grid
    the row already has (from its name) is only compared: the header wins."""
    def one(r):
        try:
            hd = header(r["url"])
            if utm:
                utm_zone(hd["epsg"])
        except Exception as e:  # noqa: BLE001
            return None, f"{r['tile_id']}: {type(e).__name__}: {e}"
        g = grid(hd["epsg"], hd["pixel"], hd["size"], hd["extent"])
        if "native_bbox" in r:   # the name's grid: a cross-check, the header wins
            a, b = r["native_bbox"], g["native_bbox"]
            if r["epsg"] != g["epsg"] or r["width"] != g["width"] or any(
                    abs(a[k] - b[k]) > hd["pixel"] / 2 for k in a):
                differ.append(r["tile_id"])
        return r | g | {"nodata": hd["nodata"] if hd["nodata"] is not None else nodata,
                        "min_elev": hd["min_elev"], "max_elev": hd["max_elev"]}, None

    got, failed, differ = [], [], []
    with cf.ThreadPoolExecutor(48) as ex:
        for row, err in ex.map(one, rows):
            (got.append(row) if row else failed.append(err))
    if differ:
        print(f"  {len(differ)} tiles whose header grid is not the one their name gives "
              f"(the header's is used): {differ[:5]}", flush=True)
    if failed:
        kinds = collections.Counter(re.sub(r"\d+", "#", f.split(": ", 1)[1])[:90] for f in failed)
        print(f"  {len(failed)} headers unread: " + "; ".join(
            f"{n} x {k}" for k, n in kinds.most_common(6)) + f" (e.g. {failed[0][:120]})",
            flush=True)
    return got, failed


def rows_arcsec(product: str, old: dict[str, dict]) -> tuple[list[dict], list[dict]]:
    prefix, per_degree = ARCSEC[product]
    rx = re.compile(re.escape(prefix) + rf"([ns]\d{{2}}[ew]\d{{3}})/USGS_{product}_\1\.tif$")
    rows = []
    for o in list_bucket(TNM, prefix):
        m = rx.match(o["key"])
        if not m:
            continue
        cell = m[1]
        lat = int(cell[1:3]) * (1 if cell[0] == "n" else -1)
        lon = int(cell[4:7]) * (-1 if cell[3] == "w" else 1)
        px, pad = 1 / per_degree, OVERLAP / per_degree
        r = base_row(product, o["key"], o, None,
                     f"{TNM}/{prefix}{cell}/USGS_{product}_{cell}.jpg")
        prev = old.get(r["tile_id"])
        if prev and prev["published"] == r["published"]:
            rows.append(prev)
            continue
        rows.append(r | {"acquired_start": None, "acquired_end": None}
                    | grid(4269, px, [per_degree + 2 * OVERLAP] * 2,
                           [lon - pad, lat - 1 - pad, lon + 1 + pad, lat + pad]))
    keep = [r for r in rows if "wkt" in r]
    print(f"  {product}: {len(rows):,} tiles, {len(rows) - len(keep):,} headers to read",
          flush=True)
    return keep, [r for r in rows if "wkt" not in r]


def grid(epsg: int, pixel: float, size: list[int], extent: list[float]) -> dict:
    return {"crs": f"EPSG:{epsg}", "epsg": epsg, "pixel_size": pixel,
            "width": size[0], "height": size[1],
            "native_bbox": dict(zip(("xmin", "ymin", "xmax", "ymax"), extent))}


def _try(fn, *a):
    try:
        return fn(*a)
    except Exception as e:  # noqa: BLE001
        return e


# ---------- footprints and checks ----------

TRANSFORMERS: dict = {}


def lonlat_wkt(r: dict) -> str:
    """The footprint in lon/lat; projected edges densified to about 1 km. A
    geographic grid half a cell past the antimeridian or a pole (Copernicus
    cells centred on whole degrees) is cut at it."""
    b = r["native_bbox"]
    x0, y0, x1, y1 = b["xmin"], b["ymin"], b["xmax"], b["ymax"]
    if r["epsg"] in (4269, 4326):   # geographic: the grid, within the world's edges
        x0, x1 = max(x0, -180.0), min(x1, 180.0)
        y0, y1 = max(y0, -90.0), min(y1, 90.0)
        ring = [(x0, y0), (x1, y0), (x1, y1), (x0, y1)]
    else:
        from pyproj import Transformer
        n = max(1, round((x1 - x0) / 1000))
        pts = ([(x0 + (x1 - x0) * i / n, y0) for i in range(n)]
               + [(x1, y0 + (y1 - y0) * i / n) for i in range(n)]
               + [(x1 - (x1 - x0) * i / n, y1) for i in range(n)]
               + [(x0, y1 - (y1 - y0) * i / n) for i in range(n)])
        t = TRANSFORMERS.setdefault(r["epsg"], Transformer.from_crs(
            r["crs"], "OGC:CRS84", always_xy=True))
        ring = [t.transform(px, py) for px, py in pts]
    ring = [(round(x, 7), round(y, 7)) for x, y in ring]
    return "POLYGON((" + ", ".join(f"{x} {y}" for x, y in ring + ring[:1]) + "))"


# What the file itself says, no sidecar statistics.
NO_PAM = {**__import__("os").environ, "GDAL_PAM_ENABLED": "NO"}


def check_parser(rows: list[dict], n: int, products: tuple[str, ...] = ("1m", "13", "1")) -> None:
    """This module's header reader against gdalinfo, on n tiles per product:
    CRS, size, pixel size, extent, nodata and statistics must agree. Run
    before the bulk reads (gdalinfo processes started after an hour of
    48-thread reads crashed on a laptop, 2026-11-15)."""
    sample = []
    for product in products:
        group = [r for r in rows if r["product"] == product]
        sample += random.Random(len(group)).sample(group, min(n, len(group)))

    def one(r):
        for attempt in range(3):   # S3 throttles bursts
            out = subprocess.run(["gdalinfo", "-json", "-nomd", f"/vsicurl/{r['url']}"],
                                 capture_output=True, text=True, check=False, env=NO_PAM)
            if not out.returncode:
                break
            time.sleep(5 * (attempt + 1))
        else:
            return (f"{r['tile_id']}: gdalinfo exit {out.returncode}: "
                    f"{(out.stderr or out.stdout).strip()[-200:]}")
        d = json.loads(out.stdout)
        try:
            hd = header(r["url"])
        except Exception as e:  # noqa: BLE001
            return f"{r['tile_id']}: header: {type(e).__name__}: {e}"
        g, size, band = d["geoTransform"], d["size"], d["bands"][0]
        got = [g[0], g[3] + g[5] * size[1], g[0] + g[1] * size[0], g[3]]
        problems = []
        if size != hd["size"]:
            problems.append(f"size {size} != {hd['size']}")
        if abs(g[1] - hd["pixel"]) > 1e-12:
            problems.append(f"pixel {g[1]} != {hd['pixel']}")
        if any(abs(a - c) > abs(g[1]) / 100 for a, c in zip(got, hd["extent"])):
            problems.append(f"extent {got} != {hd['extent']}")
        epsg = d.get("stac", {}).get("proj:epsg")
        if epsg and epsg != hd["epsg"]:
            problems.append(f"EPSG:{epsg} != {hd['epsg']}")
        a, c = band.get("noDataValue"), hd["nodata"]
        if (a is None) != (c is None) or (a is not None and abs(a - c) > 1e-6 * abs(a)):
            problems.append(f"nodata {band.get('noDataValue')} != {hd['nodata']}")
        md = subprocess.run(["gdalinfo", "-json", f"/vsicurl/{r['url']}"], capture_output=True,
                            text=True, check=False, env=NO_PAM)
        if not md.returncode:
            m = json.loads(md.stdout)["bands"][0].get("metadata", {}).get("", {})
            want = m.get("STATISTICS_MINIMUM")
            if (float(want) if want else None) != hd["min_elev"]:
                problems.append(f"minimum {want} != {hd['min_elev']}")
        return f"{r['tile_id']}: " + "; ".join(problems) if problems else None

    with cf.ThreadPoolExecutor(4) as ex:
        problems = [p for p in ex.map(one, sample) if p]
    if problems:
        sys.exit("the header reader disagrees with gdalinfo:\n  " + "\n  ".join(problems))
    print(f"  {len(sample)} headers read as gdalinfo reads them", flush=True)


# ---------- writing ----------

def write(con, rows: list[dict], dest: Path) -> dict:
    """GeoParquet 2.0 sorted by product and tile_id, a bbox covering, row
    groups capped by bytes (gaul.py's rewrite keeps the order)."""
    proj4 = {e: proj4_of(e) for e in {r["epsg"] for r in rows}}
    tmp = dest.with_suffix(".rows.json")
    with tmp.open("w") as f:
        for r in rows:
            f.write(json.dumps({k: r.get(k) for k in COLUMNS}
                               | {"proj4": proj4[r["epsg"]], "wkt": r.get("wkt") or lonlat_wkt(r)})
                    + "\n")
    con.execute(f"""CREATE OR REPLACE TABLE t AS
        SELECT * EXCLUDE (wkt, published, acquired_start, acquired_end),
               acquired_start::DATE AS acquired_start, acquired_end::DATE AS acquired_end,
               published::TIMESTAMPTZ AS published, ST_GeomFromText(wkt) AS geometry
        FROM read_json('{tmp}', format = 'newline_delimited', columns = {{
            tile_id: 'VARCHAR', product: 'VARCHAR', project: 'VARCHAR',
            acquired_start: 'VARCHAR', acquired_end: 'VARCHAR', published: 'VARCHAR',
            url: 'VARCHAR', s3_url: 'VARCHAR', thumbnail_url: 'VARCHAR', file_size: 'BIGINT',
            crs: 'VARCHAR', epsg: 'INTEGER', proj4: 'VARCHAR', nodata: 'DOUBLE',
            pixel_size: 'DOUBLE', width: 'INTEGER', height: 'INTEGER',
            min_elev: 'DOUBLE', max_elev: 'DOUBLE', native_bbox: 'STRUCT(xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE)',
            wkt: 'VARCHAR'}})""")
    tmp.unlink()
    con.execute(f"""CREATE OR REPLACE TABLE t AS SELECT tile_id, product, project, acquired_start,
        acquired_end, published, url, s3_url, thumbnail_url, file_size, crs, epsg, proj4, nodata,
        pixel_size, width, height, min_elev, max_elev, native_bbox,
        ST_XMin(geometry) AS west, ST_YMin(geometry) AS south,
        ST_XMax(geometry) AS east, ST_YMax(geometry) AS north, geometry, {BBOX} FROM t""")
    n, x0, y0, x1, y1 = con.execute(
        "SELECT count(*), min(west), min(south), max(east), max(north) FROM t").fetchone()
    geo = json.dumps({"version": "2.0.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "geometry_types": ["Polygon"], "bbox": [x0, y0, x1, y1],
        "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}}}}})
    raw = dest.with_suffix(".raw.parquet")
    con.execute(f"""COPY (SELECT * FROM t ORDER BY product, tile_id) TO '{raw}'
        (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', COMPRESSION ZSTD,
         KV_METADATA {{geo: '{geo.replace("'", "''")}'}})""")
    rewrite(raw, dest)
    raw.unlink()
    verify(con, dest.parent, [dest.stem])
    counts = dict(con.execute("SELECT product, count(*) FROM t GROUP BY 1 ORDER BY 1").fetchall())
    return {"file": dest.name, "rows": n, "by_product": counts, "bytes": dest.stat().st_size,
            "sha256": hashlib.sha256(dest.read_bytes()).hexdigest(), "bbox": [x0, y0, x1, y1]}


PROPERTIES = ["tile_id", "product", "project", "acquired_end", "url", "epsg", "min_elev", "max_elev"]


def write_footprints(con, src: Path, dest: Path, where: str, zooms: tuple[int, int],
                     properties: list[str] = PROPERTIES) -> dict:
    """The footprints of some products as PMTiles, layer `tiles`, one
    feature per tile (overlapping tiles stay separate features)."""
    part = dest.with_suffix(".footprints.parquet")
    expr = {"acquired_end": "strftime(acquired_end, '%Y-%m-%d') AS acquired_end",
            "min_elev": "round(min_elev, 1) AS min_elev", "max_elev": "round(max_elev, 1) AS max_elev"}
    con.execute(f"""COPY (SELECT {", ".join(expr.get(c, c) for c in properties)}, geometry
        FROM read_parquet('{src}') WHERE {where} ORDER BY tile_id)
        TO '{part}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')""")
    out = pmtiles.tile(part, dest, layer="tiles", min_zoom=zooms[0], max_zoom=zooms[1],
                       properties=properties,
                       verbatim=True)
    part.unlink()
    return {"file": dest.name, "zooms": list(zooms),
            **{k: v for k, v in out.items() if k in ("bytes", "sha256")}}


def build(out: Path, check: int, projects: int | None = None) -> None:
    out.mkdir(parents=True, exist_ok=True)
    old = published_rows()
    keep, read, odd = rows_1m(old, projects)
    n_1m = len(keep) + len(read)
    for product in ("13", "1"):
        k, r = rows_arcsec(product, old)
        keep, read = keep + k, read + r
    if check and read:
        check_parser(read, check)
    got, failed = read_headers([r for r in read if r["product"] == "1m"], utm=True)
    got_arc, failed_arc = read_headers([r for r in read if r["product"] != "1m"])
    if failed_arc:
        sys.exit(f"arc-second headers unread: {failed_arc[:5]}")
    rows = keep + got + got_arc
    odd += failed
    if odd:
        print(f"  {len(odd)} 1m files left out (unknown name pattern, or header unread): "
              f"{odd[:5]}", flush=True)
        if len(odd) > 0.01 * n_1m:
            sys.exit("more than 1% of the 1m files left out")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    files = {INDEX: write(con, rows, out / f"{INDEX}.parquet")}
    print(f"  {INDEX}.parquet: {files[INDEX]['rows']:,} rows, "
          f"{files[INDEX]['bytes'] / 1e6:.1f} MB", flush=True)
    for name, where, zooms in (("3dep-1m-tiles", "product = '1m'", (6, 12)),
                               ("3dep-arcsec-tiles", "product <> '1m'", (0, 9))):
        files[name] = write_footprints(con, out / f"{INDEX}.parquet", out / f"{name}.pmtiles",
                                       where, zooms)
        print(f"  {name}.pmtiles: {files[name]['bytes'] / 1e6:.1f} MB", flush=True)
    (out / "index.json").write_text(json.dumps({
        "listed": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": f"{TNM}/StagedProducts/Elevation/", "left_out": odd, "files": files},
        indent=1) + "\n")


def publish(out: Path, remote: str) -> None:
    subprocess.run(["rclone", "copy", str(out), f"{remote}/index", "--filter", "+ *.parquet",
                    "--filter", "+ *.pmtiles", "--filter", "+ index.json", "--filter", "- *",
                    "--header-upload", "Cache-Control: public, max-age=3600"], check=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--out-dir", type=Path, required=True)
    b.add_argument("--check", type=int, default=25, help="headers checked against gdalinfo, per product")
    b.add_argument("--projects", type=int, help="only the first n 1 m projects (tests)")
    p = sub.add_parser("publish")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--remote", required=True)
    a = ap.parse_args()
    if a.cmd == "build":
        build(a.out_dir, a.check, a.projects)
    else:
        publish(a.out_dir, a.remote)


if __name__ == "__main__":
    main()
