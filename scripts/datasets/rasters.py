#!/usr/bin/env python3
"""
A state's thematic rasters as Cloud-Optimized GeoTIFFs: the engine
ca_rasters.py and nh_rasters.py share, the raster sibling of arcgis_layers.py.
Each of them is a Profile (its item CSV, themes, attribution and licence);
this reads each item from its source and writes one COG per item, grouped by
theme:

  <out>/<theme>/<item>.tif              the COG
  <out>/<theme>/<item>.classes.parquet  the value table, for class rasters
  <out>/<theme>/<item>.png              the catalog thumbnail
  <out>/<theme>/_manifest.json          sizes, sha256, grid, classes, sources

Two readers, the CSV's `reader` column says which:
- `zip`: the publisher's archive (CDFW's file library), downloaded, the
  GeoTIFF taken out with its sidecars (the value table GDAL reads from the
  .aux.xml or .vat.dbf). Identical copies of it count as one. With no
  GeoTIFF, or one GDAL cannot read in full (CDFW's, cut at 1 MiB), the raster
  in the archive's file geodatabase: copied cell for cell onto its own grid
  (gdalwarp, nearest, the same extent and size), its mask written as the
  nodata value. Fingerprint: the archive's Last-Modified and size.
- `export`: an ArcGIS ImageServer read with exportImage at its own pixel size
  and spatial reference, in blocks of at most maxImageWidth × maxImageHeight,
  then mosaicked. Each block's grid is checked against the service's: a
  resampled block fails the item. Fingerprint: the service's grid, pixel type
  and raster attribute table.

Writing: the source grid as it is, never reprojected (PLAN-rasters.md). One
COG per item: internal tiles of 512, ZSTD, overviews (nearest for classes,
average for continuous values), the source's nodata or the profile's. A
class raster gets the colour table its value table gives, embedded in the
COG, so GDAL, QGIS and web viewers draw it in the publisher's colours. The
value table itself goes to <item>.classes.parquet, unchanged but for the
colour columns.

Verifying: the COG's layout (GDAL's LAYOUT=COG, tiles, overviews), its grid,
CRS and nodata against the source's, and the pixels: GDAL's checksum of the
full-resolution band must equal the source's. For `export`, one block is read
again and compared with the COG's window.

GDAL is the command-line tools (gdalinfo, gdal_translate, gdalbuildvrt,
gdalwarp);
the workflows run them from the ghcr.io/osgeo/gdal image at the version the
vector builds use, so the bytes are the same from a laptop with that version.

Publishing: as arcgis_layers.py does, per item as soon as it is built (COG
and value table when their bytes changed, as scripts/filedates.py says, then
the thumbnail), then the manifests, _source.json and the file stamp. A run
stopped by its time budget keeps what it published; the next one builds
only the items whose fingerprint moved.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import datetime as dt
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

CHUNK = 1 << 20
COG_OPTIONS = ["-co", "COMPRESS=ZSTD", "-co", "LEVEL=9", "-co", "BLOCKSIZE=512",
               "-co", "BIGTIFF=IF_SAFER", "-co", "NUM_THREADS=ALL_CPUS",
               "-co", "STATISTICS=NO"]
# Colour columns of a value table, as ArcGIS and GDAL name them.
COLOUR_COLUMNS = (("red", "green", "blue"), ("r", "g", "b"))
# exportImage blocks: pixels a side, and requests at a time.
BLOCK = 4096
BLOCK_THREADS = 4


@dataclass(frozen=True)
class Profile:
    name: str                      # the file stamp's name: _files/<name>.json
    prefix: str                    # under the bucket: ca/rasters
    public: str                    # where the published files are read
    items_csv: Path
    themes: dict[str, str]         # theme id -> title
    attribution: str
    licence: str                   # SPDX id
    licence_url: str
    # Column of the value table holding the pixel value, per reader.
    value_column: str = "Value"
    # Nodata where the source leaves it unset: {item id or "*": value}.
    nodata: dict[str, float] = field(default_factory=dict)
    # Colour each value through this value-table column and a legend in the
    # archive ("name|r|g|b" lines, CDFW's legend.clr), when the table's own
    # colour columns are unset.
    # A tuple: the first of these columns the table has.
    colour_by: str | tuple[str, ...] | None = None
    legend_file: str = "legend.clr"
    # The legend when the archive has none: {name in the colour_by column:
    # (r, g, b)}.
    legend: dict[str, tuple[int, int, int]] = field(default_factory=dict)
    # Value table -> (value table, note or None): a column the profile adds
    # (CDFW's older models have no suitability rank), and what the catalog
    # says about it.
    rat_hook: Callable[[list[dict]], tuple[list[dict], str | None]] | None = None
    # The colour of a value the value table does not list (CDFW's older
    # models list only part of their values); None: black, as GDAL pads.
    unlisted_colour: tuple[int, int, int] | None = None
    # Every item is a class raster: one without a value table fails (CDFW's
    # values are keys, meaningless without it).
    require_classes: bool = False
    # Item id -> why it is broken upstream: still tried every run, its
    # failure printed but left out of failed.json (as arcgis_layers.py's).
    broken: dict[str, str] = field(default_factory=dict)
    # Value-table columns left out of the Parquet file (ArcGIS bookkeeping).
    drop_classes: tuple[str, ...] = ("OBJECTID", "OID", "Rowid", "ROWID")
    # What the catalog says (rasters_catalog.RasterCatalog).
    catalog: object = None


def rows(p: Profile) -> list[dict]:
    with p.items_csv.open(newline="") as f:
        out = list(csv.DictReader(f))
    ids = [r["id"] for r in out]
    if len(ids) != len(set(ids)):
        sys.exit(f"{p.items_csv.name}: duplicate ids")
    for r in out:
        if r["theme"] not in p.themes:
            sys.exit(f"{p.items_csv.name}: {r['id']}: unknown theme {r['theme']}")
        if r["reader"] not in ("zip", "export"):
            sys.exit(f"{p.items_csv.name}: {r['id']}: unknown reader {r['reader']}")
    return out


# --- HTTP -------------------------------------------------------------------

def request(url: str, method: str = "GET", headers: dict | None = None, timeout: float = 180):
    return urllib.request.urlopen(urllib.request.Request(
        url, method=method, headers=headers or {}), timeout=timeout)


def unreachable(items: list[dict]) -> dict[str, str]:
    """Host -> why, for each host that does not answer at all: one request to
    its first item, twice. An HTTP error is an answer (the host is up); a
    connection that times out or is refused is not. Its items then fail at
    once instead of each waiting out its own timeout (CDFW's file library
    from a GitHub runner, 2026-11-15: 517 timeouts, 2 h 26 min)."""
    first: dict[str, str] = {}
    for r in items:
        first.setdefault(urllib.parse.urlsplit(r["url"]).netloc, r["url"])
    down = {}
    for host, url in first.items():
        for i in range(2):
            try:
                with request(url, method="HEAD" if url.endswith(".zip") else "GET", timeout=30):
                    pass
                break
            except urllib.error.HTTPError:
                break
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                if i:
                    down[host] = str(getattr(e, "reason", e))
                else:
                    time.sleep(5)
    return down


def get_json(url: str, params: dict | None = None, attempts: int = 5) -> dict:
    q = url + ("?" + urllib.parse.urlencode({**(params or {}), "f": "json"}))
    for i in range(attempts):
        try:
            with request(q) as r:
                doc = json.load(r)
            if "error" in doc:
                raise RuntimeError(f"{url}: {doc['error']}")
            return doc
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as e:
            if i == attempts - 1:
                raise RuntimeError(f"{url}: {e}") from e
            time.sleep(2 ** i)
    raise AssertionError


def public_json(url: str, default):
    try:
        with request(url) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code in (403, 404):
            return default
        raise


def download(url: str, dest: Path, attempts: int = 6) -> None:
    """To dest, resumed with a Range request after a cut."""
    part = dest.with_suffix(dest.suffix + ".part")
    for i in range(attempts):
        have = part.stat().st_size if part.exists() else 0
        try:
            with request(url, headers={"Range": f"bytes={have}-"} if have else None) as r:
                if have and r.status != 206:
                    have = 0  # the server ignored the range: start over
                expect = r.headers.get("Content-Length")
                got = 0
                with part.open("ab" if have else "wb") as f:
                    while chunk := r.read(CHUNK):
                        f.write(chunk)
                        got += len(chunk)
                if expect is not None and got != int(expect):
                    raise ConnectionError(f"cut short: {got} of {expect} bytes")
            part.rename(dest)
            return
        except urllib.error.HTTPError as e:
            if e.code == 416:  # already complete
                part.rename(dest)
                return
            if i == attempts - 1 or e.code in (403, 404):
                raise RuntimeError(f"{url}: HTTP {e.code}") from e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if i == attempts - 1:
                raise RuntimeError(f"{url}: {e}") from e
        time.sleep(2 ** i)


# --- GDAL -------------------------------------------------------------------

def gdal(*args: str) -> str:
    env = {**os.environ, "GDAL_PAM_ENABLED": "NO"}
    r = subprocess.run(list(args), capture_output=True, text=True, env=env, check=False)
    if r.returncode:
        raise RuntimeError(f"{args[0]}: {r.stderr.strip()[-400:]}")
    return r.stdout


def info(path: Path, *flags: str) -> dict:
    # PAM stays on here: the source's value table lives in its .aux.xml.
    r = subprocess.run(["gdalinfo", "-json", *flags, str(path)], capture_output=True, text=True,
                       check=False)
    if r.returncode:
        raise RuntimeError(f"gdalinfo {path.name}: {r.stderr.strip()[-400:]}")
    return json.loads(r.stdout)


def checksum(path: Path) -> int:
    return info(path, "-checksum", "-nomd")["bands"][0]["checksum"]


# --- Fingerprints -----------------------------------------------------------

def fingerprint(r: dict) -> str:
    if r["reader"] == "zip":
        with request(r["url"], method="HEAD", timeout=60) as resp:
            parts = [resp.headers.get("Last-Modified") or "", resp.headers.get("Content-Length") or ""]
        if not any(parts):
            raise RuntimeError(f"{r['url']}: no Last-Modified or Content-Length")
        return "|".join(parts)
    svc = get_json(r["url"])
    keys = ("extent", "pixelSizeX", "pixelSizeY", "pixelType", "bandCount", "spatialReference",
            "noDataValue", "noDataValues")
    doc = {k: svc.get(k) for k in keys}
    if svc.get("hasRasterAttributeTable"):
        doc["rat"] = get_json(r["url"] + "/rasterAttributeTable").get("features")
    return hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()[:16]


# --- Readers ----------------------------------------------------------------

def read_zip(r: dict, work: Path, nodata: float | None = None) -> tuple[Path, list[dict] | None]:
    """The archive's GeoTIFF (outside any .gdb folder) and its sidecars, or
    the raster of its file geodatabase."""
    archive = work / f"{r['id']}.zip"
    download(r["url"], archive)
    stem = r["url"].rsplit("/", 1)[-1].rsplit(".", 1)[0].lower()
    dest = work / r["id"]
    with zipfile.ZipFile(archive) as z:
        names = z.namelist()
        tifs = [n for n in names if n.lower().endswith((".tif", ".tiff"))
                and ".gdb/" not in n.lower()]
        if r.get("member"):
            tifs = [n for n in tifs if n == r["member"]]
        elif len(tifs) > 1:
            # Several (CDFW's ds2009 also holds ds2008's): the one named
            # after the archive.
            tifs = [n for n in tifs if n.rsplit("/", 1)[-1].lower() == f"{stem}.tif"] or tifs
        if len(tifs) > 1 and len({(z.getinfo(n).CRC, z.getinfo(n).file_size) for n in tifs}) == 1:
            tifs = tifs[:1]  # the same file again (CDFW's tiff0/, tiff1/, tiff2/)
        if len(tifs) > 1:
            raise RuntimeError(f"{r['url']}: {len(tifs)} GeoTIFFs in the archive: {tifs[:5]}")
        for n in names:
            if n.rsplit("/", 1)[-1].lower() == "legend.clr":
                z.extract(n, dest)
        src = None
        if tifs:
            tif = tifs[0]
            # The GeoTIFF and its sidecars (.tfw, .aux.xml, .vat.dbf, .ovr...).
            for n in names:
                if n == tif or (n.startswith(tif + ".") and not n.endswith("/")) \
                        or n == tif.rsplit(".", 1)[0] + ".tfw":
                    z.extract(n, dest)
            src = dest / tif
            try:
                checksum(src)  # read in full: a GeoTIFF cut short fails here
            except RuntimeError as e:
                print(f"  {r['id']}: {tif} unreadable ({str(e)[-120:]}), trying the geodatabase",
                      flush=True)
                src = None
        gdbs = sorted({n[:n.lower().index(".gdb/") + 4] for n in names if ".gdb/" in n.lower()})
        gdbs = [g for g in gdbs if g.rsplit("/", 1)[-1].lower() == f"{stem}.gdb"] or gdbs
        if src is None and len(gdbs) == 1:
            for n in names:
                if n.startswith(gdbs[0] + "/") and not n.endswith("/"):
                    z.extract(n, dest)
    archive.unlink()
    if src is not None:
        return src, rat_of(info(src))
    if len(gdbs) != 1:
        raise RuntimeError(f"{r['url']}: no readable GeoTIFF, and {len(gdbs)} file geodatabases "
                           f"in the archive")
    return from_gdb(dest / gdbs[0], dest / "geodatabase.tif", nodata)


def from_gdb(gdb: Path, dest: Path, nodata: float | None) -> tuple[Path, list[dict] | None]:
    """A file geodatabase raster as a GeoTIFF, cell for cell: gdalwarp onto
    its own extent and size (no cell moves or is resampled), the mask (a
    geodatabase raster has no nodata value) written as the nodata. A 32-bit
    integer raster whose values (its value table's) and nodata fit in UInt16
    is written as UInt16, the same values: a GeoTIFF colour table needs Byte
    or UInt16 (CDFW's older models are Int32, values up to about 15,000)."""
    doc = info(gdb)
    band = doc["bands"][0]
    nd = band.get("noDataValue", nodata)
    if nd is None:
        raise RuntimeError(f"{gdb.name}: no nodata value to write its mask as")
    rat = rat_of(doc)
    ot = []
    vkey = next((k for k in (rat or [{}])[0] if k.lower() == "value"), None)
    if band["type"] in ("Int32", "UInt32") and vkey and float(nd).is_integer() \
            and 0 <= nd <= 65535 and all(0 <= int(row[vkey]) <= 65535 and row[vkey] != nd
                                         for row in rat):
        ot = ["-ot", "UInt16"]
    (x0, rx, _, y0, _, ry), (w, h) = doc["geoTransform"], doc["size"]
    exact = dest.with_name(dest.stem + ".exact.tif") if ot else dest
    gdal("gdalwarp", "-q", "-overwrite", "-r", "near",
         "-te", repr(x0), repr(y0 + ry * h), repr(x0 + rx * w), repr(y0),
         "-ts", str(w), str(h), "-dstnodata", str(int(nd)) if float(nd).is_integer() else repr(nd),
         "-co", "COMPRESS=ZSTD", "-co", "TILED=YES", "-co", "BIGTIFF=IF_SAFER",
         "-wo", "NUM_THREADS=ALL_CPUS", str(gdb), str(exact))
    out = info(exact, "-nomd")
    if out["size"] != doc["size"] or any(
            abs(a - b) > 1e-9 * max(1.0, abs(b)) for a, b in zip(out["geoTransform"], doc["geoTransform"])):
        raise RuntimeError(f"{gdb.name}: the copy is not on the geodatabase raster's grid")
    if ot:
        gdal("gdal_translate", "-q", *ot, "-co", "COMPRESS=ZSTD", "-co", "TILED=YES",
             str(exact), str(dest))
        if checksum(dest) != checksum(exact):
            raise RuntimeError(f"{gdb.name}: the UInt16 copy's values differ")
        exact.unlink()
    return dest, rat


def rat_of(doc: dict) -> list[dict] | None:
    rat = doc["bands"][0].get("rat")
    if not rat or not rat.get("row"):
        return None
    names = [c["name"] for c in rat["fieldDefn"]]
    return [dict(zip(names, row["f"])) for row in rat["row"]]


def read_export(r: dict, work: Path, nodata: float | None = None) -> tuple[Path, list[dict] | None]:
    """The service's raster at its own grid, block by block, as a VRT. With
    nodata, the server fills what is outside the raster with it (otherwise
    with 0, which can be a class)."""
    svc = get_json(r["url"])
    ext, sr = svc["extent"], svc.get("spatialReference") or svc["extent"]["spatialReference"]
    px, py = svc["pixelSizeX"], svc["pixelSizeY"]
    width = round((ext["xmax"] - ext["xmin"]) / px)
    height = round((ext["ymax"] - ext["ymin"]) / py)
    # Blocks of at most BLOCK pixels a side: NH GRANIT's server answers 500
    # to its own maximum (15,000 x 4,100, 123 MB uncompressed).
    bw = min(width, svc.get("maxImageWidth") or BLOCK, BLOCK)
    bh = min(height, svc.get("maxImageHeight") or BLOCK, BLOCK)
    folder = work / r["id"]
    folder.mkdir(parents=True, exist_ok=True)
    jobs = []
    for row0 in range(0, height, bh):
        for col0 in range(0, width, bw):
            w, h = min(bw, width - col0), min(bh, height - row0)
            jobs.append((col0, row0, w, h, folder / f"b_{row0:06d}_{col0:06d}.tif"))
    with cf.ThreadPoolExecutor(BLOCK_THREADS) as ex:
        for f in [ex.submit(fetch_block, r["url"], ext, sr, px, py, *j, nodata=nodata)
                  for j in jobs]:
            f.result()
    blocks = [j[-1] for j in jobs]
    vrt = folder / "mosaic.vrt"
    gdal("gdalbuildvrt", "-q", str(vrt), *map(str, blocks))
    rat = None
    if svc.get("hasRasterAttributeTable"):
        rat = [f["attributes"] for f in get_json(r["url"] + "/rasterAttributeTable")["features"]]
    return vrt, rat


def fetch_block(url: str, ext: dict, sr: dict, px: float, py: float, col0: int, row0: int,
                w: int, h: int, dest: Path, nodata: float | None = None) -> None:
    x0 = ext["xmin"] + col0 * px
    y1 = ext["ymax"] - row0 * py
    bbox = (x0, y1 - h * py, x0 + w * px, y1)
    srs = json.dumps({k: v for k, v in sr.items() if k in ("wkid", "latestWkid", "wkt")})
    params = {"bbox": ",".join(repr(v) for v in bbox), "bboxSR": srs, "imageSR": srs,
              "size": f"{w},{h}", "format": "tiff", "interpolation": "RSP_NearestNeighbor",
              "compression": "None", "f": "image"}
    if nodata is not None:
        params["noData"] = repr(nodata) if not float(nodata).is_integer() else str(int(nodata))
    q = url + "/exportImage?" + urllib.parse.urlencode(params)
    for i in range(6):
        try:
            download(q, dest, attempts=1)
            checksum(dest)  # reads every tile: a block cut short fails here
            break
        except RuntimeError:
            dest.unlink(missing_ok=True)
            dest.with_suffix(dest.suffix + ".part").unlink(missing_ok=True)
            if i == 5:
                raise
            time.sleep(2 ** i)
    gt = info(dest)["geoTransform"]
    want = [bbox[0], px, 0.0, bbox[3], 0.0, -py]
    if any(abs(a - b) > 1e-6 * px for a, b in zip(gt, want)) or info(dest)["size"] != [w, h]:
        raise RuntimeError(f"{url}: block at {col0},{row0} came back on another grid: {gt}")


# --- Writing ----------------------------------------------------------------

def colour_table(rat: list[dict] | None, value_column: str, *,
                 by: str | tuple[str, ...] | None = None,
                 legend: Path | dict | None = None) -> dict[int, tuple[int, int, int]]:
    """Value -> RGB: from the value table's colour columns (0-1 or 0-255)
    when they are set, otherwise through the `by` column and a legend (a
    file of "name|r|g|b" lines, or a dict)."""
    if not rat:
        return {}
    keys = {k.lower(): k for k in rat[0]}
    vkey = keys.get(value_column.lower())
    if not vkey:
        return {}
    cols = next((c for c in COLOUR_COLUMNS if all(k in keys for k in c)), None)
    if cols:
        raw = [[float(row[keys[c]] or 0) for c in cols] for row in rat]
        if any(v for rgb in raw for v in rgb):
            scale = 255 if all(v <= 1 for rgb in raw for v in rgb) else 1
            return {int(row[vkey]): tuple(round(v * scale) for v in rgb)
                    for row, rgb in zip(rat, raw)}
    if isinstance(by, tuple):
        by = next((b for b in by if b.lower() in keys), None)
    if by and legend and by.lower() in keys:
        names = {}
        if isinstance(legend, dict):
            names = {k.strip().lower(): tuple(v) for k, v in legend.items()}
        elif legend.is_file():
            for line in legend.read_text(errors="replace").splitlines():
                parts = line.strip().split("|")
                if len(parts) == 4:
                    names[parts[0].strip().lower()] = tuple(int(float(v)) for v in parts[1:])
        bkey = keys[by.lower()]
        out = {int(row[vkey]): names[str(row[bkey]).strip().lower()] for row in rat
               if str(row[bkey]).strip().lower() in names}
        if out:
            return out
    return {}


def s_type(path: Path) -> str:
    return info(path, "-nomd")["bands"][0]["type"]


def write_cog(src: Path, dest: Path, *, nodata: float | None, colours: dict, classes: bool,
              unlisted: tuple[int, int, int] | None = None) -> None:
    """src as a COG at dest, through a VRT that sets nodata and the colour
    table and drops the value table (it goes to Parquet)."""
    vrt = dest.with_suffix(".vrt")
    gdal("gdal_translate", "-q", "-of", "VRT", str(src), str(vrt))
    tree = ET.parse(vrt)
    band = tree.find("VRTRasterBand")
    for tag in ("GDALRasterAttributeTable", "ColorTable", "Histograms", "Metadata"):
        for el in band.findall(tag):
            band.remove(el)
    if nodata is not None:
        el = band.find("NoDataValue")
        if el is None:
            el = ET.SubElement(band, "NoDataValue")
        el.text = repr(nodata) if not float(nodata).is_integer() else str(int(nodata))
    if colours:
        interp = band.find("ColorInterp")
        if interp is None:
            interp = ET.SubElement(band, "ColorInterp")
        interp.text = "Palette"
        table = ET.SubElement(band, "ColorTable")
        # With an unlisted colour, every value the band can hold gets one.
        top = (65535 if s_type(src) == "UInt16" else 255) if unlisted else max(colours)
        for v in range(top + 1):
            r, g, b = colours.get(v, unlisted or (0, 0, 0))
            a = 0 if v not in colours or (nodata is not None and v == nodata) else 255
            ET.SubElement(table, "Entry", c1=str(r), c2=str(g), c3=str(b), c4=str(a))
    tree.write(vrt)
    resampling = "NEAREST" if classes else "AVERAGE"
    predictor = [] if classes else ["-co", "PREDICTOR=YES"]
    gdal("gdal_translate", "-q", "-of", "COG", *COG_OPTIONS, *predictor,
         "-co", f"OVERVIEW_RESAMPLING={resampling}", str(vrt), str(dest))
    vrt.unlink()


def verify(src: Path, cog: Path, nodata: float | None) -> dict:
    s, c = info(src, "-nomd"), info(cog)
    problems = []
    if c["metadata"].get("IMAGE_STRUCTURE", {}).get("LAYOUT") != "COG":
        problems.append("not a COG layout")
    if c["size"] != s["size"]:
        problems.append(f"size {c['size']} != {s['size']}")
    if any(abs(a - b) > 1e-9 * max(1.0, abs(b)) for a, b in zip(c["geoTransform"], s["geoTransform"])):
        problems.append("geotransform differs")
    # The same CRS can be written two ways (an ESRI-style definition in the
    # source, EPSG's in the COG): the EPSG codes decide when both have one.
    se, ce = s.get("stac", {}).get("proj:epsg"), c.get("stac", {}).get("proj:epsg")
    if (se != ce) if (se and ce) else c["coordinateSystem"]["wkt"] != s["coordinateSystem"]["wkt"]:
        problems.append(f"CRS differs ({se} != {ce})")
    band = c["bands"][0]
    if max(c["size"]) > 512 and not band.get("overviews"):
        problems.append("no overviews")
    if band["block"] != [512, 512] and max(c["size"]) > 512:
        problems.append(f"blocks {band['block']}")
    if nodata is not None and band.get("noDataValue") != nodata:
        problems.append(f"nodata {band.get('noDataValue')} != {nodata}")
    if not problems and checksum(cog) != checksum(src):
        problems.append("pixels differ from the source (checksum)")
    if problems:
        raise RuntimeError(f"{cog.name}: " + "; ".join(problems))
    return c


def write_classes(rat: list[dict], dest: Path, drop: tuple[str, ...]) -> list[dict]:
    import pyarrow as pa
    import pyarrow.parquet as pq
    keep = [k for k in rat[0] if k not in drop]
    table = pa.Table.from_pylist([{k: row[k] for k in keep} for row in rat])
    pq.write_table(table, dest, compression="zstd")
    return [{k: row[k] for k in keep} for row in rat]


def thumbnail(cog: Path, dest: Path, classes: bool) -> None:
    args = ["gdal_translate", "-q", "-of", "PNG", "-outsize", "512", "0", "-r",
            "nearest" if classes else "average"]
    if classes and info(cog)["bands"][0].get("colorTable"):
        args += ["-expand", "rgba"]
    else:
        args += ["-scale", "-ot", "Byte"]
    gdal(*args, str(cog), str(dest))
    for aux in dest.parent.glob(dest.name + ".aux.xml"):
        aux.unlink()


# --- One item ---------------------------------------------------------------

def build_item(p: Profile, r: dict, work: Path, out: Path) -> dict:
    t0 = time.monotonic()
    folder = out / r["theme"]
    folder.mkdir(parents=True, exist_ok=True)
    want = p.nodata.get(r["id"], p.nodata.get("*"))
    src, rat = (read_zip if r["reader"] == "zip" else read_export)(r, work, want)
    if rat is None and p.require_classes:
        shutil.rmtree(work / r["id"], ignore_errors=True)
        raise RuntimeError(f"{r['url']}: the raster has no value table")
    note = None
    if rat and p.rat_hook:
        rat, note = p.rat_hook(rat)
    ok = False
    try:
        s = info(src, "-nomd")
        band = s["bands"][0]
        nodata = band.get("noDataValue")
        if nodata is None:
            nodata = want
        classes = rat is not None
        legend = (next(iter((work / r["id"]).rglob(p.legend_file)), None) or p.legend or None) \
            if p.colour_by else None
        colours = colour_table(rat, p.value_column, by=p.colour_by, legend=legend) \
            if classes else {}
        cog = folder / f"{r['id']}.tif"
        write_cog(src, cog, nodata=nodata, colours=colours, classes=classes,
                  unlisted=p.unlisted_colour if note else None)
        c = verify(src, cog, nodata)
        if r["reader"] == "export":
            recheck_block(r, cog, work, nodata)
        table = None
        if classes:
            table = write_classes(rat, folder / f"{r['id']}.classes.parquet", p.drop_classes)
        thumbnail(cog, folder / f"{r['id']}.png", classes)
        ok = True
    finally:
        shutil.rmtree(work / r["id"], ignore_errors=True)
        if not ok:  # nothing half-written is left to publish
            for f in folder.glob(f"{r['id']}.*"):
                f.unlink()
    stac = c.get("stac", {})
    entry = {
        "id": r["id"], "theme": r["theme"], "title": r["title"],
        "file": f"{r['id']}.tif", "bytes": cog.stat().st_size,
        "sha256": sha256(cog), "data_type": c["bands"][0]["type"],
        "size": c["size"], "geo_transform": c["geoTransform"],
        "proj": {k: v for k, v in stac.items() if k.startswith("proj:")},
        "unit": unit_of(c["coordinateSystem"]["wkt"]),
        "wgs84_extent": c.get("wgs84Extent"),
        "nodata": nodata, "kind": "classes" if classes else "values",
        "overviews": [o["size"] for o in c["bands"][0].get("overviews", [])],
        "source": {k: r[k] for k in r if k not in ("theme", "title") and r[k]},
        **({"read_from": "geodatabase"} if src.name == "geodatabase.tif" else {}),
        **({"note": note} if note else {}),
        "built": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if table is not None:
        entry["classes"] = {"file": f"{r['id']}.classes.parquet", "rows": len(table),
                            "value_column": p.value_column,
                            "bytes": (folder / f"{r['id']}.classes.parquet").stat().st_size,
                            "sha256": sha256(folder / f"{r['id']}.classes.parquet")}
    (folder / f"{r['id']}.entry.json").write_text(json.dumps(entry, indent=1) + "\n")
    print(f"  {r['theme']}/{r['id']}: {c['size'][0]:,} x {c['size'][1]:,} {entry['data_type']}, "
          f"{entry['bytes'] / 1e6:.1f} MB, {len(table) if table else 0} classes, "
          f"{time.monotonic() - t0:.0f} s", flush=True)
    return entry


def unit_of(wkt: str) -> str | None:
    """The CRS's linear unit, as its WKT names it (the last LENGTHUNIT)."""
    import re
    names = re.findall(r'LENGTHUNIT\["([^"]+)"', wkt)
    return {"metre": "m", "meter": "m", "US survey foot": "US ft", "foot": "ft"}.get(
        names[-1], names[-1]) if names else None


def recheck_block(r: dict, cog: Path, work: Path, nodata: float | None) -> None:
    """One block of the service read again, against the COG's window."""
    svc = get_json(r["url"])
    ext, sr = svc["extent"], svc.get("spatialReference") or svc["extent"]["spatialReference"]
    px, py = svc["pixelSizeX"], svc["pixelSizeY"]
    width = round((ext["xmax"] - ext["xmin"]) / px)
    height = round((ext["ymax"] - ext["ymin"]) / py)
    w, h = min(512, width), min(512, height)
    col0, row0 = (width - w) // 2, (height - h) // 2
    folder = work / f"{r['id']}.check"
    folder.mkdir(parents=True, exist_ok=True)
    try:
        fresh = folder / "fresh.tif"
        fetch_block(r["url"], ext, sr, px, py, col0, row0, w, h, fresh, nodata=nodata)
        window = folder / "window.tif"
        gdal("gdal_translate", "-q", "-srcwin", str(col0), str(row0), str(w), str(h),
             str(cog), str(window))
        if checksum(fresh) != checksum(window):
            raise RuntimeError(f"{cog.name}: a block read again differs from the COG")
    finally:
        shutil.rmtree(folder, ignore_errors=True)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


# --- Publishing -------------------------------------------------------------

class Publisher:
    """Each item as soon as it is built: its COG and value table when their
    bytes changed, its thumbnail; then the manifests, index and
    _source.json, and the file stamp last."""

    def __init__(self, p: Profile, out: Path, remote: str):
        from filedates import as_rfc3339, fetch_json, now_rfc3339
        self.p, self.out, self.remote = p, out, remote.rstrip("/")
        self.stamp = fetch_json(f"{p.public}/_files/{p.name}.json")
        self.first = self.stamp is None
        self.now = now_rfc3339()
        self.since = as_rfc3339(dt.date.today().isoformat())
        self.cache = os.environ.get("CACHE", "Cache-Control: public, max-age=3600")

    def copy(self, rel: str, *headers: str) -> None:
        cmd = ["rclone", "copyto", str(self.out / rel), f"{self.remote}/{rel}", "--ignore-times",
               "--no-traverse", "--retries", "5"]
        for h in headers:
            cmd += ["--header-upload", h]
        subprocess.run(cmd, check=True)

    def item(self, theme: str, item_id: str) -> None:
        from filedates import stamp
        folder = self.out / theme
        files = [f for f in (folder / f"{item_id}.tif", folder / f"{item_id}.classes.parquet")
                 if f.is_file()]
        doc = stamp(self.out, self.p.name, self.stamp, self.since, self.now, only=files,
                    first=self.first)
        upload = doc.pop("upload")
        self.stamp = doc
        types = {".tif": "image/tiff; application=geotiff; profile=cloud-optimized",
                 ".parquet": "application/vnd.apache.parquet"}
        for rel in upload:
            self.copy(rel, self.cache, f"Content-Type: {types[Path(rel).suffix]}")
        if (folder / f"{item_id}.png").is_file():
            self.copy(f"{theme}/{item_id}.png", self.cache)
        print(f"    published {theme}/{item_id}: {len(upload)} data file(s) changed", flush=True)

    def meta(self, rels: list[str]) -> None:
        for rel in rels:
            if (self.out / rel).is_file():
                self.copy(rel, "Cache-Control: public, max-age=300")
        if self.stamp is not None:
            f = self.out / "_files" / f"{self.p.name}.json"
            f.parent.mkdir(exist_ok=True)
            f.write_text(json.dumps(self.stamp, indent=1) + "\n")
            self.copy(f"_files/{self.p.name}.json", "Cache-Control: public, max-age=300")


# --- A build ----------------------------------------------------------------

def build(p: Profile, work: Path, out: Path, *, only: list[str] | None, force: bool,
          max_minutes: float | None, workers: int, publish: str | None) -> int:
    t0 = time.monotonic()
    work.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    items = rows(p)
    if only:
        unknown = set(only) - {r["id"] for r in items}
        if unknown:
            sys.exit(f"not in {p.items_csv.name}: {sorted(unknown)}")
    published = public_json(f"{p.public}/_source.json", {})
    pool = cf.ThreadPoolExecutor(8)
    prints: dict[str, str] = {}
    failed: dict[str, str] = {}
    look = [r for r in items if not only or r["id"] in only]
    down = unreachable(look)
    for host, why in down.items():
        print(f"  UNREACHABLE {host}: {why}", flush=True)
    for r in look:
        if (host := urllib.parse.urlsplit(r["url"]).netloc) in down:
            failed[r["id"]] = f"unreachable: {host}: {down[host]}"
    look = [r for r in look if r["id"] not in failed]
    for r, fp in zip(look, pool.map(lambda r: _try(fingerprint, r), look)):
        if isinstance(fp, Exception):
            failed[r["id"]] = f"fingerprint: {fp}"
        else:
            prints[r["id"]] = fp
    pool.shutdown()
    todo = [r for r in items if r["id"] in prints and (
        force or (only and r["id"] in only)
        or (not only and published.get(r["id"], {}).get("fingerprint") != prints[r["id"]]))]
    for i, e in failed.items():
        if not e.startswith("unreachable: "):
            print(f"  FAIL {i}: {e}", flush=True)
    print(f"  {len(items)} items, {len(todo)} to build, {len(failed)} unreachable, "
          f"{workers} at a time", flush=True)
    publisher = Publisher(p, out, publish) if publish else None
    built: dict[str, dict] = {}
    deferred: list[str] = []
    with cf.ThreadPoolExecutor(max(1, workers)) as ex:
        running: dict = {}
        queue_ = list(todo)
        while queue_ or running:
            while queue_ and len(running) < max(1, workers):
                if max_minutes and time.monotonic() - t0 > max_minutes * 60:
                    deferred += [r["id"] for r in queue_]
                    queue_ = []
                    break
                r = queue_.pop(0)
                running[ex.submit(build_item, p, r, work, out)] = r
            if not running:
                break
            done, _ = cf.wait(running, return_when=cf.FIRST_COMPLETED)
            for f in done:
                r = running.pop(f)
                try:
                    e = f.result()
                except Exception as exc:  # noqa: BLE001
                    failed[r["id"]] = f"{type(exc).__name__}: {exc}"[:500]
                    print(f"  FAIL {r['theme']}/{r['id']}: {failed[r['id']]}", flush=True)
                    continue
                e["fingerprint"] = prints[r["id"]]
                if r["reader"] == "zip":  # "Last-Modified|size": the archive's date
                    e["source"]["last_modified"] = http_date(prints[r["id"]].split("|")[0])
                (out / r["theme"] / f"{r['id']}.entry.json").write_text(
                    json.dumps(e, indent=1) + "\n")
                built[r["id"]] = e
                if publisher:
                    publisher.item(r["theme"], r["id"])
    # Manifests: the published entries, with the items just built replaced.
    themes: dict[str, list[dict]] = {}
    old = {t: public_json(f"{p.public}/{t}/_manifest.json", {}).get("items", {})
           for t in {r["theme"] for r in items}}
    for r in items:
        e = built.get(r["id"]) or local_entry(out, r, prints.get(r["id"])) \
            or old.get(r["theme"], {}).get(r["id"])
        if e:
            themes.setdefault(r["theme"], []).append(e)
    rels = []
    for theme, entries in themes.items():
        doc = {"theme": theme, "title": p.themes[theme], "attribution": p.attribution,
               "licence": p.licence, "items": {e["id"]: e for e in entries}}
        (out / theme).mkdir(parents=True, exist_ok=True)
        (out / theme / "_manifest.json").write_text(json.dumps(doc, indent=1) + "\n")
        rels.append(f"{theme}/_manifest.json")
    index = {"name": p.name, "themes": {t: {"title": p.themes[t], "items": len(v)}
                                        for t, v in sorted(themes.items())}}
    (out / "index.json").write_text(json.dumps(index, indent=1) + "\n")
    source = {}
    for r in items:
        e = built.get(r["id"]) or local_entry(out, r, prints.get(r["id"]))
        if e:
            source[r["id"]] = {"fingerprint": e["fingerprint"]}
        elif r["id"] in published:
            source[r["id"]] = published[r["id"]]
    (out / "_source.json").write_text(json.dumps(source, indent=1, sort_keys=True) + "\n")
    known = {k: v for k, v in failed.items() if k in p.broken}
    (out / "failed.json").write_text(
        json.dumps({k: v for k, v in failed.items() if k not in known}, indent=1) + "\n")
    rels += ["index.json", "_source.json"]
    if publisher:
        publisher.meta(rels)
    if deferred:
        print(f"  time budget of {max_minutes:.0f} min reached: {len(deferred)} items left for "
              f"the next run", flush=True)
    if known:
        print(f"  {len(known)} items still broken upstream (Profile.broken, not in failed.json): "
              f"{', '.join(known)}", flush=True)
    if fixed := sorted(k for k in p.broken if k in built):
        print(f"  built again, to take out of Profile.broken: {', '.join(fixed)}", flush=True)
    new = [k for k in failed if k not in known]
    if new:
        print(f"  {len(new)} items failed (failed.json): {', '.join(new)}", flush=True)
    print(f"  {len(built)} built in {time.monotonic() - t0:.0f} s", flush=True)
    return 1 if new else 0


def local_entry(out: Path, r: dict, fingerprint: str | None) -> dict | None:
    """An item built here by an earlier run (a local build in steps): kept
    when this run did not look at its source (--item), or when it is still
    the source's current state."""
    f = out / r["theme"] / f"{r['id']}.entry.json"
    if not f.is_file():
        return None
    e = json.loads(f.read_text())
    if "fingerprint" not in e or (fingerprint and e["fingerprint"] != fingerprint):
        return None
    return e


def http_date(s: str) -> str | None:
    from email.utils import parsedate_to_datetime
    try:
        return parsedate_to_datetime(s).astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError):
        return None


def _try(fn, *a):
    try:
        return fn(*a)
    except Exception as e:  # noqa: BLE001
        return e


def main(p: Profile, doc: str) -> None:
    ap = argparse.ArgumentParser(description=doc, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="read, write, verify and (with --publish) upload the items")
    b.add_argument("--work-dir", type=Path, required=True)
    b.add_argument("--out-dir", type=Path, required=True)
    b.add_argument("--item", action="append", help="only this item (repeatable)")
    b.add_argument("--force", action="store_true", help="rebuild every item")
    b.add_argument("--max-minutes", type=float)
    b.add_argument("--workers", type=int, default=2)
    b.add_argument("--publish", metavar="REMOTE", help="rclone remote, e.g. parquetry:parquetry/ca/rasters")
    c = sub.add_parser("catalog", help="render the Portolan catalog (from --out-dir or what is live)")
    c.add_argument("--out-dir", type=Path, help="build's --out-dir (default: the published files)")
    c.add_argument("--dest", type=Path, help="write the tree here and check it")
    c.add_argument("--stamp", help="the file stamp (default: the published one)")
    c.add_argument("--publish", metavar="REMOTE", help="check, then upload under REMOTE/catalog/")
    c.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if args.cmd == "build":
        sys.exit(build(p, args.work_dir, args.out_dir, only=args.item, force=args.force,
                       max_minutes=args.max_minutes, workers=args.workers, publish=args.publish))
    if args.cmd == "catalog":
        import rasters_catalog as rc
        if args.publish:
            rc.publish(p, p.catalog, args.out_dir, args.publish, dry_run=args.dry_run)
            return
        if not args.dest:
            sys.exit("catalog: --dest or --publish")
        n = rc.build(p, p.catalog, args.out_dir, args.dest, stamp=args.stamp)
        print(f"wrote {args.dest}/ ({n:,} collections)")
        sys.exit(rc.check(args.dest))
