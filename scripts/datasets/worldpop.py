#!/usr/bin/env python3
"""
WorldPop Global2 population, 2025, on H3 cells: per country, a world file
and one PMTiles archive.

WorldPop's Global2 release R2025A estimates the population of every 1 km
pixel of the land, constrained to built-up areas (people are placed only
where there are buildings), with 20 age bands per sex. This counts it on H3
cells and writes:

  <out>/R2025A/2025/country=<ISO3>/worldpop_2025.parquet
        H3 resolution 7 (about 5 km2), the cells of one country
  <out>/R2025A/2025/worldpop_2025_world_r5.parquet
        H3 resolution 5 (about 250 km2), the whole world
  <out>/R2025A/2025/worldpop_2025.pmtiles
        H3 resolutions 3 to 7 in zoom bands, for web maps
  <out>/R2025A/2025/_manifest.json, ATTRIBUTION.txt

Columns: `h3` (the cell, as text), `country` (GAUL 2024 `iso3_code`),
`population` (WorldPop's total), `pop_0_14`, `pop_15_64`, `pop_65_plus`,
`pop_80_plus` (sums of WorldPop's age bands, both sexes), `female`, `male`,
`area_km2` (the cell's area), `lon` and `lat` (the cell centre), then
`bbox` and the hexagon as `geometry`.

How a pixel is counted: each 1 km pixel goes whole to the resolution 7 cell
holding its centre. A cell is about 5 pixels, so the counts are WorldPop's
own, moved by at most half a cell, and every total is conserved: the build
fails unless each band sums, over all cells, to the raster's own sum.
Resolution 8 would be finer than the source and is not offered.

The country of a cell is the GAUL 2024 L0 unit (our derived layer) holding
its centre, the same borders as gaul/ and clc/. GAUL keeps disputed areas
apart under their own codes (xJK Jammu and Kashmir, xAC Aksai Chin, ...),
and they get their own folders. A populated cell whose centre GAUL leaves in
the sea (coasts, small islands GAUL simplifies away) goes to the nearest
GAUL unit. The world file
counts every person once: a resolution 5 cell across a border is one row,
with the country holding most of its people.

The age bands come from separate rasters (t_00 ... t_90, both sexes; f_ and
m_ per sex) and need not add up to the total exactly: the manifest records
both sums per country.

WorldPop's server ignores Range requests, so each raster (about 280 MB) is
downloaded whole, PARALLEL at a time (each connection is slow), counted, and
deleted. A counted raster leaves a small file of cell sums behind, so a
rerun picks up where it stopped.

Usage:
  python3 scripts/datasets/worldpop.py upstream
  uv run --project scripts --with rasterio python scripts/datasets/worldpop.py build \\
      --work-dir work --out-dir out/worldpop [--gaul GAUL_2024_L0_derived.parquet]
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles
from datasets.gaul import BBOX_STRUCT, geo_metadata, rewrite, verify

RELEASE = "R2025A"
VERSION = "v1"
YEAR = 2025
STEM = f"worldpop_{YEAR}"
RES = 7               # the country files
WORLD_RES = 5         # the world file
PARALLEL = 8          # downloads at a time: WorldPop serves ~270 KB/s a connection
SITE = "https://data.worldpop.org/GIS"
POP_DIR = (f"{SITE}/Population/Global_2015_2030/{RELEASE}/{YEAR}/0_Mosaicked/{VERSION}/"
           "1km/constrained")
AGESEX_DIR = (f"{SITE}/AgeSex_structures/Global_2015_2030/{RELEASE}/{YEAR}/0_Mosaicked/"
              f"{VERSION}/1km/constrained")
RELEASES_DIR = f"{SITE}/Population/Global_2015_2030/"
PAGE = "https://hub.worldpop.org/project/categories?id=3"
DOI = "https://doi.org/10.5258/SOTON/WP00846"
CITATION = ("Bondarenko M., Priyatikanto R., Tejedor-Garavito N., Zhang W., McKeen T., "
            "Cunningham A., Woods T., Hilton J., Cihan D., Nosatiuk B., Brinkhoff T., "
            "Tatem A., Sorichetta A. 2025. Constrained estimates of 2015-2030 total number "
            "of people per grid square at a resolution of 30 arc (approximately 1km at the "
            "equator) R2025A version v1. Global Demographic Data Project - Funded by The "
            "Bill and Melinda Gates Foundation (INV-045237). WorldPop - School of Geography "
            "and Environmental Science, University of Southampton. DOI:10.5258/SOTON/WP00846")
PUBLIC = "https://parquetry.geomermaids.com"
GAUL_L0 = f"{PUBLIC}/gaul/2024/GAUL_2024_L0_derived.parquet"

AGES = ["00", "01", "05", "10", "15", "20", "25", "30", "35", "40", "45", "50", "55", "60",
        "65", "70", "75", "80", "85", "90"]
# Band -> raster file name. `pop` is the total; t_ both sexes, f_ and m_ per sex.
BANDS = {"pop": f"global_pop_{YEAR}_CN_1km_{RELEASE}_{VERSION}.tif",
         **{f"{s}_{a}": f"global_{s}_{a}_{YEAR}_CN_1km_{RELEASE}_{VERSION}.tif"
            for s in "tfm" for a in AGES}}
# Output column -> the bands it sums. t_00 is under 1, t_01 is 1 to 4, then
# five-year bands up to t_90, 90 and over.
COLUMNS = {
    "population": ["pop"],
    "pop_0_14": ["t_00", "t_01", "t_05", "t_10"],
    "pop_15_64": [f"t_{a}" for a in AGES[4:14]],
    "pop_65_plus": [f"t_{a}" for a in AGES[14:]],
    "pop_80_plus": ["t_80", "t_85", "t_90"],
    "female": [f"f_{a}" for a in AGES],
    "male": [f"m_{a}" for a in AGES],
}
DESCRIPTIONS = {
    "h3": f"H3 cell index (resolution {RES} in the country files), as text.",
    "country": "GAUL 2024 L0 iso3_code of the unit holding the cell centre (x codes are "
               "disputed areas GAUL keeps apart). In the world file: the country holding "
               "most of the cell's people.",
    "population": "People in the cell, WorldPop's total estimate (sum of the 1 km pixels "
                  "whose centre is in the cell).",
    "pop_0_14": "People aged 0 to 14 (WorldPop bands t_00, t_01, t_05, t_10).",
    "pop_15_64": "People aged 15 to 64 (bands t_15 to t_60).",
    "pop_65_plus": "People aged 65 and over (bands t_65 to t_90).",
    "pop_80_plus": "People aged 80 and over (bands t_80, t_85, t_90).",
    "female": "Women and girls, all ages (bands f_00 to f_90).",
    "male": "Men and boys, all ages (bands m_00 to m_90).",
    "area_km2": "Area of the H3 cell, km2 (density = population / area_km2).",
    "lon": "Longitude of the cell centre (H3), degrees. Use it, not ST_Centroid(geometry), for "
           "distances: a cell cut at the antimeridian has its centroid far away.",
    "lat": "Latitude of the cell centre (H3), degrees.",
    "bbox": "Bounding box of the cell, the GeoParquet covering (FLOAT, rounded outwards).",
    "geometry": "The H3 cell as a polygon, lon/lat (OGC:CRS84); cells across the "
                "antimeridian are cut in two.",
}
# PMTiles: (min zoom, max zoom, H3 resolution), as clc_tiles.py bands its cells.
TILE_BANDS = [(0, 2, 3), (3, 3, 4), (4, 5, 5), (6, 6, 6), (7, 9, 7)]
# Degrees within which a cell GAUL leaves in the sea looks for its country.
# 5: GAUL 2024 lacks the Azores islands of Sao Miguel, Santa Maria and Flores
# (2 to 3 degrees from Terceira) and atolls of the Marshall Islands.
NEAR = [0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0]
TILE_LAYER = "population"
TILE_PROPERTIES = ["h3", "population", "density", "pop_65_plus", "pop_0_14"]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def request(url: str, method: str = "GET") -> tuple[dict, bytes]:
    """WorldPop's server answers 503 now and then under load: retry with
    backoff before failing."""
    for attempt in range(6):
        try:
            req = urllib.request.Request(url, method=method)
            with urllib.request.urlopen(req, timeout=60) as r:
                return dict(r.headers), r.read() if method == "GET" else b""
        except urllib.error.HTTPError as exc:
            if exc.code < 500 or attempt == 5:
                raise
        except urllib.error.URLError:
            if attempt == 5:
                raise
        time.sleep(5 * 2 ** attempt)
    raise AssertionError("unreachable")


def head(url: str) -> dict:
    h, _ = request(url, "HEAD")
    return {"url": url, "bytes": int(h["Content-Length"]),
            "last_modified": h.get("Last-Modified"), "etag": h.get("ETag")}


def releases() -> list[str]:
    """The Global2 releases WorldPop lists (R2025A, then R2026A...)."""
    _, body = request(RELEASES_DIR)
    page = body.decode("utf-8", "replace")
    return sorted(set(re.findall(r'href="(R20\d\d[A-Z])/"', page)))


def upstream() -> dict:
    """The total raster's headers, and any release newer than ours."""
    out = {"release": RELEASE, "version": VERSION, "year": YEAR,
           "total": head(f"{POP_DIR}/{BANDS['pop']}"), "releases": releases()}
    out["newer"] = [r for r in out["releases"] if r > RELEASE]
    return out


def fetch(url: str, dest: Path) -> Path:
    """Whole-file download (no Range support), size checked."""
    part = dest.with_suffix(dest.suffix + ".part")
    expected = head(url)["bytes"]
    subprocess.run(["curl", "-fsS", "--retry", "5", "--retry-all-errors", "-o", str(part), url],
                   check=True)
    if part.stat().st_size != expected:
        sys.exit(f"{dest.name}: {part.stat().st_size:,} bytes, expected {expected:,}")
    part.rename(dest)
    return dest


def count_band(con, tif: Path, dest: Path) -> dict:
    """Sum the band's pixels per resolution RES cell, by pixel centre.
    Read in strips; only pixels with people are passed on."""
    import numpy as np
    import pyarrow as pa
    import rasterio

    with rasterio.open(tif) as r:
        if r.crs.to_epsg() != 4326:
            sys.exit(f"{tif.name}: CRS {r.crs}, expected EPSG:4326")
        t, nodata = r.transform, r.nodata
        if t.b or t.d:
            sys.exit(f"{tif.name}: rotated geotransform")
        con.execute("CREATE OR REPLACE TEMP TABLE part (cell UBIGINT, v DOUBLE)")
        total, pixels, negative = 0.0, 0, 0
        step = max(r.block_shapes[0][0], 256)
        step = (1024 // step) * step or step
        cols = np.arange(r.width)
        lon = t.c + (cols + 0.5) * t.a
        for row0 in range(0, r.height, step):
            h = min(step, r.height - row0)
            a = r.read(1, window=((row0, row0 + h), (0, r.width)))
            valid = np.isfinite(a)
            if nodata is not None:
                valid &= a != nodata
            negative += int((valid & (a < 0)).sum())
            valid &= a > 0
            rows, ci = np.nonzero(valid)
            if not rows.size:
                continue
            v = a[rows, ci].astype(np.float64)
            total += float(v.sum())
            pixels += int(rows.size)
            lat = t.f + (row0 + rows + 0.5) * t.e
            strip = pa.table({"lat": lat, "lon": lon[ci], "v": v})
            con.register("strip", strip)
            con.execute(f"INSERT INTO part SELECT h3_latlng_to_cell(lat, lon, {RES}), sum(v) "
                        "FROM strip GROUP BY 1")
            con.unregister("strip")
    if negative:
        sys.exit(f"{tif.name}: {negative:,} negative pixels")
    con.execute(f"COPY (SELECT cell, sum(v) AS v FROM part GROUP BY cell) TO '{dest}' "
                "(FORMAT parquet, COMPRESSION zstd)")
    con.execute("DROP TABLE part")
    return {"file": tif.name, "sum": total, "pixels": pixels}


def count_all(con, work: Path, keep: bool = False) -> dict:
    """Every band: download (PARALLEL at a time), count, delete. A band
    whose cell sums are already in work/bands/ is skipped."""
    bands_dir, src = work / "bands", work / "src"
    bands_dir.mkdir(parents=True, exist_ok=True)
    src.mkdir(parents=True, exist_ok=True)
    done_path = work / "bands.json"
    done = json.loads(done_path.read_text()) if done_path.exists() else {}
    todo = [b for b in BANDS if b not in done or not (bands_dir / f"{b}.parquet").exists()]

    def url(band: str) -> str:
        return f"{POP_DIR if band == 'pop' else AGESEX_DIR}/{BANDS[band]}"

    def get(band: str) -> Path:
        dest = src / BANDS[band]
        return dest if dest.exists() else fetch(url(band), dest)

    t0 = time.monotonic()
    with cf.ThreadPoolExecutor(PARALLEL) as pool:
        futures = {pool.submit(get, b): b for b in todo}
        for fut in cf.as_completed(futures):
            band = futures[fut]
            tif = fut.result()
            done[band] = {**count_band(con, tif, bands_dir / f"{band}.parquet"),
                          "url": url(band)}
            done_path.write_text(json.dumps(done, indent=2) + "\n")
            if not keep:
                tif.unlink()
            print(f"  {band}: {done[band]['pixels']:,} pixels, {done[band]['sum']:,.0f} people, "
                  f"{len(done)}/{len(BANDS)}, {time.monotonic() - t0:.0f} s", flush=True)
    return done


def cell_table(con, work: Path, bands: dict) -> None:
    """`cells`: one row per populated resolution RES cell, the output columns.
    Fails unless every band's cell sums add up to the raster's own sum."""
    files = ", ".join(f"'{work / 'bands' / f'{b}.parquet'}'" for b in BANDS)
    sums = ",\n".join(
        f"coalesce(sum(v) FILTER (WHERE band IN ({', '.join(repr(b) for b in bs)})), 0) AS {c}"
        for c, bs in COLUMNS.items())
    con.execute(f"""
        CREATE OR REPLACE TABLE cells AS
        SELECT cell, {sums}
        FROM (SELECT cell, v, regexp_extract(filename, '([a-z]+(_[0-9]+)?)\\.parquet$', 1) AS band
              FROM read_parquet([{files}], filename = true))
        GROUP BY cell""")
    for band in BANDS:
        got = con.execute(f"SELECT sum(v) FROM read_parquet('{work / 'bands' / band}.parquet')"
                          ).fetchone()[0]
        want = bands[band]["sum"]
        if abs(got - want) > 1e-9 * max(want, 1):
            sys.exit(f"{band}: cells sum to {got!r}, raster to {want!r}")
    for column, bs in COLUMNS.items():
        got = con.execute(f"SELECT sum({column}) FROM cells").fetchone()[0]
        want = sum(bands[b]["sum"] for b in bs)
        if abs(got - want) > 1e-9 * max(want, 1):
            sys.exit(f"{column}: cells sum to {got!r}, bands to {want!r}")


def countries(con, gaul: Path) -> dict:
    """`cell_country`: the GAUL L0 unit holding each populated cell's
    centre. GAUL is cut into pieces of at most clc.MAX_POINTS vertices first
    (clc.quadtree), so the point-in-polygon join stays cheap. A centre in
    two units (a shared edge) keeps the first code in order. A centre GAUL
    leaves in the sea goes to the nearest piece, searched within growing
    distances (NEAR, degrees)."""
    from datasets.clc import quadtree

    con.execute(f"""CREATE OR REPLACE TABLE piece AS
                    SELECT iso3_code AS country, unnest(ST_Dump(geometry::GEOMETRY)).geom AS geom
                    FROM read_parquet('{gaul}')""")
    levels = quadtree(con, "piece", "country")
    pieces = con.execute("SELECT count(*) FROM piece").fetchone()[0]
    con.execute("""CREATE OR REPLACE TABLE pt AS
                   SELECT cell, ST_Point(h3_cell_to_lng(cell), h3_cell_to_lat(cell)) AS geom
                   FROM cells""")
    con.execute("""CREATE OR REPLACE TABLE cell_country AS
                   SELECT pt.cell, min(piece.country) AS country, count(DISTINCT piece.country) AS n,
                          0.0::DOUBLE AS near
                   FROM pt JOIN piece ON ST_Intersects(piece.geom, pt.geom)
                   GROUP BY pt.cell""")
    shared_edge = con.execute("SELECT count(*) FROM cell_country WHERE n > 1").fetchone()[0]
    for d in NEAR:
        left = con.execute("SELECT count(*) FROM pt ANTI JOIN cell_country USING (cell)"
                           ).fetchone()[0]
        if not left:
            break
        con.execute(f"""
            INSERT INTO cell_country
            SELECT cell, arg_min(country, dist), 1, {d} FROM (
                SELECT pt.cell, piece.country, ST_Distance(piece.geom, pt.geom) AS dist
                FROM (SELECT * FROM pt ANTI JOIN cell_country USING (cell)) pt
                JOIN piece ON ST_DWithin(piece.geom, pt.geom, {d}))
            GROUP BY cell""")
    left = con.execute("SELECT count(*) FROM pt ANTI JOIN cell_country USING (cell)").fetchone()[0]
    if left:
        sys.exit(f"{left:,} populated cells more than {NEAR[-1]} degrees from any GAUL unit")
    near = dict(con.execute("SELECT near, count(*) FROM cell_country GROUP BY 1 ORDER BY 1"
                            ).fetchall())
    return {"gaul_pieces": pieces, "quadtree_levels": levels, "shared_edge_cells": shared_edge,
            "cells_by_search_degrees": {str(k): v for k, v in near.items()}}


def country_names(con, gaul: Path) -> dict[str, str]:
    """ISO3 -> a name: ISO 3166's common name, else GAUL's unit names (the
    disputed x codes)."""
    import pycountry

    out = {}
    for code, names in con.execute(f"""SELECT iso3_code, list(DISTINCT gaul0_name ORDER BY gaul0_name)
                                       FROM read_parquet('{gaul}') GROUP BY 1""").fetchall():
        c = pycountry.countries.get(alpha_3=code)
        out[code] = (getattr(c, "common_name", None) or c.name) if c else ", ".join(names)
    return out


def antimeridian(con, table: str) -> int:
    """Cut the hexagons that cross the antimeridian in two: H3 gives them
    vertices on both sides (179.99, -179.99), which drawn as is span the
    globe."""
    import shapely
    from shapely.geometry import MultiPolygon, Polygon, box
    from shapely.affinity import translate

    rows = con.execute(f"""SELECT h3, ST_AsWKB(geometry) FROM {table}
                           WHERE ST_XMax(geometry) - ST_XMin(geometry) > 180""").fetchall()
    fixed = []
    for h3, wkb in rows:
        ring = shapely.from_wkb(bytes(wkb)).exterior.coords
        shifted = Polygon([(x + 360 if x < 0 else x, y) for x, y in ring])
        east = shifted.intersection(box(0, -90, 180, 90))
        west = translate(shifted.intersection(box(180, -90, 540, 90)), xoff=-360)
        parts = [g for g in (east, west) if not g.is_empty]
        fixed.append((h3, shapely.to_wkb(MultiPolygon(parts) if len(parts) > 1 else parts[0])))
    if fixed:
        con.execute("CREATE OR REPLACE TEMP TABLE am (h3 VARCHAR, wkb BLOB)")
        con.executemany("INSERT INTO am VALUES (?, ?)", fixed)
        con.execute(f"""UPDATE {table} SET geometry = ST_GeomFromWKB(am.wkb)
                        FROM am WHERE {table}.h3 = am.h3""")
    return len(fixed)


def write_cells(con, query: str, dest: Path) -> dict:
    """One GeoParquet 2.0 file: bbox covering, Hilbert order, row groups
    capped by bytes (gaul.rewrite), checked by gaul.verify."""
    con.execute(f"CREATE OR REPLACE TEMP TABLE w AS {query}")
    n_am = antimeridian(con, "w")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE w AS
                    SELECT * EXCLUDE (geometry), {BBOX_STRUCT.replace('geom)', 'geometry)')},
                           geometry FROM w""")
    n, types, xmin, ymin, xmax, ymax = con.execute("""
        SELECT count(*), list(DISTINCT ST_GeometryType(geometry)::VARCHAR),
               min(ST_XMin(geometry)), min(ST_YMin(geometry)),
               max(ST_XMax(geometry)), max(ST_YMax(geometry)) FROM w""").fetchone()
    names = {"POLYGON": "Polygon", "MULTIPOLYGON": "MultiPolygon"}
    geo = geo_metadata((xmin, ymin, xmax, ymax), [names[t] for t in types])
    dest.parent.mkdir(parents=True, exist_ok=True)
    raw = dest.with_suffix(".raw.parquet")
    con.execute(f"""
        COPY (SELECT * FROM w ORDER BY ST_Hilbert(geometry,
                  ST_Extent(ST_MakeEnvelope({xmin!r}, {ymin!r}, {xmax!r}, {ymax!r}))))
        TO '{raw}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', COMPRESSION ZSTD,
                    KV_METADATA {{geo: '{geo}'}})""")
    rewrite(raw, dest)
    raw.unlink()
    verify(con, dest.parent, [dest.stem])
    sums = con.execute(f"SELECT {', '.join(f'sum({c})' for c in COLUMNS)} FROM w").fetchone()
    return {"file": dest.name, "features": n, "bytes": dest.stat().st_size,
            "sha256": sha256(dest), "antimeridian_cells": n_am,
            "bbox": [xmin, ymin, xmax, ymax], **dict(zip(COLUMNS, sums))}


def cell_columns(cell: str = "cell") -> str:
    return (f"h3_h3_to_string({cell}) AS h3, country, "
            + ", ".join(COLUMNS) + f", h3_cell_area({cell}, 'km^2') AS area_km2, "
            f"h3_cell_to_lng({cell}) AS lon, h3_cell_to_lat({cell}) AS lat, "
            f"ST_GeomFromWKB(h3_cell_to_boundary_wkb({cell})) AS geometry")


def tiles(con, work: Path, dest: Path) -> dict:
    """One archive, a resolution per zoom band (TILE_BANDS)."""
    archives = []
    for lo, hi, res in TILE_BANDS:
        src = work / f"tiles_r{res}.parquet"
        parent = "cell" if res == RES else f"h3_cell_to_parent(cell, {res})"
        con.execute(f"""CREATE OR REPLACE TEMP TABLE t AS
            SELECT h3_h3_to_string(p) AS h3, round(population, 1) AS population,
                   round(population / h3_cell_area(p, 'km^2'), 1) AS density,
                   round(pop_65_plus, 1) AS pop_65_plus, round(pop_0_14, 1) AS pop_0_14,
                   ST_GeomFromWKB(h3_cell_to_boundary_wkb(p)) AS geometry
            FROM (SELECT {parent} AS p, sum(population) AS population,
                         sum(pop_65_plus) AS pop_65_plus, sum(pop_0_14) AS pop_0_14
                  FROM cells GROUP BY 1)""")
        antimeridian(con, "t")
        con.execute(f"COPY t TO '{src}' (FORMAT parquet)")
        band = work / f"z{lo}-{hi}.pmtiles"
        pmtiles.tile(src, band, layer=TILE_LAYER, min_zoom=lo, max_zoom=hi,
                     properties=TILE_PROPERTIES, verbatim=True)
        archives.append((lo, hi, band, TILE_LAYER))
        print(f"  tiles z{lo}-{hi} (r{res}) done", flush=True)
    entry = pmtiles.pyramid(archives, dest)
    entry.update({"layer": TILE_LAYER, "properties": TILE_PROPERTIES,
                  "bands": [{"min_zoom": lo, "max_zoom": hi, "h3_res": r}
                            for lo, hi, r in TILE_BANDS]})
    return entry


def attribution(source: dict, gaul_accessed: str | None) -> str:
    return f"""\
# Data attribution

This data is WorldPop's Global2 population estimates, release {RELEASE}
version {VERSION}, for {YEAR}: constrained, 1 km (30 arc seconds), total
population and age and sex structures.

  Source:   {POP_DIR}/
            {AGESEX_DIR}/
  Licence:  Creative Commons Attribution 4.0 International (CC BY 4.0),
            https://creativecommons.org/licenses/by/4.0/
  DOI:      {DOI}

Cite it as:

  {CITATION}

WorldPop (www.worldpop.org) is a research programme of the University of
Southampton. Years after 2020 are modelled estimates, not counts.

## What was changed here

The 1 km pixels are summed on H3 cells: each pixel goes whole to the
resolution {RES} cell holding its centre (resolution {WORLD_RES} for the world
file), so every total is WorldPop's, moved by at most half a cell. The age
columns are sums of WorldPop's age bands (both sexes); `female` and `male`
sum the per-sex bands. The `country` column is not WorldPop's: it is the
GAUL 2024 L0 unit holding the cell centre (FAO, Global Administrative Unit
Layers 2024{f', accessed {gaul_accessed}' if gaul_accessed else ''}, following the UN
delineation; disputed areas keep GAUL's x codes). WorldPop's own national
totals use WorldPop's boundaries and differ at borders. The PMTiles archive
is derived for web maps.

Source files, as read for this build: {len(BANDS)} rasters under the two
folders above; the total, {source['total']['url'].rsplit('/', 1)[-1]},
Last-Modified {source['total']['last_modified']}.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def build(work: Path, out: Path, gaul: str, gaul_accessed: str | None, *,
          keep: bool = False, only: list[str] | None = None, threads: int = 4,
          memory: str = "4GB") -> None:
    work.mkdir(parents=True, exist_ok=True)
    source = upstream()
    con = duckdb.connect(str(work / "worldpop.duckdb"))
    con.execute(f"INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs; "
                f"INSTALL h3 FROM community; LOAD h3; SET threads = {threads}; "
                f"SET memory_limit = '{memory}'; SET temp_directory = '{work / 'tmp'}'; "
                "SET geometry_always_xy = true")
    t0 = time.monotonic()
    bands = count_all(con, work, keep=keep)
    cell_table(con, work, bands)
    gaul_path = work / "gaul_l0.parquet"
    if not gaul_path.exists():
        con.execute(f"COPY (SELECT iso3_code, gaul0_name, geometry FROM read_parquet('{gaul}')) "
                    f"TO '{gaul_path}' (FORMAT parquet)")
    assigned = countries(con, gaul_path)
    con.execute("""CREATE OR REPLACE TABLE cells AS
                   SELECT c.*, k.country FROM cells c JOIN cell_country k USING (cell)""")
    print(f"  {con.execute('SELECT count(*) FROM cells').fetchone()[0]:,} cells, countries "
          f"assigned, {time.monotonic() - t0:.0f} s", flush=True)

    folder = out / RELEASE / str(YEAR)
    folder.mkdir(parents=True, exist_ok=True)
    codes = [r[0] for r in con.execute("SELECT DISTINCT country FROM cells ORDER BY 1"
                                       ).fetchall()]
    names = country_names(con, gaul_path)
    entries = []
    for code in codes:
        if only and code not in only:
            continue
        e = write_cells(con, f"SELECT {cell_columns()} FROM cells WHERE country = '{code}'",
                        folder / f"country={code}" / f"{STEM}.parquet")
        entries.append({"country": code, "name": names[code],
                        "path": f"country={code}/{STEM}.parquet", **e})
    print(f"  {len(entries)} country files, {time.monotonic() - t0:.0f} s", flush=True)

    world = write_cells(con, f"""
        SELECT {cell_columns('p')} FROM (
            SELECT p, arg_max(country, population) AS country,
                   {', '.join(f'sum({c}) AS {c}' for c in COLUMNS)}
            FROM (SELECT h3_cell_to_parent(cell, {WORLD_RES}) AS p, country,
                         {', '.join(COLUMNS)} FROM cells)
            GROUP BY p)""", folder / f"{STEM}_world_r{WORLD_RES}.parquet")
    print(f"  world file: {world['features']:,} cells, {time.monotonic() - t0:.0f} s",
          flush=True)
    tile_entry = tiles(con, work, folder / f"{STEM}.pmtiles")
    print(f"  {tile_entry['file']}: {tile_entry['tiles']:,} tiles, "
          f"{tile_entry['bytes'] / 1e6:.1f} MB, {time.monotonic() - t0:.0f} s", flush=True)

    totals = dict(zip(COLUMNS, con.execute(
        f"SELECT {', '.join(f'sum({c})' for c in COLUMNS)} FROM cells").fetchone()))
    (folder / "ATTRIBUTION.txt").write_text(attribution(source, gaul_accessed))
    (folder / "_manifest.json").write_text(json.dumps({
        "state_name": f"WorldPop Global2 {RELEASE}, {YEAR}, on H3",
        "release": RELEASE, "version": VERSION, "year": YEAR,
        "h3_res": RES, "world_h3_res": WORLD_RES, "crs": "OGC:CRS84",
        "total_features": sum(e["features"] for e in entries),
        "totals": totals,
        "bands": {b: {"sum": v["sum"], "pixels": v["pixels"], "url": v["url"]}
                  for b, v in bands.items()},
        "columns": COLUMNS, "descriptions": DESCRIPTIONS,
        "countries": assigned,
        "source": source,
        "partitions": {"key": "country", "column": "country", "files": len(entries),
                       "bytes": sum(e["bytes"] for e in entries), "entries": entries},
        "world": world,
        "pmtiles": [{"name": STEM, **tile_entry}],
    }, indent=2) + "\n")
    # The parquetry repository files at the dataset root.
    (out / "index.json").write_text(json.dumps({"datasets": [
        {"path": f"{RELEASE}/{YEAR}", "code": f"{RELEASE}-{YEAR}",
         "name": f"WorldPop Global2 {RELEASE}, {YEAR}, on H3"}]}, indent=2) + "\n")
    (out / "snapshots.json").write_text(json.dumps({"latest": "", "snapshots": []}) + "\n")
    (out / "_source.json").write_text(json.dumps(source, indent=2) + "\n")
    con.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="headers of the source and newer releases, as JSON")
    b = sub.add_parser("build", help="download, count, assign countries, write, tile")
    b.add_argument("--work-dir", type=Path, required=True)
    b.add_argument("--out-dir", type=Path, required=True)
    b.add_argument("--gaul", default=GAUL_L0, help="GAUL_2024_L0_derived.parquet, path or URL")
    b.add_argument("--gaul-accessed", help="GAUL's access date, from gaul/2024/_manifest.json")
    b.add_argument("--keep", action="store_true", help="keep the rasters after counting")
    b.add_argument("--only", nargs="*", help="write only these countries (ISO3)")
    b.add_argument("--threads", type=int, default=4)
    b.add_argument("--memory", default="4GB")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
    else:
        build(args.work_dir, args.out_dir, args.gaul, args.gaul_accessed, keep=args.keep,
              only=args.only, threads=args.threads, memory=args.memory)


if __name__ == "__main__":
    main()
