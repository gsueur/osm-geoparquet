#!/usr/bin/env python3
"""
FEMA's USA Structures as GeoParquet: one file of building footprints per
state, each with its occupancy class and ORNL's estimate of the people in it.

USA Structures is produced by Oak Ridge National Laboratory (ORNL) with FEMA's
Geospatial Response Office and published on FEMA's public S3 bucket
fema-femadata, one zip per state (a file geodatabase with one layer) under
Partners/ORNL/USA_Structures/<State Name>/. States are refreshed in batches:
the folders keep earlier deliveries beside the newest, under two naming
schemes (XX_Structures_20260913.zip, Deliverable20250606XX.zip), so the
newest zip of each folder is read, and each state carries its own vintage.
This writes:

  <out>/latest/state=<XX>/structures.parquet   the footprints of one state, postal code
  <out>/latest/state=<XX>/cells.parquet        their counts on H3 cells (res 4, 6, 8)
  <out>/latest/state=<XX>/structures.pmtiles   the state's map: cells, then footprints
  <out>/latest/_manifest.json    per state: vintage, source zip, rows, sizes, sha256, gate
  <out>/latest/ATTRIBUTION.txt
  <out>/index.json, <out>/_source.json         _source.json: the zip read per state

One path per state, `latest/state=<XX>/`, rewritten when that state's zip
changes: states move one at a time, so a release folder would never hold a
consistent set. The manifest says which vintage each state is.

Columns are FEMA's, names unchanged, except: Shape_Area and Shape_Length
(the geodatabase's own measures, in square degrees and degrees here) are
dropped; UUID loses the braces some deliveries wrap it in. Added: a `bbox`
declared as the GeoParquet covering and the footprint geometry (OGC:CRS84,
FEMA ships EPSG:4326). Rows are sorted on a Hilbert curve and cut into row
groups of at most 4 MiB of geometry and strings, streamed (the largest
states do not fit in memory at once).

The gate, per state: every feature of the layer is written (none without a
geometry), the delivery's columns are the expected ones, UUID duplicates and
footprints outside the state's TIGER/Line 2026 box are counted (kept).

Usage:
  python3 scripts/datasets/structures.py upstream
  python3 scripts/datasets/structures.py changed --published https://parquetry.geomermaids.com/structures
  python3 scripts/datasets/structures.py build --state RI --work-dir work --out-dir out/structures
  python3 scripts/datasets/structures.py retile --state RI --work-dir work --out-dir out/structures
  python3 scripts/datasets/structures.py finalize --out-dir out/structures
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles  # noqa: E402
from datasets.geoconnex import (  # noqa: E402
    BBOX_STRUCT,
    GROUP_BYTES,
    MAX_GROUP_ROWS,
    ZSTD_LEVEL,
    sha256,
    verify,
    write_json,
)
from datasets.infra import GEOMETRY_TYPE_NAMES  # noqa: E402

BUCKET_URL = "https://fema-femadata.s3.amazonaws.com"
PREFIX = "Partners/ORNL/USA_Structures/"
PAGE = "https://gis-fema.hub.arcgis.com/pages/usa-structures-data"
PUBLIC = "https://parquetry.geomermaids.com"
PUBLIC_DATA = f"{PUBLIC}/structures"
TIGER_STATES = f"{PUBLIC}/census/tiger/2026/substategeo/state.parquet"
RELEASE = "latest"
CREDIT = "Oak Ridge National Laboratory (ORNL); FEMA Geospatial Response Office"
S3_NS = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}

# The delivery's columns in layer order (checked on the 2025-06, 2025-09
# and 2026-09 deliveries), the geometry apart.
SOURCE_COLUMNS = [
    "Shape_Area", "Shape_Length", "BUILD_ID", "OCC_CLS", "PRIM_OCC", "SEC_OCC", "PROP_ADDR",
    "PROP_CITY", "PROP_ST", "PROP_ZIP", "PROP_CNTY", "OUTBLDG", "HEIGHT", "SQMETERS", "SQFEET",
    "H_ADJ_ELEV", "L_ADJ_ELEV", "FIPS", "CENSUSCODE", "PROD_DATE", "SOURCE", "USNG", "LONGITUDE",
    "LATITUDE", "IMAGE_NAME", "IMAGE_DATE", "VAL_METHOD", "REMARKS", "UUID", "B_CODE",
    "POP_CI95_UPPER", "POP_MEDIAN", "POP_CI95_LOWER",
]
DROPPED = {"Shape_Area", "Shape_Length"}
KEPT = [c for c in SOURCE_COLUMNS if c not in DROPPED]
FILE_COLUMNS = KEPT + ["bbox", "geometry"]

# Cells on H3, and the map's bands: (min zoom, max zoom, H3 resolution).
H3_RESOLUTIONS = (4, 6, 8)
H3_BANDS = ((0, 4, 4), (5, 7, 6), (8, 12, 8))
CELL_PROPS = ["h3", "structures", "residential", "population"]
# Footprints from zoom 13, every one at zoom 14 (tylertoo thins the coarser
# zoom, the largest footprint winning a cell); the H3 cells up to the zoom
# before. The steps of the ladder, tried in order: pmtiles.tile() fails a
# step that puts a tile over the cap. Every property of FOOTPRINT_PROPS
# from zoom 12 is over the 500 KB cap even in Rhode Island.
FOOTPRINT_MAX_ZOOM = 14
FOOTPRINT_PROPS = ["BUILD_ID", "OCC_CLS", "PRIM_OCC", "HEIGHT", "SQFEET", "POP_MEDIAN"]
# SQFEET stays: --sort-key reads it.
FOOTPRINT_MIN = ["OCC_CLS", "SQFEET"]
FOOTPRINT_STEPS = [(FOOTPRINT_PROPS, 13), (FOOTPRINT_MIN, 13), (FOOTPRINT_PROPS, 14),
                   (FOOTPRINT_MIN, 14)]
FOOTPRINT_LAYER = "structures"


# ---------- source ----------

def listing() -> list[dict]:
    """Every object under PREFIX, through S3's anonymous ListObjectsV2."""
    out, token = [], None
    while True:
        url = f"{BUCKET_URL}/?list-type=2&prefix={urllib.parse.quote(PREFIX)}"
        if token:
            url += f"&continuation-token={urllib.parse.quote(token)}"
        with urllib.request.urlopen(url, timeout=120) as r:
            root = ET.fromstring(r.read())
        for c in root.findall("s:Contents", S3_NS):
            out.append({"key": c.find("s:Key", S3_NS).text,
                        "last_modified": c.find("s:LastModified", S3_NS).text,
                        "etag": c.find("s:ETag", S3_NS).text.strip('"'),
                        "bytes": int(c.find("s:Size", S3_NS).text)})
        if root.find("s:IsTruncated", S3_NS).text != "true":
            return out
        token = root.find("s:NextContinuationToken", S3_NS).text


NAME_PATTERNS = (re.compile(r"^(?P<st>[A-Z]{2})_Structures_(?P<date>\d{8})\.zip$"),
                 re.compile(r"^Deliverable(?P<date>\d{6,8})(?P<st>[A-Z]{2})\.zip$"))


def parse_name(file: str) -> tuple[str, str | None] | None:
    """(postal code, vintage YYYY-MM-DD or None) from a zip name. The older
    Deliverable names drop leading zeros (Deliverable202496AL.zip), so only
    an eight-digit date is read as one."""
    for p in NAME_PATTERNS:
        m = p.match(file)
        if m:
            d = m.group("date")
            return m.group("st"), (f"{d[:4]}-{d[4:6]}-{d[6:]}" if len(d) == 8 else None)
    return None


def upstream() -> dict[str, dict]:
    """The newest zip of each state folder (by upload time), keyed by
    postal code. A folder whose newest zip has an unknown name, or whose
    postal code repeats, stops the build: the naming moved."""
    folders: dict[str, list[dict]] = {}
    for o in listing():
        parts = o["key"][len(PREFIX):].split("/")
        if len(parts) == 2 and parts[1].lower().endswith(".zip"):
            folders.setdefault(parts[0], []).append({**o, "file": parts[1]})
    states = {}
    for folder, objs in sorted(folders.items()):
        newest = max(objs, key=lambda o: o["last_modified"])
        parsed = parse_name(newest["file"])
        if not parsed:
            sys.exit(f"{folder}: newest zip {newest['file']!r} matches no known name")
        st, vintage = parsed
        if st in states:
            sys.exit(f"{folder}: postal code {st} already read from {states[st]['folder']}")
        states[st] = {"folder": folder, "file": newest["file"], "key": newest["key"],
                      "url": f"{BUCKET_URL}/{urllib.parse.quote(newest['key'])}",
                      "vintage": vintage, "bytes": newest["bytes"], "etag": newest["etag"],
                      "last_modified": newest["last_modified"]}
    return states


def get_json(url: str) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def changed(published: str) -> list[str]:
    """States whose newest zip differs (key or ETag) from the one the
    published _source.json records, plus states gone upstream: build skips
    those, and finalize drops them from the manifest."""
    up = upstream()
    was = (get_json(f"{published}/_source.json") or {}).get("states", {})
    moved = [st for st, s in up.items()
             if (was.get(st) or {}).get("key") != s["key"] or was[st].get("etag") != s["etag"]]
    return sorted(moved + sorted(set(was) - set(up)))


def fetch(state: dict, work: Path) -> Path:
    """The state's zip, resumed with ranges; a copy of the right size is reused."""
    dest = work / f"{state['file']}"
    if dest.is_file() and dest.stat().st_size == state["bytes"]:
        return dest
    work.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, 6):
        r = subprocess.run(["curl", "-fsSL", "--retry", "5", "--retry-delay", "15", "-C", "-",
                            "-o", str(dest), state["url"]], check=False)
        if r.returncode == 0 and dest.stat().st_size == state["bytes"]:
            return dest
        print(f"  fetch attempt {attempt}: {dest.stat().st_size if dest.exists() else 0:,} "
              f"of {state['bytes']:,} bytes", flush=True)
        time.sleep(30 * attempt)
    sys.exit(f"{state['url']}: got {dest.stat().st_size if dest.exists() else 0:,} bytes, "
             f"the listing says {state['bytes']:,}")


# ---------- build ----------

def connect(work: Path) -> duckdb.DuckDBPyConnection:
    """A disk-backed database: the largest states (Texas, California) do not
    fit in memory, and the Hilbert sort spills to work/spill."""
    work.mkdir(parents=True, exist_ok=True)
    db = work / "build.duckdb"
    db.unlink(missing_ok=True)
    con = duckdb.connect(str(db))
    con.execute(f"INSTALL spatial; LOAD spatial; INSTALL h3 FROM community; LOAD h3; "
                f"INSTALL httpfs; LOAD httpfs; SET temp_directory = '{work / 'spill'}'; "
                "SET memory_limit = '4GB'; SET threads = 4; SET preserve_insertion_order = false")
    return con


def gdb_path(archive: Path, st: str) -> tuple[str, str]:
    return f"/vsizip/{archive}/{st}_Structures.gdb", f"{st}_Structures"


def load(con, archive: Path, st: str) -> int:
    """The layer into table `src`, read straight from the zip; the source
    count is the layer's own (ST_Read_Meta), compared with what was read."""
    path, layer = gdb_path(archive, st)
    meta = con.execute(f"SELECT layers FROM ST_Read_Meta('{path}')").fetchone()[0]
    layers = {m["name"]: m for m in meta}
    # Deliveries differ in case (AR 2025-09-13 names it AR_structures): match
    # the layer case-insensitively, but only when exactly one fits.
    fits = [n for n in layers if n.lower() == layer.lower()]
    if len(fits) != 1:
        sys.exit(f"{archive.name}: no single layer {layer}, has {sorted(layers)}")
    layer = fits[0]
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ST_Read('{path}', layer = '{layer}')")
            .fetchall() if r[0] != "Shape"]
    if cols != SOURCE_COLUMNS:
        sys.exit(f"{archive.name}: columns differ from the expected delivery: extra "
                 f"{sorted(set(cols) - set(SOURCE_COLUMNS))}, missing "
                 f"{sorted(set(SOURCE_COLUMNS) - set(cols))}")
    select = ", ".join("trim(UUID, '{}') AS UUID" if c == "UUID" else f'"{c}"' for c in KEPT)
    con.execute(f"""
        CREATE OR REPLACE TABLE src AS
        SELECT {select}, Shape::GEOMETRY AS geom
        FROM ST_Read('{path}', layer = '{layer}')""")
    return layers[layer]["feature_count"]


def gate(con, st: str, expected: int) -> dict:
    n, no_geom = con.execute("SELECT count(*), count(*) FILTER (WHERE geom IS NULL) FROM src").fetchone()
    if n != expected:
        sys.exit(f"{st}: read {n:,} rows, the layer has {expected:,}")
    if no_geom:
        sys.exit(f"{st}: {no_geom:,} features without a geometry")
    uuid_repeated = con.execute("""
        SELECT count(*) FROM (SELECT UUID FROM src WHERE UUID IS NOT NULL GROUP BY 1
                              HAVING count(*) > 1)""").fetchone()[0]
    outside = con.execute(f"""
        SELECT count(*) FROM src, (SELECT bbox.xmin AS x0, bbox.ymin AS y0, bbox.xmax AS x1,
                                          bbox.ymax AS y1 FROM read_parquet('{TIGER_STATES}')
                                   WHERE STUSPS = '{st}') t
        WHERE NOT (ST_XMin(geom) >= t.x0 - 0.01 AND ST_XMax(geom) <= t.x1 + 0.01
                   AND ST_YMin(geom) >= t.y0 - 0.01 AND ST_YMax(geom) <= t.y1 + 0.01)
        """).fetchone()[0]
    stats = con.execute("""
        SELECT sum(POP_MEDIAN), sum(POP_CI95_LOWER), sum(POP_CI95_UPPER),
               count(*) FILTER (WHERE OCC_CLS = 'Residential'),
               count(*) FILTER (WHERE OCC_CLS = 'Residential' AND POP_MEDIAN IS NOT NULL),
               count(*) FILTER (WHERE POP_CI95_LOWER > POP_MEDIAN OR POP_MEDIAN > POP_CI95_UPPER),
               min(PROD_DATE)::DATE::VARCHAR, max(PROD_DATE)::DATE::VARCHAR
        FROM src""").fetchone()
    occ = dict(con.execute("SELECT coalesce(OCC_CLS, ''), count(*) FROM src GROUP BY 1 "
                           "ORDER BY 2 DESC").fetchall())
    return {"layer_features": expected, "rows_written": n, "uuid_values_repeated": uuid_repeated,
            "outside_state_box": outside, "population_median": stats[0],
            "population_lower": stats[1], "population_upper": stats[2],
            "residential": stats[3], "residential_with_population": stats[4],
            "population_bounds_out_of_order": stats[5],
            "prod_date_min": stats[6], "prod_date_max": stats[7], "occupancy": occ}


def stream_rewrite(src: Path, dest: Path) -> None:
    """geoconnex.rewrite()'s row groups (at most GROUP_BYTES of geometry and
    strings, MAX_GROUP_ROWS rows, same order and writer settings), read and
    written one batch at a time: Texas, about 12 million footprints, does
    not fit in memory as one Arrow table on a 16 GB runner."""
    import geoarrow.pyarrow  # noqa: F401  registers the geoarrow.wkb extension type
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(src)
    schema = pf.schema_arrow
    leaves = pf.metadata.schema
    strings = [leaves.column(k).path for k in range(len(leaves))
               if leaves.column(k).physical_type == "BYTE_ARRAY" and leaves.column(k).path != "geometry"]
    floats = [leaves.column(k).path for k in range(len(leaves))
              if leaves.column(k).physical_type in ("FLOAT", "DOUBLE")]

    def row_sizes(batch: pa.RecordBatch) -> np.ndarray:
        sizes = np.zeros(batch.num_rows, dtype=np.int64)
        for col in batch.columns:
            typ = col.type.storage_type if isinstance(col.type, pa.ExtensionType) else col.type
            if pa.types.is_binary(typ) or pa.types.is_string(typ) or pa.types.is_large_string(typ):
                arr = col.storage if isinstance(col, pa.ExtensionArray) else col
                sizes += pc.fill_null(pc.binary_length(arr), 0).to_numpy(zero_copy_only=False)
            else:
                sizes += 8
        return sizes

    pending: list[pa.RecordBatch] = []
    size, rows = 0, 0
    with pq.ParquetWriter(dest, schema, compression="zstd", compression_level=ZSTD_LEVEL,
                          use_dictionary=strings, use_byte_stream_split=floats,
                          write_statistics=True, data_page_size=1 << 20) as w:
        def flush():
            nonlocal pending, size, rows
            if pending:
                t = pa.Table.from_batches(pending, schema=schema)
                w.write_table(t, row_group_size=t.num_rows)
            pending, size, rows = [], 0, 0

        for batch in pf.iter_batches(batch_size=65_536):
            sizes = row_sizes(batch)
            start = 0
            for i, n in enumerate(sizes.tolist()):
                if rows and (size + n > GROUP_BYTES or rows == MAX_GROUP_ROWS):
                    pending.append(batch.slice(start, i - start))
                    start = i
                    flush()
                size += n
                rows += 1
            pending.append(batch.slice(start))
        flush()


def write_footprints(con, dest: Path) -> None:
    """Sorted, bbox-covered GeoParquet 2.0 of `src`, as geoconnex.write()
    does it, with the row groups cut by stream_rewrite()."""
    xmin, ymin, xmax, ymax, types = con.execute("""
        SELECT min(ST_XMin(geom)), min(ST_YMin(geom)), max(ST_XMax(geom)),
               max(ST_YMax(geom)), list(DISTINCT ST_GeometryType(geom)::VARCHAR)
        FROM src""").fetchone()
    geo = json.dumps({
        "version": "2.0.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB",
            "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in types),
            "bbox": [xmin, ymin, xmax, ymax],
            "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}},
        }},
    }).replace("'", "''")
    attrs = ", ".join(f'"{c}"' for c in KEPT)
    box = f"ST_Extent(ST_MakeEnvelope({xmin!r}, {ymin!r}, {xmax!r}, {ymax!r}))"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.stem + ".duckdb.parquet")
    con.execute(f"""
        COPY (SELECT {attrs}, {BBOX_STRUCT}, geom AS geometry FROM src
              ORDER BY ST_Hilbert(geom, {box}))
        TO '{tmp}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', COMPRESSION ZSTD,
                    COMPRESSION_LEVEL 1, ROW_GROUP_SIZE 65536, KV_METADATA {{geo: '{geo}'}})""")
    stream_rewrite(tmp, dest)
    tmp.unlink()


def write_cells(con, dest: Path) -> dict[int, int]:
    """Footprints, residential footprints and ORNL's median population per
    H3 cell (by the footprint's centroid), at each resolution of
    H3_RESOLUTIONS, with the cell outline."""
    parts = " UNION ALL ".join(
        f"SELECT {r} AS h3_res, h3_latlng_to_cell(ST_Y(c), ST_X(c), {r}) AS cell, OCC_CLS, "
        f"POP_MEDIAN FROM (SELECT ST_Centroid(geom) AS c, OCC_CLS, POP_MEDIAN FROM src)"
        for r in H3_RESOLUTIONS)
    con.execute(f"""
        COPY (
          WITH c AS ({parts}),
          g AS (SELECT h3_res, cell, count(*) AS structures,
                       count(*) FILTER (WHERE OCC_CLS = 'Residential') AS residential,
                       coalesce(sum(POP_MEDIAN), 0)::BIGINT AS population
                FROM c GROUP BY ALL)
          SELECT h3_h3_to_string(cell) AS h3, h3_res, structures, residential, population,
                 ST_GeomFromWKB(h3_cell_to_boundary_wkb(cell)) AS geometry
          FROM g ORDER BY h3_res, cell)
        TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    return {int(r): n for r, n in con.execute(
        f"SELECT h3_res, count(*) FROM read_parquet('{dest}') GROUP BY 1 ORDER BY 1").fetchall()}


def footprint_band(footprints: Path, work: Path) -> tuple[Path, dict]:
    """The footprint zooms, down the ladder of FOOTPRINT_STEPS."""
    part = work / "footprints.pmtiles"
    tried = []
    for props, min_zoom in FOOTPRINT_STEPS:
        try:
            entry = pmtiles.tile(footprints, part, layer=FOOTPRINT_LAYER, min_zoom=min_zoom,
                                 max_zoom=FOOTPRINT_MAX_ZOOM, properties=props, coverage=False,
                                 extra=["--sort-key", "SQFEET", "--max-tile-size", "500K"])
        except SystemExit as e:
            print(f"    footprints: {len(props)} properties from z{min_zoom}: {e}", flush=True)
            tried.append({"properties": props, "min_zoom": min_zoom})
            continue
        return part, {**entry, **({"fallbacks": tried} if tried else {})}
    sys.exit(f"{footprints}: no footprint tile setting fits the 500 KB cap")


def tiles(cells: Path, footprints: Path, work: Path, dest: Path) -> dict:
    """One archive per state: the H3 cells at the coarse zooms (NAD's
    bands, cells drawn as they are), the footprints from the zoom
    footprint_band() settled on; pmtiles.pyramid() joins them. Run after
    the build database is closed, so tylertoo has its memory."""
    work.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    fp_part, fp = footprint_band(footprints, work)
    bands = []
    for i, (lo, hi, res) in enumerate(H3_BANDS):
        # The last band of cells reaches the first zoom of the footprints,
        # whichever step of the ladder that is: no blank zoom between them.
        hi = fp["min_zoom"] - 1 if i == len(H3_BANDS) - 1 else min(hi, fp["min_zoom"] - 1)
        if lo > hi:
            continue
        src = work / f"cells_r{res}.parquet"
        con.execute(f"COPY (SELECT * EXCLUDE (h3_res) FROM read_parquet('{cells}') "
                    f"WHERE h3_res = {res}) TO '{src}' (FORMAT PARQUET)")
        part = work / f"cells_r{res}.pmtiles"
        pmtiles.tile(src, part, layer=f"cells_r{res}", min_zoom=lo, max_zoom=hi,
                     properties=CELL_PROPS, verbatim=True)
        bands.append((lo, hi, part, f"cells_r{res}"))
        src.unlink()
    bands.append((fp["min_zoom"], FOOTPRINT_MAX_ZOOM, fp_part, FOOTPRINT_LAYER))
    entry = pmtiles.pyramid(bands, dest)
    entry["bands"] = [{"min_zoom": lo, "max_zoom": hi, "layer": layer} for lo, hi, _, layer in bands]
    entry["footprints"] = {k: fp[k] for k in ("min_zoom", "properties", "tiles")
                           if k in fp} | ({"fallbacks": fp["fallbacks"]} if "fallbacks" in fp else {})
    for _, _, part, _ in bands:
        part.unlink(missing_ok=True)
    return entry


def file_entry(con, path: Path) -> dict:
    groups, largest = con.execute(f"""
        SELECT count(*), max(b) FROM (SELECT row_group_id, sum(total_compressed_size) AS b
                                      FROM parquet_metadata('{path}') GROUP BY 1)""").fetchone()
    return {"file": path.name, "bytes": path.stat().st_size, "row_groups": groups,
            "largest_row_group_bytes": largest, "sha256": sha256(path)}


def check_columns(dest: Path) -> None:
    import pyarrow.parquet as pq
    have = pq.ParquetFile(dest).schema_arrow.names
    if have != FILE_COLUMNS:
        sys.exit(f"{dest}: columns differ: extra {sorted(set(have) - set(FILE_COLUMNS))}, "
                 f"missing {sorted(set(FILE_COLUMNS) - set(have))}")


def build_state(st: str, state: dict, work: Path, out: Path, *, no_tiles: bool,
                low_disk: bool) -> dict:
    t0 = time.monotonic()
    print(f"{st}: {state['file']} ({state['bytes'] / 1e6:,.1f} MB, vintage {state['vintage']})",
          flush=True)
    archive = fetch(state, work)
    con = connect(work)
    expected = load(con, archive, st)
    if low_disk:
        archive.unlink()
    g = gate(con, st, expected)
    print(f"  loaded {g['rows_written']:,} footprints, {time.monotonic() - t0:.0f} s", flush=True)
    sdir = out / RELEASE / f"state={st}"
    footprints = sdir / "structures.parquet"
    write_footprints(con, footprints)
    check_columns(footprints)
    verify(con, footprints, expected)
    cells = sdir / "cells.parquet"
    counts = write_cells(con, cells)
    entry = {"state": st, "path": f"state={st}/structures.parquet", "features": expected,
             **file_entry(con, footprints),
             "source": {k: state[k] for k in ("file", "key", "vintage", "bytes", "etag",
                                               "last_modified")},
             "gate": g,
             "cells": {"path": f"state={st}/cells.parquet", "cells": counts,
                       "bytes": cells.stat().st_size, "sha256": sha256(cells)}}
    print(f"  written {entry['bytes'] / 1e6:,.1f} MB in {entry['row_groups']} groups, "
          f"{time.monotonic() - t0:.0f} s", flush=True)
    con.close()
    (work / "build.duckdb").unlink(missing_ok=True)
    if not no_tiles:
        dest = sdir / "structures.pmtiles"
        t = tiles(cells, footprints, work, dest)
        entry["pmtiles"] = {**t, "path": f"state={st}/structures.pmtiles"}
        print(f"  tiles {t['bytes'] / 1e6:,.1f} MB, {t['tiles']:,} tiles, footprints from "
              f"z{t['footprints']['min_zoom']}, {time.monotonic() - t0:.0f} s", flush=True)
    write_json(out / "entries" / f"{st}.json", entry)
    return entry


def fetch_published(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["curl", "-fsSL", "--retry", "5", "-o", str(dest), url], check=True)


def retile(states: list[str], work: Path, out: Path, published: str) -> None:
    """New maps for published states (a change of tile settings), from the
    published footprints and cells: their entries keep everything else."""
    old = get_json(f"{published}/{RELEASE}/_manifest.json") or {}
    have = {e["state"]: e for e in old.get("states", [])}
    for st in states:
        if st not in have:
            sys.exit(f"{st}: not in the published manifest")
        e = {k: v for k, v in have[st].items() if k != "name"}
        sdir = out / RELEASE / f"state={st}"
        footprints, cells = sdir / "structures.parquet", sdir / "cells.parquet"
        fetch_published(f"{published}/{RELEASE}/{e['path']}", footprints)
        fetch_published(f"{published}/{RELEASE}/{e['cells']['path']}", cells)
        if sha256(footprints) != e["sha256"]:
            sys.exit(f"{st}: published footprints differ from the manifest's sha256")
        t = tiles(cells, footprints, work, sdir / "structures.pmtiles")
        e["pmtiles"] = {**t, "path": f"state={st}/structures.pmtiles"}
        # Only the map goes up again: the data stay as published.
        footprints.unlink()
        cells.unlink()
        write_json(out / "entries" / f"{st}.json", e)
        print(f"{st}: tiles {t['bytes'] / 1e6:,.1f} MB, footprints from "
              f"z{t['footprints']['min_zoom']}", flush=True)


def tiger_names() -> dict[str, str]:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    return dict(con.execute(f"SELECT STUSPS, NAME FROM read_parquet('{TIGER_STATES}')").fetchall())


def attribution(states: list[dict]) -> str:
    vintages = sorted({e["source"]["vintage"] or "unknown" for e in states})
    return f"""\
USA Structures (FEMA, ORNL), building footprints of the United States
Republished as GeoParquet by Geomermaids: {PUBLIC_DATA}/

Source: {BUCKET_URL}/{PREFIX}<State Name>/ (FEMA's public bucket fema-femadata),
the newest zip of each state; _manifest.json names each one. Vintages in this
set: {', '.join(vintages)}.
About USA Structures: {PAGE}

Licence: a work of the US Government; FEMA's metadata states no access or use
constraints. Credit "{CREDIT}".

Population estimates (POP_MEDIAN and its 95% bounds) are ORNL's modelled
residents per structure, not a census count.

Changes made here: values unchanged; Shape_Area and Shape_Length dropped (the
geodatabase's measures, in degrees); UUID braces removed; added `bbox` and the
footprint `geometry` (OGC:CRS84, from FEMA's EPSG:4326). Per state, the H3
cell counts (cells.parquet) and the map (structures.pmtiles) are derived here.
"""


def finalize(out: Path, published: str | None) -> None:
    """The manifest from this run's per-state entries over the published
    one: a run that rebuilt only the states that moved keeps the others'
    entries as published."""
    # Not the ._<name> files macOS writes beside each file on an exFAT disk.
    entries = {p.stem: json.loads(p.read_text()) for p in sorted((out / "entries").glob("*.json"))
               if not p.name.startswith("._")}
    old = get_json(f"{published}/{RELEASE}/_manifest.json") if published else None
    merged = {e["state"]: e for e in (old or {}).get("states", [])}
    merged.update(entries)
    up = upstream()
    gone = sorted(set(merged) - set(up))
    if gone:
        print(f"  dropped, gone upstream: {gone}", flush=True)
        for st in gone:
            merged.pop(st)
    names = tiger_names()
    unknown = sorted(set(merged) - set(names))
    if unknown:
        sys.exit(f"postal codes not in TIGER/Line 2026: {unknown}")
    states = [{**merged[st], "name": names[st]} for st in sorted(merged)]
    rel = out / RELEASE
    rel.mkdir(parents=True, exist_ok=True)
    write_json(rel / "_manifest.json", {
        "dataset": "structures", "release": RELEASE, "crs": "OGC:CRS84",
        "total_features": sum(e["features"] for e in states),
        "population_median": sum(e["gate"]["population_median"] or 0 for e in states),
        "states": states,
        "glob": f"s3://parquetry/structures/{RELEASE}/state=*/structures.parquet",
        "cells_glob": f"s3://parquetry/structures/{RELEASE}/state=*/cells.parquet",
        "gone_upstream": gone,
    })
    (rel / "ATTRIBUTION.txt").write_text(attribution(states))
    write_json(out / "index.json", {"datasets": [
        {"path": RELEASE, "code": "structures", "name": "USA Structures (FEMA, ORNL)"}]})
    write_json(out / "_source.json", {"states": {e["state"]: e["source"] for e in states}})
    print(f"  manifest: {len(states)} states, {sum(e['features'] for e in states):,} footprints "
          f"({len(entries)} built in this run)", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="print the newest zip of each state")
    c = sub.add_parser("changed", help="print the states whose zip moved since the published set")
    c.add_argument("--published", default=PUBLIC_DATA)
    b = sub.add_parser("build", help="fetch, load, gate and write some states")
    b.add_argument("--state", action="append", required=True, help="postal code, repeatable")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help="the structures/ prefix")
    b.add_argument("--no-tiles", action="store_true")
    b.add_argument("--low-disk", action="store_true", help="delete each zip once loaded")
    b.add_argument("--published", default=PUBLIC_DATA)
    r = sub.add_parser("retile", help="rebuild the maps of published states")
    r.add_argument("--state", action="append", required=True)
    r.add_argument("--work-dir", type=Path, default=Path("work"))
    r.add_argument("--out-dir", type=Path, required=True)
    r.add_argument("--published", default=PUBLIC_DATA)
    f = sub.add_parser("finalize", help="write the manifest, attribution and _source.json")
    f.add_argument("--out-dir", type=Path, required=True)
    f.add_argument("--published", default=PUBLIC_DATA,
                   help="merge over this published set ('' for none)")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
    elif args.cmd == "changed":
        print(" ".join(changed(args.published)))
    elif args.cmd == "build":
        up = upstream()
        # A state gone upstream that the published set still lists is
        # skipped (changed() lists it; finalize drops it); any other unknown
        # code is a typo.
        was = (get_json(f"{args.published}/_source.json") or {}).get("states", {})
        bad = [st for st in args.state if st not in up and st not in was]
        if bad:
            sys.exit(f"not upstream: {bad}")
        for st in args.state:
            if st not in up:
                # Gone upstream (changed() lists it): finalize drops it.
                print(f"{st}: not upstream any more, skipped", flush=True)
                continue
            build_state(st, up[st], args.work_dir, args.out_dir, no_tiles=args.no_tiles,
                        low_disk=args.low_disk)
    elif args.cmd == "retile":
        retile(args.state, args.work_dir, args.out_dir, args.published)
    else:
        finalize(args.out_dir, args.published or None)


if __name__ == "__main__":
    main()
