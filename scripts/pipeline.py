#!/usr/bin/env python3
"""
Admin-region-first OSM -> GeoParquet pipeline.

Reads a single GeoJSON FeatureCollection of admin regions (OSM-derived, with
ISO3166-2 and name in properties), clips the source PBF per region, extracts
each theme declared in themes.py, writes optimized GeoParquet 2.0.

Country is derived from the ISO3166-2 prefix (e.g. 'US-NY' -> 'US',
'CA-ON' -> 'CA'), so the same pipeline invocation can process mixed-country
inputs.

Output layout:
  out/country=<CC>/state=<ISO3166-2>/<theme>.parquet
  out/country=<CC>/state=<ISO3166-2>/_manifest.json

Usage:
  python3 scripts/pipeline.py \
      --source-pbf data/north-america-latest.osm.pbf \
      --states-geojson data/admin_regions.geojson \
      --out-dir out/

  # Subset:
  python3 scripts/pipeline.py ... --states US-MA CA-ON --themes buildings roads
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from multiprocessing import Manager
from pathlib import Path
from queue import Empty

import duckdb

from themes import THEMES, POST_FILTERS, Theme, filter_predicate

# 0.4.0: the `geo` metadata now declares the bbox column as a GeoParquet
# covering, and each manifest records every file's size and sha256 so the
# catalog can publish file:size and file:checksum.
# 0.3.0: the per-file `state` column is now `state_name`, so it no longer
# collides with the `state=<ISO>` Hive key (DuckDB's hive auto-detection
# silently replaced the column with the path value on globbed reads).
# osm_id / osm_type are populated (they were NULL / 'Feature' before). The
# manifest gains `source_timestamp` and `theme_stats` for the catalog.
SCHEMA_VERSION = "0.4.0"

# ST_GeometryType spelling -> the GeoParquet `geometry_types` spelling.
GEOMETRY_TYPE_NAMES = {
    "POINT": "Point", "LINESTRING": "LineString", "POLYGON": "Polygon",
    "MULTIPOINT": "MultiPoint", "MULTILINESTRING": "MultiLineString",
    "MULTIPOLYGON": "MultiPolygon", "GEOMETRYCOLLECTION": "GeometryCollection",
}

# Set by main(); workers inherit it through fork. True = echo every subprocess
# command + per-theme status; False = quiet, main prints one progress line
# per completed state.
VERBOSE = False


# ---------- shell helpers ----------

def run(cmd: list[str]) -> None:
    if VERBOSE:
        print(f"    $ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, capture_output=not VERBOSE)

DEFAULT_RELATION_TYPES = "multipolygon"


def osmium_extract(src_pbf: Path, poly: Path, out_pbf: Path,
                   relation_types: str = DEFAULT_RELATION_TYPES) -> None:
    run([
        "osmium", "extract",
        "-p", str(poly),
        "-s", "smart",  # same strategy as osmium_extract_batch, see there
        "-S", f"types={relation_types}",
        str(src_pbf),
        "-o", str(out_pbf),
        "--overwrite",
    ])


def osmium_extract_batch(
    src_pbf: Path,
    state_polys: dict[str, Path],
    work_dir: Path,
    relation_types: str = DEFAULT_RELATION_TYPES,
) -> None:
    """Extract many per-state PBFs in one scan of the source PBF.

    Orders of magnitude faster than calling osmium_extract per state when
    the source is large — the source is only scanned once regardless of
    how many extracts we produce.

    Each state's output lands at work_dir/<ISO>/<ISO>.osm.pbf, matching
    what process_state expects when it later reuses the clipped state PBF.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    extracts = []
    for iso, poly_path in state_polys.items():
        (work_dir / iso).mkdir(parents=True, exist_ok=True)
        extracts.append({
            "output": f"{iso}/{iso}.osm.pbf",
            "polygon": {
                "file_name": str(poly_path.resolve()),
                "file_type": "geojson",
            },
        })
    config_path = work_dir / "_extract_config.json"
    config_path.write_text(json.dumps({"extracts": extracts}))
    # Strategy "smart": nodes inside the polygon, every way touching the
    # region kept whole with all its nodes, and relations of the types in
    # `-S types=` completed with all their members (osmium-extract(1)). The
    # default completes type=multipolygon only; boundary relations then get
    # their edge ways whole, which recovered most of them, but a member way
    # with no node inside the region polygon is still dropped and leaves the
    # ring open. The parent-extract boundaries job passes
    # multipolygon,boundary (--complete-relations): its source holds nothing
    # but administrative relations, so completing them costs nothing there,
    # whereas on a full extract the cost is unmeasured and the per-region
    # jobs keep the default. "simple" kept only the nodes inside the polygon; a
    # handful of missing edge nodes left rings open, and osmium export
    # silently dropped those areas: 20 of 32 Mexican state boundaries,
    # Ontario's, and border towns and counties everywhere (issue #5).
    # Keeping cross-border ways whole added only 0.0-0.2% ways, since an OSM
    # way is a short segment. Memory grows with outputs per call; see
    # --extract-batch-size.
    run([
        "osmium", "extract",
        "-c", str(config_path),
        "-d", str(work_dir),
        "-s", "smart",
        "-S", f"types={relation_types}",
        "--overwrite",
        str(src_pbf),
    ])
    config_path.unlink(missing_ok=True)


def osmium_tags_filter(src_pbf: Path, expr: str, out_pbf: Path) -> None:
    tokens = expr.split()
    run(
        ["osmium", "tags-filter", str(src_pbf), *tokens,
         "-o", str(out_pbf), "--overwrite"],
    )


def osmium_export(src_pbf: Path, geometry_types: str, out_jsonseq: Path) -> None:
    run([
        "osmium", "export",
        str(src_pbf),
        "--geometry-types", geometry_types,
        # Top-level feature id "n123" / "w123" / "r123" (areas take the id of
        # the way or relation they were built from). Without it osmium writes
        # no id at all, which left osm_id NULL and osm_type 'Feature' in every
        # file up to schema 0.2.0.
        "--add-unique-id", "type_id",
        "--output-format", "geojsonseq",
        "-x", "print_record_separator=false",
        "-o", str(out_jsonseq),
        "--overwrite",
    ])


# ---------- state loading ----------

@dataclass(frozen=True)
class State:
    iso: str        # 'US-VT'
    name: str       # 'Vermont'
    feature: dict   # original GeoJSON feature


def load_states(geojson_path: Path) -> list[State]:
    """Load states from a single GeoJSON FeatureCollection.

    Expects each feature to have properties:
      - 'ISO3166-2' (e.g. 'US-VT')
      - 'name'
    """
    data = json.loads(geojson_path.read_text())
    if data.get("type") != "FeatureCollection":
        sys.exit(f"{geojson_path} is not a FeatureCollection")

    states: list[State] = []
    skipped = 0
    for feat in data["features"]:
        props = feat.get("properties") or {}
        iso = props.get("ISO3166-2")
        name = props.get("name")
        if not iso or not name:
            skipped += 1
            continue
        states.append(State(iso=iso, name=name, feature=feat))

    if not states:
        sys.exit(f"No usable features in {geojson_path}")
    if skipped:
        print(f"Loaded {len(states)} states "
              f"(skipped {skipped} non-state features from {geojson_path.name})")
    return states


def write_state_polygon(state: State, dest: Path) -> None:
    """Write a single-feature FeatureCollection for osmium extract."""
    fc = {"type": "FeatureCollection", "features": [state.feature]}
    dest.write_text(json.dumps(fc))


# ---------- per-theme writer ----------

def write_theme_parquet(
    con: duckdb.DuckDBPyConnection,
    theme: Theme,
    jsonseq: Path,
    out_parquet: Path,
    *,
    country: str,
    state_name: str,
    state_iso: str,
) -> int:
    if not jsonseq.exists() or jsonseq.stat().st_size == 0:
        if VERBOSE:
            print(f"    [{theme.name}] empty jsonseq, skipping")
        return 0

    con.execute("DROP VIEW IF EXISTS src")
    con.execute(f"""
        CREATE VIEW src AS
        SELECT
            -- osmium ids: n<node>, w<way>, r<relation>, and a<area> for the
            -- areas it assembles. An area id is the source way id doubled, or
            -- the source relation id doubled plus one, so halving it recovers
            -- the element the polygon was built from and its parity says which
            -- kind that was.
            CASE WHEN kind = 'a' THEN num // 2 ELSE num END AS osm_id,
            CASE kind
                WHEN 'n' THEN 'node'
                WHEN 'w' THEN 'way'
                WHEN 'r' THEN 'relation'
                WHEN 'a' THEN IF(num % 2 = 0, 'way', 'relation')
            END                                         AS osm_type,
            tags,
            geometry
        FROM (
          SELECT
            left(id, 1)                                 AS kind,
            TRY_CAST(substr(id, 2) AS BIGINT)           AS num,
            CAST(properties AS MAP(VARCHAR, VARCHAR))   AS tags,
            ST_GeomFromGeoJSON(geometry)                AS geometry
          FROM read_json_auto(
            '{jsonseq}',
            format = 'newline_delimited',
            -- 256 MB per object. OSM multipolygon relations for complex
            -- landuse / natural areas can serialize to tens of MB of
            -- GeoJSON each (Alaska had a ~36 MB amenities_polygons row).
            maximum_object_size = 268435456,
            columns = {{'type': 'VARCHAR', 'id': 'VARCHAR',
                       'properties': 'JSON', 'geometry': 'JSON'}}
          )
        )
        WHERE geometry IS NOT NULL
    """)

    # tags-filter keeps the objects a match references so geometries can be
    # assembled, and export writes the tagged ones as features of their own
    # (coastline rings in boundaries, crossing nodes in railways). Re-apply
    # the theme's own filter to each row; see filter_predicate.
    where = (f"({filter_predicate(theme.osmium_filter)}) "
             f"AND ({POST_FILTERS.get(theme.name, 'TRUE')})")

    # One pass over the JSON view gives the row count, the extent (for both the
    # Hilbert box and the `geo` metadata) and the geometry types the file will
    # declare. It also lets the COPY below use a literal box instead of a CTE,
    # which drops one scan of the source.
    count, xmin, ymin, xmax, ymax, geom_types = con.execute(f"""
        SELECT COUNT(*),
               MIN(ST_XMin(geometry)), MIN(ST_YMin(geometry)),
               MAX(ST_XMax(geometry)), MAX(ST_YMax(geometry)),
               list(DISTINCT ST_GeometryType(geometry)::VARCHAR)
        FROM src WHERE {where}
    """).fetchone()
    if count == 0:
        if VERBOSE:
            print(f"    [{theme.name}] 0 features after filter, skipping")
        return 0

    typed_sql = ",\n            ".join(
        f"{expr} AS {col}" for col, expr in theme.typed_columns
    )

    out_parquet.parent.mkdir(parents=True, exist_ok=True)

    # DuckDB writes GeoParquet 2.0 directly (native Parquet GEOMETRY logical type),
    # Hilbert-ordered via the ST_Extent_Agg CTE. We also emit an explicit `bbox`
    # struct column: the native GEOMETRY type's row-group stats are only readable
    # by DuckDB-class engines, whereas a plain float bbox carries standard Parquet
    # min/max stats that *any* engine (Spark, Trino, polars, pyarrow) can use to
    # prune row groups via `WHERE bbox.xmin <= ... AND bbox.xmax >= ...`.
    #
    # Stored as FLOAT (not DOUBLE): float64 coordinates barely compress (+28% file
    # size on dense polygon themes vs +10% for float32). To keep the box a correct
    # outer bound we round each value OUTWARD (min down, max up) by a tiny relative
    # epsilon so it always contains the geometry — verified 0 violations, ~9 m max
    # slack, negligible for row-group pruning.
    #
    # The `covering` that points spec-aware readers at that bbox column is not
    # part of GeoParquet 2.0 yet, so DuckDB's writer does not emit it. We write
    # the whole `geo` value ourselves through KV_METADATA instead, which leaves
    # the native GEOMETRY logical type, the bloom filters and the file size
    # unchanged (gpio's `add bbox-metadata` rewrites the file at its own
    # compression level, +36% on RI roads).
    #
    # That is also why the COPY asks for GEOPARQUET_VERSION 'NONE' rather than
    # 'V2'. Under 'V2' DuckDB writes its own `geo` block *as well as* the one
    # passed through KV_METADATA, and the footer ends up carrying the key
    # twice, ours with the covering and DuckDB's without, with the winner left
    # to whichever entry the reader happens to keep. pyarrow, gpio inspect and
    # DuckDB's own reader all collapse the pair and show one, so it does not
    # surface until the footer is read entry by entry. Drop this once the
    # writer declares the covering itself.
    geo_metadata = json.dumps({
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in geom_types),
                "bbox": [xmin, ymin, xmax, ymax],
                "covering": {"bbox": {
                    "xmin": ["bbox", "xmin"], "ymin": ["bbox", "ymin"],
                    "xmax": ["bbox", "xmax"], "ymax": ["bbox", "ymax"],
                }},
            }
        },
    }).replace("'", "''")
    hilbert_box = (f"ST_Extent(ST_MakeEnvelope({xmin!r}, {ymin!r}, "
                   f"{xmax!r}, {ymax!r}))")

    con.execute(f"""
        COPY (
            SELECT
                osm_id,
                osm_type,
                ? AS country,
                ? AS state_name,
                ? AS state_iso,
                {typed_sql},
                tags,
                struct_pack(
                    xmin := (ST_XMin(geometry) - abs(ST_XMin(geometry)) * 1e-6 - 1e-9)::FLOAT,
                    ymin := (ST_YMin(geometry) - abs(ST_YMin(geometry)) * 1e-6 - 1e-9)::FLOAT,
                    xmax := (ST_XMax(geometry) + abs(ST_XMax(geometry)) * 1e-6 + 1e-9)::FLOAT,
                    ymax := (ST_YMax(geometry) + abs(ST_YMax(geometry)) * 1e-6 + 1e-9)::FLOAT
                ) AS bbox,
                geometry
            FROM src
            WHERE {where}
            ORDER BY ST_Hilbert(geometry, {hilbert_box})
        ) TO '{out_parquet}' (
            FORMAT PARQUET,
            -- 'NONE' governs the `geo` *metadata* only: the geometry column is
            -- still written with the native GEOMETRY logical type and its
            -- per-column geo statistics. See the note above the `geo` value.
            GEOPARQUET_VERSION 'NONE',
            COMPRESSION ZSTD,
            -- Level 15 (DuckDB default is 3): ~30% smaller files, mostly from
            -- the WKB geometry column, which is ~80% of every file and
            -- barely compresses at level 3. Write cost is a few seconds per
            -- theme; pruned reads are unaffected, full scans decompress
            -- ~50% slower but move 30% fewer bytes over HTTP. Measured on
            -- CT buildings/roads 2026-09-14; 19+ buys 11% more for 3x the
            -- write time and 2x slower full scans.
            COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE 50000,
            KV_METADATA {{geo: '{geo_metadata}'}}
        )
    """, [country, state_name, state_iso])

    if VERBOSE:
        size_mb = out_parquet.stat().st_size / (1024 * 1024)
        print(f"    [{theme.name}] {count:,} features, {size_mb:.1f} MB (v2.0 + bbox)")
    return count


def parquet_stats(con: duckdb.DuckDBPyConnection, path: Path) -> dict:
    """Extent and schema of a written theme file, recorded in the manifest so
    the Portolan catalog (scripts/catalog.py) is built without reading any
    parquet over the network.

    The extent comes from the `bbox` column, an outer bound by construction,
    rounded outward to 1e-6 degrees so it stays one. hive_partitioning is off:
    the path contains country=/state=, and DuckDB would otherwise add those
    keys to the schema as if they were columns of the file.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)

    src = f"read_parquet('{path}', hive_partitioning = false)"
    xmin, ymin, xmax, ymax = con.execute(f"""
        SELECT min(bbox.xmin), min(bbox.ymin), max(bbox.xmax), max(bbox.ymax)
        FROM {src}
    """).fetchone()
    columns = con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
    return {
        "bbox": [math.floor(xmin * 1e6) / 1e6, math.floor(ymin * 1e6) / 1e6,
                 math.ceil(xmax * 1e6) / 1e6, math.ceil(ymax * 1e6) / 1e6],
        "columns": [[name, col_type] for name, col_type, *_ in columns],
        "size_bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
    }


def pbf_timestamp(pbf: Path) -> str | None:
    """OSM replication timestamp from the PBF header (Geofabrik sets it), i.e.
    the moment the data reflects. None when the header does not carry it."""
    try:
        r = subprocess.run(
            ["osmium", "fileinfo", "-g",
             "header.option.osmosis_replication_timestamp", str(pbf)],
            check=True, capture_output=True, text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return r.stdout.strip() or None

# ---------- per-state orchestration ----------

def process_state(
    state: State,
    source_pbf: Path,
    work_dir: Path,
    out_dir: Path,
    themes: list[Theme],
    keep_intermediate: bool,
    verbose: bool = False,
    progress_queue=None,
    source_timestamp: str | None = None,
    relation_types: str = DEFAULT_RELATION_TYPES,
) -> dict:
    """Run clip + all themes for one state.

    If progress_queue is provided (a Manager().Queue), emit events so a
    parent process can drive a live multi-bar display:
      ('start',       iso, {'name': str, 'total': int})
      ('step_done',   iso, {'step': str})
      ('done',        iso, {'manifest': dict, 'duration': float})
    """
    global VERBOSE
    VERBOSE = verbose  # make subprocess echoing consistent in this worker

    def emit(kind: str, **data) -> None:
        if progress_queue is not None:
            progress_queue.put((kind, state.iso, data))

    t_state = time.time()

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    country = state.iso.split("-")[0]  # 'US-NY' -> 'US', 'CA-ON' -> 'CA'

    state_work = work_dir / state.iso
    state_work.mkdir(parents=True, exist_ok=True)

    state_poly_path = state_work / f"{state.iso}.geojson"
    state_pbf       = state_work / f"{state.iso}.osm.pbf"
    state_out_dir   = out_dir / f"country={country}" / f"state={state.iso}"
    state_out_dir.mkdir(parents=True, exist_ok=True)

    # 1 clip step + one step per theme
    emit("start", name=state.name, total=1 + len(themes))

    if verbose:
        print(f"\n=== {state.iso} ({state.name}) ===")

    write_state_polygon(state, state_poly_path)

    if not state_pbf.exists():
        if verbose:
            print(f"  [clip] {source_pbf.name} -> {state_pbf.name}")
        t0 = time.time()
        osmium_extract(source_pbf, state_poly_path, state_pbf, relation_types)
        if verbose:
            print(f"  [clip] done in {time.time()-t0:.1f}s "
                  f"({state_pbf.stat().st_size/(1024*1024):.1f} MB)")
    elif verbose:
        print(f"  [clip] reusing {state_pbf.name}")
    emit("step_done", step="clip")

    counts: dict[str, int] = {}
    theme_stats: dict[str, dict] = {}
    for theme in themes:
        t0 = time.time()
        theme_pbf     = state_work / f"{theme.name}.osm.pbf"
        theme_jsonseq = state_work / f"{theme.name}.geojsonseq"
        theme_parquet = state_out_dir / f"{theme.name}.parquet"

        try:
            osmium_tags_filter(state_pbf, theme.osmium_filter, theme_pbf)
            osmium_export(theme_pbf, theme.geometry_types, theme_jsonseq)
            n = write_theme_parquet(
                con, theme, theme_jsonseq, theme_parquet,
                country=country, state_name=state.name,
                state_iso=state.iso,
            )
            counts[theme.name] = n
            if n > 0:
                theme_stats[theme.name] = parquet_stats(con, theme_parquet)
            if verbose:
                print(f"    [{theme.name}] {time.time()-t0:.1f}s total")
        except subprocess.CalledProcessError as e:
            # Failures are always loud, even in quiet mode. Surface stderr
            # (captured by run() in quiet mode) so we don't have to replay.
            stderr = e.stderr.decode(errors="replace") if e.stderr else ""
            print(f"    [{state.iso}/{theme.name}] FAILED: {e}")
            if stderr.strip():
                print(f"      stderr: {stderr.strip()[:500]}")
            counts[theme.name] = -1

        if not keep_intermediate:
            theme_pbf.unlink(missing_ok=True)
            theme_jsonseq.unlink(missing_ok=True)
        emit("step_done", step=theme.name)

    duration = round(time.time() - t_state, 1)
    total_features = sum(c for c in counts.values() if c > 0)

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "country": country,
        "state_iso": state.iso,
        "state_name": state.name,
        "source_pbf": source_pbf.name,
        "source_timestamp": source_timestamp,
        "duration_s": duration,
        "total_features": total_features,
        "themes": counts,
        "theme_stats": theme_stats,
    }
    (state_out_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2))

    if not keep_intermediate:
        state_pbf.unlink(missing_ok=True)
        state_poly_path.unlink(missing_ok=True)
        try:
            state_work.rmdir()
        except OSError:
            pass

    emit("done", manifest=manifest, duration=duration)
    return manifest


# ---------- execution strategies ----------

def _bulk_extract(source_pbf: Path, states: list[State],
                  work_dir: Path, verbose: bool,
                  batch_size: int = 3,
                  relation_types: str = DEFAULT_RELATION_TYPES) -> None:
    """Osmium extract producing one state PBF per input state.

    osmium's `smart` strategy holds per-output bookkeeping in memory for
    every polygon opened in the same invocation, so peak memory follows the
    number of outputs per call. We batch into groups of `batch_size` — each
    batch is one scan of the source PBF with bounded memory; the source is
    read once per batch, not once per state.

    If every expected state PBF already exists (e.g. a previous run with
    --keep-intermediate), the whole thing is skipped.
    """
    expected = {s.iso: work_dir / s.iso / f"{s.iso}.osm.pbf" for s in states}

    if all(p.exists() and p.stat().st_size > 0 for p in expected.values()):
        print(f"Reusing {len(expected)} existing state PBFs in {work_dir}/")
        return

    # Write state polygons for osmium to clip against.
    state_polys: dict[str, Path] = {}
    for s in states:
        state_work = work_dir / s.iso
        state_work.mkdir(parents=True, exist_ok=True)
        poly = state_work / f"{s.iso}.geojson"
        write_state_polygon(s, poly)
        state_polys[s.iso] = poly

    batches = [
        dict(list(state_polys.items())[i:i + batch_size])
        for i in range(0, len(state_polys), batch_size)
    ]
    n_batches = len(batches)
    size_gb = source_pbf.stat().st_size / 1e9

    def run_batches() -> None:
        for i, batch in enumerate(batches, 1):
            if verbose:
                print(f"  batch {i}/{n_batches} ({len(batch)} states)")
            osmium_extract_batch(source_pbf, batch, work_dir, relation_types)

    label = (f"Bulk-extracting {len(state_polys)} state PBFs from "
             f"{source_pbf.name} ({size_gb:.1f} GB) — {n_batches} scans "
             f"of {batch_size} states each")

    t0 = time.time()
    if verbose:
        print(f"\n{label}")
        run_batches()
    else:
        from rich.console import Console
        from rich.progress import (
            Progress, SpinnerColumn, BarColumn, TextColumn,
            MofNCompleteColumn, TimeElapsedColumn,
        )
        console = Console()
        with Progress(
            SpinnerColumn(style="cyan"),
            TextColumn("[cyan]{task.description}[/cyan]"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"bulk-extract ({n_batches} scans × {batch_size} states)",
                total=n_batches,
            )
            for i, batch in enumerate(batches, 1):
                osmium_extract_batch(source_pbf, batch, work_dir, relation_types)
                progress.advance(task)
        console.print(
            f"[green]✓[/green] bulk-extract — "
            f"{len(state_polys)} state PBFs in {n_batches} scans, "
            f"{time.time()-t0:.1f}s total"
        )


def _run_verbose(states: list[State], kwargs_common: dict, workers: int) -> None:
    """Plain stdout log, one summary line per state as in the earlier UI."""
    total = len(states)

    def report(i: int, m: dict) -> None:
        print(f"[{i:3d}/{total}]  {m['state_iso']:7s} {m['state_name']:30.30s}  "
              f"{m['total_features']:>11,} features  {m['duration_s']:>6.1f}s")

    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(process_state, state=s, **kwargs_common) for s in states]
            for i, f in enumerate(as_completed(futures), 1):
                report(i, f.result())
    else:
        for i, state in enumerate(states, 1):
            report(i, process_state(state=state, **kwargs_common))


def _run_with_progress(states: list[State], kwargs_common: dict, workers: int) -> None:
    """Docker-style stacked bars via rich.Progress.

    Completed states scroll up as ✓ log lines; currently-running states
    keep a spinner + bar at the bottom of the screen until they finish.
    """
    from rich.console import Console
    from rich.progress import (
        Progress, SpinnerColumn, BarColumn, TextColumn,
        MofNCompleteColumn, TimeElapsedColumn,
    )

    console = Console(log_path=False, log_time_format="%H:%M:%S")
    total = len(states)
    done_count = [0]  # mutable cell for closures

    progress = Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[cyan]{task.fields[iso]:<7}[/cyan]"),
        TextColumn("{task.fields[name]:<22.22}"),
        BarColumn(bar_width=20),
        MofNCompleteColumn(),
        TextColumn("{task.fields[step]:<18.18}", style="dim"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    )

    stop = threading.Event()
    q = Manager().Queue()
    tasks: dict[str, int] = {}  # iso -> TaskID

    def drainer() -> None:
        while not stop.is_set() or not q.empty():
            try:
                kind, iso, data = q.get(timeout=0.1)
            except Empty:
                continue
            if kind == "start":
                tasks[iso] = progress.add_task(
                    description="",
                    total=data["total"],
                    iso=iso,
                    name=data["name"],
                    step="clipping…",
                )
            elif kind == "step_done":
                tid = tasks.get(iso)
                if tid is not None:
                    progress.update(tid, advance=1, step=f"→ {data['step']}")
            elif kind == "done":
                tid = tasks.pop(iso, None)
                if tid is not None:
                    progress.remove_task(tid)
                done_count[0] += 1
                m = data["manifest"]
                console.print(
                    f"[green]✓[/green] [{done_count[0]:>3}/{total}]  "
                    f"[cyan]{iso:<7}[/cyan] {m['state_name']:<22.22}  "
                    f"{m['total_features']:>11,} features  "
                    f"{data['duration']:>6.1f}s"
                )

    thread = threading.Thread(target=drainer, daemon=True)
    thread.start()

    try:
        with progress:
            kw = {**kwargs_common, "progress_queue": q}
            if workers > 1:
                with ProcessPoolExecutor(max_workers=workers) as ex:
                    futures = [ex.submit(process_state, state=s, **kw) for s in states]
                    for f in as_completed(futures):
                        f.result()  # re-raise on worker failure
            else:
                for state in states:
                    process_state(state=state, **kw)
    finally:
        stop.set()
        thread.join(timeout=5)


# ---------- main ----------

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source-pbf",     required=True, type=Path)
    p.add_argument("--states-geojson", required=True, type=Path,
                   help="Single GeoJSON FeatureCollection with all state polygons. "
                        "Each feature must have 'ISO3166-2' and 'name' properties.")
    p.add_argument("--out-dir",        required=True, type=Path)
    p.add_argument("--states",         nargs="*",
                   help="Subset of admin-region ISO codes, e.g. US-MA CA-ON. Default: all in geojson.")
    p.add_argument("--themes",         nargs="*",
                   help="Subset of theme names. Default: all in themes.py.")
    p.add_argument("--keep-intermediate", action="store_true")
    p.add_argument("--workers", type=int, default=1,
                   help="Process states in parallel with N workers. "
                        "Mind CPU/disk I/O: osmium extract is already multithreaded. "
                        "Sweet spot is usually 2-3 on a laptop, 4-6 on a fat box.")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Echo every subprocess command and per-theme status. "
                        "Default is quiet — one line per completed state.")
    p.add_argument("--extract-batch-size", type=int, default=3,
                   help="States per osmium-extract invocation during bulk extract. "
                        "Peak memory follows outputs per call (osmium holds "
                        "membership state for every output at once). Measured "
                        "with -s smart on Mexico's 32 states, 16 GB runner: "
                        "5 per call 14.9 GB, 3 per call 10.8 GB in the same "
                        "time, 2 per call 7.3 GB and ~30%% slower. Raise if you "
                        "have more RAM, lower if less.")
    p.add_argument("--complete-relations", default=DEFAULT_RELATION_TYPES,
                   metavar="TYPES",
                   help="Relation types the clip completes with all their members "
                        "(osmium extract -S types=...). Default: multipolygon. The "
                        "parent-extract boundaries job passes multipolygon,boundary "
                        "so a state relation whose member ways lie outside the "
                        "region polygon still assembles (issue #5).")
    args = p.parse_args()

    global VERBOSE
    VERBOSE = args.verbose

    all_states = load_states(args.states_geojson)
    if args.states:
        wanted = set(args.states)
        states = [s for s in all_states if s.iso in wanted]
        missing = wanted - {s.iso for s in states}
        if missing:
            sys.exit(f"States not found in geojson: {', '.join(sorted(missing))}")
    else:
        states = all_states

    if args.themes:
        wanted = set(args.themes)
        themes = [t for t in THEMES if t.name in wanted]
        missing = wanted - {t.name for t in themes}
        if missing:
            sys.exit(f"Unknown themes: {', '.join(sorted(missing))}")
    else:
        themes = list(THEMES)

    print(f"Source PBF: {args.source_pbf}")
    print(f"States:     {', '.join(s.iso for s in states)}")
    print(f"Themes:     {', '.join(t.name for t in themes)}")
    print(f"Output:     {args.out_dir}")

    work_dir = args.out_dir / "_work"
    work_dir.mkdir(parents=True, exist_ok=True)

    kwargs_common = dict(
        source_pbf=args.source_pbf,
        work_dir=work_dir,
        out_dir=args.out_dir,
        themes=themes,
        keep_intermediate=args.keep_intermediate,
        verbose=args.verbose,
        source_timestamp=pbf_timestamp(args.source_pbf),
        relation_types=args.complete_relations,
    )

    if args.workers > 1:
        print(f"Workers:    {args.workers}")
    print()

    t0 = time.time()

    # Stage 0: one scan of the source PBF produces per-state PBFs for every
    # requested state. Orders of magnitude faster than re-scanning per state.
    _bulk_extract(args.source_pbf, states, work_dir,
                  verbose=args.verbose, batch_size=args.extract_batch_size,
                  relation_types=args.complete_relations)

    # Stage 1: per-state theme processing. process_state now just reuses
    # the state_pbf that bulk extract already wrote.
    if args.verbose:
        _run_verbose(states, kwargs_common, args.workers)
    else:
        _run_with_progress(states, kwargs_common, args.workers)
    print(f"\nDone in {time.time()-t0:.1f}s")

    if not args.keep_intermediate:
        try:
            shutil.rmtree(work_dir)
        except OSError:
            pass


if __name__ == "__main__":
    main()
