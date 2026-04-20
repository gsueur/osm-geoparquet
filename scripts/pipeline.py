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
import json
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

from themes import THEMES, POST_FILTERS, Theme

SCHEMA_VERSION = "0.1.0"

# Set by main(); workers inherit it through fork. True = echo every subprocess
# command + per-theme status; False = quiet, main prints one progress line
# per completed state.
VERBOSE = False


# ---------- shell helpers ----------

def run(cmd: list[str]) -> None:
    if VERBOSE:
        print(f"    $ {' '.join(cmd)}")
    subprocess.run(cmd, check=True, capture_output=not VERBOSE)

def osmium_extract(src_pbf: Path, poly: Path, out_pbf: Path) -> None:
    run([
        "osmium", "extract",
        "-p", str(poly),
        "-s", "complete_ways",
        str(src_pbf),
        "-o", str(out_pbf),
        "--overwrite",
    ])


def osmium_extract_batch(
    src_pbf: Path,
    state_polys: dict[str, Path],
    work_dir: Path,
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
    run([
        "osmium", "extract",
        "-c", str(config_path),
        "-d", str(work_dir),
        "-s", "complete_ways",
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
            TRY_CAST(id AS BIGINT)                      AS osm_id,
            CAST(type AS VARCHAR)                       AS osm_type,
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
        WHERE geometry IS NOT NULL
    """)

    where = POST_FILTERS.get(theme.name, "TRUE")

    count = con.execute(f"SELECT COUNT(*) FROM src WHERE {where}").fetchone()[0]
    if count == 0:
        if VERBOSE:
            print(f"    [{theme.name}] 0 features after filter, skipping")
        return 0

    typed_sql = ",\n            ".join(
        f"{expr} AS {col}" for col, expr in theme.typed_columns
    )

    out_parquet.parent.mkdir(parents=True, exist_ok=True)

    # DuckDB writes GeoParquet 2.0 directly (native Parquet GEOMETRY logical type).
    # Hilbert-ordered via ST_Extent_Agg CTE; no sidecar bbox column needed in v2.0.
    con.execute(f"""
        COPY (
            WITH extent AS (
                SELECT ST_Extent(ST_Extent_Agg(geometry)) AS box
                FROM src
                WHERE {where}
            )
            SELECT
                osm_id,
                osm_type,
                ? AS country,
                ? AS state,
                ? AS state_iso,
                {typed_sql},
                tags,
                geometry
            FROM src, extent
            WHERE {where}
            ORDER BY ST_Hilbert(geometry, extent.box)
        ) TO '{out_parquet}' (
            FORMAT PARQUET,
            GEOPARQUET_VERSION 'V2',
            COMPRESSION ZSTD,
            ROW_GROUP_SIZE 50000
        )
    """, [country, state_name, state_iso])

    if VERBOSE:
        size_mb = out_parquet.stat().st_size / (1024 * 1024)
        print(f"    [{theme.name}] {count:,} features, {size_mb:.1f} MB (v2.0)")
    return count

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
        osmium_extract(source_pbf, state_poly_path, state_pbf)
        if verbose:
            print(f"  [clip] done in {time.time()-t0:.1f}s "
                  f"({state_pbf.stat().st_size/(1024*1024):.1f} MB)")
    elif verbose:
        print(f"  [clip] reusing {state_pbf.name}")
    emit("step_done", step="clip")

    counts: dict[str, int] = {}
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
        "duration_s": duration,
        "total_features": total_features,
        "themes": counts,
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
                  batch_size: int = 25) -> None:
    """Osmium extract producing one state PBF per input state.

    osmium's `complete_ways` strategy holds per-output bookkeeping in memory
    for every polygon opened in the same invocation, so a single pass over
    a 19 GB source with 100+ continental polygons can need 20+ GB of RAM.
    We batch into groups of `batch_size` (default 25) — each batch is one
    scan of the source PBF with bounded memory; the source is read once
    per batch, not once per state, so total cost is still orders of
    magnitude less than per-state extracts.

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
            osmium_extract_batch(source_pbf, batch, work_dir)

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
                osmium_extract_batch(source_pbf, batch, work_dir)
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
    p.add_argument("--extract-batch-size", type=int, default=25,
                   help="States per osmium-extract invocation during bulk extract. "
                        "Lower if memory-constrained (each output holds ~200 MB of "
                        "per-polygon bookkeeping in complete_ways mode); higher if "
                        "you have plenty of RAM and want fewer source-PBF scans. "
                        "Default 25 (~5 GB RAM for continental-scale polygons).")
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
    )

    if args.workers > 1:
        print(f"Workers:    {args.workers}")
    print()

    t0 = time.time()

    # Stage 0: one scan of the source PBF produces per-state PBFs for every
    # requested state. Orders of magnitude faster than re-scanning per state.
    _bulk_extract(args.source_pbf, states, work_dir,
                  verbose=args.verbose, batch_size=args.extract_batch_size)

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
