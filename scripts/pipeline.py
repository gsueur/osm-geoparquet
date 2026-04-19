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
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import duckdb

from themes import THEMES, POST_FILTERS, Theme

SCHEMA_VERSION = "0.1.0"


# ---------- shell helpers ----------

def run(cmd: list[str]) -> None:
    print(f"    $ {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def osmium_extract(src_pbf: Path, poly: Path, out_pbf: Path) -> None:
    run([
        "osmium", "extract",
        "-p", str(poly),
        "-s", "complete_ways",
        str(src_pbf),
        "-o", str(out_pbf),
        "--overwrite",
    ])


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
            maximum_object_size = 33554432,
            columns = {{'type': 'VARCHAR', 'id': 'VARCHAR',
                       'properties': 'JSON', 'geometry': 'JSON'}}
        )
        WHERE geometry IS NOT NULL
    """)

    where = POST_FILTERS.get(theme.name, "TRUE")

    count = con.execute(f"SELECT COUNT(*) FROM src WHERE {where}").fetchone()[0]
    if count == 0:
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
) -> dict:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    country = state.iso.split("-")[0]  # 'US-NY' -> 'US', 'CA-ON' -> 'CA'

    state_work = work_dir / state.iso
    state_work.mkdir(parents=True, exist_ok=True)

    state_poly_path = state_work / f"{state.iso}.geojson"
    state_pbf       = state_work / f"{state.iso}.osm.pbf"
    state_out_dir   = out_dir / f"country={country}" / f"state={state.iso}"
    state_out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n=== {state.iso} ({state.name}) ===")

    write_state_polygon(state, state_poly_path)

    if not state_pbf.exists():
        print(f"  [clip] {source_pbf.name} -> {state_pbf.name}")
        t0 = time.time()
        osmium_extract(source_pbf, state_poly_path, state_pbf)
        print(f"  [clip] done in {time.time()-t0:.1f}s "
              f"({state_pbf.stat().st_size/(1024*1024):.1f} MB)")
    else:
        print(f"  [clip] reusing {state_pbf.name}")

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
            print(f"    [{theme.name}] {time.time()-t0:.1f}s total")
        except subprocess.CalledProcessError as e:
            print(f"    [{theme.name}] FAILED: {e}")
            counts[theme.name] = -1

        if not keep_intermediate:
            theme_pbf.unlink(missing_ok=True)
            theme_jsonseq.unlink(missing_ok=True)

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "country": country,
        "state_iso": state.iso,
        "state_name": state.name,
        "source_pbf": source_pbf.name,
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

    return manifest


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
    args = p.parse_args()

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
    )

    t0 = time.time()
    if args.workers > 1:
        print(f"Workers:    {args.workers}")
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(process_state, state=s, **kwargs_common): s for s in states}
            for f in as_completed(futures):
                f.result()  # re-raise on worker failure
    else:
        for state in states:
            process_state(state=state, **kwargs_common)
    print(f"\nDone in {time.time()-t0:.1f}s")

    if not args.keep_intermediate:
        try:
            shutil.rmtree(work_dir)
        except OSError:
            pass


if __name__ == "__main__":
    main()
