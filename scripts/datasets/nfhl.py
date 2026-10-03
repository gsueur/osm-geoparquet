#!/usr/bin/env python3
"""
FEMA National Flood Hazard Layer, one file per county delivery, updated daily.

FEMA publishes the NFHL one county-wide delivery at a time (a DFIRM, e.g.
12086C), as zipped shapefiles, and replaces a delivery whenever it changes.
The published layout follows that unit, so a day's update rewrites only the
few deliveries FEMA republished:

  <out>/latest/state=<XX>/<DFIRM_ID>.parquet   one delivery: its flood zones cut into
                                               pieces of at most 100 vertices, sorted
                                               along a Hilbert curve, bbox covering
  <out>/latest/counties.parquet                the index: one row per delivery, its
                                               bbox, date, rows, size and sha256. A
                                               reader filters it, then opens 1 to 3 files
  <out>/latest/state=<XX>/_manifest.json       the state's deliveries as themes, the
                                               parquetry repository contract
  <out>/latest/_manifest.json, changes.json, skipped.json, ATTRIBUTION.txt
  <out>/index.json, snapshots.json             the repository files: one dataset per
                                               state; latest/ plus a frozen copy a month

Reading, normalizing and cutting a delivery is the pipeline of
https://github.com/gsueur/nfhl-geoparquet-workshop (imported by `update`
only, from its own environment). This script packages its per-county output
and keeps the index. Nothing is changed but the order: every file holds the
same pieces and geometries (hashed) as the pipeline's.

  # once: package a whole local build (the pipeline's silver_subdivided/)
  python3 scripts/datasets/nfhl.py bootstrap --subdivided /Volumes/T9/DATA/nfhl/silver_subdivided \\
      --control-db /Volumes/T9/DATA/nfhl/control/fema_control.duckdb --out-dir out/nfhl
  # daily: what FEMA republished since the published index
  uv run --project ../nfhl-geoparquet-workshop --with pycountry \\
      python scripts/datasets/nfhl.py update --out-dir out/nfhl
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pycountry

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.infra import GEOMETRY_TYPE_NAMES

STEM = "flood_hazard_areas"
PUBLIC = "https://parquetry.geomermaids.com/nfhl"
LATEST = "latest"
INDEX = "counties.parquet"
CRS = "EPSG:4269"          # NAD83, as FEMA delivers it
CRS_CODE = 4269
# 20,480 pieces are about 8 MB: a point lookup reads one group.
ROW_GROUP_ROWS = 20_480
ZSTD_LEVEL = 9            # see clc.py: 15 costs 11x the time for nothing with DuckDB's writer
COLUMNS = """state county dfirm_id fema_update_date source_feature_id piece_id
             flood_zone zone_subtype sfha static_bfe dual_zone risk floodplain subzone""".split()
BBOX = ("{xmin: ST_XMin(geometry), ymin: ST_YMin(geometry), "
        "xmax: ST_XMax(geometry), ymax: ST_YMax(geometry)}")
# FEMA lists about 2,500 county-wide deliveries. Far fewer means the portal
# page came back truncated, not that counties were withdrawn.
MIN_LISTED = 2_400
KEEP_SNAPSHOTS = 3        # frozen monthly copies; older ones are pruned
KEEP_RUNS = 120           # runs kept in changes.json
NO_LAYER = "not in archive"   # the pipeline's error for a delivery without S_FLD_HAZ_AR
# The index columns, in order. path is relative to latest/.
INDEX_COLUMNS = {
    "state": "VARCHAR", "county": "VARCHAR", "dfirm_id": "VARCHAR",
    "fema_update_date": "DATE", "zip_name": "VARCHAR", "path": "VARCHAR",
    "features": "BIGINT", "zones": "BIGINT", "shared_ids": "BIGINT", "bytes": "BIGINT",
    "row_groups": "INTEGER", "largest_row_group_bytes": "BIGINT", "sha256": "VARCHAR",
    "xmin": "DOUBLE", "ymin": "DOUBLE", "xmax": "DOUBLE", "ymax": "DOUBLE",
    "published_at": "TIMESTAMP",
}


def state_name(code: str) -> str:
    return pycountry.subdivisions.get(code=f"US-{code}").name


def connect(work_dir: Path, memory_limit: str) -> duckdb.DuckDBPyConnection:
    work_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET temp_directory = '{work_dir}'; SET memory_limit = '{memory_limit}'")
    return con


def projjson(con: duckdb.DuckDBPyConnection) -> dict:
    """The CRS as PROJJSON, for the `geo` footer: DuckDB's V2 writer puts it
    in the footer of a probe file."""
    with tempfile.TemporaryDirectory() as tmp:
        probe = Path(tmp) / "probe.parquet"
        con.execute(f"""COPY (SELECT ST_SetCRS(ST_Point(0, 0), '{CRS}') AS g)
                        TO '{probe}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')""")
        geo = con.execute(f"SELECT decode(value) FROM parquet_kv_metadata('{probe}') "
                          "WHERE key = 'geo'").fetchone()[0]
    crs = json.loads(geo)["columns"]["g"]["crs"]
    assert crs["id"] == {"authority": "EPSG", "code": CRS_CODE}, crs.get("id")
    return crs


def write(con: duckdb.DuckDBPyConnection, query: str, dest: Path, crs: dict) -> None:
    """One GeoParquet 2.0 file: native GEOMETRY typed EPSG:4269, a `geo`
    footer with the bbox covering, row groups of ROW_GROUP_ROWS."""
    xmin, ymin, xmax, ymax, types = con.execute(f"""
        SELECT min(bbox.xmin), min(bbox.ymin), max(bbox.xmax), max(bbox.ymax),
               list(DISTINCT ST_GeometryType(geometry)::VARCHAR)
        FROM ({query})""").fetchone()
    geo = json.dumps({
        "version": "2.0.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB",
            "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in types),
            "crs": crs,
            "bbox": [xmin, ymin, xmax, ymax],
            "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}},
        }},
    }).replace("'", "''")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    con.execute(f"""
        COPY ({query}) TO '{tmp}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE',
            COMPRESSION ZSTD, COMPRESSION_LEVEL {ZSTD_LEVEL}, ROW_GROUP_SIZE {ROW_GROUP_ROWS},
            KV_METADATA {{geo: '{geo}'}})""")
    os.replace(tmp, dest)


def verify(con: duckdb.DuckDBPyConnection, path: Path) -> dict:
    """Fail unless the file is what its footer says; return its size facts."""
    geos = [v for k, v in con.execute("SELECT key::VARCHAR, decode(value) FROM parquet_kv_metadata(?)",
                                      [str(path)]).fetchall() if k == "geo"]
    assert len(geos) == 1, f"{path}: {len(geos)} geo keys"
    col = json.loads(geos[0])["columns"]["geometry"]
    assert col["crs"]["id"]["code"] == CRS_CODE, f"{path}: crs {col['crs'].get('id')}"
    logical = con.execute("SELECT logical_type FROM parquet_schema(?) WHERE name = 'geometry'",
                          [str(path)]).fetchone()[0]
    assert logical and logical.startswith("GeometryType(crs=") and str(CRS_CODE) in logical, \
        f"{path}: geometry is {str(logical)[:80]}"
    groups = [n for _, n in con.execute("""
        SELECT DISTINCT row_group_id, row_group_num_rows FROM parquet_metadata(?)
        ORDER BY row_group_id""", [str(path)]).fetchall()]
    assert all(n == ROW_GROUP_ROWS for n in groups[:-1]), f"{path}: row groups {groups[:5]}"
    rows, bad, typ = con.execute(f"""
        SELECT count(*),
               count(*) FILTER (WHERE bbox.xmin > ST_XMin(geometry) OR bbox.ymin > ST_YMin(geometry)
                                   OR bbox.xmax < ST_XMax(geometry) OR bbox.ymax < ST_YMax(geometry)),
               any_value(typeof(geometry))
        FROM read_parquet('{path}')""").fetchone()
    assert bad == 0, f"{path}: {bad} bboxes do not contain their geometry"
    assert typ.upper() == f"GEOMETRY('{CRS}')", f"{path}: reads as {typ}"
    largest = con.execute("""SELECT max(b) FROM (SELECT sum(total_compressed_size) AS b
                             FROM parquet_metadata(?) GROUP BY row_group_id)""", [str(path)]).fetchone()[0]
    return {"features": rows, "bytes": path.stat().st_size, "row_groups": len(groups),
            "largest_row_group_bytes": largest, "sha256": sha256(path)}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 24), b""):
            h.update(chunk)
    return h.hexdigest()


def fingerprint(con: duckdb.DuckDBPyConnection, path: Path | str) -> tuple:
    """What must not change: the pieces, the zones (each has a piece 0) and the geometries."""
    return con.execute(f"""
        SELECT count(*), count(*) FILTER (WHERE piece_id = 0),
               sum(hash(ST_AsWKB(geometry))::HUGEINT)
        FROM read_parquet('{path}')""").fetchone()


def sql_path(path: Path | str) -> str:
    """A path inside a SQL literal: county names carry apostrophes (O'Brien)."""
    return str(path).replace("'", "''")


def package(con: duckdb.DuckDBPyConnection, src: Path, latest: Path, crs: dict,
            zip_name: str | None) -> dict:
    """One delivery of the pipeline (silver_subdivided/state=XX/county=Name.parquet)
    to latest/state=XX/<DFIRM_ID>.parquet. Returns its index row."""
    s = sql_path(src)
    facts = con.execute(f"""
        SELECT count(DISTINCT state), any_value(state), count(DISTINCT county), any_value(county),
               count(DISTINCT dfirm_id), any_value(dfirm_id),
               count(DISTINCT fema_update_date), any_value(fema_update_date)::VARCHAR
        FROM read_parquet('{s}')""").fetchone()
    assert facts[0] == facts[2] == facts[4] == facts[6] == 1, f"{src}: not one delivery: {facts}"
    state, county, dfirm, date = facts[1], facts[3], facts[5], facts[7]
    rel = f"state={state}/{dfirm}.parquet"
    dest = latest / rel
    before = fingerprint(con, s)
    write(con, f"""
        SELECT {", ".join(COLUMNS)}, ST_SetCRS(geometry, '{CRS}') AS geometry, {BBOX} AS bbox
        FROM read_parquet('{s}')
        ORDER BY ST_Hilbert(geometry,
                            (SELECT ST_Extent(ST_Extent_Agg(geometry)) FROM read_parquet('{s}')))
    """, dest, crs)
    entry = verify(con, dest)
    after = fingerprint(con, dest)
    assert after == before, f"{dest}: {after} differs from the pipeline's {before}"
    # FEMA's FLD_AR_ID is not unique inside every county.
    shared, xmin, ymin, xmax, ymax = con.execute(f"""
        SELECT (SELECT coalesce(sum(n), 0) FROM (
                    SELECT count(*) AS n FROM read_parquet('{dest}') WHERE piece_id = 0
                    GROUP BY source_feature_id HAVING n > 1)),
               min(bbox.xmin), min(bbox.ymin), max(bbox.xmax), max(bbox.ymax)
        FROM read_parquet('{dest}')""").fetchone()
    return {"state": state, "county": county, "dfirm_id": dfirm, "fema_update_date": date,
            "zip_name": zip_name, "path": rel, **entry, "zones": before[1], "shared_ids": int(shared),
            "xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
            "published_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")}


# ---------- the index and the repository files ----------

def read_index(con: duckdb.DuckDBPyConnection, source: str) -> list[dict]:
    cols = ", ".join(f"{c}::VARCHAR AS {c}" if t in ("DATE", "TIMESTAMP") else c
                     for c, t in INDEX_COLUMNS.items())
    cur = con.execute(f"SELECT {cols} FROM read_parquet('{source}') ORDER BY path")
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


def write_index(con: duckdb.DuckDBPyConnection, rows: list[dict], dest: Path, crs: dict) -> None:
    """counties.parquet: one row per delivery, its bbox as the geometry."""
    with tempfile.TemporaryDirectory() as tmp:
        src = Path(tmp) / "index.json"
        src.write_text(json.dumps(rows))
        spec = "{" + ", ".join(f"'{c}': '{t}'" for c, t in INDEX_COLUMNS.items()) + "}"
        con.execute(f"CREATE OR REPLACE TEMP TABLE idx AS "
                    f"SELECT * FROM read_json('{src}', columns = {spec}, format = 'array')")
    write(con, f"""
        SELECT {", ".join(INDEX_COLUMNS)},
               ST_SetCRS(ST_MakeEnvelope(xmin, ymin, xmax, ymax), '{CRS}') AS geometry,
               {{xmin: xmin, ymin: ymin, xmax: xmax, ymax: ymax}} AS bbox
        FROM idx ORDER BY state, dfirm_id""", dest, crs)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
    assert n == len(rows), f"{dest}: {n} rows, expected {len(rows)}"


def get_json(url: str, default):
    if not url.startswith(("http://", "https://")):
        path = Path(url)
        return json.loads(path.read_text()) if path.exists() else default
    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return default
        raise


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def write_repository(con: duckdb.DuckDBPyConnection, out: Path, rows: list[dict], crs: dict,
                     today: str, snapshots: list[dict], changes: dict, skipped: dict) -> None:
    """Everything but the delivery files, rebuilt from the index rows."""
    latest = out / LATEST
    rows = sorted(rows, key=lambda r: r["path"])
    paths = [r["path"] for r in rows]
    assert len(set(paths)) == len(paths), "two index rows for one file"
    write_index(con, rows, latest / INDEX, crs)

    by_state: dict[str, list[dict]] = {}
    for r in rows:
        by_state.setdefault(r["state"], []).append(r)
    datasets = []
    for code, entries in sorted(by_state.items()):
        name = state_name(code)
        write_json(latest / f"state={code}" / "_manifest.json", {
            "country": "US", "state": code, "state_name": name, "updated": today,
            "total_features": sum(e["features"] for e in entries),
            # The workbench opens <theme>.parquet beside the manifest.
            "themes": {e["dfirm_id"]: e["features"] for e in entries},
            "files": [{"file": e["path"].split("/", 1)[1], "county": e["county"],
                       "dfirm_id": e["dfirm_id"], "fema_update_date": e["fema_update_date"],
                       "features": e["features"], "zones": e["zones"], "bytes": e["bytes"],
                       "row_groups": e["row_groups"],
                       "largest_row_group_bytes": e["largest_row_group_bytes"],
                       "sha256": e["sha256"]} for e in entries],
        })
        datasets.append({"path": f"state={code}", "code": code, "name": name})

    dates = sorted(r["fema_update_date"] for r in rows)
    total = sum(r["features"] for r in rows)
    zones = sum(r["zones"] for r in rows)
    shared = sum(r["shared_ids"] for r in rows)
    shared_in = sum(1 for r in rows if r["shared_ids"])
    write_json(latest / "_manifest.json", {
        "state_name": "FEMA NFHL flood hazard areas, United States",
        "updated": today, "source": "FEMA National Flood Hazard Layer, S_FLD_HAZ_AR",
        "total_features": total, "zones": zones, "county_deliveries": len(rows),
        "zones_sharing_a_source_feature_id": shared, "deliveries_with_shared_ids": shared_in,
        "fema_update_dates": [dates[0], dates[-1]],
        "bytes": sum(r["bytes"] for r in rows), "index": INDEX,
        "states": {code: sum(e["features"] for e in es) for code, es in sorted(by_state.items())},
        "themes": {},
    })
    write_json(latest / "changes.json", changes)
    write_json(latest / "skipped.json", skipped)
    (latest / "ATTRIBUTION.txt").write_text(attribution(
        today, total, zones, len(rows), len(by_state), dates[0], dates[-1], shared, shared_in))
    write_json(out / "index.json", {"datasets": datasets})
    write_json(out / "snapshots.json", {"latest": f"{LATEST}/", "snapshots": snapshots})


def plan_snapshots(out: Path, published: list[dict], today: str) -> list[dict]:
    """A frozen copy of latest/ on the first run of each month, KEEP_SNAPSHOTS
    kept. The workflow makes the copy (server side) and the prune from the
    two files written here."""
    snaps = [s for s in published if s["path"] != f"{LATEST}/"]
    new = not any(s["date"][:7] == today[:7] for s in snaps)
    if new:
        snaps.append({"date": today, "path": f"{today}/"})
    snaps.sort(key=lambda s: s["date"], reverse=True)
    (out / "snapshot_new.txt").write_text(f"{today}/\n" if new else "")
    (out / "snapshot_prune.txt").write_text("".join(s["path"] + "\n" for s in snaps[KEEP_SNAPSHOTS:]))
    return snaps[:KEEP_SNAPSHOTS]


def attribution(today: str, pieces: int, zones: int, counties: int, states: int,
                first: str, last: str, shared: int, shared_in: int) -> str:
    return f"""\
# Data attribution

This data is derived from the National Flood Hazard Layer (NFHL) of the
Federal Emergency Management Agency (FEMA), U.S. Department of Homeland
Security.

  Source:       https://msc.fema.gov/portal/home (FEMA Flood Map Service Center)
                https://hazards.fema.gov/femaportal/NFHL/searchResult
  Product:      NFHL, layer S_FLD_HAZ_AR (flood hazard areas), the
                county-wide deliveries FEMA lists
  Producer:     Federal Emergency Management Agency (FEMA)
  Licence:      U.S. federal government work, public domain. No restriction
                on use or redistribution.
  Attribution:  FEMA National Flood Hazard Layer

# Not for official use

These files are a repackaging, not a FEMA product. They are not the
effective Flood Insurance Rate Map and must not be used for flood zone
determinations, insurance rating, regulatory or legal purposes: for those,
use the FEMA Flood Map Service Center. FEMA has not reviewed, approved or
endorsed these files. FEMA updates the NFHL continuously: these files are
checked against FEMA's list every day, and a delivery FEMA republished is
replaced (last check: {today}). Each row carries the date of its delivery
(`fema_update_date`, from {first} to {last}); changes.json lists what each
day replaced.

# Coverage

{counties:,} county-wide deliveries in {states} states and territories: every
county-wide dataset FEMA lists that holds a flood hazard layer
(skipped.json names those that hold none). Counties FEMA has not mapped
digitally are absent, as are the community-level deliveries (a town or
city published on its own). An absent area is unmapped here, not free of
flood hazard.

# What was changed

  1. Pieces. Flood zone polygons reach hundreds of thousands of vertices.
     Each one is cut into pieces of at most 100 vertices (ST_Subdivide), so
     a point lookup tests a small polygon: {zones:,} zones make {pieces:,}
     pieces. A row is a piece, not a zone: `piece_id` numbers the pieces
     of a zone from 0, so `piece_id = 0` counts zones. `source_feature_id`
     is FEMA's FLD_AR_ID, which FEMA does not keep unique inside every
     delivery: {shared:,} zones in {shared_in:,} deliveries share theirs with
     another zone. Elsewhere (`dfirm_id`, `source_feature_id`) identifies
     the zone and dissolving on it gives the original outline back.
     Invalid source geometries were made valid first.
  2. Columns. `flood_zone` (FLD_ZONE), `zone_subtype` (ZONE_SUBTY), `sfha`
     (SFHA_TF), `static_bfe` (STATIC_BFE, NULL where FEMA writes -9999),
     `dual_zone` (DUAL_ZONE), `dfirm_id` and `source_feature_id`
     (FLD_AR_ID) are FEMA's values. `state` (postal code), `county` and
     `fema_update_date` come from the delivery. Three columns are not part
     of the NFHL, they are a reading of FEMA's zone and subtype added by
     Geomermaids: `risk` (1 percent flood zone, 0.2 percent flood zone,
     minimal, undetermined, water, unmapped), `floodplain` (the same as
     `sfha`) and `subzone` (the zone of a dual or AR zone).
  3. Layout. One file per delivery, state=<XX>/<DFIRM_ID>.parquet, sorted
     along a Hilbert curve in row groups of {ROW_GROUP_ROWS:,} pieces with a
     covering bbox column. counties.parquet indexes them: filter it on
     bbox or state, then read the files it names.

Coordinates are FEMA's, NAD83 (EPSG:4269), unchanged.

The pipeline that builds these files is public:
  https://github.com/gsueur/nfhl-geoparquet-workshop

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


# ---------- commands ----------

def bootstrap(args) -> None:
    """Package a whole local build into latest/, written from scratch. No
    snapshot: the first daily run freezes the month's."""
    out = args.out_dir
    if out.exists():
        shutil.rmtree(out)
    latest = out / LATEST
    con = connect(args.work_dir, args.memory_limit)
    crs = projjson(con)
    ctl = duckdb.connect(str(args.control_db), read_only=True)
    states = f"AND state IN ({', '.join(repr(x) for x in args.states)})" if args.states else ""
    zips = dict(ctl.execute("SELECT dfirm_id, zip_name FROM import_log "
                            f"WHERE status = 'subdivided' {states}").fetchall())
    skipped = {d: {"dfirm_id": d, "state": st, "county": c, "fema_update_date": str(dt),
                   "zip_name": z, "reason": e, "checked": args.date}
               for d, st, c, dt, z, e in ctl.execute(
                   "SELECT dfirm_id, state, county, fema_update_date, zip_name, error "
                   f"FROM import_log WHERE error LIKE '%{NO_LAYER}%' {states}").fetchall()}
    ctl.close()
    # exFAT volumes carry AppleDouble "._*" siblings.
    files = sorted(f for f in args.subdivided.glob("state=*/county=*.parquet")
                   if not f.name.startswith("._")
                   and (not args.states or f.parent.name.split("=")[1] in args.states))
    t0 = time.monotonic()
    rows = []
    for i, src in enumerate(files, 1):
        dfirm = con.execute(f"SELECT any_value(dfirm_id) FROM read_parquet('{sql_path(src)}')").fetchone()[0]
        rows.append(package(con, src, latest, crs, zips.get(dfirm)))
        if i % 100 == 0 or i == len(files):
            print(f"  {i:,}/{len(files):,} deliveries, {time.monotonic() - t0:,.0f} s", flush=True)
    assert len(rows) == len(zips), f"{len(rows):,} files, {len(zips):,} subdivided in the control database"
    changes = {"runs": [{"date": args.date, "bootstrap": len(rows)}]}
    write_repository(con, out, rows, crs, args.date, [], changes, skipped)
    print(f"  {len(rows):,} deliveries, {sum(r['features'] for r in rows):,} pieces, "
          f"{sum(r['bytes'] for r in rows) / 1e9:.1f} GB, {len(skipped)} skipped", flush=True)


def update(args) -> None:
    """Re-process what FEMA republished since the published index."""
    # The workshop pipeline, from its own environment (see the module docstring).
    # Its package is called nfhl too: this script's folder must not shadow it.
    here = Path(__file__).resolve().parent
    sys.path[:] = [p for p in sys.path if Path(p or ".").resolve() != here]
    from nfhl import catalog as fema
    from nfhl.ingest import ingest_county
    from nfhl.normalize import normalize_county
    from nfhl.stages import subdivide_county

    today = args.date
    out = args.out_dir
    if out.exists():
        shutil.rmtree(out)
    latest = out / LATEST
    latest.mkdir(parents=True)
    pipeline = args.work_dir / "pipeline"
    os.environ["NFHL_DATA_ROOT"] = str(pipeline)
    con = connect(args.work_dir / "duckdb", args.memory_limit)
    crs = projjson(con)

    rows = {r["dfirm_id"]: r for r in read_index(con, f"{args.public}/{LATEST}/{INDEX}")}
    changes = get_json(f"{args.public}/{LATEST}/changes.json", {"runs": []})
    skipped = get_json(f"{args.public}/{LATEST}/skipped.json", {})
    published = get_json(f"{args.public}/snapshots.json", {}).get("snapshots", [])
    print(f"  published: {len(rows):,} deliveries, {len(skipped)} skipped", flush=True)

    listed = fema.list_datasets()
    if len(listed) < MIN_LISTED:
        sys.exit(f"FEMA lists {len(listed):,} county-wide deliveries, fewer than {MIN_LISTED:,}: "
                 "the portal page looks truncated; nothing changed")
    if args.states:
        listed = [d for d in listed if d["state"] in args.states]
        rows = {k: r for k, r in rows.items() if r["state"] in args.states}
    todo = []
    for d in listed:
        have = rows.get(d["dfirm_id"])
        skip = skipped.get(d["dfirm_id"])
        if skip and skip["zip_name"] == d["zip_name"]:
            continue
        if have is None or str(d["fema_update_date"]) > have["fema_update_date"]:
            todo.append(d)
    todo.sort(key=lambda d: (d["fema_update_date"], d["dfirm_id"]))
    deferred = todo[args.max:]
    todo = todo[:args.max]
    not_listed = sorted(set(rows) - {d["dfirm_id"] for d in listed})
    print(f"  FEMA lists {len(listed):,}: {len(todo)} to process, {len(deferred)} deferred, "
          f"{len(not_listed)} published but no longer listed", flush=True)

    run = {"date": today, "listed": len(listed), "replaced": [], "added": [], "skipped": [],
           "failed": [], "deferred": len(deferred), "not_listed": not_listed}
    for d in todo:
        t0 = time.monotonic()
        key = {"dfirm_id": d["dfirm_id"], "state": d["state"], "county": d["county"],
               "fema_update_date": str(d["fema_update_date"]), "zip_name": d["zip_name"]}
        try:
            ingest_county(d["state"], d["county"], d["url"], d["fema_update_date"])
            normalize_county(d["state"], d["county"])
            subdivide_county(d["state"], d["county"], 100)
            src = pipeline / "silver_subdivided" / f"state={d['state']}" / f"county={d['county']}.parquet"
            row = package(con, src, latest, crs, d["zip_name"])
        except FileNotFoundError as e:
            if NO_LAYER not in str(e):
                run["failed"].append({**key, "error": f"FileNotFoundError: {e}"[:500]})
                print(f"  FAIL {d['state']} {d['county']} {d['dfirm_id']}: {e}", flush=True)
                continue
            # The delivery holds no flood hazard layer: tried again when FEMA replaces it.
            skipped[d["dfirm_id"]] = {**key, "reason": f"FileNotFoundError: {e}", "checked": today}
            run["skipped"].append(key)
            print(f"  skip {d['state']} {d['county']} {d['dfirm_id']}: {e}", flush=True)
            continue
        except Exception as e:  # noqa: BLE001 - one delivery must not stop the others
            run["failed"].append({**key, "error": f"{type(e).__name__}: {e}"[:500]})
            print(f"  FAIL {d['state']} {d['county']} {d['dfirm_id']}: {type(e).__name__}: {e}",
                  flush=True)
            continue
        finally:
            shutil.rmtree(pipeline, ignore_errors=True)
        old = rows.get(d["dfirm_id"])
        if old is not None and old["path"] != row["path"]:
            sys.exit(f"{d['dfirm_id']} moved from {old['path']} to {row['path']}")
        skipped.pop(d["dfirm_id"], None)
        rows[d["dfirm_id"]] = row
        change = {**key, "features": row["features"]}
        if old is None:
            run["added"].append(change)
        else:
            run["replaced"].append({**change, "previous_date": old["fema_update_date"]})
        print(f"  {'add ' if old is None else 'repl'} {d['state']} {d['county']} {d['dfirm_id']} "
              f"{d['fema_update_date']}: {row['features']:,} pieces, "
              f"{time.monotonic() - t0:.0f} s", flush=True)

    snapshots = plan_snapshots(out, published, today)
    if run["replaced"] or run["added"] or run["skipped"] or run["failed"]:
        changes = {"runs": ([run] + changes.get("runs", []))[:KEEP_RUNS]}
    write_repository(con, out, list(rows.values()), crs, today, snapshots, changes, skipped)
    write_json(out / "run.json", run)
    print(f"  replaced {len(run['replaced'])}, added {len(run['added'])}, "
          f"skipped {len(run['skipped'])}, failed {len(run['failed'])}", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    today = datetime.now(timezone.utc).date().isoformat()
    for name in ("bootstrap", "update"):
        sp = sub.add_parser(name)
        sp.add_argument("--out-dir", type=Path, required=True,
                        help="written from scratch: latest/ and the repository files")
        sp.add_argument("--work-dir", type=Path, default=Path("work"), help="scratch space")
        sp.add_argument("--memory-limit", default="12GB")
        sp.add_argument("--date", default=today, help="the day FEMA's list is read (default: today UTC)")
    b = sub.choices["bootstrap"]
    b.add_argument("--subdivided", type=Path, required=True,
                   help="the pipeline's silver_subdivided/ folder")
    b.add_argument("--control-db", type=Path, required=True,
                   help="the pipeline's control database: zip names and deliveries without a layer")
    b.add_argument("--states", nargs="*", help="only these states (a test run)")
    u = sub.choices["update"]
    u.add_argument("--public", default=PUBLIC, help="where the published index is read")
    u.add_argument("--max", type=int, default=200, help="deliveries per run; the rest waits a day")
    u.add_argument("--states", nargs="*", help="only these states (a test run, never published)")
    args = p.parse_args()
    time.strptime(args.date, "%Y-%m-%d")
    {"bootstrap": bootstrap, "update": update}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
