#!/usr/bin/env python3
"""
Geoconnex (Internet of Water): US water reference features and the features
data providers publish, as GeoParquet that reads fast over HTTP.

Geoconnex links US water data to the places it describes. It curates
reference features (rivers, gages, dams, watersheds, aquifers) and harvests
the features hundreds of providers publish (Water Quality Portal sites,
USGS monitoring locations, state gages...). Its own GeoParquet export holds
all of them in one 3.9 GB file, unsorted, in row groups of up to 416 MB:
a bbox around Boston read for 95 s. This republishes it as:

  <out>/latest/reference/<layer>.parquet     one file per reference layer,
                                             with the layer's full attributes
  <out>/latest/providers/<source>.parquet    one file per harvested source
  <out>/latest/{reference,providers}/_manifest.json
  <out>/latest/ATTRIBUTION.txt, _source.json what was read, and its versions
  <out>/index.json, snapshots.json           the parquetry repository files

Every file is sorted along a Hilbert curve over its own extent, carries a
`bbox` covering column, and is cut into row groups of at most GROUP_BYTES
of uncompressed rows, so a bbox filter reads a couple of MB.

Each reference layer comes from where its attributes are complete:

  mainstems   ref_rivers GitHub release, mainstems.gpkg (the export has no
              length, drainage area or downstream link)
  gages, dams, the three aquifer layers
              reference.geoconnex.us, OGC API Features, paged
  hu02..hu12, water_systems
              the export (the API answers 500 on HU12 polygons; name and URI
              are what the export has)

Providers are the export's other sitemaps, as they are: uri, name,
description, mainstem_uri, geometry.

  python3 scripts/datasets/geoconnex.py fetch --work-dir work
  python3 scripts/datasets/geoconnex.py build --work-dir work --out-dir out/geoconnex
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.infra import GEOMETRY_TYPE_NAMES

EXPORT = "https://storage.googleapis.com/metadata-geoconnex-us/exports/geoconnex_features.parquet"
API = "https://reference.geoconnex.us"
RIVERS_REPO = "internetofwater/ref_rivers"
API_LAYERS = {            # layer -> API collection
    "gages": "gages",
    "dams": "dams",
    "principal_aquifers": "principal_aq",
    "national_aquifers": "nat_aq",
    "hydrogeologic_regions": "sec_hydrg_reg",
}
EXPORT_LAYERS = {         # layer -> export sitemap
    "hu02": "ref:hu02", "hu04": "ref:hu04", "hu06": "ref:hu06",
    "hu08": "ref:hu08", "hu10": "ref:hu10", "hu12": "ref:hu12",
    "water_systems": "ref:pws",
}
LABELS = {
    "mainstems": "Rivers: mainstems, head to outlet",
    "gages": "Stream gages",
    "dams": "Dams",
    "hu02": "Watersheds HU02 (regions)",
    "hu04": "Watersheds HU04 (subregions)",
    "hu06": "Watersheds HU06 (basins)",
    "hu08": "Watersheds HU08 (subbasins)",
    "hu10": "Watersheds HU10",
    "hu12": "Watersheds HU12 (subwatersheds)",
    "principal_aquifers": "Principal aquifers",
    "national_aquifers": "National aquifers",
    "hydrogeologic_regions": "Secondary hydrogeologic regions",
    "water_systems": "Public water systems",
}
# Census layers duplicate what GAUL and the Census publish; not republished.
SKIPPED_SITEMAPS = {"ref:states", "ref:counties", "ref:places", "ref:cbsa", "ref:ua10", "ref:aiannh",
                    # the API copies, read from there with their attributes
                    "ref:mainstems", "ref:gages", "ref:dams",
                    "ref:princi_aq", "ref:nat_aq", "ref:sec_hydrg_reg"}
PAGE = 10_000
# Uncompressed bytes of a row group: geometry plus every string. A bbox
# filter on one point then reads one group of 1 to 2 MB compressed. A
# feature bigger than this gets a group of its own.
GROUP_BYTES = 4 << 20
MAX_GROUP_ROWS = 131_072
ZSTD_LEVEL = 15

BBOX_STRUCT = """struct_pack(
                    xmin := (ST_XMin(geom) - abs(ST_XMin(geom)) * 1e-6 - 1e-9)::FLOAT,
                    ymin := (ST_YMin(geom) - abs(ST_YMin(geom)) * 1e-6 - 1e-9)::FLOAT,
                    xmax := (ST_XMax(geom) + abs(ST_XMax(geom)) * 1e-6 + 1e-9)::FLOAT,
                    ymax := (ST_YMax(geom) + abs(ST_YMax(geom)) * 1e-6 + 1e-9)::FLOAT
                ) AS bbox"""


# ---------- fetch ----------

def get(url: str, dest: Path | None = None, tries: int = 5):
    """GET with retries; JSON when dest is None, else streamed to dest."""
    for attempt in range(tries):
        try:
            headers = {"User-Agent": "geomermaids-parquetry"}
            # Runners share IPs: unauthenticated GitHub API calls hit the
            # 60-an-hour limit.
            if url.startswith("https://api.github.com/") and os.environ.get("GITHUB_TOKEN"):
                headers["Authorization"] = f"Bearer {os.environ['GITHUB_TOKEN']}"
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=300) as r:
                if dest is None:
                    return json.load(r), dict(r.headers)
                with dest.open("wb") as f:
                    shutil.copyfileobj(r, f, 1 << 24)
                return None, dict(r.headers)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            if attempt == tries - 1:
                raise
            print(f"  retry {attempt + 1} {url}: {e}", flush=True)
            time.sleep(10 * (attempt + 1))


def upstream() -> dict:
    """The versions of everything read: the export's ETag and date, the
    ref_rivers release holding mainstems.gpkg. The workflow compares this
    with the published _source.json and skips a run when nothing moved."""
    req = urllib.request.Request(EXPORT, method="HEAD", headers={"User-Agent": "geomermaids-parquetry"})
    with urllib.request.urlopen(req, timeout=60) as r:
        export = {"url": EXPORT, "etag": r.headers.get("ETag"), "last_modified": r.headers.get("Last-Modified"),
                  "bytes": int(r.headers.get("Content-Length"))}
    releases, _ = get(f"https://api.github.com/repos/{RIVERS_REPO}/releases?per_page=20")
    rel = next(r for r in releases if any(a["name"] == "mainstems.gpkg" for a in r["assets"]))
    asset = next(a for a in rel["assets"] if a["name"] == "mainstems.gpkg")
    return {"export": export,
            "mainstems": {"repo": RIVERS_REPO, "release": rel["tag_name"],
                          "url": asset["browser_download_url"], "bytes": asset["size"]},
            "api": {"url": API, "collections": sorted(API_LAYERS.values())}}


def fetch(args) -> None:
    work = args.work_dir
    work.mkdir(parents=True, exist_ok=True)
    src = upstream()
    (work / "upstream.json").write_text(json.dumps(src, indent=2) + "\n")
    print(json.dumps(src, indent=2), flush=True)

    for name, url, size in [("export.parquet", src["export"]["url"], src["export"]["bytes"]),
                            ("mainstems.gpkg", src["mainstems"]["url"], src["mainstems"]["bytes"])]:
        dest = work / name
        if dest.exists() and dest.stat().st_size == size:
            print(f"  {name}: already here", flush=True)
            continue
        t0 = time.monotonic()
        get(url, dest)
        assert dest.stat().st_size == size, f"{name}: {dest.stat().st_size:,} bytes, expected {size:,}"
        print(f"  {name}: {size / 1e6:,.0f} MB in {time.monotonic() - t0:.0f} s", flush=True)

    gaps = {}
    for layer, coll in API_LAYERS.items():
        pages = work / "api" / layer
        if pages.exists():
            shutil.rmtree(pages)
        pages.mkdir(parents=True)
        matched, ids, offset = None, set(), 0
        while matched is None or offset < matched:
            page, _ = get(f"{API}/collections/{coll}/items?f=json&limit={PAGE}&offset={offset}")
            matched = page["numberMatched"] if matched is None else matched
            if not page["features"]:
                break
            (pages / f"{offset:09d}.ndjson").write_text(
                "".join(json.dumps(f) + "\n" for f in page["features"]))
            new = {f["id"] for f in page["features"]}
            assert not ids & new, f"{layer}: a page repeats ids, the paging is not stable"
            ids |= new
            offset += PAGE
        # numberMatched counts a few rows the server never returns (one dam,
        # nine gages in October 2026, the same count as the export holds):
        # a small gap is upstream's, a large one a broken run.
        gap = matched - len(ids)
        print(f"  {layer}: {len(ids):,} of {matched:,} features from the API", flush=True)
        assert 0 <= gap <= max(10, matched // 10_000), f"{layer}: paged {len(ids):,}, the API matched {matched:,}"
        gaps[layer] = {"served": len(ids), "matched": matched}
    (work / "api" / "counts.json").write_text(json.dumps(gaps, indent=2) + "\n")


# ---------- build ----------

def connect(work: Path) -> duckdb.DuckDBPyConnection:
    db = work / "build.duckdb"
    db.unlink(missing_ok=True)
    con = duckdb.connect(str(db))
    con.execute(f"INSTALL spatial; LOAD spatial; SET temp_directory = '{work / 'spill'}'; "
                "SET preserve_insertion_order = false")
    return con


def slug(sitemap: str) -> str:
    """epa:wqp -> epa_wqp, iow:state_gages:ndwr -> iow_state_gages_ndwr."""
    s = re.sub(r"[^a-z0-9]+", "_", sitemap.lower()).strip("_")
    assert s, sitemap
    return s


def snake(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def load_reference(con: duckdb.DuckDBPyConnection, work: Path) -> dict[str, str]:
    """One table per reference layer, `geom` untagged lon/lat (CRS84).
    Returns layer -> where it was read from."""
    origin = {}
    # mainstems: EPSG:4326 in the GeoPackage, lon/lat order as stored.
    cols = [c for c, *_ in con.execute(
        f"DESCRIBE SELECT * FROM ST_Read('{work / 'mainstems.gpkg'}')").fetchall()
        if c not in ("fid", "geom")]
    sel = ", ".join(f'"{c}" AS {snake(c)}' for c in cols)
    con.execute(f"""CREATE TABLE ref_mainstems AS
                    SELECT {sel}, ST_SetCRS(geom, '')::GEOMETRY AS geom
                    FROM ST_Read('{work / 'mainstems.gpkg'}')""")
    origin["mainstems"] = "ref_rivers mainstems.gpkg"

    for layer, coll in API_LAYERS.items():
        files = sorted(str(f) for f in (work / "api" / layer).glob("*.ndjson"))
        con.execute(f"""
            CREATE TEMP TABLE raw AS
            SELECT json AS f FROM read_ndjson_objects({files!r})""")
        # A stable column order: the identifiers first, then alphabetical.
        props = sorted((k for k, in con.execute(
            "SELECT DISTINCT unnest(json_keys(f->'properties')) FROM raw").fetchall()),
            key=lambda k: (["uri", "id", "name"].index(k) if k in ("uri", "id", "name") else 3, k))
        sel = ", ".join(f"f->'properties'->>'{k}' AS {snake(k)}" for k in props if snake(k) != "fid")
        con.execute(f"""CREATE TABLE ref_{layer} AS
                        SELECT {sel}, ST_GeomFromGeoJSON(f->'geometry') AS geom FROM raw""")
        con.execute("DROP TABLE raw")
        origin[layer] = f"{API}/collections/{coll}"
    numeric = {"gages": ["dasqkm_diff", "gage_totdasqkm", "nhdpv2_offset_m", "nhdpv2_reach_measure",
                         "nhdpv2_totdasqkm"],
               "dams": ["drainage_area_sqkm", "drainage_area_sqkm_nhdpv2", "nhdpv2_reach_measure"]}
    for layer, columns in numeric.items():
        have = {c for c, *_ in con.execute(f"DESCRIBE ref_{layer}").fetchall()}
        for c in columns:
            if c in have:
                con.execute(f"ALTER TABLE ref_{layer} ALTER {c} TYPE DOUBLE USING TRY_CAST({c} AS DOUBLE)")

    for layer, sitemap in EXPORT_LAYERS.items():
        code = "huc" if layer.startswith("hu") else "pwsid"
        con.execute(f"""
            CREATE TABLE ref_{layer} AS
            SELECT id AS uri, regexp_extract(id, '/([^/]+)$', 1) AS {code},
                   feature_name AS name, geometry::GEOMETRY AS geom
            FROM read_parquet('{work / 'export.parquet'}') WHERE geoconnex_sitemap = '{sitemap}'""")
        origin[layer] = f"export, sitemap {sitemap}"
    return origin


def write(con: duckdb.DuckDBPyConnection, table: str, dest: Path) -> dict:
    """Sorted, bbox-covered, byte-capped GeoParquet 2.0 of `table` (geom)."""
    n, xmin, ymin, xmax, ymax, types = con.execute(f"""
        SELECT count(*), min(ST_XMin(geom)), min(ST_YMin(geom)), max(ST_XMax(geom)), max(ST_YMax(geom)),
               list(DISTINCT ST_GeometryType(geom)::VARCHAR)
        FROM {table} WHERE geom IS NOT NULL""").fetchone()
    assert n, f"{table}: no features"
    geo = json.dumps({
        "version": "2.0.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB",
            "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in types),
            "bbox": [xmin, ymin, xmax, ymax],
            "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}},
        }},
    }).replace("'", "''")
    attrs = ", ".join(f'"{c}"' for c, *_ in con.execute(f"DESCRIBE {table}").fetchall() if c != "geom")
    box = f"ST_Extent(ST_MakeEnvelope({xmin!r}, {ymin!r}, {xmax!r}, {ymax!r}))"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.stem + ".duckdb.parquet")
    con.execute(f"""
        COPY (SELECT {attrs}, {BBOX_STRUCT}, geom AS geometry FROM {table}
              WHERE geom IS NOT NULL ORDER BY ST_Hilbert(geom, {box}))
        TO '{tmp}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', COMPRESSION ZSTD,
                    COMPRESSION_LEVEL 1, ROW_GROUP_SIZE 1048576, KV_METADATA {{geo: '{geo}'}})""")
    rewrite(tmp, dest)
    tmp.unlink()
    dropped = con.execute(f"SELECT count(*) FROM {table} WHERE geom IS NULL").fetchone()[0]
    groups, largest = con.execute(f"""
        SELECT count(*), max(b) FROM (SELECT row_group_id, sum(total_compressed_size) AS b
                                      FROM parquet_metadata('{dest}') GROUP BY 1)""").fetchone()
    return {"file": dest.name, "features": n, "without_geometry": dropped, "bytes": dest.stat().st_size,
            "row_groups": groups, "largest_row_group_bytes": largest, "sha256": sha256(dest)}


def rewrite(src: Path, dest: Path) -> None:
    """Copy a DuckDB-written file into row groups of at most GROUP_BYTES of
    uncompressed geometry and strings, and MAX_GROUP_ROWS rows, rows in the
    same (Hilbert) order. Same approach as gaul.py: DuckDB will not write a
    group under 2,048 rows and does not cap by bytes; pyarrow with
    geoarrow-pyarrow registered keeps the native GEOMETRY type and the
    `geo` footer."""
    import geoarrow.pyarrow  # noqa: F401  registers the geoarrow.wkb extension type
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    table = pq.read_table(src)
    sizes = np.zeros(table.num_rows, dtype=np.int64)
    for name in table.column_names:
        col = table.column(name)
        typ = col.type.storage_type if isinstance(col.type, pa.ExtensionType) else col.type
        if pa.types.is_binary(typ) or pa.types.is_string(typ) or pa.types.is_large_string(typ):
            arr = pa.chunked_array([c.storage if isinstance(c, pa.ExtensionArray) else c
                                    for c in col.chunks])
            sizes += pc.fill_null(pc.binary_length(arr), 0).to_numpy()
        else:
            sizes += 8
    cuts, size, rows = [0], 0, 0
    for i, n in enumerate(sizes.tolist()):
        if rows and (size + n > GROUP_BYTES or rows == MAX_GROUP_ROWS):
            cuts.append(i)
            size, rows = 0, 0
        size += n
        rows += 1
    cuts.append(table.num_rows)

    leaves = pq.ParquetFile(src).metadata.schema
    strings = [leaves.column(k).path for k in range(len(leaves))
               if leaves.column(k).physical_type == "BYTE_ARRAY" and leaves.column(k).path != "geometry"]
    floats = [leaves.column(k).path for k in range(len(leaves))
              if leaves.column(k).physical_type in ("FLOAT", "DOUBLE")]
    with pq.ParquetWriter(dest, table.schema, compression="zstd", compression_level=ZSTD_LEVEL,
                          use_dictionary=strings, use_byte_stream_split=floats,
                          write_statistics=True, data_page_size=1 << 20) as w:
        for a, b in zip(cuts, cuts[1:]):
            w.write_table(table.slice(a, b - a), row_group_size=b - a)


def verify(con: duckdb.DuckDBPyConnection, path: Path, expected: int) -> None:
    """Refuse a file whose footer, bbox, type, groups or row count is wrong."""
    problems = []
    kv = con.execute(f"SELECT key::VARCHAR, decode(value) FROM parquet_kv_metadata('{path}')").fetchall()
    geos = [v for k, v in kv if k == "geo"]
    if len(geos) != 1:
        sys.exit(f"{path}: {len(geos)} geo keys, want exactly 1")
    col = json.loads(geos[0])["columns"]["geometry"]
    if col.get("covering", {}).get("bbox") != {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}:
        problems.append(f"covering {col.get('covering')!r}")
    n, outside, xmin, ymin, xmax, ymax = con.execute(f"""
        SELECT count(*),
               count(*) FILTER (WHERE bbox.xmin > ST_XMin(geometry) OR bbox.ymin > ST_YMin(geometry)
                                   OR bbox.xmax < ST_XMax(geometry) OR bbox.ymax < ST_YMax(geometry)),
               min(ST_XMin(geometry)), min(ST_YMin(geometry)), max(ST_XMax(geometry)), max(ST_YMax(geometry))
        FROM read_parquet('{path}')""").fetchone()
    if n != expected:
        problems.append(f"{n:,} rows, expected {expected:,}")
    if outside:
        problems.append(f"{outside:,} geometries outside their bbox")
    if [round(v, 6) for v in col["bbox"]] != [round(v, 6) for v in (xmin, ymin, xmax, ymax)]:
        problems.append(f"declared extent {col['bbox']} is not the data's")
    logical = con.execute(f"SELECT logical_type FROM parquet_schema('{path}') WHERE name = 'geometry'").fetchone()[0]
    if not (logical and str(logical).startswith("GeometryType")):
        problems.append(f"geometry logical type {logical!r}")
    over = con.execute(f"""
        SELECT count(*) FROM (SELECT row_group_id, any_value(row_group_num_rows) AS n,
                                     sum(total_uncompressed_size) AS b
                              FROM parquet_metadata('{path}') GROUP BY 1)
        WHERE n > 1 AND (b > {GROUP_BYTES} * 2 OR n > {MAX_GROUP_ROWS})""").fetchone()[0]
    if over:
        problems.append(f"{over} row groups over the cap")
    if problems:
        sys.exit(f"{path}: " + "; ".join(problems))


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 24), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def build(args) -> None:
    work, out = args.work_dir, args.out_dir
    if out.exists():
        shutil.rmtree(out)
    latest = out / "latest"
    src = json.loads((work / "upstream.json").read_text())
    con = connect(work)
    t0 = time.monotonic()

    origin = load_reference(con, work)
    reference = {}
    for layer in LABELS:
        expected = con.execute(f"SELECT count(*) FROM ref_{layer} WHERE geom IS NOT NULL").fetchone()[0]
        e = write(con, f"ref_{layer}", latest / "reference" / f"{layer}.parquet")
        verify(con, latest / "reference" / e["file"], expected)
        reference[layer] = {**e, "source": origin[layer]}
        print(f"  reference/{layer}: {e['features']:,} features, {e['bytes'] / 1e6:,.1f} MB, "
              f"{e['row_groups']} groups, {time.monotonic() - t0:.0f} s", flush=True)

    export = f"read_parquet('{work / 'export.parquet'}')"
    sitemaps = con.execute(f"SELECT geoconnex_sitemap, count(*) FROM {export} GROUP BY 1 ORDER BY 2 DESC").fetchall()
    unknown = [s for s, _ in sitemaps if s.startswith("ref:") and s not in SKIPPED_SITEMAPS
               and s not in EXPORT_LAYERS.values()]
    assert not unknown, f"new reference sitemaps in the export, decide where they go: {unknown}"
    providers = {}
    for sitemap, n in sitemaps:
        if sitemap.startswith("ref:"):
            continue
        theme = slug(sitemap)
        assert theme not in providers, f"two sitemaps make {theme}"
        con.execute(f"""
            CREATE OR REPLACE TABLE p AS
            SELECT id AS uri, feature_name AS name, feature_description AS description,
                   mainstem_uri, geometry::GEOMETRY AS geom
            FROM {export} WHERE geoconnex_sitemap = '{sitemap}'""")
        expected = con.execute("SELECT count(*) FROM p WHERE geom IS NOT NULL").fetchone()[0]
        e = write(con, "p", latest / "providers" / f"{theme}.parquet")
        verify(con, latest / "providers" / e["file"], expected)
        providers[theme] = {**e, "sitemap": sitemap}
        print(f"  providers/{theme}: {e['features']:,} features, {e['bytes'] / 1e6:,.1f} MB, "
              f"{e['row_groups']} groups, {time.monotonic() - t0:.0f} s", flush=True)

    today = dt.date.today().isoformat()
    for folder, layers, title, labels in [
            ("reference", reference, "Geoconnex reference features: rivers, gages, dams, watersheds, aquifers",
             LABELS),
            ("providers", providers, "Geoconnex: features published by data providers, by source",
             {k: v["sitemap"] for k, v in providers.items()})]:
        write_json(latest / folder / "_manifest.json", {
            "state_name": title, "updated": today,
            "total_features": sum(e["features"] for e in layers.values()),
            "themes": {k: e["features"] for k, e in layers.items()},
            "labels": labels,
            "files": [{"theme": k, **e} for k, e in layers.items()],
        })
    write_json(out / "index.json", {"datasets": [
        {"path": "reference", "code": "", "name": "Reference features (rivers, gages, dams, watersheds, aquifers)"},
        {"path": "providers", "code": "", "name": "Features published by data providers, by source"}]})
    # One version online, replaced when upstream moves; the file stays
    # because repository readers (GeoPQ Workbench) look for it.
    write_json(out / "snapshots.json", {"latest": "latest/", "snapshots": []})
    api_counts = json.loads((work / "api" / "counts.json").read_text())
    write_json(latest / "_source.json", {**src, "api_counts": api_counts, "built": today})
    (latest / "ATTRIBUTION.txt").write_text(attribution(src, today))
    print(f"  {len(reference)} reference layers, {len(providers)} provider files, "
          f"{time.monotonic() - t0:.0f} s", flush=True)


def attribution(src: dict, today: str) -> str:
    return f"""\
# Data attribution

These files republish Geoconnex, a system of the Internet of Water
Coalition and the Center for Geospatial Solutions (Lincoln Institute of
Land Policy) that links US water data to the places it describes.

  Geoconnex:    https://geoconnex.us, https://docs.geoconnex.us
  Licence:      CC0 1.0 (Geoconnex documentation, reference repositories)
  Read:         {today}
    export      {src['export']['url']}
                (Last-Modified {src['export']['last_modified']})
    rivers      {src['mainstems']['url']}
                (ref_rivers {src['mainstems']['release']}, CC0 1.0)
    API         {src['api']['url']} (gages, dams, aquifers)

No attribution is required. Please credit "Geoconnex, Internet of Water"
anyway, and the agencies behind each source: USGS (NHDPlus, the Watershed
Boundary Dataset, gages, aquifers, GNIS, the National Geologic Map
Database), the US Army Corps of Engineers (National Inventory of Dams), the
US EPA (Water Quality Portal with USGS and the National Water Quality
Monitoring Council; Safe Drinking Water Information System), and the state
agencies that publish their own gages. US federal works are in the public
domain.

# What was changed

Nothing in the features. The reference layers keep every attribute their
source publishes; column names are lower snake case. The provider files
keep the export's columns (uri, name, description, mainstem_uri) and one
file per Geoconnex sitemap. Census layers (states, counties, places,
metropolitan and urban areas, tribal areas) are not republished.

Geometry is lon/lat (OGC:CRS84). Each file is sorted along a Hilbert curve
over its own extent, carries a `bbox` covering column, and is cut into row
groups of at most {GROUP_BYTES >> 20} MiB uncompressed: filter on `bbox` first, and
a query on a small area reads a few MB.

Every `uri` resolves at geoconnex.us to the feature's landing page, and
from there to the data published about it.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch", help="download the export, mainstems.gpkg and the API layers")
    f.add_argument("--work-dir", type=Path, required=True)
    b = sub.add_parser("build", help="write and verify every file from what fetch downloaded")
    b.add_argument("--work-dir", type=Path, required=True)
    b.add_argument("--out-dir", type=Path, required=True)
    sub.add_parser("upstream", help="print the upstream versions as JSON")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
    else:
        {"fetch": fetch, "build": build}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
