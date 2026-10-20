#!/usr/bin/env python3
"""
Geobox's samples: open city datasets offered in Get started on box.geomermaids.com, as GeoParquet and PMTiles.

Geobox links these files (nothing copied into a box): a newcomer clicks "Add it" and the sample is on a map at once,
credited from its collection in geobox_samples_catalog.py. One folder per sample under the geobox-samples/ prefix:

  <out>/nyc/nyc_buildings.parquet      New York City's building footprints (NYC Open Data, BUILDING)
  <out>/nyc/nyc_buildings.pmtiles
  <out>/la/la_neighborhoods.parquet    the City of Los Angeles's neighborhoods (Los Angeles Times, via the City's GeoHub)
  <out>/la/la_neighborhoods.pmtiles
  <out>/<folder>/_manifest.json        rows, sizes, sha256, tiles, source
  <out>/<folder>/ATTRIBUTION.txt

SAMPLES says what each keeps; adding one is a table entry (and its texts in the catalog module).

Each sample keeps only the columns a map or a newcomer uses, typed (the City of New York's export sends every value as
text; a height or year of 0 means unknown and becomes null). Geometry is 2D lon/lat (OGC:CRS84, the sources' own),
with a `bbox` declared as the GeoParquet covering, rows sorted on a Hilbert curve and cut into row groups of at most
4 MiB of geometry (gaul.py's rewrite), so a map view reads only the groups around it. The tiles are set per sample:
the buildings as small squares from zoom 8 to 10 (the whole city shows when a map opens on it), then outlines up to
every building at zoom 15, two bands joined by pmtiles.pyramid(); the neighborhoods, a coverage, whole at every zoom.

Usage:
  python3 scripts/datasets/geobox_samples.py build --work-dir work --out-dir out/geobox-samples
  python3 scripts/datasets/geobox_samples.py build --out-dir out/geobox-samples --sample la
  python3 scripts/datasets/geobox_samples.py build --out-dir out/geobox-samples --source nyc=~/Downloads/buildings.geojson
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles
from datasets.gaul import rewrite, verify
from datasets.infra import GEOMETRY_TYPE_NAMES

BBOX = """struct_pack(
            xmin := (ST_XMin(geometry) - abs(ST_XMin(geometry)) * 1e-6 - 1e-9)::FLOAT,
            ymin := (ST_YMin(geometry) - abs(ST_YMin(geometry)) * 1e-6 - 1e-9)::FLOAT,
            xmax := (ST_XMax(geometry) + abs(ST_XMax(geometry)) * 1e-6 + 1e-9)::FLOAT,
            ymax := (ST_YMax(geometry) + abs(ST_YMax(geometry)) * 1e-6 + 1e-9)::FLOAT
        ) AS bbox"""

SAMPLES = {
    "nyc": {
        "file": "nyc_buildings",
        "title": "New York City buildings",
        # the whole dataset as one GeoJSON (about 850 MB), the export the dataset page offers
        "url": "https://data.cityofnewyork.us/api/geospatial/5zhs-2jue?method=export&format=GeoJSON",
        "page": "https://data.cityofnewyork.us/d/5zhs-2jue",
        "credit": "City of New York, Office of Technology and Innovation (OTI), NYC Open Data",
        "terms": "https://www.nyc.gov/main/terms-of-use",
        "select": """NULLIF(trim(name), '') AS name,
                     TRY_CAST(bin AS BIGINT) AS bin,
                     -- over 1,600 ft is taller than any roof in the city (Central Park Tower: 1,550): an entry
                     -- error (one building has its BIN, 2,130,353, as its height), unknown like 0
                     CASE WHEN TRY_CAST(height_roof AS DOUBLE) BETWEEN 0.01 AND 1600
                          THEN TRY_CAST(height_roof AS DOUBLE) END AS height_roof_ft,
                     NULLIF(TRY_CAST(ground_elevation AS DOUBLE), 0) AS ground_elevation_ft,
                     NULLIF(TRY_CAST(construction_year AS INTEGER), 0) AS year_built,
                     TRY_CAST(feature_code AS INTEGER) AS feature_code""",
        # Two bands, joined by pyramid(). Zooms 8 to 10, where a building is a pixel or less: every building as a small
        # square with its roof height and year built, so the whole city shows when a map opens on it and can be coloured
        # by either at every zoom (Geobox then needs no map of its own for them). tylertoo keeps a run's finest level
        # verbatim, so that band is built to zoom 11 and only 8 to 10 are kept ("use"); its zoom 11 outlines, thrown
        # away, would break any cap, so the band has none (its squares reach 2 MB a tile at zoom 10, 800 KB compressed, measured).
        # Zooms 11 to 15: outlines, thinned at 11 and 12 with the tallest building winning each cell, every building
        # from 13, under a 1 MB cap (MapLibre reads it fine).
        "bands": [
            {"zooms": (8, 11), "use": (8, 10), "properties": ["height_roof_ft", "year_built"], "coverage": False,
             "options": ["--max-tile-size", "0", "--representation", "8-10:square"]},
            {"zooms": (11, 15), "properties": ["name", "height_roof_ft", "year_built"], "coverage": False,
             "options": ["--max-tile-size", "1M", "--sort-key", "height_roof_ft"]},
        ],
    },
    "la": {
        "file": "la_neighborhoods",
        "title": "Los Angeles neighborhoods",
        # the City of Los Angeles's copy of the Los Angeles Times boundaries, as GeoJSON from its feature service
        "url": ("https://services5.arcgis.com/7nsPwEMP38bSkCjy/arcgis/rest/services/LA_Times_Neighborhoods/"
                "FeatureServer/0/query?where=1%3D1&outFields=name&outSR=4326&f=geojson"),
        "page": "https://geohub.lacity.org/datasets/lahub::la-times-neighborhood-boundaries",
        "credit": "Los Angeles Times, via the City of Los Angeles GeoHub (CC BY 4.0)",
        "terms": "https://creativecommons.org/licenses/by/4.0/",
        "select": "name",
        # a coverage: every neighborhood at every zoom, borders shared
        "bands": [{"zooms": (0, 14), "properties": ["name"], "coverage": True}],
    },
}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(key: str, work: Path) -> Path:
    """The sample's source as GeoJSON in work/, downloaded once."""
    dest = work / f"{key}.geojson"
    if dest.is_file() and dest.stat().st_size:
        return dest
    part = dest.with_suffix(".part")
    req = urllib.request.Request(SAMPLES[key]["url"], headers={"User-Agent": "osm-geoparquet (geobox samples)"})
    with urllib.request.urlopen(req, timeout=1800) as r, part.open("wb") as f:
        while chunk := r.read(1 << 20):
            f.write(chunk)
    part.rename(dest)
    return dest


def write(con, src: Path, spec: dict, dest: Path) -> dict:
    """The kept columns to GeoParquet 2.0: 2D lon/lat geometry, bbox covering, Hilbert order, then row groups capped
    by bytes. No CRS in the footer: OGC:CRS84 is GeoParquet's default and the sources'."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE src AS
        SELECT *, {BBOX} FROM (
            SELECT {spec['select']}, ST_Force2D(geom) AS geometry
            FROM ST_Read('{src}') WHERE geom IS NOT NULL)""")
    xmin, ymin, xmax, ymax, types, n = con.execute("""
        SELECT min(ST_XMin(geometry)), min(ST_YMin(geometry)), max(ST_XMax(geometry)),
               max(ST_YMax(geometry)), list(DISTINCT ST_GeometryType(geometry)::VARCHAR), count(*)
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
    raw = dest.with_suffix(".raw.parquet")
    con.execute(f"""
        COPY (SELECT * FROM src ORDER BY ST_Hilbert(geometry,
                  ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))))
        TO '{raw}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', COMPRESSION ZSTD,
                    KV_METADATA {{geo: '{geo}'}})""")
    rewrite(raw, dest)
    raw.unlink()
    groups, largest = con.execute(f"""
        SELECT count(*), max(b) FROM (SELECT row_group_id, sum(total_compressed_size) AS b
        FROM parquet_metadata('{dest}') GROUP BY 1)""").fetchone()
    return {"file": dest.name, "features": n, "bytes": dest.stat().st_size, "row_groups": groups,
            "largest_row_group_bytes": largest, "sha256": sha256(dest)}


def tile(folder: Path, spec: dict) -> dict:
    """The sample's PMTiles: one archive per band, each through pmtiles.tile()'s size check (no feature dropped),
    joined by pmtiles.pyramid() when there are several."""
    src, dest = folder / f"{spec['file']}.parquet", folder / f"{spec['file']}.pmtiles"
    layer, bands = spec["file"], spec["bands"]
    if len(bands) == 1:
        b = bands[0]
        return pmtiles.tile(src, dest, layer=layer, min_zoom=b["zooms"][0], max_zoom=b["zooms"][1],
                            properties=b["properties"], coverage=b["coverage"], extra=b.get("options"))
    for stale in folder.glob(f"{spec['file']}.z*-*.pmtiles"):   # bands left by a run that failed its size check
        stale.unlink()
    parts = []
    for b in bands:
        part = folder / f"{spec['file']}.z{b['zooms'][0]}-{b['zooms'][1]}.pmtiles"
        pmtiles.tile(src, part, layer=layer, min_zoom=b["zooms"][0], max_zoom=b["zooms"][1],
                     properties=b["properties"], coverage=b["coverage"], extra=b.get("options"))
        lo, hi = b.get("use", b["zooms"])
        parts.append((lo, hi, part, layer))
    entry = pmtiles.pyramid(parts, dest)
    for *_, part, _ in parts:
        part.unlink()
    return {**entry, "layer": layer, "properties": bands[-1]["properties"],
            "bands": [{"zooms": list(b.get("use", b["zooms"])), "properties": b["properties"]} for b in bands]}


def attribution(spec: dict, retrieved: str) -> str:
    return (f"{spec['title']}\n\nSource: {spec['credit']}\n{spec['page']}\nTerms: {spec['terms']}\n"
            f"Retrieved {retrieved}.\n\nWhat was changed: only the columns listed in the catalog are kept, typed "
            f"(numbers as numbers, zero heights and years as unknown); the geometry is 2D, sorted on a Hilbert curve, "
            f"with a bbox covering. The map tiles come from the same rows. Processed and hosted by Geomermaids "
            f"(https://geoparquet.geomermaids.com/) for Geobox (https://box.geomermaids.com/).\n")


def build(work: Path, out: Path, local: dict[str, Path], only: list[str] | None = None) -> None:
    work.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; SET temp_directory = '{work}'")
    t0 = time.monotonic()
    for key, spec in SAMPLES.items():
        if only and key not in only:
            continue
        retrieved = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        src = local.get(key) or fetch(key, work)
        folder = out / key
        folder.mkdir(parents=True, exist_ok=True)
        entry = write(con, src, spec, folder / f"{spec['file']}.parquet")
        verify(con, folder, [spec["file"]])
        tiles = tile(folder, spec)
        (folder / "ATTRIBUTION.txt").write_text(attribution(spec, retrieved))
        (folder / "_manifest.json").write_text(json.dumps({
            "state_name": spec["title"],
            "total_features": entry["features"],
            "crs": "OGC:CRS84",
            "source": {"url": spec["url"], "page": spec["page"], "credit": spec["credit"],
                       "terms": spec["terms"], "retrieved": retrieved},
            "files": [{"name": spec["file"], **entry}],
            "pmtiles": [{"name": spec["file"], **tiles}],
        }, indent=2) + "\n")
        print(f"  {key}/{spec['file']}: {entry['features']:,} rows, {entry['bytes'] / 1e6:.1f} MB, "
              f"{entry['row_groups']} row groups; tiles {tiles['bytes'] / 1e6:.1f} MB, "
              f"{time.monotonic() - t0:.0f} s", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="fetch, convert, tile and verify")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help="the geobox-samples/ prefix, e.g. out/geobox-samples")
    b.add_argument("--source", action="append", default=[], metavar="SAMPLE=PATH",
                   help="use a GeoJSON already on disk")
    b.add_argument("--sample", action="append", choices=list(SAMPLES),
                   help="build only this sample (repeatable; default: all)")
    args = p.parse_args()
    local = {}
    for spec in args.source:
        key, path = spec.split("=", 1)
        if key not in SAMPLES:
            sys.exit(f"unknown sample {key!r}")
        local[key] = Path(path).expanduser()
    build(args.work_dir, args.out_dir, local, args.sample)


if __name__ == "__main__":
    main()
