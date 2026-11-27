#!/usr/bin/env python3
"""
Scene index of Sentinel-2 Level-2A (Collection 1) as Parquet, one row per
scene, for pickers (Geobox's Sentinel-2 page) and DuckDB: which images exist
for a 110 km square, when, and how cloudy, without asking an API. Metadata
only; the images stay in Element 84's bucket on AWS Open Data
(e84-earth-search-sentinel-data, public, range requests).

  sentinel-2/index/year=<YYYY>/scenes.parquet   one row per scene sensed that year (UTC)
  sentinel-2/index/index.json                   rows per year, the newest scene, sizes, sha256

Source: Element 84's Earth Search STAC API, collection sentinel-2-c1-l2a
(https://earth-search.aws.element84.com/v1). A scene is in the API about two
hours after it is sensed. The first build reads the whole collection, month
by month (about 31 million scenes, 2015 to now); later builds ask only for
the scenes whose `updated` is past the published index's newest, and rewrite
the year files those fall in (in practice the current year). Each rebuilt
year is then counted against the API: a difference fails the build.

Rows are sorted by tile then time, in row groups of 20,000: a reader that
filters on `tile` fetches one or two row groups over HTTP.

Columns:
  id                 the scene, S2C_T31TFL_20261121T103351_L2A
  tile               its MGRS square, 31TFL or 09TWM (grid:code without "MGRS-"; the API
                     writes zones 1 to 9 without their zero, the scene ids with it)
  datetime           when it was sensed (TIMESTAMPTZ, UTC)
  cloud_cover        percent of the scene under cloud (eo:cloud_cover)
  nodata_percent     percent of the square with no image: a square at the edge
                     of a satellite's pass is only partly imaged
  snow_ice_percent, vegetation_percent, water_percent   from the scene classification
  platform           sentinel-2a, -2b or -2c
  processing_baseline  ESA's processor version, 05.11
  lat, lon           the centre of the imaged part (proj:centroid), degrees
  west, south, east, north   the box around the imaged part, degrees (the item's bbox). A
                     scene across the antimeridian (zones 1 and 60) has west > east:
                     west 176.9, east -179.7. The
                     outline itself is not kept (it would triple the files): a place inside
                     the box of a half-imaged square may be in the half without image.
  created, updated   when the API first held the scene, and last changed it

No address column: a scene's folder follows from the other columns,
  https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/sentinel-2-c1-l2a/
    <zone, no leading zero>/<band letter>/<square>/<year>/<month, no leading zero>/<id>/
(31TFL in November 2026: 31/T/FL/2026/11/<id>/), year and month being those of
`datetime`. In it: one COG per band (B04.tif ... at 10, 20 or 60 m), TCI.tif
(true colour, 10 m), L2A_PVI.jpg (a 343 px preview) and the STAC item.

Contains modified Copernicus Sentinel data, processed by ESA; catalogued and
hosted by Element 84 (Earth Search).

Usage:
  python3 scripts/datasets/sentinel2_index.py build --out-dir out/index            # only what changed
  python3 scripts/datasets/sentinel2_index.py build --out-dir out/index --full     # the whole collection
  python3 scripts/datasets/sentinel2_index.py build --out-dir out/index --years 2026
  python3 scripts/datasets/sentinel2_index.py publish --out-dir out/index --remote parquetry:parquetry/sentinel-2
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

API = "https://earth-search.aws.element84.com/v1/search"
COLLECTION = "sentinel-2-c1-l2a"
PUBLIC = "https://parquetry.geomermaids.com/sentinel-2/index"
FIRST_YEAR = 2015
ATTRIBUTION = ("Contains modified Copernicus Sentinel data, processed by ESA; catalogued and "
               "hosted by Element 84 (Earth Search).")
AGENT = {"Content-Type": "application/json", "User-Agent": "parquetry-sentinel2-index/1.0 (geomermaids.com)"}
# What a row needs, and nothing else: an item is 15 KB with its assets, 400 bytes without.
PROPERTIES = {"datetime": "datetime", "created": "created", "updated": "updated",
              "eo:cloud_cover": "cloud_cover", "grid:code": "grid", "platform": "platform",
              "proj:centroid": "centroid", "s2:nodata_pixel_percentage": "nodata_percent",
              "s2:snow_ice_percentage": "snow_ice_percent",
              "s2:vegetation_percentage": "vegetation_percent",
              "s2:water_percentage": "water_percent", "s2:processing_baseline": "processing_baseline"}
FIELDS = {"include": ["id", "bbox", *(f"properties.{p}" for p in PROPERTIES)],
          "exclude": ["assets", "geometry", "links", "stac_extensions", "stac_version",
                      "type", "collection"]}
# The API answers through a gateway that refuses bodies past about 6 MB: 7,500 slim items are 3.6.
PAGE = 7500
# Scenes updated shortly before the published newest are asked for again (the upsert drops repeats).
OVERLAP = dt.timedelta(hours=6)
JSON_COLUMNS = ("{id: 'VARCHAR', grid: 'VARCHAR', datetime: 'TIMESTAMPTZ', cloud_cover: 'DOUBLE', "
                "nodata_percent: 'DOUBLE', snow_ice_percent: 'DOUBLE', vegetation_percent: 'DOUBLE', "
                "water_percent: 'DOUBLE', platform: 'VARCHAR', processing_baseline: 'VARCHAR', "
                "lat: 'DOUBLE', lon: 'DOUBLE', bbox: 'DOUBLE[]', created: 'TIMESTAMPTZ', updated: 'TIMESTAMPTZ'}")
ROW = """id, lpad(regexp_replace(grid, '^MGRS-', ''), 5, '0') AS tile, datetime,
    CAST(round(cloud_cover, 2) AS REAL) AS cloud_cover, CAST(round(nodata_percent, 2) AS REAL) AS nodata_percent,
    CAST(round(snow_ice_percent, 2) AS REAL) AS snow_ice_percent,
    CAST(round(vegetation_percent, 2) AS REAL) AS vegetation_percent,
    CAST(round(water_percent, 2) AS REAL) AS water_percent, platform, processing_baseline,
    round(lat, 5) AS lat, round(lon, 5) AS lon,
    CAST(bbox[1] AS REAL) AS west, CAST(bbox[2] AS REAL) AS south, CAST(bbox[3] AS REAL) AS east,
    CAST(bbox[4] AS REAL) AS north, created, updated"""
COLUMNS = ["id", "tile", "datetime", "cloud_cover", "nodata_percent", "snow_ice_percent",
           "vegetation_percent", "water_percent", "platform", "processing_baseline", "lat", "lon",
           "west", "south", "east", "north", "created", "updated"]


def iso(t: dt.datetime) -> str:
    return t.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"


def search(body: dict, attempts: int = 7) -> dict:
    """One page of the API. A 5xx is tried again, with a smaller page from the third time."""
    for i in range(attempts):
        try:
            req = urllib.request.Request(API, data=json.dumps(body).encode(), headers=AGENT)
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if (e.code < 500 and e.code != 429) or i == attempts - 1:
                raise
            if i >= 1 and "next" not in body:
                body = body | {"limit": max(500, body["limit"] // 2)}
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError):
            if i == attempts - 1:
                raise
        time.sleep(min(60, 2 ** i))
    raise AssertionError


def matched(datetime: str) -> int:
    body = {"collections": [COLLECTION], "limit": 1, "datetime": datetime,
            "fields": {"include": ["id"], "exclude": ["assets", "links", "geometry", "properties"]}}
    return int(search(body)["context"]["matched"])


def crawl(part: Path, datetime: str | None = None, query: dict | None = None) -> int:
    """Every scene of a time range (or matching a query) as one JSON object per line."""
    body = {"collections": [COLLECTION], "limit": PAGE, "fields": FIELDS}
    if datetime:
        body["datetime"] = datetime
    if query:
        body["query"] = query
    n = 0
    tmp = part.with_suffix(".tmp")
    with tmp.open("w") as f:
        while True:
            page = search(body)
            for item in page["features"]:
                p = item.get("properties") or {}
                row = {"id": item["id"]} | {name: p.get(prop) for prop, name in PROPERTIES.items()}
                c = row.pop("centroid") or {}
                row["lat"], row["lon"] = c.get("lat"), c.get("lon")
                row["bbox"] = item.get("bbox")
                f.write(json.dumps(row, separators=(",", ":")) + "\n")
            n += len(page["features"])
            nxt = [link for link in page.get("links", []) if link.get("rel") == "next"]
            if not nxt or not page["features"]:
                break
            body = nxt[0]["body"]
    tmp.rename(part)
    return n


def year_range(y: int) -> str:
    return f"{y}-01-01T00:00:00Z/{y}-12-31T23:59:59.999Z"


def months(years: list[int], now: dt.datetime) -> list[tuple[str, str]]:
    out = []
    for y in years:
        for m in range(1, 13):
            a = dt.datetime(y, m, 1, tzinfo=dt.UTC)
            if a > now:
                break
            b = dt.datetime(y + (m == 12), m % 12 + 1, 1, tzinfo=dt.UTC) - dt.timedelta(milliseconds=1)
            out.append((f"{y}-{m:02d}", f"{iso(a)}/{iso(b)}"))
    return out


def published() -> dict | None:
    """The published index.json, or None when there is no index yet."""
    try:
        with urllib.request.urlopen(urllib.request.Request(f"{PUBLIC}/index.json", headers=AGENT),
                                    timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def fetch(url: str, dest: Path) -> bool:
    """A published year file, to a local path. False when there is none."""
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=AGENT), timeout=600) as r, \
                dest.open("wb") as f:
            while chunk := r.read(1 << 20):
                f.write(chunk)
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def write_year(con: duckdb.DuckDBPyConnection, y: int, sources: list[str], dest: Path) -> dict | None:
    """The year's scenes, the latest version of each, sorted by tile then time. None when
    the year has no scene (2015 and 2016, before Collection 1 starts)."""
    if not sources:
        return None
    union = " UNION ALL BY NAME ".join(sources)
    # In two steps: DuckDB 1.5 fails ("Failed to cast expression") when the time filter is
    # pushed into the JSON reader.
    con.execute(f"CREATE OR REPLACE TEMP TABLE read AS SELECT {', '.join(COLUMNS)} FROM ({union})")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scenes AS SELECT * FROM read
        WHERE datetime >= TIMESTAMPTZ '{y}-01-01 00:00:00+00' AND datetime < TIMESTAMPTZ '{y + 1}-01-01 00:00:00+00'
        QUALIFY row_number() OVER (PARTITION BY id ORDER BY updated DESC NULLS LAST) = 1""")
    con.execute("DROP TABLE read")
    if not con.execute("SELECT count(*) FROM scenes").fetchone()[0]:
        return None
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""COPY (SELECT * FROM scenes ORDER BY tile, datetime, id) TO '{dest}'
        (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 9, ROW_GROUP_SIZE 20000)""")
    con.execute("DROP TABLE scenes")
    rows, bad, newest, updated = con.execute(f"""SELECT count(*),
        count(*) FILTER (WHERE tile IS NULL OR NOT regexp_full_match(tile, '[0-9]{{2}}[C-X][A-Z]{{2}}')
                         OR split_part(id, '_', 2) <> 'T' || tile OR datetime IS NULL OR cloud_cover IS NULL
                         OR west IS NULL OR south > north),
        strftime(max(datetime) AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ'),
        strftime(max(updated) AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%gZ')
        FROM read_parquet('{dest}')""").fetchone()
    if bad:
        sys.exit(f"{y}: {bad:,} rows without a tile, a time, a cloud cover or a box, or whose id names another tile")
    return {"rows": rows, "bytes": dest.stat().st_size, "newest": newest, "updated": updated,
            "sha256": hashlib.sha256(dest.read_bytes()).hexdigest()}


def counts_agree(rows: int, api: int, current: bool) -> bool:
    """A finished year must match the API's count; the current one may trail it by the
    scenes that arrived during the build (about 600 an hour)."""
    return 0 <= api - rows <= (3000 if current else 0)


def build(out: Path, full: bool, years: list[int] | None, workers: int) -> None:
    out.mkdir(parents=True, exist_ok=True)
    work = out / "_work"
    work.mkdir(exist_ok=True)
    now = dt.datetime.now(dt.UTC)
    old = None if full else published()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")

    class fresh:  # the rows of the JSON lines just read
        @staticmethod
        def format(paths: list[Path]) -> list[str]:
            # an empty file among them trips DuckDB's JSON reader: left out
            names = ", ".join(f"'{p}'" for p in sorted(paths) if p.stat().st_size)
            return [f"SELECT {ROW} FROM read_json([{names}], format = 'newline_delimited', "
                    f"columns = {JSON_COLUMNS})"] if names else []


    if old is None or years:
        # The whole collection (or the years asked for), month by month.
        todo = years or list(range(FIRST_YEAR, now.year + 1))
        parts = months(todo, now)
        print(f"  reading {len(parts)} months of {todo[0]} to {todo[-1]}", flush=True)

        def one(job: tuple[str, str]) -> int:
            part = work / f"{job[0]}.jsonl"
            return sum(1 for _ in part.open()) if part.exists() else crawl(part, datetime=job[1])

        with cf.ThreadPoolExecutor(workers) as ex:
            for (name, _), n in zip(parts, ex.map(one, parts)):
                print(f"    {name}: {n:,}", flush=True)
        sources = {y: fresh.format(list(work.glob(f"{y}-*.jsonl"))) for y in todo}
    else:
        # Only the scenes the API changed since the published index's newest.
        since = dt.datetime.fromisoformat(old["updated_through"].replace("Z", "+00:00")) - OVERLAP
        part = work / "updated.jsonl"
        part.unlink(missing_ok=True)
        n = crawl(part, query={"updated": {"gte": iso(since)}})
        print(f"  {n:,} scenes updated since {iso(since)}", flush=True)
        new = fresh.format([part])
        touched = [r[0] for r in con.execute(
            f"SELECT DISTINCT year(datetime) FROM ({new[0]}) ORDER BY 1").fetchall()] if new else []
        sources = {}
        for y in touched:
            sources[y] = list(new)
            if fetch(f"{PUBLIC}/year={y}/scenes.parquet", work / f"published-{y}.parquet"):
                sources[y].append(f"SELECT * FROM read_parquet('{work}/published-{y}.parquet')")

    files = dict((old or {}).get("years", {})) if not full else {}
    whole = old is None or bool(years)
    for y, src in sorted(sources.items()):
        dest = out / f"year={y}" / "scenes.parquet"
        f = write_year(con, y, src, dest)
        if f is None:
            if matched(year_range(y)):
                sys.exit(f"{y}: no row read, but the API has scenes")
            files.pop(str(y), None)
            continue
        # The API's own count for the year, asked after the read. Only scenes that arrived
        # meanwhile may be missing; anything else (scenes the API dropped, a page lost) and
        # the year is read again whole, once.
        api = matched(year_range(y))
        if not counts_agree(f["rows"], api, y == now.year) and not whole:
            print(f"  {y}: {f['rows']:,} rows but the API counts {api:,}: reading the year again", flush=True)
            for name, rng in months([y], now):
                crawl(work / f"{name}.jsonl", datetime=rng)
            f = write_year(con, y, fresh.format(list(work.glob(f"{y}-*.jsonl"))), dest)
            api = matched(year_range(y))
        print(f"  {y}: {f['rows']:,} rows, {f['bytes'] / 1e6:.1f} MB, newest {f['newest']} (API: {api:,})",
              flush=True)
        if not counts_agree(f["rows"], api, y == now.year):
            sys.exit(f"{y}: {f['rows']:,} rows but the API counts {api:,}")
        files[str(y)] = f
    if not files:
        sys.exit("no scene at all: nothing to publish")
    # Readers open the current year's file from 1 January 00:00 UTC, hours before its first
    # scene is in the API: the year that starts within a day gets its file now, empty.
    for y in sorted({now.year, (now + dt.timedelta(days=1)).year}):
        if str(y) not in files:
            dest = out / f"year={y}" / "scenes.parquet"
            dest.parent.mkdir(parents=True, exist_ok=True)
            con.execute(f"""COPY (SELECT {ROW} FROM (SELECT NULL::VARCHAR AS id, NULL::VARCHAR AS grid,
                NULL::TIMESTAMPTZ AS datetime, NULL::DOUBLE AS cloud_cover, NULL::DOUBLE AS nodata_percent,
                NULL::DOUBLE AS snow_ice_percent, NULL::DOUBLE AS vegetation_percent, NULL::DOUBLE AS water_percent,
                NULL::VARCHAR AS platform, NULL::VARCHAR AS processing_baseline, NULL::DOUBLE AS lat,
                NULL::DOUBLE AS lon, NULL::DOUBLE[] AS bbox, NULL::TIMESTAMPTZ AS created,
                NULL::TIMESTAMPTZ AS updated) WHERE false) TO '{dest}' (FORMAT parquet, COMPRESSION zstd)""")
            files[str(y)] = {"rows": 0, "bytes": dest.stat().st_size, "newest": None, "updated": None,
                             "sha256": hashlib.sha256(dest.read_bytes()).hexdigest()}
            sources[y] = []
            print(f"  {y}: no scene yet, an empty file", flush=True)
    through = max(f["updated"] for f in files.values() if f.get("updated"))
    (out / "index.json").write_text(json.dumps({
        "built": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "source": API.rsplit("/", 1)[0],
        "collection": COLLECTION, "attribution": ATTRIBUTION,
        "scenes": sum(f["rows"] for f in files.values()),
        "newest": max(f["newest"] for f in files.values() if f["newest"]), "updated_through": through,
        "rebuilt": sorted(str(y) for y in sources),
        "years": dict(sorted(files.items()))}, indent=1) + "\n")
    print(f"  {sum(f['rows'] for f in files.values()):,} scenes, newest "
          f"{max(f['newest'] for f in files.values() if f['newest'])}", flush=True)


def publish(out: Path, remote: str) -> None:
    """The year files this build wrote, then index.json, which names them."""
    cache = ["--header-upload", "Cache-Control: public, max-age=300", "--ignore-times"]
    subprocess.run(["rclone", "copy", str(out), f"{remote}/index", "--filter", "+ year=*/scenes.parquet",
                    "--filter", "- *", *cache], check=True)
    subprocess.run(["rclone", "copyto", str(out / "index.json"), f"{remote}/index/index.json", *cache,
                    "--header-upload", "Content-Type: application/json"], check=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--out-dir", type=Path, required=True)
    b.add_argument("--full", action="store_true", help="read the whole collection again")
    b.add_argument("--years", type=int, nargs="+", help="read these years again, keep the others")
    b.add_argument("--workers", type=int, default=4, help="months read at a time")
    p = sub.add_parser("publish")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--remote", required=True)
    a = ap.parse_args()
    if a.cmd == "build":
        build(a.out_dir, a.full, a.years, a.workers)
    else:
        publish(a.out_dir, a.remote)


if __name__ == "__main__":
    main()
