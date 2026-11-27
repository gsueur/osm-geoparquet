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
hours after it is sensed. Three kinds of build:

  the first, or --full   the whole collection, a year at a time, month by month
                         (about 31 million scenes, 2015 to now)
  an update (default)    the scenes whose `updated` is past the published index's mark
                         (index.json's updated_through, less 6 hours), merged into the
                         year files they fall in: in practice the current year's
  --years                those years read again whole; the others and the mark are kept

Checks, in every build:
  - a rebuilt year is counted against the API: a finished year must match, the current
    one may trail by the scenes that arrived meanwhile. A merged year that does not
    match is read again whole.
  - an update also counts the years nothing changed in (one request each): a scene the
    API withdrew leaves no `updated` trace, and the year is read again.
  - a published file is merged into only when it is the one index.json describes
    (sha256), and the files of the other years must have the length it gives;
    otherwise the year is read again.
  - the newest scene must be less than 36 hours old (--max-age): older, and the API
    stopped taking scenes in, or this index stopped following it.
publish sends the years index.json lists as rebuilt, each checked against its sha256,
then index.json. If a run stops between the two, the next update finds the file
differs from index.json and reads that year again. Publish from the workflow (its
concurrency group runs one at a time), not from a workstation while it is scheduled.

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
  python3 scripts/datasets/sentinel2_index.py build --out-dir out/index --years 2024 2025
  python3 scripts/datasets/sentinel2_index.py publish --out-dir out/index --remote parquetry:parquetry/sentinel-2
  python3 scripts/datasets/sentinel2_index.py selftest     # the builds against a stub API, no network
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import shutil
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
            if i >= 1 and "next" not in body and body["limit"] > 500:
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
    """The published index.json, or None when there is no index yet. Asked past the
    edge's cache (files there are kept 5 minutes): the last build's, not the one before."""
    try:
        with urllib.request.urlopen(urllib.request.Request(
                f"{PUBLIC}/index.json?t={time.time_ns()}", headers=AGENT), timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def fetch(url: str, dest: Path) -> bool:
    """A published year file, to a local path. False when there is none."""
    try:
        with urllib.request.urlopen(urllib.request.Request(f"{url}?t={time.time_ns()}", headers=AGENT),
                                    timeout=600) as r, dest.open("wb") as f:
            while chunk := r.read(1 << 20):
                f.write(chunk)
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def size_of(url: str) -> int | None:
    """A published file's length (None when there is none), past the edge's cache."""
    try:
        with urllib.request.urlopen(urllib.request.Request(
                f"{url}?t={time.time_ns()}", headers=AGENT, method="HEAD"), timeout=60) as r:
            return int(r.headers["Content-Length"])
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def rows_of(paths: list[Path]) -> list[str]:
    """The rows of JSON lines just read, as a query (none when every file is empty: an
    empty file among them trips DuckDB's JSON reader)."""
    names = ", ".join(f"'{p}'" for p in sorted(paths) if p.stat().st_size)
    return [f"SELECT {ROW} FROM read_json([{names}], format = 'newline_delimited', "
            f"columns = {JSON_COLUMNS})"] if names else []


def write_year(con: duckdb.DuckDBPyConnection, y: int, sources: list[str], dest: Path) -> dict | None:
    """The year's scenes, the latest version of each, sorted by tile then time. None, and
    no file, when the year has no scene."""
    dest.unlink(missing_ok=True)
    if not sources:
        return None
    union = " UNION ALL BY NAME ".join(sources)
    # In two steps: DuckDB 1.5 fails ("Failed to cast expression") when the time filter is
    # pushed into the JSON reader.
    con.execute(f"CREATE OR REPLACE TEMP TABLE read AS SELECT {', '.join(COLUMNS)} FROM ({union})")
    if n := con.execute("SELECT count(*) FROM read WHERE datetime IS NULL OR id IS NULL").fetchone()[0]:
        sys.exit(f"{y}: {n:,} scenes without an id or a time in what the API returned")
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
    # A box across the antimeridian has west > east: only the latitudes are ordered.
    rows, bad, newest, updated = con.execute(f"""SELECT count(*),
        count(*) FILTER (WHERE tile IS NULL OR NOT regexp_full_match(tile, '[0-9]{{2}}[C-X][A-Z]{{2}}')
                         OR split_part(id, '_', 2) <> 'T' || tile OR datetime IS NULL OR cloud_cover IS NULL
                         OR west IS NULL OR south IS NULL OR east IS NULL OR north IS NULL OR south > north),
        strftime(max(datetime) AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ'),
        strftime(max(updated) AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%gZ')
        FROM read_parquet('{dest}')""").fetchone()
    if bad:
        sys.exit(f"{y}: {bad:,} rows without a tile, a time, a cloud cover or a whole box, or whose id "
                 "names another tile")
    return {"rows": rows, "bytes": dest.stat().st_size, "newest": newest, "updated": updated,
            "sha256": sha256(dest)}


def write_empty(con: duckdb.DuckDBPyConnection, dest: Path) -> dict:
    """A year file with the columns and no row."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    nulls = ", ".join(f"NULL::{t} AS {c}" for c, t in (
        ("id", "VARCHAR"), ("grid", "VARCHAR"), ("datetime", "TIMESTAMPTZ"), ("cloud_cover", "DOUBLE"),
        ("nodata_percent", "DOUBLE"), ("snow_ice_percent", "DOUBLE"), ("vegetation_percent", "DOUBLE"),
        ("water_percent", "DOUBLE"), ("platform", "VARCHAR"), ("processing_baseline", "VARCHAR"),
        ("lat", "DOUBLE"), ("lon", "DOUBLE"), ("bbox", "DOUBLE[]"), ("created", "TIMESTAMPTZ"),
        ("updated", "TIMESTAMPTZ")))
    con.execute(f"COPY (SELECT {ROW} FROM (SELECT {nulls}) WHERE false) TO '{dest}' "
                "(FORMAT parquet, COMPRESSION zstd)")
    return {"rows": 0, "bytes": dest.stat().st_size, "newest": None, "updated": None,
            "sha256": sha256(dest)}


def counts_agree(rows: int, api: int, current: bool) -> bool:
    """A finished year must match the API's count; the current one may trail it by the
    scenes that arrived during the build (about 600 an hour), unless it has none yet."""
    return 0 <= api - rows <= (3000 if current and rows else 0)


def build(out: Path, full: bool, years: list[int] | None, workers: int, max_age: float = 36,
          now: dt.datetime | None = None) -> None:
    now = now or dt.datetime.now(dt.UTC)
    years = sorted(set(years or []))
    if any(y < FIRST_YEAR or y > now.year for y in years):
        sys.exit(f"--years: between {FIRST_YEAR} and {now.year}")
    old = None if full else published()
    if years and old is None:
        sys.exit("--years rebuilds years of a published index, and there is none: the first build "
                 "is --full")
    out.mkdir(parents=True, exist_ok=True)
    work = out / "_work"
    if work.exists():           # nothing an earlier build read is used again
        shutil.rmtree(work)
    work.mkdir()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")

    def read_years(ys: list[int]) -> dict[int, list[str]]:
        """Each year read whole from the API, month by month."""
        parts = months(ys, now)
        print(f"  reading {len(parts)} months of {ys[0]} to {ys[-1]}", flush=True)

        def one(job: tuple[str, str]) -> int:
            part = work / f"{job[0]}.jsonl"
            return crawl(part, datetime=job[1])

        with cf.ThreadPoolExecutor(workers) as ex:
            for (name, _), n in zip(parts, ex.map(one, parts)):
                print(f"    {name}: {n:,}", flush=True)
        return {y: rows_of(list(work.glob(f"{y}-*.jsonl"))) for y in ys}

    files = dict(old["years"]) if old else {}
    whole: set[int] = set()        # years read whole in this build
    through = old["updated_through"] if old else None
    # year -> the queries its rows come from; None: the year is read whole from the API, when
    # its file is written (one year at a time: the whole collection is 12 GB of JSON lines)
    sources: dict[int, list[str] | None]
    if old is None:
        # The whole collection.
        sources = dict.fromkeys(range(FIRST_YEAR, now.year + 1))
        whole = set(sources)
    elif years:
        # The years asked for, read again; the others and the update mark are kept.
        sources = dict.fromkeys(years)
        whole = set(sources)
    else:
        # Only the scenes the API changed since the published index's mark.
        since = dt.datetime.fromisoformat(through.replace("Z", "+00:00")) - OVERLAP
        part = work / "updated.jsonl"
        n = crawl(part, query={"updated": {"gte": iso(since)}})
        print(f"  {n:,} scenes updated since {iso(since)}", flush=True)
        new = rows_of([part])
        seen, touched = (con.execute(
            f"""SELECT strftime(max(updated) AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%S.%gZ'),
                       list(DISTINCT year(datetime) ORDER BY year(datetime)) FROM ({new[0]})""").fetchone()
            if new else (None, []))
        through = max(through, seen or through)
        sources, reread = {}, []
        for y in touched:
            want = files.get(str(y))
            got = work / f"published-{y}.parquet"
            if not want:
                sources[y] = list(new)
            elif fetch(f"{PUBLIC}/year={y}/scenes.parquet", got) and sha256(got) == want["sha256"]:
                sources[y] = [*new, f"SELECT * FROM read_parquet('{got}')"]
            else:
                # the file is not the one index.json describes (an upload cut short): not merged into
                print(f"  {y}: the published file is not the one index.json describes", flush=True)
                reread.append(y)
        # The years no scene changed in. Is the published file the length index.json gives (a
        # publish cut short before index.json leaves a file it does not describe)? Is it still
        # the API's count (a scene it withdrew leaves no trace)?
        for y in sorted(int(k) for k in files if int(k) not in touched and int(k) not in reread):
            if size_of(f"{PUBLIC}/year={y}/scenes.parquet") != files[str(y)]["bytes"]:
                print(f"  {y}: the published file is not the one index.json describes", flush=True)
                reread.append(y)
                continue
            api = matched(year_range(y))
            if not counts_agree(files[str(y)]["rows"], api, y == now.year):
                print(f"  {y}: {files[str(y)]['rows']:,} rows published, the API counts {api:,}", flush=True)
                reread.append(y)
        sources |= dict.fromkeys(reread)
        whole = set(reread)

    def settle(y: int, src: list[str] | None) -> None:
        """The year's file and its entry, counted against the API (asked after the read: only
        scenes that arrived meanwhile may be missing). A year that was merged and does not
        match is read again whole, once."""
        dest = out / f"year={y}" / "scenes.parquet"
        if src is None:
            src = read_years([y])[y]
        for attempt in (1, 2):
            f = write_year(con, y, src, dest)
            api = matched(year_range(y))
            rows = f["rows"] if f else 0
            if counts_agree(rows, api, y == now.year):
                break
            if attempt == 2 or y in whole:
                sys.exit(f"{y}: {rows:,} rows but the API counts {api:,}")
            print(f"  {y}: {rows:,} rows but the API counts {api:,}: reading the year again", flush=True)
            src = read_years([y])[y]
            whole.add(y)
        for part in work.glob(f"{y}-*.jsonl"):
            part.unlink()
        if f is None and str(y) in files:
            # a published year the API has no scene left for: its file is replaced by an empty one
            files[str(y)] = write_empty(con, dest)
            print(f"  {y}: no scene left, an empty file", flush=True)
            return
        if f is None:
            print(f"  {y}: no scene", flush=True)
            return
        print(f"  {y}: {f['rows']:,} rows, {f['bytes'] / 1e6:.1f} MB, newest {f['newest']} (API: {api:,})",
              flush=True)
        files[str(y)] = f

    rebuilt = set()     # the years whose file this build wrote (a year left without a scene has none)
    for y, src in sorted(sources.items()):
        settle(y, src)
        if str(y) in files:
            rebuilt.add(y)
    # Readers open the current year's file from 1 January 00:00 UTC, hours before its first
    # scene is in the API: the year that starts within three days gets its file now, empty
    # (the build runs once a day, hours late at times, and a run may be dropped).
    for y in sorted({now.year, (now + dt.timedelta(days=3)).year}):
        if str(y) in files:
            continue
        if y <= now.year and matched(year_range(y)):
            whole.add(y)
            settle(y, None)
        if str(y) not in files:
            files[str(y)] = write_empty(con, out / f"year={y}" / "scenes.parquet")
            print(f"  {y}: no scene yet, an empty file", flush=True)
        rebuilt.add(y)
    if not any(f["rows"] for f in files.values()):
        sys.exit("no scene at all: nothing to publish")
    newest = max(f["newest"] for f in files.values() if f["newest"])
    age = (now - dt.datetime.fromisoformat(newest.replace("Z", "+00:00"))).total_seconds() / 3600
    print(f"  {sum(f['rows'] for f in files.values()):,} scenes, newest {newest} ({age:.1f} h ago)",
          flush=True)
    if max_age and age > max_age:
        sys.exit(f"the newest scene is {age:.1f} hours old (more than {max_age:g}): the API stopped "
                 "taking scenes in, or this index stopped following it")
    if old is None:
        # every year was read: the mark is the newest change seen, never past the start of the read
        through = min(max(f["updated"] for f in files.values() if f["updated"]), iso(now))
    (out / "index.json").write_text(json.dumps({
        "built": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "source": API.rsplit("/", 1)[0],
        "collection": COLLECTION, "attribution": ATTRIBUTION,
        "scenes": sum(f["rows"] for f in files.values()), "newest": newest,
        "updated_through": through, "rebuilt": sorted(str(y) for y in rebuilt),
        "years": dict(sorted(files.items()))}, indent=1) + "\n")


def publish(out: Path, remote: str) -> None:
    """The year files the build rewrote (index.json's `rebuilt`, each checked against its
    sha256 before anything is sent), then index.json, which describes them. Files of other
    years that an earlier build left in the folder are not sent."""
    index = json.loads((out / "index.json").read_text())
    todo = []
    for y in index["rebuilt"]:
        path = out / f"year={y}" / "scenes.parquet"
        if y not in index["years"]:
            sys.exit(f"{y}: rebuilt, but not in index.json")
        if not path.exists() or sha256(path) != index["years"][y]["sha256"]:
            sys.exit(f"{path}: not the file index.json describes")
        todo.append((y, path))
    cache = ["--header-upload", "Cache-Control: public, max-age=300", "--ignore-times"]
    for y, path in todo:
        subprocess.run(["rclone", "copyto", str(path), f"{remote}/index/year={y}/scenes.parquet", *cache],
                       check=True)
    subprocess.run(["rclone", "copyto", str(out / "index.json"), f"{remote}/index/index.json", *cache,
                    "--header-upload", "Content-Type: application/json"], check=True)


def selftest() -> None:
    """The three kinds of build against a stub of the API and of what is published: no network."""
    import tempfile
    g = globals()
    now = dt.datetime(2026, 6, 15, 12, tzinfo=dt.UTC)       # a fixed clock: the years below never move
    y0, y1, y2 = now.year - 2, now.year - 1, now.year

    def scene(tile: str, when: dt.datetime, cloud: float, updated: dt.datetime) -> dict:
        return {"id": f"S2A_T{tile}_{when:%Y%m%dT%H%M%S}_L2A", "bbox": [4.2, 45.0, 5.7, 46.0],
                "properties": {"datetime": iso(when), "created": iso(updated), "updated": iso(updated),
                               "eo:cloud_cover": cloud, "grid:code": f"MGRS-{tile.lstrip('0')}",
                               "platform": "sentinel-2a", "proj:centroid": {"lat": 45.5, "lon": 5.0},
                               "s2:nodata_pixel_percentage": 0, "s2:snow_ice_percentage": 0,
                               "s2:vegetation_percentage": 50, "s2:water_percentage": 1,
                               "s2:processing_baseline": "05.11"}}

    def at(y: int, m: int = 3, d: int = 1) -> dt.datetime:
        return dt.datetime(y, m, d, 10, 30, tzinfo=dt.UTC)

    long_ago = now - dt.timedelta(days=40)
    api = [scene("31TGL", at(y0), 10, long_ago), scene("09TWM", at(y0, 7), 20, long_ago),
           scene("31TGL", at(y1), 30, long_ago), scene("31TFL", at(y1, 7), 40, long_ago),
           scene("31TGL", now - dt.timedelta(hours=30), 50, now - dt.timedelta(hours=28))]

    def stub_search(body: dict, attempts: int = 7) -> dict:
        hits = api
        if "datetime" in body:
            a, b = body["datetime"].split("/")
            hits = [s for s in hits if a <= s["properties"]["datetime"] <= b]
        if "query" in body:
            hits = [s for s in hits if s["properties"]["updated"] >= body["query"]["updated"]["gte"]]
        # two scenes a page, so a month with more is read over several
        start, size = body.get("next", 0), min(body["limit"], 2)
        more = [{"rel": "next", "body": body | {"next": start + size}}] if start + size < len(hits) else []
        return {"features": hits[start:start + size], "links": more, "context": {"matched": len(hits)}}

    with tempfile.TemporaryDirectory() as tmp:
        live = Path(tmp) / "live"       # what is published

        def stub_published() -> dict | None:
            return json.loads((live / "index.json").read_text()) if (live / "index.json").exists() else None

        def stub_fetch(url: str, dest: Path) -> bool:
            src = live / url.removeprefix(f"{PUBLIC}/")
            if src.exists():
                shutil.copy(src, dest)
            return src.exists()

        def release(out: Path) -> dict:
            """What publish does, into the folder: the rebuilt years, then index.json."""
            index = json.loads((out / "index.json").read_text())
            for y in index["rebuilt"]:
                (live / f"year={y}").mkdir(parents=True, exist_ok=True)
                shutil.copy(out / f"year={y}" / "scenes.parquet", live / f"year={y}" / "scenes.parquet")
            shutil.copy(out / "index.json", live / "index.json")
            return index

        def rows(y: int) -> list[tuple]:
            return duckdb.connect().execute(
                f"SELECT id, tile, round(cloud_cover) FROM read_parquet('{live}/year={y}/scenes.parquet') "
                "ORDER BY id").fetchall()

        def stub_size(url: str) -> int | None:
            src = live / url.removeprefix(f"{PUBLIC}/")
            return src.stat().st_size if src.exists() else None

        def run(name: str, full: bool = False, years: list[int] | None = None) -> dict:
            build(Path(tmp) / name, full, years, 2, now=now)
            return release(Path(tmp) / name)

        saved = {k: g[k] for k in ("search", "published", "fetch", "size_of")}
        g.update(search=stub_search, published=stub_published, fetch=stub_fetch, size_of=stub_size)
        try:
            # 1. nothing published: --years is refused, a first build reads everything
            try:
                build(Path(tmp) / "a", False, [y1], 2, now=now)
                raise AssertionError("--years with nothing published should stop")
            except SystemExit:
                pass
            api += [scene("31TGL", at(y0, 3, d), 15, long_ago) for d in (2, 3, 4)]      # a month of 4: two pages
            first = run("a")
            assert len(rows(y0)) == 5, rows(y0)
            del api[-3:]
            first = run("a2", full=True)
            assert first["scenes"] == 5 and [len(rows(y)) for y in (y0, y1, y2)] == [2, 2, 1], first
            assert rows(y0)[0][1] == "09TWM", "zones 1 to 9 keep their zero"
            assert set(first["rebuilt"]) >= {str(y0), str(y1), str(y2)}

            # 2. an update: a past scene corrected, a new scene; the untouched year is left alone
            api[2] = scene("31TGL", at(y1), 5, now - dt.timedelta(hours=1))
            api.append(scene("31TFL", now - dt.timedelta(hours=3), 60, now - dt.timedelta(hours=1)))
            second = run("b")
            assert second["rebuilt"] == [str(y1), str(y2)], second["rebuilt"]
            assert second["years"][str(y0)] == first["years"][str(y0)]
            assert (api[2]["id"], "31TGL", 5.0) in rows(y1) and len(rows(y1)) == 2, rows(y1)
            assert len(rows(y2)) == 2 and second["updated_through"] > first["updated_through"]
            assert not (Path(tmp) / "b" / f"year={y0}").exists(), "an untouched year is not rewritten"

            # 3. a scene withdrawn from a year nothing else changed in: seen by the count, read again
            gone = api.pop(0)
            third = run("c")
            assert str(y0) in third["rebuilt"] and gone["id"] not in [r[0] for r in rows(y0)], third

            # 4. --years reads those years again and keeps the update mark
            api[0] = scene("09TWM", at(y0, 7), 99, now + dt.timedelta(hours=1))
            fourth = run("d", years=[y0, y0])
            assert fourth["rebuilt"] == [str(y0)] and rows(y0)[0][2] == 99.0, fourth
            assert fourth["updated_through"] == third["updated_through"]

            # 5. a published file that is not the one index.json describes is not merged into
            (live / f"year={y2}" / "scenes.parquet").write_bytes(
                (live / f"year={y0}" / "scenes.parquet").read_bytes())
            api.append(scene("31TGL", now - dt.timedelta(hours=2), 70, now - dt.timedelta(minutes=30)))
            fifth = run("e")
            assert len(rows(y2)) == 3 and str(y2) in fifth["rebuilt"], rows(y2)

            # 5b. the same for a year nothing changed in: seen by its length
            (live / f"year={y1}" / "scenes.parquet").write_bytes(b"cut short")
            assert str(y1) in run("e2")["rebuilt"] and len(rows(y1)) == 2

            # 5c. a year the API has no scene left for keeps a file, empty
            kept = [s for s in api if not s["properties"]["datetime"].startswith(str(y0))]
            api[:] = kept
            sixth = run("e3")
            assert sixth["years"][str(y0)]["rows"] == 0 and rows(y0) == [], sixth["years"][str(y0)]

            # 6. publish refuses a file that is not the one it built
            (Path(tmp) / "e3" / f"year={y0}" / "scenes.parquet").write_bytes(b"not the file")
            try:
                publish(Path(tmp) / "e3", "nowhere:")
                raise AssertionError("publish should stop on a changed file")
            except SystemExit:
                pass

            # 7. a stale API stops the build
            del api[:]
            api.append(scene("31TGL", now - dt.timedelta(hours=40), 1, now - dt.timedelta(hours=39)))
            try:
                build(Path(tmp) / "f", True, None, 2, now=now)
                raise AssertionError("a newest scene 40 hours old should stop the build")
            except SystemExit:
                pass
        finally:
            g.update(saved)
    print("selftest ok")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--out-dir", type=Path, required=True)
    which = b.add_mutually_exclusive_group()
    which.add_argument("--full", action="store_true", help="read the whole collection again")
    which.add_argument("--years", type=int, nargs="+",
                       help="read these years again; the others, and the published index's update mark, are kept")
    b.add_argument("--workers", type=int, default=4, help="months read at a time")
    b.add_argument("--max-age", type=float, default=36,
                   help="stop when the newest scene is older than this many hours (0: no check)")
    p = sub.add_parser("publish")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--remote", required=True)
    sub.add_parser("selftest")
    a = ap.parse_args()
    if a.cmd == "build":
        build(a.out_dir, a.full, a.years, a.workers, a.max_age)
    elif a.cmd == "publish":
        publish(a.out_dir, a.remote)
    else:
        selftest()


if __name__ == "__main__":
    main()
