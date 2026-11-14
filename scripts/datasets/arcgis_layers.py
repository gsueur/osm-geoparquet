#!/usr/bin/env python3
"""
A state's ArcGIS REST layers as GeoParquet and PMTiles: the engine maine.py,
nh.py, ny.py and ca.py share. Each of them is a Profile (its servers, layer CSV, themes,
colours, outline and attribution); this reads each layer from its query
endpoint and writes one GeoParquet file per layer, and its PMTiles archive,
grouped by theme:

  <out>/<theme>/<layer>.parquet
  <out>/<theme>/<layer>.pmtiles
  <out>/<theme>/<layer>.png         the catalog thumbnail
  <out>/<theme>/_manifest.json      rows, sizes, sha256, titles, sources, tile settings
  <out>/<theme>/ATTRIBUTION.txt

The layer CSV says which layers: one row per source layer, its theme, its
server (a key of the profile's servers), the service and layer number. Rows
sharing an id are one published layer, each source told apart by a column
the CSV gives (FOLD_COLUMNS: `year`, `category`, `from_year` and `to_year`,
`scenario`).

Fetching: pages of the layer's maxRecordCount (at most 2,000), walked on the
object id (`OBJECTID > last`, ordered on it), in the layer's own spatial
reference or the profile's CRS (outSR: the server projects), as Esri JSON
read by GDAL, which keeps the field types (dates as timestamps). A large
layer's ids are cut into ranges walked by up to page_workers threads, put
back together in id order (the rows, and so the files' bytes, are those of
one walk). The rows fetched must equal the layer's returnCountOnly count, or
the layer fails. Features without a geometry are dropped and counted in the
manifest.

Building: --workers layers at a time, each in a process of its own, the
outline first, then the largest first, never two of over BIG features side
by side. With --publish, the main process uploads each layer as soon as it
is built (Publisher): a run stopped by the time budget, a timeout or a
cancel keeps all it published.

Columns and values are the publisher's, unchanged, but for the columns a
profile drops (drop_columns: New York's parcel owner names and mailing
addresses), listed in the manifest entry. The geometry is 2D, with
a `bbox` column declared as the GeoParquet covering; rows are sorted on a
Hilbert curve and cut into row groups of at most 4 MiB of geometry. Writing,
tiling and thumbnails are massgis.py's.

A build reads the published _source.json and rebuilds only the layers whose
fingerprint moved (each source's last data and schema edit and its count),
or the ones named with --layer. A source without edit dates is fingerprinted
as its server says (`undated`): `daily`, the build day (rebuilt every run);
`monthly`, the count, the highest object id, the fields and the month (a
rebuild when rows are added or removed or the schema changes, and at least
once a month for edits in place); or `changes`, the same without the month
(for a publisher that replaces a layer rather than edit it in place, and
layers too large to rebuild every month). Either way scripts/filedates.py keeps a
file's date, and the file on the bucket, as they were when its bytes did
not change.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import html
import http.client
import itertools
import json
import math
import multiprocessing
import os
import queue
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import (FIRST_COMPLETED, ProcessPoolExecutor,
                                ThreadPoolExecutor, wait)
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.massgis import (BBOX, close_rings, get_json, thumbnail, tile_layer,  # noqa: E402
                              write_layer)

# The columns that tell a folded layer's sources apart, and their types.
FOLD_COLUMNS = {"year": "SMALLINT", "category": "VARCHAR", "from_year": "SMALLINT",
                "to_year": "SMALLINT", "scenario": "VARCHAR"}
PAGE = 2000
# Esri's own codes for Web Mercator.
WKID_ALIASES = {102100: 3857, 102113: 3857, 900913: 3857}
# A page the server cannot answer (Maine DEP's biomonitoring drainages: 50
# full-resolution basins are 30 MB of JSON, 500 fail) is asked again in
# halves, down to this many features.
MIN_PAGE = 25
# Numbers the page files of a run (fetch_source).
PAGE_FILES = itertools.count()


@dataclass
class Profile:
    """One state's layers. servers: CSV `server` value -> {url (with
    {service} and {layer}), kind, publisher, undated ('daily', 'monthly'
    or 'changes'), and optionally workers (threads reading one layer)}."""
    name: str                      # the bucket prefix, e.g. "maine"
    public: str                    # where the published files are read
    layers_csv: Path
    servers: dict[str, dict]
    themes: dict[str, str]
    colors: dict[str, str]
    outline: tuple[str, str]       # (theme, layer) the thumbnails are drawn over
    outline_crs: str
    theme_title: Callable[[str], str]
    attribution: Callable[[str, list[dict]], str]
    crs: str | None = None         # every layer in this CRS; None: each its own
    tile_max_zoom: dict[str, int] = field(default_factory=dict)
    # Layers published as GeoParquet only: too dense for any tile a browser
    # can load.
    untiled: frozenset[str] = frozenset()
    # Folded layer id -> the columns kept as columns (lower case); each
    # source's other fields go into one JSON text column, other_attributes.
    # For folds whose sources share few fields (CA's 96 vegetation maps have
    # 738 between them): a union by name would be hundreds of columns wide.
    fold_keep: dict[str, tuple[str, ...]] = field(default_factory=dict)
    # Layer id -> the key column (case-insensitive) its sources are joined
    # on instead of unioned: layers drawing the same features, each with its
    # own attributes (WA's ShoreZone inventory: 13 layers of the same 7,365
    # shore units). The first source gives the geometry and the shared
    # columns; the others add the columns it lacks.
    join_on: dict[str, str] = field(default_factory=dict)
    # Layer id -> the publisher's columns left out of the file.
    drop_columns: dict[str, tuple[str, ...]] = field(default_factory=dict)
    # Threads reading one large layer, each a range of its object ids (a
    # server's `workers` overrides it: 1 for a fragile one).
    page_workers: int = 4


def layers(p: Profile) -> list[dict]:
    """The CSV's rows grouped into published layers, in CSV order, the
    state outline first (the thumbnails draw on it)."""
    with p.layers_csv.open() as f:
        rows = list(csv.DictReader(f))
    unknown = {r["theme"] for r in rows} - set(p.themes)
    if unknown:
        sys.exit(f"{p.layers_csv.name}: themes not in THEMES: {sorted(unknown)}")
    out: dict[str, dict] = {}
    for r in rows:
        server = r.get("server") or ""
        if server not in p.servers:
            sys.exit(f"{p.layers_csv.name}: {r['id']}: unknown server {server!r}")
        e = out.setdefault(r["id"], {"id": r["id"], "theme": r["theme"], "title": r["title"],
                                     "note": r["note"], "server": server, "sources": []})
        if e["theme"] != r["theme"] or e["server"] != server:
            sys.exit(f"{p.layers_csv.name}: {r['id']} is in two themes or on two servers")
        fold = {c: (r[c] if t == "VARCHAR" else int(r[c]))
                for c, t in FOLD_COLUMNS.items() if r.get(c)}
        e["sources"].append({"server": server, "service": r["service"],
                             "layer": int(r["layer"]), "fold": fold,
                             "year": fold.get("year")})
    for e in out.values():
        keys = [tuple(sorted(s["fold"].items())) for s in e["sources"]]
        cols = {tuple(sorted(s["fold"])) for s in e["sources"]}
        if len(e["sources"]) > 1 and (() in keys or len(set(keys)) != len(keys) or len(cols) != 1):
            sys.exit(f"{p.layers_csv.name}: {e['id']} folds several layers: each needs its own "
                     f"value in the same fold columns ({', '.join(FOLD_COLUMNS)})")
    ordered = list(out.values())
    ordered.sort(key=lambda e: (e["theme"], e["id"]) != p.outline)
    return ordered


def url(p: Profile, src: dict) -> str:
    return p.servers[src.get("server") or ""]["url"].format(**src)


def get(u: str, params: dict | None = None, attempts: int = 6) -> dict:
    """An ArcGIS REST call as JSON, retried with backoff: ArcGIS answers an
    overloaded request with HTTP 200 and an `error` object as often as with a
    5xx. A response cut off midway (IncompleteRead) is retried too."""
    full = u + "?" + urllib.parse.urlencode({**(params or {}), "f": "json"})
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(full, timeout=180) as r:
                doc = json.load(r)
            if "error" not in doc:
                return doc
            err = doc["error"]
        except (urllib.error.URLError, http.client.HTTPException, TimeoutError, ConnectionError,
                json.JSONDecodeError) as e:
            err = e
        if attempt == attempts:
            raise RuntimeError(f"{u}: {err}")
        time.sleep(min(60, 5 * 2 ** (attempt - 1)))
    raise AssertionError


def ms_iso(ms: int | None) -> str | None:
    if not ms:
        return None
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def source_info(p: Profile, src: dict) -> dict:
    """What the service says of one source layer: its CRS, page size, object
    id field, last edits and count."""
    u = url(p, src)
    info = get(u)
    sr = info.get("sourceSpatialReference") or (info.get("extent") or {}).get("spatialReference") or {}
    wkid = sr.get("latestWkid") or sr.get("wkid")
    if not wkid:
        # No spatial reference in the layer's description (a few hosted CSV
        # layers): the one its query answers in.
        ext = get(u + "/query", {"where": "1=1", "returnExtentOnly": "true"}, attempts=2)
        sr = (ext.get("extent") or {}).get("spatialReference") or ext.get("spatialReference") or {}
        wkid = sr.get("latestWkid") or sr.get("wkid")
    wkid = WKID_ALIASES.get(wkid, wkid)
    if not wkid and sr.get("wkt") and p.crs:
        # A custom WKT and no id (WA Nearshore's kelp history: Washington
        # South's parameters on an unnamed GRS80 datum): read in the
        # profile's CRS, which the server projects to from its own WKT.
        wkid = int(p.crs.split(":")[1])
    if not wkid:
        raise RuntimeError(f"{u}: no spatial reference")
    edits = info.get("editingInfo") or {}
    # A map service layer names its object id only among its fields.
    oid = info.get("objectIdField") or next(
        (f["name"] for f in info.get("fields") or [] if f["type"] == "esriFieldTypeOID"), None)
    if not oid:
        raise RuntimeError(f"{u}: no object id field")
    count = get(u + "/query", {"where": "1=1", "returnCountOnly": "true"})["count"]
    # A service that can neither page nor order (the California Water Boards'
    # map services) is walked in windows of object ids instead.
    paging = (info.get("advancedQueryCapabilities") or {}).get("supportsPagination") is not False
    max_oid = None
    if not edits or not paging:
        # No edit dates (Maine DEP's map services): the highest object id
        # and the fields stand in for them in the fingerprint.
        max_oid = highest_oid(u, oid)
    fields = hashlib.sha256(json.dumps(
        [(f["name"], f["type"]) for f in info.get("fields") or []]).encode()).hexdigest()[:12]
    return {
        "url": u, "server": src.get("server") or "", "fold": src["fold"],
        "year": src["year"], "name": info.get("name"),
        "crs": f"EPSG:{wkid}", "oid": oid, "max_oid": max_oid, "fields": fields,
        "field_names": sorted({f["name"].lower() for f in info.get("fields") or []}),
        "page": min(PAGE, info.get("maxRecordCount") or PAGE), "paging": paging,
        "geometry_type": info.get("geometryType"),
        "data_edited": ms_iso(edits.get("dataLastEditDate") or edits.get("lastEditDate")),
        "schema_edited": ms_iso(edits.get("schemaLastEditDate")),
        "count": count,
        "description": clean(info.get("description")),
        "copyright": clean(info.get("copyrightText")),
    }


def highest_oid(u: str, oid: str) -> int | None:
    """The layer's highest object id: its statistics, or, from a service
    that has none, the largest of its ids."""
    try:
        stats = get(u + "/query", {"where": "1=1", "outStatistics": json.dumps(
            [{"statisticType": "max", "onStatisticField": oid, "outStatisticFieldName": "m"}])},
            attempts=2)
        feats = stats.get("features") or [{}]
        return (feats[0].get("attributes") or {}).get("m")
    except RuntimeError:
        ids = get(u + "/query", {"where": "1=1", "returnIdsOnly": "true"}).get("objectIds") or []
        return max(ids) if ids else None


def clean(text: str | None) -> str | None:
    """Esri's HTML descriptions as plain text."""
    if not text:
        return None
    text = re.sub(r"<br\s*/?>|</p>|</div>", "\n", text, flags=re.IGNORECASE)
    text = html.unescape(re.sub(r"<[^>]+>", "", text))
    text = re.sub(r"[ \t\xa0]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text).strip()
    return text or None


def upstream(p: Profile, only: list[str] | None = None) -> dict:
    """Each layer's sources and a fingerprint of their last edits and counts:
    a layer is rebuilt when it moves. A source without an edit date is
    fingerprinted as its server's `undated` says."""
    rows = [r for r in layers(p) if not only or r["id"] in only]
    flat = [(r["id"], s) for r in rows for s in r["sources"]]

    def safe(src: dict) -> dict:
        # One unreachable service must not stop the healthy layers.
        try:
            return source_info(p, src)
        except (Exception, SystemExit) as e:  # noqa: BLE001
            return {"error": f"{url(p, src)}: {type(e).__name__}: {e}"[:500]}

    with ThreadPoolExecutor(8) as ex:
        infos = list(ex.map(lambda x: safe(x[1]), flat))
    out: dict[str, dict] = {}
    today = dt.date.today().isoformat()
    for (layer_id, _), info in zip(flat, infos):
        out.setdefault(layer_id, {"sources": []})["sources"].append(info)
    month = today[:7]

    def state(s: dict) -> str:
        if s["data_edited"]:
            return f"{s['url']}:{s['data_edited']}:{s['schema_edited']}:{s['count']}"
        undated = p.servers[s["server"]].get("undated")
        if undated == "monthly":
            # No edit dates: rows added or removed, a new field, or a new month.
            return f"{s['url']}:{month}:{s['max_oid']}:{s['fields']}:{s['count']}"
        if undated == "changes":
            return f"{s['url']}:{s['max_oid']}:{s['fields']}:{s['count']}"
        return f"{s['url']}:{today}:{s['schema_edited']}:{s['count']}"

    for e in out.values():
        errors = [s["error"] for s in e["sources"] if "error" in s]
        if errors:
            # Unknown upstream state: no fingerprint, the layer keeps what
            # is published and is reported as failed.
            e.update({"error": "; ".join(errors), "fingerprint": None,
                      "last_modified": None, "count": None})
            continue
        key = "|".join(state(s) for s in e["sources"])
        e["fingerprint"] = hashlib.sha256(key.encode()).hexdigest()[:16]
        edited = [s["data_edited"] for s in e["sources"] if s["data_edited"]]
        e["last_modified"] = max(edited) if edited else None
        e["count"] = sum(s["count"] for s in e["sources"])
    return out


def finite(c) -> bool:
    """Every number in a geometry's coordinates is a finite number."""
    if isinstance(c, list):
        return all(finite(x) for x in c)
    return isinstance(c, (int, float)) and not isinstance(c, bool) and math.isfinite(c)


def clean_geometry(f: dict) -> None:
    """A geometry with a non-numeric coordinate ("NaN" for an unlocated
    point: Maine DEP's lake sample stations) becomes null, so it is left out
    and counted like a feature without one, instead of failing the layer."""
    g = f.get("geometry")
    if not g:
        return
    coords = [g[k] for k in ("x", "y", "points", "paths", "rings") if k in g]
    if not finite(coords):
        f["geometry"] = None


def oid_ranges(info: dict, workers: int) -> list[tuple[int, int | None]]:
    """The source's object ids cut into up to `workers` ranges (lo, hi], each
    walked by a thread of its own: [(-1, None)] (one walk) for a small layer,
    one thread, or a service that cannot say its lowest and highest ids."""
    pages = math.ceil(info["count"] / info["page"])
    if workers < 2 or pages < 4:
        return [(-1, None)]
    oid = info["oid"]
    try:
        stats = get(info["url"] + "/query", {"where": "1=1", "outStatistics": json.dumps(
            [{"statisticType": t, "onStatisticField": oid, "outStatisticFieldName": t}
             for t in ("min", "max")])}, attempts=2)
        a = {k.lower(): v for k, v in
             ((stats.get("features") or [{}])[0].get("attributes") or {}).items()}
        lo, hi = int(a["min"]), int(a["max"])
    except (RuntimeError, KeyError, TypeError, ValueError):
        return [(-1, None)]
    n = min(workers, pages // 2)
    cuts = [lo - 1 + (hi - lo + 1) * i // n for i in range(n)] + [hi]
    # The last range is open: rows added past the highest id while the layer
    # is read are fetched too, and the count check sees them.
    return [(a, b) for a, b in zip(cuts, cuts[1:-1] + [None]) if b is None or b > a]


def fetch_source(con, info: dict, work: Path, table: str, target_crs: str | None = None,
                 workers: int = 1) -> int:
    """One source layer into `table`, walked on its object id, in target_crs
    (default: its own). Returns the rows fetched.

    With workers > 1 the ids are cut into ranges walked by as many threads,
    each range in id order into a table of its own; the tables are then
    appended in range order, so `table` has the rows in the order one walk
    gives (the files are sorted on a Hilbert key with no tie-breaker: rows
    on the same key keep this order, and the bytes stay the same)."""
    target_crs = target_crs or info["crs"]
    oid = info["oid"]
    ranges = oid_ranges(info, workers)
    names = info.get("fold_names") or {}
    extra = "".join(
        f", {v}::{FOLD_COLUMNS[c]} AS {names.get(c, c)}" if FOLD_COLUMNS[c] != "VARCHAR"
        else f", '{str(v).replace(chr(39), chr(39) * 2)}'::VARCHAR AS {names.get(c, c)}"
        for c, v in (info.get("fold") or {}).items())
    pages: queue.Queue = queue.Queue(maxsize=2 * len(ranges))
    stop = threading.Event()

    def put(item: tuple) -> None:
        """Hand a page over, unless the reading thread gave up."""
        while not stop.is_set():
            try:
                pages.put(item, timeout=1)
                return
            except queue.Full:
                continue
        if isinstance(item[1], Path):
            item[1].unlink(missing_ok=True)

    paging = info.get("paging", True)

    def walk(k: int, lo: int, hi: int | None) -> None:
        last, page = lo, info["page"]
        try:
            while not stop.is_set():
                if paging:
                    where = f"{oid} > {last}" + (f" AND {oid} <= {hi}" if hi is not None else "")
                    params = {"where": where, "outFields": "*", "orderByFields": f"{oid} ASC",
                              "resultRecordCount": page}
                else:
                    # No paging and no ordering: a window of ids at a time,
                    # sorted here, until the highest id.
                    if info.get("max_oid") is None or last >= info["max_oid"]:
                        break
                    params = {"where": f"{oid} > {last} AND {oid} <= {last + page}",
                              "outFields": "*"}
                params |= {"outSR": target_crs.split(":")[1], "returnZ": "false", "returnM": "false"}
                while True:
                    try:
                        # Two tries before halving; the smallest page gets them all.
                        doc = get(info["url"] + "/query", params,
                                  attempts=2 if page > MIN_PAGE else 6)
                        break
                    except RuntimeError as e:
                        if page <= MIN_PAGE:
                            raise
                        page = max(MIN_PAGE, page // 2)
                        if paging:
                            params["resultRecordCount"] = page
                        else:
                            params["where"] = f"{oid} > {last} AND {oid} <= {last + page}"
                        print(f"    {table}: {e}; pages of {page}", flush=True)
                feats = doc.get("features") or []
                if not paging:
                    feats.sort(key=lambda f: f["attributes"][oid])
                    if not feats:
                        last += page  # a gap in the ids
                        continue
                if not feats:
                    break
                if "NaN" in json.dumps([f.get("geometry") for f in feats]):
                    for f in feats:
                        clean_geometry(f)
                # A page file of its own, in the whole run: a page GDAL failed
                # to open stays marked as open in this thread, and any later
                # read of that path fails ("GDALOpen() called recursively").
                page_file = work / f"page-{table}-{os.getpid()}-{next(PAGE_FILES)}.json"
                # The features last: GDAL tells Esri JSON from GeoJSON by the
                # keys at the start of the file, and NH GRANIT's servers put
                # `features` before `geometryType`.
                page_file.write_text(json.dumps(
                    {key: v for key, v in doc.items() if key != "features"} | {"features": feats}))
                put((k, page_file, len(feats)))
                last = max(f["attributes"][oid] for f in feats) if paging else last + page
            put((k, None, 0))
        except BaseException as e:  # noqa: BLE001 - handed to the reading thread
            put((k, e, 0))

    # One walk writes straight into the table.
    chunks = [table] if len(ranges) == 1 else [f"{table}_r{k}" for k in range(len(ranges))]
    created = [False] * len(ranges)
    n, running = 0, len(ranges)
    with ThreadPoolExecutor(len(ranges)) as ex:
        for k, (lo, hi) in enumerate(ranges):
            ex.submit(walk, k, lo, hi)
        try:
            while running:
                k, item, count = pages.get()
                if item is None:
                    running -= 1
                    continue
                if isinstance(item, BaseException):
                    raise item
                sel = f"SELECT *{extra} FROM ST_Read('{item}')"
                try:
                    if created[k]:
                        con.execute(f"INSERT INTO {chunks[k]} BY NAME {sel}")
                    else:
                        con.execute(f"CREATE TABLE {chunks[k]} AS {sel}")
                        created[k] = True
                finally:
                    item.unlink(missing_ok=True)
                n += count
        finally:
            stop.set()
            while True:  # unblock the threads still putting pages
                try:
                    _, item, _ = pages.get_nowait()
                except queue.Empty:
                    break
                if isinstance(item, Path):
                    item.unlink(missing_ok=True)
    if len(ranges) == 1:
        return n
    # Appended in range order: the order of one walk on the object id.
    have = con.execute(f"SELECT count(*) FROM duckdb_tables() WHERE table_name = '{table}'"
                       ).fetchone()[0] > 0
    for k, c in enumerate(chunks):
        if not created[k]:
            continue
        if have:
            con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM {c}")
        else:
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM {c}")
            have = True
        con.execute(f"DROP TABLE {c}")
    return n


def narrow(con, table: str, keep: tuple[str, ...], folds: set[str]) -> None:
    """One source's table down to the kept columns (lower case, in keep's
    order), its fold columns, its geometry and other_attributes: its other
    fields as a JSON object, in its own field order."""
    cols = con.execute(f"DESCRIBE {table}").fetchall()
    by_lower = {c.lower(): c for c, *_ in cols}
    g = next(c for c, t, *_ in cols if t.startswith("GEOMETRY"))
    kept = [f'"{by_lower[k]}" AS "{k}"' for k in keep if k in by_lower]
    rest = [c for c, *_ in cols if c.lower() not in keep and c not in folds and c != g]
    pairs = ", ".join(f'"{c}" := "{c}"' for c in rest)
    other = f"to_json(struct_pack({pairs}))::VARCHAR" if rest else "NULL::VARCHAR"
    select = ", ".join([*kept, *(f'"{c}"' for c in sorted(folds)), f"{other} AS other_attributes", f'"{g}"'])
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT {select} FROM {table}")


def join(con, tables: list[str], key: str) -> None:
    """The tables joined on key into raw: the first one's columns and
    geometry, then each other's columns not seen yet. Exits when a table
    repeats a key (rows would multiply)."""
    first = tables[0]
    seen = {c.lower() for (c,) in con.execute(f"SELECT column_name FROM (DESCRIBE {first})").fetchall()}
    select, joins = [f"{first}.*"], []
    for t in tables[1:]:
        cols = con.execute(f"DESCRIBE {t}").fetchall()
        by_lower = {c.lower(): c for c, *_ in cols}
        if key.lower() not in by_lower:
            sys.exit(f"{t}: no join column {key}")
        dup = con.execute(f'SELECT count(*) - count(DISTINCT "{by_lower[key.lower()]}") FROM {t}').fetchone()[0]
        if dup:
            sys.exit(f"{t}: {dup:,} repeated values of the join column {key}")
        new = [c for c, ty, *_ in cols if c.lower() not in seen and not ty.startswith("GEOMETRY")]
        seen |= {c.lower() for c in new}
        select += [f'{t}."{c}"' for c in new]
        k0 = next(c for (c,) in con.execute(f"SELECT column_name FROM (DESCRIBE {first})").fetchall()
                  if c.lower() == key.lower())
        joins.append(f'LEFT JOIN {t} ON {first}."{k0}" = {t}."{by_lower[key.lower()]}"')
    con.execute(f"CREATE OR REPLACE TABLE raw AS SELECT {', '.join(select)} FROM {first} "
                + " ".join(joins) + f" ORDER BY {first}.rowid")


def load(p: Profile, con, sources: list[dict], work: Path,
         keep: tuple[str, ...] | None = None, join_on: str | None = None) -> tuple[str, int]:
    """The layer's sources into the temp table src, geometry 2D in the
    profile's CRS (or the sources' own), with its bbox. keep: the columns
    kept as columns, the others of each source in other_attributes (JSON).
    join_on: the key the sources are joined on instead (no fold columns).
    Returns the CRS and the rows dropped for having no geometry."""
    if join_on:
        for s in sources:
            s["fold"] = {}
    if p.crs:
        crs = p.crs
    else:
        # A folded layer's sources can differ in CRS (one of Maine DEP's
        # seagrass surveys is in NAD83(2011) / UTM 19N, EPSG:6348, the others
        # in NAD83 / UTM 19N): the server projects the others to the most
        # common one.
        counts: dict[str, int] = {}
        for s in sources:
            counts[s["crs"]] = counts.get(s["crs"], 0) + 1
        crs = max(counts, key=lambda c: (counts[c], c))
    for s in sources:
        if s["crs"] != crs:
            s["projected_from"] = s["crs"]
    # A fold column whose name a source already uses (Maine DEP's 2013 and
    # 2018 seagrass surveys have a text `Year`, its redemption centers a
    # `CATEGORY`) becomes source_<name>: the publisher's field stays as is.
    names = {c: (f"source_{c}" if any(c in s.get("field_names", []) for s in sources) else c)
             for c in sources[0]["fold"]}
    for s in sources:
        s["fold_names"] = names
    # One table per source, unioned by name: yearly layers do not all have
    # the same columns.
    tables = [f"raw_{i}" for i in range(len(sources))]
    for attempt in (1, 2):
        for t in tables:
            con.execute(f"DROP TABLE IF EXISTS {t}")
        short = [(s, got) for s, t in zip(sources, tables)
                 if (got := fetch_source(con, s, work, t, crs, p.servers[s["server"]].get(
                     "workers", p.page_workers))) != s["count"]]
        if not short:
            break
        if attempt == 2:
            s, got = short[0]
            sys.exit(f"{s['url']}: fetched {got:,} rows, the service counts {s['count']:,}")
        # A layer edited while it was read: count again, and read it again.
        for s in sources:
            s["count"] = get(s["url"] + "/query", {"where": "1=1", "returnCountOnly": "true"})["count"]
    # A source with no rows (a verified empty year) has no table.
    present = [t for (t,) in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE table_name LIKE 'raw\\_%' ESCAPE '\\'"
    ).fetchall() if t in tables]
    if not present:
        sys.exit("every source is empty")
    if keep:
        for t in present:
            narrow(con, t, keep, set(names.values()))
    if join_on:
        if len(present) != len(tables):
            sys.exit("a joined layer's source is empty")
        join(con, present, join_on)
    else:
        con.execute("CREATE OR REPLACE TABLE raw AS "
                    + " UNION ALL BY NAME ".join(f"SELECT * FROM {t}" for t in present))
    for t in present:
        con.execute(f"DROP TABLE {t}")
    cols = con.execute("DESCRIBE raw").fetchall()
    g = next(c for c, t, *_ in cols if t.startswith("GEOMETRY"))
    total = con.execute("SELECT count(*) FROM raw").fetchone()[0]
    con.execute(f"""CREATE OR REPLACE TEMP TABLE src AS
        SELECT *, {BBOX} FROM (
            SELECT * EXCLUDE ("{g}"), ST_SetCRS(ST_Force2D("{g}"), '{crs}') AS geometry FROM raw
            WHERE "{g}" IS NOT NULL AND NOT ST_IsEmpty("{g}"))""")
    con.execute("DROP TABLE raw")
    close_rings(con)
    kept = con.execute("SELECT count(*) FROM src").fetchone()[0]
    return crs, total - kept


def build_layer(p: Profile, con, r: dict, info: dict, work: Path, out: Path,
                no_tiles: bool) -> dict:
    """One layer: fetch, write and verify the GeoParquet, tile it, draw its
    thumbnail. Returns its manifest entry, also kept beside the files as
    <id>.entry.json."""
    folder = out / r["theme"]
    folder.mkdir(parents=True, exist_ok=True)
    crs, dropped = load(p, con, info["sources"], work, p.fold_keep.get(r["id"]), p.join_on.get(r["id"]))
    have = {c for (c,) in con.execute("SELECT column_name FROM (DESCRIBE src)").fetchall()}
    left_out = [c for c in p.drop_columns.get(r["id"], ()) if c in have]
    for c in left_out:
        con.execute(f'ALTER TABLE src DROP COLUMN "{c}"')
    dest = folder / f"{r['id']}.parquet"
    entry = write_layer(con, dest, crs)
    tiles = None
    if not no_tiles and r["id"] not in p.untiled:
        tiles = tile_layer(con, dest, r["id"], crs, entry["geometry_types"], 0,
                           **({"max_zoom": p.tile_max_zoom[r["id"]]}
                              if r["id"] in p.tile_max_zoom else {}))
    outline = out / p.outline[0] / f"{p.outline[1]}.parquet"
    thumb = thumbnail(con, dest, crs, folder / f"{r['id']}.png", r["theme"], r["title"],
                      outline if outline.exists() else None, color=p.colors[r["theme"]],
                      outline_crs=p.outline_crs)
    first = info["sources"][0]
    folds = {c: [s["fold"][c] for s in info["sources"]] for c in first["fold"]}
    names = first.get("fold_names") or {c: c for c in folds}
    e = {"name": r["id"], "theme": r["theme"], "title": r["title"], "server": r["server"],
         "description": first["description"], "copyright": first["copyright"],
         # `years` when the column is `year` (yearly layers); any fold adds
         # `folds` (values per fold column) and `fold_columns` (each one's
         # name in the file).
         **({"years": folds["year"]} if names.get("year") == "year" else {}),
         **({"folds": folds, "fold_columns": names} if folds else {}),
         **entry, "dropped_without_geometry": dropped,
         **({"columns_left_out": left_out} if left_out else {}),
         "source": {k: v for k, v in info.items() if k != "sources"} | {
             "urls": [s["url"] for s in info["sources"]],
             "sources": [{k: s[k] for k in ("url", "name", "year", "fold", "data_edited",
                                            "count", "projected_from") if k in s}
                         for s in info["sources"]]},
         "pmtiles": tiles, "thumbnail": thumb,
         "built": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    con.execute("DROP TABLE IF EXISTS src")
    (folder / f"{r['id']}.entry.json").write_text(json.dumps(e, indent=2) + "\n")
    return e


# A layer of more features than this is big: one at a time, so two of them
# never sort and tile side by side in the runner's 16 GB.
BIG = 1_000_000


def run_layer(p: Profile, r: dict, info: dict, work: Path, out: Path,
              no_tiles: bool) -> tuple[bool, dict | str]:
    """build_layer in a worker process, on a connection and a temp folder of
    its own. Returns (True, entry) or (False, why it failed)."""
    tmp = work / f"layer-{r['id']}"
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; SET temp_directory = '{tmp}'; "
                "SET memory_limit = '6GB'")
    try:
        return True, build_layer(p, con, r, info, tmp, out, no_tiles)
    except (Exception, SystemExit) as e:  # noqa: BLE001 - one layer must not stop the others
        for f in (out / r["theme"]).glob(f"{r['id']}.*"):
            f.unlink(missing_ok=True)
        return False, f"{type(e).__name__}: {e}"[:500]
    finally:
        con.close()
        for f in sorted(tmp.rglob("*"), reverse=True):
            f.unlink() if f.is_file() else f.rmdir()
        tmp.rmdir()


class Publisher:
    """Uploads each layer as soon as it is built, in the order a reader can
    trust: its GeoParquet and PMTiles (those whose bytes changed, as
    scripts/filedates.py says) and thumbnail, then its theme's manifest and
    attribution, the index, _source.json, and the file stamp last. A run cut
    short keeps everything published before it; the next one starts from the
    published _source.json."""

    def __init__(self, p: Profile, out: Path, remote: str, public: str):
        from filedates import as_rfc3339, fetch_json, now_rfc3339
        self.p, self.out, self.remote = p, out, remote.rstrip("/")
        self.stamp = fetch_json(f"{public}/_files/{p.name}.json")
        self.first = self.stamp is None
        self.now = now_rfc3339()
        self.since = as_rfc3339(dt.date.today().isoformat())
        self.cache = os.environ.get("CACHE", "Cache-Control: public, max-age=3600")

    def copy(self, rel: str, *headers: str) -> None:
        cmd = ["rclone", "copyto", str(self.out / rel), f"{self.remote}/{rel}", "--ignore-times",
               "--no-traverse", "--retries", "5"]
        for h in headers:
            cmd += ["--header-upload", h]
        subprocess.run(cmd, check=True)

    def data(self, theme: str, layer_id: str) -> None:
        from filedates import stamp
        folder = self.out / theme
        files = [f for f in (folder / f"{layer_id}.parquet", folder / f"{layer_id}.pmtiles")
                 if f.is_file()]
        doc = stamp(self.out, self.p.name, self.stamp, self.since, self.now, only=files,
                    first=self.first)
        upload = doc.pop("upload")
        self.stamp = doc
        types = {".parquet": "application/vnd.apache.parquet", ".pmtiles": "application/vnd.pmtiles"}
        for rel in upload:
            self.copy(rel, self.cache, f"Content-Type: {types[Path(rel).suffix]}")
        if (folder / f"{layer_id}.png").is_file():
            self.copy(f"{theme}/{layer_id}.png", self.cache)
        print(f"    published {theme}/{layer_id}: {len(upload)} data file(s) changed", flush=True)

    def meta(self, rels: list[str]) -> None:
        """Manifests, index and _source.json, then the stamp, last."""
        for rel in rels:
            if (self.out / rel).is_file():
                self.copy(rel, "Cache-Control: public, max-age=300")
        if self.stamp is not None:
            stamp_file = self.out / "_files" / f"{self.p.name}.json"
            stamp_file.parent.mkdir(exist_ok=True)
            stamp_file.write_text(json.dumps(self.stamp, indent=1) + "\n")
            self.copy(f"_files/{self.p.name}.json", "Cache-Control: public, max-age=300")


def build(p: Profile, work: Path, out: Path, *, public: str, only: list[str] | None,
          force: bool, no_tiles: bool, max_minutes: float | None = None, workers: int = 1,
          publish: str | None = None) -> None:
    work.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)  # written to even when nothing is built
    rows = layers(p)
    if only and (unknown := set(only) - {r["id"] for r in rows}):
        sys.exit(f"not in {p.layers_csv.name}: {sorted(unknown)}")
    published = get_json(f"{public}/_source.json", {})
    t0 = time.monotonic()
    source = upstream(p, only)
    print(f"  upstream read in {time.monotonic() - t0:.0f} s", flush=True)
    publisher = Publisher(p, out, publish, public) if publish else None

    def complete(r: dict, e: dict) -> bool:
        """The entry's files are all here, the PMTiles too unless --no-tiles."""
        if not (out / r["theme"] / e["file"]).is_file():
            return False
        return no_tiles or r["id"] in p.untiled or bool(e.get("pmtiles")
                                and (out / r["theme"] / e["pmtiles"]["file"]).is_file())

    def done(r: dict) -> dict | None:
        """This out dir's entry for the layer, if built from the same
        upstream state and with what this run asks for: a rerun after a
        failure resumes."""
        f = out / r["theme"] / f"{r['id']}.entry.json"
        e = json.loads(f.read_text()) if f.is_file() else None
        if not e or e["source"]["fingerprint"] != source[r["id"]]["fingerprint"]:
            return None
        return e if complete(r, e) else None

    def discard(r: dict) -> None:
        """A local build this run will not publish: its files go, so the
        upload (every file under the out dir) cannot carry them."""
        for f in (out / r["theme"]).glob(f"{r['id']}.*"):
            f.unlink(missing_ok=True)

    failed: dict[str, str] = {}
    for r in rows:
        if source.get(r["id"], {}).get("error"):
            failed[r["id"]] = source[r["id"]]["error"]
            print(f"  FAIL {r['theme']}/{r['id']}: {failed[r['id']]}", flush=True)
    if only:
        todo = [r for r in rows if r["id"] in only and r["id"] not in failed]
    else:
        todo = [r for r in rows if r["id"] not in failed and (
            force or published.get(r["id"], {}).get("fingerprint")
            != source[r["id"]]["fingerprint"])]
    built: dict[str, dict] = {}
    for r in list(todo):
        if not only and not force and (e := done(r)):
            built[r["id"]] = e
            todo.remove(r)
    print(f"  {len(rows)} layers, {len(todo)} to build"
          + (f", {len(built)} already built here" if built else "")
          + (f", {workers} at a time" if workers > 1 else ""), flush=True)

    prev_cache: dict[str, dict] = {}

    def published_entries(theme: str) -> dict:
        if theme not in prev_cache:
            prev_cache[theme] = {f["name"]: f for f in
                                 get_json(f"{public}/{theme}/_manifest.json", {}).get("files", [])}
        return prev_cache[theme]

    def write_theme(theme: str, entries: list[dict]) -> None:
        folder = out / theme
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "ATTRIBUTION.txt").write_text(p.attribution(theme, entries))
        crs = {e["crs"] for e in entries}
        (folder / "_manifest.json").write_text(json.dumps({
            "state_name": p.theme_title(theme),
            **({"crs": crs.pop()} if len(crs) == 1 else {}),
            "total_features": sum(e["features"] for e in entries),
            "themes": {e["name"]: e["features"] for e in entries},
            "labels": {e["name"]: e["title"] for e in entries},
            "files": entries,
            "pmtiles": [{"name": e["name"], **e["pmtiles"]} for e in entries if e.get("pmtiles")],
        }, indent=2) + "\n")

    def write_index(themes: set[str]) -> None:
        (out / "index.json").write_text(json.dumps({"datasets": [
            {"path": theme, "code": theme, "name": p.theme_title(theme)}
            for theme in p.themes if theme in themes]}, indent=2) + "\n")
        (out / "snapshots.json").write_text(json.dumps({"latest": "", "snapshots": []}) + "\n")

    def write_source() -> None:
        # A layer not built keeps its published fingerprint, so the next run
        # tries it again.
        (out / "_source.json").write_text(json.dumps(
            {r["id"]: {k: built[r["id"]]["source"][k]
                       for k in ("fingerprint", "last_modified", "count")}
             if r["id"] in built else published[r["id"]]
             for r in rows if r["id"] in built or r["id"] in published}, indent=2) + "\n")

    def published_now(r: dict, e: dict) -> None:
        """One layer just built: its files, then everything that names it."""
        publisher.data(r["theme"], r["id"])
        entries = [built[x["id"]] if x["id"] in built else published_entries(x["theme"])[x["id"]]
                   for x in rows if x["theme"] == r["theme"]
                   and (x["id"] in built or x["id"] in published_entries(x["theme"]))]
        write_theme(r["theme"], entries)
        themes = {x["theme"] for x in rows
                  if x["id"] in built or x["id"] in published_entries(x["theme"])}
        write_index(themes)
        write_source()
        publisher.meta([f"{r['theme']}/_manifest.json", f"{r['theme']}/ATTRIBUTION.txt",
                        "index.json", "snapshots.json", "_source.json"])

    def report(r: dict, ok: bool, result: dict | str) -> None:
        if not ok:
            failed[r["id"]] = result
            print(f"  FAIL {r['theme']}/{r['id']}: {result}", flush=True)
            return
        e = built[r["id"]] = result
        tiles = e["pmtiles"]
        print(f"  {r['theme']}/{r['id']}: {e['features']:,} rows, {e['bytes'] / 1e6:.1f} MB, "
              f"{e['crs']}"
              + (f", tiles {tiles['bytes'] / 1e6:.1f} MB from z{tiles['min_zoom']}"
                 f"{'' if tiles['all_properties'] else ' (naming columns)'}" if tiles else "")
              + f", {time.monotonic() - t0:.0f} s", flush=True)
        if publisher:
            try:
                published_now(r, e)
            except (Exception, SystemExit) as err:  # noqa: BLE001 - the others still publish
                # Not published: the layer keeps its published state (its
                # fingerprint too, so the next run tries it again).
                del built[r["id"]]
                failed[r["id"]] = f"publish: {type(err).__name__}: {err}"[:500]
                print(f"  FAIL {r['theme']}/{r['id']}: {failed[r['id']]}", flush=True)

    def result(f) -> tuple[bool, dict | str]:
        try:
            return f.result()
        except BrokenProcessPool:
            raise
        except Exception as e:  # noqa: BLE001
            return False, f"{type(e).__name__}: {e}"[:500]

    def size(r: dict) -> int:
        return source[r["id"]].get("count") or 0

    # The outline first, alone: every thumbnail is drawn over it. Then the
    # largest first, so the long ones start early and the small ones fill the
    # other workers around them; never two big ones side by side.
    first = [r for r in todo if (r["theme"], r["id"]) == p.outline]
    waiting = sorted((r for r in todo if r not in first), key=size, reverse=True)
    deferred: list[str] = []
    running: dict = {}
    ctx = multiprocessing.get_context("spawn")
    try:
        with ProcessPoolExecutor(max(1, workers), mp_context=ctx) as pool:
            for r in first:
                f = pool.submit(run_layer, p, r, source[r["id"]], work, out, no_tiles)
                running[f] = r
                report(r, *result(f))
                del running[f]
            while waiting or running:
                if waiting and max_minutes and time.monotonic() - t0 > max_minutes * 60:
                    # The rest waits for the next run: not in _source.json, so
                    # it counts as moved then.
                    deferred += [r["id"] for r in waiting]
                    waiting = []
                big = any(size(r) > BIG for r in running.values())
                pick = next((r for r in waiting if not (big and size(r) > BIG)), None)
                if pick and len(running) < max(1, workers):
                    waiting.remove(pick)
                    running[pool.submit(run_layer, p, pick, source[pick["id"]], work, out,
                                        no_tiles)] = pick
                    continue
                if not running:
                    break
                done_, _ = wait(running, return_when=FIRST_COMPLETED)
                for f in done_:
                    report(running.pop(f), *result(f))
    except BrokenProcessPool:
        # A worker died (out of memory): the layers in flight fail, the rest
        # waits for the next run; what is published stays.
        for r in running.values():
            failed[r["id"]] = "BrokenProcessPool: a worker process died (out of memory?)"
            print(f"  FAIL {r['theme']}/{r['id']}: {failed[r['id']]}", flush=True)
        deferred += [r["id"] for r in waiting]

    # Manifests: the published entries, with the layers just built replaced.
    by_theme: dict[str, list[dict]] = {}
    absent = []
    for r in rows:
        if r["id"] in built:
            by_theme.setdefault(r["theme"], []).append(built[r["id"]])
            continue
        # Built here by an earlier --layer run (a local build in steps). On a
        # full build only if it is the current upstream state: an older
        # local entry must not replace a newer published one.
        local = out / r["theme"] / f"{r['id']}.entry.json"
        if local.is_file():
            e = json.loads(local.read_text())
            current = source.get(r["id"], {}).get("fingerprint")
            if (r["id"] not in failed and complete(r, e)
                    and (only or (current and e["source"]["fingerprint"] == current))):
                built[r["id"]] = e
                by_theme.setdefault(r["theme"], []).append(e)
                continue
        # Nothing local is published for this layer: whatever an earlier or
        # interrupted run left here (with or without an entry) goes.
        discard(r)
        prev = published_entries(r["theme"])
        if r["id"] in prev:
            by_theme.setdefault(r["theme"], []).append(prev[r["id"]])
        else:
            absent.append(r["id"])
    if absent:
        print(f"  {len(absent)} layers neither built nor published: left out of the manifests",
              flush=True)
    for theme, entries in by_theme.items():
        write_theme(theme, entries)
    write_index(set(by_theme))
    write_source()
    (out / "failed.json").write_text(json.dumps(failed, indent=2) + "\n")
    if publisher:
        publisher.meta([f"{t}/{f}" for t in by_theme for f in ("_manifest.json", "ATTRIBUTION.txt")]
                       + ["index.json", "snapshots.json", "_source.json"])
    if deferred:
        print(f"  time budget of {max_minutes:.0f} min reached: {len(deferred)} layers left for "
              f"the next run: {', '.join(deferred)}", flush=True)
    if failed:
        print(f"  {len(failed)} layers failed (failed.json): {', '.join(failed)}", flush=True)


def main(p: Profile, doc: str) -> None:
    ap = argparse.ArgumentParser(description=doc.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    u = sub.add_parser("upstream", help="print each layer's fingerprint")
    u.add_argument("--layer", action="append")
    b = sub.add_parser("build", help="fetch, convert, tile and verify the layers that moved")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help=f"the {p.name}/ prefix")
    b.add_argument("--public", default=p.public, help="where the published manifests are read")
    b.add_argument("--layer", action="append", help="build this layer (repeatable)")
    b.add_argument("--force", action="store_true", help="build every layer")
    b.add_argument("--no-tiles", action="store_true", help="GeoParquet only (a test run)")
    b.add_argument("--max-minutes", type=float, default=None,
                   help="start no layer after this many minutes; the rest waits for the next run")
    b.add_argument("--workers", type=int, default=1,
                   help="layers built at a time, each in a process of its own (one big at a time)")
    b.add_argument("--publish", metavar="REMOTE",
                   help="upload each layer as soon as it is built (rclone remote of the prefix)")
    args = ap.parse_args()
    if args.cmd == "upstream":
        src = upstream(p, args.layer)
        for k, v in src.items():
            if v.get("error"):
                print(f"unreachable, left out: {k}: {v['error']}", file=sys.stderr)
        print(json.dumps({k: {x: v[x] for x in ("fingerprint", "last_modified", "count")}
                          for k, v in src.items() if not v.get("error")}, indent=2))
        return
    build(p, args.work_dir, args.out_dir, public=args.public, only=args.layer, force=args.force,
          no_tiles=args.no_tiles, max_minutes=args.max_minutes, workers=args.workers,
          publish=args.publish)
