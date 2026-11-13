#!/usr/bin/env python3
"""
A state's ArcGIS REST layers as GeoParquet and PMTiles: the engine maine.py
and nh.py share. Each of them is a Profile (its servers, layer CSV, themes,
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
read by GDAL, which keeps the field types (dates as timestamps). The rows
fetched must equal the layer's returnCountOnly count, or the layer fails.
Features without a geometry are dropped and counted in the manifest.

Columns and values are the publisher's, unchanged. The geometry is 2D, with
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
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
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
    or 'changes')}."""
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
    wkid = WKID_ALIASES.get(wkid, wkid)
    if not wkid:
        raise RuntimeError(f"{u}: no spatial reference")
    edits = info.get("editingInfo") or {}
    # A map service layer names its object id only among its fields.
    oid = info.get("objectIdField") or next(
        (f["name"] for f in info.get("fields") or [] if f["type"] == "esriFieldTypeOID"), None)
    if not oid:
        raise RuntimeError(f"{u}: no object id field")
    count = get(u + "/query", {"where": "1=1", "returnCountOnly": "true"})["count"]
    max_oid = None
    if not edits:
        # No edit dates (Maine DEP's map services): the highest object id
        # and the fields stand in for them in the fingerprint.
        stats = get(u + "/query", {"where": "1=1", "outStatistics": json.dumps(
            [{"statisticType": "max", "onStatisticField": oid, "outStatisticFieldName": "m"}])})
        feats = stats.get("features") or [{}]
        max_oid = (feats[0].get("attributes") or {}).get("m")
    fields = hashlib.sha256(json.dumps(
        [(f["name"], f["type"]) for f in info.get("fields") or []]).encode()).hexdigest()[:12]
    return {
        "url": u, "server": src.get("server") or "", "fold": src["fold"],
        "year": src["year"], "name": info.get("name"),
        "crs": f"EPSG:{wkid}", "oid": oid, "max_oid": max_oid, "fields": fields,
        "field_names": sorted({f["name"].lower() for f in info.get("fields") or []}),
        "page": min(PAGE, info.get("maxRecordCount") or PAGE),
        "geometry_type": info.get("geometryType"),
        "data_edited": ms_iso(edits.get("dataLastEditDate") or edits.get("lastEditDate")),
        "schema_edited": ms_iso(edits.get("schemaLastEditDate")),
        "count": count,
        "description": clean(info.get("description")),
        "copyright": clean(info.get("copyrightText")),
    }


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


def fetch_source(con, info: dict, work: Path, table: str, target_crs: str | None = None) -> int:
    """One source layer into `table` (created on the first page), walked on
    its object id, in target_crs (default: its own). Returns the rows
    fetched."""
    target_crs = target_crs or info["crs"]
    last, n, created = -1, 0, con.execute(
        f"SELECT count(*) FROM duckdb_tables() WHERE table_name = '{table}'").fetchone()[0] > 0
    oid = info["oid"]
    page = info["page"]
    while True:
        params = {"where": f"{oid} > {last}", "outFields": "*", "orderByFields": f"{oid} ASC",
                  "resultRecordCount": page, "outSR": target_crs.split(":")[1],
                  "returnZ": "false", "returnM": "false"}
        while True:
            try:
                # Two tries before halving; the smallest page gets them all.
                doc = get(info["url"] + "/query", params, attempts=2 if page > MIN_PAGE else 6)
                break
            except RuntimeError as e:
                if page <= MIN_PAGE:
                    raise
                page = params["resultRecordCount"] = max(MIN_PAGE, page // 2)
                print(f"    {table}: {e}; pages of {page}", flush=True)
        feats = doc.get("features") or []
        if not feats:
            break
        if "NaN" in json.dumps([f.get("geometry") for f in feats]):
            for f in feats:
                clean_geometry(f)
        # A page file of its own, in the whole run: a page GDAL failed to
        # open stays marked as open in this thread, and any later read of
        # that path fails ("GDALOpen() called recursively"), every layer
        # after it.
        page_file = work / f"page-{table}-{next(PAGE_FILES)}.json"
        # The features last: GDAL tells Esri JSON from GeoJSON by the keys
        # at the start of the file, and NH GRANIT's servers put `features`
        # before `geometryType`.
        page_file.write_text(json.dumps(
            {key: v for key, v in doc.items() if key != "features"} | {"features": feats}))
        names = info.get("fold_names") or {}
        extra = "".join(
            f", {v}::{FOLD_COLUMNS[c]} AS {names.get(c, c)}" if FOLD_COLUMNS[c] != "VARCHAR"
            else f", '{str(v).replace(chr(39), chr(39) * 2)}'::VARCHAR AS {names.get(c, c)}"
            for c, v in (info.get("fold") or {}).items())
        sel = f"SELECT *{extra} FROM ST_Read('{page_file}')"
        try:
            if created:
                con.execute(f"INSERT INTO {table} BY NAME {sel}")
            else:
                con.execute(f"CREATE TABLE {table} AS {sel}")
                created = True
        finally:
            page_file.unlink(missing_ok=True)
        n += len(feats)
        last = max(f["attributes"][oid] for f in feats)
    return n


def load(p: Profile, con, sources: list[dict], work: Path) -> tuple[str, int]:
    """The layer's sources into the temp table src, geometry 2D in the
    profile's CRS (or the sources' own), with its bbox. Returns the CRS and
    the rows dropped for having no geometry."""
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
                 if (got := fetch_source(con, s, work, t, crs)) != s["count"]]
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
    crs, dropped = load(p, con, info["sources"], work)
    dest = folder / f"{r['id']}.parquet"
    entry = write_layer(con, dest, crs)
    tiles = None
    if not no_tiles:
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


def build(p: Profile, work: Path, out: Path, *, public: str, only: list[str] | None,
          force: bool, no_tiles: bool, max_minutes: float | None = None) -> None:
    work.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)  # written to even when nothing is built
    rows = layers(p)
    if only and (unknown := set(only) - {r["id"] for r in rows}):
        sys.exit(f"not in {p.layers_csv.name}: {sorted(unknown)}")
    published = get_json(f"{public}/_source.json", {})
    t0 = time.monotonic()
    source = upstream(p, only)
    print(f"  upstream read in {time.monotonic() - t0:.0f} s", flush=True)
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; SET temp_directory = '{work}'; "
                "SET memory_limit = '6GB'")

    def complete(r: dict, e: dict) -> bool:
        """The entry's files are all here, the PMTiles too unless --no-tiles."""
        if not (out / r["theme"] / e["file"]).is_file():
            return False
        return no_tiles or bool(e.get("pmtiles")
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
          + (f", {len(built)} already built here" if built else ""), flush=True)
    deferred: list[str] = []
    for r in todo:
        if max_minutes and time.monotonic() - t0 > max_minutes * 60:
            # The rest waits for the next run: not in _source.json, so it
            # counts as moved then (a first build is longer than a job may run).
            deferred.append(r["id"])
            continue
        try:
            built[r["id"]] = build_layer(p, con, r, source[r["id"]], work, out, no_tiles)
        except (Exception, SystemExit) as e:  # noqa: BLE001 - one layer must not stop the others
            failed[r["id"]] = f"{type(e).__name__}: {e}"[:500]
            print(f"  FAIL {r['theme']}/{r['id']}: {failed[r['id']]}", flush=True)
            for f in (out / r["theme"]).glob(f"{r['id']}.*"):
                f.unlink(missing_ok=True)
            for (t,) in con.execute("SELECT table_name FROM duckdb_tables() WHERE "
                                    "table_name = 'src' OR table_name LIKE 'raw%'").fetchall():
                con.execute(f"DROP TABLE IF EXISTS {t}")
            continue
        e = built[r["id"]]
        tiles = e["pmtiles"]
        print(f"  {r['theme']}/{r['id']}: {e['features']:,} rows, {e['bytes'] / 1e6:.1f} MB, "
              f"{e['crs']}"
              + (f", tiles {tiles['bytes'] / 1e6:.1f} MB from z{tiles['min_zoom']}"
                 f"{'' if tiles['all_properties'] else ' (naming columns)'}" if tiles else "")
              + f", {time.monotonic() - t0:.0f} s", flush=True)
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
        prev = {f["name"]: f for f in get_json(f"{public}/{r['theme']}/_manifest.json", {})
                .get("files", [])}
        if r["id"] in prev:
            by_theme.setdefault(r["theme"], []).append(prev[r["id"]])
        else:
            absent.append(r["id"])
    if absent:
        print(f"  {len(absent)} layers neither built nor published: left out of the manifests",
              flush=True)
    for theme, entries in by_theme.items():
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
    (out / "index.json").write_text(json.dumps({"datasets": [
        {"path": theme, "code": theme, "name": p.theme_title(theme)}
        for theme in p.themes if theme in by_theme]}, indent=2) + "\n")
    (out / "snapshots.json").write_text(json.dumps({"latest": "", "snapshots": []}) + "\n")
    # A layer not built keeps its published fingerprint, so the next run
    # tries it again.
    (out / "_source.json").write_text(json.dumps(
        {r["id"]: {k: built[r["id"]]["source"][k] for k in ("fingerprint", "last_modified", "count")}
         if r["id"] in built else published[r["id"]]
         for r in rows if r["id"] in built or r["id"] in published}, indent=2) + "\n")
    (out / "failed.json").write_text(json.dumps(failed, indent=2) + "\n")
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
          no_tiles=args.no_tiles, max_minutes=args.max_minutes)
