#!/usr/bin/env python3
"""
The Maine GeoLibrary's statewide vector layers as GeoParquet and PMTiles.

The Maine Office of GIS (MEGIS) publishes the state agencies' layers through
the Maine GeoLibrary, as ArcGIS Online feature services
(https://mainegeolibrary-maine.hub.arcgis.com, organisation RbMX0mRVOFNTdLzd).
This reads each layer from its FeatureServer query endpoint and writes one
GeoParquet file per layer, and its PMTiles archive, grouped by theme:

  <out>/<theme>/<layer>.parquet     e.g. places/e911_roads.parquet
  <out>/<theme>/<layer>.pmtiles
  <out>/<theme>/<layer>.png         the catalog thumbnail
  <out>/<theme>/_manifest.json      rows, sizes, sha256, titles, sources, tile settings
  <out>/<theme>/ATTRIBUTION.txt

maine_layers.csv says which layers: one row per source layer, its theme, the
feature service and layer number. Rows sharing an id are one published
layer: the Department of Marine Resources publishes its shellfish P90 scores
and NSSP classifications as one layer a year, folded here into one file
each, with a `year` column from the CSV. The GeoLibrary also lists label
layers, catalogue footprints, imagery indexes, tables and copies, none of
which are published here; the 2-ft contours (about 1,000 tiles on
gis.maine.gov) and DEP's own services are not part of it yet.

Fetching: pages of the layer's maxRecordCount (at most 2,000), walked on the
object id (`OBJECTID > last`, ordered on it), in the layer's own spatial
reference (outSR), as Esri JSON read by GDAL, which keeps the field types
(dates as timestamps). The rows fetched must equal the layer's
returnCountOnly count, or the layer fails. Features without a geometry are
dropped and counted in the manifest.

Columns and values are the GeoLibrary's, unchanged. The geometry is 2D, in
the layer's own CRS (UTM zone 19N, EPSG:26919, for most; a few are in Web
Mercator, NAD83 or WGS 84 lon/lat, each declared in its file), with a `bbox`
column declared as the GeoParquet covering; rows are sorted on a Hilbert
curve and cut into row groups of at most 4 MiB of geometry. Writing, tiling
and thumbnails are massgis.py's.

A build reads the published _source.json and rebuilds only the layers whose
fingerprint moved (each source's last data and schema edit and its count),
or the ones named with --layer; a layer whose service gives no edit date is
rebuilt every run, and scripts/filedates.py keeps its date and its files on
the bucket as they were when its bytes did not change.

Usage:
  python3 scripts/datasets/maine.py upstream
  uv run --project scripts --with matplotlib python scripts/datasets/maine.py build \\
      --work-dir /Volumes/T9/maine/tmp --out-dir /Volumes/T9/maine/out [--layer e911_roads]
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import html
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.massgis import (BBOX, close_rings, get_json, thumbnail, tile_layer,  # noqa: E402
                              write_layer)

ORG = "https://services1.arcgis.com/RbMX0mRVOFNTdLzd/arcgis/rest/services"
HUB = "https://mainegeolibrary-maine.hub.arcgis.com"
PUBLIC = "https://parquetry.geomermaids.com/maine"
LAYERS_CSV = Path(__file__).with_name("maine_layers.csv")
# The state outline the thumbnails are drawn over.
OUTLINE = ("boundaries", "state_boundary_polygon")
# The terms the GeoLibrary's items state, and nothing more.
TERMS = "Access: Public. Use: User accepts risk."
PAGE = 2000
# Esri's own codes for Web Mercator.
WKID_ALIASES = {102100: 3857, 102113: 3857, 900913: 3857}

THEMES = {
    "boundaries": "Boundaries, Districts and Parcels",
    "places": "Addresses, Roads and Facilities",
    "land": "Conservation, Lakes and Land Use",
    "habitat": "Wildlife and Habitat",
    "marine": "Marine Resources and Fisheries",
    "coast": "Coastal Geology and Hazards",
    "geology": "Geology, Minerals and Groundwater",
}
COLORS = {
    "boundaries": "#8e44ad", "places": "#d35400", "land": "#5aa02c", "habitat": "#3f8a4a",
    "marine": "#2f7fc1", "coast": "#1b8a8a", "geology": "#9a6b3a",
}


def layers() -> list[dict]:
    """The CSV's rows grouped into published layers, in CSV order, the
    state outline first (the thumbnails draw on it)."""
    with LAYERS_CSV.open() as f:
        rows = list(csv.DictReader(f))
    unknown = {r["theme"] for r in rows} - set(THEMES)
    if unknown:
        sys.exit(f"{LAYERS_CSV.name}: themes not in THEMES: {sorted(unknown)}")
    out: dict[str, dict] = {}
    for r in rows:
        e = out.setdefault(r["id"], {"id": r["id"], "theme": r["theme"], "title": r["title"],
                                     "note": r["note"], "sources": []})
        if e["theme"] != r["theme"]:
            sys.exit(f"{LAYERS_CSV.name}: {r['id']} is in two themes")
        e["sources"].append({"service": r["service"], "layer": int(r["layer"]),
                             "year": int(r["year"]) if r["year"] else None})
    for e in out.values():
        years = [s["year"] for s in e["sources"]]
        if len(e["sources"]) > 1 and (None in years or len(set(years)) != len(years)):
            sys.exit(f"{LAYERS_CSV.name}: {e['id']} folds several layers: each needs its own year")
    ordered = list(out.values())
    ordered.sort(key=lambda e: (e["theme"], e["id"]) != OUTLINE)
    return ordered


def url(src: dict) -> str:
    return f"{ORG}/{src['service']}/FeatureServer/{src['layer']}"


def get(u: str, params: dict | None = None, attempts: int = 6) -> dict:
    """An ArcGIS REST call as JSON, retried with backoff: ArcGIS answers an
    overloaded request with HTTP 200 and an `error` object as often as with a
    5xx."""
    full = u + "?" + urllib.parse.urlencode({**(params or {}), "f": "json"})
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(full, timeout=180) as r:
                doc = json.load(r)
            if "error" not in doc:
                return doc
            err = doc["error"]
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as e:
            err = e
        if attempt == attempts:
            raise RuntimeError(f"{u}: {err}")
        time.sleep(min(60, 5 * 2 ** (attempt - 1)))
    raise AssertionError


def ms_iso(ms: int | None) -> str | None:
    if not ms:
        return None
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def source_info(src: dict) -> dict:
    """What the service says of one source layer: its CRS, page size, object
    id field, last edits and count."""
    info = get(url(src))
    sr = info.get("sourceSpatialReference") or (info.get("extent") or {}).get("spatialReference") or {}
    wkid = sr.get("latestWkid") or sr.get("wkid")
    wkid = WKID_ALIASES.get(wkid, wkid)
    if not wkid:
        raise RuntimeError(f"{url(src)}: no spatial reference")
    edits = info.get("editingInfo") or {}
    count = get(url(src) + "/query", {"where": "1=1", "returnCountOnly": "true"})["count"]
    return {
        "url": url(src), "year": src["year"], "name": info.get("name"),
        "crs": f"EPSG:{wkid}", "oid": info["objectIdField"],
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


def upstream(only: list[str] | None = None) -> dict:
    """Each layer's sources and a fingerprint of their last edits and counts:
    a layer is rebuilt when it moves. A source without an edit date gets
    the build day in its fingerprint, so its layer is rebuilt every run."""
    rows = [r for r in layers() if not only or r["id"] in only]
    flat = [(r["id"], s) for r in rows for s in r["sources"]]
    with ThreadPoolExecutor(8) as ex:
        infos = list(ex.map(lambda x: source_info(x[1]), flat))
    out: dict[str, dict] = {}
    today = dt.date.today().isoformat()
    for (layer_id, _), info in zip(flat, infos):
        out.setdefault(layer_id, {"sources": []})["sources"].append(info)
    for e in out.values():
        key = "|".join(f"{s['url']}:{s['data_edited'] or today}:{s['schema_edited']}:{s['count']}"
                       for s in e["sources"])
        e["fingerprint"] = hashlib.sha256(key.encode()).hexdigest()[:16]
        edited = [s["data_edited"] for s in e["sources"] if s["data_edited"]]
        e["last_modified"] = max(edited) if edited else None
        e["count"] = sum(s["count"] for s in e["sources"])
    return out


def fetch_source(con, info: dict, work: Path, table: str) -> int:
    """One source layer into `table` (created on the first page), walked on
    its object id. Returns the rows fetched."""
    page_file = work / "page.json"
    last, n, created = -1, 0, con.execute(
        f"SELECT count(*) FROM duckdb_tables() WHERE table_name = '{table}'").fetchone()[0] > 0
    oid = info["oid"]
    while True:
        params = {"where": f"{oid} > {last}", "outFields": "*", "orderByFields": f"{oid} ASC",
                  "resultRecordCount": info["page"], "outSR": info["crs"].split(":")[1],
                  "returnZ": "false", "returnM": "false"}
        doc = get(info["url"] + "/query", params)
        feats = doc.get("features") or []
        if not feats:
            break
        page_file.write_text(json.dumps(doc))
        sel = f"SELECT * FROM ST_Read('{page_file}')"
        if info["year"] is not None:
            sel = f"SELECT *, {info['year']}::SMALLINT AS year FROM ST_Read('{page_file}')"
        if created:
            con.execute(f"INSERT INTO {table} BY NAME {sel}")
        else:
            con.execute(f"CREATE TABLE {table} AS {sel}")
            created = True
        n += len(feats)
        last = max(f["attributes"][oid] for f in feats)
    page_file.unlink(missing_ok=True)
    return n


def load(con, sources: list[dict], work: Path) -> tuple[str, int]:
    """The layer's sources into the temp table src, geometry 2D in the
    sources' CRS, with its bbox. Returns the CRS and the rows dropped for
    having no geometry."""
    crs = {s["crs"] for s in sources}
    if len(crs) != 1:
        sys.exit(f"sources in several CRS: {sorted(crs)}")
    crs = crs.pop()
    # One table per source, unioned by name: DMR's yearly layers do not all
    # have the same columns.
    tables = [f"raw_{i}" for i in range(len(sources))]
    for attempt in (1, 2):
        for t in tables:
            con.execute(f"DROP TABLE IF EXISTS {t}")
        short = [(s, got) for s, t in zip(sources, tables)
                 if (got := fetch_source(con, s, work, t)) != s["count"]]
        if not short:
            break
        if attempt == 2:
            s, got = short[0]
            sys.exit(f"{s['url']}: fetched {got:,} rows, the service counts {s['count']:,}")
        # A layer edited while it was read: count again, and read it again.
        for s in sources:
            s["count"] = get(s["url"] + "/query", {"where": "1=1", "returnCountOnly": "true"})["count"]
    con.execute("CREATE OR REPLACE TABLE raw AS "
                + " UNION ALL BY NAME ".join(f"SELECT * FROM {t}" for t in tables))
    for t in tables:
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


def build_layer(con, r: dict, info: dict, work: Path, out: Path, no_tiles: bool) -> dict:
    """One layer: fetch, write and verify the GeoParquet, tile it, draw its
    thumbnail. Returns its manifest entry, also kept beside the files as
    <id>.entry.json."""
    folder = out / r["theme"]
    folder.mkdir(parents=True, exist_ok=True)
    crs, dropped = load(con, info["sources"], work)
    dest = folder / f"{r['id']}.parquet"
    entry = write_layer(con, dest, crs)
    tiles = None
    if not no_tiles:
        tiles = tile_layer(con, dest, r["id"], crs, entry["geometry_types"], 0)
    outline = out / OUTLINE[0] / f"{OUTLINE[1]}.parquet"
    thumb = thumbnail(con, dest, crs, folder / f"{r['id']}.png", r["theme"], r["title"],
                      outline if outline.exists() else None, color=COLORS[r["theme"]],
                      outline_crs="EPSG:26919")
    first = info["sources"][0]
    e = {"name": r["id"], "theme": r["theme"], "title": r["title"],
         "description": first["description"], "copyright": first["copyright"],
         **({"years": [s["year"] for s in info["sources"]]} if first["year"] is not None else {}),
         **entry, "dropped_without_geometry": dropped,
         "source": {k: v for k, v in info.items() if k != "sources"} | {
             "urls": [s["url"] for s in info["sources"]],
             "sources": [{k: s[k] for k in ("url", "name", "year", "data_edited", "count")}
                         for s in info["sources"]]},
         "pmtiles": tiles, "thumbnail": thumb,
         "built": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    con.execute("DROP TABLE IF EXISTS src")
    (folder / f"{r['id']}.entry.json").write_text(json.dumps(e, indent=2) + "\n")
    return e


def attribution(theme: str, entries: list[dict]) -> str:
    lines = "\n".join(f"  {e['name']:<44} {e['source']['urls'][0]}" for e in entries)
    return f"""\
# Data attribution

These layers are published by the State of Maine through the Maine
GeoLibrary, maintained by the Maine Office of GIS (MEGIS), for the state
agencies that produce them (Marine Resources, Inland Fisheries and Wildlife,
the Maine Geological Survey, Agriculture, Conservation and Forestry's
Natural Areas Program and Beginning with Habitat, Transportation, the E911
Bureau and others):

  GeoLibrary: {HUB}
  Services:   {ORG}
  Producer:   State of Maine

{THEMES[theme]}: each layer and the feature service layer it is read from
(each one's last edit and count are in _manifest.json):

{lines}

Terms: the GeoLibrary's items state "{TERMS}" There is no other licence
statement. Credit the State of Maine, the Maine GeoLibrary and the agency
each layer names when you use it. The data is provided as is, without
warranty.

## What was changed here

One GeoParquet 2.0 file per layer (the shellfish P90 scores and NSSP
classifications, published by DMR one layer a year, are one file each with
a `year` column). Columns and values are the GeoLibrary's, unchanged;
features without a geometry are left out (counted in _manifest.json). The
geometry is 2D, in the layer's own CRS as published, with a `bbox` column
declared as the GeoParquet covering, rows sorted along a Hilbert curve. The
PMTiles archives beside them are derived for web maps, in lon/lat.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def build(work: Path, out: Path, *, public: str, only: list[str] | None, force: bool,
          no_tiles: bool) -> None:
    work.mkdir(parents=True, exist_ok=True)
    rows = layers()
    if only and (unknown := set(only) - {r["id"] for r in rows}):
        sys.exit(f"not in {LAYERS_CSV.name}: {sorted(unknown)}")
    published = get_json(f"{public}/_source.json", {})
    t0 = time.monotonic()
    source = upstream(only)
    print(f"  upstream read in {time.monotonic() - t0:.0f} s", flush=True)
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; SET temp_directory = '{work}'; "
                "SET memory_limit = '6GB'")

    def done(r: dict) -> dict | None:
        """This out dir's entry for the layer, if built from the same
        upstream state: a rerun after a failure resumes."""
        f = out / r["theme"] / f"{r['id']}.entry.json"
        e = json.loads(f.read_text()) if f.is_file() else None
        return e if e and e["source"]["fingerprint"] == source[r["id"]]["fingerprint"] else None

    if only:
        todo = [r for r in rows if r["id"] in only]
    else:
        todo = [r for r in rows if force or published.get(r["id"], {}).get("fingerprint")
                != source[r["id"]]["fingerprint"]]
    built: dict[str, dict] = {}
    for r in list(todo):
        if not only and (e := done(r)):
            built[r["id"]] = e
            todo.remove(r)
    print(f"  {len(rows)} layers, {len(todo)} to build"
          + (f", {len(built)} already built here" if built else ""), flush=True)
    failed: dict[str, str] = {}
    for r in todo:
        try:
            built[r["id"]] = build_layer(con, r, source[r["id"]], work, out, no_tiles)
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
        (folder / "ATTRIBUTION.txt").write_text(attribution(theme, entries))
        crs = {e["crs"] for e in entries}
        (folder / "_manifest.json").write_text(json.dumps({
            "state_name": f"Maine GeoLibrary: {THEMES[theme]}",
            **({"crs": crs.pop()} if len(crs) == 1 else {}),
            "total_features": sum(e["features"] for e in entries),
            "themes": {e["name"]: e["features"] for e in entries},
            "labels": {e["name"]: e["title"] for e in entries},
            "files": entries,
            "pmtiles": [{"name": e["name"], **e["pmtiles"]} for e in entries if e.get("pmtiles")],
        }, indent=2) + "\n")
    (out / "index.json").write_text(json.dumps({"datasets": [
        {"path": theme, "code": theme, "name": f"Maine GeoLibrary: {title}"}
        for theme, title in THEMES.items() if theme in by_theme]}, indent=2) + "\n")
    (out / "snapshots.json").write_text(json.dumps({"latest": "", "snapshots": []}) + "\n")
    # A layer not built keeps its published fingerprint, so the next run
    # tries it again.
    (out / "_source.json").write_text(json.dumps(
        {r["id"]: {k: v for k, v in source[r["id"]].items() if k != "sources"}
         if r["id"] in built else published[r["id"]]
         for r in rows if r["id"] in built or r["id"] in published}, indent=2) + "\n")
    (out / "failed.json").write_text(json.dumps(failed, indent=2) + "\n")
    if failed:
        print(f"  {len(failed)} layers failed (failed.json): {', '.join(failed)}", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    u = sub.add_parser("upstream", help="print each layer's fingerprint")
    u.add_argument("--layer", action="append")
    b = sub.add_parser("build", help="fetch, convert, tile and verify the layers that moved")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help="the maine/ prefix")
    b.add_argument("--public", default=PUBLIC, help="where the published manifests are read")
    b.add_argument("--layer", action="append", help="build this layer (repeatable)")
    b.add_argument("--force", action="store_true", help="build every layer")
    b.add_argument("--no-tiles", action="store_true", help="GeoParquet only (a test run)")
    args = p.parse_args()
    if args.cmd == "upstream":
        src = upstream(args.layer)
        print(json.dumps({k: {x: v[x] for x in ("fingerprint", "last_modified", "count")}
                          for k, v in src.items()}, indent=2))
        return
    build(args.work_dir, args.out_dir, public=args.public, only=args.layer, force=args.force,
          no_tiles=args.no_tiles)


if __name__ == "__main__":
    main()
