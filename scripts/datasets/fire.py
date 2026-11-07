#!/usr/bin/env python3
"""
US wildfire perimeters as GeoParquet and PMTiles: NIFC's live WFIGS layers
and the MTBS burned-area history.

Two groups, each published on its own schedule under fire/:

  wfigs/latest/current.parquet          active fire perimeters (WFIGS, NIFC)
  wfigs/latest/year_to_date.parquet     perimeters of the fires of this year
  wfigs/latest/all_since_2016.parquet   every WFIGS perimeter, discoveries since 2016-03
  wfigs/latest/incidents_ytd.parquet    incident points of this year (no PMTiles)
  wfigs/latest/<layer>.pmtiles          the three perimeter layers
  wfigs/latest/_manifest.json, ATTRIBUTION.txt
  mtbs/latest/mtbs_perimeters.parquet   MTBS burned-area boundaries, 1984 onward
  mtbs/latest/mtbs_perimeters.pmtiles
  mtbs/latest/_manifest.json, ATTRIBUTION.txt, mtbs_perims_DD_metadata.xml

WFIGS (Wildland Fire Interagency Geospatial Services) publishes hosted
ArcGIS layers that NIFC refreshes every few minutes. A build reads each
layer's editingInfo.lastEditDate and, when every layer's date equals the one
in the published manifest, writes nothing (run.json says so) and the
workflow stops there. Otherwise each layer is read in pages of 2,000 by
OBJECTID (a keyset: OBJECTIDs are stable and only grow), up to the highest
OBJECTID seen when the read starts, so edits made during the read do not
shift the pages. The gate: the rows read equal the server's count for the
same OBJECTID range, read right after, and every OBJECTID is read once.

MTBS (Monitoring Trends in Burn Severity, USGS and USDA Forest Service)
ships one national shapefile. The zip is regenerated daily with the same
content, so its Last-Modified and ETag say nothing: the build hashes the
shapefile inside and rebuilds only when that hash differs from the
published manifest's.

Columns: WFIGS's 119 are trimmed to the ones a question about a fire needs
(names, IRWIN and unique fire ids, acres, dates, containment, cause,
complexity, place of origin, agencies, personnel, cost), named as NIFC names
them; dates become TIMESTAMP WITH TIME ZONE (UTC). MTBS keeps all its
columns but GDAL's OGC_FID, with ig_date as a DATE and the burn-boundary
centre as DOUBLE. Geometry in lon/lat (OGC:CRS84): WFIGS serves WGS84,
MTBS ships NAD83 (EPSG:4269). Invalid polygons are repaired with
ST_MakeValid, keeping the polygonal part, and counted in the manifest.
Each row gets a `bbox` covering, rows are Hilbert-sorted and cut into row
groups of at most 4 MiB of geometry (gaul.py's rewrite). An empty layer
(no active fire) is written with its columns and no rows, and not tiled.

Usage:
  python3 scripts/datasets/fire.py upstream
  python3 scripts/datasets/fire.py build --group wfigs --work-dir work --out-dir out/fire [--force]
  python3 scripts/datasets/fire.py build --group mtbs --work-dir work --out-dir out/fire [--force]
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles
from datasets.census import BBOX
from datasets.gaul import rewrite, verify
from datasets.infra import GEOMETRY_TYPE_NAMES

PUBLIC = "https://parquetry.geomermaids.com/fire"
SERVICES = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services"
WFIGS_PAGE = "https://data-nifc.opendata.arcgis.com/"
MTBS_ZIP = ("https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/MTBS_Fire/data/"
            "composite_data/burned_area_extent_shapefile/mtbs_perimeter_data.zip")
MTBS_PAGE = "https://www.mtbs.gov/direct-download"
MTBS_STEM = "mtbs_perims_DD"
PAGE = 2000
USER_AGENT = "geomermaids-fire/1.0 (+https://geoparquet.geomermaids.com/)"

# The perimeter columns kept, as WFIGS names them; all three perimeter views
# have them (the current view lacks attr_IsValid and attr_IsQuarantined, so
# neither is kept).
PERIMETER_COLUMNS = [
    "poly_IncidentName", "poly_FeatureCategory", "poly_MapMethod", "poly_GISAcres",
    "poly_PolygonDateTime", "poly_DateCurrent", "poly_IRWINID", "poly_Source",
    "attr_IrwinID", "attr_UniqueFireIdentifier", "attr_IncidentName",
    "attr_IncidentTypeCategory", "attr_IncidentTypeKind", "attr_FireDiscoveryDateTime",
    "attr_ContainmentDateTime", "attr_ControlDateTime", "attr_FireOutDateTime",
    "attr_IncidentSize", "attr_DiscoveryAcres", "attr_FinalAcres", "attr_PercentContained",
    "attr_FireCause", "attr_FireCauseGeneral", "attr_FireCauseSpecific",
    "attr_FireMgmtComplexity", "attr_IncidentComplexityLevel", "attr_IsCpxChild",
    "attr_CpxName", "attr_POOState", "attr_POOCounty", "attr_POOFips", "attr_POOCity",
    "attr_POOJurisdictionalAgency", "attr_POOProtectingAgency", "attr_POOLandownerCategory",
    "attr_GACC", "attr_TotalIncidentPersonnel", "attr_EstimatedCostToDate",
    "attr_InitialLatitude", "attr_InitialLongitude", "attr_ModifiedOnDateTime_dt",
    "GlobalID",
]
INCIDENT_COLUMNS = [
    "IncidentName", "IrwinID", "UniqueFireIdentifier", "IncidentTypeCategory",
    "IncidentTypeKind", "FireDiscoveryDateTime", "ContainmentDateTime", "ControlDateTime",
    "FireOutDateTime", "IncidentSize", "DiscoveryAcres", "FinalAcres", "PercentContained",
    "FireCause", "FireCauseGeneral", "FireCauseSpecific", "FireMgmtComplexity",
    "IncidentComplexityLevel", "IsCpxChild", "CpxName", "POOState", "POOCounty", "POOFips",
    "POOCity", "POOJurisdictionalAgency", "POOProtectingAgency", "POOLandownerCategory",
    "GACC", "TotalIncidentPersonnel", "EstimatedCostToDate", "ModifiedOnDateTime_dt",
    "GlobalID",
]
# Esri field type -> DuckDB type; dates arrive as epoch milliseconds.
ESRI_TYPES = {
    "esriFieldTypeString": "VARCHAR", "esriFieldTypeGlobalID": "VARCHAR",
    "esriFieldTypeGUID": "VARCHAR", "esriFieldTypeDouble": "DOUBLE",
    "esriFieldTypeSingle": "DOUBLE", "esriFieldTypeInteger": "INTEGER",
    "esriFieldTypeSmallInteger": "SMALLINT", "esriFieldTypeOID": "BIGINT",
    "esriFieldTypeDate": "TIMESTAMPTZ",
}

# name -> service, kind, title, columns, PMTiles (zooms, properties) or None.
# Minimum zooms are the lowest at which no tile goes over the size cap.
PERIMETER_TILES = ["poly_IncidentName", "poly_GISAcres", "attr_IncidentTypeCategory",
                   "attr_PercentContained", "attr_UniqueFireIdentifier"]
WFIGS = {
    "current": {
        "service": "WFIGS_Interagency_Perimeters_Current", "kind": "perimeter",
        "title": "Active fire perimeters", "columns": PERIMETER_COLUMNS,
        "tiles": ((0, 14), PERIMETER_TILES),
    },
    "year_to_date": {
        "service": "WFIGS_Interagency_Perimeters_YearToDate", "kind": "perimeter",
        "title": "Fire perimeters, year to date", "columns": PERIMETER_COLUMNS,
        "tiles": ((0, 13), PERIMETER_TILES),
    },
    "all_since_2016": {
        "service": "WFIGS_Interagency_Perimeters", "kind": "perimeter",
        "title": "Fire perimeters since 2016", "columns": PERIMETER_COLUMNS,
        "tiles": ((0, 12), PERIMETER_TILES),
    },
    "incidents_ytd": {
        "service": "WFIGS_Incident_Locations_YearToDate", "kind": "incident",
        "title": "Fire incident locations, year to date", "columns": INCIDENT_COLUMNS,
        "tiles": None,
    },
}
MTBS_TILES = ((0, 12), ["event_id", "incid_name", "incid_type", "ig_date", "burnbndac"])

WFIGS_CREDIT = ("National Interagency Fire Center (NIFC), Wildland Fire Interagency "
                "Geospatial Services (WFIGS)")
WFIGS_PARTNERS = ("DOI Office of Wildland Fire (IRWIN), USDA Forest Service, National Park "
                  "Service, US Fish and Wildlife Service, Bureau of Indian Affairs, Bureau of "
                  "Land Management, National Association of State Foresters, US Fire "
                  "Administration, National Wildfire Coordinating Group")
MTBS_CREDIT = ("Monitoring Trends in Burn Severity (MTBS), U.S. Geological Survey and USDA "
               "Forest Service")


# ---------- HTTP ----------

def get_json(url: str, params: dict | None = None, *, attempts: int = 5) -> dict:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=180) as r:
                body = json.load(r)
            if "error" in body:
                raise RuntimeError(f"{url}: {body['error']}")
            return body
        except Exception as exc:  # noqa: BLE001  network and server errors alike
            if i == attempts - 1:
                raise
            wait = 5 * 2 ** i
            print(f"  {exc}; retry in {wait} s", file=sys.stderr, flush=True)
            time.sleep(wait)
    raise AssertionError("unreachable")


def layer_url(service: str) -> str:
    return f"{SERVICES}/{service}/FeatureServer/0"


def ms_to_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).isoformat().replace("+00:00", "Z")


def layer_info(service: str) -> dict:
    meta = get_json(layer_url(service), {"f": "json"})
    count = get_json(f"{layer_url(service)}/query",
                     {"where": "1=1", "returnCountOnly": "true", "f": "json"})["count"]
    edit = meta.get("editingInfo", {})
    return {"service": service, "url": layer_url(service),
            "last_edit": ms_to_iso(edit.get("lastEditDate")),
            "data_last_edit": ms_to_iso(edit.get("dataLastEditDate")),
            "count": count,
            "fields": {f["name"]: f["type"] for f in meta["fields"]}}


def mtbs_head() -> dict:
    out = subprocess.run(["curl", "-fsSIL", "--retry", "3", "-A", USER_AGENT, MTBS_ZIP],
                         check=True, capture_output=True, text=True).stdout
    headers = {}
    for line in out.replace("\r", "").strip().split("\n\n")[-1].splitlines()[1:]:
        k, _, v = line.partition(":")
        headers[k.strip().lower()] = v.strip()
    return {"url": MTBS_ZIP, "last_modified": headers.get("last-modified"),
            "etag": headers.get("etag", "").strip('"'),
            "bytes": int(headers["content-length"]) if headers.get("content-length") else None}


def upstream() -> dict:
    return {"wfigs": {name: {k: v for k, v in layer_info(s["service"]).items() if k != "fields"}
                      for name, s in WFIGS.items()},
            "mtbs": mtbs_head()}


def published(group: str) -> dict | None:
    """The live manifest of a group, or None when there is none yet."""
    try:
        return get_json(f"{PUBLIC}/{group}/latest/_manifest.json", attempts=2)
    except Exception as exc:  # noqa: BLE001
        print(f"  no published {group} manifest ({exc})", file=sys.stderr)
        return None


# ---------- shared writing ----------

def remove(path: Path) -> None:
    """Delete a directory tree. A missing entry is fine: on exFAT the
    AppleDouble `._` files vanish with the file they shadow."""
    def gone_already(func, p, exc):
        if not isinstance(exc, FileNotFoundError):
            raise exc

    if path.exists():
        shutil.rmtree(path, onexc=gone_already)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def connect(work: Path):
    con = duckdb.connect()
    (work / "tmp").mkdir(parents=True, exist_ok=True)
    con.execute("INSTALL spatial; LOAD spatial; SET memory_limit = '4GB'; SET threads = 4; "
                "SET preserve_insertion_order = false; "
                f"SET temp_directory = '{work / 'tmp'}'; SET TimeZone = 'UTC'")
    return con


def repair(con, table: str) -> dict:
    """Make every geometry valid in place, keeping the polygonal part of a
    repaired polygon. Returns the counts the manifest records."""
    null, invalid = con.execute(f"""SELECT count(*) FILTER (WHERE geometry IS NULL),
        count(*) FILTER (WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry))
        FROM {table}""").fetchone()
    if invalid:
        con.execute(f"""UPDATE {table}
            SET geometry = ST_CollectionExtract(ST_MakeValid(geometry), 3)
            WHERE geometry IS NOT NULL AND NOT ST_IsValid(geometry)""")
    empty = con.execute(f"SELECT count(*) FROM {table} WHERE geometry IS NOT NULL "
                        "AND ST_IsEmpty(geometry)").fetchone()[0]
    still = con.execute(f"SELECT count(*) FROM {table} WHERE geometry IS NOT NULL "
                        "AND NOT ST_IsValid(geometry)").fetchone()[0]
    if still:
        sys.exit(f"{table}: {still} geometries still invalid after ST_MakeValid")
    return {"null_geometries": null, "repaired_geometries": invalid,
            "empty_after_repair": empty}


def write_geoparquet(con, table: str, dest: Path) -> dict:
    """GeoParquet 2.0 with a bbox covering, Hilbert order, then row groups
    capped by bytes. An empty table is written with its columns and no
    bbox, and not run through the rewrite."""
    con.execute(f"CREATE OR REPLACE TABLE out_rows AS SELECT *, {BBOX} FROM {table}")
    n, xmin, ymin, xmax, ymax, types = con.execute("""
        SELECT count(*), min(ST_XMin(geometry)), min(ST_YMin(geometry)), max(ST_XMax(geometry)),
               max(ST_YMax(geometry)), list(DISTINCT ST_GeometryType(geometry)::VARCHAR)
               FILTER (WHERE geometry IS NOT NULL)
        FROM out_rows""").fetchone()
    column = {"encoding": "WKB",
              "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in (types or [])),
              "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}}}
    if n:
        column["bbox"] = [xmin, ymin, xmax, ymax]
    geo = json.dumps({"version": "2.0.0", "primary_column": "geometry",
                      "columns": {"geometry": column}}).replace("'", "''")
    order = (f"ORDER BY ST_Hilbert(geometry, ST_Extent(ST_MakeEnvelope({xmin!r}, {ymin!r}, "
             f"{xmax!r}, {ymax!r})))" if n else "")
    raw = dest.with_suffix(".raw.parquet")
    con.execute(f"""COPY (SELECT * FROM out_rows {order})
        TO '{raw}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', COMPRESSION ZSTD,
                    KV_METADATA {{geo: '{geo}'}})""")
    if n:
        rewrite(raw, dest)
        raw.unlink()
        verify(con, dest.parent, [dest.stem])
    else:
        raw.rename(dest)
    groups, largest = con.execute(f"""
        SELECT count(*), max(b) FROM (SELECT row_group_id, sum(total_compressed_size) AS b
        FROM parquet_metadata('{dest}') GROUP BY 1)""").fetchone()
    return {"file": dest.name, "features": n, "bytes": dest.stat().st_size,
            "row_groups": groups, "largest_row_group_bytes": largest,
            "bbox": [xmin, ymin, xmax, ymax] if n else None, "sha256": sha256(dest)}


def tile(dest: Path, name: str, spec: tuple[tuple[int, int], list[str]]) -> dict:
    (lo, hi), props = spec
    return {"name": name, **pmtiles.tile(dest, dest.with_suffix(".pmtiles"), layer=name,
                                         min_zoom=lo, max_zoom=hi, properties=props)}


# ---------- WFIGS ----------

def fetch_layer(service: str, columns: list[str], pages: Path) -> dict:
    """Every row up to the highest OBJECTID at the start, in OBJECTID order,
    as newline-delimited GeoJSON features, one file a page. Returns the
    read's bounds and counts for the gate."""
    url = f"{layer_url(service)}/query"
    stats = get_json(url, {"where": "1=1", "f": "json", "outStatistics": json.dumps([
        {"statisticType": "max", "onStatisticField": "OBJECTID", "outStatisticFieldName": "mx"}])})
    top = stats["features"][0]["attributes"]["mx"] if stats.get("features") else None
    remove(pages)
    pages.mkdir(parents=True)
    if top is None:
        return {"max_objectid": None, "rows": 0, "server_rows": 0, "pages": 0}
    last, k, rows = -1, 0, 0
    while True:
        body = get_json(url, {
            "where": f"OBJECTID > {last} AND OBJECTID <= {top}", "outFields": ",".join(
                ["OBJECTID", *columns]), "orderByFields": "OBJECTID ASC",
            "resultRecordCount": PAGE, "outSR": 4326, "f": "geojson"})
        feats = body.get("features", [])
        if not feats:
            break
        with (pages / f"page-{k:04d}.ndjson").open("w") as out:
            for f in feats:
                out.write(json.dumps({"properties": f["properties"],
                                      "geometry": f.get("geometry")}) + "\n")
        ids = [f["properties"]["OBJECTID"] for f in feats]
        if ids != sorted(ids) or ids[0] <= last:
            sys.exit(f"{service}: page {k} is not in OBJECTID order after {last}")
        last, k, rows = ids[-1], k + 1, rows + len(feats)
        print(f"    {service}: {rows:,} rows", flush=True)
        if len(feats) < PAGE and not body.get("properties", {}).get("exceededTransferLimit"):
            break
    server = get_json(url, {"where": f"OBJECTID <= {top}", "returnCountOnly": "true",
                            "f": "json"})["count"]
    return {"max_objectid": top, "rows": rows, "server_rows": server, "pages": k}


def load_layer(con, pages: Path, columns: list[str], fields: dict, table: str) -> None:
    missing = [c for c in columns if c not in fields]
    if missing:
        sys.exit(f"{table}: columns no longer served: {missing}")
    selects = ["(properties->>'OBJECTID')::BIGINT AS OBJECTID"]
    for c in columns:
        t = ESRI_TYPES[fields[c]]
        v = f"(properties->>'{c}')"
        selects.append(f"to_timestamp({v}::DOUBLE / 1000) AS {c}" if t == "TIMESTAMPTZ"
                       else f"{v}::{t} AS {c}")
    selects.append("ST_GeomFromGeoJSON(geometry) AS geometry")
    cols = ", ".join(["NULL::BIGINT AS OBJECTID"]
                     + [f"NULL::{ESRI_TYPES[fields[c]]} AS {c}" for c in columns]
                     + ["NULL::GEOMETRY AS geometry"])
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT {cols} WHERE false")
    # One page at a time: the 900 MB of JSON of all_since_2016 read at once
    # needs more than 4 GB. read_json sizes its buffers by
    # maximum_object_size, per thread: set it from the page's longest
    # feature (a few MB) rather than to a large constant.
    for f in sorted(pages.glob("page-*.ndjson")):
        longest = max(len(line) for line in f.open("rb"))
        size = max(16 << 20, 2 * longest)
        con.execute(f"""INSERT INTO {table}
            SELECT {', '.join(selects)}
            FROM read_json('{f}', format = 'newline_delimited',
                           columns = {{properties: 'JSON', geometry: 'JSON'}},
                           maximum_object_size = {size})""")


def wfigs_build(work: Path, out: Path, *, force: bool) -> dict:
    infos = {name: layer_info(s["service"]) for name, s in WFIGS.items()}
    before = published("wfigs")
    if before and not force:
        same = all(before.get("source", {}).get(n, {}).get("last_edit") == i["last_edit"]
                   for n, i in infos.items())
        if same:
            print("  wfigs: every layer's lastEditDate is the published one; nothing to do")
            return {"group": "wfigs", "changed": False}
    folder = out / "wfigs" / "latest"
    folder.mkdir(parents=True, exist_ok=True)
    con = connect(work)
    files, tiles, gates = [], [], {}
    for name, spec in WFIGS.items():
        t0 = time.monotonic()
        for attempt in range(3):
            read = fetch_layer(spec["service"], spec["columns"], work / "pages" / name)
            if read["rows"] == read["server_rows"]:
                break
            print(f"  {name}: read {read['rows']:,} rows, the server counts "
                  f"{read['server_rows']:,} for the same range; reading again", flush=True)
        else:
            sys.exit(f"{name}: three reads disagree with the server's count")
        load_layer(con, work / "pages" / name, spec["columns"], infos[name]["fields"], name)
        n, ids = con.execute(f"SELECT count(*), count(DISTINCT OBJECTID) FROM {name}").fetchone()
        if n != read["rows"] or ids != n:
            sys.exit(f"{name}: {n:,} rows loaded from {read['rows']:,} read, {n - ids} repeated "
                     "OBJECTIDs")
        if spec["kind"] == "incident":
            # WFIGS serves the points in NAD83; outSR=4326 has the server
            # return lon/lat WGS84, which is what GeoJSON carries.
            nulls = con.execute(f"SELECT count(*) FROM {name} WHERE geometry IS NULL").fetchone()[0]
            fixes = {"null_geometries": nulls, "repaired_geometries": 0, "empty_after_repair": 0}
        else:
            fixes = repair(con, name)
        dest = folder / f"{name}.parquet"
        entry = write_geoparquet(con, name, dest)
        gates[name] = {**read, **fixes}
        files.append({"name": name, "service": spec["service"], "kind": spec["kind"], **entry,
                      "gate": gates[name]})
        if spec["tiles"] and entry["features"]:
            tiles.append(tile(dest, name, spec["tiles"]))
        print(f"  {name}: {entry['features']:,} rows, {entry['bytes'] / 1e6:.1f} MB, "
              f"{fixes['repaired_geometries']} repaired, {time.monotonic() - t0:.0f} s", flush=True)
    source = {n: {k: v for k, v in i.items() if k != "fields"} for n, i in infos.items()}
    (folder / "ATTRIBUTION.txt").write_text(wfigs_attribution(source))
    (folder / "_manifest.json").write_text(json.dumps({
        "group": "wfigs",
        "state_name": "NIFC WFIGS wildfire perimeters and incidents",
        "built": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "total_features": sum(f["features"] for f in files),
        "crs": "OGC:CRS84",
        "source": source,
        "files": files,
        "pmtiles": tiles,
    }, indent=2) + "\n")
    return {"group": "wfigs", "changed": True,
            "rows": {f["name"]: f["features"] for f in files}}


def wfigs_attribution(source: dict) -> str:
    layers = "\n".join(f"  {n}: {s['url']} (lastEditDate {s['last_edit']}, {s['count']:,} rows)"
                       for n, s in source.items())
    return f"""\
# Data attribution

Wildland fire perimeters and incident locations from the {WFIGS_CREDIT}.
The data comes from the interagency fire reporting systems (IRWIN and the
agencies' perimeter mapping), with the participation of: {WFIGS_PARTNERS}.

  Source:   {WFIGS_PAGE} (NIFC Open Data), hosted ArcGIS layers
  Status:   works of US federal agencies; NIFC states no licence, only the
            disclaimer below

Credit "{WFIGS_CREDIT}" when you use it.

## NIFC's disclaimer

The data is provided as is, without warranty of any kind, and is not a legal
document. Perimeters are the best available at the time of mapping and can
lag the fire. Do not use it for navigation, evacuation or any decision where
an error could cause harm: follow the instructions of local authorities.

## What was changed here

One GeoParquet 2.0 file per layer, with the columns listed in the catalog
(a subset of WFIGS's, named as WFIGS names them), dates as UTC timestamps,
lon/lat (OGC:CRS84) geometry with a `bbox` covering, Hilbert-sorted. Invalid
polygons were repaired with ST_MakeValid (counts in _manifest.json). The
PMTiles archives are derived for web maps.

Layers, as read for this build:
{layers}

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


# ---------- MTBS ----------

def mtbs_fetch(work: Path) -> tuple[Path, dict]:
    head = mtbs_head()
    dest = work / "mtbs_perimeter_data.zip"
    part = dest.with_suffix(".part")
    subprocess.run(["curl", "-fsSL", "--retry", "5", "--retry-all-errors", "-A", USER_AGENT,
                    "-o", str(part), MTBS_ZIP], check=True)
    if head["bytes"] and part.stat().st_size != head["bytes"]:
        sys.exit(f"{MTBS_ZIP}: got {part.stat().st_size} bytes, the server lists {head['bytes']}")
    if not zipfile.is_zipfile(part):
        sys.exit(f"{MTBS_ZIP}: not a zip")
    part.rename(dest)
    folder = work / "mtbs"
    remove(folder)
    with zipfile.ZipFile(dest) as z:
        z.extractall(folder, [m for m in z.namelist() if not m.startswith(("__MACOSX", "._"))])
    h = hashlib.sha256()
    for ext in (".shp", ".dbf", ".prj"):
        h.update((folder / f"{MTBS_STEM}{ext}").read_bytes())
    head["content_sha256"] = h.hexdigest()
    return folder, head


def mtbs_build(work: Path, out: Path, *, force: bool) -> dict:
    work.mkdir(parents=True, exist_ok=True)
    src, head = mtbs_fetch(work)
    before = published("mtbs")
    if before and not force and before.get("source", {}).get("content_sha256") == head[
            "content_sha256"]:
        print("  mtbs: the shapefile is the published one; nothing to do")
        return {"group": "mtbs", "changed": False}
    prj = (src / f"{MTBS_STEM}.prj").read_text()
    if "North_American_1983" not in prj:
        sys.exit(f"{MTBS_STEM}.prj is not NAD83 lon/lat: {prj[:120]}")
    con = connect(work)
    shp = src / f"{MTBS_STEM}.shp"
    # MTBS's columns in its order; ST_Transform from NAD83 to OGC:CRS84
    # swaps nothing: both are lon/lat.
    casts = {"ig_date": "TRY_CAST(ig_date AS DATE) AS ig_date",
             "burnbndlat": "TRY_CAST(burnbndlat AS DOUBLE) AS burnbndlat",
             "burnbndlon": "TRY_CAST(burnbndlon AS DOUBLE) AS burnbndlon",
             "geom": "ST_Transform(geom, 'EPSG:4269', 'OGC:CRS84', always_xy := true) AS geometry"}
    names = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ST_Read('{shp}')").fetchall()
             if r[0] != "OGC_FID"]
    con.execute(f"""CREATE OR REPLACE TABLE mtbs AS
        SELECT {', '.join(casts.get(c, c) for c in names)} FROM ST_Read('{shp}')""")
    n, ids, bad_dates = con.execute("""SELECT count(*), count(DISTINCT event_id),
        count(*) FILTER (WHERE ig_date IS NULL) FROM mtbs""").fetchone()
    shp_rows = con.execute(f"SELECT count(*) FROM ST_Read('{src / f'{MTBS_STEM}.shp'}')"
                           ).fetchone()[0]
    if n != shp_rows or ids != n:
        sys.exit(f"mtbs: {n:,} rows of {shp_rows:,}, {n - ids} repeated event_id")
    if bad_dates:
        sys.exit(f"mtbs: {bad_dates} ig_date values are not dates")
    fixes = repair(con, "mtbs")
    first, last = con.execute("SELECT min(ig_date), max(ig_date) FROM mtbs").fetchone()
    folder = out / "mtbs" / "latest"
    folder.mkdir(parents=True, exist_ok=True)
    dest = folder / "mtbs_perimeters.parquet"
    entry = write_geoparquet(con, "mtbs", dest)
    tiles = [tile(dest, "mtbs_perimeters", MTBS_TILES)]
    shutil.copyfile(src / f"{MTBS_STEM}_metadata.xml", folder / f"{MTBS_STEM}_metadata.xml")
    head["ig_date_first"], head["ig_date_last"] = str(first), str(last)
    (folder / "ATTRIBUTION.txt").write_text(mtbs_attribution(head))
    (folder / "_manifest.json").write_text(json.dumps({
        "group": "mtbs",
        "state_name": "MTBS burned area boundaries",
        "built": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "total_features": entry["features"],
        "crs": "OGC:CRS84",
        "source": head,
        "files": [{"name": "mtbs_perimeters", "kind": "perimeter", **entry,
                   "gate": {"shapefile_rows": shp_rows, "unique_event_ids": ids, **fixes}}],
        "pmtiles": tiles,
        "metadata": {"file": f"{MTBS_STEM}_metadata.xml",
                     "bytes": (folder / f"{MTBS_STEM}_metadata.xml").stat().st_size,
                     "sha256": sha256(folder / f"{MTBS_STEM}_metadata.xml")},
    }, indent=2) + "\n")
    print(f"  mtbs: {entry['features']:,} rows ({first} to {last}), {entry['bytes'] / 1e6:.1f} MB, "
          f"{fixes['repaired_geometries']} repaired", flush=True)
    return {"group": "mtbs", "changed": True, "rows": {"mtbs_perimeters": entry["features"]}}


def mtbs_attribution(head: dict) -> str:
    return f"""\
# Data attribution

Burned area boundaries from {MTBS_CREDIT}, fires from 1984 onward
(ignitions {head['ig_date_first']} to {head['ig_date_last']} in this build).

  Source:   {MTBS_ZIP}
  Page:     {MTBS_PAGE}
  Terms:    "There are no restrictions on use, except for reasonable and
            proper acknowledgement of information sources." (MTBS's FGDC
            metadata, mtbs_perims_DD_metadata.xml, beside this file)

Credit "{MTBS_CREDIT}" when you use it.

MTBS maps fires of 1,000 acres or more in the western US and 500 acres or
more in the east from Landsat imagery, a year or two after the fire: recent
and small fires are not in it (see the WFIGS layers under fire/wfigs/).

## What was changed here

One GeoParquet 2.0 file with MTBS's columns, except GDAL's OGC_FID; ig_date
as a DATE, burnbndlat and burnbndlon as DOUBLE. Geometry from NAD83
(EPSG:4269) to lon/lat (OGC:CRS84), with a `bbox` covering, Hilbert-sorted;
invalid polygons repaired with ST_MakeValid (counts in _manifest.json). The
PMTiles archive is derived for web maps.

Source zip as read for this build: Last-Modified {head['last_modified']},
{head['bytes']:,} bytes, shapefile sha256 {head['content_sha256']}.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


# ---------- CLI ----------

def repository_files(out: Path) -> None:
    """The parquetry repository files at the dataset root."""
    (out / "index.json").write_text(json.dumps({"datasets": [
        {"path": "wfigs/latest", "code": "wfigs", "name": "NIFC WFIGS wildfire perimeters"},
        {"path": "mtbs/latest", "code": "mtbs", "name": "MTBS burned area boundaries"},
    ]}, indent=2) + "\n")
    (out / "snapshots.json").write_text(json.dumps({"latest": "", "snapshots": []}) + "\n")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="each WFIGS layer's lastEditDate and count, MTBS's zip headers")
    b = sub.add_parser("build", help="fetch, convert, gate, tile and verify one group")
    b.add_argument("--group", choices=["wfigs", "mtbs"], required=True)
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help="the fire/ prefix, e.g. out/fire")
    b.add_argument("--force", action="store_true", help="build even when nothing changed")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
        return
    work = args.work_dir / args.group
    work.mkdir(parents=True, exist_ok=True)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    run = (wfigs_build if args.group == "wfigs" else mtbs_build)(work, args.out_dir,
                                                                 force=args.force)
    if run["changed"]:
        repository_files(args.out_dir)
    (args.out_dir / "run.json").write_text(json.dumps(run, indent=2) + "\n")
    print(json.dumps(run))


if __name__ == "__main__":
    main()
