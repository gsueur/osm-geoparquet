#!/usr/bin/env python3
"""
NCES school locations (EDGE geocodes) as GeoParquet and PMTiles.

The National Center for Education Statistics' EDGE program publishes the
estimated location of every school in its three universe collections, one
zip a school year (https://nces.ed.gov/programs/edge/Geographic/SchoolLocations):
public schools (Common Core of Data, yearly), private schools (Private School
Survey, every other year) and postsecondary institutions (IPEDS, yearly). This
writes one GeoParquet file and one PMTiles archive per collection:

  <out>/latest/public.parquet          public elementary and secondary schools, key NCESSCH
  <out>/latest/private.parquet         private schools, key PPIN
  <out>/latest/postsecondary.parquet   colleges, universities, trade schools, key UNITID
  <out>/latest/<name>.pmtiles          the same points for web maps
  <out>/latest/_manifest.json          rows, sizes, sha256, school years, gate
  <out>/latest/ATTRIBUTION.txt

Each collection has its own school year (FILES); a newer one replaces the
file in latest/. Columns and values are NCES's, as its record layouts name
them, with the point as native GEOMETRY (lon/lat, OGC:CRS84: NCES ships
WGS 84), a `bbox` covering, rows in Hilbert order, in row groups of
GROUP_ROWS points. One change: the private school shapefile ships
CNTY as the 3-digit county code and the county name as NAMELSAD; the
record layout (and the other two files) have the 5-digit state and county
code as CNTY and the name as NMCNTY, which is what is written here.

The gate, before anything is written out: rows match the source layer's
feature count, keys are unique and present, every point has coordinates in
range and equal to NCES's LAT and LON columns, and every point's state FIPS
(STFIP) is the state the point falls in on TIGER/Line 2026 (counted, not
failed: NCES assigns STFIP from the point, so a mismatch is a boundary case
or a point outside every state). For public schools, LEAID is compared with
the TIGER/Line 2026 school district GEOIDs (unified, elementary, secondary,
administrative): the share that joins is counted in the manifest, since
charter, state-run and regional agencies have no district polygon.

Usage:
  python3 scripts/datasets/schools.py upstream
  python3 scripts/datasets/schools.py build --work-dir work --out-dir out/schools
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles
from datasets.gaul import verify

SOURCE_PAGE = "https://nces.ed.gov/programs/edge/Geographic/SchoolLocations"
SOURCE_DIR = "https://nces.ed.gov/programs/edge/data"
DOCS_DIR = "https://nces.ed.gov/programs/edge/docs"
PUBLIC = "https://parquetry.geomermaids.com"
TIGER = f"{PUBLIC}/census/tiger/2026"
RELEASE = "latest"

# name -> the EDGE file (kind and school year as NCES writes them in the
# zip name), key, title, documentation, PMTiles (zooms, properties). A new
# school year is a new file name: upstream() reports it and the build fails
# until the year here is updated (a code change, as the layout may move).
FILES = {
    "public": {
        "kind": "PUBLICSCH", "year": "2425", "key": "NCESSCH",
        "title": "Public schools", "survey": "Common Core of Data (CCD)",
        "doc": f"{DOCS_DIR}/EDGE_GEOCODE_PUBLIC_TECHDOC.pdf",
        "tiles": ((0, 13), ["NCESSCH", "LEAID", "NAME", "CITY", "STATE", "LOCALE"]),
    },
    "private": {
        "kind": "PRIVATESCH", "year": "2324", "key": "PPIN",
        "title": "Private schools", "survey": "Private School Survey (PSS)",
        "doc": f"{DOCS_DIR}/EDGE_GEOCODE_PSS_TECHDOC.pdf",
        "tiles": ((0, 13), ["PPIN", "NAME", "CITY", "STATE", "LOCALE"]),
    },
    "postsecondary": {
        "kind": "POSTSECSCH", "year": "2526", "key": "UNITID",
        "title": "Postsecondary institutions",
        "survey": "Integrated Postsecondary Education Data System (IPEDS)",
        "doc": f"{DOCS_DIR}/EDGE_GEOCODE_POSTSEC_TECHDOC.pdf",
        "tiles": ((0, 13), ["UNITID", "NAME", "CITY", "STATE", "LOCALE"]),
    },
}
# The kinds as the page has named them over the years: older postsecondary
# files are POSTSECONDARYSCH_ or POSTSEC_.
PAGE_KINDS = {"PUBLICSCH": "public", "PRIVATESCH": "private", "POSTSECSCH": "postsecondary",
              "POSTSECONDARYSCH": "postsecondary", "POSTSEC": "postsecondary"}
# TIGER/Line 2026 school district layers on parquetry, for the LEAID join.
DISTRICTS = ["unsd", "elsd", "scsd", "sdadm"]


def school_year(year: str) -> str:
    """'2425' -> '2024-25'."""
    return f"20{year[:2]}-{year[2:]}"


def zip_url(name: str) -> str:
    spec = FILES[name]
    return f"{SOURCE_DIR}/EDGE_GEOCODE_{spec['kind']}_{spec['year']}.zip"


# nces.ed.gov answers curl; every request goes through it, as nri.py does.
def curl(*args: str) -> str:
    return subprocess.run(["curl", "-fsSL", "--retry", "3", "--max-time", "120", *args],
                          check=True, capture_output=True, text=True).stdout


def head(url: str) -> dict:
    """Last-Modified, ETag and size, from the final response's headers."""
    blocks = curl("-I", url).replace("\r", "").strip().split("\n\n")
    headers = {}
    for line in blocks[-1].splitlines()[1:]:
        k, _, v = line.partition(":")
        headers[k.strip().lower()] = v.strip()
    return {"url": url, "last_modified": headers["last-modified"],
            "etag": headers.get("etag", "").strip('"'),
            "bytes": int(headers["content-length"])}


def years_on_page() -> dict[str, list[str]] | None:
    """The school years the page links to, per collection, newest last, or
    None when the page cannot be read. Links inside HTML comments are files
    NCES has not released yet (the page carries them commented out)."""
    try:
        html = curl(SOURCE_PAGE)
    except subprocess.CalledProcessError as exc:
        print(f"  {SOURCE_PAGE}: curl exit {exc.returncode}", file=sys.stderr)
        return None
    html = re.sub(r"<!--.*?-->", "", html, flags=re.S)
    found: dict[str, set[str]] = {name: set() for name in FILES}
    for kind, year in re.findall(r"EDGE_GEOCODE_([A-Z]+)_(\d{2}_?\d{2})\.zip", html):
        if kind in PAGE_KINDS:
            found[PAGE_KINDS[kind]].add(year.replace("_", ""))
    return {name: sorted(years) for name, years in found.items()}


def upstream() -> dict:
    """Each pinned zip's Last-Modified, ETag and size, and the years the page
    lists. A pinned zip that does not answer fails: the build reads only these."""
    return {"files": {name: {"year": school_year(spec["year"]), **head(zip_url(name))}
                      for name, spec in FILES.items()},
            "page_years": years_on_page()}


def newer_years(source: dict) -> dict[str, str]:
    """Collections whose page lists a school year newer than the pinned one."""
    page = source.get("page_years") or {}
    return {name: max(page[name]) for name, spec in FILES.items()
            if page.get(name) and max(page[name]) > spec["year"]}


def remove(path: Path) -> None:
    """Delete a file or a directory tree. A missing entry is fine: on exFAT
    the AppleDouble `._` files vanish with the file they shadow."""
    def gone_already(func, p, exc):
        if not isinstance(exc, FileNotFoundError):
            raise exc

    if path.is_dir():
        shutil.rmtree(path, onexc=gone_already)
    else:
        path.unlink(missing_ok=True)


def fetch(url: str, dest: Path, stale: Path) -> Path:
    """The zip, kept in dest with the server's ETag and Last-Modified beside
    it: a copy whose stamp still matches is reused. When the zip is fetched
    again, what was extracted from the old one (stale) is removed."""
    meta = head(url)
    stamp = dest.with_name(dest.name + ".source.json")
    want = {k: meta[k] for k in ("etag", "last_modified", "bytes")}
    if (dest.is_file() and dest.stat().st_size == meta["bytes"] and stamp.is_file()
            and json.loads(stamp.read_text()) == want):
        return dest
    remove(stale)
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(".part")
    subprocess.run(["curl", "-fsSL", "--retry", "5", "--retry-all-errors", "-o", str(part), url],
                   check=True)
    if part.stat().st_size != meta["bytes"] or not zipfile.is_zipfile(part):
        sys.exit(f"{url}: got {part.stat().st_size} bytes, the server lists {meta['bytes']}, "
                 f"or not a zip")
    part.rename(dest)
    stamp.write_text(json.dumps(want))
    return dest


def shapefile(archive: Path, dest: Path) -> Path:
    """The shapefile out of an EDGE zip. NCES ships it three ways: at the top
    of the zip (private), or in a zip inside it (public: Shapefile_SCH.zip,
    postsecondary: Shapefile.zip)."""
    def shp_in(z: zipfile.ZipFile) -> str | None:
        found = [m for m in z.namelist() if m.lower().endswith(".shp") and "/" not in m]
        if len(found) > 1:
            sys.exit(f"{archive.name}: several shapefiles: {found}")
        return found[0] if found else None

    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as z:
        shp = shp_in(z)
        if shp:
            stem = shp[:-4]
            z.extractall(dest, [m for m in z.namelist() if m.startswith(stem + ".")])
            return dest / shp
        inner = [m for m in z.namelist() if m.lower().endswith(".zip")]
        if len(inner) != 1:
            sys.exit(f"{archive.name}: no shapefile, and {len(inner)} inner zips")
        z.extract(inner[0], dest)
    with zipfile.ZipFile(dest / inner[0]) as z:
        shp = shp_in(z)
        if not shp:
            sys.exit(f"{archive.name}/{inner[0]}: no shapefile")
        stem = shp[:-4]
        z.extractall(dest, [m for m in z.namelist() if m.startswith(stem + ".")])
    return dest / shp


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


BBOX = """struct_pack(
            xmin := (ST_X(geometry) - abs(ST_X(geometry)) * 1e-6 - 1e-9)::FLOAT,
            ymin := (ST_Y(geometry) - abs(ST_Y(geometry)) * 1e-6 - 1e-9)::FLOAT,
            xmax := (ST_X(geometry) + abs(ST_X(geometry)) * 1e-6 + 1e-9)::FLOAT,
            ymax := (ST_Y(geometry) + abs(ST_Y(geometry)) * 1e-6 + 1e-9)::FLOAT
        ) AS bbox"""


def read_layer(con, name: str, shp: Path) -> int:
    """The shapefile as table src, NCES's columns in their order, the point
    as `geometry`; the private file's county columns as the record layout
    has them. Returns the layer's feature count, for the gate."""
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ST_Read('{shp}')").fetchall()
            if r[0] not in ("geom", "OGC_FID")]  # OGC_FID: GDAL's feature number
    if name == "private":
        if "NAMELSAD" not in cols or "NMCNTY" in cols:
            sys.exit(f"private: county columns are {[c for c in cols if 'CNTY' in c or 'LSAD' in c]}"
                     f", not CNTY and NAMELSAD as the 2023-24 shapefile ships them")
        select = [("CASE WHEN length(CNTY) = 3 THEN STFIP || CNTY ELSE CNTY END AS CNTY"
                   if c == "CNTY" else "NAMELSAD AS NMCNTY" if c == "NAMELSAD" else c)
                  for c in cols]
    else:
        select = cols
    con.execute(f"""CREATE OR REPLACE TABLE src AS
        SELECT *, {BBOX} FROM (
            -- No CRS on the column: GeoParquet's default, OGC:CRS84. The
            -- shapefile's EPSG:4326 would declare latitude first.
            SELECT {', '.join(select)}, geom::GEOMETRY AS geometry FROM ST_Read('{shp}'))""")
    return con.execute(f"SELECT count(*) FROM ST_Read('{shp}')").fetchone()[0]


# Rows per row group. gaul.py's 4 MiB cap is on geometry bytes, which for
# points (21 bytes each) would put a whole file in one group: a bbox query
# would read all of it. 10,000 Hilbert-ordered points (DuckDB rounds up to
# 10,240) make a group of under 2 MB that covers a small area, so a query
# reads one or two.
GROUP_ROWS = 10_000


def write_layer(con, dest: Path) -> dict:
    """src to GeoParquet 2.0: Hilbert order, bbox covering, row groups of
    GROUP_ROWS points, then verified."""
    n, xmin, ymin, xmax, ymax = con.execute("""
        SELECT count(*), min(ST_X(geometry)), min(ST_Y(geometry)), max(ST_X(geometry)),
               max(ST_Y(geometry)) FROM src""").fetchone()
    geo = json.dumps({
        "version": "2.0.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB", "geometry_types": ["Point"],
            "bbox": [xmin, ymin, xmax, ymax],
            "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}},
        }},
    }).replace("'", "''")
    con.execute(f"""
        COPY (SELECT * FROM src ORDER BY ST_Hilbert(geometry,
                  ST_Extent(ST_MakeEnvelope({xmin!r}, {ymin!r}, {xmax!r}, {ymax!r}))))
        TO '{dest}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', COMPRESSION ZSTD,
                     ROW_GROUP_SIZE {GROUP_ROWS}, KV_METADATA {{geo: '{geo}'}})""")
    verify(con, dest.parent, [dest.stem])
    groups, largest = con.execute(f"""
        SELECT count(*), max(b) FROM (SELECT row_group_id, sum(total_compressed_size) AS b
        FROM parquet_metadata('{dest}') GROUP BY 1)""").fetchone()
    return {"file": dest.name, "features": n, "bytes": dest.stat().st_size,
            "row_groups": groups, "largest_row_group_bytes": largest, "sha256": sha256(dest)}


def gate(con, name: str, dest: Path, layer_rows: int) -> dict:
    """Rows, keys, coordinates and states; for public schools, the LEAID join
    to TIGER/Line's school districts. The first three fail the build; the
    state and district comparisons are counted."""
    key = FILES[name]["key"]
    data = f"read_parquet('{dest}')"
    rows, nulls, distinct, no_point, off, mismatch = con.execute(f"""
        SELECT count(*), count(*) FILTER (WHERE {key} IS NULL OR {key} = ''),
               count(DISTINCT {key}),
               count(*) FILTER (WHERE geometry IS NULL OR ST_IsEmpty(geometry)),
               count(*) FILTER (WHERE NOT (ST_X(geometry) BETWEEN -180 AND 180
                                           AND ST_Y(geometry) BETWEEN -90 AND 90)),
               count(*) FILTER (WHERE abs(ST_X(geometry) - LON) > 1e-6
                                   OR abs(ST_Y(geometry) - LAT) > 1e-6)
        FROM {data}""").fetchone()
    problems = []
    if rows != layer_rows:
        problems.append(f"{rows:,} rows, the source layer has {layer_rows:,}")
    if nulls or distinct != rows:
        problems.append(f"{key}: {nulls} empty, {rows - distinct} repeated")
    if no_point or off:
        problems.append(f"{no_point} rows without a point, {off} out of range")
    if mismatch:
        problems.append(f"{mismatch} points differ from NCES's LAT and LON")
    if problems:
        sys.exit(f"{name}: gate failed: " + "; ".join(problems))
    out = {"rows": rows, "source_rows": layer_rows, "key": key}
    # The state each point falls in, against NCES's STFIP (assigned from the
    # same point): boundary cases and points outside every state.
    states = f"read_parquet('{TIGER}/substategeo/state.parquet')"
    in_state, outside = con.execute(f"""
        WITH hit AS (
            SELECT d.{key}, d.STFIP, s.GEOID
            FROM {data} d LEFT JOIN {states} s
              ON s.bbox.xmin <= ST_X(d.geometry) AND s.bbox.xmax >= ST_X(d.geometry)
             AND s.bbox.ymin <= ST_Y(d.geometry) AND s.bbox.ymax >= ST_Y(d.geometry)
             AND ST_Contains(s.geometry, d.geometry))
        SELECT count(*) FILTER (WHERE GEOID = STFIP), count(*) FILTER (WHERE GEOID IS NULL)
        FROM (SELECT {key}, any_value(STFIP) AS STFIP,
                     coalesce(max(GEOID) FILTER (WHERE GEOID = STFIP), max(GEOID)) AS GEOID
              FROM hit GROUP BY 1)""").fetchone()
    out["states"] = {"file": f"{TIGER}/substategeo/state.parquet",
                     "stfip_matches_tiger_state": in_state,
                     "in_another_state": rows - in_state - outside,
                     "outside_every_state": outside}
    if name == "public":
        union = " UNION ALL ".join(
            f"SELECT GEOID, '{d}' AS layer FROM read_parquet('{TIGER}/school/{d}.parquet')"
            for d in DISTRICTS)
        per = dict(con.execute(f"""
            SELECT layer, count(*) FROM {data} JOIN ({union}) t ON t.GEOID = LEAID
            GROUP BY 1""").fetchall())
        joined, leas, leas_joined = con.execute(f"""
            SELECT count(*) FILTER (WHERE LEAID IN (SELECT GEOID FROM ({union}))),
                   count(DISTINCT LEAID),
                   count(DISTINCT LEAID) FILTER (WHERE LEAID IN (SELECT GEOID FROM ({union})))
            FROM {data}""").fetchone()
        out["districts"] = {
            "files": [f"{TIGER}/school/{d}.parquet" for d in DISTRICTS],
            "schools_joined": joined, "schools_share": round(joined / rows, 4),
            "per_layer": {d: per.get(d, 0) for d in DISTRICTS},
            "leas": leas, "leas_joined": leas_joined}
    return out


def tiles(parquet: Path, name: str) -> dict:
    """Points thinned at the coarse zooms, every school at the maximum."""
    zooms, props = FILES[name]["tiles"]
    return pmtiles.tile(parquet, parquet.with_suffix(".pmtiles"), layer=name,
                        min_zoom=zooms[0], max_zoom=zooms[1], properties=props,
                        coverage=False)


def attribution(source: dict) -> str:
    rows = "\n".join(f"  {name + '.parquet':22s} {v['url']}  ({v['year']}, Last-Modified "
                     f"{v['last_modified']})" for name, v in source["files"].items())
    return f"""\
# Data attribution

This data is the National Center for Education Statistics' (NCES) EDGE
school locations: the estimated location of public schools (Common Core of
Data), private schools (Private School Survey) and postsecondary
institutions (IPEDS), from NCES's Education Demographic and Geographic
Estimates program, U.S. Department of Education.

  Page:       {SOURCE_PAGE}
  Methods:    {DOCS_DIR}/EDGE_GEOCODE_PUBLIC_TECHDOC.pdf
              {DOCS_DIR}/EDGE_GEOCODE_PSS_TECHDOC.pdf
              {DOCS_DIR}/EDGE_GEOCODE_POSTSEC_TECHDOC.pdf
  Status:     a work of the United States Government, in the public domain
              in the United States (17 U.S.C. 105); NCES: "Unless specifically
              noted, all information contained herein is in the public domain."

Credit: U.S. Department of Education, National Center for Education
Statistics, EDGE school locations.

## What the locations are

NCES geocodes the physical address each school reports to its survey: a
location is an estimate, usually on the building or its parcel, sometimes on
the street. Schools without a physical location (virtual schools, programs)
carry the address of their administration. County, CBSA, congressional and
legislative districts and NCES's locale code are assigned from the point.

## What was changed here

One GeoParquet 2.0 file per collection. Columns and values are NCES's, with
the point as native GEOMETRY (lon/lat, OGC:CRS84), a `bbox` column declared
as the GeoParquet covering, rows sorted along a Hilbert curve. The private
school shapefile's CNTY (3-digit county code) is written as the 5-digit
state and county code, and its NAMELSAD as NMCNTY, as the record layout and
the other two files have them. The PMTiles archives are derived for web
maps.

Source files, as read for this build:
{rows}

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def build(work: Path, out: Path, threads: int | None = None) -> None:
    work.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs; "
                f"SET temp_directory = '{work / 'tmp'}'")
    if threads:
        con.execute(f"SET threads = {threads}")
    source = upstream()
    ahead = newer_years(source)
    if ahead:
        sys.exit(f"NCES lists newer school years than schools.py builds: {ahead}; "
                 f"set the year in FILES")
    folder = out / RELEASE
    folder.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    files, tile_entries, themes, labels = [], [], {}, {}
    for name, spec in FILES.items():
        archive = fetch(zip_url(name), work / f"{name}.zip", stale=work / name)
        shp = shapefile(archive, work / name)
        layer_rows = read_layer(con, name, shp)
        dest = folder / f"{name}.parquet"
        entry = write_layer(con, dest)
        g = gate(con, name, dest, layer_rows)
        year = school_year(spec["year"])
        files.append({"name": name, "key": spec["key"], "school_year": year,
                      "survey": spec["survey"], "source": source["files"][name]["url"],
                      **entry, "gate": g})
        themes[name], labels[name] = entry["features"], spec["title"]
        tile_entries.append({"name": name, **tiles(dest, name)})
        print(f"  {name} {year}: {entry['features']:,} rows, {entry['bytes'] / 1e6:.1f} MB, "
              f"{entry['row_groups']} row groups, states {g['states']}, "
              + (f"districts {g['districts']['schools_share']:.1%}, " if "districts" in g else "")
              + f"{time.monotonic() - t0:.0f} s", flush=True)
    (folder / "ATTRIBUTION.txt").write_text(attribution(source))
    (folder / "_manifest.json").write_text(json.dumps({
        "state_name": "NCES EDGE school locations",
        "built": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_features": sum(themes.values()),
        "themes": themes,
        "labels": labels,
        "crs": "OGC:CRS84",
        "source": source,
        "files": files,
        "pmtiles": tile_entries,
    }, indent=2) + "\n")
    # The parquetry repository files at the dataset root.
    (out / "index.json").write_text(json.dumps({"datasets": [
        {"path": RELEASE, "code": RELEASE, "name": "NCES EDGE school locations"}]},
        indent=2) + "\n")
    (out / "snapshots.json").write_text(json.dumps({"latest": "", "snapshots": []}) + "\n")
    (out / "_source.json").write_text(json.dumps(source, indent=2) + "\n")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="print each pinned zip's Last-Modified and the page's years")
    b = sub.add_parser("build", help="fetch, convert, gate, tile and verify")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help="the schools/ prefix")
    b.add_argument("--threads", type=int)
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
        return
    build(args.work_dir, args.out_dir, args.threads)


if __name__ == "__main__":
    main()
