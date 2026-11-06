#!/usr/bin/env python3
"""
Gazetteers: names to coordinates, for the US (the Census Bureau's Gazetteer
files) and the world (GeoNames), as GeoParquet.

  <out>/census/<year>/<code>.parquet          one per Census Gazetteer layer
  <out>/census/<year>/_manifest.json, ATTRIBUTION.txt
  <out>/geonames/latest/geonames.parquet      every GeoNames feature, one world file
  <out>/geonames/latest/country=<CC>/geonames.parquet   the same rows, one file per country
  <out>/geonames/latest/cities1000.parquet    populated places of 1,000 people or more,
                                              and administrative seats
  <out>/geonames/latest/cities1000.pmtiles    those places for map labels
  <out>/geonames/latest/_manifest.json, ATTRIBUTION.txt
  <out>/_source.json                          what each part was built from
  <out>/index.json, snapshots.json            for the parquetry viewer

Census Gazetteer: 17 national delimited text files (pipes since 2026, tabs before), one row per area with its
land and water area and an internal point (INTPTLAT, INTPTLONG). Same
vintage as the TIGER/Line files on parquetry (census.py), so GEOID joins
them; the file names follow TIGER's layer codes (counties -> county, ua ->
uac20). Columns and values unchanged, GEOID kept as text (leading zeros).
The point is NAD83 (EPSG:4269), like TIGER. Yearly.

GeoNames: the `allCountries` dump, about 13.5 million names, rebuilt daily
by GeoNames. Its columns get the readme's names, and decoded companions
(feature class and code, country, admin1 and admin2 names) from GeoNames'
own lookup files. WGS84 lon/lat. Published as a rolling `latest/`, rebuilt
weekly: GeoNames keeps no history of its own and a name list changes
slowly, so dated snapshots would cost storage for no reader we know of;
the dump's date is in the manifest. Rows without a country (oceans,
undersea features) are in the world file and under country=none/.

Every file: Hilbert-sorted, bbox covering, row groups of at most 4 MiB of
geometry (gaul.py's rewrite and verify).

Usage:
  python3 scripts/datasets/gazetteer.py upstream
  python3 scripts/datasets/gazetteer.py build --work-dir work --out-dir out/gazetteer [--part census|geonames]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles
from datasets.census import BBOX, projjson, write_src
from datasets.gaul import verify, write_layer

PUBLIC = "https://parquetry.geomermaids.com"

# ---------- Census Gazetteer ----------

YEAR = "2026"
CENSUS_PAGE = ("https://www.census.gov/geographies/reference-files/time-series/geo/"
               "gazetteer-files.html")
CENSUS_DIR = f"https://www2.census.gov/geo/docs/maps-data/data/gazetteer/{YEAR}_Gazetteer"
CENSUS_CRS = "EPSG:4269"
TIGER = f"{PUBLIC}/census/tiger/{YEAR}"

# Gazetteer layer -> (file code, title, TIGER/Line file on parquetry and its
# GEOID column, or None where parquetry has no such layer).
CENSUS_LAYERS = {
    "state": ("state", "States and equivalent entities", ("substategeo/state", "GEOID")),
    "counties": ("county", "Counties and equivalent entities",
                 ("substategeo/county", "GEOID")),
    "cousubs": ("cousub", "County subdivisions", ("substategeo/cousub", "GEOID")),
    "place": ("place", "Places: incorporated places and census designated places",
              ("substategeo/place", "GEOID")),
    "tracts": ("tract", "Census tracts", ("substategeo/tract", "GEOID")),
    "zcta": ("zcta520", "ZIP Code Tabulation Areas (2020)", ("nationgeo/zcta520", "GEOID20")),
    "cbsa": ("cbsa", "Core based statistical areas: metropolitan and micropolitan",
             ("nationgeo/cbsa", "GEOID")),
    "ua": ("uac20", "Urban areas (2020)", ("nationgeo/uac20", "GEOID20")),
    "120CDs": ("cd120", "Congressional districts (120th Congress)",
               ("legislative/cd120", "GEOID")),
    "sldu": ("sldu", "State legislative districts, upper chamber",
             ("legislative/sldu", "GEOID")),
    "sldl": ("sldl", "State legislative districts, lower chamber",
             ("legislative/sldl", "GEOID")),
    "unsd": ("unsd", "Unified school districts", ("school/unsd", "GEOID")),
    "elsd": ("elsd", "Elementary school districts", ("school/elsd", "GEOID")),
    "scsd": ("scsd", "Secondary school districts", ("school/scsd", "GEOID")),
    "sdadm": ("sdadm", "School district administrative areas (Vermont)",
              ("school/sdadm", "GEOID")),
    "aiannh": ("aiannh", "American Indian, Alaska Native and Native Hawaiian areas", None),
    "aiannhrt": ("aiannhrt", "American Indian reservations and off-reservation trust lands",
                 None),
}
CENSUS_INTEGERS = {"ALAND", "AWATER"}
CENSUS_DOUBLES = {"ALAND_SQMI", "AWATER_SQMI", "INTPTLAT", "INTPTLONG"}


def census_url(layer: str) -> str:
    return f"{CENSUS_DIR}/{YEAR}_Gaz_{layer}_national.zip"


# ---------- GeoNames ----------

GEONAMES = "https://download.geonames.org/export/dump"
GEONAMES_PAGE = "https://www.geonames.org/"
GEONAMES_FILES = ["allCountries.zip", "featureCodes_en.txt", "admin1CodesASCII.txt",
                  "admin2Codes.txt", "countryInfo.txt", "readme.txt"]
# The readme's column names, in the dump's order, and their types.
GEONAMES_COLUMNS = [
    ("geonameid", "BIGINT"), ("name", "VARCHAR"), ("asciiname", "VARCHAR"),
    ("alternatenames", "VARCHAR"), ("latitude", "DOUBLE"), ("longitude", "DOUBLE"),
    ("feature_class", "VARCHAR"), ("feature_code", "VARCHAR"), ("country_code", "VARCHAR"),
    ("cc2", "VARCHAR"), ("admin1_code", "VARCHAR"), ("admin2_code", "VARCHAR"),
    ("admin3_code", "VARCHAR"), ("admin4_code", "VARCHAR"), ("population", "BIGINT"),
    ("elevation", "INTEGER"), ("dem", "INTEGER"), ("timezone", "VARCHAR"),
    ("modification_date", "DATE"),
]
# https://www.geonames.org/export/codes.html
FEATURE_CLASSES = {
    "A": "country, state, region", "H": "stream, lake", "L": "parks, area",
    "P": "city, village", "R": "road, railroad", "S": "spot, building, farm",
    "T": "mountain, hill, rock", "U": "undersea", "V": "forest, heath",
}
# Seats of government kept in cities1000 whatever their population, as in
# GeoNames' own cities1000 extract.
SEATS = ("PPLC", "PPLA", "PPLA2", "PPLA3", "PPLG")
NO_COUNTRY = "none"  # the partition of rows without a country code
CITIES_TILES = ((0, 12), ["geonameid", "name", "feature_code", "country_code", "population"])


def head(url: str) -> dict:
    req = urllib.request.Request(url, method="HEAD")
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return {"url": url, "last_modified": r.headers["Last-Modified"],
                        "bytes": int(r.headers["Content-Length"])}
        except OSError as e:
            if attempt == 3:
                sys.exit(f"{url}: {e}")
            time.sleep(10 * attempt)


def census_upstream() -> dict:
    return {layer: head(census_url(layer)) for layer in CENSUS_LAYERS}


def geonames_upstream() -> dict:
    return {f: head(f"{GEONAMES}/{f}") for f in GEONAMES_FILES}


def upstream() -> dict:
    """Last-Modified and size of every source file: a change rebuilds its part."""
    return {"census": census_upstream(), "geonames": geonames_upstream()}


def download(url: str, dest: Path) -> Path:
    """curl to dest; a .zip must be one (census.gov can answer a refused
    request with an HTML page and HTTP 200). A failure fails the build."""
    if dest.is_file() and (dest.suffix != ".zip" or zipfile.is_zipfile(dest)):
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, 4):
        subprocess.run(["curl", "-fsSL", "--retry", "3", "-o", str(tmp), url], check=True)
        if dest.suffix != ".zip" or zipfile.is_zipfile(tmp):
            tmp.rename(dest)
            return dest
        print(f"  {url}: not a zip (attempt {attempt})", flush=True)
        time.sleep(20 * attempt)
    sys.exit(f"{url}: not served as a zip after 3 attempts")


# ---------- build: Census ----------

def census_table(con, layer: str, zip_path: Path, work: Path) -> int:
    """The layer's text file as the temp table `src` (typed columns, a NAD83
    point and its bbox). Returns the source's row count."""
    with zipfile.ZipFile(zip_path) as z:
        name = next(n for n in z.namelist() if n.endswith(".txt") and "__MACOSX" not in n)
        text = z.read(name).decode("utf-8")
    lines = text.rstrip("\n").split("\n")
    # Pipe-delimited since 2026, tab-delimited before.
    delim = "\t" if "\t" in lines[0] else "|"
    header = [c.strip() for c in lines[0].split(delim)]
    rows = len(lines) - 1
    txt = work / name
    txt.write_text(text)
    cols = ", ".join(f"'{c}': 'VARCHAR'" for c in header)
    casts = ", ".join(
        f"TRIM(\"{c}\")::BIGINT AS \"{c}\"" if c in CENSUS_INTEGERS else
        f"TRIM(\"{c}\")::DOUBLE AS \"{c}\"" if c in CENSUS_DOUBLES else
        f"NULLIF(TRIM(\"{c}\"), '') AS \"{c}\"" for c in header)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE src AS
        SELECT *, {BBOX} FROM (
            SELECT {casts},
                   ST_SetCRS(ST_Point(TRIM(INTPTLONG)::DOUBLE, TRIM(INTPTLAT)::DOUBLE),
                             '{CENSUS_CRS}') AS geometry
            FROM read_csv('{txt}', delim = '{delim}', header = false, skip = 1, quote = '',
                          escape = '', columns = {{{cols}}}))""")
    txt.unlink()
    return rows


def census_gate(con, layer: str, dest: Path, rows: int) -> dict:
    """Rows equal the source's, GEOID unique, points in range, and every
    GEOID in the TIGER/Line layer on parquetry. TIGER areas missing here are
    counted, not fatal: the Gazetteer leaves out the island areas."""
    data = f"read_parquet('{dest}')"
    n, ids, bad = con.execute(f"""
        SELECT count(*), count(DISTINCT GEOID),
               count(*) FILTER (WHERE INTPTLAT NOT BETWEEN -90 AND 90
                                   OR INTPTLONG NOT BETWEEN -180 AND 180)
        FROM {data}""").fetchone()
    problems = []
    if n != rows:
        problems.append(f"{n:,} rows, the source has {rows:,}")
    if ids != n:
        problems.append(f"{n - ids:,} duplicate GEOIDs")
    if bad:
        problems.append(f"{bad:,} points out of range")
    if problems:
        sys.exit(f"census/{layer}: " + "; ".join(problems))
    out = {"rows": n, "source_rows": rows}
    tiger = CENSUS_LAYERS[layer][2]
    if tiger:
        path, key = tiger
        t = f"read_parquet('{TIGER}/{path}.parquet')"
        not_in_tiger, not_in_gaz = con.execute(f"""
            SELECT (SELECT count(*) FROM {data} g WHERE NOT EXISTS
                        (SELECT 1 FROM {t} t WHERE t.{key} = g.GEOID)),
                   (SELECT count(*) FROM {t} t WHERE NOT EXISTS
                        (SELECT 1 FROM {data} g WHERE g.GEOID = t.{key}))""").fetchone()
        if not_in_tiger:
            sys.exit(f"census/{layer}: {not_in_tiger:,} GEOIDs not in census/tiger/{YEAR}/"
                     f"{path}.parquet ({key}): the file would not join TIGER/Line")
        out["tiger"] = {"file": f"census/tiger/{YEAR}/{path}.parquet", "key": key,
                        "gazetteer_not_in_tiger": not_in_tiger,
                        "tiger_not_in_gazetteer": not_in_gaz}
    return out


def census_attribution(source: dict) -> str:
    rows = "\n".join(f"  {CENSUS_LAYERS[k][0] + '.parquet':22s} {v['url'].rsplit('/', 1)[1]}"
                     f"  (Last-Modified {v['last_modified']})" for k, v in source.items())
    return f"""\
US Census Bureau, {YEAR} Gazetteer Files
{CENSUS_PAGE}

Public domain (a work of the US federal government, 17 U.S.C. 105).
Credit: U.S. Census Bureau, {YEAR} Gazetteer Files.

Each file and the national text file it comes from:
{rows}

What was changed: the delimited text read into GeoParquet with its
columns and values unchanged (header whitespace trimmed, GEOID kept as text,
areas as integers, square miles and coordinates as numbers), plus a point
geometry at the internal point (INTPTLONG, INTPTLAT) in NAD83 (EPSG:4269),
a bbox covering column, Hilbert order and byte-capped row groups.
Republished by Geomermaids.
"""


def build_census(con, work: Path, out: Path) -> dict:
    source = census_upstream()
    dest_dir = out / "census" / YEAR
    dest_dir.mkdir(parents=True, exist_ok=True)
    crs = projjson(con)
    files, gates = [], {}
    for layer, (code, title, _) in CENSUS_LAYERS.items():
        zip_path = download(census_url(layer), work / "census" / f"{YEAR}_Gaz_{layer}_national.zip")
        rows = census_table(con, layer, zip_path, work)
        dest = dest_dir / f"{code}.parquet"
        e = write_src(con, dest, crs)
        verify(con, dest_dir, [code])
        gates[code] = census_gate(con, layer, dest, rows)
        files.append({"name": code, "layer": layer, "title": title, **e,
                      "source": source[layer]})
        t = gates[code].get("tiger")
        print(f"  census/{code}: {e['features']:,} rows, {e['bytes'] / 1e6:.1f} MB"
              + (f", {t['gazetteer_not_in_tiger']} not in TIGER, "
                 f"{t['tiger_not_in_gazetteer']} TIGER areas not here" if t else ""),
              flush=True)
    manifest = {"name": f"Census Gazetteer {YEAR}", "year": YEAR, "crs": CENSUS_CRS,
                "built": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source_page": CENSUS_PAGE, "files": files, "gate": gates}
    (dest_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (dest_dir / "ATTRIBUTION.txt").write_text(census_attribution(source))
    return source


# ---------- build: GeoNames ----------

def geonames_lookups(con, src: Path) -> None:
    """GeoNames' lookup files as tables: feature codes, countries, admin1 and
    admin2 names."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE feature_codes AS
        SELECT column0 AS code, column1 AS feature_name, column2 AS feature_description
        FROM read_csv('{src / 'featureCodes_en.txt'}', delim = '\t', header = false,
                      quote = '', escape = '', all_varchar = true)""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE admin1 AS
        SELECT column0 AS code, column1 AS name
        FROM read_csv('{src / 'admin1CodesASCII.txt'}', delim = '\t', header = false,
                      quote = '', escape = '', all_varchar = true)""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE admin2 AS
        SELECT column0 AS code, column1 AS name
        FROM read_csv('{src / 'admin2Codes.txt'}', delim = '\t', header = false,
                      quote = '', escape = '', all_varchar = true)""")
    # countryInfo.txt: comment lines start with '#'.
    lines = [ln for ln in (src / "countryInfo.txt").read_text(encoding="utf-8").splitlines()
             if ln and not ln.startswith("#")]
    con.execute("CREATE OR REPLACE TEMP TABLE countries (iso VARCHAR, name VARCHAR)")
    con.executemany("INSERT INTO countries VALUES (?, ?)",
                    [(ln.split("\t")[0], ln.split("\t")[4]) for ln in lines])
    classes = ", ".join(f"('{k}', '{v}')" for k, v in FEATURE_CLASSES.items())
    con.execute(f"CREATE OR REPLACE TEMP TABLE classes AS "
                f"SELECT * FROM (VALUES {classes}) t(code, name)")


def geonames_table(con, src: Path) -> int:
    """allCountries.txt as the table `g`: typed, decoded, with a point."""
    txt = src / "allCountries.txt"
    with zipfile.ZipFile(src / "allCountries.zip") as z:
        z.extract("allCountries.txt", src)
    cols = ", ".join(f"'{c}': 'VARCHAR'" for c, _ in GEONAMES_COLUMNS)
    typed = ", ".join(f"NULLIF(r.{c}, '')::{t} AS {c}" if t != "VARCHAR" else
                      f"NULLIF(r.{c}, '') AS {c}" for c, t in GEONAMES_COLUMNS)
    con.execute(f"""CREATE OR REPLACE TABLE g AS
        SELECT g.*, c.name AS feature_class_name, f.feature_name, f.feature_description,
               k.name AS country_name, a1.name AS admin1_name, a2.name AS admin2_name,
               ST_Point(g.longitude, g.latitude) AS geom
        FROM (SELECT {typed} FROM read_csv('{txt}', delim = '\t', header = false, quote = '',
                                           escape = '', columns = {{{cols}}}) r) g
        LEFT JOIN classes c ON c.code = g.feature_class
        LEFT JOIN feature_codes f ON f.code = g.feature_class || '.' || g.feature_code
        LEFT JOIN countries k ON k.iso = g.country_code
        LEFT JOIN admin1 a1 ON a1.code = g.country_code || '.' || g.admin1_code
        LEFT JOIN admin2 a2 ON a2.code = g.country_code || '.' || g.admin1_code || '.'
                                         || g.admin2_code""")
    rows = sum(1 for _ in txt.open("rb"))
    txt.unlink()
    return rows


# The published column order: the readme's, each code followed by its name.
GEONAMES_SELECT = """geonameid, name, asciiname, alternatenames, latitude, longitude,
    feature_class, feature_class_name, feature_code, feature_name, feature_description,
    country_code, country_name, cc2, admin1_code, admin1_name, admin2_code, admin2_name,
    admin3_code, admin4_code, population, elevation, dem, timezone, modification_date, geom"""


def geonames_gate(con, rows: int, world: dict, parts: list[dict]) -> dict:
    n, ids, bad, undecoded = con.execute("""
        SELECT count(*), count(DISTINCT geonameid),
               count(*) FILTER (WHERE latitude NOT BETWEEN -90 AND 90
                                   OR longitude NOT BETWEEN -180 AND 180),
               count(*) FILTER (WHERE feature_class IS NOT NULL AND feature_class_name IS NULL)
        FROM g""").fetchone()
    in_parts = sum(p["features"] for p in parts)
    problems = []
    if n != rows:
        problems.append(f"{n:,} rows, allCountries.txt has {rows:,} lines")
    if ids != n:
        problems.append(f"{n - ids:,} duplicate geonameids")
    if bad:
        problems.append(f"{bad:,} coordinates out of range")
    if undecoded:
        problems.append(f"{undecoded:,} feature classes not in FEATURE_CLASSES")
    if world["features"] != n or in_parts != n:
        problems.append(f"world file {world['features']:,} and country files {in_parts:,} "
                        f"rows, the table {n:,}")
    if problems:
        sys.exit("geonames: " + "; ".join(problems))
    codes, a1, a2, nocountry = con.execute("""
        SELECT count(*) FILTER (WHERE feature_code IS NOT NULL AND feature_name IS NULL),
               count(*) FILTER (WHERE admin1_code IS NOT NULL AND admin1_code <> '00'
                                  AND admin1_name IS NULL),
               count(*) FILTER (WHERE admin2_code IS NOT NULL AND admin2_name IS NULL),
               count(*) FILTER (WHERE country_code IS NULL)
        FROM g""").fetchone()
    return {"rows": n, "source_rows": rows, "country_files_rows": in_parts,
            "no_country": nocountry, "feature_code_unknown": codes,
            "admin1_code_unknown": a1, "admin2_code_unknown": a2}


def geonames_attribution(source: dict, dump_date: str) -> str:
    return f"""\
GeoNames geographical database, dump of {dump_date}
{GEONAMES_PAGE}  (files: {GEONAMES}/)

Licence: Creative Commons Attribution 4.0 International (CC BY 4.0),
https://creativecommons.org/licenses/by/4.0/
Credit: GeoNames (www.geonames.org), CC BY 4.0. GeoNames states the data
is provided "as is" without warranty or any representation of accuracy,
timeliness or completeness.

Sources: allCountries.zip (Last-Modified {source['allCountries.zip']['last_modified']}),
featureCodes_en.txt, admin1CodesASCII.txt, admin2Codes.txt, countryInfo.txt.

What was changed: the dump's columns under the readme's names, typed;
decoded companions added from GeoNames' own lookup files
(feature_class_name, feature_name, feature_description, country_name,
admin1_name, admin2_name); a WGS84 point at (longitude, latitude), a bbox
covering column, Hilbert order and byte-capped row groups. The same rows
also as one file per country (country=<ISO 3166-1 alpha-2>, `none` for rows
without a country), and cities1000: populated places (class P) of 1,000
people or more plus administrative seats ({', '.join(SEATS)}).
Republished by Geomermaids.
"""


def build_geonames(con, work: Path, out: Path) -> dict:
    source = geonames_upstream()
    src = work / "geonames"
    for f in GEONAMES_FILES:
        download(f"{GEONAMES}/{f}", src / f)
    dest_dir = out / "geonames" / "latest"
    dest_dir.mkdir(parents=True, exist_ok=True)
    geonames_lookups(con, src)
    rows = geonames_table(con, src)
    dump_date = str(con.execute("SELECT max(modification_date) FROM g").fetchone()[0])
    print(f"  geonames: {rows:,} rows, dump of {dump_date}", flush=True)

    world = write_layer(con, "geonames", f"SELECT {GEONAMES_SELECT} FROM g", "GeoNames",
                        dest_dir, out=dest_dir / "geonames.parquet")
    verify(con, dest_dir, ["geonames"])
    keys = [r[0] for r in con.execute(
        f"SELECT DISTINCT coalesce(country_code, '{NO_COUNTRY}') FROM g ORDER BY 1").fetchall()]
    bad = [k for k in keys if "/" in k or k != k.strip() or not k]
    if bad:
        sys.exit(f"geonames: country codes unfit for a path: {bad}")
    parts = []
    for key in keys:
        where = "country_code IS NULL" if key == NO_COUNTRY else f"country_code = '{key}'"
        pdir = dest_dir / f"country={key}"
        e = write_layer(con, "geonames", f"SELECT {GEONAMES_SELECT} FROM g WHERE {where}",
                        "GeoNames", pdir, out=pdir / "geonames.parquet", quiet=True)
        verify(con, pdir, ["geonames"])
        name = con.execute("SELECT name FROM countries WHERE iso = ?", [key]).fetchone()
        parts.append({"country": key, "name": name[0] if name else None,
                      "features": e["features"], "bytes": e["bytes"],
                      "row_groups": e["row_groups"],
                      "largest_row_group_bytes": e["largest_row_group_bytes"],
                      "sha256": e["sha256"]})
    print(f"  geonames: {len(parts)} country files, "
          f"{sum(p['bytes'] for p in parts) / 1e6:,.0f} MB", flush=True)

    seats = ", ".join(f"'{s}'" for s in SEATS)
    cities = write_layer(
        con, "cities1000",
        f"SELECT {GEONAMES_SELECT} FROM g WHERE feature_class = 'P' "
        f"AND (population >= 1000 OR feature_code IN ({seats}))",
        "GeoNames", dest_dir, out=dest_dir / "cities1000.parquet")
    verify(con, dest_dir, ["cities1000"])
    gate = geonames_gate(con, rows, world, parts)

    # Labels: thinned per zoom, the most populous place winning each cell.
    zooms, props = CITIES_TILES
    tiles = pmtiles.tile(dest_dir / "cities1000.parquet", dest_dir / "cities1000.pmtiles",
                         layer="cities1000", min_zoom=zooms[0], max_zoom=zooms[1],
                         properties=props, coverage=False,
                         extra=["--sort-key", "population"])
    print(f"  cities1000: {cities['features']:,} places, tiles z{zooms[0]}-{zooms[1]}, "
          f"{tiles['bytes'] / 1e6:.1f} MB", flush=True)

    def entry(e: dict) -> dict:
        return {k: e[k] for k in ("name", "file", "features", "bytes", "row_groups",
                                  "largest_row_group_bytes", "sha256")}

    manifest = {
        "name": "GeoNames", "dump_date": dump_date, "crs": "OGC:CRS84",
        "built": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_page": GEONAMES_PAGE, "source": source,
        "files": [entry(world), entry(cities)],
        "partitions": {"name": "geonames", "key": "country", "column": "country_code",
                       "no_country": NO_COUNTRY, "files": len(parts),
                       "bytes": sum(p["bytes"] for p in parts), "entries": parts},
        "pmtiles": [{"name": "cities1000", **tiles}],
        "gate": gate,
    }
    (dest_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (dest_dir / "ATTRIBUTION.txt").write_text(geonames_attribution(source, dump_date))
    con.execute("DROP TABLE g")
    return source


# ---------- build ----------

def build(work: Path, out: Path, parts: list[str]) -> None:
    (work / "duckdb").mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs; "
                f"SET temp_directory = '{work}/duckdb'; SET preserve_insertion_order = false")
    t0 = time.monotonic()
    source = {}
    if "census" in parts:
        source["census"] = build_census(con, work, out)
    if "geonames" in parts:
        source["geonames"] = build_geonames(con, work, out)
    (out / "_source.json").write_text(json.dumps(source, indent=2) + "\n")
    (out / "index.json").write_text(json.dumps({"datasets": [
        {"path": f"census/{YEAR}", "code": f"census-{YEAR}",
         "name": f"Census Gazetteer {YEAR}"},
        {"path": "geonames/latest", "code": "geonames", "name": "GeoNames"},
    ]}, indent=2) + "\n")
    (out / "snapshots.json").write_text(json.dumps({"latest": "", "snapshots": []}) + "\n")
    print(f"  built {', '.join(parts)} in {time.monotonic() - t0:.0f} s", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="Last-Modified and size of every source file")
    b = sub.add_parser("build")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help="the gazetteer/ prefix")
    b.add_argument("--part", action="append", choices=["census", "geonames"],
                   help="build this part only (repeatable; default both)")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
        return
    build(args.work_dir, args.out_dir, args.part or ["census", "geonames"])


if __name__ == "__main__":
    main()
