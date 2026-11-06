#!/usr/bin/env python3
"""
FEMA's National Risk Index (NRI) as GeoParquet and PMTiles.

FEMA retired the NRI application in 2025 and publishes the data on OpenFEMA
(https://www.fema.gov/about/openfema/data-sets/national-risk-index-data),
one zipped file geodatabase per level, the version in the path (v120 for
1.20, December 2025). This writes one GeoParquet file and one PMTiles archive
per level, and FEMA's hazard table:

  <out>/v<version>/tracts.parquet           85,154 census tracts, key TRACTFIPS
  <out>/v<version>/counties.parquet         3,232 counties, key STCOFIPS
  <out>/v<version>/states.parquet           56 states and territories, key STATEFIPS
  <out>/v<version>/tribal_counties.parquet  tribal areas within counties
  <out>/v<version>/hazards.parquet          the 18 hazards: prefix, period of record
  <out>/v<version>/<level>.pmtiles   tracts, counties, states, tribal_counties
  <out>/v<version>/NRIDataDictionary.csv    FEMA's field dictionary, as shipped
  <out>/v<version>/_manifest.json           rows, sizes, sha256, column aliases, gate
  <out>/v<version>/ATTRIBUTION.txt

Columns and values are FEMA's, unchanged, with three left out: the
geodatabase's OBJECTID (a feature id), and Shape_Length and Shape_Area,
which are measured in Web Mercator metres and would mislead (AREA, in square miles, is FEMA's
own area). FEMA ships Web Mercator (EPSG:3857); the geometry here is
reprojected to lon/lat (OGC:CRS84, the same WGS84 datum), so the files read
like the other US datasets and tile directly. Each row gets a `bbox` declared
as the GeoParquet covering, rows are sorted on a Hilbert curve and cut into
row groups of at most 4 MiB of geometry (gaul.py's rewrite). The geometry is
FEMA's otherwise, including the polygons that are not valid (82 tracts,
77 counties in v1.20; the manifest counts them per level).

The gate, before anything is written out: each level's rows and keys match
FEMA's table CSV of the same release, keys are unique (the tribal counties
have none), every tract nests in a county of the county file, and the keys
are compared with the published TIGER/Line 2026 geography (census/): the
mismatches are counted in the manifest, not dropped.

Usage:
  python3 scripts/datasets/nri.py upstream
  python3 scripts/datasets/nri.py build --work-dir work --out-dir out/nri
"""

from __future__ import annotations

import argparse
import csv
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
from datasets.census import BBOX
from datasets.gaul import rewrite, verify
from datasets.infra import GEOMETRY_TYPE_NAMES

VERSION = "1.20"
VDIR = "v120"          # the version as OpenFEMA writes it in the path
RELEASED = "December 2025"
SOURCE_PAGE = "https://www.fema.gov/about/openfema/data-sets/national-risk-index-data"
SOURCE_DIR = f"https://www.fema.gov/about/reports-and-data/openfema/nri/{VDIR}"
DOCS = {
    "technical": "https://www.fema.gov/sites/default/files/documents/"
                 "fema_national-risk-index_technical-documentation.pdf",
    "glossary": "https://www.fema.gov/sites/default/files/documents/"
                "fema_national-risk-index_data-glossary-documentation.pdf",
}
PUBLIC = "https://parquetry.geomermaids.com"
TIGER = f"{PUBLIC}/census/tiger/2026"

# name -> OpenFEMA file stem, geodatabase layer, key column, title, PMTiles
# (zooms, properties) or None. The minimum zooms are the lowest at which no
# tile goes over the tiler's size cap (pmtiles.tile() checks).
LEVELS = {
    "tracts": {
        "stem": "CensusTracts", "layer": "NRI_CensusTracts", "key": "TRACTFIPS",
        "title": "Census tracts", "tiger": ("substategeo/tract.parquet", "GEOID"),
        "tiles": ((7, 12), ["TRACTFIPS", "RISK_RATNG", "RISK_SCORE"]),
    },
    "counties": {
        "stem": "Counties", "layer": "NRI_Counties", "key": "STCOFIPS",
        "title": "Counties", "tiger": ("substategeo/county.parquet", "GEOID"),
        "tiles": ((0, 10), ["STCOFIPS", "COUNTY", "STATEABBRV", "RISK_RATNG", "RISK_SCORE",
                            "EAL_RATNG", "SOVI_RATNG", "RESL_RATNG"]),
    },
    "states": {
        "stem": "States", "layer": "NRI_States", "key": "STATEFIPS",
        "title": "States and territories", "tiger": ("substategeo/state.parquet", "GEOID"),
        # FEMA rates states on expected annual loss only (no composite, social
        # vulnerability or resilience columns): the map colours EAL_RATNG.
        "tiles": ((0, 10), ["STATEFIPS", "STATE", "STATEABBRV", "POPULATION", "EAL_RATNG",
                            "EAL_SCORE", "EAL_VALT"]),
    },
    "tribal_counties": {
        "stem": "Tribal_Counties", "layer": "NRI_Tribal_Counties", "key": None,
        "title": "Tribal areas within counties", "tiger": None,
        "tiles": ((0, 12), ["NRI_ID", "AIANNHCE", "JURS_NAME", "STATEABBRV", "COUNTY",
                            "STCOFIPS", "RISK_RATNG", "RISK_SCORE", "EAL_RATNG", "SOVI_RATNG",
                            "RESL_RATNG"]),
    },
}
# OBJECTID, the geodatabase's feature id, is not read as a column.
DROPPED = ["Shape_Length", "Shape_Area"]

# The five rating classes, low to high, then FEMA's two non-ratings.
RATINGS = ["Very Low", "Relatively Low", "Relatively Moderate", "Relatively High", "Very High"]
NON_RATINGS = ["No Rating", "Insufficient Data", "Not Applicable"]


def zip_url(kind: str, stem: str) -> str:
    """kind: GDB (geometry and attributes) or Table (the same as CSV)."""
    return f"{SOURCE_DIR}/NRI_{kind}_{stem}.zip"


# fema.gov's edge answers Python's urllib with 403 whatever the User-Agent,
# and lets curl through: every request goes through curl.
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


def versions_on_page() -> list[str] | None:
    """The NRI versions OpenFEMA's page links to (v120, ...), or None when the
    page cannot be read. The file URLs carry the version, so a new release
    appears here as a new path rather than as a changed file."""
    try:
        html = curl(SOURCE_PAGE)
    except subprocess.CalledProcessError as exc:
        print(f"  {SOURCE_PAGE}: curl exit {exc.returncode}", file=sys.stderr)
        return None
    return sorted(set(re.findall(r"/openfema/nri/(v\d+)/", html)), key=lambda v: int(v[1:]))


def upstream() -> dict:
    """Each source zip's Last-Modified, ETag and size, and the versions the
    page lists. A zip that does not answer fails: the build reads only these."""
    files = {}
    for name, spec in LEVELS.items():
        for kind in ("GDB", "Table"):
            files[f"{name}/{kind}"] = head(zip_url(kind, spec["stem"]))
    return {"version": VERSION, "path": VDIR, "page_versions": versions_on_page(), "files": files}


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


def fetch(url: str, dest: Path, stale: list[Path]) -> Path:
    """The zip, kept in dest with the server's ETag and Last-Modified beside
    it: a copy whose stamp still matches is reused. When the zip is fetched
    again, what was extracted from the old one (stale) is removed, so it is
    extracted again from the new one."""
    meta = head(url)
    stamp = dest.with_name(dest.name + ".source.json")
    want = {k: meta[k] for k in ("etag", "last_modified", "bytes")}
    if (dest.is_file() and dest.stat().st_size == meta["bytes"] and stamp.is_file()
            and json.loads(stamp.read_text()) == want):
        return dest
    for path in stale:
        remove(path)
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


def extracted(work: Path, stem: str) -> dict[str, Path]:
    """What unpack() and table_csv() write for a level."""
    return {"gdb": work / f"NRI_GDB_{stem}.gdb", "dictionary": work / stem,
            "csv": work / f"NRI_Table_{stem}.csv"}


def unpack(archive: Path, work: Path, stem: str) -> tuple[Path, Path]:
    """The geodatabase and the dictionary out of a GDB zip (fetch() removes
    both when the zip changes)."""
    gdb = extracted(work, stem)["gdb"]
    with zipfile.ZipFile(archive) as z:
        if not gdb.is_dir():
            z.extractall(work, [m for m in z.namelist() if m.startswith(f"{gdb.name}/")])
        z.extract("NRIDataDictionary.csv", work / stem)
    return gdb, work / stem / "NRIDataDictionary.csv"


def table_csv(archive: Path, work: Path, stem: str) -> Path:
    dest = extracted(work, stem)["csv"]
    if not dest.is_file():
        with zipfile.ZipFile(archive) as z:
            z.extract(dest.name, work)
    return dest


def dictionary(path: Path) -> dict[str, dict]:
    """FEMA's dictionary: field -> alias, hazard (Relevant Layer), metric."""
    with path.open(encoding="utf-8-sig") as f:
        return {r["Field Name"]: {"alias": r["Field Alias"], "layer": r["Relevant Layer"],
                                  "metric": r["Metric Type"], "type": r["Type"]}
                for r in csv.DictReader(f)}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def geo_footer(con, table: str) -> tuple[str, int, tuple[float, float, float, float]]:
    n, xmin, ymin, xmax, ymax, types = con.execute(f"""
        SELECT count(*), min(ST_XMin(geometry)), min(ST_YMin(geometry)), max(ST_XMax(geometry)),
               max(ST_YMax(geometry)), list(DISTINCT ST_GeometryType(geometry)::VARCHAR)
        FROM {table}""").fetchone()
    # No `crs`: GeoParquet's default, OGC:CRS84, which this is.
    geo = json.dumps({
        "version": "2.0.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB",
            "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in types),
            "bbox": [xmin, ymin, xmax, ymax],
            "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}},
        }},
    }).replace("'", "''")
    return geo, n, (xmin, ymin, xmax, ymax)


def write_level(con, gdb: Path, layer: str, dest: Path) -> dict:
    """One level to GeoParquet 2.0: FEMA's columns, lon/lat geometry, bbox
    covering, Hilbert order, then row groups capped by bytes."""
    con.execute(f"""CREATE OR REPLACE TABLE src AS
        SELECT *, {BBOX} FROM (
            SELECT * EXCLUDE (Shape, {', '.join(DROPPED)}),
                   ST_Transform(Shape, 'EPSG:3857', 'OGC:CRS84') AS geometry
            FROM ST_Read('{gdb}', layer = '{layer}'))""")
    geo, n, (xmin, ymin, xmax, ymax) = geo_footer(con, "src")
    raw = dest.with_suffix(".raw.parquet")
    con.execute(f"""
        COPY (SELECT * FROM src ORDER BY ST_Hilbert(geometry,
                  ST_Extent(ST_MakeEnvelope({xmin!r}, {ymin!r}, {xmax!r}, {ymax!r}))))
        TO '{raw}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', COMPRESSION ZSTD,
                    KV_METADATA {{geo: '{geo}'}})""")
    rewrite(raw, dest)
    raw.unlink()
    verify(con, dest.parent, [dest.stem])
    groups, largest = con.execute(f"""
        SELECT count(*), max(b) FROM (SELECT row_group_id, sum(total_compressed_size) AS b
        FROM parquet_metadata('{dest}') GROUP BY 1)""").fetchone()
    invalid = con.execute(f"SELECT count(*) FROM read_parquet('{dest}') "
                          "WHERE NOT ST_IsValid(geometry)").fetchone()[0]
    return {"file": dest.name, "features": n, "bytes": dest.stat().st_size,
            "row_groups": groups, "largest_row_group_bytes": largest,
            "invalid_geometries": invalid, "sha256": sha256(dest)}


def gate(con, name: str, dest: Path, csv_path: Path, counties: Path | None) -> dict:
    """Rows and keys against FEMA's table CSV, key uniqueness, nesting and
    the TIGER comparison. A mismatch with FEMA's own table fails; one with
    TIGER is counted and reported."""
    spec = LEVELS[name]
    data = f"read_parquet('{dest}')"
    table = f"read_csv('{csv_path}', all_varchar = true, header = true)"
    rows, table_rows = (con.execute(f"SELECT count(*) FROM {data}").fetchone()[0],
                        con.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
    out = {"rows": rows, "table_rows": table_rows}
    problems = []
    if rows != table_rows:
        problems.append(f"{rows:,} rows, FEMA's table has {table_rows:,}")
    # NRI_ID is the per-row identifier of every level but the tribal one,
    # where it names the county.
    id_col = spec["key"] or "NRI_ID"
    missing = con.execute(f"""SELECT count(*) FROM (SELECT {id_col} FROM {table}
                              EXCEPT SELECT {id_col} FROM {data})""").fetchone()[0]
    extra = con.execute(f"""SELECT count(*) FROM (SELECT {id_col} FROM {data}
                            EXCEPT SELECT {id_col} FROM {table})""").fetchone()[0]
    if missing or extra:
        problems.append(f"{id_col}: {missing} in FEMA's table only, {extra} in the "
                        f"geodatabase only")
    key = spec["key"]
    if key:
        nulls, distinct = con.execute(f"SELECT count(*) FILTER (WHERE {key} IS NULL), "
                                      f"count(DISTINCT {key}) FROM {data}").fetchone()
        if nulls or distinct != rows:
            problems.append(f"{key}: {nulls} null, {rows - distinct} repeated")
        out["key"] = key
    else:
        # The tribal layer: an area per tribal land and county.
        out["repeated_tribe_county_pairs"] = con.execute(f"""
            SELECT count(*) - count(DISTINCT (AIANNHCE, STCOFIPS)) FROM {data}""").fetchone()[0]
    if name == "tracts":
        bad = con.execute(f"SELECT count(*) FROM {data} WHERE NOT starts_with(TRACTFIPS, STCOFIPS)"
                          ).fetchone()[0]
        orphans = con.execute(f"""SELECT count(*) FROM {data} t
            ANTI JOIN read_parquet('{counties}') c USING (STCOFIPS)""").fetchone()[0]
        if bad or orphans:
            problems.append(f"{bad} TRACTFIPS outside their STCOFIPS, {orphans} tracts in "
                            f"no county of the county file")
    if spec["tiger"]:
        file, col = spec["tiger"]
        tiger = f"read_parquet('{TIGER}/{file}')"
        nri_only = [r[0] for r in con.execute(f"""SELECT {key} FROM {data}
            EXCEPT SELECT {col} FROM {tiger} ORDER BY 1""").fetchall()]
        tiger_only = [r[0] for r in con.execute(f"""SELECT {col} FROM {tiger}
            EXCEPT SELECT {key} FROM {data} ORDER BY 1""").fetchall()]
        out["tiger"] = {"file": f"{TIGER}/{file}", "nri_only": len(nri_only),
                        "tiger_only": len(tiger_only), "nri_only_keys": nri_only[:50],
                        "tiger_only_keys": tiger_only[:50]}
        if name == "tracts" and tiger_only:
            # TIGER's tracts that FEMA leaves out: water-only tracts are 99xxxx.
            out["tiger"]["tiger_only_water_tracts"] = sum(k[5:7] == "99" for k in tiger_only)
    if problems:
        sys.exit(f"{name}: gate failed: " + "; ".join(problems))
    return out


def hazards(con, gdb: Path, dest: Path) -> dict:
    """FEMA's NRI_HazardInfo: the 18 hazards, their column prefix, period of
    record and which consequence types have a loss. No geometry."""
    con.execute(f"""COPY (SELECT * FROM ST_Read('{gdb}', layer = 'NRI_HazardInfo')
                          ORDER BY Prefix)
                    TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
    if n != 18:
        sys.exit(f"{dest.name}: {n} hazards, FEMA documents 18")
    return {"file": dest.name, "features": n, "bytes": dest.stat().st_size,
            "sha256": sha256(dest)}


def tiles(parquet: Path, name: str, zooms: tuple[int, int], properties: list[str]) -> dict:
    return pmtiles.tile(parquet, parquet.with_suffix(".pmtiles"), layer=name,
                        min_zoom=zooms[0], max_zoom=zooms[1], properties=properties)


def attribution(source: dict) -> str:
    return f"""\
# Data attribution

This data is FEMA's National Risk Index (NRI), version {VERSION} ({RELEASED}),
produced by the Federal Emergency Management Agency.

  Source:     {SOURCE_DIR}/ (OpenFEMA; one GDB and one Table zip per level)
  Page:       {SOURCE_PAGE}
  Methods:    {DOCS['technical']}
  Glossary:   {DOCS['glossary']}
  Producer:   FEMA, Risk Management Directorate (FEMA-NRI@fema.dhs.gov)
  Status:     a work of the United States Government, not subject to copyright
              in the United States (17 U.S.C. 105)

Credit FEMA, National Risk Index, version {VERSION}, when you use it.

FEMA retired the National Risk Index application in 2025 and moved the data
to OpenFEMA and to the Resilience Analysis and Planning Tool (RAPT). The
Future Risk Index is not part of this release.

## What the values mean

Scores are national percentiles among communities of the same level
(county or tract). Ratings are one of five classes, Very Low to Very High,
from that comparison. Values are dollars a year for the Risk Index and
Expected Annual Loss, and index values for Social Vulnerability and
Community Resilience. A score is relative: compare tracts with tracts and
counties with counties.

## What was changed here

One GeoParquet 2.0 file per level (tracts, counties, states, tribal
counties) and FEMA's hazard table. Columns and values are FEMA's, unchanged,
except OBJECTID, Shape_Length and Shape_Area, left out (the last two are in
Web Mercator metres; AREA, square miles, is FEMA's area). The geometry is
reprojected from Web Mercator (EPSG:3857) to lon/lat (OGC:CRS84), with a
`bbox` column declared as the GeoParquet covering, rows sorted along a
Hilbert curve. FEMA's field dictionary is beside the files,
NRIDataDictionary.csv. The PMTiles archives are derived for web maps.

Source files, as read for this build:
{chr(10).join(f"  {k}: {v['url']} (Last-Modified {v['last_modified']})" for k, v in source['files'].items())}

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def build(work: Path, out: Path, only: list[str] | None = None) -> None:
    work.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs; "
                f"SET temp_directory = '{work / 'tmp'}'")
    source = upstream()
    folder = out / f"v{VERSION}"
    folder.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    files, tile_entries, gates, labels, themes = [], [], {}, {}, {}
    fields: dict[str, dict] = {}
    # Counties first: the tract gate checks the nesting against them.
    order = sorted(LEVELS, key=lambda n: n != "counties")
    for name in order:
        if only and name not in only and name != "counties":
            continue
        spec = LEVELS[name]
        out_of = extracted(work, spec["stem"])
        gdb_zip = fetch(zip_url("GDB", spec["stem"]), work / f"NRI_GDB_{spec['stem']}.zip",
                        stale=[out_of["gdb"], out_of["dictionary"]])
        table_zip = fetch(zip_url("Table", spec["stem"]), work / f"NRI_Table_{spec['stem']}.zip",
                          stale=[out_of["csv"]])
        gdb, dict_path = unpack(gdb_zip, work, spec["stem"])
        fields = dictionary(dict_path)
        dest = folder / f"{name}.parquet"
        entry = write_level(con, gdb, spec["layer"], dest)
        gates[name] = gate(con, name, dest, table_csv(table_zip, work, spec["stem"]),
                           folder / "counties.parquet")
        columns = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{dest}')"
                                             ).fetchall()]
        undocumented = [c for c in columns if c not in fields and c not in ("geometry", "bbox")]
        if undocumented:
            sys.exit(f"{name}: columns not in FEMA's dictionary: {undocumented}")
        files.append({"name": name, "layer": spec["layer"], "key": spec["key"], **entry,
                      "gate": gates[name]})
        themes[name], labels[name] = entry["features"], spec["title"]
        if spec["tiles"]:
            tile_entries.append({"name": name, **tiles(dest, name, *spec["tiles"])})
        print(f"  {name}: {entry['features']:,} rows, {entry['bytes'] / 1e6:.1f} MB, "
              f"{entry['row_groups']} row groups, {time.monotonic() - t0:.0f} s", flush=True)
    files.append({"name": "hazards", "layer": "NRI_HazardInfo", "key": "Prefix",
                  **hazards(con, gdb, folder / "hazards.parquet")})
    dict_out = folder / "NRIDataDictionary.csv"
    shutil.copyfile(dict_path, dict_out)
    (folder / "ATTRIBUTION.txt").write_text(attribution(source))
    (folder / "_manifest.json").write_text(json.dumps({
        "state_name": f"FEMA National Risk Index {VERSION}",
        "version": VERSION,
        "released": RELEASED,
        "total_features": sum(themes.values()),
        "themes": themes,
        "labels": labels,
        "crs": "OGC:CRS84",
        "source": source,
        "files": files,
        "pmtiles": tile_entries,
        # FEMA's dictionary, for the catalog's column descriptions.
        "dictionary": {"file": dict_out.name, "bytes": dict_out.stat().st_size,
                       "sha256": sha256(dict_out)},
        "fields": fields,
    }, indent=2) + "\n")
    # The parquetry repository files at the dataset root.
    (out / "index.json").write_text(json.dumps({"datasets": [
        {"path": f"v{VERSION}", "code": f"v{VERSION}",
         "name": f"FEMA National Risk Index {VERSION}"}]}, indent=2) + "\n")
    (out / "snapshots.json").write_text(json.dumps({"latest": "", "snapshots": []}) + "\n")
    (out / "_source.json").write_text(json.dumps(source, indent=2) + "\n")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="print each source zip's Last-Modified and the page's versions")
    b = sub.add_parser("build", help="fetch, convert, gate, tile and verify")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True, help="the nri/ prefix, e.g. out/nri")
    b.add_argument("--level", action="append", choices=list(LEVELS),
                   help="build only this level (repeatable; counties are always built)")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
        return
    build(args.work_dir, args.out_dir, args.level)


if __name__ == "__main__":
    main()
