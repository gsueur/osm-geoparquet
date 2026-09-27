#!/usr/bin/env python3
"""
Corine Land Cover 2018, one file for Europe and one per country.

CLC 2018 is one pan-European mosaic: 2,375,406 polygons in ETRS89 / LAEA
Europe (EPSG:3035), with no country attribute (IDs are `EU_<n>`). This
gives every polygon a `country` (ISO 3166-1 alpha-2) and writes:

  <out>/clc_2018.parquet                   Europe, sorted by country, then
                                           along a Hilbert curve
  <out>/country=<XX>/clc_2018.parquet      one country, same columns
  <out>/_manifest.json, country=<XX>/_manifest.json, ../index.json
                                           the parquetry repository files
  <out>/ATTRIBUTION.txt

A polygon is never cut: it goes whole to the country it overlaps most, so
geometry, `Area_Ha` and `ID` are the source's and the country files add up
to the Europe file exactly. Borders are GAUL 2024 L0 (our derived layer,
reprojected to EPSG:3035), cut down to pieces of at most 1,000 vertices so
the intersections stay cheap. A polygon that overlaps no country (tidal flats, sea, coastal
lagoons outside GAUL's coastline, the Azores islands GAUL lacks) goes to
the nearest one.

GAUL follows the UN delineation: it has no Kosovo, so CLC's Kosovo
polygons go to Serbia (RS), and Northern Cyprus is Cyprus (CY).

The source is the published Europe file itself: the Copernicus download
needs an EU Login, and the file online is the source unchanged. Only the
source columns are read, so a re-run on this script's own output gives the
same result. Nothing is written unless every check passes: same rows, same
IDs, same geometries (hashed), same total area, every row in one country.

  python3 scripts/datasets/clc.py --source clc_2018.parquet \\
      --gaul GAUL_2024_L0_derived.parquet --gaul-manifest gaul_manifest.json \\
      --out-dir out/2018
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

import duckdb
import pycountry

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.infra import GEOMETRY_TYPE_NAMES

RELEASE = "2018"
STEM = f"clc_{RELEASE}"
SOURCE_COLUMNS = ["Shape", "OBJECTID", "Code_18", "Remark", "Area_Ha", "ID"]
ROW_GROUP_ROWS = 10_240   # a multiple of DuckDB's 2,048-row vectors, ~16 MB
# The distributing-geoparquet guide asks for 15 or more. Measured on 204,800
# CLC rows with DuckDB's writer: 9 -> 170.6 MB, 15 -> 170.7 MB (11x slower
# for nothing), 19 -> 159.3 MB (17x slower). Reads take the same time.
ZSTD_LEVEL = 19
MAX_POINTS = 1_000        # vertices per piece of border
CHUNK = 250_000           # source polygons per pass of the border join
HEAVY = 5_000             # vertices above which a CLC polygon is cut into parts
NEAREST = (50_000, 500_000)  # metres: the nearest-country passes, see assign()

# CLC 2018 covers the EEA39: the EU 27, then IS LI NO CH TR GB AL BA ME MK RS
# (and XK, which GAUL has not). The micro-states and territories GAUL keeps
# apart are candidates too, so a polygon in them is not pushed next door;
# a candidate that wins no polygon gets no folder. Neighbours outside CLC
# (RU, BY, UA, MD, SY, the Maghreb) are not candidates: a border polygon
# that GAUL puts mostly across goes to the CLC side.
COUNTRIES = """
    AT BE BG CY CZ DE DK EE ES FI FR GR HR HU IE IT LT LU LV MT NL PL PT RO SE SI SK
    IS LI NO CH TR GB AL BA ME MK RS
    AD MC SM VA GI AX FO GG JE IM
""".split()


def country_name(alpha2: str) -> str:
    c = pycountry.countries.get(alpha_2=alpha2)
    return getattr(c, "common_name", None) or c.name


def projjson(con: duckdb.DuckDBPyConnection) -> dict:
    """EPSG:3035 as PROJJSON, for the `geo` footer. DuckDB has no function
    for it, but its V2 writer puts it in the footer of a probe file."""
    with tempfile.TemporaryDirectory() as tmp:
        probe = Path(tmp) / "probe.parquet"
        con.execute(f"""COPY (SELECT ST_SetCRS(ST_Point(0, 0), 'EPSG:3035') AS g)
                        TO '{probe}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')""")
        geo = con.execute(f"SELECT decode(value) FROM parquet_kv_metadata('{probe}') "
                          "WHERE key = 'geo'").fetchone()[0]
    crs = json.loads(geo)["columns"]["g"]["crs"]
    assert crs["id"] == {"authority": "EPSG", "code": 3035}, crs.get("id")
    return crs


def borders(con: duckdb.DuckDBPyConnection, gaul: Path) -> None:
    """`piece`: the candidate countries in EPSG:3035, cut into small pieces."""
    iso3 = {pycountry.countries.get(alpha_2=a).alpha_3: a for a in COUNTRIES}
    con.execute("CREATE TABLE iso (iso3 VARCHAR, country VARCHAR)")
    con.executemany("INSERT INTO iso VALUES (?, ?)", list(iso3.items()))
    con.execute(f"""
        CREATE TABLE border AS
        SELECT i.country,
               ST_Transform(g.geometry::GEOMETRY, 'EPSG:4326', 'EPSG:3035',
                            always_xy := true) AS geom
        FROM read_parquet('{gaul}') g JOIN iso i ON i.iso3 = g.iso3_code""")
    missing = set(COUNTRIES) - {r[0] for r in con.execute("SELECT country FROM border").fetchall()}
    assert not missing, f"not in GAUL L0: {sorted(missing)}"
    con.execute("CREATE TABLE piece AS SELECT country, geom FROM border")
    levels = quadtree(con, "piece", "country")
    con.execute("ALTER TABLE piece ADD COLUMN pid BIGINT")
    con.execute("UPDATE piece SET pid = rowid")
    n, pts = con.execute("SELECT count(*), max(ST_NPoints(geom)) FROM piece").fetchone()
    print(f"  borders: {n:,} pieces of at most {pts:,} vertices, {levels} levels", flush=True)


def quadtree(con: duckdb.DuckDBPyConnection, table: str, key: str) -> int:
    """Split every row of `table` (key, geom) with more than MAX_POINTS
    vertices into its four quadrants, level by level, until none is left.

    A spatial join copies the probe geometry into every candidate pair,
    2,048 pairs to a vector: one polygon of 400,000 vertices whose bbox
    covers thousands of border pieces is gigabytes of copies. Cut down, the
    pieces are small on both sides, and areas still add up over them. A
    flat grid would not do either: each tile row would carry a copy of
    the whole geometry. Returns the number of levels."""
    for level in range(30):
        big = con.execute(f"SELECT count(*) FROM {table} WHERE ST_NPoints(geom) > {MAX_POINTS}").fetchone()[0]
        if not big:
            return level
        con.execute(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT {key}, geom FROM {table} WHERE ST_NPoints(geom) <= {MAX_POINTS}
            UNION ALL
            SELECT {key}, ST_Intersection(geom, quad) FROM (
                SELECT {key}, geom, unnest([
                    ST_MakeEnvelope(x0, y0, xm, ym), ST_MakeEnvelope(xm, y0, x1, ym),
                    ST_MakeEnvelope(x0, ym, xm, y1), ST_MakeEnvelope(xm, ym, x1, y1)]) AS quad
                FROM (SELECT {key}, geom, ST_XMin(geom) AS x0, ST_YMin(geom) AS y0,
                             ST_XMax(geom) AS x1, ST_YMax(geom) AS y1,
                             (ST_XMin(geom) + ST_XMax(geom)) / 2 AS xm,
                             (ST_YMin(geom) + ST_YMax(geom)) / 2 AS ym
                      FROM {table} WHERE ST_NPoints(geom) > {MAX_POINTS}))
            WHERE ST_Intersects(geom, quad)""")
        con.execute(f"DELETE FROM {table} WHERE ST_IsEmpty(geom) OR ST_Area(geom) = 0")
    sys.exit(f"the {table} quadtree did not converge")


def assign(con: duckdb.DuckDBPyConnection, source: Path) -> None:
    """`assign`: (id, country, how), one row per source polygon.

    Polygons of more than HEAVY vertices (5,633 of them, 105 M vertices)
    are cut into `part`s first, see quadtree(); the others are read from
    the file as they are, in slices of CHUNK rows (a file_row_number range
    reads only its own row groups)."""
    src = f"read_parquet('{source}', file_row_number = true)"
    con.execute(f"""
        CREATE TABLE part AS
        SELECT ID AS id, ST_MakeValid(Shape) AS geom FROM {src} WHERE ST_NPoints(Shape) > {HEAVY}""")
    heavy = con.execute("SELECT count(*) FROM part").fetchone()[0]
    quadtree(con, "part", "id")
    print(f"  heavy polygons: {heavy:,}, cut into {con.execute('SELECT count(*) FROM part').fetchone()[0]:,} parts",
          flush=True)

    def light(where: str = "true") -> str:
        return (f"(SELECT ID AS id, Shape AS geom FROM {src} "
                f"WHERE ST_NPoints(Shape) <= {HEAVY} AND {where})")

    rows = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
    con.execute("CREATE TABLE hit (id VARCHAR, country VARCHAR)")
    for start in [*range(0, rows, CHUNK), None]:
        shapes = ("part" if start is None else
                  light(f"file_row_number >= {start} AND file_row_number < {start + CHUNK}"))
        con.execute(f"""
            INSERT INTO hit
            SELECT DISTINCT s.id, p.country
            FROM {shapes} s JOIN piece p ON ST_Intersects(s.geom, p.geom)""")
    con.execute("CREATE TABLE hits AS SELECT DISTINCT id, country FROM hit")
    con.execute("""
        CREATE TABLE many AS
        SELECT id FROM hits GROUP BY id HAVING count(*) > 1""")
    # Areas only for the polygons across a border (about 2%).
    con.execute(f"""
        CREATE TABLE assign AS
        SELECT id, any_value(country) AS country, 'inside' AS how
        FROM hits ANTI JOIN many USING (id) GROUP BY id
        UNION ALL
        SELECT id, arg_max(country, area), 'overlap' FROM (
            SELECT s.id, p.country, sum(ST_Area(ST_Intersection(ST_MakeValid(s.geom), p.geom))) AS area
            FROM (SELECT * FROM {light()} SEMI JOIN many USING (id)
                  UNION ALL SELECT * FROM part SEMI JOIN many USING (id)) s
            JOIN piece p ON ST_Intersects(s.geom, p.geom)
            GROUP BY ALL)
        GROUP BY id""")
    # Nearest in two passes: 50 km settles the coast; the wider pass is for
    # islands GAUL 2024 does not have (São Miguel, Santa Maria, Flores and
    # Corvo in the Azores), a few hundred polygons.
    for radius in NEAREST:
        con.execute(f"""
            INSERT INTO assign
            SELECT s.id, arg_min(p.country, ST_Distance(s.geom, p.geom)), 'nearest'
            FROM (SELECT * FROM {light()} ANTI JOIN assign a USING (id)
                  UNION ALL SELECT * FROM part ANTI JOIN assign a USING (id)) s
            JOIN piece p ON ST_DWithin(s.geom, p.geom, {radius})
            GROUP BY s.id""")
    left = con.execute(f"SELECT count(*) FROM {src} x ANTI JOIN assign a ON a.id = x.ID").fetchone()[0]
    assert left == 0, f"{left:,} polygons are more than {NEAREST[-1] / 1000:.0f} km from every country"


def write(con: duckdb.DuckDBPyConnection, query: str, dest: Path, crs: dict) -> None:
    """One GeoParquet 2.0 file: native GEOMETRY typed EPSG:3035, a `geo`
    footer with the bbox covering, row groups of ROW_GROUP_ROWS."""
    xmin, ymin, xmax, ymax, types = con.execute(f"""
        SELECT min(bbox.xmin), min(bbox.ymin), max(bbox.xmax), max(bbox.ymax),
               list(DISTINCT ST_GeometryType(Shape)::VARCHAR)
        FROM ({query})""").fetchone()
    geo = json.dumps({
        "version": "2.0.0", "primary_column": "Shape",
        "columns": {"Shape": {
            "encoding": "WKB",
            "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in types),
            "crs": crs,
            "bbox": [xmin, ymin, xmax, ymax],
            "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}},
        }},
    }).replace("'", "''")
    dest.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY ({query}) TO '{dest}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE',
            COMPRESSION ZSTD, COMPRESSION_LEVEL {ZSTD_LEVEL}, ROW_GROUP_SIZE {ROW_GROUP_ROWS},
            KV_METADATA {{geo: '{geo}'}})""")


def verify(con: duckdb.DuckDBPyConnection, path: Path) -> dict:
    """Fail unless the file is what its footer says; return its manifest entry."""
    geos = [v for k, v in con.execute("SELECT key::VARCHAR, decode(value) FROM parquet_kv_metadata(?)",
                                      [str(path)]).fetchall() if k == "geo"]
    assert len(geos) == 1, f"{path}: {len(geos)} geo keys"
    col = json.loads(geos[0])["columns"]["Shape"]
    assert col["crs"]["id"]["code"] == 3035, f"{path}: crs {col['crs'].get('id')}"
    logical = con.execute("SELECT logical_type FROM parquet_schema(?) WHERE name = 'Shape'",
                          [str(path)]).fetchone()[0]
    assert logical and logical.startswith("GeometryType(crs=") and "3035" in logical, \
        f"{path}: Shape is {str(logical)[:80]}"
    groups = [n for _, n in con.execute("""
        SELECT DISTINCT row_group_id, row_group_num_rows FROM parquet_metadata(?)
        ORDER BY row_group_id""", [str(path)]).fetchall()]
    assert all(n == ROW_GROUP_ROWS for n in groups[:-1]), f"{path}: row groups {groups}"
    rows, bad, typ = con.execute(f"""
        SELECT count(*),
               count(*) FILTER (WHERE bbox.xmin > ST_XMin(Shape) OR bbox.ymin > ST_YMin(Shape)
                                   OR bbox.xmax < ST_XMax(Shape) OR bbox.ymax < ST_YMax(Shape)),
               any_value(typeof(Shape))
        FROM read_parquet('{path}')""").fetchone()
    assert bad == 0, f"{path}: {bad} bboxes do not contain their geometry"
    assert typ == "GEOMETRY('EPSG:3035')", f"{path}: reads as {typ}"
    largest = con.execute("""SELECT max(b) FROM (SELECT sum(total_compressed_size) AS b
                             FROM parquet_metadata(?) GROUP BY row_group_id)""", [str(path)]).fetchone()[0]
    return {"file": path.name, "features": rows, "bytes": path.stat().st_size,
            "row_groups": len(groups), "largest_row_group_bytes": largest,
            "sha256": sha256(path)}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 24), b""):
            h.update(chunk)
    return h.hexdigest()


def fingerprint(con: duckdb.DuckDBPyConnection, path: Path | str) -> tuple:
    """What must not change: rows, IDs, geometries, area."""
    return con.execute(f"""
        SELECT count(*), count(DISTINCT ID), sum(hash(ST_AsWKB(Shape))::HUGEINT),
               round(sum(Area_Ha), 3)
        FROM read_parquet('{path}')""").fetchone()


def attribution(gaul_accessed: str) -> str:
    return f"""\
# Data attribution

This data is Corine Land Cover (CLC) 2018, produced by the European
Environment Agency (EEA) under the framework of the Copernicus Land
Monitoring Service.

  Source:       https://land.copernicus.eu/pan-european/corine-land-cover
  Product:      Corine Land Cover (CLC) 2018, Version 2020 20u1
  Producer:     European Environment Agency (EEA), Copernicus programme
  Access:       Full, open and free access under the Copernicus data and
                information policy, Regulation (EU) No 1159/2013
  Attribution:  (c) European Union, Copernicus Land Monitoring Service,
                European Environment Agency (EEA)

Downstream users of these files MUST:
  1. Credit the Copernicus Land Monitoring Service and the EEA in any
     product produced from this data.
  2. Keep this notice with the data when redistributing it.

# Nomenclature and legend

Land cover classes are the standard CLC three-digit codes in the
`Code_18` column (111 continuous urban fabric ... 523 sea and ocean, 999
NODATA). The official 47-colour legend is built into GeoPQ Workbench:
styling by `Code_18` applies it, with the class names, automatically.

# Countries

The `country` column (ISO 3166-1 alpha-2) is not part of CLC. It was
added by Geomermaids: each polygon goes whole to the country it overlaps
most, or to the nearest one when it overlaps none (tidal flats, sea). No
polygon is cut, so a polygon across a border sits in one country's file
only. The borders are GAUL 2024, following the UN delineation: Kosovo's
polygons are in Serbia (RS), and Northern Cyprus is in Cyprus (CY).

  FAO. 2024. Global Administrative Unit Layers (GAUL).
  [Accessed on {gaul_accessed}]. https://data.apps.fao.org/?lang=en.
  Licence: CC-BY-4.0

FAO has not participated in, sponsored, approved or endorsed this use of
GAUL. The designations employed and the presentation of material in GAUL
do not imply the expression of any opinion whatsoever on the part of FAO
concerning the legal status of any country, territory, city or area or of
its authorities, or concerning the delimitation of its frontiers or
boundaries.

# Source notes

Original distribution: GeoPackage, ETRS89 / LAEA Europe (EPSG:3035),
2,375,406 features. Converted to GeoParquet with a covering bbox column,
sorted by country then along a Hilbert curve, in row groups of
{ROW_GROUP_ROWS:,} polygons, so a filter on country or on a bbox reads only
the groups it touches. Geometry, attributes and CRS are unchanged from the
source; `country` is the one column added.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--source", type=Path, required=True, help="the published clc_2018.parquet")
    p.add_argument("--gaul", type=Path, required=True, help="GAUL_2024_L0_derived.parquet")
    p.add_argument("--gaul-manifest", type=Path, required=True,
                   help="the live gaul/2024/_manifest.json (its access date goes in the citation)")
    p.add_argument("--out-dir", type=Path, required=True, help="the release folder, e.g. out/2018")
    p.add_argument("--work-dir", type=Path, default=Path("work"),
                   help="DuckDB database and spill space")
    args = p.parse_args()

    out = args.out_dir
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    args.work_dir.mkdir(parents=True, exist_ok=True)
    db = args.work_dir / "clc.duckdb"
    db.unlink(missing_ok=True)
    con = duckdb.connect(str(db))
    con.execute(f"INSTALL spatial; LOAD spatial; SET temp_directory = '{args.work_dir}'")
    t0 = time.monotonic()

    before = fingerprint(con, args.source)
    assert before[0] == before[1], f"{before[0] - before[1]:,} duplicate IDs in the source"
    print(f"  source: {before[0]:,} polygons", flush=True)

    borders(con, args.gaul)
    assign(con, args.source)
    for how, n in con.execute("SELECT how, count(*) FROM assign GROUP BY 1 ORDER BY 2 DESC").fetchall():
        print(f"  {how:8} {n:>10,}", flush=True)
    print(f"  assigned in {time.monotonic() - t0:.0f} s", flush=True)

    crs = projjson(con)
    xmin, ymin, xmax, ymax = con.execute(f"""
        SELECT min(ST_XMin(Shape)), min(ST_YMin(Shape)), max(ST_XMax(Shape)), max(ST_YMax(Shape))
        FROM read_parquet('{args.source}')""").fetchone()
    cols = ", ".join(f"s.{c}" for c in SOURCE_COLUMNS[1:])
    europe = out / f"{STEM}.parquet"
    write(con, f"""
        SELECT ST_SetCRS(s.Shape, 'EPSG:3035') AS Shape, {cols}, a.country,
               struct_pack(xmin := ST_XMin(s.Shape), ymin := ST_YMin(s.Shape),
                           xmax := ST_XMax(s.Shape), ymax := ST_YMax(s.Shape)) AS bbox
        FROM read_parquet('{args.source}') s JOIN assign a ON a.id = s.ID
        ORDER BY a.country,
                 ST_Hilbert(s.Shape, ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))
    """, europe, crs)
    files = [verify(con, europe)]
    after = fingerprint(con, europe)
    assert after == before, f"the Europe file differs from the source: {after} vs {before}"
    print(f"  {europe.name}: {files[0]['bytes'] / 1e9:.2f} GB, {files[0]['row_groups']} row groups, "
          f"{time.monotonic() - t0:.0f} s", flush=True)

    # The Europe file is already in country, then Hilbert, order: each
    # country file is a slice of it.
    counts = dict(con.execute(f"SELECT country, count(*) FROM read_parquet('{europe}') "
                              "GROUP BY 1 ORDER BY 1").fetchall())
    datasets = [{"path": "", "code": "", "name": "Corine Land Cover 2018 (Europe)"}]
    total = 0
    for code, n in counts.items():
        dest = out / f"country={code}" / f"{STEM}.parquet"
        write(con, f"""SELECT * FROM read_parquet('{europe}') WHERE country = '{code}'
                       ORDER BY ST_Hilbert(Shape, ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})))""",
              dest, crs)
        entry = verify(con, dest)
        assert entry["features"] == n, f"{code}: {entry['features']} rows, expected {n}"
        total += n
        name = country_name(code)
        (dest.parent / "_manifest.json").write_text(json.dumps({
            "country": code, "state_name": name,
            "total_features": n, "themes": {STEM: n}, "files": [entry],
        }, indent=2) + "\n")
        datasets.append({"path": f"country={code}", "code": code, "name": name})
        print(f"  {code} {name:28} {n:>9,}  {entry['bytes'] / 1e6:8.1f} MB", flush=True)
    assert total == before[0], f"country files hold {total:,} rows, the source {before[0]:,}"

    (out / "_manifest.json").write_text(json.dumps({
        "state_name": "Corine Land Cover 2018, Version 2020 20u1 (EEA / Copernicus)",
        "total_features": before[0], "themes": {STEM: before[0]},
        "files": files, "countries": counts,
        "assignment": dict(con.execute("SELECT how, count(*) FROM assign GROUP BY 1 ORDER BY 1").fetchall()),
    }, indent=2) + "\n")
    (out.parent / "index.json").write_text(json.dumps({"datasets": datasets}, indent=2) + "\n")
    gaul_accessed = json.loads(args.gaul_manifest.read_text())["accessed"]
    (out / "ATTRIBUTION.txt").write_text(attribution(
        time.strftime("%d %B %Y", time.strptime(gaul_accessed, "%Y-%m-%d"))))
    print(f"  {len(counts)} countries, done in {time.monotonic() - t0:.0f} s", flush=True)


if __name__ == "__main__":
    sys.exit(main())
