#!/usr/bin/env python3
"""
US Census Bureau TIGER/Line national GeoPackages as GeoParquet and PMTiles.

The Census Bureau publishes TIGER/Line as national GeoPackages
(https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-geopackage-file.html),
one zipped file per product, several layers per file. This writes one
GeoParquet file per layer, under the layer's TIGER code, and its PMTiles
archive:

  <out>/tiger/<year>/<product>/<code>.parquet   e.g. tiger/2026/legislative/cd120.parquet
  <out>/tiger/<year>/<product>/<code>.pmtiles
  <out>/tiger/<year>/<product>/_manifest.json   rows, sizes, sha256, labels, source
  <out>/tiger/<year>/<product>/ATTRIBUTION.txt

PRODUCTS says which products and layers; adding one is a table entry.

Columns and values are TIGER's, unchanged (no renaming: joins with other
Census tables keep working), including the per-state ZZZ rows that stand
for areas where no district is defined. Two changes: the geometry is 2D
(the GeoPackages declare Z and M and hold neither), and each row gets a
`bbox` declared as the GeoParquet covering. The CRS stays NAD83
(EPSG:4269), geographic like lon/lat. Rows are sorted on a Hilbert curve and
cut into row groups of at most 4 MiB of geometry (gaul.py's rewrite), so a
bbox filter reads only the groups around it.

The tiles treat NAD83 as lon/lat, which is within 2 m of WGS84 in the
conterminous US: tylertoo reads lon/lat or Web Mercator only.

Usage:
  python3 scripts/datasets/census.py upstream                # Last-Modified of each source
  python3 scripts/datasets/census.py build --work-dir work --out-dir out
  python3 scripts/datasets/census.py build --gpkg legislative=~/Downloads/tlgpkg_2026_us_legislative.gpkg ...
"""

from __future__ import annotations

import argparse
import hashlib
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
from datasets.gaul import GROUP_BYTES, rewrite, verify
from datasets.infra import GEOMETRY_TYPE_NAMES

YEAR = "2026"
SOURCE_PAGE = ("https://www.census.gov/geographies/mapping-files/time-series/geo/"
               "tiger-geopackage-file.html")
SOURCE_DIR = f"https://www2.census.gov/geo/tiger/TGRGPKG{YEAR[2:]}"
LAYOUTS = (f"https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp{YEAR}/"
           f"{YEAR}_TIGERLINE_GPKG_Record_Layouts.pdf")
CRS = "EPSG:4269"

# product -> its GeoPackage and layers. Each layer: (layer name in the
# GeoPackage, TIGER code, title, the column naming the area, PMTiles zooms,
# properties kept in the tiles).
PRODUCTS = {
    "legislative": {
        "title": "Legislative Areas",
        "gpkg": f"tlgpkg_{YEAR}_us_legislative.gpkg",
        "layers": [
            ("Congressional Districts", "cd120",
             "Congressional Districts (120th Congress)", "CD120FP", (0, 12),
             ["STATEFP", "CD120FP", "GEOID", "NAMELSAD", "ALAND", "AWATER"]),
            ("State Legislative Districts - Upper", "sldu",
             "State Legislative Districts, Upper Chamber", "SLDUST", (0, 12),
             ["STATEFP", "SLDUST", "GEOID", "NAMELSAD", "ALAND", "AWATER"]),
            ("State Legislative Districts - Lower", "sldl",
             "State Legislative Districts, Lower Chamber", "SLDLST", (0, 12),
             ["STATEFP", "SLDLST", "GEOID", "NAMELSAD", "ALAND", "AWATER"]),
        ],
    },
}

# State FIPS -> (USPS code, name), for the per-state counts the page draws.
STATES = {
    "01": ("AL", "Alabama"), "02": ("AK", "Alaska"), "04": ("AZ", "Arizona"),
    "05": ("AR", "Arkansas"), "06": ("CA", "California"), "08": ("CO", "Colorado"),
    "09": ("CT", "Connecticut"), "10": ("DE", "Delaware"), "11": ("DC", "District of Columbia"),
    "12": ("FL", "Florida"), "13": ("GA", "Georgia"), "15": ("HI", "Hawaii"),
    "16": ("ID", "Idaho"), "17": ("IL", "Illinois"), "18": ("IN", "Indiana"),
    "19": ("IA", "Iowa"), "20": ("KS", "Kansas"), "21": ("KY", "Kentucky"),
    "22": ("LA", "Louisiana"), "23": ("ME", "Maine"), "24": ("MD", "Maryland"),
    "25": ("MA", "Massachusetts"), "26": ("MI", "Michigan"), "27": ("MN", "Minnesota"),
    "28": ("MS", "Mississippi"), "29": ("MO", "Missouri"), "30": ("MT", "Montana"),
    "31": ("NE", "Nebraska"), "32": ("NV", "Nevada"), "33": ("NH", "New Hampshire"),
    "34": ("NJ", "New Jersey"), "35": ("NM", "New Mexico"), "36": ("NY", "New York"),
    "37": ("NC", "North Carolina"), "38": ("ND", "North Dakota"), "39": ("OH", "Ohio"),
    "40": ("OK", "Oklahoma"), "41": ("OR", "Oregon"), "42": ("PA", "Pennsylvania"),
    "44": ("RI", "Rhode Island"), "45": ("SC", "South Carolina"), "46": ("SD", "South Dakota"),
    "47": ("TN", "Tennessee"), "48": ("TX", "Texas"), "49": ("UT", "Utah"),
    "50": ("VT", "Vermont"), "51": ("VA", "Virginia"), "53": ("WA", "Washington"),
    "54": ("WV", "West Virginia"), "55": ("WI", "Wisconsin"), "56": ("WY", "Wyoming"),
    "60": ("AS", "American Samoa"), "66": ("GU", "Guam"),
    "69": ("MP", "Northern Mariana Islands"), "72": ("PR", "Puerto Rico"),
    "78": ("VI", "U.S. Virgin Islands"),
}

BBOX = """struct_pack(
            xmin := (ST_XMin(geometry) - abs(ST_XMin(geometry)) * 1e-6 - 1e-9)::FLOAT,
            ymin := (ST_YMin(geometry) - abs(ST_YMin(geometry)) * 1e-6 - 1e-9)::FLOAT,
            xmax := (ST_XMax(geometry) + abs(ST_XMax(geometry)) * 1e-6 + 1e-9)::FLOAT,
            ymax := (ST_YMax(geometry) + abs(ST_YMax(geometry)) * 1e-6 + 1e-9)::FLOAT
        ) AS bbox"""


def zip_url(product: str) -> str:
    return f"{SOURCE_DIR}/{PRODUCTS[product]['gpkg']}.zip"


def upstream() -> dict:
    """Last-Modified and size of each product's zip, to tell a rebuild."""
    out = {}
    for product in PRODUCTS:
        req = urllib.request.Request(zip_url(product), method="HEAD")
        with urllib.request.urlopen(req, timeout=60) as r:
            out[product] = {"url": zip_url(product),
                            "last_modified": r.headers["Last-Modified"],
                            "bytes": int(r.headers["Content-Length"])}
    return out


def fetch(product: str, work: Path) -> Path:
    gpkg = work / PRODUCTS[product]["gpkg"]
    if gpkg.is_file():
        return gpkg
    archive = work / f"{gpkg.name}.zip"
    subprocess.run(["curl", "-fsSL", "--retry", "3", "-o", str(archive), zip_url(product)],
                   check=True)
    with zipfile.ZipFile(archive) as z:
        z.extract(gpkg.name, work)
    archive.unlink()
    return gpkg


def projjson(con) -> dict:
    """NAD83 as PROJJSON for the `geo` footer, from a probe file DuckDB writes."""
    probe = Path(con.execute("SELECT current_setting('temp_directory')").fetchone()[0]) / "crs.parquet"
    con.execute(f"COPY (SELECT ST_SetCRS(ST_Point(0, 0), '{CRS}') AS g) TO '{probe}' "
                "(FORMAT PARQUET, GEOPARQUET_VERSION 'V2')")
    geo = con.execute(f"SELECT decode(value) FROM parquet_kv_metadata('{probe}') "
                      "WHERE key = 'geo'").fetchone()[0]
    probe.unlink()
    crs = json.loads(geo)["columns"]["g"]["crs"]
    assert crs["id"] == {"authority": "EPSG", "code": 4269}, crs.get("id")
    return crs


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_layer(con, gpkg: Path, layer: str, dest: Path, crs: dict) -> dict:
    """One layer to GeoParquet 2.0: TIGER's columns, 2D NAD83 geometry,
    bbox covering, Hilbert order, then row groups capped by bytes."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE src AS
        SELECT *, {BBOX} FROM (
            SELECT * EXCLUDE (shape), ST_SetCRS(ST_Force2D(shape), '{CRS}') AS geometry
            FROM ST_Read('{gpkg}', layer = '{layer}'))""")
    xmin, ymin, xmax, ymax, types, n = con.execute("""
        SELECT min(ST_XMin(geometry)), min(ST_YMin(geometry)), max(ST_XMax(geometry)),
               max(ST_YMax(geometry)), list(DISTINCT ST_GeometryType(geometry)::VARCHAR), count(*)
        FROM src""").fetchone()
    geo = json.dumps({
        "version": "2.0.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB",
            "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in types),
            "crs": crs,
            "bbox": [xmin, ymin, xmax, ymax],
            "covering": {"bbox": {k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}},
        }},
    }).replace("'", "''")
    raw = dest.with_suffix(".raw.parquet")
    con.execute(f"""
        COPY (SELECT * FROM src ORDER BY ST_Hilbert(geometry,
                  ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))))
        TO '{raw}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE', COMPRESSION ZSTD,
                    KV_METADATA {{geo: '{geo}'}})""")
    rewrite(raw, dest)
    raw.unlink()
    groups, largest = con.execute(f"""
        SELECT count(*), max(b) FROM (SELECT row_group_id, sum(total_compressed_size) AS b
        FROM parquet_metadata('{dest}') GROUP BY 1)""").fetchone()
    return {"file": dest.name, "features": n, "bytes": dest.stat().st_size,
            "row_groups": groups, "largest_row_group_bytes": largest, "sha256": sha256(dest)}


def by_state(con, path: Path, key: str) -> dict[str, int]:
    """Districts per state, without TIGER's placeholders: ZZ / ZZZ (no
    district defined) and, for Congress, 98 (the non-voting delegates)."""
    rows = con.execute(f"""SELECT STATEFP, count(*) FROM read_parquet('{path}')
                           WHERE {key} NOT LIKE 'ZZ%' AND {key} <> '98'
                           GROUP BY 1 ORDER BY 1""").fetchall()
    unknown = [s for s, _ in rows if s not in STATES]
    if unknown:
        sys.exit(f"{path.name}: state FIPS not in STATES: {unknown}")
    return dict(rows)


def state_bounds(con, path: Path) -> dict[str, list[float]]:
    """Each state's box, from its districts. A state across the antimeridian
    (Alaska's Aleutians) is boxed on the side where most of it lies: its
    parts east of 180 are shifted by -360."""
    rows = con.execute(f"""
        WITH parts AS (
            SELECT STATEFP, unnest(ST_Dump(geometry)).geom AS g FROM read_parquet('{path}')),
        boxes AS (
            SELECT STATEFP, ST_XMin(g) AS x0, ST_YMin(g) AS y0, ST_XMax(g) AS x1, ST_YMax(g) AS y1
            FROM parts),
        span AS (SELECT STATEFP, max(x1) - min(x0) > 180 AS wraps FROM boxes GROUP BY 1)
        SELECT b.STATEFP,
               min(CASE WHEN s.wraps AND b.x0 > 0 THEN b.x0 - 360 ELSE b.x0 END),
               min(b.y0),
               max(CASE WHEN s.wraps AND b.x1 > 0 THEN b.x1 - 360 ELSE b.x1 END),
               max(b.y1)
        FROM boxes b JOIN span s USING (STATEFP) GROUP BY 1 ORDER BY 1""").fetchall()
    return {r[0]: [round(v, 3) for v in r[1:]] for r in rows}


def check_crs(con, path: Path) -> None:
    """gaul.verify checks footer, covering, bbox and groups; this the CRS,
    which must survive the pyarrow rewrite in both places."""
    geo = json.loads(con.execute(f"SELECT decode(value) FROM parquet_kv_metadata('{path}') "
                                 "WHERE key = 'geo'").fetchone()[0])
    code = geo["columns"]["geometry"]["crs"]["id"]["code"]
    typ = con.execute(f"SELECT any_value(typeof(geometry)) FROM read_parquet('{path}')").fetchone()[0]
    if code != 4269 or "4269" not in typ:
        sys.exit(f"{path}: CRS lost (footer {code}, type {typ})")


COLORS = 6  # the page's district palette


def color_index(con, parquet: Path) -> dict[str, int]:
    """A colour per district such that no two neighbours share one: greedy
    colouring, most-connected first, over the districts that touch.
    Neighbours are found on geometries simplified to ~100 m, which is
    enough to tell who borders whom and keeps the join quick."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE simple AS
        SELECT GEOID, ST_SimplifyPreserveTopology(geometry::GEOMETRY, 0.001) AS g, bbox
        FROM read_parquet('{parquet}')""")
    pairs = con.execute("""
        SELECT a.GEOID, b.GEOID FROM simple a JOIN simple b
          ON a.GEOID < b.GEOID
         AND a.bbox.xmin <= b.bbox.xmax AND b.bbox.xmin <= a.bbox.xmax
         AND a.bbox.ymin <= b.bbox.ymax AND b.bbox.ymin <= a.bbox.ymax
        WHERE ST_DWithin(a.g, b.g, 0.002)""").fetchall()
    ids = [r[0] for r in con.execute("SELECT GEOID FROM simple").fetchall()]
    near = {i: set() for i in ids}
    for a, b in pairs:
        near[a].add(b)
        near[b].add(a)
    colour: dict[str, int] = {}
    for i in sorted(ids, key=lambda i: -len(near[i])):
        used = {colour[j] for j in near[i] if j in colour}
        colour[i] = next(c for c in range(len(ids)) if c not in used)
    clashes = sum(1 for a, b in pairs if colour[a] == colour[b])
    over = sum(1 for c in colour.values() if c >= COLORS)
    if clashes or over > len(ids) // 100:
        sys.exit(f"{parquet.name}: colouring needs more than {COLORS} colours "
                 f"({over} districts over, {clashes} clashes)")
    # A rare district that needed a 7th colour takes the least used among
    # its neighbours' complement, accepting one clash at most.
    for i, c in colour.items():
        if c >= COLORS:
            counts = [sum(1 for j in near[i] if colour[j] == k) for k in range(COLORS)]
            colour[i] = counts.index(min(counts))
    return colour


def tiles(con, parquet: Path, code: str, zooms: tuple[int, int], properties: list[str]) -> dict:
    """The layer's PMTiles. tylertoo reads lon/lat or Web Mercator, so the
    tiler gets a copy whose geometry is relabelled CRS84 (NAD83 is within
    2 m of it), with `color_index` added: the tiles only, the GeoParquet
    stays TIGER's."""
    colours = color_index(con, parquet)
    con.execute("CREATE OR REPLACE TEMP TABLE colours (GEOID VARCHAR, color_index UTINYINT)")
    con.executemany("INSERT INTO colours VALUES (?, ?)", list(colours.items()))
    lonlat = parquet.with_suffix(".lonlat.parquet")
    con.execute(f"""COPY (SELECT {', '.join(f'p.{c}' for c in properties)}, c.color_index,
                                 ST_SetCRS(p.geometry::GEOMETRY, 'OGC:CRS84') AS geometry
                          FROM read_parquet('{parquet}') p JOIN colours c USING (GEOID))
                    TO '{lonlat}' (FORMAT PARQUET)""")
    properties = [*properties, "color_index"]
    entry = pmtiles.tile(lonlat, parquet.with_suffix(".pmtiles"), layer=code,
                         min_zoom=zooms[0], max_zoom=zooms[1], properties=properties)
    lonlat.unlink()
    return entry


def attribution(product: str, source: dict) -> str:
    p = PRODUCTS[product]
    return f"""\
# Data attribution

This data is the {YEAR} TIGER/Line {p['title']} National GeoPackage, produced
by the U.S. Census Bureau from its MAF/TIGER system.

  Source:     {source['url']}
              (Last-Modified {source['last_modified']})
  Page:       {SOURCE_PAGE}
  Layouts:    {LAYOUTS}
  Producer:   U.S. Census Bureau, Geography Division (geo.geography@census.gov)
  Status:     a work of the United States Government, not subject to copyright
              in the United States (17 U.S.C. 105)

Credit the U.S. Census Bureau, TIGER/Line {YEAR}, when you use it.

## Legal disclaimer (from the Census Bureau, record layouts section 2)

No warranty, expressed or implied, is made regarding the accuracy of the data
in the TIGER/Line GeoPackages, and no liability is assumed by the United
States Government in general, or the Census Bureau specifically, as to the
positional or attribute accuracy of the data. The boundary information in the
TIGER/Line GeoPackages is for statistical data collection and tabulation
purposes only. Their depiction and designation for statistical purposes does
not constitute a determination of jurisdictional authority or rights of
ownership or entitlement and they are not legal land descriptions.

## What was changed here

One GeoParquet 2.0 file per GeoPackage layer, named by its TIGER code
({', '.join(code for _, code, *_ in p['layers'])}). Columns and values are the
Census Bureau's, unchanged. The geometry is 2D (the GeoPackage declares Z and M
and holds neither), in NAD83 (EPSG:4269) as published, with a `bbox` column
declared as the GeoParquet covering, rows sorted along a Hilbert curve. The
PMTiles archives beside them are derived for web maps.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def build(work: Path, out: Path, local: dict[str, Path]) -> None:
    work.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"INSTALL spatial; LOAD spatial; SET temp_directory = '{work}'")
    crs = projjson(con)
    source = upstream()
    t0 = time.monotonic()
    for product, spec in PRODUCTS.items():
        gpkg = local.get(product) or fetch(product, work)
        folder = out / "tiger" / YEAR / product
        folder.mkdir(parents=True, exist_ok=True)
        files, tile_entries, labels, themes = [], [], {}, {}
        for layer, code, title, key, zooms, props in spec["layers"]:
            dest = folder / f"{code}.parquet"
            entry = write_layer(con, gpkg, layer, dest, crs)
            verify(con, folder, [code])
            check_crs(con, dest)
            files.append({"name": code, "layer": layer, "key": key, **entry,
                          "by_state": by_state(con, dest, key)})
            themes[code], labels[code] = entry["features"], title
            tile_entries.append({"name": code, **tiles(con, dest, code, zooms, props)})
            print(f"  {product}/{code}: {entry['features']:,} rows, {entry['bytes'] / 1e6:.1f} MB, "
                  f"{entry['row_groups']} row groups, {time.monotonic() - t0:.0f} s", flush=True)
        bounds = state_bounds(con, folder / f"{spec['layers'][0][1]}.parquet")
        (folder / "ATTRIBUTION.txt").write_text(attribution(product, source[product]))
        (folder / "_manifest.json").write_text(json.dumps({
            "state_name": f"TIGER/Line {YEAR}: {spec['title']}",
            "total_features": sum(themes.values()),
            "themes": themes,
            "labels": labels,
            "crs": CRS,
            "source": source[product],
            "files": files,
            "pmtiles": tile_entries,
            "states": {fips: {"code": c, "name": n, "bounds": bounds.get(fips)}
                       for fips, (c, n) in STATES.items()},
        }, indent=2) + "\n")
    # The parquetry repository files at the dataset root.
    (out / "index.json").write_text(json.dumps({"datasets": [
        {"path": f"tiger/{YEAR}/{product}", "code": product,
         "name": f"TIGER/Line {YEAR}: {spec['title']}"} for product, spec in PRODUCTS.items()
    ]}, indent=2) + "\n")
    (out / "snapshots.json").write_text(json.dumps({"latest": "", "snapshots": []}) + "\n")
    (out / "_source.json").write_text(json.dumps(source, indent=2) + "\n")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("upstream", help="print each source's Last-Modified")
    b = sub.add_parser("build", help="fetch, convert, tile and verify")
    b.add_argument("--work-dir", type=Path, default=Path("work"))
    b.add_argument("--out-dir", type=Path, required=True,
                   help="the census/ prefix, e.g. out/census")
    b.add_argument("--gpkg", action="append", default=[], metavar="PRODUCT=PATH",
                   help="use a GeoPackage already on disk")
    args = p.parse_args()
    if args.cmd == "upstream":
        print(json.dumps(upstream(), indent=2))
        return
    local = {}
    for spec in args.gpkg:
        product, path = spec.split("=", 1)
        if product not in PRODUCTS:
            sys.exit(f"unknown product {product!r}")
        local[product] = Path(path).expanduser()
    build(args.work_dir, args.out_dir, local)


if __name__ == "__main__":
    main()
