#!/usr/bin/env python3
"""
Convert FAO GAUL 2024 to GeoParquet for the parquetry bucket.

GAUL is the UN FAO's Global Administrative Unit Layers: subnational
administrative units for every country, consistent with the UN delineation of
international boundaries. FAO ships it as ESRI shapefiles, which nothing can
query over HTTP. This writes the same features as GeoParquet with a bbox
covering, so a reader can prune row groups instead of downloading 775 MB.

  L1  3,110 units    first-level  (provinces, states, regions)
  L2  45,524 units   second-level (districts, departments)
  L0  countries      DERIVED HERE, see below

Each layer is written twice: once as a single whole-world file, for anyone
who wants one download, and once split per country under
country=<iso3_code>/, for readers that want a small file (the map viewer,
a bbox query on one country). DuckDB will not write row groups under 2,048
rows, so a whole-world L1 is two groups of ~185 MB and L0 is one group of
286 MB; a bbox filter cannot skip anything, and a browser has to fetch the
whole group. The per-country files are the answer to that.

FAO does not publish an L0. The GAUL 2024 package stops at L1, and a country
layer is a boundary statement rather than a statistical convenience, so its
absence looks deliberate. The Terms of Use permit derivative works (para 4,
CC BY 4.0), so L0 is dissolved from L1 here and labelled as ours: the file
carries a `derived_by` column, the manifest says so, and ATTRIBUTION.txt
repeats it. What it must never do is read as an FAO product, because para 8
forbids representing FAO as having approved or endorsed our use.

Terms of Use obligations this script encodes, from GAUL2024TermsOfUse.pdf
shipped inside each source ZIP:

  para 7   the citation is a fixed string including the access date
  para 8   no suggestion that FAO sponsored, approved or endorsed this
  para 9   the UN disclaimer on legal status and frontiers travels with it
  para 6   GAUL may contain third-party data that cannot be redistributed,
           and determining that is the redistributor's responsibility

Usage:
  python3 scripts/datasets/gaul.py --work-dir <dir with the unpacked .shp>
  python3 scripts/datasets/gaul.py --work-dir <dir> --out-dir out/gaul
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

import duckdb

VERSION = "2024"
ACCESS_URL = "https://data.apps.fao.org/?lang=en"
SOURCE_ZIPS = {
    "L1": "https://storage.googleapis.com/fao-maps-catalog-data/boundaries/GAUL_2024_L1.zip",
    "L2": "https://storage.googleapis.com/fao-maps-catalog-data/boundaries/GAUL_2024_L2.zip",
}

# The attribute columns FAO documents, in their order. Carried through
# untouched: renaming them would break joins against FAO statistics, which is
# what the codes exist for.
L1_COLUMNS = ["iso3_code", "map_code", "gaul0_code", "gaul0_name",
              "gaul1_code", "gaul1_name", "continent", "disp_en"]
L2_COLUMNS = L1_COLUMNS[:6] + ["gaul2_code", "gaul2_name", "continent", "disp_en"]


def attribution(accessed: dt.date) -> str:
    """ATTRIBUTION.txt for the dataset prefix.

    The citation in the first block is quoted from para 7 of the Terms of Use
    and is not ours to reword. The rest states what we changed, because a
    reader who finds these files without the catalog still has to be able to
    tell the FAO layers from the one we dissolved.
    """
    return f"""\
# Data attribution

This dataset is the Global Administrative Unit Layers (GAUL) 2024, developed
and owned by the Food and Agriculture Organization of the United Nations
(FAO), redistributed here in GeoParquet form.

## Citation (required by the GAUL 2024 Terms of Use, para 7)

  FAO. 2024. Global Administrative Unit Layers (GAUL).
  [Accessed on {accessed.strftime('%d %B %Y')}]. {ACCESS_URL}.
  Licence: CC-BY-4.0

## Licence

Creative Commons Attribution 4.0 International (CC BY 4.0).
  https://creativecommons.org/licenses/by/4.0/

The GAUL 2024 Terms of Use, shipped as GAUL2024TermsOfUse.pdf inside the
source archives, apply in addition and prevail where they conflict with CC
BY 4.0. Some third-party data included in GAUL may carry terms other than
FAO's own (Terms of Use, para 6); anyone redistributing it further is
responsible for checking that.

## What was changed here

The L1 and L2 layers are the FAO source features, unmodified in geometry and
attributes. They were converted from ESRI shapefile to GeoParquet, sorted on
a Hilbert curve, and given a `bbox` column so readers can prune row groups.

The L0 layer is NOT an FAO product. FAO does not publish a GAUL 2024 country
layer. This one was derived by geomermaids by dissolving the L1 units of each
country, and carries a `derived_by` column recording that. Use it as a
convenience, not as an authority.

## Disclaimers

FAO has not participated in, sponsored, approved or endorsed this
redistribution (Terms of Use, para 8).

The designations employed and the presentation of material in the GAUL
dataset do not imply the expression of any opinion whatsoever on the part of
FAO concerning the legal status of any country, territory, city or area or of
its authorities, or concerning the delimitation of its frontiers or
boundaries (Terms of Use, para 9).

GAUL 2024 should not be considered an authoritative or official
representation of subnational boundaries. Its primary purpose is to support
the representation of subnational statistics and attributes.

## Source

  {SOURCE_ZIPS['L1']}
  {SOURCE_ZIPS['L2']}

Technical guidelines: Franceschini, G., Khan, A., Moretti, L., Nyabuti, K.,
Asif, M., Bezuidenhoudt, E. and Morteo, K. 2025. The Global Administrative
Unit Layers (GAUL) 2024. Technical guidelines. Rome, FAO.
https://doi.org/10.4060/cd4262en
"""


def geo_metadata(extent: tuple, geom_types: list[str]) -> str:
    """The `geo` footer value, covering included.

    Written by hand through KV_METADATA for the same reason pipeline.py does
    it: `covering` is not in GeoParquet 2.0 yet, so DuckDB's writer will not
    emit one, and GEOPARQUET_VERSION 'V2' would add a second `geo` key beside
    ours. 'NONE' governs the metadata only; the geometry column is still
    written with the native GEOMETRY logical type.
    """
    xmin, ymin, xmax, ymax = extent
    return json.dumps({
        "version": "2.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": sorted(geom_types),
                "bbox": [xmin, ymin, xmax, ymax],
                "covering": {"bbox": {
                    "xmin": ["bbox", "xmin"], "ymin": ["bbox", "ymin"],
                    "xmax": ["bbox", "xmax"], "ymax": ["bbox", "ymax"],
                }},
            }
        },
    }).replace("'", "''")


# A bbox that is an outer bound after being narrowed to FLOAT: round each
# value away from the centre. float32 has ~7 decimal digits, so the epsilon is
# far larger than the rounding error and still negligible against a polygon
# measured in degrees. Same expression as pipeline.py, kept identical on
# purpose so both datasets prune the same way.
BBOX_STRUCT = """struct_pack(
                    xmin := (ST_XMin(geom) - abs(ST_XMin(geom)) * 1e-6 - 1e-9)::FLOAT,
                    ymin := (ST_YMin(geom) - abs(ST_YMin(geom)) * 1e-6 - 1e-9)::FLOAT,
                    xmax := (ST_XMax(geom) + abs(ST_XMax(geom)) * 1e-6 + 1e-9)::FLOAT,
                    ymax := (ST_YMax(geom) + abs(ST_YMax(geom)) * 1e-6 + 1e-9)::FLOAT
                ) AS bbox"""


# DuckDB does not use ROW_GROUP_SIZE literally. Measured on a 45,524-row
# file: asking for 256, 1,000 or 2,048 all give 23 groups of ~1,979 rows;
# 3,000 and 5,000 give 12 of ~3,794; 20,000 gives 3. It snaps the request to a
# power-of-two multiple of 1,024, and 2,048 rows is the floor. So asking for
# LESS than 2,048 is how you get the smallest groups, and asking for exactly
# 2,048 rounds up to 4,096 and gives you larger ones. ROW_GROUP_SIZE_BYTES
# does not override any of it.
#
# The floor is what limits these layers. L1 has 3,110 features, so 2 groups is
# the best available; L0 has 272 and lands in one whatever is asked. Only L2
# has enough rows for the bbox covering to prune usefully. Partitioning by
# country is the fix if that becomes a problem, not a smaller row group.
SMALLEST_GROUP_REQUEST = 256


def row_group_rows(feature_count: int, source_bytes: int) -> int:
    """Rows per group, aimed at ~48 MB of geometry rather than a fixed count.

    pipeline.py's flat 50,000 suits OSM, whose features are small. A GAUL L1
    unit averages 145 KB, so 50,000 rows would put the whole layer in one
    group and a bbox filter would have nothing to skip.
    """
    avg = max(source_bytes / max(feature_count, 1), 1)
    return max(SMALLEST_GROUP_REQUEST, min(20_000, int(48 * 1024 * 1024 / avg)))


def write_layer(con, name: str, select_sql: str, source: str, out_dir: Path,
                feature_count: int, source_bytes: int, *,
                out: Path | None = None, quiet: bool = False) -> dict:
    con.execute(f"CREATE OR REPLACE TEMP VIEW layer AS {select_sql}")
    count, xmin, ymin, xmax, ymax, types = con.execute("""
        SELECT COUNT(*), MIN(ST_XMin(geom)), MIN(ST_YMin(geom)),
               MAX(ST_XMax(geom)), MAX(ST_YMax(geom)),
               list(DISTINCT ST_GeometryType(geom)::VARCHAR)
        FROM layer
    """).fetchone()
    if count == 0:
        sys.exit(f"{name}: no features, refusing to write an empty layer")

    out = out or out_dir / f"{name}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    cols = [c for c in con.execute("DESCRIBE layer").fetchall() if c[0] != "geom"]
    attrs = ",\n                ".join(c[0] for c in cols)
    rows = row_group_rows(feature_count, source_bytes)
    box = f"ST_Extent(ST_MakeEnvelope({xmin!r}, {ymin!r}, {xmax!r}, {ymax!r}))"
    # Bound to a local before the f-string below: interpolating the function's
    # own name would quietly write its repr into the footer.
    geo = geo_metadata((xmin, ymin, xmax, ymax), types)

    con.execute(f"""
        COPY (
            SELECT
                {attrs},
                {BBOX_STRUCT},
                geom AS geometry
            FROM layer
            ORDER BY ST_Hilbert(geom, {box})
        ) TO '{out}' (
            FORMAT PARQUET,
            GEOPARQUET_VERSION 'NONE',
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL 15,
            ROW_GROUP_SIZE {rows},
            KV_METADATA {{geo: '{geo}'}}
        )
    """)

    size = out.stat().st_size
    groups, largest = con.execute(f"""
        SELECT count(*), max(b) FROM (
            SELECT row_group_id, sum(total_compressed_size) AS b
            FROM parquet_metadata('{out}') GROUP BY 1)
    """).fetchone()
    note = "" if groups > 4 else "   <- too few rows to prune well"
    if not quiet:
        print(f"  {name}: {count:,} features, {size/1e6:.1f} MB, "
              f"{groups} row group(s), largest {largest/1e6:.1f} MB{note}")
    return {"name": name, "features": count, "bytes": size,
            "row_groups": groups, "largest_row_group_bytes": largest,
            "sha256": hashlib.sha256(out.read_bytes()).hexdigest(),
            "source": source}


PARTITION_KEY = "country"       # the Hive key in the path
PARTITION_COLUMN = "iso3_code"  # the column it takes its values from


def write_partitions(con, name: str, table: str, source: str, out_dir: Path) -> dict:
    """The same layer as one file per country, country=<iso3_code>/<name>.parquet.

    iso3_code is the readable choice for a Hive key, with two quirks a reader
    should know: FAO uses pseudo-codes for disputed areas (xAB Abyei, xJK
    Jammu and Kashmir, xxx), and a few codes group several GAUL units (AUS
    also holds Ashmore and Cartier Islands, xFR the scattered French islands).
    gaul0_code is unique but opaque, and stays inside the files as a column.
    """
    keys = [r[0] for r in con.execute(
        f"SELECT DISTINCT {PARTITION_COLUMN} FROM {table} ORDER BY 1").fetchall()]
    bad = [k for k in keys if not k or "/" in k or k != k.strip()]
    if bad:
        sys.exit(f"{name}: {PARTITION_COLUMN} values unfit for a path: {bad}")
    entries = []
    for key in keys:
        names = [r[0] for r in con.execute(
            f"SELECT DISTINCT gaul0_name FROM {table} WHERE {PARTITION_COLUMN} = ? ORDER BY 1",
            [key]).fetchall()]
        count = con.execute(
            f"SELECT count(*) FROM {table} WHERE {PARTITION_COLUMN} = ?", [key]).fetchone()[0]
        e = write_layer(
            con, name,
            f"SELECT * FROM {table} WHERE {PARTITION_COLUMN} = '{key}'",
            source, out_dir, feature_count=count, source_bytes=0,
            out=out_dir / f"{PARTITION_KEY}={key}" / f"{name}.parquet", quiet=True)
        entries.append({"country": key, "names": names, "features": e["features"],
                        "bytes": e["bytes"], "row_groups": e["row_groups"],
                        "sha256": e["sha256"]})
    total = sum(e["bytes"] for e in entries)
    print(f"  {name}: {len(entries)} per-country files, {total/1e6:.1f} MB, "
          f"largest {max(e['bytes'] for e in entries)/1e6:.1f} MB")
    return {"name": name, "key": PARTITION_KEY, "column": PARTITION_COLUMN,
            "files": len(entries), "bytes": total, "entries": entries}


def verify(con, out_dir: Path, names: list[str]) -> None:
    """Refuse to ship a file whose footer or bbox is wrong.

    The footer is the part that fails silently. An earlier version of this
    script interpolated the name of the `geo_metadata` function instead of
    calling it, and wrote `<function geo_metadata at 0x...>` into the footer
    three builds running: every file was the right size, held the right
    features and read back fine, because nothing reads `geo` unless asked.
    """
    problems = []
    for name in names:
        f = out_dir / f"{name}.parquet"
        if len(problems) >= 20:
            problems.append("... stopping after 20 problems")
            break
        kv = con.execute(
            f"SELECT key, value FROM parquet_kv_metadata('{f}')").fetchall()
        keys = [k.decode() for k, _ in kv]
        if keys.count("geo") != 1:
            problems.append(f"{name}: {keys.count('geo')} 'geo' footer keys, want exactly 1")
            continue
        try:
            geo = json.loads(next(v for k, v in kv if k == b"geo").decode())
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            problems.append(f"{name}: 'geo' is not JSON ({exc})")
            continue
        covering = geo["columns"]["geometry"].get("covering", {}).get("bbox")
        if covering != {"xmin": ["bbox", "xmin"], "ymin": ["bbox", "ymin"],
                        "xmax": ["bbox", "xmax"], "ymax": ["bbox", "ymax"]}:
            problems.append(f"{name}: covering missing or malformed: {covering!r}")

        # A bbox that is not an outer bound would silently drop rows from
        # every pruned query, which is worse than having no bbox at all.
        outside = con.execute(f"""
            SELECT count(*) FROM read_parquet('{f}')
            WHERE bbox.xmin > ST_XMin(geometry) OR bbox.ymin > ST_YMin(geometry)
               OR bbox.xmax < ST_XMax(geometry) OR bbox.ymax < ST_YMax(geometry)
        """).fetchone()[0]
        if outside:
            problems.append(f"{name}: {outside} geometries fall outside their bbox")

        declared = [round(v, 6) for v in geo["columns"]["geometry"]["bbox"]]
        actual = [round(v, 6) for v in con.execute(f"""
            SELECT MIN(ST_XMin(geometry)), MIN(ST_YMin(geometry)),
                   MAX(ST_XMax(geometry)), MAX(ST_YMax(geometry))
            FROM read_parquet('{f}')""").fetchone()]
        if declared != actual:
            problems.append(f"{name}: declared bbox {declared} != actual {actual}")

    if problems:
        sys.exit("verification failed:\n  " + "\n  ".join(problems))
    print(f"  verified {len(names)} files: one geo key, covering, bbox bounds, extent")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--work-dir", type=Path, required=True,
                   help="Directory holding the unpacked GAUL_2024_L*.shp")
    p.add_argument("--out-dir", type=Path, default=Path("out/gaul") / VERSION)
    p.add_argument("--accessed", default=None,
                   help="Access date for the required citation (YYYY-MM-DD, "
                        "default: today UTC)")
    args = p.parse_args()

    accessed = (dt.date.fromisoformat(args.accessed) if args.accessed
                else dt.datetime.now(dt.timezone.utc).date())
    shp = {lvl: args.work_dir / f"GAUL_2024_{lvl}.shp" for lvl in ("L1", "L2")}
    for lvl, path in shp.items():
        if not path.is_file():
            sys.exit(f"{path} not found. Unpack {SOURCE_ZIPS[lvl]} into --work-dir.")

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    print(f"GAUL {VERSION} -> {args.out_dir}")

    # Each layer is materialised once, then written twice: the whole-world
    # file, and one file per country from the same rows.
    layers, partitions = [], []
    for lvl, columns in (("L1", L1_COLUMNS), ("L2", L2_COLUMNS)):
        cols = ", ".join(columns)
        con.execute(f"CREATE TEMP TABLE src_{lvl} AS "
                    f"SELECT {cols}, geom FROM ST_Read('{shp[lvl].as_posix()}')")
        count = con.execute(f"SELECT count(*) FROM src_{lvl}").fetchone()[0]
        layers.append(write_layer(
            con, lvl, f"SELECT * FROM src_{lvl}", SOURCE_ZIPS[lvl], args.out_dir,
            feature_count=count, source_bytes=shp[lvl].stat().st_size))
        partitions.append(write_partitions(con, lvl, f"src_{lvl}", SOURCE_ZIPS[lvl],
                                           args.out_dir))

    # L0, ours. ST_Union_Agg on the L1 units of each country: the shared
    # internal borders cancel and the country outline is what is left. Nothing
    # below level 0 survives, so only the country-level codes are carried.
    print("  L0: dissolving L1 by country (derived, not an FAO layer)")
    con.execute("""CREATE TEMP TABLE src_L0_derived AS
        SELECT iso3_code, map_code, gaul0_code, gaul0_name, continent,
               'geomermaids, dissolved from GAUL 2024 L1' AS derived_by,
               ST_Union_Agg(geom) AS geom
        FROM src_L1
        GROUP BY ALL""")
    l0_source = "derived from " + SOURCE_ZIPS["L1"]
    layers.append(write_layer(
        con, "L0_derived", "SELECT * FROM src_L0_derived", l0_source, args.out_dir,
        feature_count=con.execute("SELECT count(*) FROM src_L0_derived").fetchone()[0],
        source_bytes=shp["L1"].stat().st_size))
    partitions.append(write_partitions(con, "L0_derived", "src_L0_derived", l0_source,
                                       args.out_dir))

    verify(con, args.out_dir,
           [l["name"] for l in layers]
           + [f"{p['key']}={e['country']}/{p['name']}"
              for p in partitions for e in p["entries"]])

    (args.out_dir / "ATTRIBUTION.txt").write_text(attribution(accessed))
    (args.out_dir / "_manifest.json").write_text(json.dumps({
        "state_name": f"FAO Global Administrative Unit Layers (GAUL) {VERSION}",
        "total_features": sum(l["features"] for l in layers),
        "themes": {l["name"]: l["features"] for l in layers},
        "accessed": accessed.isoformat(),
        "licence": "CC-BY-4.0",
        "files": layers,
        "partitions": partitions,
    }, indent=2) + "\n")
    print(f"  ATTRIBUTION.txt, _manifest.json  (accessed {accessed.isoformat()})")


if __name__ == "__main__":
    main()
