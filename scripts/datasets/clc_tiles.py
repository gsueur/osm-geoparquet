#!/usr/bin/env python3
"""
Corine Land Cover 2018 on H3 cells and as one PMTiles archive for Europe.

CLC cannot be tiled in one pass. Its 2.4 million polygons tile the land
with no gaps, and below zoom 9 a tile holds tens of thousands of them: past
the size cap the encoder drops up to 89% of a tile's features (measured on
France). Simplifying does not reduce the count, and dissolving by class
does not either, since CLC never has two neighbours of the same class. The
small scales need aggregation, which no vector tiler does.

So, as Fields of the World does with its cells, the archive hands over at
zoom 9 between two layers:

  cells     z0-8   H3 cells, one resolution per zoom band (BANDS), a few
                   pixels wide: the dominant CLC class, its share, the share
                   of each CLC group, and how much of the cell CLC covers
  clc_2018  z9-12  the CLC polygons, reprojected to lon/lat

The cells come from one sample: every H3 resolution 9 cell (about 0.1 km2)
takes the class of the polygon holding its centre. The polygons are not
simplified first: simplified one by one, neighbours overlap and 0.13% of the
cells land in two classes (measured on France). Each coarser cell then counts its resolution 9 children, so a
share is a share of area (children of one parent are near-equal in area).
The cells of every resolution are also published as clc_2018_h3.parquet, a
table to join land cover onto anything indexed by H3.

Every feature of both layers carries `Code_18`, and `color` (the official
hex) and `label` (the class name) from meta/clc_legend.json, so any client
can draw the CLC legend with ["get", "color"] alone. pmtiles.tile() fails a
band where a tile would lose features; pmtiles.pyramid() joins the bands.
Everything runs in DuckDB, with the h3 community extension.

Usage:
  python3 scripts/datasets/clc_tiles.py --source clc_2018.parquet \\
      --out-dir out/2018 --work-dir work
  # writes out/2018/clc_2018.pmtiles and out/2018/clc_2018_h3.parquet,
  # and adds both to out/2018/_manifest.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import pmtiles
from datasets.clc import STEM

REPO = Path(__file__).resolve().parent.parent.parent
LEGEND = REPO / "meta" / "clc_legend.json"
LAYER = STEM
CELLS_LAYER = "cells"
CELLS_FILE = f"{STEM}_h3.parquet"
SAMPLE_RES = 9
POLYGON_PROPERTIES = ["Code_18", "color", "label"]
GROUPS = {"artificial": "1", "agricultural": "2", "forest": "3", "wetland": "4", "water": "5"}
CELL_PROPERTIES = ["h3", "Code_18", "color", "label", "dominant_pct",
                   *(f"{g}_pct" for g in GROUPS), "coverage_pct"]

# (min zoom, max zoom, H3 resolution). A cell is 3 to 7 pixels wide at
# 45-50 degrees north across its band, small enough to read as land cover
# and large enough that a tile holds a few thousand. None: the polygons.
BANDS = [
    (0, 2, 3),
    (3, 3, 4),
    (4, 5, 5),
    (6, 6, 6),
    (7, 8, 7),
    (9, 12, None),
]


def legend_table(con) -> None:
    classes = json.loads(LEGEND.read_text())["classes"]
    con.execute("CREATE OR REPLACE TEMP TABLE legend (code VARCHAR, color VARCHAR, label VARCHAR)")
    con.executemany("INSERT INTO legend VALUES (?, ?, ?)",
                    [(c["code"], c["color"], c["label"]) for c in classes])


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def polygons(con, source: Path, dest: Path) -> int:
    """The CLC polygons in lon/lat with the legend columns, for z9-12."""
    con.execute(f"""
        COPY (SELECT s.Code_18, l.color, l.label,
                     ST_Transform(s.Shape, 'EPSG:3035', 'OGC:CRS84') AS geometry
              FROM read_parquet('{source}') s JOIN legend l ON l.code = s.Code_18)
        TO '{dest}' (FORMAT parquet, COMPRESSION zstd)""")
    return con.execute(f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]


def sample(con, polys: Path, dest: Path) -> int:
    """Every resolution 9 cell whose centre is in a CLC polygon, with its
    class. CLC is a partition, so each cell comes once."""
    con.execute(f"""
        COPY (SELECT unnest(h3_polygon_wkb_to_cells(ST_AsWKB(part), {SAMPLE_RES})) AS cell,
                     Code_18
              FROM (SELECT unnest(ST_Dump(geometry)).geom AS part, Code_18
                    FROM read_parquet('{polys}')))
        TO '{dest}' (FORMAT parquet, COMPRESSION zstd)""")
    n, distinct = con.execute(f"SELECT count(*), count(DISTINCT cell) "
                              f"FROM read_parquet('{dest}')").fetchone()
    if n != distinct:
        sys.exit(f"{n - distinct:,} resolution {SAMPLE_RES} cells sampled twice: "
                 f"CLC polygons overlap?")
    return n


def aggregate(con, samples: Path, dest: Path) -> dict[int, int]:
    """One row per cell of every band resolution: dominant class and shares.
    Written sorted by resolution then cell, so a read of one resolution
    touches its own row groups."""
    resolutions = [r for _, _, r in BANDS if r is not None]
    groups = ",\n".join(
        f"round(100.0 * count(*) FILTER (WHERE Code_18 LIKE '{prefix}%') / any_value(total))"
        f"::UTINYINT AS {name}_pct" for name, prefix in GROUPS.items())
    con.execute(f"""
        COPY (
          WITH p AS (
            SELECT r AS h3_res, h3_cell_to_parent(cell, r) AS parent, Code_18
            FROM read_parquet('{samples}'), (SELECT unnest({resolutions}) AS r)),
          c AS (SELECT h3_res, parent, Code_18, count(*) AS n FROM p GROUP BY ALL),
          t AS (SELECT h3_res, parent, sum(n) AS total, arg_max(Code_18, n) AS dominant,
                       max(n) AS top FROM c GROUP BY ALL),
          s AS (SELECT p.h3_res, p.parent, {groups}
                FROM p JOIN t USING (h3_res, parent) GROUP BY ALL)
          SELECT h3_h3_to_string(t.parent) AS h3, t.h3_res, t.dominant AS Code_18,
                 l.color, l.label,
                 round(100.0 * t.top / t.total)::UTINYINT AS dominant_pct,
                 {", ".join(f"s.{g}_pct" for g in GROUPS)},
                 least(100, round(100.0 * t.total / pow(7, {SAMPLE_RES} - t.h3_res)))::UTINYINT
                     AS coverage_pct,
                 ST_GeomFromWKB(h3_cell_to_boundary_wkb(t.parent)) AS geometry
          FROM t JOIN s USING (h3_res, parent) JOIN legend l ON l.code = t.dominant
          ORDER BY t.h3_res, t.parent)
        TO '{dest}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)""")
    return dict(con.execute(f"SELECT h3_res, count(*) FROM read_parquet('{dest}') "
                            f"GROUP BY 1 ORDER BY 1").fetchall())


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--source", type=Path, required=True, help="the published clc_2018.parquet")
    p.add_argument("--out-dir", type=Path, required=True,
                   help="the release folder holding _manifest.json, e.g. out/2018")
    p.add_argument("--work-dir", type=Path, default=Path("work"))
    args = p.parse_args()

    work = args.work_dir / "tiles"
    work.mkdir(parents=True, exist_ok=True)
    manifest_path = args.out_dir / "_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    t0 = time.monotonic()

    def done(what: str) -> None:
        print(f"  {what}, {time.monotonic() - t0:.0f} s", flush=True)

    con = duckdb.connect(str(work / "tiles.duckdb"))
    con.execute(f"INSTALL spatial; LOAD spatial; INSTALL h3 FROM community; LOAD h3; "
                f"SET temp_directory = '{work}'; SET geometry_always_xy = true")
    legend_table(con)
    unknown = con.execute(f"""SELECT DISTINCT Code_18 FROM read_parquet('{args.source}')
                              WHERE Code_18 NOT IN (SELECT code FROM legend)""").fetchall()
    if unknown:
        sys.exit(f"Code_18 values missing from {LEGEND.name}: {sorted(r[0] for r in unknown)}")

    polys = work / "polygons.parquet"
    done(f"{polygons(con, args.source, polys):,} polygons in lon/lat")
    samples = work / "h3_r9.parquet"
    done(f"{sample(con, polys, samples):,} resolution {SAMPLE_RES} cells sampled")
    cells = args.out_dir / CELLS_FILE
    counts = aggregate(con, samples, cells)
    done("cells " + ", ".join(f"r{r}: {n:,}" for r, n in counts.items()))

    archives = []
    for lo, hi, res in BANDS:
        band = work / f"z{lo}-{hi}.pmtiles"
        if res is None:
            pmtiles.tile(polys, band, layer=LAYER, min_zoom=lo, max_zoom=hi,
                         properties=POLYGON_PROPERTIES)
            archives.append((lo, hi, band, LAYER))
        else:
            src = work / f"cells_r{res}.parquet"
            con.execute(f"COPY (SELECT * EXCLUDE (h3_res) FROM read_parquet('{cells}') "
                        f"WHERE h3_res = {res}) TO '{src}' (FORMAT parquet)")
            # Hexagons are drawn as they are: simplifying would only bend them.
            pmtiles.tile(src, band, layer=CELLS_LAYER, min_zoom=lo, max_zoom=hi,
                         properties=CELL_PROPERTIES, verbatim=True)
            archives.append((lo, hi, band, CELLS_LAYER))
        done(f"z{lo}-{hi} tiled")
    con.close()

    dest = args.out_dir / f"{STEM}.pmtiles"
    entry = pmtiles.pyramid(archives, dest)
    entry["layers"] = [CELLS_LAYER, LAYER]
    entry["bands"] = [{"min_zoom": lo, "max_zoom": hi, "layer": layer,
                       "h3_res": res} for (lo, hi, res), (*_, layer) in zip(BANDS, archives)]
    entry["properties"] = {CELLS_LAYER: CELL_PROPERTIES, LAYER: POLYGON_PROPERTIES}
    manifest["pmtiles"] = [{"name": STEM, **entry}]
    manifest["h3"] = {"file": CELLS_FILE, "sample_res": SAMPLE_RES, "cells": counts,
                      "bytes": cells.stat().st_size, "sha256": sha256(cells)}
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    done(f"{dest.name}: {entry['tiles']:,} tiles, {entry['bytes'] / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
