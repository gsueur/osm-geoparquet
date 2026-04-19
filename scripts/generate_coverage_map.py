#!/usr/bin/env python3
"""
Generate site/public/coverage.svg from data/admin_regions.geojson.

- Clips geometries to western-hemisphere northern so far-flung Pacific
  territories don't blow up the viewBox.
- Reprojects to NA Albers Equal Area (ESRI:102008). Areas look right;
  Canada is no longer absurdly wide and Mexico doesn't get stretched.
- Simplifies at ~10 km (a continental-scale tolerance).
- Emits one <path data-iso="..."> per region so CSS / JS can style
  "covered" vs "uncovered" separately later.

Run once (or whenever admin_regions.geojson changes); commit the SVG.
"""

from __future__ import annotations
import argparse
from pathlib import Path

import duckdb

# Geographic (lat/lng) clip before projecting. Drops US-GU/AS/MP and US-UM
# which are in the Pacific and would otherwise skew the projection.
CLIP_BBOX_WGS84 = "ST_MakeEnvelope(-180, 5, -50, 85)"

# NA Albers Equal Area Conic (ESRI:102008). Standard parallels 20° & 60°,
# centered on -96° lon, 40° lat. Covers all of North America without
# gross distortion.
TARGET_CRS = "ESRI:102008"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--geojson", default="data/admin_regions.geojson")
    ap.add_argument("--out", default="site/public/coverage.svg")
    ap.add_argument("--tolerance-m", type=float, default=20000,
                    help="ST_Simplify tolerance in metres (Albers units)")
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Materialize projected & simplified geometries once, then take
    # both the extent and each feature's SVG path from the same table.
    con.execute(f"""
        CREATE TEMP TABLE projected AS
        SELECT
            "ISO3166-2" AS iso,
            name,
            ST_Simplify(
                ST_Transform(
                    ST_Intersection(geom, {CLIP_BBOX_WGS84}),
                    'EPSG:4326', '{TARGET_CRS}', true
                ),
                {args.tolerance_m}
            ) AS g
        FROM ST_Read('{args.geojson}')
        WHERE "ISO3166-2" IS NOT NULL
          AND "ISO3166-2" NOT IN ('US-AS', 'US-GU', 'US-MP', 'US-UM')
          AND ST_Intersects(geom, {CLIP_BBOX_WGS84})
    """)

    xmin, xmax, ymin, ymax = con.execute("""
        SELECT MIN(ST_XMin(g)), MAX(ST_XMax(g)),
               MIN(ST_YMin(g)), MAX(ST_YMax(g))
        FROM projected WHERE g IS NOT NULL
    """).fetchone()

    # Pad a touch and round to 10 km so the numbers are cleaner.
    pad = 50000
    vb_x = int((xmin - pad) // 10000) * 10000
    vb_y = int((-ymax - pad) // 10000) * 10000  # SVG Y is negated
    vb_w = int((xmax - xmin + 2 * pad) // 10000) * 10000
    vb_h = int((ymax - ymin + 2 * pad) // 10000) * 10000

    rows = con.execute("""
        SELECT iso, name, ST_AsSVG(g, false, 0) AS d
        FROM projected
        WHERE g IS NOT NULL
        ORDER BY iso
    """).fetchall()

    paths: list[str] = []
    for iso, name, d in rows:
        if not d:
            continue
        country = iso.split("-")[0]
        safe_name = (name or "").replace('"', "&quot;")
        paths.append(
            f'<path data-iso="{iso}" data-country="{country}" '
            f'data-name="{safe_name}" d="{d}"/>'
        )

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg"
  viewBox="{vb_x} {vb_y} {vb_w} {vb_h}"
  role="img" aria-label="Coverage map of North America (NA Albers Equal Area)"
  preserveAspectRatio="xMidYMid meet">
<style>
  path {{ fill: var(--covered-fill, #4fd1a3); stroke: var(--covered-stroke, rgba(0,0,0,0.35)); stroke-width: 2000; stroke-linejoin: round; }}
</style>
{chr(10).join(paths)}
</svg>
"""
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(svg)
    print(f"wrote {out} ({len(svg):,} bytes, {len(paths)} regions)")
    print(f"  projection: {TARGET_CRS}")
    print(f"  viewBox:    {vb_x} {vb_y} {vb_w} {vb_h}")


if __name__ == "__main__":
    main()
