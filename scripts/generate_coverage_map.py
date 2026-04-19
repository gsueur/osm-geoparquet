#!/usr/bin/env python3
"""
Generate site/public/coverage.svg from data/admin_regions.geojson.

- Clips geometries to western-hemisphere northern (drops Pacific US
  territories like Guam / Am Samoa / N Marianas / USVI issues).
- Simplifies at ~0.1 degrees (good for a continental-scale static map).
- Emits an SVG where each covered region is a <path data-iso="...">
  so CSS / JS can style "covered" vs "uncovered" separately.

Run once (or whenever admin_regions.geojson changes); commit the SVG.
"""

from __future__ import annotations
import argparse
from pathlib import Path

import duckdb

# Western-hemisphere, northern-ish bounding box so Guam / Samoa / etc.
# don't explode the viewBox. Covers mainland Alaska, Hawaii, PR.
CLIP_BBOX = "ST_MakeEnvelope(-180, 5, -50, 85)"

VIEWBOX_X_MIN = -170
VIEWBOX_Y_MIN = -85   # SVG Y is negated latitude
VIEWBOX_WIDTH = 115   # -170 to -55
VIEWBOX_HEIGHT = 72   # covers ~-85 (lat 85) to ~-13 (lat 13)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--geojson", default="data/admin_regions.geojson")
    ap.add_argument("--out", default="site/public/coverage.svg")
    ap.add_argument("--tolerance", type=float, default=0.08,
                    help="ST_Simplify tolerance in degrees")
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    rows = con.execute(f"""
        SELECT
            "ISO3166-2" AS iso,
            name,
            ST_AsSVG(
                ST_Simplify(
                    ST_Intersection(geom, {CLIP_BBOX}),
                    {args.tolerance}
                ),
                false,
                2
            ) AS d
        FROM ST_Read('{args.geojson}')
        WHERE "ISO3166-2" IS NOT NULL
          AND "ISO3166-2" NOT IN ('US-AS', 'US-GU', 'US-MP', 'US-UM')
          AND ST_Intersects(geom, {CLIP_BBOX})
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
  viewBox="{VIEWBOX_X_MIN} {VIEWBOX_Y_MIN} {VIEWBOX_WIDTH} {VIEWBOX_HEIGHT}"
  role="img" aria-label="Coverage map of North America"
  preserveAspectRatio="xMidYMid meet">
<style>
  path {{ fill: var(--covered-fill, #4fd1a3); stroke: var(--covered-stroke, rgba(0,0,0,0.35)); stroke-width: 0.1; }}
</style>
{chr(10).join(paths)}
</svg>
"""
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(svg)
    print(f"wrote {out} ({len(svg):,} bytes, {len(paths)} regions)")


if __name__ == "__main__":
    main()
