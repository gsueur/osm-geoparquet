#!/usr/bin/env python3
"""
Render catalog/thumbnails/<theme>.png, the preview image every catalog
collection carries (PORTO-CORE-067).

The images are committed and only need re-rendering when a theme is added or
its selection changes, so this runs by hand rather than nightly. It reads one
region straight from the bucket, draws it with flat styling by geometry type
over the region outline, and frames it at 3:2 (600 x 400).

  uv run --project scripts --with matplotlib python scripts/thumbnails.py
  uv run --project scripts --with matplotlib python scripts/thumbnails.py \
      --region US-DE --themes buildings roads
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import duckdb
import shapely

sys.path.insert(0, str(Path(__file__).resolve().parent))
from themes import THEMES  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "catalog" / "thumbnails"
BASE = "https://parquetry.geomermaids.com/osm"

BACKGROUND = "#f7f5f0"
REGION_FILL = "#ebe7de"
REGION_EDGE = "#b9b2a3"
COLORS = {
    "buildings": "#b5553c", "roads": "#4a4a4a", "railways": "#6b3fa0",
    "waterways": "#2f7fc1", "water": "#3b8fd4", "landuse": "#7a9a3a",
    "natural_areas": "#3f8a4a", "natural_features": "#5f7d2e", "places": "#c0392b",
    "boundaries": "#8e44ad", "pois": "#d35400", "amenities_polygons": "#e08a2c",
    "power": "#c9a100", "aeroways": "#34495e", "barriers": "#7f6a52",
    "public_transport": "#16a085",
}


def frame(xmin: float, ymin: float, xmax: float, ymax: float,
          ratio: float = 1.5, pad: float = 0.04) -> tuple[float, float, float, float, float]:
    """Pad a lon/lat box to a 3:2 screen shape. Returns the box and the
    y/x aspect that keeps shapes undistorted at that latitude."""
    aspect = 1 / math.cos(math.radians((ymin + ymax) / 2))
    w, h = (xmax - xmin), (ymax - ymin) * aspect
    w, h = w * (1 + pad), h * (1 + pad)
    if w / h < ratio:
        w = h * ratio
    else:
        h = w / ratio
    cx, cy = (xmin + xmax) / 2, (ymin + ymax) / 2
    return cx - w / 2, cy - h / aspect / 2, cx + w / 2, cy + h / aspect / 2, aspect


def region_outline(con, iso: str, geojson: Path):
    if not geojson.is_file():
        return None
    row = con.execute(
        "SELECT ST_AsWKB(geom) FROM ST_Read(?) WHERE \"ISO3166-2\" = ?",
        [str(geojson), iso]).fetchone()
    return shapely.from_wkb(bytes(row[0])) if row else None


def draw(ax, geoms, color: str, tolerance: float) -> None:
    from matplotlib.collections import LineCollection, PolyCollection

    parts = shapely.get_parts(geoms)
    kinds = shapely.get_type_id(parts)
    polys = parts[kinds == 3]
    lines = parts[kinds == 1]
    points = parts[kinds == 0]
    if len(polys):
        rings = shapely.get_exterior_ring(shapely.simplify(polys, tolerance))
        ax.add_collection(PolyCollection(
            [shapely.get_coordinates(r) for r in rings],
            facecolors=color, edgecolors=color, linewidths=0.15, alpha=0.75))
    if len(lines):
        ax.add_collection(LineCollection(
            [shapely.get_coordinates(g) for g in shapely.simplify(lines, tolerance)],
            colors=color, linewidths=0.35, alpha=0.85))
    if len(points):
        xy = shapely.get_coordinates(points)
        ax.scatter(xy[:, 0], xy[:, 1], s=1.2, c=color, linewidths=0, alpha=0.8)


def render(con, theme: str, region: str, date: str, outline) -> Path:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    country = region.split("-")[0]
    url = f"{BASE}/{date}/country={country}/state={region}/{theme}.parquet"
    rows = con.execute(
        f"SELECT ST_AsWKB(geometry) FROM read_parquet('{url}', hive_partitioning = false)"
    ).fetchall()
    geoms = shapely.from_wkb([bytes(r[0]) for r in rows])
    box = shapely.bounds(outline) if outline is not None else shapely.total_bounds(geoms)
    x0, y0, x1, y1, aspect = frame(*box)

    fig = plt.figure(figsize=(6, 4), dpi=100)
    ax = fig.add_axes((0, 0, 1, 1))
    ax.set_facecolor(BACKGROUND)
    fig.patch.set_facecolor(BACKGROUND)
    if outline is not None:
        from matplotlib.collections import PolyCollection
        rings = shapely.get_exterior_ring(shapely.get_parts(outline))
        ax.add_collection(PolyCollection(
            [shapely.get_coordinates(r) for r in rings],
            facecolors=REGION_FILL, edgecolors=REGION_EDGE, linewidths=0.6))
    draw(ax, geoms, COLORS[theme], tolerance=(x1 - x0) / 1200)
    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.set_aspect(aspect)
    ax.axis("off")

    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"{theme}.png"
    fig.savefig(path, dpi=100, facecolor=BACKGROUND)
    plt.close(fig)
    print(f"  {theme:20} {len(geoms):>9,} features -> {path.relative_to(REPO)}")
    return path


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--region", default="US-RI", help="ISO 3166-2 region to preview")
    p.add_argument("--date", default="latest", help="snapshot date or 'latest'")
    p.add_argument("--themes", nargs="*", help="subset (default: all)")
    p.add_argument("--admin-geojson", type=Path, default=REPO / "data" / "admin_regions.geojson",
                   help="region outline for context; skipped when absent")
    args = p.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    outline = region_outline(con, args.region, args.admin_geojson)
    for t in THEMES:
        if args.themes and t.name not in args.themes:
            continue
        render(con, t.name, args.region, args.date, outline)


if __name__ == "__main__":
    main()
