"""Longitude extents that may cross the antimeridian.

A plain min/max over longitudes turns data on both sides of 180 degrees
(Alaska's Aleutians, Guam, American Samoa) into a box around the whole
globe: [-179.2, ..., 179.9, ...]. A reader takes that box for "everywhere",
and the Portolan registry put the OSM North America marker on France, the
box's centre. STAC has a form for this (Collection spec, Spatial Extent):
a box that crosses the antimeridian has west > east, e.g.
[172.4, 14.5, -52.6, 83.3].

This module finds that box from the data. The longitude intervals a file
covers come from its per-row `bbox` column; a row whose box spans more than
180 degrees (a multipolygon with parts on both sides) is split into its
parts first. Intervals closer than GAP_DEG are merged, so a state's islands
collapse into a few intervals that fit in a manifest. The extent is then the
circle minus its largest empty arc: when that arc does not contain 180, the
box crosses it.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

GAP_DEG = 1.0      # intervals closer than this merge into one
WIDE_DEG = 180.0   # a row box wider than this is split into parts


def intervals_sql(src: str, where: str = "") -> str:
    """SQL giving (x0, x1) for every row of `src` (a FROM clause item with
    `bbox` and `geometry` columns), and for every part of a row whose box
    is wider than WIDE_DEG."""
    cond = f"AND ({where})" if where else ""
    return f"""
        SELECT bbox.xmin::DOUBLE AS x0, bbox.xmax::DOUBLE AS x1 FROM {src}
        WHERE bbox.xmax - bbox.xmin <= {WIDE_DEG} {cond}
        UNION ALL
        SELECT ST_XMin(p), ST_XMax(p) FROM (
            SELECT unnest(ST_Dump(geometry::GEOMETRY)).geom AS p FROM {src}
            WHERE bbox.xmax - bbox.xmin > {WIDE_DEG} {cond})"""


def merge(intervals: Iterable[Sequence[float]], gap: float = GAP_DEG) -> list[list[float]]:
    """Sorted, merged [x0, x1] intervals, joining those closer than `gap`.
    An interval wider than WIDE_DEG (a part that itself crosses 180) is
    taken as the whole circle."""
    out: list[list[float]] = []
    for x0, x1 in sorted((float(a), float(b)) for a, b in intervals):
        if x1 - x0 > WIDE_DEG:
            return [[-180.0, 180.0]]
        if out and x0 <= out[-1][1] + gap:
            out[-1][1] = max(out[-1][1], x1)
        else:
            out.append([x0, x1])
    return out


def lon_intervals(con, src: str, where: str = "") -> list[list[float]]:
    """The merged longitude intervals a GeoParquet source covers. The
    distinct rows are reduced in SQL to a few thousand at most (rounded
    outward to 1e-4 degree) before merging here."""
    rows = con.execute(f"""
        SELECT floor(x0 * 1e4) / 1e4, ceil(x1 * 1e4) / 1e4
        FROM ({intervals_sql(src, where)}) GROUP BY ALL""").fetchall()
    return merge(rows)


def wrapped_bbox(intervals: Iterable[Sequence[float]], ymin: float, ymax: float) -> list[float]:
    """[west, south, east, north] around the merged intervals: the circle
    minus its largest empty arc. west > east when that arc does not
    contain the antimeridian."""
    iv = merge(intervals)
    if not iv:
        raise ValueError("no longitudes")
    # Gaps between neighbours, and the one across 180 (last end to first start).
    across = (iv[0][0] + 360.0) - iv[-1][1]
    best, at = across, None
    for i in range(len(iv) - 1):
        g = iv[i + 1][0] - iv[i][1]
        if g > best:
            best, at = g, i
    # A float32 bbox rounded outward can overshoot the antimeridian by a
    # few 1e-4: a STAC bbox stays within WGS84.
    lon = lambda x: max(-180.0, min(180.0, x))  # noqa: E731
    lat = lambda y: max(-90.0, min(90.0, y))  # noqa: E731
    if at is None:
        return [lon(iv[0][0]), lat(ymin), lon(iv[-1][1]), lat(ymax)]
    return [lon(iv[at + 1][0]), lat(ymin), lon(iv[at][1]), lat(ymax)]


def bbox_from_parts(boxes: Iterable[Sequence[float]]) -> list[float]:
    """The wrapped union of several boxes (catalog-level aggregation). A box
    given with west > east is read as crossing 180."""
    intervals, ys0, ys1 = [], [], []
    for b in boxes:
        x0, y0, x1, y1 = b[0], b[1], b[2], b[3]
        intervals += [[x0, 180.0], [-180.0, x1]] if x0 > x1 else [[x0, x1]]
        ys0.append(y0)
        ys1.append(y1)
    return wrapped_bbox(intervals, min(ys0), max(ys1))
