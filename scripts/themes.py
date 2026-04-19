"""
Theme catalog for the OSM -> GeoParquet pipeline.

Each theme declares:
  - name           : output filename stem
  - osmium_filter  : tags-filter expression (see `osmium tags-filter --help`)
  - geometry_types : comma-separated list for `osmium export --geometry-types`
                     (one of: point, linestring, polygon, or combos)
  - typed_columns  : list of (col_name, sql_expr) pairs promoted from `tags`

The SQL expressions run inside DuckDB against a row where `tags` is a
MAP<VARCHAR,VARCHAR>. They must be valid DuckDB SQL.

Edit this file to add/remove themes or change promoted columns. Pipeline
code in pipeline.py stays unchanged.
"""

from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class Theme:
    name: str
    osmium_filter: str
    geometry_types: str
    typed_columns: list[tuple[str, str]]


def _s(key: str) -> str:
    """Helper: tag as VARCHAR."""
    return f"TRY_CAST(tags['{key}'] AS VARCHAR)"


def _i(key: str) -> str:
    """Helper: tag as INT."""
    return f"TRY_CAST(tags['{key}'] AS INT)"


def _d(key: str) -> str:
    """Helper: tag as DOUBLE."""
    return f"TRY_CAST(tags['{key}'] AS DOUBLE)"


THEMES: list[Theme] = [
    Theme(
        name="buildings",
        osmium_filter="wr/building",
        geometry_types="polygon",
        typed_columns=[
            ("building",         _s("building")),
            ("name",             _s("name")),
            ("levels",           _i("building:levels")),
            ("height",           _d("height")),
            ("addr_street",      _s("addr:street")),
            ("addr_housenumber", _s("addr:housenumber")),
            ("addr_postcode",    _s("addr:postcode")),
            ("addr_city",        _s("addr:city")),
            ("operator",         _s("operator")),
            ("start_date",       _s("start_date")),
        ],
    ),
    Theme(
        name="roads",
        osmium_filter="w/highway",
        geometry_types="linestring",
        typed_columns=[
            ("highway",       _s("highway")),
            ("name",          _s("name")),
            ("ref",           _s("ref")),
            ("oneway",        _s("oneway")),
            ("surface",       _s("surface")),
            ("maxspeed",      _s("maxspeed")),
            ("lanes",         _i("lanes")),
            ("bridge",        _s("bridge")),
            ("tunnel",        _s("tunnel")),
            ("access",        _s("access")),
            ("bicycle",       _s("bicycle")),
            ("foot",          _s("foot")),
            ("motor_vehicle", _s("motor_vehicle")),
        ],
    ),
    Theme(
        name="railways",
        osmium_filter="nwr/railway",
        geometry_types="point,linestring",
        typed_columns=[
            ("railway",     _s("railway")),
            ("name",        _s("name")),
            ("ref",         _s("ref")),
            ("operator",    _s("operator")),
            ("gauge",       _i("gauge")),
            ("electrified", _s("electrified")),
            ("usage",       _s("usage")),
            ("service",     _s("service")),
        ],
    ),
    Theme(
        name="waterways",
        osmium_filter="w/waterway",
        geometry_types="linestring",
        typed_columns=[
            ("waterway",     _s("waterway")),
            ("name",         _s("name")),
            ("width",        _d("width")),
            ("intermittent", _s("intermittent")),
            ("tunnel",       _s("tunnel")),
        ],
    ),
    Theme(
        name="water",
        osmium_filter="wr/natural=water wr/water",
        geometry_types="polygon",
        typed_columns=[
            ("water",        _s("water")),
            ("natural",      _s("natural")),
            ("name",         _s("name")),
            ("intermittent", _s("intermittent")),
            ("salt",         _s("salt")),
        ],
    ),
    Theme(
        name="landuse",
        osmium_filter="wr/landuse",
        geometry_types="polygon",
        typed_columns=[
            ("landuse",  _s("landuse")),
            ("name",     _s("name")),
            ("operator", _s("operator")),
        ],
    ),
    Theme(
        name="natural_areas",
        osmium_filter="wr/natural",
        geometry_types="polygon",
        typed_columns=[
            ("natural", _s("natural")),
            ("name",    _s("name")),
            ("wetland", _s("wetland")),
        ],
    ),
    Theme(
        name="natural_features",
        osmium_filter="n/natural",
        geometry_types="point",
        typed_columns=[
            ("natural",    _s("natural")),
            ("name",       _s("name")),
            ("ele",        _d("ele")),
            ("prominence", _d("prominence")),
        ],
    ),
    Theme(
        name="places",
        osmium_filter="n/place",
        geometry_types="point",
        typed_columns=[
            ("place",       _s("place")),
            ("name",        _s("name")),
            ("population",  _i("population")),
            ("admin_level", _i("admin_level")),
            ("capital",     _s("capital")),
            ("is_in",       _s("is_in")),
        ],
    ),
    Theme(
        name="boundaries",
        osmium_filter="r/boundary=administrative",
        geometry_types="polygon",
        typed_columns=[
            ("boundary",    _s("boundary")),
            ("admin_level", _i("admin_level")),
            ("name",        _s("name")),
            ("border_type", _s("border_type")),
            ("iso3166_1",   _s("ISO3166-1")),
            ("iso3166_2",   _s("ISO3166-2")),
        ],
    ),
    Theme(
        name="pois",
        osmium_filter="n/amenity n/shop n/tourism n/leisure n/office n/healthcare",
        geometry_types="point",
        typed_columns=[
            ("amenity",       _s("amenity")),
            ("shop",          _s("shop")),
            ("tourism",       _s("tourism")),
            ("leisure",       _s("leisure")),
            ("office",        _s("office")),
            ("healthcare",    _s("healthcare")),
            ("name",          _s("name")),
            ("brand",         _s("brand")),
            ("operator",      _s("operator")),
            ("website",       _s("website")),
            ("opening_hours", _s("opening_hours")),
            ("phone",         _s("phone")),
            ("cuisine",       _s("cuisine")),
        ],
    ),
    Theme(
        name="amenities_polygons",
        osmium_filter="wr/amenity wr/shop wr/tourism wr/leisure wr/office wr/healthcare",
        geometry_types="polygon",
        typed_columns=[
            ("amenity",       _s("amenity")),
            ("shop",          _s("shop")),
            ("tourism",       _s("tourism")),
            ("leisure",       _s("leisure")),
            ("office",        _s("office")),
            ("healthcare",    _s("healthcare")),
            ("name",          _s("name")),
            ("brand",         _s("brand")),
            ("operator",      _s("operator")),
            ("website",       _s("website")),
            ("opening_hours", _s("opening_hours")),
            ("phone",         _s("phone")),
            ("cuisine",       _s("cuisine")),
        ],
    ),
    Theme(
        name="power",
        osmium_filter="nwr/power",
        geometry_types="point,linestring,polygon",
        typed_columns=[
            ("power",     _s("power")),
            ("name",      _s("name")),
            ("voltage",   _i("voltage")),
            ("frequency", _d("frequency")),
            ("operator",  _s("operator")),
            ("cables",    _i("cables")),
        ],
    ),
    Theme(
        name="aeroways",
        osmium_filter="nwr/aeroway",
        geometry_types="point,linestring,polygon",
        typed_columns=[
            ("aeroway", _s("aeroway")),
            ("name",    _s("name")),
            ("iata",    _s("iata")),
            ("icao",    _s("icao")),
            ("ref",     _s("ref")),
            ("surface", _s("surface")),
        ],
    ),
    Theme(
        name="barriers",
        osmium_filter="nw/barrier",
        geometry_types="point,linestring",
        typed_columns=[
            ("barrier",  _s("barrier")),
            ("name",     _s("name")),
            ("access",   _s("access")),
            ("height",   _d("height")),
            ("material", _s("material")),
        ],
    ),
    Theme(
        name="public_transport",
        osmium_filter=(
            "n/public_transport "
            "n/highway=bus_stop "
            "n/railway=station,halt,tram_stop,subway_entrance"
        ),
        geometry_types="point",
        typed_columns=[
            ("public_transport", _s("public_transport")),
            ("highway",          _s("highway")),
            ("railway",          _s("railway")),
            ("name",             _s("name")),
            ("ref",              _s("ref")),
            ("operator",         _s("operator")),
            ("network",          _s("network")),
            ("bench",            _s("bench")),
            ("shelter",          _s("shelter")),
        ],
    ),
]


# SQL predicates applied AFTER osmium export, before parquet write.
POST_FILTERS: dict[str, str] = {
    # natural_areas excludes water (handled in its own theme)
    "natural_areas": "COALESCE(tags['natural'], '') <> 'water'",
}
