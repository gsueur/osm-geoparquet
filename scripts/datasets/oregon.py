#!/usr/bin/env python3
"""
Oregon's statewide vector layers, from the Oregon GEOHub, as GeoParquet and
PMTiles. (oregon.py, not or.py: `or` is a Python keyword. The bucket prefix
is or/.)

The Oregon GEOHub (https://geohub.oregon.gov) lists the layers the state's
agencies publish as ArcGIS services: address points, ODOT's public roads,
highway network and mileposts, Water Resources' water rights and wells,
Fish and Wildlife's fish habitat distribution and passage barriers,
DOGAMI's faults, landslides and liquefaction hazard, urban growth
boundaries, statewide zoning, the public land survey, fire stations and
others. This reads each layer from its query endpoint and writes one
GeoParquet file per layer, and its PMTiles archive, grouped by theme:

  <out>/<theme>/<layer>.parquet     e.g. cadastre/address_points.parquet
  <out>/<theme>/<layer>.pmtiles
  <out>/<theme>/<layer>.png         the catalog thumbnail
  <out>/<theme>/_manifest.json      rows, sizes, sha256, titles, sources, tile settings
  <out>/<theme>/ATTRIBUTION.txt

oregon_layers.csv says which layers: one row per source layer, its theme,
its server (a key of SERVERS: the agency's ArcGIS Online organisation or
server), the service and layer number. Rows sharing an id are one published
layer, each source told apart by its `category`: the water rights' points
of diversion and places of use by source and use (75 layers, 2 files), the
fish habitat distribution by species (136 layers: streams and lakes), the
liquefaction hazard by class.

Not published: copies of national datasets published elsewhere (NHD, 3DHP,
the Watershed Boundary Dataset, GNIS, Census, NWI, wilderness areas, USFS
boundaries), statewide building footprints (USA Structures covers them, as
for New York), rasters, layers covering less than a fifth of the state's
extent, indexes of studies and maps, and the yearly urban growth boundaries
(the history layer has them). The GEOHub's terms allow any lawful use of its
public data, commercial use included. PLAN-or-layers.csv, outside the
repository, kept the reasons.

Every layer is in NAD83(HARN) / Oregon GIC Lambert (ft) (EPSG:2992), the
state's standard: the server projects the others as they are read. Columns
and values are the agencies', unchanged.

The fetch, the fingerprints, the parallel build and the per-layer publish
are arcgis_layers.py's, which maine.py, nh.py, ny.py, ca.py and wa.py share;
writing, tiling and thumbnails are massgis.py's.

Usage:
  python3 scripts/datasets/oregon.py upstream
  uv run --project scripts --with matplotlib python scripts/datasets/oregon.py build \\
      --work-dir /Volumes/T9/or/tmp --out-dir /Volumes/T9/or/out [--layer address_points]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import arcgis_layers  # noqa: E402

PORTAL = "https://geohub.oregon.gov"


def server(base: str, kind: str, publisher: str) -> dict:
    return {"url": base + "/rest/services/{service}/" + kind + "/{layer}",
            "kind": "map service" if kind == "MapServer" else "feature service",
            "publisher": publisher, "undated": "changes"}


STATE = "State of Oregon"
PARKS = "Oregon Parks and Recreation Department"

# Where a CSV row's layer is read: its `server` column. A layer without edit
# dates is rebuilt when its count, highest object id or fields move.
SERVERS = {
    '': server('https://services8.arcgis.com/8PAo5HGmvRMlF2eU/arcgis', 'FeatureServer',
           'State of Oregon (Geospatial Enterprise Office)'),
    'blm': server('https://services1.arcgis.com/KbxwQRRfWyEYLgp4/arcgis', 'FeatureServer',
           'Bureau of Land Management, Oregon State Office'),
    'dlcd': server('https://gis.lcd.state.or.us/server', 'FeatureServer',
           'Oregon Department of Land Conservation and Development'),
    'dogami': server('https://gis.dogami.oregon.gov/arcgis', 'MapServer',
           'Oregon Department of Geology and Mineral Industries'),
    'dsl': server('https://maps.dsl.state.or.us/arcgis', 'FeatureServer',
           'Oregon Department of State Lands'),
    'navigator': server('https://navigator.state.or.us/arcgis', 'MapServer', STATE),
    'oda': server('https://maps.oda.oregon.gov/arcgis', 'MapServer',
           'Oregon Department of Agriculture'),
    'odfw': server('https://nrimp.dfw.state.or.us/arcgis', 'FeatureServer',
           'Oregon Department of Fish and Wildlife'),
    'odot': server('https://gis.odot.state.or.us/arcgis1006', 'MapServer',
           'Oregon Department of Transportation'),
    'oregon_ago': server('https://services.arcgis.com/uUvqNMGPm7axC2dD/arcgis', 'FeatureServer', STATE),
    'oregon_ago2': server('https://services.arcgis.com/uUvqNMGPm7axC2dD/ArcGIS', 'FeatureServer', STATE),
    'owrd': server('https://gis.wrd.state.or.us/server', 'FeatureServer',
           'Oregon Water Resources Department'),
    'prd': server('https://maps.prd.state.or.us/arcgis', 'FeatureServer', PARKS),
    'prd2': server('https://gis.prd.state.or.us/arcgis', 'FeatureServer', PARKS),
}
PUBLIC = "https://parquetry.geomermaids.com/or"
LAYERS_CSV = Path(__file__).with_name("oregon_layers.csv")
CRS = "EPSG:2992"
OUTLINE = ("boundaries", "state_boundary")
CREDIT = "Oregon GEOHub and the agency each layer names"

THEMES = {
    "boundaries": "Boundaries, Districts and Planning",
    "cadastre": "Addresses, Public Land Survey and State Lands",
    "transport": "Transportation",
    "water": "Water and Water Rights",
    "fish_wildlife": "Fish and Wildlife",
    "geology_hazards": "Geology and Geologic Hazards",
    "land_resources": "Land, Agriculture and Parks",
    "coast": "Coast and Shorelines",
    "public_safety": "Public Safety and Wildfire",
    "community": "Communities and Facilities",
}
COLORS = {
    "boundaries": "#8e44ad", "cadastre": "#7f6a4f", "transport": "#d35400",
    "water": "#2f7fc1", "fish_wildlife": "#3f8a4a", "geology_hazards": "#9a6b3a",
    "land_resources": "#6a8e23", "coast": "#1b8a8a", "public_safety": "#c0392b",
    "community": "#b03a2e",
}


def theme_title(theme: str) -> str:
    return f"Oregon: {THEMES[theme]}"


def attribution(theme: str, entries: list[dict]) -> str:
    lines = "\n".join(f"  {e['name']:<44} {e['source']['urls'][0]}" for e in entries)
    return f"""\
# Data attribution

These layers are published by Oregon's state agencies through the Oregon
GEOHub:

  Portal:     {PORTAL}
  Producers:  the agency each layer names (Transportation, Water Resources,
              Fish and Wildlife, Geology and Mineral Industries, Land
              Conservation and Development, Agriculture, State Lands, Parks
              and Recreation, the Geospatial Enterprise Office and others)

{THEMES[theme]}: each layer and the service layer it is read from (the
agency for each one is in _manifest.json):

{lines}

Terms: credit the agency a layer names. The GEOHub's terms of use allow any
lawful use of its public data, commercial use included; the agencies provide
their layers as is, without warranty, for information and not for legal,
engineering or survey use.

## What was changed here

One GeoParquet 2.0 file per layer. Folded into one file each, a `category`
column telling the source layers apart: the water rights' points of
diversion and places of use (source and use), the fish habitat distribution
(species; streams and lakes apart) and the liquefaction hazard (class).
Columns and values are the agencies', unchanged; features without a
geometry are left out (counted in _manifest.json). The geometry is 2D, in
NAD83(HARN) / Oregon GIC Lambert (ft) (EPSG:2992), the server projecting the
layers published in another CRS, with a `bbox` column declared as the
GeoParquet covering, rows sorted along a Hilbert curve. The PMTiles archives
beside them are derived for web maps, in lon/lat.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


PROFILE = arcgis_layers.Profile(
    name="or", public=PUBLIC, layers_csv=LAYERS_CSV, servers=SERVERS, themes=THEMES,
    colors=COLORS, outline=OUTLINE, outline_crs=CRS, theme_title=theme_title,
    attribution=attribution, crs=CRS)


def layers() -> list[dict]:
    return arcgis_layers.layers(PROFILE)


if __name__ == "__main__":
    arcgis_layers.main(PROFILE, __doc__)
