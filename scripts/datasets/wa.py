#!/usr/bin/env python3
"""
Washington's statewide vector layers, from the Washington Geospatial Open
Data Portal, as GeoParquet and PMTiles.

The Washington Geospatial Open Data Portal (https://geo.wa.gov) lists the
layers the state's agencies publish as ArcGIS services: the statewide tax
parcels (all 39 counties, without owner names), WSDOT's state routes,
roadway data, traffic counts and fish passage barriers, Natural Resources'
geology and geologic hazards, forest practices layers and aquatic lands,
Ecology's water rights, wells and water quality, Fish and Wildlife's hunting
units and habitat, State Parks, Health, Emergency Management and others.
This reads each layer from its query endpoint and writes one GeoParquet file
per layer, and its PMTiles archive, grouped by theme:

  <out>/<theme>/<layer>.parquet     e.g. cadastre/parcels.parquet
  <out>/<theme>/<layer>.pmtiles
  <out>/<theme>/<layer>.png         the catalog thumbnail
  <out>/<theme>/_manifest.json      rows, sizes, sha256, titles, sources, tile settings
  <out>/<theme>/ATTRIBUTION.txt

wa_layers.csv says which layers: one row per source layer, its theme, its
server (a key of SERVERS: the agency's ArcGIS Online organisation or
server), the service and layer number. Rows sharing an id are one published
layer: WSDOT's historic traffic counts and sections told apart by `year`,
its stormwater assets (one file per geometry) and facilities by `category`.
The ShoreZone inventory's 13 layers, the same shore units each with its own
attributes, are joined on UNIT_ID into one file (join_on).

Not published: layers whose terms forbid redistribution or commercial use
(none found), copies of national datasets published elsewhere (NHD, the
Watershed Boundary Dataset, GNIS, Census TIGER), rasters, layers covering
less than a fifth of the state's extent, indexes of surveys and imagery,
older editions, the per-year and per-indicator layers whose content is in
another layer (the forest health aerial survey, the Environmental Health
Disparities map), Emergency Management's Hazus model inputs (copies of
census and road data), COVID-19 era layers and Commerce's zoning atlas
prototype. PLAN-wa-layers.csv, outside the repository, kept the reasons.

Every layer is in NAD83(HARN) / Washington South (ftUS) (EPSG:2927), the CRS
the state's agencies use statewide: the server projects the others as they
are read. Columns and values are the agencies', unchanged.

The fetch, the fingerprints, the parallel build and the per-layer publish
are arcgis_layers.py's, which maine.py, nh.py, ny.py and ca.py share;
writing, tiling and thumbnails are massgis.py's.

Usage:
  python3 scripts/datasets/wa.py upstream
  uv run --project scripts --with matplotlib python scripts/datasets/wa.py build \\
      --work-dir /Volumes/T9/wa/tmp --out-dir /Volumes/T9/wa/out [--layer parcels]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import arcgis_layers  # noqa: E402

PORTAL = "https://geo.wa.gov"


def server(base: str, kind: str, publisher: str) -> dict:
    return {"url": base + "/rest/services/{service}/" + kind + "/{layer}",
            "kind": "map service" if kind == "MapServer" else "feature service",
            "publisher": publisher, "undated": "changes"}


DNR = "Washington State Department of Natural Resources"
WSDOT = "Washington State Department of Transportation"
ECOLOGY = "Washington State Department of Ecology"
WDFW = "Washington Department of Fish and Wildlife"
RCO = "Washington State Recreation and Conservation Office"
DOR = "Washington State Department of Revenue"

# Where a CSV row's layer is read: its `server` column. A layer without edit
# dates is rebuilt when its count, highest object id or fields move.
SERVERS = {
    'commerce': server('https://services6.arcgis.com/tboeqGwETr5ppr5Q/arcgis', 'FeatureServer',
           'Washington State Department of Commerce'),
    'dahp': server('https://services6.arcgis.com/yIPFYZqx6a8IC4Hk/arcgis', 'FeatureServer',
           'Department of Archaeology and Historic Preservation'),
    'dnr1': server('https://gis.dnr.wa.gov/site1', 'MapServer', DNR),
    'dnr2': server('https://gis.dnr.wa.gov/site2', 'MapServer', DNR),
    'dnr2_fs': server('https://gis.dnr.wa.gov/site2', 'FeatureServer', DNR),
    'dnr3': server('https://gis.dnr.wa.gov/site3', 'MapServer', DNR),
    'dnr3_fs': server('https://gis.dnr.wa.gov/site3', 'FeatureServer', DNR),
    'dnr_nearshore': server('https://services.arcgis.com/4x406oNViizbGo13/arcgis', 'FeatureServer',
           f'{DNR}, Nearshore Habitat Program'),
    'doh': server('https://services8.arcgis.com/rGGrs6HCnw87OFOT/arcgis', 'FeatureServer',
           'Washington State Department of Health'),
    'dor': server('https://webgis.dor.wa.gov/ArcGIS', 'MapServer', DOR),
    'dor2': server('https://webgis.dor.wa.gov/arcgis', 'MapServer', DOR),
    'dshs': server('https://services2.arcgis.com/WW3T8U6q5EkZ9U3n/arcgis', 'FeatureServer',
           'Washington State Department of Social and Health Services'),
    'ecology': server('https://services.arcgis.com/6lCKYNJLvwTXqrmp/arcgis', 'FeatureServer', ECOLOGY),
    'ecology2': server('https://services.arcgis.com/6lCKYNJLvwTXqrmp/ArcGIS', 'FeatureServer', ECOLOGY),
    'emd': server('https://services7.arcgis.com/vUVXhXafpruJFs3l/arcgis', 'FeatureServer',
           'Washington State Military Department, Emergency Management Division'),
    'geoportal': server('https://services.arcgis.com/jsIt88o09Q0r1j8h/arcgis', 'FeatureServer',
           'Washington State Geospatial Portal'),
    'ocio': server('https://services3.arcgis.com/ikVUj9YUYY9keza1/arcgis', 'FeatureServer',
           'Washington State Office of the Chief Information Officer'),
    'ofm': server('https://services.arcgis.com/bCYnGqM4FMTBSjd1/arcgis', 'FeatureServer',
           'Washington State Office of Financial Management'),
    'ospi': server('https://services9.arcgis.com/fWunDXKkvCx1CM4b/arcgis', 'FeatureServer',
           'Office of Superintendent of Public Instruction'),
    'parks': server('https://services5.arcgis.com/4LKAHwqnBooVDUlX/arcgis', 'FeatureServer',
           'Washington State Parks and Recreation Commission'),
    'psp': server('https://services7.arcgis.com/iAd79FjHxHKsLP0y/arcgis', 'FeatureServer',
           'Puget Sound Partnership'),
    'rco': server('https://gismanager.rco.wa.gov/arcgis', 'MapServer', RCO),
    'rco_ago': server('https://services2.arcgis.com/TGEC20q86HQAeMS6/arcgis', 'FeatureServer', RCO),
    'utc': server('https://services2.arcgis.com/lXwA5ckdH5etcXUm/arcgis', 'FeatureServer',
           'Washington Utilities and Transportation Commission'),
    'wdfw': server('https://geodataservices.wdfw.wa.gov/arcgis', 'MapServer', WDFW),
    'wdfw_ago': server('https://services.arcgis.com/rcya3vExsaVBGUDp/arcgis', 'FeatureServer', WDFW),
    'wsdot': server('https://data.wsdot.wa.gov/arcgis', 'MapServer', WSDOT),
    'wsdot_fs': server('https://data.wsdot.wa.gov/arcgis', 'FeatureServer', WSDOT),
    'wsdot_ago': server('https://services.arcgis.com/IYrj3otxNjPsrTRD/ArcGIS', 'FeatureServer', WSDOT),
    'wsdot_ago2': server('https://services.arcgis.com/IYrj3otxNjPsrTRD/arcgis', 'FeatureServer', WSDOT),
    'wsdot_it': server('https://wsdot.wa.gov/arcgis', 'MapServer', WSDOT),
}
PUBLIC = "https://parquetry.geomermaids.com/wa"
LAYERS_CSV = Path(__file__).with_name("wa_layers.csv")
CRS = "EPSG:2927"
OUTLINE = ("boundaries", "state_boundary")
CREDIT = "Washington Geospatial Open Data Portal and the agency each layer names"

THEMES = {
    "boundaries": "Boundaries and Districts",
    "cadastre": "Parcels and Public Land Survey",
    "transport": "Transportation",
    "water_environment": "Water and Environment",
    "geology": "Geology and Geologic Hazards",
    "forest": "Forests and Forest Practices",
    "coast": "Coast, Shorelines and Aquatic Lands",
    "wildlife": "Fish and Wildlife",
    "recreation": "Parks and Recreation",
    "emergency": "Emergency Management and Wildfire",
    "community": "Health, Communities and Facilities",
}
COLORS = {
    "boundaries": "#8e44ad", "cadastre": "#7f6a4f", "transport": "#d35400",
    "water_environment": "#2f7fc1", "geology": "#9a6b3a", "forest": "#2e7d32",
    "coast": "#1b8a8a", "wildlife": "#3f8a4a", "recreation": "#6a8e23",
    "emergency": "#c0392b", "community": "#b03a2e",
}


def theme_title(theme: str) -> str:
    return f"Washington: {THEMES[theme]}"


def attribution(theme: str, entries: list[dict]) -> str:
    lines = "\n".join(f"  {e['name']:<44} {e['source']['urls'][0]}" for e in entries)
    return f"""\
# Data attribution

These layers are published by Washington's state agencies through the
Washington Geospatial Open Data Portal:

  Portal:     {PORTAL}
  Producers:  the agency each layer names (Transportation, Natural
              Resources, Ecology, Fish and Wildlife, State Parks, Health,
              Emergency Management, Financial Management, the Geospatial
              Portal and others)

{THEMES[theme]}: each layer and the service layer it is read from (the
agency for each one is in _manifest.json):

{lines}

Terms: credit the agency a layer names. The agencies provide their layers
as is, without warranty; WSDOT's are for planning use only, and its terms
ask users to hold WSDOT harmless for their use of the data. Layers whose
terms forbid redistribution or commercial use are not published here.

## What was changed here

One GeoParquet 2.0 file per layer. Folded into one file each, a column
telling the source layers apart: `year` for WSDOT's historic traffic counts
and sections, `category` for its stormwater assets (one file per geometry)
and facilities. The ShoreZone inventory's 13 layers, the same shore units
each with its own attributes, are joined on UNIT_ID into one file. Columns
and values are the agencies', unchanged; features without a geometry are
left out (counted in _manifest.json). The geometry is 2D, in NAD83(HARN) /
Washington South (ftUS) (EPSG:2927), the server projecting the layers
published in another CRS, with a `bbox` column declared as the GeoParquet
covering, rows sorted along a Hilbert curve. The PMTiles archives beside
them are derived for web maps, in lon/lat.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


PROFILE = arcgis_layers.Profile(
    name="wa", public=PUBLIC, layers_csv=LAYERS_CSV, servers=SERVERS, themes=THEMES,
    colors=COLORS, outline=OUTLINE, outline_crs=CRS, theme_title=theme_title,
    attribution=attribution, crs=CRS,
    join_on={"shorezone_inventory": "UNIT_ID"})


def layers() -> list[dict]:
    return arcgis_layers.layers(PROFILE)


if __name__ == "__main__":
    arcgis_layers.main(PROFILE, __doc__)
