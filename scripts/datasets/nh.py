#!/usr/bin/env python3
"""
NH GRANIT's statewide vector layers as GeoParquet and PMTiles.

NH GRANIT, New Hampshire's statewide GIS clearinghouse at the University of
New Hampshire, publishes the state's layers through the NH Geodata Portal
(https://granit.unh.edu), as ArcGIS feature services on nhgeodata.unh.edu
(a few as map services, and the 2022 SLAMM runs on ArcGIS Online). This
reads each layer from its query endpoint and writes one GeoParquet file per
layer, and its PMTiles archive, grouped by theme:

  <out>/<theme>/<layer>.parquet     e.g. cadastre/parcel_mosaic_polygons.parquet
  <out>/<theme>/<layer>.pmtiles
  <out>/<theme>/<layer>.png         the catalog thumbnail
  <out>/<theme>/_manifest.json      rows, sizes, sha256, titles, sources, tile settings
  <out>/<theme>/ATTRIBUTION.txt

nh_layers.csv says which layers: one row per source layer, its theme, its
server (empty: the portal's hosted feature services; `map`: its map
services; `agol`: the SLAMM runs on ArcGIS Online), the service and layer
number. Rows sharing an id are one published layer, each source told apart
by its `scenario`: the 2023 sea-level-rise scenarios, the groundwater rise
and vulnerability scenarios and the simulated saltwater intrusion. The
portal also lists imagery, LiDAR rasters and web apps, none of which are
published here; nor are the 2-ft contours (about 100 tiles), the canopy
height polygons, NWI Plus (the national NWI is on parquetry already), the
2012 districts, or the items whose terms say distribution is discouraged
(the Great Bay eelgrass years, the dam inventory, lake bathymetry, NPDES),
nor the coastal impervious surfaces (2015, 2021: their map services can
neither page nor give statistics).

Every layer is in NAD83(NSRS2007) / New Hampshire, US survey feet
(EPSG:3437), the CRS of most of the portal's layers: the server projects
the others (NAD83(2011) / New Hampshire, EPSG:6525, for SLAMM and the
groundwater layers) as they are read. Columns and values are GRANIT's,
unchanged.

The fetch, the fingerprints and the manifests are arcgis_layers.py's, which
maine.py shares; writing, tiling and thumbnails are massgis.py's. The
portal's hosted services give no edit dates: a layer is rebuilt when its
count, its highest object id or its fields change (GRANIT replaces a layer
when it updates it; a full build is 5 to 8 hours, too long to repeat every
month). The SLAMM runs on ArcGIS Online give edit dates, and are rebuilt
when they move. scripts/filedates.py keeps a file's date, and the file on the
bucket, as they were when its bytes did not change.

Usage:
  python3 scripts/datasets/nh.py upstream
  uv run --project scripts --with matplotlib python scripts/datasets/nh.py build \\
      --work-dir /Volumes/T9/nh/tmp --out-dir /Volumes/T9/nh/out [--layer dot_roads]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import arcgis_layers  # noqa: E402

PORTAL = "https://granit.unh.edu"
HOSTED = "https://nhgeodata.unh.edu/hosting/rest/services"
MAPS = "https://nhgeodata.unh.edu/nhgeodata/rest/services"
AGOL = "https://services9.arcgis.com/wnvDDrXX8EouLkZP/arcgis/rest/services"
# Where a CSV row's layer is read: its `server` column. No edit dates on the
# portal's services: GRANIT replaces a layer when it updates it, which moves its
# count or highest object id (`changes`, arcgis_layers.py).
SERVERS = {
    "": {"url": HOSTED + "/{service}/FeatureServer/{layer}", "kind": "feature service",
         "publisher": "NH GRANIT", "undated": "changes"},
    "map": {"url": MAPS + "/{service}/MapServer/{layer}", "kind": "map service",
            "publisher": "NH GRANIT", "undated": "changes"},
    "agol": {"url": AGOL + "/{service}/FeatureServer/{layer}", "kind": "feature service",
             "publisher": "NH GRANIT", "undated": "changes"},
}
PUBLIC = "https://parquetry.geomermaids.com/nh"
LAYERS_CSV = Path(__file__).with_name("nh_layers.csv")
CRS = "EPSG:3437"
# The towns the thumbnails are drawn over (the state boundary is a line).
OUTLINE = ("boundaries", "political_boundaries")
CREDIT = "NH GRANIT"

THEMES = {
    "boundaries": "Boundaries and Districts",
    "cadastre": "Parcels",
    "transport_utilities": "Transportation and Utilities",
    "facilities": "Facilities and Places",
    "water": "Water Resources",
    "geology_soils": "Geology and Soils",
    "environment": "Conservation and Wildlife",
    "coast": "Coast and Sea-Level Rise",
    "land_cover": "Land Use and Land Cover",
}
COLORS = {
    "boundaries": "#8e44ad", "cadastre": "#7f6a4f", "transport_utilities": "#d35400",
    "facilities": "#c0392b", "water": "#2f7fc1", "geology_soils": "#9a6b3a",
    "environment": "#3f8a4a", "coast": "#1b8a8a", "land_cover": "#5aa02c",
}


def theme_title(theme: str) -> str:
    return f"NH GRANIT: {THEMES[theme]}"


def attribution(theme: str, entries: list[dict]) -> str:
    lines = "\n".join(f"  {e['name']:<44} {e['source']['urls'][0]}" for e in entries)
    return f"""\
# Data attribution

These layers are published by NH GRANIT, New Hampshire's statewide GIS
clearinghouse at the University of New Hampshire, for the state agencies
and partners that produce them:

  Portal:     {PORTAL}
  Services:   {HOSTED}
  Producer:   NH GRANIT and the agencies each layer names

{THEMES[theme]}: each layer and the service layer it is read from (each
one's count and highest object id are in _manifest.json):

{lines}

Terms: cite NH GRANIT. Most of the portal's items ask for it ("Please cite
NH GRANIT"), many say the data is not for legal use, and some name the
agency that owns the data (the parcel mosaic: the NH Department of Revenue
Administration). Credit NH GRANIT, and the agency a layer names, when you
use it. The data is provided as is, without warranty.

## What was changed here

One GeoParquet 2.0 file per layer. Folded into one file each, a `scenario`
column telling the source layers apart: the 2023 sea-level-rise scenarios,
groundwater rise, the areas vulnerable to groundwater rise and the simulated
saltwater intrusion. Columns and values are GRANIT's, unchanged; features
without a geometry are left out (counted in _manifest.json). The geometry
is 2D, in NAD83(NSRS2007) / New Hampshire, US survey feet (EPSG:3437), the
server projecting the layers published in another CRS, with a `bbox` column
declared as the GeoParquet covering, rows sorted along a Hilbert curve. The
PMTiles archives beside them are derived for web maps, in lon/lat.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


# GeoParquet only: polygons traced from rasters, pixel staircases still over
# a 4 MB tile at zoom 14 (run of 2026-11-13). A map of them belongs with the
# rasters.
UNTILED = frozenset({
    "sea_level_rise_2023", "groundwater_rise", "groundwater_vulnerable_areas",
    "slamm_2022_0_3_m_slr_at_year_2050", "slamm_2022_0_5_m_slr_at_year_2050",
    "slamm_2022_0_75_m_slr_at_year_2100", "slamm_2022_1_2_m_slr_at_year_2100",
    "slamm_2022_1_5_m_slr_at_year_2100",
})


PROFILE = arcgis_layers.Profile(
    name="nh", public=PUBLIC, layers_csv=LAYERS_CSV, servers=SERVERS, themes=THEMES,
    colors=COLORS, outline=OUTLINE, outline_crs=CRS, theme_title=theme_title,
    attribution=attribution, crs=CRS, untiled=UNTILED)


def layers() -> list[dict]:
    return arcgis_layers.layers(PROFILE)


if __name__ == "__main__":
    arcgis_layers.main(PROFILE, __doc__)
