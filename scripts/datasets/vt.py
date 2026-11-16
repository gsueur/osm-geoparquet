#!/usr/bin/env python3
"""
Vermont's statewide vector layers, from the Vermont Open Geodata Portal, as
GeoParquet and PMTiles.

The Vermont Open Geodata Portal (https://geodata.vermont.gov), run by the
Vermont Center for Geographic Information (VCGI), lists the layers the
state's agencies publish as ArcGIS services: the statewide standardized
parcels, the E911 Board's address points, road centerlines and driveways,
VTrans' traffic counts, bridges and linear referencing, the Agency of
Natural Resources' BioFinder, wetlands, water infrastructure and stream
assessments, the Geological Survey's bedrock and surficial maps, Public
Service's broadband status, the Act 174 wind and solar potential and others.
This reads each layer from its query endpoint and writes one GeoParquet file
per layer, and its PMTiles archive, grouped by theme:

  <out>/<theme>/<layer>.parquet     e.g. cadastre/parcels.parquet
  <out>/<theme>/<layer>.pmtiles
  <out>/<theme>/<layer>.png         the catalog thumbnail
  <out>/<theme>/_manifest.json      rows, sizes, sha256, titles, sources, tile settings
  <out>/<theme>/ATTRIBUTION.txt

vt_layers.csv says which layers: one row per source layer, its theme, its
server (a key of SERVERS), the service and layer number. Rows sharing an id
are one published layer, each source told apart by its `category` or
`year`: the towns' zoning districts and overlays (371 layers from the
regional planning commissions, one file, `category` "<town>: <map>"; the
columns they share kept, each town's others in other_attributes), the
broadband status by year (2019 to 2026), the Health Department's prior
years of TickTracker reports.

Not published: the regional planning commissions' other layers (local
copies and maps), copies of national datasets published elsewhere (Census,
NWI, NHD, the Watershed Boundary Dataset, GNIS), statewide building
footprints (USA Structures covers them, as for New York and Oregon), older
editions, rasters, layers covering less than a fifth of the state's extent,
and indexes of maps and surveys. The parcels leave out the owner names and
mailing addresses (DROP_COLUMNS). PLAN-vt-layers.csv, outside the
repository, kept the reasons.

Every layer is in NAD83 / Vermont (EPSG:32145, metres), VCGI's standard: the
server projects the others (Web Mercator, for about half) as they are read.
Columns and values are the agencies', unchanged but for the owner columns.
Layers their publisher marks CC BY-SA 4.0 (the `license` column; most of the
Agency of Natural Resources' and the Geological Survey's) keep that licence.

The fetch, the fingerprints, the parallel build and the per-layer publish
are arcgis_layers.py's, which maine.py, nh.py, ny.py, ca.py, wa.py and
oregon.py share; writing, tiling and thumbnails are massgis.py's.

Usage:
  python3 scripts/datasets/vt.py upstream
  uv run --project scripts --with matplotlib python scripts/datasets/vt.py build \\
      --work-dir /Volumes/T9/vt/tmp --out-dir /Volumes/T9/vt/out [--layer parcels]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import arcgis_layers  # noqa: E402

PORTAL = "https://geodata.vermont.gov"


def server(base: str, kind: str, publisher: str) -> dict:
    return {"url": base + "/rest/services/{service}/" + kind + "/{layer}",
            "kind": "map service" if kind == "MapServer" else "feature service",
            "publisher": publisher, "undated": "changes"}


ANR = "the Vermont Agency of Natural Resources"
VTRANS = "the Vermont Agency of Transportation (VTrans)"
HEALTH = "the Vermont Department of Health"
PSD = "the Vermont Department of Public Service"

# Where a CSV row's layer is read: its `server` column. A layer without edit
# dates is rebuilt when its count, highest object id or fields move.
SERVERS = {
    '': server('https://services1.arcgis.com/BkFxaEFNwHqX3tAw/arcgis', 'FeatureServer',
               'the Vermont Center for Geographic Information (VCGI)'),
    'anr': server('https://anrmaps.vermont.gov/arcgis', 'MapServer', ANR),
    'anr_ago': server('https://services5.arcgis.com/Uzks6LSde6r23wwG/arcgis', 'FeatureServer', ANR),
    'vtrans': server('https://maps.vtrans.vermont.gov/arcgis', 'FeatureServer', VTRANS),
    'vtrans_map': server('https://maps.vtrans.vermont.gov/arcgis', 'MapServer', VTRANS),
    'vtrans_ago': server('https://services1.arcgis.com/NXmBVyW5TaiCXqFs/arcgis', 'FeatureServer', VTRANS),
    'health': server('https://maps.healthvermont.gov/arcgis', 'FeatureServer', HEALTH),
    'health_map': server('https://maps.healthvermont.gov/arcgis', 'MapServer', HEALTH),
    'health_ago': server('https://services.arcgis.com/YKJ5JtnaPQ2jDbX8/arcgis', 'FeatureServer', HEALTH),
    'vcgi_map': server('https://maps.vcgi.vermont.gov/arcgis', 'MapServer', PSD),
    'vcgi_dev_map': server('https://dev.maps.vcgi.vermont.gov/arcgis', 'MapServer', PSD),
    'psd': server('https://services3.arcgis.com/5tLOjexclC0TuD0h/arcgis', 'FeatureServer', PSD),
    'agriculture': server('https://services1.arcgis.com/YQcKxYgJhOGW9xID/arcgis', 'FeatureServer',
                          'the Vermont Agency of Agriculture, Food and Markets'),
    'vce': server('https://services1.arcgis.com/d3OaJoSAh2eh6OA9/arcgis', 'FeatureServer',
                  'the Vermont Center for Ecostudies'),
    # The zoning maps, each on its regional planning commission's ArcGIS
    # Online organisation: the CSV's service is the whole service URL.
    'rpc': {"url": "{service}/{layer}", "kind": "feature service",
            "publisher": "Vermont's regional planning commissions", "undated": "changes"},
}
PUBLIC = "https://parquetry.geomermaids.com/vt"
LAYERS_CSV = Path(__file__).with_name("vt_layers.csv")
CRS = "EPSG:32145"
OUTLINE = ("boundaries_planning", "state_boundary")
CREDIT = "VCGI and the agency each layer names, through the Vermont Open Geodata Portal"

THEMES = {
    "boundaries_planning": "Boundaries, Districts and Zoning",
    "cadastre": "Parcels, Addresses and Protected Lands",
    "transport": "Transportation",
    "water": "Water, Wetlands and Rivers",
    "natural_heritage": "Habitat, Species and Natural Communities",
    "land_resources": "Forests, Farms and Recreation",
    "geology_soils": "Geology and Soils",
    "energy_telecom": "Energy, Utilities and Broadband",
    "environmental_sites": "Waste, Hazardous Sites and Permits",
    "health_safety": "Health and Emergency Services",
    "community": "Communities and Facilities",
}
COLORS = {
    "boundaries_planning": "#8e44ad", "cadastre": "#7f6a4f", "transport": "#d35400",
    "water": "#2f7fc1", "natural_heritage": "#3f8a4a", "land_resources": "#6a8e23",
    "geology_soils": "#9a6b3a", "energy_telecom": "#c79a00", "environmental_sites": "#7b3f61",
    "health_safety": "#c0392b", "community": "#b03a2e",
}

# The statewide parcels: owner names and mailing addresses left out.
OWNER_COLUMNS = ("OWNER1", "OWNER2", "ADDRGL1", "ADDRGL2", "CITYGL", "STGL", "ZIPGL")
DROP_COLUMNS = {"parcels": OWNER_COLUMNS}

# The zoning maps' columns (207 between the 371 maps): the ones most share,
# the others of each map in other_attributes (JSON).
ZONING_COLUMNS = ("district", "overlay", "zone", "dist_name", "dist_type", "note")


def theme_title(theme: str) -> str:
    return f"Vermont: {THEMES[theme]}"


def attribution(theme: str, entries: list[dict]) -> str:
    lines = "\n".join(f"  {e['name']:<44} {e['source']['urls'][0]}" for e in entries)
    return f"""\
# Data attribution

These layers are published by Vermont's state agencies through the Vermont
Open Geodata Portal, run by the Vermont Center for Geographic Information
(VCGI):

  Portal:     {PORTAL}
  Producers:  the agency each layer names (VCGI, the E911 Board, the Agency
              of Natural Resources and its departments, the Geological
              Survey, VTrans, Public Service, Health, Housing and Community
              Development and others); the towns' zoning maps by the
              regional planning commissions

{THEMES[theme]}: each layer and the service layer it is read from (the
agency for each one is in _manifest.json):

{lines}

Terms: credit the agency a layer names. The agencies provide their layers
as is, without warranty, and not for legal, engineering or survey use.
Layers whose publisher marks them Creative Commons Attribution-ShareAlike
4.0 (https://creativecommons.org/licenses/by-sa/4.0/) stay under that
licence: credit the publisher and share what you derive from them alike.
Their catalog collections say so.

## What was changed here

One GeoParquet 2.0 file per layer. Folded into one file each, a column
telling the source layers apart: `category` for the towns' zoning maps
("<town>: <map>"; the columns most share kept, each map's others in
other_attributes, JSON) and the prior years of TickTracker reports, `year`
for the broadband status. The parcels leave out the owner names and mailing
addresses. Other columns and values are the agencies', unchanged; features
without a geometry are left out (counted in _manifest.json). The geometry
is 2D, in NAD83 / Vermont (EPSG:32145, metres), the server projecting the
layers published in another CRS, with a `bbox` column declared as the
GeoParquet covering, rows sorted along a Hilbert curve. The PMTiles archives
beside them are derived for web maps, in lon/lat.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


PROFILE = arcgis_layers.Profile(
    name="vt", public=PUBLIC, layers_csv=LAYERS_CSV, servers=SERVERS, themes=THEMES,
    colors=COLORS, outline=OUTLINE, outline_crs=CRS, theme_title=theme_title,
    attribution=attribution, crs=CRS, drop_columns=DROP_COLUMNS,
    fold_keep={"zoning": ZONING_COLUMNS},
    # BioFinder's 916,000 landscape priority polygons and the 520,000 Act 174
    # wind potential polygons: the largest polygon layers, built with no
    # other beside them (WA, 2026-11-15: a 1M+ polygon layer tiled beside
    # two others took the runner down).
    alone=frozenset({"biofinder_landscape_scale_overall_priorities", "act_174_wind_potential"}))


def layers() -> list[dict]:
    return arcgis_layers.layers(PROFILE)


if __name__ == "__main__":
    arcgis_layers.main(PROFILE, __doc__)
