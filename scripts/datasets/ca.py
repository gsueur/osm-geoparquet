#!/usr/bin/env python3
"""
California's statewide vector layers, from the California State Geoportal,
as GeoParquet and PMTiles.

The California State Geoportal (https://gis.data.ca.gov) lists the layers
the state's departments publish as ArcGIS services: Fish and Wildlife
(species ranges, Areas of Conservation Emphasis, hunting and fishing
zones, Marine Protected Areas), CAL FIRE (fire hazard severity zones, fire
perimeters), Caltrans, the Water Boards, Conservation (farmland, mines,
oil and gas wells), the Board of Equalization and CDTFA (tax rate areas),
Education, State Parks, State Lands and others. This reads each layer from
its query endpoint and writes one GeoParquet file per layer, and its
PMTiles archive, grouped by theme:

  <out>/<theme>/<layer>.parquet     e.g. fire/fire_hazard_severity_zones_sra.parquet
  <out>/<theme>/<layer>.pmtiles
  <out>/<theme>/<layer>.png         the catalog thumbnail
  <out>/<theme>/_manifest.json      rows, sizes, sha256, titles, sources, tile settings
  <out>/<theme>/ATTRIBUTION.txt

ca_layers.csv says which layers: one row per source layer, its theme, its
server (a key of SERVERS: the department's ArcGIS Online organisation or
server), the service and layer number. Rows sharing an id are one published
layer, each source told apart by its `category` (the CWHR species ranges,
elk, deer and pronghorn herds, the
Board of Equalization's tax rate areas by county) or its `year` (Important
Farmland, the sales and use tax rates, Caltrans planning grants).

Not published: the layers whose terms forbid redistribution or commercial
use (the Energy Commission's, wherever they are hosted; CGS seismic hazard
zones; Caltrans' odometer), Esri's own layers, rasters (the CWHR predicted
habitat models among them: a later raster dataset), layers covering less
than a fifth of the state's extent (local studies), provider directories,
field survey records, live feeds and older editions. PLAN-ca-layers.csv,
outside the repository, kept the reasons.

Every layer is in NAD83 / California Albers (EPSG:3310): the server projects
the others as they are read. Columns and values are the departments',
unchanged.

The fetch, the fingerprints, the parallel build and the per-layer publish
are arcgis_layers.py's, which maine.py, nh.py and ny.py share; writing,
tiling and thumbnails are massgis.py's.

Usage:
  python3 scripts/datasets/ca.py upstream
  uv run --project scripts --with matplotlib python scripts/datasets/ca.py build \\
      --work-dir /Volumes/T9/ca/tmp --out-dir /Volumes/T9/ca/out [--layer cwhr_species_ranges]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import arcgis_layers  # noqa: E402

PORTAL = "https://gis.data.ca.gov"


def server(base: str, kind: str, publisher: str) -> dict:
    return {"url": base + "/rest/services/{service}/" + kind + "/{layer}",
            "kind": "map service" if kind == "MapServer" else "feature service",
            "publisher": publisher, "undated": "changes"}


# Where a CSV row's layer is read: its `server` column. A layer without edit
# dates is rebuilt when its count, highest object id or fields move.
SERVERS = {
    '': server('https://services2.arcgis.com/Uq9r85Potqm3MfRV/arcgis', 'FeatureServer',
           'California Department of Fish and Wildlife'),
    'boe': server('https://services7.arcgis.com/iwxhJVOFEKDxO7gk/arcgis', 'FeatureServer',
           'California State Board of Equalization'),
    'broadband': server('https://services1.arcgis.com/8CpMUd3fdw6aXef7/arcgis', 'FeatureServer',
           'California Department of Technology'),
    'calepa': server('https://services8.arcgis.com/elq7Fz2RcwtTdDmp/arcgis', 'FeatureServer',
           'California Environmental Protection Agency'),
    'calfire': server('https://services1.arcgis.com/jUJYIo9tSA7EHvfZ/arcgis', 'FeatureServer',
           'California Department of Forestry and Fire Protection'),
    'calfire2': server('https://jujyio9tsa7ehvfz.svcs1.arcgis.com/jUJYIo9tSA7EHvfZ/arcgis', 'FeatureServer',
           'California Department of Forestry and Fire Protection'),
    'calfire3': server('https://services3.arcgis.com/T4QMspbfLg3qTGWY/ArcGIS', 'FeatureServer',
           'California Department of Forestry and Fire Protection'),
    'caloes': server('https://services.arcgis.com/BLN4oKB0N1YSgvY8/arcgis', 'FeatureServer',
           "CA Governor's Office of Emergency Services"),
    'caltrans': server('https://caltrans-gis.dot.ca.gov/arcgis', 'FeatureServer',
           'Caltrans'),
    'carb': server('https://services6.arcgis.com/x7ftScCDR8g2kVFB/arcgis', 'FeatureServer',
           'California Air Resources Board'),
    'cde': server('https://services3.arcgis.com/fdvHcZVgB2QSRNkL/arcgis', 'FeatureServer',
           'California Department of Education'),
    'cdfa': server('https://services2.arcgis.com/rFh2EpMO892UxQuz/arcgis', 'FeatureServer',
           'California Department of Food and Agriculture'),
    'cdpr': server('https://services3.arcgis.com/P3UbZpsmlNlDsarV/arcgis', 'FeatureServer',
           'California Department of Pesticide Regulation'),
    'cdt': server('https://services3.arcgis.com/uknczv4rpevve42E/arcgis', 'FeatureServer',
           'California Department of Technology'),
    'cdt2': server('https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS', 'FeatureServer',
           'California Department of Technology'),
    'cdtfa': server('https://services6.arcgis.com/snwvZ3EmaoXJiugR/arcgis', 'FeatureServer',
           'California Department of Tax and Fee Administration'),
    'chp': server('https://services9.arcgis.com/eN3ItO6OZehV2lSW/arcgis', 'FeatureServer',
           'California Highway Patrol'),
    'coastal_commission': server('https://services9.arcgis.com/wwVnNW92ZHUIr0V0/arcgis', 'FeatureServer',
           'CA Coastal Commission Mapping Unit'),
    'coastal_conservancy': server('https://services3.arcgis.com/PsrqKIurrMFqlhX3/arcgis', 'FeatureServer',
           'California State Coastal Conservancy'),
    'conservation': server('https://gis.conservation.ca.gov/server', 'FeatureServer',
           'California Department of Conservation'),
    'conservation_map': server('https://gis.conservation.ca.gov/server', 'MapServer',
           'California Department of Conservation'),
    'dgs': server('https://services8.arcgis.com/a4GMqC2tQHvYiVtK/arcgis', 'FeatureServer',
           'CA Department of General Services'),
    'dmv': server('https://services6.arcgis.com/8zbIxywG5xu2vq8a/arcgis', 'FeatureServer',
           'California Department of Motor Vehicles'),
    'dtsc': server('https://services3.arcgis.com/Oy2JTCD10wkoelxS/arcgis', 'FeatureServer',
           'California Department of Toxic Substances Control'),
    'dwr': server('https://services.arcgis.com/Wvy1i2QPQwOxNumH/arcgis', 'FeatureServer',
           'California Department of Water Resources'),
    'health': server('https://services5.arcgis.com/fMBfBrOnc6OOzh7V/arcgis', 'FeatureServer',
           'CA Department of Health Care Access and Information'),
    'hsr': server('https://services3.arcgis.com/rGGp0aiv6Rf11t2H/arcgis', 'FeatureServer',
           'California High-Speed Rail Authority'),
    'middle_mile': server('https://services6.arcgis.com/sAv98EYUZbLCVPW0/arcgis', 'FeatureServer',
           'Middle Mile Broadband for California'),
    'oehha': server('https://services1.arcgis.com/PCHfdHz4GlDNAhBb/arcgis', 'FeatureServer',
           'California Office of Environmental Health Hazard Assessment'),
    'opr': server('https://services8.arcgis.com/Xr1lDrwMv89PhjD9/arcgis', 'FeatureServer',
           "Governor's Office of Land Use and Climate Innovation"),
    'parks': server('https://services2.arcgis.com/AhxrK3F6WM8ECvDi/arcgis', 'FeatureServer',
           'California State Parks'),
    'slc': server('https://services3.arcgis.com/5aaQCuq3e4GRvkFG/arcgis', 'FeatureServer',
           'California State Lands Commission'),
    'snc': server('https://services1.arcgis.com/sTaVXkn06Nqew9yU/arcgis', 'FeatureServer',
           'Sierra Nevada Conservancy'),
    'waterboards': server('https://gispublic.waterboards.ca.gov/portalserver', 'FeatureServer',
           'California Water Boards'),
    'waterboards_map': server('https://gispublic.waterboards.ca.gov/portalserver', 'MapServer',
           'California Water Boards'),
}
PUBLIC = "https://parquetry.geomermaids.com/ca"
LAYERS_CSV = Path(__file__).with_name("ca_layers.csv")
CRS = "EPSG:3310"
OUTLINE = ("boundaries", "state_boundary")
CREDIT = "California State Geoportal and the department each layer names"

THEMES = {
    "boundaries": "Boundaries and Districts",
    "tax": "Tax Rate Areas",
    "transport": "Transportation",
    "fire": "Fire",
    "water": "Water",
    "land_geology": "Land, Farmland and Geology",
    "wildlife": "Fish, Wildlife and Habitat",
    "coast_marine": "Coast and State Lands",
    "community": "Communities, Facilities and Environment",
}
COLORS = {
    "boundaries": "#8e44ad", "tax": "#7f6a4f", "transport": "#d35400", "fire": "#c0392b",
    "water": "#2f7fc1", "land_geology": "#9a6b3a", "wildlife": "#3f8a4a",
    "coast_marine": "#1b8a8a", "community": "#b03a2e",
}


def theme_title(theme: str) -> str:
    return f"California: {THEMES[theme]}"


def attribution(theme: str, entries: list[dict]) -> str:
    lines = "\n".join(f"  {e['name']:<44} {e['source']['urls'][0]}" for e in entries)
    return f"""\
# Data attribution

These layers are published by California's state departments through the
California State Geoportal:

  Portal:     {PORTAL}
  Producers:  the department each layer names (Fish and Wildlife, CAL FIRE,
              Caltrans, the Water Boards, Conservation, the Board of
              Equalization, CDTFA, Education, State Parks, State Lands and
              others)

{THEMES[theme]}: each layer and the service layer it is read from (the
department for each one is in _manifest.json):

{lines}

Terms: credit the department a layer names. Fish and Wildlife's layers are
licensed CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/); the other
departments provide theirs as is, without warranty, most of them not for
legal or survey use (CAL FIRE, the Board of Equalization, the Water Boards,
Conservation). Layers whose terms forbid redistribution or commercial use
are not published here.

## What was changed here

One GeoParquet 2.0 file per layer. Folded into one file each, a column
telling the source layers apart: `category` for the CWHR species ranges, the
elk, deer and pronghorn herds and the
tax rate areas by county; `year` for Important Farmland, the sales and use
tax rates and Caltrans' planning grants. Caltrans' climate vulnerability is
one file per hazard. Columns and values are the departments',
unchanged; features without a geometry are left out (counted in
_manifest.json). The geometry is 2D, in NAD83 / California Albers
(EPSG:3310), the server projecting the layers published in another CRS, with
a `bbox` column declared as the GeoParquet covering, rows sorted along a
Hilbert curve. The PMTiles archives beside them are derived for web maps, in
lon/lat.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


PROFILE = arcgis_layers.Profile(
    name="ca", public=PUBLIC, layers_csv=LAYERS_CSV, servers=SERVERS, themes=THEMES,
    colors=COLORS, outline=OUTLINE, outline_crs=CRS, theme_title=theme_title,
    attribution=attribution, crs=CRS,
    # GeoParquet only. The species ranges: 762, 63 M vertices, overlapping
    # statewide; no tile under the size cap holds them, even with one
    # property from zoom 10. The farmland history: 20 surveys stacked on the
    # same ground; important_farmland_most_recent is the one to map.
    untiled=frozenset({"cwhr_species_ranges", "important_farmland_history"}))


def layers() -> list[dict]:
    return arcgis_layers.layers(PROFILE)


if __name__ == "__main__":
    arcgis_layers.main(PROFILE, __doc__)
