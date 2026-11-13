#!/usr/bin/env python3
"""
New York State's statewide vector layers, from the NYS GIS Clearinghouse,
as GeoParquet and PMTiles.

The NYS GIS Clearinghouse (https://data.gis.ny.gov), run by the Office of
Information Technology Services' Geospatial Services, lists the layers the
state's agencies publish as ArcGIS feature services: ITS's own framework
layers (parcels, address points, streets, civil boundaries, schools), and
the Departments of State, Environmental Conservation and Transportation,
the Thruway Authority, OPRHP and the Natural Heritage Program. This reads
each layer from its query endpoint and writes one GeoParquet file per
layer, and its PMTiles archive, grouped by theme:

  <out>/<theme>/<layer>.parquet     e.g. cadastre/tax_parcels.parquet
  <out>/<theme>/<layer>.pmtiles
  <out>/<theme>/<layer>.png         the catalog thumbnail
  <out>/<theme>/_manifest.json      rows, sizes, sha256, titles, sources, tile settings
  <out>/<theme>/ATTRIBUTION.txt

ny_layers.csv says which layers: one row per source layer, its theme, its
server (a key of SERVERS: the agency's ArcGIS Online organisation or
server), the service and layer number. Rows sharing an id are one published
layer, each source told apart by its `category` (the school layers, the
polling sites, the Natural Heritage important areas, NYSDOT's material
suppliers, the Thruway rest stops, the tax-credit tracts) or its `year` (the
South Shore benthic habitat surveys).

Not published: the layers whose terms forbid redistribution (most of DEC's,
"Secondary distribution of the data is not allowed" or "not for
distribution to third parties"; OPRHP's parks, trails and National
Register listings and TNC's forest blocks, non-commercial only), county
and local layers, indexes, rasters, the building footprints (USA
Structures covers them), and the layers already on parquetry from the
Census Bureau. The tax parcels (the 38 counties that let ITS share their
polygons) and the parcel centroids (all 62) leave out the owner names and
mailing addresses (DROP_COLUMNS).

Every layer is in NAD83 / UTM zone 18N (EPSG:26918), the CRS of ITS's
framework layers: the server projects the others (Web Mercator, for about
half) as they are read. Columns and values are the agencies', unchanged but
for the owner columns.

The fetch, the fingerprints and the manifests are arcgis_layers.py's, which
maine.py and nh.py share; writing, tiling and thumbnails are massgis.py's.
Most services give edit dates; those that do not are rebuilt when their
count, highest object id or fields change (`changes`). scripts/filedates.py
keeps a file's date, and the file on the bucket, as they were when its
bytes did not change.

Usage:
  python3 scripts/datasets/ny.py upstream
  uv run --project scripts --with matplotlib python scripts/datasets/ny.py build \\
      --work-dir /Volumes/T9/ny/tmp --out-dir /Volumes/T9/ny/out [--layer streets]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import arcgis_layers  # noqa: E402

PORTAL = "https://data.gis.ny.gov"
ITS = "https://services6.arcgis.com/EbVsqZ18sv1kVJ3k/arcgis/rest/services"
FEATURE = "/{service}/FeatureServer/{layer}"


def server(base: str, publisher: str) -> dict:
    return {"url": base + "/rest/services" + FEATURE, "kind": "feature service",
            "publisher": publisher, "undated": "changes"}


# Where a CSV row's layer is read: its `server` column. A layer without edit
# dates is rebuilt when its count, highest object id or fields move.
SERVERS = {
    "": server("https://services6.arcgis.com/EbVsqZ18sv1kVJ3k/arcgis", "NYS ITS Geospatial Services"),
    "geohub": server("https://nysgeohub.ny.gov/arcgis", "NYS ITS Geospatial Services"),
    "dec": server("https://services6.arcgis.com/DZHaqZm9cxOD4CWM/arcgis",
                  "NYS Department of Environmental Conservation"),
    "dos": server("https://services2.arcgis.com/okXm0pb6aWH6XOGI/arcgis", "NYS Department of State"),
    "thruway": server("https://services2.arcgis.com/gubH6kG9JCAsMX2M/arcgis", "NYS Thruway Authority"),
    "oprhp": server("https://services.arcgis.com/1xFZPtKn1wKC6POA/arcgis",
                    "NYS Office of Parks, Recreation and Historic Preservation"),
    "dot": server("https://gis.dot.ny.gov/hostingny", "NYS Department of Transportation"),
    "dotportal": server("https://gisportalny.dot.ny.gov/hostingny", "NYS Department of Transportation"),
}
PUBLIC = "https://parquetry.geomermaids.com/ny"
LAYERS_CSV = Path(__file__).with_name("ny_layers.csv")
CRS = "EPSG:26918"
OUTLINE = ("boundaries", "state_shoreline")
CREDIT = "New York State GIS Clearinghouse (NYS ITS Geospatial Services) and the agency each layer names"
# The owners' names and mailing addresses on the assessment rolls.
OWNER_COLUMNS = ("PRIMARY_OWNER", "MAIL_ADDR", "PO_BOX", "MAIL_CITY", "MAIL_STATE", "MAIL_ZIP",
                 "ADD_OWNER", "ADD_MAIL_ADDR", "ADD_MAIL_PO_BOX", "ADD_MAIL_CITY",
                 "ADD_MAIL_STATE", "ADD_MAIL_ZIP")
DROP_COLUMNS = {"tax_parcels": OWNER_COLUMNS, "tax_parcel_centroids": OWNER_COLUMNS}

THEMES = {
    "boundaries": "Boundaries and Districts",
    "cadastre": "Tax Parcels",
    "addresses": "Addresses and Streets",
    "transport": "Transportation",
    "facilities": "Facilities and Permits",
    "environment": "Wetlands, Habitat and Conservation",
    "coast": "Coast and Waterfronts",
    "community": "Community Development",
    "heritage": "Historic Preservation",
}
COLORS = {
    "boundaries": "#8e44ad", "cadastre": "#7f6a4f", "addresses": "#b03a2e",
    "transport": "#d35400", "facilities": "#c0392b", "environment": "#3f8a4a",
    "coast": "#1b8a8a", "community": "#2f7fc1", "heritage": "#9a6b3a",
}


def theme_title(theme: str) -> str:
    return f"New York State: {THEMES[theme]}"


def attribution(theme: str, entries: list[dict]) -> str:
    lines = "\n".join(f"  {e['name']:<40} {e['source']['urls'][0]}" for e in entries)
    return f"""\
# Data attribution

These layers are published by New York State agencies through the NYS GIS
Clearinghouse, run by the Office of Information Technology Services'
Geospatial Services:

  Portal:     {PORTAL}
  Producers:  NYS ITS Geospatial Services, the Departments of State,
              Environmental Conservation and Transportation, the Thruway
              Authority, OPRHP and the New York Natural Heritage Program

{THEMES[theme]}: each layer and the service layer it is read from (the
agency for each one is in _manifest.json):

{lines}

Terms: credit the New York State GIS Clearinghouse and the agency a layer
names. ITS provides its layers as is, without warranty that they are error
free, complete or current; the Department of State's require citation of
the source and are not for legal, engineering, navigation or survey use;
DEC's Coastal Erosion Hazard Areas are a screening tool, the official map
being the one on file. Layers whose terms forbid redistribution are not
published here.

## What was changed here

One GeoParquet 2.0 file per layer. Folded into one file each, a `category`
column telling the source layers apart: the schools and education
facilities, the polling sites, the Natural Heritage important areas,
NYSDOT's material suppliers, the Thruway rest stops and the tax-credit
tracts; a `year` column for the South Shore benthic habitat surveys. The
tax parcels and the parcel centroids leave out the owner names and mailing
addresses ({", ".join(OWNER_COLUMNS)}). Other columns and values are the
agencies', unchanged; features without a geometry are left out (counted in
_manifest.json). The geometry is 2D, in NAD83 / UTM zone 18N (EPSG:26918),
the server projecting the layers published in another CRS, with a `bbox`
column declared as the GeoParquet covering, rows sorted along a Hilbert
curve. The PMTiles archives beside them are derived for web maps, in
lon/lat.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


PROFILE = arcgis_layers.Profile(
    name="ny", public=PUBLIC, layers_csv=LAYERS_CSV, servers=SERVERS, themes=THEMES,
    colors=COLORS, outline=OUTLINE, outline_crs=CRS, theme_title=theme_title,
    attribution=attribution, crs=CRS, drop_columns=DROP_COLUMNS)


def layers() -> list[dict]:
    return arcgis_layers.layers(PROFILE)


if __name__ == "__main__":
    arcgis_layers.main(PROFILE, __doc__)
