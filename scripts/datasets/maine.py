#!/usr/bin/env python3
"""
The Maine GeoLibrary's statewide vector layers as GeoParquet and PMTiles.

The Maine Office of GIS (MEGIS) publishes the state agencies' layers through
the Maine GeoLibrary, as ArcGIS Online feature services
(https://mainegeolibrary-maine.hub.arcgis.com, organisation RbMX0mRVOFNTdLzd).
This reads each layer from its FeatureServer query endpoint and writes one
GeoParquet file per layer, and its PMTiles archive, grouped by theme:

  <out>/<theme>/<layer>.parquet     e.g. places/e911_roads.parquet
  <out>/<theme>/<layer>.pmtiles
  <out>/<theme>/<layer>.png         the catalog thumbnail
  <out>/<theme>/_manifest.json      rows, sizes, sha256, titles, sources, tile settings
  <out>/<theme>/ATTRIBUTION.txt

The Maine Department of Environmental Protection publishes its own layers
on the state's ArcGIS Server (https://gis.maine.gov/mapservices/rest/services/dep,
map services), most of which the GeoLibrary does not list: they are the
`dep` theme, read the same way.

maine_layers.csv says which layers: one row per source layer, its theme, its
server (empty: the GeoLibrary's feature services; `dep`: DEP's map
services), the service and layer number. Rows sharing an id are one
published layer, each source told apart by a column the CSV gives: `year`
(DMR's yearly shellfish P90 scores and NSSP classifications, DEP's seagrass
surveys and water classification networks), `category` (DEP's recycling
layers) or `from_year` and `to_year` (DEP's water classification changes).
The GeoLibrary also lists label layers, catalogue footprints, imagery
indexes, tables and copies, none of which are published here; nor are the
2-ft contours (about 1,000 tiles on gis.maine.gov).

Fetching: pages of the layer's maxRecordCount (at most 2,000), walked on the
object id (`OBJECTID > last`, ordered on it), in the layer's own spatial
reference (outSR), as Esri JSON read by GDAL, which keeps the field types
(dates as timestamps). The rows fetched must equal the layer's
returnCountOnly count, or the layer fails. Features without a geometry are
dropped and counted in the manifest.

Columns and values are the GeoLibrary's, unchanged. The geometry is 2D, in
the layer's own CRS (UTM zone 19N, EPSG:26919, for most; a few are in Web
Mercator, NAD83 or WGS 84 lon/lat, each declared in its file), with a `bbox`
column declared as the GeoParquet covering; rows are sorted on a Hilbert
curve and cut into row groups of at most 4 MiB of geometry. Writing, tiling
and thumbnails are massgis.py's; the fetch, the fingerprints and the
manifests are arcgis_layers.py's, which nh.py shares.

A build reads the published _source.json and rebuilds only the layers whose
fingerprint moved (each source's last data and schema edit and its count),
or the ones named with --layer. A GeoLibrary layer whose service gives no
edit date is rebuilt every run. DEP's map services give none at all: their
fingerprint is the count, the highest object id, the fields and the month,
so a DEP layer is rebuilt when rows are added or removed or its schema
changes, and at least once a month for edits in place. Either way
scripts/filedates.py keeps a file's date, and the file on the bucket, as
they were when its bytes did not change.

Usage:
  python3 scripts/datasets/maine.py upstream
  uv run --project scripts --with matplotlib python scripts/datasets/maine.py build \\
      --work-dir /Volumes/T9/maine/tmp --out-dir /Volumes/T9/maine/out [--layer e911_roads]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets import arcgis_layers  # noqa: E402

ORG = "https://services1.arcgis.com/RbMX0mRVOFNTdLzd/arcgis/rest/services"
HUB = "https://mainegeolibrary-maine.hub.arcgis.com"
DEP = "https://gis.maine.gov/mapservices/rest/services/dep"
DEP_SITE = "https://www.maine.gov/dep/gis/datamaps/"
# Where a CSV row's layer is read: its `server` column.
SERVERS = {
    "": {"url": ORG + "/{service}/FeatureServer/{layer}", "kind": "feature service",
         "publisher": "the Maine GeoLibrary", "undated": "daily"},
    "dep": {"url": DEP + "/{service}/MapServer/{layer}", "kind": "map service",
            "publisher": "the Maine Department of Environmental Protection", "undated": "monthly",
            # Its map services fail on large pages already: one reader.
            "workers": 1},
}
PUBLIC = "https://parquetry.geomermaids.com/maine"
LAYERS_CSV = Path(__file__).with_name("maine_layers.csv")
# The state outline the thumbnails are drawn over.
OUTLINE = ("boundaries", "state_boundary_polygon")
# The terms the GeoLibrary's items state, and nothing more.
TERMS = "Access: Public. Use: User accepts risk."
# Points too dense for one tile at MassGIS's zoom 14, even with one
# property and a 4 MB cap (MGS nearshore survey: about 1.07M soundings
# along survey tracks). Tiles go deeper so zoom 14 can be thinned.
TILE_MAX_ZOOM = {"nearshore_survey_bathymetric_elevation_points": 16}

THEMES = {
    "boundaries": "Boundaries, Districts and Parcels",
    "places": "Addresses, Roads and Facilities",
    "land": "Conservation, Lakes and Land Use",
    "habitat": "Wildlife and Habitat",
    "marine": "Marine Resources and Fisheries",
    "coast": "Coastal Geology and Hazards",
    "geology": "Geology, Minerals and Groundwater",
    "dep": "Environmental Protection (Maine DEP)",
}
COLORS = {
    "boundaries": "#8e44ad", "places": "#d35400", "land": "#5aa02c", "habitat": "#3f8a4a",
    "marine": "#2f7fc1", "coast": "#1b8a8a", "geology": "#9a6b3a", "dep": "#c0392b",
}


def theme_title(theme: str) -> str:
    """A theme's title with its publisher: the GeoLibrary, or the DEP."""
    if theme == "dep":
        return "Maine DEP: Environmental Protection"
    return f"Maine GeoLibrary: {THEMES[theme]}"


def attribution(theme: str, entries: list[dict]) -> str:
    lines = "\n".join(f"  {e['name']:<44} {e['source']['urls'][0]}" for e in entries)
    if theme == "dep":
        return dep_attribution(lines)
    return f"""\
# Data attribution

These layers are published by the State of Maine through the Maine
GeoLibrary, maintained by the Maine Office of GIS (MEGIS), for the state
agencies that produce them (Marine Resources, Inland Fisheries and Wildlife,
the Maine Geological Survey, Agriculture, Conservation and Forestry's
Natural Areas Program and Beginning with Habitat, Transportation, the E911
Bureau and others):

  GeoLibrary: {HUB}
  Services:   {ORG}
  Producer:   State of Maine

{THEMES[theme]}: each layer and the feature service layer it is read from
(each one's last edit and count are in _manifest.json):

{lines}

Terms: the GeoLibrary's items state "{TERMS}" There is no other licence
statement. Credit the State of Maine, the Maine GeoLibrary and the agency
each layer names when you use it. The data is provided as is, without
warranty.

## What was changed here

One GeoParquet 2.0 file per layer (the shellfish P90 scores and NSSP
classifications, published by DMR one layer a year, are one file each with
a `year` column). Columns and values are the GeoLibrary's, unchanged;
features without a geometry are left out (counted in _manifest.json). The
geometry is 2D, in the layer's own CRS as published, with a `bbox` column
declared as the GeoParquet covering, rows sorted along a Hilbert curve. The
PMTiles archives beside them are derived for web maps, in lon/lat.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


def dep_attribution(lines: str) -> str:
    return f"""\
# Data attribution

These layers are published by the Maine Department of Environmental
Protection (DEP) on the State of Maine's ArcGIS Server:

  Services:   {DEP}
  Data page:  {DEP_SITE}
  Producer:   Maine Department of Environmental Protection

{THEMES["dep"]}: each layer and the map service layer it is read from (each
one's count, highest object id and the producer named in its copyright text
are in _manifest.json):

{lines}

Terms: DEP's map services state no terms of use and no licence; their
copyright text names the producer (the Maine DEP, with partners for some
layers, such as the seagrass surveys). The Maine GeoLibrary's items,
including the DEP layers it lists, state "{TERMS}" Credit the Maine
Department of Environmental Protection, and the partners a layer names, when
you use it. The data is provided as is, without warranty.

## What was changed here

One GeoParquet 2.0 file per layer. Folded into one file each: the seagrass
and eelgrass surveys (2013 to 2025, a `year` column), the recycling and
redemption locations (eight layers, a `category` column: the layer's name),
the water classification of rivers and streams (eleven years from 1965 to
2019, `year`) and its changes between two classifications (`from_year`,
`to_year`). Columns and values are DEP's, unchanged; features without a
geometry are left out (counted in _manifest.json). The geometry is 2D, in
the layer's own CRS as published (UTM zone 19N or WGS 84), with a `bbox`
column declared as the GeoParquet covering, rows sorted along a Hilbert
curve. The PMTiles archives beside them are derived for web maps, in
lon/lat.

This data is packaged and hosted by Geomermaids:
  https://geoparquet.geomermaids.com/
  contact: gsueur@geomermaids.com
"""


PROFILE = arcgis_layers.Profile(
    name="maine", public=PUBLIC, layers_csv=LAYERS_CSV, servers=SERVERS, themes=THEMES,
    colors=COLORS, outline=OUTLINE, outline_crs="EPSG:26919", theme_title=theme_title,
    attribution=attribution, tile_max_zoom=TILE_MAX_ZOOM)


def layers() -> list[dict]:
    return arcgis_layers.layers(PROFILE)


if __name__ == "__main__":
    arcgis_layers.main(PROFILE, __doc__)
