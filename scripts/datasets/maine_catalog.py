#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the Maine GeoLibrary
layers.

  maine/catalog/catalog.json                        root: Maine GeoLibrary
  maine/catalog/<theme>/catalog.json                one sub-catalog per theme
  maine/catalog/<theme>/<layer>/collection.json     one per layer

maine.py's THEMES and maine_layers.csv drive it, through arcgis_catalog.py
(which nh_catalog.py shares), the same way as massgis_catalog.py. Each
collection has the layer's GeoParquet (`data`, its CRS declared with the
projection extension), its PMTiles registered as formats.md asks (a
`rel: pmtiles` link with `pmtiles:layers`, web-map-links v1.3.0, and the
file as a `visual` asset), a MapLibre style and the thumbnail the build
rendered. Counts, sizes, checksums, titles and the GeoLibrary's own
descriptions come from each theme's _manifest.json, local (--out-dir) or
published. `updated` and `checked` come from the `maine` file stamp
(scripts/filedates.py). Spec: portolan-spec v0.2.0. Validator: rashid,
through scripts/catalog.py.

Usage:
  python3 scripts/datasets/maine_catalog.py build --dest build/maine-catalog [--out-dir out/maine]
  python3 scripts/datasets/maine_catalog.py check build/maine-catalog
  python3 scripts/datasets/maine_catalog.py publish --remote parquetry:parquetry/maine [--out-dir out/maine]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog import BUCKET, CONTACT_EMAIL, PUBLIC_BASE, S3_ENDPOINT, SITE_URL  # noqa: E402
from datasets import arcgis_catalog  # noqa: E402
from datasets.maine import DEP, DEP_SITE, HUB, ORG, PROFILE as LAYERS, TERMS, THEMES  # noqa: E402

PUBLIC_DATA = f"{PUBLIC_BASE}/maine"
CATALOG_ID = "maine-geolibrary"
TITLE = "Maine GeoLibrary: Maine statewide GIS layers"
ATTRIBUTION = "State of Maine, Maine GeoLibrary (Maine Office of GIS) and the producing agencies"

LICENSE_LINK = {
    "rel": "license",
    "href": HUB,
    "type": "text/html",
    "title": f'Maine GeoLibrary terms: "{TERMS}" No other licence is stated.',
}
VIA_LINK = {
    "rel": "via",
    "href": HUB,
    "type": "text/html",
    "title": "The Maine GeoLibrary data catalog, where the layers are published",
}
PROVIDERS = [
    {
        "name": "Maine Office of GIS (MEGIS) and the Maine GeoLibrary",
        "description": "The State of Maine's GIS office and its public data library. "
                       "Publishes the state agencies' layers (Marine Resources, Inland "
                       "Fisheries and Wildlife, the Maine Geological Survey, Transportation, "
                       "the E911 Bureau and others) as ArcGIS feature services.",
        "url": HUB,
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted the layers to GeoParquet, tiled them, and maintains and "
                       "hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]
LICENSE_MD = (
    f'The Maine GeoLibrary\'s items state: "{TERMS}" There is no other licence statement. '
    "Credit the State of Maine, the Maine GeoLibrary and the agency each layer names when "
    "you use it. The data is provided as is, without warranty.")

# The dep theme: the Maine DEP's own map services.
DEP_ATTRIBUTION = "Maine Department of Environmental Protection"
DEP_LICENSE_LINK = {
    "rel": "license",
    "href": DEP_SITE,
    "type": "text/html",
    "title": (f'Maine DEP: no terms stated on its map services; the GeoLibrary\'s items state '
              f'"{TERMS}"'),
}
DEP_VIA_LINK = {
    "rel": "via",
    "href": DEP_SITE,
    "type": "text/html",
    "title": "Maine DEP GIS data and maps, where the layers are published",
}
DEP_PROVIDERS = [
    {
        "name": "Maine Department of Environmental Protection",
        "description": "Maine's environmental regulator. Publishes its sites, permits, "
                       "water quality, lakes, PFAS and seagrass layers as map services on "
                       "the State of Maine's ArcGIS Server.",
        "url": DEP_SITE,
        "roles": ["producer", "licensor"],
    },
    PROVIDERS[1],
]
DEP_LICENSE_MD = (
    "The Maine DEP's map services state no terms of use and no licence; their copyright text "
    "names the producer. The Maine GeoLibrary's items, including the DEP layers it lists, "
    f'state: "{TERMS}" Credit the Maine Department of Environmental Protection, and the '
    "partners a layer names, when you use it. The data is provided as is, without warranty.")


def terms(theme: str) -> dict:
    """Who publishes a theme and on what terms: the GeoLibrary, or the DEP."""
    if theme == "dep":
        return {"attribution": DEP_ATTRIBUTION, "license": DEP_LICENSE_LINK,
                "via": DEP_VIA_LINK, "providers": DEP_PROVIDERS, "license_md": DEP_LICENSE_MD,
                "keyword": "Maine DEP", "sources": DEP}
    return {"attribution": ATTRIBUTION, "license": LICENSE_LINK, "via": VIA_LINK,
            "providers": PROVIDERS, "license_md": LICENSE_MD, "keyword": "Maine GeoLibrary",
            "sources": ORG}


def theme_about(theme: str) -> str:
    if theme == "dep":
        return ("The Maine Department of Environmental Protection's own layers, read from its "
                "map services: regulated sites and spills, storage tanks, landfills, "
                "wastewater discharges, PFAS investigation, water quality assessment and "
                "classification, lakes, biomonitoring, seagrass surveys, dams and natural "
                "drainageways")
    return f"The Maine GeoLibrary's statewide layers on {THEMES[theme].lower()}"


def has_dep(themes: list[dict]) -> bool:
    return any(t["_theme"] == "dep" for t in themes)


def root_description(themes: list[dict], total: int) -> str:
    return (
        f"Statewide vector layers of the State of Maine, from the Maine GeoLibrary's "
        f"feature services, republished as GeoParquet and PMTiles: {total} collections in "
        f"{len(themes)} themes, from parcels, E911 addresses and roads to conserved lands, "
        f"wildlife habitat, marine fisheries, coastal hazards and geology"
        + (", and the Maine Department of Environmental Protection's own layers (sites, "
           "spills, tanks, PFAS, water quality, lakes, seagrass) from its map services"
           if has_dep(themes) else "")
        + f". Columns and values unchanged, in each layer's own CRS (UTM zone 19N, "
        f"EPSG:26919, for most). Credit: {ATTRIBUTION}"
        + (f"; {DEP_ATTRIBUTION} for its layers" if has_dep(themes) else "") + ".")


def root_license_md(themes: list[dict]) -> str:
    return LICENSE_MD + (f"\n\nThe `dep` theme: {DEP_LICENSE_MD}" if has_dep(themes) else "")


def root_agents_md(themes: list[dict], listing: str) -> str:
    return f"""\
The Maine GeoLibrary's statewide layers as GeoParquet, one sub-catalog per theme;
the `dep` theme is the Maine DEP's own layers, from its map services.

{listing}

Layout: `{PUBLIC_DATA}/<theme>/<layer>.parquet`, anonymous S3 at
`s3://{BUCKET}/maine/` on `{S3_ENDPOINT}`. Coordinates are in each
layer's CRS (EPSG:26919, UTM zone 19N metres, for most): transform a lon/lat
point before filtering on `bbox`. Sources: {ORG} and, for `dep`, {DEP}. Credit:
{ATTRIBUTION}; for `dep`, {DEP_ATTRIBUTION}."""


PROFILE = arcgis_catalog.CatalogProfile(
    layers=LAYERS, state="Maine", catalog_id=CATALOG_ID, title=TITLE, terms=terms,
    theme_about=theme_about, root_description=root_description,
    root_license_md=root_license_md, root_agents_md=root_agents_md,
    example=(437_652, 4_906_290, "EPSG:26919", 2000,
             "within 2 km of the State House in Augusta, in metres (EPSG:26919)"))


if __name__ == "__main__":
    arcgis_catalog.main(PROFILE, __doc__)
