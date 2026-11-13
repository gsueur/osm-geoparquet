#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the New York State
layers.

  ny/catalog/catalog.json                        root: New York State
  ny/catalog/<theme>/catalog.json                one sub-catalog per theme
  ny/catalog/<theme>/<layer>/collection.json     one per layer

ny.py's THEMES and ny_layers.csv drive it, through arcgis_catalog.py (which
maine_catalog.py and nh_catalog.py share). Counts, sizes, checksums, titles
and the agencies' own descriptions come from each theme's _manifest.json,
local (--out-dir) or published. `updated` and `checked` come from the `ny`
file stamp (scripts/filedates.py). Spec: portolan-spec v0.2.0. Validator:
rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/ny_catalog.py build --dest build/ny-catalog [--out-dir out/ny]
  python3 scripts/datasets/ny_catalog.py check build/ny-catalog
  python3 scripts/datasets/ny_catalog.py publish --remote parquetry:parquetry/ny [--out-dir out/ny]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog import BUCKET, CONTACT_EMAIL, PUBLIC_BASE, S3_ENDPOINT, SITE_URL  # noqa: E402
from datasets import arcgis_catalog  # noqa: E402
from datasets.ny import CREDIT, PORTAL, PROFILE as LAYERS, THEMES  # noqa: E402

PUBLIC_DATA = f"{PUBLIC_BASE}/ny"
CATALOG_ID = "ny-gis"
TITLE = "New York State: statewide GIS layers"

LICENSE_LINK = {
    "rel": "license",
    "href": PORTAL,
    "type": "text/html",
    "title": "NYS GIS Clearinghouse: credit the agency; provided as is",
}
VIA_LINK = {
    "rel": "via",
    "href": PORTAL,
    "type": "text/html",
    "title": "The NYS GIS Clearinghouse, where the layers are listed",
}
PROVIDERS = [
    {
        "name": "New York State Office of Information Technology Services, Geospatial Services",
        "description": "Runs the NYS GIS Clearinghouse and publishes the state's framework "
                       "layers: tax parcels, address points, streets, civil boundaries, "
                       "schools.",
        "url": PORTAL,
        "roles": ["producer", "licensor"],
    },
    {
        "name": "New York State agencies",
        "description": "The Departments of State, Environmental Conservation and "
                       "Transportation, the Thruway Authority, OPRHP and the New York Natural "
                       "Heritage Program, each producing the layers it publishes.",
        "url": PORTAL,
        "roles": ["producer"],
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
    "Credit the New York State GIS Clearinghouse and the agency a layer names. ITS provides "
    "its layers as is, without warranty that they are error free, complete or current; the "
    "Department of State's require citation of the source and are not for legal, "
    "engineering, navigation or survey use. Layers whose terms forbid redistribution are not "
    "published here. The tax parcels and parcel centroids leave out the owner names and "
    "mailing addresses.")
TERMS = {"attribution": CREDIT, "license": LICENSE_LINK, "via": VIA_LINK,
         "providers": PROVIDERS, "license_md": LICENSE_MD, "keyword": "New York State",
         "sources": PORTAL}


def terms(theme: str) -> dict:
    return TERMS


def theme_about(theme: str) -> str:
    return f"New York State's statewide layers on {THEMES[theme].lower()}"


def root_description(themes: list[dict], total: int) -> str:
    return (
        f"Statewide vector layers of New York State, from the agencies' services listed on "
        f"the NYS GIS Clearinghouse, republished as GeoParquet and PMTiles: {total} "
        f"collections in {len(themes)} themes, from tax parcels, address points, streets and "
        f"civil boundaries to NYSDOT's roadway inventory and bridges, wetlands, coastal "
        f"erosion hazard areas and the Department of State's waterfront programs. Columns "
        f"and values unchanged but for the parcel owner names, every layer in NAD83 / UTM "
        f"zone 18N (EPSG:26918). Credit: {CREDIT}.")


def root_license_md(themes: list[dict]) -> str:
    return LICENSE_MD


def root_agents_md(themes: list[dict], listing: str) -> str:
    return f"""\
New York State's statewide layers as GeoParquet, one sub-catalog per theme.

{listing}

Layout: `{PUBLIC_DATA}/<theme>/<layer>.parquet`, anonymous S3 at
`s3://{BUCKET}/ny/` on `{S3_ENDPOINT}`. Coordinates are in EPSG:26918 (NAD83
/ UTM zone 18N, metres) for every layer: transform a lon/lat point before
filtering on `bbox`. Tax parcel polygons cover only the 38 counties that let
ITS share them; the parcel centroids cover all 62. Neither has owner names.
Source: the NYS GIS Clearinghouse {PORTAL}. Credit: {CREDIT}."""


PROFILE = arcgis_catalog.CatalogProfile(
    layers=LAYERS, state="New York", catalog_id=CATALOG_ID, title=TITLE, terms=terms,
    theme_about=theme_about, root_description=root_description,
    root_license_md=root_license_md, root_agents_md=root_agents_md,
    example=(601_884, 4_722_987, "EPSG:26918", 2000,
             "within 2 km of the State Capitol in Albany (EPSG:26918)"))


if __name__ == "__main__":
    arcgis_catalog.main(PROFILE, __doc__)
