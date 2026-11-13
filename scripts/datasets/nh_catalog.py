#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the NH GRANIT layers.

  nh/catalog/catalog.json                        root: NH GRANIT
  nh/catalog/<theme>/catalog.json                one sub-catalog per theme
  nh/catalog/<theme>/<layer>/collection.json     one per layer

nh.py's THEMES and nh_layers.csv drive it, through arcgis_catalog.py (which
maine_catalog.py shares). Counts, sizes, checksums, titles and GRANIT's own
descriptions come from each theme's _manifest.json, local (--out-dir) or
published. `updated` and `checked` come from the `nh` file stamp
(scripts/filedates.py). Spec: portolan-spec v0.2.0. Validator: rashid,
through scripts/catalog.py.

Usage:
  python3 scripts/datasets/nh_catalog.py build --dest build/nh-catalog [--out-dir out/nh]
  python3 scripts/datasets/nh_catalog.py check build/nh-catalog
  python3 scripts/datasets/nh_catalog.py publish --remote parquetry:parquetry/nh [--out-dir out/nh]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog import BUCKET, CONTACT_EMAIL, PUBLIC_BASE, S3_ENDPOINT, SITE_URL  # noqa: E402
from datasets import arcgis_catalog  # noqa: E402
from datasets.nh import CREDIT, HOSTED, PORTAL, PROFILE as LAYERS, THEMES  # noqa: E402

PUBLIC_DATA = f"{PUBLIC_BASE}/nh"
CATALOG_ID = "nh-granit"
TITLE = "NH GRANIT: New Hampshire statewide GIS layers"

LICENSE_LINK = {
    "rel": "license",
    "href": PORTAL,
    "type": "text/html",
    "title": "NH GRANIT: cite NH GRANIT; most layers are not for legal use",
}
VIA_LINK = {
    "rel": "via",
    "href": PORTAL,
    "type": "text/html",
    "title": "The NH Geodata Portal (NH GRANIT), where the layers are published",
}
PROVIDERS = [
    {
        "name": "NH GRANIT",
        "description": "New Hampshire's statewide GIS clearinghouse, at the University of New "
                       "Hampshire. Publishes the state agencies' and partners' layers "
                       "(Revenue Administration's parcel mosaic, Transportation, Environmental "
                       "Services, Fish and Game, the coastal program and others) as ArcGIS "
                       "services on the NH Geodata Portal.",
        "url": PORTAL,
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
    "Cite NH GRANIT. Most of the portal's items ask for it (\"Please cite NH GRANIT\"), many "
    "say the data is not for legal use, and some name the agency that owns the data (the "
    "parcel mosaic: the NH Department of Revenue Administration). Credit NH GRANIT, and the "
    "agency a layer names, when you use it. The data is provided as is, without warranty.")
TERMS = {"attribution": CREDIT, "license": LICENSE_LINK, "via": VIA_LINK,
         "providers": PROVIDERS, "license_md": LICENSE_MD, "keyword": "NH GRANIT",
         "sources": HOSTED}


def terms(theme: str) -> dict:
    return TERMS


def theme_about(theme: str) -> str:
    return f"NH GRANIT's statewide layers on {THEMES[theme].lower()}"


def root_description(themes: list[dict], total: int) -> str:
    return (
        f"Statewide vector layers of New Hampshire, from NH GRANIT's services on the NH "
        f"Geodata Portal, republished as GeoParquet and PMTiles: {total} collections in "
        f"{len(themes)} themes, from the statewide parcel mosaic, districts, roads and "
        f"facilities to hydrography, soils, conserved lands, wildlife habitat and the "
        f"coast's sea-level-rise, groundwater and marsh migration scenarios. Columns and "
        f"values unchanged, every layer in NAD83(NSRS2007) / New Hampshire, US survey feet "
        f"(EPSG:3437). Credit: {CREDIT}.")


def root_license_md(themes: list[dict]) -> str:
    return LICENSE_MD


def root_agents_md(themes: list[dict], listing: str) -> str:
    return f"""\
NH GRANIT's statewide layers as GeoParquet, one sub-catalog per theme.

{listing}

Layout: `{PUBLIC_DATA}/<theme>/<layer>.parquet`, anonymous S3 at
`s3://{BUCKET}/nh/` on `{S3_ENDPOINT}`. Coordinates are in EPSG:3437 (New
Hampshire State Plane, US survey feet) for every layer: transform a lon/lat
point before filtering on `bbox`. Sources: {HOSTED} and the portal
{PORTAL}. Credit: {CREDIT}."""


PROFILE = arcgis_catalog.CatalogProfile(
    layers=LAYERS, state="New Hampshire", catalog_id=CATALOG_ID, title=TITLE, terms=terms,
    theme_about=theme_about, root_description=root_description,
    root_license_md=root_license_md, root_agents_md=root_agents_md,
    example=(1_018_669, 257_660, "EPSG:3437", 6562,
             "within 2 km (6,562 US survey feet) of the State House in Concord (EPSG:3437)"))


if __name__ == "__main__":
    arcgis_catalog.main(PROFILE, __doc__)
