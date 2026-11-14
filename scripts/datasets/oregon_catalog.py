#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the Oregon layers.

  or/catalog/catalog.json                        root: Oregon
  or/catalog/<theme>/catalog.json                one sub-catalog per theme
  or/catalog/<theme>/<layer>/collection.json     one per layer

oregon.py's THEMES and oregon_layers.csv drive it, through arcgis_catalog.py (which
maine_catalog.py, nh_catalog.py, ny_catalog.py, ca_catalog.py and wa_catalog.py share). Counts, sizes,
checksums, titles and the departments' own descriptions come from each
theme's _manifest.json, local (--out-dir) or published. `updated` and
`checked` come from the `or` file stamp (scripts/filedates.py). Spec:
portolan-spec v0.2.0. Validator: rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/oregon_catalog.py build --dest build/or-catalog [--out-dir out/or]
  python3 scripts/datasets/oregon_catalog.py check build/or-catalog
  python3 scripts/datasets/oregon_catalog.py publish --remote parquetry:parquetry/or [--out-dir out/or]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog import BUCKET, CONTACT_EMAIL, PUBLIC_BASE, S3_ENDPOINT, SITE_URL  # noqa: E402
from datasets import arcgis_catalog  # noqa: E402
from datasets.oregon import CREDIT, PORTAL, PROFILE as LAYERS, THEMES  # noqa: E402

PUBLIC_DATA = f"{PUBLIC_BASE}/or"
CATALOG_ID = "or-gis"
TITLE = "Oregon: statewide GIS layers"

LICENSE_LINK = {
    "rel": "license",
    "href": "https://geohub.oregon.gov/pages/terms-of-use",
    "type": "text/html",
    "title": "Oregon GEOHub terms of use: any lawful use of public data, commercial use included",
}
VIA_LINK = {
    "rel": "via",
    "href": PORTAL,
    "type": "text/html",
    "title": "The Oregon GEOHub, where the layers are listed",
}
PROVIDERS = [
    {
        "name": "Oregon state agencies",
        "description": "Transportation, Water Resources, Fish and Wildlife, Geology and Mineral "
                       "Industries, Land Conservation and Development, Agriculture, State Lands, "
                       "Parks and Recreation, the Geospatial Enterprise Office and others, each "
                       "producing the layers it publishes on the Oregon GEOHub.",
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
    "Credit the agency a layer names. The Oregon GEOHub's terms of use allow any lawful use of "
    "its public data, commercial use included; the agencies provide their layers as is, without "
    "warranty, for information and not for legal, engineering or survey use.")
TERMS = {"attribution": CREDIT, "license": LICENSE_LINK, "via": VIA_LINK,
         "providers": PROVIDERS, "license_md": LICENSE_MD, "keyword": "Oregon",
         "sources": PORTAL}


def terms(theme: str) -> dict:
    return TERMS


def theme_about(theme: str) -> str:
    return f"Oregon's statewide layers on {THEMES[theme].lower()}"


def root_description(themes: list[dict], total: int) -> str:
    return (
        f"Statewide vector layers of Oregon, from the agencies' services listed on the Oregon "
        f"GEOHub, republished as GeoParquet and PMTiles: {total} collections in {len(themes)} "
        f"themes, from address points, ODOT's public roads and urban growth boundaries to "
        f"Water Resources' water rights and wells, Fish and Wildlife's fish habitat "
        f"distribution and DOGAMI's faults and landslides. Columns and values unchanged, "
        f"every layer in NAD83(HARN) / Oregon GIC Lambert (ft) (EPSG:2992). Credit: {CREDIT}.")


def root_license_md(themes: list[dict]) -> str:
    return LICENSE_MD


def root_agents_md(themes: list[dict], listing: str) -> str:
    return f"""\
Oregon's statewide layers as GeoParquet, one sub-catalog per theme.

{listing}

Layout: `{PUBLIC_DATA}/<theme>/<layer>.parquet`, anonymous S3 at
`s3://{BUCKET}/or/` on `{S3_ENDPOINT}`. Coordinates are in EPSG:2992
(NAD83(HARN) / Oregon GIC Lambert, international feet) for every layer:
transform a lon/lat point before filtering on `bbox`, and give distances in
feet. Folded layers tell their sources apart with `category` (the species in
fish_habitat_distribution_streams, source and use in the water rights).
Source: the Oregon GEOHub {PORTAL}. Credit: {CREDIT}."""


PROFILE = arcgis_catalog.CatalogProfile(
    layers=LAYERS, state="Oregon", catalog_id=CATALOG_ID, title=TITLE, terms=terms,
    theme_about=theme_about, root_description=root_description,
    root_license_md=root_license_md, root_agents_md=root_agents_md,
    example=(657_328, 1_172_354, "EPSG:2992", 6562,
             "within 2 km (6,562 feet) of the State Capitol in Salem (EPSG:2992)"))


if __name__ == "__main__":
    arcgis_catalog.main(PROFILE, __doc__)
