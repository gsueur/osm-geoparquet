#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the California layers.

  ca/catalog/catalog.json                        root: California
  ca/catalog/<theme>/catalog.json                one sub-catalog per theme
  ca/catalog/<theme>/<layer>/collection.json     one per layer

ca.py's THEMES and ca_layers.csv drive it, through arcgis_catalog.py (which
maine_catalog.py, nh_catalog.py and ny_catalog.py share). Counts, sizes,
checksums, titles and the departments' own descriptions come from each
theme's _manifest.json, local (--out-dir) or published. `updated` and
`checked` come from the `ca` file stamp (scripts/filedates.py). Spec:
portolan-spec v0.2.0. Validator: rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/ca_catalog.py build --dest build/ca-catalog [--out-dir out/ca]
  python3 scripts/datasets/ca_catalog.py check build/ca-catalog
  python3 scripts/datasets/ca_catalog.py publish --remote parquetry:parquetry/ca [--out-dir out/ca]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog import BUCKET, CONTACT_EMAIL, PUBLIC_BASE, S3_ENDPOINT, SITE_URL  # noqa: E402
from datasets import arcgis_catalog  # noqa: E402
from datasets.ca import CREDIT, PORTAL, PROFILE as LAYERS, THEMES  # noqa: E402

PUBLIC_DATA = f"{PUBLIC_BASE}/ca"
CATALOG_ID = "ca-gis"
TITLE = "California: statewide GIS layers"

LICENSE_LINK = {
    "rel": "license",
    "href": "https://creativecommons.org/licenses/by/4.0/",
    "type": "text/html",
    "title": "CC BY 4.0 for Fish and Wildlife's layers; the others as is, credit the department",
}
VIA_LINK = {
    "rel": "via",
    "href": PORTAL,
    "type": "text/html",
    "title": "The California State Geoportal, where the layers are listed",
}
PROVIDERS = [
    {
        "name": "California state departments",
        "description": "Fish and Wildlife, CAL FIRE, Caltrans, the Water Boards, Conservation, "
                       "the Board of Equalization, CDTFA, Education, State Parks, State Lands "
                       "and others, each producing the layers it publishes on the California "
                       "State Geoportal.",
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
    "Credit the department a layer names. Fish and Wildlife's layers are licensed CC BY 4.0; "
    "the other departments provide theirs as is, without warranty, most of them not for legal "
    "or survey use. Layers whose terms forbid redistribution or commercial use are not "
    "published here.")
TERMS = {"attribution": CREDIT, "license": LICENSE_LINK, "via": VIA_LINK,
         "providers": PROVIDERS, "license_md": LICENSE_MD, "keyword": "California",
         "sources": PORTAL}


def terms(theme: str) -> dict:
    return TERMS


def theme_about(theme: str) -> str:
    return f"California's statewide layers on {THEMES[theme].lower()}"


def root_description(themes: list[dict], total: int) -> str:
    return (
        f"Statewide vector layers of California, from the departments' services listed on "
        f"the California State Geoportal, republished as GeoParquet and PMTiles: {total} "
        f"collections in {len(themes)} themes, from fire hazard severity zones, tax rate "
        f"areas, Caltrans' road network and the Water Boards' groundwater layers to Fish "
        f"and Wildlife's species ranges, Areas of Conservation Emphasis and Marine "
        f"Protected Areas. Columns and values unchanged, every layer in NAD83 / California "
        f"Albers (EPSG:3310). Credit: {CREDIT}.")


def root_license_md(themes: list[dict]) -> str:
    return LICENSE_MD


def root_agents_md(themes: list[dict], listing: str) -> str:
    return f"""\
California's statewide layers as GeoParquet, one sub-catalog per theme.

{listing}

Layout: `{PUBLIC_DATA}/<theme>/<layer>.parquet`, anonymous S3 at
`s3://{BUCKET}/ca/` on `{S3_ENDPOINT}`. Coordinates are in EPSG:3310 (NAD83 /
California Albers, metres) for every layer: transform a lon/lat point before
filtering on `bbox`. Folded layers tell their sources apart with `category`
(e.g. the species in cwhr_species_ranges) or `year`. Source: the
California State Geoportal {PORTAL}. Credit: {CREDIT}."""


PROFILE = arcgis_catalog.CatalogProfile(
    layers=LAYERS, state="California", catalog_id=CATALOG_ID, title=TITLE, terms=terms,
    theme_about=theme_about, root_description=root_description,
    root_license_md=root_license_md, root_agents_md=root_agents_md,
    example=(-129_953, 63_312, "EPSG:3310", 2000,
             "within 2 km of the State Capitol in Sacramento (EPSG:3310)"))


if __name__ == "__main__":
    arcgis_catalog.main(PROFILE, __doc__)
