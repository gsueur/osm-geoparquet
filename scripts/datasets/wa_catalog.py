#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the Washington layers.

  wa/catalog/catalog.json                        root: Washington
  wa/catalog/<theme>/catalog.json                one sub-catalog per theme
  wa/catalog/<theme>/<layer>/collection.json     one per layer

wa.py's THEMES and wa_layers.csv drive it, through arcgis_catalog.py (which
maine_catalog.py, nh_catalog.py, ny_catalog.py and ca_catalog.py share). Counts, sizes,
checksums, titles and the departments' own descriptions come from each
theme's _manifest.json, local (--out-dir) or published. `updated` and
`checked` come from the `wa` file stamp (scripts/filedates.py). Spec:
portolan-spec v0.2.0. Validator: rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/wa_catalog.py build --dest build/wa-catalog [--out-dir out/wa]
  python3 scripts/datasets/wa_catalog.py check build/wa-catalog
  python3 scripts/datasets/wa_catalog.py publish --remote parquetry:parquetry/wa [--out-dir out/wa]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog import BUCKET, CONTACT_EMAIL, PUBLIC_BASE, S3_ENDPOINT, SITE_URL  # noqa: E402
from datasets import arcgis_catalog  # noqa: E402
from datasets.wa import CREDIT, PORTAL, PROFILE as LAYERS, THEMES  # noqa: E402

PUBLIC_DATA = f"{PUBLIC_BASE}/wa"
CATALOG_ID = "wa-gis"
TITLE = "Washington: statewide GIS layers"

LICENSE_LINK = {
    "rel": "license",
    "href": "https://geo.wa.gov/",
    "type": "text/html",
    "title": "As is, credit the agency; each layer's terms are on the Washington Geospatial Open Data Portal",
}
VIA_LINK = {
    "rel": "via",
    "href": PORTAL,
    "type": "text/html",
    "title": "The Washington Geospatial Open Data Portal, where the layers are listed",
}
PROVIDERS = [
    {
        "name": "Washington state agencies",
        "description": "Transportation, Natural Resources, Ecology, Fish and Wildlife, State "
                       "Parks, Health, Emergency Management, Financial Management, the "
                       "Geospatial Portal and others, each producing the layers it publishes on "
                       "the Washington Geospatial Open Data Portal.",
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
    "Credit the agency a layer names. The agencies provide their layers as is, without "
    "warranty; WSDOT's are for planning use only, and its terms ask users to hold WSDOT "
    "harmless for their use of the data. Layers whose terms forbid redistribution or "
    "commercial use are not published here.")
TERMS = {"attribution": CREDIT, "license": LICENSE_LINK, "via": VIA_LINK,
         "providers": PROVIDERS, "license_md": LICENSE_MD, "keyword": "Washington",
         "sources": PORTAL}


def terms(theme: str) -> dict:
    return TERMS


def theme_about(theme: str) -> str:
    return f"Washington's statewide layers on {THEMES[theme].lower()}"


def root_description(themes: list[dict], total: int) -> str:
    return (
        f"Statewide vector layers of Washington, from the agencies' services listed on "
        f"the Washington Geospatial Open Data Portal, republished as GeoParquet and PMTiles: "
        f"{total} collections in {len(themes)} themes, from the statewide tax parcels (without "
        f"owner names), WSDOT's state routes, roadway data and traffic counts, Natural "
        f"Resources' geologic hazards and forest practices layers to Ecology's water rights "
        f"and wells and Fish and Wildlife's hunting units. Columns and values unchanged, "
        f"every layer in NAD83(HARN) / Washington South (ftUS) (EPSG:2927). Credit: {CREDIT}.")


def root_license_md(themes: list[dict]) -> str:
    return LICENSE_MD


def root_agents_md(themes: list[dict], listing: str) -> str:
    return f"""\
Washington's statewide layers as GeoParquet, one sub-catalog per theme.

{listing}

Layout: `{PUBLIC_DATA}/<theme>/<layer>.parquet`, anonymous S3 at
`s3://{BUCKET}/wa/` on `{S3_ENDPOINT}`. Coordinates are in EPSG:2927
(NAD83(HARN) / Washington South, US survey feet) for every layer: transform a
lon/lat point before filtering on `bbox`, and give distances in feet. Folded
layers tell their sources apart with `year` (traffic_counts_history) or
`category` (the stormwater assets, the facilities). Source: the Washington
Geospatial Open Data Portal {PORTAL}. Credit: {CREDIT}."""


PROFILE = arcgis_catalog.CatalogProfile(
    layers=LAYERS, state="Washington", catalog_id=CATALOG_ID, title=TITLE, terms=terms,
    theme_about=theme_about, root_description=root_description,
    root_license_md=root_license_md, root_agents_md=root_agents_md,
    example=(1_040_935, 630_014, "EPSG:2927", 6562,
             "within 2 km (6,562 US survey feet) of the State Capitol in Olympia (EPSG:2927)"))


if __name__ == "__main__":
    arcgis_catalog.main(PROFILE, __doc__)
