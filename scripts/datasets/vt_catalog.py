#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the Vermont layers.

  vt/catalog/catalog.json                        root: Vermont
  vt/catalog/<theme>/catalog.json                one sub-catalog per theme
  vt/catalog/<theme>/<layer>/collection.json     one per layer

vt.py's THEMES and vt_layers.csv drive it, through arcgis_catalog.py (which
maine_catalog.py, nh_catalog.py, ny_catalog.py, ca_catalog.py, wa_catalog.py
and oregon_catalog.py share). Counts, sizes, checksums, titles and the
agencies' own descriptions come from each theme's _manifest.json, local
(--out-dir) or published. `updated` and `checked` come from the `vt` file
stamp (scripts/filedates.py). The layers vt_layers.csv marks CC-BY-SA-4.0
(their publisher's licence) say so in their collection. Spec: portolan-spec
v0.3.0. Validator: rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/vt_catalog.py build --dest build/vt-catalog [--out-dir out/vt]
  python3 scripts/datasets/vt_catalog.py check build/vt-catalog
  python3 scripts/datasets/vt_catalog.py publish --remote parquetry:parquetry/vt [--out-dir out/vt]
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from catalog import BUCKET, CONTACT_EMAIL, PUBLIC_BASE, S3_ENDPOINT, SITE_URL  # noqa: E402
from datasets import arcgis_catalog  # noqa: E402
from datasets.vt import CREDIT, LAYERS_CSV, PORTAL, PROFILE as LAYERS, THEMES  # noqa: E402

PUBLIC_DATA = f"{PUBLIC_BASE}/vt"
CATALOG_ID = "vt-gis"
TITLE = "Vermont: statewide GIS layers"

LICENSE_LINK = {
    "rel": "license",
    "href": PORTAL,
    "type": "text/html",
    "title": "Vermont Open Geodata Portal: each layer's terms are on its page",
}
BY_SA_LINK = {
    "rel": "license",
    "href": "https://creativecommons.org/licenses/by-sa/4.0/",
    "type": "text/html",
    "title": "Creative Commons Attribution-ShareAlike 4.0 International",
}
VIA_LINK = {
    "rel": "via",
    "href": PORTAL,
    "type": "text/html",
    "title": "The Vermont Open Geodata Portal, where the layers are listed",
}
PROVIDERS = [
    {
        "name": "Vermont state agencies",
        "description": "The Vermont Center for Geographic Information (VCGI), the E911 Board, "
                       "the Agency of Natural Resources and its departments, the Geological "
                       "Survey, the Agency of Transportation, Public Service, Health and others, "
                       "each producing the layers it publishes on the Vermont Open Geodata "
                       "Portal; the towns' zoning maps by the regional planning commissions.",
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
    "warranty, and not for legal, engineering or survey use. Layers whose publisher marks them "
    "Creative Commons Attribution-ShareAlike 4.0 (most of the Agency of Natural Resources' and "
    "the Geological Survey's) stay under that licence, and their collections say so.")
BY_SA_MD = (
    "Creative Commons Attribution-ShareAlike 4.0 International "
    "(https://creativecommons.org/licenses/by-sa/4.0/), as its publisher marks it: credit the "
    "agency the layer names and share what you derive from it under the same licence. The "
    "agency provides it as is, without warranty, and not for legal, engineering or survey use.")
TERMS = {"attribution": CREDIT, "license": LICENSE_LINK, "via": VIA_LINK,
         "providers": PROVIDERS, "license_md": LICENSE_MD, "keyword": "Vermont",
         "sources": PORTAL}
BY_SA = {**TERMS, "license": BY_SA_LINK, "license_md": BY_SA_MD, "spdx": "CC-BY-SA-4.0"}


def by_sa_layers() -> frozenset[str]:
    with LAYERS_CSV.open() as f:
        return frozenset(r["id"] for r in csv.DictReader(f) if r.get("license") == "CC-BY-SA-4.0")


BY_SA_LAYERS = by_sa_layers()


def terms(theme: str) -> dict:
    return TERMS


def layer_terms(theme: str, name: str) -> dict | None:
    return BY_SA if name in BY_SA_LAYERS else None


def theme_about(theme: str) -> str:
    return f"Vermont's statewide layers on {THEMES[theme].lower()}"


def root_description(themes: list[dict], total: int) -> str:
    return (
        f"Statewide vector layers of Vermont, from the agencies' services listed on the Vermont "
        f"Open Geodata Portal, republished as GeoParquet and PMTiles: {total} collections in "
        f"{len(themes)} themes, from the statewide parcels, the E911 address points and roads "
        f"and the towns' zoning maps to VTrans' traffic counts and bridges, BioFinder, the "
        f"wetlands, the bedrock and surficial geology and the broadband status. Columns and "
        f"values unchanged but for the parcels' owner names, every layer in NAD83 / Vermont "
        f"(EPSG:32145). Credit: {CREDIT}.")


def root_license_md(themes: list[dict]) -> str:
    return LICENSE_MD


def root_agents_md(themes: list[dict], listing: str) -> str:
    return f"""\
Vermont's statewide layers as GeoParquet, one sub-catalog per theme.

{listing}

Layout: `{PUBLIC_DATA}/<theme>/<layer>.parquet`, anonymous S3 at
`s3://{BUCKET}/vt/` on `{S3_ENDPOINT}`. Coordinates are in EPSG:32145
(NAD83 / Vermont, metres) for every layer: transform a lon/lat point before
filtering on `bbox`, and give distances in metres. Folded layers tell their
sources apart with `category` (the town and map in zoning, "Waterbury:
Zoning districts"; each town's own columns are in other_attributes, JSON) or
`year` (broadband_status). The parcels have no owner names. Layers marked
CC BY-SA 4.0 in their collection carry that licence. Source: the Vermont
Open Geodata Portal {PORTAL}. Credit: {CREDIT}."""


PROFILE = arcgis_catalog.CatalogProfile(
    layers=LAYERS, state="Vermont", catalog_id=CATALOG_ID, title=TITLE, terms=terms,
    theme_about=theme_about, root_description=root_description,
    root_license_md=root_license_md, root_agents_md=root_agents_md,
    example=(493_564, 195_776, "EPSG:32145", 2000,
             "within 2 km of the State House in Montpelier (EPSG:32145)"),
    layer_terms=layer_terms)


if __name__ == "__main__":
    arcgis_catalog.main(PROFILE, __doc__)
