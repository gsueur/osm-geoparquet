#!/usr/bin/env python3
"""
California's CWHR predicted habitat rasters as COGs, published to
parquetry:parquetry/ca/rasters/ (scripts/datasets/rasters.py is the engine).

The California Department of Fish and Wildlife's California Wildlife Habitat
Relationships (CWHR) models: for each species, the predicted habitat
suitability of every 30 m cell, from the CWHR habitat type and its size and
density classes. One raster per species, one theme per taxon group
(amphibians, birds, mammals, reptiles).

Source: the species' archive in CDFW's file library (filelib.wildlife.ca.gov,
`ds####.zip`), listed in the gis.data.ca.gov DCAT feed. The image services
the feed lists beside them are tiled caches that refuse exportImage, so the
archives are read instead. Each holds a GeoTIFF (UInt16, 30 m, mostly WGS 84
/ Pseudo-Mercator, a few California Albers) and its value table: a pixel
value is a key into the table (CWHR habitat type, size and density, the
suitability `Suit_Max` and its rank `Suit_Rank`, Low, Medium or High, and
the colour of that rank). The COG keeps the publisher's grid and values; the
table is `<item>.classes.parquet`.

ca_rasters_items.csv lists the 517 species whose archive is public and whose
licence is CC BY 4.0 (feed of 2026-11-15). Out: the 206 sensitive species
CDFW withholds (no archive, and the feed's licence field holds the model
disclaimer instead), and 7 with no archive. The feed pointed three species to
another species' archive (a009, a010, m156); the CSV points them to their
own (`ds` id of the item).

Usage:
  python3 scripts/datasets/ca_rasters.py build --work-dir W --out-dir O \\
      [--item a001_california_tiger_salamander] [--publish parquetry:parquetry/ca/rasters]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import rasters
from rasters_catalog import RasterCatalog

from catalog import CONTACT_EMAIL, SITE_URL

PREFIX = "ca/rasters"
PUBLIC = f"https://parquetry.geomermaids.com/{PREFIX}"

CWHR = "https://wildlife.ca.gov/Data/CWHR"
CATALOG = RasterCatalog(
    catalog_id="ca-rasters",
    title="California: CWHR predicted habitat rasters",
    state="California", keyword="CDFW", keywords=("CWHR", "habitat suitability"),
    description=(
        "The California Department of Fish and Wildlife's California Wildlife Habitat "
        "Relationships (CWHR) predicted habitat: for each species, the habitat suitability of "
        "every 30 m cell, as Cloud-Optimized GeoTIFFs on CDFW's grid, each with its value "
        "table. Sensitive species CDFW withholds are not included."),
    theme_about={t: f"CWHR predicted habitat of California's {t}"
                 for t in ("amphibians", "birds", "mammals", "reptiles")},
    providers=[
        {"name": "California Department of Fish and Wildlife",
         "description": "Builds and publishes the CWHR System's predicted habitat models, in "
                        "its BIOS file library.",
         "url": CWHR, "roles": ["producer", "licensor"]},
        {"name": "Geomermaids",
         "description": "Converted the rasters to Cloud-Optimized GeoTIFFs on the publisher's "
                        "grid, and maintains and hosts this catalog and its data.",
         "url": SITE_URL, "email": CONTACT_EMAIL, "roles": ["processor", "host"]},
    ],
    license_link={"rel": "license", "href": "https://creativecommons.org/licenses/by/4.0/",
                  "type": "text/html", "title": "CC BY 4.0"},
    via_link={"rel": "via", "href": CWHR, "type": "text/html",
              "title": "CDFW's California Wildlife Habitat Relationships program"},
    spdx="CC-BY-4.0",
    license_md=(
        "CC BY 4.0, as CDFW publishes these models. Credit \"California Department of Fish and "
        "Wildlife, California Wildlife Habitat Relationships (CWHR) System\". CDFW notes that "
        "the predicted habitat suitability is limited by the accuracy and scale of its input "
        "data: a model, not a survey of where a species occurs."),
    classes_md=(
        "A cell value is a key into the value table: the cell's CWHR habitat type "
        "(`WHRTYPE`, `WHRNAME`), size and density classes (`WHRSIZE`, `WHRDENSITY`), and the "
        "species' predicted suitability there (`Suit_Max`, 0 to 1, and its rank `Suit_Rank`, "
        "Low, Medium or High, the three colours of the COG). Join on `Value`. A few species "
        "come from CDFW's older model, with `MEAN` (0 to 1) instead of `Suit_Max` and no rank: "
        "their `Suit_Rank_derived` is Geomermaids' (see the collection's note)."),
)

# CDFW's legend.clr, the same three lines in every archive that has one; the
# archives read from their geodatabase have none.
LEGEND = {"Low": (245, 245, 135), "Medium": (204, 138, 8), "High": (0, 102, 0)}

OLDER_MODEL = (
    "CDFW publishes this species from an older CWHR model: its value table gives the mean "
    "predicted suitability (`MEAN`, 0 to 1) but no rank. `Suit_Rank_derived` is Geomermaids', "
    "from `MEAN` with the cut points of CDFW's current models (Low up to 0.33, Medium up to "
    "0.66, High above), and gives the COG its colours. The older model's value table lists "
    "only part of the raster's values: a cell whose value it does not list has no attributes "
    "in CDFW's data and is drawn grey.")


def older_model_ranks(rat: list[dict]) -> tuple[list[dict], str | None]:
    """CDFW's older models (2016 to 2018 archives) have MEAN and no
    Suit_Rank: the rank from MEAN, at the current models' cut points (Low
    0.08-0.33, Medium 0.35-0.66, High 0.68-1.0 in their tables)."""
    keys = {k.lower(): k for k in rat[0]}
    if "suit_rank" in keys or "mean" not in keys:
        return rat, None

    def rank(v) -> str:
        v = round(float(v), 2)
        return "Low" if v <= 0.33 else "Medium" if v <= 0.66 else "High"

    return [{**row, "Suit_Rank_derived": rank(row[keys["mean"]])} for row in rat], OLDER_MODEL


# Archives with nothing to read (2026-11-15): still tried every week.
BROKEN = {
    "a013_southern_california_slender_salamander": "CDFW's archive is an empty zip (22 bytes)",
    "b240_marbled_murrelet": "CDFW's archive answers 404",
    "b413_bell_s_vireo": "CDFW's archive answers 404",
    # Older-model geodatabases with no value table: habitat keys, no
    # suitability.
    "b129_peregrine_falcon": "CDFW's raster has no value table",
    "r105_northern_three_lined_boa": "CDFW's raster has no value table",
}

PROFILE = rasters.Profile(
    name="ca-rasters", prefix=PREFIX, public=PUBLIC,
    items_csv=Path(__file__).with_name("ca_rasters_items.csv"),
    themes={"amphibians": "Amphibians", "birds": "Birds", "mammals": "Mammals",
            "reptiles": "Reptiles"},
    attribution="California Department of Fish and Wildlife, California Wildlife Habitat "
                "Relationships (CWHR) System",
    licence="CC-BY-4.0", licence_url="https://creativecommons.org/licenses/by/4.0/",
    # The archives' GeoTIFFs say 65535; a few leave it unset.
    nodata={"*": 65535},
    # The value tables' colour columns are unset; legend.clr colours the
    # three suitability ranks (the older models: the rank derived from MEAN).
    colour_by=("Suit_Rank", "Suit_Rank_derived"), legend=LEGEND,
    rat_hook=older_model_ranks, unlisted_colour=(190, 190, 190), require_classes=True,
    broken=BROKEN,
    catalog=CATALOG,
)

if __name__ == "__main__":
    rasters.main(PROFILE, __doc__)
