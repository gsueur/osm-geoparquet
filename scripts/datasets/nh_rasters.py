#!/usr/bin/env python3
"""
New Hampshire's thematic rasters as COGs, published to
parquetry:parquetry/nh/rasters/ (scripts/datasets/rasters.py is the engine).

Read from NH GRANIT's image services (nhgeodata.unh.edu/image) with
exportImage at each service's own grid: the services have no Download.

- wap_2025_habitat_land_cover: the NH Wildlife Action Plan 2025 habitat land
  cover (NH Fish and Game and its partners), 10 m, NH State Plane in US
  feet, UInt16. A pixel value is a key into the value table (1,006 rows: the
  habitat, 23 of them, and the NatureServe ecological system, NETH); the
  table is `<item>.classes.parquet`. The same layer is in the vector dataset
  as polygons (nh/environment/wap_2025_habitat_land_cover).

Out, as PLAN-rasters.md says: the statewide 2 m nDSM (11 billion pixels) and
NAIP (linked from the root catalog). The canopy height metrics are polygons.

The colours are those of the habitat land cover feature service's renderer
(hosting/rest/services/Hosted/EC_NHWAP/FeatureServer/1); three habitats it
draws with picture fills (grassland, lowland spruce-fir, rocky ridge) get
plain colours chosen here.

Usage:
  python3 scripts/datasets/nh_rasters.py build --work-dir W --out-dir O \\
      [--publish parquetry:parquetry/nh/rasters]
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import rasters
from rasters_catalog import RasterCatalog

from catalog import CONTACT_EMAIL, SITE_URL

PREFIX = "nh/rasters"
PUBLIC = f"https://parquetry.geomermaids.com/{PREFIX}"

HABITAT_COLOURS = {
    "Alpine": (197, 0, 255),
    "Appalachian oak-pine": (215, 194, 158),
    "Cliff and Talus": (255, 0, 0),
    "Coastal island and Rocky shore": (194, 158, 215),
    "Developed": (255, 255, 255),
    "Developed Impervious": (225, 225, 225),
    "Dune and Beach": (137, 112, 68),
    "Floodplain forest": (255, 255, 0),
    "Grassland": (233, 255, 190),             # ours: a picture fill upstream
    "Hemlock-hardwood-pine": (199, 215, 158),
    "High-elevation spruce-fir": (0, 168, 132),
    "Lowland spruce-fir": (38, 115, 0),       # ours: a picture fill upstream
    "Northern hardwood-conifer": (255, 128, 86),
    "Northern swamp": (0, 112, 255),
    "Open Water": (115, 178, 255),
    "Peatland": (0, 56, 147),
    "Pine barren": (168, 168, 0),
    "Rocky ridge": (156, 156, 156),           # ours: a picture fill upstream
    "Salt marsh": (85, 255, 0),
    "Sand-gravel": (252, 182, 231),
    "Shrubland": (171, 26, 143),
    "Temperate swamp": (0, 197, 255),
    "Wet meadow-shrub wetland": (115, 255, 223),
}

PORTAL = "https://granit.unh.edu/"
CATALOG = RasterCatalog(
    catalog_id="nh-rasters",
    title="New Hampshire: thematic rasters (NH GRANIT)",
    state="New Hampshire", keyword="NH GRANIT", keywords=("Wildlife Action Plan",),
    description=(
        "New Hampshire's thematic rasters from NH GRANIT's image services, as Cloud-Optimized "
        "GeoTIFFs on the publisher's grid, each with its value table: for now the Wildlife "
        "Action Plan 2025 habitat land cover."),
    theme_about={"habitat": "Habitat maps of New Hampshire"},
    providers=[
        {"name": "NH Fish and Game Department",
         "description": "Produced the Wildlife Action Plan 2025 habitat land cover with its "
                        "conservation partners.",
         "url": "https://www.wildlife.nh.gov/wildlife-action-plan", "roles": ["producer"]},
        {"name": "NH GRANIT",
         "description": "New Hampshire's statewide GIS clearinghouse, at the University of New "
                        "Hampshire, which serves the rasters as image services.",
         "url": PORTAL, "roles": ["licensor"]},
        {"name": "Geomermaids",
         "description": "Read the rasters at their native grid and wrote them as "
                        "Cloud-Optimized GeoTIFFs, and maintains and hosts this catalog and its "
                        "data.",
         "url": SITE_URL, "email": CONTACT_EMAIL, "roles": ["processor", "host"]},
    ],
    license_link={"rel": "license", "href": PORTAL, "type": "text/html",
                  "title": "NH GRANIT: cite NH GRANIT and the producing agency"},
    via_link={"rel": "via", "href": PORTAL, "type": "text/html",
              "title": "The NH Geodata Portal (NH GRANIT), where the rasters are published"},
    license_md=(
        "Cite NH GRANIT and the agency that produced the raster: \"NH Fish and Game Department "
        "and its conservation partners, 2025\" for the habitat land cover. NH Fish and Game and "
        "its partners do not warrant the completeness or accuracy of the data; it is provided "
        "as is."),
    classes_md=(
        "A cell value is a key into the value table: the cell's habitat (`HABITAT`, 23 of "
        "them, the colours of the COG) and its NatureServe ecological system (`NETH`). Join "
        "on `Value`."),
)

PROFILE = rasters.Profile(
    name="nh-rasters", prefix=PREFIX, public=PUBLIC,
    items_csv=Path(__file__).with_name("nh_rasters_items.csv"),
    themes={"habitat": "Habitat"},
    attribution="NH Fish and Game Department and its conservation partners (Wildlife Action "
                "Plan 2025), through NH GRANIT",
    licence="other", licence_url="https://granit.unh.edu/",
    colour_by="HABITAT", legend=HABITAT_COLOURS,
    # The raster's own nodata (a fringe along the state line); exportImage
    # fills the rest of the extent with it too when asked.
    nodata={"*": 65535},
    catalog=CATALOG,
)

if __name__ == "__main__":
    rasters.main(PROFILE, __doc__)
