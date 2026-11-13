#!/usr/bin/env python3
"""
Render the root catalog of catalog.geomermaids.com: one Portolan catalog with
a child link to each Geomermaids dataset catalog on parquetry.

Nothing is copied. Each child is linked by its absolute URL, so the nightly,
daily and weekly republishing of each dataset catalog shows through. The
root reads each child at render time for its title (a child link's title
must match the child, PTL-TTL-003) and its licence. The one-line summaries
and the pages are written here, so they do not go stale with a build date.
Rerun it when a dataset is added, or when a child's title changes.

The root is served by the catalog-site Worker beside the browser, not from
the bucket: catalog-site/public/catalog.json, README.md, AGENTS.md, logo.png.
rashid checks the root only. It does not follow children on another host,
and each dataset workflow checks its own catalog.

Usage:
  python3 scripts/catalog_root.py build [--dest catalog-site/public]
  python3 scripts/catalog_root.py check [catalog-site/public]
  python3 scripts/catalog_root.py guides [--remote parquetry:parquetry] [--dry-run]

`guides` uploads our guides for the external catalogs (catalog/<name>/AGENTS.md
to <remote>/<name>/AGENTS.md). Run it from a workstation after a guide changes.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import filedates  # noqa: E402
from catalog import (CONTACT_EMAIL, LOGO, PORTOLAN_SCHEMA, PUBLIC_BASE, REPO,  # noqa: E402
                     REPO_URL, check, md_link, now_rfc3339, write_json)

ROOT_URL = "https://catalog.geomermaids.com"
DEST = REPO / "catalog-site" / "public"
FILES = ("catalog.json", "README.md", "AGENTS.md", "logo.png")
CATALOG_ID = "geomermaids"
TITLE = "Geomermaids catalogs"
SITE = "https://geoparquet.geomermaids.com/"
# The dataset catalogs on parquetry, in the order the root lists them: their
# prefix, a summary that does not go stale, and the page that presents them.
# geobox-samples keeps its own catalog: sample data for Geobox, not a dataset.
DATASETS = [
    ("osm", "OpenStreetMap for Canada, Mexico and the United States in 16 themes, "
            "one file per state or province, rebuilt nightly.", SITE),
    ("gmwid", "Power, telecoms, oil and gas, and water infrastructure worldwide, "
              "from OpenStreetMap.", "https://gmwid.geomermaids.com/"),
    ("gaul", "FAO's administrative units for every country: countries, first and "
             "second levels.", SITE),
    ("worldpop", "WorldPop's 2025 population for every country on H3 cells, with age groups "
                 "and sex: 8.2 billion people, one file per country and a world file.", SITE),
    ("clc", "The EEA's pan-European land cover inventory, for Europe and per country.", SITE),
    ("natural-earth", "Natural Earth's world basemap layers at three scales: countries, "
                      "states, places, coastlines, rivers, lakes, ports and airports.", SITE),
    ("nfhl", "FEMA's flood zones for the United States, one file per county delivery, "
             "checked daily.", "https://nfhl.geomermaids.com/"),
    ("geoconnex", "US water reference features (rivers, dams, gages, watersheds) and "
                  "monitoring sites.", SITE),
    ("fire", "US wildfire perimeters: NIFC's active and recent fires, refreshed several "
             "times a day, and MTBS burned areas since 1984.", SITE),
    ("nri", "FEMA's National Risk Index for 18 natural hazards, by census tract and "
            "county: expected annual loss, social vulnerability and resilience.", SITE),
    ("wetlands", "USFWS's National Wetlands Inventory: wetland and deepwater polygons with "
                 "their Cowardin class, one file per state, copies across state downloads removed.", SITE),
    ("padus", "USGS's Protected Areas Database: federal, state, local and private "
              "conservation lands, with manager, owner, GAP status and public access.", SITE),
    ("schools", "NCES's school locations: every US public school, private school and college "
                "as a point with its NCES id, county, districts and locale, one file per survey.", SITE),
    ("structures", "FEMA's USA Structures: 146 million US building footprints with their "
                   "occupancy class and ORNL's population estimate, one file per state.", SITE),
    ("addresses", "The US National Address Database: 99 million address points, one file "
                  "per state, and a map of where states contribute.", SITE),
    ("gazetteer", "Place names to coordinates: the Census Gazetteer 2026 for US areas and "
                  "GeoNames' 13 million names worldwide.", SITE),
    ("census", "TIGER/Line geography, including the congressional and state legislative "
               "districts, the ACS 5-year estimates on it, the 2020 blocks with their counts "
               "and LEHD's LODES jobs by block.",
     "https://census.geomermaids.com/"),
    ("massgis", "The statewide vector layers of the Massachusetts Bureau of Geographic "
                "Information, checked weekly.", SITE),
    ("maine", "The Maine GeoLibrary's statewide layers: parcels, E911 addresses and roads, "
              "conserved lands, habitat, marine fisheries, coastal hazards and geology, and "
              "the Maine DEP's sites, spills, PFAS, water quality and lakes layers, "
              "checked weekly.", SITE),
    ("nh", "NH GRANIT's statewide layers: the parcel mosaic, districts, roads and "
           "facilities, hydrography, soils, conserved lands, wildlife habitat and the "
           "coast's sea-level-rise and groundwater scenarios, checked weekly.", SITE),
]

# Catalogs published by others, linked as they are: nothing is built or stored
# on parquetry. Their title is ours, saying whose catalog it is (Overture's
# root has no title), and a guide of ours may stand in for an AGENTS.md they
# do not ship (catalog/<name>/AGENTS.md, uploaded to parquetry/<name>/).
EXTERNAL = [
    {"url": "https://stac.overturemaps.org/catalog.json",
     "title": "Overture Maps (Overture's own catalog)",
     "summary": "Overture's buildings, places, transportation, divisions, addresses and "
                "base layers worldwide, one release a month. Files and metadata are "
                "Overture's; the guide is ours.",
     "license": "ODbL-1.0 for most themes; places and addresses per source",
     "page": "https://docs.overturemaps.org/",
     "guide": f"{PUBLIC_BASE}/overture/AGENTS.md"},
]


def child_url(prefix: str) -> str:
    return f"{PUBLIC_BASE}/{prefix}/catalog/catalog.json"


def fetch(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=60) as r:
        return json.load(r)


def read_children() -> list[dict]:
    out = []
    for prefix, summary, page in DATASETS:
        try:
            cat = fetch(child_url(prefix))
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
            # A dataset added here before its first publish: left out until then.
            print(f"  {prefix}: no catalog published yet, left out", file=sys.stderr)
            continue
        lic = next((link for link in cat["links"] if link["rel"] == "license"), {})
        out.append({"url": child_url(prefix), "title": cat["title"], "summary": summary,
                    "license": lic.get("title", "see the catalog"), "page": page,
                    "updated": cat.get("updated"), "checked": cat.get("checked")})
    return out + [dict(e) for e in EXTERNAL]


def build_root(children: list[dict], updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (
            "Every dataset Geomermaids republishes as GeoParquet: OpenStreetMap for North "
            "America and its infrastructure worldwide, administrative units, population, "
            "land cover, a world basemap, place names, addresses, schools, building footprints, protected areas, "
            "wetlands, flood zones, wildfire perimeters, natural hazard risk, water features, "
            "census geography and estimates, and the Massachusetts and Maine statewide layers. One sub-catalog per dataset, each kept current by its own build and "
            "under its own licence. Overture Maps is linked to Overture's own catalog, "
            "not copied. With [Geobox](https://geobox.cloud) you can add any of them to your "
            "own box, or query them in place from DuckDB, QGIS or an AI assistant."),
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{ROOT_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": c["url"], "type": "application/json",
               "title": c["title"]} for c in children),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            # One licence per dataset: the README lists them.
            {"rel": "license", "href": "./README.md", "type": "text/markdown",
             "title": "Each dataset catalog states its own licence"},
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


def root_readme(root: dict, children: list[dict]) -> str:
    rows = "\n".join(
        f"| [{c['title']}]({c['url']}) | {c['summary']}"
        + (f" [Guide]({c['guide']})." if c.get("guide") else "")
        + f" | {c['license']} | "
        f"[{c['page'].removeprefix('https://').rstrip('/')}]({c['page']}) |" for c in children)
    return f"""\
# {root['title']}

{root['description']}

| Catalog | What it holds | Licence | Page |
|---|---|---|---|
{rows}

## Access

Every Geomermaids file is on {PUBLIC_BASE} (Overture's are on Overture's
own buckets) and can be read in place over HTTPS with
range requests: DuckDB, GDAL, polars and pyarrow need no download and no
credentials. Each dataset catalog's README says how its files are laid out
and how to query them.

## Licences

Each dataset keeps the licence of its source, stated in its catalog and its
README, with the credit a map or an analysis must show. This root catalog
adds no terms of its own.

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(children: list[dict]) -> str:
    listing = "\n".join(f"- {c['title']}: {c['url']}\n  {c['summary']} Licence: {c['license']}."
                        + (f"\n  Guide: {c['guide']}" if c.get("guide") else "")
                        for c in children)
    return f"""\
# {CATALOG_ID}: agent guide

The root of the Geomermaids catalogs. It holds no data: each child is a
complete Portolan catalog on {PUBLIC_BASE}, with its own README.md and
AGENTS.md beside its catalog.json, except Overture Maps: that child is
Overture's own STAC catalog, read through the guide named below. Read the child's AGENTS.md before
querying it: layouts differ (one file per region, per country, per county or
per layer).

## Catalogs

{listing}

## Conventions

- Files are GeoParquet with a `bbox` covering column: filter on it before
  touching geometry.
- Read in place over HTTPS; no credentials anywhere.
- Each catalog states its licence and the attribution it requires. Carry it
  into any map or result.
"""


def render(dest: Path) -> int:
    children = read_children()
    # When the data last changed anywhere below, and the last check
    # (scripts/filedates.py); not when this root was rendered.
    dated = [c for c in children if c.get("updated")]
    updated, checked = filedates.rollup(dated) if dated else (now_rfc3339(), now_rfc3339())
    root = build_root(children, updated)
    root["checked"] = checked
    dest.mkdir(parents=True, exist_ok=True)
    write_json(dest / "catalog.json", root)
    (dest / "README.md").write_text(root_readme(root, children))
    (dest / "AGENTS.md").write_text(root_agents(children))
    shutil.copyfile(LOGO, dest / "logo.png")
    return len(children)


def check_root(tree: Path) -> int:
    """rashid over the root files alone: dest also holds the browser."""
    with tempfile.TemporaryDirectory(prefix="catalog-root-") as tmp:
        for name in FILES:
            shutil.copyfile(tree / name, Path(tmp) / name)
        return check(Path(tmp))


def upload_guides(remote: str, *, dry_run: bool) -> None:
    for e in EXTERNAL:
        if not e.get("guide"):
            continue
        name = e["guide"].removeprefix(f"{PUBLIC_BASE}/").split("/")[0]
        src = REPO / "catalog" / name / "AGENTS.md"
        cmd = ["rclone", "copyto", "--ignore-times",
               "--header-upload", "Cache-Control: public, max-age=300",
               "--header-upload", "Content-Type: text/markdown; charset=utf-8",
               str(src), f"{remote}/{name}/AGENTS.md"]
        print(f"  $ {' '.join(cmd)}")
        if not dry_run:
            subprocess.run(cmd, check=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="render the root from the live child catalogs, then check it")
    b.add_argument("--dest", type=Path, default=DEST)
    c = sub.add_parser("check", help="rashid over a rendered root")
    c.add_argument("tree", type=Path, nargs="?", default=DEST)
    g = sub.add_parser("guides", help="upload our guides for the external catalogs")
    g.add_argument("--remote", default="parquetry:parquetry")
    g.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if args.cmd == "guides":
        upload_guides(args.remote, dry_run=args.dry_run)
        return
    if args.cmd == "build":
        print(f"wrote {args.dest}/ ({render(args.dest)} child catalogs)")
        sys.exit(check_root(args.dest))
    sys.exit(check_root(args.tree))


if __name__ == "__main__":
    main()
