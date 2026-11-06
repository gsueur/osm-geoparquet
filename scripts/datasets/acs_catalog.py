#!/usr/bin/env python3
"""
The ACS sub-catalog of the census Portolan catalog (census_catalog.py):

  census/catalog/acs-2024/catalog.json                 ACS 2020-2024 5-year estimates
  census/catalog/acs-2024/acs2024-<geo>/collection.json         the measures, no geometry
  census/catalog/acs-2024/acs2024-<geo>-geo/collection.json     joined to TIGER outlines

acs.py's _manifest.json drives it (counts, sizes, checksums, column docs,
gates), local (--out-dir) or published. Until acs.py's files are published
there is no manifest, and census_catalog.py renders the catalog without
this sub-catalog. Thumbnails: `census_catalog.py thumbnails --acs`.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.acs import DATA, GEOS, HANDBOOK, KNOWN_GAPS, SHELLS_URL, SOURCE_PAGE, SPAN, YEAR
from datasets.census import YEAR as TIGER_YEAR

from catalog import (
    ALTERNATE_EXT, BUCKET, CONTACT_EMAIL, FILE_EXT, PARQUET_TYPE, PORTOLAN_SCHEMA, PUBLIC_BASE,
    REPO_URL, S3_ENDPOINT, SITE_URL, TABLE_EXT, VERSION_EXT, md_link, multihash, write_json,
)

PREFIX = f"census/acs/{YEAR}"
URL = f"{PUBLIC_BASE}/{PREFIX}"
CATALOG_ID = f"acs-{YEAR}"
TITLE = f"American Community Survey {SPAN}, 5-year estimates"
ATTRIBUTION = f"U.S. Census Bureau, American Community Survey {SPAN} 5-year estimates"
PROJ_EXT = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
LICENSE_LINK = {
    "rel": "license",
    "href": "https://www.usa.gov/government-copyright",
    "type": "text/html",
    "title": "Works of the US federal government are not subject to copyright in the "
             "United States (17 U.S.C. 105)",
}
VIA_LINK = {"rel": "via", "href": SOURCE_PAGE, "type": "text/html",
            "title": "ACS Summary File on census.gov, where it is published"}
PROVIDERS = [
    {
        "name": "U.S. Census Bureau, American Community Survey Office",
        "description": "Conducts the American Community Survey and publishes its estimates, "
                       "with margins of error, as the table-based Summary File. A US "
                       "government work, not subject to copyright in the United States.",
        "url": "https://www.census.gov/programs-surveys/acs",
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Curated the measures, derived the rates, joined them to the TIGER/Line "
                       "outlines, and maintains and hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]
CAVEAT = ("Estimates come from a sample: each has a 90 percent margin of error in its _moe "
          "column, and small areas (block groups) carry large ones.")
GEO_COLUMNS = {
    "ALAND": "Land area, square metres, from TIGER/Line.",
    "AWATER": "Water area, square metres, from TIGER/Line.",
    "geometry": "Area outline from TIGER/Line, native Parquet GEOMETRY (WKB), NAD83 "
                "(EPSG:4269), longitude and latitude, 2D.",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the GeoParquet "
            "covering. Filter on it to prune row groups.",
}
# A point each example query finds an area at: Boston City Hall, or a ZIP there.
EXAMPLE = (-71.0589, 42.3601)


def read_manifest(out_dir: Path | None) -> dict | None:
    local = out_dir / "acs" / YEAR / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    try:
        with urllib.request.urlopen(f"{URL}/_manifest.json") as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def data_source(out_dir: Path | None, file: str) -> str:
    local = out_dir / "acs" / YEAR / file if out_dir else None
    return str(local) if local and local.is_file() else f"{URL}/{file}"


def collection_id(name: str) -> str:
    return f"acs{YEAR}-{name.replace('_', '-')}"


def thumb_name(name: str) -> str:
    """The table and its outlined copy share one map."""
    return f"acs{YEAR}-{name.removesuffix('_geo')}.png"


def tiger_collection(geo: str) -> str:
    product = GEOS[geo][2].split("/")[0]
    return f"../../tiger-{TIGER_YEAR}-{product}/{geo}/collection.json"


def build_collection(entry: dict, docs: dict[str, str], columns: list[tuple[str, str]],
                     extent: list[float], thumbs: Path, updated: str) -> dict:
    name, spatial = entry["name"], entry["geometry"]
    geo = name.removesuffix("_geo")
    title = GEOS[geo][1]
    path = f"{PREFIX}/{entry['file']}"
    undocumented = [c for c, _ in columns if c not in docs and c not in GEO_COLUMNS]
    if undocumented:
        sys.exit(f"{name}: no column doc for {undocumented}")
    dropped = entry.get("without_outline", 0)
    thumb = thumbs / thumb_name(name)
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT,
                            *([PROJ_EXT] if spatial else [])],
        "id": collection_id(name),
        "title": f"{title}, ACS {SPAN}" + (", with outlines" if spatial else ""),
        "description": (
            f"American Community Survey {SPAN} 5-year estimates for {entry['features']:,} "
            f"{title.lower()}: population, age, households, income, poverty, housing value, "
            f"rent, tenure and vacancy, race and Hispanic origin, education and commuting, "
            f"each with its margin of error, keyed by TIGER/Line's GEOID. "
            + (f"Joined to the TIGER/Line {TIGER_YEAR} outlines, NAD83 (EPSG:4269), with a bbox "
               f"covering, Hilbert-sorted"
               + (f"; {dropped} areas with no {TIGER_YEAR} outline are left out" if dropped else "")
               + ". " if spatial else
               "A table without geometry: join it to the TIGER/Line layer on GEOID, or read "
               "the copy with outlines. ")
            + f"{CAVEAT} Credit: {ATTRIBUTION}."
        ),
        "keywords": ["American Community Survey", "ACS", "US Census Bureau", "demographics",
                     "population", "income", "housing", title.lower(), "GeoParquet"],
        "license": "other",
        "attribution": ATTRIBUTION,
        "version": SPAN,
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {
            "spatial": {"bbox": [extent]},
            "temporal": {"interval": [[f"{int(YEAR) - 4}-01-01T00:00:00Z",
                                       f"{YEAR}-12-31T23:59:59Z"]]},
        },
        "table:row_count": entry["features"],
        **({"table:primary_geometry": "geometry"} if spatial else {}),
        "table:columns": [{"name": n, "type": t, "description": docs.get(n) or GEO_COLUMNS[n]}
                          for n, t in columns],
        "assets": {
            "data": {
                "href": f"{PUBLIC_BASE}/{path}",
                "type": PARQUET_TYPE,
                "title": f"{title}, ACS {SPAN}" + (", with outlines" if spatial else ""),
                "description": f"{entry['features']:,} rows in {entry['row_groups']} row groups"
                               + (" of at most 4 MiB of geometry." if spatial else
                                  ", sorted by GEOID."),
                "roles": ["data"],
                **({"proj:code": "EPSG:4269"} if spatial else {}),
                "file:size": entry["bytes"],
                "file:checksum": "1220" + entry["sha256"],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                     "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            "thumbnail": {
                "href": "./thumbnail.png",
                "type": "image/png",
                "title": f"{title}: median household income",
                "roles": ["thumbnail"],
                "file:size": thumb.stat().st_size,
                "file:checksum": multihash(thumb),
            },
        },
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{title}, ACS {SPAN}: README"),
            md_link("agents", "./AGENTS.md", f"{title}, ACS {SPAN}: agent guide"),
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "describedby", "href": SHELLS_URL, "type": "text/plain",
             "title": "ACS table shells: every variable's label"},
            {"rel": "related", "href": tiger_collection(geo), "type": "application/json",
             "title": f"The TIGER/Line {TIGER_YEAR} layer it joins on GEOID"},
            {"rel": "related", "href": f"../{collection_id(geo if spatial else geo + '_geo')}"
                                       f"/collection.json", "type": "application/json",
             "title": "The same measures without geometry" if spatial
             else "The same measures with outlines"},
            {"rel": "alternate", "href": f"{URL}/", "type": "text/html",
             "title": "Browse the files"},
        ],
    }


def access_md(entry: dict) -> str:
    name, spatial = entry["name"], entry["geometry"]
    geo = name.removesuffix("_geo")
    x, y = EXAMPLE
    if spatial:
        return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
SELECT GEOID, NAME, population, median_household_income, median_household_income_moe
FROM read_parquet('{URL}/{entry['file']}')
WHERE bbox.xmin <= {x} AND bbox.xmax >= {x}
  AND bbox.ymin <= {y} AND bbox.ymax >= {y}
  AND ST_Contains(geometry, ST_Point({x}, {y}));
```"""
    prefix = {"state": "25", "county": "25", "place": "25", "tract": "25025",
              "bg": "25025", "zcta520": "021"}[geo]
    return f"""\
```sql
INSTALL httpfs; LOAD httpfs;
SELECT GEOID, NAME, population, population_moe, poverty_rate, poverty_rate_moe
FROM read_parquet('{URL}/{entry['file']}')
WHERE GEOID LIKE '{prefix}%'   -- sorted by GEOID: a prefix reads few row groups
ORDER BY population DESC LIMIT 10;
```"""


def collection_readme(col: dict, entry: dict) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, {entry['row_groups']} row groups, sha256 `{entry['sha256'][:12]}...` |
| Rows | {entry['features']:,} |
| CRS | {'NAD83, EPSG:4269 (geographic)' if entry['geometry'] else 'none (a table)'} |

## Access

{access_md(entry)}

The same file at `s3://{BUCKET}/{PREFIX}/{entry['file']}` on the anonymous S3
endpoint `{S3_ENDPOINT}`.

## Schema

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

{len(GEOS)} geographies and 16 detailed tables of the table-based Summary File
({DATA}/), read by [acs.py]({REPO_URL}/blob/main/scripts/datasets/acs.py).
Estimates and margins of error as published; rates and the mean travel time
derived with the ACS handbook's formulas ({HANDBOOK}). Annotation codes are
not numbers here: an estimate of -666666666 is NULL, an MOE of -555555555
(controlled) is 0, an MOE of -333333333 or -222222222 is NULL. Before
publishing, every area is checked against TIGER/Line {TIGER_YEAR}, and
populations must add up block group to tract to county to state, but for
the source's known gaps: {'; '.join(f'{k}: {v}' for k, v in KNOWN_GAPS.items())}.

## License

The ACS is a work of the US federal government, not subject to copyright in
the United States (17 U.S.C. 105). Credit "{ATTRIBUTION}".
"""


def collection_agents(col: dict, entry: dict) -> str:
    return f"""\
# {col['title']}: agent guide

One row per area, key `GEOID` (TIGER/Line's identifier), name `NAME`.
{"Geometry column `geometry`, NAD83 (EPSG:4269), `bbox` covering." if entry['geometry'] else "No geometry: join the TIGER/Line layer or the `-geo` collection on GEOID."}

## Access

{access_md(entry)}

## Query tips

- Every measure has a `<name>_moe`, the 90 percent margin of error. Say so
  when comparing areas: a difference smaller than the MOEs may not be real.
- Rates are percent (0 to 100). Medians cannot be summed or averaged across
  areas; counts can (their MOE is the root of the summed squared MOEs).
- For a population within a distance, weight each tract or block group by
  the share of its area inside the buffer, or count those whose centroid is
  inside: say which.
- Medians at their published top and bottom codes (income 250001, home value
  2000001, rent 3501) mean "this much or more".
- Credit: {ATTRIBUTION}.
"""


def build_catalog(collections: list[dict], manifest: dict, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (f"Curated American Community Survey {SPAN} 5-year estimates for states, "
                        f"counties, places, census tracts, block groups and ZCTAs, keyed like "
                        f"the TIGER/Line layers, each as a table and as a copy joined to the "
                        f"TIGER/Line {TIGER_YEAR} outlines. Summary File last modified "
                        f"{manifest['source']['last_modified']}."),
        "version": SPAN,
        "updated": updated,
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            LICENSE_LINK,
            {"rel": "related", "href": f"{URL}/ATTRIBUTION.txt", "type": "text/plain",
             "title": "Attribution and what was changed"},
        ],
    }


def catalog_readme(cat: dict, collections: list[dict]) -> str:
    rows = "\n".join(f"| [{c['title']}](./{c['id']}/README.md) | "
                     f"`{c['assets']['data']['href'].rsplit('/', 1)[1]}` | "
                     f"{c['table:row_count']:,} |" for c in collections)
    return f"""\
# {cat['title']}

{cat['description']}

| Collection | File | Rows |
|---|---|---|
{rows}

{CAVEAT}

## License

A work of the US federal government, not subject to copyright in the United
States (17 U.S.C. 105). Credit "{ATTRIBUTION}".
"""


def catalog_agents(cat: dict, collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {cat['id']}: agent guide

{cat['description']}

{listing}

Files: `{URL}/<geo>.parquet` (no geometry) and `<geo>_geo.parquet` (TIGER
outline, NAD83), <geo> one of {', '.join(GEOS)}. Key `GEOID`. Every measure
has a `_moe` column. Credit: {ATTRIBUTION}.
"""


def build(out_dir: Path | None, dest: Path, thumbs: Path, updated: str,
          footer, layer_extent) -> dict | None:
    """Write the sub-catalog under dest/acs-<year>/ and return it, or None
    when acs.py's files are not published yet. footer and layer_extent are
    census_catalog's."""
    manifest = read_manifest(out_dir)
    if manifest is None:
        print(f"  no {URL}/_manifest.json: the ACS sub-catalog is left out", flush=True)
        return None
    docs = {c["name"]: c["description"] for c in manifest["columns"]}
    extents = {}
    for entry in manifest["files"]:
        if entry["geometry"]:
            extents[entry["name"].removesuffix("_geo")] = layer_extent(
                data_source(out_dir, entry["file"]))
    cdir = dest / CATALOG_ID
    collections = []
    for entry in manifest["files"]:
        thumb = thumbs / thumb_name(entry["name"])
        if not thumb.is_file():
            sys.exit(f"missing {thumb}: run census_catalog.py thumbnails --acs")
        source = data_source(out_dir, entry["file"])
        col = build_collection(entry, docs, footer(source),
                               extents[entry["name"].removesuffix("_geo")], thumbs, updated)
        d = cdir / col["id"]
        write_json(d / "collection.json", col)
        d.joinpath("thumbnail.png").write_bytes(thumb.read_bytes())
        (d / "README.md").write_text(collection_readme(col, entry))
        (d / "AGENTS.md").write_text(collection_agents(col, entry))
        collections.append(col)
    cat = build_catalog(collections, manifest, updated)
    write_json(cdir / "catalog.json", cat)
    (cdir / "README.md").write_text(catalog_readme(cat, collections))
    (cdir / "AGENTS.md").write_text(catalog_agents(cat, collections))
    return cat


def thumbnails(out_dir: Path, thumbs: Path) -> None:
    """Median household income per area over the conterminous US, one map
    per geography (its table and its outlined copy share it)."""
    import duckdb
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    from thumbnails import BACKGROUND, frame

    conus = (-125.0, 24.0, -66.5, 49.5)
    # Sequential, light to dark, in the site's accent.
    ramp = ["#fae0d8", "#f2b39f", "#e8866a", "#d2553a", "#a53b24", "#6e2414"]
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    thumbs.mkdir(parents=True, exist_ok=True)
    x0, y0, x1, y1, aspect = frame(*conus)
    for geo in GEOS:
        path = out_dir / "acs" / YEAR / f"{geo}_geo.parquet"
        tolerance = 0.02
        rows = con.execute(f"""
            SELECT median_household_income,
                   ST_AsWKB(ST_SimplifyPreserveTopology(geometry::GEOMETRY, {tolerance}))
            FROM read_parquet('{path}')
            WHERE bbox.xmax > {conus[0]} AND bbox.xmin < {conus[2]}
              AND bbox.ymax > {conus[1]} AND bbox.ymin < {conus[3]}""").fetchall()
        cuts = [r[0] for r in con.execute(f"""
            SELECT unnest(quantile_cont(median_household_income, [0.2, 0.4, 0.6, 0.8, 0.95]))
            FROM read_parquet('{path}')""").fetchall()]
        polys, fills = [], []
        for income, wkb in rows:
            colour = "#d9d8d2" if income is None else ramp[sum(income > c for c in cuts)]
            for part in shapely.get_parts(shapely.from_wkb(bytes(wkb))):
                if part.is_empty:
                    continue
                polys.append(shapely.get_coordinates(part.exterior))
                fills.append(colour)
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.set_facecolor(BACKGROUND)
        fig.patch.set_facecolor(BACKGROUND)
        dense = len(rows) > 20_000
        ax.add_collection(PolyCollection(polys, facecolors=fills,
                                         edgecolors="none" if dense else "#ffffff",
                                         linewidths=0 if dense else 0.2))
        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
        ax.set_aspect(aspect)
        ax.axis("off")
        out = thumbs / thumb_name(geo)
        fig.savefig(out, dpi=100, facecolor=BACKGROUND)
        plt.close(fig)
        print(f"  {geo}: {len(rows):,} areas -> {out.name}")
