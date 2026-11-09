#!/usr/bin/env python3
"""
The LODES sub-catalog of the census Portolan catalog (census_catalog.py):

  census/catalog/lodes-2024/catalog.json                         LODES8, 2024
  census/catalog/lodes-2024/lodes2024-wac/collection.json        jobs by workplace block, per state
  census/catalog/lodes-2024/lodes2024-rac/collection.json        jobs by residence block, per state
  census/catalog/lodes-2024/lodes2024-od/collection.json         jobs from home block to work block
  census/catalog/lodes-2024/lodes2024-<wac|rac>-<tract|county>/collection.json
                                                                 the jobs per 2020 tract and county,
                                                                 with outlines
  census/catalog/lodes-2024/lodes2024-<wac|rac>-tract/styles/default.json   jobs per km2

The three block tables are partitioned like the 2020 blocks
(blocks_catalog.py): a `partition:glob` over the state files through the
anonymous S3 endpoint, and one `data-<xx>` asset per state. The tract
rollups carry their PMTiles (a `rel: pmtiles` link with `pmtiles:layers`,
the file as a `visual` asset) and a MapLibre style.

lodes.py's _manifest.json drives it (rows, sizes, checksums, years, gate),
local (--out-dir) or published. Until lodes.py's files are published there
is no manifest, and census_catalog.py renders the catalog without this
sub-catalog. Thumbnails: `census_catalog.py thumbnails --lodes`.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.census import STATES  # noqa: E402
from datasets.gaul_catalog import STYLE_TYPE, WEB_MAP_LINKS_EXT  # noqa: E402
from datasets.lodes import (  # noqa: E402
    BASE, PREFIX, SOURCE_PAGE, TECHDOC, VERSION, YEAR,
)
from datasets.pmtiles import PMTILES_TYPE  # noqa: E402

from catalog import (  # noqa: E402
    ALTERNATE_EXT, BUCKET, CONTACT_EMAIL, FILE_EXT, PARQUET_TYPE, PARTITION_EXT,
    PORTOLAN_SCHEMA, PUBLIC_BASE, REPO_URL, S3_ENDPOINT, SITE_URL, TABLE_EXT, VERSION_EXT,
    md_link, multihash, write_json,
)

URL = f"{PUBLIC_BASE}/census/{PREFIX}"
CATALOG_ID = f"lodes-{YEAR}"
TITLE = f"LEHD Origin-Destination Employment Statistics (LODES), {YEAR}"
ATTRIBUTION = f"U.S. Census Bureau, LEHD Origin-Destination Employment Statistics ({VERSION})"
PROJ_EXT = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
NAMES = {st: name for st, name in STATES.values()}
LICENSE_LINK = {
    "rel": "license",
    "href": "https://www.usa.gov/government-copyright",
    "type": "text/html",
    "title": "Works of the US federal government are not subject to copyright in the "
             "United States (17 U.S.C. 105)",
}
VIA_LINK = {"rel": "via", "href": f"{BASE}/", "type": "text/html",
            "title": f"{VERSION} on lehd.ces.census.gov, where it is published"}
ABOUT_LINK = {"rel": "describedby", "href": TECHDOC, "type": "application/pdf",
              "title": "LODES technical document 8.5 (file structures, data availability)"}
PROVIDERS = [
    {
        "name": "U.S. Census Bureau, Longitudinal Employer-Household Dynamics (LEHD)",
        "description": "Produces LODES from state unemployment insurance records, federal "
                       "employment records and Census Bureau data, with noise infusion to "
                       "protect confidentiality. A US government work, not subject to "
                       "copyright in the United States.",
        "url": SOURCE_PAGE,
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Converted the state CSVs to Parquet, rolled the jobs up to 2020 tracts "
                       "and counties with their TIGER/Line outlines and tiled them, and "
                       "maintains and hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]

# ---------- column docs (technical document 8.5) ----------

NAICS = ["11 (Agriculture, Forestry, Fishing and Hunting)",
         "21 (Mining, Quarrying, and Oil and Gas Extraction)", "22 (Utilities)",
         "23 (Construction)", "31-33 (Manufacturing)", "42 (Wholesale Trade)",
         "44-45 (Retail Trade)", "48-49 (Transportation and Warehousing)", "51 (Information)",
         "52 (Finance and Insurance)", "53 (Real Estate and Rental and Leasing)",
         "54 (Professional, Scientific, and Technical Services)",
         "55 (Management of Companies and Enterprises)",
         "56 (Administrative and Support and Waste Management and Remediation Services)",
         "61 (Educational Services)", "62 (Health Care and Social Assistance)",
         "71 (Arts, Entertainment, and Recreation)", "72 (Accommodation and Food Services)",
         "81 (Other Services, except Public Administration)", "92 (Public Administration)"]
SEGMENT_DOCS = {
    "C000": "Total number of jobs.",
    "CA01": "Jobs of workers age 29 or younger (14 to 29).",
    "CA02": "Jobs of workers age 30 to 54.",
    "CA03": "Jobs of workers age 55 or older (55 to 99).",
    "CE01": "Jobs with earnings of $1,250 a month or less.",
    "CE02": "Jobs with earnings of $1,251 to $3,333 a month.",
    "CE03": "Jobs with earnings greater than $3,333 a month.",
    **{f"CNS{i:02d}": f"Jobs in NAICS sector {n}." for i, n in enumerate(NAICS, 1)},
    "CR01": "Jobs of workers of race White alone.",
    "CR02": "Jobs of workers of race Black or African American alone.",
    "CR03": "Jobs of workers of race American Indian or Alaska Native alone.",
    "CR04": "Jobs of workers of race Asian alone.",
    "CR05": "Jobs of workers of race Native Hawaiian or Other Pacific Islander alone.",
    "CR07": "Jobs of workers of two or more race groups.",
    "CT01": "Jobs of workers not Hispanic or Latino.",
    "CT02": "Jobs of workers Hispanic or Latino.",
    "CD01": "Jobs of workers with less than high school (workers 30 and older only).",
    "CD02": "Jobs of workers with high school or equivalent, no college (workers 30 and older "
            "only).",
    "CD03": "Jobs of workers with some college or an associate degree (workers 30 and older "
            "only).",
    "CD04": "Jobs of workers with a bachelor's or advanced degree (workers 30 and older only).",
    "CS01": "Jobs of male workers.",
    "CS02": "Jobs of female workers.",
}
FIRM_DOCS = {
    **{f"CFA{i:02d}": f"Jobs at firms aged {a}. Zero in every row: LEHD publishes firm age "
                      "for private jobs (JT02) only, and these are all jobs (JT00)."
       for i, a in enumerate(["0 to 1 year", "2 to 3 years", "4 to 5 years", "6 to 10 years",
                              "11 years or more"], 1)},
    **{f"CFS{i:02d}": f"Jobs at firms of {n} employees. Zero in every row: LEHD publishes firm "
                      "size for private jobs (JT02) only, and these are all jobs (JT00)."
       for i, n in enumerate(["0 to 19", "20 to 49", "50 to 249", "250 to 499", "500 or more"],
                             1)},
}
YEAR_DOC = (f"Data year of the state's files: {YEAR}, or the latest year LEHD publishes the "
            f"state's OD and WAC for (the manifest's `fallback`). Read it before summing across "
            f"states.")
COMMON_DOCS = {
    "createdate": "Date the row's file was processed at LEHD, YYYYMMDD (text, as published). "
                  "Not the data year and not the release vintage.",
    "year": YEAR_DOC,
}
OD_DOCS = {
    "w_geocode": "Workplace 2020 Census block (15 characters: state, county, tract, block). "
                 "Its first 11 characters are the 2020 tract, the first 5 the county.",
    "h_geocode": "Residence 2020 Census block (15 characters).",
    "S000": "Total number of jobs from the residence block to the workplace block.",
    "SA01": "Jobs of workers age 29 or younger.",
    "SA02": "Jobs of workers age 30 to 54.",
    "SA03": "Jobs of workers age 55 or older.",
    "SE01": "Jobs with earnings of $1,250 a month or less.",
    "SE02": "Jobs with earnings of $1,251 to $3,333 a month.",
    "SE03": "Jobs with earnings greater than $3,333 a month.",
    "SI01": "Jobs in goods producing sectors (NAICS 11, 21, 23, 31-33).",
    "SI02": "Jobs in trade, transportation and utilities (NAICS 42, 44-45, 48-49, 22).",
    "SI03": "Jobs in all other services (the other twelve NAICS sectors).",
    "interstate": "True for the rows of LODES's aux file: the residence is outside the "
                  "workplace's state. False for the main file: both in the state.",
    **COMMON_DOCS,
}
WAC_DOCS = {"w_geocode": "Workplace 2020 Census block (15 characters: state, county, tract, "
                         "block). Its first 11 characters are the 2020 tract, the first 5 the "
                         "county.",
            **SEGMENT_DOCS, **FIRM_DOCS, **COMMON_DOCS}
RAC_DOCS = {"h_geocode": "Residence 2020 Census block (15 characters: state, county, tract, "
                         "block). Its first 11 characters are the 2020 tract, the first 5 the "
                         "county.",
            **{k: v.replace("Jobs", "Jobs held by residents", 1) if k != "C000" else
               "Total number of jobs held by the block's residents, wherever the job is."
               for k, v in SEGMENT_DOCS.items()},
            **COMMON_DOCS}
ROLLUP_DOCS = {
    "GEOID": "2020 Census tract (11 characters) or county (5) GEOID, as TIGER/Line 2020 "
             "writes it.",
    "NAMELSAD": "Name and legal/statistical description, from TIGER/Line 2020.",
    "state": "USPS postal code of the state.",
    "year": YEAR_DOC,
    "blocks": "Blocks of the area with a row in the state file.",
    "ALAND": "Land area, square metres, from TIGER/Line 2020.",
    "AWATER": "Water area, square metres, from TIGER/Line 2020.",
    "geometry": "Area outline from TIGER/Line 2020, native Parquet GEOMETRY (WKB), NAD83 "
                "(EPSG:4269), longitude and latitude, 2D.",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the GeoParquet "
            "covering. Filter on it to prune row groups.",
}

# table -> (title, what a row is, column docs)
TABLES = {
    "wac": ("Jobs by workplace block (WAC)", "workplace blocks", WAC_DOCS),
    "rac": ("Jobs by residence block (RAC)", "residence blocks", RAC_DOCS),
    "od": ("Jobs from home block to work block (OD)", "(workplace, residence) block pairs",
           OD_DOCS),
}
# Jobs per km2, light to dark: workplaces concentrate far more than homes.
DENSITY_BREAKS = {"wac": [5, 50, 250, 1_000, 5_000], "rac": [5, 50, 250, 750, 2_000]}
RAMP = ["#dbe6f1", "#a9c8e2", "#6fa6d2", "#3a7fbd", "#1b5a9c", "#0b3366"]
EXAMPLE_COUNTY = "25025"  # Suffolk County, MA (Boston)


def read_manifest(out_dir: Path | None) -> dict | None:
    local = out_dir / PREFIX / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    try:
        with urllib.request.urlopen(f"{URL}/_manifest.json") as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def source(out_dir: Path | None, file: str) -> str:
    local = out_dir / PREFIX / file if out_dir else None
    return str(local) if local and local.is_file() else f"{URL}/{file}"


def asset_s3(path: str) -> dict:
    return {"s3": {"href": f"s3://{BUCKET}/{path}",
                   "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}


def collection_id(name: str) -> str:
    return f"lodes{YEAR}-{name.replace('_', '-')}"


def thumb_name(name: str) -> str:
    """One map by workplace (WAC, OD and their rollups), one by residence."""
    return f"lodes{YEAR}-{'rac' if name.startswith('rac') else 'wac'}.png"


def years_text(m: dict) -> str:
    fb = m["fallback"]
    if not fb:
        return f"Every state's files are {YEAR}'s."
    listed = " and ".join(f"{NAMES[st]} {y}" for st, y in sorted(fb.items()))
    return (f"Every state's files are {YEAR}'s but for the states LEHD has no OD and WAC for "
            f"in {YEAR}: {listed}, whose three tables all come from that year (their {YEAR} "
            f"RAC leaves out the jobs held in the state, so it is not used). The `year` column "
            f"says it in every row: a national sum mixes data years, so filter on "
            f"year = {YEAR} for a single-year figure, and remember the other side of the gap "
            f"(a {YEAR} job in a fallback state held by a resident of another state is in no "
            f"{YEAR} file). Puerto Rico is not in LODES.")


def gate_text(m: dict) -> str:
    g = m["gate"]
    residents = (f" Across states, each {YEAR} state's RAC jobs equal its residents' jobs in "
                 f"every {YEAR} state's OD ({g['residents']['states_compared']} states)."
                 if g.get("residents") else "")
    return (f"Gate: for all {g['states']} states, every row is written, every geocode is a "
            f"15-digit block of the state (an OD aux residence outside it), no key repeats, "
            f"and the WAC's jobs equal the OD's.{residents} Every rolled-up area is a "
            f"TIGER/Line 2020 tract or county.")


CAVEAT = ("LODES counts jobs, not people: a worker with two jobs counts twice. Counts are "
          "noise-infused and partly synthetic to protect confidentiality, so a single block's "
          "figures are approximate; sums over many blocks are reliable.")


def style(name: str, tiles: dict) -> dict:
    table = name.split("_")[0]
    density = ["/", ["get", "C000"], ["max", ["/", ["get", "ALAND"], 1_000_000], 0.01]]
    fill = ["step", density, RAMP[0]]
    for threshold, colour in zip(DENSITY_BREAKS[table], RAMP[1:]):
        fill += [threshold, colour]
    what = "workplace" if table == "wac" else "residence"
    return {
        "version": 8,
        "name": f"LODES {YEAR}: jobs per km2 by {what} tract",
        "sources": {"data": {"type": "vector", "url": f"pmtiles://{URL}/{tiles['file']}",
                             "attribution": ATTRIBUTION}},
        "layers": [
            {"id": f"{name}-fill", "type": "fill", "source": "data", "source-layer": tiles["layer"],
             "paint": {"fill-color": fill, "fill-opacity": 0.8}},
            {"id": f"{name}-line", "type": "line", "source": "data",
             "source-layer": tiles["layer"], "minzoom": 9,
             "paint": {"line-color": "#ffffff", "line-width": 0.3}},
        ],
    }


def common(m: dict, extent: list[float], updated: str) -> dict:
    first = min([int(YEAR), *m["fallback"].values()])
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "license": "other",
        "attribution": ATTRIBUTION,
        "version": f"{VERSION} {YEAR}",
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [extent]},
                   "temporal": {"interval": [[f"{first}-01-01T00:00:00Z",
                                              f"{YEAR}-12-31T23:59:59Z"]]}},
    }


def links(title: str, extra: list[dict]) -> list[dict]:
    return [
        {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
        {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
        md_link("describedby", "./README.md", f"{title}: README"),
        md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
        LICENSE_LINK, VIA_LINK, ABOUT_LINK,
        {"rel": "related", "href": f"{URL}/ATTRIBUTION.txt", "type": "text/plain",
         "title": "Attribution and what was changed"},
        *extra,
    ]


def thumbnail_asset(thumbs: Path, name: str) -> dict:
    thumb = thumbs / thumb_name(name)
    what = "residence" if name.startswith("rac") else "workplace"
    return {"href": "./thumbnail.png", "type": "image/png",
            "title": f"Jobs per km2 by {what} tract, {YEAR}", "roles": ["thumbnail"],
            "file:size": thumb.stat().st_size, "file:checksum": multihash(thumb)}


def table_collection(m: dict, table: str, columns, extent, thumbs: Path, updated: str) -> dict:
    title, rows_are, docs = TABLES[table]
    undocumented = [c for c, _ in columns if c not in docs]
    if undocumented:
        sys.exit(f"lodes {table}: no column doc for {undocumented}")
    assets = {}
    for f in m["files"]:
        e = f[table]
        path = f"census/{PREFIX}/state={f['state']}/{e['file']}"
        assets[f"data-{f['state'].lower()}"] = {
            "href": f"{PUBLIC_BASE}/{path}", "type": PARQUET_TYPE,
            "title": f"{NAMES[f['state']]} ({f['state']}), {f['year']}",
            "description": f"{e['rows']:,} rows, {e['row_groups']} row groups, data year "
                           f"{f['year']}",
            "roles": ["data"], "file:size": e["bytes"], "file:checksum": "1220" + e["sha256"],
            "alternate": asset_s3(path)}
    rows = m["rows"][table]
    glob = m["globs"][table]
    key = "w_geocode" if table != "rac" else "h_geocode"
    return {
        **common(m, extent, updated),
        "stac_extensions": [PORTOLAN_SCHEMA, PARTITION_EXT, TABLE_EXT, FILE_EXT, VERSION_EXT,
                            ALTERNATE_EXT],
        "id": collection_id(table),
        "title": f"{title}, LODES {YEAR}",
        "description": (
            f"LODES {YEAR} All Jobs (JT00): {rows:,} {rows_are} of the 50 states and DC, one "
            f"Parquet file per state (postal code), a table without geometry keyed on 2020 "
            f"Census blocks: join the 2020 blocks (tiger-2020-block) on GEOID20 = {key}, or "
            f"read the tract and county rollups with outlines. LODES's columns unchanged, plus "
            f"`year`" + (" and `interstate` (the aux part)" if table == "od" else "")
            + f", sorted by {key}"
            + (", then h_geocode" if table == "od" else "")
            + f". Read every state at once with the partition glob {glob} through the "
              f"anonymous S3 endpoint {S3_ENDPOINT} (path-style, empty credentials), or one "
              f"state over HTTPS. {years_text(m)} {gate_text(m)} {CAVEAT} Credit: "
              f"{ATTRIBUTION}."),
        "keywords": ["LODES", "LEHD", "jobs", "employment", "commuting", "origin-destination",
                     "2020 Census blocks", "US Census Bureau", "Parquet"],
        "partition:scheme": "hive",
        "partition:strategy": "attribute",
        "partition:keys": [{"name": "state", "type": "string",
                            "description": "USPS postal code of the state or DC. One file "
                                           "per state."}],
        "partition:file_count": len(m["files"]),
        "partition:glob": glob,
        "table:row_count": rows,
        "table:columns": [{"name": n, "type": t, "description": docs[n]} for n, t in columns],
        "assets": {**assets, "thumbnail": thumbnail_asset(thumbs, table)},
        "links": links(f"{title}, LODES {YEAR}", [
            {"rel": "related", "href": "../../tiger-2020-block/tabblock20/collection.json",
             "type": "application/json", "title": "The 2020 Census blocks it joins on GEOID20"},
            {"rel": "alternate", "href": f"{URL}/", "type": "text/html",
             "title": "Browse the files"},
        ]),
    }


def rollup_collection(m: dict, entry: dict, columns, extent, thumbs: Path, updated: str,
                      tiles: dict | None) -> dict:
    name = entry["name"]
    table, level = name.split("_")
    base_title, _, docs = TABLES[table]
    docs = {**docs, **ROLLUP_DOCS}
    undocumented = [c for c, _ in columns if c not in docs]
    if undocumented:
        sys.exit(f"lodes {name}: no column doc for {undocumented}")
    title = f"{base_title.split(' (')[0].replace('block', level)}, LODES {YEAR}"
    path = f"census/{PREFIX}/{entry['file']}"
    tiles_path = f"census/{PREFIX}/{tiles['file']}" if tiles else None
    return {
        **common(m, extent, updated),
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT,
                            PROJ_EXT, *([WEB_MAP_LINKS_EXT] if tiles else [])],
        "id": collection_id(name),
        "title": title,
        "description": (
            f"The {'WAC' if table == 'wac' else 'RAC'} job counts of LODES {YEAR} summed per "
            f"2020 Census {level} ({entry['features']:,} {level}s, "
            f"{entry['areas_without_jobs']:,} 2020 {level}s have none), joined to the "
            f"TIGER/Line 2020 outline: NAD83 (EPSG:4269), bbox covering, Hilbert-sorted. The "
            f"firm age and size columns are left out (zero in JT00). {years_text(m)} {CAVEAT} "
            f"Credit: {ATTRIBUTION}."),
        "keywords": ["LODES", "LEHD", "jobs", "employment", f"census {level}s",
                     "US Census Bureau", "GeoParquet"],
        "table:row_count": entry["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": docs[n]} for n, t in columns],
        "assets": {
            "data": {"href": f"{PUBLIC_BASE}/{path}", "type": PARQUET_TYPE, "title": title,
                     "description": f"{entry['features']:,} rows in {entry['row_groups']} row "
                                    f"groups of at most 4 MiB of geometry.",
                     "roles": ["data"], "proj:code": "EPSG:4269", "file:size": entry["bytes"],
                     "file:checksum": "1220" + entry["sha256"], "alternate": asset_s3(path)},
            **({
                "pmtiles": {"href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
                            "title": f"{title}, vector tiles",
                            "description": f"PMTiles, zoom {tiles['min_zoom']} to "
                                           f"{tiles['max_zoom']}, {tiles['tiles']:,} tiles, "
                                           f"layer {tiles['layer']}; properties "
                                           f"{', '.join(tiles['properties'])} (join the rest on "
                                           f"GEOID). Built with tylertoo {tiles['tylertoo']}.",
                            "roles": ["visual"], "file:size": tiles["bytes"],
                            "file:checksum": "1220" + tiles["sha256"],
                            "alternate": asset_s3(tiles_path)},
                "style": {"href": "./styles/default.json", "type": STYLE_TYPE,
                          "title": "Jobs per km2, light to dark", "roles": ["style", "default"]},
            } if tiles else {}),
            "thumbnail": thumbnail_asset(thumbs, name),
        },
        "links": links(title, [
            {"rel": "related", "href": f"../{collection_id(table)}/collection.json",
             "type": "application/json", "title": "The block table it sums"},
            {"rel": "alternate", "href": f"{URL}/", "type": "text/html",
             "title": "Browse the files"},
            *([{"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
                "title": f"{title}, vector tiles", "pmtiles:layers": [tiles["layer"]]}]
              if tiles else []),
        ]),
    }


def build_catalog(collections: list[dict], m: dict, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (
            f"Where the jobs are, where the workers live and how they commute: LEHD's LODES "
            f"{YEAR} (All Jobs, {m['jobs']:,} jobs by workplace) for the 50 states and DC, per "
            f"2020 Census block, as workplace (WAC), residence (RAC) and origin-destination (OD) "
            f"tables, one Parquet file per state, plus the jobs per 2020 tract and county with "
            f"outlines and a map. {years_text(m)} Credit: {ATTRIBUTION}."),
        "version": f"{VERSION} {YEAR}",
        "updated": updated,
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            LICENSE_LINK, VIA_LINK, ABOUT_LINK,
        ],
    }


# ---------- markdown ----------

LICENSE_MD = (f"A work of the US federal government, not subject to copyright in the United "
              f"States (17 U.S.C. 105). Credit \"{ATTRIBUTION}\".")

# Tested queries (see the PR): Suffolk County, MA, as the example.
WAC_SQL = """\
INSTALL httpfs; LOAD httpfs;
-- Jobs by sector in Suffolk County, MA (Boston): a geocode prefix reads few row groups.
SELECT year, sum(C000) AS jobs, sum(CNS16) AS health_care, sum(CNS12) AS professional
FROM read_parquet('{url}/state=MA/wac.parquet')
WHERE w_geocode LIKE '{county}%'
GROUP BY year;"""
OD_SQL = """\
INSTALL httpfs; LOAD httpfs;
-- Where the people working in Suffolk County, MA live: top 10 home counties.
SELECT left(h_geocode, 5) AS home_county, sum(S000) AS jobs, bool_or(interstate) AS other_state
FROM read_parquet('{url}/state=MA/od.parquet')
WHERE w_geocode LIKE '{county}%'
GROUP BY 1 ORDER BY jobs DESC LIMIT 10;"""
BLOCKS_SQL = """\
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
-- Jobs per block joined to the 2020 blocks (geometry, population) in Rhode Island.
SELECT b.GEOID20, b.POP20, w.C000 AS jobs, b.geometry
FROM read_parquet('{blocks}/state=RI/blocks.parquet', hive_partitioning = false) b
JOIN read_parquet('{url}/state=RI/wac.parquet', hive_partitioning = false) w
  ON b.GEOID20 = w.w_geocode
ORDER BY jobs DESC LIMIT 10;"""
NATIONAL_SQL = """\
INSTALL httpfs; LOAD httpfs;
-- Jobs by workplace per state and data year: read `year` before summing nationally.
SELECT state, year, sum(C000) AS jobs
FROM read_parquet('{url}/wac_county.parquet')
GROUP BY ALL ORDER BY jobs DESC;"""
MAP_SQL = """\
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
-- The densest employment tracts: jobs per km2 of land.
SELECT GEOID, NAMELSAD, state, year, C000 AS jobs, round(C000 / (ALAND / 1e6)) AS jobs_per_km2
FROM read_parquet('{url}/{file}')
WHERE ALAND > 0 ORDER BY jobs_per_km2 DESC LIMIT 10;"""
BLOCKS_URL = f"{PUBLIC_BASE}/census/tiger/2020/block"


def sql_for(name: str, file: str = "") -> str:
    fmt = {"url": URL, "county": EXAMPLE_COUNTY, "blocks": BLOCKS_URL, "file": file}
    if name == "wac":
        return f"{WAC_SQL.format(**fmt)}\n\n{BLOCKS_SQL.format(**fmt)}"
    if name == "rac":
        return (WAC_SQL.replace("wac.parquet", "rac.parquet").replace("w_geocode", "h_geocode")
                .replace("Jobs by sector in", "Residents' jobs by sector, residents of")
                .format(**fmt))
    if name == "od":
        return OD_SQL.format(**fmt)
    return (f"{MAP_SQL.format(**fmt)}\n\n{NATIONAL_SQL.format(**fmt)}" if name == "wac_county"
            else MAP_SQL.format(**fmt))


def state_table(m: dict) -> str:
    rows = "\n".join(
        f"| {f['state']} | {NAMES[f['state']]} | {f['year']} | {f['jobs']:,} | "
        f"{f['jobs_by_residence']:,} | {f['od']['rows']:,} |" for f in m["files"])
    return ("| State | Name | Year | Jobs by workplace | Jobs by residence | OD rows |\n"
            "|---|---|---|---|---|---|\n" + rows)


def readme(title: str, description: str, body: str) -> str:
    return f"""\
# {title}

{description}

{body}

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def agents(col: dict, name: str, m: dict, file: str = "") -> str:
    fb = ", ".join(f"{st} {y}" for st, y in sorted(m["fallback"].items())) or "none"
    if name in TABLES:
        layout = (f"One Parquet file per state at `{URL}/state=<XX>/{name}.parquet` (postal "
                  f"code), no geometry; all states: `{m['globs'][name]}` (anonymous S3 on "
                  f"`{S3_ENDPOINT}`, path style, empty credentials).")
    else:
        layout = (f"One GeoParquet file, `{URL}/{file}`, NAD83 (EPSG:4269), `bbox` covering, "
                  f"key `GEOID` (2020 tract or county).")
    return f"""\
# {col['id']}: agent guide

{col['title']}. {layout}

## Query tips

- Data years: {YEAR}, but for {fb}. Every row has `year`: filter on
  `year = {YEAR}` for a single-year figure, or say the total mixes years.
- Geocodes are 2020 blocks: the first 11 characters are the 2020 tract, the
  first 5 the county, the first 2 the state. A prefix filter on the sort key
  reads few row groups.
- Join the 2020 blocks (census/tiger/2020/block) on GEOID20 = the geocode for
  outlines and population; ACS 2020-2024 tracts join on the tract prefix
  (2020 geography, but Connecticut's tracts there use the planning regions).
- Counts are jobs, not people, and noise-infused: report sums over many
  blocks, not one block's count.
- Credit: {ATTRIBUTION}.

```sql
{sql_for(name, file)}
```
"""


def catalog_agents(cat: dict, collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {cat['id']}: agent guide

{cat['description']}

{listing}
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, thumbs: Path, updated: str, footer, layer_extent,
          partial: bool = False) -> dict | None:
    """Write the sub-catalog under dest/lodes-<year>/ and return it, or None
    when lodes.py's files are not published yet. footer and layer_extent
    are census_catalog's. partial renders a test build's manifest (a few
    states), never published."""
    m = read_manifest(out_dir)
    if m is None:
        print(f"  no {URL}/_manifest.json: the LODES sub-catalog is left out", flush=True)
        return None
    if not m.get("complete") and not partial:
        sys.exit(f"{URL}/_manifest.json: a partial build (test states only); not catalogued")
    for name in ("wac", "rac"):
        if not (thumbs / thumb_name(name)).is_file():
            sys.exit(f"missing {thumbs / thumb_name(name)}: run census_catalog.py thumbnails "
                     f"--lodes")
    tract = source(out_dir, "wac_tract.parquet")
    extent = layer_extent(tract)
    cdir = dest / CATALOG_ID
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    collections = []
    for table in TABLES:
        first = source(out_dir, f"state={m['files'][0]['state']}/{table}.parquet")
        # The state file's own columns: footer() would add the state= partition key.
        columns = [(r[0], r[1]) for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{first}', hive_partitioning = false)"
        ).fetchall()]
        col = table_collection(m, table, columns, extent, thumbs, updated)
        body = (f"## States\n\n{state_table(m)}\n\n## Query\n\n```sql\n{sql_for(table)}\n```")
        collections.append((col, table, body, ""))
    tiles = {t["name"]: t for t in m.get("pmtiles", [])}
    for entry in m["rollups"]:
        file = entry["file"]
        col = rollup_collection(m, entry, footer(source(out_dir, file)), extent, thumbs, updated,
                                tiles.get(entry["name"]))
        body = f"## Query\n\n```sql\n{sql_for(entry['name'], file)}\n```"
        collections.append((col, entry["name"], body, file))
    for col, name, body, file in collections:
        d = cdir / col["id"]
        if "style" in col["assets"]:
            sfile = d / "styles" / "default.json"
            write_json(sfile, style(name, tiles[name]))
            col["assets"]["style"]["file:size"] = sfile.stat().st_size
            col["assets"]["style"]["file:checksum"] = multihash(sfile)
        write_json(d / "collection.json", col)
        (d / "thumbnail.png").write_bytes((thumbs / thumb_name(name)).read_bytes())
        (d / "README.md").write_text(readme(col["title"], col["description"], body))
        (d / "AGENTS.md").write_text(agents(col, name, m, file))
    cols = [c for c, *_ in collections]
    cat = build_catalog(cols, m, updated)
    write_json(cdir / "catalog.json", cat)
    listing = "\n".join(f"- [{c['title']}](./{c['id']}/README.md)" for c in cols)
    (cdir / "README.md").write_text(readme(cat["title"], cat["description"], listing))
    (cdir / "AGENTS.md").write_text(catalog_agents(cat, cols))
    return cat


def thumbnails(out_dir: Path | None, thumbs: Path) -> None:
    """Jobs per km2 by workplace tract and by residence tract over the
    conterminous US, coloured as the default style."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    from thumbnails import BACKGROUND, frame

    conus = (-125.0, 24.0, -66.5, 49.5)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    thumbs.mkdir(parents=True, exist_ok=True)
    x0, y0, x1, y1, aspect = frame(*conus)
    for table in ("wac", "rac"):
        path = source(out_dir, f"{table}_tract.parquet")
        rows = con.execute(f"""
            SELECT C000 / greatest(ALAND / 1e6, 0.01),
                   ST_AsWKB(ST_SimplifyPreserveTopology(geometry::GEOMETRY, 0.02))
            FROM read_parquet('{path}')
            WHERE bbox.xmax > {conus[0]} AND bbox.xmin < {conus[2]}
              AND bbox.ymax > {conus[1]} AND bbox.ymin < {conus[3]}""").fetchall()
        polys, fills = [], []
        for density, wkb in rows:
            colour = RAMP[sum(density >= b for b in DENSITY_BREAKS[table])]
            for part in shapely.get_parts(shapely.from_wkb(bytes(wkb))):
                if part.is_empty:
                    continue
                polys.append(shapely.get_coordinates(part.exterior))
                fills.append(colour)
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.set_facecolor(BACKGROUND)
        fig.patch.set_facecolor(BACKGROUND)
        ax.add_collection(PolyCollection(polys, facecolors=fills, edgecolors="none"))
        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
        ax.set_aspect(aspect)
        ax.axis("off")
        out = thumbs / thumb_name(table)
        fig.savefig(out, dpi=100, facecolor=BACKGROUND)
        plt.close(fig)
        print(f"  {table}: {len(rows):,} tracts -> {out.name}")
