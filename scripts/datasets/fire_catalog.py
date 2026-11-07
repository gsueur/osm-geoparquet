#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for the US wildfire data.

  fire/catalog/catalog.json                     root: US wildfire perimeters
  fire/catalog/<collection>/collection.json     current, year_to_date, all_since_2016,
                                                incidents_ytd, mtbs_perimeters

fire.py's WFIGS layers and its MTBS file drive it. Each collection has its
GeoParquet (`data`), a thumbnail and, for perimeters, the PMTiles archive
registered as formats.md asks (a `rel: pmtiles` link with `pmtiles:layers`,
web-map-links v1.3.0, the file as a `visual` asset) with a MapLibre style.
Counts, sizes, checksums and the gates come from each group's
_manifest.json, local (--out-dir, for the group just built) or published
(the other group): a WFIGS run renders the MTBS collection from its live
manifest. An empty layer (no active fire) keeps its collection, with the
US extent and no PMTiles. Spec: portolan-spec v0.2.0. Validator: rashid,
through scripts/catalog.py.

Usage:
  python3 scripts/datasets/fire_catalog.py build --dest build/fire-catalog [--out-dir out/fire]
  python3 scripts/datasets/fire_catalog.py check build/fire-catalog
  python3 scripts/datasets/fire_catalog.py publish --remote parquetry:parquetry/fire [--out-dir out/fire]
  uv run --project scripts --with matplotlib --with shapely \\
      python scripts/datasets/fire_catalog.py thumbnails --out-dir out/fire
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.fire import (
    MTBS_CREDIT, MTBS_PAGE, MTBS_ZIP, WFIGS, WFIGS_CREDIT, WFIGS_PAGE,
    WFIGS_PARTNERS,
)
from datasets.gaul_catalog import STYLE_TYPE, WEB_MAP_LINKS_EXT
from datasets.pmtiles import PMTILES_TYPE

import catalog as shared  # scripts/catalog.py
from extent import lon_intervals, wrapped_bbox
from catalog import (
    ALTERNATE_EXT,
    BUCKET,
    CONTACT_EMAIL,
    FILE_EXT,
    LOGO,
    PARQUET_TYPE,
    PORTOLAN_SCHEMA,
    PUBLIC_BASE,
    REPO,
    REPO_URL,
    S3_ENDPOINT,
    SITE_URL,
    TABLE_EXT,
    VERSION_EXT,
    md_link,
    multihash,
    now_rfc3339,
    write_json,
)

DATASET_PREFIX = "fire"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "us-wildfire"
TITLE = "US wildfire perimeters: NIFC WFIGS and MTBS"
THUMBS = REPO / "catalog" / "thumbnails" / "fire"
XML_TYPE = "application/xml"
# Where an empty layer says it would be: the US and its territories.
US_EXTENT = [-179.9, 17.5, -64.5, 71.5]

# collection -> group, title, description lead, and the date column the
# temporal extent is measured on.
COLLECTIONS = {
    **{name: {"group": "wfigs", "title": spec["title"], "kind": spec["kind"],
              "date": ("attr_FireDiscoveryDateTime" if spec["kind"] == "perimeter"
                       else "FireDiscoveryDateTime")}
       for name, spec in WFIGS.items()},
    "mtbs_perimeters": {"group": "mtbs", "title": "Burned areas since 1984 (MTBS)",
                        "kind": "perimeter", "date": "ig_date"},
}
LEADS = {
    "current": "The perimeters of the fires burning now, as the incident teams map them: "
               "WFIGS's current interagency perimeters, refreshed by NIFC every few minutes and "
               "rebuilt here several times a day.",
    "year_to_date": "The latest perimeter of every fire of this calendar year (WFIGS year to "
                    "date), active or out.",
    "all_since_2016": "The latest perimeter of every fire WFIGS holds, discoveries since March "
                      "2016.",
    "incidents_ytd": "One point per fire incident of this calendar year, where it started "
                     "(WFIGS incident locations, year to date): most incidents are small and "
                     "never get a perimeter.",
    "mtbs_perimeters": "Burned area boundaries mapped from Landsat and Sentinel imagery by "
                       "Monitoring Trends in Burn Severity (USGS and USDA Forest Service): fires "
                       "of 1,000 acres or more in the West and 500 or more in the East, from "
                       "1984, with the burn severity thresholds of each assessment. A fire "
                       "reaches MTBS a year or two after it burns.",
}

# ---------- column docs ----------

# The WFIGS incident attributes; on the perimeter layers the same names
# carry an `attr_` prefix (the incident the perimeter belongs to).
INCIDENT_DOCS = {
    "IncidentName": "Incident name.",
    "IrwinID": "IRWIN id of the incident (the interagency incident record), in braces: joins "
               "perimeters, incidents and MTBS's irwinid.",
    "UniqueFireIdentifier": "Unique fire identifier: year, protecting unit and local number "
                            "(e.g. 2026-CASCU-012345).",
    "IncidentTypeCategory": "Incident type category: WF wildfire, RX prescribed fire, CX complex "
                            "(several fires managed as one).",
    "IncidentTypeKind": "Incident kind: FI fire.",
    "FireDiscoveryDateTime": "When the fire was discovered (UTC).",
    "ContainmentDateTime": "When the fire was declared contained (UTC).",
    "ControlDateTime": "When the fire was declared controlled (UTC).",
    "FireOutDateTime": "When the fire was declared out (UTC); empty while it burns.",
    "IncidentSize": "Reported size of the incident, acres.",
    "DiscoveryAcres": "Size at discovery, acres.",
    "FinalAcres": "Final size, acres, once reported.",
    "PercentContained": "Containment reported, percent.",
    "FireCause": "Cause: Human, Natural, Undetermined or Unknown.",
    "FireCauseGeneral": "General cause (e.g. Lightning, Debris Burning, Equipment).",
    "FireCauseSpecific": "Specific cause, where determined.",
    "FireMgmtComplexity": "Fire management complexity (Type 1 most complex to Type 5).",
    "IncidentComplexityLevel": "Incident complexity level.",
    "IsCpxChild": "1 when the fire is part of a complex (CpxName).",
    "CpxName": "Name of the complex the fire belongs to.",
    "POOState": "Point of origin: state, as US-XX.",
    "POOCounty": "Point of origin: county.",
    "POOFips": "Point of origin: county FIPS code (5 digits); joins TIGER/Line county GEOID.",
    "POOCity": "Point of origin: nearest city.",
    "POOJurisdictionalAgency": "Agency with jurisdiction at the point of origin.",
    "POOProtectingAgency": "Agency responsible for fire protection at the point of origin.",
    "POOLandownerCategory": "Landowner category at the point of origin (USFS, BLM, Private...).",
    "GACC": "Geographic Area Coordination Center (e.g. CAONCC, NWCC).",
    "TotalIncidentPersonnel": "Personnel assigned, as last reported.",
    "EstimatedCostToDate": "Estimated cost to date, US dollars, as last reported.",
    "InitialLatitude": "Latitude of the point of origin, as first reported.",
    "InitialLongitude": "Longitude of the point of origin, as first reported.",
    "ModifiedOnDateTime_dt": "When the incident record was last modified (UTC).",
    "GlobalID": "WFIGS's global id of the row.",
}
PERIMETER_DOCS = {
    "poly_IncidentName": "Incident name, as written on the perimeter.",
    "poly_FeatureCategory": "Perimeter category (e.g. Wildfire Daily Fire Perimeter, Wildfire "
                            "Final Fire Perimeter, Prescribed Fire).",
    "poly_MapMethod": "How the perimeter was mapped (Infrared Image, GPS, Hand Sketch, "
                      "Mixed Methods...).",
    "poly_GISAcres": "Area of the perimeter, acres, as computed in GIS by WFIGS.",
    "poly_PolygonDateTime": "When the perimeter was mapped (UTC).",
    "poly_DateCurrent": "When the perimeter record was last updated (UTC).",
    "poly_IRWINID": "IRWIN id on the perimeter; equals attr_IrwinID.",
    "poly_Source": "System the perimeter came from.",
    **{f"attr_{k}": f"Incident: {v[0].lower()}{v[1:]}" for k, v in INCIDENT_DOCS.items()},
}
MTBS_DOCS = {
    "event_id": "MTBS event id, unique per fire.",
    "irwinid": "IRWIN id of the incident, where known: joins the WFIGS layers' IrwinID.",
    "incid_name": "Name of the fire (UNNAMED when the fire occurrence databases give none).",
    "incid_type": "Type of fire: Wildfire, Prescribed Fire, Wildland Fire Use, Other, "
                  "Unknown, Complex.",
    "map_id": "Mapping id.",
    "map_prog": "Mapping program or protocol the fire was mapped with.",
    "asmnt_type": "Assessment: Initial or Extended (SS: single scene), or Emergency.",
    "burnbndac": "Acres mapped inside the burned area boundary.",
    "burnbndlat": "Latitude of the mapped centroid of the perimeter.",
    "burnbndlon": "Longitude of the mapped centroid of the perimeter.",
    "ig_date": "Date of ignition, from the fire occurrence databases.",
    "pre_id": "Landsat or Sentinel pre-fire scene id.",
    "post_id": "Landsat or Sentinel post-fire scene id.",
    "perim_id": "Landsat or Sentinel scene used to delineate an Extended assessment, when used.",
    "dnbr_offst": "Mean dNBR sampled in an unburned area outside the perimeter.",
    "dnbr_stddv": "Standard deviation of that dNBR sample.",
    "nodata_t": "No-data threshold (dNBR, or NBR for single-scene assessments).",
    "incgreen_t": "Increased-greenness threshold (dNBR, or NBR for single-scene assessments).",
    "low_t": "Unburned to low severity threshold (dNBR, or NBR for single-scene assessments).",
    "mod_t": "Low to moderate severity threshold (dNBR, or NBR for single-scene assessments).",
    "high_t": "Moderate to high severity threshold (dNBR, or NBR for single-scene assessments).",
    "comment": "Notes from the mapping analyst.",
}
COMMON_DOCS = {
    "OBJECTID": "WFIGS's OBJECTID: stable for a row, but not across WFIGS's layers.",
    "geometry": "Native Parquet GEOMETRY (WKB), lon/lat (OGC:CRS84).",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the GeoParquet "
            "covering. Filter on it to prune row groups.",
}


def column_doc(name: str) -> str | None:
    return (COMMON_DOCS.get(name) or PERIMETER_DOCS.get(name) or INCIDENT_DOCS.get(name)
            or MTBS_DOCS.get(name))


# ---------- measured inputs ----------

def latest_url(group: str) -> str:
    return f"{PUBLIC_DATA}/{group}/latest"


def read_manifest(group: str, out_dir: Path | None) -> dict | None:
    """The group's manifest, local or published; None when the group has
    never been published (the first run of the other group)."""
    local = out_dir / group / "latest" / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    try:
        with urllib.request.urlopen(f"{latest_url(group)}/_manifest.json") as r:
            return json.load(r)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            print(f"  {group}: not published yet, its collections are left out")
            return None
        raise


def data_source(group: str, out_dir: Path | None, file: str) -> str:
    local = out_dir / group / "latest" / file if out_dir else None
    return str(local) if local and local.is_file() else f"{latest_url(group)}/{file}"


def connect():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs; "
                "SET TimeZone = 'UTC'")
    return con


def measure(source: str, date_col: str, rows: int) -> tuple[list[float], list[str | None]]:
    """Spatial extent (antimeridian-aware, scripts/extent.py) and the first
    and last date, as RFC 3339 strings."""
    if not rows:
        return US_EXTENT, [None, None]
    con = connect()
    src = f"read_parquet('{source}')"
    y0, y1, d0, d1 = con.execute(f"""SELECT min(bbox.ymin), max(bbox.ymax),
        strftime(min({date_col})::TIMESTAMP, '%Y-%m-%dT%H:%M:%SZ'),
        strftime(max({date_col})::TIMESTAMP, '%Y-%m-%dT%H:%M:%SZ') FROM {src}""").fetchone()
    b = wrapped_bbox(lon_intervals(con, src), y0, y1)
    return ([round(b[0], 6), max(-90.0, round(b[1], 6)), round(b[2], 6),
             min(90.0, round(b[3], 6))], [d0, d1])


def footer(source: str) -> list[tuple[str, str]]:
    return [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1]) for r in
            connect().execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()]


# ---------- styles ----------

# Wildfire red, prescribed fire green, complexes dark red, the rest grey.
WFIGS_FILL = ["match", ["get", "attr_IncidentTypeCategory"],
              "WF", "#d7301f", "RX", "#41ab5d", "CX", "#7f0000", "#969696"]
MTBS_FILL = ["match", ["get", "incid_type"],
             "Wildfire", "#d7301f", "Prescribed Fire", "#41ab5d", "Wildland Fire Use", "#fd8d3c",
             "#969696"]


def style(name: str, tiles: dict, attribution: str, url: str) -> dict:
    fill = MTBS_FILL if name == "mtbs_perimeters" else WFIGS_FILL
    return {
        "version": 8, "name": f"{name}: by fire type",
        "sources": {"data": {"type": "vector", "url": f"pmtiles://{url}/{tiles['file']}",
                             "attribution": attribution}},
        "layers": [
            {"id": f"{name}-fill", "type": "fill", "source": "data", "source-layer": name,
             "paint": {"fill-color": fill, "fill-opacity": 0.55}},
            {"id": f"{name}-outline", "type": "line", "source": "data", "source-layer": name,
             "paint": {"line-color": fill,
                       "line-width": ["interpolate", ["linear"], ["zoom"], 4, 0.3, 12, 1.5]}},
        ],
    }


# ---------- STAC ----------

LICENSE_LINK = {
    "rel": "license",
    "href": "https://www.usa.gov/government-copyright",
    "type": "text/html",
    "title": "Works of the US federal government are not subject to copyright in the "
             "United States (17 U.S.C. 105)",
}
VIA = {
    "wfigs": {"rel": "via", "href": WFIGS_PAGE, "type": "text/html",
              "title": "NIFC Open Data, where WFIGS publishes the layers"},
    "mtbs": {"rel": "via", "href": MTBS_PAGE, "type": "text/html",
             "title": "MTBS direct download, where the burned area boundaries are published"},
}
CREDIT = {"wfigs": WFIGS_CREDIT, "mtbs": MTBS_CREDIT}
PROVIDERS = {
    "wfigs": [{"name": "National Interagency Fire Center (NIFC), WFIGS",
               "description": "Publishes the interagency fire perimeters and incident locations, "
                              f"compiled with {WFIGS_PARTNERS}.",
               "url": WFIGS_PAGE, "roles": ["producer", "licensor"]}],
    "mtbs": [{"name": "Monitoring Trends in Burn Severity (USGS EROS and USDA Forest Service)",
              "description": "Maps burned area boundaries and burn severity from satellite "
                             "imagery.",
              "url": "https://www.mtbs.gov/", "roles": ["producer", "licensor"]}],
}
GEOMERMAIDS = {"name": "Geomermaids",
               "description": "Converted the layers to GeoParquet, tiled them, and maintains and "
                              "hosts this catalog and its data.",
               "url": SITE_URL, "email": CONTACT_EMAIL, "roles": ["processor", "host"]}
NOT_FOR_SAFETY = ("Not for evacuation or safety decisions: perimeters lag the fire; follow "
                  "local authorities.")


def asset_s3(path: str) -> dict:
    return {"s3": {"href": f"s3://{BUCKET}/{path}", "title": f"S3 endpoint {S3_ENDPOINT}, path style"}}


def gate_text(entry: dict, group: str) -> str:
    g = entry["gate"]
    if group == "wfigs":
        text = (f"Read in {g['pages']} pages by OBJECTID; {g['rows']:,} rows, the server's count "
                f"for the same range")
    else:
        text = (f"{g['shapefile_rows']:,} rows, the shapefile's, every event_id unique")
    if g.get("repaired_geometries"):
        text += f"; {g['repaired_geometries']:,} invalid polygons repaired with ST_MakeValid"
    return text + "."


def build_collection(name: str, entry: dict, manifest: dict, tiles: dict | None,
                     extent: list[float], dates: list[str | None],
                     columns: list[tuple[str, str]], updated: str) -> dict:
    spec = COLLECTIONS[name]
    group = spec["group"]
    url = latest_url(group)
    path = f"{DATASET_PREFIX}/{group}/latest/{entry['file']}"
    undocumented = [c for c, _ in columns if column_doc(c) is None]
    if undocumented:
        sys.exit(f"{name}: no column doc for {undocumented}")
    built = manifest["built"][:10]
    assets = {
        "data": {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": spec["title"],
            "description": f"{entry['features']:,} rows in {entry['row_groups']} row groups of at "
                           f"most 4 MiB of geometry.",
            "roles": ["data"],
            "file:size": entry["bytes"],
            "file:checksum": "1220" + entry["sha256"],
            "alternate": asset_s3(path),
        },
        "thumbnail": {
            "href": "./thumbnail.png",
            "type": "image/png",
            "title": f"{spec['title']}, as of the thumbnail's rendering",
            "roles": ["thumbnail"],
            "file:size": (THUMBS / f"{name}.png").stat().st_size,
            "file:checksum": multihash(THUMBS / f"{name}.png"),
        },
    }
    if group == "mtbs":
        md = manifest["metadata"]
        assets["metadata"] = {
            "href": f"{url}/{md['file']}", "type": XML_TYPE,
            "title": "MTBS's FGDC metadata, as shipped", "roles": ["metadata"],
            "file:size": md["bytes"], "file:checksum": "1220" + md["sha256"]}
    links = [
        {"rel": "root", "href": "../catalog.json", "type": "application/json"},
        {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
        md_link("describedby", "./README.md", f"{spec['title']}: README"),
        md_link("agents", "./AGENTS.md", f"{spec['title']}: agent guide"),
        LICENSE_LINK,
        VIA[group],
        {"rel": "related", "href": f"{url}/ATTRIBUTION.txt", "type": "text/plain",
         "title": "Attribution and what was changed"},
        {"rel": "alternate", "href": f"{url}/", "type": "text/html", "title": "Browse the files"},
    ]
    extensions = [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT]
    if tiles:
        tiles_path = f"{DATASET_PREFIX}/{group}/latest/{tiles['file']}"
        assets["pmtiles"] = {
            "href": f"{PUBLIC_BASE}/{tiles_path}",
            "type": PMTILES_TYPE,
            "title": f"{spec['title']}, vector tiles",
            "description": f"PMTiles, layer `{tiles['layer']}`, zoom {tiles['min_zoom']} to "
                           f"{tiles['max_zoom']}, {tiles['tiles']:,} tiles; properties "
                           f"{', '.join(tiles['properties'])}. Built with tylertoo "
                           f"{tiles['tylertoo']}.",
            "roles": ["visual"],
            "file:size": tiles["bytes"],
            "file:checksum": "1220" + tiles["sha256"],
            "alternate": asset_s3(tiles_path),
        }
        assets["style"] = {"href": "./styles/default.json", "type": STYLE_TYPE,
                           "title": "By fire type: wildfire red, prescribed fire green",
                           "roles": ["style", "default"]}
        links.append({"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}",
                      "type": PMTILES_TYPE, "title": f"{spec['title']}, vector tiles",
                      "pmtiles:layers": [tiles["layer"]]})
        extensions.append(WEB_MAP_LINKS_EXT)
    empty = "" if entry["features"] else (
        " No fire is burning at the time of this build: the file holds the columns and no "
        "rows.")
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": extensions,
        "id": name,
        "title": spec["title"],
        "description": (
            f"{LEADS[name]} {entry['features']:,} "
            + ("perimeters" if spec["kind"] == "perimeter" else "incidents")
            + f" as of {built}.{empty} Columns as {('MTBS' if group == 'mtbs' else 'WFIGS')} "
            "names them, dates in UTC. Lon/lat geometry with a bbox covering, Hilbert-sorted, as "
            "one GeoParquet 2.0 file read in place over HTTPS or the anonymous S3 endpoint "
            f"{S3_ENDPOINT}" + (", and a PMTiles archive for web maps" if tiles else "")
            + f". {gate_text(entry, group)} {NOT_FOR_SAFETY} Credit: {CREDIT[group]}."
        ),
        "keywords": ["wildfire", "fire perimeters", "burned area", "United States", "GeoParquet",
                     "MTBS" if group == "mtbs" else "WFIGS", name],
        "license": "other",
        "attribution": CREDIT[group],
        "version": built,
        "updated": updated,
        "providers": [*PROVIDERS[group], GEOMERMAIDS],
        "extent": {
            "spatial": {"bbox": [extent]},
            "temporal": {"interval": [dates if any(dates) else [f"{built}T00:00:00Z", None]]},
        },
        "table:row_count": entry["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": column_doc(n)}
                          for n, t in columns],
        "assets": assets,
        "links": links,
    }


def build_root(collections: list[dict], updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": (
            "US wildfire perimeters as GeoParquet and PMTiles: the fires burning now and this "
            "year's, and every perimeter since 2016, from NIFC's Wildland Fire Interagency "
            "Geospatial Services (rebuilt here several times a day), this year's incident "
            "points, and the burned areas of large fires since 1984 from Monitoring Trends in "
            "Burn Severity (checked weekly). Lon/lat, columns as the sources name them. US "
            f"federal data. {NOT_FOR_SAFETY} Credit: {WFIGS_CREDIT}; {MTBS_CREDIT}."),
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK,
            VIA["wfigs"],
            VIA["mtbs"],
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


# ---------- markdown ----------

LICENSE_MD = f"""\
Works of US federal agencies, not subject to copyright in the United States
(17 U.S.C. 105). NIFC states no licence for WFIGS, only a disclaimer (the data
is provided as is, is not a legal document, and perimeters can lag the fire).
MTBS: "There are no restrictions on use, except for reasonable and proper
acknowledgement of information sources." Credit "{WFIGS_CREDIT}" for the
WFIGS layers and "{MTBS_CREDIT}" for the burned areas."""

# A point inside the 2018 Camp Fire (Paradise, California), and the
# Sierra Nevada around it for the bbox example.
POINT = (-121.6219, 39.7596)
AREA = (-122.5, 38.5, -119.5, 40.5)


def access_md(name: str) -> str:
    spec = COLLECTIONS[name]
    url = f"{latest_url(spec['group'])}/{name}.parquet"
    x0, y0, x1, y1 = AREA
    if name == "mtbs_perimeters":
        query = f"""SELECT event_id, incid_name, incid_type, ig_date, burnbndac
FROM read_parquet('{url}')
WHERE bbox.xmin <= {POINT[0]} AND bbox.xmax >= {POINT[0]}
  AND bbox.ymin <= {POINT[1]} AND bbox.ymax >= {POINT[1]}
  AND ST_Contains(geometry, ST_Point({POINT[0]}, {POINT[1]}))
ORDER BY ig_date;"""
    elif name == "current":
        query = f"""SELECT poly_IncidentName, attr_POOState, poly_GISAcres, attr_PercentContained,
       attr_FireDiscoveryDateTime
FROM read_parquet('{url}')
ORDER BY poly_GISAcres DESC LIMIT 10;  -- the largest fires burning now, nationwide"""
    elif spec["kind"] == "incident":
        query = f"""SELECT IncidentName, FireDiscoveryDateTime, IncidentSize, FireCause, POOCounty
FROM read_parquet('{url}')
WHERE bbox.xmin <= {x1} AND bbox.xmax >= {x0} AND bbox.ymin <= {y1} AND bbox.ymax >= {y0}
ORDER BY IncidentSize DESC NULLS LAST LIMIT 10;"""
    else:
        query = f"""SELECT poly_IncidentName, attr_FireDiscoveryDateTime, poly_GISAcres,
       attr_PercentContained, attr_FireOutDateTime
FROM read_parquet('{url}')
WHERE bbox.xmin <= {x1} AND bbox.xmax >= {x0} AND bbox.ymin <= {y1} AND bbox.ymax >= {y0}
ORDER BY poly_GISAcres DESC LIMIT 10;"""
    return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
{query}
```

The same file at `s3://{BUCKET}/{DATASET_PREFIX}/{spec['group']}/latest/{name}.parquet` on
the anonymous S3 endpoint `{S3_ENDPOINT}`."""


def collection_readme(col: dict, name: str, entry: dict, tiles: dict | None) -> str:
    spec = COLLECTIONS[name]
    url = latest_url(spec["group"])
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    tiles_row = (f"| Map tiles | `{tiles['file']}`, {tiles['bytes'] / 1e6:,.1f} MB, zoom "
                 f"{tiles['min_zoom']} to {tiles['max_zoom']} |\n" if tiles else "")
    tiles_md = (f"""
## Map tiles

`{url}/{tiles['file']}`, tile layer `{tiles['layer']}`, properties
{', '.join(f"`{p}`" for p in tiles['properties'])}. `styles/default.json` colours
the perimeters by fire type: wildfire red, prescribed fire green.
""" if tiles else "")
    provenance = (
        f"`{MTBS_ZIP}`, the national shapefile, rebuilt here when its content changes "
        "(checked weekly; the zip itself is regenerated daily)" if spec["group"] == "mtbs" else
        f"the hosted layer `{WFIGS[name]['service']}` on NIFC's ArcGIS server, read in pages of "
        "2,000 by OBJECTID several times a day, when its lastEditDate has moved")
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, {entry['row_groups']} row groups, sha256 `{entry['sha256'][:12]}...` |
| Rows | {entry['features']:,} |
{tiles_row}| CRS | lon/lat, OGC:CRS84 |
| Updated | `{url}/_manifest.json` gives the build time and the source's edit date |

## Access

{access_md(name)}
{tiles_md}
## Columns

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

From {provenance}. [fire.py]({REPO_URL}/blob/main/scripts/datasets/fire.py)
keeps the columns above, turns dates into UTC timestamps, repairs invalid
polygons with ST_MakeValid, adds the `bbox` covering, sorts along a Hilbert
curve and cuts row groups at 4 MiB of geometry; counts are checked against
the source and the footer, bbox and groups verified before upload.

## License

{LICENSE_MD}
"""


def collection_agents(col: dict, name: str) -> str:
    spec = COLLECTIONS[name]
    if name == "mtbs_perimeters":
        tips = """\
- One row per fire (`event_id`), large fires only, a year or two late: for this
  year's fires use `year_to_date`, for small ones `incidents_ytd`.
- `irwinid` joins the WFIGS layers' `attr_IrwinID` / `IrwinID` where both exist.
- Fires overlap where land re-burned: summing `burnbndac` counts a re-burn
  twice (acres burned); the union of the geometries gives the area burned at
  least once."""
    elif spec["kind"] == "incident":
        tips = """\
- Points of origin, one per incident; most never get a perimeter.
- `POOFips` is the county FIPS code: joins TIGER/Line county `GEOID`.
- `IrwinID` joins the perimeter layers' `attr_IrwinID`."""
    else:
        tips = """\
- One row per fire: its latest perimeter (`poly_` columns) with the incident's
  attributes (`attr_` columns). `attr_IrwinID` joins `incidents_ytd.IrwinID` and
  MTBS's `irwinid`.
- `attr_FireOutDateTime` is empty while the fire burns; `attr_PercentContained`
  is the last reported containment.
- `attr_IncidentTypeCategory`: WF wildfire, RX prescribed fire, CX complex.
- Perimeters can overlap (a complex and its fires; re-burns): dissolve before
  summing an area."""
    return f"""\
# {col['title']}: agent guide

{LEADS[name]} Lon/lat (OGC:CRS84), geometry column `geometry`, dates in UTC.

## Access

{access_md(name)}

## Query tips

- Filter on `bbox` before `ST_Contains` or `ST_Intersects`.
{tips}
- {NOT_FOR_SAFETY}
- Credit: {CREDIT[spec['group']]}.
"""


def root_readme(root: dict, collections: list[dict]) -> str:
    rows = "\n".join(
        f"| [{c['title']}](./{c['id']}/README.md) | `{COLLECTIONS[c['id']]['group']}/latest/"
        f"{c['id']}.parquet` | {c['table:row_count']:,} |" for c in collections)
    return f"""\
# {root['title']}

{root['description']}

| Collection | File | Rows |
|---|---|---|
{rows}

Files under `{PUBLIC_DATA}/`: `wfigs/latest/` (rebuilt several times a day) and
`mtbs/latest/` (rebuilt when MTBS changes), each with `_manifest.json` and
`ATTRIBUTION.txt`. Join to people and buildings with the other parquetry
datasets: census/ (TIGER/Line, ACS), padus/, nfhl/, addresses/.

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {CATALOG_ID}: agent guide

US wildfire perimeters and incidents, one collection per layer.

{listing}

Layout: `{PUBLIC_DATA}/wfigs/latest/<layer>.parquet` and
`{PUBLIC_DATA}/mtbs/latest/mtbs_perimeters.parquet`, anonymous S3 at
`s3://{BUCKET}/{DATASET_PREFIX}/` on `{S3_ENDPOINT}`. Lon/lat, dates UTC. Pick
the layer by time: `current` burning now, `year_to_date` this year,
`all_since_2016` WFIGS's history, `mtbs_perimeters` large fires since 1984.
The IRWIN id joins them. {NOT_FOR_SAFETY} Credit: {WFIGS_CREDIT}; {MTBS_CREDIT}.
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    manifests = {g: read_manifest(g, out_dir) for g in ("wfigs", "mtbs")}
    if not any(manifests.values()):
        sys.exit("neither group is built or published: nothing to catalog")
    collections, total = [], 0
    for name, spec in COLLECTIONS.items():
        group = spec["group"]
        manifest = manifests[group]
        if manifest is None:
            continue
        entry = {f["name"]: f for f in manifest["files"]}[name]
        tiles = {t["name"]: t for t in manifest["pmtiles"]}.get(name)
        if not (THUMBS / f"{name}.png").is_file():
            sys.exit(f"missing {THUMBS / name}.png: run the thumbnails command")
        source = data_source(group, out_dir, entry["file"])
        extent, dates = measure(source, spec["date"], entry["features"])
        col = build_collection(name, entry, manifest, tiles, extent, dates, footer(source),
                               updated)
        cdir = dest / name
        if tiles:
            write_json(cdir / "styles" / "default.json",
                       style(name, tiles, CREDIT[group], latest_url(group)))
            asset = col["assets"]["style"]
            asset["file:size"] = (cdir / asset["href"]).stat().st_size
            asset["file:checksum"] = multihash(cdir / asset["href"])
        write_json(cdir / "collection.json", col)
        shutil.copyfile(THUMBS / f"{name}.png", cdir / "thumbnail.png")
        (cdir / "README.md").write_text(collection_readme(col, name, entry, tiles))
        (cdir / "AGENTS.md").write_text(collection_agents(col, name))
        collections.append(col)
        total += entry["features"]
    root = build_root(collections, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, collections))
    (dest / "AGENTS.md").write_text(root_agents(collections))
    return total


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="fire-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        total = build(out_dir, tree)
        n = len(json.loads((tree / "catalog.json").read_text())["links"])
        print(f"  rendered {total:,} rows ({n} root links)")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

CONUS = (-125.0, 24.0, -66.5, 49.5)
# The states under the fires, so a map with a few active fires still reads.
STATES = f"{PUBLIC_BASE}/natural-earth/v5.1.2/50m/admin_1_states_provinces.parquet"


def thumbnails(out_dir: Path, only: list[str] | None = None) -> None:
    """Each layer over the conterminous US and its state outlines, coloured
    as the default style."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    from thumbnails import BACKGROUND, frame

    colours = {"WF": "#d7301f", "RX": "#41ab5d", "CX": "#7f0000", "Wildfire": "#d7301f",
               "Prescribed Fire": "#41ab5d", "Wildland Fire Use": "#fd8d3c"}
    con = connect()
    THUMBS.mkdir(parents=True, exist_ok=True)
    x0, y0, x1, y1, aspect = frame(*CONUS)
    states = []
    for (wkb,) in con.execute(f"""SELECT ST_AsWKB(geometry) FROM read_parquet('{STATES}')
                                  WHERE iso_a2 = 'US'""").fetchall():
        for part in shapely.get_parts(shapely.from_wkb(bytes(wkb))):
            if part.geom_type == "Polygon":
                states.append(shapely.get_coordinates(part.exterior))
    for name, spec in COLLECTIONS.items():
        if only and name not in only:
            continue
        path = out_dir / spec["group"] / "latest" / f"{name}.parquet"
        kind_col = ("incid_type" if name == "mtbs_perimeters" else
                    "attr_IncidentTypeCategory" if spec["kind"] == "perimeter" else
                    "IncidentTypeCategory")
        where = (f"bbox.xmax > {CONUS[0]} AND bbox.xmin < {CONUS[2]} "
                 f"AND bbox.ymax > {CONUS[1]} AND bbox.ymin < {CONUS[3]}")
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.set_facecolor(BACKGROUND)
        fig.patch.set_facecolor(BACKGROUND)
        ax.add_collection(PolyCollection(states, facecolors="#ebe7de", edgecolors="#b9b2a3",
                                         linewidths=0.4))
        if spec["kind"] == "incident":
            rows = con.execute(f"""SELECT {kind_col}, ST_X(geometry), ST_Y(geometry)
                FROM read_parquet('{path}') WHERE {where}""").fetchall()
            ax.scatter([r[1] for r in rows], [r[2] for r in rows], s=0.6, linewidths=0,
                       c=[colours.get(r[0], "#969696") for r in rows])
        else:
            rows = con.execute(f"""SELECT {kind_col},
                       ST_AsWKB(ST_SimplifyPreserveTopology(geometry, 0.01))
                FROM read_parquet('{path}') WHERE {where} AND geometry IS NOT NULL""").fetchall()
            polys, fills = [], []
            for kind, wkb in rows:
                for part in shapely.get_parts(shapely.from_wkb(bytes(wkb))):
                    if part.geom_type != "Polygon" or part.is_empty:
                        continue
                    coords = shapely.get_coordinates(part.exterior)
                    # A fire a few hundred metres across still shows as a dot.
                    if len(coords) < 4:
                        continue
                    polys.append(coords)
                    fills.append(colours.get(kind, "#969696"))
            ax.add_collection(PolyCollection(polys, facecolors=fills, edgecolors=fills,
                                             linewidths=0.6, alpha=0.8))
        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
        ax.set_aspect(aspect)
        ax.axis("off")
        fig.savefig(THUMBS / f"{name}.png", dpi=100, facecolor=BACKGROUND)
        plt.close(fig)
        print(f"  {name}: {len(rows):,} features -> {(THUMBS / f'{name}.png').relative_to(REPO)}")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp, required=False):
        sp.add_argument("--out-dir", type=Path, default=None, required=required,
                        help="fire.py's --out-dir (default: read the published files)")

    b = sub.add_parser("build")
    out_dir_arg(b)
    b.add_argument("--dest", type=Path, required=True)
    c = sub.add_parser("check")
    c.add_argument("tree", type=Path)
    u = sub.add_parser("publish")
    out_dir_arg(u)
    u.add_argument("--remote", required=True)
    u.add_argument("--dry-run", action="store_true")
    t = sub.add_parser("thumbnails")
    out_dir_arg(t, required=True)
    t.add_argument("--only", nargs="*", choices=list(COLLECTIONS))
    args = p.parse_args()
    if args.cmd == "build":
        print(f"wrote {args.dest}/ ({build(args.out_dir, args.dest):,} rows)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    elif args.cmd == "publish":
        publish(args.out_dir, args.remote, dry_run=args.dry_run)
    elif args.cmd == "thumbnails":
        thumbnails(args.out_dir, args.only)


if __name__ == "__main__":
    main()
