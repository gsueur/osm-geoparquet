#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for Geoconnex.

Two groups, as the files are laid out: reference/ (rivers, gages, dams,
watersheds, aquifers, water systems) and providers/ (one file per source
Geoconnex harvests). One STAC Collection per file, its data asset the file
itself. Counts, sizes, row groups and sha256 come from the folder's
_manifest.json, extents and column types from each file's footer, versions
from latest/_source.json: everything is read from what was just published
(over HTTPS, or from a local geoconnex.py output dir). latest/ is replaced
when upstream moves, and the catalog is rendered again in the same run, so
the checksums stay current. The prose lives here. Spec, validator (rashid),
STAC constants and uploader are shared with the other catalogs
(scripts/catalog.py).

The catalog is published beside the data, at /geoconnex/catalog/.

Usage:
  python3 scripts/datasets/geoconnex_catalog.py build --dest build/geoconnex-catalog
  python3 scripts/datasets/geoconnex_catalog.py build --out-dir out/geoconnex --dest build/geoconnex-catalog
  python3 scripts/datasets/geoconnex_catalog.py check build/geoconnex-catalog
  python3 scripts/datasets/geoconnex_catalog.py publish --remote parquetry:parquetry/geoconnex
  # the committed thumbnails, re-rendered only when the data changes a lot
  uv run --project scripts --with matplotlib --with shapely python \\
      scripts/datasets/geoconnex_catalog.py thumbnails --out-dir out/geoconnex
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import urllib.request
from email.utils import parsedate_to_datetime
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.geoconnex import GROUP_BYTES, LABELS

import catalog as shared  # scripts/catalog.py
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

DATASET_PREFIX = "geoconnex"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
DATA_URL = f"{PUBLIC_DATA}/latest"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "geoconnex-geoparquet"
THUMBS = REPO / "catalog" / "thumbnails" / "geoconnex"
GEOCONNEX_URL = "https://geoconnex.us"
DOCS_URL = "https://docs.geoconnex.us"

LICENSE = "CC0-1.0"
LICENSE_LINK = {
    "rel": "license",
    "href": "https://creativecommons.org/publicdomain/zero/1.0/",
    "type": "text/html",
    "title": "CC0 1.0: Geoconnex and its reference repositories",
}
VIA_LINK = {
    "rel": "via",
    "href": f"{DOCS_URL}/access/downloads",
    "type": "text/html",
    "title": "Geoconnex downloads: the GeoParquet export this republishes",
}

PROVIDERS = [
    {
        "name": "Internet of Water Coalition, Center for Geospatial Solutions",
        "description": "Build Geoconnex: curate the reference features and harvest the "
                       "features water data providers publish.",
        "url": GEOCONNEX_URL,
        "roles": ["producer", "licensor"],
    },
    {
        "name": "Geomermaids",
        "description": "Republished Geoconnex as GeoParquet that reads fast over HTTP: one "
                       "file per layer or source, Hilbert-sorted, bbox covering, byte-capped "
                       "row groups. Maintains and hosts this catalog and its data.",
        "url": SITE_URL,
        "email": CONTACT_EMAIL,
        "roles": ["processor", "host"],
    },
]

GROUPS = {
    "reference": (
        "Reference features",
        "The features Geoconnex curates as the common reference for US water data: rivers "
        "from head to outlet with their network, stream gages, dams, watersheds HU02 to "
        "HU12, aquifers, hydrogeologic regions and public water systems, each with the "
        "attributes its source publishes."),
    "providers": (
        "Features published by data providers",
        "The features Geoconnex harvests from water data providers, one file per source: "
        "Water Quality Portal sites, USGS monitoring locations, GNIS names, the National "
        "Geologic Map, state gages and western water sources."),
}

# Reference layers: what each holds and where it comes from.
REFERENCE_DOCS = {
    "mainstems": "Every river in the United States as one line from head to outlet, with its "
                 "length, drainage area at the outlet and the river it flows into, so the "
                 "network can be walked upstream or downstream. From the ref_rivers release "
                 "(mainstems v3).",
    "gages": "Reference stream gages: USGS and other providers' gages located on the "
             "NHDPlus V2 network, with drainage areas and the river they sit on.",
    "dams": "Reference dams from the US Army Corps of Engineers National Inventory of Dams, "
            "located on the NHDPlus V2 network where possible, with drainage areas and the "
            "river they sit on.",
    "hu02": "Two-digit hydrologic regions of the Watershed Boundary Dataset.",
    "hu04": "Four-digit hydrologic subregions of the Watershed Boundary Dataset.",
    "hu06": "Six-digit hydrologic basins of the Watershed Boundary Dataset.",
    "hu08": "Eight-digit hydrologic subbasins of the Watershed Boundary Dataset.",
    "hu10": "Ten-digit watersheds of the Watershed Boundary Dataset.",
    "hu12": "Twelve-digit subwatersheds of the Watershed Boundary Dataset, at full resolution.",
    "principal_aquifers": "USGS principal aquifers of the United States (2003 data release).",
    "national_aquifers": "USGS national aquifers, the aquifer code list of the National Water "
                         "Information System, with links to their GIS data.",
    "hydrogeologic_regions": "USGS secondary hydrogeologic regions of the conterminous "
                             "United States (2018 data release).",
    "water_systems": "Public water system service areas, keyed by the EPA Safe Drinking Water "
                     "Information System id.",
}
# Providers: Geoconnex sitemap -> (title, description).
PROVIDER_DOCS = {
    "epa:wqp": ("Water Quality Portal sites",
                "Monitoring locations of the Water Quality Portal (EPA, USGS and the National "
                "Water Quality Monitoring Council), where water quality samples are taken."),
    "usgs:monitoring_locations": ("USGS monitoring locations",
                                  "USGS monitoring locations: stream gages, wells, springs, "
                                  "lakes and other sites of the National Water Information "
                                  "System. Most also appear in the Water Quality Portal."),
    "usgs:national_geologic_map": ("National Geologic Map units",
                                   "Geologic map units of the USGS National Geologic Map "
                                   "Database, as polygons."),
    "usgs:gnis": ("GNIS names",
                  "Named features of the USGS Geographic Names Information System."),
    "usgs:sciencebase": ("ScienceBase items",
                         "USGS ScienceBase data releases, by their footprint."),
    "ca_gage_assessment:gages": ("California stream gage assessment",
                                 "Stream gages of California's gage assessment, the state's "
                                 "inventory of where streamflow is measured."),
    "iow:state_gages:cdss": ("Colorado state gages",
                             "Stream gages of the Colorado Division of Water Resources, from its "
                             "Decision Support Systems (CDSS)."),
    "iow:state_gages:mtdnrc": ("Montana state gages",
                               "Stream gages of the Montana Department of Natural Resources and "
                               "Conservation."),
    "iow:state_gages:ndwr": ("Nevada state gages",
                             "Stream gages of the Nevada Division of Water Resources."),
    "iow:state_gages:nednr": ("Nebraska state gages",
                              "Stream gages of the Nebraska Department of Natural Resources."),
    "iow:state_gages:wyseo": ("Wyoming state gages",
                              "Stream gages of the Wyoming State Engineer's Office."),
    "utah:water_systems": ("Utah water systems",
                           "Service areas of Utah's public water systems."),
    "wwdh:awdb_forecasts": ("NRCS water supply forecast points",
                            "Water supply forecast points of the NRCS Air and Water Database, "
                            "through the Western Water Data Hub."),
    "wwdh:noaa_rfc": ("NOAA River Forecast Center points",
                      "Forecast points of the NOAA River Forecast Centers, through the Western "
                      "Water Data Hub."),
    "wwdh:snotel": ("SNOTEL stations",
                    "NRCS snow telemetry (SNOTEL) stations, through the Western Water Data Hub."),
    "wwdh:usace:access_to_water": ("USACE reservoirs",
                                   "US Army Corps of Engineers reservoirs and projects (Access "
                                   "to Water), through the Western Water Data Hub."),
    "wwdh:usbr:rise": ("Bureau of Reclamation locations",
                       "Reservoirs, dams and other locations of the Bureau of Reclamation's "
                       "RISE catalog, through the Western Water Data Hub."),
}
PROVIDER_DEFAULT = "Features this source publishes to Geoconnex."

COLUMN_DOCS = {
    "uri": "Persistent Geoconnex identifier; resolves at geoconnex.us to the feature's page "
           "and the data published about it",
    "id": "Identifier within the reference layer",
    "name": "Name",
    "description": "Description, as the provider publishes it",
    "mainstem_uri": "URI of the river (mainstem) the feature sits on, when Geoconnex has "
                    "placed it on one",
    "bbox": "Bounding box of the geometry (covering), float32 rounded outward: filter on it first",
    "geometry": "Geometry, lon/lat (OGC:CRS84)",
    "huc": "Hydrologic unit code",
    "pwsid": "Public water system id (EPA SDWIS)",
    # mainstems
    "featuretype": "HY_Features types of the river",
    "downstream_mainstem_id": "URI of the river this one flows into; empty at the sea or a sink",
    "encompassing_mainstem_basins": "URIs of the larger rivers whose basin holds this one",
    "name_at_outlet": "Name of the river at its outlet",
    "name_at_outlet_gnis_id": "GNIS URI of the name at the outlet",
    "primary_name": "Most common name along the river",
    "primary_name_gnis_id": "GNIS URI of the primary name",
    "lengthkm": "Length from head to outlet, km",
    "outlet_drainagearea_sqkm": "Drainage area at the outlet, km²",
    "superseded": "True when a newer mainstem replaces this one; filter these out",
    "new_mainstemid": "The mainstem that supersedes this one",
    # gages
    "cluster": "URI of the gage this one is grouped with when several share a location",
    "dasqkm_diff": "Difference between the gage's and NHDPlus drainage areas, km²",
    "gage_totdasqkm": "Drainage area reported for the gage, km²",
    "nhdpv2_totdasqkm": "Drainage area of the NHDPlus V2 flowline, km²",
    "nhdpv2_comid": "NHDPlus V2 flowline (COMID) the feature is located on",
    "nhdpv2_link_source": "Source of the NHDPlus V2 location",
    "nhdpv2_offset_m": "Distance from the feature to the flowline, m",
    "nhdpv2_reach_measure": "Position along the NHDPlus reach, percent from downstream",
    "nhdpv2_reachcode": "NHDPlus reach code",
    "nhdpv2_reachcode_uri": "URI of the NHDPlus reach",
    "nws_url": "National Weather Service page of the gage",
    "hivis_camera_url": "USGS HIVIS camera page of the gage",
    "provider": "Agency that publishes the feature",
    "provider_id": "The feature's id at that agency",
    "subjectof": "The feature's page at that agency",
    # dams
    "drainage_area_sqkm": "Drainage area reported for the dam, km²",
    "drainage_area_sqkm_nhdpv2": "Drainage area of the NHDPlus V2 flowline, km²",
    "feature_data_source": "Where the dam record comes from",
    "index_type": "How the dam was placed on the NHDPlus network",
    # aquifers and regions
    "aq_code": "USGS aquifer code",
    "aq_name": "Aquifer name",
    "rock_name": "Rock type group",
    "rock_type": "Rock type group code",
    "sameas": "The same aquifer in the other aquifer layer",
    "nat_aqfr_cd": "National aquifer code (NWIS)",
    "link": "USGS page about the aquifer",
    "gis_data": "Aquifer GIS data",
    "gis_data2": "Aquifer GIS data, second source",
    "gis_metadata": "Metadata of the GIS data",
    "gis_metadata2": "Metadata of the second GIS data",
    "valid_states": "States where the aquifer code is valid",
    "shr": "Secondary hydrogeologic region name",
    "subprovinc": "Subprovince",
    "geologicpr": "Geologic province",
    "primarylit": "Primary lithology",
    "type": "Region type code",
}
for _end in ("head", "outlet"):
    for _col, _what in [("2020huc12", "2020 HUC12"), ("nhd_permid", "NHD permanent id"),
                        ("nhdplushr_id", "NHDPlus HR id"), ("nhdpv1_comid", "NHDPlus V1 COMID"),
                        ("nhdpv2_comid", "NHDPlus V2 COMID"), ("nhdpv2huc12", "NHDPlus V2 HUC12"),
                        ("rf1id", "Reach File 1 id")]:
        COLUMN_DOCS[f"{_end}_{_col}"] = f"{_what} at the river's {_end}"


# ---------- inputs ----------

def read_json(base: str, rel: str) -> dict:
    if base.startswith("http"):
        with urllib.request.urlopen(f"{base}/{rel}", timeout=60) as r:
            return json.load(r)
    return json.loads(Path(base, rel).read_text())


def measure(base: str) -> tuple[dict, dict]:
    """(source, layers): latest/_source.json, and per collection its group,
    manifest entry, label, extent, geometry types and columns."""
    source = read_json(base, "latest/_source.json")
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    layers = {}
    for group in GROUPS:
        manifest = read_json(base, f"latest/{group}/_manifest.json")
        for e in manifest["files"]:
            theme = e["theme"]
            f = f"{base}/latest/{group}/{e['file']}"
            geo = json.loads(con.execute(
                f"SELECT decode(value) FROM parquet_kv_metadata('{f}') WHERE key = 'geo'").fetchone()[0])
            col = geo["columns"]["geometry"]
            columns = [(r[0], "GEOMETRY" if r[1].startswith("GEOMETRY") else r[1])
                       for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{f}')").fetchall()]
            missing = [n for n, _ in columns if n not in COLUMN_DOCS]
            if missing:
                sys.exit(f"{group}/{theme}: no description for columns {missing}")
            b = col["bbox"]
            layers[theme] = {
                "group": group, "entry": e, "label": manifest["labels"][theme],
                "bbox": [max(-180.0, round(b[0], 6)), max(-90.0, round(b[1], 6)),
                         min(180.0, round(b[2], 6)), min(90.0, round(b[3], 6))],
                "types": col["geometry_types"], "columns": columns,
            }
    return source, layers


def interval(source: dict) -> list[str]:
    """From the Geoconnex export's date to the day this was built."""
    start = parsedate_to_datetime(source["export"]["last_modified"]).strftime("%Y-%m-%dT%H:%M:%SZ")
    return [start, f"{source['built']}T00:00:00Z"]


def title_of(theme: str, layer: dict) -> str:
    if layer["group"] == "reference":
        return LABELS[theme]
    return PROVIDER_DOCS.get(layer["label"], (layer["label"], None))[0]


def describe(theme: str, layer: dict) -> str:
    if layer["group"] == "reference":
        return REFERENCE_DOCS[theme]
    doc = PROVIDER_DOCS.get(layer["label"], (None, PROVIDER_DEFAULT))[1]
    return f"{doc} Geoconnex sitemap `{layer['label']}`."


# ---------- STAC ----------

def data_asset(theme: str, layer: dict) -> dict:
    e = layer["entry"]
    path = f"{DATASET_PREFIX}/latest/{layer['group']}/{e['file']}"
    return {
        "href": f"{PUBLIC_BASE}/{path}",
        "type": PARQUET_TYPE,
        "title": title_of(theme, layer),
        "description": f"{e['features']:,} features in {e['row_groups']:,} row groups of at most "
                       f"{GROUP_BYTES >> 20} MiB, Hilbert-sorted: a bbox filter reads only the "
                       f"groups it touches.",
        "roles": ["data"],
        "file:size": e["bytes"],
        "file:checksum": "1220" + e["sha256"],
        "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                             "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
    }


def build_collection(theme: str, layer: dict, span: list[str], updated: str) -> dict:
    e = layer["entry"]
    title = title_of(theme, layer)
    thumb = THUMBS / f"{theme}.png"
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, VERSION_EXT, ALTERNATE_EXT],
        "id": theme,
        "title": title,
        "description": (
            f"{describe(theme, layer)} From Geoconnex (Internet of Water): {e['features']:,} "
            f"features in one GeoParquet file ({e['bytes'] / 1e6:,.1f} MB), Hilbert-sorted "
            f"with a bbox covering, so a bbox filter reads only the row groups it touches. "
            f"Read in place over HTTPS or through the anonymous S3 endpoint {S3_ENDPOINT}. "
            f"This collection reads latest/, replaced when Geoconnex publishes a new export."
        ),
        "keywords": ["Geoconnex", "Internet of Water", "water", "hydrology", "United States",
                     "GeoParquet", layer["group"], theme.replace("_", " ")],
        "license": LICENSE,
        "version": span[-1][:10],
        "updated": updated,
        "providers": PROVIDERS,
        "extent": {"spatial": {"bbox": [layer["bbox"]]}, "temporal": {"interval": [span]}},
        "table:row_count": e["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": COLUMN_DOCS[n]}
                          for n, t in layer["columns"]],
        "assets": {
            "data": data_asset(theme, layer),
            "thumbnail": {
                "href": "./thumbnail.png",
                "type": "image/png",
                "title": "Density over the conterminous United States",
                "roles": ["thumbnail"],
                "file:size": thumb.stat().st_size,
                "file:checksum": multihash(thumb),
            },
        },
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{title}: README"),
            md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "alternate", "href": f"{DATA_URL}/{layer['group']}/", "type": "text/html",
             "title": "Browse the latest files"},
        ],
    }


def build_group(group: str, collections: list[dict], span: list[str], updated: str) -> dict:
    title, description = GROUPS[group]
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": f"{CATALOG_ID}-{group}",
        "title": title,
        "description": f"{description} From Geoconnex (Internet of Water).",
        "version": span[-1][:10],
        "updated": updated,
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json",
               "type": "application/json", "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{title}: README"),
            md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
            LICENSE_LINK,
        ],
    }


def build_root(layers: dict, source: dict, span: list[str], updated: str) -> dict:
    n_ref = sum(1 for l in layers.values() if l["group"] == "reference")
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, VERSION_EXT],
        "id": CATALOG_ID,
        "title": "Geoconnex: US water reference features and monitoring sites as GeoParquet",
        "description": (
            "Geoconnex (Internet of Water) links US water data to the places it describes. "
            f"This republishes it as GeoParquet that reads fast over HTTP: {n_ref} reference "
            "layers with their full attributes (852,673 rivers head to outlet with their "
            "network, stream gages, dams, watersheds HU02 to HU12, aquifers, public water "
            f"systems) and {len(layers) - n_ref} files of the features data providers publish "
            "(4.6 million Water Quality Portal and USGS monitoring sites, GNIS, the National "
            "Geologic Map, state gages). One file per layer or source, Hilbert-sorted with a "
            "bbox covering and row groups of at most 4 MiB: a small bbox reads a few MB. "
            f"Geoconnex export of {span[0][:10]}, checked weekly. CC0."
        ),
        "version": span[-1][:10],
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{g}/catalog.json", "type": "application/json",
               "title": GROUPS[g][0]} for g in GROUPS),
            md_link("describedby", "./README.md", "Catalog README"),
            md_link("agents", "./AGENTS.md", "Catalog agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            LICENSE_LINK,
            VIA_LINK,
            {"rel": "about", "href": GEOCONNEX_URL, "type": "text/html",
             "title": "Geoconnex, Internet of Water"},
            {"rel": "related", "href": f"{DATA_URL}/ATTRIBUTION.txt", "type": "text/plain",
             "title": "Attribution, sources and what was changed"},
            {"rel": "related", "href": f"{DATA_URL}/_source.json", "type": "application/json",
             "title": "Versions read: the Geoconnex export, the rivers release, the API counts"},
            {"rel": "vcs", "href": REPO_URL, "type": "text/html",
             "title": "Builder and catalog source"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
            {"rel": "alternate", "href": SITE_URL, "type": "text/html", "title": "Project site"},
        ],
    }


# ---------- markdown ----------

LICENSE_MD = f"""\
[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/): no attribution
required. Please credit "Geoconnex, Internet of Water" and the agency behind
each source (USGS, USACE, EPA, the state agencies); the notice is in
[ATTRIBUTION.txt]({DATA_URL}/ATTRIBUTION.txt)."""

PROVENANCE = f"""\
Built by [geoconnex.py]({REPO_URL}/blob/main/scripts/datasets/geoconnex.py)
from the Geoconnex GeoParquet export ({DOCS_URL}/access/downloads), the
`mainstems.gpkg` of the [ref_rivers](https://github.com/internetofwater/ref_rivers)
release, and the gages, dams and aquifer collections of
[reference.geoconnex.us](https://reference.geoconnex.us). Features and
attributes are unchanged; column names are lower snake case. Each file is
sorted along a Hilbert curve over its own extent, gets a float32 `bbox`
covering rounded outward, and is cut into row groups of at most
{GROUP_BYTES >> 20} MiB uncompressed. Every file is checked (geo footer,
covering, bbox bounds, extent, GEOMETRY type, row groups, row count) before
publication. Census layers (states, counties, places) are not republished."""


def access_section(path: str) -> str:
    return f"""\
Over HTTPS, no credentials. Filter on `bbox` first, here a box around Boston:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT count(*) FROM read_parquet('{DATA_URL}/{path}')
WHERE bbox.xmin <= -71.0 AND bbox.xmax >= -71.1 AND bbox.ymin <= 42.4 AND bbox.ymax >= 42.3;
```

`bbox` is float32: compare it with literals or `::FLOAT` values. DuckDB casts
the column when the bounds are DOUBLE (any computed value) and then skips no
row group, 5 to 10 times slower.

The same file is `s3://{BUCKET}/{DATASET_PREFIX}/latest/{path}` on the
anonymous S3 endpoint `{S3_ENDPOINT}` (path-style, empty credentials)."""


def collection_readme(col: dict, layer: dict, span: list[str]) -> str:
    e = layer["entry"]
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |"
                       for c in col["table:columns"])
    return f"""\
# {col['title']}

{describe(col['id'], layer)}

| | |
|---|---|
| Features | {e['features']:,} |
| File | `{layer['group']}/{e['file']}`, {e['bytes'] / 1e6:,.1f} MB, {e['row_groups']:,} row groups |
| Geometry | {", ".join(layer['types'])}, lon/lat (OGC:CRS84) |
| Extent | {shared.fmt_bbox(layer['bbox'])} |
| Data as of | Geoconnex export of {span[0][:10]}, built {span[-1][:10]} |
| License | CC0 1.0 |

## Access

{access_section(f"{layer['group']}/{e['file']}")}

## Schema

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

{PROVENANCE}

## License

{LICENSE_MD}
"""


def collection_agents(col: dict, layer: dict) -> str:
    e = layer["entry"]
    tips = {
        "mainstems": "- Names repeat (two Colorado Rivers): pick by `outlet_drainagearea_sqkm` or "
                     "place. Drop `superseded` rows.\n- Walk the network with a recursive CTE on "
                     "`downstream_mainstem_id` (upstream: rows whose `downstream_mainstem_id` is "
                     "the river's `uri`). Read only `uri` and `downstream_mainstem_id` for that.\n",
        "dams": "- `mainstem_uri` is set for about 21% of dams: a count along a river misses the "
                "others. For a whole basin, test dams against the watershed polygons.\n",
        "gages": "- `mainstem_uri` is set for about half the gages.\n",
        "epa_wqp": "- USGS sites appear here and in `usgs_monitoring_locations`: count distinct "
                   "site numbers (`NWIS/USGS-<id>` here, `monitoring-location/USGS-<id>` there).\n",
    }.get(col["id"], "")
    return f"""\
# {col['title']}: agent guide

{describe(col['id'], layer)} One row per feature, OGC:CRS84.

## Access

{access_section(f"{layer['group']}/{e['file']}")}

## Query tips

- One file, `{DATA_URL}/{layer['group']}/{e['file']}`. Filter on `bbox` for a
  window, and project only the columns you need; `geometry` is the heavy one.
{tips}- Every `uri` resolves at geoconnex.us to the feature's page and its data:
  give it in answers.
- CC0. Credit "Geoconnex, Internet of Water" and the source agency anyway.
"""


def group_readme(cat: dict, collections: list[dict], layers: dict) -> str:
    rows = "\n".join(
        f"| [{c['title']}](./{c['id']}/README.md) | `{c['id']}` | "
        f"{layers[c['id']]['entry']['features']:,} | "
        f"{layers[c['id']]['entry']['bytes'] / 1e6:,.1f} MB |" for c in collections)
    return f"""\
# {cat['title']}

{cat['description']}

| Collection | File | Features | Size |
|---|---|---|---|
{rows}
"""


def group_agents(cat: dict, collections: list[dict], group: str) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {cat['title']}: agent guide

{cat['description']}

## Collections

{listing}

Each collection is one file, `{DATA_URL}/{group}/<id>.parquet`; the catalog's
root AGENTS.md (../AGENTS.md) has the access and conventions.
"""


def root_readme(root: dict, collections: list[dict], layers: dict, span: list[str]) -> str:
    rows = "\n".join(
        f"| [{c['title']}](./{layers[c['id']]['group']}/{c['id']}/README.md) | "
        f"`{layers[c['id']]['group']}/{c['id']}` | {layers[c['id']]['entry']['features']:,} | "
        f"{layers[c['id']]['entry']['bytes'] / 1e6:,.1f} MB |" for c in collections)
    return f"""\
# {root['title']}

{root['description']}

| Collection | File | Features | Size |
|---|---|---|---|
{rows}

## Versions

Only `latest/` is published. A weekly job compares the Geoconnex export and
the rivers release with `latest/_source.json` and rebuilds when either moved.
This build: Geoconnex export of {span[0][:10]}, built {span[-1][:10]}.

## Access

Every collection is one file under `{DATA_URL}/reference/` or
`{DATA_URL}/providers/`, readable in place with HTTP range requests. Filter
on `bbox` (float32: compare with literals or `::FLOAT`). The same paths exist
under `s3://{BUCKET}/{DATASET_PREFIX}/latest/` on the anonymous S3 endpoint
`{S3_ENDPOINT}`. GeoPQ Workbench has the dataset built in as a repository.

## Provenance

{PROVENANCE}

## License

{LICENSE_MD}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(collections: list[dict], layers: dict) -> str:
    listing = "\n".join(
        f"- `{c['id']}`: {c['title']}. {layers[c['id']]['group']}/{c['id']}/AGENTS.md"
        for c in collections)
    return f"""\
# {CATALOG_ID}: agent guide

Geoconnex (Internet of Water), US water reference features and the features
water data providers publish, as GeoParquet 2.0. One collection per file.

## Collections

{listing}

## Access

- `{DATA_URL}/reference/<id>.parquet` and `{DATA_URL}/providers/<id>.parquet`
  over HTTPS, no credentials (each collection's `data` asset), or the same
  path on the anonymous S3 endpoint `{S3_ENDPOINT}`, path-style, empty
  credentials.
- Filter on `bbox` first. It is float32: compare it with literals or
  `::FLOAT` values, never computed DOUBLEs (no row group is skipped then).

## Conventions

- Geometry is native Parquet GEOMETRY, OGC:CRS84; `bbox` is the covering.
- `uri` is the Geoconnex persistent identifier; it resolves to the feature's
  page and data. `mainstem_uri` links a feature to the river it sits on.
- Rivers (`mainstems`) form a network through `downstream_mainstem_id`.
- CC0. Credit "Geoconnex, Internet of Water" and the source agency.
"""


# ---------- build ----------

def build(base: str, dest: Path, *, updated: str | None = None) -> dict:
    """Render the catalog tree into dest (replaced). Returns the layers."""
    source, layers = measure(base)
    for theme in layers:
        if not (THUMBS / f"{theme}.png").is_file():
            sys.exit(f"missing thumbnail {THUMBS}/{theme}.png: run the thumbnails command")
    span = interval(source)
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    collections = []
    for theme, layer in layers.items():
        col = build_collection(theme, layer, span, updated)
        cdir = dest / layer["group"] / theme
        write_json(cdir / "collection.json", col)
        shutil.copyfile(THUMBS / f"{theme}.png", cdir / "thumbnail.png")
        (cdir / "README.md").write_text(collection_readme(col, layer, span))
        (cdir / "AGENTS.md").write_text(collection_agents(col, layer))
        collections.append(col)
    for group in GROUPS:
        members = [c for c in collections if layers[c["id"]]["group"] == group]
        cat = build_group(group, members, span, updated)
        write_json(dest / group / "catalog.json", cat)
        (dest / group / "README.md").write_text(group_readme(cat, members, layers))
        (dest / group / "AGENTS.md").write_text(group_agents(cat, members, group))
    root = build_root(layers, source, span, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(root, collections, layers, span))
    (dest / "AGENTS.md").write_text(root_agents(collections, layers))
    return layers


def check(tree: Path) -> int:
    # Metadata pass: the data hrefs are remote and checked by the build.
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(base: str, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix="geoconnex-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        layers = build(base, tree)
        print(f"  rendered {len(layers)} collections, "
              f"{sum(l['entry']['features'] for l in layers.values()):,} features")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

CONUS = (-125.0, 24.0, -66.5, 49.5)
COLORS = {"reference": "#2f6db5", "providers": "#1f8a70"}
# Polygon files with fewer features are drawn as shapes: a density grid of
# 22 regions or 64 aquifers is a scatter of dots.
SHAPES_BELOW = 30_000


def thumbnails(out_dir: Path) -> None:
    """A map per file over the conterminous US: the shapes of small polygon
    files, simplified; otherwise features per cell, log-scaled, placed by
    their bbox centre."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.colors import LinearSegmentedColormap
    from thumbnails import BACKGROUND, frame

    x0, y0, x1, y1, aspect = frame(*CONUS)
    nx, ny = 600, 400
    con = duckdb.connect()
    THUMBS.mkdir(parents=True, exist_ok=True)
    con.execute("INSTALL spatial; LOAD spatial")
    for group in GROUPS:
        for f in sorted((out_dir / "latest" / group).glob("*.parquet")):
            n, polygons = con.execute(f"""
                SELECT count(*), bool_and(ST_GeometryType(geometry)::VARCHAR LIKE '%POLYGON')
                FROM read_parquet('{f}')""").fetchone()
            if polygons and n < SHAPES_BELOW:
                draw_shapes(con, f, group, (x0, y0, x1, y1, aspect))
                continue
            cells = con.execute(f"""
                SELECT floor(((bbox.xmin + bbox.xmax) / 2 - {x0}) / {(x1 - x0) / nx})::INT AS i,
                       floor(((bbox.ymin + bbox.ymax) / 2 - {y0}) / {(y1 - y0) / ny})::INT AS j,
                       count(*) AS n
                FROM read_parquet('{f}') GROUP BY 1, 2""").fetchall()
            grid = np.zeros((ny, nx))
            for i, j, n in cells:
                if 0 <= i < nx and 0 <= j < ny:
                    grid[j, i] = n
            cmap = LinearSegmentedColormap.from_list(f.stem, [BACKGROUND, COLORS[group]])
            fig = plt.figure(figsize=(6, 4), dpi=100)
            ax = fig.add_axes((0, 0, 1, 1))
            ax.imshow(np.log1p(grid), origin="lower", extent=(x0, x1, y0, y1), cmap=cmap,
                      vmin=0, vmax=max(np.log1p(grid).max(), 1) * 0.8, interpolation="nearest")
            ax.set_aspect(aspect)
            ax.axis("off")
            fig.patch.set_facecolor(BACKGROUND)
            path = THUMBS / f"{f.stem}.png"
            fig.savefig(path, dpi=100, facecolor=BACKGROUND)
            plt.close(fig)
            print(f"  {group}/{f.stem:32} {int(grid.sum()):>11,} in CONUS -> {path.relative_to(REPO)}")


def draw_shapes(con, f: Path, group: str, box: tuple) -> None:
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection
    from thumbnails import BACKGROUND

    x0, y0, x1, y1, aspect = box
    rows = con.execute(f"""
        SELECT ST_AsWKB(ST_SimplifyPreserveTopology(geometry, {(x1 - x0) / 1200}))
        FROM read_parquet('{f}')
        WHERE bbox.xmin <= {x1} AND bbox.xmax >= {x0} AND bbox.ymin <= {y1} AND bbox.ymax >= {y0}
    """).fetchall()
    rings = []
    for (wkb,) in rows:
        g = shapely.from_wkb(bytes(wkb))
        for poly in getattr(g, "geoms", [g]):
            if poly.geom_type == "Polygon" and not poly.is_empty:
                rings.append(list(poly.exterior.coords))
    fig = plt.figure(figsize=(6, 4), dpi=100)
    ax = fig.add_axes((0, 0, 1, 1))
    ax.add_collection(PolyCollection(rings, facecolors=COLORS[group] + "40",
                                     edgecolors=COLORS[group], linewidths=0.5))
    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.set_aspect(aspect)
    ax.axis("off")
    fig.patch.set_facecolor(BACKGROUND)
    path = THUMBS / f"{f.stem}.png"
    fig.savefig(path, dpi=100, facecolor=BACKGROUND)
    plt.close(fig)
    print(f"  {group}/{f.stem:32} {len(rows):>11,} shapes -> {path.relative_to(REPO)}")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build", help="render the catalog tree")
    b.add_argument("--out-dir", type=Path, help="a geoconnex.py output dir (default: the published files)")
    b.add_argument("--dest", type=Path, required=True)
    c = sub.add_parser("check", help="validate a rendered tree with rashid")
    c.add_argument("tree", type=Path)
    u = sub.add_parser("publish", help="render, validate and upload")
    u.add_argument("--out-dir", type=Path, help="a geoconnex.py output dir (default: the published files)")
    u.add_argument("--remote", required=True, help="e.g. parquetry:parquetry/geoconnex")
    u.add_argument("--dry-run", action="store_true")
    t = sub.add_parser("thumbnails", help="render the committed thumbnails from a local build")
    t.add_argument("--out-dir", type=Path, required=True)
    args = p.parse_args()

    if args.cmd == "thumbnails":
        thumbnails(args.out_dir)
        return
    if args.cmd == "check":
        sys.exit(check(args.tree))
    base = str(args.out_dir.resolve()) if args.out_dir else PUBLIC_DATA
    if args.cmd == "build":
        layers = build(base, args.dest)
        print(f"  {len(layers)} collections -> {args.dest}")
    else:
        publish(base, args.remote, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
