#!/usr/bin/env python3
"""
Render, validate and publish the Portolan catalog for Geobox's samples.

  geobox-samples/catalog/catalog.json                    root: Geobox samples
  geobox-samples/catalog/<sample>/collection.json        one per sample (nyc-buildings, la-neighborhoods)

geobox_samples.py's SAMPLES drives it. Each collection carries its producer and licence (`providers`, `license`, a
license link): Geobox reads them when a newcomer adds the sample from Get started and credits the dataset with them,
on its maps and in the box. Each also has its GeoParquet (`data`), its PMTiles (a `visual` asset and a `rel: pmtiles`
link, web-map-links v1.3.0), a MapLibre style, a thumbnail, and a README and an AGENTS guide. Counts, sizes and
checksums come from the sample's _manifest.json, local (--out-dir) or published. Spec: portolan-spec v0.2.0.
Validator: rashid, through scripts/catalog.py.

Usage:
  python3 scripts/datasets/geobox_samples_catalog.py build --dest build/geobox-samples-catalog [--out-dir out/geobox-samples]
  python3 scripts/datasets/geobox_samples_catalog.py check build/geobox-samples-catalog
  python3 scripts/datasets/geobox_samples_catalog.py publish --remote parquetry:parquetry/geobox-samples [--out-dir ...]
  uv run --project scripts --with matplotlib --with shapely \\
      python scripts/datasets/geobox_samples_catalog.py thumbnails --out-dir out/geobox-samples
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from datasets.gaul_catalog import STYLE_TYPE, WEB_MAP_LINKS_EXT
from datasets.geobox_samples import SAMPLES
from datasets.pmtiles import PMTILES_TYPE

import catalog as shared  # scripts/catalog.py
from catalog import (
    ALTERNATE_EXT, BUCKET, CONTACT_EMAIL, FILE_EXT, LOGO, PARQUET_TYPE, PORTOLAN_SCHEMA, PUBLIC_BASE, REPO, REPO_URL,
    S3_ENDPOINT, SITE_URL, TABLE_EXT, md_link, multihash, now_rfc3339, write_json,
)

DATASET_PREFIX = "geobox-samples"
PUBLIC_DATA = f"{PUBLIC_BASE}/{DATASET_PREFIX}"
CATALOG_PREFIX = "catalog"
CATALOG_URL = f"{PUBLIC_DATA}/{CATALOG_PREFIX}"
CATALOG_ID = "geobox-samples"
TITLE = "Geobox samples"
GEOBOX_URL = "https://box.geomermaids.com/"
THUMBS = REPO / "catalog" / "thumbnails" / "geobox-samples"
GEOMERMAIDS = {
    "name": "Geomermaids",
    "description": "Kept the useful columns, converted them to GeoParquet, tiled them, and maintains and hosts this "
                   "catalog and its data for Geobox.",
    "url": SITE_URL,
    "email": CONTACT_EMAIL,
    "roles": ["processor", "host"],
}

# Per sample: the collection id, licence, producer, keywords, colour and column docs. The texts a person reads in
# Geobox come from here: the producer's name is the dataset's credit.
CATALOG = {
    "nyc": {
        "id": "nyc-buildings",
        "license": "other",
        "license_link": {"rel": "license", "href": "https://www.nyc.gov/main/terms-of-use", "type": "text/html",
                         "title": "NYC.gov terms of use, which NYC Open Data's data sets follow"},
        "producer": {
            "name": "City of New York, Office of Technology and Innovation (OTI)",
            "description": "Maintains the BUILDING dataset, every building footprint in the city, on NYC Open Data.",
            "url": "https://data.cityofnewyork.us/d/5zhs-2jue",
            "roles": ["producer", "licensor"],
        },
        "summary": ("Every building footprint in New York City, from the City's BUILDING dataset on NYC Open Data, "
                    "maintained by the Office of Technology and Innovation (OTI)."),
        "keywords": ["buildings", "footprints", "New York City", "NYC Open Data", "roof height"],
        "colour": "#2f6db5",
        "example": ("SELECT name, height_roof_ft, year_built", "WHERE height_roof_ft > 700 ORDER BY height_roof_ft DESC"),
        "columns": {
            "name": "The building's name, where it has one (about 2,200 do).",
            "bin": "Building Identification Number (BIN), the City's identifier for the building.",
            "height_roof_ft": "Roof height above ground, in feet. Null where unknown (0 in the source).",
            "ground_elevation_ft": "Ground elevation at the building, in feet above sea level. Null where unknown.",
            "year_built": "Year the building was constructed. Null where unknown (0 in the source).",
            "feature_code": "The source's feature code, the kind of structure (2100: building; others for garages, "
                            "sheds, towers and the like, documented on NYC Open Data).",
        },
    },
    "la": {
        "id": "la-neighborhoods",
        "license": "CC-BY-4.0",
        "license_link": {"rel": "license", "href": "https://creativecommons.org/licenses/by/4.0/",
                         "type": "text/html", "title": "Creative Commons Attribution 4.0 International"},
        "producer": {
            "name": "Los Angeles Times, via the City of Los Angeles GeoHub",
            "description": "The Los Angeles Times Data Desk drew the boundaries (Mapping L.A.); the City of Los Angeles "
                           "publishes them on its GeoHub under CC BY 4.0.",
            "url": "https://geohub.lacity.org/datasets/lahub::la-times-neighborhood-boundaries",
            "roles": ["producer", "licensor"],
        },
        "summary": ("The 114 neighborhoods of the City of Los Angeles as drawn by the Los Angeles Times Data Desk "
                    "(Mapping L.A.), published by the City of Los Angeles on its GeoHub."),
        "keywords": ["neighborhoods", "Los Angeles", "Mapping L.A.", "boundaries"],
        "colour": "#0e8a7e",
        "example": ("SELECT name", "WHERE bbox.xmin <= -118.2437 AND bbox.xmax >= -118.2437 "
                                   "AND bbox.ymin <= 34.0522 AND bbox.ymax >= 34.0522 "
                                   "AND ST_Contains(geometry, ST_Point(-118.2437, 34.0522))"),
        "columns": {"name": "The neighborhood's name, as the Los Angeles Times gives it."},
    },
}
COMMON_COLUMNS = {
    "geometry": "The outline as native Parquet GEOMETRY (WKB), longitude and latitude (OGC:CRS84), 2D.",
    "bbox": "Per-row bounding box (xmin, ymin, xmax, ymax), declared as the GeoParquet covering. Filter on it to "
            "prune row groups.",
}


def column_doc(key: str, name: str) -> str:
    doc = CATALOG[key]["columns"].get(name) or COMMON_COLUMNS.get(name)
    if not doc:
        sys.exit(f"{key}: no column doc for {name!r}")
    return doc


# ---------- measured inputs ----------

def read_manifest(out_dir: Path | None, key: str) -> dict:
    local = out_dir / key / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    with urllib.request.urlopen(f"{PUBLIC_DATA}/{key}/_manifest.json") as r:
        return json.load(r)


def data_source(out_dir: Path | None, key: str, file: str) -> str:
    local = out_dir / key / file if out_dir else None
    return str(local) if local and local.is_file() else f"{PUBLIC_DATA}/{key}/{file}"


def measure(source: str) -> tuple[list[float], list[tuple[str, str]]]:
    """The extent (measured on the data) and the footer's columns."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    src = f"read_parquet('{source}')"
    x0, y0, x1, y1 = con.execute(f"SELECT min(bbox.xmin), min(bbox.ymin), max(bbox.xmax), max(bbox.ymax) FROM {src}").fetchone()
    cols = [(r[0], "GEOMETRY" if r[1].upper().startswith("GEOMETRY") else r[1])
            for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()]
    return [round(x0, 6), round(y0, 6), round(x1, 6), round(y1, 6)], cols


# ---------- styles ----------

def style(key: str, tiles: dict) -> dict:
    spec, c = SAMPLES[key], CATALOG[key]
    return {
        "version": 8, "name": spec["title"],
        "sources": {"data": {"type": "vector", "url": f"pmtiles://{PUBLIC_DATA}/{key}/{tiles['file']}",
                             "attribution": c["producer"]["name"]}},
        "layers": [
            {"id": f"{key}-fill", "type": "fill", "source": "data", "source-layer": tiles["layer"],
             "paint": {"fill-color": c["colour"], "fill-opacity": 0.45}},
            {"id": f"{key}-outline", "type": "line", "source": "data", "source-layer": tiles["layer"],
             "paint": {"line-color": c["colour"], "line-width": ["interpolate", ["linear"], ["zoom"], 8, 0.2, 15, 1]}},
        ],
    }


# ---------- STAC ----------

def tiles_text(tiles: dict) -> str:
    bands = tiles.get("bands")
    how = ("; ".join(f"zoom {b['zooms'][0]} to {b['zooms'][1]}: "
                     + (f"properties {', '.join(b['properties'])}" if b["properties"] else "every feature as a small square, geometry only")
                     for b in bands) if bands else f"properties {', '.join(tiles['properties'])}")
    return (f"PMTiles, layer `{tiles['layer']}`, zoom {tiles['min_zoom']} to {tiles['max_zoom']}, {tiles['tiles']:,} tiles "
            f"({how}). Built with tylertoo {tiles['tylertoo']}.")


def build_collection(key: str, entry: dict, tiles: dict, extent: list[float], columns: list[tuple[str, str]],
                     manifest: dict, updated: str) -> dict:
    spec, c = SAMPLES[key], CATALOG[key]
    path, tiles_path = f"{DATASET_PREFIX}/{key}/{entry['file']}", f"{DATASET_PREFIX}/{key}/{tiles['file']}"
    kept = ", ".join(f"`{n}`" for n, _ in columns if n not in COMMON_COLUMNS)
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, ALTERNATE_EXT, WEB_MAP_LINKS_EXT],
        "id": c["id"],
        "title": spec["title"],
        "description": (f"{c['summary']} {entry['features']:,} features. Kept: {kept}, typed; 2D lon/lat geometry with a "
                        f"bbox covering, Hilbert-sorted, as one GeoParquet 2.0 file read in place over HTTPS or the "
                        f"anonymous S3 endpoint {S3_ENDPOINT}, and a PMTiles archive for web maps. Offered as a sample "
                        f"in Geobox ({GEOBOX_URL}). Credit: {c['producer']['name']}."),
        "keywords": [*c["keywords"], "Geobox", "GeoParquet", "PMTiles"],
        "license": c["license"],
        "updated": updated,
        "providers": [c["producer"], GEOMERMAIDS],
        "extent": {
            "spatial": {"bbox": [extent]},
            "temporal": {"interval": [[f"{manifest['source']['retrieved']}T00:00:00Z", None]]},
        },
        "table:row_count": entry["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t, "description": column_doc(key, n)} for n, t in columns],
        "assets": {
            "data": {
                "href": f"{PUBLIC_BASE}/{path}", "type": PARQUET_TYPE, "title": spec["title"],
                "description": f"{entry['features']:,} features in {entry['row_groups']} row groups of at most 4 MiB of geometry.",
                "roles": ["data"], "file:size": entry["bytes"], "file:checksum": "1220" + entry["sha256"],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}", "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            "pmtiles": {
                "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE, "title": f"{spec['title']}, vector tiles",
                "description": tiles_text(tiles), "roles": ["visual"],
                "file:size": tiles["bytes"], "file:checksum": "1220" + tiles["sha256"],
                "alternate": {"s3": {"href": f"s3://{BUCKET}/{tiles_path}", "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
            },
            "style": {"href": "./styles/default.json", "type": STYLE_TYPE, "title": "Default style", "roles": ["style", "default"]},
            "thumbnail": {
                "href": "./thumbnail.png", "type": "image/png", "title": f"{spec['title']}, overview map", "roles": ["thumbnail"],
                "file:size": (THUMBS / f"{key}.png").stat().st_size, "file:checksum": multihash(THUMBS / f"{key}.png"),
            },
        },
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{spec['title']}: README"),
            md_link("agents", "./AGENTS.md", f"{spec['title']}: agent guide"),
            c["license_link"],
            {"rel": "via", "href": spec["page"], "type": "text/html", "title": "The original source"},
            {"rel": "related", "href": f"{PUBLIC_DATA}/{key}/ATTRIBUTION.txt", "type": "text/plain",
             "title": "Attribution and what was changed"},
            {"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}", "type": PMTILES_TYPE,
             "title": f"{spec['title']}, vector tiles", "pmtiles:layers": [tiles["layer"]]},
        ],
    }


def build_root(collections: list[dict], updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": CATALOG_ID,
        "title": TITLE,
        "description": ("Open datasets offered as samples in Geobox's Get started (box.geomermaids.com), as GeoParquet "
                        "2.0 with map tiles, each credited to its producer under its own licence."),
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{CATALOG_URL}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json", "title": c["title"]}
              for c in collections),
            md_link("describedby", "./README.md", f"{TITLE}: README"),
            md_link("agents", "./AGENTS.md", f"{TITLE}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            {"rel": "license", "href": f"{CATALOG_URL}/README.md", "type": "text/markdown",
             "title": "Each collection carries its own licence"},
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html", "title": "Report a problem"},
            {"rel": "alternate", "href": GEOBOX_URL, "type": "text/html", "title": "Geobox"},
        ],
    }


# ---------- markdown ----------

def access_md(key: str, file: str) -> str:
    select, where = CATALOG[key]["example"]
    return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
{select}
FROM read_parquet('{PUBLIC_DATA}/{key}/{file}')
{where};
```

The same file at `s3://{BUCKET}/{DATASET_PREFIX}/{key}/{file}` on the anonymous S3 endpoint `{S3_ENDPOINT}`."""


def collection_readme(col: dict, key: str, entry: dict, tiles: dict, manifest: dict) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` | {c['description']} |" for c in col["table:columns"])
    src = manifest["source"]
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, {entry['row_groups']} row groups, sha256 `{entry['sha256'][:12]}...` |
| Features | {entry['features']:,} |
| Map tiles | `{tiles['file']}`, {tiles['bytes'] / 1e6:,.1f} MB, zoom {tiles['min_zoom']} to {tiles['max_zoom']} |
| CRS | OGC:CRS84 (longitude, latitude) |
| In Geobox | Get started > {col['title']} > Add it, on {GEOBOX_URL} |

## Access

{access_md(key, entry['file'])}

## Map tiles

{tiles_text(tiles)} `styles/default.json` draws them.

## Schema

| Column | Type | Description |
|---|---|---|
{schema}

## Provenance

From {src['url']} ({src['page']}), retrieved {src['retrieved']}.
[geobox_samples.py]({REPO_URL}/blob/main/scripts/datasets/geobox_samples.py) reads it with DuckDB, keeps the columns
above, typed, makes the geometry 2D, adds the `bbox` covering, sorts along a Hilbert curve and cuts row groups at 4 MiB
of geometry; the file's footer, bbox and groups are verified before upload. The PMTiles come from the same rows, with
tylertoo.

## License

{col['license'] if col['license'] != 'other' else 'The source terms'}: {CATALOG[key]['license_link']['href']}.
Credit "{src['credit']}".
"""


def collection_agents(col: dict, key: str, entry: dict) -> str:
    return f"""\
# {col['title']}: agent guide

One row per feature, OGC:CRS84, geometry column `geometry`, a `bbox` covering.

## Access

{access_md(key, entry['file'])}

## Query tips

- Filter on `bbox` before a spatial predicate: the file is Hilbert-sorted, so a small area reads few row groups.
- For a map, use the `rel: pmtiles` link (source-layer `{SAMPLES[key]['file']}`) and the `default` style asset.
- Credit: {CATALOG[key]['producer']['name']}. Licence: {CATALOG[key]['license_link']['href']}.
"""


def root_readme(root: dict, collections: list[dict]) -> str:
    listing = "\n".join(f"- [{c['title']}](./{c['id']}/README.md): {c['table:row_count']:,} features, "
                        f"credit {c['providers'][0]['name']}" for c in collections)
    return f"""\
# {root['title']}

{root['description']}

{listing}

Files: `{PUBLIC_DATA}/<sample>/<file>.parquet` and `.pmtiles`.

## License

Each collection carries its producer's licence: see its `license`, its license link and its README.

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {CATALOG_ID}: agent guide

Open datasets offered as samples in Geobox, one collection each, GeoParquet 2.0 and PMTiles.

{listing}

Layout: `{PUBLIC_DATA}/<sample>/<file>.parquet`, anonymous S3 at `s3://{BUCKET}/{DATASET_PREFIX}/` on `{S3_ENDPOINT}`.
OGC:CRS84. Each collection names its producer and licence.
"""


# ---------- build ----------

def build(out_dir: Path | None, dest: Path, *, updated: str | None = None) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    collections, total = [], 0
    for key in SAMPLES:
        if not (THUMBS / f"{key}.png").is_file():
            sys.exit(f"missing {THUMBS / key}.png: run the thumbnails command")
        manifest = read_manifest(out_dir, key)
        entry, tiles = manifest["files"][0], manifest["pmtiles"][0]
        extent, columns = measure(data_source(out_dir, key, entry["file"]))
        col = build_collection(key, entry, tiles, extent, columns, manifest, updated)
        cdir = dest / col["id"]
        write_json(cdir / "styles" / "default.json", style(key, tiles))
        col["assets"]["style"]["file:size"] = (cdir / "styles" / "default.json").stat().st_size
        col["assets"]["style"]["file:checksum"] = multihash(cdir / "styles" / "default.json")
        write_json(cdir / "collection.json", col)
        shutil.copyfile(THUMBS / f"{key}.png", cdir / "thumbnail.png")
        (cdir / "README.md").write_text(collection_readme(col, key, entry, tiles, manifest))
        (cdir / "AGENTS.md").write_text(collection_agents(col, key, entry))
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
    with tempfile.TemporaryDirectory(prefix="geobox-samples-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        total = build(out_dir, tree)
        print(f"  rendered {len(SAMPLES)} collections, {total:,} features")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


# ---------- thumbnails ----------

def thumbnails(out_dir: Path, only: list[str] | None = None) -> None:
    """Each sample over its own extent, in its catalog colour. Dense layers (the buildings) are drawn as their
    centroids: a million outlines at this size are a solid block."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shapely
    from matplotlib.collections import PolyCollection

    from thumbnails import BACKGROUND, frame

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    THUMBS.mkdir(parents=True, exist_ok=True)
    for key, spec in SAMPLES.items():
        if only and key not in only:
            continue
        path = out_dir / key / f"{spec['file']}.parquet"
        colour = CATALOG[key]["colour"]
        x0, y0, x1, y1 = con.execute(f"SELECT min(bbox.xmin), min(bbox.ymin), max(bbox.xmax), max(bbox.ymax) "
                                     f"FROM read_parquet('{path}')").fetchone()
        fx0, fy0, fx1, fy1, aspect = frame(x0, y0, x1, y1)
        fig = plt.figure(figsize=(6, 4), dpi=100)
        ax = fig.add_axes((0, 0, 1, 1))
        ax.set_facecolor(BACKGROUND)
        fig.patch.set_facecolor(BACKGROUND)
        n = con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
        if n > 20_000:
            xy = con.execute(f"SELECT ST_X(ST_Centroid(geometry)), ST_Y(ST_Centroid(geometry)) FROM read_parquet('{path}')").fetchall()
            ax.scatter([p[0] for p in xy], [p[1] for p in xy], s=0.05, c=colour, linewidths=0, alpha=0.6)
        else:
            polys = []
            for (wkb,) in con.execute(f"SELECT ST_AsWKB(geometry) FROM read_parquet('{path}')").fetchall():
                polys += [shapely.get_coordinates(p.exterior) for p in shapely.get_parts(shapely.from_wkb(bytes(wkb)))]
            ax.add_collection(PolyCollection(polys, facecolors=colour, edgecolors="#ffffff", linewidths=0.4, alpha=0.75))
        ax.set_xlim(fx0, fx1)
        ax.set_ylim(fy0, fy1)
        ax.set_aspect(aspect)
        ax.axis("off")
        fig.savefig(THUMBS / f"{key}.png", dpi=100, facecolor=BACKGROUND)
        plt.close(fig)
        print(f"  {key}: {n:,} features -> {(THUMBS / f'{key}.png').relative_to(REPO)}")


# ---------- CLI ----------

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    def out_dir_arg(sp, required=False):
        sp.add_argument("--out-dir", type=Path, default=None, required=required,
                        help="geobox_samples.py's --out-dir (default: read the published files)")

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
    t.add_argument("--sample", action="append", choices=list(SAMPLES))
    args = p.parse_args()
    if args.cmd == "build":
        print(f"wrote {args.dest}/ ({build(args.out_dir, args.dest):,} features)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    elif args.cmd == "publish":
        publish(args.out_dir, args.remote, dry_run=args.dry_run)
    elif args.cmd == "thumbnails":
        thumbnails(args.out_dir, args.sample)


if __name__ == "__main__":
    main()
