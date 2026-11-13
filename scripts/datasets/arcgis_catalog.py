#!/usr/bin/env python3
"""
The Portolan catalog of a state's ArcGIS layers (arcgis_layers.py): the
engine maine_catalog.py and nh_catalog.py share.

  <prefix>/catalog/catalog.json                        root
  <prefix>/catalog/<theme>/catalog.json                one sub-catalog per theme
  <prefix>/catalog/<theme>/<layer>/collection.json     one per layer

The builder's Profile (themes, layer CSV, servers) and a CatalogProfile
(titles, terms, descriptions, the example point) drive it, the same way as
massgis_catalog.py, whose styles, extents and footer reading it shares. Each
collection has the layer's GeoParquet (`data`, its CRS declared with the
projection extension), its PMTiles registered as formats.md asks (a
`rel: pmtiles` link with `pmtiles:layers`, web-map-links v1.3.0, and the
file as a `visual` asset), a MapLibre style and the thumbnail the build
rendered. Counts, sizes, checksums, titles and the publisher's own
descriptions come from each theme's _manifest.json, local (--out-dir) or
published. `updated` and `checked` come from the dataset's file stamp
(scripts/filedates.py). Spec: portolan-spec v0.2.0. Validator: rashid,
through scripts/catalog.py.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import catalog as shared  # scripts/catalog.py
import filedates
from catalog import (
    ALTERNATE_EXT,
    BUCKET,
    CONTACT_EMAIL,
    FILE_EXT,
    LOGO,
    PARQUET_TYPE,
    PORTOLAN_SCHEMA,
    PUBLIC_BASE,
    REPO_URL,
    S3_ENDPOINT,
    TABLE_EXT,
    md_link,
    multihash,
    now_rfc3339,
    write_json,
)
from datasets import arcgis_layers
from datasets.gaul import GROUP_BYTES
from datasets.gaul_catalog import STYLE_TYPE, WEB_MAP_LINKS_EXT
from datasets.massgis_catalog import PROJ_EXT, footer, lonlat_extent, short, style
from datasets.pmtiles import PMTILES_TYPE

CATALOG_PREFIX = "catalog"


@dataclass
class CatalogProfile:
    """What a state's catalog says. terms(theme) -> {attribution, license
    (link), via (link), providers, license_md, keyword, sources}."""
    layers: arcgis_layers.Profile
    state: str                     # keyword, e.g. "Maine"
    catalog_id: str
    title: str
    terms: Callable[[str], dict]
    theme_about: Callable[[str], str]
    root_description: Callable[[list[dict], int], str]
    root_license_md: Callable[[list[dict]], str]
    root_agents_md: Callable[[list[dict], str], str]   # themes, their listing
    # The access example: x, y, their CRS, a distance in its units, the
    # comment above the query.
    example: tuple[int, int, str, int, str]

    @property
    def name(self) -> str:
        return self.layers.name

    @property
    def public_data(self) -> str:
        return f"{PUBLIC_BASE}/{self.name}"

    @property
    def catalog_url(self) -> str:
        return f"{self.public_data}/{CATALOG_PREFIX}"

    @property
    def stamp(self) -> str:
        # The file stamp (filedates.py), at <prefix>/_files/<prefix>.json.
        return f"{self.public_data}/_files/{self.name}.json"


def folds_of(entry: dict) -> tuple[dict, dict]:
    """A folded layer's values per fold column, and each column's name in
    the file (source_<name> where the publisher already uses the name)."""
    folds = entry.get("folds") or ({"year": entry["years"]} if entry.get("years") else {})
    return folds, entry.get("fold_columns") or {c: c for c in folds}


def fold_text(entry: dict) -> str:
    """How a folded layer's sources are told apart, for its description."""
    folds, col = folds_of(entry)
    n = len(entry["source"]["urls"])
    if "from_year" in folds:
        return (f", one per pair of classifications in the `{col['from_year']}` and "
                f"`{col['to_year']}` columns, {min(folds['from_year'])} to "
                f"{max(folds['to_year'])}")
    if "year" in folds:
        return (f", one a year: {min(folds['year'])} to {max(folds['year'])}, in the "
                f"`{col['year']}` column")
    if "category" in folds:
        return f", one per category, in the `{col['category']}` column ({n} categories)"
    if "scenario" in folds:
        return f", one per scenario, in the `{col['scenario']}` column ({n} scenarios)"
    return ""


def fold_years(entry: dict) -> tuple[int, int] | None:
    folds, _ = folds_of(entry)
    if "from_year" in folds:
        return min(folds["from_year"]), max(folds["to_year"])
    if "year" in folds:
        return min(folds["year"]), max(folds["year"])
    return None


# ---------- measured inputs ----------

def theme_url(p: CatalogProfile, theme: str) -> str:
    return f"{p.public_data}/{theme}"


def read_manifest(p: CatalogProfile, out_dir: Path | None, theme: str) -> dict:
    local = out_dir / theme / "_manifest.json" if out_dir else None
    if local and local.is_file():
        return json.loads(local.read_text())
    with urllib.request.urlopen(f"{theme_url(p, theme)}/_manifest.json") as r:
        return json.load(r)


def data_source(p: CatalogProfile, out_dir: Path | None, theme: str, file: str) -> str:
    local = out_dir / theme / file if out_dir else None
    return str(local) if local and local.is_file() else f"{theme_url(p, theme)}/{file}"


def thumbnail_file(p: CatalogProfile, out_dir: Path | None, theme: str, entry: dict,
                   dest: Path) -> None:
    local = out_dir / theme / entry["thumbnail"]["file"] if out_dir else None
    if local and local.is_file():
        shutil.copyfile(local, dest)
        return
    with urllib.request.urlopen(f"{theme_url(p, theme)}/{entry['thumbnail']['file']}") as r:
        dest.write_bytes(r.read())


# ---------- STAC ----------

def build_collection(p: CatalogProfile, theme: str, entry: dict, extent: list[float],
                     columns: list[tuple[str, str]], updated: str) -> dict:
    name, title = entry["name"], entry["title"]
    path = f"{p.name}/{theme}/{entry['file']}"
    src = entry["source"]
    lm = src.get("last_modified") or entry["built"]
    tiles = entry.get("pmtiles")
    years = fold_years(entry)
    t = p.terms(theme)
    server = p.layers.servers[entry.get("server") or ""]
    first_url = src["urls"][0]
    projected = [x for x in src.get("sources", []) if x.get("projected_from")]
    description = (
        f"{short(entry.get('description')) or title}\n\n"
        f"{server['publisher'][0].upper()}{server['publisher'][1:]}'s layer, "
        f"{entry['features']:,} rows, read from its {server['kind']} ({first_url}"
        + (f" and {len(src['urls']) - 1} more{fold_text(entry)}" if len(src["urls"]) > 1 else "")
        + f"). Columns and values unchanged; one GeoParquet 2.0 file, 2D geometry in "
        f"{entry['crs']} as published"
        + (f" (the server projected {len(projected)} of the sources from "
           f"{', '.join(sorted({x['projected_from'] for x in projected}))})" if projected else "")
        + f", with a bbox covering, Hilbert-sorted, and a PMTiles "
        f"archive for web maps, read in place over HTTPS or the anonymous S3 endpoint "
        f"{S3_ENDPOINT}. Credit: {t['attribution']}.")
    assets = {
        "data": {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": PARQUET_TYPE,
            "title": title,
            "description": (f"{entry['features']:,} rows in {entry['row_groups']} row groups "
                            f"of at most {GROUP_BYTES >> 20} MiB of geometry."),
            "roles": ["data"],
            "proj:code": entry["crs"],
            "file:size": entry["bytes"],
            "file:checksum": "1220" + entry["sha256"],
            "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                 "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
        },
    }
    links = []
    if tiles:
        tiles_path = f"{p.name}/{theme}/{tiles['file']}"
        props = ("every attribute" if tiles.get("all_properties") else
                 "properties " + ", ".join(tiles["properties"]) if tiles["properties"] else
                 "no attributes")
        assets["pmtiles"] = {
            "href": f"{PUBLIC_BASE}/{tiles_path}",
            "type": PMTILES_TYPE,
            "title": f"{title}, vector tiles",
            "description": f"PMTiles in lon/lat, layer `{tiles['layer']}`, zoom "
                           f"{tiles['min_zoom']} to {tiles['max_zoom']}, {tiles['tiles']:,} "
                           f"tiles, {props}. Built with tylertoo {tiles['tylertoo']}.",
            "roles": ["visual"],
            "file:size": tiles["bytes"],
            "file:checksum": "1220" + tiles["sha256"],
            "alternate": {"s3": {"href": f"s3://{BUCKET}/{tiles_path}",
                                 "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
        }
        assets["style"] = {"href": "./styles/default.json", "type": STYLE_TYPE,
                           "title": "Default style", "roles": ["style", "default"]}
        links.append({"rel": "pmtiles", "href": f"{PUBLIC_BASE}/{tiles_path}",
                      "type": PMTILES_TYPE, "title": f"{title}, vector tiles",
                      "pmtiles:layers": [tiles["layer"]]})
    assets["thumbnail"] = {"href": "./thumbnail.png", "type": "image/png",
                           "title": f"{title}, overview", "roles": ["thumbnail"]}
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, TABLE_EXT, FILE_EXT, ALTERNATE_EXT, PROJ_EXT,
                            *([WEB_MAP_LINKS_EXT] if tiles else [])],
        "id": name,
        "title": title,
        "description": description,
        "keywords": [p.state, t["keyword"], p.layers.themes[theme], "GeoParquet",
                     *(["PMTiles"] if tiles else [])],
        "license": "other",
        "attribution": t["attribution"],
        "updated": updated,
        "providers": t["providers"],
        "extent": {
            "spatial": {"bbox": [extent]},
            "temporal": {"interval": [[f"{years[0]}-01-01T00:00:00Z",
                                       f"{years[1]}-12-31T23:59:59Z"] if years else [lm, lm]]},
        },
        "table:row_count": entry["features"],
        "table:primary_geometry": "geometry",
        "table:columns": [{"name": n, "type": t} for n, t in columns],
        "assets": assets,
        "links": [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            md_link("describedby", "./README.md", f"{title}: README"),
            md_link("agents", "./AGENTS.md", f"{title}: agent guide"),
            t["license"],
            t["via"],
            *({"rel": "derived_from", "href": u, "type": "text/html",
               "title": f"The {server['kind']} layer this was read from ({u.split('/services/')[1]})"}
              for u in src["urls"]),
            {"rel": "alternate", "href": f"{theme_url(p, theme)}/", "type": "text/html",
             "title": "Browse the files"},
            *links,
        ],
    }


def build_theme(p: CatalogProfile, theme: str, collections: list[dict], updated: str) -> dict:
    t = p.terms(theme)
    about = p.theme_about(theme)
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": f"{p.name}-{theme}",
        "title": p.layers.theme_title(theme),
        "description": (f"{about}, one collection per layer ({len(collections)}). Credit: "
                        f"{t['attribution']}."),
        "updated": updated,
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{c['id']}/collection.json", "type": "application/json",
               "title": c["title"]} for c in collections),
            md_link("describedby", "./README.md", f"{p.layers.themes[theme]}: README"),
            md_link("agents", "./AGENTS.md", f"{p.layers.themes[theme]}: agent guide"),
            t["license"],
            {"rel": "related", "href": f"{theme_url(p, theme)}/ATTRIBUTION.txt",
             "type": "text/plain", "title": "Attribution, sources and what was changed"},
        ],
    }


def build_root(p: CatalogProfile, themes: list[dict], total: int, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": p.catalog_id,
        "title": p.title,
        "description": p.root_description(themes, total),
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{p.catalog_url}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{t['_theme']}/catalog.json", "type": "application/json",
               "title": t["title"]} for t in themes),
            md_link("describedby", "./README.md", f"{p.title}: README"),
            md_link("agents", "./AGENTS.md", f"{p.title}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            p.terms("")["license"],
            p.terms("")["via"],
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


# ---------- markdown ----------

def access_md(p: CatalogProfile, theme: str, entry: dict) -> str:
    url = f"{theme_url(p, theme)}/{entry['file']}"
    x, y, crs, d, label = p.example
    if entry["crs"] == crs:
        where = (f"WHERE bbox.xmin <= {x + d} AND bbox.xmax >= {x - d}\n"
                 f"  AND bbox.ymin <= {y + d} AND bbox.ymax >= {y - d}\n"
                 f"  AND ST_DWithin(geometry, ST_Point({x}, {y}), {d})")
        note = f"-- {label}"
    else:
        where, note = "LIMIT 10", f"-- coordinates in {entry['crs']}"
    return f"""\
```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
{note}
SELECT * EXCLUDE (geometry, bbox)
FROM read_parquet('{url}')
{where};
```

The same file at `s3://{BUCKET}/{p.name}/{theme}/{entry['file']}` on the anonymous
S3 endpoint `{S3_ENDPOINT}`."""


def collection_readme(p: CatalogProfile, col: dict, theme: str, entry: dict) -> str:
    schema = "\n".join(f"| `{c['name']}` | `{c['type']}` |" for c in col["table:columns"])
    tiles = entry.get("pmtiles")
    tiles_row = (f"| Map tiles | `{tiles['file']}`, {tiles['bytes'] / 1e6:,.1f} MB, zoom "
                 f"{tiles['min_zoom']} to {tiles['max_zoom']} |\n" if tiles else "")
    src = entry["source"]
    dropped = entry.get("dropped_without_geometry") or 0
    server = p.layers.servers[entry.get("server") or ""]
    script = f"{p.name}.py"
    if p.layers.crs:
        reference, keeps = f"{p.layers.crs} (the server projects)", ""
    else:
        reference, keeps = "its own spatial reference", "keeps its CRS, "
    return f"""\
# {col['title']}

{entry.get('description') or col['title']}

| | |
|---|---|
| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, sha256 `{entry['sha256'][:12]}...` |
| Rows | {entry['features']:,}{f" ({dropped:,} without a geometry left out)" if dropped else ""} |
{tiles_row}| CRS | {entry['crs']} |
| Source | {", ".join(f"<{u}>" for u in src['urls'])}, last edited {src.get('last_modified') or 'not stated'} |

## Access

{access_md(p, theme, entry)}

## Schema

| Column | Type |
|---|---|
{schema}

## Provenance

{server['publisher'][0].upper()}{server['publisher'][1:]}'s {server['kind']}, read page by page on the object id in
{reference}, the rows checked against the service's count.
[{script}]({REPO_URL}/blob/main/scripts/datasets/{script}) keeps every column
and value, makes the geometry 2D, {keeps}adds the `bbox` covering,
sorts along a Hilbert curve and cuts row groups at 4 MiB of geometry; each
file's footer, CRS, bbox and groups are verified before upload. The PMTiles
come from the same rows, in lon/lat, with tylertoo.{f'''

Copyright, from the service: "{short(entry["copyright"], 300)}"''' if entry.get("copyright") else ""}

## License

{p.terms(theme)["license_md"]}
"""


def collection_agents(p: CatalogProfile, col: dict, theme: str, entry: dict) -> str:
    crs = entry["crs"]
    return f"""\
# {col['title']}: agent guide

{entry['features']:,} rows; geometry column `geometry` in {crs}, `bbox` covering in the same units.{fold_note(entry)}

## Access

{access_md(p, theme, entry)}

## Query tips

- Filter on `bbox` before an exact predicate; the file is Hilbert-sorted, so a small area reads few row groups. Coordinates are in the layer's CRS, not lon/lat unless it is EPSG:4326 or EPSG:4269: transform your point first, e.g. `ST_Transform(ST_Point(lon, lat), 'OGC:CRS84', '{crs}', always_xy := true)`.
- For a map, use the `rel: pmtiles` link and the `default` style asset.
- Credit: {p.terms(theme)["attribution"]}.
"""


def fold_note(entry: dict) -> str:
    """The agent guide's line on a folded layer's extra columns."""
    folds, col = folds_of(entry)
    same = (" (named source_ because the publisher's own field already has the name)"
            if any(v != c for c, v in col.items()) else "")
    if "from_year" in folds:
        return (f" Columns `{col['from_year']}` and `{col['to_year']}`: the two classifications "
                f"a row's change is between; filter on them for one period{same}.")
    if "year" in folds:
        return (f" Column `{col['year']}`: the yearly source layer the row comes from; filter on "
                f"it for one year{same}.")
    if "category" in folds:
        return (f" Column `{col['category']}`: the source layer the row comes from; filter on it "
                f"for one category{same}.")
    if "scenario" in folds:
        return (f" Column `{col['scenario']}`: the scenario layer the row comes from; filter on it "
                f"for one scenario{same}.")
    return ""


def theme_readme(p: CatalogProfile, cat: dict, collections: list[dict]) -> str:
    rows = "\n".join(f"| [{c['title']}](./{c['id']}/README.md) | `{c['id']}` | "
                     f"{c['table:row_count']:,} |" for c in collections)
    return f"""\
# {cat['title']}

{cat['description']}

| Collection | Id | Rows |
|---|---|---|
{rows}

## License

{p.terms(cat["id"].removeprefix(f"{p.name}-"))["license_md"]}
"""


def theme_agents(cat: dict, collections: list[dict]) -> str:
    listing = "\n".join(f"- `{c['id']}`: {c['title']}. {c['id']}/AGENTS.md" for c in collections)
    return f"""\
# {cat['id']}: agent guide

{cat['description']}

{listing}
"""


def root_readme(p: CatalogProfile, root: dict, themes: list[dict], since: str | None) -> str:
    listing = "\n".join(f"- [{t['title']}](./{t['_theme']}/README.md)" for t in themes)
    return f"""\
# {root['title']}

{root['description']}

{listing}

Files: `{p.public_data}/<theme>/<layer>.parquet` and `.pmtiles`.

{filedates.DATES_MD}{f'''

For these layers the record of changes starts with this republication: their
`updated` dates are tracked since {since[:10]}.''' if since else ''}

## License

{p.root_license_md(themes)}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(p: CatalogProfile, themes: list[dict]) -> str:
    listing = "\n".join(f"- `{t['_theme']}`: {t['title']}. {t['_theme']}/AGENTS.md"
                        for t in themes)
    return f"""\
# {p.catalog_id}: agent guide

{p.root_agents_md(themes, listing)}

{filedates.DATES_MD}
"""


# ---------- build ----------

def build(p: CatalogProfile, out_dir: Path | None, dest: Path, *, updated: str | None = None,
          stamp: str | Path | None = None) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    order = {r["id"]: i for i, r in
             enumerate(sorted(arcgis_layers.layers(p.layers), key=lambda r: r["id"]))}
    themes, count = [], 0
    for theme in p.layers.themes:
        try:
            manifest = read_manifest(p, out_dir, theme)
        except FileNotFoundError:
            continue
        except urllib.error.HTTPError as e:
            if e.code != 404:  # a 403 or 500 must not publish a catalog without the theme
                raise
            continue
        tdir = dest / theme
        collections = []
        for entry in sorted(manifest["files"], key=lambda e: order.get(e["name"], 1 << 20)):
            source = data_source(p, out_dir, theme, entry["file"])
            col = build_collection(p, theme, entry, lonlat_extent(con, entry),
                                   footer(con, source), updated)
            cdir = tdir / entry["name"]
            cdir.mkdir(parents=True)
            if entry.get("pmtiles"):
                write_json(cdir / "styles" / "default.json",
                           style(theme, entry, color=p.layers.colors[theme], url=theme_url(p, theme),
                                 attribution=p.terms(theme)["attribution"]))
            thumbnail_file(p, out_dir, theme, entry, cdir / "thumbnail.png")
            for asset in col["assets"].values():
                if asset["href"].startswith("./"):
                    asset["file:size"] = (cdir / asset["href"]).stat().st_size
                    asset["file:checksum"] = multihash(cdir / asset["href"])
            write_json(cdir / "collection.json", col)
            (cdir / "README.md").write_text(collection_readme(p, col, theme, entry))
            (cdir / "AGENTS.md").write_text(collection_agents(p, col, theme, entry))
            collections.append(col)
        cat = build_theme(p, theme, collections, updated)
        write_json(tdir / "catalog.json", cat)
        (tdir / "README.md").write_text(theme_readme(p, cat, collections))
        (tdir / "AGENTS.md").write_text(theme_agents(cat, collections))
        themes.append({**cat, "_theme": theme})
        count += len(collections)
    dates = filedates.load(p.public_data, stamp or p.stamp)
    root = build_root(p, themes, count, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(p, root, themes, dates.since))
    (dest / "AGENTS.md").write_text(root_agents(p, themes))
    filedates.apply(dest, dates)
    return count


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(p: CatalogProfile, out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix=f"{p.name}-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        n = build(p, out_dir, tree)
        print(f"  rendered {n} collections")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)


def main(p: CatalogProfile, doc: str) -> None:
    ap = argparse.ArgumentParser(description=doc.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--out-dir", type=Path, default=None,
                   help=f"{p.layers.name}.py's --out-dir (default: read the published files)")
    b.add_argument("--dest", type=Path, required=True)
    b.add_argument("--stamp", default=p.stamp, help="the file stamp (default: the published one)")
    c = sub.add_parser("check")
    c.add_argument("tree", type=Path)
    u = sub.add_parser("publish")
    u.add_argument("--out-dir", type=Path, default=None)
    u.add_argument("--remote", required=True)
    u.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    if args.cmd == "build":
        print(f"wrote {args.dest}/ ({build(p, args.out_dir, args.dest, stamp=args.stamp):,} collections)")
    elif args.cmd == "check":
        sys.exit(check(args.tree))
    else:
        publish(p, args.out_dir, args.remote, dry_run=args.dry_run)


