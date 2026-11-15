#!/usr/bin/env python3
"""
The Portolan catalog of a state's rasters (rasters.py): the raster sibling
of arcgis_catalog.py.

  <prefix>/catalog/catalog.json                      root
  <prefix>/catalog/<theme>/catalog.json              one sub-catalog per theme
  <prefix>/catalog/<theme>/<item>/collection.json    one collection per raster

Each collection has the raster's COG (`data`: projection and raster
extensions, its grid, nodata and data type), its value table as Parquet
(`classes`, for class rasters) and the thumbnail the build rendered. Sizes,
checksums, grids and sources come from each theme's _manifest.json, local
(--out-dir) or published; `updated` and `checked` from the file stamp
(scripts/filedates.py). Spec: portolan-spec v0.2.0. Validator: rashid,
through scripts/catalog.py.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import filedates

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
    REPO_URL,
    S3_ENDPOINT,
    md_link,
    multihash,
    now_rfc3339,
    write_json,
)

CATALOG_PREFIX = "catalog"
COG_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"
PROJ_EXT = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
RASTER_EXT = "https://stac-extensions.github.io/raster/v2.0.0/schema.json"
DATA_TYPES = {"Byte": "uint8", "UInt16": "uint16", "Int16": "int16", "UInt32": "uint32",
              "Int32": "int32", "Float32": "float32", "Float64": "float64", "Int8": "int8"}


@dataclass(frozen=True)
class RasterCatalog:
    """What a state's raster catalog says."""
    catalog_id: str
    title: str
    state: str                       # keyword, e.g. "California"
    keyword: str                     # the publisher, e.g. "CDFW"
    description: str                 # the root's, before the counts
    theme_about: dict[str, str]      # theme -> one sentence
    providers: list[dict]            # the host last
    license_link: dict
    via_link: dict
    license_md: str
    # What a value means, for the README and agent guide.
    classes_md: str = ""
    spdx: str = "other"
    keywords: tuple[str, ...] = field(default_factory=tuple)


def public_data(p) -> str:
    return f"{PUBLIC_BASE}/{p.prefix}"


def catalog_url(p) -> str:
    return f"{public_data(p)}/{CATALOG_PREFIX}"


def state_catalog_url(p) -> str:
    """The state's vector catalog: <state>/catalog/ beside <state>/rasters/."""
    return f"{PUBLIC_BASE}/{p.prefix.rsplit('/', 1)[0]}/{CATALOG_PREFIX}/catalog.json"


def stamp_url(p) -> str:
    return f"{public_data(p)}/_files/{p.name}.json"


def read_manifest(p, out_dir: Path | None, theme: str) -> dict:
    if out_dir:
        return json.loads((out_dir / theme / "_manifest.json").read_text())
    with urllib.request.urlopen(f"{public_data(p)}/{theme}/_manifest.json", timeout=60) as r:
        return json.load(r)


def thumbnail(p, out_dir: Path | None, theme: str, entry: dict, dest: Path) -> None:
    name = f"{entry['id']}.png"
    if out_dir:
        shutil.copyfile(out_dir / theme / name, dest)
        return
    with urllib.request.urlopen(f"{public_data(p)}/{theme}/{name}", timeout=60) as r:
        dest.write_bytes(r.read())


def bbox_of(entry: dict) -> list[float]:
    ring = entry["wgs84_extent"]["coordinates"][0]
    xs, ys = [c[0] for c in ring], [c[1] for c in ring]
    return [round(min(xs), 6), round(min(ys), 6), round(max(xs), 6), round(max(ys), 6)]


def pixel_size(entry: dict) -> float:
    return abs(entry["geo_transform"][1])


def crs_of(entry: dict) -> str:
    code = entry["proj"].get("proj:code")
    if code:
        return code
    epsg = entry["proj"].get("proj:epsg")
    return f"EPSG:{epsg}" if epsg else "the source's own CRS"


def build_collection(p, c: RasterCatalog, theme: str, entry: dict, updated: str) -> dict:
    path = f"{p.prefix}/{theme}/{entry['file']}"
    src = entry["source"]
    lm = src.get("last_modified") or entry["built"]
    w, h = entry["size"]
    crs = crs_of(entry)
    res = pixel_size(entry)
    # STAC 1.1 bands, with the raster extension v2's fields.
    band = {"data_type": DATA_TYPES.get(entry["data_type"], entry["data_type"].lower()),
            "raster:spatial_resolution": res}
    if entry.get("nodata") is not None:
        band["nodata"] = entry["nodata"]
    classes = entry.get("classes")
    unit = entry.get("unit") or "CRS units"
    description = (
        f"{entry['title']}: {w:,} × {h:,} cells of {res:g} {unit} in {crs}, on the publisher's "
        f"grid with its values, never reprojected or resampled. One Cloud-Optimized GeoTIFF "
        f"(ZSTD, tiles of 512, overviews)"
        + (f" whose cell values are keys into its value table ({classes['rows']:,} rows, "
           f"Parquet), with the publisher's colours as the COG's colour table"
           if classes else "")
        + f", read in place over HTTPS or the anonymous S3 endpoint {S3_ENDPOINT}. "
        f"Credit: {p.attribution}.")
    proj = {k: v for k, v in entry["proj"].items() if k in ("proj:code", "proj:epsg", "proj:wkt2",
                                                           "proj:projjson")}
    if "proj:code" not in proj and "proj:epsg" in proj:
        proj["proj:code"] = f"EPSG:{proj.pop('proj:epsg')}"
    proj.pop("proj:epsg", None)
    assets = {
        "data": {
            "href": f"{PUBLIC_BASE}/{path}",
            "type": COG_TYPE,
            "title": entry["title"],
            "description": (f"{w:,} × {h:,} cells, {entry['data_type']}, overviews to "
                            f"{entry['overviews'][-1][0]:,} × {entry['overviews'][-1][1]:,}"
                            if entry.get("overviews") else f"{w:,} × {h:,} cells"),
            "roles": ["data"],
            **proj,
            "proj:shape": [h, w],
            "proj:transform": [entry["geo_transform"][i] for i in (1, 2, 0, 4, 5, 3)],
            "bands": [band],
            "file:size": entry["bytes"],
            "file:checksum": "1220" + entry["sha256"],
            "alternate": {"s3": {"href": f"s3://{BUCKET}/{path}",
                                 "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
        },
    }
    if classes:
        cpath = f"{p.prefix}/{theme}/{classes['file']}"
        assets["classes"] = {
            "href": f"{PUBLIC_BASE}/{cpath}",
            "type": PARQUET_TYPE,
            "title": f"{entry['title']}, value table",
            "description": (f"{classes['rows']:,} rows, one per cell value (column "
                            f"`{classes['value_column']}`), as the publisher's table gives them."),
            "roles": ["metadata"],
            "file:size": classes["bytes"],
            "file:checksum": "1220" + classes["sha256"],
            "alternate": {"s3": {"href": f"s3://{BUCKET}/{cpath}",
                                 "title": f"S3 endpoint {S3_ENDPOINT}, path style"}},
        }
    assets["thumbnail"] = {"href": "./thumbnail.png", "type": "image/png",
                           "title": f"{entry['title']}, overview", "roles": ["thumbnail"]}
    links = [
        {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
        {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
        md_link("describedby", "./README.md", f"{entry['title']}: README"),
        md_link("agents", "./AGENTS.md", f"{entry['title']}: agent guide"),
        c.license_link,
        c.via_link,
        {"rel": "derived_from", "href": src["url"],
         "type": "application/zip" if src["reader"] == "zip" else "application/json",
         "title": ("The publisher's archive this was read from" if src["reader"] == "zip"
                   else "The image service this was read from")},
    ]
    if src.get("landing") and src["landing"] != src["url"]:
        links.append({"rel": "related", "href": src["landing"], "type": "text/html",
                      "title": "The item's page on the publisher's portal"})
    links.append({"rel": "alternate", "href": f"{public_data(p)}/{theme}/", "type": "text/html",
                  "title": "Browse the files"})
    keywords = [c.state, c.keyword, p.themes[theme], "COG", *c.keywords]
    if src.get("scientific_name"):
        keywords.append(src["scientific_name"])
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA, FILE_EXT, ALTERNATE_EXT, PROJ_EXT, RASTER_EXT],
        "id": entry["id"],
        "title": entry["title"],
        "description": description,
        "keywords": keywords,
        "license": c.spdx,
        "attribution": p.attribution,
        "updated": updated,
        "providers": c.providers,
        "extent": {"spatial": {"bbox": [bbox_of(entry)]},
                   "temporal": {"interval": [[lm, lm]]}},
        "assets": assets,
        "links": links,
    }


def access_md(p, theme: str, entry: dict) -> str:
    url = f"{public_data(p)}/{theme}/{entry['file']}"
    x0, y0, x1, y1 = bbox_of(entry)
    cx, cy = round((x0 + x1) / 2, 2), round((y0 + y1) / 2, 2)
    w_, e_, s_, n_ = (round(v, 2) for v in (cx - 0.05, cx + 0.05, cy - 0.05, cy + 0.05))
    out = f"""\
```sh
gdalinfo /vsicurl/{url}
# a window around a lon/lat point, in the raster's own grid
gdal_translate -projwin_srs EPSG:4326 -projwin {w_} {n_} {e_} {s_} \\
  /vsicurl/{url} window.tif
```

```python
import rasterio
from rasterio.warp import transform_bounds
from rasterio.windows import from_bounds

with rasterio.open("{url}") as src:
    bounds = transform_bounds("EPSG:4326", src.crs, {w_}, {s_}, {e_}, {n_})
    cells = src.read(1, window=from_bounds(*bounds, src.transform))
```"""
    if entry.get("classes"):
        cl = entry["classes"]
        curl = f"{public_data(p)}/{theme}/{cl['file']}"
        out += f"""

The value table, in DuckDB (join it to the cell values on `{cl['value_column']}`):

```sql
INSTALL httpfs; LOAD httpfs;
SELECT * FROM read_parquet('{curl}') LIMIT 10;
```"""
    return out + f"""

The same files at `s3://{BUCKET}/{p.prefix}/{theme}/` on the anonymous S3
endpoint `{S3_ENDPOINT}`."""


def collection_readme(p, c: RasterCatalog, col: dict, theme: str, entry: dict) -> str:
    src = entry["source"]
    w, h = entry["size"]
    cl = entry.get("classes")
    rows = [
        f"| File | `{entry['file']}`, {entry['bytes'] / 1e6:,.1f} MB, sha256 `{entry['sha256'][:12]}...` |",
        (f"| Grid | {w:,} × {h:,} cells of {pixel_size(entry):g} {entry.get('unit') or ''}, "
         f"{crs_of(entry)} |"),
        f"| Data type | {entry['data_type']}, nodata {entry.get('nodata')} |",
    ]
    if cl:
        rows.append(f"| Value table | `{cl['file']}`, {cl['rows']:,} rows |")
    rows.append(f"| Source | <{src['url']}>, last modified {src.get('last_modified') or 'not stated'} |")
    how = ("the publisher's archive, downloaded, its GeoTIFF taken out with its value table"
           if src["reader"] == "zip" else
           "the image service, read with exportImage at its own pixel size and spatial "
           "reference, block by block, each block's grid checked against the service's")
    table = "\n".join(rows)
    return f"""\
# {col['title']}

{col['description']}

| | |
|---|---|
{table}
{f'''
## Values

{c.classes_md}
''' if c.classes_md and cl else ''}
## Access

{access_md(p, theme, entry)}

## Provenance

Read from {how}. [rasters.py]({REPO_URL}/blob/main/scripts/datasets/rasters.py)
writes it as a COG on the same grid with the same values (no reprojection,
no resampling), sets the nodata and the colour table, and checks the result:
COG layout, grid, CRS, nodata, and GDAL's checksum of every cell against the
source's.

## License

{c.license_md}
"""


def collection_agents(p, c: RasterCatalog, col: dict, theme: str, entry: dict) -> str:
    cl = entry.get("classes")
    return f"""\
# {col['title']}: agent guide

One COG, {entry['size'][0]:,} × {entry['size'][1]:,} cells in {crs_of(entry)}, {entry['data_type']}, nodata {entry.get('nodata')}.{f" Cell values are keys into `{cl['file']}` (column `{cl['value_column']}`)." if cl else ""}

## Access

{access_md(p, theme, entry)}

## Tips

- Read windows, never the whole file: the COG serves only the tiles a window needs.
- The grid is the publisher's ({crs_of(entry)}); transform a lon/lat point or box into it first.{f'''
- {c.classes_md}''' if c.classes_md and cl else ""}
- Credit: {p.attribution}.
"""


def build_theme(p, c: RasterCatalog, theme: str, collections: list[dict], updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": f"{p.name}-{theme}",
        "title": f"{p.themes[theme]}",
        "description": (f"{c.theme_about.get(theme, p.themes[theme])}, one collection per raster "
                        f"({len(collections)}). Credit: {p.attribution}."),
        "updated": updated,
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json"},
            *({"rel": "child", "href": f"./{col['id']}/collection.json",
               "type": "application/json", "title": col["title"]} for col in collections),
            md_link("describedby", "./README.md", f"{p.themes[theme]}: README"),
            md_link("agents", "./AGENTS.md", f"{p.themes[theme]}: agent guide"),
            c.license_link,
        ],
    }


def build_root(p, c: RasterCatalog, themes: list[dict], total: int, updated: str) -> dict:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "stac_extensions": [PORTOLAN_SCHEMA],
        "id": c.catalog_id,
        "title": c.title,
        "description": f"{c.description} {total:,} rasters in {len(themes)} themes.",
        "updated": updated,
        "links": [
            {"rel": "self", "href": f"{catalog_url(p)}/catalog.json", "type": "application/json"},
            {"rel": "root", "href": "./catalog.json", "type": "application/json"},
            # A child of the state's catalog (arcgis_catalog.py links it).
            {"rel": "parent", "href": state_catalog_url(p), "type": "application/json",
             "title": "The state's catalog"},
            *({"rel": "child", "href": f"./{t['_theme']}/catalog.json",
               "type": "application/json", "title": t["title"]} for t in themes),
            md_link("describedby", "./README.md", f"{c.title}: README"),
            md_link("agents", "./AGENTS.md", f"{c.title}: agent guide"),
            {"rel": "icon", "href": "./logo.png", "type": "image/png", "title": "Geomermaids"},
            c.license_link,
            c.via_link,
            {"rel": "vcs", "href": REPO_URL, "type": "text/html", "title": "Source code"},
            {"rel": "issues", "href": f"{REPO_URL}/issues", "type": "text/html",
             "title": "Report a problem"},
        ],
    }


def theme_readme(c: RasterCatalog, cat: dict, collections: list[dict]) -> str:
    rows = "\n".join(f"| [{col['title']}](./{col['id']}/README.md) | `{col['id']}` |"
                     for col in collections)
    return f"""\
# {cat['title']}

{cat['description']}

| Collection | Id |
|---|---|
{rows}

## License

{c.license_md}
"""


def theme_agents(cat: dict, collections: list[dict]) -> str:
    listing = "\n".join(f"- `{col['id']}`: {col['title']}. {col['id']}/AGENTS.md"
                        for col in collections)
    return f"# {cat['id']}: agent guide\n\n{cat['description']}\n\n{listing}\n"


def root_readme(p, c: RasterCatalog, root: dict, themes: list[dict], since: str | None) -> str:
    listing = "\n".join(f"- [{t['title']}](./{t['_theme']}/README.md)" for t in themes)
    return f"""\
# {root['title']}

{root['description']}

{listing}

Files: `{public_data(p)}/<theme>/<item>.tif` (COG), `<item>.classes.parquet`
(value table) and `<item>.png` (overview).

{filedates.DATES_MD}{f'''

For these rasters the record of changes starts with this publication: their
`updated` dates are tracked since {since[:10]}.''' if since else ''}

## License

{c.license_md}

## Maintainer

Geomermaids, {CONTACT_EMAIL}. Source and issues: {REPO_URL}.
"""


def root_agents(p, c: RasterCatalog, themes: list[dict]) -> str:
    listing = "\n".join(f"- `{t['_theme']}`: {t['title']}. {t['_theme']}/AGENTS.md"
                        for t in themes)
    return f"""\
# {c.catalog_id}: agent guide

{root_description_short(c)}

{listing}

## Rules

1. Each raster is a Cloud-Optimized GeoTIFF on its publisher's grid: read
   windows over HTTP (GDAL `/vsicurl/`, rasterio), never the whole file.
2. Transform your area into the raster's CRS (in each collection's
   `proj:code`) before you cut a window.{'''
3. Cell values are keys: join them to the value table (`classes` asset,
   Parquet) for their meaning.''' if c.classes_md else ''}

{filedates.DATES_MD}
"""


def root_description_short(c: RasterCatalog) -> str:
    return c.description


def build(p, c: RasterCatalog, out_dir: Path | None, dest: Path, *,
          updated: str | None = None, stamp: str | Path | None = None) -> int:
    updated = updated or now_rfc3339()
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True)
    themes, count = [], 0
    for theme in p.themes:
        try:
            manifest = read_manifest(p, out_dir, theme)
        except FileNotFoundError:
            continue
        except urllib.error.HTTPError as e:
            if e.code not in (403, 404):
                raise
            continue
        tdir = dest / theme
        collections = []
        for entry in sorted(manifest["items"].values(), key=lambda e: e["id"]):
            col = build_collection(p, c, theme, entry, updated)
            cdir = tdir / entry["id"]
            cdir.mkdir(parents=True)
            thumbnail(p, out_dir, theme, entry, cdir / "thumbnail.png")
            for asset in col["assets"].values():
                if asset["href"].startswith("./"):
                    asset["file:size"] = (cdir / asset["href"]).stat().st_size
                    asset["file:checksum"] = multihash(cdir / asset["href"])
            write_json(cdir / "collection.json", col)
            (cdir / "README.md").write_text(collection_readme(p, c, col, theme, entry))
            (cdir / "AGENTS.md").write_text(collection_agents(p, c, col, theme, entry))
            collections.append(col)
        if not collections:
            continue
        cat = build_theme(p, c, theme, collections, updated)
        write_json(tdir / "catalog.json", cat)
        (tdir / "README.md").write_text(theme_readme(c, cat, collections))
        (tdir / "AGENTS.md").write_text(theme_agents(cat, collections))
        themes.append({**cat, "_theme": theme})
        count += len(collections)
    dates = filedates.load(public_data(p), stamp or stamp_url(p))
    root = build_root(p, c, themes, count, updated)
    write_json(dest / "catalog.json", root)
    shutil.copyfile(LOGO, dest / "logo.png")
    (dest / "README.md").write_text(root_readme(p, c, root, themes, dates.since))
    (dest / "AGENTS.md").write_text(root_agents(p, c, themes))
    filedates.apply(dest, dates)
    return count


def check(tree: Path) -> int:
    return shared.check(tree, *shared.LOCAL_DATA)


def publish(p, c: RasterCatalog, out_dir: Path | None, remote: str, *, dry_run: bool) -> None:
    with tempfile.TemporaryDirectory(prefix=f"{p.name}-catalog-") as tmp:
        tree = Path(tmp) / "catalog"
        n = build(p, c, out_dir, tree)
        print(f"  rendered {n} collections")
        if check(tree):
            sys.exit("catalog failed rashid; not uploading")
        shared.upload(tree, remote, CATALOG_PREFIX, dry_run=dry_run)
