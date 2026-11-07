# Overture Maps: agent guide

A guide to querying Overture Maps' GeoParquet in place. It is written by
Geomermaids because Overture ships no AGENTS.md. The data, the STAC catalog and
the licences are Overture's: nothing here is copied or rehosted.

- STAC catalog: https://stac.overturemaps.org/catalog.json
- Schema reference: https://docs.overturemaps.org/schema/
- Attribution and licences: https://docs.overturemaps.org/attribution/

## Layout

```
catalog.json                       root: one child per release; "latest": "<release>"
<release>/catalog.json             one child per theme, schema:version
<release>/<theme>/catalog.json     one collection per type, plus rel "pmtiles"
<release>/<theme>/<type>/collection.json
<release>/<theme>/<type>/<nnnnn>/<nnnnn>.json   one item per file
```

| Theme | Types | Licence (collection `license`) |
|---|---|---|
| addresses | address | `other`: per source, see `sources` |
| base | bathymetry, infrastructure, land, land_cover, land_use, water | ODbL-1.0; land_cover CC-BY-4.0, bathymetry CC0-1.0 |
| buildings | building, building_part | ODbL-1.0 |
| divisions | division, division_area, division_boundary | ODbL-1.0 |
| places | place | `other`: per source, see `sources` |
| transportation | connector, segment | ODbL-1.0 |

The root's `latest` field names the current release. The root lists only recent
releases, so resolve `latest` every time; never hard-code a release.

Each type is a hive-partitioned folder of GeoParquet 1.1 files
(`theme=<theme>/type=<type>/*.parquet`) on the public bucket
`overturemaps-us-west-2` (AWS, us-west-2, no credentials) and mirrored on Azure.
Files are big: `building` is 512 files, 2.5 billion rows, about 250 GB. Each
item is one file, with its bbox, `num_rows`, `file:size`, and assets `aws`
(HTTPS URL) and `azure`.

A PMTiles archive per theme is linked from the theme catalog (`rel: pmtiles`):
`https://tiles.overturemaps.org/<release>/<theme>.pmtiles`.

## Rules

1. **Always filter on `bbox`.** Every file has a `bbox` struct column
   (`xmin, xmax, ymin, ymax`, DOUBLE) and is spatially sorted, so a bbox
   predicate skips most row groups. Filter on it before any `ST_*` call:

   ```sql
   WHERE bbox.xmin <= :east AND bbox.xmax >= :west
     AND bbox.ymin <= :north AND bbox.ymax >= :south
   ```

2. **Pick files before reading them.** A glob over a whole type reads every
   file's footer: 512 for buildings. Choose the files whose item bbox meets your
   area first, then read only those. Measured on a Boston bbox (10,948
   buildings): 1.7 s on the one matching file, 24 s through the glob.

3. **HTTPS globs do not work.** DuckDB lists folders only over `s3://`. Use
   `s3://` with region `us-west-2` for globs, or explicit HTTPS file URLs from
   the items.

4. **Credit Overture and its sources** in any map or result (see Licences).

## Pick files by bbox

The collection's `extent.spatial.bbox` holds the overall box first, then one box
per file in item order (checked on the 2026-10-21.0 release; fall back to
reading the items if the counts differ). One fetch tells you which items to
open:

```python
import json, urllib.request
from urllib.parse import urljoin

def get(url):
    with urllib.request.urlopen(url) as r:
        return json.load(r)

def overture_files(theme, type_, west, south, east, north):
    root = "https://stac.overturemaps.org"
    release = get(f"{root}/catalog.json")["latest"]
    col_url = f"{root}/{release}/{theme}/{type_}/collection.json"
    col = get(col_url)
    items = [urljoin(col_url, l["href"]) for l in col["links"] if l["rel"] == "item"]
    boxes = col["extent"]["spatial"]["bbox"][1:]
    if len(boxes) != len(items):            # no per-file extent: read the items
        boxes = [get(h)["bbox"] for h in items]
    hits = [h for h, b in zip(items, boxes)
            if b[0] <= east and b[2] >= west and b[1] <= north and b[3] >= south]
    return [get(h)["assets"]["aws"]["href"] for h in hits]

files = overture_files("buildings", "building", -71.10, 42.34, -71.04, 42.37)
```

```sql
-- DuckDB, httpfs and spatial loaded; files from overture_files()
SELECT id, names.primary AS name, height, num_floors,
       [s.dataset FOR s IN sources] AS datasets, geometry
FROM read_parquet(['https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/...parquet'])
WHERE bbox.xmin <= -71.04 AND bbox.xmax >= -71.10
  AND bbox.ymin <= 42.37 AND bbox.ymax >= 42.34;
```

## Quick start: Overture's DuckDB views

The root links `latest.ddb` (`rel: alternate`): a 270 KB DuckDB database with one
view per type over the latest release (`address`, `building`, `place`,
`segment`, ...). Convenient for small types and exploration. Each view is a
whole-type glob, so rule 2 applies to the big ones.

```sql
INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
-- A scoped secret: works even when another S3 endpoint is set globally.
CREATE SECRET overture (TYPE s3, PROVIDER config, REGION 'us-west-2',
    ENDPOINT 's3.us-west-2.amazonaws.com', URL_STYLE 'vhost',
    SCOPE 's3://overturemaps-us-west-2');
ATTACH 'https://stac.overturemaps.org/latest.ddb' AS ov (READ_ONLY);

SELECT names.primary, basic_category, sources[1].dataset, sources[1].license
FROM ov.place
WHERE bbox.xmin BETWEEN -71.07 AND -71.05
  AND bbox.ymin BETWEEN 42.35 AND 42.36
LIMIT 10;
```

## Columns that need care

- **`names`** is a struct: `names.primary` (the local name, in the local
  script: 日本 for Japan), `names.common` (MAP of language code to name:
  `names.common['en']`, `names.common['fr']`), and `names.rules` (variants such
  as short or alternate names, some per perspective or per side of a line).
  For an English label use `coalesce(names.common['en'], names.primary)`.
- **`sources`** is a list of structs, one per contributing source:
  `property` (a JSON pointer to the part it supplied, such as
  `/properties/height`, or empty for the whole feature), `dataset` (OpenStreetMap, meta, Foursquare, Esri Community Maps,
  USGS Lidar, ...), `license`, `record_id` (the id in that source),
  `update_time`, `confidence`. It is where the per-feature licence lives:
  places from `meta` are CDLA-Permissive-2.0, from Foursquare Apache-2.0.
- **`id`** is Overture's GERS id, stable across releases for the same feature;
  use it to join releases or to link back to Overture.
- **`division_area`** has a land and a maritime polygon per country
  (`class = 'land'` or `'maritime'`): filter on `class` or every country counts
  twice.
- **`theme` and `type`** come from the hive folders when a query reads a glob.
- Geometry is `GEOMETRY('OGC:CRS84')`, lon/lat.

## Licences

Read the collection's `license` field for the theme, and `sources[].license`
for the feature. ODbL themes need "© OpenStreetMap contributors, Overture Maps
Foundation". Places and addresses mix licences per source (CDLA Permissive 2.0,
Apache 2.0, CC0, CC BY 4.0, national open licences): credit each source a
result draws on. The full list is on Overture's attribution page.

## Maintainer of this guide

Geomermaids (gsueur@geomermaids.com), not Overture. Report errors in this guide
at https://github.com/gsueur/osm-geoparquet/issues; data issues go to Overture.
