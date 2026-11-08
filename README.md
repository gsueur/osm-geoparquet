# OSM → GeoParquet

Pipeline that turns OpenStreetMap PBF extracts into cloud-native GeoParquet 2.0 files, partitioned by country, admin region (state / province), and theme.

Status: exploratory MVP.

## Query the hosted data

The data is published at `https://parquetry.geomermaids.com/osm/latest/country=<CC>/state=<ISO>/<theme>.parquet`. No download required — query straight from DuckDB:

```sql
INSTALL httpfs; LOAD httpfs;
INSTALL spatial; LOAD spatial;

SELECT building, COUNT(*)
FROM read_parquet('https://parquetry.geomermaids.com/osm/latest/country=US/state=US-RI/buildings.parquet')
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
```

Column projection, bbox filtering, and row-group pruning all work via HTTP range requests against Cloudflare's edge. See `https://parquetry.geomermaids.com/osm/snapshots.json` for what is available. `latest/` follows the nightly build, and the first build of each month is also kept for 12 months under `osm/<YYYY-MM-DD>/`, immutable.

Data © OpenStreetMap contributors, available under the [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/). See `https://parquetry.geomermaids.com/osm/ATTRIBUTION.txt` for redistribution terms.

The bucket holds several datasets, each self-contained under its own prefix (`osm/`, `gmwid/`, `gaul/`, `clc/`, `nfhl/`, `geoconnex/`, `census/`), so each can be registered with Portolan on its own. URLs published before that move still resolve: the Workers serve the old bucket-root layout indefinitely.

### Portolan catalog

The data is described by a [Portolan](https://github.com/portolan-sdi/portolan-spec) (STAC) catalog: one partitioned collection per theme, with column docs, extents, row counts, and a `partition:glob` that reads every region at once through the anonymous S3 endpoint `s3.geomermaids.com`. It is metadata only and copies no data.

`https://parquetry.geomermaids.com/osm/catalog/catalog.json` reads `latest/`, so it always describes current data, and it is rebuilt nightly.

Each collection lists its regions as STAC assets (`data-<iso>`, `application/vnd.apache.parquet`) as well as through `partition:glob`, so STAC clients can reach the files directly.

`publish.py` finalize regenerates it from the region manifests (`scripts/catalog.py`) after the completeness gate. See `catalog/CONFORMANCE.md` for what is validated where.

### catalog.geomermaids.com

One front door for every dataset catalog: a root catalog with a child link to each dataset catalog on parquetry (by absolute URL, nothing copied), and portolan-browser over it. `scripts/catalog_root.py` renders the root (rerun it when a dataset is added); `catalog-site/build.sh` builds the browser at a pinned commit and the root into `catalog-site/public/`, which is not committed. Deploy: `bash catalog-site/build.sh && (cd catalog-site && npx wrangler deploy)`.

### Schema 0.5.0

- A closed way in `power` or `aeroways` is one row. Until 0.4.0 `osmium export` wrote it twice, as a LineString and as the polygon built from it, so every substation, solar panel, hangar and apron was counted twice (a fifth of the rows of both themes in Texas). It is now the polygon, or the line when the tags are linear (`power=line/minor_line/cable`, runways, taxiways).
- `power.voltage` is the highest value of the OSM `voltage` tag, which lists one voltage per circuit (`345000;138000`). A plain integer cast made every such value NULL. The new `power.voltages` column (`INT[]`) keeps all of them in tag order.

Queries that mix older and newer snapshots in one glob need `union_by_name = true`, and counts over older snapshots include the duplicates.

### Schema 0.4.0

- The `geo` metadata declares the `bbox` column as a GeoParquet `covering`, so spec-aware readers use it automatically. DuckDB's writer does not emit a covering (it is not part of GeoParquet 2.0 yet), so `pipeline.py` writes the whole `geo` value through `KV_METADATA`, with `GEOPARQUET_VERSION 'NONE'` so that DuckDB does not add a second `geo` block of its own beside it. That setting governs the metadata only: the geometry column keeps the native `GEOMETRY` logical type and its per-column geo statistics, and file size, row groups and bloom filters are unchanged.
- Each `_manifest.json` records every file's size and sha256, which the catalog publishes as `file:size`.

### Schema 0.3.0 (breaking)

Schema 0.3.0 changed two things:

- The per-file `state` column is now `state_name`. It collided with the `state=<ISO>` path key, and DuckDB's Hive auto-detection silently replaced the region name with the ISO code on globbed reads.
- `osm_id` and `osm_type` are populated. Before, `osm_id` was always NULL and `osm_type` always `'Feature'`.

Queries that mix older and newer snapshots in one glob need `union_by_name = true`.

## Other datasets in the bucket

The repository also carries the builder for FAO GAUL 2024
(`scripts/datasets/gaul.py`, workflow `dataset-gaul.yml`): three layers under
`https://parquetry.geomermaids.com/gaul/2024/` (L1 and L2 as FAO publishes
them, and a country layer L0 dissolved here), each in two layouts of the same
rows: one whole-world file (`GAUL_2024_L1.parquet`) for a single download,
and one file per country (`country=<iso3>/L1.parquet`) for small reads.
Row groups are capped at 4 MiB of geometry (pyarrow and geoarrow, since
DuckDB writes none under 2,048 rows), so a bbox filter on one point reads a
couple of MB even in the largest country file. A `publish` re-run replaces
bytes uploaded as immutable: read the note at the top of `dataset-gaul.yml`
first. Every file has a bbox covering and a sha256 in
`_manifest.json`. Each layer also comes as vector tiles for a web map
(`GAUL_2024_L1.pmtiles`, zoom 0 to 10, L2 from zoom 3), built with
[tylertoo](https://github.com/geoparquet-io/tylertoo) through
`scripts/datasets/pmtiles.py`, which pins the release and fails the build if
a tile over the size cap would drop features; the workflow's `tiles` stage
adds them to the published release without rebuilding it. Its Portolan catalog is at
`https://parquetry.geomermaids.com/gaul/catalog/catalog.json`, rendered by
`scripts/datasets/gaul_catalog.py`. The licence obligations (citation,
disclaimers) are encoded in its `ATTRIBUTION.txt`, quoted from the Terms of
Use shipped inside the FAO archives.

US Census Bureau TIGER/Line (`scripts/datasets/census.py`, workflow
`dataset-census.yml`, checked weekly against the source's Last-Modified)
sits under `https://parquetry.geomermaids.com/census/tiger/<year>/<product>/`:
one GeoParquet file per GeoPackage layer, named by its TIGER code
(`legislative/cd120.parquet`, `sldu`, `sldl`), columns and NAD83 unchanged,
plus a PMTiles archive each with a `color_index` that keeps neighbouring
districts apart. `census.py`'s PRODUCTS table lists the products built:
legislative, substategeo, nationgeo, school and aiarelated (American Indian,
Alaska Native and Native Hawaiian areas, Alaska Native Regional Corporations,
tribal subdivisions, tribal census tracts and block groups). Portolan catalog at
`https://parquetry.geomermaids.com/census/catalog/catalog.json`
(`scripts/datasets/census_catalog.py`); page at census.geomermaids.com
(`census/`, deployed with wrangler like `nfhl/`).

American Community Survey 5-year estimates, 2020-2024 (`scripts/datasets/acs.py`,
workflow `dataset-acs.yml`, checked weekly against the Summary File's
Last-Modified) sit beside them under
`https://parquetry.geomermaids.com/census/acs/2024/`: 33 curated measures
(population, age, households, income, poverty, home value, rent, tenure,
vacancy, race and Hispanic origin, education, commuting), each with its margin
of error, for states, counties, places, tracts, block groups and ZCTAs.
`<geo>.parquet` is the table keyed by TIGER's GEOID; `<geo>_geo.parquet` joins
it to the TIGER/Line 2026 outline, and `<geo>_geo.pmtiles` holds its vector
tiles (from the TIGER layer's first zoom, with every measure that fits, else
a key subset; `stage=tiles` tiles the published files without a rebuild).
Read from the table-based Summary File (no
API key). The build checks every area against TIGER and that populations add up
block group to tract to county to state, but for a gap in the published file
(14 Suffolk County, NY tracts, listed in `acs.py`'s KNOWN_GAPS). Its collections
are the `acs-2024` sub-catalog of the census catalog (`scripts/datasets/acs_catalog.py`).

The 2020 Census blocks with their counts (`scripts/datasets/blocks.py`, workflow
`dataset-blocks.yml`, checked monthly against the zips' Last-Modified) sit at
`https://parquetry.geomermaids.com/census/tiger/2020/block/`: the 8.1 million
TABBLOCK20 blocks of the 50 states, DC and Puerto Rico with POP20 and
HOUSING20, one `state=<XX>/blocks.parquet` per state, NAD83 as shipped, and
`blocks_h3.parquet`/`blocks_h3.pmtiles`, the counts on H3 cells at
resolutions 4, 6 and 8 with a map. Only the 2020 TIGER/Line blocks carry the
counts. The build fails unless each state's POP20 adds up to its 2020 resident
population in the Census Bureau's apportionment table. The Island Areas' block
files have no counts and are left out. Its collections are the
`tiger-2020-block` sub-catalog (`scripts/datasets/blocks_catalog.py`).

FEMA's National Risk Index (`scripts/datasets/nri.py`, workflow
built and published from a workstation with
`scripts/datasets/nri_publish_local.sh`, because www.fema.gov refuses GitHub's
runners; `dataset-nri.yml` renders the catalog) sits under
`https://parquetry.geomermaids.com/nri/v1.20/`: one GeoParquet file per level
(`tracts.parquet`, `counties.parquet`, `states.parquet`,
`tribal_counties.parquet`), a PMTiles archive for each, coloured on the
composite rating (on expected annual loss for states, which have no composite), FEMA's hazard table (`hazards.parquet`, the 18 column
prefixes) and its field dictionary. FEMA retired the NRI application in 2025;
the data comes from OpenFEMA, whose paths carry the version (`v120`), so the
upstream check fails when the page lists a newer one rather than rebuilding
silently. Columns are FEMA's; the geometry is reprojected from Web Mercator
to lon/lat. `TRACTFIPS` and `STCOFIPS` join the TIGER/Line tract and county
`GEOID`; the build checks rows and keys against FEMA's table CSV and counts
the keys that differ from TIGER 2026. Portolan catalog at
`https://parquetry.geomermaids.com/nri/catalog/catalog.json`
(`scripts/datasets/nri_catalog.py`).

USGS PAD-US 4.1, the Protected Areas Database of the United States
(`scripts/datasets/padus.py`), sits under
`https://parquetry.geomermaids.com/padus/v4.1/`: one GeoParquet file per
feature class (`combined.parquet`, `fee`, `designation`, `easement`, `marine`,
`proclamation`), the Combined layer also per state
(`combined/state=<XX>/combined.parquet`) and as `combined.pmtiles` coloured on
the GAP status, PAD-US's code tables (`lookups/`) and USGS's FGDC metadata.
The national geodatabase is behind a ScienceBase login: it is downloaded by
hand once per release and built and published from a workstation with
`scripts/datasets/padus_publish_local.sh`; `dataset-padus.yml` checks the
release item monthly (it fails when ScienceBase lists a newer PAD-US or the
item changes) and renders the catalog. Columns are PAD-US's, with each coded
column decoded beside it (`<column>_Desc`); the geometry is reprojected from
USGS's Albers to NAD83 lon/lat (EPSG:4269, like the Census layers), features
crossing the 180th meridian are split there and invalid polygons repaired.
Areas overlap, in the Combined layer too (a designation on fee land, a
proclamation boundary around both): take the union of the geometries, or the
`Fee` category for ownership, never a sum of rows. Portolan catalog at
`https://parquetry.geomermaids.com/padus/catalog/catalog.json`
(`scripts/datasets/padus_catalog.py`).

The US National Address Database (`scripts/datasets/nad.py`, workflow
`dataset-nad.yml`, checked weekly against data.transportation.gov) sits under
`https://parquetry.geomermaids.com/addresses/nad/r24/`: release 24 (compiled
September 30, 2026), 99,226,272 address points as one GeoParquet file per
state (`state=<XX>/addresses.parquet`, 47 files: 45 states, DC and the US
Virgin Islands), NAD's 60 columns unchanged plus
`full_address`, a normalized one-line address for matching, lon/lat
(OGC:CRS84: NAD's coordinates are WGS 84). No national file: the states read
together through the S3 glob
`s3://parquetry/addresses/nad/r24/state=*/addresses.parquet`. Participation is
voluntary: `_coverage.json` lists rows and counties per state and the states
with no rows (Hawaii, Michigan, Mississippi, Nevada, New Hampshire and four
territories in r24), and `addresses_h3.parquet` / `.pmtiles` map the points
on H3 cells. USDOT ships the text file as a 8.5 GB Deflate64 zip of a 42 GB
CSV; `zipfile-inflate64` reads it (`uv run --with zipfile-inflate64`). The
build gate accounts for every row of the CSV. Portolan catalog at
`https://parquetry.geomermaids.com/addresses/catalog/catalog.json`
(`scripts/datasets/nad_catalog.py`).

FEMA's USA Structures (`scripts/datasets/structures.py`, workflow
`dataset-structures.yml`, checked weekly against FEMA's public bucket
`fema-femadata`) sits under `https://parquetry.geomermaids.com/structures/latest/`:
building footprints over 450 square feet, produced by ORNL, one GeoParquet file
per state (`state=<XX>/structures.parquet`, 56 states and territories), FEMA's
columns unchanged (occupancy class and use, height, area, address where known,
census tract, ORNL's population estimate per building with its 95% bounds) bar
the geodatabase's own Shape_Area and Shape_Length, lon/lat (OGC:CRS84). Beside
each file, `cells.parquet` counts footprints, residential footprints and
population on H3 cells (resolutions 4, 6, 8) and `structures.pmtiles` maps them:
population on cells to zoom 12, footprints by occupancy from zoom 13. FEMA
refreshes states in batches, so each state has its own vintage (in
`latest/_manifest.json`) and one path, rewritten when its zip changes; the
weekly run rebuilds only the states that moved, one at a time. Population
estimates are missing on part of some states' residential buildings (DC: about
27% carry one in the 2026-09-13 delivery). Public domain; credit "Oak Ridge
National Laboratory (ORNL); FEMA Geospatial Response Office". Portolan catalog
at `https://parquetry.geomermaids.com/structures/catalog/catalog.json`
(`scripts/datasets/structures_catalog.py`).

The USFWS National Wetlands Inventory (`scripts/datasets/nwi.py`, workflow
`dataset-wetlands.yml`, checked weekly; FWS publishes in October and May) sits
under `https://parquetry.geomermaids.com/wetlands/nwi/<release>/`, the release
being FWS's "last updated" month (`2026-10`): one GeoParquet file of wetland
and deepwater polygons per state or territory (`state=<XX>/wetlands.parquet`;
`UM` for Palmyra and Kingman), FWS's `ATTRIBUTE` (Cowardin code),
`WETLAND_TYPE`, `ACRES` and `NWI_ID` with the code spelled out from FWS's NWI
Code Definitions table (`codes.parquet`), the mapping projects with their
imagery year (`projects.parquet`) and `wetlands.pmtiles` coloured on
`WETLAND_TYPE`. FWS's 53 state downloads overlap at the borders (47% of Rhode
Island's polygons are also in Massachusetts' download, sometimes cut
differently): each `NWI_ID` is kept once, in the file of the state that holds
its interior point, from that state's own download. The geometry is
reprojected from FWS's three Albers projections to NAD83 lon/lat (EPSG:4269).
The workflow splits the national build into jobs through a scratch prefix
(`wetlands/_work/<release>/`, removed once published): six staging jobs by
groups of units, one dedup job on the units' indexes, six writing jobs by
groups of states, then one job that assembles and publishes.
Alaska (an 11.8 GB zip) does not fit a runner: the workflow builds every other
unit and `scripts/datasets/nwi_publish_ak.sh` builds Alaska into the same
release from a workstation, then `scripts/datasets/nwi_tiles_local.sh` draws
the map from every published state on the same workstation (a runner's disk
is too small for it), then the workflow's `catalog` stage. Public domain; not a jurisdictional delineation.
Portolan catalog at `https://parquetry.geomermaids.com/wetlands/catalog/catalog.json`
(`scripts/datasets/nwi_catalog.py`).

Gazetteers (`scripts/datasets/gazetteer.py`, workflow `dataset-gazetteer.yml`,
checked weekly) sit under `https://parquetry.geomermaids.com/gazetteer/`: the
Census Bureau's 2026 Gazetteer Files (`census/2026/<code>.parquet`, 17 files
named after the TIGER/Line layer codes: `place`, `county`, `zcta520`, `cd120`...,
every column as published, the internal point as a NAD83 point; GEOID joins
TIGER/Line and the ACS) and GeoNames (`geonames/latest/geonames.parquet`, about
13.5 million names worldwide with decoded feature types, country, admin1 and
admin2 names; the same rows under `country=<CC>/geonames.parquet`, `none` for
rows without a country; and `cities1000.parquet`, the populated places of 1,000
people or more plus seats of government, with `cities1000.pmtiles` for map
labels). GeoNames republishes its dump daily; `latest/` is rebuilt weekly and
keeps no dated snapshots. The build checks rows against the sources, GEOID and
geonameid uniqueness, coordinates, and every Census GEOID against TIGER/Line.
Census: public domain; GeoNames: CC BY 4.0. Portolan catalog at
`https://parquetry.geomermaids.com/gazetteer/catalog/catalog.json`
(`scripts/datasets/gazetteer_catalog.py`), one sub-catalog per source;
thumbnails under `catalog/thumbnails/gazetteer/`.

US wildfire perimeters (`scripts/datasets/fire.py`, workflow `dataset-fire.yml`)
sit under `https://parquetry.geomermaids.com/fire/`, in two groups.
`wfigs/latest/` holds NIFC's Wildland Fire Interagency Geospatial Services
layers, read from their public ArcGIS FeatureServers three times a day and
rebuilt when a layer's `lastEditDate` has moved: `current.parquet` (the fires
burning now), `year_to_date.parquet`, `all_since_2016.parquet` (every WFIGS
perimeter) and `incidents_ytd.parquet` (this year's points of origin), with a
PMTiles archive per perimeter layer. Each layer is read in pages by OBJECTID up
to the highest one at the start, so the edits NIFC makes every few minutes do
not shift the pages; the gate compares the rows read with the server's count
for the same range. `mtbs/latest/mtbs_perimeters.parquet` and `.pmtiles` hold
the Monitoring Trends in Burn Severity burned areas (large fires since 1984,
USGS and USDA Forest Service), checked weekly: the zip is regenerated daily
with the same content, so the build hashes the shapefile inside and rebuilds
only when it changed. 42 of WFIGS's 119 perimeter columns are kept (32 of the incident layer's), named as
WFIGS names them, dates as UTC timestamps; MTBS's columns are all kept. Lon/lat
(MTBS reprojected from NAD83), invalid polygons repaired and counted. An empty
current layer (no active fire) is published with its columns and no rows.
US federal works; WFIGS comes with a disclaimer only, MTBS asks for
acknowledgement. Portolan catalog at
`https://parquetry.geomermaids.com/fire/catalog/catalog.json`
(`scripts/datasets/fire_catalog.py`); thumbnails under
`catalog/thumbnails/fire/`.

MassGIS, the Massachusetts Bureau of Geographic Information
(`scripts/datasets/massgis.py`, workflow `dataset-massgis.yml`, checked
weekly), sits under `https://parquetry.geomermaids.com/massgis/<theme>/`: one
GeoParquet file and one PMTiles archive per current statewide vector layer,
read from MassGIS's public download bucket, grouped in 20 themes (parcels,
buildings, boundaries, water, habitat and so on). `massgis_layers.csv` lists
the layers built, one row each; dated copies, regional splits and indexes are
left out. Columns are MassGIS's and the CRS stays Massachusetts State Plane
(EPSG:26986). The statewide tax parcels come as two files joined on
`LOC_ID`: `parcels/l3_taxpar_poly.parquet` (polygons) and
`parcels/l3_assess.parquet` (assessor records, plain Parquet). A weekly run
compares each layer's zip ETags with `massgis/_source.json` and rebuilds only
the layers that moved. The densest layers tile from zoom 12; below it, their
archive holds an overview instead (H3 cells with the dominant class or a
count per km2, or the major roads only, `OVERVIEWS` in `massgis.py`), so a map
of the whole state still shows them. Portolan catalog at
`https://parquetry.geomermaids.com/massgis/catalog/catalog.json`
(`scripts/datasets/massgis_catalog.py`), one sub-catalog per theme.

Natural Earth v5.1.2 (`scripts/datasets/natural_earth.py`, workflow
`dataset-natural-earth.yml`, on demand: the release is static) sits under
`https://parquetry.geomermaids.com/natural-earth/v5.1.2/<scale>/` (10m, 50m,
110m): one GeoParquet file per layer (`10m/admin_0_countries.parquet`,
countries, states and provinces, populated places, urban areas, airports,
ports, time zones, coastline, land, ocean, rivers, lakes, graticules), every
column as Natural Earth publishes it, lon/lat. `natural_earth_layers.csv`
lists the layers, one row each. The build checks each layer's row count and
columns against its shapefile and refuses a zip newer than v5.1.2; the few
invalid polygons are repaired with ST_MakeValid and counted in
`_manifest.json`. Each file has its own PMTiles archive beside it
(`<scale>/<layer>.pmtiles`, tile layer `<layer>`, every column when the tiles
fit the size cap, else the naming columns). Each scale also has one PMTiles world basemap
(`<scale>/basemap.pmtiles`, every layer in one tile layer `ne`, told apart
by `ne_layer`, clipped to the Web Mercator band) and a MapLibre style over
it (`<scale>/basemap-style.json`). Public domain. Portolan catalog at
`https://parquetry.geomermaids.com/natural-earth/catalog/catalog.json`
(`scripts/datasets/natural_earth_catalog.py`), one sub-catalog per scale;
its thumbnails are committed under `catalog/thumbnails/natural-earth/`.

Geobox's samples (`scripts/datasets/geobox_samples.py`, no workflow: the
sources are static, rebuilt by hand) sit under
`https://parquetry.geomermaids.com/geobox-samples/<sample>/`: New York City's
1.08 million building footprints (`nyc/nyc_buildings.parquet`, NYC Open Data)
and the City of Los Angeles's 114 neighborhoods
(`la/la_neighborhoods.parquet`, Los Angeles Times via the City's GeoHub, CC BY
4.0), the useful columns typed, each with a PMTiles archive. Geobox's Get
started links them and credits each from its collection in the Portolan
catalog at `https://parquetry.geomermaids.com/geobox-samples/catalog/catalog.json`
(`scripts/datasets/geobox_samples_catalog.py`). Publish the data with the
census workflow's rclone commands (`--filter`, not `--include` with
`--exclude`), then the catalog with its `publish` command.

Corine Land Cover 2018 (`scripts/datasets/clc.py`, workflow
`dataset-clc.yml`) sits under `https://parquetry.geomermaids.com/clc/2018/`
as one Europe file (`clc_2018.parquet`) and one file per country
(`country=<alpha-2>/clc_2018.parquet`), EPSG:3035. CLC has no country of its
own: each polygon goes whole to the GAUL country it overlaps most, or the
nearest one, so no polygon is cut and the country files add up to the Europe
file. The source is the published Europe file itself (the Copernicus
download needs an EU Login); the first publish keeps its original bytes in
`clc/_source/`. `clc_2018_h3.parquet` aggregates it on H3 cells (dominant
class and the share of each CLC group, resolutions 3 to 7), and
`clc_2018.pmtiles` draws it on a web map: those cells up to zoom 8, where the
polygons do not fit in tiles, then the polygons to zoom 12, as Fields of the
World does with its cells. Every feature carries `Code_18` and its official
legend `color` and `label` from `meta/clc_legend.json`
(`scripts/datasets/clc_tiles.py`, DuckDB's h3 extension and tylertoo, the
workflow's `tiles` stage). Its Portolan catalog is at
`https://parquetry.geomermaids.com/clc/catalog/catalog.json`, rendered by
`scripts/datasets/clc_catalog.py`.

FEMA's National Flood Hazard Layer (`scripts/datasets/nfhl.py`) sits under
`https://parquetry.geomermaids.com/nfhl/latest/` as one file per FEMA
county-wide delivery (`state=<XX>/<DFIRM_ID>.parquet`), EPSG:4269 as FEMA
delivers it, with an index of those files (`counties.parquet`: bbox, date,
rows, sha256). FEMA replaces a delivery when it changes, about three a day
on a day that has any, so the layout follows that unit: `dataset-nfhl.yml`
runs three times a day, reads FEMA's list, runs the pipeline of
[nfhl-geoparquet-workshop](https://github.com/gsueur/nfhl-geoparquet-workshop)
(download, normalization, subdivision) on the deliveries republished since
the index, and uploads only those files, then the index and the manifests.
The script sorts each delivery along a Hilbert curve, writes the bbox
covering, and checks that rows and geometries are the pipeline's.
`changes.json` lists what each day replaced; `skipped.json` the listed
deliveries that hold no flood layer. `latest/` is the only version, no
frozen copies. The whole country was
built once on a workstation (90 GB of FEMA ZIPs, `nfhl.py bootstrap`). Rows
are pieces of at most 100 vertices, not FEMA zones: `ATTRIBUTION.txt` says
how to count and reassemble zones, and that the files are not for official
flood determinations. Its Portolan catalog is at
`https://parquetry.geomermaids.com/nfhl/catalog/catalog.json`, rendered by
`scripts/datasets/nfhl_catalog.py` after each daily update.

Geoconnex, the Internet of Water's US water reference system
(`scripts/datasets/geoconnex.py`, workflow `dataset-geoconnex.yml`), sits
under `https://parquetry.geomermaids.com/geoconnex/latest/` in two folders.
`reference/` holds one file per reference layer with its full attributes:
852,673 mainstem rivers (head to outlet, length, drainage area, downstream
river) from the `ref_rivers` release, gages, dams and the aquifer layers
from reference.geoconnex.us, and HU02 to HU12 watersheds and public water
systems from Geoconnex's export. `providers/` holds one file per source
Geoconnex harvests (Water Quality Portal sites, USGS monitoring locations,
GNIS, state gages, aquatic barriers...), their columns unchanged.
Geoconnex publishes that export as one unsorted 3.9 GB file with row groups
of up to 416 MB; here each file is Hilbert sorted with a bbox covering and
row groups of at most 4 MiB, so a small bbox reads a few MB. CC0. The job
runs weekly and stops when neither the export nor the rivers release moved.

WorldPop's Global2 population estimates for 2025 (release R2025A,
constrained, 1 km), summed on H3 cells (`scripts/datasets/worldpop.py`,
workflow `dataset-worldpop.yml`), sit under
`https://parquetry.geomermaids.com/worldpop/R2025A/2025/`: one GeoParquet file
per country at H3 resolution 7 (about 5 km2) under `country=<ISO3>/`, a world
file at resolution 5, and one PMTiles archive (resolutions 3 to 7 by zoom):
253 country files, 11.5 million cells, 1.3 GB; the world file 51 MB; the
tiles 1.3 GB. 8.2 billion people, 844 million aged 65 and over.
Columns: `population`, `pop_0_14`, `pop_15_64`, `pop_65_plus`, `pop_80_plus`,
`female`, `male`, the cell's `area_km2` and centre. Each 1 km pixel goes to
the cell holding its centre, so totals are WorldPop's, checked band by band
against the 61 source rasters. Countries are GAUL 2024 `iso3_code` (disputed
areas keep GAUL's x codes), so the files join `gaul/`. CC BY 4.0, cite
Bondarenko et al. 2025, doi:10.5258/SOTON/WP00846. WorldPop's server ignores
Range requests and is slow per connection: the build downloads eight rasters
at a time and deletes each once counted. Portolan catalog at
`https://parquetry.geomermaids.com/worldpop/catalog/catalog.json`
(`scripts/datasets/worldpop_catalog.py`).

The list of repositories GeoPQ Workbench shows under File > Repositories
is `meta/repositories.json`, published to
`https://parquetry.geomermaids.com/meta/repositories.json` by `meta.yml` when
it changes on main. The app reads it at start-up, so adding, renaming or
moving a dataset is an edit to that file, not an app release.

## Requirements

- Python ≥ 3.12
- [`osmium-tool`](https://osmcode.org/osmium-tool/) (`apt install osmium-tool` or `brew install osmium-tool`)
- Python deps: `uv sync --project scripts` (project metadata lives under `scripts/` so Cloudflare Pages doesn't auto-detect a Python build at the repo root)

## Output layout

`out/country=<CC>/state=<ISO3166-2>/<theme>.parquet`, with a `_manifest.json` per admin region. `country` is derived from the ISO3166-2 prefix (e.g. `US-NY` → `US`, `CA-ON` → `CA`), so mixed-country input works without config changes.

## Quick start

```bash
# Smoke test: one small state, one theme
python3 scripts/pipeline.py \
    --source-pbf data/us-northeast-latest.osm.pbf \
    --states-geojson data/us_states.geojson \
    --out-dir out/ \
    --states US-RI \
    --themes buildings

# Validate
gpio check all out/country=US/state=US-RI/buildings.parquet
```

See `scripts/pipeline.py --help` for all flags.

## Publishing

`scripts/publish.py` uploads a pipeline output directory to an S3-compatible remote (Cloudflare R2, MinIO, AWS S3) by replacing `latest/`, and maintains a `snapshots.json` (`latest/` first, then the monthly snapshots) + `ATTRIBUTION.txt` at the dataset prefix. Requires `rclone` configured with a remote named `parquetry` (or pass `--remote <name>:<bucket>[/<prefix>]`).

```bash
python3 scripts/publish.py               # uploads out/ to parquetry:parquetry/osm/latest/
python3 scripts/publish.py --dry-run     # preview
```

Publishing is split in two stages so several machines can build parts of the same build:

- `--stage upload` copies `out/` into `<remote>/_staging/<date>/`. Additive: each caller uploads its own regions.
- `--stage finalize` checks the staging folder is complete (every region in `data/admin_regions.geojson` has a manifest listing all 16 themes with no failures, and every claimed parquet exists), then syncs `latest/`, writes `ATTRIBUTION.txt` and writes `snapshots.json`. It refuses a date older than the published one. Nothing goes live until this passes.

The default (`--stage all`) does both, which is what a single-machine run wants.

## Nightly build on GitHub Actions

`.github/workflows/nightly.yml` runs the whole pipeline on hosted runners, no server needed:

1. `plan` reads `data/admin_regions.geojson` and resolves every region to the most specific Geofabrik extract via Geofabrik's index (`scripts/plan.py`). Today that is one job per US state / Canadian province and one job for Mexico's 32 states, plus one `us-boundaries` job: a per-state Geofabrik extract cuts the administrative relations that touch the state edge (the state itself, border counties and towns, issue #5), so the `boundaries` theme of the fifty states and DC is built from the US country extract instead, with the boundary relations filtered out of the 12 GB download first and the clip completing boundary relations (`--complete-relations multipolygon,boundary`). Those state jobs skip the theme; Puerto Rico and the US Virgin Islands are not in Geofabrik's US file and keep building it from their own extracts. Alaska's state relation is the one that still does not assemble.
2. `build` (matrix, up to 20 in parallel) downloads its extract, runs `pipeline.py` with the admin polygon so the clip semantics are unchanged, validates locally, and uploads its regions with `publish.py --stage upload`. The boundaries job uploads its manifests as `_manifest.boundaries.json` fragments beside the state jobs' manifests.
3. `finalize` runs only if every build job succeeded: `publish.py --stage finalize` folds the manifest fragments into each region's `_manifest.json`, checks completeness, syncs `latest/`, then remote validation and `prune.py --execute`, which keeps the first build of each month as an immutable `<YYYY-MM-DD>/` snapshot, deletes snapshots past 12 months and clears `_staging/`.
4. `notify` posts a recap to ntfy.sh when `NTFY_TOPIC` is set.

Setup:

- `data/admin_regions.geojson` is too large for git (114 MB), so it is hosted at `https://parquetry.geomermaids.com/meta/admin_regions.geojson` and the repository variable `ADMIN_REGIONS_URL` points there. Update it with `rclone copyto data/admin_regions.geojson parquetry:parquetry/meta/admin_regions.geojson --header-upload "Cache-Control: public, max-age=300"`. `meta/` sits at the bucket root, not under `osm/`: it is a shared place for cross-dataset files rather than one dataset's.
- Repository secrets: `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT` (`https://<account-id>.r2.cloudflarestorage.com`). Optional: `NTFY_TOPIC`.
- rclone is configured from those secrets through `RCLONE_CONFIG_PARQUETRY_*` environment variables, so no config file is stored anywhere.

Manual runs (`workflow_dispatch`) accept a build date, a region subset (`states: US-RI US-VT`, which skips finalize so a partial upload never goes live), and a switch to disable upload. Use "Re-run failed jobs" on a failed nightly: successful uploads are kept in the staging folder, so the re-run completes it and finalize follows. A staging folder that never completes is deleted by the next successful run.

## Data

Source PBFs and admin-region polygons are not tracked — download separately into `data/`:

- Source PBF: pick a Geofabrik regional extract, e.g. North America: https://download.geofabrik.de/north-america-latest.osm.pbf (~14 GB), or the smaller us-northeast: https://download.geofabrik.de/north-america/us-northeast-latest.osm.pbf (~1.6 GB).
- Admin-region GeoJSON: OSM-derived FeatureCollection with `ISO3166-2` (e.g. `US-NY`, `CA-ON`) and `name` in feature properties. One file can mix countries.
