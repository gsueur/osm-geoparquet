# OSM → GeoParquet

Pipeline that turns OpenStreetMap PBF extracts into cloud-native GeoParquet 2.0 files, partitioned by country, admin region (state / province), and theme.

Status: exploratory MVP.

## Query the hosted data

Snapshots are published at `https://parquetry.geomermaids.com/osm/<YYYY-MM-DD>/country=<CC>/state=<ISO>/<theme>.parquet`. No download required — query straight from DuckDB:

```sql
INSTALL httpfs; LOAD httpfs;
INSTALL spatial; LOAD spatial;

SELECT building, COUNT(*)
FROM read_parquet('https://parquetry.geomermaids.com/osm/2026-04-19/country=US/state=US-RI/buildings.parquet')
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
```

Column projection, bbox filtering, and row-group pruning all work via HTTP range requests against Cloudflare's edge. See `https://parquetry.geomermaids.com/osm/snapshots.json` for the list of available snapshots.

Data © OpenStreetMap contributors, available under the [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/). See `https://parquetry.geomermaids.com/osm/ATTRIBUTION.txt` for redistribution terms.

The bucket holds several datasets, each self-contained under its own prefix (`osm/`, `clc/`, `geoboundaries/`, ...), so each can be registered with Portolan on its own. URLs published before that move still resolve: the Workers serve the old bucket-root layout indefinitely.

### Portolan catalog

The data is described by a [Portolan](https://github.com/portolan-sdi/portolan-spec) (STAC) catalog: one partitioned collection per theme, with column docs, extents, row counts, and a `partition:glob` that reads every region at once through the anonymous S3 endpoint `s3.geomermaids.com`. It is metadata only and copies no data.

- `https://parquetry.geomermaids.com/osm/catalog/catalog.json` is the live catalog. It reads `latest/`, so it always describes current data, and it is rebuilt nightly.
- `https://parquetry.geomermaids.com/osm/catalog/<YYYY-MM-DD>/catalog.json` pins one snapshot. These are published only for the snapshots retention keeps (the first of each month, plus Dec 31), never rewritten, and deleted with their snapshot. Use one when a result has to be reproducible.

Each collection lists its regions as STAC assets (`data-<iso>`, `application/vnd.apache.parquet`) as well as through `partition:glob`, so STAC clients can reach the files directly.

`publish.py` finalize regenerates both from the region manifests (`scripts/catalog.py`) after the completeness gate. See `catalog/CONFORMANCE.md` for what is validated where.

### Schema 0.4.0

- The `geo` metadata declares the `bbox` column as a GeoParquet `covering`, so spec-aware readers use it automatically. DuckDB's writer does not emit a covering (it is not part of GeoParquet 2.0 yet), so `pipeline.py` writes the whole `geo` value through `KV_METADATA`, with `GEOPARQUET_VERSION 'NONE'` so that DuckDB does not add a second `geo` block of its own beside it. That setting governs the metadata only: the geometry column keeps the native `GEOMETRY` logical type and its per-column geo statistics, and file size, row groups and bloom filters are unchanged.
- Each `_manifest.json` records every file's size and sha256, which the catalog publishes as `file:size` and (on pinned catalogs) `file:checksum`.

### Schema 0.3.0 (breaking)

Snapshots whose `_manifest.json` says `"schema_version": "0.3.0"` changed two things:

- The per-file `state` column is now `state_name`. It collided with the `state=<ISO>` path key, and DuckDB's Hive auto-detection silently replaced the region name with the ISO code on globbed reads.
- `osm_id` and `osm_type` are populated. Before, `osm_id` was always NULL and `osm_type` always `'Feature'`.

Queries that mix older and newer snapshots in one glob need `union_by_name = true`.

## Other datasets in the bucket

The repository also carries the builder for FAO GAUL 2024
(`scripts/datasets/gaul.py`, workflow `dataset-gaul.yml`): three layers under
`https://parquetry.geomermaids.com/gaul/2024/` (L1 and L2 as FAO publishes
them, and a country layer L0 dissolved here), each in two layouts of the same
rows: one whole-world file (`L1.parquet`) for a single download, and one
file per country (`country=<iso3>/L1.parquet`) for small reads, since DuckDB
writes no row group under 2,048 rows and a whole-world L1 is two groups of
185 MB. Every file has a bbox covering and a sha256 in `_manifest.json`. Its Portolan catalog is at
`https://parquetry.geomermaids.com/gaul/catalog/catalog.json`, rendered by
`scripts/datasets/gaul_catalog.py`. The licence obligations (citation,
disclaimers) are encoded in its `ATTRIBUTION.txt`, quoted from the Terms of
Use shipped inside the FAO archives.

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

`scripts/publish.py` uploads a pipeline output directory to an S3-compatible remote (Cloudflare R2, MinIO, AWS S3) as a dated immutable snapshot, and maintains a `snapshots.json` index + `ATTRIBUTION.txt` at the dataset prefix. Requires `rclone` configured with a remote named `parquetry` (or pass `--remote <name>:<bucket>[/<prefix>]`).

```bash
python3 scripts/publish.py               # uploads out/ to parquetry:parquetry/osm/<today>/
python3 scripts/publish.py --dry-run     # preview
python3 scripts/publish.py --date 2026-04-18   # backfill a specific date
```

Publishing is split in two stages so several machines can build parts of the same snapshot:

- `--stage upload` copies `out/` into `<remote>/<date>/`. Additive: each caller uploads its own regions.
- `--stage finalize` checks the dated prefix is complete (every region in `data/admin_regions.geojson` has a manifest listing all 16 themes with no failures, and every claimed parquet exists), then syncs `latest/`, writes `ATTRIBUTION.txt` and rebuilds `snapshots.json`. Nothing goes live until this passes.

The default (`--stage all`) does both, which is what a single-machine run wants.

## Nightly build on GitHub Actions

`.github/workflows/nightly.yml` runs the whole pipeline on hosted runners, no server needed:

1. `plan` reads `data/admin_regions.geojson` and resolves every region to the most specific Geofabrik extract via Geofabrik's index (`scripts/plan.py`). Today that is one job per US state / Canadian province and one job for Mexico's 32 states, plus one `us-boundaries` job: a per-state Geofabrik extract cuts the administrative relations that touch the state edge (the state itself, border counties and towns, issue #5), so the `boundaries` theme of every US state is built from the US country extract instead, with the boundary relations filtered out of the 12 GB download first. The US state jobs skip that theme.
2. `build` (matrix, up to 20 in parallel) downloads its extract, runs `pipeline.py` with the admin polygon so the clip semantics are unchanged, validates locally, and uploads its regions with `publish.py --stage upload`. The boundaries job uploads its manifests as `_manifest.boundaries.json` fragments beside the state jobs' manifests.
3. `finalize` runs only if every build job succeeded: `publish.py --stage finalize` folds the manifest fragments into each region's `_manifest.json`, checks completeness, syncs `latest/`, then remote validation and `prune.py --execute --purge-incomplete`.
4. `notify` posts a recap to ntfy.sh when `NTFY_TOPIC` is set.

Setup:

- `data/admin_regions.geojson` is too large for git (114 MB), so it is hosted at `https://parquetry.geomermaids.com/meta/admin_regions.geojson` and the repository variable `ADMIN_REGIONS_URL` points there. Update it with `rclone copyto data/admin_regions.geojson parquetry:parquetry/meta/admin_regions.geojson --header-upload "Cache-Control: public, max-age=300"`. `meta/` sits at the bucket root, not under `osm/`: it is a shared place for cross-dataset files rather than one dataset's.
- Repository secrets: `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT` (`https://<account-id>.r2.cloudflarestorage.com`). Optional: `NTFY_TOPIC`.
- rclone is configured from those secrets through `RCLONE_CONFIG_PARQUETRY_*` environment variables, so no config file is stored anywhere.

Manual runs (`workflow_dispatch`) accept a snapshot date, a region subset (`states: US-RI US-VT`, which skips finalize so a partial upload never goes live), and switches to disable upload or prune. Use "Re-run failed jobs" on a failed nightly: successful uploads are kept in the dated prefix, so the re-run completes it and finalize follows. A dated prefix that never completes is deleted by the next successful run's prune step.

## Data

Source PBFs and admin-region polygons are not tracked — download separately into `data/`:

- Source PBF: pick a Geofabrik regional extract, e.g. North America: https://download.geofabrik.de/north-america-latest.osm.pbf (~14 GB), or the smaller us-northeast: https://download.geofabrik.de/north-america/us-northeast-latest.osm.pbf (~1.6 GB).
- Admin-region GeoJSON: OSM-derived FeatureCollection with `ISO3166-2` (e.g. `US-NY`, `CA-ON`) and `name` in feature properties. One file can mix countries.
