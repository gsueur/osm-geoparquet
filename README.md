# OSM → GeoParquet

Pipeline that turns OpenStreetMap PBF extracts into cloud-native GeoParquet 2.0 files, partitioned by country, admin region (state / province), and theme.

Status: exploratory MVP.

## Query the hosted data

Snapshots are published at `https://parquetry.geomermaids.com/<YYYY-MM-DD>/country=<CC>/state=<ISO>/<theme>.parquet`. No download required — query straight from DuckDB:

```sql
INSTALL httpfs; LOAD httpfs;
INSTALL spatial; LOAD spatial;

SELECT building, COUNT(*)
FROM read_parquet('https://parquetry.geomermaids.com/2026-04-19/country=US/state=US-RI/buildings.parquet')
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
```

Column projection, bbox filtering, and row-group pruning all work via HTTP range requests against Cloudflare's edge. See `https://parquetry.geomermaids.com/snapshots.json` for the list of available snapshots.

Data © OpenStreetMap contributors, available under the [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/). See the `ATTRIBUTION.txt` file at the bucket root for redistribution terms.

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

`scripts/publish.py` uploads a pipeline output directory to an S3-compatible remote (Cloudflare R2, MinIO, AWS S3) as a dated immutable snapshot, and maintains a bucket-root `snapshots.json` index + `ATTRIBUTION.txt`. Requires `rclone` configured with a remote named `parquetry` (or pass `--remote <name>:<bucket>`).

```bash
python3 scripts/publish.py               # uploads out/ to parquetry:parquetry/<today>/
python3 scripts/publish.py --dry-run     # preview
python3 scripts/publish.py --date 2026-04-18   # backfill a specific date
```

Publishing is split in two stages so several machines can build parts of the same snapshot:

- `--stage upload` copies `out/` into `<remote>/<date>/`. Additive: each caller uploads its own regions.
- `--stage finalize` checks the dated prefix is complete (every region in `data/admin_regions.geojson` has a manifest listing all 16 themes with no failures, and every claimed parquet exists), then syncs `latest/`, writes `ATTRIBUTION.txt` and rebuilds `snapshots.json`. Nothing goes live until this passes.

The default (`--stage all`) does both, which is what a single-machine run wants.

## Nightly build on GitHub Actions

`.github/workflows/nightly.yml` runs the whole pipeline on hosted runners, no server needed:

1. `plan` reads `data/admin_regions.geojson` and resolves every region to the most specific Geofabrik extract via Geofabrik's index (`scripts/plan.py`). Today that is one job per US state / Canadian province and one job for Mexico's 32 states.
2. `build` (matrix, up to 20 in parallel) downloads its extract, runs `pipeline.py` with the admin polygon so the clip semantics are unchanged, validates locally, and uploads its regions with `publish.py --stage upload`.
3. `finalize` runs only if every build job succeeded: `publish.py --stage finalize`, remote validation, then `prune.py --execute --purge-incomplete`.
4. `notify` posts a recap to ntfy.sh when `NTFY_TOPIC` is set.

Setup:

- `data/admin_regions.geojson` is too large for git (114 MB), so it is hosted at `https://parquetry.geomermaids.com/meta/admin_regions.geojson` and the repository variable `ADMIN_REGIONS_URL` points there. Update it with `rclone copyto data/admin_regions.geojson parquetry:parquetry/meta/admin_regions.geojson --header-upload "Cache-Control: public, max-age=300"`.
- Repository secrets: `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ENDPOINT` (`https://<account-id>.r2.cloudflarestorage.com`). Optional: `NTFY_TOPIC`.
- rclone is configured from those secrets through `RCLONE_CONFIG_PARQUETRY_*` environment variables, so no config file is stored anywhere.

Manual runs (`workflow_dispatch`) accept a snapshot date, a region subset (`states: US-RI US-VT`, which skips finalize so a partial upload never goes live), and switches to disable upload or prune. Use "Re-run failed jobs" on a failed nightly: successful uploads are kept in the dated prefix, so the re-run completes it and finalize follows. A dated prefix that never completes is deleted by the next successful run's prune step.

## Data

Source PBFs and admin-region polygons are not tracked — download separately into `data/`:

- Source PBF: pick a Geofabrik regional extract, e.g. North America: https://download.geofabrik.de/north-america-latest.osm.pbf (~14 GB), or the smaller us-northeast: https://download.geofabrik.de/north-america/us-northeast-latest.osm.pbf (~1.6 GB).
- Admin-region GeoJSON: OSM-derived FeatureCollection with `ISO3166-2` (e.g. `US-NY`, `CA-ON`) and `name` in feature properties. One file can mix countries.
