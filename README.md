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
- Python deps: `uv sync`

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

## Data

Source PBFs and admin-region polygons are not tracked — download separately into `data/`:

- Source PBF: pick a Geofabrik regional extract, e.g. North America: https://download.geofabrik.de/north-america-latest.osm.pbf (~14 GB), or the smaller us-northeast: https://download.geofabrik.de/north-america/us-northeast-latest.osm.pbf (~1.6 GB).
- Admin-region GeoJSON: OSM-derived FeatureCollection with `ISO3166-2` (e.g. `US-NY`, `CA-ON`) and `name` in feature properties. One file can mix countries.
