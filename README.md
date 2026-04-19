# OSM → GeoParquet

Pipeline that turns OpenStreetMap PBF extracts into cloud-native GeoParquet 2.0 files, partitioned by country, admin region (state / province), and theme.

Status: exploratory MVP.

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

## Data

Source PBFs and admin-region polygons are not tracked — download separately into `data/`:

- Source PBF: pick a Geofabrik regional extract, e.g. North America: https://download.geofabrik.de/north-america-latest.osm.pbf (~14 GB), or the smaller us-northeast: https://download.geofabrik.de/north-america/us-northeast-latest.osm.pbf (~1.6 GB).
- Admin-region GeoJSON: OSM-derived FeatureCollection with `ISO3166-2` (e.g. `US-NY`, `CA-ON`) and `name` in feature properties. One file can mix countries.
