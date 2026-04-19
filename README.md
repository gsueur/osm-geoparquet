# OSM → GeoParquet

Pipeline that turns OpenStreetMap PBF extracts into cloud-native GeoParquet 2.0 files, partitioned by US state and theme.

Status: exploratory MVP.

## Requirements

- Python ≥ 3.12
- [`osmium-tool`](https://osmcode.org/osmium-tool/) (`apt install osmium-tool` or `brew install osmium-tool`)
- Python deps: `uv sync`

## Output layout

`out/states/state=<ISO>/<theme>.parquet`, with a `_manifest.json` per state.

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
gpio check all out/states/state=US-RI/buildings.parquet
```

See `scripts/pipeline.py --help` for all flags.

## Data

Source PBFs and state polygons are not tracked — download separately into `data/`:

- US Northeast PBF: https://download.geofabrik.de/north-america/us-northeast.html
- US states GeoJSON: OSM-derived FeatureCollection with `ISO3166-2` and `name` in feature properties.
