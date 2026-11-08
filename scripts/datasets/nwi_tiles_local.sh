#!/usr/bin/env bash
# Build and publish the NWI's national map (wetlands.pmtiles) from a
# workstation. Tiling 35 million polygons from z6 to z13 runs a GitHub
# runner's disk full (run 42426219352), so dataset-wetlands.yml publishes the
# state files and this tiles them, after Alaska (nwi_publish_ak.sh), from the
# published release: each state file streamed into a lon/lat copy in the work
# dir, tiled with tylertoo, the archive uploaded, then the manifest naming it.
# The catalog last, since it names the map:
#
#   bash scripts/datasets/nwi_tiles_local.sh /Volumes/T9/claude-scratch/osm-geoparquet/wetlands/tiles
#   gh workflow run dataset-wetlands.yml -f stage=catalog
#
# Needs tylertoo on PATH at the version pmtiles.py pins (pmtiles.py install),
# rclone with a `parquetry` remote (the R2 bucket), uv, curl and jq. The work
# dir holds the lon/lat copies and tylertoo's buffers: well over the 56 GB of
# the release. On exFAT (T9) macOS writes ._* files beside every output: the
# filters below keep them out of the bucket.
#
# DuckDB runs 4 threads (THREADS): at a workstation's 12, copying Alaska's
# state file (polygons up to 221 MB) ran out of 12GB (MEMORY).
#
# DRY_RUN=1 tiles and prints the uploads instead of running them.
set -euo pipefail

work=${1:?usage: nwi_tiles_local.sh <work dir>}
remote=${REMOTE:-parquetry:parquetry/wetlands}
public=${PUBLIC:-https://parquetry.geomermaids.com/wetlands}
dry=${DRY_RUN:-}
repo=$(cd "$(dirname "$0")/../.." && pwd)
cd "$repo"
mkdir -p "$work/tmp"

# Alaska first: a map without it would be published as complete.
rel=$(curl -fsS "$public/index.json" | jq -r '.datasets[0].path')
curl -fsS "$public/$rel/_manifest.json" | jq -e '[.files[].state] | index("AK")' >/dev/null \
  || { echo "$public/$rel has no Alaska yet: run nwi_publish_ak.sh first" >&2; exit 1; }

out="$work/out"
rm -rf "$out"
TMPDIR="$work/tmp" uv run --project scripts python scripts/datasets/nwi.py tiles \
  --work-dir "$work/src" --out-dir "$out" --memory "${MEMORY:-8GB}" --threads "${THREADS:-4}" --live "$public"

run() { if [ -n "$dry" ]; then echo "  \$ $*"; else "$@"; fi; }
# The archive, then the manifest that names it.
run rclone copyto "$out/$rel/wetlands.pmtiles" "$remote/$rel/wetlands.pmtiles" \
  --header-upload "Cache-Control: public, max-age=3600" \
  --header-upload "Content-Type: application/vnd.pmtiles"
run rclone copyto "$out/$rel/_manifest.json" "$remote/$rel/_manifest.json" --ignore-times \
  --header-upload "Cache-Control: public, max-age=300"

if [ -n "$dry" ]; then
  echo "dry run: nothing uploaded"
  jq -c '.pmtiles | {min_zoom, max_zoom, tiles, bytes, states: (.states | length)}' "$out/$rel/_manifest.json"
  exit 0
fi
listing=$(rclone lsf -R "$remote/$rel")
if grep -q '\._' <<<"$listing"; then
  echo "._* files reached the bucket" >&2; exit 1
fi
fail=0
range=$(curl -s -o /dev/null -w '%{http_code}' -r 0-126 "$public/$rel/wetlands.pmtiles")
[ "$range" = "206" ] || { echo "BAD pmtiles range request -> $range"; fail=1; }
type=$(curl -sI "$public/$rel/wetlands.pmtiles" | tr -d '\r' | awk -F': ' 'tolower($1)=="content-type"{print $2}')
[ "$type" = "application/vnd.pmtiles" ] || { echo "BAD pmtiles Content-Type: $type"; fail=1; }
curl -fsS "$public/$rel/_manifest.json" | jq -e '.pmtiles.states | index("AK")' >/dev/null \
  || { echo "BAD the published manifest's map does not include AK"; fail=1; }
[ "$fail" = 0 ] && echo "now: gh workflow run dataset-wetlands.yml -f stage=catalog"
exit $fail
