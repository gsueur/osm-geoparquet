#!/usr/bin/env bash
# Build and publish the NWI's Alaska from a workstation. Alaska's geodatabase
# zip (11.8 GB, 3.8 million polygons) does not fit a GitHub runner's disk, so
# dataset-wetlands.yml builds every other unit and this builds Alaska into the
# same release: its state file, its projects added to projects.parquet, its
# part added to the published _manifest.json. Run it after the workflow has
# published the release (it refuses otherwise), then the map and the catalog:
#
#   bash scripts/datasets/nwi_publish_ak.sh /Volumes/T9/claude-scratch/osm-geoparquet/wetlands/ak
#   gh workflow run dataset-wetlands.yml -f stage=tiles
#
# Needs ogr2ogr and ogrinfo (GDAL 3.13 was used), rclone with a `parquetry`
# remote (the R2 bucket), uv, curl and jq. The work dir holds about 40 GB
# (the zip, the staged rows, the output). On exFAT (T9) macOS writes ._*
# files beside every output: the filters below keep them out of the bucket.
#
# For a test: UNIT=HI (a small unit, built the same way), DRY_RUN=1 (prints
# the uploads instead of running them), PUBLIC=<another published prefix>.
set -euo pipefail

work=${1:?usage: nwi_publish_ak.sh <work dir>}
unit=${UNIT:-AK}
remote=${REMOTE:-parquetry:parquetry/wetlands}
public=${PUBLIC:-https://parquetry.geomermaids.com/wetlands}
dry=${DRY_RUN:-}
repo=$(cd "$(dirname "$0")/../.." && pwd)
cd "$repo"
mkdir -p "$work"

uv run --project scripts python scripts/datasets/nwi.py upstream > "$work/upstream.json"
release=$(jq -r '.release' "$work/upstream.json")
live=$(curl -fsS "$public/_source.json" || echo '{}')
published=$(echo "$live" | jq -r '.nwi.release // empty')
if [ "$release" != "$published" ]; then
  echo "FWS lists release $release, $public has ${published:-none}: publish the other units first" >&2
  echo "  gh workflow run dataset-wetlands.yml -f stage=publish" >&2
  exit 1
fi

out="$work/out"
rm -rf "$out"
uv run --project scripts python scripts/datasets/nwi.py build --unit "$unit" \
  --work-dir "$work/work" --out-dir "$out" --no-tiles --memory "${MEMORY:-6GB}" --live "$public"
rel="nwi/$release"
# The published _source.json, with this unit's zip as read now: the weekly
# check then sees Alaska as done, and the other units as the runner left them.
echo "$live" | jq --slurpfile up "$work/upstream.json" \
  ".nwi.units.\"$unit\" = \$up[0].units.\"$unit\"" > "$out/_source.json"

run() { if [ -n "$dry" ]; then echo "  \$ $*"; else "$@"; fi; }
cache="Cache-Control: public, max-age=3600"
short="Cache-Control: public, max-age=300"
# The data first, then the files that name it.
run rclone copyto "$out/$rel/state=$unit/wetlands.parquet" "$remote/$rel/state=$unit/wetlands.parquet" \
  --header-upload "$cache" --header-upload "Content-Type: application/vnd.apache.parquet"
run rclone copyto "$out/$rel/projects.parquet" "$remote/$rel/projects.parquet" \
  --header-upload "$cache" --header-upload "Content-Type: application/vnd.apache.parquet"
for f in _manifest.json ATTRIBUTION.txt; do
  run rclone copyto "$out/$rel/$f" "$remote/$rel/$f" --ignore-times --header-upload "$short"
done
run rclone copyto "$out/_source.json" "$remote/_source.json" --ignore-times --header-upload "$short"

if [ -n "$dry" ]; then
  echo "dry run: nothing uploaded"
  jq -c '{files: [.files[].state], parts: [.parts[].units], total_features}' "$out/$rel/_manifest.json"
  exit 0
fi
# The whole listing first: grep -q under pipefail can exit before rclone
# has listed everything, and the pipe then reports a failure.
listing=$(rclone lsf -R "$remote/$rel")
if grep -q '\._' <<<"$listing"; then
  echo "._* files reached the bucket" >&2; exit 1
fi
fail=0
for f in _source.json $rel/_manifest.json $rel/projects.parquet $rel/state=$unit/wetlands.parquet; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -I "$public/$f")
  [ "$code" = "200" ] || { echo "BAD $f -> $code"; fail=1; continue; }
  echo "ok  $f"
done
curl -fsS "$public/$rel/_manifest.json" | jq -e --arg u "$unit" '[.files[].state] | index($u)' >/dev/null \
  || { echo "BAD the published manifest does not list $unit"; fail=1; }
[ "$fail" = 0 ] && echo "now: gh workflow run dataset-wetlands.yml -f stage=tiles"
exit $fail
