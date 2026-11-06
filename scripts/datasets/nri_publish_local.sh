#!/usr/bin/env bash
# Build and publish the FEMA NRI from a workstation. www.fema.gov answers
# GitHub's runners with an error, so dataset-nri.yml cannot fetch it; this
# runs the same upstream check, build and upload as its stages, then the
# workflow renders the catalog. Run it when FEMA corrects or releases NRI.
#
#   bash scripts/datasets/nri_publish_local.sh /Volumes/T9/claude-scratch/osm-geoparquet/nri
#   gh workflow run dataset-nri.yml -f stage=catalog
#
# Needs rclone with a `parquetry` remote (the R2 bucket), uv, curl, jq and
# tylertoo (scripts/datasets/pmtiles.py install). The work dir holds about
# 4 GB. On exFAT (T9) macOS writes ._* files beside every output: the
# filters below keep them out of the bucket.
set -euo pipefail

work=${1:?usage: nri_publish_local.sh <work dir>}
remote=${REMOTE:-parquetry:parquetry/nri}
public=${PUBLIC:-https://parquetry.geomermaids.com/nri}
repo=$(cd "$(dirname "$0")/../.." && pwd)
cd "$repo"
mkdir -p "$work"

uv run --project scripts python scripts/datasets/nri.py upstream > "$work/upstream.json"
pinned=$(jq -r '.path' "$work/upstream.json")
newest=$(jq -r '.page_versions // [] | last // empty' "$work/upstream.json")
if [ -n "$newest" ] && [ "${newest#v}" -gt "${pinned#v}" ]; then
  echo "OpenFEMA lists NRI $newest, nri.py builds $pinned: set VERSION and VDIR in scripts/datasets/nri.py" >&2
  exit 1
fi

uv run --project scripts python scripts/datasets/nri.py build \
  --work-dir "$work/src" --out-dir "$work/out"

out="$work/out"
cache="Cache-Control: public, max-age=3600"
short="Cache-Control: public, max-age=300"
# --filter rules apply in order: drop ._* first, then take one kind of file.
up() { rclone copy "$out" "$remote" --filter "- ._*" "$@" --stats-one-line --stats 60s; }
up --filter "+ *.parquet" --filter "- *" --transfers 6 \
  --header-upload "$cache" --header-upload "Content-Type: application/vnd.apache.parquet"
up --filter "+ *.pmtiles" --filter "- *" --transfers 6 \
  --header-upload "$cache" --header-upload "Content-Type: application/vnd.pmtiles"
up --filter "+ *.csv" --filter "- *" \
  --header-upload "$cache" --header-upload "Content-Type: text/csv; charset=utf-8"
up --filter "+ _manifest.json" --filter "+ ATTRIBUTION.txt" --filter "- *" \
  --ignore-times --header-upload "$short"
up --max-depth 1 --filter "+ index.json" --filter "+ snapshots.json" --filter "+ _source.json" \
  --filter "- *" --ignore-times --header-upload "$short"

if rclone lsf -R "$remote" | grep -q '\._'; then
  echo "._* files reached the bucket" >&2; exit 1
fi
v=$(jq -r '.version' "$work/upstream.json")
fail=0
for f in index.json _source.json v$v/_manifest.json v$v/ATTRIBUTION.txt \
         v$v/NRIDataDictionary.csv v$v/hazards.parquet v$v/tracts.parquet \
         v$v/counties.parquet v$v/states.parquet v$v/tribal_counties.parquet \
         v$v/tracts.pmtiles v$v/counties.pmtiles v$v/states.pmtiles \
         v$v/tribal_counties.pmtiles; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -I "$public/$f")
  [ "$code" = "200" ] || { echo "BAD $f -> $code"; fail=1; continue; }
  echo "ok  $f"
done
range=$(curl -s -o /dev/null -w '%{http_code}' -r 0-126 "$public/v$v/tracts.pmtiles")
[ "$range" = "206" ] || { echo "BAD pmtiles range request -> $range"; fail=1; }
exit $fail
