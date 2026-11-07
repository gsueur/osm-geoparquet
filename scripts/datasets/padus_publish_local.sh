#!/usr/bin/env bash
# Build and publish USGS PAD-US from a workstation. The national geodatabase
# is behind a ScienceBase login, so it is downloaded by hand (once per
# release) and unpacked; this runs the upstream check, the build and the
# upload, then the workflow renders the catalog:
#
#   bash scripts/datasets/padus_publish_local.sh \
#     /Volumes/T9/claude-scratch/osm-geoparquet/padus/src/PADUS4_1Geodatabase/PADUS4_1Geodatabase.gdb \
#     /Volumes/T9/claude-scratch/osm-geoparquet/padus
#   gh workflow run dataset-padus.yml -f stage=catalog
#
# The FGDC metadata XML and VersHist_PADUS.txt must sit beside the .gdb, as
# they come out of PADUS4_1Geodatabase.zip. Needs ogr2ogr (GDAL 3.13 was
# used), rclone with a `parquetry` remote (the R2 bucket), uv, curl, jq and
# tylertoo (scripts/datasets/pmtiles.py install). The work dir holds about
# 10 GB. On exFAT (T9) macOS writes ._* files beside every output: the
# filters below keep them out of the bucket.
set -euo pipefail

gdb=${1:?usage: padus_publish_local.sh <PADUS geodatabase .gdb> <work dir>}
work=${2:?usage: padus_publish_local.sh <PADUS geodatabase .gdb> <work dir>}
remote=${REMOTE:-parquetry:parquetry/padus}
public=${PUBLIC:-https://parquetry.geomermaids.com/padus}
repo=$(cd "$(dirname "$0")/../.." && pwd)
cd "$repo"
mkdir -p "$work"

uv run --project scripts python scripts/datasets/padus.py upstream > "$work/upstream.json"
newer=$(jq -r '.newer // [] | join(", ")' "$work/upstream.json")
if [ -n "$newer" ]; then
  echo "ScienceBase lists a newer PAD-US ($newer): download it, then set VERSION in scripts/datasets/padus.py" >&2
  exit 1
fi

out="$work/out"
uv run --project scripts python scripts/datasets/padus.py build \
  --gdb "$gdb" --work-dir "$work/work" --out-dir "$out"

cache="Cache-Control: public, max-age=3600"
short="Cache-Control: public, max-age=300"
# --filter rules apply in order: drop ._* first, then take one kind of file.
up() { rclone copy "$out" "$remote" --filter "- ._*" "$@" --stats-one-line --stats 60s; }
up --filter "+ *.parquet" --filter "- *" --transfers 6 \
  --header-upload "$cache" --header-upload "Content-Type: application/vnd.apache.parquet"
up --filter "+ *.pmtiles" --filter "- *" --transfers 6 \
  --header-upload "$cache" --header-upload "Content-Type: application/vnd.pmtiles"
up --filter "+ *.xml" --filter "- *" \
  --header-upload "$cache" --header-upload "Content-Type: application/xml"
up --filter "+ VersHist_PADUS.txt" --filter "- *" \
  --header-upload "$cache" --header-upload "Content-Type: text/plain; charset=utf-8"
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
         v$v/PADUS4_1_Metadata_XML_FGDC.xml v$v/VersHist_PADUS.txt \
         v$v/combined.parquet v$v/combined/state=UT/combined.parquet \
         v$v/fee.parquet v$v/designation.parquet v$v/easement.parquet v$v/marine.parquet \
         v$v/proclamation.parquet v$v/lookups/GAP_Status.parquet v$v/combined.pmtiles; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -I "$public/$f")
  [ "$code" = "200" ] || { echo "BAD $f -> $code"; fail=1; continue; }
  echo "ok  $f"
done
range=$(curl -s -o /dev/null -w '%{http_code}' -r 0-126 "$public/v$v/combined.pmtiles")
[ "$range" = "206" ] || { echo "BAD pmtiles range request -> $range"; fail=1; }
exit $fail
