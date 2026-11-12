#!/usr/bin/env bash
# Stamp a build's output and upload only the data that changed.
#
#   scripts/upload_changed.sh OUT REMOTE PUBLIC NAME [PREFIX] [-- stamp options]
#
# OUT     the build's output folder (uploaded to REMOTE as is)
# REMOTE  its rclone destination, e.g. parquetry:parquetry/census/acs/2024
# PUBLIC  the same folder's public URL
# NAME    the stamp's name, e.g. census-acs (one per workflow)
# PREFIX  OUT's folder under the dataset root the catalog reads, e.g. acs/2024
#
# scripts/filedates.py compares each GeoParquet's sha256 with the published
# stamp; a PMTiles follows its GeoParquet. Unchanged files are not uploaded,
# so their ETag and Last-Modified stay as they were. Upload the manifests
# after this, then the stamp itself last:
#
#   rclone copy "$OUT/_files" "$REMOTE/_files" --include "*.json" --ignore-times \
#     --header-upload "Cache-Control: public, max-age=300"
#
# Needs CACHE (the data's Cache-Control header) in the environment.
set -euo pipefail

out=$1 remote=$2 public=$3 name=$4 prefix=${5:-}
shift $(( $# < 5 ? $# : 5 ))
[ "${1:-}" = "--" ] && shift
here=$(cd "$(dirname "$0")" && pwd)

uv run --project "$here" python "$here/filedates.py" stamp --out-dir "$out" --name "$name" \
  --prefix "$prefix" --public "$public" --since "$(date -u +%F)" "$@"

list="$out/_files/$name.upload"
upload() {  # suffix, content type
  local files
  files=$(mktemp)
  grep -E "\\.$1\$" "$list" > "$files" || true
  if [ -s "$files" ]; then
    echo "uploading $(wc -l < "$files") .$1 file(s)"
    rclone copy "$out" "$remote" --files-from "$files" --no-traverse --ignore-times \
      --header-upload "$CACHE" --header-upload "Content-Type: $2" \
      --transfers 8 --stats 30s --stats-one-line
  else
    echo "no .$1 file changed"
  fi
  rm -f "$files"
}
upload parquet application/vnd.apache.parquet
upload pmtiles application/vnd.pmtiles
