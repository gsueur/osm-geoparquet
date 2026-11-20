#!/usr/bin/env bash
# Download a Geofabrik extract, surviving a broken `-latest` alias.
#
#   fetch_pbf.sh URL DEST [USER_AGENT]
#
# URL is the `<region>-latest.osm.pbf` alias from Geofabrik's index. When the
# alias fails (2026-09-30: it redirected to itself for hours while the dated
# files were fine), the newest `<region>-YYMMDD.osm.pbf` in the alias's own
# directory listing is fetched instead and checked against its published md5.
# The directory works for both of Geofabrik's layouts: `us/vermont-latest`
# beside its siblings until 2026-11, `us/vermont/vermont-latest` since.
set -euo pipefail

URL="$1"
DEST="$2"
UA="${3:-osm-geoparquet/1.0 (+https://geoparquet.geomermaids.com/)}"

get() {  # get URL DEST [extra curl options]
  local url="$1" dest="$2"
  shift 2
  curl -fL -sS --max-redirs 5 --retry-delay 30 -A "$UA" "$@" -o "$dest" "$url"
}

# Tries spaced over about 15 minutes (1, 2, 4 and 8 minutes apart): on
# 2026-11-20 Geofabrik refused connections from some runners for minutes at
# a time while others downloaded, and three tries 30 s apart all failed.
for wait in 60 120 240 480 0; do
  if get "$URL" "$DEST"; then
    echo "fetched $URL"
    exit 0
  fi
  [ "$wait" -eq 0 ] && break
  echo "download failed, trying again in ${wait} s" >&2
  sleep "$wait"
done

case "$URL" in
  *-latest.osm.pbf) ;;
  *) echo "::error::download failed and $URL is not a -latest alias, no fallback" >&2; exit 1 ;;
esac

dir="${URL%/*}"                 # https://download.geofabrik.de/north-america/us/delaware
name="${URL##*/}"
name="${name%-latest.osm.pbf}"  # delaware
page="$(mktemp)"
get "$dir/" "$page" --retry 5 --retry-all-errors
dated="$(grep -oE "$name-[0-9]{6}\.osm\.pbf" "$page" | sort -u | tail -n 1 || true)"
rm -f "$page"
if [ -z "$dated" ]; then
  echo "::error::$URL failed and $dir/ lists no dated extract" >&2
  exit 1
fi

echo "::warning::$URL failed, falling back to $dated"
get "$dir/$dated" "$DEST" --retry 5 --retry-all-errors
want="$(get "$dir/$dated.md5" - --retry 5 --retry-all-errors | cut -d' ' -f1)"
have="$(md5sum "$DEST" | cut -d' ' -f1)"
if [ "$want" != "$have" ]; then
  echo "::error::$dated md5 is $have, Geofabrik publishes $want" >&2
  exit 1
fi
echo "fetched $dated (md5 ok)"
