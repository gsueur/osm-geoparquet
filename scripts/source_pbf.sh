#!/usr/bin/env bash
# A region's source extract, kept between runs and brought up to date with
# Geofabrik's daily diffs instead of downloaded whole every night.
#
#   source_pbf.sh get  STATE_KEY URL DEST [USER_AGENT]
#   source_pbf.sh save STATE_KEY DEST
#
# STATE_KEY is the extract's rclone path in the private state bucket, for
# example parquetry:parquetry-state/osm/us/vermont.osm.pbf.
#
# get: copies the kept extract to DEST and applies the diffs since its
# replication timestamp with pyosmium-up-to-date (the URL of the diffs is in
# the file's header). With no kept extract, a failed update or a result
# older than MAX_AGE_DAYS, it downloads URL whole with fetch_pbf.sh instead.
#
# save: copies DEST back to STATE_KEY for the next run.
#
# Why: Geofabrik blocks an address that downloads more than 200 GB in three
# days, and GitHub's runners share addresses; on 2026-11-20 a third of the
# nightly's 68 downloads (31.6 GB a night) were refused. Geofabrik asked for
# pyosmium-up-to-date: the daily diffs of a region are a few MB.
set -euo pipefail

PYOSMIUM="osmium==4.3.2"
MAX_AGE_DAYS=3
HERE="$(cd "$(dirname "$0")" && pwd)"

replication() {  # FILE -> "<sequence> <whole days since its timestamp>", or "none"
  uv run --quiet --no-project --with "$PYOSMIUM" python - "$1" <<'EOF'
import datetime as dt, sys
try:
    import osmium
    h = osmium.io.Reader(sys.argv[1], osmium.osm.osm_entity_bits.NOTHING).header()
    seq = h.get("osmosis_replication_sequence_number")
    ts = h.get("osmosis_replication_timestamp")
    assert seq and ts and h.get("osmosis_replication_base_url")
    t = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
    print(seq, (dt.datetime.now(dt.timezone.utc) - t).days)
except Exception:  # unreadable, or not a Geofabrik extract with diffs
    print("none")
EOF
}

update() {  # FILE: apply diffs until up to date (exit 1 = more to apply)
  local before after rc
  while :; do
    before="$(replication "$1")"
    rc=0
    uv run --quiet --no-project --with "$PYOSMIUM" \
      pyosmium-up-to-date -v --size 2048 --tmpdir "$(dirname "$1")" "$1" || rc=$?
    [ "$rc" -eq 0 ] && return 0
    after="$(replication "$1")"
    # Another pass only when this one moved the sequence forward.
    if [ "$rc" -ne 1 ] || [ "$after" = "none" ] || [ "${after%% *}" = "${before%% *}" ]; then
      return 1
    fi
  done
}

cmd="${1:?get or save}"
case "$cmd" in
  get)
    key="${2:?state key}"; url="${3:?url}"; dest="${4:?dest}"
    ua="${5:-osm-geoparquet/1.0 (+https://geoparquet.geomermaids.com/)}"
    if rclone copyto "$key" "$dest" 2>/dev/null; then
      rep="$(replication "$dest")"
      if [ "$rep" = "none" ]; then
        echo "::warning::$key is unreadable or has no diffs to follow; downloading it whole"
      elif update "$dest"; then
        rep="$(replication "$dest")"
        if [ "$rep" != "none" ] && [ "${rep##* }" -le "$MAX_AGE_DAYS" ]; then
          echo "updated from Geofabrik's diffs: sequence ${rep%% *}, ${rep##* } day(s) old"
          exit 0
        fi
        echo "::warning::$key is still out of date after the diffs ($rep); downloading it whole"
      else
        echo "::warning::the diffs did not apply to $key; downloading it whole"
      fi
      rm -f "$dest"
    else
      echo "no kept extract at $key: downloading it whole"
    fi
    "$HERE/fetch_pbf.sh" "$url" "$dest" "$ua"
    ;;
  save)
    key="${2:?state key}"; dest="${3:?dest}"
    rclone copyto "$dest" "$key"
    echo "kept for the next run: $key"
    ;;
  *)
    echo "usage: $0 get STATE_KEY URL DEST [USER_AGENT] | save STATE_KEY DEST" >&2
    exit 2
    ;;
esac
