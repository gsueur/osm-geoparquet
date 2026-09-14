#!/usr/bin/env bash
# Make sure data/admin_regions.geojson exists.
# Preferred: the file is committed. Fallback: download it from
# $ADMIN_REGIONS_URL (repository variable) when the file is too large for git.
set -euo pipefail

f="data/admin_regions.geojson"
if [[ -s "$f" ]]; then
    echo "$f: $(du -h "$f" | cut -f1) (from repo)"
    exit 0
fi
if [[ -z "${ADMIN_REGIONS_URL:-}" ]]; then
    echo "::error::$f is missing and ADMIN_REGIONS_URL is not set"
    exit 1
fi
mkdir -p data
curl -fsSL --retry 5 --retry-delay 10 --retry-all-errors -o "$f" "$ADMIN_REGIONS_URL"
python3 -c "import json,sys; d=json.load(open('$f')); assert d.get('type')=='FeatureCollection', 'not a FeatureCollection'; print('$f:', len(d['features']), 'features (downloaded)')"
