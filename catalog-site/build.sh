#!/usr/bin/env bash
# Build portolan-browser at a pinned commit into catalog-site/public/, then
# render the root catalog beside it. public/ is not committed (about 19 MB a
# browser version): the pinned commit makes the build reproducible, so each
# deploy builds it. To take a new browser version, change BROWSER_SHA.
#
#   bash catalog-site/build.sh && (cd catalog-site && npx wrangler deploy)
#
# Needs git, Node 20+ and network. The checkout and node_modules go to a
# temporary directory on the internal disk, removed on exit.
set -euo pipefail

BROWSER_REPO=https://github.com/portolan-sdi/portolan-browser
BROWSER_SHA=15af01335da62a60d209911c9bde994981fd29be  # main, 2026-11-06
PNPM=pnpm@10
ACCENT='#d2553a'  # the site's --accent

here=$(cd "$(dirname "$0")" && pwd)
repo=$(dirname "$here")
work=$(mktemp -d "${TMPDIR:-/tmp}/portolan-browser.XXXXXX")
trap 'rm -rf "$work"' EXIT

git -C "$work" init -q
git -C "$work" fetch -q --depth 1 "$BROWSER_REPO" "$BROWSER_SHA"
git -C "$work" checkout -q FETCH_HEAD

# Fixes not in the pinned commit yet, each sent upstream; drop a patch once
# BROWSER_SHA includes it. git apply refuses a patch that no longer fits.
#   0001: the data preview sent one range request per column chunk, all at
#   once, and Cloudflare answered part of the burst with 503 (shown as a
#   cross-origin error); at most 16 in flight now, table capped at 1,000 rows.
shopt -s nullglob  # no patch left once BROWSER_SHA includes them all
for p in "$here"/patches/*.patch; do
  git -C "$work" apply "$p" || { echo "build.sh: $(basename "$p") does not apply" >&2; exit 1; }
done

# The accent colour is a compile-time SCSS variable, not an option.
sed -i.bak -E "s/^\\\$primary:[[:space:]]+#[0-9a-fA-F]+;/\$primary:   $ACCENT;/" \
  "$work/src/theme/variables.scss"
grep -q "^\$primary:   $ACCENT;" "$work/src/theme/variables.scss" \
  || { echo "build.sh: \$primary not found in variables.scss" >&2; exit 1; }

(cd "$work" && npx -y "$PNPM" install --frozen-lockfile --silent \
  && SB_CONFIG="$here/browser.config.mjs" npx -y "$PNPM" run build)

rm -rf "$here/public"
cp -R "$work/dist" "$here/public"
# Upstream's GitHub Pages domain does not apply here.
rm -f "$here/public/CNAME"
# Source maps are two thirds of the build and no use to a reader.
find "$here/public" -name "*.map" -delete
echo "$BROWSER_SHA" > "$here/public/BROWSER_VERSION"

cd "$repo"
uv run --project scripts python scripts/catalog_root.py build --dest "$here/public"
