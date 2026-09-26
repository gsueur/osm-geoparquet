// Browseable directory listing + raw object reads over the `parquetry`
// R2 bucket. Sibling to api/ (geomermaids-s3-api) — same bucket,
// different hostname, no signing, no writes.
//
//   Browser (Accept: text/html) hits a "directory" path → HTML listing
//   Anything else (curl / DuckDB httpfs HTTPS / rclone) → raw bytes or
//   404, byte-identical to the R2 public custom domain.

const BUCKET_NAME = "parquetry";

// OSM used to sit at the bucket root and now lives under `osm/`, beside the
// other datasets. Every URL published before the move still has to resolve:
// the site, the README, the Portolan catalog, the GeoParquet distribution
// guide and any query somebody saved all point at the old paths, and there
// is no way to know who copied one. So the old layout is served forever.
//
// Duplicated in api/src/index.js rather than shared. Each Worker builds with
// its own directory as the root, so neither can import from above it.
const DATASET_PREFIX = "osm/";
const LEGACY_DATED = /^\d{4}-\d{2}-\d{2}\//;
// `meta/` is deliberately absent: it stays at the bucket root as a shared
// place for cross-dataset files, so it never moved and must never be
// rewritten. Rewriting it would send anything added there into osm/.
const LEGACY_DIRS = ["latest/", "catalog/"];
const LEGACY_FILES = ["snapshots.json", "ATTRIBUTION.txt"];

// True for a key written under the old layout. A bare `latest` with no
// slash is not one: it would be ambiguous with a future object of that
// name, and every client that globs sends the trailing slash.
function isLegacyKey(key) {
  return (
    LEGACY_FILES.includes(key) ||
    LEGACY_DIRS.some((d) => key.startsWith(d)) ||
    LEGACY_DATED.test(key)
  );
}

function currentKey(key) {
  return isLegacyKey(key) ? DATASET_PREFIX + key : key;
}

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          ...corsHeaders(),
          "Access-Control-Allow-Headers": "*",
          "Access-Control-Max-Age": "86400",
        },
      });
    }

    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("Method Not Allowed", {
        status: 405,
        headers: { Allow: "GET, HEAD, OPTIONS", ...corsHeaders() },
      });
    }

    const url = new URL(request.url);
    const path = decodeURIComponent(url.pathname.slice(1));
    const wantsHtml = (request.headers.get("accept") || "").includes("text/html");
    const isDirRequest = path === "" || path.endsWith("/");

    if (isDirRequest) {
      // A browsable directory moves in the open: redirect, so the address
      // bar, the breadcrumbs and the links in the listing all agree on where
      // the file actually is. Objects are rewritten silently instead (below),
      // because a Range request should not have to survive a redirect.
      if (isLegacyKey(path)) {
        return new Response(null, {
          status: 301,
          headers: { Location: "/" + currentKey(path), ...corsHeaders() },
        });
      }
      if (wantsHtml) return renderListing(path, env);
      return new Response("Not Found", { status: 404, headers: corsHeaders() });
    }
    return handleObject(currentKey(path), request, env);
  },
};

// R2's delimiter-list scans up to `limit` (max 1000) underlying objects
// per call, then groups by delimiter. With ~1635 objects under each
// dated snapshot at the bucket root, a single 1000-limit call only
// surfaces the first dated dir and truncates — silently dropping the
// other 7 from the rendered listing. Paginate via cursor so we cover
// every prefix at this level.
//
// Cap pages as a safety net: 50 × 1000 = 50K objects scanned per render,
// well above the bucket root's ~13K and any deeper level (countries,
// states, themes are all O(10s)). The 60s page cache amortizes the cost.
const MAX_LIST_PAGES = 50;

async function listAllAtPrefix(env, prefix) {
  const objects = [];
  const prefixSet = new Set();
  let cursor;
  let pages = 0;
  let lastTruncated = false;
  while (true) {
    const r = await env.BUCKET.list({
      prefix,
      delimiter: "/",
      limit: 1000,
      cursor,
    });
    for (const o of r.objects) objects.push(o);
    for (const p of r.delimitedPrefixes || []) prefixSet.add(p);
    pages++;
    lastTruncated = r.truncated;
    if (!r.truncated || pages >= MAX_LIST_PAGES) break;
    cursor = r.cursor;
  }
  return {
    objects,
    delimitedPrefixes: [...prefixSet],
    truncated: lastTruncated && pages >= MAX_LIST_PAGES,
  };
}

async function renderListing(prefix, env) {
  const list = await listAllAtPrefix(env, prefix);

  const folders = list.delimitedPrefixes
    .map((p) => p.slice(prefix.length))
    .sort(folderCompare);
  const files = list.objects
    .filter((o) => o.key !== prefix)
    .map((o) => ({
      name: o.key.slice(prefix.length),
      size: o.size,
      modified: o.uploaded,
    }))
    .sort((a, b) => a.name.localeCompare(b.name));

  const html = renderListingHtml(prefix, folders, files, list.truncated);
  return new Response(html, {
    status: 200,
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "public, max-age=60",
      ...corsHeaders(),
    },
  });
}

const SITE_TITLE = "Great datasets, in GeoParquet";
const SITE_SUBTITLE = "Open geospatial data, repacked cloud-native. Query straight from a URL.";

// One entry per dataset prefix. The listing renders whichever one the path
// falls under, so a page under clc/ describes Corine Land Cover and links
// its licence rather than OpenStreetMap's, which is what the single
// OSM-shaped header and footer used to do at every level of the bucket.
//
// `attribution` is a full path because the datasets keep theirs at
// different depths: OSM at its prefix root, the others inside the version
// directory. A new vintage moves that file, so it is spelled out here.
const DATASETS = {
  "osm/": {
    subtitle: "OpenStreetMap, North America. Hilbert-sorted, Hive-partitioned " +
      "by state and theme. Rebuilt nightly.",
    attribution: "osm/ATTRIBUTION.txt",
    snapshots: "osm/snapshots.json",
  },
  // Not caught by "osm/": the keys are matched with startsWith, and
  // "osm-infrastructure/" does not start with "osm/".
  "osm-infrastructure/": {
    subtitle: "OpenStreetMap infrastructure, worldwide: power, telecoms, oil and " +
      "gas, water. One folder per country and state, latest build only.",
    attribution: "osm-infrastructure/ATTRIBUTION.txt",
  },
  "clc/": {
    subtitle: "Corine Land Cover 2018, Europe.",
    attribution: "clc/2018/ATTRIBUTION.txt",
    snapshots: "clc/snapshots.json",
  },
  "geoboundaries/": {
    subtitle: "geoBoundaries CGAZ, global administrative boundaries, ADM0 to ADM2.",
    attribution: "geoboundaries/6.0.0/ATTRIBUTION.txt",
    snapshots: "geoboundaries/snapshots.json",
  },
  "gaul/": {
    subtitle: "FAO GAUL 2024, global administrative units, L1 and L2 as published " +
      "and a country layer (L0) dissolved here.",
    attribution: "gaul/2024/ATTRIBUTION.txt",
  },
  "meta/": {
    subtitle: "Shared inputs the builds read. Not a dataset of its own.",
  },
};

// The dataset a listed path belongs to, or null at the bucket root.
function datasetFor(prefix) {
  const key = Object.keys(DATASETS).find((d) => prefix.startsWith(d));
  return key ? DATASETS[key] : null;
}
// Public host of the deck.gl-based parquet viewer. Lives on the website
// (Cloudflare Pages), not this Worker — kept as a constant so the listing
// can link "view" next to each .parquet entry.
const VIEWER_BASE = "https://geoparquet.geomermaids.com/viewer.html";

// Sort folders so the most-useful entries surface first:
//   latest/         → top (most-used)
//   YYYY-MM-DD/     → newest first (descending)
//   everything else → alphabetical ascending
//
// At deeper levels (country=, state=, ...) no folders match the date
// pattern so this falls through to plain alphabetical order.
const DATED_RE = /^\d{4}-\d{2}-\d{2}\/$/;
function folderCompare(a, b) {
  if (a === "latest/") return -1;
  if (b === "latest/") return 1;
  const aDated = DATED_RE.test(a);
  const bDated = DATED_RE.test(b);
  if (aDated && bDated) return b.localeCompare(a);
  if (aDated) return -1;
  if (bDated) return 1;
  return a.localeCompare(b);
}

function renderListingHtml(prefix, folders, files, truncated) {
  const dataset = datasetFor(prefix);
  const segments = prefix.split("/").filter(Boolean);
  const crumbs = [`<a href="/">ROOT</a>`];
  for (let i = 0; i < segments.length; i++) {
    const href = "/" + segments.slice(0, i + 1).join("/") + "/";
    crumbs.push(`<a href="${escapeHtml(href)}">${escapeHtml(segments[i])}</a>`);
  }

  const rows = [];
  if (segments.length > 0) {
    const parent =
      segments.length === 1 ? "/" : "/" + segments.slice(0, -1).join("/") + "/";
    rows.push(
      `<tr><td><a href="${escapeHtml(parent)}">..</a></td><td></td><td></td><td></td></tr>`,
    );
  }
  for (const f of folders) {
    const href = "/" + prefix + f;
    rows.push(
      `<tr><td><a href="${escapeHtml(href)}">${escapeHtml(f)}</a></td><td></td><td></td><td></td></tr>`,
    );
  }
  for (const f of files) {
    const href = "/" + prefix + f.name;
    // Absolute https URL — the viewer takes ?url=<absolute>, and we're
    // hostname-agnostic so reconstruct from the request origin via window.
    const absUrl = "https://parquetry.geomermaids.com" + href;
    const viewCell = f.name.endsWith(".parquet")
      ? `<a class="view" href="${escapeHtml(VIEWER_BASE + "?url=" + encodeURIComponent(absUrl))}" title="Preview on a map" target="_blank" rel="noopener">view</a>`
      : "";
    rows.push(
      `<tr>` +
        `<td><a href="${escapeHtml(href)}">${escapeHtml(f.name)}</a></td>` +
        `<td class="num">${formatBytes(f.size)}</td>` +
        `<td class="num">${f.modified.toISOString().slice(0, 19).replace("T", " ")}Z</td>` +
        `<td class="action">${viewCell}</td>` +
        `</tr>`,
    );
  }

  const truncNote = truncated
    ? `<p class="note">Listing truncated at 1000 entries. ` +
      `<a href="https://s3.geomermaids.com">Use the S3 API</a> for full enumeration.</p>`
    : "";

  return (
    `<!doctype html>` +
    `<html lang="en"><head><meta charset="utf-8">` +
    `<meta name="viewport" content="width=device-width,initial-scale=1">` +
    `<title>${SITE_TITLE}${prefix ? " — /" + escapeHtml(prefix) : ""}</title>` +
    `<style>${LISTING_CSS}</style>` +
    `</head><body>` +
    `<header>` +
    `<h1><a href="/">${SITE_TITLE}</a></h1>` +
    `<p class="subtitle">${escapeHtml(dataset?.subtitle ?? SITE_SUBTITLE)}</p>` +
    `<nav class="crumbs">${crumbs.join(" / ")}</nav>` +
    `</header>` +
    `<table>` +
    `<thead><tr><th>Name</th><th class="num">Size</th><th class="num">Modified (UTC)</th><th class="action"></th></tr></thead>` +
    `<tbody>${rows.join("")}</tbody>` +
    `</table>` +
    truncNote +
    `<footer>` +
    `<a href="https://www.geomermaids.com">&copy; 2026 geomermaids.com</a> &middot; ` +
    (dataset?.attribution
      ? `<a href="/${escapeHtml(dataset.attribution)}">attribution</a> &middot; `
      : "") +
    (dataset?.snapshots
      ? `<a href="/${escapeHtml(dataset.snapshots)}">snapshots.json</a> &middot; `
      : "") +
    `<a href="https://s3.geomermaids.com">s3 api</a> &middot; ` +
    `<a href="${escapeHtml(VIEWER_BASE)}">map viewer</a>` +
    // The same folder on the S3 endpoint, so a page deep inside any dataset
    // shows its own s3:// path rather than sending the reader to the
    // endpoint's landing text for a generic example.
    (prefix
      ? `<br>this folder over S3: <code>s3://parquetry/${escapeHtml(prefix)}</code>` +
        ` on <code>s3.geomermaids.com</code>, path style, empty credentials`
      : "") +
    `</footer>` +
    `<!-- Cloudflare Web Analytics --><script defer src='https://static.cloudflareinsights.com/beacon.min.js' data-cf-beacon='{"token": "662d86a95662419abf6b79622cf413dc"}'></script><!-- End Cloudflare Web Analytics -->` +
    `</body></html>`
  );
}

const LISTING_CSS = `
  :root { color-scheme: light dark; }
  body { font: 14px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; max-width: 960px; margin: 2rem auto; padding: 0 1rem; }
  header { margin-bottom: 1.25rem; }
  h1 { font-size: 1.15rem; font-weight: 600; margin: 0 0 .25rem; }
  h1 a { color: inherit; text-decoration: none; }
  h1 a:hover { text-decoration: underline; }
  .subtitle { font-size: .9rem; opacity: .75; margin: 0 0 .5rem; }
  .crumbs { font-size: .85rem; opacity: .7; }
  .crumbs a { color: inherit; text-decoration: none; }
  .crumbs a:hover { text-decoration: underline; }
  table { width: 100%; border-collapse: collapse; }
  th, td { padding: .25rem .5rem; text-align: left; border-bottom: 1px solid color-mix(in srgb, currentColor 15%, transparent); }
  th { font-weight: 600; opacity: .7; }
  td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
  td.action, th.action { text-align: right; white-space: nowrap; width: 1%; }
  a { color: #0a7cff; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .note { opacity: .7; margin-top: 1rem; }
  a.view {
    padding: 0 .5rem;
    font-size: .78rem;
    border: 1px solid color-mix(in srgb, currentColor 25%, transparent);
    border-radius: 3px;
    color: color-mix(in srgb, currentColor 55%, transparent);
    text-decoration: none;
  }
  a.view:hover { color: #0a7cff; border-color: #0a7cff; text-decoration: none; }
  footer { margin-top: 2rem; font-size: .85rem; opacity: .6; }
  footer a { color: inherit; }
`;

function formatBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KiB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MiB`;
  return `${(n / 1024 ** 3).toFixed(2)} GiB`;
}

async function handleObject(key, request, env) {
  if (request.method === "HEAD") {
    const head = await env.BUCKET.head(key);
    if (!head) return new Response("Not Found", { status: 404, headers: corsHeaders() });
    const headers = objectHeaders(head);
    headers.set("Accept-Ranges", "bytes");
    headers.set("Content-Length", head.size.toString());
    return new Response(null, { status: 200, headers });
  }

  const rangeHeader = request.headers.get("range");
  const ifNoneMatch = request.headers.get("if-none-match");
  const getOpts = {};
  const parsedRange = rangeHeader ? parseRange(rangeHeader) : undefined;
  if (parsedRange) getOpts.range = parsedRange;
  if (ifNoneMatch) getOpts.onlyIf = { etagDoesNotMatch: ifNoneMatch };

  const object = await env.BUCKET.get(key, getOpts);
  if (!object) return new Response("Not Found", { status: 404, headers: corsHeaders() });

  // R2 returns a metadata-only object (body === null) when onlyIf fails
  // — the client's cached etag still matches. Turn that into 304.
  if (object.body === null) {
    return new Response(null, { status: 304, headers: objectHeaders(object) });
  }

  const headers = objectHeaders(object);
  headers.set("Accept-Ranges", "bytes");

  if (parsedRange && object.range) {
    // Bounds come from the request we parsed, not from `object.range`: R2
    // hands that back in its own shape, and reading `.offset`/`.length`
    // off it produced `Content-Range: bytes NaN-NaN/<size>`.
    const { offset, length } = computeRangeBounds(parsedRange, object.size);
    const end = offset + length - 1;
    headers.set("Content-Range", `bytes ${offset}-${end}/${object.size}`);
    headers.set("Content-Length", length.toString());
    return new Response(object.body, { status: 206, headers });
  }

  headers.set("Content-Length", object.size.toString());
  return new Response(object.body, { status: 200, headers });
}

function objectHeaders(obj) {
  const headers = new Headers(corsHeaders());
  obj.writeHttpMetadata(headers);
  headers.set("ETag", obj.httpEtag);
  return headers;
}

function parseRange(header) {
  const m = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!m) return undefined;
  const start = m[1] === "" ? undefined : parseInt(m[1], 10);
  const end = m[2] === "" ? undefined : parseInt(m[2], 10);
  if (start !== undefined && end !== undefined) return { offset: start, length: end - start + 1 };
  if (start !== undefined) return { offset: start };
  if (end !== undefined) return { suffix: end };
  return undefined;
}

function computeRangeBounds(range, size) {
  if ("suffix" in range) {
    const length = Math.min(range.suffix, size);
    return { offset: size - length, length };
  }
  const offset = range.offset ?? 0;
  // A request past the end is clamped to what R2 actually returned.
  const length = Math.min(range.length ?? size - offset, size - offset);
  return { offset, length };
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Expose-Headers": "Content-Type, Content-Length, Content-Range, Accept-Ranges, ETag",
  };
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
