// S3-compatible read-only facade over the `parquetry` R2 bucket.
//
// Purpose: let DuckDB / httpfs clients glob our public data with
// `s3://parquetry/osm/latest/country=*/state=*/<theme>.parquet`. Plain HTTPS
// can't do that (no LIST), so this Worker answers path-style
// ListObjectsV2 and GETs backed by an R2 binding.
//
// Everything is anonymous + read-only. No signing, no writes.

const BUCKET_NAME = "parquetry";

// OSM used to sit at the bucket root and now lives under `osm/`, beside the
// other datasets. `s3://parquetry/latest/country=*/state=*/x.parquet` has
// been the documented glob for months, so the old layout keeps answering:
// a LIST under a legacy prefix is served from the new keys and reported back
// under the old ones, and the GETs that follow are rewritten the same way.
// Consistent in both directions, so a client never sees the two mixed.
//
// Duplicated in files/src/index.js rather than shared. Each Worker builds
// with its own directory as the root, so neither can import from above it.
const DATASET_PREFIX = "osm/";
const LEGACY_DATED = /^\d{4}-\d{2}-\d{2}\//;
// `meta/` is deliberately absent: it stays at the bucket root as a shared
// place for cross-dataset files, so it never moved and must never be
// rewritten. Rewriting it would send anything added there into osm/.
const LEGACY_DIRS = ["latest/", "catalog/"];
const LEGACY_FILES = ["snapshots.json", "ATTRIBUTION.txt"];

// True for a key written under the old layout. The trailing slash is
// required, so an empty prefix still lists the real root and a glob that
// stops short of one (`s3://parquetry/lat*`) is left alone. Every client
// that globs a theme sends at least `latest/country=`.
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

    if (request.method === "GET" && url.pathname === "/") {
      return new Response(landingText(), {
        status: 200,
        headers: { "Content-Type": "text/plain; charset=utf-8", ...corsHeaders() },
      });
    }

    const prefix = `/${BUCKET_NAME}`;
    if (url.pathname !== prefix && !url.pathname.startsWith(`${prefix}/`)) {
      return new Response("Not Found", { status: 404, headers: corsHeaders() });
    }

    const afterBucket = url.pathname.slice(prefix.length);

    if (afterBucket === "" || afterBucket === "/") {
      if (url.searchParams.get("list-type") === "2") {
        return handleList(url, env);
      }
      return new Response("Expected list-type=2", {
        status: 400,
        headers: corsHeaders(),
      });
    }

    const key = decodeURIComponent(afterBucket.slice(1));
    return handleObject(currentKey(key), request, env);
  },
};

async function handleList(url, env) {
  const prefix = url.searchParams.get("prefix") || "";
  const delimiter = url.searchParams.get("delimiter") || undefined;
  const continuationToken = url.searchParams.get("continuation-token") || undefined;
  const startAfter = url.searchParams.get("start-after") || undefined;
  const maxKeysParam = url.searchParams.get("max-keys");
  const maxKeys = Math.min(maxKeysParam ? parseInt(maxKeysParam, 10) : 1000, 1000);

  // A continuation token is opaque R2 state tied to the prefix that produced
  // it. The follow-up call carries the same legacy prefix and gets rewritten
  // the same way, so the cursor stays valid across pages.
  const legacy = isLegacyKey(prefix);
  const listOpts = { prefix: legacy ? DATASET_PREFIX + prefix : prefix, limit: maxKeys };
  if (delimiter) listOpts.delimiter = delimiter;
  if (continuationToken) listOpts.cursor = continuationToken;
  if (startAfter && !continuationToken) {
    listOpts.startAfter = legacy ? DATASET_PREFIX + startAfter : startAfter;
  }

  const list = await env.BUCKET.list(listOpts);

  const xml = buildListXml(list, {
    // Echoed and reported as the client wrote them: it asked about the old
    // layout and gets an answer entirely in the old layout.
    prefix,
    asRequested: legacy ? (k) => k.slice(DATASET_PREFIX.length) : (k) => k,
    delimiter,
    maxKeys,
    continuationToken,
    startAfter,
  });

  return new Response(xml, {
    status: 200,
    headers: {
      "Content-Type": "application/xml; charset=utf-8",
      "Cache-Control": "public, max-age=60",
      ...corsHeaders(),
    },
  });
}

function buildListXml(list, params) {
  const { prefix, asRequested, delimiter, maxKeys, continuationToken, startAfter } = params;
  const prefixes = list.delimitedPrefixes || [];
  const keyCount = list.objects.length + prefixes.length;

  const contents = list.objects
    .map(
      (obj) =>
        `<Contents>` +
        `<Key>${xmlEscape(asRequested(obj.key))}</Key>` +
        `<LastModified>${obj.uploaded.toISOString()}</LastModified>` +
        `<ETag>${xmlEscape(obj.httpEtag)}</ETag>` +
        `<Size>${obj.size}</Size>` +
        `<StorageClass>STANDARD</StorageClass>` +
        `</Contents>`,
    )
    .join("");

  const commonPrefixes = prefixes
    .map((p) => `<CommonPrefixes><Prefix>${xmlEscape(asRequested(p))}</Prefix></CommonPrefixes>`)
    .join("");

  const parts = [
    `<?xml version="1.0" encoding="UTF-8"?>`,
    `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`,
    `<Name>${BUCKET_NAME}</Name>`,
    `<Prefix>${xmlEscape(prefix)}</Prefix>`,
    delimiter ? `<Delimiter>${xmlEscape(delimiter)}</Delimiter>` : "",
    `<MaxKeys>${maxKeys}</MaxKeys>`,
    `<KeyCount>${keyCount}</KeyCount>`,
    `<IsTruncated>${list.truncated ? "true" : "false"}</IsTruncated>`,
    continuationToken
      ? `<ContinuationToken>${xmlEscape(continuationToken)}</ContinuationToken>`
      : "",
    list.truncated && list.cursor
      ? `<NextContinuationToken>${xmlEscape(list.cursor)}</NextContinuationToken>`
      : "",
    startAfter ? `<StartAfter>${xmlEscape(startAfter)}</StartAfter>` : "",
    contents,
    commonPrefixes,
    `</ListBucketResult>`,
  ];

  return parts.filter(Boolean).join("");
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
    const { offset, length } = computeRangeBounds(object.range, object.size);
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
  const length = range.length ?? size - offset;
  return { offset, length };
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Expose-Headers": "Content-Type, Content-Length, Content-Range, Accept-Ranges, ETag",
  };
}

function xmlEscape(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function landingText() {
  return (
    `Geomermaids parquetry — S3-compatible read-only API\n` +
    `\n` +
    `Bucket:   ${BUCKET_NAME}\n` +
    `Endpoint: https://s3.geomermaids.com\n` +
    `\n` +
    `Setup (DuckDB):\n` +
    `  INSTALL httpfs; LOAD httpfs;\n` +
    `  SET s3_endpoint='s3.geomermaids.com';\n` +
    `  SET s3_url_style='path';\n` +
    `  SET s3_use_ssl=true;\n` +
    `  SET s3_access_key_id='';\n` +
    `  SET s3_secret_access_key='';\n` +
    `\n` +
    `Datasets, one prefix each (browse them at https://parquetry.geomermaids.com/):\n` +
    `  osm/            OpenStreetMap, North America, nightly. Partitioned; glob it:\n` +
    `    SELECT count(*) FROM read_parquet(\n` +
    `      's3://${BUCKET_NAME}/${DATASET_PREFIX}latest/country=*/state=*/aeroways.parquet');\n` +
    `  gaul/           FAO GAUL 2024, global admin units. One file per level:\n` +
    `    SELECT count(*) FROM read_parquet('s3://${BUCKET_NAME}/gaul/2024/L2.parquet');\n` +
    `  clc/            Corine Land Cover 2018, Europe:\n` +
    `    SELECT count(*) FROM read_parquet('s3://${BUCKET_NAME}/clc/2018/clc_2018.parquet');\n` +
    `  geoboundaries/  geoBoundaries CGAZ, ADM0 to ADM2:\n` +
    `    SELECT count(*) FROM read_parquet('s3://${BUCKET_NAME}/geoboundaries/6.0.0/cgaz_adm1.parquet');\n` +
    `\n` +
    `Every https://parquetry.geomermaids.com/<key> is s3://${BUCKET_NAME}/<key> here.\n` +
    `Fast browser-friendly downloads: https://parquetry.geomermaids.com/\n`
  );
}
