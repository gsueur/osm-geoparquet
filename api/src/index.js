// S3-compatible read-only facade over the `parquetry` R2 bucket.
//
// Purpose: let DuckDB / httpfs clients glob our public data with
// `s3://parquetry/latest/country=*/state=*/<theme>.parquet`. Plain HTTPS
// can't do that (no LIST), so this Worker answers path-style
// ListObjectsV2 and GETs backed by an R2 binding.
//
// Everything is anonymous + read-only. No signing, no writes.

const BUCKET_NAME = "parquetry";

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
    return handleObject(key, request, env);
  },
};

async function handleList(url, env) {
  const prefix = url.searchParams.get("prefix") || "";
  const delimiter = url.searchParams.get("delimiter") || undefined;
  const continuationToken = url.searchParams.get("continuation-token") || undefined;
  const startAfter = url.searchParams.get("start-after") || undefined;
  const maxKeysParam = url.searchParams.get("max-keys");
  const maxKeys = Math.min(maxKeysParam ? parseInt(maxKeysParam, 10) : 1000, 1000);

  const listOpts = { prefix, limit: maxKeys };
  if (delimiter) listOpts.delimiter = delimiter;
  if (continuationToken) listOpts.cursor = continuationToken;
  if (startAfter && !continuationToken) listOpts.startAfter = startAfter;

  const list = await env.BUCKET.list(listOpts);

  const xml = buildListXml(list, {
    prefix,
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
  const { prefix, delimiter, maxKeys, continuationToken, startAfter } = params;
  const prefixes = list.delimitedPrefixes || [];
  const keyCount = list.objects.length + prefixes.length;

  const contents = list.objects
    .map(
      (obj) =>
        `<Contents>` +
        `<Key>${xmlEscape(obj.key)}</Key>` +
        `<LastModified>${obj.uploaded.toISOString()}</LastModified>` +
        `<ETag>${xmlEscape(obj.httpEtag)}</ETag>` +
        `<Size>${obj.size}</Size>` +
        `<StorageClass>STANDARD</StorageClass>` +
        `</Contents>`,
    )
    .join("");

  const commonPrefixes = prefixes
    .map((p) => `<CommonPrefixes><Prefix>${xmlEscape(p)}</Prefix></CommonPrefixes>`)
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
    return new Response(null, { status: 200, headers: objectHeaders(head) });
  }

  const rangeHeader = request.headers.get("range");
  const getOpts = {};
  const parsedRange = rangeHeader ? parseRange(rangeHeader) : undefined;
  if (parsedRange) getOpts.range = parsedRange;

  const object = await env.BUCKET.get(key, getOpts);
  if (!object) return new Response("Not Found", { status: 404, headers: corsHeaders() });

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
    "Access-Control-Expose-Headers": "ETag, Content-Length, Content-Range, Accept-Ranges",
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
    `Example (DuckDB):\n` +
    `  INSTALL httpfs; LOAD httpfs;\n` +
    `  SET s3_endpoint='s3.geomermaids.com';\n` +
    `  SET s3_url_style='path';\n` +
    `  SET s3_use_ssl=true;\n` +
    `  SET s3_access_key_id='';\n` +
    `  SET s3_secret_access_key='';\n` +
    `  SELECT count(*) FROM read_parquet(\n` +
    `    's3://${BUCKET_NAME}/latest/country=*/state=*/aeroways.parquet'\n` +
    `  );\n` +
    `\n` +
    `Fast browser-friendly downloads: https://parquetry.geomermaids.com/\n`
  );
}
