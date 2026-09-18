# Portolan conformance

Two catalogs are published: the live one at
`https://parquetry.geomermaids.com/catalog/catalog.json`, whose globs read
`latest/`, and a pinned copy under `catalog/<YYYY-MM-DD>/` for every snapshot
retention keeps. Both are rendered by the same generator and validated the
same way, and the gates below cover both. They target
[portolan-spec](https://github.com/portolan-sdi/portolan-spec) v0.2.0 and is
validated with rashid 0.1.8, pinned in `scripts/pyproject.toml`. Where rashid
and the spec disagree, the spec decides; file the disagreement against rashid
with the `PORTO` id and record it here.

`scripts/catalog.py` renders the catalog from the per-region manifests.
Nothing in `catalog/` except this file and `thumbnails/` is hand-edited, and
the published JSON and Markdown are never edited in place.

## What runs where

| Gate | Runs | Covers |
|---|---|---|
| `catalog.py selftest` | `catalog.yml`, every PR and push touching the catalog | Profile schema, STAC structure, links, providers, license, mirror rules, thumbnail bytes. The fixture catalog must pass in both forms, live and pinned; eight planted violations must each fail. |
| `catalog.py check-data` | `nightly.yml`, every build job, before upload | rashid data pass (`PTL-DAT-*`) over that job's partitions: 150,000-row cap, spatial statistics, GeoParquet version, one schema per theme. |
| `catalog.py` via `publish.py` finalize | `nightly.yml`, finalize, before upload | Same metadata pass on the real catalog. A failure stops the catalog upload and fails the run. |

The published glob is `s3://parquetry/latest/...` or `s3://parquetry/<date>/...`,
which rashid cannot expand from a local tree, so the data rules would never
reach the partitions in a metadata run. `check-data` renders a throwaway
catalog whose globs point at the job's local files instead.

A note on what the live catalog claims: its row counts, extents and file
counts are measured from the snapshot it was generated from, while its globs
read `latest/`, which the next night replaces. The two agree until the next
build finishes. A pinned catalog has no such gap, which is why documentation
sends anyone who needs reproducibility to one.

## Findings accepted as they stand

No error-severity finding is accepted. These lower-severity findings are
expected on every run:

| Rule | Severity | Why |
|---|---|---|
| `PTL-PRO-002` | info | No `canonical` link. OpenStreetMap publishes no STAC catalog to point at; the `via` link names the source. |
| `PTL-DAT-007` | warning | Per-row-group spatial statistics come from native Parquet `GEOMETRY` statistics (allowed for GeoParquet 2.x), not a declared `bbox` covering. The `bbox` column exists with min/max statistics, but declaring it as a covering needs a rewrite that drops DuckDB's bloom filters (see `write_theme_parquet` in `scripts/pipeline.py`). |
| `PTL-DAT-006` | info | rashid evaluates spatial ordering from the covering column, so with none declared it cannot score it. Rows are Hilbert-ordered on each file's own extent. |

No PMTiles are published, so the collections have no visualization
derivative and no style assets (`PTL-VIZ-002` does not apply). Each carries a
thumbnail rendered by `scripts/thumbnails.py`.

## Negative controls

Run on 2026-09-15 with rashid 0.1.8:

- `check-data` over US-RI and US-DE with RI buildings rewritten as one
  262,722-row row group: `PTL-DAT-008` fired at `/partition:glob`, exit 1.
- Same pair with the `lanes` column dropped from DE roads: `PTL-DAT-014`
  fired at `/partition:glob`, exit 1.
- `selftest`: proprietary license, missing agents link, missing host
  provider, missing thumbnail, missing via link, stale thumbnail checksum,
  missing `partition:keys` and a missing README were each caught.

## Hosting

rashid's live probe (`--live`) against `parquetry.geomermaids.com` found range
requests conformant and one CORS gap: `Access-Control-Expose-Headers` omitted
`Content-Type` (`PTL-LIV-004`). Fixed in `files/` and `api/` and deployed on
2026-09-16. On 2026-09-17, rashid 0.1.8 with `--schema --live` over a mirror of
the published live catalog reported no errors and no warnings.
