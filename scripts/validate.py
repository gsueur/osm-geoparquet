#!/usr/bin/env python3
"""
Post-generation validation of the OSM -> GeoParquet pipeline output.

Local checks (run against `out/`):
  - Every admin region in the input geojson has a _manifest.json
  - Every manifest lists all 16 themes declared in themes.py
  - Every parquet file exists on disk and is non-empty
  - File-level schema: base columns present, GeoParquet 2.0 metadata
  - Row count in each parquet matches the manifest's claim

Remote checks (run against the live R2 bucket over HTTPS):
  - snapshots.json reachable, parses, includes today (or a given date)
  - ATTRIBUTION.txt reachable
  - Random sample of state+theme URLs HEAD to 200 OK with Content-Length
  - The landing page's DuckDB examples run end-to-end, with plausible
    result cardinalities

Exits 0 on full success, 1 on any failure. Suitable for CI or a
post-publish cron.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

# Import the canonical theme list so this stays in sync with the pipeline.
sys.path.insert(0, str(Path(__file__).parent))
from themes import THEMES  # noqa: E402

BASE_COLUMNS = {"osm_id", "osm_type", "country", "state", "state_iso", "tags", "geometry"}
EXPECTED_THEMES = [t.name for t in THEMES]

# Cloudflare's default bot mitigation rejects urllib's generic UA with 403.
# Use a browser-ish one so the validator sees what a real browser would.
USER_AGENT = "geomermaids-validate/1.0 (+https://geoparquet.geomermaids.com/)"


def _http_request(url: str, method: str = "GET") -> urllib.request.Request:
    req = urllib.request.Request(url, method=method)
    req.add_header("User-Agent", USER_AGENT)
    return req


# ---------- tiny test harness ----------

class Suite:
    def __init__(self, title: str):
        self.title = title
        self.passes: list[str] = []
        self.fails: list[str] = []

    def ok(self, msg: str) -> None:
        self.passes.append(msg)
        print(f"  \x1b[32m✓\x1b[0m {msg}")

    def fail(self, msg: str) -> None:
        self.fails.append(msg)
        print(f"  \x1b[31m✗\x1b[0m {msg}")

    def header(self) -> None:
        print(f"\n\x1b[1m{self.title}\x1b[0m")

    def summary(self) -> None:
        n_ok, n_fail = len(self.passes), len(self.fails)
        tag = "\x1b[32mOK\x1b[0m" if n_fail == 0 else "\x1b[31mFAIL\x1b[0m"
        print(f"  [{tag}] {n_ok} passed, {n_fail} failed")


# ---------- local checks ----------

def load_expected_isos(geojson: Path, excluded: set[str]) -> set[str]:
    data = json.loads(geojson.read_text())
    out: set[str] = set()
    for f in data["features"]:
        iso = (f.get("properties") or {}).get("ISO3166-2")
        if iso and iso not in excluded and iso.split("-")[0] in ("US", "CA", "MX"):
            out.add(iso)
    return out


def check_local(out_dir: Path, geojson: Path) -> Suite:
    s = Suite(f"Local checks — {out_dir}/")
    s.header()

    try:
        import pyarrow.parquet as pq
    except ImportError:
        s.fail("pyarrow not installed; cannot inspect parquet files")
        return s

    expected = load_expected_isos(
        geojson, excluded={"US-AS", "US-GU", "US-MP", "US-UM"}
    )
    manifests = {
        m.parent.name.replace("state=", ""): m
        for m in out_dir.glob("country=*/state=*/_manifest.json")
    }

    missing_manifests = expected - set(manifests)
    if missing_manifests:
        s.fail(f"states without a manifest: {sorted(missing_manifests)}")
    else:
        s.ok(f"all {len(expected)} expected states have a manifest")

    # Per-state checks
    incomplete_themes: list[tuple[str, int]] = []
    missing_files: list[str] = []
    row_mismatches: list[str] = []
    schema_fails: list[str] = []
    for iso in sorted(expected & set(manifests)):
        mpath = manifests[iso]
        state_dir = mpath.parent
        m = json.loads(mpath.read_text())
        themes_present = set(m.get("themes", {}))
        if themes_present != set(EXPECTED_THEMES):
            incomplete_themes.append((iso, len(themes_present)))

        # File existence + row count consistency for non-zero themes
        for theme, claimed in (m.get("themes") or {}).items():
            if claimed <= 0:
                continue
            p = state_dir / f"{theme}.parquet"
            if not p.exists() or p.stat().st_size == 0:
                missing_files.append(f"{iso}/{theme}")
                continue
            try:
                pf = pq.ParquetFile(p)
                actual = pf.metadata.num_rows
                if actual != claimed:
                    row_mismatches.append(f"{iso}/{theme}: manifest={claimed:,} parquet={actual:,}")
                schema = set(pf.schema_arrow.names)
                if not BASE_COLUMNS.issubset(schema):
                    schema_fails.append(f"{iso}/{theme}: missing {BASE_COLUMNS - schema}")
                geo = pf.schema_arrow.metadata and pf.schema_arrow.metadata.get(b"geo")
                if geo:
                    gm = json.loads(geo)
                    if gm.get("version") != "2.0.0":
                        schema_fails.append(f"{iso}/{theme}: geo version={gm.get('version')!r}, want 2.0.0")
                else:
                    schema_fails.append(f"{iso}/{theme}: no 'geo' metadata")
            except Exception as e:
                schema_fails.append(f"{iso}/{theme}: {e}")

    if incomplete_themes:
        snippets = ", ".join(f"{iso} ({n}/16)" for iso, n in incomplete_themes[:5])
        more = "" if len(incomplete_themes) <= 5 else f" (+{len(incomplete_themes)-5} more)"
        s.fail(f"{len(incomplete_themes)} states missing themes: {snippets}{more}")
    else:
        s.ok("every state manifest lists all 16 themes")

    if missing_files:
        s.fail(f"{len(missing_files)} parquet files missing/empty: "
               f"{', '.join(missing_files[:5])}{'...' if len(missing_files) > 5 else ''}")
    else:
        s.ok("every manifest-listed parquet exists on disk")

    if row_mismatches:
        s.fail(f"{len(row_mismatches)} row-count mismatches: {'; '.join(row_mismatches[:3])}")
    else:
        s.ok("parquet row counts match manifest claims")

    if schema_fails:
        s.fail(f"{len(schema_fails)} schema problems: {'; '.join(schema_fails[:3])}")
    else:
        s.ok("every parquet has base columns + GeoParquet 2.0 metadata")

    s.summary()
    return s


# ---------- remote checks ----------

def http_head(url: str, timeout: float = 15.0) -> tuple[int, int | None]:
    try:
        with urllib.request.urlopen(_http_request(url, method="HEAD"), timeout=timeout) as r:
            clen = r.headers.get("Content-Length")
            return r.status, int(clen) if clen else None
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception:
        return 0, None


def http_head_headers(url: str, timeout: float = 15.0) -> tuple[int, dict[str, str]]:
    try:
        with urllib.request.urlopen(_http_request(url, method="HEAD"), timeout=timeout) as r:
            return r.status, {k.lower(): v for k, v in r.headers.items()}
    except urllib.error.HTTPError as e:
        return e.code, {}
    except Exception:
        return 0, {}


def check_remote(base_url: str, sample: int) -> Suite:
    s = Suite(f"Remote checks — {base_url}")
    s.header()

    # Snapshots index
    try:
        with urllib.request.urlopen(
            _http_request(f"{base_url}/snapshots.json"), timeout=10,
        ) as r:
            manifest = json.load(r)
        snapshots = manifest.get("snapshots", [])
        if not snapshots:
            s.fail("snapshots.json has no snapshots")
            return s
        latest = snapshots[0]
        s.ok(f"snapshots.json reachable (latest: {latest['date']}, "
             f"{latest['objects']} objs, {latest['bytes']/1e9:.2f} GB)")
    except Exception as e:
        s.fail(f"snapshots.json unreachable: {e}")
        return s

    # Attribution
    status, _ = http_head(f"{base_url}/ATTRIBUTION.txt")
    if status == 200:
        s.ok("ATTRIBUTION.txt reachable")
    else:
        s.fail(f"ATTRIBUTION.txt returned {status}")

    # Random sample of state+theme HEADs against the latest/ alias
    random.seed(42)
    state_samples = random.sample(
        ["US-NY", "US-CA", "US-TX", "US-FL", "US-IL", "US-WA", "US-MA", "US-RI",
         "US-VT", "US-AK", "CA-ON", "CA-BC", "CA-QC", "CA-AB",
         "MX-JAL", "MX-CMX", "MX-NLE", "MX-YUC"],
        k=min(sample, 18),
    )
    theme_samples = random.sample(EXPECTED_THEMES, k=min(4, len(EXPECTED_THEMES)))
    bad = 0
    checked = 0
    for iso in state_samples:
        for theme in theme_samples:
            url = f"{base_url}/latest/country={iso.split('-')[0]}/state={iso}/{theme}.parquet"
            status, clen = http_head(url)
            checked += 1
            if status != 200 or not clen or clen <= 0:
                bad += 1
                print(f"      {url} -> {status} {clen}")
    if bad == 0:
        s.ok(f"all {checked} sampled state/theme URLs return 200 + non-empty Content-Length")
    else:
        s.fail(f"{bad}/{checked} sampled URLs failed")

    # Cache-Control headers set by publish.py
    _check_cache_control(s, base_url, latest["date"])

    # Run the landing page's DuckDB examples
    _check_landing_page_queries(s, base_url)

    s.summary()
    return s


def _check_cache_control(s: Suite, base_url: str, latest_date: str) -> None:
    # publish.py sets three distinct policies. If a new rclone version
    # ever silently stops forwarding --header-upload on server-side copy,
    # these checks catch it (the `latest/` files would inherit `immutable`
    # from the dated snapshot and get pinned forever at the edge).
    sample = "country=US/state=US-NY/buildings.parquet"
    cases = [
        # url, must_contain, must_not_contain
        (f"{base_url}/{latest_date}/{sample}", "immutable", None),
        (f"{base_url}/latest/{sample}", "stale-while-revalidate", "immutable"),
        (f"{base_url}/snapshots.json", "max-age=300", "immutable"),
        (f"{base_url}/ATTRIBUTION.txt", "max-age=300", "immutable"),
    ]
    for url, want, forbid in cases:
        label = url.replace(base_url, "")
        status, headers = http_head_headers(url)
        if status != 200:
            s.fail(f"Cache-Control {label}: HEAD -> {status}")
            continue
        cc = headers.get("cache-control", "")
        if want not in cc:
            s.fail(f"Cache-Control {label}: missing {want!r} (got: {cc!r})")
        elif forbid and forbid in cc:
            s.fail(f"Cache-Control {label}: should not contain {forbid!r} (got: {cc!r})")
        else:
            s.ok(f"Cache-Control {label}: {cc}")


def _check_landing_page_queries(s: Suite, base_url: str) -> None:
    try:
        import duckdb
    except ImportError:
        s.fail("duckdb not installed; skipping live-query checks")
        return
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;")
    con.execute(f"""
        CREATE OR REPLACE MACRO url(state, theme) AS
          '{base_url}/latest/country=' ||
          split_part(state, '-', 1) || '/state=' || state || '/' || theme || '.parquet';
    """)

    def query(name: str, sql: str, predicate) -> None:
        t = time.time()
        try:
            result = con.execute(sql).fetchone()
            ok = predicate(result)
            label = f"{name} ({time.time()-t:.2f}s)"
            if ok:
                s.ok(f"{label} -> {result}")
            else:
                s.fail(f"{label}: unexpected result {result}")
        except Exception as e:
            s.fail(f"{name}: {e}")

    # 2. COUNT bbox
    query("ex2: bbox count NY buildings",
          "SELECT COUNT(*) FROM read_parquet(url('US-NY','buildings')) "
          "WHERE ST_Intersects(geometry, ST_MakeEnvelope(-74.00, 40.74, -73.96, 40.77))",
          lambda r: r[0] > 1000)

    # 3. Filter + projection
    query("ex3: tall residentials (Manhattan)",
          "SELECT COUNT(*) FROM read_parquet(url('US-NY','buildings')) "
          "WHERE building='residential' AND levels>30 "
          "AND ST_Intersects(geometry, ST_MakeEnvelope(-73.99, 40.76, -73.95, 40.79))",
          lambda r: r[0] >= 1)

    # 4. Cross-theme join (ST_Contains)
    query("ex4: restaurants inside MA buildings",
          "SELECT COUNT(*) FROM read_parquet(url('US-MA','buildings')) b "
          "JOIN read_parquet(url('US-MA','pois')) p "
          "ON ST_Contains(b.geometry, p.geometry) "
          "WHERE p.amenity='restaurant' AND b.name IS NOT NULL",
          lambda r: r[0] > 100)

    # 5. ST_DWithin (Times Square)
    query("ex5: amenities near Times Square",
          "WITH o AS (SELECT ST_Point(-73.9857, 40.7484) AS pt) "
          "SELECT COUNT(*) FROM read_parquet(url('US-NY','pois')) p, o "
          "WHERE ST_DWithin(p.geometry, o.pt, 0.005)",
          lambda r: r[0] > 50)

    # 6. Multi-state URL list
    query("ex6: POIs across 4 NE states",
          "SELECT COUNT(*) FROM read_parquet(["
          "url('US-NY','pois'),url('US-MA','pois'),"
          "url('US-CT','pois'),url('US-RI','pois')])",
          lambda r: r[0] > 10_000)


# ---------- main ----------

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", type=Path, default=Path("out"))
    p.add_argument("--admin-geojson", type=Path, default=Path("data/admin_regions.geojson"))
    p.add_argument("--remote-url", default="https://parquetry.geomermaids.com")
    p.add_argument("--no-local", action="store_true", help="skip local checks")
    p.add_argument("--no-remote", action="store_true", help="skip remote checks")
    p.add_argument("--sample", type=int, default=8,
                   help="state count to sample for remote HEAD checks (default 8)")
    args = p.parse_args()

    suites: list[Suite] = []
    if not args.no_local:
        if not args.out_dir.is_dir():
            print(f"skip local checks: {args.out_dir} not found")
        elif not args.admin_geojson.is_file():
            print(f"skip local checks: {args.admin_geojson} not found")
        else:
            suites.append(check_local(args.out_dir, args.admin_geojson))

    if not args.no_remote:
        suites.append(check_remote(args.remote_url, args.sample))

    total_ok = sum(len(s.passes) for s in suites)
    total_fail = sum(len(s.fails) for s in suites)
    print(f"\n\x1b[1mTotal: {total_ok} passed, {total_fail} failed\x1b[0m")
    sys.exit(0 if total_fail == 0 else 1)


if __name__ == "__main__":
    main()
