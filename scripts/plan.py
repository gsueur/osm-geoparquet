#!/usr/bin/env python3
"""
Build the GitHub Actions build matrix: one job per Geofabrik extract.

Reads the admin-region GeoJSON, resolves every ISO3166-2 code to the most
specific Geofabrik extract that covers it (state/province extract when one
exists, otherwise the country extract), and groups regions by extract.

Geofabrik's index carries `iso3166-2` / `iso3166-1:alpha2` per extract, so
no hand-maintained mapping is needed; adding a country to the coverage is a
GeoJSON change only.

Usage:
  python3 scripts/plan.py --states-geojson data/admin_regions.geojson
  python3 scripts/plan.py ... --only US-RI US-VT          # manual subset
  python3 scripts/plan.py ... --output matrix.json        # for $GITHUB_OUTPUT

Output (stdout or --output): {"include": [{id, url, states, workers}, ...]}
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

INDEX_URL = "https://download.geofabrik.de/index-v1-nogeom.json"

# Present in the admin-region GeoJSON but out of coverage (matches validate.py).
DEFAULT_EXCLUDE = ["US-AS", "US-GU", "US-MP", "US-UM"]

# Per-job worker cap. Runners have 4 vCPUs; osmium and DuckDB are already
# multithreaded, so more than 3 concurrent states just thrashes.
MAX_WORKERS = 3


def load_isos(geojson: Path) -> list[str]:
    data = json.loads(geojson.read_text())
    isos = []
    for f in data.get("features", []):
        iso = (f.get("properties") or {}).get("ISO3166-2")
        if iso:
            isos.append(iso)
    if not isos:
        sys.exit(f"no ISO3166-2 features in {geojson}")
    return sorted(set(isos))


def load_index(src: str) -> list[dict]:
    if src.startswith(("http://", "https://")):
        req = urllib.request.Request(src, headers={"User-Agent": "osm-geoparquet-plan/1.0"})
        with urllib.request.urlopen(req, timeout=60) as r:
            data = json.load(r)
    else:
        data = json.loads(Path(src).read_text())
    return [f["properties"] for f in data["features"]]


def depth(props: dict, by_id: dict[str, dict]) -> int:
    """Number of ancestors; deeper = more specific extract."""
    d, cur = 0, props
    while cur.get("parent"):
        d += 1
        cur = by_id.get(cur["parent"], {})
    return d


def resolve(isos: list[str], index: list[dict]) -> dict[str, dict]:
    by_id = {p["id"]: p for p in index}
    by_iso2: dict[str, list[dict]] = {}
    by_country: dict[str, list[dict]] = {}
    for p in index:
        for code in p.get("iso3166-2") or []:
            by_iso2.setdefault(code, []).append(p)
        for code in p.get("iso3166-1:alpha2") or []:
            by_country.setdefault(code, []).append(p)

    groups: dict[str, dict] = {}
    unresolved = []
    for iso in isos:
        cands = by_iso2.get(iso) or by_country.get(iso.split("-")[0]) or []
        if not cands:
            unresolved.append(iso)
            continue
        best = max(cands, key=lambda p: depth(p, by_id))
        g = groups.setdefault(best["id"], {
            "id": best["id"],
            "url": best["urls"]["pbf"],
            "states": [],
        })
        g["states"].append(iso)

    if unresolved:
        sys.exit(f"no Geofabrik extract found for: {', '.join(unresolved)}")
    return groups


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--states-geojson", type=Path, default=Path("data/admin_regions.geojson"))
    p.add_argument("--index", default=INDEX_URL,
                   help="Geofabrik index (URL or local JSON). Default: live index.")
    p.add_argument("--exclude", nargs="*", default=DEFAULT_EXCLUDE)
    p.add_argument("--only", nargs="*", default=None,
                   help="Restrict to these ISO codes (manual runs).")
    p.add_argument("--output", type=Path, default=None,
                   help="Write matrix JSON here instead of stdout.")
    args = p.parse_args()

    isos = [i for i in load_isos(args.states_geojson) if i not in set(args.exclude)]
    if args.only:
        wanted = set(args.only)
        missing = wanted - set(isos)
        if missing:
            sys.exit(f"--only codes not in geojson (or excluded): {', '.join(sorted(missing))}")
        isos = [i for i in isos if i in wanted]

    groups = resolve(isos, load_index(args.index))

    include = []
    # Biggest groups first so the long-running multi-region jobs start early.
    for g in sorted(groups.values(), key=lambda g: (-len(g["states"]), g["id"])):
        include.append({
            "id": g["id"],
            "url": g["url"],
            "states": " ".join(sorted(g["states"])),
            "workers": min(MAX_WORKERS, len(g["states"])),
        })

    matrix = {"include": include}
    text = json.dumps(matrix, separators=(",", ":"))
    if args.output:
        args.output.write_text(text)
    else:
        print(text)

    print(f"{len(isos)} regions -> {len(include)} jobs", file=sys.stderr)
    for e in include:
        n = len(e["states"].split())
        print(f"  {e['id']:<32} {n:>3} region{'s' if n != 1 else ''}", file=sys.stderr)


if __name__ == "__main__":
    main()
