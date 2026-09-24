#!/usr/bin/env python3
"""
Write the files that make /gaul/ a parquetry repository, from its manifest.

A parquetry repository browser (GeoPQ Workbench's File > Repositories)
reads three things: `snapshots.json` at the base (which prefix `latest`
is), `index.json` at the base (the dataset folders), and a `_manifest.json`
in each folder (`themes`: file stem -> row count, one `<stem>.parquet` per
theme beside it). GAUL has one whole-world dataset directly under the
release and one folder per country, all described by the release's own
_manifest.json, so everything here is derived from it and no parquet is read.

  python3 scripts/datasets/gaul_repository.py --manifest _manifest.json \\
      --out-dir build/gaul-repo
  rclone copy build/gaul-repo parquetry:parquetry/gaul \\
      --header-upload "Cache-Control: public, max-age=300"

Writes <out>/snapshots.json, <out>/index.json,
<out>/<release>/_manifest.json (the input, `themes` keyed by file), and
<out>/<release>/country=<iso3>/_manifest.json.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

RELEASE = "2024"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--manifest", type=Path, required=True,
                   help="the live <release>/_manifest.json")
    p.add_argument("--out-dir", type=Path, required=True)
    args = p.parse_args()
    m = json.loads(args.manifest.read_text())
    out = args.out_dir
    rel = out / RELEASE

    # The whole-world files: `themes` named after the files they are.
    m["themes"] = {Path(f["file"]).stem: f["features"] for f in m["files"]}
    rel.mkdir(parents=True, exist_ok=True)
    (rel / "_manifest.json").write_text(json.dumps(m, indent=2) + "\n")

    # Per country: every layer partitioned to it, in the release's order.
    countries: dict[str, dict] = {}
    for part in m["partitions"]:
        for e in part["entries"]:
            c = countries.setdefault(e["country"], {"names": e["names"], "themes": {}})
            c["themes"][part["name"]] = e["features"]
    order = [Path(f["file"]).stem.removeprefix(f"GAUL_{RELEASE}_") for f in m["files"]]
    datasets = [{"path": "", "code": "WORLD",
                 "name": f"GAUL {RELEASE}, whole world"}]
    for code in sorted(countries):
        c = countries[code]
        themes = {t: c["themes"][t] for t in order if t in c["themes"]}
        name = " / ".join(c["names"]) or code
        folder = rel / f"country={code}"
        folder.mkdir(exist_ok=True)
        (folder / "_manifest.json").write_text(json.dumps({
            "country": code, "state_name": name,
            "total_features": sum(themes.values()), "themes": themes,
        }, indent=2) + "\n")
        datasets.append({"path": f"country={code}", "code": code, "name": name})

    (out / "index.json").write_text(json.dumps({"datasets": datasets}, indent=2) + "\n")
    (out / "snapshots.json").write_text(json.dumps({
        "latest": f"{RELEASE}/",
        "snapshots": [{"date": RELEASE, "path": f"{RELEASE}/"}],
    }, indent=2) + "\n")
    print(f"  {len(countries)} country manifests, index.json, snapshots.json -> {out}")


if __name__ == "__main__":
    main()
