"""Issue #5: which step loses administrative areas that touch the region edge?

Compares the admin-boundary relations `osmium export` can assemble at each
stage for one region:

  source      the unclipped Geofabrik extract
  pipeline    the clipped PBF the pipeline actually produced (-s simple, batch config)
  simple      osmium extract -p <region polygon> -s simple   (should match pipeline)
  complete    same, -s complete_ways
  smart       same, -s smart

then, for relations present in the source but lost in the pipeline, runs
`osmium check-refs` on the relation's members taken from the clipped PBF and
from the source.

  python issue5.py SRC.pbf POLY.geojson PIPELINE.pbf BOUNDARIES.parquet WORKDIR
"""
import json
import re
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import duckdb

SRC, POLY, PIPE_PBF, PARQUET, W = (Path(a) for a in sys.argv[1:6])
W.mkdir(parents=True, exist_ok=True)
TARGETS = {7723824: "Westerly (missing)", 191206: "Woonsocket (missing)",
           7669390: "Foster (present)", 191212: "Pawtucket (present)"}
out: list[str] = []


def say(line: str = "") -> None:
    out.append(line)
    print(line, flush=True)


def run(cmd: list[str], check: bool = True) -> tuple[subprocess.CompletedProcess, float, int]:
    t0 = time.time()
    r = subprocess.run(["/usr/bin/time", "-v", *cmd], capture_output=True, text=True)
    m = re.search(r"Maximum resident set size \(kbytes\): (\d+)", r.stderr)
    if check and r.returncode != 0:
        raise SystemExit(f"failed ({r.returncode}): {' '.join(cmd)}\n{r.stderr[-3000:]}")
    return r, time.time() - t0, int(m.group(1)) if m else 0


def admin_areas(pbf: Path, tag: str) -> tuple[dict[int, tuple[str, str]], list[str]]:
    """Admin relations osmium can assemble into areas: {relation id: (name, level)}."""
    filtered = W / f"{tag}.admin.pbf"
    run(["osmium", "tags-filter", str(pbf), "r/boundary=administrative",
         "-o", str(filtered), "--overwrite"])
    seq = W / f"{tag}.geojsonseq"
    base = ["osmium", "export", str(filtered), "--geometry-types", "polygon",
            "--add-unique-id", "type_id", "-f", "geojsonseq",
            "-x", "print_record_separator=false", "-o", str(seq), "--overwrite"]
    r, _, _ = run(base + ["--show-errors"], check=False)
    if r.returncode != 0:  # older osmium without --show-errors
        r, _, _ = run(base)
    errors = [l for l in r.stdout.splitlines() if l.strip()]
    areas = {}
    for line in seq.read_text().splitlines():
        f = json.loads(line)
        kind, num = f["id"][0], int(f["id"][1:])
        p = f.get("properties") or {}
        if kind == "a" and num % 2 == 1 and p.get("boundary") == "administrative":
            areas[num // 2] = (p.get("name", "?"), p.get("admin_level", "?"))
    return areas, errors


def levels(areas: dict) -> str:
    c = Counter(lvl for _, lvl in areas.values())
    return ", ".join(f"{k}:{c[k]}" for k in sorted(c, key=lambda x: (len(x), x)))


say(f"## Issue #5 debug: {POLY.stem}")
say()
say("osmium: `" + subprocess.run(["osmium", "--version"], capture_output=True,
                                  text=True).stdout.splitlines()[0] + "`")
say()

stages: dict[str, dict] = {}
stats: dict[str, tuple[float, int]] = {}
stages["source"], src_errors = admin_areas(SRC, "source")
stages["pipeline"], pipe_errors = admin_areas(PIPE_PBF, "pipeline")
for strategy, tag in (("simple", "simple"), ("complete_ways", "complete"), ("smart", "smart")):
    clipped = W / f"{tag}.osm.pbf"
    _, secs, rss = run(["osmium", "extract", "-p", str(POLY), "-s", strategy,
                        str(SRC), "-o", str(clipped), "--overwrite"])
    stats[tag] = (secs, rss)
    stages[tag], _ = admin_areas(clipped, tag)

con = duckdb.connect()
parquet_rel = {r[0] for r in con.execute(
    f"SELECT osm_id FROM read_parquet('{PARQUET}', hive_partitioning=false) "
    "WHERE osm_type = 'relation'").fetchall()}

say("### Admin relations assembled per stage")
say()
say("| Stage | Relations | By admin_level | Lost vs source | Extract time | Peak RSS |")
say("|---|---:|---|---:|---:|---:|")
src = stages["source"]
for name, areas in stages.items():
    lost = len(set(src) - set(areas))
    t = stats.get(name)
    ts = f"{t[0]:.1f} s" if t else ""
    rs = f"{t[1] / 1024:.0f} MB" if t else ""
    say(f"| {name} | {len(areas)} | {levels(areas)} | {lost} | {ts} | {rs} |")
say(f"| pipeline parquet | {len(parquet_rel)} | | {len(set(src) - parquet_rel)} | | |")
say()
say("Note: the source extract also holds relations outside the region "
    "(neighbouring towns cut by Geofabrik), so 'lost vs source' includes those.")
say()

say("### Target towns")
say()
say("| Relation | Town | " + " | ".join(stages) + " |")
say("|---|---|" + "---|" * len(stages))
for rid, label in TARGETS.items():
    say(f"| {rid} | {label} | " + " | ".join("yes" if rid in a else "**no**"
                                             for a in stages.values()) + " |")
say()

lost = sorted(set(src) - set(stages["pipeline"]))
inside = [rid for rid in lost if rid in stages["complete"] or rid in stages["smart"]]
say(f"### Lost by the pipeline clip: {len(lost)} relations; "
    f"{len(inside)} of them recovered by complete_ways or smart")
say()
say("| Relation | Name | Level | simple | complete_ways | smart |")
say("|---|---|---|---|---|---|")
for rid in lost:
    n, lvl = src[rid]
    say(f"| {rid} | {n} | {lvl} | " + " | ".join(
        "yes" if rid in stages[s] else "no" for s in ("simple", "complete", "smart")) + " |")
say()

say("### Member completeness of lost relations (osmium check-refs)")
say()
for rid in (inside[:8] or lost[:8]):
    for tag, pbf in (("pipeline", PIPE_PBF), ("source", SRC)):
        sub = W / f"r{rid}.{tag}.pbf"
        g, _, _ = run(["osmium", "getid", "-r", str(pbf), f"r{rid}", "-o", str(sub),
                       "--overwrite"], check=False)
        c, _, _ = run(["osmium", "check-refs", "-r", str(sub)], check=False)
        missing = [l.strip() for l in (c.stdout + c.stderr).splitlines()
                   if "missing" in l.lower()]
        notfound = [l.strip() for l in g.stderr.splitlines() if "not find" in l.lower()]
        say(f"- r{rid} {src[rid][0]} [{tag}]: " + "; ".join(missing + notfound))
say()

say("### Geometry errors reported by osmium export")
say()
for tag, errs in (("source", src_errors), ("pipeline", pipe_errors)):
    say(f"- {tag}: {len(errs)} lines")
    for l in errs[:15]:
        say(f"  - `{l[:200]}`")

(W / "report.md").write_text("\n".join(out) + "\n")
