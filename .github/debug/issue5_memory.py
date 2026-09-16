"""Issue #5, second run: what does clipping with smart or complete_ways cost on
the heaviest nightly jobs, and does it bring the boundaries back at scale?

Mirrors pipeline.osmium_extract_batch exactly (config file, -d, batches of
--extract-batch-size regions per call, default 5) and only varies -s.

  python issue5_memory.py SOURCE.pbf ADMIN.geojson WORKDIR ISO_OR_PREFIX [...]
    e.g. ... US-CA            one region
         ... MX-              every region whose ISO starts with MX-
"""
import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from pipeline import load_states, write_state_polygon  # noqa: E402

SRC, ADMIN, W = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
WANT = sys.argv[4:]
BATCH = 5
STRATEGIES = ["simple", "smart", "complete_ways"]
W.mkdir(parents=True, exist_ok=True)
out: list[str] = []


def say(line: str = "") -> None:
    out.append(line)
    print(line, flush=True)


def timed(cmd: list[str]) -> tuple[float, int]:
    t0 = time.time()
    r = subprocess.run(["/usr/bin/time", "-v", *cmd], capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"failed: {' '.join(cmd)}\n{r.stderr[-3000:]}")
    rss = int(re.search(r"Maximum resident set size \(kbytes\): (\d+)", r.stderr).group(1))
    return time.time() - t0, rss


def counts(pbf: Path) -> dict[str, int]:
    r = subprocess.run(["osmium", "fileinfo", "-e", "-j", str(pbf)],
                       capture_output=True, text=True, check=True)
    c = json.loads(r.stdout)["data"]["count"]
    return {k: c[k] for k in ("nodes", "ways", "relations")}


def admin_levels(pbf: Path, tmp: Path) -> dict[str, int]:
    f = tmp / "admin.pbf"
    seq = tmp / "admin.geojsonseq"
    subprocess.run(["osmium", "tags-filter", str(pbf), "r/boundary=administrative",
                    "-o", str(f), "--overwrite"], check=True, capture_output=True)
    subprocess.run(["osmium", "export", str(f), "--geometry-types", "polygon",
                    "--add-unique-id", "type_id", "-f", "geojsonseq",
                    "-x", "print_record_separator=false", "-o", str(seq), "--overwrite"],
                   check=True, capture_output=True)
    levels: dict[str, int] = {}
    for line in seq.read_text().splitlines():
        feat = json.loads(line)
        p = feat.get("properties") or {}
        if feat["id"].startswith("a") and int(feat["id"][1:]) % 2 == 1 \
                and p.get("boundary") == "administrative":
            lvl = p.get("admin_level", "?")
            levels[lvl] = levels.get(lvl, 0) + 1
    return levels


states = [s for s in load_states(ADMIN)
          if any(s.iso == w or (w.endswith("-") and s.iso.startswith(w)) for w in WANT)]
if not states:
    raise SystemExit(f"no regions match {WANT}")
polys = {}
for s in states:
    (W / "polys").mkdir(exist_ok=True)
    polys[s.iso] = W / "polys" / f"{s.iso}.geojson"
    write_state_polygon(s, polys[s.iso])
isos = sorted(polys)
batches = [isos[i:i + BATCH] for i in range(0, len(isos), BATCH)]

version = subprocess.run(["osmium", "--version"], capture_output=True, text=True).stdout.splitlines()[0]
say(f"## {', '.join(WANT)}: {len(isos)} regions, {len(batches)} extract call(s) of up to {BATCH}")
say()
say(f"Source `{SRC.name}` {SRC.stat().st_size / 1e9:.2f} GB, {version}")
say()
src_counts = counts(SRC)
say(f"Source elements: {src_counts['nodes']:,} nodes, {src_counts['ways']:,} ways, "
    f"{src_counts['relations']:,} relations")
say()

rows = []
per_region: dict[str, dict[str, dict]] = {iso: {} for iso in isos}
for strategy in STRATEGIES:
    dest = W / strategy
    shutil.rmtree(dest, ignore_errors=True)
    dest.mkdir()
    total_s, peak = 0.0, 0
    for batch in batches:
        for iso in batch:
            (dest / iso).mkdir(exist_ok=True)
        cfg = dest / "_extract_config.json"
        cfg.write_text(json.dumps({"extracts": [
            {"output": f"{iso}/{iso}.osm.pbf",
             "polygon": {"file_name": str(polys[iso].resolve()), "file_type": "geojson"}}
            for iso in batch]}))
        secs, rss = timed(["osmium", "extract", "-c", str(cfg), "-d", str(dest),
                           "-s", strategy, "--overwrite", str(SRC)])
        total_s += secs
        peak = max(peak, rss)
    size = sum((dest / iso / f"{iso}.osm.pbf").stat().st_size for iso in isos)
    elems = {"nodes": 0, "ways": 0, "relations": 0}
    with_state = admin_total = 0
    for iso in isos:
        pbf = dest / iso / f"{iso}.osm.pbf"
        c = counts(pbf)
        for k in elems:
            elems[k] += c[k]
        lv = admin_levels(pbf, dest / iso)
        per_region[iso][strategy] = lv
        admin_total += sum(lv.values())
        with_state += 1 if lv.get("4") else 0
    rows.append((strategy, total_s, peak, size, elems, admin_total, with_state))
    shutil.rmtree(dest)  # keep the runner disk free for the next strategy

base = rows[0]
say("| Strategy | Extract time | Peak RSS | Output | Ways | Admin areas | Regions with own admin_level 4 |")
say("|---|---:|---:|---:|---:|---:|---:|")
for strategy, secs, rss, size, elems, admin_total, with_state in rows:
    grow = f" (+{100 * (elems['ways'] / base[4]['ways'] - 1):.1f}%)" if strategy != "simple" else ""
    say(f"| {strategy} | {secs:.0f} s | {rss / 1024 / 1024:.1f} GB | {size / 1e9:.2f} GB | "
        f"{elems['ways']:,}{grow} | {admin_total:,} | {with_state} of {len(isos)} |")
say()
if len(isos) > 1:
    say("Admin areas per region (simple → smart):")
    say()
    for iso in isos:
        a = sum(per_region[iso]["simple"].values())
        b = sum(per_region[iso]["smart"].values())
        s4 = "yes" if per_region[iso]["smart"].get("4") else "no"
        say(f"- {iso}: {a} → {b}, own state relation with smart: {s4}")
(W / "report.md").write_text("\n".join(out) + "\n")
