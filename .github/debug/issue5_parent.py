"""Issue #5, second half: do US admin relations assemble from a parent extract?

Compares, per US region, the boundaries the pipeline produces from a parent
extract (the US or North America file, boundary relations pre-filtered) with
what the current nightly publishes from the per-state extract:

  rows            boundary rows in the region file
  own relation    a row with admin_level 4 and the region's ISO3166-2 code
  counties        rows with admin_level 6

  python issue5_parent.py OUT_DIR US-AL US-AK ...
"""
import sys
from pathlib import Path

import duckdb

OUT = Path(sys.argv[1])
ISOS = sys.argv[2:]
LATEST = "https://parquetry.geomermaids.com/osm/latest"

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs; SET http_timeout=600000;")


def measure(path: str, iso: str) -> tuple[int, bool, int] | None:
    try:
        rows, own, counties = con.execute(f"""
            SELECT count(*),
                   bool_or(admin_level = 4 AND iso3166_2 = ?),
                   count(*) FILTER (WHERE admin_level = 6)
            FROM read_parquet('{path}', hive_partitioning = false)
        """, [iso]).fetchone()
        return rows, bool(own), counties
    except Exception as e:  # noqa: BLE001
        print(f"  {iso}: {path}: {str(e)[:120]}")
        return None


lines = ["| Region | Rows: latest | Rows: parent | Own relation: latest | Own relation: parent | "
         "Counties: latest | Counties: parent |", "|---|---:|---:|---|---|---:|---:|"]
own_before = own_after = 0
rows_before = rows_after = 0
gained = []
for iso in ISOS:
    new = measure(str(OUT / "country=US" / f"state={iso}" / "boundaries.parquet"), iso)
    old = measure(f"{LATEST}/country=US/state={iso}/boundaries.parquet", iso)
    if new is None or old is None:
        lines.append(f"| {iso} | {'?' if old is None else old[0]} | "
                     f"{'?' if new is None else new[0]} | | | | |")
        continue
    own_before += old[1]
    own_after += new[1]
    rows_before += old[0]
    rows_after += new[0]
    if new[1] and not old[1]:
        gained.append(iso)
    yes = lambda b: "yes" if b else "no"  # noqa: E731
    lines.append(f"| {iso} | {old[0]:,} | {new[0]:,} | {yes(old[1])} | {yes(new[1])} | "
                 f"{old[2]:,} | {new[2]:,} |")

summary = [
    "## Boundaries from a parent extract, per US region",
    "",
    f"- Regions with their own admin_level 4 relation: **{own_before} of {len(ISOS)}** from the "
    f"per-state extracts (latest), **{own_after} of {len(ISOS)}** from the parent extract.",
    f"- Boundary rows, all US regions: {rows_before:,} (latest) -> {rows_after:,} (parent).",
    f"- Gained their own relation: {', '.join(gained) or 'none'}.",
    "",
    *lines,
]
report = "\n".join(summary)
print(report)
Path("report.md").write_text(report + "\n")
