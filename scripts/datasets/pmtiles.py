#!/usr/bin/env python3
"""
PMTiles for the static datasets, with tylertoo.

A Portolan vector collection pairs its GeoParquet with a PMTiles archive, the
visualization derivative a browser draws without a server (formats.md,
PMTiles). This module builds that archive from one GeoParquet file and
measures it for the catalog; each dataset says which layer, which zooms and
which properties.

tylertoo (https://github.com/geoparquet-io/tylertoo) is a Rust binary. Its
PyPI package exposes neither the density options nor the property filters,
so the CLI is used, pinned to one release and checked against the sha256
values below, recorded from that release's checksums file. `install`
downloads it; `tile` finds it on PATH or in $TYLERTOO.

Two defaults do not suit administrative units and are always turned off
here: the per-level density budget and polygon thinning both drop polygons
from a layer that must tile the land without holes. A tile over the size cap
also drops features, with only a log line to say so: tile() reads the report
and fails instead, so a layer that does not fit is fixed (a later minimum
zoom, fewer properties) rather than published with gaps.

A layer too dense for its coarse zooms (CLC: 2.4 million polygons that tile
the land) is built in bands, one archive per zoom range from its own input
(aggregates on H3 cells for the coarse zooms), joined by pyramid().

Usage:
  python3 scripts/datasets/pmtiles.py install --dest ~/.local/bin
  python3 scripts/datasets/pmtiles.py tile in.parquet out.pmtiles --layer l1 --max-zoom 10
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

TYLERTOO_VERSION = "0.8.0"
RELEASES = "https://github.com/geoparquet-io/tylertoo/releases/download"
SHA256 = {
    "aarch64-apple-darwin": "987c7d1dd39e79b8569101c1b01b4af98c95680f8c51b1e1d9673f3c3bb04945",
    "x86_64-apple-darwin": "b6fa9eeeb923cdea60b80cd86a42d35a54f43d1cb1a7e3072d202bb98e70778c",
    "x86_64-unknown-linux-gnu": "d70592996bf59571842ae29e81780e6186b2f89b36e81dfaae1de7949fa52da6",
}
PMTILES_TYPE = "application/vnd.pmtiles"


def target() -> str:
    system, machine = platform.system(), platform.machine().lower()
    if system == "Darwin":
        return "aarch64-apple-darwin" if machine in ("arm64", "aarch64") else "x86_64-apple-darwin"
    if system == "Linux" and machine in ("x86_64", "amd64"):
        return "x86_64-unknown-linux-gnu"
    sys.exit(f"no pinned tylertoo build for {system} {machine}")


def install(dest: Path) -> Path:
    """Download the pinned release for this platform, check it, unpack the binary."""
    t = target()
    name = f"tylertoo-{TYLERTOO_VERSION}-{t}"
    with urllib.request.urlopen(f"{RELEASES}/v{TYLERTOO_VERSION}/{name}.tar.gz") as r:
        blob = r.read()
    got = hashlib.sha256(blob).hexdigest()
    if got != SHA256[t]:
        sys.exit(f"{name}.tar.gz: sha256 {got}, expected {SHA256[t]}")
    dest.mkdir(parents=True, exist_ok=True)
    out = dest / "tylertoo"
    with tarfile.open(fileobj=io.BytesIO(blob)) as tar:
        member = tar.extractfile(f"{name}/tylertoo")
        if member is None:
            sys.exit(f"{name}.tar.gz has no tylertoo binary")
        out.write_bytes(member.read())
    out.chmod(0o755)
    return out


def binary() -> str:
    path = os.environ.get("TYLERTOO") or shutil.which("tylertoo")
    if not path:
        sys.exit("tylertoo not found: run `pmtiles.py install --dest <dir on PATH>` "
                 "or set TYLERTOO")
    version = subprocess.run([path, "--version"], capture_output=True, text=True).stdout
    if TYLERTOO_VERSION not in version:
        sys.exit(f"{path} reports {version.strip()!r}, this script pins {TYLERTOO_VERSION}")
    return path


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def tile(src: Path, dest: Path, *, layer: str, min_zoom: int = 0, max_zoom: int = 10,
         properties: list[str] | None = None, verbatim: bool = False) -> dict:
    """Tile src into dest; fail if any tile lost features. Returns the
    measurements the catalog needs (size, sha256, zooms, layer). verbatim
    turns simplification off too, for shapes that must keep theirs (H3
    cells)."""
    with tempfile.TemporaryDirectory(prefix="tiles-") as tmp:
        report_path = Path(tmp) / "report.json"
        cmd = [binary(), "tiles", str(src), str(dest),
               "--layer-name", layer, "--min-zoom", str(min_zoom), "--max-zoom", str(max_zoom),
               "--no-density-drop", "--polygon-thinning", "0",
               "--report", str(report_path), "--force"]
        if verbatim:
            cmd.append("--verbatim")
        for p in properties or []:
            cmd += ["--include-property", p]
        subprocess.run(cmd, check=True)
        report = json.loads(report_path.read_text())
    zooms = report["export"]["zooms"]
    lossy = [(z["zoom"], z["oversized_tiles"], z["encode_dropped_features"])
             for z in zooms if z["oversized_tiles"] or z["encode_dropped_features"]]
    if lossy:
        sys.exit(f"{dest.name}: tiles over the size cap dropped features "
                 f"(zoom, oversized tiles, dropped): {lossy}. Raise min_zoom or keep "
                 f"fewer properties.")
    return {
        "file": dest.name,
        "layer": layer,
        "min_zoom": min_zoom,
        "max_zoom": max_zoom,
        "tiles": sum(z["tile_count"] for z in zooms),
        "properties": properties,
        "bytes": dest.stat().st_size,
        "sha256": sha256(dest),
        "tylertoo": TYLERTOO_VERSION,
    }


def pyramid(bands: list[tuple[int, int, Path, str]], dest: Path) -> dict:
    """Join archives that each hold one zoom range (built by tile(), so each
    already passed the size check) into one archive. Each band is
    (min zoom, max zoom, archive, tile layer); bands may use different
    layers, as a handover from aggregates to features does."""
    cmd = [binary(), "pyramid", str(dest), "--force"]
    for lo, hi, path, layer in bands:
        cmd += ["--band", f"{lo}-{hi}:{path}:{layer}"]
    subprocess.run(cmd, check=True)
    stats = json.loads(subprocess.run([binary(), "stats", "--json", str(dest)], check=True,
                                      capture_output=True, text=True).stdout)
    return {
        "file": dest.name,
        "min_zoom": min(b[0] for b in bands),
        "max_zoom": max(b[1] for b in bands),
        "tiles": sum(z["tile_count"] for z in stats["per_zoom"]),
        "bytes": dest.stat().st_size,
        "sha256": sha256(dest),
        "tylertoo": TYLERTOO_VERSION,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    i = sub.add_parser("install", help="download the pinned tylertoo binary")
    i.add_argument("--dest", type=Path, required=True)
    t = sub.add_parser("tile", help="tile one GeoParquet file")
    t.add_argument("src", type=Path)
    t.add_argument("dest", type=Path)
    t.add_argument("--layer", required=True)
    t.add_argument("--min-zoom", type=int, default=0)
    t.add_argument("--max-zoom", type=int, default=10)
    t.add_argument("--property", action="append", dest="properties")
    args = p.parse_args()
    if args.cmd == "install":
        print(install(args.dest))
    else:
        print(json.dumps(tile(args.src, args.dest, layer=args.layer, min_zoom=args.min_zoom,
                              max_zoom=args.max_zoom, properties=args.properties), indent=2))


if __name__ == "__main__":
    main()
