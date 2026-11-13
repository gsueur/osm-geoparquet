"""When each published file last changed, and which files a build must upload.

STAC `updated` means "the data changed", `checked` means "the last build that
looked". A build stamps its output directory before upload:

    filedates.py stamp --out-dir OUT --name census-acs --prefix acs/2024 \\
        --public https://parquetry.geomermaids.com/census/acs/2024 --since 2026-09-01

It writes OUT/_files/<name>.json, one entry per GeoParquet and PMTiles file
(path relative to the dataset root: --prefix, then the path under OUT):

    {"name": ..., "checked": T, "updated": max, "files": {
        "tiger/2026/legislative/cd120.parquet":
            {"sha256": ..., "bytes": ..., "updated": ..., "checked": T},
        "tiger/2026/legislative/cd120.pmtiles": {..., "follows": "...cd120.parquet"}}}

and OUT/_files/<name>.upload, the paths under OUT that rclone must copy
(--files-from). Upload the stamp itself last, after the data and manifests.

A partial build (one state, a tiles-only stage) keeps the published entries
of the files it did not produce: they are still live. A file deleted from the
bucket on purpose leaves the stamp with --complete, which drops every entry
the build did not produce. --retile uploads every PMTiles present (new tiling
settings) without moving `updated`: the data did not change.

The rules:
- A GeoParquet whose sha256 equals the published one keeps its published
  `updated`; a new or changed one gets the build time. Only GeoParquet counts.
- A PMTiles we produce is a rendering of its GeoParquet (same folder, same
  stem): it takes that file's `updated` and is uploaded only when that file
  changed (or when it is new on the bucket). Its own bytes are never compared:
  tiling is not byte-stable and does not need to be.
- A PMTiles with no GeoParquet beside it (a pyramid of several files, an H3
  rollup) follows the newest GeoParquet of its folder when there is one, or
  else is compared on its own sha256.
- First run, nothing published yet: an unchanged-looking file has no history,
  so it gets --since (the source's own date where we know it, otherwise the
  day tracking started, said so in the README). No invented dates.

The catalog builders read the stamp with `load()` and roll it up: an asset's
`updated` is its file's, a collection's the max of its data assets, a
catalog's the max of its collections.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

DATA = (".parquet",)

# For each catalog's README and AGENTS.md: what the two dates mean.
DATES_MD = """\
## Dates

`updated`, on every file, collection and catalog, is when the data last
changed: a rebuild that writes the same GeoParquet bytes leaves it alone. A
PMTiles archive takes the date of the GeoParquet it is drawn from. The first
value is the source's own release date where the publisher gives one.
`checked` is the last build that compared the data with its source."""
TILES = (".pmtiles",)


def now_rfc3339() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def as_rfc3339(day: str) -> str:
    """2026-09-01 or a full timestamp, as RFC 3339 UTC."""
    return day if "T" in day else f"{day}T00:00:00Z"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_json(url: str) -> dict | None:
    """The published stamp, or None when there is none yet (404). A query
    string past the CDN cache: the catalog step reads a stamp uploaded
    seconds before, and the previous one may still be cached for 5 min."""
    sep = "&" if "?" in url else "?"
    req = urllib.request.Request(f"{url}{sep}fresh={now_rfc3339()}", headers={"Cache-Control": "no-cache",
                                               "User-Agent": "parquetry-filedates"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def stamp(out: Path, name: str, published: dict | None, since: str, now: str, *,
          prefix: str = "", complete: bool = False, retile: bool = False,
          only: list[Path] | None = None, first: bool | None = None) -> dict:
    """The new stamp for every data and tile file under out, or only those
    in `only` (a build that publishes each layer as it is built: `published`
    is then the stamp so far, and `first` says whether the run started with
    no published stamp at all)."""
    before = (published or {}).get("files", {})
    first = published is None if first is None else first
    pre = prefix.strip("/") + "/" if prefix.strip("/") else ""
    files = sorted(p for p in (only if only is not None else out.rglob("*")) if p.is_file()
                   and p.suffix in DATA + TILES and not p.name.startswith("._"))
    entries: dict[str, dict] = {}
    upload: set[str] = set()

    def judge(rel: str, digest: str) -> str:
        old = before.get(rel)
        if old is None:
            # Not on the bucket yet: new data now, unless there is no stamp
            # at all (first run), where the file is very likely already live.
            return since if first else now
        if old["sha256"] == digest:
            return old.get("updated") or since
        return now

    for p in files:
        if p.suffix not in DATA:
            continue
        rel = pre + p.relative_to(out).as_posix()
        digest = sha256(p)
        updated = judge(rel, digest)
        entries[rel] = {"sha256": digest, "bytes": p.stat().st_size,
                        "updated": updated, "checked": now}
        if updated == now or rel not in before:
            upload.add(rel)

    if not complete:  # still live, not rebuilt this time
        entries = {**{k: v for k, v in before.items() if k.endswith(DATA)}, **entries}

    for p in files:
        if p.suffix not in TILES:
            continue
        rel = pre + p.relative_to(out).as_posix()
        twin = pre + p.with_suffix(".parquet").relative_to(out).as_posix()
        if twin not in entries:
            siblings = [r for r in entries if Path(r).parent == Path(rel).parent]
            twin = max(siblings, key=lambda r: entries[r]["updated"]) if siblings else None
        entry = {"bytes": p.stat().st_size, "checked": now}
        if twin:
            entry |= {"follows": twin, "updated": entries[twin]["updated"]}
            if twin in upload or rel not in before or retile:
                upload.add(rel)
                entry["sha256"] = sha256(p)
            else:
                entry["sha256"] = before[rel].get("sha256")  # what is live
                entry["bytes"] = before[rel].get("bytes", entry["bytes"])
        else:
            digest = sha256(p)
            entry |= {"sha256": digest, "updated": judge(rel, digest)}
            if entry["updated"] == now or rel not in before or retile:
                upload.add(rel)
        entries[rel] = entry

    if not complete:
        entries = {**before, **entries}

    return {
        "name": name,
        "checked": now,
        "updated": max((e["updated"] for e in entries.values()), default=since),
        "since": (published or {}).get("since", since),
        "files": entries,
        "upload": sorted(u[len(pre):] for u in upload),
    }


def stamp_digests(files: dict[str, dict], name: str, published: dict | None, since: str,
                  now: str, *, complete: bool = False) -> dict:
    """stamp() for files no single machine holds, from the sha256 and size
    the build's manifests record (the wetlands release: written by many jobs
    and copied server-side, 70 GB). Same rules and output, `upload` included.

    files maps each path (relative to the dataset root) to {"sha256",
    "bytes"}, and optionally "built", that file's build time (default now:
    a re-run then gives the same dates), or "follows", the GeoParquet path
    a PMTiles is drawn from (it takes that file's dates)."""
    before = (published or {}).get("files", {})
    entries: dict[str, dict] = {}
    upload: set[str] = set()
    for rel, f in sorted(files.items()):
        if f.get("follows"):
            continue
        t = f.get("built") or now
        old = before.get(rel)
        if old is None:
            updated = since if published is None else t
        else:
            updated = (old.get("updated") or since) if old["sha256"] == f["sha256"] else t
        entries[rel] = {"sha256": f["sha256"], "bytes": f["bytes"], "updated": updated,
                        "checked": t}
        if updated == t or old is None:
            upload.add(rel)
    if not complete:
        entries = {**{k: v for k, v in before.items() if k.endswith(DATA)}, **entries}
    for rel, f in sorted(files.items()):
        twin = f.get("follows")
        if not twin:
            continue
        if twin not in entries:
            sys.exit(f"{rel} follows {twin}, which is not stamped")
        entries[rel] = {"sha256": f["sha256"], "bytes": f["bytes"], "follows": twin,
                        "updated": entries[twin]["updated"], "checked": entries[twin]["checked"]}
        if twin in upload or rel not in before:
            upload.add(rel)
    if not complete:
        entries = {**before, **entries}
    return {
        "name": name,
        "checked": now,
        "updated": max((e["updated"] for e in entries.values()), default=since),
        "since": (published or {}).get("since", since),
        "files": entries,
        "upload": sorted(upload),
    }


def walk(url: str):
    """Every collection of a published catalog tree, by its child links."""
    doc = fetch_json(url)
    if doc is None:
        sys.exit(f"no catalog at {url}")
    if doc.get("type") == "Collection":
        yield doc
    for link in doc.get("links", []):
        if link.get("rel") == "child":
            yield from walk(urllib.parse.urljoin(url, link["href"]))


def since_for(rel: str, since: dict[str, str]) -> str:
    """The first-run date of a path: the longest matching folder prefix in
    `since`, or its "" default."""
    best = max((k for k in since if k == "" or rel.startswith(k.rstrip("/") + "/")), key=len)
    return since[best]


def seed(catalog_url: str, base_url: str, name: str, prefix: str, since: dict[str, str],
         now: str) -> dict:
    """A first stamp from what is published, for a dataset that has none:
    every asset under base_url/prefix with a `file:checksum` is a live file
    with that checksum. Nothing is downloaded or hashed. `updated` is the
    source's own date where we have it (per folder: a state's own release),
    else the day tracking started."""
    base = base_url.rstrip("/") + "/"
    pre = prefix.strip("/") + "/" if prefix.strip("/") else ""
    files = {}
    for col in walk(catalog_url):
        for asset in col.get("assets", {}).values():
            href, checksum = asset.get("href", ""), asset.get("file:checksum", "")
            rel = href[len(base):] if href.startswith(base) else None
            if (rel and rel.startswith(pre) and rel.endswith(DATA + TILES)
                    and checksum.startswith("1220")):
                files[rel] = {"sha256": checksum[4:], "bytes": asset.get("file:size"),
                              "updated": since_for(rel, since), "checked": now}
    for rel in files:
        if rel.endswith(TILES) and rel[:-len(".pmtiles")] + ".parquet" in files:
            files[rel]["follows"] = rel[:-len(".pmtiles")] + ".parquet"
    for rel, e in files.items():  # a rendering takes its data's date
        if e.get("follows"):
            e["updated"] = files[e["follows"]]["updated"]
    return {"name": name, "checked": now,
            "updated": max((e["updated"] for e in files.values()), default=since[""]),
            "since": min(since.values()),
            "seeded_from": catalog_url, "files": dict(sorted(files.items()))}


def load(base_url: str, *stamps: Path | str) -> "Dates":
    """The stamps a catalog builder reads (local paths or URLs). base_url is
    the dataset root the stamp paths are relative to, e.g.
    https://parquetry.geomermaids.com/census."""
    merged: dict[str, dict] = {}
    meta = []
    for s in stamps:
        doc = fetch_json(str(s)) if str(s).startswith("http") else (
            json.loads(Path(s).read_text()) if Path(s).is_file() else None)
        if doc is None:
            print(f"  no file stamp at {s}: its assets keep the build time", file=sys.stderr)
            continue
        merged |= doc["files"]
        meta.append(doc)
    return Dates(base_url.rstrip("/"), merged, meta)


class Dates:
    """`updated`, `checked` and the live size and checksum of each stamped
    file, looked up by its public href."""

    def __init__(self, base_url: str, files: dict[str, dict], meta: list[dict]):
        self.base, self.files, self.meta = base_url, files, meta

    def get(self, href: str) -> dict | None:
        if not href.startswith(self.base + "/"):
            return None
        return self.files.get(href[len(self.base) + 1:])

    @property
    def since(self) -> str | None:
        return min((m.get("since", m["checked"]) for m in self.meta), default=None)


def apply(tree: Path, dates: Dates) -> None:
    """Stamp a rendered catalog tree in place, before rashid and the upload.

    Each asset whose href is a stamped file gets that file's `updated` and
    `checked`, and its live `file:size` and `file:checksum` (a PMTiles that
    was not re-uploaded keeps the published bytes). A collection's `updated`
    is the newest of its data assets, `checked` the newest check; a catalog's
    the newest of its children. Thumbnails, styles and text never count.
    A collection with no stamped asset keeps what its builder wrote."""
    docs: dict[Path, dict] = {}
    for path in sorted(tree.rglob("*.json")):
        if path.name in ("collection.json", "catalog.json"):
            docs[path.resolve()] = json.loads(path.read_text())

    for path, doc in docs.items():
        if doc.get("type") != "Collection":
            continue
        seen = []
        for asset in doc.get("assets", {}).values():
            entry = dates.get(asset.get("href", ""))
            if entry is None:
                continue
            asset["updated"], asset["checked"] = entry["updated"], entry["checked"]
            if entry.get("bytes"):
                asset["file:size"] = entry["bytes"]
            if entry.get("sha256") and "file:checksum" in asset:
                asset["file:checksum"] = "1220" + entry["sha256"]
            if "data" in asset.get("roles", []):
                seen.append(entry)
        if seen:
            doc["updated"] = max(e["updated"] for e in seen)
            doc["checked"] = max(e["checked"] for e in seen)

    # Catalogs, deepest first, from their child links.
    for path in sorted((p for p, d in docs.items() if d.get("type") == "Catalog"),
                       key=lambda p: -len(p.parts)):
        doc = docs[path]
        kids = [docs.get((path.parent / l["href"]).resolve()) for l in doc.get("links", [])
                if l.get("rel") == "child" and not l["href"].startswith("http")]
        kids = [k for k in kids if k and "checked" in k]
        if kids:
            doc["updated"], doc["checked"] = rollup(kids)

    for path, doc in docs.items():
        path.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")


def rollup(docs: list[dict]) -> tuple[str, str]:
    """(updated, checked) of a parent: the newest of its children."""
    return (max(d["updated"] for d in docs), max(d.get("checked") or d["updated"] for d in docs))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("stamp", help="stamp OUT and list the files to upload")
    s.add_argument("--out-dir", type=Path, required=True)
    s.add_argument("--name", required=True, help="this build's stamp name, e.g. census-tiger")
    s.add_argument("--public", required=True, help="public URL of OUT on parquetry")
    s.add_argument("--since", required=True,
                   help="first-run date for files with no history (YYYY-MM-DD)")
    s.add_argument("--prefix", default="",
                   help="OUT's folder under the dataset root, e.g. acs/2024")
    s.add_argument("--complete", action="store_true",
                   help="OUT is the whole dataset: drop entries it did not produce")
    s.add_argument("--retile", action="store_true",
                   help="upload every PMTiles present, without moving updated")
    s.add_argument("--now", help="the build time (default: now)")
    d = sub.add_parser("seed", help="a first stamp from the published catalog, nothing hashed")
    d.add_argument("--catalog", required=True, help="the published catalog.json URL")
    d.add_argument("--public", required=True, help="the dataset root URL the paths are under")
    d.add_argument("--name", required=True)
    d.add_argument("--prefix", default="", help="only files under this folder of the root")
    d.add_argument("--since", action="append", required=True, metavar="[FOLDER=]DATE",
                   help="the source's date, or the day tracking starts; FOLDER=DATE for one "
                        "folder of the root (repeatable, longest folder wins); one bare DATE "
                        "is the default")
    d.add_argument("--dest", type=Path, required=True, help="where to write <name>.json")
    args = p.parse_args()

    if args.cmd == "seed":
        since = {}
        for item in args.since:
            folder, _, day = item.rpartition("=")
            since[folder] = as_rfc3339(day)
        if "" not in since:
            sys.exit("--since needs one bare DATE as the default")
        doc = seed(args.catalog, args.public, args.name, args.prefix, since, now_rfc3339())
        args.dest.mkdir(parents=True, exist_ok=True)
        (args.dest / f"{args.name}.json").write_text(json.dumps(doc, indent=1) + "\n")
        print(f"{args.name}: seeded {len(doc['files'])} files, updated {doc['updated']}")
        return

    now = args.now or now_rfc3339()
    published = fetch_json(f"{args.public.rstrip('/')}/_files/{args.name}.json")
    doc = stamp(args.out_dir, args.name, published, as_rfc3339(args.since), now,
                prefix=args.prefix, complete=args.complete, retile=args.retile)
    upload = doc.pop("upload")
    folder = args.out_dir / "_files"
    folder.mkdir(exist_ok=True)
    (folder / f"{args.name}.json").write_text(json.dumps(doc, indent=1) + "\n")
    (folder / f"{args.name}.upload").write_text("".join(f"{u}\n" for u in upload))
    n = len(doc["files"])
    print(f"{args.name}: {n} files, {len(upload)} to upload, updated {doc['updated']}, "
          f"{'first stamp' if published is None else 'compared with the published stamp'}")


if __name__ == "__main__":
    main()
