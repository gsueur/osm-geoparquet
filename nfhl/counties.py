#!/usr/bin/env python3
"""
public/counties.json: US county outlines for the coverage map, keyed by FIPS.

Source: US Census Bureau cartographic boundary file, counties, 1:20,000,000
(cb_2024_us_county_20m), public domain. Connecticut comes from the 2021
vintage: since 2022 the Census uses its planning regions, while FEMA still
delivers the eight former counties (09001C...). A county-wide FEMA delivery is named
<FIPS>C (12086C), so the page joins it to the index in the browser. Outlines
are kept at 4 decimals (about 10 m): the map shows them at state and country
scale only. Run again only when the Census publishes a new vintage.

  uv run --project scripts python nfhl/counties.py
"""

import json
import tempfile
import urllib.request
from pathlib import Path

import duckdb

URL = "https://www2.census.gov/geo/tiger/GENZ{year}/shp/cb_{year}_us_county_20m.zip"
OUT = Path(__file__).resolve().parent / "public" / "counties.json"

with tempfile.TemporaryDirectory() as tmp:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    rows = []
    for year, where in ((2024, "STATEFP <> '09'"), (2021, "STATEFP = '09'")):
        z = Path(tmp) / f"counties_{year}.zip"
        urllib.request.urlretrieve(URL.format(year=year), z)
        rows += con.execute(f"""
            SELECT GEOID, NAME, STUSPS, ST_AsGeoJSON(ST_ReducePrecision(geom, 0.0001))
            FROM ST_Read('/vsizip/{z}/cb_{year}_us_county_20m.shp')
            WHERE {where}""").fetchall()
    rows.sort()

features = [{"type": "Feature", "id": int(g), "properties": {"fips": g, "name": n, "state": s},
             "geometry": json.loads(geom)} for g, n, s, geom in rows]
OUT.write_text(json.dumps({"type": "FeatureCollection", "features": features},
                          separators=(",", ":")))
print(f"{len(features):,} counties -> {OUT.name}, {OUT.stat().st_size / 1e6:.2f} MB")
