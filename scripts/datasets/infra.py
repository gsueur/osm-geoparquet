#!/usr/bin/env python3
"""
OpenStreetMap infrastructure as partitioned GeoParquet: power, telecoms, oil
and gas, water. The content of Open Infrastructure Map (openinframap.org),
built from OSM extracts rather than a live PostGIS.

Parity is with openinframap/openinframap as of 2026-09: every table of its
imposm mapping (imposm/*.py) is a layer here but one, every attribute its tile layers
serve (tegola/layers.yml) is a column, and its derived values (schema/
functions.sql, views.sql: voltages in kV, outputs in MW, solar estimates,
pipeline categories, site relations merged from their members, circuit
lengths) are computed the same way. Their code is BSD-licensed; the rules are
credited in ATTRIBUTION.txt. Where they drop data we keep it: every geometry
type of a layer (not only the one their tiles draw), all name:* variants, the
full tag map, and a power_other layer for power=* values they do not map.

The exception is their water_reservoir table (water=reservoir,
man_made=reservoir_covered), left out on purpose: water=reservoir is mostly
ponds and lakes, not infrastructure (61,933 of them in Florida alone, next to
14 covered reservoirs), and it dwarfed every other water layer.

Two stages, so the worldwide build can fan out over Geofabrik regions:

  filter  one extract -> the infrastructure subset (about 0.8% of the input)
  build   one (merged) subset -> <out>/<layer>.parquet, one file per layer
          for the whole world, <out>/_manifest.json, <out>/stats/, and
          index.json in <out>'s parent (the repository base; <out> is
          `latest`, the only version published: no snapshots.json)

Each layer is one file, sorted along a Hilbert curve over its extent, in row
groups of 32,000: a bbox filter reads only the groups it touches (a box
around Bayern, 27 of the towers' 1,245), and a filter on country and state
skips most of them (Texas: 153 read, 31 hold it; a group on a border spans
a wide range of state names). It replaced one file per country
and state: ~41,000 files, 15,000 of them under 10 KB, and a glob over them
spent minutes on footers before reading a row. _regions.json lists every
country and state slug with its names and GAUL code. index.json and _manifest.json
make it a parquetry repository with one dataset, the whole world, as GeoPQ
Workbench's browser reads it.

US completeness (--eia): the operating plants the EIA-860M inventory reports
and OpenStreetMap lacks are added to power_plant as points, origin = 'eia'
(97.8% of US capacity is already in OSM; what is missing is mostly recent
and small solar). _coverage.json says how each EIA plant was found.

Regions: every row carries the country and state holding a point on it
(ST_PointOnSurface). On land the state is its FAO GAUL 2024 L1 unit, as a
slug of the unit's name (`texas`; GAUL has no ISO 3166-2 codes, and matching
names to them fails for a third of the units). Offshore it is the country of
Marine Regions' union of land and EEZ, with state `_offshore`; elsewhere
country and state `_intl` (high seas). Countries are ISO 3166-1 alpha-2 like
the OSM dataset; GAUL's non-ISO codes for disputed areas (xJK, xAB, ...) are
kept as they are.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

OSMIUM = "osmium"

# --------------------------------------------------------------------------
# Stage 1: what to keep from an extract.
#
# The union of every key the layers below read. tags-filter also keeps what a
# match references (nodes of ways, members of relations), so circuit and site
# relations arrive with their members.
FILTER = [
    # power
    "nwr/power", "nwr/construction:power", "nwr/disused:power",
    "nwr/abandoned:power", "r/route=power",
    # telecoms
    "nwr/communication=line,cable", "nwr/construction:communication=line,cable",
    "nwr/telecom", "nwr/building=data_center,data_centre,telephone_exchange",
    "nwr/office=telecommunication",
    "nwr/man_made=mast,tower,communications_tower,antenna,utility_pole,"
    "street_cabinet,telephone_office",
    "nwr/tower:type=communication",
    # oil and gas
    "nwr/man_made=pipeline,petroleum_well,oil_well,offshore_platform",
    "nwr/construction:man_made=pipeline", "nwr/pipeline", "nwr/marker",
    "nwr/industrial=oil,fracking,oil_storage,petroleum_terminal,hydrocarbons,"
    "oil_sands,gas,gas_storage,natural_gas,wellsite,well_cluster,refinery",
    # water
    "nwr/man_made=water_works,desalination_plant,wastewater_plant,"
    "pumping_station,water_tower,water_well",
    "w/waterway=pressurised",
]


def run(cmd: list[str]) -> None:
    print("  $", " ".join(cmd[:4]), "..." if len(cmd) > 4 else "", flush=True)
    subprocess.run(cmd, check=True)


def filter_extract(src: Path, dest: Path) -> None:
    run([OSMIUM, "tags-filter", str(src), *FILTER, "-o", str(dest), "--overwrite"])


# --------------------------------------------------------------------------
# SQL helpers, registered as DuckDB macros.

MACROS = r"""
-- A tag's value regardless of lifecycle: power=line, construction:power=line,
-- power=construction + construction=line, disused:power=line all give 'line'.
CREATE OR REPLACE MACRO lc_val(t, k) AS coalesce(
    CASE WHEN t[k] IN ('construction', 'proposed', 'disused', 'abandoned')
         THEN t[t[k]] ELSE t[k] END,
    t['construction:' || k], t['proposed:' || k],
    t['disused:' || k], t['abandoned:' || k]);

CREATE OR REPLACE MACRO lifecycle(t, k) AS CASE
    WHEN t[k] = 'construction' OR t['construction:' || k] IS NOT NULL THEN 'construction'
    WHEN t[k] = 'proposed' OR t['proposed:' || k] IS NOT NULL THEN 'proposed'
    WHEN t[k] = 'disused' OR t['disused:' || k] IS NOT NULL
         OR t['disused'] = 'yes' THEN 'disused'
    WHEN t[k] = 'abandoned' OR t['abandoned:' || k] IS NOT NULL
         OR t['abandoned'] = 'yes' THEN 'abandoned'
    ELSE 'active' END;

CREATE OR REPLACE MACRO first_semi(v) AS nullif(trim(split_part(v, ';', 1)), '');

-- OIM convert_number: leading number, comma or dot decimal, trailing text
-- ignored; anything else NULL.
CREATE OR REPLACE MACRO to_number(v) AS TRY_CAST(replace(
    nullif(regexp_extract(v, '^\s*([0-9]+[.,]?[0-9]*)', 1), ''), ',', '.') AS DOUBLE);

-- OIM convert_integer: the whole value is 1-9 digits.
CREATE OR REPLACE MACRO to_int(v) AS
    CASE WHEN regexp_full_match(trim(v), '[0-9]{1,9}') THEN trim(v)::INTEGER END;

-- Voltages of a semicolon list, in kV, entries that are not integers dropped.
CREATE OR REPLACE MACRO voltages_kv(v) AS nullif(list_filter(
    list_transform(string_split(v, ';'), x -> to_int(x) / 1000.0),
    x -> x IS NOT NULL), []);

-- OIM line_voltages: one entry per circuit, highest first. A single voltage
-- on a multi-circuit line is repeated `circuits` times.
CREATE OR REPLACE MACRO line_voltages_kv(v, circuits) AS list_reverse(list_sort(
    CASE WHEN len(string_split(v, ';')) > 1 THEN voltages_kv(v)
         WHEN circuits IS NOT NULL AND circuits BETWEEN 1 AND 64
              AND to_int(v) IS NOT NULL
         THEN list_transform(range(circuits), i -> to_int(v) / 1000.0)
         ELSE voltages_kv(v) END));

-- OIM convert_power: a number with a W, kW, MW or GW unit, in MW. No unit is
-- invalid (NULL), as is `yes`.
CREATE OR REPLACE MACRO power_mw(v) AS
    CASE regexp_extract(upper(v), '([0-9]+[.,]?[0-9]*) ?([KMG]?W)', 2)
        WHEN 'W' THEN 1e-6 WHEN 'KW' THEN 1e-3 WHEN 'MW' THEN 1 WHEN 'GW' THEN 1e3 END
    * TRY_CAST(replace(regexp_extract(upper(v), '([0-9]+[.,]?[0-9]*) ?([KMG]?W)', 1),
                       ',', '.') AS DOUBLE);

-- OIM pipeline_type.
CREATE OR REPLACE MACRO pipeline_category(s) AS CASE
    WHEN s IN ('natural_gas', 'gas', 'oil', 'fuel', 'cng', 'lpg', 'ngl', 'lng',
        'y-grade', 'hydrocarbons', 'hydrogen', 'ethylene', 'ethene', 'propylene',
        'propene', 'methane', 'ethane', 'isobutane', 'butane', 'propane',
        'condensate', 'butadiene', 'naphtha') THEN 'petroleum'
    WHEN s IN ('water', 'hot_water', 'rainwater', 'wastewater', 'sewage',
        'waterwaste', 'steam') THEN 'water'
    ELSE 'other' END;

CREATE OR REPLACE MACRO names_of(t) AS map_from_entries(list_transform(
    list_filter(map_entries(t), e -> starts_with(e.key, 'name:')),
    e -> {'key': substr(e.key, 6), 'value': e.value}));

-- Geodesic measures. DuckDB's spheroid functions read (lat, lon).
CREATE OR REPLACE MACRO length_km(g) AS
    round(ST_Length_Spheroid(ST_FlipCoordinates(g)) / 1000, 3);
CREATE OR REPLACE MACRO area_m2(g) AS CASE
    WHEN ST_Dimension(g) = 2 THEN round(ST_Area_Spheroid(ST_FlipCoordinates(g))) END;
"""

# The pipeline's closed-way rule (themes.LINEAR_WHEN_CLOSED): osmium exports a
# closed way as a line and as an area; keep the line only when this holds.
LINEAR = """(lc_val(tags, 'power') IN ('line', 'minor_line', 'cable')
    OR lc_val(tags, 'communication') IN ('line', 'cable')
    OR lc_val(tags, 'man_made') = 'pipeline'
    OR tags['waterway'] = 'pressurised')"""


# --------------------------------------------------------------------------
# Layers. `where` selects rows from `feat` (every exported element, one
# geometry each); `columns` are (name, SQL) after the common ones.

@dataclass(frozen=True)
class Layer:
    name: str
    group: str
    key: str                   # the tag whose lifecycle prefixes apply
    where: str
    columns: list[tuple[str, str]] = field(default_factory=list)
    source: str = "feat"       # or a relation-derived table


def s(key: str) -> str:
    return f"tags['{key}']"


TRANSFORMER = [
    ("voltage_primary_kv", "list_max(voltages_kv(tags['voltage:primary']))"),
    ("voltage_secondary_kv", "list_max(voltages_kv(tags['voltage:secondary']))"),
    ("voltage_tertiary_kv", "list_max(voltages_kv(tags['voltage:tertiary']))"),
    ("rating", s("rating")), ("windings", s("windings")), ("phases", s("phases")),
    ("windings_configuration", s("windings:configuration")),
    ("transformer_type", s("transformer")), ("transformer_devices", s("transformer:devices")),
]

POWER_TOWER_TYPES = "('tower', 'pole', 'portal')"
POWER_SWITCHGEAR_TYPES = "('switch', 'transformer', 'compensator', 'insulator', 'terminal', 'converter')"
POWER_MAPPED = (f"('line', 'minor_line', 'cable', 'circuit', 'substation', 'plant', "
                f"'generator', 'tower', 'pole', 'portal', 'switch', 'transformer', "
                f"'compensator', 'insulator', 'terminal', 'converter', 'marker')")

LAYERS: list[Layer] = [
    # ---- power
    Layer("power_line", "power", "power",
          "lc_val(tags, 'power') IN ('line', 'minor_line', 'cable')", [
              ("line", s("line")),
              ("circuits", "to_int(tags['circuits'])"),
              ("voltages_kv", "line_voltages_kv(tags['voltage'], to_int(tags['circuits']))"),
              ("voltage_kv", "list_max(voltages_kv(tags['voltage']))"),
              ("cables", "to_int(tags['cables'])"),
              ("frequency_hz", "to_number(first_semi(tags['frequency']))"),
              ("location", s("location")),
              ("tunnel", s("tunnel")),
              ("material", s("material")),
              ("length_km", "length_km(geometry)"),
          ]),
    Layer("power_circuit", "power", "power", "TRUE", [
        ("voltages_kv", "voltages_kv(tags['voltage'])"),
        ("voltage_kv", "list_max(voltages_kv(tags['voltage']))"),
        ("frequency_hz", "to_number(first_semi(tags['frequency']))"),
        ("circuit_kind", "tags['type']"),
        ("section_count", "section_count"),
        ("length_km", "length_km(geometry)"),
        ("substation_members", "substation_members"),
    ], source="circuit"),
    Layer("power_tower", "power", "power",
          f"lc_val(tags, 'power') IN {POWER_TOWER_TYPES}", [
              ("transition", "tags['location:transition'] = 'yes'"),
              ("design", s("design")), ("design_ref", s("design:ref")),
              ("line_attachment", s("line_attachment")),
              ("line_management", s("line_management")),
              ("line_arrangement", s("line_arrangement")),
              ("material", s("material")),
              ("height_m", "to_number(tags['height'])"),
              ("switch", s("switch")), ("substation", s("substation")),
              *TRANSFORMER,
          ]),
    Layer("power_substation", "power", "power",
          "lc_val(tags, 'power') = 'substation'", [
              ("substation", s("substation")),
              ("voltages_kv", "voltages_kv(tags['voltage'])"),
              ("voltage_kv", "list_max(voltages_kv(tags['voltage']))"),
              ("frequency_hz", "to_number(first_semi(tags['frequency']))"),
              ("location", s("location")),
              ("rating", s("rating")),
              ("circuit_ids", "circuit_ids"),
              ("area_m2", "area_m2(geometry)"),
          ], source="substation"),
    Layer("power_plant", "power", "power",
          "lc_val(tags, 'power') = 'plant'", [
              ("source", "first_semi(tags['plant:source'])"),
              ("sources", "tags['plant:source']"),
              ("method", s("plant:method")),
              ("storage", s("plant:storage")),
              ("output_mw", "power_mw(tags['plant:output:electricity'])"),
              ("output_mw_estimated", "output_mw_estimated"),
              ("output_basis", "output_basis"),
              ("repd_id", s("repd:id")),
              ("eia_plant_id", "TRY_CAST(first_semi(tags['ref:US:EIA']) AS INTEGER)"),
              ("location", s("location")),
              ("generator_count", "generator_count"),
              ("generator_output_mw", "generator_output_mw"),
              ("area_m2", "area_m2(geometry)"),
          ], source="plant"),
    Layer("power_generator", "power", "power",
          "lc_val(tags, 'power') = 'generator'", [
              ("source", "first_semi(tags['generator:source'])"),
              ("method", s("generator:method")),
              ("generator_type", s("generator:type")),
              ("output_mw", "power_mw(tags['generator:output:electricity'])"),
              ("output_mw_estimated", "output_mw_estimated"),
              ("output_basis", "output_basis"),
              ("plant_role", s("generator:plant")),
              ("frequency_hz", "to_number(first_semi(tags['frequency']))"),
              ("area_m2", "area_m2(geometry)"),
          ], source="generator"),
    Layer("power_switchgear", "power", "power",
          f"lc_val(tags, 'power') IN {POWER_SWITCHGEAR_TYPES}", [
              ("voltages_kv", "voltages_kv(tags['voltage'])"),
              ("voltage_kv", "list_max(voltages_kv(tags['voltage']))"),
              ("switch_type", s("switch")),
              ("gas_insulated", s("gas_insulated")),
              ("cables", "to_int(tags['cables'])"),
              ("compensator_type", s("compensator")),
              ("location", s("location")),
              *TRANSFORMER,
          ]),
    # Beyond OIM: every other power=* value (busbars are lines, so not here).
    Layer("power_other", "power", "power",
          f"lc_val(tags, 'power') NOT IN {POWER_MAPPED}", [
              ("voltages_kv", "voltages_kv(tags['voltage'])"),
              ("voltage_kv", "list_max(voltages_kv(tags['voltage']))"),
              ("location", s("location")),
          ]),
    # ---- telecoms
    Layer("telecom_cable", "telecoms", "communication",
          "lc_val(tags, 'communication') IN ('line', 'cable')", [
              ("location", s("location")),
              ("length_km", "length_km(geometry)"),
          ]),
    Layer("telecom_building", "telecoms", "telecom",
          """tags['building'] IN ('data_center', 'data_centre', 'telephone_exchange')
             OR tags['telecom'] IN ('data_center', 'data_centre', 'central_office', 'exchange')
             OR tags['office'] = 'telecommunication'
             OR tags['man_made'] = 'telephone_office'""", [
              ("telecom", s("telecom")),
              ("area_m2", "area_m2(geometry)"),
          ]),
    Layer("telecom_location", "telecoms", "telecom",
          "tags['telecom'] IN ('connection_point', 'distribution_point')"),
    Layer("telecom_mast", "telecoms", "man_made",
          """lc_val(tags, 'man_made') IN ('mast', 'communications_tower')
             OR tags['tower:type'] IN ('communication', 'radio', 'antenna')""", [
              ("mast_type", s("mast:type")),
              ("tower_type", s("tower:type")),
              ("height_m", "to_number(tags['height'])"),
              ("material", s("material")),
              ("services", "nullif(list_sort(list_transform(list_filter(map_entries(tags), "
                           "e -> starts_with(e.key, 'communication:') AND e.value NOT IN ('no')), "
                           "e -> substr(e.key, 15))), [])"),
          ]),
    Layer("telecom_antenna", "telecoms", "man_made",
          "lc_val(tags, 'man_made') = 'antenna'", [
              ("height_m", "to_number(tags['height'])"),
          ]),
    Layer("utility_pole", "telecoms", "man_made",
          "tags['man_made'] = 'utility_pole'", [("utility", s("utility"))]),
    Layer("street_cabinet", "telecoms", "man_made",
          "tags['man_made'] = 'street_cabinet'", [
              ("utility", "first_semi(tags['utility'])"),
              ("utilities", s("utility")),
          ]),
    # ---- oil and gas (pipelines of every substance, as OIM's osm_pipeline)
    Layer("pipeline", "petroleum", "man_made",
          "lc_val(tags, 'man_made') = 'pipeline'", [
              ("substance", s("substance")),
              ("category", "pipeline_category(coalesce(tags['substance'], tags['type']))"),
              ("pipeline_type", s("type")),
              ("usage", s("usage")),
              ("diameter", s("diameter")),
              ("pressure", s("pressure")),
              ("material", s("material")),
              ("location", s("location")),
              ("length_km", "length_km(geometry)"),
          ]),
    Layer("petroleum_site", "petroleum", "industrial",
          """tags['industrial'] IN ('oil', 'fracking', 'oil_storage', 'petroleum_terminal',
                 'hydrocarbons', 'oil sands', 'oil_sands', 'gas', 'gas_storage',
                 'natural_gas', 'wellsite', 'well_cluster', 'refinery')
             OR tags['pipeline'] = 'substation'""", [
              ("industrial", s("industrial")),
              ("area_m2", "area_m2(geometry)"),
          ]),
    Layer("petroleum_well", "petroleum", "man_made",
          "lc_val(tags, 'man_made') IN ('petroleum_well', 'oil_well')", [
              ("substance", s("substance")),
          ]),
    Layer("offshore_platform", "petroleum", "man_made",
          "lc_val(tags, 'man_made') = 'offshore_platform'", [
              ("area_m2", "area_m2(geometry)"),
          ]),
    Layer("pipeline_feature", "petroleum", "pipeline",
          "tags['pipeline'] IN ('valve', 'flare')", [
              ("substance", s("substance")),
              ("valve", s("valve")),
          ]),
    Layer("marker", "petroleum", "marker",
          "tags['pipeline'] = 'marker' OR tags['power'] = 'marker' OR tags['marker'] IS NOT NULL", [
              ("marker", s("marker")),
              ("utility", s("utility")),
              ("substance", s("substance")),
          ]),
    # ---- water
    Layer("water_treatment_plant", "water", "man_made",
          "lc_val(tags, 'man_made') IN ('water_works', 'desalination_plant')", [
              ("area_m2", "area_m2(geometry)"),
          ]),
    Layer("wastewater_plant", "water", "man_made",
          "lc_val(tags, 'man_made') = 'wastewater_plant'", [
              ("area_m2", "area_m2(geometry)"),
          ]),
    Layer("pumping_station", "water", "man_made",
          "lc_val(tags, 'man_made') = 'pumping_station'", [
              ("pumping_station", s("pumping_station")),
              ("substance", s("substance")),
              ("area_m2", "area_m2(geometry)"),
          ]),
    Layer("water_tower", "water", "man_made",
          "lc_val(tags, 'man_made') = 'water_tower'", [
              ("height_m", "to_number(tags['height'])"),
          ]),
    Layer("water_well", "water", "man_made",
          "lc_val(tags, 'man_made') = 'water_well'", [
              ("pump", s("pump")),
              ("drinking_water", s("drinking_water")),
          ]),
    Layer("pressurised_waterway", "water", "waterway",
          "tags['waterway'] = 'pressurised'", [
              ("length_km", "length_km(geometry)"),
          ]),
]

# The value that names what the feature is, per layer (OIM's `type` column).
TYPE_EXPR = {
    "power": "lc_val(tags, 'power')",
    "communication": "lc_val(tags, 'communication')",
    "telecom": "coalesce(tags['telecom'], tags['building'], tags['office'], tags['man_made'])",
    "man_made": "lc_val(tags, 'man_made')",
    "industrial": "coalesce(tags['industrial'], tags['pipeline'])",
    "pipeline": "tags['pipeline']",
    "marker": "coalesce(tags['pipeline'], tags['power'], tags['marker'])",
    "waterway": "tags['waterway']",
}

COMMON = [
    ("type", None),  # TYPE_EXPR[layer.key]
    ("lifecycle", None),
    ("name", "tags['name']"),
    ("names", "names_of(tags)"),
    ("operator", "tags['operator']"),
    ("operator_wikidata", "tags['operator:wikidata']"),
    ("ref", "tags['ref']"),
    ("wikidata", "tags['wikidata']"),
    ("wikipedia", "tags['wikipedia']"),
    ("start_date", "tags['start_date']"),
    ("website", "coalesce(tags['website'], tags['contact:website'], tags['url'])"),
]


# --------------------------------------------------------------------------
# Loading

OPL_ESCAPE = re.compile(r"%([0-9a-fA-F]+)%")
OPL_TYPES = {"n": "node", "w": "way", "r": "relation"}


def opl_unescape(v: str) -> str:
    return OPL_ESCAPE.sub(lambda m: chr(int(m.group(1), 16)), v)


def relations_jsonl(pbf: Path, dest: Path) -> int:
    """Circuit and site relations, with their members, as JSON lines.
    osmium export builds no geometry for them (only multipolygons), so they
    are read from OPL and assembled in SQL."""
    # Streamed: worldwide, the relations' OPL is too big to hold as one string.
    proc = subprocess.Popen([OSMIUM, "cat", str(pbf), "-t", "relation", "-f",
                             "opl,add_metadata=false"], stdout=subprocess.PIPE, text=True)
    n = 0
    with dest.open("w") as f:
        for line in proc.stdout:
            line = line.rstrip("\n")
            parts = line.split(" ")
            rid = int(parts[0][1:])
            tags, members = {}, []
            for p in parts[1:]:
                if p.startswith("T") and len(p) > 1:
                    for kv in p[1:].split(","):
                        k, _, v = kv.partition("=")
                        tags[opl_unescape(k)] = opl_unescape(v)
                elif p.startswith("M") and len(p) > 1:
                    for m in p[1:].split(","):
                        ref, _, role = m.partition("@")
                        members.append({"type": OPL_TYPES[ref[0]], "id": int(ref[1:]),
                                         "role": opl_unescape(role)})
            if tags.get("type") not in ("site", "route", "power"):
                continue
            f.write(json.dumps({"osm_id": rid, "tags": tags, "members": members}) + "\n")
            n += 1
    if proc.wait():
        raise subprocess.CalledProcessError(proc.returncode, proc.args)
    return n


def load(con: duckdb.DuckDBPyConnection, pbf: Path, work: Path) -> None:
    jsonseq = work / "infra.geojsonseq"
    run([OSMIUM, "export", str(pbf), "--geometry-types", "point,linestring,polygon",
         "--add-unique-id", "type_id", "--output-format", "geojsonseq",
         "-x", "print_record_separator=false", "-o", str(jsonseq), "--overwrite"])
    con.execute(f"""
        CREATE TABLE raw AS
        SELECT CASE WHEN kind = 'a' THEN num // 2 ELSE num END AS osm_id,
               CASE kind WHEN 'n' THEN 'node' WHEN 'w' THEN 'way' WHEN 'r' THEN 'relation'
                         WHEN 'a' THEN IF(num % 2 = 0, 'way', 'relation') END AS osm_type,
               tags, geometry
        FROM (SELECT left(id, 1) AS kind, TRY_CAST(substr(id, 2) AS BIGINT) AS num,
                     CAST(properties AS MAP(VARCHAR, VARCHAR)) AS tags,
                     ST_GeomFromGeoJSON(geometry) AS geometry
              FROM read_json('{jsonseq}', format = 'newline_delimited',
                             maximum_object_size = 268435456,
                             columns = {{'id': 'VARCHAR', 'properties': 'JSON',
                                        'geometry': 'JSON'}}))
        WHERE geometry IS NOT NULL
    """)
    jsonseq.unlink()   # tens of GB worldwide, and loaded now
    # One geometry per element: see LINEAR.
    con.execute(f"""
        CREATE TABLE feat AS SELECT * FROM raw
        QUALIFY count(*) OVER (PARTITION BY osm_type, osm_id) = 1
             OR (ST_GeometryType(geometry)::VARCHAR IN ('LINESTRING', 'MULTILINESTRING'))
                = coalesce({LINEAR}, false)
    """)
    con.execute("DROP TABLE raw")

    rel = work / "relations.jsonl"
    n = relations_jsonl(pbf, rel)
    if n:
        con.execute(f"""
            CREATE TABLE rel AS
            SELECT osm_id, CAST(tags AS MAP(VARCHAR, VARCHAR)) AS tags,
                   CAST(members AS STRUCT(type VARCHAR, id BIGINT, role VARCHAR)[]) AS members
            FROM read_json('{rel}', format = 'newline_delimited',
                           columns = {{'osm_id': 'BIGINT', 'tags': 'JSON', 'members': 'JSON'}})
        """)
    else:
        con.execute("""
            CREATE TABLE rel (osm_id BIGINT, tags MAP(VARCHAR, VARCHAR),
                              members STRUCT(type VARCHAR, id BIGINT, role VARCHAR)[])""")
    con.execute("""
        CREATE TABLE member AS
        SELECT r.osm_id AS rel_id, m.type AS osm_type, m.id AS osm_id, m.role
        FROM rel r, unnest(r.members) AS u(m)
    """)


# --------------------------------------------------------------------------
# Relation-derived and enriched sources

def build_sources(con: duckdb.DuckDBPyConnection) -> None:
    # Circuits: route=power, or type=power + power=circuit. Geometry is the
    # collection of their line members (OIM sums role=section for length;
    # older circuits carry no roles, so any line member counts).
    con.execute("""
        CREATE TABLE circuit AS
        SELECT r.osm_id, 'relation' AS osm_type, any_value(r.tags) AS tags,
               ST_Union_Agg(f.geometry) AS geometry,
               count(*) AS section_count,
               (SELECT nullif(list(struct_pack(osm_type := m2.osm_type, osm_id := m2.osm_id)
                                   ORDER BY m2.osm_type, m2.osm_id), [])
                FROM member m2 WHERE m2.rel_id = r.osm_id AND m2.role = 'substation'
               ) AS substation_members
        FROM rel r
        JOIN member m ON m.rel_id = r.osm_id AND m.role IN ('section', '')
        JOIN feat f ON f.osm_type = m.osm_type AND f.osm_id = m.osm_id
             AND ST_Dimension(f.geometry) = 1
        WHERE r.tags['route'] = 'power'
           OR (r.tags['type'] = 'power' AND lc_val(r.tags, 'power') = 'circuit')
        GROUP BY r.osm_id
    """)

    # Substations: elements, plus site relations as the convex hull of their
    # members with the members' voltages merged in (OIM
    # power_substation_relation). circuit_ids: the circuits naming it as a
    # substation member (OIM /api/substation).
    con.execute("""
        CREATE TABLE substation AS
        WITH site AS (
            SELECT r.osm_id, any_value(r.tags) AS rtags,
                   flatten(list(string_split(coalesce(f.tags['voltage'], ''), ';'))) AS mv,
                   ST_ConvexHull(ST_Union_Agg(f.geometry)) AS geometry
            FROM rel r
            JOIN member m ON m.rel_id = r.osm_id
            JOIN feat f ON f.osm_type = m.osm_type AND f.osm_id = m.osm_id
            WHERE r.tags['type'] = 'site' AND lc_val(r.tags, 'power') = 'substation'
            GROUP BY r.osm_id
        ), el AS (
            SELECT osm_id, osm_type, tags, geometry FROM feat
            WHERE lc_val(tags, 'power') = 'substation'
            UNION ALL
            SELECT osm_id, 'relation',
                   map_concat(rtags, map {'voltage': array_to_string(list_filter(list_distinct(
                       mv || string_split(coalesce(rtags['voltage'], ''), ';')),
                       x -> trim(x) <> ''), ';')}),
                   geometry
            FROM site
        )
        SELECT el.*, (SELECT nullif(list(DISTINCT c.rel_id ORDER BY c.rel_id), [])
                      FROM member c JOIN circuit ci ON ci.osm_id = c.rel_id
                      WHERE c.osm_type = el.osm_type AND c.osm_id = el.osm_id
                        AND c.role = 'substation') AS circuit_ids
        FROM el
    """)

    # Generators, with OIM's solar estimate (power_heatmap): the tagged output,
    # else modules x 250 W, else 150 W/m2 of panel, else 4 kW for a point.
    con.execute("""
        CREATE TABLE generator AS
        SELECT *,
               CASE WHEN power_mw(tags['generator:output:electricity']) IS NOT NULL THEN
                        power_mw(tags['generator:output:electricity'])
                    WHEN solar AND to_int(tags['generator:solar:modules']) IS NOT NULL THEN
                        -- BIGINT: someone tags 61,158,751 modules, and x 250 overflows INT32.
                        to_int(tags['generator:solar:modules'])::BIGINT * 250 / 1e6
                    WHEN solar AND ST_Dimension(geometry) = 2 THEN
                        ST_Area_Spheroid(ST_FlipCoordinates(geometry)) * 150 / 1e6
                    WHEN solar AND ST_Dimension(geometry) = 0 THEN 0.004
               END AS output_mw_estimated,
               CASE WHEN power_mw(tags['generator:output:electricity']) IS NOT NULL THEN 'tagged'
                    WHEN solar AND to_int(tags['generator:solar:modules']) IS NOT NULL THEN 'modules'
                    WHEN solar AND ST_Dimension(geometry) = 2 THEN 'area'
                    WHEN solar AND ST_Dimension(geometry) = 0 THEN 'point'
               END AS output_basis
        FROM (SELECT osm_id, osm_type, tags, geometry,
                     first_semi(tags['generator:source']) = 'solar' AS solar
              FROM feat WHERE lc_val(tags, 'power') = 'generator')
    """)

    # Plants: elements, plus site relations as a concave hull of their members
    # buffered by about 10 m (OIM power_plant_relation). The generator summary
    # (OIM plant detail page) sums the generators inside the plant or listed
    # as its members; solar plants without an output get OIM's 40 W/m2.
    con.execute("""
        CREATE TABLE plant_el AS
        SELECT osm_id, osm_type, tags, geometry FROM feat
        WHERE lc_val(tags, 'power') = 'plant'
        UNION ALL
        SELECT r.osm_id, 'relation', any_value(r.tags),
               ST_Buffer(ST_ConcaveHull(ST_Collect(list(f.geometry)), 0.95, false), 0.0001)
        FROM rel r
        JOIN member m ON m.rel_id = r.osm_id
        JOIN feat f ON f.osm_type = m.osm_type AND f.osm_id = m.osm_id
        WHERE r.tags['type'] = 'site' AND lc_val(r.tags, 'power') = 'plant'
        GROUP BY r.osm_id
    """)
    con.execute("""
        CREATE TABLE plant AS
        WITH gen AS (
            SELECT p.osm_type AS p_type, p.osm_id AS p_id, g.osm_type, g.osm_id,
                   g.output_mw_estimated
            FROM plant_el p JOIN generator g
              ON ST_Dimension(p.geometry) = 2 AND ST_Intersects(p.geometry, g.geometry)
            UNION
            SELECT 'relation', m.rel_id, g.osm_type, g.osm_id, g.output_mw_estimated
            FROM member m JOIN generator g ON g.osm_type = m.osm_type AND g.osm_id = m.osm_id
        ), summary AS (
            SELECT p_type, p_id, count(*) AS generator_count,
                   round(sum(output_mw_estimated), 3) AS generator_output_mw
            FROM gen GROUP BY ALL
        )
        SELECT p.*, s.generator_count, s.generator_output_mw,
               coalesce(power_mw(p.tags['plant:output:electricity']),
                        s.generator_output_mw,
                        CASE WHEN first_semi(p.tags['plant:source']) = 'solar'
                              AND ST_Dimension(p.geometry) = 2
                             THEN ST_Area_Spheroid(ST_FlipCoordinates(p.geometry)) * 40 / 1e6
                        END) AS output_mw_estimated,
               CASE WHEN power_mw(p.tags['plant:output:electricity']) IS NOT NULL THEN 'tagged'
                    WHEN s.generator_output_mw IS NOT NULL THEN 'generators'
                    WHEN first_semi(p.tags['plant:source']) = 'solar'
                         AND ST_Dimension(p.geometry) = 2 THEN 'area'
               END AS output_basis
        FROM plant_el p LEFT JOIN summary s ON s.p_type = p.osm_type AND s.p_id = p.osm_id
    """)


# --------------------------------------------------------------------------
# Countries

# Point-in-polygon cost follows the vertices of every candidate whose box holds
# the point, and a country's box can be most of a hemisphere (Denmark's,
# through Greenland, covers the Netherlands with 1.1 M vertices). DuckDB has
# no ST_Subdivide, so this is one: parts are split out, and any part over
# MAX_POINTS is cut into the four quarters of its box, round after round,
# until every candidate is small and local. Quartering copies a polygon four
# times per round; a fixed grid copied it once per cell, and Russia's and
# Nunavut's GAUL units span hundreds of cells (9.4 GB and out of memory on
# the planet build).
MAX_POINTS = 2000


def subdivide(con: duckdb.DuckDBPyConnection, src: str, dest: str) -> None:
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sd_todo AS
        SELECT region, unnest(ST_Dump(geometry)).geom AS geometry FROM {src}""")
    con.execute(f"CREATE TABLE {dest} AS SELECT region, geometry FROM sd_todo LIMIT 0")
    for _ in range(40):
        con.execute(f"""INSERT INTO {dest} SELECT region, geometry FROM sd_todo
                        WHERE ST_NPoints(geometry) <= {MAX_POINTS}""")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE sd_todo AS
            WITH big AS (
                SELECT region, geometry,
                       ST_XMin(geometry) AS x0, ST_YMin(geometry) AS y0,
                       ST_XMax(geometry) AS x1, ST_YMax(geometry) AS y1
                FROM sd_todo WHERE ST_NPoints(geometry) > {MAX_POINTS}
            ), cut AS (
                SELECT region, ST_Intersection(geometry, ST_MakeEnvelope(
                           CASE WHEN q % 2 = 0 THEN x0 ELSE (x0 + x1) / 2 END,
                           CASE WHEN q < 2 THEN y0 ELSE (y0 + y1) / 2 END,
                           CASE WHEN q % 2 = 0 THEN (x0 + x1) / 2 ELSE x1 END,
                           CASE WHEN q < 2 THEN (y0 + y1) / 2 ELSE y1 END)) AS piece
                FROM big, range(4) AS t(q)
            )
            SELECT region, geometry FROM (
                SELECT region, unnest(ST_Dump(piece)).geom AS geometry FROM cut
                WHERE NOT ST_IsEmpty(piece))
            WHERE ST_Dimension(geometry) = 2""")
        if not con.execute("SELECT count(*) FROM sd_todo").fetchone()[0]:
            break
    else:
        raise RuntimeError(f"{src}: pieces still over {MAX_POINTS} points after 40 rounds")
    con.execute("DROP TABLE sd_todo")


def load_regions(con: duckdb.DuckDBPyConnection, land: str, eez: str) -> None:
    import pycountry
    con.execute("CREATE TEMP TABLE iso (iso3 VARCHAR, iso2 VARCHAR)")
    con.executemany("INSERT INTO iso VALUES (?, ?)",
                    [(c.alpha_3, c.alpha_2) for c in pycountry.countries])
    xmin, ymin, xmax, ymax = con.execute("""
        SELECT min(ST_XMin(geometry)), min(ST_YMin(geometry)),
               max(ST_XMax(geometry)), max(ST_YMax(geometry)) FROM feat
    """).fetchone()
    env = f"ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})"
    # State folders are named after the unit, so a listing reads: ASCII,
    # lower case, hyphens (`provence-alpes-cote-d-azur`). Slugged over all
    # of GAUL, not only this extract, so a unit's folder does not depend on
    # what else was built; the few names repeated within a country (GAUL's
    # "Administrative unit not available", two Saint-Louis) take their GAUL
    # code as a suffix.
    con.execute(f"""
        CREATE TEMP TABLE state_slug AS
        WITH s AS (
            SELECT DISTINCT iso3_code, gaul1_code,
                   trim(regexp_replace(strip_accents(lower(gaul1_name)), '[^a-z0-9]+', '-', 'g'),
                        '-') AS slug
            FROM read_parquet('{land}')
        )
        SELECT gaul1_code,
               CASE WHEN count(*) OVER (PARTITION BY iso3_code, slug) > 1
                    THEN slug || '-' || gaul1_code ELSE slug END AS slug
        FROM s
    """)
    # `region` is `<country>/<state>`, the folder a feature lands in.
    con.execute(f"""
        CREATE TEMP TABLE land_src AS
        SELECT coalesce(iso.iso2, l.iso3_code) || '/' || ss.slug AS region,
               l.gaul0_name AS country_name, l.gaul1_name AS state_name,
               l.gaul1_code AS gaul1_code,
               ST_Intersection(l.geometry, {env}) AS geometry
        FROM read_parquet('{land}') l LEFT JOIN iso ON iso.iso3 = l.iso3_code
        JOIN state_slug ss USING (gaul1_code)
        WHERE l.bbox.xmax >= {xmin} AND l.bbox.xmin <= {xmax}
          AND l.bbox.ymax >= {ymin} AND l.bbox.ymin <= {ymax}
          AND ST_Intersects(l.geometry, {env})
    """)
    con.execute(f"""
        CREATE TEMP TABLE sea_src AS
        SELECT coalesce(iso.iso2, e.code) || '/_offshore' AS region,
               e.name AS country_name, e.name || ' (offshore)' AS state_name,
               NULL::BIGINT AS gaul1_code, e.geometry
        FROM (SELECT coalesce(iso_ter1, iso_sov1) AS code,
                     coalesce(territory1, "union") AS name,
                     ST_Intersection(ST_MakeValid(geom), {env}) AS geometry
              FROM ST_Read('{eez}') WHERE ST_Intersects(geom, {env})) e
        LEFT JOIN iso ON iso.iso3 = e.code
        WHERE e.code IS NOT NULL
    """)
    # Land names first: a country both layers know is named the GAUL way.
    con.execute("""
        CREATE TABLE region_name AS
        WITH r AS (
            SELECT DISTINCT region, country_name, state_name, gaul1_code, 0 AS src FROM land_src
            UNION ALL SELECT DISTINCT region, country_name, state_name, gaul1_code, 1 FROM sea_src
        ), c AS (
            SELECT split_part(region, '/', 1) AS country, arg_min(country_name, src) AS name
            FROM r GROUP BY 1
        )
        SELECT r.region, c.name AS country_name,
               CASE WHEN r.src = 1 THEN c.name || ' (offshore)' ELSE r.state_name END AS state_name,
               r.gaul1_code
        FROM r JOIN c ON c.country = split_part(r.region, '/', 1)
        UNION ALL SELECT '_intl/_intl', 'High seas', 'High seas', NULL
    """)
    subdivide(con, "land_src", "land")
    subdivide(con, "sea_src", "sea")


def assign_region(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Add `country` and `state` to a source table: GAUL L1 on land, else
    EEZ, else _intl. Each step joins plain tables so DuckDB plans a
    SPATIAL_JOIN."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pt AS
        SELECT osm_type, osm_id, ST_PointOnSurface(geometry) AS g FROM {table}""")
    con.execute("""CREATE OR REPLACE TEMP TABLE hit AS
        SELECT pt.osm_type, pt.osm_id, min(land.region) AS region
        FROM pt JOIN land ON ST_Contains(land.geometry, pt.g) GROUP BY ALL""")
    con.execute("""CREATE OR REPLACE TEMP TABLE pt AS
        SELECT pt.* FROM pt ANTI JOIN hit USING (osm_type, osm_id)""")
    con.execute("""INSERT INTO hit
        SELECT pt.osm_type, pt.osm_id, min(sea.region)
        FROM pt JOIN sea ON ST_Contains(sea.geometry, pt.g) GROUP BY ALL""")
    con.execute(f"""CREATE OR REPLACE TABLE {table} AS
        SELECT t.* EXCLUDE (r), split_part(r, '/', 1) AS country, split_part(r, '/', 2) AS state
        FROM (SELECT t.*, coalesce(hit.region, '_intl/_intl') AS r
              FROM {table} t LEFT JOIN hit USING (osm_type, osm_id)) t""")


# --------------------------------------------------------------------------
# EIA: US plants OSM does not have yet

# EIA-860M technology -> the OSM tags a mapper would use. Unlisted
# technologies get no source/method (the row still counts, with its MW).
EIA_TAGS = [
    ("Solar Photovoltaic", "solar", "photovoltaic"),
    ("Solar Thermal%", "solar", "thermal"),
    ("%Wind%", "wind", "wind_turbine"),
    ("Batteries", "battery", None),
    ("Flywheels", None, None),
    ("Nuclear", "nuclear", "fission"),
    ("Hydroelectric Pumped Storage", "hydro", "water-pumped-storage"),
    ("Conventional Hydroelectric", "hydro", None),
    ("%Coal%", "coal", "combustion"),
    ("Natural Gas%", "gas", "combustion"),
    ("Other Natural Gas", "gas", "combustion"),
    ("Other Gases", "gas", "combustion"),
    ("Petroleum%", "oil", "combustion"),
    ("Landfill Gas", "biogas", "combustion"),
    ("Wood/Wood Waste Biomass", "biomass", "combustion"),
    ("Other Waste Biomass", "biomass", "combustion"),
    ("Municipal Solid Waste", "waste", "combustion"),
    ("Geothermal", "geothermal", None),
]


def add_eia_plants(con: duckdb.DuckDBPyConnection, eia_xlsx: str) -> dict:
    """Add to `plant` the operating US plants EIA-860M reports and OSM lacks.

    A plant counts as present when OSM has it by ref:US:EIA, holds its point
    in a plant polygon, has a plant within 2 km, or has generators within
    500 m (small solar is often mapped as panels only): a borderline plant
    stays OSM-only rather than risk a duplicate. The rest become point rows
    with the tags their EIA record implies, osm_type and osm_id NULL (so
    origin = 'eia'). Returns the coverage figures published in _coverage.json.
    """
    con.execute("INSTALL excel; LOAD excel;")
    sheets = " UNION ALL ".join(
        f"SELECT * FROM read_xlsx('{eia_xlsx}', sheet='{sh}', range='A3:BZ200000', "
        f"header=true, all_varchar=true)" for sh in ("Operating", "Operating_PR"))
    cases = " ".join(f"WHEN technology LIKE '{t}' THEN {'NULL' if v is None else repr(v)}"
                     for t, v, _ in EIA_TAGS)
    methods = " ".join(f"WHEN technology LIKE '{t}' THEN {'NULL' if m is None else repr(m)}"
                       for t, _, m in EIA_TAGS)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE eia AS
        WITH g AS (
            SELECT TRY_CAST("Plant ID" AS INTEGER) AS plant_id, "Plant Name" AS name,
                   "Entity Name" AS operator, "Technology" AS technology,
                   TRY_CAST("Nameplate Capacity (MW)" AS DOUBLE) AS mw,
                   TRY_CAST("Operating Year" AS INTEGER) AS year,
                   TRY_CAST("Latitude" AS DOUBLE) AS lat, TRY_CAST("Longitude" AS DOUBLE) AS lon
            FROM ({sheets})
            WHERE TRY_CAST("Plant ID" AS INTEGER) IS NOT NULL
        )
        SELECT plant_id, any_value(name) AS name, any_value(operator) AS operator,
               arg_max(technology, mw) AS technology, round(sum(mw), 3) AS mw,
               min(year) AS year, any_value(lat) AS lat, any_value(lon) AS lon
        FROM g WHERE lat IS NOT NULL AND lon IS NOT NULL GROUP BY plant_id
    """)
    if not con.execute("SELECT count(*) FROM eia").fetchone()[0]:
        sys.exit(f"{eia_xlsx}: no operating plants read")
    # Only the plants the build covers: a partial build (one extract) would
    # otherwise count every plant outside it as missing. It still counts
    # those in the states its extract's box touches (Florida's: Alabama's
    # Barry plant), which is why a partial run never publishes.
    con.execute("""
        DELETE FROM eia WHERE NOT EXISTS (
            SELECT 1 FROM land WHERE split_part(region, '/', 1) IN ('US', 'PR')
              AND ST_Contains(geometry, ST_Point(eia.lon, eia.lat)))""")
    # Matching in metres (CONUS Albers; the thresholds are loose enough for
    # Alaska, Hawaii and Puerto Rico).
    albers = "'EPSG:4326', 'EPSG:5070', always_xy := true"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE eia_m AS
        SELECT e.*, ST_Transform(ST_Point(lon, lat), {albers}) AS p FROM eia e""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE osm_us AS
        SELECT TRY_CAST(first_semi(tags['ref:US:EIA']) AS INTEGER) AS eia_ref,
               ST_Transform(geometry, {albers}) AS g
        FROM plant WHERE country IN ('US', 'PR')""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE gen_us AS
        SELECT ST_Transform(ST_PointOnSurface(geometry), {albers}) AS g
        FROM generator WHERE country IN ('US', 'PR')""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE eia_how AS
        WITH ref AS (SELECT DISTINCT e.plant_id FROM eia_m e JOIN osm_us o ON o.eia_ref = e.plant_id),
             inside AS (SELECT DISTINCT e.plant_id FROM eia_m e JOIN osm_us o ON ST_Intersects(o.g, e.p)),
             near AS (SELECT DISTINCT e.plant_id FROM eia_m e JOIN osm_us o ON ST_DWithin(o.g, e.p, 2000)),
             gens AS (SELECT DISTINCT e.plant_id FROM eia_m e JOIN gen_us o ON ST_DWithin(o.g, e.p, 500))
        SELECT e.plant_id, e.mw,
               CASE WHEN e.plant_id IN (SELECT * FROM ref) THEN 'ref'
                    WHEN e.plant_id IN (SELECT * FROM inside) THEN 'inside'
                    WHEN e.plant_id IN (SELECT * FROM near) THEN 'near'
                    WHEN e.plant_id IN (SELECT * FROM gens) THEN 'generators'
                    ELSE 'added' END AS how
        FROM eia_m e
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE eia_add AS
        SELECT 'eia' AS osm_type, e.plant_id AS osm_id,
               map_from_entries(list_filter([
                   struct_pack(k := 'power', v := 'plant'),
                   struct_pack(k := 'name', v := e.name),
                   struct_pack(k := 'operator', v := e.operator),
                   struct_pack(k := 'plant:source', v := CASE {cases} END),
                   struct_pack(k := 'plant:method', v := CASE {methods} END),
                   struct_pack(k := 'plant:output:electricity', v := e.mw::VARCHAR || ' MW'),
                   struct_pack(k := 'ref:US:EIA', v := e.plant_id::VARCHAR),
                   struct_pack(k := 'start_date', v := e.year::VARCHAR)
               ], x -> x.v IS NOT NULL)) AS tags,
               ST_Point(e.lon, e.lat) AS geometry,
               NULL::BIGINT AS generator_count, NULL::DOUBLE AS generator_output_mw,
               e.mw AS output_mw_estimated, 'tagged' AS output_basis
        FROM eia e JOIN eia_how h USING (plant_id) WHERE h.how = 'added'
    """)
    assign_region(con, "eia_add")
    con.execute("UPDATE eia_add SET osm_type = NULL, osm_id = NULL")
    con.execute("INSERT INTO plant BY NAME SELECT * FROM eia_add")
    con.execute("DROP TABLE eia_add")
    tiers = {how: {"plants": n, "mw": round(mw, 1)} for how, n, mw in con.execute(
        "SELECT how, count(*), sum(mw) FROM eia_how GROUP BY 1").fetchall()}
    total = con.execute("SELECT count(*), sum(mw) FROM eia").fetchone()
    return {"source": Path(eia_xlsx).name, "eia_plants": total[0],
            "eia_mw": round(total[1], 1), "tiers": tiers}


# --------------------------------------------------------------------------
# Writing

GEOMETRY_TYPE_NAMES = {
    "POINT": "Point", "LINESTRING": "LineString", "POLYGON": "Polygon",
    "MULTIPOINT": "MultiPoint", "MULTILINESTRING": "MultiLineString",
    "MULTIPOLYGON": "MultiPolygon", "GEOMETRYCOLLECTION": "GeometryCollection",
}
BBOX = """struct_pack(
    xmin := (ST_XMin(geometry) - abs(ST_XMin(geometry)) * 1e-6 - 1e-9)::FLOAT,
    ymin := (ST_YMin(geometry) - abs(ST_YMin(geometry)) * 1e-6 - 1e-9)::FLOAT,
    xmax := (ST_XMax(geometry) + abs(ST_XMax(geometry)) * 1e-6 + 1e-9)::FLOAT,
    ymax := (ST_YMax(geometry) + abs(ST_YMax(geometry)) * 1e-6 + 1e-9)::FLOAT)"""


def layer_select(layer: Layer) -> str:
    cols = []
    for name, expr in COMMON:
        if name == "type":
            expr = TYPE_EXPR[layer.key]
        elif name == "lifecycle":
            expr = f"lifecycle(tags, '{layer.key}')"
        cols.append(f"{expr} AS {name}")
    cols += [f"{expr} AS {name}" for name, expr in layer.columns]
    # Rows added from another source (EIA plants OSM lacks) have no OSM
    # element: no osm_id/osm_type, and no tags, since theirs only exist to
    # derive the columns.
    return (f"SELECT osm_id, osm_type, "
            f"CASE WHEN osm_type IS NULL THEN 'eia' ELSE 'osm' END AS origin, "
            f"country, state, {', '.join(cols)}, "
            f"CASE WHEN osm_type IS NULL THEN NULL ELSE tags END AS tags, "
            f"{BBOX} AS bbox, geometry FROM {layer.source} WHERE {layer.where}")


def write_layer(con: duckdb.DuckDBPyConnection, layer: Layer, staging: Path) -> dict:
    """The layer, sorted along a Hilbert curve over its extent, as one DuckDB
    file in staging (re-cut into exact row groups afterwards). Written
    straight from its query: materialising it first put it on disk twice (a
    temp table lives in the temp directory, and the sort spills beside it)."""
    t0 = time.monotonic()
    n, types, xmin, ymin, xmax, ymax, countries, regions = con.execute(f"""
        SELECT count(*), list(DISTINCT ST_GeometryType(geometry)::VARCHAR),
               min(ST_XMin(geometry)), min(ST_YMin(geometry)),
               max(ST_XMax(geometry)), max(ST_YMax(geometry)),
               count(DISTINCT country), count(DISTINCT (country, state))
        FROM {layer.source} WHERE {layer.where}""").fetchone()
    if not n:
        print(f"  {layer.name}: empty", flush=True)
        return {"layer": layer.name, "rows": 0}
    geo = json.dumps({
        "version": "2.0.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB",
            "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in types),
            "bbox": [xmin, ymin, xmax, ymax],
            "covering": {"bbox": {"xmin": ["bbox", "xmin"], "ymin": ["bbox", "ymin"],
                                  "xmax": ["bbox", "xmax"], "ymax": ["bbox", "ymax"]}},
        }},
    }).replace("'", "''")
    dest = staging / f"{layer.name}.parquet"
    con.execute(f"""
        COPY ({layer_select(layer)} ORDER BY
              ST_Hilbert(geometry, ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))))
        TO '{dest}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE',
                     COMPRESSION ZSTD, COMPRESSION_LEVEL 1, ROW_GROUP_SIZE 1048576,
                     KV_METADATA {{geo: '{geo}'}})
    """)
    print(f"  {layer.name}: {n:,} rows, {countries} countries, sorted in "
          f"{time.monotonic() - t0:.0f} s", flush=True)
    return {"layer": layer.name, "group": layer.group, "rows": n,
            "countries": countries, "regions": regions}


ROW_GROUP_ROWS = 32_000


def rewrite(src: Path, dest: Path) -> int:
    """Re-cut a DuckDB-written file into row groups of exactly ROW_GROUP_ROWS,
    streamed so memory does not follow the layer (40 M towers).

    DuckDB only writes row groups in multiples of its 2,048-row vector size
    (ROW_GROUP_SIZE 32000 gives 32,768). pyarrow 22 with geoarrow-pyarrow
    registered keeps the native Parquet GEOMETRY logical type, its geospatial
    statistics and the `geo` footer. Measured on 83k NL generators: +3% for
    32k groups over ~50k, +8% for pyarrow's writer. Returns the row count.
    """
    import geoarrow.pyarrow  # noqa: F401  registers the geoarrow.wkb extension type
    import pyarrow as pa
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(src)
    leaves = [pf.metadata.schema.column(i) for i in range(pf.metadata.num_columns)]
    dest.parent.mkdir(parents=True, exist_ok=True)
    rows, pending, pending_rows = 0, [], 0
    with pq.ParquetWriter(dest, pf.schema_arrow, **writer_options(leaves)) as w:
        def flush(n: int) -> None:
            nonlocal pending, pending_rows
            table = pa.Table.from_batches(pending)
            w.write_table(table.slice(0, n), row_group_size=ROW_GROUP_ROWS)
            rest = table.slice(n)
            pending, pending_rows = rest.to_batches(), rest.num_rows
        for batch in pf.iter_batches(batch_size=ROW_GROUP_ROWS):
            pending.append(batch)
            pending_rows += batch.num_rows
            rows += batch.num_rows
            while pending_rows >= ROW_GROUP_ROWS:
                flush(ROW_GROUP_ROWS)
        if pending_rows:
            flush(pending_rows)
    return rows


def writer_options(leaves) -> dict:
    """pyarrow writer settings: what DuckDB gets for free (byte-stream-split
    floats, delta-packed ids), zstd 15, 1 MB pages."""
    return dict(
        compression="zstd", compression_level=15, write_statistics=True,
        use_dictionary=[c.path for c in leaves
                        if c.physical_type == "BYTE_ARRAY" and c.path != "geometry"],
        use_byte_stream_split=[c.path for c in leaves
                               if c.physical_type in ("FLOAT", "DOUBLE")],
        column_encoding={"osm_id": "DELTA_BINARY_PACKED"},
        data_page_size=1 << 20)


def verify(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """Fail loudly unless a written file is what the `geo` footer says it is."""
    kv = con.execute("SELECT key::VARCHAR, value FROM parquet_kv_metadata(?)",
                     [str(path)]).fetchall()
    geos = [v for k, v in kv if k == "geo"]
    assert len(geos) == 1, f"{path}: {len(geos)} geo keys"
    col = json.loads(geos[0])["columns"]["geometry"]
    # The covering must name real FLOAT fields of a real struct column.
    cov = col["covering"]["bbox"]
    assert {k: v for k, v in cov.items()} == {
        k: ["bbox", k] for k in ("xmin", "ymin", "xmax", "ymax")}, f"{path}: covering {cov}"
    fields = dict(con.execute("""
        SELECT name, type FROM parquet_schema(?) WHERE name IN ('xmin', 'ymin', 'xmax', 'ymax', 'geometry')
    """, [str(path)]).fetchall())
    assert all(fields.get(k) == "FLOAT" for k in ("xmin", "ymin", "xmax", "ymax")), f"{path}: {fields}"
    logical = con.execute("SELECT logical_type FROM parquet_schema(?) WHERE name = 'geometry'",
                          [str(path)]).fetchone()[0]
    assert logical and logical.startswith("GeometryType"), f"{path}: geometry is {logical}"
    groups = [n for _, n in con.execute("""
        SELECT DISTINCT row_group_id, row_group_num_rows FROM parquet_metadata(?)
        ORDER BY row_group_id""", [str(path)]).fetchall()]
    assert all(n == ROW_GROUP_ROWS for n in groups[:-1]) and groups[-1] <= ROW_GROUP_ROWS, \
        f"{path}: row groups {groups}"
    # Every bbox really holds its geometry, and every type is declared.
    bad, types = con.execute(f"""
        SELECT count(*) FILTER (WHERE bbox.xmin > ST_XMin(geometry) OR bbox.ymin > ST_YMin(geometry)
                                   OR bbox.xmax < ST_XMax(geometry) OR bbox.ymax < ST_YMax(geometry)),
               list(DISTINCT ST_GeometryType(geometry)::VARCHAR)
        FROM read_parquet('{path}')""").fetchone()
    assert bad == 0, f"{path}: {bad} bboxes do not contain their geometry"
    undeclared = {GEOMETRY_TYPE_NAMES[t] for t in types} - set(col["geometry_types"])
    assert not undeclared, f"{path}: undeclared geometry types {undeclared}"


def write_repository(out: Path, report: list[dict]) -> None:
    """_manifest.json in <out> and index.json at the repository base, the
    parquetry protocol GeoPQ Workbench reads: one dataset, the whole world,
    at an empty path (its files are <out>/<layer>.parquet), as GAUL's
    whole-world entry."""
    themes = {r["layer"]: r["rows"] for r in report if r["rows"]}
    (out / "_manifest.json").write_text(json.dumps({
        "state_name": "The whole world", "total_features": sum(themes.values()),
        "themes": themes,
    }, indent=2))
    (out.parent / "index.json").write_text(json.dumps({"datasets": [
        {"path": "", "code": "WORLD", "name": "The whole world"}]}, indent=2))


STATS = {
    # OIM stats.power_line: length by country and (highest) voltage.
    "power_line_length": """
        SELECT country, voltage_kv, count(*) AS lines, round(sum(length_km), 3) AS length_km
        FROM read_parquet('{out}/power_line.parquet')
        WHERE lifecycle = 'active' GROUP BY ALL ORDER BY ALL""",
    # OIM stats.power_plant / power_generator: count and output by source.
    "power_plant_by_source": """
        SELECT country, source, count(*) AS plants,
               round(sum(output_mw), 3) AS output_mw_tagged,
               round(sum(output_mw_estimated), 3) AS output_mw_estimated
        FROM read_parquet('{out}/power_plant.parquet')
        WHERE lifecycle = 'active' GROUP BY ALL ORDER BY ALL""",
    "power_generator_by_source": """
        SELECT country, source, count(*) AS generators,
               round(sum(output_mw), 3) AS output_mw_tagged,
               round(sum(output_mw_estimated), 3) AS output_mw_estimated
        FROM read_parquet('{out}/power_generator.parquet')
        WHERE lifecycle = 'active' GROUP BY ALL ORDER BY ALL""",
    # OIM stats.substation: count by country and highest voltage.
    "power_substation_by_voltage": """
        SELECT country, voltage_kv, count(*) AS substations
        FROM read_parquet('{out}/power_substation.parquet')
        WHERE lifecycle = 'active' GROUP BY ALL ORDER BY ALL""",
}


def write_stats(con: duckdb.DuckDBPyConnection, out: Path) -> None:
    (out / "stats").mkdir(exist_ok=True)
    for name, sql in STATS.items():
        con.execute(f"COPY ({sql.format(out=out)}) TO '{out}/stats/{name}.parquet' "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)")


# --------------------------------------------------------------------------

def build(pbf: Path, out: Path, work: Path, land: str, eez: str,
          eia: str | None = None) -> None:
    work.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(work / "infra.duckdb"))
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET temp_directory = '{work}/tmp'")
    # DuckDB's default (80% of RAM) left no room on a 16 GB runner: the
    # planet build peaked at 13.8 GB while loading. A lower cap makes it
    # spill to disk sooner, and insertion order is never relied on here
    # (every write orders explicitly).
    # INFRA_MEMORY_LIMIT (e.g. 8800MB) overrides it, to rehearse a runner locally.
    ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    limit = os.environ.get("INFRA_MEMORY_LIMIT") or f"{int(ram * 0.55 / 2**20)}MB"
    con.execute(f"SET memory_limit = '{limit}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(MACROS)
    load(con, pbf, work)
    build_sources(con)
    load_regions(con, land, eez)
    for table in ("feat", "circuit", "substation", "generator", "plant"):
        assign_region(con, table)
    coverage = add_eia_plants(con, eia) if eia else None
    if coverage:
        added = coverage["tiers"].get("added", {"plants": 0, "mw": 0})
        print(f"  EIA-860M: {coverage['eia_plants']:,} US plants, {added['plants']:,} "
              f"({added['mw'] / 1000:,.1f} GW) added where OSM has none", flush=True)
    staging = work / "staging"
    staging.mkdir(exist_ok=True)
    report = [write_layer(con, layer, staging) for layer in LAYERS]
    # The slugs rows carry, with their names and GAUL codes: how a reader
    # finds that Île-de-France is state = 'ile-de-france'. A sidecar (the
    # leading _ keeps folder scans from taking it for a dataset).
    regions = [dict(zip(("country", "state", "country_name", "state_name", "gaul1_code"), r))
               for r in con.execute("""
                   SELECT split_part(region, '/', 1), split_part(region, '/', 2),
                          country_name, state_name, gaul1_code
                   FROM region_name ORDER BY 1, 4""").fetchall()]
    (out / "_regions.json").write_text(json.dumps(regions, indent=1, ensure_ascii=False))
    con.close()
    check = duckdb.connect()
    check.execute("INSTALL spatial; LOAD spatial;")
    for r in report:
        if not r["rows"]:
            continue
        t0 = time.monotonic()
        dest = out / f"{r['layer']}.parquet"
        n = rewrite(staging / f"{r['layer']}.parquet", dest)
        # Nothing is published unless every source row reached its file.
        if n != r["rows"]:
            sys.exit(f"{dest.name}: {n:,} rows written, {r['rows']:,} in the source")
        verify(check, dest)
        (staging / f"{r['layer']}.parquet").unlink()
        print(f"  {dest.name}: {n:,} rows, {dest.stat().st_size / 1e6:,.0f} MB, "
              f"{time.monotonic() - t0:.0f} s", flush=True)
    write_repository(out, report)
    if coverage:
        (out / "_coverage.json").write_text(json.dumps(coverage, indent=2))
    write_stats(check, out)
    (out / "_layers.json").write_text(json.dumps(report, indent=2))
    for r in report:
        print(f"  {r['layer']:24} {r['rows']:>11,} rows  {r.get('countries', 0):>3} countries"
              f"  {r.get('regions', 0):>4} regions")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("filter", help="extract -> infrastructure subset")
    f.add_argument("src", type=Path)
    f.add_argument("dest", type=Path)
    b = sub.add_parser("build", help="subset -> partitioned GeoParquet")
    b.add_argument("pbf", type=Path)
    b.add_argument("--out-dir", type=Path, required=True)
    b.add_argument("--work-dir", type=Path, required=True)
    b.add_argument("--land", required=True,
                   help="GAUL 2024 L1 parquet (path or URL)")
    b.add_argument("--eia", help="EIA-860M generator workbook (.xlsx): adds the "
                   "operating US plants OSM lacks, and writes _coverage.json")
    b.add_argument("--eez", required=True,
                   help="Marine Regions eez_land as GeoJSON (path)")
    a = p.parse_args()
    if a.cmd == "filter":
        filter_extract(a.src, a.dest)
    else:
        build(a.pbf, a.out_dir, a.work_dir, a.land, a.eez, a.eia)


if __name__ == "__main__":
    sys.exit(main())
