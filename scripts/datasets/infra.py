#!/usr/bin/env python3
"""
OpenStreetMap infrastructure as partitioned GeoParquet: power, telecoms, oil
and gas, water. The content of Open Infrastructure Map (openinframap.org),
built from OSM extracts rather than a live PostGIS.

Parity is with openinframap/openinframap as of 2026-09: every table of its
imposm mapping (imposm/*.py) is a layer here, every attribute its tile layers
serve (tegola/layers.yml) is a column, and its derived values (schema/
functions.sql, views.sql: voltages in kV, outputs in MW, solar estimates,
pipeline categories, site relations merged from their members, circuit
lengths) are computed the same way. Their code is BSD-licensed; the rules are
credited in ATTRIBUTION.txt. Where they drop data we keep it: every geometry
type of a layer (not only the one their tiles draw), all name:* variants, the
full tag map, and a power_other layer for power=* values they do not map.

Two stages, so the worldwide build can fan out over Geofabrik regions:

  filter  one extract -> the infrastructure subset (about 0.4% of the input)
  build   one (merged) subset -> <out>/<layer>/country=<ISO3>/data_0.parquet
          plus <out>/stats/*.parquet

Countries: a feature goes to exactly one country, the one holding a point on
it (ST_PointOnSurface), from FAO GAUL 2024 L0 on land, else from Marine
Regions' union of land and EEZ offshore, else `_intl` (high seas).
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
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
    "pumping_station,water_tower,water_well,reservoir_covered",
    "w/waterway=pressurised", "wr/water=reservoir",
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
    Layer("water_reservoir", "water", "man_made",
          "tags['man_made'] = 'reservoir_covered' OR tags['water'] = 'reservoir'", [
              ("area_m2", "area_m2(geometry)"),
          ]),
]

# The value that names what the feature is, per layer (OIM's `type` column).
TYPE_EXPR = {
    "power": "lc_val(tags, 'power')",
    "communication": "lc_val(tags, 'communication')",
    "telecom": "coalesce(tags['telecom'], tags['building'], tags['office'], tags['man_made'])",
    "man_made": "coalesce(lc_val(tags, 'man_made'), tags['water'])",
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
    out = subprocess.run([OSMIUM, "cat", str(pbf), "-t", "relation", "-f",
                          "opl,add_metadata=false"],
                         check=True, capture_output=True, text=True).stdout
    n = 0
    with dest.open("w") as f:
        for line in out.splitlines():
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
                        to_int(tags['generator:solar:modules']) * 250 / 1e6
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
# no ST_Subdivide, so parts are split out and large ones clipped to a 1 degree
# grid: every candidate is then small and local.
SUBDIVIDE = """
    WITH parts AS (
        SELECT country, unnest(ST_Dump(geometry)).geom AS geometry FROM {src}
    ), big AS (
        SELECT country, geometry,
               floor(ST_XMin(geometry))::INT AS x0, ceil(ST_XMax(geometry))::INT AS x1,
               floor(ST_YMin(geometry))::INT AS y0, ceil(ST_YMax(geometry))::INT AS y1
        FROM parts WHERE ST_NPoints(geometry) > 2000
    )
    SELECT country, geometry FROM parts WHERE ST_NPoints(geometry) <= 2000
    UNION ALL
    SELECT country, piece AS geometry FROM (
        SELECT country, ST_Intersection(geometry, ST_MakeEnvelope(x, y, x + 1, y + 1)) AS piece
        FROM big, range(x0, x1) AS gx(x), range(y0, y1) AS gy(y)
        WHERE ST_Intersects(geometry, ST_MakeEnvelope(x, y, x + 1, y + 1))
    ) WHERE NOT ST_IsEmpty(piece) AND ST_Dimension(piece) = 2
"""


def load_countries(con: duckdb.DuckDBPyConnection, land: str, eez: str) -> None:
    xmin, ymin, xmax, ymax = con.execute("""
        SELECT min(ST_XMin(geometry)), min(ST_YMin(geometry)),
               max(ST_XMax(geometry)), max(ST_YMax(geometry)) FROM feat
    """).fetchone()
    env = f"ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})"
    con.execute(f"""
        CREATE TEMP TABLE land_src AS
        SELECT iso3_code AS country, ST_Intersection(geometry, {env}) AS geometry
        FROM read_parquet('{land}')
        WHERE bbox.xmax >= {xmin} AND bbox.xmin <= {xmax}
          AND bbox.ymax >= {ymin} AND bbox.ymin <= {ymax}
          AND ST_Intersects(geometry, {env})
    """)
    con.execute(f"""
        CREATE TEMP TABLE sea_src AS
        SELECT coalesce(iso_ter1, iso_sov1) AS country,
               ST_Intersection(ST_MakeValid(geom), {env}) AS geometry
        FROM ST_Read('{eez}')
        WHERE ST_Intersects(geom, {env})
    """)
    con.execute(f"CREATE TABLE land AS {SUBDIVIDE.format(src='land_src')}")
    con.execute(f"CREATE TABLE sea AS {SUBDIVIDE.format(src='sea_src')}")


def assign_country(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Add `country` to a source table: GAUL on land, else EEZ, else _intl.
    Each step joins plain tables so DuckDB plans a SPATIAL_JOIN."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pt AS
        SELECT osm_type, osm_id, ST_PointOnSurface(geometry) AS g FROM {table}""")
    con.execute("""CREATE OR REPLACE TEMP TABLE hit AS
        SELECT pt.osm_type, pt.osm_id, min(land.country) AS country
        FROM pt JOIN land ON ST_Contains(land.geometry, pt.g) GROUP BY ALL""")
    con.execute("""CREATE OR REPLACE TEMP TABLE pt AS
        SELECT pt.* FROM pt ANTI JOIN hit USING (osm_type, osm_id)""")
    con.execute("""INSERT INTO hit
        SELECT pt.osm_type, pt.osm_id, min(sea.country)
        FROM pt JOIN sea ON ST_Contains(sea.geometry, pt.g) GROUP BY ALL""")
    con.execute(f"""CREATE OR REPLACE TABLE {table} AS
        SELECT t.*, coalesce(hit.country, '_intl') AS country
        FROM {table} t LEFT JOIN hit USING (osm_type, osm_id)""")


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
    return (f"SELECT osm_id, osm_type, country, {', '.join(cols)}, tags, "
            f"{BBOX} AS bbox, geometry FROM {layer.source} WHERE {layer.where}")


def write_layer(con: duckdb.DuckDBPyConnection, layer: Layer, out: Path) -> dict:
    con.execute(f"CREATE OR REPLACE TEMP TABLE l AS {layer_select(layer)}")
    n, types, xmin, ymin, xmax, ymax = con.execute("""
        SELECT count(*), list(DISTINCT ST_GeometryType(geometry)::VARCHAR),
               min(ST_XMin(geometry)), min(ST_YMin(geometry)),
               max(ST_XMax(geometry)), max(ST_YMax(geometry)) FROM l""").fetchone()
    if not n:
        return {"layer": layer.name, "rows": 0}
    # Per-file extents differ by partition, so the `geo` block declares the
    # geometry types (a superset per file, which the spec allows) and the
    # covering, and leaves the optional bbox out.
    geo = json.dumps({
        "version": "2.0.0", "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB",
            "geometry_types": sorted(GEOMETRY_TYPE_NAMES[t] for t in types),
            "covering": {"bbox": {"xmin": ["bbox", "xmin"], "ymin": ["bbox", "ymin"],
                                  "xmax": ["bbox", "xmax"], "ymax": ["bbox", "ymax"]}},
        }},
    }).replace("'", "''")
    dest = out / layer.name
    con.execute(f"""
        COPY (SELECT * FROM l ORDER BY country,
              ST_Hilbert(geometry, ST_Extent(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}))))
        TO '{dest}' (FORMAT PARQUET, PARTITION_BY (country), WRITE_PARTITION_COLUMNS true,
                     OVERWRITE_OR_IGNORE, GEOPARQUET_VERSION 'NONE',
                     COMPRESSION ZSTD, COMPRESSION_LEVEL 15, ROW_GROUP_SIZE 50000,
                     KV_METADATA {{geo: '{geo}'}})
    """)
    countries = con.execute("SELECT count(DISTINCT country) FROM l").fetchone()[0]
    return {"layer": layer.name, "group": layer.group, "rows": n, "countries": countries}


STATS = {
    # OIM stats.power_line: length by country and (highest) voltage.
    "power_line_length": """
        SELECT country, voltage_kv, count(*) AS lines, round(sum(length_km), 3) AS length_km
        FROM read_parquet('{out}/power_line/*/*.parquet')
        WHERE lifecycle = 'active' GROUP BY ALL ORDER BY ALL""",
    # OIM stats.power_plant / power_generator: count and output by source.
    "power_plant_by_source": """
        SELECT country, source, count(*) AS plants,
               round(sum(output_mw), 3) AS output_mw_tagged,
               round(sum(output_mw_estimated), 3) AS output_mw_estimated
        FROM read_parquet('{out}/power_plant/*/*.parquet')
        WHERE lifecycle = 'active' GROUP BY ALL ORDER BY ALL""",
    "power_generator_by_source": """
        SELECT country, source, count(*) AS generators,
               round(sum(output_mw), 3) AS output_mw_tagged,
               round(sum(output_mw_estimated), 3) AS output_mw_estimated
        FROM read_parquet('{out}/power_generator/*/*.parquet')
        WHERE lifecycle = 'active' GROUP BY ALL ORDER BY ALL""",
    # OIM stats.substation: count by country and highest voltage.
    "power_substation_by_voltage": """
        SELECT country, voltage_kv, count(*) AS substations
        FROM read_parquet('{out}/power_substation/*/*.parquet')
        WHERE lifecycle = 'active' GROUP BY ALL ORDER BY ALL""",
}


def write_stats(con: duckdb.DuckDBPyConnection, out: Path) -> None:
    (out / "stats").mkdir(exist_ok=True)
    for name, sql in STATS.items():
        con.execute(f"COPY ({sql.format(out=out)}) TO '{out}/stats/{name}.parquet' "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)")


# --------------------------------------------------------------------------

def build(pbf: Path, out: Path, work: Path, land: str, eez: str) -> None:
    work.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(work / "infra.duckdb"))
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET temp_directory = '{work}/tmp'")
    con.execute(MACROS)
    load(con, pbf, work)
    build_sources(con)
    load_countries(con, land, eez)
    for table in ("feat", "circuit", "substation", "generator", "plant"):
        assign_country(con, table)
    report = [write_layer(con, layer, out) for layer in LAYERS]
    write_stats(con, out)
    (out / "_layers.json").write_text(json.dumps(report, indent=2))
    for r in report:
        print(f"  {r['layer']:24} {r['rows']:>9,} rows  {r.get('countries', 0):>3} countries")


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
                   help="GAUL 2024 L0 parquet (path or URL)")
    b.add_argument("--eez", required=True,
                   help="Marine Regions eez_land as GeoJSON (path)")
    a = p.parse_args()
    if a.cmd == "filter":
        filter_extract(a.src, a.dest)
    else:
        build(a.pbf, a.out_dir, a.work_dir, a.land, a.eez)


if __name__ == "__main__":
    sys.exit(main())
