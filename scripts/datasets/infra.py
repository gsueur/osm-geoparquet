#!/usr/bin/env python3
"""
GMWID, the GeoMermaids World Infrastructures Dataset: OpenStreetMap
infrastructure as GeoParquet, enriched from authoritative sources: power, telecoms, oil
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
make it a parquetry repository with two datasets as GeoPQ Workbench's
browser reads it: the whole world, and what is abandoned.

Abandoned features (abandoned:*=* or abandoned=yes in OSM, abandoned in
place in a source) are not in the layer files: they are in <out>/abandoned/,
the same layers with the same schema, so the main files hold what stands or
is being built. Disused (out of service, still standing) stays in the main
files, with lifecycle = 'disused'.

US completeness (--eia): the operating plants the EIA-860M inventory reports
and OpenStreetMap lacks are added to power_plant as points, origin = 'eia'
(97.8% of US capacity is already in OSM; what is missing is mostly recent
and small solar). With --uspvdb, an added solar plant USPVDB has gets its
outline instead of EIA's point. _coverage.json says how each EIA plant was
found, and how many added plants have an outline.

More sources complete other layers the same way: each row says where it
comes from (origin), source_id is its ID there, and _coverage.json has, per
source, how its records were matched (by an ID OSM carries, or by distance)
and how many were added. OSM rows are never changed.

  --uswtdb  US wind turbines (USGS/LBNL/ACP USWTDB) -> power_generator
  --fcc     US communication towers (FCC Antenna Structure Registration)
            -> telecom_mast
  --ore     French HTA/BT substations (Agence ORE, every distribution
            operator) -> power_substation
  --boem    US offshore wells, platforms and pipelines (BSEE, BOEM)
            -> petroleum_well, offshore_platform, pipeline
  --ogim    the world's oil and gas wells, sites, platforms and pipelines
            (OGIM v3.0, EDF/MethaneSAT, CC BY 4.0), except from the sources
            whose terms restrict reuse -> petroleum_well, petroleum_site,
            offshore_platform, pipeline

UK oil and gas (--nsta): the wells, platforms and pipelines the North Sea
Transition Authority reports and OpenStreetMap lacks. NSTA's licence is
non-commercial, so they are not added to the layers: they go to
<out>/../nsta/, one file per layer with the layer's schema (origin =
'nsta'), with its LICENSE.txt. _coverage.json carries both measures.

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
from dataclasses import dataclass, field, replace
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


def memory(label: str, con: duckdb.DuckDBPyConnection | None = None) -> None:
    """Print the process's resident memory now and at its peak, and DuckDB's
    own share: the build's peak on the 16 GB runner was 5 GB over DuckDB's
    cap, and this says which step holds it."""
    now = peak = None
    try:
        status = Path("/proc/self/status").read_text()
        now = int(re.search(r"VmRSS:\s+(\d+)", status).group(1)) / 2**20
        peak = int(re.search(r"VmHWM:\s+(\d+)", status).group(1)) / 2**20
    except OSError:   # macOS: only the peak, in bytes
        import resource
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**30
    duck = con.execute("SELECT sum(memory_usage_bytes) FROM duckdb_memory()").fetchone()[0] \
        if con else None
    print(f"  [memory] {label}: " + ", ".join(f"{k} {v:.1f} GB" for k, v in (
        ("rss", now), ("peak", peak), ("duckdb", duck / 2**30 if duck is not None else None))
        if v is not None), flush=True)


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

-- The feature's name: name, else its lifecycle form, else the tags that name
-- the feature itself (not its address, operator, design, oil field or a
-- former name), else its only name:<lang>.
CREATE OR REPLACE MACRO best_name(t) AS coalesce(
    t['name'], t['construction:name'], t['proposed:name'], t['planned:name'],
    t['disused:name'], t['abandoned:name'],
    t['official_name'], t['name:en'], t['short_name'], t['alt_name'], t['loc_name'],
    t['seamark:name'], t['substation:name'], t['site_name'],
    CASE WHEN cardinality(names_of(t)) = 1 THEN map_values(names_of(t))[1] END);

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
              ("field_name", s("field_name")),
              ("length_km", "length_km(geometry)"),
          ]),
    Layer("petroleum_site", "petroleum", "industrial",
          """tags['industrial'] IN ('oil', 'fracking', 'oil_storage', 'petroleum_terminal',
                 'hydrocarbons', 'oil sands', 'oil_sands', 'gas', 'gas_storage',
                 'natural_gas', 'wellsite', 'well_cluster', 'refinery')
             OR tags['pipeline'] = 'substation'""", [
              ("industrial", s("industrial")),
              ("field_name", s("field_name")),
              ("area_m2", "area_m2(geometry)"),
          ]),
    Layer("petroleum_well", "petroleum", "man_made",
          "lc_val(tags, 'man_made') IN ('petroleum_well', 'oil_well')", [
              ("substance", s("substance")),
              ("field_name", s("field_name")),
          ]),
    Layer("offshore_platform", "petroleum", "man_made",
          "lc_val(tags, 'man_made') = 'offshore_platform'", [
              ("field_name", s("field_name")),
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
    ("name", "best_name(tags)"),
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


def assign_region(con: duckdb.DuckDBPyConnection, table: str,
                  source_id: bool = False) -> None:
    """Add `country` and `state` to a source table: GAUL L1 on land, else
    EEZ, else _intl, and with `source_id` an empty source_id column (see
    add_rows). Each step joins plain tables so DuckDB plans a SPATIAL_JOIN."""
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
               {", NULL::VARCHAR AS source_id" if source_id else ""}
        FROM (SELECT t.*, coalesce(hit.region, '_intl/_intl') AS r
              FROM {table} t LEFT JOIN hit USING (osm_type, osm_id)) t""")


# --------------------------------------------------------------------------
# Rows from other sources
#
# Each source step matches its records against OSM and builds a table of the
# ones OSM lacks, shaped like the source table of their layer: osm_type is
# the source's name (eia, fcc, ...) and osm_id a row number, so the pair
# stays a key while regions are assigned; source_id is the record's ID in
# its source; tags are the tags a mapper would use, from which the layer's
# columns derive. layer_select turns osm_type into `origin` and drops the
# rest. OSM values are never changed: sources only add rows.

IS_OSM = "osm_type IN ('node', 'way', 'relation')"
SOURCE_TABLES = ("feat", "circuit", "substation", "generator", "plant")


def add_rows(con: duckdb.DuckDBPyConnection, target: str, rows: str) -> int:
    """Assign regions to the source rows in `rows`, append them to `target`
    (feat, generator, substation, plant) and drop `rows`."""
    assign_region(con, rows)
    n = con.execute(f"SELECT count(*) FROM {rows}").fetchone()[0]
    con.execute(f"INSERT INTO {target} BY NAME SELECT * FROM {rows}")
    con.execute(f"DROP TABLE {rows}")
    return n


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


def uspvdb_outlines(con: duckdb.DuckDBPyConnection, path: str) -> None:
    """Table `pv_outline(eia_id, geometry)` from the USGS/LBNL US Large-Scale
    Solar Photovoltaic Database (a .geojson, or the .zip USGS publishes)."""
    src = path
    if path.endswith(".zip"):
        import zipfile
        member = next(n for n in zipfile.ZipFile(path).namelist() if n.endswith(".geojson"))
        src = f"/vsizip/{path}/{member}"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE pv_outline AS
        SELECT eia_id, ST_MakeValid(geom) AS geometry FROM ST_Read('{src}')
        WHERE eia_id IS NOT NULL""")
    if not con.execute("SELECT count(*) FROM pv_outline").fetchone()[0]:
        sys.exit(f"{path}: no USPVDB outline read")


def add_eia_plants(con: duckdb.DuckDBPyConnection, eia_xlsx: str,
                   uspvdb: str | None = None) -> dict:
    """Add to `plant` the operating US plants EIA-860M reports and OSM lacks.

    A plant counts as present when OSM has it by ref:US:EIA, holds its point
    in a plant polygon, has a plant within 2 km, or has generators within
    500 m (small solar is often mapped as panels only): a borderline plant
    stays OSM-only rather than risk a duplicate. The rest become point rows
    with the tags their EIA record implies, origin = 'eia'. With `uspvdb`, a solar plant USPVDB has (joined on its
    EIA plant ID) gets USPVDB's outline instead of EIA's point. Returns the
    coverage figures published in _coverage.json.
    """
    con.execute("INSTALL excel; LOAD excel;")
    if uspvdb:
        uspvdb_outlines(con, uspvdb)
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
        SELECT 'eia' AS osm_type, e.plant_id AS osm_id, e.plant_id::VARCHAR AS source_id,
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
               {"coalesce(pv.geometry, ST_Point(e.lon, e.lat))" if uspvdb else "ST_Point(e.lon, e.lat)"}
                   AS geometry,
               NULL::BIGINT AS generator_count, NULL::DOUBLE AS generator_output_mw,
               e.mw AS output_mw_estimated, 'tagged' AS output_basis
        FROM eia e JOIN eia_how h USING (plant_id)
        {"LEFT JOIN pv_outline pv ON pv.eia_id = e.plant_id" if uspvdb else ""}
        WHERE h.how = 'added'
    """)
    outlined = con.execute("""
        SELECT count(*), coalesce(sum(output_mw_estimated), 0)
        FROM eia_add WHERE ST_Dimension(geometry) = 2""").fetchone()
    add_rows(con, "plant", "eia_add")
    tiers = {how: {"plants": n, "mw": round(mw, 1)} for how, n, mw in con.execute(
        "SELECT how, count(*), sum(mw) FROM eia_how GROUP BY 1").fetchall()}
    total = con.execute("SELECT count(*), sum(mw) FROM eia").fetchone()
    return {"source": Path(eia_xlsx).name, "eia_plants": total[0],
            "eia_mw": round(total[1], 1), "tiers": tiers,
            "added_with_outline": {"source": Path(uspvdb).name if uspvdb else None,
                                   "plants": outlined[0], "mw": round(outlined[1], 1)}}


# --------------------------------------------------------------------------
# NSTA: UK oil and gas OSM does not have yet, published apart
#
# The North Sea Transition Authority's open data comes under its User
# Agreement, which allows publishing and adapting it but exploiting it
# non-commercially only. The ODbL grants commercial reuse, so these rows
# cannot go into the layers: they are written to <repository>/nsta/, one file
# per layer with the layer's exact schema (origin = 'nsta'), under NSTA's
# terms. A UNION ALL BY NAME with the layer completes it. Permission to
# merge them was requested on 2026-09-27.

NSTA_READ = {
    "surface": "UKCS offshore infrastructure surface points (WGS84).shp",
    "pipeline": "UKCS offshore infrastructure pipeline linear (WGS84).shp",
    "well_offshore": "UKCS offshore wellbore top holes (WGS84).shp",
    "well_onshore": "UK England petroleum wells top holes (WGS84).shp",
}
NSTA_LAYERS = {"petroleum_well": "nsta_well", "offshore_platform": "nsta_platform",
               "pipeline": "nsta_pipeline"}
NSTA_SUBSTANCE = {"GAS": "gas", "OIL": "oil", "MIXED HYDROCARBONS": "hydrocarbons",
                  "CONDENSATE": "condensate", "WATER": "water", "SEAWATER": "water",
                  "METHANOL": "methanol", "CHEMICAL": "chemicals"}
NSTA_LICENCE = """\
UK oil and gas infrastructure OpenStreetMap does not have, from the NSTA
=======================================================================

Contains information provided by the North Sea Transition Authority and/or
other third parties.

Source: NSTA Open Data, offshore infrastructure and wells ({source}),
https://opendata-nstauthority.hub.arcgis.com/, downloaded {date}.

Licence: NSTA User Agreement (June 2023),
https://www.nstauthority.co.uk/media/u51lhvio/nsta-user-agreeement-june-2023.pdf
You may copy, publish, distribute, transmit and adapt this information and
exploit it NON-COMMERCIALLY, with the attribution statement above. It is not
under the ODbL, unlike the layers in ../latest/, and is kept apart from them
for that reason. For other uses, ask the NSTA (correspondence@nstauthority.co.uk).

Each file has the schema of the layer of the same name in ../latest/
(origin = 'nsta', no osm_id and no tags) and holds only what OSM lacks: how
each NSTA feature was matched is in ../latest/_coverage.json (nsta).
Abandoned wells, platforms and pipelines are in abandoned/, as in ../latest/.
"""


def nsta_rows(con: duckdb.DuckDBPyConnection, zip_path: str) -> dict:
    """Tables nsta_well, nsta_platform and nsta_pipeline: the UK wells,
    platforms and pipelines the NSTA reports and OSM lacks, as rows with the
    tags a mapper would use (osm_type NULL). A well counts as present when
    OSM has its registration number (ref_no, from the old DECC import) or a
    well within 25 m of its top hole; a platform, when OSM has one within
    500 m; a pipeline, when OSM has its PL number, or pipelines within 250 m
    along half its length. Returns the coverage figures."""
    src = {k: f"/vsizip/{zip_path}/{v}" for k, v in NSTA_READ.items()}
    metres = "'EPSG:4326', 'EPSG:3035', always_xy := true"
    date = "strftime(TRY_STRPTIME(left({}, 8), '%Y%m%d'), '%Y-%m-%d')"

    def lc_key(key: str) -> str:
        return f"CASE WHEN state = 'active' THEN '{key}' ELSE state || ':{key}' END"

    def tags(*pairs: tuple[str, str]) -> str:
        return ("map_from_entries(list_filter(["
                + ", ".join(f"struct_pack(k := {k}, v := {v})" for k, v in pairs)
                + "], x -> x.v IS NOT NULL))")

    # Wells: every wellbore, onshore England and offshore, by registration number.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE nw AS
        SELECT WELLREGNO AS id, TDOPERATOR AS operator, {date.format('SPUDDATE')} AS spud,
               CASE WHEN ORIGINSTAT = 'Planned' THEN 'proposed'
                    WHEN ORIGINSTAT = 'Decommissioned' OR WELLOPSTAT = 'Decomissioned' THEN 'abandoned'
                    WHEN WELLOPSTAT = 'Constructing' THEN 'construction'
                    WHEN WELLOPSTAT = 'Suspended' OR ORIGINSTAT = 'Derogated' THEN 'disused'
                    ELSE 'active' END AS state,
               geom AS geometry, ST_Transform(geom, {metres}) AS g
        FROM (SELECT * FROM ST_Read('{src["well_offshore"]}')
              UNION ALL BY NAME SELECT * FROM ST_Read('{src["well_onshore"]}'))
        WHERE WELLREGNO IS NOT NULL AND geom IS NOT NULL""")
    # One OSM well often lists its sidetracks: ref_no=204/22-2;204/22-2Z.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ow AS
        SELECT upper(replace(unnest(string_split(coalesce(
                   tags['ref_no'], tags['ref:GB:decc'], tags['ref'], ''), ';')), ' ', '')) AS ref,
               ST_Transform(ST_PointOnSurface(geometry), {metres}) AS g
        FROM feat WHERE country = 'GB'
          AND lc_val(tags, 'man_made') IN ('petroleum_well', 'oil_well')""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE nw_how AS
        WITH ref AS (SELECT DISTINCT n.id FROM nw n
                     JOIN ow ON ow.ref = upper(replace(n.id, ' ', ''))),
             near AS (SELECT DISTINCT n.id FROM nw n JOIN ow ON ST_DWithin(ow.g, n.g, 25))
        SELECT n.id, CASE WHEN n.id IN (SELECT * FROM ref) THEN 'ref'
                          WHEN n.id IN (SELECT * FROM near) THEN 'near'
                          ELSE 'added' END AS how
        FROM nw n""")
    con.execute(f"""
        CREATE OR REPLACE TABLE nsta_well AS
        SELECT 'nsta' AS osm_type, row_number() OVER () AS osm_id, id AS source_id,
               {tags((lc_key('man_made'), "'petroleum_well'"), ("'ref'", "id"),
                     ("'operator'", "operator"), ("'start_date'", "spud"))} AS tags,
               geometry
        FROM nw JOIN nw_how h USING (id) WHERE h.how = 'added'""")

    # Platforms and floating production units.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE np AS
        SELECT FEATURE_ID AS id, NAME AS name, REP_GROUP AS operator,
               {date.format('START_DATE')} AS started,
               CASE STATUS WHEN 'NOT IN USE' THEN 'disused'
                           WHEN 'ABANDONED' THEN 'abandoned' ELSE 'active' END AS state,
               geom AS geometry, ST_Transform(geom, {metres}) AS g
        FROM ST_Read('{src["surface"]}')
        WHERE INF_TYPE IN ('PLATFORM', 'FPSO', 'FSO') AND STATUS <> 'REMOVED'""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE op AS
        SELECT ST_Transform(ST_PointOnSurface(geometry), {metres}) AS g FROM feat
        WHERE country = 'GB' AND lc_val(tags, 'man_made') = 'offshore_platform'""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE np_how AS
        WITH near AS (SELECT DISTINCT n.id FROM np n JOIN op ON ST_DWithin(op.g, n.g, 500))
        SELECT n.id, CASE WHEN n.id IN (SELECT * FROM near) THEN 'near' ELSE 'added' END AS how
        FROM np n""")
    con.execute(f"""
        CREATE OR REPLACE TABLE nsta_platform AS
        SELECT 'nsta' AS osm_type, row_number() OVER () AS osm_id, id::VARCHAR AS source_id,
               {tags((lc_key('man_made'), "'offshore_platform'"), ("'name'", "name"),
                     ("'operator'", "operator"), ("'start_date'", "started"))} AS tags,
               geometry
        FROM np JOIN np_how h USING (id) WHERE h.how = 'added'""")

    # Pipelines proper (not umbilicals, risers, rock dumps or mattresses).
    substance = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in NSTA_SUBSTANCE.items())
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE nl AS
        SELECT row_number() OVER () AS rid, NSTAPIPNO AS id, PIPE_NAME AS name,
               CASE FLUID {substance} END AS substance,
               CASE WHEN DIAMETERMM > 0 THEN round(DIAMETERMM)::INTEGER::VARCHAR END AS diameter,
               {date.format('START_DATE')} AS started,
               CASE STATUS WHEN 'NOT IN USE' THEN 'disused' WHEN 'ABANDONED' THEN 'abandoned'
                           WHEN 'PRECOMMISSIONED' THEN 'construction'
                           WHEN 'PROPOSED' THEN 'proposed' ELSE 'active' END AS state,
               geom AS geometry, ST_Transform(geom, {metres}) AS g
        FROM ST_Read('{src["pipeline"]}')
        WHERE INF_TYPE = 'PIPELINE' AND STATUS <> 'REMOVED' AND geom IS NOT NULL""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ol AS
        SELECT upper(replace(unnest(string_split(coalesce(tags['ref'], ''), ';')), ' ', '')) AS ref,
               ST_Transform(geometry, {metres}) AS g
        FROM feat WHERE country = 'GB' AND lc_val(tags, 'man_made') = 'pipeline'""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE nl_cov AS
        SELECT n.rid, ST_Length(ST_Intersection(any_value(n.g), ST_Union_Agg(ST_Buffer(o.g, 250))))
                          / nullif(ST_Length(any_value(n.g)), 0) AS share
        FROM nl n JOIN ol o ON ST_DWithin(n.g, o.g, 250) GROUP BY n.rid""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE nl_how AS
        WITH ref AS (SELECT DISTINCT n.rid FROM nl n
                     JOIN ol ON ol.ref = upper(replace(n.id, ' ', '')))
        SELECT n.rid, ST_Length(n.g) / 1000 AS km, CASE
            WHEN n.rid IN (SELECT * FROM ref) THEN 'ref'
            WHEN c.share >= 0.5 THEN 'near'
            ELSE 'added' END AS how
        FROM nl n LEFT JOIN nl_cov c USING (rid)""")
    con.execute(f"""
        CREATE OR REPLACE TABLE nsta_pipeline AS
        SELECT 'nsta' AS osm_type, row_number() OVER () AS osm_id, id AS source_id,
               {tags((lc_key('man_made'), "'pipeline'"), ("'ref'", "id"), ("'name'", "name"),
                     ("'substance'", "substance"), ("'diameter'", "diameter"),
                     ("'start_date'", "started"))} AS tags,
               geometry
        FROM nl JOIN nl_how h USING (rid) WHERE h.how = 'added'""")

    for table in NSTA_LAYERS.values():
        assign_region(con, table)

    def measure(read: str, how: str, km: bool = False) -> dict:
        n = con.execute(f"SELECT count(*) FROM {read}").fetchone()[0]
        if not n:
            sys.exit(f"{zip_path}: nothing read for {read}")
        rows = con.execute(f"SELECT how, count(*){', round(sum(km))' if km else ''} "
                           f"FROM {how} GROUP BY 1 ORDER BY 1").fetchall()
        return {"nsta": n, "tiers": {r[0]: {"features": r[1], **({"km": r[2]} if km else {})}
                                     for r in rows}}

    return {"source": Path(zip_path).name,
            "licence": "NSTA User Agreement, non-commercial: what OSM lacks is in "
                       "../nsta/, not in these layers",
            "petroleum_well": measure("nw", "nw_how"),
            "offshore_platform": measure("np", "np_how"),
            "pipeline": measure("nl", "nl_how", km=True)}


# --------------------------------------------------------------------------
# USWTDB: US wind turbines OSM does not have yet
#
# The U.S. Wind Turbine Database (USGS, LBNL, American Clean Power) lists
# every utility-scale turbine built in the US and its territories, located on
# imagery; decommissioned turbines are removed from each release, so every
# record is standing. Public domain (U.S. federal government work).


# The regions USWTDB covers: states, Puerto Rico and Guam, on land and at
# sea (Block Island, Coastal Virginia, Vineyard Wind are offshore).
USWTDB_COUNTRIES = "('US', 'PR', 'GU')"
USWTDB_NEAR_M = 100


def uswtdb_rows(con: duckdb.DuckDBPyConnection, path: str) -> dict:
    """Table uswtdb_add: the turbines USWTDB has and OSM lacks, as generator
    rows with the tags a mapper would use (osm_type 'uswtdb', source_id its
    case_id). A turbine counts as present when OSM has a wind generator, or
    a generator of no stated source, within USWTDB_NEAR_M of it: OSM has no
    key for USWTDB's IDs, so position is the only link. Returns the coverage
    figures. A missing turbine USWTDB itself has not seen on imagery
    (t_conf_loc 1: the image predates it, or shows none) is counted as
    `unverified`, not added."""
    src = path
    if path.endswith(".zip"):
        import zipfile
        # DuckDB reads no zip: extract the one CSV beside the archive.
        member = next(n for n in zipfile.ZipFile(path).namelist()
                      if n.startswith("uswtdb") and n.endswith(".csv"))
        zipfile.ZipFile(path).extract(member, Path(path).parent)
        src = str(Path(path).parent / member)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE wt AS
        SELECT case_id::VARCHAR AS id, p_name, p_year, t_manu, t_model,
               t_cap, t_hh, t_rd, t_ttlh, t_offshore, t_conf_loc,
               ST_Point(xlong, ylat) AS geometry
        FROM read_csv('{src}', header = true, all_varchar = false,
                      types = {{'case_id': 'BIGINT', 'p_year': 'INTEGER', 't_conf_loc': 'INTEGER',
                               't_cap': 'DOUBLE', 't_hh': 'DOUBLE', 't_rd': 'DOUBLE',
                               't_ttlh': 'DOUBLE', 'xlong': 'DOUBLE', 'ylat': 'DOUBLE'}})
        WHERE case_id IS NOT NULL AND xlong IS NOT NULL AND ylat IS NOT NULL""")
    if not con.execute("SELECT count(*) FROM wt").fetchone()[0]:
        sys.exit(f"{path}: no USWTDB turbine read")
    # Only the turbines the build covers (see add_eia_plants).
    con.execute(f"""
        DELETE FROM wt WHERE NOT EXISTS (
            SELECT 1 FROM land WHERE split_part(region, '/', 1) IN {USWTDB_COUNTRIES}
              AND ST_Contains(geometry, wt.geometry))
          AND NOT EXISTS (
            SELECT 1 FROM sea WHERE split_part(region, '/', 1) IN {USWTDB_COUNTRIES}
              AND ST_Contains(sea.geometry, wt.geometry))""")
    albers = "'EPSG:4326', 'EPSG:5070', always_xy := true"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE wt_m AS
        SELECT id, t_cap, t_conf_loc, ST_Transform(geometry, {albers}) AS g FROM wt""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE og AS
        SELECT ST_Transform(ST_PointOnSurface(geometry), {albers}) AS g
        FROM generator WHERE country IN {USWTDB_COUNTRIES}
          AND (coalesce(first_semi(lc_val(tags, 'generator:source')), 'wind') = 'wind'
               OR lc_val(tags, 'generator:method') = 'wind_turbine')""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE wt_how AS
        WITH near AS (SELECT DISTINCT w.id FROM wt_m w
                      JOIN og ON ST_DWithin(og.g, w.g, {USWTDB_NEAR_M}))
        SELECT w.id, w.t_cap / 1000 AS mw,
               CASE WHEN w.id IN (SELECT * FROM near) THEN 'near'
                    WHEN w.t_conf_loc = 1 THEN 'unverified'
                    ELSE 'added' END AS how
        FROM wt_m w""")

    def num(col: str) -> str:
        # 105.0 -> '105', 17.5 -> '17.5'; USWTDB leaves unknowns empty.
        return f"CASE WHEN {col} > 0 THEN regexp_replace(round({col}, 2)::VARCHAR, '\\.0+$', '') END"

    tags = [("'power'", "'generator'"), ("'generator:source'", "'wind'"),
            ("'generator:method'", "'wind_turbine'"),
            ("'generator:type'", "'horizontal_axis'"),
            ("'generator:output:electricity'",
             f"CASE WHEN t_cap > 0 THEN {num('t_cap / 1000')} || ' MW' END"),
            ("'height:hub'", num("t_hh")), ("'rotor:diameter'", num("t_rd")),
            ("'height'", num("t_ttlh")),
            ("'manufacturer'", "nullif(t_manu, 'missing')"),
            ("'model'", "nullif(t_model, 'missing')"),
            ("'start_date'", "p_year::VARCHAR")]
    con.execute(f"""
        CREATE OR REPLACE TABLE uswtdb_add AS
        SELECT 'uswtdb' AS osm_type, row_number() OVER ()::BIGINT AS osm_id, id AS source_id,
               map_from_entries(list_filter([{', '.join(
                   f"struct_pack(k := {k}, v := {v})" for k, v in tags)}],
                   x -> x.v IS NOT NULL)) AS tags,
               geometry, FALSE AS solar,
               CASE WHEN t_cap > 0 THEN t_cap / 1000 END AS output_mw_estimated,
               CASE WHEN t_cap > 0 THEN 'tagged' END AS output_basis
        FROM wt JOIN wt_how h USING (id) WHERE h.how = 'added'""")
    tiers = {how: {"features": n, "mw": round(mw or 0, 1)} for how, n, mw in con.execute(
        "SELECT how, count(*), sum(mw) FROM wt_how GROUP BY 1 ORDER BY 1").fetchall()}
    total = con.execute("SELECT count(*) FROM wt").fetchone()[0]
    return {"source": Path(src).name,
            "power_generator": {"uswtdb": total, "tiers": tiers}}


# --------------------------------------------------------------------------
# BOEM/BSEE: US offshore wells, platforms and pipelines OSM does not have yet
#
# The Bureau of Safety and Environmental Enforcement's data center publishes
# the Outer Continental Shelf's boreholes and platform structures as raw
# delimited files, and BOEM/BSEE's mapping site its pipeline segments as a
# shapefile, all refreshed daily (pipelines monthly). US federal government
# works, public domain: they go into the layers, origin = 'boem'.
#
# Coordinates are NAD27 (the shapefile's .prj; NAD_YEAR_CD = 27 on the
# structures, the borehole file's documented datum): transformed to WGS84,
# a shift of about 30 m in the Gulf.

BOEM_READ = {
    # https://www.data.bsee.gov/Well/Files/BoreholeRawData.zip
    "borehole": "BoreholeRawData/mv_boreholes_all.txt",
    # https://www.data.bsee.gov/Platform/Files/PlatStrucRawData.zip
    "structure": "PlatStrucRawData/mv_platstruc_structures.txt",
    # https://www.data.boem.gov/Mapping/Files/ppl_arcs.zip
    "pipeline": "ppl_arcs.shp",
}
BOEM_LAYERS = {"petroleum_well": "boem_well", "offshore_platform": "boem_platform",
               "pipeline": "boem_pipeline"}

# Borehole status (BOREHOLE_STAT_CD) of a well's latest borehole -> lifecycle.
# Left out: PA (permanently abandoned: plugged, casing cut below the mudline,
# nothing left), and boreholes never drilled (APD approved, AST approved
# sidetrack, CNL cancelled), which are skipped when picking the latest.
BOEM_WELL_STATE = {"COM": "active", "ST": "active", "TA": "disused",
                   "DSI": "construction", "DRL": "construction", "VCW": "active"}
# Pipeline status (STATUS_COD) -> lifecycle. Left out: REM removed, PREM
# (partially removed), CNCL cancelled. Abandoned in place (ABN, A/C) is still
# on the seabed, as the NSTA's ABANDONED rows are kept.
BOEM_PIPE_STATE = {"ACT": "active", "OUT": "disused", "PABN": "disused",
                   "ABN": "abandoned", "A/C": "abandoned", "COMB": "active",
                   "PROP": "proposed"}
# Product code (PROD_CODE) -> substance. Umbilicals and power cables (UMB*,
# UBEH, CBL*) are not pipelines, as in NSTA's INF_TYPE = 'PIPELINE'.
BOEM_PRODUCT = {"GAS": "gas", "BLKG": "gas", "G/C": "gas", "CSNG": "gas", "LIFT": "gas",
                "SPLY": "gas", "FLG": "gas", "GASH": "gas", "BLGH": "gas", "G/CH": "gas",
                "OIL": "oil", "BLKO": "oil", "OILH": "oil", "BLOH": "oil", "O/W": "oil",
                "G/O": "hydrocarbons", "G/OH": "hydrocarbons", "COND": "condensate",
                "H2O": "water", "PWTR": "water", "METH": "methanol", "CHEM": "chemicals",
                "ACID": "chemicals", "AIR": "air", "SULF": "sulphur"}
BOEM_NOT_PIPE = ("UMB%", "UBEH", "CBL%")


def boem_rows(con: duckdb.DuckDBPyConnection, folder: str) -> dict:
    """Tables boem_well, boem_platform and boem_pipeline: the US OCS wells,
    platforms and pipelines BSEE/BOEM report and OSM lacks, as rows with the
    tags a mapper would use (osm_type 'boem', source_id their BSEE ID).
    `folder` holds the three downloads, unzipped (BOEM_READ).

    A well counts as present when OSM has its API number (ref:US:API or ref,
    10 digits) or a well within 25 m; a platform, when OSM has one within
    500 m; a pipeline segment, when OSM has its right-of-way number
    (ref:US:BOEM:ROW, what mappers tag), or pipelines within 250 m along
    half its length. Returns the coverage figures."""
    paths = {k: f"{folder}/{v}" for k, v in BOEM_READ.items()}
    for k, v in paths.items():
        if not Path(v).exists():
            sys.exit(f"BOEM/BSEE: {v} missing")
    metres = "'EPSG:4267', 'EPSG:5070', always_xy := true"
    osm_m = "'EPSG:4326', 'EPSG:5070', always_xy := true"
    wgs = "'EPSG:4267', 'EPSG:4326', always_xy := true"
    date = "strftime(TRY_STRPTIME({}, '%m/%d/%Y'), '%Y-%m-%d')"

    def lc_key(key: str) -> str:
        return f"CASE WHEN state = 'active' THEN '{key}' ELSE state || ':{key}' END"

    def tags(*pairs: tuple[str, str]) -> str:
        return ("map_from_entries(list_filter(["
                + ", ".join(f"struct_pack(k := {k}, v := {v})" for k, v in pairs)
                + "], x -> x.v IS NOT NULL))")

    def read_txt(key: str) -> str:
        return f"read_csv('{paths[key]}', all_varchar = true, header = true)"

    # Wells: one per API well number (10 digits; the last two of the 12 are
    # the borehole), placed and dated by its latest drilled borehole.
    states = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in BOEM_WELL_STATE.items())
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE bw AS
        WITH b AS (
            SELECT left(API_WELL_NUMBER, 10) AS id, API_WELL_NUMBER AS api12,
                   BOREHOLE_STAT_CD AS stat, replace(COMPANY_NAME, '&amp;', '&') AS operator,
                   TRY_CAST(SURF_LATITUDE AS DOUBLE) AS lat,
                   TRY_CAST(SURF_LONGITUDE AS DOUBLE) AS lon,
                   {date.format('WELL_SPUD_DATE')} AS spud
            FROM {read_txt('borehole')}
            WHERE BOREHOLE_STAT_CD NOT IN ('APD', 'AST', 'CNL')
        ), w AS (
            SELECT id, arg_max(stat, api12) AS stat, arg_max(operator, api12) AS operator,
                   arg_max(lat, api12) AS lat, arg_max(lon, api12) AS lon, min(spud) AS spud
            FROM b GROUP BY id
        )
        SELECT id, operator, spud, CASE stat {states} END AS state,
               ST_Transform(ST_Point(lon, lat), {wgs}) AS geometry,
               ST_Transform(ST_Point(lon, lat), {metres}) AS g
        FROM w WHERE stat IN ({', '.join(f"'{k}'" for k in BOEM_WELL_STATE)})
          AND lat IS NOT NULL AND lon IS NOT NULL""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ow AS
        SELECT regexp_replace(coalesce(tags['ref:US:API'], tags['ref'], ''), '[^0-9]', '', 'g') AS ref,
               ST_Transform(ST_PointOnSurface(geometry), {osm_m}) AS g
        FROM feat WHERE country = 'US'
          AND lc_val(tags, 'man_made') IN ('petroleum_well', 'oil_well')""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE bw_how AS
        WITH ref AS (SELECT DISTINCT b.id FROM bw b JOIN ow ON left(ow.ref, 10) = b.id),
             near AS (SELECT DISTINCT b.id FROM bw b JOIN ow ON ST_DWithin(ow.g, b.g, 25))
        SELECT b.id, CASE WHEN b.id IN (SELECT * FROM ref) THEN 'ref'
                                   WHEN b.id IN (SELECT * FROM near) THEN 'near'
                                   ELSE 'added' END AS how
        FROM bw b""")
    con.execute(f"""
        CREATE OR REPLACE TABLE boem_well AS
        SELECT 'boem' AS osm_type, row_number() OVER ()::BIGINT AS osm_id, id AS source_id,
               {tags((lc_key('man_made'), "'petroleum_well'"), ("'ref:US:API'", "id"),
                     ("'operator'", "operator"), ("'start_date'", "spud"))} AS tags,
               geometry
        FROM bw JOIN bw_how h USING (id) WHERE h.how = 'added'""")

    # Platforms: every structure installed and not removed (fixed platforms,
    # caissons, well protectors, spars, TLPs, FPSOs), named as BSEE does:
    # area, block, structure name (EI 119 F).
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE bp AS
        SELECT COMPLEX_ID_NUM || '/' || STRUCTURE_NUMBER AS id,
               AREA_CODE || ' ' || trim(BLOCK_NUMBER) || ' ' || trim(STRUCTURE_NAME) AS name,
               replace(BUS_ASC_NAME, '&amp;', '&') AS operator, {date.format('INSTALL_DATE')} AS installed,
               'active' AS state,
               ST_Transform(ST_Point(TRY_CAST(LONGITUDE AS DOUBLE), TRY_CAST(LATITUDE AS DOUBLE)), {wgs}) AS geometry,
               ST_Transform(ST_Point(TRY_CAST(LONGITUDE AS DOUBLE), TRY_CAST(LATITUDE AS DOUBLE)), {metres}) AS g
        FROM {read_txt('structure')}
        WHERE REMOVAL_DATE IS NULL AND INSTALL_DATE IS NOT NULL
          AND TRY_CAST(LATITUDE AS DOUBLE) IS NOT NULL""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE op AS
        SELECT ST_Transform(ST_PointOnSurface(geometry), {osm_m}) AS g FROM feat
        WHERE country = 'US' AND lc_val(tags, 'man_made') = 'offshore_platform'""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE bp_how AS
        WITH near AS (SELECT DISTINCT b.id FROM bp b JOIN op ON ST_DWithin(op.g, b.g, 500))
        SELECT b.id, CASE WHEN b.id IN (SELECT * FROM near) THEN 'near' ELSE 'added' END AS how
        FROM bp b""")
    con.execute(f"""
        CREATE OR REPLACE TABLE boem_platform AS
        SELECT 'boem' AS osm_type, row_number() OVER ()::BIGINT AS osm_id, id AS source_id,
               {tags((lc_key('man_made'), "'offshore_platform'"), ("'name'", "name"),
                     ("'operator'", "operator"), ("'start_date'", "installed"))} AS tags,
               geometry
        FROM bp JOIN bp_how h USING (id) WHERE h.how = 'added'""")

    # Pipeline segments. PPL_SIZE_C is the nominal diameter in inches
    # ('06', or a range '04-06'); OSM's diameter is millimetres.
    pstates = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in BOEM_PIPE_STATE.items())
    substance = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in BOEM_PRODUCT.items())
    not_pipe = " AND ".join(f"PROD_CODE NOT LIKE '{p}'" for p in BOEM_NOT_PIPE)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE bl AS
        SELECT SEGMENT_NU::VARCHAR AS id, nullif(trim(ROW_NUMBER), '') AS row_no,
               replace(SDE_COMPAN, '&amp;', '&') AS operator, CASE PROD_CODE {substance} END AS substance,
               CASE WHEN regexp_full_match(trim(PPL_SIZE_C), '[0-9]+')
                    THEN round(trim(PPL_SIZE_C)::INTEGER * 25.4)::INTEGER::VARCHAR END AS diameter,
               CASE STATUS_COD {pstates} END AS state,
               ST_Transform(geom, {wgs}) AS geometry, ST_Transform(geom, {metres}) AS g
        FROM ST_Read('{paths['pipeline']}')
        WHERE STATUS_COD IN ({', '.join(f"'{k}'" for k in BOEM_PIPE_STATE)})
          AND {not_pipe} AND geom IS NOT NULL""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ol AS
        SELECT upper(replace(unnest(string_split(coalesce(tags['ref:US:BOEM:ROW'], ''), ';')), ' ', '')) AS ref,
               ST_Transform(geometry, {osm_m}) AS g
        FROM feat WHERE country = 'US' AND lc_val(tags, 'man_made') = 'pipeline'""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE bl_cov AS
        SELECT b.id, ST_Length(ST_Intersection(any_value(b.g), ST_Union_Agg(ST_Buffer(o.g, 250))))
                         / nullif(ST_Length(any_value(b.g)), 0) AS share
        FROM bl b JOIN ol o ON ST_DWithin(b.g, o.g, 250) GROUP BY b.id""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE bl_how AS
        WITH ref AS (SELECT DISTINCT b.id FROM bl b JOIN ol ON ol.ref = upper(b.row_no))
        SELECT b.id, ST_Length(b.g) / 1000 AS km, CASE
            WHEN b.id IN (SELECT * FROM ref) THEN 'ref'
            WHEN c.share >= 0.5 THEN 'near'
            ELSE 'added' END AS how
        FROM bl b LEFT JOIN bl_cov c USING (id)""")
    con.execute(f"""
        CREATE OR REPLACE TABLE boem_pipeline AS
        SELECT 'boem' AS osm_type, row_number() OVER ()::BIGINT AS osm_id, id AS source_id,
               {tags((lc_key('man_made'), "'pipeline'"), ("'location'", "'underwater'"),
                     ("'ref:US:BOEM:ROW'", "row_no"), ("'ref:US:BOEM:segment'", "id"),
                     ("'operator'", "operator"), ("'substance'", "substance"),
                     ("'diameter'", "diameter"))} AS tags,
               geometry
        FROM bl JOIN bl_how h USING (id) WHERE h.how = 'added'""")

    def measure(read: str, how: str, km: bool = False) -> dict:
        n = con.execute(f"SELECT count(*) FROM {read}").fetchone()[0]
        if not n:
            sys.exit(f"BOEM/BSEE: nothing read for {read}")
        rows = con.execute(f"SELECT how, count(*){', round(sum(km))' if km else ''} "
                           f"FROM {how} GROUP BY 1 ORDER BY 1").fetchall()
        return {"bsee": n, "tiers": {r[0]: {"features": r[1], **({"km": r[2]} if km else {})}
                                     for r in rows}}

    return {"source": "BSEE boreholes and platform structures, BOEM pipeline segments",
            "licence": "US federal government work, public domain",
            "petroleum_well": measure("bw", "bw_how"),
            "offshore_platform": measure("bp", "bp_how"),
            "pipeline": measure("bl", "bl_how", km=True)}


# --------------------------------------------------------------------------
# FCC ASR: US communication towers OSM does not have yet
#
# The FCC's Antenna Structure Registration (ASR) lists every structure that
# needs FAA notice: over 200 ft (61 m) above ground, or near an airport. It is
# the reference for tall US masts, not for small cell poles or rooftops.
# Source: the weekly complete file of the FCC Universal Licensing System,
# https://data.fcc.gov/download/pub/uls/complete/r_tower.zip (~38 MB, rebuilt
# every Sunday), pipe-delimited .dat files without header, laid out as in
# the FCC's "ASR Public Access Database Definitions". A U.S. federal
# government work, so public domain (17 U.S.C. 105); the FCC asks for no more
# than a source credit.
#
# Record layouts used (0-based column numbers, as DuckDB names them):
#   RA (registration)  3 registration number, 8 status code, 12 date
#                      constructed, 13 date dismantled, 30 overall height
#                      above ground (m, with appurtenances), 32 structure type
#   CO (coordinates)   3 registration number, 5 coordinate type (T = the
#                      structure; A = the array it belongs to), 6-9 latitude
#                      d/m/s/N-S, 11-14 longitude d/m/s/E-W (NAD83, taken as
#                      WGS84: the shift is ~1 m)
#   EN (entities)      3 registration number, 5 entity type (O = owner),
#                      9 entity name
#
# Status codes: C constructed (141,818 of 197,635 on 2026-09-27), G granted
# but not built, I dismantled, T terminated, A cancelled. Only C without a
# dismantle date is kept.

# ASR structure type (a regular expression over the whole code) -> (man_made, tower:type, tower:construction). Kept:
# the free-standing communication structures a mapper would draw as a mast or
# a tower. Left out, with their constructed counts on 2026-09-27:
#   B, BANT, BMAST, BPIPE, BPOLE, BTWR (1,6k-ish, on a building: a rooftop
#     antenna, mapped as the building or man_made=antenna, not a mast)
#   UPOLE (592, a utility pole carrying antennas: the pole is power or
#     telecom infrastructure already, adding it would double utility_pole)
#   TANK (659, water towers: the water_tower layer), SILO, STACK (chimneys),
#   SIGN, BRIDG, RIG (offshore rigs), PIPE, NNTANN, and no type (3,546
#     old records whose structure is unknown)
# nTAm, nGTAm, nLTAm, nMTAm are tower m of an n-tower array (AM directional
# antennas): each element is its own mast. 2TOWER..7TOWER, NTOWER: several
# structures under one registration, drawn as one.
FCC_TYPES = [
    ("GTOWER", "mast", None, "guyed_lattice"),    # guyed tower
    ("MTOWER", "mast", None, "freestanding"),     # monopole
    ("MAST", "mast", None, None),
    ("POLE", "mast", None, None),                  # a pole carrying antennas
    ("TREE", "mast", None, "concealed"),           # monopine, "stealth" tower
    ("LTOWER", "tower", "communication", "lattice"),
    ("TOWER", "tower", "communication", None),
    ("[0-9]+GTA[0-9]+", "mast", None, "guyed_lattice"),   # guyed array element
    ("[0-9]+LTA[0-9]+", "tower", "communication", "lattice"),
    ("[0-9]+MTA[0-9]+", "mast", None, "freestanding"),
    ("[0-9]+TA[0-9]+", "mast", None, None),                # array element, any other
    ("([0-9]+|N)TOWER", "tower", "communication", None),   # 2TOWER .. NTOWER
]
# The keys OSM mappers put ASR numbers in (taginfo, 2026-09: fcc:registration_number
# 1,271, ref:fcc 37, a few one-offs).
FCC_REF_KEYS = ["fcc:registration_number", "ref:fcc", "ref:US:fcc:asr",
                "fcc:asr", "ref:US:FCC", "asr"]
# Present when OSM has any mast or tower this close. ASR coordinates are the
# structure's (FAA-surveyed for most): of the constructed towers OSM has
# nearby, most are within 30 m, and past 100 m the next mast is mostly a
# different structure (tower farms, AM arrays). 2026-09-28, US: see _coverage.
FCC_NEAR_M = 100


def fcc_asr_rows(con: duckdb.DuckDBPyConnection, path: str) -> dict:
    """Table fcc_add: the constructed US communication towers the FCC's
    Antenna Structure Registration lists and OSM lacks, as rows with the tags
    a mapper would use (osm_type 'fcc', source_id the registration number).
    A tower counts as present when OSM has its registration number, or any
    mast, tower, communications tower or antenna within FCC_NEAR_M: a
    borderline tower stays OSM-only rather than risk a duplicate. Returns the
    coverage figures."""
    import tempfile
    import zipfile

    tmp = tempfile.mkdtemp(prefix="fcc_asr_")
    with zipfile.ZipFile(path) as z:
        for name in ("RA.dat", "CO.dat", "EN.dat"):
            z.extract(name, tmp)

    def dat(name: str) -> str:
        # No quoting in ULS files: a " in an address is data.
        return (f"read_csv('{tmp}/{name}', delim='|', header=false, all_varchar=true, "
                f"quote='', escape='', null_padding=true)")

    def when(i: int) -> str:
        return " ".join(f"WHEN regexp_full_match(stype, '{t[0]}') THEN {'NULL' if t[i] is None else repr(t[i])}"
                        for t in FCC_TYPES)

    date = "strftime(TRY_STRPTIME({}, '%m/%d/%Y'), '%Y-%m-%d')"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE fcc AS
        WITH ra AS (
            SELECT column03 AS reg, column32 AS stype,
                   {date.format('column12')} AS built,
                   TRY_CAST(column30 AS DOUBLE) AS height
            FROM {dat('RA.dat')}
            WHERE column08 = 'C' AND column13 IS NULL
        ), co AS (
            SELECT column03 AS reg,
                   (TRY_CAST(column06 AS DOUBLE) + TRY_CAST(column07 AS DOUBLE) / 60
                    + coalesce(TRY_CAST(column08 AS DOUBLE), 0) / 3600)
                       * CASE column09 WHEN 'S' THEN -1 ELSE 1 END AS lat,
                   (TRY_CAST(column11 AS DOUBLE) + TRY_CAST(column12 AS DOUBLE) / 60
                    + coalesce(TRY_CAST(column13 AS DOUBLE), 0) / 3600)
                       * CASE column14 WHEN 'W' THEN -1 ELSE 1 END AS lon
            FROM {dat('CO.dat')} WHERE column05 = 'T'
        ), owner AS (
            SELECT column03 AS reg, any_value(column09) AS owner
            FROM {dat('EN.dat')} WHERE column05 = 'O' GROUP BY 1
        )
        SELECT ra.reg, ra.stype, ra.built, ra.height, owner.owner,
               CASE {when(1)} END AS man_made, CASE {when(2)} END AS tower_type,
               CASE {when(3)} END AS construction,
               ST_Point(co.lon, co.lat) AS geometry
        FROM ra JOIN co USING (reg) LEFT JOIN owner USING (reg)
        WHERE co.lat IS NOT NULL AND co.lon IS NOT NULL
    """)
    import shutil
    shutil.rmtree(tmp, ignore_errors=True)
    kept = "man_made IS NOT NULL"
    by_type = dict(con.execute(
        f"SELECT CASE WHEN {kept} THEN 'kept' ELSE 'other types' END, count(*) "
        f"FROM fcc GROUP BY 1").fetchall())
    con.execute(f"DELETE FROM fcc WHERE NOT ({kept})")
    if not con.execute("SELECT count(*) FROM fcc").fetchone()[0]:
        sys.exit(f"{path}: no constructed antenna structure read")
    # Only the towers the build covers (the US and its territories on land;
    # a partial build counts only its own).
    con.execute("""
        DELETE FROM fcc WHERE NOT EXISTS (
            SELECT 1 FROM land
            WHERE split_part(region, '/', 1) IN ('US', 'PR', 'GU', 'VI', 'AS', 'MP')
              AND ST_Contains(geometry, fcc.geometry))""")

    # What OSM has: any mast, tower or antenna (a tower of another tower:type,
    # or none, is often the same structure mapped loosely), and ASR numbers.
    # Distances on the spheroid: one projection does not fit CONUS, Alaska,
    # Hawaii and Guam. A 0.01 degree box first (>= 360 m at 71 N) keeps it a
    # spatial join.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE osm_mast AS
        SELECT tags, ST_PointOnSurface(geometry) AS g FROM feat
        WHERE country IN ('US', 'PR', 'GU', 'VI', 'AS', 'MP')
          AND (lc_val(tags, 'man_made') IN ('mast', 'tower', 'communications_tower', 'antenna')
               OR tags['tower:type'] IN ('communication', 'radio', 'antenna'))""")
    refs = " UNION ALL ".join(
        f"SELECT unnest(string_split(tags['{k}'], ';')) AS v FROM osm_mast "
        f"WHERE tags['{k}'] IS NOT NULL" for k in FCC_REF_KEYS)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE fcc_ref AS
        SELECT DISTINCT lpad(ltrim(regexp_replace(v, '[^0-9]', '', 'g'), '0'), 7, '0') AS reg
        FROM ({refs}) WHERE regexp_replace(v, '[^0-9]', '', 'g') <> ''""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE fcc_near AS
        SELECT f.reg, min(ST_Distance_Spheroid(ST_FlipCoordinates(f.geometry),
                                               ST_FlipCoordinates(o.g))) AS m
        FROM fcc f JOIN osm_mast o ON ST_DWithin(f.geometry, o.g, 0.01)
        GROUP BY f.reg""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE fcc_how AS
        SELECT f.reg, n.m,
               CASE WHEN lpad(ltrim(f.reg, '0'), 7, '0') IN (SELECT reg FROM fcc_ref) THEN 'ref'
                    WHEN n.m <= {FCC_NEAR_M} THEN 'near'
                    ELSE 'added' END AS how
        FROM fcc f LEFT JOIN fcc_near n USING (reg)""")
    con.execute("""
        CREATE OR REPLACE TABLE fcc_add AS
        SELECT 'fcc' AS osm_type, row_number() OVER (ORDER BY f.reg) AS osm_id,
               f.reg AS source_id,
               map_from_entries(list_filter([
                   struct_pack(k := 'man_made', v := f.man_made),
                   struct_pack(k := 'tower:type', v := f.tower_type),
                   struct_pack(k := 'tower:construction', v := f.construction),
                   struct_pack(k := 'height', v := CASE WHEN f.height > 0
                                                       THEN round(f.height, 1)::VARCHAR END),
                   struct_pack(k := 'operator', v := f.owner),
                   struct_pack(k := 'fcc:registration_number', v := f.reg),
                   struct_pack(k := 'start_date', v := f.built)
               ], x -> x.v IS NOT NULL)) AS tags,
               f.geometry
        FROM fcc f JOIN fcc_how h USING (reg) WHERE h.how = 'added'""")
    tiers = {how: {"features": n} for how, n in con.execute(
        "SELECT how, count(*) FROM fcc_how GROUP BY 1 ORDER BY 1").fetchall()}
    within = {f"{d}m": n for d, n in zip((50, 100, 250), con.execute(
        "SELECT count(*) FILTER (WHERE m <= 50), count(*) FILTER (WHERE m <= 100), "
        "count(*) FILTER (WHERE m <= 250) FROM fcc_how").fetchone())}
    total = con.execute("SELECT count(*) FROM fcc").fetchone()[0]
    return {"source": Path(path).name,
            "telecom_mast": {"fcc": total, "tiers": tiers,
                             "osm_mast_within": within,
                             "other_structure_types": by_type.get("other types", 0)}}


# --------------------------------------------------------------------------
# ORE: French HTA/BT substations OSM does not have yet
#
# Agence ORE publishes the public HTA/BT substations ("postes de distribution
# publique") of every French electricity distribution operator: Enedis
# (~94%), the local ones (GÉRÉDIS, SRD, Strasbourg Électricité Réseaux, SICAEs,
# régies) and EDF SEI in Corsica and overseas. Enedis's own dataset has the
# same Enedis points and nothing else, so ORE is the source. Licence Ouverte
# 2.0 (Etalab): reuse, commercial included, with attribution; compatible with
# the ODbL.
#
# The records are points with the operator and, for some operators only (EDF
# SEI), a name. No code (Enedis's own, ref:FR:Enedis=50173P0111 in OSM, is not
# published), no voltage, and no kind: a pole-mounted transformer (H61) is a
# poste like a cabin. OSM maps H61 as power=pole + transformer=*, so a record
# near one counts as present; an H61 OSM lacks is added as a substation,
# the only model a point without a kind allows.

ORE_TERRITORIES = ("FR", "RE", "GP", "MQ", "GF", "YT", "PM", "BL", "MF")
ORE_WIKIDATA = {"Enedis": "Q3587594"}
ORE_NEAR_M = 30


def ore_download(dest: str) -> int:
    """The ORE dataset as one CSV, through its data-fair lines API: pages of
    10,000, following each Link rel=next (no bulk file is published)."""
    import urllib.request
    url = ("https://opendata.agenceore.fr/data-fair/api/v1/datasets/"
           "postes-de-distribution-publique-postes-htabt/lines"
           "?format=csv&select=_geopoint,nom_poste,nom_grd,date_maj,code_insee&size=10000")
    ua = {"User-Agent": "osm-geoparquet-infrastructure/1.0 (+https://geoparquet.geomermaids.com/)"}
    n = 0
    with open(dest, "wb") as out:
        while url:
            for attempt in range(5):
                try:
                    r = urllib.request.urlopen(urllib.request.Request(url, headers=ua), timeout=120)
                    lines = r.read().decode("utf-8-sig").splitlines(keepends=True)
                    break
                except OSError:
                    if attempt == 4:
                        raise
                    time.sleep(10 * (attempt + 1))
            out.write("".join(lines if n == 0 else lines[1:]).encode())
            n += len(lines) - 1
            m = re.search(r'<([^>]+)>;\s*rel="?next"?', r.headers.get("Link", ""))
            url = m.group(1) if m and len(lines) > 1 else None
    return n


def ore_rows(con: duckdb.DuckDBPyConnection, path: str) -> dict:
    """Table ore_add: the HTA/BT substations Agence ORE publishes and OSM
    lacks, as power=substation + substation=minor_distribution points
    (osm_type 'ore', for add_rows). A record counts as present when OSM has,
    within ORE_NEAR_M metres, a substation (its outline, not only its
    centre) or a transformer (power=transformer, or a pole or other feature
    carrying transformer=*, the H61 model). Returns the coverage figures."""
    # Metres: Lambert-93 for mainland France and Corsica; overseas, web
    # mercator, whose scale error below 21 degrees is under 7% (a 30 m
    # threshold acts as 28 to 30 m). The two ranges never overlap in y.
    def metres(g: str) -> str:
        return (f"CASE WHEN ST_YMax({g}) > 41 "
                f"THEN ST_Transform({g}, 'EPSG:4326', 'EPSG:2154', always_xy := true) "
                f"ELSE ST_Transform({g}, 'EPSG:4326', 'EPSG:3857', always_xy := true) END")
    territories = ", ".join(f"'{c}'" for c in ORE_TERRITORIES)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ore AS
        SELECT row_number() OVER () AS rid, nullif(trim(NOM_POSTE), '') AS name,
               NOM_GRD AS operator, p AS geometry
        FROM (SELECT DISTINCT ON (_geopoint) *,
                     ST_Point(TRY_CAST(split_part(_geopoint, ',', 2) AS DOUBLE),
                              TRY_CAST(split_part(_geopoint, ',', 1) AS DOUBLE)) AS p
              FROM read_csv('{path}', all_varchar = true, header = true))
        WHERE ST_X(p) IS NOT NULL AND ST_Y(p) IS NOT NULL""")
    if not con.execute("SELECT count(*) FROM ore").fetchone()[0]:
        sys.exit(f"{path}: no substation read")
    # Only what the build covers (see add_eia_plants).
    con.execute(f"""
        DELETE FROM ore WHERE NOT EXISTS (
            SELECT 1 FROM land WHERE split_part(region, '/', 1) IN ({territories})
              AND ST_Contains(geometry, ore.geometry))""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ore_m AS
        SELECT rid, {metres('geometry')} AS g FROM ore""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE osm_sub AS
        SELECT {metres('geometry')} AS g
        FROM substation WHERE country IN ({territories})""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE osm_tr AS
        SELECT {metres('geometry')} AS g FROM feat
        WHERE country IN ({territories})
          AND (lc_val(tags, 'power') = 'transformer' OR tags['transformer'] IS NOT NULL)""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ore_how AS
        WITH sub AS (SELECT DISTINCT o.rid FROM ore_m o JOIN osm_sub s
                     ON ST_DWithin(s.g, o.g, {ORE_NEAR_M})),
             tr AS (SELECT DISTINCT o.rid FROM ore_m o JOIN osm_tr t
                    ON ST_DWithin(t.g, o.g, {ORE_NEAR_M}))
        SELECT o.rid, CASE WHEN o.rid IN (SELECT * FROM sub) THEN 'near_substation'
                           WHEN o.rid IN (SELECT * FROM tr) THEN 'near_transformer'
                           ELSE 'added' END AS how
        FROM ore_m o""")
    wikidata = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in ORE_WIKIDATA.items())
    con.execute(f"""
        CREATE OR REPLACE TABLE ore_add AS
        SELECT 'ore' AS osm_type, row_number() OVER () AS osm_id, NULL::VARCHAR AS source_id,
               map_from_entries(list_filter([
                   struct_pack(k := 'power', v := 'substation'),
                   struct_pack(k := 'substation', v := 'minor_distribution'),
                   struct_pack(k := 'name', v := o.name),
                   struct_pack(k := 'operator', v := o.operator),
                   struct_pack(k := 'operator:wikidata', v := CASE o.operator {wikidata} END)
               ], x -> x.v IS NOT NULL)) AS tags,
               o.geometry, NULL::BIGINT[] AS circuit_ids
        FROM ore o JOIN ore_how h USING (rid) WHERE h.how = 'added'""")
    tiers = {how: {"features": n} for how, n in con.execute(
        "SELECT how, count(*) FROM ore_how GROUP BY 1 ORDER BY 1").fetchall()}
    by_operator = dict(con.execute("""
        SELECT o.operator, count(*) FROM ore o JOIN ore_how h USING (rid)
        WHERE h.how = 'added' GROUP BY 1 ORDER BY 2 DESC""").fetchall())
    n = con.execute("SELECT count(*) FROM ore").fetchone()[0]
    return {"source": Path(path).name,
            "power_substation": {"ore": n, "near_m": ORE_NEAR_M, "tiers": tiers,
                                 "added_by_operator": by_operator}}


# --------------------------------------------------------------------------
# OGIM: oil and gas infrastructure OSM does not have, worldwide
#
# The Oil and Gas Infrastructure Mapping database (EDF / MethaneSAT, v3.0,
# CC BY 4.0) compiles about 190 public sources into one GeoPackage. CC BY
# 4.0 is compatible with the ODbL, so what OSM lacks goes into the layers
# with origin = 'ogim' and source_id = OGIM_ID. Two filters come first:
#
#   - Sources whose own terms restrict reuse, or that we take elsewhere,
#     are dropped by SRC_REF_ID (OGIM_EXCLUDED; a record citing several
#     sources is dropped if any of them is excluded).
#   - Only what is (or was) there: wells drilled and not plugged, sites
#     operating, pipelines not abandoned. OGIM's machine-learning well pads,
#     VIIRS flares, fields, blocks, basins and production tables are not read.
#
# It runs after the BOEM/BSEE step and matches against all of feat,
# so a Gulf platform BSEE already added is not added twice.

OGIM_EXCLUDED = {
    # Terms that restrict reuse (the ODbL grants commercial reuse downstream).
    # A source with no stated licence is used (Guillaume, 2026-09-28): only
    # explicit restrictions keep a source out.
    "Alberta Energy Regulator, non-commercial": [1, 2, 4, 5, 6, 7, 8, 9, 222],
    "Petrinex (Alberta, Manitoba), AER terms": [3, 52],
    "North Sea Transition Authority, non-commercial (published apart, nsta/)": [12, 13, 265],
    "BC Energy Regulator, 'representation purposes only'": [25, 26, 27, 29, 33],
    # Our earlier decisions.
    "HIFLD, measured only (licence 'other', some layers of commercial origin)": [87, 91, 93, 94, 95, 96, 244],
    "Chen et al. 2024, tanks detected from satellite imagery": [242],
    "USGS world energy maps, small scale: lines hundreds of metres off": [158],
    "BOEM, taken from the producer": [266],
}
OGIM_EXCLUDED_IDS = sorted(i for ids in OGIM_EXCLUDED.values() for i in ids)

# Facility tables -> the tags that put them in petroleum_site. The layer reads
# tags['industrial'] and tags['pipeline'] without lifecycle prefixes, so only
# operating sites (and those with no status) are taken.
OGIM_SITES = {
    "Crude_Oil_Refineries": [("'industrial'", "'refinery'")],
    "Gathering_and_Processing": [("'industrial'", "CASE WHEN FAC_TYPE LIKE '%GAS%' THEN 'gas' ELSE 'oil' END")],
    "LNG_Facilities": [("'industrial'", "'gas'"), ("'product'", "'lng'")],
    "Natural_Gas_Compressor_Stations": [("'pipeline'", "'substation'"), ("'substation'", "'compression'"),
                                        ("'substance'", "'gas'")],
    "Petroleum_Terminals": [("'industrial'", "'petroleum_terminal'")],
    "Stations_Other": [("'pipeline'", "'substation'"), ("'substation'", """CASE
        WHEN FAC_TYPE LIKE '%REGULAT%' THEN 'reduction'
        WHEN FAC_TYPE LIKE '%METER%' OR FAC_TYPE LIKE '%MEASUREMENT%' OR FAC_TYPE LIKE '%LACT%'
            THEN 'measurement'
        WHEN FAC_TYPE LIKE '%PUMP%' THEN 'pumping' END""")],
}

OGIM_LAYERS = {"petroleum_well": "ogim_well", "offshore_platform": "ogim_platform",
               "petroleum_site": "ogim_site", "pipeline": "ogim_pipeline"}
OGIM_LICENCE = ("CC BY 4.0. Contains data from the Oil and Gas Infrastructure Mapping (OGIM) "
                "database v3.0, Environmental Defense Fund and MethaneSAT, LLC, "
                "https://doi.org/10.5281/zenodo.22835235")


def ogim_rows(con: duckdb.DuckDBPyConnection, path: str) -> dict:
    """Tables ogim_well, ogim_platform, ogim_site and ogim_pipeline: what
    OGIM reports and feat (OSM, plus the rows earlier steps added) lacks, as
    rows with the tags a mapper would use, osm_type 'ogim' and source_id the
    OGIM_ID. A well counts as present when OSM has its ID (API number and the
    like) in a ref tag or a well within 25 m; a platform, when one is within
    500 m; a site, when a petroleum site or platform is within 500 m (or holds
    it); a pipeline, when pipelines are within 250 m of half the points
    sampled along it. Returns the coverage figures."""
    ex = ", ".join(map(str, OGIM_EXCLUDED_IDS))

    def read(layer: str, cols: str) -> str:
        # 'N/A', -999 and 1900-01-01 are OGIM's missing values.
        return f"""
            SELECT OGIM_ID::VARCHAR AS id, {cols},
                   nullif(nullif(trim(OPERATOR), 'N/A'), '') AS operator, geom AS geometry
            FROM ST_Read('{path}', layer = '{layer}')
            WHERE geom IS NOT NULL AND NOT list_has_any(
                list_transform(string_split(SRC_REF_ID, ','), x -> TRY_CAST(trim(x) AS INTEGER)),
                [{ex}])"""

    def na(col: str) -> str:
        return f"nullif(nullif(trim({col}), 'N/A'), '')"

    def date(col: str) -> str:
        return f"nullif({na(col)}, '1900-01-01')"

    def tags(*pairs: tuple[str, str]) -> str:
        return ("map_from_entries(list_filter(["
                + ", ".join(f"struct_pack(k := {k}, v := {v})" for k, v in pairs)
                + "], x -> x.v IS NOT NULL))")

    def lc_key(key: str) -> str:
        return f"CASE WHEN state = 'active' THEN '{key}' ELSE state || ':{key}' END"

    # Distances in metres anywhere: Web Mercator stretches lengths by
    # 1/cos(latitude), so a join in EPSG:3857 at 4x the distance keeps every
    # candidate up to 75 degrees, and scaling by cos(latitude) gives metres
    # (to ~0.1% over the few hundred metres compared here).
    merc = "'EPSG:4326', 'EPSG:3857', always_xy := true"

    def within(a: str, b: str, lat: str, m: int) -> str:
        return (f"ST_DWithin({a}, {b}, {4 * m}) "
                f"AND ST_Distance({a}, {b}) * cos(radians({lat})) <= {m}")

    # OSM (and already added) features, per kind, in Web Mercator.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE og_osm AS
        SELECT CASE WHEN lc_val(tags, 'man_made') IN ('petroleum_well', 'oil_well') THEN 'well'
                    WHEN lc_val(tags, 'man_made') = 'offshore_platform' THEN 'platform'
                    WHEN lc_val(tags, 'man_made') = 'pipeline' THEN 'pipeline'
                    ELSE 'site' END AS kind,
               list_transform(list_filter(map_entries(tags),
                   e -> e.key = 'ref' OR starts_with(e.key, 'ref:') OR e.key = 'api'),
                   e -> e.value) AS refs,
               ST_Transform(geometry, {merc}) AS g
        FROM feat
        WHERE lc_val(tags, 'man_made') IN ('petroleum_well', 'oil_well', 'offshore_platform', 'pipeline')
           OR tags['industrial'] IN ('oil', 'fracking', 'oil_storage', 'petroleum_terminal',
                 'hydrocarbons', 'oil sands', 'oil_sands', 'gas', 'gas_storage',
                 'natural_gas', 'wellsite', 'well_cluster', 'refinery')
           OR tags['pipeline'] = 'substation'""")

    # Wells: drilled and not plugged. N/A is kept (Texas reports plugged
    # wells as such and leaves the rest without a status), dry holes and
    # water supply wells are not.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ow AS
        SELECT *, ST_Y(geometry) AS lat, ST_Transform(geometry, {merc}) AS g FROM (
            SELECT id, operator, geometry, {na('FAC_ID')} AS ref,
                   {na('FAC_NAME')} AS name, coalesce({date('SPUD_DATE')}, {date('COMP_DATE')}) AS started,
                   CASE WHEN FAC_TYPE LIKE '%INJECT%' OR FAC_TYPE LIKE '%DISPOSAL%' THEN NULL
                        WHEN FAC_TYPE LIKE '%OIL%' AND FAC_TYPE LIKE '%GAS%' THEN 'hydrocarbons'
                        WHEN FAC_TYPE LIKE '%GAS%' THEN 'gas'
                        WHEN FAC_TYPE LIKE '%OIL%' THEN 'oil' END AS substance,
                   CASE OGIM_STATUS WHEN 'DRILLING' THEN 'construction'
                                    WHEN 'INACTIVE' THEN 'disused' ELSE 'active' END AS state
            FROM ({read('Oil_and_Natural_Gas_Wells',
                        'FAC_ID, FAC_NAME, FAC_TYPE, SPUD_DATE, COMP_DATE, OGIM_STATUS')})
            WHERE OGIM_STATUS IN ('PRODUCING', 'COMPLETED', 'DRILLING', 'INJECTING', 'INACTIVE',
                                  'STORAGE, MAINTENANCE, OR OBSERVATION', 'N/A')
              AND coalesce(FAC_TYPE, '') NOT SIMILAR TO '.*(DRY HOLE|WATER SUPPLY|BRINE|STRATIGRAPHIC).*')""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE ow_ref AS
        SELECT DISTINCT regexp_replace(upper(r), '[^A-Z0-9]', '', 'g') AS ref
        FROM (SELECT unnest(flatten(list_transform(refs, v -> string_split(v, ';')))) AS r
              FROM og_osm WHERE kind = 'well') WHERE r <> ''""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ow_how AS
        WITH ref AS (SELECT DISTINCT w.id FROM ow w
                     JOIN ow_ref o ON o.ref = regexp_replace(upper(w.ref), '[^A-Z0-9]', '', 'g')),
             near AS (SELECT DISTINCT w.id FROM ow w JOIN og_osm o
                      ON o.kind = 'well' AND {within('o.g', 'w.g', 'w.lat', 25)})
        SELECT w.id, CASE WHEN w.id IN (SELECT * FROM ref) THEN 'ref'
                          WHEN w.id IN (SELECT * FROM near) THEN 'near'
                          ELSE 'added' END AS how
        FROM ow w""")
    con.execute(f"""
        CREATE OR REPLACE TABLE ogim_well AS
        SELECT 'ogim' AS osm_type, row_number() OVER ()::BIGINT AS osm_id, id AS source_id,
               {tags((lc_key('man_made'), "'petroleum_well'"), ("'ref'", "ref"), ("'name'", "name"),
                     ("'operator'", "operator"), ("'substance'", "substance"),
                     ("'start_date'", "started"))} AS tags,
               geometry
        FROM ow JOIN ow_how USING (id) WHERE how = 'added'""")

    # Platforms.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE op AS
        SELECT *, ST_Y(geometry) AS lat, ST_Transform(geometry, {merc}) AS g FROM (
            SELECT id, operator, geometry, {na('FAC_NAME')} AS name, {date('INSTALL_DATE')} AS started,
                   CASE OGIM_STATUS WHEN 'INACTIVE' THEN 'disused'
                                    WHEN 'UNDER CONSTRUCTION' THEN 'construction' ELSE 'active' END AS state
            FROM ({read('Offshore_Platforms', 'FAC_NAME, INSTALL_DATE, OGIM_STATUS')})
            WHERE OGIM_STATUS IN ('OPERATIONAL', 'INACTIVE', 'UNDER CONSTRUCTION', 'N/A'))""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE op_how AS
        WITH near AS (SELECT DISTINCT p.id FROM op p JOIN og_osm o
                      ON o.kind = 'platform' AND {within('o.g', 'p.g', 'p.lat', 500)})
        SELECT p.id, CASE WHEN p.id IN (SELECT * FROM near) THEN 'near' ELSE 'added' END AS how
        FROM op p""")
    con.execute(f"""
        CREATE OR REPLACE TABLE ogim_platform AS
        SELECT 'ogim' AS osm_type, row_number() OVER ()::BIGINT AS osm_id, id AS source_id,
               {tags((lc_key('man_made'), "'offshore_platform'"), ("'name'", "name"),
                     ("'operator'", "operator"), ("'start_date'", "started"))} AS tags,
               geometry
        FROM op JOIN op_how USING (id) WHERE how = 'added'""")

    # Sites: refineries, processing plants, LNG, compressor, metering and
    # pumping stations, terminals. Matched against any petroleum site or
    # platform, whatever its kind: OSM often tags a gas plant industrial=oil.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE os AS
        SELECT *, ST_Y(geometry) AS lat, ST_Transform(geometry, {merc}) AS g FROM (
            {" UNION ALL ".join(f'''
            SELECT id, '{layer}' AS category, geometry,
                   {tags(*pairs, ("'name'", na('FAC_NAME')), ("'operator'", "operator"),
                         ("'start_date'", date('INSTALL_DATE')))} AS tags
            FROM ({read(layer, 'FAC_NAME, FAC_TYPE, INSTALL_DATE, OGIM_STATUS')})
            WHERE OGIM_STATUS IN ('OPERATIONAL', 'N/A')''' for layer, pairs in OGIM_SITES.items())})""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE os_how AS
        WITH near AS (SELECT s.id, min(ST_Distance(o.g, s.g) * cos(radians(s.lat))) AS d
                      FROM os s JOIN og_osm o
                      ON o.kind IN ('site', 'platform') AND {within('o.g', 's.g', 's.lat', 500)}
                      GROUP BY s.id)
        SELECT s.id, s.category, CASE WHEN n.d = 0 THEN 'inside' WHEN n.d IS NOT NULL THEN 'near'
                                      ELSE 'added' END AS how
        FROM os s LEFT JOIN near n USING (id)""")
    con.execute(f"""
        CREATE OR REPLACE TABLE ogim_site AS
        SELECT 'ogim' AS osm_type, row_number() OVER ()::BIGINT AS osm_id, id AS source_id,
               tags, geometry
        FROM os JOIN os_how USING (id) WHERE how = 'added'""")

    # Pipelines, not abandoned. Points every 500 m along each part (2 to 40
    # of them) are checked against OSM's pipelines: a line is present when half
    # its points are within 250 m of one.
    substance = """CASE
        WHEN COMMODITY LIKE '%WATER%' THEN 'water'
        WHEN COMMODITY LIKE '%GAS LIQUID%' OR COMMODITY LIKE '%VOLATILE LIQUID%'
             OR COMMODITY LIKE '%NGL%' THEN 'ngl'
        WHEN COMMODITY LIKE '%CONDENSATE%' THEN 'condensate'
        WHEN COMMODITY LIKE '%EFFLUENT%' OR COMMODITY LIKE '%MULTIPHASE%' THEN 'hydrocarbons'
        WHEN COMMODITY LIKE '%GAS%' THEN 'gas'
        WHEN COMMODITY LIKE '%CRUDE%' OR COMMODITY LIKE '%OIL%' THEN 'oil'
        WHEN COMMODITY LIKE '%PRODUCT%' OR COMMODITY LIKE '%REFINED%'
             OR COMMODITY LIKE '%GASOLINE%' OR COMMODITY LIKE '%DIESEL%' THEN 'fuel' END"""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ol AS
        SELECT *, ST_Length_Spheroid(ST_FlipCoordinates(geometry)) / 1000 AS km FROM (
            SELECT id, operator, ST_LineMerge(geometry) AS geometry, {na('FAC_NAME')} AS name,
                   {date('INSTALL_DATE')} AS started, {substance} AS substance,
                   CASE WHEN COMMODITY LIKE '%GATHERING%' THEN 'gathering'
                        WHEN COMMODITY LIKE '%TRANSMISSION%' THEN 'transmission' END AS usage,
                   CASE WHEN PIPE_DIAMETER_MM > 0 THEN round(PIPE_DIAMETER_MM)::INTEGER::VARCHAR END AS diameter,
                   CASE OGIM_STATUS WHEN 'INACTIVE' THEN 'disused'
                                    WHEN 'UNDER CONSTRUCTION' THEN 'construction' ELSE 'active' END AS state
            FROM ({read('Oil_and_Natural_Gas_Pipelines',
                        'FAC_NAME, COMMODITY, INSTALL_DATE, PIPE_DIAMETER_MM, OGIM_STATUS')})
            WHERE OGIM_STATUS IN ('OPERATIONAL', 'INACTIVE', 'UNDER CONSTRUCTION', 'N/A'))
        WHERE NOT ST_IsEmpty(geometry)""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ol_pt AS
        SELECT id, part, i, ST_Y(p) AS lat, ST_Transform(p, {merc}) AS g FROM (
            SELECT id, part, i, ST_LineInterpolatePoint(line, (i + 0.5) / n) AS p
            FROM (SELECT id, part, line, n, unnest(range(n)) AS i
                  FROM (SELECT id, part, line, least(greatest(ceil(
                               ST_Length_Spheroid(ST_FlipCoordinates(line)) / 500)::INTEGER, 2), 40) AS n
                        FROM (SELECT id, coalesce(d.path[1], 1) AS part, d.geom AS line
                              FROM (SELECT id, unnest(ST_Dump(geometry)) AS d FROM ol))
                        WHERE ST_GeometryType(line) = 'LINESTRING')))""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ol_how AS
        WITH hit AS (SELECT DISTINCT p.id, p.part, p.i FROM ol_pt p JOIN og_osm o
                     ON o.kind = 'pipeline' AND {within('o.g', 'p.g', 'p.lat', 250)}),
             share AS (SELECT p.id, count(h.i) / count(*) AS share
                       FROM ol_pt p LEFT JOIN hit h USING (id, part, i) GROUP BY p.id)
        SELECT l.id, l.km, CASE WHEN s.share >= 0.5 THEN 'near' ELSE 'added' END AS how
        FROM ol l LEFT JOIN share s USING (id)""")
    con.execute(f"""
        CREATE OR REPLACE TABLE ogim_pipeline AS
        SELECT 'ogim' AS osm_type, row_number() OVER ()::BIGINT AS osm_id, id AS source_id,
               {tags((lc_key('man_made'), "'pipeline'"), ("'name'", "name"), ("'operator'", "operator"),
                     ("'substance'", "substance"), ("'usage'", "usage"), ("'diameter'", "diameter"),
                     ("'start_date'", "started"))} AS tags,
               geometry
        FROM ol JOIN ol_how USING (id) WHERE how = 'added'""")

    def measure(read: str, how: str, by: str = "", km: bool = False) -> dict:
        n = con.execute(f"SELECT count(*) FROM {read}").fetchone()[0]
        if not n:
            sys.exit(f"{path}: nothing read for {read}")
        rows = con.execute(f"SELECT {by or 'NULL'}, how, count(*){', round(sum(km))' if km else ''} "
                           f"FROM {how} GROUP BY ALL ORDER BY ALL").fetchall()
        tiers: dict = {}
        for r in rows:
            t = tiers.setdefault(r[0], {}) if by else tiers
            t[r[1]] = {"features": r[2], **({"km": r[3]} if km else {})}
        return {"ogim": n, "tiers": tiers}

    return {"source": Path(path).name, "licence": OGIM_LICENCE,
            "excluded_sources": OGIM_EXCLUDED,
            "petroleum_well": measure("ow", "ow_how"),
            "offshore_platform": measure("op", "op_how"),
            "petroleum_site": measure("os", "os_how", by="category"),
            "pipeline": measure("ol", "ol_how", km=True)}


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
    # Rows added from another source (see add_rows) carry the source's name in
    # osm_type until here. They have no OSM element: no osm_id/osm_type, and
    # no tags, since theirs only exist to derive the columns; source_id is
    # the record's ID in its source.
    return (f"SELECT CASE WHEN {IS_OSM} THEN osm_id END AS osm_id, "
            f"CASE WHEN {IS_OSM} THEN osm_type END AS osm_type, "
            f"CASE WHEN {IS_OSM} THEN 'osm' ELSE osm_type END AS origin, source_id, "
            f"country, state, {', '.join(cols)}, "
            f"CASE WHEN {IS_OSM} THEN tags END AS tags, "
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
    floats, delta-packed ids), zstd 19, 1 MB pages.

    19 rather than the guide's minimum of 15: measured 2026-09-27 on three
    published layers, 13-15% smaller (power_line 598 -> 506 MB,
    power_generator 370 -> 319 MB, power_substation 78 -> 67 MB) for 2-3x
    the write time; reads take the same time."""
    return dict(
        compression="zstd", compression_level=19, write_statistics=True,
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


ABANDONED = "abandoned"


def write_repository(out: Path, report: list[dict], abandoned: list[dict]) -> None:
    """_manifest.json in <out> and in <out>/abandoned, and index.json at the
    repository base, the parquetry protocol GeoPQ Workbench reads: two
    datasets, the whole world at an empty path (its files are
    <out>/<layer>.parquet, as GAUL's whole-world entry), and what is
    abandoned in abandoned/."""
    for folder, name, rep in ((out, "The whole world", report),
                              (out / ABANDONED, "Abandoned, the whole world", abandoned)):
        themes = {r["layer"]: r["rows"] for r in rep if r["rows"]}
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "_manifest.json").write_text(json.dumps({
            "state_name": name, "total_features": sum(themes.values()),
            "themes": themes,
        }, indent=2))
    (out.parent / "index.json").write_text(json.dumps({"datasets": [
        {"path": "", "code": "WORLD", "name": "The whole world"},
        {"path": ABANDONED, "code": "ABANDONED", "name": "Abandoned, the whole world"},
    ]}, indent=2))


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

def print_added(name: str, coverage: dict) -> None:
    """One line per layer: how many of a source's records OSM lacked."""
    def added(tiers: dict) -> int:   # tiers, or tiers per category
        return tiers["added"]["features"] if "added" in tiers else sum(
            added(t) for t in tiers.values() if isinstance(t, dict) and "features" not in t)

    print(f"  {name}: " + ", ".join(
        f"{layer} {added(v['tiers']):,} of "
        f"{next(n for k, n in v.items() if k != 'tiers'):,} added"
        for layer, v in coverage.items() if isinstance(v, dict) and "tiers" in v), flush=True)


def build(pbf: Path, out: Path, work: Path, land: str, eez: str,
          eia: str | None = None, uspvdb: str | None = None,
          nsta: str | None = None, uswtdb: str | None = None,
          boem: str | None = None, fcc: str | None = None,
          ore: str | None = None, ogim: str | None = None) -> None:
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
    memory("loaded", con)
    build_sources(con)
    memory("relations and generators", con)
    load_regions(con, land, eez)
    # source_id is added while the table is rewritten anyway: DuckDB 1.5.2
    # fails to commit an INSERT into a stored table with a GEOMETRY column
    # after ALTER TABLE ADD COLUMN ("Unsupported geometry type in legacy
    # geometry" on the planet build).
    for table in SOURCE_TABLES:
        assign_region(con, table, source_id=True)
    memory("regions", con)
    # How OSM compares with each authoritative source, and what was added:
    # _coverage.json, keyed by source.
    sources = {}
    if eia:
        coverage = sources["eia"] = add_eia_plants(con, eia, uspvdb)
        added = coverage["tiers"].get("added", {"plants": 0, "mw": 0})
        print(f"  EIA-860M: {coverage['eia_plants']:,} US plants, {added['plants']:,} "
              f"({added['mw'] / 1000:,.1f} GW) added where OSM has none, "
              f"{coverage['added_with_outline']['plants']:,} with a USPVDB outline", flush=True)
    if uswtdb:
        sources["uswtdb"] = uswtdb_rows(con, uswtdb)
        add_rows(con, "generator", "uswtdb_add")
        print_added("USWTDB", sources["uswtdb"])
    if fcc:
        sources["fcc"] = fcc_asr_rows(con, fcc)
        add_rows(con, "feat", "fcc_add")
        print_added("FCC ASR", sources["fcc"])
    if ore:
        sources["ore"] = ore_rows(con, ore)
        add_rows(con, "substation", "ore_add")
        print_added("ORE", sources["ore"])
    if boem:
        sources["boem"] = boem_rows(con, boem)
        for table in BOEM_LAYERS.values():
            add_rows(con, "feat", table)
        print_added("BOEM/BSEE", sources["boem"])
    if ogim:   # after BOEM/BSEE: OGIM is matched against what it added too
        sources["ogim"] = ogim_rows(con, ogim)
        for table in OGIM_LAYERS.values():
            add_rows(con, "feat", table)
        print_added("OGIM", sources["ogim"])
    if nsta:
        sources["nsta"] = nsta_rows(con, nsta)
        print_added("NSTA (apart, in nsta/)", sources["nsta"])
    staging = work / "staging"
    staging.mkdir(exist_ok=True)
    memory("sources", con)
    # Abandoned features (OSM's abandoned:* and abandoned=yes, and sources'
    # abandoned-in-place records) are a dataset of their own, in the
    # abandoned/ folder with the same layers and schema: the main files
    # hold what stands or is being built. Disused (out of service, still
    # standing) stays in the main files.
    def split(layers: list[Layer], staged: Path) -> tuple[list[dict], list[dict]]:
        (staged / ABANDONED).mkdir(parents=True, exist_ok=True)
        return tuple([write_layer(con, replace(layer, where=f"({layer.where}) AND "
                                               f"lifecycle(tags, '{layer.key}') {op} 'abandoned'"),
                                  dest)
                      for layer in layers]
                     for op, dest in (("<>", staged), ("=", staged / ABANDONED)))

    report, abandoned_report = split(LAYERS, staging)
    memory("layers sorted", con)
    nsta_report, nsta_abandoned = [], []
    if nsta:
        by_name = {layer.name: layer for layer in LAYERS}
        nsta_report, nsta_abandoned = split(
            [replace(by_name[name], source=table) for name, table in NSTA_LAYERS.items()],
            staging / "nsta")
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
    nsta_out = out.parent / "nsta"
    for r, staged, dest_dir in [(r, staging, out) for r in report] + \
                               [(r, staging / ABANDONED, out / ABANDONED)
                                for r in abandoned_report] + \
                               [(r, staging / "nsta", nsta_out) for r in nsta_report] + \
                               [(r, staging / "nsta" / ABANDONED, nsta_out / ABANDONED)
                                for r in nsta_abandoned]:
        if not r["rows"]:
            continue
        t0 = time.monotonic()
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / f"{r['layer']}.parquet"
        n = rewrite(staged / f"{r['layer']}.parquet", dest)
        # Nothing is published unless every source row reached its file.
        if n != r["rows"]:
            sys.exit(f"{dest.name}: {n:,} rows written, {r['rows']:,} in the source")
        verify(check, dest)
        (staged / f"{r['layer']}.parquet").unlink()
        print(f"  {dest.name}: {n:,} rows, {dest.stat().st_size / 1e6:,.0f} MB, "
              f"{time.monotonic() - t0:.0f} s", flush=True)
    memory("files rewritten")
    write_repository(out, report, abandoned_report)
    if sources:
        (out / "_coverage.json").write_text(json.dumps(sources, indent=2))
    if nsta:
        (nsta_out / "LICENSE.txt").write_text(NSTA_LICENCE.format(
            source=sources["nsta"]["source"], date=time.strftime("%Y-%m-%d", time.gmtime())))
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
    b.add_argument("--uspvdb", help="USGS US Large-Scale Solar PV Database (.geojson or "
                   "its .zip): outlines for the solar plants --eia adds")
    b.add_argument("--nsta", help="NSTA offshore open data (UKCS_OFF_WGS84_SHP.zip): "
                   "the UK wells, platforms and pipelines OSM lacks, written to "
                   "<out>/../nsta/ under NSTA's non-commercial terms")
    b.add_argument("--uswtdb", help="USGS US Wind Turbine Database (uswtdbCSV.zip): "
                   "the US turbines OSM lacks, added to power_generator")
    b.add_argument("--boem", help="folder with BSEE's BoreholeRawData and PlatStrucRawData "
                   "and BOEM's ppl_arcs, unzipped: the US offshore wells, platforms and "
                   "pipelines OSM lacks")
    b.add_argument("--fcc", help="FCC Antenna Structure Registration, weekly complete "
                   "file (r_tower.zip): the US communication towers OSM lacks, added to "
                   "telecom_mast")
    b.add_argument("--ore", help="Agence ORE HTA/BT substations as CSV (infra.py fetch-ore): "
                   "the French distribution substations OSM lacks, added to power_substation")
    b.add_argument("--ogim", help="OGIM v3.0 GeoPackage (EDF/MethaneSAT, CC BY 4.0): the "
                   "wells, sites, platforms and pipelines OSM lacks, worldwide, except "
                   "from the sources whose terms restrict reuse")
    b.add_argument("--eez", required=True,
                   help="Marine Regions eez_land as GeoJSON (path)")
    o = sub.add_parser("fetch-ore", help="download Agence ORE's HTA/BT substations as CSV")
    o.add_argument("dest")
    a = p.parse_args()
    if a.cmd == "filter":
        filter_extract(a.src, a.dest)
    elif a.cmd == "fetch-ore":
        n = ore_download(a.dest)
        if n < 900_000:   # ~1.03 M in 2026: a short read is a failed download
            sys.exit(f"{a.dest}: only {n:,} substations")
        print(f"{a.dest}: {n:,} substations")
    else:
        build(a.pbf, a.out_dir, a.work_dir, a.land, a.eez, a.eia, a.uspvdb, a.nsta,
              a.uswtdb, a.boem, a.fcc, a.ore, a.ogim)


if __name__ == "__main__":
    sys.exit(main())
