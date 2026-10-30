"""
Department rankings for the layers without a pipeline of their own (flood and
deforestation have build_flood_rank.py / build_deforest_rank.py).

Each value is a physical reading of the layer per department, computed from
the dept parquets the CDN serves and written into the `rank` block of each
department in the layer's dept summary JSON (src/lib/data):

- Census layers (4 AR provinces): the hexagons carry real Census 2022 rates and
  the households or people each one is over (hog_total, pob_total, pob_18_mas,
  … — build_census_rates.py), so a department rate is Σ rate·base / Σ base and a
  count is Σ rate/100·base: the same as summing the radios, not a mean of
  hexagons, which would weigh an empty rural cell like a city block.
- Physical layers (24 territories): area-weighted (H3 res-9 cells are equal
  area). PM2.5 and accessibility describe the territory, not its people:
  Paraguay and Brazil have no population per hexagon here.
- forestry_aptitude / agri_potential are 0–100 composite scores: the ranking
  uses hectares with score ≥ HIGH_SCORE on the fixed scale, not the mean score.

    python pipeline/build_layer_ranks.py                    # every layer, every territory
    python pipeline/build_layer_ranks.py --layer pm25_drivers
"""

import argparse
import json
import os
import sys

import h3

sys.path.insert(0, os.path.dirname(__file__))
from config import TERRITORY_CONFIGS, get_territory
from refresh_dept_parquets import FRESH   # CDN edge-cache buster

CDN = 'https://cdn.spatia.ar/data'
SRC_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data')
HA = h3.average_hexagon_area(9, unit='km^2') * 100
HIGH_SCORE = 70

def rate(col: str, base: str) -> str:
    """Department rate: Σ rate·base / Σ base over the hexagons that have the rate."""
    return f'SUM({col} * {base}) / NULLIF(SUM(CASE WHEN {col} IS NOT NULL THEN {base} END), 0)'


def count(col: str, base: str) -> str:
    return f'SUM({col} / 100.0 * {base})'


# layer → {rank field: (SQL aggregate, decimals)}
SPECS = {
    'pm25_drivers': {
        'pm25_ug': ('AVG(c_pm25_mean)', 1),
        'pm25_delta': ('AVG(c_pm25_mean_delta)', 1),
    },
    'accessibility': {
        'min_cabecera': ('AVG(travel_min_cabecera_raw)', 0),
        'min_capital': ('AVG(travel_min_capital_raw)', 0),
        'km_hospital': ('AVG(dist_nearest_hospital_km_raw)', 1),
    },
    'land_use': {
        'trees_pct': ('100 * AVG(frac_trees)', 1), 'trees_ha': (f'SUM(frac_trees) * {HA}', 0),
        'crops_pct': ('100 * AVG(frac_crops)', 1), 'crops_ha': (f'SUM(frac_crops) * {HA}', 0),
        'built_pct': ('100 * AVG(frac_built)', 1), 'built_ha': (f'SUM(frac_built) * {HA}', 0),
    },
    'forestry_aptitude': {
        'high_ha': (f'SUM(CASE WHEN score >= {HIGH_SCORE} THEN 1 ELSE 0 END) * {HA}', 0),
        'high_pct': (f'100 * AVG(CASE WHEN score >= {HIGH_SCORE} THEN 1.0 ELSE 0 END)', 1),
    },
    'agri_potential': {
        'high_ha': (f'SUM(CASE WHEN score >= {HIGH_SCORE} THEN 1 ELSE 0 END) * {HA}', 0),
        'high_pct': (f'100 * AVG(CASE WHEN score >= {HIGH_SCORE} THEN 1.0 ELSE 0 END)', 1),
    },
    'service_deprivation': {
        'nbi_pct': (rate('c_nbi', 'hog_total'), 1), 'nbi_n': (count('c_nbi', 'hog_total'), 0),
        'cloacas_pct': (rate('c_sin_cloacas', 'hog_total'), 1), 'cloacas_n': (count('c_sin_cloacas', 'hog_total'), 0),
        'hacin_pct': (rate('c_hacinamiento', 'hog_total'), 1), 'hacin_n': (count('c_hacinamiento', 'hog_total'), 0),
    },
    # Minutes weighted by people: the time the average resident needs.
    'health_access': {
        'health_min': (rate('c_healthcare_time', 'pob_total'), 0),
        'health_cov_pct': (rate('c_health_coverage', 'pob_total'), 1),
    },
    'education_capital': {
        'noschool_pct': (rate('c_no_schooling', 'pob_18_mas'), 1),
        'univ_pct': (rate('c_university', 'pob_18_mas'), 1),
    },
    # Teen motherhood is not shown per hexagon (girls 14–17: median 19 per radio);
    # summed per department it is a readable rate.
    'education_flow': {
        'dropout_sec_pct': (rate('c_dropout_secondary', 'pob_13_18'), 1),
        'teen_preg_pct': (rate('c_teen_pregnancy', 'muj_14_17'), 1),
    },
    'sociodemographic': {
        'nbi_pct': (rate('pct_nbi', 'hog_total'), 1),
    },
    'economic_activity': {
        'empleo_pct': (rate('tasa_empleo', 'pob_14_mas'), 1),
        'univ_pct': (rate('pct_universitario', 'pob_18_mas'), 1),
        'viirs': ('AVG(viirs_mean_radiance)', 2),
    },
}


# Fields where 0 cannot be a department mean (minutes, km, µg/m³): a 0 there is
# a missing input averaged as 0 — accessibility has no cabecera times for Chaco
# and Formosa and almost no hospital distances in Brazil.
ZERO_IS_MISSING = {
    'accessibility': ['min_cabecera', 'min_capital', 'km_hospital'],
    'health_access': ['health_min'],
    'pm25_drivers': ['pm25_ug'],
}
MIN_COVERAGE = 0.8   # a field shows in a territory only if ≥ 80 % of departments have it
# Fields excluded per territory, if an input is not comparable there. Empty since
# 29-oct-2026: health_access used proxies outside Misiones (distance × 12 min/km
# in Corrientes, time-to-cities in Chaco and Formosa) until build_oxford_radios.py
# put the real Oxford/MAP healthcare times in.
EXCLUDE: dict = {}


def clean(layer: str, summary: dict, tid: str) -> list:
    """Apply EXCLUDE, ZERO_IS_MISSING and MIN_COVERAGE in place; return fields dropped."""
    depts = [d for d in summary['departments'] if d.get('rank')]
    dropped = []
    for field in SPECS[layer]:
        if tid in EXCLUDE.get((layer, field), set()):
            for d in depts:
                d['rank'][field] = None
        if field in ZERO_IS_MISSING.get(layer, []):
            for d in depts:
                if d['rank'].get(field) == 0:
                    d['rank'][field] = None
        have = sum(d['rank'].get(field) is not None for d in depts)
        if depts and have / len(depts) < MIN_COVERAGE:
            for d in depts:
                d['rank'][field] = None
            dropped.append(field)
    return dropped


def summary_path(layer: str, tid: str) -> str | None:
    names = [f'sat_{layer}_dept_summary.json'] if tid == 'misiones' else [f'{tid}_sat_{layer}_summary.json']
    for n in names:
        p = os.path.join(SRC_DATA_DIR, n)
        if os.path.exists(p):
            return p
    return None


def main() -> None:
    import duckdb
    ap = argparse.ArgumentParser()
    ap.add_argument('--layer', choices=list(SPECS))
    ap.add_argument('--clean-only', action='store_true', help='Re-apply the missing-data rules to the JSONs, no download')
    args = ap.parse_args()
    if args.clean_only:
        for layer in ([args.layer] if args.layer else SPECS):
            for tid in TERRITORY_CONFIGS:
                path = summary_path(layer, tid)
                if not path:
                    continue
                with open(path, encoding='utf-8') as f:
                    summary = json.load(f)
                dropped = clean(layer, summary, tid)
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(summary, f, ensure_ascii=False, indent=2)
                if dropped:
                    print(f'{layer}/{tid}: no data for {dropped}')
        return
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs; SET enable_http_metadata_cache = false; SET enable_external_file_cache = false;')
    for layer in ([args.layer] if args.layer else SPECS):
        spec = SPECS[layer]
        for tid in TERRITORY_CONFIGS:
            path = summary_path(layer, tid)
            if not path:
                continue
            with open(path, encoding='utf-8') as f:
                summary = json.load(f)
            prefix = get_territory(tid)['output_prefix']
            urls = {f"{CDN}/{prefix}sat_dpto/sat_{layer}_{d['parquetKey']}.parquet?{FRESH}": d['parquetKey']
                    for d in summary['departments']}
            url_list = ', '.join(f"'{u}'" for u in urls)
            sel = ', '.join(f'{expr} AS {field}' for field, (expr, _) in spec.items())
            try:
                df = con.execute(f'SELECT filename, {sel} FROM read_parquet([{url_list}], filename = true, '
                                 f'union_by_name = true) GROUP BY filename').df()
            except Exception as e:
                print(f'{layer}/{tid}: skipped ({str(e)[:120]})', flush=True)
                continue
            df['parquetKey'] = df.filename.map(urls)
            by = df.set_index('parquetKey')
            for d in summary['departments']:
                k = d['parquetKey']
                if k not in by.index:
                    continue
                r = by.loc[k]
                vals = {}
                for field, (_, dec) in spec.items():
                    v = r[field]
                    vals[field] = None if v != v or v is None else (round(float(v), dec) if dec else int(round(float(v))))
                d['rank'] = {**(d.get('rank') or {}), **vals}
            dropped = clean(layer, summary, tid)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            if dropped:
                print(f'{layer}/{tid}: no data for {dropped}', flush=True)
            first = next(iter(spec))
            top = sorted((d for d in summary['departments'] if (d.get('rank') or {}).get(first) is not None),
                         key=lambda d: -d['rank'][first])[:2]
            name = lambda d: d.get('dpto') or d.get('distrito') or d.get('municipio') or d['parquetKey']
            print(f'{layer}/{tid}: {len(summary["departments"])} · top {first}: ' +
                  ', '.join(f"{name(d)} {d['rank'][first]}" for d in top), flush=True)


if __name__ == '__main__':
    main()
