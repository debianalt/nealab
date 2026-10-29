"""
Department rankings for the layers without a pipeline of their own (flood and
deforestation have build_flood_rank.py / build_deforest_rank.py).

Each value is a physical reading of the layer per department, computed from
the dept parquets the CDN serves and written into the `rank` block of each
department in the layer's dept summary JSON (src/lib/data):

- Census layers (4 AR provinces): household-weighted with the hexagon's
  hogares_estimados — a "% of households", not a mean of hexagons, which would
  weigh an empty rural cell like a city block. sociodemographic has no
  household column; its NBI (and economic_activity's employment) are served as
  percentiles, so those two rates come from local raw data (--census-rates).
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

CDN = 'https://cdn.spatia.ar/data'
SRC_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data')
HA = h3.average_hexagon_area(9, unit='km^2') * 100
HIGH_SCORE = 70

HOG = 'COALESCE(hogares_estimados, 0)'


def hog_pct(col: str) -> str:
    return f'SUM({col} * {HOG}) / NULLIF(SUM({HOG}), 0)'


def hog_count(col: str) -> str:
    return f'SUM({col} / 100.0 * {HOG})'


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
        'nbi_pct': (hog_pct('c_nbi_raw'), 1), 'nbi_n': (hog_count('c_nbi_raw'), 0),
        'cloacas_pct': (hog_pct('c_sin_cloacas_raw'), 1), 'cloacas_n': (hog_count('c_sin_cloacas_raw'), 0),
        'hacin_pct': (hog_pct('c_hacinamiento_raw'), 1), 'hacin_n': (hog_count('c_hacinamiento_raw'), 0),
    },
    'health_access': {
        'health_min': (hog_pct('c_healthcare_time_raw'), 0),
        'health_cov_pct': (hog_pct('c_health_coverage_raw'), 1),
    },
    'education_capital': {
        'noschool_pct': (hog_pct('c_no_schooling_raw'), 1),
        'univ_pct': (hog_pct('c_university_raw'), 1),
    },
    'education_flow': {
        'dropout_sec_pct': (hog_pct('c_dropout_secondary_raw'), 1),
        'teen_preg_pct': (hog_pct('c_teen_pregnancy_raw'), 1),
    },
    # sociodemographic.pct_nbi and economic_activity.tasa_empleo are served as
    # percentiles, not rates: their rankings come from local raw data
    # (--census-rates, local_census_rates). Here only night lights.
    'economic_activity': {
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


CENSUS_AR = ['misiones', 'corrientes', 'chaco', 'formosa']


def local_census_rates() -> None:
    """NBI (sociodemographic) and employment (economic_activity) as real rates.

    The served parquets carry these as 0–100 percentiles (aggregate_radio_to_h3
    ranks them "for petal compatibility"; the served Misiones copy also went
    through fill_h3_coverage, which writes 0 where there is no census). The real
    values exist locally: radio_stats_master (Misiones, per radio, with dpto and
    total_personas) and the *_raw columns of output/<t>/sat_*.parquet for the
    other three provinces. Population-weighted (NBI) / household-weighted
    (employment) department rates go into the same rank block.
    """
    import duckdb
    import pandas as pd
    out = os.path.join(os.path.dirname(__file__), 'output')
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs; SET enable_http_metadata_cache = false; SET enable_external_file_cache = false;')
    rs = pd.read_parquet(os.path.join(out, 'radio_stats_master.parquet'),
                         columns=['dpto', 'total_personas', 'pct_nbi', 'tasa_empleo'])
    for layer, field, col in [('sociodemographic', 'nbi_pop_pct', 'pct_nbi'),
                              ('economic_activity', 'empleo_pct', 'tasa_empleo')]:
        for tid in CENSUS_AR:
            path = summary_path(layer, tid)
            if not path:
                continue
            with open(path, encoding='utf-8') as f:
                summary = json.load(f)
            if tid == 'misiones':
                g = rs.dropna(subset=[col]).assign(w=lambda d: d.total_personas.fillna(0))
                rates = g.groupby('dpto').apply(lambda d: (d[col] * d.w).sum() / d.w.sum() if d.w.sum() else None)
                by_name = {str(k).strip().lower(): v for k, v in rates.items()}
                for d in summary['departments']:
                    v = by_name.get(str(d.get('dpto', '')).strip().lower())
                    d['rank'] = {**(d.get('rank') or {}), field: None if v is None else round(float(v), 1)}
            else:
                local = os.path.join(out, tid, f'sat_{layer}.parquet')
                weight = 'densidad_hab_km2_raw' if layer == 'sociodemographic' else 'hogares_estimados'
                hexes = pd.read_parquet(local, columns=['h3index', f'{col}_raw', weight])
                prefix = get_territory(tid)['output_prefix']
                urls = {f"{CDN}/{prefix}sat_dpto/sat_{layer}_{d['parquetKey']}.parquet": d['parquetKey']
                        for d in summary['departments']}
                url_list = ', '.join(f"'{u}'" for u in urls)
                assign = con.execute(f'SELECT DISTINCT h3index, filename FROM read_parquet([{url_list}], filename = true)').df()
                assign['parquetKey'] = assign.filename.map(urls)
                m = hexes.merge(assign[['h3index', 'parquetKey']], on='h3index')
                m = m[m[weight] > 0]
                rates = m.groupby('parquetKey').apply(lambda d: (d[f'{col}_raw'] * d[weight]).sum() / d[weight].sum())
                for d in summary['departments']:
                    v = rates.get(d['parquetKey'])
                    d['rank'] = {**(d.get('rank') or {}), field: None if v is None else round(float(v), 1)}
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)
            vals = sorted((d['rank'][field], d.get('dpto') or d['parquetKey']) for d in summary['departments']
                          if (d.get('rank') or {}).get(field) is not None)
            print(f'{layer}/{tid}: {len(vals)}/{len(summary["departments"])} · min {vals[0] if vals else None} · max {vals[-1] if vals else None}')


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
    ap.add_argument('--census-rates', action='store_true', help='NBI and employment rates from local raw census data')
    args = ap.parse_args()
    if args.census_rates:
        local_census_rates()
        return
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
            urls = {f"{CDN}/{prefix}sat_dpto/sat_{layer}_{d['parquetKey']}.parquet": d['parquetKey']
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
