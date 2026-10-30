"""
Census layers: real Census 2022 rates, each over an explicit total, in the 4 AR provinces.

Until 30-oct-2026 the census layers served their variables as provincial
percentiles, and the underlying radio rates did not always share a definition
across provinces (Misiones divided employment by the whole population and took
the elderly as 70+; Corrientes, Chaco and Formosa divided university by the whole
population and counted university graduates twice in "higher education"). This
recomputes every rate from the radio counts in PostGIS, the same way in the four
provinces:

    economic_activity    ocupados, activos / 14+ ; universitario completo / 18+
    service_deprivation  NBI, sin cloacas, sin piso, hacinamiento (crítico), sin computadora / hogares
    health_access        obra social o prepaga, 65+, menores de 18 / personas ; NBI / hogares
    education_capital    sin instrucción, secundario completo, superior completo, universitario / 18+
    education_flow       no asisten / 6-12 y 13-18 ; madres / mujeres de 14-17 (ranking only)
    sociodemographic     NBI, hacinamiento, propietarios, con computadora / hogares ; personas por hogar

Per hexagon it is a ratio estimator over the province's crosswalk (weight = share
of the radio's buildings in the hexagon): Σ w·numerator / Σ w·denominator. The
base columns (hog_total, pob_total, pob_18_mas, …) are Σ w·denominator: the
estimated households or people each rate is over, kept with one decimal so a
hexagon with a rate never shows "0 personas". Radios with fewer than MIN_BASE in
the denominator are left out (a 5-person radio reads as "100 %"), except teen
motherhood: its base (girls 14-17) has a median of 19 per radio, so it is not
shown per hexagon and only feeds the department ranking, unfiltered.
Population density is the hexagon's estimated people over its own area, not the
radio's density. Healthcare time is the Oxford/MAP travel time in minutes
already served as c_healthcare_time_raw. No census → null, not 0.

Starts from the parquets the CDN serves, so rows stay identical; drops the
percentile byproducts (<col>_raw, score, pca_1, pca_2). Publish with:

    python pipeline/build_census_rates.py [--layer L]
    python pipeline/refresh_dept_parquets.py --layer L --territory T --schema-change --backup-dir C:/Users/ant/r2-census-backup-2026-10
"""

import argparse
import os
import sys

import h3
import numpy as np
import pandas as pd
import psycopg2

sys.path.insert(0, os.path.dirname(__file__))
from config import OUTPUT_DIR, get_territory
from refresh_dept_parquets import CDN, fetch

PG_CENSUS = 'dbname=posadas user=postgres'
TERRITORIES = {'misiones': '54', 'corrientes': '18', 'chaco': '22', 'formosa': '34'}
MIN_BASE = 30

HOG = ('h_total', 'hog_total')
POB = ('p_total', 'pob_total')
P18 = ('p_18a', 'pob_18_mas')
P14 = ('p14', 'pob_14_mas')
# layer → {column: (numerator, (denominator, base column), min base)}
RATES = {
    'economic_activity': {
        'tasa_empleo': ('ocupados', P14, MIN_BASE),
        'tasa_actividad': ('activos', P14, MIN_BASE),
        'pct_universitario': ('p18a_unive', P18, MIN_BASE),
    },
    'service_deprivation': {
        'c_nbi': ('h_nbi', HOG, MIN_BASE),
        'c_sin_cloacas': ('h_cloaca', HOG, MIN_BASE),
        'c_piso': ('h_piso', HOG, MIN_BASE),
        'c_hacinamiento': ('h_hacinami', HOG, MIN_BASE),
        'c_hacinamiento_crit': ('h_hacina_1', HOG, MIN_BASE),
        'c_sin_computadora': ('h_sin_comp', HOG, MIN_BASE),
    },
    'health_access': {
        'c_health_coverage': ('p_cobertur', POB, MIN_BASE),
        'c_nbi': ('h_nbi', HOG, MIN_BASE),
        'c_elderly': ('p65_total', POB, MIN_BASE),
        'c_children': ('p_a17', POB, MIN_BASE),
    },
    'education_capital': {
        'c_no_schooling': ('p18a_sin_i', P18, MIN_BASE),
        'c_secondary_plus': ('p18a_secun', P18, MIN_BASE),
        'c_higher_edu': ('p18a_super', P18, MIN_BASE),
        'c_university': ('p18a_unive', P18, MIN_BASE),
    },
    'education_flow': {
        'c_dropout_primary': ('nasiste6a1', ('total_6a12', 'pob_6_12'), MIN_BASE),
        'c_dropout_secondary': ('nasiste_13', ('total13_18', 'pob_13_18'), MIN_BASE),
        'c_teen_pregnancy': ('ma_14a17', ('m_14a17', 'muj_14_17'), 1),
    },
    'sociodemographic': {
        'pct_nbi': ('h_nbi', HOG, MIN_BASE),
        'pct_hacinamiento': ('h_hacinami', HOG, MIN_BASE),
        'pct_propietario': ('h_propieta', HOG, MIN_BASE),
        'pct_computadora': ('h_computad', HOG, MIN_BASE),
    },
}
# Columns derived from the bases: (kind, target column)
DERIVED = {
    'health_access': [('density', 'c_pop_density'), ('healthcare_min', 'c_healthcare_time')],
    'sociodemographic': [('density', 'densidad_hab_km2'), ('hh_size', 'tamano_medio_hogar')],
}
DROP_PREFIXES = ('score', 'pca_')


def radio_counts() -> pd.DataFrame:
    conn = psycopg2.connect(PG_CENSUS)
    sql = {
        'act': """SELECT redcode::text redcode, SUM(ocupados) ocupados, SUM(activos) activos, SUM(total_pobl) p14
                  FROM censo_2022.censo_tasaactividad_2022 WHERE redcode IS NOT NULL GROUP BY 1""",
        'edu': """SELECT redcode::text redcode, p_total, p_a17, p_18a, p18a_sin_i, p18a_secun,
                         p18a_terci + p18a_unive AS p18a_super, p18a_unive, p_cobertur, p65_total
                  FROM censo_2022.censo_niveducativo_2022 WHERE redcode IS NOT NULL""",
        'hog': """SELECT redcode::text redcode, h_total, h_nbi, h_cloaca, h_piso, h_hacinami, h_hacina_1,
                         h_total - h_computad AS h_sin_comp, h_computad, h_propieta
                  FROM censo_2022.censo_nbi_2022 WHERE redcode IS NOT NULL""",
        'ina': """SELECT redcode::text redcode, nasiste6a1, total_6a12, nasiste_13, total13_18
                  FROM censo_2022.censo_children_noasisten_2022 WHERE redcode IS NOT NULL AND sexo = 0""",
        'fec': """SELECT redcode::text redcode, ma_14a17, m_14a17
                  FROM censo_2022.censo_fecundidad_2022 WHERE redcode IS NOT NULL""",
    }
    frames = []
    cur = conn.cursor()
    for q in sql.values():
        cur.execute(q)
        frames.append(pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description]).set_index('redcode'))
    conn.close()
    df = pd.concat(frames, axis=1).reset_index()
    for c in df.columns[1:]:
        df[c] = pd.to_numeric(df[c]).astype(float)
    return df


def crosswalk_path(t: str) -> str:
    if t == 'misiones':
        return os.path.join(OUTPUT_DIR, 'h3_radio_crosswalk.parquet')
    return os.path.join(OUTPUT_DIR, t, f'h3_radio_crosswalk_{t}.parquet')


def hex_sums(m: pd.DataFrame, num: str | None, den: str, min_base: float) -> pd.DataFrame:
    ok = m[m[den] >= min_base]
    out = pd.DataFrame({'d': ok.weight * ok[den]})
    if num:
        out['n'] = ok.weight * ok[num]
    out['h3index'] = ok.h3index
    g = out.groupby('h3index').sum()
    return g[g.d > 0]


def build(layer: str, t: str, codprov: str, counts: pd.DataFrame) -> None:
    prefix = get_territory(t)['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, prefix.rstrip('/')) if prefix else OUTPUT_DIR
    out = os.path.join(t_dir, f'sat_{layer}.parquet')
    fetch(f'{CDN}/{prefix}sat_{layer}.parquet', out)
    served = pd.read_parquet(out)

    radios = counts[counts.redcode.str.startswith(codprov)]
    xw = pd.read_parquet(crosswalk_path(t), columns=['h3index', 'redcode', 'weight'])
    xw['redcode'] = xw.redcode.astype(str)
    m = xw.merge(radios, on='redcode', how='inner')

    new = served.drop(columns=[c for c in served.columns if c.endswith('_raw') or c.startswith(DROP_PREFIXES)])
    bases = {}
    for col, (num, (den, base), min_base) in RATES[layer].items():
        g = hex_sums(m, num, den, min_base)
        new[col] = new.h3index.map((g.n / g.d * 100).round(1))
        bases.setdefault(base, new.h3index.map(g.d.round(1)))
        # The hexagon totals must give back the province's radio totals.
        r = radios[radios[den] >= min_base]
        prov = r[num].sum() / r[den].sum() * 100
        cov = new[col].notna()
        w = new.h3index[cov].map(g.d)
        hexp = (new.loc[cov, col] * w).sum() / w.sum()
        print(f'{layer}/{t} {col}: {cov.sum():,} hexes, province {prov:.1f} %, hexes {hexp:.1f} %, '
              f'median {new[col].median():.1f}, P98 {new[col].quantile(.98):.1f}')
        if abs(prov - hexp) > 0.5:
            sys.exit(f'{layer}/{t} {col}: hexagons do not add up to the province ({hexp:.1f} vs {prov:.1f})')
    for kind, col in DERIVED.get(layer, []):
        if kind == 'density':
            g = hex_sums(m, None, 'p_total', 0)
            area = new.h3index.map(lambda h: h3.cell_area(h, 'km^2'))
            new[col] = (new.h3index.map(g.d) / area).round(1)
        elif kind == 'hh_size':
            g = hex_sums(m, 'p_total', 'h_total', MIN_BASE)
            new[col] = new.h3index.map((g.n / g.d).round(2))
        elif kind == 'healthcare_min':
            new[col] = served['c_healthcare_time_raw'].round(1)
        print(f'{layer}/{t} {col}: {new[col].notna().sum():,} hexes, median {new[col].median():.1f}, '
              f'P98 {new[col].quantile(.98):.1f}')
    for base, s in bases.items():
        new[base] = s
    if layer == 'economic_activity':
        bad = (new.tasa_actividad < new.tasa_empleo).sum()
        if bad:
            sys.exit(f'{t}: {bad} hexagons with activity below employment')
    if len(new) != len(served) or not new.h3index.equals(served.h3index):
        sys.exit(f'{layer}/{t}: rows differ from served')
    new.to_parquet(out, index=False, compression='zstd')
    print(f'Wrote {out} ({len(new):,} rows, dropped {sorted(set(served.columns) - set(new.columns))})')


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--layer', choices=list(RATES))
    args = ap.parse_args()
    counts = radio_counts()
    for layer in [args.layer] if args.layer else RATES:
        for t, codprov in TERRITORIES.items():
            build(layer, t, codprov, counts)


if __name__ == '__main__':
    main()
