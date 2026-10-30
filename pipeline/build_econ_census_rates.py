"""
economic_activity: real census rates, with the INDEC denominator, in the 4 AR provinces.

Until 30-oct-2026 the layer served tasa_empleo, tasa_actividad and pct_universitario
as provincial percentiles, and the underlying rates did not share a denominator:
Misiones (radio_stats_master) divided employment and activity by the whole
population and university by the 18+; Corrientes, Chaco and Formosa
(build_radio_stats_corrientes.py) divided employment and activity by the 14+ and
university by the whole population. This recomputes all three from the radio
counts in PostGIS with the INDEC definitions:

    tasa_empleo       = ocupados / población de 14 años y más
    tasa_actividad    = activos  / población de 14 años y más
    pct_universitario = universitario completo / población de 18 años y más

Per hexagon it is a ratio estimator over the province's crosswalk (weight = share
of the radio's buildings in the hexagon): Σ w·numerator / Σ w·denominator.
pob_14_mas and pob_18_mas (Σ w·denominator) are the estimated people each rate is
computed over. Radios with fewer than MIN_BASE people in the denominator are left
out (a 5-person radio reads as "100 % universitarios"). No census → null, not 0.

Starts from the parquet the CDN serves, so rows stay identical; drops the old
percentile byproducts (<col>_raw, score, pca_1, pca_2). Publish with:

    python pipeline/build_econ_census_rates.py
    python pipeline/refresh_dept_parquets.py --layer economic_activity --territory <t> --schema-change --backup-dir C:/Users/ant/r2-econ-backup-2026-10b
"""

import os
import sys

import numpy as np
import pandas as pd
import psycopg2

sys.path.insert(0, os.path.dirname(__file__))
from config import OUTPUT_DIR, get_territory
from refresh_dept_parquets import CDN, fetch

PG_CENSUS = 'dbname=posadas user=postgres'
TERRITORIES = {'misiones': '54', 'corrientes': '18', 'chaco': '22', 'formosa': '34'}
MIN_BASE = 30
# column → (numerator, denominator, base column shown in the panel)
RATES = {
    'tasa_empleo': ('ocupados', 'p14', 'pob_14_mas'),
    'tasa_actividad': ('activos', 'p14', 'pob_14_mas'),
    'pct_universitario': ('univ', 'p18', 'pob_18_mas'),
}
DROP = ['tasa_empleo_raw', 'tasa_actividad_raw', 'pct_universitario_raw', 'score', 'pca_1', 'pca_2']


def radio_counts() -> pd.DataFrame:
    conn = psycopg2.connect(PG_CENSUS)
    cur = conn.cursor()
    cur.execute("""
        SELECT redcode::text, a.ocupados, a.activos, a.p14, e.univ, e.p18
        FROM (SELECT redcode, SUM(ocupados) ocupados, SUM(activos) activos, SUM(total_pobl) p14
              FROM censo_2022.censo_tasaactividad_2022 WHERE redcode IS NOT NULL GROUP BY redcode) a
        FULL JOIN (SELECT redcode, SUM(p18a_unive) univ, SUM(p_18a) p18
                   FROM censo_2022.censo_niveducativo_2022 WHERE redcode IS NOT NULL GROUP BY redcode) e
        USING (redcode)
    """)
    df = pd.DataFrame(cur.fetchall(), columns=['redcode', 'ocupados', 'activos', 'p14', 'univ', 'p18'])
    conn.close()
    for c in df.columns[1:]:
        df[c] = pd.to_numeric(df[c]).astype(float)
    return df


def crosswalk_path(t: str) -> str:
    if t == 'misiones':
        return os.path.join(OUTPUT_DIR, 'h3_radio_crosswalk.parquet')
    return os.path.join(OUTPUT_DIR, t, f'h3_radio_crosswalk_{t}.parquet')


def build(t: str, codprov: str, counts: pd.DataFrame) -> None:
    prefix = get_territory(t)['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, prefix.rstrip('/')) if prefix else OUTPUT_DIR
    out = os.path.join(t_dir, 'sat_economic_activity.parquet')
    fetch(f'{CDN}/{prefix}sat_economic_activity.parquet', out)
    served = pd.read_parquet(out)

    radios = counts[counts.redcode.str.startswith(codprov)].copy()
    xw = pd.read_parquet(crosswalk_path(t), columns=['h3index', 'redcode', 'weight'])
    xw['redcode'] = xw.redcode.astype(str)
    m = xw.merge(radios, on='redcode', how='inner')

    new = served.drop(columns=[c for c in DROP if c in served.columns])
    bases = {}
    for col, (num, den, base) in RATES.items():
        ok = m[m[den] >= MIN_BASE]
        g = ok.assign(n=ok.weight * ok[num], d=ok.weight * ok[den]).groupby('h3index')[['n', 'd']].sum()
        g = g[g.d > 0]
        rate = (g.n / g.d * 100).round(1)
        new[col] = new.h3index.map(rate)
        bases.setdefault(base, new.h3index.map(g.d.round(0)))
        # The hexagon totals must give back the province's radio totals.
        r = radios[radios[den] >= MIN_BASE]
        prov = r[num].sum() / r[den].sum() * 100
        cov = new[col].notna()
        hexp = (new.loc[cov, col] * new.h3index[cov].map(g.d)).sum() / new.h3index[cov].map(g.d).sum()
        print(f'{t} {col}: {cov.sum():,} hexes, province {prov:.1f} %, hexes {hexp:.1f} %, '
              f'median {new[col].median():.1f}, P90 {new[col].quantile(.9):.1f}')
        if abs(prov - hexp) > 0.5:
            sys.exit(f'{t} {col}: hexagons do not add up to the province ({hexp:.1f} vs {prov:.1f})')
    for base, s in bases.items():
        new[base] = s.astype('Int64')
    bad = (new.tasa_actividad < new.tasa_empleo).sum()
    if bad:
        sys.exit(f'{t}: {bad} hexagons with activity below employment')
    if len(new) != len(served) or not new.h3index.equals(served.h3index):
        sys.exit(f'{t}: rows differ from served')
    gap = ((new.n_edificios > 0) & new.tasa_empleo.isna()).sum()
    print(f'{t}: {gap:,} hexagons with buildings and no census rate '
          f'(served had {((served.n_edificios > 0) & ~(served.tasa_empleo > 0)).sum():,})')
    new.to_parquet(out, index=False, compression='zstd')
    print(f'Wrote {out} ({len(new):,} rows)')


def main() -> None:
    counts = radio_counts()
    for t, codprov in TERRITORIES.items():
        build(t, codprov, counts)


if __name__ == '__main__':
    main()
