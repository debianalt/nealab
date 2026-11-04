"""
Census radios as a unit of their own: one parquet per AR territory with each INDEC 2022 radio's
rates, bases, queen neighbours and the H3 cells that carry its value on the map.

Why (diagnosis of 4-nov-2026 in the Hexoma): the census rates of the web layers are constant
inside the radio in 98 % of the res-9 cells (the rest are edge cells, a building-weighted mix).
There are ~6.900 radios for ~225 k cells, so a Moran, histogram or percentile counted over cells
weighs each radio as many times as it has cells (effective n ~30× smaller than it looks). The
panel uses this file to compute those statistics over radios.

Columns:
    redcode           INDEC 2022 radio (9 digits, text)
    dpto_key          department file key of the web (parquetKey), from the radio's 5-digit code
    <rate>            the census rates of the 6 layers, Σnum / Σden × 100 over the radio, with the same
                      definitions and MIN_BASE as build_census_rates.py (null when the base < MIN_BASE)
    tamano_medio_hogar  persons / households (base ≥ MIN_BASE)
    c_healthcare_time minutes to healthcare per radio (build_oxford_radios.py), as the cells receive it
    <base>            the radio totals (hog_total, pob_total, pob_18_mas, …), unfiltered
    vecinos           list of redcodes sharing an edge or a vertex (queen), inside the territory
    celdas            list of H3 res-9 cells whose dominant radio this is (most buildings of the
                      cell; ties by weight), i.e. the cells that show this radio's value
    n_celdas          len(celdas)

Usage:
    python pipeline/build_census_radios.py [--territory T] [--check]
Output: pipeline/output/<prefix>census_radios_2022.parquet
R2 (after review): data/<prefix>census_radios_2022.parquet, then npm run purge-cdn.
"""

import argparse
import os
import sys

import geopandas as gpd
import numpy as np
import pandas as pd
from libpysal.weights import Queen

sys.path.insert(0, os.path.dirname(__file__))
from build_census_rates import MIN_BASE, RATES, TERRITORIES, crosswalk_path, radio_counts  # noqa: E402
from config import OUTPUT_DIR, get_territory  # noqa: E402

OUT_NAME = 'census_radios_2022.parquet'
HEXOMA = 'postgresql://postgres@localhost:5432/hexoma'


def radios_geom(t: str) -> gpd.GeoDataFrame:
    p = (os.path.join(OUTPUT_DIR, 'radios_misiones.parquet') if t == 'misiones'
         else os.path.join(OUTPUT_DIR, t, f'radios_{t}.parquet'))
    g = gpd.read_parquet(p)[['redcode', 'geometry']]
    g['redcode'] = g.redcode.astype(str)
    return g.reset_index(drop=True)


def oxford_minutes(t: str) -> pd.Series:
    p = (os.path.join(OUTPUT_DIR, 'radio_data', 'oxford_accessibility.parquet') if t == 'misiones'
         else os.path.join(OUTPUT_DIR, t, f'oxford_accessibility_{t}.parquet'))
    d = pd.read_parquet(p, columns=['redcode', 'accessibility_healthcare_min'])
    return d.set_index(d.redcode.astype(str)).accessibility_healthcare_min.round(1)


def dept_keys(territory: str) -> dict[str, str]:
    """5-digit department code → parquetKey, by majority of the served cells of each code (Hexoma)."""
    import psycopg2
    conn = psycopg2.connect(HEXOMA)
    q = """SELECT left(cr.redcode::text, 5) AS cod, d.parquet_key, count(*) AS n
           FROM cruce.celda_radio cr
           JOIN capa_health_access c USING (h3)
           JOIN departamentos d USING (depto_id)
           JOIN territorios t ON t.territorio_id = d.territorio_id
           WHERE cr.anio = 2022 AND cr.frac >= 0.5 AND t.territorio = %s AND c.principal
           GROUP BY 1, 2"""
    df = pd.read_sql(q, conn, params=(territory,))
    conn.close()
    df = df.sort_values('n', ascending=False).drop_duplicates('cod')
    return dict(zip(df.cod, df.parquet_key))


def dominant_cells(t: str) -> pd.Series:
    xw = pd.read_parquet(crosswalk_path(t), columns=['h3index', 'redcode', 'weight', 'n_buildings'])
    xw['redcode'] = xw.redcode.astype(str)
    xw = xw.sort_values(['h3index', 'n_buildings', 'weight'], ascending=[True, False, False])
    dom = xw.drop_duplicates('h3index')
    return dom.groupby('redcode').h3index.apply(lambda s: sorted(s.tolist()))


def build(t: str, codprov: str, counts: pd.DataFrame) -> pd.DataFrame:
    g = radios_geom(t)
    c = counts[counts.redcode.str.startswith(codprov)].set_index('redcode')
    out = pd.DataFrame({'redcode': g.redcode})
    keys = dept_keys(t)
    out['dpto_key'] = out.redcode.str[:5].map(keys)

    bases = {}
    for layer, rates in RATES.items():
        for col, (num, (den, base), min_base) in rates.items():
            n, d = out.redcode.map(c[num]), out.redcode.map(c[den])
            out[col] = (n / d * 100).where(d >= min_base).round(1)
            bases[base] = d.round(1)
    p, h = out.redcode.map(c['p_total']), out.redcode.map(c['h_total'])
    out['tamano_medio_hogar'] = (p / h).where(h >= MIN_BASE).round(2)
    out['c_healthcare_time'] = out.redcode.map(oxford_minutes(t))
    for base, s in bases.items():
        out[base] = s

    w = Queen.from_dataframe(g, use_index=False, silence_warnings=True)
    ids = g.redcode.tolist()
    out['vecinos'] = [sorted(ids[j] for j in w.neighbors[i]) for i in range(len(ids))]
    cells = dominant_cells(t)
    out['celdas'] = out.redcode.map(cells).apply(lambda v: v if isinstance(v, list) else [])
    out['n_celdas'] = out.celdas.str.len().astype('int32')

    islas = [ids[i] for i in w.islands]
    sin_key = out.dpto_key.isna().sum()
    print(f'{t}: {len(out):,} radios, {out.n_celdas.sum():,} celdas, islas {len(islas)} {islas[:8]}, '
          f'sin dpto_key {sin_key} ({out.loc[out.dpto_key.isna(), "n_celdas"].sum()} celdas), '
          f'vecinos mediana {int(np.median([len(v) for v in out.vecinos]))}')
    return out


def check(t: str, out: pd.DataFrame) -> None:
    """Cells of a single radio must carry exactly the radio's rate in the served parquets."""
    prefix = get_territory(t)['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, prefix.rstrip('/')) if prefix else OUTPUT_DIR
    xw = pd.read_parquet(crosswalk_path(t), columns=['h3index', 'redcode'])
    single = xw.groupby('h3index').redcode.agg(['nunique', 'first'])
    single = single[single['nunique'] == 1]['first'].astype(str)
    r = out.set_index('redcode')
    # symmetry of the neighbour lists
    pares = {(a, b) for a, vs in zip(out.redcode, out.vecinos) for b in vs}
    asim = sum((b, a) not in pares for a, b in pares)
    print(f'  {t}: vecinos asimétricos {asim}')
    for layer, rates in RATES.items():
        f = os.path.join(t_dir, f'sat_{layer}.parquet')
        if not os.path.exists(f):
            print(f'  {t}/{layer}: sin copia local de lo servido, salteado')
            continue
        cols = [c for c in rates if c != 'c_teen_pregnancy']
        if layer == 'health_access':
            cols.append('c_healthcare_time')
        if layer == 'sociodemographic':
            cols.append('tamano_medio_hogar')
        served = pd.read_parquet(f, columns=['h3index'] + [c for c in cols if c != 'c_healthcare_time']
                                 + (['c_healthcare_time'] if 'c_healthcare_time' in cols else []))
        served = served[served.h3index.isin(single.index)]
        rad = served.h3index.map(single)
        for col in cols:
            if col not in served:
                continue
            a = served[col]
            b = rad.map(r[col])
            m = a.notna() & b.notna()
            # one rounding step (x.x5 lands on either side: w·num / w·den vs num / den in floating point)
            step = 0.01 if col == 'tamano_medio_hogar' else 0.1
            diff = (a - b).abs()
            exact = diff[m] <= step / 10
            ok = diff[m] <= step * 1.001
            sin_radio = int((a.notna() & b.isna()).sum())      # value on the map but not in the radio: an error
            sin_celda = int((a.isna() & b.notna()).sum())      # radio has a rate, served cell is empty: coverage gap
            print(f'  {t}/{layer}.{col}: {m.sum():,} celdas de un solo radio; iguales {100 * exact.mean():.2f} %, '
                  f'a un redondeo {100 * ok.mean():.2f} %; celda vacía con radio con dato {sin_celda:,}')
            if not ok.all() or sin_radio:
                sys.exit(f'{t}/{layer}.{col}: {int((~ok).sum())} celdas difieren del radio más que un redondeo, '
                         f'{sin_radio} con valor y radio sin dato')


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--territory', choices=list(TERRITORIES))
    ap.add_argument('--check', action='store_true', help='compare with the local copies of the served parquets')
    a = ap.parse_args()
    counts = radio_counts()
    for t, codprov in TERRITORIES.items():
        if a.territory and t != a.territory:
            continue
        out = build(t, codprov, counts)
        prefix = get_territory(t)['output_prefix']
        path = os.path.join(OUTPUT_DIR, prefix, OUT_NAME) if prefix else os.path.join(OUTPUT_DIR, OUT_NAME)
        out.to_parquet(path, index=False, compression='zstd')
        print(f'  → {path} ({os.path.getsize(path) / 1e6:.2f} MB)')
        if a.check:
            check(t, out)


if __name__ == '__main__':
    main()
