"""Compare staged census layers (regrid_census.py) against the served source copies.
Usage: python pipeline/compare_regrid.py <territory> <backup-dir> <stage-dir>"""
import os, sys
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from config import get_territory
t, bdir, stage = sys.argv[1:4]
prefix = get_territory(t)['output_prefix']
for layer in ['economic_activity', 'service_deprivation', 'health_access', 'education_capital',
              'education_flow', 'sociodemographic', 'censo_temporal']:
    a = pd.read_parquet(os.path.join(bdir, 'source', prefix, f'sat_{layer}.parquet')).set_index('h3index')
    b = pd.read_parquet(os.path.join(stage, prefix, f'sat_{layer}.parquet')).set_index('h3index')
    common = a.index.intersection(b.index)
    print(f'== {layer}: filas {len(a):,} → {len(b):,} (comunes {len(common):,}); columnas iguales: {list(a.columns) == list(b.columns)}')
    a, b = a.loc[common], b.loc[common]
    for c in a.columns:
        if c not in b.columns:
            continue
        x, y = a[c], b[c]
        if x.dtype == object or y.dtype == object:
            ch = (x.fillna('∅').astype(str) != y.fillna('∅').astype(str)).sum()
            if ch: print(f'   {c:<26} texto distinto en {ch:,}')
            continue
        x, y = x.astype(float), y.astype(float)
        lost, gained = (x.notna() & y.isna()).sum(), (x.isna() & y.notna()).sum()
        both = x.notna() & y.notna()
        d = (y[both] - x[both]).abs()
        ch = (d > 1e-6).sum()
        if ch or lost or gained:
            rel = (d / x[both].abs().replace(0, np.nan)).median()
            print(f'   {c:<26} cambia {ch:>7,} | pierde {lost:>6,} | gana {gained:>6,} | |Δ| mediana {d[d > 1e-6].median() if ch else 0:.3g}, P95 {d.quantile(.95):.3g}')
