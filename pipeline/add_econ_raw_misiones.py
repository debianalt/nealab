"""
Add the real census rates to Misiones' economic_activity parquet.

tasa_empleo, tasa_actividad and pct_universitario are served as provincial
percentiles (aggregate_radio_to_h3.py ranks them). Corrientes, Chaco and
Formosa also carry the rates as <col>_raw; Misiones lost them when the parquet
was rewritten downstream (n_edificios / hogares_estimados). This puts them back
with the same weighted formula as aggregate_radio_to_h3.py (crosswalk weight ×
radio rate), only on the hexagons that carry a percentile.

    python pipeline/add_econ_raw_misiones.py
    python pipeline/refresh_dept_parquets.py --layer economic_activity --territory misiones --add-cols --backup-dir C:/Users/ant/r2-econ-backup-2026-10
"""

import os
import sys

import pandas as pd
from scipy.stats import spearmanr

sys.path.insert(0, os.path.dirname(__file__))
from config import OUTPUT_DIR

COLS = ['tasa_empleo', 'tasa_actividad', 'pct_universitario']


def main() -> None:
    path = os.path.join(OUTPUT_DIR, 'sat_economic_activity.parquet')
    df = pd.read_parquet(path)
    xwalk = pd.read_parquet(os.path.join(OUTPUT_DIR, 'h3_radio_crosswalk.parquet'))
    radio = pd.read_parquet(os.path.join(OUTPUT_DIR, 'radio_stats_master.parquet'))
    merged = xwalk.merge(radio[['redcode'] + COLS], on='redcode', how='inner')

    df = df.drop(columns=[f'{c}_raw' for c in COLS if f'{c}_raw' in df.columns])
    for col in COLS:
        valid = merged[['h3index', 'weight', col]].dropna(subset=[col])
        num = (valid['weight'] * valid[col]).groupby(valid['h3index']).sum()
        den = valid['weight'].groupby(valid['h3index']).sum()
        raw = (num / den).round(2).rename(f'{col}_raw')
        df = df.merge(raw, left_on='h3index', right_index=True, how='left')
        # Only where the percentile exists: 0 marks a hexagon with no census data.
        df.loc[~(df[col] > 0), f'{col}_raw'] = None
        both = df[df[col] > 0]
        rho = spearmanr(both[col], both[f'{col}_raw'], nan_policy='omit').statistic
        print(f'{col}_raw: {both[f"{col}_raw"].notna().sum():,} hexes, median {both[f"{col}_raw"].median():.2f}, '
              f'Spearman vs percentile {rho:.4f}')
        if rho < 0.99:
            sys.exit(f'{col}: raw rate does not follow the served percentile')

    df.to_parquet(path, index=False, compression='zstd')
    print(f'Wrote {path} ({len(df):,} rows)')


if __name__ == '__main__':
    main()
