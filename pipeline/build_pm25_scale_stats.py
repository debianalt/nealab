"""
Per-territory PM2.5 scale stats for the map legend and the methodology page.

The PM2.5 layer has two color scales: "provincial" (P2–P98 of the active
territory — the province/department/state is the unit that governs) and
"regional" (fixed range, comparable across territories). This script freezes
the provincial range and the share of hexagons in each WHO 2021 band per
territory into src/lib/content/pm25_scale.json, read by the frontend.

Reads the parquets SERVED by the CDN (what the user sees), not local outputs.
Re-run after any PM2.5 pipeline refresh:

    python pipeline/build_pm25_scale_stats.py
"""

import json
import os
import sys
from datetime import date

import duckdb

sys.path.insert(0, os.path.dirname(__file__))
from config import TERRITORY_CONFIGS

CDN = 'https://cdn.spatia.ar/data'
COL = 'c_pm25_mean'
# WHO 2021 AQG interim targets IT-4..IT-1 (µg/m³, annual mean) — same cuts as
# the layer's flowBands in src/lib/config.ts.
BANDS = [10, 15, 25, 35]
OUT = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'content', 'pm25_scale.json')


def main() -> None:
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    band_sql = ', '.join(
        [f'AVG(({COL} <= {BANDS[0]})::INT)']
        + [f'AVG(({COL} > {lo} AND {COL} <= {hi})::INT)' for lo, hi in zip(BANDS, BANDS[1:])]
        + [f'AVG(({COL} > {BANDS[-1]})::INT)']
    )
    territories = {}
    for tid, cfg in TERRITORY_CONFIGS.items():
        url = f"{CDN}/{cfg['output_prefix']}sat_pm25_drivers.parquet"
        try:
            row = con.execute(
                f'SELECT COUNT({COL}), MIN({COL}), QUANTILE_CONT({COL}, 0.02), '
                f'QUANTILE_CONT({COL}, 0.98), MAX({COL}), {band_sql} FROM \'{url}\''
            ).fetchone()
        except duckdb.Error as e:
            print(f'  {tid}: skipped ({str(e)[:80]})')
            continue
        n, lo, p2, p98, hi, *pct = row
        if not n:
            print(f'  {tid}: no values')
            continue
        territories[tid] = {
            'label': cfg['label'],
            'country': cfg['country'],
            'n': n,
            'min': round(lo, 1), 'p2': round(p2, 1), 'p98': round(p98, 1), 'max': round(hi, 1),
            'bandsPct': [round(p * 100, 1) for p in pct],
        }
        print(f"  {cfg['label']:<20} P2–P98 {p2:5.1f}–{p98:5.1f}  min–max {lo:5.1f}–{hi:5.1f}  n={n}")

    lo_all = min(t['min'] for t in territories.values())
    hi_all = max(t['max'] for t in territories.values())
    print(f'\nAll territories: {lo_all}–{hi_all} µg/m³ (regional scale must cover this)')

    out = {
        'generated': date.today().isoformat(),
        'source': 'ACAG V6 (2013–2022 annual mean), sat_pm25_drivers parquets served by the CDN',
        'column': COL,
        'bands': BANDS,
        'territories': territories,
    }
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent='\t')
        f.write('\n')
    print(f'Wrote {os.path.normpath(OUT)} ({len(territories)} territories)')


if __name__ == '__main__':
    main()
