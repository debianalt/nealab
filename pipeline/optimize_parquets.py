"""Recompress per-dpto parquets for fast DuckDB-WASM range reads.

Three optimisations stack:

1. Sort rows by h3index. Adjacent hexagons end up in adjacent rows, so the
   single column read DuckDB-WASM does for the choropleth (h3index + score)
   pulls the bytes in spatial order — better network locality on R2.

2. Smaller row groups (default 50 000 rows). A 288 K-hex parquet like
   Patiño currently lives in one row group, forcing the entire 13.6 MB file
   to be downloaded before any data renders. With 6 row groups, DuckDB-WASM
   can skip statistics-pruned groups.

3. ZSTD compression with high level (default 9). Saves 30–50 % file size
   over SNAPPY for these wide-float schemas; column statistics tighten.

Optional: --drop-temporal removes *_baseline / *_delta / score_baseline /
delta_score columns when the layer isn't actively browsed in temporal mode.
For productive_activity Patiño this halves column count and roughly halves
the file size again. Pure-temporal layers (carbon_stock with its diff
analysis) opt out by skipping --drop-temporal.

Usage:
  python pipeline/optimize_parquets.py --territory all --layer all --drop-temporal
  python pipeline/optimize_parquets.py --territory formosa --layer productive_activity
"""
from __future__ import annotations
import argparse
import os
import sys
from pathlib import Path
import multiprocessing as mp

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO / "pipeline" / "output"

TERRITORIES = ['misiones', 'corrientes', 'itapua_py', 'alto_parana_py',
               'chaco', 'formosa', 'parana_br', 'santa_catarina_br',
               'rio_grande_sul_br']

LAYERS = [
    'carbon_stock', 'pm25_drivers', 'productive_activity',
    'climate_vulnerability', 'soil_water', 'deforestation_dynamics',
    'environmental_risk', 'climate_comfort', 'green_capital',
    'change_pressure', 'forest_health',
]


def territory_dpto_dir(t: str) -> Path:
    if t == 'misiones':
        return OUTPUT_DIR / "sat_dpto"
    return OUTPUT_DIR / t / "sat_dpto"


def optimize_one(args):
    path, drop_temporal = args
    try:
        df = pd.read_parquet(path)
        n0 = len(df)
        if n0 == 0:
            return (path, 'empty', 0, 0)
        if 'h3index' not in df.columns:
            return (path, 'no_h3index', 0, 0)
        # Drop temporal columns if requested
        before_size = path.stat().st_size
        if drop_temporal:
            drop_cols = [c for c in df.columns
                         if c.endswith('_baseline') or c.endswith('_delta')
                         or c in ('score_baseline', 'delta_score')]
            df = df.drop(columns=drop_cols, errors='ignore')
        # Sort by h3index
        df = df.sort_values('h3index').reset_index(drop=True)
        # Write with smaller row groups + ZSTD
        df.to_parquet(path, index=False, compression='zstd',
                      compression_level=9, row_group_size=50_000)
        after_size = path.stat().st_size
        return (path, 'ok', before_size, after_size)
    except Exception as e:
        return (path, f'error: {e}', 0, 0)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--territory', default='all',
                    help='territory id or "all"')
    ap.add_argument('--layer', default='all',
                    help='layer id or "all"')
    ap.add_argument('--drop-temporal', action='store_true',
                    help='drop *_baseline / *_delta cols (not for temporal mode)')
    ap.add_argument('--workers', type=int, default=4)
    args = ap.parse_args()

    territories = TERRITORIES if args.territory == 'all' else [args.territory]
    layers = LAYERS if args.layer == 'all' else [args.layer]

    tasks = []
    for t in territories:
        dpto_dir = territory_dpto_dir(t)
        if not dpto_dir.exists():
            print(f'skip {t}: no sat_dpto dir')
            continue
        for layer in layers:
            for f in sorted(dpto_dir.glob(f'sat_{layer}_*.parquet')):
                tasks.append((f, args.drop_temporal))

    print(f'optimizing {len(tasks)} parquets across {len(territories)} territories x {len(layers)} layers, workers={args.workers}')
    total_before = 0
    total_after = 0
    ok = 0
    errs = 0
    with mp.Pool(args.workers) as pool:
        for i, (path, status, before, after) in enumerate(pool.imap_unordered(optimize_one, tasks, chunksize=4), 1):
            if status == 'ok':
                ok += 1
                total_before += before
                total_after += after
            else:
                errs += 1
                print(f'  {path}: {status}')
            if i % 200 == 0:
                pct = 100 * total_after / total_before if total_before else 0
                print(f'  progress {i}/{len(tasks)} | total_before={total_before/1e6:.1f}MB total_after={total_after/1e6:.1f}MB ({pct:.0f}%)')

    pct = 100 * total_after / total_before if total_before else 0
    print(f'done: {ok} ok, {errs} errors')
    print(f'total: {total_before/1e6:.1f}MB -> {total_after/1e6:.1f}MB ({pct:.0f}%)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
