"""
Publish the Paraguay layers recomputed by refresh_py_coverage.py (3-nov-2026).

Per (territory, layer): the territory parquet and every department parquet that
split_by_admin.py wrote, after checks against what the CDN served (backup made
beforehand, see --backup-dir):
  - same columns as the served territory and department files
  - territory rows = crosswalk grid (deforestation and PM2.5 drop cells by rule:
    no tree cover in 2000 / no reading; those are allowed to be fewer)
  - the department files partition the territory file (no cell lost or doubled)
  - forestry: no cell scored with zero climate (gdd or precip_total == 0)
Uploads with upload_to_r2.upload_file (size check). Bump ?v / DEPT_V, re-run the
rankings (build_deforest_rank.py, build_layer_ranks.py) and purge the CDN after.

    python pipeline/publish_py_coverage.py --layers deforestation_dynamics,pm25_drivers,forestry_aptitude [--only t1,t2] [--dry-run]
"""
import argparse
import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import OUTPUT_DIR, get_territory

SRC_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data')
T12 = ['cordillera_py', 'guaira_py', 'caaguazu_py', 'caazapa_py', 'misiones_py', 'central_py',
       'neembucu_py', 'amambay_py', 'canindeyu_py', 'presidente_hayes_py', 'boqueron_py',
       'alto_paraguay_py']
FEWER_OK = {'deforestation_dynamics', 'pm25_drivers'}


def check(t: str, layer: str, backup_dir: str) -> tuple:
    prefix = get_territory(t)['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, prefix.rstrip('/'))
    grid = pd.read_parquet(os.path.join(t_dir, 'h3_admin_crosswalk.parquet'), columns=['h3index'])
    main = pd.read_parquet(os.path.join(t_dir, f'sat_{layer}.parquet'))
    served = pd.read_parquet(os.path.join(backup_dir, t, f'sat_{layer}.parquet'))
    errs = []
    if main.columns.tolist() != served.columns.tolist():
        errs.append(f'territory columns {main.columns.tolist()} != served {served.columns.tolist()}')
    if main.h3index.duplicated().any():
        errs.append('duplicated cells in territory file')
    if not set(main.h3index) <= set(grid.h3index):
        errs.append(f'{len(set(main.h3index) - set(grid.h3index))} cells outside the grid')
    if layer not in FEWER_OK and len(main) != len(grid):
        errs.append(f'territory rows {len(main)} != grid {len(grid)}')
    if layer == 'forestry_aptitude':
        zero = int(sum((main[c] == 0).sum() for c in ('gdd', 'precip_total') if c in main.columns))
        if zero:
            errs.append(f'{zero} cells with zero climate')
    summary_path = os.path.join(SRC_DATA_DIR, f'{t}_sat_{layer}_summary.json')
    summary = json.load(open(summary_path, encoding='utf-8'))
    staged = [(os.path.join(t_dir, f'sat_{layer}.parquet'), f'data/{prefix}sat_{layer}.parquet')]
    served_dept = os.listdir(os.path.join(backup_dir, t, 'sat_dpto'))
    ref = next(f for f in served_dept if f.startswith(f'sat_{layer}_'))
    ref_cols = pd.read_parquet(os.path.join(backup_dir, t, 'sat_dpto', ref)).columns.tolist()
    cells = []
    for d in summary['departments']:
        name = f"sat_{layer}_{d['parquetKey']}.parquet"
        p = os.path.join(t_dir, 'sat_dpto', name)
        df = pd.read_parquet(p)
        if df.columns.tolist() != ref_cols:
            errs.append(f'{name}: columns {df.columns.tolist()} != served {ref_cols}')
        cells.append(df.h3index)
        staged.append((p, f'data/{prefix}sat_dpto/{name}'))
    allc = pd.concat(cells)
    if allc.duplicated().any() or set(allc) != set(main.h3index):
        errs.append(f'departments do not partition the territory ({len(allc)} vs {len(main)})')
    return errs, staged, len(main), len(served)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--layers', required=True)
    ap.add_argument('--only', default=None)
    ap.add_argument('--backup-dir', default=r'C:\Users\ant\r2-py-cobertura-backup-2026-11')
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    ts = a.only.split(',') if a.only else T12
    all_staged, bad = [], {}
    for t in ts:
        for layer in a.layers.split(','):
            errs, staged, n, n_old = check(t, layer, a.backup_dir)
            print(f'{t:20s} {layer:24s} cells {n_old:>8,} -> {n:>8,}  files {len(staged):4d}  {"OK" if not errs else errs}')
            if errs:
                bad[(t, layer)] = errs
            all_staged += staged
    if bad:
        print(f'{len(bad)} (territory, layer) with problems: nothing uploaded')
        return 1
    if a.dry_run:
        print(f'dry run: {len(all_staged)} files ready')
        return 0
    from concurrent.futures import ThreadPoolExecutor
    from upload_to_r2 import upload_file
    with ThreadPoolExecutor(6) as ex:
        ok = list(ex.map(lambda pk: upload_file(pk[0], pk[1]), all_staged))
    failed = [k for (p, k), o in zip(all_staged, ok) if not o]
    print(f'uploaded {sum(ok)}/{len(all_staged)}; failed: {failed[:5]}')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
