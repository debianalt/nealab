"""
Extend economic_activity of Corrientes, Chaco and Formosa to the full satellite grid.

Until 2-nov-2026 these three provinces carried only the hexagons with census
population (~4–7 % of the grid), so night-time radiance — the layer's one
non-census variable — stopped where the census did, while Misiones (filled by
fill_h3_coverage.py) measured it over the whole province. This adds the missing
cells with the census columns empty (no census there, not zero), then
compute_viirs_buildings_h3.py samples VIIRS and building density on every cell.

Department membership is not re-split: each department keeps the cells its served
file has, and only the NEW cells are assigned, with the territory's INDEC crosswalk
(h3_admin_crosswalk.parquet since 2-nov-2026). parquetKeys and names stay.

    python pipeline/extend_econ_full_grid.py --territory corrientes --backup-dir C:/Users/ant/r2-econ-backup-2026-11 extend
    python pipeline/compute_viirs_buildings_h3.py --territory corrientes --year 2025
    python pipeline/extend_econ_full_grid.py --territory corrientes --backup-dir C:/Users/ant/r2-econ-backup-2026-11 publish [--dry-run]

Then build_layer_ranks.py --layer economic_activity, bump ?v / DEPT_V, purge the CDN.
"""
import argparse
import json
import os
import sys
import unicodedata

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import OUTPUT_DIR, get_territory
from refresh_dept_parquets import CDN, SRC_DATA_DIR, fetch

LAYER = 'economic_activity'


def norm(s: str) -> str:
    s = s.replace('º', '°')
    s = unicodedata.normalize('NFD', s)
    return ''.join(c for c in s if not unicodedata.combining(c)).lower().strip()


def paths(t: str, backup_dir: str):
    prefix = get_territory(t)['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, prefix.rstrip('/'))
    return (prefix, t_dir, os.path.join(t_dir, f'sat_{LAYER}.parquet'),
            os.path.join(SRC_DATA_DIR, f'{t}_sat_{LAYER}_summary.json'), os.path.join(backup_dir, t))


def extend(t: str, backup_dir: str) -> None:
    prefix, t_dir, pq, _, bdir = paths(t, backup_dir)
    os.makedirs(bdir, exist_ok=True)
    served = os.path.join(bdir, f'sat_{LAYER}.parquet')
    fetch(f'{CDN}/{prefix}sat_{LAYER}.parquet', served)        # the served file is the base
    old = pd.read_parquet(served)
    cw = pd.read_parquet(os.path.join(t_dir, 'h3_admin_crosswalk.parquet'), columns=['h3index'])
    add = cw.loc[~cw.h3index.isin(old.h3index), ['h3index']].copy()
    for c in old.columns:
        if c != 'h3index':
            add[c] = np.nan                                     # no census here: empty, not 0
    full = pd.concat([old, add.astype({c: old[c].dtype for c in old.columns if old[c].dtype == object})],
                     ignore_index=True)
    full.to_parquet(pq, index=False)
    print(f'{t}: {len(old):,} served + {len(add):,} new = {len(full):,} cells -> {pq}')


def publish(t: str, backup_dir: str, dry: bool) -> None:
    prefix, t_dir, pq, summary_path, bdir = paths(t, backup_dir)
    full = pd.read_parquet(pq).drop_duplicates('h3index')
    served_main = pd.read_parquet(os.path.join(bdir, f'sat_{LAYER}.parquet'))
    if list(full.columns) != list(served_main.columns):
        sys.exit(f'{t}: columns differ from served: {list(full.columns)} vs {list(served_main.columns)}')
    cw = pd.read_parquet(os.path.join(t_dir, 'h3_admin_crosswalk.parquet'))
    with open(summary_path, encoding='utf-8') as f:
        summary = json.load(f)
    key_by_name = {norm(d['dpto']): d['parquetKey'] for d in summary['departments']}
    cw['key'] = cw['dpto'].map(lambda n: key_by_name.get(norm(n)))
    if cw['key'].isna().any():
        sys.exit(f'{t}: crosswalk departments without a summary entry: {sorted(cw.loc[cw.key.isna(), "dpto"].unique())}')

    stage = os.path.join(t_dir, f'refresh_{LAYER}', 'sat_dpto')
    os.makedirs(stage, exist_ok=True)
    os.makedirs(os.path.join(bdir, 'sat_dpto'), exist_ok=True)
    staged = [(pq, f'data/{prefix}sat_{LAYER}.parquet')]
    member: dict[str, set] = {}
    for d in summary['departments']:
        name = f"sat_{LAYER}_{d['parquetKey']}.parquet"
        bpath = os.path.join(bdir, 'sat_dpto', name)
        fetch(f"{CDN}/{prefix}sat_dpto/{name}", bpath)
        member[d['parquetKey']] = set(pd.read_parquet(bpath, columns=['h3index']).h3index)
    assigned = set().union(*member.values())
    new = cw[~cw.h3index.isin(assigned)]
    total_new = 0
    for d in summary['departments']:
        k = d['parquetKey']
        cells = member[k] | set(new.loc[new.key == k, 'h3index'])
        total_new += len(cells) - len(member[k])
        out = full[full.h3index.isin(cells)]
        if len(out) != len(cells):
            sys.exit(f'{t}/{k}: {len(cells) - len(out)} cells missing from the territory parquet')
        p = os.path.join(stage, f"sat_{LAYER}_{k}.parquet")
        out.to_parquet(p, index=False)
        staged.append((p, f"data/{prefix}sat_dpto/sat_{LAYER}_{k}.parquet"))
        d['hex_count'] = len(cells)
    print(f'{t}: {len(staged)} files staged, {total_new:,} new cells assigned to departments')
    if dry:
        return
    from upload_to_r2 import upload_file
    failed = [k for p, k in staged if not upload_file(p, k)]
    if failed:
        sys.exit(f'{t}: {len(failed)} uploads failed: {failed[:5]}')
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f'{t}: published, summary hex_count updated')


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--territory', required=True, choices=['corrientes', 'chaco', 'formosa'])
    ap.add_argument('--backup-dir', required=True)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('step', choices=['extend', 'publish'])
    a = ap.parse_args()
    if a.step == 'extend':
        extend(a.territory, a.backup_dir)
    else:
        publish(a.territory, a.backup_dir, a.dry_run)
