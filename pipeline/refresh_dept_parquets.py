"""
Publish a recomputed layer parquet without re-splitting departments.

Takes the local `output/<t>/sat_<layer>.parquet` (recomputed), the dept
parquets the CDN serves, and replaces every column but h3index by h3index —
each department keeps exactly the hexagons it had, so no assignment drift from
re-running split_by_admin.py with other flags. Rewrites avg_score (mean score)
in the dept summary JSON, backs up what is being replaced, checks that rows and
schema match, and only then uploads (upload_to_r2 verifies each object's size).

Written for the 29-oct-2026 health_access fix (real Oxford healthcare times in
Corrientes, Chaco and Formosa — build_oxford_radios.py). Bump the layer's ?v and
DEPT_V in src/lib/config.ts after publishing: the objects are immutable-cached.

    python pipeline/refresh_dept_parquets.py --layer health_access --territory chaco --backup-dir C:/Users/ant/r2-health-backup-2026-10
"""

import argparse
import json
import os
import sys
import urllib.request

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import OUTPUT_DIR, get_territory

CDN = 'https://cdn.spatia.ar/data'
SRC_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data')


def fetch(url: str, path: str) -> None:
    from urllib.parse import quote
    url = quote(url, safe=':/?=&')   # parquetKeys like "1°_de_mayo"
    req = urllib.request.Request(url, headers={'User-Agent': 'spatia-pipeline/1.0'})
    with urllib.request.urlopen(req, timeout=120) as r, open(path, 'wb') as f:
        f.write(r.read())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--layer', required=True)
    ap.add_argument('--territory', required=True)
    ap.add_argument('--backup-dir', required=True)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    layer, t = args.layer, args.territory
    prefix = get_territory(t)['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, prefix.rstrip('/')) if prefix else OUTPUT_DIR
    summary_path = os.path.join(SRC_DATA_DIR, f'sat_{layer}_dept_summary.json' if t == 'misiones'
                                else f'{t}_sat_{layer}_summary.json')
    new = pd.read_parquet(os.path.join(t_dir, f'sat_{layer}.parquet')).drop_duplicates('h3index')
    with open(summary_path, encoding='utf-8') as f:
        summary = json.load(f)

    bdir = os.path.join(args.backup_dir, t)
    stage = os.path.join(t_dir, f'refresh_{layer}')
    os.makedirs(os.path.join(bdir, 'sat_dpto'), exist_ok=True)
    os.makedirs(os.path.join(stage, 'sat_dpto'), exist_ok=True)

    main_key = f'data/{prefix}sat_{layer}.parquet'
    fetch(f'{CDN}/{prefix}sat_{layer}.parquet', os.path.join(bdir, f'sat_{layer}.parquet'))
    served_main = pd.read_parquet(os.path.join(bdir, f'sat_{layer}.parquet'))
    if list(served_main.columns) != list(new.columns) or len(served_main) != len(new):
        sys.exit(f'{t}: main parquet schema/rows differ from served ({len(served_main)} vs {len(new)})')
    staged = [(os.path.join(t_dir, f'sat_{layer}.parquet'), main_key)]

    cols = [c for c in new.columns if c != 'h3index']
    for d in summary['departments']:
        key = d['parquetKey']
        name = f'sat_{layer}_{key}.parquet'
        bpath = os.path.join(bdir, 'sat_dpto', name)
        fetch(f'{CDN}/{prefix}sat_dpto/{name}', bpath)
        old = pd.read_parquet(bpath)
        upd = old[['h3index']].merge(new, on='h3index', how='left')
        if upd[cols[0]].isna().all() or len(upd) != len(old):
            sys.exit(f'{t}/{key}: recomputed parquet does not cover this department')
        upd = upd[[c for c in old.columns if c in upd.columns]]
        if list(upd.columns) != list(old.columns):
            sys.exit(f'{t}/{key}: columns differ from served')
        p = os.path.join(stage, 'sat_dpto', name)
        upd.to_parquet(p, index=False)
        staged.append((p, f'data/{prefix}sat_dpto/{name}'))
        if 'score' in upd.columns:
            m = upd['score'].mean()
            d['avg_score'] = None if m != m else round(float(m), 1)
    if 'score' in new.columns and isinstance(summary.get('province'), dict):
        summary['province']['avg_score'] = round(float(new['score'].mean()), 1)
    summary['departments'].sort(key=lambda d: d.get('avg_score') or 0, reverse=True)

    print(f'{t}: {len(staged)} files staged (1 territory + {len(staged) - 1} departments), backup in {bdir}')
    if args.dry_run:
        return
    from upload_to_r2 import upload_file
    failed = [k for p, k in staged if not upload_file(p, k)]
    if failed:
        sys.exit(f'{t}: {len(failed)} uploads failed: {failed[:5]}')
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f'{t}: published, summary updated')


if __name__ == '__main__':
    main()
