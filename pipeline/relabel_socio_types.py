"""
Rename the sociodemographic types the CDN serves, in place (3-nov-2026).

The type of every hexagon stays; only its label changes, from the column-name label
of aggregate_radio_to_h3.auto_label ("densidad_hab_km2 bajo, nbi alto") to the name
socio_types.name_socio_type gives the cluster's profile. Per province: download the
territory parquet and every department parquet the CDN serves (backup, never
overwritten), map type_label, check that rows, columns and every other value are
unchanged and that no label is left unmapped, upload with upload_to_r2.upload_file.
No re-split; summaries and rankings untouched (they do not use the type).
Bump ?v of sat_sociodemographic and DEPT_V in src/lib/config.ts afterwards.

    python pipeline/relabel_socio_types.py --backup-dir C:/Users/ant/r2-socio-types-backup-2026-11 [--dry-run]
"""
import argparse
import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import OUTPUT_DIR, get_territory
from refresh_dept_parquets import CDN, SRC_DATA_DIR, backup
from socio_types import NO_CENSUS, name_socio_type

TERRITORIES = ['misiones', 'corrientes', 'chaco', 'formosa']
LAYER = 'sociodemographic'


def label_map(main: pd.DataFrame) -> dict:
    has = main.dropna(subset=['densidad_hab_km2', 'pct_nbi', 'pct_propietario'])
    prov_nbi, prov_own = has['pct_nbi'].mean(), has['pct_propietario'].mean()
    out = {}
    for lab, g in has.groupby('type_label'):
        if lab == NO_CENSUS:
            continue
        out[lab] = name_socio_type(g['densidad_hab_km2'].mean(), g['pct_nbi'].mean(),
                                   g['pct_propietario'].mean(), prov_nbi, prov_own)
        print(f'    {lab!r:45} → {out[lab]}  (n={len(g):,}, dens {g.densidad_hab_km2.mean():.0f}, '
              f'NBI {g.pct_nbi.mean():.1f}/{prov_nbi:.1f}, prop {g.pct_propietario.mean():.0f}/{prov_own:.0f})')
    if len(set(out.values())) != len(out):
        sys.exit(f'two types would get the same name: {out}')
    return out


def relabel(df: pd.DataFrame, m: dict, where: str) -> pd.DataFrame:
    labs = set(df['type_label'].dropna()) - {NO_CENSUS}
    missing = labs - set(m)
    if missing:
        sys.exit(f'{where}: labels without a name {missing}')
    new = df.copy()
    new['type_label'] = df['type_label'].map(lambda x: m.get(x, x))
    other = [c for c in df.columns if c != 'type_label']
    assert new[other].equals(df[other]) and list(new.columns) == list(df.columns) and len(new) == len(df)
    return new


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--backup-dir', required=True)
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    staged = []
    for t in TERRITORIES:
        prefix = get_territory(t)['output_prefix']
        print(f'{t}:')
        bdir = os.path.join(a.backup_dir, t)
        os.makedirs(os.path.join(bdir, 'sat_dpto'), exist_ok=True)
        stage = os.path.join(OUTPUT_DIR, prefix.rstrip('/'), 'relabel_socio') if prefix else os.path.join(OUTPUT_DIR, 'relabel_socio')
        os.makedirs(os.path.join(stage, 'sat_dpto'), exist_ok=True)

        main_name = f'sat_{LAYER}.parquet'
        backup(f'{CDN}/{prefix}{main_name}', os.path.join(bdir, main_name))
        main = pd.read_parquet(os.path.join(bdir, main_name))
        m = label_map(main)
        p = os.path.join(stage, main_name)
        relabel(main, m, f'{t}/territory').to_parquet(p, index=False)
        staged.append((p, f'data/{prefix}{main_name}'))

        summary_path = os.path.join(SRC_DATA_DIR, f'sat_{LAYER}_dept_summary.json' if t == 'misiones'
                                    else f'{t}_sat_{LAYER}_summary.json')
        for d in json.load(open(summary_path, encoding='utf-8'))['departments']:
            name = f"sat_{LAYER}_{d['parquetKey']}.parquet"
            bp = os.path.join(bdir, 'sat_dpto', name)
            backup(f'{CDN}/{prefix}sat_dpto/{name}', bp)
            p = os.path.join(stage, 'sat_dpto', name)
            relabel(pd.read_parquet(bp), m, f'{t}/{d["parquetKey"]}').to_parquet(p, index=False)
            staged.append((p, f'data/{prefix}sat_dpto/{name}'))
    print(f'{len(staged)} files staged')
    if a.dry_run:
        return 0
    from concurrent.futures import ThreadPoolExecutor
    from upload_to_r2 import upload_file
    with ThreadPoolExecutor(6) as ex:
        ok = list(ex.map(lambda pk: upload_file(pk[0], pk[1]), staged))
    failed = [k for (_, k), o in zip(staged, ok) if not o]
    print(f'uploaded {sum(ok)}/{len(staged)}; failed: {failed[:5]}')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
