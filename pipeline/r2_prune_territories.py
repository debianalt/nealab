"""
Back up and delete the analysis-layer objects of territories that no longer
serve analysis layers (3-nov-2026 scope rule: analysis only in NEA AR + Itapúa +
Alto Paraná; the other 15 Paraguayan departments and the 3 Brazilian states keep
buildings, census and boundaries, which live elsewhere: data/tiles/,
data/setores_stats_*, data/br_districts/).

Everything under data/<territory>/ is analysis output (sat_*.parquet, sat_dpto/,
flood_dpto/, hex_flood_risk.parquet). Each object is downloaded to
<backup-dir>/<key> and its size checked against R2; only if every object of every
territory is backed up does it delete, in batches of 1000.

    python pipeline/r2_prune_territories.py --backup-dir C:/Users/ant/r2-capas-py-br-backup-2026-11 [--dry-run]
"""
import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(__file__))
from repartir_deptos_indec import s3_client

BUCKET = 'neahub'
PRUNED = ['concepcion_py', 'san_pedro_py', 'cordillera_py', 'guaira_py', 'caaguazu_py', 'caazapa_py',
          'misiones_py', 'paraguari_py', 'central_py', 'neembucu_py', 'amambay_py', 'canindeyu_py',
          'presidente_hayes_py', 'boqueron_py', 'alto_paraguay_py',
          'parana_br', 'santa_catarina_br', 'rio_grande_sul_br']


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--backup-dir', required=True)
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    s3 = s3_client()
    objs = []
    for t in PRUNED:
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=f'data/{t}/'):
            objs += [(o['Key'], o['Size']) for o in page.get('Contents', [])]
    print(f'{len(objs)} objects, {sum(s for _, s in objs) / 1024**3:.2f} GB under data/<{len(PRUNED)} territories>/')

    def backup(ks):
        key, size = ks
        p = os.path.join(a.backup_dir, *key.split('/'))
        if os.path.exists(p) and os.path.getsize(p) == size:
            return key, True
        os.makedirs(os.path.dirname(p), exist_ok=True)
        try:
            s3_client().download_file(BUCKET, key, p + '.part')
            os.replace(p + '.part', p)
        except Exception as e:
            print(f'  ERR {key}: {e}')
            return key, False
        return key, os.path.getsize(p) == size

    with ThreadPoolExecutor(16) as ex:
        res = list(ex.map(backup, objs))
    bad = [k for k, ok in res if not ok]
    print(f'backup: {len(res) - len(bad)}/{len(res)} ok in {a.backup_dir}')
    if bad:
        print(f'{len(bad)} not backed up (first: {bad[:3]}): nothing deleted')
        return 1
    if a.dry_run:
        print('dry run: nothing deleted')
        return 0
    keys = [k for k, _ in objs]
    deleted, errors = 0, []
    for i in range(0, len(keys), 1000):
        r = s3.delete_objects(Bucket=BUCKET, Delete={'Objects': [{'Key': k} for k in keys[i:i + 1000]], 'Quiet': True})
        errors += r.get('Errors', [])
        deleted += len(keys[i:i + 1000]) - len(r.get('Errors', []))
    print(f'deleted {deleted}/{len(keys)}; errors: {errors[:3]}')
    left = sum(len(p.get('Contents', [])) for t in PRUNED
               for p in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=f'data/{t}/'))
    print(f'left under the pruned prefixes: {left}')
    return 1 if errors or left else 0


if __name__ == '__main__':
    sys.exit(main())
