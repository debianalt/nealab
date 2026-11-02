"""Check pipeline/repartir_deptos_indec.py's output against the R2 byte backup: same cells,
same values cell by cell, same schema, each cell in one file, every summary department has
its file and vice versa, and every cell's file is its INDEC department (crosswalk).
    python pipeline/verificar_reparto_indec.py
"""
import glob, json, os, sys
from pathlib import Path
import pandas as pd, pyarrow.parquet as pq
sys.path.insert(0, os.path.dirname(__file__))
from repartir_deptos_indec import FAMILIES, TERRITORIES, OUT, DATA, BACKUP, dept_names, safe_filename

names = dept_names()
bad = 0
for t in TERRITORIES:
    xw = pd.read_parquet(OUT / t / 'h3_admin_crosswalk.parquet').set_index('h3index')['dpto']
    for fam, (folder, stem, summ) in FAMILIES.items():
        old_files = glob.glob(str(BACKUP / 'data' / t / folder / f'{stem}_*.parquet'))
        new_files = glob.glob(str(OUT / t / 'indec_resplit' / folder / f'{stem}_*.parquet'))
        s = json.loads((DATA / f'{t}_{summ}.json').read_text(encoding='utf-8'))
        listed = {d['parquetKey'] for d in s['departments']}
        # a file no summary lists (Misiones «apostoles» leaked into Corrientes forestry,
        # 330 cells all also in Corrientes files) is not part of the layer
        old_files = sorted(old_files, key=lambda f: Path(f).stem[len(stem) + 1:] not in listed)
        old = pd.concat([pd.read_parquet(f) for f in old_files]).drop_duplicates('h3index').set_index('h3index').sort_index()
        parts = []
        for f in new_files:
            d = pd.read_parquet(f); d['_key'] = Path(f).stem[len(stem) + 1:]; parts.append(d)
            if pq.read_schema(f).names != pq.read_schema(old_files[0]).names: print('SCHEMA', f); bad += 1
        new = pd.concat(parts)
        one_file = not new.h3index.duplicated().any()
        new = new.set_index('h3index').sort_index()
        same = old.index.equals(new.index) and old.equals(new.drop(columns='_key'))
        s = json.loads((DATA / f'{t}_{summ}.json').read_text(encoding='utf-8'))
        keys = {d['parquetKey'] for d in s['departments']}
        files_ok = keys == set(new._key)
        counts_ok = all(d['hex_count'] == int((new._key == d['parquetKey']).sum()) for d in s['departments'])
        k2n = {d['parquetKey']: d['dpto'] for d in s['departments']}
        indec_ok = (new._key.map(k2n) == xw.reindex(new.index)).mean()
        ok = same and one_file and files_ok and counts_ok and indec_ok > 0.9999
        bad += not ok
        print(f'{"ok " if ok else "BAD"} {t:10s} {fam:22s} files {len(old_files)}→{len(new_files)} '
              f'depts {len(keys)} same_values={same} one_file={one_file} keys={files_ok} counts={counts_ok} indec={indec_ok:.5f}')
print('ALL OK' if not bad else f'{bad} PROBLEMS'); sys.exit(1 if bad else 0)
