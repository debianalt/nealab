"""Index for the fact-sheet link (?h3=<cell>): the department file each layer
really keeps the cell in, for the cells where the link's own rule gets it wrong.

The link (openFichaFromLink in src/routes/+page.svelte) picks the department whose
polygon in ar_dept_boundaries.json contains the cell centre (exterior rings, first
feature in file order) and derives each layer's parquetKey from that name. The
per-department parquets, however, assign cells by areal majority over census radios
(split_satellite_by_dpto.py / split_by_admin.py), which is what the panel, the
rankings and the summaries use. Measured 31-oct-2026: ~3.5 % of Misiones cells (and
thousands of census cells in Chaco/Corrientes) are not in the file the link opened,
so the sheet showed "sin dato" although the value exists; cells whose centre falls
outside every polygon did not open at all.

Output src/lib/data/ficha_link_index.json, lazy-loaded only by the ?h3= link:
    {"v": 1, "generated": "...", "cells": {"<h3>": {"t": territory (only when it
     differs from the polygon's), "n": department name, "k": parquetKey for every
     per-department layer, "o": {layer: parquetKey} where a layer differs}}}

Re-run after any change to a per-department split (new crosswalk, new territory):
    python pipeline/build_ficha_link_index.py            (from neahub/)
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

import duckdb
import h3
import numpy as np
import shapely
from shapely.geometry import Polygon

ROOT = Path(__file__).resolve().parent.parent
REGISTRY = ROOT / 'pipeline' / 'output' / 'hexoma' / 'registry.json'
OUT = ROOT / 'src' / 'lib' / 'data' / 'ficha_link_index.json'
AR = ['misiones', 'corrientes', 'chaco', 'formosa']          # order = tie-break for cells in two territories
AR_CODPROV = {'54': 'misiones', '18': 'corrientes', '22': 'chaco', '34': 'formosa'}  # as +page.svelte


def log(*a):
    print(*a, flush=True)


def membership(con, reg: dict) -> tuple[dict, list[str]]:
    """DuckDB table mem(t, layer, h3, k): which dept file holds each cell, from the published files."""
    fresh = int(time.time())
    layers = [l['id'] for l in reg['layers'] if l['perDepartment']]
    names = {}
    con.execute('CREATE OR REPLACE TABLE mem (t VARCHAR, layer VARCHAR, h3 VARCHAR, k VARCHAR)')
    con.execute('CREATE OR REPLACE TEMP TABLE urlmap (url VARCHAR, k VARCHAR)')
    for tid in AR:
        for lid in layers:
            spec = reg['loads'][tid].get(lid)
            if not spec or spec['scope'] != 'dept':
                continue
            urls = {f"{f['url']}{'&' if '?' in f['url'] else '?'}fresh={fresh}": f['parquetKey'] for f in spec['files']}
            for f in spec['files']:
                names[(tid, f['parquetKey'])] = names.get((tid, f['parquetKey'])) or f['dept']
            con.execute('DELETE FROM urlmap')
            con.executemany('INSERT INTO urlmap VALUES (?, ?)', list(urls.items()))
            url_list = '[' + ', '.join(f"'{u}'" for u in urls) + ']'
            con.execute(f"""INSERT INTO mem SELECT '{tid}', '{lid}', p.h3index, u.k
                FROM read_parquet({url_list}, filename = true) p JOIN urlmap u ON u.url = p.filename""")
        log(f'  {tid}: listo')
    return names, layers


def centre_departments(cells: list[str], gj: dict) -> list[str | None]:
    """redcode of the first feature (file order) whose exterior ring contains the cell centre."""
    ll = np.array([h3.cell_to_latlng(c) for c in cells])
    lat, lng = ll[:, 0], ll[:, 1]
    out = np.full(len(cells), None, dtype=object)
    todo = np.ones(len(cells), dtype=bool)
    for f in gj['features']:
        g = f['geometry']
        parts = [g['coordinates']] if g['type'] == 'Polygon' else g['coordinates'] if g['type'] == 'MultiPolygon' else []
        for p in parts:
            poly = Polygon(p[0])
            x0, y0, x1, y1 = poly.bounds
            idx = np.nonzero(todo & (lng >= x0) & (lng <= x1) & (lat >= y0) & (lat <= y1))[0]
            if not len(idx):
                continue
            hit = idx[shapely.contains_xy(poly, lng[idx], lat[idx])]
            out[hit] = str(f['properties']['redcode'])
            todo[hit] = False
    return list(out)


def main() -> None:
    t0 = time.time()
    subprocess.run(['node', 'pipeline/hexoma_registry.mjs', str(REGISTRY)], cwd=ROOT, check=True, shell=(sys.platform == 'win32'))
    reg = json.loads(REGISTRY.read_text(encoding='utf-8'))
    gj = json.loads((ROOT / reg['boundaries']['ar']).read_text(encoding='utf-8'))
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs; SET threads = 8')

    log('membership of the published dept files:')
    names, layers = membership(con, reg)
    cells = [r[0] for r in con.execute('SELECT DISTINCT h3 FROM mem ORDER BY 1').fetchall()]
    log(f'{len(cells):,} celdas; centro → departamento… ({time.time() - t0:.0f} s)')
    centre = centre_departments(cells, gj)
    import pandas as pd
    cdf = pd.DataFrame({'h3': cells, 'rc': centre})
    cdf['tp'] = cdf['rc'].map(lambda r: AR_CODPROV.get(r[:2]) if r else None)
    con.register('cdf', cdf)
    fk_rows = [(rc, l, k) for rc, v in reg['fichaKeys'].items() for l, k in v['keys'].items()]
    fkdf = pd.DataFrame(fk_rows, columns=['rc', 'layer', 'k'])
    con.register('fkdf', fkdf)
    tie = ' '.join(f"WHEN '{t}' THEN {i}" for i, t in enumerate(AR))
    # link territory T: the polygon's if the cell is in its files, else first territory holding it
    # (land_use first). One row per (cell, layer) in T; stale duplicates resolved toward the
    # deforestation split's key, then the key the link expects, then alphabetically.
    rows = con.execute(f"""
        WITH terrs AS (SELECT h3, t, bool_or(layer = 'land_use') lu FROM mem GROUP BY 1, 2),
        pick AS (
            SELECT c.h3, c.rc, c.tp,
                   coalesce(max(CASE WHEN tr.t = c.tp THEN tr.t END),
                            arg_min(tr.t, (NOT tr.lu)::INT * 10 + CASE tr.t {tie} END)) AS T
            FROM cdf c JOIN terrs tr USING (h3) GROUP BY 1, 2, 3),
        defo AS (SELECT t, h3, min(k) dk FROM mem WHERE layer = 'deforestation_dynamics' GROUP BY 1, 2),
        lk AS (
            SELECT p.h3, p.T, p.tp, m.layer,
                   arg_min(m.k, CASE WHEN m.k = d.dk THEN 0 WHEN m.k = f.k THEN 1 ELSE 2 END) AS k,
                   any_value(CASE WHEN p.tp = p.T THEN f.k END) AS expected,
                   count(*) AS n
            FROM pick p JOIN mem m ON m.h3 = p.h3 AND m.t = p.T
            LEFT JOIN defo d ON d.h3 = p.h3 AND d.t = p.T
            LEFT JOIN fkdf f ON f.rc = p.rc AND f.layer = m.layer
            GROUP BY 1, 2, 3, 4)
        SELECT h3, T, tp, list(struct_pack(layer := layer, k := k)) AS keys, sum((n > 1)::INT) AS dups
        FROM lk GROUP BY 1, 2, 3
        HAVING T IS DISTINCT FROM tp OR bool_or(k IS DISTINCT FROM expected)""").fetchall()

    index, stats, dup_resolved = {}, Counter(), 0
    for cell, T, tp, keys, dups in rows:
        file_keys = {x['layer']: x['k'] for x in keys}
        dup_resolved += dups
        k = Counter(file_keys.values()).most_common(1)[0][0]
        e = {'n': names.get((T, k)) or k, 'k': k}
        if T != tp:
            e = {'t': T, **e}
        o = {l: v for l, v in sorted(file_keys.items()) if v != k}
        if o:
            e['o'] = o
        index[cell] = e
        stats[(T, 'fuera' if not tp else 'otra prov' if T != tp else 'depto')] += 1

    OUT.write_text(json.dumps({'v': 1, 'generated': time.strftime('%Y-%m-%d'), 'cells': dict(sorted(index.items()))},
                              ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
    log('excepciones por territorio y motivo:')
    for (t, why), n in sorted(stats.items()):
        log(f'  {t:<11} {why:<10} {n:>7,}')
    log(f'{len(index):,} celdas en el índice · {OUT.stat().st_size / 1e6:.2f} MB · duplicados resueltos {dup_resolved} · {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
