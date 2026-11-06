"""¿Está al día el Hexoma? Compara la foto de las capas web (`capa_*`, `cargas`) con lo que spatia.ar sirve hoy.

Sólo lectura. Por (capa, territorio):
- archivos: los que el registro de hoy manda leer (`hexoma_registry.mjs`, mismos `?v` y parquetKey que la web) contra
  los de la última carga (`cargas.archivos`), por ruta;
- ETag: HEAD de cada archivo en cdn.spatia.ar contra el ETag guardado al cargar;
- columnas: las que el registro declara (col y rawCol) contra las de `capa_<id>`.
Además:
- crosswalks 2022 (`pipeline/output/**/h3_radio_crosswalk*.parquet`) más nuevos que `cruce.radio_h3` → hay que
  correr build_hexoma_censo.py --anio 2022, build_hexoma_cruce.py y build_hexoma_panel.py;
- `metodologia.py` más nuevo que la última carga de `meta` → build_hexoma_meta.py.
`--muestra N`: N celdas al azar por (capa, territorio) al día; lee su fila del parquet servido y la compara con
`capa_<id>` columna por columna (el valor de v_hexoma es esa columna × scale, por construcción).

Reemplaza la comparación con la ficha publicada (scripts/verify_ficha.py), que se fue con la ficha el 4-nov-2026.

    python pipeline/hexoma/vigencia.py               # sale con 1 si algo está viejo
    python pipeline/hexoma/vigencia.py --muestra 20  # + comparación de valores
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
from build_hexoma import DB, connect  # noqa: E402

NEA = ['misiones', 'corrientes', 'chaco', 'formosa']
CROSSWALKS = [ROOT / 'pipeline/output/h3_radio_crosswalk.parquet'] + [
    ROOT / f'pipeline/output/{t}/h3_radio_crosswalk_{t}.parquet' for t in NEA[1:]]
UA = {'User-Agent': 'Mozilla/5.0 (hexoma vigencia)'}


def registro(out: Path) -> dict:
    subprocess.run(['node', 'pipeline/hexoma_registry.mjs', str(out)], cwd=ROOT, check=True, capture_output=True)
    return json.loads(out.read_text(encoding='utf-8'))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--muestra', type=int, default=0)
    ap.add_argument('--registro', help='usar este registry.json en lugar de exportarlo')
    args = ap.parse_args()
    tmp = ROOT / 'pipeline/output/hexoma/registry_vigencia.json'
    reg = json.loads(Path(args.registro).read_text(encoding='utf-8')) if args.registro else registro(tmp)
    conn = connect(DB)
    cur = conn.cursor()
    cur.execute("""SELECT DISTINCT ON (capa, territorio) capa, territorio, archivos, cargado_en
                   FROM cargas ORDER BY capa, territorio, cargado_en DESC""")
    viejo = {(c, t): ({f['url'].split('?')[0]: f for f in a}, when) for c, t, a, when in cur.fetchall()}
    jobs = [(lid, t, f['url']) for t in NEA for lid, ld in reg['loads'][t].items() for f in ld['files']]
    s = requests.Session()

    def head(u):
        try:
            r = s.head(u, timeout=60, headers=UA)
            return r.status_code, r.headers.get('etag')
        except requests.RequestException as e:
            return str(e)[:40], None
    with cf.ThreadPoolExecutor(24) as ex:
        res = list(ex.map(lambda j: head(j[2]), jobs))
    est = defaultdict(lambda: {'n': 0, 'nuevo': 0, 'etag': 0, 'http': 0})
    for (lid, t, u), (st, et) in zip(jobs, res):
        e = est[(lid, t)]
        e['n'] += 1
        o = viejo.get((lid, t), ({}, None))[0]
        p = u.split('?')[0]
        if st != 200:
            e['http'] += 1
        elif p not in o:
            e['nuevo'] += 1
        elif o[p]['etag'] != et:
            e['etag'] += 1
    malos = 0
    al_dia = []
    print(f'{"":5} {"capa":24} {"territorio":11} {"archivos":>8} {"cargados":>8} {"nuevos":>6} {"etag":>5} {"http":>5}  cargado')
    for k in sorted(set(est) | {k for k in viejo if k[1] in NEA}):
        e = est.get(k, {'n': 0, 'nuevo': 0, 'etag': 0, 'http': 0})
        o, when = viejo.get(k, ({}, None))
        ok = e['nuevo'] == e['etag'] == e['http'] == 0 and len(o) == e['n']
        malos += not ok
        if ok:
            al_dia.append(k)
        print(f'{"OK" if ok else "VIEJA":5} {k[0]:24} {k[1]:11} {e["n"]:8} {len(o):8} {e["nuevo"]:6} {e["etag"]:5} {e["http"]:5}  '
              + (f'{when:%Y-%m-%d %H:%M}' if when else 'nunca'))
    for l in reg['layers']:
        want = {v['col'] for v in l['variables']} | {v['rawCol'] for v in l['variables'] if v['rawCol']}
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s",
                    (f'capa_{l["id"]}',))
        falta = sorted(want - {r[0] for r in cur.fetchall()})
        if falta:
            malos += 1
            print(f'COLUMNAS capa_{l["id"]}: faltan {falta}')
    # crosswalks y meta
    cur.execute("SELECT to_regclass('cruce.radio_h3') IS NOT NULL")
    if cur.fetchone()[0]:
        cur.execute("""SELECT max(greatest(last_analyze, last_autoanalyze, last_vacuum, last_autovacuum))
                       FROM pg_stat_user_tables WHERE schemaname IN ('cruce', 'censo_2022', 'panel')""")
        cuando = cur.fetchone()[0]
        for p in CROSSWALKS:
            if p.exists() and cuando and dt.datetime.fromtimestamp(p.stat().st_mtime, cuando.tzinfo) > cuando:
                malos += 1
                print(f'CRUCE viejo: {p.relative_to(ROOT)} es posterior a la última carga de cruce/censo_2022/panel ({cuando:%Y-%m-%d %H:%M})')
    cur.execute("""SELECT max(greatest(last_analyze, last_autoanalyze, last_vacuum, last_autovacuum))
                   FROM pg_stat_user_tables WHERE schemaname = 'meta'""")
    cm = cur.fetchone()[0]
    met = Path(__file__).with_name('metodologia.py')
    if cm and dt.datetime.fromtimestamp(met.stat().st_mtime, cm.tzinfo) > cm:
        malos += 1
        print(f'META vieja: metodologia.py es posterior a la última carga de meta ({cm:%Y-%m-%d %H:%M})')
    if args.muestra:
        malos += muestra(cur, reg, al_dia, args.muestra)
    print('al día' if not malos else f'{malos} problemas')
    return 1 if malos else 0


def muestra(cur, reg, al_dia, n) -> int:
    import duckdb
    import pandas as pd
    duck = duckdb.connect()
    duck.execute('INSTALL httpfs; LOAD httpfs')
    malos = 0
    for lid, t in al_dia:
        cur.execute(f"""SELECT c.h3::text, d.parquet_key FROM capa_{lid} c JOIN territorios x USING (territorio_id)
                        LEFT JOIN departamentos d USING (depto_id) WHERE x.territorio = %s AND c.principal
                        ORDER BY random() LIMIT %s""", (t, n))
        filas = cur.fetchall()
        if not filas:
            continue
        files = {f['parquetKey']: f['url'] for f in reg['loads'][t][lid]['files']}
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s "
                    "AND column_name NOT IN ('h3', 'territorio_id', 'depto_id', 'principal')", (f'capa_{lid}',))
        cols = [r[0] for r in cur.fetchall()]
        dif = 0
        for h3, key in filas:
            url = files.get(key) or next(iter(files.values()))
            pq = duck.execute(f"SELECT * FROM '{url}' WHERE h3index = ?", [h3]).df()
            cur.execute(f'SELECT {", ".join(chr(34) + c + chr(34) for c in cols)} FROM capa_{lid} c JOIN territorios x '
                        f'USING (territorio_id) WHERE c.h3 = %s::h3index AND x.territorio = %s AND c.principal', (h3, t))
            pg = cur.fetchone()
            if pq.empty or pg is None:
                dif += 1
                continue
            for c, v in zip(cols, pg):
                w = pq.iloc[0].get(c)
                w_na = w is None or bool(pd.isna(w))
                if v is None and w_na:
                    continue
                if isinstance(v, (int, float)) and not w_na and abs(float(v) - float(w)) <= 1e-4 * max(1, abs(float(w))):
                    continue
                if str(v) == str(w):
                    continue
                dif += 1
                print(f'  DIF {lid} {t} {h3} {c}: hexoma {v} · web {w}')
        malos += dif > 0
        print(f'  muestra {lid:24} {t:11} {len(filas)} celdas, {dif} diferencias')
    return malos


if __name__ == '__main__':
    sys.exit(main())
