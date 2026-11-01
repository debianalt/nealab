"""Hexoma — extracto congelado de una consulta, con su manifiesto de procedencia.

El Hexoma se regenera desde las fuentes y desde la web viva: un número de un artículo tiene que
poder rastrearse a la consulta, a la fecha y a la versión del código y de los datos que lo
produjeron. Este script corre una consulta sobre la base `hexoma` y deja, en la carpeta destino:

    <nombre>.parquet         el resultado
    <nombre>.manifest.json   consulta, fecha, commit de neahub (y si había cambios sin commitear),
                             objetos de la base que usa con su COMMENT, métodos de cruce por año
                             y su error de validación, fuentes de los externos, columnas con su
                             COMMENT cuando lo hay, filas y sha256 del parquet
    <nombre>.sql             la consulta, tal cual

Es el núcleo de las salidas del Hexoma. Una capa de Spatia o una exportación para terceros son
este mismo extracto más un paso de formato.

Uso (desde neahub/):
    python pipeline/hexoma/extracto.py --nombre nbi_misiones --sql consulta.sql --destino ../articles/x/data
    python pipeline/hexoma/extracto.py --nombre prueba --consulta "SELECT ..." --destino pipeline/output/hexoma/extractos
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_hexoma import DB, connect, log  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
ESQUEMAS = r'(censo_\d{4}|cruce|panel|externos|public)'


def git(*args: str) -> str:
    return subprocess.run(['git', *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--nombre', required=True)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--sql', type=Path, help='archivo con la consulta')
    g.add_argument('--consulta', help='la consulta en línea')
    ap.add_argument('--destino', required=True, type=Path)
    args = ap.parse_args()
    sql = args.sql.read_text(encoding='utf-8') if args.sql else args.consulta
    args.destino.mkdir(parents=True, exist_ok=True)
    t0 = time.time()

    conn = connect(DB)
    df = pd.read_sql(sql, conn)
    # h3index y otros tipos raros a texto
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype('string')
    pq = args.destino / f'{args.nombre}.parquet'
    df.to_parquet(pq, index=False)
    sha = hashlib.sha256(pq.read_bytes()).hexdigest()

    objetos = sorted(set(re.findall(rf'\b{ESQUEMAS}\.([a-z_0-9]+)\b', sql, flags=re.I)))
    meta_obj, cols_coment = [], {}
    with conn.cursor() as cur:
        for sch, obj in objetos:
            cur.execute("""SELECT c.relkind, obj_description(c.oid), obj_description(n.oid)
                           FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                           WHERE n.nspname = %s AND c.relname = %s""", (sch, obj))
            r = cur.fetchone()
            if not r:
                continue
            meta_obj.append({'objeto': f'{sch}.{obj}', 'tipo': r[0], 'comentario': r[1], 'esquema': r[2]})
            cur.execute("""SELECT a.attname, col_description(a.attrelid, a.attnum) FROM pg_attribute a
                           WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped""", (f'{sch}.{obj}',))
            for name, com in cur.fetchall():
                if com and name in df.columns and name not in cols_coment:
                    cols_coment[name] = f'{sch}.{obj}: {com}'
        cruce = None
        if any(s in ('cruce', 'panel') for s, _ in objetos):
            cur.execute("""SELECT anio, metodo, count(DISTINCT redcode) FROM cruce.radio_h3 GROUP BY 1, 2 ORDER BY 1, 2""")
            usado = {}
            for a, m, n in cur.fetchall():
                usado.setdefault(str(a), {})[m] = n
            cur.execute("""SELECT anio, metodo, round((wape * 100)::numeric, 2) FROM cruce.validacion WHERE clase = '0 total'""")
            val = {}
            for a, m, w in cur.fetchall():
                val.setdefault(str(a), {})[m] = float(w)
            cruce = {'radios_por_metodo': usado, 'wape_validacion_pct': val,
                     'informe': 'pipeline/hexoma/validacion_cruce.md'}
        fuentes = []
        if any(s == 'externos' for s, _ in objetos):
            cur.execute('SELECT row_to_json(f) FROM externos.fuentes f')
            fuentes = [r[0] for r in cur.fetchall()]
    conn.close()

    manifest = {
        'nombre': args.nombre,
        'fecha': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
        'base': f'postgresql local, base {DB}',
        'neahub_commit': git('rev-parse', 'HEAD'),
        'neahub_cambios_sin_commitear': bool(git('status', '--porcelain', '--', 'pipeline')),
        'consulta': sql,
        'filas': len(df),
        'columnas': {c: {'tipo': str(df[c].dtype), 'comentario': cols_coment.get(c)} for c in df.columns},
        'objetos': meta_obj,
        'cruce': cruce,
        'fuentes_externas': fuentes,
        'parquet_sha256': sha,
        'nota': 'Tasas: sumar _num y _den y dividir al final; no promediar tasas. Ver pipeline/hexoma/README.md.',
    }
    (args.destino / f'{args.nombre}.manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str),
                                                              encoding='utf-8')
    (args.destino / f'{args.nombre}.sql').write_text(sql, encoding='utf-8')
    log(f'  {pq}: {len(df):,} filas × {len(df.columns)} columnas; manifiesto y consulta al lado ({time.time() - t0:.0f} s)')


if __name__ == '__main__':
    main()
