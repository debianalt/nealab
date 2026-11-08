"""Juicio de Claude (CLI con la suscripción, sin API) sobre menciones de prensa: ¿cuentan un hecho? ¿fuera de Misiones?

Las reglas de extraer.py juntan candidatas (recall); este paso decide si cada oración informa un hecho del tipo asignado
(precisión). Piloto del 8-nov-2026 (piloto_clasificador_llm.py, 240 menciones anotadas a mano): 96 % de precisión del
tipo filtrando con «hecho», contra 75 % de las reglas solas; descarta ~11 % de los hechos anotados (varios eran alertas).

v2 (8-nov-2026, tras la muestra de celdas semilla 16: 48 % por municipio-día con reglas, 62 % con v1): suma el título de
la nota, el municipio asignado y dos juicios: reciente (no un recuerdo de otra crecida) y en_municipio.

Caché: noticias.clasificacion_llm, clave = (md5 de oración + municipio, tipo, modelo, versión de las instrucciones).
Volver a correr extraer.py no repite llamadas; cambiar INSTR obliga a subir VERSION.

Uso como módulo:  clasificar(conn, df)  con columnas oracion, tipo, medio, fecha_nota [, municipio, titulo]
                  → df + hecho, reciente, fuera, en_municipio, motivo_llm;  vale(df) = la oración sostiene su celda
Uso directo (desde neahub/):  python pipeline/noticias/clasificar_llm.py --ids 12,34,…   (mencion_id)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
from build_hexoma import DB, connect, log  # noqa: E402

MODELO = 'haiku'
VERSION = 'v2-2026-11-08'
LOTE = 60
HILOS = 4

INSTR = """Sos anotador de un archivo de prensa de Misiones (Argentina). Para cada oración de una nota periodística te doy
el título de la nota, la fecha, el tipo de evento que le asignaron unas reglas y el municipio de Misiones al que se la
asignaron. Decidí:

- hecho: true si la oración informa un hecho de ese tipo que ocurrió o está ocurriendo (un corte de puente, un corte de
  ruta o camino, una crecida o desborde, personas evacuadas o aisladas). false si es un pronóstico, una advertencia, un
  condicional, una negación («no hubo cortes», «sin evacuados»), una opinión o un tema general («los cortes de ruta
  perjudican»), una obra para evitar inundaciones, un problema crónico sin hecho puntual, o si el tipo no corresponde
  (un corte de luz no es corte de ruta; «lluvias aisladas» no son personas aisladas). Un levantamiento o una
  rehabilitación («se habilitó el puente tras la crecida», «docentes levantaron el corte») cuenta como hecho: el corte
  existió.
- reciente: true si ese hecho está ocurriendo en la fecha de la nota o en los 3 días previos; false si la oración lo
  recuerda como algo de antes («las inundaciones de 2014», «la crecida pasada», «damnificados de la última inundación»).
- fuera: true si el hecho ocurre fuera de la provincia de Misiones (otra provincia argentina, Paraguay, Brasil u otro
  país; usá también el título); false si es en Misiones o no se puede saber. Los puentes y pasos de frontera de Misiones
  cuentan como Misiones.
- en_municipio: "si" si el hecho ocurre en el municipio asignado (o en un lugar dentro de él); "no" si la oración o el
  título lo ubican en otro municipio, o lo describen para toda la provincia sin nombrar ese municipio; "?" si no se
  puede saber.
- motivo (sólo para cortes de puente o de ruta): protesta | accidente | agua | obra | otro (agua incluye lluvia,
  temporal, crecida, socavón o derrumbe por lluvia; otro = no se lee en la oración).

Respondé SOLO con un arreglo JSON, un objeto por oración, en el mismo orden:
{"i": <número>, "hecho": bool, "reciente": bool, "fuera": bool, "en_municipio": "si"|"no"|"?", "motivo": str|null}.
"""
CAMPOS = ['hecho', 'reciente', 'fuera', 'en_municipio', 'motivo_llm']


def _md5(s: str) -> str:
    return hashlib.md5(str(s).encode('utf-8')).hexdigest()


def _lote(filas: pd.DataFrame) -> list[dict]:
    txt = INSTR + '\n\nOraciones:\n' + '\n'.join(
        f'{k}. [tipo: {r.tipo}; municipio asignado: {r.municipio or "—"}; medio: {r.medio}; fecha: {r.fecha_nota}; '
        f'título: {str(r.titulo or "")[:160]}] {str(r.oracion)[:700]}'
        for k, r in enumerate(filas.itertuples()))
    for intento in range(2):
        p = subprocess.run(['claude', '-p', '--model', MODELO, '--output-format', 'json', '--allowedTools', ''],
                           input=txt, capture_output=True, text=True, encoding='utf-8', timeout=900)
        try:
            res = json.loads(p.stdout)['result']
            out = json.loads(res[res.find('['):res.rfind(']') + 1])
            if len(out) == len(filas):
                return [dict(o, clave=c) for o, c in zip(sorted(out, key=lambda o: o['i']), filas.clave)]
        except (json.JSONDecodeError, KeyError, TypeError):
            pass
        log(f'  lote de {len(filas)} sin respuesta válida (intento {intento + 1})')
    return []


def _hechas(conn) -> pd.DataFrame:
    h = pd.read_sql('SELECT oracion_md5, tipo, hecho, reciente, fuera, en_municipio, motivo motivo_llm '
                    'FROM noticias.clasificacion_llm WHERE modelo = %(m)s AND version = %(v)s', conn,
                    params={'m': MODELO, 'v': VERSION})
    h['clave'] = h.oracion_md5 + '|' + h.tipo
    return h


def clasificar(conn, df: pd.DataFrame) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS noticias.clasificacion_llm (
                     oracion_md5 text, tipo text, modelo text, version text, hecho boolean, fuera boolean,
                     motivo text, fecha timestamp DEFAULT now(), PRIMARY KEY (oracion_md5, tipo, modelo, version))""")
    cur.execute('ALTER TABLE noticias.clasificacion_llm ADD COLUMN IF NOT EXISTS reciente boolean')
    cur.execute('ALTER TABLE noticias.clasificacion_llm ADD COLUMN IF NOT EXISTS en_municipio text')
    cur.execute("COMMENT ON TABLE noticias.clasificacion_llm IS 'Juicio de Claude por oración, municipio asignado y tipo "
                "(hecho, reciente, fuera de Misiones, en el municipio, motivo), con modelo y versión de las instrucciones: "
                "pipeline/noticias/clasificar_llm.py'")
    conn.commit()
    df = df.copy()
    for c in ('municipio', 'titulo'):
        if c not in df:
            df[c] = ''
        df[c] = df[c].fillna('').astype(str).str.replace(r'^Municipio de ', '', regex=True)
    df['oracion_md5'] = (df.oracion.astype(str) + '|' + df.municipio).map(_md5)
    df['clave'] = df.oracion_md5 + '|' + df.tipo
    hechas = _hechas(conn)
    falta = df[~df.clave.isin(hechas.clave)].drop_duplicates('clave')
    if len(falta):
        lotes = [falta.iloc[k:k + LOTE] for k in range(0, len(falta), LOTE)]
        log(f'clasificar_llm: {len(falta):,} oraciones nuevas en {len(lotes)} llamadas ({MODELO}, {VERSION})')
        with ThreadPoolExecutor(HILOS) as ex:
            res = [x for r in ex.map(_lote, lotes) for x in r]
        filas = [(c.split('|')[0], c.split('|', 1)[1], MODELO, VERSION, bool(x.get('hecho')), bool(x.get('fuera')),
                  x.get('motivo'), bool(x.get('reciente', True)), str(x.get('en_municipio', '?')))
                 for x in res for c in [x['clave']]]
        cur.executemany("""INSERT INTO noticias.clasificacion_llm (oracion_md5, tipo, modelo, version, hecho, fuera,
                                                                    motivo, reciente, en_municipio)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""", filas)
        conn.commit()
        hechas = _hechas(conn)
    return df.merge(hechas[['clave'] + CAMPOS], on='clave', how='left').drop(columns='clave')


def vale(j: pd.DataFrame) -> pd.Series:
    """La oración sostiene su celda municipio-día de agua: hecho reciente, en Misiones, no ubicado en otro municipio y,
    si es un corte, con motivo agua u otro"""
    corte = j.tipo.isin(['corte_puente', 'corte_ruta_camino'])
    return (j.hecho.fillna(False).astype(bool) & j.reciente.fillna(False).astype(bool)
            & ~j.fuera.fillna(False).astype(bool) & j.en_municipio.fillna('?').ne('no')
            & (~corte | j.motivo_llm.fillna('otro').isin(['agua', 'otro'])))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--ids', required=True)
    a = ap.parse_args()
    conn = connect(DB)
    ids = [int(x) for x in a.ids.split(',')]
    m = pd.read_sql("""SELECT m.mencion_id, m.oracion, m.tipo, m.medio, m.fecha_nota::date fecha_nota, n.titulo,
                              mu.nombre municipio
                       FROM noticias.menciones m JOIN noticias.notas n USING (nota_id)
                       LEFT JOIN infra.visor_municipios mu ON ST_Contains(mu.geom, m.geom)
                       WHERE m.mencion_id = ANY(%(i)s)""", conn, params={'i': ids})
    print(clasificar(conn, m)[['mencion_id', 'tipo'] + CAMPOS].to_string(index=False))
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
