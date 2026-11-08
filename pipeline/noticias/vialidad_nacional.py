"""Estado de las rutas nacionales en Misiones según Vialidad Nacional (fuente oficial, sólo datos).

Vialidad Nacional publica el estado ACTUAL de cada tramo en una Google Sheet pública (la que alimenta
argentina.gob.ar/transporte/vialidad-nacional/estado-de-rutas). No hay histórico público: la serie empieza con la
primera captura (7-nov-2026) y crece con la tarea diaria. La Wayback Machine guarda 1 copia vieja de los datos
(9-dic-2023), que se importa una sola vez con --wayback.

  noticias.vn_estado_rutas   captura, fuente (sheet_csv | wayback), ruta, tramo, estado, calzada, km, observaciones,
                             actualizado (la fecha que pone Vialidad), url
Cada captura cruda queda en %HEXOMA_FUENTES%/noticias/vialidad_nacional/.

Uso (desde neahub/):
    python pipeline/noticias/vialidad_nacional.py            # captura de hoy
    python pipeline/noticias/vialidad_nacional.py --wayback  # además, la copia de 2023
"""
from __future__ import annotations

import argparse
import datetime as dt
import html
import io
import json
import re
import sys
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'noticias'))
from bajar_notas import DIR  # noqa: E402
from build_hexoma import DB, connect, log  # noqa: E402

UA = {'User-Agent': 'spatia-pipeline/1.0 (+https://spatia.ar; investigacion CONICET)'}
HOJA = '17AqjqeNvM4nG6cOUsUFKFaKXMiNmztYfzHIxeM9FcXk'
CSV = f'https://docs.google.com/spreadsheets/d/{HOJA}/gviz/tq?tqx=out:csv&sheet=tablavisible'
PAGINA = 'https://www.argentina.gob.ar/transporte/vialidad-nacional/estado-de-rutas'
WAYBACK = [('20231209030021', f'https://sheets.googleapis.com/v4/spreadsheets/{HOJA}/values/tablavisible'
                              '?key=AIzaSyCq2wEEKL9-6RmX-TkW23qJsrmnFHFf5tY&alt=json')]
CRUDO = DIR / 'vialidad_nacional'
COLS = ['provincia', 'ruta', 'tramo', 'estado', 'calzada', 'km', 'conoce_mas', 'observaciones', 'actualizado']


def sin_html(x) -> str | None:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    t = html.unescape(re.sub(r'<[^>]+>', ' ', str(x)))
    t = re.sub(r'\s+', ' ', t).strip()
    return t or None


def normalizar(filas: list[list], captura: dt.datetime, fuente: str, url: str) -> pd.DataFrame:
    d = pd.DataFrame([f[:9] + [None] * (9 - len(f[:9])) for f in filas], columns=COLS)
    d = d[d.provincia.astype(str).str.strip().str.lower() == 'misiones'].copy()   # sólo Misiones
    for c in COLS:
        d[c] = d[c].map(sin_html)
    d['km'] = pd.to_numeric(d.km.str.replace('.', '', regex=False).str.replace(',', '.', regex=False), errors='coerce')
    d['actualizado'] = pd.to_datetime(d.actualizado, format='%d/%m/%Y %H:%M', errors='coerce')
    d['captura'] = captura
    d['fuente'] = fuente
    d['url'] = url
    return d.drop(columns=['provincia', 'conoce_mas'])


def guardar(d: pd.DataFrame) -> int:
    conn = connect(DB)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS noticias.vn_estado_rutas (captura timestamp, fuente text, ruta text,
                   tramo text, estado text, calzada text, km real, observaciones text, actualizado timestamp, url text,
                   PRIMARY KEY (captura, ruta, tramo))""")
    cur.execute("COMMENT ON TABLE noticias.vn_estado_rutas IS 'Estado de rutas nacionales en Misiones, Vialidad Nacional "
                "(Google Sheet pública de argentina.gob.ar); una fila por tramo y captura; sin histórico oficial antes "
                "del 7-nov-2026 salvo la copia Wayback del 9-dic-2023 (pipeline/noticias/vialidad_nacional.py)'")
    cur.execute('CREATE TEMP TABLE _v (LIKE noticias.vn_estado_rutas)')
    cols = ['captura', 'fuente', 'ruta', 'tramo', 'estado', 'calzada', 'km', 'observaciones', 'actualizado', 'url']
    buf = io.StringIO()
    d[cols].to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert("COPY _v FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute('INSERT INTO noticias.vn_estado_rutas SELECT * FROM _v ON CONFLICT DO NOTHING')
    n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--wayback', action='store_true')
    a = ap.parse_args()
    CRUDO.mkdir(parents=True, exist_ok=True)
    s = requests.Session()
    s.headers.update(UA)
    ahora = dt.datetime.now().replace(second=0, microsecond=0)
    r = s.get(CSV, timeout=60)
    r.raise_for_status()
    r.encoding = 'utf-8'
    (CRUDO / f'{ahora:%Y%m%d_%H%M}.csv').write_text(r.text, encoding='utf-8')
    filas = pd.read_csv(io.StringIO(r.text), header=None, dtype=str, keep_default_na=False).values.tolist()[2:]
    d = normalizar(filas, ahora, 'sheet_csv', PAGINA)
    n = guardar(d)
    log(f'Vialidad Nacional {ahora:%Y-%m-%d %H:%M}: {len(d)} tramos de Misiones, {n} filas nuevas; '
        f'estados {d.estado.value_counts().to_dict()}')
    if a.wayback:
        for ts, url in WAYBACK:
            rr = s.get(f'https://web.archive.org/web/{ts}id_/{url}', timeout=120)
            if rr.status_code != 200:
                log(f'  wayback {ts}: {rr.status_code}')
                continue
            (CRUDO / f'wayback_{ts}.json').write_text(rr.text, encoding='utf-8')
            vals = json.loads(rr.text).get('values', [])
            dw = normalizar(vals[2:] if vals and vals[0] and vals[0][0].startswith('filtro') else vals[1:],
                            dt.datetime.strptime(ts, '%Y%m%d%H%M%S'), 'wayback', f'https://web.archive.org/web/{ts}/{url}')
            log(f'  wayback {ts}: {len(dw)} tramos de Misiones, {guardar(dw)} filas nuevas')
    return 0


if __name__ == '__main__':
    sys.exit(main())
