"""Actualización incremental del archivo de prensa y de la matriz de eventos (noticias v3, fase D).

Cada pasada trae sólo lo nuevo, con 7 días de margen (si una pasada se pierde, la siguiente la recupera):
  1. medios con API de WordPress (7, con misiones.gob.ar): cada consulta con after = última nota del medio − 7 días
  2. El Territorio, La Voz de Cataratas, Misiones Online: sub-sitemaps con <lastmod> ≥ esa fecha; MisionesCuatro:
     capturas de la Wayback posteriores (filtro v2 por la dirección, como bajar_sin_api.py)
  3. Vialidad Nacional (estado de rutas de hoy)
  4. carga (sólo suma), extraer.py, ubicar.py, eventos.py
  5. zip incremental del crudo nuevo en OneDrive/nealab/archivo_noticias/inc_AAAAMMDD.zip y una línea de resumen

Programado en el Programador de tareas de Windows (actualizar_tareas.cmd): domingos 04:00. Registro en
%HEXOMA_FUENTES%/noticias/actualizar.log.

Uso (desde neahub/):  python pipeline/noticias/actualizar.py [--desde AAAA-MM-DD]
"""
from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'noticias'))
import bajar_notas as BN  # noqa: E402
import bajar_sin_api as BS  # noqa: E402
from build_hexoma import DB, connect, log  # noqa: E402

MARGEN = dt.timedelta(days=7)
SIN_API = ['elterritorio', 'lavozdecataratas', 'misionesonline', 'misionescuatro']


def correr(script: str) -> None:
    r = subprocess.run([sys.executable, str(ROOT / 'pipeline' / 'noticias' / script)], cwd=ROOT,
                       capture_output=True, text=True, encoding='utf-8', errors='replace')
    ultima = [x for x in r.stdout.splitlines() if x.strip()][-1:] or ['']
    log(f'  {script}: {"ok" if r.returncode == 0 else "ERROR"} {ultima[0][:200]}')
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-2000:])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--desde', help='forzar la fecha de inicio (si no: última nota de cada medio − 7 días)')
    a = ap.parse_args()
    t0 = time.time()
    inicio = time.time()
    conn = connect(DB)
    ult = pd.read_sql("SELECT medio, max(fecha)::date ult, count(*) n FROM noticias.notas GROUP BY 1", conn).set_index('medio')
    antes = int(ult.n.sum())
    ya = set(pd.read_sql('SELECT url FROM noticias.notas', conn).url.map(BS.url_norm))
    conn.close()
    desde = lambda m: (dt.date.fromisoformat(a.desde) if a.desde  # noqa: E731
                       else (ult.ult.get(m, dt.date.today() - dt.timedelta(days=30)) - MARGEN))
    log(f'actualización {dt.date.today()}: {antes:,} notas en el archivo')

    with ThreadPoolExecutor(len(BN.MEDIOS)) as ex:
        res = dict(zip(BN.MEDIOS, ex.map(lambda kv: BN.bajar_medio(kv[0], kv[1], desde(kv[0])), BN.MEDIOS.items())))
    log('  API WordPress: ' + ', '.join(f'{m} {sum(v.values()):,}' for m, v in res.items()))
    BN.cargar(respaldo=False)
    with ThreadPoolExecutor(len(SIN_API)) as ex:
        res2 = dict(zip(SIN_API, ex.map(lambda m: BS.bajar_medio(m, ya, desde(m)), SIN_API)))
    log(f'  sin API: {res2}')
    BS.cargar(respaldo=False)
    correr('vialidad_nacional.py')
    correr('extraer.py')
    correr('ubicar.py')
    correr('eventos.py')

    # zip incremental: sólo los crudos escritos en esta pasada
    nuevos = [p for base in (BN.DIR / 'crudo', BN.DIR / 'crudo_html', BN.DIR / 'vialidad_nacional')
              for p in base.rglob('*') if p.is_file() and p.stat().st_mtime >= inicio]
    if nuevos:
        BN.RESPALDO.mkdir(parents=True, exist_ok=True)
        z = BN.RESPALDO / f'inc_{dt.date.today():%Y%m%d}.zip'
        with zipfile.ZipFile(z, 'a', zipfile.ZIP_DEFLATED) as zf:
            for p in nuevos:
                zf.write(p, p.relative_to(BN.DIR))
        log(f'  respaldo incremental: {z} ({len(nuevos):,} archivos)')
    conn = connect(DB)
    despues = pd.read_sql('SELECT count(*) n FROM noticias.notas', conn).n[0]
    ev = pd.read_sql('SELECT count(*) n, max(fecha_hasta) ult FROM noticias.eventos', conn).iloc[0]
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s: {despues - antes:,} notas nuevas ({despues:,}); {ev.n:,} eventos, '
        f'el último del {ev.ult}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
