"""Lluvia intensa en Misiones: GPM IMERG V07 cada media hora, resumida por día (fase 5: sólo datos).

Fuente: NASA/GPM_L3/IMERG_V07 (Google Earth Engine), 0,1°, cada 30 min, desde 1998; banda `precipitation` (mm/h,
producto final calibrado con pluviómetros GPCC a escala mensual). CHIRPS diario no discrimina los desbordes de arroyo
(demo El Soberbio): hace falta la intensidad de horas.

Por DÍA LOCAL (03:00–03:00 UTC = 00:00–24:00 de Misiones), en cada píxel de la grilla nativa de 0,1° sobre Misiones
+ margen (drenaje.BBOX), calculado del lado de GEE:
  mm_dia        lluvia del día (Σ tasa × 0,5 h)
  max_30min     tasa máxima de media hora (mm/h)
  max_3h_mm     lluvia máxima en 3 horas seguidas dentro del día (mm; ventanas de 6 medias horas)
  horas_lluvia  horas con tasa ≥ 0,1 mm/h
Un pedido por mes (una banda por día y variable), en paralelo; npz por mes en %HEXOMA_FUENTES%/infra/imerg/.
Reanudable. Las ventanas de 3 h que cruzan la medianoche local no se cuentan.

  infra.imerg_pixel    pixel_id, fila, col, lon, lat (centro)
  infra.imerg_diaria   pixel_id, fecha, mm_dia, max_30min, max_3h_mm, horas_lluvia
  infra.imerg_h3_pixel celda h3 de Misiones → píxel que contiene su centro

Uso (desde neahub/):
    python pipeline/infra/lluvia_imerg.py --bajar [--hasta-min 40]
    python pipeline/infra/lluvia_imerg.py --cargar
"""
from __future__ import annotations

import argparse
import calendar
import datetime as dt
import io
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
import drenaje  # noqa: E402

DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes')) / 'infra' / 'imerg'
COL = 'NASA/GPM_L3/IMERG_V07'
PASO = 0.1
INICIO = dt.date(1998, 1, 1)
HILOS = 8
VARS = ['mm_dia', 'max_30min', 'max_3h_mm', 'horas_lluvia']


def grilla() -> dict:
    x0 = math.floor(drenaje.BBOX[0] / PASO) * PASO
    y0 = math.ceil(drenaje.BBOX[3] / PASO) * PASO
    ancho = int(round((math.ceil(drenaje.BBOX[2] / PASO) * PASO - x0) / PASO))
    alto = int(round((y0 - math.floor(drenaje.BBOX[1] / PASO) * PASO) / PASO))
    return {'x0': x0, 'dx': PASO, 'y0': y0, 'dy': -PASO, 'ancho': ancho, 'alto': alto}


def imagen_dia(ee, d: dt.date):
    ini = ee.Date(f'{d:%Y-%m-%d}T03:00:00Z')
    col = ee.ImageCollection(COL).filterDate(ini, ini.advance(1, 'day')).select('precipitation').sort('system:time_start')
    mm_dia = col.sum().multiply(0.5).rename('mm_dia')
    max30 = col.max().rename('max_30min')
    horas = col.map(lambda i: i.gte(0.1)).sum().multiply(0.5).rename('horas_lluvia')
    lst = col.toList(48)
    n = lst.size()
    ventanas = ee.List.sequence(0, n.subtract(6)).map(
        lambda i: ee.ImageCollection(lst.slice(ee.Number(i), ee.Number(i).add(6))).sum().multiply(0.5))
    max3h = ee.ImageCollection(ventanas).max().rename('max_3h_mm')
    return ee.Image.cat([mm_dia, max30, max3h, horas]).unmask(-1).toFloat()


def bajar_mes(ee, anio: int, mes: int, g: dict) -> str:
    from gee_export_clima_diario import bajar
    dest = DIR / f'imerg_{anio}{mes:02d}.npz'
    if dest.exists():
        return 'ya'
    ult = min(calendar.monthrange(anio, mes)[1], (dt.date.today() - dt.timedelta(days=2)).day
              if (anio, mes) == (dt.date.today().year, dt.date.today().month) else 99)
    dias = [dt.date(anio, mes, k) for k in range(1, ult + 1)]
    img = ee.Image.cat([imagen_dia(ee, d).rename([f'{v}_{d:%d}' for v in VARS]) for d in dias])
    r = bajar(img, g)
    datos = {v: np.stack([r[f'{v}_{d:%d}'] for d in dias]) for v in VARS}
    DIR.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix('.part.npz')
    np.savez_compressed(tmp, fechas=np.array([d.isoformat() for d in dias]), **datos,
                        grilla=np.array([g['x0'], g['dx'], g['y0'], g['dy'], g['ancho'], g['alto']]))
    tmp.replace(dest)
    return 'nuevo'


def meses() -> list[tuple[int, int]]:
    hoy = dt.date.today()
    out, a, m = [], INICIO.year, INICIO.month
    while (a, m) <= (hoy.year, hoy.month):
        out.append((a, m))
        a, m = (a + (m == 12), m % 12 + 1)
    return out


def bajar_todo(hasta_min: float) -> int:
    import ee
    from gee_export_activity_rasters import authenticate
    authenticate()
    g = grilla()
    falta = [(a, m) for a, m in meses() if not (DIR / f'imerg_{a}{m:02d}.npz').exists()]
    print(f'IMERG: {len(meses())} meses; faltan {len(falta)}; grilla {g["ancho"]}×{g["alto"]}', flush=True)
    t0, hechos, errores = time.time(), 0, []
    with ThreadPoolExecutor(HILOS) as ex:
        futs = {}
        it = iter(falta)
        for _ in range(HILOS):
            x = next(it, None)
            if x:
                futs[ex.submit(bajar_mes, ee, *x, g)] = x
        while futs:
            for f in as_completed(list(futs)):
                x = futs.pop(f)
                try:
                    f.result()
                    hechos += 1
                except Exception as e:  # noqa: BLE001
                    errores.append((x, str(e)[:150]))
                if hechos % 12 == 0:
                    print(f'  {hechos} meses ({time.time() - t0:.0f} s)', flush=True)
                if time.time() - t0 < hasta_min * 60:
                    nxt = next(it, None)
                    if nxt:
                        futs[ex.submit(bajar_mes, ee, *nxt, g)] = nxt
                break
    resto = len(falta) - hechos - len(errores)
    print(f'tanda: {hechos} meses en {time.time() - t0:.0f} s; errores {len(errores)}; '
          f'{"completo" if resto <= 0 and not errores else f"faltan {len(falta) - hechos}"}', flush=True)
    for x, e in errores[:8]:
        print(f'  error {x}: {e}', flush=True)
    return 0


def cargar() -> int:
    import pandas as pd

    from build_hexoma import DB, connect, log
    g = grilla()
    filas = []
    for p in sorted(DIR.glob('imerg_*.npz')):
        if '.part' in p.name:
            continue
        z = np.load(p)
        nd = len(z['fechas'])
        npix = g['alto'] * g['ancho']
        d = pd.DataFrame({'pixel_id': np.tile(np.arange(npix), nd), 'fecha': np.repeat(z['fechas'], npix)})
        for v in VARS:
            x = z[v].reshape(nd, -1).ravel().astype(float)
            d[v] = np.where(x < 0, np.nan, x)
        filas.append(d)
    df = pd.concat(filas, ignore_index=True)
    ff, cc = np.indices((g['alto'], g['ancho']))
    pix = pd.DataFrame({'pixel_id': np.arange(g['alto'] * g['ancho']), 'fila': ff.ravel(), 'col': cc.ravel(),
                        'lon': g['x0'] + (cc.ravel() + 0.5) * PASO, 'lat': g['y0'] - (ff.ravel() + 0.5) * PASO})
    conn = connect(DB)
    cur = conn.cursor()
    for t, d_, ddl in [('infra.imerg_pixel', pix, 'pixel_id int PRIMARY KEY, fila int, col int, lon double precision, lat double precision'),
                       ('infra.imerg_diaria', df.round(2), 'pixel_id int, fecha date, mm_dia real, max_30min real, max_3h_mm real, horas_lluvia real')]:
        cur.execute(f'DROP TABLE IF EXISTS {t} CASCADE')
        cur.execute(f'CREATE TABLE {t} ({ddl})')
        buf = io.StringIO()
        d_.to_csv(buf, index=False, header=False, na_rep='\\N')
        buf.seek(0)
        cur.copy_expert(f"COPY {t} FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute('CREATE INDEX ON infra.imerg_diaria (pixel_id, fecha)')
    cur.execute('CREATE INDEX ON infra.imerg_diaria (fecha)')
    cur.execute('DROP TABLE IF EXISTS infra.imerg_h3_pixel')
    cur.execute(f"""CREATE TABLE infra.imerg_h3_pixel AS
        SELECT h.h3, (floor(({g['y0']} - (h3_cell_to_lat_lng(h.h3))[1]) / {PASO})::int * {g['ancho']}
                      + floor(((h3_cell_to_lat_lng(h.h3))[0] - {g['x0']}) / {PASO})::int) AS pixel_id
        FROM hexagonos h WHERE h.territorio_id = 1""")
    cur.execute("COMMENT ON TABLE infra.imerg_diaria IS 'GPM IMERG V07 por día local (03–03 UTC) en la grilla de 0,1° de "
                "Misiones: lluvia del día, máxima de 30 min (mm/h), máxima en 3 h seguidas (mm), horas con ≥ 0,1 mm/h "
                "(lluvia_imerg.py)'")
    cur.execute("""INSERT INTO infra.fuentes VALUES ('infra.imerg_diaria', 'NASA GPM IMERG V07 (Google Earth Engine)', %s,
                   %s, now(), %s, 'media hora → día local') ON CONFLICT (tabla) DO UPDATE SET fecha_fuente=EXCLUDED.fecha_fuente,
                   cargado=now(), n=EXCLUDED.n""", (COL, str(df.fecha.max()), len(df)))
    conn.commit()
    conn.close()
    log(f'infra.imerg_diaria: {len(df):,} filas, {df.fecha.nunique():,} días, {len(pix)} píxeles')
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--bajar', action='store_true')
    ap.add_argument('--hasta-min', type=float, default=40)
    ap.add_argument('--cargar', action='store_true')
    a = ap.parse_args()
    if a.bajar:
        return bajar_todo(a.hasta_min)
    if a.cargar:
        return cargar()
    return 0


if __name__ == '__main__':
    sys.exit(main())
