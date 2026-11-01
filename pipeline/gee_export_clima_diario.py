"""Baja el clima diario del NEA argentino para el Hexoma (A1, series agroclimáticas).

Descarga directa con ee.data.computePixels, en paralelo, en la grilla NATIVA de cada fuente (sin
remuestrear: el píxel bajado es el píxel original). Las exportaciones en lote a GCS corrían de a
una (≈4–8 h para todo); así tarda minutos. Un archivo por fuente y año:
%HEXOMA_FUENTES%/clima/diario/<fuente>/<fuente>_<año>.npz con un array (días × filas × columnas)
por variable, las fechas, la banda de origen y la grilla (x0, dx, y0, dy, ancho, alto).

Fuentes:
- chirps: UCSB-CHC/CHIRPS/V3/DAILY_RNL, precipitation (mm/día), 0,05°, desde 1981. Estimación
  satelital calibrada con pluviómetros; el dato diario es una desagregación de la pentada con la
  temporalidad de ERA5.
- era5: ECMWF/ERA5_LAND/DAILY_AGGR, 0,1°, desde 1950 (la colección arranca el 2-ene-1950).
  Reanálisis; día UTC. Tmax, Tmin, punto de rocío medio, viento u/v 10 m medio, presión de
  superficie y radiación solar incidente (J/m²/día): lo que pide la ET0 FAO-56 de referencia y los
  índices de temperatura. Más la humedad volumétrica del suelo, capas 1–3 (0–7, 7–28, 28–100 cm),
  en media MENSUAL (sw1..sw3, 12 bandas por año).

Hasta el 30-jun-2026 (fin de la campaña 2025/26). Idempotente: saltea los años ya bajados.

Uso:
    python pipeline/gee_export_clima_diario.py --fuente chirps
    python pipeline/gee_export_clima_diario.py --fuente era5 --anios 1950-2026 --workers 8
    python pipeline/gee_export_clima_diario.py --fuente era5 --anios 2024 --rehacer
"""
from __future__ import annotations

import argparse
import io
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import ee
import numpy as np

from gee_export_activity_rasters import authenticate
from gee_export_ghsl_epochs import BBOX

FIN = '2026-07-01'  # exclusivo
FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
DIARIO = FUENTES_DIR / 'clima' / 'diario'

FUENTES = {
    'chirps': {
        'asset': 'UCSB-CHC/CHIRPS/V3/DAILY_RNL',
        'vars': {'p': 'precipitation'},
        'desde': 1981,
        'partes': 2,  # un año entero (~49 MB) pasa el límite de computePixels
    },
    'era5': {
        'asset': 'ECMWF/ERA5_LAND/DAILY_AGGR',
        'vars': {
            'tmax': 'temperature_2m_max',          # K
            'tmin': 'temperature_2m_min',          # K
            'td': 'dewpoint_temperature_2m',       # K, media diaria
            'u10': 'u_component_of_wind_10m',      # m/s, media diaria
            'v10': 'v_component_of_wind_10m',      # m/s, media diaria
            'sp': 'surface_pressure',              # Pa
            'ssrd': 'surface_solar_radiation_downwards_sum',  # J/m²/día (Rs de FAO-56)
        },
        'mensual': {
            'sw1': 'volumetric_soil_water_layer_1',  # m³/m³, 0–7 cm
            'sw2': 'volumetric_soil_water_layer_2',  # 7–28 cm
            'sw3': 'volumetric_soil_water_layer_3',  # 28–100 cm
        },
        'desde': 1950,
        'partes': 1,
    },
}


def grilla(asset: str) -> dict:
    """Ventana de la grilla nativa que cubre BBOX."""
    p = ee.ImageCollection(asset).first().projection().getInfo()
    assert p['crs'] == 'EPSG:4326', p
    dx, _, gx0, _, dy, gy0 = p['transform']
    c0 = math.floor((BBOX[0] - gx0) / dx)
    c1 = math.ceil((BBOX[2] - gx0) / dx)
    f0 = math.floor((BBOX[3] - gy0) / dy)
    f1 = math.ceil((BBOX[1] - gy0) / dy)
    return {'x0': gx0 + c0 * dx, 'dx': dx, 'y0': gy0 + f0 * dy, 'dy': dy,
            'ancho': c1 - c0, 'alto': f1 - f0, 'gx0': gx0, 'gy0': gy0}


def bajar(img, g: dict) -> np.ndarray:
    req = {'expression': img, 'fileFormat': 'NPY',
           'grid': {'dimensions': {'width': g['ancho'], 'height': g['alto']},
                    'affineTransform': {'scaleX': g['dx'], 'shearX': 0, 'translateX': g['x0'],
                                        'shearY': 0, 'scaleY': g['dy'], 'translateY': g['y0']},
                    'crsCode': 'EPSG:4326'}}
    for intento in range(4):
        try:
            b = ee.data.computePixels(req)
            r = np.load(io.BytesIO(b), max_header_size=10 ** 7)
            return r
        except Exception as e:  # cuota o error transitorio de Google
            if intento == 3:
                raise
            time.sleep(10 * (intento + 1))
            print(f'    reintento ({e.__class__.__name__}: {str(e)[:80]})', flush=True)


def tramos(anio: int, partes: int) -> list[tuple[str, str]]:
    ini, fin = f'{anio}-01-01', min(f'{anio + 1}-01-01', FIN)
    if partes == 1 or fin <= f'{anio}-07-01':
        return [(ini, fin)]
    return [(ini, f'{anio}-07-01'), (f'{anio}-07-01', fin)]


def anio_fuente(fuente: str, anio: int, g: dict) -> Path:
    cfg = FUENTES[fuente]
    dest = DIARIO / fuente / f'{fuente}_{anio}.npz'
    datos, fechas = {v: [] for v in cfg['vars']}, []
    for ini, fin in tramos(anio, cfg['partes']):
        col = ee.ImageCollection(cfg['asset']).filterDate(ini, fin).sort('system:time_start')
        fs = None
        for corto, banda in cfg['vars'].items():
            r = bajar(col.select([banda]).toBands().toFloat(), g)
            nombres = r.dtype.names
            assert all(n.endswith('_' + banda) for n in nombres), nombres[:2]
            dias = [n.split('_')[0] for n in nombres]
            assert fs is None or dias == fs, (fuente, anio, corto)
            fs = dias
            datos[corto].append(np.stack([r[n] for n in nombres]).astype(np.float32))
        fechas += fs
    out = {v: np.concatenate(x, axis=0) for v, x in datos.items()}
    for corto, banda in cfg.get('mensual', {}).items():
        col = ee.ImageCollection(cfg['asset']).filterDate(f'{anio}-01-01', min(f'{anio + 1}-01-01', FIN))
        meses = [m for m in range(1, 13) if f'{anio}-{m:02d}-01' < FIN]
        img = ee.Image.cat([col.filter(ee.Filter.calendarRange(m, m, 'month')).select(banda).mean()
                            .rename(f'm{m:02d}') for m in meses]).toFloat()
        r = bajar(img, g)
        out[corto] = np.stack([r[f'm{m:02d}'] for m in meses]).astype(np.float32)
        out['meses'] = np.array(meses, dtype=np.int8)
    fechas_iso = np.array([f'{d[:4]}-{d[4:6]}-{d[6:]}' for d in fechas])
    assert len(set(fechas_iso)) == len(fechas_iso) and list(fechas_iso) == sorted(fechas_iso)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix('.tmp.npz')
    np.savez_compressed(tmp, fechas=fechas_iso, asset=cfg['asset'],
                        bandas=np.array([f'{k}={v}' for k, v in {**cfg['vars'], **cfg.get('mensual', {})}.items()]),
                        grilla=np.array([g['x0'], g['dx'], g['y0'], g['dy'], g['ancho'], g['alto'],
                                         g['gx0'], g['gy0']]), **out)
    tmp.replace(dest)
    return dest


def anios(spec: str | None, desde: int) -> list[int]:
    if not spec:
        return list(range(desde, 2027))
    out = []
    for parte in spec.split(','):
        a, _, b = parte.partition('-')
        out += list(range(int(a), int(b or a) + 1))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--fuente', required=True, choices=list(FUENTES))
    ap.add_argument('--anios', help='p. ej. 1981-2026 o 2024,2025')
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--rehacer', action='store_true')
    args = ap.parse_args()
    authenticate()
    cfg = FUENTES[args.fuente]
    g = grilla(cfg['asset'])
    print(f"{args.fuente}: grilla {g['ancho']}×{g['alto']} desde ({g['x0']:.4f}, {g['y0']:.4f}), paso {g['dx']}", flush=True)
    pend = [a for a in anios(args.anios, cfg['desde'])
            if args.rehacer or not (DIARIO / args.fuente / f'{args.fuente}_{a}.npz').exists()]
    t0, fallas = time.time(), []
    with ThreadPoolExecutor(args.workers) as ex:
        futs = {ex.submit(anio_fuente, args.fuente, a, g): a for a in pend}
        for f in as_completed(futs):
            a = futs[f]
            try:
                p = f.result()
                print(f'  {a}: {p.stat().st_size / 1e6:.0f} MB ({time.time() - t0:.0f} s)', flush=True)
            except Exception as e:
                fallas.append(a)
                print(f'  {a}: FALLÓ {e}', flush=True)
    print(f'{len(pend) - len(fallas)}/{len(pend)} años en {time.time() - t0:.0f} s; fallas: {sorted(fallas)}')
    return 1 if fallas else 0


if __name__ == '__main__':
    sys.exit(main())
