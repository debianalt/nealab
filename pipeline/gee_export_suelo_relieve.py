"""Baja suelo (SoilGrids 2.0) y relieve (FABDEM) del NEA argentino para el Hexoma (A1).

Descarga directa con ee.data.computePixels, en paralelo, a %HEXOMA_FUENTES%/suelo/.

- SoilGrids 2.0 (projects/soilgrids-isric/<var>_mean), en su grilla NATIVA de 250 m (proyección
  homolosina/Mollweide del asset, sin remuestrear). Por variable, la media 0–30 cm ponderada por
  espesor: (5·[0–5] + 10·[5–15] + 15·[15–30]) / 30, en las unidades enteras del asset (se pasan a
  unidades físicas en build_hexoma_suelo.py). −1 = sin predicción (agua, zonas urbanas).
  Es un MODELO (predicción con aprendizaje automático a partir de perfiles), no una medición.
- FABDEM (projects/sat-io/open-datasets/FABDEM, Copernicus GLO-30 sin dosel ni edificios, licencia
  no comercial), 1" (~30 m): pendiente (grados) calculada a 30 m y elevación, reducidas a una
  grilla de 3" (~90 m): pendiente media ×100, elevación media, mínima y máxima (m), en int16.
  Se bajan sólo los bloques que contienen algún centro de celda del Hexoma.

Uso:
    python pipeline/gee_export_suelo_relieve.py               # suelo y relieve
    python pipeline/gee_export_suelo_relieve.py --solo suelo
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
import pandas as pd

from gee_export_activity_rasters import authenticate
from gee_export_clima_diario import bajar
from gee_export_ghsl_epochs import BBOX

FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
SUELO = FUENTES_DIR / 'suelo'
CENTROS = FUENTES_DIR / 'clima' / 'centros.parquet'
SOIL_VARS = ['clay', 'sand', 'silt', 'soc', 'phh2o', 'cec', 'bdod', 'cfvo', 'nitrogen']
PROF = [('0-5cm', 5), ('5-15cm', 10), ('15-30cm', 15)]
PASO_REL = 1 / 1200  # 3" en grados
BLOQUE = 1500        # píxeles de 3" por lado (~1,25°): 4 bandas int16 ≈ 18 MB


def soilgrids(var: str) -> Path:
    dest = SUELO / f'soilgrids_{var}_0-30cm.npz'
    if dest.exists():
        return dest
    img0 = ee.Image(f'projects/soilgrids-isric/{var}_mean')
    proj = img0.projection().getInfo()
    sx, _, tx, _, sy, ty = proj['transform']
    img = img0.select(f'{var}_{PROF[0][0]}_mean').multiply(PROF[0][1])
    for prof, k in PROF[1:]:
        img = img.add(img0.select(f'{var}_{prof}_mean').multiply(k))
    img = img.divide(30).round().unmask(-1).toInt16().rename(var)
    # ventana nativa que cubre BBOX: esquinas del rectángulo en la proyección del asset
    from pyproj import Transformer
    tr = Transformer.from_crs('EPSG:4326', proj['wkt'], always_xy=True)
    xs, ys = [], []
    for lng in np.linspace(BBOX[0], BBOX[2], 30):
        for lat in np.linspace(BBOX[1], BBOX[3], 30):
            x, y = tr.transform(lng, lat)
            xs.append(x); ys.append(y)
    c0, c1 = math.floor((min(xs) - tx) / sx), math.ceil((max(xs) - tx) / sx)
    f0, f1 = math.floor((max(ys) - ty) / sy), math.ceil((min(ys) - ty) / sy)
    partes = []
    paso = 2500  # filas por pedido (int16: ~2500 × 4000 × 2 B = 20 MB)
    for r0 in range(f0, f1, paso):
        r1 = min(r0 + paso, f1)
        req = {'expression': img, 'fileFormat': 'NPY',
               'grid': {'dimensions': {'width': c1 - c0, 'height': r1 - r0},
                        'affineTransform': {'scaleX': sx, 'shearX': 0, 'translateX': tx + c0 * sx,
                                            'shearY': 0, 'scaleY': sy, 'translateY': ty + r0 * sy},
                        'crsWkt': proj['wkt']}}
        b = ee.data.computePixels(req)
        partes.append(np.load(io.BytesIO(b), max_header_size=10 ** 7)[var])
    a = np.concatenate(partes, axis=0)
    SUELO.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(dest, valor=a, wkt=proj['wkt'],
                        grilla=np.array([tx + c0 * sx, sx, ty + f0 * sy, sy, c1 - c0, f1 - f0]),
                        asset=f'projects/soilgrids-isric/{var}_mean', profundidad='0-30cm ponderado 5/10/15')
    return dest


def fabdem():
    col = ee.ImageCollection('projects/sat-io/open-datasets/FABDEM')
    proj30 = col.first().projection()
    dem = col.mosaic().setDefaultProjection(proj30).rename('elev')
    slope = ee.Terrain.slope(dem).rename('slope')
    g90 = ee.Projection('EPSG:4326').scale(PASO_REL, -PASO_REL)
    base = dem.addBands(slope)
    media = base.reduceResolution(ee.Reducer.mean(), maxPixels=64).reproject(g90)
    mn = dem.reduceResolution(ee.Reducer.min(), maxPixels=64).reproject(g90).rename('elev_min')
    mx = dem.reduceResolution(ee.Reducer.max(), maxPixels=64).reproject(g90).rename('elev_max')
    return ee.Image.cat([media.select('slope').multiply(100).round().rename('slope_x100'),
                         media.select('elev').round().rename('elev_media'), mn, mx]).toInt16()


def bloques_necesarios() -> list[tuple[int, int]]:
    cen = pd.read_parquet(CENTROS)
    x0 = math.floor(BBOX[0] / PASO_REL) * PASO_REL
    y0 = math.ceil(BBOX[3] / PASO_REL) * PASO_REL
    bc = np.floor((cen.lng.values - x0) / PASO_REL / BLOQUE).astype(int)
    bf = np.floor((y0 - cen.lat.values) / PASO_REL / BLOQUE).astype(int)
    return sorted(set(zip(bf.tolist(), bc.tolist())))


def relieve_bloque(img, bf: int, bc: int) -> Path:
    dest = SUELO / 'fabdem' / f'fabdem_{bf:02d}_{bc:02d}.npz'
    if dest.exists():
        return dest
    x0 = math.floor(BBOX[0] / PASO_REL) * PASO_REL + bc * BLOQUE * PASO_REL
    y0 = math.ceil(BBOX[3] / PASO_REL) * PASO_REL - bf * BLOQUE * PASO_REL
    g = {'x0': x0, 'dx': PASO_REL, 'y0': y0, 'dy': -PASO_REL, 'ancho': BLOQUE, 'alto': BLOQUE}
    r = bajar(img, g)
    dest.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(dest, grilla=np.array([x0, PASO_REL, y0, -PASO_REL, BLOQUE, BLOQUE]),
                        **{n: r[n] for n in r.dtype.names})
    return dest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--solo', choices=['suelo', 'relieve'])
    ap.add_argument('--workers', type=int, default=6)
    args = ap.parse_args()
    authenticate()
    t0 = time.time()
    fallas = []
    with ThreadPoolExecutor(args.workers) as ex:
        futs = {}
        if args.solo in (None, 'suelo'):
            futs.update({ex.submit(soilgrids, v): f'soilgrids {v}' for v in SOIL_VARS})
        if args.solo in (None, 'relieve'):
            img = fabdem()
            bl = bloques_necesarios()
            print(f'FABDEM: {len(bl)} bloques de {BLOQUE}×{BLOQUE} píxeles de 3"', flush=True)
            futs.update({ex.submit(relieve_bloque, img, bf, bc): f'fabdem {bf},{bc}' for bf, bc in bl})
        for f in as_completed(futs):
            try:
                p = f.result()
                print(f'  {futs[f]}: {p.stat().st_size / 1e6:.1f} MB ({time.time() - t0:.0f} s)', flush=True)
            except Exception as e:
                fallas.append(futs[f])
                print(f'  {futs[f]}: FALLÓ {str(e)[:200]}', flush=True)
    print(f'listo en {time.time() - t0:.0f} s; fallas: {fallas}')
    return 1 if fallas else 0


if __name__ == '__main__':
    sys.exit(main())
