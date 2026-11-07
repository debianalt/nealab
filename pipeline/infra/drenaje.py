"""Área drenada de toda Misiones a 30 m, para detectar dónde un camino cruza un cauce (infra.cruces).

Mismo método que el de arroyos de la demo El Soberbio (conicet(1)/04_stan/10_demo_el_nino/04_arroyos.py),
escalado a la provincia:
  - FABDEM 1" (~30 m; Copernicus GLO-30 sin dosel ni edificios: bajo el monte misionero el terreno es el
    suelo, no la copa) de Misiones con ~5 km de margen, bajado con ee.data.computePixels en bloques int16
    (decímetros) a %HEXOMA_FUENTES%/infra/fabdem/;
  - pysheds D8: fill_pits → fill_depressions → resolve_flats → flowdir → accumulation, con el área real de
    cada celda como peso (la celda de 1" mide ~31 × 27 m y cambia con la latitud), así el resultado sale
    directamente en km².

Salidas en %HEXOMA_FUENTES%/infra/:
  fabdem_misiones.tif   elevación (m, float32)
  fdir_misiones.tif     dirección D8 (uint8, codificación de pysheds: 64 N, 128 NE, 1 E, 2 SE, 4 S, 8 SO, 16 O, 32 NO)
  acc_km2_misiones.tif  área drenada a cada celda (km², float32)

Límite conocido: los ríos que entran desde fuera del recorte (Paraná, Uruguay, Iguazú, Pepirí Guazú, San
Antonio) tienen el área drenada truncada. cruces.py los marca como río de frontera; su escenario es el de los
hidrómetros, no el del área drenada.

Uso (desde neahub/):
    python pipeline/infra/drenaje.py            # baja lo que falte y recalcula
    python pipeline/infra/drenaje.py --solo-bajar
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
# El instalador de PostGIS deja PROJ_LIB apuntando a su proj.db viejo; rasterio trae el suyo.
for _k in ('PROJ_LIB', 'PROJ_DATA'):
    if 'PostgreSQL' in os.environ.get(_k, ''):
        del os.environ[_k]

FUENTES = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes')) / 'infra'
BLOQUES = FUENTES / 'fabdem'
PASO = 1 / 3600                     # 1" en grados (grilla nativa de FABDEM)
BBOX = (-56.11, -28.21, -53.59, -25.44)   # Misiones (-56.06, -28.16, -53.64, -25.49) + ~5 km
BLOQUE = 3000                       # píxeles por lado: int16 → ~18 MB por pedido (límite ~48 MB)
R = 6371008.8


def grilla() -> tuple[float, float, int, int]:
    x0 = math.floor(BBOX[0] / PASO) * PASO
    y0 = math.ceil(BBOX[3] / PASO) * PASO
    ancho = int(round((BBOX[2] - x0) / PASO))
    alto = int(round((y0 - BBOX[1]) / PASO))
    return x0, y0, ancho, alto


def bajar_bloque(img, x0, y0, bf, bc, ancho, alto) -> Path:
    from gee_export_clima_diario import bajar
    dest = BLOQUES / f'fabdem_{bf:02d}_{bc:02d}.npy'
    if dest.exists():
        return dest
    c0, r0 = bc * BLOQUE, bf * BLOQUE
    w, h = min(BLOQUE, ancho - c0), min(BLOQUE, alto - r0)
    g = {'x0': x0 + c0 * PASO, 'dx': PASO, 'y0': y0 - r0 * PASO, 'dy': -PASO, 'ancho': w, 'alto': h}
    r = bajar(img, g)
    a = r['dem'] if r.dtype.names else r
    tmp = dest.with_suffix('.part.npy')
    np.save(tmp, np.asarray(a, dtype=np.int16))
    tmp.replace(dest)
    return dest


def bajar_fabdem() -> np.ndarray:
    import ee
    from gee_export_activity_rasters import authenticate
    authenticate()
    col = ee.ImageCollection('projects/sat-io/open-datasets/FABDEM')
    img = col.mosaic().setDefaultProjection(col.first().projection()).multiply(10).round().toInt16().rename('dem')
    x0, y0, ancho, alto = grilla()
    nf, nc = math.ceil(alto / BLOQUE), math.ceil(ancho / BLOQUE)
    BLOQUES.mkdir(parents=True, exist_ok=True)
    print(f'FABDEM 1": {ancho}×{alto} px, {nf * nc} bloques', flush=True)
    t = time.time()
    with ThreadPoolExecutor(4) as ex:
        futs = {ex.submit(bajar_bloque, img, x0, y0, bf, bc, ancho, alto): (bf, bc)
                for bf in range(nf) for bc in range(nc)}
        for f in as_completed(futs):
            f.result()
            print(f'  bloque {futs[f]} listo ({time.time() - t:.0f} s)', flush=True)
    dem = np.empty((alto, ancho), dtype=np.int16)
    for bf in range(nf):
        for bc in range(nc):
            a = np.load(BLOQUES / f'fabdem_{bf:02d}_{bc:02d}.npy')
            dem[bf * BLOQUE: bf * BLOQUE + a.shape[0], bc * BLOQUE: bc * BLOQUE + a.shape[1]] = a
    return dem


def escribir(path: Path, arr: np.ndarray, nodata=None) -> None:
    import rasterio
    from rasterio.transform import from_origin
    x0, y0, ancho, alto = grilla()
    with rasterio.open(path, 'w', driver='GTiff', height=alto, width=ancho, count=1, dtype=arr.dtype,
                       crs='EPSG:4326', transform=from_origin(x0, y0, PASO, PASO), nodata=nodata,
                       compress='deflate', predictor=3 if arr.dtype.kind == 'f' else 1, tiled=True,
                       BIGTIFF='IF_SAFER') as dst:
        dst.write(arr, 1)


def area_celdas_km2(alto: int, ancho: int) -> np.ndarray:
    _, y0, _, _ = grilla()
    lat = y0 - (np.arange(alto) + 0.5) * PASO
    dy = math.radians(PASO) * R
    dx = math.radians(PASO) * R * np.cos(np.radians(lat))
    return np.broadcast_to(((dx * dy) / 1e6).astype(np.float64)[:, None], (alto, ancho))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--solo-bajar', action='store_true')
    args = ap.parse_args()
    FUENTES.mkdir(parents=True, exist_ok=True)
    p_dem = FUENTES / 'fabdem_misiones.tif'
    t = time.time()
    if not p_dem.exists():
        dem = bajar_fabdem()
        bad = int((dem <= -32000).sum())
        print(f'mosaico: {dem.shape}, sin dato {bad:,}, elevación {dem.min() / 10:.1f}–{dem.max() / 10:.1f} m', flush=True)
        escribir(p_dem, (dem.astype(np.float32) / 10))
        del dem
    if args.solo_bajar:
        return 0

    from pysheds.grid import Grid
    grid = Grid.from_raster(str(p_dem))
    dem = grid.read_raster(str(p_dem))
    print(f'pysheds: {dem.shape} ({time.time() - t:.0f} s)', flush=True)
    filled = grid.fill_pits(dem)
    filled = grid.fill_depressions(filled)
    print(f'  depresiones rellenas ({time.time() - t:.0f} s)', flush=True)
    filled = grid.resolve_flats(filled)
    print(f'  planos resueltos ({time.time() - t:.0f} s)', flush=True)
    del dem
    fdir = grid.flowdir(filled)
    del filled
    print(f'  direcciones ({time.time() - t:.0f} s)', flush=True)
    from pysheds.view import Raster
    w = Raster(np.ascontiguousarray(area_celdas_km2(*fdir.shape)), viewfinder=fdir.viewfinder)
    acc = grid.accumulation(fdir, weights=w)
    print(f'  acumulación ({time.time() - t:.0f} s); máx {float(np.nanmax(acc)):,.0f} km²', flush=True)
    escribir(FUENTES / 'fdir_misiones.tif', np.asarray(fdir).astype(np.uint8))
    escribir(FUENTES / 'acc_km2_misiones.tif', np.asarray(acc).astype(np.float32))
    n_red = int((np.asarray(acc) >= 0.1).sum())
    print(f'listo en {time.time() - t:.0f} s; celdas con ≥ 0,1 km² drenados: {n_red:,}', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
