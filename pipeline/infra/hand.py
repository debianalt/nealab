"""Altura sobre el cauce (HAND) de toda Misiones a 30 m y su resumen por celda h3 (fase 5: sólo datos).

Mismo método que los arroyos de la demo El Soberbio (conicet(1)/04_stan/10_demo_el_nino/04_arroyos.py):
FABDEM 30 m rellenado (fill_pits → fill_depressions → resolve_flats), direcciones D8 de drenaje.py, red = celdas con
≥ A_RED km² drenados; pysheds compute_hand con return_index da, para cada celda, la celda de la red a la que llega
por la pendiente. HAND = cota rellenada − cota de esa celda de red; distancia = recta hasta esa celda.

Salidas:
  %HEXOMA_FUENTES%/infra/hand_misiones.tif       HAND en decímetros (int16; -1 = sin dato)
  %HEXOMA_FUENTES%/infra/dist_cauce_misiones.tif distancia al cauce de destino en metros (uint16, tope 65534)
  infra.hand_h3   por celda h3 de Misiones (las ~120 celdas de 30 m que caen en cada hexágono): n, HAND mín., P10,
                  mediana, % del hexágono con HAND < 1, 2, 3 y 5 m, y el área drenada del cauce de destino de la
                  celda de 30 m más baja del hexágono

Uso (desde neahub/, después de drenaje.py):
    python pipeline/infra/hand.py
"""
from __future__ import annotations

import io
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
import drenaje  # noqa: E402
from build_hexoma import DB, connect, log  # noqa: E402

A_RED = 0.1
R = 6371008.8


def main() -> int:
    import rasterio
    import shapely
    from pysheds.grid import Grid
    from rasterio.features import rasterize
    t0 = time.time()
    p_dem = drenaje.FUENTES / 'fabdem_misiones.tif'
    grid = Grid.from_raster(str(p_dem))
    dem = grid.read_raster(str(p_dem))
    filled = grid.resolve_flats(grid.fill_depressions(grid.fill_pits(dem)))
    del dem
    log(f'FABDEM rellenado ({time.time() - t0:.0f} s)')
    fdir = grid.read_raster(str(drenaje.FUENTES / 'fdir_misiones.tif')).astype(np.int64)
    acc = grid.read_raster(str(drenaje.FUENTES / 'acc_km2_misiones.tif'))
    red = acc >= A_RED
    idx = np.asarray(grid.compute_hand(fdir, filled, red, return_index=True))
    log(f'índice HAND ({time.time() - t0:.0f} s)')
    fl = np.asarray(filled, dtype=np.float32)
    del filled, fdir
    alto, ancho = fl.shape
    plano = fl.ravel()
    iv = idx.ravel()
    ok = iv >= 0
    hand = np.full(plano.shape, np.nan, np.float32)
    hand[ok] = plano[ok] - plano[iv[ok]]
    n_neg = int((hand < -0.01).sum())   # con DEM rellenado no debería haber; se cuenta antes de recortar
    hand = np.maximum(hand, 0)
    # distancia recta a la celda de destino (grilla geográfica de 1")
    with rasterio.open(p_dem) as src:
        tr, perfil = src.transform, src.profile
    fila = np.arange(plano.size) // ancho
    col = np.arange(plano.size) % ancho
    lat = tr.f + (fila + 0.5) * tr.e
    dy = (fila - iv // ancho) * abs(tr.e) * np.pi / 180 * R
    dx = (col - iv % ancho) * tr.a * np.pi / 180 * R * np.cos(np.radians(lat))
    dist = np.where(ok, np.hypot(dx, dy), np.nan).astype(np.float32)
    del dx, dy, lat, fila, col
    accv = np.asarray(acc, dtype=np.float32).ravel()
    area_dest = np.where(ok, accv[np.where(ok, iv, 0)], np.nan).astype(np.float32)
    del accv, acc, red, idx, fl, plano
    log(f'HAND y distancia ({time.time() - t0:.0f} s); celdas con HAND < −0,01 m antes de recortar a 0: {n_neg:,}')

    perfil.update(dtype='int16', nodata=-1, compress='deflate', predictor=2, tiled=True, BIGTIFF='IF_SAFER')
    with rasterio.open(drenaje.FUENTES / 'hand_misiones.tif', 'w', **perfil) as dst:
        dst.write(np.where(np.isnan(hand), -1, np.minimum(np.round(hand * 10), 32766)).astype(np.int16).reshape(alto, ancho), 1)
    perfil.update(dtype='uint16', nodata=65535)
    with rasterio.open(drenaje.FUENTES / 'dist_cauce_misiones.tif', 'w', **perfil) as dst:
        dst.write(np.where(np.isnan(dist), 65535, np.minimum(np.round(dist), 65534)).astype(np.uint16).reshape(alto, ancho), 1)
    log(f'rásteres escritos ({time.time() - t0:.0f} s)')

    # resumen por celda h3 de Misiones: rasterizar los hexágonos con su índice
    conn = connect(DB)
    hx = pd.read_sql("""SELECT h3::text h3, ST_AsBinary(h3_cell_to_boundary_geometry(h3)) wkb
                        FROM hexagonos WHERE territorio_id = 1""", conn)
    geoms = shapely.from_wkb(hx.wkb.map(bytes).values)
    etiqueta = rasterize(((g, i + 1) for i, g in enumerate(geoms)), out_shape=(alto, ancho), transform=tr,
                         fill=0, dtype='int32').ravel()
    log(f'hexágonos rasterizados: {len(hx):,} ({time.time() - t0:.0f} s)')
    m = (etiqueta > 0) & ~np.isnan(hand)
    d = pd.DataFrame({'e': etiqueta[m], 'hand': hand[m], 'area': area_dest[m]})
    del etiqueta, hand, dist, area_dest
    g = d.groupby('e').hand
    res = pd.DataFrame({'n': g.size(), 'hand_min': g.min(), 'hand_p10': g.quantile(.1), 'hand_mediana': g.median()})
    for u in (1, 2, 3, 5):
        res[f'pct_lt_{u}m'] = (d.hand < u).groupby(d.e).mean() * 100
    res['area_cauce_km2'] = d.loc[g.idxmin().values, 'area'].values
    res.index = hx.h3.values[res.index - 1]
    res = res.reset_index(names='h3')
    log(f'resumen: {len(res):,} celdas con HAND ({time.time() - t0:.0f} s)')

    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS infra.hand_h3')
    cur.execute("""CREATE TABLE infra.hand_h3 (h3 h3index PRIMARY KEY, n int, hand_min real, hand_p10 real,
                   hand_mediana real, pct_lt_1m real, pct_lt_2m real, pct_lt_3m real, pct_lt_5m real, area_cauce_km2 real)""")
    buf = io.StringIO()
    res.round(3).to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert("COPY infra.hand_h3 FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute("""COMMENT ON TABLE infra.hand_h3 IS 'Altura sobre el cauce (HAND, FABDEM 30 m, red ≥ 0,1 km²) resumida por
                   celda h3 de Misiones: mínimo, P10, mediana (m), % del hexágono con HAND < 1/2/3/5 m y área drenada del
                   cauce de destino del punto más bajo (hand.py). FABDEM no ve terraplenes ni desagües.'""")
    cur.execute("""INSERT INTO infra.fuentes VALUES ('infra.hand_h3', 'Derivado: FABDEM 30 m + pysheds compute_hand',
                   'hand.py', current_date::text, now(), %s, %s) ON CONFLICT (tabla) DO UPDATE SET
                   cargado=now(), n=EXCLUDED.n, nota=EXCLUDED.nota""", (len(res), f'red ≥ 0,1 km²; celdas negativas recortadas: {n_neg}'))
    conn.commit()
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s')
    return 0


if __name__ == '__main__':
    sys.exit(main())
