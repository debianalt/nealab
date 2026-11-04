"""Macizos DNDFI que el satélite muestra como talados después de plantar (Hansen GFC v1.13, 30 m, 2001–2025).

El Inventario Nacional de Plantaciones Forestales (DNDFI, versión 23-04-2026) es una foto: un macizo de pino de
1990 puede figurar con 36 años aunque ya se haya cosechado (y replantado). Para cada macizo del NEA argentino este
script cuenta, con máscara poligonal sobre el ráster `lossyear` de Hansen (todos los macizos rasterizados de una vez
sobre la grilla de 30 m; los menores que un píxel toman el píxel de su centro), los píxeles con pérdida de cobertura arbórea en un año ≥ año de plantación + 5
(antes no hay dosel que perder) y desde 2001 (Hansen no cubre antes).

Hansen marca reemplazo del dosel: tala rasa e incendio. No ve raleos ni podas. Por eso la marca se rotula
«pérdida de cobertura después de plantar (cosecha o incendio)», no «cosechado».

Salida: pipeline/output/forestal/cosecha_hansen.parquet, una fila por macizo:
    id, territorio, anio_plant, n_px (píxeles de 30 m dentro), px_perdida (con pérdida en años válidos),
    pct_perdida (% del macizo), anio_perdida (año con más píxeles perdidos), ultimo_anio_perdida,
    perdida_post (bool: pct_perdida ≥ 50)

Uso (desde neahub/ o un worktree):
    python pipeline/forestal/cosecha_hansen.py [--rasters <dir con hansen_lossyear.tif>] [--workers 4]
"""
from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
MACIZOS = ROOT.parent / 'dndfi_inventario' / 'macizos_23_04_2026.geojson'
RASTERS_DEFAULT = Path(r'C:\Users\ant\OneDrive\nealab\neahub\pipeline\output')   # gitignored: viven en el repo principal
OUT = ROOT / 'pipeline' / 'output' / 'forestal'
PROV = {'misiones': 'MISIONES', 'corrientes': 'CORRIENTES', 'chaco': 'CHACO', 'formosa': 'FORMOSA'}
SUBDIR = {'misiones': '', 'corrientes': 'corrientes', 'chaco': 'chaco', 'formosa': 'formosa'}
MARGEN = 5          # años desde la plantación antes de los cuales una pérdida no es cosecha
UMBRAL = 50.0       # % del macizo con pérdida para marcarlo


def territorio(args: tuple[str, str]) -> pd.DataFrame:
    """Rasteriza los macizos (índice de fila) sobre la grilla de Hansen y agrega con numpy: una pasada por provincia
    en vez de leer el ráster polígono por polígono. Macizos más chicos que un píxel toman el píxel de su centro."""
    import pyogrio
    import rasterio
    from rasterio.features import rasterize

    t, rasters = args
    t0 = time.time()
    gdf = pyogrio.read_dataframe(str(MACIZOS), columns=['id', 'anio_plant', 'prov'], where=f"prov = '{PROV[t]}'")
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].reset_index(drop=True)
    ap = pd.to_numeric(gdf.anio_plant, errors='coerce')
    ap = ap.where(ap > 1900)
    # desde qué código de lossyear (1 = 2001) cuenta una pérdida: plantación + MARGEN; sin año, nunca
    desde = np.where(ap.notna(), np.maximum(1, ap.fillna(0) - 2000 + MARGEN), 99).astype(np.int16)
    with rasterio.open(Path(rasters) / SUBDIR[t] / 'hansen_lossyear.tif') as src:
        ly = src.read(1)
        tr, shape = src.transform, (src.height, src.width)
    idx = rasterize(((g, i + 1) for i, g in enumerate(gdf.geometry)), out_shape=shape, transform=tr,
                    fill=0, dtype='int32', all_touched=False)
    n = len(gdf)
    m = idx > 0
    ids, vals = idx[m] - 1, ly[m].astype(np.int16)
    del idx, m
    n_px = np.bincount(ids, minlength=n)
    # centro para los que no tienen píxel
    vacio = np.flatnonzero(n_px == 0)
    extra_ids, extra_vals = [], []
    for i in vacio:
        pt = gdf.geometry.iloc[i].representative_point()
        r, c = rasterio.transform.rowcol(tr, pt.x, pt.y)
        if 0 <= r < shape[0] and 0 <= c < shape[1]:
            extra_ids.append(i); extra_vals.append(int(ly[r, c]))
    if extra_ids:
        ids = np.concatenate([ids, np.array(extra_ids)]); vals = np.concatenate([vals, np.array(extra_vals, np.int16)])
        n_px = np.bincount(ids, minlength=n)
    ok = (vals >= desde[ids]) & (vals >= 1) & (vals <= 25)
    px_perdida = np.bincount(ids[ok], minlength=n)
    # año con más píxeles y último año, por macizo
    oi, ov = ids[ok], vals[ok]
    cnt = pd.DataFrame({'i': oi, 'y': ov}).value_counts().reset_index(name='c')
    moda = cnt.sort_values(['i', 'c'], ascending=[True, False]).drop_duplicates('i').set_index('i').y
    ult = pd.Series(ov).groupby(oi).max() if oi.size else pd.Series(dtype=float)
    df = pd.DataFrame({'id': gdf['id'], 'territorio': t, 'anio_plant': ap, 'n_px': n_px, 'px_perdida': px_perdida})
    df['anio_perdida'] = (moda.reindex(range(n)) + 2000).to_numpy()
    df['ultimo_anio_perdida'] = (ult.reindex(range(n)) + 2000).to_numpy()
    print(f'  {t}: {n:,} macizos ({len(vacio):,} menores que un píxel) en {time.time() - t0:.0f} s', flush=True)
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--rasters', default=str(RASTERS_DEFAULT))
    ap.add_argument('--workers', type=int, default=1)
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    with ProcessPoolExecutor(args.workers) as ex:
        dfs = list(ex.map(territorio, [(t, args.rasters) for t in PROV]))
    df = pd.concat(dfs, ignore_index=True)
    df['pct_perdida'] = np.where(df.n_px > 0, 100 * df.px_perdida / df.n_px.clip(lower=1), np.nan).round(1)
    df['perdida_post'] = df.pct_perdida >= UMBRAL
    for c in ('anio_plant', 'anio_perdida', 'ultimo_anio_perdida'):
        df[c] = df[c].astype('Int16')
    df.to_parquet(OUT / 'cosecha_hansen.parquet', index=False)
    print(f'{len(df):,} macizos; marcados: {int(df.perdida_post.sum()):,} → {OUT / "cosecha_hansen.parquet"}')


if __name__ == '__main__':
    main()
