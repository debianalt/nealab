"""
Add raw viirs_mean_radiance + building_density_per_km2 columns to a territory's
sat_economic_activity.parquet at H3 res-9.

- VIIRS: bilinear sampling of the annual composite raster at hex centroids
  (pixels ~500m >= 250m -> centroid interpolation per the raster-to-H3 rule,
  same as process_activity_to_h3.py; polygon masking would imprint the grid).
- Building density: GBA footprint centroids (PostGIS ndvi_misiones.
  gba_buildings_<territory>; plain gba_buildings for Misiones) bucketed to
  H3 res-9, count / cell area km².

For Misiones this REPLACES the previous columns: aggregate_radio_to_h3 had
percentile-ranked them 0-100 while config declared physical units — the values
on display were scores, not raw measurements.

Usage:
  python pipeline/compute_viirs_buildings_h3.py --territory corrientes
  python pipeline/compute_viirs_buildings_h3.py --territory misiones
"""
import argparse
import os
import sys
import time

import h3
import numpy as np
import pandas as pd
import psycopg2
import rasterio
from scipy.ndimage import map_coordinates

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, get_territory

PG = dict(dbname='ndvi_misiones', user='postgres', host='localhost')
H3_RES = 9


def sample_raster_bilinear(raster_path: str, lngs: np.ndarray, lats: np.ndarray) -> np.ndarray:
    with rasterio.open(raster_path) as src:
        band = src.read(1).astype(np.float64)
        nodata = src.nodata
        if nodata is not None:
            band[band == nodata] = np.nan
        inv = ~src.transform
        cols, rows = inv * (lngs, lats)
        vals = map_coordinates(band, [rows, cols], order=1, mode='constant', cval=np.nan)
    return vals


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--territory', required=True)
    ap.add_argument('--year', type=int, default=2024)
    ap.add_argument('--skip-viirs', action='store_true',
                    help='keep existing viirs_mean_radiance; only recompute '
                         'building_density (use when the VIIRS tif is absent).')
    args = ap.parse_args()
    t = args.territory
    get_territory(t)  # validate

    # Misiones keeps the legacy flat layout and the unsuffixed PostGIS table.
    t_dir = OUTPUT_DIR if t == 'misiones' else os.path.join(OUTPUT_DIR, t)
    pq_path = os.path.join(t_dir, 'sat_economic_activity.parquet')
    tif_path = os.path.join(t_dir, f'viirs_{t}_{args.year}.tif')
    required = (pq_path,) if args.skip_viirs else (pq_path, tif_path)
    for p in required:
        if not os.path.exists(p):
            print(f'ERROR: missing {p}')
            return 1

    df = pd.read_parquet(pq_path)
    print(f'[{t}] {len(df):,} hexes in sat_economic_activity')

    # ── VIIRS at centroids ───────────────────────────────────────────────
    if args.skip_viirs:
        if 'viirs_mean_radiance' not in df.columns:
            print('ERROR: --skip-viirs but parquet has no viirs_mean_radiance column')
            return 1
        print(f'[{t}] skip-viirs: keeping existing viirs_mean_radiance')
    else:
        t0 = time.time()
        latlngs = np.array([h3.cell_to_latlng(ix) for ix in df['h3index']])
        viirs = sample_raster_bilinear(tif_path, latlngs[:, 1], latlngs[:, 0])
        df['viirs_mean_radiance'] = np.round(np.nan_to_num(viirs, nan=0.0), 4)
        print(f'[{t}] viirs sampled in {time.time()-t0:.0f}s '
              f'(mean={np.nanmean(viirs):.3f}, max={np.nanmax(viirs):.1f} nW/cm²/sr)')

    # ── Building density ─────────────────────────────────────────────────
    # Footprint sources: GBA covers most of each territory, but in Misiones the
    # eastern departments (Guaraní, eastern Cainguás/San Pedro) are absent from
    # gba_buildings and are covered by the complementary vida_buildings table.
    # build_ar_buildings / rebuild_buildings_tiles already UNION both for the map
    # layer; reading only gba_buildings here left those hexes at 0 edif/km² while
    # the map showed buildings. Mirror the tile builder: union both for Misiones.
    t0 = time.time()
    if t == 'misiones':
        sources = [
            'SELECT ST_X(centroid), ST_Y(centroid) FROM gba_buildings',
            'SELECT ST_X(ST_Centroid(geom)), ST_Y(ST_Centroid(geom)) FROM vida_buildings',
        ]
    else:
        sources = [f'SELECT ST_X(centroid), ST_Y(centroid) FROM gba_buildings_{t}']
    con = psycopg2.connect(**PG)
    counts: dict[str, int] = {}
    n = 0
    for si, src in enumerate(sources):
        cur = con.cursor(f'bld_stream_{si}')  # server-side cursor: millions of rows
        cur.itersize = 200_000
        cur.execute(src)
        for x, y in cur:
            ix = h3.latlng_to_cell(y, x, H3_RES)
            counts[ix] = counts.get(ix, 0) + 1
            n += 1
        cur.close()
    con.close()
    print(f'[{t}] {n:,} buildings bucketed to {len(counts):,} hexes in {time.time()-t0:.0f}s')

    dens = np.zeros(len(df))
    for i, ix in enumerate(df['h3index']):
        c = counts.get(ix)
        if c:
            dens[i] = c / h3.cell_area(ix, unit='km^2')
    df['building_density_per_km2'] = np.round(dens, 2)
    nz = (dens > 0).sum()
    print(f'[{t}] density: {nz:,} hexes >0 (mean of nonzero={dens[dens>0].mean():.1f} edif/km²)')

    # ── Raw counts (edificios/hogares) from the dasymetric crosswalk ──────
    # Same source as the census layers so per-hex Edificios/Hogares match across
    # the whole platform. Hexes outside the crosswalk (no buildings) → 0.
    cw_path = (os.path.join(OUTPUT_DIR, 'h3_radio_crosswalk.parquet') if t == 'misiones'
               else os.path.join(t_dir, f'h3_radio_crosswalk_{t}.parquet'))
    df = df.drop(columns=[c for c in ('n_edificios', 'hogares_estimados') if c in df.columns])
    if os.path.exists(cw_path):
        cw = pd.read_parquet(cw_path)
        if {'n_buildings', 'est_hogares'}.issubset(cw.columns):
            mag = (cw.groupby('h3index')
                     .agg(n_edificios=('n_buildings', 'sum'),
                          hogares_estimados=('est_hogares', 'sum'))
                     .reset_index())
            df = df.merge(mag, on='h3index', how='left')
            df['n_edificios'] = df['n_edificios'].fillna(0).astype('int64')
            df['hogares_estimados'] = df['hogares_estimados'].fillna(0).round(0).astype('int64')
            print(f'[{t}] merged n_edificios/hogares_estimados from crosswalk '
                  f'(edif={int(df.n_edificios.sum()):,}, hog={int(df.hogares_estimados.sum()):,})')
        else:
            print(f'[{t}] WARN: crosswalk lacks n_buildings/est_hogares — skipping counts')
    else:
        print(f'[{t}] WARN: crosswalk not found ({cw_path}) — skipping counts')

    df.to_parquet(pq_path, index=False)
    print(f'[{t}] wrote {pq_path} ({os.path.getsize(pq_path)//1024} KB, cols={list(df.columns)})')
    return 0


if __name__ == '__main__':
    sys.exit(main())
