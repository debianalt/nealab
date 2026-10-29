"""
Department ranking for the flood layer: exposure by terrain (HAND < 5 m).

HAND (Height Above Nearest Drainage, MERIT Hydro v1.0.1, 3″ ≈ 90 m) is the
height of each point over the nearest river or stream it drains to; < 5 m is
the usual floodplain screen (Nobre et al. 2011, Rennó et al. 2008). Per
department:
  bld_hand5_pct  = % of buildings on pixels at HAND < 5 m   ← ranking
  bld_hand5_n    = number of those buildings
  land_hand5_pct = % of land (permanent water excluded) at HAND < 5 m

One method for every territory, all inputs in GEE, one 90 m export on the
MERIT grid with four area/count bands:
  land_m2     = pixel area without JRC permanent water today (transition 1
                permanent, 2 new permanent, 7 seasonal→permanent): rivers have
                HAND 0 and would otherwise dominate the land share
  low_land_m2 = land_m2 where HAND < 5 m
  bld         = buildings, Google Open Buildings Temporal v1 (2023),
                fractional count density × pixel area. At 90 m this matches the
                0.5 m native sum within 0.4 % (Posadas–Encarnación: 379,615 vs
                377,938)
  bld_low     = bld where HAND < 5 m
Pixels go to H3 res-9 cells (pixel centre) and cells to departments through
the dept parquets the CDN serves, so the ranking uses the UI's own split.

Why not JRC occurrence: in Misiones it ranks river and reservoir shores, not
floods — "% of land with occurrence > 20 %" put Capital first on the Yacyretá
lake; without permanent water and reservoirs every department fell under
0.4 % with Iguazú first on the Urugua-í shoreline, and "ever saw water" added
almost nothing: Landsat misses the short, cloudy floods of an incised plateau.

Writes a `rank` block into each department of the flood dept summary JSON;
split_flood_by_dpto.py keeps it when it rewrites the summary.

    python pipeline/build_flood_rank.py --export --all     # launch GEE exports, wait, download
    python pipeline/build_flood_rank.py --all              # tifs → JSON
    python pipeline/build_flood_rank.py --territory corrientes
"""

import argparse
import glob
import json
import os
import subprocess
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import GCS_BUCKET, EXPORT_PREFIX, OUTPUT_DIR, TERRITORY_CONFIGS, get_territory

SRC_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data')
CDN = 'https://cdn.spatia.ar/data'
HAND_CUT = 5            # metres
PERMANENT = [1, 2, 7]   # JRC transition classes that are permanent water today
OB_EPOCH = 1688108400   # Open Buildings Temporal: 2023 inference
BANDS = ['land_m2', 'low_land_m2', 'bld', 'bld_low']
NAME = 'flood_rank_hand'
SATURATED_MEDIAN = 80   # % — at or above, HAND < 5 m does not separate departments


def territory_paths(tid: str):
    prefix = get_territory(tid)['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, prefix.rstrip('/')) if prefix else OUTPUT_DIR
    summary = 'flood_dept_summary.json' if tid == 'misiones' else f'{tid}_flood_dept_summary.json'
    return prefix, t_dir, os.path.join(SRC_DATA_DIR, summary)


def launch(tid: str):
    import ee
    from gee_flood_detection import get_territory_aoi
    aoi = get_territory_aoi(tid)
    merit = ee.Image('MERIT/Hydro/v1_0_1').select('hnd')
    proj = merit.projection()
    low = merit.lt(HAND_CUT)
    perm = (ee.Image('JRC/GSW1_4/GlobalSurfaceWater').select('transition')
            .remap(PERMANENT, [1] * len(PERMANENT), 0).unmask(0))
    land = ee.Image.pixelArea().multiply(perm.Not())
    dens = (ee.ImageCollection('GOOGLE/Research/open-buildings-temporal/v1')
            .filter(ee.Filter.eq('inference_time_epoch_s', OB_EPOCH)).mosaic()
            .select('building_fractional_count').unmask(0).divide(0.25))  # per m² (0.5 m px)
    bld = dens.multiply(ee.Image.pixelArea())
    img = ee.Image.cat([
        land.rename('land_m2'), land.multiply(low).rename('low_land_m2'),
        bld.rename('bld'), bld.multiply(low).rename('bld_low'),
    ]).reproject(proj).toFloat()
    task = ee.batch.Export.image.toCloudStorage(
        image=img, description=f'{NAME}_{tid}', bucket=GCS_BUCKET,
        fileNamePrefix=f'{EXPORT_PREFIX}/{tid}/{NAME}', region=aoi,
        crs='EPSG:4326', crsTransform=proj.getInfo()['transform'],
        maxPixels=1e11, fileFormat='GeoTIFF',
    )
    task.start()
    print(f'  -> {tid}: {task.id}')
    return task


def download(tid: str, t_dir: str) -> None:
    os.makedirs(t_dir, exist_ok=True)
    for old in glob.glob(os.path.join(t_dir, f'{NAME}*.tif')):
        os.remove(old)
    src = f'gs://{GCS_BUCKET}/{EXPORT_PREFIX}/{tid}/{NAME}*.tif'
    subprocess.run(['gcloud', 'storage', 'cp', src, t_dir], check=True, shell=(os.name == 'nt'))


def hex_sums(tifs: list) -> pd.DataFrame:
    """Sum the four bands per H3 res-9 cell (pixel centre), block by block."""
    import duckdb
    import rasterio
    con = duckdb.connect()
    con.execute('INSTALL h3 FROM community; LOAD h3;')
    parts = []
    for tif in tifs:
        with rasterio.open(tif) as src:
            for _, win in src.block_windows(1):
                a = src.read(window=win).astype(np.float64)        # (4, h, w)
                keep = (a[0] > 0) | (a[2] > 0)
                if not keep.any():
                    continue
                rows, cols = np.nonzero(keep)
                t = src.window_transform(win)
                df = pd.DataFrame({
                    'lng': t.c + (cols + 0.5) * t.a, 'lat': t.f + (rows + 0.5) * t.e,
                    **{b: a[i][keep] for i, b in enumerate(BANDS)},
                })
                parts.append(con.execute(
                    'SELECT h3_latlng_to_cell_string(lat, lng, 9) AS h3index, '
                    + ', '.join(f'SUM({b}) AS {b}' for b in BANDS)
                    + ' FROM df GROUP BY 1').df())
    return pd.concat(parts).groupby('h3index', as_index=False)[BANDS].sum()


def dept_assignment(prefix: str, departments: list) -> pd.DataFrame:
    """h3index → parquetKey from the dept parquets the CDN serves (same split the UI loads)."""
    import duckdb
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    urls = {f"{CDN}/{prefix}flood_dpto/hex_flood_{d['parquetKey']}.parquet": d['parquetKey'] for d in departments}
    url_list = ', '.join(f"'{u}'" for u in urls)
    df = con.execute(f'SELECT h3index, filename FROM read_parquet([{url_list}], filename=true)').df()
    df['parquetKey'] = df['filename'].map(urls)
    return df[['h3index', 'parquetKey']].drop_duplicates('h3index')


def process(tid: str) -> None:
    prefix, t_dir, summary_path = territory_paths(tid)
    tifs = sorted(glob.glob(os.path.join(t_dir, f'{NAME}*.tif')))
    if not tifs:
        sys.exit(f'{tid}: no {NAME}*.tif in {t_dir} — run with --export')
    with open(summary_path, encoding='utf-8') as f:
        summary = json.load(f)

    hexes = hex_sums(tifs).merge(dept_assignment(prefix, summary['departments']), on='h3index', how='inner')
    by = hexes.groupby('parquetKey')[BANDS].sum()
    for d in summary['departments']:
        k = d['parquetKey']
        if k not in by.index:
            d['rank'] = None
            continue
        r = by.loc[k]
        d['rank'] = {
            'bld_hand5_pct': round(100 * r.bld_low / r.bld, 1) if r.bld >= 1 else None,
            'bld_hand5_n': int(round(r.bld_low)),
            'bld_total': int(round(r.bld)),
            'land_hand5_pct': round(100 * r.low_land_m2 / r.land_m2, 1) if r.land_m2 > 0 else None,
        }
    # On a plain nearly everything is < 5 m above some channel: Chaco, Boquerón,
    # Formosa, Ñeembucú, Pdte. Hayes and Alto Paraguay gave medians of 85–100 %
    # (Boquerón, the dry Chaco, 99.6 %). There the bars would all read ~100 % and
    # rank noise, so the UI hides them and says why.
    vals = sorted(d['rank']['bld_hand5_pct'] for d in summary['departments']
                  if d.get('rank') and d['rank']['bld_hand5_pct'] is not None)
    median = vals[len(vals) // 2] if vals else None
    summary.setdefault('metadata', {})['rank'] = {
        'metric': 'bld_hand5_pct',
        'definition': f'% de edificios (Google Open Buildings 2023) y de la tierra (sin agua permanente JRC) '
                      f'a menos de {HAND_CUT} m sobre el curso de agua más cercano (HAND, MERIT Hydro v1.0.1)',
        'median': median,
        'saturated': median is not None and median >= SATURATED_MEDIAN,
    }
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    ranked = sorted((d for d in summary['departments'] if d.get('rank') and d['rank']['bld_hand5_pct'] is not None),
                    key=lambda d: -d['rank']['bld_hand5_pct'])
    missing = sum(1 for d in summary['departments'] if not d.get('rank'))
    top = ', '.join(f"{d['dpto']} {d['rank']['bld_hand5_pct']}%" for d in ranked[:3])
    print(f'{tid}: {len(ranked)}/{len(summary["departments"])} ranked, {missing} without data · top: {top}')


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--territory', default='misiones')
    ap.add_argument('--all', action='store_true')
    ap.add_argument('--export', action='store_true', help='Launch the GEE exports, wait and download first')
    args = ap.parse_args()
    tids = list(TERRITORY_CONFIGS) if args.all else [args.territory]

    if args.export:
        from gee_flood_detection import authenticate, wait_for_tasks
        authenticate()
        tasks = [launch(t) for t in tids]
        wait_for_tasks(tasks, timeout=6 * 3600)
        for t in tids:
            download(t, territory_paths(t)[1])
    for t in tids:
        process(t)


if __name__ == '__main__':
    main()
