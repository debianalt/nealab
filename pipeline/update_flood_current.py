"""
Refresh the flood layer's current extent (Sentinel-1) for the NEA territories.

Only `flood_extent_pct` changes between runs: JRC occurrence / recurrence /
seasonality are a 1984–2021 archive and the department ranking is terrain and
radar history. So this does not rebuild the layer — it takes the parquets the
CDN serves, replaces that one column (and the composite score that uses it),
and uploads them back under the same keys:

  data/<prefix>hex_flood_risk.parquet               (territory)
  data/<prefix>flood_dpto/hex_flood_<key>.parquet   (each department, what the UI loads)
  data/flood_meta.json                              (SAR date per territory, read at runtime)

Extent = % of the H3 res-9 cell with water in the median of the last 12 days of
Sentinel-1 (VV < −15 dB, descending), computed at 30 m and averaged to 90 m in
GEE, then pixel centres → H3 cells (DuckDB h3). Cells the radar did not cover
in the window keep their previous value; the run fails before uploading
anything if coverage is too low or the result looks wrong.

Replaces `run_flood_update.py --current` in CI, which lost the department
split (no radios parquet in the runner) and could not reach the 3 other NEA
provinces. run_flood_update.py stays for full local rebuilds.

    python pipeline/update_flood_current.py                 # 4 NEA territories
    python pipeline/update_flood_current.py --territory chaco --dry-run
"""

import argparse
import glob
import json
import os
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import GCS_BUCKET, EXPORT_PREFIX, OUTPUT_DIR, get_territory

CDN = 'https://cdn.spatia.ar/data'
SRC_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data')
NEA = ['misiones', 'corrientes', 'chaco', 'formosa']
DAYS = 12
NAME = 'flood_current_frac'
SHORT_CACHE = 'public, max-age=3600'   # refreshed in place under a fixed ?v URL
MIN_COVERED = 0.90                     # share of cells the radar must cover
META_KEY = 'data/flood_meta.json'


def paths(tid: str):
    prefix = get_territory(tid)['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, prefix.rstrip('/')) if prefix else OUTPUT_DIR
    summary = 'flood_dept_summary.json' if tid == 'misiones' else f'{tid}_flood_dept_summary.json'
    return prefix, t_dir, os.path.join(SRC_DATA_DIR, summary)


def launch(tid: str):
    """Export the 90 m water fraction (−1 = no radar in the window); return (task, ISO date of the latest image)."""
    import ee
    from gee_flood_detection import get_territory_aoi, get_s1_collection, compute_water_mask
    aoi = get_territory_aoi(tid)
    end = datetime.now(timezone.utc)
    col = get_s1_collection(aoi, (end - timedelta(days=DAYS)).strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
    latest = ee.Date(col.aggregate_max('system:time_start')).format('YYYY-MM-dd').getInfo()
    proj = ee.Image('MERIT/Hydro/v1_0_1').select('hnd').projection()
    water = (compute_water_mask(col.median()).toFloat()
             .setDefaultProjection(crs='EPSG:4326', scale=30)
             .reduceResolution(ee.Reducer.mean(), maxPixels=64)
             .reproject(proj).unmask(-1).rename('water_frac'))
    task = ee.batch.Export.image.toCloudStorage(
        image=water, description=f'{NAME}_{tid}', bucket=GCS_BUCKET,
        fileNamePrefix=f'{EXPORT_PREFIX}/{tid}/{NAME}', region=aoi, crs='EPSG:4326',
        crsTransform=proj.getInfo()['transform'], maxPixels=1e11, fileFormat='GeoTIFF',
    )
    task.start()
    print(f'  -> {tid}: {task.id} (latest S1 image {latest})', flush=True)
    return task, latest


def download(tid: str, t_dir: str) -> list:
    """GCS → t_dir with the GEE service account (same client CI already uses)."""
    from download_gcs import get_gcs_client
    os.makedirs(t_dir, exist_ok=True)
    for old in glob.glob(os.path.join(t_dir, f'{NAME}*.tif')):
        os.remove(old)
    bucket = get_gcs_client().bucket(GCS_BUCKET)
    for blob in bucket.list_blobs(prefix=f'{EXPORT_PREFIX}/{tid}/{NAME}'):
        if blob.name.endswith('.tif'):
            blob.download_to_filename(os.path.join(t_dir, os.path.basename(blob.name)))
    return sorted(glob.glob(os.path.join(t_dir, f'{NAME}*.tif')))


def extent_by_cell(tifs: list) -> pd.DataFrame:
    """Mean water fraction per H3 res-9 cell over pixels with radar (value ≥ 0)."""
    import duckdb
    import rasterio
    con = duckdb.connect()
    con.execute('INSTALL h3 FROM community; LOAD h3;')
    parts = []
    for tif in tifs:
        with rasterio.open(tif) as src:
            for _, win in src.block_windows(1):
                a = src.read(1, window=win)
                rows, cols = np.nonzero(a >= 0)
                if not len(rows):
                    continue
                t = src.window_transform(win)
                df = pd.DataFrame({'lng': t.c + (cols + 0.5) * t.a, 'lat': t.f + (rows + 0.5) * t.e,
                                   'w': a[rows, cols].astype(np.float64)})
                parts.append(con.execute('SELECT h3_latlng_to_cell_string(lat, lng, 9) AS h3index, '
                                         'SUM(w) AS s, COUNT(*) AS n FROM df GROUP BY 1').df())
    g = pd.concat(parts).groupby('h3index', as_index=False)[['s', 'n']].sum()
    g['extent'] = (100 * g.s / g.n).round(2)
    return g[['h3index', 'extent']]


def read_served(url: str) -> pd.DataFrame:
    import duckdb
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    return con.execute(f"SELECT * FROM read_parquet('{url}?refresh={int(datetime.now().timestamp())}')").df()


def refresh(df: pd.DataFrame, extent: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    """New flood_extent_pct where the radar saw the cell, previous value elsewhere; rescore."""
    from process_to_h3 import compute_flood_risk_score
    out = df.merge(extent, on='h3index', how='left')
    covered = float(out.extent.notna().mean())
    out['flood_extent_pct'] = out.extent.fillna(out.flood_extent_pct)
    out = out.drop(columns='extent')
    out['flood_risk_score'] = compute_flood_risk_score(
        out.jrc_occurrence.fillna(0), out.jrc_recurrence.fillna(0), out.flood_extent_pct.fillna(0)).values
    return out[df.columns], covered


def check(tid: str, old: pd.DataFrame, new: pd.DataFrame, covered: float) -> None:
    problems = []
    if len(new) != len(old):
        problems.append(f'rows {len(old)} -> {len(new)}')
    if covered < MIN_COVERED:
        problems.append(f'radar covered {covered:.0%} of cells (< {MIN_COVERED:.0%})')
    e = new.flood_extent_pct
    if e.isna().any() or (e < 0).any() or (e > 100).any():
        problems.append('flood_extent_pct outside 0–100 or missing')
    for c in ('jrc_occurrence', 'jrc_recurrence', 'jrc_seasonality'):
        if not old[c].equals(new[c]):
            problems.append(f'{c} changed — must be untouched')
    if problems:
        sys.exit(f'{tid}: refusing to upload — ' + '; '.join(problems))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--territory', choices=NEA, help='One territory (default: all four NEA)')
    ap.add_argument('--skip-gee', action='store_true', help='Reuse the tifs already downloaded')
    ap.add_argument('--dry-run', action='store_true', help='Compute and check, upload nothing')
    args = ap.parse_args()
    tids = [args.territory] if args.territory else NEA

    dates = {}
    if not args.skip_gee:
        from gee_flood_detection import authenticate, wait_for_tasks
        authenticate()
        launched = {t: launch(t) for t in tids}
        dates = {t: d for t, (_, d) in launched.items()}
        wait_for_tasks([task for task, _ in launched.values()], timeout=3 * 3600)

    from parquet_io import write_h3_parquet
    from upload_to_r2 import upload_file
    staged = []   # (local path, r2 key) — uploaded only after every territory passed its checks
    for t in tids:
        prefix, t_dir, summary_path = paths(t)
        tifs = sorted(glob.glob(os.path.join(t_dir, f'{NAME}*.tif'))) if args.skip_gee else download(t, t_dir)
        if not tifs:
            sys.exit(f'{t}: no {NAME}*.tif')
        extent = extent_by_cell(tifs)
        out_dir = os.path.join(t_dir, 'flood_current')
        os.makedirs(os.path.join(out_dir, 'flood_dpto'), exist_ok=True)

        old = read_served(f'{CDN}/{prefix}hex_flood_risk.parquet')
        new, covered = refresh(old, extent)
        check(t, old, new, covered)
        main_path = os.path.join(out_dir, 'hex_flood_risk.parquet')
        write_h3_parquet(new, main_path)
        staged.append((main_path, f'data/{prefix}hex_flood_risk.parquet'))

        with open(summary_path, encoding='utf-8') as f:
            departments = json.load(f)['departments']
        for d in departments:
            key = d['parquetKey']
            old_d = read_served(f"{CDN}/{prefix}flood_dpto/hex_flood_{key}.parquet")
            new_d, _ = refresh(old_d, extent)
            check(f"{t}/{key}", old_d, new_d, 1.0)   # coverage is judged per territory, above
            p = os.path.join(out_dir, 'flood_dpto', f'hex_flood_{key}.parquet')
            write_h3_parquet(new_d, p)
            staged.append((p, f'data/{prefix}flood_dpto/hex_flood_{key}.parquet'))
        print(f'{t}: {len(new):,} cells, radar covered {covered:.1%}, '
              f'mean extent {old.flood_extent_pct.mean():.2f} -> {new.flood_extent_pct.mean():.2f} %, '
              f'{len(departments)} departments', flush=True)

    if args.dry_run:
        print(f'dry run: {len(staged)} files staged, nothing uploaded')
        return

    failed = [k for p, k in staged if not upload_file(p, k, cache_control=SHORT_CACHE)]
    if failed:
        sys.exit(f'{len(failed)} uploads failed: {failed[:5]}')

    try:
        req = urllib.request.Request(f'{CDN}/flood_meta.json?refresh={int(datetime.now().timestamp())}',
                                     headers={'User-Agent': 'spatia-pipeline/1.0'})
        with urllib.request.urlopen(req, timeout=30) as r:
            meta = json.load(r)
    except Exception:
        meta = {}
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    for t in tids:
        meta[t] = {'sar_date': dates.get(t) or meta.get(t, {}).get('sar_date'), 'window_days': DAYS, 'processed': today}
    meta_path = os.path.join(OUTPUT_DIR, 'flood_meta.json')
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    if not upload_file(meta_path, META_KEY, cache_control='public, max-age=600'):
        sys.exit('flood_meta.json upload failed')
    print(f'done: {len(staged)} parquets + flood_meta.json')


if __name__ == '__main__':
    main()
