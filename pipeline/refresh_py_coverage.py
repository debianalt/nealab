"""
Re-run the Paraguay layers whose rasters were exported with the original, narrow
bbox (3-nov-2026). The bbox in config.py was widened on 7-jun-2026 but only flood
was re-exported: 1.7–41 % of the grid of 12 departments fell outside treecover2000,
PM2.5 and the SDM covariates. Deforestation and PM2.5 had no cells there; forestry
aptitude scored them with zero climate (gdd 0, precipitation 0).

Per territory (3 in parallel), once its GEE exports are COMPLETED:
  1. download satellite/<t>/{hansen_*, sat_pm25_*, sdm_*, mapbiomas_*}.tif with
     gcs_staging (deleted from the bucket after download); shards are mosaicked
  2. check every raster covers the crosswalk grid (stops otherwise)
  3. process_hansen_to_h3; process_pm25_annual_to_h3 +
     compute_pm25_drivers; process_sdm_covariates_h3 +
     compute_forestry_sdm
  4. split_by_admin --only deforestation_dynamics,pm25_drivers,forestry_aptitude
Nothing is uploaded here.

Launch the exports first, one territory after the other (the GEE queue ran one
task at a time, in order, so territories complete progressively):
gee_export_hansen_loss.py --only hansen_treecover2000 --gcs --no-wait,
gee_export_pm25_annual.py --stack --gcs --years 1998..2022, gee_export_sdm_covariates.py.
lossyear already had the full bbox (v1.13 refresh); land use is Dynamic World. Then:

    python pipeline/refresh_py_coverage.py --only amambay_py,canindeyu_py
"""
import argparse
import glob
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, GCS_BUCKET
from gcs_staging import download_and_delete

T12 = ['cordillera_py', 'guaira_py', 'caaguazu_py', 'caazapa_py', 'misiones_py', 'central_py',
       'neembucu_py', 'amambay_py', 'canindeyu_py', 'presidente_hayes_py', 'boqueron_py',
       'alto_paraguay_py']
PREFIXES = ('hansen_', 'sat_pm25_', 'sdm_', 'mapbiomas_')
EE_PROJECT = 'amiable-reducer-398015'


def wait_tasks(t: str, since_ms: float, log) -> bool:
    import ee
    while True:
        # CANCELLED = replaced by a resubmission (3-nov-2026: the queue was re-launched leaner)
        mine = [x for x in ee.data.getTaskList()
                if x.get('creation_timestamp_ms', 0) >= since_ms and t in x.get('description', '')
                and x['state'] != 'CANCELLED']
        states = {}
        for x in mine:
            states[x['state']] = states.get(x['state'], 0) + 1
        log.write(f"{time.strftime('%H:%M')} {t}: {states}\n"); log.flush()
        failed = [x for x in mine if x['state'] == 'FAILED']
        if failed:
            log.write(f"FAILED: {[(x['description'], x.get('error_message')) for x in failed]}\n")
            return False
        if mine and all(x['state'] == 'COMPLETED' for x in mine):
            return True
        time.sleep(120)


def download(t: str, since_ms: float, log) -> None:
    from google.cloud import storage
    import rasterio
    from rasterio.merge import merge
    bucket = storage.Client(project=EE_PROJECT).bucket(GCS_BUCKET)
    blobs = [b for b in bucket.list_blobs(prefix=f'satellite/{t}/')
             if os.path.basename(b.name).startswith(PREFIXES) and b.name.endswith('.tif')
             and b.updated.timestamp() * 1000 >= since_ms]   # only this export, not leftovers
    groups = {}
    for b in blobs:
        base = os.path.basename(b.name)[:-4]
        stem = base.split('-')[0] if '-' in base else base   # GEE shards: name-0000000000-0000000000
        groups.setdefault(stem, []).append(b)
    t_dir = os.path.join(OUTPUT_DIR, t)
    for stem, bs in groups.items():
        dest = os.path.join(t_dir, f'{stem}.tif')
        if len(bs) == 1:
            download_and_delete(bs[0], dest + '.part')
            os.replace(dest + '.part', dest)
        else:
            parts = [download_and_delete(b, os.path.join(t_dir, '_shards', os.path.basename(b.name))) for b in bs]
            srcs = [rasterio.open(p) for p in parts]
            arr, tr = merge(srcs)
            prof = srcs[0].profile
            prof.update(height=arr.shape[1], width=arr.shape[2], transform=tr, compress='deflate', tiled=True,
                        BIGTIFF='IF_SAFER')
            with rasterio.open(dest + '.part', 'w', **prof) as dst:
                dst.write(arr)
            for s in srcs:
                s.close()
            os.replace(dest + '.part', dest)
            for p in parts:
                os.remove(p)
        log.write(f'  downloaded {stem} ({len(bs)} file(s))\n'); log.flush()
    stack = os.path.join(t_dir, 'sat_pm25_stack.tif')
    if os.path.exists(stack):
        split_pm25_stack(stack, log)


def split_pm25_stack(stack: str, log) -> None:
    """sat_pm25_stack.tif (band y<YYYY> per year) -> sat_pm25_<YYYY>.tif, then delete it:
    process_pm25_annual_to_h3.py globs sat_pm25_*.tif and reads the year from the name."""
    import rasterio
    t_dir = os.path.dirname(stack)
    with rasterio.open(stack) as src:
        prof = src.profile
        prof.update(count=1, compress='deflate')
        names = src.descriptions
        for i, name in enumerate(names, start=1):
            year = int(name.lstrip('y'))
            with rasterio.open(os.path.join(t_dir, f'sat_pm25_{year}.tif'), 'w', **prof) as dst:
                dst.write(src.read(i), 1)
    os.remove(stack)
    log.write(f'  split sat_pm25_stack into {len(names)} years\n'); log.flush()


def check_cover(t: str, log) -> bool:
    import h3
    import numpy as np
    import pandas as pd
    import rasterio
    from rasterio.warp import transform_bounds
    t_dir = os.path.join(OUTPUT_DIR, t)
    cw = pd.read_parquet(os.path.join(t_dir, 'h3_admin_crosswalk.parquet'), columns=['h3index'])
    ll = np.array([h3.cell_to_latlng(c) for c in cw.h3index[::10]])
    ok = True
    for p in sorted(glob.glob(os.path.join(t_dir, '*.tif'))):
        if not os.path.basename(p).startswith(PREFIXES):
            continue
        with rasterio.open(p) as r:
            b = transform_bounds(r.crs, 'EPSG:4326', *r.bounds) if r.crs and r.crs.to_epsg() != 4326 else r.bounds
        out = ((ll[:, 1] < b[0]) | (ll[:, 1] > b[2]) | (ll[:, 0] < b[1]) | (ll[:, 0] > b[3])).mean()
        if out > 0.001:
            log.write(f'  NOT COVERED {os.path.basename(p)}: {100 * out:.1f} % of the grid outside\n')
            ok = False
    return ok


def run(cmd: list, log) -> bool:
    log.write(f"\n$ {' '.join(cmd)}\n"); log.flush()
    env = {**os.environ, 'PYTHONUNBUFFERED': '1', 'PYTHONIOENCODING': 'utf-8'}
    return subprocess.run(cmd, cwd=SCRIPT_DIR, stdout=log, stderr=subprocess.STDOUT, env=env).returncode == 0


def one(t: str, since_ms: float, skip_gee: bool) -> tuple:
    py = sys.executable
    with open(os.path.join(OUTPUT_DIR, f'py_coverage_{t}.log'), 'w', encoding='utf-8') as log:
        if not skip_gee:
            if not wait_tasks(t, since_ms, log):
                return t, 'GEE failed'
            download(t, since_ms, log)
        if not check_cover(t, log):
            return t, 'rasters do not cover the grid'
        steps = [
            [py, 'process_hansen_to_h3.py', '--territory', t],
            [py, 'process_pm25_annual_to_h3.py', '--territory', t],
            [py, 'compute_pm25_drivers.py', '--territory', t],
            [py, 'process_sdm_covariates_h3.py', '--territory', t],
            [py, 'compute_forestry_sdm.py', '--territory', t, '--no-diagnostics'],
            [py, 'split_by_admin.py', '--territory', t, '--only', 'deforestation_dynamics,pm25_drivers,forestry_aptitude'],
        ]
        for cmd in steps:
            if not run(cmd, log):
                return t, f'failed: {cmd[1]}'
    return t, 'OK'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--only', default=None)
    ap.add_argument('--workers', type=int, default=3)
    ap.add_argument('--since-hours', type=float, default=12, help='GEE tasks created in the last N hours')
    ap.add_argument('--skip-gee', action='store_true', help='rasters already downloaded')
    a = ap.parse_args()
    ts = a.only.split(',') if a.only else T12
    if not a.skip_gee:
        import ee
        ee.Initialize(project=EE_PROJECT)
    since = time.time() * 1000 - a.since_hours * 3600 * 1000
    t0 = time.time()
    res = {}
    with ThreadPoolExecutor(a.workers) as ex:
        futs = [ex.submit(one, t, since, a.skip_gee) for t in ts]
        for f in as_completed(futs):
            t, msg = f.result()
            res[t] = msg
            print(f'  {msg:30s} {t} ({time.time() - t0:.0f}s)', flush=True)
    bad = {t: m for t, m in res.items() if m != 'OK'}
    print(f'Done {len(ts) - len(bad)}/{len(ts)}; problems: {bad} (logs: output/py_coverage_<t>.log)')
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
