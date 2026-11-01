"""
Orchestrate the Dynamic World land_use regeneration (the 24 territories).

Per territory (4 parallel workers):
  1. wait for the GEE export task `dw_cover_<territory>_2024` to COMPLETE
     (area fractions; see gee_dynamic_world.py — not the old dw_probs)
  2. download gs://spatia-satellite/satellite/<territory>/dw_cover_<territory>_2024*.tif
     (Misiones: the legacy flat gs://spatia-satellite/dw_cover_2024*.tif)
     (multiple shards are mosaicked into a VRT)
  3. process_dw_to_h3.py --territory <t>   -> output/<t>/sat_land_use.parquet (DW schema)
  4. split_by_admin.py --territory <t> --only land_use
     -> output/<prefix>sat_dpto/sat_land_use_*.parquet + src/lib/data summary JSON

Usage:
  python pipeline/run_dw_land_use.py
  python pipeline/run_dw_land_use.py --territories itapua_py,alto_parana_py
  python pipeline/run_dw_land_use.py --skip-gee   # rasters already downloaded
"""
import argparse
import glob
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))
OUTPUT_DIR = SCRIPT_DIR / 'output'

ALL_TERRITORIES = [
    'misiones', 'itapua_py', 'alto_parana_py', 'corrientes', 'chaco', 'formosa',
    'parana_br', 'santa_catarina_br', 'rio_grande_sul_br',
    'concepcion_py', 'san_pedro_py', 'cordillera_py', 'guaira_py',
    'caaguazu_py', 'caazapa_py', 'misiones_py', 'paraguari_py',
    'central_py', 'neembucu_py', 'amambay_py', 'canindeyu_py',
    'presidente_hayes_py', 'boqueron_py', 'alto_paraguay_py',
]

GCS_PREFIX = 'gs://spatia-satellite/satellite'
YEAR = 2024
PRODUCT = 'dw_cover'
UPLOAD = False


def find_gdalbuildvrt() -> str:
    """gdalbuildvrt from PATH, falling back to the QGIS install."""
    import shutil
    found = shutil.which('gdalbuildvrt')
    if found:
        return found
    qgis = r'C:\Program Files\QGIS 3.40.13\bin\gdalbuildvrt.exe'
    if os.path.exists(qgis):
        return qgis
    raise FileNotFoundError('gdalbuildvrt not found (PATH or QGIS bin)')


def gee_states() -> dict:
    """Map description -> state for dw_probs tasks."""
    import ee
    ee.Initialize()
    out = {}
    for t in ee.data.getTaskList()[:80]:
        d = t.get('description', '')
        if d.startswith(f'{PRODUCT}_'):
            # keep the most recent occurrence only (list is newest-first)
            out.setdefault(d, t.get('state', 'UNKNOWN'))
    return out


def wait_for_gee(territories, poll_secs=60):
    pending = set(territories)
    completed, failed = set(), set()
    while pending:
        states = gee_states()
        for t in sorted(pending):
            st = states.get(f'{PRODUCT}_{t}_{YEAR}', 'MISSING')
            if st == 'COMPLETED':
                completed.add(t)
                print(f'  [GEE done] {t}', flush=True)
            elif st in ('FAILED', 'CANCELLED', 'MISSING'):
                failed.add(t)
                print(f'  [GEE {st}] {t}', flush=True)
        pending -= completed | failed
        if pending:
            print(f'  ...GEE pending: {len(pending)}', flush=True)
            time.sleep(poll_secs)
        # yield completed incrementally
        for t in sorted(completed):
            completed.discard(t)
            yield t, True
    for t in sorted(failed):
        yield t, False


def download(t: str) -> str | None:
    """Download shard(s); return raster path (tif or vrt) or None."""
    flat = t == 'misiones'   # legacy flat layout
    t_dir = OUTPUT_DIR if flat else OUTPUT_DIR / t
    t_dir.mkdir(exist_ok=True)
    stem = f'{PRODUCT}_{YEAR}' if flat else f'{PRODUCT}_{t}_{YEAR}'
    pattern = (f'gs://spatia-satellite/{stem}*.tif' if flat
               else f'{GCS_PREFIX}/{t}/{stem}*.tif')
    r = subprocess.run(['gcloud', 'storage', 'cp', pattern, str(t_dir)],
                       capture_output=True, text=True, shell=(os.name == 'nt'))
    if r.returncode != 0:
        print(f'  [{t}] gcloud cp FAILED: {r.stderr.strip()[:300]}', flush=True)
        return None
    shards = sorted(glob.glob(str(t_dir / f'{stem}*.tif')))
    if not shards:
        print(f'  [{t}] no shards after download', flush=True)
        return None
    if len(shards) == 1:
        return shards[0]
    vrt = str(t_dir / f'{stem}.vrt')
    r = subprocess.run([find_gdalbuildvrt(), vrt, *shards], capture_output=True, text=True)
    if r.returncode != 0:
        print(f'  [{t}] gdalbuildvrt FAILED: {r.stderr.strip()[:300]}', flush=True)
        return None
    print(f'  [{t}] mosaicked {len(shards)} shards -> vrt', flush=True)
    return vrt


def process_territory(t: str) -> bool:
    raster = download(t)
    if not raster:
        return False
    t0 = time.time()
    r = subprocess.run([sys.executable, str(SCRIPT_DIR / 'process_dw_to_h3.py'),
                        '--territory', t, '--input', raster],
                       capture_output=True, text=True, cwd=str(SCRIPT_DIR.parent))
    if r.returncode != 0:
        print(f'  [{t}] process_dw_to_h3 FAILED:\n{r.stdout[-500:]}\n{r.stderr[-500:]}', flush=True)
        return False
    pq = (OUTPUT_DIR if t == 'misiones' else OUTPUT_DIR / t) / 'sat_land_use.parquet'
    if not pq.exists():
        print(f'  [{t}] parquet missing after processing', flush=True)
        return False
    print(f'  [{t}] H3 done in {(time.time()-t0)/60:.1f} min ({pq.stat().st_size//1024} KB)', flush=True)

    split = (['split_satellite_by_dpto.py', '--only', 'land_use'] if t == 'misiones'
             else ['split_by_admin.py', '--territory', t, '--only', 'land_use'])
    r = subprocess.run([sys.executable, str(SCRIPT_DIR / split[0]), *split[1:]],
                       capture_output=True, text=True, cwd=str(SCRIPT_DIR.parent))
    if r.returncode != 0:
        print(f'  [{t}] split_by_admin FAILED:\n{r.stdout[-500:]}\n{r.stderr[-500:]}', flush=True)
        return False
    print(f'  [{t}] split done', flush=True)
    if UPLOAD:
        # Upload as soon as the territory is ready (no CDN purge here: the site
        # keeps serving the old files until one purge at the end, so territories
        # never show mixed methods).
        r = subprocess.run([sys.executable, str(SCRIPT_DIR / 'upload_dw_land_use.py'),
                            '--territories', t, '--workers', '8'],
                           capture_output=True, text=True, cwd=str(SCRIPT_DIR.parent),
                           encoding='utf-8', errors='replace')
        tail = (r.stdout.strip().splitlines() or [''])[-1]
        print(f'  [{t}] upload: {tail}', flush=True)
        if r.returncode != 0:
            return False
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--territories', default=','.join(ALL_TERRITORIES))
    ap.add_argument('--skip-gee', action='store_true')
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--upload', action='store_true', help='upload each territory to R2 when done (no purge)')
    args = ap.parse_args()
    global UPLOAD
    UPLOAD = args.upload
    territories = [t.strip() for t in args.territories.split(',') if t.strip()]

    ok, fail = [], []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {}
        if args.skip_gee:
            for t in territories:
                futures[ex.submit(process_territory, t)] = t
        else:
            for t, gee_ok in wait_for_gee(territories):
                if gee_ok:
                    futures[ex.submit(process_territory, t)] = t
                else:
                    fail.append(t)
        for f in as_completed(futures):
            t = futures[f]
            try:
                (ok if f.result() else fail).append(t)
            except Exception as e:
                print(f'  [{t}] EXCEPTION: {e}', flush=True)
                fail.append(t)

    print('=' * 60)
    print(f'OK ({len(ok)}): {sorted(ok)}')
    print(f'FAILED ({len(fail)}): {sorted(fail)}')
    return 0 if not fail else 1


if __name__ == '__main__':
    sys.exit(main())
