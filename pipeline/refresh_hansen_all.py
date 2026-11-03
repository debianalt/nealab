"""
Refresh deforestation_dynamics in every territory with a new Hansen release.

Per territory (4 in parallel):
  1. satellite/<t>/hansen_lossyear.tif -> output/<t>/, then deleted from the bucket
     (gcs_staging.py; treecover2000 is the same baseline in every release and is reused)
  2. process_hansen_to_h3.py --territory <t> --mode comparable
  3. refresh_dept_parquets.py --layer deforestation_dynamics: each department keeps
     exactly its cells (no re-split; the INDEC partition of 2-nov-2026 stays),
     backup of what the CDN served, upload with size check.

Then: build_deforest_rank.py, bump ?v (sat_deforestation_dynamics) and DEPT_V in
src/lib/config.ts, deploy, npm run purge-cdn.

    python pipeline/gee_export_hansen_loss.py --territory <t> --gcs --no-wait   # x24, first
    python pipeline/refresh_hansen_all.py --backup-dir C:/Users/ant/r2-hansen-v112-backup-2026-11 [--only a,b] [--dry-run]
"""
import argparse
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, TERRITORY_CONFIGS, get_territory


def run(cmd: list[str], log) -> bool:
    log.write(f"\n$ {' '.join(cmd)}\n"); log.flush()
    # Unbuffered: a killed child must leave in the log every upload it already made.
    env = {**os.environ, 'PYTHONUNBUFFERED': '1'}
    return subprocess.run(cmd, cwd=SCRIPT_DIR, stdout=log, stderr=subprocess.STDOUT, env=env).returncode == 0


def one(t: str, backup_dir: str, dry: bool) -> tuple[str, bool]:
    prefix = get_territory(t)['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, prefix.rstrip('/')) if prefix else OUTPUT_DIR
    gcs_name = 'satellite/' + ('' if t == 'misiones' else f'{t}/') + 'hansen_lossyear.tif'
    with open(os.path.join(OUTPUT_DIR, f'hansen_refresh_{t}.log'), 'w', encoding='utf-8') as log:
        steps = [
            # Download and delete the export from the bucket (gcs_staging.py)
            [sys.executable, 'gcs_staging.py', gcs_name, os.path.join(t_dir, 'hansen_lossyear.tif')],
            # Guard: a stale GCS file (previous release) passes every other step silently.
            [sys.executable, '-c', "import rasterio,sys; r=rasterio.open(sys.argv[1]); "
             "m=int(r.read(1, out_shape=(max(1,r.height//4), max(1,r.width//4))).max()); "
             "print('lossyear max code', m); sys.exit(0 if m >= 25 else 1)", os.path.join(t_dir, 'hansen_lossyear.tif')],
            [sys.executable, 'process_hansen_to_h3.py', '--territory', t, '--mode', 'comparable'],
            [sys.executable, 'refresh_dept_parquets.py', '--layer', 'deforestation_dynamics', '--territory', t,
             '--backup-dir', backup_dir, '--keep-missing'] + (['--dry-run'] if dry else []),
        ]
        for cmd in steps:
            if not run(cmd, log):
                return t, False
    return t, True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--backup-dir', required=True)
    ap.add_argument('--only', default=None)
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    ts = [t.strip() for t in a.only.split(',')] if a.only else list(TERRITORY_CONFIGS)
    t0 = time.time()
    failed = []
    with ThreadPoolExecutor(max_workers=a.workers) as pool:
        futs = [pool.submit(one, t, a.backup_dir, a.dry_run) for t in ts]
        for f in as_completed(futs):
            t, ok = f.result()
            print(f"  {'OK ' if ok else 'FAIL'} {t} ({time.time() - t0:.0f}s)", flush=True)
            if not ok:
                failed.append(t)
    print(f'Done {len(ts) - len(failed)}/{len(ts)}; failed: {failed} (logs: output/hansen_refresh_<t>.log)')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
