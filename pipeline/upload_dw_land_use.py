"""
Upload regenerated DW land_use parquets to R2 (global + per-dept) for the given
territories. Keys ALWAYS carry the data/ prefix (CLAUDE.md critical rule).

Usage:
  python pipeline/upload_dw_land_use.py
  python pipeline/upload_dw_land_use.py --territories itapua_py,corrientes
  python pipeline/upload_dw_land_use.py --dry-run
"""
import argparse
import glob
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / 'output'
ROOT = SCRIPT_DIR.parent

from run_dw_land_use import ALL_TERRITORIES  # noqa: E402  (same territory list)


def put(local: str, key: str, dry: bool) -> bool:
    if dry:
        print(f'  DRY {key}')
        return True
    for attempt in range(3):
        r = subprocess.run(
            ['npx', 'wrangler', 'r2', 'object', 'put', f'neahub/{key}',
             '--file', local, '--remote'],
            capture_output=True, text=True, cwd=str(ROOT), shell=(os.name == 'nt'))
        if r.returncode == 0:
            return True
        time.sleep(2 * (attempt + 1))
    print(f'  FAILED {key}: {r.stderr.strip()[-200:]}', flush=True)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--territories', default=','.join(ALL_TERRITORIES))
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()
    territories = [t.strip() for t in args.territories.split(',') if t.strip()]

    jobs = []  # (local, key)
    missing = []
    for t in territories:
        g = OUTPUT_DIR / t / 'sat_land_use.parquet'
        if not g.exists():
            missing.append(t)
            continue
        jobs.append((str(g), f'data/{t}/sat_land_use.parquet'))
        for f in sorted(glob.glob(str(OUTPUT_DIR / t / 'sat_dpto' / 'sat_land_use_*.parquet'))):
            jobs.append((f, f'data/{t}/sat_dpto/{os.path.basename(f)}'))

    if missing:
        print(f'SKIPPED (no parquet yet): {missing}')
    print(f'Uploading {len(jobs)} objects with {args.workers} workers...')
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        results = list(ex.map(lambda j: put(j[0], j[1], args.dry_run), jobs))
    ok = sum(results)
    print(f'Done: {ok}/{len(jobs)} in {(time.time()-t0)/60:.1f} min')
    return 0 if ok == len(jobs) and not missing else 1


if __name__ == '__main__':
    sys.exit(main())
