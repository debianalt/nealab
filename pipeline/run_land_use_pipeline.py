"""
Monitor GEE land_use export tasks → download from GCS → process to H3.

Usage:
  python pipeline/run_land_use_pipeline.py
  python pipeline/run_land_use_pipeline.py --territories chaco,formosa
  python pipeline/run_land_use_pipeline.py --skip-gee   # assume TIFs already in output/
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

# GEE task IDs per territory (from current session exports)
TASK_IDS = {
    'chaco':              'U2G24BPR6TPP5S6LK5NABRYV',
    'formosa':            'PTNRWUPMP3AFUQT3ECZZQ3NE',
    'parana_br':          'OMOXPZXXMUXMTYUFPWKIYRZU',
    'santa_catarina_br':  '2X6KITQT6I4QL5VBIIYWVDAV',
    'rio_grande_sul_br':  'YDQUGPS3TDKQGZUHJM7BCC53',
}

GCS_PREFIX = 'gs://spatia-satellite/satellite'
OUTPUT_DIR = SCRIPT_DIR / 'output'


def gee_task_state(task_id: str) -> str:
    try:
        import ee
        ee.Initialize()
        status = ee.data.getTaskStatus([task_id])[0]
        return status.get('state', 'UNKNOWN')
    except Exception as e:
        return f'ERROR:{e}'


def wait_for_tasks(task_ids: dict, poll_secs: int = 90) -> dict:
    """Poll until all tasks reach terminal state. Returns {t: state}."""
    pending = dict(task_ids)
    done = {}
    print(f"Monitoring {len(pending)} GEE tasks (polling every {poll_secs}s)...")
    while pending:
        time.sleep(poll_secs)
        still_pending = {}
        for t, tid in pending.items():
            state = gee_task_state(tid)
            if state in ('COMPLETED', 'FAILED', 'CANCELLED'):
                done[t] = state
                print(f"  [{state}] {t}")
            else:
                still_pending[t] = tid
                print(f"  [{state}] {t} ...")
        pending = still_pending
    return done


def gcs_path(territory: str) -> str:
    year = 2021 if territory.endswith('_br') else 2022
    return f'{GCS_PREFIX}/{territory}/mapbiomas_{territory}_{year}.tif'


def download_tif(territory: str, t_dir: Path) -> bool:
    src = gcs_path(territory)
    dst = t_dir / Path(src).name
    if dst.exists():
        print(f"  Already exists: {dst.name}")
        return True
    print(f"  gcloud cp {src} -> {dst.name}")
    r = subprocess.run(
        ['gcloud', 'storage', 'cp', src, str(dst)],
        capture_output=True, text=True
    )
    if r.returncode != 0:
        print(f"  ERROR: {r.stderr.strip()}")
        return False
    print(f"  Downloaded: {dst.name} ({dst.stat().st_size // 1024 // 1024} MB)")
    return True


def process_h3(territory: str) -> bool:
    print(f"  Running process_mapbiomas_to_h3.py --territory {territory}")
    r = subprocess.run(
        [sys.executable, str(SCRIPT_DIR / 'process_mapbiomas_to_h3.py'),
         '--territory', territory],
        cwd=str(SCRIPT_DIR.parent)
    )
    return r.returncode == 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--territories', default=','.join(TASK_IDS.keys()),
                        help='Comma-separated territory IDs')
    parser.add_argument('--skip-gee', action='store_true',
                        help='Skip GEE polling — assume TIFs already downloaded')
    args = parser.parse_args()

    territories = [t.strip() for t in args.territories.split(',')]
    task_ids = {t: TASK_IDS[t] for t in territories if t in TASK_IDS}

    print(f"Land-use pipeline for: {territories}")
    print("=" * 60)

    # Phase 1: wait for GEE
    if not args.skip_gee and task_ids:
        results = wait_for_tasks(task_ids)
        failed_gee = [t for t, s in results.items() if s != 'COMPLETED']
        if failed_gee:
            print(f"GEE FAILED for: {failed_gee}. Check https://code.earthengine.google.com/tasks")
            territories = [t for t in territories if t not in failed_gee]
    else:
        print("Skipping GEE wait (--skip-gee)")

    # Phase 2: download + process per territory
    ok = []
    fail = []
    for t in territories:
        t_dir = OUTPUT_DIR / t
        print(f"\n[{t}]")

        if not download_tif(t, t_dir):
            fail.append(t)
            continue

        if not process_h3(t):
            fail.append(t)
            continue

        parquet = t_dir / 'sat_land_use.parquet'
        if parquet.exists():
            size_mb = parquet.stat().st_size / 1024 / 1024
            print(f"  sat_land_use.parquet: {size_mb:.1f} MB")
            ok.append(t)
        else:
            print(f"  ERROR: sat_land_use.parquet not found after processing")
            fail.append(t)

    print("\n" + "=" * 60)
    print(f"Done: {len(ok)} ok {ok}, {len(fail)} failed {fail}")
    if ok:
        print("\nNext steps:")
        print("  split_by_admin.py --only=land_use for each territory")
        print("  R2 upload globals + per-dept")
        print("  config.ts coverage flip + cache bust")
        print("  git commit + deploy")
    return 0 if not fail else 1


if __name__ == '__main__':
    sys.exit(main())
