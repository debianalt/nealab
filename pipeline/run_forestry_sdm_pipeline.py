"""
Monitor GEE SDM covariate exports → process H3 → run forestry SDM per territory.

Usage:
  python pipeline/run_forestry_sdm_pipeline.py
  python pipeline/run_forestry_sdm_pipeline.py --territories chaco,formosa
  python pipeline/run_forestry_sdm_pipeline.py --skip-gee
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

TERRITORIES = ['chaco', 'formosa', 'parana_br', 'santa_catarina_br', 'rio_grande_sul_br']

# 9 covariate names expected in GCS per territory
COVARIATE_NAMES = ['era5', 'chirps', 'terraclimate', 'soilgrids',
                   'srtm', 'ndvi', 'ghsl_smod', 'jrc_water', 'nelson']

OUTPUT_DIR = SCRIPT_DIR / 'output'

EXPECTED_PARQUETS = [
    'era5_annual_h3.parquet',
    'chirps_annual_h3.parquet',
    'terraclimate_annual_h3.parquet',
    'soilgrids_h3.parquet',
    'srtm_terrain_h3.parquet',
    'ndvi_annual_mean_h3.parquet',
    'ghsl_smod_h3.parquet',
    'jrc_water_annual_h3.parquet',
    'nelson_accessibility_h3.parquet',
]


def gee_pending_tasks() -> list[str]:
    """Return list of territories with incomplete GEE exports."""
    try:
        import ee
        ee.Initialize()
        pending = []
        for t in _territories:
            tasks = ee.data.listOperations()
            # check if any running tasks have this territory prefix
            running = [op for op in tasks
                       if op.get('metadata', {}).get('description', '').startswith(f'sdm_')
                       and t in op.get('metadata', {}).get('description', '')
                       and op.get('metadata', {}).get('state') in ('PENDING', 'RUNNING')]
            if running:
                pending.append(t)
        return pending
    except Exception as e:
        print(f"  GEE status check failed: {e}")
        return []


def all_covariates_ready(territory: str) -> bool:
    """Check if all expected parquets exist for a territory."""
    t_dir = OUTPUT_DIR / territory
    missing = [p for p in EXPECTED_PARQUETS if not (t_dir / p).exists()]
    if missing:
        print(f"  {territory}: missing {len(missing)} parquets: {missing[:3]}...")
        return False
    return True


def process_covariates(territory: str) -> bool:
    print(f"  Running process_sdm_covariates_h3.py --territory {territory}")
    r = subprocess.run(
        [sys.executable, str(SCRIPT_DIR / 'process_sdm_covariates_h3.py'),
         '--territory', territory],
        cwd=str(SCRIPT_DIR.parent)
    )
    return r.returncode == 0


def run_sdm(territory: str) -> bool:
    print(f"  Running compute_forestry_sdm.py --territory {territory}")
    r = subprocess.run(
        [sys.executable, str(SCRIPT_DIR / 'compute_forestry_sdm.py'),
         '--territory', territory, '--no-diagnostics'],
        cwd=str(SCRIPT_DIR.parent)
    )
    return r.returncode == 0


def wait_for_gee(territories: list[str], poll_secs: int = 120):
    """Poll GEE until all SDM exports complete."""
    try:
        import ee
        ee.Initialize()
    except Exception as e:
        print(f"GEE init failed: {e}. Use --skip-gee if TIFs are already in GCS.")
        return

    print(f"Polling GEE every {poll_secs}s for {len(territories)} territories × 9 exports...")
    while True:
        all_done = True
        for t in territories:
            try:
                ops = ee.data.listOperations()
                running = [
                    op for op in ops
                    if (f'sdm_' in op.get('metadata', {}).get('description', '') and
                        t in op.get('metadata', {}).get('description', '') and
                        op.get('metadata', {}).get('state') in ('PENDING', 'RUNNING'))
                ]
                if running:
                    all_done = False
                    print(f"  {t}: {len(running)} tasks still running")
            except Exception as e:
                print(f"  GEE check error for {t}: {e}")
        if all_done:
            print("All GEE exports complete (or none found running).")
            break
        time.sleep(poll_secs)


def main():
    global _territories
    parser = argparse.ArgumentParser()
    parser.add_argument('--territories', default=','.join(TERRITORIES))
    parser.add_argument('--skip-gee', action='store_true',
                        help='Skip GEE polling — process covariates directly')
    args = parser.parse_args()

    _territories = [t.strip() for t in args.territories.split(',')]

    print(f"Forestry SDM pipeline for: {_territories}")
    print("=" * 60)

    if not args.skip_gee:
        wait_for_gee(_territories)

    ok, fail = [], []
    for t in _territories:
        print(f"\n[{t}]")

        if not all_covariates_ready(t):
            if not process_covariates(t):
                fail.append(t)
                continue
            if not all_covariates_ready(t):
                print(f"  Still missing parquets after process_sdm_covariates. Check GCS.")
                fail.append(t)
                continue

        parquet = OUTPUT_DIR / t / 'sat_forestry_aptitude.parquet'
        if parquet.exists():
            size = parquet.stat().st_size // 1024
            print(f"  sat_forestry_aptitude.parquet already exists ({size} KB), re-running SDM...")

        if not run_sdm(t):
            fail.append(t)
            continue

        if parquet.exists():
            size = parquet.stat().st_size // 1024
            print(f"  sat_forestry_aptitude.parquet: {size} KB")
            ok.append(t)
        else:
            print(f"  ERROR: parquet not found after SDM run")
            fail.append(t)

    print("\n" + "=" * 60)
    print(f"Done: {len(ok)} ok {ok}, {len(fail)} failed {fail}")
    if ok:
        print("\nNext: split_by_admin --only=forestry_aptitude + R2 upload + config.ts + deploy")
    return 0 if not fail else 1


if __name__ == '__main__':
    _territories = []
    sys.exit(main())
