"""
Finisher for v1.1 NEA-complete re-baseline (Chaco + Formosa).

Coordinates the post-GEE steps:
  1. Check GEE task status (Chaco/Formosa). Exit early if not all DONE.
  2. Download rasters from GCS to pipeline/output/<territory>/
  3. process_raster_to_h3 for each analysis × {chaco, formosa}
  4. split_by_admin for each analysis × {chaco, formosa}
  5. compute_goalposts.py --dry-run over pool {misiones, corrientes, itapua_py,
     alto_parana_py, chaco, formosa}. Print delta. Exit if --dry-run.
  6. (with --commit) compute_goalposts.py to write v1.1
  7. (with --commit) Re-score all 6 territories in --mode comparable
  8. (with --commit) Re-split + R2 upload + cache bust bump in src/lib/config.ts

Usage:
  python pipeline/finish_v11_rebaseline.py --check       # status only
  python pipeline/finish_v11_rebaseline.py --download    # only step 2
  python pipeline/finish_v11_rebaseline.py --process     # steps 2-4
  python pipeline/finish_v11_rebaseline.py --dry-run     # steps 2-5 (no commit)
  python pipeline/finish_v11_rebaseline.py --commit      # full v1.1 deployment
"""

import argparse
import json
import os
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, GCS_BUCKET

NEW_TERRITORIES = ['chaco', 'formosa', 'parana_br', 'santa_catarina_br', 'rio_grande_sul_br']
ALL_TERRITORIES = ['misiones', 'corrientes', 'itapua_py', 'alto_parana_py'] + NEW_TERRITORIES

# Analyses that have raster pipelines (process_raster_to_h3)
RASTER_ANALYSES = [
    'environmental_risk', 'climate_comfort', 'green_capital',
    'change_pressure', 'forest_health', 'agri_potential', 'land_use',
]

# Analyses with their own dedicated GEE export scripts (downloaded separately)
SPECIAL_ANALYSES = {
    'carbon_stock':       'carbon/{territory}/sat_carbon_stock_raster.tif',
    'pm25_drivers':       'pm25_annual/{territory}/sat_pm25_{year}.tif',
    'soil_water':         'soil_water/{territory}/sat_soil_water_raster.tif',
    'productive_activity': 'satellite/{territory}/{territory}_act_*.tif',
    'deforestation_dynamics': 'hansen/{territory}/hansen_lossyear_{territory}.tif',
}


def run(cmd, dry_run=False):
    print(f"\n  $ {cmd}")
    if dry_run:
        return 0
    return subprocess.run(cmd, shell=True).returncode


def check_gee_status():
    """List GEE tasks for all new territories. Returns (n_done, n_pending, n_failed)."""
    print("\n" + "=" * 60)
    print(f"  STEP 1: GEE task status ({', '.join(NEW_TERRITORIES)})")
    print("=" * 60)
    result = subprocess.run(
        ['earthengine', 'task', 'list'],
        capture_output=True, text=True, timeout=120
    )
    # Match any of the new territories (Chaco/Formosa/BR-sur)
    matchers = [t.lower() for t in NEW_TERRITORIES] + ['parana', 'santa_catarina', 'rio_grande']
    lines = [l for l in result.stdout.splitlines()
             if any(m in l.lower() for m in matchers)]

    n_done = sum(1 for l in lines if 'COMPLETED' in l or 'SUCCEEDED' in l)
    n_pending = sum(1 for l in lines if 'PENDING' in l or 'RUNNING' in l)
    n_failed = sum(1 for l in lines if 'FAILED' in l or 'CANCELLED' in l)

    print(f"  Total Chaco/Formosa tasks: {len(lines)}")
    print(f"  COMPLETED: {n_done}")
    print(f"  PENDING/RUNNING: {n_pending}")
    print(f"  FAILED: {n_failed}")
    if n_failed > 0:
        print("\n  Failed tasks:")
        for l in lines:
            if 'FAILED' in l or 'CANCELLED' in l:
                print(f"    {l.strip()}")
    return n_done, n_pending, n_failed


def download_rasters(territories=NEW_TERRITORIES, dry_run=False):
    """Pull all rasters from GCS for the given territories."""
    print("\n" + "=" * 60)
    print(f"  STEP 2: Download rasters from GCS ({territories})")
    print("=" * 60)
    rc_all = 0
    for t in territories:
        out_dir = os.path.join(OUTPUT_DIR, t)
        os.makedirs(out_dir, exist_ok=True)
        # generic satellite/<territory>/ rasters (env_risk, climate, etc.)
        cmd = f"gcloud storage cp gs://{GCS_BUCKET}/satellite/{t}/*.tif {out_dir}/"
        rc = run(cmd, dry_run)
        if rc != 0:
            print(f"  WARNING: download from satellite/{t}/ partial or empty")
            rc_all = rc
        # Specialized prefixes
        for prefix in ('carbon', 'pm25_annual', 'soil_water', 'hansen'):
            sub_cmd = f"gcloud storage cp gs://{GCS_BUCKET}/{prefix}/{t}/*.tif {out_dir}/"
            run(sub_cmd, dry_run)
    return rc_all


def process_to_h3(territories=NEW_TERRITORIES, dry_run=False):
    """Process raster -> H3 parquets for the standard raster analyses."""
    print("\n" + "=" * 60)
    print("  STEP 3: process_raster_to_h3 (raster -> H3 parquet)")
    print("=" * 60)
    ids = ",".join(RASTER_ANALYSES)
    for t in territories:
        rc = run(
            f"python pipeline/process_raster_to_h3.py --territory {t} "
            f"--analysis {ids} --mode local",
            dry_run
        )
        if rc != 0:
            print(f"  ERROR processing {t}")
            return False
    return True


def split_admin(territories=NEW_TERRITORIES, dry_run=False):
    """Split each parquet by department."""
    print("\n" + "=" * 60)
    print("  STEP 4: split_by_admin (per-departamento parquets)")
    print("=" * 60)
    ids = ",".join(RASTER_ANALYSES)
    for t in territories:
        rc = run(
            f"python pipeline/split_by_admin.py --territory {t} --only {ids}",
            dry_run
        )
        if rc != 0:
            return False
    return True


def goalposts_dry_run():
    """Run compute_goalposts.py --dry-run with full pool."""
    print("\n" + "=" * 60)
    print("  STEP 5: compute_goalposts.py --dry-run (pool 6 territorios)")
    print("=" * 60)
    return run("python pipeline/compute_goalposts.py --dry-run")


def goalposts_commit():
    """Execute v1.1 goalposts re-baseline."""
    print("\n" + "=" * 60)
    print("  STEP 6: compute_goalposts.py (WRITE v1.1)")
    print("=" * 60)
    return run("python pipeline/compute_goalposts.py")


def rescore_all(dry_run=False):
    """Re-run --mode comparable scoring on all 6 territories with new goalposts."""
    print("\n" + "=" * 60)
    print("  STEP 7: Re-score all 6 territories (--mode comparable)")
    print("=" * 60)
    ids = ",".join(RASTER_ANALYSES)
    for t in ALL_TERRITORIES:
        rc = run(
            f"python pipeline/process_raster_to_h3.py --territory {t} "
            f"--analysis {ids} --mode comparable",
            dry_run
        )
        if rc != 0:
            print(f"  ERROR re-scoring {t}")
            return False
        run(
            f"python pipeline/split_by_admin.py --territory {t} --only {ids}",
            dry_run
        )
    return True


def upload_r2(dry_run=False):
    """Upload all 6 territories' parquets to R2 under their prefixes."""
    print("\n" + "=" * 60)
    print("  STEP 8: R2 upload (6 territories)")
    print("=" * 60)
    for t in ALL_TERRITORIES:
        prefix_local = '' if t == 'misiones' else t + '/'
        prefix_r2 = '' if t == 'misiones' else t + '/'
        out_dir = os.path.join(OUTPUT_DIR, prefix_local.rstrip('/')) if prefix_local else OUTPUT_DIR
        for aid in RASTER_ANALYSES:
            local = os.path.join(out_dir, f"sat_{aid}.parquet")
            if not os.path.exists(local):
                continue
            r2_key = f"neahub/data/{prefix_r2}sat_{aid}.parquet"
            run(f"npx wrangler r2 object put {r2_key} --file {local} --remote", dry_run)
        dpto_dir = os.path.join(out_dir, "sat_dpto")
        if os.path.isdir(dpto_dir):
            for f in sorted(os.listdir(dpto_dir)):
                if not f.endswith(".parquet"):
                    continue
                local = os.path.join(dpto_dir, f)
                r2_key = f"neahub/data/{prefix_r2}sat_dpto/{f}"
                run(f"npx wrangler r2 object put {r2_key} --file {local} --remote", dry_run)
    return True


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument('--check', action='store_true', help='GEE status only')
    g.add_argument('--download', action='store_true', help='Download rasters only')
    g.add_argument('--process', action='store_true', help='Download + process_to_h3 + split')
    g.add_argument('--dry-run', action='store_true', help='All steps + goalposts dry-run (no write)')
    g.add_argument('--commit', action='store_true', help='Full v1.1 deployment (writes goalposts + re-scores all)')
    args = ap.parse_args()

    if not any([args.check, args.download, args.process, args.dry_run, args.commit]):
        ap.print_help()
        return 1

    n_done, n_pending, n_failed = check_gee_status()
    if args.check:
        return 0

    if n_pending > 0:
        print(f"\n  {n_pending} GEE tasks still pending. Wait or re-run --check later.")
        if not args.dry_run and not args.commit:
            print("  (Use --download anyway to pull whatever is ready)")
            if not args.download:
                return 1

    if args.download or args.process or args.dry_run or args.commit:
        download_rasters()

    if args.download:
        return 0

    if args.process or args.dry_run or args.commit:
        if not process_to_h3():
            return 1
        if not split_admin():
            return 1

    if args.process:
        return 0

    if args.dry_run or args.commit:
        goalposts_dry_run()

    if args.dry_run:
        print("\n  DRY RUN complete. Review goalposts deltas above.")
        print("  Run with --commit to write v1.1 and re-score all 6 territories.")
        return 0

    if args.commit:
        if goalposts_commit() != 0:
            return 1
        if not rescore_all():
            return 1
        if not upload_r2():
            return 1
        print("\n  v1.1 deployment complete.")
        print("  Next manual steps:")
        print("    1. Bump cache busters in src/lib/config.ts (sat_* entries)")
        print("    2. Set ANALYSIS_REGISTRY[*].coverage.{chaco,formosa} = 'available'")
        print("    3. Update CITATION.cff version to 1.2.0 + date + bump Zenodo")
        print("    4. npm run deploy")
        return 0


if __name__ == '__main__':
    sys.exit(main())
