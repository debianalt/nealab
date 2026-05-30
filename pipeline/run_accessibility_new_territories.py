"""
Run accessibility pipeline for the 5 territories that didn't have data:
  chaco, formosa, parana_br, santa_catarina_br, rio_grande_sul_br

Steps per territory:
  1. GEE export (if lv_friction.tif / lv_cities_access.tif missing in GCS)
  2. compute_accessibility_h3.py --territory <id> --mode comparable
  3. split_by_admin.py --territory <id> --only accessibility  (also writes summary JSON)
  4. R2 upload (global + dept parquets)
  5. Update config.ts coverage flags (manual, printed as reminder)

Run from neahub/ directory:
  python pipeline/run_accessibility_new_territories.py
  python pipeline/run_accessibility_new_territories.py --territory chaco  # single territory

Chaco and Formosa: lv_friction.tif already in GCS from location_value run. Skip GEE.
Paraná/SC/RS: need GEE export first (use --gee flag or run gee_export_location_value.py manually).
"""
import argparse
import glob
import io
import sys
# Force UTF-8 output on Windows (default cp1252 can't handle wrangler/gcloud emoji output)
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import json
import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR   = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'output')
NPX        = r'C:\Program Files\nodejs\npx.cmd'
GCS_BUCKET = 'spatia-satellite'

# Territories that already have GCS rasters (lv_friction + lv_cities_access).
# Chaco and Formosa do NOT have them — gee_export_location_value.py was never
# run for them (v1.1 rebaseline only exported other analyses, not friction/cities_access).
HAVE_GCS_RASTERS: set = set()

ALL_NEW = ['chaco', 'formosa', 'parana_br', 'santa_catarina_br', 'rio_grande_sul_br']

sys.path.insert(0, SCRIPT_DIR)


def run(script, *args):
    cmd = [sys.executable, os.path.join(SCRIPT_DIR, script)] + list(args)
    label = f"{script} {' '.join(args)}"
    print(f"\n>>> {label}")
    r = subprocess.run(cmd, cwd=ROOT_DIR)
    if r.returncode != 0:
        print(f"FAILED: {label}")
        return False
    return True


def r2_upload(local_path: str, r2_key: str) -> bool:
    r = subprocess.run(
        [NPX, 'wrangler', 'r2', 'object', 'put', f'neahub/{r2_key}',
         '--file', local_path, '--remote'],
        cwd=ROOT_DIR, capture_output=True, encoding='utf-8', errors='replace', shell=True
    )
    ok = r.returncode == 0
    if ok:
        print(f"  R2 ok {r2_key}")
    else:
        print(f"  R2 FAIL {r2_key}: {r.stderr[-300:]}")
    return ok


def process_territory(territory: str, run_gee: bool) -> bool:
    prefix = '' if territory == 'misiones' else f'{territory}/'
    t_dir  = os.path.join(OUTPUT_DIR, territory)
    os.makedirs(t_dir, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"  TERRITORY: {territory}")
    print(f"{'='*60}")

    # ── Step 1: GEE export ─────────────────────────────────────────────────────
    friction_path = os.path.join(t_dir, 'lv_friction.tif')
    cities_path   = os.path.join(t_dir, 'lv_cities_access.tif')
    need_rasters  = not (os.path.exists(friction_path) and os.path.exists(cities_path))

    if need_rasters and territory in HAVE_GCS_RASTERS:
        print("\nDownloading rasters from GCS (already exported)...")
        import platform, subprocess as sp
        gcloud = 'gcloud.cmd' if platform.system() == 'Windows' else 'gcloud'
        for fname, local in [('lv_friction.tif', friction_path), ('lv_cities_access.tif', cities_path)]:
            if not os.path.exists(local):
                gcs_path = f'gs://{GCS_BUCKET}/satellite/{territory}/{fname}'
                r = sp.run([gcloud, 'storage', 'cp', gcs_path, local],
                           capture_output=True, text=True)
                if r.returncode != 0:
                    print(f"  FAIL downloading {fname}: {r.stderr.strip()}")
                    return False
                print(f"  Downloaded {fname}")

    elif need_rasters and run_gee:
        print("\nRunning GEE export (will take ~15-30 min)...")
        ok = run('gee_export_location_value.py', '--territory', territory, '--gcs')
        if not ok:
            print(f"  GEE export failed for {territory}")
            return False
    elif need_rasters:
        print(f"\nWARNING: rasters missing for {territory}.")
        print(f"  Run first: python pipeline/gee_export_location_value.py --territory {territory} --gcs")
        print(f"  Then re-run this script.")
        return False

    # ── Step 2: compute accessibility ─────────────────────────────────────────
    global_parquet = os.path.join(t_dir, 'sat_accessibility.parquet')
    if not os.path.exists(global_parquet):
        ok = run('compute_accessibility_h3.py', '--territory', territory, '--mode', 'comparable')
        if not ok or not os.path.exists(global_parquet):
            print(f"  compute_accessibility_h3 failed for {territory}")
            return False
    else:
        print(f"\n  Parquet already exists: {global_parquet} — skipping compute")

    # ── Step 3: split by admin (writes summary JSON to src/lib/data/) ──────────
    ok = run('split_by_admin.py', '--territory', territory, '--only', 'accessibility')
    if not ok:
        print(f"  split_by_admin failed for {territory}")
        return False

    # ── Step 4: R2 upload ─────────────────────────────────────────────────────
    print(f"\nUploading to R2...")
    r2_key_global = f'data/{prefix}sat_accessibility.parquet'
    r2_upload(global_parquet, r2_key_global)

    dpto_dir = os.path.join(t_dir, 'sat_dpto')
    dpto_files = glob.glob(os.path.join(dpto_dir, 'sat_accessibility_*.parquet'))
    uploaded = sum(
        1 for f in dpto_files
        if r2_upload(f, f'data/{prefix}sat_dpto/{os.path.basename(f)}')
    )
    print(f"  Uploaded {uploaded}/{len(dpto_files)} dept parquets")

    print(f"\n✓ {territory} done.")
    print(f"  Summary JSON: src/lib/data/{territory}_sat_accessibility_summary.json")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--territory', default=None,
                        help='Single territory (default: all 5)')
    parser.add_argument('--gee', action='store_true',
                        help='Run GEE export for BR territories (parana_br/sc/rs)')
    args = parser.parse_args()

    targets = [args.territory] if args.territory else ALL_NEW
    results = {}
    for t in targets:
        results[t] = process_territory(t, run_gee=args.gee)

    print(f"\n{'='*60}")
    print("SUMMARY")
    for t, ok in results.items():
        status = 'OK' if ok else 'FAIL'
        print(f"  {status} {t}")

    print("""
NEXT STEPS (manual):
  1. Commit the new summary JSONs in src/lib/data/
  2. Update config.ts HEX_LAYER_REGISTRY accessibility.coverage:
       chaco: 'available', formosa: 'available',
       parana_br: 'available', santa_catarina_br: 'available', rio_grande_sul_br: 'available'
     (currently set to 'unavailable' — change after data confirmed)
  3. Bump cache-buster for sat_accessibility in config.ts
  4. Deploy
""")


if __name__ == '__main__':
    main()
