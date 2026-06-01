"""
Process accessibility for all PY territories that have LV rasters in GCS.
Downloads only lv_friction.tif + lv_cities_access.tif (the two required).

Usage:
  python pipeline/process_accessibility_py.py
  python pipeline/process_accessibility_py.py --only concepcion_py
  python pipeline/process_accessibility_py.py --workers 4
"""
import argparse
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, GCS_BUCKET, get_territory

ALL_PY = [
    "concepcion_py", "san_pedro_py", "cordillera_py", "guaira_py",
    "caaguazu_py", "caazapa_py", "misiones_py", "paraguari_py",
    "central_py", "neembucu_py", "amambay_py", "canindeyu_py",
    "presidente_hayes_py", "boqueron_py", "alto_paraguay_py",
]

REQUIRED_LV = ("lv_friction.tif", "lv_cities_access.tif")


def gcs_exists(path):
    return subprocess.run(f"gcloud storage ls {path}", shell=True,
                          capture_output=True).returncode == 0


def run(cmd, log=None):
    kw = {"stdout": log, "stderr": subprocess.STDOUT} if log else {}
    return subprocess.run(cmd, shell=True, cwd=SCRIPT_DIR, **kw).returncode == 0


def process_territory(t_id):
    territory = get_territory(t_id)
    t_prefix = territory['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, t_prefix.rstrip('/'))
    os.makedirs(t_dir, exist_ok=True)
    log_path = os.path.join(OUTPUT_DIR, f"accessibility_{t_id}.log")

    with open(log_path, "w") as log:
        log.write(f"=== {t_id} / accessibility ===\n")

        # Check GCS and download only what's needed
        for fname in REQUIRED_LV:
            gcs = f"gs://{GCS_BUCKET}/satellite/{t_id}/{fname}"
            local = os.path.join(t_dir, fname)
            if not gcs_exists(gcs):
                log.write(f"SKIP: {fname} not in GCS yet\n")
                return t_id, False
            if not os.path.exists(local):
                log.write(f"Downloading {fname}...\n"); log.flush()
                if not run(f"gcloud storage cp {gcs} {local}", log=log):
                    return t_id, False

        if not run(f"python compute_accessibility_h3.py --territory {t_id}", log=log):
            return t_id, False
        if not run(f"python split_by_admin.py --territory {t_id} --only accessibility", log=log):
            return t_id, False

        # Upload
        parquet = os.path.join(t_dir, "sat_accessibility.parquet")
        if not os.path.exists(parquet):
            log.write("ERROR: parquet not generated\n")
            return t_id, False

        run(f"npx wrangler r2 object put neahub/data/{t_prefix}sat_accessibility.parquet "
            f"--file {parquet} --remote", log=log)

        dpto_dir = os.path.join(t_dir, "sat_dpto")
        if os.path.exists(dpto_dir):
            for f in os.listdir(dpto_dir):
                if f.startswith("sat_accessibility_") and f.endswith(".parquet"):
                    run(f"npx wrangler r2 object put neahub/data/{t_prefix}sat_dpto/{f} "
                        f"--file {os.path.join(dpto_dir, f)} --remote", log=log)

    return t_id, True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None)
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()

    territories = ALL_PY
    if args.only:
        territories = [t.strip() for t in args.only.split(',') if t.strip() in ALL_PY]

    # Filter to those with LV rasters in GCS
    ready = []
    print("Checking GCS for LV rasters...")
    for t in territories:
        gcs = f"gs://{GCS_BUCKET}/satellite/{t}/lv_cities_access.tif"
        if gcs_exists(gcs):
            ready.append(t)
            print(f"  {t}: READY")
        else:
            print(f"  {t}: waiting for GEE")

    if not ready:
        print("No territories ready yet.")
        return 0

    print(f"\nProcessing {len(ready)} territories ({args.workers} workers)...")
    t0 = time.time()
    results = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_territory, t): t for t in ready}
        for future in as_completed(futures):
            t, ok = future.result()
            results[t] = ok
            print(f"  {'OK' if ok else 'FAIL'} {t} ({time.time()-t0:.0f}s)")

    ok_count = sum(v for v in results.values())
    failed = [t for t, ok in results.items() if not ok]
    print(f"\nDone: {ok_count}/{len(ready)} OK in {time.time()-t0:.0f}s")
    if failed:
        print(f"Failed: {failed}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
