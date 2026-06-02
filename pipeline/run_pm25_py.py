"""
Full pm25_drivers pipeline for PY territories after GEE exports complete.

Steps per territory:
1. Download sat_pm25_*.tif from GCS (if not local)
2. process_pm25_annual_to_h3.py -> pm25_annual_panel.parquet
3. compute_pm25_drivers.py --mode comparable -> sat_pm25_drivers.parquet
4. split_by_admin.py --only pm25_drivers
5. Upload to R2

Usage:
  python pipeline/run_pm25_py.py
  python pipeline/run_pm25_py.py --only boqueron_py,alto_paraguay_py,presidente_hayes_py
"""
import argparse
import os
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, GCS_BUCKET, get_territory

TERRITORIES = ["boqueron_py", "alto_paraguay_py", "presidente_hayes_py"]


def run(cmd, cwd=None, log=None):
    kw = {"stdout": log, "stderr": subprocess.STDOUT} if log else {}
    return subprocess.run(cmd, shell=True, cwd=cwd or SCRIPT_DIR, **kw).returncode


def download_pm25_tifs(t_id, t_dir, log):
    """Download sat_pm25_*.tif from GCS if not already local."""
    log.write(f"\n--- Download pm25 TIFs from GCS ---\n")
    gcs_base = f"gs://{GCS_BUCKET}/satellite/{t_id}/"
    rc = run(f"gcloud storage cp \"{gcs_base}sat_pm25_*.tif\" {t_dir}/", log=log)
    if rc != 0:
        log.write(f"WARN: gcloud cp returned {rc} (some files may already exist)\n")
    local = [f for f in os.listdir(t_dir) if f.startswith("sat_pm25_") and f.endswith(".tif")]
    log.write(f"Local pm25 TIFs: {sorted(local)}\n")
    return len(local) > 0


def upload_parquet(t_prefix, t_dir, analysis):
    pq = os.path.join(t_dir, f"sat_{analysis}.parquet")
    if not os.path.exists(pq):
        return False
    run(f"npx wrangler r2 object put neahub/data/{t_prefix}sat_{analysis}.parquet "
        f"--file {pq} --remote")
    dpto = os.path.join(t_dir, "sat_dpto")
    if os.path.isdir(dpto):
        for f in sorted(os.listdir(dpto)):
            if f.startswith(f"sat_{analysis}_") and f.endswith(".parquet"):
                run(f"npx wrangler r2 object put neahub/data/{t_prefix}sat_dpto/{f} "
                    f"--file {os.path.join(dpto, f)} --remote")
    return True


def process_one(t_id):
    t0 = time.time()
    territory = get_territory(t_id)
    t_prefix = territory['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, t_prefix.rstrip('/'))
    os.makedirs(t_dir, exist_ok=True)
    log_path = os.path.join(OUTPUT_DIR, f"pm25_full_{t_id}.log")
    print(f"  [{t_id}] Starting -> {log_path}")

    with open(log_path, "w") as log:
        log.write(f"=== {t_id} / pm25_drivers (full) ===\n")

        if not download_pm25_tifs(t_id, t_dir, log):
            log.write("FAIL: no pm25 TIFs found after download\n")
            print(f"  [{t_id}] FAIL: no TIFs")
            return False

        log.write("\n--- process_pm25_annual_to_h3.py ---\n")
        rc = run(f"python process_pm25_annual_to_h3.py --territory {t_id}", log=log)
        if rc != 0:
            log.write(f"FAIL: process_pm25_annual rc={rc}\n")
            print(f"  [{t_id}] FAIL annual to h3")
            return False

        log.write("\n--- compute_pm25_drivers.py ---\n")
        rc = run(f"python compute_pm25_drivers.py --territory {t_id} --mode comparable", log=log)
        if rc != 0:
            log.write(f"FAIL: compute_pm25_drivers rc={rc}\n")
            print(f"  [{t_id}] FAIL drivers")
            return False

        log.write("\n--- split_by_admin.py ---\n")
        rc = run(f"python split_by_admin.py --territory {t_id} --only pm25_drivers", log=log)
        if rc != 0:
            log.write(f"FAIL: split rc={rc}\n")
            print(f"  [{t_id}] FAIL split")
            return False

        log.write("\n--- R2 upload ---\n")
        ok = upload_parquet(t_prefix, t_dir, "pm25_drivers")
        if not ok:
            log.write("FAIL: upload\n")
            print(f"  [{t_id}] FAIL upload")
            return False

    elapsed = time.time() - t0
    print(f"  [{t_id}] OK in {elapsed/60:.1f}min")
    return True


def check_gcs_ready(t_id):
    """Check if at least one pm25 TIF exists in GCS."""
    r = subprocess.run(
        f"gcloud storage ls gs://{GCS_BUCKET}/satellite/{t_id}/sat_pm25_2020.tif",
        shell=True, capture_output=True)
    return r.returncode == 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None)
    ap.add_argument("--force", action="store_true", help="Run even if GCS not ready")
    args = ap.parse_args()

    territories = TERRITORIES
    if args.only:
        territories = [t.strip() for t in args.only.split(",") if t.strip() in TERRITORIES]

    print(f"PM2.5 pipeline for: {territories}")
    not_ready = []
    if not args.force:
        for t in territories:
            if not check_gcs_ready(t):
                print(f"  SKIP {t}: sat_pm25_2020.tif not in GCS yet (GEE still running)")
                not_ready.append(t)
        territories = [t for t in territories if t not in not_ready]

    if not territories:
        print("No ready territories. Re-run when GEE exports complete.")
        return 1

    ok_list, fail_list = [], []
    for t in territories:
        result = process_one(t)
        if result:
            ok_list.append(t)
        else:
            fail_list.append(t)

    print(f"\n=== PM25 RESULTS ===")
    print(f"OK ({len(ok_list)}): {ok_list}")
    print(f"FAIL ({len(fail_list)}): {fail_list}")
    if ok_list:
        print("\nNext: update config.ts pm25_drivers coverage + deploy")
    return 0 if not fail_list else 1


if __name__ == "__main__":
    sys.exit(main())
