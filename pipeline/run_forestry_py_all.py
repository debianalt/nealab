"""
Run forestry SDM pipeline for all ready PY territories.
1. process_sdm_covariates_h3.py -> era5_annual_h3.parquet etc.
2. compute_forestry_sdm.py -> sat_forestry_aptitude.parquet
3. split_by_admin.py -> sat_dpto/sat_forestry_aptitude_*.parquet
4. Upload to R2

Usage:
  python pipeline/run_forestry_py_all.py
  python pipeline/run_forestry_py_all.py --workers 3
  python pipeline/run_forestry_py_all.py --only misiones_py,caaguazu_py
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

READY_TERRITORIES = [
    "misiones_py", "caaguazu_py", "caazapa_py", "central_py",
    "neembucu_py", "amambay_py", "paraguari_py", "cordillera_py",
    "guaira_py", "canindeyu_py", "boqueron_py", "alto_paraguay_py",
    # 8-tif territories — will try, may need GCS download for missing 1
    "concepcion_py", "san_pedro_py", "presidente_hayes_py",
]


def run(cmd, cwd=None, log=None):
    kw = {"stdout": log, "stderr": subprocess.STDOUT} if log else {}
    return subprocess.run(cmd, shell=True, cwd=cwd or SCRIPT_DIR, **kw).returncode


def upload_parquet(t_prefix, t_dir, analysis):
    pq = os.path.join(t_dir, f"sat_{analysis}.parquet")
    if not os.path.exists(pq):
        print(f"  WARN: {pq} not found")
        return False
    r = run(f"npx wrangler r2 object put neahub/data/{t_prefix}sat_{analysis}.parquet "
            f"--file {pq} --remote")
    if r != 0:
        print(f"  ERROR uploading sat_{analysis}.parquet")
        return False
    dpto_dir = os.path.join(t_dir, "sat_dpto")
    if os.path.isdir(dpto_dir):
        for f in sorted(os.listdir(dpto_dir)):
            if f.startswith(f"sat_{analysis}_") and f.endswith(".parquet"):
                run(f"npx wrangler r2 object put neahub/data/{t_prefix}sat_dpto/{f} "
                    f"--file {os.path.join(dpto_dir, f)} --remote")
    return True


def process_one(t_id, log_dir):
    t0 = time.time()
    territory = get_territory(t_id)
    t_prefix = territory['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, t_prefix.rstrip('/'))

    log_path = os.path.join(log_dir, f"forestry_full_{t_id}.log")
    print(f"  [{t_id}] Starting -> {log_path}")

    with open(log_path, "w") as log:
        log.write(f"=== {t_id} / forestry_aptitude (full pipeline) ===\n")

        # Step 1: process SDM covariates to H3 parquets
        log.write("\n--- process_sdm_covariates_h3.py ---\n")
        rc = run(f"python process_sdm_covariates_h3.py --territory {t_id}", log=log)
        if rc != 0:
            log.write(f"FAIL: process_sdm_covariates_h3 rc={rc}\n")
            print(f"  [{t_id}] FAIL covariates")
            return False

        # Step 2: compute SDM -> sat_forestry_aptitude.parquet
        log.write("\n--- compute_forestry_sdm.py ---\n")
        rc = run(f"python compute_forestry_sdm.py --territory {t_id} --no-diagnostics", log=log)
        if rc != 0:
            log.write(f"FAIL: compute_forestry_sdm rc={rc}\n")
            print(f"  [{t_id}] FAIL sdm")
            return False

        # Step 3: split by admin
        log.write("\n--- split_by_admin.py ---\n")
        rc = run(f"python split_by_admin.py --territory {t_id} --only forestry_aptitude", log=log)
        if rc != 0:
            log.write(f"FAIL: split_by_admin rc={rc}\n")
            print(f"  [{t_id}] FAIL split")
            return False

        # Step 4: upload to R2
        log.write("\n--- R2 upload ---\n")
        ok = upload_parquet(t_prefix, t_dir, "forestry_aptitude")
        if not ok:
            log.write("FAIL: upload\n")
            print(f"  [{t_id}] FAIL upload")
            return False

    elapsed = time.time() - t0
    print(f"  [{t_id}] OK in {elapsed/60:.1f}min")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--only", default=None)
    args = ap.parse_args()

    territories = READY_TERRITORIES
    if args.only:
        territories = [t.strip() for t in args.only.split(",") if t.strip() in READY_TERRITORIES]

    log_dir = OUTPUT_DIR
    print(f"Forestry PY all pipeline: {len(territories)} territories, {args.workers} workers")
    print(f"Territories: {territories}")

    ok_list, fail_list = [], []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_one, t, log_dir): t for t in territories}
        for f in as_completed(futures):
            t = futures[f]
            try:
                result = f.result()
                if result:
                    ok_list.append(t)
                else:
                    fail_list.append(t)
            except Exception as e:
                print(f"  [{t}] EXCEPTION: {e}")
                fail_list.append(t)

    print(f"\n=== FORESTRY RESULTS ===")
    print(f"OK ({len(ok_list)}): {ok_list}")
    print(f"FAIL ({len(fail_list)}): {fail_list}")
    print("\nNext: update config.ts coverage for forestry_aptitude")


if __name__ == "__main__":
    main()
