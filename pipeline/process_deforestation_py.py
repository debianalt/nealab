"""
Download Hansen rasters + process deforestation_dynamics for all PY territories.
Runs 4 territories in parallel.

Usage:
  python pipeline/process_deforestation_py.py
  python pipeline/process_deforestation_py.py --only concepcion_py,san_pedro_py
"""
import argparse
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, GCS_BUCKET, get_territory

PY_TERRITORIES = [
    "concepcion_py", "san_pedro_py", "cordillera_py", "guaira_py",
    "caaguazu_py", "caazapa_py", "misiones_py", "paraguari_py",
    "central_py", "neembucu_py", "amambay_py", "canindeyu_py",
    "presidente_hayes_py", "boqueron_py", "alto_paraguay_py",
]


def run(cmd, cwd=SCRIPT_DIR, log=None):
    if log:
        log.write(f"\n$ {cmd}\n"); log.flush()
    result = subprocess.run(cmd, shell=True, cwd=cwd,
                            stdout=log, stderr=subprocess.STDOUT)
    return result.returncode == 0


def process_territory(t_id: str) -> tuple[str, bool]:
    territory = get_territory(t_id)
    t_prefix = territory['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, t_prefix.rstrip('/'))
    os.makedirs(t_dir, exist_ok=True)

    log_path = os.path.join(OUTPUT_DIR, f"deforestation_{t_id}.log")
    with open(log_path, "w") as log:
        log.write(f"=== {t_id} / deforestation_dynamics ===\n")

        # Download Hansen rasters if not present
        for fname in ("hansen_lossyear.tif", "hansen_treecover2000.tif"):
            local = os.path.join(t_dir, fname)
            if not os.path.exists(local):
                gcs = f"gs://{GCS_BUCKET}/satellite/{t_id}/{fname}"
                log.write(f"\nDownloading {fname}...\n"); log.flush()
                if not run(f"gcloud storage cp {gcs} {local}", log=log):
                    log.write(f"FAILED: {fname} not in GCS\n")
                    return t_id, False

        # Process hansen → H3 (comparable mode for cross-territory consistency)
        if not run(f"python process_hansen_to_h3.py --territory {t_id} --mode comparable", log=log):
            return t_id, False

        # Split by admin
        if not run(f"python split_by_admin.py --territory {t_id} --only deforestation_dynamics", log=log):
            return t_id, False

        # Upload global parquet
        parquet = os.path.join(t_dir, "sat_deforestation_dynamics.parquet")
        if not os.path.exists(parquet):
            log.write("ERROR: parquet not generated\n")
            return t_id, False

        r2_key = f"neahub/data/{t_prefix}sat_deforestation_dynamics.parquet"
        if not run(f"npx wrangler r2 object put {r2_key} --file {parquet} --remote", log=log):
            return t_id, False

        # Upload per-dpto parquets
        dpto_dir = os.path.join(t_dir, "sat_dpto")
        if os.path.exists(dpto_dir):
            for f in os.listdir(dpto_dir):
                if f.startswith("sat_deforestation_dynamics_") and f.endswith(".parquet"):
                    r2_dpto = f"neahub/data/{t_prefix}sat_dpto/{f}"
                    run(f"npx wrangler r2 object put {r2_dpto} --file {os.path.join(dpto_dir, f)} --remote", log=log)

    return t_id, True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None)
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()

    territories = PY_TERRITORIES
    if args.only:
        territories = [t.strip() for t in args.only.split(',')
                       if t.strip() in PY_TERRITORIES]

    print(f"deforestation_dynamics: {len(territories)} territories, {args.workers} workers")
    print(f"Logs: pipeline/output/deforestation_<territory>.log\n")

    t0 = time.time()
    results = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_territory, t): t for t in territories}
        for future in as_completed(futures):
            t, ok = future.result()
            results[t] = ok
            print(f"  {'OK' if ok else 'FAIL'} {t} ({time.time()-t0:.0f}s)")

    ok_count = sum(v for v in results.values())
    failed = [t for t, ok in results.items() if not ok]
    print(f"\nDone: {ok_count}/{len(territories)} in {time.time()-t0:.0f}s")
    if failed:
        print(f"Failed: {failed}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
