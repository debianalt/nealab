"""
Flood risk pipeline for PY territories.
Polls GCS for completed flood exports (handles sharded files),
downloads, processes to H3, and uploads to R2.

Usage:
  python pipeline/run_flood_py_all.py               # check all, process ready
  python pipeline/run_flood_py_all.py --once         # single pass, no loop
  python pipeline/run_flood_py_all.py --only concepcion_py,misiones_py
  python pipeline/run_flood_py_all.py --force        # process even if no recurrence file
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
POLL_SECS = 180


def run(cmd, cwd=None, log=None):
    kw = {"stdout": log, "stderr": subprocess.STDOUT} if log else {}
    return subprocess.run(cmd, shell=True, cwd=cwd or SCRIPT_DIR, **kw).returncode == 0


def gcs_ls(pattern):
    r = subprocess.run(f"gcloud storage ls {pattern}", shell=True, capture_output=True)
    return r.returncode == 0, r.stdout.decode().strip().splitlines()


def check_flood_ready(t_id):
    """Check if flood_recurrence_historical* exists in GCS (handles shards)."""
    ok, files = gcs_ls(f"gs://{GCS_BUCKET}/flood/{t_id}/flood_recurrence_historical*.tif")
    return ok and len(files) > 0


def check_flood_complete(t_id):
    """Check if flood_current and recurrence are both available."""
    ok_rec, _ = gcs_ls(f"gs://{GCS_BUCKET}/flood/{t_id}/flood_recurrence_historical*.tif")
    ok_cur, _ = gcs_ls(f"gs://{GCS_BUCKET}/flood/{t_id}/flood_current_*.tif")
    ok_jrc, _ = gcs_ls(f"gs://{GCS_BUCKET}/flood/{t_id}/jrc_occurrence*.tif")
    return ok_rec and ok_cur and ok_jrc


def process_flood(t_id):
    territory = get_territory(t_id)
    t_prefix = territory['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, t_prefix.rstrip('/'))
    os.makedirs(t_dir, exist_ok=True)
    log_path = os.path.join(OUTPUT_DIR, f"flood_full_{t_id}.log")
    print(f"  [{t_id}] Processing flood -> {log_path}")

    with open(log_path, "w") as log:
        log.write(f"=== {t_id} / flood_risk ===\n")

        # Download all flood TIFs from GCS
        flood_dir = f"gs://{GCS_BUCKET}/flood/{t_id}/"
        log.write(f"\nDownloading from {flood_dir}\n")
        rc = run(f"gcloud storage cp \"{flood_dir}*.tif\" {t_dir}/", log=log)
        if not rc:
            log.write("WARN: gcloud cp returned non-zero (may be ok if files already local)\n")

        # Check local files
        tifs = [f for f in os.listdir(t_dir) if f.endswith(".tif") and ("flood" in f.lower() or "jrc" in f.lower())]
        log.write(f"Local flood TIFs: {sorted(tifs)}\n")
        if not tifs:
            log.write("FAIL: No local flood TIFs\n")
            return False

        # Mosaic sharded flood files (GEE splits large exports into numbered tiles)
        log.write("\n--- Mosaic flood shards ---\n")
        run(f"python mosaic_flood_shards.py {t_dir}", log=log)

        # Run flood update pipeline (upload is built-in at step 7)
        log.write("\n--- run_flood_update.py ---\n")
        rc = run(f"python run_flood_update.py --territory {t_id} --skip-gee --min-hexagons 10000", log=log)
        if not rc:
            log.write(f"FAIL: run_flood_update rc={rc}\n")
            return False

    print(f"  [{t_id}] flood OK")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="Single pass, no loop")
    ap.add_argument("--only", default=None)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--timeout-hours", type=float, default=10)
    args = ap.parse_args()

    territories = ALL_PY
    if args.only:
        territories = [t.strip() for t in args.only.split(",") if t.strip() in ALL_PY]

    done = set()
    deadline = time.time() + args.timeout_hours * 3600

    print(f"Flood PY pipeline: {len(territories)} territories, {args.workers} workers")

    while time.time() < deadline:
        ready = [t for t in territories if t not in done and
                 (args.force or check_flood_complete(t))]

        if ready:
            print(f"  Ready to process: {ready}")
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futs = {pool.submit(process_flood, t): t for t in ready}
                for f in as_completed(futs):
                    t = futs[f]
                    try:
                        ok = f.result()
                        if ok:
                            done.add(t)
                            print(f"  DONE {t} ({len(done)}/{len(territories)})")
                        else:
                            print(f"  FAIL {t}")
                    except Exception as e:
                        print(f"  ERROR {t}: {e}")

        remaining = [t for t in territories if t not in done]
        if not remaining:
            print(f"All {len(territories)} territories done!")
            break

        print(f"  Progress: {len(done)}/{len(territories)} done. Remaining: {remaining[:3]}...")

        if args.once:
            print("--once: exiting after single pass")
            break

        print(f"  Sleeping {POLL_SECS}s...")
        time.sleep(POLL_SECS)

    print(f"\n=== FLOOD RESULTS ===")
    print(f"Done ({len(done)}): {sorted(done)}")
    remaining = [t for t in territories if t not in done]
    if remaining:
        print(f"Remaining ({len(remaining)}): {remaining}")
        print("Next: run again when GEE flood exports complete")
    if done:
        print("Next: update config.ts flood_risk coverage + deploy")


if __name__ == "__main__":
    main()
