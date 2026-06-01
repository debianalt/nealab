"""
Download all sat_pm25_YYYY.tif rasters for all PY territories in parallel.
4 workers = 4 territories downloading simultaneously.

Usage:
  python pipeline/download_pm25_py.py
  python pipeline/download_pm25_py.py --only concepcion_py,san_pedro_py
  python pipeline/download_pm25_py.py --workers 6
"""
import argparse
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, GCS_BUCKET, get_territory

PY_TERRITORIES = [
    "concepcion_py", "san_pedro_py", "cordillera_py", "guaira_py",
    "caaguazu_py", "caazapa_py", "misiones_py", "paraguari_py",
    "central_py", "neembucu_py", "amambay_py", "canindeyu_py",
    "presidente_hayes_py", "boqueron_py", "alto_paraguay_py",
]

YEARS = list(range(1998, 2023))


def download_territory(t_id: str) -> tuple[str, int, int]:
    territory = get_territory(t_id)
    t_dir = os.path.join(OUTPUT_DIR, territory['output_prefix'].rstrip('/'))
    os.makedirs(t_dir, exist_ok=True)

    needed = [yr for yr in YEARS
              if not os.path.exists(os.path.join(t_dir, f"sat_pm25_{yr}.tif"))]

    if not needed:
        print(f"  {t_id}: all {len(YEARS)} years already local")
        return t_id, 0, 0

    print(f"  {t_id}: downloading {len(needed)} years...")
    failed = []
    for yr in needed:
        gcs = f"gs://{GCS_BUCKET}/satellite/{t_id}/sat_pm25_{yr}.tif"
        dst = os.path.join(t_dir, f"sat_pm25_{yr}.tif")
        rc = subprocess.run(
            f"gcloud storage cp {gcs} {dst}",
            shell=True, capture_output=True
        ).returncode
        if rc != 0:
            failed.append(yr)

    ok = len(needed) - len(failed)
    print(f"  {t_id}: {ok}/{len(needed)} downloaded" +
          (f", {len(failed)} failed: {failed[:3]}..." if failed else ""))
    return t_id, ok, len(failed)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None)
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()

    territories = PY_TERRITORIES
    if args.only:
        territories = [t.strip() for t in args.only.split(',')
                       if t.strip() in PY_TERRITORIES]

    print(f"Downloading pm25 rasters for {len(territories)} territories "
          f"({args.workers} parallel)...")

    total_ok = total_fail = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(download_territory, t): t for t in territories}
        for future in futures:
            t, ok, fail = future.result()
            total_ok += ok
            total_fail += fail

    print(f"\nDone: {total_ok} files downloaded, {total_fail} failed")
    if total_fail == 0:
        print("Next: python pipeline/process_py_parallel.py --analysis pm25 --workers 4")


if __name__ == "__main__":
    main()
