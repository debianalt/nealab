"""
Run a pipeline analysis in parallel across all 15 PY territories.
Spawns one subprocess per territory simultaneously.

Usage:
  python pipeline/process_py_parallel.py --analysis pm25
  python pipeline/process_py_parallel.py --analysis carbon
  python pipeline/process_py_parallel.py --analysis deforestation  # after GEE finishes
"""
import argparse
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

PY_TERRITORIES = [
    "concepcion_py", "san_pedro_py", "cordillera_py", "guaira_py",
    "caaguazu_py", "caazapa_py", "misiones_py", "paraguari_py",
    "central_py", "neembucu_py", "amambay_py", "canindeyu_py",
    "presidente_hayes_py", "boqueron_py", "alto_paraguay_py",
]

# Command templates per analysis
ANALYSIS_CMDS = {
    "carbon": [
        "python process_carbon_to_h3.py --territory {t} --mode comparable",
        "python split_by_admin.py --territory {t} --only carbon_stock",
        "npx wrangler r2 object put neahub/data/{prefix}sat_carbon_stock.parquet "
        "--file ../output/{t}/sat_carbon_stock.parquet --remote",
    ],
    "pm25": [
        "python process_pm25_annual_to_h3.py --territory {t}",
        "python compute_pm25_drivers.py --territory {t} --mode comparable",
        "python split_by_admin.py --territory {t} --only pm25_drivers",
        "npx wrangler r2 object put neahub/data/{prefix}sat_pm25_drivers.parquet "
        "--file ../output/{t}/sat_pm25_drivers.parquet --remote",
    ],
    "deforestation": [
        "python process_raster_to_h3.py --territory {t} --analysis deforestation_dynamics --mode comparable",
        "python split_by_admin.py --territory {t} --only deforestation_dynamics",
        "npx wrangler r2 object put neahub/data/{prefix}sat_deforestation_dynamics.parquet "
        "--file ../output/{t}/sat_deforestation_dynamics.parquet --remote",
    ],
    "accessibility": [
        "python compute_accessibility_h3.py --territory {t}",
        "python split_by_admin.py --territory {t} --only accessibility",
        "npx wrangler r2 object put neahub/data/{prefix}sat_accessibility.parquet "
        "--file ../output/{t}/sat_accessibility.parquet --remote",
    ],
    "land_use": [
        "python process_mapbiomas_to_h3.py --territory {t}",
        "python split_by_admin.py --territory {t} --only land_use",
        "npx wrangler r2 object put neahub/data/{prefix}sat_land_use.parquet "
        "--file ../output/{t}/sat_land_use.parquet --remote",
    ],
    "forestry": [
        "python compute_forestry_sdm.py --territory {t}",
        "python split_by_admin.py --territory {t} --only forestry_aptitude",
        "npx wrangler r2 object put neahub/data/{prefix}sat_forestry_aptitude.parquet "
        "--file ../output/{t}/sat_forestry_aptitude.parquet --remote",
    ],
}


def run_territory(t: str, analysis: str, log_dir: str) -> tuple[str, bool]:
    """Run all steps for one territory. Returns (territory, success)."""
    from config import get_territory
    territory = get_territory(t)
    prefix = territory['output_prefix']  # 'concepcion_py/'
    log_path = os.path.join(log_dir, f"{analysis}_{t}.log")

    cmds = ANALYSIS_CMDS[analysis]
    with open(log_path, "w") as logf:
        logf.write(f"=== {t} / {analysis} ===\n")
        for cmd_template in cmds:
            cmd = cmd_template.format(t=t, prefix=prefix)
            logf.write(f"\n$ {cmd}\n")
            logf.flush()
            rc = subprocess.run(
                cmd, shell=True, cwd=SCRIPT_DIR,
                stdout=logf, stderr=subprocess.STDOUT
            ).returncode
            if rc != 0:
                logf.write(f"\nFAILED (exit {rc})\n")
                return t, False
    return t, True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--analysis", required=True, choices=list(ANALYSIS_CMDS.keys()))
    ap.add_argument("--workers", type=int, default=4,
                    help="Max parallel territories (default: 4)")
    ap.add_argument("--only", default=None, help="Comma-separated territory IDs")
    args = ap.parse_args()

    territories = PY_TERRITORIES
    if args.only:
        territories = [t.strip() for t in args.only.split(',') if t.strip() in PY_TERRITORIES]

    log_dir = os.path.join(SCRIPT_DIR, "output")
    os.makedirs(log_dir, exist_ok=True)

    print(f"Running {args.analysis} for {len(territories)} territories "
          f"({args.workers} parallel)")
    print(f"Logs: pipeline/output/{args.analysis}_<territory>.log\n")

    t0 = time.time()
    results = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_territory, t, args.analysis, log_dir): t
                   for t in territories}
        for future in as_completed(futures):
            t, ok = future.result()
            results[t] = ok
            elapsed = time.time() - t0
            status = "OK" if ok else "FAIL"
            print(f"  {status} {t} ({elapsed:.0f}s elapsed)")

    done = sum(1 for ok in results.values() if ok)
    failed = [t for t, ok in results.items() if not ok]
    print(f"\nDone: {done}/{len(territories)} OK in {time.time()-t0:.0f}s")
    if failed:
        print(f"Failed: {failed}")
        print(f"Check logs in pipeline/output/{args.analysis}_<territory>.log")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
