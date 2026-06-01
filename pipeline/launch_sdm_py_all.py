"""
Launch GEE SDM covariate exports for all 15 PY territories.

Submits 9 composites per territory (135 tasks total) and exits immediately.
Tasks run on GEE servers (~4-8h). Follow up with monitor_and_process_py.py
to process and upload when GCS files are ready.

Usage:
  python pipeline/launch_sdm_py_all.py
  python pipeline/launch_sdm_py_all.py --only concepcion_py,san_pedro_py
"""
from __future__ import annotations
import argparse
import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

PY_TERRITORIES = [
    "concepcion_py",
    "san_pedro_py",
    "cordillera_py",
    "guaira_py",
    "caaguazu_py",
    "caazapa_py",
    "misiones_py",
    "paraguari_py",
    "central_py",
    "neembucu_py",
    "amambay_py",
    "canindeyu_py",
    "presidente_hayes_py",
    "boqueron_py",
    "alto_paraguay_py",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None, help="Comma-separated territory IDs")
    args = ap.parse_args()

    territories = PY_TERRITORIES
    if args.only:
        only = {t.strip() for t in args.only.split(",")}
        territories = [t for t in PY_TERRITORIES if t in only]

    total_tasks = 0
    for t in territories:
        print(f"\n--- {t} ---")
        result = subprocess.run(
            [sys.executable, os.path.join(SCRIPT_DIR, "gee_export_sdm_covariates.py"),
             "--territory", t],
            cwd=SCRIPT_DIR,
        )
        if result.returncode != 0:
            print(f"  WARNING: gee_export_sdm_covariates.py exited {result.returncode} for {t}")
        else:
            total_tasks += 9  # 9 composites per territory

    print(f"\nSubmitted ~{total_tasks} SDM tasks across {len(territories)} territories.")
    print("Monitor at: https://code.earthengine.google.com/tasks")
    print("\nWhen GCS files are ready, run:")
    print("  python pipeline/monitor_and_process_py.py --timeout-hours 12 --workers 4")
    return 0


if __name__ == "__main__":
    sys.exit(main())
