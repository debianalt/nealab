"""
Launch S1 SAR + JRC flood exports for all new PY territories.

Submits without waiting — tasks run on GEE's servers. Follow up with
run_flood_update.py --skip-gee per territory once GCS files are ready.

Usage:
  python pipeline/launch_flood_py_all.py                  # all 15
  python pipeline/launch_flood_py_all.py --only s1        # only S1
  python pipeline/launch_flood_py_all.py --only jrc       # only JRC
  python pipeline/launch_flood_py_all.py --territory concepcion_py
"""
from __future__ import annotations
import argparse
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from gee_flood_detection import authenticate, launch_exports

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
    ap.add_argument("--only", default="all", choices=["s1", "jrc", "all"])
    ap.add_argument("--territory", default=None, help="Single territory ID")
    args = ap.parse_args()

    territories = [args.territory] if args.territory else PY_TERRITORIES

    authenticate()
    print("GEE auth OK.")

    all_tasks = []
    for tid in territories:
        print(f"\n--- {tid} ---")

        if args.only in ("s1", "all"):
            tasks_s1 = launch_exports(
                territory_id=tid,
                historical=True,
                current=True,
                jrc=False,
            )
            for t in tasks_s1:
                print(f"  S1 task: {t.id}")
            all_tasks.extend(tasks_s1)

        if args.only in ("jrc", "all"):
            tasks_jrc = launch_exports(
                territory_id=tid,
                historical=False,
                current=False,
                jrc=True,
            )
            for t in tasks_jrc:
                print(f"  JRC task: {t.id}")
            all_tasks.extend(tasks_jrc)

    print(f"\nTotal flood tasks launched: {len(all_tasks)}")
    print("Monitor at: https://code.earthengine.google.com/tasks")
    print("\nWhen GCS files are ready (~4-6h), process per territory:")
    for tid in territories:
        print(f"  python pipeline/run_flood_update.py --territory {tid} --skip-gee --upload")
    return 0


if __name__ == "__main__":
    sys.exit(main())
