"""
Re-export flood rasters (JRC occurrence/recurrence/seasonality + S1 historical +
S1 current) for the 17 territories whose flood coverage had contiguous NaN holes.

Root cause (2026-06-07 audit): export bbox was smaller than the territory hex
grid → JRC/S1 rasters clipped → 6-41% of hexes outside the raster footprint.
The 14 too-small bboxes were enlarged in config.py (commit). The 3 bbox-OK ones
(itapua_py, corrientes, alto_parana_py) were clipped at download/mosaic time and
just need a clean re-export. alto_parana_py had NO jrc raster at all.

Submits without waiting (tasks run on GEE servers). Follow up with
run_flood_py_all.py / run_flood_update.py --skip-gee once GCS files are ready.

Usage:
  python pipeline/launch_flood_reexport.py            # launch all 17
  python pipeline/launch_flood_reexport.py --dry-run  # list, don't submit
"""
from __future__ import annotations
import argparse, os, sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from gee_flood_detection import authenticate, launch_exports

# 14 too-small bbox (enlarged) + 3 bbox-OK-but-clipped
TERRITORIES = [
    # PY too-small bbox
    "cordillera_py", "guaira_py", "caaguazu_py", "caazapa_py", "misiones_py",
    "central_py", "neembucu_py", "amambay_py", "canindeyu_py",
    "presidente_hayes_py", "boqueron_py", "alto_paraguay_py",
    # AR too-small bbox
    "chaco", "formosa",
    # bbox OK but raster was clipped at download/mosaic (alto_parana had no JRC)
    "itapua_py", "corrientes", "alto_parana_py",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.dry_run:
        for t in TERRITORIES:
            print(f"  would launch jrc+historical+current: {t}")
        print(f"\n{len(TERRITORIES)} territories")
        return 0

    authenticate()
    print(f"GEE auth OK. Launching {len(TERRITORIES)} territories.\n")
    total = 0
    for tid in TERRITORIES:
        print(f"--- {tid} ---")
        try:
            tasks = launch_exports(territory_id=tid, historical=True,
                                   current=True, jrc=True)
            total += len(tasks)
        except Exception as e:  # noqa: BLE001
            print(f"  ERROR launching {tid}: {e}")
    print(f"\nSubmitted ~{total} export task(s) across {len(TERRITORIES)} territories.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
