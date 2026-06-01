"""
Launch all missing GEE exports for the 15 new PY territories.

All tasks submitted with --no-wait so they run in parallel on GEE.
After launching, run process_py_current.py to process carbon + pm25
(rasters already in GCS).

Usage:
  python pipeline/launch_py_gee_all.py                 # all exports
  python pipeline/launch_py_gee_all.py --only hansen   # only Hansen
  python pipeline/launch_py_gee_all.py --dry-run       # print only
  python pipeline/launch_py_gee_all.py --territory concepcion_py  # single

Exports launched:
  - Hansen lossyear + treecover2000  → deforestation_dynamics
  - Oxford friction + cities access  → accessibility
  - SDM covariates (9 bands)         → forestry_aptitude
  - MapBiomas PY collection 1        → land_use
"""
import argparse
import subprocess
import sys

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

# GEE export commands per analysis group
EXPORT_GROUPS = {
    "hansen": "python pipeline/gee_export_hansen_loss.py --territory {t} --gcs --no-wait",
    "lv":     "python pipeline/gee_export_location_value.py --territory {t} --gcs --no-wait",
    "sdm":    "python pipeline/gee_export_sdm_covariates.py --territory {t}",
    "mapbiomas": "python pipeline/gee_export_mapbiomas.py --territory {t} --gcs --no-wait",
}


def run(cmd: str, dry_run: bool) -> int:
    print(f"  $ {cmd}")
    if dry_run:
        return 0
    return subprocess.run(cmd, shell=True).returncode


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None,
                    help="Comma-separated export groups: hansen,lv,sdm,mapbiomas")
    ap.add_argument("--territory", default=None,
                    help="Single territory (default: all 15)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    territories = [args.territory] if args.territory else PY_TERRITORIES
    groups = args.only.split(",") if args.only else list(EXPORT_GROUPS.keys())

    launched = 0
    failed = 0
    for group in groups:
        if group not in EXPORT_GROUPS:
            print(f"Unknown group: {group}. Available: {list(EXPORT_GROUPS.keys())}")
            continue
        template = EXPORT_GROUPS[group]
        print(f"\n{'='*60}")
        print(f"  Group: {group} — {len(territories)} territories")
        print(f"{'='*60}")
        for t in territories:
            cmd = template.format(t=t)
            rc = run(cmd, args.dry_run)
            if rc == 0:
                launched += 1
            else:
                print(f"  WARN: {group}/{t} returned exit {rc}")
                failed += 1

    print(f"\nDone: {launched} tasks submitted, {failed} failed.")
    print("Monitor at: https://code.earthengine.google.com/tasks")
    print("Next: python pipeline/process_py_current.py")


if __name__ == "__main__":
    main()
