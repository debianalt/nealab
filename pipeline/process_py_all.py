"""
Process all new PY territories after GEE exports finish.
Downloads rasters from GCS, converts to H3, splits by admin, uploads to R2.

Usage:
  # Check GEE status first:
  python pipeline/process_py_all.py --check

  # Download from GCS + process to H3 + split + upload:
  python pipeline/process_py_all.py --all

  # Skip GEE download (already downloaded):
  python pipeline/process_py_all.py --skip-download --all

  # Single territory:
  python pipeline/process_py_all.py --only concepcion_py --all
"""
import argparse
import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

PY_TERRITORIES = [
    "concepcion_py", "san_pedro_py", "cordillera_py", "guaira_py",
    "caaguazu_py", "caazapa_py", "misiones_py", "paraguari_py",
    "central_py", "neembucu_py", "amambay_py", "canindeyu_py",
    "presidente_hayes_py", "boqueron_py", "alto_paraguay_py",
]

ANALYSES = "environmental_risk,climate_comfort,green_capital,change_pressure,forest_health,agri_potential"

GCS_BUCKET = "spatia-satellite"


def run(cmd: str) -> int:
    print(f"  $ {cmd}")
    return subprocess.run(cmd, shell=True).returncode


def check_gcs(territories: list[str]):
    """Check which GCS exports are done."""
    print("Checking GCS for completed exports...")
    for t in territories:
        analyses = ANALYSES.split(",")
        done = 0
        for a in analyses:
            gcs_path = f"gs://{GCS_BUCKET}/satellite/{t}/sat_{a}_raster.tif"
            r = subprocess.run(f"gcloud storage ls {gcs_path}", shell=True,
                               capture_output=True, text=True)
            if r.returncode == 0:
                done += 1
        print(f"  {t}: {done}/{len(analyses)} analyses ready in GCS")


def process_territory(t: str, skip_download: bool, upload: bool) -> bool:
    """Run full pipeline for one territory."""
    cmd = (f"python pipeline/run_itapua_pipeline.py --territory {t} "
           f"--mode comparable {'--skip-download' if skip_download else ''} "
           f"{'--upload' if upload else ''}")
    rc = run(cmd)
    return rc == 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="Check GCS export status")
    ap.add_argument("--all", action="store_true", help="Run full pipeline")
    ap.add_argument("--skip-download", action="store_true")
    ap.add_argument("--upload", action="store_true", help="Upload to R2 after processing")
    ap.add_argument("--only", default=None)
    args = ap.parse_args()

    territories = PY_TERRITORIES
    if args.only:
        territories = [t.strip() for t in args.only.split(",") if t.strip() in PY_TERRITORIES]

    if args.check:
        check_gcs(territories)
        return

    if args.all:
        for t in territories:
            print(f"\n{'='*60}\n  Processing: {t}\n{'='*60}")
            process_territory(t, args.skip_download, args.upload)


if __name__ == "__main__":
    main()
