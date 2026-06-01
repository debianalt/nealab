"""
Process carbon_stock and pm25_drivers for all new PY territories.
Rasters for both already exist in GCS; downloads, processes, splits, uploads.

Usage:
  python pipeline/process_py_current.py                   # all 15
  python pipeline/process_py_current.py --only concepcion_py
  python pipeline/process_py_current.py --skip-download   # if rasters already local
  python pipeline/process_py_current.py --analyses carbon  # only carbon
"""
import argparse
import os
import subprocess
import sys
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

PM25_YEARS = list(range(1998, 2023))  # 1998-2022
GCS_SAT_PREFIX = f"gs://{GCS_BUCKET}/satellite"


def run(desc: str, cmd: str) -> bool:
    print(f"\n  [{desc}] $ {cmd}")
    rc = subprocess.run(cmd, shell=True, cwd=SCRIPT_DIR).returncode
    if rc != 0:
        print(f"  FAILED (exit {rc})")
    return rc == 0


def download_carbon(t_id: str, t_dir: str, skip: bool) -> bool:
    """Download sat_carbon_stock_raster.tif from GCS."""
    local = os.path.join(t_dir, "sat_carbon_stock_raster.tif")
    if skip or os.path.exists(local):
        print(f"  carbon raster: {'SKIP' if skip else 'EXISTS'}")
        return True
    gcs = f"{GCS_SAT_PREFIX}/{t_id}/sat_carbon_stock_raster.tif"
    return run(f"download carbon", f"gcloud storage cp {gcs} {local}")


def download_pm25(t_id: str, t_dir: str, skip: bool) -> bool:
    """Download sat_pm25_YYYY.tif annual rasters from GCS."""
    if skip:
        print(f"  pm25 rasters: SKIP (--skip-download)")
        return True
    needed = []
    for yr in PM25_YEARS:
        f = os.path.join(t_dir, f"sat_pm25_{yr}.tif")
        if not os.path.exists(f):
            needed.append(yr)
    if not needed:
        print(f"  pm25 rasters: ALL EXISTS ({len(PM25_YEARS)} years)")
        return True
    print(f"  pm25 rasters: downloading {len(needed)} years...")
    gcs_prefix = f"{GCS_SAT_PREFIX}/{t_id}/sat_pm25_"
    rc = subprocess.run(
        f"gcloud storage cp '{gcs_prefix}*.tif' {t_dir}/",
        shell=True, cwd=SCRIPT_DIR
    ).returncode
    if rc != 0:
        print(f"  pm25 download failed (rc={rc})")
        return False
    return True


def process_territory(t_id: str, skip_download: bool, analyses: list, upload: bool) -> dict:
    """Run full processing for one territory. Returns dict of results."""
    territory = get_territory(t_id)
    t_prefix = territory['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, t_prefix.rstrip('/'))

    if not os.path.exists(os.path.join(t_dir, 'hexagons.geojson')):
        print(f"  SKIP {t_id}: no hexagons.geojson (territory not initialized)")
        return {'skip': True}

    results = {}

    # ── carbon_stock ──────────────────────────────────────────────────────────
    if 'carbon' in analyses or 'all' in analyses:
        print(f"\n  --- carbon_stock ---")
        carbon_parquet = os.path.join(t_dir, 'sat_carbon_stock.parquet')

        dl_ok = download_carbon(t_id, t_dir, skip_download)
        if dl_ok and not os.path.exists(os.path.join(t_dir, 'sat_carbon_stock_raster.tif')):
            print(f"  carbon raster missing locally, skipping")
            dl_ok = False

        if dl_ok:
            ok = run("carbon H3", f"python process_carbon_to_h3.py --territory {t_id} --mode comparable")
            if ok:
                ok2 = run("carbon split", f"python split_by_admin.py --territory {t_id} --only carbon_stock")
                results['carbon_stock'] = ok2
                if upload and ok2:
                    _upload_analysis(t_id, t_prefix, t_dir, 'carbon_stock')
            else:
                results['carbon_stock'] = False

    # ── pm25_drivers ─────────────────────────────────────────────────────────
    if 'pm25' in analyses or 'all' in analyses:
        print(f"\n  --- pm25_drivers ---")
        panel_parquet = os.path.join(t_dir, 'pm25_annual_panel.parquet')

        dl_ok = download_pm25(t_id, t_dir, skip_download)
        if dl_ok:
            if not os.path.exists(panel_parquet):
                ok = run("pm25 panel", f"python process_pm25_annual_to_h3.py --territory {t_id}")
            else:
                print(f"  pm25 panel: EXISTS, skipping raster→H3")
                ok = True

            if ok:
                ok2 = run("pm25 drivers", f"python compute_pm25_drivers.py --territory {t_id} --mode comparable")
                if ok2:
                    ok3 = run("pm25 split", f"python split_by_admin.py --territory {t_id} --only pm25_drivers")
                    results['pm25_drivers'] = ok3
                    if upload and ok3:
                        _upload_analysis(t_id, t_prefix, t_dir, 'pm25_drivers')
                else:
                    results['pm25_drivers'] = False

    return results


def _upload_analysis(t_id: str, t_prefix: str, t_dir: str, analysis: str):
    """Upload global + per-district parquets to R2."""
    r2_prefix = f"neahub/data/{t_prefix}"
    # Global
    global_parquet = os.path.join(t_dir, f"sat_{analysis}.parquet")
    if os.path.exists(global_parquet):
        run(f"upload {analysis} global",
            f"npx wrangler r2 object put {r2_prefix}sat_{analysis}.parquet "
            f"--file {global_parquet} --remote")
    # Per-district
    dpto_dir = os.path.join(t_dir, 'sat_dpto')
    if os.path.exists(dpto_dir):
        for f in os.listdir(dpto_dir):
            if f.startswith(f"sat_{analysis}_") and f.endswith('.parquet'):
                local_f = os.path.join(dpto_dir, f)
                run(f"upload {analysis} dpto/{f}",
                    f"npx wrangler r2 object put {r2_prefix}sat_dpto/{f} "
                    f"--file {local_f} --remote")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None, help="Comma-separated territory IDs")
    ap.add_argument("--skip-download", action="store_true", help="Use local rasters (already downloaded)")
    ap.add_argument("--analyses", default="all",
                    help="Analyses to run: all | carbon | pm25 | carbon,pm25")
    ap.add_argument("--upload", action="store_true", help="Upload to R2 after processing")
    args = ap.parse_args()

    territories = PY_TERRITORIES
    if args.only:
        territories = [t.strip() for t in args.only.split(',') if t.strip() in PY_TERRITORIES]

    analyses = [a.strip() for a in args.analyses.split(',')] if args.analyses != 'all' else ['all']

    print(f"Processing {len(territories)} territories: {', '.join(territories)}")
    print(f"Analyses: {analyses}")
    print(f"Upload: {args.upload}")

    summary = {}
    for t in territories:
        t0 = time.time()
        print(f"\n{'='*60}")
        print(f"  Territory: {t}")
        print(f"{'='*60}")
        res = process_territory(t, args.skip_download, analyses, args.upload)
        elapsed = time.time() - t0
        summary[t] = {'results': res, 'elapsed_s': round(elapsed)}
        print(f"  Done in {elapsed:.0f}s: {res}")

    print(f"\n{'='*60}")
    print("  SUMMARY")
    print(f"{'='*60}")
    for t, info in summary.items():
        print(f"  {t}: {info['results']} ({info['elapsed_s']}s)")


if __name__ == '__main__':
    main()
