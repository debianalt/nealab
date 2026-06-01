"""
Monitor GCS and process PY layers as GEE exports complete.
Polls every 3 min. Processes + uploads each territory as soon as its
rasters land. Exits when all configured layers are done or timeout.

Usage:
  python pipeline/monitor_and_process_py.py
  python pipeline/monitor_and_process_py.py --timeout-hours 8
"""
import argparse
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, GCS_BUCKET, get_territory

PY_EASTERN = [
    "concepcion_py", "san_pedro_py", "cordillera_py", "guaira_py",
    "caaguazu_py", "caazapa_py", "misiones_py", "paraguari_py",
    "central_py", "neembucu_py", "amambay_py", "canindeyu_py",
    "presidente_hayes_py",
]
PY_CHACO = ["boqueron_py", "alto_paraguay_py"]
ALL_PY = PY_EASTERN + PY_CHACO

POLL_INTERVAL = 180  # 3 min


def gcs_exists(path: str) -> bool:
    r = subprocess.run(f"gcloud storage ls {path}", shell=True,
                       capture_output=True)
    return r.returncode == 0


def run(cmd: str, log=None) -> bool:
    kw = {"stdout": log, "stderr": subprocess.STDOUT} if log else {}
    return subprocess.run(cmd, shell=True, cwd=SCRIPT_DIR, **kw).returncode == 0


def upload_parquet(t_prefix: str, t_dir: str, analysis: str):
    parquet = os.path.join(t_dir, f"sat_{analysis}.parquet")
    if not os.path.exists(parquet):
        return
    run(f"npx wrangler r2 object put neahub/data/{t_prefix}sat_{analysis}.parquet "
        f"--file {parquet} --remote")
    dpto_dir = os.path.join(t_dir, "sat_dpto")
    if os.path.exists(dpto_dir):
        for f in os.listdir(dpto_dir):
            if f.startswith(f"sat_{analysis}_") and f.endswith(".parquet"):
                run(f"npx wrangler r2 object put neahub/data/{t_prefix}sat_dpto/{f} "
                    f"--file {os.path.join(dpto_dir, f)} --remote")


def process_lv(t_id: str) -> bool:
    territory = get_territory(t_id)
    t_prefix = territory['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, t_prefix.rstrip('/'))
    log_path = os.path.join(OUTPUT_DIR, f"accessibility_{t_id}.log")
    with open(log_path, "w") as log:
        log.write(f"=== {t_id} / accessibility ===\n")
        for fname in ("lv_friction.tif", "lv_cities_access.tif",
                      "lv_dem.tif", "lv_slope.tif", "lv_healthcare.tif",
                      "lv_viirs_annual.tif"):
            local = os.path.join(t_dir, fname)
            if not os.path.exists(local):
                gcs = f"gs://{GCS_BUCKET}/satellite/{t_id}/{fname}"
                run(f"gcloud storage cp {gcs} {local}", log=log)
        if not run(f"python compute_accessibility_h3.py --territory {t_id}", log=log):
            return False
        if not run(f"python split_by_admin.py --territory {t_id} --only accessibility", log=log):
            return False
        upload_parquet(t_prefix, t_dir, "accessibility")
    return True


def process_mapbiomas(t_id: str) -> bool:
    territory = get_territory(t_id)
    t_prefix = territory['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, t_prefix.rstrip('/'))
    log_path = os.path.join(OUTPUT_DIR, f"land_use_{t_id}.log")
    with open(log_path, "w") as log:
        log.write(f"=== {t_id} / land_use ===\n")
        # Download MapBiomas raster
        mb_name = f"mapbiomas_{t_id}_2023.tif"
        local = os.path.join(t_dir, mb_name)
        if not os.path.exists(local):
            gcs = f"gs://{GCS_BUCKET}/satellite/{t_id}/{mb_name}"
            if not run(f"gcloud storage cp {gcs} {local}", log=log):
                # Try alternative naming
                gcs2 = f"gs://{GCS_BUCKET}/satellite/{t_id}/mapbiomas_{t_id}_2022.tif"
                run(f"gcloud storage cp {gcs2} {local}", log=log)
        if not run(f"python process_mapbiomas_to_h3.py --territory {t_id}", log=log):
            return False
        if not run(f"python split_by_admin.py --territory {t_id} --only land_use", log=log):
            return False
        upload_parquet(t_prefix, t_dir, "land_use")
    return True


def process_forestry(t_id: str) -> bool:
    territory = get_territory(t_id)
    t_prefix = territory['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, t_prefix.rstrip('/'))
    log_path = os.path.join(OUTPUT_DIR, f"forestry_{t_id}.log")
    with open(log_path, "w") as log:
        log.write(f"=== {t_id} / forestry_aptitude ===\n")
        # Download all SDM composites
        for name in ("sdm_era5_composite", "sdm_chirps_composite",
                      "sdm_terraclimate_composite", "sdm_soilgrids_composite",
                      "sdm_terrain_composite", "sdm_ndvi_composite",
                      "sdm_ghsl_smod", "sdm_jrc_water", "sdm_mapbiomas_py"):
            local = os.path.join(t_dir, f"{name}.tif")
            if not os.path.exists(local):
                gcs = f"gs://{GCS_BUCKET}/satellite/{t_id}/{name}.tif"
                run(f"gcloud storage cp {gcs} {local}", log=log)
        if not run(f"python compute_forestry_sdm.py --territory {t_id}", log=log):
            return False
        if not run(f"python split_by_admin.py --territory {t_id} --only forestry_aptitude", log=log):
            return False
        upload_parquet(t_prefix, t_dir, "forestry_aptitude")
    return True


def process_flood(t_id: str) -> bool:
    territory = get_territory(t_id)
    t_prefix = territory['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, t_prefix.rstrip('/'))
    log_path = os.path.join(OUTPUT_DIR, f"flood_{t_id}.log")
    with open(log_path, "w") as log:
        log.write(f"=== {t_id} / flood_risk ===\n")
        flood_dir = f"gs://{GCS_BUCKET}/flood/{t_id}/"
        run(f"gcloud storage cp {flood_dir}*.tif {t_dir}/", log=log)
        if not run(f"python run_flood_update.py --territory {t_id} --skip-gee --upload", log=log):
            return False
    return True


def check_lv_ready(t_id: str) -> bool:
    return gcs_exists(f"gs://{GCS_BUCKET}/satellite/{t_id}/lv_cities_access.tif")


def check_mapbiomas_ready(t_id: str) -> bool:
    return (gcs_exists(f"gs://{GCS_BUCKET}/satellite/{t_id}/mapbiomas_{t_id}_2023.tif") or
            gcs_exists(f"gs://{GCS_BUCKET}/satellite/{t_id}/mapbiomas_{t_id}_2022.tif"))


def check_sdm_ready(t_id: str) -> bool:
    return gcs_exists(f"gs://{GCS_BUCKET}/satellite/{t_id}/sdm_era5_composite.tif")


def check_flood_ready(t_id: str) -> bool:
    return gcs_exists(f"gs://{GCS_BUCKET}/flood/{t_id}/flood_recurrence_historical.tif")


def check_hansen_ready(t_id: str) -> bool:
    return gcs_exists(f"gs://{GCS_BUCKET}/satellite/{t_id}/hansen_lossyear.tif")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--timeout-hours", type=float, default=6)
    ap.add_argument("--workers", type=int, default=3)
    args = ap.parse_args()

    deadline = time.time() + args.timeout_hours * 3600

    # Track what's been processed
    done = {
        "deforestation": set(),
        "accessibility": set(),
        "land_use": set(),
        "forestry": set(),
        "flood": set(),
    }

    # Mark already-done deforestation (11 processed + 1 in-flight at restart)
    done["deforestation"] = {
        "concepcion_py", "san_pedro_py", "cordillera_py", "guaira_py",
        "caaguazu_py", "caazapa_py", "misiones_py", "paraguari_py",
        "central_py", "neembucu_py", "amambay_py",
        # presidente_hayes was detected ready; will be caught fresh this run
    }

    print(f"Monitoring GCS. Polling every {POLL_INTERVAL}s. Timeout: {args.timeout_hours}h")
    print(f"Workers: {args.workers}")

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {}

        while time.time() < deadline:
            # --- Deforestation (remaining: amambay/canindeyu/presidente_hayes) ---
            for t in ALL_PY:
                if t not in done["deforestation"] and t not in futures.get("deforestation", {}):
                    if check_hansen_ready(t):
                        print(f"  [{t}] Hansen ready -> deforestation")
                        f = pool.submit(process_deforestation_wrap, t)
                        futures.setdefault("deforestation", {})[t] = f

            # --- Accessibility ---
            for t in ALL_PY:
                if t not in done["accessibility"] and t not in futures.get("accessibility", {}):
                    if check_lv_ready(t):
                        print(f"  [{t}] LV ready -> accessibility")
                        f = pool.submit(process_lv, t)
                        futures.setdefault("accessibility", {})[t] = f

            # --- Land use ---
            for t in ALL_PY:
                if t not in done["land_use"] and t not in futures.get("land_use", {}):
                    if check_mapbiomas_ready(t):
                        print(f"  [{t}] MapBiomas ready -> land_use")
                        f = pool.submit(process_mapbiomas, t)
                        futures.setdefault("land_use", {})[t] = f

            # --- Forestry ---
            for t in ALL_PY:
                if t not in done["forestry"] and t not in futures.get("forestry", {}):
                    if check_sdm_ready(t):
                        print(f"  [{t}] SDM ready -> forestry_aptitude")
                        f = pool.submit(process_forestry, t)
                        futures.setdefault("forestry", {})[t] = f

            # --- Flood ---
            for t in ALL_PY:
                if t not in done["flood"] and t not in futures.get("flood", {}):
                    if check_flood_ready(t):
                        print(f"  [{t}] Flood ready -> flood_risk")
                        f = pool.submit(process_flood, t)
                        futures.setdefault("flood", {})[t] = f

            # Collect completed futures
            for layer, fmap in list(futures.items()):
                for t, f in list(fmap.items()):
                    if f.done():
                        ok = f.result()
                        print(f"  {'OK' if ok else 'FAIL'} {t}/{layer}")
                        done[layer].add(t)
                        del fmap[t]

            # Status
            total_needed = len(ALL_PY) * 5  # 5 layers
            total_done = sum(len(v) for v in done.values())
            print(f"Progress: {total_done}/{total_needed} | "
                  f"deforest={len(done['deforestation'])} "
                  f"access={len(done['accessibility'])} "
                  f"landuse={len(done['land_use'])} "
                  f"forestry={len(done['forestry'])} "
                  f"flood={len(done['flood'])}")

            if total_done >= total_needed:
                print("All layers complete!")
                break

            time.sleep(POLL_INTERVAL)

    # Final summary
    print("\n=== FINAL STATUS ===")
    for layer, t_set in done.items():
        print(f"  {layer}: {len(t_set)}/{len(ALL_PY)} — {sorted(t_set)}")


def process_deforestation_wrap(t_id: str) -> bool:
    """Run deforestation pipeline as subprocess."""
    return run(
        f"python process_deforestation_py.py --only {t_id} --workers 1"
    )


if __name__ == "__main__":
    main()
