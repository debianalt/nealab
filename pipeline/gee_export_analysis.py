"""
Export multi-band GEE composites for satellite analyses at pixel level.

Each analysis is a multi-band ee.Image exported as GeoTIFF to Google Drive.
This replaces the radio-level approach — each pixel contributes independently
to H3 zonal statistics, capturing sub-radio spatial variation.

Usage:
  python pipeline/gee_export_analysis.py --analysis agri_potential
  python pipeline/gee_export_analysis.py --analysis all
"""

import argparse
import ee
import json
import os
import sys
import time

from config import MISIONES_BBOX, OUTPUT_DIR, get_territory

EXPORT_SCALE = 100  # metres — sweet spot for H3 res-9 (~174m hex side)
DRIVE_FOLDER = 'spatia-satellite'
GCS_BUCKET = 'spatia-satellite'
DATE_START = '2019-01-01'
DATE_END = '2024-12-31'


def authenticate():
    """Authenticate to GEE — service account in CI, user credentials locally."""
    key_env = os.environ.get("GEE_SERVICE_ACCOUNT_KEY", "")
    if not key_env:
        ee.Initialize()
        return False  # local dev, use Drive

    if os.path.isfile(key_env):
        with open(key_env) as f:
            key_data = json.load(f)
    else:
        key_data = json.loads(key_env)

    credentials = ee.ServiceAccountCredentials(
        key_data["client_email"],
        key_data=json.dumps(key_data),
    )
    ee.Initialize(credentials, opt_url="https://earthengine-highvolume.googleapis.com")
    return True  # CI mode, use GCS


def build_agri_potential(bbox):
    """SOC + pH + clay + precipitation + GDD + slope."""
    soil = ee.Image('projects/soilgrids-isric/ocd_mean').select('ocd_0-5cm_mean')
    soc = soil.unmask(0)

    ph = ee.Image('projects/soilgrids-isric/phh2o_mean').select('phh2o_0-5cm_mean').unmask(50)
    ph_optimal = ee.Image(1).subtract(ph.divide(10).subtract(6.25).abs().divide(4)).max(0)

    clay = ee.Image('projects/soilgrids-isric/clay_mean').select('clay_0-5cm_mean').unmask(0)

    precip = (ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY')
              .filterDate(DATE_START, DATE_END).filterBounds(bbox)
              .select('precipitation').sum().divide(6))

    # GDD base 10: sum of (T - 10) where T > 10, over year
    era5 = (ee.ImageCollection('ECMWF/ERA5_LAND/MONTHLY_AGGR')
            .filterDate(DATE_START, DATE_END).filterBounds(bbox))
    gdd = era5.select('temperature_2m').map(
        lambda img: img.subtract(283.15).max(0).multiply(30)  # ~days per month
    ).sum().divide(6)

    dem = ee.Image('USGS/SRTMGL1_003').select('elevation')
    slope = ee.Terrain.slope(dem)

    return (soc.rename('c_soc')
            .addBands(ph_optimal.rename('c_ph_optimal'))
            .addBands(clay.rename('c_clay'))
            .addBands(precip.rename('c_precipitation'))
            .addBands(gdd.rename('c_gdd'))
            .addBands(slope.rename('c_slope'))
            .clip(bbox).toFloat())


ANALYSIS_BUILDERS = {
    'agri_potential': build_agri_potential,
}

# GEE batch tasks occasionally fail with a transient "Internal error". Retry the
# affected analysis instead of failing the whole pipeline run for a Google-side blip.
EXPORT_MAX_RETRIES = 2


def _start_export(aid, bbox, use_gcs, t_prefix, scale, territory_id):
    """Build the composite for `aid` and start a GEE export task. Returns the task."""
    composite = ANALYSIS_BUILDERS[aid](bbox)
    gcs_prefix = f'satellite/{t_prefix}sat_{aid}_raster'
    description = f'{territory_id}_{aid}_raster' if territory_id != 'misiones' else f'sat_{aid}_raster'
    if use_gcs:
        task = ee.batch.Export.image.toCloudStorage(
            image=composite,
            description=description,
            bucket=GCS_BUCKET,
            fileNamePrefix=gcs_prefix,
            region=bbox,
            scale=scale,
            crs='EPSG:4326',
            maxPixels=1e9,
        )
    else:
        task = ee.batch.Export.image.toDrive(
            image=composite,
            description=description,
            folder=DRIVE_FOLDER,
            fileNamePrefix=f'{t_prefix}sat_{aid}_raster' if t_prefix else f'sat_{aid}_raster',
            region=bbox,
            scale=scale,
            crs='EPSG:4326',
            maxPixels=1e9,
        )
    task.start()
    return task


def _wait_for(tasks):
    """Block until no task in `tasks` is READY/RUNNING."""
    while True:
        statuses = [(aid, t.status()['state']) for aid, t in tasks]
        running = sum(1 for _, s in statuses if s in ('READY', 'RUNNING'))
        if running == 0:
            return
        print(f"    [{running} running] " + ', '.join(f"{a}={s}" for a, s in statuses))
        time.sleep(30)


def main():
    parser = argparse.ArgumentParser(description="Export GEE analysis composites to Drive/GCS")
    parser.add_argument("--analysis", required=True, help="Analysis ID or 'all'")
    parser.add_argument("--territory", default="misiones",
                        help="Territory ID from config.py (default: misiones)")
    parser.add_argument("--scale", type=int, default=None,
                        help="Export scale in metres (default: from territory config)")
    parser.add_argument("--gcs", action="store_true", help="Force export to GCS (default: auto-detect CI)")
    parser.add_argument("--batch-size", type=int, default=6,
                        help="Max concurrent GEE tasks (default: 6; GEE Partner limit ~25)")
    args = parser.parse_args()

    territory = get_territory(args.territory)
    scale = args.scale or territory['export_scale']
    t_prefix = territory['output_prefix']  # '' for misiones, 'itapua_py/' for itapua

    is_ci = authenticate()
    use_gcs = args.gcs or is_ci
    bbox = ee.Geometry.Rectangle(territory['bbox'])

    if args.analysis == 'all':
        analyses = list(ANALYSIS_BUILDERS.keys())
    else:
        analyses = [a.strip() for a in args.analysis.split(',')]

    dest = f"GCS ({GCS_BUCKET})" if use_gcs else f"Drive ({DRIVE_FOLDER})"
    print(f"Territory: {territory['label']} ({args.territory})")
    print(f"Exporting {len(analyses)} analyses at {scale}m scale -> {dest}")
    if t_prefix:
        print(f"Output prefix: satellite/{t_prefix}")
    tasks = []

    # Process in batches to stay within GEE concurrent task limits
    for i in range(0, len(analyses), args.batch_size):
        batch = analyses[i:i + args.batch_size]
        batch_tasks = []

        for aid in batch:
            if aid not in ANALYSIS_BUILDERS:
                print(f"  SKIP {aid}: no builder defined")
                continue

            print(f"\n  Building {aid}...")
            task = _start_export(aid, bbox, use_gcs, t_prefix, scale, args.territory)
            batch_tasks.append((aid, task))
            tasks.append((aid, task))
            print(f"    Export started: {aid}")

        # Wait for this batch before starting the next (avoid overloading GEE queue)
        if i + args.batch_size < len(analyses):
            print(f"\n  Waiting for batch {i // args.batch_size + 1} ({len(batch_tasks)} tasks)...")
            _wait_for(batch_tasks)

    if not tasks:
        print("No tasks to run")
        return 1

    # Poll for completion
    print(f"\nWaiting for {len(tasks)} exports...")
    _wait_for(tasks)

    # Retry transient failures: GEE batch tasks occasionally die with "Internal error".
    # Rebuild + restart only the failed analyses instead of failing the whole run.
    results = dict(tasks)  # aid -> task (latest attempt)
    for attempt in range(1, EXPORT_MAX_RETRIES + 1):
        failed = [aid for aid, t in results.items() if t.status()['state'] != 'COMPLETED']
        if not failed:
            break
        print(f"\n  Retry {attempt}/{EXPORT_MAX_RETRIES} for {len(failed)} failed export(s): {', '.join(failed)}")
        retried = []
        for aid in failed:
            task = _start_export(aid, bbox, use_gcs, t_prefix, scale, args.territory)
            results[aid] = task
            retried.append((aid, task))
            print(f"    Re-export started: {aid}")
        _wait_for(retried)

    # Report results
    print(f"\n{'=' * 60}")
    all_ok = True
    for aid, task in results.items():
        status = task.status()
        if status['state'] == 'COMPLETED':
            print(f"  DONE: {aid}")
        else:
            print(f"  FAILED after {EXPORT_MAX_RETRIES} retries: {aid} — {status.get('error_message', 'unknown')}")
            all_ok = False

    print(f"\nFiles in Google Drive folder '{DRIVE_FOLDER}'")
    print(f"Download and run: python pipeline/process_raster_to_h3.py --analysis <id>")
    print(f"{'=' * 60}")

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
