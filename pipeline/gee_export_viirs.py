"""
Export a VIIRS nightlights annual composite (mean radiance) for a territory.

NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG `avg_rad` (nW/cm²/sr), mean of the year's
monthly composites — same source as the Misiones radio-level
viirs_mean_radiance, exported as raster for direct H3 centroid sampling.

Usage:
  python pipeline/gee_export_viirs.py --territory corrientes --gcs --no-wait
"""
import argparse
import json
import os
import sys
import time

import ee

from config import get_territory

GCS_BUCKET = 'spatia-satellite'
DRIVE_FOLDER = 'spatia-satellite'
SCALE = 500  # native VIIRS DNB ~463m


def authenticate():
    key_env = os.environ.get("GEE_SERVICE_ACCOUNT_KEY", "")
    if not key_env:
        ee.Initialize()
        return False
    if os.path.isfile(key_env):
        with open(key_env) as f:
            key_data = json.load(f)
    else:
        key_data = json.loads(key_env)
    credentials = ee.ServiceAccountCredentials(
        key_data["client_email"], key_data=json.dumps(key_data))
    ee.Initialize(credentials, opt_url="https://earthengine-highvolume.googleapis.com")
    return True


def main():
    ap = argparse.ArgumentParser(description="Export VIIRS annual mean radiance")
    ap.add_argument("--territory", required=True)
    ap.add_argument("--year", type=int, default=2024)
    ap.add_argument("--gcs", action="store_true")
    ap.add_argument("--no-wait", action="store_true")
    args = ap.parse_args()

    is_ci = authenticate()
    use_gcs = is_ci or args.gcs
    t = get_territory(args.territory)
    bbox = ee.Geometry.Rectangle(t['bbox'])

    img = (ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG')
           .filter(ee.Filter.calendarRange(args.year, args.year, 'year'))
           .select('avg_rad')
           .mean()
           .rename('viirs')
           .toFloat()
           .clip(bbox))

    name_prefix = f'satellite/{args.territory}/viirs_{args.territory}_{args.year}'
    desc = f'viirs_{args.territory}_{args.year}'
    kwargs = dict(image=img, description=desc, region=bbox, scale=SCALE,
                  crs='EPSG:4326', maxPixels=1e9)
    if use_gcs:
        kwargs['bucket'] = GCS_BUCKET
        kwargs['fileNamePrefix'] = name_prefix
        task = ee.batch.Export.image.toCloudStorage(**kwargs)
    else:
        kwargs['folder'] = DRIVE_FOLDER
        kwargs['fileNamePrefix'] = desc
        task = ee.batch.Export.image.toDrive(**kwargs)
    task.start()
    print(f"Started {desc} (scale={SCALE}m) -> {'GCS' if use_gcs else 'Drive'}")
    if args.no_wait:
        print(f"task id: {task.status().get('id', '?')}")
        return 0

    while True:
        st = task.status()['state']
        if st in ('COMPLETED', 'FAILED', 'CANCELLED'):
            print(st)
            return 0 if st == 'COMPLETED' else 1
        time.sleep(20)


if __name__ == '__main__':
    sys.exit(main())
