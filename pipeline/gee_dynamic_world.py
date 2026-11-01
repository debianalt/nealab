"""
Export Dynamic World annual land cover for a territory, as area fractions.

Dynamic World is a 10m near-real-time land use/land cover dataset from Google.
9 classes: water, trees, grass, flooded_vegetation, crops, shrub_and_scrub,
           built, bare, snow_and_ice

Default product `dw_cover` (since 31-oct-2026): the year's most frequent class
of each 10 m pixel, one band per class (0/1), averaged to 100 m. Each 100 m
cell then holds the real share of its area in each class, and the H3 mean of
those cells (process_dw_to_h3.py) is the share of the hexagon's area.

`--probs` keeps the old product `dw_probs`: the mean per-class PROBABILITY.
That is not area: the model gives every pixel some probability in all nine
classes (~5 % snow_and_ice over subtropical forest), which inflated small
classes and shrank dominant ones. Do not publish it as % of area.

Usage:
  python pipeline/gee_dynamic_world.py                          # Misiones, Drive
  python pipeline/gee_dynamic_world.py --year 2024
  python pipeline/gee_dynamic_world.py --territory itapua_py --gcs --no-wait
  python pipeline/gee_dynamic_world.py --territory itapua_py --gcs --probs   # old product
"""

import argparse
import ee
import json
import os
import sys
import time

from config import MISIONES_BBOX, get_territory

ASSET = 'GOOGLE/DYNAMICWORLD/V1'
CLASSES = ['water', 'trees', 'grass', 'flooded_vegetation', 'crops',
           'shrub_and_scrub', 'built', 'bare', 'snow_and_ice']
SCALE = 100
GCS_BUCKET = 'spatia-satellite'
DRIVE_FOLDER = 'spatia-satellite'


def authenticate():
    """Service account in CI, user credentials locally."""
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
    parser = argparse.ArgumentParser(description="Export Dynamic World composite")
    parser.add_argument("--year", type=int, default=2024, help="Year for composite")
    parser.add_argument("--territory", default="misiones", help="Territory ID from pipeline config")
    parser.add_argument("--gcs", action="store_true", help="Export to GCS bucket instead of Drive")
    parser.add_argument("--no-wait", action="store_true", help="Start tasks and exit without polling")
    parser.add_argument("--probs", action="store_true",
                        help="Export the old mean-probability product (dw_probs) instead of area fractions")
    args = parser.parse_args()

    year = args.year
    is_ci = authenticate()
    use_gcs = is_ci or args.gcs
    t = get_territory(args.territory)
    print(f"Exporting Dynamic World {year} for {args.territory} -> {'GCS' if use_gcs else 'Drive'}...")

    bbox = ee.Geometry.Rectangle(t['bbox'] if args.territory != 'misiones' else MISIONES_BBOX)
    product = 'dw_probs' if args.probs else 'dw_cover'
    # Per-territory GCS prefix matches the satellite pipeline layout
    # (gs://spatia-satellite/satellite/<territory>/...). Misiones keeps the
    # legacy flat name so the original workflow is untouched.
    name_prefix = (f"satellite/{args.territory}/{product}_{args.territory}_{year}"
                   if args.territory != 'misiones' else f"{product}_{year}")

    # Filter collection to year
    start = f"{year}-01-01"
    end = f"{year}-12-31"

    dw = (ee.ImageCollection(ASSET)
          .filterBounds(bbox)
          .filterDate(start, end))

    n_images = dw.size().getInfo()
    print(f"  Images in collection: {n_images}")

    if n_images == 0:
        print("ERROR: No images found for this year/region")
        return 1

    if args.probs:
        image = dw.select(CLASSES).mean().clip(bbox)
    else:
        # Most frequent class of the year per 10 m pixel, one 0/1 band per class,
        # averaged to SCALE: the area share of each class in each output cell.
        px10 = ee.Projection('EPSG:4326').atScale(10)
        mode = dw.select('label').reduce(ee.Reducer.mode()).setDefaultProjection(px10)
        onehot = ee.Image.cat([mode.eq(i).rename(c) for i, c in enumerate(CLASSES)]).toFloat()
        image = (onehot
                 .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1024)
                 .reproject(crs='EPSG:4326', scale=SCALE)
                 .clip(bbox))

    desc = f'{product}_{args.territory}_{year}'
    export_kwargs = dict(
        image=image,
        description=desc,
        region=bbox,
        scale=SCALE,
        crs='EPSG:4326',
        maxPixels=1e10,
    )
    if use_gcs:
        export_kwargs['bucket'] = GCS_BUCKET
        export_kwargs['fileNamePrefix'] = name_prefix
        task = ee.batch.Export.image.toCloudStorage(**export_kwargs)
    else:
        export_kwargs['folder'] = DRIVE_FOLDER
        export_kwargs['fileNamePrefix'] = name_prefix
        task = ee.batch.Export.image.toDrive(**export_kwargs)
    task.start()
    print(f"  Started export: {name_prefix} (scale={SCALE}m)")
    tasks = [task]

    if args.no_wait:
        for t_ in tasks:
            print(f"  task id: {t_.status().get('id', '?')} ({t_.status().get('description')})")
        print("  --no-wait: exiting, monitor at https://code.earthengine.google.com/tasks")
        return 0

    # Poll for completion
    print("\n  Waiting for exports to complete...")
    while True:
        statuses = [t.status()['state'] for t in tasks]
        running = sum(1 for s in statuses if s in ('READY', 'RUNNING'))
        if running == 0:
            break
        print(f"    {statuses} ...")
        time.sleep(30)

    for t in tasks:
        status = t.status()
        if status['state'] == 'COMPLETED':
            print(f"  DONE: {status['description']}")
        else:
            print(f"  FAILED: {status['description']} — {status.get('error_message', 'unknown')}")
            return 1

    print("\n  Files exported to Google Drive folder 'spatia-satellite'")
    print("  Download them and run process_dw_to_h3.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
