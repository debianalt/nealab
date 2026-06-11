"""
Export Dynamic World annual composite for a territory.

Dynamic World is a 10m near-real-time land use/land cover dataset from Google.
9 classes: water, trees, grass, flooded_vegetation, crops, shrub_and_scrub,
           built, bare, snow_and_ice

Usage:
  python pipeline/gee_dynamic_world.py                          # Misiones, Drive
  python pipeline/gee_dynamic_world.py --year 2024
  python pipeline/gee_dynamic_world.py --territory itapua_py --gcs --no-wait
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
    parser.add_argument("--skip-label", action="store_true",
                        help="Skip the label-mode export (only probs are consumed by process_dw_to_h3)")
    args = parser.parse_args()

    year = args.year
    is_ci = authenticate()
    use_gcs = is_ci or args.gcs
    t = get_territory(args.territory)
    print(f"Exporting Dynamic World {year} for {args.territory} -> {'GCS' if use_gcs else 'Drive'}...")

    bbox = ee.Geometry.Rectangle(t['bbox'] if args.territory != 'misiones' else MISIONES_BBOX)
    # Per-territory GCS prefix matches the satellite pipeline layout
    # (gs://spatia-satellite/satellite/<territory>/...). Misiones keeps the
    # legacy flat name so the original workflow is untouched.
    name_prefix = (f"satellite/{args.territory}/dw_probs_{args.territory}_{year}"
                   if args.territory != 'misiones' else f"dw_probs_{year}")

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

    # Compute mode (most frequent class) per pixel
    label_mode = dw.select('label').mode().clip(bbox)

    # Also compute mean probability per class (more useful for H3 aggregation)
    class_probs = dw.select(CLASSES).mean().clip(bbox)

    # Export class probabilities (mean fraction per class)
    desc = f'dw_probs_{args.territory}_{year}'
    export_kwargs = dict(
        image=class_probs,
        description=desc,
        region=bbox,
        scale=SCALE,
        crs='EPSG:4326',
        maxPixels=1e9,
    )
    if use_gcs:
        export_kwargs['bucket'] = GCS_BUCKET
        export_kwargs['fileNamePrefix'] = name_prefix
        task_probs = ee.batch.Export.image.toCloudStorage(**export_kwargs)
    else:
        export_kwargs['folder'] = DRIVE_FOLDER
        export_kwargs['fileNamePrefix'] = name_prefix
        task_probs = ee.batch.Export.image.toDrive(**export_kwargs)
    task_probs.start()
    print(f"  Started export: {name_prefix} (scale={SCALE}m)")
    tasks = [task_probs]

    if not args.skip_label:
        # Export label mode
        label_kwargs = dict(
            image=label_mode.toInt8(),
            description=f'dw_label_{args.territory}_{year}',
            region=bbox,
            scale=SCALE,
            crs='EPSG:4326',
            maxPixels=1e9,
        )
        label_prefix = name_prefix.replace('dw_probs', 'dw_label')
        if use_gcs:
            label_kwargs['bucket'] = GCS_BUCKET
            label_kwargs['fileNamePrefix'] = label_prefix
            task_label = ee.batch.Export.image.toCloudStorage(**label_kwargs)
        else:
            label_kwargs['folder'] = DRIVE_FOLDER
            label_kwargs['fileNamePrefix'] = label_prefix
            task_label = ee.batch.Export.image.toDrive(**label_kwargs)
        task_label.start()
        print(f"  Started export: {label_prefix} (scale={SCALE}m)")
        tasks.append(task_label)

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
