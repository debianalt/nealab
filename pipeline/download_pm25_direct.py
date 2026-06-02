"""
Direct PM2.5 download bypassing GEE batch export queue.
Uses ee.Image.getDownloadURL() at 1km resolution (native ACAG scale).
Works for small-medium territories (<50MB output per year).

Usage:
  python pipeline/download_pm25_direct.py --territory boqueron_py
  python pipeline/download_pm25_direct.py --territory boqueron_py,alto_paraguay_py,presidente_hayes_py
"""
import argparse
import os
import sys
import urllib.request
import tempfile
import shutil
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, get_territory

COLLECTION = 'projects/sat-io/open-datasets/GLOBAL-SATELLITE-PM25/ANNUAL'
YEARS = list(range(1998, 2023))


def authenticate():
    import ee
    import json
    import os as _os
    key_env = _os.environ.get("GEE_SERVICE_ACCOUNT_KEY", "")
    if not key_env:
        ee.Initialize()
    else:
        if _os.path.isfile(key_env):
            with open(key_env) as f:
                key_data = json.load(f)
        else:
            key_data = json.loads(key_env)
        credentials = ee.ServiceAccountCredentials(key_data["client_email"], key_data=json.dumps(key_data))
        ee.Initialize(credentials)
    return True


def download_territory(t_id, years=None):
    import ee
    authenticate()

    territory = get_territory(t_id)
    bbox = territory['bbox']  # [west, south, east, north]
    region = ee.Geometry.Rectangle(bbox)
    t_dir = os.path.join(OUTPUT_DIR, territory['output_prefix'].rstrip('/'))
    os.makedirs(t_dir, exist_ok=True)

    all_years = years or YEARS
    col = ee.ImageCollection(COLLECTION).filterBounds(region)
    available_dates = col.aggregate_array('system:time_start').getInfo()
    from datetime import datetime, timezone
    available_years = {datetime.fromtimestamp(d / 1000, tz=timezone.utc).year for d in available_dates}
    target_years = [y for y in all_years if y in available_years]
    print(f"[{t_id}] Available years: {min(target_years)}-{max(target_years)} ({len(target_years)} total)")

    ok_count = 0
    for yr in sorted(target_years):
        dst = os.path.join(t_dir, f"sat_pm25_{yr}.tif")
        if os.path.exists(dst):
            ok_count += 1
            continue

        print(f"  {yr}...", end=" ", flush=True)
        try:
            img = (col
                   .filter(ee.Filter.calendarRange(yr, yr, 'year'))
                   .first()
                   .toFloat())

            # Download at 0.01 degree (native ACAG resolution ~1km)
            url = img.getDownloadURL({
                'scale': 1000,
                'format': 'GEO_TIFF',
                'region': region,
                'crs': 'EPSG:4326',
            })

            # Download to temp file then move
            with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp:
                tmp_path = tmp.name
            urllib.request.urlretrieve(url, tmp_path)
            shutil.move(tmp_path, dst)
            sz = os.path.getsize(dst) // 1024
            print(f"OK ({sz}KB)")
            ok_count += 1
        except Exception as e:
            print(f"FAIL: {e}")

    print(f"[{t_id}] Done: {ok_count}/{len(target_years)} years downloaded")
    return ok_count


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", required=True, help="Comma-separated territory IDs")
    ap.add_argument("--years", type=int, nargs="+", default=None)
    args = ap.parse_args()

    territories = [t.strip() for t in args.territory.split(",")]

    for t in territories:
        print(f"\n=== {t} ===")
        try:
            n = download_territory(t, args.years)
            print(f"[{t}] {n} years ready")
        except Exception as e:
            print(f"[{t}] ERROR: {e}")


if __name__ == "__main__":
    main()
