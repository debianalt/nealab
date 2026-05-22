"""
One-off: export EUDR deforestation raster for the Brazil area of interest
(Paraná, Santa Catarina, Rio Grande do Sul) to GCS, reusing the same Hansen +
MODIS composite as the AR/PY pipeline. Run in background — GEE processes async.

Output: gs://spatia-satellite/eudr/eudr_deforestation_br*.tif
Then: download + aggregate_eudr_region.py on GADM BRA level-1.
"""

import os
import sys
import time

import ee

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gee_deforestation_eudr import build_eudr_deforestation
from config_eudr import EXPORT_SCALE

# Rectangle covering Paraná + Santa Catarina + Rio Grande do Sul
BR_BBOX = [-57.7, -33.85, -48.2, -22.4]
BUCKET = "spatia-satellite"


def main():
    ee.Initialize()
    bbox = ee.Geometry.Rectangle(BR_BBOX)
    composite = build_eudr_deforestation(bbox)
    task = ee.batch.Export.image.toCloudStorage(
        image=composite,
        description="eudr_deforestation_br",
        bucket=BUCKET,
        fileNamePrefix="eudr/eudr_deforestation_br",
        region=bbox,
        scale=EXPORT_SCALE,
        crs="EPSG:4326",
        maxPixels=2e9,
    )
    task.start()
    print(f"Submitted BR export at {EXPORT_SCALE}m -> gs://{BUCKET}/eudr/eudr_deforestation_br*.tif")
    while True:
        st = task.status()
        state = st["state"]
        print(f"  [{time.strftime('%H:%M:%S')}] {state}")
        if state not in ("READY", "RUNNING"):
            break
        time.sleep(30)
    if task.status()["state"] == "COMPLETED":
        print("DONE — raster in GCS. Next: download + aggregate_eudr_region.py.")
        return 0
    print(f"FAILED: {task.status().get('error_message', 'unknown')}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
