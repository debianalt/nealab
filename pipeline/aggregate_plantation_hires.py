"""
Aggregate MapBiomas land cover to H3 (res-9) as plantation vs native-forest %.

For the EUDR plantation-vs-native distinction: Hansen GFC marks a plantation
harvest cycle (pine/eucalyptus) as "loss", which is a FALSE POSITIVE for
deforestation. This layer lets the screening flag those hexes as managed
forestry instead of native-forest deforestation.

Reads a MapBiomas classification raster (per-pixel class codes, as exported by
gee_export_mapbiomas.py — AR/PY MapBiomas, or BR remapped from ESA WorldCover),
assigns every valid pixel to its H3 cell, and computes per cell:
  - plantation_pct      : % of pixels = class 9 (Silvicultura / Forest Plantation)
  - native_forest_pct   : % of pixels in {3,4,5,6} (native / floodable forest)
  - mb_px               : valid pixel count (quality signal)

BR via ESA WorldCover has NO plantation class → plantation_pct will be ~0 there
(documented gap; ESA trees all map to native_forest).

Usage:
  python pipeline/aggregate_plantation_hires.py \
      --raster pipeline/output/mapbiomas_misiones_2022.tif \
      --province ar_misiones --year 2022 --res 9
"""

import argparse
import os
import sys
import time

import numpy as np
import pandas as pd
import rasterio

import h3

PLANTATION_CLASSES = {9}
NATIVE_CLASSES = {3, 4, 5, 6}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(SCRIPT_DIR, "output", "eudr", "hires")


def aggregate(raster_path, res):
    t0 = time.time()
    with rasterio.open(raster_path) as src:
        arr = src.read(1)
        transform = src.transform
        print(f"  Raster: {src.width}x{src.height}  res={src.res}  crs={src.crs}")

    valid = arr > 0  # class 0 = nodata / unclassified
    n = int(valid.sum())
    if n == 0:
        raise RuntimeError("No valid pixels (all class 0) — check the raster.")
    print(f"  Valid pixels: {n:,} ({time.time() - t0:.0f}s)")

    ridx, cidx = np.nonzero(valid)
    # pixel-center coordinates (affine: x = c + (col+0.5)*a, y = f + (row+0.5)*e)
    lons = transform.c + (cidx + 0.5) * transform.a
    lats = transform.f + (ridx + 0.5) * transform.e
    classes = arr[valid]

    is_plant = np.isin(classes, list(PLANTATION_CLASSES))
    is_native = np.isin(classes, list(NATIVE_CLASSES))

    th = time.time()
    cells = [h3.latlng_to_cell(la, lo, res) for la, lo in zip(lats, lons)]
    print(f"  H3 assignment (res-{res}): {len(cells):,} px in {time.time() - th:.0f}s")

    df = pd.DataFrame({
        "h3index": cells,
        "plant": is_plant.astype("float32"),
        "native": is_native.astype("float32"),
    })
    agg = df.groupby("h3index").agg(
        plantation_pct=("plant", "mean"),
        native_forest_pct=("native", "mean"),
        mb_px=("plant", "size"),
    ).reset_index()
    agg["plantation_pct"] = (agg["plantation_pct"] * 100).round(1)
    agg["native_forest_pct"] = (agg["native_forest_pct"] * 100).round(1)
    print(f"  Aggregated to {len(agg):,} cells ({time.time() - t0:.0f}s)")
    return agg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raster", required=True, help="MapBiomas classification GeoTIFF")
    ap.add_argument("--province", required=True,
                    help="EUDR province key (matches the EUDR parquet, e.g. ar_misiones)")
    ap.add_argument("--year", type=int, default=0, help="MapBiomas vintage year (metadata)")
    ap.add_argument("--res", type=int, default=9)
    ap.add_argument("--tag", default="", help="Filename tag, e.g. '2020' -> plantation2020_<prov>_res9.parquet")
    ap.add_argument("--out-dir", default=OUT_DIR)
    args = ap.parse_args()

    if not os.path.exists(args.raster):
        print(f"ERROR: raster not found: {args.raster}")
        return 1

    os.makedirs(args.out_dir, exist_ok=True)
    agg = aggregate(args.raster, args.res)
    agg["province"] = args.province
    if args.year:
        agg["mb_year"] = args.year

    out = os.path.join(args.out_dir, f"plantation{args.tag}_{args.province}_res{args.res}.parquet")
    agg.to_parquet(out, index=False)
    size_mb = os.path.getsize(out) / (1024 * 1024)

    print("\n  -- Validation --")
    print(f"    Province: {args.province}  res-{args.res}  cells: {len(agg):,}")
    print(f"    plantation_pct: mean={agg['plantation_pct'].mean():.1f} "
          f"max={agg['plantation_pct'].max():.1f}  "
          f"cells>50%: {(agg['plantation_pct'] > 50).sum():,}")
    print(f"    native_forest_pct: mean={agg['native_forest_pct'].mean():.1f}")
    print(f"    Parquet: {size_mb:.2f} MB  Saved: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
