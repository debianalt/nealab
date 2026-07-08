"""
High-resolution EUDR aggregator (res-8 / res-9), province-scoped.

Unlike process_deforestation_to_h3.py (which loops per-hexagon over a GeoJSON
grid and does not scale past res-7), this reads the province window ONCE,
assigns every 100m pixel to its H3 cell, and aggregates by groupby. No giant
GeoJSON intermediate, no per-hex masking loop.

Source raster is 100m/pixel (EXPORT_SCALE=100), so:
  res-7 hex (~5.16 km2) ~ 2100 px   (current production)
  res-8 hex (~0.74 km2) ~   80 px
  res-9 hex (~0.10 km2) ~    9 px    (near the data floor)

Usage:
  python pipeline/aggregate_eudr_hires.py --province misiones --res 9
  python pipeline/aggregate_eudr_hires.py --province misiones --res 8
"""

import argparse
import json
import os
import sys
import time

import glob

import numpy as np
import pandas as pd
import rasterio
from rasterio.features import geometry_mask
from rasterio.merge import merge as rio_merge
from rasterio.transform import xy
from shapely.geometry import shape

import h3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config_eudr import (
    OUTPUT_DIR,
    EUDR_PROVINCES,
    HANSEN_MAX_YEAR,
    WEIGHT_LOSS_POST_2020,
    WEIGHT_FIRE_POST_2020,
    WEIGHT_NO_FOREST_2020,
)

# Post-cutoff Hansen lossyear codes (21=2021 .. HANSEN_MAX_YEAR-2000) and the
# calendar years they map to — shared with aggregate_eudr_region.py.
LOSS_YEAR_CODES = tuple(range(21, HANSEN_MAX_YEAR - 2000 + 1))
LOSS_YEARS = tuple(range(2021, HANSEN_MAX_YEAR + 1))

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BOUNDARY_PATH = os.path.join(
    PROJECT_ROOT, "src", "lib", "data", "eudr_provinces_boundary.json"
)

# Band order must match gee_deforestation_eudr.py export
BANDS = {
    "treecover_2000": 1,
    "loss_year": 2,
    "loss_post_2020": 3,
    "treecover_current": 4,
    "fire_post_2020": 5,
}


def default_rasters():
    """Auto-detect the pipeline's EUDR raster(s) on disk.

    Prefers the combined mosaic produced by run_eudr_update.py (the file the CI
    pipeline actually writes); falls back to the legacy per-export ``10prov``
    shards if a dev still has them locally.
    """
    for pattern in ("eudr_deforestation_combined*.tif", "eudr_deforestation_10prov-*.tif"):
        hits = sorted(glob.glob(os.path.join(OUTPUT_DIR, pattern)))
        hits = [h for h in hits if os.path.exists(h)]
        if hits:
            return hits
    return []


def load_province_geom(province_id):
    with open(BOUNDARY_PATH, "r", encoding="utf-8") as f:
        fc = json.load(f)
    for feat in fc["features"]:
        if feat["properties"]["id"] == province_id:
            return shape(feat["geometry"])
    raise ValueError(f"Province '{province_id}' not found in boundary file")


def aggregate(raster_paths, province_id, res):
    t0 = time.time()
    geom = load_province_geom(province_id)
    bbox = geom.bounds  # (minx, miny, maxx, maxy)

    srcs = [rasterio.open(p) for p in raster_paths]
    n_bands = min(min(s.count for s in srcs), len(BANDS))
    print(f"  Sources: {len(srcs)}  bands={n_bands}  res={srcs[0].res}")
    # Mosaic only the province bbox window across all shards
    stack, transform = rio_merge(
        srcs, bounds=bbox, indexes=list(range(1, n_bands + 1)), nodata=np.nan,
    )
    for s in srcs:
        s.close()
    stack = stack.astype("float32")
    nbands, rows, cols = stack.shape
    print(f"  Window: {rows}x{cols} = {rows * cols:,} pixels, {nbands} bands "
          f"({time.time() - t0:.0f}s)")

    # Province mask (True = inside province)
    inside = geometry_mask([geom], out_shape=(rows, cols),
                           transform=transform, invert=True)

    # Pixel-center coordinates
    col_idx = np.arange(cols)
    row_idx = np.arange(rows)
    xs = transform.c + (col_idx + 0.5) * transform.a  # lon
    ys = transform.f + (row_idx + 0.5) * transform.e  # lat
    lon_grid, lat_grid = np.meshgrid(xs, ys)

    # Treecover band as the validity gate (nodata pixels are excluded)
    tc = stack[BANDS["treecover_2000"] - 1]
    valid = inside & np.isfinite(tc) & (tc >= 0)
    n_valid = int(valid.sum())
    print(f"  Valid in-province pixels: {n_valid:,} ({time.time() - t0:.0f}s)")
    if n_valid == 0:
        raise RuntimeError("No valid pixels — check bbox/raster alignment")

    lats = lat_grid[valid]
    lons = lon_grid[valid]

    # Assign each pixel to its H3 cell
    th = time.time()
    cells = [h3.latlng_to_cell(la, lo, res) for la, lo in zip(lats, lons)]
    print(f"  H3 assignment ({res}): {len(cells):,} cells in {time.time() - th:.0f}s")

    data = {"h3index": cells}
    for name, idx in list(BANDS.items())[:nbands]:
        band = stack[idx - 1][valid]
        # fire band can be NaN where MODIS has no burns → treat as 0
        if name == "fire_post_2020":
            band = np.nan_to_num(band, nan=0.0)
        data[name] = band

    # Per-year post-cutoff loss masks (Hansen lossyear: 21=2021..)
    if "loss_year" in BANDS and BANDS["loss_year"] <= nbands:
        ly = stack[BANDS["loss_year"] - 1][valid]
        for y in LOSS_YEAR_CODES:
            data[f"loss_y{2000 + y}"] = (ly == y).astype("float32")

    df = pd.DataFrame(data)

    # Aggregate: mean per band over pixels in each cell
    agg = df.groupby("h3index").mean()
    agg["province"] = province_id
    print(f"  Aggregated to {len(agg):,} cells ({time.time() - t0:.0f}s)")
    return agg.reset_index()


def post_process(df):
    df["forest_cover_2020"] = df["treecover_2000"].round(1)
    df["forest_cover_current"] = df["treecover_current"].round(1)
    df["loss_post_2020_pct"] = (df["loss_post_2020"] * 100).round(2)
    df["loss_total_pct"] = (df["treecover_2000"] - df["treecover_current"]).clip(lower=0).round(2)
    df["loss_pre_2020_pct"] = (df["loss_total_pct"] - df["loss_post_2020_pct"]).clip(lower=0).round(2)
    df["fire_post_2020_pct"] = (df["fire_post_2020"] * 100).round(2)

    loss_norm = (df["loss_post_2020_pct"]).clip(upper=100)
    fire_norm = (df["fire_post_2020_pct"]).clip(upper=100)
    no_forest = (100 - df["forest_cover_2020"]).clip(lower=0)
    df["risk_score"] = (
        WEIGHT_LOSS_POST_2020 * loss_norm
        + WEIGHT_FIRE_POST_2020 * fire_norm
        + WEIGHT_NO_FOREST_2020 * no_forest
    ).clip(lower=0, upper=100).round(1)

    df["deforestation_post_2020"] = (df["loss_post_2020_pct"] > 0).astype(int)

    # Per-year post-2020 loss as percentages (for the temporal curve)
    year_cols = []
    for y in LOSS_YEARS:
        src = f"loss_y{y}"
        out = f"loss_{y}_pct"
        if src in df.columns:
            df[out] = (df[src] * 100).round(2)
            year_cols.append(out)

    cols = [
        "h3index", "province",
        "forest_cover_2020", "forest_cover_current",
        "loss_total_pct", "loss_post_2020_pct", "loss_pre_2020_pct",
        "fire_post_2020_pct", "risk_score", "deforestation_post_2020",
        *year_cols,
    ]
    return df[cols].dropna(subset=["forest_cover_2020"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--province", required=True, choices=list(EUDR_PROVINCES))
    ap.add_argument("--res", type=int, default=9)
    ap.add_argument("--raster", nargs="*", default=None,
                    help="Raster(s). Default: auto-detect the combined EUDR mosaic on disk.")
    ap.add_argument("--out-dir", default=os.path.join(OUTPUT_DIR, "hires"))
    args = ap.parse_args()

    rasters = args.raster or default_rasters()
    rasters = [r for r in rasters if os.path.exists(r)]
    if not rasters:
        print("No raster shards found.")
        return 1
    print(f"  Rasters: {[os.path.basename(r) for r in rasters]}")

    os.makedirs(args.out_dir, exist_ok=True)
    t0 = time.time()
    agg = aggregate(rasters, args.province, args.res)
    result = post_process(agg)

    out = os.path.join(args.out_dir, f"eudr_{args.province}_res{args.res}.parquet")
    result.to_parquet(out, index=False)
    size_mb = os.path.getsize(out) / (1024 * 1024)

    n = len(result)
    ndef = int(result["deforestation_post_2020"].sum())
    print("\n  -- Validation --")
    print(f"    Province: {args.province}  res-{args.res}")
    print(f"    Cells: {n:,}")
    print(f"    Parquet: {size_mb:.2f} MB  ({size_mb * 1024 / (n / 1000):.2f} KB per 1k cells)")
    print(f"    risk_score: mean={result['risk_score'].mean():.1f} "
          f"median={result['risk_score'].median():.1f} max={result['risk_score'].max():.1f}")
    print(f"    deforestation_post_2020==1: {ndef:,} ({ndef / n:.1%})")
    pos = result.loc[result.loss_post_2020_pct > 0, "loss_post_2020_pct"]
    print(f"    loss_post_2020_pct>0 mean: {pos.mean():.2f}%  (cells: {len(pos):,})")
    print(f"    Total wall time: {time.time() - t0:.0f}s")
    print(f"    Saved: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
