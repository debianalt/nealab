"""
Aggregate EUDR deforestation to H3 for an arbitrary set of admin units, from a
GeoTIFF (or shard set). Generalises aggregate_eudr_hires.py beyond AR provinces
— used to extend EUDR coverage to Paraguay (and later Brazil) reusing the same
Hansen raster, vectorized pixel->H3 groupby.

Usage:
  # Paraguay departments from GADM, res-9 + res-7, from existing AR raster shards
  python pipeline/aggregate_eudr_region.py --shp pipeline/data/gadm41_PRY_1.shp \
      --name-field NAME_1 --units "Itapúa,Alto Paraná,Canindeyú,Caazapá,Misiones,Ñeembucú,Presidente Hayes,Boquerón,Alto Paraguay,Central" \
      --province-prefix py --out-tag py --res 9
"""

import argparse
import os
import sys
import time

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.features import geometry_mask
from rasterio.merge import merge as rio_merge

import h3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config_eudr import OUTPUT_DIR
from aggregate_eudr_hires import BANDS, LOSS_YEAR_CODES, post_process, default_rasters


def aggregate_geom(srcs, geom, province_label, res):
    bbox = geom.bounds
    n_bands = min(min(s.count for s in srcs), len(BANDS))
    stack, transform = rio_merge(srcs, bounds=bbox, indexes=list(range(1, n_bands + 1)), nodata=np.nan)
    stack = stack.astype("float32")
    nbands, rows, cols = stack.shape
    if rows == 0 or cols == 0:
        return None

    inside = geometry_mask([geom], out_shape=(rows, cols), transform=transform, invert=True)
    col_idx = np.arange(cols)
    row_idx = np.arange(rows)
    xs = transform.c + (col_idx + 0.5) * transform.a
    ys = transform.f + (row_idx + 0.5) * transform.e
    lon_grid, lat_grid = np.meshgrid(xs, ys)

    tc = stack[BANDS["treecover_2000"] - 1]
    valid = inside & np.isfinite(tc) & (tc >= 0)
    if int(valid.sum()) == 0:
        return None

    lats = lat_grid[valid]
    lons = lon_grid[valid]
    cells = [h3.latlng_to_cell(la, lo, res) for la, lo in zip(lats, lons)]

    data = {"h3index": cells}
    for name, idx in list(BANDS.items())[:nbands]:
        band = stack[idx - 1][valid]
        if name == "fire_post_2020":
            band = np.nan_to_num(band, nan=0.0)
        data[name] = band

    # Per-year post-cutoff loss masks (Hansen lossyear: 21=2021..)
    if "loss_year" in BANDS and BANDS["loss_year"] <= nbands:
        ly = stack[BANDS["loss_year"] - 1][valid]
        for y in LOSS_YEAR_CODES:
            data[f"loss_y{2000 + y}"] = (ly == y).astype("float32")

    df = pd.DataFrame(data)
    agg = df.groupby("h3index").mean()
    agg["province"] = province_label
    return agg.reset_index()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shp", required=True)
    ap.add_argument("--name-field", required=True)
    ap.add_argument("--units", required=True, help="comma-separated unit names to include")
    ap.add_argument("--province-prefix", default="", help="prefix for the province label, e.g. 'py'")
    ap.add_argument("--out-tag", required=True, help="output filename tag, e.g. 'py'")
    ap.add_argument("--res", type=int, default=9)
    ap.add_argument("--raster", nargs="*", default=None)
    args = ap.parse_args()

    rasters = args.raster or default_rasters()
    rasters = [r for r in rasters if os.path.exists(r)]
    if not rasters:
        print("No raster shards found.")
        return 1
    srcs = [rasterio.open(r) for r in rasters]

    gdf = gpd.read_file(args.shp)
    want = [u.strip() for u in args.units.split(",")]
    t0 = time.time()
    parts = []
    for name in want:
        m = gdf[gdf[args.name_field] == name]
        if len(m) == 0:
            print(f"  [skip] '{name}' not found in {args.name_field}")
            continue
        geom = m.geometry.iloc[0]
        label = f"{args.province_prefix}_{name}".strip("_").lower().replace(" ", "_")
        res = aggregate_geom(srcs, geom, label, args.res)
        if res is None:
            print(f"  [skip] '{name}' no valid pixels")
            continue
        print(f"  {name}: {len(res):,} cells ({time.time() - t0:.0f}s)")
        parts.append(res)

    for s in srcs:
        s.close()
    if not parts:
        print("No units aggregated.")
        return 1

    df = pd.concat(parts, ignore_index=True)
    df = df.drop_duplicates(subset="h3index")
    result = post_process(df)

    out_dir = os.path.join(OUTPUT_DIR, "hires")
    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, f"eudr_{args.out_tag}_res{args.res}.parquet")
    result.to_parquet(out, index=False)
    n = len(result)
    ndef = int(result["deforestation_post_2020"].sum())
    print(f"\n  -- {args.out_tag} res-{args.res} --")
    print(f"    cells={n:,}  parquet={os.path.getsize(out)/1024/1024:.1f}MB  "
          f"deforested={ndef:,} ({ndef/n:.1%})  risk_mean={result['risk_score'].mean():.1f}")
    print(f"    saved {out} ({time.time()-t0:.0f}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
