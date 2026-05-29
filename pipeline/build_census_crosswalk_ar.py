"""
Build H3-radio crosswalk via pure areal interpolation for AR provinces without buildings.

For territories with buildings (Corrientes), use build_crosswalk_corrientes.py instead.
This script uses only the radio polygons from radios_{territory}.parquet and computes
H3 cell weights proportional to the intersection area with each radio.

Usage:
  python pipeline/build_census_crosswalk_ar.py --territory chaco
  python pipeline/build_census_crosswalk_ar.py --territory formosa
"""
import argparse
import os
import sys
import time

import geopandas as gpd
import h3
import numpy as np
import pandas as pd
from shapely.geometry import Polygon

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, get_territory

H3_RESOLUTION = 9
UTM_CRS = "EPSG:32720"  # UTM 20S — covers Chaco/Formosa area


def cell_polygon(hid: str) -> Polygon:
    boundary = h3.cell_to_boundary(hid)
    coords = [(lng, lat) for lat, lng in boundary]
    coords.append(coords[0])
    return Polygon(coords)


def build_crosswalk(radios: gpd.GeoDataFrame) -> pd.DataFrame:
    radios_utm = radios.to_crs(UTM_CRS)
    rows = []
    n = len(radios)
    t0 = time.time()
    for i, (_, radio) in enumerate(radios.iterrows()):
        if i % 200 == 0 and i > 0:
            elapsed = time.time() - t0
            rate = i / elapsed
            eta = (n - i) / rate / 60
            print(f"  {i}/{n} ({rate:.0f} radio/s, ETA {eta:.1f} min)")

        geojson = radio.geometry.__geo_interface__
        hex_ids = list(h3.geo_to_cells(geojson, res=H3_RESOLUTION))
        if not hex_ids:
            continue

        hex_records = [{"h3index": hid, "geometry": cell_polygon(hid)} for hid in hex_ids]
        hex_gdf = gpd.GeoDataFrame(hex_records, crs="EPSG:4326").to_crs(UTM_CRS)
        radio_utm = radios_utm.iloc[[i]]
        radio_area = radio_utm.geometry.area.iloc[0]
        if radio_area <= 0:
            continue

        overlay = gpd.overlay(hex_gdf, radio_utm, how="intersection")
        if overlay.empty:
            continue

        total_intersect = overlay.geometry.area.sum()
        if total_intersect <= 0:
            continue

        for _, row in overlay.iterrows():
            rows.append({
                "h3index": row["h3index"],
                "redcode": radio["redcode"],
                "weight": float(row.geometry.area / total_intersect),
            })

    return pd.DataFrame(rows)


def validate(df: pd.DataFrame):
    weight_sums = df.groupby("redcode")["weight"].sum()
    bad = weight_sums[~weight_sums.between(0.99, 1.01)]
    if len(bad):
        print(f"  WARNING: {len(bad)} radios with weight sum outside [0.99, 1.01]")
    else:
        print(f"  Weight sums OK: {len(weight_sums):,} radios within [0.99, 1.01]")
    print(f"  Total rows: {len(df):,}")
    print(f"  Unique H3 cells: {df['h3index'].nunique():,}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", required=True, help="e.g. chaco, formosa")
    args = ap.parse_args()

    territory = get_territory(args.territory)
    if territory.get("country") != "ar":
        raise SystemExit(f"{args.territory} is not an AR territory")

    t_dir = os.path.join(OUTPUT_DIR, args.territory)
    radios_path = os.path.join(t_dir, f"radios_{args.territory}.parquet")
    output_path = os.path.join(t_dir, f"h3_radio_crosswalk_{args.territory}.parquet")

    if not os.path.exists(radios_path):
        raise SystemExit(f"Missing {radios_path}. Run export_radios_corrientes.py first.")

    print(f"Building H3-radio crosswalk for {territory['label']} (areal interpolation)")
    radios = gpd.read_parquet(radios_path)
    print(f"  {len(radios):,} radios loaded")

    t0 = time.time()
    df = build_crosswalk(radios)
    elapsed = time.time() - t0
    print(f"\nCompleted in {elapsed/60:.1f} min")

    validate(df)

    df.to_parquet(output_path, index=False)
    size_kb = os.path.getsize(output_path) // 1024
    print(f"Saved: {output_path} ({size_kb} KB)")


if __name__ == "__main__":
    main()
