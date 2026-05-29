"""
Build accessibility and road_access proxy parquets at radio level for AR provinces.

Maps sat_accessibility.parquet (H3 level) → radio level via h3_radio_crosswalk.
Produces oxford_accessibility_{t}.parquet and road_access_{t}.parquet expected
by compute_satellite_scores.py.

If sat_accessibility.parquet doesn't exist, produces empty parquets — compute_satellite_scores
will COALESCE to defaults (300 min travel, 0 road_density), which is a valid
conservative estimate for isolated regions like Chaco/Formosa.

Usage:
  python pipeline/build_census_proxies_ar.py --territory chaco
  python pipeline/build_census_proxies_ar.py --territory formosa
"""
import argparse
import os
import sys

import numpy as np
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, get_territory


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", required=True)
    args = ap.parse_args()

    territory = get_territory(args.territory)
    t = args.territory
    t_dir = os.path.join(OUTPUT_DIR, t)

    crosswalk_path = os.path.join(t_dir, f"h3_radio_crosswalk_{t}.parquet")
    accessibility_path = os.path.join(t_dir, "sat_accessibility.parquet")

    if not os.path.exists(crosswalk_path):
        raise SystemExit(f"Missing {crosswalk_path}. Run build_census_crosswalk_ar.py first.")

    crosswalk = pd.read_parquet(crosswalk_path)
    print(f"Crosswalk: {len(crosswalk):,} rows, {crosswalk['redcode'].nunique():,} radios")

    # oxford_accessibility
    oxford_out = os.path.join(t_dir, f"oxford_accessibility_{t}.parquet")
    road_out = os.path.join(t_dir, f"road_access_{t}.parquet")

    if os.path.exists(accessibility_path):
        print(f"  Using {os.path.basename(accessibility_path)} for proxy")
        acc = pd.read_parquet(accessibility_path, columns=[
            "h3index", "score"  # fallback — use overall score as proxy
        ])
        # Merge hex accessibility → radio via crosswalk weighted average
        merged = crosswalk.merge(acc, on="h3index", how="left")
        # Use score as a proxy for inverse accessibility (higher score = less isolated)
        # Map score 0-100 to travel_min: score 100 = 30 min, score 0 = 300 min
        merged["accessibility_cities_min"] = (300 - merged["score"] * 2.7).clip(30, 300)
        merged["accessibility_healthcare_min"] = merged["accessibility_cities_min"]

        radio_acc = (
            merged.groupby("redcode")
            .apply(lambda g: pd.Series({
                "accessibility_cities_min": np.average(
                    g["accessibility_cities_min"].fillna(300),
                    weights=g["weight"]
                ),
                "accessibility_healthcare_min": np.average(
                    g["accessibility_healthcare_min"].fillna(300),
                    weights=g["weight"]
                ),
            }))
            .reset_index()
        )
    else:
        print(f"  sat_accessibility.parquet not found — using NULL (COALESCE to 300 min)")
        radios = crosswalk["redcode"].unique()
        radio_acc = pd.DataFrame({
            "redcode": radios,
            "accessibility_cities_min": np.nan,
            "accessibility_healthcare_min": np.nan,
        })

    radio_acc.to_parquet(oxford_out, index=False)
    print(f"  oxford_accessibility_{t}.parquet: {len(radio_acc):,} radios")

    # road_access — no road data for Chaco/Formosa; use NULL → COALESCE to 0
    road_df = pd.DataFrame({
        "redcode": crosswalk["redcode"].unique(),
        "dist_primary_m": np.nan,
        "road_density_km_per_km2": np.nan,
    })
    road_df.to_parquet(road_out, index=False)
    print(f"  road_access_{t}.parquet: {len(road_df):,} radios (NULL -> COALESCE defaults)")


if __name__ == "__main__":
    main()
