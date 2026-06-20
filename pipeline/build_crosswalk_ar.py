"""
Build the count-weighted (building-density) dasymetric H3-radio crosswalk for
any AR province with a gba_buildings_<territory> table.

This is the SAME method Misiones uses (build_dasymetric_crosswalk.py:87) and
Corrientes (build_crosswalk_corrientes.py): weight = buildings_in_hex /
buildings_in_radio. It REPLACES the pure-areal crosswalk produced by
build_census_crosswalk_ar.py — areal smears census values onto rivers/forests;
this concentrates them where buildings (people) actually are.

Pre-requisite: ingest_gba.py --territory <t>  then  join_gba.py --territory <t>
  (so gba_buildings_<t>.redcode is populated).

Inputs:
  - PostGIS ndvi_misiones.gba_buildings_<territory> (centroids + redcode)
  - pipeline/output/<territory>/radios_<territory>.parquet (areal fallback only)

Output (overwrites the areal crosswalk in place; backs it up once):
  pipeline/output/<territory>/h3_radio_crosswalk_<territory>.parquet
  pipeline/output/<territory>/h3_radio_crosswalk_<territory>_areal.parquet  (backup)

Usage:
  python pipeline/build_crosswalk_ar.py --territory chaco
  python pipeline/build_crosswalk_ar.py --territory formosa

Next:
  python pipeline/aggregate_radio_to_h3.py --territory <t>
  python pipeline/build_census_proxies_ar.py --territory <t>
  python pipeline/compute_satellite_scores.py --territory <t>   (census layers)
"""
import argparse
import os
import shutil
import sys
import time

import geopandas as gpd
import h3
import pandas as pd
import psycopg2
from shapely.geometry import Polygon

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, get_territory

PG_BUILDINGS = "dbname=ndvi_misiones user=postgres"
H3_RESOLUTION = 9
UTM_CRS = "EPSG:32720"  # UTM 20S — covers Chaco/Formosa/Corrientes


def fetch_building_centroids(table: str) -> pd.DataFrame:
    print(f"  Querying {table}...")
    sql = f"""
        SELECT redcode,
               ST_Y(ST_Centroid(geom)) AS lat,
               ST_X(ST_Centroid(geom)) AS lng,
               COALESCE(est_personas, 0) AS est_personas,
               COALESCE(est_hogares, 0) AS est_hogares
        FROM {table}
        WHERE redcode IS NOT NULL
    """
    with psycopg2.connect(PG_BUILDINGS) as conn:
        df = pd.read_sql(sql, conn)
    print(f"    -> {len(df):,} buildings, {df['redcode'].nunique():,} radios")
    return df


def assign_h3(df: pd.DataFrame) -> pd.DataFrame:
    print("  Assigning H3 cells...")
    df["h3index"] = df.apply(
        lambda r: h3.latlng_to_cell(r["lat"], r["lng"], H3_RESOLUTION), axis=1
    )
    return df


def build_dasymetric_weights(df: pd.DataFrame) -> pd.DataFrame:
    print("  Aggregating building counts + population + households...")
    counts = (
        df.groupby(["h3index", "redcode"])
        .agg(n_buildings=("redcode", "size"),
             est_personas=("est_personas", "sum"),
             est_hogares=("est_hogares", "sum"))
        .reset_index()
    )
    radio_totals = counts.groupby("redcode")["n_buildings"].transform("sum")
    counts["weight"] = counts["n_buildings"] / radio_totals
    return counts[["h3index", "redcode", "weight", "n_buildings", "est_personas", "est_hogares"]].reset_index(drop=True)


def cell_polygon(hid: str) -> Polygon:
    boundary = h3.cell_to_boundary(hid)
    coords = [(lng, lat) for lat, lng in boundary]
    coords.append(coords[0])
    return Polygon(coords)


def build_areal_fallback(radios_path: str, radios_with_buildings: set) -> pd.DataFrame:
    """Areal crosswalk for radios with no buildings (rare — keeps weight sums valid)."""
    if not os.path.exists(radios_path):
        print(f"  WARNING: {radios_path} not found — skipping areal fallback")
        return pd.DataFrame(columns=["h3index", "redcode", "weight", "n_buildings", "est_personas", "est_hogares"])

    radios = gpd.read_parquet(radios_path)
    missing = radios[~radios["redcode"].isin(radios_with_buildings)]
    if missing.empty:
        print("  Areal fallback: 0 radios (all have buildings)")
        return pd.DataFrame(columns=["h3index", "redcode", "weight", "n_buildings", "est_personas", "est_hogares"])

    print(f"  Areal fallback for {len(missing)} radios")
    rows = []
    for _, radio in missing.iterrows():
        geojson = radio.geometry.__geo_interface__
        hex_ids = list(h3.geo_to_cells(geojson, res=H3_RESOLUTION))
        if not hex_ids:
            continue
        hex_records = [{"h3index": hid, "geometry": cell_polygon(hid)} for hid in hex_ids]
        hex_gdf = gpd.GeoDataFrame(hex_records, crs="EPSG:4326").to_crs(UTM_CRS)
        radio_gdf = gpd.GeoDataFrame(
            [{"geometry": radio.geometry}], crs="EPSG:4326"
        ).to_crs(UTM_CRS)
        overlay = gpd.overlay(hex_gdf, radio_gdf, how="intersection")
        inter_area = overlay.geometry.area.sum()
        if inter_area <= 0:
            continue
        overlay["weight"] = overlay.geometry.area / inter_area
        for _, row in overlay.iterrows():
            rows.append({"h3index": row["h3index"], "redcode": radio.redcode,
                         "weight": float(row["weight"]),
                         "n_buildings": 0, "est_personas": 0.0, "est_hogares": 0.0})
    return pd.DataFrame(rows)


def validate(df: pd.DataFrame):
    print("\n--- Validation ---")
    n_radios = df["redcode"].nunique()
    print(f"  Radios: {n_radios:,}")
    weight_sums = df.groupby("redcode")["weight"].sum()
    bad = weight_sums[~weight_sums.between(0.99, 1.01)]
    if len(bad) > 0:
        print(f"  WARNING: {len(bad)} radios with weight sum outside [0.99, 1.01]:")
        print(f"    {bad.head(10).to_dict()}")
    else:
        print(f"  Weight sums: all {n_radios:,} radios within [0.99, 1.01]")
    print(f"  Total rows: {len(df):,}  |  unique H3 cells: {df['h3index'].nunique():,}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Count-weighted dasymetric crosswalk for AR provinces")
    ap.add_argument("--territory", required=True)
    args = ap.parse_args()
    t = args.territory
    get_territory(t)  # validate territory id

    t_dir = os.path.join(OUTPUT_DIR, t)
    radios_path = os.path.join(t_dir, f"radios_{t}.parquet")
    out_path = os.path.join(t_dir, f"h3_radio_crosswalk_{t}.parquet")
    areal_backup = os.path.join(t_dir, f"h3_radio_crosswalk_{t}_areal.parquet")
    table = f"gba_buildings_{t}"

    os.makedirs(t_dir, exist_ok=True)
    t0 = time.time()
    print("=" * 60)
    print(f"BUILD DASYMETRIC (count-weighted) CROSSWALK — {t}")
    print("=" * 60)

    # Back up the existing (areal) crosswalk once, before overwriting.
    if os.path.exists(out_path) and not os.path.exists(areal_backup):
        shutil.copy2(out_path, areal_backup)
        print(f"  Backed up existing crosswalk -> {os.path.basename(areal_backup)}")

    print("\nStep 1: Fetch building centroids")
    buildings = fetch_building_centroids(table)
    if buildings.empty:
        print(f"ERROR: {table} has no redcode-assigned buildings. Run join_gba.py first.",
              file=sys.stderr)
        return 1

    print("\nStep 2: Assign H3 cells")
    buildings = assign_h3(buildings)
    print(f"  Unique hexagons with buildings: {buildings['h3index'].nunique():,}")

    print("\nStep 3: Count-weighted dasymetric weights")
    dasy = build_dasymetric_weights(buildings)

    print("\nStep 4: Areal fallback (radios without buildings)")
    fallback = build_areal_fallback(radios_path, set(dasy["redcode"].unique()))
    if not fallback.empty:
        dasy = pd.concat([dasy, fallback], ignore_index=True)
        print(f"  After fallback: {len(dasy):,} rows, {dasy['redcode'].nunique():,} radios")

    print("\nStep 5: Save crosswalk")
    dasy["h3index"] = dasy["h3index"].astype(str)
    dasy["redcode"] = dasy["redcode"].astype(str)
    dasy["weight"] = dasy["weight"].astype("float64")
    dasy["n_buildings"] = dasy["n_buildings"].fillna(0).astype("int64")
    dasy["est_personas"] = dasy["est_personas"].fillna(0).astype("float64")
    dasy["est_hogares"] = dasy["est_hogares"].fillna(0).astype("float64")
    dasy.to_parquet(out_path, index=False)
    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    print(f"  Saved {out_path} ({size_mb:.1f} MB, {len(dasy):,} rows)")

    validate(dasy)
    print(f"\nDone in {time.time() - t0:.0f}s")
    print("\nNext:")
    print(f"  python pipeline/aggregate_radio_to_h3.py --territory {t}")
    print(f"  python pipeline/build_census_proxies_ar.py --territory {t}")
    print(f"  python pipeline/compute_satellite_scores.py --territory {t}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
