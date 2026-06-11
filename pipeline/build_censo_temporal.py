"""
Build the temporal census layer (1991 · 2001 · 2010 · 2022) for the 4 NEA
Argentine territories of Spatia (Misiones, Corrientes, Chaco, Formosa).

Each census year's population/housing TOTALS (full-count) are areal-apportioned
onto the territory's EXISTING populated H3 res-9 universe (the h3index set of the
current census crosswalk), renormalized per radio so the per-territory totals
reconcile to the published INDEC figures. Comparing a hexagon across years is
valid because H3 is the invariant geography (the radio boundaries change between
censuses, the hexagon does not).

Why areal (not dasymetric building-weighting): current building footprints cannot
validly weight 1991 population. Areal interpolation to a common support is the
standard, defensible choice for a temporal series. Consequence: this layer's 2022
density differs slightly from the headline dasymetric 2022 census layer.

Source radios are read REMOTELY from Source Cooperative (no local download):
  https://data.source.coop/nlebovits/censo-argentino/<year>/radios.parquet
Geometry there is EPSG:4326 (geoparquet crs:null => CRS84 lon/lat).

Output (one parquet per territory, same filename / different dir per R2 prefix):
  output/sat_censo_temporal.parquet              (Misiones, prefix '')
  output/<territory>/sat_censo_temporal.parquet  (Corrientes/Chaco/Formosa)

Usage:
  python pipeline/build_censo_temporal.py                 # all 4 territories
  python pipeline/build_censo_temporal.py --territory misiones
"""
import argparse
import os
import sys
import time

import duckdb
import geopandas as gpd
import h3
import numpy as np
import pandas as pd
from shapely.geometry import Polygon

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, H3_RESOLUTION

BASE = "https://data.source.coop/nlebovits/censo-argentino"

# year -> (code column, population total column, housing total column, geometry column)
# 1991/2010 use the full-count 'básico' totals (B_*); 2001/2022 are single-form.
# Geometry column name differs by year: 1991/2001 = 'geom', 2010/2022 = 'geometry'.
YEARS: dict[str, tuple[str, str, str, str]] = {
    "1991": ("COD_1991", "B_POB_TOT", "B_VIV_TOT", "geom"),
    "2001": ("COD_2001", "POB_TOT", "VIV_TOT", "geom"),
    "2010": ("COD_2010", "B_POB_TOT", "B_VIV_TOT", "geometry"),
    "2022": ("COD_2022", "POB_TOT_P", "VIV_TOT_P", "geometry"),
}
YEAR_KEYS = list(YEARS.keys())

# territory -> (INDEC prov prefix, crosswalk path rel OUTPUT_DIR, output subdir, UTM epsg)
# Misiones/Corrientes ~ UTM 21S; Chaco/Formosa ~ UTM 20S.
TERRITORIES: dict[str, tuple[str, str, str, str]] = {
    "misiones":   ("54", "h3_radio_crosswalk.parquet",                       "",           "EPSG:32721"),
    "corrientes": ("18", "corrientes/h3_radio_crosswalk_corrientes.parquet", "corrientes", "EPSG:32721"),
    "chaco":      ("22", "chaco/h3_radio_crosswalk_chaco.parquet",           "chaco",      "EPSG:32720"),
    "formosa":    ("34", "formosa/h3_radio_crosswalk_formosa.parquet",       "formosa",    "EPSG:32720"),
}

# Published INDEC provincial totals (validated against the source via DuckDB probe).
# Used to assert the apportionment reconciles (Σ pob_cnt per year).
EXPECTED_POB: dict[str, dict[str, int]] = {
    "misiones":   {"1991": 788847, "2001": 965522, "2010": 1101593, "2022": 1273347},
    "corrientes": {"1991": 795502, "2001": 930991, "2010": 992595,  "2022": 1209671},
    "chaco":      {"1991": 839677, "2001": 983754, "2010": 1055259, "2022": 1124603},
    "formosa":    {"1991": 398413, "2001": 486559, "2010": 530162,  "2022": 605507},
}


def cell_poly(hid: str) -> Polygon:
    boundary = h3.cell_to_boundary(hid)
    coords = [(lng, lat) for lat, lng in boundary]
    coords.append(coords[0])
    return Polygon(coords)


def load_universe(rel_path: str, utm: str) -> gpd.GeoDataFrame:
    """Populated H3 universe = distinct h3index of the existing census crosswalk."""
    path = os.path.join(OUTPUT_DIR, rel_path)
    cells = pd.unique(pd.read_parquet(path, columns=["h3index"])["h3index"].astype(str))
    gdf = gpd.GeoDataFrame(
        {"h3index": cells},
        geometry=[cell_poly(c) for c in cells],
        crs="EPSG:4326",
    )
    return gdf.to_crs(utm)


def load_radios(con: duckdb.DuckDBPyConnection, year: str, prefix: str, utm: str) -> gpd.GeoDataFrame:
    """Read one census year's radios from Source Cooperative, filtered to the province."""
    cod, pob, viv, geomcol = YEARS[year]
    q = (
        f"SELECT {cod} AS redcode, CAST({pob} AS DOUBLE) AS pob, "
        f"CAST({viv} AS DOUBLE) AS viv, {geomcol} AS geom "
        f"FROM read_parquet('{BASE}/{year}/radios.parquet') "
        f"WHERE substr({cod}, 1, 2) = '{prefix}'"
    )
    df = con.execute(q).df()
    # geoparquet stores geometry as WKB binary; crs:null => set to 4326 explicitly.
    geom = gpd.GeoSeries.from_wkb([bytes(b) for b in df["geom"].values], crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(df.drop(columns=["geom"]), geometry=geom, crs="EPSG:4326")
    gdf = gdf[gdf.geometry.notna() & (gdf["pob"].fillna(0) >= 0)]
    return gdf.to_crs(utm)


def apportion(universe_utm: gpd.GeoDataFrame, radios_utm: gpd.GeoDataFrame) -> pd.DataFrame:
    """Areal apportionment of radio pob/viv onto universe cells, renormalized per radio.

    Weights for each radio sum to 1.0 over the universe cells it intersects, so the
    full radio population lands inside the populated universe (reconciles to INDEC).
    Radios that miss the universe entirely fall back to their nearest universe cell.
    """
    inter = gpd.overlay(
        universe_utm[["h3index", "geometry"]],
        radios_utm[["redcode", "pob", "viv", "geometry"]],
        how="intersection",
        keep_geom_type=True,
    )
    out = pd.DataFrame(columns=["pob_c", "viv_c"]).rename_axis("h3index")
    if not inter.empty:
        inter["a"] = inter.geometry.area
        denom = inter.groupby("redcode")["a"].transform("sum")
        inter["w"] = np.where(denom > 0, inter["a"] / denom, 0.0)
        inter["pob_c"] = inter["w"] * inter["pob"]
        inter["viv_c"] = inter["w"] * inter["viv"]
        out = inter.groupby("h3index")[["pob_c", "viv_c"]].sum()

    covered = set(inter["redcode"].unique()) if not inter.empty else set()
    orphans = radios_utm[~radios_utm["redcode"].isin(covered)]
    if len(orphans):
        cent = universe_utm[["h3index"]].copy()
        cent = cent.set_geometry(universe_utm.geometry.centroid)
        orph_pts = orphans[["redcode", "pob", "viv"]].copy()
        orph_pts = gpd.GeoDataFrame(orph_pts, geometry=orphans.geometry.centroid, crs=radios_utm.crs)
        near = gpd.sjoin_nearest(orph_pts, cent, how="left")
        o = near.groupby("h3index")[["pob", "viv"]].sum().rename(columns={"pob": "pob_c", "viv": "viv_c"})
        out = out.add(o, fill_value=0)
    return out


def build_territory(con: duckdb.DuckDBPyConnection, name: str) -> pd.DataFrame:
    prefix, xwalk, _subdir, utm = TERRITORIES[name]
    print(f"\n=== {name} (prov {prefix}, {utm}) ===")
    t0 = time.time()
    universe = load_universe(xwalk, utm)
    print(f"  universe: {len(universe):,} populated H3 cells")

    wide: pd.DataFrame | None = None
    for year in YEAR_KEYS:
        radios = load_radios(con, year, prefix, utm)
        agg = apportion(universe, radios)
        total = float(agg["pob_c"].sum())
        exp = EXPECTED_POB[name][year]
        err = (total - exp) / exp * 100 if exp else 0.0
        flag = "OK" if abs(err) <= 2.0 else "!! CHECK"
        print(f"  {year}: {len(radios):,} radios -> sum_pob {total:,.0f} (INDEC {exp:,}, {err:+.2f}%) {flag}")
        agg = agg.rename(columns={"pob_c": f"pob_cnt_{year}", "viv_c": f"viv_cnt_{year}"})
        wide = agg if wide is None else wide.join(agg, how="outer")

    wide = wide.reset_index()
    # density (hab/km², viv/km²) per cell — res-9 area varies slightly with latitude.
    area_km2 = wide["h3index"].map(lambda h: h3.cell_area(h, unit="km^2"))
    for year in YEAR_KEYS:
        wide[f"pob_dens_{year}"] = wide[f"pob_cnt_{year}"] / area_km2
        wide[f"viv_dens_{year}"] = wide[f"viv_cnt_{year}"] / area_km2

    # Temporal-toggle columns consumed by the existing satellite-temporal UI:
    # current = 2022, baseline = 2010, delta = 2022 − 2010. primaryVariable = pob_dens.
    wide["pob_dens"] = wide["pob_dens_2022"]
    wide["pob_dens_baseline"] = wide["pob_dens_2010"]
    wide["pob_dens_delta"] = wide["pob_dens_2022"] - wide["pob_dens_2010"]
    wide["viv_dens"] = wide["viv_dens_2022"]
    wide["viv_dens_baseline"] = wide["viv_dens_2010"]
    wide["viv_dens_delta"] = wide["viv_dens_2022"] - wide["viv_dens_2010"]

    # Round to keep the parquet small; counts to int-ish, densities to 1 decimal.
    for c in wide.columns:
        if c == "h3index":
            continue
        wide[c] = wide[c].round(2)
    print(f"  -> {len(wide):,} hexagons, {time.time()-t0:.1f}s")
    return wide


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", default="all", help="misiones|corrientes|chaco|formosa|all")
    args = ap.parse_args()
    targets = list(TERRITORIES) if args.territory == "all" else [args.territory]

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    for name in targets:
        if name not in TERRITORIES:
            raise SystemExit(f"Unknown territory '{name}'. Options: {list(TERRITORIES)}")
        df = build_territory(con, name)
        subdir = TERRITORIES[name][2]
        out_dir = os.path.join(OUTPUT_DIR, subdir) if subdir else OUTPUT_DIR
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "sat_censo_temporal.parquet")
        df.to_parquet(out_path, index=False)
        size_kb = os.path.getsize(out_path) // 1024
        print(f"  saved: {out_path} ({size_kb} KB)")


if __name__ == "__main__":
    main()
