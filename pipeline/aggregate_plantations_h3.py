"""
Aggregate the DNDFI national plantation inventory (macizos) to H3 res-9 to use
as the SDM *presence* signal for forestry_aptitude.

For each AR-NEA territory: bucket each macizo by its centroid into an H3 cell and
sum planted hectares; frac_plantada = planted_ha / hex_area_ha (capped at 1).
This is the ground truth the forestry SDM learns from, and the same inventory the
map overlay shows — so the similarity score and the overlay are the same object.

Most macizos (~3.6 ha mean in Misiones) are smaller than a res-9 hex (~10 ha), so
centroid bucketing is a faithful proxy for presence.

Output: pipeline/output/<t>/dndfi_presence_h3.parquet  (misiones -> flat output/)

Usage:
  python pipeline/aggregate_plantations_h3.py --territory all
"""
import argparse
import os
import sys

import duckdb
import h3
import numpy as np
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, get_territory

H3_RES = 9
DEFAULT_GEOJSON = os.path.join(
    os.path.dirname(os.path.dirname(SCRIPT_DIR)), "dndfi_inventario", "macizos_23_04_2026.geojson"
)
TERRITORY_PROV = {
    "misiones": "MISIONES",
    "corrientes": "CORRIENTES",
    "chaco": "CHACO",
    "formosa": "FORMOSA",
}


def territory_out_dir(t: str) -> str:
    if t == "misiones":
        return OUTPUT_DIR
    return os.path.join(OUTPUT_DIR, get_territory(t)["output_prefix"].rstrip("/"))


def aggregate(con: duckdb.DuckDBPyConnection, prov: str) -> pd.DataFrame:
    rows = con.execute(
        "SELECT ST_X(ST_Centroid(geom)) AS lng, ST_Y(ST_Centroid(geom)) AS lat, "
        "superficie, especie, genero, grupo_espe "
        "FROM macizos WHERE prov = ? AND superficie IS NOT NULL",
        [prov],
    ).fetchall()
    ha: dict[str, float] = {}
    cnt: dict[str, int] = {}
    for lng, lat, sup, *_ in rows:
        ix = h3.latlng_to_cell(lat, lng, H3_RES)
        ha[ix] = ha.get(ix, 0.0) + float(sup)
        cnt[ix] = cnt.get(ix, 0) + 1
    recs = []
    for ix, planted in ha.items():
        area_ha = h3.cell_area(ix, unit="km^2") * 100.0
        recs.append((ix, round(planted, 2), cnt[ix], round(min(1.0, planted / area_ha), 4)))
    df = pd.DataFrame(recs, columns=["h3index", "plantation_ha", "n_macizos", "frac_plantada"])
    return df


def main():
    ap = argparse.ArgumentParser(description="Aggregate DNDFI macizos to H3 presence")
    ap.add_argument("--territory", required=True, help="misiones|corrientes|chaco|formosa|all")
    ap.add_argument("--geojson", default=DEFAULT_GEOJSON)
    args = ap.parse_args()
    if not os.path.exists(args.geojson):
        sys.exit(f"ERROR: missing {args.geojson}")

    territories = list(TERRITORY_PROV) if args.territory == "all" else [args.territory]
    geojson = os.path.abspath(args.geojson).replace("\\", "/")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    provs = tuple(TERRITORY_PROV[t] for t in territories)
    ph = ", ".join("?" for _ in provs)
    print(f"Reading macizos for {provs} ...")
    con.execute(
        f"CREATE TABLE macizos AS SELECT * FROM ST_Read('{geojson}') WHERE prov IN ({ph})",
        list(provs),
    )

    for t in territories:
        df = aggregate(con, TERRITORY_PROV[t])
        out_dir = territory_out_dir(t)
        os.makedirs(out_dir, exist_ok=True)
        out = os.path.join(out_dir, "dndfi_presence_h3.parquet")
        df.to_parquet(out, index=False)
        fp = df["frac_plantada"]
        print(f"{t}: {len(df):,} hexes con plantacion | "
              f"frac>=0.05:{(fp>=0.05).sum():,} >=0.10:{(fp>=0.10).sum():,} "
              f">=0.25:{(fp>=0.25).sum():,} | max={fp.max():.2f} "
              f"| ha total={df['plantation_ha'].sum():,.0f} -> {out}")


if __name__ == "__main__":
    main()
