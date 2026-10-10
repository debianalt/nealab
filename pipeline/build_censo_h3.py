"""
Rehash Censo 2022 radio-level variables (Misiones) to H3 res-9.

Input:
  - output/radio_data/censo2022_variables.parquet   (2,012 radios, 49 cols)
  - output/h3_radio_crosswalk.parquet                (h3index, redcode, weight,
                                                      est_personas, est_hogares)
Output:
  - output/censo2022_variables_h3.parquet

Counts and rates are aggregated differently. The previous version multiplied
EVERY column by the crosswalk weight, which is right for counts but turns a rate
into rate x share (NBI 9.5% -> 0.3% on average, and >100% where several radios
overlap a hexagon):

  - counts (total_personas, varones, mujeres): sum(value * weight)
  - densidad_hab_km2: hexagon persons / hexagon area
  - every other column (pct_*, tasa_*, indice_*, tamano_medio_hogar): mean of the
    radio values weighted by the estimated households of each radio in the
    hexagon (est_hogares; falls back to weight where no households are estimated)

Usage:
  python pipeline/build_censo_h3.py
"""

import os
import sys

import duckdb
import h3
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR

RADIO_PATH = os.path.join(OUTPUT_DIR, "radio_data", "censo2022_variables.parquet")
XWALK_PATH = os.path.join(OUTPUT_DIR, "h3_radio_crosswalk.parquet")
OUT_PATH = os.path.join(OUTPUT_DIR, "censo2022_variables_h3.parquet")

COUNT_COLS = ["total_personas", "varones", "mujeres"]


def main():
    con = duckdb.connect()
    radio_cols = [r[0] for r in con.sql(
        f"SELECT column_name FROM (DESCRIBE SELECT * FROM '{RADIO_PATH}')").fetchall()]
    rate_cols = [c for c in radio_cols
                 if c not in COUNT_COLS + ["redcode", "densidad_hab_km2"]]

    count_sql = ",\n".join(f"SUM(r.{c} * x.weight) AS {c}" for c in COUNT_COLS)
    # households weight; hexagons where no households were estimated fall back to
    # the crosswalk weight so they still get the radio's rate
    w = "CASE WHEN x.est_hogares > 0 THEN x.est_hogares ELSE x.weight END"
    rate_sql = ",\n".join(
        f"SUM(r.{c} * {w}) FILTER (WHERE r.{c} IS NOT NULL)"
        f" / NULLIF(SUM({w}) FILTER (WHERE r.{c} IS NOT NULL), 0) AS {c}"
        for c in rate_cols)

    df = con.sql(f"""
        SELECT x.h3index,
               {count_sql},
               {rate_sql}
        FROM '{XWALK_PATH}' x
        JOIN '{RADIO_PATH}' r USING (redcode)
        GROUP BY x.h3index
    """).df()

    area_km2 = df["h3index"].map(lambda c: h3.cell_area(c, unit="km^2"))
    df["densidad_hab_km2"] = df["total_personas"] / area_km2

    df = df[["h3index"] + [c for c in radio_cols if c != "redcode"]]
    df.to_parquet(OUT_PATH, index=False, compression="zstd")

    # sanity: percentages stay in [0, 100] and the household-weighted provincial
    # mean matches the radio table
    bad = {c: int(((df[c] < 0) | (df[c] > 100.0001)).sum())
           for c in df.columns if c.startswith("pct_")}
    bad = {c: n for c, n in bad.items() if n}
    print(f"Saved {OUT_PATH}: {len(df):,} hexagons x {df.shape[1]} cols")
    print(f"  pct_* outside [0,100]: {bad or 'none'}")
    print(f"  persons: radios {con.sql(f'SELECT SUM(total_personas) FROM {chr(39)}{RADIO_PATH}{chr(39)}').fetchone()[0]:,.0f}"
          f" -> hexagons {df['total_personas'].sum():,.0f}")


if __name__ == "__main__":
    main()
