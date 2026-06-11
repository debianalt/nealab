"""
Department-level census totals (1991 · 2001 · 2010 · 2022) for the censo_temporal
layer — the EXACT INDEC aggregation companion to the hex apportionment of
build_censo_temporal.py.

No geometry, no apportionment: radios are summed directly by INDEC department
code (PROV || DEPTO, zero-padded, format identical across the 4 census years and
matching `redcode` in src/lib/data/ar_dept_boundaries.json). The frontend maps
dept_code -> nombre via that bundled JSON.

Output (one small parquet per territory, ~9-25 rows):
  output/sat_censo_temporal_dept.parquet              (Misiones, prefix '')
  output/<territory>/sat_censo_temporal_dept.parquet  (Corrientes/Chaco/Formosa)

Usage:
  python pipeline/build_censo_temporal_dept.py                 # all 4 territories
  python pipeline/build_censo_temporal_dept.py --territory misiones
"""
import argparse
import os
import sys

import duckdb
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR
from build_censo_temporal import BASE, YEARS, YEAR_KEYS, TERRITORIES, EXPECTED_POB

# Canonical INDEC dept code -> nombre (georef API, apis.datos.gob.ar, 2026-06-11).
# Hardcoded so the build does not depend on the API being up. NOTE: the codes in
# ar_dept_boundaries.json are NOT INDEC for Chaco/Formosa (sequential 22001..) —
# the frontend matches boundary features by nombre, never by code.
DEPT_NAMES: dict[str, str] = {
    # Misiones (54)
    "54007": "Apóstoles", "54014": "Cainguás", "54021": "Candelaria", "54028": "Capital",
    "54035": "Concepción", "54042": "Eldorado", "54049": "General Manuel Belgrano",
    "54056": "Guaraní", "54063": "Iguazú", "54070": "Leandro N. Alem",
    "54077": "Libertador General San Martín", "54084": "Montecarlo", "54091": "Oberá",
    "54098": "San Ignacio", "54105": "San Javier", "54112": "San Pedro", "54119": "25 de Mayo",
    # Corrientes (18)
    "18007": "Bella Vista", "18014": "Berón de Astrada", "18021": "Capital", "18028": "Concepción",
    "18035": "Curuzú Cuatiá", "18042": "Empedrado", "18049": "Esquina", "18056": "General Alvear",
    "18063": "General Paz", "18070": "Goya", "18077": "Itatí", "18084": "Ituzaingó",
    "18091": "Lavalle", "18098": "Mburucuyá", "18105": "Mercedes", "18112": "Monte Caseros",
    "18119": "Paso de los Libres", "18126": "Saladas", "18133": "San Cosme",
    "18140": "San Luis del Palmar", "18147": "San Martín", "18154": "San Miguel",
    "18161": "San Roque", "18168": "Santo Tomé", "18175": "Sauce",
    # Chaco (22)
    "22007": "Almirante Brown", "22014": "Bermejo", "22021": "Comandante Fernández",
    "22028": "Chacabuco", "22036": "12 de Octubre", "22039": "2 de Abril",
    "22043": "Fray Justo Santa María de Oro", "22049": "General Belgrano",
    "22056": "General Donovan", "22063": "General Güemes", "22070": "Independencia",
    "22077": "Libertad", "22084": "Libertador General San Martín", "22091": "Maipú",
    "22098": "Mayor Luis J. Fontana", "22105": "9 de Julio", "22112": "O'Higgins",
    "22119": "Presidencia de la Plaza", "22126": "1º de Mayo", "22133": "Quitilipi",
    "22140": "San Fernando", "22147": "San Lorenzo", "22154": "Sargento Cabral",
    "22161": "Tapenagá", "22168": "25 de Mayo",
    # Formosa (34)
    "34007": "Bermejo", "34014": "Formosa", "34021": "Laishi", "34028": "Matacos",
    "34035": "Patiño", "34042": "Pilagás", "34049": "Pilcomayo", "34056": "Pirané",
    "34063": "Ramón Lista",
}

# Chaco 1991 used a pre-"2 de Abril" code sequence: 22035/22042 exist only in 1991
# and their territories were redrawn when 2 de Abril was created (1992). Those 1991
# values are NOT comparable to any 2001+ department, so the orphan rows are dropped
# and 12 de Octubre / 2 de Abril / Fray Justo keep a null 1991 (the UI shows no bar
# and no Δ% for them).
ORPHAN_CODES = {"22035", "22042"}


def build_territory(con: duckdb.DuckDBPyConnection, name: str) -> pd.DataFrame:
    prefix = TERRITORIES[name][0]
    print(f"\n=== {name} (prov {prefix}) ===")
    wide: pd.DataFrame | None = None
    for year in YEAR_KEYS:
        _cod, pob, viv, _geom = YEARS[year]
        q = (
            f"SELECT CAST(PROV AS VARCHAR) || CAST(DEPTO AS VARCHAR) AS dept_code, "
            f"SUM(CAST({pob} AS DOUBLE)) AS pob_{year}, "
            f"SUM(CAST({viv} AS DOUBLE)) AS viv_{year} "
            f"FROM read_parquet('{BASE}/{year}/radios.parquet') "
            f"WHERE CAST(PROV AS VARCHAR) = '{prefix}' GROUP BY 1"
        )
        df = con.execute(q).df().set_index("dept_code")
        total = float(df[f"pob_{year}"].sum())
        exp = EXPECTED_POB[name][year]
        err = (total - exp) / exp * 100 if exp else 0.0
        flag = "OK" if abs(err) <= 0.5 else "!! CHECK"
        print(f"  {year}: {len(df)} deptos -> sum_pob {total:,.0f} (INDEC {exp:,}, {err:+.2f}%) {flag}")
        wide = df if wide is None else wide.join(df, how="outer")

    dropped = [c for c in wide.index if c in ORPHAN_CODES]
    if dropped:
        lost = wide.loc[dropped, f"pob_{YEAR_KEYS[0]}"].sum()
        print(f"  dropped 1991-only orphan codes {dropped} (pob_1991 {lost:,.0f} not comparable post-redistricting)")
        wide = wide.drop(index=dropped)

    unknown = [c for c in wide.index if c not in DEPT_NAMES]
    if unknown:
        raise SystemExit(f"  !! dept codes without name mapping: {unknown}")

    # Nullable ints: missing year stays null (UI: no bar, no Δ%), never a fake 0.
    wide = wide.round(0).astype("Int64").reset_index()
    wide.insert(1, "dept_name", wide["dept_code"].map(DEPT_NAMES))
    return wide.sort_values("dept_code")


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
        out_path = os.path.join(out_dir, "sat_censo_temporal_dept.parquet")
        df.to_parquet(out_path, index=False)
        size_kb = max(1, os.path.getsize(out_path) // 1024)
        print(f"  saved: {out_path} ({size_kb} KB, {len(df)} deptos)")


if __name__ == "__main__":
    main()
