"""
Build census radio statistics for AR provinces from posadas PostgreSQL DB.

Supports Corrientes (codprov=18), Chaco (codprov=22), Formosa (codprov=34)
via --territory flag, reading codprov from TERRITORY_CONFIGS.

Variables sourced from INDEC Censo Nacional 2022 (national coverage).
Output: pipeline/output/<territory>/radio_stats_<territory>.parquet

Usage:
  python pipeline/build_radio_stats_corrientes.py
  python pipeline/build_radio_stats_corrientes.py --territory chaco
  python pipeline/build_radio_stats_corrientes.py --territory formosa
"""

import argparse
import os
import sys

import psycopg2
import pandas as pd
import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import get_territory

PG_CENSUS = "dbname=posadas user=postgres"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", default="corrientes",
                    help="AR territory id (corrientes|chaco|formosa)")
    args = ap.parse_args()

    t = get_territory(args.territory)
    if t.get('country') != 'ar':
        raise SystemExit(f"--territory {args.territory} is not an AR province")
    codprov = t.get('codprov_indec')
    if codprov is None:
        raise SystemExit(f"TERRITORY_CONFIGS[{args.territory}] missing codprov_indec")
    codprov_str = str(codprov).zfill(2)

    out_dir = os.path.join(SCRIPT_DIR, "output", args.territory)
    out_path = os.path.join(out_dir, f"radio_stats_{args.territory}.parquet")
    os.makedirs(out_dir, exist_ok=True)

    print(f"Building radio stats for {t['label']} (codprov={codprov_str})")
    conn = psycopg2.connect(PG_CENSUS)

    print("Fetching radio base data (population, area, dpto)...")
    base = pd.read_sql("""
        SELECT redcode, dpto, radios_pob, radios_hog,
               COALESCE(radios_sup, 0) AS area_km2
        FROM censo_2022.radios_geom
        WHERE codprov = %s AND redcode IS NOT NULL
    """, conn, params=(codprov_str,))
    print(f"  Base: {len(base)} radios, {base['dpto'].nunique()} dptos")

    if len(base) == 0:
        conn.close()
        raise SystemExit(f"No radios for codprov={codprov_str}. Verify INDEC shapefile loaded.")

    print("Fetching household indicators (NBI view)...")
    nbi = pd.read_sql("""
        SELECT redcode, h_total, h_nbi, h_hacinami, h_propieta, h_computad
        FROM censo_2022.v_censo_nbi_2022
        WHERE codprov = %s AND redcode IS NOT NULL
    """, conn, params=(codprov_str,))
    print(f"  NBI: {len(nbi)} radios")

    print("Fetching education data...")
    edu = pd.read_sql("""
        SELECT redcode, p_total, p18a_unive
        FROM censo_2022.v_censo_niveducativo_2022
        WHERE codprov = %s AND redcode IS NOT NULL
    """, conn, params=(codprov_str,))
    print(f"  Education: {len(edu)} radios")

    print("Fetching labor market data...")
    labor = pd.read_sql("""
        SELECT redcode,
               SUM(ocupados)    AS ocupados,
               SUM(activos)     AS activos,
               SUM(total_pobl)  AS total_pobl
        FROM censo_2022.v_censo_tasaactividad_2022
        WHERE codprov = %s AND redcode IS NOT NULL
        GROUP BY redcode
    """, conn, params=(codprov_str,))
    print(f"  Labor: {len(labor)} radios")

    conn.close()

    df = base.merge(nbi, on='redcode', how='left')
    df = df.merge(edu, on='redcode', how='left')
    df = df.merge(labor, on='redcode', how='left')

    df['total_personas'] = df['radios_pob'].fillna(0).astype(int)
    df['total_hogares']  = df['radios_hog'].fillna(0).astype(int)

    df['densidad_hab_km2'] = np.where(
        df['area_km2'] > 0,
        (df['radios_pob'] / df['area_km2']).round(2),
        0.0
    )
    df['tamano_medio_hogar'] = np.where(
        df['radios_hog'] > 0,
        (df['radios_pob'] / df['radios_hog']).round(2),
        0.0
    )

    h_total = df['h_total'].replace(0, np.nan)
    df['pct_nbi']          = (df['h_nbi']      / h_total * 100).round(2)
    df['pct_hacinamiento'] = (df['h_hacinami'] / h_total * 100).round(2)
    df['pct_propietario']  = (df['h_propieta'] / h_total * 100).round(2)
    df['pct_computadora']  = (df['h_computad'] / h_total * 100).round(2)

    p_total = df['p_total'].replace(0, np.nan)
    df['pct_universitario'] = (df['p18a_unive'] / p_total * 100).round(2)

    total_pobl = df['total_pobl'].replace(0, np.nan)
    df['tasa_empleo']    = (df['ocupados'] / total_pobl * 100).round(2)
    df['tasa_actividad'] = (df['activos']  / total_pobl * 100).round(2)

    out_cols = [
        'redcode', 'dpto', 'area_km2', 'total_personas', 'total_hogares',
        'densidad_hab_km2', 'tamano_medio_hogar',
        'pct_nbi', 'pct_hacinamiento', 'pct_propietario', 'pct_computadora',
        'pct_universitario', 'tasa_empleo', 'tasa_actividad',
    ]
    result = df[out_cols].copy()

    null_nbi = result['pct_nbi'].isna().sum()
    print(f"  Radios with NULL pct_nbi: {null_nbi}/{len(result)}")
    if not result['pct_nbi'].isna().all():
        print(f"  pct_nbi range: {result['pct_nbi'].min():.1f} – {result['pct_nbi'].max():.1f}")
    if not result['tasa_empleo'].isna().all():
        print(f"  tasa_empleo range: {result['tasa_empleo'].min():.1f} – {result['tasa_empleo'].max():.1f}")

    result.to_parquet(out_path, index=False)
    size_kb = os.path.getsize(out_path) / 1024
    print(f"\nSaved {out_path} ({size_kb:.0f} KB, {len(result)} radios)")


if __name__ == "__main__":
    main()
