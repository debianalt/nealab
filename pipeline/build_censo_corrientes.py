"""
Extract census variables from PostGIS for AR provinces.

Supports Corrientes (codprov=18), Chaco (codprov=22), Formosa (codprov=34)
via --territory flag. codprov resolved from TERRITORY_CONFIGS.

Output: pipeline/output/<territory>/censo2022_variables_<territory>.parquet
Schema matches censo2022_variables (Misiones) for compute_satellite_scores.py.

Usage:
  python pipeline/build_censo_corrientes.py
  python pipeline/build_censo_corrientes.py --territory chaco
  python pipeline/build_censo_corrientes.py --territory formosa
"""

import argparse
import os
import sys

import pandas as pd
import psycopg2

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, get_territory


def safe_pct(num, den):
    return (num / den.replace(0, float("nan")) * 100).round(2)


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
    redcode_prefix = codprov_str + '%%'

    out_path = os.path.join(OUTPUT_DIR, args.territory,
                            f"censo2022_variables_{args.territory}.parquet")

    print(f"Building censo variables for {t['label']} (codprov={codprov_str})")
    conn = psycopg2.connect(dbname="posadas", user="postgres")

    hog = pd.read_sql(
        """
        SELECT redcode,
            h_total, h_nbi, h_cloaca, h_piso,
            h_hacinami, h_hacina_1, h_computad, h_agua_red
        FROM censo_2022.v_censo_nbi_2022
        WHERE codprov = %s
        """,
        conn, params=(codprov_str,)
    )

    per = pd.read_sql(
        """
        SELECT redcode,
            p_total, p_a17, p18a_sin_i, p18a_solos, p18a_terci, p18a_unive,
            p_18a, p_cobertur, p65_total
        FROM censo_2022.v_censo_niveducativo_2022
        WHERE redcode LIKE %s
        """,
        conn, params=(redcode_prefix,)
    )

    inas = pd.read_sql(
        """
        SELECT redcode, nasiste6a1, total_6a12, nasiste_13, total13_18
        FROM censo_2022.v_censo_children_noasisten_2022
        WHERE redcode LIKE %s AND sexo = 0
        """,
        conn, params=(redcode_prefix,)
    )

    fec = pd.read_sql(
        """
        SELECT redcode, ma_14a17, m_14a17
        FROM censo_2022.v_censo_fecundidad_2022
        WHERE redcode LIKE %s
        """,
        conn, params=(redcode_prefix,)
    )

    conn.close()

    print(f"  hogares: {len(hog)} rows")
    print(f"  personas: {len(per)} rows")
    print(f"  inasistencia: {len(inas)} rows")
    print(f"  fecundidad: {len(fec)} rows")

    if len(hog) == 0:
        raise SystemExit(f"No census data for codprov={codprov_str}. Verify "
                         f"INDEC views loaded in censo_2022.* schema.")

    radio_stats_path = os.path.join(OUTPUT_DIR, args.territory,
                                     f"radio_stats_{args.territory}.parquet")
    if not os.path.exists(radio_stats_path):
        raise SystemExit(
            f"Missing {radio_stats_path}. Run build_radio_stats_corrientes.py "
            f"(or equivalent for {args.territory}) first."
        )
    radio_stats = pd.read_parquet(radio_stats_path,
                                   columns=["redcode", "densidad_hab_km2"])

    df = (
        hog.merge(per, on="redcode", how="outer")
        .merge(inas, on="redcode", how="left")
        .merge(fec, on="redcode", how="left")
        .merge(radio_stats, on="redcode", how="left")
    )

    result = pd.DataFrame()
    result["redcode"] = df["redcode"]
    result["densidad_hab_km2"] = df["densidad_hab_km2"].round(2)
    result["pct_nbi"] = safe_pct(df["h_nbi"], df["h_total"])
    result["pct_cloacas"] = safe_pct(df["h_cloaca"], df["h_total"])
    result["pct_sin_piso_adecuado"] = safe_pct(df["h_piso"], df["h_total"])
    result["pct_hacinamiento"] = safe_pct(df["h_hacinami"], df["h_total"])
    result["pct_hacinamiento_critico"] = safe_pct(df["h_hacina_1"], df["h_total"])
    result["pct_computadora"] = safe_pct(df["h_computad"], df["h_total"])
    result["pct_cobertura_salud"] = safe_pct(df["p_cobertur"], df["p_total"])
    result["pct_adultos_mayores"] = safe_pct(df["p65_total"], df["p_total"])
    result["pct_menores_18"] = safe_pct(df["p_a17"], df["p_total"])
    result["pct_sin_instruccion"] = safe_pct(df["p18a_sin_i"], df["p_18a"])
    result["pct_secundario_comp"] = safe_pct(
        df["p18a_solos"] + df["p18a_terci"] + df["p18a_unive"], df["p_18a"]
    )
    result["pct_terciario"] = safe_pct(df["p18a_terci"] + df["p18a_unive"], df["p_18a"])
    result["pct_universitario"] = safe_pct(df["p18a_unive"], df["p_18a"])
    result["tasa_inasistencia_6a12"] = safe_pct(df["nasiste6a1"], df["total_6a12"])
    result["tasa_inasistencia_13a18"] = safe_pct(df["nasiste_13"], df["total13_18"])
    result["tasa_maternidad_adolescente"] = safe_pct(df["ma_14a17"], df["m_14a17"])

    result = result.dropna(subset=["redcode"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    result.to_parquet(out_path, index=False)
    print(f"Rows: {len(result)}")
    print(f"Columns: {list(result.columns)}")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
