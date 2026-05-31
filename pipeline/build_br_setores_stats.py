"""
build_br_setores_stats.py
Compute setor-level sociodemographic stats for Brazil (PR/SC/RS) from IBGE Censo 2022
flat-file aggregates. Output mirrors radio_stats_corrientes.parquet schema so the
frontend can reuse the same petal/enrichment machinery.

Variables (all 0-100 scale, matching AR convention):
  redcode              (= cd_setor, 15-digit IBGE setor code)
  area_km2
  total_pessoas
  total_domicilios
  densidad_hab_km2
  pct_agua_rede        % DPPO com água de rede geral de distribuição    [dom2 V00111/V00001]
  pct_esgoto_adequado  % DPPO c/ esgoto rede geral ou fossa ligada      [(V00309+V00310)/V00001]
  pct_lixo_coletado    % DPPO lixo coletado (serviço + caçamba)         [(V00397+V00398)/V00001]
  pct_sem_banheiro     % DPPO sem banheiro exclusivo c/ chuveiro+vaso   [dom2 V00495/V00001]
  pct_alfabetizado     (null — alfabetizacao cross-tab too complex, reserved for future)

V-codes verified against dicionario_de_dados_agregados_por_setores_censitarios_20260520.xlsx:
  basico     : setor=CD_SETOR, pop=v0001, dom=v0002  (lowercase 4-digit codes + AREA_KM2)
  domicilio1 : setor=CD_setor, total_DPPO=V00001      (denominator for infra rates)
  domicilio2 : setor=setor,    agua=V00111, esgoto_rede=V00309, fossa_lig=V00310,
                               lixo_serv=V00397, lixo_cacamba=V00398, sem_banh=V00495

Data sources (IBGE open-data, Decreto 8.777/2016):
  FTP: https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/

Usage:
  python pipeline/build_br_setores_stats.py
  python pipeline/build_br_setores_stats.py --territory parana_br

Pre-requisite: load_ibge_setores.py (setores_br in PostGIS with cd_setor, uf, geom)

Outputs (per territory):
  pipeline/output/setores_stats_parana_br.parquet
  pipeline/output/setores_stats_santa_catarina_br.parquet
  pipeline/output/setores_stats_rio_grande_sul_br.parquet

R2 upload:
  npx wrangler r2 object put neahub/data/setores_stats_<t>.parquet --file ... --remote
"""

import argparse
import io
import os
import sys
import time
import zipfile

import numpy as np
import pandas as pd
import psycopg2
import requests
from sqlalchemy import create_engine

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR

PG = "dbname=ndvi_misiones user=postgres"
PG_URL = "postgresql://postgres@localhost:5432/ndvi_misiones"
FTP_BASE = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/Agregados_por_Setor_csv"

UF_MAP = {
    "parana_br":         ("41", "Paraná"),
    "santa_catarina_br": ("42", "Santa Catarina"),
    "rio_grande_sul_br": ("43", "Rio Grande do Sul"),
}

CSV_FILES = {
    "basico":     f"{FTP_BASE}/Agregados_por_setores_basico_BR_20260520.zip",
    "domicilio1": f"{FTP_BASE}/Agregados_por_setores_caracteristicas_domicilio1_BR.zip",
    "domicilio2": f"{FTP_BASE}/Agregados_por_setores_caracteristicas_domicilio2_BR_20250417.zip",
}


def _download(url: str, desc: str, cache_dir: str) -> bytes:
    fname = os.path.join(cache_dir, os.path.basename(url.split("?")[0]))
    if os.path.exists(fname):
        print(f"  [cache] {desc}")
        with open(fname, "rb") as f:
            return f.read()
    print(f"  downloading {desc} ...", end=" ", flush=True)
    t0 = time.time()
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    with open(fname, "wb") as f:
        f.write(r.content)
    print(f"{len(r.content)/1e6:.1f} MB ({time.time()-t0:.0f}s)")
    return r.content


def _read_csv(zipped: bytes, uf_prefix: str, setor_col: str) -> pd.DataFrame:
    """Unzip a national IBGE CSV, rename setor column to 'redcode', filter to UF."""
    with zipfile.ZipFile(io.BytesIO(zipped)) as zf:
        csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        with zf.open(csv_name) as f:
            df = pd.read_csv(f, sep=";", encoding="latin-1", dtype=str, low_memory=False)
    # Normalise setor column name (IBGE uses CD_SETOR / CD_setor / setor)
    actual_col = next((c for c in df.columns if c.lower() == setor_col.lower()), None)
    if actual_col is None:
        raise ValueError(f"Setor column '{setor_col}' not found. Columns: {list(df.columns[:5])}")
    df = df.rename(columns={actual_col: "redcode"})
    df["redcode"] = df["redcode"].astype(str).str.zfill(15)
    return df[df["redcode"].str.startswith(uf_prefix)].copy()


def _num(series: pd.Series) -> pd.Series:
    """Parse IBGE numeric strings (comma as decimal) to float."""
    return pd.to_numeric(series.str.replace(",", ".", regex=False), errors="coerce")


def _pct(num: pd.Series, den: pd.Series) -> pd.Series:
    with np.errstate(divide="ignore", invalid="ignore"):
        result = num / den.replace(0, np.nan) * 100
    return result.clip(0, 100).round(2)


def build_territory(uf_code: str, label: str, raw: dict) -> pd.DataFrame:
    print(f"\n  [{label} UF={uf_code}]")

    # ── basico: population, domicílios, area ─────────────────────────────────
    bas = _read_csv(raw["basico"], uf_code, "CD_SETOR")
    bas["total_pessoas"]    = _num(bas.get("v0001", pd.Series(dtype=str)))
    bas["total_domicilios"] = _num(bas.get("v0002", pd.Series(dtype=str)))
    bas["area_km2_ibge"]    = _num(bas.get("AREA_KM2", pd.Series(dtype=str)))
    bas = bas[["redcode", "total_pessoas", "total_domicilios", "area_km2_ibge"]].copy()
    print(f"    basico rows={len(bas):,}  pop={int(bas['total_pessoas'].sum()):,}")

    # ── domicilio1: DPPO total (denominator for infra rates) ─────────────────
    # V00001 = Domicílios Particulares Permanentes Ocupados
    dom1 = _read_csv(raw["domicilio1"], uf_code, "CD_setor")
    dom1["dppo"] = _num(dom1.get("V00001", pd.Series(dtype=str)))
    dom1 = dom1[["redcode", "dppo"]].copy()

    # ── domicilio2: all infra variables ──────────────────────────────────────
    # V00111 = DPPO, Utiliza rede geral de distribuição (água)
    # V00309 = DPPO, Esgoto → rede geral ou pluvial
    # V00310 = DPPO, Esgoto → fossa séptica/filtro ligada à rede
    # V00397 = DPPO, Lixo coletado por serviço de limpeza
    # V00398 = DPPO, Lixo depositado em caçamba de serviço de limpeza
    # V00495 = DPPO, Sem banheiro de uso exclusivo com chuveiro e vaso sanitário
    dom2 = _read_csv(raw["domicilio2"], uf_code, "setor")
    needed = ["V00111", "V00309", "V00310", "V00397", "V00398", "V00495"]
    missing = [c for c in needed if c not in dom2.columns]
    if missing:
        raise ValueError(f"Missing columns in domicilio2: {missing}. Available: {list(dom2.columns[:10])}")

    for c in needed:
        dom2[c] = _num(dom2[c])
    dom2 = dom2[["redcode"] + needed].copy()
    print(f"    dom2 rows={len(dom2):,}  V00111 nulls={dom2['V00111'].isna().sum()}")

    # ── Merge all on redcode ──────────────────────────────────────────────────
    df = bas.merge(dom1, on="redcode", how="left")
    df = df.merge(dom2, on="redcode", how="left")

    # ── Compute rates ─────────────────────────────────────────────────────────
    den = df["dppo"]
    df["pct_agua_rede"]       = _pct(df["V00111"], den)
    df["pct_esgoto_adequado"] = _pct(df["V00309"] + df["V00310"], den)
    df["pct_lixo_coletado"]   = _pct(df["V00397"] + df["V00398"], den)
    df["pct_sem_banheiro"]    = _pct(df["V00495"], den)
    df["pct_alfabetizado"]    = np.nan  # reserved — alfabetizacao cross-tab too complex

    # ── Area from IBGE basico (use PostGIS fallback if null) ──────────────────
    con = psycopg2.connect(PG)
    geo = pd.read_sql(
        f"SELECT cd_setor AS redcode, ST_Area(geom::geography)/1e6 AS area_km2_pg "
        f"FROM setores_br WHERE uf='{uf_code}'",
        con,
    )
    con.close()
    geo["redcode"] = geo["redcode"].astype(str).str.zfill(15)
    df = df.merge(geo, on="redcode", how="left")
    df["area_km2"] = df["area_km2_ibge"].fillna(df["area_km2_pg"]).round(4)

    df["densidad_hab_km2"] = (df["total_pessoas"] / df["area_km2"].replace(0, np.nan)).round(2)

    # ── Final schema ──────────────────────────────────────────────────────────
    out_cols = [
        "redcode", "area_km2", "total_pessoas", "total_domicilios", "densidad_hab_km2",
        "pct_agua_rede", "pct_esgoto_adequado", "pct_lixo_coletado",
        "pct_alfabetizado", "pct_sem_banheiro",
    ]
    for c in out_cols:
        if c not in df.columns:
            df[c] = np.nan
    out = df[out_cols].copy()
    print(f"    rows={len(out):,}  agua nulls={out['pct_agua_rede'].isna().sum()}  "
          f"esgoto nulls={out['pct_esgoto_adequado'].isna().sum()}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", default=None, choices=list(UF_MAP.keys()))
    args = ap.parse_args()

    cache_dir = os.path.join(SCRIPT_DIR, "output", "ibge_cache")
    os.makedirs(cache_dir, exist_ok=True)

    territories = [args.territory] if args.territory else list(UF_MAP.keys())

    print("=== Build BR Setores Stats ===")
    print("Downloading IBGE CSV archives...")
    raw = {k: _download(v, k, cache_dir) for k, v in CSV_FILES.items()}

    for territory in territories:
        uf_code, label = UF_MAP[territory]
        df = build_territory(uf_code, label, raw)
        out_path = os.path.join(OUTPUT_DIR, f"setores_stats_{territory}.parquet")
        df.to_parquet(out_path, index=False)
        size_mb = os.path.getsize(out_path) / 1e6
        print(f"  -> {out_path} ({size_mb:.1f} MB)")
        print(f"     Upload: npx wrangler r2 object put neahub/data/setores_stats_{territory}.parquet "
              f"--file \"{out_path}\" --remote")

    print("\nDone.")


if __name__ == "__main__":
    main()
