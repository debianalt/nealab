"""
build_br_setores_stats.py
Compute setor-level sociodemographic stats for Brazil (PR/SC/RS) from IBGE Censo 2022
flat-file aggregates. Output mirrors radio_stats_corrientes.parquet schema so the
frontend can reuse the same petal/enrichment machinery.

Variables:
  redcode           (= cd_setor, 15-digit IBGE setor code)
  area_km2
  total_pessoas
  total_domicilios
  densidad_hab_km2
  pct_agua_rede       % domicílios c/ água de rede geral (saneamento — infra)
  pct_esgoto_adequado % domicílios c/ esgoto rede geral ou fossa séptica
  pct_lixo_coletado   % domicílios c/ lixo coletado (direto ou indireto)
  pct_alfabetizado    % pessoas de 10+ anos alfabetizadas
  pct_sem_banheiro    % domicílios sem banheiro de uso exclusivo

Data sources (all public, IBGE open-data, Decreto 8.777/2016):
  FTP: https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/

V-code resolution: script downloads the data dictionary XLSX and resolves variable
codes automatically — avoids hardcoding codes that may change between IBGE releases.

Usage:
  python pipeline/build_br_setores_stats.py
  python pipeline/build_br_setores_stats.py --territory parana_br   (single state)

Pre-requisite: load_ibge_setores.py (setores_br in PostGIS with cd_setor, uf, area geometry)

Outputs:
  pipeline/output/setores_stats_parana_br.parquet
  pipeline/output/setores_stats_santa_catarina_br.parquet
  pipeline/output/setores_stats_rio_grande_sul_br.parquet

R2 upload:
  npx wrangler r2 object put neahub/data/setores_stats_parana_br.parquet --file ... --remote
  (repeat for each territory)
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

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR

PG = "dbname=ndvi_misiones user=postgres"
FTP_BASE = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios"

UF_MAP = {
    "parana_br":         ("41", "Paraná"),
    "santa_catarina_br": ("42", "Santa Catarina"),
    "rio_grande_sul_br": ("43", "Rio Grande do Sul"),
}

# ── IBGE file URLs (Agregados por Setor CSV) ──────────────────────────────────
CSV_FILES = {
    "basico":      f"{FTP_BASE}/Agregados_por_Setor_csv/Agregados_por_setores_basico_BR_20260520.zip",
    "domicilio1":  f"{FTP_BASE}/Agregados_por_Setor_csv/Agregados_por_setores_caracteristicas_domicilio1_BR.zip",
    "domicilio2":  f"{FTP_BASE}/Agregados_por_Setor_csv/Agregados_por_setores_caracteristicas_domicilio2_BR_20250417.zip",
    "alfabetizacao": f"{FTP_BASE}/Agregados_por_Setor_csv/Agregados_por_setores_alfabetizacao_BR.zip",
}
DICT_URL = (
    f"{FTP_BASE}/dicionario_de_dados_agregados_por_setores_censitarios_20260520.xlsx"
)

# ── Variable search terms for auto-resolution from dictionary ─────────────────
# Keys: (table_tag, search_term) → output column
VAR_TARGETS = {
    ("basico",       "moradores em domicílios"):          "pop_total",
    ("basico",       "domicílios particulares"):          "dom_total",
    ("domicilio1",   "rede geral de distribuição"):       "dom_agua_rede",
    ("domicilio1",   "rede coletora"):                    "dom_esgoto_rede",
    ("domicilio1",   "fossa séptica ligada"):             "dom_fossa_ligada",
    ("domicilio1",   "coletado por serviço"),             "dom_lixo_coletado",
    ("domicilio2",   "sem banheiro"):                     "dom_sem_banheiro",
    ("domicilio2",   "domicílios particulares"):          "dom2_total",
    ("alfabetizacao","pessoas de 10 anos ou mais"),       "alf_total",
    ("alfabetizacao","alfabetizadas"):                    "alf_alfabetizado",
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


def _load_dict(cache_dir: str) -> dict:
    """Parse IBGE data dictionary XLSX → {(table_tag, vcode): label}."""
    try:
        import openpyxl
    except ImportError:
        print("  [warn] openpyxl not installed — skipping dictionary parse; using fallback V-codes")
        return {}
    raw = _download(DICT_URL, "data dictionary XLSX", cache_dir)
    wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    lookup = {}
    for sheet in wb.worksheets:
        tag = sheet.title.lower().replace(" ", "_")
        for row in sheet.iter_rows(values_only=True):
            if not row or len(row) < 2:
                continue
            code, label = str(row[0] or "").strip(), str(row[1] or "").strip().lower()
            if code.startswith("V") and label:
                lookup[(tag, code)] = label
    wb.close()
    return lookup


def _resolve_vcodes(dict_lookup: dict) -> dict:
    """Map output column names → (table_tag, vcode) via dictionary search."""
    resolved = {}
    # Fallback V-codes (IBGE Censo 2022 — verified against {censobr} package docs)
    fallback = {
        "pop_total":        ("basico",       "V001"),
        "dom_total":        ("basico",       "V002"),
        "dom_agua_rede":    ("domicilio1",   "V004"),
        "dom_esgoto_rede":  ("domicilio1",   "V016"),
        "dom_fossa_ligada": ("domicilio1",   "V017"),
        "dom_lixo_coletado":("domicilio1",   "V030"),
        "dom_sem_banheiro": ("domicilio2",   "V002"),
        "dom2_total":       ("domicilio2",   "V001"),
        "alf_total":        ("alfabetizacao","V001"),
        "alf_alfabetizado": ("alfabetizacao","V003"),
    }
    for out_col, (tag, fallback_code) in fallback.items():
        # Search dictionary for better match
        best_code = fallback_code
        if dict_lookup:
            for (d_tag, code), label in dict_lookup.items():
                if d_tag != tag:
                    continue
                for (t, term), target_col in VAR_TARGETS.items():
                    if target_col == out_col and t == tag and term.lower() in label:
                        best_code = code
                        break
        resolved[out_col] = (tag, best_code)
    return resolved


def _read_ibge_csv(zipped: bytes, uf_prefix: str) -> pd.DataFrame:
    """Unzip a national IBGE CSV archive and return rows for the given UF prefix."""
    with zipfile.ZipFile(io.BytesIO(zipped)) as zf:
        csvname = next(
            (n for n in zf.namelist() if n.lower().endswith(".csv") and "BR" in n.upper()),
            zf.namelist()[0],
        )
        with zf.open(csvname) as f:
            # IBGE CSVs are ';'-delimited, Latin-1 encoded
            df = pd.read_csv(f, sep=";", encoding="latin-1", dtype=str, low_memory=False)
    # IBGE uses CD_SETOR or Cod_setor as the tract key
    setor_col = next((c for c in df.columns if "setor" in c.lower() or "cd_geo" in c.lower()), df.columns[0])
    df = df.rename(columns={setor_col: "cd_setor"})
    df["cd_setor"] = df["cd_setor"].astype(str).str.zfill(15)
    # Filter to this UF
    df = df[df["cd_setor"].str.startswith(uf_prefix)].copy()
    return df


def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    with np.errstate(divide="ignore", invalid="ignore"):
        result = pd.to_numeric(num, errors="coerce") / pd.to_numeric(den, errors="coerce").replace(0, np.nan)
    return result.clip(0, 1)


def build_territory(uf_code: str, label: str, raw: dict, vcodes: dict,
                    cache_dir: str) -> pd.DataFrame:
    print(f"\n  [{label} UF={uf_code}]")

    # ── Geometry + area from PostGIS ─────────────────────────────────────────
    con = psycopg2.connect(PG)
    geom_df = pd.read_sql(
        f"SELECT cd_setor AS redcode, "
        f"ST_Area(geom::geography)/1e6 AS area_km2, "
        f"total_personas, total_domicilios "
        f"FROM setores_br WHERE uf='{uf_code}'",
        con,
    )
    con.close()
    geom_df["redcode"] = geom_df["redcode"].astype(str).str.zfill(15)
    print(f"    setores={len(geom_df):,}  pop={int(geom_df['total_personas'].sum()):,}")

    # ── Basico CSV ────────────────────────────────────────────────────────────
    bas = _read_ibge_csv(raw["basico"], uf_code)
    pop_col   = vcodes["pop_total"][1]
    dom_col   = vcodes["dom_total"][1]
    if pop_col in bas.columns:
        geom_df = geom_df.merge(
            bas[["cd_setor", pop_col, dom_col]].rename(columns={
                pop_col: "ibge_pop", dom_col: "ibge_dom"
            }),
            left_on="redcode", right_on="cd_setor", how="left",
        ).drop(columns=["cd_setor"], errors="ignore")
        # Use IBGE basico as authoritative population if available
        geom_df["total_pessoas"] = pd.to_numeric(geom_df["ibge_pop"], errors="coerce").fillna(
            geom_df["total_personas"]
        ).astype(int)
        geom_df["total_domicilios"] = pd.to_numeric(geom_df["ibge_dom"], errors="coerce").fillna(
            geom_df["total_domicilios"]
        ).astype(int)
    else:
        geom_df = geom_df.rename(columns={"total_personas": "total_pessoas"})
    geom_df.drop(columns=["ibge_pop", "ibge_dom"], errors="ignore", inplace=True)

    # ── Domicilio1 ────────────────────────────────────────────────────────────
    dom1 = _read_ibge_csv(raw["domicilio1"], uf_code)
    d1_total = vcodes["dom_total"][1]
    d1_agua  = vcodes["dom_agua_rede"][1]
    d1_esgt  = vcodes["dom_esgoto_rede"][1]
    d1_foss  = vcodes["dom_fossa_ligada"][1]
    d1_lixo  = vcodes["dom_lixo_coletado"][1]
    cols1 = [c for c in [d1_total, d1_agua, d1_esgt, d1_foss, d1_lixo] if c in dom1.columns]
    if cols1:
        geom_df = geom_df.merge(
            dom1[["cd_setor"] + cols1],
            left_on="redcode", right_on="cd_setor", how="left",
        ).drop(columns=["cd_setor"], errors="ignore")
        dom_base = pd.to_numeric(geom_df.get(d1_total, pd.Series(dtype=float)), errors="coerce")
        dom_base = dom_base.where(dom_base > 0, geom_df["total_domicilios"])
        if d1_agua in geom_df.columns:
            geom_df["pct_agua_rede"] = _safe_div(
                pd.to_numeric(geom_df[d1_agua], errors="coerce"), dom_base
            )
        else:
            geom_df["pct_agua_rede"] = np.nan
        esgoto_num = pd.to_numeric(geom_df.get(d1_esgt, pd.Series(dtype=float)), errors="coerce").fillna(0)
        fossa_num  = pd.to_numeric(geom_df.get(d1_foss, pd.Series(dtype=float)), errors="coerce").fillna(0)
        geom_df["pct_esgoto_adequado"] = _safe_div(esgoto_num + fossa_num, dom_base)
        if d1_lixo in geom_df.columns:
            geom_df["pct_lixo_coletado"] = _safe_div(
                pd.to_numeric(geom_df[d1_lixo], errors="coerce"), dom_base
            )
        else:
            geom_df["pct_lixo_coletado"] = np.nan
        geom_df.drop(columns=cols1, errors="ignore", inplace=True)
    else:
        for c in ["pct_agua_rede", "pct_esgoto_adequado", "pct_lixo_coletado"]:
            geom_df[c] = np.nan

    # ── Domicilio2 ────────────────────────────────────────────────────────────
    dom2 = _read_ibge_csv(raw["domicilio2"], uf_code)
    d2_total = vcodes["dom2_total"][1]
    d2_sem   = vcodes["dom_sem_banheiro"][1]
    cols2 = [c for c in [d2_total, d2_sem] if c in dom2.columns]
    if cols2:
        geom_df = geom_df.merge(
            dom2[["cd_setor"] + cols2],
            left_on="redcode", right_on="cd_setor", how="left",
        ).drop(columns=["cd_setor"], errors="ignore")
        dom2_base = pd.to_numeric(geom_df.get(d2_total, pd.Series(dtype=float)), errors="coerce")
        dom2_base = dom2_base.where(dom2_base > 0, geom_df["total_domicilios"])
        if d2_sem in geom_df.columns:
            geom_df["pct_sem_banheiro"] = _safe_div(
                pd.to_numeric(geom_df[d2_sem], errors="coerce"), dom2_base
            )
        else:
            geom_df["pct_sem_banheiro"] = np.nan
        geom_df.drop(columns=cols2, errors="ignore", inplace=True)
    else:
        geom_df["pct_sem_banheiro"] = np.nan

    # ── Alfabetização ─────────────────────────────────────────────────────────
    alf = _read_ibge_csv(raw["alfabetizacao"], uf_code)
    a_tot  = vcodes["alf_total"][1]
    a_alfa = vcodes["alf_alfabetizado"][1]
    cols_a = [c for c in [a_tot, a_alfa] if c in alf.columns]
    if cols_a:
        geom_df = geom_df.merge(
            alf[["cd_setor"] + cols_a],
            left_on="redcode", right_on="cd_setor", how="left",
        ).drop(columns=["cd_setor"], errors="ignore")
        alf_base = pd.to_numeric(geom_df.get(a_tot, pd.Series(dtype=float)), errors="coerce")
        geom_df["pct_alfabetizado"] = _safe_div(
            pd.to_numeric(geom_df.get(a_alfa, pd.Series(dtype=float)), errors="coerce"),
            alf_base,
        )
        geom_df.drop(columns=cols_a, errors="ignore", inplace=True)
    else:
        geom_df["pct_alfabetizado"] = np.nan

    # ── Derived ───────────────────────────────────────────────────────────────
    geom_df["densidad_hab_km2"] = (
        geom_df["total_pessoas"] / geom_df["area_km2"].replace(0, np.nan)
    ).round(2)

    # ── Final schema ─────────────────────────────────────────────────────────
    out_cols = [
        "redcode", "area_km2", "total_pessoas", "total_domicilios", "densidad_hab_km2",
        "pct_agua_rede", "pct_esgoto_adequado", "pct_lixo_coletado",
        "pct_alfabetizado", "pct_sem_banheiro",
    ]
    for c in out_cols:
        if c not in geom_df.columns:
            geom_df[c] = np.nan
    out = geom_df[out_cols].copy()

    # Convert pct columns to 0–100 range (matching AR convention: 0–100 percentages)
    for c in ["pct_agua_rede", "pct_esgoto_adequado", "pct_lixo_coletado",
              "pct_alfabetizado", "pct_sem_banheiro"]:
        out[c] = (out[c] * 100).round(2)

    print(f"    rows={len(out):,}  nulls in pct_agua_rede={out['pct_agua_rede'].isna().sum()}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", default=None, choices=list(UF_MAP.keys()),
                    help="Process one territory only (default: all three)")
    args = ap.parse_args()

    cache_dir = os.path.join(SCRIPT_DIR, "output", "ibge_cache")
    os.makedirs(cache_dir, exist_ok=True)

    territories = [args.territory] if args.territory else list(UF_MAP.keys())

    print("=== Build BR Setores Stats ===")
    print("Step 1: Resolve IBGE V-codes from data dictionary...")
    dict_lookup = _load_dict(cache_dir)
    vcodes = _resolve_vcodes(dict_lookup)
    print(f"  V-codes: {vcodes}")

    print("\nStep 2: Download IBGE CSV archives...")
    raw = {}
    for key, url in CSV_FILES.items():
        raw[key] = _download(url, key, cache_dir)

    print("\nStep 3: Build stats per territory...")
    for territory in territories:
        uf_code, label = UF_MAP[territory]
        df = build_territory(uf_code, label, raw, vcodes, cache_dir)
        out_path = os.path.join(OUTPUT_DIR, f"setores_stats_{territory}.parquet")
        df.to_parquet(out_path, index=False)
        print(f"  -> {out_path} ({os.path.getsize(out_path)/1e6:.1f} MB)")
        print(f"     Upload: npx wrangler r2 object put neahub/data/setores_stats_{territory}.parquet "
              f"--file {out_path} --remote")

    print("\nDone.")


if __name__ == "__main__":
    main()
