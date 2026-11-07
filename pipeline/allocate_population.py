"""
allocate_population.py
Población, hogares y viviendas por edificio, ajustadas al Censo 2022 por tipo de vivienda.

Reemplaza las fases de reparto de classify_and_allocate.py (Misiones) y
classify_allocate_territory.py (resto de AR). Diagnóstico que lo motivó (6-nov-2026):
personas y viviendas salían de dos repartos distintos (83 % de las personas de Misiones en
edificios marcados como no residenciales), el clasificador se había entrenado sin casas,
la altura de GBA se satura en los edificios altos y el área no sirve de peso para las casas.

Método, por unidad censal (radio; fracción para la validación entre niveles):
  1. Pisos por edificio: los de OSM donde hay. Si no, un modelo LightGBM de log(pisos)
     entrenado con los edificios que tienen pisos en OSM (altura GBA, GHSL, área,
     embeddings AlphaEarth, contexto), usado sólo cuando predice 3 o más pisos (desde los
     5 pisos gana a la altura GBA, que se satura; debajo sobreestima +1 piso); en el resto,
     altura GBA / 3. Validación cruzada por fracción contra OSM.
  2. Puntaje de vivienda: modelo positivo-sin-etiqueta. Positivos = edificios etiquetados
     no residenciales en OSM; el resto es «sin etiqueta» (mayoría casas).
  3. Reparto suave con las restricciones del censo (V01 tipo de vivienda y V01_1 tipo de
     vivienda ocupada, del Hexoma). Elegible = ≥ 15 m² y no etiquetado no residencial.
       casas, ranchos, casillas, piezas, locales y móviles → ∝ elegible × puntaje
         (una vivienda esperada por edificio, no por metro cuadrado);
       departamentos → ∝ elegible × puntaje × área × pisos (superficie cubierta);
       personas y hogares → ∝ viviendas ocupadas esperadas (viviendas × ocupación del
         tipo en la unidad).
     Las sumas por unidad son exactas en personas, hogares y viviendas por tipo.
     Validación entre niveles (6-nov-2026, Misiones, fracción → radio): el corte duro de los
     K primeros, el área como peso de las casas y los pesos por volumen empeoran el
     error; contar edificios elegibles × puntaje es lo mejor para las casas.

Uso (desde neahub/):
  python pipeline/allocate_population.py --territory misiones --level fraccion   # validación
  python pipeline/allocate_population.py --territory misiones                    # radio → parquet
  python pipeline/allocate_population.py --territory misiones --write            # + tabla poblacion_edificios_2022

Salidas: pipeline/output/poblacion/<territorio>/
  edificios_<nivel>.parquet   resultado por edificio
  informe_<nivel>.md / .json  métricas de pisos, puntaje y validación entre niveles
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_ROOT = os.path.join(SCRIPT_DIR, "output", "poblacion")
PG_BLD_URL = "postgresql://postgres@localhost:5432/ndvi_misiones"
PG_BLD_DSN = "dbname=ndvi_misiones host=localhost port=5432 user=postgres"
PG_HEX_URL = "postgresql://postgres@localhost:5432/hexoma"

ALPHA = [f"a{i:02d}" for i in range(64)]
MIN_AREA = 15.0        # edificios menores no son vivienda
SEED = 42

TERRITORIES = {
    "misiones": {"prov": 54, "sources": [
        {"table": "gba_buildings", "id": "gba_id", "area": "area_m2", "emb": "gba", "gba_h": "height_m"},
        {"table": "vida_buildings", "id": "fid", "area": "area_in_meters", "emb": "vida", "gba_h": None},
    ]},
    "corrientes": {"prov": 18, "sources": [
        {"table": "gba_buildings_corrientes", "id": "gid", "area": "area_m2", "emb": "corrientes", "gba_h": "best_height_m"},
    ]},
}

# V01 (variable 72): 1 casa, 2 rancho, 3 casilla, 4 departamento, 5 pieza, 6 local, 7 móvil
# V01_1 (variable 73): 1 casa A, 2 casa B, 3 rancho, 4 casilla, 5 departamento, 6 pieza, 7 local, 8 móvil
V01, V01_1, H10 = 72, 73, 2


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ── Carga ────────────────────────────────────────────────────────────────────

def table_columns(cur, table: str) -> set[str]:
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s", (table,))
    return {r[0] for r in cur.fetchall()}


def load_buildings(territory: str) -> pd.DataFrame:
    eng = create_engine(PG_BLD_URL)
    conn = psycopg2.connect(PG_BLD_DSN)
    cur = conn.cursor()
    frames = []
    for s in TERRITORIES[territory]["sources"]:
        cols = table_columns(cur, s["table"])
        opt = lambda c, alias=None: f"b.{c}" if c in cols else "NULL"  # noqa: E731
        gba_h = f"b.{s['gba_h']}" if s["gba_h"] else "NULL"
        alpha = ", ".join(f"be.{a}" for a in ALPHA)
        sql = f"""
            SELECT b.{s['id']} AS bid, b.redcode::text AS redcode, b.{s['area']}::float AS area,
                   {gba_h}::float AS gba_h, {opt('ghsl_height_m')}::float AS ghsl_h,
                   {opt('osm_levels')}::float AS osm_levels, {opt('osm_building_type')}::text AS osm_type,
                   COALESCE({opt('in_renabap')}, FALSE)::int AS in_renabap,
                   COALESCE({opt('in_urban_parcel')}, FALSE)::int AS in_urban_parcel,
                   COALESCE({opt('in_large_rural_parcel')}, FALSE)::int AS in_large_rural_parcel,
                   {opt('is_residential')}::int AS old_is_res,
                   {opt('est_personas')}::float AS old_personas,
                   {opt('est_viviendas')}::float AS old_viviendas,
                   {alpha}
            FROM {s['table']} b
            LEFT JOIN building_embeddings be ON be.source = '{s['emb']}' AND be.building_id = b.{s['id']}
            WHERE b.redcode IS NOT NULL AND b.{s['area']} > 0
        """
        log(f"  leyendo {s['table']} …")
        df = pd.read_sql(sql, eng)
        df.insert(0, "src", s["table"])
        for a in ALPHA:
            df[a] = df[a].astype(np.float32)
        frames.append(df)
        log(f"    {len(df):,} edificios, {df[ALPHA[0]].notna().mean():.1%} con embeddings, "
            f"{df.osm_levels.notna().sum():,} con pisos OSM")
    conn.close()
    b = pd.concat(frames, ignore_index=True)
    b["redcode"] = b["redcode"].str.strip()
    return b


def load_census(prov: int) -> pd.DataFrame:
    """Por radio: personas, hogares, viviendas y viviendas ocupadas por grupo de tipo."""
    eng = create_engine(PG_HEX_URL)
    rad = pd.read_sql(f"""SELECT redcode::bigint AS redcode, personas_oficial AS personas
                          FROM censo_2022.radios WHERE redcode / 10000000 = {prov}""", eng)
    k = pd.read_sql(f"""SELECT redcode::bigint AS redcode, variable_id, categoria, n::float AS n
                        FROM censo_2022.categorias
                        WHERE redcode / 10000000 = {prov} AND variable_id IN ({V01}, {V01_1}, {H10})""", eng)
    piv = lambda m: k[m].groupby("redcode").n.sum()  # noqa: E731
    c = rad.set_index("redcode")
    c["dep"] = piv((k.variable_id == V01) & (k.categoria == "4"))
    c["otras"] = piv((k.variable_id == V01) & (k.categoria != "4"))
    c["dep_ocup"] = piv((k.variable_id == V01_1) & (k.categoria == "5"))
    c["otras_ocup"] = piv((k.variable_id == V01_1) & (k.categoria != "5"))
    c["hogares"] = piv(k.variable_id == H10)
    c = c.fillna(0.0)
    c.index = c.index.astype(str)
    return c


# ── Contexto por unidad ──────────────────────────────────────────────────────

def unit_of(redcode: pd.Series, level: str) -> pd.Series:
    return redcode if level == "radio" else redcode.str[:7]


def add_context(b: pd.DataFrame, census: pd.DataFrame, level: str) -> pd.DataFrame:
    """Atributos de contexto. Los que salen del censo se calculan en la unidad del nivel
    (en la validación por fracción no se filtra información del radio)."""
    b["unit"] = unit_of(b["redcode"], level)
    cu = census.copy()
    cu["unit"] = unit_of(pd.Series(cu.index, index=cu.index), level)
    cu = cu.groupby("unit")[["personas", "dep", "otras", "dep_ocup", "otras_ocup", "hogares"]].sum()
    elig = b.area >= MIN_AREA
    nb = b[elig].groupby("unit").size()
    cu["ctx_pct_dep"] = cu.dep / (cu.dep + cu.otras).replace(0, np.nan)
    cu["ctx_viv_por_edif"] = (cu.dep + cu.otras) / nb.reindex(cu.index).replace(0, np.nan)
    cu["ctx_pers_por_edif"] = cu.personas / nb.reindex(cu.index).replace(0, np.nan)
    b = b.join(cu[["ctx_pct_dep", "ctx_viv_por_edif", "ctx_pers_por_edif"]], on="unit")
    # Contexto edilicio del radio (no censal): igual en los dos niveles
    g = b.groupby("redcode")
    b["ctx_n_edif"] = g.area.transform("size").astype(np.float32)
    b["ctx_area_media"] = g.area.transform("mean").astype(np.float32)
    b["ctx_gba_h_media"] = g.gba_h.transform("mean").astype(np.float32)
    b["log_area"] = np.log(b.area.clip(lower=1)).astype(np.float32)
    return b, cu


CTX = ["ctx_pct_dep", "ctx_viv_por_edif", "ctx_pers_por_edif", "ctx_n_edif", "ctx_area_media", "ctx_gba_h_media"]


# ── 1. Pisos ─────────────────────────────────────────────────────────────────

FLOOR_FEATS = ["gba_h", "ghsl_h", "log_area", "in_urban_parcel"] + CTX + ALPHA


def model_floors(b: pd.DataFrame, rep: dict) -> None:
    import lightgbm as lgb
    from sklearn.model_selection import GroupKFold

    tr = b.osm_levels.between(1, 60)
    log(f"  pisos: {tr.sum():,} edificios con pisos OSM para entrenar")
    X, y = b.loc[tr, FLOOR_FEATS], np.log(b.loc[tr, "osm_levels"].values)
    groups = b.loc[tr, "redcode"].str[:7].values
    params = dict(objective="regression_l1", learning_rate=0.05, num_leaves=31, min_data_in_leaf=20,
                  feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1, verbose=-1, seed=SEED)
    oof = np.zeros(len(y))
    iters = []
    for f, (i_tr, i_va) in enumerate(GroupKFold(5).split(X, y, groups)):
        m = lgb.train(params, lgb.Dataset(X.iloc[i_tr], y[i_tr]), 2000,
                      valid_sets=[lgb.Dataset(X.iloc[i_va], y[i_va])],
                      callbacks=[lgb.early_stopping(100, verbose=False)])
        oof[i_va] = m.predict(X.iloc[i_va], num_iteration=m.best_iteration)
        iters.append(m.best_iteration)
    final = lgb.train(params, lgb.Dataset(X, y), int(np.mean(iters) * 1.1))
    pred = np.clip(np.exp(final.predict(b[FLOOR_FEATS])), 1, 60)

    # Métricas por tramo: modelo (fuera de muestra) vs altura GBA / 3
    real = b.loc[tr, "osm_levels"].values
    p_oof = np.clip(np.exp(oof), 1, 60)
    base = np.clip(np.nan_to_num(b.loc[tr, "gba_h"].values, nan=3.0) / 3.0, 1, 60)
    bins = [(1, 1), (2, 2), (3, 4), (5, 9), (10, 60)]
    rows = []
    for lo, hi in bins:
        m_ = (real >= lo) & (real <= hi)
        rows.append({"pisos": f"{lo}-{hi}" if lo != hi else str(lo), "n": int(m_.sum()),
                     "error_modelo": round(float(np.mean(np.abs(p_oof[m_] - real[m_]))), 2),
                     "error_gba_h_3": round(float(np.mean(np.abs(base[m_] - real[m_]))), 2),
                     "sesgo_modelo": round(float(np.mean(p_oof[m_] - real[m_])), 2)})
    rep["pisos"] = {"n_entrenamiento": int(tr.sum()), "por_tramo": rows,
                    "error_total_modelo": round(float(np.mean(np.abs(p_oof - real))), 3),
                    "error_total_gba_h_3": round(float(np.mean(np.abs(base - real))), 3)}
    for r in rows:
        log(f"    {r['pisos']:>5} pisos  n={r['n']:>6,}  error modelo {r['error_modelo']:5.2f}  "
            f"GBA/3 {r['error_gba_h_3']:5.2f}  sesgo {r['sesgo_modelo']:+.2f}")

    # El modelo gana desde los 5 pisos y sobreestima las casas bajas (+1 piso): se usa sólo cuando
    # predice 3 o más; debajo, la altura GBA / 3 (o GHSL / 3 donde GBA no tiene altura).
    gba3 = np.clip(b.gba_h.fillna(b.ghsl_h).fillna(3.0).values / 3.0, 1, 60)
    use_model = pred >= 3
    b["pisos_est"] = np.where(tr, b.osm_levels, np.where(use_model, pred, gba3)).astype(np.float32)
    b["pisos_fuente"] = np.where(tr, "osm", np.where(use_model, "modelo", "altura_gba"))


# ── 2. Puntaje de vivienda ───────────────────────────────────────────────────

RES_FEATS = ["log_area", "gba_h", "ghsl_h", "pisos_est", "in_urban_parcel", "in_large_rural_parcel"] + CTX + ALPHA


def score_residential(b: pd.DataFrame, rep: dict) -> None:
    import lightgbm as lgb
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import GroupKFold

    nonres = (b.osm_type == "non_residential").values
    known_res = (b.osm_type.isin(["apartments", "mixed_use"]) | (b.in_renabap == 1)).values
    rng = np.random.default_rng(SEED)
    unl = np.flatnonzero(~nonres & ~known_res)
    unl_s = rng.choice(unl, size=min(len(unl), 10 * nonres.sum()), replace=False)
    idx = np.concatenate([np.flatnonzero(nonres), unl_s])
    y = nonres[idx].astype(int)
    X = b.iloc[idx][RES_FEATS]
    groups = b.iloc[idx]["redcode"].str[:7].values
    log(f"  puntaje: {nonres.sum():,} no residenciales etiquetados vs {len(unl_s):,} sin etiqueta")
    params = dict(objective="binary", learning_rate=0.05, num_leaves=63, min_data_in_leaf=50,
                  feature_fraction=0.8, bagging_fraction=0.8, bagging_freq=1, verbose=-1, seed=SEED)
    oof = np.zeros(len(y))
    iters = []
    for i_tr, i_va in GroupKFold(5).split(X, y, groups):
        m = lgb.train(params, lgb.Dataset(X.iloc[i_tr], y[i_tr]), 3000,
                      valid_sets=[lgb.Dataset(X.iloc[i_va], y[i_va])],
                      callbacks=[lgb.early_stopping(100, verbose=False)])
        oof[i_va] = m.predict(X.iloc[i_va], num_iteration=m.best_iteration)
        iters.append(m.best_iteration)
    final = lgb.train(params, lgb.Dataset(X, y), int(np.mean(iters) * 1.1))
    s = final.predict(b[RES_FEATS])
    s[idx] = oof  # fuera de muestra para los que entrenaron
    auc_pu = roc_auc_score(y, oof)
    # Control con etiquetas verdaderas de vivienda (apartments/mixed_use/RENABAP) vs no residenciales
    kr = np.flatnonzero(known_res)
    ctrl_idx = np.concatenate([np.flatnonzero(nonres), kr])
    auc_ctrl = roc_auc_score(nonres[ctrl_idx].astype(int), s[ctrl_idx])
    rep["puntaje"] = {"n_no_res": int(nonres.sum()), "n_sin_etiqueta": int(len(unl_s)),
                      "auc_no_res_vs_sin_etiqueta": round(float(auc_pu), 3),
                      "auc_no_res_vs_vivienda_conocida": round(float(auc_ctrl), 3)}
    log(f"    AUC no residencial vs sin etiqueta {auc_pu:.3f}; vs vivienda conocida {auc_ctrl:.3f}")
    p = 1.0 - s
    p[known_res] = 1.0
    p[nonres] = 0.0
    b["p_vivienda"] = p.astype(np.float32)


# ── 3. Reparto ───────────────────────────────────────────────────────────────

def _share(w: np.ndarray, unit: pd.Series, fallback: np.ndarray) -> np.ndarray:
    """w / Σw por unidad; donde Σw = 0 usa fallback / Σfallback."""
    w = pd.Series(w, index=unit.index)
    s = w.groupby(unit).transform("sum")
    f = pd.Series(fallback, index=unit.index)
    sf = f.groupby(unit).transform("sum")
    out = np.where(s > 0, w / s.where(s > 0, 1.0), f / sf.where(sf > 0, 1.0))
    return np.where((s > 0) | (sf > 0), out, 0.0)


def allocate(b: pd.DataFrame, cu: pd.DataFrame) -> pd.DataFrame:
    """Dos bolsas por unidad, en forma suave (sin cortes):
       casas y demás → edificio elegible × puntaje de vivienda (una vivienda por edificio esperada);
       departamentos → edificio elegible × puntaje × superficie cubierta (área × pisos).
       Personas y hogares ∝ viviendas ocupadas esperadas (viviendas × ocupación del tipo)."""
    u = b.unit
    area = b.area.values
    elig = ((area >= MIN_AREA) & (b.osm_type.values != "non_residential")).astype(float)
    p = b.p_vivienda.values.astype(float)
    pisos = b.pisos_est.values.astype(float)
    w_cas = elig * p
    w_dep = w_cas * area * pisos
    # Unidades sin edificios elegibles: todos los edificios por área (caso límite, se informa)
    fb = area * 1.0
    n_edge = int(((pd.Series(w_cas, index=b.index).groupby(u).transform("sum") <= 0)
                  & u.isin(cu.index[(cu.dep + cu.otras + cu.personas) > 0])).groupby(u).any().sum())
    C = u.map(cu.otras).fillna(0).values
    D = u.map(cu.dep).fillna(0).values
    cas = C * _share(w_cas, u, fb)
    unit_wcas = pd.Series(w_cas, index=b.index).groupby(u).transform("sum").values
    dep = D * _share(w_dep, u, np.where(unit_wcas > 0, w_cas, fb))
    oc = u.map(cu.otras_ocup / cu.otras.where(cu.otras > 0)).fillna(0).values
    od = u.map(cu.dep_ocup / cu.dep.where(cu.dep > 0)).fillna(0).values
    occ = cas * oc + dep * od
    viv = cas + dep
    occ = np.where(pd.Series(occ, index=b.index).groupby(u).transform("sum") > 0, occ,
                   np.where(pd.Series(viv, index=b.index).groupby(u).transform("sum") > 0, viv, fb))
    sh = _share(occ, u, fb)
    per = u.map(cu.personas).fillna(0).values * sh
    hog = u.map(cu.hogares).fillna(0).values * sh
    out = b[["src", "bid", "redcode", "unit"]].copy()
    out["pisos_est"] = b.pisos_est.values
    out["pisos_fuente"] = b.pisos_fuente.values
    out["p_vivienda"] = b.p_vivienda.values
    out["viv_dep"], out["viv_otras"] = dep, cas
    out["est_viviendas"], out["est_personas"], out["est_hogares"] = viv, per, hog
    big = 0.5  # tipo dominante: más de media vivienda de ese tipo
    out["tipo_viv"] = np.select([(dep >= big) & (cas >= big), dep >= big, cas > 0],
                                ["mixto", "departamento", "casa"], None)
    log(f"  reparto: {n_edge} unidades sin edificios elegibles (se reparte por área)")
    return out


# ── Alternativas para comparar ───────────────────────────────────────────────

def allocate_baseline(b: pd.DataFrame, cu: pd.DataFrame, weight: np.ndarray) -> pd.DataFrame:
    w = pd.Series(weight, index=b.index)
    wsum = w.groupby(b.unit).transform("sum")
    # unidades con peso cero: reparto por área
    za = wsum <= 0
    w[za] = b.area[za]
    wsum = w.groupby(b.unit).transform("sum")
    P = b.unit.map(cu.personas).fillna(0)
    V = b.unit.map(cu.dep + cu.otras).fillna(0)
    return pd.DataFrame({"redcode": b.redcode, "est_personas": P * w / wsum, "est_viviendas": V * w / wsum})


def metrics(est: pd.DataFrame, census: pd.DataFrame, multi_radio_units: set) -> dict:
    r = est.groupby("redcode")[["est_personas", "est_viviendas"]].sum()
    t = census[["personas"]].copy()
    t["viviendas"] = census.dep + census.otras
    t = t[t.index.str[:7].isin(multi_radio_units)]
    r = r.reindex(t.index).fillna(0)
    res = {}
    for col, tc in [("est_personas", "personas"), ("est_viviendas", "viviendas")]:
        e, y = r[col].values, t[tc].values
        ok = y > 0
        ape = np.abs(e[ok] - y[ok]) / y[ok]
        res[tc] = {"error_abs_medio": round(float(np.mean(np.abs(e - y))), 1),
                   "error_rel_ponderado": round(float(np.abs(e - y).sum() / y.sum()), 4),
                   "error_rel_mediano": round(float(np.median(ape)), 4),
                   "pct_radios_dentro_25": round(float(np.mean(ape <= 0.25)), 4),
                   "r2": round(float(1 - ((e - y) ** 2).sum() / ((y - y.mean()) ** 2).sum()), 4)}
    res["n_radios"] = int(len(t))
    return res


# ── Escritura ────────────────────────────────────────────────────────────────

OUT_TABLE = "poblacion_edificios_2022"


def write_db(out: pd.DataFrame, territory: str) -> None:
    """Escribe en una tabla propia (src, bid) → estimación. Las tablas de edificios no se tocan:
    los constructores de teselas y del cruce la leen con un join."""
    conn = psycopg2.connect(PG_BLD_DSN)
    cur = conn.cursor()
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {OUT_TABLE} (
        src TEXT NOT NULL, bid BIGINT NOT NULL, redcode TEXT,
        pisos_est REAL, pisos_fuente TEXT, tipo_viv TEXT, p_vivienda REAL,
        viv_dep DOUBLE PRECISION, viv_otras DOUBLE PRECISION, est_viviendas DOUBLE PRECISION,
        est_personas DOUBLE PRECISION, est_hogares DOUBLE PRECISION, corrida TEXT,
        PRIMARY KEY (src, bid))""")
    srcs = [s["table"] for s in TERRITORIES[territory]["sources"]]
    cur.execute(f"DELETE FROM {OUT_TABLE} WHERE src = ANY(%s)", (srcs,))  # filas propias de una corrida anterior
    stamp = time.strftime("%Y-%m-%d %H:%M")
    rows = list(zip(out.src, out.bid.astype(int), out.redcode, out.pisos_est.astype(float), out.pisos_fuente,
                    out.tipo_viv, out.p_vivienda.astype(float), out.viv_dep.astype(float),
                    out.viv_otras.astype(float), out.est_viviendas.astype(float),
                    out.est_personas.astype(float), out.est_hogares.astype(float), [stamp] * len(out)))
    psycopg2.extras.execute_values(cur, f"INSERT INTO {OUT_TABLE} VALUES %s", rows, page_size=20000)
    conn.commit()
    cur.execute(f"SELECT src, count(*), sum(est_personas) FROM {OUT_TABLE} WHERE src = ANY(%s) GROUP BY 1", (srcs,))
    for r in cur.fetchall():
        log(f"  {OUT_TABLE} ← {r[0]}: {r[1]:,} edificios, {r[2]:,.0f} personas")
    conn.close()


# ── Principal ────────────────────────────────────────────────────────────────

def report_md(rep: dict, path: str) -> None:
    L = [f"# Población por edificio — {rep['territorio']} (nivel {rep['nivel']})", "",
         f"Corrida: {rep['fecha']}.", ""]
    p = rep["pisos"]
    L += ["## Pisos (validación cruzada por fracción contra pisos de OSM)", "",
          "| pisos | n | error modelo | error GBA/3 | sesgo modelo |", "|---|---|---|---|---|"]
    L += [f"| {r['pisos']} | {r['n']:,} | {r['error_modelo']} | {r['error_gba_h_3']} | {r['sesgo_modelo']:+} |"
          for r in p["por_tramo"]]
    L += ["", f"Error medio: modelo {p['error_total_modelo']}, GBA/3 {p['error_total_gba_h_3']} pisos.", ""]
    s = rep["puntaje"]
    L += ["## Puntaje de vivienda", "",
          f"AUC no residencial vs sin etiqueta: {s['auc_no_res_vs_sin_etiqueta']}; "
          f"vs vivienda conocida (apartments, mixed_use, RENABAP): {s['auc_no_res_vs_vivienda_conocida']}.", ""]
    if "comparacion" in rep:
        L += [f"## Validación entre niveles: fracción → radio ({rep['comparacion']['nuevo']['n_radios']:,} radios "
              "en fracciones de más de un radio)", "",
              "| método | personas: error rel. ponderado | mediano | radios ±25 % | R² | viviendas: error rel. ponderado | mediano | radios ±25 % | R² |",
              "|---|---|---|---|---|---|---|---|---|"]
        for k, v in rep["comparacion"].items():
            a, b_ = v["personas"], v["viviendas"]
            L.append(f"| {k} | {a['error_rel_ponderado']:.3f} | {a['error_rel_mediano']:.3f} | {a['pct_radios_dentro_25']:.1%} | {a['r2']:.3f} "
                     f"| {b_['error_rel_ponderado']:.3f} | {b_['error_rel_mediano']:.3f} | {b_['pct_radios_dentro_25']:.1%} | {b_['r2']:.3f} |")
        L.append("")
    if rep.get("torres"):
        L += ["## Torres (los 30 edificios con más pisos en OSM)", "",
              "| radio | pisos OSM | altura GBA | área m² | viviendas | departamentos | personas | personas antes |",
              "|---|---|---|---|---|---|---|---|"]
        L += [f"| {r['redcode']} | {r['pisos_osm']} | {r['altura_gba']} | {r['area']} | {r['viviendas']} | "
              f"{r['departamentos']} | {r['personas']} | {r['personas_antes']} |" for r in rep["torres"]]
        L.append("")
    if "invariantes" in rep:
        L += ["## Invariantes", "", "```", json.dumps(rep["invariantes"], indent=2, ensure_ascii=False), "```", ""]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(L))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--territory", required=True, choices=list(TERRITORIES))
    ap.add_argument("--level", default="radio", choices=["radio", "fraccion"])
    ap.add_argument("--write", action="store_true", help="escribe poblacion_edificios_2022 (sólo nivel radio)")
    a = ap.parse_args()
    if a.write and a.level != "radio":
        sys.exit("--write sólo con --level radio")
    out_dir = os.path.join(OUT_ROOT, a.territory)
    os.makedirs(out_dir, exist_ok=True)
    rep = {"territorio": a.territory, "nivel": a.level, "fecha": time.strftime("%Y-%m-%d %H:%M")}

    log(f"=== {a.territory} · nivel {a.level} ===")
    census = load_census(TERRITORIES[a.territory]["prov"])
    log(f"  censo: {len(census):,} radios, {census.personas.sum():,.0f} personas, "
        f"{(census.dep + census.otras).sum():,.0f} viviendas ({census.dep.sum():,.0f} departamentos)")
    b = load_buildings(a.territory)
    b, cu = add_context(b, census, a.level)
    model_floors(b, rep)
    score_residential(b, rep)
    out = allocate(b, cu)

    # Invariantes en la unidad del nivel
    g = out.groupby("unit")[["est_personas", "est_hogares", "viv_dep", "viv_otras"]].sum()
    cuu = cu.reindex(g.index)
    inv = {
        "max_desvio_personas": float((g.est_personas - cuu.personas).abs().max()),
        "max_desvio_hogares": float((g.est_hogares - cuu.hogares).abs().max()),
        "max_desvio_departamentos": float((g.viv_dep - cuu.dep).abs().max()),
        "max_desvio_otras_viviendas": float((g.viv_otras - cuu.otras).abs().max()),
        "personas_censo_sin_edificios": float(cu.personas[~cu.index.isin(g.index)].sum()),
        "personas_en_no_residenciales_osm": float(out.est_personas[(b.osm_type == "non_residential").values].sum()),
        "edificios_con_vivienda": int((out.est_viviendas > 0).sum()),
        "pisos_por_fuente": {k: int(v) for k, v in out.pisos_fuente.value_counts().items()},
        "personas_por_vivienda_p01_p99": [round(float(x), 2) for x in np.nanpercentile(
            (out.est_personas / out.est_viviendas.replace(0, np.nan)).dropna(), [1, 99])],
    }
    rep["invariantes"] = inv
    log(f"  invariantes: {json.dumps(inv, ensure_ascii=False)}")

    # Torres: los edificios con más pisos en OSM
    t = out.assign(area=b.area.values, osm=b.osm_levels.values, gba_h=b.gba_h.values,
                   old_personas=b.old_personas.values).dropna(subset=["osm"])
    t = t.sort_values(["osm", "area"], ascending=False).head(30)
    rep["torres"] = [{"redcode": r.redcode, "pisos_osm": int(r.osm), "altura_gba": None if pd.isna(r.gba_h) else round(float(r.gba_h), 1),
                      "area": round(float(r.area)), "viviendas": round(float(r.est_viviendas), 1),
                      "departamentos": round(float(r.viv_dep), 1), "personas": round(float(r.est_personas), 1),
                      "personas_antes": None if pd.isna(r.old_personas) else round(float(r.old_personas), 1)}
                     for r in t.itertuples()]

    if a.level == "fraccion":
        nrad = census.groupby(census.index.str[:7]).size()
        multi = set(nrad[nrad > 1].index)
        gh = b.gba_h.fillna(b.ghsl_h).fillna(3.0).clip(lower=1).values
        comp = {
            "nuevo": metrics(out, census, multi),
            "sólo área": metrics(allocate_baseline(b, cu, b.area.values), census, multi),
            "área × altura GBA": metrics(allocate_baseline(b, cu, b.area.values * gh), census, multi),
            "actual (resid. × área × pisos OSM)": metrics(allocate_baseline(
                b, cu, b.old_is_res.fillna(0).values * b.area.values * b.osm_levels.fillna(1).clip(lower=1).values),
                census, multi),
            "conteo de edificios": metrics(allocate_baseline(b, cu, (b.area >= MIN_AREA).astype(float).values),
                                           census, multi),
        }
        rep["comparacion"] = comp
        for k, v in comp.items():
            log(f"    {k:<36} personas: rel.pond {v['personas']['error_rel_ponderado']:.3f} "
                f"±25% {v['personas']['pct_radios_dentro_25']:.1%} R² {v['personas']['r2']:.3f} | "
                f"viviendas: rel.pond {v['viviendas']['error_rel_ponderado']:.3f} R² {v['viviendas']['r2']:.3f}")

    out.to_parquet(os.path.join(out_dir, f"edificios_{a.level}.parquet"), index=False)
    with open(os.path.join(out_dir, f"informe_{a.level}.json"), "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2, ensure_ascii=False)
    report_md(rep, os.path.join(out_dir, f"informe_{a.level}.md"))
    log(f"  salida en {out_dir}")
    if a.write:
        write_db(out, a.territory)


if __name__ == "__main__":
    main()
