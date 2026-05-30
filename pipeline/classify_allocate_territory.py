"""
classify_allocate_territory.py
Residential classification + census population allocation for any territory,
generalizing the Misiones recipe (classify_and_allocate.py) to features available
ACROSS territories (the full Misiones model leans on Misiones-only tables —
catastro, censo-viviendas, radio NDVI — which don't transfer).

PORTABLE model: LightGBM trained on Misiones osm_building_type labels using only
AlphaEarth embeddings (a00-a63) + log_area_m2 + best_height_m + in_renabap.
Cached to residential_classifier_portable.joblib. Then applied per territory with
the same overrides (OSM non_res->0, apartments/mixed_use->1, in_renabap->1,
area<15->0) and residential-gated allocation (weights = is_res * area, largest
remainder, exact per-radio census totals from radio_stats_<t>.parquet).

Usage:
  python classify_allocate_territory.py --territory corrientes [--retrain]
"""
import argparse
import os
import sys
import time

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, get_territory

PG_DSN = "dbname=ndvi_misiones host=localhost port=5432 user=postgres"
PG_URL = "postgresql://postgres@localhost:5432/ndvi_misiones"
MODEL_PATH = os.path.join(SCRIPT_DIR, "residential_classifier_portable.joblib")
THR_PATH = os.path.join(SCRIPT_DIR, "residential_threshold_portable.joblib")

ALPHA = [f"a{i:02d}" for i in range(64)]
FEATS = ALPHA + ["log_area_m2", "best_height_m", "in_renabap"]
AR_CODPROV = {"corrientes": "18", "chaco": "22", "formosa": "34"}


def _alpha_sel(prefix="be."):
    return ", ".join(f"{prefix}{c}" for c in ALPHA)


def train_portable():
    import lightgbm as lgb
    from sklearn.model_selection import StratifiedKFold
    from sklearn.metrics import precision_recall_curve, classification_report, confusion_matrix
    import joblib
    print("=== TRAIN portable model (Misiones labels, portable features) ===")
    eng = create_engine(PG_URL)
    parts = []
    for tbl, idc, areac, src in [("gba_buildings", "gba_id", "area_m2", "gba"),
                                 ("vida_buildings", "fid", "area_in_meters", "vida")]:
        parts.append(f"""
            SELECT b.osm_building_type,
                   LN(GREATEST(b.{areac},1)) AS log_area_m2,
                   COALESCE(b.best_height_m,5) AS best_height_m,
                   COALESCE(b.in_renabap,FALSE)::int AS in_renabap,
                   {_alpha_sel()}
            FROM {tbl} b
            JOIN building_embeddings be ON be.source='{src}' AND be.building_id=b.{idc}
            WHERE b.osm_building_type IN ('non_residential','apartments','mixed_use') AND b.{areac}>0
        """)
    df = pd.read_sql(" UNION ALL ".join(parts), eng)
    df["label"] = df["osm_building_type"].isin(["apartments", "mixed_use"]).astype(int)
    print(f"  train rows={len(df):,}  res={int(df.label.sum()):,}  nonres={int((df.label==0).sum()):,}")
    X = np.nan_to_num(df[FEATS].values.astype(np.float32), nan=0.0)
    y = df["label"].values
    params = {"objective": "binary", "metric": "binary_logloss", "is_unbalance": True,
              "learning_rate": 0.05, "num_leaves": 31, "min_child_samples": 20,
              "feature_fraction": 0.8, "bagging_fraction": 0.8, "bagging_freq": 5,
              "verbose": -1, "seed": 42}
    skf = StratifiedKFold(5, shuffle=True, random_state=42)
    oof = np.zeros(len(y)); iters = []
    for tr, va in skf.split(X, y):
        d = lgb.Dataset(X[tr], label=y[tr], feature_name=FEATS)
        dv = lgb.Dataset(X[va], label=y[va], reference=d)
        m = lgb.train(params, d, 500, valid_sets=[dv], callbacks=[lgb.early_stopping(50, verbose=False)])
        oof[va] = m.predict(X[va]); iters.append(m.best_iteration)
    print(classification_report(y, (oof >= 0.5).astype(int), target_names=["nonres", "res"]))
    prec, rec, thr = precision_recall_curve(y, oof)
    mask = rec[:-1] >= 0.95
    threshold = float(thr[mask][-1]) if mask.any() else 0.3
    threshold = max(0.1, min(threshold, 0.5))
    cm = confusion_matrix(y, (oof >= threshold).astype(int))
    print(f"  threshold={threshold:.3f}  res_recall={cm[1,1]/max(cm[1].sum(),1):.3f}  "
          f"nonres_recall={cm[0,0]/max(cm[0].sum(),1):.3f}")
    model = lgb.train(params, lgb.Dataset(X, label=y, feature_name=FEATS), int(np.mean(iters))+50)
    imp = sorted(zip(FEATS, model.feature_importance("gain")), key=lambda z: -z[1])
    print("  top feats:", [f"{n}={int(g)}" for n, g in imp[:8]])
    joblib.dump(model, MODEL_PATH); joblib.dump(threshold, THR_PATH)
    print(f"  saved {os.path.basename(MODEL_PATH)}")


def classify(territory):
    """Rule-based residential classification (default-residential + OSM/area
    exclusion). Houses default to residential; only obvious non-residential is
    excluded. Same logic as build_itapua_buildings.classify_residential.
      residential = TRUE, unless:
        - osm_building_type = 'non_residential'  → FALSE
        - area_m2 > 700 (galpón/depósito/industrial) → FALSE
        - area_m2 < 15 (GBA noise/auxiliary)       → FALSE
      forced TRUE: osm apartments/mixed_use, or in_renabap (barrio popular)."""
    print(f"=== CLASSIFY {territory} (rule-based: default-residential + OSM/area) ===")
    conn = psycopg2.connect(PG_DSN); cur = conn.cursor()
    table = f"gba_buildings_{territory}"
    cur.execute(f"""
        UPDATE {table} SET is_residential = CASE
            WHEN COALESCE(in_renabap, FALSE) THEN TRUE
            WHEN osm_building_type IN ('apartments','mixed_use') THEN TRUE
            WHEN osm_building_type = 'non_residential' THEN FALSE
            WHEN area_m2 > 700 OR area_m2 < 15 THEN FALSE
            ELSE TRUE END,
        p_residential = NULL
        WHERE area_m2 IS NOT NULL
    """)
    conn.commit()
    cur.execute(f"SELECT COUNT(*) FILTER (WHERE is_residential), COUNT(*) "
                f"FROM {table} WHERE area_m2 IS NOT NULL")
    r, tot = cur.fetchone()
    cur.execute(f"SELECT COUNT(*) FILTER (WHERE osm_building_type='non_residential'), "
                f"COUNT(*) FILTER (WHERE area_m2>700), COUNT(*) FILTER (WHERE area_m2<15), "
                f"COUNT(*) FILTER (WHERE in_renabap) FROM {table}")
    nr, big, tiny, rb = cur.fetchone()
    print(f"  residential={r:,}/{tot:,} ({100*r/tot:.1f}%)  "
          f"excluded: osm_nonres={nr:,} area>700={big:,} area<15={tiny:,} | renabap_forced={rb:,}")
    cur.close(); conn.close()


def _lr(total, w):
    s = w.sum()
    if s == 0 or total == 0:
        return np.zeros(len(w), dtype=int)
    raw = w * total / s; fl = np.floor(raw).astype(int); d = total - fl.sum()
    if d > 0:
        fl[np.argsort(-(raw - fl))[:d]] += 1
    return fl


def allocate(territory):
    print(f"=== ALLOCATE {territory} ===")
    eng = create_engine(PG_URL); conn = psycopg2.connect(PG_DSN); cur = conn.cursor()
    table = f"gba_buildings_{territory}"
    census = pd.read_parquet(os.path.join(OUTPUT_DIR, territory, f"radio_stats_{territory}.parquet"))
    census["redcode"] = census["redcode"].astype(str)
    census = census.set_index("redcode")
    print(f"  census radios={len(census):,}  personas={int(census['total_personas'].sum()):,}")
    b = pd.read_sql(f"""SELECT gid, redcode, area_m2, COALESCE(is_residential,TRUE) is_residential
                        FROM {table} WHERE area_m2>0 AND redcode IS NOT NULL""", eng)
    b["redcode"] = b["redcode"].astype(str)
    cur.execute(f"UPDATE {table} SET est_personas=0, est_hogares=0, est_viviendas=0"); conn.commit()
    updates = []; n_edge = 0
    for rc, g in b.groupby("redcode"):
        if rc not in census.index:
            continue
        row = census.loc[rc]
        per = int(row.get("total_personas", 0) or 0); hog = int(row.get("total_hogares", 0) or 0)
        area = g["area_m2"].values.astype(np.float64)
        isr = g["is_residential"].values.astype(bool)
        w = isr * area
        if w.sum() == 0 and (per > 0 or hog > 0):
            n_edge += 1
            w = (area > 15).astype(float) * area
            if w.sum() == 0:
                w = np.ones(len(g))
        ap = _lr(per, w); ah = _lr(hog, w)
        ids = g["gid"].values
        for i in range(len(ids)):
            if ap[i] > 0 or ah[i] > 0:
                updates.append((int(ap[i]), int(ah[i]), int(ah[i]), int(ids[i])))
        if len(updates) >= 10000:
            psycopg2.extras.execute_batch(cur,
                f"UPDATE {table} SET est_personas=%s, est_hogares=%s, est_viviendas=%s WHERE gid=%s",
                updates, page_size=1000); conn.commit(); updates = []
    if updates:
        psycopg2.extras.execute_batch(cur,
            f"UPDATE {table} SET est_personas=%s, est_hogares=%s, est_viviendas=%s WHERE gid=%s",
            updates, page_size=1000); conn.commit()
    cur.execute(f"SELECT SUM(est_personas), COUNT(*) FILTER (WHERE est_personas>0) FROM {table}")
    sp, nb = cur.fetchone()
    print(f"  allocated: sum_personas={int(sp or 0):,}  buildings_with_pop={nb:,}  edge_radios={n_edge}")
    cur.close(); conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", required=True)
    args = ap.parse_args()
    t0 = time.time()
    classify(args.territory)
    allocate(args.territory)
    print(f"\nDONE {args.territory} in {(time.time()-t0)/60:.1f}min")


if __name__ == "__main__":
    main()
