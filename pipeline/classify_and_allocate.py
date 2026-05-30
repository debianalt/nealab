"""
classify_and_allocate.py
ML-based residential classification and census allocation for Misiones buildings.

Uses LightGBM with Google AlphaEarth satellite embeddings + cadastral features
to classify buildings as residential/non-residential, then distributes census
population, households, and dwellings via largest remainder apportionment.

Usage:
    python classify_and_allocate.py --phase {setup,features,train,classify,allocate,validate}
    python classify_and_allocate.py --phase all
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

PG_DSN = "dbname=ndvi_misiones host=localhost port=5432 user=postgres"
PG_URL = "postgresql://postgres@localhost:5432/ndvi_misiones"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(SCRIPT_DIR, "residential_classifier.joblib")
THRESHOLD_PATH = os.path.join(SCRIPT_DIR, "residential_threshold.joblib")

ALPHA_COLS = [f"a{i:02d}" for i in range(64)]
RADIO_STATS_COLS = [
    "mb_urban_frac", "mb_agriculture_frac", "building_density_per_km2",
    "viirs_mean_radiance", "canopy_cover", "floor_area_ratio", "ndvi_mean",
]
CATASTRO_COLS = [
    "n_parcelas_urbano", "n_parcelas_rural", "area_urbano_m2", "area_rural_m2",
]
CENSO_VIV_COLS = ["pct_departamento", "pct_informal"]
PLANTACIONES_COLS = ["frac_plantada"]
LULC_COLS = ["lulc_forest", "lulc_forest_plantation", "lulc_pasture", "lulc_grassland", "lulc_water"]

FEATURE_COLS = (
    ["log_area_m2", "best_height_m", "has_osm_levels", "osm_levels",
     "in_urban_parcel", "in_renabap", "in_large_rural_parcel"]
    + RADIO_STATS_COLS + CATASTRO_COLS
    + CENSO_VIV_COLS + PLANTACIONES_COLS + LULC_COLS
    + ALPHA_COLS
)

HOGARES_FORMULA = """
    (COALESCE(v.hogares_1,0)*1 + COALESCE(v.hogares_2,0)*2 +
     COALESCE(v.hogares_3,0)*3 + COALESCE(v.hogares_4,0)*4 +
     COALESCE(v.hogares_5,0)*5 + COALESCE(v.hogares_7,0)*7 +
     COALESCE(v.hogares_15,0)*15)
"""


def get_conn():
    return psycopg2.connect(PG_DSN)


def get_engine():
    return create_engine(PG_URL)


# ── Phase 1: setup ─────────────────────────────────────────────────

def phase_setup():
    print("=== PHASE 1: SETUP ===")
    conn = get_conn()
    cur = conn.cursor()

    new_cols = [
        ("p_residential", "DOUBLE PRECISION"),
        ("is_residential", "BOOLEAN"),
        ("in_urban_parcel", "BOOLEAN DEFAULT FALSE"),
        ("in_renabap", "BOOLEAN DEFAULT FALSE"),
        ("in_large_rural_parcel", "BOOLEAN DEFAULT FALSE"),
        ("est_viviendas", "INTEGER DEFAULT 0"),
        ("est_hogares", "INTEGER DEFAULT 0"),
        ("est_personas", "INTEGER DEFAULT 0"),
    ]

    for table in ["gba_buildings", "vida_buildings"]:
        for col, dtype in new_cols:
            cur.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {dtype}"
            )
            print(f"  {table}.{col} OK")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS building_classification (
            source TEXT NOT NULL,
            building_id INTEGER NOT NULL,
            redcode TEXT,
            p_residential DOUBLE PRECISION,
            is_residential BOOLEAN,
            PRIMARY KEY (source, building_id)
        )
    """)
    cur.execute("TRUNCATE building_classification")
    print("  building_classification staging table ready")

    conn.commit()
    cur.close()
    conn.close()
    print("Setup complete.\n")


# ── Phase 2: features ──────────────────────────────────────────────

def phase_features():
    print("=== PHASE 2: FEATURES (spatial joins) ===")
    conn = get_conn()
    cur = conn.cursor()

    # Partial index for large rural parcels
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_catastro_rural_large
        ON catastro_rural USING gist(geom) WHERE superficie_ha > 50
    """)
    conn.commit()
    print("  idx_catastro_rural_large ready")

    # GBA buildings — use existing centroid column
    cur.execute(
        "SELECT DISTINCT LEFT(redcode, 5) AS dpto "
        "FROM gba_buildings WHERE redcode IS NOT NULL ORDER BY 1"
    )
    deptos = [r[0] for r in cur.fetchall()]

    print(f"\nProcessing {len(deptos)} departments for gba_buildings...")
    t0 = time.time()
    for i, dpto in enumerate(deptos):
        cur.execute("""
            UPDATE gba_buildings b
            SET in_urban_parcel = EXISTS(
                SELECT 1 FROM catastro_urbano cu
                WHERE ST_Contains(cu.geom, b.centroid)
            )
            WHERE LEFT(b.redcode, 5) = %s
        """, (dpto,))
        n = cur.rowcount
        elapsed = time.time() - t0
        print(f"  [{i+1}/{len(deptos)}] catastro_urbano {dpto}: {n:,} buildings ({elapsed:.0f}s)")
        conn.commit()

    print(f"\n  RENABAP spatial join for gba_buildings...")
    for i, dpto in enumerate(deptos):
        cur.execute("""
            UPDATE gba_buildings b
            SET in_renabap = EXISTS(
                SELECT 1 FROM barrios_renabap br
                WHERE ST_Contains(br.geom, b.centroid)
            )
            WHERE LEFT(b.redcode, 5) = %s
        """, (dpto,))
        n = cur.rowcount
        elapsed = time.time() - t0
        print(f"  [{i+1}/{len(deptos)}] renabap {dpto}: {n:,} buildings ({elapsed:.0f}s)")
        conn.commit()

    print(f"\n  Catastro rural spatial join for gba_buildings...")
    for i, dpto in enumerate(deptos):
        cur.execute("""
            UPDATE gba_buildings b
            SET in_large_rural_parcel = EXISTS(
                SELECT 1 FROM catastro_rural cr
                WHERE cr.superficie_ha > 50
                  AND ST_Contains(cr.geom, b.centroid)
            )
            WHERE LEFT(b.redcode, 5) = %s
        """, (dpto,))
        n = cur.rowcount
        elapsed = time.time() - t0
        print(f"  [{i+1}/{len(deptos)}] rural {dpto}: {n:,} buildings ({elapsed:.0f}s)")
        conn.commit()

    # VIDA buildings — compute centroid on the fly
    cur.execute(
        "SELECT DISTINCT LEFT(redcode, 5) AS dpto "
        "FROM vida_buildings WHERE redcode IS NOT NULL ORDER BY 1"
    )
    deptos_vida = [r[0] for r in cur.fetchall()]

    print(f"\nProcessing {len(deptos_vida)} departments for vida_buildings...")
    for i, dpto in enumerate(deptos_vida):
        cur.execute("""
            UPDATE vida_buildings b
            SET in_urban_parcel = EXISTS(
                SELECT 1 FROM catastro_urbano cu
                WHERE ST_Contains(cu.geom, ST_Centroid(b.geom))
            )
            WHERE LEFT(b.redcode, 5) = %s
        """, (dpto,))
        n = cur.rowcount
        elapsed = time.time() - t0
        print(f"  [{i+1}/{len(deptos_vida)}] catastro_urbano {dpto}: {n:,} buildings ({elapsed:.0f}s)")
        conn.commit()

    print(f"\n  RENABAP spatial join for vida_buildings...")
    for i, dpto in enumerate(deptos_vida):
        cur.execute("""
            UPDATE vida_buildings b
            SET in_renabap = EXISTS(
                SELECT 1 FROM barrios_renabap br
                WHERE ST_Contains(br.geom, ST_Centroid(b.geom))
            )
            WHERE LEFT(b.redcode, 5) = %s
        """, (dpto,))
        n = cur.rowcount
        elapsed = time.time() - t0
        print(f"  [{i+1}/{len(deptos_vida)}] renabap {dpto}: {n:,} buildings ({elapsed:.0f}s)")
        conn.commit()

    print(f"\n  Catastro rural spatial join for vida_buildings...")
    for i, dpto in enumerate(deptos_vida):
        cur.execute("""
            UPDATE vida_buildings b
            SET in_large_rural_parcel = EXISTS(
                SELECT 1 FROM catastro_rural cr
                WHERE cr.superficie_ha > 50
                  AND ST_Contains(cr.geom, ST_Centroid(b.geom))
            )
            WHERE LEFT(b.redcode, 5) = %s
        """, (dpto,))
        n = cur.rowcount
        elapsed = time.time() - t0
        print(f"  [{i+1}/{len(deptos_vida)}] rural {dpto}: {n:,} buildings ({elapsed:.0f}s)")
        conn.commit()

    cur.close()
    conn.close()
    print(f"Features complete ({time.time() - t0:.0f}s total).\n")


# ── Phase 3: train ─────────────────────────────────────────────────

def _feature_select_sql():
    """Return the SELECT columns for feature extraction."""
    alpha = ", ".join(f"be.{c}" for c in ALPHA_COLS)
    radio = ", ".join(f"rs.{c}" for c in RADIO_STATS_COLS)
    catastro = ", ".join(f"cat.{c}" for c in CATASTRO_COLS)
    return f"""
        LN(GREATEST(b.area_m2, 1)) AS log_area_m2,
        COALESCE(b.best_height_m, 5) AS best_height_m,
        CASE WHEN b.osm_levels IS NOT NULL THEN 1 ELSE 0 END AS has_osm_levels,
        COALESCE(b.osm_levels, 0) AS osm_levels,
        COALESCE(b.in_urban_parcel, FALSE)::int AS in_urban_parcel,
        COALESCE(b.in_renabap, FALSE)::int AS in_renabap,
        COALESCE(b.in_large_rural_parcel, FALSE)::int AS in_large_rural_parcel,
        {radio},
        {catastro},
        cv.pct_departamento, cv.pct_informal,
        COALESCE(pf.frac_plantada, 0) AS frac_plantada,
        lulc.lulc_forest, lulc.lulc_forest_plantation, lulc.lulc_pasture,
        lulc.lulc_grassland, lulc.lulc_water,
        {alpha}
    """


def _feature_joins(source, id_col):
    return f"""
        LEFT JOIN radio_stats_master rs ON b.redcode = rs.redcode
        LEFT JOIN catastro_by_radio cat ON b.redcode = cat.redcode
        LEFT JOIN building_embeddings be ON be.source = '{source}' AND be.building_id = b.{id_col}
        LEFT JOIN (
            SELECT redcode,
                   COALESCE(viv_departamento::float / NULLIF(viv_particular, 0), 0) AS pct_departamento,
                   COALESCE((viv_rancho + viv_casilla)::float / NULLIF(viv_particular, 0), 0) AS pct_informal
            FROM censo2022_viviendas
        ) cv ON b.redcode = cv.redcode
        LEFT JOIN plantaciones_forestales_by_radio_2020 pf ON b.redcode = pf.redcode
        LEFT JOIN (
            SELECT redcode,
                   MAX(CASE WHEN class_name='forest' THEN fraction ELSE 0 END) AS lulc_forest,
                   MAX(CASE WHEN class_name='forest_plantation' THEN fraction ELSE 0 END) AS lulc_forest_plantation,
                   MAX(CASE WHEN class_name='pasture' THEN fraction ELSE 0 END) AS lulc_pasture,
                   MAX(CASE WHEN class_name='grassland' THEN fraction ELSE 0 END) AS lulc_grassland,
                   MAX(CASE WHEN class_name='water' THEN fraction ELSE 0 END) AS lulc_water
            FROM mapbiomas_lulc WHERE year = 2022
            GROUP BY redcode
        ) lulc ON b.redcode = lulc.redcode
    """


def phase_train():
    import lightgbm as lgb
    from sklearn.metrics import (
        classification_report,
        confusion_matrix,
        precision_recall_curve,
    )
    import joblib

    print("=== PHASE 3: TRAIN ===")
    engine = get_engine()

    feat_sql = _feature_select_sql()
    joins = _feature_joins("gba", "gba_id")

    train_sql = f"""
        SELECT
            b.gba_id AS building_id,
            b.osm_building_type,
            {feat_sql}
        FROM gba_buildings b
        {joins}
        WHERE b.osm_building_type IN ('non_residential', 'apartments', 'mixed_use')
          AND b.geom IS NOT NULL AND b.area_m2 > 0
    """

    print("Loading training data...")
    df = pd.read_sql(train_sql, engine)
    print(f"  {len(df):,} buildings with osm_building_type")
    print(f"  Classes: {df['osm_building_type'].value_counts().to_dict()}")

    df["label"] = df["osm_building_type"].isin(["apartments", "mixed_use"]).astype(int)
    print(f"  Label 0 (non-residential): {(df['label'] == 0).sum():,}")
    print(f"  Label 1 (residential):     {(df['label'] == 1).sum():,}")

    X = df[FEATURE_COLS].values.astype(np.float32)
    y = df["label"].values
    X = np.nan_to_num(X, nan=0.0)

    # 5-fold CV
    print("\n5-fold cross-validation...")
    from sklearn.model_selection import StratifiedKFold

    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    oof_preds = np.zeros(len(y))
    best_iters = []

    params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "is_unbalance": True,
        "learning_rate": 0.05,
        "num_leaves": 31,
        "min_child_samples": 20,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "verbose": -1,
        "seed": 42,
    }

    for fold, (train_idx, val_idx) in enumerate(skf.split(X, y)):
        dtrain = lgb.Dataset(
            X[train_idx], label=y[train_idx], feature_name=FEATURE_COLS
        )
        dval = lgb.Dataset(
            X[val_idx], label=y[val_idx], feature_name=FEATURE_COLS, reference=dtrain
        )
        model = lgb.train(
            params, dtrain,
            num_boost_round=500,
            valid_sets=[dval],
            callbacks=[lgb.early_stopping(50, verbose=False)],
        )
        oof_preds[val_idx] = model.predict(X[val_idx])
        best_iters.append(model.best_iteration)
        print(f"  Fold {fold + 1}: best_iter={model.best_iteration}")

    # CV metrics at default 0.5
    oof_bin = (oof_preds >= 0.5).astype(int)
    cm = confusion_matrix(y, oof_bin)
    print(f"\nConfusion matrix (threshold=0.5):")
    print(f"  TN={cm[0, 0]:,}  FP={cm[0, 1]:,}")
    print(f"  FN={cm[1, 0]:,}  TP={cm[1, 1]:,}")
    print(classification_report(
        y, oof_bin, target_names=["non_residential", "residential"]
    ))

    # Calibrate threshold — conservative: high recall for residential
    precisions, recalls, thresholds = precision_recall_curve(y, oof_preds)
    target_recall = 0.95
    valid_mask = recalls[:-1] >= target_recall
    if valid_mask.any():
        threshold = float(thresholds[valid_mask][-1])
    else:
        threshold = 0.3
    threshold = max(0.1, min(threshold, 0.5))

    oof_cal = (oof_preds >= threshold).astype(int)
    cm_cal = confusion_matrix(y, oof_cal)
    nr_recall = cm_cal[0, 0] / max(cm_cal[0, 0] + cm_cal[0, 1], 1)
    r_recall = cm_cal[1, 1] / max(cm_cal[1, 0] + cm_cal[1, 1], 1)
    print(f"Calibrated threshold: {threshold:.3f}")
    print(f"  TN={cm_cal[0, 0]:,}  FP={cm_cal[0, 1]:,}")
    print(f"  FN={cm_cal[1, 0]:,}  TP={cm_cal[1, 1]:,}")
    print(f"  Non-residential recall: {nr_recall:.3f}")
    print(f"  Residential recall:     {r_recall:.3f}")

    # Final model on all data
    avg_iter = int(np.mean(best_iters)) + 50
    print(f"\nTraining final model (num_boost_round={avg_iter})...")
    dtrain_full = lgb.Dataset(X, label=y, feature_name=FEATURE_COLS)
    final_model = lgb.train(params, dtrain_full, num_boost_round=avg_iter)

    # Feature importance
    importance = final_model.feature_importance(importance_type="gain")
    feat_imp = sorted(zip(FEATURE_COLS, importance), key=lambda x: -x[1])
    print("\nTop 20 features by gain:")
    for name, imp in feat_imp[:20]:
        print(f"  {name:30s} {imp:,.0f}")

    joblib.dump(final_model, MODEL_PATH)
    joblib.dump(threshold, THRESHOLD_PATH)
    print(f"\nModel saved to {MODEL_PATH}")
    print(f"Threshold saved to {THRESHOLD_PATH}")
    print("Training complete.\n")


# ── Phase 4: classify ──────────────────────────────────────────────

def phase_classify():
    import lightgbm as lgb  # noqa: F401
    import joblib

    print("=== PHASE 4: CLASSIFY ===")
    model = joblib.load(MODEL_PATH)
    threshold = joblib.load(THRESHOLD_PATH)
    print(f"Loaded model and threshold ({threshold:.3f})")

    engine = get_engine()
    conn = get_conn()
    cur = conn.cursor()

    sources = [
        ("gba_buildings", "gba_id", "area_m2", "b.area_m2", "gba"),
        ("vida_buildings", "fid", "area_in_meters", "b.area_in_meters", "vida"),
    ]

    for table, id_col, area_col, area_expr, source in sources:
        print(f"\nClassifying {table}...")
        feat_sql = _feature_select_sql().replace("b.area_m2", area_expr)
        joins = _feature_joins(source, id_col)

        sql = f"""
            SELECT
                b.{id_col} AS building_id,
                b.osm_building_type,
                {area_expr} AS raw_area_m2,
                {feat_sql}
            FROM {table} b
            {joins}
            WHERE b.geom IS NOT NULL AND {area_expr} > 0
        """

        df = pd.read_sql(sql, engine)
        print(f"  {len(df):,} buildings loaded")

        X = df[FEATURE_COLS].values.astype(np.float32)
        X = np.nan_to_num(X, nan=0.0)

        p_res = model.predict(X).astype(np.float64)
        is_res = p_res >= threshold

        # Forced overrides
        override_nonres = (
            (df["osm_building_type"] == "non_residential")
            | (df["raw_area_m2"] < 15)
        )
        override_res = df["osm_building_type"].isin(["apartments", "mixed_use"])

        p_res[override_nonres] = 0.0
        is_res[override_nonres] = False
        p_res[override_res] = 1.0
        is_res[override_res] = True

        # RENABAP — buildings in informal settlements are residential
        override_renabap = df["in_renabap"] == 1
        p_res[override_renabap] = 1.0
        is_res[override_renabap] = True

        n_res = int(is_res.sum())
        n_nonres = int((~is_res).sum())
        print(f"  Residential: {n_res:,} | Non-residential: {n_nonres:,}")
        print(
            f"  Overrides: {int(override_nonres.sum()):,} forced non-res, "
            f"{int(override_res.sum()):,} forced res (OSM), "
            f"{int(override_renabap.sum()):,} forced res (RENABAP)"
        )

        # Write back in batches
        print(f"  Writing to {table}...")
        ids = df["building_id"].values
        batch_size = 5000
        for start in range(0, len(ids), batch_size):
            end = min(start + batch_size, len(ids))
            values = [
                (float(p_res[i]), bool(is_res[i]), int(ids[i]))
                for i in range(start, end)
            ]
            psycopg2.extras.execute_batch(
                cur,
                f"UPDATE {table} SET p_residential = %s, is_residential = %s "
                f"WHERE {id_col} = %s",
                values,
                page_size=1000,
            )
            if (start // batch_size) % 20 == 0:
                conn.commit()
                print(f"    {end:,}/{len(ids):,}")
        conn.commit()
        print(f"  Done writing {table}")

    cur.close()
    conn.close()
    print("Classification complete.\n")


# ── Phase 5: allocate ──────────────────────────────────────────────

def _largest_remainder(total, weights):
    """Distribute integer `total` across buildings proportional to `weights`.

    Returns an int array that sums to exactly `total`.
    """
    w_sum = weights.sum()
    n = len(weights)
    if w_sum == 0 or total == 0:
        return np.zeros(n, dtype=int)

    raw = weights * total / w_sum
    floor_vals = np.floor(raw).astype(int)
    deficit = total - floor_vals.sum()

    if deficit > 0:
        remainders = raw - floor_vals
        top_idx = np.argsort(-remainders)[:deficit]
        floor_vals[top_idx] += 1

    return floor_vals


def phase_allocate():
    print("=== PHASE 5: ALLOCATE ===")
    engine = get_engine()
    conn = get_conn()
    cur = conn.cursor()

    # Census targets per radio
    census_sql = f"""
        SELECT
            v.redcode,
            COALESCE(v.viv_particular, 0) AS viv_particular,
            {HOGARES_FORMULA} AS total_hogares,
            COALESCE(c.total_personas, 0) AS total_personas
        FROM censo2022_viviendas v
        JOIN censo2022_variables c USING (redcode)
    """
    census = pd.read_sql(census_sql, engine).set_index("redcode")
    print(f"Census radios: {len(census):,}")
    print(f"  Total viviendas: {census['viv_particular'].sum():,}")
    print(f"  Total hogares:   {census['total_hogares'].sum():,}")
    print(f"  Total personas:  {census['total_personas'].sum():,}")

    for table, id_col, area_col in [
        ("gba_buildings", "gba_id", "area_m2"),
        ("vida_buildings", "fid", "area_in_meters"),
    ]:
        print(f"\nAllocating for {table}...")

        bldg_sql = f"""
            SELECT {id_col} AS building_id, redcode,
                   {area_col} AS area_m2,
                   COALESCE(is_residential, TRUE) AS is_residential,
                   COALESCE(osm_levels, 1) AS floors
            FROM {table}
            WHERE geom IS NOT NULL AND {area_col} > 0 AND redcode IS NOT NULL
            ORDER BY redcode, {id_col}
        """
        bldgs = pd.read_sql(bldg_sql, engine)
        print(f"  {len(bldgs):,} buildings loaded")

        # Reset allocation columns
        cur.execute(
            f"UPDATE {table} SET est_viviendas = 0, est_hogares = 0, est_personas = 0"
        )
        conn.commit()

        n_radios = 0
        n_edge = 0
        batch_updates = []

        for redcode, group in bldgs.groupby("redcode"):
            if redcode not in census.index:
                continue

            n_radios += 1
            row = census.loc[redcode]
            targets = {
                "est_viviendas": int(row["viv_particular"]),
                "est_hogares": int(row["total_hogares"]),
                "est_personas": int(row["total_personas"]),
            }

            is_res = group["is_residential"].values.astype(bool)
            area = group["area_m2"].values.astype(np.float64)
            floors = np.maximum(group["floors"].values.astype(np.float64), 1.0)
            weights = is_res * area * floors

            # Edge case: no residential but census > 0
            if weights.sum() == 0 and any(t > 0 for t in targets.values()):
                n_edge += 1
                eligible = area > 15
                if eligible.any():
                    weights = eligible.astype(np.float64) * area
                else:
                    weights = np.ones(len(group), dtype=np.float64)

            ids = group["building_id"].values
            alloc_viv = _largest_remainder(targets["est_viviendas"], weights)
            alloc_hog = _largest_remainder(targets["est_hogares"], weights)
            alloc_per = _largest_remainder(targets["est_personas"], weights)

            for i in range(len(ids)):
                v, h, p = int(alloc_viv[i]), int(alloc_hog[i]), int(alloc_per[i])
                if v > 0 or h > 0 or p > 0:
                    batch_updates.append((v, h, p, int(ids[i])))

            if len(batch_updates) >= 10000:
                psycopg2.extras.execute_batch(
                    cur,
                    f"UPDATE {table} SET est_viviendas=%s, est_hogares=%s, "
                    f"est_personas=%s WHERE {id_col}=%s",
                    batch_updates,
                    page_size=1000,
                )
                conn.commit()
                batch_updates = []

        if batch_updates:
            psycopg2.extras.execute_batch(
                cur,
                f"UPDATE {table} SET est_viviendas=%s, est_hogares=%s, "
                f"est_personas=%s WHERE {id_col}=%s",
                batch_updates,
                page_size=1000,
            )
            conn.commit()

        print(f"  Processed {n_radios:,} radios ({n_edge} edge-case radios)")

    # Print province-level totals from DB
    totals_sql = """
        SELECT
            SUM(est_viviendas) AS viv,
            SUM(est_hogares) AS hog,
            SUM(est_personas) AS per
        FROM (
            SELECT est_viviendas, est_hogares, est_personas
            FROM gba_buildings WHERE geom IS NOT NULL AND area_m2 > 0
            UNION ALL
            SELECT est_viviendas, est_hogares, est_personas
            FROM vida_buildings WHERE geom IS NOT NULL AND area_in_meters > 0
        ) t
    """
    tot = pd.read_sql(totals_sql, engine).iloc[0]
    print(f"\nAllocated totals: viv={int(tot['viv']):,}  "
          f"hog={int(tot['hog']):,}  per={int(tot['per']):,}")
    print("Allocation complete.\n")


# ── Phase 6: validate ──────────────────────────────────────────────

def phase_validate():
    print("=== PHASE 6: VALIDATE ===")
    engine = get_engine()
    passed = True

    # Census targets
    census_sql = f"""
        SELECT
            v.redcode,
            LEFT(v.redcode, 5) AS dpto,
            COALESCE(v.viv_particular, 0) AS viv_particular,
            {HOGARES_FORMULA} AS total_hogares,
            COALESCE(c.total_personas, 0) AS total_personas
        FROM censo2022_viviendas v
        JOIN censo2022_variables c USING (redcode)
    """
    census = pd.read_sql(census_sql, engine)

    # Building allocations per radio
    alloc_sql = """
        SELECT redcode,
               SUM(est_viviendas) AS sum_viv,
               SUM(est_hogares) AS sum_hog,
               SUM(est_personas) AS sum_per
        FROM (
            SELECT redcode, est_viviendas, est_hogares, est_personas
            FROM gba_buildings WHERE geom IS NOT NULL AND area_m2 > 0
            UNION ALL
            SELECT redcode, est_viviendas, est_hogares, est_personas
            FROM vida_buildings WHERE geom IS NOT NULL AND area_in_meters > 0
        ) t
        WHERE redcode IS NOT NULL
        GROUP BY redcode
    """
    alloc = pd.read_sql(alloc_sql, engine).set_index("redcode")

    # Per-radio exact match check
    mismatches = []
    for _, row in census.iterrows():
        rc = row["redcode"]
        if rc not in alloc.index:
            for col, val in [
                ("viviendas", row["viv_particular"]),
                ("hogares", row["total_hogares"]),
                ("personas", row["total_personas"]),
            ]:
                if val > 0:
                    mismatches.append((rc, col, int(val), 0))
            continue

        a = alloc.loc[rc]
        for census_col, alloc_col, name in [
            ("viv_particular", "sum_viv", "viviendas"),
            ("total_hogares", "sum_hog", "hogares"),
            ("total_personas", "sum_per", "personas"),
        ]:
            expected = int(row[census_col])
            actual = int(a[alloc_col])
            if expected != actual:
                mismatches.append((rc, name, expected, actual))

    if mismatches:
        print(f"MISMATCHES: {len(mismatches)} radio-variable pairs")
        for rc, name, exp, act in mismatches[:20]:
            print(f"  {rc} {name}: census={exp} allocated={act}")
        if len(mismatches) > 20:
            print(f"  ... and {len(mismatches) - 20} more")
        passed = False
    else:
        print("Per-radio check: ALL radios match exactly.")

    # Non-residential buildings should have 0 — except in edge-case radios
    # where no residential buildings exist (fallback distributes to all > 15m²)
    nonres_sql = """
        WITH has_res AS (
            SELECT redcode FROM (
                SELECT redcode, is_residential FROM gba_buildings
                UNION ALL
                SELECT redcode, is_residential FROM vida_buildings
            ) t
            WHERE is_residential = TRUE
            GROUP BY redcode
        )
        SELECT COUNT(*) AS n
        FROM (
            SELECT redcode, est_viviendas, est_hogares, est_personas
            FROM gba_buildings WHERE is_residential = FALSE
            UNION ALL
            SELECT redcode, est_viviendas, est_hogares, est_personas
            FROM vida_buildings WHERE is_residential = FALSE
        ) t
        WHERE t.redcode IN (SELECT redcode FROM has_res)
          AND (est_viviendas > 0 OR est_hogares > 0 OR est_personas > 0)
    """
    n_bad = int(pd.read_sql(nonres_sql, engine).iloc[0]["n"])
    if n_bad > 0:
        print(f"ERROR: {n_bad:,} non-residential buildings in normal radios "
              f"have non-zero allocations")
        passed = False
    else:
        print("Non-residential check: OK (all have 0 in radios with residential buildings)")

    # Department report
    print("\n--- Department report ---")
    merged = census.merge(
        alloc.reset_index(), on="redcode", how="left"
    ).fillna(0)

    depto_report = merged.groupby("dpto").agg(
        census_viv=("viv_particular", "sum"),
        alloc_viv=("sum_viv", "sum"),
        census_hog=("total_hogares", "sum"),
        alloc_hog=("sum_hog", "sum"),
        census_per=("total_personas", "sum"),
        alloc_per=("sum_per", "sum"),
    ).astype(int)

    header = (
        f"{'dpto':<8} {'c_viv':>8} {'a_viv':>8} "
        f"{'c_hog':>8} {'a_hog':>8} "
        f"{'c_per':>8} {'a_per':>8}"
    )
    print(header)
    print("-" * len(header))
    for dpto, row in depto_report.iterrows():
        print(
            f"{dpto:<8} {row['census_viv']:>8,} {row['alloc_viv']:>8,} "
            f"{row['census_hog']:>8,} {row['alloc_hog']:>8,} "
            f"{row['census_per']:>8,} {row['alloc_per']:>8,}"
        )

    totals = depto_report.sum()
    print("-" * len(header))
    print(
        f"{'TOTAL':<8} {totals['census_viv']:>8,} {totals['alloc_viv']:>8,} "
        f"{totals['census_hog']:>8,} {totals['alloc_hog']:>8,} "
        f"{totals['census_per']:>8,} {totals['alloc_per']:>8,}"
    )

    if passed:
        print("\nVALIDATION PASSED")
    else:
        print("\nVALIDATION FAILED")
        sys.exit(1)


# ── Main ────────────────────────────────────────────────────────────

PHASES = {
    "setup": phase_setup,
    "features": phase_features,
    "train": phase_train,
    "classify": phase_classify,
    "allocate": phase_allocate,
    "validate": phase_validate,
}
PHASE_ORDER = ["setup", "features", "train", "classify", "allocate", "validate"]


def main():
    parser = argparse.ArgumentParser(
        description="ML building classification + census allocation"
    )
    parser.add_argument(
        "--phase", required=True,
        choices=PHASE_ORDER + ["all"],
        help="Phase to run (or 'all' for full pipeline)",
    )
    args = parser.parse_args()

    phases = PHASE_ORDER if args.phase == "all" else [args.phase]

    t0 = time.time()
    for phase in phases:
        PHASES[phase]()

    elapsed = time.time() - t0
    print(f"Done. Total time: {elapsed:.0f}s ({elapsed / 60:.1f}min)")


if __name__ == "__main__":
    main()
