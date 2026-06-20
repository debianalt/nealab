"""
Forestry SDM retrained on the DNDFI national plantation inventory (2026) as the
presence signal, POOLED across the AR-NEA territories (Misiones, Corrientes,
Chaco, Formosa).

Why: the previous SDM trained on MapBiomas silvicultura over Misiones only and
transferred the model elsewhere. The map overlay shows the DNDFI inventory, so
the score must be defined by that SAME inventory — otherwise "zones similar to
existing plantations" is similarity to a different plantation set than the one on
screen. Pooling the 4 AR provinces also grounds each AR territory in its OWN
plantations instead of extrapolating from Misiones.

Presence = dndfi_presence_h3.frac_plantada >= PRESENCE_FRAC (see
aggregate_plantations_h3.py). One RandomForest is trained on the pooled presence
+ background, then applied per territory. Score = calibrated probability * 100,
so AR territories are mutually comparable (same model).

PY/BR have no DNDFI coverage and keep the legacy transfer path (run
compute_forestry_sdm.py for those) — out of scope here.

Usage:
  python pipeline/compute_forestry_sdm_ardndfi.py
"""
import os
import sys
import json

import duckdb
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import GroupKFold

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
import compute_forestry_sdm as sdm  # reuse covariate builder, mask, helpers, FEATURES

# Train only on the commercial mesopotamian niche (pine/eucalyptus, ~821k ha).
# Chaco/Formosa plantations are native-species dry-forest plantings (~7.7k ha) — a
# different ecological/silvicultural niche; pooling them collapses two niches and
# scores their own plantings low. They still get a commercial-aptitude score
# (honest "where pine/euc could go") and an overlay with a native-species caveat.
TRAIN_TERRITORIES = ["misiones", "corrientes"]
PREDICT_TERRITORIES = ["misiones", "corrientes", "chaco", "formosa"]
PRESENCE_FRAC = 0.10
KFEATS = ["gdd", "precip_total", "water_deficit", "slope_mean", "clay", "ndvi_mean"]
OUT_COLS = ["gdd", "precip_total", "water_deficit", "slope_mean", "clay", "soc"]


def set_globals(t: str) -> str:
    if t == "misiones":
        sdm._T_OUT_DIR, sdm._T_ID = sdm.OUTPUT_DIR, "misiones"
    else:
        cfg = sdm.get_territory(t)
        sdm._T_OUT_DIR = os.path.join(sdm.OUTPUT_DIR, cfg["output_prefix"].rstrip("/"))
        sdm._T_ID = t
    return sdm._T_OUT_DIR


def load_frame(con, t: str):
    out_dir = set_globals(t)
    df = sdm.build_prediction_frame(con, out_dir)
    df["blocked_reason"] = sdm.compute_mask(df)
    # Re-mask water by Dynamic World land cover, not JRC. compute_mask masks any hex
    # with JRC water fraction >= 0.30, but JRC's *seasonal* surface water over-masks
    # river margins and flood-prone valleys that DW classifies as land — and in Misiones
    # those follow the (river-defined) department borders, leaving "sin cobertura"
    # strips. Keep masked only hexes DW considers majority-water; re-score the rest.
    lu = pd.read_parquet(os.path.join(out_dir, "sat_land_use.parquet"))[["h3index", "frac_water"]]
    lu = lu.rename(columns={"frac_water": "_lu_water"})
    if lu["_lu_water"].max() > 1.5:  # scale-robust: MapBiomas is 0-100, DW is 0-1
        lu["_lu_water"] = lu["_lu_water"] / 100.0
    df = df.merge(lu, on="h3index", how="left")
    relax = (df["blocked_reason"] == "water") & (df["_lu_water"].fillna(0) < 0.50)
    df.loc[relax, "blocked_reason"] = ""
    # Only score hexes that have REAL covariates. Relaxing the water mask re-includes
    # some Iberá-estero hexes that have no SoilGrids/climate data; the RF would score
    # them off median-filled features (meaningless) and the tooltip shows "Sin
    # cobertura". Mark hexes missing any displayed covariate as no-data so scored
    # hexes always have a full tooltip (scored <=> has data).
    core = ["gdd", "precip_total", "water_deficit", "slope_mean", "clay", "soc"]
    nodata = df[core].isna().any(axis=1)
    df.loc[nodata & (df["blocked_reason"] == ""), "blocked_reason"] = "nodata"
    pres_path = os.path.join(out_dir, "dndfi_presence_h3.parquet")
    pres = pd.read_parquet(pres_path)[["h3index", "frac_plantada"]]
    df = df.merge(pres, on="h3index", how="left")
    df["frac_plantada"] = df["frac_plantada"].fillna(0.0)
    n_pres = int((df["frac_plantada"] >= PRESENCE_FRAC).sum())
    print(f"  {t}: {len(df):,} hexes, {n_pres:,} presence (frac>={PRESENCE_FRAC})")
    return df, out_dir


def main():
    import argparse
    ap = argparse.ArgumentParser(description="Forestry SDM pooled on DNDFI presence")
    ap.add_argument("--train", default=",".join(TRAIN_TERRITORIES))
    ap.add_argument("--predict", default=",".join(PREDICT_TERRITORIES))
    ap.add_argument("--tag", default="ardndfi", help="diagnostics filename tag")
    ap.add_argument("--dry-run", action="store_true", help="train+validate, do not write parquets")
    args = ap.parse_args()
    train_t = [t.strip() for t in args.train.split(",") if t.strip()]
    predict_t = [t.strip() for t in args.predict.split(",") if t.strip()]

    con = duckdb.connect()
    print("[1/4] Loading covariates + DNDFI presence per territory...")
    frames = {t: load_frame(con, t) for t in sorted(set(train_t) | set(predict_t))}

    print(f"[2/4] Building pooled training set ({train_t})...")
    pool = pd.concat(
        [frames[t][0].assign(territory=t) for t in train_t], ignore_index=True
    )
    valid = pool.loc[pool["blocked_reason"] == ""].copy().reset_index(drop=True)
    presence = (valid["frac_plantada"] >= PRESENCE_FRAC).values

    feat_df = valid[sdm.FEATURES].copy()
    feat_medians = {f: float(feat_df[f].median()) for f in sdm.FEATURES}
    feat_df = feat_df.fillna(pd.Series(feat_medians))

    rng = np.random.default_rng(42)
    pos_idx = np.where(presence)[0]
    neg_idx = np.where(~presence)[0]
    bg_idx = rng.choice(neg_idx, size=min(len(pos_idx) * sdm.BACKGROUND_RATIO, len(neg_idx)), replace=False)
    train_idx = np.concatenate([pos_idx, bg_idx])
    y = np.zeros(len(train_idx), dtype=int)
    y[: len(pos_idx)] = 1
    X = feat_df.iloc[train_idx].values
    groups = sdm.assign_spatial_block(valid.iloc[train_idx]["h3index"].reset_index(drop=True)).values
    print(f"  Presencias: {len(pos_idx):,} | Background: {len(bg_idx):,} | blocks: {len(set(groups))}")

    print("[3/4] Spatial-block CV + final fit...")
    gkf = GroupKFold(n_splits=5)
    aucs, aps = [], []
    for k, (tr, te) in enumerate(gkf.split(X, y, groups)):
        clf = RandomForestClassifier(**sdm.RF_PARAMS)
        clf.fit(X[tr], y[tr])
        p = clf.predict_proba(X[te])[:, 1]
        aucs.append(roc_auc_score(y[te], p))
        aps.append(average_precision_score(y[te], p))
        print(f"  Fold {k+1}: AUC={aucs[-1]:.3f} AP={aps[-1]:.3f}")
    print(f"  Pooled CV: AUC={np.mean(aucs):.3f}+/-{np.std(aucs):.3f} AP={np.mean(aps):.3f}")

    clf = RandomForestClassifier(**sdm.RF_PARAMS)
    clf.fit(X, y)

    print(f"[4/4] Predicting{' (DRY-RUN)' if args.dry_run else ' + writing'} per territory...")
    diag = {"cv_auc_mean": float(np.mean(aucs)), "cv_ap_mean": float(np.mean(aps)),
            "train": train_t, "n_presence": int(len(pos_idx)), "presence_frac": PRESENCE_FRAC,
            "feature_importance": dict(zip(sdm.FEATURES, clf.feature_importances_.tolist()))}
    for t in predict_t:
        df, out_dir = frames[t]
        set_globals(t)
        vp = df.loc[df["blocked_reason"] == ""].copy()
        fdf = vp[sdm.FEATURES].copy()
        for c in sdm.FEATURES:
            if fdf[c].isna().any():
                fdf[c] = fdf[c].fillna(feat_medians[c])
        proba = clf.predict_proba(fdf.values)[:, 1]
        vp["score"] = np.round(proba * 100.0, 1)
        vp["score_raw"] = vp["score"]

        high = vp.loc[vp["score"] >= 40].copy()
        if len(high) >= 500:
            Xk = high[KFEATS].fillna(high[KFEATS].median()).values
            Xk = (Xk - Xk.mean(axis=0)) / (Xk.std(axis=0) + 1e-9)
            km = KMeans(n_clusters=4, random_state=42, n_init=20)
            high["type"] = km.fit_predict(Xk) + 1
            high["type_label"] = high["type"].map(sdm.label_clusters(km.cluster_centers_, KFEATS))
        else:
            high["type"] = pd.NA
            high["type_label"] = pd.NA
        vp["type"] = pd.NA
        vp["type_label"] = pd.NA
        vp.loc[high.index, "type"] = high["type"].values
        vp.loc[high.index, "type_label"] = high["type_label"].values

        out = df[["h3index"]].copy()
        for col in ["score", "score_raw", "type", "type_label"]:
            out[col] = pd.NA
        out.loc[vp.index, "score"] = vp["score"].values
        out.loc[vp.index, "score_raw"] = vp["score_raw"].values
        out.loc[vp.index, "type"] = vp["type"].values
        out.loc[vp.index, "type_label"] = vp["type_label"].values
        for col in OUT_COLS:
            if col in df.columns:
                out[col] = pd.NA
                out.loc[vp.index, col] = df.loc[vp.index, col].round(2).values
        out["score"] = pd.to_numeric(out["score"], errors="coerce")
        out["type"] = pd.to_numeric(out["type"], errors="coerce").astype("Int32")

        out_path = os.path.join(out_dir, "sat_forestry_aptitude.parquet")
        if not args.dry_run:
            out.to_parquet(out_path, index=False)

        # Validation: median score on real DNDFI presence hexes (should be high)
        pres_mask = df["frac_plantada"] >= PRESENCE_FRAC
        pres_scored = out.loc[pres_mask & out["score"].notna(), "score"]
        med = float(pres_scored.median()) if len(pres_scored) else float("nan")
        n_sc = int(out["score"].notna().sum())
        terr_med = float(out["score"].median())
        diag[f"{t}_presence_score_median"] = med
        diag[f"{t}_territory_score_median"] = terr_med
        tag = "DRY" if args.dry_run else "wrote"
        print(f"  {t}: scored={n_sc:,} | presence_median={med:.1f} vs territory_median={terr_med:.1f} | {tag}")

    if not args.dry_run:
        with open(os.path.join(sdm.OUTPUT_DIR, f"forestry_sdm_{args.tag}_diagnostics.json"), "w") as f:
            json.dump(diag, f, indent=2)
    print("Done.")


if __name__ == "__main__":
    main()
