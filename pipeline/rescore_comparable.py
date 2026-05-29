"""Fast re-normalization to --mode comparable on existing parquets.

The v1.1 push generated the new territories' parquets with --mode local
(percentile rank intra-territorio) for carbon_stock, productive_activity,
pm25_drivers, soil_water and climate_vulnerability. The 4 incumbent
territories use --mode comparable (goalposts). Result: same score column
means different things across territories.

This script skips re-running zonal_stats (the slow part: 30 min – 2 h per
territory per layer) and instead reads the existing parquet's _raw columns
directly, applies the v1.0/v1.1 goalposts to them, runs the same PCA
variable selection + geometric mean as the compute scripts, and overwrites
score. Each parquet completes in seconds.

Usage:
  python pipeline/rescore_comparable.py --analysis carbon_stock --territory chaco
  python pipeline/rescore_comparable.py --analysis carbon_stock --territory all
  python pipeline/rescore_comparable.py --analysis all --territory all
"""
from __future__ import annotations
import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "pipeline"))
from scoring import score_with_goalposts, geometric_mean_score  # noqa: E402

GOALPOSTS_PATH = REPO / "pipeline" / "config" / "goalposts.json"
OUTPUT_DIR = REPO / "pipeline" / "output"

TERRITORIES = ['misiones', 'corrientes', 'itapua_py', 'alto_parana_py',
               'chaco', 'formosa', 'parana_br', 'santa_catarina_br',
               'rio_grande_sul_br']

# For each analysis: list of (scaled_component, raw_column, invert).
# raw_column is what's actually in the parquet (the physical-unit column).
# Components that have a goalpost entry under indicators[] become comparable;
# anything else falls back to whatever was in the parquet originally.
LAYER_COMPONENTS = {
    'carbon_stock': [
        ('c_agb_cci',          'c_agb_raw',          False),
        ('c_total_carbon',     'c_total_carbon_raw', False),
        ('c_soc',              'c_soc_tcha',         False),
        ('c_net_flux',         'c_net_flux_raw',     True),   # net_flux: positive = emission = WORSE
        ('c_npp',              'c_npp_raw',          False),  # also included in select sometimes
    ],
    'productive_activity': [
        ('c_viirs',       'c_viirs',       False),
        ('c_npp',         'c_npp',         False),
        ('c_ndvi',        'c_ndvi',        False),
        ('c_forest_loss', 'c_forest_loss', False),
    ],
    'pm25_drivers': [
        # pm25_drivers stores the raw PM2.5 directly; goalposts c_pm25_mean [5,30].
        # The other component scores (fire/climate/terrain/vegetation contributions)
        # are SHAP-like decomposition fractions that already sum to 100 — they don't
        # carry separate raw columns, so we only re-normalize the PM2.5 score itself.
        ('c_pm25_mean',   'c_pm25_mean',   False),  # already in physical units in parquet
    ],
    # soil_water: components don't have goalpost entries (c_soil_moisture, etc.
    # are missing). For now, leave soil_water alone — flagged in the report.
}


def load_goalposts() -> dict:
    with open(GOALPOSTS_PATH, encoding='utf-8') as f:
        return json.load(f)


def parquet_path(analysis: str, territory: str) -> Path:
    if territory == 'misiones':
        return OUTPUT_DIR / f"sat_{analysis}.parquet"
    return OUTPUT_DIR / territory / f"sat_{analysis}.parquet"


def rescore_one(analysis: str, territory: str, goalposts: dict, dry_run: bool) -> dict:
    components = LAYER_COMPONENTS.get(analysis)
    if not components:
        return {'status': 'unsupported', 'analysis': analysis}

    path = parquet_path(analysis, territory)
    if not path.exists():
        return {'status': 'missing_parquet', 'path': str(path)}

    df = pd.read_parquet(path)
    if 'score' not in df.columns:
        return {'status': 'no_score_col', 'path': str(path)}

    indicators = goalposts['indicators']
    scaled = pd.DataFrame(index=df.index)
    used: list[str] = []
    missing: list[str] = []

    for scaled_name, raw_name, invert in components:
        if raw_name not in df.columns:
            missing.append(f"{raw_name} (no col)")
            continue
        gp = indicators.get(scaled_name)
        if not gp or 'lo' not in gp or 'hi' not in gp:
            missing.append(f"{scaled_name} (no goalpost)")
            continue
        s = score_with_goalposts(df[raw_name], gp['lo'], gp['hi'], invert=invert)
        scaled[scaled_name] = s
        used.append(scaled_name)

    if not used:
        return {'status': 'no_usable_components', 'missing': missing}

    # PCA-selected variables for this layer (from goalposts.json). If we have a
    # subset present, use that. Otherwise fall back to all `used`.
    pca_sel = goalposts.get('pca_variable_selection', {}).get(analysis, [])
    if pca_sel:
        pca_used = [c for c in pca_sel if c in scaled.columns]
        if pca_used:
            score_input = scaled[pca_used]
        else:
            score_input = scaled[used]
    else:
        score_input = scaled[used]

    # Geometric mean (HDI-style). Floor at 1.0 so zero components don't kill score.
    new_score = geometric_mean_score(score_input, list(score_input.columns), floor=1.0)

    # Write back: overwrite score, also overwrite scaled component cols where we recomputed
    if dry_run:
        old = df['score'].dropna()
        cmp = new_score.dropna()
        sample = pd.DataFrame({'old': old.head(5).values, 'new': cmp.head(5).values})
        return {
            'status': 'dry_run', 'used_components': used, 'missing': missing,
            'pca_input': list(score_input.columns),
            'old_mean': float(old.mean()), 'new_mean': float(cmp.mean()),
            'old_std': float(old.std()),   'new_std': float(cmp.std()),
            'sample': sample.to_dict('records'),
        }

    df['score'] = new_score
    for col in scaled.columns:
        df[col] = scaled[col]
    df.to_parquet(path, index=False)
    return {
        'status': 'ok', 'used_components': used, 'missing': missing,
        'pca_input': list(score_input.columns),
        'new_mean': float(new_score.dropna().mean()),
        'rows': len(df),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--analysis', required=True,
                    help=f'Analysis id, or "all" (supported: {",".join(LAYER_COMPONENTS)})')
    ap.add_argument('--territory', required=True,
                    help='Territory id, or "all" (9 territories)')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    analyses = list(LAYER_COMPONENTS) if args.analysis == 'all' else [args.analysis]
    territories = TERRITORIES if args.territory == 'all' else [args.territory]

    gp = load_goalposts()
    print(f"goalposts version: {gp.get('version')}, reference: {gp.get('reference_universe', '?')[:80]}...\n")

    for a in analyses:
        print(f"=== {a} ===")
        for t in territories:
            r = rescore_one(a, t, gp, args.dry_run)
            tag = r['status']
            if tag == 'ok':
                print(f"  {t}: OK | used={r['used_components']} | pca={r['pca_input']} | new_mean={r['new_mean']:.1f} | rows={r['rows']}")
            elif tag == 'dry_run':
                print(f"  {t}: DRY | old_mean={r['old_mean']:.1f}->new_mean={r['new_mean']:.1f} | old_std={r['old_std']:.1f}->{r['new_std']:.1f} | used={r['used_components']}")
            else:
                print(f"  {t}: {tag} | {r}")
        print()
    return 0


if __name__ == '__main__':
    sys.exit(main())
