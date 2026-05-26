"""
Re-baseline goalposts v1.1 — Fase A: pool raw raster pixels from 9 territories.

Reads sat_<analysis>_raster.tif directly (not parquets, which only contain
percentile/score columns, not raw). Pools raw values across pool territories
with per-territory subsample for balance, computes P2/P98 → goalposts.json.

Scope (Fase A): only 6 core raster analyses. Specialized analyses' goalpost
entries (carbon_stock, productive_activity, deforestation_dynamics,
pm25_drivers, location_value) preserved verbatim from v1.0.

Usage:
  python pipeline/compute_goalposts_v11.py --dry-run
  python pipeline/compute_goalposts_v11.py
"""

import argparse
import json
import os
import shutil
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import rasterio

sys.path.insert(0, os.path.dirname(__file__))
from config import OUTPUT_DIR
from scoring import score_with_goalposts, select_variables

GOALPOSTS_PATH = os.path.join(os.path.dirname(__file__), 'config', 'goalposts.json')

POOL_TERRITORIES = ['misiones', 'corrientes', 'itapua_py', 'alto_parana_py',
                    'chaco', 'formosa', 'parana_br', 'santa_catarina_br',
                    'rio_grande_sul_br']

CORE_ANALYSES = ['environmental_risk', 'climate_comfort', 'green_capital',
                 'change_pressure', 'agri_potential', 'forest_health']

# Smallest territory (alto_parana_py) ~150K hex; raw rasters are larger.
# 200K balanced pool keeps variance representative.
SUBSAMPLE_PER_TERRITORY = 200_000

# (component_name, invert). Band order matches raster band order (verified).
ANALYSIS_COMPONENTS = {
    'environmental_risk': [('c_fire', False), ('c_deforest', False),
                           ('c_thermal_amp', False), ('c_slope', False),
                           ('c_hand', True)],
    'climate_comfort':    [('c_heat_day', True), ('c_heat_night', True),
                           ('c_precipitation', False), ('c_frost', True),
                           ('c_water_stress', False)],
    'green_capital':      [('c_ndvi', False), ('c_treecover', False),
                           ('c_npp', False), ('c_lai', False),
                           ('c_vcf', False)],
    'change_pressure':    [('c_viirs_trend', False), ('c_ghsl_change', False),
                           ('c_hansen_loss', False), ('c_ndvi_trend', True),
                           ('c_fire_count', False)],
    'agri_potential':     [('c_soc', False), ('c_ph_optimal', False),
                           ('c_clay', False), ('c_precipitation', False),
                           ('c_gdd', False), ('c_slope', True)],
    'forest_health':      [('c_ndvi_trend', False), ('c_loss_ratio', True),
                           ('c_fire', True), ('c_gpp', False), ('c_et', False)],
}

TIER1_INDICATORS = {'c_treecover', 'c_water_stress', 'c_ph_optimal',
                    'c_occurrence', 'c_recurrence', 'c_extent'}


def raster_path(analysis_id, territory_id):
    if territory_id == 'misiones':
        return os.path.join(OUTPUT_DIR, f'sat_{analysis_id}_raster.tif')
    return os.path.join(OUTPUT_DIR, territory_id, f'sat_{analysis_id}_raster.tif')


def read_raster_band(path, band_idx, subsample, rng):
    with rasterio.open(path) as src:
        arr = src.read(band_idx)
        nodata = src.nodata
    flat = arr.flatten().astype(np.float64)
    if nodata is not None:
        flat = flat[flat != nodata]
    flat = flat[~np.isnan(flat)]
    if subsample and len(flat) > subsample:
        flat = rng.choice(flat, size=subsample, replace=False)
    return flat


def pool_indicator_raw(analysis_id):
    components = ANALYSIS_COMPONENTS[analysis_id]
    pooled = {name: [] for name, _ in components}
    rng = np.random.default_rng(42)

    for territory in POOL_TERRITORIES:
        path = raster_path(analysis_id, territory)
        if not os.path.exists(path):
            print(f"  [SKIP] {territory}/{analysis_id}: raster missing")
            continue
        for band_idx, (name, _invert) in enumerate(components, start=1):
            arr = read_raster_band(path, band_idx, SUBSAMPLE_PER_TERRITORY, rng)
            pooled[name].append(arr)
            print(f"  {territory:25} band{band_idx} {name:18} n={len(arr):,}")

    return {name: np.concatenate(parts) for name, parts in pooled.items() if parts}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    print("=" * 70)
    print("  Compute Goalposts v1.1 — Fase A (raster-direct pool, 9 territorios)")
    print("=" * 70)
    print(f"  Pool: {len(POOL_TERRITORIES)} territories × {len(CORE_ANALYSES)} core analyses")
    print(f"  Subsample: {SUBSAMPLE_PER_TERRITORY:,} pixels per (territory, component)")

    with open(GOALPOSTS_PATH) as f:
        goalposts = json.load(f)

    new_bounds = {}
    pooled_all = {}

    for aid in CORE_ANALYSES:
        print(f"\n=== {aid} ===")
        pooled = pool_indicator_raw(aid)
        for name, arr in pooled.items():
            pooled_all[name] = arr
            if name in TIER1_INDICATORS:
                continue
            lo = float(np.percentile(arr, 2))
            hi = float(np.percentile(arr, 98))
            if hi > lo:
                new_bounds[name] = (lo, hi)

    print("\n" + "=" * 70)
    print("  Diff vs goalposts.json v1.0")
    print("=" * 70)
    changed = []
    for name, (lo_new, hi_new) in sorted(new_bounds.items()):
        entry = goalposts['indicators'].get(name)
        if not entry:
            print(f"  [WARN] {name} not in goalposts.json")
            continue
        if entry.get('tier') == 1:
            continue
        lo_old, hi_old = entry['lo'], entry['hi']
        print(f"  {name:18}  lo {lo_old:>10.3f} -> {lo_new:>10.3f}  "
              f"(d {lo_new - lo_old:+.3f})   hi {hi_old:>10.3f} -> {hi_new:>10.3f}  "
              f"(d {hi_new - hi_old:+.3f})")
        changed.append(name)

    print("\n" + "=" * 70)
    print("  Re-running PCA variable selection on pooled normalized data")
    print("=" * 70)
    new_pca = {}
    for aid in CORE_ANALYSES:
        components = ANALYSIS_COMPONENTS[aid]
        comp_names = [c for c, _ in components if c in pooled_all]
        if len(comp_names) < 2:
            continue
        min_len = min(len(pooled_all[c]) for c in comp_names)
        df = pd.DataFrame({c: pooled_all[c][:min_len] for c in comp_names})
        df_norm = pd.DataFrame()
        for c in comp_names:
            if c in new_bounds:
                lo, hi = new_bounds[c]
            else:
                entry = goalposts['indicators'][c]
                lo, hi = entry['lo'], entry['hi']
            invert = next(i for n, i in components if n == c)
            df_norm[c] = score_with_goalposts(df[c], lo, hi, invert=invert)
        retained = select_variables(df_norm.dropna(), comp_names, threshold=0.70)
        dropped = [c for c in comp_names if c not in retained]
        new_pca[aid] = retained
        print(f"  {aid:20} retained={retained}")
        if dropped:
            print(f"  {' ':20} dropped ={dropped}")

    if args.dry_run:
        print(f"\n[DRY RUN] Would update {len(changed)} Tier 2 entries + "
              f"{len(new_pca)} PCA selections. Not writing.")
        return 0

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup = GOALPOSTS_PATH + f'.bak.{ts}'
    shutil.copy2(GOALPOSTS_PATH, backup)
    print(f"\n  Backup: {backup}")

    for name, (lo_new, hi_new) in new_bounds.items():
        if name in goalposts['indicators'] and goalposts['indicators'][name].get('tier') != 1:
            goalposts['indicators'][name]['lo'] = round(lo_new, 4)
            goalposts['indicators'][name]['hi'] = round(hi_new, 4)

    for aid, retained in new_pca.items():
        goalposts.setdefault('pca_variable_selection', {})[aid] = retained

    goalposts['version'] = '1.1'
    goalposts['computed'] = datetime.now().strftime('%Y-%m-%d')
    goalposts['reference_universe'] = (
        "Full NEA + trans-frontera AR/PY + Mata Atlântica BR sur. Pooled raw "
        "pixel distribution across 9 territories: Misiones, Corrientes (AR), "
        "Itapúa, Alto Paraná (PY), Chaco, Formosa (AR), Paraná, Santa Catarina, "
        f"Rio Grande do Sul (BR). Subsample {SUBSAMPLE_PER_TERRITORY:,} pixels "
        "per (territory, component) for cross-territory balance. Only the 6 "
        "core raster analyses (environmental_risk, climate_comfort, "
        "green_capital, change_pressure, agri_potential, forest_health) were "
        "re-baselined in this v1.1 Fase A. Specialized analyses (carbon_stock, "
        "productive_activity, deforestation_dynamics, pm25_drivers, "
        "location_value) keep v1.0 bounds pending Fase B."
    )
    goalposts['territories_included_v1_1'] = POOL_TERRITORIES
    goalposts.pop('territories_pending_bump', None)
    goalposts.setdefault('changelog', []).append({
        'version': '1.1', 'date': datetime.now().strftime('%Y-%m-%d'),
        'changes': (
            f'Fase A re-baseline: pool of {len(POOL_TERRITORIES)} territories '
            f'for {len(CORE_ANALYSES)} core raster analyses ({len(changed)} '
            f'Tier 2 indicators updated, {len(new_pca)} PCA selections '
            'updated). Specialized analyses unchanged.'
        ),
    })

    with open(GOALPOSTS_PATH, 'w', encoding='utf-8') as f:
        json.dump(goalposts, f, indent=2, ensure_ascii=False)
    print(f"  Wrote {GOALPOSTS_PATH}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
