"""
Fix agri_potential c_clay monotonic bias.

Current: linear normalization (more clay = higher score) favours Misiones
lateritic oxisols over productive Chaco/RS soils.

Fix: triangular hump centred at ~30% clay (optimal for most crops).
The stored c_clay is already goalpost-normalised (0-100, lo=0 hi=579 g/kg),
so optimal_normalised = 300/579 * 100 = 51.8.

Usage:
  python pipeline/fix_agri_clay_hump.py --dry-run
  python pipeline/fix_agri_clay_hump.py --commit
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Add pipeline dir to path for scoring import
sys.path.insert(0, str(Path(__file__).parent))
from scoring import geometric_mean_score

CLAY_OPTIMAL = 51.8   # normalised value corresponding to ~30% clay (300 g/kg / 579)
CLAY_HALF_WIDTH = 51.8  # triangle zeroes at 0 and ~103.6 (clipped to 0)

PCA_COLS = ['c_soc', 'c_ph_optimal', 'c_clay', 'c_precipitation', 'c_gdd', 'c_slope']

OUTPUT_DIR = Path(__file__).parent / 'output'

TERRITORIES = [
    ('misiones',           OUTPUT_DIR),
    ('corrientes',         OUTPUT_DIR / 'corrientes'),
    ('itapua_py',          OUTPUT_DIR / 'itapua_py'),
    ('alto_parana_py',     OUTPUT_DIR / 'alto_parana_py'),
    ('chaco',              OUTPUT_DIR / 'chaco'),
    ('formosa',            OUTPUT_DIR / 'formosa'),
    ('parana_br',          OUTPUT_DIR / 'parana_br'),
    ('santa_catarina_br',  OUTPUT_DIR / 'santa_catarina_br'),
    ('rio_grande_sul_br',  OUTPUT_DIR / 'rio_grande_sul_br'),
]


def clay_hump(series: pd.Series) -> pd.Series:
    return np.maximum(0.0, (1 - np.abs(series - CLAY_OPTIMAL) / CLAY_HALF_WIDTH)) * 100


def process_territory(name: str, out_dir: Path, dry_run: bool) -> dict:
    path = out_dir / 'sat_agri_potential.parquet'
    if not path.exists():
        return {'territory': name, 'status': 'missing', 'path': str(path)}

    df = pd.read_parquet(path)

    missing = [c for c in PCA_COLS if c not in df.columns]
    if missing:
        return {'territory': name, 'status': 'missing_cols', 'missing': missing}

    old_clay_mean = df['c_clay'].mean()
    old_score_mean = df['score'].mean()

    df['c_clay'] = clay_hump(df['c_clay']).round(1)
    new_score = geometric_mean_score(df, PCA_COLS, floor=1.0).round(1)

    new_clay_mean = df['c_clay'].mean()
    new_score_mean = new_score.mean()

    result = {
        'territory': name,
        'status': 'dry_run' if dry_run else 'ok',
        'clay_before': round(old_clay_mean, 1),
        'clay_after':  round(new_clay_mean, 1),
        'score_before': round(old_score_mean, 1),
        'score_after':  round(new_score_mean, 1),
        'rows': len(df),
    }

    if not dry_run:
        df['score'] = new_score
        df.to_parquet(path, index=False)

    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--dry-run', action='store_true', help='Report before/after, do not write')
    group.add_argument('--commit',  action='store_true', help='Apply transform and overwrite parquets')
    args = parser.parse_args()

    dry_run = args.dry_run

    print(f"\n{'DRY RUN — ' if dry_run else ''}agri_potential c_clay hump fix")
    print(f"Optimal: {CLAY_OPTIMAL:.1f} normalised (~30% clay)")
    print(f"{'Territory':<25} {'clay_before':>12} {'clay_after':>10} {'score_before':>13} {'score_after':>11} {'rows':>8}")
    print("-" * 85)

    results = []
    for name, out_dir in TERRITORIES:
        r = process_territory(name, out_dir, dry_run=dry_run)
        results.append(r)
        if r['status'] in ('missing', 'missing_cols'):
            print(f"  {name:<23} SKIP: {r['status']} {r.get('path','') or r.get('missing','')}")
        else:
            print(
                f"  {name:<23}"
                f"  {r['clay_before']:>10.1f}  {r['clay_after']:>10.1f}"
                f"  {r['score_before']:>12.1f}  {r['score_after']:>11.1f}"
                f"  {r['rows']:>8,}"
            )

    if not dry_run:
        ok = sum(1 for r in results if r['status'] == 'ok')
        print(f"\nWrote {ok}/{len(TERRITORIES)} parquets.")
        print("Next: split_by_admin.py --only=agri_potential for each territory, then R2 upload.")
    else:
        print("\nDry run complete. Run with --commit to apply.")


if __name__ == '__main__':
    main()
