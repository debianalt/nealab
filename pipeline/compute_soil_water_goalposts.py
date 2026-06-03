"""Compute soil_water goalposts (P2/P98 per component) by pooling raw raster
pixels across all 9 territories. Writes back into goalposts.json under the
`indicators` block — the same shape `compute_goalposts_v11.py` uses for the
6 core analyses, so process_raster_to_h3.py --mode comparable picks them up
automatically.

Components: c_soil_moisture, c_dry_season, c_precipitation, c_actual_et.
c_precipitation already has a goalpost (kept). The other 3 are added.

Subsample 200K valid pixels per (territory, band) for balance.
"""
from __future__ import annotations
import json
import os
import sys
from pathlib import Path

import numpy as np
import rasterio

REPO = Path(__file__).resolve().parents[1]
GP_PATH = REPO / "pipeline" / "config" / "goalposts.json"
OUT_DIR = REPO / "pipeline" / "output"

TERRITORIES = ['misiones', 'corrientes', 'itapua_py', 'alto_parana_py',
               'chaco', 'formosa', 'parana_br', 'santa_catarina_br',
               'rio_grande_sul_br']

BANDS = ['c_soil_moisture', 'c_dry_season', 'c_precipitation', 'c_actual_et']

UNITS = {
    'c_soil_moisture': 'm3/m3',
    'c_dry_season':    'm3/m3',
    'c_precipitation': 'mm/yr',
    'c_actual_et':     'mm/8d',
}

# Skip re-computing entries that already exist (preserve frozen v1 lo/hi).
PRESERVE_EXISTING = {'c_precipitation'}

SUBSAMPLE = 200_000
SEED = 42


def raster_path(t: str) -> Path:
    if t == 'misiones':
        return OUT_DIR / 'sat_soil_water_raster.tif'
    return OUT_DIR / t / 'sat_soil_water_raster.tif'


def sample_band(path: Path, band_idx: int, n: int, rng: np.random.Generator) -> np.ndarray:
    with rasterio.open(path) as src:
        arr = src.read(band_idx).astype(float)
        nodata = src.nodata
    arr = arr.ravel()
    if nodata is not None:
        arr = arr[arr != nodata]
    arr = arr[np.isfinite(arr)]
    if arr.size <= n:
        return arr
    idx = rng.choice(arr.size, size=n, replace=False)
    return arr[idx]


def main() -> int:
    rng = np.random.default_rng(SEED)
    gp = json.loads(GP_PATH.read_text(encoding='utf-8'))

    print(f'Pooling {SUBSAMPLE:,} pixels per (territory, band) across {len(TERRITORIES)} territories...\n')

    for band_idx, band in enumerate(BANDS, start=1):
        if band in PRESERVE_EXISTING:
            cur = gp['indicators'].get(band, {})
            print(f'{band}: PRESERVE existing  lo={cur.get("lo")} hi={cur.get("hi")} unit={cur.get("unit")}')
            continue
        pooled = []
        for t in TERRITORIES:
            p = raster_path(t)
            if not p.exists():
                print(f'  WARN {t}/{band}: raster not found')
                continue
            s = sample_band(p, band_idx, SUBSAMPLE, rng)
            pooled.append(s)
            print(f'  {t}/{band}: n={len(s):,} p2={np.percentile(s,2):.4f} p98={np.percentile(s,98):.4f}')
        all_vals = np.concatenate(pooled)
        p2, p98 = float(np.percentile(all_vals, 2)), float(np.percentile(all_vals, 98))
        gp['indicators'][band] = {
            'lo': p2, 'hi': p98, 'tier': 2, 'unit': UNITS.get(band, ''),
        }
        print(f'  {band}: POOLED  lo={p2:.4f}  hi={p98:.4f}  unit={UNITS[band]}\n')

    # Add soil_water to PCA variable selection (full set — all 4 components).
    gp.setdefault('pca_variable_selection', {})
    gp['pca_variable_selection']['soil_water'] = list(BANDS)

    # Bump version metadata
    cl = gp.setdefault('changelog', [])
    cl.append({
        'version': '1.1b-soil_water',
        'date': '2026-05-29',
        'note': 'Add c_soil_moisture / c_dry_season / c_actual_et goalposts pooled across 9 territories. c_precipitation preserved from v1.1. soil_water added to pca_variable_selection.',
    })

    GP_PATH.write_text(json.dumps(gp, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(f'Wrote goalposts -> {GP_PATH}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
