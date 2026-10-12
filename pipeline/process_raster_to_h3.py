"""
Process multi-band GeoTIFF rasters to H3 res-9 parquets with pixel-level zonal stats.

Each band becomes a component column. Percentile rank normalisation (0-100)
is applied per band, then a PCA-validated geometric mean score is computed
(OECD Handbook / UNDP HDI methodology, consistent with compute_satellite_scores.py).
All retained components weigh the same: there are no per-component weights.

This REPLACES the radio-level crosswalk approach for analyses where
GEE rasters are available — each hexagon gets its own pixel-averaged value.

Usage:
  python pipeline/process_raster_to_h3.py --analysis agri_potential
  python pipeline/process_raster_to_h3.py --analysis all
"""

import argparse
import json
import os
import sys
import time

import numpy as np
import pandas as pd
import rasterio
from rasterio.features import geometry_mask
from rasterio.windows import from_bounds
from shapely.geometry import shape

from config import OUTPUT_DIR, H3_RESOLUTION, get_territory
from parquet_io import write_h3_parquet
from scoring import run_full_diagnostics, geometric_mean_score, load_goalposts, score_with_goalposts

HEXAGONS_PATH = os.path.join(OUTPUT_DIR, "hexagons-lite.geojson")
# Component definitions per analysis: (band_name, output_col, invert)
ANALYSIS_COMPONENTS = {
    'agri_potential': [
        ('c_soc', 'c_soc', False),
        ('c_ph_optimal', 'c_ph_optimal', False),
        ('c_clay', 'c_clay', False),
        ('c_precipitation', 'c_precipitation', False),
        ('c_gdd', 'c_gdd', False),
        ('c_slope', 'c_slope', True),
    ],
}

PIXEL_ANALYSES = list(ANALYSIS_COMPONENTS.keys())


def zonal_stats_band(src, band_idx, geom, nodata=None):
    """Compute mean of a raster band within a polygon."""
    bounds = geom.bounds
    try:
        window = from_bounds(*bounds, transform=src.transform)
        data = src.read(band_idx, window=window).astype(float)
        if data.size == 0:
            return np.nan

        transform = rasterio.windows.transform(window, src.transform)
        mask = geometry_mask([geom], out_shape=data.shape, transform=transform, invert=True)

        data[~mask] = np.nan
        if nodata is not None:
            data[data == nodata] = np.nan

        valid = data[~np.isnan(data)]
        return float(np.mean(valid)) if len(valid) > 0 else np.nan
    except Exception:
        return np.nan


def percentile_rank(series):
    """Percentile rank 0-100."""
    valid = series.notna()
    result = pd.Series(np.nan, index=series.index)
    if valid.sum() > 1:
        result[valid] = series[valid].rank(pct=True) * 100.0
    return result


def process_analysis(analysis_id, raster_path, hexgrid_features, output_path,
                     territory_id='misiones', mode='local', goalposts=None):
    """Process a single analysis raster to H3 parquet."""
    components = ANALYSIS_COMPONENTS[analysis_id]
    band_names = [c[0] for c in components]

    print(f"\n  Processing {analysis_id}: {len(band_names)} bands")
    print(f"  Raster: {raster_path}")

    t0 = time.time()
    results = []

    with rasterio.open(raster_path) as src:
        print(f"  Size: {src.width}x{src.height}, {src.count} bands, CRS={src.crs}")
        nodata = src.nodata

        # Build band index mapping
        # GEE exports band names as descriptions; fall back to ordinal
        band_map = {}
        for i, bname in enumerate(band_names):
            band_map[bname] = i + 1  # rasterio is 1-indexed

        n_features = len(hexgrid_features)
        for fi, feat in enumerate(hexgrid_features):
            if fi % 50000 == 0 and fi > 0:
                elapsed = time.time() - t0
                rate = fi / elapsed
                eta = (n_features - fi) / rate / 60
                print(f"    {fi:,}/{n_features:,} ({rate:.0f} hex/s, ETA {eta:.1f} min)")

            h3index = (feat.get("properties", {}).get("h3index")
                       or feat.get("properties", {}).get("h3_index")
                       or feat.get("id"))
            geom = shape(feat["geometry"])

            row = {"h3index": h3index}
            for bname, band_idx in band_map.items():
                if band_idx <= src.count:
                    row[bname] = zonal_stats_band(src, band_idx, geom, nodata)
                else:
                    row[bname] = np.nan
            results.append(row)

    df = pd.DataFrame(results)
    print(f"  Raw hexagons: {len(df):,}")

    # Normalise: goalpost (comparable mode) or percentile rank (local mode)
    gp_indicators = goalposts['indicators'] if goalposts else {}
    for band_name, out_col, invert in components:
        raw = df[band_name].astype(float)
        if mode == 'comparable' and out_col in gp_indicators:
            gp = gp_indicators[out_col]
            df[out_col] = score_with_goalposts(raw, gp['lo'], gp['hi'],
                                               invert=gp.get('invert', invert)).round(1)
        else:
            if invert:
                raw = -raw
            df[out_col] = percentile_rank(raw).round(1)

    # PCA-validated geometric mean score (OECD/HDI methodology)
    comp_cols = [c[1] for c in components]

    # Exclude 100% NaN columns
    valid_cols = [c for c in comp_cols if not df[c].isna().all()]
    dropped_nan = set(comp_cols) - set(valid_cols)
    if dropped_nan:
        print(f"  Excluded (no data): {dropped_nan}")

    # Run diagnostics on non-NaN subset
    valid_mask = df[valid_cols].notna().all(axis=1)
    df_valid = df.loc[valid_mask]
    print(f"  Valid hexagons for PCA: {len(df_valid):,} / {len(df):,}")

    diagnostics = run_full_diagnostics(df_valid, valid_cols, corr_threshold=0.70)

    kmo = diagnostics["kmo_bartlett"].get("kmo_overall")
    if kmo is not None:
        print(f"    KMO: {kmo:.3f} {'OK' if kmo >= 0.60 else 'WARNING < 0.60'}")

    if mode == 'comparable' and goalposts:
        locked = goalposts.get('pca_variable_selection', {}).get(analysis_id)
        if locked:
            retained = [c for c in locked if c in valid_cols]
            dropped = [c for c in valid_cols if c not in retained]
            print(f"    [comparable] Locked selection ({len(retained)}): {retained}")
            if dropped:
                print(f"    [comparable] Excluded: {dropped}")
        else:
            retained = diagnostics["variable_selection"]["retained"]
            dropped = diagnostics["variable_selection"]["dropped"]
            print(f"    [comparable] No lock: using PCA selection: {retained}")
    else:
        retained = diagnostics["variable_selection"]["retained"]
        dropped = diagnostics["variable_selection"]["dropped"]
        if dropped:
            print(f"    Dropped (|r|>0.70): {dropped}")
        print(f"    Retained ({len(retained)}): {retained}")

    df['score'] = geometric_mean_score(df, retained, floor=1.0).round(1)

    # Output columns
    out_cols = ['h3index', 'score'] + [c[1] for c in components]
    result = df[out_cols].dropna(subset=['score'])

    write_h3_parquet(result, output_path)
    elapsed = time.time() - t0
    size_kb = os.path.getsize(output_path) / 1024

    print(f"  Output: {output_path}")
    print(f"  Rows: {len(result):,}, Size: {size_kb:.0f} KB")
    print(f"  Score: mean={result['score'].mean():.1f}, median={result['score'].median():.1f}")
    print(f"  Time: {elapsed:.0f}s")

    return len(result)


def main():
    parser = argparse.ArgumentParser(description="Process GEE rasters to H3 parquets")
    parser.add_argument("--analysis", required=True, help="Analysis ID or 'all'")
    parser.add_argument("--territory", default="misiones",
                        help="Territory ID from config.py (default: misiones)")
    parser.add_argument("--input-dir", default=None,
                        help="Directory with GeoTIFFs (default: OUTPUT_DIR or territory subdir)")
    parser.add_argument("--mode", choices=['comparable', 'local'], default='local',
                        help="comparable: goalpost normalization. local: percentile rank (default).")
    args = parser.parse_args()

    goalposts = load_goalposts() if args.mode == 'comparable' else None
    if goalposts:
        print(f"  Loaded goalposts v{goalposts.get('version', '?')} ({args.mode} mode)")

    territory = get_territory(args.territory)
    t_prefix = territory['output_prefix']

    # For non-misiones territories, input/output live in a subdirectory
    if t_prefix:
        t_dir = os.path.join(OUTPUT_DIR, t_prefix.rstrip('/'))
        input_dir = args.input_dir or t_dir
        out_dir = t_dir
        hexgrid_path = os.path.join(t_dir, 'hexagons.geojson')
    else:
        input_dir = args.input_dir or OUTPUT_DIR
        out_dir = OUTPUT_DIR
        hexgrid_path = HEXAGONS_PATH  # legacy Misiones path

    os.makedirs(out_dir, exist_ok=True)

    if args.analysis == 'all':
        analyses = PIXEL_ANALYSES
    else:
        analyses = [a.strip() for a in args.analysis.split(',')]

    # Load hex grid
    print(f"Loading hexagon grid from {hexgrid_path}...")
    if not os.path.exists(hexgrid_path):
        print(f"  ERROR: hex grid not found. Run build_admin_crosswalk.py --territory {args.territory} first")
        return 1
    with open(hexgrid_path, 'r') as f:
        hexgrid = json.load(f)
    features = hexgrid['features']
    print(f"  {len(features):,} hexagons  [{territory['label']}]")

    t0 = time.time()
    results = {}

    for aid in analyses:
        if aid not in ANALYSIS_COMPONENTS:
            print(f"  SKIP {aid}: no component definition")
            continue

        raster_path = os.path.join(input_dir, f"sat_{aid}_raster.tif")
        if not os.path.exists(raster_path):
            print(f"  SKIP {aid}: raster not found at {raster_path}")
            continue

        output_path = os.path.join(out_dir, f"sat_{aid}.parquet")
        n = process_analysis(aid, raster_path, features, output_path,
                             territory_id=args.territory, mode=args.mode, goalposts=goalposts)
        results[aid] = n

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"  Processed {len(results)} analyses in {elapsed:.0f}s  [{territory['label']}]")
    for aid, n in results.items():
        print(f"    {aid}: {n:,} hexagons")
    print(f"{'=' * 60}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
