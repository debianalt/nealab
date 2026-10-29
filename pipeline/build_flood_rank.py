"""
Department ranking for the flood layer: exposure by terrain (HAND < 5 m).

HAND (Height Above Nearest Drainage, MERIT Hydro v1.0.1, 3″ ≈ 90 m) is the
height of each point over the nearest river or stream it drains to; < 5 m is
the usual floodplain screen (Nobre et al. 2011, Rennó et al. 2008). Per
department:
  bld_hand5_pct  = % of buildings on pixels at HAND < 5 m   ← ranking
  bld_hand5_n    = number of those buildings
  land_hand5_pct = % of land (permanent water excluded) at HAND < 5 m

One method for every territory, all inputs in GEE, one 90 m export on the
MERIT grid with four area/count bands:
  land_m2     = pixel area without JRC permanent water today (transition 1
                permanent, 2 new permanent, 7 seasonal→permanent): rivers have
                HAND 0 and would otherwise dominate the land share
  low_land_m2 = land_m2 where HAND < 5 m
  bld         = buildings, Google Open Buildings Temporal v1 (2023),
                fractional count density × pixel area. At 90 m this matches the
                0.5 m native sum within 0.4 % (Posadas–Encarnación: 379,615 vs
                377,938)
  bld_low     = bld where HAND < 5 m
Pixels go to H3 res-9 cells (pixel centre) and cells to departments through
the dept parquets the CDN serves, so the ranking uses the UI's own split.

Why not JRC occurrence: in Misiones it ranks river and reservoir shores, not
floods — "% of land with occurrence > 20 %" put Capital first on the Yacyretá
lake; without permanent water and reservoirs every department fell under
0.4 % with Iguazú first on the Urugua-í shoreline, and "ever saw water" added
almost nothing: Landsat misses the short, cloudy floods of an incised plateau.

Plain territories (`metadata.rank.saturated`): HAND < 5 m does not separate
departments there and HAND < 1 m ranked them backwards (Chaco's dry interior
first, Bermejo and Sargento Cabral on the río Paraguay last). `--plain` ranks
them by s1_land_pct = % of land (permanent water excluded) with water in at
least 10 % of months 2015–present (Sentinel-1, VV < −15 dB monthly median):
it puts the Bajos Submeridionales first in Chaco, eastern Formosa first and
the Ñeembucú wetlands first. It measures recurrent waterlogging, not episodic
river floods (a 3-month flood is < 10 % of the months). Shown only where the
department spread is ≥ 3 points (Alto Paraguay: 1.0–2.0 %, kept as a note).
The buildings version (% of buildings on such pixels) stayed under 3 %: noise.

WITHDRAWN the same day (29-oct-2026): Sentinel-2 MNDWI sees 0.05 % open water in
Doce de Octubre where this radar metric gives 15–19 %, in October and in June:
VV < −15 dB flags the plain's smooth, saline soils as water. --plain still
computes s1_land_pct but writes shown=False; publishing it needs a method that
survives that check (e.g. change detection against a dry baseline, or S1 ∧ S2).

Change (`--change`, 4th tab): has new construction moved to low ground?
Google Open Buildings Temporal v1 has eight annual epochs (30 June 2016…2023).
One 90 m export per territory with bld_YYYY / low_YYYY (low = HAND < 5 m) and,
per department, linear trends over the eight epochs × 7 years (one bad epoch
does not move them; 2023 ≈ 2022 in Misiones):
  low_new_n    = buildings added on low ground 2016–2023
  bld_new_n    = buildings added in total
  low_tilt_pts = % of the added buildings on low ground − % of the 2016 stock
                 there (points; > 0 = growth drifting to low ground)  ← ranking
None where fewer than MIN_LOW_NEW buildings were added on low ground or fewer
than MIN_BLD_NEW in total. Department bars are published only if their order is
stable between the 2016→2023 and 2017→2022 endpoints (Spearman ≥ CHANGE_MIN_RHO,
≥ CHANGE_MIN_N departments); otherwise low_tilt_pts is None everywhere.
The territory as a whole (metadata.rank.change.province) is always written: share
of the added buildings on low ground vs the 2016 stock, and a label from the four
PROV_WINDOWS pairs — 'toward' (all > +1 pt), 'away' (all < −1), 'none' (all within
±1) or 'unclear'. The UI shows it above the bars, or instead of them.
Misiones check (29-oct-2026): new buildings vs new dwellings of the 2010→2022
census by department, Pearson 0.99 / Spearman 0.91; province +25 % in 7 years
(≈ census pace); low-ground share of the stock rises every year 9.30 → 9.80 %;
tilt order Spearman 0.79–0.99 across 2016–23, 2017–22, 2016–20, 2019–23.

Writes a `rank` block into each department of the flood dept summary JSON
(merging, so the HAND, plain and change runs keep each other's fields);
split_flood_by_dpto.py keeps it when it rewrites the summary.

    python pipeline/build_flood_rank.py --export --all     # launch GEE exports, wait, download
    python pipeline/build_flood_rank.py --all              # tifs → JSON
    python pipeline/build_flood_rank.py --territory corrientes
    python pipeline/build_flood_rank.py --plain --export      # saturated territories, Sentinel-1
    python pipeline/build_flood_rank.py --change --export --all   # 4th tab, Open Buildings Temporal
    python pipeline/build_flood_rank.py --hex --all                # per-hex columns (local tifs)
    python pipeline/build_flood_rank.py --publish-hex --all --backup-dir C:/Users/ant/r2-flood-hand-backup-2026-10

Per hexagon (`--hex`), so the map paints the variable of the active ranking tab
when a department is open, instead of JRC occurrence:
  low_bld_n      buildings at HAND < 5 m            (tabs % edificios, Edificios)
  low_land_pct   % of the land at HAND < 5 m        (tab % tierra; null over water)
  low_bld_new_n  buildings added there 2016–2023    (tab Cambio; linear trend × 7)
Written to output/<prefix>flood_hand_hex.parquet (split_flood_by_dpto.py joins it
on full rebuilds). `--publish-hex` joins them into the parquets the CDN serves
(territory + every department) by h3index, after a byte backup, checks rows and
the per-department building sums against the ranking, and uploads with the same
short cache as the weekly CI refresh, which keeps every served column.
"""

import argparse
import glob
import json
import os
import subprocess
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import GCS_BUCKET, EXPORT_PREFIX, OUTPUT_DIR, TERRITORY_CONFIGS, get_territory

SRC_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data')
CDN = 'https://cdn.spatia.ar/data'
HAND_CUT = 5            # metres
PERMANENT = [1, 2, 7]   # JRC transition classes that are permanent water today
OB_EPOCH = 1688108400   # Open Buildings Temporal: 2023 inference
BANDS = ['land_m2', 'low_land_m2', 'bld', 'bld_low']
NAME = 'flood_rank_hand'
SATURATED_MEDIAN = 80   # % — at or above, HAND < 5 m does not separate departments
PLAIN_NAME = 'flood_rank_s1'
S1_MIN_MONTHS = 0.10    # share of months with water
PLAIN_MIN_SPREAD = 3    # points between the wettest and driest department
CHANGE_NAME = 'flood_rank_change'
OBT_EPOCHS = {           # Open Buildings Temporal inference_time_epoch_s (read from GEE, 29-oct-2026)
    2016: 1467270000, 2017: 1498806000, 2018: 1530342000, 2019: 1561878000,
    2020: 1593500400, 2021: 1625036400, 2022: 1656572400, 2023: 1688108400,
}
CHANGE_YEARS = sorted(OBT_EPOCHS)
CHANGE_BANDS = [f'{k}_{y}' for y in CHANGE_YEARS for k in ('bld', 'low')]
MIN_LOW_NEW = 200       # buildings added on low ground; below, the tilt is noise (Concepción: 121)
CHANGE_MIN_RHO = 0.7    # Spearman of the tilt, 2016→2023 vs 2017→2022 endpoints
MIN_BLD_NEW = 1000      # buildings added in total; below, a few hundred detections swing the tilt
                        # tens of points (Dr. Moisés Bertoni, 497 new: +35)
CHANGE_MIN_N = 8        # departments for the order check; with 5, ρ = 0.7 proves nothing
PROV_WINDOWS = [(2016, 2023), (2017, 2022), (2016, 2020), (2019, 2023)]
PROV_BAND = 1.0         # points: territory label 'none' when every window is within ±PROV_BAND


def write_summary(path: str, summary: dict) -> None:
    """Serialize first, then write: a failed dump must not leave a truncated JSON
    (numpy scalars are not JSON-serializable)."""
    text = json.dumps(summary, ensure_ascii=False, indent=2)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(text)


def territory_paths(tid: str):
    prefix = get_territory(tid)['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, prefix.rstrip('/')) if prefix else OUTPUT_DIR
    summary = 'flood_dept_summary.json' if tid == 'misiones' else f'{tid}_flood_dept_summary.json'
    return prefix, t_dir, os.path.join(SRC_DATA_DIR, summary)


def obt_buildings(epoch: int):
    """Buildings per pixel from Open Buildings Temporal: fractional count density
    (per 0.5 m pixel) × pixel area, so a later reproject to 90 m sums counts."""
    import ee
    dens = (ee.ImageCollection('GOOGLE/Research/open-buildings-temporal/v1')
            .filter(ee.Filter.eq('inference_time_epoch_s', epoch)).mosaic()
            .select('building_fractional_count').unmask(0).divide(0.25))  # per m² (0.5 m px)
    return dens.multiply(ee.Image.pixelArea())


def launch(tid: str):
    import ee
    from gee_flood_detection import get_territory_aoi
    aoi = get_territory_aoi(tid)
    merit = ee.Image('MERIT/Hydro/v1_0_1').select('hnd')
    proj = merit.projection()
    low = merit.lt(HAND_CUT)
    perm = (ee.Image('JRC/GSW1_4/GlobalSurfaceWater').select('transition')
            .remap(PERMANENT, [1] * len(PERMANENT), 0).unmask(0))
    land = ee.Image.pixelArea().multiply(perm.Not())
    bld = obt_buildings(OB_EPOCH)
    img = ee.Image.cat([
        land.rename('land_m2'), land.multiply(low).rename('low_land_m2'),
        bld.rename('bld'), bld.multiply(low).rename('bld_low'),
    ]).reproject(proj).toFloat()
    task = ee.batch.Export.image.toCloudStorage(
        image=img, description=f'{NAME}_{tid}', bucket=GCS_BUCKET,
        fileNamePrefix=f'{EXPORT_PREFIX}/{tid}/{NAME}', region=aoi,
        crs='EPSG:4326', crsTransform=proj.getInfo()['transform'],
        maxPixels=1e11, fileFormat='GeoTIFF',
    )
    task.start()
    print(f'  -> {tid}: {task.id}')
    return task


def download(tid: str, t_dir: str, name: str = NAME) -> None:
    os.makedirs(t_dir, exist_ok=True)
    for old in glob.glob(os.path.join(t_dir, f'{name}*.tif')):
        os.remove(old)
    src = f'gs://{GCS_BUCKET}/{EXPORT_PREFIX}/{tid}/{name}*.tif'
    subprocess.run(['gcloud', 'storage', 'cp', src, t_dir], check=True, shell=(os.name == 'nt'))


def hex_sums(tifs: list, bands: list = BANDS) -> pd.DataFrame:
    """Sum the bands per H3 res-9 cell (pixel centre), block by block. Pixels where
    every band is 0 (no land, no buildings in any year) are skipped."""
    import duckdb
    import rasterio
    con = duckdb.connect()
    con.execute('INSTALL h3 FROM community; LOAD h3;')
    parts = []
    for tif in tifs:
        with rasterio.open(tif) as src:
            for _, win in src.block_windows(1):
                a = src.read(window=win).astype(np.float64)        # (bands, h, w)
                keep = (a > 0).any(axis=0)
                if not keep.any():
                    continue
                rows, cols = np.nonzero(keep)
                t = src.window_transform(win)
                df = pd.DataFrame({
                    'lng': t.c + (cols + 0.5) * t.a, 'lat': t.f + (rows + 0.5) * t.e,
                    **{b: a[i][keep] for i, b in enumerate(bands)},
                })
                parts.append(con.execute(
                    'SELECT h3_latlng_to_cell_string(lat, lng, 9) AS h3index, '
                    + ', '.join(f'SUM({b}) AS {b}' for b in bands)
                    + ' FROM df GROUP BY 1').df())
    return pd.concat(parts).groupby('h3index', as_index=False)[bands].sum()


def dept_assignment(prefix: str, departments: list) -> pd.DataFrame:
    """h3index → parquetKey from the dept parquets the CDN serves (same split the UI loads)."""
    import duckdb
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    urls = {f"{CDN}/{prefix}flood_dpto/hex_flood_{d['parquetKey']}.parquet": d['parquetKey'] for d in departments}
    url_list = ', '.join(f"'{u}'" for u in urls)
    df = con.execute(f'SELECT h3index, filename FROM read_parquet([{url_list}], filename=true)').df()
    df['parquetKey'] = df['filename'].map(urls)
    return df[['h3index', 'parquetKey']].drop_duplicates('h3index')


def process(tid: str) -> None:
    prefix, t_dir, summary_path = territory_paths(tid)
    tifs = sorted(glob.glob(os.path.join(t_dir, f'{NAME}*.tif')))
    if not tifs:
        sys.exit(f'{tid}: no {NAME}*.tif in {t_dir} — run with --export')
    with open(summary_path, encoding='utf-8') as f:
        summary = json.load(f)

    hexes = hex_sums(tifs).merge(dept_assignment(prefix, summary['departments']), on='h3index', how='inner')
    by = hexes.groupby('parquetKey')[BANDS].sum()
    for d in summary['departments']:
        k = d['parquetKey']
        if k not in by.index:
            d['rank'] = None
            continue
        r = by.loc[k]
        d['rank'] = {
            **(d.get('rank') or {}),   # keep plain/change fields from the other runs
            'bld_hand5_pct': round(float(100 * r.bld_low / r.bld), 1) if r.bld >= 1 else None,
            'bld_hand5_n': int(round(r.bld_low)),
            'bld_total': int(round(r.bld)),
            'land_hand5_pct': round(float(100 * r.low_land_m2 / r.land_m2), 1) if r.land_m2 > 0 else None,
        }
    # On a plain nearly everything is < 5 m above some channel: Chaco, Boquerón,
    # Formosa, Ñeembucú, Pdte. Hayes and Alto Paraguay gave medians of 85–100 %
    # (Boquerón, the dry Chaco, 99.6 %). There the bars would all read ~100 % and
    # rank noise, so the UI hides them and says why.
    vals = sorted(d['rank']['bld_hand5_pct'] for d in summary['departments']
                  if d.get('rank') and d['rank'].get('bld_hand5_pct') is not None)
    median = vals[len(vals) // 2] if vals else None
    summary.setdefault('metadata', {})['rank'] = {
        **summary['metadata'].get('rank', {}),   # keep 'plain' and 'change'
        'metric': 'bld_hand5_pct',
        'definition': f'% de edificios (Google Open Buildings 2023) y de la tierra (sin agua permanente JRC) '
                      f'a menos de {HAND_CUT} m sobre el curso de agua más cercano (HAND, MERIT Hydro v1.0.1)',
        'median': median,
        'saturated': bool(median is not None and median >= SATURATED_MEDIAN),
    }
    write_summary(summary_path, summary)
    ranked = sorted((d for d in summary['departments'] if d.get('rank') and d['rank'].get('bld_hand5_pct') is not None),
                    key=lambda d: -d['rank']['bld_hand5_pct'])
    missing = sum(1 for d in summary['departments'] if not d.get('rank'))
    top = ', '.join(f"{d['dpto']} {d['rank']['bld_hand5_pct']}%" for d in ranked[:3])
    print(f'{tid}: {len(ranked)}/{len(summary["departments"])} ranked, {missing} without data · top: {top}')


def launch_plain(tid: str):
    import ee
    from gee_flood_detection import get_territory_aoi, compute_historical_recurrence
    aoi = get_territory_aoi(tid)
    proj = ee.Image('MERIT/Hydro/v1_0_1').select('hnd').projection()
    perm = (ee.Image('JRC/GSW1_4/GlobalSurfaceWater').select('transition')
            .remap(PERMANENT, [1] * len(PERMANENT), 0).unmask(0))
    land = ee.Image.pixelArea().multiply(perm.Not())
    wet = compute_historical_recurrence(aoi).select('flood_recurrence').gte(S1_MIN_MONTHS).unmask(0)
    img = ee.Image.cat([land.rename('land_m2'), land.multiply(wet).rename('land_s1')]).reproject(proj).toFloat()
    task = ee.batch.Export.image.toCloudStorage(
        image=img, description=f'{PLAIN_NAME}_{tid}', bucket=GCS_BUCKET,
        fileNamePrefix=f'{EXPORT_PREFIX}/{tid}/{PLAIN_NAME}', region=aoi,
        crs='EPSG:4326', crsTransform=proj.getInfo()['transform'],
        maxPixels=1e11, fileFormat='GeoTIFF',
    )
    task.start()
    print(f'  -> {tid} (plain): {task.id}')
    return task


def process_plain(tid: str) -> None:
    prefix, t_dir, summary_path = territory_paths(tid)
    tifs = sorted(glob.glob(os.path.join(t_dir, f'{PLAIN_NAME}*.tif')))
    if not tifs:
        sys.exit(f'{tid}: no {PLAIN_NAME}*.tif in {t_dir} — run with --plain --export')
    with open(summary_path, encoding='utf-8') as f:
        summary = json.load(f)
    plain_bands = ['land_m2', 'land_s1']
    hexes = hex_sums(tifs, plain_bands).merge(dept_assignment(prefix, summary['departments']), on='h3index', how='inner')
    by = hexes.groupby('parquetKey')[plain_bands].sum()
    vals = []
    for d in summary['departments']:
        if d['parquetKey'] not in by.index:
            continue
        r = by.loc[d['parquetKey']]
        v = round(float(100 * r.land_s1 / r.land_m2), 1) if r.land_m2 > 0 else None
        d['rank'] = {**(d.get('rank') or {}), 's1_land_pct': v}
        if v is not None:
            vals.append(v)
    spread = float(max(vals) - min(vals)) if vals else 0.0
    summary['metadata']['rank']['plain'] = {
        'metric': 's1_land_pct', 'spread': round(spread, 1), 'shown': False,  # withdrawn, see docstring
        'definition': '% de la tierra (sin agua permanente JRC) con agua en al menos el 10 % de los meses '
                      '2015–2026 (Sentinel-1, VV < −15 dB, mediana mensual)',
    }
    write_summary(summary_path, summary)
    print(f'{tid} (plain): spread {spread:.1f} -> {"shown" if spread >= PLAIN_MIN_SPREAD else "note only"}')


def launch_change(tid: str):
    import ee
    from gee_flood_detection import get_territory_aoi
    aoi = get_territory_aoi(tid)
    merit = ee.Image('MERIT/Hydro/v1_0_1').select('hnd')
    proj = merit.projection()
    low = merit.lt(HAND_CUT)
    bands = []
    for y in CHANGE_YEARS:
        bld = obt_buildings(OBT_EPOCHS[y])
        bands += [bld.rename(f'bld_{y}'), bld.multiply(low).rename(f'low_{y}')]
    img = ee.Image.cat(bands).reproject(proj).toFloat()
    task = ee.batch.Export.image.toCloudStorage(
        image=img, description=f'{CHANGE_NAME}_{tid}', bucket=GCS_BUCKET,
        fileNamePrefix=f'{EXPORT_PREFIX}/{tid}/{CHANGE_NAME}', region=aoi,
        crs='EPSG:4326', crsTransform=proj.getInfo()['transform'],
        maxPixels=1e11, fileFormat='GeoTIFF',
    )
    task.start()
    print(f'  -> {tid} (change): {task.id}')
    return task


def _tilt(r, a: int, b: int):
    """Endpoint tilt between years a and b (points), None on a small or negative base."""
    d_bld, d_low = r[f'bld_{b}'] - r[f'bld_{a}'], r[f'low_{b}'] - r[f'low_{a}']
    if d_low < MIN_LOW_NEW or d_bld < MIN_BLD_NEW or r[f'bld_{a}'] < 1:
        return None
    return 100 * d_low / d_bld - 100 * r[f'low_{a}'] / r[f'bld_{a}']


def process_change(tid: str) -> None:
    prefix, t_dir, summary_path = territory_paths(tid)
    tifs = sorted(glob.glob(os.path.join(t_dir, f'{CHANGE_NAME}*.tif')))
    if not tifs:
        sys.exit(f'{tid}: no {CHANGE_NAME}*.tif in {t_dir} — run with --change --export')
    with open(summary_path, encoding='utf-8') as f:
        summary = json.load(f)
    hexes = hex_sums(tifs, CHANGE_BANDS).merge(dept_assignment(prefix, summary['departments']), on='h3index', how='inner')
    by = hexes.groupby('parquetKey')[CHANGE_BANDS].sum()
    span = CHANGE_YEARS[-1] - CHANGE_YEARS[0]
    t = np.array(CHANGE_YEARS, dtype=float)
    vals, ep_a, ep_b = {}, {}, {}
    for d in summary['departments']:
        k = d['parquetKey']
        if k not in by.index:
            continue
        r = by.loc[k]
        bld_new = float(np.polyfit(t, r[[f'bld_{y}' for y in CHANGE_YEARS]].values.astype(float), 1)[0] * span)
        low_new = float(np.polyfit(t, r[[f'low_{y}' for y in CHANGE_YEARS]].values.astype(float), 1)[0] * span)
        b16, l16 = float(r[f'bld_{CHANGE_YEARS[0]}']), float(r[f'low_{CHANGE_YEARS[0]}'])
        tilt = None
        if low_new >= MIN_LOW_NEW and bld_new >= MIN_BLD_NEW and b16 >= 1:
            tilt = 100 * low_new / bld_new - 100 * l16 / b16
        vals[k] = {'low_new_n': int(round(low_new)), 'bld_new_n': int(round(bld_new)),
                   'low_new_share_pct': round(100 * low_new / bld_new, 1) if bld_new > 0 else None,
                   'low_stock16_pct': round(100 * l16 / b16, 1) if b16 >= 1 else None,
                   'low_tilt_pts': round(tilt, 1) if tilt is not None else None,
                   'low_tilt_all': round(tilt, 1) if tilt is not None else None}  # kept when hidden
        ep_a[k], ep_b[k] = _tilt(r, 2016, 2023), _tilt(r, 2017, 2022)
    both = [k for k in vals if ep_a[k] is not None and ep_b[k] is not None]
    rho = (float(pd.Series([ep_a[k] for k in both]).corr(pd.Series([ep_b[k] for k in both]), method='spearman'))
           if len(both) >= CHANGE_MIN_N else None)
    shown = rho is not None and rho >= CHANGE_MIN_RHO
    # Territory as a whole: the share of the stock on low ground year by year, and the
    # trend tilt. Robust even where the department order is not (few depts, small gaps).
    tot = by[[f'bld_{y}' for y in CHANGE_YEARS]].sum().values.astype(float)
    lowt = by[[f'low_{y}' for y in CHANGE_YEARS]].sum().values.astype(float)
    share = 100 * lowt / tot
    p_bld, p_low = np.polyfit(t, tot, 1)[0] * span, np.polyfit(t, lowt, 1)[0] * span
    flips = sum(1 for k in both if (ep_a[k] > 0) != (ep_b[k] > 0))
    # Label from four pairs of years, so one odd epoch cannot set it.
    yi = {y: i for i, y in enumerate(CHANGE_YEARS)}
    windows = [float(100 * (lowt[yi[b]] - lowt[yi[a]]) / (tot[yi[b]] - tot[yi[a]]) - share[yi[a]])
               for a, b in PROV_WINDOWS]
    if all(w > PROV_BAND for w in windows):
        label = 'toward'
    elif all(w < -PROV_BAND for w in windows):
        label = 'away'
    elif all(abs(w) <= PROV_BAND for w in windows):
        label = 'none'
    else:
        label = 'unclear'
    tilts = [v['low_tilt_all'] for v in vals.values() if v['low_tilt_all'] is not None]
    province = {
        'low_share_by_year': [round(float(s), 2) for s in share],
        'bld_by_year': [int(round(x)) for x in tot],
        'low_new_n': int(round(p_low)), 'bld_new_n': int(round(p_bld)),
        'share_new_pct': round(float(100 * p_low / p_bld), 1) if p_bld > 0 else None,
        'share16_pct': round(float(share[0]), 1),
        'windows': [round(w, 1) for w in windows], 'label': label,
        'tilt_pts': round(float(100 * p_low / p_bld - share[0]), 2) if p_bld > 0 else None,
        'monotonic': bool(np.all(np.diff(share) >= -0.02) or np.all(np.diff(share) <= 0.02)),
        'dept_tilt_p25_p75': [round(float(np.percentile(tilts, 25)), 1), round(float(np.percentile(tilts, 75)), 1)] if tilts else None,
        'sign_flips': flips,
    }
    for d in summary['departments']:
        v = vals.get(d['parquetKey'])
        if v is None:
            continue
        if not shown:
            v['low_tilt_pts'] = None
        d['rank'] = {**(d.get('rank') or {}), **v}
    summary.setdefault('metadata', {}).setdefault('rank', {})['change'] = {
        'metric': 'low_tilt_pts', 'years': f'{CHANGE_YEARS[0]}–{CHANGE_YEARS[-1]}',
        'min_low_new': MIN_LOW_NEW, 'rho': None if rho is None else round(rho, 2),
        'n_ranked': len(both), 'shown': shown, 'province': province,
        'definition': '% de los edificios agregados 2016–2023 (Google Open Buildings Temporal, tendencia anual) '
                      f'a menos de {HAND_CUT} m sobre el curso de agua más cercano (HAND, MERIT Hydro) '
                      'menos el % del parque de 2016 en esa franja, en puntos',
    }
    write_summary(summary_path, summary)
    names = {d['parquetKey']: d.get('dpto') or d.get('distrito') or d.get('municipio') for d in summary['departments']}
    ranked = sorted((k for k in vals if vals[k]['low_tilt_pts'] is not None), key=lambda k: -vals[k]['low_tilt_pts'])
    top = ', '.join(f"{names[k]} {vals[k]['low_tilt_pts']:+.1f}" for k in ranked[:3])
    print(f"{tid} (change): rho={rho if rho is None else round(rho, 2)} n={len(both)} "
          f"{'SHOWN' if shown else 'hidden'} · buildings {tot[0]:,.0f}→{tot[-1]:,.0f} · "
          f"low share {share[0]:.2f}→{share[-1]:.2f}% tilt {province['tilt_pts']} {label} {province['windows']} flips {flips}/{len(both)} · top: {top}")


HEX_COLS = ['low_bld_n', 'low_land_pct', 'low_bld_new_n']


def hex_path(tid: str) -> str:
    prefix = get_territory(tid)['output_prefix']
    return os.path.join(OUTPUT_DIR, f'{prefix}flood_hand_hex.parquet')


def build_hex(tid: str) -> None:
    from parquet_io import write_h3_parquet
    _, t_dir, _ = territory_paths(tid)
    hand = sorted(glob.glob(os.path.join(t_dir, f'{NAME}*.tif')))
    chg = sorted(glob.glob(os.path.join(t_dir, f'{CHANGE_NAME}*.tif')))
    if not hand or not chg:
        sys.exit(f'{tid}: missing {NAME} or {CHANGE_NAME} tifs in {t_dir}')
    a = hex_sums(hand)
    c = hex_sums(chg, CHANGE_BANDS)
    tt = np.array(CHANGE_YEARS, dtype=float)
    tc = tt - tt.mean()
    lows = c[[f'low_{y}' for y in CHANGE_YEARS]].values.astype(float)
    slope = (lows - lows.mean(axis=1, keepdims=True)) @ tc / (tc @ tc)
    c = pd.DataFrame({'h3index': c.h3index, 'low_bld_new_n': np.round(slope * (CHANGE_YEARS[-1] - CHANGE_YEARS[0]), 1)})
    out = a.merge(c, on='h3index', how='outer')
    out['low_bld_n'] = out.bld_low.fillna(0).round(1)
    land = out.land_m2.fillna(0)
    out['low_land_pct'] = np.where(land > 0, np.round(100 * out.low_land_m2.fillna(0) / land.where(land > 0, 1), 1), np.nan)
    out['low_bld_new_n'] = out.low_bld_new_n.fillna(0)
    out = out[['h3index'] + HEX_COLS]
    write_h3_parquet(out, hex_path(tid))
    b = out.low_bld_n[out.low_bld_n > 0]
    n = out.low_bld_new_n[out.low_bld_new_n > 0]
    q = lambda s: '/'.join(f'{v:.0f}' for v in s.quantile([.5, .75, .9, .99])) if len(s) else '-'
    print(f'{tid} (hex): {len(out):,} cells · low_bld_n>0 {len(b):,} p50/75/90/99 {q(b)} · '
          f'low_land_pct mean {out.low_land_pct.mean():.1f} · new>0 {len(n):,} p50/75/90/99 {q(n)}')


def join_hand_hex(df: pd.DataFrame, t_prefix: str) -> pd.DataFrame:
    """Add the per-hex relief columns from output/<prefix>flood_hand_hex.parquet, so a
    full raster rebuild (run_flood_update.py, split_flood_by_dpto.py) keeps what the
    UI paints. Unchanged (with a warning) when the file is missing."""
    path = os.path.join(OUTPUT_DIR, f'{t_prefix}flood_hand_hex.parquet')
    if not os.path.exists(path):
        print(f'  WARNING: {path} missing — relief columns not added (run build_flood_rank.py --hex)')
        return df
    hx = pd.read_parquet(path)
    out = df.drop(columns=[c for c in HEX_COLS if c in df.columns]).merge(hx, on='h3index', how='left')
    out['low_bld_n'] = out.low_bld_n.fillna(0)
    out['low_bld_new_n'] = out.low_bld_new_n.fillna(0)
    return out


def _fetch(url: str, path: str) -> None:
    import urllib.request
    from urllib.parse import quote
    from datetime import datetime
    req = urllib.request.Request(quote(url, safe=':/?=&') + f'?refresh={int(datetime.now().timestamp())}',
                                 headers={'User-Agent': 'spatia-pipeline/1.0'})
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with urllib.request.urlopen(req, timeout=180) as r, open(path, 'wb') as f:
        f.write(r.read())


def _join_hex(df: pd.DataFrame, hx: pd.DataFrame, label: str) -> pd.DataFrame:
    base = df.drop(columns=[c for c in HEX_COLS if c in df.columns])
    out = base.merge(hx, on='h3index', how='left')
    out['low_bld_n'] = out.low_bld_n.fillna(0)
    out['low_bld_new_n'] = out.low_bld_new_n.fillna(0)
    if len(out) != len(df) or set(out.h3index) != set(df.h3index):
        sys.exit(f'{label}: row/h3index mismatch after join ({len(df)} -> {len(out)})')
    return out


def stage_hex(tid: str, backup_dir: str, stage_dir: str) -> list:
    """Back up the served parquets, join the hex columns, check, write staged copies.
    Returns [(local path, r2 key)]."""
    from concurrent.futures import ThreadPoolExecutor
    from parquet_io import write_h3_parquet
    prefix, _, summary_path = territory_paths(tid)
    hx = pd.read_parquet(hex_path(tid))
    with open(summary_path, encoding='utf-8') as f:
        summary = json.load(f)
    keys = [f'data/{prefix}hex_flood_risk.parquet'] + \
           [f"data/{prefix}flood_dpto/hex_flood_{d['parquetKey']}.parquet" for d in summary['departments']]

    def one(key):
        bk = os.path.join(backup_dir, key)
        if not os.path.exists(bk):              # never overwrite the first (pre-change) backup
            _fetch(f'{CDN}/{key[len("data/"):]}', bk)
        df = pd.read_parquet(bk)
        out = _join_hex(df, hx, key)
        p = os.path.join(stage_dir, key)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        write_h3_parquet(out, p)
        return key, p, float(out.low_bld_n.sum())

    with ThreadPoolExecutor(8) as ex:
        res = list(ex.map(one, keys))
    # Department building sums must match the ranking (same split, same rasters).
    rank_n = {f"data/{prefix}flood_dpto/hex_flood_{d['parquetKey']}.parquet": (d.get('rank') or {}).get('bld_hand5_n')
              for d in summary['departments']}
    bad = [(k, s, rank_n[k]) for k, _, s in res[1:]
           if rank_n.get(k) and abs(s - rank_n[k]) > max(5, 0.02 * rank_n[k])]
    if bad:
        sys.exit(f'{tid}: building sums differ from the ranking in {len(bad)} depts, e.g. {bad[:3]}')
    print(f'{tid} (stage): {len(res)} files, territory low_bld_n {res[0][2]:,.0f}, depts match the ranking')
    return [(p, k) for k, p, _ in res]


def publish_hex(tids: list, backup_dir: str, dry_run: bool) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from upload_to_r2 import upload_file
    from update_flood_current import SHORT_CACHE
    stage_dir = os.path.join(OUTPUT_DIR, 'flood_hand_publish')
    staged = []
    for t in tids:
        staged += stage_hex(t, backup_dir, stage_dir)
    if dry_run:
        print(f'dry run: {len(staged)} files staged in {stage_dir}, nothing uploaded')
        return
    with ThreadPoolExecutor(8) as ex:
        ok = list(ex.map(lambda pk: upload_file(pk[0], pk[1], cache_control=SHORT_CACHE), staged))
    failed = [k for (p, k), o in zip(staged, ok) if not o]
    if failed:
        sys.exit(f'{len(failed)} uploads failed: {failed[:5]}')
    print(f'uploaded {len(staged)} files')


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--territory', default='misiones')
    ap.add_argument('--all', action='store_true')
    ap.add_argument('--export', action='store_true', help='Launch the GEE exports, wait and download first')
    ap.add_argument('--plain', action='store_true', help='Sentinel-1 ranking for the saturated (plain) territories')
    ap.add_argument('--change', action='store_true', help='4th tab: new buildings on low ground 2016–2023')
    ap.add_argument('--hex', action='store_true', help='Per-hex columns from the local tifs')
    ap.add_argument('--publish-hex', action='store_true', help='Join the per-hex columns into the served parquets and upload')
    ap.add_argument('--backup-dir', help='Byte backup of the served parquets (required with --publish-hex)')
    ap.add_argument('--dry-run', action='store_true', help='--publish-hex: stage and check, upload nothing')
    args = ap.parse_args()
    if args.plain:
        tids = [t for t in TERRITORY_CONFIGS
                if json.load(open(territory_paths(t)[2], encoding='utf-8'))['metadata'].get('rank', {}).get('saturated')]
        if args.export:
            from gee_flood_detection import authenticate, wait_for_tasks
            authenticate()
            wait_for_tasks([launch_plain(t) for t in tids], timeout=6 * 3600)
            for t in tids:
                t_dir = territory_paths(t)[1]
                for old in glob.glob(os.path.join(t_dir, f'{PLAIN_NAME}*.tif')):
                    os.remove(old)
                subprocess.run(['gcloud', 'storage', 'cp', f'gs://{GCS_BUCKET}/{EXPORT_PREFIX}/{t}/{PLAIN_NAME}*.tif',
                                t_dir], check=True, shell=(os.name == 'nt'))
        for t in tids:
            process_plain(t)
        return
    tids = list(TERRITORY_CONFIGS) if args.all else [args.territory]

    if args.hex:
        for t in tids:
            build_hex(t)
        return
    if args.publish_hex:
        if not args.backup_dir:
            sys.exit('--publish-hex needs --backup-dir')
        big = [t for t in tids if t.endswith('_br')]
        publish_hex([t for t in tids if t not in big] + big, args.backup_dir, args.dry_run)
        return

    if args.change:
        if args.export:
            from gee_flood_detection import authenticate, wait_for_tasks
            authenticate()
            wait_for_tasks([launch_change(t) for t in tids], timeout=8 * 3600)
            for t in tids:
                download(t, territory_paths(t)[1], CHANGE_NAME)
        for t in tids:
            process_change(t)
        return

    if args.export:
        from gee_flood_detection import authenticate, wait_for_tasks
        authenticate()
        tasks = [launch(t) for t in tids]
        wait_for_tasks(tasks, timeout=6 * 3600)
        for t in tids:
            download(t, territory_paths(t)[1])
    for t in tids:
        process(t)


if __name__ == '__main__':
    main()
