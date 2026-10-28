"""
Department ranking for the flood layer: exposure by terrain (HAND < 5 m).

HAND (Height Above Nearest Drainage, MERIT Hydro v1.0.1, 3″ ≈ 90 m) is the
height of each point over the nearest river or stream it drains to; < 5 m is
the usual floodplain screen (Nobre et al. 2011, Rennó et al. 2008). Two values
per department:
  bld_hand5_pct  = % of buildings whose centroid lies at HAND < 5 m   ← ranking
  land_hand5_pct = % of land (permanent water excluded) at HAND < 5 m
  bld_hand5_n    = number of those buildings

Why not JRC occurrence: in Misiones it ranks river and reservoir shores, not
floods. "% of land with occurrence > 20 %" put Capital first on the Yacyretá
lake; removing permanent water and reservoirs (JRC transition) left every
department under 0.4 % with Iguazú first on the Urugua-í shoreline, and the
"ever saw water" cut added almost nothing — Landsat does not catch the short,
cloudy floods of an incised plateau.

Land excludes JRC permanent water today (transition 1 permanent, 2 new
permanent, 7 seasonal→permanent): rivers have HAND 0 and would otherwise
dominate. Buildings: Misiones = gba_buildings ∪ vida_buildings (gba has a hole
over the east), other territories = gba_buildings_<t>.

Writes a `rank` block into each department of the flood dept summary JSON
(src/lib/data/<territory>_flood_dept_summary.json); split_flood_by_dpto.py
keeps it when it rewrites the summary.

    python pipeline/build_flood_rank.py --territory misiones --export   # GEE → GCS → local tifs
    python pipeline/build_flood_rank.py --territory misiones            # stats → JSON
"""

import argparse
import json
import os
import subprocess
import sys

import h3
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from config import GCS_BUCKET, EXPORT_PREFIX, OUTPUT_DIR, GRID_PATH, get_territory

SRC_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data')
PG = dict(dbname='ndvi_misiones', user='postgres', host='localhost')
HAND_CUT = 5            # metres
PERMANENT = [1, 2, 7]   # JRC transition classes that are permanent water today
BANDS = ['jrc_perm', 'hand_lt5', 'hand_lt5_land']


def territory_paths(tid: str):
    t = get_territory(tid)
    prefix = t['output_prefix']
    t_dir = os.path.join(OUTPUT_DIR, prefix.rstrip('/')) if prefix else OUTPUT_DIR
    grid = os.path.join(t_dir, 'hexagons.geojson') if prefix else GRID_PATH
    summary = 'flood_dept_summary.json' if tid == 'misiones' else f'{tid}_flood_dept_summary.json'
    return prefix, t_dir, grid, os.path.join(SRC_DATA_DIR, summary)


def export(tid: str, t_dir: str) -> None:
    import ee
    from gee_flood_detection import authenticate, get_territory_aoi, export_to_gcs, wait_for_tasks

    authenticate()
    aoi = get_territory_aoi(tid)
    gsw = ee.Image('JRC/GSW1_4/GlobalSurfaceWater')
    perm = gsw.select('transition').remap(PERMANENT, [1] * len(PERMANENT), 0).unmask(0)
    hand5 = ee.Image('MERIT/Hydro/v1_0_1').select('hnd').lt(HAND_CUT).unmask(0)
    images = {'jrc_perm': perm, 'hand_lt5': hand5, 'hand_lt5_land': hand5.And(perm.Not())}
    todo = [b for b in BANDS if not os.path.exists(os.path.join(t_dir, f'{b}.tif'))]
    tasks = [export_to_gcs(images[b].rename(b).toUint8(), b, aoi, tid) for b in todo]
    if tasks:
        wait_for_tasks(tasks)
    os.makedirs(t_dir, exist_ok=True)
    for b in todo:
        src = f'gs://{GCS_BUCKET}/{EXPORT_PREFIX}/{tid}/{b}*.tif'
        subprocess.run(['gcloud', 'storage', 'cp', src, t_dir], check=True, shell=(os.name == 'nt'))


def dept_assignment(prefix: str, departments: list) -> pd.DataFrame:
    """h3index → parquetKey from the dept parquets the CDN serves (same split the UI loads)."""
    import duckdb
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    frames = []
    for d in departments:
        url = f"https://cdn.spatia.ar/data/{prefix}flood_dpto/hex_flood_{d['parquetKey']}.parquet"
        df = con.execute(f"SELECT h3index FROM read_parquet('{url}')").df()
        df['parquetKey'] = d['parquetKey']
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def building_points(tid: str) -> np.ndarray:
    import psycopg2
    if tid == 'misiones':
        sources = [
            'SELECT ST_X(centroid), ST_Y(centroid) FROM gba_buildings',
            'SELECT ST_X(ST_Centroid(geom)), ST_Y(ST_Centroid(geom)) FROM vida_buildings',
        ]
    else:
        sources = [f'SELECT ST_X(centroid), ST_Y(centroid) FROM gba_buildings_{tid}']
    con = psycopg2.connect(**PG)
    chunks = []
    for src in sources:
        cur = con.cursor()
        cur.execute(src)
        chunks.append(np.asarray(cur.fetchall(), dtype=float))
        cur.close()
    con.close()
    return np.vstack(chunks)


def sample_binary(tif: str, lng: np.ndarray, lat: np.ndarray) -> np.ndarray:
    import rasterio
    with rasterio.open(tif) as src:
        a = src.read(1)
        rows, cols = rasterio.transform.rowcol(src.transform, lng, lat)
    rows, cols = np.asarray(rows), np.asarray(cols)
    ok = (rows >= 0) & (rows < a.shape[0]) & (cols >= 0) & (cols < a.shape[1])
    out = np.zeros(len(lng), dtype=np.uint8)
    out[ok] = a[rows[ok], cols[ok]]
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--territory', default='misiones')
    ap.add_argument('--export', action='store_true', help='Launch the GEE exports and download the tifs first')
    args = ap.parse_args()

    prefix, t_dir, grid, summary_path = territory_paths(args.territory)
    if args.export:
        export(args.territory, t_dir)
    tifs = {b: os.path.join(t_dir, f'{b}.tif') for b in BANDS}
    for p in tifs.values():
        if not os.path.exists(p):
            sys.exit(f'Missing {p} — run with --export')

    with open(summary_path, encoding='utf-8') as f:
        summary = json.load(f)
    assign = dept_assignment(prefix, summary['departments'])

    # Land: hex fractions (polygon masking), equal-area hexes → plain sums.
    from process_to_h3 import load_hex_grid, zonal_stats_rasterio
    gdf = load_hex_grid(grid)
    hexes = pd.DataFrame({'h3index': gdf['h3index'].values})
    for b in ('jrc_perm', 'hand_lt5_land'):
        print(f'  zonal stats {b}...')
        hexes[b] = zonal_stats_rasterio(gdf, tifs[b], ignore_src_nodata=True).values
    hexes = hexes.merge(assign, on='h3index', how='inner').dropna()
    land = hexes.assign(land=1 - hexes['jrc_perm']).groupby('parquetKey')[['land', 'hand_lt5_land']].sum()

    # Buildings: HAND at the centroid (raw band — a riverside house on a
    # permanent-water pixel at 30 m still counts as low).
    pts = building_points(args.territory)
    print(f'  {len(pts):,} buildings')
    low = sample_binary(tifs['hand_lt5'], pts[:, 0], pts[:, 1])
    cells = [h3.latlng_to_cell(y, x, 9) for x, y in pts]
    bld = pd.DataFrame({'h3index': cells, 'low': low}).merge(assign, on='h3index', how='inner')
    bld = bld.groupby('parquetKey')['low'].agg(['sum', 'count'])

    for d in summary['departments']:
        k = d['parquetKey']
        if k not in land.index or k not in bld.index:
            d['rank'] = None
            continue
        n, total = int(bld.loc[k, 'sum']), int(bld.loc[k, 'count'])
        d['rank'] = {
            'bld_hand5_pct': round(100 * n / total, 1) if total else None,
            'bld_hand5_n': n,
            'bld_total': total,
            'land_hand5_pct': round(float(100 * land.loc[k, 'hand_lt5_land'] / land.loc[k, 'land']), 1),
        }
        print(f"  {d['dpto']:24} {d['rank']}")

    summary.setdefault('metadata', {})['rank'] = {
        'metric': 'bld_hand5_pct',
        'definition': f'% de edificios (y de la tierra, sin agua permanente) a menos de {HAND_CUT} m '
                      'sobre el curso de agua más cercano (HAND, MERIT Hydro v1.0.1)',
    }
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f'Wrote {summary_path}')


if __name__ == '__main__':
    main()
