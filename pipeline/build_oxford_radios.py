"""
Travel time to healthcare per census tract (radio) for the 4 AR provinces, from GEE.

health_access used proxies outside Misiones until 29-oct-2026: Corrientes had
distance-to-hospital × 12 min/km (build_oxford_proxy_corrientes.py, with 300 min
where missing — 356 of 445 Capital tracts), and Chaco and Formosa copied
time-to-cities as time-to-healthcare (build_census_proxies_ar.py).

Per tract, from one ~1 km export per territory (mean of the pixels inside the tract;
tracts smaller than a pixel take the centroid pixel):

  accessibility_cities_min       Oxford/MAP/accessibility_to_cities_2015_v1_0
  accessibility_healthcare_min   min(Oxford, OSM) motorised minutes to the nearest
                                 health facility:
                                 - Oxford = Malaria Atlas Project accessibility to
                                   healthcare 2019 (supersedes Oxford/MAP/…_2019, which
                                   GEE reports "corrupted"; over Misiones' tracts it
                                   matches the old asset at r = 0.998)
                                 - OSM = cumulative cost over the same MAP friction
                                   surface (2019 v5.1, motorised) from OSM hospitals,
                                   clinics and doctors — the query compute_accessibility_h3
                                   uses. Oxford's facility inventory misses towns: urban
                                   Mercedes (Corrientes) read 78 min, and 17 % of urban
                                   tracts in Corrientes and 16 % in Formosa were > 30 min
  accessibility_healthcare_oxford / _osm   the two inputs, kept for audit
  accessibility_healthcare_walk  empty: the new asset has no walking-only band

The previous file is kept as *.proxy.parquet (Misiones: *.pre-osm.parquet).
--validate compares the Oxford part with Misiones' existing values (old asset).

    python pipeline/build_oxford_radios.py --all --export     # GEE (4 exports in parallel) + tract means
    python pipeline/build_oxford_radios.py --territory chaco   # tract means from the downloaded tif
"""

import argparse
import os
import shutil
import sys

import pandas as pd
from shapely import wkb

sys.path.insert(0, os.path.dirname(__file__))
from config import OUTPUT_DIR, get_territory

OSM_QUERY = """(
  node[amenity=hospital];
  node[amenity=clinic];
  node[amenity=doctors];
  way[amenity=hospital];
  way[amenity=clinic];
)"""
MAX_DISTANCE_M = 4e5


MIRRORS = ['https://overpass.kumi.systems/api/interpreter', 'https://overpass-api.de/api/interpreter',
           'https://overpass.private.coffee/api/interpreter', 'https://maps.mail.ru/osm/tools/overpass/api/interpreter']
MIN_FACILITIES = 50


def facilities(t: str, box: list) -> list:
    """OSM health facilities inside the buffered box: every cached
    lv_healthcare_facilities*.json of the pipeline (Misiones, Corrientes, Paraguay…)
    plus a fresh Overpass query for this territory if it has no cache."""
    import glob
    import json
    import time
    import requests
    cache = os.path.join(OUTPUT_DIR if t == 'misiones' else os.path.join(OUTPUT_DIR, t), 'lv_healthcare_facilities.json')
    if not os.path.exists(cache):
        w, s, e, n = box
        query = f'[out:json][timeout:180][bbox:{s},{w},{n},{e}];' + chr(10) + OSM_QUERY + ';' + chr(10) + 'out center;'
        for attempt in range(3):
            for srv in MIRRORS:
                try:
                    r = requests.post(srv, data={'data': query}, timeout=200,
                                      headers={'User-Agent': 'spatia-pipeline/1.0 (spatia.ar)'})
                    r.raise_for_status()
                    pts = [[el['lon'], el['lat']] if el['type'] == 'node' else [el['center']['lon'], el['center']['lat']]
                           for el in r.json().get('elements', []) if el['type'] == 'node' or 'center' in el]
                    if pts:
                        with open(cache, 'w') as f:
                            json.dump(pts, f)
                        print(f'  {t}: fetched {len(pts)} facilities from {srv}', flush=True)
                        break
                except Exception as ex:
                    print(f'  Overpass {srv}: {str(ex)[:80]}', flush=True)
            if os.path.exists(cache):
                break
            time.sleep(60)
    pts = set()
    for f in glob.glob(os.path.join(OUTPUT_DIR, '**', 'lv_healthcare_facilities.json'), recursive=True):
        data = json.load(open(f))
        for p in (data if isinstance(data, list) else data.get('points', [])):
            pts.add((round(p[0], 5), round(p[1], 5)))
    w, s, e, n = box
    inside = [p for p in pts if w <= p[0] <= e and s <= p[1] <= n]
    if len(inside) < MIN_FACILITIES:
        sys.exit(f'{t}: only {len(inside)} OSM health facilities in the box — not computing')
    return inside


def health_image(t: str):
    import ee
    w, s, e, n = get_territory(t)['bbox']
    box = [w - 0.5, s - 0.5, e + 0.5, n + 0.5]   # facilities just across the border count
    region = ee.Geometry.Rectangle(box)
    pts = facilities(t, box)
    fric = ee.Image('projects/malariaatlasproject/assets/accessibility/friction_surface/2019_v5_1').select('friction')
    fc = ee.FeatureCollection([ee.Feature(ee.Geometry.Point(list(p))) for p in pts])
    src = ee.Image().toByte().paint(fc, 1).reproject(fric.projection()).unmask(0).clip(region)
    osm = fric.clip(region).cumulativeCost(source=src, maxDistance=MAX_DISTANCE_M).rename('osm')
    ox = (ee.Image('projects/malariaatlasproject/assets/accessibility/accessibility_to_healthcare/2019')
          .select('accessibility').rename('ox'))
    cities = ee.Image('Oxford/MAP/accessibility_to_cities_2015_v1_0').select('accessibility').rename('cities')
    print(f'  {t}: {len(pts)} OSM health facilities', flush=True)
    # Where the cost is undefined (beyond MAX_DISTANCE) Oxford stands alone.
    return ee.Image.cat([cities, ox, osm, ox.min(osm.unmask(ox)).rename('health')])


def export(tids: list) -> None:
    """One GEE export per territory (4 bands at the friction grid, ~1 km), in parallel."""
    import ee
    from gee_flood_detection import wait_for_tasks
    from config import GCS_BUCKET, EXPORT_PREFIX
    tasks = []
    for t in tids:
        img = health_image(t).toFloat()
        w, s, e, n = get_territory(t)['bbox']
        task = ee.batch.Export.image.toCloudStorage(
            image=img, description=f'health_time_{t}', bucket=GCS_BUCKET,
            fileNamePrefix=f'{EXPORT_PREFIX}/{t}/health_time', region=ee.Geometry.Rectangle([w, s, e, n]),
            crs='EPSG:4326', scale=928, maxPixels=1e10, fileFormat='GeoTIFF')
        task.start()
        tasks.append(task)
        print(f'  -> {t}: {task.id}', flush=True)
    wait_for_tasks(tasks, timeout=3 * 3600)
    from download_gcs import get_gcs_client
    bucket = get_gcs_client().bucket(GCS_BUCKET)
    for t in tids:
        for blob in bucket.list_blobs(prefix=f'{EXPORT_PREFIX}/{t}/health_time'):
            if blob.name.endswith('.tif'):
                blob.download_to_filename(tif_path(t))


def tif_path(t: str) -> str:
    return os.path.join(OUTPUT_DIR if t == 'misiones' else os.path.join(OUTPUT_DIR, t), 'health_time.tif')


def tract_means(t: str, radios: pd.DataFrame, geoms: list) -> pd.DataFrame:
    """Mean of each band over the tract's pixels; centroid pixel when none falls inside."""
    import numpy as np
    import rasterio
    from rasterio.features import geometry_mask
    from rasterio.windows import from_bounds
    rows = []
    with rasterio.open(tif_path(t)) as src:
        names = ['cities', 'ox', 'osm', 'health']
        for rc, g in zip(radios.redcode, geoms):
            vals, centroid = None, False
            try:
                win = from_bounds(*g.bounds, transform=src.transform).round_offsets().round_lengths()
                if win.width >= 1 and win.height >= 1:
                    a = src.read(window=win, masked=True).astype(float)
                    inside = ~geometry_mask([g], transform=src.window_transform(win), out_shape=a.shape[1:])
                    if inside.any():
                        vals = [float(np.ma.masked_array(a[i], mask=~inside | np.ma.getmaskarray(a[i])).mean()) for i in range(4)]
            except Exception:
                vals = None
            if vals is None or any(v != v for v in vals):
                c = g.representative_point()
                r, col = src.index(c.x, c.y)
                vals = [float(v) for v in src.read(window=((r, r + 1), (col, col + 1)))[:, 0, 0]]
                centroid = True
            rows.append({'redcode': rc, **dict(zip(names, vals)), '_centroid': centroid})
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--territory', choices=['misiones', 'corrientes', 'chaco', 'formosa'])
    ap.add_argument('--all', action='store_true')
    ap.add_argument('--export', action='store_true', help='Launch the GEE exports and download first')
    ap.add_argument('--validate', action='store_true', help='Compare against the values Misiones already uses')
    args = ap.parse_args()
    tids = ['misiones', 'corrientes', 'chaco', 'formosa'] if args.all else ['misiones' if args.validate else args.territory]
    if not tids[0]:
        sys.exit('--territory, --all or --validate')
    if args.export:
        from gee_flood_detection import authenticate
        authenticate()
        export(tids)
    for t in tids:
        process(t, args.validate)


def process(t: str, validate: bool) -> None:
    t_dir = os.path.join(OUTPUT_DIR, 'radio_data') if t == 'misiones' else os.path.join(OUTPUT_DIR, t)
    radio_dir = OUTPUT_DIR if t == 'misiones' else t_dir   # Misiones' tract polygons live in output/
    radios = pd.read_parquet(os.path.join(radio_dir, f'radios_{t}.parquet'), columns=['redcode', 'geometry'])
    geoms = [wkb.loads(g) for g in radios.geometry]
    rows = tract_means(t, radios, geoms)

    df = rows
    out = pd.DataFrame({
        'redcode': df.redcode,
        'accessibility_cities_min': df.cities.round(1),
        'accessibility_healthcare_min': df.health.round(1),
        'accessibility_healthcare_oxford': df.ox.round(1),
        'accessibility_healthcare_osm': df.osm.round(1),
        'accessibility_healthcare_walk': float('nan'),
    })
    n_centroid = int(df['_centroid'].fillna(False).sum()) if '_centroid' in df else 0
    missing = int(out.accessibility_healthcare_min.isna().sum())

    if validate:
        old = pd.read_parquet(os.path.join(t_dir, 'oxford_accessibility.parquet'))
        m = out.merge(old, on='redcode', suffixes=('', '_old'))
        for new_c, old_c in (('accessibility_healthcare_oxford', 'accessibility_healthcare_min_old'),
                             ('accessibility_cities_min', 'accessibility_cities_min_old')):
            print(f'{new_c}: n={len(m)} r={m[new_c].corr(m[old_c]):.3f} median new {m[new_c].median():.1f} '
                  f'old {m[old_c].median():.1f} mean |diff| {(m[new_c] - m[old_c]).abs().mean():.1f} min')
        return
    if missing > 0.02 * len(out):
        sys.exit(f'{t}: {missing} tracts without a value — not writing')

    if t == 'misiones':
        path = os.path.join(t_dir, 'oxford_accessibility.parquet')
        backup = path.replace('.parquet', '.pre-osm.parquet')
        prev = pd.read_parquet(path)
        keep = [c for c in prev.columns if c not in out.columns or c == 'redcode']
        out = prev[keep].merge(out, on='redcode', how='left')   # keeps friction_* columns
    else:
        path = os.path.join(t_dir, f'oxford_accessibility_{t}.parquet')
        backup = path.replace('.parquet', '.proxy.parquet')
    if os.path.exists(path) and not os.path.exists(backup):
        shutil.copy(path, backup)
    out.to_parquet(path, index=False)
    q = out.accessibility_healthcare_min.quantile([.05, .5, .95]).round(1).tolist()
    gain = int((out.accessibility_healthcare_osm < out.accessibility_healthcare_oxford - 5).sum())
    print(f'{t}: {len(out)} tracts ({n_centroid} by centroid, {missing} missing) · healthcare P5/P50/P95 {q} · '
          f'{gain} tracts > 5 min closer thanks to OSM · wrote {path}')


if __name__ == '__main__':
    main()
