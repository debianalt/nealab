"""
Compute accessibility for a territory -> sat_accessibility.parquet.

Every column is a travel time by road, in minutes (decided with the author,
5-nov-2026: straight-line km confused users and missed rivers, relief and dirt roads).
Physical values only: no percentiles, normalisation, composite score or types.

Output columns (minutes):
  travel_min_capital_raw     to the provincial / departmental capital
  travel_min_cabecera_raw    to the nearest city of 50,000+ (Oxford MAP
                             accessibility_to_cities_2015 raster, sampled at the centre)
  travel_min_hospital_raw    to the nearest public hospital (IGN/SISA)             AR only
  travel_min_salud_raw       to the nearest public health establishment (IGN/SISA) AR only
  travel_min_secundaria_raw  to the nearest school with common or technical
                             secondary (Mapa Educativo Nacional, DIEE)             AR only
  travel_min_ruta_raw        to the nearest trunk road (OSM motorway/trunk/primary/secondary)

Method (all but cabecera): least-cost travel time over the Oxford/MAP motorised
friction surface 2019 v5.1 (min/m, 30" ~ 0.93 km), one raster for the whole NEA
(+0.5 deg, `gee_export_location_value.py --territory nea`) so destinations across a
provincial boundary count. The friction is reprojected (nearest) to a Lambert
azimuthal equal-area grid of 500 m centred on the NEA, so steps are metric in both
axes (the old single-source code took 1 deg = 111 km east-west too, ~12 % long at 27 S),
and MCP_Geometric runs from all destinations at once. Each cell reads the pixel of its
centre. Paraguayan territories: hospital, salud and secundaria = NaN (AR registries).

Inputs: pipeline/output/nea/lv_friction_nea.tif, pipeline/output/access_sources/
(`fetch_access_facilities.py`), pipeline/output/nea/lv_road_coords_nea.json (Overpass),
and per territory lv_cities_access.tif + hexagons(-lite).geojson.

Usage:
  python pipeline/compute_accessibility_h3.py --territory misiones
  python pipeline/compute_accessibility_h3.py --territory itapua_py --output-dir <dir>
"""

import argparse
import json
import os
import platform
import subprocess
import sys
import time

import numpy as np
import pandas as pd
import rasterio
import requests
from pyproj import Transformer
from rasterio.warp import Resampling, calculate_default_transform, reproject

from config import OUTPUT_DIR, GCS_BUCKET, get_territory
from fetch_access_facilities import NEA_BOX

_GCLOUD = 'gcloud.cmd' if platform.system() == 'Windows' else 'gcloud'

# overpass-api.de answers 406 to the default python-requests User-Agent (3-nov-2026).
OVERPASS_HEADERS = {'User-Agent': 'spatia-pipeline/1.0 (+https://www.spatia.ar)'}

# Metric grid for the cost surface: Lambert azimuthal equal-area centred on the NEA box.
LAEA = ('+proj=laea +lat_0={:.2f} +lon_0={:.2f} +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs'
        .format((NEA_BOX[1] + NEA_BOX[3]) / 2, (NEA_BOX[0] + NEA_BOX[2]) / 2))
GRID_M = 500           # finer than the 0.93 km source, so nearest keeps every road pixel
BARRIER = 1e8          # min/m for nodata pixels: impassable
DESTINATIONS_AR = ('hospital', 'salud', 'secundaria')

CAPITAL_COORDS = {
    'misiones':              (-27.367,  -55.896),   # Posadas
    'itapua_py':             (-27.336,  -55.869),   # Encarnación
    'corrientes':            (-27.4676, -58.8341),  # Corrientes capital
    'alto_parana_py':        (-25.5097, -54.6111),  # Ciudad del Este
    'chaco':                 (-27.4513, -58.9920),  # Resistencia
    'formosa':               (-26.1817, -58.1730),  # Formosa capital
    'parana_br':             (-25.4290, -49.2671),  # Curitiba
    'santa_catarina_br':     (-27.5954, -48.5480),  # Florianópolis
    'rio_grande_sul_br':     (-30.0346, -51.2177),  # Porto Alegre
    # New PY departments — capital city of each department. Nine were misplaced
    # by 20–181 km (Villa Hayes, Salto del Guairá, Fuerte Olimpo, Filadelfia,
    # Coronel Oviedo, San Juan Bautista, Pilar, Areguá across the river, Caacupé);
    # corrected 1-nov-2026 against OSM town nodes.
    'concepcion_py':         (-23.4063, -57.4340),  # Concepción
    'san_pedro_py':          (-24.1000, -57.0833),  # San Pedro de Ycuamandiyú
    'cordillera_py':         (-25.3857, -57.1405),  # Caacupé
    'guaira_py':             (-25.7667, -56.4333),  # Villarrica
    'caaguazu_py':           (-25.4501, -56.4407),  # Coronel Oviedo
    'caazapa_py':            (-26.1833, -56.3667),  # Caazapá
    'misiones_py':           (-26.6690, -57.1456),  # San Juan Bautista
    'paraguari_py':          (-25.6250, -57.1500),  # Paraguarí
    'central_py':            (-25.3129, -57.3847),  # Areguá (capital departamental)
    'neembucu_py':           (-26.8583, -58.3034),  # Pilar
    'amambay_py':            (-22.5500, -55.7333),  # Pedro Juan Caballero
    'canindeyu_py':          (-24.0617, -54.3056),  # Salto del Guairá
    'presidente_hayes_py':   (-25.0929, -57.5246),  # Villa Hayes
    'boqueron_py':           (-22.3525, -60.0352),  # Filadelfia
    'alto_paraguay_py':      (-21.0385, -57.8748),  # Fuerte Olimpo
}

# -- Helpers ------------------------------------------------------------------

def is_valid_raster(path: str) -> bool:
    if not os.path.exists(path):
        return False
    try:
        with rasterio.open(path) as src:
            data = src.read(1)
            nd = src.nodata
            valid = np.sum(data != nd) if nd is not None else np.sum(np.isfinite(data))
            return int(valid) > 0
    except Exception:
        return False


def download_from_gcs(bucket: str, gcs_key: str, local_path: str) -> bool:
    r = subprocess.run(
        [_GCLOUD, 'storage', 'cp', f'gs://{bucket}/{gcs_key}', local_path],
        capture_output=True, text=True
    )
    if r.returncode != 0:
        print(f"  WARNING: GCS download failed for {gcs_key}: {r.stderr.strip()}")
    return r.returncode == 0


def h3_centroid(h3index: str) -> tuple[float, float]:
    try:
        from h3 import cell_to_latlng
        return cell_to_latlng(h3index)
    except ImportError:
        import h3
        return h3.h3_to_geo(h3index)


def sample_raster_at_centroids(raster_path: str, lngs: list, lats: list,
                                nodata_val=None) -> np.ndarray:
    with rasterio.open(raster_path) as src:
        nd = nodata_val if nodata_val is not None else src.nodata
        coords = list(zip(lngs, lats))
        vals = np.array([v[0] for v in src.sample(coords)], dtype=np.float64)
        if nd is not None:
            vals[vals == nd] = np.nan
        # rasterio fills points outside the raster with 0 when it declares no
        # nodata: a cell beyond the export read "0 min to a city" (fixed 1-nov-2026).
        b = src.bounds
        lng_a, lat_a = np.asarray(lngs), np.asarray(lats)
        outside = (lng_a < b.left) | (lng_a > b.right) | (lat_a < b.bottom) | (lat_a > b.top)
        vals[outside] = np.nan
    return vals


def _overpass_roads(box) -> list:
    west, south, east, north = box
    query = f"""[out:json][timeout:180][bbox:{south},{west},{north},{east}];
(
  way[highway=motorway];
  way[highway=trunk];
  way[highway=primary];
  way[highway=secondary];
);
out geom;"""
    for attempt in range(3):
        for srv in ['https://overpass-api.de/api/interpreter',
                    'https://overpass.kumi.systems/api/interpreter']:
            try:
                r = requests.post(srv, data=query, timeout=240, headers=OVERPASS_HEADERS)
                r.raise_for_status()
                return [[nd['lon'], nd['lat']] for el in r.json().get('elements', [])
                        if el['type'] == 'way' and 'geometry' in el for nd in el['geometry']]
            except Exception as e:
                print(f"    Overpass {srv} (try {attempt + 1}): {e}")
        time.sleep(20)
    sys.exit(f"Overpass failed for box {box}")


def fetch_osm_road_coords(bbox: list, cache_path: str, tile_deg: float = 2.5) -> list:
    """Trunk-road vertices (motorway/trunk/primary/secondary) in bbox, cached.

    The box is queried in tiles of `tile_deg`: the whole NEA in one request times out
    (5-nov-2026). A tile with no trunk roads returns an empty list, which is fine.
    """
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            data = json.load(f)
        pts = data if isinstance(data, list) else data.get('road_points', [])
        print(f"  Loaded {len(pts):,} road points from cache")
        return pts

    west, south, east, north = bbox
    xs = list(np.arange(west, east, tile_deg)) + [east]
    ys = list(np.arange(south, north, tile_deg)) + [north]
    seen, pts = set(), []
    for x0, x1 in zip(xs[:-1], xs[1:]):
        for y0, y1 in zip(ys[:-1], ys[1:]):
            tile = _overpass_roads((x0, y0, x1, y1))
            for lon, lat in tile:
                key = (round(lon, 6), round(lat, 6))
                if key not in seen:
                    seen.add(key)
                    pts.append([lon, lat])
            print(f"    tile {x0:.2f},{y0:.2f}: {len(tile):,} vertices")
            time.sleep(2)
    if not pts:
        sys.exit(f"No OSM road points for {os.path.basename(cache_path)}")
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, 'w') as f:
        json.dump({'road_points': pts, 'bbox': list(bbox)}, f)
    print(f"  Fetched {len(pts):,} road points from Overpass")
    return pts

# -- Least-cost travel time ---------------------------------------------------

class CostSurface:
    """Friction (min/m) on a metric LAEA grid, with MCP from any set of points."""

    def __init__(self, friction_path: str):
        with rasterio.open(friction_path) as src:
            transform, width, height = calculate_default_transform(
                src.crs, LAEA, src.width, src.height, *src.bounds, resolution=GRID_M)
            dst = np.full((height, width), np.nan, dtype=np.float64)
            reproject(source=rasterio.band(src, 1), destination=dst,
                      src_transform=src.transform, src_crs=src.crs,
                      dst_transform=transform, dst_crs=LAEA,
                      resampling=Resampling.nearest, dst_nodata=np.nan)
        bad = ~np.isfinite(dst) | (dst <= 0)
        dst[bad] = BARRIER
        self.cost = dst
        self.transform = transform
        self.to_xy = Transformer.from_crs('EPSG:4326', LAEA, always_xy=True)
        print(f"  Cost grid {width}x{height} @ {GRID_M} m, {bad.mean():.1%} barrier")

    def pixels(self, lngs, lats):
        x, y = self.to_xy.transform(np.asarray(lngs, float), np.asarray(lats, float))
        col, row = ~self.transform * (np.asarray(x), np.asarray(y))
        r, c = np.floor(row).astype(int), np.floor(col).astype(int)
        ok = (r >= 0) & (r < self.cost.shape[0]) & (c >= 0) & (c < self.cost.shape[1])
        return r, c, ok

    def travel_minutes(self, src_lngs, src_lats, lngs, lats) -> np.ndarray:
        """Minutes from each (lng, lat) to the nearest source, by least cost."""
        from skimage.graph import MCP_Geometric
        sr, sc, sok = self.pixels(src_lngs, src_lats)
        starts = sorted({(int(a), int(b)) for a, b, k in zip(sr, sc, sok)
                         if k and self.cost[a, b] < BARRIER})
        if not starts:
            return np.full(len(lats), np.nan)
        mcp = MCP_Geometric(self.cost, sampling=(GRID_M, GRID_M))
        cumulative, _ = mcp.find_costs(starts)
        r, c, ok = self.pixels(lngs, lats)
        out = np.full(len(lats), np.nan)
        vals = cumulative[r[ok], c[ok]]
        vals[vals >= 1e7] = np.nan
        out[ok] = np.round(vals, 1)
        return out


def load_points(path: str):
    with open(path, encoding='utf-8') as f:
        feats = json.load(f)['features']
    pts = np.array([ft['geometry']['coordinates'][:2] for ft in feats], dtype=float)
    return pts[:, 0], pts[:, 1]


# -- Main ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Compute accessibility H3 parquet (minutes by road)")
    parser.add_argument("--territory", default="misiones")
    parser.add_argument("--output-dir", default=OUTPUT_DIR,
                        help="pipeline output root (default: this checkout's pipeline/output)")
    args = parser.parse_args()

    territory = get_territory(args.territory)
    out_prefix = territory['output_prefix'].rstrip('/')
    root = args.output_dir
    out_dir = os.path.join(root, out_prefix) if out_prefix else root
    gcs_subdir = '' if args.territory == 'misiones' else f"{args.territory}/"
    is_py = args.territory.endswith('_py')
    print(f"Territory: {args.territory}  |  Output: {out_dir}")

    friction_path = os.path.join(root, 'nea', 'lv_friction_nea.tif')
    if not is_valid_raster(friction_path):
        download_from_gcs(GCS_BUCKET, 'satellite/nea/lv_friction_nea.tif', friction_path)
    if not is_valid_raster(friction_path):
        print("ERROR: lv_friction_nea.tif missing. Run gee_export_location_value.py --territory nea.")
        return 1
    cities_path = os.path.join(out_dir, 'lv_cities_access.tif')
    if not is_valid_raster(cities_path):
        download_from_gcs(GCS_BUCKET, f'satellite/{gcs_subdir}lv_cities_access.tif', cities_path)
    if not is_valid_raster(cities_path):
        print("ERROR: lv_cities_access.tif not available. Run gee_export_location_value.py first.")
        return 1
    sources_dir = os.path.join(root, 'access_sources')
    if not is_py and not os.path.exists(os.path.join(sources_dir, 'manifest.json')):
        print("ERROR: access_sources missing. Run fetch_access_facilities.py first.")
        return 1

    # -- H3 grid --
    hex_path = os.path.join(out_dir, 'hexagons-lite.geojson')
    if not os.path.exists(hex_path):
        hex_path = os.path.join(out_dir, 'hexagons.geojson')
    with open(hex_path) as f:
        features = json.load(f)['features']
    h3ids = [ft['properties']['h3index'] for ft in features]
    latlng = np.array([h3_centroid(h) for h in h3ids])
    lats, lngs = latlng[:, 0], latlng[:, 1]
    n = len(h3ids)
    print(f"  {n:,} hexagons ({hex_path})")

    surface = CostSurface(friction_path)
    cols = {}

    def run(name, src_lngs, src_lats):
        t0 = time.time()
        v = surface.travel_minutes(src_lngs, src_lats, lngs, lats)
        print(f"  {name}: {len(src_lngs):,} destinations, valid {np.isfinite(v).sum():,}/{n:,}, "
              f"median {np.nanmedian(v):.0f} min ({time.time() - t0:.0f} s)")
        return v

    cap_lat, cap_lng = CAPITAL_COORDS[args.territory]
    cols['travel_min_capital_raw'] = run('capital', [cap_lng], [cap_lat])

    cab = sample_raster_at_centroids(cities_path, lngs, lats)
    cab[cab < 0] = np.nan       # 0 = inside a city of 50k+ (Oxford): a real value
    cols['travel_min_cabecera_raw'] = cab

    for dest in DESTINATIONS_AR:
        col = f'travel_min_{dest}_raw'
        if is_py:
            cols[col] = np.full(n, np.nan)
            continue
        s_lng, s_lat = load_points(os.path.join(sources_dir, f'{dest}.geojson'))
        cols[col] = run(dest, s_lng, s_lat)

    road = np.array(fetch_osm_road_coords(list(NEA_BOX),
                                          os.path.join(root, 'nea', 'lv_road_coords_nea.json')))
    cols['travel_min_ruta_raw'] = run('ruta', road[:, 0], road[:, 1])

    if not is_py:
        bad = int(np.sum(cols['travel_min_hospital_raw'] < cols['travel_min_salud_raw']))
        if bad:
            sys.exit(f"hospital < salud in {bad} cells: hospitals are a subset of salud")

    out = pd.DataFrame({'h3index': h3ids, **cols})
    out_path = os.path.join(out_dir, 'sat_accessibility.parquet')
    out.to_parquet(out_path, index=False)
    print(f"  Saved {out_path} ({len(out):,} rows, {os.path.getsize(out_path) / 1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
