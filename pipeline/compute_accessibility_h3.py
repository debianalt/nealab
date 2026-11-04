"""
Compute accessibility metrics for a territory → sat_accessibility.parquet.

Same sources (Oxford MAP + OSM) in every territory. Physical values only: no
percentiles, fixed-scale normalisation, composite score or types (removed 4-nov-2026).

Output columns (minutes or km):
  travel_min_capital_raw           MCP travel time to capital city (Oxford MAP friction + MCP_Geometric)
  travel_min_cabecera_raw          Oxford MAP accessibility_to_cities raster
  dist_nearest_hospital_km_raw     OSM hospital/clinic KDTree
  dist_nearest_secundaria_km_raw   OSM school KDTree
  dist_primary_m_raw               OSM road KDTree (motorway/trunk/primary/secondary), stored in km

Usage:
  python pipeline/compute_accessibility_h3.py --territory misiones
  python pipeline/compute_accessibility_h3.py --territory itapua_py
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
from scipy.spatial import cKDTree

from config import OUTPUT_DIR, GCS_BUCKET, get_territory

_GCLOUD = 'gcloud.cmd' if platform.system() == 'Windows' else 'gcloud'

# overpass-api.de answers 406 to the default python-requests User-Agent (3-nov-2026):
# with no cache the hospital/road distances came out all-NaN and were scored as neutral.
OVERPASS_HEADERS = {'User-Agent': 'spatia-pipeline/1.0 (+https://www.spatia.ar)'}

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

# ── Helpers ───────────────────────────────────────────────────────────────────

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


def compute_mcp_travel_time(friction_path: str, source_lat: float, source_lng: float,
                             lats: list, lngs: list) -> np.ndarray:
    """MCP least-cost travel time from a single source point (capital city)."""
    try:
        from skimage.graph import MCP_Geometric
    except ImportError:
        raise ImportError("scikit-image required: pip install scikit-image")

    with rasterio.open(friction_path) as src:
        friction = src.read(1).astype(np.float64)
        transform = src.transform
        nd = src.nodata

        pixel_w_m = abs(transform.a) * 111_000
        pixel_h_m = abs(transform.e) * 111_000
        pixel_size_m = (pixel_w_m + pixel_h_m) / 2

        if nd is not None:
            friction[friction == nd] = 1e8
        friction[~np.isfinite(friction) | (friction < 0)] = 1e8

        cost_surface = friction * pixel_size_m

        col_f, row_f = ~transform * (source_lng, source_lat)
        r0, c0 = int(row_f), int(col_f)
        if not (0 <= r0 < cost_surface.shape[0] and 0 <= c0 < cost_surface.shape[1]):
            print(f"  WARNING: capital coords ({source_lat},{source_lng}) outside raster extent")
            return np.full(len(lats), np.nan)

        print(f"  MCP from capital pixel ({r0},{c0})")
        mcp = MCP_Geometric(cost_surface)
        cumulative, _ = mcp.find_costs([(r0, c0)])

        result = np.full(len(lats), np.nan)
        for i, (lat, lng) in enumerate(zip(lats, lngs)):
            col_f, row_f = ~transform * (lng, lat)
            r, c = int(row_f), int(col_f)
            if 0 <= r < cumulative.shape[0] and 0 <= c < cumulative.shape[1]:
                val = cumulative[r, c]
                if val < 1e7:
                    result[i] = round(float(val), 2)
        return result


def fetch_osm_points(bbox: list, amenity_query: str, cache_path: str) -> list:
    """Fetch OSM node/way centroids for a query, return [[lon,lat],...] and cache."""
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            pts = json.load(f)
        print(f"  Loaded {len(pts):,} points from cache: {os.path.basename(cache_path)}")
        return pts

    west, south, east, north = bbox
    q_bbox = f"{south},{west},{north},{east}"
    query = f"[out:json][timeout:120][bbox:{q_bbox}];\n{amenity_query};\nout center;"

    pts = []
    for srv in ['https://overpass.kumi.systems/api/interpreter',
                'https://overpass-api.de/api/interpreter']:
        try:
            r = requests.post(srv, data=query, timeout=150, headers=OVERPASS_HEADERS)
            r.raise_for_status()
            for el in r.json().get('elements', []):
                if el['type'] == 'node':
                    pts.append([el['lon'], el['lat']])
                elif el['type'] == 'way' and 'center' in el:
                    pts.append([el['center']['lon'], el['center']['lat']])
            print(f"  Fetched {len(pts):,} points from Overpass")
            break
        except Exception as e:
            print(f"  Overpass {srv}: {e}")

    if not pts:
        # An empty list scores every cell as NaN → neutral; stop instead.
        sys.exit(f"No OSM points for {os.path.basename(cache_path)} (Overpass failed or empty)")
    with open(cache_path, 'w') as f:
        json.dump(pts, f)
    return pts


def fetch_osm_road_coords(bbox: list, cache_path: str) -> list:
    """Fetch road geometry coords from Overpass, return [[lon,lat],...] and cache."""
    if os.path.exists(cache_path):
        with open(cache_path) as f:
            data = json.load(f)
        pts = data if isinstance(data, list) else data.get('road_points', [])
        print(f"  Loaded {len(pts):,} road points from cache")
        return pts

    west, south, east, north = bbox
    q_bbox = f"{south},{west},{north},{east}"
    query = f"""[out:json][timeout:120][bbox:{q_bbox}];
(
  way[highway=motorway];
  way[highway=trunk];
  way[highway=primary];
  way[highway=secondary];
);
out geom;"""

    pts = []
    for srv in ['https://overpass.kumi.systems/api/interpreter',
                'https://overpass-api.de/api/interpreter']:
        try:
            r = requests.post(srv, data=query, timeout=150, headers=OVERPASS_HEADERS)
            r.raise_for_status()
            for el in r.json().get('elements', []):
                if el['type'] == 'way' and 'geometry' in el:
                    pts.extend([[nd['lon'], nd['lat']] for nd in el['geometry']])
            print(f"  Fetched {len(pts):,} road points from Overpass")
            break
        except Exception as e:
            print(f"  Overpass {srv}: {e}")

    if not pts:
        sys.exit(f"No OSM road points for {os.path.basename(cache_path)} (Overpass failed or empty)")
    with open(cache_path, 'w') as f:
        json.dump({'road_points': pts}, f)
    return pts


def kdtree_distances_km(query_lats: list, query_lngs: list,
                         point_coords: list) -> np.ndarray:
    """Compute distances to nearest point in km using scaled Cartesian KDTree."""
    if not point_coords:
        return np.full(len(query_lats), np.nan)
    mean_lat = float(np.mean(query_lats))
    cos_lat = np.cos(np.radians(mean_lat))
    pts = np.array(point_coords)
    pts_xy = np.column_stack([pts[:, 0] * cos_lat, pts[:, 1]])
    q_xy = np.column_stack([np.array(query_lngs) * cos_lat, np.array(query_lats)])
    tree = cKDTree(pts_xy)
    dists_deg, _ = tree.query(q_xy)
    return (dists_deg * 111_000.0 / 1000.0).astype(np.float64)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compute accessibility H3 parquet")
    parser.add_argument("--territory", default="misiones")
    args = parser.parse_args()

    territory = get_territory(args.territory)
    bbox = territory['bbox']
    out_prefix = territory['output_prefix'].rstrip('/')
    out_dir = os.path.join(OUTPUT_DIR, out_prefix) if out_prefix else OUTPUT_DIR
    gcs_subdir = '' if args.territory == 'misiones' else f"{args.territory}/"

    os.makedirs(out_dir, exist_ok=True)
    print(f"Territory: {args.territory}  |  Output: {out_dir}")

    # ── Rasters ──────────────────────────────────────────────────────────────
    friction_path = os.path.join(out_dir, 'lv_friction.tif')
    cities_path = os.path.join(out_dir, 'lv_cities_access.tif')

    for fname, path in [('lv_friction.tif', friction_path),
                         ('lv_cities_access.tif', cities_path)]:
        if not is_valid_raster(path):
            print(f"  Downloading {fname} from GCS...")
            download_from_gcs(GCS_BUCKET, f'satellite/{gcs_subdir}{fname}', path)

    if not is_valid_raster(friction_path):
        print("ERROR: lv_friction.tif not available. Run gee_export_location_value.py first.")
        return 1
    if not is_valid_raster(cities_path):
        print("ERROR: lv_cities_access.tif not available. Run gee_export_location_value.py first.")
        return 1

    # ── H3 grid ───────────────────────────────────────────────────────────────
    hex_path = os.path.join(out_dir, 'hexagons-lite.geojson')
    if not os.path.exists(hex_path):
        hex_path = os.path.join(out_dir, 'hexagons.geojson')
    print(f"Loading grid: {hex_path}")
    with open(hex_path) as f:
        features = json.load(f)['features']
    n = len(features)
    print(f"  {n:,} hexagons")

    h3ids, lats, lngs = [], [], []
    for feat in features:
        h3id = feat['properties']['h3index']
        lat, lng = h3_centroid(h3id)
        h3ids.append(h3id)
        lats.append(lat)
        lngs.append(lng)

    # ── Travel time to capital (MCP) ─────────────────────────────────────────
    cap_lat, cap_lng = CAPITAL_COORDS[args.territory]
    print(f"\nComputing MCP travel time to capital ({cap_lat:.3f},{cap_lng:.3f})...")
    t0 = time.time()
    travel_capital_raw = compute_mcp_travel_time(friction_path, cap_lat, cap_lng, lats, lngs)
    valid = np.sum(np.isfinite(travel_capital_raw))
    print(f"  valid={valid:,}/{n:,}  mean={np.nanmean(travel_capital_raw):.1f} min  "
          f"({time.time()-t0:.0f}s)")

    # ── Travel time to nearest city (cities_access raster) ───────────────────
    print("\nSampling cities_access raster...")
    travel_cabecera_raw = sample_raster_at_centroids(cities_path, lngs, lats)
    # 0 = inside a city of 50k+ (Oxford); cells without a reading come as NaN.
    # `<= 0` used to drop the city cores themselves (Posadas, Gran Asunción).
    travel_cabecera_raw[travel_cabecera_raw < 0] = np.nan
    valid = np.sum(np.isfinite(travel_cabecera_raw))
    print(f"  valid={valid:,}/{n:,}  mean={np.nanmean(travel_cabecera_raw):.1f} min")

    # ── Hospital distances (OSM KDTree) ───────────────────────────────────────
    print("\nFetching hospital coords...")
    hosp_cache = os.path.join(out_dir, 'lv_healthcare_facilities.json')
    hosp_query = """(
  node[amenity=hospital];
  node[amenity=clinic];
  node[amenity=doctors];
  way[amenity=hospital];
  way[amenity=clinic];
)"""
    hosp_pts = fetch_osm_points(bbox, hosp_query, hosp_cache)
    dist_hospital_raw = kdtree_distances_km(lats, lngs, hosp_pts)
    print(f"  mean={np.nanmean(dist_hospital_raw):.2f} km")

    # ── School distances (OSM KDTree) ─────────────────────────────────────────
    print("\nFetching school coords...")
    school_cache = os.path.join(out_dir, 'access_school_coords.json')
    school_query = """(
  node[amenity=school];
  node[amenity=college];
  way[amenity=school];
  way[amenity=college];
)"""
    school_pts = fetch_osm_points(bbox, school_query, school_cache)
    dist_school_raw = kdtree_distances_km(lats, lngs, school_pts)
    print(f"  mean={np.nanmean(dist_school_raw):.2f} km")

    # ── Road distances (OSM KDTree) ───────────────────────────────────────────
    print("\nFetching road coords...")
    road_cache = os.path.join(out_dir, 'lv_road_coords.json')
    road_pts = fetch_osm_road_coords(bbox, road_cache)
    dist_road_raw = kdtree_distances_km(lats, lngs, road_pts)
    print(f"  mean={np.nanmean(dist_road_raw):.2f} km")

    # ── Assemble output: physical values only ─────────────────────────────────
    out = pd.DataFrame({
        'h3index': h3ids,
        'travel_min_capital_raw':         travel_capital_raw,
        'travel_min_cabecera_raw':        travel_cabecera_raw,
        'dist_nearest_hospital_km_raw':   dist_hospital_raw,
        'dist_nearest_secundaria_km_raw': dist_school_raw,
        'dist_primary_m_raw':             dist_road_raw,
    })

    out_path = os.path.join(out_dir, 'sat_accessibility.parquet')
    out.to_parquet(out_path, index=False)
    size_mb = os.path.getsize(out_path) / 1024 / 1024

    print(f"\n{'='*60}")
    print(f"  Rows: {len(out):,}")
    print(f"  Saved: {out_path} ({size_mb:.1f} MB)")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
