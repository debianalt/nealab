"""
Build radios-v5.pmtiles: the 6.925 INDEC 2022 radios of Misiones, Corrientes, Chaco and Formosa.

Source: the same GeoParquets the census radio unit uses (output/radios_misiones.parquet and
output/<t>/radios_<t>.parquet), joined to output/<prefix>census_radios_2022.parquet
(build_census_radios.py) for dpto_key, so the web can filter the outlines by the open department.

Properties per feature: redcode, codprov, dpto_key.

Why v5 (4-nov-2026): radios-v4 (built elsewhere, served since jun-2026) and the old v3 builder indexed
each radio only in the tile that holds its centroid, so a radio was drawn only in that tile: at z12 the
large rural radios of Misiones showed in 1 of the 10–13 tiles they cross, and two Formosa radios of
Ramón Lista were missing altogether (their centroids fell north of the old -22.8° bbox edge). Here each
radio is indexed in every MAX_ZOOM tile its bbox covers, and the bbox reaches -22.3°.
v4 is left untouched on R2.

Usage:
  python pipeline/build_census_radios.py          # first: dpto_key
  python pipeline/rebuild_radios_tiles.py [--check]

Output: pipeline/output/radios-v5.pmtiles
R2 (after review):
  npx wrangler r2 object put neahub/data/tiles/radios-v5.pmtiles --file pipeline/output/radios-v5.pmtiles --remote
  then npm run purge-cdn and point config.ts (tiles.radios) to data/tiles/radios-v5.pmtiles.
"""

import argparse
import gzip
import math
import os
import sys
import time
from collections import defaultdict

import geopandas as gpd
import mapbox_vector_tile as mvt
import pandas as pd
from pmtiles.tile import TileType, zxy_to_tileid
from pmtiles.writer import Compression, Writer as PMTilesWriter
from shapely.geometry import box, mapping
from shapely.ops import clip_by_rect

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "radios-v5.pmtiles")

MIN_ZOOM = 5
MAX_ZOOM = 12
LAYER_NAME = "radios"
EXTENT = 4096
SIMPLIFY_TOLERANCE = 0.0005  # degrees, ~50 m; keeps polygons clean at zoom 12

# Misiones + Corrientes + Chaco + Formosa (radios: -63.42, -30.73, -53.64, -22.46). The old -22.8 north edge
# cut off the north of Ramón Lista (Formosa).
BBOX = {"west": -63.6, "south": -30.8, "east": -53.5, "north": -22.3}

SOURCES = {  # territory → (radio GeoParquet, census radio unit)
    'misiones': ('radios_misiones.parquet', 'census_radios_2022.parquet'),
    'corrientes': ('corrientes/radios_corrientes.parquet', 'corrientes/census_radios_2022.parquet'),
    'chaco': ('chaco/radios_chaco.parquet', 'chaco/census_radios_2022.parquet'),
    'formosa': ('formosa/radios_formosa.parquet', 'formosa/census_radios_2022.parquet'),
}


# ── Tile math ────────────────────────────────────────────────────────────

def lng_lat_to_tile(lng, lat, zoom):
    n = 2 ** zoom
    x = int((lng + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return max(0, min(n - 1, x)), max(0, min(n - 1, y))


def tile_bounds(x, y, z):
    n = 2 ** z
    west = x / n * 360.0 - 180.0
    east = (x + 1) / n * 360.0 - 180.0
    north = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    south = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return west, south, east, north


def get_tiles_for_bbox(bbox, zoom):
    x_min, y_min = lng_lat_to_tile(bbox["west"], bbox["north"], zoom)
    x_max, y_max = lng_lat_to_tile(bbox["east"], bbox["south"], zoom)
    return [(x, y) for x in range(x_min, x_max + 1) for y in range(y_min, y_max + 1)]


def features_to_mvt(features, tile_bounds_wsen, layer_name, extent=4096):
    w, s, e, n = tile_bounds_wsen
    buf = (e - w) * 0.01
    clipped = []
    for feat in features:
        try:
            c = clip_by_rect(feat["geometry"], w - buf, s - buf, e + buf, n + buf)
        except Exception:
            continue
        if c.is_empty:
            continue
        clipped.append({"geometry": c, "properties": feat["properties"]})
    if not clipped:
        return None
    layer = {
        "name": layer_name,
        "features": [{"geometry": mapping(f["geometry"]), "properties": f["properties"]} for f in clipped],
    }
    return mvt.encode([layer], quantize_bounds=(w, s, e, n), extents=extent)


# ── Step 1: radios ───────────────────────────────────────────────────────

def load_features():
    print("Step 1: radios from the GeoParquets…")
    features = []
    for t, (geo, unit) in SOURCES.items():
        g = gpd.read_parquet(os.path.join(OUTPUT_DIR, geo))[['redcode', 'geometry']]
        g['redcode'] = g.redcode.astype(str)
        keys = pd.read_parquet(os.path.join(OUTPUT_DIR, unit), columns=['redcode', 'dpto_key'])
        g = g.merge(keys, on='redcode', how='left')
        assert g.dpto_key.notna().all(), f'{t}: radios sin dpto_key'
        for redcode, geom, key in zip(g.redcode, g.geometry, g.dpto_key):
            if geom is None or geom.is_empty:
                continue
            simp = geom.simplify(SIMPLIFY_TOLERANCE, preserve_topology=True)
            features.append({"geometry": simp if not simp.is_empty else geom,
                             "properties": {"redcode": redcode, "codprov": redcode[:2], "dpto_key": key}})
        print(f"  {t}: {len(g):,} radios")
    print(f"  {len(features):,} features")
    return features


# ── Step 2: PMTiles ──────────────────────────────────────────────────────

def generate_pmtiles(features):
    print(f"\nStep 2: PMTiles (zoom {MIN_ZOOM}-{MAX_ZOOM})…")
    t0 = time.time()
    # Each radio in every MAX_ZOOM grid cell its bbox covers (not only its centroid's).
    grid = defaultdict(list)
    for i, feat in enumerate(features):
        w, s, e, n = feat["geometry"].bounds
        x0, y0 = lng_lat_to_tile(w, n, MAX_ZOOM)
        x1, y1 = lng_lat_to_tile(e, s, MAX_ZOOM)
        for gx in range(x0, x1 + 1):
            for gy in range(y0, y1 + 1):
                grid[(gx, gy)].append(i)
    print(f"  index: {len(grid):,} grid cells")

    tile_data = {}
    for z in range(MIN_ZOOM, MAX_ZOOM + 1):
        written, t_z = 0, time.time()
        scale = 2 ** (MAX_ZOOM - z)
        for x, y in get_tiles_for_bbox(BBOX, z):
            cand = set()
            for gx in range(x * scale, (x + 1) * scale):
                for gy in range(y * scale, (y + 1) * scale):
                    cand.update(grid.get((gx, gy), ()))
            if not cand:
                continue
            tb = features_to_mvt([features[i] for i in cand], tile_bounds(x, y, z), LAYER_NAME, EXTENT)
            if tb:
                tile_data[zxy_to_tileid(z, x, y)] = gzip.compress(tb)
                written += 1
        print(f"  z{z}: {written} tiles ({time.time() - t_z:.1f}s)")

    with open(OUTPUT_PATH, "wb") as f:
        writer = PMTilesWriter(f)
        for tile_id in sorted(tile_data):
            writer.write_tile(tile_id, tile_data[tile_id])
        header = {
            "tile_type": TileType.MVT, "tile_compression": Compression.GZIP,
            "min_zoom": MIN_ZOOM, "max_zoom": MAX_ZOOM,
            "min_lon": BBOX["west"], "min_lat": BBOX["south"], "max_lon": BBOX["east"], "max_lat": BBOX["north"],
            "center_lon": (BBOX["west"] + BBOX["east"]) / 2, "center_lat": (BBOX["south"] + BBOX["north"]) / 2,
            "center_zoom": 8,
        }
        metadata = {
            "name": "radios",
            "description": "INDEC Censo 2022 radios, Misiones + Corrientes + Chaco + Formosa (AR)",
            "vector_layers": [{"id": LAYER_NAME,
                               "fields": {"redcode": "String", "codprov": "String", "dpto_key": "String"},
                               "minzoom": MIN_ZOOM, "maxzoom": MAX_ZOOM}],
        }
        writer.finalize(header, metadata)
    print(f"  -> {OUTPUT_PATH} ({os.path.getsize(OUTPUT_PATH) / 1e6:.1f} MB, {time.time() - t0:.0f}s)")


def check(features):
    """Every radio is in every z12 tile it intersects."""
    from pmtiles.reader import MmapSource, Reader, all_tiles
    seen = defaultdict(set)
    with open(OUTPUT_PATH, 'rb') as f:
        r = Reader(MmapSource(f))
        for (z, x, y), data in all_tiles(r.get_bytes):
            if z == MAX_ZOOM:
                for ft in mvt.decode(gzip.decompress(data))[LAYER_NAME]['features']:
                    seen[ft['properties']['redcode']].add((x, y))
    falta, total = 0, 0
    for feat in features:
        w, s, e, n = feat["geometry"].bounds
        x0, y0 = lng_lat_to_tile(w, n, MAX_ZOOM)
        x1, y1 = lng_lat_to_tile(e, s, MAX_ZOOM)
        need = {(x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1)
                if feat["geometry"].intersects(box(*tile_bounds(x, y, MAX_ZOOM)))}
        total += len(need)
        falta += len(need - seen[feat["properties"]["redcode"]])
    print(f"  check z{MAX_ZOOM}: {len(seen):,} radios en las teselas; pares radio×tesela {total:,}, faltan {falta}")
    if falta or len(seen) != len(features):
        sys.exit('radios-v5 incompleto')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--check', action='store_true')
    a = ap.parse_args()
    t = time.time()
    features = load_features()
    generate_pmtiles(features)
    if a.check:
        check(features)
    print(f"\nDone in {time.time() - t:.0f}s")


if __name__ == "__main__":
    main()
