"""
Build footprints-only buildings PMTiles for a Brazilian state from
gba_buildings_<territory>. NO census/population — Brazil's IBGE setor data is
not yet loaded (see pipeline/data/brazil_census_design.md). Buildings carry
only geometry + modeled height + area; est_personas is added in a later phase
once IBGE setores censitários are integrated.

Reuses the tile math + PMTiles writer from build_ar_buildings.py.

Pre-requisite:
  python pipeline/ingest_gba.py --territory parana_br   (footprints only, no join)

Usage:
  python pipeline/build_br_buildings.py --territory parana_br
  python pipeline/build_br_buildings.py --territory santa_catarina_br
  python pipeline/build_br_buildings.py --territory rio_grande_sul_br

Output:
  pipeline/output/<territory>_buildings.pmtiles
"""
import argparse
import gzip
import os
import sys
import time
from collections import defaultdict

import psycopg2
from pmtiles.tile import TileType, zxy_to_tileid
from pmtiles.writer import Compression, Writer as PMTilesWriter
from shapely import wkb

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, get_territory
from build_ar_buildings import (
    MIN_ZOOM, MAX_ZOOM, LAYER_NAME, EXTENT, PG_BUILDINGS,
    lng_lat_to_tile, tile_bounds, get_tiles_for_bbox, features_to_mvt,
)


def load_buildings(table: str) -> list:
    print(f"\nLoading buildings from {table} (footprints only)...")
    features = []
    with psycopg2.connect(PG_BUILDINGS) as conn:
        with conn.cursor(name="br_bldg_cursor") as cur:
            cur.itersize = 50000
            cur.execute(f"""
                SELECT ST_AsBinary(geom), best_height_m, area_m2
                FROM {table}
                WHERE geom IS NOT NULL
            """)
            count = 0
            for row in cur:
                geom_wkb, height, area = row
                try:
                    geom = wkb.loads(bytes(geom_wkb))
                except Exception:
                    continue
                features.append({
                    "geometry": geom,
                    "properties": {
                        "best_height_m": round(float(height), 1) if height else 5.0,
                        "area_m2":       round(float(area), 0) if area else 0,
                    },
                })
                count += 1
                if count % 500_000 == 0:
                    print(f"    {count:,} buildings loaded...")
    print(f"  Total: {len(features):,} buildings loaded")
    return features


def generate_pmtiles(features: list, bbox: dict, out_path: str, territory: str):
    print(f"\nGenerating PMTiles (zoom {MIN_ZOOM}-{MAX_ZOOM})...")
    t0 = time.time()
    grid: dict = defaultdict(list)
    for i, feat in enumerate(features):
        c = feat["geometry"].centroid
        tx, ty = lng_lat_to_tile(c.x, c.y, MAX_ZOOM)
        grid[(tx, ty)].append(i)
    print(f"  Index: {len(grid):,} grid cells")

    tile_data: dict = {}
    total_tiles = 0
    for z in range(MIN_ZOOM, MAX_ZOOM + 1):
        tiles = get_tiles_for_bbox(bbox, z)
        written = 0
        t_z = time.time()
        for x, y in tiles:
            bounds = tile_bounds(x, y, z)
            scale = 2 ** (MAX_ZOOM - z)
            x_min, x_max = x * scale, (x + 1) * scale - 1
            y_min, y_max = y * scale, (y + 1) * scale - 1
            candidate_indices: set = set()
            for gx in range(x_min, x_max + 1):
                for gy in range(y_min, y_max + 1):
                    candidate_indices.update(grid.get((gx, gy), []))
            if not candidate_indices:
                continue
            tile_bytes = features_to_mvt(
                [features[i] for i in candidate_indices], bounds, LAYER_NAME, EXTENT
            )
            if tile_bytes:
                tile_data[zxy_to_tileid(z, x, y)] = gzip.compress(tile_bytes)
                written += 1
        total_tiles += written
        print(f"  z{z}: {written} tiles ({time.time()-t_z:.1f}s)")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    print(f"\n  Writing {out_path} ({total_tiles} tiles)...")
    with open(out_path, "wb") as f:
        writer = PMTilesWriter(f)
        for tile_id in sorted(tile_data.keys()):
            writer.write_tile(tile_id, tile_data[tile_id])
        writer.finalize(
            {
                "tile_type": TileType.MVT, "tile_compression": Compression.GZIP,
                "min_zoom": MIN_ZOOM, "max_zoom": MAX_ZOOM,
                "min_lon": bbox["west"], "min_lat": bbox["south"],
                "max_lon": bbox["east"], "max_lat": bbox["north"],
                "center_lon": (bbox["west"] + bbox["east"]) / 2,
                "center_lat": (bbox["south"] + bbox["north"]) / 2,
                "center_zoom": 12,
            },
            {
                "name": f"{territory}_buildings",
                "description": f"Building footprints for {territory} (GBA) — height/area only, no census",
                "vector_layers": [{
                    "id": LAYER_NAME,
                    "fields": {"best_height_m": "Number", "area_m2": "Number"},
                    "minzoom": MIN_ZOOM, "maxzoom": MAX_ZOOM,
                }],
            },
        )
    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    print(f"  -> {out_path} ({size_mb:.1f} MB, {time.time()-t0:.0f}s total)")


def main():
    ap = argparse.ArgumentParser(description="Build footprints-only buildings PMTiles for a BR state")
    ap.add_argument("--territory", required=True)
    args = ap.parse_args()
    t = args.territory
    cfg = get_territory(t)
    w, s, e, n = cfg["bbox"]
    bbox = {"west": w, "south": s, "east": e, "north": n}
    table = f"gba_buildings_{t}"
    out_path = os.path.join(OUTPUT_DIR, f"{t}_buildings.pmtiles")

    t_start = time.time()
    print("=" * 60)
    print(f"BUILD BUILDINGS PMTILES (footprints only) — {cfg['label']} ({t})")
    print("=" * 60)
    features = load_buildings(table)
    if not features:
        sys.exit(f"ERROR: no buildings in {table}. Run ingest_gba.py --territory {t} first.")
    generate_pmtiles(features, bbox, out_path, t)
    print(f"\nDone in {time.time()-t_start:.0f}s")
    print("\nNext:")
    print(f"  npx wrangler r2 object put neahub/data/tiles/{t}_buildings.pmtiles \\")
    print(f"    --file {out_path} --remote")


if __name__ == "__main__":
    main()
