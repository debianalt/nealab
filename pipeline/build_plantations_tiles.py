"""
Build forestry-plantation PMTiles per AR territory from the DNDFI national
inventory (Inventario Nacional de Plantaciones Forestales).

Source: dndfi_inventario/macizos_23_04_2026.geojson (2026 snapshot, 280k macizos
/ 23 provinces). We keep only the NEA-AR provinces Spatia covers and only the
polygonal geometry (POLYGON/MULTIPOLYGON + polygons extracted from the few
GEOMETRYCOLLECTIONs; the ~140 stray point/line records are dropped and logged).

Rendered as a toggleable reference overlay on the forestry_aptitude layer so the
similarity score can be read against where plantations actually exist. Reuses the
custom tiler from build_ar_buildings.py (no tippecanoe needed).

Usage:
  python pipeline/build_plantations_tiles.py --territory misiones
  python pipeline/build_plantations_tiles.py --territory all

Output:
  pipeline/output/<territory>_plantations.pmtiles

R2 upload (after inspecting output):
  npx wrangler r2 object put neahub/data/tiles/<territory>_plantations.pmtiles \\
    --file pipeline/output/<territory>_plantations.pmtiles --remote
"""
import argparse
import gzip
import os
import sys
import time
import warnings
from collections import defaultdict

warnings.filterwarnings("ignore", category=DeprecationWarning)

import duckdb
from pmtiles.tile import TileType, zxy_to_tileid
from pmtiles.writer import Compression, Writer as PMTilesWriter
from shapely import wkb
from shapely.ops import unary_union

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from build_ar_buildings import (  # reuse the tiler
    MIN_ZOOM, MAX_ZOOM, EXTENT,
    get_tiles_for_bbox, tile_bounds, lng_lat_to_tile, features_to_mvt,
)
from config import OUTPUT_DIR, get_territory

LAYER_NAME = "plantations"  # must match 'source-layer' in Map.svelte

DEFAULT_GEOJSON = os.path.join(
    os.path.dirname(os.path.dirname(SCRIPT_DIR)), "dndfi_inventario", "macizos_23_04_2026.geojson"
)

# Spatia AR territory id -> macizos `prov` value.
TERRITORY_PROV = {
    "misiones": "MISIONES",
    "corrientes": "CORRIENTES",
    "chaco": "CHACO",
    "formosa": "FORMOSA",
}

PROP_COLS = ["especie", "genero", "grupo_espe", "anio_plant", "superficie", "depto", "uso_forest"]


def _polygonal(geom):
    """Return polygonal part of a geometry, or None if there is none."""
    gt = geom.geom_type
    if gt in ("Polygon", "MultiPolygon"):
        return geom if not geom.is_empty else None
    if gt == "GeometryCollection":
        polys = [g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon")]
        return unary_union(polys) if polys else None
    return None  # Point / LineString -> drop


def load_features(con: duckdb.DuckDBPyConnection, prov: str) -> list:
    cols = ", ".join(PROP_COLS)
    rows = con.execute(
        f"SELECT ST_AsWKB(geom) AS wkb, {cols} FROM macizos WHERE prov = ?", [prov]
    ).fetchall()
    features, dropped = [], 0
    for row in rows:
        geom = _polygonal(wkb.loads(bytes(row[0])))
        if geom is None:
            dropped += 1
            continue
        props = {}
        for col, val in zip(PROP_COLS, row[1:]):
            if val is None:
                continue
            props[col] = round(float(val), 2) if col == "superficie" else (
                int(val) if col == "anio_plant" else str(val)
            )
        features.append({"geometry": geom, "properties": props})
    print(f"  {prov}: {len(features):,} polygonal macizos ({dropped} non-polygonal dropped)")
    return features


def generate_pmtiles(features: list, bbox: dict, out_path: str, territory: str):
    print(f"\nGenerating PMTiles (zoom {MIN_ZOOM}-{MAX_ZOOM})...")
    t0 = time.time()
    grid: dict = defaultdict(list)
    for i, feat in enumerate(features):
        c = feat["geometry"].centroid
        tx, ty = lng_lat_to_tile(c.x, c.y, MAX_ZOOM)
        grid[(tx, ty)].append(i)

    tile_data: dict = {}
    for z in range(MIN_ZOOM, MAX_ZOOM + 1):
        for x, y in get_tiles_for_bbox(bbox, z):
            bounds = tile_bounds(x, y, z)
            scale = 2 ** (MAX_ZOOM - z)
            x_min, x_max = x * scale, (x + 1) * scale - 1
            y_min, y_max = y * scale, (y + 1) * scale - 1
            cand: set = set()
            for gx in range(x_min, x_max + 1):
                for gy in range(y_min, y_max + 1):
                    cand.update(grid.get((gx, gy), []))
            if not cand:
                continue
            tile_bytes = features_to_mvt([features[i] for i in cand], bounds, LAYER_NAME, EXTENT)
            if tile_bytes:
                tile_data[zxy_to_tileid(z, x, y)] = gzip.compress(tile_bytes)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    print(f"  Writing {out_path} ({len(tile_data)} tiles)...")
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
                "name": f"{territory}_plantations",
                "description": "Forestry plantations (DNDFI Inventario Nacional, 2026)",
                "vector_layers": [{
                    "id": LAYER_NAME,
                    "fields": {
                        "especie": "String", "genero": "String", "grupo_espe": "String",
                        "anio_plant": "Number", "superficie": "Number",
                        "depto": "String", "uso_forest": "String",
                    },
                    "minzoom": MIN_ZOOM, "maxzoom": MAX_ZOOM,
                }],
            },
        )
    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    print(f"  -> {out_path} ({size_mb:.1f} MB, {time.time()-t0:.0f}s)")


def main():
    ap = argparse.ArgumentParser(description="Build forestry-plantation PMTiles from DNDFI inventory")
    ap.add_argument("--territory", required=True, help="misiones|corrientes|chaco|formosa|all")
    ap.add_argument("--geojson", default=DEFAULT_GEOJSON)
    args = ap.parse_args()

    if not os.path.exists(args.geojson):
        sys.exit(f"ERROR: missing {args.geojson}")

    territories = list(TERRITORY_PROV) if args.territory == "all" else [args.territory]
    for t in territories:
        if t not in TERRITORY_PROV:
            sys.exit(f"ERROR: '{t}' has no plantation coverage (AR-NEA only: {list(TERRITORY_PROV)})")

    geojson = os.path.abspath(args.geojson).replace("\\", "/")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    print(f"Loading macizos for {len(territories)} territory(ies) from {os.path.basename(geojson)}...")
    provs = tuple(TERRITORY_PROV[t] for t in territories)
    placeholders = ", ".join("?" for _ in provs)
    con.execute(
        f"CREATE TABLE macizos AS SELECT * FROM ST_Read('{geojson}') WHERE prov IN ({placeholders})",
        list(provs),
    )

    for t in territories:
        print("=" * 60)
        print(f"PLANTATIONS PMTILES — {get_territory(t)['label']} ({t})")
        feats = load_features(con, TERRITORY_PROV[t])
        if not feats:
            print(f"  WARNING: no features for {t}, skipping")
            continue
        w, s, e, n = get_territory(t)["bbox"]
        out_path = os.path.join(OUTPUT_DIR, f"{t}_plantations.pmtiles")
        generate_pmtiles(feats, {"west": w, "south": s, "east": e, "north": n}, out_path, t)

    print("\nNext: upload to R2 with")
    print("  npx wrangler r2 object put neahub/data/tiles/<t>_plantations.pmtiles \\")
    print("    --file pipeline/output/<t>_plantations.pmtiles --remote")


if __name__ == "__main__":
    main()
