"""
extract_building_embeddings.py
Extract per-building AlphaEarth embeddings (64 dims, 10m) for all Misiones buildings.

Downloads GEE tiles only where buildings exist, samples at centroids via direct
numpy indexing (no rasterio needed).

Table:
  building_embeddings (source, building_id PK): a00..a63

Usage:
    python extract_building_embeddings.py --phase {setup,extract,verify,all}
"""

import argparse
import os
import sys
import time

import numpy as np
import ee
import psycopg2
import psycopg2.extras
from pyproj import Transformer

# ── Configuration ──────────────────────────────────────────────
GEE_PROJECT = "amiable-reducer-398015"
ALPHA_COLLECTION = "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL"
PG_DSN = "dbname=ndvi_misiones host=localhost port=5432 user=postgres"
YEAR = 2025
REDUCE_SCALE = 10          # 10m per pixel
TILE_PX = 192              # pixels per tile side (192 * 10m = 1920m)
BUFFER_M = 100             # buffer around department bbox
N_BANDS = 64
BAND_NAMES = [f"A{i:02d}" for i in range(N_BANDS)]   # GEE band names (uppercase)
ALPHA_COLS = [f"a{i:02d}" for i in range(N_BANDS)]    # DB column names (lowercase)
CRS_UTM = "EPSG:32721"
CRS_GEO = "EPSG:4326"


def get_conn():
    return psycopg2.connect(PG_DSN)


# ── Phase: setup ──────────────────────────────────────────────

def phase_setup():
    print("=== PHASE: SETUP ===")
    conn = get_conn()
    cur = conn.cursor()

    cols = ",\n        ".join(f"{c} DOUBLE PRECISION" for c in ALPHA_COLS)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS building_embeddings (
            source      TEXT NOT NULL,
            building_id INTEGER NOT NULL,
            {cols},
            PRIMARY KEY (source, building_id)
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("  Table building_embeddings ready.")


# ── Phase: extract ─────────────────────────────────────────────

def phase_extract():
    print("=== PHASE: EXTRACT ===")

    ee.Initialize(project=GEE_PROJECT)
    col = ee.ImageCollection(ALPHA_COLLECTION)
    img = col.filterDate(f"{YEAR}-01-01", f"{YEAR}-12-31").mosaic()
    print(f"  GEE image ready (year={YEAR})")

    transformer = Transformer.from_crs(CRS_GEO, CRS_UTM, always_xy=True)
    tile_size_m = TILE_PX * REDUCE_SCALE  # 1920m

    conn = get_conn()
    cur = conn.cursor()

    # Get departments
    cur.execute(
        "SELECT DISTINCT LEFT(redcode, 5) AS dpto FROM radios_misiones ORDER BY 1"
    )
    deptos = [r[0] for r in cur.fetchall()]
    print(f"  Departments: {len(deptos)}")

    cols_sql = ", ".join(ALPHA_COLS)
    ph = ", ".join(["%s"] * (2 + N_BANDS))
    upd = ", ".join(f"{c}=EXCLUDED.{c}" for c in ALPHA_COLS)

    total_inserted = 0
    total_skipped_tiles = 0
    total_downloaded_tiles = 0
    t0_global = time.time()

    for d_idx, dpto in enumerate(deptos):
        t0_dpto = time.time()
        print(f"\n--- [{d_idx + 1}/{len(deptos)}] Department {dpto} ---")

        # Department bbox in UTM
        cur.execute("""
            SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e)
            FROM (SELECT ST_Extent(geom) AS e
                  FROM radios_misiones WHERE LEFT(redcode, 5) = %s) t
        """, (dpto,))
        lon_min, lat_min, lon_max, lat_max = cur.fetchone()
        x0, y0 = transformer.transform(lon_min, lat_min)
        x1, y1 = transformer.transform(lon_max, lat_max)
        bbox_xmin = min(x0, x1) - BUFFER_M
        bbox_ymin = min(y0, y1) - BUFFER_M
        bbox_xmax = max(x0, x1) + BUFFER_M
        bbox_ymax = max(y0, y1) + BUFFER_M

        # Query building centroids
        buildings = []  # (source, building_id, utm_x, utm_y)

        cur.execute("""
            SELECT gba_id, ST_X(centroid), ST_Y(centroid)
            FROM gba_buildings
            WHERE LEFT(redcode, 5) = %s AND centroid IS NOT NULL
        """, (dpto,))
        for gba_id, lon, lat in cur.fetchall():
            ux, uy = transformer.transform(lon, lat)
            buildings.append(("gba", gba_id, ux, uy))

        cur.execute("""
            SELECT fid, ST_X(ST_Centroid(geom)), ST_Y(ST_Centroid(geom))
            FROM vida_buildings
            WHERE LEFT(redcode, 5) = %s AND geom IS NOT NULL
        """, (dpto,))
        for fid, lon, lat in cur.fetchall():
            ux, uy = transformer.transform(lon, lat)
            buildings.append(("vida", fid, ux, uy))

        if not buildings:
            print("  No buildings. Skipping.")
            continue

        n_gba = sum(1 for b in buildings if b[0] == "gba")
        n_vida = sum(1 for b in buildings if b[0] == "vida")
        print(f"  Buildings: {len(buildings):,} (gba: {n_gba:,}, vida: {n_vida:,})")

        # Filter out already-extracted buildings
        existing = set()
        for src_name, id_list in [
            ("gba", [b[1] for b in buildings if b[0] == "gba"]),
            ("vida", [b[1] for b in buildings if b[0] == "vida"]),
        ]:
            for batch_start in range(0, len(id_list), 10000):
                batch = id_list[batch_start : batch_start + 10000]
                cur.execute(
                    "SELECT building_id FROM building_embeddings "
                    "WHERE source = %s AND building_id = ANY(%s)",
                    (src_name, batch),
                )
                existing.update((src_name, r[0]) for r in cur.fetchall())

        buildings = [b for b in buildings if (b[0], b[1]) not in existing]
        if not buildings:
            print(f"  All buildings already extracted. Skipping.")
            continue
        if existing:
            print(f"  Resuming: {len(existing):,} done, {len(buildings):,} remaining")

        # Tile grid
        width_px = int(np.ceil((bbox_xmax - bbox_xmin) / REDUCE_SCALE))
        height_px = int(np.ceil((bbox_ymax - bbox_ymin) / REDUCE_SCALE))
        n_tx = int(np.ceil(width_px / TILE_PX))
        n_ty = int(np.ceil(height_px / TILE_PX))
        total_tiles = n_tx * n_ty

        # Index buildings by tile
        tile_buildings = {}  # (tx, ty) -> [buildings]
        for b in buildings:
            _, _, ux, uy = b
            tx = int((ux - bbox_xmin) / tile_size_m)
            ty = int((bbox_ymax - uy) / tile_size_m)
            tx = max(0, min(tx, n_tx - 1))
            ty = max(0, min(ty, n_ty - 1))
            tile_buildings.setdefault((tx, ty), []).append(b)

        n_tiles_with = len(tile_buildings)
        n_tiles_skip = total_tiles - n_tiles_with
        print(f"  Tiles: {n_tiles_with} with buildings / {total_tiles} total "
              f"({n_tiles_skip} skipped)")

        total_skipped_tiles += n_tiles_skip
        dpto_inserted = 0
        dpto_downloaded = 0

        for tile_idx, ((tx, ty), bldgs) in enumerate(tile_buildings.items()):
            tile_xmin = bbox_xmin + tx * tile_size_m
            tile_ymax_t = bbox_ymax - ty * tile_size_m
            tw = min(TILE_PX, width_px - tx * TILE_PX)
            th = min(TILE_PX, height_px - ty * TILE_PX)
            tw = max(1, tw)
            th = max(1, th)

            params = {
                "expression": img,
                "fileFormat": "NUMPY_NDARRAY",
                "grid": {
                    "dimensions": {"width": int(tw), "height": int(th)},
                    "affineTransform": {
                        "scaleX": REDUCE_SCALE,
                        "shearX": 0,
                        "translateX": tile_xmin,
                        "shearY": 0,
                        "scaleY": -REDUCE_SCALE,
                        "translateY": tile_ymax_t,
                    },
                    "crsCode": CRS_UTM,
                },
            }

            result = None
            for attempt in range(3):
                try:
                    result = ee.data.computePixels(params)
                    break
                except Exception as e:
                    wait = 15 * (attempt + 1)
                    if attempt < 2:
                        print(f"\n    Tile ({tx},{ty}) retry {attempt+1}/3: "
                              f"{type(e).__name__}")
                        time.sleep(wait)
                    else:
                        print(f"\n    Tile ({tx},{ty}) FAILED: {e}")

            if result is None:
                continue

            dpto_downloaded += 1
            total_downloaded_tiles += 1

            # Sample at building centroids via direct numpy indexing
            insert_batch = []
            for b in bldgs:
                source, bid, ux, uy = b
                col_idx = int((ux - tile_xmin) / REDUCE_SCALE)
                row_idx = int((tile_ymax_t - uy) / REDUCE_SCALE)
                col_idx = max(0, min(col_idx, tw - 1))
                row_idx = max(0, min(row_idx, th - 1))

                vals = []
                all_nan = True
                for band in BAND_NAMES:
                    if band in result.dtype.names:
                        v = float(result[band][row_idx, col_idx])
                        if np.isnan(v):
                            vals.append(None)
                        else:
                            vals.append(v)
                            all_nan = False
                    else:
                        vals.append(None)

                if all_nan:
                    continue
                insert_batch.append([source, bid] + vals)

            if insert_batch:
                psycopg2.extras.execute_batch(
                    cur,
                    f"INSERT INTO building_embeddings (source, building_id, {cols_sql}) "
                    f"VALUES ({ph}) "
                    f"ON CONFLICT (source, building_id) DO UPDATE SET {upd}",
                    insert_batch,
                    page_size=1000,
                )
                conn.commit()
                dpto_inserted += len(insert_batch)

            if (tile_idx + 1) % 10 == 0 or tile_idx + 1 == n_tiles_with:
                elapsed = time.time() - t0_dpto
                print(f"\r  Tiles: {tile_idx + 1}/{n_tiles_with}, "
                      f"buildings: {dpto_inserted:,}, {elapsed:.0f}s",
                      end="", flush=True)

        total_inserted += dpto_inserted
        elapsed = time.time() - t0_dpto
        print(f"\n  Department {dpto}: {dpto_inserted:,} buildings, "
              f"{dpto_downloaded} tiles downloaded, {elapsed:.0f}s")

    elapsed_total = time.time() - t0_global
    print(f"\n=== EXTRACT COMPLETE ===")
    print(f"  Total inserted: {total_inserted:,}")
    print(f"  Tiles downloaded: {total_downloaded_tiles:,} "
          f"(skipped: {total_skipped_tiles:,})")
    print(f"  Time: {elapsed_total:.0f}s ({elapsed_total / 60:.1f}min)")

    cur.close()
    conn.close()


# ── Phase: verify ──────────────────────────────────────────────

def phase_verify():
    print("=== PHASE: VERIFY ===")
    conn = get_conn()
    cur = conn.cursor()

    # Total counts
    cur.execute("SELECT count(*) FROM building_embeddings WHERE source = 'gba'")
    n_gba_emb = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM building_embeddings WHERE source = 'vida'")
    n_vida_emb = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM gba_buildings WHERE centroid IS NOT NULL")
    n_gba_total = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM vida_buildings WHERE geom IS NOT NULL")
    n_vida_total = cur.fetchone()[0]

    gba_pct = n_gba_emb * 100 / n_gba_total if n_gba_total > 0 else 0
    vida_pct = n_vida_emb * 100 / n_vida_total if n_vida_total > 0 else 0
    total_emb = n_gba_emb + n_vida_emb
    total_bld = n_gba_total + n_vida_total
    total_pct = total_emb * 100 / total_bld if total_bld > 0 else 0

    print(f"  GBA:   {n_gba_emb:>10,} / {n_gba_total:>10,} ({gba_pct:.1f}%)")
    print(f"  VIDA:  {n_vida_emb:>10,} / {n_vida_total:>10,} ({vida_pct:.1f}%)")
    print(f"  Total: {total_emb:>10,} / {total_bld:>10,} ({total_pct:.1f}%)")

    # NaN check
    if total_emb > 0:
        print("\n  NaN check (sample bands):")
        for c in ["a00", "a15", "a31", "a47", "a63"]:
            cur.execute(f"SELECT count(*) FROM building_embeddings WHERE {c} IS NULL")
            nn = cur.fetchone()[0]
            pct = nn * 100 / total_emb
            status = "OK" if pct < 5 else "WARNING"
            print(f"    {c}: {nn:,} NULLs ({pct:.1f}%) [{status}]")

        # Value ranges
        print("\n  Value ranges:")
        cur.execute("""
            SELECT avg(a00), min(a00), max(a00),
                   avg(a31), min(a31), max(a31)
            FROM building_embeddings
        """)
        r = cur.fetchone()
        if r[0] is not None:
            print(f"    a00: [{r[1]:.4f}, {r[2]:.4f}] avg={r[0]:.4f}")
            print(f"    a31: [{r[4]:.4f}, {r[5]:.4f}] avg={r[3]:.4f}")

    # Per-department coverage (GBA only for brevity)
    print("\n  Per-department coverage (GBA):")
    cur.execute("""
        SELECT dpto, total, with_emb FROM (
            SELECT LEFT(b.redcode, 5) AS dpto,
                   COUNT(*) AS total,
                   COUNT(be.building_id) AS with_emb
            FROM gba_buildings b
            LEFT JOIN building_embeddings be
                ON be.source = 'gba' AND be.building_id = b.gba_id
            WHERE b.centroid IS NOT NULL
            GROUP BY 1
        ) t ORDER BY 1
    """)
    for dpto, total, with_emb in cur.fetchall():
        pct = with_emb * 100 / total if total > 0 else 0
        print(f"    {dpto}: {with_emb:,} / {total:,} ({pct:.1f}%)")

    cur.close()
    conn.close()

    if total_pct >= 95:
        print(f"\n  Coverage {total_pct:.1f}% >= 95%: OK")
    else:
        print(f"\n  Coverage {total_pct:.1f}% < 95%: WARNING")


# ── Main ───────────────────────────────────────────────────────

PHASES = {
    "setup": phase_setup,
    "extract": phase_extract,
    "verify": phase_verify,
}
PHASE_ORDER = ["setup", "extract", "verify"]


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(
        description="Extract per-building AlphaEarth embeddings"
    )
    parser.add_argument(
        "--phase",
        required=True,
        choices=PHASE_ORDER + ["all"],
        help="Phase to run (or 'all' for full pipeline)",
    )
    args = parser.parse_args()

    phases = PHASE_ORDER if args.phase == "all" else [args.phase]

    t0 = time.time()
    for phase in phases:
        PHASES[phase]()

    elapsed = time.time() - t0
    print(f"\nDone. Total time: {elapsed:.0f}s ({elapsed / 60:.1f}min)")


if __name__ == "__main__":
    main()
