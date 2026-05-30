"""
extract_embeddings_territory.py
Generalized AlphaEarth embedding extraction for any non-Misiones territory with a
gba_buildings_<territory> table (centroid column populated). Mirrors the Misiones
extract_building_embeddings.py tile logic, but: single table, building_id = gid,
source = <territory>, department bbox derived from the buildings table itself
(no radios_* dependency). Resumable.

Writes to the shared building_embeddings table (source, building_id PK): a00..a63.

Usage:
  python extract_embeddings_territory.py --territory corrientes
"""
import argparse
import sys
import time

import numpy as np
import ee
import psycopg2
import psycopg2.extras
from pyproj import Transformer

GEE_PROJECT = "amiable-reducer-398015"
ALPHA_COLLECTION = "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL"
PG_DSN = "dbname=ndvi_misiones host=localhost port=5432 user=postgres"
YEAR = 2025
REDUCE_SCALE = 10
TILE_PX = 192
BUFFER_M = 100
N_BANDS = 64
BAND_NAMES = [f"A{i:02d}" for i in range(N_BANDS)]
ALPHA_COLS = [f"a{i:02d}" for i in range(N_BANDS)]
CRS_GEO = "EPSG:4326"
# Local UTM for the tile grid (only affects tile alignment; any reasonable zone works).
UTM_BY_TERRITORY = {
    "corrientes": "EPSG:32721", "chaco": "EPSG:32720", "formosa": "EPSG:32720",
    "itapua_py": "EPSG:32721", "alto_parana_py": "EPSG:32721",
    "parana_br": "EPSG:32722", "santa_catarina_br": "EPSG:32722", "rio_grande_sul_br": "EPSG:32722",
}


def get_conn():
    return psycopg2.connect(PG_DSN)


def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", required=True)
    args = ap.parse_args()
    t = args.territory
    table = f"gba_buildings_{t}"
    source = t
    crs_utm = UTM_BY_TERRITORY.get(t, "EPSG:32721")

    conn = get_conn(); cur = conn.cursor()
    # Ensure shared embeddings table exists
    cols_def = ",\n        ".join(f"{c} DOUBLE PRECISION" for c in ALPHA_COLS)
    cur.execute(f"""CREATE TABLE IF NOT EXISTS building_embeddings (
        source TEXT NOT NULL, building_id INTEGER NOT NULL, {cols_def},
        PRIMARY KEY (source, building_id))""")
    conn.commit()

    ee.Initialize(project=GEE_PROJECT)
    img = ee.ImageCollection(ALPHA_COLLECTION).filterDate(f"{YEAR}-01-01", f"{YEAR}-12-31").mosaic()
    print(f"GEE ready (year={YEAR}); territory={t}, table={table}, UTM={crs_utm}")

    transformer = Transformer.from_crs(CRS_GEO, crs_utm, always_xy=True)
    tile_size_m = TILE_PX * REDUCE_SCALE

    # Departments from the buildings table itself
    cur.execute(f"SELECT DISTINCT LEFT(redcode,5) d FROM {table} WHERE redcode IS NOT NULL ORDER BY 1")
    deptos = [r[0] for r in cur.fetchall()]
    print(f"Departments: {len(deptos)}")

    cols_sql = ", ".join(ALPHA_COLS)
    ph = ", ".join(["%s"] * (2 + N_BANDS))
    upd = ", ".join(f"{c}=EXCLUDED.{c}" for c in ALPHA_COLS)

    total_inserted = 0
    tg = time.time()
    for d_idx, dpto in enumerate(deptos):
        td = time.time()
        print(f"\n--- [{d_idx+1}/{len(deptos)}] dpto {dpto} ---")
        cur.execute(f"""SELECT ST_XMin(e),ST_YMin(e),ST_XMax(e),ST_YMax(e) FROM
            (SELECT ST_Extent(centroid) e FROM {table} WHERE LEFT(redcode,5)=%s) q""", (dpto,))
        lon_min, lat_min, lon_max, lat_max = cur.fetchone()
        if lon_min is None:
            continue
        x0, y0 = transformer.transform(lon_min, lat_min)
        x1, y1 = transformer.transform(lon_max, lat_max)
        bx0, by0 = min(x0, x1)-BUFFER_M, min(y0, y1)-BUFFER_M
        bx1, by1 = max(x0, x1)+BUFFER_M, max(y0, y1)+BUFFER_M

        cur.execute(f"SELECT gid, ST_X(centroid), ST_Y(centroid) FROM {table} "
                    f"WHERE LEFT(redcode,5)=%s AND centroid IS NOT NULL", (dpto,))
        buildings = [(bid, *transformer.transform(lon, lat)) for bid, lon, lat in cur.fetchall()]
        if not buildings:
            continue

        # Skip already-extracted
        ids = [b[0] for b in buildings]
        existing = set()
        for s in range(0, len(ids), 10000):
            cur.execute("SELECT building_id FROM building_embeddings WHERE source=%s AND building_id=ANY(%s)",
                        (source, ids[s:s+10000]))
            existing.update(r[0] for r in cur.fetchall())
        buildings = [b for b in buildings if b[0] not in existing]
        if not buildings:
            print(f"  all {len(existing):,} done"); continue
        print(f"  {len(buildings):,} to extract ({len(existing):,} already done)")

        width_px = int(np.ceil((bx1-bx0)/REDUCE_SCALE)); height_px = int(np.ceil((by1-by0)/REDUCE_SCALE))
        n_tx = int(np.ceil(width_px/TILE_PX)); n_ty = int(np.ceil(height_px/TILE_PX))
        tile_b = {}
        for bid, ux, uy in buildings:
            tx = max(0, min(int((ux-bx0)/tile_size_m), n_tx-1))
            ty = max(0, min(int((by1-uy)/tile_size_m), n_ty-1))
            tile_b.setdefault((tx, ty), []).append((bid, ux, uy))
        print(f"  {len(tile_b)} tiles with buildings")

        d_ins = 0
        for ti, ((tx, ty), bl) in enumerate(tile_b.items()):
            txmin = bx0 + tx*tile_size_m; tymax = by1 - ty*tile_size_m
            tw = max(1, min(TILE_PX, width_px - tx*TILE_PX)); th = max(1, min(TILE_PX, height_px - ty*TILE_PX))
            params = {"expression": img, "fileFormat": "NUMPY_NDARRAY", "grid": {
                "dimensions": {"width": int(tw), "height": int(th)},
                "affineTransform": {"scaleX": REDUCE_SCALE, "shearX": 0, "translateX": txmin,
                                    "shearY": 0, "scaleY": -REDUCE_SCALE, "translateY": tymax},
                "crsCode": crs_utm}}
            res = None
            for att in range(3):
                try:
                    res = ee.data.computePixels(params); break
                except Exception as e:
                    if att < 2: time.sleep(15*(att+1))
                    else: print(f"    tile ({tx},{ty}) FAILED: {str(e)[:80]}")
            if res is None:
                continue
            batch = []
            for bid, ux, uy in bl:
                ci = max(0, min(int((ux-txmin)/REDUCE_SCALE), tw-1))
                ri = max(0, min(int((tymax-uy)/REDUCE_SCALE), th-1))
                vals = []; allnan = True
                for band in BAND_NAMES:
                    if band in res.dtype.names:
                        v = float(res[band][ri, ci])
                        if np.isnan(v): vals.append(None)
                        else: vals.append(v); allnan = False
                    else: vals.append(None)
                if not allnan:
                    batch.append([source, bid] + vals)
            if batch:
                psycopg2.extras.execute_batch(cur,
                    f"INSERT INTO building_embeddings (source, building_id, {cols_sql}) VALUES ({ph}) "
                    f"ON CONFLICT (source, building_id) DO UPDATE SET {upd}", batch, page_size=1000)
                conn.commit(); d_ins += len(batch)
            if (ti+1) % 20 == 0:
                print(f"\r  tiles {ti+1}/{len(tile_b)}, {d_ins:,} bldgs, {time.time()-td:.0f}s", end="", flush=True)
        total_inserted += d_ins
        print(f"\n  dpto {dpto}: {d_ins:,} bldgs ({time.time()-td:.0f}s)")

    print(f"\nDONE: {total_inserted:,} embeddings inserted, {(time.time()-tg)/60:.1f}min")
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
