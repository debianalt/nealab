"""
extract_embeddings_sampleregions.py
Server-side AlphaEarth embedding extraction (64-dim) for any territory with a
gba_buildings_<territory> table (centroid populated). Instead of downloading
tiles one-by-one, it samples ALL building centroids server-side via
img.reduceRegions(points, Reducer.first(), 10) in concurrent chunks against the
GEE high-volume endpoint. Same pixel values as the tile approach — just far
faster. Resumable (skips building_ids already present for this source).

Writes building_embeddings (source, building_id PK): a00..a63.

Usage:
  python extract_embeddings_sampleregions.py --territory corrientes [--workers 16] [--chunk 3000]
"""
import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import ee
import psycopg2
import psycopg2.extras

GEE_PROJECT = "amiable-reducer-398015"
HV_ENDPOINT = "https://earthengine-highvolume.googleapis.com"
ALPHA_COLLECTION = "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL"
PG_DSN = "dbname=ndvi_misiones host=localhost port=5432 user=postgres"
YEAR = 2025
N_BANDS = 64
BAND_NAMES = [f"A{i:02d}" for i in range(N_BANDS)]   # GEE band names
ALPHA_COLS = [f"a{i:02d}" for i in range(N_BANDS)]    # DB columns

_img = None  # set after ee.Initialize


def sample_chunk(points):
    """points: list of (bid, lng, lat). Returns list of [source-less] rows
    [bid, A00..A63] for points that hit data. Server-side reduceRegions."""
    feats = [ee.Feature(ee.Geometry.Point([lng, lat]), {"bid": int(bid)})
             for bid, lng, lat in points]
    fc = ee.FeatureCollection(feats)
    sampled = _img.reduceRegions(collection=fc, reducer=ee.Reducer.first(), scale=10)
    out = []
    for attempt in range(4):
        try:
            data = sampled.getInfo()
            break
        except Exception as e:
            if attempt < 3:
                time.sleep(8 * (attempt + 1)); continue
            raise
    for f in data["features"]:
        p = f["properties"]
        if "A00" not in p or p.get("A00") is None:
            continue
        vals = [p.get(b) for b in BAND_NAMES]
        if all(v is None for v in vals):
            continue
        out.append([int(p["bid"])] + vals)
    return out


def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", required=True)
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--chunk", type=int, default=3000)
    args = ap.parse_args()
    t = args.territory
    table = f"gba_buildings_{t}"
    source = t

    global _img
    ee.Initialize(project=GEE_PROJECT, opt_url=HV_ENDPOINT)
    _img = ee.ImageCollection(ALPHA_COLLECTION).filterDate(f"{YEAR}-01-01", f"{YEAR}-12-31").mosaic()
    print(f"GEE high-volume ready; territory={t}, table={table}")

    conn = psycopg2.connect(PG_DSN); cur = conn.cursor()
    cols_def = ",\n        ".join(f"{c} DOUBLE PRECISION" for c in ALPHA_COLS)
    cur.execute(f"""CREATE TABLE IF NOT EXISTS building_embeddings (
        source TEXT NOT NULL, building_id INTEGER NOT NULL, {cols_def},
        PRIMARY KEY (source, building_id))""")
    conn.commit()

    # Remaining centroids (resume: skip already-extracted)
    cur.execute(f"""
        SELECT b.gid, ST_X(b.centroid), ST_Y(b.centroid)
        FROM {table} b
        LEFT JOIN building_embeddings be ON be.source=%s AND be.building_id=b.gid
        WHERE b.centroid IS NOT NULL AND be.building_id IS NULL
    """, (source,))
    pts = cur.fetchall()
    print(f"  {len(pts):,} buildings to extract (resume-aware)")
    if not pts:
        print("  nothing to do"); return

    chunks = [pts[i:i + args.chunk] for i in range(0, len(pts), args.chunk)]
    print(f"  {len(chunks)} chunks of {args.chunk}, {args.workers} workers")

    cols_sql = ", ".join(ALPHA_COLS)
    ph = ", ".join(["%s"] * (2 + N_BANDS))
    upd = ", ".join(f"{c}=EXCLUDED.{c}" for c in ALPHA_COLS)
    ins_sql = (f"INSERT INTO building_embeddings (source, building_id, {cols_sql}) "
               f"VALUES ({ph}) ON CONFLICT (source, building_id) DO UPDATE SET {upd}")

    t0 = time.time(); done = 0; total_ins = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(sample_chunk, ch): i for i, ch in enumerate(chunks)}
        for fut in as_completed(futs):
            done += 1
            try:
                rows = fut.result()
            except Exception as e:
                print(f"\n  chunk {futs[fut]} FAILED: {str(e)[:100]}")
                continue
            if rows:
                batch = [[source] + r for r in rows]
                psycopg2.extras.execute_batch(cur, ins_sql, batch, page_size=1000)
                conn.commit(); total_ins += len(rows)
            if done % 10 == 0 or done == len(chunks):
                el = time.time() - t0
                rate = done / el if el else 0
                eta = (len(chunks) - done) / rate / 60 if rate else 0
                print(f"\r  chunks {done}/{len(chunks)} | {total_ins:,} embeddings | "
                      f"{el:.0f}s | ETA {eta:.1f}min", end="", flush=True)

    print(f"\nDONE: {total_ins:,} embeddings inserted in {(time.time()-t0)/60:.1f}min")
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
