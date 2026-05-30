"""
enrich_heights.py
Enrich gba_buildings (and vida_buildings) with three height estimation sources,
then compute best_height_m via priority cascade.

Sources (priority order):
  1. OSM building:levels via Overpass API (most precise where available)
  2. GHSL-BUILT-H 2018 via Google Earth Engine (100m satellite grid)
  3. GBA deep-learning height_m (existing, RMSE 8.9m)
  4. Population proxy: est_pop * 75 / area_m2 (server-side)
  5. Fallback: 5m

Usage:
    python enrich_heights.py                    # all phases
    python enrich_heights.py --phase setup      # only ALTER TABLE
    python enrich_heights.py --phase ghsl       # only GHSL via GEE
    python enrich_heights.py --phase osm        # only OSM via Overpass
    python enrich_heights.py --phase proxy      # only population proxy
    python enrich_heights.py --phase cascade    # only best_height_m cascade
    python enrich_heights.py --phase summary    # only print stats
"""

import argparse
import json
import time

import ee
import psycopg2
import requests

DB = "dbname=ndvi_misiones host=localhost port=5432 user=postgres"

# Misiones bounding box
MISIONES_BBOX = (-55.95, -28.17, -53.63, -25.47)


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_conn():
    conn = psycopg2.connect(DB)
    conn.autocommit = False
    return conn


def table_exists(cur, table):
    cur.execute(
        "SELECT EXISTS (SELECT FROM information_schema.tables "
        "WHERE table_name = %s)", (table,)
    )
    return cur.fetchone()[0]


# ── Phase 0: Setup ──────────────────────────────────────────────────────────

def phase_setup():
    """Add height enrichment columns to building tables."""
    print("=== Phase 0: Setup — ALTER TABLE ===")
    conn = get_conn()
    cur = conn.cursor()

    new_cols = [
        ("ghsl_height_m", "DOUBLE PRECISION"),
        ("osm_levels", "INTEGER"),
        ("proxy_height_m", "DOUBLE PRECISION"),
        ("best_height_m", "DOUBLE PRECISION"),
    ]

    for table in ["gba_buildings", "vida_buildings"]:
        if not table_exists(cur, table):
            print(f"  {table}: table not found, skipping")
            continue
        for col, dtype in new_cols:
            cur.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {dtype}"
            )
        print(f"  {table}: columns added")
    conn.commit()

    # Staging tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ghsl_built_height (
            redcode TEXT PRIMARY KEY,
            height_mean FLOAT,
            height_max FLOAT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS osm_buildings_levels (
            osm_id BIGINT PRIMARY KEY,
            levels INTEGER,
            geom GEOMETRY(Polygon, 4326)
        )
    """)
    conn.commit()
    print("  Staging tables ready (ghsl_built_height, osm_buildings_levels)")

    cur.close()
    conn.close()


# ── Phase 1: GHSL-BUILT-H via GEE ──────────────────────────────────────────

def phase_ghsl():
    """Extract GHSL Built Height per radio via GEE reduceRegions."""
    print("=== Phase 1: GHSL-BUILT-H via GEE ===")

    ee.Initialize()
    print("  GEE initialized")

    ghsl = ee.Image("JRC/GHSL/P2023A/GHS_BUILT_H/2018").select("built_height")

    conn = get_conn()
    cur = conn.cursor()

    # Load radio geometries from PostGIS
    cur.execute("""
        SELECT redcode, ST_AsGeoJSON(geom) FROM radios_misiones
        WHERE geom IS NOT NULL
    """)
    rows = cur.fetchall()
    print(f"  Loaded {len(rows):,} radio geometries")

    # Process in batches (GEE has limits on feature collection size)
    BATCH = 200
    total_inserted = 0

    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        features = []
        for redcode, geojson in batch:
            geom = json.loads(geojson)
            feat = ee.Feature(ee.Geometry(geom), {"redcode": redcode})
            features.append(feat)

        fc = ee.FeatureCollection(features)

        reducer = (
            ee.Reducer.mean()
            .combine(ee.Reducer.max(), sharedInputs=True)
        )

        results = ghsl.reduceRegions(
            collection=fc,
            reducer=reducer,
            scale=100,
        ).getInfo()

        for feat in results["features"]:
            props = feat["properties"]
            redcode = props["redcode"]
            h_mean = props.get("mean")
            h_max = props.get("max")

            if h_mean is not None or h_max is not None:
                cur.execute("""
                    INSERT INTO ghsl_built_height (redcode, height_mean, height_max)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (redcode) DO UPDATE
                    SET height_mean = EXCLUDED.height_mean,
                        height_max = EXCLUDED.height_max
                """, (redcode, h_mean, h_max))
                total_inserted += 1

        conn.commit()
        print(f"  Batch {i // BATCH + 1}/{(len(rows) + BATCH - 1) // BATCH}: "
              f"{len(batch)} radios processed")

    print(f"  Inserted {total_inserted:,} rows into ghsl_built_height")

    # Update building tables
    for table in ["gba_buildings", "vida_buildings"]:
        if not table_exists(cur, table):
            continue
        cur.execute(f"""
            UPDATE {table} b SET ghsl_height_m = gh.height_mean
            FROM ghsl_built_height gh WHERE b.redcode = gh.redcode
        """)
        updated = cur.rowcount
        conn.commit()
        print(f"  {table}: updated ghsl_height_m on {updated:,} buildings")

    cur.close()
    conn.close()


# ── Phase 2: OSM building:levels via Overpass ───────────────────────────────

def phase_osm():
    """Fetch OSM building:levels via Overpass API and spatial-join to buildings."""
    print("=== Phase 2: OSM building:levels via Overpass ===")

    # Overpass query for buildings with levels in Misiones bbox
    bbox = f"{MISIONES_BBOX[1]},{MISIONES_BBOX[0]},{MISIONES_BBOX[3]},{MISIONES_BBOX[2]}"
    query = f"""
    [out:json][timeout:120];
    (
      way["building"]["building:levels"]({bbox});
    );
    out body geom;
    """

    print("  Querying Overpass API...")
    t0 = time.time()
    resp = requests.post(
        "https://overpass-api.de/api/interpreter",
        data={"data": query},
        timeout=180,
    )
    resp.raise_for_status()
    data = resp.json()
    elements = data.get("elements", [])
    print(f"  Received {len(elements):,} ways ({time.time() - t0:.0f}s)")

    if not elements:
        print("  No OSM buildings with levels found in Misiones")
        return

    conn = get_conn()
    cur = conn.cursor()

    # Clear old data
    cur.execute("DELETE FROM osm_buildings_levels")
    conn.commit()

    inserted = 0
    for el in elements:
        osm_id = el["id"]
        levels_str = el.get("tags", {}).get("building:levels", "")
        try:
            levels = int(float(levels_str))
        except (ValueError, TypeError):
            continue
        if levels <= 0:
            continue

        # Build polygon from way geometry
        geom_nodes = el.get("geometry", [])
        if len(geom_nodes) < 4:
            continue

        coords = [[n["lon"], n["lat"]] for n in geom_nodes]
        # Ensure ring is closed
        if coords[0] != coords[-1]:
            coords.append(coords[0])

        geojson = json.dumps({
            "type": "Polygon",
            "coordinates": [coords]
        })

        cur.execute("""
            INSERT INTO osm_buildings_levels (osm_id, levels, geom)
            VALUES (%s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
            ON CONFLICT (osm_id) DO UPDATE
            SET levels = EXCLUDED.levels, geom = EXCLUDED.geom
        """, (osm_id, levels, geojson))
        inserted += 1

    conn.commit()
    print(f"  Inserted {inserted:,} OSM buildings with levels")

    # Create spatial index
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_osm_buildings_levels_geom "
        "ON osm_buildings_levels USING GIST (geom)"
    )
    conn.commit()

    # Spatial join to building tables
    for table in ["gba_buildings", "vida_buildings"]:
        if not table_exists(cur, table):
            continue
        # gba_buildings has centroid column; vida_buildings needs ST_Centroid
        centroid_expr = "b.centroid" if table == "gba_buildings" else "ST_Centroid(b.geom)"
        cur.execute(f"""
            UPDATE {table} b SET osm_levels = o.levels
            FROM osm_buildings_levels o
            WHERE ST_Intersects({centroid_expr}, o.geom)
        """)
        updated = cur.rowcount
        conn.commit()
        print(f"  {table}: matched osm_levels on {updated:,} buildings")

    cur.close()
    conn.close()


# ── Phase 3: Population proxy (server-side) ─────────────────────────────────

def phase_proxy():
    """Compute proxy height from population and building area."""
    print("=== Phase 3: Population proxy (server-side) ===")

    conn = get_conn()
    cur = conn.cursor()

    for table, area_col in [("gba_buildings", "area_m2"),
                            ("vida_buildings", "area_in_meters")]:
        if not table_exists(cur, table):
            continue

        cur.execute(f"""
            UPDATE {table} b SET proxy_height_m = LEAST(GREATEST(
                c.total_personas * (b.volume_m3 / NULLIF(rv.total_vol, 0)) * 75.0
                / NULLIF(b.{area_col}, 0), 5), 30)
            FROM radio_stats_master c
            JOIN (SELECT redcode, SUM(volume_m3) AS total_vol
                  FROM {table}
                  WHERE volume_m3 > 0
                  GROUP BY redcode) rv ON c.redcode = rv.redcode
            WHERE b.redcode = c.redcode AND b.{area_col} > 0
        """)
        updated = cur.rowcount
        conn.commit()
        print(f"  {table}: computed proxy_height_m on {updated:,} buildings")

    cur.close()
    conn.close()


# ── Phase 4: Cascade best_height_m ──────────────────────────────────────────

def phase_cascade():
    """Compute best_height_m using priority cascade."""
    print("=== Phase 4: Cascade best_height_m ===")

    conn = get_conn()
    cur = conn.cursor()

    # GBA buildings (have real height_m from deep learning)
    if table_exists(cur, "gba_buildings"):
        cur.execute("""
            UPDATE gba_buildings SET best_height_m = COALESCE(
                osm_levels * 3.0,
                NULLIF(ghsl_height_m, 0),
                CASE WHEN height_m > 0 THEN height_m END,
                proxy_height_m,
                5.0
            )
        """)
        updated = cur.rowcount
        conn.commit()
        print(f"  gba_buildings: set best_height_m on {updated:,} buildings")

    # VIDA buildings (height_m is always 5m default, skip it in cascade)
    if table_exists(cur, "vida_buildings"):
        cur.execute("""
            UPDATE vida_buildings SET best_height_m = COALESCE(
                osm_levels * 3.0,
                NULLIF(ghsl_height_m, 0),
                proxy_height_m,
                5.0
            )
        """)
        updated = cur.rowcount
        conn.commit()
        print(f"  vida_buildings: set best_height_m on {updated:,} buildings")

    cur.close()
    conn.close()


# ── Phase 5: Summary ────────────────────────────────────────────────────────

def phase_summary():
    """Print stats by height source."""
    print("=== Phase 5: Summary ===")

    conn = get_conn()
    cur = conn.cursor()

    for table in ["gba_buildings", "vida_buildings"]:
        if not table_exists(cur, table):
            continue

        height_col = "height_m" if table == "gba_buildings" else "height_m"

        cur.execute(f"""
            SELECT
                CASE
                    WHEN osm_levels IS NOT NULL THEN 'osm'
                    WHEN ghsl_height_m IS NOT NULL AND ghsl_height_m > 0 THEN 'ghsl'
                    WHEN {height_col} IS NOT NULL AND {height_col} > 0
                         AND '{table}' = 'gba_buildings' THEN 'gba'
                    WHEN proxy_height_m IS NOT NULL THEN 'proxy'
                    ELSE 'default'
                END AS source,
                COUNT(*) AS cnt,
                ROUND(AVG(best_height_m)::numeric, 1) AS avg_h,
                ROUND(MIN(best_height_m)::numeric, 1) AS min_h,
                ROUND(MAX(best_height_m)::numeric, 1) AS max_h
            FROM {table}
            GROUP BY 1
            ORDER BY cnt DESC
        """)
        rows = cur.fetchall()

        print(f"\n  {table}:")
        print(f"  {'source':<10} {'count':>10} {'avg_h':>8} {'min':>6} {'max':>6}")
        print(f"  {'-'*10} {'-'*10} {'-'*8} {'-'*6} {'-'*6}")
        for source, cnt, avg_h, min_h, max_h in rows:
            print(f"  {source:<10} {cnt:>10,} {avg_h:>8} {min_h:>6} {max_h:>6}")

    cur.close()
    conn.close()


# ── Main ─────────────────────────────────────────────────────────────────────

ALL_PHASES = {
    "setup": phase_setup,
    "ghsl": phase_ghsl,
    "osm": phase_osm,
    "proxy": phase_proxy,
    "cascade": phase_cascade,
    "summary": phase_summary,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enrich buildings with multi-source height estimates"
    )
    parser.add_argument(
        "--phase",
        choices=list(ALL_PHASES.keys()),
        help="Run only a specific phase (default: all)",
    )
    args = parser.parse_args()

    t_start = time.time()

    if args.phase:
        ALL_PHASES[args.phase]()
    else:
        for name, fn in ALL_PHASES.items():
            fn()
            print()

    print(f"\nDone ({time.time() - t_start:.0f}s total)")
