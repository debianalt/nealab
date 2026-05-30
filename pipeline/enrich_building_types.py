"""
enrich_building_types.py
Enrich gba_buildings and vida_buildings with OSM building type classification
for improved dwelling-unit dasymetric allocation.

Classification:
  non_residential — commercial, institutional, industrial, sheds, construction, ruins,
                    anything with amenity/shop/office tags, disused buildings
  mixed_use       — OSM building=mixed_use (ground floor commercial + upper residential)
  apartments      — OSM building=apartments (2-floor PH/dúplex still multi-unit)
  NULL            — everything else (default houses)

Phases:
  setup   — ALTER TABLE + create staging table
  fetch   — Overpass query for tagged buildings in Misiones
  join    — Spatial join to building tables
  summary — Print classification counts

Usage:
    python enrich_building_types.py                # all phases
    python enrich_building_types.py --phase setup  # only ALTER TABLE
    python enrich_building_types.py --phase fetch  # only Overpass
    python enrich_building_types.py --phase join   # only spatial join
    python enrich_building_types.py --phase summary
"""

import argparse
import json
import time

import psycopg2
import requests

DB = "dbname=ndvi_misiones host=localhost port=5432 user=postgres"
MISIONES_BBOX = (-55.95, -28.17, -53.63, -25.47)

NON_RESIDENTIAL_TAGS = {
    'commercial', 'retail', 'industrial', 'warehouse',
    'church', 'chapel', 'cathedral', 'mosque',
    'school', 'university', 'college', 'kindergarten',
    'hospital', 'clinic',
    'public', 'government', 'civic',
    'hotel', 'supermarket', 'office',
    'train_station', 'transportation', 'hangar',
    'garage', 'garages', 'shed', 'barn', 'farm_auxiliary', 'greenhouse',
    'construction', 'ruins',
    'service', 'kiosk', 'roof', 'tank', 'silo', 'bunker', 'toilets',
}


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


def classify(tags):
    """Classify OSM tags into dwelling class."""
    building = tags.get('building', '')
    if building == 'mixed_use':
        return 'mixed_use'
    if building == 'apartments':
        return 'apartments'
    if building in NON_RESIDENTIAL_TAGS:
        return 'non_residential'
    if tags.get('amenity') or tags.get('shop') or tags.get('office'):
        return 'non_residential'
    if tags.get('disused') == 'yes':
        return 'non_residential'
    return None


# ── Phase: Setup ─────────────────────────────────────────────────────────────

def phase_setup():
    """Add osm_building_type column and create staging table."""
    print("=== Phase: Setup — ALTER TABLE + staging ===")
    conn = get_conn()
    cur = conn.cursor()

    for table in ["gba_buildings", "vida_buildings"]:
        if not table_exists(cur, table):
            print(f"  {table}: not found, skipping")
            continue
        cur.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS osm_building_type TEXT"
        )
        print(f"  {table}: osm_building_type column added")
    conn.commit()

    cur.execute("DROP TABLE IF EXISTS osm_building_tags")
    cur.execute("""
        CREATE TABLE osm_building_tags (
            osm_id BIGINT PRIMARY KEY,
            building_tag TEXT,
            amenity_tag TEXT,
            shop_tag TEXT,
            office_tag TEXT,
            disused TEXT,
            dwelling_class TEXT,
            geom GEOMETRY(Polygon, 4326)
        )
    """)
    conn.commit()
    print("  Staging table osm_building_tags created")

    cur.close()
    conn.close()


# ── Phase: Fetch ─────────────────────────────────────────────────────────────

def phase_fetch():
    """Fetch tagged buildings from Overpass API and classify."""
    print("=== Phase: Fetch — Overpass query ===")

    bbox = f"{MISIONES_BBOX[1]},{MISIONES_BBOX[0]},{MISIONES_BBOX[3]},{MISIONES_BBOX[2]}"

    building_types = (
        "apartments|mixed_use|commercial|retail|industrial|warehouse|"
        "church|chapel|cathedral|mosque|school|university|college|kindergarten|"
        "hospital|clinic|public|government|civic|hotel|supermarket|office|"
        "train_station|transportation|hangar|garage|garages|shed|barn|"
        "farm_auxiliary|greenhouse|construction|ruins|service|kiosk|roof|"
        "tank|silo|bunker|toilets"
    )

    query = f"""
    [out:json][timeout:180];
    (
      way["building"~"{building_types}"]({bbox});
      way["building"]["amenity"]({bbox});
      way["building"]["shop"]({bbox});
      way["building"]["office"]({bbox});
      way["building"]["disused"="yes"]({bbox});
    );
    out body geom;
    """

    print("  Querying Overpass API...")
    t0 = time.time()
    resp = requests.post(
        "https://overpass-api.de/api/interpreter",
        data={"data": query},
        timeout=300,
    )
    resp.raise_for_status()
    data = resp.json()
    elements = data.get("elements", [])
    print(f"  Received {len(elements):,} ways ({time.time() - t0:.0f}s)")

    if not elements:
        print("  No matching buildings found")
        return

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM osm_building_tags")
    conn.commit()

    inserted = 0
    skipped_geom = 0
    skipped_class = 0

    for el in elements:
        osm_id = el["id"]
        tags = el.get("tags", {})

        dwelling_class = classify(tags)
        if dwelling_class is None:
            skipped_class += 1
            continue

        geom_nodes = el.get("geometry", [])
        if len(geom_nodes) < 4:
            skipped_geom += 1
            continue

        coords = [[n["lon"], n["lat"]] for n in geom_nodes]
        if coords[0] != coords[-1]:
            coords.append(coords[0])

        geojson = json.dumps({"type": "Polygon", "coordinates": [coords]})

        cur.execute("""
            INSERT INTO osm_building_tags
                (osm_id, building_tag, amenity_tag, shop_tag, office_tag,
                 disused, dwelling_class, geom)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
            ON CONFLICT (osm_id) DO UPDATE SET
                dwelling_class = EXCLUDED.dwelling_class,
                geom = EXCLUDED.geom
        """, (
            osm_id,
            tags.get('building'),
            tags.get('amenity'),
            tags.get('shop'),
            tags.get('office'),
            tags.get('disused'),
            dwelling_class,
            geojson,
        ))
        inserted += 1

    conn.commit()
    print(f"  Inserted {inserted:,} classified buildings")
    if skipped_geom:
        print(f"  Skipped {skipped_geom:,} (invalid geometry)")
    if skipped_class:
        print(f"  Skipped {skipped_class:,} (no classification)")

    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_osm_building_tags_geom "
        "ON osm_building_tags USING GIST (geom)"
    )
    conn.commit()

    cur.close()
    conn.close()


# ── Phase: Join ──────────────────────────────────────────────────────────────

def phase_join():
    """Spatial join osm_building_tags to building tables."""
    print("=== Phase: Join — spatial join to building tables ===")

    conn = get_conn()
    cur = conn.cursor()

    for table in ["gba_buildings", "vida_buildings"]:
        if not table_exists(cur, table):
            print(f"  {table}: not found, skipping")
            continue

        centroid_expr = (
            "b.centroid" if table == "gba_buildings"
            else "ST_Centroid(b.geom)"
        )

        cur.execute(f"""
            UPDATE {table} b SET osm_building_type = t.dwelling_class
            FROM osm_building_tags t
            WHERE ST_Intersects({centroid_expr}, t.geom)
              AND t.dwelling_class IS NOT NULL
        """)
        updated = cur.rowcount
        conn.commit()
        print(f"  {table}: matched osm_building_type on {updated:,} buildings")

    cur.close()
    conn.close()


# ── Phase: Summary ───────────────────────────────────────────────────────────

def phase_summary():
    """Print classification counts per table."""
    print("=== Phase: Summary ===")

    conn = get_conn()
    cur = conn.cursor()

    # Staging table counts
    cur.execute("""
        SELECT dwelling_class, COUNT(*)
        FROM osm_building_tags
        GROUP BY dwelling_class ORDER BY COUNT(*) DESC
    """)
    rows = cur.fetchall()
    print("\n  osm_building_tags:")
    print(f"  {'class':<20} {'count':>8}")
    print(f"  {'-'*20} {'-'*8}")
    for cls, cnt in rows:
        print(f"  {cls or 'NULL':<20} {cnt:>8,}")

    # Per building table
    for table in ["gba_buildings", "vida_buildings"]:
        if not table_exists(cur, table):
            continue
        cur.execute(f"""
            SELECT COALESCE(osm_building_type, 'unclassified') AS cls, COUNT(*)
            FROM {table}
            GROUP BY osm_building_type ORDER BY COUNT(*) DESC
        """)
        rows = cur.fetchall()
        print(f"\n  {table}:")
        print(f"  {'type':<20} {'count':>10}")
        print(f"  {'-'*20} {'-'*10}")
        for cls, cnt in rows:
            print(f"  {cls:<20} {cnt:>10,}")

    cur.close()
    conn.close()


# ── Main ─────────────────────────────────────────────────────────────────────

ALL_PHASES = {
    "setup": phase_setup,
    "fetch": phase_fetch,
    "join": phase_join,
    "summary": phase_summary,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enrich buildings with OSM building type classification"
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
