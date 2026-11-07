"""
enrich_osm_types_territory.py
Generalized OSM building-type enrichment for any territory with a
gba_buildings_<territory> table (centroid column). Mirrors the Misiones
enrich_building_types.py classify() rules; bbox from config.TERRITORY_CONFIGS.

Fetches tagged buildings from Overpass for the territory bbox → classifies into
non_residential / apartments / mixed_use → spatial-joins to gba_buildings_<t>.

With --levels it instead fetches building:levels (same Overpass query and centroid join as
enrich_heights.py phase_osm for Misiones) → osm_levels on gba_buildings_<t>, the floors that
allocate_population.py trains and allocates with (7-nov-2026).

Usage:
  python enrich_osm_types_territory.py --territory corrientes
  python enrich_osm_types_territory.py --territory corrientes --levels
"""
import argparse
import json
import os
import sys
import time

import psycopg2
import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import get_territory

DB = "dbname=ndvi_misiones host=localhost port=5432 user=postgres"

NON_RESIDENTIAL_TAGS = {
    'commercial', 'retail', 'industrial', 'warehouse',
    'church', 'chapel', 'cathedral', 'mosque',
    'school', 'university', 'college', 'kindergarten',
    'hospital', 'clinic', 'public', 'government', 'civic',
    'hotel', 'supermarket', 'office',
    'train_station', 'transportation', 'hangar',
    'garage', 'garages', 'shed', 'barn', 'farm_auxiliary', 'greenhouse',
    'construction', 'ruins',
    'service', 'kiosk', 'roof', 'tank', 'silo', 'bunker', 'toilets',
}


def classify(tags):
    b = tags.get('building', '')
    if b == 'mixed_use':
        return 'mixed_use'
    if b == 'apartments':
        return 'apartments'
    if b in NON_RESIDENTIAL_TAGS:
        return 'non_residential'
    if tags.get('amenity') or tags.get('shop') or tags.get('office'):
        return 'non_residential'
    if tags.get('disused') == 'yes':
        return 'non_residential'
    return None


MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.fr/api/interpreter",
]
HEADERS = {"User-Agent": "spatia-territorial/1.0 (research)"}


def overpass(query: str, what: str) -> list:
    print(f"Overpass query for {what}...")
    t0 = time.time()
    for url in MIRRORS:
        try:
            resp = requests.post(url, data={"data": query}, headers=HEADERS, timeout=900)
            resp.raise_for_status()
            els = resp.json().get("elements", [])
            print(f"  {len(els):,} ways via {url.split('/')[2]} ({time.time()-t0:.0f}s)")
            return els
        except Exception as ex:
            print(f"  {url.split('/')[2]} failed: {str(ex)[:80]}")
            time.sleep(5)
    sys.exit("ERROR: all Overpass mirrors failed")


def ring(el) -> str | None:
    nodes = el.get("geometry", [])
    if len(nodes) < 4:
        return None
    coords = [[nd["lon"], nd["lat"]] for nd in nodes]
    if coords[0] != coords[-1]:
        coords.append(coords[0])
    return json.dumps({"type": "Polygon", "coordinates": [coords]})


def levels(t: str, bbox) -> None:
    """building:levels → osm_levels (REAL), by the building centroid inside the OSM polygon."""
    w, s, e, n = bbox
    table = f"gba_buildings_{t}"
    stage = f"osm_building_levels_{t}"
    els = overpass(f"""[out:json][timeout:600];
    ( way["building"]["building:levels"]({s},{w},{n},{e}); );
    out body geom;""", f"{t} building:levels")
    conn = psycopg2.connect(DB); cur = conn.cursor()
    cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS osm_levels REAL")
    cur.execute(f"DROP TABLE IF EXISTS {stage}")
    cur.execute(f"CREATE TABLE {stage} (osm_id BIGINT PRIMARY KEY, levels REAL, geom geometry(Geometry,4326))")
    ins = 0
    for el in els:
        try:
            lv = float(str(el.get("tags", {}).get("building:levels", "")).replace(",", "."))
        except ValueError:
            continue
        gj = ring(el)
        if not (0 < lv <= 80) or gj is None:
            continue
        cur.execute(f"""INSERT INTO {stage}(osm_id, levels, geom)
            VALUES (%s,%s,ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(%s),4326)))
            ON CONFLICT (osm_id) DO NOTHING""", (el["id"], lv, gj))
        ins += 1
    conn.commit()
    cur.execute(f"CREATE INDEX ON {stage} USING GIST(geom)")
    cur.execute(f"UPDATE {table} SET osm_levels = NULL")
    cur.execute(f"""UPDATE {table} b SET osm_levels = o.lv FROM (
            SELECT b2.gid, max(st.levels) lv FROM {table} b2 JOIN {stage} st ON ST_Intersects(b2.centroid, st.geom)
            GROUP BY b2.gid) o
        WHERE b.gid = o.gid""")
    print(f"  {ins:,} OSM buildings with levels staged; osm_levels on {cur.rowcount:,} buildings")
    conn.commit()
    cur.execute(f"SELECT osm_levels, count(*) FROM {table} WHERE osm_levels IS NOT NULL GROUP BY 1 ORDER BY 1")
    print("    " + ", ".join(f"{int(l) if l == int(l) else l}: {c:,}" for l, c in cur.fetchall()))
    cur.close(); conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", required=True)
    ap.add_argument("--levels", action="store_true", help="building:levels → osm_levels instead of types")
    args = ap.parse_args()
    t = args.territory
    cfg = get_territory(t)
    if args.levels:
        return levels(t, cfg["bbox"])
    w, s, e, n = cfg["bbox"]
    table = f"gba_buildings_{t}"
    stage = f"osm_building_tags_{t}"

    conn = psycopg2.connect(DB); conn.autocommit = False; cur = conn.cursor()
    cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS osm_building_type TEXT")
    cur.execute(f"DROP TABLE IF EXISTS {stage}")
    cur.execute(f"""CREATE TABLE {stage} (
        osm_id BIGINT PRIMARY KEY, dwelling_class TEXT, geom geometry(Polygon,4326))""")
    conn.commit()

    btypes = ("apartments|mixed_use|commercial|retail|industrial|warehouse|church|chapel|"
              "cathedral|mosque|school|university|college|kindergarten|hospital|clinic|public|"
              "government|civic|hotel|supermarket|office|train_station|transportation|hangar|"
              "garage|garages|shed|barn|farm_auxiliary|greenhouse|construction|ruins|service|"
              "kiosk|roof|tank|silo|bunker|toilets")
    query = f"""[out:json][timeout:300];
    (
      way["building"~"{btypes}"]({s},{w},{n},{e});
      way["building"]["amenity"]({s},{w},{n},{e});
      way["building"]["shop"]({s},{w},{n},{e});
      way["building"]["office"]({s},{w},{n},{e});
      way["building"]["disused"="yes"]({s},{w},{n},{e});
    );
    out body geom;"""

    els = overpass(query, f"{t} bbox ({w},{s},{e},{n})")

    ins = 0
    for el in els:
        cls = classify(el.get("tags", {}))
        if cls is None:
            continue
        gj = ring(el)
        if gj is None:
            continue
        cur.execute(f"""INSERT INTO {stage}(osm_id, dwelling_class, geom)
            VALUES (%s,%s,ST_SetSRID(ST_GeomFromGeoJSON(%s),4326))
            ON CONFLICT (osm_id) DO UPDATE SET dwelling_class=EXCLUDED.dwelling_class, geom=EXCLUDED.geom""",
            (el["id"], cls, gj))
        ins += 1
    conn.commit()
    cur.execute(f"CREATE INDEX IF NOT EXISTS {stage}_geom_idx ON {stage} USING GIST(geom)")
    conn.commit()
    print(f"  {ins:,} classified buildings staged")

    cur.execute(f"""UPDATE {table} b SET osm_building_type = st.dwelling_class
        FROM {stage} st WHERE ST_Intersects(b.centroid, st.geom) AND st.dwelling_class IS NOT NULL""")
    print(f"  matched osm_building_type on {cur.rowcount:,} buildings")
    conn.commit()

    cur.execute(f"SELECT COALESCE(osm_building_type,'(none)'), COUNT(*) FROM {table} GROUP BY 1 ORDER BY 2 DESC")
    for cls, cnt in cur.fetchall():
        print(f"    {cls:<18} {cnt:>10,}")
    cur.close(); conn.close()


if __name__ == "__main__":
    main()
