"""
enrich_osm_types_territory.py
Generalized OSM building-type enrichment for any territory with a
gba_buildings_<territory> table (centroid column). Mirrors the Misiones
enrich_building_types.py classify() rules; bbox from config.TERRITORY_CONFIGS.

Fetches tagged buildings from Overpass for the territory bbox → classifies into
non_residential / apartments / mixed_use → spatial-joins to gba_buildings_<t>.

Usage:
  python enrich_osm_types_territory.py --territory corrientes
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", required=True)
    args = ap.parse_args()
    t = args.territory
    cfg = get_territory(t)
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

    print(f"Overpass query for {t} bbox ({w},{s},{e},{n})...")
    t0 = time.time()
    mirrors = [
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass.openstreetmap.fr/api/interpreter",
    ]
    headers = {"User-Agent": "spatia-territorial/1.0 (research; lsgomez001@gmail.com)"}
    els = None
    for url in mirrors:
        try:
            resp = requests.post(url, data={"data": query}, headers=headers, timeout=600)
            resp.raise_for_status()
            els = resp.json().get("elements", [])
            print(f"  {len(els):,} ways via {url.split('/')[2]} ({time.time()-t0:.0f}s)")
            break
        except Exception as ex:
            print(f"  {url.split('/')[2]} failed: {str(ex)[:80]}")
            time.sleep(5)
    if els is None:
        sys.exit("ERROR: all Overpass mirrors failed")

    ins = 0
    for el in els:
        cls = classify(el.get("tags", {}))
        if cls is None:
            continue
        nodes = el.get("geometry", [])
        if len(nodes) < 4:
            continue
        coords = [[nd["lon"], nd["lat"]] for nd in nodes]
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        gj = json.dumps({"type": "Polygon", "coordinates": [coords]})
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
