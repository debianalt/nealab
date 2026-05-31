"""
process_br_census.py
Brazil dasymetric population: assign IBGE setor population (setores_br, loaded by
load_ibge_setores.py) to GBA buildings, residential-gated, same rule as AR.

Per territory (parana_br=UF41, santa_catarina_br=42, rio_grande_sul_br=43):
  1. add columns + centroid on gba_buildings_<t>
  2. spatial-join building centroid -> setor (redcode = cd_setor)
  3. classify residential (rule: default-res + OSM non_res / area>700 / area<15)
     — run enrich_osm_types_territory.py --territory <t> BEFORE this for OSM types.
  4. allocate setor population (v0001) + domicílios (v0002) to residential buildings,
     weights = is_res * area, largest remainder, exact per-setor totals.

Usage:
  python process_br_census.py --territory santa_catarina_br
"""
import argparse
import os
import sys
import time

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras as ex
from sqlalchemy import create_engine

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import get_territory

PG = "dbname=ndvi_misiones user=postgres"
PG_URL = "postgresql://postgres@localhost:5432/ndvi_misiones"
UF = {"parana_br": "41", "santa_catarina_br": "42", "rio_grande_sul_br": "43"}


def _lr(total, w):
    s = w.sum()
    if s == 0 or total == 0:
        return np.zeros(len(w), dtype=int)
    raw = w * total / s; fl = np.floor(raw).astype(int); d = total - fl.sum()
    if d > 0:
        fl[np.argsort(-(raw - fl))[:d]] += 1
    return fl


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", required=True)
    args = ap.parse_args()
    t = args.territory
    get_territory(t)
    uf = UF[t]
    table = f"gba_buildings_{t}"
    con = psycopg2.connect(PG); con.autocommit = True; cur = con.cursor()

    print(f"=== {t} (UF {uf}) ===")
    for c, d in [("osm_building_type", "TEXT"), ("is_residential", "BOOLEAN"),
                 ("in_renabap", "BOOLEAN DEFAULT FALSE"), ("est_hogares", "INTEGER DEFAULT 0"),
                 ("est_viviendas", "INTEGER DEFAULT 0"), ("centroid", "geometry(Point,4326)")]:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {c} {d}")
    t0 = time.time()
    cur.execute(f"UPDATE {table} SET centroid=ST_Centroid(geom) WHERE centroid IS NULL")
    cur.execute(f"CREATE INDEX IF NOT EXISTS {table}_centroid_idx ON {table} USING GIST(centroid)")
    print(f"  centroid ({time.time()-t0:.0f}s)")

    # Spatial join: building centroid -> setor (only this UF's setores)
    print("  spatial join building -> setor...")
    t0 = time.time()
    cur.execute(f"""
        UPDATE {table} b SET redcode = s.cd_setor
        FROM setores_br s
        WHERE s.uf = %s AND ST_Contains(s.geom, b.centroid)
    """, (uf,))
    cur.execute(f"SELECT COUNT(*) FILTER (WHERE redcode IS NOT NULL), COUNT(*) FROM {table}")
    m, tot = cur.fetchone()
    print(f"  matched {m:,}/{tot:,} ({100*m/tot:.1f}%) to setores ({time.time()-t0:.0f}s)")

    # Classify (rule)
    cur.execute(f"""
        UPDATE {table} SET is_residential = CASE
            WHEN osm_building_type IN ('apartments','mixed_use') THEN TRUE
            WHEN osm_building_type = 'non_residential' THEN FALSE
            WHEN area_m2 > 700 OR area_m2 < 15 THEN FALSE
            ELSE TRUE END
        WHERE area_m2 IS NOT NULL
    """)
    cur.execute(f"SELECT COUNT(*) FILTER (WHERE is_residential), COUNT(*) FROM {table} WHERE area_m2 IS NOT NULL")
    r, tot2 = cur.fetchone()
    print(f"  residential={r:,}/{tot2:,} ({100*r/tot2:.1f}%)")

    # Allocate from setores_br
    print("  allocate...")
    eng = create_engine(PG_URL)
    census = pd.read_sql(f"SELECT cd_setor, total_personas, total_domicilios "
                         f"FROM setores_br WHERE uf='{uf}'", eng).set_index("cd_setor")
    print(f"  setores={len(census):,}  personas={int(census['total_personas'].sum()):,}")
    b = pd.read_sql(f"SELECT gid, redcode, area_m2, COALESCE(is_residential,TRUE) is_residential "
                    f"FROM {table} WHERE area_m2>0 AND redcode IS NOT NULL", eng)
    cur.execute(f"UPDATE {table} SET est_personas=0, est_hogares=0, est_viviendas=0")
    updates = []; n_edge = 0; t0 = time.time()
    for rc, g in b.groupby("redcode"):
        if rc not in census.index:
            continue
        row = census.loc[rc]
        per = int(row["total_personas"] or 0); dom = int(row["total_domicilios"] or 0)
        area = g["area_m2"].values.astype(np.float64)
        isr = g["is_residential"].values.astype(bool)
        w = isr * area
        if w.sum() == 0 and (per > 0 or dom > 0):
            n_edge += 1
            w = (area > 15).astype(float) * area
            if w.sum() == 0:
                w = np.ones(len(g))
        ap_ = _lr(per, w); ah = _lr(dom, w); ids = g["gid"].values
        for i in range(len(ids)):
            if ap_[i] > 0 or ah[i] > 0:
                updates.append((int(ap_[i]), int(ah[i]), int(ah[i]), int(ids[i])))
        if len(updates) >= 20000:
            ex.execute_batch(cur, f"UPDATE {table} SET est_personas=%s, est_hogares=%s, "
                             f"est_viviendas=%s WHERE gid=%s", updates, page_size=2000)
            updates = []
    if updates:
        ex.execute_batch(cur, f"UPDATE {table} SET est_personas=%s, est_hogares=%s, "
                         f"est_viviendas=%s WHERE gid=%s", updates, page_size=2000)
    cur.execute(f"SELECT SUM(est_personas), COUNT(*) FILTER (WHERE est_personas>0) FROM {table}")
    sp, nb = cur.fetchone()
    print(f"  allocated: sum_personas={int(sp or 0):,}  buildings_with_pop={nb:,}  "
          f"edge_setores={n_edge}  ({time.time()-t0:.0f}s)")
    cur.close(); con.close()
    print(f"DONE {t}")


if __name__ == "__main__":
    main()
