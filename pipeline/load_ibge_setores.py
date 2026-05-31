"""
load_ibge_setores.py
Load IBGE 2022 census-tract (setor censitário) geometry + population into PostGIS
for PR/SC/RS, the Brazil analogue of INDEC radios. Reprojects SIRGAS 2000
(EPSG:4674) → 4326. Population (v0001) and domicílios (v0002) from the national
'básico' aggregates, filtered to CD_SETOR prefixes 41(PR)/42(SC)/43(RS).

Output PostGIS table: setores_br (cd_setor PK, uf, geom, total_personas, total_domicilios)

Usage:
  python load_ibge_setores.py
"""
import csv
import glob
import os
import time
import zipfile

import geopandas as gpd
import psycopg2
import psycopg2.extras as ex

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
IBGE = os.path.join(SCRIPT_DIR, "data", "ibge")
PG = "dbname=ndvi_misiones user=postgres"


def main():
    con = psycopg2.connect(PG); con.autocommit = True; cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS setores_br")
    cur.execute("""CREATE TABLE setores_br (
        cd_setor TEXT PRIMARY KEY, uf TEXT, geom geometry(MultiPolygon,4326),
        total_personas INT DEFAULT 0, total_domicilios INT DEFAULT 0)""")

    # 1. Read each UF shapefile with geopandas (own PROJ via pyproj), reproject
    # to 4326, insert geom as WKB hex (bypasses ogr2ogr's broken proj.db).
    from shapely import wkb as shwkb
    for uf in ["PR", "SC", "RS"]:
        d = os.path.join(IBGE, f"{uf.lower()}_shp")
        if not glob.glob(os.path.join(d, "*.shp")):
            with zipfile.ZipFile(os.path.join(IBGE, f"{uf}_setores_CD2022.zip")) as z:
                z.extractall(d)
        shp = glob.glob(os.path.join(d, "*.shp"))[0]
        t0 = time.time()
        g = gpd.read_file(shp)
        if g.crs is None:
            g = g.set_crs(4674)
        g = g.to_crs(4326)
        cdcol = "CD_SETOR" if "CD_SETOR" in g.columns else "cd_setor"
        rows = []
        for cd, geom in zip(g[cdcol].astype(str), g.geometry):
            if geom is None or geom.is_empty:
                continue
            if geom.geom_type == "Polygon":
                from shapely.geometry import MultiPolygon
                geom = MultiPolygon([geom])
            rows.append((str(cd), str(cd)[:2], geom.wkb_hex))
        ex.execute_batch(cur,
            "INSERT INTO setores_br (cd_setor, uf, geom) "
            "VALUES (%s,%s, ST_GeomFromWKB(decode(%s,'hex'),4326)) "
            "ON CONFLICT (cd_setor) DO NOTHING",
            rows, page_size=2000)
        print(f"  {uf}: {len(rows):,} setores ({time.time()-t0:.0f}s)")

    cur.execute("CREATE INDEX setores_br_geom_idx ON setores_br USING GIST(geom)")
    cur.execute("SELECT uf, COUNT(*) FROM setores_br GROUP BY uf ORDER BY uf")
    print("  setores by UF:", cur.fetchall())

    # 2. Population from básico (v0001=pessoas, v0002=domicílios), prefixes 41/42/43
    print("  loading básico population...")
    csvf = glob.glob(os.path.join(IBGE, "basico", "*.csv"))[0]
    rows = []
    with open(csvf, encoding="latin-1") as f:
        for row in csv.DictReader(f, delimiter=";"):
            cd = row["CD_SETOR"]
            if cd[:2] in ("41", "42", "43"):
                def iv(v):
                    try: return int(float(str(v).replace(",", ".")))
                    except Exception: return 0
                rows.append((iv(row.get("v0001")), iv(row.get("v0002")), cd))
    print(f"    {len(rows):,} setor population rows (PR/SC/RS)")
    ex.execute_batch(cur,
        "UPDATE setores_br SET total_personas=%s, total_domicilios=%s WHERE cd_setor=%s",
        rows, page_size=5000)
    cur.execute("SELECT uf, SUM(total_personas) FROM setores_br GROUP BY uf ORDER BY uf")
    print("  population by UF:", cur.fetchall())
    cur.close(); con.close()
    print("DONE")


if __name__ == "__main__":
    main()
