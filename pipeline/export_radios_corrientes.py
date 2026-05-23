"""
Export AR-province radios from posadas PostGIS DB to GeoParquet.

Supports Corrientes (codprov=18), Chaco (codprov=22), Formosa (codprov=34)
via --territory flag, reading codprov from TERRITORY_CONFIGS.

Output: pipeline/output/<territory>/radios_<territory>.parquet
Backward-compatible: defaults to --territory corrientes (legacy filename kept).

Usage:
  python pipeline/export_radios_corrientes.py
  python pipeline/export_radios_corrientes.py --territory chaco
  python pipeline/export_radios_corrientes.py --territory formosa
"""

import argparse
import os
import sys

import geopandas as gpd
import psycopg2
from shapely import wkb

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import get_territory

PG_CENSUS = "dbname=posadas user=postgres"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", default="corrientes",
                    help="AR territory id from TERRITORY_CONFIGS (corrientes|chaco|formosa)")
    args = ap.parse_args()

    t = get_territory(args.territory)
    if t.get('country') != 'ar':
        raise SystemExit(f"--territory {args.territory} is not an AR province (country={t.get('country')})")
    codprov = t.get('codprov_indec')
    if codprov is None:
        raise SystemExit(f"TERRITORY_CONFIGS[{args.territory}] missing codprov_indec — add it to config.py")
    codprov_str = str(codprov).zfill(2)

    output_dir = os.path.join(SCRIPT_DIR, "output", args.territory)
    output_path = os.path.join(output_dir, f"radios_{args.territory}.parquet")
    os.makedirs(output_dir, exist_ok=True)

    print(f"Exporting radios for {t['label']} (codprov={codprov_str})")
    print(f"Connecting to posadas DB...")
    conn = psycopg2.connect(PG_CENSUS)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT redcode,
                   ST_AsBinary(geom) AS geom_wkb,
                   codprov,
                   dpto,
                   radios_pob,
                   radios_hog,
                   COALESCE(radios_sup, 0) AS area_km2
            FROM censo_2022.radios_geom
            WHERE codprov = %s AND geom IS NOT NULL AND redcode IS NOT NULL
        """, (codprov_str,))
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    print(f"  -> {len(rows)} radios fetched")
    if len(rows) == 0:
        raise SystemExit(
            f"No radios for codprov={codprov_str}. Verify censo_2022.radios_geom "
            f"has data loaded for this province (INDEC shapefile import)."
        )

    records = []
    for redcode, geom_wkb, codprov_col, dpto, pob, hog, area_km2 in rows:
        geom = wkb.loads(bytes(geom_wkb))
        records.append({
            "redcode": redcode,
            "geometry": geom,
            "codprov": codprov_col,
            "dpto": dpto or "",
            "total_personas": int(pob) if pob else 0,
            "total_hogares": int(hog) if hog else 0,
            "area_km2": float(area_km2) if area_km2 else 0.0,
        })

    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
    gdf.to_parquet(output_path, index=False)

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"  -> Saved {output_path} ({size_mb:.1f} MB, {len(gdf)} radios)")
    print(f"  -> Dptos: {gdf['dpto'].nunique()} unique")


if __name__ == "__main__":
    main()
