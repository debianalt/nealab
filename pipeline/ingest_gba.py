"""
Ingest Global Building Atlas (GBA, Zhu et al. 2025, ESSD) footprints + heights
into the canonical PostGIS buildings table for a territory.

WHY: only Misiones currently has true GBA data (ndvi_misiones.gba_buildings).
Corrientes/Itapúa/Alto Paraná were built from Overture only (import_gba_*.py is
misnamed — it uses Overture), so their buildings lack consistent modeled height
→ the Posadas≠Encarnación asymmetry. This script is the missing, reproducible
GBA acquisition step. It ONLY swaps the footprint source; the downstream
census-anchored dasymetric est_personas join and the PMTiles builders are
reused unchanged (they consume the same gba_buildings_<territory> schema).

SOURCE: GBA.Polygon via the Earth Engine community mirror
  projects/sat-io/open-datasets/GLOBAL_BUILDING_ATLAS/<5°-tile>
Each tile is a FeatureCollection with a per-polygon 'height' property (metres).
LICENSE: CC BY-NC 4.0 (non-commercial). Academic/non-commercial use only —
see the project licensing note before any production/redistribution step.

PIPELINE POSITION (Phase 1):
  python pipeline/ingest_gba.py --territory corrientes        # → PostGIS
  # then the EXISTING, unchanged downstream:
  #   AR  : the radio sjoin/est_personas in import_gba_corrientes.spatial_join...
  #   then build_corrientes_buildings.py → corrientes_buildings.pmtiles (local)
  # Validate locally vs Misiones BEFORE any R2/deploy (Phase 3).

Usage:
  python pipeline/ingest_gba.py --territory corrientes [--dry-run] [--keep-gcs]

Requires: earthengine-api (authed), gcloud CLI, GDAL/ogr2ogr, psycopg2.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time

import ee
import psycopg2

from config import GCS_BUCKET, get_territory

# Windows consoles default to cp1252 and choke on non-ASCII prints; force UTF-8.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# Same DB the existing builders/joins read (import_gba_corrientes.py:48).
PG_BUILDINGS = "dbname=ndvi_misiones user=postgres"

GBA_ROOT = "projects/sat-io/open-datasets/GLOBAL_BUILDING_ATLAS"
# Tile ids look like "e030_n25_e035_n20" / "w060_s30_w055_s25":
#   <hemi><deg>_<hemi><deg>_<hemi><deg>_<hemi><deg>  (lon,lat,lon,lat), 5° grid.
_TILE_RE = re.compile(
    r"([ew])(\d{1,3})_([ns])(\d{1,2})_([ew])(\d{1,3})_([ns])(\d{1,2})", re.I
)


def authenticate() -> None:
    """GEE auth — service account in CI, user credentials locally
    (mirrors gee_export_analysis.authenticate)."""
    key_env = os.environ.get("GEE_SERVICE_ACCOUNT_KEY", "")
    if not key_env:
        ee.Initialize()
        return
    key_data = json.load(open(key_env)) if os.path.isfile(key_env) else json.loads(key_env)
    creds = ee.ServiceAccountCredentials(key_data["client_email"], key_data=json.dumps(key_data))
    ee.Initialize(creds, opt_url="https://earthengine-highvolume.googleapis.com")


def _lon(hemi: str, deg: str) -> float:
    return (1 if hemi.lower() == "e" else -1) * int(deg)


def _lat(hemi: str, deg: str) -> float:
    return (1 if hemi.lower() == "n" else -1) * int(deg)


def tiles_for_bbox(bbox: list[float]) -> list[str]:
    """Return GBA tile asset ids whose extent intersects bbox [W,S,E,N]."""
    w, s, e, n = bbox
    children = ee.data.listAssets({"parent": GBA_ROOT}).get("assets", [])
    hits: list[str] = []
    for a in children:
        aid = a["id"] if "id" in a else a["name"]
        short = aid.rstrip("/").split("/")[-1]
        m = _TILE_RE.search(short)
        if not m:
            continue
        lon1 = _lon(m.group(1), m.group(2))
        lat1 = _lat(m.group(3), m.group(4))
        lon2 = _lon(m.group(5), m.group(6))
        lat2 = _lat(m.group(7), m.group(8))
        tw, te = min(lon1, lon2), max(lon1, lon2)
        ts, tn = min(lat1, lat2), max(lat1, lat2)
        # bbox-vs-tile intersection
        if tw <= e and te >= w and ts <= n and tn >= s:
            hits.append(aid)
    return hits


def build_collection(tile_ids: list[str], bbox: list[float]) -> ee.FeatureCollection:
    """Merge the covering tiles, clip to bbox, keep only the height property."""
    w, s, e, n = bbox
    region = ee.Geometry.Rectangle([w, s, e, n], proj="EPSG:4326", geodesic=False)
    fcs = [ee.FeatureCollection(tid).filterBounds(region) for tid in tile_ids]
    merged = ee.FeatureCollection(fcs).flatten()

    def _keep_height(f: ee.Feature) -> ee.Feature:
        h = ee.Number(ee.Algorithms.If(f.get("height"), f.get("height"), 0))
        return ee.Feature(f.geometry(), {"height": h})

    return merged.map(_keep_height)


def export_to_gcs(fc: ee.FeatureCollection, territory: str) -> str:
    """Start a GCS export task and block until it finishes. Returns the gs prefix."""
    prefix = f"gba/{territory}/gba_{territory}"
    desc = f"gba_ingest_{territory}"

    # Idempotent re-runs: cancel any prior task with the same description
    # (e.g. an orphan from a crashed run) so it can't write to the prefix
    # after we clean it, then wipe the GCS prefix.
    for t in ee.batch.Task.list():
        try:
            st = t.status()
            if st.get("description") == desc and st.get("state") in ("READY", "RUNNING"):
                print(f"  Cancelling prior task {t.id} ({st.get('state')})")
                t.cancel()
        except Exception:
            pass
    subprocess.run(["gcloud", "storage", "rm", f"gs://{GCS_BUCKET}/{prefix}*"], check=False)

    task = ee.batch.Export.table.toCloudStorage(
        collection=fc,
        description=desc,
        bucket=GCS_BUCKET,
        fileNamePrefix=prefix,
        fileFormat="GeoJSON",
    )
    task.start()
    print(f"  GEE export task started: {task.id} -> gs://{GCS_BUCKET}/{prefix}*")
    while True:
        st = task.status()
        state = st.get("state")
        if state in ("COMPLETED",):
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"GEE export {state}: {st.get('error_message')}")
        print(f"    ...{state}")
        time.sleep(30)
    return f"gs://{GCS_BUCKET}/{prefix}"


def load_postgis(gcs_prefix: str, territory: str, keep_gcs: bool) -> None:
    """Download the exported GeoJSON and load it into gba_buildings_<territory>."""
    table = f"gba_buildings_{territory}"
    tmpdir = tempfile.mkdtemp(prefix=f"gba_{territory}_")
    print(f"  Downloading {gcs_prefix}* → {tmpdir}")
    subprocess.run(
        ["gcloud", "storage", "cp", f"{gcs_prefix}*", tmpdir + "/"],
        check=True,
    )
    files = [os.path.join(tmpdir, f) for f in os.listdir(tmpdir) if f.endswith(".geojson")]
    if not files:
        raise RuntimeError(f"No GeoJSON downloaded from {gcs_prefix}")

    # Recreate the table with the SCHEMA the existing builders/joins expect
    # (mirrors import_gba_corrientes.py: geom, area_m2, best_height_m, redcode,
    #  est_personas). redcode/est_personas are filled by the existing
    #  census-anchored dasymetric join, NOT here — source swap only.
    with psycopg2.connect(PG_BUILDINGS) as conn, conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"""
            CREATE TABLE {table} (
                gid           SERIAL PRIMARY KEY,
                geom          geometry(Geometry, 4326),
                area_m2       FLOAT,
                best_height_m FLOAT DEFAULT 5.0,
                redcode       VARCHAR,
                est_personas  FLOAT DEFAULT 0
            )
        """)
        conn.commit()

    for i, fp in enumerate(files):
        print(f"  ogr2ogr load [{i + 1}/{len(files)}] {os.path.basename(fp)}")
        subprocess.run([
            "ogr2ogr", "-f", "PostgreSQL",
            f"PG:{PG_BUILDINGS}", fp,
            "-nln", table, "-append",
            "-nlt", "PROMOTE_TO_MULTI",
            "-lco", "GEOMETRY_NAME=geom",
            "-sql", 'SELECT height AS best_height_m FROM "OGRGeoJSON"',
        ], check=True)

    with psycopg2.connect(PG_BUILDINGS) as conn, conn.cursor() as cur:
        # area in m² from the geography cast; height floor 1.5 m like the join.
        cur.execute(f"UPDATE {table} SET area_m2 = ST_Area(geom::geography)")
        cur.execute(f"UPDATE {table} SET best_height_m = 5.0 "
                    f"WHERE best_height_m IS NULL OR best_height_m < 1.5")
        cur.execute(f"CREATE INDEX {table}_geom_idx ON {table} USING GIST(geom)")
        cur.execute(f"CREATE INDEX {table}_redcode_idx ON {table}(redcode)")
        cur.execute(f"SELECT COUNT(*), ROUND(AVG(best_height_m)::numeric,1), "
                    f"ROUND(AVG(area_m2)::numeric,1) FROM {table}")
        cnt, avg_h, avg_a = cur.fetchone()
        conn.commit()
    print(f"  {table}: {cnt:,} buildings | avg height {avg_h} m | avg area {avg_a} m²")

    if not keep_gcs:
        subprocess.run(["gcloud", "storage", "rm", f"{gcs_prefix}*"], check=False)


def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest GBA footprints+height → PostGIS")
    ap.add_argument("--territory", required=True,
                    help="territory id from config.TERRITORY_CONFIGS (e.g. corrientes)")
    ap.add_argument("--dry-run", action="store_true",
                    help="only resolve covering tiles, no export/load")
    ap.add_argument("--keep-gcs", action="store_true",
                    help="don't delete the GCS export after loading")
    args = ap.parse_args()

    cfg = get_territory(args.territory)  # raises with helpful message if unknown
    bbox = cfg["bbox"]
    print(f"Territory: {args.territory}  bbox(WSEN)={bbox}")

    authenticate()
    tiles = tiles_for_bbox(bbox)
    if not tiles:
        print("ERROR: no GBA tiles intersect this bbox", file=sys.stderr)
        return 1
    print(f"  GBA tiles covering bbox ({len(tiles)}):")
    for t in tiles:
        print(f"    {t}")
    if args.dry_run:
        print("dry-run: stopping before export.")
        return 0

    fc = build_collection(tiles, bbox)
    gcs_prefix = export_to_gcs(fc, args.territory)
    load_postgis(gcs_prefix, args.territory, args.keep_gcs)

    print("\nDONE. Next (unchanged downstream, local validation — NO R2 yet):")
    print(f"  - run the existing census-anchored est_personas join for "
          f"{args.territory} (radio INDEC for AR / distrito DGEEC for PY)")
    print(f"  - build_{args.territory}_buildings.py → PMTiles (local)")
    print(f"  - compare height/est_personas/coverage vs Misiones before Phase 3")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
