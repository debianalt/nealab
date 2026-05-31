"""
Build footprints-only buildings PMTiles for a Brazilian state at SCALE, using
tippecanoe (WSL) — the Python in-memory builder does not scale to 10M+ buildings.

Pipeline:
  1. Windows ogr2ogr (QGIS) streams gba_buildings_<territory> from PostGIS to a
     GeoJSONSeq file (geom + best_height_m + area_m2 — the props the frontend reads).
  2. WSL tippecanoe v2.49 tiles it (z8-z14, --drop-densest-as-needed) → PMTiles.
     Layer name 'buildings' to match Map.svelte source-layer.

NO census/population (Brazil IBGE not loaded — see brazil_census_design.md).

Pre-requisite: ingest_gba.py --territory <t>  (gba_buildings_<t> populated).

Usage:
  python pipeline/build_br_tiles.py --territory santa_catarina_br
  python pipeline/build_br_tiles.py --territory rio_grande_sul_br
  python pipeline/build_br_tiles.py --territory parana_br

Output: pipeline/output/<territory>_buildings.pmtiles
R2:     npx wrangler r2 object put neahub/data/tiles/<t>_buildings.pmtiles --file ... --remote
"""
import argparse
import glob
import os
import shutil
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from config import OUTPUT_DIR, get_territory

PG = "dbname=ndvi_misiones user=postgres"


def resolve_ogr2ogr() -> str:
    p = shutil.which("ogr2ogr")
    if p:
        return p
    for pat in (r"C:\Program Files\QGIS*\bin\ogr2ogr.exe", r"C:\OSGeo4W*\bin\ogr2ogr.exe"):
        hits = sorted(glob.glob(pat))
        if hits:
            return hits[-1]
    sys.exit("ogr2ogr not found (QGIS/OSGeo4W)")


def win_to_wsl(path: str) -> str:
    """C:\\Users\\... -> /mnt/c/Users/... (forward slashes)."""
    p = os.path.abspath(path).replace("\\", "/")
    drive, rest = p.split(":", 1)
    return f"/mnt/{drive.lower()}{rest}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--territory", required=True)
    ap.add_argument("--clean-geojson", action="store_true",
                    help="delete the GeoJSONSeq temp after tiling (default: keep for re-tiling)")
    args = ap.parse_args()
    t = args.territory
    cfg = get_territory(t)
    table = f"gba_buildings_{t}"
    geojsonl = os.path.join(OUTPUT_DIR, f"{t}_buildings.geojsonl")
    pmtiles = os.path.join(OUTPUT_DIR, f"{t}_buildings.pmtiles")
    ogr = resolve_ogr2ogr()

    print("=" * 60)
    print(f"BUILD BR BUILDINGS TILES (tippecanoe) — {cfg['label']} ({t})")
    print("=" * 60)

    # Step 1: PostGIS -> GeoJSONSeq (streaming, low memory). Reuse an existing
    # export so re-tiling (iterating tippecanoe flags) doesn't re-run the slow
    # multi-GB ogr2ogr dump.
    print(f"\nStep 1: ogr2ogr {table} -> {os.path.basename(geojsonl)}")
    t0 = time.time()
    if os.path.exists(geojsonl) and os.path.getsize(geojsonl) > 0:
        print(f"  reuse existing {os.path.basename(geojsonl)} ({os.path.getsize(geojsonl)/1e9:.2f} GB)")
    else:
        # Include est_personas (IBGE dasymetric) for population coloring; restrict
        # to buildings matched to a setor (redcode) so the layer covers only the
        # state's census area (GBA bbox overflows into neighbouring states).
        r = subprocess.run([
            ogr, "-f", "GeoJSONSeq", geojsonl, f"PG:{PG}",
            "-sql", (f"SELECT geom, best_height_m, area_m2, COALESCE(est_personas,0) AS est_personas "
                     f"FROM {table} WHERE geom IS NOT NULL AND redcode IS NOT NULL"),
            "-nln", "buildings",
        ], capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"ogr2ogr failed: {r.stderr[-500:]}")
        size_gb = os.path.getsize(geojsonl) / 1e9
        print(f"  -> {size_gb:.2f} GB GeoJSONSeq ({time.time()-t0:.0f}s)")

    # Step 2: tippecanoe (WSL) -> PMTiles. z8-13 (no z14) + strong simplification
    # + a tighter per-tile byte cap → footprints-only backdrop that fits under
    # wrangler's ~100MB upload limit (the >100MB libuv crash / REST-API cap make
    # 600MB+ archives unuploadable with current tooling). Buildings still appear
    # as you zoom; only the deepest individual-footprint detail is coarser.
    print(f"\nStep 2: tippecanoe -> {os.path.basename(pmtiles)}")
    t1 = time.time()
    wsl_geo = win_to_wsl(geojsonl)
    wsl_pmt = win_to_wsl(pmtiles)
    tip = (
        f"tippecanoe -o '{wsl_pmt}' -l buildings -n '{t} buildings' "
        f"-Z8 -z13 --drop-densest-as-needed --maximum-tile-bytes=350000 "
        f"--force '{wsl_geo}'"
    )
    r = subprocess.run(["wsl", "bash", "-lc", tip], text=True)
    if r.returncode != 0 or not os.path.exists(pmtiles):
        sys.exit(f"tippecanoe failed (rc={r.returncode})")
    size_mb = os.path.getsize(pmtiles) / (1024 * 1024)
    print(f"  -> {pmtiles} ({size_mb:.1f} MB, {time.time()-t1:.0f}s)")

    # Keep the GeoJSONSeq by default so re-tiling is cheap; pass --clean-geojson
    # once the tile size is confirmed to reclaim the multi-GB temp file.
    if args.clean_geojson and os.path.exists(geojsonl):
        os.remove(geojsonl)
        print(f"  cleaned {os.path.basename(geojsonl)}")

    print(f"\nDone in {time.time()-t0:.0f}s")
    print("Next:")
    print(f"  npx wrangler r2 object put neahub/data/tiles/{t}_buildings.pmtiles \\")
    print(f"    --file {pmtiles} --remote   (>100MB: use pipeline/r2_upload_large.sh)")


if __name__ == "__main__":
    main()
