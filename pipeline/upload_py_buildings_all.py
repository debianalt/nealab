"""
Upload all new PY territory building PMTiles to R2.
Run this after all build_itapua_buildings.py --territory <t> jobs complete.

Usage:
  python pipeline/upload_py_buildings_all.py
  python pipeline/upload_py_buildings_all.py --only concepcion_py,san_pedro_py
"""
import argparse
import os
import subprocess

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")

# (territory_id, local_stem) — stem strips _py suffix
PY_BUILDINGS = [
    ("concepcion_py", "concepcion"),
    ("san_pedro_py", "san_pedro"),
    ("cordillera_py", "cordillera"),
    ("guaira_py", "guaira"),
    ("caaguazu_py", "caaguazu"),
    ("caazapa_py", "caazapa"),
    ("misiones_py", "misiones"),
    ("paraguari_py", "paraguari"),
    ("central_py", "central"),
    ("neembucu_py", "neembucu"),
    ("amambay_py", "amambay"),
    ("canindeyu_py", "canindeyu"),
    ("presidente_hayes_py", "presidente_hayes"),
    ("boqueron_py", "boqueron"),
    ("alto_paraguay_py", "alto_paraguay"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None)
    args = ap.parse_args()
    only = set(args.only.split(",")) if args.only else None

    for tid, stem in PY_BUILDINGS:
        if only and tid not in only:
            continue
        local = os.path.join(OUTPUT_DIR, f"{stem}_buildings.pmtiles")
        r2_key = f"neahub/data/tiles/{tid}_buildings.pmtiles"
        if not os.path.exists(local):
            print(f"  SKIP {tid}: {local} not found")
            continue
        size_mb = os.path.getsize(local) / (1024 * 1024)
        print(f"  Uploading {tid} ({size_mb:.1f} MB)...")
        r = subprocess.run(
            f"npx wrangler r2 object put {r2_key} --file {local} --remote",
            shell=True, capture_output=True
        )
        if r.returncode == 0:
            print(f"    OK -> {r2_key}")
        else:
            print(f"    ERROR (rc={r.returncode})")

    print("\nDone. After all uploads: npm run deploy")


if __name__ == "__main__":
    main()
