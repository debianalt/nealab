"""
Export GAUL level2 district boundaries for all remaining PY territories.
Filters by ADM0_NAME='Paraguay' + ADM1_NAME to avoid naming conflicts
(e.g. 'Misiones' exists in both AR and PY in GAUL).

Output: pipeline/output/{territory_id}_gaul_distritos.geojson for each dept.

Usage:
  python pipeline/export_py_all.py
  python pipeline/export_py_all.py --only amambay_py,boqueron_py
"""
import argparse
import json
import os
import sys
import unicodedata

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")

# All 15 remaining PY territories: (territory_id, GAUL ADM1_NAME)
PY_TERRITORIES = [
    ("concepcion_py",       "Concepcion"),
    ("san_pedro_py",        "San Pedro"),
    ("cordillera_py",       "Cordillera"),
    ("guaira_py",           "Guaira"),
    ("caaguazu_py",         "Caaguazu"),
    ("caazapa_py",          "Caazapa"),
    ("misiones_py",         "Misiones"),
    ("paraguari_py",        "Paraguari"),
    ("central_py",          "Central"),
    ("neembucu_py",         "Neembucu"),
    ("amambay_py",          "Amambay"),
    ("canindeyu_py",        "Canindeyu"),
    ("presidente_hayes_py", "Presidente Hayes"),
    ("boqueron_py",         "Boqueron"),
    ("alto_paraguay_py",    "Alto Paraguay"),
]


def _fold(s: str) -> str:
    """ASCII-fold for fuzzy matching."""
    return unicodedata.normalize('NFKD', str(s)).encode('ascii', 'ignore').decode('ascii').lower()


def export_all(only: set | None = None) -> dict:
    """Export GAUL districts for all PY territories. Returns {territory_id: geojson_path}."""
    try:
        import ee
        ee.Initialize()
    except Exception as e:
        print(f"GEE init failed: {e}")
        sys.exit(1)

    # Fetch all PY level2 features at once (more efficient than one call per dept)
    print("Fetching all Paraguay GAUL level2 features...")
    py_fc = (ee.FeatureCollection("FAO/GAUL/2015/level2")
             .filter(ee.Filter.eq("ADM0_NAME", "Paraguay")))
    all_features = py_fc.getInfo()["features"]
    print(f"  {len(all_features)} total district features for Paraguay")

    # Group by ADM1_NAME (ascii-folded for matching)
    by_dept: dict[str, list] = {}
    for feat in all_features:
        adm1 = feat["properties"].get("ADM1_NAME", "")
        folded = _fold(adm1)
        if folded not in by_dept:
            by_dept[folded] = []
        by_dept[folded].append(feat)

    print(f"  Departments found: {sorted(by_dept.keys())}")

    results = {}
    for territory_id, gaul_name in PY_TERRITORIES:
        if only and territory_id not in only:
            continue

        out_path = os.path.join(OUTPUT_DIR, f"{territory_id}_gaul_distritos.geojson")
        if os.path.exists(out_path):
            print(f"  SKIP {territory_id}: already exists ({os.path.getsize(out_path) // 1024} KB)")
            results[territory_id] = out_path
            continue

        folded_name = _fold(gaul_name)
        features = by_dept.get(folded_name, [])

        if not features:
            # Try partial match
            for k, v in by_dept.items():
                if folded_name in k or k in folded_name:
                    features = v
                    print(f"  FUZZY MATCH {territory_id}: '{gaul_name}' -> '{k}' ({len(features)} features)")
                    break

        if not features:
            print(f"  WARNING: no features for {territory_id} (GAUL name: '{gaul_name}')")
            print(f"  Available: {list(by_dept.keys())}")
            continue

        geojson = {"type": "FeatureCollection", "features": features}
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False)

        # Compute bbox
        lngs, lats = [], []
        for feat in features:
            geom = feat.get("geometry", {})
            coords = geom.get("coordinates", [])
            if geom.get("type") == "Polygon":
                for ring in coords:
                    lngs.extend(c[0] for c in ring)
                    lats.extend(c[1] for c in ring)
            elif geom.get("type") == "MultiPolygon":
                for poly in coords:
                    for ring in poly:
                        lngs.extend(c[0] for c in ring)
                        lats.extend(c[1] for c in ring)

        bbox = [min(lngs), min(lats), max(lngs), max(lats)] if lngs else None
        districts = [f["properties"].get("ADM2_NAME", "") for f in features]
        size_kb = os.path.getsize(out_path) // 1024
        print(f"  OK {territory_id}: {len(features)} districts, bbox={bbox}, {size_kb} KB")
        print(f"     Districts: {sorted(districts)}")
        results[territory_id] = out_path

    return results


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None, help="Comma-separated territory IDs")
    args = ap.parse_args()
    only = set(args.only.split(",")) if args.only else None
    export_all(only)
    print("\nDone. Next: run build_admin_crosswalk.py --source geojson for each territory.")
