"""One-off: extend src/lib/data/ar_dept_boundaries.json with Chaco (codprov 22)
and Formosa (codprov 34) deptos from GADM_ARG_2 shapefile.

Existing JSON has 17 Misiones (54) + 25 Corrientes (18) deptos. We append 24
Chaco + 9 Formosa with simplified geometries. The `redcode` field is used only
for province-prefix matching in src/lib/utils/deptBoundaries.ts, so sequential
codes work (no join key to other tables).
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

import geopandas as gpd

REPO = Path(__file__).resolve().parents[1]
JSON_PATH = REPO / "src" / "lib" / "data" / "ar_dept_boundaries.json"
GADM_PATH = REPO / "pipeline" / "data" / "gadm41_ARG_2.shp"

# (NAME_1 in GADM, codprov INDEC) — codprov drives the redcode prefix
PROVINCES = [
    ("Chaco", "22"),
    ("Formosa", "34"),
]

SIMPLIFY_TOLERANCE = 0.003  # ~330m at the equator; visual-only


def load_existing() -> dict:
    with open(JSON_PATH, encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    existing = load_existing()
    existing_prefixes = {f["properties"]["redcode"][:2] for f in existing["features"]}
    print(f"[ar_boundaries] existing features: {len(existing['features'])} (prefixes: {sorted(existing_prefixes)})")

    gdf = gpd.read_file(GADM_PATH)
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    appended = 0
    for name1, codprov in PROVINCES:
        if codprov in existing_prefixes:
            print(f"[ar_boundaries] skip {name1} ({codprov}) — already present")
            continue
        sub = gdf[gdf["NAME_1"] == name1].copy()
        if len(sub) == 0:
            print(f"[ar_boundaries] WARN: {name1} not found in GADM", file=sys.stderr)
            continue
        sub["geometry"] = sub["geometry"].simplify(SIMPLIFY_TOLERANCE, preserve_topology=True)
        sub = sub.sort_values("NAME_2").reset_index(drop=True)
        for idx, row in sub.iterrows():
            redcode = f"{codprov}{idx+1:03d}"  # 22001..22024, 34001..34009
            existing["features"].append({
                "type": "Feature",
                "properties": {
                    "redcode": redcode,
                    "nombre": row["NAME_2"],
                },
                "geometry": json.loads(gpd.GeoSeries([row["geometry"]]).to_json())["features"][0]["geometry"],
            })
            appended += 1
        print(f"[ar_boundaries] +{len(sub)} {name1} deptos (codprov {codprov})")

    if appended == 0:
        print("[ar_boundaries] nothing to do")
        return 0

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = JSON_PATH.stat().st_size / 1024
    print(f"[ar_boundaries] wrote {len(existing['features'])} features -> {JSON_PATH} ({size_kb:.1f} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
