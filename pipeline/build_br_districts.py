"""Emit simplified per-state district (municipio) GeoJSONs for the 3 BR
territories in Spatia v1.1. Output goes to pipeline/output/{territory}_districts.geojson;
upload to R2 at data/br_districts/{territory}_districts.geojson with wrangler --remote.

Schema mirrors the Itapua/AP convention so deptBoundaries.ts handles BR uniformly:
  feature.properties.district = GADM NAME_2 (municipality name)

Each state geojson is fetched on-demand by the frontend (Option B), so bundle size
is unaffected. Simplified at tolerance 0.003 (~330m) — visual-only.
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

import geopandas as gpd

REPO = Path(__file__).resolve().parents[1]
GADM_PATH = REPO / "pipeline" / "data" / "gadm41_BRA_2.shp"
OUT_DIR = REPO / "pipeline" / "output"

# (territory_id matching TERRITORY_REGISTRY, NAME_1 in GADM)
TERRITORIES = [
    ("parana_br", "Paraná"),
    ("santa_catarina_br", "Santa Catarina"),
    ("rio_grande_sul_br", "Rio Grande do Sul"),
]

SIMPLIFY_TOLERANCE = 0.003


def main() -> int:
    if not GADM_PATH.exists():
        print(f"[br_districts] ERROR: {GADM_PATH} not found", file=sys.stderr)
        return 1

    gdf = gpd.read_file(GADM_PATH)
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    print(f"[br_districts] loaded GADM_BRA_2: {len(gdf)} municipios across {gdf['NAME_1'].nunique()} states")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for territory_id, name1 in TERRITORIES:
        sub = gdf[gdf["NAME_1"] == name1].copy()
        if len(sub) == 0:
            print(f"[br_districts] WARN: '{name1}' not found in GADM", file=sys.stderr)
            continue
        sub["geometry"] = sub["geometry"].simplify(SIMPLIFY_TOLERANCE, preserve_topology=True)
        features = []
        for _, row in sub.iterrows():
            geom = json.loads(gpd.GeoSeries([row["geometry"]]).to_json())["features"][0]["geometry"]
            features.append({
                "type": "Feature",
                "properties": {"district": row["NAME_2"]},
                "geometry": geom,
            })
        out = OUT_DIR / f"{territory_id}_districts.geojson"
        with open(out, "w", encoding="utf-8") as f:
            json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False, separators=(",", ":"))
        size_kb = out.stat().st_size / 1024
        print(f"[br_districts] {territory_id}: {len(features)} municipios, {size_kb:.0f} KB -> {out}")

    print("[br_districts] done. Upload with:")
    for territory_id, _ in TERRITORIES:
        print(
            f"  npx wrangler r2 object put neahub/data/br_districts/{territory_id}_districts.geojson "
            f"--file pipeline/output/{territory_id}_districts.geojson "
            f"--content-type application/geo+json --remote"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
