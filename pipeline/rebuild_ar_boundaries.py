"""Rebuild src/lib/data/ar_dept_boundaries.json with higher-resolution geometry.

The highlight of a selected department filters by `nombre`/`redcode`, so the
EXISTING json is the authority for those (INDEC redcodes). We only swap in the
hi-res GADM geometry, joined to the existing records by normalized name. The old
file was simplified at 0.003 (~330m) and cut across rivers; rebuild at 0.0008 (~90m).

Source: pipeline/data/gadm41_ARG_2.shp (NAME_1 = province, NAME_2 = department).
Covers the 4 v1.1 provinces: Corrientes(18), Chaco(22), Formosa(34), Misiones(54).
"""
from __future__ import annotations
import json, sys, unicodedata
from collections import defaultdict
from pathlib import Path

import geopandas as gpd
from shapely.geometry import mapping

REPO = Path(__file__).resolve().parents[1]
GADM = REPO / "pipeline" / "data" / "gadm41_ARG_2.shp"
JSON_PATH = REPO / "src" / "lib" / "data" / "ar_dept_boundaries.json"
SIMPLIFY_TOLERANCE = 0.0008  # ~90m

PROVINCES = {"Corrientes": "18", "Chaco": "22", "Formosa": "34", "Misiones": "54"}

# GADM spells out numbers; the JSON uses digits. Map GADM-normalized -> JSON-normalized.
ALIASES = {
    "doce de octubre": "12 de octubre",
    "nueve de julio": "9 de julio",
    "primero de mayo": "1 de mayo",
    "veinticinco de mayo": "25 de mayo",
}


def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower().strip()
    return " ".join(s.replace(".", " ").split())


def main() -> int:
    existing = json.loads(JSON_PATH.read_text(encoding="utf-8"))["features"]
    # codprov -> normname -> {redcode, nombre, feature(old, for fallback)}
    by_cp: dict[str, dict[str, dict]] = defaultdict(dict)
    for f in existing:
        rc = str(f["properties"]["redcode"])
        by_cp[rc[:2]][norm(f["properties"]["nombre"])] = {
            "redcode": f["properties"]["redcode"],
            "nombre": f["properties"]["nombre"],
            "old": f,
        }

    g = gpd.read_file(GADM)
    if g.crs is None or g.crs.to_epsg() != 4326:
        g = g.to_crs(epsg=4326)

    out = []
    produced = set()  # (cp, normname) we emitted from GADM
    for name1, cp in PROVINCES.items():
        sub = g[g["NAME_1"] == name1].copy()
        sub["geometry"] = sub["geometry"].simplify(SIMPLIFY_TOLERANCE, preserve_topology=True)
        for _, row in sub.iterrows():
            nn = norm(row["NAME_2"])
            nn = ALIASES.get(nn, nn)
            rec = by_cp[cp].get(nn)
            if not rec:
                print(f"[ar] WARN no JSON match: {name1}/{row['NAME_2']} (norm '{nn}')", file=sys.stderr)
                continue
            out.append({
                "type": "Feature",
                "properties": {"redcode": rec["redcode"], "nombre": rec["nombre"]},
                "geometry": mapping(row["geometry"]),
            })
            produced.add((cp, nn))

    # Fallback: any existing dept not produced from GADM keeps its old geometry.
    for cp, depts in by_cp.items():
        for nn, rec in depts.items():
            if (cp, nn) not in produced:
                print(f"[ar] WARN kept OLD geom (no GADM): {rec['nombre']} ({rec['redcode']})", file=sys.stderr)
                out.append(rec["old"])

    JSON_PATH.write_text(
        json.dumps({"type": "FeatureCollection", "features": out}, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    kb = JSON_PATH.stat().st_size / 1024
    print(f"[ar] wrote {len(out)} depts, {kb:.0f} KB -> {JSON_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
