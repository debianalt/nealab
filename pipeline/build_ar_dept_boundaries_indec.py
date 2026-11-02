"""Argentine department boundaries from INDEC census radios.

src/lib/data/ar_dept_boundaries.json used to draw GADM 4.1 departments
(rebuild_ar_boundaries.py, extend_ar_boundaries.py, both superseded), while the hexagons
belonged to their department by INDEC 2022 census radios. The department line (and the
hexagon clip, the ?h3= link, the Hexoma `limites`) disagreed with the hexagons in every
AR department (author, 2-nov-2026). The department here is the union of its INDEC radios,
which nest exactly in INDEC departments.

Measured 2-nov-2026: Misiones and every census layer were split by INDEC radios
(99.6-99.9 % of each file's cells inside its INDEC polygon); the satellite layers of
Corrientes, Chaco and Formosa by GADM departments. Those were re-split by INDEC the same
day (repartir_deptos_indec.py; Chaco gained «2 de Abril»), so one polygon set serves
every layer.

Radios: pipeline/output/radios_misiones.parquet and <prov>/radios_<prov>.parquet
(redcode = prov 2 + dept 3 + fraction 2 + radio 2). Names: DEPT_NAMES of
build_censo_temporal_dept.py (INDEC georef table), read without importing it.
Shared edges stay identical between neighbours (shapely.coverage_simplify, ~10 m).
Writes nothing if a department is missing or a name does not match the current file.

Output: src/lib/data/ar_dept_boundaries.json (redcode = INDEC department code).
    python pipeline/build_ar_dept_boundaries_indec.py [--dry]
"""
from __future__ import annotations
import ast, json, sys, unicodedata
from pathlib import Path

import geopandas as gpd
import shapely
from shapely.geometry import mapping

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / 'src' / 'lib' / 'data' / 'ar_dept_boundaries.json'
GADM_JSON = OUT  # names checked against the current file
RADIOS = {
    '54': REPO / 'pipeline/output/radios_misiones.parquet',
    '18': REPO / 'pipeline/output/corrientes/radios_corrientes.parquet',
    '22': REPO / 'pipeline/output/chaco/radios_chaco.parquet',
    '34': REPO / 'pipeline/output/formosa/radios_formosa.parquet',
}
EXPECTED = {'54': 17, '18': 25, '22': 25, '34': 9}
TOL = 0.0005  # degrees (~50 m): well under a res-9 cell (~350 m across)


def norm(s: str) -> str:
    s = unicodedata.normalize('NFKD', str(s)).encode('ascii', 'ignore').decode().lower()
    return ' '.join(s.replace('.', ' ').split())


def dept_names() -> dict[str, str]:
    src = (REPO / 'pipeline/build_censo_temporal_dept.py').read_text(encoding='utf-8')
    for n in ast.parse(src).body:
        tg = n.targets[0] if isinstance(n, ast.Assign) else getattr(n, 'target', None)
        if getattr(tg, 'id', '') == 'DEPT_NAMES':
            return ast.literal_eval(n.value)
    raise SystemExit('DEPT_NAMES not found')


def main() -> int:
    dry = '--dry' in sys.argv
    names = dept_names()
    old = json.loads(GADM_JSON.read_text(encoding='utf-8'))
    old_names = {(str(f['properties']['redcode'])[:2], norm(f['properties']['nombre'])) for f in old['features']}
    feats, problems = [], []
    for prov, path in RADIOS.items():
        r = gpd.read_parquet(path)[['redcode', 'geometry']].to_crs(4326)
        r['dept'] = r['redcode'].astype(str).str[:5]
        r['geometry'] = shapely.make_valid(r.geometry.values)
        d = r.dissolve('dept', as_index=False)[['dept', 'geometry']]
        if len(d) != EXPECTED[prov]:
            problems.append(f'{prov}: {len(d)} departments, expected {EXPECTED[prov]}')
        # topology-preserving: neighbours keep one shared, simplified edge
        d['geometry'] = shapely.coverage_simplify(d.geometry.values, TOL, simplify_boundary=True)
        for _, row in d.sort_values('dept').iterrows():
            nombre = names.get(row['dept'])
            if not nombre:
                problems.append(f'{row["dept"]}: no INDEC name'); continue
            if (prov, norm(nombre)) not in old_names:
                print(f'  note: {row["dept"]} {nombre} is new in the file')
            g = row['geometry']
            # drop slivers left by radio seams (< 0.01 km² parts)
            if g.geom_type == 'MultiPolygon':
                parts = [p for p in g.geoms if p.area > 1e-6]
                g = shapely.MultiPolygon(parts) if len(parts) > 1 else parts[0]
            geom = mapping(shapely.set_precision(g, 1e-5))
            feats.append({'type': 'Feature', 'properties': {'redcode': row['dept'], 'nombre': nombre}, 'geometry': geom})
    print(f'{len(feats)} departments')
    if problems:
        print('PROBLEMS:\n  ' + '\n  '.join(problems))
        return 1
    if dry:
        return 0
    OUT.write_text(json.dumps({'type': 'FeatureCollection', 'features': feats}, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
    print(f'wrote {OUT} ({OUT.stat().st_size // 1024} KB)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
