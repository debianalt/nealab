"""
Outlines of the Spatia area on the start map, in two tiers (author's decision, 7-nov-2026):
  'nea' — the 4 NEA provinces of Argentina, where the data are: strong line;
  'tf'  — Paraguay and southern Brazil, the cross-border area not yet integrated: soft line.
To integrate a territory, move it from TRANSFRONTERIZA to NEA_AR (or a new tier) and rerun.

Each tier unions its territory boundaries the map already ships (src/lib/data/*_boundary.json).
They come from different national sources (INDEC, DGEEC, IBGE) and meet along rivers
on opposite banks or the midline, so the raw union is full of seams: 84 parts and 810
holes. A morphological closing of CLOSE_DEG (~1 km) seals them (area changes 0.07 %);
then only the exterior rings of parts larger than MIN_PART_DEG2 are kept, simplified
to ~SIMPLIFY_DEG, and written as lines.

    python pipeline/build_region_outline.py
"""

import json
import os

from shapely.geometry import mapping, shape
from shapely.ops import unary_union

DATA = os.path.join(os.path.dirname(__file__), '..', 'src', 'lib', 'data')
OUT = os.path.join(DATA, 'region_outline.json')
CLOSE_DEG = 0.01
MIN_PART_DEG2 = 0.001
SIMPLIFY_DEG = 0.002

NEA_AR = ['misiones', 'corrientes', 'chaco', 'formosa']
TRANSFRONTERIZA = (
    ['itapua', 'alto_parana', 'parana_br', 'santa_catarina_br', 'rio_grande_sul_br']
    + [f'{d}_py' for d in ('concepcion', 'san_pedro', 'cordillera', 'guaira', 'caaguazu',
                           'caazapa', 'misiones', 'paraguari', 'central', 'neembucu',
                           'amambay', 'canindeyu', 'presidente_hayes', 'boqueron',
                           'alto_paraguay')]
)
TIERS = {'nea': NEA_AR, 'tf': TRANSFRONTERIZA}


def outline(territories: list[str]) -> list:
    geoms = []
    for t in territories:
        with open(os.path.join(DATA, f'{t}_boundary.json'), encoding='utf-8') as f:
            g = json.load(f)
        feats = g['features'] if g.get('type') == 'FeatureCollection' else [g]
        geoms += [shape(ft['geometry']).buffer(0) for ft in feats]
    closed = unary_union(geoms).buffer(CLOSE_DEG, join_style=2).buffer(-CLOSE_DEG, join_style=2)
    parts = [p for p in getattr(closed, 'geoms', [closed]) if p.area > MIN_PART_DEG2]
    return [p.exterior.simplify(SIMPLIFY_DEG, preserve_topology=True) for p in parts]


def main() -> None:
    feats = []
    for tier, ts in TIERS.items():
        lines = outline(ts)
        print(f'  {tier}: {len(ts)} territories → {len(lines)} outline(s)')
        feats += [{'type': 'Feature', 'properties': {'tier': tier}, 'geometry': mapping(l)} for l in lines]
    fc = {'type': 'FeatureCollection', 'features': feats}
    # 4 decimals ≈ 11 m: plenty for a line drawn at regional zoom.
    def rnd(c):
        return [round(c[0], 4), round(c[1], 4)]
    for ft in fc['features']:
        ft['geometry'] = {'type': 'LineString', 'coordinates': [rnd(c) for c in ft['geometry']['coordinates']]}
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(fc, f, separators=(',', ':'))
    n = sum(len(ft['geometry']['coordinates']) for ft in fc['features'])
    print(f'{len(feats)} outline(s), {n} vertices, '
          f'{os.path.getsize(OUT) / 1024:.0f} KB → {OUT}')


if __name__ == '__main__':
    main()
