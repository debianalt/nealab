#!/usr/bin/env python3
"""
Download the official destinations of the accessibility layer (Argentina only).

  secundaria  Mapa Educativo Nacional (DIEE, padrón oficial), WFS
              publico:establecimiento_educativo; active schools whose offer (`nen`)
              includes common secondary («Común-Secundaria…») or technical secondary
              («Secundaria TécProf(INET)»). Adult and special-education secondary out.
  salud       IGN, WFS ign:salud_020801 (IGN/SISA, public establishments only).
  hospital    the subset of `salud` whose type (`gna`) starts with «Hospital».

The box is the union of the published NEA territories plus a 0.5° margin, so a
hospital just across a provincial boundary (Santa Fe, Entre Ríos, Santiago) counts.
Paraguayan cells get NaN for these three destinations (AR registries only).

Output: pipeline/output/access_sources/{secundaria,salud,hospital}.geojson + manifest.json

WFS note: version 1.0.0 with bbox=lon_min,lat_min,lon_max,lat_max. Version 1.1.0 with
EPSG:4326 swaps the axes and returns an empty collection.
"""
import argparse
import collections
import datetime as dt
import hashlib
import json
import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import OUTPUT_DIR  # noqa: E402

# Union of misiones, corrientes, chaco, formosa, itapua_py, alto_parana_py + 0.5°.
NEA_BOX = (-64.15, -31.40, -53.05, -21.81)

SOURCES = {
    'educacion': ('https://mapa.educacion.gob.ar/geoserver/ows', 'publico:establecimiento_educativo'),
    'salud': ('https://wms.ign.gob.ar/geoserver/ows', 'ign:salud_020801'),
}
SECUNDARIA_OFFERS = ('Común-Secundaria', 'Secundaria TécProf(INET)')
UA = {'User-Agent': 'spatia-pipeline/1.0'}


def wfs_get(url: str, layer: str, box) -> dict:
    params = {
        'service': 'WFS', 'version': '1.0.0', 'request': 'GetFeature',
        'typeName': layer, 'outputFormat': 'application/json',
        'bbox': ','.join(str(v) for v in box),
    }
    r = requests.get(url, params=params, headers=UA, timeout=600)
    r.raise_for_status()
    data = r.json()
    if not data.get('features'):
        sys.exit(f'{layer}: empty response (check the bbox axis order)')
    return data


def point_features(features, keep):
    out = []
    for f in features:
        g = f.get('geometry') or {}
        if g.get('type') != 'Point' or not keep(f['properties']):
            continue
        lon, lat = g['coordinates'][:2]
        out.append({'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [lon, lat]},
                    'properties': f['properties']})
    return out


def is_secundaria(p):
    if (p.get('fun') or '').strip() != 'Activo':
        return False
    offers = [o.strip() for o in (p.get('nen') or '').split(';')]
    return any(o.startswith(SECUNDARIA_OFFERS) for o in offers)


def is_hospital(p):
    return (p.get('gna') or '').strip().lower().startswith('hospital')


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--out-dir', default=os.path.join(OUTPUT_DIR, 'access_sources'))
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    edu = wfs_get(*SOURCES['educacion'], NEA_BOX)
    sal = wfs_get(*SOURCES['salud'], NEA_BOX)

    sets = {
        'secundaria': point_features(edu['features'], is_secundaria),
        'salud': point_features(sal['features'], lambda p: True),
        'hospital': point_features(sal['features'], is_hospital),
    }

    manifest = {
        'fecha': dt.date.today().isoformat(),
        'bbox': NEA_BOX,
        'fuentes': {k: {'url': u, 'capa': l} for k, (u, l) in SOURCES.items()},
        'descargados': {'educacion': len(edu['features']), 'salud': len(sal['features'])},
        'conjuntos': {},
    }
    for name, feats in sets.items():
        path = os.path.join(args.out_dir, f'{name}.geojson')
        body = json.dumps({'type': 'FeatureCollection', 'features': feats}, ensure_ascii=False)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(body)
        field = 'gna' if name != 'secundaria' else 'amg'
        manifest['conjuntos'][name] = {
            'n': len(feats),
            'sha256': hashlib.sha256(body.encode('utf-8')).hexdigest(),
            f'por_{field}': dict(collections.Counter(
                (x['properties'].get(field) or '').strip() for x in feats).most_common()),
        }
        if name == 'secundaria':
            manifest['conjuntos'][name]['por_precision'] = dict(collections.Counter(
                x['properties'].get('pre') for x in feats))
        print(f'  {name}: {len(feats):,} points -> {path}')

    with open(os.path.join(args.out_dir, 'manifest.json'), 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)
    return 0


if __name__ == '__main__':
    sys.exit(main())
