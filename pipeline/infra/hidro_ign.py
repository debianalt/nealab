"""Hidrografía oficial del IGN en Misiones → esquema `infra` (fase 5: sólo datos).

WFS del IGN (wms.ign.gob.ar/geoserver/ows, versión 1.0.0 con bbox lon/lat: la 1.1.0 invierte los ejes y devuelve
vacío, ver fetch_access_facilities.py). Recorte de drenaje.BBOX (Misiones + ~5 km). Una tabla por capa,
infra.ign_<nombre> (nombre, atributos jsonb, geometría), más infra.fuentes.

Uso (desde neahub/):
    python pipeline/infra/hidro_ign.py [--rebajar]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

from psycopg2.extras import Json, execute_values

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
from build_hexoma import DB, connect, log  # noqa: E402
from drenaje import BBOX  # noqa: E402
from fetch_access_facilities import wfs_get  # noqa: E402

WFS = 'https://wms.ign.gob.ar/geoserver/ows'
CRUDOS = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes')) / 'infra' / 'ign_hidro'
CAPAS = {
    'cursos_perennes': ('ign:lineas_de_aguas_continentales_perenne', 'Corrientes de agua perennes'),
    'cursos_intermitentes': ('ign:lineas_de_aguas_continentales_intermitentes', 'Corrientes de agua intermitentes'),
    'canales': ('ign:lineas_de_aguas_continentales_BH020', 'Canales'),
    'espejos_perennes': ('ign:areas_de_aguas_continentales_perenne', 'Espejos de agua perennes'),
    'espejos_intermitentes': ('ign:areas_de_aguas_continentales_intermitente', 'Espejos de agua intermitentes'),
    'corrientes_area': ('ign:areas_de_aguas_continentales_BH140', 'Corrientes de agua (polígono: ríos anchos)'),
    'embalses': ('ign:areas_de_aguas_continentales_BH130', 'Embalses'),
    'embalses_rurales': ('ign:areas_de_aguas_continentales_041101', 'Embalses rurales (tajamares)'),
    'saltos': ('ign:puntos_de_aguas_continentales_BH180', 'Cataratas, cascadas y saltos'),
    'fuentes_naturales': ('ign:puntos_de_aguas_continentales_BH170', 'Fuentes naturales (vertientes)'),
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--rebajar', action='store_true')
    a = ap.parse_args()
    CRUDOS.mkdir(parents=True, exist_ok=True)
    conn = connect(DB)
    cur = conn.cursor()
    for nombre, (capa, desc) in CAPAS.items():
        p = CRUDOS / f'{nombre}.geojson'
        if a.rebajar or not p.exists():
            try:
                d = wfs_get(WFS, capa, BBOX)
            except SystemExit:
                d = {'features': []}
            p.write_text(json.dumps(d), encoding='utf-8')
        d = json.loads(p.read_text(encoding='utf-8'))
        feats = [f for f in d.get('features', []) if f.get('geometry')]
        t = f'infra.ign_{nombre}'
        cur.execute(f'DROP TABLE IF EXISTS {t}')
        cur.execute(f'CREATE TABLE {t} (gid serial PRIMARY KEY, nombre text, props jsonb, geom geometry(Geometry, 4326))')
        filas = [((f['properties'] or {}).get('fna') or (f['properties'] or {}).get('nam'), Json(f['properties'] or {}),
                  json.dumps(f['geometry'])) for f in feats]
        if filas:
            execute_values(cur, f'INSERT INTO {t} (nombre, props, geom) VALUES %s', filas,
                           template='(%s, %s, ST_Force2D(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))', page_size=2000)
        cur.execute(f'CREATE INDEX ON {t} USING gist (geom)')
        cur.execute(f'COMMENT ON TABLE {t} IS %s', (f'{desc}. IGN, WFS {capa} (hidro_ign.py)',))
        fecha = dt.datetime.fromtimestamp(p.stat().st_mtime).strftime('%Y-%m-%d')
        cur.execute("""INSERT INTO infra.fuentes VALUES (%s, 'Instituto Geográfico Nacional (WFS)', %s, %s, now(), %s, %s)
                       ON CONFLICT (tabla) DO UPDATE SET url=EXCLUDED.url, fecha_fuente=EXCLUDED.fecha_fuente, cargado=now(),
                       n=EXCLUDED.n, nota=EXCLUDED.nota""", (t, f'{WFS}?typeName={capa}', fecha, len(filas), desc))
        extra = ''
        if filas and feats[0]['geometry']['type'] in ('LineString', 'MultiLineString'):
            cur.execute(f'SELECT round(sum(ST_Length(geom::geography)) / 1000) FROM {t}')
            extra = f', {cur.fetchone()[0]:,.0f} km'
        log(f'  {t}: {len(filas):,}{extra}; campos {sorted((feats[0]["properties"] or {}))[:8] if feats else []}')
    conn.commit()
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
