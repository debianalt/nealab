"""Cauces, alcantarillas y vados de OSM en Misiones → `infra.osm_cauces` e `infra.osm_estructuras`.

`vial.osm` (importar_vial.py) sólo trae las líneas con `highway`: le faltan los cauces (`waterway`) y las
alcantarillas, que en OSM son el TRAMO DEL CAUCE que pasa bajo el camino (`waterway=* + tunnel=culvert`), no
el camino. Se leen del extracto de Argentina de Geofabrik (el mismo que usa importar_vial.py) con ogr2ogr de
QGIS, recortado a drenaje.BBOX (Misiones + ~5 km). Overpass se probó el 7-nov-2026 y devolvió 502/504 por carga
en overpass-api.de y kumi.systems aun en 9 bloques.

  infra.osm_cauces       osm_id, waterway, nombre, intermitente, alcantarilla (bool), tags jsonb, geom (línea)
  infra.osm_estructuras  osm_id, tipo ('alcantarilla' | 'vado'), nombre, tags jsonb, geom (punto: el centro
                         del tramo alcantarillado; el nodo del vado; o el tramo de camino con ford=yes de vial.osm)

Uso (desde neahub/):
    python pipeline/infra/cauces_osm.py [--pbf ruta.osm.pbf] [--rebajar]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import requests
from psycopg2.extras import Json, execute_values

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
from build_hexoma import DB, connect, log  # noqa: E402
from drenaje import BBOX  # noqa: E402

PBF_URL = 'https://download.geofabrik.de/south-america/argentina-latest.osm.pbf'
PBF = ROOT / 'pipeline' / 'output' / 'vial' / 'argentina-latest.osm.pbf'
TMP = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes')) / 'infra'
QGIS = Path(os.getenv('QGIS_HOME', r'C:\Program Files\QGIS 3.40.13'))
OGR2OGR = os.getenv('OGR2OGR', str(QGIS / 'bin' / 'ogr2ogr.exe'))
UA = {'User-Agent': 'spatia-pipeline/1.0 (+https://spatia.ar)'}
TIPOS_CAUCE = ('river', 'stream', 'canal', 'drain', 'ditch', 'brook', 'tidal_channel')
OTRAS = re.compile(r'"((?:[^"\\]|\\.)*)"=>"((?:[^"\\]|\\.)*)"')


def bajar_pbf() -> None:
    log(f'descargando {PBF_URL}')
    PBF.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(PBF_URL, stream=True, timeout=300, headers=UA) as r:
        r.raise_for_status()
        tmp = PBF.with_suffix('.part')
        with open(tmp, 'wb') as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
        tmp.replace(PBF)


def ogr(pbf: Path, capa: str, where: str, campos: str, salida: Path) -> list[dict]:
    salida.unlink(missing_ok=True)
    env = {**os.environ, 'PROJ_DATA': str(QGIS / 'share' / 'proj'), 'OSM_MAX_TMPFILE_SIZE': '1024'}
    env.pop('PROJ_LIB', None)
    subprocess.run([OGR2OGR, '-f', 'GeoJSONSeq', str(salida), str(pbf), capa, '-where', where,
                    '-spat', *map(str, BBOX), '-select', campos], check=True, env=env)
    return [json.loads(x) for x in salida.read_text(encoding='utf-8').splitlines() if x.strip()]


def tags(f: dict) -> dict:
    p = f['properties']
    t = {k: v for k, v in OTRAS.findall(p.get('other_tags') or '')}
    for k in ('name', 'waterway'):
        if p.get(k):
            t[k] = p[k]
    return t


def wkt(geom: dict) -> str:
    if geom['type'] == 'Point':
        x, y = geom['coordinates'][:2]
        return f'POINT({x} {y})'
    return 'LINESTRING(' + ','.join(f'{c[0]} {c[1]}' for c in geom['coordinates']) + ')'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--pbf')
    ap.add_argument('--rebajar', action='store_true')
    args = ap.parse_args()
    pbf = Path(args.pbf) if args.pbf else PBF
    if args.rebajar or not pbf.exists():
        bajar_pbf()
    fecha = dt.datetime.fromtimestamp(pbf.stat().st_mtime).strftime('%Y-%m-%d')
    lineas = ogr(pbf, 'lines', 'waterway IS NOT NULL', 'osm_id,name,waterway,other_tags', TMP / 'osm_cauces.geojsonl')
    puntos = ogr(pbf, 'points', "other_tags LIKE '%\"ford\"=>\"yes\"%'", 'osm_id,name,other_tags', TMP / 'osm_vados.geojsonl')
    log(f'pbf {fecha}: {len(lineas):,} líneas con waterway, {len(puntos):,} nodos de vado')

    cauces, estructuras = [], []
    for f in lineas:
        t = tags(f)
        if t.get('waterway') not in TIPOS_CAUCE or f['geometry']['type'] != 'LineString':
            continue
        oid = int(f['properties']['osm_id'])
        alc = t.get('tunnel') == 'culvert' or t.get('culvert') == 'yes'
        inter = t.get('intermittent') == 'yes' or t.get('seasonal') == 'yes'
        g = wkt(f['geometry'])
        cauces.append((oid, t['waterway'], t.get('name'), inter, alc, Json(t), g))
        if alc:
            estructuras.append((oid, 'way', 'alcantarilla', t.get('name'), Json(t), g))
    for f in puntos:
        t = tags(f)
        estructuras.append((int(f['properties']['osm_id']), 'node', 'vado', t.get('name'), Json(t), wkt(f['geometry'])))

    conn = connect(DB)
    cur = conn.cursor()
    cur.execute('CREATE SCHEMA IF NOT EXISTS infra')
    cur.execute('DROP TABLE IF EXISTS infra.osm_cauces')
    cur.execute("""CREATE TABLE infra.osm_cauces (gid serial PRIMARY KEY, osm_id bigint, waterway text, nombre text,
                   intermitente boolean, alcantarilla boolean, tags jsonb, geom geometry(LineString, 4326))""")
    execute_values(cur, 'INSERT INTO infra.osm_cauces (osm_id, waterway, nombre, intermitente, alcantarilla, tags, geom) VALUES %s',
                   cauces, template='(%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))', page_size=5000)
    cur.execute('CREATE INDEX ON infra.osm_cauces USING gist (geom)')
    cur.execute('DROP TABLE IF EXISTS infra.osm_estructuras')
    cur.execute("""CREATE TABLE infra.osm_estructuras (gid serial PRIMARY KEY, osm_id bigint, osm_tipo text, tipo text,
                   nombre text, tags jsonb, geom geometry(Point, 4326))""")
    execute_values(cur, 'INSERT INTO infra.osm_estructuras (osm_id, osm_tipo, tipo, nombre, tags, geom) VALUES %s',
                   estructuras, template='(%s, %s, %s, %s, %s, ST_PointOnSurface(ST_GeomFromText(%s, 4326)))', page_size=5000)
    # vados que en OSM son el tramo de camino (ford=yes en la vía), ya cargados en vial.osm
    x0, y0, x1, y1 = BBOX
    cur.execute(f"""INSERT INTO infra.osm_estructuras (osm_id, osm_tipo, tipo, nombre, tags, geom)
                    SELECT osm_id::bigint, 'way', 'vado', name, tags, ST_PointOnSurface(geom) FROM vial.osm
                    WHERE tags->>'ford' = 'yes' AND geom && ST_MakeEnvelope({x0}, {y0}, {x1}, {y1}, 4326)""")
    cur.execute('CREATE INDEX ON infra.osm_estructuras USING gist (geom)')
    cur.execute("""CREATE TABLE IF NOT EXISTS infra.fuentes (
        tabla text PRIMARY KEY, fuente text, url text, fecha_fuente text, cargado timestamptz, n integer, nota text)""")
    cur.execute('SELECT count(*) FROM infra.osm_estructuras')
    n_est = cur.fetchone()[0]
    for tabla, n, nota in [('infra.osm_cauces', len(cauces), 'waterway=' + '|'.join(TIPOS_CAUCE)),
                           ('infra.osm_estructuras', n_est, 'tunnel=culvert en cauces; ford=yes (nodos y vías)')]:
        cur.execute("""INSERT INTO infra.fuentes VALUES (%s, 'OpenStreetMap (extracto Geofabrik)', %s, %s, now(), %s, %s)
                       ON CONFLICT (tabla) DO UPDATE SET fuente=EXCLUDED.fuente, url=EXCLUDED.url,
                       fecha_fuente=EXCLUDED.fecha_fuente, cargado=now(), n=EXCLUDED.n, nota=EXCLUDED.nota""",
                    (tabla, PBF_URL, fecha, n, nota))
    conn.commit()
    cur.execute("SELECT waterway, count(*), round(sum(ST_Length(geom::geography))/1000) FROM infra.osm_cauces GROUP BY 1 ORDER BY 3 DESC")
    for w, n, km in cur.fetchall():
        log(f'  {w}: {n:,} tramos, {km:,.0f} km')
    cur.execute('SELECT tipo, count(*) FROM infra.osm_estructuras GROUP BY 1')
    log('  estructuras:', dict(cur.fetchall()))
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
