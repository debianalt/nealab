"""Servicios y redes de Misiones → esquema `infra` (fase 4: sólo datos).

  infra.escuelas         Mapa Educativo Nacional (DIEE, padrón oficial), WFS publico:establecimiento_educativo
  infra.salud_ign        IGN, WFS ign:salud_020801 (establecimientos públicos, IGN/SISA)
  infra.asentamientos    BAHRA por la API Georef (datos.gob.ar): asentamiento, categoría, localidad censal, municipio
  infra.osm_electrica    OSM power=line|minor_line|cable (líneas) y power=substation|plant|generator (puntos y áreas)
  infra.osm_telecom      OSM man_made=mast|tower|antenna con communication:* o tower:type=communication

Todo con props/tags completos en jsonb y el recorte de drenaje.py (Misiones + ~5 km); las tablas de padrón se
recortan a la provincia con `limites` (territorio 1) y un margen de 5 km.

Uso (desde neahub/):
    python pipeline/infra/servicios.py [--rebajar]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

import requests
from psycopg2.extras import Json, execute_values

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
from build_hexoma import DB, connect, log  # noqa: E402
from cauces_osm import PBF, ogr, tags, wkt  # noqa: E402
from drenaje import BBOX  # noqa: E402
from fetch_access_facilities import SOURCES, wfs_get  # noqa: E402

CRUDOS = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes')) / 'infra' / 'servicios'
BAHRA = 'https://infra.datos.gob.ar/georef/asentamientos.json'
UA = {'User-Agent': 'spatia-pipeline/1.0 (+https://spatia.ar)'}


def cache_json(nombre: str, bajar, rebajar: bool) -> tuple[dict, str]:
    p = CRUDOS / nombre
    if p.exists() and not rebajar:
        return json.loads(p.read_text(encoding='utf-8')), dt.datetime.fromtimestamp(p.stat().st_mtime).strftime('%Y-%m-%d')
    d = bajar()
    CRUDOS.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(d), encoding='utf-8')
    return d, dt.date.today().isoformat()


def cargar(cur, tabla: str, filas: list, ddl_extra: str, nota: str, url: str, fecha: str, fuente: str) -> int:
    cur.execute(f'DROP TABLE IF EXISTS {tabla}')
    cur.execute(f'CREATE TABLE {tabla} (gid serial PRIMARY KEY, nombre text, {ddl_extra}props jsonb, geom geometry(Geometry, 4326))')
    cols = 'nombre, ' + ''.join(c.split()[0] + ', ' for c in ddl_extra.split(', ') if c.strip()) + 'props, geom'
    n_extra = len([c for c in ddl_extra.split(', ') if c.strip()])
    tpl = '(' + ', '.join(['%s'] * (2 + n_extra)) + ', ST_Force2D(ST_SetSRID(ST_GeomFromText(%s), 4326)))'
    execute_values(cur, f'INSERT INTO {tabla} ({cols}) VALUES %s', filas, template=tpl, page_size=2000)
    # recorte a Misiones + 5 km (el padrón viene en el rectángulo)
    cur.execute(f"""DELETE FROM {tabla} t WHERE NOT EXISTS (SELECT 1 FROM limites l WHERE l.territorio_id = 1
                    AND ST_DWithin(l.geom, t.geom, 0.05))""")
    cur.execute(f'CREATE INDEX ON {tabla} USING gist (geom)')
    cur.execute(f'COMMENT ON TABLE {tabla} IS %s', (f'{nota} (pipeline/infra/servicios.py)',))
    cur.execute(f'SELECT count(*) FROM {tabla}')
    n = cur.fetchone()[0]
    cur.execute("""INSERT INTO infra.fuentes VALUES (%s, %s, %s, %s, now(), %s, %s)
                   ON CONFLICT (tabla) DO UPDATE SET fuente=EXCLUDED.fuente, url=EXCLUDED.url,
                   fecha_fuente=EXCLUDED.fecha_fuente, cargado=now(), n=EXCLUDED.n, nota=EXCLUDED.nota""",
                (tabla, fuente, url, fecha, n, nota))
    log(f'  {tabla}: {n:,} (de {len(filas):,} en el rectángulo)')
    return n


def pt(lon, lat) -> str:
    return f'POINT({lon} {lat})'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--rebajar', action='store_true')
    a = ap.parse_args()
    caja = BBOX
    conn = connect(DB)
    cur = conn.cursor()

    # escuelas
    url, capa = SOURCES['educacion']
    d, f = cache_json('escuelas.json', lambda: wfs_get(url, capa, caja), a.rebajar)
    filas = []
    for ft in d['features']:
        g = ft.get('geometry') or {}
        if g.get('type') != 'Point':
            continue
        p = ft['properties']
        filas.append((p.get('nam') or p.get('fna'), p.get('cue'), Json(p),
                      pt(*g['coordinates'][:2])))
    log(f'escuelas: campos {sorted(d["features"][0]["properties"])[:25]}')
    cargar(cur, 'infra.escuelas', filas, 'cue text, ', 'Establecimientos educativos del padrón oficial (Mapa Educativo '
           'Nacional, DIEE)', f'{url}?typeName={capa}', f, 'Ministerio de Capital Humano – Mapa Educativo Nacional')

    # salud IGN
    url, capa = SOURCES['salud']
    d, f = cache_json('salud_ign.json', lambda: wfs_get(url, capa, caja), a.rebajar)
    filas = []
    for ft in d['features']:
        g = ft.get('geometry') or {}
        if g.get('type') != 'Point':
            continue
        p = ft['properties']
        filas.append((p.get('fna') or p.get('nam'), p.get('gna'), Json(p), pt(*g['coordinates'][:2])))
    log(f'salud: campos {sorted(d["features"][0]["properties"])[:25]}')
    cargar(cur, 'infra.salud_ign', filas, 'tipo text, ', 'Establecimientos de salud públicos (IGN, capa salud_020801)',
           f'{url}?typeName={capa}', f, 'Instituto Geográfico Nacional')

    # BAHRA
    d, f = cache_json('bahra.json', lambda: requests.get(BAHRA, headers=UA, timeout=300).json(), a.rebajar)
    filas = []
    for s in d.get('asentamientos', d.get('datos', [])):
        if (s.get('provincia') or {}).get('id') not in ('54', None):
            continue
        c = s.get('centroide') or {}
        if 'lon' not in c:
            continue
        filas.append((s.get('nombre'), s.get('id'), s.get('categoria'), (s.get('localidad_censal') or {}).get('id'),
                      (s.get('municipio') or {}).get('nombre'), Json(s), pt(c['lon'], c['lat'])))
    cargar(cur, 'infra.asentamientos', filas, 'bahra_id text, categoria text, localidad_censal text, municipio text, ',
           'Asentamientos humanos de la Base BAHRA (API Georef)', BAHRA, f, 'BAHRA – Georef (datos.gob.ar)')

    # OSM: electricidad y telecomunicaciones (pbf de Geofabrik)
    tmp = CRUDOS
    tmp.mkdir(parents=True, exist_ok=True)
    fecha_osm = dt.datetime.fromtimestamp(PBF.stat().st_mtime).strftime('%Y-%m-%d')
    lin = ogr(PBF, 'lines', "other_tags LIKE '%\"power\"=>%'", 'osm_id,name,other_tags', tmp / 'osm_power_lines.geojsonl')
    pts = ogr(PBF, 'points', "other_tags LIKE '%\"power\"=>%' OR man_made IN ('mast','tower','antenna')",
              'osm_id,name,man_made,other_tags', tmp / 'osm_power_pts.geojsonl')
    pol = ogr(PBF, 'multipolygons', "other_tags LIKE '%\"power\"=>%'", 'osm_id,osm_way_id,name,other_tags',
              tmp / 'osm_power_pol.geojsonl')
    elec, tele = [], []
    for ft in lin:
        t = tags(ft)
        if t.get('power') in ('line', 'minor_line', 'cable') and ft['geometry']['type'] == 'LineString':
            elec.append((t.get('name'), t.get('power'), t.get('voltage'), t.get('operator'), Json(t), wkt(ft['geometry'])))
    for ft in pts:
        t = tags(ft)
        mm = ft['properties'].get('man_made')
        if mm:
            t['man_made'] = mm
        if t.get('power') in ('substation', 'plant', 'generator', 'transformer'):
            elec.append((t.get('name'), t.get('power'), t.get('voltage'), t.get('operator'), Json(t), wkt(ft['geometry'])))
        if mm in ('mast', 'tower', 'antenna') and (any(k.startswith('communication') for k in t)
                                                   or t.get('tower:type') == 'communication'):
            tele.append((t.get('name'), mm, t.get('operator'), Json(t), wkt(ft['geometry'])))
    from shapely.geometry import shape
    for ft in pol:
        t = tags(ft)
        if t.get('power') in ('substation', 'plant', 'generator'):
            c = shape(ft['geometry']).representative_point()
            elec.append((t.get('name'), t.get('power'), t.get('voltage'), t.get('operator'), Json(t), c.wkt))
    cargar(cur, 'infra.osm_electrica', elec, 'tipo text, tension text, operador text, ',
           'Red eléctrica de OSM: líneas (line, minor_line, cable) y subestaciones/plantas (punto)',
           'https://download.geofabrik.de/south-america/argentina-latest.osm.pbf', fecha_osm, 'OpenStreetMap (Geofabrik)')
    cargar(cur, 'infra.osm_telecom', tele, 'tipo text, operador text, ',
           'Mástiles y torres de comunicación de OSM', 'https://download.geofabrik.de/south-america/argentina-latest.osm.pbf',
           fecha_osm, 'OpenStreetMap (Geofabrik)')
    conn.commit()
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
