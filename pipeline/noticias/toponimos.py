"""Nomenclador fino de Misiones para ubicar lo que nombra la prensa: parajes, barrios, picadas, escuelas y puentes.

Complementa el nomenclador base de extraer.py (municipios, BAHRA, hidrómetros, arroyos). Cada fila lleva su fuente y
su id, para que la ubicación de un evento se pueda citar:

  OSM (pbf local de Geofabrik, recortado a Misiones)
    paraje   place = hamlet | isolated_dwelling | locality | village | farm (puntos y polígonos)
    barrio   place = neighbourhood | suburb | quarter | city_block
    picada   highway con nombre «Picada …», «Línea …», «Camino …» (punto medio del tramo más largo)
    escuela  amenity = school con nombre
  Mapa Educativo (infra.escuelas, CUE)   escuela, con la clave «escuela <número>» cuando el nombre trae «Nº»
  Visor de Defensa Civil y OSM (infra.visor_puentes)   puente con nombre propio (alt_name / bridge:name / name «Puente …»)

El IGN (bahra_paraje, WFS) trae los mismos 309 parajes del BAHRA que ya están en infra.asentamientos: no se suma.
Todo lo que cae fuera de Misiones (unión de infra.visor_municipios) se descarta.

  noticias.toponimos   toponimo_id, clase, nombre, clave, fuente, ref, geom

Uso (desde neahub/):  python pipeline/noticias/toponimos.py
"""
from __future__ import annotations

import io
import re
import sys
from pathlib import Path

import pandas as pd
from shapely.geometry import shape

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'noticias'))
from build_hexoma import DB, connect, log  # noqa: E402
from cauces_osm import PBF, ogr, tags  # noqa: E402
from extraer import clave  # noqa: E402

TMP = Path(r'C:\Users\ant\hexoma-fuentes\noticias\toponimos')
PARAJE = {'hamlet', 'isolated_dwelling', 'locality', 'village', 'farm'}
BARRIO = {'neighbourhood', 'suburb', 'quarter', 'city_block'}
RE_PICADA = re.compile(r'^(picada|linea|línea|camino)\b', re.I)
RE_ESCUELA_N = re.compile(r'\b(?:escuela|e\.?p\.?e?t?\.?|instituto|colegio|bachillerato|bop|cep|epet)\b[^\d]{0,40}?n[°º.]?\s*(\d{1,4})', re.I)


def osm() -> list[dict]:
    TMP.mkdir(parents=True, exist_ok=True)
    out = []
    for f in ogr(PBF, 'points', "place IS NOT NULL OR other_tags LIKE '%\"amenity\"=>\"school\"%'",
                 'osm_id,name,place,other_tags', TMP / 'puntos.geojsonl'):
        p, t = f['properties'], tags(f)
        if not p.get('name'):
            continue
        clase = ('paraje' if p.get('place') in PARAJE else 'barrio' if p.get('place') in BARRIO
                 else 'escuela' if t.get('amenity') == 'school' else None)
        if clase:
            out.append(dict(clase=clase, nombre=p['name'], fuente='OSM', ref=f"node/{p['osm_id']}",
                            geom=shape(f['geometry']).wkt))
    for f in ogr(PBF, 'multipolygons', "place IS NOT NULL OR amenity = 'school'",
                 'osm_id,osm_way_id,name,place,amenity', TMP / 'poligonos.geojsonl'):
        p = f['properties']
        if not p.get('name'):
            continue
        clase = ('paraje' if p.get('place') in PARAJE else 'barrio' if p.get('place') in BARRIO
                 else 'escuela' if p.get('amenity') == 'school' else None)
        if clase:
            ref = f"relation/{p['osm_id']}" if p.get('osm_id') else f"way/{p['osm_way_id']}"
            out.append(dict(clase=clase, nombre=p['name'], fuente='OSM', ref=ref,
                            geom=shape(f['geometry']).representative_point().wkt))
    tramos = {}
    for f in ogr(PBF, 'lines', "highway IS NOT NULL AND name IS NOT NULL", 'osm_id,name,highway',
                 TMP / 'lineas.geojsonl'):
        p = f['properties']
        if not RE_PICADA.match(p['name'] or ''):
            continue
        g = shape(f['geometry'])
        k = clave(p['name'])
        if k not in tramos or g.length > tramos[k][1].length:
            tramos[k] = (p, g)
    for k, (p, g) in tramos.items():
        out.append(dict(clase='picada', nombre=p['name'], fuente='OSM', ref=f"way/{p['osm_id']}",
                        geom=g.interpolate(0.5, normalized=True).wkt))
    return out


def base(conn) -> list[dict]:
    out = []
    e = pd.read_sql('SELECT cue, nombre, ST_AsText(geom) geom FROM infra.escuelas WHERE nombre IS NOT NULL', conn)
    for r in e.itertuples():
        out.append(dict(clase='escuela', nombre=r.nombre.title(), fuente='Mapa Educativo', ref=f'CUE {r.cue}', geom=r.geom))
    p = pd.read_sql("""SELECT gid, coalesce(props->>'bridge:name', props->>'alt_name',
                              CASE WHEN nombre ~* '^puente' THEN nombre END) nombre, ST_AsText(ST_PointOnSurface(geom)) geom
                       FROM infra.visor_puentes""", conn)
    # v3.1: el alt_name de muchos puentes del visor es la ruta («Ruta Nacional 12», «Ruta Costera»): no es un nombre
    p = p[p.nombre.notna() & ~p.nombre.str.match(r'(?i)^(ruta|avenida|av\.|calle|camino|acceso|autopista)\b')]
    for r in p.itertuples():
        out.append(dict(clase='puente', nombre=r.nombre, fuente='infra.visor_puentes', ref=f'gid {r.gid}', geom=r.geom))
    return out


def main() -> int:
    conn = connect(DB)
    filas = osm() + base(conn)
    d = pd.DataFrame(filas)
    d['clave'] = d.nombre.map(clave)
    # escuelas: además, la clave corta «escuela 604» (la prensa casi nunca da el nombre completo)
    num = d[d.clase == 'escuela'].nombre.str.extract(RE_ESCUELA_N, expand=False)
    cortas = d.loc[num.dropna().index].assign(clave=lambda x: 'escuela ' + num.dropna())
    d = pd.concat([d, cortas], ignore_index=True)
    d = d[d.clave.str.len() >= 4].drop_duplicates(['clase', 'clave', 'fuente', 'ref'])
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS noticias.toponimos')
    cur.execute("""CREATE TABLE noticias.toponimos (toponimo_id serial PRIMARY KEY, clase text, nombre text, clave text,
                   fuente text, ref text, geom geometry(Point, 4326))""")
    buf = io.StringIO()
    d[['clase', 'nombre', 'clave', 'fuente', 'ref', 'geom']].to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.execute('CREATE TEMP TABLE _t (clase text, nombre text, clave text, fuente text, ref text, wkt text)')
    cur.copy_expert('COPY _t FROM STDIN WITH (FORMAT csv)', buf)
    cur.execute("""INSERT INTO noticias.toponimos (clase, nombre, clave, fuente, ref, geom)
                   SELECT t.clase, t.nombre, t.clave, t.fuente, t.ref, ST_SetSRID(ST_GeomFromText(t.wkt), 4326)
                   FROM _t t
                   WHERE ST_Intersects(ST_SetSRID(ST_GeomFromText(t.wkt), 4326),
                                       (SELECT ST_Union(geom) FROM infra.visor_municipios))""")
    cur.execute('CREATE INDEX ON noticias.toponimos (clave)')
    cur.execute('CREATE INDEX ON noticias.toponimos USING gist (geom)')
    cur.execute("COMMENT ON TABLE noticias.toponimos IS 'Nomenclador fino de Misiones (parajes, barrios, picadas, escuelas, "
                "puentes) con fuente e id por fila: OSM, Mapa Educativo, visor de Defensa Civil (pipeline/noticias/toponimos.py)'")
    conn.commit()
    r = pd.read_sql('SELECT clase, fuente, count(*) n FROM noticias.toponimos GROUP BY 1, 2 ORDER BY 1, 2', conn)
    conn.close()
    log('noticias.toponimos:\n' + r.to_string(index=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
