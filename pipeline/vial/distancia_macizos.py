"""Distancia de cada macizo forestal del NEA AR (DNDFI) a la red vial del Hexoma (`vial.caminos`).

Carga los macizos del Inventario Nacional de Plantaciones Forestales (DNDFI, 23-04-2026) de Misiones, Corrientes,
Chaco y Formosa en `vial.macizos` y calcula, en metros y en línea recta desde el BORDE del macizo (0 si un camino
lo toca o lo cruza):

    dist_pav_m      a la ruta o calle pavimentada más cercana (superficie = 'pavimento')
    dist_camino_m   a cualquier camino de la red unificada (OSM vehicular + Microsoft); > 1 km = falta mapeo

La distancia es geodésica (geography) sobre los 4 candidatos más cercanos por índice (KNN en grados), para no
depender de la proyección en un NEA que cruza tres fajas POSGAR. Línea recta, no por camino: la distancia por red
es otro trabajo (pgRouting).

La usa pipeline/forestal/exportar_cosecha_web.py (columna banda_pav y gráfico «distancia a ruta pavimentada»).

Uso (desde neahub/, después de importar_vial.py):
    python pipeline/vial/distancia_macizos.py
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
from build_hexoma import DB, PG, connect, log  # noqa: E402

MACIZOS = ROOT.parent / 'dndfi_inventario' / 'macizos_23_04_2026.geojson'
QGIS = Path(os.getenv('QGIS_HOME', r'C:\Program Files\QGIS 3.40.13'))
OGR2OGR = os.getenv('OGR2OGR', str(QGIS / 'bin' / 'ogr2ogr.exe'))
K = 4   # candidatos por KNN antes de medir en geography


def cargar_macizos() -> None:
    # PG:"..." a partir de HEXOMA_PG (postgresql://usuario[:clave]@host:puerto)
    rest = PG.split('://', 1)[1]
    cred, hostport = rest.rsplit('@', 1)
    user, _, pwd = cred.partition(':')
    host, _, port = hostport.partition(':')
    dsn = f'PG:dbname={DB} host={host} port={port or 5432} user={user}' + (f' password={pwd}' if pwd else '')
    env = {**os.environ, 'PROJ_DATA': str(QGIS / 'share' / 'proj'), 'PGCLIENTENCODING': 'UTF8'}
    subprocess.run([OGR2OGR, '-f', 'PostgreSQL', dsn, str(MACIZOS), '-nln', 'vial.macizos', '-overwrite',
                    '-where', "prov IN ('MISIONES','CORRIENTES','CHACO','FORMOSA')",
                    '-select', 'id,genero,grupo_espe,anio_plant,superficie,prov,depto',
                    '-nlt', 'PROMOTE_TO_MULTI', '-lco', 'GEOMETRY_NAME=geom', '-lco', 'FID=fid',
                    '-lco', 'SPATIAL_INDEX=GIST', '-t_srs', 'EPSG:4326', '--config', 'PG_USE_COPY', 'YES'],
                   check=True, env=env)


def main() -> None:
    t0 = time.time()
    log('cargando macizos DNDFI del NEA en vial.macizos…')
    cargar_macizos()
    conn = connect(DB)
    cur = conn.cursor()
    cur.execute('SELECT count(*) FROM vial.macizos')
    log(f'  {cur.fetchone()[0]:,} macizos ({time.time() - t0:.0f} s)')
    cur.execute("""
        CREATE INDEX IF NOT EXISTS caminos_pav_geom ON vial.caminos USING gist (geom) WHERE superficie = 'pavimento';
        ALTER TABLE vial.macizos DROP COLUMN IF EXISTS dist_pav_m, DROP COLUMN IF EXISTS dist_camino_m;
        ALTER TABLE vial.macizos ADD COLUMN dist_pav_m real, ADD COLUMN dist_camino_m real;
    """)
    conn.commit()
    for col, filtro in (('dist_pav_m', "AND c.superficie = 'pavimento'"), ('dist_camino_m', '')):
        cur.execute(f"""
            UPDATE vial.macizos m SET {col} = d.dist
            FROM (SELECT m2.fid, (SELECT min(ST_Distance(m2.geom::geography, c.geom::geography))
                                  FROM (SELECT c.geom FROM vial.caminos c WHERE true {filtro}
                                        ORDER BY c.geom <-> m2.geom LIMIT {K}) c) AS dist
                  FROM vial.macizos m2) d
            WHERE m.fid = d.fid""")
        conn.commit()
        log(f'  {col} listo ({time.time() - t0:.0f} s)')
    cur.execute("""
        COMMENT ON TABLE vial.macizos IS 'Macizos DNDFI 23-04-2026 del NEA AR con distancia en línea recta desde el borde a la red vial (vial.caminos): dist_pav_m a pavimento, dist_camino_m a cualquier camino. pipeline/vial/distancia_macizos.py';
    """)
    conn.commit()
    cur.execute("""SELECT prov, count(*), round(avg(dist_pav_m)), percentile_cont(0.5) WITHIN GROUP (ORDER BY dist_pav_m),
                          round(100.0 * avg((dist_camino_m > 1000)::int), 1)
                   FROM vial.macizos GROUP BY 1 ORDER BY 1""")
    for prov, n, media, mediana, lejos in cur.fetchall():
        log(f'  {prov}: {n:,} macizos · a pavimento media {media:,.0f} m, mediana {mediana:,.0f} m · '
            f'{lejos} % a más de 1 km de cualquier camino')
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
