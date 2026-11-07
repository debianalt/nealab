"""Infraestructura del visor de emergencias de Misiones → esquema `infra` del Hexoma.

El visor (sig.misiones.gob.ar/mapas/emergencia/, Dir. de Modernización; código en
gitlab.com/Modernizacion.Misiones/mapa-emergencia) sirve sus capas como GeoJSON estáticos en
/datos/. Casi todas salen de OSM, pero algunas son propias y no están en ningún otro lado:
  - puentes-RP2: 29 puentes de la Ruta Provincial 2 (costa del Uruguay) con la COTA DE SEGURIDAD
    en msnm, el nivel del río al que el puente queda cortado;
  - altura-rios: 35 hidrómetros (Prefectura/INA) con niveles de alerta y evacuación y el series_id
    del INA, que permite bajar la serie histórica;
  - hospitales y CAPS con códigos SISA y CUIE; municipios con su zona de gestión de riesgo.

Cada capa va a su tabla `infra.visor_<capa>` (gid, nombre, props jsonb con todo lo original, geom
4326) y a `infra.fuentes`. Las capas con atributos numéricos que se usan después tienen columnas
tipadas además del jsonb. Los crudos quedan en %HEXOMA_FUENTES%/infra/visor/.

Uso (desde neahub/):
    python pipeline/infra/importar_visor.py [--rebajar]
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
from build_hexoma import DB, connect, log  # noqa: E402

BASE = 'https://sig.misiones.gob.ar/mapas/emergencia/datos/'
CRUDOS = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes')) / 'infra' / 'visor'
UA = {'User-Agent': 'Mozilla/5.0 (spatia-pipeline; +https://spatia.ar)'}

# tabla → (archivo, campo de nombre, descripción)
CAPAS = {
    'puentes': ('puentes.geojson', 'name', 'Puentes (puntos derivados de OSM, con Mapillary)'),
    'puentes_rp2': ('puentes-RP2.geojson', 'nombre', 'Puentes de la RP2 con cota de seguridad (msnm)'),
    'alcantarillas': ('alcantarillas.min.geojson', 'waterway_name', 'Alcantarillas (cruce camino × cauce)'),
    'cauces': ('RiosArroyos.min.geojson', 'name', 'Ríos y arroyos (OSM)'),
    'lineas_at': ('alta-tension.min.geojson', 'name', 'Líneas eléctricas de 13,2 a 500 kV'),
    'transformadoras': ('estacion-transformadora.min.geojson', None, 'Estaciones transformadoras'),
    'antenas': ('antenas.geojson', 'operator', 'Antenas y mástiles de comunicación (OSM)'),
    'hospitales': ('hospitales.min.geojson', 'name', 'Hospitales (con códigos SISA/CUIE)'),
    'caps': ('caps.min.geojson', 'Nombre', 'Centros de atención primaria (CAPS)'),
    'municipios': ('zonas_GIR.geojson', 'name', 'Municipios con zona de gestión integral del riesgo'),
    'zonas_inundables': ('zonas-inundables.min.N.geojson', 'Name', 'Zonas inundables (Posadas, 1983/2014)'),
    'hidrometros': ('altura-rios.json', 'nombre', 'Hidrómetros con niveles de alerta/evacuación (INA)'),
}

TIPADAS = {
    'puentes_rp2': """ALTER TABLE infra.visor_puentes_rp2
        ADD COLUMN cota_seguridad_msnm double precision
            GENERATED ALWAYS AS (NULLIF(props->>'cota_seguridad', '')::double precision) STORED,
        ADD COLUMN largo_m double precision
            GENERATED ALWAYS AS (NULLIF(props->>'largo', '')::double precision) STORED""",
    'hidrometros': """ALTER TABLE infra.visor_hidrometros
        ADD COLUMN rio text GENERATED ALWAYS AS (props->>'rio') STORED,
        ADD COLUMN series_id integer GENERATED ALWAYS AS ((props->>'series_id')::integer) STORED,
        ADD COLUMN tabla text GENERATED ALWAYS AS (props->>'tabla') STORED,
        ADD COLUMN nivel_alerta_m double precision GENERATED ALWAYS AS ((props->>'nivel_de_alerta')::double precision) STORED,
        ADD COLUMN nivel_evacuacion_m double precision GENERATED ALWAYS AS ((props->>'nivel_de_evacuacion')::double precision) STORED,
        ADD COLUMN nivel_aguas_bajas_m double precision GENERATED ALWAYS AS ((props->>'nivel_de_aguas_bajas')::double precision) STORED""",
    'lineas_at': """ALTER TABLE infra.visor_lineas_at
        ADD COLUMN tension_kv double precision
            GENERATED ALWAYS AS (NULLIF(split_part(props->>'voltage', ';', 1), '')::double precision / 1000) STORED""",
}


def bajar(archivo: str, rebajar: bool) -> tuple[Path, str]:
    dest = CRUDOS / archivo
    if dest.exists() and not rebajar:
        return dest, dt.datetime.fromtimestamp(dest.stat().st_mtime).strftime('%Y-%m-%d')
    r = requests.get(BASE + archivo, headers=UA, timeout=180)
    r.raise_for_status()
    json.loads(r.content.decode('utf-8'))  # valida antes de guardar
    dest.write_bytes(r.content)
    fecha = r.headers.get('Last-Modified', dt.date.today().isoformat())
    return dest, fecha


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--rebajar', action='store_true')
    args = ap.parse_args()
    CRUDOS.mkdir(parents=True, exist_ok=True)
    conn = connect(DB)
    cur = conn.cursor()
    cur.execute('CREATE SCHEMA IF NOT EXISTS infra')
    cur.execute("""CREATE TABLE IF NOT EXISTS infra.fuentes (
        tabla text PRIMARY KEY, fuente text, url text, fecha_fuente text, cargado timestamptz, n integer, nota text)""")
    for capa, (archivo, campo_nombre, desc) in CAPAS.items():
        path, fecha = bajar(archivo, args.rebajar)
        fc = json.loads(path.read_text(encoding='utf-8'))
        feats = [f for f in fc.get('features', []) if f.get('geometry')]
        t = f'infra.visor_{capa}'
        cur.execute(f'DROP TABLE IF EXISTS {t}')
        cur.execute(f'CREATE TABLE {t} (gid serial PRIMARY KEY, nombre text, props jsonb, geom geometry(Geometry, 4326))')
        filas = []
        for f in feats:
            p = f.get('properties') or {}
            nombre = (p.get(campo_nombre) if campo_nombre else None)
            filas.append((str(nombre).strip() if nombre not in (None, '') else None, Json(p), json.dumps(f['geometry'])))
        execute_values(cur, f'INSERT INTO {t} (nombre, props, geom) VALUES %s', filas,
                       template='(%s, %s, ST_Force2D(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)))', page_size=2000)
        if capa in TIPADAS:
            cur.execute(TIPADAS[capa])
        cur.execute(f'CREATE INDEX ON {t} USING gist (geom)')
        cur.execute(f"COMMENT ON TABLE {t} IS %s", (f'{desc}. Fuente: visor de emergencias de Misiones, {BASE}{archivo}',))
        cur.execute("""INSERT INTO infra.fuentes VALUES (%s, %s, %s, %s, now(), %s, %s)
                       ON CONFLICT (tabla) DO UPDATE SET fuente=EXCLUDED.fuente, url=EXCLUDED.url,
                       fecha_fuente=EXCLUDED.fecha_fuente, cargado=now(), n=EXCLUDED.n, nota=EXCLUDED.nota""",
                    (t, 'Visor de emergencias de Misiones (Dir. de Modernización)', BASE + archivo, fecha, len(filas), desc))
        log(f'  {t}: {len(filas):,} (de {len(fc.get("features", [])):,})')
    conn.commit()
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
