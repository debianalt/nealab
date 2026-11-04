"""Red vial detallada del NEA argentino (Misiones, Corrientes, Chaco, Formosa) en el Hexoma: esquema `vial`.

Fuentes (medidas el 4-nov-2026 en zonas forestales de prueba):
  - OpenStreetMap (Geofabrik, extracto Argentina): la base. En Misiones trae las huellas forestales casi completas
    y la superficie cargada en ~todas las rutas. ODbL.
  - Microsoft ML Road Detections (Bing, GeoParquet en source.coop): caminos detectados por imagen, sin clase ni
    superficie. Sólo se suma lo que queda a más de ~25 m de cualquier camino de OSM (en Corrientes NE agrega
    ~1.200 km donde OSM tiene ~250 km de huellas). ODbL.
  - IGN (WFS wms.ign.gob.ar, capas vial_nacional / vial_provincial / vial_terciaria / vial_AP010 / vial_AP050):
    jurisdicción y superficie oficiales, red escasa. Tabla de referencia, no entra en la red unificada. Uso libre
    con cita del IGN.

Tablas (SRID 4326):
  vial.osm        un tramo OSM con highway (todas las clases), surface, tracktype, smoothness, ref, tags (jsonb)
  vial.ms         un tramo de Microsoft; geom_extra = la parte a más de ~25 m de OSM (NULL si OSM ya lo cubre)
  vial.ign        un tramo IGN con rst/typ/hct/fun y sus etiquetas
  vial.caminos    red unificada para análisis: OSM (sin sendas peatonales ni obras) + lo que Microsoft agrega,
                  con clase y superficie normalizadas en castellano y departamento (redcode INDEC)
  vial.fuentes    fuente, URL, fecha, licencia, cita

Cada tramo lleva el departamento (`redcode` de public.limites) del punto del tramo que cae sobre la línea
(ST_PointOnSurface); los tramos cuyo punto cae fuera de los 76 departamentos del NEA AR se descartan.

Uso (desde neahub/, con el Hexoma prendido):
    python pipeline/vial/importar_vial.py [--pbf ruta.osm.pbf] [--refrescar]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
from build_hexoma import DB, connect, log  # noqa: E402

OUT = ROOT / 'pipeline' / 'output' / 'vial'
PBF_URL = 'https://download.geofabrik.de/south-america/argentina-latest.osm.pbf'
MS_URL = 'https://data.source.coop/nlebovits/microsoft-ml-road-detections/road-detections/by_country/country=ARG/ARG.parquet'
IGN_WFS = 'https://wms.ign.gob.ar/geoserver/ign/ows'
IGN_CAPAS = ['vial_nacional', 'vial_provincial', 'vial_terciaria', 'vial_AP010', 'vial_AP050']
QGIS = Path(os.getenv('QGIS_HOME', r'C:\Program Files\QGIS 3.40.13'))
OGR2OGR = os.getenv('OGR2OGR', str(QGIS / 'bin' / 'ogr2ogr.exe'))
UA = {'User-Agent': 'spatia-pipeline/1.0 (+https://www.spatia.ar)'}
BUF = 0.00025   # ~25 m en grados (22–28 m en el NEA): un camino de Microsoft a menos de esto ya está en OSM
MIN_EXTRA_M = 30  # pedazos sueltos más cortos (cruces, puntas) no son caminos nuevos

# Metadatos IGN (red_vial_*.pdf, 24-10-2021)
IGN_RST = {-1: 'sin dato', 1: 'pavimento', 19: 'ripio/consolidado', 13: 'tierra'}
IGN_HCT = {-1: 'sin dato', 1: 'ruta nacional', 2: 'ruta provincial', 4: 'camino terciario', 5: 'camino vecinal'}
IGN_TYP = {-1: 'sin dato', 41: 'autopista', 47: 'autovía', 51: 'camino de un carril', 52: 'vía de autopista',
           53: 'vía de autovía', 54: 'camino de dos carriles'}
IGN_FUN = {-1: 'sin dato', 2: 'abandonado', 4: 'desmantelado', 6: 'activo', 9: 'en construcción'}

# OSM highway → clase (None = fuera de la red de caminos para vehículos)
CLASE = {
    'motorway': 'troncal', 'motorway_link': 'troncal', 'trunk': 'troncal', 'trunk_link': 'troncal',
    'primary': 'primaria', 'primary_link': 'primaria', 'secondary': 'secundaria', 'secondary_link': 'secundaria',
    'tertiary': 'terciaria', 'tertiary_link': 'terciaria', 'unclassified': 'vecinal', 'road': 'vecinal',
    'track': 'huella', 'residential': 'urbana', 'living_street': 'urbana', 'service': 'servicio',
}
PAVIMENTO = ('asphalt', 'paved', 'concrete', 'concrete:plates', 'concrete:lanes', 'paving_stones', 'sett',
             'cobblestone', 'cobblestone:flattened', 'unhewn_cobblestone', 'chipseal', 'metal', 'wood')
RIPIO = ('gravel', 'fine_gravel', 'compacted', 'pebblestone', 'rock', 'shells')
TIERRA = ('unpaved', 'dirt', 'ground', 'earth', 'mud', 'sand', 'grass', 'soil')


def ahora() -> str:
    return dt.datetime.now().strftime('%Y-%m-%d %H:%M')


def bbox_nea() -> tuple[float, float, float, float]:
    conn = connect(DB)
    with conn.cursor() as cur:
        cur.execute('SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e) FROM (SELECT ST_Extent(geom) e FROM limites) s')
        b = cur.fetchone()
    conn.close()
    return tuple(round(v, 4) for v in b)


def descargar(url: str, dest: Path) -> str:
    """Descarga en streaming; devuelve Last-Modified (o la hora local)."""
    log(f'descargando {url}')
    with requests.get(url, stream=True, timeout=120, headers=UA) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + '.part')
        with open(tmp, 'wb') as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
        tmp.replace(dest)
        return r.headers.get('Last-Modified', ahora())


def osm_parquet(pbf: Path, bbox) -> Path:
    out = OUT / 'osm_nea.parquet'
    out.unlink(missing_ok=True)
    env = {**os.environ, 'PROJ_DATA': str(QGIS / 'share' / 'proj'), 'OSM_MAX_TMPFILE_SIZE': '1024'}
    subprocess.run([OGR2OGR, '-f', 'Parquet', str(out), str(pbf), 'lines', '-where', 'highway IS NOT NULL',
                    '-spat', *map(str, bbox), '-select', 'osm_id,name,highway,other_tags'], check=True, env=env)
    return out


def ms_parquet(bbox) -> Path:
    out = OUT / 'ms_nea.parquet'
    d = duckdb.connect()
    d.execute('INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;')
    x0, y0, x1, y1 = bbox
    d.execute(f"""COPY (SELECT width_meters AS width_m, geometry FROM read_parquet('{MS_URL}')
                 WHERE bbox.xmax > {x0} AND bbox.xmin < {x1} AND bbox.ymax > {y0} AND bbox.ymin < {y1})
                 TO '{out.as_posix()}' (FORMAT parquet)""")
    return out


def ign_geojson(bbox) -> list[Path]:
    x0, y0, x1, y1 = bbox
    outs = []
    for capa in IGN_CAPAS:
        r = requests.get(IGN_WFS, timeout=300, headers=UA, params={
            'service': 'WFS', 'version': '1.0.0', 'request': 'GetFeature', 'typeName': f'ign:{capa}',
            'outputFormat': 'application/json', 'CQL_FILTER': f'BBOX(geom,{x0},{y0},{x1},{y1})'})
        r.raise_for_status()
        fc = r.json()
        p = OUT / f'ign_{capa}.geojson'
        p.write_text(json.dumps(fc), encoding='utf-8')
        log(f'  IGN {capa}: {len(fc.get("features", [])):,} tramos')
        outs.append(p)
    return outs


def a_csv(d: duckdb.DuckDBPyConnection, select: str, dest: Path) -> Path:
    d.execute(f"COPY ({select}) TO '{dest.as_posix()}' (FORMAT csv, HEADER false)")
    return dest


def copiar(cur, tabla: str, cols: list[str], csv: Path) -> None:
    with open(csv, encoding='utf-8') as f:
        cur.copy_expert(f"COPY {tabla} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '')", f)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--pbf', help='extracto OSM Argentina ya descargado (si no, se baja de Geofabrik)')
    ap.add_argument('--refrescar', action='store_true', help='volver a bajar todas las fuentes')
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    bbox = bbox_nea()
    log(f'bbox NEA AR: {bbox}')
    fuentes = []

    # ── descargas ────────────────────────────────────────────────────────────────
    pbf = Path(args.pbf) if args.pbf else OUT / 'argentina-latest.osm.pbf'
    if args.pbf:
        fecha_osm = dt.datetime.fromtimestamp(pbf.stat().st_mtime).strftime('%Y-%m-%d')
    elif args.refrescar or not pbf.exists():
        fecha_osm = descargar(PBF_URL, pbf)
    else:
        fecha_osm = dt.datetime.fromtimestamp(pbf.stat().st_mtime).strftime('%Y-%m-%d')
    osm_pq = osm_parquet(pbf, bbox)
    fuentes.append(('OpenStreetMap (Geofabrik, Argentina)', PBF_URL, fecha_osm, 'ODbL 1.0',
                    '© OpenStreetMap contributors'))
    ms_pq = OUT / 'ms_nea.parquet'
    if args.refrescar or not ms_pq.exists():
        log('Microsoft ML Road Detections (ARG, bbox NEA)…')
        ms_parquet(bbox)
    fuentes.append(('Microsoft ML Road Detections', MS_URL, 'versión 2023 (espejo source.coop)', 'ODbL 1.0',
                    'Microsoft, Road Detections from Microsoft Maps Aerial Imagery'))
    log('IGN WFS…')
    ign = ign_geojson(bbox)
    fuentes.append(('IGN Argentina, red vial (WFS)', IGN_WFS, ahora()[:10], 'Libre uso con cita del IGN',
                    'Instituto Geográfico Nacional de la República Argentina'))

    # ── a CSV (DuckDB) ───────────────────────────────────────────────────────────
    d = duckdb.connect()
    d.execute('INSTALL spatial; LOAD spatial;')
    tag = lambda k: f"""nullif(regexp_extract(other_tags, '"{k}"=>"([^"]*)"', 1), '')"""  # noqa: E731
    osm_csv = a_csv(d, f"""SELECT osm_id, name, highway, {tag('ref')}, {tag('surface')}, {tag('tracktype')},
        {tag('smoothness')}, other_tags, ST_AsHEXWKB(geometry) FROM '{osm_pq.as_posix()}'""", OUT / 'osm.csv')
    ms_csv = a_csv(d, f"SELECT width_m, ST_AsHEXWKB(geometry) FROM '{ms_pq.as_posix()}'", OUT / 'ms.csv')
    ign_csvs = []
    for p in ign:
        capa = p.stem.removeprefix('ign_')
        n = d.execute(f"SELECT count(*) FROM ST_Read('{p.as_posix()}')").fetchone()[0]
        if not n:
            continue
        hay = {r[0] for r in d.execute(f"DESCRIBE SELECT * FROM ST_Read('{p.as_posix()}')").fetchall()}
        col = lambda c: c if c in hay else 'NULL'  # noqa: E731  (las capas no traen todas los mismos campos)
        sel = ', '.join(col(c) for c in ('gid', 'objeto', 'fna1', 'gna1', 'rst', 'typ', 'hct', 'fun', 'fdc'))
        ign_csvs.append(a_csv(d, f"""SELECT '{capa}', {sel}, ST_AsHEXWKB(geom) FROM ST_Read('{p.as_posix()}')""",
                              OUT / f'ign_{capa}.csv'))

    # ── Postgres ─────────────────────────────────────────────────────────────────
    conn = connect(DB)
    cur = conn.cursor()
    log('cargando en el Hexoma (esquema vial)…')
    cur.execute('CREATE SCHEMA IF NOT EXISTS vial; CREATE EXTENSION IF NOT EXISTS hstore')
    cur.execute("""
        DROP TABLE IF EXISTS vial.caminos, vial.osm, vial.ms, vial.ign, vial.fuentes, vial._deptos;
        CREATE UNLOGGED TABLE vial._osm (osm_id text, name text, highway text, ref text, surface text,
            tracktype text, smoothness text, other_tags text, g text);
        CREATE UNLOGGED TABLE vial._ms (width_m double precision, g text);
        CREATE UNLOGGED TABLE vial._ign (capa text, gid bigint, objeto text, nombre text, generico text,
            rst int, typ int, hct int, fun int, fdc text, g text);
        CREATE TABLE vial._deptos AS SELECT l.redcode, t.territorio AS provincia, l.geom
            FROM limites l JOIN territorios t USING (territorio_id);
        CREATE INDEX ON vial._deptos USING gist (geom);
    """)
    copiar(cur, 'vial._osm', ['osm_id', 'name', 'highway', 'ref', 'surface', 'tracktype', 'smoothness',
                              'other_tags', 'g'], osm_csv)
    copiar(cur, 'vial._ms', ['width_m', 'g'], ms_csv)
    for c in ign_csvs:
        copiar(cur, 'vial._ign', ['capa', 'gid', 'objeto', 'nombre', 'generico', 'rst', 'typ', 'hct', 'fun', 'fdc', 'g'], c)
    log(f'  staging listo ({time.time() - t0:.0f} s)')

    # Departamento por el punto del tramo sobre la línea; fuera del NEA AR → afuera.
    def con_depto(src: str, cols: str) -> str:
        return f"""SELECT {cols}, dp.redcode, dp.provincia, s.geom FROM (SELECT *, ST_SetSRID(g::geometry, 4326) AS geom FROM {src}) s
                   JOIN LATERAL (SELECT redcode, provincia FROM vial._deptos dp
                                 WHERE ST_Intersects(dp.geom, ST_PointOnSurface(s.geom)) LIMIT 1) dp ON true"""

    cur.execute(f"""
        CREATE TABLE vial.osm AS {con_depto('vial._osm', '''s.osm_id, s.name, s.highway, s.ref, s.surface, s.tracktype,
            s.smoothness, CASE WHEN s.other_tags IS NULL THEN NULL
                               ELSE hstore_to_jsonb(s.other_tags::hstore) END AS tags''')};
        CREATE INDEX ON vial.osm USING gist (geom);
        CREATE INDEX ON vial.osm (highway);
    """)
    log(f'  vial.osm listo ({time.time() - t0:.0f} s)')
    cur.execute(f"""
        CREATE TABLE vial.ms AS {con_depto('vial._ms', 's.width_m')};
        ALTER TABLE vial.ms ADD COLUMN id serial PRIMARY KEY, ADD COLUMN geom_extra geometry(MultiLineString, 4326);
        CREATE INDEX ON vial.ms USING gist (geom);
    """)
    log(f'  vial.ms listo ({time.time() - t0:.0f} s); restando lo que OSM ya tiene…')
    # Tramos de Microsoft sin ningún camino OSM a ~25 m: enteros. Con OSM cerca: la diferencia con el buffer local.
    cur.execute(f"""
        UPDATE vial.ms m SET geom_extra = ST_Multi(m.geom)
        WHERE NOT EXISTS (SELECT 1 FROM vial.osm o WHERE ST_DWithin(o.geom, m.geom, {BUF}));
        UPDATE vial.ms m SET geom_extra = ST_Multi(ST_CollectionExtract(ST_Difference(m.geom,
            (SELECT ST_Union(ST_Buffer(o.geom, {BUF}, 'quad_segs=2')) FROM vial.osm o
             WHERE ST_DWithin(o.geom, m.geom, {BUF}))), 2))
        WHERE m.geom_extra IS NULL;
        UPDATE vial.ms SET geom_extra = NULL WHERE ST_IsEmpty(geom_extra);
    """)
    log(f'  diferencia lista ({time.time() - t0:.0f} s)')
    rst = ' '.join(f"WHEN {k} THEN '{v}'" for k, v in IGN_RST.items())
    hct = ' '.join(f"WHEN {k} THEN '{v}'" for k, v in IGN_HCT.items())
    typ = ' '.join(f"WHEN {k} THEN '{v}'" for k, v in IGN_TYP.items())
    fun = ' '.join(f"WHEN {k} THEN '{v}'" for k, v in IGN_FUN.items())
    cur.execute(f"""
        CREATE TABLE vial.ign AS {con_depto('vial._ign', f'''s.capa, s.gid, s.objeto, s.nombre, s.generico,
            s.rst, CASE s.rst {rst} END AS superficie, s.hct, CASE s.hct {hct} END AS jurisdiccion,
            s.typ, CASE s.typ {typ} END AS tipo_via, s.fun, CASE s.fun {fun} END AS estado, s.fdc AS fuente_captura''')};
        CREATE INDEX ON vial.ign USING gist (geom);
    """)
    clase = ' '.join(f"WHEN '{k}' THEN '{v}'" for k, v in CLASE.items())
    lista = lambda xs: ', '.join(f"'{x}'" for x in xs)  # noqa: E731
    cur.execute(f"""
        CREATE TABLE vial.caminos AS
        SELECT 'osm' AS fuente, osm_id AS fuente_id, CASE highway {clase} END AS clase, highway AS clase_osm,
               CASE WHEN surface IN ({lista(PAVIMENTO)}) THEN 'pavimento'
                    WHEN surface IN ({lista(RIPIO)}) THEN 'ripio/consolidado'
                    WHEN surface IN ({lista(TIERRA)}) THEN 'tierra'
                    WHEN surface IS NULL AND highway IN ('motorway', 'motorway_link', 'trunk', 'trunk_link',
                                                         'primary', 'primary_link') THEN 'pavimento'
                    WHEN surface IS NULL AND highway = 'track' THEN 'tierra'
                    ELSE 'desconocida' END AS superficie,
               surface IS NOT NULL AS superficie_etiquetada, name AS nombre, ref, redcode, provincia,
               ST_Multi(geom)::geometry(MultiLineString, 4326) AS geom
        FROM vial.osm WHERE highway IN ({lista(CLASE)})
        UNION ALL
        SELECT 'microsoft', id::text, 'detectado por imagen', NULL, 'desconocida', false, NULL, NULL, redcode, provincia,
               geom_extra
        FROM vial.ms WHERE geom_extra IS NOT NULL AND ST_Length(geom_extra::geography) >= {MIN_EXTRA_M};
        ALTER TABLE vial.caminos ADD COLUMN id serial PRIMARY KEY;
        ALTER TABLE vial.caminos ADD COLUMN largo_m double precision;
        UPDATE vial.caminos SET largo_m = ST_Length(geom::geography);
        CREATE INDEX ON vial.caminos USING gist (geom);
        CREATE INDEX ON vial.caminos (superficie);
        CREATE INDEX ON vial.caminos (clase);
        CREATE TABLE vial.fuentes (fuente text, url text, fecha text, licencia text, cita text);
        DROP TABLE vial._osm, vial._ms, vial._ign;
    """)
    cur.executemany('INSERT INTO vial.fuentes VALUES (%s, %s, %s, %s, %s)', fuentes)
    cur.execute("""
        COMMENT ON SCHEMA vial IS 'Red vial NEA AR: OSM + Microsoft ML Road Detections (fuera de OSM) + IGN (referencia). pipeline/vial/importar_vial.py';
        COMMENT ON TABLE vial.caminos IS 'Red unificada para análisis: OSM (vehicular) + tramos de Microsoft a más de ~25 m de OSM. superficie: pavimento | ripio/consolidado | tierra | desconocida (troncal/primaria sin etiqueta = pavimento; huella sin etiqueta = tierra).';
    """)
    conn.commit()

    # ── resumen ──────────────────────────────────────────────────────────────────
    cur.execute("""SELECT provincia, clase, superficie, round(sum(largo_m) / 1000) km FROM vial.caminos
                   GROUP BY 1, 2, 3 ORDER BY 1, 4 DESC""")
    filas = cur.fetchall()
    for prov in ('misiones', 'corrientes', 'chaco', 'formosa'):
        fs = [f for f in filas if f[0] == prov]
        tot = sum(f[3] for f in fs)
        log(f'{prov}: {tot:,.0f} km')
        for _, cl, su, km in fs[:12]:
            log(f'    {cl:<22} {su:<18} {km:>9,.0f} km')
    # Zonas de prueba medidas el 4-nov (OSM en caja, Microsoft fuera de OSM)
    for nombre, (x0, y0, x1, y1) in {'Eldorado–San Pedro': (-54.6, -26.7, -54.0, -26.0),
                                     'Corrientes NE': (-56.6, -28.4, -56.0, -27.7),
                                     'Virasoro': (-56.3, -28.3, -55.8, -28.0)}.items():
        cur.execute(f"""SELECT fuente, round(sum(ST_Length(ST_Intersection(geom, e)::geography)) / 1000)
                        FROM vial.caminos, ST_MakeEnvelope({x0}, {y0}, {x1}, {y1}, 4326) e
                        WHERE ST_Intersects(geom, e) GROUP BY 1 ORDER BY 1""")
        log(f'  prueba {nombre}: {cur.fetchall()}')
    cur.execute("SELECT capa, count(*), round(sum(ST_Length(geom::geography)) / 1000) FROM vial.ign GROUP BY 1 ORDER BY 1")
    log(f'  IGN: {cur.fetchall()}')
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
