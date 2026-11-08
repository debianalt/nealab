"""Matriz de eventos de infraestructura de Misiones con sus referencias (noticias v3, fase C: sólo datos).

Un evento = menciones con el mismo tipo, motivo y lugar (la misma pieza: cruce, puente, mojón, paraje, barrio,
asentamiento o municipio, según ubicar.py) encadenadas mientras la fecha del evento no salte más de 2 días.
Cada evento conserva todas sus fuentes, citables.

  noticias.eventos          evento_id, tipo, motivo, precision_lugar (la mejor de sus menciones), lugar_nombre,
                            lugar_clave, fuente_ubicacion, municipio_gid, municipio, fecha_desde, fecha_hasta,
                            n_menciones, n_notas, n_medios, n_oficiales, rutas, km, altura_max_m, geom
  noticias.evento_fuentes   evento_id, mencion_id, nota_id, medio, nombre_medio, fuente_oficial, fecha_nota,
                            fecha_evento, fecha_evento_fuente, titulo, url, oracion, cita
                            cita = «Medio (dd-mm-aaaa). Título. URL»
  noticias.v_matriz_municipio_dia   municipio × día × tipo × motivo: eventos, medios (máx.), oficiales, altura máx.
  noticias.v_matriz_cruce_dia       cruce de infra.cruces (≤ 300 m) × día × tipo × motivo, sólo eventos con precisión
                                    cruce, cruce_inferido, puente o km

Las menciones sin lugar no forman eventos (siguen en noticias.menciones).

Uso (desde neahub/, después de extraer.py y ubicar.py):  python pipeline/noticias/eventos.py
"""
from __future__ import annotations

import io
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
from build_hexoma import DB, connect, log  # noqa: E402

VENTANA_DIAS = 2
ORDEN_PRECISION = ['cruce', 'puente', 'km', 'cruce_inferido', 'hidrometro', 'paraje', 'barrio', 'picada', 'escuela',
                   'nominatim', 'manual', 'cauce_localidad', 'ruta_localidad', 'asentamiento', 'municipio', 'cauce',
                   'contexto_nota']


def main() -> int:
    t0 = time.time()
    conn = connect(DB)
    m = pd.read_sql("""
        SELECT m.mencion_id, m.nota_id, m.medio, m.tipo, m.motivo, m.fecha_nota, m.fecha_evento, m.fecha_evento_fuente,
               m.precision_lugar, m.lugar_nombre, m.lugar_ref, m.fuente_ubicacion, m.ruta, m.km, m.altura_m, m.oracion,
               ST_X(m.geom) lon, ST_Y(m.geom) lat, n.titulo, n.url, f.nombre nombre_medio,
               coalesce(f.fuente_oficial, false) fuente_oficial
        FROM noticias.menciones m JOIN noticias.notas n USING (nota_id) LEFT JOIN noticias.fuentes f ON f.medio = m.medio
        WHERE m.geom IS NOT NULL AND m.fecha_evento IS NOT NULL""", conn)
    # la pieza: la tabla de origen + su id (un contexto de nota y un asentamiento nombrado con el mismo gid se unen)
    tabla = m.fuente_ubicacion.str.split(' ').str[0].str.rstrip(':')
    m['lugar_clave'] = tabla + ':' + m.lugar_ref.astype(str)
    m['fecha_evento'] = pd.to_datetime(m.fecha_evento)
    m = m.sort_values(['tipo', 'motivo', 'lugar_clave', 'fecha_evento', 'mencion_id'])
    nuevo = ((m.tipo != m.tipo.shift()) | (m.motivo != m.motivo.shift()) | (m.lugar_clave != m.lugar_clave.shift())
             | ((m.fecha_evento - m.fecha_evento.shift()).dt.days > VENTANA_DIAS))
    m['evento_id'] = nuevo.cumsum()
    m['rango'] = m.precision_lugar.map({p: i for i, p in enumerate(ORDEN_PRECISION)}).fillna(99)
    # «el Pindaytí» a secas (punto medio del cauce) se suma al evento preciso del MISMO arroyo (cruce, cruce inferido o
    # punto cercano) con el mismo tipo y motivo y fechas a ±2 días: es el mismo hecho contado con menos detalle
    m['arroyo'] = m.lugar_nombre.where(m.precision_lugar.isin(['cauce', 'cruce', 'cruce_inferido', 'cauce_localidad'])
                                       ).str.split(' × ').str[0]
    r = m.groupby('evento_id').agg(tipo=('tipo', 'first'), motivo=('motivo', 'first'), arroyo=('arroyo', 'first'),
                                   rango=('rango', 'min'), d0=('fecha_evento', 'min'), d1=('fecha_evento', 'max'),
                                   n=('mencion_id', 'size'), prec=('precision_lugar', 'first'))
    finos = r[r.prec.isin(['cruce', 'cruce_inferido', 'cauce_localidad']) & r.arroyo.notna()]
    destino = {}
    for i, x in r[(r.prec == 'cauce') & r.arroyo.notna()].iterrows():
        c = finos[(finos.tipo == x.tipo) & (finos.motivo == x.motivo) & (finos.arroyo == x.arroyo)
                  & ((finos.d0 - x.d1).dt.days <= VENTANA_DIAS) & ((x.d0 - finos.d1).dt.days <= VENTANA_DIAS)]
        if len(c):
            destino[i] = c.sort_values(['rango', 'n'], ascending=[True, False]).index[0]
    m['evento_id'] = m.evento_id.map(lambda e: destino.get(e, e))
    log(f'eventos de cauce sumados a su cruce: {len(destino):,}')
    mejor = m.sort_values('rango').drop_duplicates('evento_id').set_index('evento_id')
    ev = m.groupby('evento_id').agg(
        tipo=('tipo', 'first'), motivo=('motivo', 'first'), lugar_clave=('lugar_clave', 'first'),
        fecha_desde=('fecha_evento', 'min'), fecha_hasta=('fecha_evento', 'max'), n_menciones=('mencion_id', 'size'),
        n_notas=('nota_id', 'nunique'), n_medios=('medio', 'nunique'),
        n_oficiales=('fuente_oficial', lambda x: int(x.sum())),
        rutas=('ruta', lambda x: '|'.join(sorted(set(x.dropna()))) or None),
        km=('km', lambda x: '|'.join(sorted(set(x.dropna()))) or None), altura_max_m=('altura_m', 'max'))
    ev = ev.join(mejor[['precision_lugar', 'lugar_nombre', 'fuente_ubicacion', 'lon', 'lat']])
    ev = ev.reset_index()
    m['cita'] = (m.nombre_medio.fillna(m.medio) + ' (' + pd.to_datetime(m.fecha_nota).dt.strftime('%d-%m-%Y') + '). '
                 + m.titulo.fillna('').str.strip().str.rstrip('.') + '. ' + m.url)
    log(f'{len(m):,} menciones con lugar y fecha → {len(ev):,} eventos ({time.time() - t0:.0f} s)')

    cur = conn.cursor()
    cur.execute('DROP VIEW IF EXISTS noticias.v_matriz_municipio_dia, noticias.v_matriz_cruce_dia')
    cur.execute('DROP TABLE IF EXISTS noticias.evento_fuentes, noticias.eventos')
    cur.execute("""CREATE TABLE noticias.eventos (evento_id int PRIMARY KEY, tipo text, motivo text, precision_lugar text,
                   lugar_nombre text, lugar_clave text, fuente_ubicacion text, fecha_desde date, fecha_hasta date,
                   n_menciones int, n_notas int, n_medios int, n_oficiales int, rutas text, km text, altura_max_m real,
                   lon double precision, lat double precision)""")
    cols = ['evento_id', 'tipo', 'motivo', 'precision_lugar', 'lugar_nombre', 'lugar_clave', 'fuente_ubicacion', 'fecha_desde',
            'fecha_hasta', 'n_menciones', 'n_notas', 'n_medios', 'n_oficiales', 'rutas', 'km', 'altura_max_m', 'lon', 'lat']
    buf = io.StringIO()
    ev[cols].to_csv(buf, index=False, header=False, na_rep='\\N', date_format='%Y-%m-%d')
    buf.seek(0)
    cur.copy_expert(f"COPY noticias.eventos ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute("""ALTER TABLE noticias.eventos ADD COLUMN geom geometry(Point, 4326), ADD COLUMN municipio_gid int,
                   ADD COLUMN municipio text""")
    cur.execute('UPDATE noticias.eventos SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326)')
    cur.execute("""UPDATE noticias.eventos e SET municipio_gid =
                   (SELECT gid FROM infra.visor_municipios v ORDER BY v.geom <-> e.geom LIMIT 1)""")
    cur.execute("""UPDATE noticias.eventos e SET municipio = regexp_replace(v.nombre, '^Municipio de ', '')
                   FROM infra.visor_municipios v WHERE v.gid = e.municipio_gid""")
    cur.execute('ALTER TABLE noticias.eventos DROP COLUMN lon, DROP COLUMN lat')
    cur.execute('CREATE INDEX ON noticias.eventos USING gist (geom)')
    cur.execute('CREATE INDEX ON noticias.eventos (fecha_desde, fecha_hasta)')
    cur.execute("""CREATE TABLE noticias.evento_fuentes (evento_id int REFERENCES noticias.eventos, mencion_id int, nota_id int,
                   medio text, nombre_medio text, fuente_oficial boolean, fecha_nota timestamp, fecha_evento date,
                   fecha_evento_fuente text, titulo text, url text, oracion text, cita text)""")
    fcols = ['evento_id', 'mencion_id', 'nota_id', 'medio', 'nombre_medio', 'fuente_oficial', 'fecha_nota', 'fecha_evento',
             'fecha_evento_fuente', 'titulo', 'url', 'oracion', 'cita']
    buf = io.StringIO()
    m[fcols].to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    cur.copy_expert(f"COPY noticias.evento_fuentes ({', '.join(fcols)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    cur.execute('CREATE INDEX ON noticias.evento_fuentes (evento_id)')
    cur.execute("""CREATE VIEW noticias.v_matriz_municipio_dia AS
                   SELECT e.municipio_gid, e.municipio, d::date dia, e.tipo, e.motivo, count(*) eventos,
                          max(e.n_medios) medios_max, sum(e.n_oficiales) oficiales, max(e.altura_max_m) altura_max_m,
                          array_agg(e.evento_id ORDER BY e.evento_id) eventos_id
                   FROM noticias.eventos e, generate_series(e.fecha_desde, e.fecha_hasta, interval '1 day') d
                   GROUP BY 1, 2, 3, 4, 5""")
    cur.execute("""CREATE VIEW noticias.v_matriz_cruce_dia AS
                   SELECT c.id cruce_id, d::date dia, e.tipo, e.motivo, count(*) eventos, max(e.n_medios) medios_max,
                          array_agg(e.evento_id ORDER BY e.evento_id) eventos_id
                   FROM noticias.eventos e
                   JOIN LATERAL (SELECT id FROM infra.cruces c WHERE ST_DWithin(c.geom::geography, e.geom::geography, 300)
                                 ORDER BY c.geom <-> e.geom LIMIT 1) c ON true,
                        generate_series(e.fecha_desde, e.fecha_hasta, interval '1 day') d
                   WHERE e.precision_lugar IN ('cruce', 'cruce_inferido', 'puente', 'km')
                   GROUP BY 1, 2, 3, 4""")
    cur.execute("COMMENT ON TABLE noticias.eventos IS 'Eventos de infraestructura de Misiones según la prensa y el portal "
                "oficial: tipo, motivo, lugar con su precisión y fuente, fechas, cantidad de medios; fuentes citables en "
                "noticias.evento_fuentes (pipeline/noticias/eventos.py)'")
    conn.commit()
    r = pd.read_sql("""SELECT tipo, motivo, count(*) eventos, round(avg(n_medios), 2) medios_prom,
                              count(*) FILTER (WHERE n_medios >= 2) corroborados
                       FROM noticias.eventos GROUP BY 1, 2 ORDER BY 3 DESC""", conn)
    conn.close()
    log(f'listo en {time.time() - t0:.0f} s\n' + r.to_string(index=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
