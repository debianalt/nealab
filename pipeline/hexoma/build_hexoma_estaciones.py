"""Hexoma — esquema `estaciones`: observaciones meteorológicas de superficie del NEA, la referencia obligada.

Toda afirmación del Hexoma sobre temperatura, heladas o lluvia se contrasta con estas estaciones: son
la única medición directa. Los productos grillados (CHIRPS, ERA5-Land) se validan y se corrigen con
ellas (validar_clima.py, corregir_temperatura.py), y la ficha de cada celda muestra la estación más
cercana al lado de cada serie modelada.

Fuentes (ya bajadas a %HEXOMA_FUENTES%/estaciones/):
    GHCN-Daily (NOAA)  ghcnd/<id>.dly: SMN de Argentina, DMH de Paraguay, aeropuertos de Brasil
    INTA SIGA          extra/siga_<id>_<nombre>.csv|json, de pipeline/hexoma/bajar_siga.py

Objetos:
    estacion           metadatos, completitud por variable, píxeles ERA5/CHIRPS que la contienen
    diario             (estacion_id, fecha, tmin, tmax, p, tmin_suelo, qc) tal cual vienen; qc marca, no borra
    anual              por año calendario: heladas (abrigo < 0, < −2, ≤ 3 °C; a 5 cm < 0), primera y
                       última helada, mínima absoluta, días > 35 °C, n de días válidos
    campania           por campaña jul–jun: lluvia, días de lluvia ≥ 1 mm, grados-día base 10, días > 35 °C
    normal_1991_2020   media, mediana, P10, P90 con ≥ 20 períodos completos (≥ 95 % de días válidos)
    h3_cercana         por celda: la estación más cercana con temperatura y la convencional más cercana
                       (series largas), distancia (km) y diferencia de elevación (estación − celda, m)

qc (máscara de bits): 1 tmin > tmax · 2 tmin fuera de −15..45 °C · 4 tmax fuera de −10..48 °C ·
8 lluvia < 0 o > 400 mm · 16 salto de tmin > 15 °C con el día anterior · 32 salto de tmax > 15 °C ·
64 tmin_suelo fuera de −20..45 °C. Los conteos usan sólo valores sin la marca correspondiente.

Idempotente. Uso (desde neahub/):
    python pipeline/hexoma/build_hexoma_estaciones.py
"""
from __future__ import annotations

import io
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_hexoma import DB, connect, log  # noqa: E402
from validar_clima import ESTACIONES, leer_dly  # noqa: E402

FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
EST = FUENTES_DIR / 'estaciones'
PAIS = {'AR': 'Argentina', 'BR': 'Brasil', 'PA': 'Paraguay'}


def q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def copy_df(cur, df: pd.DataFrame, tabla: str) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep='')
    buf.seek(0)
    cur.copy_expert(f"COPY {tabla} ({', '.join(df.columns)}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)


def qc(d: pd.DataFrame) -> pd.Series:
    m = pd.Series(0, index=d.index, dtype='int16')
    tn, tx, p = d.get('tmin'), d.get('tmax'), d.get('p')
    if tn is not None and tx is not None:
        m |= ((tn > tx) & tn.notna() & tx.notna()).astype('int16') * 1
    if tn is not None:
        m |= ((tn < -15) | (tn > 45)).astype('int16') * 2
        salto = tn.diff().abs()
        m |= ((salto > 15) & (d.fecha.diff().dt.days == 1)).astype('int16') * 16
    if tx is not None:
        m |= ((tx < -10) | (tx > 48)).astype('int16') * 4
        salto = tx.diff().abs()
        m |= ((salto > 15) & (d.fecha.diff().dt.days == 1)).astype('int16') * 32
    if p is not None:
        m |= ((p < 0) | (p > 400)).astype('int16') * 8
    if 'tmin_suelo' in d:
        ts = d.tmin_suelo
        m |= ((ts < -20) | (ts > 45)).astype('int16') * 64
    return m


def leer() -> tuple[pd.DataFrame, pd.DataFrame]:
    metas, diarios = [], []
    for sid, (nombre, lat, lng, elev) in ESTACIONES.items():
        f = EST / 'ghcnd' / f'{sid}.dly'
        if not f.exists():
            continue
        d = leer_dly(f).reset_index()
        eid = f'ghcn:{sid}'
        tipo = 'sinóptica (SMN)' if sid.startswith('AR') else 'sinóptica / aeropuerto'
        metas.append(dict(estacion_id=eid, red='GHCN-Daily (NOAA)', id_origen=sid, nombre=nombre, tipo=tipo,
                          pais=PAIS.get(sid[:2]), lat=lat, lng=lng, elev_m=elev))
        d['estacion_id'] = eid
        diarios.append(d)
    for f in sorted((EST / 'extra').glob('siga_*.csv')):
        m = json.loads(f.with_suffix('.json').read_text(encoding='utf-8'))
        d = pd.read_csv(f, parse_dates=['fecha'])
        eid = f"inta:{m['id_siga']}"
        conv = m.get('id_tipo') == 1 or 'Convencional' in (m.get('tipo') or '')
        metas.append(dict(estacion_id=eid, red='INTA SIGA', id_origen=str(m['id_siga']), nombre=m['nombre'],
                          tipo=('convencional (EMC)' if conv else f"automática ({m.get('tipo')})"),
                          pais='Argentina', provincia=m.get('provincia'), lat=m['lat'], lng=m['lng'],
                          elev_m=m.get('elev') if m.get('elev') else None))
        d['estacion_id'] = eid
        diarios.append(d)
    meta = pd.DataFrame(metas)
    dia = pd.concat(diarios, ignore_index=True)
    for c in ('tmin', 'tmax', 'p', 'tmin_suelo'):
        if c not in dia:
            dia[c] = np.nan
    dia = dia[['estacion_id', 'fecha', 'tmin', 'tmax', 'p', 'tmin_suelo']].sort_values(['estacion_id', 'fecha'])
    dia = dia.drop_duplicates(['estacion_id', 'fecha'])
    dia['qc'] = pd.concat([qc(g) for _, g in dia.groupby('estacion_id', sort=False)])
    return meta, dia


def main() -> None:
    t0 = time.time()
    meta, dia = leer()
    log(f'{len(meta)} estaciones, {len(dia):,} días; con alguna marca de QC: {(dia.qc > 0).sum():,}')
    conn = connect(DB)
    with conn, conn.cursor() as cur:
        cur.execute('SET client_min_messages = warning')
        cur.execute("SET work_mem = '512MB'")
        cur.execute('DROP SCHEMA IF EXISTS estaciones CASCADE')
        cur.execute('CREATE SCHEMA estaciones')
        cur.execute("COMMENT ON SCHEMA estaciones IS 'Observaciones de superficie (GHCN-Daily, INTA SIGA): la "
                    "referencia obligada para toda afirmación del Hexoma sobre temperatura, heladas o lluvia. "
                    "pipeline/hexoma/build_hexoma_estaciones.py'")
        cur.execute("""CREATE TABLE estaciones.estacion (estacion_id text PRIMARY KEY, red text, id_origen text,
                       nombre text, tipo text, pais text, provincia text, lat float8, lng float8, elev_m real,
                       geom geometry(Point, 4326), desde date, hasta date, n_dias int, pct_tmin real, pct_tmax real,
                       pct_p real, pct_tmin_suelo real, era5_pixel bigint, chirps_pixel bigint, z_era5_m real)""")
        cols = ['estacion_id', 'red', 'id_origen', 'nombre', 'tipo', 'pais', 'provincia', 'lat', 'lng', 'elev_m']
        for c in cols:
            if c not in meta:
                meta[c] = None
        copy_df(cur, meta[cols], 'estaciones.estacion')
        cur.execute('UPDATE estaciones.estacion SET geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326)')
        cur.execute("""CREATE TABLE estaciones.diario (estacion_id text REFERENCES estaciones.estacion, fecha date,
                       tmin real, tmax real, p real, tmin_suelo real, qc smallint, PRIMARY KEY (estacion_id, fecha))""")
        dia2 = dia.copy()
        dia2['fecha'] = dia2.fecha.dt.strftime('%Y-%m-%d')
        copy_df(cur, dia2, 'estaciones.diario')
        for c, d in (('tmin', 'temperatura mínima en abrigo (1,5–2 m), °C'),
                     ('tmax', 'temperatura máxima en abrigo (1,5–2 m), °C'),
                     ('p', 'precipitación diaria, mm (INTA: día pluviométrico de 9 a 9 h; GHCN: el de la red de origen)'),
                     ('tmin_suelo', 'temperatura mínima a la intemperie a 5 cm del suelo, °C (sólo convencionales del INTA): helada agrometeorológica'),
                     ('qc', 'máscara de control de calidad (no se borra nada): 1 tmin>tmax, 2 tmin fuera de rango, 4 tmax fuera de rango, 8 lluvia fuera de rango, 16 salto de tmin >15 °C, 32 salto de tmax >15 °C, 64 tmin_suelo fuera de rango')):
            cur.execute(f'COMMENT ON COLUMN estaciones.diario.{c} IS {q(d)}')
        log('estacion y diario')

        # completitud y píxeles
        cur.execute("""UPDATE estaciones.estacion e SET desde = s.desde, hasta = s.hasta, n_dias = s.n,
                         pct_tmin = s.ptn, pct_tmax = s.ptx, pct_p = s.pp, pct_tmin_suelo = s.pts
                       FROM (SELECT estacion_id, min(fecha) desde, max(fecha) hasta, count(*) n,
                                    100.0 * count(tmin) / (max(fecha) - min(fecha) + 1) ptn,
                                    100.0 * count(tmax) / (max(fecha) - min(fecha) + 1) ptx,
                                    100.0 * count(p) / (max(fecha) - min(fecha) + 1) pp,
                                    100.0 * count(tmin_suelo) / (max(fecha) - min(fecha) + 1) pts
                             FROM estaciones.diario GROUP BY 1) s WHERE s.estacion_id = e.estacion_id""")
        cur.execute("""UPDATE estaciones.estacion e SET provincia = coalesce(e.provincia, t.etiqueta)
                       FROM limites l JOIN territorios t USING (territorio_id)
                       WHERE ST_Contains(l.geom, e.geom)""")
        for f, paso, x0, y0 in (('era5', 0.1, -180.05, 90.05), ('chirps', 0.05000000074505806, -180.0, 60.0)):
            cur.execute(f"""UPDATE estaciones.estacion SET {f}_pixel =
                            floor(({y0} - lat) / {paso})::bigint * 100000 + floor((lng - ({x0})) / {paso})::bigint""")
        cur.execute("""UPDATE estaciones.estacion e SET z_era5_m = p.z_m FROM clima.pixel p
                       WHERE p.fuente = 'era5' AND p.pixel_id = e.era5_pixel""")
        cur.execute("COMMENT ON COLUMN estaciones.estacion.z_era5_m IS 'altitud del píxel ERA5-Land que contiene la estación "
                    "(de la presión media; NULL si el píxel no cubre ninguna celda del Hexoma), m'")

        # agregados anuales y por campaña (sólo valores sin marca de QC de esa variable)
        cur.execute("""CREATE VIEW estaciones.v_diario_valido AS
            SELECT estacion_id, fecha,
                   CASE WHEN qc & (1|2|16) = 0 THEN tmin END AS tmin,
                   CASE WHEN qc & (1|4|32) = 0 THEN tmax END AS tmax,
                   CASE WHEN qc & 8 = 0 THEN p END AS p,
                   CASE WHEN qc & 64 = 0 THEN tmin_suelo END AS tmin_suelo
            FROM estaciones.diario""")
        cur.execute("""CREATE TABLE estaciones.anual AS
            SELECT estacion_id, extract(year FROM fecha)::smallint AS anio,
                   count(tmin)::smallint AS n_tmin, count(tmax)::smallint AS n_tmax, count(tmin_suelo)::smallint AS n_tmin_suelo,
                   count(*) FILTER (WHERE tmin < 0)::smallint AS dias_tmin_lt0,
                   count(*) FILTER (WHERE tmin < -2)::smallint AS dias_tmin_lt_m2,
                   count(*) FILTER (WHERE tmin <= 3)::smallint AS dias_tmin_le3,
                   count(*) FILTER (WHERE tmin_suelo < 0)::smallint AS dias_helada_suelo,
                   min(extract(doy FROM fecha)) FILTER (WHERE tmin < 0)::smallint AS primera_doy,
                   max(extract(doy FROM fecha)) FILTER (WHERE tmin < 0)::smallint AS ultima_doy,
                   min(tmin) AS tmin_abs, min(tmin_suelo) AS tmin_suelo_abs,
                   count(*) FILTER (WHERE tmax > 35)::smallint AS dias_tmax35
            FROM estaciones.v_diario_valido GROUP BY 1, 2""")
        cur.execute('ALTER TABLE estaciones.anual ADD PRIMARY KEY (estacion_id, anio)')
        cur.execute("""CREATE TABLE estaciones.campania AS
            SELECT estacion_id, (CASE WHEN extract(month FROM fecha) >= 7 THEN extract(year FROM fecha)
                                      ELSE extract(year FROM fecha) - 1 END)::smallint AS campania,
                   count(p)::smallint AS n_p, count(tmin) FILTER (WHERE tmax IS NOT NULL)::smallint AS n_t,
                   sum(p)::real AS p_mm, count(*) FILTER (WHERE p >= 1)::smallint AS dias_lluvia,
                   sum(greatest((tmax + tmin) / 2 - 10, 0))::real AS gdd10,
                   count(*) FILTER (WHERE tmax > 35)::smallint AS dias_tmax35
            FROM estaciones.v_diario_valido GROUP BY 1, 2""")
        cur.execute('ALTER TABLE estaciones.campania ADD PRIMARY KEY (estacion_id, campania)')
        cur.execute("""ALTER TABLE estaciones.anual ADD COLUMN n_dias_anio smallint, ADD COLUMN completo_t boolean,
                           ADD COLUMN completo_suelo boolean;
                       UPDATE estaciones.anual SET n_dias_anio = make_date(anio, 12, 31) - make_date(anio, 1, 1) + 1;
                       UPDATE estaciones.anual SET completo_t = n_tmin >= 0.95 * n_dias_anio,
                                                   completo_suelo = n_tmin_suelo >= 0.95 * n_dias_anio;
                       ALTER TABLE estaciones.campania ADD COLUMN completo_p boolean, ADD COLUMN completo_t boolean;
                       UPDATE estaciones.campania SET completo_p = n_p >= 0.95 * 365, completo_t = n_t >= 0.95 * 365""")
        cur.execute("COMMENT ON TABLE estaciones.anual IS 'Por año calendario. Los conteos sólo valen donde completo_t "
                    "(o completo_suelo para la helada a 5 cm) es verdadero: ≥ 95 % de días válidos'")
        cur.execute("COMMENT ON TABLE estaciones.campania IS 'Por campaña jul–jun (año de inicio). Lluvia válida donde "
                    "completo_p; grados-día y días > 35 °C donde completo_t'")

        # normales 1991–2020
        cur.execute("""CREATE TABLE estaciones.normal_1991_2020 AS
            WITH a AS (
              SELECT estacion_id, x.variable, x.valor FROM estaciones.anual t,
                LATERAL (VALUES ('dias_tmin_lt0', dias_tmin_lt0::float8), ('dias_tmin_lt_m2', dias_tmin_lt_m2::float8),
                                ('dias_tmin_le3', dias_tmin_le3::float8), ('tmin_abs', tmin_abs::float8),
                                ('dias_tmax35', dias_tmax35::float8)) x(variable, valor)
              WHERE anio BETWEEN 1991 AND 2020 AND completo_t
              UNION ALL
              SELECT estacion_id, 'dias_helada_suelo', dias_helada_suelo::float8 FROM estaciones.anual
              WHERE anio BETWEEN 1991 AND 2020 AND completo_suelo
              UNION ALL
              SELECT estacion_id, x.variable, x.valor FROM estaciones.campania t,
                LATERAL (VALUES ('p_mm', CASE WHEN completo_p THEN p_mm END::float8),
                                ('dias_lluvia', CASE WHEN completo_p THEN dias_lluvia END::float8),
                                ('gdd10', CASE WHEN completo_t THEN gdd10 END::float8)) x(variable, valor)
              WHERE campania BETWEEN 1991 AND 2020 AND x.valor IS NOT NULL)
            SELECT estacion_id, variable, count(*)::smallint AS n, avg(valor) AS media,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY valor) AS mediana,
                   percentile_cont(0.1) WITHIN GROUP (ORDER BY valor) AS p10,
                   percentile_cont(0.9) WITHIN GROUP (ORDER BY valor) AS p90
            FROM a GROUP BY 1, 2 HAVING count(*) >= 20""")
        cur.execute("COMMENT ON TABLE estaciones.normal_1991_2020 IS 'Normal 1991–2020 observada: sólo con ≥ 20 "
                    "períodos completos (≥ 95 % de días válidos) en esos 30'")
        log('anual, campaña y normales')

        # estación más cercana a cada celda
        cur.execute("""CREATE TEMP TABLE est_t AS SELECT estacion_id, geom, elev_m, tipo FROM estaciones.estacion
                       WHERE pct_tmin >= 50 AND n_dias >= 3 * 365""")
        cur.execute('CREATE INDEX ON est_t USING gist (geom)')
        cur.execute("""CREATE TABLE estaciones.h3_cercana AS
            SELECT h.h3, c.estacion_id, round(c.d / 1000)::real AS dist_km, (c.elev_m - s.elev_media_m)::real AS dif_elev_m,
                   v.estacion_id AS conv_id, round(v.d / 1000)::real AS conv_dist_km
            FROM (SELECT DISTINCT h3 FROM hexagonos) h
            LEFT JOIN suelo.h3 s USING (h3)
            CROSS JOIN LATERAL (SELECT e.estacion_id, e.elev_m,
                                       ST_Distance(e.geom::geography, h3_cell_to_geometry(h.h3)::geography) d
                                FROM est_t e ORDER BY e.geom <-> h3_cell_to_geometry(h.h3) LIMIT 1) c
            CROSS JOIN LATERAL (SELECT e.estacion_id,
                                       ST_Distance(e.geom::geography, h3_cell_to_geometry(h.h3)::geography) d
                                FROM est_t e WHERE e.tipo LIKE 'convencional%%' OR e.tipo LIKE 'sinóptica%%'
                                ORDER BY e.geom <-> h3_cell_to_geometry(h.h3) LIMIT 1) v""")
        cur.execute('ALTER TABLE estaciones.h3_cercana ADD PRIMARY KEY (h3)')
        cur.execute("COMMENT ON TABLE estaciones.h3_cercana IS 'Por celda: la estación con temperatura más cercana "
                    "(≥ 3 años, ≥ 50 % de mínimas) y la de serie larga (convencional INTA o sinóptica) más cercana. "
                    "dif_elev_m = estación − media FABDEM de la celda'")
        cur.execute('ANALYZE')
        log('estación más cercana por celda')
    verificar(conn)
    log(f'listo en {time.time() - t0:.0f} s')


def verificar(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""SELECT red, count(*), sum(n_dias), min(desde), max(hasta) FROM estaciones.estacion GROUP BY 1""")
        for r in cur.fetchall():
            print('  ', r)
        cur.execute('SELECT count(*), count(*) FILTER (WHERE qc > 0) FROM estaciones.diario')
        print('  días, con marca de QC:', cur.fetchone())
        cur.execute("""SELECT e.nombre, n.media, n.n FROM estaciones.normal_1991_2020 n JOIN estaciones.estacion e USING (estacion_id)
                       WHERE variable IN ('dias_helada_suelo') ORDER BY 2 DESC""")
        print('  heladas a 5 cm, normal 1991–2020:', [(a, round(b, 1), c) for a, b, c in cur.fetchall()])
        cur.execute("""SELECT count(*), round(avg(dist_km)::numeric, 1), round(max(dist_km)::numeric, 0),
                              round(avg(conv_dist_km)::numeric, 1) FROM estaciones.h3_cercana""")
        print('  celdas con estación cercana, distancia media y máxima (km), a la convencional/sinóptica (km):', cur.fetchone())


if __name__ == '__main__':
    main()
