"""Integridad de los datos de la fase 5 (arroyos) → pipeline/infra/validacion_datos_f5.md. Sin análisis.

Uso (desde neahub/, después de hidro_ign.py, red_arroyos.py, hand.py y lluvia_imerg.py --cargar):
    python pipeline/infra/validar_datos_f5.py
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
from build_hexoma import DB, connect  # noqa: E402
from lluvia_imerg import PASO, grilla  # noqa: E402
from validar_cruces import tabla  # noqa: E402

MD = ROOT / 'pipeline' / 'infra' / 'validacion_datos_f5.md'


def main() -> int:
    conn = connect(DB)
    q = lambda s, **kw: pd.read_sql(s, conn, **kw)  # noqa: E731
    L = ['# Integridad de los datos de la fase 5 (arroyos de Misiones)', '',
         f'Generado por `pipeline/infra/validar_datos_f5.py` el {dt.date.today().isoformat()}. Sólo datos.', '']

    L += ['## 1. Hidrografía del IGN', '', tabla(q("""
        SELECT tabla, n, nota FROM infra.fuentes WHERE tabla LIKE 'infra.ign_%' ORDER BY tabla""")), '']

    L += ['## 2. Red de arroyos derivada (FABDEM 30 m, ≥ 0,1 km²)', '', tabla(q("""
        SELECT orden::text, count(*)::int tramos, round(sum(largo_m) / 1000)::int km,
               round(percentile_cont(.5) WITHIN GROUP (ORDER BY area_abajo_km2)::numeric, 1)::float area_mediana_km2,
               round(percentile_cont(.5) WITHIN GROUP (ORDER BY pendiente_pct)::numeric, 2)::float pendiente_mediana_pct,
               count(*) FILTER (WHERE nombre IS NOT NULL)::int con_nombre
        FROM infra.arroyos GROUP BY orden ORDER BY orden""")), '']
    cob = q("""WITH p AS (
                 SELECT 'IGN perennes' fuente, (ST_DumpPoints(ST_Segmentize(geom::geography, 200)::geometry)).geom g
                 FROM infra.ign_cursos_perennes
                 UNION ALL SELECT 'OSM arroyos (stream)', (ST_DumpPoints(ST_Segmentize(geom::geography, 200)::geometry)).geom
                 FROM infra.osm_cauces WHERE waterway = 'stream')
               SELECT fuente, count(*)::int puntos,
                      round(100.0 * count(*) FILTER (WHERE EXISTS (SELECT 1 FROM infra.arroyos a
                            WHERE ST_DWithin(a.geom, p.g, 0.0007) AND ST_DWithin(a.geom::geography, p.g::geography, 60))) / count(*), 1)::float
                        AS "% a ≤ 60 m de un tramo",
                      round(100.0 * count(*) FILTER (WHERE EXISTS (SELECT 1 FROM infra.arroyos a
                            WHERE ST_DWithin(a.geom, p.g, 0.0017) AND ST_DWithin(a.geom::geography, p.g::geography, 150))) / count(*), 1)::float
                        AS "% a ≤ 150 m"
               FROM p WHERE EXISTS (SELECT 1 FROM limites l WHERE l.territorio_id = 1 AND ST_Intersects(l.geom, p.g))
               GROUP BY 1""")
    sob = q("""SELECT v.nombre, a.arroyo_id, a.orden, round(a.area_abajo_km2::numeric, 1)::float area_km2, a.nombre AS nombre_tramo
               FROM infra.visor_puentes_rp2 v CROSS JOIN LATERAL (SELECT * FROM infra.arroyos a
                    WHERE ST_DWithin(a.geom, v.geom, 0.003) ORDER BY a.area_abajo_km2 DESC LIMIT 1) a
               WHERE v.nombre IN ('Puente Arroyo Soberbio', 'Puente sobre el Arroyo Acaraguá')""")
    L += ['Cauces mapeados (puntos cada 200 m, dentro de Misiones) que caen cerca de un tramo derivado. El cauce de '
          'FABDEM queda corrido ~100 m del que dibujan OSM e IGN (fase 1), por eso las dos distancias:', '', tabla(cob), '',
          'Control: tramo de mayor área a ≤ 300 m de dos puentes RP2 (fase 1: Soberbio ~1.074 km², Acaraguá ~775 km²):', '',
          tabla(sob), '',
          tabla(q("""SELECT count(*) FILTER (WHERE arroyo_id IS NOT NULL)::int cruces_con_tramo, count(*)::int cruces
                     FROM infra.cruces""")), '']

    L += ['## 3. Altura sobre el cauce (HAND)', '', tabla(q("""
        SELECT (SELECT count(*) FROM hexagonos WHERE territorio_id = 1)::int celdas_misiones, count(*)::int celdas_con_hand,
               round(avg(n))::int pixeles_30m_por_celda,
               round(100.0 * count(*) FILTER (WHERE pct_lt_2m > 0) / count(*), 1)::float "% celdas con algo < 2 m",
               round(100.0 * count(*) FILTER (WHERE pct_lt_5m >= 50) / count(*), 1)::float "% celdas con la mitad < 5 m"
        FROM infra.hand_h3""")), '',
          tabla(q("SELECT nota FROM infra.fuentes WHERE tabla = 'infra.hand_h3'")), '',
          'Por municipio (los 10 con mayor proporción de celdas con algo bajo 2 m):', '', tabla(q("""
        SELECT m.nombre municipio, count(*)::int celdas,
               round(100.0 * count(*) FILTER (WHERE h.pct_lt_2m > 0) / count(*), 1)::float "% con algo < 2 m",
               round(percentile_cont(.5) WITHIN GROUP (ORDER BY h.hand_mediana)::numeric, 1)::float hand_mediana_tipica_m
        FROM infra.hand_h3 h JOIN infra.visor_municipios m
             ON ST_Intersects(m.geom, ST_SetSRID(h3_cell_to_lat_lng(h.h3)::geometry, 4326))
        GROUP BY 1 ORDER BY 3 DESC LIMIT 10""")), '']

    # IMERG
    d = q("SELECT min(fecha) desde, max(fecha) hasta, count(DISTINCT fecha)::int dias FROM infra.imerg_diaria").iloc[0]
    esperado = (pd.Timestamp(d.hasta) - pd.Timestamp(d.desde)).days + 1
    g = grilla()
    est = q("""SELECT e.estacion_id, e.red, e.nombre, e.lat, e.lng FROM estaciones.estacion e
               WHERE e.lng BETWEEN %s AND %s AND e.lat BETWEEN %s AND %s""",
            params=(g['x0'], g['x0'] + g['ancho'] * PASO, g['y0'] - g['alto'] * PASO, g['y0']))
    filas = []
    for e in est.itertuples():
        f = int((g['y0'] - e.lat) / PASO)
        c = int((e.lng - g['x0']) / PASO)
        pid = f * g['ancho'] + c
        m = q("""SELECT v.fecha, v.p AS est, i.mm_dia AS imerg FROM estaciones.v_diario_valido v
                 JOIN infra.imerg_diaria i ON i.pixel_id = %s AND i.fecha = v.fecha
                 WHERE v.estacion_id = %s AND v.p IS NOT NULL AND i.mm_dia IS NOT NULL""",
              params=(int(pid), e.estacion_id), parse_dates=['fecha'])
        if len(m) < 365:
            continue
        m = m.set_index('fecha').sort_index()
        r1 = m.est.corr(m.imerg)
        r1b = m.est.corr(m.imerg.shift(1))     # día de la estación 9–9 h: la lluvia de la tarde cae «ayer» en IMERG
        m3 = m.rolling('3D').sum()
        mm = m.resample('MS').sum()
        filas.append((e.red, e.nombre, len(m), round(r1, 2), round(r1b, 2), round(m3.est.corr(m3.imerg), 2),
                      round(mm.est.corr(mm.imerg), 2), round(m.imerg.sum() / m.est.sum(), 2)))
    tab = pd.DataFrame(filas, columns=['red', 'estación', 'días', 'r día', 'r día (IMERG −1)', 'r 3 días', 'r mes',
                                       'IMERG / estación']).sort_values('r mes', ascending=False)
    L += ['## 4. Lluvia IMERG V07', '',
          f'{d.dias:,} días entre {d.desde} y {d.hasta} (esperados {esperado:,}; faltan {esperado - d.dias:,}). '
          f'Grilla {g["ancho"]}×{g["alto"]} píxeles de 0,1°.', '',
          'Contra las estaciones del Hexoma (INTA SIGA y GHCN) en su píxel, días con dato en ambas. Describe la fuente: '
          'no hay regla de corte. El día de las estaciones suele ir de 9 a 9 h y el de IMERG de 0 a 24 h (local), por '
          'eso también el desfase de un día y los acumulados.', '', tabla(tab), '',
          f'Mediana: r día {tab["r día"].median():.2f}, r 3 días {tab["r 3 días"].median():.2f}, r mes '
          f'{tab["r mes"].median():.2f}; IMERG/estación {tab["IMERG / estación"].median():.2f}.', '',
          'Dato puntual: El Soberbio, 21-jul-2026 — IMERG 75,6 mm en el día (máx. 25 mm/h en media hora); el municipio '
          'informó 190 mm y CHIRPS 99 mm (demo El Soberbio). La lluvia satelital sigue subestimando los aguaceros '
          'locales.', '']
    MD.write_text('\n'.join(L), encoding='utf-8')
    print('\n'.join(L))
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
