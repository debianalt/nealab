"""Integridad de los datos de la fase 4 → pipeline/infra/validacion_datos_f4.md. Sin análisis: sólo conteos, cobertura,
coincidencia con la fuente y contra otra fuente cuando la hay.

Uso (desde neahub/, después de importar_visor.py, servicios.py, rios_parana_iguazu.py, conectividad.py y luz.py):
    python pipeline/infra/validar_datos_f4.py
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
from luz import COL_DIARIA, DIR, PASO, grilla  # noqa: E402
from validar_cruces import tabla  # noqa: E402

MD = ROOT / 'pipeline' / 'infra' / 'validacion_datos_f4.md'
CONTROL = [('Posadas', -55.8961, -27.3671), ('Oberá', -55.1203, -27.4850), ('Eldorado', -54.6262, -26.4049)]


def main() -> int:
    conn = connect(DB)
    q = lambda s, **kw: pd.read_sql(s, conn, **kw)  # noqa: E731
    L = ['# Integridad de los datos de la fase 4 (luz, servicios, conectividad, ríos)', '',
         f'Generado por `pipeline/infra/validar_datos_f4.py` el {dt.date.today().isoformat()}. Sólo datos: ningún '
         'escenario ni modelo.', '']

    L += ['## 1. Inventario (`infra.fuentes`)', '', tabla(q(
        "SELECT tabla, fuente, fecha_fuente, n FROM infra.fuentes ORDER BY tabla")), '']

    esc = q("""SELECT CASE left(cue, 2) WHEN '54' THEN 'Misiones' WHEN '18' THEN 'Corrientes (borde)' ELSE left(cue, 2) END
                      AS jurisdiccion, props->>'ges' AS gestion, count(*)::int n
               FROM infra.escuelas GROUP BY 1, 2 ORDER BY 1, 3 DESC""")
    sal = q("""SELECT count(*)::int salud_ign,
                      count(*) FILTER (WHERE EXISTS (SELECT 1 FROM infra.visor_hospitales h WHERE ST_DWithin(h.geom::geography, s.geom::geography, 150)))::int con_hospital_visor_150m,
                      count(*) FILTER (WHERE EXISTS (SELECT 1 FROM infra.visor_caps c WHERE ST_DWithin(c.geom::geography, s.geom::geography, 150)))::int con_caps_visor_150m
               FROM infra.salud_ign s""")
    L += ['## 2. Servicios', '', 'Escuelas (Mapa Educativo) por jurisdicción del CUE y gestión. El recorte de 5 km suma '
          'escuelas de Corrientes junto al límite.', '', tabla(esc), '',
          'Salud del IGN contra hospitales y CAPS del visor (a ≤ 150 m):', '', tabla(sal), '',
          'Asentamientos BAHRA por categoría:', '',
          tabla(q('SELECT categoria, count(*)::int n FROM infra.asentamientos GROUP BY 1 ORDER BY 2 DESC')), '',
          'Red eléctrica y telecomunicaciones de OSM, contra las del visor:', '',
          tabla(q("""SELECT 'osm_electrica ' || tipo AS capa, count(*)::int n FROM infra.osm_electrica GROUP BY 1
                     UNION ALL SELECT 'visor lineas_at', count(*) FROM infra.visor_lineas_at
                     UNION ALL SELECT 'visor transformadoras', count(*) FROM infra.visor_transformadoras
                     UNION ALL SELECT 'osm_telecom', count(*) FROM infra.osm_telecom
                     UNION ALL SELECT 'visor antenas', count(*) FROM infra.visor_antenas ORDER BY 1""")), '']

    rio = q("""SELECT e.serie, e.nombre, e.rio, e.desde, e.hasta, e.lecturas,
                      round(100.0 * e.lecturas / ((e.hasta - e.desde) + 1), 1)::float AS "% días",
                      e.max_m, e.nivel_alerta_m,
                      (SELECT count(*) FROM infra.rio_lecturas_otras l WHERE l.serie = e.serie AND l.qc <> 'ok')::int AS marcadas
               FROM infra.rio_estaciones_otras e ORDER BY e.rio, e.serie""")
    L += ['## 3. Hidrómetros del Paraná y del Iguazú (INA)', '', tabla(rio), '',
          '«% días» = lecturas válidas sobre días calendario entre la primera y la última: los huecos son de la fuente.', '']

    ook = q("""SELECT tipo, anio::text anio, count(DISTINCT trimestre)::int trimestres, count(*)::int teselas,
                      sum(tests)::bigint pruebas, round(avg(avg_d_kbps) / 1000.0, 1)::float bajada_media_mbps
               FROM infra.ookla GROUP BY 1, 2 ORDER BY 1, 2""")
    L += ['## 4. Conectividad (Ookla Open Data)', '', tabla(ook), '',
          'Una tesela sin pruebas no aparece: la ausencia no es falta de señal. ENACOM no tiene descarga directa (7-nov-2026).', '']

    # Black Marble
    import ee
    from gee_export_activity_rasters import authenticate
    authenticate()
    ids = ee.ImageCollection(COL_DIARIA).aggregate_array('system:index').getInfo()
    tengo = {p.stem[-8:] for p in DIR.glob('20*/vnp46a2_*.npz')}
    faltan = [i for i in ids if i.replace('_', '') not in tengo]
    g = grilla()
    filas = []
    for nombre, lon, lat in CONTROL:
        f = int((g['y0'] - lat) / PASO)
        c = int((lon - g['x0']) / PASO)
        px_lon, px_lat = g['x0'] + (c + 0.5) * PASO, g['y0'] - (f + 0.5) * PASO
        for i in reversed(ids[-120:]):   # último día con dato en ese píxel
            fecha = i.replace('_', '')
            z = np.load(DIR / fecha[:4] / f'vnp46a2_{fecha}.npz')
            if z['rad'][f, c] != 65535 and z['qf'][f, c] <= 1:
                break
        v = ee.Image(f'{COL_DIARIA}/{i}').select('DNB_BRDF_Corrected_NTL').reduceRegion(
            ee.Reducer.first(), ee.Geometry.Point([px_lon, px_lat]), scale=463.83).getInfo().get('DNB_BRDF_Corrected_NTL')
        filas.append((nombre, i, z['rad'][f, c] / 10, None if v is None else round(v, 2)))
    lit = q('SELECT count(*) FILTER (WHERE iluminado)::int iluminados, count(*)::int total FROM infra.luz_pixel').iloc[0]
    cob = q("""SELECT extract(year FROM fecha)::int::text anio, count(DISTINCT fecha)::int dias,
                      round(100.0 * sum(n_validos) / nullif(sum(n_pixeles), 0), 1)::float "% píxeles válidos"
               FROM infra.luz_asentamiento_diaria GROUP BY 1 ORDER BY 1""")
    cons = q("""WITH a AS (SELECT pixel_id, extract(year FROM mes)::int anio, avg(avg_rad) m FROM infra.luz_mensual
                           GROUP BY 1, 2)
                SELECT a.anio::text anio, corr(a.m, CASE a.anio WHEN 2014 THEN p.mediana_20140101 WHEN 2015 THEN p.mediana_20150101 WHEN 2016 THEN p.mediana_20160101 WHEN 2017 THEN p.mediana_20170101 WHEN 2018 THEN p.mediana_20180101 WHEN 2019 THEN p.mediana_20190101 WHEN 2020 THEN p.mediana_20200101 WHEN 2021 THEN p.mediana_20210101 WHEN 2022 THEN p.mediana_20220101 WHEN 2023 THEN p.mediana_20230101 WHEN 2024 THEN p.mediana_20240101 WHEN 2025 THEN p.mediana_20250101 END)::float
                       AS corr_mensual_vs_anual, count(*)::int pixeles
                FROM a JOIN infra.luz_pixel p USING (pixel_id) WHERE a.anio BETWEEN 2014 AND 2025 GROUP BY 1 ORDER BY 1""")
    L += ['## 5. Luces nocturnas', '',
          f'Black Marble diario (VNP46A2): {len(ids):,} días en GEE, {len(ids) - len(faltan):,} bajados '
          f'({100 * (len(ids) - len(faltan)) / len(ids):.2f} %). Faltan: {", ".join(faltan[:20]) or "ninguno"}.', '',
          'Control: valor bajado (radiancia ×10 redondeada / 10) contra GEE en el centro del mismo píxel:', '',
          tabla(pd.DataFrame(filas, columns=['lugar', 'día', 'bajado', 'GEE'])), '',
          f'Píxeles iluminados (mediana anual > 0,5 en algún año, 2013–2025): {lit.iluminados:,} de {lit.total:,}.', '',
          'Cobertura diaria por año (píxeles con calidad alta y despejados / píxeles iluminados asignados a un '
          'asentamiento): Misiones es nublada; la fracción válida es la que hay.', '', tabla(cob), '',
          'Meses del compuesto mensual cuya media provincial se aparta > 50 % de la mediana de su año (anomalías de la '
          'fuente; usar con cuidado):', '', tabla(q('''WITH m AS (SELECT mes, avg(avg_rad) r FROM infra.luz_mensual GROUP BY 1),
                 a AS (SELECT extract(year FROM mes) y, percentile_cont(.5) WITHIN GROUP (ORDER BY r) med FROM m GROUP BY 1)
              SELECT m.mes::text, round(m.r::numeric, 2)::float media, round(a.med::numeric, 2)::float mediana_del_anio
              FROM m JOIN a ON a.y = extract(year FROM m.mes) WHERE abs(m.r / a.med - 1) > 0.5 ORDER BY 1''')), '',
          'Mensual (VCMSLCFG, media del año) contra anual (V21 hasta 2021, V22 desde 2022; mediana) por píxel iluminado:', '', tabla(cons), '']
    MD.write_text('\n'.join(L), encoding='utf-8')
    print('\n'.join(L))
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
