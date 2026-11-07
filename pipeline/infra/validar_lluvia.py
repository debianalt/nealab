"""Validación del día de lluvia → pipeline/infra/validacion_lluvia.md y mapa HTML local.

Reglas fijadas ANTES de mirar (plan del 7-nov-2026):
  - minutos al hospital en seco contra la capa de accesibilidad de la web (fricción Oxford/MAP; otra fuente y otro
    método, no una verdad de campo): Spearman ρ ≥ 0,7 por celda, o revisar velocidades antes de usar los tiempos;
  - monotonía: seco ≤ lluvia ≤ lluvia_fuerte en toda celda con ruta (si falla, hay un error de código);
  - la población de cada escenario × cota suma lo mismo que el censo asignado.

Uso (desde neahub/, después de red_lluvia.py y desvios.py):
    python pipeline/infra/validar_lluvia.py
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'infra'))
from build_hexoma import DB, connect  # noqa: E402
from red_lluvia import PEND_PELIGRO, PAV, RIPIO, TIERRA, LLUVIA_PAV  # noqa: E402
from validar_cruces import MAPAS, tabla  # noqa: E402

MD = ROOT / 'pipeline' / 'infra' / 'validacion_lluvia.md'
RHO_MIN = 0.7
MUNICIPIO_MAPA = 'El Soberbio'


def mapa(conn) -> Path:
    import folium
    import json
    d = pd.read_sql("""
        SELECT a.h3::text h3, s.min_hospital seco, a.min_hospital fuerte, a.sin_ruta, a.personas,
               s.km_tierra_hospital km_tierra, ST_AsGeoJSON(h3_cell_to_boundary_geometry(a.h3)) g
        FROM infra.acceso_lluvia a
        JOIN infra.acceso_lluvia s ON s.h3 = a.h3 AND s.cota = a.cota AND s.escenario = 'seco'
        JOIN infra.visor_municipios m ON m.nombre ILIKE %s
             AND ST_Intersects(m.geom, ST_SetSRID(h3_cell_to_lat_lng(a.h3)::geometry, 4326))
        WHERE a.escenario = 'lluvia_fuerte' AND a.cota = 'pesimista'""", conn, params=(f'%{MUNICIPIO_MAPA}',))
    cr = pd.read_sql("""
        SELECT c.id, c.cauce_nombre, c.estructura, c.dependientes_hospital, c.desvio_min_lluvia,
               c.personas_aisladas_lluvia_fuerte, ST_X(c.geom) lon, ST_Y(c.geom) lat
        FROM infra.cruces c WHERE c.municipio ILIKE %s
          AND (c.impacto_min_persona > 0 OR c.personas_aisladas_lluvia_fuerte >= 20)""", conn,
                     params=(f'%{MUNICIPIO_MAPA}',))
    lat = cr.lat.mean() if len(cr) else -27.3
    lon = cr.lon.mean() if len(cr) else -54.2
    m = folium.Map(location=[lat, lon], zoom_start=11, tiles='OpenStreetMap')

    def color(r):
        if r.sin_ruta:
            return '#7d1a1a'
        x = r.fuerte
        return '#2da44e' if x < 15 else '#bf8700' if x < 30 else '#e16f24' if x < 60 else '#cf222e'
    for capa, campo in [('Seco', 'seco'), ('Lluvia fuerte (pesimista)', 'fuerte')]:
        fg = folium.FeatureGroup(name=capa, show=campo == 'fuerte')
        for r in d.itertuples():
            val = getattr(r, campo)
            col = color(r) if campo == 'fuerte' else ('#2da44e' if val < 15 else '#bf8700' if val < 30
                                                      else '#e16f24' if val < 60 else '#cf222e')
            txt = (f'{r.personas:,.0f} personas · seco {r.seco:.0f} min · lluvia fuerte '
                   + ('SIN RUTA' if r.sin_ruta else f'{r.fuerte:.0f} min') + f' · {r.km_tierra:.1f} km de tierra')
            folium.GeoJson(json.loads(r.g), style_function=lambda _, c=col: {'fillColor': c, 'color': c, 'weight': 0.3,
                                                                             'fillOpacity': 0.6},
                           tooltip=txt).add_to(fg)
        fg.add_to(m)
    fc = folium.FeatureGroup(name='Cruces que pesan')
    for r in cr.itertuples():
        txt = (f'#{r.id} {r.estructura} · {r.cauce_nombre or "cauce sin nombre"}<br>'
               f'dependen {r.dependientes_hospital or 0:,.0f} personas; desvío con lluvia '
               + (f'{r.desvio_min_lluvia:.0f} min' if pd.notna(r.desvio_min_lluvia) else '—')
               + (f'<br>con lluvia fuerte aísla {r.personas_aisladas_lluvia_fuerte:,.0f}'
                  if pd.notna(r.personas_aisladas_lluvia_fuerte) else ''))
        folium.CircleMarker([r.lat, r.lon], radius=4, color='#000', fill=True, fill_color='#1f6feb', fill_opacity=.9,
                            popup=folium.Popup(txt, max_width=280)).add_to(fc)
    fc.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)
    MAPAS.mkdir(parents=True, exist_ok=True)
    p = MAPAS / f'lluvia_{MUNICIPIO_MAPA.lower().replace(" ", "_")}.html'
    m.save(str(p))
    return p


def main() -> int:
    conn = connect(DB)
    q = lambda s, **kw: pd.read_sql(s, conn, **kw)  # noqa: E731
    L = ['# Validación del día de lluvia en Misiones (`infra.acceso_lluvia`, columnas de lluvia de `infra.cruces`)', '',
         f'Generado por `pipeline/infra/validar_lluvia.py` el {dt.date.today().isoformat()}. Método en los docstrings '
         'de `red_lluvia.py` y `desvios.py`.', '',
         '## Supuestos', '',
         f'Velocidades en km/h (seco / lluvia), supuestas: pavimento {PAV} (con lluvia × {LLUVIA_PAV}); ripio {RIPIO}; '
         f'tierra {TIERRA}. Tierra peligrosa con lluvia fuerte = pendiente ≥ {PEND_PELIGRO:.0f} %. Cota optimista: '
         'superficie desconocida = pavimento en calles urbanas y rutas, ripio en el resto; pesimista: = tierra. '
         'Los km de tierra del camino no dependen de las velocidades.', '']

    # 1. superficie OSM contra IGN
    s = q("""WITH p AS (
              SELECT i.superficie si, (ST_DumpPoints(ST_Segmentize(i.geom::geography, 100)::geometry)).geom g
              FROM vial.ign i WHERE i.provincia = 'misiones' AND i.superficie IN ('pavimento', 'tierra'))
            SELECT p.si AS ign, c.superficie AS osm, c.etiq, count(*)::int AS puntos
            FROM p CROSS JOIN LATERAL (SELECT k.superficie, k.superficie_etiquetada etiq FROM vial.caminos k
                 WHERE k.provincia = 'misiones' AND ST_DWithin(k.geom, p.g, 0.0002)
                 ORDER BY k.geom <-> p.g LIMIT 1) c
            GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""")
    s['pav_ign'] = s.ign == 'pavimento'
    s['pav_osm'] = s.osm == 'pavimento'
    conoc = s[s.osm != 'desconocida']
    acuerdo = (conoc.puntos * (conoc.pav_ign == conoc.pav_osm)).sum() / conoc.puntos.sum()
    t_ign = conoc[~conoc.pav_ign]
    tierra_ok = (t_ign.puntos * ~t_ign.pav_osm).sum() / max(t_ign.puntos.sum(), 1)
    L += ['## 1. Superficie: OSM contra IGN', '',
          'Puntos cada 100 m sobre los tramos del IGN con superficie conocida en Misiones, contra el camino de '
          '`vial.caminos` más cercano a ≤ ~20 m. Pavimento contra no pavimento.', '',
          tabla(s[['ign', 'osm', 'etiq', 'puntos']].rename(columns={'etiq': 'etiquetada_en_osm'})), '',
          f'Acuerdo pavimento / no pavimento (sin las desconocidas): **{acuerdo:.1%}**. De lo que el IGN dice tierra, '
          f'OSM dice no pavimento en el {tierra_ok:.1%}. El IGN cubre sobre todo rutas provinciales y terciarias: no '
          'valida las huellas, donde la tierra es por regla.', '']

    # 2. población y monotonía
    pob = q("""SELECT escenario, cota, round(sum(personas))::int personas FROM infra.acceso_lluvia GROUP BY 1, 2 ORDER BY 2, 1""")
    mono = q("""SELECT a.cota,
                  count(*) FILTER (WHERE b.min_hospital < a.min_hospital - 1e-3)::int AS lluvia_menor_que_seco,
                  count(*) FILTER (WHERE c.min_hospital < b.min_hospital - 1e-3)::int AS fuerte_menor_que_lluvia
                FROM infra.acceso_lluvia a
                JOIN infra.acceso_lluvia b ON b.h3 = a.h3 AND b.cota = a.cota AND b.escenario = 'lluvia'
                JOIN infra.acceso_lluvia c ON c.h3 = a.h3 AND c.cota = a.cota AND c.escenario = 'lluvia_fuerte'
                WHERE a.escenario = 'seco' AND NOT c.sin_ruta GROUP BY 1""")
    L += ['## 2. Población y monotonía', '', tabla(pob), '', tabla(mono), '']

    # 3. contra la capa de accesibilidad
    ac = q("""SELECT a.cota, a.min_hospital AS nuestro, w.travel_min_hospital_raw AS web, a.personas
              FROM infra.acceso_lluvia a JOIN capa_accessibility w ON w.h3 = a.h3 AND w.principal
              WHERE a.escenario = 'seco' AND NOT a.sin_ruta AND w.travel_min_hospital_raw IS NOT NULL""")
    filas, pasa = [], True
    for cota, d in ac.groupby('cota'):
        rho = d.nuestro.rank().corr(d.web.rank())
        pasa &= rho >= RHO_MIN
        filas.append((cota, len(d), round(rho, 3), round(d.nuestro.median(), 1), round(d.web.median(), 1),
                      round((d.nuestro - d.web).median(), 1)))
    L += ['## 3. Minutos al hospital en seco contra la capa de accesibilidad de la web', '',
          f'Regla fijada antes de mirar: Spearman ρ ≥ {RHO_MIN} por celda. La capa de la web usa la superficie de '
          'fricción motorizada de Oxford/MAP 2019 (1 km) y la capa de salud del IGN; no es verdad de campo.', '',
          tabla(pd.DataFrame(filas, columns=['cota', 'celdas', 'rho', 'mediana nuestra', 'mediana web', 'dif. mediana'])),
          '', f'**{"PASA" if pasa else "NO PASA"}.**', '']

    # 4. pendiente
    L += ['## 4. Pendiente por clase de camino', '', tabla(q(f"""
        SELECT a.clase, round(sum(a.largo_m)/1000)::int km,
               round(percentile_cont(.5) WITHIN GROUP (ORDER BY t.pendiente_pct)::numeric, 1)::float p50,
               round(percentile_cont(.9) WITHIN GROUP (ORDER BY t.pendiente_pct)::numeric, 1)::float p90,
               round((100 * sum(a.largo_m) FILTER (WHERE t.pendiente_pct >= {PEND_PELIGRO}) / sum(a.largo_m))::numeric, 1)::float
                 AS "% km ≥ umbral",
               round((sum(a.largo_m) FILTER (WHERE t.pendiente_pct >= {PEND_PELIGRO}
                      AND t.superficie_pesimista = 'tierra') / 1000)::numeric)::int AS "km tierra peligrosa"
        FROM infra.aristas_atributos t JOIN vial.aristas a ON a.id = t.arista_id
        JOIN vial.caminos k ON k.id = a.camino_id AND k.provincia = 'misiones'
        GROUP BY 1 ORDER BY 2 DESC""")), '',
          'La pendiente sale de FABDEM 30 m sobre 150 m de camino: en quebradas estrechas puede sobrestimarse (el '
          'camino va por la ladera y el modelo de elevación mezcla el fondo).', '']

    # 5. resumen por escenario
    L += ['## 5. Resumen por escenario', '', tabla(q("""
        WITH t AS (SELECT *, sum(personas) OVER (PARTITION BY escenario, cota ORDER BY min_hospital) acum,
                          sum(personas) FILTER (WHERE NOT sin_ruta) OVER (PARTITION BY escenario, cota) tot
                   FROM infra.acceso_lluvia)
        SELECT cota, escenario,
               min(min_hospital) FILTER (WHERE acum >= tot / 2) AS "mediana min",
               round(sum(personas) FILTER (WHERE min_hospital > 30))::int AS "> 30 min",
               round(sum(personas) FILTER (WHERE min_hospital > 60))::int AS "> 60 min",
               round(sum(personas) FILTER (WHERE sin_ruta))::int AS "sin ruta",
               round(sum(personas) FILTER (WHERE km_tierra_hospital > 5))::int AS "> 5 km de tierra"
        FROM t GROUP BY 1, 2 ORDER BY 1, array_position(ARRAY['seco','lluvia','lluvia_fuerte','todo_tiempo'], escenario)""")),
          '', '«todo_tiempo» exige que TODO el camino sea pavimento o ripio, incluida la cuadra de tierra frente a la '
          'casa: es un extremo que sobrecuenta. La medida útil ahí es «> 5 km de tierra».', '']

    # 5b. sensibilidad al umbral de pendiente
    sens = q('SELECT cota, umbral_pct, round(sin_ruta)::int sin_ruta, round(mas_60_min)::int mas_60_min '
             'FROM infra.sensibilidad_pendiente ORDER BY cota, umbral_pct')
    L += ['## 5b. Sensibilidad al umbral de pendiente (lluvia fuerte)', '',
          f'El umbral de tierra intransitable ({PEND_PELIGRO:.0f} %) es un supuesto, y decide cuánta gente queda sin ruta. '
          'La CANTIDAD no es robusta: se informa como rango. Lo que se usa de este escenario es DÓNDE pasa.', '',
          tabla(sens), '']
    mun = q('''SELECT m.nombre municipio, s.cota, s.umbral_pct, sum(c.p) sin_ruta
               FROM infra.sensibilidad_sin_ruta s
               JOIN (SELECT h3, sum(est_personas) p FROM censo_2022.cruce WHERE territorio_id = 1 GROUP BY h3) c USING (h3)
               JOIN infra.visor_municipios m ON ST_Intersects(m.geom, ST_SetSRID(h3_cell_to_lat_lng(s.h3)::geometry, 4326))
               GROUP BY 1, 2, 3''')
    piv = mun[mun.cota == 'pesimista'].pivot_table(index='municipio', columns='umbral_pct', values='sin_ruta', fill_value=0)
    ref = piv[PEND_PELIGRO]
    top_ref = set(ref.nlargest(10).index)
    filas = [(u_, round(piv[u_].rank().corr(ref.rank()), 3), len(top_ref & set(piv[u_].nlargest(10).index)))
             for u_ in piv.columns]
    L += ['¿Se mantiene el orden de los municipios? Spearman entre la gente sin ruta por municipio con cada umbral y con '
          f'el de {PEND_PELIGRO:.0f} % (cota pesimista), y cuántos de los 10 primeros coinciden:', '',
          tabla(pd.DataFrame(filas, columns=['umbral', 'rho con 10 %', 'top 10 en común'])), '']

    # 6. municipios
    L += ['## 6. Municipios con más gente sin ruta con lluvia fuerte (cota pesimista)', '',
          'Los minutos medios se calculan sobre quienes TIENEN ruta: con lluvia fuerte bajan en algunos municipios '
          'porque los más lejanos pasan a «sin ruta» y salen del promedio, no porque mejore el acceso.', '', tabla(q("""
        SELECT f.municipio, f.personas::int, f.sin_ruta::int AS sin_ruta_fuerte,
               s.min_hospital_medio AS min_seco, l.min_hospital_medio AS min_lluvia, f.min_hospital_medio AS min_fuerte,
               s.km_tierra_medio AS km_tierra_seco
        FROM infra.v_acceso_municipio f
        JOIN infra.v_acceso_municipio s ON s.municipio = f.municipio AND s.cota = f.cota AND s.escenario = 'seco'
        JOIN infra.v_acceso_municipio l ON l.municipio = f.municipio AND l.cota = f.cota AND l.escenario = 'lluvia'
        WHERE f.escenario = 'lluvia_fuerte' AND f.cota = 'pesimista'
        ORDER BY f.sin_ruta DESC NULLS LAST LIMIT 20""")), '']

    # 7. cruces
    L += ['## 7. Cruces que más pesan', '', 'Por impacto = personas que dependen del cruce para llegar al hospital × '
          'minutos de desvío con lluvia (cota pesimista):', '', tabla(q("""
        SELECT id, municipio, coalesce(cauce_nombre, '—') cauce, estructura, clase_camino, superficie,
               round(dependientes_hospital)::int dependen, round(desvio_min_seco::numeric, 1)::float desvio_seco,
               round(desvio_min_lluvia::numeric, 1)::float desvio_lluvia,
               round(impacto_min_persona / 1000)::int AS "impacto (miles min·persona)"
        FROM infra.cruces WHERE impacto_min_persona IS NOT NULL
        ORDER BY impacto_min_persona DESC LIMIT 20""")), '',
          'Por aislamiento con lluvia fuerte (sin tierra en pendiente; con todo seco ninguno pasaba de 834). «Aislado» '
          'es separado de Posadas: si el lado aislado tiene hospital o CAPS propio (salud_del_lado_aislado > 0), la '
          'gente no queda sin atención, queda sin conexión con el resto de la provincia.', '',
          tabla(q("""
        SELECT id, municipio, coalesce(cauce_nombre, '—') cauce, estructura, clase_camino, superficie,
               round(personas_aisladas)::int AS aisla_seco, round(personas_aisladas_lluvia_fuerte)::int AS aisla_fuerte,
               salud_aislada_lluvia_fuerte::int AS salud_del_lado_aislado,
               intransitable_lluvia_fuerte AS el_cruce_es_pendiente
        FROM infra.cruces WHERE personas_aisladas_lluvia_fuerte IS NOT NULL
        ORDER BY personas_aisladas_lluvia_fuerte DESC LIMIT 20""")), '']

    p = mapa(conn)
    L += ['## Mapa', '', f'Local, fuera del repo: `{p.relative_to(ROOT).as_posix()}`. Celdas coloreadas por minutos al '
          'hospital (verde < 15, ocre < 30, naranja < 60, rojo ≥ 60, bordó = sin ruta); capa «Seco» y «Lluvia fuerte»; '
          'puntos = cruces con dependientes o que aíslan ≥ 20 personas con lluvia fuerte.', '']
    MD.write_text('\n'.join(L), encoding='utf-8')
    print('\n'.join(L))
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
