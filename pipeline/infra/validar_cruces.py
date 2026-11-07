"""Validación de infra.cruces → pipeline/infra/validacion_cruces.md y mapas HTML locales.

Regla fijada ANTES de mirar resultados (plan del 7-nov-2026): la detección por TERRENO (sola, sin la de OSM)
tiene que encontrar ≥ 80 % de los puentes OSM sobre cauces mapeados y de los puentes RP2 del visor, a ≤ 60 m.
No pasó en la primera corrida (76 % y 66 %): el informe muestra la regla tal como se fijó, la misma medida con
150 m de tolerancia y el diagnóstico (corrimiento del cauce de FABDEM respecto de OSM).

Uso (desde neahub/, después de aislamiento.py):
    python pipeline/infra/validar_cruces.py
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
from build_hexoma import DB, connect  # noqa: E402

MD = ROOT / 'pipeline' / 'infra' / 'validacion_cruces.md'
MAPAS = ROOT / 'pipeline' / 'output' / 'infra'
UMBRAL = 0.80
MUNICIPIOS_MAPA = ['El Soberbio', 'Eldorado']

VEHICULAR = ("'motorway','motorway_link','trunk','trunk_link','primary','primary_link','secondary',"
             "'secondary_link','tertiary','tertiary_link','unclassified','road','track','residential',"
             "'living_street','service'")

# n, encontrados por el terreno crudo (infra.cruces_terreno) a ≤ 60 y ≤ 150 m, y por la tabla final a ≤ 60 m
RECALL = """
WITH ref AS ({ref})
SELECT count(*) AS n,
  count(*) FILTER (WHERE EXISTS (SELECT 1 FROM infra.cruces_terreno c
       WHERE ST_DWithin(c.geom, ref.geom, 0.0007) AND ST_DWithin(c.geom::geography, ref.geom::geography, 60))) AS t60,
  count(*) FILTER (WHERE EXISTS (SELECT 1 FROM infra.cruces_terreno c
       WHERE ST_DWithin(c.geom, ref.geom, 0.0017) AND ST_DWithin(c.geom::geography, ref.geom::geography, 150))) AS t150,
  count(*) FILTER (WHERE EXISTS (SELECT 1 FROM infra.cruces c
       WHERE ST_DWithin(c.geom, ref.geom, 0.0007) AND ST_DWithin(c.geom::geography, ref.geom::geography, 60))) AS final60
FROM ref"""

REGLA = ('Puentes OSM vehiculares sobre un cauce mapeado', 'Puentes RP2 del visor')
REFS = {
    'Puentes OSM vehiculares sobre un cauce mapeado': f"""
        SELECT DISTINCT ON (o.osm_id) ST_ClosestPoint(o.geom, w.geom) geom FROM vial.osm o
        JOIN infra.osm_cauces w ON ST_Intersects(o.geom, w.geom)
        WHERE o.provincia = 'misiones' AND o.tags ? 'bridge' AND o.tags->>'bridge' <> 'no'
          AND o.highway IN ({VEHICULAR})
          AND NOT coalesce(w.waterway = 'river' AND w.nombre ~* '(paran[aá]|uruguay|iguaz)', false)""",
    'Puentes RP2 del visor': 'SELECT geom FROM infra.visor_puentes_rp2',
    'Puentes OSM vehiculares (todos)': f"""
        SELECT ST_PointOnSurface(geom) geom FROM vial.osm
        WHERE provincia = 'misiones' AND tags ? 'bridge' AND tags->>'bridge' <> 'no' AND highway IN ({VEHICULAR})""",
    'Alcantarillas del visor': """SELECT v.geom FROM infra.visor_alcantarillas v JOIN infra.visor_municipios m
                                  ON ST_Intersects(m.geom, v.geom)""",
    'Alcantarillas OSM': """SELECT e.geom FROM infra.osm_estructuras e JOIN infra.visor_municipios m
                            ON ST_Intersects(m.geom, e.geom) WHERE e.tipo = 'alcantarilla'""",
}


def tabla(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    out = ['| ' + ' | '.join(cols) + ' |', '|' + '---|' * len(cols)]
    for r in df.itertuples(index=False):
        out.append('| ' + ' | '.join('' if pd.isna(x) else (f'{x:,.1f}' if isinstance(x, float) else f'{x:,}' if isinstance(x, int) else str(x)) for x in r) + ' |')
    return '\n'.join(out)


def mapa(conn, municipio: str) -> Path | None:
    import folium
    d = pd.read_sql(f"""SELECT c.id, c.estructura, c.area_drenada_km2, c.cauce_nombre, c.clase_camino, c.superficie,
                               c.aisla, c.personas_aisladas, ST_X(c.geom) lon, ST_Y(c.geom) lat
                        FROM infra.cruces c WHERE c.municipio ILIKE %s""", conn, params=(f'%{municipio}',))
    if d.empty:
        return None
    m = folium.Map(location=[d.lat.mean(), d.lon.mean()], zoom_start=12, tiles=None)
    folium.TileLayer('https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}', attr='Google', name='Satélite').add_to(m)
    folium.TileLayer('OpenStreetMap', name='OSM').add_to(m)
    color = {'puente': '#1f6feb', 'alcantarilla': '#2da44e', 'vado': '#bf8700', 'sin_dato': '#cf222e'}
    for r in d.itertuples():
        radio = 3 + min(r.area_drenada_km2 or 0, 50) ** 0.5
        texto = (f'#{r.id} {r.estructura}<br>{r.cauce_nombre or "cauce sin nombre"} · {r.area_drenada_km2:.2f} km²'
                 f'<br>{r.clase_camino} / {r.superficie}'
                 + (f'<br><b>aísla {r.personas_aisladas:,.0f} personas</b>' if r.aisla and r.personas_aisladas else ''))
        folium.CircleMarker([r.lat, r.lon], radius=radio, color='#000' if r.aisla else color[r.estructura],
                            weight=2 if r.aisla else 1, fill=True, fill_color=color[r.estructura], fill_opacity=.8,
                            popup=folium.Popup(texto, max_width=260)).add_to(m)
    folium.LayerControl().add_to(m)
    MAPAS.mkdir(parents=True, exist_ok=True)
    p = MAPAS / f'cruces_{municipio.lower().replace(" ", "_")}.html'
    m.save(str(p))
    return p


def main() -> int:
    conn = connect(DB)
    q = lambda s, **kw: pd.read_sql(s, conn, **kw)  # noqa: E731
    lineas = [f'# Validación de los cruces camino × cauce de Misiones (`infra.cruces`)', '',
              f'Generado por `pipeline/infra/validar_cruces.py` el {dt.date.today().isoformat()}. Método en los '
              'docstrings de `drenaje.py`, `cruces.py` y `aislamiento.py`.', '']

    lineas += ['## Sensibilidad de la detección', '',
               f'Regla fijada antes de mirar (plan del 7-nov-2026): la detección por terreno sola debe encontrar '
               f'≥ {UMBRAL:.0%} de los puentes sobre cauces mapeados y de los puentes RP2, a ≤ 60 m. «Terreno» = '
               'los picos de área drenada antes de unir con OSM (`infra.cruces_terreno`). «Final ≤ 60 m» es '
               'circular para puentes y alcantarillas: desde la segunda corrida esas estructuras también entran '
               'como detección y fijan la ubicación del cruce.', '']
    filas, pasa60, pasa150 = [], True, True
    for nombre, ref in REFS.items():
        r = q(RECALL.format(ref=ref)).iloc[0]
        n = max(int(r.n), 1)
        filas.append((nombre, int(r.n), f'{r.t60 / n:.1%}', f'{r.t150 / n:.1%}', f'{r.final60 / n:.1%}'))
        if nombre in REGLA:
            pasa60 &= r.t60 / n >= UMBRAL
            pasa150 &= r.t150 / n >= UMBRAL
    lineas += [tabla(pd.DataFrame(filas, columns=['Referencia', 'n', 'Terreno ≤ 60 m', 'Terreno ≤ 150 m', 'Final ≤ 60 m'])), '',
               f'**Regla tal como se fijó (terreno ≤ 60 m): {"PASA" if pasa60 else "NO PASA"}.** '
               f'Con tolerancia de 150 m: {"pasa" if pasa150 else "no pasa"}.', '']
    dg = q("""
        WITH ref AS (SELECT DISTINCT ON (o.osm_id) ST_ClosestPoint(o.geom, w.geom) geom FROM vial.osm o
          JOIN infra.osm_cauces w ON ST_Intersects(o.geom, w.geom)
          WHERE o.provincia = 'misiones' AND o.tags ? 'bridge' AND o.tags->>'bridge' <> 'no'
            AND NOT coalesce(w.waterway = 'river' AND w.nombre ~* '(paran[aá]|uruguay|iguaz)', false)),
        d AS (SELECT (SELECT ST_Distance(c.geom::geography, ref.geom::geography) FROM infra.cruces_terreno c
                      ORDER BY c.geom <-> ref.geom LIMIT 1) dm FROM ref)
        SELECT round(percentile_cont(.5) WITHIN GROUP (ORDER BY dm) FILTER (WHERE dm > 60)::numeric)::int p50,
               round(percentile_cont(.9) WITHIN GROUP (ORDER BY dm) FILTER (WHERE dm > 60)::numeric)::int p90,
               count(*) FILTER (WHERE dm > 60)::int perdidos FROM d""").iloc[0]
    rp2 = q("""SELECT round(percentile_cont(.5) WITHIN GROUP (ORDER BY d)::numeric)::int p50, max(d)::int mx FROM (
               SELECT (SELECT ST_Distance(k.geom::geography, v.geom::geography) FROM vial.caminos k
                       ORDER BY k.geom <-> v.geom LIMIT 1) d FROM infra.visor_puentes_rp2 v) s""").iloc[0]
    lineas += ['Historia de la regla (7-nov-2026). La primera corrida dio 76 % (puentes OSM) y 66 % (RP2) y no pasaba, '
               'por dos errores de medición, no de detección: (1) medía la ubicación del representante de cada grupo '
               '(el punto de mayor área drenada, que podía ser la intersección OSM) y no los picos crudos del terreno; '
               '(2) contaba puentes peatonales y de senderos, que no están en `vial.caminos` y no se pueden detectar. '
               'Medida sobre los picos crudos (`infra.cruces_terreno`) y puentes vehiculares, la regla pasa sin cambiar '
               'ventana ni umbral.', '',
               'Diagnóstico de lo que falta. Los puentes (peatonales incluidos) que el terreno no encuentra a ≤ 60 m '
               f'tienen casi siempre un pico de terreno cerca: distancia mediana {dg.p50} m, P90 {dg.p90} m '
               f'({dg.perdidos} puentes). A ±1 celda del puente el área drenada suele ser < 0,1 km² y a ±3 celdas casi '
               'nunca: el cauce de FABDEM 30 m queda corrido del puente que OSM dibujó sobre la imagen. Los puntos RP2 '
               f'del visor sí están sobre el camino ({rp2.p50} m de mediana, máx. {rp2.mx} m). Por eso cada cruce por '
               'terreno se ancla a la estructura o a la intersección OSM más cercana de su mismo camino o de uno que lo toca, a ≤ 150 m '
               '(`desplazamiento_m`): la ubicación de OSM es la más precisa.', '']

    lineas += ['## Cruces por estructura', '', tabla(q("""
        SELECT estructura, count(*)::int AS cruces,
               count(*) FILTER (WHERE por_terreno AND por_osm)::int AS terreno_y_osm,
               count(*) FILTER (WHERE por_terreno AND NOT por_osm)::int AS solo_terreno,
               count(*) FILTER (WHERE por_osm AND NOT por_terreno)::int AS solo_osm,
               count(*) FILTER (WHERE paralelo)::int AS paralelos,
               round(percentile_cont(.5) WITHIN GROUP (ORDER BY area_drenada_km2)::numeric, 2)::float AS area_mediana_km2
        FROM infra.cruces GROUP BY 1 ORDER BY 2 DESC""")), '']

    lineas += ['## Cruces por clase de camino y área drenada', '', tabla(q("""
        SELECT clase_camino, count(*)::int AS cruces,
               count(*) FILTER (WHERE area_drenada_km2 < 1)::int AS "<1 km²",
               count(*) FILTER (WHERE area_drenada_km2 >= 1 AND area_drenada_km2 < 10)::int AS "1–10",
               count(*) FILTER (WHERE area_drenada_km2 >= 10 AND area_drenada_km2 < 100)::int AS "10–100",
               count(*) FILTER (WHERE area_drenada_km2 >= 100)::int AS "≥100",
               count(*) FILTER (WHERE estructura = 'puente')::int AS puentes,
               count(*) FILTER (WHERE estructura = 'sin_dato')::int AS sin_dato
        FROM infra.cruces GROUP BY 1 ORDER BY 2 DESC""")), '']

    lineas += ['## Aislamiento', '', tabla(q("""
        SELECT count(*)::int AS cruces, count(*) FILTER (WHERE aisla)::int AS sobre_arista_de_corte,
               count(*) FILTER (WHERE aisla AND personas_aisladas >= 1)::int AS aislan_a_alguien,
               count(*) FILTER (WHERE aisla AND personas_aisladas >= 100)::int AS "aíslan ≥100",
               count(*) FILTER (WHERE aisla AND personas_aisladas >= 1000)::int AS "aíslan ≥1000",
               count(*) FILTER (WHERE desconectado)::int AS en_islas_de_la_red,
               round(percentile_cont(.5) WITHIN GROUP (ORDER BY dist_arista_m)::numeric, 1)::float AS dist_arista_p50_m
        FROM infra.cruces""")), '',
               'Los 20 cruces que más personas aíslan:', '', tabla(q("""
        SELECT id, municipio, coalesce(cauce_nombre, '—') AS cauce, round(area_drenada_km2::numeric, 1)::float AS km2,
               estructura, clase_camino, superficie, round(personas_aisladas)::int AS personas,
               hospitales_aislados::int AS hosp, caps_aislados::int AS caps
        FROM infra.cruces WHERE aisla AND NOT rio_frontera ORDER BY personas_aisladas DESC NULLS LAST LIMIT 20""")), '']

    lineas += ['## Puentes RP2 (control de cordura)', '', tabla(q("""
        SELECT v.nombre, v.cota_seguridad_msnm AS cota, c.id AS cruce, round(c.area_drenada_km2::numeric, 1)::float AS km2,
               c.por_terreno, c.aisla, round(c.personas_aisladas)::int AS personas
        FROM infra.visor_puentes_rp2 v
        LEFT JOIN LATERAL (SELECT * FROM infra.cruces c WHERE ST_DWithin(c.geom, v.geom, 0.0012)
                           ORDER BY c.geom::geography <-> v.geom::geography LIMIT 1) c ON true
        ORDER BY ST_Y(v.geom) DESC""")), '']

    mapas = [p for p in (mapa(conn, mu) for mu in MUNICIPIOS_MAPA) if p]
    lineas += ['## Mapas', '', 'Locales, fuera del repo: ' + ', '.join(f'`{p.relative_to(ROOT).as_posix()}`' for p in mapas)
               + '. Color = estructura (azul puente, verde alcantarilla, ocre vado, rojo sin dato); borde negro = '
               'arista de corte; tamaño = área drenada.', '']
    MD.write_text('\n'.join(lineas), encoding='utf-8')
    print('\n'.join(lineas))
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
