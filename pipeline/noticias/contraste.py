"""Prensa contra los escenarios de agua de Misiones, por municipio y día (análisis; 8-nov-2026).

Tabla noticias.contraste_municipio_dia (78 municipios × días 2012-01-01 → última lluvia IMERG):
  lluvia      IMERG V07 por día local (infra.imerg_diaria): mm_media (media de los píxeles cuyo centro cae en el municipio;
              si ninguno, el más cercano), mm_max (píxel más lluvioso), mm3 (suma de mm_media del día y los dos previos)
  río         rio_alto: alguna escala a <= 10 km del municipio en o sobre su nivel de alerta (INA: Uruguay 61, 63-66;
              Paraná e Iguazú 8-19); rp2_modelo: algún puente de la RP2 del municipio cortado según infra.rio_puente_diario
  prensa      corte_agua (corte de puente o de ruta con motivo agua), crecida (crecida o evacuados), corte_otro (corte sin
              motivo legible), en el día (noticias.v_matriz_municipio_dia)

Pruebas, con reglas fijadas ANTES de mirar:
  P1 dosis-respuesta: P(corte_agua) por clase de mm3, en días sin río alto. Regla: crece en cada clase (salvo una) y la
     clase más alta es >= 5 veces la más baja. Se repite en 2012-2019 y 2020-2026 (la prensa se cuadruplica).
  P2 río: réplica de la prueba A de probar_utilidad.py por municipio-día (RP2): P(corte_agua | rp2_modelo) / P(… | no).
     Regla: >= 5 veces.
  P3 de qué viene cada día de corte en la prensa: río alto, lluvia fuerte (mm3 >= 60 o mm_max >= 60, fijado antes),
     ambas o ninguna. Sin regla: es la descomposición.
  P4 corte transversal: por municipio, P(corte_agua | lluvia fuerte sin río alto) contra la exposición del modelo del día
     de lluvia (infra.v_acceso_municipio, lluvia_fuerte pesimista: sin_ruta / personas y km_tierra_medio). Regla:
     Spearman >= 0,3 en municipios con >= 30 días de lluvia fuerte.
  P5 (--haiku) los días con corte_agua y NINGUNA señal: ¿la prensa es ruido o el modelo no lo ve? Haiku juzga las
     oraciones de esos días y las de una muestra igual de días explicados (semilla 21). Si la tasa de «hecho» es
     parecida (diferencia < 15 puntos), los días sin señal son en su mayoría hechos que el modelo no ve.
  P6 2021: los cortes de la RP2 en prensa con el río bajo, uno por uno, con la lluvia del municipio.

Informe: pipeline/noticias/contraste_noticias.md

Uso (desde neahub/):  python pipeline/noticias/contraste.py [--haiku] [--sin-tabla]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'pipeline'))
sys.path.insert(0, str(ROOT / 'pipeline' / 'noticias'))
from build_hexoma import DB, connect, log  # noqa: E402

MD = ROOT / 'pipeline' / 'noticias' / 'contraste_noticias.md'
DESDE = '2012-01-01'
FUERTE_MM = 60
CLASES = [-0.1, 5, 20, 40, 60, 100, 150, 1e9]
ETIQ = ['< 5', '5–20', '20–40', '40–60', '60–100', '100–150', '≥ 150']


def tabla(d: pd.DataFrame) -> str:
    d = d.astype(str)
    return '\n'.join(['| ' + ' | '.join(d.columns) + ' |', '|' + '---|' * len(d.columns)] +
                     ['| ' + ' | '.join(r) + ' |' for r in d.values])


def armar(conn) -> pd.DataFrame:
    cur = conn.cursor()
    t0 = time.time()
    cur.execute("""
        DROP TABLE IF EXISTS _pm;
        CREATE TEMP TABLE _pm AS
          SELECT m.gid municipio_gid, p.pixel_id FROM infra.imerg_pixel p
          JOIN infra.visor_municipios m ON ST_Contains(m.geom, ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326));
        INSERT INTO _pm
          SELECT m.gid, (SELECT p.pixel_id FROM infra.imerg_pixel p
                         ORDER BY ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326) <-> ST_PointOnSurface(m.geom) LIMIT 1)
          FROM infra.visor_municipios m WHERE m.gid NOT IN (SELECT municipio_gid FROM _pm);""")
    ll = pd.read_sql(f"""SELECT p.municipio_gid, d.fecha dia, avg(d.mm_dia) mm_media, max(d.mm_dia) mm_max
                         FROM infra.imerg_diaria d JOIN _pm p USING (pixel_id)
                         WHERE d.fecha >= '{DESDE}'::date - 2 GROUP BY 1, 2""", conn)
    ll['dia'] = pd.to_datetime(ll.dia)
    ll = ll.sort_values(['municipio_gid', 'dia'])
    ll['mm3'] = ll.groupby('municipio_gid').mm_media.transform(lambda s: s.rolling(3, min_periods=1).sum())
    ll = ll[ll.dia >= DESDE]
    log(f'lluvia: {len(ll):,} municipio-días ({time.time() - t0:.0f} s)')

    cur.execute('DROP TABLE IF EXISTS _est; CREATE TEMP TABLE _est (serie int, t text, alerta float, g geometry)')
    cur.execute("""INSERT INTO _est
        SELECT serie, 'u', alerta_m, ST_SetSRID(ST_MakePoint(lon, lat), 4326) FROM infra.rio_estaciones WHERE alerta_m IS NOT NULL
        UNION ALL
        SELECT serie, 'o', nivel_alerta_m, ST_SetSRID(ST_MakePoint(lon, lat), 4326) FROM infra.rio_estaciones_otras
        WHERE nivel_alerta_m IS NOT NULL""")
    rio = pd.read_sql(f"""
        WITH em AS (SELECT m.gid municipio_gid, e.serie, e.t, e.alerta FROM infra.visor_municipios m
                    JOIN _est e ON ST_DWithin(m.geom::geography, e.g::geography, 10000)),
        lec AS (SELECT serie, 'u' t, fecha, altura_m FROM infra.rio_lecturas WHERE qc = 'ok' AND fecha >= '{DESDE}'
                UNION ALL
                SELECT serie, 'o', fecha, altura_m FROM infra.rio_lecturas_otras WHERE qc = 'ok' AND fecha >= '{DESDE}')
        SELECT em.municipio_gid, lec.fecha dia, bool_or(lec.altura_m >= em.alerta) rio_alto
        FROM em JOIN lec USING (serie, t) GROUP BY 1, 2""", conn)
    rio['dia'] = pd.to_datetime(rio.dia)
    rp2 = pd.read_sql(f"""SELECT m.gid municipio_gid, r.fecha dia, bool_or(r.cortado) rp2_modelo
                          FROM infra.rio_puente_diario r JOIN infra.visor_puentes_rp2 p USING (gid)
                          JOIN infra.visor_municipios m ON ST_Contains(m.geom, ST_PointOnSurface(p.geom))
                          WHERE r.fecha >= '{DESDE}' GROUP BY 1, 2""", conn)
    rp2['dia'] = pd.to_datetime(rp2.dia)
    pr = pd.read_sql(f"""SELECT municipio_gid, dia,
                           bool_or(tipo IN ('corte_puente', 'corte_ruta_camino') AND motivo = 'agua') corte_agua,
                           bool_or(tipo IN ('crecida', 'evacuados_aislados')) crecida,
                           bool_or(tipo IN ('corte_puente', 'corte_ruta_camino') AND motivo = 'otro') corte_otro,
                           bool_or(tipo IN ('corte_puente', 'corte_ruta_camino') AND motivo = 'agua'
                                   AND altura_max_m IS NOT NULL) corte_con_altura
                         FROM noticias.v_matriz_municipio_dia WHERE dia >= '{DESDE}' AND municipio_gid IS NOT NULL
                         GROUP BY 1, 2""", conn)
    pr['dia'] = pd.to_datetime(pr.dia)
    d = ll.merge(rio, how='left').merge(rp2, how='left').merge(pr, how='left')
    for c in ['rio_alto', 'rp2_modelo', 'corte_agua', 'crecida', 'corte_otro', 'corte_con_altura']:
        d[c] = d[c].fillna(False).astype(bool)
    d['rp2_evaluado'] = d.municipio_gid.isin(rp2.municipio_gid.unique())
    d['lluvia_fuerte'] = (d.mm3 >= FUERTE_MM) | (d.mm_max >= FUERTE_MM)
    d['senal'] = np.select([(d.rio_alto | d.rp2_modelo) & d.lluvia_fuerte, d.rio_alto | d.rp2_modelo, d.lluvia_fuerte],
                           ['ambas', 'río', 'lluvia'], 'ninguna')
    nom = pd.read_sql('SELECT gid municipio_gid, nombre municipio FROM infra.visor_municipios', conn)
    return d.merge(nom, how='left')


def guardar(conn, d: pd.DataFrame) -> None:
    import io
    cur = conn.cursor()
    cols = ['municipio_gid', 'municipio', 'dia', 'mm_media', 'mm_max', 'mm3', 'lluvia_fuerte', 'rio_alto', 'rp2_modelo',
            'senal', 'corte_agua', 'crecida', 'corte_otro']
    cur.execute('DROP TABLE IF EXISTS noticias.contraste_municipio_dia')
    cur.execute("""CREATE TABLE noticias.contraste_municipio_dia (municipio_gid int, municipio text, dia date,
                   mm_media real, mm_max real, mm3 real, lluvia_fuerte boolean, rio_alto boolean, rp2_modelo boolean,
                   senal text, corte_agua boolean, crecida boolean, corte_otro boolean)""")
    buf = io.StringIO()
    d[cols].to_csv(buf, index=False, header=False)
    buf.seek(0)
    cur.copy_expert('COPY noticias.contraste_municipio_dia FROM STDIN WITH (FORMAT csv)', buf)
    cur.execute('CREATE INDEX ON noticias.contraste_municipio_dia (municipio_gid, dia)')
    cur.execute("COMMENT ON TABLE noticias.contraste_municipio_dia IS 'Prensa (cortes y crecidas por agua) contra lluvia "
                "IMERG y río (escalas INA, modelo RP2) por municipio y día: pipeline/noticias/contraste.py'")
    conn.commit()


def p1(d: pd.DataFrame) -> list[str]:
    L = ['## P1. Dosis-respuesta: lluvia de 3 días contra cortes por agua en la prensa (días sin río alto)', '',
         'Regla: P(corte_agua) crece en cada clase salvo una, y la clase más alta es ≥ 5 veces la más baja.', '']
    s = d[~d.rio_alto & ~d.rp2_modelo].copy()
    s['clase'] = pd.cut(s.mm3, CLASES, labels=ETIQ)
    filas, veredictos = [], []
    for nombre, g in [('2012–2026', s), ('2012–2019', s[s.dia < '2020-01-01']), ('2020–2026', s[s.dia >= '2020-01-01'])]:
        r = g.groupby('clase', observed=False).agg(dias=('corte_agua', 'size'), corte=('corte_agua', 'sum'),
                                                   crecida=('crecida', 'sum'), otro=('corte_otro', 'sum'))
        p = r.corte / r.dias
        sube = int((p.diff().dropna() > 0).sum())
        ok = sube >= len(p) - 2 and p.iloc[-1] >= 5 * p.iloc[0]
        veredictos.append(ok)
        for k, x in r.iterrows():
            filas.append({'período': nombre, 'mm en 3 días': k, 'municipio-días': f'{x.dias:,}',
                          'corte_agua %': f'{x.corte / x.dias:.2%}' if x.dias else '—',
                          'crecida %': f'{x.crecida / x.dias:.2%}' if x.dias else '—',
                          'corte sin motivo %': f'{x.otro / x.dias:.2%}' if x.dias else '—'})
        L_ = f'{nombre}: sube en {sube} de {len(p) - 1} pasos; más alta / más baja = {p.iloc[-1] / p.iloc[0]:.0f} veces'
        L.append(f'- {L_} → **{"PASA" if ok else "NO PASA"}**')
    L += ['', tabla(pd.DataFrame(filas)), '']
    return L


def p2(d: pd.DataFrame) -> list[str]:
    s = d[d.rp2_evaluado]
    a, b = s[s.rp2_modelo].corte_agua.mean(), s[~s.rp2_modelo].corte_agua.mean()
    return ['## P2. Río Uruguay: puentes de la RP2 cortados según el modelo, por municipio-día', '',
            f'Municipios con puentes RP2 evaluados: {s.municipio_gid.nunique()}; municipio-días con corte modelado: '
            f'{int(s.rp2_modelo.sum()):,} de {len(s):,}.', '',
            f'P(corte_agua en la prensa | modelo cortado) = {a:.1%}; P(… | no cortado) = {b:.2%}; razón {a / b:.0f} veces '
            f'(regla ≥ 5) → **{"PASA" if a / b >= 5 else "NO PASA"}**.', '']


def p3(d: pd.DataFrame) -> list[str]:
    s = d[d.corte_agua]
    r = s.groupby('senal').size().reindex(['río', 'lluvia', 'ambas', 'ninguna']).fillna(0).astype(int)
    base = d.groupby('senal').size().reindex(r.index)
    t = pd.DataFrame({'señal ese día': r.index, 'días con corte_agua': r.values,
                      '% de los cortes': (r / r.sum()).map('{:.0%}'.format).values,
                      'municipio-días con esa señal': base.map('{:,}'.format).values,
                      'P(corte_agua | señal)': (r / base).map('{:.2%}'.format).values})
    rio_bajo = s[(s.senal == 'lluvia')]
    # diagnóstico (no regla): un corte sigue en la prensa días después de la lluvia que lo causó
    d = d.sort_values(['municipio_gid', 'dia'])
    d['fuerte_7d'] = d.groupby('municipio_gid').lluvia_fuerte.transform(lambda x: x.rolling(8, min_periods=1).max()).astype(bool)
    d['rio_7d'] = d.groupby('municipio_gid').rio_alto.transform(lambda x: x.rolling(8, min_periods=1).max()).astype(bool)
    n0 = s[s.senal == 'ninguna'][['municipio_gid', 'dia']].merge(d[['municipio_gid', 'dia', 'fuerte_7d', 'rio_7d']])
    diag = (f'Diagnóstico (no es regla): de los {len(n0):,} días sin señal, {n0.fuerte_7d.mean():.0%} tuvo lluvia fuerte '
            f'en los 7 días previos y {n0.rio_7d.mean():.0%} río alto; sin ninguna de las dos en la semana: '
            f'{(~n0.fuerte_7d & ~n0.rio_7d).mean():.0%}.')
    return ['## P3. ¿De dónde viene cada día de corte en la prensa?', '',
            f'Lluvia fuerte = mm3 ≥ {FUERTE_MM} o píxel ≥ {FUERTE_MM} mm en el día (fijado antes de mirar). Río = alguna '
            'escala a ≤ 10 km sobre su alerta, o un puente de la RP2 cortado según el modelo.', '',
            tabla(t), '',
            f'Con el río bajo, {len(rio_bajo):,} municipio-días de corte coinciden con lluvia fuerte: son los cortes por '
            'arroyos que el modelo del río no ve.', '', diag, '']


def p4(conn, d: pd.DataFrame) -> list[str]:
    ex = pd.read_sql("""SELECT v.municipio, v.personas, v.sin_ruta, v.km_tierra_medio, m.gid municipio_gid
                        FROM infra.v_acceso_municipio v JOIN infra.visor_municipios m ON m.nombre = v.municipio
                        WHERE v.escenario = 'lluvia_fuerte' AND v.cota = 'pesimista'""", conn)
    s = d[d.lluvia_fuerte & ~d.rio_alto & ~d.rp2_modelo]
    r = s.groupby('municipio_gid').agg(dias=('corte_agua', 'size'), p=('corte_agua', 'mean')).reset_index()
    r = r.merge(ex, on='municipio_gid')
    r = r[r.dias >= 30]
    r['frac_sin_ruta'] = r.sin_ruta / r.personas
    L = ['## P4. Corte transversal: exposición del modelo del día de lluvia contra la prensa', '',
         f'Municipios con ≥ 30 días de lluvia fuerte sin río alto: {len(r)} (de {ex.municipio_gid.nunique()} con '
         'exposición calculada).', '']
    for c, nombre in [('frac_sin_ruta', 'fracción sin ruta con lluvia fuerte'), ('km_tierra_medio', 'km de tierra medios'),
                      ('sin_ruta', 'personas sin ruta')]:
        rho, pv = spearmanr(r[c], r.p, nan_policy='omit')
        L.append(f'- {nombre}: Spearman {rho:.2f} (p = {pv:.3f}, n = {r[c].notna().sum()}) → '
                 f'**{"PASA" if rho >= 0.3 else "NO PASA"}** (regla ≥ 0,3)')
    # POST HOC (no estaba en la regla): la prensa cubre más a unos municipios que a otros (Oberá, Eldorado); dividir
    # por la tasa del mismo municipio en días secos (mm3 < 5) saca la cobertura y deja la respuesta a la lluvia
    seco = d[(d.mm3 < 5) & ~d.rio_alto & ~d.rp2_modelo].groupby('municipio_gid').corte_agua.mean().rename('p_seco')
    r = r.merge(seco.reset_index(), on='municipio_gid', how='left')
    r['razon'] = r.p / r.p_seco.where(r.p_seco > 0)
    L += ['', 'Post hoc (fuera de la regla): con la razón P(corte | lluvia fuerte) / P(corte | seco) del mismo municipio, '
          'que saca la cobertura de prensa:', '']
    for c, nombre in [('frac_sin_ruta', 'fracción sin ruta con lluvia fuerte'), ('km_tierra_medio', 'km de tierra medios')]:
        rho, pv = spearmanr(r[c], r.razon, nan_policy='omit')
        L.append(f'- {nombre}: Spearman {rho:.2f} (p = {pv:.3f}, n = {int((r[c].notna() & r.razon.notna()).sum())})')
    top = r.sort_values('p', ascending=False).head(12)
    L += ['', 'Municipios con más prensa de corte por día de lluvia fuerte:', '',
          tabla(pd.DataFrame({'municipio': top.municipio, 'días lluvia fuerte': top.dias, 'P(corte_agua)': top.p.map('{:.1%}'.format),
                              'sin ruta (modelo)': top.sin_ruta.map('{:,.0f}'.format),
                              'fracción sin ruta': top.frac_sin_ruta.map('{:.0%}'.format),
                              'km tierra': top.km_tierra_medio.map('{:.1f}'.format)})), '']
    return L


def p5(conn, d: pd.DataFrame) -> list[str]:
    from clasificar_llm import MODELO, VERSION, clasificar, vale
    s = d[d.corte_agua]
    sin = s[s.senal == 'ninguna'][['municipio_gid', 'dia']]
    con = s[s.senal != 'ninguna'][['municipio_gid', 'dia']]
    con = con.sample(min(len(sin), len(con)), random_state=21)
    m = pd.read_sql(f"""SELECT m.mencion_id, m.oracion, m.tipo, m.medio, m.fecha_nota::date fecha_nota, n.titulo,
                               e.municipio, e.municipio_gid, e.fecha_desde, e.fecha_hasta
                        FROM noticias.eventos e JOIN noticias.evento_fuentes f USING (evento_id)
                        JOIN noticias.menciones m USING (mencion_id)
                        JOIN noticias.notas n ON n.nota_id = m.nota_id
                        WHERE e.motivo = 'agua' AND e.tipo IN ('corte_puente', 'corte_ruta_camino')
                          AND e.fecha_hasta >= '{DESDE}'""", conn)
    m['fecha_desde'] = pd.to_datetime(m.fecha_desde)
    m['fecha_hasta'] = pd.to_datetime(m.fecha_hasta)

    def de(celdas):
        x = m.merge(celdas, on='municipio_gid')
        return x[(x.dia >= x.fecha_desde) & (x.dia <= x.fecha_hasta)].drop_duplicates(['mencion_id', 'dia'])

    a, b = de(sin.assign(grupo='sin señal')), de(con.assign(grupo='con señal'))
    c = clasificar(conn, pd.concat([a, b]).drop_duplicates(['mencion_id', 'municipio_gid']))
    c['vale'] = vale(c)
    a = a.merge(c[['mencion_id', 'municipio_gid', 'hecho', 'vale']], on=['mencion_id', 'municipio_gid'], how='left')
    b = b.merge(c[['mencion_id', 'municipio_gid', 'hecho', 'vale']], on=['mencion_id', 'municipio_gid'], how='left')

    def celdas_ok(x):   # municipio-día con al menos una oración que la sostiene (clasificar_llm.vale)
        return x.assign(ok=x.vale.fillna(False).astype(bool)).groupby(['municipio_gid', 'dia']).ok.any()

    ca, cb = celdas_ok(a), celdas_ok(b)
    dif = (cb.mean() - ca.mean()) * 100
    sin_ok = ca[ca].reset_index().merge(d[['municipio_gid', 'dia', 'municipio', 'mm3', 'mm_max']], how='left')
    return [f'## P5. Días de corte sin ninguna señal: ¿ruido o hechos que el modelo no ve? ({MODELO}, {VERSION})', '',
            f'Municipio-días sin señal: {len(sin):,} ({a.mencion_id.nunique():,} oraciones); muestra de días con señal: '
            f'{len(con):,} ({b.mencion_id.nunique():,} oraciones, semilla 21).', '',
            f'- Celdas sostenidas por alguna oración (hecho reciente, en Misiones y en el municipio): sin señal **{ca.mean():.0%}**, con señal **{cb.mean():.0%}**.',
            f'- Oraciones dadas por hecho: sin señal {a.hecho.mean():.0%}, con señal {b.hecho.mean():.0%}.',
            f'- Diferencia {dif:.0f} puntos (regla: < 15 → los días sin señal son sobre todo hechos que el modelo no ve) → '
            f'**{"hechos no vistos" if dif < 15 else "más ruido sin señal"}**.', '',
            f'Días sin señal confirmados: lluvia de 3 días mediana {sin_ok.mm3.median():.0f} mm, píxel máximo mediano '
            f'{sin_ok.mm_max.median():.0f} mm (IMERG subestima aguaceros locales: 75,6 contra 190 mm en El Soberbio, '
            '21-jul-2026).', '',
            'Municipios con más días sin señal confirmados:', '',
            tabla(sin_ok.municipio.value_counts().head(10).rename_axis('municipio').reset_index(name='días')), '']


def p6(conn, d: pd.DataFrame) -> list[str]:
    e = pd.read_sql("""SELECT e.evento_id, e.municipio_gid, e.fecha_desde, e.lugar_nombre, e.precision_lugar, e.n_medios,
                              (SELECT f.oracion FROM noticias.evento_fuentes f WHERE f.evento_id = e.evento_id
                               ORDER BY f.fuente_oficial DESC, f.fecha_nota LIMIT 1) oracion,
                              (SELECT f.cita FROM noticias.evento_fuentes f WHERE f.evento_id = e.evento_id
                               ORDER BY f.fuente_oficial DESC, f.fecha_nota LIMIT 1) cita
                       FROM noticias.eventos e
                       WHERE e.motivo = 'agua' AND e.tipo IN ('corte_puente', 'corte_ruta_camino')
                         AND extract(year FROM e.fecha_desde) = 2021
                         AND (e.rutas ~ '(^|\\D)2(\\D|$)' OR lower(e.lugar_nombre) ~ '(costera|rp 2)')""", conn)
    e['dia'] = pd.to_datetime(e.fecha_desde)
    e = e.merge(d[['municipio_gid', 'dia', 'municipio', 'mm3', 'mm_max', 'rio_alto', 'rp2_modelo']], how='left')
    e = e.sort_values('dia').drop_duplicates(['dia', 'municipio_gid', 'oracion'])
    t = pd.DataFrame({'día': e.dia.dt.strftime('%d-%m'), 'municipio': e.municipio, 'lugar': e.lugar_nombre,
                      'mm 3 días': e.mm3.round(0), 'píxel máx': e.mm_max.round(0),
                      'río alto': e.rio_alto.map({True: 'sí', False: 'no'}), 'oración': e.oracion.str.slice(0, 140)})
    return ['## P6. 2021: cortes de la RP2 en la prensa, uno por uno', '',
            f'{len(e)} eventos; con el río bajo: {int((~e.rio_alto.fillna(False).astype(bool)).sum())}.', '', tabla(t), '']


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--haiku', action='store_true')
    ap.add_argument('--sin-tabla', action='store_true')
    a = ap.parse_args()
    conn = connect(DB)
    d = armar(conn)
    log(f'{len(d):,} municipio-días; con corte_agua {int(d.corte_agua.sum()):,}')
    if not a.sin_tabla:
        guardar(conn, d)
    L = ['# Contraste de la prensa con los escenarios de agua (Misiones, municipio × día)', '',
         f'Generado por `pipeline/noticias/contraste.py`. Período {DESDE} → {d.dia.max():%Y-%m-%d}; '
         f'{d.municipio_gid.nunique()} municipios; {len(d):,} municipio-días; {int(d.corte_agua.sum()):,} con corte por '
         f'agua en la prensa. Prensa: noticias v3.2 (reglas, sin filtro de Haiku salvo en P5).', '']
    L += p1(d) + p2(d) + p3(d) + p4(conn, d)
    if a.haiku:
        L += p5(conn, d)
    L += p6(conn, d)
    MD.write_text('\n'.join(L), encoding='utf-8')
    print('\n'.join(L))
    conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
