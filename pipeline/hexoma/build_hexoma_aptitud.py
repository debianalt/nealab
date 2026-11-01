"""Hexoma A4 paso 2 — esquema `aptitud`: aptitud de 9 cultivos por celda y campaña (umbrales validados).

Entradas: aptitud_clima.parquet (indicadores por píxel ERA5 y campaña, temperatura corregida con estaciones),
clima.chirps_campania (lluvia por píxel CHIRPS), clima.par / hex_pixel, suelo.h3 (SoilGrids y FABDEM) y la serie de
MapBiomas (uso_h3_anio_clase.parquet: bosque y pastizal inundables en 2025 = proxy de anegamiento). Reglas:
aptitud_reglas.py (copia de umbrales_cultivos.md; combinación FAO por factor más limitante).

    aptitud.factores        cultivo × factor: variable, tipo, parámetros, fuente, ámbito (clima o suelo)
    aptitud.clima_campania  (par_id, campania, cultivo): clase climática 1–4, factor limitante, clases de cada factor
                            (en el orden de aptitud.factores) y días > 35 °C de la ventana (informativo)
    aptitud.suelo           una fila por celda: inundable %, pendiente %, y por cultivo clase de suelo y factor limitante
    aptitud.resumen         una fila por celda: por cultivo % de campañas S1 y S1+S2 en 1991/92–2024/25, clase mediana
                            y factor limitante
    aptitud.v_h3_campania   (h3, cultivo, campania): clase final = la peor entre clima y suelo (filtrar por h3)

Clases: 1 = S1 (óptimo), 2 = S2, 3 = S3, 4 = N (no apto). Arroz: «sin dato de agua de riego». Campo natural: sin aptitud.
Idempotente. Uso (desde neahub/):
    python pipeline/hexoma/build_hexoma_aptitud.py
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
from aptitud_reglas import CALOR, CICLOS, NOMBRES, REGLAS, ROTULO, VAR_SUELO, clasificar  # noqa: E402
from clima_a_pixel import DERIV  # noqa: E402

FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
USO_PARQUET = FUENTES_DIR / 'uso' / 'uso_h3_anio_clase.parquet'
REF = (1991, 2024)  # campañas 1991/92 a 2024/25
CLASE_TXT = {1: 'S1', 2: 'S2', 3: 'S3', 4: 'N'}


def copy(cur, df: pd.DataFrame, tabla: str) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep='')
    buf.seek(0)
    cur.copy_expert(f"COPY {tabla} ({', '.join(df.columns)}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)


def pg_array(a) -> str:
    return '{' + ','.join(str(int(x)) for x in a) + '}'


def main() -> None:
    t0 = time.time()
    conn = connect(DB)
    # ---------------------------------------------------------------- clima por par y campaña
    ind = pd.read_parquet(DERIV / 'aptitud_clima.parquet')
    par = pd.read_sql('SELECT par_id, chirps_pixel, era5_pixel FROM clima.par', conn)
    p = pd.read_sql('SELECT pixel_id AS chirps_pixel, campania, p_mm FROM clima.chirps_campania', conn)
    cl = (par.merge(ind.rename(columns={'pixel_id': 'era5_pixel'}), on='era5_pixel')
             .merge(p, on=['chirps_pixel', 'campania']))
    log(f'clima: {len(cl):,} filas par × campaña ({cl.campania.min()}–{cl.campania.max()})')
    filas_clima = []
    for cult, facs in REGLAS.items():
        fc = [f for f in facs if f[1] not in VAR_SUELO]
        clases = np.stack([clasificar(t, prm, cl[v].values) for _, v, t, prm, _ in fc], axis=1) if fc else \
            np.ones((len(cl), 1), np.int8)
        clase = clases.max(axis=1)
        lim = np.array([fc[i][0] for i in clases.argmax(axis=1)]) if fc else np.array(['ninguno'] * len(cl))
        lim = np.where(clase == 1, 'ninguno', lim)
        d = pd.DataFrame({'par_id': cl.par_id.values, 'campania': cl.campania.values, 'cultivo': cult,
                          'clase': clase, 'limitante': lim,
                          'clases': [pg_array(r) for r in clases],
                          'dias35': cl[CALOR[cult]].values if cult in CALOR else None})
        filas_clima.append(d)
    clima = pd.concat(filas_clima, ignore_index=True)
    # ---------------------------------------------------------------- suelo por celda
    import duckdb
    s = pd.read_sql("""SELECT h3::text AS h3, ph_h2o AS ph, arcilla_pct AS arcilla,
                              100 * tan(radians(pendiente_media_grados)) AS pendiente FROM suelo.h3""", conn)
    inu = duckdb.sql(f"""SELECT h3, 100.0 * sum(n_px) FILTER (WHERE clase IN (6, 11)) / sum(n_px) AS inundable
                         FROM '{USO_PARQUET.as_posix()}' WHERE anio = 2025 GROUP BY h3""").df()
    s = s.merge(inu, on='h3', how='left')
    s['inundable'] = s.inundable.fillna(0)
    log(f'suelo: {len(s):,} celdas; inundable ≥ 50 %: {(s.inundable >= 50).sum():,}')
    sw = s[['h3', 'inundable', 'pendiente']].copy()
    for cult, facs in REGLAS.items():
        fs = [f for f in facs if f[1] in VAR_SUELO]
        cls = np.stack([clasificar(t, prm, s[v].values, arcilla=s.arcilla.values) for _, v, t, prm, _ in fs], axis=1)
        sin_dato = (cls == 0).any(axis=1)
        clase = cls.max(axis=1)
        lim = np.array([fs[i][0] for i in cls.argmax(axis=1)])
        sw[f'{cult}_clase'] = clase
        sw[f'{cult}_limitante'] = np.where(clase == 1, 'ninguno', lim)
        sw[f'{cult}_sin_dato'] = sin_dato
    # ---------------------------------------------------------------- resumen por celda
    hx = pd.read_sql('SELECT h3::text AS h3, par_id FROM clima.hex_pixel', conn)
    ref = clima[(clima.campania >= REF[0]) & (clima.campania <= REF[1])]
    res = hx.copy()
    for cult in REGLAS:
        r = ref[ref.cultivo == cult]
        g = r.groupby('par_id')
        agg = pd.DataFrame({'n': g.size(), 'le1': g.clase.apply(lambda x: (x <= 1).sum()),
                            'le2': g.clase.apply(lambda x: (x <= 2).sum()), 'le3': g.clase.apply(lambda x: (x <= 3).sum()),
                            'lim_c': g.limitante.agg(lambda x: x[x != 'ninguno'].mode().iat[0] if (x != 'ninguno').any() else 'ninguno')})
        j = hx.merge(agg, left_on='par_id', right_index=True, how='left').merge(
            sw[['h3', f'{cult}_clase', f'{cult}_limitante']], on='h3', how='left')
        sc = j[f'{cult}_clase'].fillna(0).values
        pct = {k: np.where(sc <= k, j[f'le{k}'] / j.n, 0) * 100 for k in (1, 2, 3)}
        med_clima = np.select([j.le1 / j.n >= .5, j.le2 / j.n >= .5, j.le3 / j.n >= .5], [1, 2, 3], 4)
        final = np.maximum(med_clima, sc)
        lim = np.where(final == 1, 'ninguno', np.where(sc >= med_clima, j[f'{cult}_limitante'], j.lim_c))
        falta = j.n.isna().values | (sc == 0)
        res[f'{cult}_s1_pct'] = pd.Series(np.where(falta, np.nan, np.round(pct[1]))).astype('Int16').values
        res[f'{cult}_s12_pct'] = pd.Series(np.where(falta, np.nan, np.round(pct[2]))).astype('Int16').values
        res[f'{cult}_clase'] = pd.Series(np.where(falta, np.nan, final)).astype('Int16').values
        res[f'{cult}_limitante'] = np.where(falta, None, lim)
    res = res.drop(columns='par_id')
    log('resumen calculado')
    # ---------------------------------------------------------------- carga
    with conn, conn.cursor() as cur:
        cur.execute('SET client_min_messages = warning')
        cur.execute('DROP SCHEMA IF EXISTS aptitud CASCADE')
        cur.execute('CREATE SCHEMA aptitud')
        cur.execute("COMMENT ON SCHEMA aptitud IS 'Hexoma A4: aptitud de 9 cultivos por celda y campaña con umbrales "
                    "validados por el autor (umbrales_cultivos.md), temperatura corregida con estaciones. "
                    "pipeline/hexoma/build_hexoma_aptitud.py'")
        cur.execute("""CREATE TABLE aptitud.factores (cultivo text, orden smallint, factor text, variable text, tipo text,
                       parametros jsonb, fuente text, ambito text, ciclo text, PRIMARY KEY (cultivo, factor))""")
        for cult, facs in REGLAS.items():
            oc = os_ = 0
            for f, v, t, prm, fu in facs:
                amb = 'suelo' if v in VAR_SUELO else 'clima'
                if amb == 'clima':
                    oc += 1
                orden = oc if amb == 'clima' else None
                cur.execute('INSERT INTO aptitud.factores VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                            (cult, orden, f, v, t, json.dumps(prm), fu, amb, CICLOS[cult]))
        cur.execute("COMMENT ON TABLE aptitud.factores IS 'Factores de cada cultivo; orden = posición en clima_campania.clases. "
                    "Tipos y parámetros: pipeline/hexoma/aptitud_reglas.py'")
        cur.execute("""CREATE TABLE aptitud.clima_campania (par_id int, campania smallint, cultivo text, clase smallint,
                       limitante text, clases smallint[], dias35 smallint, PRIMARY KEY (cultivo, par_id, campania))""")
        copy(cur, clima.astype({'dias35': 'Int16'}), 'aptitud.clima_campania')
        for c, dsc in (('clase', 'clase climática de la campaña: 1 S1, 2 S2, 3 S3, 4 N (factor más limitante)'),
                       ('limitante', 'factor climático que fija la clase (ninguno = S1)'),
                       ('clases', 'clase de cada factor climático, en el orden de aptitud.factores'),
                       ('dias35', 'días con Tmax corregida > 35 °C en la ventana del ciclo: informativo, sin clase (regla 5)')):
            cur.execute(f"COMMENT ON COLUMN aptitud.clima_campania.{c} IS '{dsc}'")
        cols = ['h3 text', 'inundable real', 'pendiente real'] + [
            x for c in REGLAS for x in (f'{c}_clase smallint', f'{c}_limitante text', f'{c}_sin_dato boolean')]
        cur.execute(f'CREATE TEMP TABLE ts ({", ".join(cols)})')
        copy(cur, sw, 'ts')
        cur.execute('CREATE TABLE aptitud.suelo AS SELECT h3::h3index AS h3, ' + ', '.join(c.split()[0] for c in cols[1:]) + ' FROM ts')
        cur.execute('ALTER TABLE aptitud.suelo ADD PRIMARY KEY (h3)')
        cur.execute("COMMENT ON COLUMN aptitud.suelo.inundable IS '% de la celda en bosque o pastizal inundable (MapBiomas Col. 3, 2025): proxy de anegamiento'")
        cur.execute("COMMENT ON COLUMN aptitud.suelo.pendiente IS 'pendiente media, % = 100·tan(grados FABDEM)'")
        rcols = ['h3 text'] + [x for c in REGLAS for x in (f'{c}_s1_pct smallint', f'{c}_s12_pct smallint',
                                                            f'{c}_clase smallint', f'{c}_limitante text')]
        cur.execute(f'CREATE TEMP TABLE tr ({", ".join(rcols)})')
        copy(cur, res, 'tr')
        cur.execute('CREATE TABLE aptitud.resumen AS SELECT h3::h3index AS h3, ' + ', '.join(c.split()[0] for c in rcols[1:]) + ' FROM tr')
        cur.execute('ALTER TABLE aptitud.resumen ADD PRIMARY KEY (h3)')
        for c in REGLAS:
            nom = NOMBRES[c]
            for col, dsc in ((f'{c}_s1_pct', f'{nom}: % de campañas 1991/92–2024/25 en S1'),
                             (f'{c}_s12_pct', f'{nom}: % de campañas 1991/92–2024/25 en S1 o S2'),
                             (f'{c}_clase', f'{nom}: clase mediana (1 S1, 2 S2, 3 S3, 4 N) combinando clima de cada campaña y suelo'),
                             (f'{c}_limitante', f'{nom}: factor que fija la clase mediana')):
                cur.execute(f"COMMENT ON COLUMN aptitud.resumen.{col} IS {chr(39)}{dsc.replace(chr(39), chr(39) * 2)}{chr(39)}")
            cur.execute(f"COMMENT ON COLUMN aptitud.suelo.{c}_clase IS 'clase de suelo de {nom} (pH, textura, pendiente, anegamiento según el cultivo)'")
        cur.execute("COMMENT ON COLUMN aptitud.resumen.arroz_clase IS 'Arroz: clase mediana térmica y de suelo. SIN DATO DE AGUA DE RIEGO (regla 6)'")
        casos = ' '.join(f"WHEN '{c}' THEN s.{c}_clase" for c in REGLAS)
        cur.execute(f"""CREATE VIEW aptitud.v_h3_campania AS
            SELECT h.h3, c.cultivo, c.campania, c.clase AS clase_clima, CASE c.cultivo {casos} END AS clase_suelo,
                   greatest(c.clase, CASE c.cultivo {casos} END) AS clase, c.limitante AS limitante_clima, c.dias35
            FROM clima.hex_pixel h JOIN aptitud.clima_campania c ON c.par_id = h.par_id
            JOIN aptitud.suelo s ON s.h3 = h.h3""")
        cur.execute("COMMENT ON VIEW aptitud.v_h3_campania IS 'Clase por celda, cultivo y campaña: la peor entre clima y suelo "
                    "(FAO, factor más limitante). Filtrar por h3'")
        cur.execute('ANALYZE')
    verificar(conn)
    log(f'listo en {time.time() - t0:.0f} s')


def verificar(conn) -> None:
    with conn.cursor() as cur:
        for c in REGLAS:
            cur.execute(f"""SELECT t.territorio, round(avg({c}_s12_pct)) AS s12,
                                   round(100.0 * avg(({c}_clase <= 2)::int)) AS pct_celdas_s12, mode() WITHIN GROUP (ORDER BY {c}_limitante)
                            FROM aptitud.resumen r JOIN (SELECT DISTINCT ON (h3) h3, territorio_id FROM hexagonos) x USING (h3)
                            JOIN territorios t USING (territorio_id) GROUP BY 1 ORDER BY 1""")
            print(f'  {NOMBRES[c]}:', [(a, int(b) if b is not None else None, int(d) if d is not None else None, e) for a, b, d, e in cur.fetchall()])
        cur.execute("SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c JOIN pg_namespace s ON "
                    "s.oid = c.relnamespace WHERE s.nspname = 'aptitud' AND c.relkind = 'r'")
        print('  tamaño:', cur.fetchone()[0], '(por territorio: % medio de campañas S1+S2, % de celdas con clase mediana S1–S2, limitante más común)')


if __name__ == '__main__':
    main()
