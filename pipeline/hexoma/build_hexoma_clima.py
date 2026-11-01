"""Hexoma A1 — esquema `clima`: series agroclimáticas por campaña, estación y año, NEA AR.

Carga los parquets de clima_a_pixel.py (%HEXOMA_FUENTES%/clima/derivados/) y arma:

    fuentes            asset, período, píxel, rótulo y archivos de origen (bytes, sha256)
    pixel              píxeles de origen (fuente, pixel_id, fila, col, lng, lat, z_m, geom)
    hex_pixel          una fila por celda: el píxel de CHIRPS y el de ERA5-Land que contienen su
                       centro, y el par (combinación de los dos) para el balance hídrico
    par                (par_id, chirps_pixel, era5_pixel)
    chirps_campania    lluvia por campaña jul–jun (campania = año de inicio), 1981/82 → 2025/26
    chirps_estacion    lo mismo por estación (DEF/MAM/JJA/SON; DEF 2023 = dic-2023 a feb-2024)
    era5_campania      temperatura, ET0 y humedad del suelo por campaña, 1950/51 → 2025/26
    era5_estacion      lo mismo por estación
    era5_helada        heladas por AÑO CALENDARIO, 1951 → 2025
    balance_campania   P (CHIRPS) − ET0 (ERA5-Land) por par y campaña; balance_estacion, por estación
    normal_1991_2020   n, media, mediana, P10 y P90 de cada variable en 1991–2020 (campañas
                       1991/92–2020/21; heladas 1991–2020; estaciones 1991–2020), por unidad
    anomalia           campañas y heladas: valor − normal, % de la normal y pct_ref = % de los
                       años de referencia con valor estrictamente menor (como la ficha)
    v_h3_campania, v_h3_estacion, v_h3_helada, v_h3_anomalia
                       vistas por celda (h3): llevan cada valor del píxel a la celda

Unidad espacial: el píxel de origen (CHIRPS ~5,5 km ≈ 70 celdas; ERA5-Land ~11 km ≈ 1.100). Una
celda lee el píxel que contiene su centro: no hay interpolación (decisión del 1-nov-2026). Por eso
las vistas repiten el mismo valor en todas las celdas de un píxel: es lo que la fuente mide.

Idempotente: rearma el esquema entero en una transacción. build_hexoma.py --reset no lo toca.

Uso (desde neahub/):
    python pipeline/hexoma/build_hexoma_clima.py
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from build_hexoma import DB, connect, log  # noqa: E402

FUENTES_DIR = Path(os.environ.get('HEXOMA_FUENTES', r'C:\Users\ant\hexoma-fuentes'))
CLIMA = FUENTES_DIR / 'clima'
DERIV = CLIMA / 'derivados'

ROT_CHIRPS = ('CHIRPS v3 (UCSB-CHC/CHIRPS/V3/DAILY_RNL), píxel 0,05°: estimación satelital calibrada '
              'con pluviómetros; el dato diario es una desagregación (temporalidad de ERA5), los conteos '
              'de días y las rachas son menos firmes que los totales')
ROT_ERA5 = ('ERA5-Land (ECMWF/ERA5_LAND/DAILY_AGGR), píxel 0,1°: reanálisis (modelo con observaciones '
            'asimiladas), día UTC; antes de 1979, menos observaciones')

# variable → (tipo SQL, unidad y definición)
VARS = {
    'chirps': {
        'p_mm': ('real', 'lluvia total, mm. VALIDADO: r mediana 0,92 por campaña contra 23 pluviómetros (GHCN e INTA), sesgo mediano +137 mm (~+10 %)'),
        'dias_lluvia': ('smallint', 'días con lluvia ≥ 1 mm. VALIDADO: ~36 días por campaña más que los pluviómetros (+46 %; desagregación diaria)'),
        'dias_p50': ('smallint', 'días con lluvia ≥ 50 mm'),
        'racha_seca_max': ('smallint', 'racha más larga de días con lluvia < 1 mm, días. Orientativa: el dato diario de CHIRPS tiene r ≈ 0,45 con los pluviómetros'),
        'racha_seca_max_oct_mar': ('smallint', 'racha seca más larga entre octubre y marzo (ciclo de verano), días'),
        'n_nulos': ('smallint', 'días sin dato'),
    },
    'era5': {
        'gdd10': ('real', 'grados-día base 10 °C: Σ max(0, (Tmax+Tmin)/2 − 10) del dato diario, °C·d. VALIDADO: sesgo mediano −55 °C·d (−1 %), r 0,84; en las sierras de Misiones −250 a −370 °C·d (hasta −10 %)'),
        'dias_tmax35': ('smallint', 'días con Tmax > 35 °C. SUBESTIMADO: Tmax de ERA5-Land ~2,1 °C más baja que en las estaciones'),
        'tmax_media': ('real', 'media de las temperaturas máximas diarias, °C. VALIDADO: sesgo mediano −2,1 °C, r 0,94, contra 48 estaciones (17 GHCN-Daily, 31 INTA SIGA), 1991–2025'),
        'tmin_media': ('real', 'media de las temperaturas mínimas diarias, °C. VALIDADO: sesgo mediano +1,5 °C, r 0,96, contra 48 estaciones (17 GHCN-Daily, 31 INTA SIGA), 1991–2025'),
        'et0_mm': ('real', 'evapotranspiración de referencia FAO-56 Penman-Monteith (pasto), suma, mm'),
        'sw1': ('real', 'humedad volumétrica del suelo 0–7 cm, media, m³/m³ (textura genérica del modelo)'),
        'sw2': ('real', 'humedad volumétrica del suelo 7–28 cm, media, m³/m³ (textura genérica del modelo)'),
        'sw3': ('real', 'humedad volumétrica del suelo 28–100 cm, media, m³/m³ (textura genérica del modelo)'),
        'n_nulos': ('smallint', 'días sin dato'),
    },
    'helada': {
        'dias_tmin_lt0': ('smallint', 'días con Tmin < 0 °C (helada meteorológica, abrigo a 2 m). NO USAR COMO CONTEO: en abrigo las estaciones registran 0,3–6 por año (más en Chaco y Mercedes que en las sierras de Misiones) y ERA5-Land ~0; a 5 cm del suelo, 8–24 por año'),
        'dias_tmin_lt_m2': ('smallint', 'días con Tmin < −2 °C. NO USAR COMO CONTEO (ver dias_tmin_lt0)'),
        'dias_tmin_le3': ('smallint', 'días con Tmin ≤ 3 °C (umbral de helada agrometeorológica de INTA). SUBESTIMADO: ~9 días por año menos que las estaciones (−78 %); r ≈ 0,43 entre años'),
        'primera_doy': ('smallint', 'día del año de la primera Tmin < 0 °C; NULL = sin helada'),
        'ultima_doy': ('smallint', 'día del año de la última Tmin < 0 °C; NULL = sin helada'),
        'tmin_abs': ('real', 'mínima absoluta del año, °C. SESGADA HACIA ARRIBA (Tmin de ERA5-Land ~1,5 °C alta en media, más en noches frías)'),
    },
}
SIN_NORMAL = {'n_nulos'}


def q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def copy_df(cur, df: pd.DataFrame, tabla: str) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep='')
    buf.seek(0)
    cur.copy_expert(f"COPY {tabla} ({', '.join(df.columns)}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)


def sha_archivos(fuente: str) -> list[dict]:
    out = []
    for f in sorted((CLIMA / 'diario' / fuente).glob(f'{fuente}_*.npz')):
        h = hashlib.sha256(f.read_bytes()).hexdigest()
        out.append({'archivo': f.name, 'bytes': f.stat().st_size, 'sha256': h})
    return out


def crear_tabla(cur, nombre: str, claves: list[tuple[str, str]], vars_: dict, rotulo: str, comentario: str) -> None:
    cols = [f'{k} {t}' for k, t in claves] + ['n_dias smallint'] + [f'{v} {t}' for v, (t, _) in vars_.items()]
    pk = ', '.join(k for k, _ in claves)
    cur.execute(f'CREATE TABLE clima.{nombre} ({", ".join(cols)}, PRIMARY KEY ({pk}))')
    cur.execute(f'COMMENT ON TABLE clima.{nombre} IS {q(comentario + ". Fuente: " + rotulo)}')
    for v, (_, d) in vars_.items():
        cur.execute(f'COMMENT ON COLUMN clima.{nombre}.{v} IS {q(d + ". " + rotulo)}')


def cargar(cur, nombre: str, archivo: str, claves: list[str], vars_: dict) -> int:
    df = pd.read_parquet(DERIV / archivo)
    cols = claves + ['n_dias'] + list(vars_)
    df = df[cols]
    for v, (t, _) in vars_.items():
        if t == 'smallint':
            df[v] = df[v].astype('Int16')
    copy_df(cur, df, f'clima.{nombre}')
    return len(df)


def normales_sql(tabla: str, unidad: str, periodo: str, vars_: list[str], desde: int, hasta: int,
                 estacion: bool = False) -> str:
    est = 'estacion' if estacion else 'NULL::text'
    vals = ', '.join(f"({q(v)}, {v}::float8)" for v in vars_)
    return f"""
        INSERT INTO clima.normal_1991_2020
        SELECT {q(tabla)}, t.{unidad}, x.variable, {est}, count(x.valor), avg(x.valor),
               percentile_cont(0.5) WITHIN GROUP (ORDER BY x.valor),
               percentile_cont(0.1) WITHIN GROUP (ORDER BY x.valor),
               percentile_cont(0.9) WITHIN GROUP (ORDER BY x.valor)
        FROM clima.{tabla} t, LATERAL (VALUES {vals}) AS x(variable, valor)
        WHERE t.{periodo} BETWEEN {desde} AND {hasta}
        GROUP BY t.{unidad}, x.variable{', estacion' if estacion else ''}"""


def anomalia_sql(tabla: str, unidad: str, periodo: str, vars_: list[str], desde: int, hasta: int) -> str:
    vals = ', '.join(f"({q(v)}, {v}::float8)" for v in vars_)
    return f"""
        WITH largo AS (
            SELECT t.{unidad} AS unidad_id, t.{periodo} AS periodo, x.variable, x.valor
            FROM clima.{tabla} t, LATERAL (VALUES {vals}) AS x(variable, valor)),
        ref AS (
            SELECT unidad_id, variable, array_agg(valor) AS vals FROM largo
            WHERE periodo BETWEEN {desde} AND {hasta} AND valor IS NOT NULL GROUP BY 1, 2)
        INSERT INTO clima.anomalia
        SELECT {q(tabla)}, l.unidad_id, l.variable, l.periodo, l.valor, n.media,
               l.valor - n.media,
               CASE WHEN n.media <> 0 THEN 100 * (l.valor / n.media - 1) END,
               CASE WHEN l.valor IS NOT NULL THEN
                    (SELECT 100.0 * count(*) FILTER (WHERE v < l.valor) / count(*) FROM unnest(r.vals) v) END
        FROM largo l
        JOIN ref r ON r.unidad_id = l.unidad_id AND r.variable = l.variable
        JOIN clima.normal_1991_2020 n ON n.tabla = {q(tabla)} AND n.unidad_id = l.unidad_id
                                     AND n.variable = l.variable AND n.estacion IS NULL"""


def main() -> None:
    t0 = time.time()
    conn = connect(DB)
    with conn, conn.cursor() as cur:
        cur.execute('SET client_min_messages = warning')
        cur.execute("SET work_mem = '512MB'")
        cur.execute('DROP SCHEMA IF EXISTS clima CASCADE')
        cur.execute('CREATE SCHEMA clima')
        cur.execute("COMMENT ON SCHEMA clima IS 'Hexoma A1: clima por campaña jul-jun, estación y año, en la "
                    "resolución de origen (CHIRPS v3, ERA5-Land), NEA AR. pipeline/hexoma/build_hexoma_clima.py'")

        # fuentes
        cur.execute("""CREATE TABLE clima.fuentes (fuente text PRIMARY KEY, asset text, periodo text,
                       pixel text, rotulo text, archivos jsonb, cargado timestamptz DEFAULT now())""")
        for f, asset, per, px, rot in (
                ('chirps', 'UCSB-CHC/CHIRPS/V3/DAILY_RNL', '1981-01-01 a 2026-06-30', '0,05°', ROT_CHIRPS),
                ('era5', 'ECMWF/ERA5_LAND/DAILY_AGGR', '1950-01-02 a 2026-06-30', '0,1°', ROT_ERA5)):
            cur.execute('INSERT INTO clima.fuentes (fuente, asset, periodo, pixel, rotulo, archivos) '
                        'VALUES (%s, %s, %s, %s, %s, %s)', (f, asset, per, px, rot, json.dumps(sha_archivos(f))))
        log('fuentes')

        # píxeles y mapeo
        cur.execute("""CREATE TABLE clima.pixel (fuente text, pixel_id bigint, fila int, col int,
                       lng float8, lat float8, z_m real, geom geometry(Polygon, 4326), PRIMARY KEY (fuente, pixel_id))""")
        for f, paso in (('chirps', 0.05), ('era5', 0.1)):
            p = pd.read_parquet(DERIV / f'pixel_{f}.parquet')
            p['fuente'] = f
            if 'z_m' not in p:
                p['z_m'] = None
            copy_df(cur, p[['fuente', 'pixel_id', 'fila', 'col', 'lng', 'lat', 'z_m']], 'clima.pixel')
            cur.execute(f"""UPDATE clima.pixel SET geom = ST_MakeEnvelope(lng - {paso / 2}, lat - {paso / 2},
                            lng + {paso / 2}, lat + {paso / 2}, 4326) WHERE fuente = {q(f)}""")
        cur.execute("COMMENT ON COLUMN clima.pixel.z_m IS 'ERA5-Land: altitud del píxel derivada de la presión media (FAO-56 ec. 7), m'")

        hc = pd.read_parquet(DERIV / 'hex_pixel_chirps.parquet').rename(columns={'pixel_id': 'chirps_pixel'})
        he = pd.read_parquet(DERIV / 'hex_pixel_era5.parquet').rename(columns={'pixel_id': 'era5_pixel'})
        hx = hc.merge(he, on='h3', how='outer', validate='1:1')
        assert hx.notna().all().all() and len(hx) == len(hc) == len(he)
        par = hx[['chirps_pixel', 'era5_pixel']].drop_duplicates().sort_values(['chirps_pixel', 'era5_pixel'])
        par['par_id'] = range(1, len(par) + 1)
        hx = hx.merge(par, on=['chirps_pixel', 'era5_pixel'])
        cur.execute('CREATE TABLE clima.par (par_id int PRIMARY KEY, chirps_pixel bigint, era5_pixel bigint)')
        copy_df(cur, par[['par_id', 'chirps_pixel', 'era5_pixel']], 'clima.par')
        cur.execute('CREATE TEMP TABLE hx (h3 text, chirps_pixel bigint, era5_pixel bigint, par_id int)')
        copy_df(cur, hx[['h3', 'chirps_pixel', 'era5_pixel', 'par_id']], 'hx')
        cur.execute("""CREATE TABLE clima.hex_pixel AS SELECT h3::h3index AS h3, chirps_pixel, era5_pixel, par_id FROM hx""")
        cur.execute('ALTER TABLE clima.hex_pixel ADD PRIMARY KEY (h3)')
        for c in ('chirps_pixel', 'era5_pixel', 'par_id'):
            cur.execute(f'CREATE INDEX ON clima.hex_pixel ({c})')
        cur.execute("COMMENT ON TABLE clima.hex_pixel IS 'Celda → píxel que contiene su centro, en cada fuente (sin interpolar)'")
        log(f'píxeles y mapeo: {len(hx):,} celdas, {len(par):,} pares')

        # indicadores
        n = {}
        crear_tabla(cur, 'chirps_campania', [('pixel_id', 'bigint'), ('campania', 'smallint')], VARS['chirps'],
                    ROT_CHIRPS, 'Lluvia por campaña agrícola jul–jun (campania = año de inicio)')
        n['chirps_campania'] = cargar(cur, 'chirps_campania', 'campania_chirps.parquet', ['pixel_id', 'campania'], VARS['chirps'])
        v_est = {k: v for k, v in VARS['chirps'].items() if k != 'racha_seca_max_oct_mar'}
        crear_tabla(cur, 'chirps_estacion', [('pixel_id', 'bigint'), ('anio_estacion', 'smallint'), ('estacion', 'text')],
                    v_est, ROT_CHIRPS, 'Lluvia por estación (DEF del año de diciembre)')
        n['chirps_estacion'] = cargar(cur, 'chirps_estacion', 'estacion_chirps.parquet', ['pixel_id', 'anio_estacion', 'estacion'], v_est)
        crear_tabla(cur, 'era5_campania', [('pixel_id', 'bigint'), ('campania', 'smallint')], VARS['era5'],
                    ROT_ERA5, 'Temperatura, ET0 y humedad del suelo por campaña agrícola jul–jun')
        n['era5_campania'] = cargar(cur, 'era5_campania', 'campania_era5.parquet', ['pixel_id', 'campania'], VARS['era5'])
        crear_tabla(cur, 'era5_estacion', [('pixel_id', 'bigint'), ('anio_estacion', 'smallint'), ('estacion', 'text')],
                    VARS['era5'], ROT_ERA5, 'Temperatura, ET0 y humedad del suelo por estación')
        n['era5_estacion'] = cargar(cur, 'era5_estacion', 'estacion_era5.parquet', ['pixel_id', 'anio_estacion', 'estacion'], VARS['era5'])
        crear_tabla(cur, 'era5_helada', [('pixel_id', 'bigint'), ('anio', 'smallint')], VARS['helada'],
                    ROT_ERA5, 'Heladas por año calendario (el invierno entero en un mismo año)')
        n['era5_helada'] = cargar(cur, 'era5_helada', 'helada_era5.parquet', ['pixel_id', 'anio'], VARS['helada'])
        log('indicadores: ' + ', '.join(f'{k} {v:,}' for k, v in n.items()))

        # balance hídrico por par
        for tabla, clave in (('balance_campania', 'campania'), ('balance_estacion', 'anio_estacion, estacion')):
            if tabla == 'balance_campania':
                on = 'e.campania = c.campania'
                sel = 'c.campania'
            else:
                on = 'e.anio_estacion = c.anio_estacion AND e.estacion = c.estacion'
                sel = 'c.anio_estacion, c.estacion'
            cur.execute(f"""CREATE TABLE clima.{tabla} AS
                SELECT p.par_id, {sel}, c.p_mm, e.et0_mm, (c.p_mm - e.et0_mm)::real AS p_menos_et0
                FROM clima.par p JOIN clima.chirps_{tabla.split('_')[1]} c ON c.pixel_id = p.chirps_pixel
                JOIN clima.era5_{tabla.split('_')[1]} e ON e.pixel_id = p.era5_pixel AND {on}""")
            cur.execute(f'ALTER TABLE clima.{tabla} ADD PRIMARY KEY (par_id, {clave})')
            cur.execute(f"COMMENT ON COLUMN clima.{tabla}.p_menos_et0 IS "
                        f"{q('balance hídrico climático: lluvia CHIRPS − ET0 FAO-56 de ERA5-Land, mm (negativo = déficit)')}")
        log('balance hídrico')

        # normales 1991–2020
        cur.execute("""CREATE TABLE clima.normal_1991_2020 (tabla text, unidad_id bigint, variable text, estacion text,
                       n smallint, media float8, mediana float8, p10 float8, p90 float8)""")
        cur.execute("COMMENT ON TABLE clima.normal_1991_2020 IS 'Normal OMM 1991–2020 de cada unidad (píxel o par): "
                    "campañas 1991/92–2020/21, años 1991–2020, estaciones con anio_estacion 1991–2020'")
        vc = [v for v in VARS['chirps'] if v not in SIN_NORMAL]
        ve = [v for v in VARS['era5'] if v not in SIN_NORMAL]
        vh = list(VARS['helada'])
        vce = [v for v in vc if v != 'racha_seca_max_oct_mar']
        for sql in (normales_sql('chirps_campania', 'pixel_id', 'campania', vc, 1991, 2020),
                    normales_sql('era5_campania', 'pixel_id', 'campania', ve, 1991, 2020),
                    normales_sql('era5_helada', 'pixel_id', 'anio', vh, 1991, 2020),
                    normales_sql('balance_campania', 'par_id', 'campania', ['p_menos_et0'], 1991, 2020),
                    normales_sql('chirps_estacion', 'pixel_id', 'anio_estacion', vce, 1991, 2020, True),
                    normales_sql('era5_estacion', 'pixel_id', 'anio_estacion', ve, 1991, 2020, True),
                    normales_sql('balance_estacion', 'par_id', 'anio_estacion', ['p_menos_et0'], 1991, 2020, True)):
            cur.execute(sql)
        cur.execute('CREATE INDEX ON clima.normal_1991_2020 (tabla, unidad_id, variable, estacion)')
        log('normales')

        # anomalías de campañas y heladas
        cur.execute("""CREATE TABLE clima.anomalia (tabla text, unidad_id bigint, variable text, periodo smallint,
                       valor float8, normal float8, anom float8, anom_pct float8, pct_ref float8)""")
        cur.execute("COMMENT ON TABLE clima.anomalia IS 'Campañas y heladas frente a la normal 1991–2020 de la "
                    "misma unidad. anom = valor − normal; anom_pct = % sobre la normal; pct_ref = % de los años de "
                    "referencia con valor estrictamente menor. No es un goalpost: compara la celda con su propia historia'")
        for sql in (anomalia_sql('chirps_campania', 'pixel_id', 'campania', vc, 1991, 2020),
                    anomalia_sql('era5_campania', 'pixel_id', 'campania', ve, 1991, 2020),
                    anomalia_sql('era5_helada', 'pixel_id', 'anio', vh, 1991, 2020),
                    anomalia_sql('balance_campania', 'par_id', 'campania', ['p_menos_et0'], 1991, 2020)):
            cur.execute(sql)
        cur.execute('ALTER TABLE clima.anomalia ADD PRIMARY KEY (tabla, unidad_id, variable, periodo)')
        log('anomalías')

        # vistas por celda
        cc = ', '.join(f'c.{v}' for v in VARS['chirps'] if v not in SIN_NORMAL)
        ce = ', '.join(f'e.{v}' for v in VARS['era5'] if v not in SIN_NORMAL)
        cee = ', '.join(f'c.{v}' for v in vce)
        cur.execute(f"""CREATE VIEW clima.v_h3_campania AS
            SELECT h.h3, e.campania, {cc}, {ce}, b.p_menos_et0
            FROM clima.hex_pixel h
            JOIN clima.era5_campania e ON e.pixel_id = h.era5_pixel
            LEFT JOIN clima.chirps_campania c ON c.pixel_id = h.chirps_pixel AND c.campania = e.campania
            LEFT JOIN clima.balance_campania b ON b.par_id = h.par_id AND b.campania = e.campania""")
        cur.execute(f"""CREATE VIEW clima.v_h3_estacion AS
            SELECT h.h3, e.anio_estacion, e.estacion, {cee}, {ce}, b.p_menos_et0
            FROM clima.hex_pixel h
            JOIN clima.era5_estacion e ON e.pixel_id = h.era5_pixel
            LEFT JOIN clima.chirps_estacion c ON c.pixel_id = h.chirps_pixel
                 AND c.anio_estacion = e.anio_estacion AND c.estacion = e.estacion
            LEFT JOIN clima.balance_estacion b ON b.par_id = h.par_id
                 AND b.anio_estacion = e.anio_estacion AND b.estacion = e.estacion""")
        hv = ', '.join(f'x.{v}' for v in VARS['helada'])
        cur.execute(f"""CREATE VIEW clima.v_h3_helada AS
            SELECT h.h3, x.anio, {hv} FROM clima.hex_pixel h JOIN clima.era5_helada x ON x.pixel_id = h.era5_pixel""")
        cur.execute("""CREATE VIEW clima.v_h3_anomalia AS
            SELECT h.h3, a.* FROM clima.hex_pixel h
              JOIN clima.anomalia a ON a.tabla = 'chirps_campania' AND a.unidad_id = h.chirps_pixel
            UNION ALL
            SELECT h.h3, a.* FROM clima.hex_pixel h
              JOIN clima.anomalia a ON a.tabla IN ('era5_campania', 'era5_helada') AND a.unidad_id = h.era5_pixel
            UNION ALL
            SELECT h.h3, a.* FROM clima.hex_pixel h
              JOIN clima.anomalia a ON a.tabla = 'balance_campania' AND a.unidad_id = h.par_id""")
        for v, d in (('v_h3_campania', 'por celda y campaña jul–jun (lluvia desde 1981/82, el resto desde 1950/51)'),
                     ('v_h3_estacion', 'por celda y estación'), ('v_h3_helada', 'por celda y año calendario'),
                     ('v_h3_anomalia', 'anomalías de campañas y heladas por celda (filtrar por h3: la vista une tres fuentes)')):
            cur.execute(f'COMMENT ON VIEW clima.{v} IS {q("Clima " + d + ". El valor es el del píxel que contiene el centro de la celda")}')
        log('vistas')
        cur.execute('ANALYZE')
    verificar(conn)
    log(f'listo en {time.time() - t0:.0f} s')


def verificar(conn) -> None:
    with conn.cursor() as cur:
        cur.execute('SELECT count(DISTINCT h3) FROM hexagonos')
        n_hex = cur.fetchone()[0]
        cur.execute('SELECT count(*) FROM clima.hex_pixel')
        n_map = cur.fetchone()[0]
        print(f'  celdas: {n_hex:,} en hexagonos, {n_map:,} con píxel', 'OK' if n_hex == n_map else 'DIFF')
        for tabla, per in (('chirps_campania', 'campania'), ('era5_campania', 'campania'), ('era5_helada', 'anio')):
            nulos = 'count(*) FILTER (WHERE n_nulos > 0)' if tabla != 'era5_helada' else '0'
            cur.execute(f'SELECT min({per}), max({per}), count(DISTINCT {per}), {nulos} FROM clima.{tabla}')
            print(f'  {tabla}: {cur.fetchone()}  (mín, máx, n períodos, filas con días sin dato)')
        cur.execute("""SELECT count(*) FROM clima.hex_pixel h
                       WHERE NOT EXISTS (SELECT 1 FROM clima.chirps_campania c WHERE c.pixel_id = h.chirps_pixel AND c.campania = 2020)
                          OR NOT EXISTS (SELECT 1 FROM clima.era5_campania e WHERE e.pixel_id = h.era5_pixel AND e.campania = 2020)""")
        print(f'  celdas sin campaña 2020/21 en alguna fuente: {cur.fetchone()[0]:,}')
        cur.execute("""SELECT t.territorio, round(avg(n.media)::numeric, 0)
                       FROM clima.hex_pixel h JOIN hexagonos x USING (h3) JOIN territorios t USING (territorio_id)
                       JOIN clima.normal_1991_2020 n ON n.tabla = 'chirps_campania' AND n.variable = 'p_mm' AND n.unidad_id = h.chirps_pixel
                       GROUP BY 1 ORDER BY 2""")
        print('  lluvia normal 1991–2020 por territorio (mm, media de celdas):', cur.fetchall())
        for nombre, lat, lng in (('Posadas', -27.386, -55.971), ('Bernardo de Irigoyen', -26.25, -53.65),
                                 ('Las Lomitas', -24.70, -60.583), ('Corrientes', -27.446, -58.762)):
            cur.execute(f"""SELECT h.era5_pixel FROM clima.hex_pixel h
                            WHERE h.h3 = h3_lat_lng_to_cell(ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326), 9)""")
            r = cur.fetchone()
            if not r:
                print(f'  {nombre}: sin celda')
                continue
            cur.execute("""SELECT variable, round(media::numeric, 1) FROM clima.normal_1991_2020
                           WHERE tabla IN ('era5_helada', 'era5_campania') AND unidad_id = %s AND estacion IS NULL
                             AND variable IN ('dias_tmin_lt0', 'gdd10', 'dias_tmax35', 'et0_mm') ORDER BY 1""", r)
            print(f'  {nombre}:', dict(cur.fetchall()))
        cur.execute("SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c "
                    "JOIN pg_namespace s ON s.oid = c.relnamespace WHERE s.nspname = 'clima' AND c.relkind = 'r'")
        print(f'  tamaño del esquema clima: {cur.fetchone()[0]}')


if __name__ == '__main__':
    main()
