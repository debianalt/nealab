"""Hexoma — censos 1991, 2001, 2010 y 2022 por radio censal (NEA argentino), en esquemas censo_<año>.

Fuentes (detalle y verificación en pipeline/hexoma/README.md):
    2001, 2010, 2022  el censo del INDEC ya agregado por radio (radio × variable × categoría × conteo,
                      sacado de las bases REDATAM), publicado en Source Cooperative con licencia
                      CC-BY-4.0: https://source.coop/nlebovits/censo-argentino
    1991              los microdatos REDATAM exportados por Open Redatam
                      (github.com/pachadotdev/redatam-microdata, ARG1991), pasados a radio con
                      pipeline/hexoma/redatam_a_radio.py, en el mismo formato
    2010 básico       la tabla del cuestionario básico completo por radio de Poblaciones
                      (poblaciones.org), importada en la base local `posadas` (censo_2010.nbi_2010 y
                      poblacion_2010): las bases de 2010 de las dos fuentes de arriba son la del
                      ampliado, que en las localidades grandes es una muestra
    radios            radios.parquet de cada año en Source Cooperative: geometría y totales oficiales
                      de personas y viviendas (2022: los polígonos de la web, para que el cruce coincida)

1991 y 2010 son muestrales en los radios grandes: cada radio se calibra con su total oficial de
personas (ver pipeline/hexoma/censo_specs.py). Los conteos crudos quedan en `categorias` y el
factor en `radios.factor_muestra`; `indicadores` ya está calibrado.

Por año, en el esquema censo_<año>:
    variables, diccionario   qué es cada variable y cada categoría
    categorias               TODO el censo: conteo crudo por (radio, variable, categoría)
    radios                   radio: departamento, atributos, totales oficiales, factor, área, geometría
    indicadores              indicadores por radio en conteos (num y den), calibrados
    basico (sólo 2010)       la tabla básica completa de Poblaciones, tal cual
    v_categorias             categorias con rótulos y el conteo expandido
    v_tasas_radio            tasas (%) y promedios por radio
Sólo censo_2022, hasta que exista el cruce único (esquema `cruce`):
    cruce                    celda → radio, ponderado por edificios
    v_h3 (materializada)     conteos y tasas por celda: Σ num·peso / Σ den·peso

Idempotente: rearma el esquema del año entero en una transacción. build_hexoma.py --reset no lo toca.

Uso (desde neahub/):
    python pipeline/build_hexoma_censo.py                    # los cuatro años
    python pipeline/build_hexoma_censo.py --anio 2010
    python pipeline/build_hexoma_censo.py --refresh          # vuelve a bajar las fuentes
1991 necesita antes:
    python pipeline/hexoma/redatam_a_radio.py --anio 1991 --csv pipeline/output/hexoma/redatam1991/csv
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent / 'hexoma'))
from build_hexoma import DB, connect, log  # noqa: E402
from censo_specs import PROV, SPECS  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / 'pipeline' / 'output'
TMP = OUTPUT / 'hexoma' / 'tmp'
POSADAS = 'posadas'

# 2022: el cruce celda → radio de las capas censales de la web (build_dasymetric_crosswalk.py)
CROSSWALKS = {
    'misiones': OUTPUT / 'h3_radio_crosswalk.parquet',
    'corrientes': OUTPUT / 'corrientes' / 'h3_radio_crosswalk_corrientes.parquet',
    'chaco': OUTPUT / 'chaco' / 'h3_radio_crosswalk_chaco.parquet',
    'formosa': OUTPUT / 'formosa' / 'h3_radio_crosswalk_formosa.parquet',
}
AREAL = {t: p.with_name(p.stem + '_areal.parquet') for t, p in CROSSWALKS.items()}
RADIOS_2022 = {
    'misiones': OUTPUT / 'radios_misiones.parquet',
    'corrientes': OUTPUT / 'corrientes' / 'radios_corrientes.parquet',
    'chaco': OUTPUT / 'chaco' / 'radios_chaco.parquet',
    'formosa': OUTPUT / 'formosa' / 'radios_formosa.parquet',
}


def ints(xs) -> str:
    return ', '.join(str(x) for x in xs) or 'NULL'


def cache(anio: int) -> Path:
    return OUTPUT / 'hexoma' / f'censo{anio}'


def prov_list() -> str:
    return ints(PROV)


def fetch_source(anio: int, refresh: bool) -> None:
    spec, d = SPECS[anio], cache(anio)
    d.mkdir(parents=True, exist_ok=True)
    data, meta, rad = d / 'census_nea.parquet', d / 'metadata.parquet', d / 'radios_nea.parquet'
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs')
    if spec['fuente'].startswith('http'):
        src = spec['fuente']
        if refresh or not data.exists() or not meta.exists():
            log(f'  bajando {src}/census-data.parquet (NEA) …')
            con.execute(f"""COPY (SELECT * FROM '{src}/census-data.parquet' WHERE prov_code IN ({prov_list()}))
                            TO '{data.as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
            con.execute(f"COPY (SELECT * FROM '{src}/metadata.parquet') TO '{meta.as_posix()}' (FORMAT parquet)")
    elif not data.exists():
        sys.exit(f'falta {data}: correr antes pipeline/hexoma/redatam_a_radio.py --anio {anio}')
    if refresh or not rad.exists():
        base = 'https://data.source.coop/nlebovits/censo-argentino'
        log(f'  bajando {base}/{anio}/radios.parquet (NEA) …')
        con.execute(f"""COPY (SELECT * FROM '{base}/{anio}/radios.parquet'
                              WHERE try_cast(PROV AS INTEGER) IN ({prov_list()}))
                        TO '{rad.as_posix()}' (FORMAT parquet)""")
    if spec.get('BASICO'):
        bas = d / 'basico_poblaciones.parquet'
        if refresh or not bas.exists():
            import pandas as pd
            conn = connect(POSADAS)
            log('  leyendo la tabla básica de Poblaciones desde la base posadas …')
            provs = ', '.join(f"'{p}'" for p in PROV)
            nbi = pd.read_sql(f"SELECT * FROM censo_2010.nbi_2010 WHERE left(redcode::text, 2) IN ({provs})", conn)
            pob = pd.read_sql(f"SELECT * FROM censo_2010.poblacion_2010 WHERE left(redcode::text, 2) IN ({provs})", conn)
            conn.close()
            geo = {'geometry', 'idprov', 'prov', 'iddpto', 'dpto', 'redcode_1', 'radios_pob', 'radios_hog',
                   'radios_lat', 'radios_lon', 'radios_sup'}
            nbi = nbi.drop(columns=[c for c in nbi.columns if c in geo])
            pob = pob.drop(columns=[c for c in pob.columns if c in geo])
            for df in (nbi, pob):
                df['redcode'] = df['redcode'].astype(int)
            pd.merge(nbi, pob, on='redcode', how='outer').to_parquet(bas, index=False)


def build_duck(anio: int) -> duckdb.DuckDBPyConnection:
    spec, d = SPECS[anio], cache(anio)
    data, meta, rad = (d / f for f in ('census_nea.parquet', 'metadata.parquet', 'radios_nea.parquet'))
    con = duckdb.connect()
    prov_case = ' '.join(f"WHEN {k} THEN '{v}'" for k, v in PROV.items())
    con.execute(f"""
        CREATE TABLE raw AS
        SELECT id_geo::INTEGER AS redcode, prov_code, CASE prov_code {prov_case} END AS territorio,
               valor_departamento AS depto_cod, etiqueta_departamento AS departamento,
               valor_fraccion AS fraccion, valor_radio AS radio,
               codigo_variable AS codigo, valor_categoria AS categoria,
               etiqueta_categoria AS etiqueta, try_cast(valor_categoria AS INTEGER) AS cat_int,
               conteo AS n
        FROM '{data.as_posix()}'
        WHERE split_part(codigo_variable, '_', 1) IN ('PERSONA', 'HOGAR', 'VIVIENDA')
          AND prov_code IN ({prov_list()})
    """)
    con.execute(f"""
        CREATE TABLE variables AS
        WITH m AS (SELECT codigo_variable, any_value(nombre_variable) AS nombre,
                          any_value(etiqueta_variable) AS etiqueta, any_value(entidad) AS entidad
                   FROM '{meta.as_posix()}' GROUP BY 1)
        SELECT (row_number() OVER (ORDER BY v.codigo))::SMALLINT AS variable_id, v.codigo,
               m.nombre, m.etiqueta, lower(coalesce(m.entidad, split_part(v.codigo, '_', 1))) AS entidad
        FROM (SELECT DISTINCT codigo FROM raw) v LEFT JOIN m ON m.codigo_variable = v.codigo
    """)
    con.execute("""
        CREATE TABLE diccionario AS
        SELECT v.variable_id, r.categoria, any_value(r.etiqueta) AS etiqueta
        FROM raw r JOIN variables v USING (codigo) GROUP BY 1, 2
    """)
    con.execute("""
        CREATE TABLE categorias AS
        SELECT r.redcode, v.variable_id, r.categoria, sum(r.n)::INTEGER AS n
        FROM raw r JOIN variables v USING (codigo) GROUP BY 1, 2, 3
    """)
    # radios oficiales (geometría y totales) ∪ radios con datos
    geom_col = spec['geometria']
    rcols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{rad.as_posix()}'").fetchall()}
    tipo = 'TIPO' if 'TIPO' in rcols else 'NULL'
    wkb = f'hex({geom_col})' if geom_col in rcols else 'NULL::VARCHAR'
    con.execute(f"""
        CREATE TABLE oficial AS
        SELECT {spec['cod']}::INTEGER AS redcode, try_cast(PROV AS INTEGER) AS prov_code,
               {spec['personas_oficial']}::DOUBLE AS personas_oficial,
               {spec['viviendas_oficial']}::DOUBLE AS viviendas_oficial,
               {tipo}::VARCHAR AS relevamiento, {wkb} AS wkb
        FROM '{rad.as_posix()}'
    """)
    if geom_col == 'local':  # 2022: los polígonos de la web, para que el cruce de las capas coincida
        con.execute('CREATE TABLE poligonos AS ' + ' UNION ALL '.join(
            f"SELECT redcode::INTEGER AS redcode, hex(geometry) AS wkb FROM '{p.as_posix()}'"
            for p in RADIOS_2022.values()))
        con.execute("""UPDATE oficial o SET wkb = p.wkb
                       FROM (SELECT redcode, any_value(wkb) AS wkb FROM poligonos GROUP BY 1) p
                       WHERE p.redcode = o.redcode""")
    # factor de muestra por radio: personas oficiales / personas en la base
    pm = spec['persona_muestra']
    if spec['muestra']:
        con.execute(f"""
            CREATE TABLE factor AS
            SELECT o.redcode, o.personas_oficial, s.n_muestra,
                   CASE WHEN s.n_muestra > 0 THEN o.personas_oficial / s.n_muestra END AS factor
            FROM oficial o LEFT JOIN (SELECT redcode, sum(n) AS n_muestra FROM raw WHERE codigo = '{pm}' GROUP BY 1) s
                 USING (redcode)""")
    else:
        con.execute(f"""CREATE TABLE factor AS
            SELECT redcode, NULL::DOUBLE AS personas_oficial, sum(n) AS n_muestra, 1.0::DOUBLE AS factor
            FROM raw WHERE codigo = '{pm}' GROUP BY 1""")
    # atributos del radio: la categoría de la mayoría de las viviendas (o personas) del radio
    attr = spec.get('ATTR', {})
    attr_sql = ''.join(f",\n max(etiqueta) FILTER (WHERE codigo = '{v}') AS {k}" for k, v in attr.items())
    attr_in = ', '.join(f"'{v}'" for v in attr.values()) or "''"
    con.execute(f"""
        CREATE TABLE radios_attr AS
        WITH g AS (SELECT redcode, any_value(territorio) AS territorio, any_value(depto_cod) AS depto_cod,
                          any_value(departamento) AS departamento, any_value(fraccion) AS fraccion,
                          any_value(radio) AS radio FROM raw GROUP BY 1),
        moda AS (SELECT redcode, codigo, arg_max(coalesce(etiqueta, categoria), n) AS etiqueta
                 FROM (SELECT redcode, codigo, etiqueta, categoria, sum(n) AS n FROM raw
                       WHERE codigo IN ({attr_in}) GROUP BY ALL)
                 GROUP BY 1, 2)
        SELECT g.*{attr_sql}
        FROM g LEFT JOIN moda USING (redcode)
        GROUP BY ALL
    """)
    # indicadores en conteos, calibrados por el factor del radio
    con.execute("CREATE TABLE rawf AS SELECT r.*, coalesce(f.factor, 1.0) AS f FROM raw r LEFT JOIN factor f USING (redcode)")
    cols = []
    for name, var, cats, _ in spec['TOTAL']:
        cond = f'AND cat_int IN ({ints(cats)})' if cats else ''
        cols.append(f"sum(n * f) FILTER (WHERE codigo = '{var}' {cond}) AS {name}")
    for name, var, num, excl, _ in spec['PCT']:
        cols.append(f"sum(n * f) FILTER (WHERE codigo = '{var}' AND cat_int IN ({ints(num)})) AS {name}_num")
        cols.append(f"sum(n * f) FILTER (WHERE codigo = '{var}' AND cat_int NOT IN ({ints(excl)})) AS {name}_den"
                    if excl else f"sum(n * f) FILTER (WHERE codigo = '{var}') AS {name}_den")
    for name, var, excl, _ in spec['MEAN']:
        ex = f"AND cat_int NOT IN ({ints(excl)})" if excl else ''
        cols.append(f"sum(n * f * cat_int) FILTER (WHERE codigo = '{var}' {ex}) AS {name}_num")
        cols.append(f"sum(n * f) FILTER (WHERE codigo = '{var}' {ex}) AS {name}_den")
    con.execute(f"CREATE TABLE ind_datos AS SELECT redcode, {', '.join(cols)} FROM rawf GROUP BY redcode")
    # universo: radios oficiales ∪ radios con datos (∪ radios de la tabla básica)
    uni = ['SELECT redcode FROM oficial', 'SELECT redcode FROM ind_datos']
    bas_cols = ''
    if spec.get('BASICO'):
        con.execute(f"CREATE TABLE basico AS SELECT * FROM '{(cache(anio) / 'basico_poblaciones.parquet').as_posix()}'")
        uni.append('SELECT redcode FROM basico')
        bas_cols = ', b.h_total::DOUBLE AS hogares, b.p_total::DOUBLE AS personas_en_hogares, ' + ', '.join(
            f'({num})::DOUBLE AS {n}_num, ({den})::DOUBLE AS {n}_den' for n, num, den, _ in spec['BASICO'])
    con.execute(f"""
        CREATE TABLE indicadores AS
        SELECT u.redcode, o.personas_oficial, o.viviendas_oficial, d.* EXCLUDE (redcode){bas_cols}
        FROM (SELECT DISTINCT redcode FROM ({' UNION ALL '.join(uni)})) u
        LEFT JOIN oficial o USING (redcode)
        LEFT JOIN ind_datos d USING (redcode)
        {'LEFT JOIN basico b USING (redcode)' if spec.get('BASICO') else ''}
    """)
    if anio == 2022:
        build_cruce_2022(con)
    return con


def build_cruce_2022(con) -> None:
    con.execute('CREATE TABLE cruce_edif AS ' + ' UNION ALL '.join(
        f"SELECT h3index AS h3, '{t}' AS territorio, redcode::INTEGER AS redcode, weight AS peso, "
        f"n_buildings::INTEGER AS n_edificios, est_personas, est_hogares FROM '{p.as_posix()}'"
        for t, p in CROSSWALKS.items()))
    # Respaldo por área (como build_dasymetric_crosswalk.py) para los radios con población
    # que no tienen edificios en el cruce: hoy uno solo, en Esquina (Corrientes).
    con.execute('CREATE TABLE cruce_area AS ' + ' UNION ALL '.join(
        f"SELECT h3index AS h3, '{t}' AS territorio, redcode::INTEGER AS redcode, weight AS peso "
        f"FROM '{AREAL[t].as_posix()}'" for t in CROSSWALKS))
    con.execute("""
        CREATE TABLE cruce AS
        SELECT *, 'edificios' AS fuente FROM cruce_edif
        UNION ALL
        SELECT a.h3, a.territorio, a.redcode, a.peso, NULL, NULL, NULL, 'area'
        FROM cruce_area a JOIN indicadores i USING (redcode)
        WHERE i.personas > 0 AND a.redcode NOT IN (SELECT redcode FROM cruce_edif)
    """)


def copy_table(con, cur, duck_sql: str, pg_table: str, cols: list[str]) -> int:
    TMP.mkdir(parents=True, exist_ok=True)
    f = TMP / f'{pg_table.replace(".", "_")}.csv'
    con.execute(f"COPY ({duck_sql}) TO '{f.as_posix()}' (FORMAT csv, HEADER false, NULL '')")
    with open(f, encoding='utf-8') as fh:
        cur.copy_expert(f"COPY {pg_table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL '')", fh)
    n = cur.rowcount
    f.unlink()
    return n


def q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def fuente_txt(anio: int) -> str:
    s = SPECS[anio]
    txt = {
        1991: 'microdatos REDATAM del INDEC exportados por Open Redatam (github.com/pachadotdev/redatam-microdata, ARG1991), '
              'pasados a radio con pipeline/hexoma/redatam_a_radio.py; muestra calibrada por radio',
        2010: 'ampliado: source.coop/nlebovits/censo-argentino (CC-BY-4.0), muestra calibrada por radio; '
              'básico completo: tabla por radio de Poblaciones (poblaciones.org, CC-BY-4.0)',
    }.get(anio, 'source.coop/nlebovits/censo-argentino (CC-BY-4.0), sacado de la base REDATAM del INDEC')
    return f'Censo {anio} (INDEC) por radio censal, NEA argentino. Fuente: {txt}. Radios y totales oficiales: radios.parquet de Source Cooperative.' + (
        ' Muestra: ver radios.factor_muestra.' if s['muestra'] else '')


def load_pg(con, anio: int) -> None:
    spec, sch = SPECS[anio], f'censo_{anio}'
    conn = connect(DB)
    t0 = time.time()
    with conn, conn.cursor() as cur:
        cur.execute("SET client_min_messages = warning")
        cur.execute(f"DROP SCHEMA IF EXISTS {sch} CASCADE")
        if anio == 2022:
            cur.execute("DROP SCHEMA IF EXISTS censo CASCADE")  # nombre anterior del esquema 2022
        cur.execute(f"CREATE SCHEMA {sch}")
        cur.execute(f"COMMENT ON SCHEMA {sch} IS {q(fuente_txt(anio))}")

        cur.execute(f"""CREATE TABLE {sch}.variables (variable_id smallint PRIMARY KEY, codigo text UNIQUE,
                        nombre text, etiqueta text, entidad text)""")
        log('  variables', copy_table(con, cur, 'SELECT variable_id, codigo, nombre, etiqueta, entidad FROM variables',
                                      f'{sch}.variables', ['variable_id', 'codigo', 'nombre', 'etiqueta', 'entidad']))
        cur.execute(f"""CREATE TABLE {sch}.diccionario (variable_id smallint REFERENCES {sch}.variables,
                        categoria text, etiqueta text, PRIMARY KEY (variable_id, categoria))""")
        log('  diccionario', copy_table(con, cur, 'SELECT * FROM diccionario', f'{sch}.diccionario',
                                        ['variable_id', 'categoria', 'etiqueta']))

        attr = list(spec.get('ATTR', {}))
        rcols = (['redcode', 'territorio', 'depto_cod', 'departamento', 'fraccion', 'radio'] + attr
                 + ['relevamiento', 'personas_oficial', 'viviendas_oficial', 'n_muestra', 'factor_muestra', 'wkb_hex'])
        num = {'redcode', 'personas_oficial', 'viviendas_oficial', 'n_muestra', 'factor_muestra'}
        cur.execute(f"CREATE TEMP TABLE radios_in ({', '.join(c + (' double precision' if c in num else ' text') for c in rcols)})")
        prov_case = ' '.join(f"WHEN {k} THEN '{v}'" for k, v in PROV.items())
        log('  radios', copy_table(con, cur, f"""
            SELECT coalesce(o.redcode, a.redcode) AS redcode,
                   coalesce(a.territorio, CASE o.prov_code {prov_case} END) AS territorio,
                   a.depto_cod, a.departamento, a.fraccion, a.radio,
                   {''.join(f'a.{c}, ' for c in attr)}
                   o.relevamiento, o.personas_oficial, o.viviendas_oficial, f.n_muestra,
                   {'f.factor' if spec['muestra'] else '1.0'} AS factor_muestra, o.wkb
            FROM oficial o FULL JOIN radios_attr a USING (redcode) LEFT JOIN factor f ON f.redcode = coalesce(o.redcode, a.redcode)""",
            'radios_in', rcols))
        cur.execute(f"""CREATE TABLE {sch}.radios (
            redcode integer PRIMARY KEY, territorio_id smallint REFERENCES territorios,
            depto_cod text, departamento text, fraccion text, radio text,
            {''.join(f'{c} text, ' for c in attr)}
            relevamiento text, personas_oficial double precision, viviendas_oficial double precision,
            n_muestra double precision, factor_muestra double precision,
            area_km2 double precision, geom geometry(MultiPolygon, 4326))""")
        cur.execute(f"""INSERT INTO {sch}.radios
            SELECT r.redcode::integer, t.territorio_id, r.depto_cod, r.departamento, r.fraccion, r.radio,
                   {''.join(f'r.{c}, ' for c in attr)}
                   r.relevamiento, r.personas_oficial, r.viviendas_oficial, r.n_muestra, r.factor_muestra, NULL,
                   CASE WHEN r.wkb_hex IS NOT NULL THEN ST_Multi(ST_CollectionExtract(ST_MakeValid(
                        ST_SetSRID(ST_GeomFromWKB(decode(r.wkb_hex, 'hex')), 4326)), 3)) END
            FROM radios_in r LEFT JOIN territorios t USING (territorio)""")
        cur.execute(f"""UPDATE {sch}.radios SET area_km2 = ST_Area(geom::geography) / 1e6;
                        CREATE INDEX ON {sch}.radios USING gist (geom);""")

        icols = [c for c in con.execute("SELECT * FROM indicadores LIMIT 0").fetchdf().columns]
        cur.execute(f"CREATE TABLE {sch}.indicadores (redcode integer PRIMARY KEY REFERENCES {sch}.radios, "
                    + ', '.join(f'{c} double precision' for c in icols if c != 'redcode') + ')')
        log('  indicadores', copy_table(con, cur, 'SELECT * FROM indicadores', f'{sch}.indicadores', icols))

        cur.execute(f"""CREATE TABLE {sch}.categorias (redcode integer REFERENCES {sch}.radios,
                        variable_id smallint REFERENCES {sch}.variables, categoria text, n integer NOT NULL,
                        PRIMARY KEY (redcode, variable_id, categoria))""")
        log('  categorias', copy_table(con, cur, 'SELECT * FROM categorias', f'{sch}.categorias',
                                       ['redcode', 'variable_id', 'categoria', 'n']))
        cur.execute(f"CREATE INDEX ON {sch}.categorias (variable_id, categoria)")

        if spec.get('BASICO'):
            bcols = [c for c in con.execute("SELECT * FROM basico LIMIT 0").fetchdf().columns]
            cur.execute(f"CREATE TABLE {sch}.basico (redcode integer PRIMARY KEY REFERENCES {sch}.radios, "
                        + ', '.join(f'{c} double precision' for c in bcols if c != 'redcode') + ')')
            log('  basico', copy_table(con, cur, 'SELECT * FROM basico', f'{sch}.basico', bcols))

        if anio == 2022:
            load_cruce_2022(con, cur, sch)
        log(f'  cargado en {time.time() - t0:.0f} s; vistas …')
        create_views(cur, anio, sch)
        comments(cur, anio, sch)
    conn.autocommit = True
    with conn.cursor() as cur:
        for t in ('radios', 'indicadores', 'categorias') + (('cruce', 'v_h3') if anio == 2022 else ()):
            cur.execute(f'ANALYZE {sch}.{t}')
    conn.close()


def load_cruce_2022(con, cur, sch: str) -> None:
    cur.execute(f"""CREATE TABLE {sch}.cruce (h3 h3index NOT NULL, territorio_id smallint NOT NULL REFERENCES territorios,
                    redcode integer NOT NULL, peso double precision NOT NULL, n_edificios integer,
                    est_personas double precision, est_hogares double precision, fuente text NOT NULL,
                    PRIMARY KEY (h3, territorio_id, redcode))""")
    cur.execute("CREATE TEMP TABLE cruce_in (h3 text, territorio text, redcode integer, peso double precision, "
                "n_edificios integer, est_personas double precision, est_hogares double precision, fuente text)")
    log('  cruce', copy_table(con, cur, 'SELECT * FROM cruce', 'cruce_in',
                              ['h3', 'territorio', 'redcode', 'peso', 'n_edificios', 'est_personas', 'est_hogares', 'fuente']))
    cur.execute(f"""INSERT INTO {sch}.cruce
                    SELECT c.h3::h3index, t.territorio_id, c.redcode, c.peso, c.n_edificios, c.est_personas, c.est_hogares, c.fuente
                    FROM cruce_in c JOIN territorios t USING (territorio)""")
    cur.execute(f"CREATE INDEX ON {sch}.cruce (redcode)")


def specs_all(spec: dict):
    """(nombre, rótulo, tipo) de cada columna de indicadores: 'total', 'pct' o 'mean'."""
    for n, var, cats, label in spec['TOTAL']:
        yield n, label + f' ({var})', 'total'
    for n, var, num, excl, label in spec['PCT']:
        yield n, f'{label} — {var}: num {num}, den todas' + (f' salvo {excl}' if excl else ''), 'pct'
    for n, num, den, label in spec.get('BASICO', []):
        yield n, f'{label} — tabla básica: num {num}, den {den}', 'pct'
    for n, var, excl, label in spec['MEAN']:
        yield n, f'{label} — {var}' + (f', sin {excl}' if excl else ''), 'mean'


def create_views(cur, anio: int, sch: str) -> None:
    spec = SPECS[anio]
    cur.execute(f"""CREATE VIEW {sch}.v_categorias AS
        SELECT c.redcode, r.territorio_id, r.departamento, v.codigo, v.etiqueta AS variable, v.entidad,
               c.categoria, d.etiqueta AS categoria_etiqueta, c.n, c.n * coalesce(r.factor_muestra, 1) AS n_expandido
        FROM {sch}.categorias c JOIN {sch}.variables v USING (variable_id)
        JOIN {sch}.diccionario d USING (variable_id, categoria) JOIN {sch}.radios r USING (redcode)""")
    items = list(specs_all(spec))
    totals = ['i.personas_oficial', 'i.viviendas_oficial'] + [f'i.{n}' for n, _, k in items if k == 'total']
    if spec.get('BASICO'):
        totals += ['i.hogares', 'i.personas_en_hogares']
    tasas = [f"round((100.0 * i.{n}_num / nullif(i.{n}_den, 0))::numeric, 2) AS {n}" for n, _, k in items if k == 'pct']
    medias = [f"round((i.{n}_num / nullif(i.{n}_den, 0))::numeric, 3) AS {n}" for n, _, k in items if k == 'mean']
    cur.execute(f"""CREATE VIEW {sch}.v_tasas_radio AS
        SELECT i.redcode, r.territorio_id, t.territorio, r.departamento, r.relevamiento, r.factor_muestra, r.area_km2,
               round((i.personas_oficial / nullif(r.area_km2, 0))::numeric, 1) AS densidad_hab_km2,
               {', '.join(totals + tasas + medias)}
        FROM {sch}.indicadores i JOIN {sch}.radios r USING (redcode) LEFT JOIN territorios t USING (territorio_id)""")
    if anio != 2022:
        return
    # por celda: conteos repartidos por el peso; tasas = Σ num·peso / Σ den·peso
    # real (float4): 7 cifras alcanzan para conteos por celda y la vista ocupa la mitad
    tot = [n for n, _, k in items if k == 'total']
    rat = [n for n, _, k in items if k in ('pct', 'mean')]
    sums = [f"sum(i.{n} * c.peso)::real AS {n}" for n in tot]
    for n in rat:
        sums += [f"sum(i.{n}_num * c.peso)::real AS {n}_num", f"sum(i.{n}_den * c.peso)::real AS {n}_den"]
    rates = [f"round((100.0 * {n}_num / nullif({n}_den, 0))::numeric, 2) AS {n}" for n, _, k in items if k == 'pct']
    rates += [f"round(({n}_num / nullif({n}_den, 0))::numeric, 3) AS {n}" for n, _, k in items if k == 'mean']
    cur.execute(f"""CREATE MATERIALIZED VIEW {sch}.v_h3 AS
        WITH s AS (
            SELECT c.h3, c.territorio_id, count(*)::smallint AS n_radios,
                   (array_agg(c.redcode ORDER BY c.peso * coalesce(i.hogares, 0) DESC))[1] AS redcode,
                   {', '.join(sums)}
            FROM {sch}.cruce c JOIN {sch}.indicadores i USING (redcode)
            WHERE coalesce({', '.join(f'i.{n}' for n in tot)}) IS NOT NULL  -- fuera los radios sin datos (sólo en radios.parquet)
            GROUP BY 1, 2)
        SELECT s.h3, s.territorio_id, t.territorio, s.redcode, s.n_radios,
               {', '.join(f's.{n}' for n in tot)}, {', '.join(rates)},
               {', '.join(f's.{n}_num, s.{n}_den' for n in rat)}
        FROM s JOIN territorios t USING (territorio_id)""")
    cur.execute(f"CREATE UNIQUE INDEX ON {sch}.v_h3 (h3, territorio_id)")
    cur.execute(f"CREATE INDEX ON {sch}.v_h3 (h3, territorio)")


def comments(cur, anio: int, sch: str) -> None:
    spec = SPECS[anio]
    c = cur.execute
    c(f"COMMENT ON TABLE {sch}.categorias IS {q(f'Todo el censo {anio}: personas, hogares o viviendas de cada categoría de cada variable, por radio. Conteo CRUDO' + (' (en los radios con muestra, casos de la muestra: multiplicar por radios.factor_muestra)' if spec['muestra'] else ''))}")
    c(f"COMMENT ON TABLE {sch}.indicadores IS {q('Indicadores por radio en conteos: <x>_num y <x>_den, calibrados por el factor de muestra (las tasas se calculan sumando conteos: radio, celda o departamento). personas_oficial y viviendas_oficial: totales oficiales del radio')}")
    c(f"COMMENT ON TABLE {sch}.radios IS {q(f'Radios censales {anio} del NEA argentino: los de radios.parquet (geometría y totales oficiales) y los que tienen datos. relevamiento = tipo de cuestionario del radio (100 % ampliado, ampliado por muestra, sólo básico). factor_muestra = personas_oficial / n_muestra (personas en la base)')}")
    c(f"COMMENT ON VIEW {sch}.v_tasas_radio IS {q('Tasas (%) y promedios por radio, desde indicadores')}")
    for obj in (f'{sch}.indicadores', f'{sch}.v_tasas_radio'):
        c(f"COMMENT ON COLUMN {obj}.personas_oficial IS 'Población total oficial del radio (radios.parquet)'")
        c(f"COMMENT ON COLUMN {obj}.viviendas_oficial IS 'Viviendas oficiales del radio (radios.parquet)'")
    targets = [f'{sch}.v_tasas_radio'] + ([f'{sch}.v_h3'] if anio == 2022 else [])
    for n, label, k in specs_all(spec):
        if k == 'total':
            for obj in [f'{sch}.indicadores'] + targets:
                c(f"COMMENT ON COLUMN {obj}.{n} IS {q(label)}")
        else:
            c(f"COMMENT ON COLUMN {sch}.indicadores.{n}_num IS {q('Numerador: ' + label)}")
            c(f"COMMENT ON COLUMN {sch}.indicadores.{n}_den IS {q('Denominador: ' + label)}")
            for obj in targets:
                c(f"COMMENT ON COLUMN {obj}.{n} IS {q(('% ' if k == 'pct' else '') + label)}")
    if anio == 2022:
        c(f"COMMENT ON TABLE {sch}.cruce IS 'Celda → radio. peso = edificios del radio en la celda / edificios del radio (suma 1 por radio). De pipeline/output/**/h3_radio_crosswalk*.parquet; fuente = area: respaldo por superficie para radios con población sin edificios'")
        c(f"COMMENT ON MATERIALIZED VIEW {sch}.v_h3 IS 'Censo por celda: conteos × peso; tasas = Σ num·peso / Σ den·peso. redcode = radio que aporta más hogares a la celda. Unible a v_hexoma por (h3, territorio)'")
    if spec.get('BASICO'):
        c(f"COMMENT ON TABLE {sch}.basico IS 'Cuestionario básico completo por radio, tabla de Poblaciones (poblaciones.org) importada en la base posadas (censo_2010.nbi_2010 y poblacion_2010), tal cual. Las columnas usadas en indicadores están verificadas contra el censo crudo en los 3.710 radios censados completos (ver README)'")
        c(f"COMMENT ON COLUMN {sch}.indicadores.hogares IS 'Hogares (tabla básica completa, h_total)'")
        c(f"COMMENT ON COLUMN {sch}.indicadores.personas_en_hogares IS 'Población en hogares (tabla básica completa, p_total)'")


def verify(anio: int) -> None:
    """Totales por provincia y por radio contra los oficiales."""
    sch = f'censo_{anio}'
    pm = SPECS[anio]['persona_muestra']
    conn = connect(DB)
    with conn.cursor() as cur:
        cur.execute(f"""
            WITH s AS (SELECT c.redcode, sum(c.n) AS n FROM {sch}.categorias c JOIN {sch}.variables v USING (variable_id)
                       WHERE v.codigo = '{pm}' GROUP BY 1)
            SELECT t.territorio, count(*) AS radios, count(r.geom) AS con_geom, count(s.redcode) AS con_datos,
                   sum(r.personas_oficial)::bigint AS oficial,
                   sum(s.n * coalesce(r.factor_muestra, 1))::bigint AS expandido,
                   count(*) FILTER (WHERE s.n IS NOT NULL AND r.personas_oficial IS NOT NULL
                                     AND abs(s.n * coalesce(r.factor_muestra, 1) - r.personas_oficial) < 0.5) AS iguales,
                   count(*) FILTER (WHERE s.n IS NOT NULL AND r.personas_oficial IS NOT NULL) AS comparables,
                   count(*) FILTER (WHERE s.n IS NOT NULL AND r.geom IS NULL) AS datos_sin_geom,
                   sum(s.n) FILTER (WHERE r.geom IS NULL)::bigint AS personas_sin_geom
            FROM {sch}.radios r LEFT JOIN s USING (redcode) LEFT JOIN territorios t USING (territorio_id)
            GROUP BY 1 ORDER BY 1""")
        log(f'  verificación {anio}: territorio | radios | con geometría | con datos | personas oficiales | '
            'expandidas | radios iguales/comparables | con datos sin geometría (personas)')
        for r in cur.fetchall():
            log(f'    {r[0]:11} {r[1]:6,} {r[2]:6,} {r[3]:6,} {r[4] or 0:>10,} {r[5] or 0:>10,} '
                f'{r[6]:,}/{r[7]:,}  {r[8]:,} ({r[9] or 0:,})')
        cur.execute(f"""SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = '{sch}' AND relkind IN ('r','m')""")
        log(f'  esquema {sch}:', cur.fetchone()[0])
    conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--anio', type=int, choices=sorted(SPECS), action='append',
                    help='año a cargar (se puede repetir); por defecto los cuatro')
    ap.add_argument('--refresh', action='store_true', help='volver a bajar las fuentes')
    args = ap.parse_args()
    for anio in args.anio or sorted(SPECS):
        t0 = time.time()
        log(f'censo {anio}')
        fetch_source(anio, args.refresh)
        con = build_duck(anio)
        for t in ('variables', 'categorias', 'oficial', 'indicadores'):
            log(f'  {t}: {con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]:,}')
        load_pg(con, anio)
        verify(anio)
        log(f'  listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
