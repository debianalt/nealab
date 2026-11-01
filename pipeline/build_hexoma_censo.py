"""Hexoma — Censo 2022 completo por radio censal (NEA argentino), unido a cada hexágono.

Fuente: el censo 2022 del INDEC ya agregado por radio (radio × variable × categoría ×
conteo, sacado de la base REDATAM del INDEC), publicado en Source Cooperative con
licencia CC-BY-4.0: https://source.coop/nlebovits/censo-argentino (colección 2022,
census-data.parquet). Son 86 variables de personas, hogares y viviendas particulares.
Al ser frecuencias por radio no hay cruces (sexo × edad, por ejemplo) ni las variables
de la base PO (pueblos originarios, afrodescendientes); NBI, sus cinco criterios e IPMH
vienen ya calculados por el INDEC. La población es la de viviendas particulares (las
colectivas y la situación de calle no están en esta base).

Todo queda en el esquema `censo`, aparte de las tablas que reflejan la web (v_hexoma no
se toca):
    censo.variables, censo.diccionario   qué es cada variable y cada categoría
    censo.categorias                     TODO el censo: conteo por (radio, variable, categoría)
    censo.indicadores                    ~80 indicadores por radio, en conteos (num y den)
    censo.radios                         radio: departamento, tipo, localidad, área, geometría
    censo.cruce                          celda → radio, ponderado por edificios
    censo.v_categorias                   categorias con sus rótulos
    censo.v_tasas_radio                  tasas (%) y promedios por radio
    censo.v_h3 (materializada)           conteos y tasas por celda: Σ num·peso / Σ den·peso

El cruce celda → radio es el mismo de las capas censales de la web
(pipeline/output/**/h3_radio_crosswalk*.parquet, build_dasymetric_crosswalk.py):
peso = parte de los edificios del radio que cae en la celda (suma 1 por radio).

Idempotente: rearma el esquema `censo` entero en una transacción. build_hexoma.py
--reset no lo toca.

Uso (desde neahub/):
    python pipeline/build_hexoma_censo.py
    python pipeline/build_hexoma_censo.py --refresh     # vuelve a bajar la fuente
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_hexoma import DB, connect, log  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / 'pipeline' / 'output'
CACHE = OUTPUT / 'hexoma' / 'censo2022'
TMP = OUTPUT / 'hexoma' / 'tmp'
SRC = 'https://data.source.coop/nlebovits/censo-argentino/2022'

# código INDEC de provincia → territorio del Hexoma
PROV = {54: 'misiones', 18: 'corrientes', 22: 'chaco', 34: 'formosa'}
CROSSWALKS = {
    'misiones': OUTPUT / 'h3_radio_crosswalk.parquet',
    'corrientes': OUTPUT / 'corrientes' / 'h3_radio_crosswalk_corrientes.parquet',
    'chaco': OUTPUT / 'chaco' / 'h3_radio_crosswalk_chaco.parquet',
    'formosa': OUTPUT / 'formosa' / 'h3_radio_crosswalk_formosa.parquet',
}
AREAL = {t: p.with_name(p.stem + '_areal.parquet') for t, p in CROSSWALKS.items()}
RADIOS = {
    'misiones': OUTPUT / 'radios_misiones.parquet',
    'corrientes': OUTPUT / 'corrientes' / 'radios_corrientes.parquet',
    'chaco': OUTPUT / 'chaco' / 'radios_chaco.parquet',
    'formosa': OUTPUT / 'formosa' / 'radios_formosa.parquet',
}

# ── indicadores ───────────────────────────────────────────────────────────────
# (nombre, variable, categorías del numerador, categorías excluidas del denominador, rótulo)
# Denominador = todas las categorías de la variable en el radio salvo las excluidas
# (Ignorado, No corresponde…). Cada variable tiene su universo (CONDACT: personas de
# 14 años y más; H23: hogares en vivienda propia), y el rótulo lo dice.
PCT: list[tuple[str, str, list[int], list[int], str]] = [
    # población
    ('mujeres', 'PERSONA_P02', [1], [], 'Mujeres / población'),
    ('menores_15', 'PERSONA_EDADGRU', [1], [], 'Personas de 0 a 14 años / población'),
    ('mayores_65', 'PERSONA_EDADGRU', [3], [], 'Personas de 65 años y más / población'),
    ('nacidos_otra_provincia', 'PERSONA_P13', [2], [], 'Nacidos en otra provincia / población'),
    ('nacidos_otro_pais', 'PERSONA_P13', [3], [], 'Nacidos en otro país / población'),
    ('migrante_reciente', 'PERSONA_P17', [3, 4], [5],
     'Vivían hace 5 años en otra provincia u otro país / población de 5 años y más'),
    # NBI e IPMH (calculados por el INDEC)
    ('nbi', 'HOGAR_NBI_TOT', [1], [], 'Hogares con NBI (al menos un criterio) / hogares'),
    ('nbi_vivienda', 'HOGAR_NBI_VIV', [1], [], 'NBI vivienda de tipo inconveniente / hogares'),
    ('nbi_hacinamiento', 'HOGAR_NBI_HAC', [1], [], 'NBI hacinamiento (más de 3 personas por cuarto) / hogares'),
    ('nbi_sanitario', 'HOGAR_NBI_SAN', [1], [], 'NBI condiciones sanitarias / hogares'),
    ('nbi_escolaridad', 'HOGAR_NBI_ESC', [1], [], 'NBI escolaridad (niños de 6 a 12 que no asisten) / hogares'),
    ('nbi_subsistencia', 'HOGAR_NBI_SUB', [1], [], 'NBI capacidad de subsistencia / hogares'),
    ('ipmh_privacion', 'HOGAR_IPMH', [2, 3, 4], [], 'Hogares con alguna privación material (IPMH) / hogares'),
    ('ipmh_corrientes', 'HOGAR_IPMH', [2], [], 'Privación sólo de recursos corrientes (IPMH) / hogares'),
    ('ipmh_patrimoniales', 'HOGAR_IPMH', [3], [], 'Privación sólo patrimonial (IPMH) / hogares'),
    ('ipmh_convergente', 'HOGAR_IPMH', [4], [], 'Privación convergente (IPMH) / hogares'),
    # vivienda y servicios (hogares)
    ('piso_tierra', 'HOGAR_H10', [3], [], 'Piso de tierra o ladrillo suelto / hogares'),
    ('piso_sin_revestimiento', 'HOGAR_H10', [2, 3, 4], [],
     'Piso de carpeta, contrapiso, ladrillo, tierra u otro (sin cerámica, baldosa, madera…) / hogares'),
    ('techo_precario', 'HOGAR_H11', [7, 8], [99], 'Techo de chapa de cartón, caña, palma, paja o similar / hogares con dato'),
    ('sin_cielorraso', 'HOGAR_H11', [2, 4, 6, 8, 10], [99], 'Techo sin cielorraso / hogares con dato'),
    ('materiales_calidad_iii_iv', 'HOGAR_INMAT', [3, 4], [9], 'Calidad de materiales III o IV / hogares con dato'),
    ('materiales_calidad_iv', 'HOGAR_INMAT', [4], [9], 'Calidad de materiales IV / hogares con dato'),
    ('agua_red', 'HOGAR_H14', [1], [], 'Agua para beber y cocinar de red pública / hogares'),
    ('agua_precaria', 'HOGAR_H14', [4, 5, 6], [],
     'Agua de pozo sin bomba, cisterna, lluvia, río u otra procedencia / hogares'),
    ('agua_fuera_vivienda', 'HOGAR_H13', [3], [], 'Agua fuera de la vivienda (dentro del terreno) / hogares'),
    ('cloaca', 'HOGAR_H15_17_18', [1], [], 'Inodoro con descarga y desagüe a red pública / hogares'),
    ('sin_inodoro_descarga', 'HOGAR_H15_17_18', [4], [], 'Inodoro sin descarga, o sin inodoro o baño / hogares'),
    ('sin_bano', 'HOGAR_H15_17', [3], [], 'Sin baño o baño sin inodoro / hogares'),
    ('cocina_lena_carbon', 'HOGAR_H19', [5], [], 'Cocina principalmente con leña o carbón / hogares'),
    ('cocina_garrafa', 'HOGAR_H19', [4], [], 'Cocina principalmente con gas en garrafa / hogares'),
    ('cocina_gas_red', 'HOGAR_H19', [2], [], 'Cocina principalmente con gas de red / hogares'),
    ('cocina_tubo_granel', 'HOGAR_H19', [3], [], 'Cocina con gas en tubo o a granel / hogares'),
    ('cocina_electricidad', 'HOGAR_H19', [1], [], 'Cocina principalmente con electricidad / hogares'),
    ('hacinamiento', 'HOGAR_H20CP', [5, 6], [],
     '2 personas por cuarto o más (categorías INDEC «2,00–3,00» y «más de 3,00») / hogares'),
    ('hacinamiento_critico', 'HOGAR_H20CP', [6], [], 'Más de 3 personas por cuarto / hogares'),
    ('propietario', 'HOGAR_H22', [1], [], 'Vivienda propia / hogares'),
    ('inquilino', 'HOGAR_H22', [2], [], 'Vivienda alquilada / hogares'),
    ('tenencia_precaria', 'HOGAR_H22', [3, 4, 5], [], 'Vivienda cedida por trabajo, prestada u otra situación / hogares'),
    ('sin_documentacion', 'HOGAR_H23', [4], [], 'Sin documentación de la vivienda / hogares en vivienda propia'),
    ('internet', 'HOGAR_H24A', [1], [], 'Internet en la vivienda / hogares'),
    ('celular_internet', 'HOGAR_H24B', [1], [], 'Celular con internet / hogares'),
    ('computadora', 'HOGAR_H24C', [1], [], 'Computadora o tablet / hogares'),
    ('hogar_migrante', 'HOGAR_HOGMIG', [1], [], 'Hogares con al menos un migrante internacional / hogares'),
    ('clima_educativo_bajo', 'HOGAR_EDUHOG', [1, 2], [9], 'Clima educativo del hogar muy bajo o bajo / hogares con dato'),
    ('hogar_unipersonal', 'HOGAR_TIPHOGAR', [1], [], 'Hogares unipersonales / hogares'),
    ('hogar_monoparental', 'HOGAR_TIPHOGAR', [4, 7, 11], [], 'Hogares nucleares incompletos (con o sin otros) / hogares'),
    # viviendas
    ('rancho_casilla', 'VIVIENDA_V01', [2, 3], [], 'Ranchos y casillas / viviendas particulares'),
    ('viviendas_desocupadas', 'VIVIENDA_V02', [2], [], 'Viviendas sin personas presentes / viviendas particulares'),
    ('rural_agrupada', 'VIVIENDA_URP', [2], [], 'Viviendas en área rural agrupada / viviendas particulares'),
    ('rural_dispersa', 'VIVIENDA_URP', [3], [], 'Viviendas en área rural dispersa / viviendas particulares'),
    # educación (personas: el universo de MNI incluye a los niños)
    ('sin_instruccion', 'PERSONA_MNI', [1], [99], 'Sin instrucción / personas con dato (incluye niños)'),
    ('hasta_primaria_incompleta', 'PERSONA_MNI', [1, 2], [99],
     'Sin instrucción o primario incompleto / personas con dato (incluye niños)'),
    ('secundario_completo_o_mas', 'PERSONA_MNI', [5, 6, 7, 8, 9, 10, 11], [99],
     'Secundario completo o más / personas con dato (incluye niños)'),
    ('universitario_completo', 'PERSONA_MNI', [9, 10, 11], [99],
     'Universitario o posgrado completo / personas con dato (incluye niños)'),
    ('asiste', 'PERSONA_CONASI', [1], [], 'Asiste a un establecimiento educativo / población'),
    ('nunca_asistio', 'PERSONA_CONASI', [3], [], 'Nunca asistió / población'),
    # trabajo
    ('tasa_actividad', 'PERSONA_CONDACT', [1, 2], [], 'Ocupados y desocupados / población de 14 años y más'),
    ('tasa_empleo', 'PERSONA_CONDACT', [1], [], 'Ocupados / población de 14 años y más'),
    ('tasa_desocupacion', 'PERSONA_CONDACT', [2], [3], 'Desocupados / población activa (ocupados + desocupados)'),
    ('informalidad', 'PERSONA_DESCAPOR', [3], [9], 'No le descuentan ni aporta jubilación / ocupados con dato'),
    ('cat_asalariado', 'PERSONA_P30', [2], [9], 'Empleados u obreros / ocupados con dato'),
    ('cat_cuenta_propia', 'PERSONA_P30', [3], [9], 'Cuenta propia / ocupados con dato'),
    ('cat_patron', 'PERSONA_P30', [4], [9], 'Patrones o empleadores / ocupados con dato'),
    ('cat_servicio_domestico', 'PERSONA_P30', [1], [9], 'Servicio doméstico / ocupados con dato'),
    ('cat_familiar', 'PERSONA_P30', [5], [9], 'Trabajadores familiares / ocupados con dato'),
    ('rama_agro', 'PERSONA_LETRA', [1], [22, 23], 'Agricultura, ganadería, silvicultura y pesca / ocupados con rama codificada'),
    ('rama_industria', 'PERSONA_LETRA', [3], [22, 23], 'Industria manufacturera / ocupados con rama codificada'),
    ('rama_construccion', 'PERSONA_LETRA', [6], [22, 23], 'Construcción / ocupados con rama codificada'),
    ('rama_comercio', 'PERSONA_LETRA', [7], [22, 23], 'Comercio / ocupados con rama codificada'),
    ('rama_adm_publica', 'PERSONA_LETRA', [15], [22, 23], 'Administración pública / ocupados con rama codificada'),
    ('rama_ensenanza', 'PERSONA_LETRA', [16], [22, 23], 'Enseñanza / ocupados con rama codificada'),
    ('rama_salud', 'PERSONA_LETRA', [17], [22, 23], 'Salud y servicios sociales / ocupados con rama codificada'),
    ('rama_servicio_domestico', 'PERSONA_LETRA', [20], [22, 23], 'Hogares como empleadores / ocupados con rama codificada'),
    # protección social
    ('jubilacion_pension', 'PERSONA_P20', [1], [], 'Cobra jubilación o pensión / población'),
    ('cobertura_obra_social', 'PERSONA_P19', [1], [], 'Obra social o prepaga (incluye PAMI) / población'),
    ('solo_plan_estatal', 'PERSONA_P19', [2], [], 'Sólo programas o planes estatales de salud / población'),
    ('sin_cobertura', 'PERSONA_P19', [3], [], 'Sin obra social, prepaga ni plan estatal / población'),
]
# Promedios: Σ categoría × conteo / Σ conteo (la categoría es el número).
MEAN: list[tuple[str, str, list[int], str]] = [
    ('personas_por_hogar', 'HOGAR_TOTPOBH', [], 'Personas por hogar (promedio)'),
    ('hijos_por_mujer', 'PERSONA_P35', [], 'Hijos nacidos vivos por mujer (promedio, mujeres de 14 años y más)'),
    ('anios_escolaridad', 'PERSONA_AESC', [99], 'Años de escolaridad completos (promedio, personas con dato)'),
]
# Totales del radio (sólo conteo).
TOTAL: list[tuple[str, str, str]] = [
    ('personas', 'PERSONA_P02', 'Población en viviendas particulares'),
    ('hogares', 'HOGAR_NBI_TOT', 'Hogares'),
    ('viviendas', 'VIVIENDA_TIPOVIVG', 'Viviendas particulares'),
    ('viviendas_ocupadas', 'VIVIENDA_V02', 'Viviendas particulares con personas presentes'),  # filtro abajo
]


def ints(xs: list[int]) -> str:
    return ', '.join(str(x) for x in xs) or 'NULL'


def fetch_source(refresh: bool) -> tuple[Path, Path]:
    CACHE.mkdir(parents=True, exist_ok=True)
    data, meta = CACHE / 'census_nea.parquet', CACHE / 'metadata.parquet'
    if refresh or not data.exists() or not meta.exists():
        con = duckdb.connect()
        con.execute('INSTALL httpfs; LOAD httpfs')
        log(f'bajando {SRC}/census-data.parquet (NEA) …')
        con.execute(f"""COPY (SELECT * FROM '{SRC}/census-data.parquet'
                              WHERE prov_code IN ({ints(list(PROV))}))
                        TO '{data.as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
        con.execute(f"COPY (SELECT * FROM '{SRC}/metadata.parquet') TO '{meta.as_posix()}' (FORMAT parquet)")
    return data, meta


def build_duck(data: Path, meta: Path) -> duckdb.DuckDBPyConnection:
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
    # radio: identidad y la categoría única (o mayoritaria) de variables geográficas
    con.execute("""
        CREATE TABLE radios_attr AS
        WITH g AS (SELECT DISTINCT redcode, territorio, depto_cod, departamento, fraccion, radio FROM raw),
        moda AS (
            SELECT redcode, codigo, arg_max(etiqueta, n) AS etiqueta
            FROM (SELECT redcode, codigo, etiqueta, sum(n) AS n FROM raw
                  WHERE codigo IN ('VIVIENDA_TIPO_RADIO', 'VIVIENDA_CODLOC', 'VIVIENDA_CODGL',
                                   'VIVIENDA_CATGL', 'VIVIENDA_CODAGLO')
                  GROUP BY ALL)
            GROUP BY 1, 2)
        SELECT g.*,
            max(etiqueta) FILTER (WHERE codigo = 'VIVIENDA_TIPO_RADIO') AS tipo_radio,
            max(etiqueta) FILTER (WHERE codigo = 'VIVIENDA_CODLOC')     AS localidad,
            max(etiqueta) FILTER (WHERE codigo = 'VIVIENDA_CODGL')      AS gobierno_local,
            max(etiqueta) FILTER (WHERE codigo = 'VIVIENDA_CATGL')      AS categoria_gobierno_local,
            max(etiqueta) FILTER (WHERE codigo = 'VIVIENDA_CODAGLO')    AS aglomerado
        FROM g LEFT JOIN moda USING (redcode)
        GROUP BY ALL
    """)
    # indicadores en conteos
    cols = []
    for name, var, _ in TOTAL:
        cond = "AND cat_int = 1" if name == 'viviendas_ocupadas' else ''
        cols.append(f"sum(n) FILTER (WHERE codigo = '{var}' {cond})::INTEGER AS {name}")
    for name, var, num, excl, _ in PCT:
        cols.append(f"sum(n) FILTER (WHERE codigo = '{var}' AND cat_int IN ({ints(num)}))::INTEGER AS {name}_num")
        cols.append(f"sum(n) FILTER (WHERE codigo = '{var}' AND cat_int NOT IN ({ints(excl)}))::INTEGER AS {name}_den"
                    if excl else f"sum(n) FILTER (WHERE codigo = '{var}')::INTEGER AS {name}_den")
    for name, var, excl, _ in MEAN:
        ex = f"AND cat_int NOT IN ({ints(excl)})" if excl else ''
        cols.append(f"sum(n::BIGINT * cat_int::BIGINT) FILTER (WHERE codigo = '{var}' {ex})::BIGINT AS {name}_num")
        cols.append(f"sum(n) FILTER (WHERE codigo = '{var}' {ex})::INTEGER AS {name}_den")
    con.execute(f"CREATE TABLE indicadores AS SELECT redcode, {', '.join(cols)} FROM raw GROUP BY redcode")
    # cruce y polígonos
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
    con.execute('CREATE TABLE poligonos AS ' + ' UNION ALL '.join(
        f"SELECT redcode::INTEGER AS redcode, hex(geometry) AS wkb FROM '{p.as_posix()}'"
        for p in RADIOS.values()))
    return con


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


def load_pg(con) -> None:
    conn = connect(DB)
    t0 = time.time()
    with conn, conn.cursor() as cur:
        cur.execute("SET client_min_messages = warning")
        cur.execute("DROP SCHEMA IF EXISTS censo CASCADE")
        cur.execute("CREATE SCHEMA censo")
        cur.execute("COMMENT ON SCHEMA censo IS 'Censo 2022 (INDEC) por radio censal, NEA argentino, "
                    "unido a cada hexágono por el cruce ponderado por edificios. Fuente: source.coop/nlebovits/censo-argentino (CC-BY-4.0).'")

        cur.execute("""CREATE TABLE censo.variables (variable_id smallint PRIMARY KEY, codigo text UNIQUE,
                       nombre text, etiqueta text, entidad text)""")
        log('  variables', copy_table(con, cur, 'SELECT variable_id, codigo, nombre, etiqueta, entidad FROM variables',
                                      'censo.variables', ['variable_id', 'codigo', 'nombre', 'etiqueta', 'entidad']))
        cur.execute("""CREATE TABLE censo.diccionario (variable_id smallint REFERENCES censo.variables,
                       categoria text, etiqueta text, PRIMARY KEY (variable_id, categoria))""")
        log('  diccionario', copy_table(con, cur, 'SELECT * FROM diccionario', 'censo.diccionario',
                                        ['variable_id', 'categoria', 'etiqueta']))

        cur.execute("""CREATE TABLE censo.radios (
            redcode integer PRIMARY KEY, territorio_id smallint REFERENCES territorios,
            depto_cod text, departamento text, fraccion text, radio text,
            tipo_radio text, localidad text, gobierno_local text, categoria_gobierno_local text, aglomerado text,
            area_km2 double precision, geom geometry(MultiPolygon, 4326), wkb_hex text)""")
        rcols = ['redcode', 'depto_cod', 'departamento', 'fraccion', 'radio', 'tipo_radio', 'localidad',
                 'gobierno_local', 'categoria_gobierno_local', 'aglomerado', 'wkb_hex']
        cur.execute("CREATE TEMP TABLE radios_in (territorio text, "
                    + ', '.join(f'{c} {"integer" if c == "redcode" else "text"}' for c in rcols) + ')')
        log('  radios', copy_table(con, cur, """
            SELECT a.territorio, a.redcode, a.depto_cod, a.departamento, a.fraccion, a.radio, a.tipo_radio,
                   a.localidad, a.gobierno_local, a.categoria_gobierno_local, a.aglomerado, p.wkb
            FROM radios_attr a LEFT JOIN (SELECT redcode, any_value(wkb) AS wkb FROM poligonos GROUP BY 1) p USING (redcode)""",
            'radios_in', ['territorio'] + rcols))
        cur.execute(f"""INSERT INTO censo.radios (territorio_id, {', '.join(rcols)})
                        SELECT t.territorio_id, {', '.join('r.' + c for c in rcols)}
                        FROM radios_in r JOIN territorios t USING (territorio)""")
        cur.execute("""
            UPDATE censo.radios SET geom = ST_Multi(ST_CollectionExtract(ST_MakeValid(
                       ST_SetSRID(ST_GeomFromWKB(decode(wkb_hex, 'hex')), 4326)), 3))
            WHERE wkb_hex IS NOT NULL;
            UPDATE censo.radios SET area_km2 = ST_Area(geom::geography) / 1e6;
            ALTER TABLE censo.radios DROP COLUMN wkb_hex;
            CREATE INDEX ON censo.radios USING gist (geom);
        """)

        num_cols = [c for c in con.execute("SELECT * FROM indicadores LIMIT 0").fetchdf().columns]
        types = {c: ('bigint' if c.endswith('_num') and any(c == f'{m[0]}_num' for m in MEAN) else 'integer')
                 for c in num_cols if c != 'redcode'}
        cur.execute("CREATE TABLE censo.indicadores (redcode integer PRIMARY KEY REFERENCES censo.radios, "
                    + ', '.join(f'{c} {t}' for c, t in types.items()) + ')')
        log('  indicadores', copy_table(con, cur, 'SELECT * FROM indicadores', 'censo.indicadores', num_cols))

        cur.execute("""CREATE TABLE censo.categorias (redcode integer REFERENCES censo.radios,
                       variable_id smallint REFERENCES censo.variables, categoria text, n integer NOT NULL,
                       PRIMARY KEY (redcode, variable_id, categoria))""")
        log('  categorias', copy_table(con, cur, 'SELECT * FROM categorias', 'censo.categorias',
                                       ['redcode', 'variable_id', 'categoria', 'n']))
        cur.execute("CREATE INDEX ON censo.categorias (variable_id, categoria)")

        cur.execute("""CREATE TABLE censo.cruce (h3 h3index NOT NULL, territorio_id smallint NOT NULL REFERENCES territorios,
                       redcode integer NOT NULL, peso double precision NOT NULL, n_edificios integer,
                       est_personas double precision, est_hogares double precision, fuente text NOT NULL,
                       PRIMARY KEY (h3, territorio_id, redcode))""")
        cur.execute("CREATE TEMP TABLE cruce_in (h3 text, territorio text, redcode integer, peso double precision, "
                    "n_edificios integer, est_personas double precision, est_hogares double precision, fuente text)")
        log('  cruce', copy_table(con, cur, 'SELECT * FROM cruce', 'cruce_in',
                                  ['h3', 'territorio', 'redcode', 'peso', 'n_edificios', 'est_personas', 'est_hogares', 'fuente']))
        cur.execute("""INSERT INTO censo.cruce
                       SELECT c.h3::h3index, t.territorio_id, c.redcode, c.peso, c.n_edificios, c.est_personas, c.est_hogares, c.fuente
                       FROM cruce_in c JOIN territorios t USING (territorio)""")
        cur.execute("CREATE INDEX ON censo.cruce (redcode)")
        log(f'  cargado en {time.time() - t0:.0f} s; vistas …')
        create_views(cur)
        comments(cur)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('ANALYZE censo.radios; ANALYZE censo.indicadores; ANALYZE censo.categorias; ANALYZE censo.cruce; ANALYZE censo.v_h3')
    conn.close()


def create_views(cur) -> None:
    cur.execute("""CREATE VIEW censo.v_categorias AS
        SELECT c.redcode, r.territorio_id, r.departamento, v.codigo, v.etiqueta AS variable, v.entidad,
               c.categoria, d.etiqueta AS categoria_etiqueta, c.n
        FROM censo.categorias c JOIN censo.variables v USING (variable_id)
        JOIN censo.diccionario d USING (variable_id, categoria) JOIN censo.radios r USING (redcode)""")
    tasas = [f"round((100.0 * i.{n}_num / nullif(i.{n}_den, 0))::numeric, 2) AS {n}" for n, *_ in PCT]
    medias = [f"round((i.{n}_num::numeric / nullif(i.{n}_den, 0)), 3) AS {n}" for n, *_ in MEAN]
    totales = [f"i.{n}" for n, *_ in TOTAL]
    cur.execute(f"""CREATE VIEW censo.v_tasas_radio AS
        SELECT i.redcode, r.territorio_id, t.territorio, r.departamento, r.tipo_radio, r.area_km2,
               round((i.personas / nullif(r.area_km2, 0))::numeric, 1) AS densidad_hab_km2,
               {', '.join(totales + tasas + medias)}
        FROM censo.indicadores i JOIN censo.radios r USING (redcode) JOIN territorios t USING (territorio_id)""")
    # por celda: conteos repartidos por el peso; tasas = Σ num·peso / Σ den·peso
    # real (float4): 7 cifras alcanzan para conteos por celda y la vista ocupa la mitad
    sums = [f"sum(i.{n} * c.peso)::real AS {n}" for n, *_ in TOTAL]
    for n, *_ in PCT + MEAN:
        sums += [f"sum(i.{n}_num * c.peso)::real AS {n}_num", f"sum(i.{n}_den * c.peso)::real AS {n}_den"]
    rates = [f"round((100.0 * {n}_num / nullif({n}_den, 0))::numeric, 2) AS {n}" for n, *_ in PCT]
    rates += [f"round(({n}_num / nullif({n}_den, 0))::numeric, 3) AS {n}" for n, *_ in MEAN]
    cur.execute(f"""CREATE MATERIALIZED VIEW censo.v_h3 AS
        WITH s AS (
            SELECT c.h3, c.territorio_id, count(*)::smallint AS n_radios,
                   (array_agg(c.redcode ORDER BY c.peso * coalesce(i.hogares, 0) DESC))[1] AS redcode,
                   {', '.join(sums)}
            FROM censo.cruce c JOIN censo.indicadores i USING (redcode)
            GROUP BY 1, 2)
        SELECT s.h3, s.territorio_id, t.territorio, s.redcode, s.n_radios,
               {', '.join(f's.{n}' for n, *_ in TOTAL)}, {', '.join(rates)},
               {', '.join(f's.{n}_num, s.{n}_den' for n, *_ in PCT + MEAN)}
        FROM s JOIN territorios t USING (territorio_id)""")
    cur.execute("CREATE UNIQUE INDEX ON censo.v_h3 (h3, territorio_id)")
    cur.execute("CREATE INDEX ON censo.v_h3 (h3, territorio)")


def comments(cur) -> None:
    c = cur.execute
    c("COMMENT ON TABLE censo.categorias IS 'Todo el censo 2022: personas, hogares o viviendas particulares de cada categoría de cada variable, por radio'")
    c("COMMENT ON TABLE censo.indicadores IS 'Indicadores por radio en conteos: <x>_num y <x>_den (las tasas se calculan sumando conteos: radio, celda o departamento)'")
    c("COMMENT ON TABLE censo.cruce IS 'Celda → radio. peso = edificios del radio en la celda / edificios del radio (suma 1 por radio). De pipeline/output/**/h3_radio_crosswalk*.parquet; fuente = area: respaldo por superficie para radios con población sin edificios'")
    c("COMMENT ON TABLE censo.radios IS 'Radios censales 2022 del NEA argentino. tipo_radio, localidad, gobierno local y aglomerado = la categoría de la mayoría de las viviendas del radio'")
    c("COMMENT ON VIEW censo.v_tasas_radio IS 'Tasas (%) y promedios por radio, desde censo.indicadores'")
    c("COMMENT ON MATERIALIZED VIEW censo.v_h3 IS 'Censo por celda: conteos × peso; tasas = Σ num·peso / Σ den·peso. redcode = radio que aporta más hogares a la celda. Unible a v_hexoma por (h3, territorio)'")
    for n, var, label in TOTAL:
        for obj in ('censo.indicadores', 'censo.v_tasas_radio', 'censo.v_h3'):
            c(f"COMMENT ON COLUMN {obj}.{n} IS {q(label + ' (' + var + ')')}")
    for n, var, num, excl, label in PCT:
        spec = f'{label} — {var}: num {num}, den todas' + (f' salvo {excl}' if excl else '')
        c(f"COMMENT ON COLUMN censo.indicadores.{n}_num IS {q('Numerador: ' + spec)}")
        c(f"COMMENT ON COLUMN censo.indicadores.{n}_den IS {q('Denominador: ' + spec)}")
        c(f"COMMENT ON COLUMN censo.v_tasas_radio.{n} IS {q('% ' + spec)}")
        c(f"COMMENT ON COLUMN censo.v_h3.{n} IS {q('% ' + spec)}")
    for n, var, excl, label in MEAN:
        spec = f'{label} — {var}' + (f', sin {excl}' if excl else '')
        c(f"COMMENT ON COLUMN censo.v_tasas_radio.{n} IS {q(spec)}")
        c(f"COMMENT ON COLUMN censo.v_h3.{n} IS {q(spec)}")


def report() -> None:
    conn = connect(DB)
    with conn.cursor() as cur:
        cur.execute("""SELECT relname, pg_size_pretty(pg_total_relation_size(c.oid)), reltuples::bigint
                       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                       WHERE n.nspname = 'censo' AND relkind IN ('r', 'm') ORDER BY 1""")
        for r in cur.fetchall():
            log(f'  {r[0]:20} {r[1]:>10} {r[2]:>10,} filas')
        cur.execute("SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'censo' AND relkind IN ('r','m')")
        log('  esquema censo:', cur.fetchone()[0])
    conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--refresh', action='store_true', help='volver a bajar la fuente')
    args = ap.parse_args()
    t0 = time.time()
    data, meta = fetch_source(args.refresh)
    con = build_duck(data, meta)
    for t in ('variables', 'diccionario', 'categorias', 'indicadores', 'radios_attr', 'cruce', 'poligonos'):
        log(f'  {t}: {con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]:,}')
    load_pg(con)
    report()
    log(f'listo en {time.time() - t0:.0f} s')


if __name__ == '__main__':
    main()
