"""Hexoma — esquema `meta`: catálogo metodológico de fuentes, procesos y columnas.

Carga pipeline/hexoma/metodologia.py (la fuente de verdad, versionada) y arma:

    meta.fuentes     una ficha por fuente: proveedor, producto y versión, acceso, licencia, cita y DOI, tipo de
                     dato (medición, estimación satelital, reanálisis, modelo, censo, derivado), resoluciones,
                     período, procesamiento paso a paso, asignación a la celda, validación, limitaciones, usos
    meta.procesos    una ficha por proceso propio del Hexoma: método, parámetros, validación y regla de elección
    meta.objetos     cada tabla o vista del Hexoma con sus fuentes y procesos (por los patrones `objetos`)
    meta.variables   una fila por columna de todo el Hexoma: tipo, COMMENT, fuentes y procesos; marca las
                     columnas sin comentario y las tablas sin fuente ni proceso
    meta.v_ficha     una fila por columna con el texto de su fuente al lado, para consultar de un vistazo

Idempotente. Uso (desde neahub/):
    python pipeline/hexoma/build_hexoma_meta.py
"""
from __future__ import annotations

import fnmatch
import importlib
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_hexoma import DB, connect, log  # noqa: E402

CAMPOS_F = ['id', 'nombre', 'proveedor', 'producto', 'version', 'acceso', 'licencia', 'cita', 'doi', 'tipo_dato',
            'res_espacial', 'res_temporal', 'periodo', 'cobertura', 'variables', 'procesamiento', 'asignacion_celda',
            'validacion', 'limitaciones', 'uso_recomendado', 'uso_no_recomendado', 'scripts', 'verificado']
CAMPOS_P = ['id', 'nombre', 'metodo', 'parametros', 'validacion', 'regla', 'limitaciones', 'scripts', 'verificado']
# vistas y objetos que no nombran las fichas
EXTRA = {
    'clima.v_h3_*': (['chirps_v3', 'era5_land'], ['hex_pixel']),
    'clima.v_h3_*_corr': ([], ['correccion_temperatura']),
    'public.v_hexoma': ([], ['carga_web']),
    'public.v_hexoma_rangos': ([], ['distribuciones_rangos']),
}
EXCLUIR = {('public', 'spatial_ref_sys'), ('public', 'geography_columns'), ('public', 'geometry_columns'),
           ('public', 'raster_columns'), ('public', 'raster_overviews')}
# vistas cuyas columnas heredan el COMMENT de sus tablas de origen (en este orden)
HEREDA = {
    ('clima', 'v_h3_campania'): ['era5_campania', 'chirps_campania', 'balance_campania'],
    ('clima', 'v_h3_estacion'): ['era5_estacion', 'chirps_estacion', 'balance_estacion'],
    ('clima', 'v_h3_helada'): ['era5_helada'],
    ('clima', 'v_h3_campania_corr'): ['era5_campania_corr'],
    ('clima', 'v_h3_helada_corr'): ['era5_helada_corr'],
    ('clima', 'v_h3_anomalia'): ['anomalia'],
    ('estaciones', 'v_diario_valido'): ['diario'],
}
# claves y columnas genéricas que se repiten en todo el Hexoma
GENERICAS = {
    'h3': 'celda H3 resolución 9 (tipo h3index de h3-pg)',
    'territorio_id': 'territorio (provincia en el NEA argentino) según public.territorios',
    'territorio': 'nombre corto del territorio',
    'redcode': 'código del radio censal (o del departamento, 5 dígitos, en hexagonos y limites)',
    'departamento': 'nombre del departamento',
    'depto_id': 'archivo de departamento de la web del que viene la fila (public.departamentos)',
    'depto_cod': 'código del departamento',
    'principal': 'verdadero en la fila que abre la ficha cuando una celda está en dos archivos de departamento',
    'anio': 'año (calendario, o año censal en los esquemas censales)',
    'campania': 'campaña agrícola julio–junio, por su año de inicio (2023 = jul-2023 a jun-2024)',
    'anio_estacion': 'año de la estación meteorológica (DEF: el año de diciembre)',
    'estacion': 'estación meteorológica: DEF, MAM, JJA o SON (en tablas de clima); en otras, nombre de estación',
    'n_dias': 'días con dato en el período agregado',
    'n_nulos': 'días sin dato en el período agregado',
    'pixel_id': 'píxel de la fuente: fila_global × 100000 + columna_global en la grilla nativa',
    'unidad_id': 'píxel (pixel_id) o par CHIRPS×ERA5 (par_id), según la tabla de origen',
    'par_id': 'combinación de un píxel CHIRPS y uno ERA5-Land (clima.par)',
    'chirps_pixel': 'píxel de CHIRPS v3 que contiene el centro de la celda o la estación',
    'era5_pixel': 'píxel de ERA5-Land que contiene el centro de la celda o la estación',
    'estacion_id': 'estación meteorológica (estaciones.estacion): ghcn:<id> o inta:<id>',
    'geom': 'geometría (EPSG:4326)',
    'nombre': 'nombre', 'etiqueta': 'rótulo para mostrar', 'codigo': 'código de la categoría o variable',
    'variable': 'nombre de la variable', 'variable_id': 'identificador de la variable censal',
    'categoria': 'categoría de la variable censal', 'categoria_etiqueta': 'rótulo de la categoría censal',
    'n': 'cantidad de casos', 'media': 'media', 'mediana': 'mediana', 'p10': 'percentil 10', 'p90': 'percentil 90',
    'tabla': 'tabla de origen', 'fuente': 'fuente de origen', 'metodo': 'método usado', 'archivos': 'archivos de origen con bytes y sha256',
    'area_km2': 'superficie, km²', 'fraccion': 'fracción censal', 'radio': 'radio censal', 'localidad': 'localidad', 'municipio': 'municipio',
    'entidad': 'entidad del censo (vivienda, hogar o persona)', 'relevamiento': 'tipo de relevamiento del radio (completo, muestra, sólo básico)',
    'factor_muestra': 'personas oficiales / personas en la muestra (1 = censo completo)',
    'personas_oficial': 'personas según el total oficial del INDEC', 'viviendas_oficial': 'viviendas según el total oficial del INDEC',
    'n_muestra': 'personas en la base (muestra o completa)', 'n_expandido': 'conteo calibrado con factor_muestra',
    'pais': 'país', 'provincia': 'provincia', 'densidad_hab_km2': 'personas por km² del radio',
    'area': 'superficie del radio (unidad de la cartografía de origen)', 'aglomerado': 'aglomerado urbano del INDEC',
    'tipo_radio': 'tipo de radio (urbano, mixto, rural) según el INDEC', 'gobierno_local': 'gobierno local (municipio o comisión)',
    'categoria_gobierno_local': 'categoría del gobierno local', 'est_personas': 'personas estimadas en la celda por el cruce de la web',
    'est_hogares': 'hogares estimados en la celda por el cruce de la web', 'n_edificios': 'edificios en la celda (cruce de la web)',
    'capa': 'capa de la web', 'ambito': 'ámbito (departamento, provincia o región)', 'ambito_id': 'identificador del ámbito',
    'ambito_nombre': 'nombre del ámbito', 'unidad': 'unidad de la variable', 'parquet_key': 'clave del parquet por departamento en la web',
    'p25': 'percentil 25', 'p50': 'percentil 50', 'p75': 'percentil 75', 'min': 'mínimo', 'max': 'máximo',
    'calculado_en': 'fecha de cálculo', 'cargado_en': 'fecha de carga', 'version_v': 'versión ?v del archivo de la web',
    'n_archivos': 'archivos cargados', 'n_filas': 'filas cargadas', 'n_h3': 'celdas distintas', 'segundos': 'duración de la carga',
    'columnas': 'columnas cargadas', 'carga_id': 'identificador de la carga',
    # clima: anomalías, validación, corrección
    'periodo': 'campaña (año de inicio) o año calendario, según la tabla de origen',
    'valor': 'valor del período', 'normal': 'media 1991–2020 de la misma unidad', 'anom': 'valor − normal',
    'anom_pct': 'anomalía en % de la normal', 'pct_ref': '% de los 30 años de referencia (1991–2020) con valor estrictamente menor',
    'decision': 'resultado de la regla de elección por variable (JSON): MAE por método, mejora y método adoptado',
    'mes': 'mes calendario (1–12)', 'q': 'cuantiles (0,005…0,995)', 'delta': 'corrección en cada cuantil, °C',
    'metrica': 'métrica de validación', 'n_anios': 'años completos comparados', 'obs_media': 'media observada en la estación',
    'mod_media': 'media del producto grillado en el píxel de la estación', 'mae': 'error absoluto medio (producto − estación)',
    'sesgo': 'error medio (producto − estación)', 'valor_estacion': 'valor medio en la estación', 'valor_modelo': 'valor medio del producto en el píxel de la estación',
    'r': 'correlación de Pearson', 'asset': 'asset o URL de origen', 'pixel': 'tamaño del píxel', 'rotulo': 'rótulo de tipo de dato y advertencias',
    'cargado': 'fecha y hora de carga', 'resolucion': 'resolución espacial', 'fila': 'fila en la grilla global de la fuente',
    'col': 'columna en la grilla global de la fuente', 'lng': 'longitud (grados)', 'lat': 'latitud (grados)',
    # estaciones
    'fecha': 'fecha local de la observación', 'red': 'red de origen (GHCN-Daily o INTA SIGA)', 'id_origen': 'identificador en la red de origen',
    'tipo': 'tipo (convencional, automática, sinóptica; o tipo de indicador)', 'elev_m': 'elevación de la estación, m',
    'desde': 'primera fecha con dato', 'hasta': 'última fecha con dato', 'pct_tmin': '% de días con Tmin en el período',
    'pct_tmax': '% de días con Tmax', 'pct_p': '% de días con lluvia registrada', 'pct_tmin_suelo': '% de días con mínima a 5 cm',
    'n_tmin': 'días válidos de Tmin', 'n_tmax': 'días válidos de Tmax', 'n_tmin_suelo': 'días válidos de mínima a 5 cm',
    'n_p': 'días válidos de lluvia', 'n_t': 'días válidos con Tmin y Tmax', 'n_dias_anio': 'días del año calendario',
    'completo_t': 'período con ≥ 95 % de días válidos de temperatura', 'completo_suelo': 'año con ≥ 95 % de días con mínima a 5 cm',
    'completo_p': 'campaña con ≥ 95 % de días con lluvia',
    'dias_tmin_lt0': 'días con Tmin < 0 °C (abrigo)', 'dias_tmin_lt_m2': 'días con Tmin < −2 °C', 'dias_tmin_le3': 'días con Tmin ≤ 3 °C',
    'dias_helada_suelo': 'días con mínima a 5 cm < 0 °C (helada agrometeorológica)', 'primera_doy': 'día del año de la primera Tmin < 0 °C',
    'ultima_doy': 'día del año de la última Tmin < 0 °C', 'tmin_abs': 'mínima absoluta, °C', 'tmin_suelo_abs': 'mínima absoluta a 5 cm, °C',
    'dias_tmax35': 'días con Tmax > 35 °C', 'p_mm': 'lluvia total, mm', 'dias_lluvia': 'días con lluvia ≥ 1 mm',
    'gdd10': 'grados-día base 10 °C, °C·d', 'dist_km': 'distancia a la estación más cercana con temperatura, km',
    'dif_elev_m': 'elevación de la estación − elevación media de la celda (FABDEM), m', 'conv_id': 'estación de serie larga (convencional o sinóptica) más cercana',
    'conv_dist_km': 'distancia a la estación de serie larga más cercana, km',
    # cruce y panel
    'n_radios': 'radios que aportan a la celda', 'personas_est': 'personas estimadas en la celda',
    'parte_radios_grandes': 'parte de las personas que viene de radios > 10 km² (ubicación gruesa)',
    'usa_area': 'algún radio repartido por área (sin peso de época)', 'usa_muestra': 'algún radio con factor de muestra > 1,5 (tasas ruidosas)',
    'edificios': 'edificios del cruce de la web en la celda', 'frac': 'fracción de la celda dentro del radio', 'peso': 'peso de la celda dentro del radio (suma 1 por radio)',
    'clase': 'clase de tamaño de radio', 'radios': 'radios evaluados', 'personas': 'personas evaluadas', 'wape': 'error absoluto ponderado (Σ|est − real| / Σ real)',
    'error_mediano': 'error mediano por radio', 'fracciones_sin_peso': 'fracciones donde el método no tiene peso',
    'indicador': 'indicador del panel', 'columna': 'columna de origen', 'nota': 'nota de comparabilidad', 'num': 'numerador (conteo)', 'den': 'denominador (conteo)',
    'rotulo_ind': 'rótulo',
}
for _a in (1990, 2000, 2010, 2020):
    GENERICAS[f'ghsl_{_a}'] = f'm² construidos en la celda según GHS-BUILT-S {_a}'
    GENERICAS[f'construida_{_a}'] = f'GHSL marcaba construcción en {_a} en la celda o su anillo 1'



def main() -> None:
    t0 = time.time()
    m = importlib.import_module('metodologia')
    F, P = m.FUENTES, m.PROCESOS
    ids = [f['id'] for f in F] + [p['id'] for p in P]
    assert len(ids) == len(set(ids)), 'ids repetidos'
    for f in F:
        faltan = [c for c in CAMPOS_F if c not in f]
        assert not faltan, (f['id'], faltan)
    conn = connect(DB)
    with conn, conn.cursor() as cur:
        cur.execute('SET client_min_messages = warning')
        cur.execute('DROP SCHEMA IF EXISTS meta CASCADE')
        cur.execute('CREATE SCHEMA meta')
        cur.execute("COMMENT ON SCHEMA meta IS 'Catálogo metodológico del Hexoma: fuentes, procesos y columnas. "
                    "Fuente de verdad: pipeline/hexoma/metodologia.py; carga: build_hexoma_meta.py'")
        cur.execute('CREATE TABLE meta.fuentes (' + ', '.join(f'{c} text' for c in CAMPOS_F)
                    + ', objetos text[], PRIMARY KEY (id))')
        for f in F:
            cur.execute(f'INSERT INTO meta.fuentes VALUES ({", ".join(["%s"] * (len(CAMPOS_F) + 1))})',
                        [f[c] for c in CAMPOS_F] + [f['objetos']])
        cur.execute('CREATE TABLE meta.procesos (' + ', '.join(f'{c} text' for c in CAMPOS_P)
                    + ', objetos text[], fuentes text[], PRIMARY KEY (id))')
        for p in P:
            cur.execute(f'INSERT INTO meta.procesos VALUES ({", ".join(["%s"] * (len(CAMPOS_P) + 2))})',
                        [p[c] for c in CAMPOS_P] + [p['objetos'], p['fuentes']])
        for t, d in (('fuentes', 'Una ficha metodológica por fuente de datos'),
                     ('procesos', 'Una ficha por proceso propio del Hexoma (cruces, correcciones, normales…)')):
            cur.execute(f"COMMENT ON TABLE meta.{t} IS '{d}'")

        # objetos del Hexoma
        cur.execute("""SELECT n.nspname, c.relname, c.relkind, obj_description(c.oid, 'pg_class')
                       FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                       WHERE c.relkind IN ('r', 'v', 'm')
                         AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'meta', 'topology', 'tiger')
                         AND n.nspname NOT LIKE 'pg_%'""")
        objetos = [r for r in cur.fetchall() if (r[0], r[1]) not in EXCLUIR]

        def mapear(esq, tab):
            nombre = f'{esq}.{tab}'
            fs, ps = set(), set()
            for f in F:
                if any(fnmatch.fnmatchcase(nombre, pat) for pat in f['objetos']):
                    fs.add(f['id'])
            for p in P:
                if any(fnmatch.fnmatchcase(nombre, pat) for pat in p['objetos']):
                    ps.add(p['id'])
                    fs.update(p['fuentes'])
            for pat, (efs, eps) in EXTRA.items():
                if fnmatch.fnmatchcase(nombre, pat):
                    fs.update(efs)
                    ps.update(eps)
            return sorted(fs), sorted(ps)

        cur.execute("""CREATE TABLE meta.objetos (esquema text, objeto text, tipo text, comentario text,
                       fuentes text[], procesos text[], PRIMARY KEY (esquema, objeto))""")
        tipo = {'r': 'tabla', 'v': 'vista', 'm': 'vista materializada'}
        for esq, tab, k, com in objetos:
            fs, ps = mapear(esq, tab)
            cur.execute('INSERT INTO meta.objetos VALUES (%s, %s, %s, %s, %s, %s)', (esq, tab, tipo[k], com, fs, ps))
        cur.execute("COMMENT ON TABLE meta.objetos IS 'Cada tabla o vista del Hexoma con sus fuentes y procesos'")

        cur.execute("""CREATE TABLE meta.variables AS
            SELECT n.nspname AS esquema, c.relname AS objeto, a.attnum AS orden, a.attname AS columna,
                   format_type(a.atttypid, a.atttypmod) AS tipo, col_description(c.oid, a.attnum) AS comentario,
                   o.fuentes, o.procesos,
                   col_description(c.oid, a.attnum) IS NULL AS sin_comentario,
                   (cardinality(o.fuentes) = 0 AND cardinality(o.procesos) = 0) AS sin_fuente
            FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN meta.objetos o ON o.esquema = n.nspname AND o.objeto = c.relname
            WHERE a.attnum > 0 AND NOT a.attisdropped""")
        cur.execute('ALTER TABLE meta.variables ADD PRIMARY KEY (esquema, objeto, columna)')
        cur.execute('ALTER TABLE meta.variables ADD COLUMN definicion text, ADD COLUMN origen_definicion text')
        cur.execute("UPDATE meta.variables SET definicion = comentario, origen_definicion = 'COMMENT' WHERE comentario IS NOT NULL")
        for (esq, vista), tablas in HEREDA.items():
            for t in tablas:
                cur.execute("""UPDATE meta.variables v SET definicion = b.comentario, origen_definicion = 'de ' || b.objeto
                               FROM meta.variables b WHERE v.esquema = %s AND v.objeto = %s AND v.definicion IS NULL
                                 AND b.esquema = %s AND b.objeto = %s AND b.columna = v.columna AND b.comentario IS NOT NULL""",
                            (esq, vista, esq, t))
        # columnas crudas de las capas de la web: el COMMENT de su columna en v_hexoma
        cur.execute("""UPDATE meta.variables v SET definicion = b.comentario, origen_definicion = 'de v_hexoma.' || b.columna
                       FROM meta.variables b WHERE v.esquema = 'public' AND v.objeto LIKE 'capa\\_%%' AND v.definicion IS NULL
                         AND b.esquema = 'public' AND b.objeto = 'v_hexoma' AND b.comentario IS NOT NULL
                         AND b.columna = substr(v.objeto, 6) || '__' || v.columna""")
        for col, d in GENERICAS.items():
            cur.execute("""UPDATE meta.variables SET definicion = %s, origen_definicion = 'genérica'
                           WHERE columna = %s AND definicion IS NULL""", (d, col))
        # rangos y columnas internas de la carga de la web
        cur.execute("""UPDATE meta.variables SET origen_definicion = 'regla',
                         definicion = '% de celdas del ámbito ' || CASE WHEN columna LIKE '%%dep%%' THEN 'departamento'
                                       WHEN columna LIKE '%%prov%%' THEN 'provincia' ELSE 'región' END ||
                                      CASE WHEN columna LIKE '%%\\_lt' THEN ' con valor estrictamente menor' ELSE ' con el mismo valor' END ||
                                      ', en centésimos de punto (0–10000)'
                       WHERE esquema = 'public' AND objeto LIKE 'rango\\_%%' AND definicion IS NULL
                         AND (columna LIKE '%%\\_lt' OR columna LIKE '%%\\_eq')""")
        cur.execute("""UPDATE meta.variables SET origen_definicion = 'regla',
                         definicion = 'columna del parquet de la web tal cual; no la muestra la ficha (ver la fuente web_ de la capa)'
                       WHERE esquema = 'public' AND objeto LIKE 'capa\\_%%' AND definicion IS NULL""")
        cur.execute("""UPDATE meta.variables SET origen_definicion = 'regla',
                         definicion = 'columna cruda de la tabla básica de Poblaciones (censo 2010 por radio); las usadas están documentadas en censo_2010.indicadores'
                       WHERE esquema = 'censo_2010' AND objeto = 'basico' AND definicion IS NULL""")
        cur.execute("""UPDATE meta.variables SET origen_definicion = 'regla',
                         definicion = 'columna del archivo de terceros tal cual (ver externos.fuentes)'
                       WHERE esquema = 'externos' AND definicion IS NULL""")
        # mismo nombre en otra tabla del mismo esquema (último recurso, anotado)
        cur.execute("""UPDATE meta.variables v SET definicion = b.comentario, origen_definicion = 'mismo nombre en ' || b.objeto
                       FROM (SELECT DISTINCT ON (esquema, columna) esquema, columna, objeto, comentario FROM meta.variables
                             WHERE comentario IS NOT NULL ORDER BY esquema, columna, objeto) b
                       WHERE v.definicion IS NULL AND b.esquema = v.esquema AND b.columna = v.columna""")
        cur.execute('UPDATE meta.variables SET sin_comentario = definicion IS NULL')
        cur.execute("COMMENT ON TABLE meta.variables IS 'Una fila por columna del Hexoma: tipo, COMMENT, fuentes y "
                    "procesos. sin_comentario y sin_fuente marcan lo que falta documentar'")
        cur.execute("""CREATE VIEW meta.v_ficha AS
            SELECT v.esquema, v.objeto, v.columna, v.definicion, v.origen_definicion, f.id AS fuente, f.tipo_dato, f.res_espacial,
                   f.res_temporal, f.validacion, f.limitaciones
            FROM meta.variables v LEFT JOIN LATERAL unnest(v.fuentes) u(fid) ON true
            LEFT JOIN meta.fuentes f ON f.id = u.fid""")
        cur.execute("COMMENT ON VIEW meta.v_ficha IS 'Columna × fuente con tipo de dato, resolución, validación y "
                    "limitaciones al lado. Ej.: SELECT * FROM meta.v_ficha WHERE objeto = ''era5_helada_corr'''")
    verificar(conn)
    log(f'listo en {time.time() - t0:.0f} s')


def verificar(conn) -> None:
    with conn.cursor() as cur:
        cur.execute('SELECT count(*) FROM meta.fuentes'); nf = cur.fetchone()[0]
        cur.execute('SELECT count(*) FROM meta.procesos'); np_ = cur.fetchone()[0]
        cur.execute('SELECT count(*), count(*) FILTER (WHERE sin_fuente), count(*) FILTER (WHERE sin_comentario) FROM meta.variables')
        nv, sf, sc = cur.fetchone()
        print(f'  {nf} fuentes, {np_} procesos, {nv:,} columnas; sin fuente ni proceso: {sf}; sin comentario: {sc}')
        cur.execute("""SELECT esquema || '.' || objeto FROM meta.objetos
                       WHERE cardinality(fuentes) = 0 AND cardinality(procesos) = 0 ORDER BY 1""")
        r = [x[0] for x in cur.fetchall()]
        print(f'  objetos sin fuente ni proceso ({len(r)}):', r)
        cur.execute("""SELECT esquema, count(*) FILTER (WHERE sin_comentario), count(*) FROM meta.variables
                       GROUP BY 1 ORDER BY 1""")
        print('  columnas sin definición por esquema:', cur.fetchall())
        cur.execute("""SELECT esquema || '.' || objeto || '.' || columna FROM meta.variables WHERE sin_comentario ORDER BY 1 LIMIT 40""")
        print('  ejemplos sin definición:', [x[0] for x in cur.fetchall()])
        cur.execute("""SELECT id FROM meta.fuentes WHERE coalesce(procesamiento, '') = '' OR coalesce(validacion, '') = ''
                         OR coalesce(limitaciones, '') = ''""")
        print('  fuentes con campos clave vacíos:', [x[0] for x in cur.fetchall()])


if __name__ == '__main__':
    main()
