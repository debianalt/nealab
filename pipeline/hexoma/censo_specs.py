"""Especificaciones de los censos 1991, 2001, 2010 y 2022 para el Hexoma (build_hexoma_censo.py).

Cada año dice de dónde salen los conteos por radio, qué totales oficiales trae cada radio y qué
indicadores se arman, siempre en conteos: <x>_num y <x>_den por radio, de modo que una tasa de un
conjunto de radios o de celdas se calcula sumando y dividiendo al final.

Formatos:
    TOTAL  (nombre, variable, categorías | None, rótulo)            conteo del radio
    PCT    (nombre, variable, num, excluidas del den, rótulo)       den = todas las categorías salvo las excluidas
    MEAN   (nombre, variable, excluidas, rótulo)                    Σ categoría × conteo / Σ conteo
    BASICO (nombre, expresión del num, expresión del den, rótulo)   sólo 2010: columnas de la tabla básica

Las categorías se comparan como enteros (cat_int). Los códigos están verificados contra los
diccionarios de cada base (metadata.parquet) el 1-nov-2026.

Muestra (1991 y 2010). En los radios de las localidades grandes el cuestionario ampliado se
aplicó a una muestra de viviendas, y las bases REDATAM traen sólo esa muestra. Cada radio se
calibra con su total oficial de personas (`radios.parquet`): factor = personas oficiales /
personas en la muestra. El factor multiplica todos los conteos del radio (personas, hogares y
viviendas: la muestra es de viviendas con todos sus hogares y personas). En los radios censados
completos el factor es 1. Los radios «sólo básico» no tienen datos del ampliado: quedan con los
totales oficiales y los indicadores en NULL. En 2010 los indicadores de hogares y de población del
cuestionario básico salen de la tabla básica completa (BASICO), no de la muestra.
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / 'pipeline' / 'output'
SC = 'https://data.source.coop/nlebovits/censo-argentino'

# código INDEC de provincia → territorio del Hexoma
PROV = {54: 'misiones', 18: 'corrientes', 22: 'chaco', 34: 'formosa'}

R = range  # para rangos de edad


def rng(a: int, b: int) -> list[int]:
    return list(range(a, b + 1))


# ── 2022 ──────────────────────────────────────────────────────────────────────
PCT_2022 = [
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
    ('agua_dentro_vivienda', 'HOGAR_H13', [1], [], 'Agua por cañería dentro de la vivienda / hogares'),
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

# ── 2001 (cuestionario único: censo completo) ─────────────────────────────────
PCT_2001 = [
    ('mujeres', 'PERSONA_P2', [2], [], 'Mujeres / población'),
    ('menores_15', 'PERSONA_EDADAGRU', [1], [], 'Personas de 0 a 14 años / población'),
    ('mayores_65', 'PERSONA_EDADAGRU', [3], [], 'Personas de 65 años y más / población'),
    ('nacidos_otra_provincia', 'PERSONA_P7A', [3], [], 'Nacidos en otra provincia / población'),
    ('nacidos_otro_pais', 'PERSONA_P7A', [4], [], 'Nacidos en otro país / población'),
    ('migrante_reciente', 'PERSONA_P8A', [3, 4], [5],
     'Vivían hace 5 años en otra provincia u otro país / población de 5 años y más'),
    ('nbi', 'HOGAR_CHNBI', [2], [3], 'Hogares con NBI (al menos un criterio) / hogares'),
    ('nbi_vivienda', 'HOGAR_CHNBI1', [1], [2], 'NBI vivienda de tipo inconveniente / hogares'),
    ('nbi_sanitario', 'HOGAR_CHNBI2', [1], [2], 'NBI condiciones sanitarias / hogares'),
    ('nbi_hacinamiento', 'HOGAR_CHNBI3', [1], [2], 'NBI hacinamiento (más de 3 personas por cuarto) / hogares'),
    ('nbi_escolaridad', 'HOGAR_CHNBI4', [1], [2], 'NBI escolaridad (niños de 6 a 12 que no asisten) / hogares'),
    ('nbi_subsistencia', 'HOGAR_CHNBI5', [1], [2], 'NBI capacidad de subsistencia / hogares'),
    ('ipmh_privacion', 'HOGAR_CH33', [2, 3, 4], [0], 'Hogares con alguna privación material (IPMH) / hogares'),
    ('ipmh_corrientes', 'HOGAR_CH33', [2], [0], 'Privación sólo de recursos corrientes (IPMH) / hogares'),
    ('ipmh_patrimoniales', 'HOGAR_CH33', [3], [0], 'Privación sólo patrimonial (IPMH) / hogares'),
    ('ipmh_convergente', 'HOGAR_CH33', [4], [0], 'Privación convergente (IPMH) / hogares'),
    ('calmat_iii_a_v', 'HOGAR_CH28', [3, 4, 5], [0], 'Calidad de materiales (CALMAT) III, IV o V / hogares'),
    ('calmat_iv_v', 'HOGAR_CH28', [4, 5], [0], 'Calidad de materiales (CALMAT) IV o V / hogares'),
    ('techo_precario', 'HOGAR_CH13', [11, 12], [0], 'Techo de chapa de cartón, caña, tabla o paja / hogares'),
    ('agua_fuera_vivienda', 'HOGAR_H10', [2, 3], [0],
     'Agua por cañería fuera de la vivienda o fuera del terreno / hogares (el diccionario rotula mal la categoría 1, que es «dentro de la vivienda»)'),
    ('agua_dentro_vivienda', 'HOGAR_H10', [1], [0], 'Agua por cañería dentro de la vivienda / hogares'),
    ('cloaca', 'HOGAR_CH9', [1], [0], 'Inodoro con descarga y desagüe a red pública / hogares'),
    ('sin_inodoro_descarga', 'HOGAR_CH9', [4], [0], 'Inodoro sin descarga, o sin inodoro / hogares'),
    ('hacinamiento', 'HOGAR_HACIAGRU', [5, 6], [7], 'Más de 2 personas por cuarto / hogares'),
    ('hacinamiento_critico', 'HOGAR_HACIAGRU', [6], [7], 'Más de 3 personas por cuarto / hogares'),
    ('propietario', 'HOGAR_CH6', [1, 2], [0], 'Propietario de la vivienda (con o sin el terreno) / hogares'),
    ('inquilino', 'HOGAR_CH6', [3], [0], 'Inquilino / hogares'),
    ('tenencia_precaria', 'HOGAR_CH6', [4, 5, 6], [0], 'Ocupante por préstamo, por trabajo u otra situación / hogares'),
    ('computadora', 'HOGAR_CH26', [1, 2], [0], 'Computadora / hogares'),
    ('internet', 'HOGAR_CH26', [1], [0], 'Computadora con internet / hogares'),
    ('sin_heladera', 'HOGAR_CH23', [3], [0], 'Sin heladera ni freezer / hogares'),
    ('telefono_fijo', 'HOGAR_CH25', [1, 3], [0], 'Teléfono fijo / hogares'),
    ('celular', 'HOGAR_CH25', [1, 2], [0], 'Teléfono celular / hogares'),
    ('hogar_unipersonal', 'HOGAR_CH82', [1], [0], 'Hogares unipersonales / hogares'),
    ('rural_agrupada', 'VIVIENDA_URP', [2], [], 'Viviendas en área rural agrupada / viviendas'),
    ('rural_dispersa', 'VIVIENDA_URP', [3], [], 'Viviendas en área rural dispersa / viviendas'),
    ('analfabetismo', 'PERSONA_P4', [2], [], 'No sabe leer y escribir / población'),
    ('asiste', 'PERSONA_CP3', [1, 2], [4], 'Asiste a un establecimiento educativo / población con dato'),
    ('nunca_asistio', 'PERSONA_CP3', [0], [4], 'Nunca asistió / población con dato'),
    ('secundario_completo_o_mas', 'PERSONA_CP4', [9, 10, 11, 12, 29, 31, 32, 33, 34, 35, 36], [37],
     'Secundario completo o más (incluye a quienes cursan o cursaron terciario o universidad) / personas con dato (incluye niños)'),
    ('universitario_completo', 'PERSONA_CP4', [36], [37], 'Universitario completo / personas con dato (incluye niños)'),
    ('tasa_actividad', 'PERSONA_CP63', rng(1, 8), [13], 'Ocupados y desocupados / población de 14 años y más'),
    ('tasa_empleo', 'PERSONA_CP63', rng(1, 4), [13], 'Ocupados / población de 14 años y más'),
    ('tasa_desocupacion', 'PERSONA_CP63', rng(5, 8), rng(9, 13), 'Desocupados / población activa'),
    ('informalidad', 'PERSONA_CP9', [3], [0, 4], 'No le descuentan ni aporta jubilación / ocupados que reciben sueldo o ganancia'),
    ('cat_asalariado', 'PERSONA_CP7', [1, 2], [0], 'Obreros o empleados (público y privado) / ocupados'),
    ('cat_patron', 'PERSONA_CP7', [3], [0], 'Patrones / ocupados'),
    ('cat_cuenta_propia', 'PERSONA_CP7', [4], [0], 'Cuenta propia / ocupados'),
    ('cat_familiar', 'PERSONA_CP7', [5, 6], [0], 'Trabajadores familiares / ocupados'),
    ('rama_agro', 'PERSONA_RAMA2', [1, 2], [0, 18], 'Agricultura, ganadería, caza, silvicultura y pesca / ocupados con rama codificada'),
    ('rama_industria', 'PERSONA_RAMA2', [4], [0, 18], 'Industria manufacturera / ocupados con rama codificada'),
    ('rama_construccion', 'PERSONA_RAMA2', [6], [0, 18], 'Construcción / ocupados con rama codificada'),
    ('rama_comercio', 'PERSONA_RAMA2', [7], [0, 18], 'Comercio / ocupados con rama codificada'),
    ('rama_adm_publica', 'PERSONA_RAMA2', [12], [0, 18], 'Administración pública / ocupados con rama codificada'),
    ('rama_ensenanza', 'PERSONA_RAMA2', [13], [0, 18], 'Enseñanza / ocupados con rama codificada'),
    ('rama_salud', 'PERSONA_RAMA2', [14], [0, 18], 'Servicios sociales y de salud / ocupados con rama codificada'),
    ('rama_servicio_domestico', 'PERSONA_RAMA2', [16], [0, 18], 'Servicio doméstico / ocupados con rama codificada'),
]

# ── 2010: básico completo (tabla de Poblaciones) + ampliado (muestra calibrada) ──
# Columnas de la tabla básica verificadas contra el censo crudo en los 3.710 radios del NEA
# censados completos (ver README): coinciden exacto con las categorías anotadas.
BASICO_2010 = [
    ('nbi', 'h_nbi', 'h_total', 'Hogares con NBI / hogares (básico completo)'),
    ('nbi_hacinamiento', 'h_hacina_1', 'h_total', 'NBI hacinamiento, más de 3 personas por cuarto (HOGAR_NBI3) / hogares'),
    ('sin_heladera', 'h_heladera', 'h_total', 'Sin heladera (H2819A = 2) / hogares'),
    ('computadora', 'h_computad', 'h_total', 'Computadora (H2819B = 1) / hogares'),
    ('telefono_fijo', 'h_telefono', 'h_total', 'Teléfono de línea (H2819D = 1) / hogares'),
    ('agua_red', 'h_total - h_agua_red', 'h_total', 'Agua para beber y cocinar de red pública (H1409 = 1) / hogares'),
    ('cloaca', 'h_total - h_cloaca', 'h_total', 'Desagüe del inodoro a red pública (H1712 = 1) / hogares'),
    ('pozo_ciego_u_hoyo', 'h_hoyo', 'h_total', 'Desagüe sólo a pozo ciego o a hoyo (H1712 = 3, 4) / hogares'),
    ('techo_chapa_o_precario', 'h_techo', 'h_total',
     'Techo de chapa de metal, fibrocemento, cartón, caña, paja u otro (H1006 = 4–8) / hogares'),
    ('sin_cielorraso', 'h_cielorra', 'h_total', 'Techo sin cielorraso (H1107 = 2) / hogares'),
    ('piso_sin_revestimiento', 'h_piso', 'h_total',
     'Piso de cemento, ladrillo fijo, tierra u otro (H0705 = 2, 3, 4) / hogares'),
    ('cocina_garrafa_o_lena', 'h_combusti', 'h_total', 'Cocina con gas en garrafa o con leña o carbón (H2014 = 4, 6) / hogares'),
    ('propietario', 'h_propieta', 'h_total', 'Vivienda propia (H2417 = 1) / hogares'),
    ('inquilino', 'h_inquilin', 'h_total', 'Vivienda alquilada (H2417 = 2) / hogares'),
    ('tenencia_precaria', 'h_ni_propi', 'h_total', 'Vivienda prestada, cedida por trabajo u otra situación (H2417 = 3–5) / hogares'),
    ('mujeres', 'm_total', 'p_total', 'Mujeres / población (básico completo)'),
    ('menores_18', 'p_a17', 'p_total', 'Personas de 0 a 17 años / población'),
    ('mayores_70', 'p_70a', 'p_total', 'Personas de 70 años y más / población'),
    ('universitario_completo_18', 'p18a_unive', 'p_18a', 'Universitario completo (MNI = 8) / población de 18 años y más'),
]
PCT_2010 = [  # ampliado: muestra calibrada por radio
    ('nacidos_otra_provincia', 'PERSONA_P08M', [2, 3, 4], [], 'Nacidos en otra provincia / población'),
    ('nacidos_otro_pais', 'PERSONA_P05', [2], [], 'Nacidos en otro país / población'),
    ('migrante_reciente', 'PERSONA_P09M', [3, 4, 5, 6], [0, 7],
     'Vivían hace 5 años en otra provincia u otro país / población de 5 años y más'),
    ('nbi_vivienda', 'HOGAR_NBI1', [1], [2], 'NBI vivienda de tipo inconveniente / hogares'),
    ('nbi_sanitario', 'HOGAR_NBI2', [1], [2], 'NBI condiciones sanitarias / hogares'),
    ('nbi_escolaridad', 'HOGAR_NBI4', [1], [2], 'NBI escolaridad (niños de 6 a 12 que no asisten) / hogares'),
    ('nbi_subsistencia', 'HOGAR_NBI5', [1], [2], 'NBI capacidad de subsistencia / hogares'),
    ('materiales_calidad_iii_iv', 'HOGAR_INMAT', [3, 4], [0], 'Calidad de materiales III o IV / hogares'),
    ('materiales_calidad_iv', 'HOGAR_INMAT', [4], [0], 'Calidad de materiales IV / hogares'),
    ('servicios_insuficientes', 'HOGAR_INCALSERV', [3], [0], 'Calidad de conexiones a servicios básicos insuficiente / hogares'),
    ('agua_fuera_vivienda', 'HOGAR_H1308', [2, 3], [0], 'Agua por cañería fuera de la vivienda o fuera del terreno / hogares'),
    ('agua_dentro_vivienda', 'HOGAR_H1308', [1], [0], 'Agua por cañería dentro de la vivienda / hogares'),
    ('techo_precario', 'HOGAR_H1006', [6, 7], [0], 'Techo de chapa de cartón, caña, palma, tabla o paja / hogares'),
    ('menores_15', 'PERSONA_P03', rng(0, 14), [], 'Personas de 0 a 14 años / población'),
    ('mayores_65', 'PERSONA_P03', rng(65, 130), [], 'Personas de 65 años y más / población'),
    ('cocina_lena_carbon', 'HOGAR_H2014', [6], [0], 'Cocina principalmente con leña o carbón / hogares'),
    ('cocina_garrafa', 'HOGAR_H2014', [4], [0], 'Cocina principalmente con gas en garrafa / hogares'),
    ('cocina_gas_red', 'HOGAR_H2014', [1], [0], 'Cocina principalmente con gas de red / hogares'),
    ('hogar_unipersonal', 'HOGAR_TIPOHOGAR', [1], [21], 'Hogares unipersonales / hogares'),
    ('rancho_casilla', 'VIVIENDA_V01', [2, 3], [0], 'Ranchos y casillas / viviendas particulares'),
    ('rural_agrupada', 'VIVIENDA_URP', [2], [], 'Viviendas en área rural agrupada / viviendas'),
    ('rural_dispersa', 'VIVIENDA_URP', [3], [], 'Viviendas en área rural dispersa / viviendas'),
    ('inundacion_5_anios', 'VIVIENDA_CC1', [1], [0], 'Viviendas en zonas que se inundaron en los últimos 5 años / viviendas'),
    ('villa_asentamiento', 'VIVIENDA_CC10', [1], [0], 'Viviendas en villa de emergencia o asentamiento / viviendas'),
    ('cuadra_pavimentada', 'VIVIENDA_CC13', [1], [0], 'Viviendas con al menos una cuadra pavimentada / viviendas'),
    ('analfabetismo', 'PERSONA_P1707', [2], [0], 'No sabe leer y escribir / población de 10 años y más'),
    ('asiste', 'PERSONA_P1808', [1], [0, 4], 'Asiste a un establecimiento educativo / población de 3 años y más con dato'),
    ('nunca_asistio', 'PERSONA_P1808', [3], [0, 4], 'Nunca asistió / población de 3 años y más con dato'),
    ('secundario_completo_o_mas', 'PERSONA_MNI', [4, 6, 7, 8, 9], [0],
     'Secundario completo o más / población que asiste o asistió'),
    ('universitario_completo', 'PERSONA_MNI', [8], [0], 'Universitario completo / población que asiste o asistió'),
    ('informalidad', 'PERSONA_P34', [2], [0, 3], 'Obreros o empleados a los que no les descuentan jubilación / asalariados con dato'),
    ('cat_asalariado', 'PERSONA_P32', [1], [0], 'Obreros o empleados / ocupados'),
    ('cat_patron', 'PERSONA_P32', [2], [0], 'Patrones / ocupados'),
    ('cat_cuenta_propia', 'PERSONA_P32', [3], [0], 'Cuenta propia / ocupados'),
    ('cat_familiar', 'PERSONA_P32', [4], [0], 'Trabajadores familiares / ocupados'),
    ('jubilacion_pension', 'PERSONA_P15', [1], [0], 'Cobra jubilación o pensión / población'),
    ('cobertura_obra_social', 'PERSONA_P1305', [1, 2, 3], [], 'Obra social o prepaga (incluye PAMI) / población'),
    ('solo_plan_estatal', 'PERSONA_P1305', [4], [], 'Sólo programas o planes estatales de salud / población'),
    ('sin_cobertura', 'PERSONA_P1305', [5], [], 'Sin obra social, prepaga ni plan estatal / población'),
]

# ── 1991 (microdatos REDATAM; muestra calibrada por radio) ────────────────────
PCT_1991 = [
    ('mujeres', 'PERSONA_SEXO', [2], [], 'Mujeres / población'),
    ('menores_15', 'PERSONA_EDAD', rng(0, 14), [], 'Personas de 0 a 14 años / población'),
    ('mayores_65', 'PERSONA_EDAD', rng(65, 130), [], 'Personas de 65 años y más / población'),
    ('analfabetismo', 'PERSONA_LEESCRIBE', [2], [9], 'No sabe leer y escribir / población con dato'),
    ('asiste', 'PERSONA_ASISTENCIA', [1, 2], [8, 9, 10], 'Asiste a un establecimiento educativo / población con dato'),
    ('nunca_asistio', 'PERSONA_ASISTENCIA', [4], [8, 9, 10], 'Nunca asistió / población con dato'),
    ('cobertura_obra_social', 'PERSONA_OBRASOCIAL', [1], [9], 'Obra social / población con dato'),
    ('jubilacion_pension', 'PERSONA_JUBILACIO', [1], [9], 'Cobra jubilación o pensión / población con dato'),
    ('tasa_actividad', 'PERSONA_CONDACTIV', [2, 3], [0, 9], 'Ocupados y desocupados / población de 14 años y más con dato'),
    ('tasa_empleo', 'PERSONA_CONDACTIV', [2], [0, 9], 'Ocupados / población de 14 años y más con dato'),
    ('tasa_desocupacion', 'PERSONA_CONDACTIV', [3], [0, 5, 6, 7, 9], 'Desocupados / población activa'),
    ('informalidad', 'PERSONA_DESCJUBIL', [2], [9, 10], 'Ocupados a los que no les descuentan jubilación / ocupados con dato'),
    ('cat_asalariado', 'PERSONA_RELACILABO', [2, 3], [9, 10], 'Obreros o empleados (público y privado) / ocupados con dato'),
    ('cat_patron', 'PERSONA_RELACILABO', [1], [9, 10], 'Patrones / ocupados con dato'),
    ('cat_cuenta_propia', 'PERSONA_RELACILABO', [5], [9, 10], 'Cuenta propia / ocupados con dato'),
    ('cat_servicio_domestico', 'PERSONA_RELACILABO', [4], [9, 10], 'Servicio doméstico / ocupados con dato'),
    ('cat_familiar', 'PERSONA_RELACILABO', [6], [9, 10], 'Trabajadores familiares / ocupados con dato'),
    # hogares (las variables de vivienda las hereda cada hogar)
    ('nbi', 'HOGAR_CMV23NBI', rng(1, 5), [6, 7], 'Hogares en viviendas con NBI (al menos un criterio) / hogares'),
    ('nbi_hacinamiento', 'HOGAR_CMV2AHACI', [1], [], 'NBI hacinamiento / hogares'),
    ('nbi_vivienda', 'HOGAR_CMV2BTIVI', [1], [], 'NBI vivienda de tipo inconveniente / hogares'),
    ('nbi_sanitario', 'HOGAR_CMV2CRETR', [1], [], 'NBI condiciones sanitarias / hogares'),
    ('nbi_escolaridad', 'HOGAR_CMV2DEDES', [1], [], 'NBI escolaridad / hogares'),
    ('nbi_subsistencia', 'HOGAR_CMV2ECASU', [1], [], 'NBI capacidad de subsistencia / hogares'),
    ('agua_red', 'HOGAR_OBTAGUA', [1], [8, 9, 10], 'Agua de red pública / hogares con dato'),
    ('agua_fuera_vivienda', 'HOGAR_AGUA', [2, 3], [8, 9, 10], 'Agua fuera de la vivienda o fuera del terreno / hogares con dato'),
    ('agua_dentro_vivienda', 'HOGAR_AGUA', [1], [8, 9, 10], 'Agua por cañería dentro de la vivienda / hogares con dato'),
    ('cloaca', 'HOGAR_DESAGINH', [1], [8, 9, 11], 'Desagüe a red pública / hogares con dato (incluye a los sin inodoro con descarga)'),
    ('piso_tierra', 'HOGAR_PISOS', [3], [5, 6], 'Piso de tierra / hogares con dato'),
    ('piso_sin_revestimiento', 'HOGAR_PISOS', [2, 3, 4], [5, 6], 'Piso de cemento, ladrillo fijo, tierra u otro / hogares con dato'),
    ('techo_precario', 'HOGAR_TECHO', [5, 6], [9, 10], 'Techo de chapa de cartón, caña, tabla o paja / hogares con dato'),
    ('cocina_lena_carbon', 'HOGAR_COMBUSTIH', [5], [7, 8], 'Cocina con leña o carbón / hogares'),
    ('cocina_garrafa', 'HOGAR_COMBUSTIH', [2], [7, 8], 'Cocina con gas envasado / hogares'),
    ('cocina_gas_red', 'HOGAR_COMBUSTIH', [1], [7, 8], 'Cocina con gas de red / hogares'),
    ('sin_electricidad', 'HOGAR_ELECTRIC', [3], [4], 'Sin electricidad / hogares'),
    ('propietario', 'HOGAR_PROPVIVH', [1, 2], [8, 9, 10, 11], 'Propietario de la vivienda (con o sin el terreno) / hogares con dato'),
    ('inquilino', 'HOGAR_PROPVIVH', [3], [8, 9, 10, 11], 'Inquilino o arrendatario / hogares con dato'),
    ('tenencia_precaria', 'HOGAR_PROPVIVH', [4, 5, 6, 7], [8, 9, 10, 11], 'Ocupante por trabajo, préstamo, de hecho u otro / hogares con dato'),
    ('rancho', 'HOGAR_TIPOVIV', [2], [9, 10, 11], 'Hogares en ranchos (1991 no separa casillas) / hogares con dato'),
    ('villa_emergencia', 'HOGAR_VIEMERGH', [1], [3], 'Hogares en villa de emergencia / hogares'),
]

SPECS: dict[int, dict] = {
    2022: dict(
        fuente=f'{SC}/2022', muestra=False,
        cod='COD_2022', personas_oficial='POB_TOT_P', viviendas_oficial='VIV_TOT_P',
        geometria='local',
        TOTAL=[('personas', 'PERSONA_P02', None, 'Población en viviendas particulares'),
               ('hogares', 'HOGAR_NBI_TOT', None, 'Hogares'),
               ('viviendas', 'VIVIENDA_TIPOVIVG', None, 'Viviendas particulares'),
               ('viviendas_ocupadas', 'VIVIENDA_V02', [1], 'Viviendas particulares con personas presentes')],
        PCT=PCT_2022,
        MEAN=[('personas_por_hogar', 'HOGAR_TOTPOBH', [], 'Personas por hogar (promedio)'),
              ('hijos_por_mujer', 'PERSONA_P35', [], 'Hijos nacidos vivos por mujer (promedio, mujeres de 14 años y más)'),
              ('anios_escolaridad', 'PERSONA_AESC', [99], 'Años de escolaridad completos (promedio, personas con dato)')],
        ATTR={'tipo_radio': 'VIVIENDA_TIPO_RADIO', 'localidad': 'VIVIENDA_CODLOC', 'gobierno_local': 'VIVIENDA_CODGL',
              'categoria_gobierno_local': 'VIVIENDA_CATGL', 'aglomerado': 'VIVIENDA_CODAGLO'},
        persona_muestra='PERSONA_P02',
    ),
    2010: dict(
        fuente=f'{SC}/2010', muestra=True,
        cod='COD_2010', personas_oficial='B_POB_TOT', viviendas_oficial='B_VIV_P',
        geometria='geometry',
        TOTAL=[('hogares_ampliado', 'HOGAR_ALGUNBI', [0, 1], 'Hogares según el ampliado, expandidos (los completos: indicadores.hogares)')],
        PCT=PCT_2010, MEAN=[], BASICO=BASICO_2010,
        ATTR={'area': 'VIVIENDA_URP', 'localidad': 'VIVIENDA_LOCAL', 'municipio': 'VIVIENDA_MUNI',
              'aglomerado': 'VIVIENDA_CODAGLO'},
        persona_muestra='PERSONA_P02',
    ),
    2001: dict(
        fuente=f'{SC}/2001', muestra=False,
        cod='COD_2001', personas_oficial='POB_TOT', viviendas_oficial='VIV_TOT',
        geometria='geom',
        TOTAL=[('personas', 'PERSONA_P2', None, 'Población (incluye viviendas colectivas)'),
               ('personas_en_hogares', 'PERSONA_P1', rng(1, 9), 'Población en hogares (sin residentes de viviendas colectivas)'),
               ('hogares', 'HOGAR_CHNBI', [1, 2], 'Hogares')],
        PCT=PCT_2001,
        MEAN=[('anios_escolaridad', 'PERSONA_CP12', [18], 'Años de estudio aprobados (promedio; 17 = 17 o más)')],
        ATTR={'area': 'VIVIENDA_URP', 'localidad': 'VIVIENDA_LOC', 'municipio': 'VIVIENDA_MUNI',
              'aglomerado': 'VIVIENDA_AGLO'},
        persona_muestra='PERSONA_P2',
    ),
    1991: dict(
        fuente='redatam:ARG1991 (github.com/pachadotdev/redatam-microdata, release 2.0.2)', muestra=True,
        cod='COD_1991', personas_oficial='B_POB_TOT', viviendas_oficial='B_VIV_P',
        geometria='geom',
        TOTAL=[('hogares', 'HOGAR_CMV2AHACI', None, 'Hogares (expandidos)')],
        PCT=PCT_1991, MEAN=[],
        ATTR={},
        persona_muestra='PERSONA_SEXO',
    ),
}

# ── Panel 1991–2022: sólo indicadores con la misma definición en los años en que aparecen ──
# (indicador, tipo, {año: columna de censo_<año>.indicadores}, rótulo, nota de comparabilidad)
# tipo 'pct' usa <columna>_num y <columna>_den; 'total' usa la columna tal cual.
TODOS = (1991, 2001, 2010, 2022)


def todos(col: str) -> dict[int, str]:
    return {a: col for a in TODOS}


def menos(col: str, *sin: int) -> dict[int, str]:
    return {a: col for a in TODOS if a not in sin}


PANEL = [
    ('personas', 'total', todos('personas_oficial'), 'Población total (oficial del radio)',
     '2022 cuenta sólo viviendas particulares; los demás años incluyen colectivas'),
    ('viviendas', 'total', todos('viviendas_oficial'), 'Viviendas (oficial del radio)',
     '1991, 2010 y 2022: particulares; 2001: total'),
    ('hogares', 'total', todos('hogares'), 'Hogares',
     '1991 expandido de la muestra; 2010 de la tabla básica completa'),
    ('nbi', 'pct', todos('nbi'), 'Hogares con NBI / hogares',
     'Mismos cinco criterios INDEC. 1991: NBI de la vivienda, heredado por sus hogares'),
    ('nbi_hacinamiento', 'pct', todos('nbi_hacinamiento'), 'Hogares con más de 3 personas por cuarto / hogares', ''),
    ('nbi_vivienda', 'pct', todos('nbi_vivienda'), 'NBI vivienda de tipo inconveniente / hogares',
     '2010 de la muestra calibrada'),
    ('nbi_sanitario', 'pct', todos('nbi_sanitario'), 'NBI condiciones sanitarias / hogares',
     '2010 de la muestra calibrada'),
    ('nbi_escolaridad', 'pct', todos('nbi_escolaridad'), 'NBI escolaridad / hogares', '2010 de la muestra calibrada'),
    ('nbi_subsistencia', 'pct', todos('nbi_subsistencia'), 'NBI capacidad de subsistencia / hogares',
     '2010 de la muestra calibrada'),
    ('agua_dentro_vivienda', 'pct', todos('agua_dentro_vivienda'), 'Agua por cañería dentro de la vivienda / hogares',
     '2010 de la muestra calibrada'),
    ('agua_red', 'pct', menos('agua_red', 2001), 'Agua de red pública / hogares',
     '2001 no preguntó la procedencia del agua'),
    ('cloaca', 'pct', todos('cloaca'), 'Desagüe a red pública (cloaca) / hogares',
     '1991: denominador sin los hogares con dato desconocido'),
    ('piso_sin_revestimiento', 'pct', menos('piso_sin_revestimiento', 2001),
     'Piso de cemento, ladrillo, tierra u otro (sin cerámica, baldosa ni madera) / hogares', '2001 no preguntó pisos'),
    ('techo_precario', 'pct', todos('techo_precario'), 'Techo de chapa de cartón, caña, tabla o paja / hogares',
     '2010 de la muestra calibrada'),
    ('computadora', 'pct', menos('computadora', 1991), 'Computadora / hogares', '2022 incluye tablet'),
    ('sin_heladera', 'pct', {2001: 'sin_heladera', 2010: 'sin_heladera'}, 'Sin heladera / hogares', ''),
    ('telefono_fijo', 'pct', {2001: 'telefono_fijo', 2010: 'telefono_fijo'}, 'Teléfono fijo / hogares', ''),
    ('propietario', 'pct', todos('propietario'), 'Vivienda propia / hogares',
     '1991 y 2001: propietario de la vivienda con o sin el terreno'),
    ('inquilino', 'pct', todos('inquilino'), 'Vivienda alquilada / hogares', ''),
    ('cocina_lena_carbon', 'pct', menos('cocina_lena_carbon', 2001), 'Cocina principalmente con leña o carbón / hogares',
     '2001 no preguntó el combustible; 2010 de la muestra calibrada'),
    ('cocina_garrafa', 'pct', menos('cocina_garrafa', 2001), 'Cocina con gas en garrafa / hogares',
     '1991: gas envasado (garrafa o tubo)'),
    ('cocina_gas_red', 'pct', menos('cocina_gas_red', 2001), 'Cocina con gas de red / hogares', ''),
    ('hogar_unipersonal', 'pct', menos('hogar_unipersonal', 1991), 'Hogares unipersonales / hogares', ''),
    ('mujeres', 'pct', todos('mujeres'), 'Mujeres / población', ''),
    ('menores_15', 'pct', todos('menores_15'), 'Personas de 0 a 14 años / población', '2010 de la muestra calibrada'),
    ('mayores_65', 'pct', todos('mayores_65'), 'Personas de 65 años y más / población', '2010 de la muestra calibrada'),
    ('nacidos_otra_provincia', 'pct', menos('nacidos_otra_provincia', 1991), 'Nacidos en otra provincia / población', ''),
    ('nacidos_otro_pais', 'pct', menos('nacidos_otro_pais', 1991), 'Nacidos en otro país / población', ''),
    ('tasa_actividad', 'pct', menos('tasa_actividad', 2010), 'Ocupados y desocupados / población de 14 años y más',
     '2010: la base por radio no permite armar la condición de actividad'),
    ('tasa_empleo', 'pct', menos('tasa_empleo', 2010), 'Ocupados / población de 14 años y más', 'ídem'),
    ('tasa_desocupacion', 'pct', menos('tasa_desocupacion', 2010), 'Desocupados / población activa', 'ídem'),
    ('cat_asalariado', 'pct', todos('cat_asalariado'), 'Obreros o empleados / ocupados',
     '1991 y 2022 separan el servicio doméstico (no entra acá); 2001 y 2010 lo cuentan como asalariado'),
    ('cat_cuenta_propia', 'pct', todos('cat_cuenta_propia'), 'Cuenta propia / ocupados', ''),
    ('cat_patron', 'pct', todos('cat_patron'), 'Patrones / ocupados', ''),
    ('cat_familiar', 'pct', todos('cat_familiar'), 'Trabajadores familiares / ocupados', ''),
    ('cobertura_obra_social', 'pct', menos('cobertura_obra_social', 2001), 'Obra social o prepaga / población',
     '1991 pregunta sólo por obra social; 2001 no preguntó'),
    ('jubilacion_pension', 'pct', menos('jubilacion_pension', 2001), 'Cobra jubilación o pensión / población',
     '2001 no preguntó'),
    ('rural_dispersa', 'pct', menos('rural_dispersa', 1991), 'Viviendas en área rural dispersa / viviendas', ''),
]
