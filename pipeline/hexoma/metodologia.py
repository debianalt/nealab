"""Catálogo metodológico del Hexoma: una ficha por fuente y por proceso, en castellano.

Es la fuente de verdad versionada del esquema `meta` (la carga build_hexoma_meta.py). Cada afirmación
sale del código del repositorio, de los README o de la documentación del proveedor; donde algo no se
pudo verificar, el texto lo dice. Al cambiar una fuente o un proceso, se edita acá y se recarga.

Campos de FUENTES: id, nombre, proveedor, producto, version, acceso, licencia, cita, doi, tipo_dato,
res_espacial, res_temporal, periodo, cobertura, variables, procesamiento, asignacion_celda, validacion,
limitaciones, uso_recomendado, uso_no_recomendado, scripts, objetos, verificado.
Campos de PROCESOS: id, nombre, metodo, parametros, validacion, regla, limitaciones, scripts, objetos,
fuentes, verificado.
`objetos` = patrones esquema.tabla (con * al final) que la fuente o el proceso alimenta: así se arma
meta.variables columna por columna.

tipo_dato: medición in situ · estimación satelital · reanálisis · modelo · censo · registro administrativo
· derivado del Hexoma · capa de la web (derivada).
"""

V = '2026-11-06'
GEE = ' Acceso por Google Earth Engine con el proyecto amiable-reducer-398015 (nivel Partner, no comercial).'

FUENTES = [
    # ------------------------------------------------------------------ clima y suelo (A1)
    dict(
        id='chirps_v3', nombre='CHIRPS v3, lluvia diaria', proveedor='Climate Hazards Center, UC Santa Barbara',
        producto='Climate Hazards Center InfraRed Precipitation with Station data', version='3.0, producto diario RNL',
        acceso='GEE UCSB-CHC/CHIRPS/V3/DAILY_RNL, bajado con ee.data.computePixels en la grilla nativa' + GEE,
        licencia='Dato abierto del CHC con atribución (verificar los términos de la v3 en chc.ucsb.edu)',
        cita='Funk, C. et al. 2015. The climate hazards infrared precipitation with stations — a new environmental record for monitoring extremes. Scientific Data 2: 150066 (método de la v2; la v3 es de 2025, CHC)',
        doi='10.1038/sdata.2015.66', tipo_dato='estimación satelital',
        res_espacial='0,05° (~5,5 km; ~70 celdas res-9 por píxel)', res_temporal='diaria', periodo='1981-01-01 a 2026-06-30 (bajado)',
        cobertura='rectángulo del NEA argentino [-63,48, -30,79, -53,58, -22,41]',
        variables='precipitation (mm/día)',
        procesamiento=('1) gee_export_clima_diario.py baja un año por archivo, un día por banda, en la grilla del asset '
                       '(transform 0,05°, origen −180, 60), en dos pedidos por año por el límite de 48 MB, a '
                       '%HEXOMA_FUENTES%/clima/diario/chirps/*.npz. 2) clima_a_pixel.py toma sólo los 11.186 píxeles que '
                       'contienen algún centro de celda y calcula por campaña jul–jun, por estación (DEF/MAM/JJA/SON) y '
                       'sólo con períodos completos: lluvia total, días ≥ 1 mm, días ≥ 50 mm, racha seca más larga '
                       '(< 1 mm) y racha seca de octubre a marzo. 3) build_hexoma_clima.py lo carga en clima.chirps_*.'),
        asignacion_celda='Píxel que contiene el centro de la celda, sin interpolar (clima.hex_pixel.chirps_pixel).',
        validacion=('validar_clima.py contra 23 pluviómetros con campañas completas (GHCN-Daily e INTA SIGA), 1991–2025: '
                    'lluvia por campaña r mediana 0,92, sesgo mediano +137 mm (~+10 %); días de lluvia ≥ 1 mm +36 por '
                    'campaña (+46 %); lluvia diaria r 0,45. Tablas clima.validacion y pipeline/hexoma/validacion_clima.md.'),
        limitaciones=('El dato diario es una desagregación de la pentada con la temporalidad de ERA5: los conteos de '
                      'días y las rachas son orientativos. Un píxel cubre ~70 celdas: la variación dentro del píxel no '
                      'existe en el dato. Sobreestima levemente los totales en el NEA.'),
        uso_recomendado='Lluvia por campaña y por estación, anomalías contra la normal propia, balance con ET0.',
        uso_no_recomendado='Días de lluvia, rachas secas o eventos diarios como valor absoluto; contrastes entre celdas vecinas.',
        scripts='pipeline/gee_export_clima_diario.py; pipeline/hexoma/clima_a_pixel.py; pipeline/hexoma/build_hexoma_clima.py',
        objetos=['clima.chirps_*'], verificado=V),
    dict(
        id='era5_land', nombre='ERA5-Land, agregados diarios', proveedor='ECMWF / Copernicus Climate Change Service',
        producto='ERA5-Land hourly, agregado diario de GEE', version='DAILY_AGGR',
        acceso='GEE ECMWF/ERA5_LAND/DAILY_AGGR, bajado con ee.data.computePixels en la grilla nativa' + GEE,
        licencia='Licencia de los productos Copernicus (uso libre con atribución)',
        cita='Muñoz-Sabater, J. et al. 2021. ERA5-Land: a state-of-the-art global reanalysis dataset for land applications. Earth System Science Data 13: 4349–4383',
        doi='10.5194/essd-13-4349-2021', tipo_dato='reanálisis',
        res_espacial='0,1° (~11 km; ~1.100 celdas por píxel)', res_temporal='diaria (día UTC); humedad del suelo en media mensual',
        periodo='1950-01-02 a 2026-06-30 (bajado)', cobertura='rectángulo del NEA argentino',
        variables=('temperature_2m_max/min, dewpoint_temperature_2m, u/v_component_of_wind_10m, surface_pressure, '
                   'surface_solar_radiation_downwards_sum; volumetric_soil_water_layer_1–3 (media mensual)'),
        procesamiento=('1) gee_export_clima_diario.py: un archivo por año con 7 variables diarias y 3 mensuales. '
                       '2) clima_a_pixel.py, en los 2.919 píxeles con algún centro de celda: grados-día base 10 '
                       '(Σ max(0, (Tmax+Tmin)/2 − 10)), días > 35 °C, medias de máximas y mínimas, ET0 FAO-56 diaria '
                       '(proceso et0_fao56), humedad del suelo media; por campaña y estación; heladas por año '
                       'calendario. 3) corregir_temperatura.py corrige Tmin y Tmax con estaciones (proceso '
                       'correccion_temperatura). 4) build_hexoma_clima.py carga clima.era5_* y clima.era5_*_corr.'),
        asignacion_celda='Píxel que contiene el centro de la celda, sin interpolar (clima.hex_pixel.era5_pixel).',
        validacion=('Contra 48 estaciones, 1991–2025 (clima.validacion): Tmin diaria sesgo mediano +1,5 °C (r 0,96), '
                    'Tmax −2,1 °C (r 0,94). Consecuencia: casi no marca heladas (estaciones 0,3–6 por año en abrigo, '
                    'ERA5-Land ~0) y subestima los días > 35 °C. Grados-día −1 % (r 0,84), hasta −10 % en las sierras de '
                    'Misiones. Por eso las heladas y el calor se usan corregidos (era5_*_corr).'),
        limitaciones=('Reanálisis: modelo con observaciones asimiladas, no medición. Antes de 1979 asimila menos '
                      'observaciones. El día es UTC. El viento diario es el módulo del vector medio (subestima la '
                      'velocidad). La humedad del suelo usa una textura genérica del modelo. Un píxel cubre ~1.100 celdas: '
                      'no ve heladas de valle ni efectos de ladera.'),
        uso_recomendado='Variación entre años, grados-día, ET0, anomalías; temperatura extrema sólo en su versión corregida.',
        uso_no_recomendado='Conteos de heladas o de días > 35 °C sin corregir; humedad del suelo como valor absoluto.',
        scripts='pipeline/gee_export_clima_diario.py; pipeline/hexoma/clima_a_pixel.py; pipeline/hexoma/corregir_temperatura.py',
        objetos=['clima.era5_campania', 'clima.era5_estacion', 'clima.era5_helada'], verificado=V),
    dict(
        id='agera5', nombre='AgERA5 (probado y descartado)', proveedor='Copernicus C3S / Climate Engine',
        producto='Agrometeorological indicators from 1979 to present derived from reanalysis', version='v2 (copia de Climate Engine en GEE)',
        acceso='GEE projects/climate-engine-pro/assets/ce-ag-era5-v2/daily' + GEE, licencia='Licencia de los productos Copernicus',
        cita='Boogaard, H. et al. 2020. Agrometeorological indicators from 1979 to present derived from reanalysis. Copernicus C3S CDS',
        doi='10.24381/cds.6c68c9bb', tipo_dato='reanálisis', res_espacial='0,1°', res_temporal='diaria', periodo='1979–presente',
        cobertura='global', variables='Temperature_Air_2m_Min_24h, Temperature_Air_2m_Max_Day_Time',
        procesamiento='Sólo se probó el 1-nov-2026 en 6 estaciones; no se cargó.',
        asignacion_celda='No aplica.',
        validacion='Mismo sesgo que ERA5-Land: Tmin +0,8 a +1,9 °C, Tmax −2,2 a −4,5 °C; heladas 0–1,3 por año contra 2,3–7,5 observadas.',
        limitaciones='No corrige el problema de las heladas en el NEA.', uso_recomendado='Ninguno por ahora.',
        uso_no_recomendado='Como reemplazo de la corrección con estaciones.', scripts='(prueba ad hoc, sin script)',
        objetos=[], verificado=V),
    dict(
        id='soilgrids2', nombre='SoilGrids 2.0', proveedor='ISRIC — World Soil Information',
        producto='SoilGrids 250 m, propiedades del suelo', version='2.0 (2020)',
        acceso='GEE projects/soilgrids-isric/<var>_mean, bajado con computePixels en la grilla nativa (Mollweide, 250 m)' + GEE,
        licencia='CC BY 4.0', cita='Poggio, L. et al. 2021. SoilGrids 2.0: producing soil information for the globe with quantified spatial uncertainty. SOIL 7: 217–240',
        doi='10.5194/soil-7-217-2021', tipo_dato='modelo', res_espacial='250 m', res_temporal='una foto (sin serie)',
        periodo='perfiles de varias décadas; publicación 2020', cobertura='rectángulo del NEA argentino',
        variables='clay, sand, silt, soc, phh2o, cec, bdod, cfvo, nitrogen (0–5, 5–15, 15–30 cm)',
        procesamiento=('gee_export_suelo_relieve.py calcula en GEE la media 0–30 cm ponderada por espesor '
                       '(5·[0–5] + 10·[5–15] + 15·[15–30]) / 30 en las unidades enteras del asset y la baja en la '
                       'grilla nativa. build_hexoma_suelo.py pasa a unidades físicas: arcilla, arena, limo g/kg ÷ 10 → %; '
                       'COS dg/kg ÷ 10 → g/kg; pH ÷ 10; CIC mmol(c)/kg ÷ 10 → cmol(+)/kg; densidad aparente cg/cm³ ÷ 100 → '
                       'g/cm³; fragmentos cm³/dm³ ÷ 10 → %; N cg/kg ÷ 100 → g/kg.'),
        asignacion_celda='Píxel de 250 m que contiene el centro de la celda (sin interpolar). Sin predicción (agua, ciudad) = NULL.',
        validacion=('Sin validación local en el Hexoma. Control de plausibilidad: arcilla media 49,7 % en Misiones (tierra '
                    'colorada) contra 21,7 % en Corrientes; arena 50 % en Corrientes. 69.297 celdas sin predicción.'),
        limitaciones='Predicción con aprendizaje automático a partir de perfiles: no es una medición en la celda; incertidumbre alta donde hay pocos perfiles.',
        uso_recomendado='Clases de textura, pH y fertilidad a escala de paisaje; umbrales de aptitud por cultivo.',
        uso_no_recomendado='Diagnóstico de un lote o comparación fina entre celdas vecinas.',
        scripts='pipeline/gee_export_suelo_relieve.py; pipeline/hexoma/build_hexoma_suelo.py',
        objetos=['suelo.h3'], verificado=V),
    dict(
        id='fabdem', nombre='FABDEM', proveedor='University of Bristol / Fathom',
        producto='Forest And Buildings removed Copernicus DEM', version='1.2',
        acceso='GEE projects/sat-io/open-datasets/FABDEM (catálogo comunitario)' + GEE,
        licencia='CC BY-NC-SA 4.0 (no comercial: compatible con el Hexoma privado)',
        cita='Hawker, L. et al. 2022. A 30 m global map of elevation with forests and buildings removed. Environmental Research Letters 17: 024016',
        doi='10.1088/1748-9326/ac4d4f', tipo_dato='modelo', res_espacial='1" (~30 m)', res_temporal='una foto',
        periodo='Copernicus GLO-30 (2011–2015)', cobertura='rectángulo del NEA, sólo los bloques con centros de celda',
        variables='elevación (m); pendiente (grados) calculada con ee.Terrain.slope',
        procesamiento=('gee_export_suelo_relieve.py: pendiente a 30 m; pendiente y elevación reducidas a 3" (~90 m) con '
                       'media, más elevación mínima y máxima; bloques de 1500×1500 píxeles. build_hexoma_suelo.py asigna '
                       'cada píxel de 3" a la celda que contiene su centro (DuckDB h3) y promedia (~12 por celda).'),
        asignacion_celda='Media de los píxeles de 3" con centro dentro de la celda (n_px_relieve).',
        validacion='Plausibilidad: Bernardo de Irigoyen 817 m, Posadas 121 m, Ituzaingó 73 m.',
        limitaciones='Quita dosel y edificios con aprendizaje automático: puede dejar artefactos en bordes de selva y plantaciones.',
        uso_recomendado='Pendiente y altitud para aptitud y para leer el clima (heladas de ladera y valle).',
        uso_no_recomendado='Microtopografía de lote o drenaje fino.',
        scripts='pipeline/gee_export_suelo_relieve.py; pipeline/hexoma/build_hexoma_suelo.py', objetos=['suelo.h3'], verificado=V),
    dict(
        id='ghcn_daily', nombre='GHCN-Daily', proveedor='NOAA National Centers for Environmental Information',
        producto='Global Historical Climatology Network – Daily', version='bajado el 1-nov-2026',
        acceso='https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/<id>.dly', licencia='Dato público de NOAA (algunas estaciones bajo la Res. 40 de la OMM)',
        cita='Menne, M.J. et al. 2012. An overview of the Global Historical Climatology Network-Daily database. Journal of Atmospheric and Oceanic Technology 29: 897–910',
        doi='10.1175/JTECH-D-11-00103.1', tipo_dato='medición in situ', res_espacial='puntual (17 estaciones)', res_temporal='diaria',
        periodo='1951–2026 según estación', cobertura='SMN (Posadas, Corrientes, Resistencia, Formosa, Sáenz Peña, Las Lomitas, Paso de los Libres, Monte Caseros, Curuzú Cuatiá, Reconquista, Ceres, Rivadavia), Paraguay (Encarnación, Capitán Meza, Ciudad del Este) y Brasil (Iguazú Aero, Foz do Iguaçu)',
        variables='TMAX, TMIN (décimas de °C), PRCP (décimas de mm)',
        procesamiento=('validar_clima.leer_dly lee el formato fijo; se descartan los valores con bandera de calidad de NOAA. '
                       'build_hexoma_estaciones.py los guarda tal cual en estaciones.diario con el QC propio.'),
        asignacion_celda='Puntual. estaciones.h3_cercana da la estación más cercana a cada celda.',
        validacion='Es la referencia; el QC propio marca saltos y valores fuera de rango (estaciones.diario.qc).',
        limitaciones='Cobertura temporal irregular en el SMN (muchos años incompletos); sin estaciones en las sierras de Misiones.',
        uso_recomendado='Referencia obligada para validar y corregir productos grillados; normales observadas.',
        uso_no_recomendado='Interpolar a celdas lejanas sin un modelo.', scripts='pipeline/hexoma/validar_clima.py; pipeline/hexoma/build_hexoma_estaciones.py',
        objetos=['estaciones.*'], verificado=V),
    dict(
        id='inta_siga', nombre='INTA SIGA, estaciones meteorológicas', proveedor='INTA, Sistema de Información y Gestión Agrometeorológica',
        producto='Datos diarios de estaciones convencionales (EMC) y automáticas (Nimbus THP)', version='bajado el 1-nov-2026',
        acceso='API pública de https://siga.inta.gob.ar (CdnaUV0iiERRpFQE.php?param_type=estacion|diario), la misma de la aplicación',
        licencia='Dato público del INTA; el sitio no publica términos de uso', cita='INTA. SIGA — Sistema de Información y Gestión Agrometeorológica. https://siga.inta.gob.ar',
        doi='', tipo_dato='medición in situ', res_espacial='puntual (35 estaciones del NEA; 33 con datos)', res_temporal='diaria',
        periodo='convencionales desde 1959–1968 (Cerro Azul, Bella Vista, Colonia Benítez, Las Breñas, Sáenz Peña, Corrientes, Mercedes, El Colorado); automáticas desde 2006',
        cobertura='Misiones, Corrientes, Chaco, Formosa',
        variables='tempAbrigo150Min/Max (abrigo a 1,5 m), precDiaPlub (día pluviométrico 9 a 9 h), tempInte5Min (mínima a la intemperie a 5 cm, sólo convencionales)',
        procesamiento=('bajar_siga.py pide un año por estación y guarda el JSON tal cual; arma un CSV por estación '
                       '(fecha, tmin, tmax, p, tmin_suelo) y su JSON de metadatos. build_hexoma_estaciones.py lo carga.'),
        asignacion_celda='Puntual. estaciones.h3_cercana.',
        validacion='Es la referencia. QC propio en estaciones.diario.qc.',
        limitaciones='Las automáticas tienen huecos y series cortas; la convencional de Corrientes y la de El Colorado tienen años incompletos. La altura de J. J. Castelli figura como 0 m.',
        uso_recomendado='Referencia obligada; única fuente de heladas a nivel del suelo; corrección de ERA5-Land.',
        uso_no_recomendado='Tratar una automática con pocos años como normal climática.',
        scripts='pipeline/hexoma/bajar_siga.py; pipeline/hexoma/build_hexoma_estaciones.py', objetos=['estaciones.*'], verificado=V),
    dict(
        id='mapbiomas_ar_col3', nombre='MapBiomas Argentina, Colección 3', proveedor='MapBiomas Argentina (red MapBiomas; en el NEA, FVSA, INTA y universidades)',
        producto='Cobertura y uso del suelo anual, 19 clases', version='Colección 3, v1 (presentada el 24-sep-2026)',
        acceso='GEE projects/mapbiomas-public/assets/argentina/lulc/collection3/mapbiomas_argentina_collection3_coverage_v1, bajado con computePixels en la grilla nativa' + GEE,
        licencia='Datos abiertos de MapBiomas con atribución (verificar los términos de la Colección 3 en argentina.mapbiomas.org)',
        cita='Proyecto MapBiomas Argentina. Colección 3 de la serie anual de mapas de cobertura y uso del suelo de Argentina (1985–2025). https://argentina.mapbiomas.org',
        doi='', tipo_dato='estimación satelital', res_espacial='~30 m (0,000269°)', res_temporal='anual', periodo='1985–2025',
        cobertura='Argentina; en el Hexoma, el NEA argentino',
        variables='classification_<año>: 19 clases (bosque cerrado, abierto e inundable; arbustales; pastizales; plantación forestal; pastura; cultivo temporal; mosaico agricultura-pastura; cultivo perenne; urbano; agua; otras; no observado)',
        procesamiento=('build_hexoma_uso.py baja bloques de 4096² píxeles (sólo los que tienen centros de celda y sus vecinos), un año por '
                       'pedido; cada píxel va a la celda que contiene su centro (DuckDB h3) y se cuenta por clase y año. La serie completa '
                       '(h3, año, clase, píxeles) queda en parquet; Postgres guarda ‰ por grupo de clases cada 5 años y la trayectoria '
                       '1985–2025 de cada celda.'),
        asignacion_celda='Píxeles de 30 m con centro dentro de la celda (~110–130 por celda); fracción = píxeles de la clase / píxeles de la celda.',
        validacion=('‰ por celda suma 1000 en todas las filas. Contra la superficie sembrada del MAGyP (Estimaciones agrícolas 2026-03) por '
                    'departamento, unidos por nombre (los códigos de Chaco y Formosa del Hexoma no son los del MAGyP): cultivo temporal '
                    'frente a cultivos de verano r 0,87–0,95 (75 departamentos, 2000–2025), cociente 1,3–2,3; cultivo perenne frente a '
                    'yerba, té y cítricos r 0,81–0,89, cociente ~3. Sirve para ubicar la agricultura, no para medir la superficie de un '
                    'cultivo. Detalle: uso.validacion y pipeline/hexoma/validacion_uso.md.'),
        limitaciones=('Clasificación automática por año: los cambios de un año a otro incluyen ruido (cambios_dominante lo cuenta). La leyenda '
                      'en castellano es traducción propia de la del asset. «Mosaico de agricultura y pastura» mezcla usos.'),
        uso_recomendado='Trayectoria de uso por celda, validación de la aptitud por cultivo (A4), pérdida de bosque y avance agrícola.',
        uso_no_recomendado='Superficie de un cultivo específico (soja, maíz) sin un mapa por cultivo; cambios de un solo año.',
        scripts='pipeline/hexoma/build_hexoma_uso.py', objetos=['uso.*'], verificado=V),
    # ------------------------------------------------------------------ censos y cruce
    dict(
        id='censo_2022', nombre='Censo Nacional 2022 por radio', proveedor='INDEC; agregado por radio en Source Cooperative (nlebovits/censo-argentino)',
        producto='Censo de Población, Hogares y Viviendas 2022, base REDATAM agregada por radio', version='2022 (definitivo)',
        acceso='https://source.coop/nlebovits/censo-argentino (census-data.parquet, radios.parquet)', licencia='CC BY 4.0 (Source Cooperative)',
        cita='INDEC. 2023. Censo Nacional de Población, Hogares y Viviendas 2022. Agregado por radio: N. Lebovits, Source Cooperative',
        doi='', tipo_dato='censo', res_espacial='radio censal', res_temporal='una fecha (18-may-2022)', periodo='2022',
        cobertura='Misiones, Corrientes, Chaco, Formosa', variables='76 variables (conteos por categoría), NBI e IPMH del INDEC',
        procesamiento=('build_hexoma_censo.py arma el esquema censo_2022: categorias (conteo por radio, variable y categoría), '
                       'indicadores (num/den), radios con geometría de la web. La web usa censo_2022.cruce (edificios); '
                       'el Hexoma, además, cruce.radio_h3 con GHSL en los radios rurales.'),
        asignacion_celda='Por el cruce radio → celda (proceso cruce_radio_h3; censo_2022.cruce para la versión de la web).',
        validacion='Personas por radio iguales a lo publicado (6.882/6.882 radios); NBI, desocupación y tenencia iguales a la web.',
        limitaciones='Sin cruces entre variables (sexo × edad) ni variables de población ocupada; sólo viviendas particulares.',
        uso_recomendado='Indicadores por radio y por conjunto de celdas sumando num y den.', uso_no_recomendado='Promediar tasas de celdas.',
        scripts='pipeline/build_hexoma_censo.py', objetos=['censo_2022.*'], verificado=V),
    dict(
        id='censo_2010', nombre='Censo Nacional 2010 por radio', proveedor='INDEC; Source Cooperative (ampliado) y Poblaciones (básico, base local posadas)',
        producto='Censo 2010: cuestionario ampliado (muestra en localidades > 50 mil) y básico completo', version='2010',
        acceso='Source Cooperative nlebovits/censo-argentino/2010; base posadas censo_2010.nbi_2010 y poblacion_2010 (Poblaciones)',
        licencia='CC BY 4.0 (Source Cooperative); Poblaciones (verificar términos)', cita='INDEC. 2012. Censo Nacional de Población, Hogares y Viviendas 2010',
        doi='', tipo_dato='censo', res_espacial='radio censal 2010', res_temporal='una fecha (27-oct-2010)', periodo='2010',
        cobertura='NEA argentino', variables='124 variables; básico completo con NBI y servicios',
        procesamiento=('build_hexoma_censo.py: los radios con muestra se calibran con el total oficial (factor_muestra); '
                       'los indicadores del básico salen de la tabla completa de Poblaciones (censo_2010.basico).'),
        asignacion_celda='Cruce único con GHSL 2010 o edificios según el tamaño del radio (proceso cruce_radio_h3).',
        validacion='NBI de hogares por provincia igual al INDEC (Misiones 15,6; Corrientes 15,1; Chaco 18,2; Formosa 19,7 %).',
        limitaciones='Muestra en las localidades grandes: tasas ruidosas por radio; sin condición de actividad por radio.',
        uso_recomendado='Panel 1991–2022 por celda; NBI y servicios del básico.', uso_no_recomendado='Tasas de un radio con muestra aislado.',
        scripts='pipeline/build_hexoma_censo.py', objetos=['censo_2010.*'], verificado=V),
    dict(
        id='censo_2001', nombre='Censo Nacional 2001 por radio', proveedor='INDEC; Source Cooperative', producto='Censo 2001, base REDATAM por radio',
        version='2001', acceso='Source Cooperative nlebovits/censo-argentino/2001', licencia='CC BY 4.0', cita='INDEC. 2002. Censo Nacional de Población, Hogares y Viviendas 2001',
        doi='', tipo_dato='censo', res_espacial='radio censal 2001', res_temporal='una fecha (17/18-nov-2001)', periodo='2001',
        cobertura='NEA argentino', variables='81 variables', procesamiento='build_hexoma_censo.py, como 2022.',
        asignacion_celda='Cruce único (proceso cruce_radio_h3).', validacion='NBI por provincia igual al INDEC (23,5; 24,0; 27,6; 28,0 %).',
        limitaciones='Sin procedencia del agua ni combustible.', uso_recomendado='Panel por celda.', uso_no_recomendado='—',
        scripts='pipeline/build_hexoma_censo.py', objetos=['censo_2001.*'], verificado=V),
    dict(
        id='censo_1991', nombre='Censo Nacional 1991 por radio', proveedor='INDEC; microdatos de Open Redatam (pachadotdev/redatam-microdata)',
        producto='Censo 1991, microdatos REDATAM pasados a radio', version='release 2.0.2, ARG1991.zip',
        acceso='https://github.com/pachadotdev/redatam-microdata; radios.parquet 1991 de Source Cooperative', licencia='Microdatos públicos del INDEC (verificar); Source Cooperative CC BY 4.0',
        cita='INDEC. 1991. Censo Nacional de Población y Vivienda 1991', doi='', tipo_dato='censo', res_espacial='radio censal 1991',
        res_temporal='una fecha (15-may-1991)', periodo='1991', cobertura='NEA argentino', variables='114 variables',
        procesamiento='redatam_a_radio.py agrega los microdatos por radio; build_hexoma_censo.py calibra con el total oficial.',
        asignacion_celda='Cruce único con GHSL 1990 (proceso cruce_radio_h3).', validacion='NBI 29,4; 26,3; 33,1; 33,4 % (NBI de la vivienda heredado por el hogar).',
        limitaciones='Muestra en los radios grandes; sin migración usable ni nivel educativo completo; 14 radios sin geometría (160 personas).',
        uso_recomendado='Panel por celda.', uso_no_recomendado='Tasas de radios con muestra aislados.',
        scripts='pipeline/hexoma/redatam_a_radio.py; pipeline/build_hexoma_censo.py', objetos=['censo_1991.*'], verificado=V),
    dict(
        id='uca_inseguridad', nombre='Riesgo de inseguridad alimentaria por radio 2010 (UCA)', proveedor='Observatorio de la Deuda Social, UCA',
        producto='Inseguridad alimentaria por radio censal', version='2010', acceso='archivo de terceros cargado en externos', licencia='verificar',
        cita='Observatorio de la Deuda Social Argentina (UCA)', doi='', tipo_dato='modelo', res_espacial='radio 2010', res_temporal='2010', periodo='2010',
        cobertura='NEA argentino', variables='riesgo de inseguridad alimentaria',
        procesamiento='build_hexoma_externos.py lo carga con su procedencia en externos.fuentes.', asignacion_celda='Por radio 2010.',
        validacion='No validado en el Hexoma.', limitaciones='Armado con variables del censo 2010: no es independiente del NBI de ese año.',
        uso_recomendado='Contraste con el NBI.', uso_no_recomendado='Como dato independiente del censo.',
        scripts='pipeline/hexoma/build_hexoma_externos.py', objetos=['externos.*'], verificado=V),
    dict(
        id='ghsl_r2023a', nombre='GHS-BUILT-S R2023A', proveedor='Comisión Europea, JRC (Global Human Settlement Layer)',
        producto='Superficie construida por época', version='R2023A', acceso='GEE JRC/GHSL/P2023A/GHS_BUILT_S/<época>' + GEE,
        licencia='CC BY 4.0', cita='Pesaresi, M. et al. 2023. GHSL Data Package 2023. Publications Office of the EU',
        doi='10.2760/098587', tipo_dato='estimación satelital', res_espacial='100 m', res_temporal='épocas 1990, 2000, 2010, 2020',
        periodo='1990–2020', cobertura='rectángulo de los radios del NEA', variables='built_surface (m² construidos por píxel)',
        procesamiento='gee_export_ghsl_epochs.py exporta las cuatro épocas; build_hexoma_cruce.py asigna cada píxel a la celda de su centro.',
        asignacion_celda='Píxel de 100 m a la celda que contiene su centro.', validacion='Elegido por validación del cruce (proceso cruce_radio_h3).',
        limitaciones='Detecta mal la vivienda rural dispersa bajo dosel.', uso_recomendado='Peso de época para repartir radios.',
        uso_no_recomendado='Conteo de viviendas.', scripts='pipeline/gee_export_ghsl_epochs.py; pipeline/hexoma/build_hexoma_cruce.py',
        objetos=['cruce.celda_pesos', 'cruce.celda_geom'], verificado=V),
    dict(
        id='edificios_web', nombre='Edificios del cruce de la web (Global Building Atlas y Open Buildings)', proveedor='GBA (TUM); Google Open Buildings; vida_buildings',
        producto='Huellas de edificios usadas por el cruce dasimétrico de la web', version='la de los crosswalks de la web (~2023)',
        acceso='Parquets de crosswalk de pipeline/output (build_dasymetric_crosswalk.py, build_crosswalk_ar.py)',
        licencia='GBA CC BY-NC; Open Buildings CC BY 4.0 / ODbL', cita='Global Building Atlas; Sirko, W. et al. 2021, Continental-scale building detection (Google)',
        doi='', tipo_dato='estimación satelital', res_espacial='huella de edificio', res_temporal='~2023', periodo='~2023',
        cobertura='NEA argentino', variables='edificios por celda', procesamiento='build_dasymetric_crosswalk.py (Misiones: GBA + vida_buildings), build_crosswalk_ar.py (resto).',
        asignacion_celda='Centro del edificio en la celda.', validacion='Validación del cruce (proceso cruce_radio_h3): mejor en radios urbanos.',
        limitaciones='En el campo cuenta galpones y secaderos como viviendas; GBA tiene un hueco en el este de Misiones (por eso la unión con vida_buildings).',
        uso_recomendado='Peso urbano del cruce.', uso_no_recomendado='Repartir población de 1991.',
        scripts='pipeline/build_dasymetric_crosswalk.py; pipeline/build_crosswalk_ar.py', objetos=['censo_2022.cruce'], verificado=V),
    # ------------------------------------------------------------------ series anuales e institucionales (6-nov-2026)
    dict(
        id='hansen_gfc_v113', nombre='Hansen Global Forest Change v1.13, pérdida anual 2001–2025',
        proveedor='University of Maryland (GLAD) / Google', producto='Global Forest Change 2000–2025',
        version='v1.13 (UMD/hansen/global_forest_change_2025_v1_13), la misma de la capa web de deforestación y de EUDR',
        acceso='GEE, bandas treecover2000 y lossyear en la grilla nativa con ee.data.computePixels (bloques de 4096²)' + GEE,
        licencia='CC BY 4.0', cita='Hansen, M. C. et al. 2013. High-resolution global maps of 21st-century forest cover change. Science 342: 850–853',
        doi='10.1126/science.1244693', tipo_dato='estimación satelital',
        res_espacial='1″ (~30 m; ~129 píxeles por celda res-9)', res_temporal='anual', periodo='2000 (dosel) y 2001–2025 (pérdida)',
        cobertura='NEA argentino (las 3.298.621 celdas de hexagonos)',
        variables='treecover2000 (% de dosel), lossyear (año de pérdida de reemplazo de dosel)',
        procesamiento=('build_hexoma_hansen.py: cada píxel va a la celda que contiene su centro (DuckDB h3); por celda se cuentan '
                       'píxeles, píxeles con dosel ≥ 10 y ≥ 30 %, suma del dosel y píxeles perdidos por año (todos, y dentro de '
                       'dosel ≥ 10 y ≥ 30 %). ha = píxeles × área de la celda / píxeles de la celda.'),
        asignacion_celda='Centro del píxel en la celda; sin interpolación.',
        validacion='Totales 2001–2025 por provincia a ±0,4 % de un reduceRegion de GEE sobre la provincia (FAO GAUL 2015; por año, ±2,4 % como máximo, Formosa); contra c_loss_rate de la capa web r = 0,9985 (2016–2025) y 0,9983 (2001–2010) en 3,29 millones de celdas. pipeline/hexoma/validacion_hansen.md.',
        limitaciones=('«Pérdida» es reemplazo de dosel: incluye cosecha de plantaciones y no distingue causa; esta versión no trae '
                      'ganancia por año; los cambios de algoritmo a lo largo de la serie (los autores lo advierten) la hacen no del '
                      'todo homogénea.'),
        uso_recomendado='Series de pérdida por celda, departamento o zona; fronteras de pérdida con vec.frontera; EUDR (después de 2020).',
        uso_no_recomendado='Separar nativo de plantación sin otra fuente (cruzar con uso.h3_anio de MapBiomas).',
        scripts='pipeline/hexoma/build_hexoma_hansen.py', objetos=['series.hansen_*', 'series.v_hansen_*'], verificado=V),
    dict(
        id='otbn_imibio_2017', nombre='OTBN de Misiones, actualización 2017',
        proveedor='Gobierno de Misiones, Ministerio de Ecología y RNR / IMIBIO',
        producto='Ordenamiento Territorial de Bosques Nativos (Ley 26.331; Ley XVI Nº 105)',
        version='Actualización aprobada por Res. 265/2017 del MEyRNR, acreditada por la Res. RESOL-2021-2-APN-SPARN#MAD',
        acceso='Shapefile otbn_misiones_full.shp (OneDrive/gee/misiones/otbn; ver su README_otbn.md), copiado a %HEXOMA_FUENTES%/institucional',
        licencia='Dato público provincial; citar a la autoridad de aplicación',
        cita='Provincia de Misiones (2017). Actualización del OTBN, Res. 265/2017 MEyRNR', doi='—', tipo_dato='registro administrativo',
        res_espacial='vector (109.378 polígonos derivados de ráster)', res_temporal='estático (2017)', periodo='2017',
        cobertura='Misiones', variables='cat_cons: I (rojo), II (amarillo), III (verde)',
        procesamiento='Join espacial de los polígonos (ST_Subdivide) con el polígono de cada celda; % de la celda = área geodésica de la intersección / área de la celda.',
        asignacion_celda='% de la superficie de la celda en cada categoría (institucional.otbn_h3).',
        validacion='Polígonos por categoría iguales a la superficie acreditada por la Res. 2/2021 (I 233.076 contra 233.083 ha; II 901.532 contra 901.617; III 477.755 contra 477.858); suma en celdas a ±0,1 % de los polígonos. pipeline/hexoma/validacion_institucional.md.',
        limitaciones='La compilación nacional de la SAyDS trae para Misiones el OTBN de 2010, ya reemplazado: no usarla en Misiones.',
        uso_recomendado='Restricción legal al desmonte por celda; cruce con pérdida Hansen o MapBiomas posterior a 2017.',
        uso_no_recomendado='Como mapa de bosque actual (es zonificación, no cobertura).',
        scripts='pipeline/hexoma/build_hexoma_institucional.py', objetos=['institucional.otbn*', 'institucional.v_h3'], verificado=V),
    dict(
        id='otbn_sayds_2017', nombre='OTBN, compilación nacional SAyDS (nov-2017)',
        proveedor='Secretaría de Ambiente y Desarrollo Sustentable de la Nación, Dirección de Bosques',
        producto='OTBN_SIG250nov2017 (coberturas de los OTBN provinciales compiladas a escala 1:250.000)',
        version='nov-2017', acceso='WFS https://geo2.ambiente.gob.ar/geoserver/ows, capa ordenamiento:OTBN_SIG250nov2017',
        licencia='Dato público nacional; citar a la SAyDS', cita='SAyDS (2017). OTBN_SIG250nov2017', doi='—',
        tipo_dato='registro administrativo', res_espacial='vector 1:250.000', res_temporal='estático', periodo='OTBN vigentes a nov-2017',
        cobertura='Corrientes, Chaco y Formosa (en el Hexoma)', variables='cat_cons I/II/III, periodo, provincia',
        procesamiento='Igual que otbn_imibio_2017.', asignacion_celda='% de la celda en cada categoría (institucional.otbn_h3).',
        validacion='Suma en celdas contra los polígonos: Corrientes ±0,6 %, Chaco −0,2 a +3,8 % (cat. I), Formosa −5,8 % (cat. I) a +1,1 %; las diferencias de la cat. I salen de polígonos de borde entre provincias y con el Paraguay. No hay superficie acreditada a mano para contrastar. pipeline/hexoma/validacion_institucional.md.',
        limitaciones=('La SAyDS avisa post-procesamiento por falencias técnicas: no reproduce exactamente el OTBN aprobado por cada '
                      'provincia. No incluye actualizaciones posteriores a 2017.'),
        uso_recomendado='Ídem otbn_imibio_2017, con la advertencia de versión.', uso_no_recomendado='Como fuente legal de una parcela.',
        scripts='pipeline/hexoma/build_hexoma_institucional.py', objetos=['institucional.otbn*'], verificado=V),
    dict(
        id='areas_protegidas_sayds', nombre='Áreas protegidas nacionales, provinciales y sitios Ramsar',
        proveedor='SAyDS (SIFAP) y Administración de Parques Nacionales', producto='apn_sifal2016, app_polig_2016b, sitios_ramsar',
        version='2016 (nacionales y provinciales); Ramsar sin fecha de versión en el servicio',
        acceso='WFS https://geo2.ambiente.gob.ar/geoserver/ows', licencia='Dato público nacional; citar a la SAyDS/SIFAP',
        cita='SIFAP (2016). Sistema Federal de Áreas Protegidas', doi='—', tipo_dato='registro administrativo',
        res_espacial='vector', res_temporal='estático', periodo='2016', cobertura='las que tocan el NEA argentino',
        variables='nombre, categoría, UICN, provincia, creación, instrumento, superficie declarada',
        procesamiento='Igual que el OTBN; un registro por celda y área (las áreas pueden superponerse).',
        asignacion_celda='% de la celda dentro de cada área (institucional.ap_h3).', validacion='Polígono contra superficie declarada: la mayoría dentro de ±10 %; discrepancias de la fuente en Pampa del Indio (declara 8.633 ha, el polígono 22.286), Laguna Brava (9.031 contra 80) y Zanjón de Loreto (2 contra 1.214); en celdas quedan recortadas las que salen del NEA (Jaaukanigas, Santa Fe) o caen en el río (Apipé Grande: 114 de 4.707 ha). pipeline/hexoma/validacion_institucional.md.',
        limitaciones='Versión 2016: faltan áreas creadas después; algunos nombres llegan con caracteres perdidos («?») en el servicio.',
        uso_recomendado='Exclusión o contraste dentro/fuera de áreas protegidas.', uso_no_recomendado='Como límite legal exacto.',
        scripts='pipeline/hexoma/build_hexoma_institucional.py', objetos=['institucional.areas_protegidas', 'institucional.ap_h3'], verificado=V),
    dict(
        id='inai_comunidades', nombre='Comunidades indígenas con relevamiento y/o personería (INAI)',
        proveedor='Instituto Nacional de Asuntos Indígenas (datos.mininterior.gob.ar)',
        producto='Listado de comunidades indígenas con relevamiento con resolución y/o personería jurídica registrada',
        version='2025-10 (archivo del 12-nov-2025)', acceso='CSV de datos.mininterior.gob.ar (ver institucional.fuentes)',
        licencia='Datos abiertos del Estado nacional', cita='INAI (2025). Listado de comunidades indígenas', doi='—',
        tipo_dato='registro administrativo', res_espacial='punto aproximado (cuando lo trae)', res_temporal='estático',
        periodo='oct-2025', cobertura='Misiones, Corrientes, Chaco, Formosa',
        variables='comunidad, pueblo, departamento, localidad, zona, coordenadas, personería, estado y resolución del relevamiento (Ley 26.160)',
        procesamiento='Filtro por provincia; punto → celda que lo contiene; control de que la celda sea de su provincia.',
        asignacion_celda='Celda del punto (institucional.comunidades.h3).', validacion='De 388 comunidades del NEA, 132 traen coordenadas (Misiones 91 de 123, Chaco 40 de 111, Formosa 1 de 150, Corrientes 0 de 4) y las 132 caen en una celda de su provincia. pipeline/hexoma/validacion_institucional.md.',
        limitaciones='Coordenadas incompletas y aproximadas; un punto no es el territorio de la comunidad (los polígonos del relevamiento no son públicos).',
        uso_recomendado='Presencia y conteo por zona; distancia aproximada.', uso_no_recomendado='Delimitar territorios indígenas.',
        scripts='pipeline/hexoma/build_hexoma_institucional.py', objetos=['institucional.comunidades'], verificado=V),
    dict(
        id='red_vial', nombre='Red vial del NEA argentino (OSM + Microsoft ML Road Detections; IGN de referencia)',
        proveedor='OpenStreetMap (Geofabrik); Microsoft (source.coop); Instituto Geográfico Nacional',
        producto='Extracto Argentina de OSM; ML Road Detections; vial_nacional/provincial/terciaria del IGN',
        version='Bajados el 4-nov-2026', acceso='pipeline/vial/importar_vial.py (PBF de Geofabrik, GeoParquet de source.coop, WFS IGN)',
        licencia='ODbL (OSM y Microsoft); IGN uso libre con cita',
        cita='OpenStreetMap contributors; Microsoft (2022) Global ML Road Detections; IGN Red vial', doi='—',
        tipo_dato='registro colaborativo y estimación por imagen', res_espacial='vector', res_temporal='estático (nov-2026)',
        periodo='2026', cobertura='Misiones, Corrientes, Chaco, Formosa',
        variables='clase y superficie normalizadas (pavimento, ripio/consolidado, tierra, desconocida), departamento INDEC',
        procesamiento=('OSM es la base; de Microsoft sólo entra lo que queda a más de ~25 m de OSM; IGN queda como tabla de '
                       'referencia. red_vial.py arma el grafo (densificado a 50 m, empalmes ≤ 60 m) y km por la red al pavimento.'),
        asignacion_celda='No hay asignación a celda: vial es una red; se cruza por geometría.',
        validacion='Zonas de prueba (commit 35c2947): Eldorado–San Pedro OSM 8.087 km + Microsoft 81; Corrientes NE Microsoft 1.209 km (OSM 991).',
        limitaciones='Superficie inferida donde OSM no la trae (troncal sin etiqueta = pavimento, huella = tierra); Microsoft sin clase.',
        uso_recomendado='Distancias por la red, densidad de caminos, acceso a pavimento.', uso_no_recomendado='Estado real del camino.',
        scripts='pipeline/vial/importar_vial.py; pipeline/vial/red_vial.py; pipeline/vial/distancia_macizos.py', objetos=['vial.*'], verificado=V),
    dict(
        id='visor_emergencias_misiones', nombre='Visor de emergencias de Misiones (capas de infraestructura)',
        proveedor='Gobierno de Misiones, Dir. de Modernización de la Gestión',
        producto='GeoJSON estáticos de sig.misiones.gob.ar/mapas/emergencia/datos/ (código: gitlab.com/Modernizacion.Misiones/mapa-emergencia)',
        version='Bajados el 7-nov-2026', acceso='pipeline/infra/importar_visor.py (HTTP directo, sin clave)',
        licencia='—', cita='Gobierno de Misiones (2026). Mapa de emergencias', doi='—',
        tipo_dato='registro administrativo y colaborativo (casi todo derivado de OSM)', res_espacial='vector',
        res_temporal='estático (los hidrómetros traen la última semana)', periodo='2026', cobertura='Misiones',
        variables=('puentes (OSM, con Mapillary); 29 puentes de la RP2 con cota de seguridad en msnm (propios); '
                   'alcantarillas; cauces; líneas eléctricas 13,2–500 kV; 73 transformadoras; antenas; hospitales y CAPS '
                   'con códigos SISA/CUIE; 78 municipios con zona de gestión del riesgo; 35 hidrómetros con niveles de '
                   'alerta y evacuación y series_id del INA'),
        procesamiento='Una tabla infra.visor_<capa> por archivo (props jsonb + columnas tipadas donde se usan); geometría a 2D.',
        asignacion_celda='No hay asignación a celda: se cruza por geometría.',
        validacion='Conteos iguales a los GeoJSON (infra.fuentes). Puntos RP2 sobre el camino (mediana 0 m, máx. 6 m).',
        limitaciones='Sin fecha de relevamiento por objeto; la mayoría de las capas copia OSM; alturas de río sin cero de escala en msnm.',
        uso_recomendado='Evidencia de estructuras en cruces, cotas de corte de la RP2, red eléctrica troncal, salud.',
        uso_no_recomendado='Inventario completo de puentes o alcantarillas (cubre una parte).',
        scripts='pipeline/infra/importar_visor.py', objetos=['infra.visor_*', 'infra.fuentes'], verificado='2026-11-07'),
    dict(
        id='osm_cauces', nombre='Cauces, alcantarillas y vados de OpenStreetMap (Misiones)',
        proveedor='OpenStreetMap (Geofabrik)', producto='Extracto Argentina (argentina-latest.osm.pbf)',
        version='pbf del 6/7-nov-2026', acceso='pipeline/infra/cauces_osm.py (ogr2ogr de QGIS sobre el pbf; Overpass dio 502/504)',
        licencia='—', cita='OpenStreetMap contributors', doi='—',
        tipo_dato='registro colaborativo', res_espacial='vector', res_temporal='estático (nov-2026)', periodo='2026',
        cobertura='Misiones + ~5 km (recorte de drenaje.py)',
        variables='waterway (river, stream, canal, drain, ditch), nombre, intermitente, alcantarilla (tunnel=culvert); vados (ford=yes, nodos y vías)',
        procesamiento='Líneas con waterway y nodos con ford=yes del pbf; vados que son vías, desde vial.osm.',
        asignacion_celda='No hay asignación a celda.',
        validacion='7.136 tramos de arroyo (10.692 km) en el recorte; 2.032 alcantarillas y 835 vados.',
        limitaciones='Cobertura despareja: la red de terreno (FABDEM, ≥ 0,1 km²) tiene muchos más cauces que OSM.',
        uso_recomendado='Nombre del cauce, evidencia de estructura, ubicación precisa de cruces.',
        uso_no_recomendado='Red hidrográfica completa.',
        scripts='pipeline/infra/cauces_osm.py', objetos=['infra.osm_cauces', 'infra.osm_estructuras'], verificado='2026-11-07'),
    dict(
        id='ina_escalas_prefectura', nombre='Alturas diarias del río Uruguay (escalas de Prefectura, INA)',
        proveedor='Instituto Nacional del Agua (red «escalas Prefectura Nacional»)',
        producto='API a5: /obs/puntual/observaciones?series_id=60,61,63,64,65,66,67',
        version='Bajadas el 7-nov-2026', acceso='pipeline/infra/rio_uruguay.py (API pública, sin clave)',
        licencia='—', cita='INA (2026). Sistema de información y alerta hidrológico', doi='—',
        tipo_dato='medición (lectura manual diaria de escala)', res_espacial='punto (hidrómetro)', res_temporal='diaria',
        periodo='1950–2026 según la estación', cobertura='río Uruguay, Pepirí Miní – Garruchos',
        variables='altura hidrométrica (m sobre el cero de la escala)',
        procesamiento='Máximo del día; QC que marca sin borrar (fuera de rango, pico de un día > 10 m en el mismo sentido).',
        asignacion_celda='No hay asignación a celda.',
        validacion='Ver rio_uruguay_puentes.',
        limitaciones='Sin cero de escala en msnm en la API; una lectura por día (subestima el pico); huecos en Pepirí Miní, Panambí y Puerto Concepción.',
        uso_recomendado='Frecuencia de crecidas, eventos históricos.', uso_no_recomendado='Hora del pico.',
        scripts='pipeline/infra/rio_uruguay.py', objetos=['infra.rio_lecturas', 'infra.rio_estaciones'], verificado='2026-11-07'),
]

# ------------------------------------------------------------------ capas de la web (foto en public.capa_*)
# Fuente: relevamiento del código (config.ts, content/methodology.ts, scripts de pipeline/), 1-nov-2026.
CAPAS = {
    'accessibility': ('Accesibilidad', 'Oxford/MAP fricción motorizada 2019 v5.1 y accessibility_to_cities_2015; IGN capa salud_020801 (IGN/SISA, establecimientos públicos); Mapa Educativo Nacional publico:establecimiento_educativo (DIEE); OSM (Overpass) para rutas troncales',
                      'Todo en minutos por camino (5-nov-2026). Costo mínimo multiorigen (MCP_Geometric) sobre la fricción de un solo ráster NEA + 0,5°, reproyectada (vecino) a LAEA de 500 m; la celda lee el píxel de su centro. Destinos: capital; hospital (gna «Hospital…») y salud (todos) del IGN/SISA; escuela con secundaria común o técnica (nen); vértices de ruta motorway/trunk/primary/secondary. Ciudad ≥ 50k: ráster Oxford 2015 en el centroide. Sin normalización, puntaje ni tipos.',
                      'travel_min_capital_raw, travel_min_cabecera_raw, travel_min_hospital_raw, travel_min_salud_raw, travel_min_secundaria_raw, travel_min_ruta_raw (min)',
                      True, '«Cabecera» es la ciudad de ≥ 50.000 hab. (superficie 2015); velocidad de ruta libre, sin tránsito, estado del camino ni colectivo; red 2019; píxel de ~1 km; salud sólo pública; hospital, salud y secundaria sólo en Argentina (NULL en Paraguay); el ranking describe superficie, no población.',
                      'fetch_access_facilities.py; gee_export_location_value.py --territory nea; compute_accessibility_h3.py'),
    'agri_potential': ('Potencial agrícola (web, unidades físicas y aptitud por cultivo)',
                       'Hexoma: CHIRPS v3 (lluvia), ERA5-Land corregido con 48 estaciones GHCN-Daily e INTA SIGA (temperatura, heladas), SoilGrids 2.0 0–30 cm, FABDEM; aptitud A4 con umbrales validados (meta.umbrales_cultivo)',
                       'Exportado del Hexoma por pipeline/hexoma/exportar_agri_web.py (1-nov-2026): por celda, normales 1991–2020 del píxel que contiene el centro y, por cultivo, clase mediana S1–N y % de campañas S1+S2 1991/92–2024/25 (FAO, factor más limitante). Sin puntajes. Reemplaza a la versión 0–100 (retirada y borrada el 1-nov-2026; respaldo fuera del repo).',
                       'p_normal_mm, balance_normal_mm (mm); gdd_normal (°C·d); heladas_normal, heladas5_normal, calor35_normal (días/año); ph; arcilla_pct, pendiente_pct (%); elev_m (m); apt_<cultivo>_clase (1 S1 … 4 N); apt_<cultivo>_pct (% de campañas)', True,
                       'Sólo NEA argentino; SoilGrids es modelo, no medición; arroz sin dato de agua de riego; días > 35 °C corregidos sobreestiman ~5 días/año frente a estaciones (hasta 11).',
                       'pipeline/hexoma/exportar_agri_web.py (desde clima, suelo y aptitud del Hexoma)'),
    'censo_temporal': ('Población 1991–2022', 'Totales de recuento completo INDEC 1991, 2001, 2010, 2022 (Source Cooperative radios.parquet); cartografía de radios Rodríguez & de Grande (CONICET)',
                       'Reparto areal sobre el universo H3 poblado, pesos renormalizados por radio; control contra totales INDEC; densidad = conteo / área; actual 2022, base 2010.',
                       'pob_dens, viv_dens (hab/km², viv/km²) con _baseline, _delta y columnas por año', False,
                       'Densidad 2022 distinta de la capa dasimétrica; sólo NEA argentino.', 'build_censo_temporal.py'),
    'deforestation_dynamics': ('Dinámica de deforestación', 'Hansen GFC v1.13 (2001–2025), 30 m',
                               'Zonal por polígono: píxeles de pérdida por año; tasa = perdidos / píxeles / años × 100 (base 2001–2010, actual 2016–2025); filtro treecover2000 > 5 %. Pérdida como % del bosque (5-nov-2026, add_deforest_hex_derived.py) = 100 · c_loss_rate / c_treecover, sólo donde c_treecover ≥ 10 %. Sin puntaje ni tipos desde 4-nov-2026.',
                               'c_loss_rate (%/año), c_cumulative (%), c_loss_rate_baseline, c_loss_rate_delta, c_treecover, c_loss_pct_forest (+ _baseline, _delta; %/año del bosque 2000)', True,
                               'Hansen cuenta plantaciones como bosque (parte de la pérdida es cosecha de pino y eucalipto); c_loss_pct_forest puede pasar de 100 donde se plantó después de 2000. La serie anual por celda está en series.hansen_h3_anio.',
                               'gee_export_hansen_loss.py; process_hansen_to_h3.py; add_deforest_hex_derived.py; deforestation-update.yml (abril y octubre)'),
    'economic_activity': ('Actividad económica', 'Censo 2022 (actividad, nivel educativo); VIIRS DNB VCMSLCFG avg_rad 500 m (año no determinado: 2024 en el script, 2025 en el texto); Global Building Atlas (+ vida_buildings en Misiones)',
                          'Tasas censales Σw·num / Σw·den con base mínima 30; VIIRS bilineal en el centroide; densidad edilicia = edificios / área. Por celda (5-nov-2026, add_econ_hex_derived.py): tasa_desocupacion = 100 · (actividad − empleo) / actividad y ocupados_est = tasa_empleo · pob_14_mas. Transiciones (4-nov-2026, build_census_trayectorias.py): pct_universitario_2010 = Censo 2010 repartido con los edificios 2022 (celda partida entre radios 2010 por área); viirs_2014, viirs_2014_neto y viirs_2025_neto de VCMSLCFG, neto = radiancia − percentil 10 de la provincia en cada año.',
                          'tasa_empleo, tasa_actividad, tasa_desocupacion, pct_universitario, pct_universitario_2010 (%), ocupados_est (personas), viirs_mean_radiance, viirs_2014, viirs_2014_neto, viirs_2025_neto (nW/cm²/sr), building_density_per_km2', False,
                          'Radios con < 30 en la base quedan sin cobertura; ocupado = 1 h la semana anterior (incluye ayuda familiar sin pago); el fondo de VIIRS pasó de 0,06 a 0,34 nW/cm²/sr entre 2014 y 2025.', 'gee_export_viirs.py; compute_viirs_buildings_h3.py; build_census_rates.py; build_census_trayectorias.py; add_econ_hex_derived.py'),
    'education_capital': ('Capital educativo', 'Censo 2022 censo_niveducativo_2022; Censo 2010 poblacion_2010 (mismas categorías)', 'Tasas censales sobre 18+, base común (build_census_rates.py). Transiciones (4-nov-2026, build_census_trayectorias.py --layer education_capital): las cuatro tasas en 2010, repartidas con los edificios 2022 (celda partida entre radios 2010 por área; radios 2010 sin edificios, por área).',
                          'c_no_schooling, c_secondary_plus, c_higher_edu, c_university (% sobre pob_18_mas) y las mismas con _2010', True,
                          'Comparable sólo entre provincias argentinas (INDEC); hasta oct-2026 se duplicaban universitarios en Corrientes, Chaco y Formosa.', 'build_census_rates.py'),
    'education_flow': ('Trayectoria educativa', 'Censo 2022 (no asistencia, fecundidad)', 'Tasas censales, base común; c_teen_pregnancy con base mínima 1 sólo para el ranking.',
                       'c_dropout_primary (% de 6–12), c_dropout_secondary (% de 13–18), c_teen_pregnancy', True,
                       'Base de mujeres de 14–17 muy chica por radio (mediana 19).', 'build_census_rates.py'),
    'eudr': ('Señales EUDR', 'Hansen GFC v1.13; MODIS MCD64A1 500 m; MapBiomas Argentina Col. 2; corte 31-dic-2020',
             'Pérdida post-2020 (lossyear > 20) a 100 m; píxel de 100 m a la celda de su centro (~9 por celda); fuego sobre nativa leñosa (clases MapBiomas 3–6) en paso manual; sin risk_score desde 4-nov-2026.',
             'loss_post_2020_pct, fire_native_post_2020_pct, fire_post_2020_pct, forest_cover_2020, forest_cover_current (%)', False,
             'Señales indicativas, no veredictos; la pérdida no distingue causa; la cobertura actual no cuenta regeneración.',
             'gee_deforestation_eudr.py; aggregate_eudr_hires.py; apply_fire_native_to_risk.py; eudr-update.yml (manual)'),
    'flood_risk': ('Inundación', 'JRC Global Surface Water v1.4 (Landsat 1984–2021); Sentinel-1 GRD (VV < −15 dB); MERIT Hydro HAND; Google Open Buildings Temporal 2016–2023',
                   'JRC por polígono (zonal_stats_rasterio); extensión actual = mediana de 12 días de S1 descendente, 30 → 90 m, centros de píxel → celda; sin puntaje compuesto desde 4-nov-2026; ranking con HAND < 5 m.',
                   'jrc_occurrence, jrc_recurrence (%), jrc_seasonality (meses), flood_extent_pct, low_bld_pct (% de edificios con HAND < 5 m), low_bld_n, low_land_pct, low_bld_new_n', True,
                   'Landsat no ve crecidas cortas bajo nubes; S1 sobreestima agua en la llanura chaqueña; mide lo construido en zona baja, no la frecuencia de inundación.',
                   'gee_flood_detection.py; process_to_h3.py; update_flood_current.py; build_flood_rank.py; flood-update.yml'),
    'forestry_aptitude': ('Aptitud forestal', 'Presencia: Inventario Nacional de Plantaciones DNDFI 2026; covariables ERA5-Land mensual (0,1°), CHIRPS v2 (0,05°), TerraClimate (~4 km), SoilGrids (250 m), HWSD, SRTM (30 m, exportado a 90 m), NDVI, tiempo a ciudad; el clima se lee en el píxel que contiene el centro de la celda',
                          'Random Forest (400 árboles, hoja mínima 20, clases balanceadas), presencia = fracción plantada ≥ 0,10, fondo 1:3, GroupKFold 5; score = predict_proba × 100; k-means k=4 sobre score ≥ 40. El texto describe dos nichos (comercial AUC 0,90; nativo 0,78) pero el script _ardndfi entrena uno.',
                          'score (0–100), type_label, gdd (°C·d), precip_total (mm), water_deficit (mm), slope_mean (°), clay (%), soc (g/kg)', True,
                          'Fuera de Misiones y Corrientes es extrapolación; no se ve paso de calibración aunque el texto lo dice.',
                          'compute_forestry_sdm.py; compute_forestry_sdm_ardndfi.py; run_forestry_sdm_pipeline.py'),
    'health_access': ('Acceso a salud', 'Malaria Atlas Project, accesibilidad a salud 2019 y fricción 2019 v5.1; OSM; censo 2022 (cobertura, edad, NBI)',
                      'Tiempo a salud = mínimo entre Oxford y OSM, por radio (media de píxeles de 928 m); al hexágono por radio; tasas censales con base común.',
                      'c_healthcare_time (min), c_health_coverage, c_elderly, c_children (%), c_nbi (%), c_pop_density (hab/km²)', True,
                      'Huecos donde el centro de salud falta en las dos fuentes.', 'build_oxford_radios.py; build_census_rates.py'),
    'land_use': ('Uso del suelo', 'MapBiomas Argentina Colección 3 (Landsat, 30 m), 1985–2025; esquema uso del Hexoma',
                 'Píxeles por clase y celda cada 5 años (uso_h3_anio_clase.parquet) → % de superficie en 10 clases; clase del mapa = la que cubre ≥ 50 % entre 8 grupos, si no «mosaico»; trayectoria = clase 1985 frente a 2025; resúmenes por depto con saldo neto de monte y serie de ha.',
                 'pct_monte, pct_plantacion, pct_perenne, pct_cultivo, pct_pastura, pct_pastizal, pct_estero, pct_urbano, pct_agua, pct_otro, pct_agro (%); dom_2025, dom_1985 (clase 1–9), tray (1–8)', True,
                 'Yerba y té juntos; perenne ×2,8–3,3 la superficie del MAGyP; plantación ×1,46 el DNDFI en Misiones; lotes de 1–3 ha no se resuelven; sólo NEA argentino.', 'hexoma/build_hexoma_uso.py; hexoma/exportar_uso_web.py; hexoma/validacion_uso_web.md'),
    'pm25_drivers': ('PM2.5', 'ACAG V6.GL.02 (van Donkelaar et al. 2021), 0,01°, 1998–2022',
                     'Export a 100 m; centroide a H3; medias 2001–2010 y 2013–2022; sin puntaje desde 4-nov-2026; tipos por cortes OMS 2021 (10/15/25/35); conductores SHAP de un modelo a res-7.',
                     'c_pm25_mean (µg/m³), _baseline, _delta, type, conductores c_fire, c_climate, c_terrain, c_vegetation', True,
                     'Dato modelado sin estaciones de validación en el NEA; la visualización es más fina que el dato.',
                     'gee_export_pm25_annual.py; process_pm25_annual_to_h3.py; compute_pm25_drivers.py; pm25-update.yml'),
    'service_deprivation': ('Carencias de servicios', 'Censo 2022 censo_nbi_2022', 'Tasas censales, base común; sin k-means.',
                            'c_nbi, c_sin_cloacas, c_piso, c_hacinamiento, c_hacinamiento_crit, c_sin_computadora (% de hogares)', True,
                            'Comparable sólo con censos INDEC.', 'build_census_rates.py'),
    'sociodemographic': ('Perfil sociodemográfico', 'Censo 2022 (NBI, nivel educativo)', 'Tasas censales; tipos por PCA (80 % de varianza) y k-means 3–5 sobre percentiles provinciales.',
                         'densidad_hab_km2, pct_nbi, pct_hacinamiento, pct_propietario, pct_computadora (%), tamano_medio_hogar', False,
                         'Densidad de la celda, no del radio.', 'build_census_rates.py; aggregate_radio_to_h3.py'),
}
COMUN_CENSAL = ('Base común de las capas censales (build_census_rates.py): conteos por radio de censo_2022 en la base posadas; '
                'estimador de razón por celda Σw·num / Σw·den con pesos del cruce dasimétrico por edificios; radios con '
                'base < 30 descartados; corte si el total de celdas difiere > 0,5 pts del provincial; sin score compuesto.')

# Soporte de cada columna de las capas de la web: el área sobre la que el dato se midió de verdad, no la celda donde se
# guarda (todas viven en res-9, ~0,105 km²). Diagnóstico del 4-nov-2026 (celdas iguales a todas sus hermanas res-8):
# tasas censales 73–79 %, gdd 87 %, precipitación 77 %; Hansen, JRC, VIIRS y SoilGrids < 1 %.
# Por capa, (patrón fnmatch de columna, soporte); gana el primero que calza. build_hexoma_meta.py lo carga en
# meta.variables.soporte y falla si una columna de capa_* queda sin soporte.
RADIO = 'radio censal 2022 (la celda de un solo radio lleva su tasa; la de borde, la mezcla ponderada por edificios)'
REPARTO = 'radio censal 2022 repartido en las celdas por edificios (dasimétrico: la variación dentro del radio es del modelo)'
RADIO_2010 = ('radio censal 2010 repartido en las celdas con los edificios 2022 (celda partida entre radios 2010 por '
              'área; build_census_trayectorias.py)')
EDIFICIO = 'huella de edificio (Global Building Atlas y Open Buildings), sumada en la celda'
DERIVADO = 'celda res-9, derivado de las otras columnas de la capa (su soporte es el más grueso de sus insumos)'
SOPORTES = {
    'accessibility': [('travel_min_cabecera*', 'píxel Oxford/MAP accessibility_to_cities_2015 (30″, ~1 km) en el centro de la celda'),
                      ('travel_min_*', 'píxel de fricción Oxford/MAP 2019 (30″, ~1 km, llevado a 500 m), costo mínimo multiorigen hasta el centro de la celda'),
                      ('*', DERIVADO)],
    'agri_potential': [('p_normal_mm', 'píxel CHIRPS v3 0,05° (~5,5 km) que contiene el centro'),
                       ('balance_normal_mm', 'píxeles CHIRPS v3 0,05° y ERA5-Land 0,1° (~11 km) que contienen el centro'),
                       ('gdd_normal', 'píxel ERA5-Land 0,1° (~11 km) corregido con estaciones'),
                       ('heladas*', 'píxel ERA5-Land 0,1° (~11 km) corregido con estaciones'),
                       ('calor35_normal', 'píxel ERA5-Land 0,1° (~11 km) corregido con estaciones'),
                       ('ph', 'píxel SoilGrids 2.0 (250 m)'), ('arcilla_pct', 'píxel SoilGrids 2.0 (250 m)'),
                       ('pendiente_pct', 'píxel FABDEM a 3″ (~90 m)'), ('elev_m', 'píxel FABDEM a 3″ (~90 m)'),
                       ('apt_*', 'el más grueso de sus insumos: clima ERA5-Land 0,1° y CHIRPS 0,05°, suelo SoilGrids 250 m')],
    'censo_temporal': [('*', 'radio censal de cada año (1991, 2001, 2010, 2022), repartido por área en las celdas pobladas')],
    'deforestation_dynamics': [('c_*', 'píxel Hansen GFC 30 m (la celda resume ~115)'), ('*', DERIVADO)],
    'economic_activity': [('tasa_*', RADIO), ('pct_universitario', RADIO), ('pct_universitario_2010', RADIO_2010),
                          ('ocupados_est', REPARTO),
                          ('viirs_*', 'píxel VIIRS DNB 500 m (bilineal en el centro de la celda)'),
                          ('building_density_per_km2', EDIFICIO), ('n_edificios', EDIFICIO),
                          ('hogares_estimados', REPARTO), ('pob_*', REPARTO), ('*', DERIVADO)],
    'education_capital': [('c_*_2010', RADIO_2010), ('c_*', RADIO), ('n_edificios', EDIFICIO), ('*', REPARTO)],
    'education_flow': [('c_*', RADIO), ('n_edificios', EDIFICIO), ('*', REPARTO)],
    'eudr': [('fire_*', 'píxel MODIS MCD64A1 500 m (fuego sobre nativa con máscara MapBiomas 30 m)'),
             ('*', 'píxel de 100 m (Hansen GFC 30 m agregado), ~9 por celda')],
    'flood_risk': [('jrc_*', 'píxel JRC Global Surface Water 30 m (la celda resume ~115)'),
                   ('flood_extent_pct', 'píxel Sentinel-1 a 90 m, centros de píxel en la celda'),
                   ('low_land_pct', 'píxel HAND MERIT Hydro (3″, ~90 m)'),
                   ('low_bld*', 'huella de edificio (Open Buildings) sobre HAND MERIT Hydro (~90 m)'), ('*', DERIVADO)],
    'forestry_aptitude': [('gdd', 'píxel ERA5-Land mensual 0,1° (~11 km) que contiene el centro'),
                          ('precip_total', 'píxel CHIRPS v2 0,05° (~5,5 km) que contiene el centro'),
                          ('water_deficit', 'píxel TerraClimate (~4 km) que contiene el centro'),
                          ('slope_mean', 'SRTM 30 m exportado a 90 m, p90 dentro de la celda'),
                          ('clay', 'píxel SoilGrids 250 m'), ('soc', 'píxel SoilGrids 250 m'),
                          ('*', DERIVADO + '; el clima entra a 0,05–0,1°')],
    'health_access': [('c_healthcare_time', 'radio censal 2022 (media de los píxeles Oxford/MAP de ~1 km dentro del radio)'),
                      ('c_pop_density', REPARTO + ', sobre el área de la celda'),
                      ('c_*', RADIO), ('n_edificios', EDIFICIO), ('*', REPARTO)],
    'land_use': [('frac_*', 'vacía desde el pase a MapBiomas (3-nov-2026); era Dynamic World 10 m'),
                 ('score', 'vacía desde el pase a MapBiomas (3-nov-2026)'),
                 ('pct_*', 'píxel MapBiomas Argentina Col. 3, 30 m (la celda resume ~115)'), ('*', DERIVADO)],
    'pm25_drivers': [('c_pm25_mean*', 'píxel ACAG V6.GL.02 0,01° (~1 km) en el centro de la celda'),
                     ('c_*', 'modelo de conductores SHAP a res-7 (~5 km²)'), ('*', DERIVADO)],
    'service_deprivation': [('c_*', RADIO), ('n_edificios', EDIFICIO), ('*', REPARTO)],
    'sociodemographic': [('pct_*', RADIO), ('tamano_medio_hogar', RADIO),
                         ('densidad_hab_km2', REPARTO + ', sobre el área de la celda'), ('hog_total', REPARTO), ('*', DERIVADO)],
}
assert set(SOPORTES) == set(CAPAS), set(SOPORTES) ^ set(CAPAS)


def soporte(capa: str, col: str) -> str | None:
    import fnmatch
    return next((s for pat, s in SOPORTES[capa] if fnmatch.fnmatchcase(col, pat)), None)


def _res_capa(cid: str) -> str:
    vistos = list(dict.fromkeys(s for _, s in SOPORTES[cid] if s != DERIVADO and not s.startswith(DERIVADO)))
    return 'Por columna (meta.variables.soporte): ' + '; '.join(vistos) + '. Guardado en celda H3 res-9.'


for cid, (nombre, fuentes, proc, vars_, comp, lim, scripts) in CAPAS.items():
    censal = 'build_census_rates' in scripts
    FUENTES.append(dict(
        id=f'web_{cid}', nombre=f'Capa de la web: {nombre}', proveedor='Spatia (este repositorio)',
        producto=f'Capa {cid} de spatia.ar, foto cargada por build_hexoma.py', version='la publicada en cdn.spatia.ar al cargar (ver public.cargas)',
        acceso='Parquets por departamento de cdn.spatia.ar (los mismos que lee la ficha de la web)',
        licencia='AGPL-3.0 (código); los datos heredan las licencias de sus fuentes', cita='Gómez, R. E. Spatia — análisis geoespacial del NEA. https://www.spatia.ar',
        doi='Zenodo concept DOI 10.5281/zenodo.19483040', tipo_dato='capa de la web (derivada)', res_espacial=_res_capa(cid),
        res_temporal='según fuente', periodo='según fuente', cobertura='24 territorios en la web; NEA argentino en el Hexoma',
        variables=vars_, procesamiento=f'Fuentes: {fuentes}. Proceso: {proc}' + (f' {COMUN_CENSAL}' if censal else ''),
        asignacion_celda='La de la capa (ver procesamiento); en el Hexoma se guarda tal cual, por archivo de departamento (capa_*.depto_id, principal).',
        validacion='Verificada contra la ficha publicada: 75/75 valores iguales (pipeline/hexoma/verificar_ficha.py, 31-oct-2026).',
        limitaciones=lim + (' Comparable entre territorios.' if comp else ' No comparable entre territorios.'),
        uso_recomendado='Reproducir lo que muestra la web; contexto.', uso_no_recomendado='Como dato primario: ir a la fuente de origen.',
        scripts=scripts, objetos=[f'public.capa_{cid}'], verificado=V))

PROCESOS = [
    dict(id='clima_pixel', nombre='Indicadores agroclimáticos por píxel de origen',
         metodo=('Campaña jul–jun (campania = año de inicio), estación meteorológica (DEF del año de diciembre) y heladas '
                 'por año calendario. Sólo grupos completos (todos los días calendario presentes).'),
         parametros='lluvia ≥ 1 y ≥ 50 mm; racha seca < 1 mm; GDD base 10 °C; Tmax > 35 °C; heladas < 0, < −2 y ≤ 3 °C',
         validacion='Ver chirps_v3 y era5_land.', regla='—', limitaciones='La unidad es el píxel de la fuente, no la celda.',
         scripts='pipeline/hexoma/clima_a_pixel.py', objetos=['clima.chirps_*', 'clima.era5_campania', 'clima.era5_estacion', 'clima.era5_helada', 'clima.fuentes'],
         fuentes=['chirps_v3', 'era5_land'], verificado=V),
    dict(id='hex_pixel', nombre='Asignación celda → píxel de origen',
         metodo='Cada celda lee el píxel de cada fuente que contiene su centro (h3_cell_to_geometry). pixel_id = fila_global × 100000 + columna_global.',
         parametros='CHIRPS origen (−180, 60) paso 0,05°; ERA5-Land origen (−180,05, 90,05) paso 0,1°',
         validacion='3.298.621 celdas con píxel, 0 sin asignar.', regla='Sin interpolación (decisión del 1-nov-2026): el valor es lo que la fuente mide.',
         limitaciones='Bloques de celdas con el mismo valor.', scripts='pipeline/hexoma/build_hexoma_clima.py',
         objetos=['clima.pixel', 'clima.hex_pixel', 'clima.par'], fuentes=['chirps_v3', 'era5_land'], verificado=V),
    dict(id='et0_fao56', nombre='Evapotranspiración de referencia FAO-56 Penman-Monteith',
         metodo=('ET0 diaria (ec. 6 de FAO-56) con Tmax/Tmin, ea del punto de rocío, viento a 2 m = u10·4,87/ln(67,8·10 − 5,42), '
                 'γ de la presión, Rn = 0,77·Rs − Rnl (ec. 38–39) con Rso de la altitud derivada de la presión, G = 0.'),
         parametros='albedo de referencia 0,23', validacion='Ejemplo 18 de FAO-56: 3,88 mm/día (esperado 3,9), assert en clima_a_pixel.py.',
         regla='No se usa la radiación neta de ERA5 (depende del albedo real).', limitaciones='Viento del vector medio diario (subestima).',
         scripts='pipeline/hexoma/clima_a_pixel.py', objetos=['clima.era5_campania', 'clima.era5_estacion'], fuentes=['era5_land'], verificado=V),
    dict(id='balance_hidrico', nombre='Balance hídrico climático',
         metodo='P (CHIRPS) − ET0 (ERA5-Land) por par de píxeles (clima.par) y campaña o estación.', parametros='—',
         validacion='Hereda las de CHIRPS (+10 % de lluvia) y ET0.', regla='—', limitaciones='Combina dos resoluciones distintas.',
         scripts='pipeline/hexoma/build_hexoma_clima.py', objetos=['clima.balance_*'], fuentes=['chirps_v3', 'era5_land'], verificado=V),
    dict(id='normales_anomalias', nombre='Normal 1991–2020 del propio píxel y anomalías',
         metodo=('Normal OMM 1991–2020: n, media, mediana, P10, P90 por unidad y variable. Anomalía = valor − normal; % de la normal; '
                 'pct_ref = % de los 30 años de referencia con valor estrictamente menor.'),
         parametros='campañas 1991/92–2020/21; años 1991–2020', validacion='—',
         regla='Cada celda se compara con su propia historia.', limitaciones='Supone que 1991–2020 es una referencia útil (no estacionaria bajo cambio climático).',
         scripts='pipeline/hexoma/build_hexoma_clima.py', objetos=['clima.normal_1991_2020', 'clima.anomalia'], fuentes=['chirps_v3', 'era5_land'], verificado=V),
    dict(id='correccion_temperatura', nombre='Corrección de Tmin y Tmax con estaciones (heladas y calor)',
         metodo=('Mapeo de cuantiles empírico por estación, variable y mes: δ(q) = Q_estación(q) − Q_ERA5(q) en 101 cuantiles. Cada día '
                 'de un píxel se ubica en su cuantil (referencia 1991–2025) y se le suma δ trasladado desde las 4 estaciones más '
                 'cercanas a < 150 km con peso 1/d². Helada a 5 cm: δ suelo − abrigo observado en las convencionales del INTA, '
                 'desde la más cercana, sumado a la Tmin corregida.'),
         parametros='≥ 150 días por mes y ≥ 3 años por estación; 40 estaciones con temperatura',
         validacion=('Validación cruzada excluyendo la estación y sus vecinas a < 10 km; MAE de conteos anuales en años completos: '
                     'heladas (< 0 y ≤ 3 °C) 11,9 → 7,4 días/año; días > 35 °C 28,9 → 16,5; helada a 5 cm 15,6 → 9,2 '
                     '(clima.correccion_validacion, clima.correccion_decision).'),
         regla='Fijada antes de ver resultados: adoptar el método de menor MAE si mejora ≥ 25 % la cruda; si no, no publicar.',
         limitaciones=('Supone sesgo estacionario 1950–2025. Por estación persisten errores de varios días por año (p. ej. la '
                       'validación cruzada sobrecorrige las sierras de Misiones cuando se excluye Cerro Azul). Fuera del radio de '
                       '150 km usa la estación más cercana.'),
         scripts='pipeline/hexoma/corregir_temperatura.py', objetos=['clima.era5_*_corr', 'clima.correccion_*'],
         fuentes=['era5_land', 'ghcn_daily', 'inta_siga'], verificado=V),
    dict(id='aptitud_cultivo', nombre='Aptitud por cultivo, celda y campaña (A4)',
         metodo=('Umbrales validados por el autor (umbrales_cultivos.md, 8 reglas) codificados en aptitud_reglas.py; '
                 'indicadores climáticos por ventana del ciclo desde la temperatura corregida (aptitud_clima.py); clase por '
                 'factor S1/S2/S3/N; combinación FAO por factor más limitante; resumen por celda = clase mediana y % de '
                 'campañas S1 y S1+S2 en 1991/92–2024/25. Arroz: frío por decenios (Steinmetz & Amaral 2002).'),
         parametros='ventanas: soja nov–mar, maíz sep–feb, algodón oct–mar, girasol ago–dic, arroz ene–1.º mar; perennes y pasturas: campaña',
         validacion=('validar_aptitud.py (tras los ajustes del 1-nov-2026): lift de S1 contra MapBiomas 2025: algodón 2,33, '
                     'yerba 3,18 (pocas celdas S1), girasol 1,75, té 1,81, maíz 1,63, soja 1,50 (era 0,85 con el pH de ECOCROP); '
                     'cítricos sin prueba válida (el cultivo perenne de MapBiomas es yerba y té); pasturas sin S1. Departamentos '
                     'contra el MAGyP: correlaciones débiles a moderadas (soja S1 r 0,33). Ver aptitud.validacion.'),
         regla='Reglas del autor en meta.umbrales_reglas (1-nov-2026).',
         limitaciones=('S1+S2 discrimina poco en cultivos de límites amplios (maíz, cítricos, pasturas, arroz). Ajustes de '
                       'aplicación (umbrales_cultivos.md): cítricos y pasturas con temperatura de crecimiento sep–mar; té con lluvia S1 '
                       '1.400–2.200; yerba y té con la tabla de pendiente de té de Sys; soja con pH argentino 6–7 (UNLP / Porta). '
                       'Pasturas: el óptimo 28–35 °C no se alcanza en la media sep–mar del NEA. Arroz sin agua de riego. Suelo de '
                       'SoilGrids (modelo).'),
         scripts='pipeline/hexoma/aptitud_reglas.py; aptitud_clima.py; build_hexoma_aptitud.py; validar_aptitud.py',
         objetos=['aptitud.*'], fuentes=['era5_land', 'chirps_v3', 'soilgrids2', 'fabdem', 'mapbiomas_ar_col3', 'ghcn_daily', 'inta_siga'],
         verificado=V),
    dict(id='validacion_clima', nombre='Validación de productos grillados contra estaciones',
         metodo='Cada estación contra el píxel que la contiene; sesgo, MAE y r diarios y de conteos anuales o por campaña en períodos con ≥ 95 % de días.',
         parametros='1991–2025', validacion='—', regla='No se corrige en este paso: se mide.', limitaciones='Pocas estaciones con series completas de temperatura máxima en GHCN.',
         scripts='pipeline/hexoma/validar_clima.py', objetos=['clima.validacion'], fuentes=['ghcn_daily', 'inta_siga', 'chirps_v3', 'era5_land'], verificado=V),
    dict(id='estaciones_qc', nombre='Control de calidad y agregados de estaciones',
         metodo=('QC como máscara (no borra): tmin > tmax, fuera de rango, saltos > 15 °C. Agregados anuales y por campaña con % de días '
                 'válidos; normal 1991–2020 con ≥ 20 períodos completos; estación más cercana a cada celda (KNN PostGIS).'),
         parametros='completo = ≥ 95 % de días válidos', validacion='762 días marcados de 526.417.', regla='—',
         limitaciones='La estación más cercana puede estar a > 100 km en el oeste de Formosa.', scripts='pipeline/hexoma/build_hexoma_estaciones.py',
         objetos=['estaciones.*'], fuentes=['ghcn_daily', 'inta_siga'], verificado=V),
    dict(id='suelo_celda', nombre='Suelo y relieve por celda', metodo='Ver soilgrids2 y fabdem.', parametros='0–30 cm; píxeles de 3"',
         validacion='Plausibilidad por territorio.', regla='—', limitaciones='—', scripts='pipeline/hexoma/build_hexoma_suelo.py',
         objetos=['suelo.*'], fuentes=['soilgrids2', 'fabdem'], verificado=V),
    dict(id='cruce_radio_h3', nombre='Cruce único radio → celda por año censal',
         metodo=('Peso de una celda dentro de un radio = fracción de la celda en el radio × peso de época (área, edificios ~2023, '
                 'GHSL de la época, o edificios donde GHSL marcaba construcción), elegido por año y tamaño de radio.'),
         parametros='radios urbanos < 1 km², intermedios 1–10, rurales > 10',
         validacion=('Repartir la población de cada fracción entre sus radios y medir WAPE: área 71–82 %; en urbanos ganan los edificios '
                     '(25–30 %), en rurales GHSL de la época (2022: 57 contra 63 %; medido el 6-nov-2026). pipeline/hexoma/validacion_cruce.md, cruce.validacion.'),
         regla='Regla fijada antes de ver los resultados; el método ganador por año y tamaño queda en cruce.radio_h3.metodo.',
         limitaciones='En radios rurales grandes la ubicación dentro del radio es gruesa (cruce.celda_confianza).',
         scripts='pipeline/hexoma/build_hexoma_cruce.py', objetos=['cruce.*'], fuentes=['ghsl_r2023a', 'edificios_web', 'censo_1991', 'censo_2001', 'censo_2010', 'censo_2022'], verificado=V),
    dict(id='panel_censal', nombre='Panel censal 1991–2022 por celda',
         metodo='38 indicadores con definición comparable (censo_specs.PANEL) llevados a la celda con el cruce de cada año; Σ peso × conteo.',
         parametros='—', validacion='Población y NBI por provincia y año iguales a los de los radios y a los del INDEC.', regla='—',
         limitaciones='Cada indicador dice en su COMMENT qué años tiene.', scripts='pipeline/hexoma/build_hexoma_panel.py',
         objetos=['panel.*'], fuentes=['censo_1991', 'censo_2001', 'censo_2010', 'censo_2022'], verificado=V),
    dict(id='carga_web', nombre='Carga de la foto de la web en el Hexoma',
         metodo=('build_hexoma.py exporta el registro de capas de config.ts, baja los parquets por departamento de cdn.spatia.ar con '
                 'DuckDB y los carga tal cual; arma hexagonos, limites, departamentos, territorios, ficha_claves, ficha_enlace y v_hexoma.'),
         parametros='—', validacion='75/75 contra la ficha publicada (31-oct-2026); desde el 6-nov-2026, vigencia.py --muestra contra los parquets servidos.', regla='v_hexoma queda como foto fiel de la web.',
         limitaciones='Es una foto: hay que recargar cuando la web cambia (public.cargas guarda la procedencia).',
         scripts='pipeline/build_hexoma.py', objetos=['public.hexagonos', 'public.limites*', 'public.departamentos', 'public.territorios',
                                                     'public.ficha_*', 'public.cargas'], fuentes=[], verificado=V),
    dict(id='distribuciones_rangos', nombre='Distribuciones y rangos por ámbito',
         metodo='n, P10–P90, media, mín., máx. por variable × departamento/provincia/región; rango de cada celda (lt, eq en centésimos).',
         parametros='región: físicas = todos; censales comparables = censo_ar', validacion='Mismos valores que muestra la web.', regla='—',
         limitaciones='Las tablas rango_*_new son restos de una recarga.', scripts='pipeline/build_hexoma_distribuciones.py',
         objetos=['public.distribuciones', 'public.rango_*'], fuentes=[], verificado=V),
    dict(id='serie_hansen', nombre='Serie anual de pérdida Hansen por celda',
         metodo='Ver hansen_gfc_v113: conteo de píxeles por celda y año, en ha con el área de la celda.',
         parametros='dosel 2000 ≥ 10 % (FAO) y ≥ 30 % (Global Forest Watch); también sin umbral',
         validacion='Totales 2001–2025 por provincia a ±0,4 % de un reduceRegion de GEE sobre la provincia (FAO GAUL 2015; por año, ±2,4 % como máximo, Formosa); contra c_loss_rate de la capa web r = 0,9985 (2016–2025) y 0,9983 (2001–2010) en 3,29 millones de celdas. pipeline/hexoma/validacion_hansen.md.', regla='—', limitaciones='Ver hansen_gfc_v113.',
         scripts='pipeline/hexoma/build_hexoma_hansen.py', objetos=['series.*'], fuentes=['hansen_gfc_v113'], verificado=V),
    dict(id='cruce_institucional', nombre='Polígonos y puntos institucionales → celda',
         metodo=('Polígonos partidos con ST_Subdivide (256 vértices) y join espacial con el polígono de cada celda (GiST); % = área '
                 'geodésica de la intersección / área de la celda (celda entera dentro = su área). Puntos: celda que los contiene.'),
         parametros='tope 100 % por celda (superposiciones dentro de una misma fuente)',
         validacion='validacion_institucional.md: superficie de los polígonos contra la suma en celdas, por provincia y categoría.',
         regla='h3-pg 4.1 no tiene polygon_to_cells en modo overlapping: por centro se perdían los polígonos chicos del OTBN.',
         limitaciones='Las celdas en dos provincias cuentan en las dos.', scripts='pipeline/hexoma/build_hexoma_institucional.py',
         objetos=['institucional.*'], fuentes=['otbn_imibio_2017', 'otbn_sayds_2017', 'areas_protegidas_sayds', 'inai_comunidades'], verificado=V),
    dict(id='vecindad', nombre='Funciones de vecindad (esquema vec)',
         metodo='h3_grid_disk: vec.anillo, vec.vecinas, vec.media_vecinos, vec.suavizar (celda contra entorno) y vec.frontera.',
         parametros='k = distancia en celdas (k = 1: 6 vecinas, ~350 m entre centros)', validacion='Escenario 8 de escenarios.sql.',
         regla='Las celdas sin fila o con NULL no cuentan; n_vecinos lo dice.', limitaciones='En el borde del NEA cargado hay menos vecinas.',
         scripts='pipeline/hexoma/vecindad.sql (lo aplica build_hexoma_meta.py)', objetos=[], fuentes=[], verificado=V),
    dict(id='drenaje_misiones', nombre='Área drenada de Misiones a 30 m',
         metodo=('FABDEM 1" de Misiones + ~5 km (computePixels, int16 en dm); pysheds D8: fill_pits, fill_depressions, '
                 'resolve_flats, flowdir, accumulation pesada por el área real de cada celda (km²). Mismo método que los '
                 'arroyos de la demo El Soberbio.'),
         parametros='red = área drenada ≥ 0,1 km²', validacion='Puentes RP2: Arroyo Soberbio 1.074 km², Acaraguá 775 km².',
         regla='—', limitaciones=('Ríos que entran desde fuera del recorte (Paraná, Uruguay, Iguazú, Pepirí Guazú, San Antonio) '
                                  'con área truncada; el cauce queda corrido ~100 m del que OSM dibuja sobre imagen.'),
         scripts='pipeline/infra/drenaje.py', objetos=[], fuentes=['fabdem'], verificado='2026-11-07'),
    dict(id='cruces_camino_cauce', nombre='Cruces camino × cauce de Misiones',
         metodo=('Picos de área drenada a lo largo de cada camino (cada 15 m, máximo en ±30 m, prominencia ×2, ≥ 0,1 km²), '
                 'intersecciones con cauces OSM y estructuras (puentes, alcantarillas, vados) de OSM y del visor, unidos a '
                 '≤ 60 m y anclados a ≤ 150 m en el mismo camino o en uno que lo toca. Ubicación: la estructura, si no la '
                 'intersección OSM, si no el pico. Evidencia: puente > vado > alcantarilla > sin dato.'),
         parametros='PASO_M 15; A_MIN 0,1 km²; UNION_M 60; SNAP_M 150; paralelo = racha > 300 m',
         validacion=('Terreno solo (picos crudos) a ≤ 60 m: 87,0 % de 1.026 puentes OSM vehiculares sobre cauce mapeado y '
                     '89,7 % de 29 puentes RP2 (regla fijada ≥ 80 %: pasa); a ≤ 150 m, 97,0 % y 96,6 %. '
                     'pipeline/infra/validacion_cruces.md.'),
         regla='Regla de sensibilidad fijada antes de mirar (7-nov-2026); la primera corrida midió mal (ver el informe).',
         limitaciones=('Precisión no validada: no hay inventario de alcantarillas; 92 % de los cruces sin dato de estructura; '
                       'caminos de fondo de valle pueden dar picos de más (marcados paralelo).'),
         scripts='pipeline/infra/cruces.py; pipeline/infra/validar_cruces.py',
         objetos=['infra.cruces', 'infra.cruces_terreno'],
         fuentes=['fabdem', 'red_vial', 'osm_cauces', 'visor_emergencias_misiones'], verificado='2026-11-07'),
    dict(id='aristas_corte', nombre='Aislamiento por corte de cruces (aristas de corte del grafo vial)',
         metodo=('Aristas de corte de vial.aristas en el recorte de Misiones (Tarjan iterativo; aristas paralelas cuentan); '
                 'bloques = componentes sin ellas, en un bosque con raíz en Posadas; población 2022 por celda '
                 '(censo_2022.cruce) y hospitales + CAPS del visor al nodo más cercano; suma por subárbol del lado lejano.'),
         parametros='raíz: nodo más cercano a la plaza 9 de Julio de Posadas',
         validacion='Población asignada 1.273.347 (la de Misiones 2022). Cruce → arista de su camino: distancia p99 0 m.',
         regla='Exacto sobre la red: no modela transitabilidad.',
         limitaciones=('Huellas y caminos de Microsoft cuentan como alternativas aunque se corten con lluvia: el aislamiento '
                       'es un piso. Personas = las de las celdas cuyo nodo más cercano está del lado aislado.'),
         scripts='pipeline/infra/aislamiento.py', objetos=['infra.aristas_corte', 'infra.cruces'],
         fuentes=['red_vial', 'censo_2022', 'visor_emergencias_misiones'], verificado='2026-11-07'),
    dict(id='dia_de_lluvia', nombre='Día de lluvia: superficie, pendiente y acceso a salud por escenario',
         metodo=('Superficie de cada arista en dos cotas (desconocida = pavimento/ripio o = tierra); pendiente de FABDEM '
                 'sobre 150 m de camino; velocidades supuestas por clase × superficie, seco y lluvia. Escenarios seco, '
                 'lluvia, lluvia_fuerte (sin tierra con pendiente ≥ 10 %) y todo_tiempo (sólo pavimento y ripio). '
                 'Dijkstra multi-origen desde hospitales y desde hospitales + CAPS; km de tierra por el árbol de caminos.'),
         parametros='velocidades: tabla en red_lluvia.py; umbral de pendiente 10 % (sensibilidad 6–20 %)',
         validacion=('Seco contra la capa de accesibilidad (Oxford/MAP): Spearman 0,90 en 46.208 celdas (regla ≥ 0,7). '
                     'Superficie OSM contra IGN: 94,0 % de acuerdo pavimento/no pavimento. Monotonía: 0 violaciones. '
                     'Personas sin ruta con lluvia fuerte: 26–201 mil según el umbral (8–15 %); el orden de los municipios '
                     'se mantiene (Spearman ≈ 0,9). pipeline/infra/validacion_lluvia.md.'),
         regla='Reglas fijadas antes de mirar (7-nov-2026). La cantidad sin ruta NO es robusta al umbral: se informa como rango.',
         limitaciones=('Velocidades supuestas, sin calibrar; tierra por regla en las huellas (sólo 42 % etiquetada); '
                       'pendiente de FABDEM puede sobrestimar en quebradas; todo_tiempo sobrecuenta (exige pavimento hasta la puerta).'),
         scripts='pipeline/infra/red_lluvia.py; pipeline/infra/validar_lluvia.py',
         objetos=['infra.aristas_atributos', 'infra.acceso_lluvia', 'infra.v_acceso_municipio',
                  'infra.sensibilidad_pendiente', 'infra.sensibilidad_sin_ruta'],
         fuentes=['red_vial', 'fabdem', 'censo_2022', 'visor_emergencias_misiones'], verificado='2026-11-07'),
    dict(id='desvios_cruces', nombre='Peso de cada cruce: dependientes, desvío y aislamiento con lluvia fuerte',
         metodo=('Dependientes = población cuyo camino más corto al hospital en seco pasa por la arista del cruce. Desvío '
                 '= camino u→v sin esa arista (Dijkstra local en numba, corte 180 min) menos su tiempo, en seco y con '
                 'lluvia. Impacto = dependientes × desvío con lluvia. Aristas de corte del grafo de lluvia_fuerte, con '
                 'población y salud del lado aislado.'),
         parametros='cota pesimista; corte 180 min', validacion='Ver dia_de_lluvia.',
         regla='—', limitaciones='Hereda los supuestos de velocidad y del umbral de pendiente.',
         scripts='pipeline/infra/desvios.py', objetos=['infra.cruces'],
         fuentes=['red_vial', 'fabdem', 'censo_2022', 'visor_emergencias_misiones'], verificado='2026-11-07'),
    dict(id='rio_uruguay_puentes', nombre='Río Uruguay contra los puentes de la RP2 (niveles medidos)',
         metodo=('Lecturas diarias de 7 escalas de Prefectura (API INA a5, 1950–2026); cero de escala en msnm del Monitor '
                 'provincial o implícito en FABDEM (superficie del río − mediana 2011–2015); nivel en cada puente por '
                 'interpolación lineal por kilómetro de río (eje OSM encadenado) entre las estaciones con lectura ese '
                 'día; corte = nivel ≥ cota de seguridad del visor. Probabilidad anual empírica, Gumbel y GEV sobre '
                 'máximos anuales; eventos = 8 máximos anuales más altos de El Soberbio sobre la red (seco).'),
         parametros=('ceros: perfil isotónico de FABDEM (elegido por menor error de interpolación, control El Soberbio '
                     '118,9 vs 118); tramo máx. 250 km; respaldo hidrómetro ≤ 5 km; año válido ≥ 9 meses con ≥ 8 días'),
         validacion=('Prensa: 14 de 19 cortes por crecida del Uruguay (74 %; regla ≥ 2/3). 28-jun-2014: «2 m sobre el '
                     'puente» del Arroyo Soberbio pone el tablero a +0,5 m de la cota. Error de interpolación en crecida: '
                     '1–2 m El Soberbio–Alba Posse, 4–7 m Panambí–San Javier. pipeline/infra/validacion_rio.md.'),
         regla='Reglas fijadas antes de mirar (7-nov-2026): ceros y ≥ 2/3 de cortes de prensa.',
         limitaciones=('La cota de seguridad es el nivel del tablero: el cierre preventivo ocurre 3–4 m antes (El Soberbio '
                       '2023) y el modelo no lo representa. Ceros aguas abajo de El Soberbio inciertos en metros. Lectura '
                       'diaria manual: subestima el pico. No modela la crecida propia de los arroyos.'),
         scripts='pipeline/infra/rio_uruguay.py; pipeline/infra/cortes_rio.py; pipeline/infra/validar_rio.py',
         objetos=['infra.rio_estaciones', 'infra.rio_lecturas', 'infra.rio_puente_diario', 'infra.rio_puente_resumen',
                  'infra.rio_eventos', 'infra.rio_evento_celda'],
         fuentes=['ina_escalas_prefectura', 'visor_emergencias_misiones', 'fabdem', 'osm_cauces', 'red_vial',
                  'censo_2022'], verificado='2026-11-07'),
]
