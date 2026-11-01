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

V = '2026-11-01'
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
]

# ------------------------------------------------------------------ capas de la web (foto en public.capa_*)
# Fuente: relevamiento del código (config.ts, content/methodology.ts, scripts de pipeline/), 1-nov-2026.
CAPAS = {
    'accessibility': ('Accesibilidad', 'Oxford/MAP fricción motorizada 2019 v5.1 y accessibility_to_cities_2015; OSM (Overpass) para hospitales, escuelas y rutas',
                      'Tiempo a la capital por costo mínimo (MCP_Geometric) sobre la fricción, local; tiempo a ciudad en el centroide; distancias KDTree desde el centroide; goalposts (capital 5–500 min, cabecera 5–350, hospital 0–35 km, escuela 0–20, ruta 0–25); media geométrica con piso 1; k-means k=4.',
                      'travel_min_capital, travel_min_cabecera, dist_nearest_hospital_km, dist_nearest_secundaria_km, dist_primary_m (0–100) con sus *_raw en min o km (dist_primary_m_raw está en km)',
                      True, '«Cabecera» es la ciudad de ≥ 50.000 hab.; falta tiempo a ciudad en Chaco y Formosa; el ranking describe superficie, no población.',
                      'gee_export_location_value.py; compute_accessibility_h3.py; run_accessibility_new_territories.py'),
    'agri_potential': ('Potencial agrícola (web, 0–100)', 'SoilGrids v2 0–5 cm (ocd, phh2o, clay); CHIRPS v2 2019–2024; ERA5-Land mensual (GDD ≈ (T−10)·30); pendiente de USGS SRTMGL1_003 (los textos dicen FABDEM: discrepancia)',
                       'Export GEE a 100 m; pH óptimo = 1 − |pH − 6,25| / 4; enmascarado de polígono a H3; goalposts (soc 0–550, clay 0–579, precip 615,5–1940,1, gdd 2321,9–5338,9, slope 0–19,29 invertida); media geométrica de 6 variables; arcilla en joroba centrada en ~30 % (fix_agri_clay_hump.py). El camino de CI de Misiones (gee_reduce_analysis.py) usa percentil local.',
                       'c_soc, c_ph_optimal, c_clay, c_precipitation, c_gdd, c_slope, score (todas 0–100; sin rawCol)', True,
                       'Escala relativa sin unidades físicas; el GDD mensual difiere −1,4 a −1,7 % del diario; reemplazada en el Hexoma por los esquemas clima y suelo (A1) y la aptitud por cultivo (A4).',
                       'gee_export_analysis.py; gee_reduce_analysis.py; process_raster_to_h3.py; fix_agri_clay_hump.py; satellite-pixel-b*.yml (manual)'),
    'censo_temporal': ('Población 1991–2022', 'Totales de recuento completo INDEC 1991, 2001, 2010, 2022 (Source Cooperative radios.parquet); cartografía de radios Rodríguez & de Grande (CONICET)',
                       'Reparto areal sobre el universo H3 poblado, pesos renormalizados por radio; control contra totales INDEC; densidad = conteo / área; actual 2022, base 2010.',
                       'pob_dens, viv_dens (hab/km², viv/km²) con _baseline, _delta y columnas por año', False,
                       'Densidad 2022 distinta de la capa dasimétrica; sólo NEA argentino.', 'build_censo_temporal.py'),
    'deforestation_dynamics': ('Dinámica de deforestación', 'Hansen GFC v1.12 (2001–2024), 30 m',
                               'Zonal por polígono: píxeles de pérdida por año; tasa = perdidos / píxeles / años × 100 (base 2001–2010, actual 2015–2024); filtro treecover2000 > 5 %; score goalpost 0–5 %/año en modo comparable, pero el workflow corre en modo local; tipos por cuartiles.',
                               'c_loss_rate (%/año), c_cumulative (%), c_loss_rate_baseline, c_loss_rate_delta, c_treecover, score', True,
                               'Hansen cuenta plantaciones como bosque (parte de la pérdida es cosecha de pino y eucalipto).',
                               'gee_export_hansen_loss.py; process_hansen_to_h3.py; deforestation-update.yml (abril y octubre)'),
    'economic_activity': ('Actividad económica', 'Censo 2022 (actividad, nivel educativo); VIIRS DNB VCMSLCFG avg_rad 500 m (año no determinado: 2024 en el script, 2025 en el texto); Global Building Atlas (+ vida_buildings en Misiones)',
                          'Tasas censales Σw·num / Σw·den con base mínima 30; VIIRS bilineal en el centroide; densidad edilicia = edificios / área.',
                          'tasa_empleo, tasa_actividad, pct_universitario (%), viirs_mean_radiance (nW/cm²/sr), building_density_per_km2', False,
                          'Radios con < 30 en la base quedan sin cobertura.', 'gee_export_viirs.py; compute_viirs_buildings_h3.py; build_census_rates.py'),
    'education_capital': ('Capital educativo', 'Censo 2022 censo_niveducativo_2022', 'Tasas censales sobre 18+, base común (build_census_rates.py).',
                          'c_no_schooling, c_secondary_plus, c_higher_edu, c_university (% sobre pob_18_mas)', True,
                          'Comparable sólo entre provincias argentinas (INDEC); hasta oct-2026 se duplicaban universitarios en Corrientes, Chaco y Formosa.', 'build_census_rates.py'),
    'education_flow': ('Trayectoria educativa', 'Censo 2022 (no asistencia, fecundidad)', 'Tasas censales, base común; c_teen_pregnancy con base mínima 1 sólo para el ranking.',
                       'c_dropout_primary (% de 6–12), c_dropout_secondary (% de 13–18), c_teen_pregnancy', True,
                       'Base de mujeres de 14–17 muy chica por radio (mediana 19).', 'build_census_rates.py'),
    'eudr': ('Señales EUDR', 'Hansen GFC v1.13; MODIS MCD64A1 500 m; MapBiomas Argentina Col. 2; corte 31-dic-2020',
             'Pérdida post-2020 (lossyear > 20) a 100 m; píxel de 100 m a la celda de su centro (~9 por celda); fuego sobre nativa leñosa (clases MapBiomas 3–6) en paso manual; risk_score = 0,70 pérdida + 0,20 fuego + 0,10 (100 − cobertura) (no es la variable principal).',
             'loss_post_2020_pct, fire_native_post_2020_pct, fire_post_2020_pct, forest_cover_2020, forest_cover_current (%)', False,
             'Señales indicativas, no veredictos; la pérdida no distingue causa; la cobertura actual no cuenta regeneración.',
             'gee_deforestation_eudr.py; aggregate_eudr_hires.py; apply_fire_native_to_risk.py; eudr-update.yml (manual)'),
    'flood_risk': ('Inundación', 'JRC Global Surface Water v1.4 (Landsat 1984–2021); Sentinel-1 GRD (VV < −15 dB); MERIT Hydro HAND; Google Open Buildings Temporal 2016–2023',
                   'JRC por polígono (zonal_stats_rasterio); extensión actual = mediana de 12 días de S1 descendente, 30 → 90 m, centros de píxel → celda; flood_risk_score = media geométrica tier 1 (no declarada como variable); ranking con HAND < 5 m.',
                   'jrc_occurrence, jrc_recurrence (%), jrc_seasonality (meses), flood_extent_pct, low_bld_n, low_land_pct, low_bld_new_n', True,
                   'Landsat no ve crecidas cortas bajo nubes; S1 sobreestima agua en la llanura chaqueña; mide lo construido en zona baja, no la frecuencia de inundación.',
                   'gee_flood_detection.py; process_to_h3.py; update_flood_current.py; build_flood_rank.py; flood-update.yml'),
    'forestry_aptitude': ('Aptitud forestal', 'Presencia: Inventario Nacional de Plantaciones DNDFI 2026; covariables ERA5, CHIRPS, TerraClimate, SoilGrids, HWSD, SRTM, NDVI, tiempo a ciudad',
                          'Random Forest (400 árboles, hoja mínima 20, clases balanceadas), presencia = fracción plantada ≥ 0,10, fondo 1:3, GroupKFold 5; score = predict_proba × 100; k-means k=4 sobre score ≥ 40. El texto describe dos nichos (comercial AUC 0,90; nativo 0,78) pero el script _ardndfi entrena uno.',
                          'score (0–100), type_label, gdd (°C·d), precip_total (mm), water_deficit (mm), slope_mean (°), clay (%), soc (g/kg)', True,
                          'Fuera de Misiones y Corrientes es extrapolación; no se ve paso de calibración aunque el texto lo dice.',
                          'compute_forestry_sdm.py; compute_forestry_sdm_ardndfi.py; run_forestry_sdm_pipeline.py'),
    'health_access': ('Acceso a salud', 'Malaria Atlas Project, accesibilidad a salud 2019 y fricción 2019 v5.1; OSM; censo 2022 (cobertura, edad, NBI)',
                      'Tiempo a salud = mínimo entre Oxford y OSM, por radio (media de píxeles de 928 m); al hexágono por radio; tasas censales con base común.',
                      'c_healthcare_time (min), c_health_coverage, c_elderly, c_children (%), c_nbi (%), c_pop_density (hab/km²)', True,
                      'Huecos donde el centro de salud falta en las dos fuentes.', 'build_oxford_radios.py; build_census_rates.py'),
    'land_use': ('Uso del suelo', 'Dynamic World V1 (Sentinel-2, 10 m), 2024',
                 'Clase modal del año por píxel de 10 m; fracción por clase reducida a 100 m; media por polígono H3 → fracción de área; diversidad de Shannon; k-means 3–5 por silueta.',
                 'frac_trees, frac_crops, frac_grass, frac_shrub, frac_built, frac_water, frac_flooded, frac_bare, frac_snow (0–1, se muestran ×100)', True,
                 '«Árboles» incluye plantaciones; un solo año.', 'gee_dynamic_world.py; process_dw_to_h3.py; land-use-update.yml (manual)'),
    'pm25_drivers': ('PM2.5', 'ACAG V6.GL.02 (van Donkelaar et al. 2021), 0,01°, 1998–2022',
                     'Export a 100 m; centroide a H3; medias 2001–2010 y 2013–2022; score goalpost 5–30 en modo comparable; tipos por cortes OMS 2021 (10/15/25/35); conductores SHAP de un modelo a res-7.',
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

for cid, (nombre, fuentes, proc, vars_, comp, lim, scripts) in CAPAS.items():
    censal = 'build_census_rates' in scripts
    FUENTES.append(dict(
        id=f'web_{cid}', nombre=f'Capa de la web: {nombre}', proveedor='Spatia (este repositorio)',
        producto=f'Capa {cid} de spatia.ar, foto cargada por build_hexoma.py', version='la publicada en cdn.spatia.ar al cargar (ver public.cargas)',
        acceso='Parquets por departamento de cdn.spatia.ar (los mismos que lee la ficha de la web)',
        licencia='AGPL-3.0 (código); los datos heredan las licencias de sus fuentes', cita='Gómez, R. E. Spatia — análisis geoespacial del NEA. https://www.spatia.ar',
        doi='Zenodo concept DOI 10.5281/zenodo.19483040', tipo_dato='capa de la web (derivada)', res_espacial='celda H3 res-9',
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
         regla='No hay goalposts: cada celda se compara con su propia historia.', limitaciones='Supone que 1991–2020 es una referencia útil (no estacionaria bajo cambio climático).',
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
                     '(26–31 %), en rurales GHSL de la época (2022: 57 contra 68 %). pipeline/hexoma/validacion_cruce.md, cruce.validacion.'),
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
         parametros='—', validacion='75/75 contra la ficha publicada.', regla='v_hexoma queda como foto fiel de la web.',
         limitaciones='Es una foto: hay que recargar cuando la web cambia (public.cargas guarda la procedencia).',
         scripts='pipeline/build_hexoma.py', objetos=['public.hexagonos', 'public.limites*', 'public.departamentos', 'public.territorios',
                                                     'public.ficha_*', 'public.cargas'], fuentes=[], verificado=V),
    dict(id='distribuciones_rangos', nombre='Distribuciones y rangos por ámbito',
         metodo='n, P10–P90, media, mín., máx. por variable × departamento/provincia/región; rango de cada celda (lt, eq en centésimos).',
         parametros='región: físicas = todos; censales comparables = censo_ar', validacion='Igual a la ficha de la web.', regla='—',
         limitaciones='Las tablas rango_*_new son restos de una recarga.', scripts='pipeline/build_hexoma_distribuciones.py',
         objetos=['public.distribuciones', 'public.rango_*'], fuentes=[], verificado=V),
]
