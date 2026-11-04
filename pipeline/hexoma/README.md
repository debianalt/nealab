# Hexoma

El **hexoma** de una celda H3 es su vector completo en Spatia: todas las capas, en sus
escalas propias y en su posición dentro del departamento, la provincia y la región, más
los cortes temporales que tenga cada capa. **El Hexoma** es la base Postgres local que
los guarda para todo el NEA argentino (Misiones, Corrientes, Chaco y Formosa:
3.299.569 hexágonos res-9, 15 capas), más los censos 1991, 2001, 2010 y 2022 completos por radio censal
(esquemas `censo_<año>`).

Es **privado**: sirve para armar escenarios de investigación con SQL. No se publica, no
se vende y no se sube nada a R2. Es una **foto** de los parquets publicados en
`cdn.spatia.ar`: los mismos archivos y columnas que lee la ficha del hexágono de
spatia.ar, de modo que un valor del Hexoma coincide con el de la ficha y el panel. La
base no tiene backup propio porque se regenera.

## Conexión
- Servidor local `localhost:5432` (PostgreSQL 18, PostGIS 3.6, h3-pg 4.1), base `hexoma`.
  No confundir con `ndvi_misiones`, que es otra base y no se toca.
- `psql -U postgres -d hexoma`. En Windows, psql muestra mal los acentos: usar
  `SET client_encoding = 'UTF8';` y correr Python con `PYTHONIOENCODING=utf-8`.
- Desde Python: `psycopg2.connect('postgresql://postgres@localhost:5432/hexoma')`.

## Regenerar (desde `neahub/`)
| Comando | Qué hace | Tiempo medido |
|---|---|---|
| `python pipeline/build_hexoma.py` | Exporta el registro de capas desde la web (`hexoma_registry.mjs`, Vite SSR sobre `config.ts`), baja los parquets por DuckDB/httpfs, carga `capa_*`, `hexagonos` y `v_hexoma`, y al final calcula las distribuciones | ~25 s por capa y por millón de hexágonos; más 745 s de distribuciones |
| `… --territories misiones --layers land_use` | Recarga sólo esa parte. Es idempotente: borra y carga en una transacción | |
| `… --workers 4` | Un proceso por territorio | |
| `… --skip-registry` | Reusa `pipeline/output/hexoma/registry.json` | |
| `… --skip-distribuciones` | Omite el cálculo de distribuciones y rangos | |
| `… --reset` | Borra sólo las tablas del Hexoma y vuelve a cargar | |
| `python pipeline/build_hexoma_distribuciones.py [--layers …]` | Recalcula sólo `distribuciones`, `rango_*` y `v_hexoma_rangos` | 745 s con todas las capas |
| `python pipeline/build_ficha_link_index.py` | Regenera `pipeline/output/hexoma/ficha_link_index.json` (archivo de departamento de cada celda de borde), que usa el Hexoma. Correrlo cuando cambia una partición por departamento | |
| `python pipeline/build_hexoma_censo.py [--anio A] [--refresh]` | Rearma los esquemas `censo_<año>` (ver abajo). Es independiente de lo anterior, y `--reset` no lo toca. 1991 necesita antes `python pipeline/hexoma/redatam_a_radio.py --anio 1991 --csv pipeline/output/hexoma/redatam1991/csv` (los CSV de `ARG1991.zip`) | 40–110 s por año |
| `python pipeline/hexoma/build_hexoma_externos.py` | Rearma el esquema `externos` | 5 s |
| `python pipeline/gee_export_ghsl_epochs.py` y `gcloud storage cp "gs://spatia-satellite/satellite/nea_ar/ghsl_built_*.tif" pipeline/output/hexoma/ghsl/` | Exporta GHSL 1990, 2000, 2010 y 2020 del NEA (una vez; el cruce los baja solo si faltan) | ~10 min en GEE |
| `python pipeline/hexoma/build_hexoma_cruce.py [--desde pesos\|fracciones\|validacion\|final]` | Rearma el esquema `cruce` y `hexoma/validacion_cruce.md`. Correrlo después de los censos | 140 s |
| `python pipeline/hexoma/build_hexoma_panel.py` | Rearma el esquema `panel`. Correrlo después del cruce | 10 s |
| `python pipeline/gee_export_clima_diario.py --fuente chirps\|era5` | Baja de GEE el clima diario del NEA (CHIRPS v3 1981–, ERA5-Land 1950–) en la grilla nativa, a `%HEXOMA_FUENTES%/clima/diario/` (fuera de OneDrive; 5,8 GB). Idempotente | ~6 min ERA5, ~2 min CHIRPS |
| `python pipeline/gee_export_suelo_relieve.py` | Baja SoilGrids 2.0 (9 variables, 0–30 cm) y FABDEM (pendiente y elevación a 3") a `%HEXOMA_FUENTES%/suelo/` | ~3 min |
| `python pipeline/hexoma/clima_a_pixel.py` | Indicadores por píxel de origen: campaña, estación, heladas por año, ET0 FAO-56 | ~2 min |
| `python pipeline/hexoma/build_hexoma_clima.py` | Rearma el esquema `clima` (ver abajo) | ~5 min |
| `python pipeline/hexoma/build_hexoma_suelo.py` | Rearma el esquema `suelo` | ~2 min |
| `python pipeline/hexoma/bajar_siga.py` | Baja las series diarias de las estaciones del INTA (SIGA) del NEA, por la API pública de la aplicación, a `%HEXOMA_FUENTES%/estaciones/` | ~15 min |
| `python pipeline/hexoma/build_hexoma_estaciones.py` | Rearma el esquema `estaciones` (GHCN-Daily + INTA SIGA): la referencia obligada | ~5 min |
| `python pipeline/hexoma/corregir_temperatura.py` | Corrige Tmin y Tmax de ERA5-Land con las estaciones (validación cruzada y regla); después recargar `build_hexoma_clima.py` | ~4 min |
| `python pipeline/hexoma/build_hexoma_meta.py` | Rearma el catálogo metodológico `meta` desde `metodologia.py`. Correrlo al final de cualquier recarga | segundos |
| `python pipeline/hexoma/validar_clima.py` | Estaciones GHCN-Daily contra el píxel que las contiene: `clima.validacion` y `validacion_clima.md` | 1 min |
| `python pipeline/hexoma/hexoma_ficha.py <h3> […]` | Ficha HTML local de una celda con sus series: `pipeline/output/hexoma/fichas/<h3>.html` | segundos |
| `python pipeline/hexoma/extracto.py --nombre X --sql q.sql --destino carpeta` | Extracto congelado de una consulta: parquet + manifiesto de procedencia (ver abajo) | segundos |

Una recarga deja filas muertas. El script reescribe (VACUUM FULL) las tablas donde pasan
del 20 %. Si un VACUUM FULL no avanza, mirá `pg_stat_activity`: una consulta huérfana lo
bloqueó una vez durante una hora.

## Esquema
| Objeto | Contenido |
|---|---|
| `territorios`, `departamentos` | Territorios de `TERRITORY_REGISTRY` y un `depto_id` por `parquetKey` de los dept summaries |
| `limites`, `limites_sub` | Departamentos AR de `ar_dept_boundaries.json` (anillos exteriores) y su subdivisión indexada |
| `hexagonos` | (h3, territorio, redcode del polígono que contiene el centro). Geometría al vuelo: `h3_cell_to_boundary_geometry(h3)` |
| `ficha_claves`, `ficha_enlace` | Qué archivo de departamento lee la ficha para cada celda y capa |
| `cargas` | Procedencia: una fila por capa, territorio y corrida, con las URL (`?v`), ETag, Last-Modified y bytes de cada archivo |
| `capa_<id>` | Columnas del parquet tal cual (también las temporales: `pob_cnt_1991…2022`, `*_baseline`, `*_delta`), más `depto_id` (archivo de origen) y `principal` |
| `v_hexoma` | Una fila por (h3, territorio). Columnas `<capa>__<variable>` con el valor que muestra la ficha, y `<capa>__type_label`. Cada columna tiene COMMENT con rótulo y unidad |
| `distribuciones` | n, P10, P25, P50, P75, P90, media, mínimo y máximo de cada variable de la ficha, por ámbito |
| `rango_<capa>` | Posición de cada celda: `<var>__<dep\|prov\|reg>_lt` y `_eq` |
| `v_hexoma_rangos` | `<capa>__<variable>__pr_<dep\|prov\|reg>` = % por debajo, unible a `v_hexoma` por (h3, territorio) |

Tamaño medido el 31-oct-2026: **11 GB** en total. El esquema `public` ocupa 10,2 GB (capas, rangos y distribuciones) y el esquema `censo` (hoy `censo_2022`) 0,75 GB. El 1-nov-2026 se sumaron `censo_1991` (0,18 GB), `censo_2001` (0,22 GB) y `censo_2010` (0,49 GB).

## Censos 1991, 2001, 2010 y 2022 por radio (esquemas `censo_<año>`)
Cada censo se guarda completo en su unidad de origen, el radio censal de ese año, en su propio esquema. `censo_2022` es el antiguo esquema `censo` (renombrado el 1-nov-2026); es el único que hoy se une a las celdas (`cruce`, `v_h3`), por el mismo cruce que usan las capas censales de la web. Los cuatro años llegan a las celdas por el cruce único (esquema `cruce`) y el panel (esquema `panel`), más abajo. Nada de esto reemplaza a las capas de la web: `v_hexoma` sigue siendo la foto de la web.

**Fuentes:**
| Año | Conteos por radio | Radios y totales oficiales |
|---|---|---|
| 1991 | Microdatos REDATAM exportados por Open Redatam ([pachadotdev/redatam-microdata](https://github.com/pachadotdev/redatam-microdata), release 2.0.2, `ARG1991.zip`), pasados a radio con `redatam_a_radio.py`. **Muestra** en los radios grandes | `radios.parquet` 1991 de Source Cooperative |
| 2001 | [Source Cooperative](https://source.coop/nlebovits/censo-argentino) (CC-BY-4.0), de la base REDATAM. Censo completo | ídem 2001 |
| 2010 | Ampliado: Source Cooperative, **muestra** en las localidades de más de 50 mil habitantes. Básico completo: tabla por radio de Poblaciones importada en la base `posadas` (`censo_2010.nbi_2010` y `poblacion_2010`) | ídem 2010 |
| 2022 | Source Cooperative, de la base REDATAM. Censo completo | Polígonos de la web (los del cruce de las capas) y totales de `radios.parquet` 2022 |

La descarga directa del INDEC (`bases_censo2022_RedatamX.zip`) devolvía la página de inicio el 31-oct-2026. Para cruces entre variables (sexo × edad, por ejemplo), que la versión por radio no trae, están los microdatos de los cuatro años en `pachadotdev/redatam-microdata` (`ARG1991`…`ARG2022`, CSV por entidad) y `redatam_a_radio.py` como punto de partida.

**Muestra (1991 y 2010).** En esos radios la base trae sólo la muestra (en 2010, 214.819 personas en lugar de 1.346.791 en 1.310 radios del NEA). Cada radio se calibra con su total oficial: `radios.factor_muestra` = `personas_oficial / n_muestra`, y `indicadores` ya viene multiplicado por ese factor (personas, hogares y viviendas: la muestra es de viviendas enteras). `categorias` guarda el conteo crudo y `v_categorias.n_expandido` el calibrado. Las tasas de un radio con muestra salen de unas decenas de hogares: son ruidosas en la celda y en el radio, estables al sumar varios. Los radios «sólo básico» (300 en 2010, 35 en 1991) no tienen ampliado: tienen sus totales oficiales y los indicadores en NULL. `radios.relevamiento` dice el caso de cada radio.

**2010 básico.** Los indicadores de hogares y población del cuestionario básico (NBI, hacinamiento crítico, heladera, computadora, teléfono, agua de red, cloaca, techo, cielorraso, piso, combustible, tenencia, sexo, edad, universitarios) salen de la tabla básica completa de Poblaciones (`censo_2010.basico`), no de la muestra: cubre los 5.320 radios y sus hogares de Misiones suman 302.953, el total del INDEC. Cada columna usada se verificó contra el censo crudo en los 3.710 radios censados completos: coincide exacto en los 3.710 con las categorías que anota su COMMENT (varias son el complemento: `h_heladera` = sin heladera, `h_agua_red` = sin red, `h_cloaca` = sin cloaca). `h_nbi` coincide en 2.992 de 3.710 (−0,45 % de hogares con NBI): es el NBI de la base básica y reproduce el publicado.

**Qué trae cada año.** `censo_<año>.variables` (114 en 1991, 81 en 2001, 124 en 2010, 76 en 2022) y `censo_<año>.indicadores` (los COMMENT dicen variable, categorías del numerador y del denominador). Lo que no se pudo armar: 2001 no tiene procedencia del agua ni combustible; 2010 no trae condición de actividad (se arma con P25–P28 cruzadas, que sólo están en los microdatos); 1991 no trae migración usable ni nivel educativo completo.

| Objeto (por año) | Contenido |
|---|---|
| `categorias` | Todo el censo: conteo crudo por (radio, variable, categoría). Con rótulos y conteo expandido: `v_categorias` |
| `variables`, `diccionario` | Qué es cada variable y cada categoría |
| `indicadores` | Indicadores por radio en conteos calibrados, `<x>_num` y `<x>_den`, más `personas_oficial` y `viviendas_oficial` |
| `v_tasas_radio` | Las tasas (%) y los promedios por radio |
| `radios` | Departamento, atributos (área urbana o rural, localidad…), relevamiento, totales oficiales, factor de muestra, área y geometría |
| `basico` (2010) | La tabla básica completa de Poblaciones, tal cual |
| `cruce`, `v_h3` (2022) | Celda → radio por edificios y la vista por celda, como antes en el esquema `censo` |

`externos` guarda datos de terceros sobre la grilla de un censo, con su procedencia en `externos.fuentes`: hoy, el riesgo de inseguridad alimentaria por radio 2010 de la UCA (`uca_inseguridad_alimentaria_2010`; armado con variables del censo 2010, así que no es independiente del NBI de ese año). El «índice de exclusión» de `posadas.censo_2010.exclusion_2010` quedó afuera: no se encontró quién lo hizo.

**Cuándo usar cada unidad:**
- **Radio,** para preguntas cuyo dato central es censal: es donde el INDEC midió, con n honesto y errores estándar que se pueden agrupar por radio.
- **Celda,** para cruzar con las capas físicas, sobre todo en el campo, donde hay radios de cientos de km².

Para tasas de un conjunto de celdas o de un departamento, sumá `_num` y `_den` y dividí al final. No promedies tasas.

**Verificado el 1-nov-2026** (`build_hexoma_censo.py` lo imprime al final de cada año):
- **Personas por radio:** iguales al total oficial en todos los radios con datos (2001: salvo uno de Chaco, donde el total de `radios.parquet` es 692 menor; los datos dan 984.446, el total publicado de la provincia). Radios con datos y sin geometría: 0 en 2001, 2010 y 2022; 14 en 1991 (160 personas).
- **NBI de hogares por provincia** igual al publicado por el INDEC en 2001 (Misiones 23,5; Corrientes 24,0; Chaco 27,6; Formosa 28,0 %) y en 2010 (15,6; 15,1; 18,2; 19,7 %). 1991 da 29,4; 26,3; 33,1; 33,4 %: ahí el NBI es el de la vivienda, heredado por cada hogar.
- **2022, como antes del renombre:** `v_h3` tiene las mismas 318.506 celdas y las mismas sumas (±30 personas sobre 4,2 millones, por redondeo de reales). Verificado el 31-oct-2026: tasas por radio iguales a las de la web; NBI por celda igual a la capa de la web en 223.972 de 224.817 celdas (las demás tocan dos radios, y ahí la web promedia tasas).

## Cruce único radio → celda (esquema `cruce`)
Un cruce por año censal, para que cada censo llegue a las celdas con un peso de su época. Los edificios de Open Buildings son de ~2023: repartir con ellos la población de 1991 la pondría en barrios que no existían. El método de cada año y de cada tamaño de radio se eligió con una validación medida, con una regla fijada antes de ver los resultados. Todo el detalle está en el docstring de `build_hexoma_cruce.py` y en `validacion_cruce.md`.

**Pesos de celda** (`cruce.celda_pesos`, 378.105 celdas):
- edificios de los cruces 2022 de la web;
- m² construidos de GHS-BUILT-S R2023A en 1990, 2000, 2010 y 2020 (píxel de 100 m asignado a la celda de su centro);
- `construida_<época>`: si GHSL marcaba construcción en la celda o en su anillo 1.

**Métodos candidatos** (peso de una celda dentro de un radio = fracción de la celda en el radio × …):
- **área:** sin peso, densidad pareja;
- **edificios:** de ~2023;
- **ghsl:** m² construidos de la época;
- **edificios_ghsl:** edificios sólo donde GHSL ya marcaba construcción en la época.

**Validación:** se reparte la población de cada fracción censal entre sus radios y se compara con la real (WAPE = Σ|est − real| / Σ real).
- El área es mala siempre: 71–82 % de error total y hasta 207 % en radios rurales.
- En radios urbanos (< 1 km²) ganan los edificios: 26–31 %, contra 29–33 % de GHSL.
- En radios intermedios y rurales gana GHSL de la época: en los rurales de 2022, 57 contra 68 % de los edificios; en 1991, 39 contra 52 %. Una hipótesis: en el campo, Open Buildings cuenta galpones, secaderos y gallineros como si fueran viviendas.

**Elegido:**
| Año | Urbano (< 1 km²) | Intermedio (1–10 km²) | Rural (> 10 km²) |
|---|---|---|---|
| 1991 | ghsl | ghsl | ghsl |
| 2001 | edificios_ghsl | ghsl | ghsl |
| 2010 | edificios | ghsl | ghsl |
| 2022 | edificios por punto (el cruce de la web) | edificios por punto | **ghsl** |

**2022 rural difiere de la web:** las capas censales 2022 de spatia.ar reparten los radios rurales por edificios, y el Hexoma (`cruce.radio_h3`, `panel`) por GHSL, porque la validación lo mide mejor. `censo_2022.v_h3` sigue con el cruce de la web. Si se confirma, conviene llevarlo también a la web.

| Objeto | Contenido |
|---|---|
| `cruce.celda_pesos`, `cruce.celda_geom` | Pesos y polígono de cada celda con algún peso |
| `cruce.celda_radio` | Fracción de cada celda en cada radio de cada año |
| `cruce.validacion` | Error de cada método por año y tamaño de radio |
| `cruce.radio_h3` | **El cruce oficial:** (anio, h3, territorio_id, redcode, peso, metodo). El peso suma 1 por radio; `metodo` es el usado para ese radio (si no tenía peso con el elegido, el siguiente de la cadena edificios_ghsl → edificios → ghsl → área: hoy, 2 radios por año caen a área) |
| `cruce.celda_confianza` | Por celda y año: `n_radios`, `personas_est`, `parte_radios_grandes` (parte de las personas que viene de radios de más de 10 km², donde la ubicación dentro del radio es gruesa), `usa_area`, `usa_muestra` (algún radio con factor de muestra > 1,5: tasas ruidosas) |

Verificado el 1-nov-2026: el peso suma 1 por radio en los cuatro años, y toda la población oficial de los radios con geometría cae en celdas (2.822.439 en 1991, 3.366.826 en 2001, 3.679.609 en 2010, 4.213.128 en 2022).

## Panel censal 1991–2022 (esquema `panel`)
Treinta y ocho indicadores con la misma definición en los años en que aparecen (`censo_specs.PANEL`), por radio de cada año y por celda. Para comparar entre años sirve la celda, porque los radios se parten y cambian de código. Cada columna dice en su COMMENT qué años tiene y qué cuidar, por ejemplo: 2001 no preguntó agua de red ni combustible; 2010 no permite armar la condición de actividad por radio; 2022 cuenta sólo viviendas particulares.

| Objeto | Contenido |
|---|---|
| `panel.definiciones` | Indicador × año: columna de origen en `censo_<año>.indicadores`, rótulo y nota |
| `panel.radio` | (anio, redcode, indicador, num, den) en la grilla de cada año |
| `panel.v_h3` | Materializada, por (anio, h3, territorio): `<ind>_num`, `<ind>_den` o el total, = Σ peso × conteo del radio, más la confianza de la celda |
| `panel.v_h3_tasas` | Las tasas (%) de `v_h3` |

Verificado el 1-nov-2026: por provincia y año, la población y el NBI de las celdas son iguales a los de los radios (población ±12 por redondeo; NBI con dos decimales). Lo mismo con el NBI publicado por el INDEC en 2001 y 2010.

## Clima y suelo (esquemas `clima` y `suelo`, fase A1 del potencial agrícola)
Decidido el 1-nov-2026: el potencial agrícola pasa de un puntaje 0–100 a **series empíricas en unidades físicas** por celda. A1 es la base: clima por campaña y suelo y relieve en unidades. Siguen A2 (MapBiomas Argentina Col. 3, uso por año), A3 (MODIS) y A4 (aptitud por cultivo con umbrales citados).

**Reglas:**
- **Resolución de origen.** Cada serie se guarda por píxel de su fuente: CHIRPS 0,05° (~70 celdas), ERA5-Land 0,1° (~1.100 celdas). Una celda lee el píxel que contiene su centro: no hay interpolación, y las celdas de un mismo píxel repiten el valor porque eso es lo que la fuente mide. `clima.hex_pixel` lleva de la celda a sus píxeles.
- **Tiempo.** Campaña agrícola julio–junio (`campania` = año de inicio), estación (DEF del año de diciembre) y **heladas por año calendario**, para no partir el invierno.
- **Normal 1991–2020 del propio píxel** (`normal_1991_2020`) y anomalías contra ella (`anomalia`: valor − normal, % de la normal y `pct_ref`, el % de los 30 años con valor menor). Cada celda se compara con su propia historia.
- **ET0** FAO-56 Penman-Monteith de referencia, diaria, con radiación neta calculada como en FAO-56 (pasto, albedo 0,23); prueba contra el ejemplo 18 de FAO-56 en `clima_a_pixel.py`.
- **Suelo:** SoilGrids 2.0, 0–30 cm ponderado por espesor, píxel de 250 m que contiene el centro. Es un modelo, no una medición. **Relieve:** FABDEM (sin dosel ni edificios), media de los píxeles de 3" con centro en la celda (mediana: 12).

| Objeto | Contenido |
|---|---|
| `clima.fuentes`, `suelo.fuentes` | Asset, período, rótulo y sha256 de cada archivo bajado |
| `clima.pixel`, `clima.hex_pixel`, `clima.par` | Píxeles (con geometría y, en ERA5, altitud del modelo), celda → píxeles y par CHIRPS×ERA5 para el balance |
| `clima.chirps_campania`, `chirps_estacion` | Lluvia, días ≥ 1 y ≥ 50 mm, racha seca (y la de octubre–marzo) |
| `clima.era5_campania`, `era5_estacion` | Grados-día base 10, días > 35 °C, medias de máximas y mínimas, ET0, humedad del suelo en 3 capas |
| `clima.era5_helada` | Por año: días con Tmin < 0, < −2 y ≤ 3 °C, primera y última helada, mínima absoluta |
| `clima.balance_campania`, `balance_estacion` | Lluvia − ET0 |
| `clima.normal_1991_2020`, `clima.anomalia` | Normales y anomalías (campañas y heladas) |
| `clima.v_h3_campania`, `v_h3_estacion`, `v_h3_helada`, `v_h3_anomalia` | Lo mismo por celda. Filtrar por `h3` (consulta de una celda: ~10 ms) |
| `clima.validacion` | Métricas contra estaciones |
| `suelo.h3` | Arcilla, arena, limo, COS, pH, CIC, densidad aparente, fragmentos, N; pendiente y elevación media, mínima y máxima |

**Validación contra 48 estaciones, 1991–2025: 17 de GHCN-Daily y 31 del INTA (SIGA)** (`validacion_clima.md`; el COMMENT de cada columna lleva la cifra):
- **Sirven:** lluvia por campaña (r mediana 0,92; +10 %), grados-día (−1 %, r 0,84; pero −250 a −370 °C·d, hasta −10 %, en las sierras de Misiones), Tmin y Tmax diarias como variación (r 0,96 y 0,94).
- **No sirven como valor absoluto:** ERA5-Land da mínimas +1,5 °C y máximas −2,1 °C (mediana). Por eso **casi no marca heladas**: en abrigo las estaciones tienen 0,3–6 días bajo 0 °C por año (más en el Chaco y en Mercedes que en las sierras de Misiones: Bernardo de Irigoyen 1,1, Cerro Azul 1,0) y ERA5-Land ~0; **a 5 cm del suelo**, las convencionales del INTA registran 8–24 por año (Cerro Azul 9,5, Colonia Benítez 24). También subestima los días > 35 °C. AgERA5 (`projects/climate-engine-pro/assets/ce-ag-era5-v2/daily`) se probó el 1-nov-2026 y tiene el mismo sesgo. CHIRPS diario da ~36 días de lluvia por campaña de más: rachas y conteos de días son orientativos.
- Las sierras de Misiones no están en GHCN-Daily: entran por las estaciones del INTA (SIGA; `bajar_siga.py` usa la API `CdnaUV0iiERRpFQE.php` de https://siga.inta.gob.ar, la misma de la aplicación; las rutas `http://…/document/series/*.xls` del paquete R `siga` ya no responden). Las convencionales traen además la mínima a 5 cm (helada a nivel del suelo).
- El GDD de la capa web ((Tmedia mensual − 10) × 30) difiere sólo −1,4 a −1,7 % del diario en Posadas, Bernardo de Irigoyen y Las Lomitas (2019–2024): el problema de esa capa es la escala 0–100, no la fórmula.

Tamaño: `clima` 2,0 GB (la mitad es `anomalia`), `suelo` 0,36 GB.

## Uso del suelo por celda (esquema `uso`, fase A2)
MapBiomas Argentina Colección 3 (1985–2025, ~30 m, 19 clases), por celda: `uso.h3_anio` (‰ por grupo cada 5 años),
`uso.h3_trayectoria` (dominante 1985 y 2025, % de bosque y agrícola, primer año agrícola, año de pérdida de bosque) y
la serie anual completa por clase en `%HEXOMA_FUENTES%/uso/uso_h3_anio_clase.parquet` (1,08 GB, DuckDB). En 2025: bosque
5,66 M ha en Chaco, 5,79 M en Formosa, 1,61 M en Misiones; bosque del NEA 16,2 M ha en 1985 → 13,8 M en 2025.
Validación contra el MAGyP por departamento (`validar_uso.py`, `validacion_uso.md`): cultivo temporal r 0,87–0,95,
perenne r 0,81–0,89; MapBiomas sobreestima la superficie (×1,3–3), ubica bien la agricultura. **Trampa:** los códigos
de departamento del Hexoma en Chaco y Formosa no son los del INDEC/MAGyP: unir por nombre.
`python pipeline/hexoma/build_hexoma_uso.py` (~45 min; `--solo-carga` usa lo ya bajado) y después `validar_uso.py`.
**Capa web (desde el 3-nov-2026):** `python pipeline/hexoma/exportar_uso_web.py` (~2 min) escribe `sat_uso_suelo` (clase
dominante 2025 y 1985 con «mosaico» si ninguna llega al 50 %, trayectoria, % por clase) y los resúmenes con ranking y
serie cada 5 años; `--evaluar` sólo imprime la tabla del umbral. Validación y paleta: `validacion_uso_web.md`.
Reemplazó a Dynamic World en spatia.ar (sólo NEA argentino).

## Aptitud por cultivo (esquema `aptitud`, fase A4)
9 cultivos (yerba, té, cítricos, soja, maíz, algodón, girasol, arroz sin agua de riego, pasturas megatérmicas) con los
umbrales validados por el autor (`umbrales_cultivos.md`, reglas en `meta.umbrales_reglas`), la temperatura corregida
y el suelo de SoilGrids/FABDEM; anegamiento = bosque y pastizal inundables de MapBiomas 2025. `aptitud.clima_campania`
(par × campaña × cultivo), `aptitud.suelo`, `aptitud.resumen` (clase mediana, % de campañas S1 y S1+S2, factor
limitante) y `aptitud.v_h3_campania`. Regenerar: `corregir_temperatura.py` → `aptitud_clima.py` →
`build_hexoma_aptitud.py` → `validar_aptitud.py`. Resultado de la validación y limitaciones en `validacion_aptitud.md`
y en la ficha `aptitud_cultivo` de `meta.procesos`. Tras los ajustes de aplicación del 1-nov-2026 (sección de
`umbrales_cultivos.md`), S1 apunta a donde se cultiva en algodón (lift 2,33), té (1,81), girasol (1,75), maíz (1,63) y
soja (1,50, con el pH argentino 6–7); pasturas sin S1 (el óptimo no se alcanza en el NEA); para cítricos no hay prueba
válida contra MapBiomas; las correlaciones por departamento con el MAGyP son débiles a moderadas.

**En spatia.ar (desde el 1-nov-2026).** La capa «potencial agrícola» de la web sale de acá, sin puntajes y sólo
en el NEA argentino: `python pipeline/hexoma/exportar_agri_web.py` escribe `pipeline/output/hexoma/web/{prefijo}sat_agri_apt*.parquet`
(valores físicos normales 1991–2020, `apt_<cultivo>_clase` y `apt_<cultivo>_pct`) y los cuatro resúmenes con el ranking por
cultivo en `src/lib/data/`. Después: subir a R2 con las claves `data/…` (`upload_to_r2.upload_file`), `npm run purge-cdn` y,
si cambia el esquema, subir `sat_agri_apt` en `getParquetUrl`. `build_layer_ranks.py` ya no toca esta capa.

## Estaciones: la referencia obligada (esquema `estaciones`)
Toda afirmación del Hexoma sobre temperatura, heladas o lluvia se contrasta con las estaciones: son la única medición
directa. 50 estaciones (17 GHCN-Daily, 33 INTA SIGA), 526 mil días tal cual, con un QC que marca y no borra
(`diario.qc`), agregados anuales y por campaña con completitud, normales observadas 1991–2020 (≥ 20 años completos)
y la estación más cercana a cada celda (`h3_cercana`, 58 km en promedio). La ficha de cada celda la muestra al lado
de cada serie.

**Corrección de heladas y calor** (`corregir_temperatura.py`, tablas `clima.era5_*_corr`): mapeo de cuantiles mensual
de Tmin y Tmax de ERA5-Land con 40 estaciones, trasladado por 1/d² de las 4 más cercanas a < 150 km. Se eligió por
validación cruzada (cada estación sin ella ni sus vecinas a < 10 km) con una regla fijada antes (≥ 25 % de mejora):
heladas 11,9 → 7,4 días/año de error, días > 35 °C 28,9 → 16,5, helada a 5 cm 15,6 → 9,2. Normales corregidas:
heladas en abrigo Chaco 4,9, Corrientes 2,6, Formosa 2,4, Misiones 1,5 por año. Para heladas y calor se usan las
`_corr`; las crudas quedan para comparar.

## Catálogo metodológico (esquema `meta`)
`meta.fuentes` (29: clima, suelo, estaciones, censos, GHSL, edificios y las 15 capas de la web) y `meta.procesos` (13)
tienen la ficha pormenorizada: proveedor, versión, acceso, licencia, cita y DOI, **tipo de dato** (medición,
estimación satelital, reanálisis, modelo, censo, derivado), resoluciones, procesamiento paso a paso, asignación a la
celda, validación, limitaciones y usos. `meta.variables` tiene una fila por columna de todo el Hexoma (2.875) con su
definición y sus fuentes; `meta.v_ficha` pone al lado la validación y las limitaciones de la fuente:
```sql
SELECT columna, definicion, fuente, tipo_dato, limitaciones FROM meta.v_ficha WHERE objeto = 'era5_helada_corr';
```
**Soporte** (`meta.variables.soporte`, desde el 4-nov-2026): para cada columna de las capas de la web (`capa_*` y
`v_hexoma`), el área sobre la que el dato se midió de verdad. Todas se guardan en res-9 (~0,105 km²), pero una tasa
censal es del radio, gdd es del píxel ERA5-Land de 0,1° y la pérdida de Hansen es del píxel de 30 m. Sale de
`metodologia.SOPORTES` (patrones por columna) y la carga falla si una columna de `capa_*` queda sin soporte.
Diagnóstico que lo motivó (% de celdas iguales a todas sus hermanas res-8): tasas censales 73–79 % (98 % constantes
dentro del radio), gdd 87 %, precipitación 77 %; Hansen, JRC, VIIRS y SoilGrids < 1 %. Antes de un Moran, un
histograma o un ranking sobre celdas, mirá el soporte: si es el radio o un píxel grueso, el n efectivo es el de esas
unidades, no el de las celdas.
```sql
SELECT columna, soporte FROM meta.variables WHERE objeto = 'capa_service_deprivation' AND soporte IS NOT NULL;
```
La fuente de verdad es `pipeline/hexoma/metodologia.py`: al agregar una fuente o un proceso se edita ahí y se corre
`build_hexoma_meta.py`. El relevamiento de las capas de la web (1-nov-2026) encontró inconsistencias entre código y
textos de spatia.ar, anotadas en sus fichas: el CI de deforestación corre en percentil local aunque la capa
figura como comparable; el año de VIIRS de actividad económica no está determinado; el modelo forestal se
describe con dos nichos y el script entrena uno.

## Extractos para artículos (`extracto.py`)
El Hexoma se regenera desde las fuentes y desde la web viva, así que un número publicado tiene que poder rastrearse. `extracto.py` corre una consulta y deja en la carpeta destino:
- `<nombre>.parquet`;
- `<nombre>.sql`;
- `<nombre>.manifest.json`: fecha, commit de neahub y si había cambios sin commitear, objetos usados con su COMMENT, COMMENT de cada columna, métodos de cruce por año con su error de validación, fuentes de los externos, filas y sha256.

Una capa de Spatia o una exportación para terceros son este mismo extracto más un paso de formato; esos adaptadores se escriben cuando se pidan.

## Convenciones (no obvias)
- **Sin dato = NULL, nunca cero.** `avg()` y `percentile_cont()` los saltean; `count(*)` no.
- **Valor = `(rawCol ?? col) × scale`**, como `src/lib/utils/ficha.ts` (`displaySpec`). Las
  tablas `capa_*` guardan las columnas crudas; `v_hexoma` ya aplica la escala.
- **Departamento = archivo, no polígono.** Los parquets por departamento asignan cada
  celda por mayoría de área de los radios censales, no por el polígono que contiene su
  centro. El ámbito departamento es el conjunto de filas de ese archivo, que es lo que
  muestra la ficha. `v_hexoma.departamento` sale del índice del enlace y, si no, del
  polígono.
- **`principal`:** una celda en dos archivos de departamento de un mismo territorio
  conserva ambas filas, y `principal` marca la que abre la ficha. Hoy hay 0 casos: los
  119 de la carga inicial eran archivos viejos en R2.
- **Ámbitos:**
  - **departamento:** el archivo;
  - **provincia:** el territorio;
  - **región:** sólo en las capas `comparable: true`. Las físicas y modeladas juntan todos
    los territorios cargados (`todos`). Las censales comparables (carencias de servicios,
    salud, capital y trayectoria educativa) quedan **sólo dentro de su país** (`censo_ar`):
    INDEC, DGEEC e IBGE nunca se mezclan en una distribución.
  - `censo_temporal` es territorial, sin ámbito departamento.
- **Rangos** como en la ficha: `lt` = % de celdas con valor estrictamente menor, `eq` = %
  de celdas con el mismo valor, ambos sobre n. Se guardan en `smallint`, en centésimos de
  punto (0–10000; error de ±0,005 pt). La ficha dice «más alto que floor(lt)» y «más bajo
  que floor(100 − lt − eq)».
- **Celdas en dos territorios** (948, en los límites entre provincias): en departamento y
  provincia cuentan en ambos. En la región cuentan una sola vez, en el territorio que abre
  el enlace `?h3=`; su otra fila tiene rango regional NULL.

## Verificar contra la ficha publicada
```bash
PYTHONIOENCODING=utf-8 python scripts/verify_ficha.py https://www.spatia.ar 89a946191b3ffff 89a95099b8fffff 89b261968a7ffff > ficha.txt
PYTHONIOENCODING=utf-8 python pipeline/hexoma/verificar_ficha.py ficha.txt     # «N OK, 0 DIFF»
```
El 31-oct-2026 dio 75/75 (5 celdas × 15 capas). Esa comparación destapó tres errores de la
web, ya corregidos:
- archivos viejos en 3 departamentos de Misiones (`c656fdb`);
- el enlace `?h3=` que abría un departamento sin la celda (`00199e7`);
- la ficha, que contaba los «sin dato» como 0 en la distribución (`9bcad6c`).

## Escenarios
`escenarios.sql` trae cinco consultas comentadas, con tiempo y un resultado de muestra:
1. frontera de deforestación aislada;
2. inundación y carencias;
3. uso actual del suelo donde hubo pérdida EUDR después de 2020;
4. población 1991–2022 según el aislamiento;
5. hogares que cocinan con leña o carbón donde el PM2.5 es alto (esquema `censo_2022`);
6. la campaña 2022/23 frente a su normal, por departamento (esquema `clima`);
7. heladas agrometeorológicas y altitud en Misiones (esquemas `clima` y `suelo`; muestra el sesgo de ERA5-Land).

## Pendiente (hoja de ruta del 31-oct-2026)
Lo nuevo va en esquemas propios, y `v_hexoma` queda como la foto fiel de la web.
1. **Series anuales por celda:**
   - pérdida de Hansen v1.13, 2001–2025;
   - VIIRS, fuego y VCF (CHIRPS y ERA5-Land: hecho el 1-nov-2026, esquema `clima`);
   - MapBiomas resumido (falta confirmar la cobertura de Corrientes);
   - pérdida oficial de la UMSEF.

   Las series por radio de Misiones ya bajadas (`pipeline/output/radio_data/*_annual.parquet`) pueden entrar por `censo_2022.cruce` (o por el cruce único, cuando esté).
2. **Capas institucionales:** OTBN, áreas protegidas, comunidades indígenas (RENACI) y catastro o tamaño de parcela.
3. **Arreglos de capas:** plantaciones contra selva en Misiones, y componentes en unidades físicas en lugar de 0–100 (potencial agrícola: A1 hecha el 1-nov-2026, esquemas `clima` y `suelo`).
4. **Vecindad con SQL** (`h3_grid_disk`). ~~Una tabla con la resolución de origen de cada variable~~: hecho el
   4-nov-2026 (`meta.variables.soporte`).
5. ~~Un extracto congelado por paper~~: hecho el 1-nov-2026 (`extracto.py`).

Descartado: pasar a res-10 o res-11, porque da precisión falsa en las censales y en las capas de 250 m a 5 km.

Paraguay (17 departamentos) y Brasil (3 estados) siguen postergados. La carga ya los soporta (`--territories`), y las distribuciones guardan el país en el ámbito región de las censales.
