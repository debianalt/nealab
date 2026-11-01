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
| `python pipeline/build_ficha_link_index.py` | Regenera `src/lib/data/ficha_link_index.json`, que usan la web y el Hexoma. Correrlo cuando cambia una partición por departamento | |
| `python pipeline/build_hexoma_censo.py [--anio A] [--refresh]` | Rearma los esquemas `censo_<año>` (ver abajo). Es independiente de lo anterior, y `--reset` no lo toca. 1991 necesita antes `python pipeline/hexoma/redatam_a_radio.py --anio 1991 --csv pipeline/output/hexoma/redatam1991/csv` (los CSV de `ARG1991.zip`) | 40–110 s por año |
| `python pipeline/hexoma/build_hexoma_externos.py` | Rearma el esquema `externos` | 5 s |

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
Cada censo se guarda completo en su unidad de origen, el radio censal de ese año, en su propio esquema. `censo_2022` es el antiguo esquema `censo` (renombrado el 1-nov-2026); es el único que hoy se une a las celdas (`cruce`, `v_h3`), por el mismo cruce que usan las capas censales de la web. Los otros años esperan el cruce único por año (esquema `cruce`, en construcción). Nada de esto reemplaza a las capas de la web: `v_hexoma` sigue siendo la foto de la web.

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
5. hogares que cocinan con leña o carbón donde el PM2.5 es alto (esquema `censo_2022`).

## Pendiente (hoja de ruta del 31-oct-2026)
Lo nuevo va en esquemas propios, y `v_hexoma` queda como la foto fiel de la web.
1. **Series anuales por celda:**
   - pérdida de Hansen v1.13, 2001–2025;
   - VIIRS, CHIRPS, fuego y VCF;
   - MapBiomas resumido (falta confirmar la cobertura de Corrientes);
   - pérdida oficial de la UMSEF.

   Las series por radio de Misiones ya bajadas (`pipeline/output/radio_data/*_annual.parquet`) pueden entrar por `censo_2022.cruce` (o por el cruce único, cuando esté).
2. **Capas institucionales:** OTBN, áreas protegidas, comunidades indígenas (RENACI) y catastro o tamaño de parcela.
3. **Arreglos de capas:** plantaciones contra selva en Misiones, y componentes en unidades físicas en lugar de 0–100.
4. **Vecindad con SQL** (`h3_grid_disk`) y una tabla con la resolución de origen de cada variable.
5. **Un extracto congelado por paper,** porque la base se regenera desde la web viva.

Descartado: pasar a res-10 o res-11, porque da precisión falsa en las censales y en las capas de 250 m a 5 km.

Paraguay (17 departamentos) y Brasil (3 estados) siguen postergados. La carga ya los soporta (`--territories`), y las distribuciones guardan el país en el ámbito región de las censales.
