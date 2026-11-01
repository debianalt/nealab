# Hexoma

El **hexoma** de una celda H3 es su vector completo en Spatia: todas las capas, en sus
escalas propias y en su posición dentro del departamento, la provincia y la región, más
los cortes temporales que tenga cada capa. **El Hexoma** es la base Postgres local que
los guarda para todo el NEA argentino (Misiones, Corrientes, Chaco y Formosa:
3.299.569 hexágonos res-9, 15 capas), más el censo 2022 completo por radio censal,
unido a cada hexágono (esquema `censo`).

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
| `python pipeline/build_hexoma_censo.py [--refresh]` | Rearma el esquema `censo` (censo 2022 por radio, ver abajo). Es independiente de lo anterior, y `--reset` no lo toca | 85 s |

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

Tamaño medido el 31-oct-2026: **11 GB** en total. El esquema `public` ocupa 10,2 GB (capas, rangos y distribuciones) y el esquema `censo` 0,75 GB.

## Censo 2022 por radio (esquema `censo`)
El censo 2022 completo de las 4 provincias se guarda en su unidad de origen, el radio censal (6.887 radios), y se une a cada celda por el mismo cruce que usan las capas censales de la web. No reemplaza a esas capas: `v_hexoma` sigue siendo la foto de la web.

**Fuente.** El censo del INDEC ya agregado por radio (radio × variable × categoría × conteo, sacado de la base REDATAM), publicado en [Source Cooperative](https://source.coop/nlebovits/censo-argentino) con licencia CC-BY-4.0. Sin microdatos, así que no hay secreto estadístico en juego.

La descarga directa del INDEC (`bases_censo2022_RedatamX.zip`) devolvía la página de inicio el 31-oct-2026. Si vuelve y hacen falta cruces, se puede pasar a los microdatos con el paquete de R `censo2022arg`.

**Qué trae:**
- 76 variables de personas, hogares y viviendas particulares;
- NBI y sus cinco criterios, e IPMH, ya calculados por el INDEC;
- combustible para cocinar, agua, desagüe, materiales, hacinamiento, tenencia, conectividad, migración, educación, actividad, rama, categoría ocupacional, cobertura de salud y jubilación.

**Qué no trae:**
- cruces entre variables (sexo × edad, por ejemplo), porque son frecuencias por radio;
- las variables de la base PO (pueblos originarios y afrodescendientes);
- las viviendas colectivas y la situación de calle. La población es la de viviendas particulares: Misiones 1.273.347, Corrientes 1.209.671, Chaco 1.124.603, Formosa 605.507.

| Objeto | Contenido |
|---|---|
| `censo.categorias` | Todo el censo: conteo por (radio, variable, categoría). Con rótulos: `censo.v_categorias` |
| `censo.variables`, `censo.diccionario` | Qué es cada variable y cada categoría |
| `censo.indicadores` | ~80 indicadores por radio en conteos, `<x>_num` y `<x>_den`. El COMMENT de cada columna dice qué categorías entran en el numerador y en el denominador |
| `censo.v_tasas_radio` | Las tasas (%) y los promedios por radio |
| `censo.radios` | Departamento, tipo de radio (urbano, mixto o rural), localidad, gobierno local, aglomerado, área y geometría |
| `censo.cruce` | Celda → radio. Peso = parte de los edificios del radio que cae en la celda (suma 1 por radio). `fuente = 'area'` marca el respaldo por superficie para un radio de Esquina sin edificios en el cruce |
| `censo.v_h3` | Vista materializada por celda: conteos × peso y tasas (Σ num·peso / Σ den·peso), con `_num` y `_den`. `redcode` es el radio que aporta más hogares. Se une a `v_hexoma` por (`h3`, `territorio`) |

**Cuándo usar cada unidad:**
- **Radio,** para preguntas cuyo dato central es censal: es donde el INDEC midió, con n honesto y errores estándar que se pueden agrupar por radio.
- **Celda,** para cruzar con las capas físicas, sobre todo en el campo, donde hay radios de cientos de km².

Para tasas de un conjunto de celdas o de un departamento, sumá `_num` y `_den` y dividí al final. No promedies tasas.

**Verificado el 31-oct-2026:**
- **Personas por radio:** iguales a los totales publicados en 6.882 de 6.882 radios con población.
- **Tasas por radio:** NBI, hacinamiento crítico, computadora, desocupación, propietarios, inquilinos y piso sin revestimiento coinciden exactamente con las tablas de la web. Las que no coinciden difieren por definición: `pct_agua_red` y `pct_cloacas` de la web son el complemento (sin red), y la web los rotula así.
- **La celda conserva el total:** la suma de `v_h3` reproduce la de los radios (±40 personas por provincia, por redondeo).
- **NBI por celda contra la capa de la web:** igual en 223.972 de 224.817 celdas (r = 0,9997). Las 845 restantes tocan dos radios, y ahí la web promedia tasas ponderando por los hogares estimados en vez de sumar conteos.

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
5. hogares que cocinan con leña o carbón donde el PM2.5 es alto (esquema `censo`).

## Pendiente (hoja de ruta del 31-oct-2026)
Lo nuevo va en esquemas propios, y `v_hexoma` queda como la foto fiel de la web.
1. **Series anuales por celda:**
   - pérdida de Hansen v1.13, 2001–2025;
   - VIIRS, CHIRPS, fuego y VCF;
   - MapBiomas resumido (falta confirmar la cobertura de Corrientes);
   - pérdida oficial de la UMSEF.

   Las series por radio de Misiones ya bajadas (`pipeline/output/radio_data/*_annual.parquet`) pueden entrar por `censo.cruce`.
2. **Capas institucionales:** OTBN, áreas protegidas, comunidades indígenas (RENACI) y catastro o tamaño de parcela.
3. **Arreglos de capas:** plantaciones contra selva en Misiones, y componentes en unidades físicas en lugar de 0–100.
4. **Vecindad con SQL** (`h3_grid_disk`) y una tabla con la resolución de origen de cada variable.
5. **Un extracto congelado por paper,** porque la base se regenera desde la web viva.

Descartado: pasar a res-10 o res-11, porque da precisión falsa en las censales y en las capas de 250 m a 5 km.

Paraguay (17 departamentos) y Brasil (3 estados) siguen postergados. La carga ya los soporta (`--territories`), y las distribuciones guardan el país en el ámbito región de las censales.
