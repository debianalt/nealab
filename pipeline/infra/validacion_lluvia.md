# Validación del día de lluvia en Misiones (`infra.acceso_lluvia`, columnas de lluvia de `infra.cruces`)

Generado por `pipeline/infra/validar_lluvia.py` el 2026-11-07. Método en los docstrings de `red_lluvia.py` y `desvios.py`.

## Supuestos

Velocidades en km/h (seco / lluvia), supuestas: pavimento {'troncal': 80, 'primaria': 70, 'secundaria': 60, 'terciaria': 50, 'vecinal': 40, 'urbana': 30, 'servicio': 20, 'huella': 30, 'detectado por imagen': 30, 'conector': 20} (con lluvia × 0.85); ripio (40, 30); tierra {'troncal': (40, 20), 'primaria': (40, 20), 'secundaria': (40, 20), 'terciaria': (40, 20), 'vecinal': (35, 15), 'urbana': (25, 12), 'huella': (20, 8), 'servicio': (15, 8), 'detectado por imagen': (20, 8), 'conector': (20, 8)}. Tierra peligrosa con lluvia fuerte = pendiente ≥ 10 %. Cota optimista: superficie desconocida = pavimento en calles urbanas y rutas, ripio en el resto; pesimista: = tierra. Los km de tierra del camino no dependen de las velocidades.

## 1. Superficie: OSM contra IGN

Puntos cada 100 m sobre los tramos del IGN con superficie conocida en Misiones, contra el camino de `vial.caminos` más cercano a ≤ ~20 m. Pavimento contra no pavimento.

| ign | osm | etiquetada_en_osm | puntos |
|---|---|---|---|
| pavimento | desconocida | 0 | 935 |
| pavimento | desconocida | 1 | 2 |
| pavimento | pavimento | 0 | 168 |
| pavimento | pavimento | 1 | 39,385 |
| pavimento | ripio/consolidado | 1 | 24 |
| pavimento | tierra | 0 | 406 |
| pavimento | tierra | 1 | 1,277 |
| tierra | desconocida | 0 | 969 |
| tierra | desconocida | 1 | 1 |
| tierra | pavimento | 0 | 2 |
| tierra | pavimento | 1 | 1,486 |
| tierra | ripio/consolidado | 1 | 153 |
| tierra | tierra | 0 | 1,892 |
| tierra | tierra | 1 | 8,472 |

Acuerdo pavimento / no pavimento (sin las desconocidas): **94.0%**. De lo que el IGN dice tierra, OSM dice no pavimento en el 87.6%. El IGN cubre sobre todo rutas provinciales y terciarias: no valida las huellas, donde la tierra es por regla.

## 2. Población y monotonía

| escenario | cota | personas |
|---|---|---|
| lluvia | optimista | 1,273,347 |
| lluvia_fuerte | optimista | 1,273,347 |
| seco | optimista | 1,273,347 |
| todo_tiempo | optimista | 1,273,347 |
| lluvia | pesimista | 1,273,347 |
| lluvia_fuerte | pesimista | 1,273,347 |
| seco | pesimista | 1,273,347 |
| todo_tiempo | pesimista | 1,273,347 |

| cota | lluvia_menor_que_seco | fuerte_menor_que_lluvia |
|---|---|---|
| optimista | 0 | 0 |
| pesimista | 0 | 0 |

## 3. Minutos al hospital en seco contra la capa de accesibilidad de la web

Regla fijada antes de mirar: Spearman ρ ≥ 0.7 por celda. La capa de la web usa la superficie de fricción motorizada de Oxford/MAP 2019 (1 km) y la capa de salud del IGN; no es verdad de campo.

| cota | celdas | rho | mediana nuestra | mediana web | dif. mediana |
|---|---|---|---|---|---|
| optimista | 46,208 | 0.9 | 16.7 | 14.3 | 2.1 |
| pesimista | 46,208 | 0.9 | 17.0 | 14.3 | 2.4 |

**PASA.**

## 4. Pendiente por clase de camino

| clase | km | p50 | p90 | % km ≥ umbral | km tierra peligrosa |
|---|---|---|---|---|---|
| huella | 26,444 | 4.4 | 11.9 | 14.5 | 3,835.0 |
| vecinal | 15,221 | 4.3 | 10.3 | 10.5 | 1,586.0 |
| urbana | 5,913 | 3.2 | 7.6 | 3.5 | 182.0 |
| terciaria | 1,800 | 3.7 | 8.9 | 6.5 | 107.0 |
| secundaria | 1,300 | 2.6 | 6.8 | 2.0 | 3.0 |
| primaria | 1,098 | 3.1 | 7.4 | 3.0 |  |
| detectado por imagen | 944 | 3.5 | 10.2 | 10.5 | 99.0 |
| troncal | 843 | 2.5 | 6.3 | 1.4 |  |
| servicio | 500 | 2.3 | 7.4 | 5.2 | 26.0 |

La pendiente sale de FABDEM 30 m sobre 150 m de camino: en quebradas estrechas puede sobrestimarse (el camino va por la ladera y el modelo de elevación mezcla el fondo).

## 5. Resumen por escenario

| cota | escenario | mediana min | > 30 min | > 60 min | sin ruta | > 5 km de tierra |
|---|---|---|---|---|---|---|
| optimista | seco | 3.2 | 38,288 | 1,201 | 72 | 66,377.0 |
| optimista | lluvia | 3.9 | 91,429 | 19,441 | 72 | 57,404.0 |
| optimista | lluvia_fuerte | 3.6 | 31,873 | 3,653 | 97,723 | 15,344.0 |
| optimista | todo_tiempo | 3.3 | 15,689 | 981 | 322,500 |  |
| pesimista | seco | 3.4 | 39,333 | 1,372 | 72 | 71,443.0 |
| pesimista | lluvia | 4.5 | 95,149 | 20,812 | 72 | 63,484.0 |
| pesimista | lluvia_fuerte | 4.2 | 35,306 | 3,444 | 121,622 | 16,879.0 |
| pesimista | todo_tiempo | 3.8 | 63,129 | 20,998 | 863,564 |  |

«todo_tiempo» exige que TODO el camino sea pavimento o ripio, incluida la cuadra de tierra frente a la casa: es un extremo que sobrecuenta. La medida útil ahí es «> 5 km de tierra».

## 5b. Sensibilidad al umbral de pendiente (lluvia fuerte)

El umbral de tierra intransitable (10 %) es un supuesto, y decide cuánta gente queda sin ruta. La CANTIDAD no es robusta: se informa como rango. Lo que se usa de este escenario es DÓNDE pasa.

| cota | umbral_pct | sin_ruta | mas_60_min |
|---|---|---|---|
| optimista | 6.0 | 182,532 | 63 |
| optimista | 8.0 | 139,709 | 1,270 |
| optimista | 10.0 | 97,723 | 3,653 |
| optimista | 12.0 | 63,999 | 8,300 |
| optimista | 15.0 | 26,587 | 14,683 |
| optimista | 20.0 | 7,178 | 16,804 |
| pesimista | 6.0 | 325,776 | 70 |
| pesimista | 8.0 | 201,260 | 1,275 |
| pesimista | 10.0 | 121,622 | 3,444 |
| pesimista | 12.0 | 73,939 | 8,644 |
| pesimista | 15.0 | 30,271 | 15,816 |
| pesimista | 20.0 | 7,444 | 18,137 |

¿Se mantiene el orden de los municipios? Spearman entre la gente sin ruta por municipio con cada umbral y con el de 10 % (cota pesimista), y cuántos de los 10 primeros coinciden:

| umbral | rho con 10 % | top 10 en común |
|---|---|---|
| 6.0 | 0.8 | 7 |
| 8.0 | 0.9 | 9 |
| 10.0 | 1.0 | 10 |
| 12.0 | 1.0 | 8 |
| 15.0 | 0.9 | 8 |
| 20.0 | 0.7 | 7 |

## 6. Municipios con más gente sin ruta con lluvia fuerte (cota pesimista)

Los minutos medios se calculan sobre quienes TIENEN ruta: con lluvia fuerte bajan en algunos municipios porque los más lejanos pasan a «sin ruta» y salen del promedio, no porque mejore el acceso.

| municipio | personas | sin_ruta_fuerte | min_seco | min_lluvia | min_fuerte | km_tierra_seco |
|---|---|---|---|---|---|---|
| Municipio de El Soberbio | 26,054 | 14,230 | 17.3 | 28.7 | 11.1 | 4.5 |
| Municipio de San Vicente | 46,833 | 12,023 | 9.9 | 16.1 | 8.0 | 2.3 |
| Municipio de Dos de Mayo | 18,867 | 8,964 | 13.5 | 25.4 | 8.4 | 5.2 |
| Municipio de San Pedro | 26,602 | 8,385 | 13.9 | 22.8 | 11.3 | 3.1 |
| Municipio de Veinticinco de Mayo | 13,986 | 6,235 | 12.0 | 23.4 | 6.9 | 4.5 |
| Municipio de Oberá | 76,836 | 3,985 | 4.7 | 6.2 | 6.3 | 0.3 |
| Municipio de Campo Ramón | 9,982 | 3,920 | 19.3 | 28.5 | 20.6 | 3.1 |
| Municipio de Colonia Aurora | 8,548 | 3,757 | 20.2 | 33.0 | 25.3 | 5.4 |
| Municipio de Aristóbulo del Valle | 20,456 | 3,704 | 6.6 | 11.0 | 6.4 | 1.6 |
| Municipio de Pozo Azul | 7,999 | 3,545 | 40.8 | 52.9 | 47.3 | 2.2 |
| Municipio de San Antonio | 10,784 | 3,176 | 11.4 | 17.7 | 9.3 | 2.1 |
| Municipio de Panambí | 6,441 | 2,875 | 29.1 | 36.8 | 34.1 | 1.4 |
| Municipio de Fracrán | 4,262 | 2,720 | 25.2 | 36.8 | 27.0 | 3.7 |
| Municipio de Puerto Piray | 9,611 | 2,628 | 3.8 | 6.5 | 3.3 | 1.2 |
| Municipio de Bernardo de Irigoyen | 14,480 | 2,386 | 5.5 | 7.3 | 6.9 | 0.5 |
| Municipio de Comandante Andresito | 19,683 | 2,192 | 8.4 | 13.8 | 13.2 | 2.6 |
| Municipio de Puerto Iguazú | 54,680 | 2,143 | 3.6 | 5.0 | 4.9 | 0.5 |
| Municipio de Alba Posse | 8,357 | 1,870 | 12.5 | 17.3 | 14.1 | 1.2 |
| Municipio de Puerto Rico | 23,465 | 1,785 | 3.0 | 4.0 | 3.9 | 0.3 |
| Municipio de Dos Arroyos | 3,175 | 1,764 | 16.5 | 23.5 | 17.3 | 2.2 |

## 7. Cruces que más pesan

Por impacto = personas que dependen del cruce para llegar al hospital × minutos de desvío con lluvia (cota pesimista):

| id | municipio | cauce | estructura | clase_camino | superficie | dependen | desvio_seco | desvio_lluvia | impacto (miles min·persona) |
|---|---|---|---|---|---|---|---|---|---|
| 548 | Municipio de Pozo Azul | Arroyo Piray Guazú | puente | secundaria | pavimento | 4,130 | 94.8 | 130.8 | 540 |
| 13,405 | Municipio de Veinticinco de Mayo | Arroyo Barra Grande | puente | vecinal | tierra | 1,953 | 66.3 | 151.2 | 295 |
| 22,576 | Municipio de San Vicente | — | sin_dato | primaria | pavimento | 7,691 | 17.0 | 37.4 | 287 |
| 19,313 | Municipio de San Pedro | — | sin_dato | troncal | pavimento | 8,927 | 14.2 | 30.8 | 275 |
| 8,839 | Municipio de Veinticinco de Mayo | Arroyo Pindaití | puente | vecinal | tierra | 1,756 | 66.2 | 151.0 | 265 |
| 15,348 | Municipio de El Alcázar | — | sin_dato | troncal | pavimento | 3,803 | 43.3 | 67.9 | 258 |
| 17,939 | Municipio de El Alcázar | — | sin_dato | troncal | pavimento | 3,803 | 43.3 | 67.8 | 258 |
| 8,352 | Municipio de El Alcázar | Arroyo Paranay Guazú | puente | troncal | pavimento | 3,796 | 43.3 | 67.9 | 258 |
| 17,938 | Municipio de El Alcázar | — | sin_dato | troncal | pavimento | 3,791 | 43.3 | 67.8 | 257 |
| 15,960 | Municipio de El Alcázar | — | sin_dato | troncal | pavimento | 3,781 | 43.3 | 67.9 | 257 |
| 384 | Municipio de Posadas | Arroyo Zaimán | puente | primaria | pavimento | 38,590 | 5.6 | 6.6 | 256 |
| 13,031 | Municipio de Posadas | — | puente | primaria | pavimento | 38,590 | 5.6 | 6.6 | 256 |
| 613 | Municipio de Pozo Azul | Arroyo Piray Miní | puente | primaria | pavimento | 2,867 | 49.4 | 80.8 | 232 |
| 9,531 | Municipio de El Soberbio | — | sin_dato | primaria | pavimento | 4,288 | 23.3 | 52.6 | 225 |
| 20,404 | Municipio de Guaraní | — | sin_dato | secundaria | pavimento | 26,224 | 6.2 | 8.5 | 222 |
| 39,358 | Municipio de Pozo Azul | — | sin_dato | primaria | pavimento | 2,689 | 49.4 | 80.8 | 217 |
| 9,540 | Municipio de El Soberbio | — | sin_dato | primaria | pavimento | 10,136 | 10.7 | 21.2 | 215 |
| 18,276 | Municipio de El Soberbio | — | sin_dato | primaria | pavimento | 4,021 | 24.2 | 52.0 | 209 |
| 12,672 | Municipio de El Soberbio | — | sin_dato | primaria | pavimento | 3,991 | 24.2 | 52.0 | 208 |
| 39,357 | Municipio de Pozo Azul | — | sin_dato | primaria | pavimento | 2,317 | 54.3 | 80.8 | 187 |

Por aislamiento con lluvia fuerte (sin tierra en pendiente; con todo seco ninguno pasaba de 834). «Aislado» es separado de Posadas: si el lado aislado tiene hospital o CAPS propio (salud_del_lado_aislado > 0), la gente no queda sin atención, queda sin conexión con el resto de la provincia.

| id | municipio | cauce | estructura | clase_camino | superficie | aisla_seco | aisla_fuerte | salud_del_lado_aislado | el_cruce_es_pendiente |
|---|---|---|---|---|---|---|---|---|---|
| 2,212 | Municipio de Puerto Libertad | — | sin_dato | troncal | pavimento |  | 53,344 | 17 | 0 |
| 8,358 | Municipio de Puerto Libertad | Arroyo Urugua-í | alcantarilla | troncal | pavimento |  | 53,344 | 17 | 0 |
| 15,876 | Municipio de Puerto Libertad | — | sin_dato | troncal | pavimento |  | 52,546 | 15 | 0 |
| 15,875 | Municipio de Puerto Libertad | Arroyo Yasú Argentino | alcantarilla | troncal | pavimento |  | 52,541 | 15 | 0 |
| 421 | Municipio de El Soberbio | Arroyo Soberbio | puente | primaria | pavimento |  | 2,856 | 1 | 0 |
| 9,539 | Municipio de El Soberbio | — | sin_dato | primaria | pavimento |  | 2,594 | 1 | 0 |
| 9,540 | Municipio de El Soberbio | — | sin_dato | primaria | pavimento |  | 2,499 | 1 | 0 |
| 811 | Municipio de El Soberbio | — | sin_dato | primaria | pavimento |  | 1,397 | 1 | 0 |
| 812 | Municipio de El Soberbio | — | sin_dato | primaria | pavimento |  | 1,380 | 1 | 0 |
| 15,742 | Municipio de Los Helechos | — | sin_dato | terciaria | pavimento |  | 1,288 | 1 | 0 |
| 16,293 | Municipio de Los Helechos | Arroyo Tomás Guido | alcantarilla | terciaria | pavimento |  | 1,280 | 1 | 0 |
| 16,292 | Municipio de Los Helechos | — | sin_dato | terciaria | pavimento |  | 1,266 | 1 | 0 |
| 16,294 | Municipio de Florentino Ameghino | — | sin_dato | terciaria | pavimento |  | 1,106 | 1 | 0 |
| 9,572 | Municipio de Garuhapé | — | sin_dato | secundaria | pavimento |  | 1,079 | 1 | 0 |
| 9,566 | Municipio de Garuhapé | Arroyo Mocoi | alcantarilla | secundaria | pavimento |  | 991 | 1 | 0 |
| 2,116 | Municipio de Puerto Rico | Arroyo Lita | alcantarilla | secundaria | pavimento |  | 874 | 0 | 0 |
| 41,496 | Municipio de Salto Encantado | — | alcantarilla | secundaria | pavimento |  | 803 | 1 | 0 |
| 16,297 | Municipio de Caraguatay | — | sin_dato | terciaria | pavimento |  | 778 | 1 | 0 |
| 16,298 | Municipio de Caraguatay | — | alcantarilla | terciaria | pavimento |  | 778 | 1 | 0 |
| 19,259 | Municipio de Salto Encantado | — | sin_dato | secundaria | pavimento |  | 764 | 1 | 0 |

## Mapa

Local, fuera del repo: `pipeline/output/infra/lluvia_el_soberbio.html`. Celdas coloreadas por minutos al hospital (verde < 15, ocre < 30, naranja < 60, rojo ≥ 60, bordó = sin ruta); capa «Seco» y «Lluvia fuerte»; puntos = cruces con dependientes o que aíslan ≥ 20 personas con lluvia fuerte.
