# Validación de los cruces camino × cauce de Misiones (`infra.cruces`)

Generado por `pipeline/infra/validar_cruces.py` el 2026-11-07. Método en los docstrings de `drenaje.py`, `cruces.py` y `aislamiento.py`.

## Sensibilidad de la detección

Regla fijada antes de mirar (plan del 7-nov-2026): la detección por terreno sola debe encontrar ≥ 80% de los puentes sobre cauces mapeados y de los puentes RP2, a ≤ 60 m. «Terreno» = los picos de área drenada antes de unir con OSM (`infra.cruces_terreno`). «Final ≤ 60 m» es circular para puentes y alcantarillas: desde la segunda corrida esas estructuras también entran como detección y fijan la ubicación del cruce.

| Referencia | n | Terreno ≤ 60 m | Terreno ≤ 150 m | Final ≤ 60 m |
|---|---|---|---|---|
| Puentes OSM vehiculares sobre un cauce mapeado | 1,026 | 87.0% | 97.0% | 98.1% |
| Puentes RP2 del visor | 29 | 89.7% | 96.6% | 89.7% |
| Puentes OSM vehiculares (todos) | 1,091 | 83.8% | 94.9% | 97.5% |
| Alcantarillas del visor | 1,771 | 66.7% | 91.9% | 94.5% |
| Alcantarillas OSM | 1,778 | 66.4% | 91.6% | 94.4% |

**Regla tal como se fijó (terreno ≤ 60 m): PASA.** Con tolerancia de 150 m: pasa.

Historia de la regla (7-nov-2026). La primera corrida dio 76 % (puentes OSM) y 66 % (RP2) y no pasaba, por dos errores de medición, no de detección: (1) medía la ubicación del representante de cada grupo (el punto de mayor área drenada, que podía ser la intersección OSM) y no los picos crudos del terreno; (2) contaba puentes peatonales y de senderos, que no están en `vial.caminos` y no se pueden detectar. Medida sobre los picos crudos (`infra.cruces_terreno`) y puentes vehiculares, la regla pasa sin cambiar ventana ni umbral.

Diagnóstico de lo que falta. Los puentes (peatonales incluidos) que el terreno no encuentra a ≤ 60 m tienen casi siempre un pico de terreno cerca: distancia mediana 114 m, P90 376 m (181 puentes). A ±1 celda del puente el área drenada suele ser < 0,1 km² y a ±3 celdas casi nunca: el cauce de FABDEM 30 m queda corrido del puente que OSM dibujó sobre la imagen. Los puntos RP2 del visor sí están sobre el camino (0 m de mediana, máx. 6 m). Por eso cada cruce por terreno se ancla a la estructura o a la intersección OSM más cercana de su mismo camino o de uno que lo toca, a ≤ 150 m (`desplazamiento_m`): la ubicación de OSM es la más precisa.

## Cruces por estructura

| estructura | cruces | terreno_y_osm | solo_terreno | solo_osm | paralelos | area_mediana_km2 |
|---|---|---|---|---|---|---|
| sin_dato | 38,281 | 1,172 | 36,951 | 158 | 2,496 | 0.3 |
| alcantarilla | 1,674 | 1,515 | 9 | 143 | 149 | 1.8 |
| puente | 1,037 | 949 | 38 | 30 | 88 | 13.1 |
| vado | 679 | 604 | 6 | 63 | 61 | 3.9 |

## Cruces por clase de camino y área drenada

| clase_camino | cruces | <1 km² | 1–10 | 10–100 | ≥100 | puentes | sin_dato |
|---|---|---|---|---|---|---|---|
| huella | 20,831 | 14,139 | 4,517 | 1,596 | 579 | 63 | 20,133 |
| vecinal | 12,039 | 8,300 | 2,659 | 851 | 229 | 469 | 10,609 |
| urbana | 4,053 | 3,159 | 704 | 173 | 17 | 151 | 3,615 |
| terciaria | 1,331 | 841 | 318 | 136 | 36 | 118 | 1,055 |
| detectado por imagen | 1,103 | 758 | 214 | 72 | 59 | 1 | 1,094 |
| primaria | 739 | 489 | 166 | 55 | 29 | 87 | 557 |
| secundaria | 685 | 476 | 156 | 37 | 16 | 65 | 507 |
| troncal | 517 | 345 | 114 | 42 | 16 | 71 | 362 |
| servicio | 373 | 271 | 71 | 20 | 11 | 12 | 349 |

## Aislamiento

| cruces | sobre_arista_de_corte | aislan_a_alguien | aíslan ≥100 | aíslan ≥1000 | en_islas_de_la_red | dist_arista_p50_m |
|---|---|---|---|---|---|---|
| 41,671 | 13,132 | 4,070 | 70 | 0 | 32 | 0.0 |

Los 20 cruces que más personas aíslan:

| id | municipio | cauce | km2 | estructura | clase_camino | superficie | personas | hosp | caps |
|---|---|---|---|---|---|---|---|---|---|
| 5,902 | Municipio de San Vicente | — | 0.2 | sin_dato | vecinal | tierra | 834 | 0 | 0 |
| 5,903 | Municipio de San Vicente | — | 0.1 | sin_dato | vecinal | tierra | 815 | 0 | 0 |
| 5,904 | Municipio de San Vicente | — | 0.1 | sin_dato | vecinal | tierra | 804 | 0 | 0 |
| 15,024 | Municipio de Veinticinco de Mayo | Arroyo Tamanduá | 191.9 | sin_dato | vecinal | tierra | 778 | 0 | 0 |
| 164 | Municipio de Veinticinco de Mayo | Arroyo Tamanduá | 191.6 | puente | vecinal | tierra | 778 | 0 | 0 |
| 15,025 | Municipio de Veinticinco de Mayo | — | 0.3 | sin_dato | vecinal | tierra | 740 | 0 | 0 |
| 17,438 | Municipio de San Ignacio | — | 0.5 | sin_dato | vecinal | ripio/consolidado | 610 | 0 | 1 |
| 4,390 | Municipio de Posadas | Arroyo Antonica | 1.1 | sin_dato | urbana | desconocida | 609 | 0 | 0 |
| 3,854 | Municipio de Aristóbulo del Valle | — | 0.3 | sin_dato | vecinal | tierra | 492 | 0 | 1 |
| 33,537 | Municipio de Colonia Aurora | — | 1.4 | sin_dato | huella | tierra | 485 | 0 | 0 |
| 31,993 | Municipio de Aristóbulo del Valle | — | 5.9 | sin_dato | huella | tierra | 468 | 0 | 0 |
| 31,456 | Municipio de Veinticinco de Mayo | — | 0.8 | sin_dato | huella | tierra | 468 | 0 | 0 |
| 31,994 | Municipio de Aristóbulo del Valle | — | 0.2 | sin_dato | huella | tierra | 468 | 0 | 0 |
| 31,457 | Municipio de Veinticinco de Mayo | — | 5.9 | sin_dato | huella | tierra | 468 | 0 | 0 |
| 18,134 | Municipio de Apóstoles | — | 0.4 | sin_dato | vecinal | tierra | 432 | 0 | 0 |
| 17,397 | Municipio de Puerto Iguazú | — | 0.6 | sin_dato | servicio | ripio/consolidado | 418 | 0 | 0 |
| 12,835 | Municipio de El Soberbio | — | 0.4 | sin_dato | vecinal | tierra | 407 | 0 | 1 |
| 38,733 | Municipio de Puerto Iguazú | Arroyo Tacuara | 4.1 | sin_dato | servicio | desconocida | 375 | 0 | 0 |
| 8,719 | Municipio de Posadas | — | 0.2 | sin_dato | urbana | tierra | 373 | 0 | 0 |
| 15,464 | Municipio de Campo Viera | — | 0.8 | sin_dato | vecinal | tierra | 370 | 0 | 0 |

## Puentes RP2 (control de cordura)

| nombre | cota | cruce | km2 | por_terreno | aisla | personas |
|---|---|---|---|---|---|---|
| Puente sobre el Arroyo Oveja Negra |  | 590 | 18.9 | 1 | 0 |  |
| Puente sobre el Arroyo Itacaraí |  | 16,704 | 13.4 | 1 | 0 |  |
| Puente sobre el Arroyo Yerbas del Paraìso |  | 12,173 | 20.9 | 1 | 0 |  |
| Puente sobre el Arroyo Paraíso |  | 12,174 | 573.8 | 1 | 0 |  |
| Puente Arroyo Soberbio | 138.9 | 421 | 1,074.5 | 1 | 0 |  |
| Puente sobre el Arroyo Tararira | 154.9 | 660 | 29.2 | 1 | 0 |  |
| Puente sobre el Arroyo Los Muertos | 133.2 | 8,789 | 110.2 | 1 | 0 |  |
| Puente sobre el Arroyo Persiguero | 136.7 | 12,656 | 18.1 | 1 | 0 |  |
| Puente sobre el Arroyo Dorado | 139.9 | 8,792 | 73.5 | 1 | 0 |  |
| Puente sobre el Arroyo Chafariz | 136.5 | 1,860 | 319.1 | 1 | 0 |  |
| Puente sobre el Arroyo Doradito | 130.3 | 8,788 | 34.7 | 1 | 0 |  |
| Puente sobre el Arroyo Saltiño | 127.5 | 8,791 | 862.4 | 1 | 0 |  |
| Puente sobre el Arroyo Macaco | 124.8 | 8,790 | 30.3 | 1 | 0 |  |
| Puente sobre el Arroyo Pindaití | 118.7 | 8,793 | 339.5 | 1 | 0 |  |
| Puente sobre el Arroyo Chancay | 118.1 | 8,787 | 40.6 | 1 | 0 |  |
| Puente sobre el Arroyo Santa Rita | 119.0 | 8,794 | 20.3 | 1 | 0 |  |
| Puente sobre el Arroyo Canal Torto | 124.2 | 6,763 | 742.9 | 1 | 0 |  |
| Puente sobre el Arroyo Acaraguá | 110.2 | 372 | 774.8 | 1 | 0 |  |
| Puente sobre el Arroyo Ramón | 102.4 | 11,238 | 340.8 | 1 | 0 |  |
| Puente sobre el Arroyo Tomás Guido | 102.7 | 11,470 | 95.7 | 1 | 0 |  |
| Puente sobre el Arroyo Alipio | 100.6 | 10,686 | 14.1 | 1 | 0 |  |
| Puente sobre el Arroyo Once Vueltas | 101.0 | 1,720 | 391.9 | 1 | 0 |  |
| Puente sobre el Arroyo Guerrero | 96.7 | 5,644 | 118.7 | 1 | 0 |  |
| Puerte sobre el Arroyo Portera | 96.4 | 14,252 | 55.0 | 1 | 0 |  |
| Puente sobre el Arroyo Itacaruaré | 97.6 | 41,395 | 0.0 | 0 | 0 |  |
| Puente sobre el Arroyo Santa María | 97.4 | 11,665 | 136.6 | 1 | 0 |  |
| Puente sobre el Arroyo Persiguero | 98.6 | 14,577 | 1.9 | 1 | 0 |  |
| Puente sobre el Arroyo Concepción | 100.4 | 19,925 | 24.8 | 1 | 0 |  |
| Puente sobre el Arroyo Tunas | 77.3 | 4,051 | 362.6 | 1 | 0 |  |

## Mapas

Locales, fuera del repo: `pipeline/output/infra/cruces_el_soberbio.html`, `pipeline/output/infra/cruces_eldorado.html`. Color = estructura (azul puente, verde alcantarilla, ocre vado, rojo sin dato); borde negro = arista de corte; tamaño = área drenada.
