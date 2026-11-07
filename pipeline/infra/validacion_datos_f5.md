# Integridad de los datos de la fase 5 (arroyos de Misiones)

Generado por `pipeline/infra/validar_datos_f5.py` el 2026-11-07. Sólo datos.

## 1. Hidrografía del IGN

| tabla | n | nota |
|---|---|---|
| infra.ign_canales | 0 | Canales |
| infra.ign_corrientes_area | 19 | Corrientes de agua (polígono: ríos anchos) |
| infra.ign_cursos_intermitentes | 90 | Corrientes de agua intermitentes |
| infra.ign_cursos_perennes | 2,787 | Corrientes de agua perennes |
| infra.ign_embalses | 1 | Embalses |
| infra.ign_embalses_rurales | 0 | Embalses rurales (tajamares) |
| infra.ign_espejos_intermitentes | 0 | Espejos de agua intermitentes |
| infra.ign_espejos_perennes | 3 | Espejos de agua perennes |
| infra.ign_fuentes_naturales | 0 | Fuentes naturales (vertientes) |
| infra.ign_saltos | 17 | Cataratas, cascadas y saltos |

## 2. Red de arroyos derivada (FABDEM 30 m, ≥ 0,1 km²)

| orden | tramos | km | area_mediana_km2 | pendiente_mediana_pct | con_nombre |
|---|---|---|---|---|---|
| 1 | 94,041 | 36,326 | 0.2 | 3.2 | 6,226 |
| 2 | 126,516 | 48,458 | 0.8 | 2.6 | 6,717 |
| 3 | 76,240 | 29,948 | 4.0 | 1.4 | 9,336 |
| 4 | 42,133 | 16,942 | 18.9 | 0.6 | 9,628 |
| 5 | 23,081 | 9,922 | 91.8 | 0.3 | 7,663 |
| 6 | 12,974 | 6,050 | 529.3 | 0.1 | 5,140 |
| 7 | 3,759 | 1,808 | 1,837.6 | 0.1 | 1,904 |
| 8 | 2,514 | 1,326 | 12,339.8 | 0.0 | 1,232 |

Cauces mapeados (puntos cada 200 m, dentro de Misiones) que caen cerca de un tramo derivado. El cauce de FABDEM queda corrido ~100 m del que dibujan OSM e IGN (fase 1), por eso las dos distancias:

| fuente | puntos | % a ≤ 60 m de un tramo | % a ≤ 150 m |
|---|---|---|---|
| IGN perennes | 231,697 | 63.6 | 92.6 |
| OSM arroyos (stream) | 175,602 | 79.3 | 97.4 |

Control: tramo de mayor área a ≤ 300 m de dos puentes RP2 (fase 1: Soberbio ~1.074 km², Acaraguá ~775 km²):

| nombre | arroyo_id | orden | area_km2 | nombre_tramo |
|---|---|---|---|---|
| Puente Arroyo Soberbio | 271,112 | 6 | 1,075.2 |  |
| Puente sobre el Arroyo Acaraguá | 316,204 | 6 | 775.0 | Arroyo Acaraguá |

| cruces_con_tramo | cruces |
|---|---|
| 40,237 | 41,671 |

## 3. Altura sobre el cauce (HAND)

| celdas_misiones | celdas_con_hand | pixeles_30m_por_celda | % celdas con algo < 2 m | % celdas con la mitad < 5 m |
|---|---|---|---|---|
| 320,120 | 320,026 | 110 | 65.0 | 16.6 |

| nota |
|---|
| red ≥ 0,1 km²; celdas negativas recortadas: 0 |

Por municipio (los 10 con mayor proporción de celdas con algo bajo 2 m):

| municipio | celdas | % con algo < 2 m | hand_mediana_tipica_m |
|---|---|---|---|
| Municipio de Azara | 2,003 | 75.5 | 5.9 |
| Municipio de Candelaria | 1,328 | 75.0 | 5.7 |
| Municipio de Mártires | 1,147 | 74.1 | 10.8 |
| Municipio de Itacaruaré | 2,340 | 74.0 | 7.9 |
| Municipio de Corpus Christi | 1,014 | 73.7 | 8.5 |
| Municipio de Loreto | 1,163 | 73.4 | 9.4 |
| Municipio de Garupá | 3,269 | 73.1 | 6.6 |
| Municipio de Posadas | 3,545 | 72.6 | 5.9 |
| Municipio de Concepción de la Sierra | 3,977 | 71.7 | 7.6 |
| Municipio de San Javier | 1,985 | 71.7 | 9.9 |

## 4. Lluvia IMERG V07

10,536 días entre 1998-01-01 y 2026-11-05 (esperados 10,536; faltan 0). Grilla 27×29 píxeles de 0,1°.

Contra las estaciones del Hexoma (INTA SIGA y GHCN) en su píxel, días con dato en ambas. Describe la fuente: no hay regla de corte. El día de las estaciones suele ir de 9 a 9 h y el de IMERG de 0 a 24 h (local), por eso también el desfase de un día y los acumulados.

| red | estación | días | r día | r día (IMERG −1) | r 3 días | r mes | IMERG / estación |
|---|---|---|---|---|---|---|---|
| GHCN-Daily (NOAA) | Foz do Iguaçu (Cataratas) | 5,582 | 0.4 | 0.5 | 0.7 | 0.9 | 1.1 |
| INTA SIGA | INTA - Cerro Azul (EMC) | 10,326 | 0.6 | 0.1 | 0.8 | 0.9 | 1.0 |
| GHCN-Daily (NOAA) | Posadas | 4,541 | 0.5 | 0.4 | 0.7 | 0.9 | 0.8 |
| INTA SIGA | B de Irigoyen - EEA Montecarlo | 4,125 | 0.6 | 0.1 | 0.8 | 0.9 | 1.1 |
| GHCN-Daily (NOAA) | Encarnación (PY) | 4,525 | 0.5 | 0.3 | 0.7 | 0.8 | 0.9 |
| GHCN-Daily (NOAA) | Iguazú Aero | 4,406 | 0.4 | 0.4 | 0.6 | 0.8 | 0.8 |
| GHCN-Daily (NOAA) | Capitán Meza (PY) | 3,335 | 0.5 | 0.4 | 0.7 | 0.8 | 0.8 |
| INTA SIGA | San Vicente - EEA Cerro Azul | 1,785 | 0.5 | 0.1 | 0.7 | 0.8 | 1.2 |
| INTA SIGA | Cerro Azul - EEA Cerro Azul | 4,768 | 0.5 | 0.1 | 0.8 | 0.8 | 1.0 |
| INTA SIGA | V Miguel Lanus -EEA Cerro Azul | 3,807 | 0.5 | 0.1 | 0.7 | 0.8 | 1.2 |
| GHCN-Daily (NOAA) | Ciudad del Este (PY) | 1,858 | 0.5 | 0.2 | 0.7 | 0.8 | 1.1 |
| INTA SIGA | Andresito - EEA Montecarlo | 3,213 | 0.6 | 0.1 | 0.7 | 0.8 | 1.3 |
| INTA SIGA | Montecarlo - EEA Montecarlo | 3,589 | 0.5 | 0.0 | 0.7 | 0.8 | 1.3 |

Mediana: r día 0.52, r 3 días 0.70, r mes 0.85; IMERG/estación 1.07.

Dato puntual: El Soberbio, 21-jul-2026 — IMERG 75,6 mm en el día (máx. 25 mm/h en media hora); el municipio informó 190 mm y CHIRPS 99 mm (demo El Soberbio). La lluvia satelital sigue subestimando los aguaceros locales.
