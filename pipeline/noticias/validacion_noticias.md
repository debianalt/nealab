# Validación del archivo de noticias (cortes de puentes y caminos, crecidas, evacuados)

Generado por `pipeline/noticias/validar_noticias.py` el 2026-11-08. Datos: 6 medios con API de WordPress abierta, El Territorio y La Voz de Cataratas por sitemaps y HTML, MisionesCuatro por copias de la Wayback Machine (su sitio desafía a todo cliente que no sea un navegador) y el archivo completo de Misiones Online por sus sitemaps; extracción por reglas (`extraer.py`).

## 1. Notas por medio

| medio | via | notas | desde | hasta |
|---|---|---|---|---|
| primeraedicion | api_wp | 17,130 | 2012-04-01 | 2026-11-07 |
| misionesonline | api_wp | 14,031 | 2000-04-05 | 2026-11-07 |
| economis | api_wp | 7,067 | 2017-01-13 | 2026-11-07 |
| canal12 | api_wp | 5,432 | 2017-09-08 | 2026-11-07 |
| misionesopina | api_wp | 4,610 | 2015-01-17 | 2026-11-07 |
| noticiasdel6 | api_wp | 3,148 | 2023-01-01 | 2026-11-07 |
| lavozdecataratas | html | 2,132 | 2021-02-15 | 2026-11-07 |
| gobmisiones | api_wp | 1,040 | 2018-10-13 | 2026-11-07 |
| elterritorio | html | 576 | 1970-01-01 | 2026-11-07 |
| misionescuatro | wayback | 431 | 2015-10-07 | 2026-10-17 |

Notas por año (todos los medios):

| anio | notas |
|---|---|
| 1970 | 5 |
| 2000 | 105 |
| 2001 | 112 |
| 2002 | 150 |
| 2003 | 196 |
| 2004 | 335 |
| 2005 | 308 |
| 2006 | 350 |
| 2007 | 433 |
| 2008 | 433 |
| 2009 | 514 |
| 2010 | 199 |
| 2011 | 456 |
| 2012 | 1,177 |
| 2013 | 1,540 |
| 2014 | 1,794 |
| 2015 | 1,857 |
| 2016 | 1,845 |
| 2017 | 2,811 |
| 2018 | 2,780 |
| 2019 | 2,892 |
| 2020 | 3,245 |
| 2021 | 3,504 |
| 2022 | 4,205 |
| 2023 | 6,010 |
| 2024 | 5,180 |
| 2025 | 5,928 |
| 2026 | 7,233 |

## 2. Cobertura sobre las notas del agente de la fase 3

De 30 notas distintas de `cortes_rp2_prensa.csv`, 30 son de los 6 medios con API; 23 están en el corpus.

| medio | notas_del_agente | en_corpus |
|---|---|---|
| canal12 | 2 | 2 |
| economis | 1 | 1 |
| elterritorio | 7 | 0 |
| misionesonline | 11 | 11 |
| noticiasdel6 | 2 | 2 |
| primeraedicion | 7 | 7 |

Notas del agente de esos medios que no están (para revisar consultas):

| fecha | medio | lugar_textual | url |
|---|---|---|---|
| 2014-06-28 | elterritorio | Arroyo Soberbio: [...] el Puente | https://www.elterritorio.com.ar/noticias/2014/06/28/394912-el-gobierno-de-la-provincia-presento-un-estado-de-situacion |
| 2022-10-20 | elterritorio | puente sobre el arroyo Yabotí, en la ruta Costera N°2 (acceso Parque Moconá) | https://www.elterritorio.com.ar/noticias/2022/10/20/766339-el-soberbio-por-crecida-del-arroyo-suspenden-en-transito-sobre-el-puente-yaboti |
| 2023-10-09 | elterritorio | puente del arroyo Ramón, Ruta Costera N° 02, Panambí (límite con Villa Bonita) | https://www.elterritorio.com.ar/noticias/2023/10/09/806635-la-crecida-del-rio-uruguay-interrumpio-el-transito-sobre-la-ruta-costera-n-2-a-la-altura-del-puente-del-arroyo-ramon |
| 2023-10-18 | elterritorio | puente del arroyo El Soberbio (corte total «el miércoles por la tarde») | https://www.elterritorio.com.ar/noticias/2023/10/20/807965-sorprendente-crecida-del-rio-uruguay-deja-en-alerta-a-municipios-costeros |
| 2023-11-18 | elterritorio | puente sobre el arroyo homónimo (El Soberbio) totalmente clausurado | https://www.elterritorio.com.ar/noticias/2023/11/18/811444-en-el-soberbio-no-cesa-la-alerta-ante-la-crecida-del-rio-uruguay-que-se-encuentra-con-una-altura-de-19_50-metros |
| 2024-05-05 | elterritorio | ruta provincial 2 sobre el puente del arroyo Ramón | https://www.elterritorio.com.ar/noticias/2024/05/05/825869-evacuados-el-uruguay-empieza-a-bajar-pero-sigue-alerta-por-lluvias |
| 2025-06-30 | elterritorio | puente sobre el arroyo Pindaytí en la Ruta Costera N° 2 | https://www.elterritorio.com.ar/noticias/2025/06/30/858624-relevamiento-de-la-crecida-del-rio-uruguay-monitoreo-constante-y-pasos-fronterizos-cerrados |

## 3. Menciones (patrones v3)

| tipo | motivo | menciones | notas | % con lugar |
|---|---|---|---|---|
| crecida | agua | 8,347 | 4,620 | 80.8 |
| corte_ruta_camino | otro | 3,280 | 2,530 | 77.2 |
| corte_ruta_camino | protesta | 2,720 | 1,853 | 81.7 |
| corte_ruta_camino | agua | 1,721 | 1,323 | 85.0 |
| evacuados_aislados | agua | 1,528 | 1,167 | 73.1 |
| corte_puente | agua | 1,010 | 748 | 91.3 |
| corte_puente | otro | 545 | 442 | 62.8 |
| corte_puente | protesta | 230 | 193 | 58.3 |
| corte_ruta_camino | obra | 190 | 177 | 70.0 |
| corte_ruta_camino | accidente | 122 | 114 | 74.6 |
| corte_puente | obra | 39 | 37 | 66.7 |
| corte_puente | accidente | 29 | 27 | 62.1 |

Precisión de la ubicación:

| precision_lugar | menciones |
|---|---|
| contexto_nota | 7,057 |
| sin lugar | 4,021 |
| asentamiento | 2,911 |
| cauce_localidad | 1,293 |
| hidrometro | 1,287 |
| cauce | 1,215 |
| ruta_localidad | 695 |
| cruce | 537 |
| cruce_inferido | 227 |
| barrio | 130 |
| paraje | 122 |
| manual | 87 |
| puente | 57 |
| municipio | 52 |
| km | 35 |
| escuela | 15 |
| nominatim | 12 |
| picada | 8 |

Fecha del evento:

| fecha_evento_fuente | menciones |
|---|---|
| nota | 16,324 |
| relativa | 2,894 |
| explicita | 499 |
| explicita_mes | 44 |

## 3b. Eventos (`eventos.py`)

| tipo | motivo | eventos | 2+ medios | con fuente oficial |
|---|---|---|---|---|
| crecida | agua | 2,613 | 665 | 75 |
| corte_ruta_camino | otro | 1,525 | 228 | 28 |
| corte_ruta_camino | protesta | 1,289 | 198 | 0 |
| corte_ruta_camino | agua | 801 | 169 | 29 |
| evacuados_aislados | agua | 699 | 112 | 14 |
| corte_puente | agua | 472 | 105 | 21 |
| corte_puente | otro | 213 | 33 | 5 |
| corte_puente | protesta | 104 | 9 | 0 |
| corte_ruta_camino | obra | 102 | 17 | 3 |
| corte_ruta_camino | accidente | 78 | 6 | 0 |
| corte_puente | obra | 22 | 1 | 0 |
| corte_puente | accidente | 13 | 2 | 0 |

## 4a. Primera medición (patrones v1, semilla 7)

Precisión del tipo 52% (NO PASA la regla de 80%); lugar 83%. Errores típicos: «aislados» en fútbol/virus, notas de Energía de Misiones sin corte, cortes de ruta de protesta, «habilitado» de obras, desbordes en el fútbol, frases hipotéticas. Se ajustaron los patrones (v2, comentario en extraer.py) y se midió con una muestra NUEVA (semilla 8), no con la misma.

| tipo | n | tipo_ok |
|---|---|---|
| corte_luz | 22 | 0.4 |
| corte_puente | 1 | 1.0 |
| corte_ruta_camino | 12 | 0.7 |
| crecida | 18 | 0.8 |
| evacuados_aislados | 7 | 0.0 |

## 4b. Precisión de las reglas vigentes (patrones v2, muestra al azar NUEVA, semilla 8)

Regla fijada antes de mirar: precisión del tipo ≥ 80%. Anotadas 60 de 60.

**Tipo: 87% (PASA).** Lugar (menciones con lugar, 34): 85%.

Por tipo:

| tipo | n | tipo_ok |
|---|---|---|
| corte_luz | 9 | 0.8 |
| corte_puente | 3 | 1.0 |
| corte_ruta_camino | 11 | 0.7 |
| crecida | 32 | 0.9 |
| evacuados_aislados | 5 | 1.0 |

Errores (para ajustar patrones):

| mencion_id | tipo | lugar_nombre | tipo_ok | lugar_ok | nota |
|---|---|---|---|---|---|
| 3,548 | crecida |  | 0 |  | accidente en zona anegada, no crecida |
| 4,787 | corte_ruta_camino |  | 0 |  | corte hecho por manifestantes |
| 5,442 | crecida | Grande | 1 | 0.0 | «Grande» de Rio Grande do Sul tomado como arroyo de Misiones |
| 8,329 | crecida | Corrientes | 1 | 0.0 | hidrómetro Corrientes por la provincia de Corrientes |
| 16,752 | corte_luz |  | 0 |  | escuela sin luz crónica, no corte |
| 17,502 | corte_ruta_camino | Posadas | 0 | 1.0 | paro de transporte |
| 17,619 | corte_ruta_camino | Luján | 1 | 0.0 | paraje Luján en vez de Luján de Cuyo |
| 18,313 | crecida | Paraná | 1 | 0.0 | río Paraná en vez del arroyo Piray Guazú |
| 18,683 | crecida |  | 0 |  | impactos «previstos» (pronóstico) |
| 19,297 | corte_luz |  | 0 |  | cuantificar perjuicios (no evento) |
| 19,996 | crecida | Corrientes | 1 | 0.0 | hidrómetro Corrientes por la provincia de Corrientes |
| 25,476 | crecida |  | 0 |  | obra para evitar desbordes (no evento) |
| 25,691 | corte_ruta_camino | Gobernador Roca | 0 | 1.0 | línea de 13,2 kV cortada: es corte de luz, no de camino |

## 4c. Precisión en los medios nuevos (El Territorio, La Voz, MisionesCuatro; semilla 9)

Regla fijada antes de mirar: tipo ≥ 80% (el HTML puede colar menús y títulos relacionados). Anotadas 40.

**Tipo: 80% (PASA).** Lugar (13 con lugar): 77%.

| mencion_id | medio | tipo | lugar_nombre | tipo_ok | lugar_ok | nota |
|---|---|---|---|---|---|---|
| 15,261 | elterritorio | crecida | El Soberbio | 0 | 0.0 | alerta en Santa Catarina, hipotética |
| 30,448 | elterritorio | crecida | Corrientes | 1 | 0.0 | Brasil, no Corrientes |
| 30,498 | elterritorio | crecida |  | 0 |  | proyecto de obra |
| 30,521 | elterritorio | evacuados_aislados | Santo Tomé | 0 | 1.0 | «evacuó» = caudal de la represa |
| 30,971 | lavozdecataratas | crecida |  | 0 |  | genérico |
| 31,179 | lavozdecataratas | crecida |  | 0 |  | nivel del mar |
| 31,232 | lavozdecataratas | corte_ruta_camino | Corpus | 1 | 0.0 | el corte es en Santa Ana |
| 31,335 | lavozdecataratas | crecida | Tacuara | 0 | 1.0 | charla técnica, no evento |
| 31,388 | lavozdecataratas | crecida |  | 0 |  | declaración genérica |
| 31,729 | misionescuatro | corte_ruta_camino |  | 0 |  | texto vacío («corte de ruta» suelto) |

## 5. Precisión de la v3 (muestra al azar de 60, semilla 15)

Reglas fijadas antes de mirar. lugar_ok: el punto corresponde al lugar del hecho a la escala de su precisión (un contexto de nota cuenta como acierto si el hecho ocurrió en ese municipio).

| medida | n | valor | regla | resultado |
|---|---|---|---|---|
| tipo_ok | 60 | 72% | ≥ 85% | NO PASA |
| motivo_ok | 37 | 89% | ≥ 80% | PASA |
| lugar_ok | 38 | 79% | ≥ 85% | NO PASA |
| fuera_misiones | 60 | 3.3% | ≤ 5 % | PASA |
| fecha_ok (sin regla) | 43 | 98% | — | — |

Errores:

| mencion_id | tipo | motivo | precision_lugar | lugar_nombre | nota |
|---|---|---|---|---|---|
| 34 | evacuados_aislados | agua | asentamiento | San Antonio | pronóstico («chaparrones muy aislados») |
| 713 | crecida | agua | asentamiento | Tabay | arroyo Tabay × RP 7, no la localidad Tabay |
| 2,627 | corte_ruta_camino | otro |  |  | vereda restringida por un edificio |
| 2,722 | corte_puente | agua | cauce | Cuñá | obra («trabajos previstos»); «Cuñá» en vez de Cuñá Pirú II |
| 3,363 | corte_ruta_camino | agua | contexto_nota | 1º de Mayo | Puerto Iguazú, no 1º de Mayo |
| 3,613 | corte_ruta_camino | otro | contexto_nota | José Luis | opinión sobre los cortes |
| 6,659 | crecida | agua | cauce_localidad | Iguazú | texto sin hecho |
| 7,138 | corte_ruta_camino | accidente | cruce | Tunas × RP 2 | desborde del arroyo Las Tunas: motivo agua («evitar accidentes» manda) |
| 8,028 | evacuados_aislados | agua | contexto_nota | San José | Garupá/Posadas (Fátima, Crucero del Norte), no San José |
| 8,441 | crecida | agua | contexto_nota | Santa María | pronóstico («va a superar») |
| 9,538 | evacuados_aislados | agua | contexto_nota | Bella Vista | Barranqueras (Chaco) |
| 9,648 | crecida | agua | contexto_nota | Santa María | genérico (El Niño) |
| 9,684 | crecida | agua | hidrometro | Panambí | «futura inundación» |
| 11,512 | crecida | agua | contexto_nota | San Miguel | familias obereñas: Oberá, no San Miguel |
| 11,777 | crecida | agua | cauce | Iguazú | genérico («las crecidas pueden elevarlo») |
| 13,056 | crecida | agua | contexto_nota | San José | negación («no nos vimos tan complicados por las inundaciones») |
| 13,710 | corte_ruta_camino | otro | asentamiento | San Vicente | menú «Tal vez te interese leer» |
| 13,909 | corte_ruta_camino | otro | contexto_nota | Garupá | opinión sobre los cortes |
| 14,270 | corte_ruta_camino | otro | asentamiento | Garupá | corte político: motivo protesta |
| 15,344 | corte_ruta_camino | protesta | contexto_nota | Candelaria | marcha sin corte |
| 15,621 | corte_ruta_camino | protesta | contexto_nota | Caraguatay | cortes en varios municipios |
| 15,948 | corte_ruta_camino | protesta | contexto_nota | San Vicente | tema, no hecho |
| 16,214 | corte_ruta_camino | otro | contexto_nota | Colonia Victoria | corte de protesta: motivo protesta |
| 16,415 | corte_ruta_camino | protesta |  |  | opinión sobre los cortes |
| 16,791 | corte_ruta_camino | otro | contexto_nota | 9 de Julio | política, no hecho |
| 17,837 | crecida | agua | cauce_localidad | Uruguay | condicional («si no están visibles») |
| 18,492 | crecida | agua |  |  | Sumatra |
| 18,797 | evacuados_aislados | agua |  |  | condicional («si el río llega a 14 m») |
| 19,174 | evacuados_aislados | agua | contexto_nota | Leandro N. Alem | evacuados de toda la provincia |
| 20,834 | crecida | agua | contexto_nota | El Salto | Cataratas del Iguazú, no El Salto |

## 6. Ubicaciones finas (40 al azar, semilla 15)

Regla: punto a ≤ 2 km del lugar real en ≥ 85 %. **64% (NO PASA).**

| precision_lugar | n | ok |
|---|---|---|
| barrio | 5 | 0.8 |
| cruce | 16 | 0.9 |
| cruce_inferido | 11 | 0.5 |
| nominatim | 1 | 1.0 |
| paraje | 4 | 0.0 |
| puente | 2 | 0.0 |

Errores:

| mencion_id | precision_lugar | lugar_nombre | nota |
|---|---|---|---|
| 260 | cruce_inferido | Piray Miní × camino | cruce elegido sin puente OSM; el puente más cercano está a 2,7 km |
| 4,407 | cruce | del Medio × RP 210 | el hecho es el arroyo Lata en Oberá; el punto queda a 20 km |
| 4,567 | paraje | Santo Domingo Savio | paraje Santo Domingo Savio; el puente del Yabebiry está a 5 km |
| 6,531 | paraje | Paraje San Roque | San Roque y Santa Lucía de Corrientes |
| 7,698 | puente | Rivadavia | «Rivadavia» es una calle, no el puente |
| 7,832 | paraje | Paraje Tambor de Tacuarí | «en este camino» (figurado) |
| 8,158 | cruce_inferido | Acaraguá × camino | Acaraguá sin referencia: cruce sin puente, a 7 km del puente OSM más cercano |
| 8,207 | barrio | Retiro | «bloqueos» de fumigación (dengue) |
| 9,024 | cruce_inferido | Yabebiry × Ruta Nacional 12 | puente Mártires–Santo Domingo Savio, no el de la RN 12 |
| 9,373 | paraje | Paraje Delicia | Delicia sobre la RN 12 (Puerto/Colonia Delicia), no Paraje Delicia a 10 km |
| 9,518 | cruce_inferido | Soberbio × Picada Payeska | Arroyo El Soberbio × RP 2 («la misma ruta» = Costera 2), no Picada Payeska a 40 km |
| 10,715 | cruce_inferido | Ramón × camino | «Ruta Costera N° 02» no leída como RP 2: punto a 12 km, lejos del río |
| 14,321 | puente | Rivadavia | «Rivadavia» es una calle, no el puente |
| 19,684 | cruce_inferido | Acaraguá × camino | igual que 8158 |

## 7. Precisión por municipio-día (la unidad del contraste; 60 celdas al azar de la matriz por agua)

Regla fijada antes de mirar: ≥ 85% de las celdas con al menos un hecho real de agua en ese municipio ese día (±1). Con Haiku: `celdas_llm.py <semilla>` (v2 se escribió mirando la semilla 16; la 17 es la prueba fuera de muestra): 16 → 48 % reglas, 81 % Haiku v2 (pierde 3 de 29 buenas); 17 → 42 % reglas, 68 % Haiku v2 (pierde 4 de 25). Lo que queda son municipios mal asignados y fechas de eventos que se estiran: no son del tipo.

### Semilla 16: reglas solas 48% (NO PASA)

Errores:

| municipio | dia | tipos | nota |
|---|---|---|---|
| Puerto Iguazú | 2014-06-24 | crecida | pasado: la crecida fue semanas antes |
| Campo Ramón | 2015-01-06 | crecida | crónico, sin hecho |
| Alba Posse | 2015-03-24 | evacuados_aislados | pasado (2014) |
| Panambí | 2015-09-29 | crecida | pasado |
| El Soberbio | 2016-02-01 | crecida | crónico |
| San Antonio | 2016-10-04 | crecida | Cuba |
| Veinticinco de Mayo | 2017-07-05 | corte_puente,corte_ruta_camino | obra |
| San Vicente | 2017-10-12 | corte_ruta_camino | protesta |
| Apóstoles | 2017-12-13 | corte_ruta_camino | protesta |
| Santa María | 2018-02-06 | crecida,evacuados_aislados | Salta (Santa Victoria Este) |
| San Vicente | 2018-03-09 | crecida | pasado |
| El Soberbio | 2018-10-18 | crecida | pasado |
| Santa María | 2020-04-16 | crecida | obra |
| Garupá | 2020-09-07 | corte_puente | motivo no se lee: corte sin causa en la oración |
| San Vicente | 2020-10-23 | crecida | obra / crónico |
| Leandro N. Alem | 2020-10-27 | crecida | crónico |
| Bonpland | 2022-06-22 | corte_ruta_camino | corte de energía, no agua ni vía |
| Comandante Andresito | 2023-01-19 | corte_ruta_camino | Brasil (BR-376) |
| Fachinal | 2023-03-14 | evacuados_aislados | barrio San Onofre de Posadas, no Fachinal |
| Santa María | 2023-09-02 | crecida | resumen provincial |
| Comandante Andresito | 2023-09-11 | crecida | Libia |
| Santa María | 2023-10-19 | crecida,evacuados_aislados | Alba Posse y El Soberbio, no Santa María |
| San Martín | 2024-05-05 | crecida | Rio Grande do Sul |
| Garupá | 2024-11-18 | evacuados_aislados | tarifas |
| San Pedro | 2025-03-12 | evacuados_aislados | Bahía Blanca |
| Posadas | 2025-09-09 | crecida | Tacuaruzú (Profundidad), no Posadas |
| Caraguatay | 2025-11-11 | crecida | genérico (cauce del Paraná) |
| Santo Pipó | 2025-12-29 | crecida,evacuados_aislados | arroyo Tacuara de Puerto Iguazú, no Santo Pipó |
| San Pedro | 2026-07-03 | corte_ruta_camino,evacuados_aislados | Rio Grande do Sul |
| San Pedro | 2026-08-14 | corte_puente,corte_ruta_camino | acceso a Moconá (El Soberbio), no San Pedro |
| Santa María | 2026-10-15 | corte_puente,corte_ruta_camino,crecida | resumen provincial |

### Semilla 17: reglas solas 42% (NO PASA)

Errores:

| municipio | dia | tipos | nota |
|---|---|---|---|
| Bernardo de Irigoyen | 2013-03-24 | corte_ruta_camino | historia (Rosas) |
| Puerto Iguazú | 2013-07-31 | crecida | pasado / aclaración |
| El Soberbio | 2014-06-11 | corte_puente,corte_ruta_camino | puentes de la RN 12 por el Paraná, no El Soberbio |
| Azara | 2014-07-16 | crecida | relevamiento posterior, provincial |
| Colonia Aurora | 2014-08-06 | crecida | pasado |
| Alba Posse | 2015-02-03 | crecida | entrega de casas |
| Apóstoles | 2016-07-04 | crecida | cita bíblica |
| San Pedro | 2016-09-04 | evacuados_aislados | crónico / propuesta |
| Los Helechos | 2017-03-17 | evacuados_aislados | crónico (falta de puente) |
| Garuhapé | 2017-06-05 | crecida,evacuados_aislados | Oberá, no Garuhapé |
| Capioví | 2018-10-11 | crecida | pasado |
| Capioví | 2018-10-12 | crecida | pasado |
| San Vicente | 2019-03-14 | corte_ruta_camino | protesta en San José |
| Guaraní | 2019-03-25 | evacuados_aislados | pronóstico (chaparrones aislados) |
| Montecarlo | 2021-11-05 | evacuados_aislados | lluvias aisladas |
| San Vicente | 2022-01-20 | evacuados_aislados | focos de incendio aislados |
| Campo Viera | 2022-03-16 | corte_ruta_camino | protesta |
| San Pedro | 2022-05-05 | crecida,evacuados_aislados | alerta provincial; San Pedro no está sobre el Uruguay |
| Puerto Iguazú | 2022-06-09 | crecida | el cierre fue una semana antes |
| Puerto Iguazú | 2022-10-07 | crecida | el cierre fue el 11–12 de octubre |
| San Pedro | 2023-07-11 | crecida | acceso a Moconá (El Soberbio), no San Pedro |
| Puerto Iguazú | 2023-07-20 | crecida | la crecida fue la semana anterior |
| Puerto Iguazú | 2023-11-20 | crecida | el cierre ya había terminado |
| Comandante Andresito | 2023-12-26 | corte_ruta_camino,crecida,evacuados_aislados | resumen del año |
| San Vicente | 2024-01-11 | crecida,evacuados_aislados | Corrientes (Mercedes) |
| Garupá | 2024-01-17 | crecida | resumen provincial |
| Cerro Azul | 2024-05-02 | crecida | arroyo Mártires (RP 4/103), no Cerro Azul |
| Comandante Andresito | 2024-10-29 | evacuados_aislados | incomunicado por corte de luz |
| Pozo Azul | 2025-06-23 | corte_ruta_camino | árbol en la RP 17 en Santiago de Liniers, no Pozo Azul |
| Montecarlo | 2026-01-14 | evacuados_aislados | pronóstico |
| Leandro N. Alem | 2026-06-30 | crecida | genérico |
| Eldorado | 2026-08-28 | crecida | preventivo |
| Santa María | 2026-08-28 | crecida | riesgo, genérico |
| Oberá | 2026-09-17 | crecida | crónico |
| Apóstoles | 2026-10-24 | corte_puente,corte_ruta_camino | puentes del Cuña Pirú, no Apóstoles |
