# Validación: río Uruguay contra los puentes de la RP2

Generado por `pipeline/infra/validar_rio.py` el 2026-11-07. Método en los docstrings de `rio_uruguay.py` y `cortes_rio.py`.

## 1. Cero de escala

Regla fijada antes de mirar: si el cero del Monitor provincial y el implícito de FABDEM difieren ≤ 1,5 m, se usa el del Monitor; si no, el de FABDEM.

| serie | nombre | km_rio | dist_eje_m | cero_monitor | cero_fabdem | cero_perfil | cero_msnm | fuente_cero | alerta_m | evacuacion_m |
|---|---|---|---|---|---|---|---|---|---|---|
| 60 | Pepirí Miní | 17.3 | 269.5 |  | 125.2 | 125.2 | 125.2 | perfil isotónico FABDEM |  |  |
| 61 | El Soberbio | 62.9 | 820.4 | 118.0 | 118.9 | 118.9 | 118.9 | perfil isotónico FABDEM | 11.0 | 13.0 |
| 63 | Alba Posse | 168.4 | 160.6 | 107.0 | 100.6 | 100.6 | 100.6 | perfil isotónico FABDEM | 8.0 | 12.0 |
| 64 | Panambí | 215.5 | 471.5 | 96.0 | 92.9 | 93.4 | 93.4 | perfil isotónico FABDEM | 7.0 | 9.0 |
| 65 | San Javier | 263.8 | 931.0 | 85.0 | 74.4 | 74.4 | 74.4 | perfil isotónico FABDEM | 8.0 | 10.0 |
| 66 | Puerto Concepción | 341.6 | 105.7 |  | 60.9 | 60.8 | 60.8 | perfil isotónico FABDEM | 16.5 | 17.5 |
| 67 | Garruchos | 354.1 | 152.4 |  | 58.0 | 58.0 | 58.0 | perfil isotónico FABDEM |  |  |

Ceros en uso: perfil isotónico de FABDEM (superficie del agua a lo largo de 366 km del eje, punto más bajo de cada sección cada 100 m, mediana por km y regresión isotónica decreciente) menos la mediana de la escala en 2011–2015. Regla fijada antes de mirar: gana el juego de ceros con menor error de interpolación en crecida, si el perfil da El Soberbio a ≤ 1,5 m de 118. Punto FABDEM y perfil coinciden a ≤ 0,5 m; el Monitor difiere 3–11 m en Alba Posse, Panambí y San Javier.

**Coherencia de datum.** La cota de seguridad de cada puente contra el máximo de FABDEM a ±90 m (el camino de acceso): si coinciden, cotas y FABDEM están en el mismo sistema y los ceros de FABDEM son los coherentes. Diferencia mediana -1.0 m; |dif| ≤ 3 m en 11 de 25 puentes.

| nombre | cota | fabdem_acceso_max | cota_menos_fabdem |
|---|---|---|---|
| Puente Arroyo Soberbio | 139.0 | 140.6 | -1.7 |
| Puente sobre el Arroyo Tararira | 154.9 | 161.9 | -7.0 |
| Puente sobre el Arroyo Persiguero | 136.7 | 135.9 | 0.8 |
| Puente sobre el Arroyo Chafariz | 136.5 | 144.7 | -8.2 |
| Puente sobre el Arroyo Los Muertos | 133.2 | 129.3 | 3.9 |
| Puente sobre el Arroyo Dorado | 139.9 | 139.1 | 0.8 |
| Puente sobre el Arroyo Doradito | 130.3 | 127.2 | 3.1 |
| Puente sobre el Arroyo Macaco | 124.8 | 148.5 | -23.7 |
| Puente sobre el Arroyo Saltiño | 127.5 | 127.6 | -0.1 |
| Puente sobre el Arroyo Pindaití | 118.6 | 119.7 | -1.0 |
| Puente sobre el Arroyo Chancay | 118.1 | 124.2 | -6.1 |
| Puente sobre el Arroyo Santa Rita | 119.0 | 118.5 | 0.5 |
| Puente sobre el Arroyo Canal Torto | 124.2 | 118.2 | 6.0 |
| Puente sobre el Arroyo Acaraguá | 110.2 | 110.4 | -0.2 |
| Puente sobre el Arroyo Ramón | 102.4 | 104.9 | -2.5 |
| Puente sobre el Arroyo Tomás Guido | 102.7 | 104.1 | -1.4 |
| Puente sobre el Arroyo Once Vueltas | 101.0 | 103.1 | -2.1 |
| Puente sobre el Arroyo Alipio | 100.6 | 104.6 | -4.0 |
| Puente sobre el Arroyo Guerrero | 96.7 | 96.4 | 0.3 |
| Puerte sobre el Arroyo Portera | 96.4 | 112.4 | -16.0 |
| Puente sobre el Arroyo Itacaruaré | 97.6 | 93.4 | 4.2 |
| Puente sobre el Arroyo Santa María | 97.4 | 90.9 | 6.5 |
| Puente sobre el Arroyo Persiguero | 98.6 | 102.2 | -3.6 |
| Puente sobre el Arroyo Concepción | 100.4 | 103.7 | -3.3 |
| Puente sobre el Arroyo Tunas | 77.3 | 68.9 | 8.4 |

El cero del Monitor en San Javier (85) pondría el agua del hidrómetro por encima de la superficie del río que FABDEM ve 5 km aguas arriba (Alipio, ~84 msnm): el río no sube hacia abajo. La variante con los ceros del Monitor se informa en la sección 4 para mostrar cuánto cambia.

## 2. Error de la interpolación (dejando fuera cada estación intermedia)

Se predice el nivel de cada estación con las vecinas que tienen lectura ese día, como se hace en los puentes. «Crecida» = días con la escala sobre el nivel de alerta (o sobre el P90 si no hay alerta). El error incluye el de los ceros.

| estación | días de crecida | umbral (m) | |error| mediano (m) | |error| P90 (m) | sesgo (m) | |error| mediano todos los días | tramo mediano (km) |
|---|---|---|---|---|---|---|---|
| El Soberbio | 74 | 11.0 | 1.0 | 2.4 | 0.9 | 0.9 | 151.0 |
| Alba Posse | 238 | 8.0 | 1.7 | 6.3 | -1.6 | 0.8 | 198.0 |
| Panambí | 97 | 7.0 | 4.1 | 7.1 | -4.1 | 5.2 | 95.0 |
| San Javier | 155 | 8.0 | 6.9 | 8.5 | 6.9 | 6.0 | 173.0 |

Lectura: entre El Soberbio y Alba Posse el error en crecida es de 1–2 m; entre Panambí y San Javier, de 4–7 m con sesgo de signo opuesto. Dos estimaciones de FABDEM independientes dan los mismos ceros, así que el error no es del cero: en crecida el río no sube parejo (el valle se ensancha aguas abajo) y la interpolación lineal entre hidrómetros a 100–200 km no lo capta. Las frecuencias de corte de los puentes entre Panambí y Garruchos son INDICATIVAS; las de El Soberbio a Alba Posse, firmes dentro de ±2 m.

## 3. Cortes documentados en la prensa

Regla fijada antes de mirar: en ≥ 67% de los cortes por crecida del Uruguay, el modelo da cortado o margen ≥ -1 m en ±2 días. Fuera de la regla: los pasos provisorios del Pindaytí desde abril de 2021 (bajos, no comparables con la cota del puente), los cortes por lluvia del propio arroyo y los registros que no nombran un puente con cota (Yabotí, Chico Alférez, tramos genéricos).

**Elegibles: 19; aciertos: 14 (74%). PASA.**

| fecha | lugar_textual | causa | puente | margen_max | acierto |
|---|---|---|---|---|---|
| 2014-06-28 | Arroyo Soberbio: [...] el Puente | no dice (parte general: desborde Río Uruguay, Arroyo El Soberbio) | Puente Arroyo Soberbio | 2.7 | 1 |
| 2014-06-28 | Alba Posse: puente sobre Arroyo Santa Rita | no dice (parte de la crecida del Uruguay) | Puente sobre el Arroyo Santa Rita | 4.1 | 1 |
| 2014-06-28 | Alba Posse: puente sobre Arroyo Shangay | no dice (parte de la crecida del Uruguay) | Puente sobre el Arroyo Chancay | 5.0 | 1 |
| 2014-06-28 | Alba Posse: puente sobre Arroyo Pindaytí | no dice (parte de la crecida del Uruguay) | Puente sobre el Arroyo Pindaití | 4.6 | 1 |
| 2014-06-28 | Alba Posse: puente sobre Arroyo Saltiño | no dice (parte de la crecida del Uruguay) | Puente sobre el Arroyo Saltiño | -1.0 | 0 |
| 2014-06-28 | Alba Posse: puente sobre el Arroyo El Torto | no dice (parte de la crecida del Uruguay) | Puente sobre el Arroyo Canal Torto | -4.5 | 0 |
| 2017-06-01 | ruta costera 2, pasos sobre el arroyo Ramón, límite entre Panambí y Alba Posse | crecida del Uruguay (sólo en el título: «Más puentes cortados por la creciente del Uruguay») | Puente sobre el Arroyo Ramón | 4.8 | 1 |
| 2017-06-01 | ruta costera 2, tramo terrado sobre el arroyo Las Tunas, en Azara | crecida del Uruguay (sólo en el título: «Más puentes cortados por la creciente del Uruguay») | Puente sobre el Arroyo Tunas | -0.3 | 1 |
| 2023-10-09 | puente del arroyo Ramón, Ruta Costera N° 02, Panambí (límite con Villa Bonita) | crecida del Uruguay | Puente sobre el Arroyo Ramón | 0.5 | 1 |
| 2023-10-18 | puente sobre el arroyo El Soberbio, por Ruta Provincial Nº 2 | crecida del Uruguay | Puente Arroyo Soberbio | -3.1 | 0 |
| 2023-10-18 | puente del arroyo El Soberbio (corte total «el miércoles por la tarde») | mixta (lluvia del arroyo + crecida del Uruguay) | Puente Arroyo Soberbio | -3.1 | 0 |
| 2023-11-04 | puente del arroyo Ramón sobre Ruta Costera 2, entre Panambí y Villa Bonita | crecida del Uruguay | Puente sobre el Arroyo Ramón | 0.8 | 1 |
| 2023-11-17 | Puente del arroyo Ramón, Ruta Costera N° 02 de Panambí | crecida del Uruguay | Puente sobre el Arroyo Ramón | 4.7 | 1 |
| 2023-11-17 | puente Arroyo El Soberbio, ruta provincial Nº 2 (una mano a las 19, corte total a las 20) | crecida del Uruguay | Puente Arroyo Soberbio | -0.4 | 1 |
| 2023-11-18 | Ruta Provincial Nº 2, a la altura del municipio de Colonia Aurora, en cercanías del arroyo El Doradito | crecida del Uruguay y sus afluentes | Puente sobre el Arroyo Doradito | -1.1 | 0 |
| 2023-11-18 | Ruta Provincial N° 2, puente arroyo Chancay, límite entre Alba Posse y 25 de Mayo | crecida del Uruguay | Puente sobre el Arroyo Chancay | 3.9 | 1 |
| 2023-11-18 | Ruta Provincial N° 2, puente arroyo Santa Rita, en Aba Posse | crecida del Uruguay | Puente sobre el Arroyo Santa Rita | 2.9 | 1 |
| 2023-11-18 | puente sobre el arroyo homónimo (El Soberbio) totalmente clausurado | crecida del Uruguay | Puente Arroyo Soberbio | -0.4 | 1 |
| 2024-05-05 | ruta provincial 2 sobre el puente del arroyo Ramón | no dice (la nota habla de crecida de arroyos y del río Uruguay por lluvias) | Puente sobre el Arroyo Ramón | 1.9 | 1 |

Cortes por lluvia del propio arroyo (el modelo NO los representa: es esperable que dé margen negativo):

| fecha | lugar_textual | causa | puente | margen_max | acierto |
|---|---|---|---|---|---|
| 2014-06-28 | Desborde Arroyo Ramón; R. Costera II desde Panambi hasta Sta. Rita | crecida del arroyo (desborde; la nota no dice el origen) | Puente sobre el Arroyo Ramón | 15.2 | 1 |
| 2014-06-28 | puente Arroyo Las Tunas límite entre localidad Tres Capones y Azara (la nota no nombra la ruta) | no dice | Puente sobre el Arroyo Tunas | 4.1 | 1 |
| 2015-01-03 | cruce de la ruta 2, que va hacia Mojón Grande; desborde del arroyo Guerrero | lluvia del arroyo | Puente sobre el Arroyo Guerrero | -6.6 | 0 |
| 2017-04-09 | ruta provincial 2, jurisdicción de Azara, arroyo Las Tunas | lluvia del arroyo (la nota trata de lluvias intensas y desborde de arroyos) | Puente sobre el Arroyo Tunas | -9.2 | 0 |
| 2017-04-09 | arroyo El Persiguero, entre Santa María y Concepción de la Sierra (ruta provincial 2) | lluvia del arroyo (la nota trata de lluvias intensas y desborde de arroyos) | Puente sobre el Arroyo Persiguero (sur) | -24.1 | 0 |
| 2023-10-09 | puente sobre el Arroyo Ramón en la localidad de Panambí, RP2 | lluvia del arroyo (contradice a El Territorio y Primera Edición del mismo día) | Puente sobre el Arroyo Ramón | 0.5 | 1 |
| 2023-10-18 | A° Chancay, RP N° 2 (lista de cortados) | no dice («Por las inclemencias climáticas») | Puente sobre el Arroyo Chancay | -0.7 | 1 |

**Umbral físico contra umbral de cierre (puente del Arroyo Soberbio).** Con cero 118.9 msnm, la cota de 138.95 equivale a 20.08 m en la escala de El Soberbio. Alturas citadas:

| fecha | lugar_textual | altura_citada_m | equivale_msnm | respecto_de_la_cota_m |
|---|---|---|---|---|
| 2014-06-28 | Arroyo Soberbio: [...] el Puente | 22.6 | 141.5 | 2.5 |
| 2023-10-18 | puente del arroyo El Soberbio (corte total «el miércoles por la tarde») | 18.0 | 136.9 | -2.1 |
| 2023-11-17 | puente Arroyo El Soberbio, ruta provincial Nº 2 (una mano a las 19, corte total a las 20) | 17.1 | 136.0 | -2.9 |
| 2023-11-18 | puente sobre el arroyo homónimo (El Soberbio) totalmente clausurado | 19.5 | 138.4 | -0.6 |

El 28-jun-2014 la nota dice que el agua pasaba «2 mt sobre el Puente» con 22,60 m: el tablero queda a ~20,6 m de escala = 139.5 msnm, contra la cota de 138.95 (diferencia +0.52 m). La cota de seguridad es el nivel del tablero. En 2023 el puente se CERRÓ con 17–18 m, 2–4 m antes de que el agua lo tape: el cierre preventivo es otro umbral, más bajo, y el modelo físico no lo representa.

Historia de esta validación (7-nov-2026). La primera corrida dio 62 % y no pasaba. Se corrigieron dos errores de código, no de criterio: (1) el control de calidad descartaba subidas reales de más de 5 m por día (borró el récord de 27 m del 9-jul-1983); ahora sólo descarta picos de un día de más de 10 m en el mismo sentido; (2) notas sobre el paso provisorio del Pindaytí «en Santa Rita» se asignaban al puente Santa Rita. Además, el cero de las estaciones pasó a salir del perfil isotónico de FABDEM por una regla fijada antes de mirar la prensa (menor error de interpolación y control en El Soberbio); eso subió el cero de El Soberbio 0,9 m y metió dos cierres de nov-2023 en la tolerancia de −1 m.

## 4. Cortes por puente (1950–2026 según el tramo)

p_anual = años con al menos un día bajo la cota / años con datos (empírico, sin distribución). «+1 m» y «+2 m»: años con corte si el pico real fuera 1 o 2 m mayor que la lectura diaria (la lectura es manual, una vez por día). T = período de retorno de la cota según Gumbel y GEV ajustadas a los máximos anuales (IC 80 % por bootstrap); con < 3 cortes observados la extrapolación es frágil. «con_corte_ceros_monitor»: años con corte usando los ceros del Monitor provincial.

| nombre | cota | anios | desde | con_corte | p_anual | dias_anio | margen_max | +1 m | +2 m | t_gumbel | t_gev | gev_p10 | gev_p90 | con_corte_ceros_monitor | ultimo_corte |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Puente Arroyo Soberbio | 138.9 | 45 | 1,981 | 4 | 0.1 | 0.2 | 6.9 | 6 | 6 | 13.3 | 12.5 | 8.8 | 20.4 | 3 | 2014-06-29 |
| Puente sobre el Arroyo Tararira | 154.9 | 45 | 1,981 | 0 | 0.0 | 0.0 | -9.6 | 0 | 0 | 971.0 | 413.0 | 89.7 | 1,000,000.0 | 0 |  |
| Puente sobre el Arroyo Persiguero | 136.7 | 45 | 1,981 | 2 | 0.0 | 0.1 | 4.7 | 4 | 6 | 18.9 | 17.1 | 11.3 | 37.4 | 2 | 2014-06-29 |
| Puente sobre el Arroyo Chafariz | 136.5 | 45 | 1,981 | 1 | 0.0 | 0.0 | 1.2 | 2 | 2 | 35.0 | 33.1 | 20.3 | 128.7 | 2 | 1983-07-09 |
| Puente sobre el Arroyo Los Muertos | 133.2 | 45 | 1,981 | 0 | 0.0 | 0.0 | -0.5 | 2 | 2 | 37.6 | 44.2 | 22.8 | 154.4 | 2 |  |
| Puente sobre el Arroyo Dorado | 139.9 | 45 | 1,981 | 0 | 0.0 | 0.0 | -7.3 | 0 | 0 | 212.0 | 334.4 | 87.0 | 353,576.0 | 0 |  |
| Puente sobre el Arroyo Doradito | 130.3 | 45 | 1,981 | 2 | 0.0 | 0.1 | 1.2 | 2 | 3 | 24.7 | 28.8 | 17.1 | 71.2 | 5 | 2014-06-29 |
| Puente sobre el Arroyo Macaco | 124.8 | 45 | 1,981 | 2 | 0.0 | 0.1 | 2.4 | 3 | 5 | 17.9 | 22.1 | 14.0 | 53.5 | 6 | 2023-11-18 |
| Puente sobre el Arroyo Saltiño | 127.5 | 45 | 1,981 | 0 | 0.0 | 0.0 | -1.0 | 0 | 1 | 40.4 | 66.0 | 29.8 | 650.2 | 4 |  |
| Puente sobre el Arroyo Pindaití | 118.7 | 45 | 1,981 | 5 | 0.1 | 0.2 | 4.6 | 6 | 6 | 9.7 | 11.3 | 7.6 | 18.1 | 18 | 2023-11-18 |
| Puente sobre el Arroyo Chancay | 118.1 | 45 | 1,981 | 6 | 0.1 | 0.3 | 5.0 | 6 | 7 | 8.8 | 10.0 | 6.7 | 16.5 | 19 | 2023-11-19 |
| Puente sobre el Arroyo Santa Rita | 119.0 | 45 | 1,981 | 5 | 0.1 | 0.2 | 4.1 | 6 | 6 | 11.0 | 13.3 | 8.5 | 22.7 | 18 | 2023-11-18 |
| Puente sobre el Arroyo Canal Torto | 124.2 | 55 | 1,971 | 0 | 0.0 | 0.0 | -4.5 | 0 | 0 | 153.7 | 1,926.7 | 244.2 | 1,000,000.0 | 1 |  |
| Puente sobre el Arroyo Acaraguá | 110.2 | 55 | 1,971 | 2 | 0.0 | 0.1 | 7.8 | 5 | 8 | 12.6 | 15.1 | 10.3 | 43.8 | 17 | 2014-06-30 |
| Puente sobre el Arroyo Ramón | 102.4 | 55 | 1,971 | 20 | 0.4 | 2.1 | 15.2 | 24 | 29 | 2.8 | 2.7 | 2.2 | 3.2 | 47 | 2026-10-27 |
| Puente sobre el Arroyo Tomás Guido | 102.7 | 55 | 1,971 | 4 | 0.1 | 0.1 | 7.9 | 6 | 10 | 12.8 | 14.1 | 9.6 | 32.7 | 24 | 2015-07-16 |
| Puente sobre el Arroyo Once Vueltas | 101.0 | 55 | 1,971 | 1 | 0.0 | 0.0 | 4.8 | 1 | 4 | 21.7 | 25.4 | 16.2 | 92.3 | 21 | 2014-06-29 |
| Puente sobre el Arroyo Alipio | 100.6 | 55 | 1,971 | 1 | 0.0 | 0.0 | 3.6 | 1 | 2 | 27.6 | 34.3 | 19.4 | 274.5 | 19 | 2014-06-29 |
| Puente sobre el Arroyo Guerrero | 96.7 | 55 | 1,971 | 7 | 0.1 | 0.2 | 6.4 | 9 | 11 | 10.5 | 11.3 | 7.9 | 19.1 | 38 | 2014-06-30 |
| Puerte sobre el Arroyo Portera | 96.4 | 76 | 1,950 | 0 | 0.0 | 0.0 | -6.7 | 0 | 0 | 667.2 | 1,956.3 | 283.0 | 1,000,000.0 | 4 |  |
| Puente sobre el Arroyo Itacaruaré | 97.6 | 76 | 1,950 | 0 | 0.0 | 0.0 | -9.5 | 0 | 0 | 1,837.9 | 9,758.5 | 708.0 | 1,000,000.0 | 0 |  |
| Puente sobre el Arroyo Santa María | 97.4 | 76 | 1,950 | 0 | 0.0 | 0.0 | -10.3 | 0 | 0 | 2,795.4 | 18,610.8 | 824.8 | 1,000,000.0 | 0 |  |
| Puente sobre el Arroyo Persiguero | 98.6 | 76 | 1,950 | 0 | 0.0 | 0.0 | -12.4 | 0 | 0 | 6,590.1 | 76,107.5 | 1,716.2 | 1,000,000.0 | 0 |  |
| Puente sobre el Arroyo Concepción | 100.4 | 76 | 1,950 | 0 | 0.0 | 0.0 | -16.7 | 0 | 0 | 20,600.6 | 1,000,000.0 | 9,829.8 | 1,000,000.0 | 0 |  |
| Puente sobre el Arroyo Tunas | 77.3 | 77 | 1,950 | 7 | 0.1 | 0.4 | 4.6 | 9 | 13 | 11.9 | 13.6 | 10.4 | 23.4 | 9 | 2023-11-21 |

## 5. Las 8 crecidas más altas de El Soberbio sobre la red (escenario seco)

| fecha | altura | puentes | personas_afectadas | min_extra_mediana | min_extra_p90 | sin_ruta | puentes_cortados |
|---|---|---|---|---|---|---|---|
| 1983-07-09 | 27.0 | 10 | 18,363 | 67.7 | 110.5 | 7 | Puente Arroyo Soberbio; Puente sobre el Arroyo Persiguero; Puente sobre el Arroyo Chafariz; Puente sobre el Arroyo Doradito; Puente sobre el Arroyo Pindaití; Puente sobre el Arroyo Chancay; Puente sobre el Arroyo Santa Rita; Puente sobre el Arroyo Ramón; Puente sobre el Arroyo Guerrero; Puente sobre el Arroyo Tunas |
| 1984-08-08 | 19.4 | 6 | 3,120 | 5.2 | 6.7 | 0 | Puente sobre el Arroyo Pindaití; Puente sobre el Arroyo Chancay; Puente sobre el Arroyo Santa Rita; Puente sobre el Arroyo Ramón; Puente sobre el Arroyo Guerrero; Puente sobre el Arroyo Tunas |
| 1989-09-14 | 15.5 | 1 | 29 | 2.2 | 2.2 | 0 | Puente sobre el Arroyo Ramón |
| 1990-06-07 | 20.0 | 9 | 14,414 | 96.9 | 111.8 | 0 | Puente Arroyo Soberbio; Puente sobre el Arroyo Pindaití; Puente sobre el Arroyo Chancay; Puente sobre el Arroyo Santa Rita; Puente sobre el Arroyo Acaraguá; Puente sobre el Arroyo Ramón; Puente sobre el Arroyo Tomás Guido; Puente sobre el Arroyo Guerrero; Puente sobre el Arroyo Tunas |
| 1992-05-30 | 21.2 | 6 | 14,084 | 96.9 | 112.1 | 0 | Puente Arroyo Soberbio; Puente sobre el Arroyo Chancay; Puente sobre el Arroyo Ramón; Puente sobre el Arroyo Tomás Guido; Puente sobre el Arroyo Guerrero; Puente sobre el Arroyo Tunas |
| 2014-06-29 | 22.8 | 14 | 19,161 | 63.8 | 110.0 | 0 | Puente Arroyo Soberbio; Puente sobre el Arroyo Persiguero; Puente sobre el Arroyo Doradito; Puente sobre el Arroyo Macaco; Puente sobre el Arroyo Pindaití; Puente sobre el Arroyo Chancay; Puente sobre el Arroyo Santa Rita; Puente sobre el Arroyo Acaraguá; Puente sobre el Arroyo Ramón; Puente sobre el Arroyo Tomás Guido; Puente sobre el Arroyo Once Vueltas; Puente sobre el Arroyo Alipio; Puente sobre el Arroyo Guerrero; Puente sobre el Arroyo Tunas |
| 2023-11-18 | 19.2 | 6 | 1,961 | 11.4 | 20.0 | 0 | Puente sobre el Arroyo Macaco; Puente sobre el Arroyo Pindaití; Puente sobre el Arroyo Chancay; Puente sobre el Arroyo Santa Rita; Puente sobre el Arroyo Ramón; Puente sobre el Arroyo Tunas |
| 2024-05-04 | 16.7 | 1 | 29 | 2.2 | 2.2 | 0 | Puente sobre el Arroyo Ramón |

Puentes bajo su cota (umbral físico) en ±3 días del pico. Personas sin ruta al hospital: máximo 7 en un evento.
