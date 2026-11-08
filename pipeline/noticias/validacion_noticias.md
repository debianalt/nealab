# Validación del archivo de noticias (cortes de puentes y caminos, crecidas, evacuados)

Generado por `pipeline/noticias/validar_noticias.py` el 2026-11-07. Datos: 6 medios con API de WordPress abierta, El Territorio y La Voz de Cataratas por sitemaps y HTML, MisionesCuatro por copias de la Wayback Machine (su sitio desafía a todo cliente que no sea un navegador) y el archivo completo de Misiones Online por sus sitemaps; extracción por reglas (`extraer.py`).

## 1. Notas por medio

| medio | via | notas | desde | hasta |
|---|---|---|---|---|
| primeraedicion | api_wp | 13,945 | 2012-04-01 | 2026-11-07 |
| misionesonline | api_wp | 12,910 | 2000-04-06 | 2026-11-07 |
| economis | api_wp | 6,460 | 2017-01-13 | 2026-11-07 |
| canal12 | api_wp | 5,066 | 2017-09-08 | 2026-11-07 |
| misionesopina | api_wp | 4,026 | 2015-01-17 | 2026-11-07 |
| noticiasdel6 | api_wp | 2,924 | 2023-01-01 | 2026-11-07 |
| lavozdecataratas | html | 2,132 | 2021-02-15 | 2026-11-07 |
| misionescuatro | wayback | 351 | 2015-10-07 | 2026-10-13 |
| elterritorio | html | 284 | 2007-05-21 | 2026-11-06 |

Notas por año (todos los medios):

| anio | notas |
|---|---|
| 2000 | 95 |
| 2001 | 87 |
| 2002 | 109 |
| 2003 | 173 |
| 2004 | 260 |
| 2005 | 290 |
| 2006 | 301 |
| 2007 | 356 |
| 2008 | 381 |
| 2009 | 454 |
| 2010 | 186 |
| 2011 | 398 |
| 2012 | 970 |
| 2013 | 1,174 |
| 2014 | 1,469 |
| 2015 | 1,579 |
| 2016 | 1,550 |
| 2017 | 2,338 |
| 2018 | 2,406 |
| 2019 | 2,339 |
| 2020 | 2,777 |
| 2021 | 3,128 |
| 2022 | 3,761 |
| 2023 | 5,384 |
| 2024 | 4,363 |
| 2025 | 5,311 |
| 2026 | 6,459 |

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

## 3. Menciones

| tipo | menciones | notas | % con lugar | % lugar ambiguo |
|---|---|---|---|---|
| crecida | 15,604 | 6,369 | 56.3 | 44.4 |
| corte_ruta_camino | 7,162 | 4,078 | 46.3 | 68.6 |
| evacuados_aislados | 3,646 | 2,058 | 46.1 | 47.7 |
| corte_puente | 2,731 | 1,402 | 58.5 | 61.4 |

Lugares más mencionados en cortes de puente o camino:

| lugar_tipo | lugar_nombre | menciones | notas |
|---|---|---|---|
| municipio | Posadas | 335 | 281 |
| hidrometro | Corrientes | 184 | 131 |
| municipio | Montecarlo | 140 | 109 |
| cauce | Tabay | 137 | 70 |
| municipio | Eldorado | 135 | 98 |
| cauce | Uruguay | 130 | 91 |
| cauce | Garupá | 120 | 75 |
| cauce | Yabotí | 119 | 79 |
| municipio | Jardín América | 113 | 85 |
| cauce | Ramón | 99 | 44 |
| asentamiento | Piray | 97 | 54 |
| municipio | El Soberbio | 95 | 73 |
| municipio | Profundidad | 87 | 53 |
| municipio | San Vicente | 85 | 70 |
| municipio | Panambí | 83 | 43 |
| municipio | Oberá | 83 | 68 |
| municipio | Alba Posse | 80 | 47 |
| municipio | San Antonio | 76 | 69 |
| municipio | San Pedro | 72 | 59 |
| cauce | Cuñá | 68 | 36 |
| municipio | San Javier | 67 | 50 |
| cauce | Piray Guazú | 63 | 33 |
| municipio | Puerto Iguazú | 62 | 54 |
| asentamiento | Domingo Savio | 62 | 47 |
| municipio | Mártires | 62 | 46 |

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
