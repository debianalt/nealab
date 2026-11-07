# Validación del archivo de noticias (cortes de puentes, caminos y luz)

Generado por `pipeline/noticias/validar_noticias.py` el 2026-11-07. Datos: 6 medios con API de WordPress abierta; extracción por reglas (`extraer.py`).

## 1. Notas por medio

| medio | notas | desde | hasta |
|---|---|---|---|
| primeraedicion | 13,945 | 2012-04-01 | 2026-11-07 |
| economis | 6,460 | 2017-01-13 | 2026-11-07 |
| canal12 | 5,066 | 2017-09-08 | 2026-11-07 |
| misionesonline | 4,740 | 2000-05-23 | 2026-11-07 |
| misionesopina | 4,026 | 2015-01-17 | 2026-11-07 |
| noticiasdel6 | 2,924 | 2023-01-01 | 2026-11-07 |

Notas por año (todos los medios):

| anio | notas |
|---|---|
| 2000 | 25 |
| 2001 | 17 |
| 2002 | 26 |
| 2003 | 62 |
| 2004 | 84 |
| 2005 | 93 |
| 2006 | 91 |
| 2007 | 150 |
| 2008 | 98 |
| 2009 | 156 |
| 2010 | 53 |
| 2011 | 83 |
| 2012 | 660 |
| 2013 | 872 |
| 2014 | 1,053 |
| 2015 | 1,156 |
| 2016 | 1,028 |
| 2017 | 1,679 |
| 2018 | 1,859 |
| 2019 | 1,810 |
| 2020 | 2,308 |
| 2021 | 2,416 |
| 2022 | 2,926 |
| 2023 | 4,370 |
| 2024 | 3,656 |
| 2025 | 4,680 |
| 2026 | 5,750 |

## 2. Cobertura sobre las notas del agente de la fase 3

De 30 notas distintas de `cortes_rp2_prensa.csv`, 23 son de los 6 medios con API; 19 están en el corpus.

| medio | notas_del_agente | en_corpus |
|---|---|---|
| canal12 | 2 | 2 |
| economis | 1 | 1 |
| misionesonline | 11 | 7 |
| noticiasdel6 | 2 | 2 |
| primeraedicion | 7 | 7 |

Notas del agente de esos medios que no están (para revisar consultas):

| fecha | medio | lugar_textual | url |
|---|---|---|---|
| 2017-05-31 | misionesonline | Aº YABOTI – RP 2 – acceso Parque Provincial Saltos del Moconá | https://misionesonline.net/2017/05/31/por-las-crecidas-estan-clausurados-el-puente-yaboti-en-el-soberbio-y-guerrero-en-san-javier/ |
| 2022-05-08 | misionesonline | puente Pindaytí (corte «desde el miércoles») | https://misionesonline.net/2022/05/08/puente-pinayti-rehabilitacion/ |
| 2023-11-18 | misionesonline | Ruta Provincial Nº 2, a la altura del municipio de Colonia Aurora, en cercanías del arroyo El Doradito | https://misionesonline.net/2023/11/18/ruta-provincial-n2-inhabilitada/ |
| 2026-07-22 | misionesonline | puente provisorio del Ejército Argentino sobre el arroyo Pindaytí, Ruta Provincial Nº 2 (desde las 22 h) | https://misionesonline.net/2026/07/22/vialidad-provincial-interrumpira-el-paso-sobre-el-puente-provisorio-del-arroyo-pindayti/ |

## 3. Menciones

| tipo | menciones | notas | % con lugar | % lugar ambiguo |
|---|---|---|---|---|
| crecida | 12,943 | 5,210 | 57.1 | 45.4 |
| corte_luz | 6,316 | 3,505 | 37.4 | 84.8 |
| corte_ruta_camino | 5,420 | 3,131 | 46.8 | 66.5 |
| evacuados_aislados | 3,086 | 1,752 | 45.6 | 47.3 |
| corte_puente | 2,116 | 1,050 | 62.7 | 61.5 |

Lugares más mencionados en cortes de puente o camino:

| lugar_tipo | lugar_nombre | menciones | notas |
|---|---|---|---|
| municipio | Posadas | 230 | 204 |
| hidrometro | Corrientes | 138 | 100 |
| cauce | Uruguay | 114 | 78 |
| cauce | Tabay | 113 | 58 |
| cauce | Yabotí | 111 | 73 |
| cauce | Garupá | 91 | 57 |
| municipio | Jardín América | 90 | 66 |
| cauce | Ramón | 88 | 40 |
| municipio | El Soberbio | 85 | 65 |
| municipio | Eldorado | 84 | 59 |
| municipio | Montecarlo | 83 | 61 |
| asentamiento | Piray | 79 | 44 |
| municipio | Profundidad | 77 | 48 |
| municipio | Panambí | 76 | 39 |
| municipio | Alba Posse | 70 | 41 |
| municipio | San Antonio | 65 | 58 |
| municipio | Oberá | 61 | 51 |
| municipio | Mártires | 61 | 45 |
| asentamiento | Domingo Savio | 59 | 45 |
| cauce | Yabebiry | 56 | 42 |
| cauce | Cuñá | 54 | 28 |
| municipio | San Javier | 53 | 40 |
| municipio | San Vicente | 52 | 44 |
| cauce | Piray Guazú | 51 | 26 |
| municipio | San Ignacio | 47 | 42 |

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
