# Integridad de los datos de la fase 4 (luz, servicios, conectividad, ríos)

Generado por `pipeline/infra/validar_datos_f4.py` el 2026-11-07. Sólo datos: ningún escenario ni modelo.

## 1. Inventario (`infra.fuentes`)

| tabla | fuente | fecha_fuente | n |
|---|---|---|---|
| infra.asentamientos | BAHRA – Georef (datos.gob.ar) | 2026-11-07 | 450 |
| infra.escuelas | Ministerio de Capital Humano – Mapa Educativo Nacional | 2026-11-07 | 3,035 |
| infra.luz_asentamiento_diaria | NASA Black Marble / NOAA VIIRS (Google Earth Engine) | 2026-11-07 | 2,010,472 |
| infra.luz_mensual | NASA Black Marble / NOAA VIIRS (Google Earth Engine) | 2026-11-07 | 5,237,802 |
| infra.ookla | Ookla Open Data (s3 ookla-open-data) | 2026-T3 | 92,067 |
| infra.osm_cauces | OpenStreetMap (extracto Geofabrik) | 2026-11-07 | 7,894 |
| infra.osm_electrica | OpenStreetMap (Geofabrik) | 2026-11-07 | 224 |
| infra.osm_estructuras | OpenStreetMap (extracto Geofabrik) | 2026-11-07 | 2,867 |
| infra.osm_telecom | OpenStreetMap (Geofabrik) | 2026-11-07 | 605 |
| infra.rio_lecturas_otras | INA – API a5 (alerta.ina.gob.ar) | 2026-11-07 | 268,886 |
| infra.salud_ign | Instituto Geográfico Nacional | 2026-11-07 | 321 |
| infra.visor_aborigenes | Visor de emergencias de Misiones (Dir. de Modernización) | Thu, 27 Nov 2025 14:54:04 GMT | 116 |
| infra.visor_alcantarillas | Visor de emergencias de Misiones (Dir. de Modernización) | 2026-11-07 | 1,771 |
| infra.visor_ambulancias | Visor de emergencias de Misiones (Dir. de Modernización) | Tue, 10 Nov 2020 12:16:34 GMT | 91 |
| infra.visor_antenas | Visor de emergencias de Misiones (Dir. de Modernización) | 2026-11-07 | 491 |
| infra.visor_aserraderos | Visor de emergencias de Misiones (Dir. de Modernización) | Thu, 27 Nov 2025 14:55:18 GMT | 680 |
| infra.visor_barrera_ambiental | Visor de emergencias de Misiones (Dir. de Modernización) | Mon, 24 Jan 2022 19:45:00 GMT | 32 |
| infra.visor_bomberos_policia | Visor de emergencias de Misiones (Dir. de Modernización) | Sat, 17 Sep 2022 13:04:45 GMT | 15 |
| infra.visor_bomberos_voluntarios | Visor de emergencias de Misiones (Dir. de Modernización) | Wed, 25 Sep 2024 14:04:31 GMT | 37 |
| infra.visor_caps | Visor de emergencias de Misiones (Dir. de Modernización) | 2026-11-07 | 322 |
| infra.visor_cauces | Visor de emergencias de Misiones (Dir. de Modernización) | 2026-11-07 | 4,450 |
| infra.visor_cisternas | Visor de emergencias de Misiones (Dir. de Modernización) | Fri, 18 Sep 2020 11:45:44 GMT | 10 |
| infra.visor_combate_incendios | Visor de emergencias de Misiones (Dir. de Modernización) | Mon, 21 Sep 2020 22:52:38 GMT | 14 |
| infra.visor_combustible | Visor de emergencias de Misiones (Dir. de Modernización) | Mon, 04 Aug 2025 13:50:18 GMT | 154 |
| infra.visor_ecologia | Visor de emergencias de Misiones (Dir. de Modernización) | Thu, 29 Jul 2021 19:32:57 GMT | 22 |
| infra.visor_estanques | Visor de emergencias de Misiones (Dir. de Modernización) | Mon, 06 Jun 2022 13:20:35 GMT | 12,125 |
| infra.visor_hidrantes | Visor de emergencias de Misiones (Dir. de Modernización) | Thu, 27 Jan 2022 13:36:24 GMT | 327 |
| infra.visor_hidrometros | Visor de emergencias de Misiones (Dir. de Modernización) | 2026-11-07 | 35 |
| infra.visor_hospitales | Visor de emergencias de Misiones (Dir. de Modernización) | 2026-11-07 | 53 |
| infra.visor_jurisdicciones_policia | Visor de emergencias de Misiones (Dir. de Modernización) | Wed, 02 Aug 2023 11:00:27 GMT | 115 |
| infra.visor_lineas_at | Visor de emergencias de Misiones (Dir. de Modernización) | 2026-11-07 | 118 |
| infra.visor_municipalidades | Visor de emergencias de Misiones (Dir. de Modernización) | Wed, 29 Oct 2025 13:51:12 GMT | 85 |
| infra.visor_municipios | Visor de emergencias de Misiones (Dir. de Modernización) | 2026-11-07 | 78 |
| infra.visor_municipios_limites | Visor de emergencias de Misiones (Dir. de Modernización) | Fri, 01 Aug 2025 13:40:36 GMT | 79 |
| infra.visor_parque_solar | Visor de emergencias de Misiones (Dir. de Modernización) | Mon, 03 Nov 2025 13:07:18 GMT | 11 |
| infra.visor_perforaciones | Visor de emergencias de Misiones (Dir. de Modernización) | Wed, 29 Oct 2025 13:50:53 GMT | 627 |
| infra.visor_pistas | Visor de emergencias de Misiones (Dir. de Modernización) | Thu, 19 May 2022 10:28:05 GMT | 15 |
| infra.visor_policia | Visor de emergencias de Misiones (Dir. de Modernización) | Mon, 04 Aug 2025 11:54:55 GMT | 348 |
| infra.visor_progresivas | Visor de emergencias de Misiones (Dir. de Modernización) | Tue, 29 Sep 2020 19:16:30 GMT | 1,982 |
| infra.visor_progresivas_parana | Visor de emergencias de Misiones (Dir. de Modernización) | Wed, 30 Dec 2020 22:30:00 GMT | 73 |
| infra.visor_puentes | Visor de emergencias de Misiones (Dir. de Modernización) | 2026-11-07 | 1,014 |
| infra.visor_puentes_rp2 | Visor de emergencias de Misiones (Dir. de Modernización) | 2026-11-07 | 29 |
| infra.visor_reservas | Visor de emergencias de Misiones (Dir. de Modernización) | Fri, 29 May 2026 14:09:44 GMT | 141 |
| infra.visor_tanques | Visor de emergencias de Misiones (Dir. de Modernización) | Wed, 29 Oct 2025 13:51:04 GMT | 273 |
| infra.visor_telefonos_sos | Visor de emergencias de Misiones (Dir. de Modernización) | Fri, 10 Dec 2021 19:05:53 GMT | 52 |
| infra.visor_transformadoras | Visor de emergencias de Misiones (Dir. de Modernización) | 2026-11-07 | 73 |
| infra.visor_vialidad | Visor de emergencias de Misiones (Dir. de Modernización) | Thu, 10 Feb 2022 19:12:31 GMT | 15 |
| infra.visor_zonas_inundables | Visor de emergencias de Misiones (Dir. de Modernización) | 2026-11-07 | 6 |
| infra.visor_zonas_urbanas | Visor de emergencias de Misiones (Dir. de Modernización) | Wed, 29 Oct 2025 13:51:13 GMT | 133 |

## 2. Servicios

Escuelas (Mapa Educativo) por jurisdicción del CUE y gestión. El recorte de 5 km suma escuelas de Corrientes junto al límite.

| jurisdiccion | gestion | n |
|---|---|---|
| Corrientes (borde) | Estatal | 27 |
| Corrientes (borde) | Privado | 1 |
| Misiones | Estatal | 2,736 |
| Misiones | Privado | 271 |

Salud del IGN contra hospitales y CAPS del visor (a ≤ 150 m):

| salud_ign | con_hospital_visor_150m | con_caps_visor_150m |
|---|---|---|
| 321 | 47 | 177 |

Asentamientos BAHRA por categoría:

| categoria | n |
|---|---|
| Paraje | 310 |
| Localidad simple | 131 |
| Componente de localidad compuesta | 7 |
| Entidad | 2 |

Red eléctrica y telecomunicaciones de OSM, contra las del visor:

| capa | n |
|---|---|
| osm_electrica generator | 12 |
| osm_electrica line | 56 |
| osm_electrica minor_line | 63 |
| osm_electrica plant | 3 |
| osm_electrica substation | 74 |
| osm_electrica transformer | 16 |
| osm_telecom | 605 |
| visor antenas | 491 |
| visor lineas_at | 118 |
| visor transformadoras | 73 |

## 3. Hidrómetros del Paraná y del Iguazú (INA)

| serie | nombre | rio | desde | hasta | lecturas | % días | max_m | nivel_alerta_m | marcadas |
|---|---|---|---|---|---|---|---|---|---|
| 8 | Andresito | IGUAZU | 1984-01-01 | 2026-11-06 | 14,832 | 94.8 | 13.2 | 10.5 | 0 |
| 9 | Puerto Iguazú | PARANAMED | 1966-01-01 | 2026-11-06 | 18,903 | 85.1 | 40.0 | 25.0 | 0 |
| 10 | Libertad | PARANAMED | 1951-01-04 | 2026-11-06 | 22,817 | 82.4 | 37.8 | 29.0 | 1 |
| 11 | El Dorado | PARANAMED | 1950-01-04 | 2026-11-06 | 20,515 | 73.1 | 30.7 | 23.0 | 0 |
| 12 | Libertador | PARANAMED | 1956-03-01 | 2026-11-06 | 21,877 | 84.7 | 23.0 | 14.5 | 1 |
| 13 | Santa Ana | PARANAMED | 2006-01-01 | 2026-11-06 | 6,291 | 82.6 | 11.4 | 9.3 | 0 |
| 14 | Posadas | PARANAMED | 1950-01-01 | 2026-11-06 | 27,926 | 99.5 | 10.8 | 11.0 | 0 |
| 15 | Ituzaingó | PARANAMED | 1950-01-01 | 2026-11-06 | 27,782 | 99.0 | 9.7 | 3.5 | 0 |
| 16 | Itá Ibaté | PARANAMED | 1950-01-01 | 2026-11-06 | 27,094 | 96.5 | 8.2 | 7.0 | 0 |
| 17 | Itatí | PARANAMED | 1950-01-01 | 2026-11-06 | 27,399 | 97.6 | 10.1 | 6.8 | 0 |
| 18 | Paso de la Patria | PARANAMED | 1956-01-01 | 2026-11-06 | 25,410 | 98.2 | 9.5 | 6.5 | 0 |
| 19 | Corrientes | PARANAMED | 1950-01-01 | 2026-11-06 | 28,040 | 99.9 | 9.0 | 6.5 | 0 |

«% días» = lecturas válidas sobre días calendario entre la primera y la última: los huecos son de la fuente.

## 4. Conectividad (Ookla Open Data)

| tipo | anio | trimestres | teselas | pruebas | bajada_media_mbps |
|---|---|---|---|---|---|
| fixed | 2019 | 4 | 5,677 | 72,633 | 14.0 |
| fixed | 2020 | 4 | 3,193 | 34,057 | 24.4 |
| fixed | 2021 | 4 | 5,073 | 62,203 | 34.1 |
| fixed | 2022 | 4 | 9,586 | 139,483 | 36.0 |
| fixed | 2023 | 4 | 9,801 | 117,052 | 49.1 |
| fixed | 2024 | 4 | 10,928 | 125,898 | 65.8 |
| fixed | 2025 | 4 | 13,175 | 155,387 | 94.0 |
| fixed | 2026 | 3 | 10,398 | 119,483 | 120.4 |
| mobile | 2019 | 4 | 3,423 | 11,316 | 18.1 |
| mobile | 2020 | 4 | 3,534 | 12,931 | 21.1 |
| mobile | 2021 | 4 | 3,350 | 10,161 | 24.0 |
| mobile | 2022 | 4 | 3,242 | 9,714 | 25.3 |
| mobile | 2023 | 4 | 2,745 | 7,660 | 33.7 |
| mobile | 2024 | 4 | 2,681 | 6,630 | 47.4 |
| mobile | 2025 | 4 | 2,863 | 6,698 | 63.4 |
| mobile | 2026 | 3 | 2,398 | 6,141 | 68.2 |

Una tesela sin pruebas no aparece: la ausencia no es falta de señal. ENACOM no tiene descarga directa (7-nov-2026).

## 5. Luces nocturnas

Black Marble diario (VNP46A2): 5,347 días en GEE, 5,347 bajados (100.00 %). Faltan: ninguno.

Control: valor bajado (radiancia ×10 redondeada / 10) contra GEE en el centro del mismo píxel:

| lugar | día | bajado | GEE |
|---|---|---|---|
| Posadas | 2026_10_27 | 85.1 | 85.0 |
| Oberá | 2026_10_27 | 81.2 | 81.2 |
| Eldorado | 2026_10_27 | 33.0 | 33.0 |

Píxeles iluminados (mediana anual > 0,5 en algún año, 2013–2025): 34,234 de 402,325.

Cobertura diaria por año (píxeles con calidad alta y despejados / píxeles iluminados asignados a un asentamiento): Misiones es nublada; la fracción válida es la que hay.

| anio | dias | % píxeles válidos |
|---|---|---|
| 2012 | 343 | 59.1 |
| 2013 | 365 | 60.2 |
| 2014 | 365 | 57.9 |
| 2015 | 365 | 53.1 |
| 2016 | 366 | 56.5 |
| 2017 | 365 | 57.8 |
| 2018 | 365 | 59.1 |
| 2019 | 365 | 53.9 |
| 2020 | 366 | 65.8 |
| 2021 | 365 | 62.6 |
| 2022 | 350 | 57.8 |
| 2023 | 365 | 57.4 |
| 2024 | 347 | 57.5 |
| 2025 | 365 | 54.0 |
| 2026 | 290 | 52.7 |

Meses del compuesto mensual cuya media provincial se aparta > 50 % de la mediana de su año (anomalías de la fuente; usar con cuidado):

| mes | media | mediana_del_anio |
|---|---|---|
| 2018-05-01 | 5.2 | 2.9 |

Mensual (VCMSLCFG, media del año) contra anual (V21 hasta 2021, V22 desde 2022; mediana) por píxel iluminado:

| anio | corr_mensual_vs_anual | pixeles |
|---|---|---|
| 2014 | 0.9 | 34,234 |
| 2015 | 0.9 | 34,234 |
| 2016 | 0.9 | 34,234 |
| 2017 | 0.9 | 34,234 |
| 2018 | 0.4 | 34,234 |
| 2019 | 0.9 | 34,234 |
| 2020 | 0.9 | 34,234 |
| 2021 | 0.9 | 34,234 |
| 2022 | 0.9 | 34,234 |
| 2023 | 1.0 | 34,234 |
| 2024 | 1.0 | 34,234 |
| 2025 | 0.9 | 34,234 |
