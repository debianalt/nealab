## Requisitos agroclimáticos y edáficos de soja, maíz, algodón y girasol en secano (NEA): umbrales publicados con fuente

**Cómo leer la tabla**
- **Página:** en FAO 66 la página impresa es igual a la página del PDF menos 15. Están verificadas una por una con `pdftotext`.
- **«FAO Crop info»:** son las fichas web de FAO Land & Water. Su texto proviene de Doorenbos & Kassam (1979), FAO I&D Paper 33: la ficha de algodón remite a «Figure 14… see p. 40 and Chapter VI», que es la estructura de ese manual. Las URL vigentes dan 404, así que cito la captura de Wayback del 10 o 15 de diciembre de 2022. No tienen paginación.
- **ECOCROP:** leí las fichas web de FAO (ecocrop.apps.fao.org), no el archivo `.rds` del paquete Recocrop; no se me permitió abrirlo con R. Para anuales, «Temperat. requir.» es la temperatura media de la estación de crecimiento y «Rainfall (annual)» es la lluvia anual.
- **Citas secundarias:** cuando una tesis cita a otro autor, lo marco como «cita sec.».
- **Fuentes no argentinas:**
  - CIREN es Chile, 1989, y su PDF es un OCR.
  - Verdoodt & Van Ranst es Ruanda, adaptado de Sys et al. (1993).
  - NDSU e Iowa State son extensión de EE. UU.

Para no repetir la cita completa en cada fila, uso claves; la referencia completa de cada una está en la lista que sigue a la tabla.

| cultivo | variable | tipo | valor_min | valor_max | unidad | etapa o condición | cita completa | página/tabla | URL/DOI | cita textual |
|---|---|---|---|---|---|---|---|---|---|---|
| soja | T media | óptimo | 20 | 30 | °C | crecimiento | [FAO66] | p. 129 | [U-FAO66] | «optimum growing conditions at mean temperatures of 20 °C to 30 °C» |
| soja | T media | óptimo | 20 | 33 | °C | estación de crecimiento | [ECO-soja] | fila Temperat. requir. | [U-ECO-soja] | «Temperat. requir. \| 20 \| 33 \| 10 \| 38» |
| soja | T media | límite inferior / superior absoluto | 10 | 38 | °C | estación de crecimiento | [ECO-soja] | ídem | [U-ECO-soja] | ídem |
| soja | T mínima | límite inferior absoluto | 10 (crecimiento) | 15 (producción) | °C | ciclo | [CI-soja] | web s/p | [U-CI-soja] | «Minimum temperatures for growth are about 10°C and for crop production about 15°C.» |
| soja | T (caída de la tasa de crecimiento) | límites del óptimo | 18 | 35 | °C | ciclo | [CI-soja] | web s/p | [U-CI-soja] | «growth rates decrease above 35°C and below 18°C» |
| soja | T base para grados-día | — | sin fuente | — | °C | — | — | — | — | (único proxy: mínimo de crecimiento ~10 °C, fila anterior) |
| soja | grados-día del ciclo | requerido | sin fuente | — | °C·d | — | — | — | — | — |
| soja | calor: aborto floral | crítico de daño | 35 | — | °C | floración y fructificación | [FAO66] | p. 129 | [U-FAO66] | «so has abortion of flower and newly set fruit at 35 °C» |
| soja | calor: canopeo | crítico de daño | ~40 | — | °C | ciclo | [FAO66] | p. 129 | [U-FAO66] | «High canopy temperature (approaching 40 °C) reduces CO2 assimilation rate» |
| soja | calor: umbral de rendimiento | límite superior del óptimo | 30 | — | °C | estación completa (EE. UU.) | [SR09] | p. 15594 (resumen) | doi:10.1073/pnas.0906865106 | «yields increase with temperature up to 29° C for corn, 30° C for soybeans, and 32° C for cotton but that temperatures above these thresholds are very harmful» |
| soja | calor en emergencia | crítico de daño | 35 | 38 | °C | emergencia, Chaco | [INTA-LB-soja] | web | [U-LB-soja] | «temperaturas superiores a 35-38 °C reducen de manera marcada la velocidad» |
| soja | helada en hojas superiores | crítico de daño (planta) | −1,1 (30 °F) | 0 (32 °F) | °C | canopeo superior | [NDSU] | web | [U-NDSU] | «Soybean leaves on the top of the canopy are easily damaged by frost in the 30 °F to 32 °F range.» |
| soja | helada letal | crítico de daño (planta) | < −1,1 (<30 °F), sostenida | — | °C | tallos y hojas | [NDSU] | web | [U-NDSU] | «Temperatures lower than 30 °F, for any extended period, can kill soybean plants (stems and leaves).» |
| soja | helada en el meristema | crítico de daño (planta) | −2,2 (28 °F) | — | °C | plántula | [ISU] | web | [U-ISU] | «The apical meristem and/or auxiliary buds are not expected to be killed at temperatures down to 28 oF.» |
| soja | helada: etapas sensibles | — | — | — | — | emergencia y llenado; R5 la peor | [FAO66]; [NDSU] | p. 128; web | [U-FAO66] | «very sensitive to frost at seed emergence and pod filling» / «greatest yield losses occurred when frost occurred at stage R5» |
| soja | período libre de heladas | requerido | sin fuente explícita | — | días | proxy: duración del ciclo 70–140 (FAO66 p. 126); 100–130 o más ([CI-soja]); 75–180 ([ECO-soja]) | — | — | — | — |
| soja | lluvia | óptimo | 600 | 1500 | mm/año | anual | [ECO-soja] | fila Rainfall | [U-ECO-soja] | «Rainfall (annual) \| 600 \| 1500 \| 450 \| 1800» |
| soja | lluvia | límite absoluto | 450 | 1800 | mm/año | anual | [ECO-soja] | ídem | [U-ECO-soja] | ídem |
| soja | ETm del ciclo | requerido | 450 | 700 | mm/ciclo | rendimiento máximo | [CI-soja] | web s/p | [U-CI-soja] | «Water requirements (ETm) for maximum production vary between 450 and 700 mm/ season» |
| soja | ET del ciclo | rango observado | 300 | 800 | mm/ciclo | según clima y cultivar | [FAO66] | p. 128 | [U-FAO66] | «evapotranspiration (ET) of soybean varies between 300 mm and 800 mm» |
| soja | demanda hídrica regional | requerido | 650 | — | mm | Chaco, incluye barbecho | [INTA-LB-soja] | web | [U-LB-soja] | «La demanda hídrica de la soja para la región puede estimarse en torno a los 650 milímetros» |
| soja | período crítico hídrico | — | — | — | — | fin de floración y formación de vainas; meses: sin fuente | [CI-soja] | web s/p | [U-CI-soja] | «particularly the later part of the flowering period (end 2) and early part of the yield formation (pod development, 3a)» |
| soja | déficit P − ETP | — | sin fuente | — | mm | — | — | — | — | — |
| soja | pH | óptimo | 6 | 6,5 | pH | — | [CI-soja] | web s/p | [U-CI-soja] | «Optimum soil pH is 6 to 6.5.» |
| soja | pH | óptimo / absoluto | 5,5 / 4,5 | 6,5 / 8,4 | pH | — | [ECO-soja] | fila Soil PH | [U-ECO-soja] | «Soil PH \| 5.5 \| 6.5 \| 4.5 \| 8.4» |
| soja | textura | limitante | — | — | — | excluye suelos muy arenosos | [CI-soja]; [ECO-soja] | web; fila Soil texture | [U-CI-soja] | «grown on a wide range of soils except those which are very sandy» / ECOCROP óptimo «medium, organic» |
| soja | textura (arcilla) | óptimo (S1) | 20 | 60 | % arcilla | también >60 con estructura en bloques; Ruanda | [VVR03] | Tabla 4.12, p. 100 | [U-VVR] | «clay (%) & structure 20 – 60, > 60 blocky» |
| soja | anegamiento | crítico de daño | 7 | — | días | más de 7 días; pérdida >40 % en R1 o R5 | [FAO66] | p. 129 | [U-FAO66] | «tolerates flooding or waterlogging up to 7 days, but yield can be reduced by more than 40 percent» |
| soja | fertilidad (suma de bases) | óptimo (S1) | >3,5 | — | cmol(+)/kg | Ruanda | [VVR03] | Tabla 4.12, p. 100 | [U-VVR] | «SBC (cmol(+) kg-1) > 3.5» |
| soja | pendiente | S1 / S2 / S3 | ≤6 / 6–13 / 13–25 | 25–55 (N1); >55 (N2) | % | Ruanda, adaptado de Sys | [VVR03] | p. 88; Tabla 4.12 | [U-VVR] | «Very suitable for crop production is a gradient equal to or smaller than 6 %. A slope gradient between 6 and 13 % is moderately suitable» |
| todos | pendiente (clases de Sys, iguales para cualquier cultivo) | clases | 0–8 / 8–16 / 16–30 | 30–50 / >50 | % | Sys et al. 1993, citado por [VVR03] | [VVR03] | p. 88 | [U-VVR] | «Crop independent suitability classes based on the slope gradient classes: 0–8 %, 8–16 %, 16–30 %, 30–50 %, and over 50 %, have been defined by Sys et al. (1993)» |
| maíz | T media | límite inferior absoluto | 15 | — | °C | estación sin heladas | [CI-maíz] | web s/p | [U-CI-maíz] | «grown … during the period when mean daily temperatures are above 15°C and frost-free» |
| maíz | T media | óptimo / absoluto | 18 / 10 | 33 / 47 | °C | estación de crecimiento | [ECO-maíz] | fila Temperat. requir. | [U-ECO-maíz] | «Temperat. requir. 18 33 10 47» |
| maíz | T de crecimiento | óptimo | 28 | 32 | °C | Chile | [CIREN-c] | ficha Maíz, p. 23 | [U-CIREN-c] | «Rango de temperatura óptima de crecimiento 28 a 32 C» |
| maíz | T máxima | límite superior absoluto | 40 | — | °C | Chile | [CIREN-c] | ídem | [U-CIREN-c] | «Límite máximo de temperatura de crecimiento: 40 °C» |
| maíz | T máxima tolerada con agua | límite superior absoluto | 45 | — | °C | con agua suficiente | [CI-maíz] | web s/p | [U-CI-maíz] | «tolerates hot and dry atmospheric conditions so long as sufficient water is available to the plant and temperatures are below 45°C» |
| maíz | T base (y T superior) para grados-día | base | 8 | 30 (Tupper) | °C | AquaCrop | [FAO66] | p. 119 | [U-FAO66] | «the base temperature (Tbase) for maize is set at 8 °C, and the upper temperature threshold (Tupper) … is set at 30 °C» |
| maíz | T base | base | 10 | 12 | °C | Chile | [CIREN-c] | ficha Maíz, p. 23 | [U-CIREN-c] | «Temperatura base o mínima de crecimiento 10 a 12°C» |
| maíz | suma térmica del ciclo | requerido | 2500 | 3000 | °C·d (suma de medias diarias, base 0 implícita) | variedades medias; precoces ~1800; tardías ≥3700 | [CI-maíz] | web s/p | [U-CI-maíz] | «for medium varieties are 2500 to 3000 degree days, while early varieties require about 1800 and late varieties 3700 or more» |
| maíz | grados-día hasta floración / madurez | requerido | 1122 (floración) | 2045 (madurez) | GD (base no indicada) | var. Bautista INTA, siembra de primavera | [INTA-Bautista] | web | [U-Bautista] | «En siembras de primavera florece a los 1122 grados día y llega a madurez a los 2045 grados día» |
| maíz | suma térmica (T > 10) | requerido | 950 | 1400 | °C·d | Chile; valor bajo frente al resto, tomar con cautela | [CIREN-c] | ficha Maíz, p. 23 | [U-CIREN-c] | «Suma térmica T> 10 entre siembra y cosecha: 950 a 1400 días-grados» |
| maíz | calor en floración | crítico de daño | 35 | — | °C | floración (nota de prensa) | [INTA-IIACS] | web | [U-IIACS] | «el registro de temperaturas superiores a 35 °C, durante el período de floración, puede provocar disminuciones en los rindes» |
| maíz | calor: umbral de rendimiento | límite superior del óptimo | 29 | — | °C | estación completa | [SR09] | p. 15594 | doi:10.1073/pnas.0906865106 | (ver fila de soja) |
| maíz | helada | crítico de daño (planta) | −2,2 (28 °F) | — | °C | tejido emergido, un par de horas | [ISU] | web | [U-ISU] | «Temperature 28 oF or lower for a couple of hours will be lethal to growing tissue that has emerged.» |
| maíz | helada: etapa sensible | — | — | — | — | plántula | [CI-maíz] | web s/p | [U-CI-maíz] | «The crop is very sensitive to frost, particularly in the seedling stage» |
| maíz | período libre de heladas | requerido (proxy: ciclo) | 110 | 140 | días | variedades medias, T > 20 °C; precoces 80–110 | [CI-maíz] | web s/p | [U-CI-maíz] | «early grain varieties take 80 to 110 days and medium varieties 110 to 140 days to mature» |
| maíz | lluvia | óptimo / absoluto | 600 / 400 | 1200 / 1800 | mm/año | anual | [ECO-maíz] | fila Rainfall | [U-ECO-maíz] | «Rainfall (annual) 600 1200 400 1800» |
| maíz | necesidad hídrica | requerido | 500 | 800 | mm/ciclo | ciclo medio, rendimiento máximo | [CI-maíz] | web s/p | [U-CI-maíz] | «a medium maturity grain crop requires between 500 and 800 mm of water depending on climate» |
| maíz | ET del ciclo | típico | 500 (típico 650) | 800 | mm | bien regado | [FAO66] | p. 117 | [U-FAO66] | «seasonal ET ranges from less than 500 to more than 800 mm … around 650 mm» |
| maíz | período crítico hídrico | — | — | — | — | floración: panojamiento, emisión de estigmas y polinización; meses: sin fuente | [CI-maíz] | web s/p | [U-CI-maíz] | «Greatest decrease in grain yields is caused by water deficits during the flowering period (2)» |
| maíz | pH | óptimo / absoluto | 5 / 4,5 | 7 / 8,5 | pH | — | [ECO-maíz] | fila Soil PH | [U-ECO-maíz] | «Soil PH 5 7 4.5 8.5» |
| maíz | pH | óptimo (tolerado) | 6,3 (4,5) | 7,8 (9,8) | pH | Chile | [CIREN-c] | ficha Maíz, p. 23 | [U-CIREN-c] | «Mínimo tolerado 4,5 · Rango óptimo 6,3-7,8 · Máximo tolerado 9,8» |
| maíz | textura y drenaje | limitante | — | — | — | arcilla muy pesada o arena; anegamiento | [CI-maíz] | web s/p | [U-CI-maíz] | «less so on very heavy dense clay and very sandy soils … well-drained as the crop is susceptible to waterlogging» |
| maíz | anegamiento en floración | crítico de daño | — | −50 % de rendimiento | — | floración | [CI-maíz] | web s/p | [U-CI-maíz] | «Waterlogging during flowering (2) can reduce grain yields by 50 percent or more.» |
| maíz | fertilidad (suma de bases) | óptimo (S1) | >5,0 | — | cmol(+)/kg | Ruanda | [VVR03] | Tabla 4.5, p. 93 | [U-VVR] | «SBC (cmol(+) kg-1) > 5.0 · 5.0 – 3.5 · 3.5 – 2.0» |
| maíz | pendiente | S1 / S2 / S3 | ≤6 / 6–13 / 13–25 | 25–55 / >55 | % | Ruanda | [VVR03] | Tabla 4.5, p. 93 | [U-VVR] | «slope gradient (%) ≤ 6 · 6 – 13 · 13 - 25 · 25 - 55 · > 55» |
| maíz | pendiente | limitación leve / moderada / severa | 2–6 / 6–10 | 11–30 | % | Chile | [CIREN-c] | ficha Maíz, p. 23 | [U-CIREN-c] | «Pendiente: Suave 2-6% · Inclinada 6-10% · Muy inclinada 11-20% · Fuert. inclinada 21-30%» |
| algodón | T media | óptimo / absoluto | 22 / 15 | 36 / 42 | °C | estación de crecimiento | [ECO-alg] | fila Temperat. requir. | [U-ECO-alg] | «Temperat. requir. 22 36 15 42» |
| algodón | T por fase | óptimo | 21–27 (vegetativo) | 27–32 (floración) | °C | cita sec. de Waddle 1984 | [Colombo19] | p. 5 | [U-Colombo] | «la temperatura debe estar comprendida entre 21-27 y 27-32°C durante el periodo de crecimiento vegetativo y de floración» |
| algodón | T en floración y cápsulas | límites | >20 de día y >12 de noche | ≤40 de día y ≤27 de noche; >38 reduce el rendimiento | °C | botones, floración y cápsulas | [CI-alg] | web s/p | [U-CI-alg] | «daytime temperature should be higher than 20°C and night temperature higher than 12°C, but should not exceed 40 and 27°C» |
| algodón | T de suelo | límite inferior | 16 | — | °C | todo el ciclo (150–180 d) | [FAO66] | p. 156 | [U-FAO66] | «Total growing period ranges from 150 to 180 days when soil temperature is >16 °C» |
| algodón | T base para grados-día | base | 12 | — | °C | desarrollo (cita sec. de Constable & Shaw 1988) | [Colombo19]; [VyE40] | p. 2; p. 23 | [U-Colombo]; [U-VyE] | «A temperaturas menores a 12°C (temperatura base) el desarrollo del algodonero se detiene» |
| algodón | T óptima de desarrollo | óptimo | 32 | — | °C | desarrollo | [VyE40] | p. 23 | [U-VyE] | «desde una temperatura base hasta una óptima (32 °C)» |
| algodón | grados-día hasta 60 % de cápsulas abiertas | requerido | 1645 | 1867 | °C·d (base 12) | DP 402 / DP 1238, Reconquista; valores leídos de la figura | [Colombo19] | Fig. 8, p. 23 | [U-Colombo] | «Duración promedio de las fases fenológicas en grados días (GD) para los genotipos de algodón DP 402 y DP 1238» |
| algodón | calor reproductivo | crítico de daño | 33 | — | °C | etapa reproductiva (cita sec. de Reddy et al. 1991) | [Colombo19] | p. 5 | [U-Colombo] | «Los procesos reproductivos son afectados negativamente cuando la temperatura supera los 33°C» |
| algodón | calor en floración | crítico de daño | 40/30 (día/noche) | — | °C | inicio de floración, −50 % de rendimiento (cita sec. de Reddy et al. 1991) | [Colombo19] | p. 5 | [U-Colombo] | «incrementos de la temperatura hasta 40/30°C (día/noche) durante el inicio de la floración … reducen … el rendimiento en un 50%» |
| algodón | calor: umbral de rendimiento | límite superior del óptimo | 32 | — | °C | estación completa | [SR09] | p. 15594 | doi:10.1073/pnas.0906865106 | (ver fila de soja) |
| algodón | helada: temperatura crítica | crítico de daño | sin fuente numérica | — | °C | «very sensitive to frost» ([FAO66] p. 158) | — | — | — | — |
| algodón | período libre de heladas | requerido | 200 | — | días | — | [CI-alg] | web s/p | [U-CI-alg] | «The crop is very sensitive to frost and a minimum of 200 frost-free days is required.» |
| algodón | lluvia | óptimo / absoluto | 750 / 450 | 1200 / 1500 | mm/año | anual | [ECO-alg] | fila Rainfall | [U-ECO-alg] | «Rainfall (annual) 750 1200 450 1500» |
| algodón | lluvia o riego | requerido | 600 | 1200 | mm | ciclo | [FAO66] | p. 154 | [U-FAO66] | «moderate rainfall or irrigation, usually from 600 to 1 200 mm» |
| algodón | ETm | requerido | 700 | 1300 | mm/ciclo | según clima y ciclo | [CI-alg] | web s/p | [U-CI-alg] | «cotton needs some 700 to 1300 mm to meet its water requirements (ETm)» |
| algodón | período crítico hídrico | — | — | — | — | justo antes y durante la formación de botones; pico de demanda en floración (50–60 % del total); meses: sin fuente | [CI-alg] | web s/p | [U-CI-alg] | «Cotton requires adequate water supply particularly just prior and during bud formation (2a).» |
| algodón | pH | óptimo / rango | 7 (rango 5,5) | 8 (rango 8) | pH | — | [CI-alg]; [FAO66] p. 159 | web; p. 159 | [U-CI-alg] | «The pH range is 5.5 to 8 with 7 to 8 regarded as optimum.» |
| algodón | pH | óptimo / absoluto | 6 / 5 | 7,5 / 9,5 | pH | difiere de FAO 33 | [ECO-alg] | fila Soil PH | [U-ECO-alg] | «Soil PH 6 7.5 5 9.5» |
| algodón | textura y drenaje | óptimo | — | — | — | francos a arcillosos, profundos, bien drenados | [FAO66] | p. 156 | [U-FAO66] | «favoured soils are loamy to clayey, deep, well drained and with good waterholding capacity» |
| algodón | anegamiento / napa alta | crítico de daño | — | −50 % de rendimiento | — | suelo saturado por períodos largos | [CI-alg] | web s/p | [U-CI-alg] | «When grown under conditions of high groundwater tables … the yield decrease may be up to 50 percent» |
| algodón | fertilidad | óptimo | moderada | — | clase | — | [ECO-alg] | fila Soil fertility | [U-ECO-alg] | «Soil fertility moderate moderate» |
| algodón | pendiente | — | sin fuente específica | — | % | sólo las clases generales de Sys (fila «todos») | — | — | — | — |
| girasol | T media | óptimo | 18 | 25 | °C | crecimiento | [CI-gir] | web s/p | [U-CI-gir] | «Mean daily temperatures for good growth are between 18 and 25°C.» |
| girasol | T media | óptimo / absoluto | 17 / 5 | 34 / 45 | °C | estación de crecimiento | [ECO-gir] | fila Temperat. requir. | [U-ECO-gir] | «Temperat. requir. \| 17 \| 34 \| 5 \| 45» |
| girasol | T cardinales | base / óptimo / máximo | 4 / 28 | 40 | °C | desarrollo; Tupper de grados-día = 30 | [FAO66] | p. 166 | [U-FAO66] | «basal temperature = 4 °C; optimum temperature = 28 °C, and maximum temperature = 40 °C» |
| girasol | T base | base | 6 | 8 | °C | según autores | [ASAGIR-T] | web | [U-ASAGIR-T] | «La temperatura base de crecimiento es variable según los autores entre 6 y 8o C» |
| girasol | T base y máxima | base / máximo | 10 | 37 | °C | Chile | [CIREN-d] | ficha Maravilla, p. 27 | [U-CIREN-d] | «Temperatura base o mínima de crecimiento: 10 C … Límite máximo de temperatura de crecimiento: 37 C» |
| girasol | suma térmica del ciclo | requerido | 1600 | 1700 | °C·d (base 6–8) | ciclo; floración ~850 | [ASAGIR-T] | web | [U-ASAGIR-T] | «variando las sumas de temperaturas entre 1600 y 1700o C. La floración se produce con una suma térmica aproximada de 850o C» |
| girasol | suma térmica | requerido | 850 | 1200 | °C·d (T > 10) | siembra a cosecha, Chile | [CIREN-d] | ficha Maravilla, p. 27 | [U-CIREN-d] | «Suma térmica T> 10 entre siembra y cosecha: 850 a 1200 días-grados» |
| girasol | calor: aceite | crítico de daño | 35 | — | °C | llenado | [ASAGIR-T] | web | [U-ASAGIR-T] | «Temperaturas superiores a los 35o C reducen el % de materia grasa en el aquenio» |
| girasol | calor: embrión | crítico de daño | 34 | — | °C | crecimiento del embrión (cita sec. de Rondanini et al. 2003) | [Colombo19] | p. 4 | [U-Colombo] | «temperaturas superiores a 34°C durante la etapa de crecimiento del embrión de girasol acortaron la duración de esta etapa» |
| girasol | T media en llenado | óptimo | 12 | 22 | °C | peso máximo de grano | [DZ03] | p. 5 | [U-DZ03] | «Los máximos pesos se logran en un rango de temperaturas medias diarias relativamente bajas (12 a 22oC).» |
| girasol | helada en cotiledón | crítico de daño (planta) | −5 | — | °C | cotiledones (sobrevivió) | [ASAGIR-T] | web | [U-ASAGIR-T] | «plántulas en estado de cotiledón han sobrevivido a - 5o C» |
| girasol | helada en 6–8 hojas o botón | crítico de daño (planta) | 0 | — | °C | 6–8 hojas o botón floral | [ASAGIR-T] | web | [U-ASAGIR-T] | «Esta resistencia declina hasta el estado de 6 - 8 hojas y botón floral expuesto a 0o C, o menor» |
| girasol | helada | crítico de daño | −1 | — | °C | 6–8 hojas verdaderas, Chile | [CIREN-d] | ficha Maravilla, p. 27 | [U-CIREN-d] | «Temperatura crítica o de daño por heladas: -1°C» |
| girasol | helada letal | crítico de daño | 0 (crecimiento temprano) | −10 (en reposo) | °C | — | [ECO-gir] | fila Killing temp. | [U-ECO-gir] | «Killing temp. during rest \| -10 \| Killing temp. early growth \| 0» |
| girasol | ventana de siembra por heladas y calor | requerido | desde inicio de agosto | hasta el 10 de septiembre | fecha | Chaco; última helada probable en Las Breñas: 3 de septiembre | [INTA-LB-gir] | web | [U-LB-gir] | «no se recomienda iniciar la siembra antes de inicio del mes de agosto» / «no extender la fecha de siembra más allá del 10 de septiembre» |
| girasol | lluvia | óptimo / absoluto | 600 / 300 | 1000 / 1600 | mm/año | anual | [ECO-gir] | fila Rainfall | [U-ECO-gir] | «Rainfall (annual) \| 600 \| 1000 \| 300 \| 1600» |
| girasol | necesidad hídrica | requerido | 600 | 1000 | mm/ciclo | según clima y ciclo | [CI-gir] | web s/p | [U-CI-gir] | «The water requirements of sunflower vary from 600 to 1000 mm» |
| girasol | ET del ciclo | típico | 500 | 650 | mm | ciclo corto <450; ciclo largo >800 | [FAO66] | p. 167 | [U-FAO66] | «Typical values between 500 and 650 mm are normally found.» |
| girasol | agua útil del ciclo | requerido | 360 | — | mm | Chaco | [INTA-LB-gir] | web | [U-LB-gir] | «el cultivo necesitaría disponer de 360 milímetros de agua útil durante su ciclo» |
| girasol | déficit hídrico | déficit | 120 | — | mm | Chaco (se cubre con barbecho) | [INTA-LB-gir] | web | [U-LB-gir] | «la provincia presenta un déficit de 120 milímetros» |
| girasol | período crítico | — | 30 días antes | 20 días después | días respecto de floración | meses: sin fuente | [DZ03] | p. 5 | [U-DZ03] | «se extiende desde unos 30 días antes hasta unos 20 días después de la floración» |
| girasol | pH | óptimo | 6,0 | 7,5 | pH | — | [CI-gir] | web s/p | [U-CI-gir] | «Optimum soil pH is in the range of 6.0 to 7.5» |
| girasol | pH | óptimo / absoluto | 6 / 5,5 | 7,5 / 8 | pH | — | [ECO-gir] | fila Soil PH | [U-ECO-gir] | «Soil PH \| 6 \| 7.5 \| 5.5 \| 8» |
| girasol | pH | óptimo (tolerado) | 6,0 (4,5) | 7,8 (10,0) | pH | Chile | [CIREN-d] | ficha Maravilla, p. 27 | [U-CIREN-d] | «Mínimo tolerado 4,5 · Rango óptimo 6,0-7,8 · Máximo tolerado 10,0» |
| girasol | profundidad / textura | limitante | 40 | — | cm (mínimo) | — | [DZ03]; [ECO-gir] | p. 5; fila Soil texture | [U-DZ03] | «No es recomendable su cultivo en suelos con espesores inferiores a los 40 cm.» (ECOCROP óptimo: «medium, light») |
| girasol | drenaje | óptimo y absoluto | — | — | — | bien drenado | [ECO-gir] | fila Soil drainage | [U-ECO-gir] | «Soil drainage \| well (dry spells) \| well (dry spells)» |
| girasol | fertilidad | óptimo | alta | — | clase | — | [ECO-gir] | fila Soil fertility | [U-ECO-gir] | «Soil fertility \| high \| low» |
| girasol | pendiente | limitación leve / moderada / severa | 2–6 / 6–10 | 11–30 | % | Chile | [CIREN-d] | ficha Maravilla, p. 27 | [U-CIREN-d] | «Pendiente: Suave 2-6% · Inclinada 6-10% · Muy inclinada 11-20% · Fuert. inclinada 21-30%» |

**Referencias (claves de la tabla)**
- **[FAO66]** Steduto, P., Hsiao, T.C., Fereres, E. & Raes, D. (2012). *Crop yield response to water*. FAO Irrigation and Drainage Paper 66. Roma: FAO. Capítulos Maize (pp. 114–), Soybean (124–), Cotton (154–), Sunflower (164–).
  - [U-FAO66] https://www.fao.org/3/i2800e/i2800e.pdf
- **[CI-maíz] / [CI-soja] / [CI-alg] / [CI-gir]** FAO Land & Water Division (s/f). *Crop information: Maize / Soybean / Cotton / Sunflower*. FAO, Roma; texto de Doorenbos & Kassam (1979), FAO I&D Paper 33.
  - [U-CI-maíz] http://web.archive.org/web/20221210081723/https://www.fao.org/land-water/databases-and-software/crop-information/maize/en/
  - [U-CI-soja] …/20221210081713/…/soybean/en/
  - [U-CI-gir] …/20221210081719/…/sunflower/en/
  - [U-CI-alg] http://web.archive.org/web/20221215104900/https://www.fao.org/land-water/databases-and-software/crop-information/cotton/fr/ (texto en inglés)
- **[ECO-*]** FAO (s/f). *ECOCROP – Crop ecological requirements database*, fichas de cada especie. Consultado el 1-nov-2026.
  - [U-ECO-soja] https://ecocrop.apps.fao.org/ecocrop/srv/en/dataSheet?id=1150
  - [U-ECO-maíz] …id=2175
  - [U-ECO-alg] …id=1160 (*G. hirsutum*)
  - [U-ECO-gir] …id=1191
- **[SR09]** Schlenker, W. & Roberts, M.J. (2009). Nonlinear temperature effects indicate severe damages to U.S. crop yields under climate change. *PNAS* 106(37): 15594–15598.
- **[Colombo19]** Colombo, F.V. (2019). *Rendimiento y calidad de fibra de genotipos de algodón (Gossypium hirsutum L.) en respuesta a la nutrición nitrogenada y altas temperaturas*. Tesis de Maestría, FAV-UNRC (dir. M. Paytas). Repositorio INTA, EEA Reconquista.
  - [U-Colombo] https://repositorio.inta.gob.ar/xmlui/bitstream/handle/20.500.12123/8268/INTA_CRSantaFe_EEAReconquista_Colombo_FV_Rendimiento_y_calidad_de_fibra_de_genotipos_de_algodon.pdf
- **[VyE40]** Winkler, H., Scarpin, G., Dileo, P. & Paytas, M. (s/f; ensayo de la campaña 2017/18). Fecha de siembra óptima en variedades comerciales de algodón. *Voces y Ecos* Nº 40: 22–27. INTA EEA Reconquista.
  - [U-VyE] https://repositorio.inta.gob.ar/bitstream/handle/20.500.12123/4953/INTA_CRSantaFe_EEAReconquista_revista_voces_y_ecos_nro_40_p.22-27.pdf
- **[INTA-LB-soja]** INTA (29-sep-2026). Soja de primavera: claves para definir la siembra en el Chaco. Declaraciones de J. Luzzi, INTA Las Breñas.
  - [U-LB-soja] https://www.argentina.gob.ar/noticias/soja-de-primavera-claves-para-definir-la-siembra-en-el-chaco
- **[INTA-LB-gir]** INTA Informa (08-jul-2025). Girasol: pautas para una campaña con alto potencial. Declaraciones de G. Quintana, INTA Las Breñas.
  - [U-LB-gir] https://intainforma.inta.gob.ar/girasol-pautas-para-una-campana-con-alto-potencial/
- **[INTA-IIACS]** INTA Informa, reproducido en Agrofy News (16-ago-2018). Buscan un maíz que tolere los golpes de calor. Declaraciones de J. Parrado, IIACS-INTA Leales.
  - [U-IIACS] https://news.agrofy.com.ar/noticia/176766/buscan-maiz-que-tolere-golpes-calor
- **[INTA-Bautista]** INTA (s/f). Maíz: Bautista INTA.
  - [U-Bautista] https://www.argentina.gob.ar/inta/relaciones-estrategicas-del-inta/maiz-bautista-inta
- **[DZ03]** Díaz-Zorita, M., Duarte, G.A. & Plante, E. (2003). *El cultivo de girasol*. ASAGIR, diciembre de 2003.
  - [U-DZ03] https://www.asagir.org.ar/descargar-archivo-45/archivos-01e3a1a53c235fd1484651587412a506
- **[ASAGIR-T]** ASAGIR (s/f). El factor temperatura. Sección «El cultivo».
  - [U-ASAGIR-T] https://www.asagir.org.ar/acerca-de-el-factor-temperatura-478
- **[NDSU]** Kandel, H. (10-sep-2020). Evaluating frost damage in soybean. NDSU Crop & Pest Report.
  - [U-NDSU] https://www.ndsu.edu/agriculture/ag-hub/ag-topics/crop-production/crops/soybeans/evaluating-frost-damage-soybean
- **[ISU]** Licht, M. (6-may-2020). Cold temperatures on the way; what will it mean for corn and soybean seedlings? Iowa State University, ICM.
  - [U-ISU] https://crops.extension.iastate.edu/post/cold-temperatures-way-what-will-it-mean-corn-and-soybean-seedlings
- **[VVR03]** Verdoodt, A. & Van Ranst, E. (2003). *Land evaluation for agricultural production in the tropics: a large-scale land suitability classification for Rwanda*. Ghent University. Adapta Sys et al. (1993), *Land Evaluation Part III: Crop requirements*.
  - [U-VVR] https://edepot.wur.nl/484602
- **[CIREN-c] / [CIREN-d]** CIREN (1989). *Requerimientos de clima y suelo: chacras y hortalizas* / *…: cereales, cultivos industriales y flores*. Santiago de Chile: CIREN. Fichas Maíz (folio 23, PDF p. 21) y Maravilla (folio 27, PDF p. 26).
  - [U-CIREN-c] https://research.csiro.au/gestionrapel/wp-content/uploads/sites/79/2016/11/Requerimientos-de-clima-y-suelo.-Chacras-y-horatalizas-1989c.pdf
  - [U-CIREN-d] …/Requerimientos-de-clima-y-suelo.-Cereales-cultivos-industriales-y-flores-1989d.pdf

**Advertencias sobre la verificación**
- **Citas extraídas con WebFetch:** las de [INTA-LB-soja], [INTA-LB-gir], [INTA-IIACS], [NDSU] e [ISU] salen de un extractor que resume la página; el resto las saqué del texto completo. Conviene cotejar esas cinco antes de citarlas en un texto.
- **CIREN:** la helada de maíz figura en el OCR como «1C», ilegible, y la dejé afuera.
- **Desacuerdos entre fuentes, puestas lado a lado en la tabla:**
  - pH del algodón: óptimo 7–8 según FAO 33 y FAO 66, 6–7,5 según ECOCROP.
  - Temperatura base del girasol: 4 °C (FAO 66), 6–8 °C (ASAGIR), 10 °C (CIREN).
  - Temperatura base del maíz: 8 °C (FAO 66), 10–12 °C (CIREN).

**Vacíos más importantes**
1. Ninguna fuente INTA del NEA verificada da umbrales para maíz (agua, helada, grados-día con base) ni para soja (temperatura base, grados-día). El repositorio INTA cortó la conexión a mitad de la corrida (ECONNRESET) y el de la FAUBA (Kantolic 2008, Molino 2011) tiene el certificado vencido; ninguno de esos textos se leyó. No encontré nada de Bolsa de Cereales ni de MAGyP.
2. «Sin fuente» en temperatura base y grados-día de soja, temperatura crítica de helada del algodón, y período libre de heladas de soja, maíz y girasol (sólo hay la duración del ciclo como proxy).
3. Pendiente: no hay nada argentino. Sólo Sys/Verdoodt (Ruanda) y CIREN (Chile), y nada específico para algodón.
4. Faltan el efecto por día > 35 °C (sólo hay umbrales, no tasas por día) y el déficit P − ETP salvo para girasol en Chaco (120 mm). Los períodos críticos están por etapa, no por mes del NEA.
5. No pude leer el texto original de FAO 33 (Doorenbos & Kassam 1979); usé las fichas web de FAO derivadas de él, sin paginar. El PDF de FAO 56 tampoco se consultó. Del CSV o `.rds` de ECOCROP, abrirlo con R fue denegado y usé las fichas web.


## Agregado del 1-nov-2026: pH de la soja, fuentes argentinas (búsqueda dirigida)
| cultivo | variable | tipo | valor_min | valor_max | unidad | etapa o condición | cita completa | página/tabla | URL/DOI | cita textual |
|---|---|---|---|---|---|---|---|---|---|---|
| soja | pH | óptimo | 6,0 | 7,0 | pH | orientativo; toma los valores de Porta et al. 1999 | Vázquez, M.; Pagani, A. 2018. UD C5 (1 de 2): Manejo de suelos ácidos. FCAyF-UNLP | Tabla 2, p. 5 | https://aulavirtual.agro.unlp.edu.ar/pluginfile.php/75162/mod_resource/content/1/Tirada%20Interna%20UDC%20C5%20%281%20de%202%29%20Suelos%20%C3%81cidos.pdf | «Soja (Glycine max) 6,0-7,0 4,5-7,5» (verificado sobre la imagen de la página) |
| soja | pH | tolerancia para rendimiento satisfactorio | 4,5 | 7,5 | pH | ídem | ídem | Tabla 2, p. 5 | ídem | ídem |
| soja | pH | óptimo / tolerancia | 6 (4,5) | 7 (7) | pH en agua 1:2,5 | Buenos Aires y Santa Fe; cita a Vázquez 2007 | Vázquez, M. et al. 2012. Respuesta de la soja a enmiendas básicas en suelos de Buenos Aires y Santa Fe. Ciencia del Suelo 30(1): 43–55 | pp. 48–49 | https://www.scielo.org.ar/pdf/cds/v30n1/v30n1a05.pdf | «estaría dentro del rango de tolerancia (4,5-7) para la especie, se encontraría por debajo del rango óptimo de 6-7 (Vázquez, 2007)» |
| cultivos de la región | pH | óptimo | 6 | 7,5 | pH en agua | pampeana y extrapampeana; no específico de soja | Sainz Rozas, H.; Echeverría, H.; Angelini, H. 2011. Ciencia del Suelo 29(1): 29–37 | p. 34 | https://www.scielo.org.ar/pdf/cds/v29n1/v29n1a04.pdf | «6 a 7,5 […] considerados óptimos para el crecimiento de los principales cultivos de la región» |
| soja | pH | óptimo | 5,5 | 6,0 | pH en agua | Brasil, RS y SC | Oliveira Junior, A. et al. 2020. Tecnologias de produção de soja, cap. 7. Embrapa Soja | p. 142 | https://www.alice.cnptia.embrapa.br/alice/bitstream/doc/1128401/1/cap-7-SP-17-2020.pdf | «o pH em água adequado para a cultura da soja situa-se entre 5,5 e 6,0» |
| maíz | pH | óptimo / tolerancia | 5,5 (5,0) | 7,0 (8,0) | pH | orientativo | Vázquez & Pagani 2018 (UNLP), de Porta et al. 1999 | Tabla 2, p. 5 | ídem | «Maiz (Zea mays) 5,5-7,0 5,0-8,0» (verificado sobre la imagen) |
| algodón | pH | óptimo / tolerancia | 5,2 (4,8) | 6,0 (7,5) | pH | orientativo | ídem | Tabla 2, p. 5 | ídem | «Algodón (Gossypium hirsutum) 5,2-6,0 4,8-7,5» (verificado sobre la imagen) |
| girasol | pH | tolerancia | 6,0 | 7,5 | pH | sin óptimo en la tabla | ídem | Tabla 2, p. 5 | ídem | «Girasol (Helianthus annus) [óptimo vacío] 6,0-7,5» (verificado sobre la imagen) |
| arroz | pH | óptimo / tolerancia | 5,0 (4,0) | 7,0 (8,0) | pH | orientativo | ídem | Tabla 2, p. 5 | ídem | «Arroz (Oryza sativa) 5,0-7,0 4,0-8,0» (verificado sobre la imagen) |
Sin fuente del NEA (INTA Sáenz Peña, Las Breñas, UNNE): no se encontró valor de pH para soja.
