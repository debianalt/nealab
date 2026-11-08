# Contraste de la prensa con los escenarios de agua (Misiones, municipio × día)

Generado por `pipeline/noticias/contraste.py`. Período 2012-01-01 → 2026-11-05; 78 municipios; 422,994 municipio-días; 1,029 con corte por agua en la prensa. Prensa: noticias v3.2 (reglas, sin filtro de Haiku salvo en P5).

## P1. Dosis-respuesta: lluvia de 3 días contra cortes por agua en la prensa (días sin río alto)

Regla: P(corte_agua) crece en cada clase salvo una, y la clase más alta es ≥ 5 veces la más baja.

- 2012–2026: sube en 6 de 6 pasos; más alta / más baja = 24 veces → **PASA**
- 2012–2019: sube en 6 de 6 pasos; más alta / más baja = 16 veces → **PASA**
- 2020–2026: sube en 6 de 6 pasos; más alta / más baja = 33 veces → **PASA**

| período | mm en 3 días | municipio-días | corte_agua % | crecida % | corte sin motivo % |
|---|---|---|---|---|---|
| 2012–2026 | < 5 | 218,285 | 0.13% | 0.39% | 0.35% |
| 2012–2026 | 5–20 | 87,601 | 0.16% | 0.54% | 0.29% |
| 2012–2026 | 20–40 | 55,669 | 0.20% | 0.78% | 0.39% |
| 2012–2026 | 40–60 | 27,385 | 0.37% | 1.19% | 0.40% |
| 2012–2026 | 60–100 | 22,721 | 0.59% | 1.74% | 0.54% |
| 2012–2026 | 100–150 | 7,313 | 1.23% | 4.29% | 0.59% |
| 2012–2026 | ≥ 150 | 2,446 | 3.19% | 8.50% | 1.47% |
| 2012–2019 | < 5 | 114,412 | 0.11% | 0.32% | 0.28% |
| 2012–2019 | 5–20 | 47,230 | 0.11% | 0.40% | 0.23% |
| 2012–2019 | 20–40 | 30,586 | 0.15% | 0.45% | 0.28% |
| 2012–2019 | 40–60 | 15,783 | 0.23% | 0.82% | 0.27% |
| 2012–2019 | 60–100 | 12,681 | 0.37% | 1.05% | 0.43% |
| 2012–2019 | 100–150 | 4,871 | 0.55% | 2.79% | 0.29% |
| 2012–2019 | ≥ 150 | 1,384 | 1.73% | 5.64% | 0.36% |
| 2020–2026 | < 5 | 103,873 | 0.16% | 0.47% | 0.42% |
| 2020–2026 | 5–20 | 40,371 | 0.21% | 0.72% | 0.37% |
| 2020–2026 | 20–40 | 25,083 | 0.26% | 1.18% | 0.51% |
| 2020–2026 | 40–60 | 11,602 | 0.55% | 1.70% | 0.59% |
| 2020–2026 | 60–100 | 10,040 | 0.86% | 2.62% | 0.68% |
| 2020–2026 | 100–150 | 2,442 | 2.58% | 7.29% | 1.19% |
| 2020–2026 | ≥ 150 | 1,062 | 5.08% | 12.24% | 2.92% |

## P2. Río Uruguay: puentes de la RP2 cortados según el modelo, por municipio-día

Municipios con puentes RP2 evaluados: 12; municipio-días con corte modelado: 74 de 65,076.

P(corte_agua en la prensa | modelo cortado) = 28.4%; P(… | no cortado) = 0.39%; razón 73 veces (regla ≥ 5) → **PASA**.

## P3. ¿De dónde viene cada día de corte en la prensa?

Lluvia fuerte = mm3 ≥ 60 o píxel ≥ 60 mm en el día (fijado antes de mirar). Río = alguna escala a ≤ 10 km sobre su alerta, o un puente de la RP2 cortado según el modelo.

| señal ese día | días con corte_agua | % de los cortes | municipio-días con esa señal | P(corte_agua | señal) |
|---|---|---|---|---|
| río | 52 | 5% | 1,183 | 4.40% |
| lluvia | 308 | 30% | 33,346 | 0.92% |
| ambas | 41 | 4% | 391 | 10.49% |
| ninguna | 628 | 61% | 388,074 | 0.16% |

Con el río bajo, 308 municipio-días de corte coinciden con lluvia fuerte: son los cortes por arroyos que el modelo del río no ve.

Diagnóstico (no es regla): de los 628 días sin señal, 32% tuvo lluvia fuerte en los 7 días previos y 4% río alto; sin ninguna de las dos en la semana: 68%.

## P4. Corte transversal: exposición del modelo del día de lluvia contra la prensa

Municipios con ≥ 30 días de lluvia fuerte sin río alto: 78 (de 78 con exposición calculada).

- fracción sin ruta con lluvia fuerte: Spearman 0.02 (p = 0.859, n = 76) → **NO PASA** (regla ≥ 0,3)
- km de tierra medios: Spearman -0.23 (p = 0.047, n = 78) → **NO PASA** (regla ≥ 0,3)
- personas sin ruta: Spearman 0.37 (p = 0.001, n = 76) → **PASA** (regla ≥ 0,3)

Post hoc (fuera de la regla): con la razón P(corte | lluvia fuerte) / P(corte | seco) del mismo municipio, que saca la cobertura de prensa:

- fracción sin ruta con lluvia fuerte: Spearman -0.03 (p = 0.852, n = 58)
- km de tierra medios: Spearman 0.06 (p = 0.645, n = 60)

Municipios con más prensa de corte por día de lluvia fuerte:

| municipio | días lluvia fuerte | P(corte_agua) | sin ruta (modelo) | fracción sin ruta | km tierra |
|---|---|---|---|---|---|
| Municipio de Oberá | 432 | 5.3% | 3,985 | 5% | 0.2 |
| Municipio de El Soberbio | 415 | 3.9% | 14,230 | 55% | 0.7 |
| Municipio de Alba Posse | 418 | 3.6% | 1,870 | 22% | 0.6 |
| Municipio de Jardín América | 419 | 3.6% | 165 | 1% | 0.5 |
| Municipio de San Vicente | 422 | 2.8% | 12,023 | 26% | 0.7 |
| Municipio de Santa María | 487 | 2.5% | 451 | 25% | 3.0 |
| Municipio de Ruiz de Montoya | 410 | 2.4% | 204 | 5% | 0.7 |
| Municipio de Eldorado | 374 | 2.4% | 1,549 | 2% | 0.3 |
| Municipio de Campo Ramón | 431 | 2.3% | 3,920 | 39% | 0.5 |
| Municipio de Apóstoles | 481 | 2.3% | 21 | 0% | 0.2 |
| Municipio de Montecarlo | 414 | 2.2% | 937 | 4% | 1.0 |
| Municipio de San Ignacio | 416 | 2.2% | 141 | 1% | 1.6 |

## P5. Días de corte sin ninguna señal: ¿ruido o hechos que el modelo no ve? (haiku, v2-2026-11-08)

Municipio-días sin señal: 628 (1,296 oraciones); muestra de días con señal: 401 (1,292 oraciones, semilla 21).

- Celdas sostenidas por alguna oración (hecho reciente, en Misiones y en el municipio): sin señal **36%**, con señal **68%**.
- Oraciones dadas por hecho: sin señal 80%, con señal 89%.
- Diferencia 32 puntos (regla: < 15 → los días sin señal son sobre todo hechos que el modelo no ve) → **más ruido sin señal**.

Días sin señal confirmados: lluvia de 3 días mediana 12 mm, píxel máximo mediano 0 mm (IMERG subestima aguaceros locales: 75,6 contra 190 mm en El Soberbio, 21-jul-2026).

Municipios con más días sin señal confirmados:

| municipio | días |
|---|---|
| Municipio de El Soberbio | 27 |
| Municipio de Alba Posse | 20 |
| Municipio de San Pedro | 17 |
| Municipio de Jardín América | 16 |
| Municipio de Oberá | 12 |
| Municipio de Eldorado | 10 |
| Municipio de San Vicente | 9 |
| Municipio de Montecarlo | 7 |
| Municipio de Caraguatay | 6 |
| Municipio de Mártires | 6 |

## P6. 2021: cortes de la RP2 en la prensa, uno por uno

10 eventos; con el río bajo: 10.

| día | municipio | lugar | mm 3 días | píxel máx | río alto | oración |
|---|---|---|---|---|---|---|
| 17-01 | Municipio de San Pedro | Fortaleza × RP 21 | 74.0 | 86.0 | no | La intensidad de la lluvia causó anegamiento en varias calles y rotura de un puente, siendo uno de éstos el que se ubica sobre ruta provinci |
| 09-03 | Municipio de El Soberbio | Soberbio × RP 2 | 0.0 | 0.0 | no | Desde las 6.30 se lleva adelante nuevamente el corte en la ruta Costera 2 a la altura del arroyo el Soberbio. |
| 04-05 | Municipio de Alba Posse | Pindaití × RP 2 | 5.0 | 7.0 | no | Hoy arrancarán las evaluaciones del estado del puente sobre el arroyo Pindaytí de la ruta provincial 2, cerca de Santa Rita , donde el tráns |
| 11-10 | Municipio de Alba Posse | Pindaití × RP 2 | 32.0 | 22.0 | no | La Dirección Provincial de Vialidad (DPV) informó que tras la suba del cauce del arroyo Pindaytí se determinó el corte total del paso vehicu |
| 11-10 | Municipio de Veinticinco de Mayo | 25 de Mayo | 56.0 | 43.0 | no | Sin embargo, se comunicó que los puentes en los accesos a las picadas Amanecer y Libertad, se encuentran cortados debido al sobrepaso del ca |
| 11-10 | Municipio de Veinticinco de Mayo | 25 de Mayo | 56.0 | 43.0 | no | Tránsito bloqueado en la Ruta Costera entre 25 De Mayo y Alba Posse por crecida del arroyo Pindayti. |
| 18-10 | Municipio de Alba Posse | Pindaití × RP 2 | 0.0 | 0.0 | no | El puente provisorio sobre el arroyo Pindaytí, en el trazado de la ruta provincial 2 (costera), que hace ya una semana permanece cerrado al  |
| 26-10 | Municipio de Veinticinco de Mayo | 25 de Mayo | 0.0 | 0.0 | no | El paso provisorio, construido luego de que se determinara el corte total del paso vehicular y peatonal sobre el antiguo puente en Ruta Prov |
| 30-10 | Municipio de Garupá | Garupá | 9.0 | 5.0 | no | El paso provisorio, construido luego de que se determinara el corte total del paso vehicular y peatonal sobre el antiguo puente en RP 2, se  |
| 26-12 | Municipio de Alba Posse | Pindaití × RP 2 | 0.0 | 0.0 | no | Este domingo a la siesta, se desplomó buena parte del viejo puente sobre el arroyo Pindaytí, en la ruta costera 2 , que desde hace meses ya  |
