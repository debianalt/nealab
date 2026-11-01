# A4 · Umbrales por cultivo — PROPUESTA para validar

Estado: **propuesto (1-nov-2026), sin validar por el autor.** No se calcula ninguna aptitud hasta que esta tabla
esté validada. Fuente de verdad: este archivo; se carga en `meta.umbrales_cultivo` (propuesta) y
`meta.umbrales_evidencia` (todas las filas con cita) con `build_hexoma_umbrales.py`.

## Cómo se armó
- Tres búsquedas en paralelo (perennes; extensivos; arroz y pasturas), con fuentes en este orden: INTA, INYM,
  manuales argentinos, papers regionales (sur de Brasil, Paraguay, Uruguay), FAO 66 / FAO 33 y FAO ECOCROP como
  respaldo uniforme. Cada valor trae cita completa, página o tabla, URL y cita textual: la evidencia completa está en
  `umbrales_evidencia_*.md`.
- **Verificación propia (1-nov-2026):** las citas de heladas, frío, calor y lluvia se buscaron en los textos bajados
  (PDF → texto): 40 de 42 aparecen literales; las 2 restantes son páginas web que no se guardaron (pH del té del TRI
  de Sri Lanka; heladas del té de Wang et al. 2021), marcadas «no verificada localmente». Los parámetros de ECOCROP
  se verificaron contra el CSV de la base (13 especies, todos iguales a lo reportado).
- **Abrigo frente a planta:** casi todos los umbrales de helada de las fuentes son temperatura de la planta o del
  aire junto al cultivo, no de abrigo. La hoja expuesta queda 3–5 °C por debajo del abrigo (Oliveira et al. 2012,
  Embrapa Doc. 346, p. 11 y 20; Wang et al. 2021: 4–8 °C). Por eso la propuesta usa la **helada a 5 cm**
  (`dias_helada_suelo`, corregida con las convencionales del INTA) para los umbrales de planta cercanos a 0 °C, y la
  mínima de abrigo corregida (`tmin_abs`) para los umbrales de daño severo.
- **Clases:** óptimo = S1; entre óptimo y límite = S2; fuera del límite = N. La combinación entre factores (factor más
  limitante, por ejemplo) se decide en A4 paso 2, no acá.

## Variables del Hexoma que se usan
| Variable | Tabla | Nota |
|---|---|---|
| Lluvia de la campaña, mm | `clima.chirps_campania.p_mm` | CHIRPS +10 % frente a pluviómetros |
| Balance P − ET0 de la campaña o de DEF, mm | `clima.balance_*` | |
| Media de máximas y mínimas, °C (campaña o estación) | `clima.era5_campania_corr`, `era5_estacion_corr` | corregidas con estaciones |
| Grados-día base 10, °C·d | `clima.era5_campania_corr.gdd10` | las fuentes usan bases 0, 4, 6–8, 8, 12: hay que recalcular con la base de cada cultivo desde el diario |
| Días > 35 °C | `clima.era5_campania_corr.dias_tmax35` | |
| Heladas en abrigo y a 5 cm, mínima absoluta, primera y última | `clima.era5_helada_corr` | período libre de heladas ≈ 365 − (última − primera) |
| pH, arcilla, arena, CIC | `suelo.h3` | SoilGrids 0–30 cm (modelo) |
| Pendiente, % = 100·tan(grados) | `suelo.h3.pendiente_media_grados` | FABDEM |
| Anegamiento (proxy) | `uso.h3_anio` (pastizal y bosque inundables) | no hay dato de drenaje de suelo |

## Propuesta por cultivo
Claves de fuente: ver `umbrales_evidencia_*.md` (referencias completas). [E] = FAO ECOCROP.

### Yerba mate
| Factor | S1 (óptimo) | Límite (N fuera) | Fuente | Nota |
|---|---|---|---|---|
| Lluvia anual | 1.500–2.500 mm | ≥ 1.200 mm | Zelada & González 2019 p. 18; Oliveira & Rotta 1985 p. 26; ICRAF (Orwa et al. 2009) | Misiones no supera 2.500: sin techo |
| T media anual | 20–23 °C | 15,5–25,5 °C | Oliveira & Rotta 1985 p. 26; Zelada & González 2019 p. 18 | |
| Helada severa (abrigo) | mínima absoluta > −6 °C | ≥ −11,6 °C (ocurrencia en el área natural) | [E] KTMPR −6; ICRAF; Carvalho 2003 p. 459 | la planta establecida tolera −6 |
| Helada en brotación (5 cm) | sin heladas a 5 cm después del 1-sep | — | [E] KTMP −1 (brotación) | requiere la fecha de la última helada a 5 cm (se calcula en A4) |
| pH | 4,5–5,5 | ≥ 4,0 | Zelada & González 2019 p. 33; Embrapa 2014 | **ECOCROP (6,5–7) e ICRAF (5,8–6,8) contradicen a todas las fuentes regionales: se descartan** |
| Arcilla | > 35 % | ≥ 15 % | Oliveira & Rotta 1985 p. 28; Carvalho 2003 | |
| Anegamiento | sin suelos hidromórficos | — | Oliveira & Rotta 1985 p. 28; Embrapa 2014 | proxy MapBiomas |
| Pendiente | — | — | Burtnik 2006 p. 11 da 3–4 % sólo para lomas del NE de Corrientes | **a decidir**: ¿sin límite de pendiente, o las clases de Sys? |

### Té
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| Lluvia anual | 1.400–2.200 mm | ≥ 1.200 mm | [E] 1.400–2.000; Prat Kricun et al. 2020 p. 24 (zona: 1.800–2.200); Mwesige et al. 2026 (mín. 1.000–1.200) | |
| T de crecimiento (media) | 18–25 °C | 13–30 °C | Mwesige et al. 2026; Crane & Balerdi (EDIS) p. 1 | |
| Helada en brotación (5 cm) | sin heladas a 5 cm después del 1-sep | — | Hu et al. 2013 p. 27 (−2 a −3 °C en brotación, planta) | Wang et al. 2021 (umbral de abrigo 4 °C): no verificada localmente |
| Helada severa (abrigo) | mínima absoluta > −5 °C | — | [E] KTMPR −5; Hu et al. 2013 | |
| pH | 4,5–5,5 | 4,0–6,0 | [E]; Parra (MAGyP, Alimentos Argentinos 34) | |
| Pendiente | 8,7–17,6 % (5–10°) | ≤ 47–58 % (25–30°) | Mwesige et al. 2026 | la fuente convierte mal a %: 5–10° = 8,7–17,6 % |
| Anegamiento | no tolera | — | Crane & Balerdi (EDIS) p. 2 | proxy MapBiomas |

### Cítricos (naranja y mandarina)
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| Lluvia anual | 1.200–2.000 mm (bien distribuida, ~100 mm/mes) | ≥ 450 mm (con riego complementario) | INTA EEA Concordia cap. IV p. 2; [E] naranja | |
| T de crecimiento | 23–34 °C | 12,8–37 °C | INTA Concordia cap. IV p. 1; Oliveira et al. 2012 p. 10 | |
| Helada en flor y fruto chico (5 cm) | sin heladas a 5 cm en floración (sep–oct) | — | INTA Concordia cap. IV p. 1 (−1 °C) | |
| Helada severa (abrigo) | mínima absoluta > −3 °C | > −8 °C (letal foliar) | INTA Concordia (−3 °C fruto desarrollado); Oliveira et al. 2012 p. 12 (−2,5 a −8 °C) | mandarina más tolerante que naranja |
| pH | 5,5–6,5 | 4,0–8,3 | Veloso et al. 2000 (Pará); [E] naranja | no hay pH de INTA Bella Vista ni Concordia |
| Textura | areno-arcillosos a arcillo-arenosos, profundos | — | INTA Concordia cap. IV p. 8 | sin umbral numérico: **a decidir** si se traduce a % de arcilla |
| Pendiente | ≤ 5 % | — | Veloso et al. 2000 (> 5 %: curvas de nivel) | |

### Soja (secano)
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| T media del ciclo (DEF) | 20–30 °C | 10–38 °C | FAO 66 p. 129; [E] | |
| Calor en floración | — | — | FAO 66 p. 129 (aborto a 35 °C) | ninguna fuente da cuántos días > 35 °C toleran: **se informa el conteo, sin umbral** |
| Agua del ciclo | lluvia de la campaña 600–1.500 mm | ≥ 450 mm | [E]; FAO CI (ETm 450–700); INTA Las Breñas (650 mm regional) | |
| Heladas | período libre ≥ 130 días | ≥ 100 días | FAO CI (ciclo 100–130 d o más) | proxy: duración del ciclo; no hay período libre explícito |
| pH | 6,0–6,5 | 4,5–8,4 | FAO CI; [E] | |
| Arcilla | 20–60 % | — | Verdoodt & Van Ranst 2003 (Sys) | excluye suelos muy arenosos (FAO CI) |
| Anegamiento | < 7 días | — | FAO 66 p. 129 | proxy MapBiomas |
| Pendiente | ≤ 6 % | ≤ 25 % (S3) | Verdoodt & Van Ranst 2003, Tabla 4.12 | Ruanda, adaptado de Sys et al. 1993 |

### Maíz (secano)
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| T media | 18–33 °C | > 15 °C sin heladas | [E]; FAO CI | |
| Grados-día | Tbase 8 °C, Tupper 30 °C | — | FAO 66 p. 119 | suma requerida: 2.500–3.000 (base 0, FAO CI); Bautista INTA 2.045 a madurez (base no indicada) |
| Calor en floración | — | — | INTA IIACS (> 35 °C en floración) | conteo sin umbral |
| Agua del ciclo | lluvia de la campaña 600–1.200 mm | ≥ 400 mm | [E]; FAO CI (500–800 mm) | |
| pH | 5,0–7,0 | 4,5–8,5 | [E] | |
| Pendiente | ≤ 6 % | ≤ 25 % | Verdoodt & Van Ranst 2003, Tabla 4.5 | |

### Algodón (secano)
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| T media | 22–36 °C | 15–42 °C | [E] | |
| Grados-día | Tbase 12 °C | 1.645–1.867 °C·d a 60 % de cápsulas abiertas | Colombo 2019 (INTA Reconquista), Fig. 8 | |
| Período libre de heladas | ≥ 200 días | — | FAO CI | |
| Calor reproductivo | — | — | Colombo 2019 p. 5 (> 33 °C) | conteo sin umbral |
| Agua | lluvia de la campaña 750–1.200 mm | ≥ 450 mm | [E]; FAO 66 p. 154 (600–1.200) | |
| pH | 6,0–8,0 | 5,5–8,0 | FAO CI y FAO 66 p. 159 (7–8 ópt.); [E] (6–7,5) | **contradicción**: propongo el rango común 6–8 |

### Girasol (secano)
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| T media | 18–25 °C | 5–45 °C | FAO CI; [E] | |
| Grados-día | Tbase 6–8 °C, 1.600–1.700 °C·d al ciclo | — | ASAGIR | Tbase 4 °C en FAO 66 |
| Helada tardía (5 cm) | sin heladas a 5 cm con 6–8 hojas o botón (0 °C) | cotiledón sobrevive −5 °C | ASAGIR | INTA Las Breñas: siembra de agosto al 10-sep en el Chaco |
| Calor en llenado | — | — | ASAGIR (> 35 °C baja el aceite) | conteo sin umbral |
| Agua | lluvia de la campaña 600–1.000 mm | ≥ 300 mm | FAO CI; [E]; INTA Las Breñas (360 mm de agua útil) | |
| pH | 6,0–7,5 | 5,5–8,0 | FAO CI; [E] | |

### Arroz (riego por inundación)
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| Frío en microsporogénesis | Tmin > 17–19 °C (sensibles) | > 15 °C (tolerantes) | SOSBAI 2025 p. 32; Steinmetz et al. 2004 p. 35 | requiere contar días con Tmin < 15 °C en ene–feb desde el diario corregido (A4) |
| Calor en antesis | Tmax ≤ 35 °C | — | SOSBAI 2025 p. 32 | |
| Agua | — | — | Currie & Ruberto 2003 (12.500–15.000 m³/ha·ciclo en Corrientes) | **la aptitud depende del agua de riego, que el Hexoma no tiene**: la lluvia no define la aptitud |
| pH | 5,5–7,0 | 4,5–9,0 | [E]; SOSBAI p. 42 (encalar < 5,5) | |
| Suelo | hidromórfico, horizonte poco permeable | — | SOSBAI p. 29 | proxy: pastizal inundable, arcilla alta |
| Pendiente | 0,5–1 % | ≤ 3 % | guía RENARE (Uruguay) p. 6 | |

### Pasturas megatérmicas implantadas
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| T de crecimiento | 28–35 °C | se detiene < 15 °C | Veneciano et al. 2002 p. 2; Tb 12–16 °C (Mendonça & Rassini 2006) | |
| Lluvia anual | ≥ 800 mm (Brachiaria, Setaria) | Gatton 500–1.000; Rhodes 700–1.200 | Pérego 1999; Mc Lean et al. 2015; INTA Las Breñas 2005; Tropical Forages | por especie |
| Heladas | — | Setaria −3/−4 °C (sobre el pasto); Rhodes −1 (crecimiento) / −10 (reposo) | Tropical Forages; [E] | Brachiaria, Pangola y Gatton: sin valor numérico |
| Salinidad | — | Rhodes tolera > 10 dS/m | Tropical Forages | el Hexoma no tiene salinidad |
| Anegamiento | Setaria y Rhodes (hasta 15 días) toleran; Gatton no | — | Tropical Forages; INTA Las Breñas | proxy MapBiomas |

### Campo natural
Sin umbrales publicados como unidad. **Propuesta:** no calcular aptitud; describir la oferta con la estacionalidad
térmica (Gándara & Pereira, INTA El Sombrerito) y el uso observado de MapBiomas. El proxy de ECOCROP (*Paspalum
notatum*) lo eligió el agente, no una fuente, y queda afuera.

## Decisiones que necesito del autor
1. **Pendiente:** ¿se usan las clases de Sys et al. (1993) para todos los cultivos (0–8 / 8–16 / 16–30 / 30–50 / > 50 %)
   o sólo donde hay fuente específica?
2. **pH de la yerba:** se descartan ECOCROP e ICRAF (6,5–7) a favor de las fuentes regionales (4,5–5,5). ¿De acuerdo?
3. **pH del algodón:** FAO (7–8) frente a ECOCROP (6–7,5). Propuesta: 6–8.
4. **Calor:** ninguna fuente dice cuántos días > 35 °C tolera un cultivo. Propuesta: informar el conteo por campaña sin
   convertirlo en clase.
5. **Arroz:** sin dato de agua de riego, ¿se calcula sólo la aptitud térmica y de suelo, rotulada «sin agua»?
6. **Campo natural:** ¿sin aptitud, como se propone?
7. **Combinación de factores** (A4 paso 2): factor más limitante (FAO) o conteo de factores limitantes.

## Vacíos que quedan
- Fuentes del INTA o del INYM con temperaturas críticas de helada en yerba por estado: no se encontraron.
- Grados-día de soja y yerba: sin fuente verificada.
- Período libre de heladas explícito para soja, maíz y girasol: sólo hay la duración del ciclo.
- Pendiente con fuente argentina: sólo Burtnik (yerba, NE de Corrientes).
- Salinidad, profundidad efectiva y drenaje: el Hexoma no los tiene.
