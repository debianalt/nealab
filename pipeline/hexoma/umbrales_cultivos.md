# A4 · Umbrales por cultivo

Estado: **validado por el autor el 1-nov-2026** (reglas de abajo). Fuente de verdad: este archivo; se carga en
`meta.umbrales_cultivo` y `meta.umbrales_evidencia` (todas las filas con cita) con `build_hexoma_umbrales.py`.
La aptitud se calcula en A4 paso 2, sobre la temperatura corregida con estaciones.

## Reglas decididas (1-nov-2026)
1. **Precedencia de fuentes, en toda la tabla:** regionales (INTA, INYM, ASAGIR, universidades del NEA; sur de Brasil,
   Paraguay y Uruguay) → FAO ECOCROP → FAO 66 y FAO Crop info → otras internacionales. *Precisada el 1-nov-2026:*
   «regional» = dato de ensayos o relevamientos de la región. Una compilación de manual extranjero citada en material
   argentino, sin ensayo local que la respalde y que contradice a las demás fuentes y a lo observado, no cuenta como
   regional. FAO queda sólo donde ECOCROP
   no tiene el dato (temperatura base de grados-día, días de anegamiento, período libre de heladas, agua del ciclo).
2. **Pendiente:** clases de Sys et al. (1993) por defecto para todos los cultivos: S1 0–8 %, S2 8–16 %, S3 16–30 %,
   N1 30–50 %, N2 > 50 %. Donde hay fuente propia del cultivo, esa fija el óptimo (S1); ninguna funciona como límite de
   exclusión por sí sola.
3. **pH de la yerba:** fuentes regionales (4,5–5,5; tolera hasta 4). Se descartan ECOCROP e ICRAF (6,5–7; 5,8–6,8).
4. **pH del algodón:** ECOCROP antes que FAO (6,0–7,5; 5,0–9,5). *Reemplazada el 1-nov-2026 por la regla 1 al aparecer
   una fuente argentina (ajuste 5).*
5. **Calor:** ninguna fuente dice cuántos días > 35 °C tolera un cultivo: se informa el conteo por campaña sin
   convertirlo en clase.
6. **Arroz:** aptitud térmica y de suelo, rotulada «sin dato de agua de riego».
7. **Campo natural:** sin aptitud; se rotula por el uso observado de MapBiomas: «Campo natural (pastizal o arbustal,
   pastoreo extensivo)». El potencial de mejora lo cubren las pasturas implantadas.
8. **Combinación de factores:** FAO, factor más limitante: la clase de la celda es la peor de sus factores.

## Ajustes de aplicación (1-nov-2026, tras la primera validación)
La primera validación contra MapBiomas y el MAGyP (`validacion_aptitud.md`) mostró cuatro problemas de aplicación. El
autor pidió corregirlos. Cada ajuste tiene su evidencia:
1. **Cítricos y pasturas, temperatura:** el rango validado es de temperatura «de crecimiento». Comparado con la media
   anual (~22 °C) no dejaba ninguna celda en S1. Ahora se compara con la media de **septiembre a marzo**, la estación
   de crecimiento. El rango no cambia. Pasturas: el óptimo (28–35 °C) sigue sin alcanzarse en la media sep–mar del NEA
   (25 °C); se informa como hallazgo.
2. **Té, lluvia:** 1.800–2.200 mm (Parra) describe la zona tealera, no es un requisito, y la lluvia de cada campaña cae
   fuera de esa banda muy seguido. Ahora S1 = 1.400–2.200 mm (piso de ECOCROP, techo de Parra) y el límite sigue en
   ≥ 1.000 mm. Las heladas del té no cambian: las convencionales del INTA y el modelo dan la misma frecuencia de años
   con helada a 5 cm en sep–oct (Cerro Azul 35 % y 35 %; Bella Vista 43 % y 54 %; Las Breñas 66 % y 67 %).
3. **Pendiente de yerba y té:** clases de la tabla de té de Verdoodt & Van Ranst (2003, Tabla 4.15, p. 103; adaptada de
   Sys et al. 1993): S2 ≤ 25 %, S3 ≤ 55 %, N > 55 %. Las clases generales ponían la zona yerbatera de Guaraní en S3. El
   óptimo propio sigue fijando S1 (yerba ≤ 4 %, té ≤ 17,6 %). Cítricos sigue con las clases generales (no hay tabla).
4. **Soja, pH:** el 67 % de las celdas cultivadas en 2025 tiene pH de SoilGrids > 6,5, el techo del óptimo de ECOCROP:
   el óptimo de la literatura internacional dejaba afuera los suelos neutros donde se siembra. No hay fuente del NEA;
   hay fuentes argentinas (pampeanas) con número, que por la regla 1 van antes que ECOCROP: Vázquez & Pagani (2018,
   UNLP, Tabla 2, p. 5, que toma los valores de Porta et al. 1999) dan óptimo 6,0–7,0 y tolerancia 4,5–7,5
   (verificado sobre la página); Vázquez et al. (2012, *Ciencia del Suelo* 30(1), pp. 48–49) confirman el óptimo 6–7
   con tolerancia 4,5–7. Se usa S1 6,0–7,0 y límite 4,5–7,5. Las otras fuentes de la búsqueda (Embrapa 5,5–6,0 para
   oxisoles; Sainz Rozas et al. 2011, 6–7,5 para los cultivos de la región) están en la evidencia.
5. **pH de maíz, algodón, girasol y arroz:** la misma tabla argentina (Vázquez & Pagani 2018, UNLP, Tabla 2, de Porta et
   al. 1999; verificada sobre la imagen de la página) trae valores para estos cultivos. Por la regla 1 (regionales antes
   que ECOCROP) reemplaza a ECOCROP: maíz 5,5–7,0 (tolerancia 5,0–8,0); algodón 5,2–6,0 (4,8–7,5); arroz 5,0–7,0
   (4,0–8,0); girasol sólo tolerancia 6,0–7,5, con el óptimo de ECOCROP. Advertencia: el óptimo del algodón (5,2–6,0) es
   más ácido que el de todas las otras fuentes (FAO 7–8; ECOCROP 6–7,5), y los autores aclaran que los valores son
   «sólo orientativos». **Se probó y se descartó para el algodón:** con 5,2–6,0 el lift de S1 contra MapBiomas cayó de
   2,33 a 0,11 y la correlación por departamento con el MAGyP quedó negativa (−0,22), porque el algodonero del Chaco está
   en suelos de pH 6,5–7,5. El algodón vuelve a ECOCROP (6,0–7,5; 5,0–9,5) por la regla 1 precisada. Maíz, girasol y
   arroz quedan con la tabla UNLP (no contradice lo observado: maíz igual, girasol y arroz mejoran levemente); la soja
   también, respaldada además por un ensayo argentino (Vázquez et al. 2012).

## Ajustes del 7-nov-2026 (pedido del autor: techo de lluvia en S2 y revisar pasturas)
6. **Techo de lluvia de los extensivos.** El límite de lluvia de soja, maíz, algodón y girasol no tenía cota superior:
   el exceso nunca bajaba de S2 y en la parte húmeda del NEA soja, maíz y algodón daban el mismo mapa. Ahora el S2 llega
   hasta el **máximo absoluto de ECOCROP**, que ya estaba en la evidencia (`umbrales_evidencia_extensivos.md`): soja
   1.800, maíz 1.800, algodón 1.500 y girasol 1.600 mm. **Por encima, S3 (marginal), no N.** Con el techo como límite
   de exclusión, el 99,7 % de Misiones (lluvia normal > 1.800 mm) quedaba N para soja y maíz, y el 61 % de Corrientes
   para algodón. Eso contradice lo observado: hay maíz en Misiones, y soja y maíz con 1.700–2.000 mm en el oeste de
   Paraná y en Alto Paraná.
   Validación de tres corridas contra la de hoy (`validar_aptitud.py`; regla fijada antes de ver los números: N sólo
   si no baja ninguna prueba):

   | Cultivo | Lift S1+S2 (MapBiomas) hoy → con techo | Spearman S1+S2 por depto (MAGyP) hoy → con techo |
   |---|---|---|
   | Soja | 1,36 → 1,49 | 0,37 → 0,33 |
   | Maíz | 1,06 → 1,13 | 0,26 → 0,20 |
   | Algodón | 1,06 → 1,25 | 0,31 → 0,22 |
   | Girasol | 1,28 → 1,29 | 0,52 → 0,29 |

   Con N o con S3 el S1+S2 es el mismo (las pruebas no distinguen entre S3 y N). La correlación por departamento baja,
   así que va S3. Los perennes ya tenían el techo fuera del rango del NEA (yerba 2.500, naranja 2.700, té 5.000 mm;
   el máximo de Misiones es 2.333): no cambian.
7. **Pasturas, temperatura: sin cambio.** El S1 de 28–35 °C (Veneciano et al. 2002) compila a Huss (1986) y Hernández
   (1985) y es temperatura de crecimiento. Se probaron los óptimos de temperatura media de ECOCROP: *M. maximus* 19–30
   (abs 6–35), *B. brizantha* 25–35 (15–38) y la unión de las tres especies (19–35; 6–38). Ninguna valida: S1 = 73 %,
   33 % y 73 % de las celdas con pastura implantada, con lift 0,90, 0,84 y 0,90. La temperatura no explica dónde se
   implantan las pasturas en el NEA. Queda como hallazgo: el óptimo térmico no se alcanza en la media sep–mar (25 °C) y
   la clase de pasturas la deciden la lluvia y la pendiente.

## Cómo se armó
- Tres búsquedas en paralelo (perennes; extensivos; arroz y pasturas), con cita completa, página o tabla, URL y cita
  textual por valor: la evidencia completa está en `umbrales_evidencia_*.md`.
- **Verificación propia (1-nov-2026):** las citas de heladas, frío, calor y lluvia se buscaron en los textos bajados
  (PDF → texto): 40 de 42 aparecen literales; las 2 restantes son páginas web no guardadas (pH del té del TRI de Sri
  Lanka; heladas del té de Wang et al. 2021), marcadas «no verificada localmente». Los parámetros de ECOCROP se
  verificaron contra el CSV de la base (13 especies).
- **Abrigo frente a planta:** casi todos los umbrales de helada son temperatura de la planta o del aire junto al
  cultivo. La hoja expuesta queda 3–5 °C por debajo del abrigo (Oliveira et al. 2012, Embrapa Doc. 346, p. 11 y 20).
  Por eso se usa la **helada a 5 cm** (`dias_helada_suelo`, corregida con las convencionales del INTA) para los
  umbrales de planta cercanos a 0 °C, y la mínima de abrigo corregida (`tmin_abs`) para el daño severo.
- **Clases:** S1 = óptimo; S2 = entre óptimo y límite; N = fuera del límite. Pendiente: clases de Sys.

## Variables del Hexoma que se usan
| Variable | Tabla | Nota |
|---|---|---|
| Lluvia de la campaña, mm | `clima.chirps_campania.p_mm` | CHIRPS +10 % frente a pluviómetros |
| Balance P − ET0 de la campaña o de DEF, mm | `clima.balance_*` | |
| Media de máximas y mínimas, °C (campaña o estación) | `clima.era5_campania_corr`, `era5_estacion_corr` | corregidas con estaciones |
| Grados-día, °C·d | desde el diario corregido, con la base de cada cultivo | la tabla guarda base 10; A4 recalcula con 8, 12, 6–8 |
| Días > 35 °C | `clima.era5_campania_corr.dias_tmax35` | se informa, sin clase |
| Heladas en abrigo y a 5 cm, mínima absoluta, primera y última | `clima.era5_helada_corr` | período libre ≈ 365 − (última − primera) |
| pH, arcilla, arena, CIC | `suelo.h3` | SoilGrids 0–30 cm (modelo) |
| Pendiente, % = 100·tan(grados) | `suelo.h3.pendiente_media_grados` | FABDEM |
| Anegamiento (proxy) | `uso.h3_anio` (pastizal y bosque inundables) | no hay dato de drenaje de suelo |

## Umbrales por cultivo
Claves de fuente: ver `umbrales_evidencia_*.md` (referencias completas). [E] = FAO ECOCROP. «Sys» = clases de
pendiente de la regla 2.

### Yerba mate
| Factor | S1 (óptimo) | Límite (N fuera) | Fuente | Nota |
|---|---|---|---|---|
| Lluvia anual | 1.500–2.500 mm | ≥ 1.200 mm | Zelada & González 2019 p. 18; Oliveira & Rotta 1985 p. 26; Carvalho 2003 p. 459 | Misiones no supera 2.500: sin techo |
| T media anual | 20–23 °C | 15,5–25,5 °C | Oliveira & Rotta 1985 p. 26; Zelada & González 2019 p. 18 | |
| Helada severa (abrigo) | mínima absoluta > −6 °C | ≥ −11,6 °C | [E] KTMPR −6; Carvalho 2003 p. 459 (ocurrencia en el área natural) | sin valor regional numérico |
| Helada en brotación (5 cm) | sin heladas a 5 cm después del 1-sep | — | [E] KTMP −1 (brotación) | requiere la última helada a 5 cm (A4) |
| pH | 4,5–5,5 | ≥ 4,0 | Zelada & González 2019 p. 33; Embrapa 2014 | regla 3 |
| Arcilla | > 35 % | ≥ 15 % | Oliveira & Rotta 1985 p. 28; Carvalho 2003 | |
| Anegamiento | sin suelos hidromórficos | — | Oliveira & Rotta 1985 p. 28; Embrapa 2014 | proxy MapBiomas |
| Pendiente | ≤ 4 % | S2 ≤ 25 %, S3 ≤ 55 %, N > 55 % | Burtnik 2006 p. 11 (óptimo); Verdoodt & Van Ranst 2003, Tabla 4.15 (té, Sys) | ajuste 3 |

### Té
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| Lluvia anual | 1.400–2.200 mm | ≥ 1.000 mm | [E] (piso y límite); Parra (MAGyP, Alimentos Argentinos 34) (techo) | ajuste 2 |
| T media del ciclo | 20–30 °C | 8–35 °C | [E] | sin valor regional |
| Helada en brotación (5 cm) | sin heladas a 5 cm después del 1-sep | — | [E] KTMP 0 (brotación) | |
| Helada severa (abrigo) | mínima absoluta > −5 °C | — | [E] KTMPR −5 | |
| pH | 4,5–5,5 | 4,0–6,0 | Parra (MAGyP); [E] | |
| Pendiente | ≤ 17,6 % (5–10°) | S2 ≤ 25 %, S3 ≤ 55 %, N > 55 % | Mwesige et al. 2026 (óptimo); Verdoodt & Van Ranst 2003, Tabla 4.15 | ajuste 3 |
| Anegamiento | no tolera | — | Crane & Balerdi (EDIS) p. 2 | proxy MapBiomas |

### Cítricos (naranja y mandarina)
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| Lluvia anual | 1.200–2.000 mm (~100 mm/mes) | ≥ 450 mm (con riego complementario) | INTA EEA Concordia cap. IV p. 2; [E] naranja | |
| T de crecimiento (media sep–mar) | 23–34 °C | 12,8–37 °C | INTA Concordia cap. IV p. 1; Oliveira et al. 2012 p. 10 (Embrapa, sur de Brasil) | ajuste 1 |
| Helada en flor y fruto chico (5 cm) | sin heladas a 5 cm en sep–oct | — | INTA Concordia cap. IV p. 1 (−1 °C) | |
| Helada severa (abrigo) | mínima absoluta > −3 °C | > −8 °C | INTA Concordia (−3 °C, fruto); Oliveira et al. 2012 p. 12 (letal foliar) | mandarina más tolerante |
| pH | naranja 5,0–6,0; mandarina 6,0–6,8 | 4,0–8,3 | [E] naranja y mandarina | sin pH regional |
| Textura | areno-arcillosos a arcillo-arenosos, profundos | — | INTA Concordia cap. IV p. 8 | cualitativo: no entra en la clase |
| Pendiente | ≤ 5 % | Sys | Veloso et al. 2000 (Embrapa) | |

### Soja (secano)
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| T media del ciclo (DEF) | 20–33 °C | 10–38 °C | [E] | regla 1 (antes FAO 66: 20–30) |
| Calor en floración | — | — | FAO 66 p. 129 (aborto a 35 °C) | regla 5: se informa el conteo |
| Agua (lluvia de la campaña) | 600–1.500 mm | 450–1.800 mm (> 1.800: S3) | [E]; INTA Las Breñas (demanda regional 650 mm) | ajuste 6 |
| Período libre de heladas | ≥ 130 días | ≥ 100 días | FAO CI (ciclo 100–130 d o más) | proxy: duración del ciclo |
| pH | 6,0–7,0 | 4,5–7,5 | Vázquez & Pagani 2018 (UNLP), Tabla 2 p. 5, de Porta et al. 1999; Vázquez et al. 2012, Ciencia del Suelo 30(1) pp. 48–49 | ajuste 4 (antes [E] 5,5–6,5) |
| Arcilla | 20–60 % | — | Verdoodt & Van Ranst 2003 (Sys) | excluye suelos muy arenosos |
| Anegamiento | < 7 días | — | FAO 66 p. 129 | proxy MapBiomas |
| Pendiente | Sys | Sys | Verdoodt & Van Ranst 2003 | |

### Maíz (secano)
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| T media | 18–33 °C | 10–47 °C | [E] | regla 1 (antes FAO CI: > 15 °C) |
| Grados-día | Tbase 8 °C, Tupper 30 °C | — | FAO 66 p. 119 | ECOCROP no tiene base; Bautista INTA 2.045 GD a madurez |
| Calor en floración | — | — | INTA IIACS (> 35 °C) | regla 5 |
| Agua (lluvia de la campaña) | 600–1.200 mm | 400–1.800 mm (> 1.800: S3) | [E] | ajuste 6 |
| pH | 5,5–7,0 | 5,0–8,0 | Vázquez & Pagani 2018 (UNLP), Tabla 2 p. 5, de Porta et al. 1999 | ajuste 5 (antes [E] 5,0–7,0 / 4,5–8,5) |
| Pendiente | Sys | Sys | Verdoodt & Van Ranst 2003 | |

### Algodón (secano)
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| T media | 22–36 °C | 15–42 °C | [E] | |
| Grados-día | Tbase 12 °C; 1.645–1.867 °C·d a 60 % de cápsulas abiertas | — | Colombo 2019 (INTA Reconquista), Fig. 8 | regional |
| Período libre de heladas | ≥ 200 días | — | FAO CI | ECOCROP no lo tiene |
| Calor reproductivo | — | — | Colombo 2019 p. 5 (> 33 °C) | regla 5 |
| Agua (lluvia de la campaña) | 750–1.200 mm | 450–1.500 mm (> 1.500: S3) | [E] | ajuste 6 |
| pH | 6,0–7,5 | 5,0–9,5 | [E] | regla 1 precisada (ajuste 5): la tabla UNLP (5,2–6,0) se probó y se descartó |
| Pendiente | Sys | Sys | — | |

### Girasol (secano)
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| T media | 17–34 °C | 5–45 °C | [E] | regla 1 (antes FAO CI: 18–25) |
| Grados-día | Tbase 6–8 °C; 1.600–1.700 °C·d al ciclo | — | ASAGIR | regional |
| Helada tardía (5 cm) | sin heladas a 5 cm con 6–8 hojas o botón (0 °C) | cotiledón sobrevive −5 °C | ASAGIR | INTA Las Breñas: siembra de agosto al 10-sep en el Chaco |
| Calor en llenado | — | — | ASAGIR (> 35 °C baja el aceite) | regla 5 |
| Agua (lluvia de la campaña) | 600–1.000 mm | 300–1.600 mm (> 1.600: S3) | [E]; INTA Las Breñas (360 mm de agua útil) | ajuste 6 |
| pH | 6,0–7,5 | 6,0–7,5 | [E] (óptimo; la tabla UNLP no da óptimo); Vázquez & Pagani 2018 (UNLP), Tabla 2 p. 5, de Porta et al. 1999 (tolerancia) | ajuste 5 (antes límite [E] 5,5–8,0) |
| Pendiente | Sys | Sys | — | |

### Arroz (riego por inundación) — sin dato de agua de riego
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| Frío en microsporogénesis | Tmin > 17–19 °C (sensibles) | > 15 °C (tolerantes) | SOSBAI 2025 p. 32; Steinmetz et al. 2004 p. 35 | días con Tmin < 15 °C en ene–feb, del diario corregido (A4) |
| Calor en antesis | Tmax ≤ 35 °C | — | SOSBAI 2025 p. 32 | regla 5 |
| Agua | — | — | Currie & Ruberto 2003 (12.500–15.000 m³/ha·ciclo) | regla 6: rótulo «sin dato de agua de riego» |
| pH | 5,0–7,0 | 4,0–8,0 | Vázquez & Pagani 2018 (UNLP), Tabla 2 p. 5, de Porta et al. 1999; SOSBAI p. 42 (encalar < 5,5) | ajuste 5 (antes [E] 5,5–7,0 / 4,5–9,0) |
| Suelo | hidromórfico, horizonte poco permeable | — | SOSBAI p. 29 | proxy: pastizal inundable y arcilla alta |
| Pendiente | 0,5–1 % | ≤ 3 % | guía RENARE (Uruguay) p. 6 | regional |

### Pasturas megatérmicas implantadas
| Factor | S1 | Límite | Fuente | Nota |
|---|---|---|---|---|
| T de crecimiento (media sep–mar) | 28–35 °C | se detiene < 15 °C | Veneciano et al. 2002 p. 2 (INTA San Luis) | ajuste 1; Tb 12–16 °C (Mendonça & Rassini 2006); ajuste 7: ECOCROP por especie probado y descartado |
| Lluvia anual | Brachiaria y Setaria ≥ 800 mm; Gatton 500–1.000; Rhodes 600–1.500 | — | Pérego 1999; Mc Lean et al. 2015; INTA Las Breñas 2005; [E] Rhodes | por especie |
| Heladas | — | Setaria −1 (crecimiento) / −4 (reposo); Rhodes −1 / −10 | [E] | Brachiaria, Pangola y Gatton: sin valor numérico |
| Anegamiento | Setaria y Rhodes toleran (Rhodes hasta 15 días); Gatton no | — | INTA Las Breñas; Tropical Forages | proxy MapBiomas |
| Pendiente | Sys | Sys | — | |

### Campo natural
Sin aptitud (regla 7). Se rotula con el uso observado de MapBiomas: «Campo natural (pastizal o arbustal, pastoreo
extensivo)». La estacionalidad de la oferta forrajera (nov–mar alta, jun–ago muy baja; Gándara & Pereira, INTA El
Sombrerito) se describe con el clima de la celda, sin clase.

## Vacíos que quedan
- Fuentes del INTA o del INYM con temperaturas críticas de helada en yerba por estado: no se encontraron.
- Grados-día de soja y yerba: sin fuente verificada.
- Período libre de heladas explícito para soja, maíz y girasol: sólo la duración del ciclo.
- Pendiente con fuente argentina: sólo Burtnik (yerba, NE de Corrientes).
- Salinidad, profundidad efectiva, drenaje y agua de riego: el Hexoma no los tiene.
