# Reglamento del director de Spatia

Reglas vigentes que el director hace cumplir. Fuente: memoria del proyecto y `CLAUDE.md`, consolidadas el 3-nov-2026. Ante dos notas que se contradicen gana la más nueva. Las decisiones posteriores del autor van a `decisiones.md` y prevalecen sobre este archivo.

**El director no edita este archivo ni `reglas.yaml`.** Si una regla le parece mal, lo pregunta en el informe.

## 1. Qué puede arreglar solo y qué no

- **Arregla y despliega:** cualquier cambio en `src/` o `static/` que mejore el sitio sin cambiar qué capas existen ni dónde. Incluye textos, i18n, leyendas, unidades, escalas de color, tooltip/hover, gráficos, paneles, layout, accesibilidad, errores de consola y código del mapa (`Map.svelte`, stores, `+page.svelte`).
- **Nunca, aunque parezca mejora:**
  - Crear o eliminar una capa (ids de `ANALYSIS_REGISTRY` / `HEX_LAYER_REGISTRY`) o un territorio.
  - Cambiar `coverage`, que define el alcance.
  - Cambiar los cache-busters `?v`, que indican un cambio de datos.
  - Cambiar URLs de datos.
  - Tocar R2, el pipeline, GEE, el Hexoma, workflows, `wrangler.toml`, CORS o dependencias.
- **Si el error viene de los datos** (parquet incompleto, columna faltante, valor imposible): se informa como «datos» para una sesión local. No se tapa en el frontend.

## 2. Alcance territorial

- Capas de análisis sólo en NEA AR (Misiones, Corrientes, Chaco, Formosa), Itapúa y Alto Paraná. En el resto de PY y en BR: edificios, censo y límites. Razón: costo de mantenimiento sin nadie que lo pague (3-nov).
- Excepción: `/eudr/check` cubre PY completo y el sur de BR. Sus textos nombran PY/BR a propósito.
- Departamentos AR = radios INDEC 2022, nunca GADM. Chaco tiene 25 departamentos, incluido «2 de Abril» (2-nov).

## 3. Lo que el sitio no debe decir

La lista máquina está en `reglas.yaml → frases_prohibidas`.
- «inteligencia geoespacial/territorial/comercial». El término rector es **«análisis geoespacial»** (19-jun).
- «ciencia ciudadana» (2-nov).
- Puntaje EUDR 70/20/10 o cualquier score ponderado. EUDR muestra pérdida en % (0–40) con el fuego aparte (3-nov).
- «compliance check», «certificación EUDR», «conforme / no conforme EUDR», «deforestación» afirmada como hecho sobre un lote: es demandable (18-jun).
- Que el acceso libre «forma parte del STAN». El STAN es un servicio contratado aparte (2-nov).
- Chat o asistente conversacional en la UI.
- GPL: la licencia es **AGPL-3.0**. No mencionar SoftwareX.
- «riesgo hídrico», «Riesgo alto/bajo», «Score alto», «(invertida)», «media geométrica» en inundación.
- «Hogares estimados». «0 % = nativo» en PY/BR. «Piso inadecuado» (es «Sin piso revestido»).
- «Tipo 1/2/3» o tipos con nombre de columna.
- Cifras viejas: «23 análisis», «18 departamentos» de Itapúa (son 17), Hansen v1.12 (es v1.13, 2016–2025).
- Que la yerba o el té están alcanzados por EUDR: en la región aplica sólo a madera.

## 4. Identidad y textos legales

- Afiliación: «CONICET / FHyCS-UNaM · GEE Partner».
- Términos v2 (`spatia_terms_v2`).
- Encuadre: «noreste argentino y sus regiones transfronterizas», no «centrada en Misiones». Catastro, términos (jurisdicción Posadas), UNaM y el SDM forestal sí son de Misiones.
- Lente «Economía y comercio» (no hay capa de infraestructura).

## 5. Idiomas

- El castellano va sin prefijo. Rutas `/en` `/pt` `/gn` sólo para páginas de texto. `/es/...` da 404. hreflang recíproco.
- Guaraní: sólo etiquetas cortas. Los textos largos (metodología, servicios) se muestran en castellano vía `gnFallback.ts`, porque no hay quien verifique la traducción.
- `method` existe sólo en es/en; pt muestra un aviso. `/demo/el-nino` y `/calibracion` son sólo en castellano.
- Una clave i18n visible en pantalla (`rank.flood.tab.pct`) es un error: falta la entrada en el diccionario.
- Texto fijo en castellano dentro de la UI del mapa es un error de i18n, salvo en páginas declaradas sólo en castellano.

## 6. Panel, leyenda y mapa

- Cada capa tiene howToRead, implications y method.
- Toda variable lleva `unit`. La fila usa `rawUnit ?? '%'` y `scale`. **La leyenda, el tooltip y el panel muestran el mismo número con la misma unidad y la misma escala** que lo que pinta el mapa en la pestaña activa.
- No quedan variables «percentil». Las censales van en tasas reales, y el % dice sobre qué total («Ocupados (% de 14 años y más)»).
- Bases con un decimal y «<1». Nunca «0 personas» al lado de una tasa. Radios con menos de 30 personas → sin dato.
- Magnitud por hexágono = sólo «Edificios».
- `type_label` va sólo como chip, nunca como fila. Deforestación no lleva chip.
- **0 real ≠ sin cobertura.** Un valor 0 no se pinta ni se rotula como «sin dato». Columna cruda NULL → «Sin dato».
- PM2.5 tiene dos escalas: provincial P2–P98 (por defecto) y regional fija 7–20 µg/m³ (`&escala=regional`). Las clases se nombran por meta OMS.
- El recuadro «valor alto» sigue la pestaña activa.
- Inundación:
  - Pestañas % edificios / Edificios / % tierra / Cambio; cada una pinta su propia columna (HAND < 5 m).
  - En la llanura saturada se pinta JRC con una nota, sin barras.
  - La metodología advierte que Sentinel-1 sobreestima el agua en la llanura chaqueña.
- Usos del suelo: desde el 3-nov-2026 sale del Hexoma con MapBiomas Argentina Col. 3 (clases 2025 y 1985, trayectoria; sólo NEA AR), en reemplazo de Dynamic World. Los textos tienen que nombrar la fuente que pinta la capa en producción: si el texto y la config no coinciden, es un hallazgo; si la transición está a medio desplegar, es pregunta, no arreglo.
- Edificios coloreados por volumen (área × altura).
- Al mezclar países aparece el aviso de comparabilidad censal (`warn.censusCrossCountry`).
- Con `a`, `h3`, `dept` o `zona` en la URL no se abre la bienvenida.
- Un solo llamado a la acción: `RequestDiagnostic` (mailto).
- Gráficos: ningún NaN, Infinity, undefined ni eje vacío. El valor de un departamento coincide en ranking, tooltip, ficha y resumen.

## 7. Estilo visual

- Paleta «Tierra colorada»: tokens en `src/app.css` y `src/lib/utils/sello.ts`. Divergente río · gris · tierra, **nunca selva↔tierra** (falla en deuteranopía).
- Tipografía: Plex Sans, Plex Mono y Newsreader para números. Roboto Condensed sólo en /eudr/informe.
- Concentración con un decimal. Nunca «100 %» si no es exacto.
- Los textos de lectura no nombran colores: `scripts/check_color_words.py`.

## 8. Código

- `import * as maplibregl from '$lib/maplibre'`, nunca `'maplibre-gl'` directo.
- Filtros MapLibre como expresiones, nunca la sintaxis legacy `'$type'`.
- Scoring: media geométrica de componentes PCA de igual peso. Nunca promedios ponderados. k-means sólo para tipologías. Etiquetas en castellano.
- Antes de cambiar un componente compartido (mapa, comparación), listar qué funciona y verificar que siga funcionando: lo hacen las sondas.

## 9. Proceso: el director nunca

- Despliega a una rama preview ni desde HEAD desacoplado.
- Hace force-push, reescribe historia o borra ramas ajenas.
- Corre wrangler contra R2, purga la CDN o cambia la configuración de Cloudflare.
- Declara arreglado algo sin re-sondearlo en el navegador.
- Manda mensajes fuera de GitHub.
