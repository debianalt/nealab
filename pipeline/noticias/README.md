# Noticias: archivo de prensa de cortes por agua en Misiones (esquema `noticias` del Hexoma)

Verdad observada para contrastar los escenarios de `pipeline/infra/` (río contra la RP2, arroyos, cruces que aíslan,
día de lluvia) **a escala de municipio y día**. Sólo datos: la comparación con los modelos es otra fase.

| Comando (desde `neahub/`) | Qué hace |
|---|---|
| `python pipeline/noticias/bajar_notas.py` | 6 medios con la API de WordPress abierta × consultas fijas → `noticias.notas` y `noticias.fuentes`; crudos en `%HEXOMA_FUENTES%/noticias/crudo/`, zip en `OneDrive/nealab/archivo_noticias/`. Reanudable. |
| `python pipeline/noticias/bajar_sin_api.py [--medios …] [--muestra N]` | El Territorio y La Voz de Cataratas (sitemaps + HTML), MisionesCuatro (copias de la Wayback Machine), archivo completo de Misiones Online (sitemaps + API por lotes de 20). Filtro v2 por la dirección. Reanudable. |
| `python pipeline/noticias/extraer.py` | Menciones por reglas (tipo, lugar, ruta, km, altura del río, fecha relativa), con la oración, → `noticias.menciones` |
| `python pipeline/noticias/validar_noticias.py [--muestra]` | Muestra a anotar a mano e informe `validacion_noticias.md` |
| `python pipeline/noticias/probar_utilidad.py` | Novedad de los medios sin API y prueba del río contra la RP2 |

- **Estado (7-nov-2026)**: 48.098 notas de 9 medios (2000–2026, denso desde 2017) y 29.143 menciones de cuatro
  tipos: corte de puente, corte de ruta o camino, crecida, evacuados o aislados.
  - Precisión del tipo 87 % y del lugar 85 % (`muestra_precision_v2.csv`); en los medios sin API, 80 % y 77 %
    (`muestra_precision_nuevos.csv`).
- **Para qué sirve** (`probar_utilidad.py`): con la RP2 cortada según el modelo del río hay prensa de corte el 25 %
  de los días, contra 1,7 % el resto (14 veces). Para contrastar cruces o hexágonos uno por uno, no: la mitad de las
  menciones no tiene lugar y la otra mitad es un municipio, un asentamiento o un cauce.
- **Descartado el 7-nov-2026**:
  - **Corte de luz**: contra Black Marble no da señal (radiancia 0,90 de la mediana del asentamiento con corte en
    prensa, 0,87 en días de control). Las notas siguen en `noticias.notas`; sólo dejaron de extraerse.
  - **Tabla `noticias.oraciones`** (231 MB): la oración va dentro de cada mención.
  - **Bajada completa de El Territorio y MisionesCuatro**: aportan 11–18 % de eventos nuevos reales
    (`muestra_novedad_nuevos.csv`). Quedan una muestra al azar de 300 (semilla 11) más lo bajado antes de un corte.
- **Línea con los sitios**: User-Agent que dice quiénes somos, pausa de 1 s por diario (2 s en archive.org),
  `robots.txt`. MisionesCuatro desafía a todo cliente que no sea un navegador: no se sortea, se leen sus copias de la
  Wayback Machine (`via = wayback`, con la fecha de captura).
- **El archivo no se regenera**: los diarios borran y editan notas. El crudo y su zip en OneDrive son el respaldo.
- **Trampas**:
  - La búsqueda de WordPress encuentra palabras adentro de otras («vado» → «privado») y las combina con Y; entre
    comillas busca la frase. El buscador de Misiones Online recorta por relevancia: de ahí sus sitemaps.
  - Con 100 notas completas por pedido, su API corta: van de a 20.
  - Fechas en formatos mezclados: hay que leerlas con `format='ISO8601'`, si no pandas anula las que no coinciden
    con la primera.
- **Errores conocidos de los patrones v2**: piquetes sin palabra de protesta («docentes autoconvocados») cuentan como
  corte de ruta; homónimos de otras provincias (río San Antonio de Córdoba) caen en Misiones; se cuelan menús «Lee
  también» del HTML. Arreglarlos sería una v3 a medir de nuevo.
- **Lugar** = el nombrado en la oración, no el punto del corte. Un nombre presente en varios lugares queda `ambiguo`.
  **Fecha** = la de la nota; «ayer», «anoche», etc. quedan como texto en `fecha_relativa`.
- En QGIS: grupo «Prensa» de `hexoma.qgz` (regenerar con `pipeline/infra/proyecto_qgis.py`).
