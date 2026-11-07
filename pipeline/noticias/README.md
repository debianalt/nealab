# Noticias: archivo de prensa de cortes en Misiones (esquema `noticias` del Hexoma)

Verdad observada para validar los escenarios de `pipeline/infra/` (cruces que aíslan, día de lluvia, río contra la
RP2, cortes de luz). Sólo datos: la comparación con los modelos es otra fase.

| Comando (desde `neahub/`) | Qué hace |
|---|---|
| `python pipeline/noticias/bajar_notas.py` | 6 medios con la API de WordPress abierta × 24 consultas fijas → `noticias.notas` (texto completo) y `noticias.fuentes`; crudos en `%HEXOMA_FUENTES%/noticias/crudo/` y copia zip en `OneDrive/nealab/archivo_noticias/`. Reanudable. |
| `python pipeline/noticias/extraer.py` | Oraciones → `noticias.oraciones`; menciones por reglas (tipo, lugar, ruta, km, altura del río, fecha relativa) → `noticias.menciones` |
| `python pipeline/noticias/validar_noticias.py --muestra` y luego sin argumentos | Muestra de 60 menciones a anotar a mano (`muestra_precision.csv`) e informe `validacion_noticias.md` |

- **El archivo no se regenera**: las notas se borran o se editan. El crudo y su copia en OneDrive son el respaldo.
- **Medios fuera**: El Territorio (no es WordPress), MisionesCuatro (desafío anti-bots de Cloudflare: no se sortea),
  La Voz de Cataratas (API cerrada, 401). User-Agent honesto y una pausa de 1 s por pedido y medio.
- **Estado (7-nov-2026)**: 37.161 notas (2000–2026), 874.653 oraciones, 29.881 menciones; precisión del tipo 87 % y
  del lugar 85 % (muestra nueva de 60, patrones v2; la v1 dio 52 % y se corrigió). Muestras anotadas:
  `muestra_precision_v1.csv` y `muestra_precision_v2.csv`.
- **Trampa de la búsqueda de WordPress**: busca cada palabra adentro de otras palabras («vado» → «privado», «EMSA» →
  «empresa») y las combina con Y; entre comillas busca la frase exacta. El buscador de Misiones Online, en cambio,
  recorta por relevancia («inhabilitada» devuelve 15): para tenerlo completo hay que listar su archivo (pendiente).
- **Lugar** = el nombrado en la oración (municipio, asentamiento BAHRA, arroyo, hidrómetro), no el punto exacto del
  corte. Un nombre presente en varios lugares queda `ambiguo`.
- **Fecha** = la de la nota; «ayer», «anoche», etc. quedan como texto en `fecha_relativa`.
- En QGIS: grupo «Prensa» de `hexoma.qgz` (regenerar con `pipeline/infra/proyecto_qgis.py`).
