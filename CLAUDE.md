# nealab — análisis geoespacial (NEA y regiones transfronterizas)

## Tech Stack
- SvelteKit 5 + adapter-static
- Svelte 5 (runes syntax: `$state`, `$derived`, `$effect`)
- TypeScript
- TailwindCSS 4
- Cloudflare: Pages, Workers, R2, D1
- DuckDB-WASM (parquet queries from R2)
- MapLibre GL 6 + PMTiles. Importar siempre `import * as maplibregl from '$lib/maplibre'`, nunca `'maplibre-gl'` directo: la 6 es sólo ESM y ese módulo registra el worker (`setWorkerUrl`); sin él el mapa no carga. Filtros siempre como expresiones (`['geometry-type']`, `['get', …]`), no la sintaxis legacy (`'$type'`).

## Project Structure
```
src/                    # SvelteKit app
  lib/config.ts         # PARQUETS URLs, HEX_LAYER_REGISTRY, ANALYSIS_REGISTRY
  lib/stores/           # hex.svelte.ts, map.svelte.ts, i18n.svelte.ts, duckdb.ts
  lib/components/analyses/  # OvertureAnalysis (panel de capa hex)
  routes/+page.svelte   # Main page (map, hex click, analysis choropleth)
functions/api/          # Cloudflare Pages Functions (eudr, satellite, esri)
pipeline/               # Python GEE/raster pipeline
  config.py             # MISIONES_BBOX, H3_RESOLUTION, thresholds
  gee_flood_detection.py  # S1 SAR water detection + export
  process_to_h3.py      # zonal_stats_rasterio, compute_flood_risk_score
  run_flood_update.py   # Orchestrator: GEE → download → H3 → validate → R2
  upload_to_r2.py       # Wrangler R2 uploads (ALWAYS use --remote)
  output/               # Generated rasters (.tif) and parquets (gitignored)
db/                     # D1 schema + import scripts (seed.sql gitignored)
```

## Critical Rules
- **Nunca romper funcionalidad existente al implementar features nuevas.**
- Antes de editar archivos compartidos (comparison, map components), listar qué funciona actualmente y verificar que sigue funcionando después de los cambios.
- Para bugs de radio/click: simular el escenario de first-click.

## Development Practices
- Después de implementar features UI/mapa, describir qué debería ser visible y pedir confirmación visual antes de avanzar.
- Antes de escribir scripts de ingestión, verificar si los datos ya existen en la DB.

## Pipeline Conventions
- **Raster nodata**: rasterio nodata=0 en binarios S1 conflicta con dry land → usar `ignore_src_nodata=True`. JRC usa -128 (not water) y -32768 (nodata) → se maneja en `zonal_stats_rasterio`.
- **Zonal stats**: SIEMPRE usar `zonal_stats_rasterio` (polygon masking), NUNCA `zonal_stats_sampling` (centroid, solo legacy).
- **Bbox**: padded a [-56.10, -28.20, -53.55, -25.44] para cubrir hexágonos del borde.
- **GCS bucket `spatia-satellite`**: EXISTE en proyecto `amiable-reducer-398015` (billing activo). Preferir GCS sobre Drive. Para forzar GCS localmente: `--gcs` flag. Descargar: `gcloud storage cp gs://spatia-satellite/satellite/*.tif pipeline/output/`.
- **GEE auth**: credenciales en `~/.config/earthengine/credentials` (OAuth user, scopes: earthengine, cloud-platform, drive, devstorage).
- **R2 uploads**: `npx wrangler r2 object put neahub/data/... --file ... --remote` — `upload_to_r2.py` pasa `--remote` y, desde el 29-oct-2026, confirma que R2 sirva el tamaño subido (antes, en Linux/CI, `shell=True` con lista corría sólo `npx`: jun–oct todas las subidas de CI fueron no-ops en verde). Objetos refrescados en el mismo URL `?v` necesitan `cache_control` corto, no el immutable por defecto. **CRÍTICO**: el key DEBE incluir el prefijo `data/` (ej: `neahub/data/sat_dpto/...`, NO `neahub/sat_dpto/...`). El bucket público sirve desde `pub-...r2.dev/data/...`, si subís sin el prefijo el archivo queda huérfano y el browser lee una versión vieja sin darse cuenta.
- **CDN cache (desde 30-oct-2026)**: `cdn.spatia.ar` se cachea en el borde 1 día (`scripts/cdn_cache.py setup`; regla con ref `spatia_cdn_cache`; reemplazó la regla del 1-jul que dejaba sin caché al 87 % de los objetos, todos los PMTiles incluidos; CORS lo sigue resolviendo R2 con `cors-rules.json`: el borde guarda una copia por Origin). **Tras subir a R2 a mano: `npm run purge-cdn`** (o `python scripts/cdn_cache.py purge data/<key>`); si no, el borde sirve la copia vieja hasta 24 h. Los workflows purgan solos. Token único de Cloudflare (sin vencimiento, todos los permisos de Spatia): `~/Documents/secrets/cloudfare1.env` local (dotenv: token, account, claves S3 de R2; también variable de usuario `CLOUDFLARE_API_TOKEN`) y secret `CLOUDFLARE_API_TOKEN` en CI; no crear tokens nuevos por tarea, ampliar ése. Rollback: `python scripts/cdn_cache.py disable`. Archivos >512 MB (PMTiles BR) excluidos por ruta en `UNCACHEABLE`: si no, el primer pedido devuelve 200 con el archivo entero en vez del rango; **todo archivo nuevo >512 MB va a esa lista**.
- **Parquet cache**: DuckDB-WASM en browser cachea parquets. Usar cache-buster `?v=N` en `getParquetUrl()` al cambiar schema (incrementar N en config.ts).
- **Departamentos AR = radios INDEC 2022** (desde 2-nov-2026): `ar_dept_boundaries.json` son los radios disueltos por depto (`pipeline/build_ar_dept_boundaries_indec.py`); GADM ya no se usa en AR (`rebuild_ar_boundaries.py`/`extend_ar_boundaries.py` retirados). Capas censales: celda → depto por mayoría de área de radios; satelitales de Ctes/Chaco/Fsa: `h3_admin_crosswalk.parquet` = centro de la celda en los radios (`pipeline/repartir_deptos_indec.py`, verificado con `verificar_reparto_indec.py`; Chaco tiene 25 deptos con «2 de Abril»). Respaldo de los archivos GADM: `C:\Users\ant\r2-dept-gadm-backup-2026-11`.
- **Ficha del hexágono: borrada de la web** (2026-11-04). Sus datos están en el Hexoma (`v_hexoma`, `v_hexoma_rangos`; ficha local con `pipeline/hexoma/hexoma_ficha.py`). No reponerla en la web sin pedido del autor.
- **Índice de departamento por celda** (`pipeline/output/hexoma/ficha_link_index.json`, insumo de la tabla `ficha_enlace` del Hexoma): celdas cuyo archivo de depto no es el del polígono que contiene su centro. Regenerar con `python pipeline/build_ficha_link_index.py` tras cambiar cualquier partición por departamento.

## Valores, no puntajes (CRÍTICO)
- **Sin goalposts, nunca** (decisión del autor, 2026-11-04): ni la palabra en textos ni el método. Las capas publican y pintan valores físicos (%, µg/m³, %/año, min, km) o tasas censales; no hay normalización a escala fija, puntaje compuesto 0–100 ni `--mode comparable|local`. Se borraron `goalposts.json`, `compute_goalposts.py`, `compute_satellite_scores.py`, `compute_overture_scores.py`, carbono y actividad productiva, y `flood_risk_score`/`score` de los productores (pm25, Hansen, accesibilidad, inundación). No proponer reintroducirlos.
- Excepciones que no son puntajes: aptitud forestal (probabilidad del SDM × 100) y las clases de aptitud agrícola (FAO S1–N). Las escalas de color fijas (PM2.5 regional 7–20 µg/m³, estiramiento P2–P98 por territorio, EUDR 0–40 %) sólo eligen colores sobre el valor crudo.
- `comparable: true` en `ANALYSIS_REGISTRY` = misma definición física en todos los territorios que cubre; decide el grupo del menú, el comparador entre territorios y el eje Y del bivariado.
- Las columnas `score*` que quedan en R2 (pm25, deforestación, inundación) no las lee nadie y desaparecen cuando cada productor vuelve a correr.
- `pipeline/scoring.py` conserva sólo diagnósticos (correlación, KMO, PCA) y la media geométrica; hoy sólo lo importa `process_air_quality_to_h3.py` (capa vieja, sin uso en la web).

## Flood Risk Layer
- **Lo que pinta el mapa**: exposición por relieve (HAND < 5 m, MERIT Hydro) con Google Open Buildings: pestañas % de edificios / edificios / % de tierra / edificios nuevos 2016–2023 (`pipeline/build_flood_rank.py`, columnas hex `low_bld_pct`, `low_bld_n`, `low_land_pct`, `low_bld_new_n`; cada pestaña pinta la suya). En la llanura chaqueña (ranking saturado) pinta `jrc_occurrence` (JRC GSW 1984–2021). Sentinel-1 = extensión actual (`flood_extent_pct`). Sin score compuesto (`flood_risk_score` fuera de la UI en jun-2026 y del pipeline el 4-nov-2026).
- **Parquet columns**: `h3index`, `jrc_occurrence`, `jrc_recurrence`, `jrc_seasonality`, `flood_extent_pct`, `low_bld_pct`, `low_bld_n`, `low_land_pct`, `low_bld_new_n`
- **Pipeline**: `python pipeline/run_flood_update.py --skip-gee` (con rasters locales)

## Deployment
- **Production branch:** `main` — el branch local es `main` y trackea `origin/main` (NO existe `master` local). Push con `git push origin main` (o `git push`). Mirror público AGPL-3.0: `git push nealab main:master`.
- **Deploy:** `npm run deploy` (build + wrangler pages deploy --project-name neahub --branch main)
- **R2 uploads:** SIEMPRE usar `--remote` flag
- **CORS R2:** PascalCase (AllowedOrigins, AllowedMethods) — como en `cors-rules.json`
- **URLs:** https://www.spatia.ar / https://neahub.pages.dev
- **NUNCA deployar a preview branch salvo que se pida explícitamente.**

## Director de Spatia (`director/`, desde 3-nov-2026)
- Mantenimiento automático quincenal (`.github/workflows/director.yml`, domingos de semanas ISO pares 03:00 ART): sondas Playwright sobre www.spatia.ar → juez (Claude, suscripción del autor vía secret `CLAUDE_CODE_OAUTH_TOKEN`) → arreglos en `src/`/`static/` → `director/guardia.py` + re-sondeo del build → push a `main` + deploy + re-sondeo de producción (revierte solo si empeora) → informe en un issue con label `director` (preguntas al autor incluidas; se contestan con un comentario).
- Sondas (14 jobs en paralelo, ~500 min de Actions por corrida): `mapa.py` (territorio × capa × pestaña, en 8 partes), `textos.py` (páginas y paneles es/en/pt/gn), `estatico.py` (registros, colores en textos, esquemas R2, fallos de CI), `extras.py --solo temporal|oscuro|movil` (línea de base/cambio y escala regional, tema oscuro, 390×844).
- Revisor de coherencia: 3 capas por corrida (las que cambiaron primero, después las de revisión más vieja; estado en `director/coherencia.yaml`), texto es/en/pt contra config y contra los parquets (`director/consulta.py`, sólo lectura).
- Reglas: `director/reglamento.md` + `director/reglas.yaml` (no los edita el director; cambiarlos es decisión del autor). Respuestas del autor: `director/decisiones.md`. Falsos positivos: `director/ignorar.yaml`. Modelo y esfuerzo: `director/config.yaml`.
- Correr a mano: `gh workflow run director.yml -f modo=diagnostico` (sólo informa) o `-f modo=completo`. Sondas locales: `python director/sondas/mapa.py https://www.spatia.ar out --territorios misiones --capas flood_risk`.
- Gancho de sondas: con `localStorage.spatia_sonda=1` la página expone `window.__spatia` (`+page.svelte`, listener `map-ready`). No quitarlo.
- El director commitea en `main`: la copia local se pone al día sola al abrir sesión (`director/sincronizar.sh`, hook SessionStart local).
- Cloudflare desafía el user-agent `HeadlessChrome` y las descargas hechas por el proxy de Playwright desde el runner: las sondas usan UA normal y el build local se sirve en `localhost:5173` (ya en `cors-rules.json`), sin proxy.

## Database
- PostgreSQL/PostGIS local (db: `posadas`)
- D1 schema in `db/schema.sql`, seed auto-generated by `db/import_d1.py`
