# nealab — análisis geoespacial (NEA y regiones transfronterizas)

## Tech Stack
- SvelteKit 5 + adapter-static
- Svelte 5 (runes syntax: `$state`, `$derived`, `$effect`)
- TypeScript
- TailwindCSS 4
- Cloudflare: Pages, Workers, R2, D1
- DuckDB-WASM (parquet queries from R2)
- MapLibre GL 6 + PMTiles. Importar siempre `import * as maplibregl from '$lib/maplibre'`, nunca `'maplibre-gl'` directo: la 6 es sólo ESM y ese módulo registra el worker (`setWorkerUrl`); sin él el mapa no carga. Filtros siempre como expresiones (`['geometry-type']`, `['get', …]`), no la sintaxis legacy (`'$type'`).
- Claude SDK (Haiku) via Pages Function proxy (`functions/api/chat.ts`)

## Project Structure
```
src/                    # SvelteKit app
  lib/config.ts         # PARQUETS URLs, HEX_LAYER_REGISTRY, ANALYSIS_REGISTRY
  lib/stores/           # hex.svelte.ts, map.svelte.ts, i18n.svelte.ts, duckdb.ts
  lib/components/analyses/  # FloodRiskAnalysis, RealEstate, TerritorialProfile
  routes/+page.svelte   # Main page (map, hex click, analysis choropleth)
functions/api/          # Cloudflare Pages Functions (chat proxy)
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
- Antes de editar archivos compartidos (chat, comparison, map components), listar qué funciona actualmente y verificar que sigue funcionando después de los cambios.
- Para bugs de radio/click: simular el escenario de first-click.
- Para chat flows: verificar cadena completa (user input → LLM → tool call → map action → response).

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
- **Índice del enlace `?h3=`** (`src/lib/data/ficha_link_index.json`): los archivos por depto asignan celdas por mayoría de área de radios, no por los polígonos de `ar_dept_boundaries.json`; el índice dice qué archivo tiene cada celda donde difieren (~23K). Regenerar con `python pipeline/build_ficha_link_index.py` tras cambiar cualquier partición por departamento.

## Scoring Methodology
- **All composite scores** use PCA-validated geometric mean (OECD Handbook / UNDP HDI style)
- **Pipeline**: percentile rank → correlation diagnostics → KMO/Bartlett → PCA (varimax) → select variables (drop |r|>0.70) → geometric mean (floor=1.0)
- **Sin pesos**: los componentes retenidos pesan igual. No hay promedio ponderado en ningún score (el modo `--legacy` y los pesos por componente se borraron el 12-oct-2026).
- **Shared module**: `pipeline/scoring.py` — all diagnostics and scoring functions
- **Diagnostics**: `--diagnostics` flag on `compute_satellite_scores.py` emits `*_diagnostics.json` per analysis
- **Scripts**: `compute_satellite_scores.py` = 4 capas censales (service_deprivation, health_access, education_capital, education_flow). `process_raster_to_h3.py` / `gee_reduce_analysis.py` = agri_potential.

## Scoring Modes — Dual Architecture (CRITICAL — do not infer otherwise)
nealab has TWO scoring modes coexisting. Both are deployed. Do NOT propose "adding a dual mode" — it exists.

- **`--mode comparable`**: fixed goalpost P2/P98 normalization → cross-territory comparable. Score 60 in Misiones = 60 in Itapúa. PCA variable selection LOCKED from `goalposts.json:pca_variable_selection`. Used for the 23 comparable satellite layers.
- **`--mode local`**: percentile rank within territory (default historical). Used for census-based layers whose schemas differ across countries (INDEC vs DGEEC).

UI exposes both groups in `src/lib/components/AnalysisMenu.svelte:39-83`:
- "↔ Comparables entre territorios" → layers with `comparable: true` in `ANALYSIS_REGISTRY`
- "Solo <territorio>" → layers with `comparable: false`

**Tier 1 (physical, invariant)**: c_recurrence, c_occurrence, c_extent, c_treecover, c_water_stress, c_ph_optimal. Fractions/percentages/bounded ratios. NEVER recalibrated.

**Tier 2 frozen v1** (commit `58e225f`): P2/P98 over pooled Mis+Ita reference universe. Intra-ecoregion additions DO NOT recompute. Bump v(n+1) only when leaving the ecoregion OR on deliberate methodological revision (recomputes ALL territories + full history + documents the break).

**`pipeline/compute_goalposts.py`**: recalculates Tier 2 bounds from pooled territory data. Run only on a bump. Tier 1 entries never touched. Re-runs PCA variable selection on pooled normalized data.

## Flood Risk Layer
- **Data sources**: JRC Global Surface Water v1.4 (Landsat 1984–2021) + Sentinel-1 SAR (current extent)
- **Parquet columns**: `h3index`, `jrc_occurrence` (0-100%), `jrc_recurrence` (0-100%), `jrc_seasonality` (0-12), `flood_extent_pct` (0-100%), `flood_risk_score` (0-100)
- **Score formula**: Geometric mean of PCA-validated components (occurrence, recurrence, extent)
- **Pipeline**: `python pipeline/run_flood_update.py --skip-gee` (con rasters locales)

## Deployment
- **Production branch:** `main` — el branch local es `main` y trackea `origin/main` (NO existe `master` local). Push con `git push origin main` (o `git push`). Mirror público GPL: `git push nealab main:master`.
- **Deploy:** `npm run deploy` (build + wrangler pages deploy --project-name neahub --branch main)
- **R2 uploads:** SIEMPRE usar `--remote` flag
- **CORS R2:** PascalCase (AllowedOrigins, AllowedMethods) — como en `cors-rules.json`
- **URLs:** https://www.spatia.ar / https://neahub.pages.dev
- **NUNCA deployar a preview branch salvo que se pida explícitamente.**

## Database
- PostgreSQL/PostGIS local (db: `posadas`)
- D1 schema in `db/schema.sql`, seed auto-generated by `db/import_d1.py`
