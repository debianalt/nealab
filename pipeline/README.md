# Pipelines

This directory holds two unrelated pipelines. The **flood risk** one is documented
below; the **EUDR / MapBiomas** analysis scripts are indexed first, because they
back figures published in a report and each one has to be traceable to the number
it produces.

## EUDR — which script produces which published figure

### Reproducible with only an Earth Engine account

These three read **only public Earth Engine assets** (MapBiomas Argentina
Collection 2 and Hansen GFC) plus `src/lib/data/eudr_provinces_boundary.json`,
which is versioned here. No local raster and no credential beyond a registered
Earth Engine account is needed, so a third party can reproduce the numbers.

| Script | Published figures it produces |
|---|---|
| `eudr_split_cosecha_nativo.py` | Split of post-cutoff loss into harvest vs native conversion (22.8 % overall; 91.2 % Corrientes, 28.9 % Misiones), stability against the final year of the series (22.9 % to 2024), native→plantation conversion (34,907 ha) and 2020 plantation area (864,957 ha) |
| `eudr_subcategorias_nativo.py` | Breakdown of the 539,741 ha lost over native cover into closed forest (97.2 %) and open/flooded forest, plus 2020 native cover composition |
| `eudr_validacion_modulo_mapbiomas.py` | Hansen vs MapBiomas Vegetation Loss module contrast, 2021–2024, per province |

```bash
python pipeline/eudr_split_cosecha_nativo.py
python pipeline/eudr_subcategorias_nativo.py
python pipeline/eudr_validacion_modulo_mapbiomas.py
```

A fourth one needs no Earth Engine account at all: it reads the served layer.

| Script | Published figures it produces |
|---|---|
| `orden_sin_tercer_termino.py` | Weight of the third score term (69.0 % of the mean score against a nominal 10 %) and the provincial ordering once it is excluded: Misiones 4th→2nd, Corrientes 2nd→4th, Formosa 1st in both |

```bash
python pipeline/orden_sin_tercer_termino.py
```

It prefers the local res-7 parquet and falls back to downloading
`cdn.spatia.ar/data/eudr/eudr_deforestation.parquet` — the same layer the viewer
reads — so it runs from a clean clone. Both paths were checked to give identical
provincial means. It refuses to report anything unless the published `risk_score`
is first reproduced from the weights in `config_eudr.py`.

### Requires local intermediates (NOT reproducible from a clean clone)

These two operate on rasters and parquets produced earlier in the pipeline and
living under `pipeline/output/`, which is gitignored. They are listed for
traceability, not as a one-command reproduction path.

| Script | What it does | Inputs it needs |
|---|---|---|
| `aggregate_fire_native.py` | Burned area restricted to native woody cover (`fire_native_post_2020_pct`) | `eudr_deforestation_combined_2025*.tif` shards, `mapbiomas_col2_nea.tif`, `pipeline/data/ar_eudr_provinces.geojson` (derivable from GADM 4.1 — see `.github/workflows/eudr-update.yml`) |
| `apply_fire_native_to_risk.py` | Substitutes filtered fire for raw fire inside `risk_score` | the res-7/res-9 parquets under `pipeline/output/eudr/` |

> The fire filter is currently a **manual local step**: it is not invoked by
> `run_eudr_update.py` nor by the CI workflow. The monthly cron in
> `.github/workflows/eudr-update.yml` is suspended for that reason — an
> automatic run would republish the layers with raw fire and contradict the
> published methodology. See the comment at the top of that workflow.

### Shared constants

Hansen vintage, MapBiomas assets, class codes and boundary path live in
`config_eudr.py`. Import them rather than redefining them: several of the
published numbers depend on using the *same* boundary file and the same tie-break
rule for border cells, and a different one shifted a result by 98 percentage
points once.

On class names: this analysis uses the **Argentine** Collection 2 legend, where
classes 3, 4 and 6 are *bosques cerrados*, *bosques abiertos* and *bosques
inundables* — all three inside the "Bosques" category — and class 5 does not
exist. The Brazilian names ("formação florestal", "formação savânica") do not
apply to Argentine products.

---

# Flood Risk Pipeline

Automated pipeline: Sentinel-1 SAR → GEE export → GCS → H3 zonal stats → R2.

## Quick Start

```bash
# Dry run (show steps without executing)
python pipeline/run_flood_update.py --dry-run

# Full run (current extent only, ~20min)
python pipeline/run_flood_update.py --current

# Full run with historical recurrence (~4h)
python pipeline/run_flood_update.py --historical

# Reprocess local GeoTIFFs (skip GEE/GCS)
python pipeline/run_flood_update.py --skip-gee
```

## Pipeline Steps

| Step | Script | Description |
|------|--------|-------------|
| 1 | `gee_flood_detection.py` | Authenticate to GEE |
| 2 | `gee_flood_detection.py` | Launch S1 flood export to GCS |
| 3 | `gee_flood_detection.py` | Poll GEE tasks (60s interval, 4h timeout) |
| 4 | `download_gcs.py` | Download GeoTIFFs from `gs://spatia-satellite/flood/` |
| 5 | `process_to_h3.py` | H3 zonal stats → `hex_flood_risk.parquet` |
| 6 | `upload_to_r2.py` | Upload parquet to R2 (`neahub-public`) |

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GEE_SERVICE_ACCOUNT_KEY` | GEE service account JSON key (path or string) | For GEE steps |
| `CLOUDFLARE_API_TOKEN` | Cloudflare API token with R2 write access | For R2 upload |

## GitHub Actions

Workflow: `.github/workflows/flood-update.yml`

- **Schedule**: 1st of each month, 06:00 UTC
- **Manual trigger**: Actions tab → "Update Flood Risk Layer" → Run workflow
- **Secrets needed**: `GEE_SERVICE_ACCOUNT_KEY`, `CLOUDFLARE_API_TOKEN`

### Pushing the workflow file

GitHub requires the `workflow` OAuth scope to push workflow files:

```bash
gh auth refresh -s workflow
git push origin master:main
```

## Troubleshooting

- **GEE timeout**: Increase `--days` to get more S1 images, or check [GEE task monitor](https://code.earthengine.google.com/tasks)
- **No GeoTIFFs in GCS**: Verify the service account has `storage.objects.list` on `spatia-satellite`
- **R2 upload fails**: Check `CLOUDFLARE_API_TOKEN` has R2 write permissions; ensure wrangler is installed
- **No hexagon grid**: Run `python pipeline/generate_h3_grid.py` first

## Individual Scripts

Each script can also run standalone:

```bash
# GEE export only
python pipeline/gee_flood_detection.py --current --wait

# Download from GCS only
python pipeline/download_gcs.py

# H3 processing only
python pipeline/process_to_h3.py --recurrence output/recurrence.tif --current output/current.tif --grid output/hexagons.geojson

# R2 upload only
python pipeline/upload_to_r2.py --file pipeline/output/hex_flood_risk.parquet --dest data/hex_flood_risk.parquet
```
