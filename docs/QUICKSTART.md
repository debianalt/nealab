# Quick Start: Running Spatia with Example Data

This guide walks through running the Spatia pipeline and frontend with minimal setup.

## Prerequisites

- Python 3.12+
- Node.js 22+
- A [Google Earth Engine](https://earthengine.google.com/) account (free for research)

## 1. Install dependencies

```bash
# Python pipeline
pip install -r pipeline/requirements.txt

# Frontend
npm install
```

## 2. Authenticate to Google Earth Engine

```bash
earthengine authenticate
```

This stores credentials at `~/.config/earthengine/credentials`.

## 3. Generate the H3 grid

All pipeline scripts require the hexagonal grid as input:

```bash
python pipeline/generate_h3_grid.py
```

This creates `pipeline/output/hexagons.parquet` — the H3 resolution 9 grid for Misiones (~320K hexagons).

## 4. Run a single analysis (flood risk)

The flood risk pipeline is the simplest end-to-end example:

```bash
# Full run: GEE export → download → H3 zonal stats → parquet
python pipeline/run_flood_update.py --current

# Or, if you already have GeoTIFFs locally:
python pipeline/run_flood_update.py --skip-gee
```

Output: `pipeline/output/hex_flood_risk.parquet`

## 5. Run satellite composite scores

To compute all 16 satellite-derived analyses:

```bash
# Export rasters from GEE (takes ~2-4 hours)
python pipeline/gee_export_analysis.py --wait

# Download from Google Cloud Storage
python pipeline/download_gcs.py

# Compute H3 zonal stats + PCA-validated scores
python pipeline/compute_satellite_scores.py --diagnostics
```

The `--diagnostics` flag outputs KMO/Bartlett/PCA results as JSON files in `pipeline/output/`.

## 6. Run the frontend locally

```bash
npm run dev
```

The frontend at `localhost:5173` reads parquet files from the production R2 bucket by default. To use local parquets, modify the proxy target in `vite.config.ts`.

## Data Sources

All input data is obtained from public sources:

| Source | How to Access |
|--------|--------------|
| Sentinel-1/2, MODIS, Landsat | Via Google Earth Engine (free research account) |
| JRC Global Surface Water | Via GEE: `JRC/GSW1_4/GlobalSurfaceWater` |
| Hansen Global Forest Change | Via GEE: `UMD/hansen/global_forest_change_2023_v1_12` |
| SoilGrids v2.0 | Via GEE: `projects/soilgrids-isric` |
| CHIRPS v2.0 | Via GEE: `UCSB-CHG/CHIRPS/DAILY` |
| ERA5-Land | Via GEE: `ECMWF/ERA5_LAND/MONTHLY_AGGR` |
| VIIRS Nighttime Lights | Via GEE: `NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG` |
| Overture Maps | Via DuckDB: `read_parquet('s3://overturemaps-us-west-2/...')` |
| INDEC Census 2022 | https://www.indec.gob.ar/indec/web/Institucional-Indec-BasesDeDatos-6 |
| MapBiomas | Via GEE: `projects/mapbiomas-public/assets/argentina/collection2` |

## Pipeline Architecture

```
1. gee_export_*.py     → Export rasters from GEE to Google Cloud Storage
2. download_gcs.py     → Download GeoTIFFs to pipeline/output/
3. process_*_to_h3.py  → Zonal statistics on H3 hexagonal grid
4. compute_*.py        → PCA-validated composite scores
5. upload_to_r2.py     → Upload parquets to Cloudflare R2
```

Each step can be run independently. Orchestrator scripts (`run_*.py`) chain them together.

## Validating Scores

Run PCA diagnostics for any analysis:

```bash
python pipeline/compute_satellite_scores.py --diagnostics
```

Check `pipeline/output/*_diagnostics.json` for:
- **KMO statistic** (> 0.50 = adequate sampling)
- **Bartlett's test** (p < 0.05 = variables are correlated)
- **PCA loadings** and variance explained
- **Correlation matrix** with flagged high-correlation pairs
