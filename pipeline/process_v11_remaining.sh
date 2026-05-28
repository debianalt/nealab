#!/usr/bin/env bash
# Process remaining v1.1 layers for one territory: carbon_stock, pm25_drivers,
# productive_activity, deforestation_dynamics, climate_vulnerability.
# territorial_scores is handled by a separate Overture ingest+scoring run.
#
# Usage: bash pipeline/process_v11_remaining.sh <territory>
# Designed to be run in parallel across territories.

set -e
T=$1
if [ -z "$T" ]; then echo "usage: $0 <territory>"; exit 1; fi
LOG="pipeline/output/$T/v11_remaining.log"
mkdir -p pipeline/output/$T
exec > >(tee -a "$LOG") 2>&1
echo "=== $(date) | $T | start ==="

# Hansen H3 panel — input for productive_activity, deforestation_dynamics
echo "--- process_hansen_to_h3 ---"
python pipeline/process_hansen_to_h3.py --territory $T --mode local || echo "  WARN hansen failed"

# Carbon stock — raster already downloaded
echo "--- process_carbon_to_h3 ---"
python pipeline/process_carbon_to_h3.py --territory $T --mode local || echo "  WARN carbon failed"

# PM2.5 annual panel (input for pm25_drivers)
echo "--- process_pm25_annual_to_h3 ---"
python pipeline/process_pm25_annual_to_h3.py --territory $T || echo "  WARN pm25_annual failed"

# PM2.5 drivers composite
echo "--- compute_pm25_drivers ---"
python pipeline/compute_pm25_drivers.py --territory $T --mode local || echo "  WARN pm25_drivers failed"

# Productive activity (needs activity rasters + hansen_h3)
echo "--- process_activity_to_h3 ---"
python pipeline/process_activity_to_h3.py --territory $T --mode local || echo "  WARN activity failed"
echo "--- compute_productive_activity ---"
python pipeline/compute_productive_activity.py --territory $T --mode local || echo "  WARN prod_activity failed"

# Deforestation dynamics (composite from hansen)
echo "--- compute_deforestation_layer ---"
python pipeline/compute_deforestation_layer.py --territory $T --mode local || echo "  WARN deforest failed"

# Soil water — process raster
echo "--- process_raster_to_h3 soil_water ---"
# soil_water raster may already produce parquet — check naming. For now skip if no script.

# Climate vulnerability — composite from other layers (needs them to exist)
echo "--- compute_climate_vulnerability ---"
python pipeline/compute_climate_vulnerability.py --territory $T --mode local || echo "  WARN clim_vuln failed"

echo "=== $(date) | $T | done ==="
