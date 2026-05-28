#!/usr/bin/env bash
# Finalize v1.1 layer rollout per territory:
#   1. split_by_admin (per-dept parquets + bundled summary JSON in src/lib/data/)
#   2. R2 upload (global parquet + sat_dpto/* + overture_scores)
#
# Run AFTER process_v11_remaining.sh + compute_overture_scores have populated
# pipeline/output/<territory>/.
#
# Usage: bash pipeline/finalize_v11_layers.sh <territory>
# Parallelizable across territories.

set -e
T=$1
if [ -z "$T" ]; then echo "usage: $0 <territory>"; exit 1; fi
LOG="pipeline/output/$T/finalize_v11.log"
exec > >(tee -a "$LOG") 2>&1
echo "=== $(date) | $T | finalize start ==="

# Capas satélite que se splittean vía split_by_admin (lee sat_<id>.parquet)
SAT_LAYERS="carbon_stock,pm25_drivers,productive_activity,deforestation_dynamics,climate_vulnerability,soil_water"

# 0) soil_water (process_raster_to_h3 — si v11_remaining no lo cubrió)
echo "--- soil_water via process_raster_to_h3 ---"
if [ ! -f "pipeline/output/$T/sat_soil_water.parquet" ]; then
  python pipeline/process_raster_to_h3.py --territory $T --analysis soil_water --mode local || echo "  WARN soil_water failed"
fi

# 1) Split satellite parquets — writes bundled summary JSONs to src/lib/data/<t>_sat_<id>_summary.json
echo "--- split_by_admin (sat layers) ---"
python pipeline/split_by_admin.py --territory $T --only $SAT_LAYERS || echo "  WARN sat split partial"

# 2) Split Overture territorial scores (different parquet name + summary path)
echo "--- split_scores_by_dpto (territorial_scores) ---"
python pipeline/split_scores_by_dpto.py --territory $T || echo "  WARN scores split failed"

# 3) R2 upload — global parquets
echo "--- R2 upload globals ---"
OUT="pipeline/output/$T"
ALL_LAYERS="carbon_stock pm25_drivers productive_activity deforestation_dynamics climate_vulnerability soil_water"
for id in $ALL_LAYERS; do
  local_file="$OUT/sat_${id}.parquet"
  if [ -f "$local_file" ]; then
    echo "  -> sat_${id}.parquet"
    npx wrangler r2 object put "neahub/data/${T}/sat_${id}.parquet" --file "$local_file" --remote 2>&1 | tail -1
  else
    echo "  SKIP $id"
  fi
done
if [ -f "$OUT/overture_scores.parquet" ]; then
  echo "  -> overture_scores.parquet"
  npx wrangler r2 object put "neahub/data/${T}/overture_scores.parquet" --file "$OUT/overture_scores.parquet" --remote 2>&1 | tail -1
fi

echo "--- R2 upload per-dept ---"
DPTO_DIR="$OUT/sat_dpto"
if [ -d "$DPTO_DIR" ]; then
  uploaded=0
  for f in "$DPTO_DIR"/*.parquet; do
    [ -f "$f" ] || continue
    name=$(basename "$f")
    npx wrangler r2 object put "neahub/data/${T}/sat_dpto/${name}" --file "$f" --remote > /dev/null 2>&1
    uploaded=$((uploaded + 1))
  done
  echo "  uploaded $uploaded per-dept parquets"
fi

echo "=== $(date) | $T | finalize done ==="
