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

# Capas que deberíamos haber generado en process_v11_remaining + overture
LAYERS="carbon_stock,pm25_drivers,productive_activity,deforestation_dynamics,climate_vulnerability,territorial_scores"

# 1) Split by admin — also writes bundled summary JSONs to src/lib/data/<t>_sat_<id>_summary.json
echo "--- split_by_admin ---"
python pipeline/split_by_admin.py --territory $T --only $LAYERS || echo "  WARN split partial"

# 2) R2 upload
echo "--- R2 upload globals ---"
OUT="pipeline/output/$T"
for id in $(echo $LAYERS | tr ',' ' '); do
  case $id in
    territorial_scores) local_file="$OUT/overture_scores.parquet"; r2_name="overture_scores.parquet" ;;
    *) local_file="$OUT/sat_${id}.parquet"; r2_name="sat_${id}.parquet" ;;
  esac
  if [ -f "$local_file" ]; then
    echo "  -> $r2_name"
    npx wrangler r2 object put "neahub/data/${T}/${r2_name}" --file "$local_file" --remote 2>&1 | tail -2
  else
    echo "  SKIP $id (no $local_file)"
  fi
done

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
