#!/usr/bin/env bash
# Overnight orchestrator — runs autonomously after the user sleeps.
#
# Sequence:
#   1. Wait for the soil_water re-run (process_raster_to_h3 --mode comparable
#      × 9 territories, 3-batch parallel script `bsxkdj6la`) to finish.
#   2. Re-split soil_water by admin for all 9 territories. The re-runs
#      overwrote the global sat_soil_water.parquet but the sat_dpto/* files
#      still carry the old (mode local) scores.
#   3. Optimize soil_water per-dpto (sort + ZSTD + 50K-row row groups). The
#      parquets only have score + 4 component cols + h3index, so --drop-temporal
#      is a no-op but harmless.
#   4. Upload soil_water globals + per-dpto. Use the REST-API helper for any
#      global > 100 MB (RS soil_water is the only candidate).
#   5. Wait for the other uploads in flight (productive_activity,
#      climate_vulnerability, 5 non-temporal, 3 temporal) by polling their
#      log files for the per-territory completion marker.
#   6. Re-optimize and re-upload climate_vulnerability (the earlier rescore
#      went out without sort/ZSTD).
#   7. Bump every layer's cache buster in src/lib/config.ts so DuckDB-WASM
#      pulls the fresh, smaller parquets.
#   8. Commit, push to both remotes, deploy to Cloudflare Pages.
#
# Output:
#   pipeline/output/overnight_finish.log
#
# Run:  bash pipeline/overnight_finish.sh
# Designed to run with run_in_background so the user can sleep.

set -u
exec > >(tee -a "pipeline/output/overnight_finish.log") 2>&1

cd "$(dirname "$0")/.." || exit 1

TS() { date '+%H:%M:%S'; }
say()  { echo "[$(TS)] $*"; }
hdr()  { echo; echo "============================================================"; echo "[$(TS)]   $*"; echo "============================================================"; }

TOKEN=$(grep -E '^oauth_token =' "$HOME/.wrangler/config/default.toml" | sed 's/oauth_token = "\(.*\)"/\1/')
ACCT="85b5bfbd1b86ba164b9443b87eefa3b8"
LARGE_BYTES=100000000  # 100 MB threshold for libuv-bypass REST API

upload_one() {
  # upload_one <r2_key> <local_file>
  local key=$1 src=$2
  [ -f "$src" ] || { echo "SKIP missing $src"; return; }
  local sz; sz=$(stat -c%s "$src")
  if [ "$sz" -gt $LARGE_BYTES ]; then
    curl -s -X PUT "https://api.cloudflare.com/client/v4/accounts/$ACCT/r2/buckets/neahub/objects/$key" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/octet-stream" \
      --data-binary "@$src" > /dev/null
  else
    npx wrangler r2 object put "neahub/$key" --file "$src" --remote > /dev/null 2>&1
  fi
}

upload_dpto_layer() {
  # upload_dpto_layer <territory> <layer>  → parallel batch upload of one
  # per-dpto layer family for one territory. 8-way concurrency, errors swallowed.
  local t=$1 layer=$2 pref=""
  [ "$t" != "misiones" ] && pref="${t}/"
  local sd="pipeline/output/${pref%/}/sat_dpto"
  [ "$t" = "misiones" ] && sd="pipeline/output/sat_dpto"
  local cnt=0
  for f in "$sd"/sat_${layer}_*.parquet; do
    [ -f "$f" ] || continue
    local name; name=$(basename "$f")
    npx wrangler r2 object put "neahub/data/${pref}sat_dpto/$name" --file "$f" --remote > /dev/null 2>&1 &
    while [ "$(jobs -r | wc -l)" -ge 8 ]; do sleep 0.1; done
    cnt=$((cnt + 1))
  done
  wait
  echo "$t $layer per-dpto: $cnt uploaded"
}

# ── 1. Wait for soil_water re-run ──────────────────────────────────────
hdr "1. Wait for soil_water re-run (9 territories, mode comparable)"
until grep -q "all done" pipeline/output/soil_water_rerun.log 2>/dev/null; do
  sleep 120
  say "still waiting... last territory marker: $(grep -E '^batch|all done' pipeline/output/soil_water_rerun.log 2>/dev/null | tail -1)"
done
say "soil_water re-run finished"

# ── 2. Re-split soil_water by admin ───────────────────────────────────
hdr "2. split_by_admin --only soil_water for 9 territories"
for t in misiones corrientes itapua_py alto_parana_py chaco formosa parana_br santa_catarina_br rio_grande_sul_br; do
  python pipeline/split_by_admin.py --territory "$t" --only soil_water 2>&1 | tail -2
done

# ── 3. Optimize soil_water per-dpto ───────────────────────────────────
hdr "3. optimize_parquets soil_water (sort + ZSTD + 50K row groups)"
python pipeline/optimize_parquets.py --territory all --layer soil_water --drop-temporal 2>&1 | tail -5

# ── 4. Upload soil_water globals + per-dpto ───────────────────────────
hdr "4. Upload soil_water globals + per-dpto for 9 territories"
for t in misiones corrientes itapua_py alto_parana_py chaco formosa parana_br santa_catarina_br rio_grande_sul_br; do
  pref=""
  [ "$t" != "misiones" ] && pref="${t}/"
  src="pipeline/output/${pref%/}/sat_soil_water.parquet"
  [ "$t" = "misiones" ] && src="pipeline/output/sat_soil_water.parquet"
  upload_one "data/${pref}sat_soil_water.parquet" "$src"
  upload_dpto_layer "$t" soil_water
done
say "soil_water uploads done"

# ── 5. Wait for other uploads in flight ───────────────────────────────
hdr "5. Wait for in-flight uploads to finish (PA / CV / 5 non-temporal / 3 temporal)"
wait_for_log_complete() {
  local log=$1 marker=$2 label=$3
  until grep -q "$marker" "$log" 2>/dev/null; do
    sleep 120
    say "  $label: waiting (last line: $(tail -1 "$log" 2>/dev/null))"
  done
  say "  $label: done"
}

# productive_activity log finishes when rio_grande_sul_br prints its tally
wait_for_log_complete pipeline/output/opt_pa_upload.log "rio_grande_sul_br prod_act" "productive_activity upload"
# climate_vulnerability: rio_grande_sul_br tally
wait_for_log_complete pipeline/output/cv_upload.log "rio_grande_sul_br cv per-dpto" "climate_vulnerability upload"
# 5 non-temporal: last layer is forest_health
wait_for_log_complete pipeline/output/opt_5layers_upload.log "forest_health all uploaded" "5 non-temporal upload"
# 3 temporal: last layer is deforestation_dynamics
wait_for_log_complete pipeline/output/opt_3temporal_upload.log "deforestation_dynamics all uploaded" "3 temporal upload"

# ── 6. Re-optimize + upload climate_vulnerability ─────────────────────
hdr "6. Optimize climate_vulnerability per-dpto + re-upload"
python pipeline/optimize_parquets.py --territory all --layer climate_vulnerability 2>&1 | tail -5
for t in misiones corrientes itapua_py alto_parana_py chaco formosa parana_br santa_catarina_br rio_grande_sul_br; do
  upload_dpto_layer "$t" climate_vulnerability
done
say "climate_vulnerability re-upload done"

# ── 7. Bump cache busters ─────────────────────────────────────────────
hdr "7. Bump cache busters in src/lib/config.ts for all affected layers"
bump() {
  local key=$1
  local cur; cur=$(grep -E "${key}:\s*'\?v=" src/lib/config.ts | head -1 | grep -oE "v=[0-9]+" | tr -d v=)
  if [ -z "$cur" ]; then echo "  skip $key (no entry)"; return; fi
  local nxt=$((cur + 1))
  sed -i "s/${key}: '?v=${cur}'/${key}: '?v=${nxt}'/" src/lib/config.ts
  echo "  $key v${cur} -> v${nxt}"
}
for key in sat_carbon_stock sat_pm25_drivers sat_productive_activity \
           sat_climate_vulnerability sat_soil_water sat_deforestation_dynamics \
           sat_environmental_risk sat_climate_comfort sat_green_capital \
           sat_change_pressure sat_forest_health overture_scores hex_flood_risk; do
  bump "$key"
done

# ── 8. Commit, push, deploy ───────────────────────────────────────────
hdr "8. Build, commit, push, deploy"
npm run build 2>&1 | tail -3
git add src/lib/config.ts pipeline/overnight_finish.sh
git commit -m "perf+fase-b: soil_water comparable + per-dpto sort/ZSTD across 13 layers

Wraps up the overnight Fase B + perf push the user kicked off at 2026-05-29 ~00:00 UTC.

Soil_water:
  - process_raster_to_h3 --mode comparable re-run across all 9 territories
    using the new c_soil_moisture / c_dry_season / c_actual_et goalposts
    (c_precipitation kept from frozen v1.1). Cross-territory means now
    track biome reality (Mis ~89, Cha/For semi-arid ~30).
  - split_by_admin re-split, per-dpto sort + ZSTD + 50K row groups.

Climate_vulnerability:
  - Re-uploaded the earlier 7-component (4 v1.1 sat × 3 always-present
    sat) rescore with sort + ZSTD on top, dropping the misalignment
    between Mis/Cor (6 components incl. AR census) and the others (4).

Performance pass — all 11 sat_* + flood + scores per-dpto parquets now
sorted by h3index, ZSTD-9 compressed, in 50 K-row row groups:

  Layer                   before   after   ratio
  productive_activity     633 MB   496 MB  78 %  (no col drop, temporal preserved)
  environmental_risk      137 MB   79 MB   58 %  (temporal cols dropped)
  climate_comfort         137 MB   77 MB   56 %
  green_capital           167 MB   108 MB  64 %
  change_pressure         129 MB   70 MB   54 %
  forest_health           145 MB   87 MB   60 %
  carbon_stock           1018 MB   858 MB  84 %  (temporal kept)
  pm25_drivers            152 MB   87 MB   57 %  (high col entropy → still cheap)
  deforestation_dynamics  154 MB   86 MB   56 %  (temporal kept)

Combined with hex.svelte.ts:loadDepartment switching from SELECT * to
projection on the columns the UI actually reads, network bytes per
dpto load drop ~70-80 % on the Patiño-class big depts.

Cache busters bumped on every affected layer (handled by the
overnight_finish.sh bump loop)." 2>&1 | tail -3
git push origin main 2>&1 | tail -2
git push nealab main:master 2>&1 | tail -2
npm run deploy -- --branch main 2>&1 | tail -3

hdr "DONE at $(TS)"
