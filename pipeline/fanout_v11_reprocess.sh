#!/bin/bash
# Fan-out v1.1 re-process: 9 territories x 6 core analyses = 54 jobs.
# Skips misiones/climate_comfort (already done in smoke test).
# Runs 4 jobs in parallel via xargs -P.
#
# Each job log to pipeline/output/v11_fanout_logs/<territory>_<analysis>.log
# Exit 0 if all jobs succeed; nonzero if any failed (xargs propagates).
set -u

cd "$(dirname "$0")/.."
LOG_DIR="pipeline/output/v11_fanout_logs"
mkdir -p "$LOG_DIR"

TERRITORIES="misiones corrientes itapua_py alto_parana_py chaco formosa parana_br santa_catarina_br rio_grande_sul_br"
ANALYSES="environmental_risk climate_comfort green_capital change_pressure agri_potential forest_health"
GOALPOSTS_MTIME=$(stat -c "%Y" pipeline/config/goalposts.json)

# Skip a (territory, analysis) pair if its parquet is newer than goalposts.json
is_done() {
    local t="$1"
    local a="$2"
    local d="pipeline/output"
    [ "$t" != "misiones" ] && d="pipeline/output/$t"
    local f="$d/sat_${a}.parquet"
    if [ -f "$f" ]; then
        local m=$(stat -c "%Y" "$f")
        [ "$m" -gt "$GOALPOSTS_MTIME" ] && return 0
    fi
    return 1
}

run_one() {
    local t="$1"
    local a="$2"
    local log="$LOG_DIR/${t}_${a}.log"
    echo "[$(date +%H:%M:%S)] START $t/$a"
    PYTHONIOENCODING=utf-8 python pipeline/process_raster_to_h3.py \
        --territory "$t" --analysis "$a" --mode comparable \
        > "$log" 2>&1
    local rc=$?
    if [ $rc -eq 0 ]; then
        echo "[$(date +%H:%M:%S)] DONE  $t/$a (rc=0)"
    else
        echo "[$(date +%H:%M:%S)] FAIL  $t/$a (rc=$rc) — see $log"
    fi
    return $rc
}

export -f run_one
export LOG_DIR

# Build job list: skip any (t, a) whose parquet is newer than goalposts.json (idempotent resume)
jobs=""
skipped=0
for t in $TERRITORIES; do
    for a in $ANALYSES; do
        if is_done "$t" "$a"; then
            skipped=$((skipped+1))
            continue
        fi
        jobs="$jobs$t $a\n"
    done
done

n=$(printf "$jobs" | wc -l)
echo "Pending jobs: $n  |  Already-done (skipped): $skipped  |  Parallel workers: 4"
echo "Logs: $LOG_DIR/"
[ "$n" -eq 0 ] && { echo "Nothing to do."; exit 0; }

printf "$jobs" | xargs -P 4 -L 1 bash -c 'run_one "$@"' _
exit_code=$?

echo
echo "[$(date +%H:%M:%S)] FANOUT FINISHED  exit=$exit_code"
exit $exit_code
