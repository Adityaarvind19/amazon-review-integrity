#!/usr/bin/env bash
# Full real-data run on your Mac. Produces every evidence file under docs/results/.
# Usage: scripts/run_real.sh            (categories: All_Beauty then Video_Games)
set -euo pipefail
cd "$(dirname "$0")/.."
ulimit -n 10240
FIRST=${FIRST:-All_Beauty}
SECOND=${SECOND:-Video_Games}
EVAL_CAT=${EVAL_CAT:-All_Beauty}
mkdir -p docs/results
export RESULTS_DIR=docs/results
log() { echo; echo "=== $* ==="; }

log "1/8 download"
scripts/download_data.sh "$FIRST" "$SECOND"

log "2/8 first category only"
python -m revint run --categories "$FIRST" --publish --tag first_category 2>&1 | tee docs/results/log_first_category.txt

log "3/8 incremental: add second category (only it should be processed)"
python -m revint run --categories "$FIRST" "$SECOND" --publish --tag incremental 2>&1 | tee docs/results/log_incremental.txt

log "4/8 full timed run (force) = main evidence"
python -m revint run --categories "$FIRST" "$SECOND" --force --publish --tag main 2>&1 | tee docs/results/log_main.txt

log "5/8 idempotency + incremental no-op proof"
python -m revint idempotency --categories "$FIRST" "$SECOND" 2>&1 | tee docs/results/log_idempotency.txt

log "6/8 evaluation with injected campaigns (3 seeds)"
python -m revint eval --category "$EVAL_CAT" 2>&1 | tee docs/results/log_eval.txt

log "7/8 SQL analytics + dashboard"
python -m revint sql
python -m revint dashboard

log "8/8 resume numbers"
python -m revint report
cat docs/results/RESUME_NUMBERS.md
