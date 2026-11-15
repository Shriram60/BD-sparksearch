#!/usr/bin/env bash
# End-to-end run of one phase (CONTEXT §5). Run on node3.
#   ./scripts/run_phase.sh N                 # all steps, ingestion over ssh on node1
#   ./scripts/run_phase.sh N --skip-ingest   # Arnav already ran ingestion on node1
# Stops at the first failing step; Node 4 then keeps serving the previous version.
#
# Env overrides:
#   NODE1_REPO   repo path on node1          (default: same path as here)
#   NODE1_DATA   dir with pages.jsonl / changes.jsonl on node1 (default: ~/data)
#   NODE4_URL    search service             (default: http://node4:8000)
set -euo pipefail

PHASE="${1:?usage: run_phase.sh N [--skip-ingest]}"
SKIP_INGEST="${2:-}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
NODE1_REPO="${NODE1_REPO:-$REPO}"
NODE1_DATA="${NODE1_DATA:-~/data}"
NODE4_URL="${NODE4_URL:-http://node4:8000}"
[[ "$PHASE" == 0 ]] && FILE=pages.jsonl || FILE=changes.jsonl

START=$(date +%s)
step() {  # name, command...
  local name="$1"; shift
  local t0=$(date +%s)
  printf '\n===== [%s] %s =====\n' "$(date +%T)" "$name"
  "$@"
  printf -- '----- %s done in %ss\n' "$name" "$(( $(date +%s) - t0 ))"
}
submit() { spark-submit --master spark://node3:7077 "$@"; }

if [[ "$SKIP_INGEST" != "--skip-ingest" ]]; then
  step "1. upload + ingest (node1)" ssh node1 "bash -lc '
    set -e
    hdfs dfs -mkdir -p /searchengine/raw/phase$PHASE
    hdfs dfs -put -f $NODE1_DATA/$FILE /searchengine/raw/phase$PHASE/$FILE
    cd $NODE1_REPO && spark-submit --master spark://node3:7077 node1_ingest/ingest.py --phase $PHASE'"
fi

cd "$REPO/node3_compute"
step "2. index" submit index.py --phase "$PHASE"
step "3. pagerank" submit pagerank.py --phase "$PHASE"
step "4. export" submit export.py --phase "$PHASE"

# Step 5 is Node 4's puller; nudge it and wait until it reports the new version.
step "5. node4 switch to v$PHASE" bash -c "
  curl -fsS -X POST '$NODE4_URL/reload' >/dev/null || echo 'reload call failed, waiting for the poller'
  for i in \$(seq 1 60); do
    if curl -fsS '$NODE4_URL/health' | grep -qE '\"version\": ?$PHASE\\b'; then echo 'node4 is serving v$PHASE'; exit 0; fi
    sleep 5
  done
  echo 'node4 did not switch within 5 min (it keeps serving the previous version)'; exit 1"

printf '\nphase %s finished in %ss\n' "$PHASE" "$(( $(date +%s) - START ))"
