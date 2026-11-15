#!/usr/bin/env bash
# Idempotency demo (rubric item 4, Arnav).
#
#   demo/idempotency.sh <phase> [file]        # e.g. demo/idempotency.sh 1
#   LOCAL=1 demo/idempotency.sh 1 node1_ingest/toy_data/changes.jsonl
#
# 1. fingerprint documents / links / rejected / ingest_log (rows, content hash, snapshot id)
# 2. re-run ingestion of the SAME phase
# 3. fingerprint again and compare -> prints UNCHANGED or CHANGED
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/node1_ingest/spark_args.sh"
PHASE="${1:?usage: $0 <phase> [file]}"
shift
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

state() { spark-submit "${SPARK_ARGS[@]}" "$ROOT/node1_ingest/table_state.py" --out "$1" 2>/dev/null; }

echo "==================== BEFORE re-ingesting phase $PHASE ===================="
state "$TMP/before.json"
echo
echo "==================== re-running ingestion of phase $PHASE ================="
"$ROOT/node1_ingest/run_ingest.sh" "$PHASE" "$@" 2>&1 | grep -v -E "^\s*$|WARN|INFO" || true
echo
echo "==================== AFTER ==============================================="
state "$TMP/after.json"
echo
if diff -u "$TMP/before.json" "$TMP/after.json"; then
  echo "RESULT: UNCHANGED  (same row counts, same content hashes, no new snapshots)"
else
  echo "RESULT: CHANGED  (see diff above)"
  exit 1
fi
