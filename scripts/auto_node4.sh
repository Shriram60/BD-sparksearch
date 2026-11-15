#!/usr/bin/env bash
# Monty / node4: make sure the search service runs, wait until it serves vN, run the demo queries.
#   ./scripts/auto_node4.sh N
# Starts uvicorn in the background (log: /tmp/node4_search.log) if nothing answers on :8000.
# Needs no Spark or Hadoop: it only watches the export on WebHDFS and its own /health.
set -euo pipefail
source "$(dirname "$0")/lib_auto.sh"
PHASE="${1:?usage: auto_node4.sh N}"
URL="${SEARCH_URL:-http://localhost:8000}"
WEBHDFS="http://node2:9870/webhdfs/v1/searchengine/export/v$PHASE/_SUCCESS?op=GETFILESTATUS"

health_field() { curl -s "$URL/health" | python3 -c "import json,sys; print(json.load(sys.stdin).get('$1'))" 2>/dev/null; }
serving()      { [[ "$(health_field version)" == "$PHASE" ]]; }
query() {
  curl -s "$URL/search?q=$(python3 -c 'import sys,urllib.parse; print(urllib.parse.quote_plus(sys.argv[1]))' "$1")&k=3" |
    python3 -c "import json,sys; d=json.load(sys.stdin); print(f\"  v{d['version']}  {sys.argv[1]!r:24} \", ' | '.join(f\"{r['title']} ({r['doc_id']})\" for r in d['results']) or 'no results')" "$1"
}

banner "node4: phase $PHASE"
if curl -s -o /dev/null "$URL/health"; then
  ok "search service already running on $URL"
else
  [[ -x "$REPO/node4_search/.venv/bin/uvicorn" ]] || die "no venv: see node4_search/README.md -> Setup"
  info "starting search service (log /tmp/node4_search.log)"
  (cd "$REPO/node4_search" && nohup .venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000 > /tmp/node4_search.log 2>&1 &)
  wait_for "search service on $URL" curl -sf -o /dev/null "$URL/health"
fi
info "currently serving v$(health_field version), $(health_field num_docs) docs"

if serving; then
  ok "already serving v$PHASE"
else
  banner "waiting for node3's export v$PHASE"
  wait_for "export v$PHASE/_SUCCESS on HDFS" curl -sf -o /dev/null "$WEBHDFS"
  wait_for "this service to pull, validate and switch to v$PHASE" serving
fi
info "/health: $(curl -s "$URL/health")"

banner "demo queries on v$PHASE (demo/queries.md)"
if [[ "$PHASE" == 0 ]]; then
  for q in "solar system" "river" "language" "plague" "alan turing" "breakfast sausage"; do query "$q"; done
else
  echo "  Phase 1 changes:"
  for q in "added during project" "plague" "alan turing" "breakfast sausage"; do query "$q"; done
  echo "  Stable:"
  for q in "solar system" "river" "language"; do query "$q"; done
fi
banner "node4 phase $PHASE finished: UI at http://node4:8000"
