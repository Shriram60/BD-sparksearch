#!/usr/bin/env bash
# Arnav / node1: wait for node3's "go", ingest phase N, report back. Start it any time.
#   ./scripts/auto_node1.sh N                  # ingest only
#   ./scripts/auto_node1.sh N --idempotency    # then wait for the whole phase, run demo/idempotency.sh N
# Data files: $DATA_DIR/pages.jsonl (phase 0) or $DATA_DIR/changes.jsonl, DATA_DIR defaults to ~/data.
set -euo pipefail
source "$(dirname "$0")/lib_auto.sh"
PHASE="${1:?usage: auto_node1.sh N [--idempotency]}"
IDEMPOTENCY="${2:-}"
DATA_DIR="${DATA_DIR:-$HOME/data}"
FILE="$DATA_DIR/$([[ "$PHASE" == 0 ]] && echo pages.jsonl || echo changes.jsonl)"

banner "node1: phase $PHASE"
[[ -f "$FILE" ]] || die "input file not found: $FILE"
ok "input file $FILE"
"$REPO/tools/check_versions.sh" >/dev/null || die "tool versions don't match tools/versions.env (run tools/check_versions.sh)"
ok "tool versions"

banner "1. waiting for node3"
wait_for "HDFS reachable" hdfs_up
wait_for "go signal from node3 (Shriram runs ./scripts/auto_node3.sh $PHASE)" marker_exists "$PHASE" go
marker_rm "$PHASE" go

banner "2. ingest"
LOG="$(mktemp)"
if DATA_DIR="$DATA_DIR" "$REPO/node1_ingest/run_ingest.sh" "$PHASE" 2>&1 | tee "$LOG" \
     | grep --line-buffered -vE " (INFO|WARN) |^\s*$|NativeCodeLoader"; [[ ${PIPESTATUS[0]} -eq 0 ]]; then
  marker_write "$PHASE" ingest_done "$(sed -n '/INGEST SUMMARY/,/^\[run_ingest\]/p' "$LOG")"
  ok "ingest done, node3 notified"
else
  marker_write "$PHASE" ingest_failed "$(tail -5 "$LOG")"
  die "ingest failed, node3 notified"
fi
rm -f "$LOG"

if [[ "$IDEMPOTENCY" == "--idempotency" ]]; then
  banner "3. waiting for node3 to finish the phase"
  wait_for "node3: index, PageRank, export and node4 switch" marker_exists "$PHASE" done
  banner "4. idempotency demo"
  "$REPO/demo/idempotency.sh" "$PHASE"
fi
banner "node1 phase $PHASE finished"
