#!/usr/bin/env bash
# Shriram / node3: the coordinator. Run one phase end to end, waiting on the other nodes.
#   ./scripts/auto_node3.sh N
# 1. starts this node's daemons (and node2's HDFS too, if node2 temporarily points at this machine)
# 2. waits until HDFS and Spark are healthy
# 3. signals node1 "go", waits for node1's ingest
# 4. index -> PageRank -> export -> waits until node4 serves vN (scripts/run_phase.sh)
# 5. signals "done" (node1's --idempotency step waits for this)
# Env: MIN_WORKERS / MIN_DATANODES (default 2, or 1 when node2 runs on this machine).
set -euo pipefail
source "$(dirname "$0")/lib_auto.sh"
PHASE="${1:?usage: auto_node3.sh N}"
START=$(date +%s)

banner "node3: phase $PHASE"
"$REPO/tools/check_versions.sh" >/dev/null || die "tool versions don't match tools/versions.env (run tools/check_versions.sh)"
ok "tool versions"

banner "1. daemons on this machine"
if node2_is_local; then
  info "node2 points at this machine: running HDFS NameNode here too (temporary setup)"
  jps | grep -qw NameNode || hdfs --daemon start namenode
  DEFAULT_MIN=1
else
  wait_for "NameNode on node2 (Blason: hdfs --daemon start namenode)" bash -c "timeout 3 bash -c '</dev/tcp/node2/9000'"
  DEFAULT_MIN=2
fi
MIN_WORKERS="${MIN_WORKERS:-$DEFAULT_MIN}"; MIN_DATANODES="${MIN_DATANODES:-$DEFAULT_MIN}"
jps | grep -qw DataNode || hdfs --daemon start datanode
jps | grep -qw Master   || start-master.sh >/dev/null
sleep 3
jps | grep -qw Worker   || start-worker.sh spark://node3:7077 >/dev/null
ok "NameNode/DataNode/Master/Worker running here: $(jps | awk '$2 ~ /NameNode|DataNode|Master|Worker/ {printf "%s ", $2}')"

banner "2. cluster health"
wait_for "HDFS reachable" hdfs_up
wait_for "$MIN_DATANODES live DataNode(s)" at_least live_datanodes "$MIN_DATANODES"
wait_for "$MIN_WORKERS Spark worker(s) on node3:8080" at_least spark_workers "$MIN_WORKERS"
hdfs dfs -mkdir -p /searchengine

banner "3. ingest on node1"
for m in go ingest_done ingest_failed done; do marker_rm "$PHASE" "$m"; done
marker_write "$PHASE" go
info "signalled node1. Arnav runs: ./scripts/auto_node1.sh $PHASE"
wait_for "node1 to pick up the go signal" bash -c "! hdfs dfs -test -e $(control_dir "$PHASE")/go"
ok "node1 started ingesting"
wait_for "node1 ingest to finish" bash -c "hdfs dfs -test -e $(control_dir "$PHASE")/ingest_done || hdfs dfs -test -e $(control_dir "$PHASE")/ingest_failed"
marker_exists "$PHASE" ingest_failed && die "node1 ingest failed: $(marker_cat "$PHASE" ingest_failed)"
ok "node1 ingest done"
marker_cat "$PHASE" ingest_done | sed 's/^/    /'

banner "4. index -> PageRank -> export -> node4"
if ! "$REPO/scripts/run_phase.sh" "$PHASE" --skip-ingest 2>&1 \
     | tr '\r' '\n' | grep --line-buffered -vE " (INFO|WARN) |^\s*$|NativeCodeLoader|log level|setLogLevel"; then
  die "run_phase.sh failed (node4 keeps serving the previous version)"
fi

marker_write "$PHASE" done
banner "phase $PHASE COMPLETE in $(( $(date +%s) - START ))s: node4 is serving v$PHASE"
