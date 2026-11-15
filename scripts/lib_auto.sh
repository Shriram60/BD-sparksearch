# Shared helpers for scripts/auto_node{1,3,4}.sh. Sourced, not run.
#
# The three scripts coordinate through marker files on HDFS under /searchengine/control/phaseN/:
#   go             node3 -> node1   "cluster is ready, ingest now"   (node1 deletes it when it starts)
#   ingest_done    node1 -> node3   "ingest finished"                 (contains the summary)
#   ingest_failed  node1 -> node3   "ingest failed, stop waiting"
#   done           node3 -> node1   "index, PageRank, export finished and node4 switched"
# node4 needs no markers: it watches its own /health and the export on WebHDFS.

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f /opt/spark/conf/cluster-env.sh ]] && source /opt/spark/conf/cluster-env.sh
WAIT_TIMEOUT="${WAIT_TIMEOUT:-3600}"   # seconds before any wait gives up

banner() { printf '\n\033[1;36m==== [%s] %s ====\033[0m\n' "$(date +%T)" "$*"; }
info()   { printf '[%s] %s\n' "$(date +%T)" "$*"; }
ok()     { printf '\033[1;32m[%s] OK  %s\033[0m\n' "$(date +%T)" "$*"; }
die()    { printf '\033[1;31m[%s] FAILED  %s\033[0m\n' "$(date +%T)" "$*" >&2; exit 1; }

# wait_for "description" command...   retries every 5 s, prints a status line every 30 s
wait_for() {
  local desc="$1"; shift
  local start=$(date +%s) last=0
  until "$@" >/dev/null 2>&1; do
    local now=$(date +%s)
    (( now - start > WAIT_TIMEOUT )) && die "timed out after ${WAIT_TIMEOUT}s waiting for: $desc"
    if (( now - last >= 30 )); then info "waiting for: $desc ($(( now - start ))s)"; last=$now; fi
    sleep 5
  done
  ok "$desc"
}

control_dir() { echo "/searchengine/control/phase$1"; }
marker_exists() { hdfs dfs -test -e "$(control_dir "$1")/$2"; }
marker_write() {  # phase, name, [content]
  local tmp; tmp="$(mktemp)"; printf '%s\n' "${3:-$(date -Is) $(hostname)}" > "$tmp"
  hdfs dfs -mkdir -p "$(control_dir "$1")" && hdfs dfs -put -f "$tmp" "$(control_dir "$1")/$2"
  rm -f "$tmp"
}
marker_rm()   { hdfs dfs -rm -f -skipTrash "$(control_dir "$1")/$2" >/dev/null 2>&1 || true; }
marker_cat()  { hdfs dfs -cat "$(control_dir "$1")/$2" 2>/dev/null; }

hdfs_up()        { hdfs dfs -test -d / ; }
spark_workers()  { curl -s http://node3:8080/json/ | python3 -c "import json,sys; print(sum(w['state']=='ALIVE' for w in json.load(sys.stdin)['workers']))" 2>/dev/null || echo 0; }
live_datanodes() { hdfs dfsadmin -report 2>/dev/null | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p'; }
at_least() { [[ "$($1)" -ge "$2" ]] 2>/dev/null; }

# True when node2's HDFS role temporarily runs on this machine (node2 resolves to one of our IPs).
node2_is_local() {
  local ip; ip="$(getent ahostsv4 node2 | awk 'NR==1{print $1}')"
  [[ -n "$ip" ]] && hostname -I | tr ' ' '\n' | grep -qx "$ip"
}
