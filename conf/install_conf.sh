#!/usr/bin/env bash
# Copies the shared configs into /opt/hadoop and /opt/spark on this node.
#   ./conf/install_conf.sh node1|node2|node3 [worker_cores] [worker_memory]
# Reads this node's IP from /etc/hosts, so add conf/hosts there first.
# Existing config files are kept as *.bak.
set -euo pipefail

NODE="${1:?usage: install_conf.sh node1|node2|node3 [cores] [memory]}"
CORES="${2:-2}"
MEM="${3:-3g}"
HERE="$(cd "$(dirname "$0")" && pwd)"
HCONF=/opt/hadoop/etc/hadoop
SCONF=/opt/spark/conf

IP="$(awk -v n="$NODE" '$2 == n && $1 !~ /^#/ {print $1}' /etc/hosts | head -1)"
[[ -n "$IP" ]] || { echo "no '$NODE' entry in /etc/hosts; add conf/hosts first" >&2; exit 1; }
if grep -qE '^127\.0\.1\.1' /etc/hosts; then
  echo "WARNING: /etc/hosts still has a 127.0.1.1 line; comment it out or daemons bind to loopback" >&2
fi

install_file() {  # src, dst
  [[ -f "$2" ]] && cp "$2" "$2.bak"
  cp "$1" "$2"
}

for f in core-site.xml hdfs-site.xml; do install_file "$HERE/$f" "$HCONF/$f"; done
[[ -f "$HCONF/hadoop-env.sh" ]] && cp "$HCONF/hadoop-env.sh" "$HCONF/hadoop-env.sh.bak"
grep -v -e '^export JAVA_HOME=' -e '^export HADOOP_LOG_DIR=' "$HCONF/hadoop-env.sh.bak" > "$HCONF/hadoop-env.sh" 2>/dev/null || true
cat "$HERE/hadoop-env.sh" >> "$HCONF/hadoop-env.sh"

[[ -f "$SCONF/spark-env.sh" ]] && cp "$SCONF/spark-env.sh" "$SCONF/spark-env.sh.bak"
sed -e "s/__LOCAL_IP__/$IP/" -e "s/__WORKER_CORES__/$CORES/" -e "s/__WORKER_MEMORY__/$MEM/" \
  "$HERE/spark-env.sh" > "$SCONF/spark-env.sh"
chmod +x "$SCONF/spark-env.sh"

install_file "$HERE/spark-defaults.conf" "$SCONF/spark-defaults.conf"
# Each driver machine advertises its own ZeroTier IP so executors can call back (CONTEXT §4.4).
echo "spark.driver.host                         $IP" >> "$SCONF/spark-defaults.conf"

sudo mkdir -p /data/hdfs/datanode /data/hadoop/tmp
[[ "$NODE" == node2 ]] && sudo mkdir -p /data/hdfs/namenode
sudo chown -R "$USER:$USER" /data

echo "installed configs for $NODE ($IP), worker cores=$CORES memory=$MEM"
