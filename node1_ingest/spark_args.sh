# Sourced by node1_ingest/run_ingest.sh and demo/idempotency.sh. Sets SPARK_ARGS.
#   LOCAL=1  -> Spark local mode + local Iceberg warehouse (dev/testing, no cluster needed)
#   default  -> cluster: master/catalog/warehouse come from /opt/spark/conf/spark-defaults.conf
#               (CONTEXT 4.4). On node1 that file MUST set spark.driver.host = node1's ZeroTier IP.
# Same local root as node3_compute/common_spark.py, so local Node 1 + Node 3 runs share one warehouse.
# Over `ssh node1 "bash -lc ..."` (scripts/run_phase.sh) ~/.bashrc returns early for
# non-interactive shells, so hdfs/spark-submit may not be on PATH. Load the cluster env written by
# tools/install_node.sh, and fall back to the standard install paths (CONTEXT 4.1).
[[ -f /opt/spark/conf/cluster-env.sh ]] && source /opt/spark/conf/cluster-env.sh
command -v spark-submit >/dev/null 2>&1 || export PATH="/opt/spark/bin:/opt/spark/sbin:$PATH"
command -v hdfs >/dev/null 2>&1 || export PATH="/opt/hadoop/bin:/opt/hadoop/sbin:$PATH"
if [[ -z "${JAVA_HOME:-}" ]]; then
  JAVA_HOME="$(ls -d /usr/lib/jvm/java-11-openjdk-* 2>/dev/null | head -1)"; export JAVA_HOME
fi
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/opt/hadoop/etc/hadoop}"

LOCAL_WAREHOUSE="${LOCAL_WAREHOUSE:-file://${SEARCH_LOCAL_ROOT:-/tmp/searchengine}/warehouse}"
if [[ "${LOCAL:-0}" == "1" ]]; then
  SPARK_ARGS=(
    --master "local[*]"
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog
    --conf spark.sql.catalog.lake.type=hadoop
    --conf "spark.sql.catalog.lake.warehouse=${LOCAL_WAREHOUSE}"
    --conf spark.sql.shuffle.partitions=4
    --conf spark.ui.showConsoleProgress=false
  )
else
  SPARK_ARGS=(--conf spark.ui.showConsoleProgress=false)
fi
