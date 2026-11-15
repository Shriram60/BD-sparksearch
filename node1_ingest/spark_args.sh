# Sourced by node1_ingest/run_ingest.sh and demo/idempotency.sh. Sets SPARK_ARGS.
#   LOCAL=1  -> Spark local mode + local Iceberg warehouse (dev/testing, no cluster needed)
#   default  -> cluster: master/catalog/warehouse come from /opt/spark/conf/spark-defaults.conf
#               (CONTEXT 4.4). On node1 that file MUST set spark.driver.host = node1's ZeroTier IP.
LOCAL_WAREHOUSE="${LOCAL_WAREHOUSE:-file:///tmp/sparksearch-warehouse}"
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
