"""SparkSession + paths shared by the Node 3 jobs.

Cluster mode (default): everything comes from /opt/spark/conf/spark-defaults.conf (CONTEXT §4.4).
Local mode (--local): local[*] master and a local Iceberg warehouse, for developing on toy data.
  SEARCH_LOCAL_ROOT  base dir for warehouse/export/checkpoints (default /tmp/searchengine)
  ICEBERG_JAR        path to the Iceberg runtime jar if Spark doesn't already have it
"""
import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession

REPO = Path(__file__).resolve().parent.parent
HDFS_ROOT = "hdfs://node2:9000/searchengine"


def parse_args(description, extra=None):
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--phase", type=int, required=True)
    p.add_argument("--local", action="store_true", help="local[*] + local warehouse, for testing")
    if extra:
        extra(p)
    return p.parse_args()


def root(local):
    if local:
        return "file://" + os.environ.get("SEARCH_LOCAL_ROOT", "/tmp/searchengine")
    return HDFS_ROOT


def get_spark(app, local):
    b = SparkSession.builder.appName(app)
    if local:
        b = (b.master("local[*]")
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.lake.type", "hadoop")
             .config("spark.sql.catalog.lake.warehouse", root(True) + "/warehouse")
             .config("spark.sql.shuffle.partitions", "4"))
        if os.environ.get("ICEBERG_JAR"):
            b = b.config("spark.jars", os.environ["ICEBERG_JAR"])
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.search")
    return spark


def load_stopwords():
    with open(REPO / "common" / "stopwords.txt", encoding="utf-8") as f:
        return sorted({line.strip() for line in f if line.strip()})


def latest_ingest(spark, phase):
    """Latest ingest_log row for this phase (written by Node 1), or fail loudly."""
    rows = (spark.table("lake.search.ingest_log")
            .where(f"phase = {phase}")
            .orderBy("run_at", ascending=False)
            .limit(1).collect())
    if not rows:
        raise SystemExit(f"no ingest_log row for phase {phase}; run Node 1 ingestion first")
    return rows[0]


def current_snapshot_id(spark, table):
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.history "
                     "WHERE is_current_ancestor ORDER BY made_current_at DESC LIMIT 1").collect()
    return rows[0][0] if rows else None
