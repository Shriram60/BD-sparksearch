"""
Drop the Iceberg tables so the pipeline can start again from Phase 0, e.g. when switching from
the toy dataset to the real pages.jsonl. Without a reset, real records would be compared against
leftover toy rows (same doc_ids, same version 0) and silently ignored as "no change".

    node1_ingest/reset_tables.sh            # Node 1 tables only (asks for confirmation)
    node1_ingest/reset_tables.sh --all      # every table in lake.search (Node 1 + Node 3)

Tables are dropped with PURGE, so their data and snapshot history are deleted. Never run this on
evaluation day after Phase 0 has been prepared.
"""
import argparse
import sys

from pyspark.sql import SparkSession

NS = "lake.search"
NODE1_TABLES = ["documents", "links", "rejected", "ingest_log", "raw_links", "tombstones"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="drop every table in lake.search, not only Node 1's")
    ap.add_argument("--yes", action="store_true", help="do not ask for confirmation")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("node1-reset-tables").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    existing = sorted(r["tableName"] for r in spark.sql(f"SHOW TABLES IN {NS}").collect())
    targets = existing if args.all else [t for t in NODE1_TABLES if t in existing]

    print(f"warehouse : {spark.conf.get('spark.sql.catalog.lake.warehouse', '?')}")
    print(f"will DROP : {', '.join(targets) or '(nothing to drop)'}")
    if not targets:
        return 0
    if not args.yes:
        if input("type 'yes' to drop these tables: ").strip() != "yes":
            print("aborted, nothing dropped")
            return 1
    for t in targets:
        spark.sql(f"DROP TABLE IF EXISTS {NS}.{t} PURGE")
        print(f"dropped {NS}.{t}")
    if not args.all:
        print("Node 3 tables (postings, doc_stats, pagerank) were kept; use --all to drop them too.")
    print("Also clear old exports before re-running Phase 0: "
          "hdfs dfs -rm -r -f /searchengine/export (cluster) or rm -rf /tmp/searchengine/export (local), "
          "and /serving/v* on node4.")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
