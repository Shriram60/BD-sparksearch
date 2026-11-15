"""
Print a fingerprint of the Node 1 tables, used by demo/idempotency.sh.

For each table: row count, an order-independent content hash, and the snapshot id `main`
points to. If a rerun is idempotent, every value is identical before and after.

    spark-submit node1_ingest/table_state.py --out state.json
"""
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLES = ["documents", "links", "rejected", "ingest_log"]


def fingerprint(spark, name):
    full = f"lake.search.{name}"
    df = spark.table(full)
    # xxhash64 of every column of a row, summed as a DECIMAL so it cannot overflow.
    # A sum is order-independent, so it only depends on table CONTENTS.
    row = df.select(
        F.count(F.lit(1)).alias("rows"),
        F.coalesce(F.sum(F.xxhash64(*df.columns).cast("decimal(38,0)")), F.lit(0)).cast("string").alias("hash"),
    ).collect()[0]
    snap = spark.sql(f"SELECT snapshot_id FROM {full}.refs WHERE name = 'main'").collect()
    n_snaps = spark.sql(f"SELECT count(*) FROM {full}.snapshots").collect()[0][0]
    return {"rows": row["rows"], "hash": row["hash"],
            "snapshot_id": snap[0][0] if snap else None, "num_snapshots": n_snaps}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    spark = SparkSession.builder.appName("node1-table-state").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    state = {t: fingerprint(spark, t) for t in TABLES}
    with open(args.out, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    for t, s in state.items():
        print(f"  {t:<11} rows={s['rows']:<6} snapshots={s['num_snapshots']:<3} "
              f"snapshot_id={s['snapshot_id']}  hash={s['hash']}")
    spark.stop()


if __name__ == "__main__":
    main()
