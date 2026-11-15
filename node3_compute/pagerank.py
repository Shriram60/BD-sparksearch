"""PageRank over lake.search.documents + lake.search.links -> lake.search.pagerank (doc_id, rank).

Full recompute every phase (it's global). Damping 0.85, initial rank 1/N, dangling-node mass
spread evenly over all N pages, so ranks always sum to 1. Prints the L1 delta per iteration.
Ranks are checkpointed to HDFS every few iterations to cut the lineage; that is also what lets the
job survive a worker being killed mid-run (lost partitions are recomputed from the last checkpoint).
"""
import time

from pyspark.sql import functions as F

from common_spark import get_spark, parse_args, root

DAMPING = 0.85
CHECKPOINT_EVERY = 5


def main():
    def extra(p):
        p.add_argument("--iterations", type=int, default=20)
    args = parse_args("Compute PageRank", extra)
    if args.iterations < 10:
        raise SystemExit("spec requires at least 10 iterations")

    spark = get_spark(f"pagerank-phase{args.phase}", args.local)
    spark.sparkContext.setCheckpointDir(root(args.local) + "/checkpoints/pagerank")
    t0 = time.time()

    docs = spark.table("lake.search.documents").select("doc_id").distinct().cache()
    n = docs.count()
    if n == 0:
        raise SystemExit("documents table is empty")

    # Only edges between existing docs, no self-links, no duplicates (Node 1 should already do this).
    links = (spark.table("lake.search.links")
             .where("src_id <> dst_id").distinct()
             .join(docs.withColumnRenamed("doc_id", "src_id"), "src_id")
             .join(docs.withColumnRenamed("doc_id", "dst_id"), "dst_id"))
    out_deg = links.groupBy("src_id").agg(F.count("*").alias("out_deg"))
    edges = links.join(out_deg, "src_id").select("src_id", "dst_id", "out_deg").cache()
    dangling = docs.join(out_deg.withColumnRenamed("src_id", "doc_id"), "doc_id", "left_anti").cache()
    print(f"N={n} edges={edges.count()} dangling={dangling.count()}")

    ranks = docs.withColumn("rank", F.lit(1.0 / n)).cache()
    for i in range(1, args.iterations + 1):
        dangling_mass = dangling.join(ranks, "doc_id").agg(F.sum("rank")).first()[0] or 0.0
        contribs = (edges.join(ranks.withColumnRenamed("doc_id", "src_id"), "src_id")
                    .groupBy("dst_id").agg(F.sum(F.col("rank") / F.col("out_deg")).alias("c"))
                    .withColumnRenamed("dst_id", "doc_id"))
        base = (1 - DAMPING) / n + DAMPING * dangling_mass / n
        new = (docs.join(contribs, "doc_id", "left")
               .select("doc_id", (F.lit(base) + DAMPING * F.coalesce("c", F.lit(0.0))).alias("rank")))
        new = new.checkpoint() if i % CHECKPOINT_EVERY == 0 else new.cache()

        delta = (new.join(ranks.withColumnRenamed("rank", "old"), "doc_id")
                 .agg(F.sum(F.abs(F.col("rank") - F.col("old")))).first()[0])
        print(f"iteration {i:2d}  L1 delta {delta:.3e}")
        ranks.unpersist()
        ranks = new

    total = ranks.agg(F.sum("rank")).first()[0]
    spark.sql("CREATE TABLE IF NOT EXISTS lake.search.pagerank (doc_id STRING, rank DOUBLE) USING iceberg")
    ranks.writeTo("lake.search.pagerank").overwrite(F.lit(True))
    print(f"phase {args.phase}: wrote {n} ranks, sum={total:.6f}, took={time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
