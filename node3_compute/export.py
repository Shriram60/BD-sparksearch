"""Export for Node 4 (CONTEXT §4.7): <root>/export/v{N}/{documents/, postings/, manifest.json, _SUCCESS}.

_SUCCESS is written last, only after the Parquet files were read back and validated. If vN already
has _SUCCESS the export is skipped (idempotent); --force rebuilds it (e.g. after an Iceberg rollback).
A vN without _SUCCESS is a failed earlier attempt and is deleted and redone.
"""
import json
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from common_spark import current_snapshot_id, get_spark, parse_args, root


class Fs:
    """Thin wrapper over the Hadoop FileSystem API, so the same code works on HDFS and file://."""

    def __init__(self, spark, base):
        jvm = spark._jvm
        self.Path = jvm.org.apache.hadoop.fs.Path
        self.fs = self.Path(base).getFileSystem(spark._jsc.hadoopConfiguration())

    def exists(self, p):
        return self.fs.exists(self.Path(p))

    def delete(self, p):
        self.fs.delete(self.Path(p), True)

    def write_text(self, p, text):
        out = self.fs.create(self.Path(p), True)
        out.write(bytearray(text.encode("utf-8")))
        out.close()


def main():
    def extra(p):
        p.add_argument("--force", action="store_true", help="rebuild even if vN/_SUCCESS exists")
    args = parse_args("Export Parquet for Node 4", extra)
    spark = get_spark(f"export-phase{args.phase}", args.local)
    # Spark would otherwise drop its own _SUCCESS inside documents/ and postings/; only ours counts.
    spark._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    t0 = time.time()

    out = f"{root(args.local)}/export/v{args.phase}"
    fs = Fs(spark, out)
    if fs.exists(f"{out}/_SUCCESS") and not args.force:
        print(f"{out} already complete, skipping (use --force to rebuild)")
        return
    if fs.exists(out):
        print(f"removing previous export at {out}")
        fs.delete(out)

    docs = (spark.table("lake.search.documents").alias("d")
            .join(spark.table("lake.search.doc_stats").alias("s"), "doc_id", "left")
            .join(spark.table("lake.search.pagerank").alias("p"), "doc_id", "left")
            .select("doc_id", "url", "title",
                    F.substring(F.coalesce("text", F.lit("")), 1, 200).alias("snippet"),
                    F.coalesce("doc_len", F.lit(0)).cast("int").alias("doc_len"),
                    F.coalesce("rank", F.lit(0.0)).alias("pagerank")))
    postings = spark.table("lake.search.postings").select("term", "doc_id", "tf")

    docs.coalesce(1).write.parquet(f"{out}/documents")
    postings.coalesce(1).write.parquet(f"{out}/postings")

    # Validate what Node 4 will actually download, not the in-memory plan.
    d = spark.read.parquet(f"{out}/documents")
    p = spark.read.parquet(f"{out}/postings")
    stats = d.agg(F.count("*").alias("n"), F.avg("doc_len").alias("avgdl"),
                  F.max("pagerank").alias("max_pr"),
                  F.sum(F.col("doc_id").isNull().cast("int")).alias("null_ids")).first()
    num_postings = p.count()
    orphans = p.join(d, "doc_id", "left_anti").count()
    problems = []
    if stats["n"] == 0:
        problems.append("no documents")
    if num_postings == 0:
        problems.append("no postings")
    if stats["null_ids"]:
        problems.append(f"{stats['null_ids']} null doc_ids")
    if p.where("doc_id IS NULL").count():
        problems.append("null doc_ids in postings")
    if orphans:
        problems.append(f"{orphans} postings point to missing documents")
    if problems:
        raise SystemExit(f"export v{args.phase} NOT published: {'; '.join(problems)}")

    manifest = {
        "version": args.phase,
        "phase": args.phase,
        "num_docs": stats["n"],
        "num_postings": num_postings,
        "avgdl": float(stats["avgdl"] or 0.0),
        "max_pagerank": float(stats["max_pr"] or 0.0),
        "documents_snapshot_id": current_snapshot_id(spark, "lake.search.documents"),
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    fs.write_text(f"{out}/manifest.json", json.dumps(manifest, indent=2))
    fs.write_text(f"{out}/_SUCCESS", "")
    print(json.dumps(manifest))
    print(f"published {out} in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
