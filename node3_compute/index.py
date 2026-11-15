"""Inverted index: lake.search.postings (term, doc_id, tf) + lake.search.doc_stats (doc_id, doc_len).

Phase 0 tokenizes every document. Phase N>0 re-tokenizes only the doc_ids listed in that phase's
ingest_log row and carries the rest over. Each table is replaced in ONE overwrite commit, so a
reader never sees a half-updated index and a rerun produces identical contents.

Tokenizing uses built-in Spark SQL functions, not a Python UDF, so the rules run inside the JVM
and must stay identical to common/tokenizer.py.
"""
import time

from pyspark.sql import functions as F

from common_spark import get_spark, latest_ingest, load_stopwords, parse_args


def tokenize_docs(docs, stopwords):
    tokens = (docs
              .select("doc_id", F.explode(F.split(
                  F.regexp_replace(F.lower(F.coalesce(F.col("text"), F.lit(""))), "[^a-z0-9]", " "),
                  r"\s+")).alias("term"))
              .where((F.length("term") >= 2) & ~F.col("term").isin(stopwords)))
    postings = tokens.groupBy("term", "doc_id").agg(F.count("*").cast("int").alias("tf"))
    # Docs with no surviving tokens still get doc_len 0, so every document has a length.
    doc_stats = (docs.select("doc_id")
                 .join(tokens.groupBy("doc_id").agg(F.count("*").cast("int").alias("doc_len")),
                       "doc_id", "left")
                 .fillna(0, ["doc_len"]))
    return postings, doc_stats


def main():
    args = parse_args("Build/update the inverted index")
    spark = get_spark(f"index-phase{args.phase}", args.local)
    t0 = time.time()

    spark.sql("CREATE TABLE IF NOT EXISTS lake.search.postings "
              "(term STRING, doc_id STRING, tf INT) USING iceberg")
    spark.sql("CREATE TABLE IF NOT EXISTS lake.search.doc_stats "
              "(doc_id STRING, doc_len INT) USING iceberg")

    log = latest_ingest(spark, args.phase)
    changed = list(log["changed_doc_ids"] or [])
    documents = spark.table("lake.search.documents")
    stopwords = load_stopwords()

    if args.phase == 0:
        postings, doc_stats = tokenize_docs(documents, stopwords)
    elif not changed:
        print(f"phase {args.phase}: ingest_log lists 0 changed docs, index left as is")
        return
    else:
        changed_df = spark.createDataFrame([(d,) for d in changed], "doc_id STRING")
        # Deleted docs are in `changed` but not in documents: their rows drop out and nothing replaces them.
        new_postings, new_stats = tokenize_docs(documents.join(changed_df, "doc_id"), stopwords)
        postings = (spark.table("lake.search.postings").join(changed_df, "doc_id", "left_anti")
                    .select("term", "doc_id", "tf").unionByName(new_postings))
        doc_stats = (spark.table("lake.search.doc_stats").join(changed_df, "doc_id", "left_anti")
                     .unionByName(new_stats))

    # The overwrite reads the old snapshot of the same table; materialize first to keep it simple.
    postings = postings.localCheckpoint()
    doc_stats = doc_stats.localCheckpoint()
    postings.writeTo("lake.search.postings").overwrite(F.lit(True))
    doc_stats.writeTo("lake.search.doc_stats").overwrite(F.lit(True))

    print(f"phase {args.phase}: changed docs={len(changed) if args.phase else 'all'} "
          f"postings={postings.count()} doc_stats={doc_stats.count()} "
          f"terms={postings.select('term').distinct().count()} took={time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
