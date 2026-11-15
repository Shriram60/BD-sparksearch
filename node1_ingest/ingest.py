"""
Node 1 ingestion job (owner: Arnav).

    spark-submit node1_ingest/ingest.py --phase N [--input PATH]

Pipeline (CONTEXT section 4.6 / ARNAV_node1.md section C):
    raw JSONL -> parse (from_json, never crashes on bad lines)
              -> validate (bad rows -> lake.search.rejected with a reason)
              -> dedup by doc_id (highest version wins)
              -> change detection vs. lake.search.documents (only new / higher version)
              -> MERGE into lake.search.documents
              -> sync lake.search.links for the changed docs
              -> append one row to lake.search.ingest_log

Idempotency: every write is guarded by "is there anything to change?". Re-running the same
phase finds 0 changed docs, 0 new rejects and 0 link differences, so NO table gets a new
snapshot and no ingest_log row is appended (the latest ingest_log row for the phase stays the
one with the real changes, so Node 3 can safely re-run index.py too).

Spark/Iceberg settings (master, catalog `lake`, warehouse) come from spark-defaults.conf
(CONTEXT section 4.4) or --conf flags; this file hardcodes none of them.
"""

import argparse
import sys
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Field names in the raw JSON (verified against the real pages.jsonl / changes.json, see
# CONTEXT 4.5). The real data has NO version field and uses op = "UPSERT" / "DELETE".
# ---------------------------------------------------------------------------
F_DOC_ID = "doc_id"
F_URL = "url"
F_TITLE = "title"
F_TEXT = "text"
F_VERSION = "version"  # optional; when absent the record's version = its phase number
F_LINKS = "links"
F_OP = "op"          # optional; "delete" removes the doc. Absent/null/"upsert" = upsert.

DELETE_OPS = ["delete", "deleted", "remove"]
UPSERT_OPS = ["upsert", "insert", "update", "add", "create"]

CATALOG = "lake"
NS = f"{CATALOG}.search"
T_DOCS = f"{NS}.documents"
T_LINKS = f"{NS}.links"
T_REJECTED = f"{NS}.rejected"
T_LOG = f"{NS}.ingest_log"
# Node-1-internal table (not part of the shared contract, nobody else reads it):
# every link a document declares, before dropping dangling targets. Needed so that a link
# A -> B dropped in phase 0 (B missing) comes back when B is added in phase 1, even though
# A itself did not change.
T_RAW_LINKS = f"{NS}.raw_links"
# Node-1-internal: (doc_id, version) of every applied delete. Without it, re-ingesting an
# OLDER phase (e.g. phase 0 after phase 1) would resurrect documents that were deleted later.
T_TOMBSTONES = f"{NS}.tombstones"

# Every field is read as a STRING on purpose: a JSON number/array/object put into a
# StringType field keeps its raw text instead of nulling the whole record, so we can say
# exactly WHAT is wrong (e.g. bad_version_type) instead of a generic parse failure.
RAW_SCHEMA = StructType([
    StructField(F_DOC_ID, StringType()),
    StructField(F_URL, StringType()),
    StructField(F_TITLE, StringType()),
    StructField(F_TEXT, StringType()),
    StructField(F_VERSION, StringType()),
    StructField(F_LINKS, StringType()),
    StructField(F_OP, StringType()),
    StructField("_corrupt_record", StringType()),
])


def log(msg):
    print(f"[ingest {datetime.now():%H:%M:%S}] {msg}", flush=True)


def create_tables(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    props = "TBLPROPERTIES ('format-version'='2')"
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_DOCS} (
        doc_id STRING, url STRING, title STRING, text STRING,
        version BIGINT, phase INT, ingested_at TIMESTAMP) USING iceberg {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_LINKS} (
        src_id STRING, dst_id STRING) USING iceberg {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_REJECTED} (
        raw STRING, reason STRING, phase INT) USING iceberg {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_LOG} (
        phase INT, documents_snapshot_id BIGINT, links_snapshot_id BIGINT,
        changed_doc_ids ARRAY<STRING>, accepted BIGINT, rejected BIGINT,
        run_at TIMESTAMP) USING iceberg {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_RAW_LINKS} (
        src_id STRING, dst_id STRING) USING iceberg {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {T_TOMBSTONES} (
        doc_id STRING, version BIGINT) USING iceberg {props}""")


def current_snapshot(spark, table):
    """Snapshot id the `main` branch points to (correct even after a rollback)."""
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()
    return rows[0][0] if rows else None


def parse(spark, input_path):
    # line_no keeps file order (monotonically_increasing_id grows with partition index and
    # row position), used to break ties between equal versions: the first line wins.
    raw = (spark.read.text(input_path)
           .withColumnRenamed("value", "raw")
           .withColumn("line_no", F.monotonically_increasing_id())
           .withColumn("raw", F.regexp_replace("raw", "\r$", "")))
    blank = raw.filter(F.trim("raw") == "")
    raw = raw.filter(F.trim("raw") != "")

    parsed = raw.withColumn("j", F.from_json(
        "raw", RAW_SCHEMA, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}))
    df = parsed.select(
        "raw", "line_no",
        F.col("j._corrupt_record").alias("_corrupt"),
        F.trim(F.col(f"j.{F_DOC_ID}")).alias("doc_id"),
        F.trim(F.col(f"j.{F_URL}")).alias("url"),
        F.trim(F.col(f"j.{F_TITLE}")).alias("title"),
        F.col(f"j.{F_TEXT}").alias("text"),
        F.trim(F.col(f"j.{F_VERSION}")).alias("version_raw"),
        F.col(f"j.{F_LINKS}").alias("links_raw"),
        F.lower(F.trim(F.col(f"j.{F_OP}"))).alias("op_raw"),
    )
    # links must be a JSON array of strings (or numbers, which we read as strings), or absent.
    df = df.withColumn("links", F.from_json("links_raw", ArrayType(StringType())))
    df = df.withColumn("is_delete", F.coalesce(F.col("op_raw").isin(DELETE_OPS), F.lit(False)))
    return df, blank.count()


def validate(df, phase):
    """Attach `reason` (null = valid). The first failing rule wins, so each reject has one reason."""
    def empty(c):
        return F.col(c).isNull() | (F.trim(F.col(c)) == "")

    is_delete = F.col("is_delete")
    # A JSON line that is valid JSON but not an object (e.g. `[1,2]` or `"x"`) or that is
    # truncated ends up either in _corrupt_record or as an all-null struct.
    not_object = ~F.trim("raw").startswith("{")
    reason = (
        F.when(F.col("_corrupt").isNotNull() | not_object, "invalid_json")
        .when(F.col("op_raw").isNotNull() & ~F.col("op_raw").isin(DELETE_OPS + UPSERT_OPS), "bad_op")
        .when(empty("doc_id"), "missing_doc_id")
        # version is optional (the real data has none); if present it must be an integer.
        .when(~empty("version_raw") & ~F.col("version_raw").rlike(r"^[0-9]{1,18}$"), "bad_version_type")
        # A delete only needs doc_id + version; everything below applies to upserts.
        .when(~is_delete & empty("url"), "missing_url")
        .when(~is_delete & ~F.lower("url").rlike(r"^https?://\S+$"), "bad_url")
        .when(~is_delete & empty("title"), "missing_title")
        .when(~is_delete & empty("text"), "missing_text")
        .when(~is_delete & F.col("links_raw").isNotNull() & F.col("links").isNull(), "bad_links_type")
    )
    # Effective version: explicit version if given, else the phase number. So "highest
    # version wins" also holds ACROSS phases: phase 1 data (v1) beats phase 0 data (v0),
    # and re-ingesting phase 0 later can never overwrite phase 1 changes.
    version = F.when(empty("version_raw"), F.lit(phase)).otherwise(F.col("version_raw")).cast("bigint")
    return df.withColumn("reason", reason).withColumn("version", version)


def dedup(valid):
    """One row per doc_id: highest version wins. Equal versions (the same doc twice in one
    phase file, no version field): the FIRST occurrence wins and later copies count as
    duplicates (in the course toy data the second copy is the junk "Soil duplicate").
    Updates across phases still win through the higher version. Deterministic, so reruns
    always pick the same row."""
    w = Window.partitionBy("doc_id").orderBy(
        F.col("version").desc(), F.col("line_no").asc())
    return valid.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def main():
    ap = argparse.ArgumentParser(description="Node 1 ingestion job")
    ap.add_argument("--phase", type=int, required=True)
    ap.add_argument("--input", help="file/dir to read (default: hdfs:///searchengine/raw/phase<N>/)")
    args = ap.parse_args()
    phase = args.phase
    input_path = args.input or f"/searchengine/raw/phase{phase}/"

    spark = (SparkSession.builder.appName(f"ingest-phase{phase}").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    log(f"phase={phase} input={input_path} master={spark.sparkContext.master}")
    create_tables(spark)

    # ---- 1. parse + validate ----------------------------------------------------------
    df, blank_lines = parse(spark, input_path)
    df = validate(df, phase).cache()
    total = df.count()

    rejects = (df.filter(F.col("reason").isNotNull())
               .select("raw", "reason", F.lit(phase).cast("int").alias("phase")))
    reject_counts = {r["reason"]: r["count"] for r in rejects.groupBy("reason").count().collect()}
    n_rejected = sum(reject_counts.values())

    valid = df.filter(F.col("reason").isNull())
    n_accepted = valid.count()

    # ---- 2. dedup within this batch ---------------------------------------------------
    batch = dedup(valid).cache()
    n_unique = batch.count()
    dup_collapsed = n_accepted - n_unique

    # ---- 3. change detection against what is already stored -----------------------------
    # "known" version of a doc = its version in documents, or, if it was deleted, the
    # version of the delete (tombstone). Only strictly higher versions get through.
    docs_now = spark.table(T_DOCS).select("doc_id", F.col("version").alias("cur_version"),
                                          F.lit(True).alias("exists"))
    tombs = (spark.table(T_TOMBSTONES).join(docs_now, "doc_id", "left_anti")
             .select("doc_id", F.col("version").alias("cur_version"), F.lit(False).alias("exists")))
    known = docs_now.unionByName(tombs)
    joined = (batch.join(known, "doc_id", "left")
              .withColumn("exists", F.coalesce("exists", F.lit(False))))
    newer = F.col("cur_version").isNull() | (F.col("version") > F.col("cur_version"))
    upserts = joined.filter(~F.col("is_delete") & newer)
    deletes = joined.filter(F.col("is_delete") & F.col("exists") & newer)
    ignored_stale = joined.filter(~F.col("is_delete") & ~newer).count()
    ignored_deletes = joined.filter(F.col("is_delete") & ~(F.col("exists") & newer)).count()

    ingested_at = F.current_timestamp()
    changes = (upserts.select(
        "doc_id", "url", "title", "text", "version",
        F.lit(phase).cast("int").alias("phase"), ingested_at.alias("ingested_at"),
        F.lit("upsert").alias("op"), "links", (~F.col("exists")).alias("is_new"))
        .unionByName(deletes.select(
            "doc_id", F.lit(None).cast("string").alias("url"), F.lit(None).cast("string").alias("title"),
            F.lit(None).cast("string").alias("text"), "version",
            F.lit(phase).cast("int").alias("phase"), ingested_at.alias("ingested_at"),
            F.lit("delete").alias("op"), F.lit(None).cast("array<string>").alias("links"),
            F.lit(False).alias("is_new")))
    ).localCheckpoint(eager=True)
    # localCheckpoint materialises the change set NOW. Without it, Spark could lazily
    # recompute `changes` after the MERGE below, compare against the already-updated
    # documents table, and find nothing (= silently lose the links step / log row).
    n_inserted = changes.filter("op = 'upsert' AND is_new").count()
    n_updated = changes.filter("op = 'upsert' AND NOT is_new").count()
    n_deleted = changes.filter("op = 'delete'").count()
    changed_ids = sorted(r[0] for r in changes.select("doc_id").collect())

    # ---- 4. rejected (deduped on raw + phase, so reruns add nothing) -------------------
    new_rejects = (rejects.dropDuplicates(["raw", "phase"])
                   .join(spark.table(T_REJECTED).filter(F.col("phase") == phase), ["raw", "phase"], "left_anti"))
    n_new_rejects = new_rejects.count()
    if n_new_rejects:
        new_rejects.createOrReplaceTempView("new_rejects")
        spark.sql(f"""MERGE INTO {T_REJECTED} t USING new_rejects s
                      ON t.raw = s.raw AND t.phase = s.phase
                      WHEN NOT MATCHED THEN INSERT (raw, reason, phase) VALUES (s.raw, s.reason, s.phase)""")

    links_dropped = 0
    links_added = 0
    if not changed_ids:
        log("0 changes -> documents / links / ingest_log NOT touched (idempotent rerun)")
    else:
        # ---- 5. MERGE documents ---------------------------------------------------------
        changes.createOrReplaceTempView("changes")
        spark.sql(f"""
            MERGE INTO {T_DOCS} t
            USING changes s
            ON t.doc_id = s.doc_id
            WHEN MATCHED AND s.op = 'delete' AND s.version > t.version THEN DELETE
            WHEN MATCHED AND s.op = 'upsert' AND s.version > t.version THEN UPDATE SET
                t.url = s.url, t.title = s.title, t.text = s.text, t.version = s.version,
                t.phase = s.phase, t.ingested_at = s.ingested_at
            WHEN NOT MATCHED AND s.op = 'upsert' THEN INSERT
                (doc_id, url, title, text, version, phase, ingested_at)
                VALUES (s.doc_id, s.url, s.title, s.text, s.version, s.phase, s.ingested_at)
        """)

        if n_deleted:
            spark.sql(f"""
                MERGE INTO {T_TOMBSTONES} t
                USING (SELECT doc_id, version FROM changes WHERE op = 'delete') s
                ON t.doc_id = s.doc_id
                WHEN MATCHED THEN UPDATE SET t.version = s.version
                WHEN NOT MATCHED THEN INSERT (doc_id, version) VALUES (s.doc_id, s.version)
            """)

        # ---- 6. raw_links: replace the declared out-links of every changed doc ---------
        new_raw = (changes.filter("op = 'upsert'")
                   .select(F.col("doc_id").alias("src_id"), F.explode_outer("links").alias("dst_id"))
                   .withColumn("dst_id", F.trim("dst_id"))
                   .filter(F.col("dst_id").isNotNull() & (F.col("dst_id") != ""))
                   .filter(F.col("src_id") != F.col("dst_id"))          # no self-links
                   .dropDuplicates(["src_id", "dst_id"]))
        new_raw.createOrReplaceTempView("new_raw_links")
        spark.createDataFrame([(i,) for i in changed_ids], "doc_id STRING").createOrReplaceTempView("changed_ids")
        spark.sql(f"DELETE FROM {T_RAW_LINKS} WHERE src_id IN (SELECT doc_id FROM changed_ids)")
        if new_raw.limit(1).count():
            new_raw.writeTo(T_RAW_LINKS).append()

        # ---- 7. links = raw_links whose src AND dst both exist in documents ------------
        desired = spark.sql(f"""
            SELECT DISTINCT r.src_id, r.dst_id
            FROM {T_RAW_LINKS} r
            JOIN {T_DOCS} s ON r.src_id = s.doc_id
            JOIN {T_DOCS} d ON r.dst_id = d.doc_id
            WHERE r.src_id <> r.dst_id
        """).cache()
        current_links = spark.table(T_LINKS)
        to_add = desired.join(current_links, ["src_id", "dst_id"], "left_anti").count()
        to_drop = current_links.join(desired, ["src_id", "dst_id"], "left_anti").count()
        links_dropped = (spark.table(T_RAW_LINKS).count() - desired.count())  # dangling / to-deleted
        links_added = to_add
        if to_add or to_drop:
            desired.createOrReplaceTempView("desired_links")
            spark.sql(f"""
                MERGE INTO {T_LINKS} t USING desired_links s
                ON t.src_id = s.src_id AND t.dst_id = s.dst_id
                WHEN NOT MATCHED THEN INSERT (src_id, dst_id) VALUES (s.src_id, s.dst_id)
                WHEN NOT MATCHED BY SOURCE THEN DELETE
            """)
        log(f"links: +{to_add} -{to_drop} (declared links currently dangling: {links_dropped})")

        # ---- 8. ingest_log ---------------------------------------------------------------
        docs_snap = current_snapshot(spark, T_DOCS)
        links_snap = current_snapshot(spark, T_LINKS)
        log_row = spark.createDataFrame(
            [(phase, docs_snap, links_snap, changed_ids, n_accepted, n_rejected)],
            "phase INT, documents_snapshot_id BIGINT, links_snapshot_id BIGINT, "
            "changed_doc_ids ARRAY<STRING>, accepted BIGINT, rejected BIGINT"
        ).withColumn("run_at", F.current_timestamp())
        log_row.writeTo(T_LOG).append()

    # ---- 9. summary ----------------------------------------------------------------------
    print()
    print("=" * 60)
    print(f" INGEST SUMMARY  phase {phase}")
    print("=" * 60)
    print(f" lines read (non-blank)       : {total}   (blank lines skipped: {blank_lines})")
    print(f" rejected                     : {n_rejected}   (new in rejected table: {n_new_rejects})")
    for reason, cnt in sorted(reject_counts.items()):
        print(f"     - {reason:<24}: {cnt}")
    print(f" accepted                     : {n_accepted}")
    print(f" duplicates collapsed         : {dup_collapsed}")
    print(f" ignored (stale/same version) : {ignored_stale}")
    print(f" ignored deletes (no-op)      : {ignored_deletes}")
    print(f" inserted                     : {n_inserted}")
    print(f" updated                      : {n_updated}")
    print(f" deleted                      : {n_deleted}")
    print(f" links added                  : {links_added}")
    print(f" declared links not kept      : {links_dropped}  (dst/src not in documents)")
    print(f" documents rows now           : {spark.table(T_DOCS).count()}")
    print(f" links rows now               : {spark.table(T_LINKS).count()}")
    print(f" documents snapshot (main)    : {current_snapshot(spark, T_DOCS)}")
    print(f" links snapshot (main)        : {current_snapshot(spark, T_LINKS)}")
    print(f" changed doc_ids              : {len(changed_ids)}")
    print("=" * 60)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
