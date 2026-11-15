# Arnav — Node 1 (Ingestion)

> **For Claude:** read `00_CONTEXT.md` first. Arnav owns Node 1 only: the ingestion Spark job and
> data quality. Node 1 is a **client**: it runs the Spark driver but no Spark or HDFS daemons.
> Do not change the Iceberg table schemas or `ingest_log` columns in CONTEXT §4.6 without telling
> the team; Node 3 reads them.

## Rubric you carry
- Idempotency test (2 M): re-ingesting the same phase must not change table contents.
- Phase 1 live demo: your ingestion is step 1.
- Viva: validation rules, dedup logic, Iceberg MERGE semantics, snapshots, how idempotency is guaranteed, why the driver needs `spark.driver.host`.

---

## A. Node 1 setup
- Java 11, Hadoop 3.3.6 (client only, for `hdfs dfs` commands), Spark 3.5.3, Iceberg jar in `/opt/spark/jars/`. Shriram gives you `conf/` files.
- In your `spark-defaults.conf` set `spark.driver.host = <node1 ZeroTier IP>` (otherwise executors on node2/node3 can't call back to your driver).
- Test: `hdfs dfs -ls /` works and `spark-submit --master spark://node3:7077` runs a trivial job that shows up on `node3:8080`.

## B. First thing: inspect the data
`head -n 3` both files, then **update CONTEXT §4.5** with the real field names and tell the team. Check for deletes in `changes.jsonl` and for what "malformed" looks like.

## C. `node1_ingest/ingest.py --phase N`

1. **Upload** (in a wrapper script or the job): `hdfs dfs -put -f <file> /searchengine/raw/phaseN/`.
2. **Parse:** read as text (`spark.read.text`), parse each line with `from_json` against an explicit schema, so a bad line gives nulls instead of killing the job. Keep the raw line.
3. **Validate → reject** (write to `lake.search.rejected` with a `reason`):
   - unparseable JSON
   - missing/empty required fields (`doc_id`, `url`, `title`, `text`, `version`, whatever the real schema requires)
   - wrong types (version not an int)
   - add any other dirty patterns you find in the data
4. **Dedup by doc_id, highest version wins:** window `partitionBy(doc_id).orderBy(version desc)` → keep row_number 1.
5. **Find real changes:** anti-join against current `lake.search.documents` on `(doc_id, version)`; keep only rows that are new docs or have a **higher** version than what's stored. Lower or equal versions are ignored.
6. **If nothing changed:** do NOT write anything to documents/links; just log "0 changes". This is what makes the idempotency demo clean (no new snapshot, identical contents).
7. **MERGE into `lake.search.documents`:**
   ```sql
   MERGE INTO lake.search.documents t USING changes s ON t.doc_id = s.doc_id
   WHEN MATCHED AND s.version > t.version THEN UPDATE SET *
   WHEN NOT MATCHED THEN INSERT *
   ```
   Handle deletes too if the change set has them (`WHEN MATCHED AND s.op = 'delete' THEN DELETE`).
8. **Links:** for changed docs, delete their old outgoing links and insert the new ones. Then **drop links whose `dst_id` is not in documents** (and links from/to deleted docs). Dedupe `(src_id, dst_id)` and drop self-links.
9. **Append a row to `lake.search.ingest_log`**: phase, new snapshot ids of documents and links, `changed_doc_ids` (new + updated + deleted), accepted and rejected counts. Node 3 reads this to update only what changed.
10. Print a summary: read, rejected (by reason), duplicates collapsed, inserted, updated, deleted, links dropped.

Rejected rows: if a rerun would insert the same rejects again, dedupe them (e.g. MERGE on `raw` + `phase`), so reruns are fully idempotent.

## D. Idempotency demo (prepare a script)
`demo/idempotency.sh`: record `count(*)` + a content hash (e.g. `sum(hash(*))`) of documents and links + latest snapshot id → rerun `ingest.py --phase 1` → record again → print "UNCHANGED" if equal.

## E. Order
1. Inspect data, fix CONTEXT §4.5.
2. Build and test `ingest.py` in **local mode** with toy data and a local warehouse (`file:///tmp/warehouse`).
3. Switch to the cluster once Shriram says it's up.
4. Run idempotency script after Phase 0 and after Phase 1.
