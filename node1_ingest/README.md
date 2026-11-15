# Node 1 — Ingestion (Arnav)

Spark driver for ingestion only. Node 1 runs **no Spark or HDFS daemons**; it is a client that
uploads raw files to HDFS and submits `ingest.py` to the Spark master on node3.

| File | What it is |
|---|---|
| `ingest.py` | The ingestion job: parse → validate → dedup → change detection → MERGE → links → ingest_log |
| `run_ingest.sh` | `hdfs dfs -put -f` the phase file into `raw/phaseN/`, then `spark-submit ingest.py` |
| `spark_args.sh` | `LOCAL=1` → local mode + local warehouse; otherwise uses `spark-defaults.conf` |
| `table_state.py` | Row count + content hash + current snapshot of each Node 1 table (for the idempotency demo) |
| `toy_data/` | Small Phase 0 / Phase 1 files covering every reject reason, dups, stale versions, deletes |
| `../demo/idempotency.sh` | Idempotency demo: fingerprint → re-ingest → fingerprint → `UNCHANGED` |

## Running

```bash
# Dev / testing, no cluster needed (warehouse at file:///tmp/searchengine/warehouse)
rm -rf /tmp/searchengine/warehouse
LOCAL=1 node1_ingest/run_ingest.sh 0 node1_ingest/toy_data/pages.jsonl
LOCAL=1 node1_ingest/run_ingest.sh 1 node1_ingest/toy_data/changes.jsonl
LOCAL=1 demo/idempotency.sh 1 node1_ingest/toy_data/changes.jsonl

# Cluster (on node1). Files default to $DATA_DIR/{pages,changes}.jsonl, DATA_DIR=~/data
node1_ingest/run_ingest.sh 0
node1_ingest/run_ingest.sh 1
demo/idempotency.sh 1
```

`scripts/run_phase.sh` (Shriram) uploads the file itself and calls `ingest.py --phase N` on node1 over ssh;
`ingest.py` reads `/searchengine/raw/phaseN/` by default, so both entry points work.

## Node 1 setup checklist

- Java 11, Hadoop 3.3.6 (client only), Spark **3.5.3** in `/opt/spark`,
  `iceberg-spark-runtime-3.5_2.12-1.6.1.jar` in `/opt/spark/jars/`.
- `/etc/hosts` with node1–node4, the `127.0.1.1` line commented out.
- Hadoop `core-site.xml` / `hdfs-site.xml` and `spark-defaults.conf` from Shriram's `conf/`.
- In `/opt/spark/conf/spark-defaults.conf` on node1 add:
  `spark.driver.host <node1 ZeroTier IP>` (plus `spark.driver.port 7078`, `spark.blockManager.port 7079`).
- Test: `hdfs dfs -ls /` works, and
  `spark-submit --master spark://node3:7077 /opt/spark/examples/src/main/python/pi.py 10`
  appears on `http://node3:8080`.

## What the job does (CONTEXT §4.6, ARNAV_node1.md §C)

1. **Parse.** `spark.read.text` + `from_json` (PERMISSIVE, with a `_corrupt_record` column). Every
   field is parsed as a STRING first, so one bad value never kills the job or nulls the whole record;
   we can then say exactly *what* is wrong. The raw line is kept for the rejected table.
2. **Validate.** First failing rule becomes the `reason` in `lake.search.rejected`:
   `invalid_json`, `bad_op`, `missing_doc_id`, `missing_version`, `bad_version_type` (not a
   non-negative integer: `"abc"`, `1.5`), `missing_url`, `bad_url` (not http/https), `missing_title`,
   `missing_text` (empty/whitespace), `bad_links_type` (links not an array). Deletes
   (`"op": "delete"`) only need `doc_id` + `version`. Blank lines are skipped, not rejected.
3. **Dedup in the batch.** `row_number()` over `partitionBy(doc_id).orderBy(version desc, is_delete desc, raw asc)`,
   keep row 1. The extra tie-breakers make the winner deterministic, so reruns pick the same row.
4. **Change detection.** Left join with current `documents` on `doc_id`. Keep only *new* docs or a
   **strictly higher** version. Equal/lower versions are ignored. A delete only applies if the doc
   exists and the delete's version is higher.
   The change set is materialised (`localCheckpoint`) before any write, so Spark can't lazily
   recompute it against the already-updated table.
5. **Nothing changed → write nothing.** No MERGE, no links write, no `ingest_log` row → no new
   snapshots anywhere. This is the idempotency guarantee.
6. **MERGE into `documents`** — one atomic Iceberg commit (one snapshot):
   ```sql
   MERGE INTO lake.search.documents t USING changes s ON t.doc_id = s.doc_id
   WHEN MATCHED AND s.op='delete' AND s.version > t.version THEN DELETE
   WHEN MATCHED AND s.op='upsert' AND s.version > t.version THEN UPDATE SET ...
   WHEN NOT MATCHED AND s.op='upsert' THEN INSERT ...
   ```
7. **Links.** Node 1 keeps an internal table `lake.search.raw_links` with every link each document
   *declares* (deduped, no self-links). For changed docs their old out-links are deleted and the new
   ones inserted. Then `links` = `raw_links` where **both** `src_id` and `dst_id` exist in
   `documents`, synced with one `MERGE … WHEN NOT MATCHED BY SOURCE THEN DELETE` (only if
   something differs). Why the extra table: if page A links to B and B only arrives in Phase 1, the
   A→B link must appear in Phase 1 even though A itself didn't change. Links into a deleted doc are
   removed automatically for the same reason.
   `raw_links` and `tombstones` are Node 1 private; they do not change any shared contract.
8. **`ingest_log`.** One row per run that changed something: phase, the `main` snapshot ids of
   `documents` and `links` after the writes, `changed_doc_ids` (inserted + updated + deleted),
   accepted and rejected counts. Node 3's `index.py` reads the latest row for the phase.
   Because no-op reruns don't append a row, the latest row always holds the real changes, so a
   rerun of ingest followed by a rerun of index can't lose updates.
9. **Rejected** rows are deduped on `(raw, phase)` and only *new* ones are MERGEd in, so reruns
   don't duplicate them either.

### Deletes and re-ingesting an older phase
Node 1 keeps a private `lake.search.tombstones (doc_id, version)` table with every applied delete.
Change detection compares an incoming record against the stored version **or** the tombstone
version, so re-running Phase 0 after Phase 1 does not resurrect deleted documents (tested: the
fingerprints stay identical). A doc can still come back with a *higher* version than its delete.

### Tested locally (toy data, Spark 3.5.3 + Iceberg 1.6.1, local mode)
Phase 0: 11 inserted, 12 rejected (11 stored, the repeated bad line once), 2 duplicates
collapsed, 16 links (1→99 dangling, dropped). Phase 1: 2 inserted, 1 updated, 1 deleted,
2 stale + 2 no-op deletes ignored, links +4 −2 (1→99 restored, links to/from deleted doc 10
removed). `demo/idempotency.sh 1` and `demo/idempotency.sh 0` both print `UNCHANGED`.

## Viva notes

- **Why `spark.driver.host`?** In client mode the driver runs on node1. Executors on node2/node3
  must open connections *back* to the driver (to fetch tasks, send results, and the block manager).
  The driver advertises its address; without the setting it may advertise `127.0.1.1`/the wrong
  interface, which the executors can't reach over ZeroTier. Fixed ports 7078/7079 make the firewall
  story simple.
- **Iceberg MERGE semantics.** Copy-on-write by default: data files containing matched rows are
  rewritten, and the new table state is committed as **one new snapshot** by atomically swapping the
  metadata pointer. Readers see either the old or the new snapshot, never half. Each `WHEN` clause
  is evaluated per matched/unmatched row; the first clause whose condition holds is applied.
- **Snapshots.** Every commit (MERGE, DELETE, append) creates a snapshot with an id. Time travel:
  `SELECT … FROM lake.search.documents VERSION AS OF <id>`; rollback:
  `CALL lake.system.rollback_to_snapshot('search.documents', <id>)`. `ingest_log` records which
  snapshot each phase produced.
- **How idempotency is guaranteed.** (a) dedup is deterministic, (b) change detection only keeps
  strictly higher versions (than the stored doc or its tombstone), so a second run sees 0 changes, (c) zero changes → zero writes, so no new
  snapshots, (d) rejects are inserted only if `(raw, phase)` is not already stored. Proven by
  `demo/idempotency.sh`: counts, content hashes and snapshot ids identical before and after.
- **Why `from_json` on `spark.read.text`, not `spark.read.json`?** `read.json` infers one schema for
  the file and can put a whole bad line into `_corrupt_record` without telling you which field was
  wrong; parsing per line with an explicit schema keeps the raw line and lets each rule give a
  precise reason. It also never fails the job.
- **Why highest version wins, not latest line?** Line order in a distributed read is not
  guaranteed; `version` is the only reliable ordering.
