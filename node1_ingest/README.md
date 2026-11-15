# Node 1 — Ingestion (Arnav)

Spark driver for ingestion only. Node 1 runs **no Spark or HDFS daemons**; it is a client that
uploads raw files to HDFS and submits `ingest.py` to the Spark master on node3.

| File | What it is |
|---|---|
| `ingest.py` | The ingestion job: parse → validate → dedup → change detection → MERGE → links → ingest_log |
| `run_ingest.sh` | `hdfs dfs -put -f` the phase file into `raw/phaseN/`, then `spark-submit ingest.py` |
| `spark_args.sh` | `LOCAL=1` → local mode + local warehouse; otherwise uses `spark-defaults.conf` |
| `table_state.py` | Row count + content hash + current snapshot of each Node 1 table (for the idempotency demo) |
| `reset_tables.sh` / `.py` | Drop the tables to start again from Phase 0 (needed when switching toy data → real data) |
| `../demo/node1_checks.sql` | Read-only verification queries for the demo (counts, reject reasons, no dups, no dangling links, ingest_log, snapshots) |
| `toy_data/` | Small Phase 0 / Phase 1 files covering every reject reason, dups, stale versions, deletes |
| `../demo/idempotency.sh` | Idempotency demo: fingerprint → re-ingest → fingerprint → `UNCHANGED` |

## Running

```bash
# Dev / testing, no cluster needed (warehouse at file:///tmp/searchengine/warehouse)
rm -rf /tmp/searchengine/warehouse
LOCAL=1 node1_ingest/run_ingest.sh 0 node1_ingest/toy_data/pages.jsonl
LOCAL=1 node1_ingest/run_ingest.sh 1 node1_ingest/toy_data/changes.jsonl
LOCAL=1 demo/idempotency.sh 1 node1_ingest/toy_data/changes.jsonl

# Real corpus, local mode (files in ~/data/pages.jsonl and ~/data/changes.jsonl)
LOCAL=1 node1_ingest/run_ingest.sh 0 && LOCAL=1 node1_ingest/run_ingest.sh 1

# Cluster (on node1). Files default to $DATA_DIR/{pages,changes}.jsonl, DATA_DIR=~/data
node1_ingest/run_ingest.sh 0
node1_ingest/run_ingest.sh 1
demo/idempotency.sh 1
```

**Switching from toy data to the real `pages.jsonl`: reset first.** Toy doc_ids (`1`–`5`) collide
with real ones at the same version 0, so without a reset the real records would be ignored as
"no change":
```bash
node1_ingest/reset_tables.sh --all            # asks for 'yes'; LOCAL=1 for the local warehouse
hdfs dfs -rm -r -f /searchengine/export       # old exports (Shriram), and /serving/v* on node4 (Monty)
```
Course toy files in standard names: `DATA_DIR=~/data/toy node1_ingest/run_ingest.sh 0`.

Verification queries: `spark-sql -f demo/node1_checks.sql` (prefix with `LOCAL=1` flags locally).

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
   `invalid_json`, `bad_op`, `missing_doc_id`, `bad_version_type` (a `version` is present but not a
   non-negative integer), `missing_url`, `bad_url` (not http/https), `missing_title`,
   `missing_text` (empty/whitespace), `bad_links_type` (links not an array). Deletes
   (`"op": "DELETE"`) only need `doc_id`. A missing `links` key means "no links". Blank lines are
   skipped, not rejected.
   **Version:** the real data has no `version` field, so a record's version = its **phase number**
   (an explicit `version`, if a file ever has one, is used instead). Phase 1 rows (v1) therefore beat
   Phase 0 rows (v0), and re-running an older phase can never overwrite a newer one.
3. **Dedup in the batch.** `row_number()` over `partitionBy(doc_id).orderBy(version desc, line_no asc)`,
   keep row 1: highest version wins, and for equal versions the **first** line in the file wins
   (`line_no` = file order). Deterministic, so reruns pick the same row.
4. **Change detection.** Left join with current `documents` on `doc_id`. Keep only *new* docs or a
   **strictly higher** version (explicit version, else the phase number). Equal/lower versions are ignored. A delete only applies if the doc
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

### Tested locally (Spark 3.5.3 + Iceberg 1.6.1, local mode, Python 3.12)
| Data | Result |
|---|---|
| Real Phase 0 (`pages.jsonl`, 1,040 lines) | 20 rejected (`invalid_json`), 20 duplicates collapsed, **1,000 docs, 499 links** |
| Real Phase 1 (`changes.jsonl`, 100 lines) | 25 inserted, 25 updated, 50 deleted -> **975 docs, 467 links** (7 links to deleted docs dropped) |
| Re-run Phase 1, re-run Phase 0 after Phase 1 | `demo/idempotency.sh`: **UNCHANGED** both times |
| Node 3 jobs on top (index -> pagerank -> export) | `export/v0` (1,000 docs) and `export/v1` (975 docs) published with `_SUCCESS` |
| Course toy data | 2 rejected (`missing_doc_id`), dup "Soil duplicate" dropped; Phase 1: 1 new, 1 updated, 1 deleted |
| `toy_data/` (own edge cases) | every reject reason, explicit versions, stale versions, no-op deletes |

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
