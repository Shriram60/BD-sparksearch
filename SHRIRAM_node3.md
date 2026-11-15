# Node 3: Shriram (Spark master + worker, HDFS DataNode, compute)

Node 3 is the compute centre of the cluster. It runs the **Spark master** (every job is submitted
here), a **Spark worker**, and the second **HDFS DataNode**. It also hosts the drivers for the three
compute jobs: inverted index, PageRank and export. Shriram also owns the shared configs, the
version-pinning tools, the shared tokenizer and the end-to-end runner.

## Rubric
| Item | Marks | Role |
|---|---|---|
| Live Phase 1 demo | 6 | runs `run_phase.sh 1`, explains each step as it runs |
| Infrastructure & Phase 0 | 3 | shared with Blason: 2 workers, 2 DataNodes, replication |
| Individual viva | 6 | Spark architecture, stages, fault tolerance, PageRank, inverted index, Iceberg |

## What Node 3 owns

| Path | What |
|---|---|
| `node3_compute/index.py` | inverted index → `lake.search.postings`, `lake.search.doc_stats` |
| `node3_compute/pagerank.py` | PageRank → `lake.search.pagerank` |
| `node3_compute/export.py` | Parquet export for Node 4 → `/searchengine/export/vN/` |
| `node3_compute/common_spark.py` | Spark session (cluster or `--local`), stopwords, `ingest_log` lookup |
| `scripts/run_phase.sh` | one phase end to end, with timings |
| `conf/` | Hadoop + Spark configs for nodes 1-3, `hosts`, `install_conf.sh` |
| `tools/` | pinned versions, `install_node.sh`, `check_versions.sh` |
| `common/tokenizer.py`, `common/stopwords.txt` | tokenizer shared with Node 4 |
| `demo/node3_demos.md`, `demo/make_iceberg_demo.py` | demo runbook, Iceberg demo SQL generator |

## The three jobs

### `index.py --phase N`
- Reads the latest `lake.search.ingest_log` row for phase N (written by Node 1).
- **Phase 0:** tokenizes every document. **Phase N > 0:** re-tokenizes only `changed_doc_ids` and
  keeps every other document's postings. Deleted docs simply drop out.
- Tokenizer, in built-in Spark SQL functions (identical rules to `common/tokenizer.py`): lowercase →
  every non-`[a-z0-9]` character becomes a space → split → drop tokens under 2 chars → drop stopwords.
- Writes each table in **one overwrite commit**: readers never see a half-updated index, and a rerun
  gives identical tables.
- Real data: Phase 0 → 400,519 postings, 72,408 terms. Phase 1 → only 100 docs re-indexed, 377,989 postings.

### `pagerank.py --phase N [--iterations 20]`
- Full recompute each phase over `documents` + `links`. Damping 0.85, initial rank 1/N, 20 iterations
  (minimum 10 enforced). Prints the L1 delta each iteration.
- Dangling pages (no out-links) spread their rank evenly over all N pages, so ranks sum to 1.
- Checkpoints ranks to HDFS every 5 iterations: short lineage, and lost partitions are recomputed
  from the checkpoint if a worker dies.
- Real data: N = 1000, 499 edges, 501 dangling; L1 delta falls from 0.43 to 0.00015 over 20 iterations.

### `export.py --phase N [--force]`
- Joins documents + doc_stats + pagerank → `documents/` Parquet (doc_id, url, title, 200-char
  snippet, doc_len, pagerank). Copies postings → `postings/` Parquet.
- Reads both back and validates them: no null ids, no orphan postings, counts > 0. Then writes
  `manifest.json` and **finally `_SUCCESS`**.
- `vN` already complete → skip. Half-written → delete and redo. `--force` rebuilds.

## Running

```bash
# whole phase (ingest on node1 over ssh, then index, PageRank, export, wait for node4)
./scripts/run_phase.sh N
# if Arnav runs ingestion himself on node1
./scripts/run_phase.sh N --skip-ingest

# single jobs
cd node3_compute
spark-submit index.py --phase N
spark-submit pagerank.py --phase N
spark-submit export.py --phase N

# local mode on toy data (no cluster): add --local; warehouse in /tmp/searchengine
```
Real timings: Phase 0 node3 part **72 s**, Phase 1 **75 s**.

## Cluster setup and daemons
The full order is in `demo/cluster_bringup.md`. Node 3's own part:
```bash
./tools/install_node.sh                 # once
./conf/install_conf.sh node3            # once, or after config changes
start-master.sh && start-worker.sh spark://node3:7077
hdfs --daemon start datanode            # after node2's NameNode is up
```
Check: http://node3:8080 shows 2 workers, and `hdfs dfsadmin -report` shows `Live datanodes (2)`.

## Demos
Details in `demo/node3_demos.md`.
- **Fault tolerance:** run `pagerank.py --phase 1 --iterations 30`, Blason runs `stop-worker.sh` on
  node2 at about iteration 5, and the job still finishes on node3. Restart the worker afterwards.
- **Replication:** `hdfs fsck /searchengine -files -blocks -locations` shows each block on node2 and node3.
- **Iceberg demo SQL for Blason:** after each Phase 1 run, `spark-submit demo/make_iceberg_demo.py`,
  then commit and push `demo/iceberg_demo.sql`.

## Viva notes
- **Master / worker / driver / executor:** the master only allocates resources. Workers launch
  executors. The driver (on the submitting machine) builds the DAG and schedules tasks. Executors
  connect back to the driver on 7078/7079, which is why every driver machine needs
  `spark.driver.host` and an open firewall (Arnav's first job hung exactly on this).
- **Stages and shuffles:** each join / groupBy is a shuffle boundary that starts a new stage.
  Shuffle partitions went 200 → 8: PageRank 256 s → 34 s, because each iteration had been
  scheduling hundreds of empty tasks.
- **Fault tolerance:** RDDs and DataFrames remember their lineage. If an executor dies, its lost
  partitions are recomputed. The checkpoint every 5 iterations cuts the lineage so recomputation
  stays cheap. It's a reliable HDFS checkpoint, not a local one, because a local checkpoint dies
  with the worker.
- **PageRank:** `PR(p) = (1−d)/N + d·(Σ PR(q)/out(q) + dangling_mass/N)`. Example: Chemistry fell 6×
  in Phase 1 because its only in-link (from Britain) was rewired. Cooking gained 2 in-links but
  still fell, because its main source lost rank upstream. Verified with a separate implementation.
- **Inverted index:** term → (doc_id, tf), plus doc_len for BM25 length normalisation.
  Incremental: only `changed_doc_ids` are re-tokenized.
- **Iceberg:** every commit is a snapshot, an atomic swap of the metadata pointer. Time travel with
  `VERSION AS OF`, rollback with `rollback_to_snapshot`. Our writes are single commits, so they're
  all-or-nothing.
- **Why `_SUCCESS` last:** Node 4 treats its presence as "the export is complete and validated";
  a crash mid-export leaves no `_SUCCESS`, so Node 4 keeps serving the old version.
