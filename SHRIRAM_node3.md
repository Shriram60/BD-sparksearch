# Shriram — Node 3 (Spark master + worker, HDFS DataNode, compute)

> **For Claude:** read `00_CONTEXT.md` first. Shriram owns Node 3, the shared configs, and the
> end-to-end runner. He is also the integrator, so he can touch shared contracts, but any change
> must be announced to the team. Do not run `git add/commit/push`; tell Shriram what to commit.

## Rubric you carry
- Live Phase 1 demo (6 M): you drive it.
- Infrastructure (3 M): shared with Blason.
- Viva: Spark master/worker/driver/executor, DAGs and stages, lineage-based fault tolerance, PageRank, inverted index, Iceberg incremental reads.

---

## A. Cluster setup (do first, with Blason on a call)

1. Write `conf/` for nodes 1–3 and hand Blason his copies:
   - `core-site.xml`: `fs.defaultFS = hdfs://node2:9000`
   - `hdfs-site.xml`: `dfs.replication=2`, NameNode dir `/data/hdfs/namenode` (node2), DataNode dir `/data/hdfs/datanode`, `dfs.namenode.rpc-bind-host=0.0.0.0`, `dfs.namenode.http-bind-host=0.0.0.0`, `dfs.client.use.datanode.hostname=true`, `dfs.datanode.use.datanode.hostname=true`, `dfs.namenode.datanode.registration.ip-hostname-check=false`
   - `hadoop-env.sh`: `JAVA_HOME`
   - `spark-env.sh`: `SPARK_MASTER_HOST=node3`, `SPARK_LOCAL_IP=<this node's ZeroTier IP>`, `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY` (2–4g depending on RAM)
   - `spark-defaults.conf`: everything in CONTEXT §4.4
2. Download the Iceberg runtime jar into `/opt/spark/jars/` on nodes 1, 2, 3.
3. On node3: start DataNode (`hdfs --daemon start datanode`), Spark master (`start-master.sh`), Spark worker (`start-worker.sh spark://node3:7077`).
4. Verify: `hdfs dfsadmin -report` shows 2 live DataNodes; master UI at `node3:8080` shows 2 workers.
5. Write a `common/tokenizer.py` + `common/stopwords.txt` and send to Monty (Node 4 must tokenize queries identically).

## B. Jobs (`node3_compute/`)

### `index.py --phase N`
- Read the latest row of `lake.search.ingest_log` for phase N → `changed_doc_ids`.
- Phase 0: build postings for all docs. Phase N>0: delete postings + doc_stats for changed doc_ids (and deleted docs), then insert recomputed rows for the changed docs. Use Iceberg `MERGE`/`DELETE` so it's a clean snapshot.
- Tokenize with the shared tokenizer (as a Spark UDF or `regexp_replace` + `split` + `explode`).
- Output: `lake.search.postings (term, doc_id, tf)`, `lake.search.doc_stats (doc_id, doc_len)`.
- Must be idempotent: rerunning with the same ingest_log row gives the same tables.

### `pagerank.py --phase N`
- Read `lake.search.documents` + `lake.search.links`. Full recompute every phase (it's global).
- Damping 0.85, **≥10 iterations** (use 20, log the L1 delta per iteration for the viva).
- Handle dangling nodes (no out-links): redistribute their mass uniformly over all N pages.
- Initial rank 1/N. Cache/checkpoint the ranks RDD/DataFrame every few iterations to keep lineage short.
- Write `lake.search.pagerank` (overwrite).

### `export.py --phase N`
- Join documents + doc_stats + pagerank → `documents/` Parquet; copy postings → `postings/` Parquet.
- Compute `num_docs`, `avgdl`, `max_pagerank`, row counts → `manifest.json`.
- Validate: no null doc_ids, every postings doc_id exists in documents, counts > 0. Only then write `_SUCCESS`.
- If `export/vN/` already exists without `_SUCCESS`, delete it and redo; if it exists with `_SUCCESS`, skip (idempotent).

### `scripts/run_phase.sh N`
- Runs the steps in CONTEXT §5 in order, prints timings, stops on first failure (`set -e`).
- Ingestion is submitted from node1 (ssh to node1, or Arnav runs his step and you run the rest; decide which).

## C. Demos you own
- **Fault tolerance:** during `pagerank.py`, `stop-worker.sh` on node2 → show the master UI marking it dead and tasks re-running on node3, job still finishes. Restart the worker after.
- **HDFS replication:** `hdfs fsck /searchengine -files -blocks -locations` shows each block on node2 and node3.
- Fill `demo/queries.md` with Monty: 8–10 queries, expected top results for v0 and v1, including at least 3 whose results change in Phase 1.

## D. Order
1. Configs + cluster up with Blason.
2. Write jobs against toy data in local mode while the cluster comes up.
3. Agree export contract with Monty, ingest_log contract with Arnav (already in CONTEXT; just confirm).
4. Full Phase 0 on the main corpus → Node 4 serving v0.
5. Rehearse Phase 1 twice (snapshot + rollback between rehearsals so Phase 0 state is restorable).
