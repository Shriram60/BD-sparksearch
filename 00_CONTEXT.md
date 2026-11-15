# Distributed Search Engine — Shared Team Context

> **For Claude:** this is the single source of truth for the project. Read it fully before doing anything.
> Your user owns ONE node (see "Ownership"). Only build what that node owns. Treat everything under
> "Shared contracts" as fixed: if a change seems necessary, tell your user to raise it with the team
> instead of changing it silently, because the other three nodes are coded against it.

Course: Big Data 2026 (UE24CS343AB2), PES University. Team: Shriram, Arnav, Monty, Blason.
Official spec (rubric, dataset, architecture): `Distributed Search Engine.html` in the repo root; this file wins on team decisions, the spec wins on requirements.

---

## 1. What we are building

A mini-Google running on 4 machines connected over ZeroTier:

- **Corpus:** ~1,000 Simple English Wikipedia pages + ~500 links.
- **Pipeline (Spark on HDFS + Iceberg):** ingest → validate → dedupe → merge into Iceberg → inverted index → PageRank → export Parquet.
- **Serving (Node 4):** FastAPI loads the Parquet export into memory, computes BM25 per query, blends it with precomputed PageRank, and serves a web UI.

Two data phases:
- **Phase 0:** `pages.jsonl` (base corpus). Must already be **done and serving before the evaluation starts**.
- **Phase 1:** `changes.jsonl` (updates on top of Phase 0). **Run live in front of the evaluator.**

---

## 2. Ownership

| Node | Hostname | Owner | Runs | Owns |
|---|---|---|---|---|
| 1 | `node1` | **Arnav** | Spark driver for ingestion only (client, no daemons) | Ingestion job, validation, dedup, Iceberg MERGE, change detection, idempotency |
| 2 | `node2` | **Blason** | HDFS NameNode + DataNode, Spark worker | HDFS storage, Iceberg warehouse on HDFS, snapshot/time-travel/rollback demo |
| 3 | `node3` | **Shriram** | Spark master + worker, HDFS DataNode, drivers for index/PageRank/export | Inverted index, PageRank, export job, fault-tolerance demo, cluster configs, pipeline runner script |
| 4 | `node4` | **Monty** | FastAPI + UI (no Spark/HDFS daemons) | Pulling exports, version switching, BM25, score blending, frontend |

Blason does not use Claude. Shriram writes all Node 2 config files for him (Node 2 and Node 3 configs must match anyway).

---

## 3. Evaluation (20 M, scaled to 10)

| # | Item | Marks | Main owner |
|---|---|---|---|
| 1 | Infrastructure & Phase 0 verification | 3 | Blason + Shriram |
| 2 | Live Phase 1 demo | 6 | Shriram (runs the pipeline), everyone |
| 3 | Search verification | 3 | Monty |
| 4 | Idempotency test (re-ingest same phase → no change) | 2 | Arnav |
| 5 | Individual viva | 6 | **Everyone, individually.** Know your own node deeply and the others at a high level. |

On evaluation day: Phase 0 already executed, Node 4 already serving Phase 0 from memory. Only Phase 1 is triggered live.

---

## 4. Shared contracts (do not change without the whole team agreeing)

### 4.1 Versions (identical on Nodes 1, 2, 3)

| Component | Version |
|---|---|
| OS | Ubuntu 22.04/24.04 (any Linux is fine) |
| Java | OpenJDK **11** (`openjdk-11-jdk`) |
| Hadoop | **3.3.6** |
| Spark | **3.5.3**, prebuilt for Hadoop 3, Scala 2.12 |
| Iceberg | `iceberg-spark-runtime-3.5_2.12-1.6.1.jar`, copied into `$SPARK_HOME/jars/` (so no `--packages`, no internet needed at run time) |
| Python | 3.10+ (PySpark jobs use the cluster's Spark 3.5.3; do NOT pip-install a different pyspark version) |

Node 4 only needs Python 3.10+ with `fastapi uvicorn pyarrow pandas requests`.

Install paths (all nodes): `/opt/hadoop`, `/opt/spark`. Linux user: whatever you like, but data dirs are under `/data/`.

### 4.2 Network

- All four machines on the same ZeroTier network. Fill in real IPs here once known:

```
# /etc/hosts — IDENTICAL on all 4 machines
10.147.17.11  node1
10.147.17.12  node2
10.147.17.13  node3
10.147.17.14  node4
```

- **Ubuntu gotcha:** delete or comment out the `127.0.1.1 <your-hostname>` line in `/etc/hosts`, otherwise the NameNode / Spark master binds to loopback and no other node can connect.
- Firewall: `sudo ufw disable` or allow everything from the ZeroTier subnet.
- Fixed ports (so nothing is random):

| Service | Host | Port |
|---|---|---|
| HDFS NameNode RPC | node2 | 9000 |
| HDFS NameNode Web UI / WebHDFS | node2 | 9870 |
| HDFS DataNode HTTP (WebHDFS redirects here) | node2, node3 | 9864 |
| Spark master | node3 | 7077 |
| Spark master UI | node3 | 8080 |
| Spark worker UI | node2, node3 | 8081 |
| Spark driver / block manager | any driver host | 7078 / 7079 |
| Search API + UI | node4 | 8000 |

### 4.3 HDFS layout

`fs.defaultFS = hdfs://node2:9000`, `dfs.replication = 2` (DataNodes on node2 and node3).

```
/searchengine/
  raw/phase0/pages.jsonl          # uploaded by Node 1
  raw/phase1/changes.jsonl        # uploaded by Node 1
  warehouse/                      # Iceberg Hadoop catalog root
  export/v0/ ...                  # export of Phase 0
  export/v1/ ...                  # export of Phase 1
  export/vN/_SUCCESS              # written LAST by the export job
```

### 4.4 Spark config every job uses

```
spark.master                              spark://node3:7077
spark.sql.extensions                      org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lake                    org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lake.type               hadoop
spark.sql.catalog.lake.warehouse          hdfs://node2:9000/searchengine/warehouse
spark.driver.port                         7078
spark.blockManager.port                   7079
spark.driver.host                         <ZeroTier IP of the machine running the driver>
```

Shriram puts these in `/opt/spark/conf/spark-defaults.conf` on nodes 1–3 (except `spark.driver.host`, which each driver machine sets for itself).

### 4.5 Dataset schema

**VERIFIED** by Arnav against the real files (download link in `Distributed Search Engine.html`).
On node1 the files live at `~/data/pages.jsonl` and `~/data/changes.jsonl` (the download names
the Phase 1 file `changes.json`; rename it to `changes.jsonl`, which is what `run_phase.sh` uses).

`pages.jsonl` (Phase 0), 1,040 lines:
```json
{"doc_id": "1", "url": "https://simple.wikipedia.org/wiki/April", "title": "April", "text": "<wikitext>", "links": ["5", "7"]}
```
`changes.jsonl` (Phase 1), 100 lines:
```json
{"doc_id": "94", "op": "UPSERT", "url": "...", "title": "Beard (Phase 1)", "text": "...", "links": ["144"]}
{"doc_id": "1", "op": "DELETE"}
```
- All values are strings; `doc_id` is a string (mostly digits, but also e.g. `phase1-new-001`).
- **There is no `version` field.** Node 1 uses `version = phase number` (Phase 0 rows -> 0, Phase 1
  rows -> 1), so "highest version wins" also works across phases. `documents.version` holds that.
- Links are embedded per page (`links`: array of doc_id strings, ~500 in total). 40 pages have no
  `links` key at all, which is treated as "no links", not as an error.
- `op` only appears in `changes.jsonl`: `UPSERT` or `DELETE`. A `DELETE` row has only `doc_id` + `op`.
- `text` is raw wikitext (`[[links]]`, `{{templates}}`, `'''bold'''`); the tokenizer strips the punctuation.
- Dirty data in Phase 0: 20 truncated JSON lines (`{"doc_id": "malformed-N", "url":`) and 20 doc_ids
  that appear twice (identical copies). Phase 1 has no malformed rows; 7 of its links point at docs
  it deletes.
- Expected Node 1 result: Phase 0 -> 1,000 documents, 499 links, 20 rejected. Phase 1 -> 25 new,
  25 updated, 50 deleted -> 975 documents, 467 links.

### 4.6 Iceberg tables (catalog `lake`, namespace `search`)

| Table | Columns | Written by |
|---|---|---|
| `lake.search.documents` | `doc_id STRING, url STRING, title STRING, text STRING, version BIGINT, phase INT, ingested_at TIMESTAMP` | Node 1 |
| `lake.search.links` | `src_id STRING, dst_id STRING` (only links whose `dst_id` exists in documents) | Node 1 |
| `lake.search.rejected` | `raw STRING, reason STRING, phase INT` | Node 1 |
| `lake.search.ingest_log` | `phase INT, documents_snapshot_id BIGINT, links_snapshot_id BIGINT, changed_doc_ids ARRAY<STRING>, accepted BIGINT, rejected BIGINT, run_at TIMESTAMP` | Node 1 |
| `lake.search.postings` | `term STRING, doc_id STRING, tf INT` | Node 3 |
| `lake.search.doc_stats` | `doc_id STRING, doc_len INT` | Node 3 |
| `lake.search.pagerank` | `doc_id STRING, rank DOUBLE` | Node 3 |

**Tokenizer (shared by Node 3 indexing and Node 4 queries, so they MUST match):**
lowercase → replace every char that is not `[a-z0-9]` with a space → split on whitespace → drop tokens shorter than 2 chars → drop English stopwords (use the list in `common/stopwords.txt`, owned by Shriram). No stemming.

### 4.7 Export contract (Node 3 writes, Node 4 reads)

`hdfs://node2:9000/searchengine/export/v{N}/` where N = phase number (0, 1, ...).

| File | Columns |
|---|---|
| `documents/` (Parquet) | `doc_id, url, title, snippet (first 200 chars of text), doc_len, pagerank` |
| `postings/` (Parquet) | `term, doc_id, tf` |
| `manifest.json` | `{"version": N, "phase": N, "num_docs": int, "num_postings": int, "avgdl": float, "max_pagerank": float, "documents_snapshot_id": int, "created_at": "ISO time"}` |
| `_SUCCESS` | empty file, written **last**, only after everything above is written and row counts were checked |

Node 4 rule: **never switch to a version without `_SUCCESS`**, and if download/validation of vN fails, keep serving the previous version.

### 4.8 Ranking (Node 4)

- BM25 with **k₁ = 1.2, b = 0.75**:
  `BM25(D,Q) = Σ_t IDF(t) · tf·(k₁+1) / (tf + k₁·(1 − b + b·len(D)/avgdl))`
  `IDF(t) = ln( (N − df + 0.5)/(df + 0.5) + 1 )`
- PageRank: damping **0.85**, at least **10 iterations** (Node 3).
- Final score: `0.7 · BM25/max(BM25 over this query's results) + 0.3 · PageRank/max(PageRank over corpus)`.

---

## 5. End-to-end run (what `run_phase.sh N` does, owned by Shriram)

1. **Node 1:** `hdfs dfs -put` the phase file into `raw/phaseN/` → submit `ingest.py --phase N`.
2. **Node 3:** `index.py --phase N` (uses `ingest_log` to update only changed docs).
3. **Node 3:** `pagerank.py --phase N` (full recompute, ≥10 iterations).
4. **Node 3:** `export.py --phase N` → writes `export/vN/`, `_SUCCESS` last.
5. **Node 4:** puller sees `vN/_SUCCESS` → downloads to `/serving/vN.tmp/` → validates against manifest → renames to `/serving/vN/` → hot-swaps in-memory index.

Any step failing stops the run and **Node 4 keeps serving the previous version**.

---

## 6. Demo checklist (rehearse twice before eval)

- [ ] `hdfs dfsadmin -report` shows **2 live DataNodes**; `hdfs fsck /searchengine -files -blocks` shows replication 2.
- [ ] Spark master UI (node3:8080) shows **2 workers**.
- [ ] Node 4 UI serving **v0**; a few prepared queries give sensible results.
- [ ] Run Phase 1 live; show new Iceberg snapshots; Node 4 switches to **v1**; show queries whose results changed.
- [ ] Re-run Phase 1 ingestion → row counts and table contents unchanged (idempotency).
- [ ] Kill a Spark worker mid-PageRank → job still finishes (fault tolerance).
- [ ] Iceberg: list snapshots, `VERSION AS OF` time travel, `rollback_to_snapshot`.
- [ ] Prepared queries list in `demo/queries.md` with expected top results for v0 and v1.

---

## 7. Repo layout

```
search-engine/
  00_CONTEXT.md            # this file
  tasks/                   # one task file per person
  common/stopwords.txt     # shared tokenizer stopwords (Shriram)
  common/tokenizer.py       # reference tokenizer, copied by Node 3 & Node 4
  conf/                    # Hadoop + Spark configs for nodes 1–3 (Shriram)
  node1_ingest/            # Arnav
  node3_compute/           # Shriram
  node4_search/            # Monty
  scripts/run_phase.sh     # Shriram
  demo/                    # queries.md, iceberg_demo.sql, runbooks
```

Develop with the toy dataset in Spark **local mode** (`--master local[*]` with a local Iceberg warehouse path) until the cluster is up. Final runs must use the main corpus.
