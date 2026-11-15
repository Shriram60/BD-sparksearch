# BD-sparksearch

A distributed mini search engine on 4 machines. **Spark** on **HDFS** with **Apache Iceberg** tables
ingests about 1,000 Simple English Wikipedia pages, builds an inverted index and computes PageRank.
A **FastAPI** service ranks results with **BM25 + PageRank** and serves a web UI. The data arrives
in two phases, and the search service switches from version v0 to v1 live, without downtime.

Big Data 2026 (UE24CS343AB2), PES University.

| Node | Person | Role |
|---|---|---|
| node1 | Arnav | ingestion: validation, dedup, Iceberg MERGE, change detection |
| node2 | Blason | HDFS NameNode + DataNode, Spark worker, Iceberg snapshot demo |
| node3 | Shriram | Spark master + worker, HDFS DataNode, index / PageRank / export, configs, runner |
| node4 | Monty | search API + UI, BM25 + PageRank blend, hot version switching |

## How it works

```
 pages.jsonl / changes.jsonl
        │  node1: upload to HDFS, validate, dedup, MERGE into Iceberg
        ▼
 Iceberg tables on HDFS (node2 + node3, replication 2)
        │  node3: inverted index → PageRank (20 iterations) → Parquet export, _SUCCESS last
        ▼
 /searchengine/export/vN/
        │  node4: pull over WebHDFS → validate → atomic rename → hot swap
        ▼
 http://node4:8000   BM25 (k₁ 1.2, b 0.75) × 0.7  +  PageRank × 0.3
```

- **Phase 0** (`pages.jsonl`): 1,040 lines → 20 rejected, 20 duplicates → **1,000 docs, 499 links**.
- **Phase 1** (`changes.jsonl`): 25 added, 25 updated, 50 deleted → **975 docs, 467 links**. Only
  the 100 changed docs are re-indexed. Node 4 switches to v1 by itself.
- Re-ingesting a phase changes nothing (idempotent). Every Iceberg write is a snapshot, so you can
  time travel and roll back.

## Documentation

| Read this | For |
|---|---|
| [`PROJECT_OVERVIEW.md`](PROJECT_OVERVIEW.md) | **start here**: every component, decisions and why, what has happened, real numbers, status |
| [`demo/cluster_bringup.md`](demo/cluster_bringup.md) | **run plan**: who runs what, in what order, and what to wait for |
| [`00_CONTEXT.md`](00_CONTEXT.md) | shared contracts: versions, ports, HDFS layout, table schemas, export format, ranking |
| [`Distributed Search Engine.html`](Distributed%20Search%20Engine.html) | official spec and rubric (open in a browser) |
| [`demo/verification_runbook.md`](demo/verification_runbook.md) | course verification checks mapped to our setup, with results |
| [`demo/queries.md`](demo/queries.md) | demo queries with expected v0 / v1 results |
| `ARNAV_node1.md`, `BLASON_node2.md`, `SHRIRAM_node3.md`, `MONTY_node4.md` | per-node guides and viva notes |
| `node1_ingest/README.md`, `node4_search/README.md` | implementation details for those nodes |

## Stack (identical on nodes 1-3)

| | Version |
|---|---|
| Java | OpenJDK 11 |
| Hadoop | 3.3.6 |
| Spark | 3.5.3 (Scala 2.12) |
| Iceberg | 1.6.1 (`iceberg-spark-runtime-3.5_2.12`) |
| Python | 3.12 |
| Network | ZeroTier, network `3b19b3a71682751e` |

Versions live only in [`tools/versions.env`](tools/versions.env). Node 4 needs only Python and the
pinned packages in `tools/requirements-node4.txt`.

## Quick start

**1. Install and check (every node)**
```bash
git clone https://github.com/Shriram60/BD-sparksearch && cd BD-sparksearch
./tools/install_node.sh            # nodes 1-3   (node 4: ./tools/install_node.sh node4)
./tools/check_versions.sh          # FINGERPRINT line must match on nodes 1-3
```

**2. Network and configs (nodes 1-3)**
```bash
sudo hostnamectl set-hostname nodeN
# add conf/hosts to /etc/hosts and comment out the 127.0.1.1 line
sudo ufw allow from 10.113.232.0/24
./conf/install_conf.sh nodeN
```

**3. Start the cluster** (order matters; full table in the run plan)
```bash
# node2
hdfs namenode -format                     # first time ever only
hdfs --daemon start namenode && hdfs --daemon start datanode
# node3
start-master.sh && start-worker.sh spark://node3:7077
hdfs --daemon start datanode
# node2
start-worker.sh spark://node3:7077
# node4
cd node4_search && .venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000
```
Check: http://node3:8080 shows 2 workers, http://node2:9870 shows 2 DataNodes, http://node4:8000 is up.

**4. Run a phase**
```bash
node1_ingest/run_ingest.sh 0                   # node1
./scripts/run_phase.sh 0 --skip-ingest         # node3: index → PageRank → export → node4 switch
```
Then the same with `1`. With ssh from node3 to node1 set up, `./scripts/run_phase.sh N` does all of it.

**5. Demos**
```bash
demo/idempotency.sh 1                          # node1 → RESULT: UNCHANGED
spark-submit demo/make_iceberg_demo.py         # node3 → demo/iceberg_demo.sql for node2
```
Fault tolerance and replication: `demo/node3_demos.md`.

**Local mode (no cluster):** `LOCAL=1 node1_ingest/run_ingest.sh 0 node1_ingest/toy_data/pages.jsonl`,
then `spark-submit node3_compute/index.py --phase 0 --local` (same for `pagerank.py` and `export.py`).

## Repository layout

```
tools/            pinned versions, install_node.sh, check_versions.sh
conf/             Hadoop + Spark configs, hosts, install_conf.sh
common/           tokenizer.py + stopwords.txt (shared by node3 indexing and node4 queries)
node1_ingest/     ingest.py, run_ingest.sh, reset_tables.*, table_state.py, toy_data/
node3_compute/    index.py, pagerank.py, export.py, common_spark.py
node4_search/     app.py, puller.py, search_index.py, static/ (UI), make_fake_export.py
scripts/          run_phase.sh (one phase end to end, with timings)
demo/             run plan, verification runbook, queries, idempotency + Iceberg demo scripts
```

## Ports

| Service | Host:port |
|---|---|
| HDFS NameNode RPC / UI + WebHDFS | node2:9000 / node2:9870 |
| HDFS DataNode HTTP | node2, node3 :9864 |
| Spark master / master UI / worker UI | node3:7077 / node3:8080 / :8081 |
| Spark driver / block manager | driver host :7078 / :7079 |
| Search API + UI | node4:8000 |

## Status

| Rubric | Marks | |
|---|---|---|
| Infrastructure & Phase 0 verification | 3 | ⏳ final run needs node2 (2 DataNodes, 2 workers) |
| Live Phase 1 demo | 6 | ✅ rehearsed on real data, about 75 s |
| Search verification | 3 | ✅ demo queries confirmed |
| Idempotency | 2 | ✅ `UNCHANGED` |
| Individual viva | 6 | ⏳ per person |

Details and remaining steps: [`PROJECT_OVERVIEW.md`](PROJECT_OVERVIEW.md) §11.
