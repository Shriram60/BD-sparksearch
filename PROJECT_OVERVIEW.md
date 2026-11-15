# BD-sparksearch: project overview

A mini search engine on 4 machines: Spark + HDFS + Iceberg build the index and PageRank, FastAPI
serves ranked results. Big Data 2026 (UE24CS343AB2), PES University.
Team: Arnav (node1), Blason (node2), Shriram (node3), Monty (node4).

This file explains **what everything is and what has happened so far**. For details:

| Need | File |
|---|---|
| Official requirements and rubric | `Distributed Search Engine.html` |
| Shared contracts (schemas, ports, export format) | `00_CONTEXT.md` |
| Per-person task lists | `ARNAV_node1.md`, `BLASON_node2.md`, `SHRIRAM_node3.md`, `MONTY_node4.md` |
| **Who runs what, in what order** | `demo/cluster_bringup.md` |
| Course verification checks, mapped to our setup, with results | `demo/verification_runbook.md` |
| Demo queries (v0 / v1) | `demo/queries.md` |

---

## 1. What the system does

1. **Input:** about 1,000 Simple English Wikipedia pages + about 500 links (`pages.jsonl`, Phase 0),
   then a change set (`changes.jsonl`, Phase 1) that adds, updates and deletes pages.
2. **Ingestion (node1):** clean the data, write it into Iceberg tables on HDFS.
3. **Compute (node3):** build an inverted index, compute PageRank, export plain Parquet.
4. **Serving (node4):** load the export into memory, rank with BM25 + PageRank, serve a web UI.
5. **Phase 0** is done before the evaluation. **Phase 1** runs live in front of the evaluator, and the
   UI switches from v0 to v1 by itself.

```
 node1 (Arnav)          node2 (Blason)              node3 (Shriram)              node4 (Monty)
 ─────────────          ──────────────              ───────────────              ─────────────
 pages.jsonl ──put──▶  HDFS NameNode + DataNode ◀─▶ HDFS DataNode
 ingest.py ──submit──────────────────────────────▶ Spark master
                       Spark worker   ◀── tasks ──▶ Spark worker
                       Iceberg tables live on HDFS ◀── index.py / pagerank.py / export.py
                       /searchengine/export/vN/ ──── WebHDFS (HTTP :9870) ───────▶ puller.py
                                                                                   FastAPI :8000 + UI
```

## 2. Machines

ZeroTier network `3b19b3a71682751e`. `/etc/hosts` is identical on all 4 machines (`conf/hosts`).

| Node | Person | IP | Daemons | Owns |
|---|---|---|---|---|
| node1 | Arnav | 10.113.232.70 | none (client) | `node1_ingest/`: validation, dedup, Iceberg MERGE, change detection, idempotency |
| node2 | Blason | 10.113.232.74 | NameNode, DataNode, Spark worker | HDFS storage, Iceberg warehouse, snapshot / time-travel demo |
| node3 | Shriram | 10.113.232.128 | Spark master, Spark worker, DataNode | `node3_compute/`, `conf/`, `tools/`, `scripts/run_phase.sh`, shared tokenizer |
| node4 | Monty | 10.113.232.224 | none (FastAPI only) | `node4_search/`: puller, BM25 + blend, hot swap, UI |

Ports: HDFS RPC 9000, NameNode UI / WebHDFS 9870, DataNode HTTP 9864, Spark master 7077,
master UI 8080, worker UI 8081, driver 7078, block manager 7079, search 8000.

## 3. Tool versions (identical on nodes 1-3)

Pinned in **`tools/versions.env`**, the only place versions are defined.

| Tool | Version |
|---|---|
| Java | OpenJDK 11 |
| Hadoop | 3.3.6 (`/opt/hadoop`) |
| Spark | 3.5.3, Scala 2.12 (`/opt/spark`) |
| Iceberg | `iceberg-spark-runtime-3.5_2.12-1.6.1.jar` in `/opt/spark/jars/` |
| Python | 3.12 (driver and executors must match, or PySpark refuses Python workers) |
| Node 4 | `tools/requirements-node4.txt` (fastapi, uvicorn, pyarrow, pandas, requests, pinned) |

- `tools/install_node.sh` installs exactly these (checksums verified; never deletes a wrong version,
  stops and tells you instead). `tools/install_node.sh node4` for Monty.
- `tools/check_versions.sh` prints OK/FAIL per tool plus a `FINGERPRINT` line; nodes 1-3 must
  print the same line. Status: **all nodes match.**

## 4. Configs (`conf/`)

| File | Key settings |
|---|---|
| `core-site.xml` | `fs.defaultFS = hdfs://node2:9000` |
| `hdfs-site.xml` | replication 2, bind 0.0.0.0, connect by hostname, **permissions off** (4 different Linux users write the same tables) |
| `spark-env.sh` | master node3:7077, `SPARK_LOCAL_IP` per node, worker cores and memory |
| `spark-defaults.conf` | master, Iceberg catalog `lake` (hadoop type) on `hdfs://node2:9000/searchengine/warehouse`, ports 7078/7079, **shuffle partitions 8** |
| `hosts` | the 4 node lines |

`conf/install_conf.sh nodeN [cores] [mem]` copies them into `/opt`, fills in the node's IP from
`/etc/hosts`, adds `spark.driver.host`, and keeps `.bak` copies of the old files.

## 5. Data on HDFS

```
/searchengine/
  raw/phase0/pages.jsonl        uploaded by node1
  raw/phase1/changes.jsonl      uploaded by node1
  warehouse/search/...          Iceberg tables (catalog lake, namespace search)
  checkpoints/pagerank/         PageRank lineage checkpoints
  export/v0/  export/v1/        Parquet for node4; _SUCCESS written last
```

## 6. Iceberg tables

| Table | Written by | What |
|---|---|---|
| `documents` | node1 | doc_id, url, title, text, version, phase, ingested_at |
| `links` | node1 | src_id → dst_id, only between existing docs |
| `rejected` | node1 | raw line + reason |
| `ingest_log` | node1 | per phase: snapshot ids, **changed_doc_ids**, accepted / rejected counts |
| `raw_links`, `tombstones` | node1 (private) | links as declared; applied deletes (so an old phase can't bring a deleted doc back) |
| `postings` | node3 | term, doc_id, tf |
| `doc_stats` | node3 | doc_id, doc_len |
| `pagerank` | node3 | doc_id, rank |

## 7. The pipeline, step by step

### 7.1 Ingestion: node1 (`node1_ingest/ingest.py`, run via `run_ingest.sh N`)
1. `hdfs dfs -put` the phase file, read it as text, parse each line with an explicit schema, so a
   bad line never kills the job.
2. **Validate**: reject with a reason (`invalid_json`, missing fields, bad url, bad version, …).
3. **Dedup** by doc_id: highest version wins; ties go to the first line in the file.
4. **Change detection**: keep only new docs or strictly higher versions. **0 changes → write
   nothing** (that's the idempotency guarantee).
5. One Iceberg `MERGE` into `documents` (insert / update / delete), then links, then append an
   `ingest_log` row.

### 7.2 Index: node3 (`node3_compute/index.py --phase N`)
- Reads the latest `ingest_log` row for the phase.
- Phase 0: tokenizes every doc. Phase N > 0: re-tokenizes **only the changed docs** and keeps every
  other doc's postings.
- Tokenizer (identical to `common/tokenizer.py`, which Node 4 uses for queries): lowercase → every
  non-`[a-z0-9]` character becomes a space → split → drop tokens shorter than 2 → drop stopwords.
  Implemented with built-in Spark SQL functions, so no Python worker processes are needed.
- Each table is replaced in **one overwrite commit**, so a reader never sees half an index, and a
  rerun produces identical tables (checked: 0 differing rows between snapshots).

### 7.3 PageRank: node3 (`node3_compute/pagerank.py --phase N`)
- Full recompute every phase (PageRank is global). Damping 0.85, start at 1/N, **20 iterations**
  (spec minimum is 10), and the change per iteration (L1 delta) is printed for the viva.
- Pages with no outgoing links ("dangling") spread their rank evenly over all N pages, so ranks
  always sum to 1.
- Ranks are checkpointed to HDFS every 5 iterations. This keeps the lineage short, and if a worker
  dies, lost partitions are recomputed from the last checkpoint (the fault-tolerance demo).

### 7.4 Export: node3 (`node3_compute/export.py --phase N`)
- Writes `documents/` (doc_id, url, title, 200-char snippet, doc_len, pagerank) and `postings/`
  (term, doc_id, tf) as Parquet, plus `manifest.json` (num_docs, num_postings, avgdl, max_pagerank,
  snapshot id, created_at).
- **Reads the files back and validates them** (no null ids, no postings pointing to missing docs,
  counts > 0). **Only then writes `_SUCCESS`.**
- Already complete → skip (idempotent). Half-written → delete and redo. `--force` rebuilds.

### 7.5 Serving: node4 (`node4_search/`)
- `puller.py` polls WebHDFS every 10 s. For a new `vN` with `_SUCCESS`: download into
  `/serving/vN.tmp/` → validate against the manifest → rename to `/serving/vN/` → load.
- **Hot swap:** the new index is fully built, then one Python reference is replaced, so queries see
  the old or the new index, never a mix. If anything fails, it keeps serving the previous version.
- Ranking: BM25 (k₁ = 1.2, b = 0.75), then
  `final = 0.7 · bm25 / max_bm25_in_results + 0.3 · pagerank / max_pagerank`.
- API: `/search?q=…&k=…`, `/health`, `/versions`, `POST /reload`; UI with a live version badge.

### 7.6 One command: `scripts/run_phase.sh N`
On node3: (ingest on node1 over ssh, or `--skip-ingest` if Arnav runs it) → index → PageRank →
export → tells node4 to reload and waits until `/health` reports vN. Prints the time for each step
and stops at the first failure (node4 then keeps serving the previous version).

## 8. What has happened so far

| When | What |
|---|---|
| 15 Nov | Repo set up. Tools pinned, configs written, tokenizer shared, node3 jobs written and tested locally on toy tables (incremental index, idempotency, export validation). |
| 15 Nov | Arnav pushed ingestion, Monty pushed the search service. All three teams' code tested together locally on Arnav's toy data: works end to end. |
| 15 Nov | Real ZeroTier IPs filled in. Node3 set up: hostname, hosts, firewall, configs, Spark master + worker, test job OK. |
| 16 Nov | **Blason away → node2's HDFS role temporarily moved to node3's machine** (`node2` in `/etc/hosts` points at 10.113.232.128 on nodes 1, 3 and 4). |
| 16 Nov | Toy pipeline on the real cluster: worked, but PageRank took 256 s. Fixed with shuffle partitions 200 → 8: **34 s**. |
| 16 Nov | Arnav's node1 → node3 test hung: his firewall blocked port 7078 (executors connect back to the driver). Fixed with `ufw allow from 10.113.232.0/24`. |
| 16 Nov | **Real Phase 0**: 1,040 lines → 20 rejected, 20 duplicates → **1,000 docs, 499 links**; 400,519 postings; PageRank 20 iterations; v0 exported; node4 serving v0. Node3 part: **72 s**. |
| 16 Nov | Monty filled `demo/queries.md` (v0). |
| 16 Nov | **Real Phase 1**: 25 inserted, 25 updated, 50 deleted → **975 docs, 467 links**; only the **100 changed docs** re-indexed; v1 exported; **node4 switched to v1 by itself**. Node3 part: **75 s**. All 4 demo queries behave as predicted. |
| 16 Nov | Idempotency: `demo/idempotency.sh 1` → **UNCHANGED**. |
| 16 Nov | Iceberg demo SQL generated with the real snapshot ids (`demo/iceberg_demo.sql`). |
| 16 Nov | Every course verification check run on the cluster (`demo/verification_runbook.md`): all pass except the 2-DataNode ones, which need node2 back. |

### Real numbers

| | Phase 0 | Phase 1 |
|---|---|---|
| Documents | 1,000 | 975 |
| Links | 499 | 467 |
| Postings | 400,519 | 377,989 |
| Distinct terms | 72,408 | 69,802 |
| Pages with no out-links | 501 | 508 |
| avgdl | 1046.3 | 1017.9 |
| max PageRank | 0.0017555 | 0.0039872 |
| Docs re-indexed | all | 100 |
| Ingest time (node1) | 129 s | about 2 min |
| Index + PageRank + export + switch (node3) | 72 s | 75 s |

## 9. Decisions and why

| Decision | Why |
|---|---|
| Tokenizer named `common/tokenizer.py`, not `tokenize.py` | `tokenize.py` shadows Python's stdlib `tokenize` module, which `inspect` / `linecache` import, and that breaks FastAPI in confusing ways |
| Tokenizer in Spark SQL functions, no Python function | runs in the JVM, so no Python workers and no driver/worker Python version issues for node3 jobs |
| One overwrite commit per table | readers never see a half-updated index; reruns give identical tables |
| `spark.sql.shuffle.partitions = 8` | the default 200 made each PageRank iteration schedule hundreds of empty tasks (256 s → 34 s) |
| PageRank checkpoint every 5 iterations, to HDFS | short lineage; survives a worker being killed (a local-only checkpoint would be lost with the worker) |
| `_SUCCESS` written last, after validating the files read back | node4 never loads a half-written or broken export |
| HDFS permissions off | each person writes to HDFS as a different Linux user |
| Firewall: allow the ZeroTier subnet instead of disabling it | Spark and HDFS have no authentication; only team machines should reach them |
| Iceberg demo SQL generated from `ingest_log` | snapshot ids change on every reset; no hand-copying |

## 10. Things that came up (and the answers)

- **Chemistry drops to #7 in v1.** Its only incoming link was from Britain (105), and Phase 1 changes
  Britain's links from `['108']` to `['158']`. All 124 links that disappeared are explained by the
  change file (deleted sources, deleted targets, rewired pages); 0 links from unchanged pages were
  lost. Chemistry moved to the Avoid list.
- **Doc 146 (Cooking) gains in-links but its PageRank falls** (0.0017555 → 0.0013917). Verified
  with a separate PageRank implementation (matches to 2e-18). Its main source, 144, loses rank when
  Phase 1 rewires the chain feeding it, and the new in-links come from pages nobody links to. See
  `demo/verification_runbook.md`, issue 1, for the evaluator answer.
- **Computer and China fall out of the top 5 in v1**: max PageRank rises 2.3×, so every page's
  PageRank share shrinks. Correct, but confusing live, so they're on the Avoid list.
- **501 of 1,000 pages have no out-links**: normal with only 499 links; their rank is spread evenly.

## 11. Status

| Rubric | Marks | Status |
|---|---|---|
| Infrastructure & Phase 0 verification | 3 | ⏳ needs node2 back (2 DataNodes, 2 workers, replication 2) |
| Live Phase 1 demo | 6 | ✅ rehearsed on real data |
| Search verification | 3 | ✅ queries chosen and confirmed (`limit` alias: Monty, open) |
| Idempotency | 2 | ✅ UNCHANGED |
| Individual viva | 6 | ⏳ each person prepares their own node |

### Still to do
1. **Move HDFS back to node2** once Blason's laptop is available:
   - nodes 1, 3 and 4: `node2` → `10.113.232.74` in `/etc/hosts`
   - node3: `hdfs --daemon stop namenode && hdfs --daemon stop datanode && rm -rf /data/hdfs/namenode`
   - node4: `rm -rf /serving/v*`
   - then `demo/cluster_bringup.md` A7/A8 (node2) → B → C (Phase 0) → D (Phase 1 rehearsal)
   - regenerate `demo/iceberg_demo.sql` (`spark-submit demo/make_iceberg_demo.py`)
2. Fault-tolerance demo (kill node2's worker mid-PageRank) and replication check (`hdfs fsck` showing
   2 replicas): both need node2's worker and DataNode.
3. Node 4: accept `limit` as an alias of `k` (Monty).
4. Night before the evaluation: section B + C of the run plan, so node4 is serving **v0**.
5. Viva prep (everyone).

## 12. Repo map

```
00_CONTEXT.md                 shared contracts
PROJECT_OVERVIEW.md           this file
Distributed Search Engine.html  official spec
*_nodeN.md                    per-person tasks
tools/                        versions.env, install_node.sh, check_versions.sh, requirements-node4.txt
conf/                         Hadoop + Spark configs, hosts, install_conf.sh
common/                       tokenizer.py, stopwords.txt (shared by node3 and node4)
node1_ingest/                 ingest.py, run_ingest.sh, reset_tables.*, table_state.py, toy_data/
node3_compute/                index.py, pagerank.py, export.py, common_spark.py
node4_search/                 app.py, puller.py, search_index.py, static/, make_fake_export.py
scripts/run_phase.sh          one phase end to end
demo/                         cluster_bringup.md (run plan), verification_runbook.md, queries.md,
                              node3_demos.md, idempotency.sh, node1_checks.sql,
                              make_iceberg_demo.py, iceberg_demo.sql
```
