# Verification runbook: course checks mapped to our setup

The course's "Project 1 Verification Runbook" (bd-proj1-verify) is written for a different setup.
This file translates every check to **our** names, paths and ports, with the result we got running
the real data end to end (Node 1 → Node 3 → Node 4, local rehearsal on 15 Nov 2026).

**Demo rule from the teachers:** arrive with **Phase 0 already executed and serving**: the search
service on node4 already running with the Phase 0 export in memory. **Only Phase 1 runs live.**

## Name mapping

| Course runbook | Ours |
|---|---|
| `vm1` … `vm4` | `node1` … `node4` |
| `--master spark://vm3:7077 --properties-file ~/spark-config/iceberg.conf` | nothing: `/opt/spark/conf/spark-defaults.conf` already sets the master and the `lake` catalog (installed by `conf/install_conf.sh`) |
| `iceberg_catalog.lake.pages` | `lake.search.documents` |
| `iceberg_catalog.lake.inverted_index` | `lake.search.postings` |
| `iceberg_catalog.lake.pagerank` | `lake.search.pagerank` |
| `pages.history` | `lake.search.documents.history` |
| HDFS `/serving/vN/` with `docs.parquet`, `index.parquet` | HDFS `/searchengine/export/vN/` with `documents/*.parquet`, `postings/*.parquet`, `manifest.json` |
| `/serving/v1/_READY` | `/searchengine/export/v1/_SUCCESS` |
| `/search?q=…&limit=5` | `/search?q=…&k=5` (see open issue 2) |

---

## 1. After Phase 0 has been executed (before the evaluator arrives)

**node2**
```bash
hdfs dfsadmin -report | grep -E "Live datanodes|^Name:"
```
Expect: `Live datanodes (2)`, node2 and node3. UI: `http://node2:9870` → Datanodes tab lists both.

**pages.jsonl on HDFS** (any node with the Hadoop client)
```bash
hdfs dfs -ls -R / | grep -i jsonl
hdfs fsck /searchengine/raw/phase0/pages.jsonl -files -blocks -locations
```
Expect: `/searchengine/raw/phase0/pages.jsonl`, size **9834481**, status **HEALTHY**, 1 block with
2 locations (node2 and node3). UI: `http://node2:9870` → Utilities → Browse the file system →
`/searchengine/raw/phase0`.

### Phase 0 results on the real data (what you must see before the demo)

| Check | Expected |
|---|---|
| `node1_ingest/run_ingest.sh 0` summary | 1040 lines read, **20 rejected** (`invalid_json`), 20 duplicates collapsed, **1000 inserted**, 1000 documents, **499 links** |
| `spark-sql -f demo/node1_checks.sql` | 1000 / 499 / 20; 0 duplicate doc_ids; 0 dangling links; 1 `ingest_log` row (phase 0, 1000 changed); 1 snapshot |
| `index.py --phase 0` | `postings=400519 doc_stats=1000 terms=72408` |
| `pagerank.py --phase 0` | `N=1000 edges=499 dangling=501`, 20 iterations, `sum=1.000000` |
| `export.py --phase 0` | manifest `num_docs 1000`, `num_postings 400519`, `avgdl 1046.259`, `max_pagerank 0.0017555…`; `_SUCCESS` written |
| `curl -s http://node4:8000/health` | `"status":"ok","version":0,"num_docs":1000` |
| `demo/idempotency.sh 0` (node1) | `RESULT: UNCHANGED` |

### Baseline (write these down; compared in section 3)

**node4**
```bash
curl -s "http://localhost:8000/search?q=beard&k=5" | python3 -m json.tool
```
Expect (v0): 1 Mustache (500), **2 Beard (94), title plain "Beard"**, 3 Goatee (318), 4 Hair (359), 5 Russia (2964).

**node1 or node3**
```bash
spark-sql -e "SELECT doc_id, rank FROM lake.search.pagerank WHERE doc_id = '146';" 2>/dev/null
```
Expect: `146  0.0017555209979530687` (in v0 this equals the corpus max PageRank).
`curl -s "http://localhost:8000/search?q=cooking&k=5"` → Cooking (146) is #1 with score 1.0.

---

## 2. Live Phase 1 (one job at a time, check after each)

On node3: `./scripts/run_phase.sh 1` (or node1 runs `node1_ingest/run_ingest.sh 1`, then node3
`./scripts/run_phase.sh 1 --skip-ingest`). To pause between jobs as the course asks, run the
steps by hand:
```bash
# node1
node1_ingest/run_ingest.sh 1
# node3
cd node3_compute
spark-submit index.py --phase 1
spark-submit pagerank.py --phase 1
spark-submit export.py --phase 1
```

### 2.1 Ingest (node1)
Summary: **25 inserted, 25 updated, 50 deleted**, 975 documents, 467 links.
```bash
spark-sql -e "SELECT snapshot_id, made_current_at FROM lake.search.documents.history;" 2>/dev/null
```
Expect: **2 snapshots** (Phase 0 and Phase 1).

### 2.2 Index (node3)
`phase 1: changed docs=100 postings=377989 doc_stats=975 terms=69802`
```bash
spark-sql -e "SELECT term, doc_id FROM lake.search.postings WHERE doc_id = 'phase1-new-001' LIMIT 5;" 2>/dev/null
```
Expect rows, e.g. `document`, `added`, `project`, `phase` for `phase1-new-001`.

### 2.3 PageRank (node3)
`N=975 edges=467 dangling=508`, `sum=1.000000`
```bash
spark-sql -e "SELECT COUNT(*) AS deleted_doc_ranks FROM lake.search.pagerank WHERE doc_id = '1';" 2>/dev/null
```
Expect **0** (doc 1 "April" was deleted in Phase 1).

### 2.4 Export (node3)
```bash
hdfs dfs -ls -R /searchengine/export/v0/ /searchengine/export/v1/
```
Expect in both: `documents/part-*.parquet`, `postings/part-*.parquet`, `manifest.json`, `_SUCCESS`.

---

## 3. Search verification

```bash
curl -s http://localhost:8000/health                                              # node4
hdfs dfs -test -e /searchengine/export/v1/_SUCCESS && echo "v1 ready"            # node3
```
Expect `{"status":"ok","version":1,"num_docs":975,...}` and `v1 ready`.

### 3.1 Doc 94 (Beard) updated in place
```bash
curl -s "http://localhost:8000/search?q=beard&k=5" | python3 -m json.tool        # node4
spark-sql -e "SELECT doc_id, title FROM lake.search.documents WHERE doc_id = '94';" 2>/dev/null   # node1/node3
```
Expect: `94  Beard (Phase 1)`. Search (v1): 1 Mustache, 2 Goatee, 3 Hair, **4 Beard (Phase 1)**, 5 Russia.

### 3.2 Doc 146 (Cooking): new in-links
```bash
curl -s "http://localhost:8000/search?q=cooking&k=5" | python3 -m json.tool      # node4
spark-sql -e "SELECT doc_id, rank FROM lake.search.pagerank WHERE doc_id = '146';
              SELECT src_id, dst_id FROM lake.search.links WHERE dst_id = '146';" 2>/dev/null
```
Our result: in-links go from **1 (144)** to **3 (144, 98, phase1-new-011)**, as the course page
says; Cooking stays **#1** for "cooking". **But its PageRank goes down: 0.0017555 → 0.0013917.**
See open issue 1; prepare to explain it.

### 3.3 Multi-word query
```bash
curl -s "http://localhost:8000/search?q=plant+growth&k=5"
```
Expect HTTP 200 with valid JSON (top result: 628 "Plant").

---

## 4. Idempotency (node1)

```bash
spark-sql -e "SELECT COUNT(*) FROM lake.search.documents;" 2>/dev/null   # 975
node1_ingest/run_ingest.sh 1                                              # "0 changes -> ... NOT touched"
spark-sql -e "SELECT COUNT(*) FROM lake.search.documents;" 2>/dev/null   # 975 again
```
Stronger version (row counts, content hashes and snapshot ids of all Node 1 tables):
`demo/idempotency.sh 1` → `RESULT: UNCHANGED`.

---

## Open issues for the team

1. **Check 3.2 (Shriram, everyone):** the course expects doc 146's PageRank to rise after Phase 1.
   On the real data it falls (0.0017555 → 0.0013917). Our PageRank is correct: an independent
   Python implementation gives the same numbers. Every standard variant (10/20/100 iterations,
   with or without dangling redistribution, normalised or not) also falls. Reason: Phase 0's links
   form long chains (… → 142 → 144 → 146), and rank builds up along a chain. Phase 1 deletes and
   rewires pages upstream, so 144 drops from 0.0017555 to 0.0007522. The two new in-links (98,
   phase1-new-011) come from pages with no in-links (rank ≈ 0.00028), which can't make up for
   it. 146 only rises (to 0.0024873) if the Phase 1 deletes are ignored, which contradicts check
   2.3. **Show the in-link growth and explain the drop with these numbers.**
2. **`limit` parameter (Monty):** the course's commands use `?limit=5`; our API takes `k`, and
   FastAPI silently ignores `limit` (returns 10 results). Suggest `/search` accept `limit` as an
   alias of `k`, so the evaluator's copy-pasted commands behave as they expect.
3. **Export layout (Shriram, optional):** the course expects `docs.parquet` / `index.parquet` and
   `_READY` under `/serving/vN/` on HDFS. Ours is `/searchengine/export/vN/documents/`,
   `postings/`, `_SUCCESS` (CONTEXT §4.7). The page allows setup differences; just show ours.

---

## Verified on the cluster (16 Nov 2026, Shriram)

Every check above was run against the real cluster (Spark master node3, HDFS, Node 4 live on v1),
after the real Phase 0 + Phase 1 runs. **Temporary setup:** Blason was away, so node2's role
(NameNode + DataNode) ran on node3's machine. That's why there is 1 DataNode and 1 replica below;
redo section 1 once node2 is back.

| Check | Result | |
|---|---|---|
| 1. `dfsadmin -report` | `Live datanodes (1)` | ⚠️ needs node2 back: expect 2 |
| 1. `pages.jsonl` on HDFS | `/searchengine/raw/phase0/pages.jsonl`, 9834481 bytes, `HEALTHY` | ⚠️ `Live_repl=1` (target 2) until node2 is back |
| 1. baseline beard (v0) | Beard (94) #2, plain title | ✅ |
| 1. baseline rank 146 (v0) | 0.0017555 | ✅ |
| 2.1 documents history | 2 snapshots: 6998397766212633409 (P0), 1807384531269279055 (P1) | ✅ |
| 2.2 postings for `phase1-new-001` | document, added, phase, project | ✅ |
| 2.3 rank rows for deleted doc 1 | 0 | ✅ |
| 2.4 export v0 + v1 | `documents/*.parquet`, `postings/*.parquet`, `manifest.json`, `_SUCCESS` in both | ✅ |
| 3. `/health` | `{"status":"ok","version":1,"num_docs":975,...}`, `v1 ready` | ✅ |
| 3.1 doc 94 | `94  Beard (Phase 1)`; search: Mustache, Goatee, Hair, **Beard (Phase 1)**, Russia | ✅ |
| 3.2 doc 146 | in-links 1 → 3 (144, 98, phase1-new-011); Cooking still #1; rank 0.0017555 → 0.0013917 | ✅ explained, see issue 1 |
| 3.3 `plant growth` | HTTP 200, top 628 Plant | ✅ |
| 4. idempotency | count 975 before and after; `demo/idempotency.sh 1` → `UNCHANGED` | ✅ |

### Issue 1: independently confirmed (Shriram)

A separate pure-Python PageRank over the same Iceberg snapshots matches `pagerank.py` to
**2e-18 on every doc**. Doc 146: v0 0.0017555 → v1 0.0013917 (100 iterations: same; without
dangling redistribution: 0.0010000 → 0.0007685, also a drop). Doc 144, 146's main source, drops
0.0017555 → 0.0007522 even though it gains an in-link (from 94), because the chain of pages feeding
it is rewired upstream. The new in-links from 98 and phase1-new-011 come from pages nobody links
to, which only have the base rank of about 0.00028. **Evaluator answer:** "146 gains 2 in-links
(show the links query), but PageRank depends on how important the linking pages are, not how many
there are. Its main source 144 lost rank when Phase 1 rewired the chain feeding it, so 146's total
falls. Our numbers match an independent implementation exactly."

### Issue 2: fixed (Monty, 443de01)

`/search` now accepts `limit` as an alias of `k`: `/search?q=beard&limit=3` returns 3 results
(Mustache, Goatee, Hair). The course's copy-pasted commands work unchanged.
