# BD-sparksearch: the complete guide

Everything about this project in one place: what we built, **every concept it uses explained from
the ground up**, the full process step by step, what each person does, what has happened, and a
glossary. Big Data 2026 (UE24CS343AB2), PES University.
Team: Arnav (node1), Blason (node2), Shriram (node3), Monty (node4).

Other documents go deeper on one thing each:

| Need | File |
|---|---|
| Official requirements and rubric | `Distributed Search Engine.html` |
| Exact contracts (schemas, ports, export format) | `00_CONTEXT.md` |
| Who runs what, in what order | `demo/cluster_bringup.md` |
| Course verification checks + results | `demo/verification_runbook.md` |
| Demo queries | `demo/queries.md` |
| Per-node guides + viva notes | `ARNAV_node1.md`, `BLASON_node2.md`, `SHRIRAM_node3.md`, `MONTY_node4.md` |

## Contents
1. [The big picture](#1-the-big-picture)
2. [The machines and the network](#2-the-machines-and-the-network)
3. [Concepts: storage (HDFS)](#3-concepts-storage-hdfs)
4. [Concepts: compute (Spark)](#4-concepts-compute-spark)
5. [Concepts: tables (Iceberg and Parquet)](#5-concepts-tables-iceberg-and-parquet)
6. [Concepts: search (tokenizing, inverted index, BM25)](#6-concepts-search-tokenizing-inverted-index-bm25)
7. [Concepts: PageRank](#7-concepts-pagerank)
8. [Concepts: correctness (idempotency, atomicity, versioning)](#8-concepts-correctness-idempotency-atomicity-versioning)
9. [The whole process, step by step](#9-the-whole-process-step-by-step)
10. [Who does what](#10-who-does-what)
11. [Tools, versions and configs](#11-tools-versions-and-configs)
12. [What has happened so far](#12-what-has-happened-so-far)
13. [Decisions and why](#13-decisions-and-why)
14. [Things that came up, and the answers](#14-things-that-came-up-and-the-answers)
15. [Evaluation and status](#15-evaluation-and-status)
16. [Glossary](#16-glossary)
17. [Repo map](#17-repo-map)

---

## 1. The big picture

We built a **mini Google**. You type words; it returns the most relevant Wikipedia pages, ranked.

Behind that are two problems every search engine solves:

1. **Which pages contain my words, and how well do they match?** We answer this with an
   **inverted index** (a lookup table from word → pages) and a scoring formula called **BM25**.
2. **Which of those pages are important?** We answer this with **PageRank**, which scores a page by
   how many pages link to it, and how important *those* pages are.

The final score blends the two: **70 % text match + 30 % importance**.

The heavy work (cleaning data, building the index, computing PageRank) is done by **Apache Spark**
spread over several machines, reading and writing files on **HDFS** (a file system spread over
several machines), organised into versioned tables with **Apache Iceberg**. The result is exported
as plain **Parquet** files, which a small **FastAPI** web service loads into memory to answer
searches fast.

The data comes in **two phases**:
- **Phase 0**, `pages.jsonl`: the base corpus, about 1,000 Simple English Wikipedia pages and about 500 links.
- **Phase 1**, `changes.jsonl`: a change set (new pages, edited pages, deleted pages).

Phase 0 is already done and being served when the evaluator arrives. Phase 1 is run **live**, and
the search UI switches from **version v0** to **v1** without going down.

```
 pages.jsonl / changes.jsonl                                      (node1: Arnav)
        │  upload to HDFS, validate, remove duplicates, MERGE into Iceberg
        ▼
 Iceberg tables stored on HDFS                       (node2: Blason + node3: Shriram)
        │  inverted index → PageRank → Parquet export, _SUCCESS written last   (node3)
        ▼
 /searchengine/export/vN/  on HDFS
        │  pulled over HTTP (WebHDFS), validated, swapped in               (node4: Monty)
        ▼
 http://node4:8000   score = 0.7 × BM25 (normalised) + 0.3 × PageRank (normalised)
```

---

## 2. The machines and the network

| Node | Person | ZeroTier IP | Runs (daemons) | Owns |
|---|---|---|---|---|
| node1 | Arnav | 10.113.232.70 | nothing (client) | ingestion job, data quality, idempotency |
| node2 | Blason | 10.113.232.74 | HDFS NameNode + DataNode, Spark worker | storage, Iceberg warehouse, snapshot demo |
| node3 | Shriram | 10.113.232.128 | Spark master + worker, HDFS DataNode | index, PageRank, export, configs, runner |
| node4 | Monty | 10.113.232.224 | nothing Spark/HDFS (just a web server) | search API, ranking, UI |

**ZeroTier.** Our four laptops are on different home and college networks. ZeroTier creates a
private virtual network on top of the internet, so each laptop gets a fixed private IP
(`10.113.232.x`) and they can reach each other as if they were on the same LAN. Network ID:
`3b19b3a71682751e`.

**Hostnames and `/etc/hosts`.** Every config refers to machines by name (`node2`, `node3`), never
by IP. `/etc/hosts` on each machine maps names to IPs, and it is identical on all four
(`conf/hosts`). Two Ubuntu details matter:
- Ubuntu adds a line `127.0.1.1 <hostname>`. If it stays, a daemon looking up its own name binds to
  **loopback** (only reachable from the same machine), and no other node can connect. We comment it out.
- Each machine's hostname is set to its node name, because HDFS DataNodes register with the NameNode
  **by hostname**, and other machines must be able to resolve that name.

**Daemon.** A long-running background service, for example the NameNode or a Spark worker.
Node1 and node4 run no Spark or HDFS daemons. Node1 only *submits* jobs, and node4 only *reads*
results.

**Firewall.** Ubuntu's firewall (`ufw`) blocks incoming connections by default. Spark and HDFS have
no passwords, so rather than turning the firewall off, each machine allows everything from the
ZeroTier subnet only: `sudo ufw allow from 10.113.232.0/24`.

**Ports** (each service listens on a fixed port, so firewalls and configs are predictable):

| Service | Where | Port |
|---|---|---|
| HDFS NameNode RPC (clients talk to HDFS here) | node2 | 9000 |
| HDFS NameNode web UI + WebHDFS (HTTP API) | node2 | 9870 |
| HDFS DataNode HTTP (file downloads) | node2, node3 | 9864 |
| Spark master (jobs are submitted here) | node3 | 7077 |
| Spark master web UI | node3 | 8080 |
| Spark worker web UI | node2, node3 | 8081 |
| Spark driver / block manager | whichever machine submits | 7078 / 7079 |
| Search API + UI | node4 | 8000 |

---

## 3. Concepts: storage (HDFS)

**HDFS (Hadoop Distributed File System)** stores files across several machines, so they survive a
machine failing and can be read in parallel.

- **Blocks.** A file is cut into blocks (128 MB each by default). Our `pages.jsonl` is 9.8 MB, so it
  is a single block.
- **NameNode** (node2). The "table of contents". It holds the directory tree, which blocks make up each
  file, and which machines hold each block. It stores **no file data**, only metadata. If it's down,
  HDFS is down.
- **DataNode** (node2 and node3). Stores the actual blocks on local disk (`/data/hdfs/datanode`).
  Sends a **heartbeat** to the NameNode every few seconds; a DataNode that stops sending them is
  marked dead.
- **Replication factor 2.** Every block is stored on **two different DataNodes** (node2 and
  node3). If one machine dies, the other still has every block. `hdfs fsck … -locations` shows
  where each copy lives. This is checked in the evaluation.
- **Writing a file.** The client asks the NameNode where to write, then streams the data to the
  first DataNode, which forwards it to the second (a **write pipeline**).
- **Formatting.** `hdfs namenode -format -clusterId bd-sparksearch` creates an empty file system. It is run **once ever**:
  running it again wipes everything.
- **`fs.defaultFS = hdfs://node2:9000`** tells every client where the NameNode is.
- **WebHDFS.** An HTTP API to HDFS on port 9870. Node4 uses it to download files without
  installing Hadoop: `GET …/webhdfs/v1/<path>?op=LISTSTATUS` lists a folder; `op=OPEN` reads a file.
  The NameNode answers with a **redirect** to a DataNode (port 9864) that holds the data, which is
  why node4 also needs `/etc/hosts` entries for node2 and node3.
- **Permissions.** We switched HDFS permission checks off (`dfs.permissions.enabled=false`), because
  each of us writes as a different Linux user into the same Iceberg folders.

Commands you'll see: `hdfs dfs -ls / -put / -cat / -rm` (file operations),
`hdfs dfsadmin -report` (live DataNodes, capacity), `hdfs fsck <path> -files -blocks -locations`
(block health and replicas).

---

## 4. Concepts: compute (Spark)

**Apache Spark** runs a computation in parallel across many machines. We use its **standalone**
cluster mode, where Spark manages its own machines (no YARN or Kubernetes).

### The pieces
- **Master** (node3:7077). The resource manager. It knows which workers exist and how many CPU cores
  and how much memory each offers. It hands those resources to applications. It does **not** run
  your code.
- **Worker** (node2, node3). A daemon on each machine that offers its cores and memory to the master
  (we use 2 cores, 3 GB each) and starts executors on request.
- **Driver.** Your program's main process, started by `spark-submit` **on the machine you run it
  from** (client mode). For ingestion that is node1; for index/PageRank/export, node3. The driver
  plans the work and hands out tasks.
- **Executor.** A worker process, launched for your application, that runs tasks and holds cached
  data. **Executors connect back to the driver** (ports 7078/7079). That's why every driver
  machine must advertise its reachable IP (`spark.driver.host`) and allow incoming connections.

### How work is split
- **Partition.** A slice of the data. Spark processes partitions in parallel, one **task** per
  partition.
- **Transformations vs actions.** `filter`, `join`, `groupBy` are *transformations*: they only
  describe work. Nothing runs until an *action* (`count`, `write`, `collect`). This is **lazy
  evaluation**, and it lets Spark plan the whole computation at once.
- **DAG.** The plan is a directed acyclic graph of steps. Spark's optimiser (Catalyst) rewrites it,
  for example pushing filters down and choosing join strategies.
- **Narrow vs wide.** A *narrow* step (`filter`, `select`) works on each partition alone. A *wide*
  step (`groupBy`, `join`) needs rows with the same key together, so data is redistributed across the
  network: a **shuffle**.
- **Job → stages → tasks.** Each action is a *job*. Spark cuts it into *stages* at every shuffle,
  and each stage runs one *task* per partition.
- **Shuffle partitions.** How many partitions a shuffle produces (`spark.sql.shuffle.partitions`).
  The default is 200, sensible for big data and terrible for 1,000 docs: every join scheduled 200
  near-empty tasks. We set **8**, and PageRank went from 256 s to 34 s.

### Fault tolerance
- **Lineage.** Spark remembers *how* each partition was computed (its chain of transformations). If an
  executor dies, Spark recomputes only the lost partitions from their inputs. No data is copied in
  advance.
- **Caching** keeps a result in executor memory for reuse (lost if the executor dies, then recomputed).
- **Checkpointing** writes a result to reliable storage (HDFS) and **cuts the lineage**. Iterative
  algorithms like PageRank grow a longer lineage every iteration; we checkpoint every 5 iterations so
  recovery stays cheap. A *local* checkpoint (`localCheckpoint`) is faster but lives on the executor
  and dies with it, so PageRank uses the reliable HDFS one.
- **Demo:** kill node2's worker mid-PageRank. The master marks it dead, its tasks are re-run on
  node3, and the job finishes.

### PySpark and Python versions
Our jobs are Python (PySpark), but almost all the work runs inside the JVM as Spark SQL. Python
*worker* processes only start for Python functions (UDFs). When they do, driver and workers must
run the **same Python minor version**, which is why Python 3.12 is pinned on nodes 1-3. Node3's
tokenizer uses built-in SQL functions instead of a Python UDF, so node3's jobs never need Python
workers at all.

---

## 5. Concepts: tables (Iceberg and Parquet)

### Parquet
A **columnar** file format: values of each column are stored together, compressed (Snappy) and
typed, with the schema inside the file. Reading two columns of a wide table only reads those two.
All our table data and our exports are Parquet files.

### Why not just Parquet files?
A folder of Parquet files has no transactions: if a job crashes halfway, readers see half the new
files. There's no "update this row", and no history. **Apache Iceberg** adds all of that on top of
plain files.

### How Iceberg works
- A table is **data files** (Parquet) plus **metadata files** (JSON + Avro) describing which data
  files make up the table right now.
- **Snapshot.** Every commit (insert, update, delete, overwrite) creates a new snapshot: a complete
  list of the table's data files at that moment, with an ID and a timestamp. Old snapshots stay.
- **Atomic commit.** A write adds new data files, then swaps one pointer to the new metadata file.
  Readers see the old snapshot or the new one, **never half**. Two writers can't both win: the second
  commit fails and retries (optimistic concurrency).
- **Catalog.** Where Iceberg finds the current metadata of each table. We use a **Hadoop catalog**
  named `lake`, which is just a folder on HDFS (`/searchengine/warehouse`, with `version-hint.text`
  pointing at the latest metadata). Tables are `lake.search.<name>`.
- **MERGE INTO.** One SQL statement that inserts, updates and deletes rows by matching a source
  against the table, committed as **one snapshot**. Iceberg's default here is **copy-on-write**:
  data files containing changed rows are rewritten.
- **Time travel.** `SELECT … FROM t VERSION AS OF <snapshot_id>` reads the table as it was.
- **Rollback.** `CALL lake.system.rollback_to_snapshot('search.documents', <id>)` makes an old
  snapshot current again; `set_current_snapshot` moves forward again.
- **Metadata tables.** `t.snapshots`, `t.history`, `t.files` let you inspect all of this with SQL.

### Our tables (catalog `lake`, namespace `search`)

| Table | Written by | Columns / purpose |
|---|---|---|
| `documents` | node1 | doc_id, url, title, text, version, phase, ingested_at |
| `links` | node1 | src_id → dst_id, only between documents that exist |
| `rejected` | node1 | the raw bad line + a reason |
| `ingest_log` | node1 | one row per phase that changed something: snapshot ids, **changed_doc_ids**, counts |
| `raw_links` | node1 (private) | every link each page *declares*, even to pages that don't exist yet |
| `tombstones` | node1 (private) | applied deletes, so re-ingesting an older phase can't resurrect a page |
| `postings` | node3 | term, doc_id, tf: the inverted index |
| `doc_stats` | node3 | doc_id, doc_len (number of tokens) |
| `pagerank` | node3 | doc_id, rank |

---

## 6. Concepts: search (tokenizing, inverted index, BM25)

### Tokenizing
Turning text into searchable words (**tokens**). Our rules, shared by node3 (indexing pages) and
node4 (reading your query), so both sides see words the same way:
1. lowercase everything
2. every character that is not `a-z` or `0-9` becomes a space
3. split on whitespace
4. drop tokens shorter than 2 characters
5. drop **stopwords** (very common words like *the, and, of* that carry no meaning;
   `common/stopwords.txt`)
6. no **stemming** (*run* and *running* stay different words)

Example: `"The Quick-brown fox's 2nd jump!"` → `quick, brown, fox, 2nd, jump`.

### Inverted index
A normal ("forward") index maps page → words. An **inverted index** maps **word → pages**, which is
what search needs. Each entry is a **posting**: `(term, doc_id, tf)`, where **tf** (term frequency)
= how often the term appears in that page. We also store **doc_len**, the page's token count.

```
"plague" → (100, tf 14), (460, tf 2), (371, tf 1), …
"river"  → (673, tf 31), (702, tf 12), (799, tf 9), …
```
Real size: Phase 0 has 400,519 postings over 72,408 distinct terms.

**Incremental indexing.** In Phase 1 only 100 pages changed, so only those are re-tokenized; every
other page's postings are kept as they are.

### BM25
The standard text-relevance score (what Elasticsearch/Lucene use by default). For a query Q and a
document D, for each query term t:

```
BM25(D, Q) = Σ  IDF(t) · tf · (k₁ + 1) / ( tf + k₁ · (1 − b + b · len(D) / avgdl) )
IDF(t)     = ln( (N − df + 0.5) / (df + 0.5) + 1 )
```

| Symbol | Meaning |
|---|---|
| tf | how often t appears in D |
| df | how many documents contain t |
| N | number of documents (1,000 in v0) |
| len(D), avgdl | D's length, and the average length (1046 tokens in v0) |
| k₁ = 1.2 | **term-frequency saturation**: the 10th occurrence adds far less than the 2nd; the score can never exceed (k₁+1) × IDF per term |
| b = 0.75 | **length normalisation**: long pages are penalised so they don't win just by containing more words |
| IDF | **inverse document frequency**: rare words count more. A word in almost every page tells you little; the `+1` keeps IDF positive even for very common words |

Example, ignoring length: a word in 10 of 1,000 docs has IDF ≈ ln(990.5/10.5 + 1) ≈ 4.56; a word in
500 docs has IDF ≈ ln(500.5/500.5 + 1) ≈ 0.69. Matching the rare word is worth about 6× more.

### Blending with PageRank
```
final = 0.7 × BM25 / max(BM25 among this query's results) + 0.3 × PageRank / max(PageRank in corpus)
```
Both parts are scaled to 0…1 first (**normalisation**), otherwise the weights would be meaningless:
BM25 values are around 5-20, PageRank values around 0.001. BM25 is normalised **per query** (its scale
depends on the query); PageRank **per corpus**.
A consequence: if the corpus's top PageRank goes up (it did in Phase 1, 0.00176 → 0.00399), every
other page's PageRank share shrinks, and some results reorder (section 14).

---

## 7. Concepts: PageRank

**Idea (the random surfer).** Imagine someone clicking links at random. With probability **d =
0.85** they follow a random link on the current page; with probability 0.15 they get bored and jump
to a random page. A page's PageRank is the long-run fraction of time the surfer spends on it.
Pages linked from many important pages score high.

**Formula** (N pages; out(q) = number of links on page q):
```
PR(p) = (1 − d)/N  +  d · ( Σ over pages q linking to p:  PR(q) / out(q)   +   dangling_mass / N )
```
- **Damping factor d = 0.85.** The 0.15 "teleport" guarantees the computation settles to one answer.
- **Dangling pages** have no outgoing links (501 of our 1,000). Their rank would leak out of the
  system, so their total ("dangling mass") is spread evenly over all N pages. Result: the ranks always
  **sum to exactly 1** (we check this every run).
- **Iterative computation.** Start every page at 1/N, apply the formula to all pages, repeat.
  Each round is a few Spark joins and aggregations. The spec requires at least 10 iterations; we
  do **20**.
- **Convergence.** We print the **L1 delta**, the total change in all ranks between rounds. On real
  data it falls from 0.43 to 0.00015 over 20 rounds, so the ranks have settled.
- **Full recompute each phase.** One changed link can shift ranks everywhere, so PageRank is
  recomputed from scratch in Phase 1 (unlike the index, which is incremental).
- **Verified:** a separate pure-Python implementation over the same data matches ours to 2·10⁻¹⁸ on
  every page.

---

## 8. Concepts: correctness (idempotency, atomicity, versioning)

**Idempotency.** Running the same step twice gives the same result as running it once. It's
graded (2 marks), and it makes reruns after a failure safe. How each layer achieves it:

| Step | How |
|---|---|
| Ingest | dedup is deterministic; only *strictly newer* versions count as changes; **0 changes → no writes at all** (no new snapshot, no `ingest_log` row) |
| Index | rebuilds from the latest `ingest_log` row, one overwrite commit; same input → identical tables |
| PageRank | full recompute from the same tables, one overwrite commit |
| Export | `vN/_SUCCESS` exists → skip; half-written `vN/` → delete and redo |
| Serving | an already-served version is not pulled again |

Demo: `demo/idempotency.sh 1` takes row counts, content hashes and snapshot IDs, re-ingests Phase 1,
compares, and prints **`RESULT: UNCHANGED`**.

**Atomicity: all or nothing, never half.** Used at every hand-off:
- Iceberg commits: one pointer swap.
- **`_SUCCESS` marker:** node3 writes the export, reads it back, validates it, and only then writes
  an empty `_SUCCESS` file. No `_SUCCESS` means "not finished", and node4 ignores that version.
- **Atomic rename:** node4 downloads into `vN.tmp/`, validates it, then renames it to `vN/`. A
  rename on one file system is instant and indivisible.
- **Hot swap:** node4 builds the complete new in-memory index, then replaces one Python reference.
  Each search request reads that reference once, so it uses the old index or the new one, never a mix.

**Versioning.** Data versions exist at three levels:
- Iceberg snapshots: every table change, with time travel and rollback.
- Export versions `v0`, `v1` on HDFS: one per phase.
- Serving versions `/serving/v0`, `/serving/v1` on node4, with the live one shown as a badge in the UI.

If anything in Phase 1 fails, **node4 keeps serving v0**, which the spec requires.

---

## 9. The whole process, step by step

### 9.1 Setting up (once)
1. Everyone: same ZeroTier network, hostname `nodeN`, `/etc/hosts` from `conf/hosts`, firewall rule.
2. Nodes 1-3: `tools/install_node.sh` installs exactly Java 11, Hadoop 3.3.6, Spark 3.5.3 and
   Iceberg 1.6.1. `tools/check_versions.sh` must print the same `FINGERPRINT` line on all three.
3. Nodes 1-3: `conf/install_conf.sh nodeN` installs the Hadoop and Spark configs.
4. node2: `hdfs namenode -format -clusterId bd-sparksearch`, once ever.

### 9.2 Starting the cluster (every session, this order)
1. node2: NameNode, then DataNode.
2. node3: Spark master, Spark worker, DataNode.
3. node2: Spark worker.
4. Check: 2 live DataNodes, 2 Spark workers.
5. node4: start the search service.

### 9.3 One phase, end to end
**Step 1. Ingest (node1, Arnav):** `node1_ingest/run_ingest.sh N`
1. Upload the phase file to `/searchengine/raw/phaseN/` on HDFS.
2. Read every line as text, then parse it as JSON against an explicit schema. A broken line becomes
   nulls instead of crashing the job, and the raw line is kept.
3. **Validate** each record and reject bad ones with a reason: `invalid_json`, missing doc_id / url /
   title / text, bad url, bad version, links not a list. Real Phase 0: 20 lines rejected as
   `invalid_json`.
4. **Deduplicate** by doc_id: highest version wins; on a tie, the first line in the file wins (a
   window function, `row_number()` over doc_id). Real Phase 0: 20 duplicates collapsed. A record
   without a version gets the phase number as its version, so Phase 1 always beats Phase 0.
5. **Change detection:** compare against what's already stored (and the tombstones). Keep only new
   pages, strictly newer versions, and valid deletes. **Nothing changed → stop, write nothing.**
6. **MERGE** the changes into `documents` in one commit (insert / update / delete).
7. **Links:** replace the out-links of changed pages, then keep only links whose source *and* target
   exist (via `raw_links`, so a link to a page that only appears later is restored then).
8. Append a row to **`ingest_log`**: snapshot IDs, the list of changed doc_ids, counts.

**Step 2. Index (node3, Shriram):** `index.py --phase N`
Read the phase's `ingest_log` row. Phase 0: tokenize every page. Phase 1: tokenize only the changed
pages, drop their old postings, keep everyone else's. Write `postings` and `doc_stats`, one commit each.

**Step 3. PageRank (node3):** `pagerank.py --phase N`
Read `documents` + `links`, run 20 iterations (checkpoint every 5), print the L1 delta, check the
sum is 1, write `pagerank`.

**Step 4. Export (node3):** `export.py --phase N`
Join documents + doc_stats + pagerank into `documents/` Parquet (id, url, title, 200-char snippet,
length, PageRank) and copy postings into `postings/` Parquet, under `/searchengine/export/vN/`. Read
them back and validate (no null IDs, no postings pointing at missing pages, non-empty). Write
`manifest.json` (counts, avgdl, max PageRank, snapshot ID, time). Write **`_SUCCESS` last**.

**Step 5. Serve (node4, Monty):** automatic
The puller checks WebHDFS every 10 s. It sees `vN/_SUCCESS`, downloads into `/serving/vN.tmp/`,
validates against the manifest, renames to `/serving/vN/`, builds the new in-memory index, and hot
swaps it in. The UI badge changes to vN.

**Steps 2-5 in one command (node3):** `./scripts/run_phase.sh N --skip-ingest` (or without
`--skip-ingest` to also run step 1 on node1 over ssh). It prints the time for each step, stops at the
first failure, and at the end calls node4's `/reload` and waits until `/health` reports vN.

### 9.4 Answering a search (node4)
1. Tokenize the query with the shared tokenizer, removing duplicate words.
2. For each query term, look up its postings: those are the candidate pages.
3. Compute BM25 for every candidate.
4. Blend: `0.7 · bm25/max_bm25 + 0.3 · pagerank/max_pagerank`.
5. Return the top k (`?k=` or `?limit=`), with title, URL, snippet and all three scores.
   About 60 ms on the real corpus.

### 9.5 Evaluation day
Phase 0 is already done the night before, and node4 is serving v0. In front of the evaluator:
infrastructure checks → Phase 1 live (Arnav ingests, Shriram runs the rest, the UI flips to v1) →
search queries → idempotency → Iceberg snapshots/time travel → fault tolerance if asked. Exact steps:
`demo/cluster_bringup.md` section E.

---

## 10. Who does what

### Arnav: node1, ingestion
- **Builds:** `node1_ingest/` (ingest job, run script, table reset, state fingerprint, toy data with
  every kind of bad record), `demo/idempotency.sh`, `demo/node1_checks.sql`,
  `demo/verification_runbook.md`.
- **Runs:** `run_ingest.sh 0` (night before), `run_ingest.sh 1` (live), `idempotency.sh 1` (live).
- **Rubric:** idempotency (2 M), step 1 of the live demo.
- **Viva:** validation rules, dedup with a window function, change detection, MERGE semantics,
  snapshots, why ingestion is idempotent, why the driver needs `spark.driver.host`.

### Blason: node2, storage
- **Runs:** NameNode + DataNode (HDFS), a Spark worker. Formats HDFS once. Stops their worker during the
  fault-tolerance demo.
- **Shows:** `jps`, `hdfs dfsadmin -report` (2 DataNodes), the NameNode UI, and the Iceberg demo
  (`demo/iceberg_demo.sql`: snapshots, time travel, what changed, rollback if asked).
- **Rubric:** infrastructure (3 M, with Shriram), Iceberg demo.
- **Viva:** NameNode vs DataNode, blocks, replication, what a Spark worker is, what Iceberg snapshots,
  time travel and rollback are, why Iceberg instead of plain Parquet.

### Shriram: node3, compute + integration
- **Builds:** `node3_compute/` (index, PageRank, export), `scripts/run_phase.sh`, `conf/` (all
  Hadoop/Spark configs), `tools/` (version pinning), `common/` (shared tokenizer),
  `demo/make_iceberg_demo.py`, run plan and docs.
- **Runs:** Spark master + worker + DataNode; `run_phase.sh 0` (night before), `run_phase.sh 1` (live);
  the fault-tolerance and replication demos.
- **Rubric:** live Phase 1 demo (6 M, drives it), infrastructure (3 M, with Blason).
- **Viva:** master/worker/driver/executor, stages and shuffles, lineage and checkpointing,
  PageRank, inverted index, incremental updates via `ingest_log`, export atomicity.

### Monty: node4, search
- **Builds:** `node4_search/` (puller, index + scoring, FastAPI app, UI with live badge and
  side-by-side compare, fake export generator), `demo/queries.md`.
- **Runs:** the search service; shows the v0 → v1 switch and the prepared queries.
- **Rubric:** search verification (3 M), the visible "after" of the live demo.
- **Viva:** BM25 (k₁, b, IDF), normalisation and blending, hot swap and why it's safe, atomic
  publish, what happens when an export fails.

---

## 11. Tools, versions and configs

**Versions** (identical on nodes 1-3, defined only in `tools/versions.env`):

| Tool | Version | Why it must match |
|---|---|---|
| Java | OpenJDK 11 | Hadoop 3.3 and Spark 3.5 both run on it |
| Hadoop | 3.3.6 | HDFS client and server must speak the same protocol |
| Spark | 3.5.3, Scala 2.12 | the driver and executors must run the same Spark |
| Iceberg | `iceberg-spark-runtime-3.5_2.12-1.6.1.jar` | must match the Spark minor version and Scala build |
| Python | 3.12 | PySpark refuses to start Python workers whose version differs from the driver's |

Node 4 only needs Python plus the pinned packages in `tools/requirements-node4.txt`.

- `tools/install_node.sh`: installs exactly these, verifies download checksums, and never deletes a
  wrong version (it stops and says what to remove).
- `tools/check_versions.sh`: OK/FAIL per tool plus a `FINGERPRINT` line to compare across nodes.

**Configs** (`conf/`, installed by `conf/install_conf.sh nodeN`, which keeps `.bak` copies):

| File | What it sets |
|---|---|
| `core-site.xml` | `fs.defaultFS = hdfs://node2:9000` |
| `hdfs-site.xml` | replication 2, data folders, bind to all interfaces, connect by hostname, permissions off |
| `hadoop-env.sh` | Java 11 path |
| `spark-env.sh` | master = node3:7077, this node's IP, worker cores and memory, Python 3 |
| `spark-defaults.conf` | master URL, Iceberg extensions, catalog `lake` on HDFS, ports 7078/7079, shuffle partitions 8, plus `spark.driver.host` per node |
| `hosts` | the 4 node lines for `/etc/hosts` |

**HDFS layout:**
```
/searchengine/
  raw/phase0/pages.jsonl        raw/phase1/changes.jsonl
  warehouse/search/<table>/     Iceberg data/ + metadata/
  checkpoints/pagerank/         PageRank checkpoints
  export/v0/  export/v1/        documents/, postings/, manifest.json, _SUCCESS
```

---

## 12. What has happened so far

| When | What |
|---|---|
| 15 Nov | Repo set up: versions pinned, configs written, tokenizer shared, node3 jobs written and tested locally (incremental index, idempotency, export validation). |
| 15 Nov | Arnav's ingestion and Monty's search service pushed. All three teams' code tested together locally on toy data: works end to end. |
| 15 Nov | Real ZeroTier IPs filled in. Node3 set up: hostname, hosts, firewall, configs, Spark master + worker, test job OK. |
| 16 Nov | **Blason away → node2's HDFS role temporarily run on node3's machine** (`node2` in `/etc/hosts` pointed at 10.113.232.128 on nodes 1, 3 and 4). |
| 16 Nov | Toy pipeline on the real cluster works, but PageRank takes 256 s. Shuffle partitions 200 → 8: **34 s**. |
| 16 Nov | Arnav's first cluster job hung: his firewall blocked port 7078, so executors couldn't reach his driver. Fixed with the ZeroTier firewall rule. |
| 16 Nov | **Real Phase 0:** 1,040 lines → 20 rejected, 20 duplicates → **1,000 docs, 499 links**; 400,519 postings; v0 served by node4. Node3 part: **72 s**. |
| 16 Nov | Monty fills `demo/queries.md` for v0. |
| 16 Nov | **Real Phase 1:** 25 added, 25 updated, 50 deleted → **975 docs, 467 links**; only 100 docs re-indexed; **node4 switches to v1 by itself**. Node3 part: **75 s**. All 4 demo queries behave as predicted. |
| 16 Nov | Idempotency: **UNCHANGED**. Iceberg demo SQL generated with real snapshot IDs. |
| 16 Nov | Every course verification check run on the cluster: all pass except the 2-DataNode ones (need node2). Monty adds `?limit=` support. |

### Real numbers

| | Phase 0 | Phase 1 |
|---|---|---|
| Documents | 1,000 | 975 |
| Links | 499 | 467 |
| Postings | 400,519 | 377,989 |
| Distinct terms | 72,408 | 69,802 |
| Pages with no out-links | 501 | 508 |
| avgdl (tokens per page) | 1046.3 | 1017.9 |
| max PageRank | 0.0017555 | 0.0039872 |
| Docs re-indexed | all | 100 |
| Ingest (node1) | 129 s | about 2 min |
| Index + PageRank + export + switch (node3) | 72 s | 75 s |
| Search latency | about 60 ms | about 60 ms |

---

## 13. Decisions and why

| Decision | Why |
|---|---|
| Tokenizer file named `tokenizer.py`, not `tokenize.py` | `tokenize.py` shadows Python's built-in `tokenize` module, which `inspect`/`linecache` import, and that breaks FastAPI |
| Tokenizer written with Spark SQL functions, no Python UDF | runs inside the JVM: faster, and no Python worker version issues |
| One overwrite commit per table in index and PageRank | readers never see a half-updated table; reruns are identical |
| Shuffle partitions 8 | default 200 made each PageRank join schedule hundreds of empty tasks |
| PageRank checkpoint every 5 iterations, on HDFS | short lineage; survives a worker being killed |
| 20 PageRank iterations | double the minimum; L1 delta below 0.0002 by the end |
| Export reads itself back before `_SUCCESS` | node4 never loads a broken export |
| Node4 pulls over WebHDFS | node4 needs no Hadoop install; plain HTTP |
| HDFS permissions off | four Linux users write into the same tables |
| Firewall allows the ZeroTier subnet instead of being disabled | Spark/HDFS have no authentication |
| All versions in one file | one place to change; the checker compares every node against it |
| Iceberg demo SQL generated from `ingest_log` | snapshot IDs change after every reset; no hand-copying |

---

## 14. Things that came up, and the answers

- **Chemistry drops to #7 in v1.** Its only incoming link was from Britain (105). Phase 1 changes
  Britain's links from `['108']` to `['158']`, so Chemistry loses its only in-link and its PageRank
  falls 6×. All 124 links that disappeared are explained by the change file (deleted sources,
  deleted targets, rewired pages); **no link from an unchanged page was lost**.
- **Cooking (146) gains 2 in-links, yet its PageRank falls** (0.0017555 → 0.0013917). The course
  page expected a rise. PageRank depends on *how important* the linking pages are, not *how many*
  there are. Cooking's main source (144) lost rank when Phase 1 rewired the pages feeding it, and the
  two new links come from pages nobody links to. Confirmed with an independent implementation.
- **Computer and China fall out of the top 5 in v1.** The max PageRank rises 2.3×, so every page's
  PageRank share shrinks. Correct, but confusing live; they're on the Avoid list.
- **501 of 1,000 pages have no out-links.** Normal with only 499 links; handled by spreading the
  dangling rank evenly.
- **First cluster job on node1 hung** ("Initial job has not accepted any resources"). Executors
  couldn't connect back to the driver through node1's firewall. The lesson: the driver must be reachable.

---

## 15. Evaluation and status

| # | Rubric item | Marks | Who | Status |
|---|---|---|---|---|
| 1 | Infrastructure & Phase 0 verification | 3 | Blason + Shriram | ⏳ final run needs node2 (2 DataNodes, 2 workers, replication 2) |
| 2 | Live Phase 1 demo | 6 | Shriram drives, everyone | ✅ rehearsed on real data, about 75 s |
| 3 | Search verification | 3 | Monty | ✅ queries confirmed, `?limit=` works |
| 4 | Idempotency test | 2 | Arnav | ✅ UNCHANGED |
| 5 | Individual viva | 6 | everyone | ⏳ |

Total 20, scaled to 10.

### Still to do
1. **Move HDFS back to node2** once Blason's laptop is available:
   - nodes 1, 3 and 4: `node2` → `10.113.232.74` in `/etc/hosts`
   - node3: `hdfs --daemon stop namenode && hdfs --daemon stop datanode && rm -rf /data/hdfs/namenode`
   - node4: `rm -rf /serving/v*`
   - node2: format with `-clusterId bd-sparksearch` (node3's DataNode already carries that ID; a plain format gives a random `CID-…` and node3's DataNode is rejected with "Incompatible clusterIDs")
   - then `demo/cluster_bringup.md` A7/A8 → B → C → D, and regenerate `demo/iceberg_demo.sql`
2. Fault-tolerance and replication demos (need node2's worker and DataNode).
3. Night before the evaluation: run-plan sections B + C, so node4 is serving **v0**.
4. Viva prep: each person's node in depth, everyone else's at a high level (this file).

---

## 16. Glossary

| Term | Meaning |
|---|---|
| **Action** (Spark) | an operation that triggers execution (`count`, `write`, `collect`) |
| **Atomic** | happens completely or not at all; never visible half-done |
| **avgdl** | average document length in tokens, used by BM25 |
| **BM25** | text relevance score with term-frequency saturation and length normalisation |
| **Block** (HDFS) | a chunk of a file (128 MB by default), the unit of storage and replication |
| **Catalog** (Iceberg) | where table metadata is found; ours is a Hadoop catalog named `lake` on HDFS |
| **Checkpoint** (Spark) | saving a result to reliable storage and cutting its lineage |
| **Client mode** | the Spark driver runs on the machine that ran `spark-submit` |
| **Copy-on-write** | updates rewrite the affected data files instead of editing them in place |
| **DAG** | directed acyclic graph: Spark's plan of computation steps |
| **Daemon** | a background service (NameNode, DataNode, master, worker) |
| **Damping factor** | 0.85 in PageRank: probability of following a link instead of jumping randomly |
| **Dangling page** | a page with no outgoing links |
| **DataNode** | HDFS daemon that stores blocks |
| **df** | document frequency: how many documents contain a term |
| **Driver** | the Spark process that plans the job and schedules tasks |
| **Executor** | Spark process on a worker that runs tasks and caches data |
| **Export** | the Parquet snapshot of index + documents for node4, `export/vN/` |
| **Heartbeat** | periodic "I'm alive" message from DataNodes / workers |
| **Hot swap** | replacing the in-memory index without stopping the service |
| **Idempotent** | running it again changes nothing |
| **IDF** | inverse document frequency: rarer terms weigh more |
| **Incremental** | only reprocess what changed |
| **Inverted index** | term → list of (document, frequency) |
| **`ingest_log`** | our table recording each phase's changed doc_ids and snapshot IDs |
| **Iceberg** | table format adding transactions, snapshots, time travel to files on HDFS |
| **Lazy evaluation** | Spark only computes when an action needs the result |
| **Lineage** | the recorded chain of transformations that produced a dataset |
| **L1 delta** | sum of absolute rank changes between PageRank iterations |
| **Manifest** (ours) | `manifest.json` in each export: counts, avgdl, max PageRank, snapshot ID |
| **Master** (Spark) | allocates cluster resources to applications |
| **MERGE INTO** | SQL that inserts / updates / deletes by matching source rows to a table |
| **NameNode** | HDFS daemon holding all file system metadata |
| **Normalisation** | scaling scores to 0…1 so they can be combined |
| **PageRank** | link-based importance score (random surfer model) |
| **Parquet** | columnar, compressed file format |
| **Partition** (Spark) | a slice of data processed by one task |
| **Posting** | one (term, doc_id, tf) entry of the inverted index |
| **Replication factor** | number of copies of each HDFS block (ours: 2) |
| **Rollback** | making an earlier Iceberg snapshot current again |
| **Shuffle** | redistributing data across the network by key (joins, groupBy) |
| **Snapshot** (Iceberg) | the full state of a table after one commit |
| **Stage** | a part of a Spark job between shuffles |
| **Stopword** | a very common word that is ignored (the, and, of) |
| **`_SUCCESS`** | empty marker file written last: "this export is complete and valid" |
| **Task** | the unit of work on one partition |
| **tf** | term frequency: occurrences of a term in a document |
| **Time travel** | querying a table as of an earlier snapshot |
| **Token / tokenizer** | a word unit / the rules that split text into tokens |
| **Tombstone** | a record that a document was deleted, so it can't be resurrected by old data |
| **Transformation** (Spark) | a lazy operation that describes new data (`filter`, `join`) |
| **UDF** | user-defined function (a Python function called from Spark) |
| **WebHDFS** | HTTP API to HDFS on port 9870 |
| **Worker** (Spark) | daemon offering a machine's cores and memory to the master |
| **ZeroTier** | virtual private network connecting our four laptops |

---

## 17. Repo map

```
README.md                     project summary + quick start
PROJECT_OVERVIEW.md           this guide
00_CONTEXT.md                 shared contracts
Distributed Search Engine.html  official spec
ARNAV_node1.md  BLASON_node2.md  SHRIRAM_node3.md  MONTY_node4.md   per-node guides
tools/                        versions.env, install_node.sh, check_versions.sh, requirements-node4.txt
conf/                         Hadoop + Spark configs, hosts, install_conf.sh
common/                       tokenizer.py, stopwords.txt
node1_ingest/                 ingest.py, run_ingest.sh, reset_tables.*, table_state.py, toy_data/
node3_compute/                index.py, pagerank.py, export.py, common_spark.py
node4_search/                 app.py, puller.py, search_index.py, static/, make_fake_export.py
scripts/run_phase.sh          one phase end to end
demo/                         cluster_bringup.md (run plan), verification_runbook.md, queries.md,
                              node3_demos.md, idempotency.sh, node1_checks.sql,
                              make_iceberg_demo.py, iceberg_demo.sql
```
