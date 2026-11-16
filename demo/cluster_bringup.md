# Run plan: who runs what, in what order

Go top to bottom. **Don't start a step until the "Done when" of the step before it is true.**
Commands run from the repo root (`cd ~/BD-sparksearch`) in a **new terminal**.

| Node | Person | IP | Runs |
|---|---|---|---|
| node1 | Arnav | 10.113.232.70 | ingestion driver (no daemons) |
| node2 | Blason | 10.113.232.74 | HDFS NameNode + DataNode, Spark worker |
| node3 | Shriram | 10.113.232.128 | Spark master + worker, HDFS DataNode, index/PageRank/export, `run_phase.sh` |
| node4 | Monty | 10.113.232.224 | search API + UI on :8000 |

Shriram can also run every node2 step remotely over `ssh node2`, or read the commands out on the call.

---

## ⚡ Automatic mode (recommended): one command per person

Once section A (setup) is done, each person runs **one** script on their own machine, in **any
order**. The scripts wait for each other through marker files on HDFS (`/searchengine/control/`).

| Who | Command | What it does by itself |
|---|---|---|
| Shriram (node3) | `./scripts/auto_node3.sh N` | starts node3's daemons, waits for HDFS + workers, signals node1, waits for ingest, runs index → PageRank → export, waits for node4 to switch |
| Arnav (node1) | `./scripts/auto_node1.sh N --idempotency` | waits for node3's go, ingests, reports back; then waits for the whole phase and runs the idempotency demo (drop `--idempotency` to skip it) |
| Monty (node4) | `./scripts/auto_node4.sh N` | starts the search service if needed, waits until it serves vN, runs the demo queries |

Blason (node2) still starts his NameNode, DataNode and worker by hand (section B: B1, B4);
`auto_node3.sh` waits until they are up. Scripts print a status line every 30 s while waiting and
stop with a red FAILED line on any error; node4 then keeps serving the previous version.

---

## A. One-time setup (skip any step already done)

| # | Who | Command | Done when |
|---|---|---|---|
| A1 | everyone | `git pull` | up to date |
| A2 | everyone | `sudo hostnamectl set-hostname nodeN` (your N from the table) | `hostname` prints nodeN in a new terminal |
| A3 | everyone | add the 4 lines of `conf/hosts` to `/etc/hosts`, put `#` before the `127.0.1.1` line | `getent ahostsv4 node2` prints 10.113.232.74 |
| A4 | everyone | `sudo ufw allow from 10.113.232.0/24` | `ping node1` … `ping node4` all answer |
| A5 | Arnav, Blason, Shriram | `./tools/install_node.sh` | `FINGERPRINT` line identical on all three |
| A6 | Monty | `node4_search/README.md` → Setup (venv, `/serving`) | `./tools/check_versions.sh node4` all OK |
| A7 | Arnav | `./conf/install_conf.sh node1` | prints `installed configs for node1 (10.113.232.70)` |
| A7 | Blason | `./conf/install_conf.sh node2` | prints `installed configs for node2 (10.113.232.74)` |
| A7 | Shriram | `./conf/install_conf.sh node3` | prints `installed configs for node3 (10.113.232.128)` |
| A8 | **Blason** | `hdfs namenode -format -clusterId bd-sparksearch` **ONLY ONCE EVER** (it wipes HDFS) | `successfully formatted` |
| A9 | Arnav | put `pages.jsonl` and `changes.jsonl` in `~/data/` | `ls ~/data` shows both |
| A10 | Shriram | optional, to run ingestion from node3: `ssh-copy-id <arnav-user>@node1` | `ssh node1 hostname` prints node1, no password |

---

## B. Start the cluster (every session, in this order)

| # | Who | Command | Done when |
|---|---|---|---|
| B1 | **Blason** | `hdfs --daemon start namenode && hdfs --daemon start datanode` | `jps` shows NameNode + DataNode; http://node2:9870 opens |
| B2 | **Shriram** | `start-master.sh && start-worker.sh spark://node3:7077` | http://node3:8080 shows ALIVE master |
| B3 | **Shriram** | `hdfs --daemon start datanode` | `jps` shows DataNode |
| B4 | **Blason** | `start-worker.sh spark://node3:7077` | http://node3:8080 shows **2 workers** |
| B5 | **Shriram** | `hdfs dfsadmin -report \| grep "Live datanodes"` | **(2)** |
| B6 | Blason | first time only: `hdfs dfs -mkdir -p /searchengine` | `hdfs dfs -ls /` shows it |
| B7 | Arnav | `hdfs dfs -ls / && spark-submit --master spark://node3:7077 /opt/spark/examples/src/main/python/pi.py 10` | prints `Pi is roughly 3.14…` |
| B8 | Monty | `cd node4_search && .venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000` (leave running) | http://node4:8000 opens |

---

## C. First real Phase 0 (the night before, NOT at the evaluation)

| # | Who | Command | Done when |
|---|---|---|---|
| C1 | Arnav | only if toy data was ever ingested on the cluster: `node1_ingest/reset_tables.sh --all` | tables dropped |
| C2 | Arnav | `node1_ingest/run_ingest.sh 0` | prints `phase 0 done` → tell Shriram |
| C3 | Shriram | `./scripts/run_phase.sh 0 --skip-ingest` | prints `node4 is serving v0` |
| C4 | Monty | open http://node4:8000 | badge shows **v0**, queries return results |
| C5 | Arnav | `spark-sql -f demo/node1_checks.sql` | counts and reject reasons look sane |
| C6 | Monty + Shriram | try 15–20 queries, fill the v0 column of `demo/queries.md` | 8–10 good queries chosen |

With A10 done, C2 + C3 become one command on node3: `./scripts/run_phase.sh 0`.

---

## D. Phase 1 rehearsal (do it twice before the evaluation)

| # | Who | Command | Done when |
|---|---|---|---|
| D1 | Arnav | `node1_ingest/run_ingest.sh 1` | prints `phase 1 done` → tell Shriram |
| D2 | Shriram | `./scripts/run_phase.sh 1 --skip-ingest` | prints `node4 is serving v1` |
| D3 | Monty | rerun the prepared queries | badge shows **v1**; fill the v1 column; at least 3 queries changed |
| D4 | Arnav | `demo/idempotency.sh 1` | prints `RESULT: UNCHANGED` |
| D5 | Shriram | `spark-submit demo/make_iceberg_demo.py`, then commit + push `demo/iceberg_demo.sql` | pushed |
| D6 | Blason | `git pull`, `spark-sql --master spark://node3:7077`, paste statements 1–3 of `demo/iceberg_demo.sql` | snapshots listed; count now ≠ count at Phase 0 |
| D7 | Shriram | `spark-submit node3_compute/pagerank.py --phase 1 --iterations 30` | iterations start printing |
| D8 | Blason | at ~iteration 5: `stop-worker.sh` | node3:8080 shows node2 worker DEAD |
| D9 | Shriram | watch D7 | still finishes (`wrote N ranks`) |
| D10 | Blason | `start-worker.sh spark://node3:7077` | 2 workers again |
| D11 | Shriram | `hdfs fsck /searchengine -files -blocks -locations \| tail -20` | blocks on node2 **and** node3 |

### Reset after a rehearsal (back to "only Phase 0 done")

| # | Who | Command | Done when |
|---|---|---|---|
| R1 | Monty | stop uvicorn, `rm -rf /serving/v*` | `/serving` empty |
| R2 | Arnav | `node1_ingest/reset_tables.sh --all` (type `yes`) | all lake.search tables dropped |
| R3 | Shriram | `hdfs dfs -rm -r -f /searchengine/export /searchengine/checkpoints` | gone |
| R4 | everyone | Monty restarts uvicorn (B8), then redo **C2 → C4** | badge shows v0 again |

---

## E. Evaluation day

**Before the evaluator arrives:** run section **B**, check Node 4 shows **v0** (Phase 0 was done the night
before; `/serving/v0` survives restarts). Keep http://node3:8080, http://node2:9870 and http://node4:8000 open.

| # | Rubric | Who | Command / action |
|---|---|---|---|
| E1 | Infra (3 M) | Blason | `jps`, `hdfs dfsadmin -report`, node2:9870 → Datanodes tab |
| E2 | Infra | Shriram | node3:8080 (2 workers), D11 |
| E3 | Live Phase 1 (6 M) | Arnav | D1, explain the reject / dedup summary |
| E4 | Live Phase 1 | Shriram | D2, explain each step as it runs |
| E5 | Live Phase 1 | Monty | badge flips v0 → v1, show the changed queries |
| E6 | Search (3 M) | Monty | prepared queries from `demo/queries.md`, explain BM25 + blend |
| E7 | Idempotency (2 M) | Arnav | D4 → `UNCHANGED` |
| E8 | Iceberg | Shriram, then Blason | D5, then D6 |
| E9 | Fault tolerance (if asked) | Shriram + Blason | D7 → D10 |

Rollback only if asked: statement 4 in `demo/iceberg_demo.sql`, and **always roll forward right after**.

---

## F. Shut down (reverse order)

| # | Who | Command |
|---|---|---|
| F1 | Monty | Ctrl-C uvicorn |
| F2 | Blason | `stop-worker.sh` |
| F3 | Shriram | `stop-worker.sh && stop-master.sh && hdfs --daemon stop datanode` |
| F4 | Blason | `hdfs --daemon stop datanode && hdfs --daemon stop namenode` |

Next session: section **B** again. **Never** rerun A8.

---

## When it breaks

| Symptom | Fix |
|---|---|
| Only 1 live DataNode | `127.0.1.1` line still active, or hostname isn't `nodeN`; fix, restart that DataNode |
| Worker missing on node3:8080 | start the master (B2) first, then rerun `start-worker.sh` |
| Job stuck at "Initial job has not accepted any resources" | wrong `spark.driver.host` on the driver machine (rerun A7) or firewall (A4) |
| node3 DataNode log: `Incompatible clusterIDs` | NameNode was formatted without `-clusterId bd-sparksearch`; on node3 stop the DataNode, `rm -rf /data/hdfs/datanode/*`, start it again |
| Node 4 downloads fail | node4 `/etc/hosts` missing node2/node3 (WebHDFS redirects to `:9864`) |
| A `run_phase.sh` step fails | Node 4 keeps serving the previous version; fix it, rerun the same command (every step is idempotent) |
| NameNode won't start | `tail -50 /opt/hadoop/logs/*namenode*.log` |
