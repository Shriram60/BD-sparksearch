# Cluster bring-up (all 4 machines)

Do the steps in order; each one depends on the previous.

## 0. Once, on all 4 machines (everyone)

1. Join the same ZeroTier network. Note each machine's ZeroTier IP, put the real IPs in `conf/hosts`, push.
2. Set the hostname, so HDFS DataNodes register with a name the others can resolve:
   ```bash
   sudo hostnamectl set-hostname node3        # node1 / node2 / node3 / node4
   ```
3. `/etc/hosts`: paste the 4 lines from `conf/hosts` and **comment out the `127.0.1.1` line**.
4. Firewall: allow only the ZeroTier subnet instead of disabling it:
   ```bash
   sudo ufw allow from 10.147.17.0/24 && sudo ufw enable   # use the real subnet
   ```
5. Check: `ping node1`, `ping node2`, `ping node3`, `ping node4` all work from every machine.
6. Tools: `git pull`, then
   - nodes 1-3: `./tools/install_node.sh`, post the `FINGERPRINT` line in chat; all three must be identical
   - node 4: `./tools/install_node.sh node4`

## 1. Configs (nodes 1-3)

```bash
./conf/install_conf.sh node1    # Arnav
./conf/install_conf.sh node2    # Blason (with Shriram on a call)
./conf/install_conf.sh node3    # Shriram
```
Optional worker size per laptop: `./conf/install_conf.sh node3 4 4g` (cores, memory).

## 2. Start daemons, in this order

| # | Where | Command |
|---|---|---|
| 1 | node2 (first time only) | `hdfs namenode -format` |
| 2 | node2 | `hdfs --daemon start namenode` then `hdfs --daemon start datanode` |
| 3 | node3 | `hdfs --daemon start datanode` |
| 4 | node3 | `start-master.sh` then `start-worker.sh spark://node3:7077` |
| 5 | node2 | `start-worker.sh spark://node3:7077` |
| 6 | node2 | `hdfs dfs -mkdir -p /searchengine` |

Verify from node3:
```bash
hdfs dfsadmin -report | grep "Live datanodes"    # (2)
```
http://node3:8080 shows 2 ALIVE workers.

## 3. Client nodes

- **Node 1 (Arnav):** `hdfs dfs -ls /` works; `pages.jsonl` and `changes.jsonl` are in `~/data/`.
- **Node 4 (Monty):** `sudo mkdir -p /serving && sudo chown $USER /serving`, then from `node4_search/`:
  `.venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000`. Check http://node4:8000.

## 4. Who runs ingestion

- **Option A (one command from node3):** passwordless ssh node3 → node1:
  ```bash
  ssh-keygen -t ed25519            # on node3, if no key yet
  ssh-copy-id <arnav-user>@node1
  ```
  If the Linux username differs, add `Host node1` / `User <arnav-user>` to `~/.ssh/config`.
  If the repo path on node1 differs, pass `NODE1_REPO=/path/on/node1`.
- **Option B (simpler):** Arnav runs `node1_ingest/run_ingest.sh 0` on node1, then node3 runs
  `./scripts/run_phase.sh 0 --skip-ingest`.

## 5. First real run (node3)

```bash
./scripts/run_phase.sh 0          # add --skip-ingest for option B
```
Success: each step prints its time, `export/v0/_SUCCESS` exists, Node 4's UI badge shows **v0**.
Then rehearse Phase 1 with `demo/node3_demos.md`.

## When it breaks

| Symptom | Likely cause |
|---|---|
| Only 1 live DataNode | `127.0.1.1` line still active, or hostname not set to `nodeN` |
| Spark job stuck at "Initial job has not accepted any resources" | wrong `spark.driver.host` on the driver machine, or firewall blocking 7078/7079 |
| Node 4 downloads fail | node4's `/etc/hosts` missing node2/node3 (WebHDFS redirects to `node2:9864` / `node3:9864`) |
| NameNode won't start | `tail -50 /opt/hadoop/logs/*namenode*.log` |
