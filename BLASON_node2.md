# Blason — Node 2 (HDFS NameNode + DataNode, Spark worker)

You don't need to write any code. Your job is to set up and run the storage machine that every
other node depends on, then show it working during the evaluation. Do steps 1–6 on a call with
Shriram the first time.

**Your marks:** Infrastructure check (3 M, shared with Shriram) and the Iceberg snapshot demo. The
individual viva (6 M) is yours alone, so read section 8 properly.

---

## 1. Install (Ubuntu)

```bash
sudo apt update
sudo apt install -y openjdk-11-jdk ssh rsync curl
java -version            # must say 11

cd /tmp
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
sudo tar -xzf hadoop-3.3.6.tar.gz -C /opt && sudo mv /opt/hadoop-3.3.6 /opt/hadoop
sudo tar -xzf spark-3.5.3-bin-hadoop3.tgz -C /opt && sudo mv /opt/spark-3.5.3-bin-hadoop3 /opt/spark
sudo chown -R $USER:$USER /opt/hadoop /opt/spark
sudo mkdir -p /data/hdfs/namenode /data/hdfs/datanode && sudo chown -R $USER:$USER /data
```

Add to the end of `~/.bashrc`, then run `source ~/.bashrc`:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=/opt/spark
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

## 2. Network

1. Join the team's ZeroTier network; tell the group your ZeroTier IP (`ip a`, look for `zt...`).
2. `sudo nano /etc/hosts`:
   - put a `#` in front of the line that starts with `127.0.1.1`
   - add the four `nodeX` lines Shriram sends (same on everyone's machine)
3. `sudo ufw disable`
4. Test: `ping node3` works.

## 3. Config files

Shriram sends you a folder. Copy them in:
```bash
cp core-site.xml hdfs-site.xml hadoop-env.sh   /opt/hadoop/etc/hadoop/
cp spark-env.sh spark-defaults.conf            /opt/spark/conf/
cp iceberg-spark-runtime-3.5_2.12-1.6.1.jar    /opt/spark/jars/
```

## 4. Format HDFS (ONE TIME ONLY, EVER)

```bash
hdfs namenode -format -clusterId bd-sparksearch
```
⚠️ **Never run this again.** Running it a second time erases the cluster and the DataNodes will refuse to join. If something breaks, ask Shriram instead.

## 5. Start everything (every time you boot the machine)

```bash
hdfs --daemon start namenode
hdfs --daemon start datanode
start-worker.sh spark://node3:7077      # only works once Shriram has started the master on node3
jps                                     # should list NameNode, DataNode, Worker
```

Stop everything:
```bash
stop-worker.sh
hdfs --daemon stop datanode
hdfs --daemon stop namenode
```

## 6. Check it works

```bash
hdfs dfsadmin -report                   # "Live datanodes (2)"
hdfs dfs -mkdir -p /searchengine
hdfs dfs -ls /
```
In a browser: `http://node2:9870` (HDFS UI; Datanodes tab should show node2 and node3) and `http://node3:8080` (Spark UI; should show 2 workers).

## 7. Your evaluation demo

**a) Infrastructure (do this first when the evaluator arrives):**
```bash
jps
hdfs dfsadmin -report
hdfs fsck /searchengine -files -blocks -locations | tail -n 20    # shows replication 2, blocks on node2 and node3
hdfs dfs -ls -R /searchengine/export
```
Then show the two web UIs above.

**b) Iceberg snapshots, time travel, rollback.** Open Spark SQL:
```bash
spark-sql --master spark://node3:7077
```
Then run the statements in `demo/iceberg_demo.sql` (`git pull` first; Shriram generates it with the real snapshot ids after each run). The general shape:
```sql
-- 1. history of changes to the documents table
SELECT snapshot_id, committed_at, operation FROM lake.search.documents.snapshots;

-- 2. number of documents now vs. after Phase 0 (time travel)
SELECT count(*) FROM lake.search.documents;
SELECT count(*) FROM lake.search.documents VERSION AS OF <phase0_snapshot_id>;

-- 3. rollback (ONLY if the evaluator asks; roll forward again afterwards)
CALL lake.system.rollback_to_snapshot('search.documents', <phase0_snapshot_id>);
CALL lake.system.set_current_snapshot('search.documents', <phase1_snapshot_id>);
```

**c) Replication fault demo (if asked):** Shriram stops the DataNode on node3; you show `hdfs dfs -cat` on a file still works because a copy is on node2.

## 8. Viva prep (learn these, in your own words)

- **NameNode:** keeps the file system map: which files exist and which blocks are on which machine. It stores no file data itself. Ours is on node2.
- **DataNode:** stores the actual blocks. We have two: node2 and node3.
- **Block:** HDFS splits files into blocks (default 128 MB; our files are small so each is one block).
- **Replication factor 2:** every block is stored on both DataNodes, so if one machine dies the data is still readable from the other.
- **Why the NameNode is a weak point:** if it's down, nobody can find any file, even though the data is still on the DataNodes. Real clusters fix this with a standby NameNode.
- **Spark worker:** a process that offers CPU and memory to the Spark master (node3). The master gives it tasks; if a worker dies, its tasks are re-run on the other worker.
- **Iceberg:** a table format on top of plain files in HDFS. Every change creates a **snapshot** (a full version of the table). That lets you see history, query an old version (**time travel**), and undo a bad update (**rollback**).
- **Why Iceberg, not just Parquet files:** safe updates and merges, a consistent version for readers, and history.
- **The whole pipeline in one line:** Node 1 cleans the data and stores it in Iceberg on HDFS (my node) → Node 3 builds the index and PageRank with Spark (my worker helps) → exports Parquet → Node 4 serves search.

## 9. If something goes wrong
- `jps` missing NameNode → `cat /opt/hadoop/logs/*namenode*.log | tail -50` and send to Shriram.
- Only 1 live DataNode → check `ping node3`, check `/etc/hosts`, check the `127.0.1.1` line is commented.
- Worker not on Spark UI → master on node3 must be started first, then rerun `start-worker.sh`.
