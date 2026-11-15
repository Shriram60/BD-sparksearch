# Node 3 demo runbook (Shriram)

## Cluster health (Infrastructure, 3 M)
```bash
./tools/check_versions.sh              # on nodes 1-3: FINGERPRINT lines must be identical
hdfs dfsadmin -report | grep -E 'Live datanodes|Name:'   # 2 live DataNodes
hdfs fsck /searchengine -files -blocks -locations | tail -20   # every block on node2 AND node3
```
Spark master UI http://node3:8080 shows 2 ALIVE workers.

## Live Phase 1
```bash
./scripts/run_phase.sh 1          # or --skip-ingest if Arnav runs step 1 himself
```

## Fault tolerance (kill a worker mid-PageRank)
1. Terminal A on node3: `spark-submit node3_compute/pagerank.py --phase 1 --iterations 30`
2. When the iteration log reaches ~5, on node2: `stop-worker.sh`
3. Show http://node3:8080: node2 worker DEAD, its tasks re-run on node3 and the job still finishes.
   What saved it: lineage. Lost partitions are recomputed from the last checkpoint (every 5 iterations, on HDFS).
4. Restart: on node2 `start-worker.sh spark://node3:7077`.

## Iceberg incremental index (viva)
- index.py reads `changed_doc_ids` from the latest `lake.search.ingest_log` row of the phase.
- Phase 1 re-tokenizes only those docs, keeps every other posting, writes ONE overwrite snapshot.
- Rerun = same input rows → same table contents (idempotent).
```sql
SELECT snapshot_id, committed_at, operation FROM lake.search.postings.snapshots;
```

## Rehearsal reset (back to Phase 0 state)
Roll back each table to its Phase 0 snapshot (`CALL lake.system.rollback_to_snapshot('search.<table>', <id>)`),
then `export.py --phase 1 --force` on the next rehearsal so v1 is rebuilt instead of skipped.
