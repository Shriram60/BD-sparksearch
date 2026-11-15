-- Node 1 verification queries (Arnav). Run on node1:
--   spark-sql -f demo/node1_checks.sql                 (cluster, uses spark-defaults.conf)
--   LOCAL=1: see node1_ingest/README.md for the local-mode flags
-- Every statement is read-only.

-- 1. Row counts (Phase 0 real data: 1000 / 499 / 20; after Phase 1: 975 / 467 / 20)
SELECT 'documents' AS tbl, count(*) AS n FROM lake.search.documents
UNION ALL SELECT 'links', count(*) FROM lake.search.links
UNION ALL SELECT 'rejected', count(*) FROM lake.search.rejected;

-- 2. Why records were rejected, per phase
SELECT phase, reason, count(*) AS n FROM lake.search.rejected GROUP BY phase, reason ORDER BY phase, reason;

-- 3. A few rejected raw lines
SELECT phase, reason, substr(raw, 1, 80) AS raw FROM lake.search.rejected ORDER BY phase, raw LIMIT 5;

-- 4. Dedup worked: no doc_id appears twice
SELECT count(*) AS duplicate_doc_ids FROM (SELECT doc_id FROM lake.search.documents GROUP BY doc_id HAVING count(*) > 1);

-- 5. No dangling links: every src and dst exists in documents, no self-links
SELECT count(*) AS dangling_or_self_links FROM lake.search.links l
LEFT JOIN lake.search.documents s ON l.src_id = s.doc_id
LEFT JOIN lake.search.documents d ON l.dst_id = d.doc_id
WHERE s.doc_id IS NULL OR d.doc_id IS NULL OR l.src_id = l.dst_id;

-- 6. Which phase last wrote each document (version = phase number for the real data)
SELECT phase, version, count(*) AS docs FROM lake.search.documents GROUP BY phase, version ORDER BY phase;

-- 7. ingest_log: one row per run that changed something (what Node 3 reads)
SELECT phase, documents_snapshot_id, links_snapshot_id, size(changed_doc_ids) AS changed,
       accepted, rejected, run_at
FROM lake.search.ingest_log ORDER BY run_at;

-- 8. Iceberg snapshots of documents (one per ingest that changed something)
SELECT snapshot_id, committed_at, operation FROM lake.search.documents.snapshots ORDER BY committed_at;
