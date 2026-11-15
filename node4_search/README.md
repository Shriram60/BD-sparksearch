# Node 4: search service (Monty)

Reads the Parquet export (CONTEXT §4.7) over WebHDFS, ranks with BM25 + PageRank (§4.8), serves a UI on :8000.
No Spark, no HDFS daemons, no Iceberg on this node.

| File | What |
|---|---|
| `search_index.py` | Export validation (`read_export`) and the in-memory index + BM25/blend scoring |
| `puller.py` | WebHDFS → `/serving/vN.tmp/` → validate → rename to `/serving/vN/` |
| `app.py` | FastAPI: `/`, `/search`, `/health`, `/versions`, `POST /reload`; background poller; hot swap |
| `static/index.html` | UI with live-version badge and side-by-side compare against an older version |
| `make_fake_export.py` | Fake v0/v1 export in the exact §4.7 layout, for testing without the cluster |

## Setup (once)
```bash
python3 -m venv node4_search/.venv
node4_search/.venv/bin/pip install -r tools/requirements-node4.txt
(. node4_search/.venv/bin/activate && ./tools/check_versions.sh node4)
sudo mkdir -p /serving && sudo chown $USER /serving
```
`/etc/hosts` needs node1–node4 (WebHDFS redirects downloads to `node2:9864` / `node3:9864`).

## Run
```bash
cd node4_search && .venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000
```
Starts from the highest valid `/serving/vN/` (no cluster needed), then polls `node2:9870` every 10 s.
`run_phase.sh` calls `POST /reload` and waits for `/health` to report the new version.

Manual pull: `.venv/bin/python puller.py --version 1` (it POSTs `/reload` to the local API afterwards).

## Without the cluster
```bash
cd node4_search
.venv/bin/python make_fake_export.py --out /tmp/fake_export --version 0
SERVING_DIR=/tmp/serving EXPORT_SOURCE=file:///tmp/fake_export .venv/bin/uvicorn app:app --port 8000
.venv/bin/python make_fake_export.py --out /tmp/fake_export --version 1   # UI flips to v1 within 10 s
```

## Demo: "Phase 0 stays available if export fails"
| Situation | What Node 4 does |
|---|---|
| `export/vN/` exists but no `_SUCCESS` (export killed mid-way) | Ignores vN, keeps serving v(N-1) |
| Downloaded file corrupt / truncated / row count ≠ manifest | `vN.tmp/` deleted, error in log and in `GET /versions` → `failed_pulls`, keeps serving v(N-1) |
| NameNode unreachable | Keeps serving from memory; `/health` shows `last_error` |
| Node 4 restarted | Loads the highest valid `/serving/vN/` from disk |
| vN re-exported with `export.py --force` (rehearsal reset) | Manifest `created_at` changed → pulled again and swapped |

## Viva notes
- **BM25** `Σ IDF(t)·tf·(k₁+1) / (tf + k₁·(1−b+b·dl/avgdl))`. k₁=1.2 caps how much repeated terms help
  (tf saturates toward k₁+1). b=0.75 penalises long docs, which match more terms just by being long.
  IDF `ln((N−df+0.5)/(df+0.5)+1)` weights rare terms up; the `+1` keeps it positive for very common terms.
- **Blend** `0.7·bm25/max_bm25_in_results + 0.3·pr/max_pagerank`. Both terms normalised to [0,1] so
  the weights mean something; BM25 normalised per query (its scale depends on the query), PageRank per corpus.
- **Hot swap** the new index is built and validated completely, then one Python reference is reassigned
  (atomic under the GIL). Each request reads that reference once, so it sees the old or the new index, never a mix.
- **Atomic publish on disk** download into `vN.tmp/`, validate, `rename` to `vN/` (atomic on one filesystem).
  `_SUCCESS` is written last by Node 3, so its presence means the export is complete.
- Query words are deduplicated (`paris paris` = `paris`); empty or stopword-only queries return `[]`.
