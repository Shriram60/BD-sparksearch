# Node 4: Monty (search service + UI)

Node 4 serves search. It pulls the validated Parquet export from HDFS over WebHDFS, holds it in
memory, ranks results with BM25 + PageRank, and serves a web UI. It runs **no Spark, no HDFS
daemons, and never queries Iceberg**; it only reads the export format in `00_CONTEXT.md` §4.7.
Implementation details and commands: `node4_search/README.md`.

## Rubric
| Item | Marks | Role |
|---|---|---|
| Search verification | 3 | prepared queries, explain BM25 and the blend |
| Live Phase 1 demo | 6 (shared) | the visible "after": the UI badge flips v0 → v1 |
| Individual viva | 6 | BM25, normalisation and blending, hot version switching |

## Components

| File | What |
|---|---|
| `node4_search/puller.py` | polls `http://node2:9870/webhdfs/v1/searchengine/export` every 10 s; for a new `vN` with `_SUCCESS`: download to `/serving/vN.tmp/` → validate against `manifest.json` → rename to `/serving/vN/` |
| `node4_search/search_index.py` | export validation + in-memory index + BM25 / blend scoring |
| `node4_search/app.py` | FastAPI on :8000: `/`, `/search?q=…&k=…` (or `&limit=…`), `/health`, `/versions`, `POST /reload`; background poller; hot swap |
| `node4_search/static/index.html` | UI with live-version badge and side-by-side compare against an older version |
| `node4_search/make_fake_export.py` | fake export in the exact §4.7 layout, for testing without the cluster |

Queries are tokenized with `common/tokenizer.py`, the same rules Node 3 uses to build the index.

## Ranking
- BM25 with k₁ = 1.2, b = 0.75:
  `Σ IDF(t)·tf·(k₁+1) / (tf + k₁·(1 − b + b·dl/avgdl))`, `IDF(t) = ln((N − df + 0.5)/(df + 0.5) + 1)`.
- Final score: `0.7 · bm25 / max_bm25_in_results + 0.3 · pagerank / max_pagerank`.
- Empty or stopword-only queries return `[]`, not an error.

## Running
```bash
# once
python3 -m venv node4_search/.venv
node4_search/.venv/bin/pip install -r tools/requirements-node4.txt
sudo mkdir -p /serving && sudo chown $USER /serving

# every session
cd node4_search && .venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000
```
On start it loads the highest valid `/serving/vN/` from disk (no cluster needed), then polls for
newer versions. `scripts/run_phase.sh` calls `POST /reload` and waits for `/health` to report vN.

## Safety: "Phase 0 stays available if Phase 1 fails"
| Situation | Node 4 |
|---|---|
| `export/vN/` without `_SUCCESS` | ignores vN, keeps serving v(N-1) |
| corrupt / truncated download, counts ≠ manifest | deletes `vN.tmp/`, logs the error, keeps v(N-1) |
| NameNode unreachable | keeps serving from memory; `/health` shows `last_error` |
| Node 4 restarted | loads the highest valid `/serving/vN/` |

## Results on the real data
- v0: 1,000 docs; v1: 975 docs. The switch to v1 happened by itself within 6 s of the export.
- Demo queries and expected results: `demo/queries.md`. Phase 1 queries: `added during project`
  (new pages), `plague` (update in place), `alan turing` (delete), `breakfast sausage` (delete + update).
- Avoid list (correct but confusing live): `computer`, `china`, `chemistry`, because the max
  PageRank rises 2.3× in v1, so each page's PageRank share shrinks.
- `/search` also accepts `?limit=` as an alias of `k`, because the course runbook uses `?limit=5`.

## Viva notes
- **k₁ = 1.2:** term-frequency saturation; the 10th repeat of a word adds far less than the 2nd.
  **b = 0.75:** length normalisation; long pages don't win just by containing more words.
  **IDF:** rare terms count more; the `+1` keeps it positive for very common terms.
- **Blend:** both parts are normalised to [0, 1] so the 0.7 / 0.3 weights mean something. BM25 is
  normalised per query (its scale depends on the query), PageRank per corpus.
- **Hot swap:** the new index is fully built and validated, then a single reference is reassigned
  (atomic under the GIL). Each request reads that reference once, so it sees the old or the new
  index, never a mix.
- **Atomic publish on disk:** download into `vN.tmp/`, validate, `rename` to `vN/` (atomic on one
  filesystem). `_SUCCESS` is written last by Node 3, so its presence means the export is complete.
