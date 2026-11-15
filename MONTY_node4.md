# Monty — Node 4 (Search service + UI)

> **For Claude:** read `00_CONTEXT.md` first. Monty owns Node 4 only. Node 4 runs **no Spark, no
> HDFS daemons, and never queries Iceberg**. It only reads the Parquet export defined in CONTEXT §4.7.
> The query tokenizer must be identical to Node 3's (CONTEXT §4.6): use `common/tokenizer.py` from Shriram.

## Rubric you carry
- Search verification (3 M).
- Phase 1 demo: the visible "after" is your UI switching to v1.
- Viva: BM25 (why k₁ and b, saturation, length normalization, IDF), score normalization and blending, how hot version switching works and why it's safe.

---

## A. Setup
Python 3.10+, `pip install fastapi uvicorn pyarrow pandas requests`. `/etc/hosts` with node1–node4 (needed because WebHDFS redirects downloads to `node2:9864` / `node3:9864`).

## B. Puller (`node4_search/puller.py`)
Pull exports over **WebHDFS** (plain HTTP on `node2:9870`, no Hadoop install needed):
- Poll every ~10 s: `GET http://node2:9870/webhdfs/v1/searchengine/export?op=LISTSTATUS`.
- For a new `vN` that has `_SUCCESS` and is newer than what's served:
  1. Download `manifest.json`, `documents/*.parquet`, `postings/*.parquet` (`op=OPEN`, follow redirects) into `/serving/vN.tmp/`.
  2. Validate: row counts match manifest, required columns present, no null doc_ids.
  3. `rename /serving/vN.tmp → /serving/vN` (atomic), then tell the API to load it.
- On any failure: delete `vN.tmp`, log it, keep serving the current version.
- Also allow manual pull: `python puller.py --version N`.

## C. API (`node4_search/app.py`, FastAPI on port 8000)
- **On load of a version:** read Parquet into memory:
  - `docs: dict[doc_id] → {url, title, snippet, doc_len, pagerank}`
  - `postings: dict[term] → list[(doc_id, tf)]`, `df[term] = len(list)`
  - `N`, `avgdl`, `max_pagerank` from manifest
- **Hot swap:** build the whole new index object first, then swap a single reference (`state.current = new_index`). Queries in flight use the old one. Never half-loaded.
- **Endpoints:**
  - `GET /search?q=...&k=10` → `{"version": N, "query": ..., "took_ms": ..., "results": [{"rank", "doc_id", "title", "url", "snippet", "bm25", "pagerank", "score"}]}`
  - `GET /health` → current version, num docs, loaded_at
  - `GET /versions` → versions on disk + which is live
  - `POST /reload` → trigger puller check (handy in the demo)
- **Scoring** (CONTEXT §4.8): tokenize query with the shared tokenizer → BM25 over docs containing ≥1 query term (k₁ 1.2, b 0.75, IDF = ln((N−df+0.5)/(df+0.5)+1)) → `final = 0.7·bm25/max_bm25_in_results + 0.3·pr/max_pagerank`. Return top k by final. Empty/no-match query → empty list, not a crash.
- **Startup:** load the highest valid `/serving/vN/` so a restart doesn't need the cluster.

## D. UI
One page served by FastAPI (plain HTML + JS is enough): search box, results list (title as link to URL, doc_id, snippet, the three scores), and a badge showing **live version** (v0/v1) so the evaluator sees the switch. Optional: small dropdown to query a specific older version side by side.

## E. Demo prep
- With Shriram, fill `demo/queries.md`: 8–10 queries, expected top results in v0 and v1.
- Test: kill export mid-way (no `_SUCCESS`) → Node 4 must stay on old version. Corrupt a downloaded file → validation fails → stays on old version. Show this if asked about "keep Phase 0 available if export fails".

## F. Order
1. Build API + UI against a **fake export** you generate yourself (a few docs) in the exact CONTEXT §4.7 format.
2. Write the puller, test against HDFS once the cluster is up.
3. Load real v0, sanity-check rankings, tune nothing (parameters are fixed by the spec).
