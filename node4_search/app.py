"""Node 4 search service: FastAPI on :8000 serving the UI and BM25 + PageRank search (CONTEXT §4.8).

  cd node4_search && .venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000

On startup it loads the highest valid SERVING_DIR/vN/ (no cluster needed), then a background thread
runs the puller every POLL_SECONDS (0 disables it). Every version on disk is loaded so older ones can
be queried side by side; the newest valid one is live.

Hot swap: a new SearchIndex is fully built and validated first, then one reference (state.view) is
replaced. A request reads state.view once, so it uses either the old or the new index, never a mix.

Env: SERVING_DIR, EXPORT_SOURCE (see puller.py), POLL_SECONDS (default 10).
"""
import logging
import os
import shutil
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from puller import SERVING_DIR, Puller, local_versions
from search_index import InvalidExport, SearchIndex, read_manifest

POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "10"))
STATIC = Path(__file__).resolve().parent / "static"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("node4")


class View:
    """Everything a request needs, swapped as one object."""

    def __init__(self, indexes):
        self.indexes = indexes  # version -> SearchIndex
        self.live = indexes[max(indexes)] if indexes else None


class State:
    def __init__(self, serving=SERVING_DIR):
        self.serving = serving
        self.view = View({})
        self.puller = Puller(serving=serving)
        self.lock = threading.Lock()  # one sync/refresh at a time (poller thread vs POST /reload)
        self.last_check = None
        self.last_error = None
        self.invalid = {}  # version -> reason, for versions on disk that failed validation

    def refresh(self):
        """Load new/changed versions from disk, then swap. Unchanged versions reuse their index."""
        old = self.view.indexes
        new, invalid = {}, {}
        for n in local_versions(self.serving):
            vdir = Path(self.serving) / f"v{n}"
            try:
                if n in old and old[n].manifest == read_manifest(vdir):
                    new[n] = old[n]
                    continue
                t0 = time.time()
                new[n] = SearchIndex.from_dir(vdir)
                log.info("loaded v%d (%d docs, %d terms) in %.2fs", n, len(new[n].docs),
                         len(new[n].postings), time.time() - t0)
            except InvalidExport as e:
                log.error("v%d on disk is invalid, not serving it: %s", n, e)
                invalid[n] = str(e)
        before = self.view.live.version if self.view.live else None
        self.view, self.invalid = View(new), invalid  # the swap
        after = self.view.live.version if self.view.live else None
        if after != before:
            log.info("live version: %s -> %s", before, after)

    def check(self):
        """Pull from the cluster, then refresh. A cluster outage only means no new version."""
        with self.lock:
            pulled = []
            try:
                pulled = self.puller.sync()
                self.last_error = None
            except Exception as e:
                self.last_error = f"source unreachable: {e}"
                log.warning(self.last_error)
            self.last_check = time.strftime("%Y-%m-%dT%H:%M:%S%z")
            self.refresh()
            return pulled


state = State()


def _poll_forever():
    while True:
        time.sleep(POLL_SECONDS)
        try:
            state.check()
        except Exception:
            log.exception("poll failed")


@asynccontextmanager
async def lifespan(_app):
    Path(state.serving).mkdir(parents=True, exist_ok=True)
    for leftover in Path(state.serving).glob("v*.tmp"):  # from a puller killed mid-download
        shutil.rmtree(leftover, ignore_errors=True)
    with state.lock:
        state.refresh()
    if POLL_SECONDS > 0:
        threading.Thread(target=_poll_forever, name="poller", daemon=True).start()
    yield


app = FastAPI(title="SparkSearch Node 4", lifespan=lifespan)


@app.get("/", include_in_schema=False)
def ui():
    return FileResponse(STATIC / "index.html")


@app.get("/search")
def search(q: str = "", k: int = Query(10, ge=1, le=100), version: int | None = None):
    view = state.view  # read once: this request sticks to one index even if a swap happens
    if version is None:
        index = view.live
        if index is None:
            raise HTTPException(503, "no version loaded yet")
    else:
        index = view.indexes.get(version)
        if index is None:
            raise HTTPException(404, f"version {version} is not loaded")
    t0 = time.perf_counter()
    terms, results = index.search(q, k)
    return {"version": index.version, "query": q, "terms": terms,
            "took_ms": round((time.perf_counter() - t0) * 1000, 3), "results": results}


@app.get("/health")
def health():
    live = state.view.live
    return {"status": "ok" if live else "no_data",
            "version": live.version if live else None,
            "num_docs": len(live.docs) if live else 0,
            "loaded_at": live.loaded_at if live else None,
            "last_check": state.last_check, "last_error": state.last_error}


@app.get("/versions")
def versions():
    view = state.view
    out = []
    for n, ix in sorted(view.indexes.items()):
        m = ix.manifest
        out.append({"version": n, "num_docs": m["num_docs"], "num_postings": m["num_postings"],
                    "avgdl": m["avgdl"], "documents_snapshot_id": m["documents_snapshot_id"],
                    "created_at": m["created_at"], "loaded_at": ix.loaded_at,
                    "live": ix is view.live})
    failed_pulls = {n: err for n, (_, err) in state.puller.failed.items()}
    return {"live": view.live.version if view.live else None, "versions": out,
            "invalid_on_disk": state.invalid, "failed_pulls": failed_pulls}


@app.post("/reload")
def reload():
    pulled = state.check()
    live = state.view.live
    return {"pulled": pulled, "version": live.version if live else None, "error": state.last_error}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
