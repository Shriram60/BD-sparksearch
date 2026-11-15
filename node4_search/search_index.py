"""In-memory search index for one export version (CONTEXT §4.7) and BM25 + PageRank ranking (§4.8).

read_export() validates a vN/ directory and is used both by the puller (before the atomic rename)
and by the API (before a version goes live), so a version that fails validation is never served.
"""
import json
import math
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.tokenizer import tokenize  # noqa: E402  (same rules as Node 3's index.py)

K1 = 1.2
B = 0.75
W_BM25 = 0.7
W_PAGERANK = 0.3

DOC_COLUMNS = {"doc_id", "url", "title", "snippet", "doc_len", "pagerank"}
POSTING_COLUMNS = {"term", "doc_id", "tf"}
MANIFEST_KEYS = {"version", "phase", "num_docs", "num_postings", "avgdl", "max_pagerank",
                 "documents_snapshot_id", "created_at"}


class InvalidExport(Exception):
    pass


def read_manifest(vdir):
    try:
        manifest = json.loads((Path(vdir) / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise InvalidExport(f"manifest.json unreadable: {e}") from e
    missing = MANIFEST_KEYS - manifest.keys()
    if missing:
        raise InvalidExport(f"manifest.json missing keys {sorted(missing)}")
    return manifest


def _read_table(vdir, name, columns):
    d = Path(vdir) / name
    files = sorted(p for p in d.glob("*.parquet")) if d.is_dir() else []
    if not files:
        raise InvalidExport(f"{name}/ has no parquet files")
    try:
        table = pq.ParquetDataset([str(f) for f in files]).read()
    except Exception as e:  # corrupt/truncated file
        raise InvalidExport(f"{name}/ unreadable: {e}") from e
    missing = columns - set(table.column_names)
    if missing:
        raise InvalidExport(f"{name}/ missing columns {sorted(missing)}")
    if table.column("doc_id").null_count:
        raise InvalidExport(f"{name}/ has {table.column('doc_id').null_count} null doc_ids")
    return table


def read_export(vdir):
    """Validate vN/ against its manifest. Returns (manifest, documents, postings) or raises InvalidExport."""
    manifest = read_manifest(vdir)
    docs = _read_table(vdir, "documents", DOC_COLUMNS)
    postings = _read_table(vdir, "postings", POSTING_COLUMNS)
    if docs.num_rows != manifest["num_docs"]:
        raise InvalidExport(f"documents has {docs.num_rows} rows, manifest says {manifest['num_docs']}")
    if postings.num_rows != manifest["num_postings"]:
        raise InvalidExport(f"postings has {postings.num_rows} rows, manifest says {manifest['num_postings']}")
    return manifest, docs, postings


class SearchIndex:
    """Immutable once built: the API swaps whole SearchIndex objects, it never mutates one."""

    def __init__(self, manifest, docs, postings, path=None):
        self.manifest = manifest
        self.version = int(manifest["version"])
        self.path = str(path) if path else None
        self.loaded_at = time.strftime("%Y-%m-%dT%H:%M:%S%z")

        self.docs = {}
        for row in docs.select(sorted(DOC_COLUMNS)).to_pylist():
            doc_id = row.pop("doc_id")
            row["doc_len"] = row["doc_len"] or 0
            row["pagerank"] = row["pagerank"] or 0.0
            self.docs[doc_id] = row

        self.postings = {}
        cols = postings.select(["term", "doc_id", "tf"]).to_pydict()
        for term, doc_id, tf in zip(cols["term"], cols["doc_id"], cols["tf"]):
            self.postings.setdefault(term, []).append((doc_id, tf))

        self.N = int(manifest["num_docs"])
        self.avgdl = float(manifest["avgdl"]) or 1.0
        self.max_pagerank = float(manifest["max_pagerank"])

    @classmethod
    def from_dir(cls, vdir):
        return cls(*read_export(vdir), path=vdir)

    def idf(self, term):
        df = len(self.postings.get(term, ()))
        return math.log((self.N - df + 0.5) / (df + 0.5) + 1)

    def search(self, query, k=10):
        # Repeated query words count once: "paris paris" ranks like "paris".
        terms = list(dict.fromkeys(tokenize(query)))
        bm25 = {}
        for term in terms:
            plist = self.postings.get(term)
            if not plist:
                continue
            idf = self.idf(term)
            for doc_id, tf in plist:
                dl = self.docs[doc_id]["doc_len"]
                norm = K1 * (1 - B + B * dl / self.avgdl)
                bm25[doc_id] = bm25.get(doc_id, 0.0) + idf * tf * (K1 + 1) / (tf + norm)
        if not bm25:
            return terms, []

        max_bm25 = max(bm25.values()) or 1.0
        max_pr = self.max_pagerank or 1.0
        scored = []
        for doc_id, s in bm25.items():
            pr = self.docs[doc_id]["pagerank"]
            scored.append((W_BM25 * s / max_bm25 + W_PAGERANK * pr / max_pr, s, pr, doc_id))
        # Ties broken by doc_id so the same query always returns the same order.
        scored.sort(key=lambda r: (-r[0], r[3]))

        results = []
        for rank, (score, s, pr, doc_id) in enumerate(scored[:k], 1):
            d = self.docs[doc_id]
            results.append({"rank": rank, "doc_id": doc_id, "title": d["title"], "url": d["url"],
                            "snippet": d["snippet"], "bm25": round(s, 6), "pagerank": round(pr, 8),
                            "score": round(score, 6)})
        return terms, results
