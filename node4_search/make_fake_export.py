"""Writes a small fake export in the exact CONTEXT §4.7 layout, for testing Node 4 without the cluster.

  python make_fake_export.py --out /tmp/fake_export --version 0
  python make_fake_export.py --out /tmp/fake_export --version 1   # v0 + an edited, a new and a deleted doc

Then point the puller at it:  python puller.py --source file:///tmp/fake_export --once
"""
import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from search_index import tokenize

BASE = {
    "1": ("Paris", "Paris is the capital and largest city of France. Paris is on the river Seine."),
    "2": ("France", "France is a country in Western Europe. Its capital city is Paris."),
    "3": ("Berlin", "Berlin is the capital of Germany and its largest city."),
    "4": ("Germany", "Germany is a country in Central Europe. The capital is Berlin."),
    "5": ("River", "A river is a natural stream of water that flows into an ocean, sea or lake."),
    "6": ("Seine", "The Seine is a river in northern France. It flows through Paris into the sea."),
    "7": ("Europe", "Europe is a continent. France and Germany are countries in Europe."),
    "8": ("Python (programming language)", "Python is a programming language created by Guido van Rossum."),
}
PAGERANK = {"1": 0.22, "2": 0.18, "3": 0.12, "4": 0.11, "5": 0.06, "6": 0.08, "7": 0.15, "8": 0.04}

PHASE1 = {  # edited doc 8, new doc 9, deleted doc 5
    "8": ("Python (programming language)",
          "Python is a popular programming language. Python is used for data science and Spark jobs."),
    "9": ("Apache Spark", "Apache Spark is an engine for big data processing. PySpark lets Python use Spark."),
}


def corpus(version):
    docs = dict(BASE)
    pr = dict(PAGERANK)
    if version >= 1:
        docs.update(PHASE1)
        del docs["5"]
        pr.pop("5")
        pr["9"] = 0.05
    return docs, pr


def write(out_root, version):
    docs, pr = corpus(version)
    vdir = Path(out_root) / f"v{version}"
    (vdir / "documents").mkdir(parents=True, exist_ok=True)
    (vdir / "postings").mkdir(parents=True, exist_ok=True)
    (vdir / "_SUCCESS").unlink(missing_ok=True)

    doc_rows, posting_rows = [], []
    for doc_id, (title, text) in sorted(docs.items()):
        tokens = tokenize(text)
        doc_rows.append({"doc_id": doc_id, "url": f"https://simple.wikipedia.org/wiki/{title.replace(' ', '_')}",
                         "title": title, "snippet": text[:200], "doc_len": len(tokens),
                         "pagerank": pr[doc_id]})
        posting_rows += [{"term": t, "doc_id": doc_id, "tf": tf} for t, tf in sorted(Counter(tokens).items())]

    doc_schema = pa.schema([("doc_id", pa.string()), ("url", pa.string()), ("title", pa.string()),
                            ("snippet", pa.string()), ("doc_len", pa.int32()), ("pagerank", pa.float64())])
    post_schema = pa.schema([("term", pa.string()), ("doc_id", pa.string()), ("tf", pa.int32())])
    # Same file names Spark's coalesce(1).write.parquet produces.
    pq.write_table(pa.Table.from_pylist(doc_rows, doc_schema), vdir / "documents" / "part-00000-fake.snappy.parquet")
    pq.write_table(pa.Table.from_pylist(posting_rows, post_schema), vdir / "postings" / "part-00000-fake.snappy.parquet")

    manifest = {
        "version": version, "phase": version, "num_docs": len(doc_rows), "num_postings": len(posting_rows),
        "avgdl": sum(d["doc_len"] for d in doc_rows) / len(doc_rows),
        "max_pagerank": max(d["pagerank"] for d in doc_rows),
        "documents_snapshot_id": 1000 + version,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (vdir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    (vdir / "_SUCCESS").write_text("")  # last, like export.py
    return vdir, manifest


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--version", type=int, default=0)
    a = ap.parse_args()
    vdir, m = write(a.out, a.version)
    print(f"wrote {vdir}: {m['num_docs']} docs, {m['num_postings']} postings")
