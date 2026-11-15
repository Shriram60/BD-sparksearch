"""Shared tokenizer (CONTEXT §4.6). Node 3 indexes with the Spark version of these exact rules
(node3_compute/index.py) and Node 4 tokenizes queries with this file, so the two must not drift.

Rules: lowercase -> every char outside [a-z0-9] becomes a space -> split on whitespace
-> drop tokens shorter than 2 chars -> drop stopwords. No stemming.

Not named tokenize.py on purpose: that would shadow Python's stdlib `tokenize` module.
"""
import re
from pathlib import Path

STOPWORDS_PATH = Path(__file__).with_name("stopwords.txt")
_NON_ALNUM = re.compile(r"[^a-z0-9]")


def load_stopwords(path=STOPWORDS_PATH):
    with open(path, encoding="utf-8") as f:
        return frozenset(line.strip() for line in f if line.strip())


STOPWORDS = load_stopwords()


def tokenize(text):
    if not text:
        return []
    return [t for t in _NON_ALNUM.sub(" ", text.lower()).split()
            if len(t) >= 2 and t not in STOPWORDS]


if __name__ == "__main__":
    import sys
    print(tokenize(" ".join(sys.argv[1:]) or "The Quick-brown fox's 2nd jump, über a log!"))
