# Prepared demo queries

At least 3 queries must change between v0 and v1. Top 3 shown as `Title (doc_id)`.
v0 = Phase 0 export of the main corpus (1,000 docs, 400,519 postings, documents snapshot 6998397766212633409).

| # | Query | Expected top 3 in v0 | Expected top 3 in v1 | Changes in v1? |
|---|---|---|---|---|
| 1 | solar system | Solar System (800), Planet (639), Asteroid (51) | | |
| 2 | planet | Planet (639), Asteroid (51), Solar System (800) | | |
| 3 | computer | Computer science (110), Computer (112), Software (1987) | | |
| 4 | internet | Internet (362), Microsoft Windows (927), Internet slang (540) | | |
| 5 | river | River (673), Saint Lawrence River (702), Stream (799) | | |
| 6 | ocean | Indian Ocean (1978), Arctic Ocean (1980), Pacific Ocean (1975) | | |
| 7 | language | Language (419), Tone language (2012), First language (497) | | |
| 8 | human body | Human body (353), Anatomy (50), Skin (772) | | |
| 9 | chemistry | Chemistry (108), Microscope (512), Molecule (520) | | |
| 10 | religion | Religion (653), Supernatural (752), Being (85) | | |
| 11 | _Phase 1 topic (from Arnav)_ | | | yes |
| 12 | _Phase 1 topic (from Arnav)_ | | | yes |
| 13 | _Phase 1 topic (from Arnav)_ | | | yes |

Rows 11–13: topics that `changes.jsonl` updates, adds or deletes (ask Arnav), filled after the Phase 1 rehearsal.

Avoid in the demo (odd top 3 in v0): football, president, animal, mountain (Zebra #1), earthquake (months),
moon, dinosaur, painting, electricity, democracy, climate change, ancient egypt, world war.

Talking point: "ocean" top 3 all score ≈1.00 because those pages share the corpus-max PageRank
(0.00176, so the PageRank part is the full 0.3) and have near-equal BM25 (5.02 / 5.01 / 5.01);
the small BM25 differences set the order.
