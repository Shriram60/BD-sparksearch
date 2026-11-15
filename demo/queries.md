# Prepared demo queries

At least 3 queries must change between v0 and v1. Top results shown as `Title (doc_id)`.
- v0 = Phase 0 export of the main corpus: 1,000 docs, 400,519 postings, documents snapshot 6998397766212633409.
- v1 = Phase 1 rehearsal export: 975 docs, 377,989 postings (25 updated, 25 added, 50 deleted).

Side by side in the UI: query on the live v1, then "Compare with" → v0.

## Phase 1 queries (run these live after the switch to v1)

| # | Query | Shows | v0 top 3 | v1 top 3 |
|---|---|---|---|---|
| 1 | added during project | New pages | MediaWiki (495), Beer (2020), GNU Free Documentation License (302) | MediaWiki (495), Phase 1 Added Page 1 (phase1-new-001), Phase 1 Added Page 2 (phase1-new-002) |
| 2 | plague | Update in place | Bubonic plague (100), London (460), Idiom (371) | Bubonic plague (Phase 1) (100), Flea (3523), London (460) |
| 3 | alan turing | Deleted page | Alan Turing (13), List of mathematicians (3433), Computer science (110) | List of mathematicians (3433), 20th century (3407), First language (497) |
| 4 | breakfast sausage | Delete + update | Breakfast sausage (74), Sausage (710), Chorizo (133) | Sausage (710), Chorizo (Phase 1) (133), Salami (708) |

Order for the demo: #1 first (those pages don't exist in v0 at all), then #2 (same doc_id 100 with the new
title and no duplicate: the update replaced the document), then #3 and #4.

Backups:
| Query | v0 top 3 | v1 top 3 |
|---|---|---|
| apple | Apple (39), Apple Macintosh (517), List of fruits (454) | Apple Macintosh (517), E Prime (227), List of fruits (454) |
| atom | Atom (47), Chemical element (247), Chemistry (108) | Chemical element (247), Isotope (3489), Ion (2099) |
| beard | Mustache (500), Beard (94), Goatee (318) | Mustache (500), Goatee (318), Hair (359); Beard (Phase 1) (94) is #4 |

## Stable queries (sanity checks)

| # | Query | v0 top 3 | v1 top 3 | Changed? |
|---|---|---|---|---|
| 5 | internet | Internet (362), Microsoft Windows (927), Internet slang (540) | Internet (362), Microsoft Windows (927), Internet slang (540) | no |
| 6 | river | River (673), Saint Lawrence River (702), Stream (799) | River (673), Saint Lawrence River (702), Stream (799) | no (China (120) drops out of #4) |
| 7 | ocean | Indian Ocean (1978), Arctic Ocean (1980), Pacific Ocean (1975) | Indian Ocean (1978), Arctic Ocean (1980), Pacific Ocean (1975) | no |
| 8 | human body | Human body (353), Anatomy (50), Skin (772) | Human body (353), Death (209), Skin (772) | Anatomy (50) deleted |

## Changed for PageRank reasons only (don't use in the demo until the PageRank issue below is settled)

| Query | v0 top 3 | v1 top 3 |
|---|---|---|
| solar system | Solar System (800), Planet (639), Asteroid (51) | Comet (168), Solar System (800), Planet (639) |
| planet | Planet (639), Asteroid (51), Solar System (800) | Crust (166), Planet (639), Solar System (800) |
| language | Language (419), Tone language (2012), First language (497) | Dutton's Speedwords (191), Capitalization (176), Language (419) |
| religion | Religion (653), Supernatural (752), Being (85) | Christian (170), Religion (653), Cuba (178) |

## Open issue: PageRank shift in v1 (Node 1 / Node 3, found from the exports on node4)

- Every one of the 25 updated pages lost 71–84% of its PageRank (v1/v0 ratio 0.16–0.29), e.g. Chemistry (108)
  0.00176 → 0.00028, Beard (94) 0.00176 → 0.00028. Their update only appends one sentence to the text.
- Unchanged pages: median ratio 1.06, but some C-range pages roughly doubled (Cube 180, Capitalization 176,
  Christian 170, Cuba 178, Comet 168, Crust 166), which pushes them to #1 on unrelated queries.
- Looks like links pointing TO updated pages were dropped by the Phase 1 links MERGE. Check on the cluster:
  ```sql
  SELECT count(*) FROM lake.search.links VERSION AS OF <phase0 links snapshot> WHERE dst_id = '108';
  SELECT count(*) FROM lake.search.links WHERE dst_id = '108';
  ```
  If the second count is ~0, the MERGE is removing incoming links of changed docs.
- Also: in v0, 480 of 1,000 docs share exactly the max PageRank (only 21 distinct values), so v0 PageRank is close
  to flat. Worth a look at the links table for Phase 0 too.

## Avoid

- computer, china: the updated pages Computer (112) and China (120) fall out of the top 5 in v1 (same PageRank issue).
- chemistry: Chemistry (Phase 1) (108) drops from #1 to #7 in v1. It still has the highest BM25 (5.71), but its
  PageRank fell 0.00176 → 0.00028 while the pages above it kept ~0.0019. The corpus max rising (0.00176 → 0.00399)
  is only part of it: the max rose 2.3×, Chemistry's own PageRank fell 6.3×.
- Odd top 3 in v0: football, president, animal, mountain (Zebra #1), earthquake (months), moon, dinosaur,
  painting, electricity, democracy, climate change, ancient egypt, world war.
- "phase 1" by title: search only indexes page text, not titles.

## Talking point

"ocean" top 3 all score ≈1.00 in v0 because those pages share the corpus-max PageRank (0.00176, so the PageRank
part is the full 0.3) and have near-equal BM25 (5.02 / 5.01 / 5.01); the small BM25 differences set the order.
