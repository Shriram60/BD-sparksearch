# Prepared demo queries

At least 3 queries must change between v0 and v1. Top results shown as `Title (doc_id)`.
v0 = Phase 0 export of the main corpus (1,000 docs, 400,519 postings, documents snapshot 6998397766212633409).
v1 column for rows 1–4 is what Arnav saw in his local Phase 1 run; confirm at the rehearsal.

## Phase 1 queries (run these live after the switch to v1)

| # | Query | Shows | v0 top 3 | Expected v1 |
|---|---|---|---|---|
| 1 | added during project | New pages | MediaWiki (495), Beer (2020), GNU Free Documentation License (302) | MediaWiki, then Phase 1 Added Page 1, 2, 3, 4 |
| 2 | plague | Update in place | Bubonic plague (100), London (460), Idiom (371) | #1 Bubonic plague (Phase 1) (100): same doc_id, new title, no duplicate |
| 3 | alan turing | Deleted page | Alan Turing (13), List of mathematicians (3433), Computer science (110) | Alan Turing gone; #1 List of mathematicians (3433) |
| 4 | breakfast sausage | Delete + update | Breakfast sausage (74), Sausage (710), Chorizo (133) | Breakfast sausage gone; #1 Sausage (710), #2 Chorizo (Phase 1) (133) |

Order for the demo: #1 first (those pages don't exist in v0 at all), then #2 (cleanest proof an update
replaces the document instead of adding a duplicate), then #3 and #4.

Backups: `apple` (Apple (39) #1 in v0, deleted in v1), `atom` (Atom (47) #1 in v0, deleted in v1),
`beard` (Beard (94) #2 in v0 → "Beard (Phase 1)" drops to #4 in v1).

## Stable queries (sanity checks, v0 top 3)

| # | Query | v0 top 3 | v1 top 3 | Changed? |
|---|---|---|---|---|
| 5 | solar system | Solar System (800), Planet (639), Asteroid (51) | | |
| 6 | planet | Planet (639), Asteroid (51), Solar System (800) | | |
| 7 | internet | Internet (362), Microsoft Windows (927), Internet slang (540) | | |
| 8 | river | River (673), Saint Lawrence River (702), Stream (799) | | |
| 9 | ocean | Indian Ocean (1978), Arctic Ocean (1980), Pacific Ocean (1975) | | |
| 10 | language | Language (419), Tone language (2012), First language (497) | | |
| 11 | human body | Human body (353), Anatomy (50), Skin (772) | | |
| 12 | chemistry | Chemistry (108), Microscope (512), Molecule (520) | | expected: Chemistry (Phase 1) |
| 13 | religion | Religion (653), Supernatural (752), Being (85) | | |

## Avoid

- computer, china: the updated pages "Computer" (112) and "China" (120) fall out of the top 5 in v1. Phase 1's
  max PageRank rises from 0.00176 to 0.00399, so each page's PageRank share (pr / max_pagerank) shrinks. Correct
  behaviour, but confusing to explain live.
- Odd top 3 in v0: football, president, animal, mountain (Zebra #1), earthquake (months), moon, dinosaur,
  painting, electricity, democracy, climate change, ancient egypt, world war.
- "phase 1" by title: search only indexes page text, not titles.

## Talking point

"ocean" top 3 all score ≈1.00 because those pages share the corpus-max PageRank (0.00176, so the PageRank
part is the full 0.3) and have near-equal BM25 (5.02 / 5.01 / 5.01); the small BM25 differences set the order.
