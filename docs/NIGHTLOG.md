# Nightlog

Append-only record of the loop in [LOOP.md](LOOP.md). Read top to bottom: what needs a human
first, then the numbers, then the chronology.

## Review needed

(nothing yet)

## Numbers

(nothing yet)

## Chronology

2026-09-11T00:00Z | — | seeded | loop, backlog B-001..B-018 and this log created | next B-001
2026-09-11T02:05Z | — | recovered | untracked `weida-sample-transport.md` committed as-is (81d1df7); zeughaus-side design doc, docs-only, no gate | next B-001
2026-09-11T02:16Z | B-001 | done 56ead42 | decision 0004: `Stored(Written|Flushed)`, `Replicated(n, flushed)`, per-axis validation, adapter mapping table; SYNTHESIS §8.3 closed; decisions/README.md created | next B-003
2026-09-11T02:30Z | B-003 | done 052e741 | decision 0006: guarantee sets over the §3 dimensions, `core` as default, HELLO intersection with failure instead of downgrade, adapter edge ends at the foreign transfer point, degradation only as named configuration; SYNTHESIS §8.7 closed | next B-002/B-004/B-005 verification, else B-006
