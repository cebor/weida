# Nightlog

Append-only record of the loop in [LOOP.md](LOOP.md). Read top to bottom: what needs a human
first, then the numbers, then the chronology.

## Review needed

(nothing yet)

## Numbers

**B-009 — DATA header cost per message.**
`cargo bench -p weida --bench patterns -- header --warm-up-time 1 --measurement-time 3`
(two runs, release, loopback, AMD Ryzen 7 5800X). A 64-byte push is a **135 B** DATA frame
minimally and **215 B** with a `uint` sequence plus a `sha256:<64 hex>` producer name
(**+80 B**, +59.3 %); throughput **179.4 Kmsg/s** against **162.9 Kmsg/s**, i.e. **−9 %**,
against a run-to-run spread of ±3 % (p = 0.38-0.41). The producer key is the expensive half:
74 B as a hex string against 35 B as a 32-byte CBOR `bstr`. Recorded in IMPLEMENTATION.md §4.

**B-010 — reorder buffer under cross-stream reordering.**
`cargo test -p weida --test streams reverse_order_completion -- --nocapture` (debug,
loopback, five identical runs). N transfers opened, finished from last to first. Queued
back to back the reversal never reaches the receiver: N = 16 gives **0** out of dispatch
position and a **0**-deep buffer, N = 256 gives **90 of 256** and a peak of **84** (33 % of
the transfers in flight, first divergence at position 29) purely from concurrent dispatch.
Awaiting each FIN's transport receipt does reverse it: N = 16 gives **16 of 16** and the peak
**15 = N − 1**. So an eager reassembler's bound is N − 1 and it is reachable; the cap must be
a configured number. Recorded in IMPLEMENTATION.md §4.

## Chronology

2026-09-11T00:00Z | — | seeded | loop, backlog B-001..B-018 and this log created | next B-001
2026-09-11T02:05Z | — | recovered | untracked `weida-sample-transport.md` committed as-is (81d1df7); zeughaus-side design doc, docs-only, no gate | next B-001
2026-09-11T02:16Z | B-001 | done 56ead42 | decision 0004: `Stored(Written|Flushed)`, `Replicated(n, flushed)`, per-axis validation, adapter mapping table; SYNTHESIS §8.3 closed; decisions/README.md created | next B-003
2026-09-11T02:30Z | B-003 | done 052e741 | decision 0006: guarantee sets over the §3 dimensions, `core` as default, HELLO intersection with failure instead of downgrade, adapter edge ends at the foreign transfer point, degradation only as named configuration; SYNTHESIS §8.7 closed | next B-002/B-004/B-005 verification, else B-006
2026-09-11T02:58Z | B-009 | done 178a95c | header-cost bench in `benches/patterns.rs` (64-byte push, minimal vs two extra keys), full gate green, numbers above and in IMPLEMENTATION.md §4; fed 0008 §4.4's encoding choice | next B-010
2026-09-11T03:22Z | B-002 | done edabf00 | decision 0005 (parallel worker), verified against acceptance: race stays documented, refusal deterministic only beyond the peer's stream window or in Req/Rep, no L0 application ACK; SYNTHESIS §8.5 closed, README row added | next B-004
2026-09-11T03:23Z | B-004 | done c06c072 | decision 0007 (parallel worker), verified: paths stay opaque, topic filters `.`-segmented with `*` and trailing `#` (zero-or-more), full foreign mapping table; SYNTHESIS §8.9 closed | next B-005
2026-09-11T03:24Z | B-005 | done 7d3d31b | decision 0008 (parallel worker), verified: fingerprint is the peer across connections and binds control to bulk, no L0 session, resumption is L2, producer key absent by default else 32-byte `bstr` from B-009's numbers; 0001 §8 item closed | next B-010, B-007 and B-018 delegated
2026-09-11T03:40Z | B-010 | done a236cbb | reorder probe in `tests/streams.rs` with two FIN modes, full gate green (218 tests), numbers above and in IMPLEMENTATION.md §4 | next review pass (§8), then B-011
