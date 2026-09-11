# Nightlog

Append-only record of the loop in [LOOP.md](LOOP.md). Read top to bottom: what needs a human
first, then the numbers, then the chronology.

## Review needed

- **A stray document at the repository root.** `weida-sample-transport.md` was left untracked
  by a previous session and committed as-is (81d1df7, `chore: recover uncommitted work`). It
  is a *zeughaus*-side design note about carrying video frames over weida, with links pointing
  outside this repository. It is useful input (it names five things weida would need, led by
  streaming fan-out) but it does not belong at the root of this tree. Decide: move it under
  `docs/` as an external-requirements note, or drop it and take its five requests into the
  backlog.
- **No provisional decisions.** 0004-0008 are all `accepted`; nothing was decided on a
  reversible flag that a human still has to confirm.
- **Nothing parked, nothing blocked, no stashes.**

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

**B-011 — what a connection to a peer costs.**
`cargo bench -p weida --bench connections -- --warm-up-time 1 --measurement-time 3` (release,
loopback, pinned trust, mTLS off, three runs). A cold handshake is **1.04-1.10 ms**; dialling
a peer the pool already holds is **3.8 µs**, about **280x** cheaper, because it is not a
connection at all. 64 handshakes in series take 65.6-73.9 ms with no degradation. Memory:
**0-4 KiB** per idle runtime, **488-596 KiB** per live connection at 2 and **750-850 KiB** at
64, covering both ends in one process. So 0002's second connection per peer costs about a
millisecond and under a megabyte for the pair, against the unbounded head-of-line coupling it
removes. Recorded in IMPLEMENTATION.md §4.

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
2026-09-11T03:50Z | review | 4e0c899 | 4 findings, all filed: B-019 FAILURE_MODEL per 0005, B-020 segment matcher in code, B-021 its cost in the fan-out path, B-022 capped reassembly + drop detection; B-017's acceptance extended with 0008 §4.2's fingerprint binding. Gate green on the tree; no public API changed since the anchor (bench and test only); no stale identifier in the normative docs — the four that no longer exist in code (`transfer_id`, `role`, `correlation_id`, `ack_mode`, plus `max_pending`) are PROTOCOL's own "these are gone" statements; no new remote-influenced allocation | next B-011
2026-09-11T04:06Z | B-018 | done fecd996 | ZMTP mapping document (parallel worker), verified: socket table, stream mapping, HWM against the two L0 credit units, CURVE/ZAP against Identity/Trust, transfer points, ten named losses, six refused configurations, interop bench plan on zmq.rs; `docs/adapters/README.md` added as the template and index | next B-007
2026-09-11T04:07Z | B-007 | done 30b8942 | GUARANTEES.md synced (parallel worker), verified: §1 durability levels per 0004, §3 `PerProducer(detect|reassemble)`, `Bounded(window)`, `PerKey` L2-only, guarantee-set subsection per 0006, receipt paragraph citing RFC 9000 §3.1/§3.2/§4.1 per 0005, §6 backpressure row naming bytes and streams as the two credit units; stale-term grep clean, the race is stated as decided rather than open | next B-011, then B-006; B-016 delegated on branch `b016-runtime`
2026-09-11T04:20Z | B-011 | done 417a359 | new bench target `benches/connections.rs`: cold handshake 1.04-1.10 ms, pooled dial 3.8 µs, 0-4 KiB per idle runtime, 750-850 KiB per live connection at 64; full gate green; numbers above and in IMPLEMENTATION.md §4 | next B-006
2026-09-11T04:45Z | B-006 | done 191043c | PROTOCOL.md on the decided state: DATA keys 6/7 (sequence uint, producer 32-byte bstr, optional and skipped by v0), HELLO keys 5/6 with the new §6.5 guarantee-set encoding and the per-dimension intersection in §2.3, §2.5 control-plus-bulk connections bound by fingerprint, §6.4 segmented filter grammar per 0007, §10.1 control and bulk limit profiles with `max_connections_per_peer`, kind 5 reserved for the L2 credit frame, §9.2 on the refusal race; docs-only, no gate | next B-019 is delegated, so B-008 — but it needs B-006 and B-007 only, both done
2026-09-11T05:05Z | B-008 | done 64ba40b | PATTERNS §1.3 narrowed to bulk writers on one path's connection, §1.4 states the stream budget is the L0 message credit, §1.6 cites 0005 including "a late refusal reaches no observer", §1.7 carries B-010's reorder cost, §4 gains the segmented grammar and subscriber-side drop detection, §7 gains the "must know whether it was accepted" row; INVARIANTS permits the control connection, names three unimplemented bounds and makes the mapping document the home of adapter honesty; ARCHITECTURE §5 describes both pool tiers, the fingerprint binding and the two Limits profiles with B-011's numbers; SYNTHESIS §8 closes 8.1/8.2/8.4 and states that only 8.6 and 8.8 remain open; docs-only, no gate | next merge B-016 from `b016-runtime`
