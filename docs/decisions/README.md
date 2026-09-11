# Decision notes

One note per decision the evidence in [../research/](../research/) left open. A note states the
question, the condensed evidence with references, the options with their named losses, the
decision, and the follow-ups it creates. `Status: provisional` means it was taken without a
human and is reversible (a config default, a feature flag, or docs-only); `accepted` means it is
normative for the documents it names.

The catalogue the questions come from is [SYNTHESIS.md](../research/SYNTHESIS.md) §8; a closed
question carries a `Closed by [NNNN]` paragraph there.

| id | title | status | summary |
| --- | --- | --- | --- |
| [0001](0001-sequence-field.md) | Sequence field — scope and layer | accepted | Two new L0 DATA keys: a per-producer monotone sequence for ordering and gap detection, and a separate producer identity for bounded deduplication; `PerKey` stays L2. |
| [0002](0002-control-and-bulk-separation.md) | Control and bulk traffic — separation against head-of-line coupling | accepted | Control traffic gets its own connection per peer and bulk traffic a connection per path, so a stalled reader cannot withhold a control frame; no multiplexed control stream. |
| [0003](0003-credit-unit.md) | Flow-control credit — unit and layer | accepted | L0 carries no application credit — QUIC's windows are the byte credit, the stream budget the message credit; L2 gets an absolute per-subscription message credit on the control connection. |
| [0004](0004-durability-levels.md) | Durability levels for `Stored` and `Replicated` | accepted | `Stored(Written\|Flushed)` names the failure domain it survives and `Replicated(n, flushed)` counts replicas that reached at least `Written`, leader included; the two axes are a partial order and `n` is achieved, not configured. |
