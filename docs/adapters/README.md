# Adapter mapping documents

One document per foreign protocol, written from that protocol's research sheet in
[`../research/`](../research/README.md) and from the accepted decisions in
[`../decisions/`](../decisions/). A sheet describes a protocol on its own terms and never
mentions weida; a mapping document is the opposite — it is the place where the two
vocabularies are lined up, and where everything that does **not** line up is named.

A mapping document is written as Phase B slice 2 for its protocol, before the bridge code and
after the sans-I/O codec ([LOOP.md](../LOOP.md) §9). It is the artefact the adapter-honesty
invariant is checked against: "Protocol adapters may not silently invent guarantees their
source protocol cannot provide" ([INVARIANTS.md](../INVARIANTS.md)), and
[0006](../decisions/0006-guarantee-sets.md) §4.9 makes this document, not the code, the home
of that check.

## Required sections

Every mapping document answers all of these, in this order. A section that does not apply says
so in one line rather than being omitted.

```
# <Protocol> <version> — adapter mapping

## 1. Scope
Which directions (inbound, outbound), what the adapter terminates, what is out of scope.

## 2. <Protocol primitive> to weida pattern
A table: every socket/link/channel/queue kind of the foreign protocol, its weida counterpart,
whether the mapping is faithful, and what the adapter must do where it is not.

## 3. Stream mapping
How one foreign message becomes weida transfers and streams; how a foreign connection maps
onto the control and per-path bulk connections of 0002; framing, envelopes, sizes, liveness,
reconnect.

## 4. Credit and backpressure mapping
Unit on each side, who grants it, what happens at the bound, and which pairings preserve the
overload policy rather than converting one into the other.

## 5. Identity, security and authorization
Each foreign mechanism against weida Identity/Trust; what terminates at the adapter; the rule
that a self-asserted foreign identity is never presented as a weida peer identity (0008).

## 6. Topic or address mapping
The row group this protocol owns in 0007 §5, reproduced, plus what the adapter must do per row.

## 7. Transfer points and guarantee mapping
The transfer points of both sides lined up (SYNTHESIS §4), where the guarantee chain ends
(0006 §4.6), the guarantee set carried in each direction, ordering and duplicates.

## 8. Named losses
A numbered list. Each is something the adapter cannot carry, with the sourced reason.

## 9. Configurations the adapter refuses
Refusal is at configuration time and is the default at an edge (0006 §4.7); degradation exists
only as a named configuration entry.

## 10. Interop bench plan
The upstream implementation, what it can and cannot cover, golden vectors, fuzz target, the
inbound and outbound matrices, a test per observable named loss, and the numbers to record.

## 11. Open questions
What the sources do not settle, and which weida decision or measurement would settle it.

## 12. Sources
weida documents and decisions with section numbers; the research sheet with section numbers.
```

Two rules keep these documents honest:

1. **Every claim carries its source.** A weida claim names the document, section or decision;
   a foreign claim names the research sheet's section. A claim with no source does not belong
   here — it belongs in a research item that produces one.
2. **A loss is named, never absorbed.** Where the two protocols cannot be made to coincide,
   the document says which side loses what, what the adapter does instead, and whether the
   configuration is refused. This is the difference between a mapping and a wish.

## Documents

| File | Protocol | Sheet | Status |
| --- | --- | --- | --- |
| `zmtp.md` | ZeroMQ ZMTP 3.1 (libzmq 4.3.x, CURVE/ZAP) | [`zeromq.md`](../research/zeromq.md) | codec, both bridge directions and the interop run against the pure-Rust `zeromq` crate |
| `nng.md` | nanomsg / NNG Scalability Protocols (SP v1 RFCs rev 01, NNG 1.10.0) | [`nanomsg-nng.md`](../research/nanomsg-nng.md) | mapping written; codec built (`weida-sp`), bridges next |
| `mqtt5.md` | MQTT 5.0 (OASIS Standard 2019-03-07; 3.1.1 differences) | [`mqtt5.md`](../research/mqtt5.md) | mapping written for the **client** ([0014](../decisions/0014-parallel-libraries.md) §2); the broker half of each rule is marked deferred to Phase D, and no forwarder exists yet |

Planned, in the order the parallel workstreams build them
([0014](../decisions/0014-parallel-libraries.md) §2): AMQP 1.0 and NATS core. Each gets its
document in the same shape before its bridge code exists. MQTT is the first entry whose
document precedes a *library* rather than a bridge, because MQTT's server side is a broker and
the broker is Phase D ([LOOP.md](../LOOP.md) §9).
