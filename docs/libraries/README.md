# Library parity documents

One document per foreign protocol this repository implements as a **standalone library** —
a crate usable with no weida in the picture ([0013](../decisions/0013-competitor-libraries.md)
§4, §5.5). The document is the parity table: it states, row by row, what the library does with
every inventory item of the protocol's reference implementation.

## A library document is not an adapter document

The two are easy to confuse and answer different questions, so the distinction is the first
thing this index says.

| | `docs/adapters/<proto>.md` | `docs/libraries/<proto>.md` |
| --- | --- | --- |
| Subject | a **bridge** between weida and a foreign network | a **library** that speaks the foreign protocol and nothing else |
| Question it answers | what does a weida guarantee become on the other side, and what is lost | does this implementation have what the reference implementation has |
| Vocabulary | both, lined up — the document exists to line them up | the foreign protocol's only; weida is not mentioned except where a weida crate replaces a foreign construct |
| Invariant it carries | adapter honesty: "protocol adapters may not silently invent guarantees their source protocol cannot provide" ([INVARIANTS.md](../INVARIANTS.md), [0006](../decisions/0006-guarantee-sets.md) §4.9) | parity honesty: every inventory row is present, refused with a reason, or absent with a reason — and **no row says "partial" without naming what is missing** (0013 §4.7 clause 6) |
| Named loss | what the guarantee chain cannot carry across the edge | nothing at all: a library terminates no weida guarantee, so it has no edge and no loss list |
| When it is written | Phase B slice 2, before the bridge code ([LOOP.md](../LOOP.md) §9) | last, after the library's slices are done, because it is the record of what they came to |

A protocol may have both, and ZeroMQ does: [`zmq.md`](zmq.md) is the parity table for the
`weida-zmq` library, and [`../adapters/zmtp.md`](../adapters/zmtp.md) maps the bridge that
sits between a weida endpoint and a ZeroMQ socket. Neither replaces the other, and neither
is the protocol's research sheet — a sheet in [`../research/`](../research/README.md)
describes a protocol on its own terms and cites its specifications; a parity document cites
the sheet and the code.

## Required sections

Every parity document answers all of these, in this order. A section that does not apply says
so in one line rather than being omitted.

```
# <Protocol> (<reference implementation and version>) — feature parity

## 1. What a row means
The three verdicts, the rule against "partial", and the versions every measured claim was
measured against.

## 2. <Primitive> types
Every socket/link/channel kind of the reference implementation's own table, with the rows that
table has, and where each one is implemented or why it is absent.

## 3. Transports
Every transport the sheet inventories, present or absent-with-reason.

## 4. Mechanisms and authentication
Every security mechanism, and the authentication dialog if the protocol has one.

## 5. Options
Every configuration option of the reference implementation, honoured under the name this
library gives it or refused with the reason.

## 6. Observability
The event set, counters or tracing surface, and what a row's value carries where it differs.

## 7. Devices and helpers
The intermediaries the reference implementation ships as API.

## 8. Interop evidence
Which implementations were run against this one, in which roles, how many pairings, and what
disagreed — each disagreement measured rather than inferred.

## 9. Deliberate deviations and bounds the reference implementation lacks
Where this library is on purpose not identical, and every ceiling it adds.

## 10. The definition of done
The clauses this library was built against, each with its verdict and the section that proves
it.

## 11. Sources
The research sheet with section numbers, the decisions, and the code paths the table's
verdicts were read from.
```

Two rules keep these documents honest:

1. **Every row carries its evidence.** A "present" row names the module or the test; a
   "refused" row names the reason the code gives at configuration time; an "absent" row names
   what is missing. A row whose verdict cannot be checked from the repository does not belong
   here.
2. **No aggregate verdicts.** "Mostly complete", "partial support" and "planned" are not
   verdicts. Either the row is present, or the document says what a caller who wants it does
   not get.

## Documents

| File | Library crates | Reference implementation | Sheet | Status |
| --- | --- | --- | --- | --- |
| [`zmq.md`](zmq.md) | `weida-zmtp` (codec), `weida-zmq` (implementation) | libzmq 4.3.5, plus the pure-Rust `zeromq` 0.6.0 | [`zeromq.md`](../research/zeromq.md) | complete against 0013 §4.7's six clauses |
| [`zmq-py.md`](zmq-py.md) | `weida-py-core` (the shared PyO3 foundation), `weida-zmq-py` (the binding) | pyzmq 27.2.0 over libzmq 4.3.5 | [`zeromq.md`](../research/zeromq.md) | covers: all eleven socket types, the three transports, the 98-row option table, PLAIN/CURVE/ZAP with a handler in Python, the monitor, the devices, and a synchronous surface beside the asyncio one. Does not cover: `zmq.STREAM` and the DRAFT socket types, `zmq.Poller`/`sock.fd`, `send_string`/`send_json`/`Frame`, pyzmq's `zmq.auth` policy framework, and **concurrent send and recv on one socket** — see §9.1, which is the library's `&mut self` and is filed against `weida-zmq` |

Planned: `nng.md`, when `weida-nng` exists beside the `weida-sp` codec (0013 §4.1). It gets
its document in this shape, and the SP research sheet
([`nanomsg-nng.md`](../research/nanomsg-nng.md)) is what it cites.
