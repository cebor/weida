# The weida guide

This is the document that teaches. The others specify: [PROTOCOL.md](PROTOCOL.md) is the wire,
[PATTERNS.md](PATTERNS.md) is the reference for what each pattern does, [GUARANTEES.md](GUARANTEES.md)
is what a claim about a message is allowed to mean, [FAILURE_MODEL.md](FAILURE_MODEL.md) is what
happens when something breaks. None of them tells you how to **build** something, and that is
what a reader needs first.

The model is ZeroMQ's guide. Not its API documentation — its *guide*, which teaches ways of
constructing networked software and happens to use one library to do it. Whatever else is true
of ZeroMQ, that document is the reason a generation of people could build message-passing
systems without a distributed-systems course, and it earned that by being concrete: every
pattern is a program you can run, every claim is small enough to check, and the failure cases
are in the same chapter as the success case rather than in an appendix.

**Two rules keep this document honest, and they are enforced rather than promised.**

1. **Every program here is a file in this repository, and a test drives that file.** The
   chapters do not carry snippets written for the page. Chapter 1's program is
   `crates/weida/examples/guide_one_transfer.rs`, chapter 2's is
   `crates/weida/examples/guide_many_peers.rs`, and chapter 4's is
   `crates/interop/cross-tests/examples/guide_depth.rs` — in that crate because its chain needs
   two foreign protocols, and the two adapters know nothing of each other on purpose. Each is
   included as a module by a test beside it, which asserts the claims its chapter makes. A
   claim added to this document without a program and an assertion fails
   `every_claim_the_chapters_make_has_a_program_and_a_test`.
2. **No number here is an estimate.** Measured figures name the item that measured them
   ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4); arithmetic derived from them says so in the
   sentence. Where nothing has been measured, the guide says *that* instead of guessing — and
   §0.4 names the gaps.

Run the chapters before reading them:

```text
cargo run -p weida --example guide_one_transfer
cargo run -p weida --example guide_many_peers          # --release for the timing figures
cargo run -p weida-cross-tests --example guide_depth
cargo test -p weida --test guide
cargo test -p weida-cross-tests --test guide_depth
```

---

## 0. The question: C8B

nginx came out of a question with a number in it. C10K asked how one machine serves ten
thousand concurrent connections, and the answer — event loops instead of a thread per
connection — reshaped server software, because the question named a *structure* problem in the
shape of a quantity.

weida's question is **C8B: how do you scale a software system to eight billion people?**

It is utopian on purpose, and the road is the goal. Nothing here will serve eight billion
anybody. What the number is for is the same thing C10K's was: it makes structural questions
answerable. A design either has an answer for what happens at eight billion recipients or it
does not, and the arithmetic tells you which, in an afternoon, without building anything.

### 0.1 The arithmetic

Take the simplest possible C8B workload: **one 64-byte message to every person, once.**

The measured pieces, all from this repository's own runs on one desktop machine
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4):

| Measured | Value | Item |
| --- | --- | --- |
| Message rate, 64-byte payload, one producer | **179.4 Kmsg/s** | B-009 |
| Bytes on the wire for that message | **135 B** frame for 64 B of payload | B-009 |
| Resident memory per live connection, both ends together | **750-850 KiB** | B-011 |
| Cold handshake | **1.04-1.10 ms**; a pooled dial 3.8 µs | B-011 |
| Publish cost at 256 subscribers | **22.94 µs** per message → **88.8 ns per subscriber** | B-247 |
| Fan-out delivery rate at 256 subscribers | **276-279 Kcopies/s** | B-247 |
| Transport state per subscriber, both ends | **396-436 KiB** | B-247 |
| Latency to a subscriber at width 256, idle | **507-539 µs** median, **816-989 µs** p99 | B-247 |
| Topic filter match | **14 ns**, shape-independent | B-021 |

Everything below is **arithmetic on those numbers**, and says so where it is.

**One sender cannot do it.** 8·10⁹ ÷ 179,400 ≈ **12.4 hours** for one producer to hand out one
message each. So the shape has to be a tree, and the only question is how wide and how deep.

**How wide can one node be?** Not a memory question, which is the first thing the measurement
corrected. At **396-436 KiB** of transport state per subscriber, 64 GiB holds on the order of
**1.5·10⁵** of them, and the publisher's CPU is no obstacle either: 88.8 ns per subscriber is
**0.9 ms** of work to hand one message to 10⁴ subscribers. What binds is the **delivery rate**,
276-279 Kcopies/s measured at width 256:

| Width *d* | One message to all of them | Messages per second that width sustains |
| --- | --- | --- |
| 10³ | ~3.6 ms | ~276 |
| 10⁴ | ~36 ms | ~27 |
| 10⁵ | ~360 ms | ~2.8 |

So the width is a **choice against a message rate** rather than a ceiling: a node serves 10³
subscribers at a few hundred messages a second, or 10⁴ at tens. Call the width *d* ∈ [10³, 10⁴]
— the same interval the first version of this section derived from memory, for an entirely
different reason.

**Then the depth is tiny.** log(8·10⁹) ÷ log(10³) ≈ 3.3, and ÷ log(10⁴) ≈ 2.5. So

> **two to four hops reach everybody.**

At *d* = 10⁴ that is **8·10⁵ leaf nodes**, 80 above them and one root; the leaf hop spends
36 ms of its delivery capacity on the message and the whole path is **~0.1 s** of fan-out work.

That single line is why this project's design decisions look the way they do. The scaling
problem at C8B is not throughput — 8·10⁹ ÷ 179,400 ÷ 10⁶ nodes is microseconds of work per
node — and it is not depth. It is **what a hop is allowed to claim**, because there are only
three or four of them between a publisher and a person, and every one of them is a place where
responsibility either transfers or is faked.

### 0.2 What the arithmetic decides

Four design decisions in this repository are what they are because of the numbers above, and
each one would be defensible without C8B and is *forced* with it.

**No end-to-end acknowledgement.** At *d* = 10³ and four hops, an acknowledgement per recipient
means 8·10⁹ acknowledgements converging back on one publisher: 8·10⁹ × 135 B ≈ **1.1 TB of
acknowledgement traffic for one message**, all of it funnelling into the root. The tree that
made the fan-out affordable makes the acknowledgement impossible. So a completion is **hop-local
by construction** ([GUARANTEES.md](GUARANTEES.md) §1): each hop says what it took
responsibility for, to the hop that handed it over, and nobody claims anything about the far
end.

**A completion is a cursor, not a verdict.** At depth, "did it arrive" has no answer that is
both cheap and true. "How far did this hop get" does: an absolute byte offset, which is
idempotent, coalesces for free, and stays meaningful when a transfer is interrupted
([decisions/0023](decisions/0023-completion-is-a-cursor.md)). Chapter 1 §1.4 is that mechanism
in twenty lines.

**Nothing materializes a payload.** A hop that must hold a message before forwarding it turns
every intermediary into a memory ceiling: three hops × 8·10⁹ recipients is a lot of copies of
anything. weida's transfers are streams end to end, and the measured consequence is the one
that matters — a 4 GiB payload crosses with **14.4 MiB of peak resident memory**
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4). Chapter 1 §1.2 is that property at 64 MiB.

**Fan-out drops and counts; it never blocks.** One slow subscriber out of 10⁴ must not stall the
other 9,999, so a publisher's answer to overload is a counted drop rather than backpressure
([PATTERNS.md](PATTERNS.md) §4) — and the count is per topic and per cause, because "something
was dropped" is not an operational answer. What a stalled subscriber costs the publisher is
measured on both sides of the crossover the defaults put at a **32 KiB** message: below it the
queue refuses first (0.34-1.19 MiB held per stalled subscriber at 1 KiB), above it the byte
budget does (9.3-10.7 MiB at 64 KiB), and the counter says which
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4, B-247). Chapter 2 §2.3 is that experiment as a
program.

### 0.3 What the arithmetic says about the bytes

One more line of the same arithmetic, because it changes how a frame reads.

8·10⁹ × 135 B ≈ **1.08 TB** for one 64-byte message to everybody, at the leaf edge alone. Of
those 135 bytes, 64 are payload; the largest single item in the rest is a **55-byte W3C
`traceparent`** that the runtime writes on every DATA header whether anything traces or not.
That is ≈ **470 GB per message-to-everyone** spent on a trace context nobody asked for — about
44 % of the total — against ZMTP's one to nine bytes of framing per message and SP's eight.

At C10K scale that is a micro-optimisation. At C8B it is the third-largest line in the budget,
which is why it is filed as a decision rather than a cleanup (B-246), and why the guide states
it here rather than leaving a reader to discover it with a packet capture.

### 0.4 What is not answered

Honesty is part of the method, so the gaps are named where the arithmetic needs them:

- **The width is measured to 256 subscribers, and *d* is 10³ to 10⁴.** B-247 replaced the
  derivation this section used to carry, but it replaced it with a measurement one order of
  magnitude below the number the depth argument uses: 256 subscribers on one desktop, both ends
  in one process, over loopback. The per-subscriber cost is flat from 16 to 256 — 396-436 KiB
  and a falling CPU cost per subscriber — which is the shape that extrapolates, and an
  extrapolation is what it remains. What a real 10⁴-subscriber node does with **one** network
  interface, across hosts, is not known here.
- **Nothing is measured across machines at all.** Every number in the table above is loopback
  with both ends in one process, which is the right way to isolate this library's own cost and
  the wrong way to learn what a deployment does: it has no propagation delay, no NIC, no packet
  loss, and a congestion controller that never sees a real bottleneck.
- **Federation across organisations is unbuilt.** Two to four hops inside one operator's
  network is the easy reading of §0.1. Eight billion people are not one operator's network, and
  what a hop between two *administrations* may claim is a question [0006](decisions/0006-guarantee-sets.md)
  answers in the protocol (a guarantee set per hop, intersected, never downgraded silently) and
  nothing yet answers in a deployment.
- **The cluster and the store are specified and not finished.** [STORE.md](STORE.md) and
  [0020](decisions/0020-cluster-and-discovery.md)-[0022](decisions/0022-consensus-topology.md)
  are the plan; `weida-broker` today has queues, a publisher confirm and credit, and no
  durability. A chapter about a hop that survives a restart cannot be written yet, and this
  guide will not pretend otherwise.

---

## 1. One transfer, and what you may say about it

The program: `examples/guide_one_transfer.rs`. The test: `crates/weida/tests/guide.rs`. Run
both before reading on — the output of the first is the shape of this chapter.

If you come from ZeroMQ, one sentence orients everything below: **a weida transfer is a stream,
not a message.** A socket in ZeroMQ takes a message you already have; an endpoint in weida
hands you a stream you write into. Everything in this chapter follows from that one
difference, including the parts that look like they are about errors.

### 1.1 Hello

**Claim §1.1: an address and a trust decision are the whole setup.** No broker, no registry, no
certificate authority, no configuration file.

```rust,ignore
let identity = Identity::generate()?;                      // a key pair, in memory
let fingerprint = identity.fingerprint()?;
let binding = listener.bind_quic("127.0.0.1:0".parse()?, identity).await?;
let url = format!("weida://{fingerprint}@127.0.0.1:{}/hello", binding.local_addr().port());

let replier = listener.replier("/hello")?;                 // the serving side
let requester = client.requester(Trust::by_address());     // the dialling side
requester.connect(&url).await?;
let reply = requester.request(b"world").await?.collect(1024).await?;
```

Two things in that URL are worth more than they look.

**The peer is its public key.** `weida://sha256:…@host:port/path` names a key, and
`Trust::by_address` means *accept exactly the peer this URL names and nobody else*. There is no
name to resolve into an authority, no chain to validate, nothing to expire. An address is a
complete trust statement, which is why this chapter needs no setup section
([ARCHITECTURE.md](ARCHITECTURE.md) §2).

**The path is opaque and it is where dispatch happens.** `/hello` is not a topic, not a queue
name and not a hierarchy weida parses. It is the key a replier registered under, and the only
thing the acceptor matches on ([decisions/0007](decisions/0007-topic-namespace.md)).

And one thing that is *not* in the program: `accept` hands the replier a request whose payload
has not been read yet, and the reply half exists before the request has finished arriving. A
replier can answer a 4 GiB request without holding it — which is §1.2, and is the reason the
API looks like this rather than like `fn handle(request: Vec<u8>) -> Vec<u8>`.

### 1.2 The same four calls carry a gigabyte

**Claim §1.2: the payload never has to exist anywhere, and a cap is a refusal rather than a
bigger buffer.**

The program sends 64 MiB with the same `open`/`write_all`/`finish` that sent five bytes in
§1.1, and the reader folds it through one 64 KiB buffer:

```rust,ignore
let mut buffer = vec![0u8; CHUNK];          // the only buffer in the path
loop {
    let read = transfer.read(&mut buffer).await?;
    if read == 0 { break; }
    checksum = fold(checksum, &buffer[..read]);
}
```

There is no `collect` in that loop, and that is the point. `collect(max_bytes)` exists and is
the right call for a small payload, but it is **opt-in and capped**: you state what you are
willing to hold, and a payload that exceeds it is refused rather than admitted. The program
proves both halves — the streamed transfer arrives whole, and the same payload offered to
`collect(1 MiB)` is refused.

**The refusal reaches the sender.** This is the part that surprises people: the reader's
`LimitExceeded` becomes a `STOP_SENDING` on the wire, so the sender's `write_all` fails
mid-payload with `Error::Rejected` rather than succeeding into a void. A cap is a conversation,
not a silent truncation.

For the gigabyte itself, this repository has the measurement rather than a claim:
`examples/large_stream.rs` moves 4 GiB between two processes at 908.7 MiB/s with **14.4 MiB of
peak resident memory**, and `tests/large.rs` asserts a 512 MiB ceiling on a 1 GiB echo
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4). §0.2 is why that number is load-bearing rather
than impressive.

### 1.3 The outcome is a value

**Claim §1.3: a send's outcome is a value with three cases — and which case you can be sure of
is decided by the pattern, not by the error type.**

The three cases, and they are not interchangeable:

| Outcome | What it means | What an application may do |
| --- | --- | --- |
| `Ok(())` from `delivered()` | the peer's **transport** holds every byte and the FIN | nothing more; it is not a claim the peer's application read them ([GUARANTEES.md](GUARANTEES.md) §3) |
| an error with `is_definite_failure()` | it definitely did not happen | retry without worrying about duplication |
| `Error::Indeterminate` | it may or may not have happened | decide — and the decision needs idempotency or a human ([FAILURE_MODEL.md](FAILURE_MODEL.md) §5) |

`Indeterminate` is the one people delete when they write their own framework, and it is the one
that matters at depth: a connection that dies after the FIN went out and before the
acknowledgement came back leaves exactly that state, and calling it a failure is a lie that
costs duplicate work.

**Now the part the program is really for.** Run it three times and watch the second line
change:

```text
push, served path:   Delivered
push, unserved path: Delivered      <- and, on another run, Refused("peer has no such endpoint")
request, same path:  Refused("peer has no such endpoint")   <- every time
```

A one-way transfer to a path **nobody serves** may be reported as delivered. That is not a bug
and it is not a race in the implementation: the payload fit in the peer's stream window, so
QUIC acknowledged the FIN before the dispatcher's refusal travelled back, and the receipt
answered with what it knew. [decisions/0005](decisions/0005-refusal-race.md) is the decision
that keeps it that way rather than inventing a handshake to hide it, and it states the rule:

> a refusal is guaranteed only beyond the peer's stream window, or in Req/Rep.

The remedy is in the same three lines of output. An exchange has a reply half, and a reply half
cannot be acknowledged into existence — so `request` to an unserved path is `UnknownEndpoint`,
definitely, every time. **If you must know whether the far side accepted it, ask a question
instead of making a statement.** That is the first genuinely architectural choice in this guide,
and it costs a round trip, which is §1.5.

### 1.4 How far did it get

**Claim §1.4: "did it arrive" and "how far did the far end get" are two different questions, and
weida answers both, separately.**

```rust,ignore
let meta = TransferMeta::default().with_report([stage()]);   // order a report
let mut transfer = pusher.open(meta).await?;
let mut cursors = transfer.cursors().expect("a report was ordered");
transfer.write_all(&payload).await?;
let delivered = transfer.finish()?.delivered().await.is_ok();  // fact one: the transport
let reported = cursors.changed().await.and_then(|s| s.offset(stage()));  // fact two: the application
```

On the receiving side, the mechanism is three lines:

```rust,ignore
let mut reporter = transfer.reporter().expect("the sender ordered a report");
let body = transfer.collect(64 * 1024).await?;
reporter.report(stage(), body.len() as u64).await?;          // an absolute offset
```

Four properties, each of which is a decision rather than an implementation detail:

- **The topology does not change.** That push is still one unidirectional stream. The report
  rides a stream of its own, so ordering cursors costs no pattern its shape
  ([decisions/0024](decisions/0024-three-families-one-back-channel.md) §4.4a).
- **The offset is absolute**, so a duplicated or reordered record is a no-op and coalescing is
  free. This is the same property that makes credit idempotent
  ([decisions/0003](decisions/0003-credit-unit.md)).
- **An order is not a guarantee.** A receiver that cannot reach a level simply does not report
  it, and the transfer does not fail for it. A level a peer *must* reach is the negotiated
  `acknowledgement` dimension of the handshake instead
  ([decisions/0006](decisions/0006-guarantee-sets.md) §4.4).
- **`stage()` is an application level** — the value 16, the first one an application may name.
  weida carries it and never interprets it, exactly as it carries a topic without parsing it
  ([PROTOCOL.md](PROTOCOL.md) §6.7).

The reason this exists is §0.2: at two to four hops, the only honest statement a hop can make is
about itself, and the only useful shape for it is a number.

### 1.5 What the receipt costs

**Claim §1.5: the receipt is a round trip, and it is not free.**

The program sends 1 KiB eight times each way and prints both:

```text
1 KiB, 8 rounds each: 165.864µs without the receipt, 19.978744ms with it
```

That is two orders of magnitude, and none of it is weida's overhead: it is QUIC's delayed
acknowledgement on an idle connection, which [GUARANTEES.md](GUARANTEES.md) §3 measured at
~26 ms against ~7.9 µs for the same push without the receipt. Under load the delay disappears
into the traffic — an acknowledgement rides the next packet — which is why the honest way to
present this number is to run it yourself rather than to quote it.

What to take from it:

- **Fire and forget is the default for a reason.** `send` returns when the FIN is queued.
- **A receipt per message is a design smell at scale.** If you need one per message you are
  asking for a request/reply pattern; use one.
- **A receipt on the *last* message of a batch is usually what you wanted**, and costs one
  round trip per batch. `examples/push_pull.rs` does exactly that.

### 1.6 What this chapter does not tell you

Chapter 1 is one transfer between two peers. Nothing in it is wrong at scale, and nothing in it
is *enough* at scale:

- one peer per side, so no selection policy, no fan-out, no drops — chapter 2;
- both ends in one process on loopback, so no reconnection and no partition;
- no hop in the middle, so responsibility never transfers;
- nothing durable, so no restart survives anything.

The rest are filed rather than written: this document grows one chapter per slice, each with
its programs and its assertions, because a guide whose examples do not run is worse than no
guide ([decisions/0025](decisions/0025-the-website.md) §2 is the same argument about a landing
page). The arc is in
[decisions/0026](decisions/0026-the-guide-and-the-c8b-question.md) §4.4, and the backlog holds
the slices.

---

## 2. Many peers, and the drop

The program: `examples/guide_many_peers.rs`. The test: the same `crates/weida/tests/guide.rs`.

One peer on each side is the only configuration in which a send has no policy. From two peers
on, every pattern answers the same three questions — **which peer, what if it cannot keep up,
and who else is affected** — and the answers differ per pattern on purpose. This chapter is
those answers, and it is where the C8B arithmetic of §0.1 gets its numbers.

### 2.1 Selection is the pattern's, not the call's

**Claim §2.1: the pattern chooses the peer set; the call does not change.**

The program connects one sender to three peers three times over, and sends the same way each
time:

```text
6 pushes over 3 pullers:      [2, 2, 2]
2 publishes to 3 subscribers: [2, 2, 2]
3 BUS members, one send each: [2, 2, 2]   <- never itself
```

Three policies, one call shape:

| Pattern | Who gets it | Mechanism |
| --- | --- | --- |
| Push | **one** live peer per send, rotating | one `fetch_add` on a cursor, `crates/weida/src/stream.rs` |
| Pub/Sub | **every** subscriber whose filter matches the topic | one stream per subscriber per message |
| BUS | **every** member except the sender | one writer per joined member |

Nothing at the call site selects a policy, and that is the design: `pusher.send(&body)` and
`publisher.publish(topic, body)` are the same statement about one message, and the peer set is
a property of the endpoint you built ([PATTERNS.md](PATTERNS.md) §3, §4, §7). ZeroMQ readers
will recognise this exactly — it is `zmq_socket(3)`'s socket-type table, with the type chosen
by a factory call instead of a constant.

The round-robin is worth one sentence more, because it is the one people assume wrongly: it
rotates over **live peers at the moment of the send**, so a peer that died is skipped rather
than waited for, and a peer that arrives joins the rotation. There is no hashing, no affinity,
and no ordering across peers — a Push fan-in is a work queue, not a partitioned log.

### 2.2 A subscriber that stops reading

**Claim §2.2: a subscriber that stops reading costs its neighbour nothing — and costs itself
nothing either, until the publisher outruns its transport.**

Two subscribers at the default limits. One drains; the other never calls `recv` once. The
program runs the publisher twice over the same pair:

```text
paced:    512 published, all 512 enqueued for both, the reader got 512, 0 dropped
flat out: 1537 more absorbed by a subscriber that has never read, then 1 dropped, cause: the queue
```

**The first line is the isolation claim, and it is the whole argument for a stream per
subscriber.** The reading subscriber received every message while its neighbour read nothing at
all. In a design with one queue per socket, the silent subscriber's backlog is the reader's
problem; here it is not, because each subscriber has its own stream, its own queue and its own
byte budget ([PATTERNS.md](PATTERNS.md) §1.3). What that isolation costs is measured in B-247
and quoted in §2.4.

**The second line is the part that surprises people, including the author of this program.**
A subscriber that has read nothing for two megabytes loses nothing at all while the publisher
is paced — and the reason is that a publisher's byte budget is released when the bytes reach
the **wire**, not when somebody reads them. QUIC absorbs the difference. The loss starts only
when the publisher outruns the silent subscriber's *transport*, which on this machine took
another ~1500 messages.

> **Overload is a rate, not a backlog.**

That sentence is why the drop counters are per cause rather than per subscriber, and why the
first two versions of this program measured the wrong thing: a paced publisher reported zero
drops for a subscriber that had read nothing, and a tiny budget starved the reader as well as
the silent one. Both attempts are in the program's doc comment, because the mistakes are more
instructive than the result.

### 2.3 Which ceiling binds, and how you know

**Claim §2.3: a stalled subscriber is bounded twice, and the counter says which bound it hit.**

A subscriber costs a publisher two bounded resources: `endpoint_queue` **messages** and
`Limits::subscriber_buffer_bytes` of **payload**. Which one refuses first is not a matter of
taste, and it is not the payload size either:

```text
budget   64 KiB:   321 messages ( 321 KiB) accepted, then the byte budget
budget 8192 KiB:  1665 messages (1665 KiB) accepted, then the queue
```

One variable, two outcomes, and the mechanism is QUIC. A copy's budget permit is released when
its bytes reach the wire, and a connection absorbs a window's worth before it stops — 0.6 to
1.5 MiB on this machine. So a budget **below** that band is spent while QUIC is still
accepting, and the budget refuses; a budget **above** it never fills, because the wire stops
the writer first and then the queue backs up. `benches/fanout.rs` sees the same thing at the
defaults (B-247): the 8 MiB default means the **queue** is what refuses, at 1153-1537 messages
of 1 KiB.

**The operational point is the counter, not the rule.** `dropped_on(topic)` separates
`subscriber_budget` from `subscriber_queue` from `no_parked_connection`, so a starving
subscriber is a lookup rather than a packet capture — and the two answers mean different
remedies: a budget refusal wants a bigger budget or a smaller payload, a queue refusal wants a
faster consumer or a deeper queue. "Messages are being dropped" is not an operational answer,
which is why this API does not offer it.

What a stalled subscriber costs in **memory** is the second half, measured in B-247: 0.34-1.19
MiB resident per stalled subscriber at a 1 KiB payload, 9.3-10.7 MiB at 64 KiB. And one
correction that a reader sizing a node will want: `subscribers × subscriber_buffer_bytes`
**overstates** the cost, because one `publish` is one `Bytes` that every copy shares. The
budget is an accounting bound; the resident cost is one payload per distinct message.

### 2.4 What width costs the publisher

**Claim §2.4: the publisher's cost per message grows with the width, and its cost per
subscriber falls.**

The program prints its own numbers — in a debug build unless you pass `--release`, and it says
so. The figures worth quoting are the benchmark's, from B-247
([IMPLEMENTATION.md](IMPLEMENTATION.md) §4), because they are release builds over three runs:

| Width | One publish | Per subscriber | Idle latency, median | Transport state per subscriber |
| --- | --- | --- | --- | --- |
| 1 | **287 ns** | — | 34.7-37.9 µs | 1.63-1.76 MiB (includes the one-off endpoint state) |
| 16 | **3.04 µs** | 184 ns | 76.2-77.8 µs | 402-418 KiB |
| 256 | **22.94 µs** | **88.8 ns** | 507-539 µs | 396-436 KiB |

Three things to take from that table.

**The marginal cost per subscriber falls as the width grows** — 184 ns at 16 against 88.8 ns at
256 — because the fixed cost of a publish is amortized and the registry walk gets its cache.
So the number to plan with is the wide one, and a node's publisher CPU at 10⁴ subscribers is
about **0.9 ms per message**.

**Neither memory nor CPU is what limits the width.** At ~420 KiB per subscriber, 64 GiB of
transport state is on the order of 1.5·10⁵ subscribers. What binds is the delivery rate:
**276-279 Kcopies/s**, so one message to 10⁴ subscribers spends ~36 ms of a node's capacity and
one to 10³ spends ~3.6 ms. §0.1's width table is that division, and it is the reason the C8B
arithmetic has a width of 10³-10⁴ rather than 10⁵.

**A wide publisher is not a slow publisher, it is a *late* one.** The median latency at width
256 is 507 µs against 35 µs at width 1 — fourteen times worse, on an idle machine, with
nothing else running. A subscriber at the far end of a wide fan-out waits for the publisher to
get to it, and the only structural answer is another hop, which is §0.1's depth.

### 2.5 A peer that never answers

**Claim §2.5: a survey is a fan-out with a deadline, so a silent peer is a number rather than a
hang.**

Push rotates past a dead peer and Pub/Sub drops a copy, but neither has to answer the hardest
version of the question: what if a peer takes the message and then says nothing? SURVEY is the
pattern that must, and its answer is the deadline the caller passes:

```text
asked 3, answered 2, silent 1 within the deadline
```

The silent respondent in that program is not unreachable, not refusing and not slow to connect
— it completed the handshake, accepted the question and held it. The surveyor returns what
arrived, `late()` counts what arrived after the deadline, and `respondents()` says how many
were asked ([PATTERNS.md](PATTERNS.md) §5). No configuration, no retry policy, no circuit
breaker: **the deadline is the caller's and the result is a count**.

That shape is the pattern's whole contribution, and it is worth naming because the alternative
is so common: a fan-out that waits for everybody has its availability set by its worst member,
which at C8B scale is a certainty rather than a risk.

### 2.6 What this chapter does not tell you

- **Every number here is one process on loopback.** No propagation delay, no NIC, no loss, and
  a congestion controller that never sees a real bottleneck (§0.4).
- **The widest fan-out measured anywhere in this repository is 256.** The C8B arithmetic uses
  10³-10⁴, which is an extrapolation from a flat per-subscriber cost between 16 and 256 (§0.4).
- **A drop is invisible to the subscriber here.** It need not be: the sequence key of
  [PROTOCOL.md](PROTOCOL.md) §6.2 and the `PerProducer` detect level turn a fan-out drop into a
  reported `Gap`, and [PATTERNS.md](PATTERNS.md) §4 has the mechanism. It is implemented and
  this chapter does not teach it yet.
- **Nothing here takes responsibility for a message.** Every peer in this chapter forgets
  immediately, which is the next question rather than an omission — a hop that takes
  responsibility is chapter 3, and it needs a queue that survives its consumer.

---

## 4. Depth, and what a chain may claim

The program: `crates/interop/cross-tests/examples/guide_depth.rs`. The test:
`crates/interop/cross-tests/tests/guide_depth.rs`.

```text
cargo run -p weida-cross-tests --example guide_depth
```

**Why chapter 4 before chapter 3.** The chapters are numbered by what a reader needs and
written in the order of what can be *asserted*. Chapter 3 is a hop that takes responsibility,
which needs a queue that survives its consumer — filed, not built (B-203). This chapter needs
two foreign protocols and two bridges, which exist and are tested, so it is written first
([decisions/0026](decisions/0026-the-guide-and-the-c8b-question.md) §4.4). Nothing in it
depends on chapter 3.

§0.1 ended with a number that makes this chapter the centre of the whole question: at a
fan-out width of 10³-10⁴, **two to four hops reach eight billion people**. So a message's fate
is decided by three or four intermediaries, and the only question that matters at that depth is
**what each of them is allowed to claim**.

This chapter's chain is deliberately harsher than a weida deployment:

```text
ZeroMQ peer --ZMTP--> bridge --weida--> bridge --SP--> nng peer
```

Three protocols, two translations, both foreign ends the real implementations — `zeromq` in
pure Rust and `nng`'s C library. A chain of weida hops would be the easy case; this is the
case a reader will actually deploy, because a system that reaches everybody does not get to
choose everybody's protocol.

### 4.1 A hop is a translation

**Claim §4.1: a hop is a translation, and what crosses is what both protocols can express.**

The topic makes it visible, because all three protocols express it differently:

```text
ZMTP sent two frames, "sport.football" and "goal"
SP received one body, "sport.football\0goal" — the frame boundary became a NUL
```

| Protocol | How a topic exists | Why |
| --- | --- | --- |
| ZMTP | its **own frame** | a prefix match "won't cross a frame boundary" ([zmtp](adapters/zmtp.md) §6) |
| weida | a **segmented field** in the DATA header | a topic is not a path and `*`/`#` match segments ([PROTOCOL.md](PROTOCOL.md) §6.4) |
| SP | **leading bytes** of the body | SP has no topic field at all ([nng](adapters/nng.md) §6) |

So the chain translates frame → weida topic → leading bytes, and the NUL delimiter is what
lets the last step be undone by whoever reads it — NUL because it is the one octet a weida
topic cannot contain. **The frame boundary the sender used does not exist at the far end.** The
delimiter is a convention the reader has to know, and that convention is in a document rather
than on the wire.

That is what a translating hop costs, and it is the cheapest example in this repository. The
expensive ones are in [adapters/zmtp.md](adapters/zmtp.md) §8, which names ten of them — L1
multipart, L2 byte-prefix subscriptions, L5 ROUTER's silent drop, L10 no application
acknowledgement — as **named losses** rather than caveats, because a loss with a number can be
refused at configuration time.

### 4.2 What the chain may claim

**Claim §4.2: the chain's claim is the weakest hop's, and nothing composes upward.**

```text
the ZeroMQ send succeeded: true; anything arrived at the SP end: false
```

That pair is the whole chapter in two booleans. The program closes the `nng` receiver
**before** sending, the ZeroMQ send succeeds anyway, and a fresh receiver on the same address
proves nothing arrived.

Neither protocol is lying. ZeroMQ's transfer point is `zmq_send` returning and nothing further
([zmtp](adapters/zmtp.md) §7); SP has **no transfer point at all** — no application
acknowledgement, no broker receipt, no persistence signal ([nng](adapters/nng.md) §7). weida's
own `delivered()` proves one hop's transport and says nothing about the application, let alone
about the next protocol's ([GUARANTEES.md](GUARANTEES.md) §1). Composed:

> **`BestEffort` ∩ `BestEffort` = `BestEffort`**, and the chain's honest end-to-end claim is
> that a message was accepted somewhere near the beginning.

This is also why the guarantee vocabulary is per hop by construction and not a property of a
message ([decisions/0006](decisions/0006-guarantee-sets.md)). A hop may promise what it can
keep. A chain of hops promises the intersection, and no amount of bookkeeping at the edges
makes the middle stronger.

§0.2 is the reason this is a design constraint rather than a disappointment: at two to four
hops and 8·10⁹ recipients, an acknowledgement per recipient converging on one root is **1.1 TB
of acknowledgement traffic for one message**. The tree that makes the fan-out affordable makes
the end-to-end acknowledgement impossible, so hop-local is not a weakening of a stronger
design — it is the only design that exists at this scale.

### 4.3 Whose limit decides

**Claim §4.3: the smaller ceiling decides, and it decides at the first hop that sees it.**

```text
8192 B offered through a 4096 B near cap and a 1 MiB far cap: far end received false
```

The ZMTP edge holds a 4 KiB ceiling and the SP edge keeps its 1 MiB default. The 8 KiB message
dies at the first hop, and the second hop **buffers nothing** — which is the part worth a
program rather than a sentence: a chain does not carry a payload as far as it can and then
discard it, so a refused message costs the far hop no memory at all. At depth that is the
difference between one node's bad configuration and a whole tree's.

The general rule, and it is the same rule as §4.2 in a different currency: **a chain's
capability is the minimum over its hops, per dimension.** A caller sizing a payload has to know
the smallest ceiling on the path, and there is no protocol mechanism that discovers it — each
edge is configured, each edge refuses what it cannot hold, and the first refusal is the one you
get.

### 4.4 A loss that cannot travel

**Claim §4.4: a loss at the first hop is invisible to the sender and total for the chain.**

```text
2 ZMTP frames in one message: the send succeeded (true), the far end received false
```

A two-frame ZeroMQ message has no weida counterpart — there is "no message-part concept
anywhere in v0" ([zmtp](adapters/zmtp.md) §3) — and concatenating the frames would invent an
application protocol on the application's behalf, so the bridge refuses. That is **L1** of the
named losses, and the composition is the lesson: **the SP side could have carried those bytes
perfectly well, and it never sees them.** A chain's capability is an intersection, not a union.

The sender's half is the uncomfortable one, and it follows from §4.2 rather than being a second
defect: `zmq_send` returned, so by ZeroMQ's own contract the send succeeded — of a message that
will never exist anywhere else. An application that needs to know otherwise has to ask a
question instead of making a statement, which is chapter 1 §1.3's rule surviving two protocol
boundaries unchanged.

### 4.5 The intersection, as arithmetic

**Claim §4.5: the intersection is arithmetic a caller can do, and the runtime's own answer is
not observable.**

```text
core ∩ core = BestEffort delivery, TransportReceipt acknowledgement
core ∩ (AtLeastOnce, Accepted) = BestEffort delivery, TransportReceipt acknowledgement
can an application ask a live connection what it negotiated? false
```

The second line is the one that carries the claim, and the first cannot: `core ∩ core` is
symmetric, so it would read the same if the arithmetic took the **stronger** level. A hop
offering `AtLeastOnce` delivery and a broker's `Accepted` completion still agrees on core with
a hop that offers neither, and the chapter's test asserts *that* pair for exactly this reason.

`GuaranteeSet::intersect` is the whole mechanism, and it is a public function: per dimension
the weaker of the two levels, an exact match required for the dimensions that are not ordered,
`durability` and `replicas` dropped when the weakened acknowledgement cannot carry them, and a
connection **refused** with `NEGOTIATION_FAILED` when the result does not reach what a peer
requires. There is no downgrade path ([GUARANTEES.md](GUARANTEES.md) §4,
[decisions/0006](decisions/0006-guarantee-sets.md) §4.4). Both sides run it on their own and
the peer's HELLO, so both reach the same verdict without a round trip.

**And then the honest half.** No public accessor reports the negotiated set of a *live*
connection: the value is computed, stored and enforced inside the runtime, and `Agreed` is
`pub(crate)`. An application that wants to know what its connection agreed to has to read the
adapter's mapping document instead of asking the connection. The chapter's test asserts that
gap — `observable_on_a_connection` is `false` — so that adding an accessor breaks the test and
forces this paragraph to be rewritten rather than left quietly wrong.

That is not a large defect, and it is exactly the kind a guide finds: the specification
documents describe a negotiated set as an observable property of a connection, the API does not
expose it, and nobody noticed until a chapter tried to print it. Filed as **B-262**.

### 4.6 What this chapter does not tell you

- **Two hops, not four.** Every experiment here is one translation in each direction. The
  arithmetic of §0.1 needs three or four, and nothing in this repository runs a chain that
  long.
- **Both foreign ends are real, and both are on loopback.** No propagation delay between hops,
  which is exactly the term that makes a deep chain's latency interesting.
- **A hop that takes responsibility is chapter 3**, and it is the missing half of this chapter:
  everything here is a hop that forwards. What `Accepted` may mean, and what it may not, needs
  a queue that survives its consumer (B-203).
- **Federation between administrations is unbuilt** (§0.4). Two protocols in one process is not
  two operators, and the question of what a hop across an organisational boundary may claim has
  a protocol answer and no deployment answer.
