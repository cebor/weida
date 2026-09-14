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
   chapters do not carry snippets written for the page. `examples/guide_one_transfer.rs` is
   chapter 1's program; `crates/weida/tests/guide.rs` includes it as a module and asserts the
   claims the chapter makes. A claim added to this document without a program and an assertion
   fails `every_claim_the_chapter_makes_has_a_program_and_a_test`.
2. **No number here is an estimate.** Measured figures name the item that measured them
   ([IMPLEMENTATION.md](IMPLEMENTATION.md) §4); arithmetic derived from them says so in the
   sentence. Where nothing has been measured, the guide says *that* instead of guessing — and
   §0 has one such gap, named.

Run chapter 1 before reading it:

```text
cargo run -p weida --example guide_one_transfer
cargo test -p weida --test guide
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
| Subscriber byte budget, default | **8 MiB** per subscriber | `Limits` |
| Topic filter match | **14 ns**, shape-independent | B-021 |

Everything below is **arithmetic on those numbers**, not a measurement.

**One sender cannot do it.** 8·10⁹ ÷ 179,400 ≈ **12.4 hours** for one producer to hand out one
message each. So the shape has to be a tree, and the only question is how wide and how deep.

**How wide can one node be?** A fan-out node pays two things per subscriber: transport state,
and the byte budget its slow-reader policy reserves. At the measured ~400 KiB per side and the
default 8 MiB budget, a node with 64 GiB of memory holds on the order of **10³ to 10⁴**
subscribers — 10³ comfortably, 10⁴ only with the budget cut to about a megabyte, which is a
configuration decision and not a code change ([PATTERNS.md](PATTERNS.md) §4). Call the width
*d* ∈ [10³, 10⁴].

**Then the depth is tiny.** log(8·10⁹) ÷ log(10³) ≈ 3.3, and ÷ log(10⁴) ≈ 2.5. So

> **two to four hops reach everybody.**

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
was dropped" is not an operational answer.

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

- **The fan-out width above is derived, not measured.** Nothing in this repository has measured
  a publisher at 10³ or 10⁴ subscribers; the largest measured fan-out is **eight** (65.2 µs to
  publish and drain 1 KiB to 8 subscribers, B-021's bench). The width *d* is the single number
  the whole depth argument rests on, and it is filed as B-247 for exactly that reason.
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

- one peer per side, so no selection policy, no fan-out, no drops;
- both ends in one process on loopback, so no reconnection and no partition;
- no hop in the middle, so responsibility never transfers;
- nothing durable, so no restart survives anything.

Those are the next chapters, and they are filed rather than written: this document grows one
chapter per slice, each with its programs and its assertions, because a guide whose examples do
not run is worse than no guide ([decisions/0025](decisions/0025-the-website.md) §2 is the same
argument about a landing page). The arc is in
[decisions/0026](decisions/0026-the-guide-and-the-c8b-question.md) §4.4, and the backlog holds
the slices.
