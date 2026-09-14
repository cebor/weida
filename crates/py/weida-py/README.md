# weida for Python

QUIC-native messaging from asyncio, on the Rust implementation: all six patterns — Req/Rep,
Push/Pull, Pub/Sub, Pair, Survey and Bus — each with the whole-payload calls a caller reaches
for first, and the streamed forms for a payload that does not fit memory where the pattern has
one.

```python
import asyncio
import weida

async def main():
    server = weida.Runtime()
    binding = await server.bind("127.0.0.1:0", weida.Identity.generate())
    replier = binding.replier("/echo")

    async def serve():
        request = await replier.accept(1 << 20)
        await request.reply(request.payload)

    answering = asyncio.create_task(serve())

    client = weida.Runtime()
    requester = client.requester(weida.Trust.by_address())
    await requester.connect(binding.url("/echo"))
    assert await requester.request(b"ping", 1 << 20) == b"ping"
    await answering

asyncio.run(main())
```

```python
# A payload larger than a subscriber's whole budget: `publish` refuses it,
# `open` carries it, one stream per subscriber.
fan = publisher.open("px.eur")
for chunk in frames:
    await fan.write_within(chunk, 5.0)   # waits for room, drops who has none
await fan.finish()
```

`write_within(chunk, seconds)` waits up to `seconds` for a subscriber with no room and then
drops *that* subscriber's copy; `write_now(chunk)` never waits, which is what a signal whose
next chunk supersedes this one wants. Neither is a default, because the choice is the
publisher's.

## A pair, a survey and a bus

```python
# PAIR: one peer, both directions. The first peer is the one kept — a second
# dialler's transfer is refused and the first keeps working.
link = binding.pair("/link")
await link.send(b"anything")
payload, meta = await link.recv(1 << 20)

# SURVEY: one question to every respondent, bounded by *your* deadline.
survey = await surveyor.survey(b"who is there", 0.5, 1 << 20)
print(survey.replies, survey.asked, survey.silent(), survey.failed)

# BUS: every message to every other member, never to the sender.
member = binding.bus("/mesh", weida.Trust.by_address())
await member.connect(other_url)
reached = await member.send(b"hello all")
```

A survey is a value and not an iterator: it is asked, waited out, and read. Silence is a
number — `silent()` is who said nothing before the deadline — and a respondent that refused or
died is `failed`, so "nobody answered" is an answer rather than an exception. The same three
patterns are on `weida.sync` with the same shapes.

## A verdict for a transfer that has no reply

```python
# The producer orders a report and reads it. `Processed` is not something a
# FIN can carry, so it arrives on a stream of its own, after the payload.
cursors = await pusher.send(work, report=[weida.ACCEPTED, weida.PROCESSED])
while (latest := await cursors.changed()) is not None:
    if weida.PROCESSED in latest:
        break

# The receiver answers as it gets there.
payload, meta, reporter = await puller.recv_reporting(1 << 20)
await reporter.report(weida.ACCEPTED, len(payload))
...
await reporter.report(weida.PROCESSED, len(payload))
await reporter.finish()
```

A level is an integer: the named rungs are `weida.TRANSPORT_RECEIPT`, `ACCEPTED`, `STORED`,
`REPLICATED` and `PROCESSED`, and an application names its own stages at or above
`weida.APPLICATION_FLOOR` — carried and ordered, never interpreted. Offsets are absolute, so a
report coalesced on the way loses nothing, and nothing ever waits on a cursor: a peer that
never reports fails no transfer. On `weida.sync` the same two handles are there, and
`changed(seconds)` takes a deadline, because a parked thread is interrupted by nothing.

## A peer is its public key

`binding.url(path)` prints `weida://sha256:…@host:port/path`, and that address is the whole
of a client's configuration: `Trust.by_address()` accepts exactly the key it names and nothing
else. No certificate authority, no certificate file, no trust store. A peer with any other key
fails `connect` with `weida.Untrusted`, whose `cause` names the key that answered so an
operator can check it out of band and paste it in. The alternatives are there where they mean
something: `Trust.pin(fingerprint)` for a key known ahead of the address, and
`Trust.anchor_file(path)` for a certificate authority, where the host the address names is
verified against the certificate.

`Identity.generate()` mints a fresh self-signed identity held only in memory, so a restart
changes the address; `Identity.from_pem_file(path)` keeps it stable.

## Every receive takes a ceiling

`accept`, `request` and `recv` each take a maximum payload size in bytes. weida's payloads are
streams and a Python object is not, so the caller who wants the bytes in memory is the one who
says how many there may be — a default here would be a decision about how much memory a
stranger may make this process allocate. A payload above the ceiling raises
`weida.LimitExceeded`.

## Failures are classes

Every failure of the library is a class under `weida.WeidaError`, named for the library's own
variant and carrying `errno` and `cause`:

```python
try:
    await requester.request(b"ping", 1 << 20)
except weida.Rejected:
    ...          # the peer said no
except weida.UnknownEndpoint:
    ...          # nothing is registered at that path
except weida.Indeterminate:
    ...          # the outcome is genuinely unknown: do not retry as a failure
```

`weida.Indeterminate` is the one worth reading twice. It means the transfer may or may not
have arrived, and it is deliberately not a kind of `ConnectionLost`: a caller that treats it
as a definite failure is wrong.

## No timeouts of its own

Nothing here takes a timeout argument, because `asyncio.wait_for` already is one and cancelling
a weida coroutine resets the streams it owns — so the peer learns rather than waits. The
exceptions are the three deadlines that are part of what the call *means*:
`Runtime.drain(seconds)`, `Surveyor.survey(payload, seconds, ceiling)` and
`FanOut.write_within(chunk, seconds)`. Each is mandatory: an unbounded drain, an unbounded
survey and an unbounded wait for a subscriber are all hangs with a rationale.

## Building it

```sh
sh crates/py/weida-py/develop.sh
```

It creates a `uv`-managed virtualenv at the repository root, installs `maturin` and `pytest`
into it and nothing into the system interpreter, runs `maturin develop`, then runs the tests.
Both halves of every test are this library, so nothing external has to be running. The wheel
is `abi3` from CPython 3.9, so one build serves every later interpreter.
