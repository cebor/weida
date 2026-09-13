# weida for Python

QUIC-native messaging from asyncio, on the Rust implementation. Req/Rep and Push/Pull today;
Pub/Sub and the streaming surface are the next slice.

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
a weida coroutine resets the streams it owns — so the peer learns rather than waits. The one
exception is `Runtime.drain(seconds)`, whose deadline is mandatory: an unbounded drain is a
hang with a rationale.

## Building it

```sh
sh crates/py/weida-py/develop.sh
```

It creates a `uv`-managed virtualenv at the repository root, installs `maturin` and `pytest`
into it and nothing into the system interpreter, runs `maturin develop`, then runs the tests.
Both halves of every test are this library, so nothing external has to be running. The wheel
is `abi3` from CPython 3.9, so one build serves every later interpreter.
