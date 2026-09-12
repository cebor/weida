# weida-nats

Core NATS for asyncio, on a Rust client implementation. No C library is linked
and no shared object beyond the extension itself is needed.

```python
import asyncio
import weida_nats

async def main():
    nats = await weida_nats.connect("127.0.0.1", 4222)

    orders = await nats.subscribe("orders.>")
    await nats.publish("orders.created", b"{}")
    message = await orders.next()
    assert message.subject_str == "orders.created"

    # The window is mandatory: a request API that can hang is a bug.
    reply = await nats.request("service.echo", b"ping", 2.0)
    assert reply.payload == b"ping"

    await nats.close()

asyncio.run(main())
```

## What is here

- `connect(host, port, **options)`, whose connection owns its reactor, plus
  `Connection.connect_current(...)` for an ambient Tokio one and
  `Connection.connect_sharing(other, ...)` for a second connection on the
  first one's threads.
- `publish`, `publish_with` (reply subject and `NATS/1.0` headers),
  `subscribe`, `subscribe_with_queue_group`, `request` and `request_many`,
  `flush`, `close`, and the server's view: `info()`, `max_payload()`,
  `headers_supported()`, `is_lame_duck()`, `lame_duck_notice()`, `state()`,
  `closed()`.
- `Subscription` as an async iterator — `async for message in subscription` —
  beside `next()`, `try_next()`, `unsubscribe()` and `unsubscribe_after(n)`.
- `weida_nats.sync`: the same objects with no coroutines, for a process with
  no event loop. `sync.connect(...)`, and `Subscription.next_msg(timeout)` as
  the blocking drain.
- The library's failure vocabulary as exception classes, each under
  `NatsError`, each carrying `errno` and `cause`. `NoResponders` and
  `RequestTimeout` are distinct classes, because 503 means "nobody was
  listening, now" and a timeout means "somebody may well have been".

Every call of the asynchronous surface is a coroutine: nothing blocks the
event loop, nothing holds the GIL while waiting, and a cancelled task cancels
the operation underneath. Every call of the synchronous surface releases the
GIL for as long as it blocks.

## What is not here

TLS. `weida-nats` completes it where `INFO` demands it, and it needs the
caller's `rustls::ClientConfig` to know which certificates are valid — there
is no Python object to hand it, so this binding does not choose a trust store
on its callers' behalf. A server whose `INFO` says `tls_required` arrives as
`weida_nats.TlsRequired`.

There is no reconnect loop either, here or in the library: reconnection is a
client-library policy, and the material one needs is exposed —
`info().connect_urls`, `lame_duck_notice()` and `closed()`.

## Building it from a checkout

```sh
sh crates/nats/weida-nats-py/develop.sh
```

It creates a `uv`-managed virtualenv at the repository root, installs
`maturin` and `pytest` into it, runs `maturin develop` and then the Python
tests. Nothing is installed into a system interpreter. The tests script the
server half in Python and need no `nats-server`.

```sh
sh crates/nats/weida-nats-py/package.sh
```

builds the `abi3` release wheel, installs it into a throwaway virtualenv and
runs one publish/subscribe round trip, one request-reply and the
`NATS/1.0 503` with `cargo`, `rustc` and `maturin` off `PATH` — so "no Rust
toolchain and no C library to install this" is checked rather than claimed.
