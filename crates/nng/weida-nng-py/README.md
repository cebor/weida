# weida-nng

nanomsg/NNG's Scalability Protocols for asyncio, on a Rust implementation of
the protocols rather than on the NNG C library. No C library is linked and no
shared object beyond the extension itself is needed.

```python
import asyncio
import weida_nng

async def main():
    context = weida_nng.Context()
    server = weida_nng.RepSocket(context)
    client = weida_nng.ReqSocket(context)

    url = await server.listen("tcp://127.0.0.1:0")  # the port the kernel chose
    await client.dial(url)

    await client.send(b"Hello")
    assert await server.recv() == b"Hello"
    await server.send(b"World")
    assert await client.recv() == b"World"

    await context.shutdown()

asyncio.run(main())
```

## What is here

- `Context`, three ways: `Context()` owns its reactor, `Context.current()`
  takes an ambient Tokio one, `Context.sharing(other)` takes another context's.
  NNG has no such object — its reactor, `inproc` namespace and resource
  ceiling are process-global — and all three are explicit here.
- The eleven SP protocols as eleven classes — `ReqSocket`, `RepSocket`,
  `PushSocket`, `PullSocket`, `PubSocket`, `SubSocket`, `Pair0Socket`,
  `Pair1Socket`, `SurveyorSocket`, `RespondentSocket`, `BusSocket` — each with
  `dial`, `dial_nowait`, `listen`, `pipe_count` and `close` over `tcp://`,
  `ipc://` and `inproc://`.
- `nng_ctx` as Python objects: `socket.context()` on REQ, REP, SURVEYOR and
  RESPONDENT hands out a transaction of its own, so concurrent requests are
  concurrent rather than queued, and `ESTATE` is per context.
- NNG's errno vocabulary as exception classes: `ESTATE`, `ETIMEDOUT`,
  `ECLOSED`, `EMSGSIZE` and nineteen more, each under `NngError`, each
  carrying `errno` and `cause`.
- `SocketOptions`, validated where it is written, and `OPTIONS` /
  `option("NNG_OPT_...")`: the whole of `nng_options(5)`, saying for every
  name whether this library honours it, and if not, why.

## What is not here

- **Raw sockets and `nng_device`.** Raw mode hands the protocol header to the
  application; the library has both in Rust, and a Python surface for them
  would be a byte-slicing API over the header these classes keep.
- **The `tls+tcp://` transport.** The library implements it; this binding
  refuses the URL until the certificate surface is designed for Python,
  rather than offering a transport a caller cannot configure.
- **`NNG_OPT_PAIR1_POLY`**, which is refused by the library itself and for
  its reasons — see `docs/libraries/nng.md`.

## No event loop? `weida_nng.sync`

The same eleven protocols and the same contexts, blocking, for a process
with no asyncio loop in it:

```python
from weida_nng import SocketOptions, sync

# A timeout is what turns a stalled exchange into an exception rather than a
# parked thread: NNG's default is to wait forever.
options = SocketOptions(recv_timeout=5.0, send_timeout=5.0)

context = sync.Context()
server = sync.RepSocket(context, options)
client = sync.ReqSocket(context, options)
url = server.listen("tcp://127.0.0.1:0")
client.dial(url)

client.send(b"Hello")
assert server.recv() == b"Hello"
server.send(b"World")
assert client.recv() == b"World"

context.shutdown()
```

It is a facade over `weida-nng`'s own `blocking` module, which is `block_on`
around the asynchronous sockets: **no protocol behaviour is implemented
twice**, and the two surfaces interoperate because they are one
implementation. The GIL is released while a call blocks, so one thread
parked in `recv` does not stop another.

## Concurrency

Two coroutines may use one socket at the same time: an SP socket's `send` and
`recv` take `&self` in Rust, so a parked receive does not hold the socket
against a send. Where a protocol needs one transaction at a time it says so
per context, with `ESTATE`, which is the protocol's own answer rather than a
binding-level lock.

## Building it

```sh
./develop.sh          # venv, maturin develop, pytest
./develop.sh -k req   # arguments go to pytest
```

The wheel is `abi3` from CPython 3.9, so one build serves every later
interpreter.

```sh
./package.sh
```

builds the `abi3` release wheel, installs it into a throwaway virtualenv and
runs one REQ/REP round trip on each surface with `cargo`, `rustc` and
`maturin` off `PATH` — so "no Rust toolchain and no C library to install
this" is checked rather than claimed.
