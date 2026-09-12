# weida-zmq

ZeroMQ for asyncio, on a Rust implementation of the protocol rather than on
libzmq. No C library is linked and no shared object beyond the extension itself
is needed.

```python
import asyncio
import weida_zmq

async def main():
    context = weida_zmq.Context()
    server = weida_zmq.RepSocket(context)
    client = weida_zmq.ReqSocket(context)

    endpoint = await server.bind("tcp://127.0.0.1:0")  # the port the kernel chose
    await client.connect(endpoint)

    await client.send(b"Hello")
    assert await server.recv() == [b"Hello"]
    await server.send(b"World")
    assert await client.recv() == [b"World"]

    await context.shutdown()

asyncio.run(main())
```

## What is here

- `Context`, three ways: `Context()` owns its reactor, `Context.current()`
  takes an ambient Tokio one, `Context.sharing(other)` takes another context's.
- The eleven stable socket types as eleven classes — `ReqSocket`, `RepSocket`,
  `DealerSocket`, `RouterSocket`, `PubSocket`, `SubSocket`, `XPubSocket`,
  `XSubSocket`, `PushSocket`, `PullSocket`, `PairSocket` — each with `bind`,
  `connect`, `unbind`, `disconnect`, `last_endpoint`, `peer_count` and `close`
  over `tcp://`, `ipc://` and `inproc://`.
- libzmq's errno vocabulary as exception classes: `EAGAIN`, `EFSM`, `ETERM`
  and sixteen more, each under `ZmqError`, each carrying `errno` and `cause`.

Every call is a coroutine: nothing blocks the event loop, nothing holds the GIL
while waiting, and a cancelled task cancels the operation underneath.

## What is not here yet

`send` and `recv` exist on `ReqSocket` and `RepSocket`. The other nine socket
types, the `Multipart` value, the non-blocking forms and the per-call timeouts
are the next item; the option table, security, the monitor, the devices and the
synchronous surface follow it.

## Building it from a checkout

```sh
crates/zmq/weida-zmq-py/develop.sh
```

It creates a `uv`-managed virtualenv at the repository root, installs `maturin`
and `pytest` into it, runs `maturin develop` and then the Python tests. Nothing
is installed into a system interpreter.
