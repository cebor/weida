# weida-amqp — AMQP 1.0 for Python

An AMQP 1.0 client for asyncio, on a Rust implementation rather than on Proton.
No C library, no broker required to install it, and the OASIS standard's own
error conditions as exception classes.

```python
import asyncio
import weida_amqp

async def main():
    connection = await weida_amqp.connect("127.0.0.1", 5672, container_id="app")
    session = await connection.begin()

    sender = await session.attach("orders", "sender", "/queues/orders")
    outcome = await sender.send(b"an order")
    assert outcome.name == "accepted"        # what the receiver committed to

    receiver = await session.attach("invoices", "receiver", "/queues/invoices")
    await receiver.grant_credit(10)          # nothing arrives before this
    async for delivery in receiver:
        print(delivery.body())
        await receiver.accept(delivery.delivery_id)
        break

    await connection.close()

asyncio.run(main())
```

Synchronously, with no event loop in the process:

```python
from weida_amqp import sync

connection = sync.connect("127.0.0.1", 5672)
session = connection.begin()
sender = session.attach("orders", "sender", "/queues/orders")
assert sender.send(b"an order").name == "accepted"
connection.close()
```

## Three things this client insists on

- **`await send` returns the delivery's terminal state**, not a boolean. On an
  unsettled link it completes when the peer's `disposition` has settled the
  delivery. On a `settled`-mode link it returns `None`, because nothing is
  coming and nothing can be concluded — which is that mode's whole cost, said
  out loud.
- **Credit is granted explicitly.** `grant_credit(n)`, and nothing before it.
  Link credit is the receiver's instrument; a client that granted some behind
  your back would be choosing your prefetch.
- **The answering `attach` is data.** `negotiated()` reports the settle modes
  actually in force and the addresses the peer actually created, because a
  broker may narrow what you asked for.

## Errors

One family under `weida_amqp.AmqpError`: a class per client-side failure
(`HandshakeTimeout`, `ConnectionGone`, `Configuration`, …) and a class per
error condition the specification names (`LinkStolen`,
`SessionWindowViolation`, `ResourceLimitExceeded`, …). `weida_amqp.CONDITIONS`
maps each class name to its `amqp:…` symbol.

## Differences from `python-qpid-proton`

Row by row in [`docs/libraries/amqp-py.md`](../../../docs/libraries/amqp-py.md).

## Building from this checkout

```sh
sh crates/amqp/weida-amqp-py/develop.sh    # venv, maturin develop, pytest
sh crates/amqp/weida-amqp-py/package.sh    # release wheel, proved with no Rust on PATH
```

Licensed under MIT OR Apache-2.0.
