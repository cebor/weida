# weida-mqtt for Python

An MQTT 5.0 **client** for asyncio, on a Rust implementation: no C library, no
broker, and MQTT's reason codes as exception classes.

```python
import asyncio
import weida_mqtt

async def main():
    context = weida_mqtt.Context()
    options = weida_mqtt.ConnectOptions("sensor-1")
    client, events = await context.connect("127.0.0.1:1883", options)

    granted = await client.subscribe([weida_mqtt.Subscription("room/+", 2)])
    assert granted == [2]                      # one granted code per filter

    done = await client.publish(weida_mqtt.Message("room/12", b"21.5", qos=2))
    assert done.kind == "complete"             # the PUBCOMP arrived

    async for delivery in events:
        print(delivery.topic, delivery.payload, delivery.qos)
        break

    await client.disconnect()

asyncio.run(main())
```

## What it is

* **Asyncio first.** Every call is a coroutine on the reactor the context
  owns. Nothing blocks the event loop and nothing holds the GIL while waiting.
* **Reason codes as classes.** Every failure is a class under
  `weida_mqtt.MqttError`, named for the MQTT reason code it carries and
  carrying the byte as `.reason_code`:

  ```python
  try:
      await client.publish(weida_mqtt.Message("a", b"x", retain=True))
  except weida_mqtt.RetainNotSupported as refused:
      assert refused.reason_code == 0x9A
  ```

  That refusal never reaches the wire: the server declared `Retain Available`
  0 in CONNACK and the client answers with the code the server *would* have
  sent.
* **What a completion certifies, in the type.** `Completion.kind` is `"sent"`
  at QoS 0, `"acknowledged"` on a PUBACK, `"complete"` on a PUBCOMP and
  `"refused"` on a PUBREC of 0x80 or above. None of them certifies durability
  and none reaches past this hop.
* **Sessions are objects.** A `Session` outlives every connection made on it,
  which is what makes a reconnect a *resumption*: the unacknowledged QoS 1 and
  2 exchanges live there and are retransmitted once, just after a CONNACK with
  `Session Present` 1. There is no retry timer anywhere.

## What it is not

* **Not a broker.** MQTT's topology is asymmetric — retained storage, the
  session store, subscription routing and the Will's timer are all the
  server's. `docs/libraries/mqtt.md` §0 draws the line row by row.
* **No TLS.** The Rust library has it; this binding turns the feature off,
  because the trust anchors are the caller's `rustls::ClientConfig` and this
  surface has no way to hand one in from Python. `Client.is_encrypted` is
  therefore always `False`, and says so rather than being absent.
* **No automatic reconnect.** A reconnect is a new `connect_session` with the
  session you hold. A loop that did it invisibly would decide the Clean Start
  flag for you, and that flag decides whether messages are lost.
* **The session is in memory.** A process that restarts and reconnects with
  `clean_start=False` will be told `Session Present` 1 by a broker that still
  holds its half, with nothing local to match it against — which
  [MQTT-3.2.2-4] says must close the connection, and which arrives as
  `SessionPresentWithoutState`. The answer is `clean_start=True`. That is a
  cost of MQTT's session model rather than of this binding.

## Building and testing it

```sh
./develop.sh              # virtualenv, maturin develop, pytest
./package.sh              # the abi3 wheel, proved in a venv with no Rust
```

The interop tests need a broker and skip with the command that starts one:

```sh
cargo install rumqttd --version 0.20.0
rumqttd -c ../weida-mqtt/tests/interop/rumqttd.toml -q
```
