# MQTT 5.0 — retired mapping

Status: rejected architecture record
Date: 2026-09-12
Retired: 2026-09-18

The former document mapped MQTT topics, QoS, sessions and transfer points onto weida patterns.
That global mapping is not a product contract and has been removed under
[0013](../decisions/0013-competitor-libraries.md).

Current sources:

- [`../research/mqtt5.md`](../research/mqtt5.md) records MQTT 5.0 behavior.
- [`../libraries/mqtt.md`](../libraries/mqtt.md) records `weida-mqtt-codec` and
  `weida-mqtt` parity against named brokers.

An application may explicitly compose `weida-mqtt` and `weida`. A future broker Connector may
operate one explicitly configured MQTT source or sink attached to a Queue. Neither case
establishes a protocol-wide MQTT-to-weida equivalence.
