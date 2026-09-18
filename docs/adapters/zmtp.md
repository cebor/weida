# ZMTP 3.1 — retired mapping

Status: rejected architecture record
Date: 2026-09-11
Retired: 2026-09-18

The former document mapped ZeroMQ socket types, topics, transfer points and guarantees onto
weida patterns. That global mapping is not a product contract and has been removed under
[0013](../decisions/0013-competitor-libraries.md).

Current sources:

- [`../research/zeromq.md`](../research/zeromq.md) records ZeroMQ and ZMTP behavior.
- [`../libraries/zmq.md`](../libraries/zmq.md) records `weida-zmtp` and `weida-zmq` parity
  against libzmq.
- `weida_zmq::proxy` and `proxy_steerable` remain native ZeroMQ device helpers; they move
  ZeroMQ multipart messages between compatible ZeroMQ sockets and never speak weida.

An application may explicitly compose `weida-zmq` and `weida`. A future broker Connector may
operate one explicitly configured ZeroMQ source or sink attached to a Queue. Neither case
establishes a protocol-wide socket-to-pattern equivalence.
