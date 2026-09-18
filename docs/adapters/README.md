# Retired protocol mappings

Status: rejected architecture record

This directory is retained so historical decisions, backlog entries and release notes keep
stable links. Its former documents defined global mappings between foreign protocol primitives
and weida patterns. That product category was rejected by
[0013](../decisions/0013-competitor-libraries.md).

The current boundaries are:

- [`../libraries/`](../libraries/README.md) documents standalone foreign-protocol libraries on
  their own terms.
- [`../research/`](../research/README.md) records protocol facts and upstream behavior.
- Applications may compose public libraries explicitly and own that conversion.
- Broker integration, when implemented, is an explicit managed Connector resource attached to
  a Queue. Its resource specification owns one concrete conversion policy; it does not create a
  protocol-wide socket-to-pattern equivalence.
- ZeroMQ-native devices remain in `weida_zmq::proxy` and operate only on ZeroMQ sockets.

The former mapping tables are intentionally removed. These files are rejection records, not
specifications or roadmap.

## Records

- [`zmtp.md`](zmtp.md) — retired ZMTP-to-weida mapping
- [`nng.md`](nng.md) — retired SP-to-weida mapping
- [`mqtt5.md`](mqtt5.md) — retired MQTT-to-weida mapping
