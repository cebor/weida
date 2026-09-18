# NNG / SP v1 — retired mapping

Status: rejected architecture record
Date: 2026-09-11
Retired: 2026-09-18

The former document mapped SP protocols, topic conventions, transfer points and guarantees onto
weida patterns. That global mapping is not a product contract and has been removed under
[0013](../decisions/0013-competitor-libraries.md).

Current sources:

- [`../research/nanomsg-nng.md`](../research/nanomsg-nng.md) records SP and NNG behavior.
- [`../libraries/nng.md`](../libraries/nng.md) records `weida-sp` and `weida-nng` parity
  against NNG.

An application may explicitly compose `weida-nng` and `weida`. A future broker Connector may
operate one explicitly configured SP source or sink attached to a Queue. Neither case
establishes a protocol-wide protocol-to-pattern equivalence.
