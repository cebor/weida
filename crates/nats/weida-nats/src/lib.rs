//! A NATS core client in Rust, usable with no weida in the picture.
//!
//! This crate is a **product, not an adapter**: it implements the client half
//! of the NATS client protocol for applications that speak NATS, rather than
//! bridging NATS into weida
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4, §4.5).
//!
//! What it shares with weida is the machine room: `weida-runtime`'s reactor,
//! resolver and close budget, and `weida-core`'s error vocabulary at the
//! boundary. `[dependencies]` names `weida-runtime`, `weida-core` and
//! `weida-nats-codec`, and **never** `weida` or `weida-protocol` [0013 §4.2].
//!
//! # Client, not server
//!
//! Routes, gateways, leaf nodes and JetStream are server features
//! ([0014](../../../docs/decisions/0014-parallel-libraries.md) §2), so this
//! crate is Core NATS as a client sees it: `INFO`/`CONNECT`, `PING`/`PONG`,
//! publish, subscribe with queue groups, and request-reply over an inbox. A
//! queue group is not a broker queue — it is a set of subscriptions sharing
//! one eligible delivery per publication — and that is a client-side concept
//! the protocol defines for a client, so it belongs here.
//!
//! # Sources
//!
//! Every number and every quoted rule comes from `docs/research/nats.md`,
//! which cites the [client protocol
//! reference](https://docs.nats.io/reference/protocols/client) and the NATS
//! ADRs.

#![warn(missing_docs)]
