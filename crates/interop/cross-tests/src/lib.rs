//! No library code: this crate is a home for tests that belong to no single
//! adapter.
//!
//! `weida-zmq-bridge` and `weida-nng-bridge` each know one foreign protocol
//! and nothing of the other, which is what keeps either checkable against its
//! own specification (`docs/ARCHITECTURE.md` §4). A chain that runs a message
//! in through one and out through the other therefore belongs to neither
//! crate, and lives here: `tests/cross.rs`, Phase B slice 6 of
//! `docs/LOOP.md` §9.
