//! I/O-free core model of the weida messaging framework.
//!
//! This crate contains no networking, no `tokio` and no serialization: only the
//! vocabulary types (identifiers, endpoint addresses, policies, limits, trace
//! context, errors) and the pure state machines that decide transfer outcomes.
//! Everything here is unit-testable without a socket, per Phase 1 of the master
//! architecture document.
