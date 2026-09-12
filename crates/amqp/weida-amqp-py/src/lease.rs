//! One handle, leased to one coroutine at a time.
//!
//! # Why not a mutex
//!
//! A `weida-amqp` [`Session`](weida_amqp::Session) and
//! [`Link`](weida_amqp::Link) own the receiving half of a channel, so reading
//! an event takes `&mut self` and neither type is `Sync`. The obvious answer,
//! a `tokio::sync::Mutex`, is the wrong one: its guard hands out `&T`, so it
//! is `Send` only for `T: Send + Sync`, and a future holding one across an
//! `await` could not be spawned on a multi-thread reactor.
//!
//! What is allowed is exactly what `Send` means: **move** the value to the
//! thread that will use it, one thread at a time. That is what a [`Lease`] is
//! — it *owns* the value while it holds it — and it is why a future holding
//! one is `Send` even though the value is not `Sync`.
//!
//! # Cancellation gives the handle back
//!
//! A cancelled `asyncio.Task` drops the Rust future, which drops the lease,
//! and [`Lease`]'s `Drop` returns the value to its [`Slot`] and wakes whoever
//! is waiting. That is what keeps "a cancelled receive leaves the link usable"
//! true without the binding doing anything at the call site, and it is why the
//! lease holds an `Option`: the `Drop` has to be able to take it.
//!
//! The shape is `weida-zmq-py`'s `lease.rs`, for the same reason and with the
//! same consequences. It is duplicated rather than shared because it is eleven
//! lines of mechanism over a type parameter and `weida-py-core` is the place
//! for what every binding needs — a third binding that wants it moves it
//! there.

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// Where a handle lives when nobody is using it.
pub struct Slot<S> {
    /// `None` while a [`Lease`] holds it. A `std::sync::Mutex`, never held
    /// across an `await`: it guards a move, not an operation.
    held: Mutex<Option<S>>,
    /// Woken every time a handle is returned.
    returned: Notify,
}

impl<S: Send + 'static> Slot<S> {
    /// A slot holding `value`.
    pub fn new(value: S) -> Arc<Slot<S>> {
        Arc::new(Slot {
            held: Mutex::new(Some(value)),
            returned: Notify::new(),
        })
    }

    /// Takes the handle, waiting for whoever has it to give it back.
    ///
    /// The wait is unbounded on purpose: it ends when the operation in front
    /// finishes or is cancelled. A caller that wants a bound puts it on its
    /// own `await` — `asyncio.wait_for` — which cancels this one.
    pub async fn acquire(self: &Arc<Slot<S>>) -> Lease<S> {
        loop {
            // Subscribe before looking: a handle returned between the two must
            // wake this wait rather than be missed by it.
            let returned = self.returned.notified();
            if let Some(lease) = self.try_acquire() {
                return lease;
            }
            returned.await;
        }
    }

    /// Takes the handle if it is free, and does not wait if it is not.
    pub fn try_acquire(self: &Arc<Slot<S>>) -> Option<Lease<S>> {
        let value = self.lock().take()?;
        Some(Lease {
            slot: Arc::clone(self),
            value: Some(value),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<S>> {
        self.held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Exclusive ownership of the handle, given back when this is dropped.
pub struct Lease<S: Send + 'static> {
    slot: Arc<Slot<S>>,
    /// Always `Some` until `Drop`.
    value: Option<S>,
}

impl<S: Send + 'static> Deref for Lease<S> {
    type Target = S;

    fn deref(&self) -> &S {
        self.value.as_ref().expect("a lease holds its value")
    }
}

impl<S: Send + 'static> DerefMut for Lease<S> {
    fn deref_mut(&mut self) -> &mut S {
        self.value.as_mut().expect("a lease holds its value")
    }
}

impl<S: Send + 'static> Drop for Lease<S> {
    fn drop(&mut self) {
        if let Some(value) = self.value.take() {
            *self.slot.lock() = Some(value);
            self.slot.returned.notify_one();
        }
    }
}
