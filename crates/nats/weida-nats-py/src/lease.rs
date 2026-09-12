//! One subscription, leased to one coroutine at a time.
//!
//! # Why not a mutex
//!
//! A [`weida_nats::Subscription`] owns the receiving half of its message
//! channel, so `next`, `unsubscribe` and `unsubscribe_after` all take
//! `&mut self` — which is right: two readers of one subscription would be two
//! halves of a channel that has one. The obvious answer, a
//! `tokio::sync::Mutex`, would work here (a `Subscription` is `Send + Sync`)
//! and is still not what this is: a `MutexGuard` borrows, and a future
//! holding a borrow of a `#[pyclass]`'s field cannot be `'static`, which is
//! what [`Bridge::awaitable`](weida_py_core::Bridge::awaitable) requires of
//! everything it spawns.
//!
//! What is allowed is exactly what `Send` means: **move** the subscription to
//! the task that will use it, one task at a time. That is what a [`Lease`] is
//! — it *owns* the subscription while it holds it, rather than borrowing it —
//! and it is why a future holding one is `'static`.
//!
//! It is also what turns "two coroutines read one subscription" from
//! undefined ordering into a queue: the second waits for the first, and a
//! call that was told not to wait ([`Slot::try_acquire`]) says so instead.
//!
//! # Cancellation returns the subscription
//!
//! A cancelled `asyncio.Task` drops the Rust future, which drops the lease,
//! and [`Lease`]'s `Drop` puts the subscription back in its [`Slot`] and
//! wakes whoever is waiting. That is what keeps "a cancelled `next` leaves
//! the subscription usable" true without the binding doing anything at the
//! call site — a message already in the queue is still in the queue — and it
//! is why the lease owns an `Option` rather than a subscription: the `Drop`
//! has to be able to take it.
//!
//! Copied in shape from `crates/zmq/weida-zmq-py/src/lease.rs`, which solves
//! the same problem for a socket that is `!Sync`.

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// Where a subscription lives when nobody is using it.
pub struct Slot<S> {
    /// `None` while a [`Lease`] holds it. A `std::sync::Mutex`, never held
    /// across an `await`: it guards a move, not an operation.
    held: Mutex<Option<S>>,
    /// Woken every time a subscription is returned.
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

    /// Takes the subscription, waiting for whoever has it to give it back.
    ///
    /// The wait is unbounded on purpose: it ends when the operation in front
    /// finishes or is cancelled, and putting a timeout on *queueing* would
    /// turn a slow publisher into a spurious failure the protocol never
    /// named. A caller that wants a bound puts it on its own `await`
    /// (`asyncio.wait_for`), which cancels this one.
    pub async fn acquire(self: &Arc<Slot<S>>) -> Lease<S> {
        loop {
            // Subscribe before looking: a subscription returned between the
            // two must wake this wait rather than be missed by it.
            let returned = self.returned.notified();
            if let Some(lease) = self.try_acquire() {
                return lease;
            }
            returned.await;
        }
    }

    /// Takes the subscription if it is free, and does not wait if it is not.
    pub fn try_acquire(self: &Arc<Slot<S>>) -> Option<Lease<S>> {
        let held = self.lock().take()?;
        Some(Lease {
            slot: Arc::clone(self),
            held: Some(held),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<S>> {
        self.held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Exclusive ownership of the subscription, given back when this is dropped.
pub struct Lease<S: Send + 'static> {
    slot: Arc<Slot<S>>,
    /// Always `Some` until `Drop`.
    held: Option<S>,
}

impl<S: Send + 'static> Deref for Lease<S> {
    type Target = S;

    fn deref(&self) -> &S {
        self.held.as_ref().expect("a lease holds its value")
    }
}

impl<S: Send + 'static> DerefMut for Lease<S> {
    fn deref_mut(&mut self) -> &mut S {
        self.held.as_mut().expect("a lease holds its value")
    }
}

impl<S: Send + 'static> Drop for Lease<S> {
    fn drop(&mut self) {
        if let Some(held) = self.held.take() {
            *self.slot.lock() = Some(held);
            self.slot.returned.notify_one();
        }
    }
}
