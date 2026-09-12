//! One socket, leased to one coroutine at a time.
//!
//! # Why not a mutex
//!
//! A `weida-zmq` socket is `Send` and **not** `Sync` — libzmq's thread rule as
//! a type — and its `send` and `recv` take `&mut self`. The obvious answer, a
//! `tokio::sync::Mutex`, does not compile and should not: a `MutexGuard` hands
//! out `&T`, so sending one to another thread would put a shared reference to
//! a `!Sync` value there, and tokio's guard is `Send` only for `T: Send +
//! Sync`. A future holding such a guard across an `await` is not `Send` and
//! cannot be spawned on a multi-thread reactor.
//!
//! What is allowed is exactly what `Send` means: **move** the socket to the
//! thread that will use it, one thread at a time. That is what a [`Lease`] is
//! — it *owns* the socket while it holds it, rather than borrowing it — and it
//! is why a future holding one is `Send` even though the socket is not `Sync`.
//!
//! # Cancellation returns the socket
//!
//! A cancelled `asyncio.Task` drops the Rust future, which drops the lease,
//! and [`Lease`]'s `Drop` puts the socket back in its [`Slot`] and wakes
//! whoever is waiting. That is what keeps "a cancelled receive leaves the
//! socket usable" true without the binding doing anything at the call site,
//! and it is why the lease owns an `Option` rather than a socket: the `Drop`
//! has to be able to take it.

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// Where a socket lives when nobody is using it.
pub struct Slot<S> {
    /// `None` while a [`Lease`] holds the socket. A `std::sync::Mutex`, never
    /// held across an `await`: it guards a move, not an operation.
    socket: Mutex<Option<S>>,
    /// Woken every time a socket is returned.
    returned: Notify,
}

impl<S: Send + 'static> Slot<S> {
    /// A slot holding `socket`.
    pub fn new(socket: S) -> Arc<Slot<S>> {
        Arc::new(Slot {
            socket: Mutex::new(Some(socket)),
            returned: Notify::new(),
        })
    }

    /// Takes the socket, waiting for whoever has it to give it back.
    ///
    /// The wait is unbounded on purpose: it ends when the operation in front
    /// finishes or is cancelled, and putting a timeout on *queueing* would
    /// turn a slow peer into a spurious `EAGAIN` that no libzmq option asked
    /// for. A caller that wants a bound puts it on its own `await`
    /// (`asyncio.wait_for`), which cancels this one.
    pub async fn acquire(self: &Arc<Slot<S>>) -> Lease<S> {
        loop {
            // Subscribe before looking: a socket returned between the two must
            // wake this wait rather than be missed by it.
            let returned = self.returned.notified();
            if let Some(lease) = self.try_acquire() {
                return lease;
            }
            returned.await;
        }
    }

    /// Takes the socket if it is free, and does not wait if it is not.
    pub fn try_acquire(self: &Arc<Slot<S>>) -> Option<Lease<S>> {
        let socket = self.lock().take()?;
        Some(Lease {
            slot: Arc::clone(self),
            socket: Some(socket),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<S>> {
        self.socket
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Exclusive ownership of the socket, given back when this is dropped.
pub struct Lease<S: Send + 'static> {
    slot: Arc<Slot<S>>,
    /// Always `Some` until `Drop`.
    socket: Option<S>,
}

impl<S: Send + 'static> Deref for Lease<S> {
    type Target = S;

    fn deref(&self) -> &S {
        self.socket.as_ref().expect("a lease holds its socket")
    }
}

impl<S: Send + 'static> DerefMut for Lease<S> {
    fn deref_mut(&mut self) -> &mut S {
        self.socket.as_mut().expect("a lease holds its socket")
    }
}

impl<S: Send + 'static> Drop for Lease<S> {
    fn drop(&mut self) {
        if let Some(socket) = self.socket.take() {
            *self.slot.lock() = Some(socket);
            self.slot.returned.notify_one();
        }
    }
}
