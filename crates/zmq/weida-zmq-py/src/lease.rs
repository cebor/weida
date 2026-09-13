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
//!
//! # A split socket leaves its slot for good
//!
//! `sock.split()` consumes the Rust socket and hands its two halves to two
//! new slots. The original slot is then **retired**: it will never hold a
//! socket again, and a caller that still uses the old object gets
//! `ENOTSOCK` naming the split rather than a wait that never ends.

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use weida_py_core::Errno;

/// Where a socket lives when nobody is using it.
pub struct Slot<S> {
    /// `None` while a [`Lease`] holds the socket, or for good once
    /// retired. A `std::sync::Mutex`, never held across an `await`: it
    /// guards a move, not an operation.
    socket: Mutex<Occupant<S>>,
    /// Woken every time a socket is returned, and once when the slot
    /// retires.
    returned: Notify,
}

/// What a slot holds.
enum Occupant<S> {
    /// The socket, free to be leased.
    Present(S),
    /// A lease holds it.
    Leased,
    /// It was split into halves and will not come back.
    Retired,
}

impl<S: Send + 'static> Slot<S> {
    /// A slot holding `socket`.
    pub fn new(socket: S) -> Arc<Slot<S>> {
        Arc::new(Slot {
            socket: Mutex::new(Occupant::Present(socket)),
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
    ///
    /// # Errors
    ///
    /// `ENOTSOCK` once the slot is retired: the socket was split and lives
    /// on in its halves.
    pub async fn acquire(self: &Arc<Slot<S>>) -> Result<Lease<S>, Errno> {
        loop {
            // Subscribe before looking: a socket returned between the two must
            // wake this wait rather than be missed by it.
            let returned = self.returned.notified();
            if let Some(lease) = self.try_acquire()? {
                return Ok(lease);
            }
            returned.await;
        }
    }

    /// Takes the socket if it is free, and does not wait if it is not.
    ///
    /// # Errors
    ///
    /// `ENOTSOCK` once the slot is retired.
    pub fn try_acquire(self: &Arc<Slot<S>>) -> Result<Option<Lease<S>>, Errno> {
        let mut occupant = self.lock();
        match std::mem::replace(&mut *occupant, Occupant::Leased) {
            Occupant::Present(socket) => Ok(Some(Lease {
                slot: Arc::clone(self),
                socket: Some(socket),
            })),
            Occupant::Leased => Ok(None),
            Occupant::Retired => {
                *occupant = Occupant::Retired;
                Err(retired())
            }
        }
    }

    /// Takes the socket out for good, waiting for a lease in front to end.
    ///
    /// What `split` calls: after it the slot is retired and every later
    /// `acquire` is `ENOTSOCK`.
    pub async fn retire(self: &Arc<Slot<S>>) -> Result<S, Errno> {
        let mut lease = self.acquire().await?;
        let socket = lease.socket.take().expect("a lease holds its socket");
        // The lease's `Drop` finds nothing to return; the slot stays
        // `Leased` from `try_acquire`, so mark it.
        *self.lock() = Occupant::Retired;
        self.returned.notify_waiters();
        Ok(socket)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Occupant<S>> {
        self.socket
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn retired() -> Errno {
    Errno::new(
        "ENOTSOCK",
        "this socket was split; use the halves split() returned",
    )
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
            *self.slot.lock() = Occupant::Present(socket);
            self.slot.returned.notify_one();
        }
    }
}
