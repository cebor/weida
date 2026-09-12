//! A subscription: one `SUB`, and an async iterator over what it receives.
//!
//! ```python
//! orders = await nats.subscribe("orders.>")
//! async for message in orders:
//!     print(message.subject_str, message.payload)
//! ```
//!
//! # Why the end of the stream is `StopAsyncIteration`
//!
//! A subscription ends when it is unsubscribed, when an auto-unsubscribe
//! count is reached, or when the connection goes away — a Core NATS
//! subscription has no existence apart from its connection. The library says
//! so by returning `None` from `next`, which for an `async for` is not an
//! error but the end of the iteration, and Python spells that end with an
//! exception. [`crate::errors::to_py`] is where the two meet, exactly as
//! `weida_zmq`'s monitor does it.
//!
//! `next()` itself returns `None` instead, because a caller who wrote
//! `message = await subscription.next()` asked a question rather than
//! started a loop.
//!
//! # One reader at a time
//!
//! `weida_nats::Subscription::next` takes `&mut self` — it owns the
//! receiving half of the subscription's channel — so two coroutines cannot
//! hold it at once. [`crate::lease`] is how that becomes a queue rather than
//! a borrow error, and how a cancelled `next` gives the subscription back.

use std::sync::Arc;

use pyo3::prelude::*;
use weida_nats::Subscription;
use weida_py_core::{Bridge, Errno, py_bytes};

use crate::connection::Reactor;
use crate::errors::{STOP_ASYNC_ITERATION, WOULD_BLOCK, to_py};
use crate::lease::Slot;
use crate::values::PyMessage;

/// `weida_nats.Subscription`.
#[pyclass(frozen, name = "Subscription", module = "weida_nats")]
pub struct PySubscription {
    held: Arc<Slot<Subscription>>,
    bridge: Bridge,
    /// Kept here rather than read through the lease, because these three
    /// never change and a getter that had to wait for a coroutine parked in
    /// `next` would be a getter that hangs.
    subject: Vec<u8>,
    queue_group: Option<Vec<u8>>,
    sid: u64,
    /// The connection's reactor, held so that a program which keeps a
    /// subscription and drops the connection object still has the runtime
    /// its messages arrive on.
    _reactor: Reactor,
}

impl PySubscription {
    /// Wraps a live subscription.
    pub fn of(subscription: Subscription, bridge: Bridge, reactor: Reactor) -> PySubscription {
        PySubscription {
            subject: subscription.subject().to_vec(),
            queue_group: subscription.queue_group().map(<[u8]>::to_vec),
            sid: subscription.sid(),
            held: Slot::new(subscription),
            bridge,
            _reactor: reactor,
        }
    }
}

#[pymethods]
impl PySubscription {
    /// The subject or pattern this subscription asked for, as octets.
    fn subject<'py>(&self, py: Python<'py>) -> Bound<'py, pyo3::types::PyBytes> {
        py_bytes(py, &self.subject)
    }

    /// The queue group this subscription joined, where it joined one.
    fn queue_group<'py>(&self, py: Python<'py>) -> Option<Bound<'py, pyo3::types::PyBytes>> {
        self.queue_group.as_deref().map(|group| py_bytes(py, group))
    }

    /// The `sid` this client chose, which every `MSG` for this subscription
    /// carries.
    fn sid(&self) -> u64 {
        self.sid
    }

    fn __aiter__(slf: PyRef<'_, PySubscription>) -> PyRef<'_, PySubscription> {
        slf
    }

    /// The next message, or `StopAsyncIteration` once the subscription has
    /// ended.
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.held);
        self.bridge.awaitable(py, async move {
            match slot.acquire().await.next().await {
                Some(message) => Ok(PyMessage::of(message)),
                None => Err(Errno::new(
                    STOP_ASYNC_ITERATION,
                    "the subscription has ended, so no further message can arrive on it",
                )),
            }
        })
    }

    /// The next message, waiting for one, or `None` once the subscription
    /// has ended.
    ///
    /// Unbounded on purpose: the protocol gives a subscription no deadline,
    /// and a caller that wants one wraps this in `asyncio.wait_for`, which
    /// cancels it and gives the subscription back.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.held);
        self.bridge.awaitable(py, async move {
            Ok(slot.acquire().await.next().await.map(PyMessage::of))
        })
    }

    /// The next message if one is already queued, and `None` if not.
    ///
    /// Not a coroutine: there is nothing to await. It raises
    /// `BlockingIOError` — Python's own `EAGAIN` — when another coroutine is
    /// reading this subscription, because returning `None` would say "no
    /// message" about a subscription that may well have one.
    fn try_next(&self, py: Python<'_>) -> PyResult<Option<PyMessage>> {
        let Some(mut subscription) = self.held.try_acquire() else {
            return Err(to_py(
                py,
                &Errno::new(
                    WOULD_BLOCK,
                    "another coroutine is reading this subscription, and this call was told \
                     not to wait",
                ),
            ));
        };
        Ok(subscription.try_next().map(PyMessage::of))
    }

    /// `UNSUB <sid>`: removes the subscription now.
    ///
    /// Messages the server had already written are still in the queue and
    /// still readable: dropping them would be pretending the `UNSUB`
    /// travelled back in time.
    fn unsubscribe<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.held);
        self.bridge.awaitable(py, async move {
            slot.acquire().await.unsubscribe().await;
            Ok(())
        })
    }

    /// `UNSUB <sid> <max_msgs>`: removes the subscription once it has
    /// received `max_msgs` messages in total.
    ///
    /// The count is the total on this subscription, as the server counts it,
    /// so a subscription that has already had that many ends at once.
    fn unsubscribe_after<'py>(
        &self,
        py: Python<'py>,
        max_msgs: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let slot = Arc::clone(&self.held);
        self.bridge.awaitable(py, async move {
            slot.acquire().await.unsubscribe_after(max_msgs).await;
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_nats.Subscription {:?} sid={}{}>",
            String::from_utf8_lossy(&self.subject),
            self.sid,
            self.queue_group
                .as_deref()
                .map_or_else(String::new, |group| {
                    format!(" queue_group={:?}", String::from_utf8_lossy(group))
                })
        )
    }
}
