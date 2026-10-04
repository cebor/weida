//! Pub/Sub: fan-out that never waits, and the streamed publish that makes the
//! byte budget a chunk's rather than a payload's.
//!
//! # Two write forms, and a binding may not pick one for the caller
//!
//! `Publisher.publish(topic, payload)` is the whole-message call and refuses
//! a payload above `Limits::subscriber_buffer_bytes`, because such a message
//! could not be enqueued for anybody. `Publisher.open(topic)` is the other
//! half ([PATTERNS.md](../../../../docs/PATTERNS.md) §4.1): the budget bounds
//! a **chunk**, so the payload has no ceiling — and the two ways of writing a
//! chunk mean different things, so both are here:
//!
//! | Python | What it does |
//! | --- | --- |
//! | `await fan.write_within(chunk, seconds)` | waits up to `seconds` for a subscriber with no room, then drops **that** subscriber's copy. The bound is mandatory and finite, for the reason a drain's is. |
//! | `await fan.write_now(chunk)` | never waits for room: a subscriber without any right now loses the transfer. Fan-out's `Drop` in its purest form, for a signal where a later chunk supersedes an earlier one. |
//!
//! A binding that offered only the first would make a video publisher wait on
//! its slowest viewer; one that offered only the second would make a payload
//! larger than the budget undeliverable to anybody, because the publisher
//! outruns its own budget and aborts every copy. The choice stays the
//! caller's, as it is in Rust.
//!
//! `write_now` is a coroutine even though it never waits on a subscriber: two
//! coroutines may hold the same `FanOut`, and the lock that keeps them from
//! writing into it at once is the only thing it can wait for. Dropping a
//! `FanOut` without `finish()` resets every copy, so no subscriber mistakes a
//! partial payload for a whole one.

use std::sync::Arc;

use pyo3::prelude::*;
use weida::{FanOut, Publisher, Runtime, Subscriber};
use weida_py_core::{Bridge, Errno, payload_of};

use crate::errors::{errno_of, raise, to_py};
use crate::values::PyIncomingMeta;

/// `weida.Publisher`: fan-out to every subscriber whose filter matches.
#[pyclass(frozen, name = "Publisher", module = "weida")]
pub struct PyPublisher {
    publisher: Arc<Publisher>,
    bridge: Bridge,
    /// Held so the reactor outlives this endpoint.
    _runtime: Arc<Runtime>,
}

impl PyPublisher {
    pub(crate) fn new(publisher: Publisher, bridge: Bridge, runtime: Arc<Runtime>) -> PyPublisher {
        PyPublisher {
            publisher: Arc::new(publisher),
            bridge,
            _runtime: runtime,
        }
    }
}

#[pymethods]
impl PyPublisher {
    /// The endpoint path this publisher serves.
    fn path(&self) -> String {
        self.publisher.path().to_owned()
    }

    /// Fans `payload` out to every matching subscriber and returns how many
    /// it was enqueued for.
    ///
    /// Not a coroutine, because a publish never waits for a subscriber: a
    /// slow one loses the message and is counted, which is the one place
    /// weida answers overload by discarding.
    ///
    /// # Errors
    ///
    /// `weida.LimitExceeded` for a payload above
    /// `Limits::subscriber_buffer_bytes` — such a message could not be
    /// enqueued for anybody, so it is an error rather than a silent drop for
    /// everyone. `open` is the call with no such ceiling.
    fn publish(&self, py: Python<'_>, topic: &str, payload: &Bound<'_, PyAny>) -> PyResult<usize> {
        let body = payload_of(payload)?;
        raise(py, self.publisher.publish(topic, body))
    }

    /// Opens a streamed publish: one stream per matched subscriber, written
    /// chunk by chunk and never held whole.
    ///
    /// The subscriber set is fixed here: one that arrives mid-payload would
    /// receive a fragment with no way to know it, so it gets the next
    /// message.
    fn open(&self, topic: &str) -> PyFanOut {
        PyFanOut {
            topic: topic.to_owned(),
            fan: Arc::new(tokio::sync::Mutex::new(Some(self.publisher.open(topic)))),
            bridge: self.bridge.clone(),
            _runtime: Arc::clone(&self._runtime),
        }
    }

    /// Subscribers currently connected.
    fn subscriber_count(&self) -> usize {
        self.publisher.subscriber_count()
    }

    /// Filters registered on this publisher, summed over subscribers.
    ///
    /// What a caller waits on before publishing: a subscription is recorded
    /// in the frame-processing task, so it becomes visible a moment after
    /// `subscribe` returns, and polling this is exact where a sleep is a
    /// guess.
    fn filter_count(&self) -> usize {
        self.publisher.filter_count()
    }

    /// Copies dropped because a subscriber could not take them, summed over
    /// topics and causes.
    fn dropped(&self) -> u64 {
        self.publisher.dropped()
    }

    /// What was dropped on `topic` as
    /// `(total, subscriber_budget, subscriber_queue, no_parked_connection)`,
    /// or `None` if nothing was.
    ///
    /// Three causes rather than one number, because they are three different
    /// failures: the first two are the subscriber not keeping up, the third
    /// is a local transport having no connection to carry the copy.
    fn dropped_on(&self, topic: &str) -> Option<(u64, u64, u64, u64)> {
        self.publisher.dropped_on(topic).map(|drops| {
            (
                drops.total(),
                drops.subscriber_budget,
                drops.subscriber_queue,
                drops.no_parked_connection,
            )
        })
    }

    fn __repr__(&self) -> String {
        format!("<weida.Publisher {}>", self.publisher.path())
    }
}

/// `weida.FanOut`: a publish in progress, written chunk by chunk.
#[pyclass(frozen, name = "FanOut", module = "weida")]
pub struct PyFanOut {
    topic: String,
    /// `None` once finished: a transfer ends once. A `tokio::sync::Mutex`
    /// because the guard is held across an await, which a `std` one may not
    /// be.
    fan: Arc<tokio::sync::Mutex<Option<FanOut>>>,
    bridge: Bridge,
    _runtime: Arc<Runtime>,
}

/// What a call on a finished transfer gets, rather than a panic.
fn finished() -> Errno {
    Errno::new(
        "NoReply",
        "this fan-out was already finished; a transfer ends once",
    )
}

#[pymethods]
impl PyFanOut {
    /// The topic this transfer is published on.
    #[getter]
    fn topic(&self) -> &str {
        &self.topic
    }

    /// Subscribers still receiving this transfer. It only falls: one that
    /// loses a chunk is gone from this transfer, and one that subscribes
    /// while it is in flight receives the next message.
    fn subscribers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let fan = Arc::clone(&self.fan);
        self.bridge.awaitable(py, async move {
            let guard = fan.lock().await;
            guard.as_ref().map(FanOut::subscribers).ok_or_else(finished)
        })
    }

    /// Writes the next chunk, waiting up to `seconds` for a subscriber with
    /// no room, and returns how many subscribers are left.
    ///
    /// # Errors
    ///
    /// `weida.LimitExceeded` for a chunk above
    /// `Limits::subscriber_buffer_bytes`: the *payload* need not fit, a chunk
    /// must.
    fn write_within<'py>(
        &self,
        py: Python<'py>,
        chunk: &Bound<'py, PyAny>,
        seconds: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let body = payload_of(chunk)?;
        let limit = std::time::Duration::try_from_secs_f64(seconds).map_err(|e| {
            to_py(
                py,
                &errno_of(weida::Error::Runtime(format!("write_within: {e}"))),
            )
        })?;
        let fan = Arc::clone(&self.fan);
        self.bridge.awaitable(py, async move {
            let mut guard = fan.lock().await;
            let fan = guard.as_mut().ok_or_else(finished)?;
            fan.write_within(body, limit).await.map_err(errno_of)
        })
    }

    /// Writes the next chunk without waiting for room: a subscriber that has
    /// none loses the transfer.
    fn write_now<'py>(
        &self,
        py: Python<'py>,
        chunk: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let body = payload_of(chunk)?;
        let fan = Arc::clone(&self.fan);
        self.bridge.awaitable(py, async move {
            let mut guard = fan.lock().await;
            let fan = guard.as_mut().ok_or_else(finished)?;
            fan.write_now(body).map_err(errno_of)
        })
    }

    /// Ends the transfer and returns how many subscribers received all of it
    /// as far as this side can tell.
    ///
    /// "As far as this side can tell" is the honest claim: a fan-out copy
    /// carries no receipt, so the transport acknowledgement is the drain's
    /// business and nobody else's.
    fn finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let fan = Arc::clone(&self.fan);
        self.bridge.awaitable(py, async move {
            let taken = fan.lock().await.take().ok_or_else(finished)?;
            Ok(taken.finish())
        })
    }

    fn __repr__(&self) -> String {
        format!("<weida.FanOut {}>", self.topic)
    }
}

/// `weida.Subscriber`: whole published messages, one call each.
#[pyclass(frozen, name = "Subscriber", module = "weida")]
pub struct PySubscriber {
    subscriber: Arc<Subscriber>,
    bridge: Bridge,
    _runtime: Arc<Runtime>,
}

impl PySubscriber {
    pub(crate) fn new(
        subscriber: Subscriber,
        bridge: Bridge,
        runtime: Arc<Runtime>,
    ) -> PySubscriber {
        PySubscriber {
            subscriber: Arc::new(subscriber),
            bridge,
            _runtime: runtime,
        }
    }
}

#[pymethods]
impl PySubscriber {
    /// Dials `url`. See `Requester.connect` for the address forms.
    fn connect<'py>(&self, py: Python<'py>, url: String) -> PyResult<Bound<'py, PyAny>> {
        let subscriber = Arc::clone(&self.subscriber);
        self.bridge.awaitable(py, async move {
            subscriber.connect(&url).await.map_err(errno_of)
        })
    }

    /// One `weida.ConnectionStats` per live connection; see
    /// `Requester.connection_stats`.
    fn connection_stats(&self) -> Vec<crate::values::PyConnectionStats> {
        crate::values::PyConnectionStats::all(&self.subscriber.connection_stats())
    }

    /// Subscribes to `filter`; the empty filter takes every topic.
    ///
    /// The grammar is the segmented one of
    /// [PROTOCOL.md](../../../../docs/PROTOCOL.md) §6.4 — `.` separates,
    /// `*` is exactly one segment and a trailing `#` is zero or more — so
    /// `px.*` selects `px.eur` and not `px.eur.spot`.
    ///
    /// # Errors
    ///
    /// `weida.Protocol` for a filter the grammar rejects,
    /// `weida.LimitExceeded` past `max_subscriptions`.
    fn subscribe<'py>(&self, py: Python<'py>, filter: String) -> PyResult<Bound<'py, PyAny>> {
        let subscriber = Arc::clone(&self.subscriber);
        self.bridge.awaitable(py, async move {
            subscriber.subscribe(&filter).await.map_err(errno_of)
        })
    }

    /// Withdraws one filter.
    fn unsubscribe<'py>(&self, py: Python<'py>, filter: String) -> PyResult<Bound<'py, PyAny>> {
        let subscriber = Arc::clone(&self.subscriber);
        self.bridge.awaitable(py, async move {
            subscriber.unsubscribe(&filter).await.map_err(errno_of)
        })
    }

    /// Waits for the next published message, at most `max_bytes`, and
    /// returns `(payload, meta)`.
    ///
    /// The topic is on the metadata, and so is `missed`: under `PerProducer`
    /// ordering a fan-out drop reaches the subscriber as a number rather than
    /// as silence.
    fn recv<'py>(&self, py: Python<'py>, max_bytes: usize) -> PyResult<Bound<'py, PyAny>> {
        let subscriber = Arc::clone(&self.subscriber);
        self.bridge.awaitable(py, async move {
            let transfer = subscriber.recv().await.map_err(errno_of)?;
            let meta = PyIncomingMeta::of(transfer.meta());
            let payload = transfer.collect(max_bytes).await.map_err(errno_of)?;
            Ok((payload, meta))
        })
    }

    /// Waits for the next published message and returns it as a stream, for
    /// a payload too large to hold: `(stream, meta)`.
    fn recv_stream<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let subscriber = Arc::clone(&self.subscriber);
        let bridge = self.bridge.clone();
        let runtime = Arc::clone(&self._runtime);
        self.bridge.awaitable(py, async move {
            let transfer = subscriber.recv().await.map_err(errno_of)?;
            let meta = PyIncomingMeta::of(transfer.meta());
            Ok((
                crate::streams::PyIncomingStream::new(transfer, bridge, runtime),
                meta,
            ))
        })
    }

    /// Peers this subscriber is connected to.
    fn peer_count(&self) -> usize {
        self.subscriber.peer_count()
    }

    fn __repr__(&self) -> String {
        "<weida.Subscriber>".to_owned()
    }
}
