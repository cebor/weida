//! The eleven stable socket types, one Python class each.
//!
//! libzmq has one `Socket` type and a constant to say what it is; this module
//! has eleven classes, because the patterns are not interchangeable and a type
//! error at construction beats `ENOTSUP` at the first send. It is the shape
//! `weida-zmq` already has in Rust, carried across unchanged.
//!
//! # One operation at a time, per socket
//!
//! A `weida-zmq` socket is `Send` and **not** `Sync`, and its `send` and
//! `recv` take `&mut self`: libzmq's "applications MUST NOT use a socket from
//! multiple threads" expressed as a type. So each socket here lives in a
//! [`Slot`](crate::lease::Slot) and every call leases it for as long as the
//! operation runs. Two coroutines that use one socket therefore *queue*; they
//! do not interleave and they do not race.
//!
//! The consequence is worth stating plainly rather than discovering: a task
//! parked in `await sock.recv()` holds the socket, so a concurrent
//! `await sock.send(...)` on that same socket waits for the receive to finish
//! or to be cancelled. Two sockets — which is what the patterns are for — or
//! one coroutine owning the socket are the two ways round it, and both are
//! also what the Rust API requires.
//!
//! # Why every call is a coroutine
//!
//! Even the calls `weida-zmq` answers synchronously (`connect`, `unbind`,
//! `last_endpoint`, `close`) are `await`ed here. They need the socket, the
//! socket may be held by a parked receive, and a *synchronous* Python method
//! that waited for it would block the event-loop thread itself — the one thing
//! an asyncio library must never do. Awaiting costs a coroutine; blocking the
//! loop costs the program.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes};
use weida_py_core::Errno;
use weida_zmq::{Message, Multipart};

use crate::context::Context;
use crate::errors::errno_of;
use crate::lease::Slot;

/// What a `disconnect` threw away, in messages.
///
/// libzmq's `zmq_disconnect` reports nothing at all, so a caller cannot tell a
/// clean disconnect from one that dropped a hundred queued messages. This is
/// that number, in both directions.
#[pyclass(frozen, get_all, name = "Discarded", module = "weida_zmq")]
pub struct Discarded {
    /// Messages queued for the peer and never sent.
    pub outgoing: usize,
    /// Messages received from the peer and never read.
    pub incoming: usize,
}

#[pymethods]
impl Discarded {
    fn __repr__(&self) -> String {
        format!(
            "<weida_zmq.Discarded outgoing={} incoming={}>",
            self.outgoing, self.incoming
        )
    }
}

/// Takes a message from Python: one `bytes` frame, or an iterable of them.
///
/// The copy here is the one `Message`'s owned `Vec<u8>` forces, and it is the
/// only one on this path — PyO3 lends the interpreter's buffer rather than
/// copying it first (`weida_py_core::bytes`).
pub fn message_from(object: &Bound<'_, PyAny>) -> PyResult<Multipart> {
    if object.is_instance_of::<PyBytes>() || object.is_instance_of::<PyByteArray>() {
        return Ok(Multipart::single(Message::from(weida_py_core::payload_of(
            object,
        )?)));
    }
    let mut frames = Vec::new();
    for frame in object.try_iter()? {
        frames.push(Message::from(weida_py_core::payload_of(&frame?)?));
    }
    Multipart::new(frames).map_err(|error| crate::errors::to_py(object.py(), &errno_of(error)))
}

/// Hands a message to Python as a list of `bytes`, one per frame.
pub fn message_into(message: Multipart) -> Vec<Vec<u8>> {
    message
        .into_frames()
        .into_iter()
        .map(Message::into_bytes)
        .collect()
}

/// The class, the constructor and the endpoint surface every socket type has,
/// plus whatever the pattern adds — in **one** `#[pymethods]` block, because a
/// second one needs PyO3's `multiple-pymethods` feature and inventory
/// registration, which an `abi3` wheel should not pay for.
macro_rules! python_socket {
    (
        $class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal,
        { $($pattern:tt)* }
    ) => {
        #[doc = $what]
        #[pyclass(frozen, name = $python, module = "weida_zmq")]
        pub struct $class {
            socket: Arc<Slot<weida_zmq::$rust>>,
            bridge: weida_py_core::Bridge,
        }

        #[pymethods]
        impl $class {
            /// Opens the socket on `context`, against its `ZMQ_MAX_SOCKETS`
            /// ceiling.
            #[new]
            fn new(py: Python<'_>, context: &Context) -> PyResult<$class> {
                let socket = crate::errors::raise(py, weida_zmq::$rust::new(context.inner()))?;
                Ok($class {
                    socket: Slot::new(socket),
                    bridge: context.bridge().clone(),
                })
            }

            /// The socket type this socket announces in its ZMTP `READY`.
            #[getter]
            fn socket_type(&self) -> &'static str {
                $kind
            }

            /// Binds an endpoint and returns the one actually bound.
            ///
            /// `tcp://127.0.0.1:0` comes back with the port the kernel chose,
            /// which is the only way to learn a wildcard port — libzmq's
            /// `ZMQ_LAST_ENDPOINT`.
            fn bind<'py>(&self, py: Python<'py>, endpoint: String) -> PyResult<Bound<'py, PyAny>> {
                let slot = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    slot.acquire()
                        .await
                        .bind(&endpoint)
                        .await
                        .map(|bound| bound.to_string())
                        .map_err(errno_of)
                })
            }

            /// Connects an endpoint. Returns as soon as the peer exists, like
            /// `zmq_connect`: the queue is there and the dialling happens
            /// behind it.
            fn connect<'py>(
                &self,
                py: Python<'py>,
                endpoint: String,
            ) -> PyResult<Bound<'py, PyAny>> {
                let slot = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    slot.acquire().await.connect(&endpoint).map_err(errno_of)
                })
            }

            /// Stops accepting on an endpoint. Peers already accepted there
            /// keep their connections.
            fn unbind<'py>(
                &self,
                py: Python<'py>,
                endpoint: String,
            ) -> PyResult<Bound<'py, PyAny>> {
                let slot = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    slot.acquire().await.unbind(&endpoint).map_err(errno_of)
                })
            }

            /// Disconnects an endpoint and reports what its queues held.
            fn disconnect<'py>(
                &self,
                py: Python<'py>,
                endpoint: String,
            ) -> PyResult<Bound<'py, PyAny>> {
                let slot = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    slot.acquire()
                        .await
                        .disconnect(&endpoint)
                        .map(|discarded| Discarded {
                            outgoing: discarded.outgoing,
                            incoming: discarded.incoming,
                        })
                        .map_err(errno_of)
                })
            }

            /// `ZMQ_LAST_ENDPOINT`: the last endpoint bound or connected, or
            /// `None` when there has been neither.
            fn last_endpoint<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let slot = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    Ok::<Option<String>, Errno>(
                        slot.acquire()
                            .await
                            .last_endpoint()
                            .map(|endpoint| endpoint.to_string()),
                    )
                })
            }

            /// Peers this socket may send to right now.
            fn peer_count<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let slot = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    Ok::<usize, Errno>(slot.acquire().await.peer_count())
                })
            }

            /// `zmq_close`: stops accepting and dialling, and destroys every
            /// queue. The socket's slot under `ZMQ_MAX_SOCKETS` is returned
            /// when Python collects the object.
            fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let slot = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    slot.acquire().await.close();
                    Ok::<(), Errno>(())
                })
            }

            fn __repr__(&self) -> String {
                concat!("<weida_zmq.", $python, " ", $kind, ">").to_owned()
            }

            $($pattern)*
        }
    };
    // The nine socket types whose pattern methods arrive with B-112.
    ($class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal) => {
        python_socket!($class, $rust, $python, $kind, $what, {});
    };
}

/// `send` and `recv` for the two socket types B-058's round trip is made of.
///
/// The other nine get theirs in B-112, together with the `Multipart` value,
/// `send_nowait`/`recv_nowait` and the per-call timeout. This pair is what
/// makes this item's proof — an asyncio REQ/REP exchange between two sockets of
/// this module — something that runs rather than something described.
macro_rules! request_reply_socket {
    ($class:ident, $rust:ident, $python:literal, $kind:literal, $what:literal) => {
        python_socket!($class, $rust, $python, $kind, $what, {
            /// Sends a message: one `bytes` frame, or an iterable of them.
            ///
            /// Bounded by `ZMQ_SNDTIMEO`, which reports `EAGAIN` when it expires.
            fn send<'py>(
                &self,
                py: Python<'py>,
                message: &Bound<'py, PyAny>,
            ) -> PyResult<Bound<'py, PyAny>> {
                let message = crate::sockets::message_from(message)?;
                let slot = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    slot.acquire()
                        .await
                        .send(message)
                        .await
                        .map(|_| ())
                        .map_err(errno_of)
                })
            }

            /// Receives the next message as a list of `bytes`, one per frame.
            ///
            /// Bounded by `ZMQ_RCVTIMEO`.
            fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let slot = Arc::clone(&self.socket);
                self.bridge.awaitable(py, async move {
                    slot.acquire()
                        .await
                        .recv()
                        .await
                        .map(crate::sockets::message_into)
                        .map_err(errno_of)
                })
            }
        });
    };
}

request_reply_socket!(
    PyReqSocket,
    ReqSocket,
    "ReqSocket",
    "REQ",
    "A REQ socket: one request out, one reply in, in that order (`EFSM` otherwise)."
);
request_reply_socket!(
    PyRepSocket,
    RepSocket,
    "RepSocket",
    "REP",
    "A REP socket: one request in, one reply out, with the envelope kept between them."
);
python_socket!(
    PyDealerSocket,
    DealerSocket,
    "DealerSocket",
    "DEALER",
    "A DEALER socket: request-reply without the lockstep, round-robin out and fair-queued in."
);
python_socket!(
    PyRouterSocket,
    RouterSocket,
    "RouterSocket",
    "ROUTER",
    "A ROUTER socket: every message carries the peer's routing id as its first frame."
);
python_socket!(
    PyPubSocket,
    PubSocket,
    "PubSocket",
    "PUB",
    "A PUB socket: publisher-side prefix matching, and a drop rather than a block at the high-water mark."
);
python_socket!(
    PySubSocket,
    SubSocket,
    "SubSocket",
    "SUB",
    "A SUB socket: nothing arrives until something is subscribed to."
);
python_socket!(
    PyXPubSocket,
    XPubSocket,
    "XPubSocket",
    "XPUB",
    "An XPUB socket: PUB, with its subscribers' subscriptions delivered to the application."
);
python_socket!(
    PyXSubSocket,
    XSubSocket,
    "XSubSocket",
    "XSUB",
    "An XSUB socket: SUB, with subscriptions sent upstream as messages."
);
python_socket!(
    PyPushSocket,
    PushSocket,
    "PushSocket",
    "PUSH",
    "A PUSH socket: round-robin over the workers with room, blocking rather than discarding."
);
python_socket!(
    PyPullSocket,
    PullSocket,
    "PullSocket",
    "PULL",
    "A PULL socket: the fair-queued sink of a pipeline."
);
python_socket!(
    PyPairSocket,
    PairSocket,
    "PairSocket",
    "PAIR",
    "A PAIR socket: exactly one peer, no reconnection, no routing."
);
