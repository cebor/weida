//! The blocking facade: weida for a caller with no executor.
//!
//! Behind the `blocking` feature, which is **not** default. Everything else
//! in this crate is `async`, because the transport is, and that is right for
//! an application that already has a reactor. It is wrong for a script, a
//! test harness, a CLI or a thread pool that has none — and this repository's
//! four protocol libraries all learned that, one facade each
//! (`weida_zmq::blocking`, `weida_mqtt::blocking`, `weida_nng::blocking`).
//! This is the same shape for weida's own patterns (B-194).
//!
//! # It is a wrapper, and nothing else
//!
//! Every method here is a `block_on` around the asynchronous endpoint's own
//! method. **No protocol behaviour is decided twice**: HELLO, negotiation,
//! the dispatch rules, the drain, the limits and the guarantee sets belong to
//! the asynchronous surface, and this module cannot disagree with them
//! because it does not implement them. Two weidas in one crate would be a bug
//! that only shows up in the one the tests do not cover.
//!
//! What it adds is the shape a synchronous caller needs: a payload is a
//! `Vec<u8>` rather than a stream, and every receive takes an explicit
//! ceiling in bytes — [`IncomingTransfer::read_capped`](crate::IncomingTransfer::read_capped)'s
//! argument, passed through rather than defaulted, because a caller who
//! wants the whole payload in memory is the one who knows how much of it
//! there may be ([INVARIANTS.md](../../../docs/INVARIANTS.md)).
//!
//! A caller who needs the streaming surface — a payload too large to hold, a
//! reply written while the request is still arriving — wants the
//! asynchronous API, and every wrapper here hands it out
//! ([`Requester::endpoint`], [`Replier::endpoint`], and so on).
//!
//! # The reactor is the library's, and blocking a worker is refused
//!
//! [`Runtime::new`] builds a [`crate::Runtime::owned`] reactor: its threads
//! belong to this crate, are named, are sized by
//! [`RuntimeConfig::worker_threads`](crate::RuntimeConfig::worker_threads)
//! and die with the handle. The futures are then driven on the **calling**
//! thread with `futures::executor::block_on`, so two threads may each block
//! on their own endpoint, which is how every synchronous program is written.
//!
//! A caller who is already inside a Tokio runtime must not use this facade:
//! blocking a reactor worker parks the thread that has to drive what the
//! caller is waiting for, and the result is a deadlock with no message. That
//! mistake is **detected rather than documented** — every call checks for an
//! ambient runtime first and fails with [`Error::Runtime`] naming the fix —
//! because a deadlock is the one failure a user cannot debug from the
//! outside.
//!
//! ```no_run
//! use weida::blocking::Runtime;
//! use weida::{RuntimeConfig, Trust};
//!
//! # fn main() -> Result<(), weida::Error> {
//! let runtime = Runtime::new(RuntimeConfig::default())?;
//! let requester = runtime.requester(Trust::by_address());
//! requester.connect("weida://sha256:…@127.0.0.1:7443/echo")?;
//! let reply = requester.request(b"ping", 1024 * 1024)?;
//! println!("{}", String::from_utf8_lossy(&reply));
//! # Ok(())
//! # }
//! ```

use std::time::Duration;

use weida_core::{Error, Limits};

use crate::{
    ClientTls, Drained, Identity, IncomingMeta, RuntimeConfig, ServerTls, TransferMeta, Trust,
};

/// A runtime that owns its reactor, for a synchronous caller.
///
/// The counterpart of [`crate::Runtime::owned`], and the reason that
/// constructor exists: a caller with no executor of its own gets one it never
/// sees, and a caller that *has* one wants [`crate::Runtime::new`] and the
/// asynchronous endpoints instead.
pub struct Runtime {
    inner: crate::Runtime,
}

impl Runtime {
    /// A runtime with its own reactor.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] when `worker_threads` is `0` or the OS refuses the
    /// threads, and when this is called from inside a Tokio runtime — see the
    /// [module documentation](self).
    pub fn new(config: RuntimeConfig) -> Result<Runtime, Error> {
        outside_a_reactor()?;
        Ok(Runtime {
            inner: crate::Runtime::owned(config)?,
        })
    }

    /// The asynchronous runtime underneath, for everything this facade does
    /// not wrap.
    pub fn runtime(&self) -> &crate::Runtime {
        &self.inner
    }

    /// A requester on this runtime.
    pub fn requester(&self, trust: impl Into<ClientTls>) -> Requester {
        Requester {
            endpoint: self.inner.requester(trust),
        }
    }

    /// A pusher on this runtime.
    pub fn pusher(&self, trust: impl Into<ClientTls>) -> Pusher {
        Pusher {
            endpoint: self.inner.pusher(trust),
        }
    }

    /// A subscriber on this runtime.
    pub fn subscriber(&self, trust: impl Into<ClientTls>) -> Subscriber {
        Subscriber {
            endpoint: self.inner.subscriber(trust),
        }
    }

    /// Binds a QUIC socket and returns the listener's endpoints.
    ///
    /// The address includes the port the kernel chose, which is how a
    /// wildcard port is read back.
    ///
    /// # Errors
    ///
    /// As [`crate::Listener::bind_quic`]; plus [`Error::Runtime`] from inside
    /// a reactor.
    pub fn bind_quic(
        &self,
        addr: std::net::SocketAddr,
        tls: impl Into<ServerTls>,
    ) -> Result<Binding, Error> {
        outside_a_reactor()?;
        let listener = self.inner.listener();
        let binding = drive(listener.bind_quic(addr, tls))?;
        Ok(Binding { listener, binding })
    }

    /// The runtime's limits, as configured.
    pub fn limits(&self) -> &Limits {
        &self.inner.config().limits
    }

    /// Stops admitting work and waits for finished transfers to reach the
    /// peer's transport, bounded by `deadline`.
    ///
    /// The deadline is mandatory and finite for the reason
    /// [`crate::Runtime::drain`] gives.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] from inside a reactor.
    pub fn drain(self, deadline: Duration) -> Result<Drained, Error> {
        outside_a_reactor()?;
        Ok(drive(self.inner.drain(deadline)))
    }

    /// Closes every connection without waiting for anything in flight.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] from inside a reactor.
    pub fn shutdown(self) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(self.inner.shutdown());
        Ok(())
    }
}

/// A bound QUIC socket and the endpoints registered on it.
pub struct Binding {
    listener: crate::Listener,
    binding: crate::Binding,
}

impl Binding {
    /// The address the socket is bound to, with the port the kernel chose.
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.binding.local_addr()
    }

    /// Registers a replier at `path`.
    ///
    /// # Errors
    ///
    /// [`Error::AlreadyRegistered`] when the path is taken,
    /// [`Error::InvalidEndpointPath`] for a malformed one.
    pub fn replier(&self, path: &str) -> Result<Replier, Error> {
        Ok(Replier {
            endpoint: self.listener.replier(path)?,
        })
    }

    /// Registers a puller at `path`.
    ///
    /// # Errors
    ///
    /// As [`Binding::replier`].
    pub fn puller(&self, path: &str) -> Result<Puller, Error> {
        Ok(Puller {
            endpoint: self.listener.puller(path)?,
        })
    }

    /// Registers a publisher at `path`.
    ///
    /// # Errors
    ///
    /// As [`Binding::replier`].
    pub fn publisher(&self, path: &str) -> Result<Publisher, Error> {
        Ok(Publisher {
            endpoint: self.listener.publisher(path)?,
        })
    }

    /// The listener underneath, for the registrations this facade does not
    /// wrap.
    pub fn listener(&self) -> &crate::Listener {
        &self.listener
    }
}

/// A received payload with the metadata that described it.
///
/// A `Vec<u8>` rather than a stream, which is the whole difference between
/// this facade and the asynchronous surface: the bytes are already here, and
/// the ceiling the caller passed is what bounds them.
#[derive(Clone, Debug)]
pub struct Message {
    /// The payload, at most the ceiling the caller passed.
    pub payload: Vec<u8>,
    /// What the DATA header said, including the topic and the peer.
    pub meta: IncomingMeta,
}

/// Drives one of this crate's futures on the **calling** thread.
///
/// Not on the reactor: its threads are driving the connections, and a caller
/// that parked one of them would be waiting for the worker that has to
/// complete what it is waiting for.
fn drive<F: Future>(future: F) -> F::Output {
    futures::executor::block_on(future)
}

/// Refuses the one mistake that produces a deadlock instead of an error.
///
/// `Handle::try_current` is `Ok` exactly on a thread a Tokio runtime is
/// driving, which is where `block_on` must never be called: the future this
/// facade blocks on needs that runtime to make progress. Checked at every
/// entry point rather than documented, because the symptom is a hang with no
/// message and no stack anyone can read.
fn outside_a_reactor() -> Result<(), Error> {
    match tokio::runtime::Handle::try_current() {
        Err(_) => Ok(()),
        Ok(_) => Err(Error::Runtime(
            "weida::blocking blocks the calling thread and was called from inside a Tokio \
             runtime, which would deadlock: use the asynchronous API there, or call this from \
             a thread the runtime does not own"
                .to_owned(),
        )),
    }
}

/// Writes the endpoint accessor and the connect call every dialling wrapper
/// has.
macro_rules! dialling {
    ($name:ident, $inner:ty) => {
        impl $name {
            /// Connects to a `weida://`, `weida+unix://`, `weida+pipe://` or
            /// `weida+inproc://` address.
            ///
            /// # Errors
            ///
            /// As the asynchronous `connect`; plus [`Error::Runtime`] from
            /// inside a reactor.
            pub fn connect(&self, url: &str) -> Result<(), Error> {
                outside_a_reactor()?;
                drive(self.endpoint.connect(url))
            }

            /// The asynchronous endpoint underneath, for the streaming
            /// surface this facade does not wrap.
            pub fn endpoint(&self) -> &$inner {
                &self.endpoint
            }
        }
    };
}

/// A requester: one exchange at a time, whole payloads in and out.
pub struct Requester {
    endpoint: crate::Requester,
}

dialling!(Requester, crate::Requester);

impl Requester {
    /// Sends `body` and returns the reply, at most `max_reply_bytes`.
    ///
    /// # Errors
    ///
    /// [`Error::Rejected`], [`Error::UnknownEndpoint`], [`Error::NoReply`] and
    /// [`Error::Indeterminate`] as the asynchronous `request`;
    /// [`Error::LimitExceeded`] when the reply exceeds the ceiling;
    /// [`Error::Runtime`] from inside a reactor.
    pub fn request(&self, body: &[u8], max_reply_bytes: usize) -> Result<Vec<u8>, Error> {
        self.request_with(TransferMeta::default(), body, max_reply_bytes)
    }

    /// [`Requester::request`] with explicit metadata.
    ///
    /// # Errors
    ///
    /// As [`Requester::request`].
    pub fn request_with(
        &self,
        meta: TransferMeta,
        body: &[u8],
        max_reply_bytes: usize,
    ) -> Result<Vec<u8>, Error> {
        outside_a_reactor()?;
        drive(async {
            let reply = self.endpoint.request_with(meta, body).await?;
            reply.collect(max_reply_bytes).await
        })
    }
}

/// A pusher: fire-and-forget transfers, with the receipt awaited.
pub struct Pusher {
    endpoint: crate::Pusher,
}

dialling!(Pusher, crate::Pusher);

impl Pusher {
    /// Sends `body` and waits for the peer's transport to acknowledge it.
    ///
    /// Waiting is the honest synchronous shape: a caller that returns before
    /// the receipt has nothing to check and nothing to retry on. It is the
    /// transport's acknowledgement and **not** an application one
    /// ([GUARANTEES.md](../../../docs/GUARANTEES.md) §1).
    ///
    /// # Errors
    ///
    /// [`Error::ConnectionLost`] before the FIN, [`Error::Indeterminate`]
    /// when the connection died with the receipt outstanding,
    /// [`Error::Rejected`] when the peer refused the payload,
    /// [`Error::Runtime`] from inside a reactor.
    pub fn send(&self, body: &[u8]) -> Result<(), Error> {
        self.send_with(
            TransferMeta::default().with_content_len(body.len() as u64),
            body,
        )
    }

    /// [`Pusher::send`] with explicit metadata.
    ///
    /// # Errors
    ///
    /// As [`Pusher::send`].
    pub fn send_with(&self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(async {
            let mut transfer = self.endpoint.open(meta).await?;
            transfer.write_all(body).await?;
            transfer.finish()?.delivered().await
        })
    }
}

/// A subscriber: whole published messages, one call each.
pub struct Subscriber {
    endpoint: crate::Subscriber,
}

dialling!(Subscriber, crate::Subscriber);

impl Subscriber {
    /// Subscribes to `filter`, the empty filter taking every topic.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`] for a filter the grammar of
    /// [`PROTOCOL.md`](../../../docs/PROTOCOL.md) §6.4 rejects,
    /// [`Error::LimitExceeded`] past `max_subscriptions`, [`Error::Runtime`]
    /// from inside a reactor.
    pub fn subscribe(&self, filter: &str) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(self.endpoint.subscribe(filter))
    }

    /// Withdraws one filter.
    ///
    /// # Errors
    ///
    /// As [`Subscriber::subscribe`].
    pub fn unsubscribe(&self, filter: &str) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(self.endpoint.unsubscribe(filter))
    }

    /// Waits for the next published message, at most `max_bytes`.
    ///
    /// The topic is on [`Message::meta`]'s `topic`, and a gap — under
    /// `PerProducer` ordering — on its `gap`.
    ///
    /// # Errors
    ///
    /// [`Error::NotConnected`] when the subscription is gone,
    /// [`Error::LimitExceeded`] past the ceiling, [`Error::Runtime`] from
    /// inside a reactor.
    pub fn recv(&self, max_bytes: usize) -> Result<Message, Error> {
        outside_a_reactor()?;
        drive(async {
            let transfer = self.endpoint.recv().await?;
            let meta = transfer.meta().clone();
            let payload = transfer.collect(max_bytes).await?;
            Ok(Message { payload, meta })
        })
    }
}

/// A replier: accept a request, answer it, whole payloads both ways.
pub struct Replier {
    endpoint: crate::Replier,
}

impl Replier {
    /// The endpoint path this replier serves.
    pub fn path(&self) -> &str {
        self.endpoint.path()
    }

    /// Waits for the next request and reads its body, at most `max_bytes`.
    ///
    /// # Errors
    ///
    /// [`Error::NotConnected`] when the listener is gone,
    /// [`Error::LimitExceeded`] past the ceiling, [`Error::Runtime`] from
    /// inside a reactor.
    pub fn accept(&self, max_bytes: usize) -> Result<Request, Error> {
        outside_a_reactor()?;
        drive(async {
            let mut request = self.endpoint.accept().await?;
            let meta = request.meta().clone();
            let payload = request.take_body().collect(max_bytes).await?;
            Ok(Request {
                request,
                message: Message { payload, meta },
            })
        })
    }

    /// The asynchronous replier underneath.
    pub fn endpoint(&self) -> &crate::Replier {
        &self.endpoint
    }
}

/// An accepted request whose body is already read, and the reply it owes.
///
/// Dropping it without [`Request::reply`] causes ERROR `{NO_REPLY}` on the
/// reply half, exactly as it does on the asynchronous surface: a requester
/// never waits out an idle timeout for an answer nobody will send.
pub struct Request {
    request: crate::IncomingRequest,
    message: Message,
}

impl Request {
    /// The request payload and its metadata.
    pub fn message(&self) -> &Message {
        &self.message
    }

    /// The payload, taken.
    pub fn into_payload(self) -> Vec<u8> {
        self.message.payload
    }

    /// Answers with `body`.
    ///
    /// # Errors
    ///
    /// [`Error::Canceled`] when the requester walked away,
    /// [`Error::ConnectionLost`], [`Error::Runtime`] from inside a reactor.
    pub fn reply(self, body: &[u8]) -> Result<(), Error> {
        self.reply_with(
            TransferMeta::default().with_content_len(body.len() as u64),
            body,
        )
    }

    /// [`Request::reply`] with explicit metadata.
    ///
    /// # Errors
    ///
    /// As [`Request::reply`].
    pub fn reply_with(self, meta: TransferMeta, body: &[u8]) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(async {
            let mut out = self.request.reply(meta).await?;
            out.write_all(body).await?;
            out.finish()?;
            Ok(())
        })
    }

    /// Declines the request with `code`, which the requester observes as the
    /// matching error.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] from inside a reactor.
    pub fn refuse(self, code: weida_core::ErrorCode) -> Result<(), Error> {
        outside_a_reactor()?;
        drive(self.request.refuse(code));
        Ok(())
    }

    /// The asynchronous request underneath, for a streamed reply.
    pub fn into_inner(self) -> crate::IncomingRequest {
        self.request
    }
}

/// A puller: whole transfers, one call each.
pub struct Puller {
    endpoint: crate::Puller,
}

impl Puller {
    /// The endpoint path this puller serves.
    pub fn path(&self) -> &str {
        self.endpoint.path()
    }

    /// Waits for the next transfer and reads it, at most `max_bytes`.
    ///
    /// # Errors
    ///
    /// As [`Subscriber::recv`].
    pub fn recv(&self, max_bytes: usize) -> Result<Message, Error> {
        outside_a_reactor()?;
        drive(async {
            let transfer = self.endpoint.recv().await?;
            let meta = transfer.meta().clone();
            let payload = transfer.collect(max_bytes).await?;
            Ok(Message { payload, meta })
        })
    }

    /// The asynchronous puller underneath.
    pub fn endpoint(&self) -> &crate::Puller {
        &self.endpoint
    }
}

/// A publisher: fan-out that never waits, exactly as the asynchronous one.
pub struct Publisher {
    endpoint: crate::Publisher,
}

impl Publisher {
    /// The endpoint path this publisher serves.
    pub fn path(&self) -> &str {
        self.endpoint.path()
    }

    /// Fans `payload` out to every matching subscriber, returning how many it
    /// was enqueued for.
    ///
    /// Synchronous on the asynchronous surface too — a publish never waits
    /// for a subscriber — so this is the one call in the facade that does not
    /// block at all, and it is here so that a synchronous caller does not
    /// have to reach for the asynchronous type to publish.
    ///
    /// # Errors
    ///
    /// [`Error::LimitExceeded`] for a payload above
    /// `Limits::subscriber_buffer_bytes`, which a streamed publish
    /// ([`crate::Publisher::open`]) has no ceiling for.
    pub fn publish(&self, topic: &str, payload: impl Into<bytes::Bytes>) -> Result<usize, Error> {
        self.endpoint.publish(topic, payload)
    }

    /// Subscribers currently connected.
    pub fn subscriber_count(&self) -> usize {
        self.endpoint.subscriber_count()
    }

    /// Copies dropped because a subscriber could not take them.
    pub fn dropped(&self) -> u64 {
        self.endpoint.dropped()
    }

    /// The asynchronous publisher underneath, for the streaming fan-out.
    pub fn endpoint(&self) -> &crate::Publisher {
        &self.endpoint
    }
}

/// A self-signed identity, for a synchronous caller that binds.
///
/// Re-exported so that a program using this facade needs no other import to
/// serve: the identity is the whole of a weida server's configuration.
#[cfg(feature = "generate")]
pub fn identity() -> Result<Identity, Error> {
    Identity::generate()
}

/// Trust by what the address names, which is the common client case.
pub fn trust_by_address() -> Trust {
    Trust::by_address()
}
