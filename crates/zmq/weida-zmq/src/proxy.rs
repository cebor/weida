//! The devices: `zmq_proxy` and `zmq_proxy_steerable`.
//!
//! "For many-to-many use-cases the pattern provides raw socket types (XPUB,
//! XSUB) to construct distribution proxies, also called brokers"
//! (`docs/research/zeromq.md` §4.3). A proxy is the smallest of them: read a
//! message on one socket, write it on the other, and hand a copy to a
//! capture socket if there is one. The zguide builds the forwarder, the
//! streamer, the queue device and Espresso out of exactly that.
//!
//! # What differs from `zmq_proxy`'s C signature
//!
//! `int zmq_proxy (void *frontend, void *backend, void *capture)` takes three
//! `void*` and returns `-1` with `errno` set, or `0` when the context is
//! terminated. Here:
//!
//! * **The operands are typed.** [`proxy`] takes `&mut` socket values behind
//!   the [`Device`] trait, so a socket that cannot receive cannot be a
//!   frontend and the compiler says so — where C finds out at run time with
//!   `ENOTSUP`. The trait is implemented for the socket types a proxy is
//!   built from, and its two halves are exactly what a device needs: receive
//!   one whole message, send one whole message.
//! * **`capture` is an `Option`, not a null pointer.** libzmq's third
//!   argument is `NULL` for "no capture"; a missing socket has a name here.
//! * **It returns the counters.** libzmq's `zmq_proxy` returns nothing but
//!   success or failure, and only `zmq_proxy_steerable`'s `STATISTICS`
//!   command can be asked for numbers. Both return [`ProxyStatistics`] here,
//!   because a proxy that ends has counted its traffic either way and
//!   throwing that away would be a loss nobody asked for.
//! * **There is no polling loop.** libzmq's proxy is a `zmq_poll` over two
//!   sockets; this is a `select!` over two futures, which is the same
//!   decision made by the reactor instead of by a timeout.
//!
//! # No unbounded buffering
//!
//! One message at a time, and the send that forwards it is the blocking one:
//! a message is read, optionally copied to the capture socket, and written to
//! the other side **before** the next message is read. The proxy therefore
//! holds exactly one message, and backpressure is the peer's high-water mark
//! reaching back through the blocking send — which is how a slow backend
//! slows its frontend down rather than filling the proxy's memory.
//!
//! The one exception is the socket types that drop rather than block: XPUB,
//! XSUB and PUB "SHALL silently drop the message if the queue for a
//! subscriber is full" (29/PUBSUB), so a pub-sub proxy loses a message for a
//! slow subscriber instead of stalling every other one. That is the
//! mechanism's rule and not this module's choice, and it is why the
//! statistics count what was handed over rather than what arrived.

use std::fmt;

use crate::dealerrouter::{DealerSocket, RouterSocket};
use crate::error::{Error, Result};
use crate::message::Multipart;
use crate::pair::PairSocket;
use crate::pipeline::{PullSocket, PushSocket};
use crate::pubsub::{PubSocket, SubSocket};
use crate::xpubxsub::{XPubSocket, XSubSocket};

/// `PAUSE`: stop reading either side until `RESUME`.
pub const CONTROL_PAUSE: &[u8] = b"PAUSE";
/// `RESUME`: read again.
pub const CONTROL_RESUME: &[u8] = b"RESUME";
/// `TERMINATE`: end the proxy and return its counters.
pub const CONTROL_TERMINATE: &[u8] = b"TERMINATE";
/// `STATISTICS`: reply with the eight counters, on the control socket.
pub const CONTROL_STATISTICS: &[u8] = b"STATISTICS";

/// One end of a device: something that can receive and send a whole message.
///
/// Implemented for the socket types a proxy is built from. A socket type
/// that cannot do one half says so: [`Device::receives`] is `false` for a
/// PUSH or a PUB, and the proxy then never reads that side — it is a side
/// that never delivers, which is what the zguide's streamer (PULL in, PUSH
/// out) needs — while a `send` toward a PULL or a SUB reports `ENOTSUP`,
/// which is `zmq_socket(3)`'s own table rather than a limitation here.
/// The futures are **`Send`**, and that is written out rather than left to
/// `async fn` in a trait: a socket is `!Sync` but it is `Send`, and a
/// `&mut socket` future is therefore `Send` too, so a device may be driven on
/// a multi-thread executor — which every caller outside `block_on` needs,
/// `weida-zmq-py`'s `proxy` among them. An `async fn` in a trait promises no
/// such bound and cannot be spawned at all.
pub trait Device {
    /// Whether this end ever delivers a message. `false` for a socket type
    /// that only sends, so a proxy does not wait on a receive that can only
    /// fail. A method rather than a constant, because a device end whose
    /// type is erased — `weida-zmq-py`'s — only knows at run time.
    fn receives(&self) -> bool {
        true
    }

    /// Receives one whole message, waiting for one.
    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send;

    /// Sends one whole message.
    fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send;
}

/// A socket type that receives but does not send, and one that sends but does
/// not receive, said once each.
macro_rules! one_way_device {
    ($socket:ty, recv_only) => {
        impl Device for $socket {
            fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
                <$socket>::recv(self)
            }

            fn send(&mut self, _message: Multipart) -> impl Future<Output = Result<()>> + Send {
                std::future::ready(Err(Error::ENOTSUP(
                    concat!(
                        "a ",
                        stringify!($socket),
                        " does not send, so it can only be the frontend a device reads"
                    )
                    .into(),
                )))
            }
        }
    };
    ($socket:ty, publish_only) => {
        impl Device for $socket {
            fn receives(&self) -> bool {
                false
            }

            fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
                std::future::ready(Err(Error::ENOTSUP(
                    concat!(
                        "a ",
                        stringify!($socket),
                        " does not receive, so it can only be the backend a device writes"
                    )
                    .into(),
                )))
            }

            fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
                // A publisher drops at the high-water mark rather than
                // blocking, which 29/PUBSUB requires; the report says how
                // many peers took it and the proxy does not turn a drop into
                // an error.
                <$socket>::publish(self, message);
                std::future::ready(Ok(()))
            }
        }
    };
}

impl Device for PairSocket {
    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
        PairSocket::recv(self)
    }

    fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
        PairSocket::send(self, message)
    }
}

impl Device for DealerSocket {
    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
        DealerSocket::recv(self)
    }

    fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
        DealerSocket::send(self, message)
    }
}

impl Device for RouterSocket {
    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
        RouterSocket::recv(self)
    }

    /// The routing id is the first frame, which is exactly what a ROUTER's
    /// own `recv` prepended on the other side of the device: the queue device
    /// forwards the envelope rather than inventing one.
    async fn send(&mut self, message: Multipart) -> Result<()> {
        RouterSocket::send(self, message).await.map(|_| ())
    }
}

impl Device for XSubSocket {
    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
        XSubSocket::recv(self)
    }

    /// An XSUB sends subscriptions upstream, and a pub-sub proxy's XSUB is
    /// where the subscriptions its XPUB read are forwarded.
    fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
        XSubSocket::send(self, message);
        std::future::ready(Ok(()))
    }
}

impl Device for XPubSocket {
    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
        XPubSocket::recv(self)
    }

    fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
        XPubSocket::publish(self, message);
        std::future::ready(Ok(()))
    }
}

one_way_device!(PullSocket, recv_only);
one_way_device!(SubSocket, recv_only);
one_way_device!(PubSocket, publish_only);

impl Device for PushSocket {
    fn receives(&self) -> bool {
        false
    }

    fn recv(&mut self) -> impl Future<Output = Result<Multipart>> + Send {
        std::future::ready(Err(Error::ENOTSUP(
            "a PUSH socket does not receive, so it can only be the backend a device writes".into(),
        )))
    }

    fn send(&mut self, message: Multipart) -> impl Future<Output = Result<()>> + Send {
        PushSocket::send(self, message)
    }
}

/// A device with two ends that never deliver has nothing to do.
fn nothing_to_read() -> Error {
    Error::ENOTSUP("neither side of this device receives, so it would forward nothing".into())
}

/// One direction's traffic.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counter {
    /// Messages handed over.
    pub messages: u64,
    /// Octets in those messages, frame bodies only — the same thing
    /// `ZMQ_MAXMSGSIZE` counts on the way in.
    pub bytes: u64,
}

impl Counter {
    fn record(&mut self, message: &Multipart) {
        self.messages += 1;
        self.bytes += message.total_bytes();
    }
}

/// The eight counters `zmq_proxy_steerable`'s `STATISTICS` reports.
///
/// libzmq replies with eight 64-bit numbers in this order, one frame each:
/// the frontend's messages in, bytes in, messages out, bytes out, then the
/// backend's four. "In" is what the socket received and "out" is what it was
/// given to send.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProxyStatistics {
    /// What the frontend received.
    pub frontend_in: Counter,
    /// What the frontend was given to send.
    pub frontend_out: Counter,
    /// What the backend received.
    pub backend_in: Counter,
    /// What the backend was given to send.
    pub backend_out: Counter,
}

impl ProxyStatistics {
    /// The eight frames, in libzmq's order, each an eight-octet little-endian
    /// number.
    ///
    /// Little endian for the reason [`crate::monitor`]'s header gives: libzmq
    /// copies the numbers into the frames, so the octet order is the host's,
    /// and every platform this library runs on is little endian.
    pub fn encode(&self) -> Multipart {
        let mut frames = self.as_array().into_iter();
        let first = frames.next().expect("eight counters");
        let mut message = Multipart::single(first.to_le_bytes().to_vec());
        for value in frames {
            message.push(value.to_le_bytes().to_vec());
        }
        message
    }

    /// Reads the eight frames back.
    ///
    /// # Errors
    ///
    /// `ENOCOMPATPROTO` unless the message is eight frames of eight octets.
    pub fn decode(message: &Multipart) -> Result<ProxyStatistics> {
        let frames = message.frames();
        if frames.len() != 8 {
            return Err(Error::ENOCOMPATPROTO(
                format!(
                    "a STATISTICS reply is eight frames; this one has {}",
                    frames.len()
                )
                .into(),
            ));
        }
        let mut values = [0u64; 8];
        for (value, frame) in values.iter_mut().zip(frames) {
            let bytes: [u8; 8] = frame.as_slice().try_into().map_err(|_| {
                Error::ENOCOMPATPROTO(
                    format!(
                        "a STATISTICS counter is eight octets; this one has {}",
                        frame.len()
                    )
                    .into(),
                )
            })?;
            *value = u64::from_le_bytes(bytes);
        }
        Ok(ProxyStatistics {
            frontend_in: Counter {
                messages: values[0],
                bytes: values[1],
            },
            frontend_out: Counter {
                messages: values[2],
                bytes: values[3],
            },
            backend_in: Counter {
                messages: values[4],
                bytes: values[5],
            },
            backend_out: Counter {
                messages: values[6],
                bytes: values[7],
            },
        })
    }

    fn as_array(&self) -> [u64; 8] {
        [
            self.frontend_in.messages,
            self.frontend_in.bytes,
            self.frontend_out.messages,
            self.frontend_out.bytes,
            self.backend_in.messages,
            self.backend_in.bytes,
            self.backend_out.messages,
            self.backend_out.bytes,
        ]
    }
}

impl fmt::Display for ProxyStatistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "frontend in {}/{}B out {}/{}B, backend in {}/{}B out {}/{}B",
            self.frontend_in.messages,
            self.frontend_in.bytes,
            self.frontend_out.messages,
            self.frontend_out.bytes,
            self.backend_in.messages,
            self.backend_in.bytes,
            self.backend_out.messages,
            self.backend_out.bytes,
        )
    }
}

/// What a control message asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Steer {
    /// `PAUSE`.
    Pause,
    /// `RESUME`.
    Resume,
    /// `TERMINATE`.
    Terminate,
    /// `STATISTICS`.
    Statistics,
}

impl Steer {
    /// Reads a control message.
    ///
    /// # Errors
    ///
    /// `EINVAL` for a command libzmq does not define, naming the four that
    /// exist — a proxy that ignored an unknown command would be steered by
    /// something nobody can see.
    pub fn parse(message: &Multipart) -> Result<Steer> {
        let command = message.frames()[0].as_slice();
        match command {
            CONTROL_PAUSE => Ok(Steer::Pause),
            CONTROL_RESUME => Ok(Steer::Resume),
            CONTROL_TERMINATE => Ok(Steer::Terminate),
            CONTROL_STATISTICS => Ok(Steer::Statistics),
            other => Err(Error::EINVAL(
                format!(
                    "{} is not a proxy command; zmq_proxy_steerable defines PAUSE, RESUME, \
                     TERMINATE and STATISTICS",
                    String::from_utf8_lossy(other)
                )
                .into(),
            )),
        }
    }
}

/// `zmq_proxy`: moves messages both ways until a socket ends, copying every
/// one to `capture` if there is one.
///
/// A side that does not receive — a PUSH or a PUB — is never read, so the
/// zguide's streamer, PULL in and PUSH out, forwards instead of failing at
/// its first poll; both sides not receiving is `ENOTSUP`, because such a
/// device would forward nothing. The steerable form has no such refusal:
/// its control socket is always read, so a `TERMINATE` still ends it.
///
/// Runs until one of the sockets reports an error — which is what closing a
/// socket or terminating the context looks like from in here — and returns
/// the counters it reached.
///
/// # Errors
///
/// Whatever a socket reports. `ENOTSOCK` and `ETERM` are the ordinary
/// endings: the sockets went away.
pub async fn proxy<F: Device, B: Device, C: Device>(
    frontend: &mut F,
    backend: &mut B,
    capture: Option<&mut C>,
) -> Result<ProxyStatistics> {
    let mut statistics = ProxyStatistics::default();
    let mut capture = capture;
    let (front_receives, back_receives) = (frontend.receives(), backend.receives());
    loop {
        tokio::select! {
            arrived = frontend.recv(), if front_receives => {
                let message = arrived?;
                statistics.frontend_in.record(&message);
                capture_copy(&mut capture, &message).await?;
                statistics.backend_out.record(&message);
                backend.send(message).await?;
            }
            arrived = backend.recv(), if back_receives => {
                let message = arrived?;
                statistics.backend_in.record(&message);
                capture_copy(&mut capture, &message).await?;
                statistics.frontend_out.record(&message);
                frontend.send(message).await?;
            }
            else => return Err(nothing_to_read()),
        }
    }
}

/// `zmq_proxy_steerable`: a [`proxy`] with a control socket.
///
/// `PAUSE` stops reading both data sockets — their queues fill and their
/// peers feel it, which is the point: a paused proxy is backpressure rather
/// than a black hole. `RESUME` reads again. `STATISTICS` replies on the
/// control socket with [`ProxyStatistics::encode`]'s eight frames. `TERMINATE`
/// returns the counters.
///
/// # Errors
///
/// As [`proxy`], plus `EINVAL` for a control command libzmq does not define.
pub async fn proxy_steerable<F: Device, B: Device, C: Device, S: Device>(
    frontend: &mut F,
    backend: &mut B,
    capture: Option<&mut C>,
    control: &mut S,
) -> Result<ProxyStatistics> {
    let mut statistics = ProxyStatistics::default();
    let mut capture = capture;
    let (front_receives, back_receives) = (frontend.receives(), backend.receives());
    let mut paused = false;
    loop {
        tokio::select! {
            steering = control.recv() => {
                match Steer::parse(&steering?)? {
                    Steer::Pause => paused = true,
                    Steer::Resume => paused = false,
                    Steer::Statistics => control.send(statistics.encode()).await?,
                    Steer::Terminate => return Ok(statistics),
                }
            }
            arrived = frontend.recv(), if front_receives && !paused => {
                let message = arrived?;
                statistics.frontend_in.record(&message);
                capture_copy(&mut capture, &message).await?;
                statistics.backend_out.record(&message);
                backend.send(message).await?;
            }
            arrived = backend.recv(), if back_receives && !paused => {
                let message = arrived?;
                statistics.backend_in.record(&message);
                capture_copy(&mut capture, &message).await?;
                statistics.frontend_out.record(&message);
                frontend.send(message).await?;
            }
        }
    }
}

/// Hands the capture socket a copy, if there is one.
///
/// A copy and not the message: the capture socket is an observer, so what it
/// gets must not be what the backend gets. This is the one clone a proxy
/// makes, and it is made only when somebody is watching.
async fn capture_copy<C: Device>(capture: &mut Option<&mut C>, message: &Multipart) -> Result<()> {
    match capture {
        Some(capture) => capture.send(message.clone()).await,
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{Context, ContextConfig};
    use crate::message::Message;

    fn context() -> Context {
        Context::new(ContextConfig::default()).expect("context")
    }

    /// Claim: the eight counters go both ways through libzmq's frame form, and
    /// a malformed reply is refused.
    #[test]
    fn the_statistics_are_eight_counters() {
        let statistics = ProxyStatistics {
            frontend_in: Counter {
                messages: 1,
                bytes: 2,
            },
            frontend_out: Counter {
                messages: 3,
                bytes: 4,
            },
            backend_in: Counter {
                messages: 5,
                bytes: 6,
            },
            backend_out: Counter {
                messages: 7,
                bytes: 8,
            },
        };
        let message = statistics.encode();
        assert_eq!(message.len(), 8);
        assert!(message.frames().iter().all(|frame| frame.len() == 8));
        assert_eq!(
            ProxyStatistics::decode(&message).expect("decode"),
            statistics
        );
        assert!(ProxyStatistics::decode(&Multipart::single(vec![0u8; 8])).is_err());
        let mut short = Multipart::single(vec![0u8; 7]);
        for _ in 0..7 {
            short.push(vec![0u8; 8]);
        }
        assert!(ProxyStatistics::decode(&short).is_err());
    }

    /// Claim: the four commands parse and nothing else does.
    #[test]
    fn the_control_commands_are_libzmqs_four() {
        for (frame, expected) in [
            (CONTROL_PAUSE, Steer::Pause),
            (CONTROL_RESUME, Steer::Resume),
            (CONTROL_TERMINATE, Steer::Terminate),
            (CONTROL_STATISTICS, Steer::Statistics),
        ] {
            assert_eq!(
                Steer::parse(&Multipart::single(frame.to_vec())).expect("a command"),
                expected
            );
        }
        let err = Steer::parse(&Multipart::single(b"STOP".to_vec())).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
        assert!(err.cause().contains("TERMINATE"), "{err}");
    }

    /// Claim: a pub-sub proxy over XSUB/XPUB forwards a publisher's messages
    /// to a subscriber, forwards the subscriber's subscription upstream, and
    /// gives the capture socket a copy of what passed — the zguide's
    /// forwarder and Espresso in one.
    #[tokio::test]
    async fn a_pubsub_proxy_forwards_both_ways_and_captures() {
        use crate::pubsub::{PubSocket, SubSocket};

        let context = context();
        let mut publisher = PubSocket::new(&context).expect("pub");
        let front = publisher.bind("tcp://127.0.0.1:0").await.expect("bind");

        let mut frontend = XSubSocket::new(&context).expect("xsub");
        frontend.connect(&front.to_string()).expect("connect");
        let mut backend = XPubSocket::new(&context).expect("xpub");
        let back = backend.bind("tcp://127.0.0.1:0").await.expect("bind");

        let mut subscriber = SubSocket::new(&context).expect("sub");
        subscriber.connect(&back.to_string()).expect("connect");
        subscriber.subscribe("px").expect("subscribe");

        // The capture socket: a PAIR over inproc, which is what the Espresso
        // recipe reads.
        let mut capture = PairSocket::new(&context).expect("pair");
        capture
            .bind("inproc://capture.pubsub")
            .await
            .expect("bind capture");
        let mut watcher = PairSocket::new(&context).expect("pair");
        watcher
            .connect("inproc://capture.pubsub")
            .expect("connect capture");

        let device =
            tokio::spawn(
                async move { proxy(&mut frontend, &mut backend, Some(&mut capture)).await },
            );

        // The subscription reaches the publisher through the proxy, so a
        // message published after it arrives is matched. The publisher drops
        // what nobody wants, so this is retried until one lands.
        let delivered = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                publisher.publish(Multipart::single(Message::from("px.eur 1.09")));
                if let Ok(message) = subscriber.try_recv() {
                    return message;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the subscriber got a message");
        assert_eq!(delivered.frames()[0].as_slice(), b"px.eur 1.09");

        // The capture socket saw a copy of what went past — subscriptions
        // included, since a proxy forwards those too.
        let seen = tokio::time::timeout(std::time::Duration::from_secs(10), watcher.recv())
            .await
            .expect("a captured message")
            .expect("a message");
        assert!(
            !seen.frames().is_empty(),
            "the capture socket received a copy"
        );
        device.abort();
    }

    /// Claim: the queue device over ROUTER/DEALER carries a request to a
    /// worker and its reply back to the right client, which is the envelope
    /// passing through the proxy untouched.
    #[tokio::test]
    async fn a_queue_device_routes_a_request_and_its_reply() {
        use crate::reqrep::{RepSocket, ReqSocket};

        let context = context();
        let mut frontend = RouterSocket::new(&context).expect("router");
        let front = frontend.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut backend = DealerSocket::new(&context).expect("dealer");
        let back = backend.bind("tcp://127.0.0.1:0").await.expect("bind");

        let mut client = ReqSocket::new(&context).expect("req");
        client.connect(&front.to_string()).expect("connect");
        let mut worker = RepSocket::new(&context).expect("rep");
        worker.connect(&back.to_string()).expect("connect");

        let device = tokio::spawn(async move {
            proxy::<_, _, PairSocket>(&mut frontend, &mut backend, None).await
        });

        client.send(Multipart::single("work")).await.expect("send");
        let request = tokio::time::timeout(std::time::Duration::from_secs(10), worker.recv())
            .await
            .expect("the request arrived")
            .expect("a request");
        assert_eq!(request.frames()[0].as_slice(), b"work");
        worker.send(Multipart::single("done")).await.expect("reply");
        let reply = tokio::time::timeout(std::time::Duration::from_secs(10), client.recv())
            .await
            .expect("the reply arrived")
            .expect("a reply");
        assert_eq!(reply.frames()[0].as_slice(), b"done");
        device.abort();
    }

    /// Claim: the zguide's streamer — PULL in, PUSH out — forwards, and so
    /// does the same pair the other way round; a side that cannot receive is
    /// a side that never delivers, not a failure at the first poll. The
    /// steerable form and a send-only capture socket get the same treatment.
    #[tokio::test]
    async fn a_streamer_forwards_through_a_side_that_never_receives() {
        let context = context();
        // PULL frontend, PUSH backend: the streamer as the guide draws it.
        let mut frontend = PullSocket::new(&context).expect("pull");
        let front = frontend.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut backend = PushSocket::new(&context).expect("push");
        let back = backend.bind("tcp://127.0.0.1:0").await.expect("bind");
        // The capture socket is a PUSH too: it only ever sends.
        let mut capture = PushSocket::new(&context).expect("push");
        let tap = capture.bind("tcp://127.0.0.1:0").await.expect("bind");

        let mut producer = PushSocket::new(&context).expect("push");
        producer.connect(&front.to_string()).expect("connect");
        let mut consumer = PullSocket::new(&context).expect("pull");
        consumer.connect(&back.to_string()).expect("connect");
        let mut watcher = PullSocket::new(&context).expect("pull");
        watcher.connect(&tap.to_string()).expect("connect");

        let device =
            tokio::spawn(
                async move { proxy(&mut frontend, &mut backend, Some(&mut capture)).await },
            );

        producer
            .send(Multipart::single("job 1"))
            .await
            .expect("send");
        let crossed = tokio::time::timeout(std::time::Duration::from_secs(10), consumer.recv())
            .await
            .expect("the job crossed the streamer")
            .expect("a job");
        assert_eq!(crossed.frames()[0].as_slice(), b"job 1");
        let seen = tokio::time::timeout(std::time::Duration::from_secs(10), watcher.recv())
            .await
            .expect("the capture socket saw it")
            .expect("a copy");
        assert_eq!(seen.frames()[0].as_slice(), b"job 1");
        assert!(
            !device.is_finished(),
            "a streamer does not die at its first poll"
        );
        device.abort();

        // The same pair the other way round: PUSH frontend, PULL backend.
        // Traffic flows backend to frontend and nothing is read from the
        // PUSH.
        let mut frontend = PushSocket::new(&context).expect("push");
        let front = frontend.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut backend = PullSocket::new(&context).expect("pull");
        let back = backend.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut control = PairSocket::new(&context).expect("pair");
        control
            .bind("inproc://proxy.streamer.control")
            .await
            .expect("bind control");
        let mut steer = PairSocket::new(&context).expect("pair");
        steer
            .connect("inproc://proxy.streamer.control")
            .expect("connect control");
        let mut consumer = PullSocket::new(&context).expect("pull");
        consumer.connect(&front.to_string()).expect("connect");
        let mut producer = PushSocket::new(&context).expect("push");
        producer.connect(&back.to_string()).expect("connect");

        let device = tokio::spawn(async move {
            proxy_steerable::<_, _, PushSocket, _>(&mut frontend, &mut backend, None, &mut control)
                .await
        });
        producer
            .send(Multipart::single("job 2"))
            .await
            .expect("send");
        let crossed = tokio::time::timeout(std::time::Duration::from_secs(10), consumer.recv())
            .await
            .expect("the job crossed the reversed streamer")
            .expect("a job");
        assert_eq!(crossed.frames()[0].as_slice(), b"job 2");
        steer
            .send(Multipart::single(CONTROL_TERMINATE.to_vec()))
            .await
            .expect("terminate");
        let statistics = tokio::time::timeout(std::time::Duration::from_secs(10), device)
            .await
            .expect("the proxy terminated")
            .expect("the task")
            .expect("the counters");
        assert_eq!(statistics.backend_in.messages, 1);
        assert_eq!(statistics.frontend_out.messages, 1);
        assert_eq!(
            statistics.frontend_in.messages, 0,
            "a PUSH frontend is never read"
        );
    }

    /// Claim: a device whose two ends both never deliver is refused rather
    /// than parked forever.
    #[tokio::test]
    async fn a_device_with_nothing_to_read_is_refused() {
        let context = context();
        let mut frontend = PushSocket::new(&context).expect("push");
        let mut backend = PubSocket::new(&context).expect("pub");
        let err = proxy::<_, _, PairSocket>(&mut frontend, &mut backend, None)
            .await
            .expect_err("nothing to forward");
        assert_eq!(err.errno(), "ENOTSUP", "{err}");
    }

    /// Claim: the control socket steers — `PAUSE` stops the traffic,
    /// `RESUME` starts it again, `STATISTICS` answers with the eight
    /// counters, and `TERMINATE` ends the proxy and returns them.
    #[tokio::test]
    async fn the_steerable_proxy_pauses_resumes_reports_and_terminates() {
        let context = context();
        // A PAIR-to-PAIR proxy: the simplest device there is, and the one
        // where a paused proxy is observable without a pattern's own rules in
        // the way.
        let mut frontend = PairSocket::new(&context).expect("pair");
        let front = frontend.bind("tcp://127.0.0.1:0").await.expect("bind");
        let mut backend = PairSocket::new(&context).expect("pair");
        let back = backend.bind("tcp://127.0.0.1:0").await.expect("bind");

        let mut sender = PairSocket::new(&context).expect("pair");
        sender.connect(&front.to_string()).expect("connect");
        let mut receiver = PairSocket::new(&context).expect("pair");
        receiver.connect(&back.to_string()).expect("connect");

        let mut control = PairSocket::new(&context).expect("pair");
        control
            .bind("inproc://proxy.control")
            .await
            .expect("bind control");
        let mut steer = PairSocket::new(&context).expect("pair");
        steer
            .connect("inproc://proxy.control")
            .expect("connect control");

        let device = tokio::spawn(async move {
            proxy_steerable::<_, _, PairSocket, _>(&mut frontend, &mut backend, None, &mut control)
                .await
        });

        // Running: a message goes through.
        sender.send(Multipart::single("first")).await.expect("send");
        let arrived = tokio::time::timeout(std::time::Duration::from_secs(10), receiver.recv())
            .await
            .expect("forwarded")
            .expect("a message");
        assert_eq!(arrived.frames()[0].as_slice(), b"first");

        // Paused: the next message waits in the frontend's queue rather than
        // reaching the receiver.
        steer
            .send(Multipart::single(CONTROL_PAUSE.to_vec()))
            .await
            .expect("pause");
        // The pause has to be observed before the message is sent, and the
        // only observable that does not race is STATISTICS: the proxy answers
        // it from the same loop that took the PAUSE.
        steer
            .send(Multipart::single(CONTROL_STATISTICS.to_vec()))
            .await
            .expect("ask");
        let reply = tokio::time::timeout(std::time::Duration::from_secs(10), steer.recv())
            .await
            .expect("a statistics reply")
            .expect("a reply");
        let statistics = ProxyStatistics::decode(&reply).expect("decode");
        assert_eq!(statistics.frontend_in.messages, 1);
        assert_eq!(statistics.backend_out.messages, 1);
        assert_eq!(statistics.frontend_in.bytes, 5, "\"first\" is five octets");

        sender
            .send(Multipart::single("second"))
            .await
            .expect("send");
        assert!(
            receiver.try_recv().is_err(),
            "a paused proxy forwards nothing"
        );

        // Resumed: it arrives.
        steer
            .send(Multipart::single(CONTROL_RESUME.to_vec()))
            .await
            .expect("resume");
        let arrived = tokio::time::timeout(std::time::Duration::from_secs(10), receiver.recv())
            .await
            .expect("forwarded after the resume")
            .expect("a message");
        assert_eq!(arrived.frames()[0].as_slice(), b"second");

        // Terminated: the proxy ends and hands back its counters.
        steer
            .send(Multipart::single(CONTROL_TERMINATE.to_vec()))
            .await
            .expect("terminate");
        let statistics = tokio::time::timeout(std::time::Duration::from_secs(10), device)
            .await
            .expect("the proxy ended")
            .expect("the task")
            .expect("the counters");
        assert_eq!(statistics.frontend_in.messages, 2);
        assert_eq!(statistics.backend_out.messages, 2);
        assert_eq!(statistics.frontend_in.bytes, 11, "five and six octets");
        assert_eq!(
            statistics.backend_in.messages, 0,
            "nothing came back the other way"
        );
    }
}
