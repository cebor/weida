//! The context, the client handle, and the one task that owns the socket.
//!
//! **Three constructors, mirroring `weida-runtime`'s `Exec`.** A protocol
//! library's audience is overwhelmingly synchronous and must not have to stand
//! in a reactor to call it, which is why
//! [0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4 gives
//! `weida-zmq` a context with three, and this one copies the shape:
//! [`Context::new`] borrows the ambient runtime, [`Context::with_handle`]
//! takes a handle to somebody else's, and [`Context::owned`] creates one the
//! context keeps alive.
//!
//! **One task owns the socket, and the handle is a channel.** Every ordering
//! obligation MQTT states is per connection — PUBACK in receipt order, PUBREC
//! in receipt order, PUBREL in PUBREC-receipt order ([MQTT-4.6.0-2] to
//! [MQTT-4.6.0-4]) [mqtt5 §7] — and the specification says nothing about
//! threads [mqtt5 §2]. A single reader and a single writer make those
//! obligations fall out of the structure instead of being reimposed by a
//! sorter, which is the cheapest correct answer.
//!
//! **The keep-alive timer is the client's own liveness, and both halves of it
//! are bounded.** Keep Alive "bounds the gap from finishing one client packet
//! to starting the next; absent other traffic the client MUST send PINGREQ"
//! ([MQTT-3.1.2-20]), and a server receiving nothing for 1.5 times the
//! interval MUST close ([MQTT-3.1.2-22]) [mqtt5 §1]. So the task sends PINGREQ
//! only when nothing else has been sent within the interval — a client that
//! pinged on a timer regardless would be spending the bandwidth MQTT exists to
//! save. The other half, waiting for the PINGRESP, has **no** specification
//! number at all [mqtt5 §1]; it is
//! [`crate::ConnectOptions::effective_ping_timeout`] here, because an
//! unbounded wait is a hang with a rationale ([LOOP.md] §2).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use weida_mqtt_codec::{Disconnect, DisconnectReasonCode, Packet, PacketType, Properties, Pubrel};
use weida_runtime::{Exec, OwnedReactor};

use crate::connection::{Authenticator, NoAuthenticator, Reader, Writer, handshake};
use crate::error::{Error, Result};
use crate::limits::ServerLimits;
use crate::options::{ConnectOptions, interval_seconds};
use crate::session::{Resend, Resumption, Session, expiry_seconds};

/// The reactor every client of this context runs on.
///
/// Cloning is a handle clone; the reactor [`Context::owned`] created dies with
/// the last clone.
#[derive(Clone)]
pub struct Context {
    exec: Exec,
    /// Kept alive for as long as any clone of this context is, and dropped
    /// last. `Arc` because a context is cloneable and the reactor must
    /// outlive every clone.
    reactor: Option<Arc<OwnedReactor>>,
}

impl Context {
    /// A context on the ambient Tokio runtime.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] when the calling thread is not inside a runtime.
    /// Failing here — at construction — beats failing later at a connect.
    pub fn new() -> Result<Context> {
        Ok(Context {
            exec: Exec::current()?,
            reactor: None,
        })
    }

    /// A context on the runtime `handle` names, for a process whose reactor
    /// runs somewhere other than the calling thread.
    #[must_use]
    pub fn with_handle(handle: tokio::runtime::Handle) -> Context {
        Context {
            exec: Exec::from_handle(handle),
            reactor: None,
        }
    }

    /// A context that owns a reactor with `worker_threads` workers.
    ///
    /// For a caller with no reactor at all, which is most of a protocol
    /// library's audience: the futures this context hands back may be driven
    /// on any executor, `futures::executor::block_on` included.
    ///
    /// # Errors
    ///
    /// [`Error::Runtime`] for `worker_threads` of 0, or [`Error::Io`] when the
    /// OS refuses the threads.
    pub fn owned(worker_threads: usize) -> Result<Context> {
        let (exec, reactor) = Exec::owned(worker_threads, "weida-mqtt")?;
        Ok(Context {
            exec,
            reactor: Some(Arc::new(reactor)),
        })
    }

    /// The executor, for a caller composing this client with something else on
    /// the same reactor.
    #[must_use]
    pub fn exec(&self) -> &Exec {
        &self.exec
    }
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("owns_reactor", &self.reactor.is_some())
            .finish()
    }
}

/// What the connection task reports to the application.
#[derive(Debug)]
#[non_exhaustive]
pub enum Event {
    /// The connection ended, with the reason. A server DISCONNECT arrives
    /// here as [`Error::ServerDisconnected`] carrying its code, which is the
    /// whole of B-141's "surfaced as a named error rather than as a closed
    /// socket".
    Disconnected(Error),
}

/// A command for the connection task.
enum Command {
    /// Send DISCONNECT and close.
    Disconnect {
        reason_code: DisconnectReasonCode,
        /// A revised `Session Expiry Interval`, which a client MAY set at
        /// close (3.14.2.2.2) [mqtt5 §1].
        session_expiry: Option<Duration>,
        done: oneshot::Sender<Result<()>>,
    },
    /// Send PINGREQ now, whatever the timer thinks.
    Ping { done: oneshot::Sender<Result<()>> },
}

/// The application's end of one connection.
#[derive(Debug)]
pub struct Client {
    commands: mpsc::Sender<Command>,
    limits: Arc<ServerLimits>,
    /// The client half of the session. A clone, so it outlives this handle:
    /// the application's copy is what makes a reconnect a *resumption*.
    session: Session,
    session_present: bool,
    resumption: Resumption,
    client_id: String,
    keep_alive: Option<Duration>,
}

/// The application's end of the event stream.
#[derive(Debug)]
pub struct Events {
    events: mpsc::Receiver<Event>,
}

impl Events {
    /// The next event, or `None` once the connection task has finished and
    /// every event has been taken.
    pub async fn next(&mut self) -> Option<Event> {
        self.events.recv().await
    }
}

impl Client {
    /// Connects to `address` (`host:port`, or `[v6]:port`), sends CONNECT and
    /// awaits CONNACK, on a session of its own.
    ///
    /// For a caller that does not intend to reconnect: the session is created
    /// here and dies with the returned handle, so nothing is carried across
    /// connections. A caller that *does* intend to reconnect holds its own
    /// [`Session`] and uses [`Client::connect_session`], because the session
    /// is what outlives the connection.
    ///
    /// # Errors
    ///
    /// Everything [`crate::connection`]'s handshake reports: a configuration
    /// refusal, a transport failure, [`Error::ConnectionRefused`] with the
    /// server's code, or [`Error::Timeout`].
    pub async fn connect(
        context: &Context,
        address: &str,
        options: ConnectOptions,
    ) -> Result<(Client, Events)> {
        let session = Session::new(options.client_id.clone(), &options.limits);
        Client::connect_session(context, address, options, &session).await
    }

    /// The same, on a session the caller owns and reuses.
    ///
    /// This is the reconnect path, and it is where the three rules of
    /// 3.2.2.1.1 are applied: Clean Start 1 discards the session before
    /// connecting ([MQTT-3.1.2-4]), `Session Present` 0 with local state
    /// discards it on arrival ([MQTT-3.2.2-5]), and `Session Present` 1 with
    /// no local state refuses the connection ([MQTT-3.2.2-4]).
    ///
    /// It is also the **only** place retransmission happens: on
    /// [`Resumption::Resumed`] the unacknowledged exchanges of
    /// [`Session::resend`] go back on the wire with their original Packet
    /// Identifiers and DUP 1, in the order the originals were sent
    /// ([MQTT-4.6.0-1]), before the connection task starts.
    ///
    /// # Errors
    ///
    /// As [`Client::connect`], plus [`Error::SessionPresentWithoutState`].
    pub async fn connect_session(
        context: &Context,
        address: &str,
        options: ConnectOptions,
        session: &Session,
    ) -> Result<(Client, Events)> {
        Client::connect_full(context, address, options, session, &NoAuthenticator).await
    }

    /// The same, answering the server's AUTH challenges through
    /// `authenticator` (4.12) [mqtt5 §10].
    ///
    /// # Errors
    ///
    /// As [`Client::connect`], plus [`Error::AuthenticationMethodMismatch`]
    /// where the server changes method mid-exchange and whatever the
    /// authenticator itself reports.
    pub async fn connect_with(
        context: &Context,
        address: &str,
        options: ConnectOptions,
        authenticator: &dyn Authenticator,
    ) -> Result<(Client, Events)> {
        let session = Session::new(options.client_id.clone(), &options.limits);
        Client::connect_full(context, address, options, &session, authenticator).await
    }

    /// The one implementation the four constructors funnel through.
    ///
    /// # Errors
    ///
    /// The union of what the other three report.
    pub async fn connect_full(
        context: &Context,
        address: &str,
        options: ConnectOptions,
        session: &Session,
        authenticator: &dyn Authenticator,
    ) -> Result<(Client, Events)> {
        options.validate()?;

        // "Clean Start 1 discards any existing Session" ([MQTT-3.1.2-4]), so
        // the client's half goes before the CONNECT rather than after the
        // CONNACK: a resumed identifier space in a session the server threw
        // away would collide with nothing and confuse everything.
        if options.clean_start {
            session.clear();
        }
        // Recorded now because the DISCONNECT rule of 3.14.2.2.2 needs to
        // know what this CONNECT declared, and after the handshake there is
        // nothing left to read it from.
        session.declare_expiry(expiry_seconds(options.session_expiry));

        let mut handshake = handshake(&context.exec, address, &options, authenticator).await?;

        // "The sender sets an initial send quota, non-zero and not exceeding
        // the peer's Receive Maximum" ([MQTT-4.9.0-1]): the server's number
        // replaces the client's placeholder here and nowhere else.
        session.set_quota(handshake.limits.receive_maximum);

        let resumption = session.accept_connack(handshake.session_present)?;
        if resumption == Resumption::Resumed {
            retransmit(&mut handshake.writer, session).await?;
        }

        let limits = Arc::new(handshake.limits);
        let (commands_tx, commands_rx) = mpsc::channel(options.limits.incoming_queue);
        let (events_tx, events_rx) = mpsc::channel(options.limits.incoming_queue);

        let task = Task {
            reader: handshake.reader,
            writer: handshake.writer,
            commands: commands_rx,
            events: events_tx,
            keep_alive: handshake.keep_alive,
            ping_timeout: options.effective_ping_timeout(),
            max_packet_size: options.limits.maximum_packet_size,
            exec: context.exec.clone(),
        };
        context.exec.spawn(task.run());

        Ok((
            Client {
                commands: commands_tx,
                limits,
                session: session.clone(),
                session_present: handshake.session_present,
                resumption,
                client_id: handshake.client_id,
                keep_alive: handshake.keep_alive,
            },
            Events { events: events_rx },
        ))
    }

    /// What the server declared in CONNACK, with §11's defaults applied to
    /// what it left out.
    #[must_use]
    pub fn server_limits(&self) -> &ServerLimits {
        &self.limits
    }

    /// `Session Present` from CONNACK (3.2.2.1.1) [mqtt5 §1].
    ///
    /// What a client must *do* about it — close when it has no state and sees
    /// 1 ([MQTT-3.2.2-4]), discard when it has state and sees 0
    /// ([MQTT-3.2.2-5]) — needs a session to compare against and is B-142's.
    #[must_use]
    pub const fn session_present(&self) -> bool {
        self.session_present
    }

    /// The Client Identifier in force: the one that was sent, or the
    /// `Assigned Client Identifier` the server chose for a zero-length one
    /// ([MQTT-3.2.2-16]) [mqtt5 §2].
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// The Keep Alive in force: the client's, or `Server Keep Alive` where
    /// the server sent one ([MQTT-3.2.2-21]). `None` means the mechanism is
    /// disabled and the server will not time this connection out.
    #[must_use]
    pub const fn keep_alive(&self) -> Option<Duration> {
        self.keep_alive
    }

    /// The session this connection runs on. Cloning it is how an application
    /// keeps it across a reconnect.
    #[must_use]
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// What `Session Present` obliged this client to do: resume, start
    /// fresh, or discard what it held (3.2.2.1.1) [mqtt5 §1].
    ///
    /// The fourth case — `Session Present` 1 with no local state — is not a
    /// value here because it is [`Error::SessionPresentWithoutState`]:
    /// [MQTT-3.2.2-4] obliges the client to close, so there is no connection
    /// to report it on.
    #[must_use]
    pub const fn resumption(&self) -> Resumption {
        self.resumption
    }

    /// Sends DISCONNECT and closes.
    ///
    /// `DisconnectReasonCode::NormalDisconnection` makes the server discard
    /// the Will without publishing it ([MQTT-3.14.4-3]);
    /// `DisconnectWithWillMessage` asks for the Will anyway [mqtt5 §1].
    ///
    /// # Errors
    ///
    /// [`Error::NotConnected`] where the task has already finished, and
    /// whatever the write reported.
    pub async fn disconnect(&self, reason_code: DisconnectReasonCode) -> Result<()> {
        self.disconnect_with(reason_code, None).await
    }

    /// The same, revising the `Session Expiry Interval` at close, which a
    /// client MAY do — "so a session's lifetime can be shortened or extended
    /// at close" (3.14.2.2.2) [mqtt5 §1].
    ///
    /// **The rule the codec deferred is enforced here, before sending**: "a
    /// non-zero value when CONNECT carried zero is a Protocol Error"
    /// (3.14.2.2.2) [mqtt5 §1], which needs the CONNECT that came before and
    /// therefore needs the session. Shortening to zero is always permitted,
    /// and is what the specification advises a client that is finished to do
    /// so that a session it will never return to is not orphaned
    /// (3.1.2.11.2) [mqtt5 §9].
    ///
    /// # Errors
    ///
    /// As [`Client::disconnect`], plus [`Error::Configuration`] for an
    /// interval a Four Byte Integer cannot carry or for the zero-then-non-zero
    /// Protocol Error.
    pub async fn disconnect_with(
        &self,
        reason_code: DisconnectReasonCode,
        session_expiry: Option<Duration>,
    ) -> Result<()> {
        interval_seconds("session_expiry", session_expiry)?;
        self.session
            .revise_expiry_on_disconnect(expiry_seconds(session_expiry))?;
        let (done, wait) = oneshot::channel();
        self.commands
            .send(Command::Disconnect {
                reason_code,
                session_expiry,
                done,
            })
            .await
            .map_err(|_| Error::NotConnected)?;
        wait.await.map_err(|_| Error::NotConnected)?
    }

    /// Sends a PINGREQ now.
    ///
    /// The timer sends one by itself when nothing else has been sent within
    /// the interval; this is for a caller that wants to probe liveness on its
    /// own schedule.
    ///
    /// # Errors
    ///
    /// [`Error::NotConnected`], or whatever the write reported.
    pub async fn ping(&self) -> Result<()> {
        let (done, wait) = oneshot::channel();
        self.commands
            .send(Command::Ping { done })
            .await
            .map_err(|_| Error::NotConnected)?;
        wait.await.map_err(|_| Error::NotConnected)?
    }
}

/// The task that owns the socket.
struct Task {
    reader: Reader,
    writer: Writer,
    commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<Event>,
    keep_alive: Option<Duration>,
    ping_timeout: Option<Duration>,
    max_packet_size: u32,
    exec: Exec,
}

impl Task {
    async fn run(mut self) {
        let outcome = self.drive().await;
        let error = match outcome {
            Ok(()) => Error::NotConnected,
            Err(error) => error,
        };
        // A send failure here means the application dropped its `Events`,
        // which is its right: the connection is over either way.
        let _ = self.events.send(Event::Disconnected(error)).await;
    }

    /// Reads, answers and pings until something ends the connection.
    async fn drive(&mut self) -> Result<()> {
        // Whether a PINGREQ is outstanding, and therefore whether the
        // unquantified PINGRESP deadline is running.
        let mut awaiting_pingresp = false;
        // Keep Alive "bounds the gap from finishing one Client packet to
        // starting the next" ([MQTT-3.1.2-20]) [mqtt5 §1], so the interval is
        // measured from the last **write** and not from the last loop turn.
        // Measuring it per turn would let a chatty server keep resetting a
        // silent client's timer until the server itself closed the connection
        // at 1.5x — the exact failure the mechanism exists to prevent.
        let mut last_write = std::time::Instant::now();

        loop {
            // The idle interval: what is left of Keep Alive while nothing is
            // outstanding, the PINGRESP deadline once one is. Both may be
            // absent, in which case nothing is timed and the loop waits on
            // traffic alone.
            let idle = if awaiting_pingresp {
                self.ping_timeout
            } else {
                self.keep_alive
                    .map(|interval| interval.saturating_sub(last_write.elapsed()))
            };

            let step = {
                let read = self.reader.next();
                let command = self.commands.recv();
                match idle {
                    Some(idle) => {
                        let timer = self.exec.sleep(idle);
                        tokio::select! {
                            packet = read => Step::Packet(packet.map(<[u8]>::to_vec)),
                            command = command => Step::Command(command),
                            () = timer => Step::Idle,
                        }
                    }
                    None => tokio::select! {
                        packet = read => Step::Packet(packet.map(<[u8]>::to_vec)),
                        command = command => Step::Command(command),
                    },
                }
            };

            match step {
                Step::Packet(bytes) => {
                    let bytes = bytes?;
                    let (packet, _) = Packet::decode(&bytes, self.max_packet_size)?;
                    match packet {
                        Packet::Pingresp => awaiting_pingresp = false,
                        Packet::Disconnect(disconnect) => {
                            // The whole point of 5.0's server-to-client
                            // DISCONNECT: the code reaches the application.
                            return Err(Error::ServerDisconnected(disconnect.reason_code));
                        }
                        Packet::Pingreq
                        | Packet::Connect(_)
                        | Packet::Subscribe(_)
                        | Packet::Unsubscribe(_) => {
                            // Client-to-server only (2.1.2) [mqtt5 §12/P11].
                            return Err(Error::UnexpectedPacket {
                                packet_type: packet.packet_type(),
                            });
                        }
                        Packet::Connack(_) => {
                            // "The Server MUST NOT send more than one CONNACK"
                            // ([MQTT-3.2.0-1]); a second one is a violation.
                            return Err(Error::UnexpectedPacket {
                                packet_type: PacketType::Connack,
                            });
                        }
                        // The delivery and subscription packets are B-143's
                        // and B-144's; until those items land, a server that
                        // sends one is sending something this connection never
                        // asked for, so saying so beats discarding it.
                        other => {
                            return Err(Error::UnexpectedPacket {
                                packet_type: other.packet_type(),
                            });
                        }
                    }
                }
                Step::Command(None) => {
                    // Every `Client` handle is gone and nobody asked for a
                    // DISCONNECT. Closing without one is what triggers the
                    // Will [mqtt5 §4.5], and inventing an orderly close on the
                    // application's behalf would suppress it.
                    return Ok(());
                }
                Step::Command(Some(Command::Ping { done })) => {
                    let sent = self.writer.send(&Packet::Pingreq).await;
                    let failed = sent.is_err();
                    let _ = done.send(sent);
                    if failed {
                        return Err(Error::ConnectionClosed);
                    }
                    last_write = std::time::Instant::now();
                    awaiting_pingresp = true;
                }
                Step::Command(Some(Command::Disconnect {
                    reason_code,
                    session_expiry,
                    done,
                })) => {
                    let sent = self.send_disconnect(reason_code, session_expiry).await;
                    let _ = done.send(sent);
                    // "After sending DISCONNECT the sender MUST send nothing
                    // more and MUST close" ([MQTT-3.14.4-1],
                    // [MQTT-3.14.4-2]) [mqtt5 §12/P17].
                    return Ok(());
                }
                Step::Idle if awaiting_pingresp => {
                    // The number the specification declines to give, spent.
                    return Err(Error::Timeout("PINGRESP"));
                }
                Step::Idle => {
                    // "Absent other traffic the Client MUST send a PINGREQ"
                    // ([MQTT-3.1.2-20]). The timer runs from the last write,
                    // so reaching here *is* "absent other traffic".
                    self.writer.send(&Packet::Pingreq).await?;
                    last_write = std::time::Instant::now();
                    awaiting_pingresp = true;
                }
            }
        }
    }

    async fn send_disconnect(
        &mut self,
        reason_code: DisconnectReasonCode,
        session_expiry: Option<Duration>,
    ) -> Result<()> {
        let properties = Properties {
            session_expiry_interval: interval_seconds("session_expiry", session_expiry)?,
            ..Properties::new()
        };
        self.writer
            .send(&Packet::Disconnect(Disconnect {
                reason_code,
                properties,
            }))
            .await
    }
}

/// Puts the session's unacknowledged exchanges back on the wire.
///
/// **The only retransmission in this crate**, called from exactly one place:
/// just after a CONNACK with `Session Present` 1. "This is the only
/// circumstance where a Client or Server is REQUIRED to resend messages.
/// Clients and Servers MUST NOT resend messages at any other time"
/// ([MQTT-4.4.0-1]) [mqtt5 §6].
///
/// The order is the order the originals were sent ([MQTT-4.6.0-1]), the
/// identifiers are the originals, and every resent PUBLISH carries DUP 1
/// ([MQTT-3.3.1-1]) — while a PUBREL, which has no DUP flag, carries nothing
/// extra.
async fn retransmit(writer: &mut Writer, session: &Session) -> Result<()> {
    for item in session.resend() {
        match item {
            Resend::Publish { packet_id, message } => {
                let pairs = message.borrowed_user_properties();
                let publish = message.to_publish(packet_id, true, &pairs);
                writer.send(&Packet::Publish(publish)).await?;
            }
            Resend::Pubrel { packet_id } => {
                writer.send(&Packet::Pubrel(Pubrel::new(packet_id))).await?;
            }
        }
    }
    Ok(())
}

/// What woke the connection task.
enum Step {
    Packet(Result<Vec<u8>>),
    Command(Option<Command>),
    Idle,
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_mqtt_codec::varint;

    /// The three constructors, and the one that needs no reactor at all.
    #[test]
    fn an_owned_context_needs_no_ambient_runtime() {
        // Not inside a runtime: `new` must fail here and `owned` must not.
        assert!(Context::new().is_err());
        let context = Context::owned(1).expect("owns a reactor");
        assert!(format!("{context:?}").contains("owns_reactor: true"));
        // A clone keeps the reactor alive after the original is dropped: a
        // task spawned on it still runs, which is the only way to observe
        // that the reactor was not shut down with the original.
        let clone = context.clone();
        drop(context);
        let ran = clone.exec().spawn(async { 7u8 });
        assert_eq!(futures::executor::block_on(ran).expect("the task ran"), 7);
    }

    #[tokio::test]
    async fn an_ambient_context_borrows_the_running_runtime() {
        let context = Context::new().expect("ambient");
        assert!(format!("{context:?}").contains("owns_reactor: false"));
    }

    /// A `Maximum Packet Size` a Four Byte Integer cannot carry is refused at
    /// configuration time, which is also what keeps the decoder's cap sane.
    #[test]
    fn the_decoder_cap_is_the_declared_maximum() {
        let options = ConnectOptions::new("a");
        assert!(options.limits.maximum_packet_size <= varint::MAX + 5);
        assert!(options.validate().is_ok());
    }
}
