//! The connection: version negotiation, the security layers, `open`, the
//! idle timeout and `close`.
//!
//! ```text
//! TCP connect
//!   |
//!   +-- TlsMode::Direct ------> TLS handshake (no header at all)
//!   |
//!   +-- TlsMode::Layered -----> AMQP %d2 1.0.0  both ways, TLS handshake
//!   |
//!   +-- Sasl != None ---------> AMQP %d3 1.0.0  both ways
//!   |                           sasl-mechanisms / sasl-init / [challenge,
//!   |                           response]* / sasl-outcome
//!   v
//! AMQP %d0 1.0.0  both ways
//! open  ---------------------->
//!       <---------------------- open
//! (empty frame every idle-time-out/2 while there is nothing else to send)
//! close ---------------------->                 the last thing ever written
//!       <---------------------- close
//! ```
//!
//! # The four ways negotiation fails, kept apart
//!
//! Part 2 §2.2 puts them all in the same eight octets, and a client that
//! folded them would retry the wrong thing:
//!
//! 1. **A different protocol-id.** "Protocol-id is not part of the version,
//!    so 'highest supported version' does not apply to it": a server
//!    requiring SASL answers a `%d0` request with `%d3` and closes. That is a
//!    *demand*, and this client reports it as
//!    [`Error::SecurityLayerRequired`] rather than retrying blind, because a
//!    client with no credentials configured cannot satisfy it and looping
//!    would only hide that.
//! 2. **A different version.** `AMQP` + `0 0 9 1` is an AMQP 0-9-1 server on
//!    the shared port 5672 — [`Error::VersionMismatch`].
//! 3. **Octets that are not a header at all** —
//!    [`Error::BadProtocolHeader`].
//! 4. **Nothing at all.** The protocol gives no deadline for the answering
//!    header, so this client applies its own:
//!    [`Error::HandshakeTimeout`].
//!
//! # The idle timeout is two independent clocks
//!
//! `open.idle-time-out` states "the maximum period the *sender of that open*
//! wants between frames arriving from its partner", and each peer has its own
//! (Part 2 §2.4.5). So there are two:
//!
//! * **Ours to keep**: the peer's advertised value. This client sends an
//!   empty frame every half of it when it has nothing else to send, which is
//!   the same halving the specification recommends for the advertisement and
//!   for the same reason — a keep-alive that arrives exactly at the deadline
//!   arrives too late.
//! * **Ours to enforce**: [`ConnectionOptions::idle_time_out`]. On expiry
//!   this client closes with `amqp:connection:forced` and an explanation,
//!   which is what Part 2 §2.4.5 says a peer SHOULD do, and only then drops
//!   the transport.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::performative::{Close, Open, Performative};
use weida_amqp_codec::protocol_header::{ProtocolHeader, ProtocolId};
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{Limits, Multiple};
use weida_runtime::Exec;

use crate::error::{Condition, Error, Result};
use crate::options::{ConnectionOptions, Sasl, TlsMode};
use crate::sasl;
use crate::transport::{self, FrameReader, FrameWriter, Wire};

/// How many outgoing frames may be queued for the driver before a sender
/// waits.
///
/// A local bound: the protocol has no queue here at all, and an unbounded
/// channel would let an application outrun the transport without ever being
/// told. Backpressure on the sender is the honest answer, and it is what the
/// session window and link credit do one layer up.
pub const OUTGOING_QUEUE: usize = 64;

/// What the peer said in its `open`, owned.
///
/// A borrowed `Open` does not outlive the frame it arrived in, and every one
/// of these fields is consulted for the life of the connection: the frame
/// size bounds every write, the channel maximum bounds the session table, the
/// idle timeout drives a timer, and the capabilities decide what may be used
/// at all.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteOpen {
    /// The peer's container name.
    pub container_id: String,
    /// The host the peer thinks it is.
    pub hostname: Option<String>,
    /// The largest frame the peer will accept.
    pub max_frame_size: u32,
    /// The highest channel number the peer will accept.
    pub channel_max: u16,
    /// Milliseconds the peer wants between frames from us; `None` means it
    /// asked for no timeout.
    pub idle_time_out: Option<u32>,
    /// Capabilities the peer offers.
    pub offered_capabilities: Vec<String>,
    /// Capabilities the peer wants. A peer MUST NOT use one it did not list.
    pub desired_capabilities: Vec<String>,
}

impl RemoteOpen {
    fn from_codec(open: &Open<'_>) -> Self {
        Self {
            container_id: open.container_id.to_owned(),
            hostname: open.hostname.map(str::to_owned),
            max_frame_size: open.max_frame_size,
            channel_max: open.channel_max,
            // Zero equals unset (Part 2 §2.4.5), so it is folded into `None`
            // here rather than left to every caller to remember.
            idle_time_out: open.idle_time_out.filter(|ms| *ms != 0),
            offered_capabilities: open
                .offered_capabilities
                .iter()
                .map(str::to_owned)
                .collect(),
            desired_capabilities: open
                .desired_capabilities
                .iter()
                .map(str::to_owned)
                .collect(),
        }
    }

    /// Whether the peer offered `capability`.
    #[must_use]
    pub fn offers(&self, capability: &str) -> bool {
        self.offered_capabilities.iter().any(|c| c == capability)
    }

    /// The negotiated frame size: the smaller of what we will accept and what
    /// the peer will accept, never below `MIN-MAX-FRAME-SIZE`.
    ///
    /// Both directions matter and they are different numbers. This is the one
    /// that bounds what *we* may write, because "a peer MUST NOT send frames
    /// larger than its partner can handle" (Part 2 §2.7.1).
    #[must_use]
    pub fn outgoing_frame_size(&self) -> u32 {
        self.max_frame_size.max(MIN_MAX_FRAME_SIZE)
    }
}

/// Where a connection is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    /// Both `open` frames have been exchanged.
    Open,
    /// We sent `close` and are waiting for the peer's.
    Closing,
    /// Done. `Some` where either side gave a condition.
    Closed(Option<Condition>),
    /// The connection failed; the string is the error as it was reported.
    Failed(String),
}

impl State {
    /// Whether anything further can be sent.
    #[must_use]
    pub const fn is_usable(&self) -> bool {
        matches!(self, Self::Open)
    }
}

/// What the driver accepts from a handle.
///
/// One variant, because a connection with no sessions has exactly one thing
/// a handle can ask it to do. The frame-writing variant the sessions need
/// arrives with the sessions rather than sitting here unused.
enum Command {
    /// `close`, with an optional condition, and a channel to report on.
    Close {
        error: Option<Condition>,
        done: oneshot::Sender<Result<()>>,
    },
}

/// An AMQP 1.0 connection.
///
/// Cloning shares the connection: every clone speaks to the same driver task
/// and sees the same state, which is what lets a session hold one without
/// owning it.
#[derive(Clone, Debug)]
pub struct Connection {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    exec: Exec,
    options: Arc<ConnectionOptions>,
    remote: RemoteOpen,
    commands: mpsc::Sender<Command>,
    state: watch::Receiver<State>,
}

impl Connection {
    /// Opens a connection to `host:port` over plain TCP.
    ///
    /// Refuses [`TlsMode::Layered`] and [`TlsMode::Direct`], because those
    /// need the caller's `rustls::ClientConfig` and silently dropping TLS
    /// from a configuration that asked for it is exactly the thing 0013 §4.4
    /// item 4 forbids.
    pub async fn connect(
        exec: &Exec,
        host: &str,
        port: u16,
        options: ConnectionOptions,
    ) -> Result<Self> {
        options.validate()?;
        if !matches!(options.tls, TlsMode::None) {
            return Err(Error::Configuration(
                "this configuration asks for TLS; use Connection::connect_tls, \
                 which takes the rustls::ClientConfig the trust decision needs"
                    .into(),
            ));
        }
        let stream = transport::connect(exec, host, port, &options).await?;
        Self::establish(exec, Wire::new(stream), options, None).await
    }

    /// Opens a connection to `host:port` with TLS, reached the way
    /// [`ConnectionOptions::tls`] says.
    ///
    /// `config` is the caller's: which certificates are valid is the
    /// application's decision. The SNI name is
    /// [`ConnectionOptions::tls_server_name`] or, failing that, `host`.
    #[cfg(feature = "tls")]
    pub async fn connect_tls(
        exec: &Exec,
        host: &str,
        port: u16,
        options: ConnectionOptions,
        config: Arc<tokio_rustls::rustls::ClientConfig>,
    ) -> Result<Self> {
        options.validate()?;
        let server_name = options
            .tls_server_name
            .clone()
            .unwrap_or_else(|| host.to_owned());
        let stream = transport::connect(exec, host, port, &options).await?;
        let mut wire = Wire::new(stream);

        match options.tls {
            TlsMode::None => {
                return Err(Error::Configuration(
                    "connect_tls with TlsMode::None: set TlsMode::Layered or \
                     TlsMode::Direct, or use Connection::connect"
                        .into(),
                ));
            }
            TlsMode::Direct => {}
            TlsMode::Layered => {
                // `AMQP %d2 1.0.0` both ways, in the clear, before the
                // handshake. A server that answers with anything else has
                // refused the layering, and that is the same demand a `%d3`
                // answer is.
                wire.write_protocol_header(ProtocolHeader::TLS).await?;
                let answer = step(
                    exec,
                    &options,
                    "the answering TLS protocol header",
                    wire.read_protocol_header(),
                )
                .await?;
                check_header(answer, ProtocolId::Tls)?;
            }
        }

        let stream = transport::upgrade_tls(wire.into_stream(), config, &server_name).await?;
        Self::establish(exec, Wire::new(stream), options, None).await
    }

    /// Runs the handshake on an already-established stream and starts the
    /// driver.
    ///
    /// Separate from the constructors so that a test can hand in a socket
    /// pair, and so that the TLS and non-TLS paths share one sequence rather
    /// than two that can drift.
    pub async fn establish(
        exec: &Exec,
        mut wire: Wire,
        options: ConnectionOptions,
        _reserved: Option<()>,
    ) -> Result<Self> {
        let options = Arc::new(options);

        if options.sasl != Sasl::None {
            wire.write_protocol_header(ProtocolHeader::SASL).await?;
            let answer = step(
                exec,
                &options,
                "the answering SASL protocol header",
                wire.read_protocol_header(),
            )
            .await?;
            check_header(answer, ProtocolId::Sasl)?;
            sasl::dialog(exec, &mut wire, &options).await?;
        }

        // `AMQP %d0 1.0.0`, either directly or after the layer above.
        wire.write_protocol_header(ProtocolHeader::AMQP).await?;
        let answer = step(
            exec,
            &options,
            "the answering AMQP protocol header",
            wire.read_protocol_header(),
        )
        .await?;
        check_header(answer, ProtocolId::Amqp)?;

        // `open` on channel 0, as the first frame. Pipelined in the sense
        // Part 2 §2.4.2 permits: sent without waiting for the peer's, and
        // staying inside what every implementation must support until the
        // peer's `open` says otherwise.
        let mut out = Vec::new();
        let local_open = local_open(&options);
        frame::write(&mut out, FrameKind::Amqp, 0, MIN_MAX_FRAME_SIZE, |body| {
            Performative::Open(local_open).encode(body)
        })?;
        wire.queue().extend_from_slice(&out);
        wire.flush().await?;

        let remote = {
            let frame = step(exec, &options, "the peer's open", wire.read_frame()).await?;
            if frame.header.channel != 0 {
                return Err(Error::Local(Condition::described(
                    condition::CONNECTION_FRAMING_ERROR,
                    format!(
                        "open arrived on channel {}, and the first frame MUST be \
                         on channel 0",
                        frame.header.channel
                    ),
                )));
            }
            let (performative, _) = Performative::decode(frame.body, Limits::DEFAULT)?;
            match performative {
                Performative::Open(open) => RemoteOpen::from_codec(&open),
                Performative::Close(Close { error }) => {
                    return Err(Error::Closed(error.as_ref().map(Condition::from_codec)));
                }
                other => {
                    return Err(Error::Local(Condition::described(
                        condition::ILLEGAL_STATE,
                        format!("expected open, the peer sent {}", other.name()),
                    )));
                }
            }
        };

        wire.negotiated(options.max_frame_size);
        let (mut reader, mut writer) = wire.split();
        reader.negotiated(options.max_frame_size);
        writer.negotiated(remote.outgoing_frame_size());

        let (commands, rx) = mpsc::channel(OUTGOING_QUEUE);
        let (state_tx, state) = watch::channel(State::Open);
        let driver = Driver {
            exec: exec.clone(),
            options: Arc::clone(&options),
            remote: remote.clone(),
            rx,
            state: state_tx,
        };
        exec.spawn(driver.run(reader, writer));

        Ok(Self {
            inner: Arc::new(Inner {
                exec: exec.clone(),
                options,
                remote,
                commands,
                state,
            }),
        })
    }

    /// What the peer said in its `open`.
    #[must_use]
    pub fn remote(&self) -> &RemoteOpen {
        &self.inner.remote
    }

    /// What this client said in its `open`.
    #[must_use]
    pub fn options(&self) -> &ConnectionOptions {
        &self.inner.options
    }

    /// The reactor this connection runs on.
    #[must_use]
    pub fn exec(&self) -> &Exec {
        &self.inner.exec
    }

    /// Where the connection is now.
    #[must_use]
    pub fn state(&self) -> State {
        self.inner.state.borrow().clone()
    }

    /// Waits for the connection to leave [`State::Open`].
    pub async fn closed(&self) -> State {
        let mut state = self.inner.state.clone();
        loop {
            if !state.borrow().is_usable() {
                return state.borrow().clone();
            }
            if state.changed().await.is_err() {
                return State::Failed("the connection driver stopped".into());
            }
        }
    }

    /// Sends `close`, waits for the peer's, and drops the transport.
    ///
    /// Part 2 §2.4.3: `close` MUST be the last thing ever written, the sender
    /// SHOULD keep reading until the partner's `close` arrives, and SHOULD
    /// apply a timeout before dropping the transport. The timeout is
    /// [`ConnectionOptions::close_budget`], and it is finite for the same
    /// reason every close budget in this repository is.
    ///
    /// Idempotent: closing a closed connection is `Ok(())`, because a caller
    /// that races its own shutdown should not have to hold a lock to be
    /// correct.
    pub async fn close(&self) -> Result<()> {
        self.close_with(None).await
    }

    /// Sends `close(error=...)`.
    pub async fn close_with(&self, error: Option<Condition>) -> Result<()> {
        if !self.state().is_usable() {
            return Ok(());
        }
        let (done, wait) = oneshot::channel();
        if self
            .inner
            .commands
            .send(Command::Close { error, done })
            .await
            .is_err()
        {
            return Ok(());
        }
        match self
            .inner
            .exec
            .within(self.inner.options.close_budget, wait)
            .await
        {
            Some(Ok(result)) => result,
            // The driver dropped the channel, which means it finished.
            Some(Err(_)) => Ok(()),
            None => Ok(()),
        }
    }
}

/// Builds this client's `open`.
fn local_open(options: &ConnectionOptions) -> Open<'_> {
    let mut open = Open::new(&options.container_id);
    open.hostname = options.hostname.as_deref();
    open.max_frame_size = options.max_frame_size;
    open.channel_max = options.channel_max;
    open.idle_time_out = options.advertised_idle_time_out();
    open.offered_capabilities = multiple(&options.offered_capabilities);
    open.desired_capabilities = multiple(&options.desired_capabilities);
    open
}

fn multiple(items: &[String]) -> Multiple<'_> {
    match items {
        [] => Multiple::None,
        [one] => Multiple::One(one),
        many => Multiple::Many(many.iter().map(String::as_str).collect()),
    }
}

/// Bounds one handshake step on wall-clock time.
///
/// Every step needs it and none of them has a deadline in the protocol: a
/// server that accepts the TCP connection and then says nothing would
/// otherwise hold the client for as long as the OS allows.
async fn step<T, F>(
    exec: &Exec,
    options: &ConnectionOptions,
    name: &'static str,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    match exec.within(options.handshake_timeout, future).await {
        Some(result) => result,
        None => Err(Error::HandshakeTimeout { step: name }),
    }
}

/// Reads the peer's answering header as the demand it is.
fn check_header(answer: ProtocolHeader, requested: ProtocolId) -> Result<()> {
    if answer.id != requested {
        return Err(Error::SecurityLayerRequired {
            requested,
            offered: answer.id,
        });
    }
    if !answer.version.is_1_0_0() {
        return Err(Error::VersionMismatch {
            offered: answer.version,
        });
    }
    Ok(())
}

/// The task that owns the wire once the handshake is done.
struct Driver {
    exec: Exec,
    options: Arc<ConnectionOptions>,
    remote: RemoteOpen,
    rx: mpsc::Receiver<Command>,
    state: watch::Sender<State>,
}

impl Driver {
    async fn run(mut self, mut reader: FrameReader, mut writer: FrameWriter) {
        // Ours to send: an empty frame at half the interval the peer asked
        // for. Ours to enforce: our own threshold, measured from the last
        // frame that arrived.
        let keepalive = self
            .remote
            .idle_time_out
            .map(|ms| Duration::from_millis(u64::from(ms)) / 2)
            .filter(|d| !d.is_zero());
        let deadline = self.options.idle_time_out;

        let (frames_tx, mut frames) = mpsc::channel::<Result<Vec<u8>>>(4);
        self.exec.spawn(async move {
            loop {
                let next = reader.next_frame().await;
                let failed = next.is_err();
                if frames_tx.send(next).await.is_err() || failed {
                    return;
                }
            }
        });

        let mut closing = false;
        let mut close_done: Option<oneshot::Sender<Result<()>>> = None;
        let outcome: State;

        loop {
            let tick = async {
                match keepalive {
                    Some(period) => self.exec.sleep(period).await,
                    None => std::future::pending().await,
                }
            };
            let expiry = async {
                match deadline {
                    Some(period) => self.exec.sleep(period).await,
                    None => std::future::pending().await,
                }
            };

            tokio::select! {
                biased;

                command = self.rx.recv() => match command {
                    Some(Command::Close { error, done }) => {
                        let result = write_close(&mut writer, error.as_ref()).await;
                        closing = true;
                        self.state.send_replace(State::Closing);
                        if let Err(error) = result {
                            let _ = done.send(Err(error));
                            outcome = State::Closed(None);
                            break;
                        }
                        close_done = Some(done);
                    }
                    // Every handle is gone and nobody asked for a close.
                    // Part 2 §2.4.3 wants one written anyway: a connection
                    // dropped without it leaves the peer to discover the
                    // loss by timeout.
                    None => {
                        if !closing {
                            let _ = write_close(&mut writer, None).await;
                        }
                        let _ = writer.shutdown().await;
                        outcome = State::Closed(None);
                        break;
                    }
                },

                incoming = frames.recv() => match incoming {
                    Some(Ok(bytes)) => {
                        match self.handle(&bytes, &mut writer, closing).await {
                            Ok(None) => {}
                            Ok(Some(state)) => {
                                if let Some(done) = close_done.take() {
                                    let _ = done.send(Ok(()));
                                }
                                outcome = state;
                                break;
                            }
                            Err(error) => {
                                if let Some(done) = close_done.take() {
                                    let _ = done.send(Err(Error::ConnectionGone));
                                }
                                outcome = State::Failed(error.to_string());
                                break;
                            }
                        }
                    }
                    gone => {
                        let message = match gone {
                            Some(Err(error)) => error.to_string(),
                            _ => "the frame reader stopped".to_owned(),
                        };
                        if let Some(done) = close_done.take() {
                            // The peer went away without answering, which is
                            // the case the close budget exists for.
                            let _ = done.send(Ok(()));
                            outcome = State::Closed(None);
                        } else {
                            outcome = State::Failed(message);
                        }
                        break;
                    }
                },

                () = tick, if keepalive.is_some() => {
                    if closing {
                        continue;
                    }
                    // An empty frame: a header, no body, no meaning beyond
                    // liveness. Channel 0, which the specification says it
                    // SHOULD be and MUST be before `open` has been received.
                    if let Err(error) = writer.send(&frame::empty(0)).await {
                        outcome = State::Failed(error.to_string());
                        break;
                    }
                }

                () = expiry, if deadline.is_some() => {
                    let after_ms = u32::try_from(
                        deadline.unwrap_or_default().as_millis()
                    ).unwrap_or(u32::MAX);
                    // SHOULD close with an error explaining why, and MAY then
                    // drop the socket.
                    let _ = write_close(
                        &mut writer,
                        Some(&Condition::described(
                            condition::CONNECTION_FORCED,
                            format!("no frame within the {after_ms} ms idle threshold"),
                        )),
                    )
                    .await;
                    let _ = writer.shutdown().await;
                    outcome = State::Failed(
                        Error::IdleTimeout { after_ms }.to_string(),
                    );
                    break;
                }
            }
        }

        let _ = writer.shutdown().await;
        self.state.send_replace(outcome);
    }

    /// One incoming frame. `Ok(Some(state))` ends the connection.
    async fn handle(
        &mut self,
        bytes: &[u8],
        writer: &mut FrameWriter,
        closing: bool,
    ) -> Result<Option<State>> {
        let frame = frame::decode(
            bytes,
            writer.max_frame_size().max(self.options.max_frame_size),
        )?;
        if frame.is_empty() {
            // The keep-alive. Nothing to do but have received it, which the
            // idle deadline already noticed by being reset.
            return Ok(None);
        }
        let (performative, _used) = Performative::decode(frame.body, Limits::DEFAULT)?;
        match performative {
            Performative::Close(Close { error }) => {
                let condition = error.as_ref().map(Condition::from_codec);
                if !closing {
                    // Simultaneous close is legal at both levels; the only
                    // observable difference is which error each side reports
                    // (Part 2 §2.4.4). Answering is what makes it orderly.
                    let _ = write_close(writer, None).await;
                }
                Ok(Some(State::Closed(condition)))
            }
            other if closing => {
                // We have sent `close` and are only reading until the
                // partner's arrives. Discarding is what the specification
                // asks for, and logging it is how a reader of the log finds
                // out the peer was still talking.
                tracing::debug!(
                    performative = other.name(),
                    "discarding a frame while closing"
                );
                Ok(None)
            }
            other => {
                // Sessions arrive in B-158. Until then a frame on any channel
                // is a frame for a session that was never begun, and the
                // honest answer is to say so rather than ignore it.
                let condition = Condition::described(
                    condition::NOT_ALLOWED,
                    format!(
                        "{} arrived on channel {} and this connection has no sessions",
                        other.name(),
                        frame.header.channel
                    ),
                );
                let _ = write_close(writer, Some(&condition)).await;
                Ok(Some(State::Closed(Some(condition))))
            }
        }
    }
}

/// Writes `close`, which is the last thing ever written on a connection.
async fn write_close(writer: &mut FrameWriter, error: Option<&Condition>) -> Result<()> {
    let mut out = Vec::new();
    let codec = error.map(Condition::as_codec);
    frame::write(
        &mut out,
        FrameKind::Amqp,
        0,
        writer.max_frame_size(),
        |body| Performative::Close(Close { error: codec }).encode(body),
    )?;
    writer.send(&out).await
}
