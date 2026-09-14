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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, mpsc, oneshot, watch};
use weida_amqp_codec::frame::{self, FrameKind, MIN_MAX_FRAME_SIZE};
use weida_amqp_codec::message::Reassembly;
use weida_amqp_codec::performative::{Close, End, Open, Performative};
use weida_amqp_codec::protocol_header::{ProtocolHeader, ProtocolId};
use weida_amqp_codec::types::condition;
use weida_amqp_codec::{Limits, Role};
use weida_runtime::Exec;

use crate::credit::Credit;
use crate::error::{Condition, Error, Result};
use crate::link::{self, Incoming, Link, LinkOptions, Links};
use crate::options::{ConnectionOptions, Sasl, TlsMode, multiple};
use crate::sasl;
use crate::session::{
    self, Entry, SESSION_QUEUE, Session, SessionEvent, SessionOptions, SessionState, Shared, Table,
};
use crate::settlement::{DEFAULT_MAX_UNSETTLED, Outcome, Settlement, Unsettled};
use crate::transport::{self, FrameReader, FrameWriter, Wire};
use crate::window::Windows;

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

    /// Why nothing further can be sent, as the error a caller gets.
    ///
    /// `None` while the connection is usable. The peer's own condition is
    /// kept rather than replaced by "the connection is gone": the condition
    /// is the only explanation the protocol carries, and a caller that asked
    /// for a session on a connection the peer refused wants to read
    /// `amqp:resource-limit-exceeded`, not a synonym for silence.
    #[must_use]
    pub fn refusal(&self) -> Option<Error> {
        match self {
            Self::Open => None,
            Self::Closing | Self::Closed(None) => Some(Error::ConnectionGone),
            Self::Closed(Some(condition)) => Some(Error::Closed(Some(condition.clone()))),
            // The reason a connection failed went to whoever was waiting on
            // the operation that failed; it stays readable through
            // [`Connection::state`]. A caller arriving afterwards is told the
            // connection is gone, which is all its own call can act on.
            Self::Failed(_) => Some(Error::ConnectionGone),
        }
    }
}

/// What the driver accepts from a handle.
enum Command {
    /// Begin a session on the lowest free outgoing channel, and report the
    /// handle once the answering `begin` has tied the two channel numberings
    /// together.
    Begin {
        options: SessionOptions,
        reply: oneshot::Sender<Result<Session>>,
    },
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

/// What a session asks the connection driver to do.
///
/// Sessions do not own the transport: everything they send goes through the
/// driver, which is what keeps `close` the last thing ever written and what
/// lets the driver refuse a frame once it has sent one. Attaching is a
/// request rather than a frame because the driver owns the handle space and
/// the link table.
#[derive(Debug)]
pub(crate) enum Outbound {
    /// A whole frame, channel already in its header.
    Frame(Vec<u8>),
    /// Attach a link on the lowest free handle of this session.
    Attach {
        /// The session's outgoing channel.
        channel: u16,
        /// Boxed because `LinkOptions` carries two termini and this enum's
        /// other variant is one `Vec`.
        options: Box<LinkOptions>,
        /// Answered once the answering `attach` has arrived.
        reply: oneshot::Sender<Result<Link>>,
    },
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
        let (outbound, outbound_rx) = mpsc::channel(OUTGOING_QUEUE);
        let (state_tx, state) = watch::channel(State::Open);
        let driver = Driver {
            exec: exec.clone(),
            options: Arc::clone(&options),
            remote: remote.clone(),
            rx,
            outbound_rx,
            outbound: outbound.clone(),
            state: state_tx,
            sessions: Table::default(),
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

    /// Begins a session on the lowest free outgoing channel.
    ///
    /// Returns once the answering `begin` has arrived, because until then
    /// there is no incoming channel to route the peer's frames by and
    /// therefore nothing a caller could do with the handle. The channel is
    /// the lowest free one, as Part 2 §2.5.1 recommends, and the bound on how
    /// many sessions there can be is the *peer's* `channel-max` — the number
    /// that says which channels it will accept.
    pub async fn begin(&self, options: SessionOptions) -> Result<Session> {
        if let Some(refusal) = self.state().refusal() {
            return Err(refusal);
        }
        let (reply, wait) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Begin { options, reply })
            .await
            .map_err(|_| Error::ConnectionGone)?;
        match self
            .inner
            .exec
            .within(self.inner.options.handshake_timeout, wait)
            .await
        {
            Some(Ok(result)) => result,
            Some(Err(_)) => Err(Error::ConnectionGone),
            None => Err(Error::HandshakeTimeout {
                step: "the answering begin",
            }),
        }
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
    outbound_rx: mpsc::Receiver<Outbound>,
    /// Kept so that a `Session` handed to a caller can be given a sender
    /// without the driver having to clone one out of thin air.
    outbound: mpsc::Sender<Outbound>,
    state: watch::Sender<State>,
    sessions: Table,
}

impl Driver {
    async fn run(mut self, mut reader: FrameReader, mut writer: FrameWriter) {
        // Ours to send: an empty frame at half the interval the peer asked
        // for. Ours to enforce: our own threshold, measured from the last
        // frame that arrived.
        //
        // Both are deadlines that outlive a turn of the loop below, not
        // timers built inside it. A `Sleep` created in the loop body restarts
        // from zero whenever any *other* `select!` branch completes, so
        // neither sentence above would hold under traffic: a connection with
        // commands flowing would never emit the empty frame and the peer
        // would close on its own threshold, and its read threshold would
        // never be reached however long the peer had been silent, because
        // this side's own sends kept postponing it.
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

        // Both start from the handshake — a peer that says nothing at all
        // after its `open` must still be timed out — and are re-armed at the
        // top of each turn from the event their rule names: `tick` from the
        // last frame *this side* wrote, which the writer records for every
        // path that writes one, and `expiry` from the last frame that
        // arrived. Re-arming beats rebuilding: a `Sleep` is one slot on the
        // timer wheel either way, and resetting it here is the only place
        // that has to be read to see what each deadline measures.
        let mut last_frame = tokio::time::Instant::now();
        let mut tick = std::pin::pin!(self.exec.sleep(Duration::ZERO));
        let mut expiry = std::pin::pin!(self.exec.sleep(Duration::ZERO));

        loop {
            // The guards on the two timer arms disable them when the
            // connection asked for no timeout, so an unarmed `Sleep` is never
            // awaited.
            if let Some(period) = keepalive {
                tick.as_mut().reset(writer.last_write() + period);
            }
            if let Some(period) = deadline {
                expiry.as_mut().reset(last_frame + period);
            }

            tokio::select! {
                biased;

                command = self.rx.recv() => match command {
                    Some(Command::Begin { options, reply }) => {
                        if closing {
                            let _ = reply.send(Err(Error::ConnectionGone));
                            continue;
                        }
                        if let Err(error) = self.begin(options, reply, &mut writer).await {
                            outcome = State::Failed(error.to_string());
                            break;
                        }
                    }

                    Some(Command::Close { error, done }) => {
                        // Everything already handed to the driver is written
                        // first. "`close` MUST be the last frame ever
                        // written" makes it *last*; it is not a licence to
                        // drop a transfer a sender had already completed
                        // (Part 2 §2.4.3).
                        if let Err(error) = self.flush_outbound(&mut writer).await {
                            let _ = done.send(Err(error));
                            outcome = State::Closed(None);
                            break;
                        }
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
                            let _ = self.flush_outbound(&mut writer).await;
                            let _ = write_close(&mut writer, None).await;
                        }
                        let _ = writer.shutdown().await;
                        outcome = State::Closed(None);
                        break;
                    }
                },

                out = self.outbound_rx.recv() => match out {
                    Some(Outbound::Frame(bytes)) => {
                        // `close` MUST be the last thing ever written, so a
                        // session frame that raced the close is dropped
                        // rather than written after it.
                        if closing {
                            continue;
                        }
                        if let Err(error) = writer.send(&bytes).await {
                            outcome = State::Failed(error.to_string());
                            break;
                        }
                    }
                    Some(Outbound::Attach {
                        channel,
                        options,
                        reply,
                    }) => {
                        if closing {
                            let _ = reply.send(Err(Error::ConnectionGone));
                            continue;
                        }
                        if let Err(error) =
                            self.attach(channel, *options, reply, &mut writer).await
                        {
                            outcome = State::Failed(error.to_string());
                            break;
                        }
                    }
                    // Only the driver's own clone is left, which cannot
                    // happen while the driver runs.
                    None => continue,
                },

                incoming = frames.recv() => match incoming {
                    Some(Ok(bytes)) => {
                        // The one event the read threshold measures from. It
                        // moves here and nowhere else, so a connection whose
                        // peer has silently gone is timed out even while this
                        // side keeps sending.
                        last_frame = tokio::time::Instant::now();
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

                () = tick.as_mut(), if keepalive.is_some() => {
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

                () = expiry.as_mut(), if deadline.is_some() => {
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

        // "Sessions also end automatically when the connection closes or is
        // interrupted" (Part 2 §2.5.2). Telling each one is what lets a
        // caller holding a `Session` find out, rather than waiting on a
        // channel nobody will ever send on.
        let ended = match &outcome {
            State::Closed(condition) => condition.clone(),
            State::Failed(why) => Some(Condition::described(
                condition::CONNECTION_FORCED,
                why.clone(),
            )),
            _ => None,
        };
        for entry in self.sessions.drain() {
            *entry.shared.state.lock().expect("not poisoned") = SessionState::Ended(ended.clone());
            let _ = entry.events.send(SessionEvent::Ended(ended.clone())).await;
        }
        let _ = writer.shutdown().await;
        self.state.send_replace(outcome);
    }

    /// Writes everything already queued, without waiting for more.
    ///
    /// Called before `close` so that a frame a caller had already handed
    /// over is not overtaken by it. Only what is queued *now* is written: a
    /// frame offered after the close was asked for is still dropped, because
    /// by then `close` is the last frame and that is the whole rule.
    async fn flush_outbound(&mut self, writer: &mut FrameWriter) -> Result<()> {
        while let Ok(out) = self.outbound_rx.try_recv() {
            match out {
                Outbound::Frame(bytes) => writer.send(&bytes).await?,
                Outbound::Attach { reply, .. } => {
                    let _ = reply.send(Err(Error::ConnectionGone));
                }
            }
        }
        Ok(())
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
        let (performative, used) = Performative::decode(frame.body, Limits::DEFAULT)?;
        // Everything after the performative in the same body is the
        // `transfer` payload, byte for byte — Part 2 §2.7.5 defines it as
        // exactly that and nothing parses it here.
        let payload = &frame.body[used..];
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
                self.route(frame.header.channel, other, payload, bytes, writer)
                    .await
            }
        }
    }

    /// Begins a session: lowest free outgoing channel, `begin` on the wire,
    /// and the caller parked until the answer ties the two numberings
    /// together.
    async fn begin(
        &mut self,
        options: SessionOptions,
        reply: oneshot::Sender<Result<Session>>,
        writer: &mut FrameWriter,
    ) -> Result<()> {
        // The bound is the peer's `channel-max`: it is the number that says
        // which channels the peer will accept, and ours says nothing about
        // what we may use.
        let Some(channel) = self.sessions.lowest_free(self.remote.channel_max) else {
            let _ = reply.send(Err(Error::Local(Condition::described(
                condition::RESOURCE_LIMIT_EXCEEDED,
                format!(
                    "every channel up to the peer's channel-max of {} is in use",
                    self.remote.channel_max
                ),
            ))));
            return Ok(());
        };

        let windows = Windows::new(0, options.incoming_window, options.outgoing_window);
        let bytes = match session::begin_frame(channel, &options, &windows) {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = reply.send(Err(error));
                return Ok(());
            }
        };

        let (events, rx) = mpsc::channel(SESSION_QUEUE);
        self.sessions.insert(
            channel,
            Entry {
                shared: Arc::new(Shared {
                    outgoing_channel: channel,
                    incoming_channel: Mutex::new(None),
                    windows: Mutex::new(windows),
                    state: Mutex::new(SessionState::Beginning),
                    options,
                }),
                events,
                remote_handle_max: 0,
                links: Links::default(),
                pending: Some(reply),
                outbound: self.outbound.clone(),
                rx: Some(rx),
            },
        );
        writer.send(&bytes).await
    }

    /// Attaches a link: lowest free handle, `attach` on the wire, and the
    /// caller parked until the answer says what the peer actually created.
    ///
    /// A link whose name and direction are already attached steals the
    /// incumbent, which is detached with `amqp:link:stolen` first
    /// (Part 2 §2.6.1).
    async fn attach(
        &mut self,
        channel: u16,
        options: LinkOptions,
        reply: oneshot::Sender<Result<Link>>,
        writer: &mut FrameWriter,
    ) -> Result<()> {
        // Read before the table is borrowed: the bound is the peer's
        // `handle-max` from its answering `begin`, and the frame ceiling is
        // the peer's `max-frame-size` from its `open` — a message is split on
        // what the *peer* will accept, never on what we would.
        let handle_max = self.remote_handle_max(channel);
        let outgoing_frame_size = self.remote.outgoing_frame_size();
        let Some(entry) = self.sessions.get_mut(channel) else {
            let _ = reply.send(Err(Error::ConnectionGone));
            return Ok(());
        };

        // The steal. Doing it before allocating a handle means the
        // incumbent's handle is not a candidate for the newcomer, which
        // matters because a stolen link is an errored one.
        let stolen = entry.links.by_name(&options.name, options.role);
        if let Some(incumbent) = stolen {
            let condition = Condition::described(
                link::STOLEN,
                format!(
                    "link {} was attached again in the same direction",
                    options.name
                ),
            );
            let _ = write_detach(writer, channel, incumbent, true, Some(&condition)).await;
            if let Some(loser) = entry.links.remove(incumbent, true) {
                *loser.shared.state.lock().expect("not poisoned") =
                    crate::link::LinkState::Detached(Some(condition.clone()));
                let _ = loser
                    .events
                    .send(crate::link::LinkEvent::Detached(Some(condition)))
                    .await;
            }
        }

        let Some(handle) = entry.links.lowest_free(handle_max) else {
            let _ = reply.send(Err(Error::Local(Condition::described(
                condition::RESOURCE_LIMIT_EXCEEDED,
                format!("every handle up to the peer's handle-max of {handle_max} is in use"),
            ))));
            return Ok(());
        };

        let bytes = match link::attach_frame(channel, handle, &options) {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = reply.send(Err(error));
                return Ok(());
            }
        };

        let (events, rx) = mpsc::channel(link::LINK_QUEUE);
        let credit = match options.role {
            // A sender starts where its own `attach.initial-delivery-count`
            // said and with no credit at all: nothing may go out until the
            // receiver grants.
            Role::Sender => Credit::sender(options.initial_delivery_count),
            Role::Receiver => Credit::receiver(),
        };
        // Our own `max-message-size` bounds what may land in our buffer; the
        // peer's bounds what we may send, and that one arrives in the
        // answering `attach`.
        let ours = options.max_message_size.unwrap_or(0);
        let session = Arc::clone(&entry.shared);
        entry.links.insert(
            handle,
            crate::link::Entry {
                shared: Arc::new(crate::link::Shared {
                    name: options.name.clone(),
                    role: options.role,
                    output_handle: handle,
                    channel,
                    state: Mutex::new(crate::link::LinkState::Attaching),
                    negotiated: Mutex::new(None),
                    input_handle: Mutex::new(None),
                    credit: Mutex::new(credit),
                    session,
                    flow: Notify::new(),
                    max_frame_size: outgoing_frame_size,
                    next_tag: Mutex::new(0),
                    unsettled: Mutex::new(Unsettled::new(DEFAULT_MAX_UNSETTLED)),
                }),
                events,
                pending: Some(reply),
                outbound: self.outbound.clone(),
                rx: Some(rx),
                options: Box::new(options),
                reassembly: Reassembly::new(ours),
            },
        );
        writer.send(&bytes).await
    }

    /// The `handle-max` the peer advertised for this session.
    ///
    /// Kept per session because `begin.handle-max` is per session, and
    /// falling back to our own would be using the wrong side's number: the
    /// peer's is what says which handles it will accept.
    fn remote_handle_max(&mut self, channel: u16) -> u32 {
        self.sessions
            .get_mut(channel)
            .map_or(0, |entry| entry.remote_handle_max)
    }

    /// One frame for a session, or for a channel that has none.
    async fn route(
        &mut self,
        channel: u16,
        performative: Performative<'_>,
        payload: &[u8],
        bytes: &[u8],
        writer: &mut FrameWriter,
    ) -> Result<Option<State>> {
        // A channel above what we advertised is a framing error and the
        // specification says so: "out-of-range channel MUST close the
        // connection with amqp:connection:framing-error" (Part 2 §2.7.1).
        if channel > self.options.channel_max {
            let condition = Condition::described(
                condition::CONNECTION_FRAMING_ERROR,
                format!(
                    "channel {channel} is above the channel-max of {} this connection advertised",
                    self.options.channel_max
                ),
            );
            let _ = write_close(writer, Some(&condition)).await;
            return Ok(Some(State::Closed(Some(condition))));
        }

        // The answering `begin` is the one frame that arrives on a channel
        // with no mapping yet, and its `remote-channel` is what creates the
        // mapping.
        if let Performative::Begin(begin) = &performative
            && let Some(ours) = begin.remote_channel
            && !self.sessions.is_mapped(channel)
        {
            return self.complete_begin(channel, ours, begin);
        }

        if self.sessions.by_incoming(channel).is_none() {
            // In range, but no session was ever begun on it. The
            // specification names no condition for this case - it covers
            // only the out-of-range one above - so the condition is ours and
            // the description says which channel it was.
            let condition = Condition::described(
                session::UNMAPPED_CHANNEL_CONDITION,
                format!(
                    "{} arrived on channel {channel}, which is not mapped to a session",
                    performative.name()
                ),
            );
            let _ = write_close(writer, Some(&condition)).await;
            return Ok(Some(State::Closed(Some(condition))));
        }

        // A session that sent or received `end(error=...)` discards
        // everything until the partner's `end`. Counted and logged, never
        // acted upon.
        let discarding = self.sessions.by_incoming(channel).is_some_and(|entry| {
            entry
                .shared
                .state
                .lock()
                .expect("not poisoned")
                .is_discarding()
        });
        if discarding && !matches!(performative, Performative::End(_)) {
            tracing::debug!(
                channel,
                performative = performative.name(),
                "discarding a frame for a session in DISCARDING"
            );
            return Ok(None);
        }

        // `attach`, `detach` and `disposition` belong to the link layer, and
        // they are taken first because they change tables the session's own
        // bookkeeping does not touch. A `disposition` names no handle at all
        // — it names a range of delivery-ids and a role — so it is the one
        // frame that has to be offered to every link of the session.
        match &performative {
            Performative::Attach(attach) => {
                return self.route_attach(channel, attach, writer).await;
            }
            Performative::Detach(detach) => {
                return self.route_detach(channel, detach, writer).await;
            }
            Performative::Disposition(disposition) => {
                return self.route_disposition(channel, disposition, writer).await;
            }
            _ => {}
        }

        let entry = self
            .sessions
            .by_incoming(channel)
            .expect("checked just above");
        match &performative {
            Performative::Flow(flow) => {
                entry
                    .shared
                    .windows
                    .lock()
                    .expect("not poisoned")
                    .apply_flow(session::remote_flow_of_flow(flow));
                // The session window may have grown, and a sender parked on
                // it is parked on its link. Every `flow` refreshes session
                // state whether or not it names a handle, so every `flow`
                // wakes them.
                entry.links.wake_all();
            }
            Performative::Transfer(_) => {
                let violation = {
                    let mut windows = entry.shared.windows.lock().expect("not poisoned");
                    let id = windows.next_incoming_id;
                    windows.record_received(id)
                };
                if let Err(violation) = violation {
                    // Part 2 §2.8.17: `end` the session with
                    // `amqp:session:window-violation`.
                    let condition = Condition::described(
                        condition::SESSION_WINDOW_VIOLATION,
                        violation.to_string(),
                    );
                    let outgoing = entry.shared.outgoing_channel;
                    *entry.shared.state.lock().expect("not poisoned") =
                        SessionState::Discarding(condition.clone());
                    let _ = write_end(writer, outgoing, Some(&condition)).await;
                    return Ok(None);
                }
            }
            Performative::End(End { error }) => {
                let condition = error.as_ref().map(Condition::from_codec);
                let outgoing = entry.shared.outgoing_channel;
                let answered = matches!(
                    *entry.shared.state.lock().expect("not poisoned"),
                    SessionState::Ending | SessionState::Discarding(_)
                );
                *entry.shared.state.lock().expect("not poisoned") =
                    SessionState::Ended(condition.clone());
                let events = entry.events.clone();
                if !answered {
                    // Answering is what makes it orderly; simultaneous `end`
                    // is legal and the only difference is which condition
                    // each side reports.
                    let _ = write_end(writer, outgoing, None).await;
                }
                // "Sessions also end automatically when the connection
                // closes"; a session ending takes its links with it for the
                // same reason.
                if let Some(session) = self.sessions.remove(outgoing) {
                    drain_links(session, condition.clone()).await;
                }
                let _ = events.send(SessionEvent::Ended(condition)).await;
                return Ok(None);
            }
            _ => {}
        }

        // A frame naming a handle goes to the link; anything else goes to
        // the session.
        if let Some(handle) = addressed_handle(&performative) {
            let attached = self
                .sessions
                .by_incoming(channel)
                .is_some_and(|entry| entry.links.by_input(handle).is_some());
            if attached {
                return self
                    .route_to_link(channel, handle, &performative, payload, bytes, writer)
                    .await;
            }
            let Some(entry) = self.sessions.by_incoming(channel) else {
                return Ok(None);
            };
            if entry.links.was_detached(handle) {
                // A frame for a link that was cleanly detached is a race with
                // the `detach`, not a fault: the peer's frame may already have
                // been in flight when the two crossed. `fe2o3-amqp`'s
                // acceptor really does send a `flow` for a handle it has just
                // detached, and ending the session over it would make every
                // orderly link close a coin toss.
                tracing::debug!(
                    handle,
                    performative = performative.name(),
                    "discarding a frame for a link that was already detached"
                );
                return Ok(None);
            }
            // Two different conditions, and the specification distinguishes
            // them. A handle detached **with an error**: "any later input on
            // that handle or its delivery-ids MUST end the session with
            // `amqp:session:errant-link`" (Part 2 §2.6.5). A handle never
            // attached at all: `amqp:session:unattached-handle` (§2.8.17).
            let condition = if entry.links.was_errant(handle) {
                Condition::described(
                    condition::SESSION_ERRANT_LINK,
                    format!(
                        "{} arrived on handle {handle}, whose link was detached with an error",
                        performative.name()
                    ),
                )
            } else {
                Condition::described(
                    condition::SESSION_UNATTACHED_HANDLE,
                    format!(
                        "{} arrived on handle {handle}, which is not an attached link",
                        performative.name()
                    ),
                )
            };
            let outgoing = entry.shared.outgoing_channel;
            *entry.shared.state.lock().expect("not poisoned") =
                SessionState::Discarding(condition.clone());
            let _ = write_end(writer, outgoing, Some(&condition)).await;
            return Ok(None);
        }

        let entry = self
            .sessions
            .by_incoming(channel)
            .expect("checked just above");
        let _ = entry.events.send(SessionEvent::Frame(bytes.to_vec())).await;
        Ok(None)
    }

    /// A `disposition` from the peer.
    ///
    /// It names no handle. `role` names the *speaker*, so a receiver's
    /// `disposition` is about deliveries this end sent and a sender's is
    /// about deliveries this end received; the frame is offered to every link
    /// of the session in the addressed direction, and a link that holds none
    /// of the ids simply does nothing. That is also where idempotence comes
    /// from: a repeated `disposition` finds nothing to change.
    async fn route_disposition(
        &mut self,
        channel: u16,
        disposition: &weida_amqp_codec::performative::Disposition<'_>,
        writer: &mut FrameWriter,
    ) -> Result<Option<State>> {
        let first = disposition.first;
        // "The highest delivery-id covered; unset means `first` alone."
        let last = disposition.last.unwrap_or(first);
        let state = match &disposition.state {
            Some(value) => Some(weida_amqp_codec::state::DeliveryState::from_value(
                value.clone(),
            )?),
            None => None,
        };
        let outcome = match &state {
            Some(state) => Outcome::of(state)?,
            None => None,
        };

        let Some(session) = self.sessions.by_incoming(channel) else {
            return Ok(None);
        };
        let outgoing = session.shared.outgoing_channel;
        let mut owed: Vec<(u32, u32, Outcome)> = Vec::new();
        let mut reports: Vec<(mpsc::Sender<crate::link::LinkEvent>, u32, Outcome, bool)> =
            Vec::new();
        for link in session.links.addressed(disposition.role) {
            let second = link.second_mode();
            let events = link.events.clone();
            let handle = link.shared.output_handle;
            for (delivery_id, settlement) in
                link.accept_disposition(first, last, outcome.as_ref(), disposition.settled)
            {
                match settlement {
                    Settlement::Nothing | Settlement::Progress => {}
                    Settlement::Settled(outcome) => {
                        reports.push((events.clone(), delivery_id, outcome, true));
                    }
                    Settlement::Provisional(outcome) => {
                        // `rcv-settle-mode=second`: the receiver published an
                        // outcome and is waiting for this end to settle
                        // before it settles itself. Owed by the driver,
                        // because the application has nothing left to decide
                        // — and once written, the delivery is over here, so
                        // the report is final.
                        let final_here = second && link.shared.role == Role::Sender;
                        if final_here {
                            owed.push((handle, delivery_id, outcome.clone()));
                        }
                        reports.push((events.clone(), delivery_id, outcome, final_here));
                    }
                }
            }
        }

        for (_, delivery_id, outcome) in &owed {
            write_disposition(
                writer,
                outgoing,
                Role::Sender,
                *delivery_id,
                Some(outcome),
                true,
            )
            .await?;
        }
        if !owed.is_empty() {
            // Settled here as well now, so the delivery is over at this end.
            if let Some(session) = self.sessions.by_incoming(channel) {
                for (handle, delivery_id, _) in &owed {
                    if let Some(link) = session.links.get_mut(*handle) {
                        link.settle_here(*delivery_id);
                    }
                }
            }
        }
        for (events, delivery_id, outcome, settled) in reports {
            let _ = events
                .send(crate::link::LinkEvent::Outcome {
                    delivery_id,
                    outcome,
                    settled,
                })
                .await;
        }
        Ok(None)
    }

    /// One frame for a link that is attached: a `transfer`, a `flow` naming a
    /// handle, or anything else this layer does not read.
    ///
    /// The work is done in two halves on purpose. The first borrows the link
    /// out of the session table and decides; the second writes and, where the
    /// link must go, takes it out of the table — which cannot happen while
    /// the borrow is alive.
    async fn route_to_link(
        &mut self,
        channel: u16,
        handle: u32,
        performative: &Performative<'_>,
        payload: &[u8],
        bytes: &[u8],
        writer: &mut FrameWriter,
    ) -> Result<Option<State>> {
        let Some(session) = self.sessions.by_incoming(channel) else {
            return Ok(None);
        };
        let outgoing = session.shared.outgoing_channel;
        // A snapshot, because an answering `flow` carries the session's three
        // mandatory fields and the link must not hold the window lock while
        // it builds one.
        let windows = *session.shared.windows.lock().expect("not poisoned");
        let Some(link) = session.links.by_input(handle) else {
            return Ok(None);
        };
        let events = link.events.clone();
        let output = link.shared.output_handle;
        let decision = match performative {
            Performative::Transfer(transfer) => {
                Decision::Incoming(link.accept_transfer(transfer, payload))
            }
            Performative::Flow(flow) => {
                Decision::Flow(link.accept_flow(flow, &windows), link.credit())
            }
            _ => Decision::Frame,
        };

        match decision {
            Decision::Incoming(Incoming::Nothing) => {}
            Decision::Incoming(Incoming::Delivery(delivery)) => {
                if !delivery.settled {
                    // The sender has not settled, so this end keeps state for
                    // the delivery until the application says what happened.
                    // A delivery that arrived settled is recorded nowhere:
                    // there is nothing to answer and nothing the answer could
                    // change.
                    if let Some(session) = self.sessions.by_incoming(channel)
                        && let Some(link) = session.links.by_input(handle)
                    {
                        link.arrived(delivery.delivery_id, delivery.delivery_tag.clone());
                    }
                }
                let _ = events
                    .send(crate::link::LinkEvent::Delivery(delivery))
                    .await;
            }
            Decision::Incoming(Incoming::Refused(condition)) => {
                // "An errored link endpoint MUST be detached with
                // detach(error=...) and destroyed", and its handle is
                // poisoned for the life of the session so that a late frame
                // for the dead link cannot land on a live one.
                let _ = write_detach(writer, outgoing, output, true, Some(&condition)).await;
                if let Some(session) = self.sessions.by_incoming(channel)
                    && let Some(link) = session.links.remove(output, true)
                {
                    *link.shared.state.lock().expect("not poisoned") =
                        crate::link::LinkState::Detached(Some(condition.clone()));
                }
                let _ = events
                    .send(crate::link::LinkEvent::Detached(Some(condition)))
                    .await;
            }
            Decision::Flow(answer, credit) => {
                let _ = events.send(crate::link::LinkEvent::Flow(credit)).await;
                if let Some(answer) = answer {
                    write_flow(writer, outgoing, answer).await?;
                }
            }
            Decision::Frame => {
                let _ = events
                    .send(crate::link::LinkEvent::Frame(bytes.to_vec()))
                    .await;
            }
        }
        Ok(None)
    }

    /// An `attach` from the peer: either the answer to ours, or an
    /// unsolicited one.
    async fn route_attach(
        &mut self,
        channel: u16,
        attach: &weida_amqp_codec::performative::Attach<'_>,
        writer: &mut FrameWriter,
    ) -> Result<Option<State>> {
        let entry = self
            .sessions
            .by_incoming(channel)
            .expect("the caller checked the channel");
        let outgoing = entry.shared.outgoing_channel;

        // "Attaching on a handle already in use MUST be answered with an
        // immediate close carrying amqp:session:handle-in-use"
        // (Part 2 §2.6.2). The handle space in question is the *input* one:
        // the peer's own numbering.
        if entry.links.input_in_use(attach.handle) {
            let condition = Condition::described(
                condition::SESSION_HANDLE_IN_USE,
                format!("handle {} is already attached", attach.handle),
            );
            let _ = write_close(writer, Some(&condition)).await;
            return Ok(Some(State::Closed(Some(condition))));
        }

        // The answer is correlated by *name*, because the two ends choose
        // their handles independently and the numbers do not agree.
        let ours = entry.links.by_name(attach.name, attach.role.opposite());
        let Some(ours) = ours else {
            // An unsolicited `attach`. This client holds no nodes — AMQP 1.0
            // "defines no operation to create, configure, enumerate or delete
            // a node" and a client is not a broker — so the specified refusal
            // is an answering `attach` with the terminus null followed by an
            // immediate `detach` (Part 2 §2.6.3).
            let refusal = refuse_attach(channel, attach)?;
            writer.send(&refusal).await?;
            let _ = write_detach(writer, outgoing, attach.handle, true, None).await;
            tracing::debug!(
                link = attach.name,
                handle = attach.handle,
                "refused an unsolicited attach: this client offers no nodes"
            );
            return Ok(None);
        };

        let Some(link) = entry.links.get_mut(ours) else {
            return Ok(None);
        };
        let negotiated = match link::negotiate(&link.options, attach) {
            Ok(negotiated) => negotiated,
            Err(error) => {
                if let Some(reply) = link.pending.take() {
                    let _ = reply.send(Err(error));
                }
                return Ok(None);
            }
        };
        *link.shared.input_handle.lock().expect("not poisoned") = Some(attach.handle);
        *link.shared.negotiated.lock().expect("not poisoned") = Some(negotiated);
        if link.shared.role == Role::Receiver {
            // Where the sender's sequence starts. A receiver's credit is a
            // distance from a `delivery-count` it does not choose, so until
            // this arrives it cannot name one in a `flow` — and MUST NOT
            // (Part 2 §2.7.4).
            link.shared
                .credit
                .lock()
                .expect("not poisoned")
                .seen_sender_attach(attach.initial_delivery_count.unwrap_or_default());
        }
        *link.shared.state.lock().expect("not poisoned") = crate::link::LinkState::Attached;
        let handed = match (link.pending.take(), link.rx.take()) {
            (Some(reply), Some(rx)) => {
                let handle = Link::new(Arc::clone(&link.shared), link.outbound.clone(), rx);
                let _ = reply.send(Ok(handle));
                true
            }
            _ => false,
        };
        entry.links.map_input(attach.handle, ours);
        tracing::debug!(
            link = attach.name,
            output = ours,
            input = attach.handle,
            links = entry.links.len(),
            handed_over = handed,
            "link attached"
        );
        Ok(None)
    }

    /// A `detach` from the peer.
    ///
    /// Answered, unless this end sent one first. A link endpoint is destroyed
    /// only when **both** ends have detached (Part 2 §2.6.4), so a peer that
    /// initiated the detach is waiting for this frame — and a peer whose
    /// application holds the link until it arrives waits forever without it.
    /// `fe2o3-amqp`'s acceptor is such a peer: its `close()` does not return
    /// until the answering `detach` lands.
    async fn route_detach(
        &mut self,
        channel: u16,
        detach: &weida_amqp_codec::performative::Detach<'_>,
        writer: &mut FrameWriter,
    ) -> Result<Option<State>> {
        let entry = self
            .sessions
            .by_incoming(channel)
            .expect("the caller checked the channel");
        let outgoing = entry.shared.outgoing_channel;
        let condition = detach.error.as_ref().map(Condition::from_codec);
        let Some(link) = entry.links.by_input(detach.handle) else {
            tracing::debug!(
                handle = detach.handle,
                "a detach for a handle that is not attached"
            );
            return Ok(None);
        };
        let output = link.shared.output_handle;
        // Whether this end had already sent its own `detach`; answering twice
        // would be a frame for a link that no longer exists at the peer.
        let answered = matches!(
            *link.shared.state.lock().expect("not poisoned"),
            crate::link::LinkState::Detaching | crate::link::LinkState::Detached(_)
        );
        *link.shared.state.lock().expect("not poisoned") =
            crate::link::LinkState::Detached(condition.clone());
        let events = link.events.clone();
        // An errored endpoint's handle is never reused, because the peer is
        // entitled to still be sending frames for it.
        let errored = condition.is_some();
        entry.links.remove(output, errored);
        if !answered {
            // The same `closed` flag the peer used: answering `closed=true`
            // with `closed=false` would claim the deliveries are still live.
            let _ = write_detach(writer, outgoing, output, detach.closed, None).await;
        }
        let _ = events
            .send(crate::link::LinkEvent::Detached(condition))
            .await;
        Ok(None)
    }

    /// The answering `begin`: tie the peer's outgoing channel to ours and
    /// hand the caller its handle.
    fn complete_begin(
        &mut self,
        incoming: u16,
        ours: u16,
        begin: &weida_amqp_codec::performative::Begin<'_>,
    ) -> Result<Option<State>> {
        let exec = self.exec.clone();
        let attach_timeout = self.options.handshake_timeout;
        let Some(entry) = self.sessions.get_mut(ours) else {
            // A `remote-channel` naming a session we never begun. Nothing to
            // attach it to, and inventing one would be inventing state.
            tracing::warn!(
                incoming,
                remote_channel = ours,
                "an answering begin named a channel with no session"
            );
            return Ok(None);
        };
        *entry.shared.incoming_channel.lock().expect("not poisoned") = Some(incoming);
        entry
            .shared
            .windows
            .lock()
            .expect("not poisoned")
            .apply_begin(session::remote_flow_of_begin(begin));
        // The bound on this session's link table, and the peer's number: it
        // states which handles the peer will accept.
        entry.remote_handle_max = begin.handle_max;
        *entry.shared.state.lock().expect("not poisoned") = SessionState::Begun;
        let handle = match (entry.pending.take(), entry.rx.take()) {
            (Some(reply), Some(rx)) => {
                let session = Session::new(
                    Arc::clone(&entry.shared),
                    entry.outbound.clone(),
                    rx,
                    exec,
                    attach_timeout,
                );
                let _ = reply.send(Ok(session));
                true
            }
            // A second `begin` for a session already answered. The peer is
            // confused; the mapping is already right and there is nobody
            // waiting.
            _ => false,
        };
        self.sessions.map_incoming(incoming, ours);
        tracing::debug!(
            outgoing = ours,
            incoming,
            handed_over = handle,
            sessions = self.sessions.len(),
            "session begun"
        );
        Ok(None)
    }
}

/// What the driver decided about one frame while it held the link, so that
/// the writing and the table surgery happen after the borrow has ended.
#[derive(Debug)]
enum Decision {
    /// A `transfer`, accounted for and reassembled as far as it goes.
    Incoming(Incoming),
    /// A `flow`: the answer this end owes, if any, and the credit state the
    /// application should see.
    Flow(
        Option<weida_amqp_codec::performative::Flow<'static>>,
        Credit,
    ),
    /// Anything else naming a handle, handed over whole.
    Frame,
}

/// The handle a frame names, where it names one.
///
/// `flow` may carry link state on top of session state and may not, which is
/// why its handle is an `Option` and `transfer` and `flow` are not treated
/// alike: a `disposition` names no handle at all, it names a range of
/// delivery-ids on the session.
const fn addressed_handle(performative: &Performative<'_>) -> Option<u32> {
    match performative {
        Performative::Transfer(transfer) => Some(transfer.handle),
        Performative::Flow(flow) => flow.handle,
        _ => None,
    }
}

/// Tells every link of an ending session that it is over.
///
/// A session ending destroys its link endpoints, so a caller holding a
/// [`Link`] finds out here rather than waiting on a channel nobody will send
/// on again.
async fn drain_links(mut session: session::Entry, condition: Option<Condition>) {
    for link in session.links.drain() {
        *link.shared.state.lock().expect("not poisoned") =
            crate::link::LinkState::Detached(condition.clone());
        let _ = link
            .events
            .send(crate::link::LinkEvent::Detached(condition.clone()))
            .await;
    }
}

/// The specified refusal of an `attach` this client cannot honour: an
/// answering `attach` with both termini null.
///
/// Part 2 §2.6.3: "a partner that will not provide a terminus answers with
/// that field null, which establishes a link to a nonexistent terminus, and
/// MUST then immediately detach." An error code would be inventing one; this
/// is the absence the specification asks for.
fn refuse_attach(
    channel: u16,
    theirs: &weida_amqp_codec::performative::Attach<'_>,
) -> Result<Vec<u8>> {
    let mut answer = weida_amqp_codec::performative::Attach::new(
        theirs.name,
        theirs.handle,
        theirs.role.opposite(),
    );
    // Both termini null: nothing was created.
    answer.source = None;
    answer.target = None;
    if answer.role == weida_amqp_codec::Role::Sender {
        answer.initial_delivery_count = Some(0);
    }
    let mut bytes = Vec::new();
    frame::write(&mut bytes, FrameKind::Amqp, channel, u32::MAX, |body| {
        Performative::Attach(answer).encode(body)
    })?;
    Ok(bytes)
}

/// Writes `detach` on one session's outgoing channel.
async fn write_detach(
    writer: &mut FrameWriter,
    channel: u16,
    handle: u32,
    closed: bool,
    error: Option<&Condition>,
) -> Result<()> {
    let mut out = Vec::new();
    let codec = error.map(Condition::as_codec);
    frame::write(
        &mut out,
        FrameKind::Amqp,
        channel,
        writer.max_frame_size(),
        |body| {
            Performative::Detach(weida_amqp_codec::performative::Detach {
                handle,
                closed,
                error: codec,
            })
            .encode(body)
        },
    )?;
    writer.send(&out).await
}

/// Writes `flow` on one session's outgoing channel.
///
/// The driver writes this one rather than the link handle, because the two
/// cases that produce it are answers the *driver* owes: an `echo` asking for
/// our state, and the report a drained sender MUST send. Neither waits for an
/// application to notice.
async fn write_flow(
    writer: &mut FrameWriter,
    channel: u16,
    flow: weida_amqp_codec::performative::Flow<'_>,
) -> Result<()> {
    let mut out = Vec::new();
    frame::write(
        &mut out,
        FrameKind::Amqp,
        channel,
        writer.max_frame_size(),
        |body| Performative::Flow(flow).encode(body),
    )?;
    writer.send(&out).await
}

/// Writes `disposition` on one session's outgoing channel.
///
/// The driver writes this one because the case that produces it is not the
/// application's: under `rcv-settle-mode=second` the sender MUST settle once
/// the receiver has published its outcome, and nothing is left to decide.
async fn write_disposition(
    writer: &mut FrameWriter,
    channel: u16,
    role: Role,
    delivery_id: u32,
    outcome: Option<&Outcome>,
    settled: bool,
) -> Result<()> {
    let state = outcome.map(Outcome::to_state);
    let mut disposition = weida_amqp_codec::performative::Disposition::new(role, delivery_id);
    disposition.settled = settled;
    disposition.state = state
        .as_ref()
        .map(weida_amqp_codec::state::DeliveryState::to_value);
    let mut out = Vec::new();
    frame::write(
        &mut out,
        FrameKind::Amqp,
        channel,
        writer.max_frame_size(),
        |body| Performative::Disposition(disposition).encode(body),
    )?;
    writer.send(&out).await
}

/// Writes `end` on one session's outgoing channel.
async fn write_end(
    writer: &mut FrameWriter,
    channel: u16,
    error: Option<&Condition>,
) -> Result<()> {
    let mut out = Vec::new();
    let codec = error.map(Condition::as_codec);
    frame::write(
        &mut out,
        FrameKind::Amqp,
        channel,
        writer.max_frame_size(),
        |body| Performative::End(End { error: codec }).encode(body),
    )?;
    writer.send(&out).await
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
