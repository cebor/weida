//! The guide's chapter 1: **one transfer, and what you may say about it.**
//!
//! ```text
//! cargo run -p weida --example guide_one_transfer
//! ```
//!
//! Four programs, one per claim `docs/GUIDE.md` §1 makes. Each returns its
//! outcome as a **value** rather than printing it, because a claim a reader
//! is asked to believe should be a claim a test can read — the shape the
//! zguide recipes in `crates/zmq/weida-zmq/examples` already use, and
//! `crates/weida/tests/guide.rs` is the test that reads these.
//!
//! Everything runs in one process on loopback. That is not a simplification
//! of the protocol: the bytes on the wire are the same ones two machines
//! exchange, and §1.4 of the guide says what loopback does hide.

use std::net::SocketAddr;
use std::time::Duration;

use weida::{CursorLevel, Error, Identity, Runtime, RuntimeConfig, TransferMeta, Trust};

/// One `weida://` address and the peer it names, which together are
/// everything a dialling side needs.
pub struct Bound {
    /// `weida://<fingerprint>@127.0.0.1:<port>/<path>`.
    pub url: String,
    /// Kept alive: dropping a runtime closes its connections.
    pub runtime: Runtime,
    /// Kept alive: dropping the listener releases the binding.
    pub listener: weida::Listener,
    _binding: weida::Binding,
}

/// Binds `path` on an ephemeral loopback port under a fresh identity.
///
/// The fingerprint goes **into the URL**. A weida peer is its public key,
/// so an address that names the key is a complete trust statement and there
/// is no certificate authority anywhere in this file.
///
/// # Errors
///
/// Whatever generating an identity or binding a socket reports.
pub async fn bind(path: &str) -> Result<Bound, Error> {
    let identity = Identity::generate()?;
    let fingerprint = identity.fingerprint()?;
    let runtime = Runtime::new(RuntimeConfig::default())?;
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0"
                .parse::<SocketAddr>()
                .expect("a loopback literal"),
            identity,
        )
        .await?;
    let url = format!(
        "weida://{fingerprint}@127.0.0.1:{}{path}",
        binding.local_addr().port()
    );
    Ok(Bound {
        url,
        runtime,
        listener,
        _binding: binding,
    })
}

// --- §1.1 Hello ------------------------------------------------------------

/// What one exchange produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answered {
    /// The reply body.
    pub reply: Vec<u8>,
    /// The fingerprint the replier proved during the handshake, as the
    /// requester's own address named it.
    pub peer_was_named: bool,
}

/// Claim §1.1: **an address and a trust decision are the whole setup.** One
/// exchange, both ends in this process, no configuration file, no broker, no
/// registry.
///
/// # Errors
///
/// Whatever binding, dialling or the exchange itself reports.
pub async fn hello() -> Result<Answered, Error> {
    let bound = bind("/hello").await?;
    let replier = bound.listener.replier("/hello")?;

    // The replying side. `accept` hands over a request whose payload is a
    // stream that has not been read yet — the reply half exists before the
    // request has finished arriving, which is what lets a replier answer a
    // 4 GiB request without holding it.
    let serving = tokio::spawn(async move {
        let mut request = replier.accept().await?;
        let body = request.body().read_capped(1024).await?;
        let mut reply = request.reply(TransferMeta::default()).await?;
        reply
            .write_all(format!("hello, {}", String::from_utf8_lossy(&body)).as_bytes())
            .await?;
        reply.finish()?;
        Ok::<(), Error>(())
    });

    // The dialling side. `Trust::by_address` means: accept exactly the peer
    // this URL names and nobody else.
    let client = Runtime::new(RuntimeConfig::default())?;
    let requester = client.requester(Trust::by_address());
    requester.connect(&bound.url).await?;
    let reply = requester.request(b"world").await?.collect(1024).await?;

    serving.await.map_err(|e| Error::Runtime(e.to_string()))??;
    client.shutdown().await;
    Ok(Answered {
        reply,
        peer_was_named: bound.url.contains("sha256:"),
    })
}

// --- §1.2 The same four calls carry a gigabyte ------------------------------

/// Payload size for the streaming claim. Large enough that no test would
/// `collect` it, small enough to run in a suite: the guide points at
/// `examples/large_stream.rs` and `tests/large.rs` for the gigabyte.
pub const STREAMED: usize = 64 * 1024 * 1024;
/// What the reader is willing to hold at once. The only buffer in the path.
pub const CHUNK: usize = 64 * 1024;

/// What a streamed transfer produced, without either side holding it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Streamed {
    /// Bytes the reader saw.
    pub bytes: u64,
    /// A checksum folded as the bytes went past, never over a whole buffer.
    pub checksum: u64,
    /// What `collect` answered when asked for the same payload under a cap a
    /// careful application would set.
    pub collect_refused: bool,
    /// What the **sender** saw when the reader refused: a definite refusal,
    /// mid-payload, rather than a write that succeeded into nothing.
    pub sender_was_told: Option<String>,
}

/// Claim §1.2: **the payload never has to exist anywhere, and a cap is a
/// refusal rather than a bigger buffer.** The same
/// `open`/`write_all`/`finish` that sent five bytes in §1.1 sends 64 MiB
/// here and the reader folds it through one `CHUNK` buffer. The second
/// transfer is the same payload offered to `collect` under a 1 MiB cap: the
/// reader gets `LimitExceeded`, and the **sender** is told — the refusal
/// reaches it as `Error::Rejected` while it is still writing.
///
/// # Errors
///
/// Whatever binding, dialling or the first transfer reports. The refusal of
/// the second one is a value here, because it is half the claim.
pub async fn gigabyte() -> Result<Streamed, Error> {
    use tokio::io::AsyncReadExt;

    let bound = bind("/bulk").await?;
    let puller = bound.listener.puller("/bulk")?;

    let reading = tokio::spawn(async move {
        // The first transfer is read as a stream: one `CHUNK` buffer, reused,
        // and a fold. Peak memory is the buffer, whatever the payload is.
        let mut transfer = puller.recv().await?;
        let mut buffer = vec![0u8; CHUNK];
        let (mut bytes, mut checksum) = (0u64, 0xcbf2_9ce4_8422_2325u64);
        loop {
            let read = transfer.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            bytes += read as u64;
            for byte in &buffer[..read] {
                checksum ^= u64::from(*byte);
                checksum = checksum.wrapping_mul(0x100_0000_01b3);
            }
        }

        // The second transfer is the same payload under a cap. Dropping the
        // refused handle is what resets the stream, and that reset is what
        // the sender reads as a refusal.
        let second = puller.recv().await?;
        let refused = matches!(second.collect(1024 * 1024).await, Err(Error::LimitExceeded));
        Ok::<(u64, u64, bool), Error>((bytes, checksum, refused))
    });

    let client = Runtime::new(RuntimeConfig::default())?;
    let pusher = client.pusher(Trust::by_address());
    pusher.connect(&bound.url).await?;

    let block = vec![0x5au8; CHUNK];
    let mut transfer = pusher.open(TransferMeta::default()).await?;
    for _ in 0..STREAMED / CHUNK {
        transfer.write_all(&block).await?;
    }
    transfer.finish()?;

    // The same payload again, for a reader that will refuse it.
    let mut capped = pusher.open(TransferMeta::default()).await?;
    let mut sender_was_told = None;
    for _ in 0..STREAMED / CHUNK {
        if let Err(e) = capped.write_all(&block).await {
            sender_was_told = Some(e.to_string());
            break;
        }
    }
    if sender_was_told.is_none() {
        // It all fit in the windows before the reader refused; then the
        // refusal arrives on the receipt instead, which is the same fact at
        // a different moment.
        if let Err(e) = capped.finish()?.delivered().await {
            sender_was_told = Some(e.to_string());
        }
    }

    let (bytes, checksum, collect_refused) =
        reading.await.map_err(|e| Error::Runtime(e.to_string()))??;
    client.shutdown().await;
    Ok(Streamed {
        bytes,
        checksum,
        collect_refused,
        sender_was_told,
    })
}

// --- §1.3 The outcome is a value -------------------------------------------

/// The three answers a sender can get, as one enum a `match` can be
/// exhaustive over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The peer's **transport** holds every byte and the FIN. Not a claim
    /// that its application read them.
    Delivered,
    /// It definitely did not happen. `Error::is_definite_failure()` is true,
    /// so a retry is safe with respect to duplication.
    Refused(String),
    /// It may or may not have happened. Retrying is the application's
    /// decision and nobody else's.
    Indeterminate,
}

/// Classifies one receipt into the guide's three answers.
pub fn classify(result: Result<(), Error>) -> Outcome {
    match result {
        Ok(()) => Outcome::Delivered,
        Err(Error::Indeterminate) => Outcome::Indeterminate,
        Err(e) if e.is_definite_failure() => Outcome::Refused(e.to_string()),
        // Everything else is a failure whose definiteness the error itself
        // decides; there is no fourth kind of answer in the model.
        Err(e) => Outcome::Refused(e.to_string()),
    }
}

/// The three senders of §1.3, and what each one was told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcomes {
    /// A one-way transfer to a path somebody serves.
    pub push_served: Outcome,
    /// A one-way transfer to a path **nobody** serves. This one is a race
    /// by design, and the guide says why.
    pub push_unserved: Outcome,
    /// The same misroute as an exchange. This one is definite, always.
    pub request_unserved: Outcome,
}

/// Claim §1.3: **a send's outcome is a value with three cases — and which
/// case you can be sure of depends on the pattern, not on the error type.**
///
/// Three senders on one address. The push to a served path is delivered.
/// The push to a path nobody serves is the interesting one: the payload fits
/// in the peer's stream window, so the FIN is acknowledged before the
/// dispatcher's refusal travels back, and the receipt can legitimately say
/// `Ok`. That is
/// [decisions/0005](../../../docs/decisions/0005-refusal-race.md) — a
/// refusal is guaranteed only beyond the window or in Req/Rep — and the
/// third sender is the remedy: an exchange has a reply half, so
/// `UnknownEndpoint` is definite every time.
///
/// # Errors
///
/// Whatever binding or dialling reports. The outcomes themselves are values.
pub async fn outcome() -> Result<Outcomes, Error> {
    let bound = bind("/served").await?;
    let puller = bound.listener.puller("/served")?;
    let drain = tokio::spawn(async move {
        while let Ok(transfer) = puller.recv().await {
            let _ = transfer.collect(1024).await;
        }
    });

    let client = Runtime::new(RuntimeConfig::default())?;

    let served = client.pusher(Trust::by_address());
    served.connect(&bound.url).await?;
    let mut transfer = served.open(TransferMeta::default()).await?;
    transfer.write_all(b"for a path somebody serves").await?;
    let push_served = classify(transfer.finish()?.delivered().await);

    let missing_url = bound.url.replace("/served", "/nobody-serves-this");
    let missing = client.pusher(Trust::by_address());
    missing.connect(&missing_url).await?;
    let mut transfer = missing.open(TransferMeta::default()).await?;
    let write = transfer.write_all(b"for a path nobody serves").await;
    let push_unserved = match write {
        Err(e) => classify(Err(e)),
        Ok(()) => classify(transfer.finish()?.delivered().await),
    };

    // The same misroute, asked as a question. A reply half cannot be
    // acknowledged into existence, so the answer is the refusal itself.
    let asking = client.requester(Trust::by_address());
    asking.connect(&missing_url).await?;
    let request_unserved = match asking.request(b"is anybody there").await {
        Ok(_) => Outcome::Delivered,
        Err(e) => classify(Err(e)),
    };

    drain.abort();
    client.shutdown().await;
    Ok(Outcomes {
        push_served,
        push_unserved,
        request_unserved,
    })
}

// --- §1.4 How far did it get ------------------------------------------------

/// What the sender learned about a transfer after it had finished sending it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HowFar {
    /// The transport receipt: the peer's stack has the bytes.
    pub delivered: bool,
    /// The offset the **receiving application** reported for the level the
    /// sender ordered — a number, not a verdict.
    pub reported: Option<u64>,
    /// Payload length, for comparison with the offset above.
    pub sent: u64,
}

/// The application stage this chapter reports on. `16` is the first value an
/// application may name; weida carries it and never interprets it
/// (`docs/PROTOCOL.md` §6.7).
pub fn stage() -> CursorLevel {
    CursorLevel::application(CursorLevel::APPLICATION_FLOOR).expect("16 is at the floor")
}

/// Claim §1.4: **"did it arrive" and "how far did the far end get" are two
/// different questions, and weida answers both separately.** A one-way
/// transfer orders a report; it stays one unidirectional stream; the receiver
/// reports an absolute offset as it consumes; the sender reads it after its
/// own FIN.
///
/// # Errors
///
/// Whatever binding, dialling or the transfer reports.
pub async fn how_far() -> Result<HowFar, Error> {
    let bound = bind("/work").await?;
    let puller = bound.listener.puller("/work")?;

    let working = tokio::spawn(async move {
        let transfer = puller.recv().await?;
        // The reporter exists only because the sender ordered a report. A
        // receiver that reports nothing fails no transfer.
        let mut reporter = transfer.reporter().expect("the sender ordered a report");
        let body = transfer.collect(64 * 1024).await?;
        // The payload is now processed as far as this application is
        // concerned, and that is what the offset says: bytes, absolute.
        reporter.report(stage(), body.len() as u64).await?;
        reporter.finish().await?;
        Ok::<usize, Error>(body.len())
    });

    let client = Runtime::new(RuntimeConfig::default())?;
    let pusher = client.pusher(Trust::by_address());
    pusher.connect(&bound.url).await?;

    let payload = b"eight billion of these".to_vec();
    let meta = TransferMeta::default().with_report([stage()]);
    let mut transfer = pusher.open(meta).await?;
    let mut cursors = transfer.cursors().expect("a report was ordered");
    transfer.write_all(&payload).await?;
    let delivered = transfer.finish()?.delivered().await.is_ok();

    // The receipt above is already in; this is the other fact, and it
    // arrives after the FIN on a stream of its own.
    let reported = cursors.changed().await.and_then(|set| set.offset(stage()));

    working.await.map_err(|e| Error::Runtime(e.to_string()))??;
    client.shutdown().await;
    Ok(HowFar {
        delivered,
        reported,
        sent: payload.len() as u64,
    })
}

// --- §1.5 What the receipt costs -------------------------------------------

/// How long the two send shapes took, over the same connection.
#[derive(Clone, Copy, Debug)]
pub struct ReceiptCost {
    /// Fire and forget: `send` returns when the FIN is queued.
    pub without: Duration,
    /// The same transfer with `delivered()` awaited.
    pub with: Duration,
    /// How many of each were sent.
    pub rounds: usize,
}

/// Claim §1.5: **the receipt is a round trip and it is not free.** The same
/// payload, the same connection, once without the receipt and once with it.
///
/// The numbers are the machine's, not the protocol's, and they are printed
/// rather than asserted: on an idle loopback connection a receipt waits for
/// the peer's delayed acknowledgement — `GUARANTEES.md` §3 measured ~26 ms
/// against ~7.9 µs for the same 1 KiB push without it. What a test can
/// honestly assert is that both shapes deliver, which is what
/// `tests/guide.rs` does.
///
/// # Errors
///
/// Whatever binding, dialling or the transfers report.
pub async fn receipt_cost(rounds: usize) -> Result<ReceiptCost, Error> {
    let bound = bind("/cost").await?;
    let puller = bound.listener.puller("/cost")?;
    let draining = tokio::spawn(async move {
        while let Ok(transfer) = puller.recv().await {
            let _ = transfer.collect(64 * 1024).await;
        }
    });

    let client = Runtime::new(RuntimeConfig::default())?;
    let pusher = client.pusher(Trust::by_address());
    pusher.connect(&bound.url).await?;
    let payload = vec![0x5au8; 1024];

    let started = std::time::Instant::now();
    for _ in 0..rounds {
        pusher.send(&payload).await?;
    }
    let without = started.elapsed() / rounds as u32;

    let started = std::time::Instant::now();
    for _ in 0..rounds {
        let mut transfer = pusher.open(TransferMeta::default()).await?;
        transfer.write_all(&payload).await?;
        transfer.finish()?.delivered().await?;
    }
    let with = started.elapsed() / rounds as u32;

    draining.abort();
    client.shutdown().await;
    Ok(ReceiptCost {
        without,
        with,
        rounds,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("§1.1 hello");
    let answered = hello().await?;
    println!(
        "     reply {:?}, the address named the peer: {}",
        String::from_utf8_lossy(&answered.reply),
        answered.peer_was_named
    );

    println!("§1.2 the same four calls carry {} MiB", STREAMED >> 20);
    let streamed = gigabyte().await?;
    println!(
        "     {} bytes folded through a {} KiB buffer, checksum {:#x}",
        streamed.bytes,
        CHUNK >> 10,
        streamed.checksum
    );
    println!(
        "     the same payload under a 1 MiB cap: reader refused it ({}), sender was told {:?}",
        streamed.collect_refused, streamed.sender_was_told
    );

    println!("§1.3 the outcome is a value");
    let outcomes = outcome().await?;
    println!("     push, served path:   {:?}", outcomes.push_served);
    println!(
        "     push, unserved path: {:?}  <- a race by design, see 0005",
        outcomes.push_unserved
    );
    println!(
        "     request, same path:  {:?}  <- definite, every time",
        outcomes.request_unserved
    );

    println!("§1.4 how far did it get");
    let far = how_far().await?;
    println!(
        "     transport receipt: {}, application reported {:?} of {} bytes",
        far.delivered, far.reported, far.sent
    );

    println!("§1.5 what the receipt costs");
    let cost = receipt_cost(8).await?;
    println!(
        "     1 KiB, {} rounds each: {:?} without the receipt, {:?} with it",
        cost.rounds, cost.without, cost.with
    );
    Ok(())
}
