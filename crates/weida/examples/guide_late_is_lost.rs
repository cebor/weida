//! The guide's §2.6: **late is lost.**
//!
//! ```text
//! cargo run -p weida --example guide_late_is_lost
//! ```
//!
//! Three programs, one per claim `docs/GUIDE.md` §2.6 and §2.7 make, in the shape of
//! the chapters before it: each returns its outcome as a **value**, so
//! `crates/weida/tests/guide.rs` asserts the code a reader runs.
//!
//! * [`supersession`]: a radio sends ten segments to two dishes. One reads
//!   every segment; the other never reads. The fast dish gets all ten, whole;
//!   the stalled one loses the old segment each time a new one opens — never
//!   the new one, and never at the fast dish's expense.
//! * [`sfu`]: a selective forwarding unit and a relay, each a loop over
//!   opaque payload. Microphones are datagram flows into the SFU, which
//!   republishes every frame as a datagram segment on its radio; a relay is a
//!   dish upstream and a radio downstream that copies stream segments chunk
//!   by chunk.
//! * [`layered`]: quality without re-encoding. A sharer sends three layers
//!   per segment upstream with `Peer::segment`; a relay maps each upstream
//!   layer onto a radio segment that follows upstream; a viewer capped at
//!   layer 0 receives the base layer of every segment and nothing else, and
//!   an uncapped viewer receives all three.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use weida::{
    DEFAULT_DATAGRAM_RECEIVE_BYTES, Dish, Error, FlowMeta, Identity, Incoming, IncomingTransfer,
    JoinTerms, Limits, Radio, Received, Runtime, RuntimeConfig, Segment, SegmentTerms, Trust,
};

/// A runtime, its listener and the port it serves on, kept alive together.
pub struct Served {
    /// Kept alive: dropping a runtime closes its connections.
    pub runtime: Runtime,
    /// The listener patterns register on.
    pub listener: weida::Listener,
    fingerprint: weida::Fingerprint,
    binding: weida::Binding,
}

impl Served {
    /// `weida://<fingerprint>@127.0.0.1:<port><path>`.
    pub fn url(&self, path: &str) -> String {
        format!(
            "weida://{}@127.0.0.1:{}{path}",
            self.fingerprint,
            self.binding.local_addr().port()
        )
    }
}

/// Serves on an ephemeral loopback port under a fresh identity.
///
/// # Errors
///
/// Whatever generating an identity or binding a socket reports.
pub async fn serve(limits: Limits) -> Result<Served, Error> {
    let identity = Identity::generate()?;
    let fingerprint = identity.fingerprint()?;
    let runtime = Runtime::new(RuntimeConfig {
        limits,
        ..RuntimeConfig::default()
    })?;
    let listener = runtime.listener();
    let binding = listener
        .bind_quic(
            "127.0.0.1:0"
                .parse::<SocketAddr>()
                .expect("a loopback literal"),
            identity,
        )
        .await?;
    Ok(Served {
        runtime,
        listener,
        fingerprint,
        binding,
    })
}

/// Limits with datagram flows on.
pub fn with_datagrams() -> Limits {
    Limits {
        datagram_receive_bytes: DEFAULT_DATAGRAM_RECEIVE_BYTES,
        ..Limits::default()
    }
}

/// A dish on a runtime of its own — one connection per dish, as a deployment
/// has — joined to `filter` under `terms` and connected to `url`.
///
/// # Errors
///
/// Whatever building a runtime, joining or dialling reports.
pub async fn dish(
    url: &str,
    filter: &str,
    limits: Limits,
    terms: JoinTerms,
) -> Result<(Runtime, Dish), Error> {
    let runtime = Runtime::new(RuntimeConfig {
        limits,
        ..RuntimeConfig::default()
    })?;
    let dish = runtime.dish(Trust::by_address());
    dish.join(filter, terms).await?;
    dish.connect(url).await?;
    Ok((runtime, dish))
}

/// Waits until `radio` has seen `n` joins: a join is a SUBSCRIBE, and the
/// radio learns it when that stream arrives.
pub async fn joined(radio: &Radio, n: usize) {
    while radio.dish_count() < n {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Deterministic noise: payload the forwarding code cannot have made up.
struct Noise(u64);

impl Noise {
    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len)
            .map(|_| {
                self.0 ^= self.0 << 13;
                self.0 ^= self.0 >> 7;
                self.0 ^= self.0 << 17;
                self.0 as u8
            })
            .collect()
    }
}

// --- §2.6 Supersession --------------------------------------------------------

/// What [`supersession`] observed.
pub struct Supersession {
    /// The segment numbers the reading dish received whole, in order.
    pub fast_received: Vec<u64>,
    /// Copies the radio reset at the stalled dish because a newer segment
    /// opened.
    pub stalled_superseded: u64,
}

/// Ten segments of 256 KiB to two dishes, one reading and one stalled.
///
/// # Errors
///
/// Whatever binding, dialling or reading reports.
pub async fn supersession() -> Result<Supersession, Error> {
    const SEGMENTS: u64 = 10;
    const CHUNK: usize = 16 * 1024;
    const CHUNKS: usize = 16;

    let server = serve(Limits::default()).await?;
    let radio = server.listener.radio("/live")?;
    let (_fast_rt, fast) = dish(
        &server.url("/live"),
        "frames",
        Limits::default(),
        JoinTerms::default(),
    )
    .await?;
    // The stalled dish never reads, and its window is small enough that a
    // segment it does not read cannot be acknowledged: what it holds is
    // exactly what a stalled viewer holds.
    let stalled_limits = Limits {
        stream_receive_window: 64 * 1024,
        ..Limits::default()
    };
    let (_stalled_rt, _stalled) = dish(
        &server.url("/live"),
        "frames",
        stalled_limits,
        JoinTerms::default(),
    )
    .await?;
    joined(&radio, 2).await;

    let (read_tx, mut read_rx) = mpsc::channel(1);
    let reader = tokio::spawn(async move {
        for _ in 0..SEGMENTS {
            let Ok(Received::Segment(transfer)) = fast.recv().await else {
                break;
            };
            let number = transfer.meta().segment;
            let whole = transfer.collect(CHUNK * CHUNKS).await.map(|b| b.len());
            let _ = read_tx.send((number, whole)).await;
        }
    });
    let mut fast_received = Vec::new();
    // One segment per 40 ms frame, which is a 25 fps camera: the radio does
    // not wait for anybody, it simply has nothing newer to send sooner.
    let mut frame = tokio::time::interval(Duration::from_millis(40));
    for _ in 0..SEGMENTS {
        frame.tick().await;
        let mut segment = radio.segment("frames", SegmentTerms::default())?;
        for _ in 0..CHUNKS {
            segment.write(vec![0x42; CHUNK])?;
        }
        segment.finish();
        // A viewer that keeps up has taken this segment, and its transport
        // has acknowledged it, before the next frame exists — so
        // supersession never reaches it. The stalled viewer's copy is still
        // held back by its flow control when the next frame opens, every
        // time.
        match read_rx.recv().await {
            Some((Some(number), Ok(len))) if len == CHUNK * CHUNKS => fast_received.push(number),
            _ => break,
        }
    }
    let _ = reader.await;
    Ok(Supersession {
        fast_received,
        stalled_superseded: radio.dropped_on("frames").map_or(0, |d| d.superseded),
    })
}

// --- §2.6 An SFU and a relay ----------------------------------------------------

/// What [`sfu`] observed.
pub struct Sfu {
    /// Voice frames that reached **both** listening dishes, of 100 spoken.
    pub forwarded: u64,
    /// Video segments that crossed the relay whole, of 5 sent.
    pub relayed_segments: u64,
    /// Every byte that arrived anywhere is a byte that was sent: nothing on
    /// the way read, changed or reframed the payload.
    pub payload_opaque: bool,
}

/// An SFU with two listeners and one speaker, then a relay between the SFU's
/// radio and one more dish.
///
/// # Errors
///
/// Whatever binding, dialling, sending or reading reports.
pub async fn sfu() -> Result<Sfu, Error> {
    const FRAMES: usize = 100;
    const SEGMENTS: usize = 5;
    const SEGMENT: usize = 64 * 1024;

    // The SFU: flows in on `/mic`, datagram segments out on `/room`. This
    // loop is the whole forwarding path; the payload is ciphertext to it.
    let sfu = serve(with_datagrams()).await?;
    let mics = sfu.listener.acceptor("/mic")?;
    let room = sfu.listener.radio("/room")?;
    let forward = room.clone();
    tokio::spawn(async move {
        while let Ok(Incoming::Flow(mic)) = mics.accept().await {
            let radio = forward.clone();
            tokio::spawn(async move {
                while let Some(frame) = mic.recv().await {
                    let _ = radio.datagram("room.voice", frame);
                }
            });
        }
    });

    // Two listeners and a relay joined upstream.
    let (_a_rt, a) = dish(
        &sfu.url("/room"),
        "room.voice",
        with_datagrams(),
        JoinTerms::default(),
    )
    .await?;
    let (_b_rt, b) = dish(
        &sfu.url("/room"),
        "room.voice",
        with_datagrams(),
        JoinTerms::default(),
    )
    .await?;
    let relay = serve(Limits::default()).await?;
    let downstream = relay.listener.radio("/relay")?;
    let watching = downstream.clone();
    let upstream = relay.runtime.dish(Trust::by_address());
    upstream.join("room.video", JoinTerms::default()).await?;
    upstream.connect(&sfu.url("/room")).await?;
    joined(&room, 3).await;

    // The relay: a dish in, a radio out, each segment copied chunk by chunk
    // as it arrives — never materialized.
    tokio::spawn(async move {
        while let Ok(Received::Segment(mut incoming)) = upstream.recv().await {
            let topic = incoming.meta().topic.clone().unwrap_or_default();
            let Ok(mut outgoing) = downstream.segment(&topic, SegmentTerms::default()) else {
                continue;
            };
            let mut chunk = vec![0u8; 16 * 1024];
            loop {
                match incoming.read(&mut chunk).await {
                    Ok(0) => {
                        outgoing.finish();
                        break;
                    }
                    Ok(n) => {
                        let _ = outgoing.write(chunk[..n].to_vec());
                    }
                    // The upstream copy was reset: dropping the downstream
                    // segment resets it too, so nobody receives a fragment.
                    Err(_) => break,
                }
            }
        }
    });
    let (_viewer_rt, viewer) = dish(
        &relay.url("/relay"),
        "room.video",
        Limits::default(),
        JoinTerms::default(),
    )
    .await?;
    joined(&watching, 1).await;

    // One speaker, 100 frames of noise at 5 ms.
    let speaker_rt = Runtime::new(RuntimeConfig {
        limits: with_datagrams(),
        ..RuntimeConfig::default()
    })?;
    let speaker = speaker_rt.peer(Trust::by_address());
    speaker.connect(&sfu.url("/mic")).await?;
    let mic = speaker.open_flow(FlowMeta::default()).await?;
    let mut noise = Noise(0x9e37_79b9_7f4a_7c15);
    let spoken: Vec<Vec<u8>> = (0..FRAMES).map(|_| noise.bytes(160)).collect();
    let listen = |dish: Dish| {
        tokio::spawn(async move {
            let mut heard = Vec::new();
            while let Ok(Ok(Received::Datagram { payload, .. })) =
                tokio::time::timeout(Duration::from_millis(500), dish.recv()).await
            {
                heard.push(payload.to_vec());
            }
            heard
        })
    };
    let (heard_a, heard_b) = (listen(a), listen(b));
    for frame in &spoken {
        mic.send(frame.clone())?;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let heard_a = heard_a.await.unwrap_or_default();
    let heard_b = heard_b.await.unwrap_or_default();
    let forwarded = heard_a
        .iter()
        .filter(|frame| heard_b.contains(frame))
        .count() as u64;
    let mut payload_opaque = heard_a
        .iter()
        .chain(&heard_b)
        .all(|frame| spoken.contains(frame));

    // Five video segments through the relay, each read whole before the next.
    let mut relayed_segments = 0;
    for _ in 0..SEGMENTS {
        let sent = noise.bytes(SEGMENT);
        let mut segment = room.segment("room.video", SegmentTerms::default())?;
        for chunk in sent.chunks(16 * 1024) {
            segment.write(chunk.to_vec())?;
        }
        segment.finish();
        let received = tokio::time::timeout(Duration::from_secs(5), viewer.recv()).await;
        let Ok(Ok(Received::Segment(transfer))) = received else {
            break;
        };
        let body = transfer.collect(2 * SEGMENT).await?;
        if body == sent {
            relayed_segments += 1;
        } else {
            payload_opaque = false;
        }
    }
    speaker_rt.shutdown().await;
    Ok(Sfu {
        forwarded,
        relayed_segments,
        payload_opaque,
    })
}

/// What [`layered`] observed.
pub struct Layered {
    /// `(segment, layer)` the uncapped viewer received, in arrival order.
    pub uncapped: Vec<(u64, u8)>,
    /// `(segment, layer)` the viewer capped at layer 0 received.
    pub capped: Vec<(u64, u8)>,
    /// Whether the capped viewer received anything after the last segment.
    pub capped_heard_more: bool,
    /// Whether every layer arrived byte for byte as the sharer wrote it.
    pub payload_whole: bool,
}

/// One radio segment a relay is filling from upstream.
struct Relayed {
    segment: Segment,
    /// Layers whose upstream stream ended whole, one bit each.
    eof: u16,
    /// Every layer below this one is finished downstream.
    finished: u8,
    /// Upstream layer streams of this segment still being read.
    reading: u8,
}

/// The relay's segments: the newest number it opened, and every segment
/// still filling — the newest, and older ones whose upstream layers have not
/// ended yet.
#[derive(Default)]
struct RelayState {
    newest: Option<u64>,
    segments: BTreeMap<u64, Relayed>,
}

fn lock(state: &Mutex<RelayState>) -> std::sync::MutexGuard<'_, RelayState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Relays one upstream layer stream onto the radio segment of its number
/// ([decisions/0037](../../../docs/decisions/0037-layered-segments.md)
/// §4.8): a newer number opens a radio segment that follows upstream, each
/// chunk of layer *k* goes to `write_layer(k)`, and a layer is finished only
/// once every lower layer is. An upstream layer that was reset drops the
/// segment, which resets every layer not finished downstream.
async fn relay_layer(
    room: Radio,
    state: Arc<Mutex<RelayState>>,
    topic: String,
    number: u64,
    layer: u8,
    mut upstream: IncomingTransfer,
) {
    {
        let mut state = lock(&state);
        if state.newest.is_none_or(|newest| number > newest) {
            let terms = SegmentTerms::default().with_follows_upstream(true);
            let Ok(segment) = room.segment(&topic, terms) else {
                return;
            };
            state.newest = Some(number);
            // Older segments with nothing left to read are done.
            state.segments.retain(|_, relayed| relayed.reading > 0);
            state.segments.insert(
                number,
                Relayed {
                    segment,
                    eof: 0,
                    finished: 0,
                    reading: 0,
                },
            );
        }
        match state.segments.get_mut(&number) {
            Some(relayed) => relayed.reading += 1,
            None => return,
        }
    }
    let mut chunk = vec![0u8; 16 * 1024];
    let whole = loop {
        match upstream.read(&mut chunk).await {
            Ok(0) => break true,
            Ok(n) => {
                if let Some(relayed) = lock(&state).segments.get_mut(&number) {
                    let _ = relayed.segment.write_layer(layer, chunk[..n].to_vec());
                }
            }
            Err(_) => break false,
        }
    };
    let mut state = lock(&state);
    let newest = state.newest;
    let Some(relayed) = state.segments.get_mut(&number) else {
        return;
    };
    relayed.reading -= 1;
    if !whole {
        state.segments.remove(&number);
        return;
    }
    relayed.eof |= 1 << layer;
    while relayed.finished < 16 && relayed.eof & (1 << relayed.finished) != 0 {
        relayed.segment.finish_layer(relayed.finished);
        relayed.finished += 1;
    }
    if relayed.reading == 0 && newest != Some(number) {
        state.segments.remove(&number);
    }
}

/// The guide's §2.7: quality without re-encoding. A sharer writes three
/// layers per segment upstream; a relay forwards each layer as it arrives;
/// one viewer joins uncapped, one with `max_layer = 0`.
///
/// # Errors
///
/// Whatever binding, dialling, sending or reading reports.
pub async fn layered() -> Result<Layered, Error> {
    const SEGMENTS: u64 = 5;
    const LAYERS: u8 = 3;
    let relay = serve(Limits::default()).await?;
    let room = relay.listener.radio("/room")?;
    let up = relay.listener.acceptor("/up")?;
    let state = Arc::new(Mutex::new(RelayState::default()));
    let relaying = room.clone();
    tokio::spawn(async move {
        while let Ok(incoming) = up.accept().await {
            let Incoming::Stream(upstream) = incoming else {
                continue;
            };
            let meta = upstream.meta();
            let (Some(topic), Some(number), Some(layer)) =
                (meta.topic.clone(), meta.segment, meta.layer)
            else {
                continue;
            };
            tokio::spawn(relay_layer(
                relaying.clone(),
                Arc::clone(&state),
                topic,
                number,
                layer,
                upstream,
            ));
        }
    });

    let (_uncapped_rt, uncapped) = dish(
        &relay.url("/room"),
        "share.video",
        Limits::default(),
        JoinTerms::default(),
    )
    .await?;
    let (_capped_rt, capped) = dish(
        &relay.url("/room"),
        "share.video",
        Limits::default(),
        JoinTerms::default().with_max_layer(0),
    )
    .await?;
    joined(&room, 2).await;

    let sharer_rt = Runtime::new(RuntimeConfig::default())?;
    let sharer = sharer_rt.peer(Trust::by_address());
    sharer.connect(&relay.url("/up")).await?;
    let mut noise = Noise(0x2545_f491_4f6c_dd1d);
    let mut seen = Layered {
        uncapped: Vec::new(),
        capped: Vec::new(),
        capped_heard_more: false,
        payload_whole: true,
    };
    for _ in 0..SEGMENTS {
        let mut segment = sharer
            .segment("share.video", SegmentTerms::default())
            .await?;
        let sent: Vec<Vec<u8>> = (0..LAYERS).map(|_| noise.bytes(8 * 1024)).collect();
        for (layer, body) in (0..LAYERS).zip(&sent) {
            segment.write_layer(layer, body.clone())?;
        }
        segment.finish();
        for (viewer, count, heard) in [
            (&uncapped, LAYERS, &mut seen.uncapped),
            (&capped, 1, &mut seen.capped),
        ] {
            for _ in 0..count {
                let received = tokio::time::timeout(Duration::from_secs(5), viewer.recv()).await;
                let Ok(Ok(Received::Segment(transfer))) = received else {
                    seen.payload_whole = false;
                    break;
                };
                let (Some(number), Some(layer)) = (transfer.meta().segment, transfer.meta().layer)
                else {
                    seen.payload_whole = false;
                    continue;
                };
                let body = transfer.collect(64 * 1024).await?;
                if sent.get(usize::from(layer)) != Some(&body.to_vec()) {
                    seen.payload_whole = false;
                }
                heard.push((number, layer));
            }
        }
    }
    seen.capped_heard_more = tokio::time::timeout(Duration::from_millis(300), capped.recv())
        .await
        .is_ok();
    sharer_rt.shutdown().await;
    Ok(seen)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("§2.6 a dish that falls behind loses the old segment");
    let seen = supersession().await?;
    println!(
        "fast dish received segments {:?}; the stalled dish lost {} to supersession",
        seen.fast_received, seen.stalled_superseded
    );
    println!("§2.6 an SFU and a relay are loops over opaque payload");
    let sfu = sfu().await?;
    println!(
        "{} of 100 frames reached both listeners; {} of 5 segments crossed the relay; payload untouched: {}",
        sfu.forwarded, sfu.relayed_segments, sfu.payload_opaque
    );
    println!("§2.7 a viewer's quality is a cap it states, not a second encoding");
    let layered = layered().await?;
    println!(
        "capped viewer received {} segments of layer 0 and nothing else: {}; uncapped viewer received {} layer segments; payload untouched: {}",
        layered.capped.len(),
        !layered.capped_heard_more,
        layered.uncapped.len(),
        layered.payload_whole
    );
    Ok(())
}
