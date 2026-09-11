//! The zguide's **Paranoid Pirate**, chapter 4: `ppqueue.c` and
//! `ppworker.c`.
//!
//! ```text
//! cargo run -p weida-zmq --example paranoid_pirate
//! ```
//!
//! The guide's problem: "Simple Pirate survives neither a queue restart nor
//! an idle dead worker." The mechanism is heartbeats both ways, a worker that
//! switches REQ to DEALER "so it can send and receive at any time and manages
//! envelopes explicitly", and a queue that keeps a per-worker expiry instead
//! of one `heartbeat_at`. The claim the test asserts is the one that costs
//! the guide five hours of debugging: **the queue evicts lost workers
//! instead of discovering them through a failed request.**
//!
//! `ppqueue.c`'s worker bookkeeping, which is what this file is about:
//!
//! ```c
//! #define HEARTBEAT_LIVENESS  3       //  3-5 is reasonable
//! #define HEARTBEAT_INTERVAL  1000    //  msecs
//!
//! static void s_worker_ready (worker_t *self, zlist_t *workers) { ... }
//!
//! //  Send heartbeats to idle workers if it's time
//! if (zclock_time () >= heartbeat_at) {
//!     for (worker = zlist_first (workers); worker; worker = zlist_next (workers)) {
//!         zframe_t *frame = zframe_dup (worker->identity);
//!         zmsg_t *msg = zmsg_new ();
//!         zmsg_add (msg, frame);
//!         zmsg_addstr (msg, PPP_HEARTBEAT);
//!         zmsg_send (&msg, backend);
//!     }
//!     heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
//! }
//! s_workers_purge (workers);
//! ```
//!
//! and `s_workers_purge`, the eviction the claim is about: a worker whose
//! `expiry` has passed is dropped from the list without anybody having sent
//! it work.
//!
//! # Which surface, and why
//!
//! **Async, on both sides.** The queue's C original is a `zmq_poll` with a
//! timeout it must never let be infinite, precisely so that the heartbeat
//! timer and the purge run when no message arrives; `tokio::select!` over
//! two sockets and a sleep is that loop with the timeout arithmetic removed.
//! The worker is a DEALER for the guide's own reason — "so it can send and
//! receive at any time" — which is a socket that cannot be driven by a
//! blocking call in one direction.
//!
//! # The three differences from the C
//!
//! * **The intervals are arguments.** `HEARTBEAT_LIVENESS` and
//!   `HEARTBEAT_INTERVAL` below are the guide's values and what `main` uses;
//!   a test passes shorter ones so that eviction happens in milliseconds.
//! * **Eviction is counted, not printed.** The C logs "W: heartbeat failure,
//!   can't reach worker"; here [`PpQueue::evicted`] and
//!   [`PpQueue::live_workers`] are atomics, because the claim is about what
//!   the queue *did* when nobody asked it for anything.
//! * **The worker's exponential backoff is left out**, with the reason: the
//!   guide's worker "sleeps the reconnect interval, doubles it up to 32
//!   seconds, destroys and recreates its socket". This library reconnects a
//!   dialled endpoint by itself with `ZMQ_RECONNECT_IVL` backoff, so the
//!   socket churn the C needs is the engine's job here; what stays in the
//!   worker is the liveness counting, which is the part the recipe is about.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use weida_zmq::{Context, ContextConfig, DealerSocket, Message, Multipart, Result, RouterSocket};

/// `#define HEARTBEAT_LIVENESS 3 // 3-5 is reasonable`
pub const HEARTBEAT_LIVENESS: u32 = 3;
/// `#define HEARTBEAT_INTERVAL 1000 // msecs`
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(1000);
/// `#define PPP_READY "\001"`
pub const PPP_READY: &[u8] = b"\x01";
/// `#define PPP_HEARTBEAT "\002"`
pub const PPP_HEARTBEAT: &[u8] = b"\x02";

/// A running Paranoid Pirate queue.
pub struct PpQueue {
    /// Where clients connect.
    pub frontend: String,
    /// Where workers connect.
    pub backend: String,
    /// Workers on the list right now.
    pub live_workers: Arc<AtomicUsize>,
    /// Workers dropped because their expiry passed — the claim, counted.
    pub evicted: Arc<AtomicUsize>,
    _running: tokio::task::JoinHandle<()>,
}

/// `ppqueue.c`: the load-balancing broker with per-worker expiry and
/// heartbeats both ways.
///
/// # Errors
///
/// What binding a socket reports.
pub async fn paranoid_pirate_queue(
    context: &Context,
    interval: Duration,
    liveness: u32,
) -> Result<PpQueue> {
    let frontend = RouterSocket::new(context)?;
    let backend = RouterSocket::new(context)?;
    let frontend_endpoint = frontend.bind("tcp://127.0.0.1:0").await?.to_string();
    let backend_endpoint = backend.bind("tcp://127.0.0.1:0").await?.to_string();
    let live_workers = Arc::new(AtomicUsize::new(0));
    let evicted = Arc::new(AtomicUsize::new(0));

    let running = tokio::spawn({
        let live_workers = Arc::clone(&live_workers);
        let evicted = Arc::clone(&evicted);
        let mut frontend = frontend;
        let mut backend = backend;
        async move {
            //  The queue of available workers, each with its own expiry —
            //  "The queue keeps a per-worker expiry instead of one
            //  heartbeat_at".
            let mut workers: Vec<Message> = Vec::new();
            let mut expiry: HashMap<Vec<u8>, Instant> = HashMap::new();
            let lifetime = interval * liveness;

            loop {
                tokio::select! {
                    arrived = backend.recv() => {
                        let Ok(message) = arrived else { return };
                        let mut frames = message.into_frames();
                        let identity = frames.remove(0);
                        let key = identity.as_slice().to_vec();
                        //  "any message resets liveness to three": a
                        //  heartbeat and a reply are the same evidence.
                        expiry.insert(key.clone(), Instant::now() + lifetime);
                        match frames.first().map(|frame| frame.as_slice()) {
                            Some(PPP_READY) | Some(PPP_HEARTBEAT) => {
                                if !workers.iter().any(|w| w.as_slice() == key.as_slice()) {
                                    workers.push(identity);
                                }
                            }
                            _ => {
                                //  A reply: the worker is free again, and the
                                //  client's envelope goes out as it is.
                                if !workers.iter().any(|w| w.as_slice() == key.as_slice()) {
                                    workers.push(identity);
                                }
                                let Ok(reply) = Multipart::new(frames) else { continue };
                                if frontend.send(reply).await.is_err() {
                                    return;
                                }
                            }
                        }
                        live_workers.store(workers.len(), Ordering::Relaxed);
                    }
                    //  Poll the frontend only when a worker is free.
                    arrived = frontend.recv(), if !workers.is_empty() => {
                        let Ok(message) = arrived else { return };
                        let worker = workers.remove(0);
                        live_workers.store(workers.len(), Ordering::Relaxed);
                        //  A DEALER worker gets no delimiter of its own: the
                        //  client's envelope is what it sees.
                        let mut frames = vec![worker];
                        frames.extend(message.into_frames());
                        let Ok(routed) = Multipart::new(frames) else { continue };
                        if backend.send(routed).await.is_err() {
                            return;
                        }
                    }
                    //  Send heartbeats to idle workers if it's time, then
                    //  purge. The timeout is never infinite, which is what
                    //  makes eviction happen without a client.
                    () = tokio::time::sleep(interval) => {
                        for worker in &workers {
                            let beat = Multipart::new(vec![
                                worker.clone(),
                                Message::from(PPP_HEARTBEAT.to_vec()),
                            ])
                            .expect("two frames");
                            let _ = backend.send(beat).await;
                        }
                        //  s_workers_purge
                        let now = Instant::now();
                        let before = workers.len();
                        workers.retain(|worker| {
                            expiry
                                .get(worker.as_slice())
                                .is_some_and(|deadline| *deadline > now)
                        });
                        let gone = before - workers.len();
                        if gone > 0 {
                            println!("W: heartbeat failure, can't reach {gone} worker(s)");
                            evicted.fetch_add(gone, Ordering::Relaxed);
                        }
                        live_workers.store(workers.len(), Ordering::Relaxed);
                    }
                }
            }
        }
    });

    Ok(PpQueue {
        frontend: frontend_endpoint,
        backend: backend_endpoint,
        live_workers,
        evicted,
        _running: running,
    })
}

/// `ppworker.c`: a DEALER that signals `READY`, beats, and counts the
/// queue's beats against its liveness.
///
/// Returns the task, which is also how a test kills the worker: dropping the
/// handle's abort stops it beating without a word, which is the failure the
/// queue has to notice.
///
/// # Errors
///
/// What constructing or connecting a socket reports.
pub fn paranoid_pirate_worker(
    context: &Context,
    backend: &str,
    interval: Duration,
    liveness: u32,
) -> Result<tokio::task::JoinHandle<()>> {
    let mut worker = DealerSocket::new(context)?;
    worker.connect(backend)?;
    Ok(tokio::spawn(async move {
        //  Tell the queue we're ready for work
        if worker
            .send(Multipart::single(PPP_READY.to_vec()))
            .await
            .is_err()
        {
            return;
        }
        let mut alive = liveness;
        loop {
            tokio::select! {
                arrived = worker.recv() => {
                    let Ok(message) = arrived else { return };
                    //  "any message resets liveness to three"
                    alive = liveness;
                    let frames = message.into_frames();
                    match frames.first().map(|frame| frame.as_slice()) {
                        Some(PPP_HEARTBEAT) => {}
                        _ => {
                            //  A request: [client id][empty][body]. Echo it,
                            //  envelope and all.
                            let Ok(reply) = Multipart::new(frames) else { continue };
                            if worker.send(reply).await.is_err() {
                                return;
                            }
                        }
                    }
                }
                () = tokio::time::sleep(interval) => {
                    //  Silence decrements liveness; at zero the queue is
                    //  gone. This library reconnects the endpoint by itself,
                    //  so the worker keeps beating rather than rebuilding its
                    //  socket.
                    alive = alive.saturating_sub(1);
                    if alive == 0 {
                        println!("W: heartbeat failure, can't reach queue");
                        alive = liveness;
                    }
                    if worker
                        .send(Multipart::single(PPP_HEARTBEAT.to_vec()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    }))
}

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new(ContextConfig::default())?;
    let queue = paranoid_pirate_queue(&context, HEARTBEAT_INTERVAL, HEARTBEAT_LIVENESS).await?;

    let worker = paranoid_pirate_worker(
        &context,
        &queue.backend,
        HEARTBEAT_INTERVAL,
        HEARTBEAT_LIVENESS,
    )?;
    tokio::time::sleep(HEARTBEAT_INTERVAL).await;
    println!(
        "workers on the list: {}",
        queue.live_workers.load(Ordering::Relaxed)
    );

    //  The worker vanishes without a word, and nobody sends it any work.
    worker.abort();
    tokio::time::sleep(HEARTBEAT_INTERVAL * (HEARTBEAT_LIVENESS + 1)).await;
    println!(
        "after the silence: {} on the list, {} evicted",
        queue.live_workers.load(Ordering::Relaxed),
        queue.evicted.load(Ordering::Relaxed)
    );
    Ok(())
}
