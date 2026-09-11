//! The zguide's **Majordomo**, chapter 4, as [18/MDP](https://rfc.zeromq.org/spec/18/)
//! version 0.2 specifies it: `mdbroker.c`, `mdworker.c` and `mdclient.c`
//! (`mdcliapi`/`mdwrkapi` inlined, since a Rust reader does not need a C
//! API to hide `zmsg_t` from them).
//!
//! ```text
//! cargo run -p weida-zmq --example majordomo
//! ```
//!
//! MDP's own stated goals, which are the claims the test asserts: "route by
//! abstract service name"; per-service worker registration so the broker has
//! "one request queue and one worker queue per service"; "Allow the broker to
//! implement a 'least recently used' pattern for task distribution to workers
//! for a given service"; both peers detecting disconnection by heartbeating,
//! where "any received command except `DISCONNECT` acts as a heartbeat"; and
//! on `DISCONNECT` the worker "MUST close its socket and reconnect on a new
//! one — This mechanism allows workers to re-register after a broker failure
//! and recovery". MMI rides on top without changing MDP: "names beginning
//! `mmi.` are handled internally, and `mmi.service` answers 200 if workers
//! are registered and 404 otherwise".
//!
//! The six-byte header is the part that makes a reconnect checkable:
//!
//! ```c
//! #define MDPC_CLIENT         "MDPC02"
//! #define MDPW_WORKER         "MDPW02"
//! //  MDP/Worker commands, as strings
//! #define MDPW_READY          "\001"
//! #define MDPW_REQUEST        "\002"
//! #define MDPW_PARTIAL        "\003"
//! #define MDPW_FINAL          "\004"
//! #define MDPW_HEARTBEAT      "\005"
//! #define MDPW_DISCONNECT     "\006"
//! ```
//!
//! and `mdbroker.c`'s dispatch, in one sentence of C: a worker's `READY`
//! appends it to `service->waiting`, a client's `REQUEST` appends the message
//! to `service->requests`, and `s_service_dispatch` marries the two while
//! both lists are non-empty.
//!
//! # Which surface, and why
//!
//! **Async throughout.** Every one of the three programs is a `zmq_poll`
//! loop in C: the broker polls one ROUTER with a heartbeat timeout, the
//! worker polls its DEALER so that heartbeats keep flowing while it waits for
//! work, and even `mdcliapi`'s synchronous-looking `send`/`recv` is a poll
//! with a timeout underneath. `tokio::select!` is that loop, and the worker
//! could not be written with a blocking call in one direction — which is the
//! same reason MDP makes it a DEALER rather than a REQ.
//!
//! # The three differences from the C
//!
//! * **The commands are constants, not a numbered `zmsg` dance.** The C
//!   builds each message frame by frame with CZMQ; here the frames are a
//!   `Vec<Message>` in the same order, and the six-byte header is checked on
//!   the way in rather than assumed.
//! * **The intervals are arguments.** `HEARTBEAT_INTERVAL` (2500 ms) and
//!   `HEARTBEAT_LIVENESS` (3) are the C's values and what `main` uses; a test
//!   passes milliseconds so that an eviction and a re-registration can be
//!   asserted quickly.
//! * **What the broker did is countable.** The C prints `READY`s, evictions
//!   and `DISCONNECT`s; here they are atomics, because "workers re-register
//!   after a broker failure" is a claim about a count and not about a log.
//!
//! What is deliberately **not** here: the guide's own omissions stay
//! omissions. There is no exponential backoff in the worker ("the reference
//! API does no exponential backoff"), and empty service queues are never
//! deleted — both are recorded as failure modes in
//! `docs/research/zeromq.md` §9 rather than quietly fixed, because a recipe
//! that behaves better than its original teaches the wrong thing about the
//! original.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use weida_zmq::{Context, ContextConfig, DealerSocket, Message, Multipart, Result, RouterSocket};

/// `#define MDPC_CLIENT "MDPC02"`: the client sub-protocol's six-byte header.
pub const MDPC_CLIENT: &[u8] = b"MDPC02";
/// `#define MDPW_WORKER "MDPW02"`: the worker sub-protocol's.
pub const MDPW_WORKER: &[u8] = b"MDPW02";

/// `MDPC_REQUEST`.
pub const MDPC_REQUEST: &[u8] = b"\x01";
/// `MDPC_PARTIAL`.
pub const MDPC_PARTIAL: &[u8] = b"\x02";
/// `MDPC_FINAL`.
pub const MDPC_FINAL: &[u8] = b"\x03";

/// `MDPW_READY`.
pub const MDPW_READY: &[u8] = b"\x01";
/// `MDPW_REQUEST`.
pub const MDPW_REQUEST: &[u8] = b"\x02";
/// `MDPW_PARTIAL`.
pub const MDPW_PARTIAL: &[u8] = b"\x03";
/// `MDPW_FINAL`.
pub const MDPW_FINAL: &[u8] = b"\x04";
/// `MDPW_HEARTBEAT`.
pub const MDPW_HEARTBEAT: &[u8] = b"\x05";
/// `MDPW_DISCONNECT`.
pub const MDPW_DISCONNECT: &[u8] = b"\x06";

/// `#define HEARTBEAT_INTERVAL 2500 // msecs`
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(2500);
/// `#define HEARTBEAT_LIVENESS 3 // 3-5 is reasonable`
pub const HEARTBEAT_LIVENESS: u32 = 3;

/// A running broker.
pub struct Broker {
    /// The one endpoint both sub-protocols use — MDP "MAY use one socket for
    /// both".
    pub endpoint: String,
    /// `READY` commands accepted, which counts re-registrations too.
    pub ready_seen: Arc<AtomicUsize>,
    /// `DISCONNECT`s sent to workers whose liveness ran out.
    pub disconnects_sent: Arc<AtomicUsize>,
    _running: tokio::task::JoinHandle<()>,
}

/// One registered worker.
struct Worker {
    identity: Message,
    service: String,
    expiry: Instant,
}

/// `mdbroker.c`: one ROUTER, one worker queue and one request queue per
/// service, heartbeats, `DISCONNECT` and `mmi.service`.
///
/// # Errors
///
/// What binding a socket reports.
pub async fn majordomo_broker(
    context: &Context,
    interval: Duration,
    liveness: u32,
) -> Result<Broker> {
    let socket = RouterSocket::new(context)?;
    let endpoint = socket.bind("tcp://127.0.0.1:0").await?.to_string();
    let ready_seen = Arc::new(AtomicUsize::new(0));
    let disconnects_sent = Arc::new(AtomicUsize::new(0));

    let running = tokio::spawn({
        let ready_seen = Arc::clone(&ready_seen);
        let disconnects_sent = Arc::clone(&disconnects_sent);
        let mut socket = socket;
        async move {
            let lifetime = interval * liveness;
            //  service -> waiting workers, least recently used first
            let mut waiting: HashMap<String, VecDeque<Message>> = HashMap::new();
            //  service -> queued requests: [client identity, body...]
            let mut requests: HashMap<String, VecDeque<Vec<Message>>> = HashMap::new();
            let mut workers: Vec<Worker> = Vec::new();

            loop {
                tokio::select! {
                    arrived = socket.recv() => {
                        let Ok(message) = arrived else { return };
                        let mut frames = message.into_frames();
                        if frames.len() < 3 {
                            continue;
                        }
                        let sender = frames.remove(0);
                        let header = frames.remove(0);
                        let command = frames.remove(0);
                        match header.as_slice() {
                            MDPC_CLIENT => {
                                if command.as_slice() != MDPC_REQUEST || frames.is_empty() {
                                    continue;
                                }
                                let service =
                                    String::from_utf8_lossy(frames.remove(0).as_slice())
                                        .into_owned();
                                //  MMI: "names beginning mmi. are handled
                                //  internally".
                                if let Some(query) = service.strip_prefix("mmi.") {
                                    let asked_about = frames
                                        .first()
                                        .map(|frame| {
                                            String::from_utf8_lossy(frame.as_slice()).into_owned()
                                        })
                                        .unwrap_or_default();
                                    let status = if query == "service"
                                        && workers.iter().any(|w| w.service == asked_about)
                                    {
                                        "200"
                                    } else if query == "service" {
                                        "404"
                                    } else {
                                        "501"
                                    };
                                    let reply = Multipart::new(vec![
                                        sender,
                                        Message::from(MDPC_CLIENT.to_vec()),
                                        Message::from(MDPC_FINAL.to_vec()),
                                        Message::from(service.into_bytes()),
                                        Message::from(status.as_bytes().to_vec()),
                                    ])
                                    .expect("five frames");
                                    let _ = socket.send(reply).await;
                                    continue;
                                }
                                let mut queued = vec![sender];
                                queued.extend(frames);
                                requests.entry(service.clone()).or_default().push_back(queued);
                                dispatch(&mut socket, &service, &mut waiting, &mut requests).await;
                            }
                            MDPW_WORKER => match command.as_slice() {
                                MDPW_READY => {
                                    let Some(service) = frames.first() else { continue };
                                    let service =
                                        String::from_utf8_lossy(service.as_slice()).into_owned();
                                    workers.retain(|w| w.identity != sender);
                                    workers.push(Worker {
                                        identity: sender.clone(),
                                        service: service.clone(),
                                        expiry: Instant::now() + lifetime,
                                    });
                                    waiting.entry(service.clone()).or_default().push_back(sender);
                                    ready_seen.fetch_add(1, Ordering::Relaxed);
                                    dispatch(&mut socket, &service, &mut waiting, &mut requests)
                                        .await;
                                }
                                MDPW_FINAL | MDPW_PARTIAL => {
                                    //  [client identity][empty][body...]
                                    if frames.len() < 2 {
                                        continue;
                                    }
                                    let client = frames.remove(0);
                                    frames.remove(0);
                                    let Some(worker) =
                                        workers.iter_mut().find(|w| w.identity == sender)
                                    else {
                                        continue;
                                    };
                                    worker.expiry = Instant::now() + lifetime;
                                    let service = worker.service.clone();
                                    let mut reply = vec![
                                        client,
                                        Message::from(MDPC_CLIENT.to_vec()),
                                        Message::from(MDPC_FINAL.to_vec()),
                                        Message::from(service.clone().into_bytes()),
                                    ];
                                    reply.extend(frames);
                                    let Ok(reply) = Multipart::new(reply) else { continue };
                                    let _ = socket.send(reply).await;
                                    //  The worker is free again, least
                                    //  recently used last.
                                    waiting.entry(service.clone()).or_default().push_back(sender);
                                    dispatch(&mut socket, &service, &mut waiting, &mut requests)
                                        .await;
                                }
                                MDPW_HEARTBEAT => {
                                    //  "any received command except
                                    //  DISCONNECT acts as a heartbeat"
                                    if let Some(worker) =
                                        workers.iter_mut().find(|w| w.identity == sender)
                                    {
                                        worker.expiry = Instant::now() + lifetime;
                                    }
                                }
                                MDPW_DISCONNECT => {
                                    workers.retain(|w| w.identity != sender);
                                    for queue in waiting.values_mut() {
                                        queue.retain(|identity| *identity != sender);
                                    }
                                }
                                _ => {}
                            },
                            //  A header this broker does not speak: the
                            //  six-byte header exists so that this is
                            //  detectable rather than a mystery.
                            _ => {}
                        }
                    }
                    () = tokio::time::sleep(interval) => {
                        //  Beat at the workers, then disconnect the silent
                        //  ones: "a peer is disconnected if none arrives
                        //  within some multiple of that interval".
                        let now = Instant::now();
                        for worker in &workers {
                            let beat = Multipart::new(vec![
                                worker.identity.clone(),
                                Message::from(MDPW_WORKER.to_vec()),
                                Message::from(MDPW_HEARTBEAT.to_vec()),
                            ])
                            .expect("three frames");
                            let _ = socket.send(beat).await;
                        }
                        let expired: Vec<Message> = workers
                            .iter()
                            .filter(|worker| worker.expiry <= now)
                            .map(|worker| worker.identity.clone())
                            .collect();
                        for identity in expired {
                            let goodbye = Multipart::new(vec![
                                identity.clone(),
                                Message::from(MDPW_WORKER.to_vec()),
                                Message::from(MDPW_DISCONNECT.to_vec()),
                            ])
                            .expect("three frames");
                            let _ = socket.send(goodbye).await;
                            disconnects_sent.fetch_add(1, Ordering::Relaxed);
                            workers.retain(|w| w.identity != identity);
                            for queue in waiting.values_mut() {
                                queue.retain(|waiting| *waiting != identity);
                            }
                        }
                    }
                }
            }
        }
    });

    Ok(Broker {
        endpoint,
        ready_seen,
        disconnects_sent,
        _running: running,
    })
}

/// `s_service_dispatch`: marry queued requests to waiting workers while both
/// lists have something in them.
async fn dispatch(
    socket: &mut RouterSocket,
    service: &str,
    waiting: &mut HashMap<String, VecDeque<Message>>,
    requests: &mut HashMap<String, VecDeque<Vec<Message>>>,
) {
    loop {
        let (Some(workers), Some(queue)) = (waiting.get_mut(service), requests.get_mut(service))
        else {
            return;
        };
        if workers.is_empty() || queue.is_empty() {
            return;
        }
        //  Least recently used: the front of the queue is the worker that has
        //  been waiting longest.
        let worker = workers.pop_front().expect("checked");
        let mut request = queue.pop_front().expect("checked");
        let client = request.remove(0);
        let mut frames = vec![
            worker,
            Message::from(MDPW_WORKER.to_vec()),
            Message::from(MDPW_REQUEST.to_vec()),
            client,
            Message::empty(),
        ];
        frames.extend(request);
        let Ok(message) = Multipart::new(frames) else {
            return;
        };
        let _ = socket.send(message).await;
    }
}

/// What a worker has done, for the claims that are about counts.
pub struct WorkerStats {
    /// Requests answered with a `FINAL`.
    pub served: Arc<AtomicUsize>,
    /// Times this worker sent `READY` — one more than zero means it
    /// re-registered after a `DISCONNECT`.
    pub registrations: Arc<AtomicUsize>,
    /// `DISCONNECT`s received from the broker.
    pub disconnected: Arc<AtomicUsize>,
    /// The task, so a test can stop the worker.
    pub task: tokio::task::JoinHandle<()>,
}

/// `mdworker.c`: a DEALER that registers for one service, answers requests
/// with `FINAL`, beats, and on `DISCONNECT` closes its socket and reconnects
/// on a new one.
///
/// # Errors
///
/// What constructing or connecting a socket reports.
pub fn majordomo_worker(
    context: &Context,
    endpoint: &str,
    service: &str,
    interval: Duration,
) -> Result<WorkerStats> {
    let served = Arc::new(AtomicUsize::new(0));
    let registrations = Arc::new(AtomicUsize::new(0));
    let disconnected = Arc::new(AtomicUsize::new(0));
    let task = tokio::spawn({
        let context = context.clone();
        let endpoint = endpoint.to_owned();
        let service = service.to_owned();
        let served = Arc::clone(&served);
        let registrations = Arc::clone(&registrations);
        let disconnected = Arc::clone(&disconnected);
        async move {
            'reconnect: loop {
                let Ok(mut socket) = DealerSocket::new(&context) else {
                    return;
                };
                if socket.connect(&endpoint).is_err() {
                    return;
                }
                let ready = Multipart::new(vec![
                    Message::from(MDPW_WORKER.to_vec()),
                    Message::from(MDPW_READY.to_vec()),
                    Message::from(service.clone().into_bytes()),
                ])
                .expect("three frames");
                if socket.send(ready).await.is_err() {
                    return;
                }
                registrations.fetch_add(1, Ordering::Relaxed);

                loop {
                    tokio::select! {
                        arrived = socket.recv() => {
                            let Ok(message) = arrived else { continue 'reconnect };
                            let mut frames = message.into_frames();
                            if frames.len() < 2 || frames[0].as_slice() != MDPW_WORKER {
                                continue;
                            }
                            frames.remove(0);
                            let command = frames.remove(0);
                            match command.as_slice() {
                                MDPW_REQUEST => {
                                    //  [client identity][empty][body...]
                                    if frames.len() < 2 {
                                        continue;
                                    }
                                    let client = frames.remove(0);
                                    frames.remove(0);
                                    let mut reply = vec![
                                        Message::from(MDPW_WORKER.to_vec()),
                                        Message::from(MDPW_FINAL.to_vec()),
                                        client,
                                        Message::empty(),
                                    ];
                                    reply.extend(frames);
                                    let Ok(reply) = Multipart::new(reply) else { continue };
                                    if socket.send(reply).await.is_err() {
                                        continue 'reconnect;
                                    }
                                    served.fetch_add(1, Ordering::Relaxed);
                                }
                                MDPW_HEARTBEAT => {}
                                MDPW_DISCONNECT => {
                                    //  "the worker MUST close its socket and
                                    //  reconnect on a new one"
                                    disconnected.fetch_add(1, Ordering::Relaxed);
                                    socket.close();
                                    continue 'reconnect;
                                }
                                _ => {}
                            }
                        }
                        () = tokio::time::sleep(interval) => {
                            let beat = Multipart::new(vec![
                                Message::from(MDPW_WORKER.to_vec()),
                                Message::from(MDPW_HEARTBEAT.to_vec()),
                            ])
                            .expect("two frames");
                            if socket.send(beat).await.is_err() {
                                continue 'reconnect;
                            }
                        }
                    }
                }
            }
        }
    });
    Ok(WorkerStats {
        served,
        registrations,
        disconnected,
        task,
    })
}

/// `mdclient.c`/`mdcliapi`: one request to a named service, with the API's
/// own retries.
///
/// Returns the `FINAL` body, or `None` when the broker never answered — the
/// same "in-order reply or abandonment" Lazy Pirate has, which is what
/// `mdcliapi` is built on.
///
/// # Errors
///
/// What constructing or connecting a socket reports.
pub async fn majordomo_request(
    context: &Context,
    endpoint: &str,
    service: &str,
    body: &str,
    timeout: Duration,
    retries: usize,
) -> Result<Option<String>> {
    for _ in 0..retries {
        let mut client = DealerSocket::new(context)?;
        client.connect(endpoint)?;
        let request = Multipart::new(vec![
            Message::from(MDPC_CLIENT.to_vec()),
            Message::from(MDPC_REQUEST.to_vec()),
            Message::from(service.as_bytes().to_vec()),
            Message::from(body.as_bytes().to_vec()),
        ])
        .expect("four frames");
        client.send(request).await?;
        if let Ok(reply) = client.recv_timeout(timeout).await {
            let frames = reply.into_frames();
            //  [MDPC02][FINAL][service][body...]
            if frames.len() >= 4
                && frames[0].as_slice() == MDPC_CLIENT
                && frames[1].as_slice() == MDPC_FINAL
                && frames[2].as_slice() == service.as_bytes()
            {
                return Ok(Some(
                    String::from_utf8_lossy(frames[3].as_slice()).into_owned(),
                ));
            }
        }
        println!("W: no reply, reconnecting...");
    }
    Ok(None)
}

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new(ContextConfig::default())?;
    let broker = majordomo_broker(&context, HEARTBEAT_INTERVAL, HEARTBEAT_LIVENESS).await?;
    let worker = majordomo_worker(&context, &broker.endpoint, "echo", HEARTBEAT_INTERVAL)?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let reply = majordomo_request(
        &context,
        &broker.endpoint,
        "echo",
        "Hello world",
        HEARTBEAT_INTERVAL,
        3,
    )
    .await?;
    println!("echo said {reply:?}");

    //  Service discovery, which rides on MDP without changing it.
    for service in ["echo", "nosuchservice"] {
        let status = majordomo_request(
            &context,
            &broker.endpoint,
            "mmi.service",
            service,
            HEARTBEAT_INTERVAL,
            3,
        )
        .await?;
        println!("mmi.service {service} -> {status:?}");
    }
    println!(
        "worker served {} request(s), registered {} time(s)",
        worker.served.load(Ordering::Relaxed),
        worker.registrations.load(Ordering::Relaxed)
    );
    Ok(())
}
