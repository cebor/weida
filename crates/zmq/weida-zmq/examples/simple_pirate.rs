//! The zguide's **Simple Pirate**, chapter 4: `spqueue.c` (the chapter 3
//! load-balancing broker) and `spworker.c`, with `lpclient.c` unchanged in
//! front of them.
//!
//! ```text
//! cargo run -p weida-zmq --features blocking --example simple_pirate
//! ```
//!
//! The guide's problem: "Lazy Pirate cannot fail over to another worker."
//! The mechanism is "an unchanged Lazy Pirate client in front of the chapter
//! 3 load-balancing broker; the server becomes a stateless worker signalling
//! `ready` with REQ". The claim the test asserts: **workers may crash and
//! restart repeatedly while the queue runs, and client retries recover from
//! a dead worker.**
//!
//! That the client is unchanged is not a comment here — this file *includes*
//! [`lazy_pirate`] as a module and calls its request function.
//!
//! `spqueue.c`, the part that matters:
//!
//! ```c
//! zmq_pollitem_t items [] = {
//!     { backend,  0, ZMQ_POLLIN, 0 },
//!     { frontend, 0, ZMQ_POLLIN, 0 }
//! };
//! //  Poll frontend only if we have available workers
//! int rc = zmq_poll (items, zlist_size (workers) ? 2 : 1, -1);
//! //  Handle worker activity on backend
//! if (items [0].revents & ZMQ_POLLIN) {
//!     zmsg_t *msg = zmsg_recv (backend);
//!     zframe_t *identity = zmsg_unwrap (msg);
//!     zlist_append (workers, identity);
//!     //  Forward message to client if it's not a READY
//!     zframe_t *frame = zmsg_first (msg);
//!     if (memcmp (zframe_data (frame), WORKER_READY, 1) == 0) zmsg_destroy (&msg);
//!     else zmsg_send (&msg, frontend);
//! }
//! if (items [1].revents & ZMQ_POLLIN) {
//!     //  Get client request, route to first available worker
//!     zmsg_t *msg = zmsg_recv (frontend);
//!     zmsg_wrap (msg, (zframe_t *) zlist_pop (workers));
//!     zmsg_send (&msg, backend);
//! }
//! ```
//!
//! # Which surface, and why
//!
//! * The **queue is async**. Its C original is a `zmq_poll` over two
//!   sockets, with the frontend polled only when a worker is free, and
//!   `tokio::select!` with a guard is that loop written once. A blocking
//!   wrapper would have to poll, which is the thing the reactor already does.
//! * The **worker is the blocking facade**. `spworker.c` is a REQ socket and
//!   straight-line `recv`/`send`; there is nothing to interleave.
//! * The **client is Lazy Pirate's**, so whatever that recipe uses, this one
//!   uses: the blocking facade.
//!
//! # The three differences from the C
//!
//! * **`zmsg_wrap`/`zmsg_unwrap` are explicit frames.** The C hides the
//!   envelope behind CZMQ; here the routing id is frame zero of what a ROUTER
//!   receives and frame zero of what it sends, and the empty delimiter is
//!   written out. Nothing is added or removed relative to the C — the same
//!   five frames cross the backend.
//! * **The worker's crash is a parameter.** `spworker.c` exits at random
//!   ("simulate various problems"); here it dies after a given number of
//!   requests, so a test can make the crash happen where it matters.
//! * **The queue counts what it does.** `READY`s seen and requests routed are
//!   atomics a test can read, where the C prints them.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use weida_zmq::blocking::{BlockingContext, ReqSocket};
use weida_zmq::{Context, ContextConfig, Message, Multipart, Result, RouterSocket};

#[path = "lazy_pirate.rs"]
#[allow(dead_code)]
pub mod lazy_pirate;

/// `#define WORKER_READY "\001"`: the signal a worker sends when it has
/// nothing in hand.
pub const WORKER_READY: &[u8] = b"\x01";

/// A running load-balancing broker: its two endpoints, and what it has done.
pub struct Queue {
    /// Where Lazy Pirate clients connect.
    pub frontend: String,
    /// Where workers connect.
    pub backend: String,
    /// `READY` signals received, which is how a test knows a worker joined.
    pub ready_seen: Arc<AtomicUsize>,
    /// Requests routed to a worker.
    pub routed: Arc<AtomicUsize>,
    _running: tokio::task::JoinHandle<()>,
}

/// `spqueue.c`: ROUTER in front, ROUTER behind, a queue of free workers.
///
/// # Errors
///
/// What binding a socket reports.
pub async fn load_balancing_broker(context: &Context) -> Result<Queue> {
    let frontend = RouterSocket::new(context)?;
    let backend = RouterSocket::new(context)?;
    let frontend_endpoint = frontend.bind("tcp://127.0.0.1:0").await?.to_string();
    let backend_endpoint = backend.bind("tcp://127.0.0.1:0").await?.to_string();
    let ready_seen = Arc::new(AtomicUsize::new(0));
    let routed = Arc::new(AtomicUsize::new(0));

    let running = tokio::spawn({
        let ready_seen = Arc::clone(&ready_seen);
        let routed = Arc::clone(&routed);
        let mut frontend = frontend;
        let mut backend = backend;
        async move {
            //  Queue of available workers, oldest first.
            let mut workers: VecDeque<Message> = VecDeque::new();
            loop {
                tokio::select! {
                    //  Handle worker activity on backend
                    arrived = backend.recv() => {
                        let Ok(message) = arrived else { return };
                        let mut frames = message.into_frames();
                        //  zmsg_unwrap: the routing id, then the delimiter a
                        //  REQ socket wrote.
                        let identity = frames.remove(0);
                        if !frames.is_empty() && frames[0].is_empty() {
                            frames.remove(0);
                        }
                        workers.push_back(identity);
                        match frames.first() {
                            //  Forward message to client if it's not a READY
                            Some(frame) if frame.as_slice() == WORKER_READY => {
                                ready_seen.fetch_add(1, Ordering::Relaxed);
                            }
                            _ => {
                                let Ok(reply) = Multipart::new(frames) else { continue };
                                if frontend.send(reply).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    //  Poll frontend only if we have available workers
                    arrived = frontend.recv(), if !workers.is_empty() => {
                        let Ok(message) = arrived else { return };
                        let worker = workers.pop_front().expect("a worker was available");
                        //  zmsg_wrap: the worker's routing id and a delimiter
                        //  in front of the client's own envelope.
                        let mut frames = vec![worker, Message::empty()];
                        frames.extend(message.into_frames());
                        let Ok(routed_message) = Multipart::new(frames) else { continue };
                        if backend.send(routed_message).await.is_err() {
                            return;
                        }
                        routed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    });

    Ok(Queue {
        frontend: frontend_endpoint,
        backend: backend_endpoint,
        ready_seen,
        routed,
        _running: running,
    })
}

/// `spworker.c`: a stateless worker that signals `READY` and then echoes,
/// dying after `crash_after` requests if that is `Some`.
///
/// # Errors
///
/// What constructing or connecting a socket reports.
pub fn simple_pirate_worker(
    context: &BlockingContext,
    backend: &str,
    crash_after: Option<usize>,
) -> Result<thread::JoinHandle<()>> {
    let mut worker = ReqSocket::new(context)?;
    worker.connect(backend)?;
    //  Tell the queue we're ready for work
    worker.send(Multipart::single(WORKER_READY.to_vec()))?;
    Ok(thread::spawn(move || {
        let mut served = 0;
        loop {
            //  The request arrives with the client's envelope in front of it:
            //  [client id][empty][body].
            let Ok(request) = worker.recv() else { return };
            if crash_after.is_some_and(|limit| served >= limit) {
                println!("I: simulating a crash");
                return;
            }
            served += 1;
            //  Echo it back, envelope and all, which is what makes the
            //  client's sequence check pass.
            if worker.send(request).is_err() {
                return;
            }
        }
    }))
}

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new(ContextConfig::default())?;
    let queue = load_balancing_broker(&context).await?;
    let blocking = BlockingContext::new()?;

    //  A worker that dies on its first request, and a second one that takes
    //  over — "workers may crash and restart repeatedly while the queue
    //  runs".
    let _doomed = simple_pirate_worker(&blocking, &queue.backend, Some(0))?;
    thread::sleep(Duration::from_millis(100));
    let _healthy = simple_pirate_worker(&blocking, &queue.backend, None)?;

    let frontend = queue.frontend.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        lazy_pirate::lazy_pirate_request(
            &blocking,
            &frontend,
            1,
            Duration::from_millis(500),
            lazy_pirate::REQUEST_RETRIES,
        )
    })
    .await
    .expect("the client thread")?;
    println!("{outcome:?}");
    println!(
        "queue: {} READY, {} routed",
        queue.ready_seen.load(Ordering::Relaxed),
        queue.routed.load(Ordering::Relaxed)
    );
    Ok(())
}
