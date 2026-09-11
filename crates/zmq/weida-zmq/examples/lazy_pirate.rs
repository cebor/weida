//! The zguide's **Lazy Pirate**, chapter 4: `lpclient.c` and `lpserver.c`.
//!
//! ```text
//! cargo run -p weida-zmq --features blocking --example lazy_pirate
//! ```
//!
//! The guide's problem and claim, verbatim: "A blocking REQ client hangs
//! forever if its server crashes, or request/reply is lost." The mechanism is
//! to poll the REQ socket, resend on timeout, and give up after several
//! attempts; and because REQ enforces strict alternation and yields `EFSM`
//! otherwise, "The brute-force remedy is close and reopen the REQ socket
//! after an error." The guarantee is the thing the test asserts: **the client
//! gets an in-order reply or abandons, never blocking indefinitely.**
//!
//! `lpclient.c`, for comparison:
//!
//! ```c
//! #define REQUEST_TIMEOUT     2500    //  msecs, (> 1000!)
//! #define REQUEST_RETRIES     3       //  Before we abandon
//!
//! void *client = zsocket_new (ctx, ZMQ_REQ);
//! zsocket_connect (client, SERVER_ENDPOINT);
//! int sequence = 0;
//! int retries_left = REQUEST_RETRIES;
//! while (retries_left) {
//!     char request [10];
//!     sprintf (request, "%d", ++sequence);
//!     zstr_send (client, request);
//!     int expect_reply = 1;
//!     while (expect_reply) {
//!         zmq_pollitem_t items [] = { { client, 0, ZMQ_POLLIN, 0 } };
//!         int rc = zmq_poll (items, 1, REQUEST_TIMEOUT * ZMQ_POLL_MSEC);
//!         if (items [0].revents & ZMQ_POLLIN) {
//!             char *reply = zstr_recv (client);
//!             if (atoi (reply) == sequence) { retries_left = REQUEST_RETRIES; expect_reply = 0; }
//!             else printf ("E: malformed reply from server: %s\n", reply);
//!         }
//!         else if (--retries_left == 0) { puts ("E: server seems to be offline, abandoning"); break; }
//!         else {
//!             puts ("W: no response from server, retrying...");
//!             zsocket_destroy (ctx, client);          //  Old socket is confused
//!             client = zsocket_new (ctx, ZMQ_REQ);    //  Create new socket
//!             zsocket_connect (client, SERVER_ENDPOINT);
//!             zstr_send (client, request);            //  Send request again
//!         }
//!     }
//! }
//! ```
//!
//! # Which surface, and why
//!
//! The **blocking facade**. The C client is straight-line synchronous code
//! whose only concurrency is a `zmq_poll` with a timeout, and
//! [`RepSocket::recv_timeout`](weida_zmq::blocking::ReqSocket::recv_timeout)
//! *is* that poll: one socket, one wait, one bound. There is nothing for a
//! reactor to interleave, so borrowing one would be decoration.
//!
//! # The three differences from the C
//!
//! * **The timeout and the retry count are arguments**, not `#define`s, so a
//!   test can drive the same code in milliseconds. `REQUEST_TIMEOUT` and
//!   `REQUEST_RETRIES` below are the guide's own values and are what `main`
//!   uses.
//! * **The outcome is a value.** The C prints and breaks; this returns
//!   [`Outcome`], because "an in-order reply or abandonment" is a claim a
//!   test should be able to read.
//! * **A reply out of sequence is counted, not just printed.** The C logs
//!   "malformed reply" and keeps waiting; so does this, and it reports how
//!   many it discarded, which is what makes "in-order" observable.

use std::thread;
use std::time::Duration;

use weida_zmq::blocking::{BlockingContext, RepSocket, ReqSocket};
use weida_zmq::{Multipart, Result};

/// `#define REQUEST_TIMEOUT 2500 // msecs, (> 1000!)`
pub const REQUEST_TIMEOUT: Duration = Duration::from_millis(2500);
/// `#define REQUEST_RETRIES 3 // Before we abandon`
pub const REQUEST_RETRIES: usize = 3;

/// What one Lazy Pirate exchange ended as — the guide's guarantee, as a
/// value: an in-order reply, or abandonment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The reply whose sequence number matches the request's.
    Reply {
        /// The reply body.
        body: String,
        /// How many attempts it took, the first one included.
        attempts: usize,
        /// Replies discarded for carrying another sequence number.
        out_of_sequence: usize,
    },
    /// No reply after `REQUEST_RETRIES` attempts. The client is free, not
    /// blocked.
    Abandoned {
        /// How many attempts were made.
        attempts: usize,
    },
}

/// One Lazy Pirate request: send, wait, resend, abandon.
///
/// # Errors
///
/// Only what constructing or connecting a socket reports; a silent server is
/// [`Outcome::Abandoned`] rather than an error, because that is the whole
/// point of the recipe.
pub fn lazy_pirate_request(
    context: &BlockingContext,
    endpoint: &str,
    sequence: u64,
    timeout: Duration,
    retries: usize,
) -> Result<Outcome> {
    let request = sequence.to_string();
    let mut client = ReqSocket::new(context)?;
    client.connect(endpoint)?;
    client.send(Multipart::single(request.clone()))?;

    let mut attempts = 1;
    let mut out_of_sequence = 0;
    loop {
        match client.recv_timeout(timeout) {
            Ok(reply) => {
                let body = String::from_utf8_lossy(reply.frames()[0].as_slice()).into_owned();
                if body == request {
                    println!("I: server replied OK ({body})");
                    return Ok(Outcome::Reply {
                        body,
                        attempts,
                        out_of_sequence,
                    });
                }
                //  E: malformed reply from server
                println!("E: malformed reply from server: {body}");
                out_of_sequence += 1;
            }
            Err(_) => {
                if attempts >= retries {
                    println!("E: server seems to be offline, abandoning");
                    return Ok(Outcome::Abandoned { attempts });
                }
                println!("W: no response from server, retrying...");
                //  Old socket is confused; close it and open a new one. Ours
                //  reports `EFSM` on the next send otherwise, exactly as
                //  libzmq does.
                client.close();
                client = ReqSocket::new(context)?;
                client.connect(endpoint)?;
                client.send(Multipart::single(request.clone()))?;
                attempts += 1;
            }
        }
    }
}

/// `lpserver.c`, which "simulates a crash": it takes a request, dies without
/// answering, and is restarted on the same endpoint.
///
/// The C original exits the process at random and a human restarts it; the
/// crash the client sees is the same either way, and the client's side is
/// what the recipe is about. `crashes` requests are swallowed, each one
/// destroying and rebinding the socket, and everything after that is
/// echoed — an echo being what makes a reply "in order".
pub struct CrashingServer {
    /// The endpoint it bound, port included: a wildcard bind is read back.
    pub endpoint: String,
    _served: thread::JoinHandle<()>,
}

/// Starts the server. Binding happens here rather than in the thread, so the
/// endpoint is known before a client dials it.
///
/// # Errors
///
/// What binding a socket reports.
pub fn lazy_pirate_server(context: &BlockingContext, crashes: usize) -> Result<CrashingServer> {
    let mut server = RepSocket::new(context)?;
    let bound = server.bind("tcp://127.0.0.1:0")?;
    let endpoint = bound.to_string();
    let restart_at = endpoint.clone();
    let context = context.clone();
    let served = thread::spawn(move || {
        let mut crashes_left = crashes;
        loop {
            let Ok(request) = server.recv() else { return };
            if crashes_left > 0 {
                crashes_left -= 1;
                println!("I: simulating a server crash");
                //  The crash: the socket is destroyed with the request
                //  unanswered, which is what the client has to survive.
                server.close();
                thread::sleep(Duration::from_millis(20));
                let Ok(restarted) = RepSocket::new(&context) else {
                    return;
                };
                server = restarted;
                if server.bind(&restart_at).is_err() {
                    return;
                }
                continue;
            }
            if server.send(request).is_err() {
                return;
            }
        }
    });
    Ok(CrashingServer {
        endpoint,
        _served: served,
    })
}

fn main() -> Result<()> {
    let context = BlockingContext::new()?;
    //  One crash, then a healthy server: the first request is lost and the
    //  retry gets its answer.
    let server = lazy_pirate_server(&context, 1)?;

    for sequence in 1..=3 {
        let outcome = lazy_pirate_request(
            &context,
            &server.endpoint,
            sequence,
            Duration::from_millis(500),
            REQUEST_RETRIES,
        )?;
        println!("{outcome:?}");
    }

    //  And the guarantee's other half: a server that is simply not there.
    let outcome = lazy_pirate_request(
        &context,
        "tcp://127.0.0.1:1",
        99,
        Duration::from_millis(200),
        REQUEST_RETRIES,
    )?;
    println!("no server at all: {outcome:?}");
    Ok(())
}
