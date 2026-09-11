//! The zguide's Hello World, chapter 1: `hwserver.c` and `hwclient.c`.
//!
//! Run it with the facade it exists for:
//!
//! ```text
//! cargo run -p weida-zmq --features blocking --example hello_world
//! ```
//!
//! The point of this file is that it reads like the C. Each function is the
//! zguide's own program with the same statements in the same order, and the
//! comments are the C lines they stand for:
//!
//! ```c
//! //  Hello World server
//! void *context = zmq_ctx_new ();
//! void *responder = zmq_socket (context, ZMQ_REP);
//! int rc = zmq_bind (responder, "tcp://*:5555");
//! assert (rc == 0);
//! while (1) {
//!     char buffer [10];
//!     zmq_recv (responder, buffer, 10, 0);
//!     printf ("Received Hello\n");
//!     sleep (1);
//!     zmq_send (responder, "World", 5, 0);
//! }
//! ```
//!
//! Three things differ, and all three are the language rather than the
//! library:
//!
//! * **The endpoint is `tcp://127.0.0.1:5555`, not `tcp://*:5555`.** The
//!   wildcard host binds every interface, which an example that runs in a
//!   test suite should not do.
//! * **A ZeroMQ message is a value, not a `char buffer[10]`.** `zmq_recv`
//!   into a fixed array truncates at ten octets; `recv` returns the whole
//!   message, and `frames()[0]` is the frame the C code was reading into its
//!   buffer.
//! * **The server's `while (1)` is the client's lifetime.** The C original
//!   loops for ever and is killed with the process, and so does this one: the
//!   server thread is left serving and `main` returns when the client is
//!   done. That is not decoration — a reply is *queued* when `send` returns,
//!   and a socket dropped before its session wrote that queue out takes the
//!   reply with it. libzmq answers that with `ZMQ_LINGER` on `zmq_close`;
//!   this library answers it with a finite close budget on the context
//!   (`DEFAULT_CLOSE_BUDGET`), so a server that outlives its clients has
//!   nothing to linger for.
//!
//! What does *not* differ is the shape: a context, a socket, a bind or a
//! connect, and then send and receive in the order the pattern demands, with
//! no executor, no `async`, no `await` and no runtime in sight.

use std::thread;
use std::time::Duration;

use weida_zmq::Multipart;
use weida_zmq::blocking::{BlockingContext, RepSocket, ReqSocket};

/// How many exchanges: `hwclient.c`'s `for (request_nbr = 0; request_nbr !=
/// 10; request_nbr++)`.
const REQUESTS: usize = 10;

/// `hwserver.c`. Returns the endpoint it bound, since the port is a wildcard
/// here and `ZMQ_LAST_ENDPOINT` is how one reads that back.
fn server(context: &BlockingContext) -> weida_zmq::Result<String> {
    //  Socket to talk to clients
    let mut responder = RepSocket::new(context)?;
    let bound = responder.bind("tcp://127.0.0.1:0")?;
    let endpoint = bound.to_string();

    thread::spawn(move || {
        //  while (1)
        while let Ok(request) = responder.recv() {
            println!(
                "Received {}",
                String::from_utf8_lossy(request.frames()[0].as_slice())
            );
            //  Do some 'work'
            thread::sleep(Duration::from_millis(10));
            //  Send reply back to client
            if responder.send(Multipart::single("World")).is_err() {
                return;
            }
        }
    });
    Ok(endpoint)
}

/// `hwclient.c`.
fn client(context: &BlockingContext, endpoint: &str) -> weida_zmq::Result<()> {
    println!("Connecting to hello world server…");
    let mut requester = ReqSocket::new(context)?;
    requester.connect(endpoint)?;

    for request_nbr in 0..REQUESTS {
        println!("Sending Hello {request_nbr}…");
        requester.send(Multipart::single("Hello"))?;
        let reply = requester.recv()?;
        println!(
            "Received {} {request_nbr}",
            String::from_utf8_lossy(reply.frames()[0].as_slice())
        );
    }
    Ok(())
}

fn main() -> weida_zmq::Result<()> {
    //  One context per process, as the guide says: "Call zmq_ctx_new() once
    //  at the start of a process".
    let context = BlockingContext::new()?;
    let endpoint = server(&context)?;
    client(&context, &endpoint)
}
