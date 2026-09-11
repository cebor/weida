//! The zguide's **Freelance** pattern, chapter 4, models one and two as
//! [10/FLP](https://rfc.zeromq.org/spec/10/) describes them: `flserver1.c`
//! with `flclient1.c`, and `flserver2.c` with `flclient2.c`/`flcliapi`.
//!
//! ```text
//! cargo run -p weida-zmq --example freelance
//! ```
//!
//! The pattern's problem, in the guide's words: "brokerless" reliability,
//! where a client holds a list of server endpoints and "the name service
//! disappears" - no broker to route through, so the client itself has to
//! decide which server answered. 10/FLP's own guarantee is the one asserted
//! in `tests/zguide_majordomo.rs`: a client "SHALL discard replies that do
//! not carry the sequence number of the pending request", which is what
//! makes a shotgunned request safe to retry.
//!
//! The two models, and the C they come from:
//!
//! * **Model one, `flclient1.c`** - "the brutal shotgun massacre". One REQ
//!   socket per endpoint, tried in turn with a timeout; the first server that
//!   answers wins, and a dead endpoint costs one timeout.
//!
//!   ```c
//!   //  Look for at least one server to answer
//!   for (argn = 1; argn < argc; argn++) {
//!       char *endpoint = argv [argn];
//!       printf ("I: trying echo service at %s...\n", endpoint);
//!       void *client = zmq_socket (ctx, ZMQ_REQ);
//!   ```
//!
//! * **Model two, `flclient2.c`** - "the complex shotgun massacre". *One*
//!   DEALER connected to every endpoint, the request sent once per server so
//!   that the round-robin delivers a copy to each, and a sequence number in
//!   the request so that the first matching reply is taken and every later or
//!   older one is thrown away.
//!
//!   ```c
//!   //  We send N spray requests, and wait for a single reply
//!   self->sequence++;
//!   zmsg_t *msg = zmsg_dup (request);
//!   zmsg_pushstrf (msg, "%u", self->sequence);
//!   ```
//!
//!   The empty frame the C pushes in front is not decoration: the servers are
//!   REP sockets, which need a valid envelope, and a DEALER does not write
//!   one for you.
//!
//! # Which surface, and why
//!
//! **Async.** Model one is a poll with a timeout per endpoint, which is
//! `recv_timeout` either way; model two cannot be written any other way -
//! its whole point is one socket with N replies racing on it, where the
//! client keeps reading while discarding.
//!
//! # The three differences from the C
//!
//! * **The sequence number is a `u32` in its own frame, not `%u` printed
//!   into a string.** The wire is `[empty][sequence][body]`, the frame count
//!   and order of `flcliapi`'s, with the number in decimal ASCII exactly as
//!   the C prints it - a reader can put the two side by side.
//! * **Discards are counted, not logged.** "Discard replies that do not
//!   carry the sequence number of the pending request" is a claim about what
//!   the client does with a stale reply, so `stale_discarded` is returned to
//!   the caller instead of printed and forgotten.
//! * **No connection-state machine (that is model three).** `flcliapi`
//!   tracks each server as alive or dead and pings the dead ones; model two
//!   in the guide does not, and neither does this. The tick is that the
//!   shotgun makes liveness unnecessary for a single request, which is what
//!   the example demonstrates by leaving one endpoint unbound.

use std::time::Duration;

use weida_zmq::{
    Context, ContextConfig, DealerSocket, Message, Multipart, RepSocket, ReqSocket, Result,
};

/// `flserver1.c`/`flserver2.c`: a REP socket that echoes, optionally after a
/// delay, which is how a stale reply is made to happen on purpose.
pub struct FreelanceServer {
    /// The endpoint it bound, for the client's list.
    pub endpoint: String,
    /// The serving task.
    pub task: tokio::task::JoinHandle<()>,
}

/// `flserver2.c`: "the server is a REP socket, and nothing else".
///
/// `slow_for` delays the reply to the first `slow_for.1` requests, which no C
/// original has - it is how the stale reply 10/FLP talks about is produced
/// without a race.
///
/// # Errors
///
/// What binding a socket reports.
pub async fn freelance_server(
    context: &Context,
    name: &str,
    slow_for: Option<(Duration, usize)>,
) -> Result<FreelanceServer> {
    let mut server = RepSocket::new(context)?;
    let endpoint = server.bind("tcp://127.0.0.1:0").await?.to_string();
    let name = name.to_owned();
    let task = tokio::spawn(async move {
        let (delay, mut slow_left) = slow_for.unwrap_or((Duration::ZERO, 0));
        while let Ok(request) = server.recv().await {
            if slow_left > 0 {
                slow_left -= 1;
                tokio::time::sleep(delay).await;
            }
            //  Echo the request, with the answering server named so that a
            //  reader can see which one won the shotgun.
            let mut frames = request.into_frames();
            frames.push(Message::from(name.clone().into_bytes()));
            let Ok(reply) = Multipart::new(frames) else {
                return;
            };
            if server.send(reply).await.is_err() {
                return;
            }
        }
    });
    Ok(FreelanceServer { endpoint, task })
}

/// What a Freelance request came to.
#[derive(Debug, PartialEq, Eq)]
pub struct FreelanceReply {
    /// The echo body, with the answering server's name appended.
    pub body: String,
    /// Endpoints tried (model one) or copies sprayed (model two).
    pub attempts: usize,
    /// Replies thrown away because they did not carry the pending sequence
    /// number.
    pub stale_discarded: usize,
}

/// `flclient1.c`: try each endpoint in turn with a REQ socket and a timeout,
/// and take the first answer.
///
/// Returns `None` when no endpoint in the list answered, which is the
/// model's whole failure mode - "if no server is there, the client gives
/// up".
///
/// # Errors
///
/// What constructing or connecting a socket reports.
pub async fn freelance_model_one(
    context: &Context,
    endpoints: &[String],
    body: &str,
    timeout: Duration,
) -> Result<Option<FreelanceReply>> {
    for (index, endpoint) in endpoints.iter().enumerate() {
        println!("I: trying echo service at {endpoint}...");
        let mut client = ReqSocket::new(context)?;
        client.connect(endpoint)?;
        client
            .send(Multipart::single(body.as_bytes().to_vec()))
            .await?;
        if let Ok(reply) = client.recv_timeout(timeout).await {
            let frames = reply.into_frames();
            let text = frames
                .iter()
                .map(|frame| String::from_utf8_lossy(frame.as_slice()).into_owned())
                .collect::<Vec<_>>()
                .join(" ");
            return Ok(Some(FreelanceReply {
                body: text,
                attempts: index + 1,
                stale_discarded: 0,
            }));
        }
        //  The C closes the socket here for the same reason Lazy Pirate does:
        //  a REQ that timed out is out of step with its peer for good.
        println!("W: no response from {endpoint}, moving on");
    }
    Ok(None)
}

/// `flcliapi`: one DEALER connected to every endpoint, with the request
/// sprayed once per server and the pending sequence number deciding which
/// reply counts.
pub struct FreelanceClient {
    socket: DealerSocket,
    servers: usize,
    sequence: u32,
}

impl FreelanceClient {
    /// Connects to every endpoint in the list.
    ///
    /// # Errors
    ///
    /// What constructing or connecting a socket reports.
    pub fn new(context: &Context, endpoints: &[String]) -> Result<Self> {
        let socket = DealerSocket::new(context)?;
        for endpoint in endpoints {
            socket.connect(endpoint)?;
        }
        Ok(Self {
            socket,
            servers: endpoints.len(),
            sequence: 0,
        })
    }

    /// One request, sprayed to all servers, answered by the first reply that
    /// carries this request's sequence number.
    ///
    /// Replies for an earlier request - a slow server answering a question
    /// the client has already moved past - are discarded and counted, which
    /// is 10/FLP's "SHALL discard replies that do not carry the sequence
    /// number of the pending request".
    ///
    /// # Errors
    ///
    /// What sending on the socket reports.
    pub async fn request(
        &mut self,
        body: &str,
        timeout: Duration,
    ) -> Result<Option<FreelanceReply>> {
        self.sequence += 1;
        let sequence = self.sequence.to_string();
        for _ in 0..self.servers {
            //  The empty delimiter is what makes this a valid envelope for a
            //  REP peer; a DEALER writes none.
            let request = Multipart::new(vec![
                Message::empty(),
                Message::from(sequence.clone().into_bytes()),
                Message::from(body.as_bytes().to_vec()),
            ])
            .expect("three frames");
            self.socket.send(request).await?;
        }
        let mut stale_discarded = 0;
        //  Keep reading while the clock allows: a stale reply must not eat
        //  the wait for the real one.
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return Ok(None);
            }
            let Ok(reply) = self.socket.recv_timeout(left).await else {
                return Ok(None);
            };
            let frames = reply.into_frames();
            //  [empty][sequence][body...]
            if frames.len() < 3 || !frames[0].as_slice().is_empty() {
                stale_discarded += 1;
                continue;
            }
            if frames[1].as_slice() != sequence.as_bytes() {
                stale_discarded += 1;
                continue;
            }
            let text = frames[2..]
                .iter()
                .map(|frame| String::from_utf8_lossy(frame.as_slice()).into_owned())
                .collect::<Vec<_>>()
                .join(" ");
            return Ok(Some(FreelanceReply {
                body: text,
                attempts: self.servers,
                stale_discarded,
            }));
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new(ContextConfig::default())?;
    let live = freelance_server(&context, "live", None).await?;

    //  Model one, with a dead endpoint in front of the live one: the list is
    //  tried in order and the timeout is what a dead server costs.
    let endpoints = vec!["tcp://127.0.0.1:1".to_owned(), live.endpoint.clone()];
    let reply = freelance_model_one(
        &context,
        &endpoints,
        "random name",
        Duration::from_millis(300),
    )
    .await?;
    println!("model one: {reply:?}");

    //  Model two, twice: once with a server that answers, and once with one
    //  whose answers are always a request behind, which is what the sequence
    //  number is there for.
    let mut client = FreelanceClient::new(&context, std::slice::from_ref(&live.endpoint))?;
    println!(
        "model two: {:?}",
        client.request("one", Duration::from_secs(2)).await?
    );

    let slow = freelance_server(
        &context,
        "slow",
        Some((Duration::from_millis(250), usize::MAX)),
    )
    .await?;
    let mut client = FreelanceClient::new(&context, std::slice::from_ref(&slow.endpoint))?;
    println!(
        "model two, abandoned: {:?}",
        client.request("one", Duration::from_millis(100)).await?
    );
    println!(
        "model two, stale reply discarded: {:?}",
        client.request("two", Duration::from_secs(2)).await?
    );
    Ok(())
}
