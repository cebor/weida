//! PLAIN and the ZAP dialog through the public API.
//!
//! Two sockets, a handler on a REP socket over `inproc://zeromq.zap.01`, and
//! the questions an application can actually ask: does a good password get
//! through, does a bad one get refused before anything flows, and can the
//! application see who was let in.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use weida_zmq::{
    Context, ContextConfig, Multipart, RepSocket, ReqSocket, SocketOptions, ZAP_ENDPOINT, ZapReply,
    ZapRequest, ZapStatus,
};

fn context() -> Context {
    Context::new(ContextConfig::default()).expect("context")
}

fn text(message: &Multipart) -> Vec<u8> {
    message.frames()[0].as_slice().to_vec()
}

/// What the handler was asked, so that a test can check the request as well
/// as the outcome.
type Asked = Arc<Mutex<Vec<ZapRequest>>>;

/// Starts a ZAP handler that answers with `decide`.
///
/// Bound before it is spawned, which is 27/ZAP's "the handler SHALL start
/// before any server starts" — and also what keeps the task's future `Send`,
/// since a socket may be moved into a task but never shared with one.
async fn handler(
    context: &Context,
    decide: impl Fn(ZapRequest) -> ZapReply + Send + 'static,
) -> (Asked, tokio::task::JoinHandle<()>) {
    let asked: Asked = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&asked);
    let mut socket = RepSocket::new(context).expect("rep");
    socket.bind(ZAP_ENDPOINT).await.expect("bind the handler");
    let task = tokio::spawn(async move {
        while let Ok(message) = socket.recv().await {
            let request = ZapRequest::decode(&message).expect("a ZAP request");
            seen.lock().expect("seen").push(request.clone());
            if socket.send(decide(request).encode()).await.is_err() {
                return;
            }
        }
    });
    (asked, task)
}

fn plain_server(domain: &str) -> SocketOptions {
    SocketOptions {
        plain_server: true,
        zap_domain: domain.to_owned(),
        ..SocketOptions::default()
    }
}

fn plain_client(username: &str, password: &str) -> SocketOptions {
    SocketOptions {
        plain_username: Some(username.to_owned()),
        plain_password: Some(password.to_owned()),
        ..SocketOptions::default()
    }
}

/// Claim: a PLAIN exchange the handler allows works end to end, the handler
/// is asked exactly what 27/ZAP says it is asked, and the user id it returned
/// is readable on the peer — a per-connection fact, and never an identity.
#[tokio::test]
async fn a_plain_client_the_handler_allows_gets_through() {
    let ctx = context();
    let (asked, zap) = handler(&ctx, |request| {
        let (username, password) = request.plain_credentials().expect("PLAIN credentials");
        if username == b"admin" && password == b"secret" {
            ZapReply::allowed(request.request_id, "operator")
        } else {
            ZapReply::refused(
                request.request_id,
                ZapStatus::AuthenticationFailure,
                "no such user",
            )
        }
    })
    .await;

    let mut server = RepSocket::with_options(&ctx, plain_server("test")).expect("rep");
    let endpoint = server.bind("tcp://127.0.0.1:0").await.expect("bind");
    let mut client = ReqSocket::with_options(&ctx, plain_client("admin", "secret")).expect("req");
    client.connect(&endpoint.to_string()).expect("connect");

    client.send("question").await.expect("request");
    assert_eq!(text(&server.recv().await.expect("recv")), b"question");
    server.send("answer").await.expect("reply");
    assert_eq!(text(&client.recv().await.expect("reply")), b"answer");

    // What the handler saw.
    let seen = asked.lock().expect("asked");
    let request = seen.first().expect("one request");
    assert_eq!(request.mechanism, "PLAIN");
    assert_eq!(request.domain, "test");
    assert_eq!(request.address, "127.0.0.1", "a tcp peer has an address");
    assert!(
        request.local_principal.is_none(),
        "a tcp peer has no kernel credentials"
    );
    drop(seen);

    // And what the server kept: the user id, on the connection it belongs to.
    let peer = server
        .connections()
        .into_iter()
        .next()
        .expect("one connection");
    assert_eq!(
        peer.user_id.expect("a user id").as_str(),
        "operator",
        "the handler's answer is held per connection"
    );
    zap.abort();
}

/// Claim: a 400 refuses the connection **before any message flows** — the
/// client's request never reaches the server's application, and the server
/// has no peer to show for it.
#[tokio::test]
async fn a_four_hundred_stops_the_connection_before_any_message() {
    let ctx = context();
    let (asked, zap) = handler(&ctx, |request| {
        ZapReply::refused(
            request.request_id,
            ZapStatus::AuthenticationFailure,
            "wrong password",
        )
    })
    .await;

    let mut server = RepSocket::with_options(&ctx, plain_server("test")).expect("rep");
    let endpoint = server.bind("tcp://127.0.0.1:0").await.expect("bind");
    let mut client = ReqSocket::with_options(
        &ctx,
        SocketOptions {
            // Without a bound, a refused REQ would wait forever for a reply
            // that cannot come, which is the correct behaviour and a bad
            // test.
            recv_timeout: Some(Duration::from_millis(200)),
            ..plain_client("admin", "guess")
        },
    )
    .expect("req");
    client.connect(&endpoint.to_string()).expect("connect");

    // The send is accepted: a queue exists before a connection does. What
    // never happens is the delivery.
    client.send("question").await.expect("queued");
    let err = client.recv().await.unwrap_err();
    assert_eq!(err.errno(), "EAGAIN", "{err}");
    assert!(
        server.try_recv().is_err(),
        "a refused connection must deliver nothing to the application"
    );
    let questions = asked.lock().expect("asked").len();
    assert!(questions > 0, "the handler was asked");
    zap.abort();
}

/// Claim: a PLAIN server with no handler in its context admits nobody — the
/// credentials would otherwise be theatre, and 27/ZAP requires the handler to
/// exist before the server does.
#[tokio::test]
async fn a_plain_server_without_a_handler_admits_nobody() {
    let ctx = context();
    let mut server = RepSocket::with_options(&ctx, plain_server("test")).expect("rep");
    let endpoint = server.bind("tcp://127.0.0.1:0").await.expect("bind");
    let mut client = ReqSocket::with_options(
        &ctx,
        SocketOptions {
            recv_timeout: Some(Duration::from_millis(200)),
            ..plain_client("admin", "secret")
        },
    )
    .expect("req");
    client.connect(&endpoint.to_string()).expect("connect");

    client.send("question").await.expect("queued");
    let err = client.recv().await.unwrap_err();
    assert_eq!(err.errno(), "EAGAIN", "{err}");
    assert!(server.try_recv().is_err());
}

/// Claim: one handler per context, enforced by the namespace itself — the
/// second bind of the ZAP endpoint is `EADDRINUSE`, and a second context has
/// its own handler and its own answer.
#[tokio::test]
async fn one_handler_per_context() {
    let here = context();
    let (_asked, zap) = handler(&here, |request| {
        ZapReply::allowed(request.request_id, "here")
    })
    .await;

    let second = RepSocket::new(&here).expect("rep");
    let err = second.bind(ZAP_ENDPOINT).await.unwrap_err();
    assert_eq!(err.errno(), "EADDRINUSE", "{err}");

    // A second context is a second ZeroMQ instance, so the same endpoint is
    // free there.
    let there = context();
    let (_theirs, other) = handler(&there, |request| {
        ZapReply::refused(request.request_id, ZapStatus::TemporaryError, "not now")
    })
    .await;

    let server = RepSocket::with_options(&there, plain_server("test")).expect("rep");
    let endpoint = server.bind("tcp://127.0.0.1:0").await.expect("bind");
    let mut client = ReqSocket::with_options(
        &there,
        SocketOptions {
            recv_timeout: Some(Duration::from_millis(200)),
            ..plain_client("admin", "secret")
        },
    )
    .expect("req");
    client.connect(&endpoint.to_string()).expect("connect");
    client.send("question").await.expect("queued");
    assert!(
        client.recv().await.is_err(),
        "this context's handler refuses, and the other context's does not speak for it"
    );

    zap.abort();
    other.abort();
}

/// Claim: the configurations that cannot be delivered are refused at
/// construction, which is this library's rule for every option — a socket
/// that is both PLAIN ends, a username with no password, and
/// `ZMQ_ZAP_ENFORCE_DOMAIN` with nothing to enforce.
#[tokio::test]
async fn unusable_security_configurations_are_refused() {
    let ctx = context();
    for (options, what) in [
        (
            SocketOptions {
                plain_server: true,
                ..plain_client("admin", "secret")
            },
            "both ends of PLAIN",
        ),
        (
            SocketOptions {
                plain_username: Some("admin".to_owned()),
                ..SocketOptions::default()
            },
            "a username with no password",
        ),
        (
            SocketOptions {
                plain_password: Some("secret".to_owned()),
                ..SocketOptions::default()
            },
            "a password with no username",
        ),
        (
            SocketOptions {
                zap_enforce_domain: true,
                ..SocketOptions::default()
            },
            "enforce with no domain",
        ),
        (
            SocketOptions {
                plain_username: Some("x".repeat(256)),
                plain_password: Some(String::new()),
                ..SocketOptions::default()
            },
            "a username past the field's length octet",
        ),
    ] {
        let err = RepSocket::with_options(&ctx, options).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{what}: {err}");
    }
}
