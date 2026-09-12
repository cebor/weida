//! The option table, walked, and the one option the manual insists is
//! per-endpoint.

use std::time::Duration;

use weida_nng::{
    Context, ContextConfig, Disposition, EndpointOptions, OPTIONS, PullSocket, PushSocket,
    SocketOptions, optiontable,
};

/// Claim: a walk over the whole table finds every option either honoured
/// under a named construct or refused with a reason — never silently
/// ignored (§4.4 item 4 of decision 0013).
///
/// The plausible bug this fails on is a row whose refusal prints nothing
/// a caller can act on, which is how "refused with the reason named"
/// becomes "refused".
#[test]
fn every_option_is_honoured_or_refused_with_a_reason() {
    let mut honoured = 0usize;
    let mut refused = 0usize;
    for row in OPTIONS {
        match row.disposition {
            Disposition::Honoured(what) => {
                honoured += 1;
                assert!(
                    optiontable::require_honoured(row.name).is_ok(),
                    "{} is honoured by {what} but the lookup refuses it",
                    row.name
                );
            }
            Disposition::Refused(why) => {
                refused += 1;
                let error = optiontable::require_honoured(row.name)
                    .expect_err("a refused option is refused");
                assert!(error.cause().contains(row.name));
                assert!(error.cause().contains(why.reason()));
            }
        }
    }
    assert!(honoured > 25, "only {honoured} options are honoured");
    assert!(refused > 8, "only {refused} options are refused");
}

/// Claim: `NNG_OPT_RECVMAXSZ` is settable **per endpoint before that
/// endpoint starts**, as the manual requires, and the endpoint's number
/// wins over the socket's — which is what makes two trust boundaries on
/// one socket possible (§3, §11).
#[tokio::test]
async fn recvmaxsz_is_settable_per_endpoint() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let generous = SocketOptions {
        recv_max_size: 1024 * 1024,
        recv_timeout: Some(Duration::from_millis(500)),
        handshake_timeout: Duration::from_secs(2),
        ..SocketOptions::default()
    };

    // The socket would take a mebibyte; this listener takes 64 bytes,
    // because it is the address a stranger reaches.
    let sink = PullSocket::with_options(&ctx, generous.clone()).expect("pull");
    let tight = sink
        .listen_with("tcp://127.0.0.1:0", EndpointOptions::with_recv_max_size(64))
        .await
        .expect("listen");
    let tight_url = tight.url().to_string();

    // And a second listener on the socket's own generous limit, to show
    // the two numbers coexist rather than one replacing the other.
    let roomy = sink
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let over = PushSocket::with_options(&ctx, generous.clone()).expect("push");
    over.dial(&tight_url).await.expect("dial");
    over.send(vec![0u8; 4096]).await.expect("send");
    for _ in 0..200 {
        if over.pipe_count() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        over.pipe_count(),
        0,
        "4 KiB past a 64-byte per-listener limit costs the pipe"
    );

    let under = PushSocket::with_options(&ctx, generous).expect("push");
    under.dial(&roomy).await.expect("dial");
    under.send(vec![0u8; 4096]).await.expect("send");
    assert_eq!(
        sink.recv().await.expect("the roomy listener took it").len(),
        4096,
        "the other listener still runs on the socket's own limit"
    );
}
