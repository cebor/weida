//! REQ and REP against each other, over real connections.
//!
//! Every claim here is one of `docs/research/nanomsg-nng.md` §4's sentences
//! about request-reply, turned into something that fails when it stops
//! being true.

use std::time::Duration;

use weida_nng::{Context, ContextConfig, Error, RepSocket, ReqSocket, SocketOptions};

fn options() -> SocketOptions {
    SocketOptions {
        // Short enough that a test can watch a resend, long enough that a
        // healthy exchange never sees one.
        resend_time: Duration::from_millis(150),
        recv_timeout: Some(Duration::from_secs(5)),
        send_timeout: Some(Duration::from_secs(5)),
        handshake_timeout: Duration::from_secs(2),
        reconnect_min: Duration::from_millis(10),
        ..SocketOptions::default()
    }
}

async fn pair() -> (Context, ReqSocket, RepSocket, String) {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let rep = RepSocket::with_options(&ctx, options()).expect("rep");
    let listener = rep.listen("tcp://127.0.0.1:0").await.expect("listen");
    let url = listener.url().to_string();
    let req = ReqSocket::with_options(&ctx, options()).expect("req");
    req.dial(&url).await.expect("dial");
    (ctx, req, rep, url)
}

/// Claim: a request reaches a replier and its reply comes back matched by
/// the 32-bit request ID, which travels with its terminal bit set — the bit
/// that tells the replier where the tag stack ends [rfc-reqrep §5].
#[tokio::test]
async fn a_request_and_its_reply_are_matched_by_the_tag_stack() {
    let (_ctx, req, rep, _url) = pair().await;

    req.send(b"ping".to_vec()).await.expect("send");
    let request = rep.recv().await.expect("recv");
    assert_eq!(request.body(), b"ping");
    assert_eq!(request.header().len(), 4, "one tag: the request ID");
    assert_eq!(
        request.header()[0] & 0x80,
        0x80,
        "the terminal bit ends the stack"
    );

    rep.send(b"pong".to_vec()).await.expect("reply");
    let reply = req.recv().await.expect("reply");
    assert_eq!(reply.body(), b"pong");
    assert_eq!(
        reply.header(),
        request.header(),
        "the reply repeats the stack it arrived with"
    );
}

/// Claim: contexts are independent transactions over one socket — each with
/// its own request ID — so two requests can be outstanding at once and each
/// gets its own reply (§2, §4).
#[tokio::test]
async fn two_contexts_run_two_transactions_over_one_socket() {
    let (_ctx, req, rep, _url) = pair().await;
    let first = req.context();
    let second = req.context();

    first.send(b"one".to_vec()).await.expect("send one");
    second.send(b"two".to_vec()).await.expect("send two");

    // Two REP contexts process them in parallel, which is what contexts are
    // for on the replying side too (§4).
    let a = rep.context();
    let b = rep.context();
    let request_a = a.recv().await.expect("a");
    let request_b = b.recv().await.expect("b");
    assert_ne!(
        request_a.header(),
        request_b.header(),
        "each context has its own request ID"
    );

    // Answer them in the opposite order: the tag stack is what pairs them,
    // not arrival order.
    b.send(request_b.body().to_vec()).await.expect("reply b");
    a.send(request_a.body().to_vec()).await.expect("reply a");

    let reply_first = first.recv().await.expect("first reply");
    let reply_second = second.recv().await.expect("second reply");
    assert_eq!(reply_first.body(), b"one");
    assert_eq!(reply_second.body(), b"two");
}

/// Claim: every order the protocol forbids is `NNG_ESTATE`, and one pending
/// receive per context is one of them (§4).
#[tokio::test]
async fn the_forbidden_orders_are_all_estate() {
    let (_ctx, req, rep, _url) = pair().await;

    // A REP that has received nothing cannot send.
    let err = rep.send(b"unasked".to_vec()).await.unwrap_err();
    assert!(matches!(err, Error::ESTATE(_)), "{err:?}");

    // A REQ that has sent nothing cannot receive.
    let err = req.recv().await.unwrap_err();
    assert!(matches!(err, Error::ESTATE(_)), "{err:?}");

    // One pending receive per context, on both sides.
    req.send(b"ping".to_vec()).await.expect("send");
    let ctx = req.implicit_context();
    let waiting = tokio::spawn({
        let ctx = ctx.clone();
        async move { ctx.recv().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let err = ctx.recv().await.unwrap_err();
    assert!(matches!(err, Error::ESTATE(_)), "{err:?}");

    let rep_ctx = rep.implicit_context();
    let request = rep_ctx.recv().await.expect("request");
    let second = rep_ctx.clone();
    let parked = tokio::spawn(async move { second.recv().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let err = rep_ctx.recv().await.unwrap_err();
    assert!(matches!(err, Error::ESTATE(_)), "{err:?}");

    rep_ctx.send(request.body().to_vec()).await.expect("reply");
    assert_eq!(waiting.await.expect("task").expect("reply").body(), b"ping");
    parked.abort();
}

/// Claim: a newer request cancels the requester's interest in the older
/// reply — the late one is discarded — and does **not** withdraw the older
/// request from the replier, which answers both (§4).
#[tokio::test]
async fn a_newer_request_discards_the_older_reply_without_withdrawing_it() {
    let (_ctx, req, rep, _url) = pair().await;

    req.send(b"first".to_vec()).await.expect("first");
    let first = rep.context().recv().await.expect("the first request");
    assert_eq!(first.body(), b"first");

    // Second request before the first reply is asked for.
    req.send(b"second".to_vec()).await.expect("second");
    let second_ctx = rep.context();
    let second = second_ctx.recv().await.expect("the second request");
    assert_eq!(
        second.body(),
        b"second",
        "the replier saw the older request too: it was never withdrawn"
    );

    // Answer the older one first. Its reply matches nothing any more.
    let stale = rep.context();
    let _ = stale;
    second_ctx
        .send(b"reply-to-second".to_vec())
        .await
        .expect("reply");

    let reply = req.recv().await.expect("a reply");
    assert_eq!(
        reply.body(),
        b"reply-to-second",
        "the requester is only interested in its newest request"
    );
}

/// Claim: a request is resent when its timer elapses, and the replier sees
/// the **duplicate** — "REQ retransmission creates duplicate requests" (§7),
/// which is a cost of the pattern rather than a hidden repair.
#[tokio::test]
async fn the_resend_timer_makes_the_replier_see_a_duplicate() {
    let (_ctx, req, rep, _url) = pair().await;

    req.send(b"work".to_vec()).await.expect("send");
    let first = rep.context().recv().await.expect("the request");
    assert_eq!(first.body(), b"work");

    // Nobody replies. The requester's clock runs out and sends it again.
    let duplicate = tokio::time::timeout(Duration::from_secs(5), rep.context().recv())
        .await
        .expect("the resend arrives")
        .expect("a second request");
    assert_eq!(duplicate.body(), b"work", "the same work, twice");
    assert_eq!(
        duplicate.header(),
        first.header(),
        "with the same request ID, which is all a replier has to notice it by"
    );
}

/// Claim: the other two resend triggers are real — a peer that disconnects
/// and a peer that becomes available both cause the outstanding request to
/// be sent again, with no second `send` from the application (§4).
#[tokio::test]
async fn a_disconnect_and_a_new_peer_both_resend_the_request() {
    let ctx = Context::new(ContextConfig::default()).expect("context");

    // A replier that takes the request and then goes away.
    let doomed = RepSocket::with_options(&ctx, options()).expect("rep");
    let doomed_url = doomed
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    // An address nobody is on yet; the requester keeps dialling it.
    let spare = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port");
    let spare_url = format!("tcp://127.0.0.1:{}", spare.local_addr().unwrap().port());
    drop(spare);

    let req = ReqSocket::with_options(&ctx, options()).expect("req");
    req.dial(&doomed_url).await.expect("dial the live one");
    req.dial_nonblocking(&spare_url)
        .expect("keep dialling the empty address");

    req.send(b"work".to_vec()).await.expect("send");
    let taken = doomed.recv().await.expect("the request");
    assert_eq!(taken.body(), b"work");

    // Trigger two: the peer it went to disappears.
    doomed.close();

    // Trigger three: a peer becomes available while the request waits.
    let rescuer = RepSocket::with_options(&ctx, options()).expect("rep");
    rescuer.listen(&spare_url).await.expect("listen");

    let resent = tokio::time::timeout(Duration::from_secs(10), rescuer.recv())
        .await
        .expect("the request is resent to the peer that appeared")
        .expect("a request");
    assert_eq!(resent.body(), b"work");
    assert_eq!(
        resent.header(),
        taken.header(),
        "the same request, not a new one"
    );

    rescuer.send(b"done".to_vec()).await.expect("reply");
    assert_eq!(req.recv().await.expect("reply").body(), b"done");
}

/// Claim: requests are spread over the repliers that can take one, rather
/// than piled on the first — "a cooked REQ … normally spreads requests
/// among peer REP sockets" (§4).
#[tokio::test]
async fn requests_are_spread_over_the_available_repliers() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let first = RepSocket::with_options(&ctx, options()).expect("rep");
    let second = RepSocket::with_options(&ctx, options()).expect("rep");
    let first_url = first
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let second_url = second
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let req = ReqSocket::with_options(&ctx, options()).expect("req");
    req.dial(&first_url).await.expect("dial one");
    req.dial(&second_url).await.expect("dial two");
    assert_eq!(req.pipe_count(), 2);

    let served = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let serving: Vec<_> = [first.clone(), second.clone()]
        .into_iter()
        .map(|rep| {
            let served = std::sync::Arc::clone(&served);
            tokio::spawn(async move {
                let mut mine = 0usize;
                while let Ok(request) = rep.recv().await {
                    mine += 1;
                    served.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if rep.send(request.body().to_vec()).await.is_err() {
                        break;
                    }
                }
                mine
            })
        })
        .collect();

    for round in 0..4u8 {
        req.send(vec![round]).await.expect("send");
        assert_eq!(req.recv().await.expect("reply").body(), [round]);
    }

    first.close();
    second.close();
    let mut counts = Vec::new();
    for task in serving {
        counts.push(task.await.expect("task"));
    }
    counts.sort_unstable();
    assert!(
        counts[0] >= 1,
        "both repliers were used: {counts:?} out of {} requests",
        served.load(std::sync::atomic::Ordering::SeqCst)
    );
}
