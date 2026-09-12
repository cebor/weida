//! PUSH and PULL against each other, over real connections.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use weida_nng::{Context, ContextConfig, Error, PullSocket, PushSocket, SocketOptions};
use weida_sp::header::{EndpointType, HEADER_LEN, ProtocolHeader};
use weida_sp::message;

fn options() -> SocketOptions {
    SocketOptions {
        recv_timeout: Some(Duration::from_secs(5)),
        send_timeout: Some(Duration::from_secs(5)),
        handshake_timeout: Duration::from_secs(2),
        reconnect_min: Duration::from_millis(10),
        ..SocketOptions::default()
    }
}

/// Claim: work reaches a worker, and a pusher with several workers spreads
/// it over them rather than piling it on the first (§4).
#[tokio::test]
async fn work_is_spread_over_the_pullers() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let push = PushSocket::with_options(&ctx, options()).expect("push");
    let url = push
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let workers: Vec<PullSocket> = (0..3)
        .map(|_| PullSocket::with_options(&ctx, options()).expect("pull"))
        .collect();
    for worker in &workers {
        worker.dial(&url).await.expect("dial");
    }
    while push.pipe_count() < 3 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let taken = Arc::new(AtomicUsize::new(0));
    let mut serving = Vec::new();
    for worker in workers.clone() {
        let taken = Arc::clone(&taken);
        serving.push(tokio::spawn(async move {
            let mut mine = 0usize;
            while let Ok(message) = worker.recv().await {
                let _ = message;
                mine += 1;
                if taken.fetch_add(1, Ordering::SeqCst) + 1 >= 30 {
                    break;
                }
            }
            mine
        }));
    }

    for job in 0..30u8 {
        push.send(vec![job]).await.expect("send");
    }

    let mut counts = Vec::new();
    for task in serving {
        counts.push(
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .expect("a worker finished")
                .expect("task"),
        );
    }
    let served: usize = counts.iter().sum();
    assert!(served >= 30, "only {served} of 30 jobs were taken");
    counts.sort_unstable();
    assert!(
        counts[0] > 0,
        "one worker got nothing at all: {counts:?} — that is not a rotation"
    );
}

/// Claim: a puller that cannot accept a message is **skipped**, not waited
/// for. A worker that never reads fills up and the rest of the work goes
/// past it to the one that does — "unavailable peers are excluded by flow
/// control" (§4).
#[tokio::test]
async fn a_puller_that_cannot_accept_is_skipped_rather_than_waited_for() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let push = PushSocket::with_options(&ctx, options()).expect("push");
    let url = push
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    // One worker with the shallowest possible queue that never reads, and
    // one that does. The stalled one absorbs its queue and whatever the
    // kernel buffers, and is then unavailable for good.
    let stalled = PullSocket::with_options(
        &ctx,
        SocketOptions {
            recv_depth: Some(1),
            ..options()
        },
    )
    .expect("pull");
    let working = PullSocket::with_options(&ctx, options()).expect("pull");
    stalled.dial(&url).await.expect("dial");
    working.dial(&url).await.expect("dial");
    while push.pipe_count() < 2 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let received = Arc::new(AtomicUsize::new(0));
    let reader = {
        let working = working.clone();
        let received = Arc::clone(&received);
        tokio::spawn(async move {
            while working.recv().await.is_ok() {
                received.fetch_add(1, Ordering::SeqCst);
            }
        })
    };

    // Big enough that the kernel's buffers fill quickly rather than
    // swallowing the whole test.
    let job = vec![0xABu8; 32 * 1024];
    for _ in 0..200 {
        tokio::time::timeout(Duration::from_secs(5), push.send(job.clone()))
            .await
            .expect("a send blocked although a worker was free")
            .expect("send");
    }

    for _ in 0..200 {
        if received.load(Ordering::SeqCst) >= 100 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let got = received.load(Ordering::SeqCst);
    assert!(
        got >= 100,
        "the working puller took only {got} of 200 jobs; the stalled one was not skipped"
    );
    reader.abort();
}

/// Claim: with no puller that can take a message the send waits, and
/// `NNG_OPT_SENDTIMEO` turns that wait into `NNG_ETIMEDOUT` rather than a
/// hang (§4, §5).
#[tokio::test]
async fn a_send_with_no_puller_times_out() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let push = PushSocket::with_options(
        &ctx,
        SocketOptions {
            send_timeout: Some(Duration::from_millis(100)),
            ..options()
        },
    )
    .expect("push");

    let err = push.send(b"nobody".to_vec()).await.unwrap_err();
    assert!(matches!(err, Error::ETIMEDOUT(_)), "{err:?}");

    // And the non-blocking form says the same thing without the wait.
    let err = push.try_send(b"nobody".to_vec()).unwrap_err();
    assert!(matches!(err, Error::ETIMEDOUT(_)), "{err:?}");
}

/// Claim: a puller fair-queues its pushers — messages from two pushers both
/// arrive, with no order promised between them (§4).
#[tokio::test]
async fn a_puller_takes_from_every_pusher() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let pull = PullSocket::with_options(&ctx, options()).expect("pull");
    let url = pull
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let first = PushSocket::with_options(&ctx, options()).expect("push");
    let second = PushSocket::with_options(&ctx, options()).expect("push");
    first.dial(&url).await.expect("dial");
    second.dial(&url).await.expect("dial");

    for _ in 0..5 {
        first.send(b"a".to_vec()).await.expect("send a");
        second.send(b"b".to_vec()).await.expect("send b");
    }

    let mut from_first = 0;
    let mut from_second = 0;
    for _ in 0..10 {
        match pull.recv().await.expect("recv").body() {
            b"a" => from_first += 1,
            b"b" => from_second += 1,
            other => panic!("unexpected body {other:?}"),
        }
    }
    assert_eq!(from_first, 5);
    assert_eq!(from_second, 5);
}

/// Claim: a pusher whose only puller goes away does not lose the pipeline —
/// the pipe is gone, the send waits for another worker, and the message is
/// not silently discarded (§4, §12/P4).
#[tokio::test]
async fn a_send_after_the_last_puller_leaves_waits_rather_than_discarding() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let push = PushSocket::with_options(
        &ctx,
        SocketOptions {
            send_timeout: Some(Duration::from_millis(200)),
            ..options()
        },
    )
    .expect("push");
    let url = push
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let pull = PullSocket::with_options(&ctx, options()).expect("pull");
    pull.dial(&url).await.expect("dial");
    while push.pipe_count() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    push.send(b"first".to_vec()).await.expect("send");
    assert_eq!(pull.recv().await.expect("recv").body(), b"first");

    pull.close();
    for _ in 0..200 {
        if push.pipe_count() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let err = push.send(b"orphan".to_vec()).await.unwrap_err();
    assert!(
        matches!(err, Error::ETIMEDOUT(_)),
        "a PUSH never discards; it waits and then times out: {err:?}"
    );
}

/// Claim: a message that crossed the wire is delivered even when the peer
/// closes immediately behind it.
///
/// NNG holds received messages in the socket's own receive buffer, not in
/// the pipe (§5), so losing the pipe does not lose them. Without that a
/// REP peer that answers and hangs up - which is every short-lived
/// responder - would have its answer thrown away in the race between the
/// last read and the close. The peer is driven by hand because the point
/// is the ordering: the whole message, then the close, with nothing in
/// between that could be mistaken for linger.
#[tokio::test]
async fn a_message_survives_the_sender_closing_right_behind_it() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let pull = PullSocket::with_options(&ctx, options()).expect("pull");
    let url = pull
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let addr = url.trim_start_matches("tcp://").to_owned();

    let mut peer = tokio::net::TcpStream::connect(&addr)
        .await
        .expect("connect");
    peer.write_all(&ProtocolHeader::new(EndpointType::Push).encode())
        .await
        .expect("our header");
    let mut theirs = [0u8; HEADER_LEN];
    peer.read_exact(&mut theirs).await.expect("their header");
    peer.write_all(&message::encode(b"last words"))
        .await
        .expect("the message");
    peer.shutdown().await.expect("close");
    drop(peer);

    let received = pull
        .recv()
        .await
        .expect("the message arrived before the close");
    assert_eq!(received.body(), b"last words");
}

/// Claim: the buffer those survivors live in has a ceiling of its own, and
/// a peer that connects, sends and disconnects in a loop cannot grow it.
///
/// Retiring a pipe frees its slot, so `max_pipes` bounds how many peers
/// are connected at once and not how many have ever been - the buffer is
/// therefore bounded separately, at one pipe's receive depth, and the
/// oldest messages are the ones kept, because they are the ones somebody
/// is already waiting for (`docs/INVARIANTS.md`).
#[tokio::test]
async fn a_peer_that_reconnects_in_a_loop_cannot_grow_the_buffer() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let depth = 4;
    let pull = PullSocket::with_options(
        &ctx,
        SocketOptions {
            recv_depth: Some(depth),
            ..options()
        },
    )
    .expect("pull");
    let url = pull
        .listen("tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    let addr = url.trim_start_matches("tcp://").to_owned();

    // Four times the ceiling, and nobody receives while they arrive.
    let cycles = depth * 4;
    for n in 0..cycles {
        let mut peer = tokio::net::TcpStream::connect(&addr)
            .await
            .expect("connect");
        peer.write_all(&ProtocolHeader::new(EndpointType::Push).encode())
            .await
            .expect("our header");
        let mut theirs = [0u8; HEADER_LEN];
        peer.read_exact(&mut theirs).await.expect("their header");
        peer.write_all(&message::encode(format!("{n}").as_bytes()))
            .await
            .expect("the message");
        peer.shutdown().await.expect("close");
        drop(peer);
        while pull.pipe_count() > 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    let mut held = Vec::new();
    while let Ok(message) = pull.try_recv() {
        held.push(String::from_utf8(message.body().to_vec()).expect("utf8"));
    }
    assert_eq!(
        held.len(),
        depth,
        "the buffer holds one pipe's depth however many pipes have come and gone: {held:?}"
    );
    assert_eq!(
        held,
        vec!["0", "1", "2", "3"],
        "and it is the newest that are refused, not the oldest that are dropped"
    );
}

/// Claim: an operation on a closed socket is `NNG_ECLOSED`, at the close.
///
/// NNG's rule is that a closed socket answers `NNG_ECLOSED`; a receive that
/// instead sat out `NNG_OPT_RECVTIMEO` and then said `NNG_ETIMEDOUT` would
/// be telling the caller the wrong thing about the wrong moment - and with
/// NNG's default of forever, would never answer at all.
#[tokio::test]
async fn a_closed_socket_answers_rather_than_waiting_out_its_timeout() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let pull = PullSocket::with_options(&ctx, options()).expect("pull");
    pull.listen("tcp://127.0.0.1:0").await.expect("listen");
    let push = PushSocket::with_options(&ctx, options()).expect("push");

    pull.close();
    push.close();

    let started = std::time::Instant::now();
    let receiving = pull.recv().await.unwrap_err();
    let sending = push.send(b"nowhere".to_vec()).await.unwrap_err();
    assert!(
        matches!(receiving, Error::ECLOSED(_)),
        "a receive on a closed socket: {receiving:?}"
    );
    assert!(
        matches!(sending, Error::ECLOSED(_)),
        "a send on a closed socket: {sending:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "both answered at the close, not at the timeout"
    );
}

/// Claim: a close ends a receive that is already parked.
#[tokio::test]
async fn closing_a_socket_ends_the_receive_already_waiting_on_it() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let pull = Arc::new(PullSocket::with_options(&ctx, options()).expect("pull"));
    pull.listen("tcp://127.0.0.1:0").await.expect("listen");

    let waiting = tokio::spawn({
        let pull = Arc::clone(&pull);
        async move { pull.recv().await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    pull.close();

    let outcome = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("the parked receive ended at the close")
        .expect("task");
    assert!(matches!(outcome.unwrap_err(), Error::ECLOSED(_)));
}
