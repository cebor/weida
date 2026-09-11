//! weida driven from an executor that is not Tokio.
//!
//! The runtime owns its reactor ([`Runtime::owned`]), so every task, timer and
//! name lookup the library needs runs there, while the test itself drives the
//! futures under `futures::executor::block_on` and touches the payload through
//! the `futures-io` traits. Nothing here is a `#[tokio::test]`, and the
//! assertion at the top of each test is that no ambient reactor exists.

mod common;

use std::future::Future;
use std::time::Duration;

use common::Certs;
use futures::future::{Either, select};
use futures::io::{AsyncReadExt, AsyncWriteExt};
use weida::{Runtime, RuntimeConfig, TransferMeta};

/// The same ceiling the tokio suites give `within()`; enforced by a watchdog
/// thread here, because a timer would need the reactor this test refuses to
/// have.
const DEADLINE: Duration = Duration::from_secs(10);

/// Runs `body` to completion under a foreign executor, or fails the test.
fn within<F: Future>(body: F) -> F::Output {
    let (tx, rx) = futures::channel::oneshot::channel::<()>();
    std::thread::spawn(move || {
        std::thread::sleep(DEADLINE);
        let _ = tx.send(());
    });
    futures::executor::block_on(async move {
        let deadline = Box::pin(async move {
            let _ = rx.await;
        });
        match select(Box::pin(body), deadline).await {
            Either::Left((out, _)) => out,
            Either::Right(((), _)) => panic!("operation timed out after {DEADLINE:?}"),
        }
    })
}

#[test]
fn a_req_rep_round_trip_runs_without_a_tokio_executor() {
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "this test must run with no ambient tokio runtime"
    );

    let certs = Certs::generate();
    let runtime = Runtime::owned(RuntimeConfig::default()).expect("owned runtime");
    let listener = runtime.listener();
    let replier = listener.replier("/transform").expect("replier");

    within(async {
        let binding = listener
            .bind_quic(
                "127.0.0.1:0".parse().expect("loopback address"),
                certs.server_tls(),
            )
            .await
            .expect("bind");
        let url = format!(
            "weida://127.0.0.1:{}/transform",
            binding.local_addr().port()
        );

        // The uppercasing handler, on the same executor as its client: no
        // spawn, because the caller has nothing to spawn onto.
        let server = async {
            let mut request = replier.accept().await.expect("accept");
            let mut body = request.take_body();
            let mut out = request
                .reply(TransferMeta::default())
                .await
                .expect("open reply");
            let mut received = Vec::new();
            // `futures-io`, not `tokio::io`: this is the trait pair the test
            // exists to prove.
            body.read_to_end(&mut received)
                .await
                .expect("read the request body");
            received.make_ascii_uppercase();
            AsyncWriteExt::write_all(&mut out, &received)
                .await
                .expect("write the reply");
            out.finish().expect("finish the reply");
        };

        let client = async {
            let requester = runtime.requester(certs.client_tls());
            requester.connect(&url).await.expect("connect");
            let (mut transfer, reply) = requester
                .open(TransferMeta::default())
                .await
                .expect("open the exchange");
            AsyncWriteExt::write_all(&mut transfer, b"hello weida")
                .await
                .expect("write the request");
            transfer.finish().expect("finish the request");

            let mut answer = reply.recv().await.expect("reply");
            let mut bytes = Vec::new();
            answer
                .read_to_end(&mut bytes)
                .await
                .expect("read the reply body");
            bytes
        };

        let ((), answer) = futures::future::join(server, client).await;
        assert_eq!(answer, b"HELLO WEIDA");

        runtime.shutdown().await;
    });
}
