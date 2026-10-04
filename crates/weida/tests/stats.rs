//! A dialling endpoint reads the health of its own connections without
//! holding a flow
//! ([0036](../../../docs/decisions/0036-connection-statistics.md)).

mod common;

use std::time::Duration;

use common::{Certs, Restartable, Server};
use tokio::time::timeout;
use weida::{PeerEvent, PeerEvents, ReconnectPolicy, Runtime, RuntimeConfig, TransferMeta};

const DEADLINE: Duration = Duration::from_secs(10);

async fn within<F: Future>(f: F) -> F::Output {
    timeout(DEADLINE, f).await.expect("operation timed out")
}

/// The next event that is not `Retrying`.
async fn next_transition(events: &mut PeerEvents) -> PeerEvent {
    within(async {
        loop {
            match events.recv().await.expect("event stream open") {
                PeerEvent::Retrying { .. } => continue,
                event => return event,
            }
        }
    })
    .await
}

/// Claim: a requester sees its connection's path and traffic with no flow
/// on it, labelled by the URL exactly as dialled, and the record names no
/// address the application did not write.
///
/// The URL says `localhost`; the connection runs to `127.0.0.1`. Neither
/// that address nor `::1` may appear anywhere in the record.
#[tokio::test]
async fn a_requester_reads_its_connection_without_a_flow() {
    let server = Server::start().await;
    let replier = server.listener.replier("/echo").expect("replier");
    let serving = tokio::spawn(async move {
        while let Ok(mut request) = replier.accept().await {
            let body = request
                .take_body()
                .collect(1 << 20)
                .await
                .expect("request body");
            let mut reply = request.reply(TransferMeta::default()).await.expect("reply");
            reply.write_all(&body).await.expect("write reply");
            reply.finish().expect("finish reply");
        }
    });

    let client = server.client_runtime();
    let requester = client.requester(server.trust());
    assert!(
        requester.connection_stats().is_empty(),
        "nothing dialled, nothing to report"
    );

    let url = format!("weida://localhost:{}/echo", server.addr.port());
    within(requester.connect(&url)).await.expect("connect");
    let first = requester.connection_stats();
    assert_eq!(first.len(), 1, "one dialled address, one record");
    let first = &first[0];
    assert_eq!(&*first.url, url, "the label is the URL as given");
    assert_eq!(first.redials, 0, "the first dial is not a redial");
    let before = first.transport.expect("QUIC has a path");
    assert!(before.path.rtt > Duration::ZERO, "{before:?}");
    assert!(before.path.min_rtt > Duration::ZERO, "{before:?}");
    assert!(before.path.min_rtt <= before.path.rtt, "{before:?}");
    assert!(before.path.current_mtu >= 1200, "{before:?}");

    let body = vec![7u8; 64 * 1024];
    let reply = within(requester.request(&body)).await.expect("request");
    let echoed = within(reply.collect(1 << 20)).await.expect("reply body");
    assert_eq!(echoed.len(), body.len());

    let after = requester.connection_stats();
    let after = after[0].transport.expect("still QUIC");
    assert!(
        after.tx.bytes >= before.tx.bytes + body.len() as u64,
        "the request is in tx: {before:?} -> {after:?}"
    );
    assert!(
        after.rx.bytes >= before.rx.bytes + body.len() as u64,
        "the reply is in rx: {before:?} -> {after:?}"
    );
    assert!(after.tx.datagrams > before.tx.datagrams);
    assert!(after.rx.datagrams > before.rx.datagrams);
    assert!(after.path.sent_packets > before.path.sent_packets);

    let text = format!("{:?}", requester.connection_stats());
    for address in ["127.0.0.1", "::1"] {
        assert!(
            !text.contains(address),
            "the record names {address}, which the application never wrote: {text}"
        );
    }

    assert!(requester.disconnect(&url));
    assert!(
        requester.connection_stats().is_empty(),
        "a forgotten address is not reported"
    );

    client.shutdown().await;
    serving.abort();
}

/// Claim: a local connection is reported with its age and without
/// transport numbers, because it has no path to measure.
#[tokio::test]
async fn a_local_connection_has_an_age_and_no_transport() {
    let bus = format!("weida-stats-{}", std::process::id());
    let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
    let listener = server.listener();
    let _binding = listener.bind_inproc(&bus).expect("bind inproc");
    let _puller = listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
    let pusher = client.pusher(weida::Trust::by_address());
    let url = format!("weida+inproc://{bus}/jobs");
    within(pusher.connect(&url)).await.expect("connect");

    let first = pusher.connection_stats();
    assert_eq!(first.len(), 1);
    assert_eq!(&*first[0].url, url);
    assert_eq!(first[0].transport, None, "in process there is no path");
    tokio::time::sleep(Duration::from_millis(20)).await;
    let later = pusher.connection_stats();
    assert!(
        later[0].age >= first[0].age + Duration::from_millis(20),
        "age grows with the connection: {:?} -> {:?}",
        first[0].age,
        later[0].age
    );

    client.shutdown().await;
    server.shutdown().await;
}

/// Claim: a transparent redial is counted on the address, and the age
/// belongs to the connection, so it starts again on the new one. While the
/// address is down it has no live connection to report.
#[tokio::test]
async fn a_redial_is_counted_and_restarts_the_age() {
    let certs = Certs::generate();
    let first = Restartable::start(&certs, "127.0.0.1:0".parse().expect("loopback")).await;
    let addr = first.addr;
    let puller = first.listener.puller("/jobs").expect("puller");

    let client = Runtime::new(RuntimeConfig {
        reconnect: ReconnectPolicy {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(50),
            ..ReconnectPolicy::default()
        },
        ..RuntimeConfig::default()
    })
    .expect("client runtime");
    let pusher = client.pusher(certs.client_tls());
    let mut events = pusher.events();
    let url = format!("weida://127.0.0.1:{}/jobs", addr.port());
    within(pusher.connect(&url)).await.expect("connect");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));

    tokio::time::sleep(Duration::from_millis(300)).await;
    let old = pusher.connection_stats();
    assert_eq!(old[0].redials, 0);
    let old_age = old[0].age;
    assert!(old_age >= Duration::from_millis(300));

    drop(puller);
    first.stop().await;
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Lost { .. }
    ));
    assert!(
        pusher.connection_stats().is_empty(),
        "an address being redialled has no live connection"
    );

    let second = Restartable::start(&certs, addr).await;
    let _puller = second.listener.puller("/jobs").expect("puller");
    assert!(matches!(
        next_transition(&mut events).await,
        PeerEvent::Connected { .. }
    ));
    let new = pusher.connection_stats();
    assert_eq!(new.len(), 1);
    assert_eq!(&*new[0].url, url);
    assert_eq!(new[0].redials, 1, "one successful redial");
    assert!(
        new[0].age < old_age,
        "the new connection's age started again: {:?} after {old_age:?}",
        new[0].age
    );

    client.shutdown().await;
}
