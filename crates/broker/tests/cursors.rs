//! The verdict a producer gets without an exchange.
//!
//! This file is the end-to-end proof of
//! [0024](../../../docs/decisions/0024-three-families-one-back-channel.md)
//! §4.4a from the broker's side: a Push producer on **one unidirectional
//! stream** orders `Accepted` and receives it at the admitted body length, so
//! a reliable verdict costs it neither a reply half nor a round trip.
//!
//! Three negatives matter as much as that: a refusal reports nothing, a level
//! this broker cannot reach is absent rather than failed, and an exchange's
//! confirm is exactly the DATA key `8` reply it always was.

use std::time::Duration;

use weida::{
    Acknowledgement, Binding, ClientTls, CursorLevel, Identity, Listener, Pusher, Requester,
    Runtime, RuntimeConfig, ServerTls, TransferMeta, Trust,
};
use weida_broker::{Broker, BrokerConfig, PER_MESSAGE_OVERHEAD};

/// Generous ceiling: every assertion below settles in milliseconds.
const DEADLINE: Duration = Duration::from_secs(10);

const ACCEPTED: CursorLevel = CursorLevel::Known(Acknowledgement::Accepted);
const STORED: CursorLevel = CursorLevel::Known(Acknowledgement::Stored);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// A broker on a loopback port, and what a producer needs to reach it.
struct Harness {
    runtime: Runtime,
    /// Held: dropping the listener stops serving.
    _listener: Listener,
    /// Held: dropping the binding closes the port.
    _binding: Binding,
    broker: Broker,
    client: ClientTls,
    port: u16,
}

impl Harness {
    async fn start(config: BrokerConfig) -> Harness {
        let runtime = Runtime::new(RuntimeConfig::default()).expect("runtime");
        let listener = runtime.listener();
        let identity = Identity::generate().expect("identity");
        let fingerprint = identity.fingerprint().expect("fingerprint");
        let binding = listener
            .bind_quic(
                "127.0.0.1:0".parse().expect("loopback address"),
                ServerTls::new(identity),
            )
            .await
            .expect("bind");
        let port = binding.local_addr().port();
        let broker = Broker::new(&listener, config).expect("broker");
        let serving = broker.clone();
        tokio::spawn(async move { serving.serve().await });
        Harness {
            runtime,
            _listener: listener,
            _binding: binding,
            broker,
            client: ClientTls::new(Trust::pin(fingerprint)),
            port,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("weida://127.0.0.1:{}{}", self.port, path)
    }

    async fn pusher(&self, path: &str) -> Pusher {
        let pusher = self.runtime.pusher(self.client.clone());
        pusher.connect(&self.url(path)).await.expect("connect");
        pusher
    }

    async fn requester(&self, path: &str) -> Requester {
        let requester = self.runtime.requester(self.client.clone());
        requester.connect(&self.url(path)).await.expect("connect");
        requester
    }
}

#[tokio::test]
async fn a_one_way_producer_that_orders_accepted_gets_it() {
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.pusher("/jobs").await;

    let body = b"work that wants a verdict";
    let mut transfer = within(producer.open(TransferMeta::default().with_report([ACCEPTED])))
        .await
        .expect("open");
    let mut cursors = transfer.cursors().expect("the transfer ordered a report");
    within(transfer.write_all(body)).await.expect("write");
    let delivery = transfer.finish().expect("finish");
    within(delivery.delivered()).await.expect("delivered");

    // One unidirectional payload stream, no exchange, and a verdict anyway.
    let set = within(cursors.changed()).await.expect("a cursor arrived");
    assert_eq!(
        set.offset(ACCEPTED),
        Some(body.len() as u64),
        "the broker reports `Accepted` at the admitted body length"
    );
    assert_eq!(set.len(), 1, "one verdict, not a progress series: {set:?}");

    let stats = harness.broker.stats("/jobs").expect("the queue exists");
    assert_eq!(stats.messages, 1);
    assert_eq!(stats.bytes, PER_MESSAGE_OVERHEAD + body.len());
}

#[tokio::test]
async fn an_exchange_still_gets_the_key_8_confirm_and_no_cursor_stream() {
    // The reply half stays the application's answer: a producer that orders
    // nothing sees exactly the confirm it saw before cursors existed.
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.requester("/jobs").await;

    let reply = within(producer.request(b"work")).await.expect("accepted");
    assert_eq!(reply.meta().achieved, Some(Acknowledgement::Accepted));
    // Nothing was ordered, so nothing reports: the confirm is the whole answer.
    assert!(reply.meta().report.is_empty());
    assert_eq!(reply.meta().report_id, None);
    assert!(reply.reporter().is_none());
}

#[tokio::test]
async fn a_refused_admission_reports_no_cursor() {
    // Room for exactly one 64-byte message, then the queue is at its bound.
    let config = BrokerConfig {
        queue_bytes: PER_MESSAGE_OVERHEAD + 64,
        ..BrokerConfig::with_queues(["/jobs"])
    };
    let harness = Harness::start(config).await;
    let producer = harness.pusher("/jobs").await;

    let first = [b'a'; 64];
    let mut transfer = within(producer.open(TransferMeta::default().with_report([ACCEPTED])))
        .await
        .expect("open");
    let mut cursors = transfer.cursors().expect("cursors");
    within(transfer.write_all(&first)).await.expect("write");
    transfer.finish().expect("finish");
    assert_eq!(
        within(cursors.changed())
            .await
            .expect("the first is admitted")
            .offset(ACCEPTED),
        Some(64)
    );

    // The second does not fit. A refusal on a unidirectional stream is
    // `STOP_SENDING(REJECTED)` and **no cursor**: the producer learns from the
    // refusal, not from a missing record.
    let mut refused = within(producer.open(TransferMeta::default().with_report([ACCEPTED])))
        .await
        .expect("open");
    let mut refused_cursors = refused.cursors().expect("cursors");
    let _ = refused.write_all(&[b'b'; 64]).await;
    let _ = refused.finish();

    assert!(
        tokio::time::timeout(Duration::from_millis(500), refused_cursors.changed())
            .await
            .is_err(),
        "a refused admission must report nothing"
    );
    assert!(refused_cursors.snapshot().is_empty());

    let stats = harness.broker.stats("/jobs").expect("the queue exists");
    assert_eq!(stats.messages, 1, "the second message was refused");
}

#[tokio::test]
async fn a_stored_order_is_not_reported_by_a_broker_with_no_store() {
    // The honest-absence rule: this broker holds messages in memory, so
    // `Stored` is a claim it must not make (`docs/GUARANTEES.md` §1). It
    // reports nothing rather than failing the transfer or reporting a level it
    // cannot reach.
    let harness = Harness::start(BrokerConfig::with_queues(["/jobs"])).await;
    let producer = harness.pusher("/jobs").await;

    let mut transfer = within(producer.open(TransferMeta::default().with_report([STORED])))
        .await
        .expect("open");
    let mut cursors = transfer.cursors().expect("cursors");
    within(transfer.write_all(b"durable?"))
        .await
        .expect("write");
    let delivery = transfer.finish().expect("finish");
    within(delivery.delivered()).await.expect("delivered");

    assert!(
        tokio::time::timeout(Duration::from_millis(500), cursors.changed())
            .await
            .is_err(),
        "a level this broker cannot reach is absent, not reported"
    );
    assert_eq!(cursors.snapshot().offset(STORED), None);

    // And the message is in the queue: the unreportable level cost the
    // transfer nothing.
    let stats = harness.broker.stats("/jobs").expect("the queue exists");
    assert_eq!(stats.messages, 1);
}
