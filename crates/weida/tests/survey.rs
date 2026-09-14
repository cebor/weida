//! SURVEY: one exchange per respondent, and a deadline that is the caller's.
//!
//! The only pattern of the family with a time dimension, which is why it is
//! worth a type: the deadline, the partial result and the late-reply rule are
//! exactly what an application otherwise rebuilds wrongly. Four of the five
//! tests here are about what happens when a respondent does *not* answer the
//! way it should.

mod common;

use std::time::Duration;

use common::Server;
use weida::{Error, ErrorCode, TransferMeta};

/// Generous ceiling: every assertion below should settle in milliseconds.
const DEADLINE: Duration = Duration::from_secs(15);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// Answers every question with `body` until the respondent goes away.
fn answer_with(respondent: weida::Respondent, body: &'static [u8]) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok(mut request) = respondent.accept().await {
            let _ = request.body().read_capped(1024).await;
            let Ok(mut reply) = request.reply(TransferMeta::default()).await else {
                return;
            };
            if reply.write_all(body).await.is_err() {
                return;
            }
            let _ = reply.finish();
        }
    })
}

#[tokio::test]
async fn every_respondent_answers_within_the_deadline() {
    // Two *distinct* respondents behind one identity, so one surveyor with
    // one set of trust anchors reaches both: this is the fan-out the pattern
    // exists for, and Req/Rep's round-robin would have asked exactly one.
    let certs = common::Certs::generate();
    let server = weida::Runtime::new(weida::RuntimeConfig::default()).expect("runtime");
    let mut answering = Vec::new();
    let mut urls = Vec::new();
    let mut bindings = Vec::new();
    for body in [&b"a"[..], &b"b"[..]] {
        let listener = server.listener();
        let binding = listener
            .bind_quic("127.0.0.1:0".parse().expect("loopback"), certs.server_tls())
            .await
            .expect("bind");
        urls.push(format!(
            "weida://127.0.0.1:{}/poll",
            binding.local_addr().port()
        ));
        let respondent = listener.respondent("/poll").expect("respondent");
        answering.push(answer_with(
            respondent,
            if body == b"a" { b"a" } else { b"b" },
        ));
        bindings.push((listener, binding));
    }

    let client = weida::Runtime::new(weida::RuntimeConfig::default()).expect("client runtime");
    let surveyor = client.surveyor(certs.client_tls());
    for url in &urls {
        within(surveyor.connect(url)).await.expect("connect");
    }
    assert_eq!(surveyor.peer_count(), 2);

    let mut run = within(surveyor.survey(b"who is there", Duration::from_secs(5)))
        .await
        .expect("survey");
    assert_eq!(run.respondents(), 2, "one exchange per respondent");

    let mut answers = Vec::new();
    while let Some(answer) = within(run.next(1024)).await {
        answers.push(answer.expect("a reply, not an error"));
    }
    answers.sort();
    assert_eq!(answers, vec![b"a".to_vec(), b"b".to_vec()]);
    // Every respondent answered, so the run ends **before** its deadline
    // rather than waiting it out, and nothing was late.
    assert_eq!(run.late(), 0);

    for task in answering {
        task.abort();
    }
    client.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn a_late_reply_is_counted_and_not_delivered() {
    let server = Server::start().await;
    let respondent = server.listener.respondent("/poll").expect("respondent");

    // Answers, but well after any deadline this survey will use.
    let slow = tokio::spawn(async move {
        let mut request = respondent.accept().await.expect("accept");
        let _ = request.body().read_capped(1024).await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        let Ok(mut reply) = request.reply(TransferMeta::default()).await else {
            return;
        };
        let _ = reply.write_all(b"too late").await;
        let _ = reply.finish();
    });

    let client = server.client_runtime();
    let surveyor = client.surveyor(server.trust());
    within(surveyor.connect(&server.url("/poll")))
        .await
        .expect("connect");

    let mut run = within(surveyor.survey(b"quick", Duration::from_millis(100)))
        .await
        .expect("survey");
    assert_eq!(run.respondents(), 1);
    // The deadline, not an error: a partial result is the point of a survey.
    assert!(
        within(run.next(1024)).await.is_none(),
        "the answer is later than the deadline"
    );

    // The answer arrives, is dropped, and is counted — the way the fan-out's
    // drop counter already works (`docs/GUARANTEES.md` §6).
    slow.await.expect("respondent");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(run.late(), 1, "a late answer is counted, not delivered");

    client.shutdown().await;
}

#[tokio::test]
async fn a_respondent_that_refuses_is_one_error_among_replies() {
    let server = Server::start().await;
    let respondent = server.listener.respondent("/poll").expect("respondent");

    let refusing = tokio::spawn(async move {
        let request = respondent.accept().await.expect("accept");
        request.refuse(ErrorCode::Rejected).await;
    });

    let client = server.client_runtime();
    let surveyor = client.surveyor(server.trust());
    within(surveyor.connect(&server.url("/poll")))
        .await
        .expect("connect");

    let mut run = within(surveyor.survey(b"who is there", Duration::from_secs(5)))
        .await
        .expect("survey");
    let outcome = within(run.next(1024)).await.expect("an outcome");
    let err = outcome.expect_err("the respondent refused");
    assert!(matches!(err, Error::Rejected), "{err:?}");
    // A refusal ends that respondent's exchange, not the survey.
    assert!(within(run.next(1024)).await.is_none());

    refusing.await.expect("respondent");
    client.shutdown().await;
}

#[tokio::test]
async fn a_respondent_that_dies_mid_reply_does_not_end_the_survey() {
    let server = Server::start().await;
    let respondent = server.listener.respondent("/poll").expect("respondent");

    let dying = tokio::spawn(async move {
        let mut request = respondent.accept().await.expect("accept");
        let _ = request.body().read_capped(1024).await;
        let mut reply = request.reply(TransferMeta::default()).await.expect("reply");
        reply.write_all(b"half").await.expect("write");
        // Abandoned mid-payload: the stream is reset, so the surveyor sees a
        // failure rather than a short answer.
        reply.cancel();
    });

    let client = server.client_runtime();
    let surveyor = client.surveyor(server.trust());
    within(surveyor.connect(&server.url("/poll")))
        .await
        .expect("connect");

    let mut run = within(surveyor.survey(b"who is there", Duration::from_secs(5)))
        .await
        .expect("survey");
    let outcome = within(run.next(1024)).await.expect("an outcome");
    assert!(
        outcome.is_err(),
        "an interrupted reply is never a complete answer: {outcome:?}"
    );
    assert!(within(run.next(1024)).await.is_none());

    dying.await.expect("respondent");
    client.shutdown().await;
}

#[tokio::test]
async fn a_survey_with_no_respondents_is_empty_not_an_error() {
    let server = Server::start().await;
    let client = server.client_runtime();
    let surveyor = client.surveyor(server.trust());

    // Nobody connected: "nobody answered" is an answer, so this is an empty
    // run rather than `NotConnected`.
    let mut run = within(surveyor.survey(b"anyone?", Duration::from_secs(5)))
        .await
        .expect("a survey with no respondents is not an error");
    assert_eq!(run.respondents(), 0);
    assert!(within(run.next(1024)).await.is_none());
    assert_eq!(run.late(), 0);

    client.shutdown().await;
}
