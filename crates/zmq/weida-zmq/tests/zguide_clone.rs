//! The zguide's chapter 5 state and pub-sub recipes - Clone (12/CHP), Binary
//! Star and the two proxy recipes - driven and **asserted against the RFC's
//! and the guide's own claims**, 0013 §4.7 clause 5.
//!
//! The examples are included as modules, so the code under test is the file a
//! reader runs.
//!
//! The claims, each named on its test:
//!
//! * CHP: `ICANHAZ?` is "answered by zero or more `KVSYNC` and then
//!   `KTHXBAI` carrying the highest sequence", and the client that subscribed
//!   first "discards those at or below the snapshot sequence" - the
//!   strict-increment rule, set up so that every raced update *must* be
//!   discarded rather than left to a race between two publishers.
//! * CHP: the server "centralizes every change and imposes one sequence in
//!   arrival order"; an update newer than the snapshot is applied; "an empty
//!   value meaning delete"; and `HUGZ` is a beat and not state.
//! * Binary Star: "at most one active server"; the three-way rule, "server
//!   will not become active until it receives application connection requests
//!   **and** cannot see peer", asserted in both halves; and "two actives
//!   would mean split-brain", which is the one case a peer can detect.
//! * Espresso: "all bridged traffic, control and data, is observable" - the
//!   subscriptions, the data and the unsubscriptions, in order.
//! * Last Value Caching: "immediate catch-up to the cached last value per
//!   subscribed topic, **not** the full stream".

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use weida_zmq::{Context, ContextConfig, Message, Multipart, PubSocket, Result, SubSocket};

#[path = "../examples/binary_star.rs"]
#[allow(dead_code)]
mod binary_star;
#[path = "../examples/clone.rs"]
#[allow(dead_code)]
mod clone;
#[path = "../examples/espresso.rs"]
#[allow(dead_code)]
mod espresso;
#[path = "../examples/last_value_cache.rs"]
#[allow(dead_code)]
mod last_value_cache;

use binary_star::{BinaryStar, Event, State, Verdict, bind_state_pub, start_star};
use clone::{CloneClient, clone_server};
use espresso::{espresso, render};
use last_value_cache::last_value_cache;

/// Fast enough that a test is quick, slow enough that a loaded machine is not
/// a failure.
const BEAT: Duration = Duration::from_millis(100);
/// A message this library cannot deliver in this long is a bug.
const PATIENCE: Duration = Duration::from_secs(5);
/// Long enough that "nothing more arrived" means it.
const QUIET: Duration = Duration::from_millis(300);

fn context() -> Result<Context> {
    Context::new(ContextConfig::default())
}

/// Claim, 12/CHP: **the snapshot dialog carries the whole state and the
/// sequence it was taken at**, and **every update that raced the snapshot is
/// discarded by the strict-increment rule.**
///
/// Deterministic by construction, not by racing: the reader subscribes and
/// waits for a `HUGZ`, so its subscription is live *before* any update is
/// written. Every one of the three updates therefore reaches its SUB queue,
/// and every one carries a sequence at or below the snapshot's - so the
/// discard count is exactly three, and a rule that applied them instead would
/// not merely be slow to notice, it would fail here.
#[tokio::test]
async fn a_snapshot_carries_the_state_and_the_raced_updates_are_discarded() -> Result<()> {
    let context = context()?;
    let server = clone_server(&context, BEAT).await?;
    let mut writer = CloneClient::connect(&context, &server.ports, "/client/")?;
    let mut reader = CloneClient::connect(&context, &server.ports, "/client/")?;
    //  The subscription is live once a beat has arrived over it.
    reader.wait_for_beat(PATIENCE).await?;

    for (key, value) in [("/client/a", "1"), ("/client/b", "2"), ("/client/a", "3")] {
        writer.set(key, value.as_bytes(), PATIENCE).await?;
    }
    //  Wait for the server to have numbered all three before asking, so the
    //  snapshot is the one that contains them.
    until(|| server.sequence.load(Ordering::Relaxed) == 3).await;

    reader.snapshot(PATIENCE).await?;
    assert_eq!(reader.sequence, 3, "KTHXBAI carried the highest sequence");
    assert_eq!(reader.map.len(), 2, "two keys, one of them written twice");
    assert_eq!(
        reader.map.get("/client/a").map(Vec::as_slice),
        Some(b"3".as_slice()),
        "the snapshot has the later value"
    );

    //  The three updates are sitting in the SUB queue, all at or below the
    //  snapshot sequence.
    until_async(
        |reader: &mut CloneClient| reader.discarded == 3,
        &mut reader,
    )
    .await;
    reader.apply_pending(QUIET).await?;
    assert_eq!(
        reader.discarded, 3,
        "every update that raced the snapshot was discarded"
    );
    assert_eq!(reader.sequence, 3, "and none of them moved the sequence");
    Ok(())
}

/// Claim, 12/CHP: **an update newer than the snapshot is applied, an empty
/// value deletes, and `HUGZ` is a beat rather than state.** The other side of
/// the discard rule: strictly greater is the test, so the first update after
/// the snapshot must land.
#[tokio::test]
async fn an_update_after_the_snapshot_is_applied_and_an_empty_value_deletes() -> Result<()> {
    let context = context()?;
    let server = clone_server(&context, BEAT).await?;
    let mut writer = CloneClient::connect(&context, &server.ports, "/client/")?;
    let mut reader = CloneClient::connect(&context, &server.ports, "/client/")?;
    reader.wait_for_beat(PATIENCE).await?;
    writer.set("/client/a", b"1", PATIENCE).await?;
    until(|| server.sequence.load(Ordering::Relaxed) == 1).await;
    reader.snapshot(PATIENCE).await?;
    reader.apply_pending(QUIET).await?;
    let beats_after_snapshot = reader.beats;

    //  Newer than the snapshot: applied.
    writer.set("/client/b", b"2", PATIENCE).await?;
    until_async(
        |reader: &mut CloneClient| reader.map.contains_key("/client/b"),
        &mut reader,
    )
    .await;
    assert_eq!(reader.sequence, 2, "the sequence moved with it");

    //  "an empty value meaning delete"
    writer.set("/client/a", b"", PATIENCE).await?;
    until_async(
        |reader: &mut CloneClient| !reader.map.contains_key("/client/a"),
        &mut reader,
    )
    .await;
    assert_eq!(
        reader.map.keys().collect::<Vec<_>>(),
        vec!["/client/b"],
        "the delete removed the key rather than storing an empty value"
    );

    //  And the beats kept coming without touching the map.
    let keys_before = reader.map.len();
    reader.apply_pending(BEAT * 3).await?;
    assert!(
        reader.beats > beats_after_snapshot,
        "HUGZ kept arriving: {} then {}",
        beats_after_snapshot,
        reader.beats
    );
    assert_eq!(reader.map.len(), keys_before, "and changed no state");
    Ok(())
}

/// Claim, Binary Star: **at most one active server.** A primary and a backup
/// that can see each other settle into exactly one active and one passive,
/// and the passive refuses client requests - "the passive does no work".
#[tokio::test]
async fn a_pair_settles_on_exactly_one_active() -> Result<()> {
    let context = context()?;
    let (primary_pub, primary_endpoint) = bind_state_pub(&context).await?;
    let (backup_pub, backup_endpoint) = bind_state_pub(&context).await?;
    let primary = start_star(
        &context,
        State::Primary,
        primary_pub,
        &backup_endpoint,
        BEAT,
    )?;
    let backup = start_star(&context, State::Backup, backup_pub, &primary_endpoint, BEAT)?;

    until(|| primary.state() == State::Active && backup.state() == State::Passive).await;
    //  Both halves of "at most one": the active serves, the passive refuses.
    assert_eq!(primary.client_request()?, Verdict::Serve, "the active one");
    assert_eq!(backup.client_request()?, Verdict::Reject, "the passive one");
    assert_eq!(
        backup.rejected.load(Ordering::Relaxed),
        1,
        "and the refusal is counted, which is the client's cue to try the other address"
    );
    Ok(())
}

/// Claim, Binary Star: **the three-way rule** - "server will not become
/// active until it receives application connection requests **and** cannot
/// see peer". Both halves, and the third case the C asserts: a server that
/// has never seen its peer has no evidence of silence and stays put.
#[test]
fn a_passive_server_needs_both_the_silence_and_the_vote() {
    let now = Instant::now();
    //  Never met the peer: a request is not a vote, however long ago the
    //  server started - `bstar.c` asserts `peer_expiry > 0` here.
    let fresh = BinaryStar::new(State::Backup, BEAT);
    assert_eq!(
        fresh.classify(now + BEAT * 100),
        Event::ClientRequest,
        "no peer ever seen is not evidence of a dead peer"
    );

    //  Peer visible: a request is a request, and a passive server refuses it.
    let mut passive = BinaryStar::new(State::Backup, BEAT);
    passive.peer_seen(now);
    assert_eq!(
        passive.event(Event::Peer(State::Active)).unwrap(),
        Verdict::Noted
    );
    assert_eq!(passive.state(), State::Passive);
    assert_eq!(passive.classify(now + BEAT), Event::ClientRequest);
    assert_eq!(
        passive.event(Event::ClientRequest).unwrap(),
        Verdict::Reject,
        "the peer is doing the work"
    );
    assert_eq!(passive.state(), State::Passive, "and nothing changed");

    //  Two heartbeats of silence: the same request is now a vote, and the
    //  vote is what promotes.
    assert_eq!(
        passive.classify(now + BEAT * 2),
        Event::ClientVote,
        "two heartbeats of silence make a request a vote"
    );
    assert_eq!(passive.event(Event::ClientVote).unwrap(), Verdict::Serve);
    assert_eq!(passive.state(), State::Active, "and only then it is active");
}

/// Claim, Binary Star: **"two actives would mean split-brain"** - the one
/// case a peer can detect from the inside, reported as `EFSM` rather than
/// swallowed.
#[test]
fn an_active_server_told_its_peer_is_active_reports_a_split_brain() {
    let mut star = BinaryStar::new(State::Primary, BEAT);
    star.event(Event::Peer(State::Backup))
        .expect("a backup peer");
    assert_eq!(star.state(), State::Active);
    let error = star
        .event(Event::Peer(State::Active))
        .expect_err("a second active is fatal");
    assert_eq!(error.errno(), "EFSM", "{error:?}");
    assert!(
        error.cause().contains("dual actives"),
        "the C's own words: {error:?}"
    );
}

/// Claim, Binary Star: **failover happens "reliably when needed, and only
/// when needed"** - a live pair where the primary vanishes. Silence alone
/// leaves the backup passive for as long as nobody asks; the first client
/// request after it promotes it.
#[tokio::test]
async fn a_backup_fails_over_only_when_a_client_asks() -> Result<()> {
    let context = context()?;
    let (primary_pub, primary_endpoint) = bind_state_pub(&context).await?;
    let (backup_pub, backup_endpoint) = bind_state_pub(&context).await?;
    let primary = start_star(
        &context,
        State::Primary,
        primary_pub,
        &backup_endpoint,
        BEAT,
    )?;
    let backup = start_star(&context, State::Backup, backup_pub, &primary_endpoint, BEAT)?;
    until(|| backup.state() == State::Passive).await;

    //  The active server disappears without a word.
    primary.task.abort();
    tokio::time::sleep(BEAT * 6).await;
    assert_eq!(
        backup.state(),
        State::Passive,
        "silence alone does not promote: nobody asked for the service"
    );

    //  Now a client asks, and the silence makes it a vote.
    assert_eq!(backup.client_request()?, Verdict::Serve, "the vote carried");
    assert_eq!(backup.state(), State::Active);
    Ok(())
}

/// Claim, Espresso: **all bridged traffic, control and data, is observable**
/// on the capture socket - the subscriptions, the message, and the
/// unsubscriptions, which is exactly the trace the guide prints.
#[tokio::test]
async fn the_capture_socket_shows_subscriptions_data_and_unsubscriptions() -> Result<()> {
    let context = context()?;
    let mut publisher = PubSocket::new(&context)?;
    let publisher_endpoint = publisher.bind("tcp://127.0.0.1:0").await?.to_string();
    let mut trace = espresso(&context, &publisher_endpoint).await?;

    let mut subscriber = SubSocket::new(&context)?;
    subscriber.connect(&trace.subscriber_endpoint)?;
    subscriber.subscribe("A")?;
    subscriber.subscribe("B")?;
    let message = Multipart::new(vec![
        Message::from(b"A".to_vec()),
        Message::from(b"hello".to_vec()),
    ])
    .expect("two frames");
    //  Published until it crosses, which is the slow joiner and not a fault.
    let deadline = Instant::now() + PATIENCE;
    while publisher.publish(message.clone()).delivered == 0 {
        assert!(Instant::now() < deadline, "the subscription never crossed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    subscriber.recv_timeout(PATIENCE).await?;
    subscriber.unsubscribe("A")?;
    subscriber.unsubscribe("B")?;

    let mut captured = Vec::new();
    while let Ok(message) = trace.capture.recv_timeout(QUIET).await {
        captured.push(render(&message));
    }
    captured.sort();
    assert_eq!(
        captured,
        vec!["00A", "00B", "01A", "01B", "A hello"],
        "control frames both ways and the data, all of it"
    );
    Ok(())
}

/// Claim, Last Value Caching: **immediate catch-up to the cached last value
/// per subscribed topic, not the full stream.** Three things at once: the
/// late subscriber gets a message with nothing published after it arrived, it
/// is the *last* value for that topic rather than the first, and it is only
/// the topic it subscribed to.
#[tokio::test]
async fn a_late_subscriber_catches_up_to_the_last_value_and_no_more() -> Result<()> {
    let context = context()?;
    let mut publisher = PubSocket::new(&context)?;
    let publisher_endpoint = publisher.bind("tcp://127.0.0.1:0").await?.to_string();
    let cache = last_value_cache(&context, &publisher_endpoint).await?;

    //  Somebody has to be subscribed for anything to cross the cache: a
    //  publisher with no subscriber downstream delivers nothing at all.
    let mut early = SubSocket::new(&context)?;
    early.connect(&cache.subscriber_endpoint)?;
    early.subscribe("A")?;
    early.subscribe("B")?;
    for (topic, body) in [("A", "one"), ("B", "one"), ("A", "two")] {
        let message = Multipart::new(vec![
            Message::from(topic.as_bytes().to_vec()),
            Message::from(body.as_bytes().to_vec()),
        ])
        .expect("two frames");
        let deadline = Instant::now() + PATIENCE;
        while publisher.publish(message.clone()).delivered == 0 {
            assert!(Instant::now() < deadline, "the subscription never crossed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        early.recv_timeout(PATIENCE).await?;
    }

    //  A new subscriber, and **nothing published after it**: without the
    //  cache it would wait for the next update, which is the 500 seconds the
    //  guide complains about.
    let mut late = SubSocket::new(&context)?;
    late.connect(&cache.subscriber_endpoint)?;
    late.subscribe("A")?;
    let caught_up = late.recv_timeout(PATIENCE).await?;
    assert_eq!(
        (
            caught_up.frames()[0].as_slice(),
            caught_up.frames()[1].as_slice()
        ),
        (b"A".as_slice(), b"two".as_slice()),
        "the last value for A, not the first"
    );
    //  "not the full stream": one message, and nothing for a topic this
    //  subscriber never asked about.
    assert!(
        late.recv_timeout(QUIET).await.is_err(),
        "the cache replayed one message per subscribed topic and stopped"
    );
    assert_eq!(
        cache.served_from_cache.load(Ordering::Relaxed),
        1,
        "one cached topic matched the subscription"
    );
    Ok(())
}

/// Waits for a condition, failing the test rather than hanging the suite.
async fn until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while !ready() {
        assert!(Instant::now() < deadline, "the condition never held");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The same, for a condition that needs the client to read its socket first.
async fn until_async(
    mut ready: impl FnMut(&mut CloneClient) -> bool,
    client: &mut CloneClient,
) -> () {
    let deadline = Instant::now() + PATIENCE;
    while !ready(client) {
        assert!(Instant::now() < deadline, "the condition never held");
        client
            .apply_pending(Duration::from_millis(50))
            .await
            .expect("reading the update stream");
    }
}
