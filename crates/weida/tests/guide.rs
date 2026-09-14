//! The guide's claims, asserted against the guide's own programs.
//!
//! `docs/GUIDE.md` §1 makes five claims and `examples/guide_one_transfer.rs`
//! is the program that demonstrates each. This file drives **that** file,
//! included as a module, so what is asserted is the code a reader runs and
//! not a second copy of it written to be testable — the discipline
//! `crates/zmq/weida-zmq/tests/zguide_pirates.rs` already applies to the
//! zguide's recipes.
//!
//! One of the five claims is a **race**, and it is asserted as one: a
//! one-way transfer to a path nobody serves may be reported delivered,
//! because the FIN can be acknowledged before the dispatcher's refusal
//! travels back ([decisions/0005](../../../docs/decisions/0005-refusal-race.md)).
//! Pinning either side of that would be pinning a coin flip; what the test
//! pins is the guarantee that survives it — the answer is never
//! `Indeterminate`, and the same misroute as an exchange is definite.

mod common;

use std::time::Duration;

use weida::Error;

#[path = "../examples/guide_one_transfer.rs"]
#[allow(dead_code)]
mod guide_one_transfer;

use guide_one_transfer::{Outcome, STREAMED, gigabyte, hello, how_far, outcome, receipt_cost};

/// Generous ceiling: the streaming claim moves 64 MiB twice over loopback.
const DEADLINE: Duration = Duration::from_secs(60);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// Claim §1.1: an address and a trust decision are the whole setup.
#[tokio::test]
async fn an_address_and_a_trust_decision_are_the_whole_setup() {
    let answered = within(hello()).await.expect("the exchange");
    assert_eq!(answered.reply, b"hello, world");
    assert!(
        answered.peer_was_named,
        "the address carries the peer's fingerprint, which is what `Trust::by_address` trusts"
    );
}

/// Claim §1.2: the payload never has to exist anywhere, and a cap is a
/// refusal rather than a bigger buffer — on both sides.
#[tokio::test]
async fn the_same_calls_carry_a_payload_no_cap_would_admit() {
    let streamed = within(gigabyte()).await.expect("the transfers");
    assert_eq!(
        streamed.bytes, STREAMED as u64,
        "every byte arrived, through one 64 KiB buffer"
    );
    assert!(
        streamed.collect_refused,
        "`collect` under a 1 MiB cap must refuse a 64 MiB payload rather than grow to it"
    );
    let told = streamed
        .sender_was_told
        .expect("the sender learns of a refused payload rather than succeeding into nothing");
    assert!(
        told.contains("reject") || told.contains("cancel"),
        "the refusal reaches the sender as a refusal: {told}"
    );
}

/// Claim §1.3: the outcome is a value, and which case a sender can be sure
/// of is decided by the pattern.
#[tokio::test]
async fn what_a_sender_can_be_sure_of_is_decided_by_the_pattern() {
    let outcomes = within(outcome()).await.expect("the senders");
    assert_eq!(
        outcomes.push_served,
        Outcome::Delivered,
        "a transfer to a path somebody serves resolves on the peer's transport receipt"
    );

    // The race of 0005, asserted as a race: either answer is correct, and
    // neither is `Indeterminate` — nothing here loses a connection.
    assert!(
        matches!(
            outcomes.push_unserved,
            Outcome::Delivered | Outcome::Refused(_)
        ),
        "a misrouted push is delivered or refused depending on whether the refusal \
         overtakes the acknowledgement, never indeterminate: {:?}",
        outcomes.push_unserved
    );

    match outcomes.request_unserved {
        Outcome::Refused(reason) => assert!(
            reason.contains("endpoint"),
            "an exchange to an unserved path is refused by name: {reason}"
        ),
        other => panic!("an exchange has a reply half, so the refusal is definite: {other:?}"),
    }
}

/// Claim §1.4: "did it arrive" and "how far did the far end get" are two
/// questions, and both are answered separately.
#[tokio::test]
async fn the_receipt_and_the_cursor_are_two_different_facts() {
    let far = within(how_far()).await.expect("the transfer");
    assert!(far.delivered, "the transport receipt resolved");
    assert_eq!(
        far.reported,
        Some(far.sent),
        "the receiving application reported an absolute offset, and it consumed the whole payload"
    );
}

/// Claim §1.5: the receipt is a round trip. What is asserted is that both
/// shapes deliver; the numbers are printed by the example, because a timing
/// assertion on a shared machine pins a load rather than a protocol.
#[tokio::test]
async fn both_send_shapes_deliver_and_one_of_them_waits() {
    let cost = within(receipt_cost(4)).await.expect("both shapes");
    assert_eq!(cost.rounds, 4);
    assert!(
        cost.with > Duration::ZERO && cost.without > Duration::ZERO,
        "both shapes completed and were measured"
    );
}

/// The guide's own rule, checked against the guide: every claim §1 makes is
/// one of the tests above.
///
/// Not a test of prose — a test of the **count**. `docs/GUIDE.md` §1 numbers
/// its claims `§1.1` to `§1.5`, and each is a `pub` entry point in
/// `examples/guide_one_transfer.rs`. A sixth claim added to the document
/// without a program and a test fails here, which is the only mechanism that
/// keeps a guide from drifting into prose nobody runs.
#[test]
fn every_claim_the_chapter_makes_has_a_program_and_a_test() {
    let guide = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../docs/GUIDE.md")
            .canonicalize()
            .expect("the guide is in the tree"),
    )
    .expect("read the guide");

    let claims: Vec<&str> = guide
        .lines()
        .filter(|line| line.starts_with("**Claim §1."))
        .collect();
    assert_eq!(
        claims.len(),
        5,
        "chapter 1 states these claims, and this file asserts five of them: {claims:?}"
    );
    for (n, claim) in claims.iter().enumerate() {
        let expected = format!("**Claim §1.{}", n + 1);
        assert!(
            claim.starts_with(&expected),
            "the claims are numbered in order; expected {expected}, found {claim}"
        );
    }
}

/// The `Error` half of §1.3's vocabulary, without a network: the three
/// answers are distinct in the type system, not only in prose.
#[test]
fn the_three_answers_are_distinguishable_without_a_peer() {
    assert!(Error::UnknownEndpoint.is_definite_failure());
    assert!(Error::Rejected.is_definite_failure());
    assert!(
        !Error::Indeterminate.is_definite_failure(),
        "the whole point of `Indeterminate` is that it is not a failure"
    );
}
