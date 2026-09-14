//! The guide's chapter 4, asserted against the chapter's own program.
//!
//! `docs/GUIDE.md` §4 makes five claims and `examples/guide_depth.rs` is the
//! program that demonstrates each — the same discipline `crates/weida/tests/guide.rs`
//! applies to chapters 1 and 2, and the reason this chapter's tests live in
//! *this* crate is the reason the crate exists: the chain needs two foreign
//! protocols, and the two adapter crates know nothing of each other
//! (`docs/ARCHITECTURE.md` §4).
//!
//! The claim **count** is still checked in `crates/weida/tests/guide.rs`,
//! which reads the document and holds one row per written chapter. That split
//! is deliberate: the count is a property of the document and belongs with the
//! document's other checks, while the assertions belong where the
//! dependencies are.
//!
//! What is asserted here is every claim's *mechanism*, never a timing: a chain
//! through two bridges and two foreign libraries has too many schedulers in it
//! for a number to mean anything, and each of these claims is a yes-or-no
//! about what crossed.

use std::time::Duration;

#[path = "../examples/guide_depth.rs"]
#[allow(dead_code)]
mod guide_depth;

use guide_depth::{
    chain_claim, crossing, intersection, refused_at_the_first_hop, smaller_cap_decides,
};
// `Delivery` is the transfer receipt in this library's public surface, so the
// guarantee dimension is re-exported as `DeliveryLevel` — a name collision
// worth knowing about before writing a chapter that quotes both.
use weida::{Acknowledgement, DeliveryLevel};

/// Generous: every program in this file stands up two bridges, two runtimes
/// and two foreign sockets, and one of them waits out a deliberate silence.
const DEADLINE: Duration = Duration::from_secs(120);

async fn within<F: Future>(f: F) -> F::Output {
    tokio::time::timeout(DEADLINE, f)
        .await
        .expect("operation timed out")
}

/// Claim §4.1: a hop is a translation, and what crosses is what both
/// protocols can express.
///
/// The assertion is the **shape** of the far end's body, because that is where
/// the translation is visible: two ZMTP frames became one SP body with a NUL
/// where the frame boundary was. A chain that forwarded the frames as payload,
/// or dropped the topic, or delimited it differently would fail here.
#[tokio::test]
async fn a_hop_translates_and_the_translation_is_visible() {
    let crossed = within(crossing()).await;
    assert_eq!(
        crossed.sp_body, b"sport.football\0goal",
        "the ZMTP topic frame became a weida topic and then SP's leading bytes"
    );
    let (topic, rest) = crossed
        .sp_body
        .split_at(crossed.sp_body.iter().position(|b| *b == 0).expect("a NUL"));
    assert_eq!(topic, crossed.zmtp_topic_frame.as_slice());
    assert_eq!(&rest[1..], crossed.zmtp_payload_frame.as_slice());
}

/// Claim §4.2: the chain's claim is the weakest hop's, and nothing composes
/// upward.
///
/// The pair is the claim: the near edge **accepted** and the far edge received
/// **nothing**. Either half alone proves nothing — a send that fails is not
/// interesting, and a far end that receives is the happy path.
#[tokio::test]
async fn the_chain_claims_no_more_than_its_weakest_hop() {
    let claim = within(chain_claim()).await;
    assert!(
        claim.near_edge_accepted,
        "ZeroMQ's transfer point is `zmq_send` returning, and it returned"
    );
    assert!(
        !claim.far_edge_received,
        "a message accepted at the ZeroMQ edge is not a message delivered at the SP edge"
    );
}

/// Claim §4.3: the smaller ceiling decides, and it decides at the first hop
/// that sees it.
#[tokio::test]
async fn the_smaller_ceiling_decides_at_the_first_hop() {
    let verdict = within(smaller_cap_decides()).await;
    assert!(
        verdict.offered > verdict.near_cap,
        "the payload has to exceed the near ceiling for this to mean anything"
    );
    assert!(
        !verdict.far_edge_received,
        "the oversized payload was refused before the SP edge could hold any of it"
    );
}

/// Claim §4.4: a loss at the first hop is invisible to the sender and total
/// for the chain.
///
/// The SP side could carry those bytes perfectly well; it never sees them.
/// That is the composition rule — a chain's capability is an intersection,
/// not a union — and the sender's successful `send` is the uncomfortable half.
#[tokio::test]
async fn a_loss_at_the_first_hop_is_total_and_silent() {
    let refused = within(refused_at_the_first_hop()).await;
    assert_eq!(refused.frames, 2, "a multipart message needs two frames");
    assert!(
        refused.near_edge_accepted,
        "the ZeroMQ send succeeded, of a message that will never exist anywhere else"
    );
    assert!(
        !refused.far_edge_received,
        "the refusal happened at the ZMTP edge; the SP edge saw nothing to carry"
    );
}

/// Claim §4.5: the intersection is arithmetic a caller can do, and a live
/// connection's set is the one the caller configured.
///
/// Three assertions, and the third is the one that makes the claim useful.
///
/// The **arithmetic** is `core ∩ core = core`, whose delivery and
/// acknowledgement levels are the weakest two in the vocabulary — which is
/// what makes a chain of them `BestEffort` end to end.
///
/// The **direction** needs a second pair, because `core ∩ core` is symmetric
/// and would read the same if the intersection took the stronger level. A hop
/// offering `AtLeastOnce` delivery and `Accepted` completion still agrees on
/// core with a hop that offers neither.
///
/// The **consequence** is why this library needs no accessor for a negotiated
/// set: `RuntimeConfig::guarantees` is offered *and* required, so a live
/// connection's agreed set is the minimum of two offers that each reached the
/// requirement — the configured set exactly. A peer that requires more does
/// not get less; it gets `Error::Negotiation` at connect time. That is
/// asserted here against a real handshake rather than argued.
#[tokio::test]
async fn the_intersection_is_arithmetic_and_a_live_connection_carries_what_you_configured() {
    let intersected = within(intersection()).await;
    assert_eq!(
        intersected.agreed, intersected.core,
        "core intersected with core is core"
    );
    assert_eq!(intersected.agreed.delivery, DeliveryLevel::BestEffort);
    assert_eq!(
        intersected.agreed.acknowledgement,
        Acknowledgement::TransportReceipt
    );

    assert_eq!(
        intersected.stronger.delivery,
        DeliveryLevel::AtLeastOnce,
        "the second set has to be stronger for this to mean anything"
    );
    assert_eq!(
        intersected.agreed_with_stronger, intersected.core,
        "the intersection takes the weaker level per dimension, so the agreed set is core"
    );

    assert!(
        !intersected.stronger_peer_connected,
        "a peer requiring more than its peer offers must be refused, not quietly given less"
    );
    let told = intersected
        .stronger_peer_was_told
        .as_deref()
        .expect("a refused dial reports why");
    // Only that it was told something, not what. The wording depends on which
    // side observes the close first: this dial reports `negotiation failed`
    // when it has read the peer's CONNECTION_CLOSE reason and `connection
    // lost` when the local teardown wins that race — both observed, on the
    // same commit, from the same program. Pinning either would pin a coin
    // flip, which is the lesson chapter 1 §1.3 already paid for.
    assert!(
        !told.is_empty(),
        "the refusal carries a reason, whichever of the two it is"
    );
}
