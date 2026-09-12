//! Translating ZeroMQ subscriptions into weida filters.
//!
//! One named loss of `docs/adapters/zmtp.md` §8 lives here, and it is a
//! refusal rather than a silent widening:
//!
//! * **L2/L4 — a byte prefix is not a segmented filter.** ZeroMQ matches bytes
//!   from the start of the message and knows nothing about boundaries;
//!   weida's filter is `.`-segmented with `*` and a trailing `#`
//!   ([0007](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0007-topic-namespace.md)
//!   §4.2). A prefix that ends mid-segment selects a different set than any
//!   filter does, so it is refused — unless the configuration opts into
//!   subscribing at the enclosing boundary and re-applying the byte prefix
//!   locally, which is what §9.3 asks for.
//!
//! **What left this module.** L3 — "subscribing to 'A' and 'A' counts as two
//! subscriptions, and would require two CANCEL commands to undo" — is
//! 37/ZMTP's rule about a *socket*, not about a bridge, and the reference
//! counting that implemented it is now `weida-zmq`'s XPUB
//! ([0013](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0013-competitor-libraries.md)
//! §5.2): that socket delivers a subscription to its application only when
//! the count for a prefix crosses zero, which is exactly the signal this
//! bridge used to compute for itself. The same move took the per-peer prefix
//! ceilings, which are `SocketOptions::max_subscriptions` and
//! `max_subscription_bytes` and are set from [`crate::InboundConfig`].
//!
//! What remains is the translation, plus the small ledger the *weida* side
//! needs: two different ZeroMQ prefixes can map to one weida filter under
//! [`MidSegment::BoundaryAndRefilter`], and cancelling one of them must not
//! unsubscribe the other. That is not ZeroMQ's counting rule — it is a
//! consequence of the mapping being many-to-one, so it belongs to the mapping.

use std::collections::HashMap;

use crate::error::BridgeError;

/// weida's topic separator ([0007] §4.2).
///
/// [0007]: https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0007-topic-namespace.md
const SEPARATOR: char = '.';

/// What to do with a byte prefix that does not end at a segment boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MidSegment {
    /// Refuse the subscription and say why. The default, and the rule at an
    /// adapter edge: a guarantee that cannot be carried is refused rather
    /// than approximated
    /// ([0006](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0006-guarantee-sets.md)
    /// §4.7).
    #[default]
    Refuse,
    /// Subscribe at the enclosing segment boundary and re-apply the byte
    /// prefix locally, before handing a message to the ZeroMQ peer.
    ///
    /// The result is what the peer asked for, at the price of carrying
    /// messages across the network that are then dropped at the bridge. Opt-in
    /// because that price belongs to whoever configured it.
    ///
    /// **The local re-filter is now the socket's own prefix match.** The
    /// bridge subscribes on the weida side at the boundary and publishes the
    /// topic as the message's first frame; `weida-zmq`'s XPUB holds the
    /// prefix the peer actually sent and matches it against that frame, which
    /// is 29/PUBSUB's publisher-side filtering doing the re-filter for free.
    BoundaryAndRefilter,
}

/// One ZeroMQ subscription, translated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Translated {
    /// The weida filter to subscribe with.
    pub filter: String,
    /// The byte prefix the weida filter is wider than, when it is. `None`
    /// when the filter is exactly the subscription. Kept for the doc comment's
    /// sake and for the ledger's arithmetic; the re-filtering itself is the
    /// ZeroMQ socket's prefix match.
    pub refilter: Option<Vec<u8>>,
}

/// Translates a ZeroMQ subscription prefix into a weida filter.
///
/// The mapping of `docs/adapters/zmtp.md` §6:
///
/// * empty prefix → `""`, which matches every topic in both protocols;
/// * a prefix ending at a segment boundary (`px.`) → `px.#`, exactly;
/// * a prefix that is a whole segment sequence with nothing after it
///   (`px.eur`) → ambiguous, because the peer's prefix also matches
///   `px.eurusd`: refused, or `px.#` plus a local re-filter;
/// * a prefix containing `.`-adjacent wildcards (`*`, `#`) → refused (L4):
///   those bytes are literal in a ZeroMQ prefix and are not in a weida filter,
///   so no filter means what the peer asked.
pub(crate) fn translate(prefix: &[u8], mid: MidSegment) -> Result<Translated, BridgeError> {
    if prefix.is_empty() {
        return Ok(Translated {
            filter: String::new(),
            refilter: None,
        });
    }
    let text = std::str::from_utf8(prefix).map_err(|_| {
        BridgeError::Subscription(
            "a weida filter is text and this subscription is not valid UTF-8".into(),
        )
    })?;
    if text.contains('*') || text.contains('#') {
        return Err(BridgeError::Subscription(format!(
            "the prefix {text:?} contains a weida wildcard, which is a literal byte in ZeroMQ \
             and cannot be expressed as a filter (loss L4)"
        )));
    }

    if let Some(head) = text.strip_suffix(SEPARATOR) {
        // Ends exactly at a boundary: `px.` selects `px` and everything under
        // it, which is what `px.#` means.
        if head.is_empty() {
            return Err(BridgeError::Subscription(
                "the prefix \".\" names no segment".into(),
            ));
        }
        return Ok(Translated {
            filter: format!("{head}.#"),
            refilter: None,
        });
    }

    // Does not end at a boundary. `px.eur` matches `px.eurusd` for the peer
    // and would not for any filter, so there is nothing exact to translate to.
    match mid {
        MidSegment::Refuse => Err(BridgeError::Subscription(format!(
            "the prefix {text:?} does not end at a segment boundary, so no weida filter selects \
             the same messages (loss L2); end it with '{SEPARATOR}' or configure \
             boundary-subscribe-plus-local-refilter"
        ))),
        MidSegment::BoundaryAndRefilter => {
            // The enclosing boundary: everything before the last separator,
            // or everything when there is none.
            let filter = match text.rsplit_once(SEPARATOR) {
                Some((head, _)) => format!("{head}.#"),
                None => String::new(),
            };
            Ok(Translated {
                filter,
                refilter: Some(prefix.to_vec()),
            })
        }
    }
}

/// Which weida filters the bridge holds, and how many ZeroMQ prefixes need
/// each.
///
/// **Not ZeroMQ's subscription counting** — that is the socket's, and it is
/// gone from here. This counts the other direction of the same many-to-one
/// mapping: `px.eur` and `px.usd` both translate to `px.#`, so the weida side
/// must be told once and untold once, and a cancellation of one prefix must
/// leave the other's filter in place.
#[derive(Default)]
pub(crate) struct Filters {
    held: HashMap<String, usize>,
}

impl Filters {
    /// Records that `filter` is needed by one more prefix. True when the weida
    /// side has to be told.
    pub(crate) fn hold(&mut self, filter: &str) -> bool {
        let holders = self.held.entry(filter.to_owned()).or_insert(0);
        *holders += 1;
        *holders == 1
    }

    /// Records that one prefix no longer needs `filter`. True when the weida
    /// side has to be untold.
    pub(crate) fn release(&mut self, filter: &str) -> bool {
        match self.held.get_mut(filter) {
            Some(holders) => {
                *holders -= 1;
                if *holders == 0 {
                    self.held.remove(filter);
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter_of(prefix: &str) -> String {
        translate(prefix.as_bytes(), MidSegment::Refuse)
            .expect("translatable")
            .filter
    }

    #[test]
    fn an_empty_subscription_matches_everything_in_both_protocols() {
        let translated = translate(b"", MidSegment::Refuse).expect("empty is legal");
        assert_eq!(translated.filter, "");
        assert_eq!(translated.refilter, None);
    }

    #[test]
    fn a_prefix_that_ends_at_a_boundary_is_exact() {
        assert_eq!(filter_of("px."), "px.#");
        assert_eq!(filter_of("px.eur."), "px.eur.#");
    }

    #[test]
    fn a_prefix_that_ends_mid_segment_is_refused_by_default() {
        // The reason it must be: for the ZeroMQ peer `px.eur` also matches the
        // topic `px.eurusd`, and no weida filter selects that set.
        let err = translate(b"px.eur", MidSegment::Refuse).expect_err("ambiguous");
        assert!(matches!(err, BridgeError::Subscription(_)), "{err:?}");
        assert!(!err.is_fatal(), "a refused subscription is not fatal");
    }

    #[test]
    fn the_opt_in_subscribes_at_the_boundary_and_keeps_the_prefix() {
        let translated = translate(b"px.eur", MidSegment::BoundaryAndRefilter).expect("opted in");
        assert_eq!(translated.filter, "px.#");
        assert_eq!(translated.refilter.as_deref(), Some(&b"px.eur"[..]));

        // A prefix with no separator at all widens to everything.
        let translated = translate(b"px", MidSegment::BoundaryAndRefilter).expect("opted in");
        assert_eq!(translated.filter, "");
        assert_eq!(translated.refilter.as_deref(), Some(&b"px"[..]));
    }

    #[test]
    fn a_prefix_carrying_a_weida_wildcard_is_refused_either_way() {
        for mode in [MidSegment::Refuse, MidSegment::BoundaryAndRefilter] {
            for prefix in [&b"px.*"[..], b"px.#", b"*", b"#"] {
                let err = translate(prefix, mode).expect_err("wildcards are literal in ZeroMQ");
                assert!(matches!(err, BridgeError::Subscription(_)), "{err:?}");
            }
        }
    }

    /// Claim: two prefixes that translate to one filter hold it together, so
    /// cancelling one leaves the other's messages flowing. This is the half of
    /// the old reference counting that belongs to the *mapping* rather than to
    /// ZeroMQ's non-idempotence rule.
    #[test]
    fn two_prefixes_that_share_a_filter_hold_it_together() {
        let mut filters = Filters::default();
        assert!(filters.hold("px.#"), "the first holder subscribes");
        assert!(!filters.hold("px.#"), "the second is already covered");
        assert!(!filters.release("px.#"), "one holder left");
        assert!(filters.release("px.#"), "the last one unsubscribes");
        // And a release of something never held changes nothing.
        assert!(!filters.release("px.#"));
    }
}
