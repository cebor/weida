//! Translating ZeroMQ subscriptions into weida filters, and counting them.
//!
//! Two named losses of `docs/adapters/zmtp.md` §8 live here, and both are
//! refusals rather than silent widenings:
//!
//! * **L3 — subscriptions are not idempotent in ZeroMQ.** "Subscribing to 'A'
//!   and 'A' counts as two subscriptions, and would require two CANCEL
//!   commands to undo", while weida's SUBSCRIBE for a filter already held is
//!   idempotent. So this module reference-counts: the weida side is told only
//!   when a count crosses zero.
//! * **L2/L4 — a byte prefix is not a segmented filter.** ZeroMQ matches bytes
//!   from the start of the message and knows nothing about boundaries;
//!   weida's filter is `.`-segmented with `*` and a trailing `#`
//!   ([0007](https://github.com/tuco86/weida/blob/main/docs/decisions/0007-topic-namespace.md)
//!   §4.2). A prefix that ends mid-segment selects a different set than any
//!   filter does, so it is refused — unless the configuration opts into
//!   subscribing at the enclosing boundary and re-applying the byte prefix
//!   locally, which is what §9.3 asks for.

use std::collections::HashMap;

use crate::error::BridgeError;

/// weida's topic separator ([0007] §4.2).
///
/// [0007]: https://github.com/tuco86/weida/blob/main/docs/decisions/0007-topic-namespace.md
const SEPARATOR: char = '.';

/// What to do with a byte prefix that does not end at a segment boundary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MidSegment {
    /// Refuse the subscription and say why. The default, and the rule at an
    /// adapter edge: a guarantee that cannot be carried is refused rather
    /// than approximated
    /// ([0006](https://github.com/tuco86/weida/blob/main/docs/decisions/0006-guarantee-sets.md)
    /// §4.7).
    #[default]
    Refuse,
    /// Subscribe at the enclosing segment boundary and re-apply the byte
    /// prefix locally, before handing a message to the ZeroMQ peer.
    ///
    /// The result is what the peer asked for, at the price of carrying
    /// messages across the network that are then dropped at the bridge. Opt-in
    /// because that price belongs to whoever configured it.
    BoundaryAndRefilter,
}

/// One ZeroMQ subscription, translated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Translated {
    /// The weida filter to subscribe with.
    pub filter: String,
    /// The byte prefix to re-apply locally, when the filter is wider than the
    /// subscription. `None` when the filter is exactly the subscription.
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

/// The subscriptions of one ZeroMQ peer, reference-counted the way ZeroMQ
/// counts them and deduplicated the way weida needs them.
#[derive(Default)]
pub(crate) struct Subscriptions {
    /// Raw prefixes and how many times the peer subscribed to each: ZeroMQ's
    /// own non-idempotent count (L3).
    raw: HashMap<Vec<u8>, usize>,
    /// weida filters and how many *distinct* raw prefixes translated to each.
    /// Two prefixes can share a filter under `BoundaryAndRefilter`, and
    /// cancelling one must not unsubscribe the other.
    filters: HashMap<String, usize>,
    /// Live re-filters, for the messages a wider filter brings in.
    refilters: Vec<Vec<u8>>,
}

/// What the weida side must be told after a subscription change.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Change {
    /// Subscribe with this filter: it is newly needed.
    Subscribe(String),
    /// Unsubscribe: nothing maps to it any more.
    Unsubscribe(String),
    /// Nothing to do — the count changed but the filter set did not, which is
    /// the whole reason L3 needs counting.
    None,
}

impl Subscriptions {
    /// Records a `SUBSCRIBE`.
    pub(crate) fn subscribe(
        &mut self,
        prefix: &[u8],
        mid: MidSegment,
    ) -> Result<Change, BridgeError> {
        let translated = translate(prefix, mid)?;
        let count = self.raw.entry(prefix.to_vec()).or_insert(0);
        *count += 1;
        if *count > 1 {
            // The same prefix again: ZeroMQ counts it, weida is not told.
            return Ok(Change::None);
        }
        if let Some(refilter) = translated.refilter {
            self.refilters.push(refilter);
        }
        let holders = self.filters.entry(translated.filter.clone()).or_insert(0);
        *holders += 1;
        if *holders == 1 {
            Ok(Change::Subscribe(translated.filter))
        } else {
            Ok(Change::None)
        }
    }

    /// Records a `CANCEL`. An unknown prefix is ignored, as an unsubscribe of
    /// something never subscribed always is.
    pub(crate) fn cancel(&mut self, prefix: &[u8], mid: MidSegment) -> Change {
        let Some(count) = self.raw.get_mut(prefix) else {
            return Change::None;
        };
        *count -= 1;
        if *count > 0 {
            // "Two subscriptions would require two CANCEL commands to undo."
            return Change::None;
        }
        self.raw.remove(prefix);
        // The translation is deterministic, so it can be recomputed rather
        // than stored twice; a prefix that was accepted translates again.
        let Ok(translated) = translate(prefix, mid) else {
            return Change::None;
        };
        if let Some(refilter) = translated.refilter
            && let Some(at) = self.refilters.iter().position(|live| *live == refilter)
        {
            self.refilters.remove(at);
        }
        match self.filters.get_mut(&translated.filter) {
            Some(holders) => {
                *holders -= 1;
                if *holders == 0 {
                    self.filters.remove(&translated.filter);
                    Change::Unsubscribe(translated.filter)
                } else {
                    Change::None
                }
            }
            None => Change::None,
        }
    }

    /// Does this peer want a message on `topic`?
    ///
    /// True unless a re-filter is live and none of them matches: the weida
    /// filter was widened to a segment boundary, so the byte prefix the peer
    /// actually asked for is applied here instead (L2).
    pub(crate) fn wants(&self, topic: &str) -> bool {
        if self.refilters.is_empty() {
            return true;
        }
        // A peer may hold both exact and widened subscriptions. An exact one
        // is already enforced by the weida filter, so a message that arrived
        // for it must not be dropped by somebody else's re-filter: the test
        // is whether *any* live prefix matches, or whether the peer holds a
        // subscription that needed no re-filter at all.
        if self.filters.len() > self.refilters.len() {
            return true;
        }
        self.refilters
            .iter()
            .any(|prefix| topic.as_bytes().starts_with(prefix))
    }

    /// Filters currently held on the weida side.
    #[cfg(test)]
    pub(crate) fn filters(&self) -> impl Iterator<Item = &str> {
        self.filters.keys().map(String::as_str)
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

    #[test]
    fn two_subscribes_need_two_cancels() {
        let mut subs = Subscriptions::default();
        assert_eq!(
            subs.subscribe(b"px.", MidSegment::Refuse).expect("first"),
            Change::Subscribe("px.#".into())
        );
        // ZeroMQ counts the repeat; weida is told nothing, because its
        // SUBSCRIBE is idempotent and an UNSUBSCRIBE later must not undo both.
        assert_eq!(
            subs.subscribe(b"px.", MidSegment::Refuse).expect("second"),
            Change::None
        );
        assert_eq!(subs.cancel(b"px.", MidSegment::Refuse), Change::None);
        assert_eq!(
            subs.cancel(b"px.", MidSegment::Refuse),
            Change::Unsubscribe("px.#".into())
        );
        assert_eq!(subs.filters().count(), 0);
        // And one cancel too many is ignored rather than counted.
        assert_eq!(subs.cancel(b"px.", MidSegment::Refuse), Change::None);
    }

    #[test]
    fn two_prefixes_that_share_a_filter_hold_it_together() {
        let mut subs = Subscriptions::default();
        assert_eq!(
            subs.subscribe(b"px.eur", MidSegment::BoundaryAndRefilter)
                .expect("first"),
            Change::Subscribe("px.#".into())
        );
        assert_eq!(
            subs.subscribe(b"px.usd", MidSegment::BoundaryAndRefilter)
                .expect("second"),
            Change::None,
            "the filter is already held"
        );
        assert_eq!(
            subs.cancel(b"px.eur", MidSegment::BoundaryAndRefilter),
            Change::None,
            "the other prefix still needs it"
        );
        assert_eq!(
            subs.cancel(b"px.usd", MidSegment::BoundaryAndRefilter),
            Change::Unsubscribe("px.#".into())
        );
    }

    #[test]
    fn a_widened_filter_is_re_applied_locally() {
        let mut subs = Subscriptions::default();
        subs.subscribe(b"px.eur", MidSegment::BoundaryAndRefilter)
            .expect("subscribe");
        assert!(subs.wants("px.eurusd"));
        assert!(subs.wants("px.eur.spot"));
        assert!(
            !subs.wants("px.gbp"),
            "the weida filter brought it in; the peer's prefix did not ask for it"
        );

        // Nothing widened: nothing is dropped locally.
        let mut exact = Subscriptions::default();
        exact.subscribe(b"px.", MidSegment::Refuse).expect("exact");
        assert!(exact.wants("px.anything"));
    }
}
