//! Topic Aliases: two tables, one per direction, both dying with the
//! connection.
//!
//! A Topic Alias replaces a Topic Name with a two-byte integer for the rest of
//! the connection (3.3.2.3.4) [mqtt5 §3]. Four rules decide everything here,
//! and each one is a place an implementation goes wrong:
//!
//! 1. **Per direction.** "Topic Alias mappings exist only within a Network
//!    Connection and Session, and are **not** shared between directions"
//!    ([MQTT-3.3.2-8]). A server declaring `Topic Alias Maximum` 10 in CONNACK
//!    has said what *it* will accept from us; what we will accept from it is
//!    the number *we* declared in CONNECT. So there are two tables with two
//!    different bounds, and one shared table would silently accept an alias
//!    the peer was never allowed to send.
//! 2. **Per connection.** "Topic Alias mappings MUST NOT be carried across
//!    Network Connections" ([MQTT-3.3.2-7]). Both tables are constructed with
//!    the connection and dropped with it; [`crate::StoredPublish`] has no
//!    alias field at all, so a retransmitted PUBLISH goes out with its full
//!    Topic Name on the new connection.
//! 3. **Established by a non-zero-length Topic Name.** A PUBLISH carrying an
//!    alias *and* a topic sets or overwrites the mapping; one carrying an
//!    alias and a zero-length topic uses it ([MQTT-3.3.4-6]). There is no
//!    other way to establish one - no negotiation packet, no handshake.
//! 4. **Never zero.** "A Topic Alias of 0 is a Protocol Error"
//!    ([MQTT-3.3.2-8]), and one above the receiver's declared maximum is
//!    too - both earning 0x94 (Topic Alias invalid).
//!
//! # Where the bound is
//!
//! Both tables are bounded by a number the *receiver* declared, so neither can
//! be grown by the peer: the outbound table by the server's `Topic Alias
//! Maximum` from CONNACK, the inbound table by
//! [`crate::Limits::topic_alias_maximum`] from our own CONNECT. A peer that
//! sends alias 40,000 against our declared 10 does not allocate a 40,000-entry
//! map; it earns a Protocol Error.

use std::collections::HashMap;

use crate::error::{Error, Feature, Result};

/// The aliases this client has handed out on this connection.
///
/// Bounded by the server's `Topic Alias Maximum`, which is also the highest
/// alias value it will accept (3.2.2.3.8): aliases are allocated 1..=max, so
/// the table's size and the protocol's ceiling are the same number and no
/// second bound is needed.
///
/// **No eviction.** Once `max` topics have aliases, further topics are
/// published with their full Topic Name and no alias. Replacing a mapping is
/// legal - a PUBLISH with an established alias and a new Topic Name
/// overwrites it ([MQTT-3.3.4-6]) - and deliberately not done, because the
/// only policy that could beat "keep the first `max`" needs to know which
/// topic this client will publish to next, and a library that guessed would
/// spend a full Topic Name on every publish in the worst case instead of
/// saving one in the common case. A caller that knows its hot topics
/// publishes to them first.
#[derive(Debug)]
pub struct OutboundAliases {
    max: u16,
    assigned: HashMap<String, u16>,
}

/// What to put on the wire for one publish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Aliased {
    /// Send the Topic Name in full and no alias: either the server declined
    /// aliases entirely, or the table is full.
    Full,
    /// Send the Topic Name in full **and** this alias, establishing it
    /// ([MQTT-3.3.4-6]).
    Establish(u16),
    /// Send a zero-length Topic Name and this alias. The only case where a
    /// zero-length Topic Name is legal (3.3.2.1).
    Use(u16),
}

impl OutboundAliases {
    /// A table bounded by the server's `Topic Alias Maximum`. Zero - the
    /// protocol's default - means the server accepts no aliases
    /// ([MQTT-3.2.2-18]), so nothing is ever aliased.
    #[must_use]
    pub fn new(maximum: u16) -> OutboundAliases {
        OutboundAliases {
            max: maximum,
            assigned: HashMap::new(),
        }
    }

    /// How many aliases are in use.
    #[must_use]
    pub fn len(&self) -> usize {
        self.assigned.len()
    }

    /// Whether none are.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.assigned.is_empty()
    }

    /// What this publish should carry for `topic`.
    pub fn publish(&mut self, topic: &str) -> Aliased {
        if self.max == 0 || topic.is_empty() {
            return Aliased::Full;
        }
        if let Some(alias) = self.assigned.get(topic) {
            return Aliased::Use(*alias);
        }
        let next = u16::try_from(self.assigned.len()).unwrap_or(u16::MAX);
        if next >= self.max {
            return Aliased::Full;
        }
        // 1..=max: "Topic Alias 0 is a Protocol Error" ([MQTT-3.3.2-8]).
        let alias = next + 1;
        self.assigned.insert(topic.to_owned(), alias);
        Aliased::Establish(alias)
    }
}

/// The aliases the server has established toward this client on this
/// connection.
///
/// Bounded by the `Topic Alias Maximum` this client declared in CONNECT, which
/// is the only number the server is permitted to use ([MQTT-3.2.2-18] in the
/// other direction, 3.1.2.11.5): an alias outside 1..=max is refused before
/// the map is touched, so the map never holds more than `max` entries however
/// hostile the peer.
#[derive(Debug, Default)]
pub struct InboundAliases {
    max: u16,
    known: HashMap<u16, String>,
}

impl InboundAliases {
    /// A table bounded by what this client declared it would accept.
    #[must_use]
    pub fn new(maximum: u16) -> InboundAliases {
        InboundAliases {
            max: maximum,
            known: HashMap::new(),
        }
    }

    /// How many aliases the server has established.
    #[must_use]
    pub fn len(&self) -> usize {
        self.known.len()
    }

    /// Whether none are.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.known.is_empty()
    }

    /// The Topic Name a delivery is really about.
    ///
    /// Three cases, all of 3.3.4's rules:
    ///
    /// * no alias: the Topic Name as sent, which must not be zero-length;
    /// * an alias with a Topic Name: the mapping is set or overwritten, and
    ///   the Topic Name is the answer ([MQTT-3.3.4-6]);
    /// * an alias with a zero-length Topic Name: the mapping is used.
    ///
    /// # Errors
    ///
    /// [`Error::Unavailable`] with 0x94 (Topic Alias invalid) for alias 0, for
    /// an alias above the maximum this client declared, and for a zero-length
    /// Topic Name whose alias was never established. The last is not
    /// pedantry - it is the difference between reporting a Protocol Error and
    /// delivering a message on an unknown topic.
    pub fn resolve(&mut self, alias: Option<u16>, topic: &str) -> Result<String> {
        let Some(alias) = alias else {
            if topic.is_empty() {
                return Err(Error::InvalidTopic {
                    topic: String::new(),
                    reason: "a zero-length Topic Name is legal only with a Topic Alias \
                             (3.3.2.1)",
                });
            }
            return Ok(topic.to_owned());
        };
        if alias == 0 || alias > self.max {
            // "A Topic Alias of 0 is a Protocol Error", and one above the
            // declared maximum is too ([MQTT-3.3.2-8]).
            return Err(Error::Unavailable {
                feature: Feature::TopicAlias,
                reason_code: Feature::TopicAlias.reason_code(),
            });
        }
        if topic.is_empty() {
            return self.known.get(&alias).cloned().ok_or(Error::Unavailable {
                feature: Feature::TopicAlias,
                reason_code: Feature::TopicAlias.reason_code(),
            });
        }
        self.known.insert(alias, topic.to_owned());
        Ok(topic.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An alias is established by a full Topic Name and then used by a
    /// zero-length one, which is the whole mechanism ([MQTT-3.3.4-6]).
    #[test]
    fn an_alias_is_established_once_and_used_after() {
        let mut aliases = OutboundAliases::new(4);
        assert_eq!(aliases.publish("a/b"), Aliased::Establish(1));
        assert_eq!(aliases.publish("a/b"), Aliased::Use(1));
        assert_eq!(aliases.publish("a/b"), Aliased::Use(1));
        assert_eq!(aliases.publish("c/d"), Aliased::Establish(2));
        assert_eq!(aliases.publish("a/b"), Aliased::Use(1), "still its own");
        assert_eq!(aliases.len(), 2);
    }

    /// The first alias is 1, never 0 ([MQTT-3.3.2-8]), and the last is the
    /// maximum the server declared.
    #[test]
    fn aliases_run_from_one_to_the_servers_maximum() {
        let mut aliases = OutboundAliases::new(3);
        let handed: Vec<Aliased> = ["a", "b", "c", "d", "e"]
            .iter()
            .map(|topic| aliases.publish(topic))
            .collect();
        assert_eq!(
            handed,
            [
                Aliased::Establish(1),
                Aliased::Establish(2),
                Aliased::Establish(3),
                // Full, and the topics keep going out in full rather than
                // stealing an alias from a topic that is still in use.
                Aliased::Full,
                Aliased::Full,
            ]
        );
        assert_eq!(aliases.len(), 3, "never more than the declared maximum");
        // And the established ones still work, which is what no eviction buys.
        assert_eq!(aliases.publish("a"), Aliased::Use(1));
    }

    /// `Topic Alias Maximum` 0 - the protocol's own default - means the peer
    /// accepts none ([MQTT-3.2.2-18]), so nothing is ever aliased.
    #[test]
    fn a_maximum_of_zero_aliases_nothing() {
        let mut aliases = OutboundAliases::new(0);
        assert_eq!(aliases.publish("a/b"), Aliased::Full);
        assert_eq!(aliases.publish("a/b"), Aliased::Full);
        assert!(aliases.is_empty());
    }

    /// The inbound half: set by a full Topic Name, read by a zero-length one.
    #[test]
    fn an_inbound_alias_resolves_what_it_was_set_to() {
        let mut aliases = InboundAliases::new(4);
        assert_eq!(aliases.resolve(Some(1), "a/b").unwrap(), "a/b");
        assert_eq!(aliases.resolve(Some(1), "").unwrap(), "a/b");
        assert_eq!(aliases.resolve(Some(1), "").unwrap(), "a/b");

        // "A sender can modify the Topic Alias mapping by sending another
        // PUBLISH with the same alias and a different Topic Name"
        // ([MQTT-3.3.4-6]).
        assert_eq!(aliases.resolve(Some(1), "c/d").unwrap(), "c/d");
        assert_eq!(aliases.resolve(Some(1), "").unwrap(), "c/d");
        assert_eq!(aliases.len(), 1, "overwritten, not added");

        // No alias at all is the ordinary case.
        assert_eq!(aliases.resolve(None, "e/f").unwrap(), "e/f");
        assert_eq!(aliases.len(), 1);
    }

    /// Every way an alias can be invalid, each earning 0x94.
    #[test]
    fn an_invalid_inbound_alias_is_a_protocol_error() {
        let mut aliases = InboundAliases::new(2);

        // Zero.
        assert_eq!(
            aliases.resolve(Some(0), "a").unwrap_err().reason_code(),
            Some(0x94)
        );
        // Above what this client declared - the hostile case, and the map is
        // not grown by it.
        assert_eq!(
            aliases
                .resolve(Some(40_000), "a")
                .unwrap_err()
                .reason_code(),
            Some(0x94)
        );
        assert!(aliases.is_empty(), "a refused alias allocates nothing");
        // Never established, so there is nothing to resolve.
        assert_eq!(
            aliases.resolve(Some(2), "").unwrap_err().reason_code(),
            Some(0x94)
        );
        // A zero-length Topic Name with no alias at all.
        assert!(aliases.resolve(None, "").is_err());
    }

    /// A client that declared `Topic Alias Maximum` 0 - which is what
    /// [`crate::Limits`] does not default to, but a caller may choose - accepts
    /// no alias whatever, because 1 is already above 0.
    #[test]
    fn declaring_zero_refuses_every_alias() {
        let mut aliases = InboundAliases::new(0);
        assert_eq!(
            aliases.resolve(Some(1), "a").unwrap_err().reason_code(),
            Some(0x94)
        );
        assert_eq!(aliases.resolve(None, "a").unwrap(), "a");
    }

    /// The two directions are independent ([MQTT-3.3.2-8]): the same number in
    /// each is a different permission, and alias 3 inbound has nothing to do
    /// with alias 3 outbound.
    #[test]
    fn the_two_directions_do_not_share_a_table() {
        let mut outbound = OutboundAliases::new(1);
        let mut inbound = InboundAliases::new(4);

        assert_eq!(outbound.publish("ours"), Aliased::Establish(1));
        assert_eq!(outbound.publish("second"), Aliased::Full, "our bound is 1");

        // The server's own alias 1 is a different mapping, and its 2, 3 and 4
        // are permitted because *we* declared 4.
        assert_eq!(inbound.resolve(Some(1), "theirs").unwrap(), "theirs");
        assert_eq!(inbound.resolve(Some(4), "far").unwrap(), "far");
        assert_eq!(inbound.resolve(Some(1), "").unwrap(), "theirs");
        assert_eq!(outbound.publish("ours"), Aliased::Use(1));
    }
}
