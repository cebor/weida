//! One row per SP protocol: what it is on the wire, and what a socket
//! speaking it may do.
//!
//! "A protocol-specific socket implements exactly one SP protocol, has
//! endpoint sets, and can send and/or receive only as that protocol permits"
//! (`docs/research/nanomsg-nng.md` §2). This module is that sentence as a
//! table, and it is the reason the crate has one socket type per SP protocol
//! rather than one handle with a tag: PUB cannot receive and SUB cannot send
//! (§4), which is a compile-time fact wherever the socket types are distinct
//! and a runtime surprise wherever they are not.
//!
//! **The wire half is `weida-sp`'s and is not repeated here.** The 12-bit
//! protocol ID, the 4-bit role and the pairing rule live in
//! [`weida_sp::EndpointType`], which is checked against the SP RFCs in a
//! crate with an empty `[dependencies]`
//! ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.3). What
//! this module adds is the half that is a *library's* behaviour and not a
//! format: direction, and whether the protocol holds the per-transaction
//! state an `nng_ctx` is made of.

use weida_sp::EndpointType;

use crate::error::{Error, Result};

/// What one SP protocol permits, beside what its wire form says.
///
/// A `'static` row rather than a method soup, so that a new protocol is one
/// entry in [`PROTOCOLS`] and a reader can see the whole table at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Protocol {
    /// The wire identity: the protocol ID, the role and the pairing rule.
    pub endpoint: EndpointType,
    /// NNG's own name for this protocol, as `nng_pair1_open()` and
    /// `nng_req0_open()` spell it: `pair1`, `req0`. Used in error messages
    /// and in `NNG_OPT_PROTONAME`.
    pub name: &'static str,
    /// Whether a cooked socket of this protocol has a send operation at all.
    /// "PUB has no receive operation; SUB has no send operation" (§4).
    pub can_send: bool,
    /// Whether a cooked socket of this protocol has a receive operation.
    pub can_recv: bool,
    /// Whether this protocol holds the per-transaction state an `nng_ctx` is
    /// made of — a request ID and its resend timer, a survey and its
    /// deadline — and therefore supports contexts (§2).
    ///
    /// The protocols that do are exactly the ones the sheet names: REQ and
    /// REP with one request each, SURVEYOR and RESPONDENT with one survey
    /// each (§4 "Context concurrency boundaries"). A protocol with no such
    /// state gains nothing from a context and refuses one by name rather
    /// than handing back an object that does nothing.
    pub contexts: bool,
}

impl Protocol {
    /// The 16-bit type field this protocol puts in its SP protocol header.
    pub const fn wire_id(&self) -> u16 {
        self.endpoint.id()
    }

    /// The protocol a peer must speak for a pipe to be admitted.
    pub const fn peer(&self) -> EndpointType {
        self.endpoint.peer()
    }

    /// Refuses a context for a protocol that has no per-transaction state,
    /// with NNG's own code and the reason.
    pub fn require_contexts(&self) -> Result<()> {
        if self.contexts {
            return Ok(());
        }
        Err(Error::ENOTSUP(
            format!(
                "{} holds no per-transaction state, so a context would hold nothing; \
                 contexts exist for req0, rep0, surveyor0 and respondent0",
                self.name
            )
            .into(),
        ))
    }
}

/// Every SP protocol, once, in the order `weida_sp::EndpointType` lists
/// them.
///
/// The table is load-bearing: [`protocol`] indexes it, and a test asserts
/// that every [`EndpointType`] appears exactly once, so a protocol cannot be
/// added to the codec and forgotten here.
pub const PROTOCOLS: &[Protocol] = &[
    Protocol {
        endpoint: EndpointType::PairV0,
        name: "pair0",
        can_send: true,
        can_recv: true,
        contexts: false,
    },
    Protocol {
        endpoint: EndpointType::PairV1,
        name: "pair1",
        can_send: true,
        can_recv: true,
        contexts: false,
    },
    Protocol {
        endpoint: EndpointType::Pub,
        name: "pub0",
        can_send: true,
        can_recv: false,
        contexts: false,
    },
    Protocol {
        endpoint: EndpointType::Sub,
        name: "sub0",
        can_send: false,
        can_recv: true,
        contexts: false,
    },
    Protocol {
        endpoint: EndpointType::Req,
        name: "req0",
        can_send: true,
        can_recv: true,
        contexts: true,
    },
    Protocol {
        endpoint: EndpointType::Rep,
        name: "rep0",
        can_send: true,
        can_recv: true,
        contexts: true,
    },
    Protocol {
        endpoint: EndpointType::Push,
        name: "push0",
        can_send: true,
        can_recv: false,
        contexts: false,
    },
    Protocol {
        endpoint: EndpointType::Pull,
        name: "pull0",
        can_send: false,
        can_recv: true,
        contexts: false,
    },
    Protocol {
        endpoint: EndpointType::Surveyor,
        name: "surveyor0",
        can_send: true,
        can_recv: true,
        contexts: true,
    },
    Protocol {
        endpoint: EndpointType::Respondent,
        name: "respondent0",
        can_send: true,
        can_recv: true,
        contexts: true,
    },
    Protocol {
        endpoint: EndpointType::Bus,
        name: "bus0",
        can_send: true,
        can_recv: true,
        contexts: false,
    },
];

/// The row for one protocol.
pub fn protocol(endpoint: EndpointType) -> &'static Protocol {
    PROTOCOLS
        .iter()
        .find(|row| row.endpoint == endpoint)
        .expect("every EndpointType has a row in PROTOCOLS, which a test asserts")
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVERY_ENDPOINT_TYPE: &[EndpointType] = &[
        EndpointType::PairV0,
        EndpointType::PairV1,
        EndpointType::Pub,
        EndpointType::Sub,
        EndpointType::Req,
        EndpointType::Rep,
        EndpointType::Push,
        EndpointType::Pull,
        EndpointType::Surveyor,
        EndpointType::Respondent,
        EndpointType::Bus,
    ];

    /// Claim: the table covers every protocol the codec knows, exactly once,
    /// with a distinct NNG name each. Without this a protocol could be added
    /// to `weida-sp` and silently have no library behaviour at all.
    #[test]
    fn every_sp_protocol_has_exactly_one_row() {
        assert_eq!(PROTOCOLS.len(), EVERY_ENDPOINT_TYPE.len());
        let names: std::collections::BTreeSet<&str> =
            PROTOCOLS.iter().map(|row| row.name).collect();
        assert_eq!(names.len(), PROTOCOLS.len());
        for endpoint in EVERY_ENDPOINT_TYPE {
            let row = protocol(*endpoint);
            assert_eq!(row.endpoint, *endpoint);
            assert_eq!(row.wire_id(), endpoint.id());
            assert!(
                row.can_send || row.can_recv,
                "{} can neither send nor receive",
                row.name
            );
        }
    }

    /// Claim: the one-directional protocols are one-directional in the
    /// table, which is what makes the socket types one-directional in the
    /// type system (§4).
    #[test]
    fn the_one_way_protocols_are_one_way() {
        assert!(protocol(EndpointType::Pub).can_send);
        assert!(!protocol(EndpointType::Pub).can_recv);
        assert!(!protocol(EndpointType::Sub).can_send);
        assert!(protocol(EndpointType::Sub).can_recv);
        assert!(protocol(EndpointType::Push).can_send);
        assert!(!protocol(EndpointType::Push).can_recv);
        assert!(!protocol(EndpointType::Pull).can_send);
        assert!(protocol(EndpointType::Pull).can_recv);
    }

    /// Claim: contexts exist for exactly the four protocols that hold a
    /// transaction, and the other seven refuse one with `NNG_ENOTSUP` and a
    /// reason rather than returning something inert.
    #[test]
    fn contexts_exist_where_there_is_a_transaction_to_hold() {
        let with: Vec<&str> = PROTOCOLS
            .iter()
            .filter(|row| row.contexts)
            .map(|row| row.name)
            .collect();
        assert_eq!(with, ["req0", "rep0", "surveyor0", "respondent0"]);

        for row in PROTOCOLS {
            match row.require_contexts() {
                Ok(()) => assert!(row.contexts),
                Err(err) => {
                    assert!(!row.contexts);
                    assert!(matches!(err, Error::ENOTSUP(_)), "{err:?}");
                    assert!(err.cause().contains(row.name));
                }
            }
        }
    }

    /// Claim: the pairing rule a socket admits a pipe under is the codec's,
    /// not a second copy of it — so a protocol cannot disagree with the
    /// header it sends.
    #[test]
    fn pairing_comes_from_the_codec() {
        assert_eq!(protocol(EndpointType::Req).peer(), EndpointType::Rep);
        assert_eq!(protocol(EndpointType::Bus).peer(), EndpointType::Bus);
        assert_eq!(
            protocol(EndpointType::Surveyor).peer(),
            EndpointType::Respondent
        );
    }
}
