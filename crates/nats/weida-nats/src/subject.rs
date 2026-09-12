//! Subjects, the two wildcards, and what is not a subject at all.
//!
//! Four sentences from the sheet (`docs/research/nats.md` §4) are the whole
//! specification of this module:
//!
//! > Subject tokens are separated by dots, such as `orders.created`.
//! > `*` matches exactly one complete subject token.
//! > `>` matches one or more trailing subject tokens and must be the final
//! > token.
//! > Thus `orders.*` matches `orders.created` but not `orders.eu.created`;
//! > `orders.>` matches both.
//!
//! Everything here is a pure function over octets. Nothing allocates and
//! nothing touches the connection, which is why the rules can be tested
//! exhaustively instead of inferred from a server's behaviour.
//!
//! # Octets, not text
//!
//! A subject is `&[u8]` here, as it is in `weida-nats-codec`: a subject
//! arriving in a `MSG` is remote input, and a client that refused a
//! non-UTF-8 subject would refuse a message a conforming server delivered.
//! Token splitting, the two wildcards and the dot are all ASCII, so the rules
//! need no more than octets to state.
//!
//! # The degenerate subjects, decided
//!
//! The reference gives the grammar as tokens separated by dots and stops
//! there, so the shapes with an *empty* token are decided here and the choice
//! is stated rather than left to whichever comparison happened to be written
//! first. All four are refused, by one rule: **no token may be empty**.
//!
//! | Subject | Verdict | Why |
//! | --- | --- | --- |
//! | `""` | refused | A subject is one or more tokens; the empty string has none, and `nats-server` answers `-ERR 'Invalid Subject'` |
//! | `.a` | refused | A leading dot is an empty first token |
//! | `a.` | refused | A trailing dot is an empty last token; it is also the shape a naive `prefix + "." + suffix` produces from an empty suffix, which is exactly the bug worth catching locally |
//! | `a..b` | refused | A doubled dot is an empty middle token |
//!
//! Refusing rather than normalising is the point: `a..b` and `a.b` are
//! different subjects to a server that accepted them, so silently collapsing
//! the dot would send a publication somewhere the caller did not name.
//!
//! # A wildcard is a whole token or it is a literal
//!
//! `*` and `>` are wildcards only where they are the *entire* token.
//! `orders.*x` has a literal second token `*x`, and `nats-server`'s own
//! literal-subject check agrees: it refuses a token that *is* `*` or `>` and
//! accepts everything else. So [`matches`] compares `*x` byte for byte, and
//! [`check_publish_subject`] refuses `orders.*` while accepting `orders.*x`.
//!
//! # `>` out of place is not a wildcard either
//!
//! "and must be the final token" is a rule about the *pattern*, so
//! `orders.>.created` is refused by [`check_subscribe_subject`] rather than
//! quietly matched as something. A pattern whose `>` is in the middle is a
//! pattern whose author expected a behaviour the protocol does not have.

use crate::error::{Error, Result};

/// The token separator: `orders.created` is two tokens.
pub const SEPARATOR: u8 = b'.';

/// The single-token wildcard, legal in a subscription subject only.
pub const MATCH_ONE: u8 = b'*';

/// The trailing-tokens wildcard, legal as the last token of a subscription
/// subject only.
pub const MATCH_MANY: u8 = b'>';

/// Whether `pattern` matches `subject`.
///
/// `pattern` is a subscription subject and may hold wildcards; `subject` is
/// the literal subject a `MSG` arrived on. Neither is validated here —
/// [`check_subscribe_subject`] and [`check_publish_subject`] do that at the
/// edge, once, where a caller can be told what was wrong. A `>` that is not
/// the final token matches nothing at all, which is the safe reading of a
/// pattern this crate refuses to send in the first place.
///
/// ```
/// use weida_nats::subject::matches;
///
/// assert!(matches(b"orders.*", b"orders.created"));
/// assert!(!matches(b"orders.*", b"orders.eu.created"));
/// assert!(matches(b"orders.>", b"orders.created"));
/// assert!(matches(b"orders.>", b"orders.eu.created"));
/// ```
#[must_use]
pub fn matches(pattern: &[u8], subject: &[u8]) -> bool {
    let mut subject_tokens = subject.split(|byte| *byte == SEPARATOR);
    let mut pattern_tokens = pattern.split(|byte| *byte == SEPARATOR).peekable();

    while let Some(token) = pattern_tokens.next() {
        if token == [MATCH_MANY] {
            // "one or more trailing subject tokens": at least one must be
            // left, and a `>` that is not last is a pattern this crate does
            // not send, so it matches nothing rather than guessing.
            return pattern_tokens.peek().is_none() && subject_tokens.next().is_some();
        }
        let Some(against) = subject_tokens.next() else {
            return false;
        };
        if token == [MATCH_ONE] {
            // "exactly one complete subject token" — and an empty token is
            // not a token, so `orders.*` does not match `orders.`.
            if against.is_empty() {
                return false;
            }
            continue;
        }
        if token != against {
            return false;
        }
    }
    // Every pattern token is spent, so the subject must be spent too:
    // `orders.*` is not `orders.eu.created`.
    subject_tokens.next().is_none()
}

/// Refuses a subject this client will not publish on.
///
/// A publish subject is literal: it names one destination. So a token that
/// *is* `*` or `>` is refused — publishing to `orders.*` would be publishing
/// to a pattern, which Core NATS does not do — while a token that merely
/// contains one (`orders.*x`) is a literal token and is allowed, because
/// `nats-server`'s own literal-subject check allows it.
pub fn check_publish_subject(subject: &[u8]) -> Result<()> {
    check_tokens(subject)?;
    for token in subject.split(|byte| *byte == SEPARATOR) {
        if token == [MATCH_ONE] || token == [MATCH_MANY] {
            return Err(invalid(
                subject,
                "is a pattern, and a publish names one literal subject",
            ));
        }
    }
    Ok(())
}

/// Refuses a subscription subject this client will not send in a `SUB`.
///
/// Wildcards are allowed; a `>` anywhere but the final token is not, because
/// "must be the final token" is the rule and a pattern that breaks it would
/// be matched by the server against nothing a caller intended.
pub fn check_subscribe_subject(subject: &[u8]) -> Result<()> {
    check_tokens(subject)?;
    let mut tokens = subject.split(|byte| *byte == SEPARATOR).peekable();
    while let Some(token) = tokens.next() {
        if token == [MATCH_MANY] && tokens.peek().is_some() {
            return Err(invalid(
                subject,
                "has a `>` that is not its final token, and `>` matches one or \
                 more *trailing* tokens",
            ));
        }
    }
    Ok(())
}

/// Refuses a queue-group name this client will not send in a `SUB`.
///
/// A queue group is a control-line argument, so it is one token in the
/// argument-count sense: a group name with a space in it would shift every
/// argument after it and turn the `sid` into something else entirely.
pub fn check_queue_group(group: &[u8]) -> Result<()> {
    if group.is_empty() {
        return Err(invalid(group, "is an empty queue group name"));
    }
    if group.iter().any(|byte| is_separator_or_worse(*byte)) {
        return Err(invalid(
            group,
            "holds whitespace or a control byte, and a queue group is one \
             control-line argument",
        ));
    }
    Ok(())
}

/// The rule every subject shares: at least one token, no empty token, and
/// nothing in a token that the control line could not carry.
fn check_tokens(subject: &[u8]) -> Result<()> {
    if subject.is_empty() {
        return Err(invalid(subject, "is empty, and a subject has at least one token"));
    }
    if subject.iter().any(|byte| is_separator_or_worse(*byte)) {
        return Err(invalid(
            subject,
            "holds whitespace or a control byte, and a subject is one \
             control-line argument",
        ));
    }
    for token in subject.split(|byte| *byte == SEPARATOR) {
        if token.is_empty() {
            return Err(invalid(
                subject,
                "has an empty token: a leading, trailing or doubled dot names \
                 a token that is not there",
            ));
        }
    }
    Ok(())
}

/// Whitespace splits a control line into arguments, and a control byte ends
/// it. Either one inside a subject means the sender and the receiver disagree
/// about what they are looking at.
const fn is_separator_or_worse(byte: u8) -> bool {
    byte == b' ' || byte == b'\t' || byte == b'\r' || byte == b'\n' || byte < 0x20
}

fn invalid(subject: &[u8], why: &'static str) -> Error {
    Error::InvalidSubject {
        subject: String::from_utf8_lossy(subject).into_owned(),
        why,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sheet's own two sentences, asserted literally.
    #[test]
    fn the_sheets_two_named_cases() {
        assert!(matches(b"orders.*", b"orders.created"));
        assert!(!matches(b"orders.*", b"orders.eu.created"));
        assert!(matches(b"orders.>", b"orders.created"));
        assert!(matches(b"orders.>", b"orders.eu.created"));
    }

    /// "exactly one complete subject token": not zero, not two.
    #[test]
    fn star_is_exactly_one_token() {
        assert!(matches(b"*", b"orders"));
        assert!(matches(b"*.*", b"orders.created"));
        assert!(!matches(b"orders.*", b"orders"), "not zero tokens");
        assert!(!matches(b"orders.*", b"orders.eu.created"), "not two");
        assert!(
            matches(b"*.created", b"orders.created"),
            "a wildcard is positional, not trailing"
        );
        assert!(!matches(b"*.created", b"orders.eu.created"));
    }

    /// "one or more trailing subject tokens": one is enough, zero is not.
    #[test]
    fn angle_is_one_or_more_trailing_tokens() {
        assert!(matches(b">", b"orders"));
        assert!(matches(b">", b"orders.eu.created"));
        assert!(matches(b"orders.>", b"orders.eu.created.v2"));
        assert!(
            !matches(b"orders.>", b"orders"),
            "`>` needs at least one trailing token"
        );
    }

    /// A `>` in the middle is not a wildcard: it is a pattern this crate
    /// refuses, and refusing it is where the caller finds out.
    #[test]
    fn angle_must_be_last() {
        assert!(check_subscribe_subject(b"orders.>").is_ok());
        let error = check_subscribe_subject(b"orders.>.created").expect_err("refused");
        assert!(
            matches!(&error, Error::InvalidSubject { why, .. } if why.contains("final token")),
            "{error}"
        );
        assert!(
            !matches(b"orders.>.created", b"orders.eu.created"),
            "and it matches nothing either"
        );
    }

    /// A wildcard is a whole token or it is literal text.
    #[test]
    fn a_wildcard_is_a_whole_token() {
        assert!(matches(b"orders.*x", b"orders.*x"));
        assert!(!matches(b"orders.*x", b"orders.created"));
        assert!(
            check_publish_subject(b"orders.*x").is_ok(),
            "a token that merely contains a star is a literal token"
        );
        assert!(
            check_publish_subject(b"orders.*").is_err(),
            "a token that is a star is a pattern"
        );
        assert!(check_publish_subject(b"orders.>").is_err());
    }

    /// The four degenerate subjects of the module table, each refused by both
    /// checks, with the empty-token rule as the single reason.
    #[test]
    fn the_degenerate_subjects_are_refused() {
        for subject in [&b""[..], b".a", b"a.", b"a..b"] {
            let publish = check_publish_subject(subject)
                .expect_err("a publish subject with an empty token is refused");
            let subscribe = check_subscribe_subject(subject)
                .expect_err("a subscription subject with an empty token is refused");
            for error in [publish, subscribe] {
                assert!(
                    matches!(error, Error::InvalidSubject { .. }),
                    "{subject:?}: {error}"
                );
            }
        }
        assert!(check_publish_subject(b"a.b").is_ok());
    }

    /// Whitespace in a subject would shift every control-line argument after
    /// it, so it is refused on the way out — the codec's own
    /// `ArgumentNotOneToken` rule, enforced one layer earlier where the
    /// caller can be told which subject it was.
    #[test]
    fn whitespace_and_control_bytes_are_refused() {
        for subject in [&b"a b"[..], b"a\tb", b"a\r\nb", b"a\0b"] {
            assert!(
                check_publish_subject(subject).is_err(),
                "{subject:?} must be refused"
            );
        }
    }

    /// A queue group is one argument too, and an empty one would vanish from
    /// the line and turn a grouped `SUB` into an ordinary one.
    #[test]
    fn a_queue_group_is_one_nonempty_argument() {
        assert!(check_queue_group(b"workers").is_ok());
        assert!(check_queue_group(b"").is_err());
        assert!(check_queue_group(b"two words").is_err());
    }
}
