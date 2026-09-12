//! Applying the subscriptions an options object carried.
//!
//! `SocketOptions(subscribe=...)` is one object that says what a socket is,
//! and only one socket type has subscriptions. A trait with one real
//! implementation and ten refusing ones is how that stays a compile-time
//! fact: a prefix set given to a PUSH socket is refused when the socket is
//! opened, by name, rather than silently ignored.

use weida_nng::{Error, Result};

/// What a socket type does with a prefix list.
pub trait Subscribes {
    /// Subscribes to each prefix, or says why this protocol cannot.
    fn apply_prefixes(&self, prefixes: &[Vec<u8>]) -> Result<()>;
}

impl Subscribes for weida_nng::SubSocket {
    fn apply_prefixes(&self, prefixes: &[Vec<u8>]) -> Result<()> {
        for prefix in prefixes {
            self.subscribe(prefix.clone());
        }
        Ok(())
    }
}

/// Every socket type but SUB: a prefix list is a configuration error.
macro_rules! no_subscriptions {
    ($($rust:ident),+ $(,)?) => {
        $(impl Subscribes for weida_nng::$rust {
            fn apply_prefixes(&self, prefixes: &[Vec<u8>]) -> Result<()> {
                if prefixes.is_empty() {
                    return Ok(());
                }
                Err(Error::ENOTSUP(
                    concat!(
                        "NNG_OPT_SUB_SUBSCRIBE is a SUB option and a ",
                        stringify!($rust),
                        " has no subscriptions",
                    )
                    .into(),
                ))
            }
        })+
    };
}

no_subscriptions!(
    ReqSocket,
    RepSocket,
    PushSocket,
    PullSocket,
    PubSocket,
    Pair0Socket,
    Pair1Socket,
    SurveyorSocket,
    RespondentSocket,
    BusSocket,
);
