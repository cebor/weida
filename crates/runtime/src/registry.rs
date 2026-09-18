//! A namespace of bound names with a byte budget.

use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::{Notify, mpsc};
use weida_core::Error;

/// A namespace of names that can be bound, dialled and unbound, generic over
/// what a bound name hands its acceptor.
///
/// **What it is for.** Every protocol with an in-process transport needs the
/// same object: a set of names, one owner per name, a queue from the dialler
/// to the owner, and a ceiling on how long a name may be. weida's
/// `weida+inproc://<bus>/<path>` buses and ZeroMQ's `inproc://` endpoints are
/// that object twice, down to the 256-byte budget, which libzmq set first
/// ([decisions/0010](../../../docs/decisions/0010-local-transport.md) §4.8,
/// `docs/research/zeromq.md` §11).
///
/// `T` is whatever binding a name entitles its owner to receive — one side of
/// a connection, a pipe pair, a socket. The registry never looks at it.
///
/// **Scope is the holder's choice.** A `static` registry is a process-wide
/// namespace, which is what a library with a process-global transport wants;
/// one owned by a context object is a per-context namespace, which is what
/// libzmq's `inproc://` actually is. Neither needs a different type.
///
/// **The byte budget is remote input's bound.** A name may come from a peer,
/// an address or a configuration file, so its length is capped at
/// construction time and every entry point checks it
/// (`docs/INVARIANTS.md`). Control bytes are refused for the same reason a
/// name is a name: it ends up in a log line, an error message and a
/// comparison.
pub struct NameRegistry<T> {
    max_name_bytes: usize,
    entries: Mutex<HashMap<String, mpsc::UnboundedSender<T>>>,
    /// Woken on every bind, for a dialler waiting for a name to appear.
    bound: Notify,
}

impl<T> NameRegistry<T> {
    /// A registry whose names may be at most `max_name_bytes` bytes long.
    pub fn new(max_name_bytes: usize) -> NameRegistry<T> {
        NameRegistry {
            max_name_bytes,
            entries: Mutex::new(HashMap::new()),
            bound: Notify::new(),
        }
    }

    /// The byte budget names in this registry live under.
    pub const fn max_name_bytes(&self) -> usize {
        self.max_name_bytes
    }

    /// Checks `name` against the budget and against the control bytes.
    ///
    /// A consumer whose own grammar forbids more than this — a separator
    /// byte, say, because the name is one field of an address — checks that
    /// itself and calls this for the rest.
    pub fn validate(&self, name: &str) -> Result<(), Error> {
        if name.is_empty() || name.len() > self.max_name_bytes {
            return Err(Error::InvalidAddress(format!(
                "name must be 1..={} bytes: {name:?}",
                self.max_name_bytes
            )));
        }
        if name.bytes().any(|b| b < 0x20) {
            return Err(Error::InvalidAddress(format!(
                "invalid byte in name: {name:?}"
            )));
        }
        Ok(())
    }

    /// Binds `name` and returns the queue of whatever is dialled to it.
    ///
    /// Fails with [`Error::InvalidAddress`] for a name that
    /// [`NameRegistry::validate`] refuses and with
    /// [`Error::AlreadyRegistered`] for a name somebody already holds: one
    /// owner per name, and the second caller is told rather than silently
    /// displacing the first.
    pub fn bind(&self, name: &str) -> Result<mpsc::UnboundedReceiver<T>, Error> {
        self.validate(name)?;
        let mut entries = self.entries.lock().expect("name registry poisoned");
        if entries.contains_key(name) {
            return Err(Error::AlreadyRegistered);
        }
        let (tx, rx) = mpsc::unbounded_channel();
        entries.insert(name.to_owned(), tx);
        drop(entries);
        self.bound.notify_waiters();
        Ok(rx)
    }

    /// Removes `name`. The binding's own drop is the caller; unbinding a name
    /// nobody holds is not an error, because a drop cannot fail.
    pub fn unbind(&self, name: &str) {
        self.entries
            .lock()
            .expect("name registry poisoned")
            .remove(name);
    }

    /// The sender for `name`, or `None` when nothing is bound there.
    ///
    /// `None` is the in-process equivalent of a dial to a closed port, and
    /// the caller reports it in its own vocabulary: this crate has no opinion
    /// on what "nobody is listening" means to a protocol.
    pub fn lookup(&self, name: &str) -> Option<mpsc::UnboundedSender<T>> {
        self.entries
            .lock()
            .expect("name registry poisoned")
            .get(name)
            .cloned()
    }

    /// Resolves once `name` is bound, at once if it already is.
    ///
    /// The in-process counterpart of redialling a socket: a bus is back
    /// exactly when its name is registered again, so a dialler waits on the
    /// registry rather than on a clock. Registered before the check, so a
    /// bind between the check and the wait is not missed.
    pub async fn wait_bound(&self, name: &str) {
        loop {
            let bound = self.bound.notified();
            tokio::pin!(bound);
            bound.as_mut().enable();
            if self.lookup(name).is_some() {
                return;
            }
            bound.await;
        }
    }
}

impl<T> std::fmt::Debug for NameRegistry<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NameRegistry")
            .field("max_name_bytes", &self.max_name_bytes)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: a name is bounded at both ends and carries no control bytes,
    /// and the budget is the one the registry was built with.
    #[test]
    fn a_name_is_bounded_by_the_registry_budget() {
        let registry: NameRegistry<u8> = NameRegistry::new(8);
        assert_eq!(registry.max_name_bytes(), 8);
        assert!(registry.validate("orders").is_ok());
        assert!(registry.validate(&"x".repeat(8)).is_ok());
        assert!(registry.validate(&"x".repeat(9)).is_err());
        assert!(registry.validate("").is_err());
        assert!(registry.validate("has\ncontrol").is_err());
    }

    /// Claim: one owner per name — the second bind is refused rather than
    /// displacing the first — and unbinding frees the name again.
    #[test]
    fn one_owner_per_name() {
        let registry: NameRegistry<u8> = NameRegistry::new(16);
        let first = registry.bind("orders").expect("first bind");
        assert!(matches!(
            registry.bind("orders"),
            Err(Error::AlreadyRegistered)
        ));
        drop(first);
        // Dropping the receiver does not free the name: the binding's own
        // drop is what unbinds, so a dropped acceptor cannot be replaced
        // behind the binding's back.
        assert!(matches!(
            registry.bind("orders"),
            Err(Error::AlreadyRegistered)
        ));
        registry.unbind("orders");
        assert!(registry.bind("orders").is_ok());
    }

    /// Claim: what a dialler hands over reaches the name's owner, and a name
    /// nobody holds resolves to nothing at all.
    #[test]
    fn a_dial_reaches_the_owner_and_an_unbound_name_reaches_nobody() {
        let registry: NameRegistry<u8> = NameRegistry::new(16);
        assert!(registry.lookup("orders").is_none());
        let mut incoming = registry.bind("orders").expect("bind");
        registry
            .lookup("orders")
            .expect("bound name has a sender")
            .send(9)
            .expect("the owner is still listening");
        assert_eq!(incoming.try_recv().expect("delivered"), 9);

        registry.unbind("orders");
        assert!(registry.lookup("orders").is_none());
    }

    /// Claim: a name over the budget is refused by `bind` and `validate`
    /// alike, so the check cannot be skipped by going through the registry.
    #[test]
    fn an_oversized_name_cannot_be_bound() {
        let registry: NameRegistry<u8> = NameRegistry::new(4);
        let err = registry.bind("toolong").unwrap_err();
        assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
    }
}
