//! The process-level runtime: configuration, the client connection pool and
//! shutdown.

use std::sync::Arc;
use std::sync::Mutex;

use quinn::VarInt;
use weida_core::{Error, Fingerprint};
use weida_protocol::codes;

use crate::config::{ClientTls, RuntimeConfig};
use crate::conn::ConnHandle;
use crate::endpoint::{Endpoint, PushState, Pusher, ReqState, Requester, SubState, Subscriber};
use crate::listener::Listener;
use crate::pool::ClientPool;
use crate::stream::Peer;

pub(crate) struct RuntimeInner {
    pub(crate) config: RuntimeConfig,
    pub(crate) pool: ClientPool,
    /// Every QUIC endpoint this runtime owns, for shutdown.
    endpoints: Mutex<Vec<quinn::Endpoint>>,
}

impl RuntimeInner {
    pub(crate) fn track_endpoint(&self, endpoint: quinn::Endpoint) {
        self.endpoints
            .lock()
            .expect("endpoint list poisoned")
            .push(endpoint);
    }

    /// Dials, or reuses a pooled connection to, `host:port` on `tls`'s terms,
    /// accepting only `expected` when the address named a peer.
    pub(crate) async fn connect(
        &self,
        host: &str,
        port: u16,
        tls: &Arc<ClientTls>,
        expected: Option<Fingerprint>,
    ) -> Result<ConnHandle, Error> {
        self.pool
            .connect(&self.config, host, port, tls, expected)
            .await
    }
}

/// A process-level execution and resource container.
///
/// Cloning shares the same pool, limits and bindings. A `Runtime` needs an
/// ambient Tokio reactor: `quinn` drives its sockets on it, and creating one
/// without a reactor would fail later, at an unrelated call site.
#[derive(Clone)]
pub struct Runtime {
    inner: Arc<RuntimeInner>,
}

impl Runtime {
    /// Creates a runtime on the current Tokio reactor.
    pub fn new(config: RuntimeConfig) -> Result<Runtime, Error> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Runtime("Runtime::new requires an ambient tokio runtime".into()))?;
        Ok(Runtime {
            inner: Arc::new(RuntimeInner {
                config,
                pool: ClientPool::new(),
                endpoints: Mutex::new(Vec::new()),
            }),
        })
    }

    /// Creates an empty messaging namespace.
    ///
    /// Nothing is bound and no credentials are needed yet: a Listener is a
    /// namespace, and server identity belongs to the individual bindings
    /// ([`Listener::bind_quic`]).
    pub fn listener(&self) -> Listener {
        Listener::new(Arc::clone(&self.inner))
    }

    /// Creates a raw L0 peer that dials on `tls`'s terms.
    ///
    /// Below the patterns: a [`Peer`] opens unidirectional and bidirectional
    /// streams directly, with exactly QUIC's guarantees and no pattern
    /// vocabulary layered on top.
    ///
    /// `tls` may be a bare [`crate::Trust`] when the endpoint dials
    /// anonymously, or a [`ClientTls`] when it also presents an identity.
    pub fn peer(&self, tls: impl Into<ClientTls>) -> Peer {
        Peer::new(Arc::clone(&self.inner), Arc::new(tls.into()))
    }

    /// Creates a requester that dials on `tls`'s terms.
    ///
    /// Trust belongs to the dialling endpoint, not to the runtime: one process
    /// may legitimately talk to an internal service behind an internal CA and
    /// to a public one, and it should not need two runtimes to do so. An
    /// endpoint may still dial many peers — they simply share these terms.
    pub fn requester(&self, tls: impl Into<ClientTls>) -> Requester {
        Endpoint::from_state(ReqState::new(Arc::clone(&self.inner), Arc::new(tls.into())))
    }

    /// Creates a pusher that dials on `tls`'s terms.
    pub fn pusher(&self, tls: impl Into<ClientTls>) -> Pusher {
        Endpoint::from_state(PushState::new(
            Arc::clone(&self.inner),
            Arc::new(tls.into()),
        ))
    }

    /// Creates a subscriber that dials on `tls`'s terms.
    ///
    /// Inbound published messages queue up to `Limits::endpoint_queue`; a
    /// subscriber that stops reading therefore stalls its own delivery and,
    /// once the publisher's byte budget for it is exhausted, starts losing
    /// messages rather than slowing the publisher down.
    pub fn subscriber(&self, tls: impl Into<ClientTls>) -> Subscriber {
        Endpoint::from_state(SubState::new(
            Arc::clone(&self.inner),
            Arc::new(tls.into()),
            self.inner.config.limits.endpoint_queue,
        ))
    }

    /// Closes every binding and pooled connection, then waits for the sockets
    /// to go idle so peers see a clean `SHUTDOWN` rather than a timeout.
    pub async fn shutdown(self) {
        let endpoints: Vec<quinn::Endpoint> = self
            .inner
            .endpoints
            .lock()
            .expect("endpoint list poisoned")
            .drain(..)
            .collect();
        let client = self.inner.pool.take_endpoint().await;

        for endpoint in endpoints.iter().chain(client.iter()) {
            endpoint.close(
                VarInt::from_u32(codes::SHUTDOWN as u32),
                b"runtime shutting down",
            );
        }
        for endpoint in endpoints.iter().chain(client.iter()) {
            endpoint.wait_idle().await;
        }
    }

    /// The configuration this runtime was created with.
    pub fn config(&self) -> &RuntimeConfig {
        &self.inner.config
    }
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("limits", &self.inner.config.limits)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClientTls, Identity, Trust};

    /// An endpoint that trusts only what an address names; a plain address
    /// under it must fail before any packet is sent.
    fn no_trust() -> ClientTls {
        ClientTls::new(Trust::by_address())
    }

    #[test]
    fn creating_a_runtime_without_a_reactor_fails() {
        let err = Runtime::new(RuntimeConfig::default()).unwrap_err();
        assert!(matches!(err, Error::Runtime(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_runtime_clone_shares_its_configuration() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        let clone = rt.clone();
        assert_eq!(clone.config().limits, rt.config().limits);
        assert_eq!(clone.requester(no_trust()).peer_count(), 0);
        rt.shutdown().await;
    }

    #[tokio::test]
    async fn a_binding_with_unreadable_tls_material_fails_before_serving() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        // Creating the namespace needs no credentials at all.
        let listener = rt.listener();
        let err = listener
            .bind_quic(
                "127.0.0.1:0".parse().unwrap(),
                Identity::from_pem_files("/nonexistent/c.pem", "/nonexistent/k.pem"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[tokio::test]
    async fn opening_without_a_peer_fails_with_not_connected() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        let requester = rt.requester(no_trust());
        let err = requester
            .open(crate::TransferMeta::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotConnected), "{err:?}");
    }

    #[tokio::test]
    async fn connecting_without_trust_fails_unless_the_address_names_the_peer() {
        // Short idle timeout: the second dial goes to a port nobody answers
        // on, and QUIC gives up only when the handshake idles out.
        let rt = Runtime::new(RuntimeConfig {
            idle_timeout: std::time::Duration::from_millis(200),
            ..RuntimeConfig::default()
        })
        .unwrap();
        // An endpoint with nothing to trust cannot authenticate anyone, so
        // dialling a plain address fails rather than trusting whatever answers.
        let requester = rt.requester(no_trust());
        let err = requester
            .connect("weida://127.0.0.1:1/x")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");

        // With a fingerprint in the address there is something to check, so
        // the dial proceeds — and fails on the socket, since nothing listens.
        let err = requester
            .connect("weida://sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08@127.0.0.1:1/x")
            .await
            .unwrap_err();
        assert!(!matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_malformed_url_is_rejected_before_dialling() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        let err = rt
            .requester(no_trust())
            .connect("http://127.0.0.1:7443/x")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
    }
}
