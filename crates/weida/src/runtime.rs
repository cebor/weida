//! The process-level runtime: configuration, the client connection pool and
//! shutdown.

use std::sync::Arc;
use std::sync::Mutex;

use quinn::VarInt;
use weida_core::Error;
use weida_protocol::codes;

use crate::config::{RuntimeConfig, ServerTls};
use crate::conn::ConnHandle;
use crate::endpoint::{Endpoint, PushState, Pusher, ReqState, Requester, SubState, Subscriber};
use crate::listener::Listener;
use crate::pool::ClientPool;
use crate::tls;

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

    /// Dials, or reuses a pooled connection to, `host:port`.
    pub(crate) async fn connect(&self, host: &str, port: u16) -> Result<ConnHandle, Error> {
        self.pool.connect(&self.config, host, port).await
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

    /// Creates a listener with the given server identity.
    ///
    /// The TLS material is loaded and validated here, so a misconfigured
    /// certificate fails before any socket is bound.
    pub async fn listener(&self, tls: ServerTls) -> Result<Listener, Error> {
        let server_config = tls::server_config(
            &tls,
            &self.inner.config.limits,
            self.inner.config.idle_timeout,
        )?;
        Ok(Listener::new(Arc::clone(&self.inner), server_config))
    }

    /// Creates a requester. It dials on [`Requester::connect`].
    pub fn requester(&self) -> Requester {
        Endpoint::from_state(ReqState::new(Arc::clone(&self.inner)))
    }

    /// Creates a pusher. It dials on [`Pusher::connect`].
    pub fn pusher(&self) -> Pusher {
        Endpoint::from_state(PushState::new(Arc::clone(&self.inner)))
    }

    /// Creates a subscriber. It dials on [`Subscriber::connect`].
    ///
    /// Inbound published messages queue up to `Limits::endpoint_queue`; a
    /// subscriber that stops reading therefore stalls its own delivery and,
    /// once the publisher's byte budget for it is exhausted, starts losing
    /// messages rather than slowing the publisher down.
    pub fn subscriber(&self) -> Subscriber {
        Endpoint::from_state(SubState::new(
            Arc::clone(&self.inner),
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
        assert_eq!(clone.requester().peer_count(), 0);
        rt.shutdown().await;
    }

    #[tokio::test]
    async fn a_listener_with_unreadable_tls_material_fails_early() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        let err = rt
            .listener(ServerTls::new("/nonexistent/c.pem", "/nonexistent/k.pem"))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[tokio::test]
    async fn opening_without_a_peer_fails_with_not_connected() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        let requester = rt.requester();
        let err = requester
            .open(crate::TransferMeta::default())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotConnected), "{err:?}");
    }

    #[tokio::test]
    async fn connecting_without_trust_anchors_fails() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        let requester = rt.requester();
        let err = requester
            .connect("weida://127.0.0.1:1/x")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_malformed_url_is_rejected_before_dialling() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        let err = rt
            .requester()
            .connect("http://127.0.0.1:7443/x")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
    }
}
