//! Listener, bindings and the endpoint namespace.
//!
//! A Listener is one externally reachable messaging namespace, not one socket
//! (master doc §3): several bindings may serve the same set of endpoints.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use quinn::VarInt;
use tokio::sync::mpsc;
use weida_core::{Error, Limits, validate_endpoint_path};
use weida_protocol::codes;

use crate::conn::ConnCtx;
use crate::endpoint::{Endpoint, RepState, Replier};
use crate::runtime::RuntimeInner;
use crate::transfer::IncomingRequest;

/// Endpoint path to accept-queue map.
///
/// A plain `RwLock<HashMap>`, not an async lock: lookup is a hash and a channel
/// clone, and the guard is never held across an `await`.
pub(crate) struct Namespace {
    routes: RwLock<HashMap<Arc<str>, mpsc::Sender<IncomingRequest>>>,
}

impl Namespace {
    fn new() -> Namespace {
        Namespace {
            routes: RwLock::new(HashMap::new()),
        }
    }

    /// Looks a path up. The path is compared byte for byte: it is an opaque
    /// identifier, so there is no splitting, prefix matching or wildcarding
    /// (master doc §4, §81 rule 5).
    pub(crate) fn lookup(&self, path: &str) -> Option<mpsc::Sender<IncomingRequest>> {
        self.routes
            .read()
            .expect("namespace lock poisoned")
            .get(path)
            .cloned()
    }

    fn register(&self, path: &str, queue: mpsc::Sender<IncomingRequest>) -> Result<(), Error> {
        let mut routes = self.routes.write().expect("namespace lock poisoned");
        match routes.entry(Arc::from(path)) {
            Entry::Occupied(_) => Err(Error::AlreadyRegistered),
            Entry::Vacant(slot) => {
                slot.insert(queue);
                Ok(())
            }
        }
    }
}

pub(crate) struct ListenerInner {
    pub(crate) runtime: Arc<RuntimeInner>,
    pub(crate) namespace: Arc<Namespace>,
    pub(crate) server_config: quinn::ServerConfig,
}

/// One externally reachable messaging namespace.
#[derive(Clone)]
pub struct Listener {
    inner: Arc<ListenerInner>,
}

impl Listener {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, server_config: quinn::ServerConfig) -> Listener {
        Listener {
            inner: Arc::new(ListenerInner {
                runtime,
                namespace: Arc::new(Namespace::new()),
                server_config,
            }),
        }
    }

    /// Adds a native QUIC binding.
    ///
    /// Pass port `0` to let the OS choose; read it back with
    /// [`Binding::local_addr`].
    pub async fn bind_quic(&self, addr: SocketAddr) -> Result<Binding, Error> {
        let endpoint =
            quinn::Endpoint::server(self.inner.server_config.clone(), addr).map_err(Error::Io)?;
        let local_addr = endpoint.local_addr().map_err(Error::Io)?;
        self.inner.runtime.track_endpoint(endpoint.clone());

        tokio::spawn(accept_connections(
            endpoint.clone(),
            Arc::clone(&self.inner.namespace),
            self.inner.runtime.config.limits,
        ));

        tracing::info!(%local_addr, "quic binding listening");
        Ok(Binding {
            endpoint,
            local_addr,
        })
    }

    /// Registers a replier for `path`.
    ///
    /// The path must be a valid endpoint path and must not already be
    /// registered on this listener.
    pub fn replier(&self, path: &str) -> Result<Replier, Error> {
        validate_endpoint_path(path)?;
        let (tx, rx) = mpsc::channel(self.inner.runtime.config.limits.endpoint_queue);
        self.inner.namespace.register(path, tx)?;
        Ok(Endpoint::from_state(RepState::new(path, rx)))
    }
}

impl std::fmt::Debug for Listener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Listener").finish_non_exhaustive()
    }
}

/// One concrete transport binding of a [`Listener`].
#[derive(Debug)]
pub struct Binding {
    endpoint: quinn::Endpoint,
    local_addr: SocketAddr,
}

impl Binding {
    /// The address actually bound, with the port resolved.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops accepting and closes this binding's connections.
    pub async fn close(&self) {
        self.endpoint.close(shutdown_code(), b"binding closed");
        self.endpoint.wait_idle().await;
    }
}

fn shutdown_code() -> VarInt {
    VarInt::from_u32(codes::SHUTDOWN as u32)
}

/// Accepts connections until the endpoint is closed.
async fn accept_connections(endpoint: quinn::Endpoint, namespace: Arc<Namespace>, limits: Limits) {
    let live = Arc::new(AtomicUsize::new(0));
    while let Some(incoming) = endpoint.accept().await {
        if live.load(Ordering::Relaxed) >= limits.max_connections {
            // Complete the handshake, then say why: a bare refusal leaves the
            // peer unable to distinguish overload from a routing mistake.
            tracing::warn!(
                max = limits.max_connections,
                "connection limit reached; refusing"
            );
            tokio::spawn(async move {
                if let Ok(conn) = incoming.await {
                    conn.close(
                        VarInt::from_u32(codes::LIMIT_EXCEEDED as u32),
                        b"connection limit reached",
                    );
                }
            });
            continue;
        }

        let namespace = Arc::clone(&namespace);
        let live = Arc::clone(&live);
        live.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    let remote = conn.remote_address();
                    tracing::debug!(%remote, "connection accepted");
                    // The handle must outlive the connection: it owns the
                    // actor's control channel.
                    let _ctx = ConnCtx::spawn(conn.clone(), limits, Some(namespace));
                    let reason = conn.closed().await;
                    tracing::debug!(%remote, %reason, "connection closed");
                }
                Err(e) => tracing::debug!(error = %e, "handshake failed"),
            }
            live.fetch_sub(1, Ordering::Relaxed);
        });
    }
    tracing::debug!("binding stopped accepting");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn namespace_rejects_duplicate_paths() {
        let ns = Namespace::new();
        let (tx, _rx) = mpsc::channel(1);
        ns.register("/a", tx.clone()).unwrap();
        assert!(matches!(
            ns.register("/a", tx.clone()),
            Err(Error::AlreadyRegistered)
        ));
        ns.register("/b", tx).unwrap();
    }

    #[tokio::test]
    async fn namespace_lookup_is_exact() {
        let ns = Namespace::new();
        let (tx, _rx) = mpsc::channel(1);
        ns.register("/jobs/a", tx).unwrap();
        assert!(ns.lookup("/jobs/a").is_some());
        // No prefix matching, no wildcards, no trailing-slash equivalence.
        assert!(ns.lookup("/jobs").is_none());
        assert!(ns.lookup("/jobs/a/").is_none());
        assert!(ns.lookup("/jobs/*").is_none());
        assert!(ns.lookup("/JOBS/A").is_none());
    }
}
