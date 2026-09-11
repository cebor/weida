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
use weida_protocol::header::GuaranteeSet;

use crate::config::ServerTls;
use crate::conn::ConnCtx;
use crate::endpoint::{Endpoint, PubState, Publisher, PullState, Puller, RepState, Replier};
use crate::inproc;
use crate::pubsub::SubRegistry;
use crate::runtime::{Exec, RuntimeInner, Shared};
use crate::stream::{Acceptor, Incoming};
use crate::tls;
use crate::transfer::{IncomingRequest, IncomingTransfer};
use crate::transport::Link;

/// What a registered endpoint path does with an inbound transfer.
pub(crate) enum Route {
    /// A replier: inbound exchanges become [`IncomingRequest`]s.
    Request(mpsc::Sender<IncomingRequest>),
    /// A puller or subscriber: inbound one-way transfers are queued as-is.
    Transfer(mpsc::Sender<IncomingTransfer>),
    /// A raw L0 acceptor: both stream kinds are queued, tagged.
    Raw(mpsc::Sender<Incoming>),
    /// A publisher. Nothing inbound is accepted on this path; the entry exists
    /// so the path is claimed exclusively and so SUBSCRIBE can be answered for
    /// a path that really is served here.
    Pub,
}

/// Endpoint path to accept-queue map.
///
/// A plain `RwLock<HashMap>`, not an async lock: lookup is a hash and a channel
/// clone, and the guard is never held across an `await`.
pub(crate) struct Namespace {
    routes: RwLock<HashMap<Arc<str>, Route>>,
}

impl Namespace {
    pub(crate) fn new() -> Namespace {
        Namespace {
            routes: RwLock::new(HashMap::new()),
        }
    }

    /// Looks a path up. The path is compared byte for byte: it is an opaque
    /// identifier, so there is no splitting, prefix matching or wildcarding
    /// (master doc §4, §81 rule 5).
    ///
    /// Returns a clone of the route's sender rather than a guard, so the lock
    /// is never held across an `await`.
    pub(crate) fn lookup(&self, path: &str) -> Option<Route> {
        self.routes
            .read()
            .expect("namespace lock poisoned")
            .get(path)
            .map(Route::clone_sender)
    }

    pub(crate) fn register(&self, path: &str, route: Route) -> Result<(), Error> {
        let mut routes = self.routes.write().expect("namespace lock poisoned");
        match routes.entry(Arc::from(path)) {
            Entry::Occupied(_) => Err(Error::AlreadyRegistered),
            Entry::Vacant(slot) => {
                slot.insert(route);
                Ok(())
            }
        }
    }

    /// Releases a path so the same endpoint can be registered again.
    ///
    /// Used by subscriber teardown: a `Sub` claims its path on the pooled
    /// client connection and must give it back, or a later `Sub` on the same
    /// connection would fail with [`Error::AlreadyRegistered`] forever.
    pub(crate) fn unregister(&self, path: &str) {
        self.routes
            .write()
            .expect("namespace lock poisoned")
            .remove(path);
    }
}

impl Route {
    fn clone_sender(&self) -> Route {
        match self {
            Route::Request(tx) => Route::Request(tx.clone()),
            Route::Transfer(tx) => Route::Transfer(tx.clone()),
            Route::Raw(tx) => Route::Raw(tx.clone()),
            Route::Pub => Route::Pub,
        }
    }
}

pub(crate) struct ListenerInner {
    pub(crate) runtime: Arc<RuntimeInner>,
    pub(crate) namespace: Arc<Namespace>,
    pub(crate) subs: Arc<SubRegistry>,
}

/// One externally reachable messaging namespace.
///
/// A Listener is a namespace, not a socket and not a transport: it owns the
/// endpoint routing table and nothing else (master doc §3). Credentials belong
/// to the individual bindings, because they are transport-specific — a QUIC
/// binding needs a certificate and key, a future in-process or adapter binding
/// needs neither — and because two interfaces of one service may legitimately
/// present different certificates.
#[derive(Clone)]
pub struct Listener {
    inner: Arc<ListenerInner>,
}

impl Listener {
    pub(crate) fn new(runtime: Arc<RuntimeInner>) -> Listener {
        let limits = runtime.config.limits;
        let ordering = runtime.config.guarantees.ordering;
        Listener {
            inner: Arc::new(ListenerInner {
                runtime,
                namespace: Arc::new(Namespace::new()),
                subs: Arc::new(SubRegistry::new(limits, ordering)),
            }),
        }
    }

    /// Adds a native QUIC binding with its own server identity.
    ///
    /// `tls` may be a bare [`crate::Identity`] — the binding then accepts
    /// anonymous peers — or a [`ServerTls`] that also requires clients to
    /// present an identity it trusts. The material is loaded and validated
    /// here, so a misconfigured certificate fails before the socket serves
    /// anything. Pass port `0` to let the OS choose; read it back with
    /// [`Binding::local_addr`].
    ///
    /// Several bindings may serve the same Listener, each with its own
    /// identity: the endpoints they expose are the same, the identities they
    /// present need not be.
    pub async fn bind_quic(
        &self,
        addr: SocketAddr,
        tls: impl Into<ServerTls>,
    ) -> Result<Binding, Error> {
        let tls = tls.into();
        let limits = self.inner.runtime.config.limits;
        let server_config =
            tls::server_config(&tls, &limits, self.inner.runtime.config.idle_timeout)?;
        let exec = self.inner.runtime.exec.clone();

        // Inside the runtime context: `quinn` registers the socket with the
        // reactor as it is constructed, and the calling thread may have none.
        let endpoint = {
            let _guard = exec.enter();
            quinn::Endpoint::server(server_config, addr).map_err(Error::Io)?
        };
        let local_addr = endpoint.local_addr().map_err(Error::Io)?;
        self.inner.runtime.track_endpoint(endpoint.clone());

        exec.spawn(accept_connections(
            endpoint.clone(),
            Arc::clone(&self.inner.namespace),
            Arc::clone(&self.inner.subs),
            limits,
            exec.clone(),
            self.inner.runtime.config.guarantees,
            self.inner.runtime.shared(),
        ));

        tracing::info!(%local_addr, "quic binding listening");
        Ok(Binding {
            endpoint,
            local_addr,
        })
    }

    /// Binds this Listener's endpoints on an in-process bus.
    ///
    /// The counterpart of [`Listener::bind_quic`] for the transport that has
    /// no socket: `weida+inproc://<bus>/<path>` reaches the same endpoints
    /// from the same process, with the same frames, the same HELLO and the
    /// same negotiation, and without TLS or credentials
    /// (`docs/PROTOCOL.md` §2.1,
    /// [decisions/0010](../../../docs/decisions/0010-local-transport.md)
    /// §4.1). The bus name is at most 256 bytes and unique within the
    /// process; binding a name twice fails with
    /// [`Error::AlreadyRegistered`].
    pub fn bind_inproc(&self, bus: &str) -> Result<LocalBinding, Error> {
        let incoming = inproc::bind(bus)?;
        let exec = self.inner.runtime.exec.clone();
        exec.spawn(accept_local(
            incoming,
            Arc::clone(&self.inner.namespace),
            Arc::clone(&self.inner.subs),
            self.inner.runtime.config.limits,
            exec.clone(),
            self.inner.runtime.config.guarantees,
            self.inner.runtime.shared(),
        ));
        tracing::info!(bus, "inproc binding listening");
        Ok(LocalBinding {
            bus: bus.to_owned(),
        })
    }

    /// Registers a replier for `path`.
    ///
    /// The path must be a valid endpoint path and must not already be
    /// registered on this listener.
    pub fn replier(&self, path: &str) -> Result<Replier, Error> {
        validate_endpoint_path(path)?;
        let (tx, rx) = mpsc::channel(self.inner.runtime.config.limits.endpoint_queue);
        self.inner.namespace.register(path, Route::Request(tx))?;
        Ok(Endpoint::from_state(RepState::new(path, rx)))
    }

    /// Registers a puller for `path`.
    ///
    /// Pull binds and Push connects, mirroring Rep/Req. The path must be a
    /// valid endpoint path and must not already be registered on this
    /// listener, whatever pattern claimed it.
    pub fn puller(&self, path: &str) -> Result<Puller, Error> {
        validate_endpoint_path(path)?;
        let (tx, rx) = mpsc::channel(self.inner.runtime.config.limits.endpoint_queue);
        self.inner.namespace.register(path, Route::Transfer(tx))?;
        Ok(Endpoint::from_state(PullState::new(path, rx)))
    }

    /// Registers a publisher for `path`.
    ///
    /// Pub binds and Sub connects. Nothing inbound is accepted on the path —
    /// the registration claims it exclusively — and subscriptions that arrived
    /// before this call are already recorded, so a publisher created after its
    /// subscribers still reaches them.
    pub fn publisher(&self, path: &str) -> Result<Publisher, Error> {
        validate_endpoint_path(path)?;
        self.inner.namespace.register(path, Route::Pub)?;
        Ok(Endpoint::from_state(PubState::new(
            path,
            Arc::clone(&self.inner.subs),
            self.inner.runtime.config.limits.subscriber_buffer_bytes,
        )))
    }

    /// Registers a raw L0 acceptor for `path`.
    ///
    /// Below the patterns: an acceptor takes both stream kinds on one path and
    /// hands them over tagged ([`crate::Incoming`]), leaving the shape of the
    /// conversation to the application. The path is claimed exactly like a
    /// replier's or a puller's.
    pub fn acceptor(&self, path: &str) -> Result<Acceptor, Error> {
        validate_endpoint_path(path)?;
        let (tx, rx) = mpsc::channel(self.inner.runtime.config.limits.endpoint_queue);
        self.inner.namespace.register(path, Route::Raw(tx))?;
        Ok(Acceptor::new(path, rx))
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

/// One in-process binding: a bus name and nothing else.
///
/// No socket, no TLS and no credentials — there is nothing to configure and
/// nobody to prove ([decisions/0010](../../../docs/decisions/0010-local-transport.md)
/// §4.1, §4.4). Dropping it unregisters the bus, after which a dial to that
/// name fails as a dial to a closed port does.
#[derive(Debug)]
pub struct LocalBinding {
    bus: String,
}

impl LocalBinding {
    /// The bus this binding answers on.
    pub fn bus(&self) -> &str {
        &self.bus
    }
}

impl Drop for LocalBinding {
    fn drop(&mut self) {
        inproc::unbind(&self.bus);
    }
}

/// Accepts in-process connections until the bus is unbound.
async fn accept_local(
    mut incoming: mpsc::UnboundedReceiver<inproc::LocalConn>,
    namespace: Arc<Namespace>,
    subs: Arc<SubRegistry>,
    limits: Limits,
    exec: Exec,
    guarantees: GuaranteeSet,
    shared: Arc<Shared>,
) {
    while let Some(conn) = incoming.recv().await {
        if shared.drain.is_draining() {
            // Admission stopped: the same rule as on a QUIC binding
            // (`docs/decisions/0009-drain.md` §4.5).
            conn.close(codes::SHUTDOWN, "runtime draining");
            continue;
        }
        let namespace = Arc::clone(&namespace);
        let subs = Arc::clone(&subs);
        let exec_for_conn = exec.clone();
        let shared = Arc::clone(&shared);
        exec.spawn(async move {
            let ctx = ConnCtx::spawn(
                Link::Local(conn),
                limits,
                namespace,
                Some(Arc::clone(&subs)),
                exec_for_conn,
                guarantees,
                shared,
            );
            let conn_id = ctx.conn.stable_id();
            let reason = ctx.conn.closed().await;
            subs.remove_connection(conn_id);
            tracing::debug!(%reason, "local connection closed");
        });
    }
}

fn shutdown_code() -> VarInt {
    VarInt::from_u32(codes::SHUTDOWN as u32)
}

/// Accepts connections until the endpoint is closed.
async fn accept_connections(
    endpoint: quinn::Endpoint,
    namespace: Arc<Namespace>,
    subs: Arc<SubRegistry>,
    limits: Limits,
    exec: Exec,
    guarantees: GuaranteeSet,
    shared: Arc<Shared>,
) {
    let live = Arc::new(AtomicUsize::new(0));
    while let Some(incoming) = endpoint.accept().await {
        // A draining runtime admits nothing new: the handshake is refused
        // outright rather than accepted and then closed
        // (`docs/decisions/0009-drain.md` §4.5).
        if shared.drain.is_draining() {
            incoming.refuse();
            continue;
        }
        if live.load(Ordering::Relaxed) >= limits.max_connections {
            // Complete the handshake, then say why: a bare refusal leaves the
            // peer unable to distinguish overload from a routing mistake.
            tracing::warn!(
                max = limits.max_connections,
                "connection limit reached; refusing"
            );
            exec.spawn(async move {
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
        let subs = Arc::clone(&subs);
        let live = Arc::clone(&live);
        live.fetch_add(1, Ordering::Relaxed);
        let exec_for_conn = exec.clone();
        let shared = Arc::clone(&shared);
        exec.spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    let remote = conn.remote_address();
                    let conn_id = conn.stable_id();
                    tracing::debug!(%remote, "connection accepted");
                    // The handle must outlive the connection: it owns the
                    // actor's control channel.
                    let _ctx = ConnCtx::spawn(
                        Link::Quic(conn.clone()),
                        limits,
                        namespace,
                        Some(Arc::clone(&subs)),
                        exec_for_conn,
                        guarantees,
                        shared,
                    );
                    let reason = conn.closed().await;
                    // A peer that goes away takes its subscriptions with it;
                    // otherwise connection churn would grow the registry.
                    subs.remove_connection(conn_id);
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
        ns.register("/a", Route::Request(tx.clone())).unwrap();
        assert!(matches!(
            ns.register("/a", Route::Request(tx.clone())),
            Err(Error::AlreadyRegistered)
        ));
        // The pattern behind the path is irrelevant: a path is claimed once.
        assert!(matches!(
            ns.register("/a", Route::Pub),
            Err(Error::AlreadyRegistered)
        ));
        ns.register("/b", Route::Request(tx)).unwrap();
    }

    #[tokio::test]
    async fn namespace_lookup_is_exact() {
        let ns = Namespace::new();
        let (tx, _rx) = mpsc::channel(1);
        ns.register("/jobs/a", Route::Request(tx)).unwrap();
        assert!(ns.lookup("/jobs/a").is_some());
        // No prefix matching, no wildcards, no trailing-slash equivalence.
        assert!(ns.lookup("/jobs").is_none());
        assert!(ns.lookup("/jobs/a/").is_none());
        assert!(ns.lookup("/jobs/*").is_none());
        assert!(ns.lookup("/JOBS/A").is_none());
    }

    #[tokio::test]
    async fn unregistering_releases_the_path() {
        let ns = Namespace::new();
        let (tx, _rx) = mpsc::channel(1);
        ns.register("/s", Route::Transfer(tx)).unwrap();
        ns.unregister("/s");
        assert!(ns.lookup("/s").is_none());
        // Unregistering an unknown path is not an error.
        ns.unregister("/s");
        ns.register("/s", Route::Pub).unwrap();
    }
}
