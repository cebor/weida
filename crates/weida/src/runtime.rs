//! The process-level runtime: configuration, the client connection pool,
//! shutdown, and the one place this crate touches the async runtime.
//!
//! Every task, timer and name lookup in `weida` goes through [`Exec`]. That
//! is what lets a caller drive weida from an executor that is not Tokio:
//! `quinn` needs a Tokio reactor for its sockets and timers, nothing else
//! here does, so the reactor is an implementation detail the runtime owns
//! rather than an ambient requirement on every caller.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use quinn::VarInt;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use weida_core::{EndpointAddr, Error};
use weida_protocol::codes;

use crate::config::{ClientTls, RuntimeConfig};
use crate::conn::ConnHandle;
use crate::endpoint::{Endpoint, PushState, Pusher, ReqState, Requester, SubState, Subscriber};
use crate::listener::Listener;
use crate::pool::ClientPool;
use crate::stream::Peer;

/// The crate's whole surface onto the async runtime: tasks, timers and DNS.
///
/// An `Exec` is a Tokio handle and nothing more. It never owns the runtime,
/// so a task holding one can neither keep the runtime alive nor drop it from
/// inside itself. Cloning is a handle clone.
#[derive(Clone)]
pub(crate) struct Exec {
    handle: Handle,
}

impl Exec {
    pub(crate) fn from_handle(handle: Handle) -> Exec {
        Exec { handle }
    }

    /// The ambient handle, for [`Runtime::new`].
    pub(crate) fn current() -> Result<Exec, Error> {
        Handle::try_current()
            .map(Exec::from_handle)
            .map_err(|_| Error::Runtime("Runtime::new requires an ambient tokio runtime".into()))
    }

    /// Enters the runtime context, for the two `quinn` constructors that
    /// register a socket with the reactor. Held around the constructor only,
    /// never across an await.
    pub(crate) fn enter(&self) -> tokio::runtime::EnterGuard<'_> {
        self.handle.enter()
    }

    /// Spawns a task on the runtime. Works from any thread, with or without
    /// an ambient reactor — which is why nothing in this crate calls
    /// `tokio::spawn`.
    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// A timer on this runtime's wheel. The `Sleep` is created inside the
    /// runtime context, so the returned future may be awaited anywhere.
    pub(crate) fn sleep(&self, duration: Duration) -> tokio::time::Sleep {
        let _guard = self.handle.enter();
        tokio::time::sleep(duration)
    }

    /// Awaits `future`, giving up after `limit`.
    ///
    /// `None` means the future had not finished; it is dropped, so whatever
    /// it held is released. This is the only place the crate bounds an await
    /// on wall-clock time, for the same reason `sleep` lives here: the timer
    /// belongs to the runtime, not to the caller.
    pub(crate) async fn within<F: Future>(&self, limit: Duration, future: F) -> Option<F::Output> {
        let deadline = self.sleep(limit);
        tokio::select! {
            output = future => Some(output),
            () = deadline => None,
        }
    }

    /// Resolves `host:port` to every address the resolver offers, in its
    /// order, at most `max_addresses` of them.
    ///
    /// An address that is already an IP literal is not resolved at all: it is
    /// parsed in place, with no allocation, no task and no join handle. Every
    /// pinned deployment dials literals — the address carries the peer's
    /// fingerprint, not a name — and the round trip through the runtime cost
    /// 12 % of a cold handshake when it applied to them too
    /// (`docs/IMPLEMENTATION.md` §4, B-012, B-025).
    ///
    /// A real hostname keeps the task: `lookup_host` needs a Tokio context,
    /// and awaiting the join handle does not. All of the addresses are
    /// returned rather than the first, because the first is not necessarily
    /// reachable: `localhost` commonly resolves to both `::1` and
    /// `127.0.0.1`, and a server bound to one of them is unreachable through
    /// the other. The caller tries them in order (`pool::dial`). The count is
    /// capped because a resolver answer is remote input
    /// (`docs/INVARIANTS.md`).
    pub(crate) async fn resolve(
        &self,
        host: &str,
        port: u16,
        max_addresses: usize,
    ) -> Result<Vec<SocketAddr>, Error> {
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        let query = (host.to_owned(), port);
        let looked_up = self
            .spawn(async move {
                tokio::net::lookup_host(query)
                    .await
                    .map(|addrs| addrs.collect::<Vec<SocketAddr>>())
            })
            .await
            .map_err(|e| Error::Runtime(format!("name resolution task failed: {e}")))?;
        let addrs: Vec<SocketAddr> = looked_up
            .map_err(|e| Error::InvalidAddress(format!("cannot resolve {host}:{port}: {e}")))?
            .into_iter()
            .take(max_addresses)
            .collect();
        if addrs.is_empty() {
            return Err(Error::InvalidAddress(format!(
                "{host}:{port} resolved to no addresses"
            )));
        }
        Ok(addrs)
    }
}

/// Keeps a Tokio runtime created by [`Runtime::owned`] alive for as long as
/// the weida runtime that created it.
struct OwnedRuntime(Option<tokio::runtime::Runtime>);

impl Drop for OwnedRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            // The last `Runtime` clone may go out of scope on one of this
            // runtime's own worker threads. Dropping a Tokio runtime there
            // panics; shutting it down in the background does not.
            runtime.shutdown_background();
        }
    }
}

pub(crate) struct RuntimeInner {
    pub(crate) config: RuntimeConfig,
    pub(crate) pool: ClientPool,
    pub(crate) exec: Exec,
    /// Present only for a runtime created by [`Runtime::owned`]; dropped with
    /// the last handle.
    _owned: Option<OwnedRuntime>,
    /// Every QUIC endpoint this runtime owns, for shutdown.
    endpoints: Mutex<Vec<quinn::Endpoint>>,
    /// Duplicates suppressed by any connection this runtime owns.
    pub(crate) duplicates: Arc<AtomicU64>,
}

impl RuntimeInner {
    pub(crate) fn track_endpoint(&self, endpoint: quinn::Endpoint) {
        self.endpoints
            .lock()
            .expect("endpoint list poisoned")
            .push(endpoint);
    }

    /// Dials, or reuses a pooled connection to, `path` on `host:port` on
    /// `tls`'s terms, accepting only `expected` when the address named a peer.
    ///
    /// The path is part of the pool key: one connection per dialled endpoint
    /// path, so two paths on one peer cannot stall each other
    /// ([decisions/0002](../../../docs/decisions/0002-control-and-bulk-separation.md) §6.2).
    pub(crate) async fn connect(
        &self,
        addr: &EndpointAddr,
        tls: &Arc<ClientTls>,
    ) -> Result<ConnHandle, Error> {
        self.pool.connect(&self.config, &self.exec, addr, tls).await
    }

    /// The shared suppression counter, handed to every connection.
    pub(crate) fn duplicates(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.duplicates)
    }
}

/// A process-level execution and resource container.
///
/// Cloning shares the same pool, limits and bindings. Every task, timer and
/// name lookup the runtime needs goes through the Tokio handle it holds, so
/// only the runtime needs a reactor: a caller may drive weida futures on any
/// executor, including `futures::executor::block_on`.
///
/// Three ways to get one, differing only in where the reactor comes from:
/// [`Runtime::new`] borrows the ambient one, [`Runtime::with_handle`] takes a
/// handle to somebody else's, and [`Runtime::owned`] creates and owns one.
#[derive(Clone)]
pub struct Runtime {
    inner: Arc<RuntimeInner>,
}

impl Runtime {
    /// Creates a runtime on the current Tokio reactor.
    ///
    /// Fails when there is none: `quinn` needs one, and failing here beats
    /// failing later at an unrelated call site.
    pub fn new(config: RuntimeConfig) -> Result<Runtime, Error> {
        Ok(Runtime::from_parts(config, Exec::current()?, None))
    }

    /// Creates a runtime on the Tokio runtime `handle` names.
    ///
    /// For a process that already runs a reactor somewhere other than the
    /// calling thread: nothing has to be ambient, and the caller's own
    /// executor is never consulted.
    pub fn with_handle(handle: tokio::runtime::Handle, config: RuntimeConfig) -> Runtime {
        Runtime::from_parts(config, Exec::from_handle(handle), None)
    }

    /// Creates a runtime that **owns** a multi-thread Tokio runtime with
    /// [`RuntimeConfig::worker_threads`] workers.
    ///
    /// The caller needs no reactor of its own, now or later: weida's tasks,
    /// timers and name lookups run on the owned runtime while the caller
    /// drives the futures it awaits on whatever executor it likes. The owned
    /// runtime is shut down when the last clone of this `Runtime` — and every
    /// endpoint made from it — is dropped.
    ///
    /// Fails when `worker_threads` is `0`, or when the OS refuses the threads.
    pub fn owned(config: RuntimeConfig) -> Result<Runtime, Error> {
        if config.worker_threads == 0 {
            return Err(Error::Runtime(
                "RuntimeConfig::worker_threads must be at least 1".into(),
            ));
        }
        let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(config.worker_threads)
            .thread_name("weida")
            .build()
            .map_err(Error::Io)?;
        let exec = Exec::from_handle(tokio_runtime.handle().clone());
        Ok(Runtime::from_parts(
            config,
            exec,
            Some(OwnedRuntime(Some(tokio_runtime))),
        ))
    }

    fn from_parts(config: RuntimeConfig, exec: Exec, owned: Option<OwnedRuntime>) -> Runtime {
        // One counter per runtime: the pool hands it to the connections it
        // dials, the listener to the connections it accepts.
        let duplicates = Arc::new(AtomicU64::new(0));
        Runtime {
            inner: Arc::new(RuntimeInner {
                config,
                pool: ClientPool::new(Arc::clone(&duplicates)),
                exec,
                _owned: owned,
                endpoints: Mutex::new(Vec::new()),
                duplicates,
            }),
        }
    }

    /// Duplicates suppressed by every connection this runtime owns.
    ///
    /// The counterpart of `Publisher::dropped` on the receiving side: a
    /// message the peer sent twice inside the negotiated
    /// `Deduplication::Bounded` window was read, thrown away and counted
    /// here, and the application never saw it. Zero unless deduplication is
    /// negotiated, because nothing is suppressed without it.
    pub fn suppressed_duplicates(&self) -> u64 {
        self.inner.duplicates.load(Ordering::Relaxed)
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
            self.inner.config.endpoint_queue,
        ))
    }

    /// Closes every binding and pooled connection, then waits — for at most
    /// [`RuntimeConfig::shutdown_timeout`] — for the sockets to go idle, so
    /// peers see a clean `SHUTDOWN` rather than a timeout.
    ///
    /// The close is **abortive**: a transfer still in flight is reset, and one
    /// whose FIN is queued but unacknowledged may never arrive. That is what
    /// `Runtime::shutdown` has always meant
    /// (`docs/decisions/0009-drain.md` §4.1); giving a finished transfer its
    /// chance is a drain, which is a different operation.
    ///
    /// The wait is bounded on purpose. Without a bound its length is decided
    /// by the path — QUIC's closing and draining periods last about three
    /// times the current probe timeout, which grows with round-trip time and
    /// loss — so a process that must exit within a budget of its own could not
    /// use it [0009 §4.4].
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

        // One budget for the whole shutdown, not one per endpoint: what a
        // caller cares about is when `shutdown` returns.
        let idle = async {
            for endpoint in endpoints.iter().chain(client.iter()) {
                endpoint.wait_idle().await;
            }
        };
        let deadline = self.inner.exec.sleep(self.inner.config.shutdown_timeout);
        tokio::select! {
            () = idle => {}
            () = deadline => tracing::debug!(
                timeout_ms = self.inner.config.shutdown_timeout.as_millis(),
                "shutdown timeout reached before the sockets went idle"
            ),
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
    use weida_core::Limits;

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

    #[test]
    fn zero_worker_threads_is_rejected() {
        let err = Runtime::owned(RuntimeConfig {
            worker_threads: 0,
            ..RuntimeConfig::default()
        })
        .unwrap_err();
        assert!(matches!(err, Error::Runtime(_)), "{err:?}");
    }

    /// The whole point of `owned`: no ambient reactor, on this thread or any
    /// other, and weida still binds a socket and shuts it down.
    #[test]
    fn an_owned_runtime_needs_no_ambient_reactor() {
        assert!(Handle::try_current().is_err());
        let rt = Runtime::owned(RuntimeConfig::default()).expect("owned runtime");
        let listener = rt.listener();
        futures::executor::block_on(async {
            let binding = listener
                .bind_quic(
                    "127.0.0.1:0".parse().unwrap(),
                    Identity::generate().expect("identity"),
                )
                .await
                .expect("bind");
            assert_ne!(binding.local_addr().port(), 0);
            rt.shutdown().await;
        });
    }

    /// `with_handle` takes somebody else's reactor; the calling thread still
    /// has none.
    #[test]
    fn with_handle_uses_the_handed_runtime() {
        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        let rt = Runtime::with_handle(tokio_rt.handle().clone(), RuntimeConfig::default());
        assert!(Handle::try_current().is_err());
        assert_eq!(rt.config().worker_threads, 1);
        assert_eq!(rt.requester(no_trust()).peer_count(), 0);
    }

    #[tokio::test]
    async fn resolves_ip_literals_without_dns() {
        let exec = Exec::current().expect("ambient runtime");
        assert_eq!(
            exec.resolve("127.0.0.1", 7443, 8).await.unwrap(),
            vec![SocketAddr::from(([127, 0, 0, 1], 7443))]
        );
        let v6 = exec.resolve("::1", 7443, 8).await.unwrap();
        assert_eq!(v6.len(), 1);
        assert_eq!(v6[0].port(), 7443);
        assert!(v6[0].is_ipv6());
    }

    /// Claim: a hostname yields every address the resolver offers, in its
    /// order and no more than the cap. `localhost` is the case that matters —
    /// it commonly resolves to both `::1` and `127.0.0.1`, and dialling only
    /// the first reaches a server bound to the other never.
    #[tokio::test]
    async fn a_hostname_resolves_to_every_address_up_to_the_cap() {
        let exec = Exec::current().expect("ambient runtime");
        let all = exec.resolve("localhost", 7443, 8).await.unwrap();
        assert!(!all.is_empty());
        assert!(all.iter().all(|a| a.port() == 7443));

        let capped = exec.resolve("localhost", 7443, 1).await.unwrap();
        assert_eq!(capped.len(), 1, "the cap must bound the answer");
        assert_eq!(capped[0], all[0], "and it must keep the resolver's order");
    }

    #[tokio::test]
    async fn an_unresolvable_host_is_an_address_error() {
        let exec = Exec::current().expect("ambient runtime");
        let err = exec.resolve("host.invalid.", 7443, 8).await.unwrap_err();
        assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_runtime_clone_shares_its_configuration() {
        let rt = Runtime::new(RuntimeConfig::default()).unwrap();
        let clone = rt.clone();
        assert_eq!(clone.config().limits, rt.config().limits);
        assert_eq!(clone.requester(no_trust()).peer_count(), 0);
        rt.shutdown().await;
    }

    /// Claim: a hostname whose first address is unreachable still connects,
    /// because the dialling path tries the rest.
    ///
    /// This is the debt entry that said "use the IP literal until address
    /// selection learns to try more than one". On a host where `localhost`
    /// resolves to `::1` before `127.0.0.1` — the common Linux ordering, and
    /// this machine's — a server bound to `127.0.0.1` was simply unreachable
    /// by name. The address is pinned so trust does not depend on the name: a
    /// pin consults no hostname.
    #[tokio::test]
    async fn a_hostname_whose_first_address_is_unreachable_still_connects() {
        let identity = Identity::generate().expect("identity");
        let fingerprint = identity.fingerprint().expect("fingerprint");

        let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
        let listener = server.listener();
        let binding = listener
            .bind_quic("127.0.0.1:0".parse().unwrap(), identity)
            .await
            .expect("bind");
        let _puller = listener.puller("/sink").expect("puller");
        let url = format!(
            "weida://{fingerprint}@localhost:{}/sink",
            binding.local_addr().port()
        );

        let client = Runtime::new(RuntimeConfig::default()).expect("client runtime");
        let pusher = client.pusher(Trust::by_address());
        pusher.connect(&url).await.expect("connect by name");
        assert_eq!(pusher.peer_count(), 1);

        client.shutdown().await;
        server.shutdown().await;
    }

    /// Claim: `shutdown`'s wait for idle sockets is bounded by
    /// `shutdown_timeout`, so process exit never waits on somebody else's
    /// network.
    ///
    /// The peer is made unreachable the hard way: the server runtime is
    /// dropped without being shut down, which closes its socket and sends
    /// nothing, so the client's `CONNECTION_CLOSE` is answered by silence and
    /// the close runs out its draining period — about three times the path's
    /// probe timeout, ~96 ms on loopback and longer as round-trip time and
    /// loss grow.
    ///
    /// The assertion is **relative**, and measured in the same run: the same
    /// shutdown with a 1 ms cap must be at least twice as fast as one with a
    /// cap far beyond the draining period. An absolute millisecond bound would
    /// pin this machine; an unbounded wait — the defect this defends against —
    /// makes the two times equal and fails it.
    #[tokio::test]
    async fn the_wait_for_idle_sockets_is_bounded() {
        async fn shutdown_with(cap: Duration) -> Duration {
            let identity = Identity::generate().expect("identity");
            let trust = Trust::pin(identity.fingerprint().expect("fingerprint"));

            let server = Runtime::new(RuntimeConfig::default()).expect("server runtime");
            let listener = server.listener();
            let binding = listener
                .bind_quic("127.0.0.1:0".parse().unwrap(), identity)
                .await
                .expect("bind");
            let url = format!("weida://127.0.0.1:{}/sink", binding.local_addr().port());
            let puller = listener.puller("/sink").expect("puller");

            let client = Runtime::new(RuntimeConfig {
                shutdown_timeout: cap,
                ..RuntimeConfig::default()
            })
            .expect("client runtime");
            let pusher = client.pusher(trust);
            pusher.connect(&url).await.expect("connect");

            // The peer stops existing without saying so.
            drop(pusher);
            drop(puller);
            drop(binding);
            drop(listener);
            drop(server);

            let start = std::time::Instant::now();
            client.shutdown().await;
            start.elapsed()
        }

        let capped = shutdown_with(Duration::from_millis(1)).await;
        let uncapped = shutdown_with(Duration::from_secs(10)).await;
        assert!(
            capped * 2 < uncapped,
            "the cap must bite: capped {capped:?} against uncapped {uncapped:?}"
        );
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
            limits: Limits {
                idle_timeout: std::time::Duration::from_millis(200),
                ..Limits::default()
            },
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
