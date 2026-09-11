//! The one surface onto the async runtime: tasks, timers and DNS.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use weida_core::Error;

/// A library's whole surface onto the async runtime: tasks, timers and DNS.
///
/// An `Exec` is a Tokio handle and nothing more. It never owns the runtime,
/// so a task holding one can neither keep the runtime alive nor drop it from
/// inside itself. Cloning is a handle clone.
///
/// **Why a type rather than plain `tokio::spawn`.** A library that calls
/// `tokio::spawn`, `tokio::time` or `tokio::net::lookup_host` directly
/// demands that *its caller* be inside a Tokio reactor, on the very thread
/// that called it. Holding an `Exec` moves that requirement into the library:
/// the reactor is wherever the `Exec` points, the caller may drive the
/// returned futures on any executor — `futures::executor::block_on`
/// included — and a grep for those three names over the library's own `src`
/// is the proof that no path escaped
/// (`docs/ARCHITECTURE.md` §5).
#[derive(Clone)]
pub struct Exec {
    handle: Handle,
}

impl Exec {
    /// Wraps the runtime `handle` names.
    ///
    /// For a process that already runs a reactor somewhere other than the
    /// calling thread: nothing has to be ambient, and the caller's own
    /// executor is never consulted.
    pub fn from_handle(handle: Handle) -> Exec {
        Exec { handle }
    }

    /// The ambient handle.
    ///
    /// Fails when the calling thread is not inside a Tokio runtime. Failing
    /// here — at construction — beats failing later at an unrelated call
    /// site.
    pub fn current() -> Result<Exec, Error> {
        Handle::try_current()
            .map(Exec::from_handle)
            .map_err(|_| Error::Runtime("no ambient tokio runtime to run on".into()))
    }

    /// Creates a multi-thread Tokio runtime with `worker_threads` workers,
    /// named `thread_name`, and returns a handle onto it beside the
    /// [`OwnedReactor`] that keeps it alive.
    ///
    /// This is the constructor for a library whose users have no reactor at
    /// all, which is most of a synchronous protocol library's audience: the
    /// library owns the reactor its sockets and timers need, and the caller
    /// keeps its own executor — or none. The reactor dies with the returned
    /// [`OwnedReactor`], so a library holds it beside every handle it hands
    /// out and drops it last.
    ///
    /// Fails when `worker_threads` is `0`, or when the OS refuses the
    /// threads.
    pub fn owned(worker_threads: usize, thread_name: &str) -> Result<(Exec, OwnedReactor), Error> {
        if worker_threads == 0 {
            return Err(Error::Runtime("worker_threads must be at least 1".into()));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(worker_threads)
            .thread_name(thread_name)
            .build()
            .map_err(Error::Io)?;
        let exec = Exec::from_handle(runtime.handle().clone());
        Ok((exec, OwnedReactor(Some(runtime))))
    }

    /// Enters the runtime context, for the constructors that register a
    /// socket with the reactor as they are built — `quinn`'s endpoints,
    /// `tokio::net`'s listeners. Held around the constructor only, never
    /// across an await.
    pub fn enter(&self) -> tokio::runtime::EnterGuard<'_> {
        self.handle.enter()
    }

    /// Spawns a task on the runtime. Works from any thread, with or without
    /// an ambient reactor — which is why a library holding an `Exec` never
    /// calls `tokio::spawn`.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// A timer on this runtime's wheel. The `Sleep` is created inside the
    /// runtime context, so the returned future may be awaited anywhere.
    ///
    /// Every clock a protocol has is one of these: a reconnect interval, a
    /// handshake deadline, a heartbeat, a connect timeout, a send or receive
    /// timeout, a linger budget.
    pub fn sleep(&self, duration: Duration) -> tokio::time::Sleep {
        let _guard = self.handle.enter();
        tokio::time::sleep(duration)
    }

    /// Awaits `future`, giving up after `limit`.
    ///
    /// `None` means the future had not finished; it is dropped, so whatever
    /// it held is released. This is the only place an await is bounded on
    /// wall-clock time, for the same reason [`Exec::sleep`] lives here: the
    /// timer belongs to the runtime, not to the caller.
    pub async fn within<F: Future>(&self, limit: Duration, future: F) -> Option<F::Output> {
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
    /// and awaiting the join handle does not. That is also what keeps a
    /// stalled resolver from stalling the library — libzmq's named
    /// architectural mistake was synchronous DNS, where "when DNS was
    /// unavailable, the whole library, including the sockets that haven't
    /// used DNS, just hung" (`docs/research/zeromq.md` §13).
    ///
    /// All of the addresses are returned rather than the first, because the
    /// first is not necessarily reachable: `localhost` commonly resolves to
    /// both `::1` and `127.0.0.1`, and a server bound to one of them is
    /// unreachable through the other. The caller tries them in order.
    ///
    /// `max_addresses` is mandatory and is the caller's number, not a
    /// constant here: a resolver answer is remote input, so the count is
    /// capped (`docs/INVARIANTS.md`), and where that cap comes from belongs
    /// to the consumer's own configuration rather than to this crate.
    pub async fn resolve(
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

impl std::fmt::Debug for Exec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Exec").finish_non_exhaustive()
    }
}

/// Keeps a Tokio runtime created by [`Exec::owned`] alive for as long as
/// whatever created it.
///
/// Hold it beside every handle onto that runtime and drop it last. Dropping
/// it shuts the runtime down **in the background** rather than blocking,
/// because the last owner may go out of scope on one of that runtime's own
/// worker threads, where dropping a Tokio runtime panics.
pub struct OwnedReactor(Option<tokio::runtime::Runtime>);

impl Drop for OwnedReactor {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            // The last owner may go out of scope on one of this runtime's own
            // worker threads. Dropping a Tokio runtime there panics; shutting
            // it down in the background does not.
            runtime.shutdown_background();
        }
    }
}

impl std::fmt::Debug for OwnedReactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedReactor").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_exec_without_an_ambient_reactor_fails() {
        let err = Exec::current().unwrap_err();
        assert!(matches!(err, Error::Runtime(_)), "{err:?}");
    }

    #[test]
    fn zero_worker_threads_is_rejected() {
        let err = Exec::owned(0, "test").unwrap_err();
        assert!(matches!(err, Error::Runtime(_)), "{err:?}");
    }

    /// The whole point of `owned`: no ambient reactor, on this thread or any
    /// other, and the library's tasks still run — driven here by an executor
    /// that is not Tokio at all.
    #[test]
    fn an_owned_reactor_needs_no_ambient_one() {
        assert!(Handle::try_current().is_err());
        let (exec, reactor) = Exec::owned(1, "test").expect("owned reactor");
        let joined = exec.spawn(async { 7u8 });
        assert_eq!(futures::executor::block_on(joined).expect("task"), 7);
        drop(reactor);
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
    async fn within_gives_up_on_a_future_that_never_finishes() {
        let exec = Exec::current().expect("ambient runtime");
        assert!(
            exec.within(Duration::from_millis(1), std::future::pending::<()>())
                .await
                .is_none()
        );
        assert_eq!(
            exec.within(Duration::from_secs(30), async { 3 }).await,
            Some(3)
        );
    }
}
