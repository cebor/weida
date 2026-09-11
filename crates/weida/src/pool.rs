//! Client connection pool.
//!
//! One QUIC endpoint per runtime and one connection per `host:port`. Pooling is
//! keyed on the authority as written rather than on the resolved address: the
//! TLS server name comes from the same string, so two spellings of one address
//! are genuinely different peers as far as authentication is concerned.

use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use tokio::sync::Mutex;
use weida_core::{Error, Fingerprint};

use crate::config::{ClientTls, RuntimeConfig};
use crate::conn::{ConnCtx, ConnHandle, conn_error};
use crate::listener::Namespace;
use crate::runtime::Exec;
use crate::tls;

pub(crate) struct ClientPool {
    state: Mutex<PoolState>,
}

/// Keyed by authority, trust configuration **and** the fingerprint the
/// address named. Two endpoints dialling the same `host:port` on different
/// terms must not share a connection: the peer was authenticated on one set
/// of terms, not both.
type PoolKey = (String, u16, Arc<ClientTls>, Option<Fingerprint>);

#[derive(Default)]
struct PoolState {
    endpoint: Option<quinn::Endpoint>,
    connections: HashMap<PoolKey, ConnHandle>,
}

impl ClientPool {
    pub(crate) fn new() -> ClientPool {
        ClientPool {
            state: Mutex::new(PoolState::default()),
        }
    }

    /// Removes and returns the client endpoint, for shutdown.
    pub(crate) async fn take_endpoint(&self) -> Option<quinn::Endpoint> {
        let mut state = self.state.lock().await;
        state.connections.clear();
        state.endpoint.take()
    }

    /// Returns a live connection to `host:port` authenticated on `tls`'s
    /// terms — and, when the address named one, as `expected` — dialling if
    /// necessary.
    ///
    /// The returned handle has completed the HELLO exchange, so callers never
    /// have to think about negotiation.
    pub(crate) async fn connect(
        &self,
        config: &RuntimeConfig,
        exec: &Exec,
        host: &str,
        port: u16,
        tls: &Arc<ClientTls>,
        expected: Option<Fingerprint>,
    ) -> Result<ConnHandle, Error> {
        let key: PoolKey = (host.to_owned(), port, Arc::clone(tls), expected);
        let mut state = self.state.lock().await;

        if let Some(existing) = state.connections.get(&key) {
            if existing.conn.close_reason().is_none() {
                return Ok(ConnHandle::clone(existing));
            }
            state.connections.remove(&key);
        }

        let endpoint = match &state.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                let endpoint = bind_client_endpoint(exec)?;
                state.endpoint = Some(endpoint.clone());
                endpoint
            }
        };

        let (client_config, refused) = tls::client_config(
            tls,
            expected,
            &config.limits,
            config.keep_alive,
            config.idle_timeout,
        )?;

        let addr = exec.resolve(host, port).await?;
        tracing::debug!(%addr, host, "dialling");
        let connecting = {
            // Inside the runtime context: `quinn` reads its own runtime out
            // of the ambient reactor while it builds the attempt.
            let _guard = exec.enter();
            endpoint
                .connect_with(client_config, addr, host)
                .map_err(|e| Error::Transport(format!("connect to {addr} failed: {e}")))?
        };
        // The handshake is driven *on* the runtime rather than by whoever
        // awaits `connect`: completing it spawns the connection driver, and
        // the caller's executor need not be tokio. Awaiting the join handle
        // needs nothing.
        let handshake = exec
            .spawn(connecting)
            .await
            .map_err(|e| Error::Runtime(format!("dial task failed: {e}")))?;
        let conn = match handshake {
            Ok(conn) => conn,
            Err(e) => {
                // The verifier saw the peer before the handshake died: report
                // who answered, so the operator can decide whether to pin it.
                let refused = refused.lock().expect("refusal record poisoned").take();
                return Err(match refused {
                    Some(presented) => Error::Untrusted(presented),
                    None => conn_error(e),
                });
            }
        };

        // A fresh namespace per client connection: a subscriber registers its
        // path here so fanned-out copies have somewhere to go. It is not the
        // listener's namespace — a client serves nothing on its own account.
        let handle = ConnCtx::spawn(
            conn,
            config.limits,
            Arc::new(Namespace::new()),
            None,
            exec.clone(),
        );
        // Negotiation must complete before the caller can send anything: a
        // DATA frame ahead of our own HELLO would be parked by the peer, and a
        // version mismatch must fail `connect`, not the first request.
        handle.negotiated().await?;

        state.connections.insert(key, ConnHandle::clone(&handle));
        Ok(handle)
    }
}

/// Binds the shared client socket, preferring dual-stack IPv6.
///
/// Inside the runtime context: `quinn` registers the socket with the reactor
/// as it is constructed, and the caller's thread may have none.
fn bind_client_endpoint(exec: &Exec) -> Result<quinn::Endpoint, Error> {
    let _guard = exec.enter();
    let v6 = SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0));
    match quinn::Endpoint::client(v6) {
        Ok(endpoint) => Ok(endpoint),
        Err(e) => {
            tracing::debug!(error = %e, "IPv6 client socket unavailable; falling back to IPv4");
            quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))).map_err(Error::Io)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_client_socket_binds() {
        let exec = Exec::current().expect("ambient runtime");
        let endpoint = bind_client_endpoint(&exec).unwrap();
        assert_ne!(endpoint.local_addr().unwrap().port(), 0);
    }
}
