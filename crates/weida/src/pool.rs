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
use weida_core::Error;

use crate::config::RuntimeConfig;
use crate::conn::{ConnCtx, ConnHandle, conn_error};
use crate::listener::Namespace;
use crate::tls;

pub(crate) struct ClientPool {
    state: Mutex<PoolState>,
}

#[derive(Default)]
struct PoolState {
    endpoint: Option<quinn::Endpoint>,
    connections: HashMap<(String, u16), ConnHandle>,
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

    /// Returns a live connection to `host:port`, dialling if necessary.
    ///
    /// The returned handle has completed the HELLO exchange, so callers never
    /// have to think about negotiation.
    pub(crate) async fn connect(
        &self,
        config: &RuntimeConfig,
        host: &str,
        port: u16,
    ) -> Result<ConnHandle, Error> {
        let key = (host.to_owned(), port);
        let mut state = self.state.lock().await;

        if let Some(existing) = state.connections.get(&key) {
            if existing.conn.close_reason().is_none() {
                return Ok(ConnHandle::clone(existing));
            }
            state.connections.remove(&key);
        }

        let client_tls = config
            .client_tls
            .as_ref()
            .ok_or_else(|| Error::Tls("RuntimeConfig::client_tls is required to connect".into()))?;

        let endpoint = match &state.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                let endpoint = bind_client_endpoint()?;
                state.endpoint = Some(endpoint.clone());
                endpoint
            }
        };

        let client_config = tls::client_config(
            client_tls,
            &config.limits,
            config.keep_alive,
            config.idle_timeout,
        )?;

        let addr = resolve(host, port).await?;
        tracing::debug!(%addr, host, "dialling");
        let conn = endpoint
            .connect_with(client_config, addr, host)
            .map_err(|e| Error::Transport(format!("connect to {addr} failed: {e}")))?
            .await
            .map_err(conn_error)?;

        // A fresh namespace per client connection: a subscriber registers its
        // path here so fanned-out copies have somewhere to go. It is not the
        // listener's namespace — a client serves nothing on its own account.
        let handle = ConnCtx::spawn(conn, config.limits, Arc::new(Namespace::new()), None);
        // Negotiation must complete before the caller can send anything: a
        // DATA frame ahead of our own HELLO would be parked by the peer, and a
        // version mismatch must fail `connect`, not the first request.
        handle.negotiated().await?;

        state.connections.insert(key, ConnHandle::clone(&handle));
        Ok(handle)
    }
}

/// Binds the shared client socket, preferring dual-stack IPv6.
fn bind_client_endpoint() -> Result<quinn::Endpoint, Error> {
    let v6 = SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0));
    match quinn::Endpoint::client(v6) {
        Ok(endpoint) => Ok(endpoint),
        Err(e) => {
            tracing::debug!(error = %e, "IPv6 client socket unavailable; falling back to IPv4");
            quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))).map_err(Error::Io)
        }
    }
}

/// Resolves `host:port`, preferring an address family the client socket can
/// reach.
async fn resolve(host: &str, port: u16) -> Result<SocketAddr, Error> {
    let mut addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| Error::InvalidAddress(format!("cannot resolve {host}:{port}: {e}")))?;
    addrs
        .next()
        .ok_or_else(|| Error::InvalidAddress(format!("{host}:{port} resolved to no addresses")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn resolves_ip_literals_without_dns() {
        assert_eq!(
            resolve("127.0.0.1", 7443).await.unwrap(),
            SocketAddr::from(([127, 0, 0, 1], 7443))
        );
        let v6 = resolve("::1", 7443).await.unwrap();
        assert_eq!(v6.port(), 7443);
        assert!(v6.is_ipv6());
    }

    #[tokio::test]
    async fn an_unresolvable_host_is_an_address_error() {
        let err = resolve("host.invalid.", 7443).await.unwrap_err();
        assert!(matches!(err, Error::InvalidAddress(_)), "{err:?}");
    }

    #[tokio::test]
    async fn the_client_socket_binds() {
        let endpoint = bind_client_endpoint().unwrap();
        assert_ne!(endpoint.local_addr().unwrap().port(), 0);
    }
}
