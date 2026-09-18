//! The HTTP client: one base address, one token, JSON in and out.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde_json::Value;
use tokio_rustls::rustls;
use weida::Pem;

use crate::auth::{Auth, TokenInfo};
use crate::{Error, Result};

/// How to reach OpenBao.
#[derive(Clone, Debug)]
pub struct Config {
    /// `https://bao.example:8200` or, for a dev server, `http://…`. The
    /// value of `BAO_ADDR`.
    pub address: String,
    /// The CA that issued OpenBao's own certificate (`BAO_CACERT`). `None`
    /// trusts the public roots, which is right for a public certificate and
    /// wrong for an internal CA.
    pub ca: Option<Pem>,
    /// An enterprise-style namespace, sent as `X-Vault-Namespace`. Rarely
    /// set on OpenBao.
    pub namespace: Option<String>,
    /// Per-request timeout.
    pub timeout: Duration,
}

impl Config {
    /// Reaches `address` with the public roots and a 10 s timeout.
    pub fn new(address: impl Into<String>) -> Config {
        Config {
            address: address.into(),
            ca: None,
            namespace: None,
            timeout: Duration::from_secs(10),
        }
    }

    /// From the environment the `bao` CLI reads: `BAO_ADDR` (falling back to
    /// `VAULT_ADDR`), `BAO_CACERT`, `BAO_NAMESPACE`.
    pub fn from_env() -> Result<Config> {
        let address = std::env::var("BAO_ADDR")
            .or_else(|_| std::env::var("VAULT_ADDR"))
            .map_err(|_| Error::Http("neither BAO_ADDR nor VAULT_ADDR is set".into()))?;
        let mut config = Config::new(address);
        if let Ok(path) = std::env::var("BAO_CACERT").or_else(|_| std::env::var("VAULT_CACERT")) {
            config.ca = Some(Pem::File(path.into()));
        }
        if let Ok(namespace) =
            std::env::var("BAO_NAMESPACE").or_else(|_| std::env::var("VAULT_NAMESPACE"))
        {
            config.namespace = Some(namespace);
        }
        Ok(config)
    }
}

/// What the client holds once logged in.
struct Session {
    token: String,
    info: TokenInfo,
}

/// An OpenBao client: a base address and the token of the moment.
///
/// Cloning shares the token, so the renewal task and every source built on
/// the client see the same session.
#[derive(Clone)]
pub struct OpenBao {
    http: reqwest::Client,
    base: String,
    namespace: Option<String>,
    session: Arc<RwLock<Option<Session>>>,
}

impl OpenBao {
    /// A client for `config`, not yet authenticated.
    ///
    /// Must be created inside a Tokio runtime: the HTTP client registers
    /// with the reactor as it is built.
    pub fn new(config: Config) -> Result<OpenBao> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        match &config.ca {
            Some(pem) => {
                use rustls_pki_types::pem::PemObject as _;
                let certs: Vec<rustls_pki_types::CertificateDer<'static>> = match pem {
                    Pem::Bytes(bytes) => rustls_pki_types::CertificateDer::pem_slice_iter(bytes)
                        .collect::<std::result::Result<_, _>>()
                        .map_err(|e| Error::Tls(format!("parsing the CA: {e}")))?,
                    Pem::File(path) => rustls_pki_types::CertificateDer::pem_file_iter(path)
                        .map_err(|e| Error::Tls(format!("reading the CA: {e}")))?
                        .collect::<std::result::Result<_, _>>()
                        .map_err(|e| Error::Tls(format!("parsing the CA: {e}")))?,
                };
                for cert in certs {
                    roots
                        .add(cert)
                        .map_err(|e| Error::Tls(format!("adding the CA: {e}")))?;
                }
            }
            None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        }
        let tls = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| Error::Tls(e.to_string()))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let http = reqwest::Client::builder()
            .use_preconfigured_tls(tls)
            .timeout(config.timeout)
            .build()
            .map_err(|e| Error::Http(e.to_string()))?;
        Ok(OpenBao {
            http,
            base: config.address.trim_end_matches('/').to_owned(),
            namespace: config.namespace,
            session: Arc::new(RwLock::new(None)),
        })
    }

    /// Authenticates, replacing any previous session, and keeps the token
    /// renewed for as long as this client — or any clone — lives.
    ///
    /// For [`Auth::Handoff`] this is the redemption: it runs before any
    /// other request this client makes, and its failure is
    /// [`Error::HandoffStolen`], on which the caller should exit (0032 §2).
    pub async fn login(&self, auth: Auth) -> Result<TokenInfo> {
        let (token, info) = auth.login(self).await?;
        *self.session.write().expect("session poisoned") = Some(Session {
            token,
            info: info.clone(),
        });
        if info.renewable {
            let client = self.clone();
            tokio::spawn(async move { client.renew_forever().await });
        }
        Ok(info)
    }

    /// The token accessor of the current session, for a controller that
    /// revokes by accessor at process exit.
    pub fn accessor(&self) -> Option<String> {
        self.session
            .read()
            .expect("session poisoned")
            .as_ref()
            .and_then(|s| s.info.accessor.clone())
    }

    /// Sets the session without starting a renewal: for a login that needs
    /// a token to ask about itself.
    pub(crate) fn install(&self, token: String, info: TokenInfo) {
        *self.session.write().expect("session poisoned") = Some(Session { token, info });
    }

    /// What the current session knows about its token.
    pub fn token_info(&self) -> Option<TokenInfo> {
        self.session
            .read()
            .expect("session poisoned")
            .as_ref()
            .map(|s| s.info.clone())
    }

    fn token(&self) -> Result<String> {
        self.session
            .read()
            .expect("session poisoned")
            .as_ref()
            .map(|s| s.token.clone())
            .ok_or(Error::Unauthenticated)
    }

    /// Renews the session token at half its TTL, retrying on failure, until
    /// the client is gone. A renewal that reports a shorter TTL is followed
    /// at that pace; a token that stops being renewable ends the loop.
    async fn renew_forever(self) {
        loop {
            let ttl = match self.token_info() {
                Some(info) if info.renewable => info.ttl,
                _ => return,
            };
            let wait = ttl
                .checked_div(2)
                .unwrap_or(Duration::from_secs(1))
                .max(Duration::from_secs(1));
            tokio::time::sleep(wait).await;
            if Arc::strong_count(&self.session) == 1 {
                // Only this task holds the session: the client is gone.
                return;
            }
            match self.renew_self().await {
                Ok(info) => {
                    if let Some(session) = self.session.write().expect("session poisoned").as_mut()
                    {
                        session.info = info;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "openbao token renewal failed; retrying");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// `auth/token/renew-self` for the current session.
    pub async fn renew_self(&self) -> Result<TokenInfo> {
        let answer = self
            .post("auth/token/renew-self", Value::Object(Default::default()))
            .await?;
        TokenInfo::from_auth(&answer)
    }

    /// `GET /v1/<path>`.
    pub async fn get(&self, path: &str) -> Result<Value> {
        let request = self.http.get(self.url(path));
        self.send(request, Some(self.token()?)).await
    }

    /// `POST /v1/<path>` with a JSON body.
    pub async fn post(&self, path: &str, body: Value) -> Result<Value> {
        let request = self.http.post(self.url(path)).json(&body);
        self.send(request, Some(self.token()?)).await
    }

    /// `POST /v1/<path>` asking OpenBao to **wrap** the answer: what comes
    /// back is a wrapping token, redeemable once within `wrap_ttl`, and the
    /// real answer waits in the cubbyhole behind it. The controller's half of
    /// the hand-off (0032 §2): `post_wrapped("auth/token/create", …, 5 s)`
    /// mints the credential a service redeems with [`Auth::Handoff`].
    pub async fn post_wrapped(
        &self,
        path: &str,
        body: Value,
        wrap_ttl: Duration,
    ) -> Result<String> {
        let request = self.http.post(self.url(path)).json(&body).header(
            "X-Vault-Wrap-TTL",
            format!("{}s", wrap_ttl.as_secs().max(1)),
        );
        let answer = self.send(request, Some(self.token()?)).await?;
        answer
            .get("wrap_info")
            .and_then(|w| w.get("token"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| Error::Shape("no `wrap_info.token`: the answer was not wrapped".into()))
    }

    /// `auth/token/revoke-accessor`: the controller's other half, at the
    /// service's exit. Revokes the token and every lease under it without
    /// ever having held the token.
    pub async fn revoke_accessor(&self, accessor: &str) -> Result<()> {
        self.post(
            "auth/token/revoke-accessor",
            serde_json::json!({ "accessor": accessor }),
        )
        .await
        .map(|_| ())
    }

    /// `POST /v1/<path>` with a JSON body and **no** token: logins.
    pub(crate) async fn post_anonymous(&self, path: &str, body: Value) -> Result<Value> {
        let request = self.http.post(self.url(path)).json(&body);
        self.send(request, None).await
    }

    /// `sys/wrapping/unwrap` with `wrapping_token` as the token: the one
    /// request a wrapping token is good for. A token that was already
    /// redeemed — or never existed — is [`Error::HandoffStolen`].
    pub async fn unwrap(&self, wrapping_token: &str) -> Result<Value> {
        let request = self.http.post(self.url("sys/wrapping/unwrap"));
        match self.send(request, Some(wrapping_token.to_owned())).await {
            Ok(answer) => Ok(answer),
            Err(Error::Api { status, errors })
                if status == 400
                    && errors
                        .iter()
                        .any(|e| e.contains("wrapping token is not valid")) =>
            {
                Err(Error::HandoffStolen)
            }
            Err(e) => Err(e),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/v1/{}", self.base, path.trim_start_matches('/'))
    }

    async fn send(&self, request: reqwest::RequestBuilder, token: Option<String>) -> Result<Value> {
        let mut request = request.header("X-Vault-Request", "true");
        if let Some(token) = token {
            request = request.header("X-Vault-Token", token);
        }
        if let Some(namespace) = &self.namespace {
            request = request.header("X-Vault-Namespace", namespace);
        }
        let response = request
            .send()
            .await
            .map_err(|e| Error::Http(e.to_string()))?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(|e| Error::Http(e.to_string()))?;
        if !(200..300).contains(&status) {
            let errors = serde_json::from_slice::<Value>(&body)
                .ok()
                .and_then(|v| {
                    v.get("errors")?.as_array().map(|a| {
                        a.iter()
                            .filter_map(|e| e.as_str().map(str::to_owned))
                            .collect()
                    })
                })
                .unwrap_or_else(|| vec![String::from_utf8_lossy(&body).into_owned()]);
            return Err(Error::Api { status, errors });
        }
        if body.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&body).map_err(|e| Error::Shape(format!("not JSON: {e}")))
    }
}

impl std::fmt::Debug for OpenBao {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenBao")
            .field("base", &self.base)
            .field(
                "authenticated",
                &self.session.read().expect("session poisoned").is_some(),
            )
            .finish_non_exhaustive()
    }
}
