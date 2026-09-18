//! An identity read from a KV v2 secret.

use std::time::Duration;

use serde_json::Value;
use weida::{Identity, IdentitySource};

use crate::client::OpenBao;
use crate::{Error, Result};

/// Certificate chain and key stored as two fields of one KV v2 secret:
/// distribution and backup of an identity somebody made elsewhere, with no
/// rotation of its own (0032 §2). The secret holds the private key, which
/// is the trade this source is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Kv {
    /// The mount, `secret` by default.
    pub mount: String,
    /// The secret's path under the mount.
    pub path: String,
    /// The field holding the PEM chain, `certificate` by default.
    pub cert_field: String,
    /// The field holding the PEM key, `private_key` by default.
    pub key_field: String,
    /// How often the secret is read again; `None` reads it once. A changed
    /// key is a new peer, and the source says so.
    pub refresh: Option<Duration>,
}

impl Kv {
    /// `secret/data/<path>` with the default field names, read once.
    pub fn new(path: impl Into<String>) -> Kv {
        Kv {
            mount: "secret".into(),
            path: path.into(),
            cert_field: "certificate".into(),
            key_field: "private_key".into(),
            refresh: None,
        }
    }

    /// Reads the secret now and, with `refresh` set, keeps the returned
    /// source current for as long as the client lives.
    pub async fn start(self, client: OpenBao) -> Result<IdentitySource> {
        let identity = self.fetch(&client).await?;
        let source = IdentitySource::external(identity)?;
        if let Some(refresh) = self.refresh {
            let refreshing = source.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(refresh).await;
                    match self.fetch(&client).await {
                        Ok(identity) => {
                            if identity != refreshing.current() {
                                let _ = refreshing.update(identity);
                            }
                        }
                        Err(e) => refreshing.report_failure(e.to_string()),
                    }
                }
            });
        }
        Ok(source)
    }

    async fn fetch(&self, client: &OpenBao) -> Result<Identity> {
        let answer = client
            .get(&format!("{}/data/{}", self.mount, self.path))
            .await?;
        let data = answer
            .get("data")
            .and_then(|d| d.get("data"))
            .ok_or_else(|| Error::Shape("no `data.data` in the KV answer".into()))?;
        let field = |name: &str| {
            data.get(name)
                .and_then(Value::as_str)
                .map(|s| s.as_bytes().to_vec())
                .ok_or_else(|| Error::Shape(format!("no `{name}` field in the secret")))
        };
        Ok(Identity::from_pem(
            field(&self.cert_field)?,
            field(&self.key_field)?,
        ))
    }
}
