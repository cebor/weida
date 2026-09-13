//! The values a caller hands in or reads back: trust, identity, metadata.
//!
//! Each is a class rather than a string, for the reason the Rust API has
//! types for them: `Trust::by_address()` and `Trust::anchor_file(path)` are
//! different decisions about who may answer, and a binding that took a
//! string would have to invent a grammar for that difference
//! ([PATTERNS.md](../../../../docs/PATTERNS.md) §1.9).

use std::path::PathBuf;

use pyo3::prelude::*;
use pyo3::types::PyType;
use weida::{Fingerprint, Identity, IncomingMeta, Trust};

use crate::errors::raise;

/// `weida.Trust`: whom a dialling endpoint accepts.
///
/// Three constructors, one per answer the Rust API has, and no default: a
/// caller states the trust it wants, because "whatever the peer presents" is
/// not among the choices.
#[pyclass(frozen, from_py_object, name = "Trust", module = "weida")]
#[derive(Clone)]
pub struct PyTrust {
    pub(crate) trust: Trust,
    shape: &'static str,
}

#[pymethods]
impl PyTrust {
    /// Accept exactly the key the address names (`weida://sha256:…@host/path`).
    ///
    /// The common client case, and the one that needs no file: the address is
    /// the whole configuration.
    #[classmethod]
    fn by_address(_class: &Bound<'_, PyType>) -> PyTrust {
        PyTrust {
            trust: Trust::by_address(),
            shape: "by_address",
        }
    }

    /// Accept exactly `fingerprint`, whatever the address says.
    ///
    /// # Errors
    ///
    /// `weida.InvalidFingerprint` unless the text is `sha256:` and 64 hex
    /// digits.
    #[classmethod]
    fn pin(_class: &Bound<'_, PyType>, py: Python<'_>, fingerprint: &str) -> PyResult<PyTrust> {
        let fingerprint: Fingerprint = raise(py, fingerprint.parse())?;
        Ok(PyTrust {
            trust: Trust::pin(fingerprint),
            shape: "pin",
        })
    }

    /// Trust a certificate authority from a PEM file, and verify the host the
    /// address names against the certificate it presents.
    #[classmethod]
    fn anchor_file(_class: &Bound<'_, PyType>, path: PathBuf) -> PyTrust {
        PyTrust {
            trust: Trust::anchor_file(path),
            shape: "anchor_file",
        }
    }

    fn __repr__(&self) -> String {
        format!("<weida.Trust {}>", self.shape)
    }
}

/// `weida.Identity`: the key and certificate an endpoint answers with.
///
/// A weida peer *is* its public key, so an identity is the whole of a
/// server's configuration and its fingerprint is what a client pins
/// ([0008](../../../../docs/decisions/0008-session-identity.md)).
#[pyclass(frozen, from_py_object, name = "Identity", module = "weida")]
#[derive(Clone)]
pub struct PyIdentity {
    pub(crate) identity: Identity,
}

#[pymethods]
impl PyIdentity {
    /// A fresh self-signed identity held only in memory.
    ///
    /// Made for pinning: the certificate carries no names, so it can only be
    /// trusted by fingerprint. A process that restarts gets a new one and
    /// therefore a new address — `from_pem_file` is how an address stays
    /// stable.
    #[classmethod]
    fn generate(_class: &Bound<'_, PyType>, py: Python<'_>) -> PyResult<PyIdentity> {
        Ok(PyIdentity {
            identity: raise(py, Identity::generate())?,
        })
    }

    /// A fresh identity whose certificate also names `names`, so a client
    /// trusting the certificate as an anchor can verify the host it dialled.
    #[classmethod]
    fn generate_for(
        _class: &Bound<'_, PyType>,
        py: Python<'_>,
        names: Vec<String>,
    ) -> PyResult<PyIdentity> {
        Ok(PyIdentity {
            identity: raise(py, Identity::generate_for(names))?,
        })
    }

    /// An identity loaded from a PEM file holding the certificate chain and
    /// the private key.
    #[classmethod]
    fn from_pem_file(_class: &Bound<'_, PyType>, path: PathBuf) -> PyIdentity {
        PyIdentity {
            identity: Identity::from_pem_file(path),
        }
    }

    /// The SHA-256 fingerprint of the public key, in the `sha256:…` text form
    /// an address carries.
    ///
    /// # Errors
    ///
    /// `weida.Tls` when the material cannot be read or parsed — which is why
    /// this is a method and not a field: a `from_pem_file` identity is read
    /// lazily, so a corrupt file fails here, with the path in hand.
    fn fingerprint(&self, py: Python<'_>) -> PyResult<String> {
        Ok(raise(py, self.identity.fingerprint())?.to_string())
    }

    fn __repr__(&self) -> String {
        "<weida.Identity>".to_owned()
    }
}

/// `weida.IncomingMeta`: what the DATA header said about one arrival.
#[pyclass(
    frozen,
    get_all,
    skip_from_py_object,
    name = "IncomingMeta",
    module = "weida"
)]
#[derive(Clone)]
pub struct PyIncomingMeta {
    /// The endpoint path the stream was addressed to, when it carried one.
    pub endpoint: Option<String>,
    /// The advisory content length the sender declared, which is **not** a
    /// guarantee: the payload ends at FIN.
    pub content_len: Option<u64>,
    /// The content type the sender declared, uninterpreted here.
    pub content_type: Option<String>,
    /// The Pub/Sub topic, on a published copy.
    pub topic: Option<String>,
    /// The peer, as the **transport** proved it: `sha256:…` over QUIC,
    /// `uid=…` over a local socket, or `None` for an anonymous client and for
    /// an in-process peer. Never from a header, so it can be authorized on
    /// and not claimed ([0015](../../../../docs/decisions/0015-peer-authorization.md)).
    pub peer: Option<String>,
    /// The producer's sequence number for this transfer, under `PerProducer`
    /// ordering.
    pub sequence: Option<u64>,
    /// How many messages this arrival is known to have missed, under
    /// `PerProducer` detect: the honest report of a fan-out drop.
    pub missed: Option<u64>,
    /// The W3C `traceparent` of this transfer, for a caller that propagates a
    /// trace.
    pub traceparent: Option<String>,
}

impl PyIncomingMeta {
    /// The metadata of one arrival, flattened into the Python shape.
    pub fn of(meta: &IncomingMeta) -> PyIncomingMeta {
        PyIncomingMeta {
            endpoint: meta.endpoint.clone(),
            content_len: meta.content_len,
            content_type: meta.content_type.clone(),
            topic: meta.topic.clone(),
            peer: meta.peer.as_ref().map(ToString::to_string),
            sequence: meta.sequence,
            missed: meta.gap.as_ref().map(|gap| gap.missed()),
            traceparent: meta.trace.map(|trace| trace.to_traceparent()),
        }
    }
}

#[pymethods]
impl PyIncomingMeta {
    fn __repr__(&self) -> String {
        format!(
            "<weida.IncomingMeta endpoint={:?} topic={:?} peer={:?} bytes={:?}>",
            self.endpoint, self.topic, self.peer, self.content_len
        )
    }
}
