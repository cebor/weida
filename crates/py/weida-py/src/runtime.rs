//! The runtime, the reactor it owns, and the QUIC binding.
//!
//! # Where the reactor comes from
//!
//! `weida.Runtime()` builds [`weida::Runtime::owned`]: the reactor's threads
//! belong to this module, are named, are sized by `worker_threads` and die
//! with the last object that can cause work on them. A plain Python process
//! has no ambient Tokio runtime, so this is the only constructor that can
//! serve it — and it is the only one offered, because the alternatives mean
//! nothing here: `Runtime::new` needs an ambient reactor and
//! `Runtime::with_handle` needs a handle no Python caller can hold.
//!
//! **One reactor, not two.** The asyncio bridge drives this crate's futures
//! on the runtime's own executor ([`weida::Runtime::exec`]), which is why
//! that accessor exists: a bridge with an executor of its own would put two
//! Tokio runtimes in one process, the cost
//! [0013](../../../../docs/decisions/0013-competitor-libraries.md) §4.2
//! names for not sharing one.
//!
//! # Nothing blocks the event loop
//!
//! Every call that waits is a coroutine, and the waiting happens on the
//! reactor with the GIL released. `bind` is a coroutine for the same reason
//! the CLI's is: it creates a socket and a QUIC endpoint, and a `__new__`
//! that did that would be a constructor that blocks a loop.

use std::sync::Arc;

use pyo3::prelude::*;
use weida::{Runtime, RuntimeConfig};
use weida_py_core::Bridge;

use crate::endpoints::{PyPuller, PyPusher, PyReplier, PyRequester};
use crate::errors::{errno_of, raise, to_py};
use crate::pubsub::{PyPublisher, PySubscriber};
use crate::values::{PyIdentity, PyTrust};

/// `weida.Runtime`: a reactor of this module's own and the endpoints on it.
#[pyclass(frozen, name = "Runtime", module = "weida")]
pub struct PyRuntime {
    runtime: Arc<Runtime>,
    bridge: Bridge,
}

#[pymethods]
impl PyRuntime {
    /// A runtime with `worker_threads` reactor threads.
    ///
    /// # Errors
    ///
    /// `weida.Runtime` when `worker_threads` is `0` or the OS refuses the
    /// threads.
    #[new]
    #[pyo3(signature = (worker_threads=1))]
    fn new(py: Python<'_>, worker_threads: usize) -> PyResult<PyRuntime> {
        let config = RuntimeConfig {
            worker_threads,
            ..RuntimeConfig::default()
        };
        let runtime = raise(py, Runtime::owned(config))?;
        let bridge = Bridge::new(runtime.exec().clone(), to_py as _);
        Ok(PyRuntime {
            runtime: Arc::new(runtime),
            bridge,
        })
    }

    /// Binds a QUIC socket and returns the [`PyBinding`] its endpoints are
    /// registered on.
    ///
    /// `addr` is `host:port`; port `0` lets the kernel choose, and
    /// [`PyBinding::local_addr`] reads back what it chose.
    ///
    /// # Errors
    ///
    /// `weida.InvalidAddress` for a malformed address, `weida.Io` when the
    /// socket cannot be bound, `weida.Tls` when the identity cannot be used.
    fn bind<'py>(
        &self,
        py: Python<'py>,
        addr: &str,
        identity: PyIdentity,
    ) -> PyResult<Bound<'py, PyAny>> {
        let addr: std::net::SocketAddr = addr
            .parse()
            .map_err(|e| to_py(py, &errno_of(weida::Error::InvalidAddress(format!("{e}")))))?;
        let runtime = Arc::clone(&self.runtime);
        let bridge = self.bridge.clone();
        self.bridge.awaitable(py, async move {
            let listener = runtime.listener();
            let binding = listener
                .bind_quic(addr, identity.identity.clone())
                .await
                .map_err(errno_of)?;
            let fingerprint = identity
                .identity
                .fingerprint()
                .map_err(errno_of)?
                .to_string();
            Ok(PyBinding {
                _runtime: runtime,
                listener,
                local: binding.local_addr(),
                fingerprint,
                _binding: Arc::new(binding),
                bridge,
            })
        })
    }

    /// A requester on this runtime, dialling on `trust`'s terms.
    fn requester(&self, trust: PyTrust) -> PyRequester {
        PyRequester::new(
            self.runtime.requester(trust.trust.clone()),
            self.bridge.clone(),
            Arc::clone(&self.runtime),
        )
    }

    /// A pusher on this runtime.
    fn pusher(&self, trust: PyTrust) -> PyPusher {
        PyPusher::new(
            self.runtime.pusher(trust.trust.clone()),
            self.bridge.clone(),
            Arc::clone(&self.runtime),
        )
    }

    /// A subscriber on this runtime.
    fn subscriber(&self, trust: PyTrust) -> PySubscriber {
        PySubscriber::new(
            self.runtime.subscriber(trust.trust.clone()),
            self.bridge.clone(),
            Arc::clone(&self.runtime),
        )
    }

    /// Stops admitting work and waits up to `deadline` seconds for finished
    /// transfers to reach the peer's transport, returning
    /// `(delivered, outstanding)`.
    ///
    /// The deadline is mandatory and finite for the reason
    /// [0009](../../../../docs/decisions/0009-drain.md) §4.4 gives: waiting on
    /// a peer without one is how a process hangs at shutdown.
    fn drain<'py>(&self, py: Python<'py>, deadline: f64) -> PyResult<Bound<'py, PyAny>> {
        let seconds = std::time::Duration::try_from_secs_f64(deadline).map_err(|e| {
            to_py(
                py,
                &errno_of(weida::Error::Runtime(format!("deadline: {e}"))),
            )
        })?;
        // `Runtime` is `Clone` and cloning shares the same pool, bindings and
        // drain state, so draining a clone drains this runtime: the `self`
        // the Rust signature takes is a handle, not the runtime.
        let runtime = Runtime::clone(&self.runtime);
        self.bridge.awaitable(py, async move {
            let drained = runtime.drain(seconds).await;
            Ok((drained.delivered, drained.outstanding))
        })
    }

    fn __repr__(&self) -> String {
        "<weida.Runtime>".to_owned()
    }
}

/// `weida.Binding`: a bound QUIC socket, and the endpoints registered on it.
#[pyclass(frozen, name = "Binding", module = "weida")]
pub struct PyBinding {
    /// Held so the runtime outlives every endpoint taken from this binding.
    _runtime: Arc<Runtime>,
    /// Held so the socket stays bound: dropping the binding unbinds it.
    _binding: Arc<weida::Binding>,
    listener: weida::Listener,
    local: std::net::SocketAddr,
    fingerprint: String,
    bridge: Bridge,
}

#[pymethods]
impl PyBinding {
    /// The address the socket is bound to, with the port the kernel chose.
    fn local_addr(&self) -> String {
        self.local.to_string()
    }

    /// The fingerprint a client pins, in the `sha256:…` text form.
    fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// The address a client dials for `path`: the fingerprint, the socket and
    /// the path, which is the whole of a client's configuration.
    fn url(&self, path: &str) -> String {
        format!("weida://{}@{}{path}", self.fingerprint, self.local)
    }

    /// Registers a replier at `path`.
    ///
    /// # Errors
    ///
    /// `weida.AlreadyRegistered` when the path is taken,
    /// `weida.InvalidEndpointPath` for a malformed one.
    fn replier(&self, py: Python<'_>, path: &str) -> PyResult<PyReplier> {
        let replier = raise(py, self.listener.replier(path))?;
        Ok(PyReplier::new(
            replier,
            self.bridge.clone(),
            Arc::clone(&self._runtime),
        ))
    }

    /// Registers a puller at `path`.
    ///
    /// # Errors
    ///
    /// As [`PyBinding::replier`].
    fn puller(&self, py: Python<'_>, path: &str) -> PyResult<PyPuller> {
        let puller = raise(py, self.listener.puller(path))?;
        Ok(PyPuller::new(
            puller,
            self.bridge.clone(),
            Arc::clone(&self._runtime),
        ))
    }

    /// Registers a publisher at `path`.
    ///
    /// # Errors
    ///
    /// As [`PyBinding::replier`].
    fn publisher(&self, py: Python<'_>, path: &str) -> PyResult<PyPublisher> {
        let publisher = raise(py, self.listener.publisher(path))?;
        Ok(PyPublisher::new(
            publisher,
            self.bridge.clone(),
            Arc::clone(&self._runtime),
        ))
    }

    fn __repr__(&self) -> String {
        format!("<weida.Binding {}>", self.local)
    }
}
