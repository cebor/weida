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
use crate::patterns::{PyBusMember, PyDish, PyPaired, PyRadio, PyRespondent, PySurveyor};
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
    /// `datagrams=True` enables datagram flows on this runtime's connections
    /// (`Limits::datagram_receive_bytes` at its documented 64 KiB), which a
    /// dish needs for datagram segments; both ends must enable it.
    /// `path_report=True` sends this side's view of each QUIC path to the
    /// peer every 2 s, and reads the peer's as `ConnectionStats.remote`;
    /// both ends must enable it too.
    ///
    /// # Errors
    ///
    /// `weida.Runtime` when `worker_threads` is `0` or the OS refuses the
    /// threads.
    #[new]
    #[pyo3(signature = (worker_threads=1, datagrams=false, path_report=false))]
    fn new(
        py: Python<'_>,
        worker_threads: usize,
        datagrams: bool,
        path_report: bool,
    ) -> PyResult<PyRuntime> {
        let config = RuntimeConfig {
            worker_threads,
            limits: crate::limits_with(datagrams, path_report),
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
            let local = binding.local_addr();
            Ok(PyBinding {
                _runtime: runtime,
                listener,
                local: local.to_string(),
                fingerprint: Some(fingerprint.clone()),
                url_base: format!("weida://{fingerprint}@{local}"),
                _bound: Held::Quic {
                    _binding: Arc::new(binding),
                },
                bridge,
            })
        })
    }

    /// Binds an in-process bus, which a client **in this process** dials as
    /// `weida+inproc://<bus>/<path>`; no socket and no identity.
    ///
    /// # Errors
    ///
    /// `weida.AlreadyRegistered` when the bus is taken in this process.
    fn bind_inproc(&self, py: Python<'_>, bus: &str) -> PyResult<PyBinding> {
        let listener = self.runtime.listener();
        let binding = listener
            .bind_inproc(bus)
            .map_err(|e| to_py(py, &errno_of(e)))?;
        Ok(PyBinding {
            _runtime: Arc::clone(&self.runtime),
            listener,
            local: bus.to_owned(),
            fingerprint: None,
            url_base: format!("weida+inproc://{bus}"),
            _bound: Held::Inproc {
                _binding: Arc::new(binding),
            },
            bridge: self.bridge.clone(),
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

    /// A dialling pair on this runtime.
    ///
    /// One type for both roles, as in the Rust API: what distinguishes a
    /// dialling pair from a bound one is which of them calls `connect`.
    fn pair(&self, trust: PyTrust) -> PyPaired {
        PyPaired::new(
            self.runtime.pair(trust.trust.clone()),
            self.bridge.clone(),
            Arc::clone(&self.runtime),
        )
    }

    /// A surveyor on this runtime.
    fn surveyor(&self, trust: PyTrust) -> PySurveyor {
        PySurveyor::new(
            self.runtime.surveyor(trust.trust.clone()),
            self.bridge.clone(),
            Arc::clone(&self.runtime),
        )
    }

    /// A dish on this runtime.
    fn dish(&self, trust: PyTrust) -> PyDish {
        PyDish::new(
            self.runtime.dish(trust.trust.clone()),
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

/// What a [`PyBinding`] keeps bound; dropping it unbinds.
enum Held {
    Quic { _binding: Arc<weida::Binding> },
    Inproc { _binding: Arc<weida::LocalBinding> },
}

/// `weida.Binding`: a bound QUIC socket or in-process bus, and the endpoints
/// registered on it.
#[pyclass(frozen, name = "Binding", module = "weida")]
pub struct PyBinding {
    /// Held so the runtime outlives every endpoint taken from this binding.
    _runtime: Arc<Runtime>,
    /// Held so the socket or bus stays bound.
    _bound: Held,
    listener: weida::Listener,
    /// The socket address with the port the kernel chose, or the bus name.
    local: String,
    /// The fingerprint a client pins; `None` on an in-process bus.
    fingerprint: Option<String>,
    /// What a client's URL starts with, before the path.
    url_base: String,
    bridge: Bridge,
}

#[pymethods]
impl PyBinding {
    /// The address the socket is bound to, with the port the kernel chose;
    /// the bus name for an in-process binding.
    fn local_addr(&self) -> &str {
        &self.local
    }

    /// The fingerprint a client pins, in the `sha256:…` text form; `None`
    /// for an in-process binding, which the process itself vouches for.
    fn fingerprint(&self) -> Option<&str> {
        self.fingerprint.as_deref()
    }

    /// The address a client dials for `path`, which is the whole of a
    /// client's configuration.
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.url_base)
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

    /// Registers a bound pair at `path`.
    ///
    /// It has nothing to dial: the peer comes to it, and the first peer is
    /// the one it keeps.
    ///
    /// # Errors
    ///
    /// As [`PyBinding::replier`].
    fn pair(&self, py: Python<'_>, path: &str) -> PyResult<PyPaired> {
        let paired = raise(py, self.listener.pair(path))?;
        Ok(PyPaired::new(
            paired,
            self.bridge.clone(),
            Arc::clone(&self._runtime),
        ))
    }

    /// Registers a respondent at `path`.
    ///
    /// # Errors
    ///
    /// As [`PyBinding::replier`].
    fn respondent(&self, py: Python<'_>, path: &str) -> PyResult<PyRespondent> {
        let respondent = raise(py, self.listener.respondent(path))?;
        Ok(PyRespondent::new(
            respondent,
            self.bridge.clone(),
            Arc::clone(&self._runtime),
        ))
    }

    /// Registers a bus member at `path`, dialling other members on `trust`'s
    /// terms.
    ///
    /// A member is bound **and** dialling: it accepts here and joins others
    /// with `connect`, which is why this one takes a trust and the other
    /// registrations do not.
    ///
    /// # Errors
    ///
    /// As [`PyBinding::replier`].
    fn bus(&self, py: Python<'_>, path: &str, trust: PyTrust) -> PyResult<PyBusMember> {
        let member = raise(py, self.listener.bus(path, trust.trust.clone()))?;
        Ok(PyBusMember::new(
            member,
            self.bridge.clone(),
            Arc::clone(&self._runtime),
        ))
    }

    /// Registers a radio at `path`.
    ///
    /// # Errors
    ///
    /// As [`PyBinding::replier`].
    fn radio(&self, py: Python<'_>, path: &str) -> PyResult<PyRadio> {
        let radio = raise(py, self.listener.radio(path))?;
        Ok(PyRadio::new(radio, Arc::clone(&self._runtime)))
    }

    fn __repr__(&self) -> String {
        format!("<weida.Binding {}>", self.local)
    }
}
