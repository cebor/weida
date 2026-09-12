//! The context: the reactor, the `inproc://` namespace and the socket ceiling.
//!
//! [0013](../../../../docs/decisions/0013-competitor-libraries.md) §4.4 gives
//! the context three constructors, differing only in where the reactor comes
//! from, and all three are here because all three mean something to a Python
//! process:
//!
//! | Python | Rust | What it is for |
//! | --- | --- | --- |
//! | `Context()` / `Context.owned()` | `Context::owned` | The default, and the only one a plain Python program can use: the context owns a reactor sized by `worker_threads`, and no asyncio loop is involved in driving it. |
//! | `Context.current()` | `Context::new` | The *ambient* Tokio reactor. A plain Python process has none, so this raises `EMTHREAD` — which is the honest answer rather than a hidden second reactor. It is reachable when this module is imported into a process that a Rust host already runs a reactor in. |
//! | `Context.sharing(other)` | `Context::with_handle` | A second context on the first one's reactor: two `inproc://` namespaces and two socket ceilings over one thread pool. |
//!
//! A context is *shared*, not copied: two Python `Context` objects built from
//! the same Rust context see one namespace and one ceiling, exactly as
//! libzmq's "two contexts are two separate ZeroMQ instances" implies about
//! one.

use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyType;
use weida_py_core::Bridge;
use weida_zmq::{ContextConfig, Error};

use crate::errors;

/// `weida_zmq.Context`.
#[pyclass(frozen, name = "Context", module = "weida_zmq")]
pub struct Context {
    inner: weida_zmq::Context,
    bridge: Bridge,
}

impl Context {
    /// The library context every socket of this module is opened on.
    pub fn inner(&self) -> &weida_zmq::Context {
        &self.inner
    }

    /// The reactor and errno mapping every socket of this context inherits.
    pub fn bridge(&self) -> &Bridge {
        &self.bridge
    }

    fn wrap(inner: weida_zmq::Context) -> Context {
        let bridge = Bridge::new(inner.exec().clone(), errors::to_py);
        Context { inner, bridge }
    }
}

/// The same configuration, as a Python failure: what `weida_zmq.sync`'s
/// context takes, so that the two contexts are configured by one function.
pub(crate) fn configuration(
    py: Python<'_>,
    options: Option<&crate::options::PyContextOptions>,
    max_sockets: Option<usize>,
    worker_threads: Option<usize>,
    close_budget: Option<f64>,
) -> PyResult<ContextConfig> {
    errors::raise(
        py,
        config(options, max_sockets, worker_threads, close_budget),
    )
}

/// Builds the library's configuration, refusing a value at configuration time
/// rather than rounding it into something usable.
fn config(
    options: Option<&crate::options::PyContextOptions>,
    max_sockets: Option<usize>,
    worker_threads: Option<usize>,
    close_budget: Option<f64>,
) -> Result<ContextConfig, Error> {
    // The option table first, the keyword arguments over it: two spellings of
    // the same three numbers, and the one written at the call site wins.
    let mut config = options.map(|o| o.library()).unwrap_or_default();
    if let Some(max_sockets) = max_sockets {
        config.max_sockets = max_sockets;
    }
    if let Some(worker_threads) = worker_threads {
        config.worker_threads = worker_threads;
    }
    if let Some(seconds) = close_budget {
        if !seconds.is_finite() || seconds < 0.0 {
            return Err(Error::EINVAL(
                format!(
                    "close_budget must be a finite, non-negative number of seconds, not {seconds}"
                )
                .into(),
            ));
        }
        config.close_budget = Duration::from_secs_f64(seconds);
    }
    Ok(config)
}

#[pymethods]
impl Context {
    /// A context that owns its reactor: `Context::owned`.
    ///
    /// `close_budget` is `ZMQ_LINGER` as a number of seconds, and it is
    /// **finite by default** (one second) where libzmq's is infinite — one of
    /// this library's two deliberate differences from libzmq, readable here as
    /// `Context().close_budget`.
    #[new]
    #[pyo3(signature = (*, options=None, max_sockets=None, worker_threads=None, close_budget=None))]
    fn new(
        py: Python<'_>,
        options: Option<&crate::options::PyContextOptions>,
        max_sockets: Option<usize>,
        worker_threads: Option<usize>,
        close_budget: Option<f64>,
    ) -> PyResult<Context> {
        let config = errors::raise(
            py,
            config(options, max_sockets, worker_threads, close_budget),
        )?;
        errors::raise(py, weida_zmq::Context::owned(config)).map(Context::wrap)
    }

    /// The same as `Context(...)`, spelled for a reader who wants to see which
    /// of the three constructors is in use.
    #[classmethod]
    #[pyo3(signature = (*, options=None, max_sockets=None, worker_threads=None, close_budget=None))]
    fn owned(
        _class: &Bound<'_, PyType>,
        py: Python<'_>,
        options: Option<&crate::options::PyContextOptions>,
        max_sockets: Option<usize>,
        worker_threads: Option<usize>,
        close_budget: Option<f64>,
    ) -> PyResult<Context> {
        Context::new(py, options, max_sockets, worker_threads, close_budget)
    }

    /// A context on the **ambient** Tokio reactor: `Context::new`.
    ///
    /// Raises `EMTHREAD` when there is none, which is what a plain Python
    /// process has. `worker_threads` is not taken, because the reactor was
    /// sized by whoever created it.
    #[classmethod]
    #[pyo3(signature = (*, options=None, max_sockets=None, close_budget=None))]
    fn current(
        _class: &Bound<'_, PyType>,
        py: Python<'_>,
        options: Option<&crate::options::PyContextOptions>,
        max_sockets: Option<usize>,
        close_budget: Option<f64>,
    ) -> PyResult<Context> {
        let config = errors::raise(py, config(options, max_sockets, None, close_budget))?;
        errors::raise(py, weida_zmq::Context::new(config)).map(Context::wrap)
    }

    /// A second context on `other`'s reactor: `Context::with_handle`.
    ///
    /// Two namespaces and two ceilings over one thread pool, which is what a
    /// process wants when it isolates two subsystems from each other's
    /// `inproc://` names without paying for a second thread pool.
    #[classmethod]
    #[pyo3(signature = (other, *, options=None, max_sockets=None, close_budget=None))]
    fn sharing(
        _class: &Bound<'_, PyType>,
        py: Python<'_>,
        other: &Context,
        options: Option<&crate::options::PyContextOptions>,
        max_sockets: Option<usize>,
        close_budget: Option<f64>,
    ) -> PyResult<Context> {
        let config = errors::raise(py, config(options, max_sockets, None, close_budget))?;
        // `Exec` hands out no handle, so the handle is taken the way Tokio
        // itself hands one out: from inside the runtime's context, which
        // `Exec::enter` is exactly for. The guard is held across nothing but
        // this call.
        let handle = {
            let _entered = other.inner.exec().enter();
            tokio::runtime::Handle::current()
        };
        errors::raise(py, weida_zmq::Context::with_handle(handle, config)).map(Context::wrap)
    }

    /// `ZMQ_MAX_SOCKETS`: how many sockets this context may hold at once.
    #[getter]
    fn max_sockets(&self) -> usize {
        self.inner.config().max_sockets
    }

    /// The reactor's worker threads, which replace `ZMQ_IO_THREADS`.
    #[getter]
    fn worker_threads(&self) -> usize {
        self.inner.config().worker_threads
    }

    /// `ZMQ_LINGER` as a finite budget, in seconds.
    #[getter]
    fn close_budget(&self) -> f64 {
        self.inner.config().close_budget.as_secs_f64()
    }

    /// Sockets currently open on this context.
    #[getter]
    fn socket_count(&self) -> usize {
        self.inner.socket_count()
    }

    /// Whether [`shutdown`](Context::shutdown) has begun.
    #[getter]
    fn terminated(&self) -> bool {
        self.inner.is_terminated()
    }

    /// `zmq_ctx_term`, with a bound: admits no further socket, then waits at
    /// most `close_budget` for the open ones to be closed.
    ///
    /// Returns how many sockets were still open when the budget ran out — a
    /// number to log or retry against, not an exception, because a peer that
    /// will not go away is not this call's failure.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let context = self.inner.clone();
        self.bridge.awaitable(
            py,
            async move { Ok(context.shutdown().await.outstanding()) },
        )
    }

    fn __repr__(&self) -> String {
        format!(
            "<weida_zmq.Context sockets={} max_sockets={}{}>",
            self.inner.socket_count(),
            self.inner.config().max_sockets,
            if self.inner.is_terminated() {
                " terminated"
            } else {
                ""
            }
        )
    }
}
