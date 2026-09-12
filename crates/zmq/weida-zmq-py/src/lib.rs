//! ZeroMQ for Python, on the Rust implementation rather than on libzmq.
//!
//! `import weida_zmq` gets a context, eleven socket types and libzmq's errno
//! vocabulary as exception classes. Underneath is `weida-zmq` — a ZeroMQ
//! implementation with no C library, no `weida` and no `weida-protocol` in it
//! ([0013](../../../../docs/decisions/0013-competitor-libraries.md) §4) — and
//! `weida-py-core`, the PyO3 foundation every binding in this workspace shares
//! ([0014](../../../../docs/decisions/0014-parallel-libraries.md) §2).
//!
//! ```python
//! import asyncio
//! import weida_zmq
//!
//! async def main():
//!     context = weida_zmq.Context()
//!     server = weida_zmq.RepSocket(context)
//!     client = weida_zmq.ReqSocket(context)
//!     endpoint = await server.bind("tcp://127.0.0.1:0")   # the chosen port
//!     await client.connect(endpoint)
//!
//!     await client.send(b"Hello")
//!     assert await server.recv() == [b"Hello"]
//!     await server.send(b"World")
//!     assert await client.recv() == [b"World"]
//!
//!     await context.shutdown()
//!
//! asyncio.run(main())
//! ```
//!
//! # Asyncio first
//!
//! Every call is a coroutine, driven on the reactor the context owns; nothing
//! blocks the event loop and nothing holds the GIL while waiting. A cancelled
//! task cancels the Rust future underneath rather than leaving it running. The
//! synchronous surface — the same eleven types over `weida-zmq`'s `blocking`
//! facade, for a process with no loop at all — is B-116 and is deliberately
//! second, because the facade exists in Rust and building it here first would
//! build it twice (0014 §2).
//!
//! # Errors
//!
//! Every failure is a class of this module, named for libzmq's errno and
//! derived from `weida_zmq.ZmqError`, carrying `errno` and `cause`:
//!
//! ```python
//! try:
//!     await socket.recv()
//! except weida_zmq.EFSM as wrong_order:
//!     assert wrong_order.errno == "EFSM"
//! ```
//!
//! # Building it
//!
//! `crates/zmq/weida-zmq-py/develop.sh` is the whole of it: it creates a
//! `uv`-managed virtualenv under the worktree, installs `maturin` and `pytest`
//! into that virtualenv and nothing into the system interpreter, runs
//! `maturin develop` and then the Python tests. The wheel is `abi3` from
//! CPython 3.9, so one build serves every later interpreter.

use pyo3::prelude::*;

mod context;
mod errors;
mod lease;
mod ops;
mod sockets;
mod values;

/// `weida_zmq`, as Python sees it.
#[pymodule]
fn weida_zmq(module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::install(module)?;
    module.add_class::<context::Context>()?;
    module.add_class::<sockets::Discarded>()?;
    module.add_class::<values::PyMultipart>()?;
    module.add_class::<values::PySent>()?;
    module.add_class::<values::PyPublished>()?;
    module.add_class::<sockets::PyReqSocket>()?;
    module.add_class::<sockets::PyRepSocket>()?;
    module.add_class::<sockets::PyDealerSocket>()?;
    module.add_class::<sockets::PyRouterSocket>()?;
    module.add_class::<sockets::PyPubSocket>()?;
    module.add_class::<sockets::PySubSocket>()?;
    module.add_class::<sockets::PyXPubSocket>()?;
    module.add_class::<sockets::PyXSubSocket>()?;
    module.add_class::<sockets::PyPushSocket>()?;
    module.add_class::<sockets::PyPullSocket>()?;
    module.add_class::<sockets::PyPairSocket>()?;
    module.add("__all__", every_name())?;
    Ok(())
}

/// What `from weida_zmq import *` gets: the classes above and the exception
/// family, which `errors::install` adds by name.
fn every_name() -> Vec<&'static str> {
    let mut names = vec![
        "Context",
        "Discarded",
        "Multipart",
        "Sent",
        "Published",
        "ReqSocket",
        "RepSocket",
        "DealerSocket",
        "RouterSocket",
        "PubSocket",
        "SubSocket",
        "XPubSocket",
        "XSubSocket",
        "PushSocket",
        "PullSocket",
        "PairSocket",
        "ZmqError",
    ];
    names.extend_from_slice(errors::NAMES);
    names
}
