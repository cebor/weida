//! weida for Python, on the Rust implementation.
//!
//! `import weida` gets a runtime, a QUIC binding, the two patterns whose
//! shape a Python caller reaches for first — Req/Rep and Push/Pull — and the
//! library's failure vocabulary as exception classes. Underneath is `weida`
//! itself, so this is the first binding in the workspace of the **product**
//! rather than of a competitor implementation: the other five bind ZeroMQ,
//! SP, MQTT, AMQP and NATS
//! ([0014](../../../../docs/decisions/0014-parallel-libraries.md) §2).
//!
//! ```python
//! import asyncio
//! import weida
//!
//! async def main():
//!     server = weida.Runtime()
//!     identity = weida.Identity.generate()
//!     binding = await server.bind("127.0.0.1:0", identity)
//!     replier = binding.replier("/echo")
//!
//!     async def serve():
//!         request = await replier.accept(1 << 20)
//!         await request.reply(request.payload)
//!
//!     answering = asyncio.create_task(serve())
//!
//!     client = weida.Runtime()
//!     requester = client.requester(weida.Trust.by_address())
//!     # The address names the key that must answer, so nothing else is
//!     # configured: no CA, no certificate file.
//!     await requester.connect(binding.url("/echo"))
//!     assert await requester.request(b"ping", 1 << 20) == b"ping"
//!     await answering
//!
//! asyncio.run(main())
//! ```
//!
//! # Every receive takes a ceiling, and that is not an inconvenience
//!
//! `accept`, `request`, `recv` each take a maximum payload size in bytes.
//! weida's payloads are streams — [`INVARIANTS.md`](../../../../docs/INVARIANTS.md)
//! forbids the core from materializing them — and a Python object *is*
//! materialized, so the caller who wants the bytes in memory is the one who
//! has to say how many of them there may be. A binding that chose a default
//! would be choosing how much memory a stranger may make a Python process
//! allocate. A payload larger than the ceiling is `weida.LimitExceeded`.
//!
//! # What is here, and what is not
//!
//! **Three of weida's six patterns**, on both surfaces. Asyncio:
//! `Requester`/`Replier`,
//! `Pusher`/`Puller` and `Publisher`/`Subscriber` (B-200, B-204), each with
//! the whole-payload calls a caller reaches for first **and** the streamed
//! forms for a payload that does not fit memory — `pusher.open()`,
//! `requester.open()`, `publisher.open(topic)`, `puller.recv_stream()` and
//! `subscriber.recv_stream()`. Synchronous: `weida.sync` (B-205), which is
//! `weida::blocking` with argument conversion around it, for a process with
//! no event loop; it shares every value class with the asyncio surface and
//! deliberately has no streamed form, because the facade takes whole
//! payloads and a `sync` module that invented streaming would be inventing a
//! second facade.
//!
//! What is absent, with the reason: the raw L0 surface — `Peer` and
//! `Acceptor`, weida's own stream-level API — because every pattern above is
//! built on it and a Python caller that wants a bare stream wants the Rust
//! API; **PAIR, SURVEY and BUS**, the three patterns the library gained after
//! this binding was written, filed as B-244 because each costs both surfaces
//! here; the **cursor** surface — `TransferMeta`'s report order,
//! `Cursors` and `Reporter` — filed as B-243 for the same reason, which is
//! also why `IncomingMeta`'s three report fields are not among the attributes
//! below; and type stubs, which `docs/libraries/weida-py.md` §10 names as the
//! follow-up they are.
//!
//! # No protocol behaviour lives here
//!
//! HELLO, negotiation, the dispatch rules of
//! [PROTOCOL.md](../../../../docs/PROTOCOL.md) §9.4, the drain, every limit
//! and every guarantee are `weida`'s. This crate converts arguments, awaits,
//! and maps failures onto classes.

use pyo3::prelude::*;

mod endpoints;
mod errors;
mod pubsub;
mod runtime;
mod streams;
mod sync;
mod values;

/// `weida`, as Python sees it.
#[pymodule]
fn weida(module: &Bound<'_, PyModule>) -> PyResult<()> {
    errors::install(module)?;
    module.add_class::<runtime::PyRuntime>()?;
    module.add_class::<runtime::PyBinding>()?;
    module.add_class::<values::PyTrust>()?;
    module.add_class::<values::PyIdentity>()?;
    module.add_class::<values::PyIncomingMeta>()?;
    module.add_class::<endpoints::PyRequester>()?;
    module.add_class::<endpoints::PyReplier>()?;
    module.add_class::<endpoints::PyRequest>()?;
    module.add_class::<endpoints::PyPusher>()?;
    module.add_class::<endpoints::PyPuller>()?;
    module.add_class::<pubsub::PyPublisher>()?;
    module.add_class::<pubsub::PySubscriber>()?;
    module.add_class::<pubsub::PyFanOut>()?;
    module.add_class::<streams::PyOutgoingStream>()?;
    module.add_class::<streams::PyIncomingStream>()?;
    module.add_class::<streams::PyReply>()?;
    sync::install(module)?;
    module.add("VERSION", ::weida::VERSION)?;
    module.add("ALPN", ::weida::ALPN)?;
    module.add("__all__", every_name())?;
    Ok(())
}

/// What `from weida import *` gets: the classes above and the exception
/// family, which `errors::install` adds by name.
fn every_name() -> Vec<&'static str> {
    let mut names = vec![
        "Runtime",
        "Binding",
        "Trust",
        "Identity",
        "IncomingMeta",
        "Requester",
        "Replier",
        "Request",
        "Pusher",
        "Puller",
        "Publisher",
        "Subscriber",
        "FanOut",
        "OutgoingStream",
        "IncomingStream",
        "Reply",
        "sync",
        "VERSION",
        "ALPN",
        "WeidaError",
    ];
    names.extend_from_slice(errors::NAMES);
    names
}
