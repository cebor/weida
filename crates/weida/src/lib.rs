//! weida: a QUIC-native messaging framework.
//!
//! This crate hosts the runtime, the native QUIC transport, the raw stream core
//! and the brokerless messaging patterns: Req/Rep, Push/Pull and Pub/Sub.
//! `docs/ARCHITECTURE.md` describes the layer model,
//! `docs/PROTOCOL.md` is the normative wire specification, and
//! `docs/FAILURE_MODEL.md` defines what each outcome means.
//!
//! ```no_run
//! use weida::{Runtime, RuntimeConfig, TransferMeta, Trust};
//!
//! # async fn example() -> weida::Result<()> {
//! let runtime = Runtime::new(RuntimeConfig::default())?;
//!
//! // Trust belongs to the dialling endpoint, not to the runtime. Here the
//! // address itself names the peer's public key, so nothing else is needed.
//! let requester = runtime.requester(Trust::by_address());
//! requester
//!     .connect("weida://sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08@127.0.0.1:7443/transform")
//!     .await?;
//!
//! // One bidirectional stream: the request half and the reply half. The
//! // stream is the correlation, so nothing on the wire names the exchange.
//! let (mut transfer, reply) = requester.open(TransferMeta::default()).await?;
//! transfer.write_all(b"hello weida").await?;
//! transfer.finish()?;
//!
//! let body = reply.recv().await?.collect(64 * 1024).await?;
//! println!("{}", String::from_utf8_lossy(&body));
//! # Ok(())
//! # }
//! ```
//!
//! The example above runs on the caller's ambient Tokio reactor. A caller
//! that has none — or whose executor is not Tokio at all — uses
//! [`Runtime::owned`] instead: the runtime then owns the reactor `quinn`
//! needs, and every task, timer and name lookup weida performs runs there,
//! while the futures it hands back may be driven by any executor. Transfer
//! payloads implement both the `tokio::io` and the `futures-io` traits for
//! the same reason.

mod config;
mod conn;
mod dedup;
mod endpoint;
mod listener;
mod ordering;
mod pool;
mod pubsub;
mod runtime;
mod stream;
mod tls;
mod transfer;

pub use weida_core::{
    EndpointAddr, Error, ErrorCode, Fingerprint, Limits, Result, StopReason, TraceContext,
};
pub use weida_protocol::{ALPN, VERSION, codes};

pub use config::{ClientTls, Identity, Pem, RuntimeConfig, ServerTls, Trust};
pub use endpoint::{
    Endpoint, Pattern, Pub, Publisher, Pull, Puller, Push, Pusher, Rep, Replier, Req, Requester,
    Sub, Subscriber,
};
pub use listener::{Binding, Listener};
pub use ordering::Gap;
// The guarantee vocabulary of `docs/PROTOCOL.md` §6.5, for `RuntimeConfig`.
// `Delivery` keeps its transfer-receipt meaning at this level, so the
// dimension of the same name is re-exported under the name the guarantee
// documents use for it.
pub use runtime::Runtime;
pub use stream::{Acceptor, Incoming, Peer};
pub use transfer::{
    Delivery, IncomingMeta, IncomingRequest, IncomingTransfer, OutgoingTransfer, ReplyStream,
    TransferMeta,
};
pub use weida_protocol::header::{
    Acknowledgement, Backpressure, Deduplication, Delivery as DeliveryLevel, Durability,
    GuaranteeSet, OrderingMode, ProducerNaming,
};
