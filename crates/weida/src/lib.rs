//! weida: a QUIC-native messaging framework.
//!
//! This crate hosts the runtime, the native QUIC transport, the raw stream core
//! and the brokerless messaging patterns: Req/Rep, Push/Pull and Pub/Sub.
//! `docs/ARCHITECTURE.md` describes the layer model,
//! `docs/PROTOCOL.md` is the normative wire specification, and
//! `docs/FAILURE_MODEL.md` defines what each outcome means.
//!
//! ```no_run
//! use weida::{Runtime, RuntimeConfig, ClientTls, TransferMeta};
//!
//! # async fn example() -> weida::Result<()> {
//! let runtime = Runtime::new(RuntimeConfig::default())?;
//!
//! // Trust belongs to the dialling endpoint, not to the runtime.
//! let requester = runtime.requester(ClientTls::from_pem_file("ca.pem"));
//! requester.connect("weida://127.0.0.1:7443/transform").await?;
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

mod config;
mod conn;
mod endpoint;
mod listener;
mod pool;
mod pubsub;
mod runtime;
mod stream;
mod tls;
mod transfer;

pub use weida_core::{EndpointAddr, Error, ErrorCode, Limits, Result, StopReason, TraceContext};
pub use weida_protocol::{ALPN, VERSION, codes};

pub use config::{ClientTls, Pem, RuntimeConfig, ServerTls};
pub use endpoint::{
    Endpoint, Pattern, Pub, Publisher, Pull, Puller, Push, Pusher, Rep, Replier, Req, Requester,
    Sub, Subscriber,
};
pub use listener::{Binding, Listener};
pub use runtime::Runtime;
pub use stream::{Acceptor, Incoming, Peer};
pub use transfer::{
    Delivery, IncomingMeta, IncomingRequest, IncomingTransfer, OutgoingTransfer, ReplyStream,
    TransferMeta,
};
