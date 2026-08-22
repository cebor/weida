//! weida: a QUIC-native messaging framework.
//!
//! This crate hosts the runtime, the native QUIC transport and the brokerless
//! messaging patterns: Req/Rep, Push/Pull and Pub/Sub.
//! `docs/ARCHITECTURE.md` describes the layer model,
//! `docs/PROTOCOL.md` is the normative wire specification, and
//! `docs/FAILURE_MODEL.md` defines what each outcome means.
//!
//! ```no_run
//! use weida::{AckMode, Runtime, RuntimeConfig, ClientTls, TransferMeta};
//!
//! # async fn example() -> weida::Result<()> {
//! let runtime = Runtime::new(RuntimeConfig::default())?;
//!
//! // Trust belongs to the dialling endpoint, not to the runtime.
//! let requester = runtime.requester(ClientTls::from_pem_file("ca.pem"));
//! requester.connect("weida://127.0.0.1:7443/transform").await?;
//!
//! let (mut transfer, pending) = requester
//!     .open(TransferMeta::default().with_ack(AckMode::Accepted))
//!     .await?;
//! transfer.write_all(b"hello weida").await?;
//! let outcome = transfer.finish().await?;
//!
//! let reply = pending.recv().await?;
//! let body = reply.collect(64 * 1024).await?;
//! println!("{outcome}: {}", String::from_utf8_lossy(&body));
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
mod tls;
mod transfer;

pub use weida_core::{
    AckMode, AckState, EndpointAddr, Error, ErrorCode, Limits, Outcome, Result, Role, StopReason,
    TraceContext, TransferId,
};
pub use weida_protocol::{ALPN, VERSION, codes};

pub use config::{ClientTls, Pem, RuntimeConfig, ServerTls};
pub use endpoint::{
    Endpoint, Pattern, Pub, Publisher, Pull, Puller, Push, Pusher, Rep, Replier, Req, Requester,
    Sub, Subscriber,
};
pub use listener::{Binding, Listener};
pub use runtime::Runtime;
pub use transfer::{
    IncomingMeta, IncomingRequest, IncomingTransfer, OutgoingTransfer, PendingReply, TransferMeta,
};
