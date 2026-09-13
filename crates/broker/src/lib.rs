//! The minimal L2 broker: a queue at an endpoint path, `Accepted` in.
//!
//! A broker is an ordinary weida process that holds messages for a while. It
//! registers a queue on each endpoint path its configuration names; a producer
//! sends a message to that path **as an exchange** and the reply half carries
//! the broker's certificate; a producer that will not wait sends a one-way
//! transfer instead and gets the transport receipt and nothing more. Both
//! shapes are the patterns this workspace already has: L2 is a layer, not a
//! fork (`docs/ARCHITECTURE.md` §1).
//!
//! # What this crate certifies, and what it refuses to
//!
//! [`Achieved::Accepted`] and nothing above it. Everything a queue holds is in
//! memory and vanishes with the process, so `Stored(Written)` — defined as
//! surviving "the broker process dying: crash, restart or orderly stop" —
//! would be a prohibited claim, not an optimistic one: "`Stored` MUST NOT be
//! reported for an in-memory buffer" (`docs/GUARANTEES.md` §1). The refusal is
//! in the type rather than in a comment: [`Achieved`] has one variant, so no
//! code path here can issue a durable certificate, and the variants arrive
//! with the store that makes them true (Phase 5) and the replication that
//! makes `Replicated` measurable (Phase 7).
//!
//! A certificate is also about **one hop**. `Accepted` says this broker took
//! responsibility for the message; it says nothing about a consumer, and the
//! broker never relays a consumer's outcome back to the producer
//! (`docs/GUARANTEES.md` §2,
//! [0018](https://git.doodleshnookie.net/tuco86/weida/blob/main/docs/decisions/0018-minimal-broker.md)
//! §4.3). A producer that needs to know a consumer processed its message uses
//! an application reply.
//!
//! # What this slice does not do
//!
//! **It delivers nothing.** Queues admit, confirm and hold; the credit frame a
//! consumer grants and the delivery under it are B-202, and the consumer
//! acknowledgement, redelivery and the queue drain are B-203. A queue with no
//! consumer is the thing being built here on purpose: a confirm that arrives
//! with nobody listening is the temporal decoupling a queue exists for.
//!
//! # Example
//!
//! ```no_run
//! use weida::{Runtime, RuntimeConfig, ServerTls, Identity};
//! use weida_broker::{Broker, BrokerConfig};
//!
//! # async fn run() -> Result<(), weida::Error> {
//! let runtime = Runtime::new(RuntimeConfig::default())?;
//! let listener = runtime.listener();
//! let identity = Identity::generate()?;
//! listener
//!     .bind_quic("127.0.0.1:0".parse().unwrap(), ServerTls::new(identity))
//!     .await?;
//! let broker = Broker::new(&listener, BrokerConfig::with_queues(["/jobs"]))?;
//! let serving = broker.clone();
//! tokio::spawn(async move { serving.serve().await });
//! # Ok(())
//! # }
//! ```

mod broker;
mod queue;

pub use broker::{Achieved, Broker, BrokerConfig};
pub use queue::{PER_MESSAGE_OVERHEAD, QueueStats};
