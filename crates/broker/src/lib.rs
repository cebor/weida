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
//! # Delivery, and what a consumer must do to get any
//!
//! A consumer is an ordinary [`weida::Subscriber`]: it subscribes to the
//! queue's path with a filter and then **grants credit** with
//! [`weida::Subscriber::grant`], an absolute delivery limit per subscription.
//! Until it does, it receives nothing — initial credit is zero, the only
//! default that cannot surprise a consumer with a flood. Each message goes to
//! exactly one consumer with credit, as a one-way transfer; a queue with no
//! credit anywhere keeps its messages, which is the temporal decoupling a
//! queue exists for.
//!
//! Two properties are worth stating because they are what a credit scheme is
//! bought for. The limit is **absolute, cumulative and monotone at the
//! broker** — the highest limit seen wins — so a duplicated or reordered
//! grant changes nothing, which is what makes credit safe on a transport that
//! does not order the streams control frames ride. And **the only pause is
//! the `0` a fresh subscription starts at**: restating the count already
//! delivered is not above the standing limit, so it changes nothing at all
//! unless that count had already reached it. v0 offers no way to lower a
//! standing limit, so a consumer that wants to stay in control grants in
//! increments it is willing to receive rather than one large number it means
//! to withdraw later.
//!
//! # What this slice does not do
//!
//! **A delivery has no outcome yet.** A delivered message leaves the queue
//! immediately, because there is nothing to wait for: the consumer
//! acknowledgement, the redelivery of what was not acknowledged, and
//! `Broker::drain` are B-203.
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
mod consumers;
mod queue;

pub use broker::{Achieved, Broker, BrokerConfig};
pub use queue::{PER_MESSAGE_OVERHEAD, QueueStats};
