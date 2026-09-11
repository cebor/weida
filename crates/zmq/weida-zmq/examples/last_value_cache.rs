//! The zguide's **Last Value Caching**, chapter 5: `lvcache.c` - an
//! application-programmable XSUB/XPUB proxy that answers a new subscription
//! from a cache.
//!
//! ```text
//! cargo run -p weida-zmq --example last_value_cache
//! ```
//!
//! The problem, in the guide's arithmetic: a new subscriber to a rarely
//! updated topic waits for the next update, and "with 1000 topics and one
//! update per second, a new subscriber waits 500 seconds on average for
//! data". The mechanism is "an application-programmable proxy where a PGM
//! switch would sit - XSUB upstream, XPUB downstream, latest message cached
//! per topic, republished on an XPUB subscription notification". The
//! guarantee is precise and worth keeping precise: **immediate catch-up to
//! the cached last value per subscribed topic, not the full stream.** One
//! message per topic, the most recent one; nothing older is kept and nothing
//! is replayed.
//!
//! Two notes the guide attaches, because a reader will hit both:
//!
//! * In production this needs `ZMQ_XPUB_VERBOSE`, or the XPUB reports only
//!   the *first* subscription to a topic and later subscribers get nothing
//!   from the cache. This library's XPUB reports every subscription, which is
//!   the verbose behaviour, so the option is a no-op rather than a
//!   requirement - the option table says so (`ZMQ_XPUB_VERBOSE`).
//! * libzmq's DRAFT `ZMQ_XPUB_MANUAL_LAST_VALUE` and `ZMQ_XPUB_WELCOME_MSG`
//!   support the same shape inside the socket; this recipe is the portable
//!   form, and the one that shows what those options do.
//!
//! # Which surface, and why
//!
//! **Async.** The C is a `zmq_poll` over both sockets, because the cache has
//! to be updated by traffic in one direction and read by control frames
//! arriving in the other; `select!` is that loop. It cannot be
//! [`weida_zmq::proxy`], which is the point of the recipe: the forwarding is
//! ordinary, the *subscription* handling is not.
//!
//! # The three differences from the C
//!
//! * **The cache is per topic, keyed by the first frame.** The C uses a
//!   `zhash` of `char*` keys from `zmsg_popstr`; here the key is the topic
//!   frame's bytes, which is what 29/PUBSUB actually matches on and works for
//!   a binary topic too.
//! * **Republished messages are counted.** "Immediate catch-up" is a claim
//!   about a message arriving with nothing published after the subscription,
//!   so the count is a field rather than a log line.
//! * **The subscription is still forwarded upstream.** The C's loop does the
//!   same, and it matters: a cache hit must not stop the real publisher from
//!   learning that somebody wants this topic.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use weida_zmq::{
    Context, ContextConfig, Message, Multipart, PubSocket, Result, SubSocket, XPubSocket,
    XSubSocket,
};

/// A running last-value cache.
pub struct LastValueCache {
    /// Where subscribers connect - the XPUB side.
    pub subscriber_endpoint: String,
    /// Messages served from the cache on a subscription.
    pub served_from_cache: Arc<AtomicUsize>,
    /// The proxy loop.
    pub task: tokio::task::JoinHandle<()>,
}

/// `lvcache.c`: forward everything, remember the last message per topic, and
/// answer a new subscription with it.
///
/// # Errors
///
/// What constructing, binding or connecting the two sockets reports.
pub async fn last_value_cache(context: &Context, publisher: &str) -> Result<LastValueCache> {
    let mut frontend = XSubSocket::new(context)?;
    frontend.connect(publisher)?;
    let mut backend = XPubSocket::new(context)?;
    let subscriber_endpoint = backend.bind("tcp://127.0.0.1:0").await?.to_string();
    let served_from_cache = Arc::new(AtomicUsize::new(0));

    let task = tokio::spawn({
        let served_from_cache = Arc::clone(&served_from_cache);
        async move {
            let mut cache: HashMap<Vec<u8>, Multipart> = HashMap::new();
            loop {
                tokio::select! {
                    //  Upstream traffic: cache the last message per topic and
                    //  pass it on.
                    arrived = frontend.recv() => {
                        let Ok(message) = arrived else { return };
                        let topic = message.frames()[0].as_slice().to_vec();
                        cache.insert(topic, message.clone());
                        backend.publish(message);
                    }
                    //  A control frame from a subscriber: 0x01 plus the
                    //  topic to subscribe, 0x00 plus the topic to cancel.
                    arrived = backend.recv() => {
                        let Ok(message) = arrived else { return };
                        let frame = message.frames()[0].as_slice().to_vec();
                        let Some((&action, topic)) = frame.split_first() else { continue };
                        //  The real publisher must still hear about it.
                        frontend.send(message);
                        if action != 1 {
                            continue;
                        }
                        //  "republished on an XPUB subscription
                        //  notification": every cached topic the new
                        //  subscription covers, and nothing else.
                        for (cached_topic, cached) in &cache {
                            if cached_topic.starts_with(topic) {
                                backend.publish(cached.clone());
                                served_from_cache.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }
    });
    Ok(LastValueCache {
        subscriber_endpoint,
        served_from_cache,
        task,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new(ContextConfig::default())?;
    let mut publisher = PubSocket::new(&context)?;
    let publisher_endpoint = publisher.bind("tcp://127.0.0.1:0").await?.to_string();
    let cache = last_value_cache(&context, &publisher_endpoint).await?;

    //  A cache can only hold what crossed it, and a publisher with no
    //  subscriber downstream delivers nothing at all - so the recipe's
    //  situation is a second subscriber, which is also the only situation
    //  `ZMQ_XPUB_VERBOSE` exists for.
    let mut early = SubSocket::new(&context)?;
    early.connect(&cache.subscriber_endpoint)?;
    early.subscribe("A")?;
    early.subscribe("B")?;

    //  Two topics, A updated twice: the cache keeps the latest per topic.
    //  Topic and body are separate frames, as `lvcache.c` sends them - the
    //  cache is keyed by the topic frame, so a one-frame message would make
    //  every distinct body its own topic.
    for (topic, body) in [("A", "one"), ("B", "one"), ("A", "two")] {
        let message = Multipart::new(vec![
            Message::from(topic.as_bytes().to_vec()),
            Message::from(body.as_bytes().to_vec()),
        ])
        .expect("two frames");
        loop {
            if publisher.publish(message.clone()).delivered > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    while early.recv_timeout(Duration::from_millis(200)).await.is_ok() {}

    //  A subscriber arriving now, with nothing published after it: without
    //  the cache it would wait for the next update on topic A.
    let mut late = SubSocket::new(&context)?;
    late.connect(&cache.subscriber_endpoint)?;
    late.subscribe("A")?;
    let caught_up = late.recv_timeout(Duration::from_secs(2)).await?;
    println!(
        "late subscriber caught up with {} {}, {} message(s) from the cache",
        String::from_utf8_lossy(caught_up.frames()[0].as_slice()),
        String::from_utf8_lossy(caught_up.frames()[1].as_slice()),
        cache.served_from_cache.load(Ordering::Relaxed)
    );
    Ok(())
}
