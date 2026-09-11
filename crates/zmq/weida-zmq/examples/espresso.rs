//! The zguide's **Espresso**, chapter 5: `espresso.c` - pub-sub tracing
//! through `zmq_proxy()`'s third socket.
//!
//! ```text
//! cargo run -p weida-zmq --example espresso
//! ```
//!
//! The problem: "no visibility into a pub-sub network". The mechanism is the
//! capture socket, which `zmq_proxy` "shall send all messages, received on
//! both frontend and backend, to", including the subscription control
//! frames - so the trace shows `0141`, `0142`, the data, and then the
//! unsubscriptions `0041`, `0042`. The guarantee: **all bridged traffic,
//! control and data, is observable.** The cost: a proxy in the path.
//!
//! The listener in the C is a thread reading the other end of an `inproc`
//! PAIR; here the listener is whoever called [`espresso`], which is handed
//! that end. A PAIR is right for this and a PUB would not be: the trace must
//! not be dropped at a high-water mark by the very socket type whose drops
//! you are trying to see.
//!
//! # Which surface, and why
//!
//! **Async**: this *is* [`weida_zmq::proxy`], which is one call and a loop.
//!
//! # The three differences from the C
//!
//! * **The capture end is returned, not spawned.** `espresso.c` starts a
//!   listener thread that prints; a test wants the frames, so the PAIR end
//!   comes back to the caller and printing is `main`'s business.
//! * **The `inproc` name is derived, not literal.** The C hardcodes
//!   `inproc://capture`; two proxies in one process would collide, so the
//!   name carries the XPUB's port.
//! * **No topic is invented.** The C publishes two fixed topics from its own
//!   thread; here the publisher is the caller's, because what the recipe
//!   provides is the tap and not the traffic.

use std::time::Duration;

use weida_zmq::{
    Context, ContextConfig, Message, Multipart, PairSocket, PubSocket, Result, SubSocket,
    XPubSocket, XSubSocket, proxy,
};

/// A running Espresso proxy, with the capture end in the caller's hands.
pub struct Espresso {
    /// Where subscribers connect - the XPUB side.
    pub subscriber_endpoint: String,
    /// The listener half of the capture PAIR: every frame both sides of the
    /// proxy saw.
    pub capture: PairSocket,
    /// The proxy itself.
    pub task: tokio::task::JoinHandle<()>,
}

/// `espresso.c`: XSUB upstream, XPUB downstream, and the capture socket
/// between the caller and everything that crosses.
///
/// # Errors
///
/// What constructing, binding or connecting the three sockets reports.
pub async fn espresso(context: &Context, publisher: &str) -> Result<Espresso> {
    let mut frontend = XSubSocket::new(context)?;
    frontend.connect(publisher)?;
    let mut backend = XPubSocket::new(context)?;
    let subscriber_endpoint = backend.bind("tcp://127.0.0.1:0").await?.to_string();
    //  The C's `inproc://capture`, with the port appended so that two
    //  proxies in one process do not fight over the name.
    let port = subscriber_endpoint
        .rsplit(':')
        .next()
        .expect("a tcp endpoint ends in a port")
        .to_owned();
    let mut tap = PairSocket::new(context)?;
    let capture_name = format!("inproc://capture-{port}");
    tap.bind(&capture_name).await?;
    let capture = PairSocket::new(context)?;
    capture.connect(&capture_name)?;

    let task = tokio::spawn(async move {
        //  "shall send all messages, received on both frontend and backend,
        //  to the capture socket"
        let _ = proxy(&mut frontend, &mut backend, Some(&mut tap)).await;
    });
    Ok(Espresso {
        subscriber_endpoint,
        capture,
        task,
    })
}

/// One captured message, rendered the way `espresso.c` prints it: a
/// subscription frame as `01` plus the topic, an unsubscription as `00` plus
/// the topic, data as itself.
pub fn render(message: &Multipart) -> String {
    message
        .frames()
        .iter()
        .map(|frame| {
            let bytes = frame.as_slice();
            match bytes.first() {
                Some(0x01) => format!("01{}", String::from_utf8_lossy(&bytes[1..])),
                Some(0x00) => format!("00{}", String::from_utf8_lossy(&bytes[1..])),
                _ => String::from_utf8_lossy(bytes).into_owned(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[tokio::main]
async fn main() -> Result<()> {
    let context = Context::new(ContextConfig::default())?;
    let mut publisher = PubSocket::new(&context)?;
    let publisher_endpoint = publisher.bind("tcp://127.0.0.1:0").await?.to_string();
    let mut trace = espresso(&context, &publisher_endpoint).await?;

    let mut subscriber = SubSocket::new(&context)?;
    subscriber.connect(&trace.subscriber_endpoint)?;
    subscriber.subscribe("A")?;
    subscriber.subscribe("B")?;

    //  Publish until the subscription has crossed the proxy, which is the
    //  slow joiner and not a fault. Topic and body are separate frames, the
    //  way `espresso.c` sends them.
    let message = Multipart::new(vec![
        Message::from(b"A".to_vec()),
        Message::from(b"hello".to_vec()),
    ])
    .expect("two frames");
    loop {
        if publisher.publish(message.clone()).delivered > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = subscriber.recv_timeout(Duration::from_secs(1)).await;
    subscriber.unsubscribe("A")?;
    subscriber.unsubscribe("B")?;

    //  Everything the proxy saw, in order.
    while let Ok(captured) = trace.capture.recv_timeout(Duration::from_secs(1)).await {
        println!("captured: {}", render(&captured));
    }
    Ok(())
}
