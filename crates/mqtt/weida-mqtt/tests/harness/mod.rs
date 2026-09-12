//! A scriptable MQTT 5.0 server, built on the codec, for testing the client.
//!
//! It is faithful on the wire and is deliberately **not** an independent
//! implementation: it shares this workspace's codec, so it proves the client's
//! behaviour and not interoperability. Interoperability is B-148's `rumqttd`
//! run and B-149's Mosquitto run, against implementations that share no code
//! with ours.
//!
//! What it is for is the half of the client's behaviour a real broker cannot
//! be made to exhibit on demand: a CONNACK with one particular availability
//! flag clear, a `Server Keep Alive` that overrides, a PINGREQ deliberately
//! left unanswered, a DISCONNECT with a chosen reason code, a connection
//! dropped mid-exchange so a session has something to resume.
//!
//! It records the **bytes** of everything the client sent, not just the packet
//! types, because some of what the client must get right is inside the packet:
//! a retransmission carries DUP 1 and its original Packet Identifier
//! ([MQTT-3.3.1-1], [MQTT-4.6.0-1]), and asserting that needs the packet.

// The harness is compiled separately into every integration-test binary, and
// no single one of them uses all of it: `connection.rs` never looks inside a
// packet, `session.rs` never needs an idle step. Without this each binary
// reports the other's half as dead.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use weida_mqtt_codec::{FixedHeader, Packet, PacketType, varint};

/// The ceiling every harness decode is done under: the largest packet the
/// encoding can express (2.1.4). The harness is not the thing under test, so
/// it declines nothing.
const CAP: u32 = varint::MAX + 5;

/// What the scripted server does after the CONNECT arrives.
#[derive(Clone, Debug)]
pub enum Act {
    /// Send this packet's bytes.
    Send(Vec<u8>),
    /// Read one packet and record it.
    Expect,
    /// Wait, so a client timer can expire.
    Idle(Duration),
    /// Drop the connection with no DISCONNECT, which is always available to a
    /// server and is what 3.1.1 did for every error [mqtt5 §1.9]. Also how a
    /// test leaves a session with something to resume.
    Close,
}

/// A server that runs one script per connection, in order.
///
/// A reconnect test hands it two scripts: the first connection gets the first,
/// the second the second.
pub struct Server {
    address: SocketAddr,
    seen: Arc<Mutex<Vec<Vec<u8>>>>,
    connects: Arc<AtomicUsize>,
    done: mpsc::Receiver<()>,
}

impl Server {
    /// Binds an ephemeral port and runs `script` against the first client.
    pub async fn start(script: Vec<Act>) -> Server {
        Server::start_all(vec![script]).await
    }

    /// The same for several consecutive connections, one script each.
    ///
    /// The CONNECT is read before each script starts, so a script's first
    /// `Send` is the answer to it.
    pub async fn start_all(scripts: Vec<Vec<Act>>) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local addr");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let connects = Arc::new(AtomicUsize::new(0));
        let (finished, done) = mpsc::channel(scripts.len().max(1));

        let task_seen = Arc::clone(&seen);
        let task_connects = Arc::clone(&connects);
        tokio::spawn(async move {
            for script in scripts {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                task_connects.fetch_add(1, Ordering::SeqCst);
                run(stream, script, Arc::clone(&task_seen)).await;
                if finished.send(()).await.is_err() {
                    return;
                }
            }
        });

        Server {
            address,
            seen,
            connects,
            done,
        }
    }

    /// The address a client dials.
    pub fn address(&self) -> String {
        self.address.to_string()
    }

    /// The packet types received, in order, across every connection. Each
    /// connection's CONNECT is included.
    pub async fn seen(&self) -> Vec<PacketType> {
        self.seen
            .lock()
            .await
            .iter()
            .map(|bytes| {
                Packet::decode(bytes, CAP)
                    .map(|(packet, _)| packet.packet_type())
                    .unwrap_or_else(|error| panic!("the harness received {error}"))
            })
            .collect()
    }

    /// The bytes of every packet received, in order, for a test that must look
    /// inside one.
    pub async fn seen_bytes(&self) -> Vec<Vec<u8>> {
        self.seen.lock().await.clone()
    }

    /// How many clients have connected.
    pub fn connects(&self) -> usize {
        self.connects.load(Ordering::SeqCst)
    }

    /// Waits for one script to finish.
    pub async fn finished(&mut self) {
        let _ = self.done.recv().await;
    }
}

async fn run(mut stream: TcpStream, script: Vec<Act>, seen: Arc<Mutex<Vec<Vec<u8>>>>) {
    // The client's first packet MUST be CONNECT ([MQTT-3.1.0-1]), so reading
    // it unconditionally is not an assumption but the protocol.
    let mut buf = Vec::new();
    match read_packet(&mut stream, &mut buf).await {
        Some(packet) => seen.lock().await.push(packet),
        None => return,
    }

    for act in script {
        match act {
            Act::Send(bytes) => {
                if stream.write_all(&bytes).await.is_err() {
                    return;
                }
                let _ = stream.flush().await;
            }
            Act::Expect => match read_packet(&mut stream, &mut buf).await {
                Some(packet) => seen.lock().await.push(packet),
                None => return,
            },
            Act::Idle(duration) => tokio::time::sleep(duration).await,
            Act::Close => return,
        }
    }
    // Hold the connection open until the client goes away, so a test that
    // asserts the client's own close sees it.
    let mut sink = [0u8; 64];
    while let Ok(read) = stream.read(&mut sink).await {
        if read == 0 {
            return;
        }
    }
}

/// Reads exactly one packet, returning its bytes.
async fn read_packet(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    loop {
        if let Ok((header, header_len)) = FixedHeader::decode(buf, CAP) {
            let total = header_len + header.remaining_length as usize;
            if buf.len() >= total {
                let packet = buf[..total].to_vec();
                buf.drain(..total);
                return Some(packet);
            }
        }
        let before = buf.len();
        buf.resize(before + 1024, 0);
        let read = stream.read(&mut buf[before..]).await.ok()?;
        buf.truncate(before + read);
        if read == 0 {
            return None;
        }
    }
}

/// One packet's bytes, for a script.
pub fn bytes(packet: &Packet<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    packet.encode(&mut out).expect("a script packet encodes");
    out
}

/// Decodes one of [`Server::seen_bytes`]'s entries.
pub fn decode(bytes: &[u8]) -> Packet<'_> {
    Packet::decode(bytes, CAP)
        .map(|(packet, _)| packet)
        .expect("a recorded packet decodes")
}
