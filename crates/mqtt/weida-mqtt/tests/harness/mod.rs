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
//! left unanswered, a DISCONNECT with a chosen reason code. A script says what
//! the server does; the test says what the client must then do.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use weida_mqtt_codec::{FixedHeader, Packet, PacketType, varint};

/// What the scripted server does after the CONNECT arrives.
#[derive(Clone, Debug)]
pub enum Act {
    /// Send this packet's bytes.
    Send(Vec<u8>),
    /// Read one packet and record its type.
    Expect,
    /// Wait, so a client timer can expire.
    Idle(Duration),
    /// Drop the connection with no DISCONNECT, which is always available to a
    /// server and is what 3.1.1 did for every error [mqtt5 §1.9].
    Close,
}

/// A server that runs one script against one connection.
pub struct Server {
    address: SocketAddr,
    seen: Arc<Mutex<Vec<PacketType>>>,
    connects: Arc<AtomicUsize>,
    done: mpsc::Receiver<()>,
}

impl Server {
    /// Binds an ephemeral port and runs `script` against the first client.
    ///
    /// The CONNECT is read before the script starts, so a script's first
    /// `Send` is the answer to it.
    pub async fn start(script: Vec<Act>) -> Server {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local addr");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let connects = Arc::new(AtomicUsize::new(0));
        let (finished, done) = mpsc::channel(1);

        let task_seen = Arc::clone(&seen);
        let task_connects = Arc::clone(&connects);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            task_connects.fetch_add(1, Ordering::SeqCst);
            run(stream, script, task_seen).await;
            let _ = finished.send(()).await;
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

    /// The packet types the script's `Expect` steps read, in order. The
    /// CONNECT is always the first.
    pub async fn seen(&self) -> Vec<PacketType> {
        self.seen.lock().await.clone()
    }

    /// How many clients have connected.
    pub fn connects(&self) -> usize {
        self.connects.load(Ordering::SeqCst)
    }

    /// Waits for the script to finish.
    pub async fn finished(&mut self) {
        let _ = self.done.recv().await;
    }
}

async fn run(mut stream: TcpStream, script: Vec<Act>, seen: Arc<Mutex<Vec<PacketType>>>) {
    // The client's first packet MUST be CONNECT ([MQTT-3.1.0-1]), so reading
    // it unconditionally is not an assumption but the protocol.
    let mut buf = Vec::new();
    match read_packet(&mut stream, &mut buf).await {
        Some(packet_type) => seen.lock().await.push(packet_type),
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
                Some(packet_type) => seen.lock().await.push(packet_type),
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

/// Reads exactly one packet, returning its type.
async fn read_packet(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Option<PacketType> {
    buf.clear();
    loop {
        if let Ok((header, header_len)) = FixedHeader::decode(buf, varint::MAX + 5) {
            let total = header_len + header.remaining_length as usize;
            if buf.len() >= total {
                let packet_type = Packet::decode(&buf[..total], varint::MAX + 5)
                    .map(|(packet, _)| packet.packet_type())
                    .unwrap_or(header.packet_type);
                buf.drain(..total);
                return Some(packet_type);
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
