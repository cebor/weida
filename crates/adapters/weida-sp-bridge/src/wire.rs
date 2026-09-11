//! SP over a byte stream: the I/O the codec deliberately does not do.
//!
//! `weida-sp` is sans-I/O - it turns bytes into headers, messages and tag
//! stacks and back, and nothing else - so this is where a socket, a buffer
//! and the handshake sequence live. The split is the point: everything
//! byte-exact is checked against the SP RFCs in a crate that cannot see
//! weida, and everything here is plumbing that can be read for correctness
//! against that crate's types.
//!
//! The handshake is two writes and two reads with no round trip in between:
//! "As soon as the underlying TCP connection is established, both parties
//! MUST send the protocol header immediately. Both endpoints MUST then wait
//! for the protocol header from the peer before proceeding on" [rfc-tcp §2].

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use weida_sp::header::{EndpointType, HEADER_LEN};
use weida_sp::{ProtocolHeader, message};

use crate::error::BridgeError;

/// Read buffer growth step. An SP message header is eight octets, so the
/// interesting case is a payload, which is read straight into this buffer.
const CHUNK: usize = 16 * 1024;

/// An SP connection: the socket, the inbound buffer and the cap.
pub(crate) struct Session<S> {
    io: S,
    /// Unparsed inbound bytes. Persisting across calls is half of what makes
    /// reading cancel-safe; the other half is that bytes are appended only
    /// once a read has completed - see [`Session::fill`].
    buf: Vec<u8>,
    /// Where a read lands before it is appended. Reused, so a read costs no
    /// allocation, and separate from `buf` so a cancelled read cannot change
    /// what is parsed.
    scratch: Vec<u8>,
    /// How many octets one message body may occupy.
    ///
    /// SP grants no credit on the wire and a message may declare 2^64-1
    /// octets, with `RECVMAXSZ` unlimited by default
    /// (`docs/research/nanomsg-nng.md` §5, §12/P12), so this is the bound that
    /// keeps a remote peer from choosing the allocation
    /// (`docs/adapters/nng.md` §3).
    max_message_bytes: u64,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Session<S> {
    pub(crate) fn new(io: S, max_message_bytes: u64) -> Session<S> {
        Session {
            io,
            buf: Vec::new(),
            scratch: vec![0u8; CHUNK],
            max_message_bytes,
        }
    }

    /// Sends this side's protocol header, reads the peer's, and checks the
    /// pairing.
    ///
    /// Three failures end the connection and none of them can be answered,
    /// because SP has no error frame: a header that is not
    /// `0x00 'S' 'P' 0x00 <type> 0x0000` [rfc-tcp §2], and a peer whose
    /// endpoint type may not talk to `ours`
    /// (`docs/research/nanomsg-nng.md` §1, §2). The peer sees a close, which
    /// is exactly what a real NNG socket gives it.
    pub(crate) async fn handshake(
        &mut self,
        ours: EndpointType,
    ) -> Result<EndpointType, BridgeError> {
        self.io
            .write_all(&ProtocolHeader::new(ours).encode())
            .await?;
        self.io.flush().await?;

        let mut theirs = [0u8; HEADER_LEN];
        self.read_exactly(&mut theirs).await?;
        let peer = ProtocolHeader::decode(&theirs)?;
        if !peer.accepts(ours) {
            return Err(BridgeError::EndpointType {
                ours,
                theirs: peer.endpoint,
            });
        }
        Ok(peer.endpoint)
    }

    /// Reads one whole message body, refusing an over-large declaration
    /// before the body is read.
    ///
    /// Cancel-safe: the buffer survives, so a dropped call loses nothing but
    /// the in-flight `read`.
    pub(crate) async fn read_message(&mut self) -> Result<Vec<u8>, BridgeError> {
        loop {
            match message::decode(&self.buf, self.max_message_bytes) {
                Ok((body, used)) => {
                    let body = body.to_vec();
                    self.buf.drain(..used);
                    return Ok(body);
                }
                Err(e) if !e.is_violation() => self.fill().await?,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Writes one message whose body is `parts` concatenated.
    ///
    /// SP has no multipart and no envelope: a message is a length and a body
    /// [rfc-tcp §3]. Parts exist here only because a body is often assembled
    /// from two pieces the bridge holds separately - a tag stack and a
    /// payload, or a topic and a payload - and joining them in the caller
    /// would cost a copy this does not.
    pub(crate) async fn write_message(&mut self, parts: &[&[u8]]) -> Result<(), BridgeError> {
        let len: usize = parts.iter().map(|p| p.len()).sum();
        let mut out = Vec::with_capacity(message::SIZE_LEN + len);
        message::encode_size(len as u64, &mut out);
        for part in parts {
            out.extend_from_slice(part);
        }
        self.io.write_all(&out).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Reads exactly `out.len()` octets, using whatever is already buffered.
    async fn read_exactly(&mut self, out: &mut [u8]) -> Result<(), BridgeError> {
        while self.buf.len() < out.len() {
            self.fill().await?;
        }
        out.copy_from_slice(&self.buf[..out.len()]);
        self.buf.drain(..out.len());
        Ok(())
    }

    /// Reads more bytes, or reports the peer's close.
    ///
    /// **Cancel-safe, and it has to be**: the PUB loop selects over this and
    /// the weida subscriber, so this future is dropped routinely. Reading
    /// into `scratch` and appending afterwards is what makes that safe -
    /// `AsyncReadExt::read` is itself cancel-safe, so a dropped call reads
    /// nothing and leaves `buf` exactly as it was.
    async fn fill(&mut self) -> Result<(), BridgeError> {
        let n = self.io.read(&mut self.scratch).await?;
        if n == 0 {
            return Err(BridgeError::PeerClosed);
        }
        self.buf.extend_from_slice(&self.scratch[..n]);
        Ok(())
    }
}

impl Session<tokio::net::TcpStream> {
    /// Splits into a reader and a writer, so requests can be served
    /// concurrently.
    ///
    /// A cooked REQ socket holds one outstanding request **per context**, and
    /// a socket may own many contexts (`docs/research/nanomsg-nng.md` §2), so
    /// a peer may legitimately have several requests in flight on one pipe -
    /// unlike ZMTP's lockstep REQ. weida's `Replier` accepts concurrent
    /// exchanges (`docs/ARCHITECTURE.md` §6b), so the bridge does too, and
    /// the tag stack is what pairs each reply with its request rather than
    /// arrival order [rfc-reqrep §5].
    pub(crate) fn split(self) -> (Reader, tokio::net::tcp::OwnedWriteHalf) {
        let Session {
            io,
            buf,
            scratch,
            max_message_bytes,
        } = self;
        let (read, write) = io.into_split();
        (
            Reader {
                io: read,
                buf,
                scratch,
                max_message_bytes,
            },
            write,
        )
    }
}

/// The reading half of a split session.
pub(crate) struct Reader {
    io: tokio::net::tcp::OwnedReadHalf,
    buf: Vec<u8>,
    scratch: Vec<u8>,
    max_message_bytes: u64,
}

impl Reader {
    /// Reads one whole message body, under the same cap as before the split.
    pub(crate) async fn read_message(&mut self) -> Result<Vec<u8>, BridgeError> {
        loop {
            match message::decode(&self.buf, self.max_message_bytes) {
                Ok((body, used)) => {
                    let body = body.to_vec();
                    self.buf.drain(..used);
                    return Ok(body);
                }
                Err(e) if !e.is_violation() => {
                    let n = self.io.read(&mut self.scratch).await?;
                    if n == 0 {
                        return Err(BridgeError::PeerClosed);
                    }
                    self.buf.extend_from_slice(&self.scratch[..n]);
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

/// Frames one message for the writer task: a length and a body.
pub(crate) fn frame(parts: &[&[u8]]) -> Vec<u8> {
    let len: usize = parts.iter().map(|p| p.len()).sum();
    let mut out = Vec::with_capacity(message::SIZE_LEN + len);
    message::encode_size(len as u64, &mut out);
    for part in parts {
        out.extend_from_slice(part);
    }
    out
}
