//! Resource limits.
//!
//! Master doc §50: no internal queue may be unbounded, and every allocation
//! that a remote peer can influence must have a defensive ceiling. Each field
//! below names the remote-controlled quantity it bounds.

/// Local resource limits for one runtime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Largest frame header this side will accept, in bytes. Checked against
    /// the preamble length *before* any allocation.
    pub max_header_bytes: u64,
    /// QUIC concurrent inbound unidirectional streams. Bounds the number of
    /// live per-stream parse tasks, hence worst-case header memory
    /// (`max_concurrent_uni_streams * max_header_bytes`).
    pub max_concurrent_uni_streams: u32,
    /// QUIC per-stream receive window in bytes. Bounds buffered payload for one
    /// transfer and provides backpressure to the sender.
    pub stream_receive_window: u64,
    /// QUIC per-connection receive window in bytes.
    pub connection_receive_window: u64,
    /// Concurrently accepted connections per binding. Excess connections are
    /// closed immediately with `LIMIT_EXCEEDED`.
    pub max_connections: usize,
    /// Locally registered pending entries (awaiting ACK plus awaiting reply)
    /// per connection. Exceeding this fails `open()` with
    /// [`crate::Error::LimitExceeded`]; it never kills the connection.
    pub max_pending: usize,
    /// Depth of a replier's accept queue. Senders await a free slot, so QUIC
    /// flow control carries the backpressure to the peer.
    pub endpoint_queue: usize,
    /// How long to wait for the peer HELLO before closing the connection with
    /// `NEGOTIATION_FAILED`, in milliseconds.
    pub hello_timeout_ms: u64,
    /// Subscription filters one peer connection may hold at once, summed over
    /// every path. Exceeding it closes the connection with `LIMIT_EXCEEDED`:
    /// SUBSCRIBE has no transfer id to answer with an ERROR frame.
    pub max_subscriptions: usize,
    /// Payload bytes a publisher may hold queued for one subscriber. A message
    /// that does not fit is dropped for that subscriber and counted; the
    /// publisher never blocks on a slow consumer.
    pub subscriber_buffer_bytes: usize,
}

impl Limits {
    /// Worst-case header memory a single hostile connection can pin, in bytes.
    pub const fn worst_case_header_memory(&self) -> u64 {
        self.max_header_bytes * self.max_concurrent_uni_streams as u64
    }
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_header_bytes: 16 * 1024,
            max_concurrent_uni_streams: 2048,
            stream_receive_window: 1024 * 1024,
            connection_receive_window: 16 * 1024 * 1024,
            max_connections: 1024,
            max_pending: 4096,
            endpoint_queue: 256,
            hello_timeout_ms: 10_000,
            max_subscriptions: 256,
            subscriber_buffer_bytes: 8 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_protocol_document() {
        let l = Limits::default();
        assert_eq!(l.max_header_bytes, 16384);
        assert_eq!(l.max_concurrent_uni_streams, 2048);
        assert_eq!(l.stream_receive_window, 1 << 20);
        assert_eq!(l.connection_receive_window, 16 << 20);
        assert_eq!(l.max_connections, 1024);
        assert_eq!(l.max_pending, 4096);
        assert_eq!(l.endpoint_queue, 256);
        assert_eq!(l.hello_timeout_ms, 10_000);
        assert_eq!(l.max_subscriptions, 256);
        assert_eq!(l.subscriber_buffer_bytes, 8 << 20);
    }

    #[test]
    fn worst_case_header_memory_is_32_mib() {
        assert_eq!(Limits::default().worst_case_header_memory(), 32 << 20);
    }
}
