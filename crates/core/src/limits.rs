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
    /// live per-stream parse tasks for one-way transfers.
    pub max_concurrent_uni_streams: u32,
    /// QUIC concurrent inbound bidirectional streams; bounds live request
    /// exchanges per connection and, with `max_header_bytes`, worst-case
    /// header memory.
    pub max_concurrent_bidi_streams: u32,
    /// QUIC per-stream receive window in bytes. Bounds buffered payload for one
    /// transfer and provides backpressure to the sender.
    pub stream_receive_window: u64,
    /// QUIC per-connection receive window in bytes.
    pub connection_receive_window: u64,
    /// Concurrently accepted connections per binding. Excess connections are
    /// closed immediately with `LIMIT_EXCEEDED`.
    pub max_connections: usize,
    /// Depth of a replier's accept queue. Senders await a free slot, so QUIC
    /// flow control carries the backpressure to the peer.
    pub endpoint_queue: usize,
    /// How long to wait for the peer HELLO before closing the connection with
    /// `NEGOTIATION_FAILED`, in milliseconds.
    pub hello_timeout_ms: u64,
    /// Subscription filters one peer connection may hold at once, summed over
    /// every path. Exceeding it closes the connection with `LIMIT_EXCEEDED`:
    /// SUBSCRIBE has no stream to answer with an ERROR frame.
    pub max_subscriptions: usize,
    /// Payload bytes a publisher may hold queued for one subscriber. A message
    /// that does not fit is dropped for that subscriber and counted; the
    /// publisher never blocks on a slow consumer.
    pub subscriber_buffer_bytes: usize,
    /// Producer scopes — paths and topics — a receiver tracks per connection
    /// for gap detection or reassembly, when `PerProducer` ordering is
    /// negotiated. The peer chooses the scope names, so the table needs a
    /// ceiling; at the cap a new scope is simply not tracked, no gap is
    /// reported for it and nothing is held back for it.
    pub max_sequence_scopes: usize,
    /// Transfers a receiver may hold back at once, summed over scopes, when
    /// `PerProducer(reassemble)` ordering is negotiated. A held transfer is
    /// an unread stream, so the bytes it pins are quinn's — up to
    /// `stream_receive_window` for it and `connection_receive_window` for the
    /// hold as a whole. At the cap the oldest held transfer is released out
    /// of order with its gap reported, never held in a growing buffer.
    pub max_reorder_hold: usize,
    /// Identities a receiver remembers per connection for `Bounded`
    /// deduplication. The window bounds how long an identity is kept, not
    /// how many arrive within it, so the count needs its own ceiling; at the
    /// cap the oldest entry is evicted, which costs suppression rather than
    /// memory.
    pub max_dedup_entries: usize,
}

impl Limits {
    /// Worst-case header memory a single hostile connection can pin, in bytes.
    ///
    /// Both stream budgets count: a peer may open its full uni *and* bidi
    /// allowance, and every accepted stream starts with one header.
    pub const fn worst_case_header_memory(&self) -> u64 {
        self.max_header_bytes
            * (self.max_concurrent_uni_streams as u64 + self.max_concurrent_bidi_streams as u64)
    }
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_header_bytes: 16 * 1024,
            max_concurrent_uni_streams: 2048,
            max_concurrent_bidi_streams: 1024,
            stream_receive_window: 1024 * 1024,
            connection_receive_window: 16 * 1024 * 1024,
            max_connections: 1024,
            endpoint_queue: 256,
            hello_timeout_ms: 10_000,
            max_subscriptions: 256,
            subscriber_buffer_bytes: 8 * 1024 * 1024,
            max_sequence_scopes: 1024,
            max_reorder_hold: 256,
            max_dedup_entries: 4096,
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
        assert_eq!(l.max_concurrent_bidi_streams, 1024);
        assert_eq!(l.stream_receive_window, 1 << 20);
        assert_eq!(l.connection_receive_window, 16 << 20);
        assert_eq!(l.max_connections, 1024);
        assert_eq!(l.endpoint_queue, 256);
        assert_eq!(l.hello_timeout_ms, 10_000);
        assert_eq!(l.max_subscriptions, 256);
        assert_eq!(l.subscriber_buffer_bytes, 8 << 20);
        assert_eq!(l.max_sequence_scopes, 1024);
        assert_eq!(l.max_reorder_hold, 256);
        assert_eq!(l.max_dedup_entries, 4096);
    }

    #[test]
    fn worst_case_header_memory_is_48_mib() {
        assert_eq!(Limits::default().worst_case_header_memory(), 48 << 20);
    }
}
