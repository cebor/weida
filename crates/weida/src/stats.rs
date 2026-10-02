//! Connection statistics, in weida's own types
//! ([decisions/0036](../../../docs/decisions/0036-connection-statistics.md)).
//!
//! Everything here is copied out of one `quinn::Connection::stats()` call, so
//! no `quinn` type is part of the public surface
//! ([decisions/0034](../../../docs/decisions/0034-late-is-lost.md) §4.9).
//!
//! **No statistics type carries an address** (0036 §4.4). The only label is
//! the URL the application dialled, which it already holds and can leave off a
//! screen; a socket address — remote or local, resolved or observed — appears
//! in none of them. `tests::statistics_types_carry_no_address` holds every
//! field of every type here, and of [`crate::FlowStats`], to that rule.

use std::sync::Arc;
use std::time::Duration;

/// What the path under a connection looks like now — what a jitter buffer or
/// a bitrate controller sizes itself by.
///
/// `quinn` 0.11 reports no RTT variation, so none is passed through
/// (0036 §4.7); `min_rtt` beside `rtt` gives the floor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct PathStats {
    /// Smoothed round-trip time.
    pub rtt: Duration,
    /// The smallest round-trip time seen on this path, ignoring ack delay.
    pub min_rtt: Duration,
    /// Congestion window, in bytes.
    pub cwnd: u64,
    /// Congestion events the controller has reacted to.
    pub congestion_events: u64,
    /// Packets this side sent and declared lost.
    pub lost_packets: u64,
    /// Bytes this side sent and declared lost.
    pub lost_bytes: u64,
    /// Packets this side sent.
    pub sent_packets: u64,
    /// The largest UDP payload the path carries now.
    pub current_mtu: u16,
    /// The largest datagram the connection carries now, prefix included;
    /// `None` when the peer accepts none.
    pub max_datagram_size: Option<usize>,
}

/// UDP datagrams and the bytes inside them, in one direction of one
/// connection. Every QUIC packet travels in one, so this counts all of a
/// connection's traffic, not one flow's payload.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct UdpCounts {
    /// UDP datagrams.
    pub datagrams: u64,
    /// Bytes inside them.
    pub bytes: u64,
}

/// The transport's view of one QUIC connection: its path and its traffic in
/// both directions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TransportStats {
    /// The path.
    pub path: PathStats,
    /// What this side sent.
    pub tx: UdpCounts,
    /// What this side received.
    pub rx: UdpCounts,
}

/// One live connection of a dialling handle, labelled by the address the
/// application dialled (0036 §4.1).
///
/// The record is per dialled address: `url` and `redials` belong to the
/// handle, `age` and `transport` to the connection. A connection the pool
/// shares between two handles that dialled the same URL on the same terms
/// appears under both, with the same transport counters (0036 §4.6).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectionStats {
    /// The URL exactly as the application gave it to `connect`.
    pub url: Arc<str>,
    /// How long the current connection has been up. A redial starts it
    /// again.
    pub age: Duration,
    /// Successful transparent redials of this address
    /// ([decisions/0031](../../../docs/decisions/0031-transparent-redial-and-the-sender-outbox.md)).
    /// The first `connect` is not one.
    pub redials: u64,
    /// The transport's numbers; `None` on a local transport, which has no
    /// path to measure.
    pub transport: Option<TransportStats>,
}

impl TransportStats {
    /// Copies `quinn`'s statistics of `conn` into weida's types.
    pub(crate) fn of(conn: &quinn::Connection) -> TransportStats {
        let stats = conn.stats();
        let path = stats.path;
        let udp = |s: quinn::UdpStats| UdpCounts {
            datagrams: s.datagrams,
            bytes: s.bytes,
        };
        TransportStats {
            path: PathStats {
                rtt: path.rtt,
                min_rtt: path.min_rtt,
                cwnd: path.cwnd,
                congestion_events: path.congestion_events,
                lost_packets: path.lost_packets,
                lost_bytes: path.lost_bytes,
                sent_packets: path.sent_packets,
                current_mtu: path.current_mtu,
                max_datagram_size: conn.max_datagram_size(),
            },
            tx: udp(stats.udp_tx),
            rx: udp(stats.udp_rx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FlowStats;

    /// A field type a statistics type may carry. There is deliberately no
    /// implementation for any address type: adding one is the line a review
    /// has to refuse (0036 §4.4).
    trait NoAddress {}
    impl NoAddress for u16 {}
    impl NoAddress for u64 {}
    impl NoAddress for usize {}
    impl NoAddress for Duration {}
    impl NoAddress for Arc<str> {}
    impl NoAddress for PathStats {}
    impl NoAddress for UdpCounts {}
    impl NoAddress for TransportStats {}
    impl<T: NoAddress> NoAddress for Option<T> {}

    fn allowed<T: NoAddress>(_: &T) {}

    /// Fails to compile when a field is added to a statistics type until the
    /// field is named here, and fails again when its type is an address.
    #[test]
    fn statistics_types_carry_no_address() {
        fn path(s: &PathStats) {
            let PathStats {
                rtt,
                min_rtt,
                cwnd,
                congestion_events,
                lost_packets,
                lost_bytes,
                sent_packets,
                current_mtu,
                max_datagram_size,
            } = s;
            allowed(rtt);
            allowed(min_rtt);
            allowed(cwnd);
            allowed(congestion_events);
            allowed(lost_packets);
            allowed(lost_bytes);
            allowed(sent_packets);
            allowed(current_mtu);
            allowed(max_datagram_size);
        }
        fn udp(s: &UdpCounts) {
            let UdpCounts { datagrams, bytes } = s;
            allowed(datagrams);
            allowed(bytes);
        }
        fn transport(s: &TransportStats) {
            let TransportStats { path, tx, rx } = s;
            allowed(path);
            allowed(tx);
            allowed(rx);
        }
        fn connection(s: &ConnectionStats) {
            let ConnectionStats {
                url,
                age,
                redials,
                transport,
            } = s;
            allowed(url);
            allowed(age);
            allowed(redials);
            allowed(transport);
        }
        fn flow(s: &FlowStats) {
            let FlowStats {
                sent,
                too_large,
                discarded,
                not_live,
                received,
                overflow,
            } = s;
            allowed(sent);
            allowed(too_large);
            allowed(discarded);
            allowed(not_live);
            allowed(received);
            allowed(overflow);
        }

        let path_stats = PathStats {
            rtt: Duration::from_millis(20),
            min_rtt: Duration::from_millis(18),
            cwnd: 12_000,
            congestion_events: 0,
            lost_packets: 0,
            lost_bytes: 0,
            sent_packets: 10,
            current_mtu: 1200,
            max_datagram_size: None,
        };
        let transport_stats = TransportStats {
            path: path_stats,
            tx: UdpCounts::default(),
            rx: UdpCounts::default(),
        };
        path(&path_stats);
        udp(&transport_stats.tx);
        transport(&transport_stats);
        connection(&ConnectionStats {
            url: Arc::from("weida://example.org:4433/voice"),
            age: Duration::ZERO,
            redials: 0,
            transport: Some(transport_stats),
        });
        flow(&FlowStats::default());
    }
}
