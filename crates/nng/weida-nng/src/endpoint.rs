//! Endpoints: `transport://address`, parsed and bounded.
//!
//! Four transports, which are the ones this library implements: `tcp`,
//! `ipc`, `inproc` and `tls+tcp`. NNG 1.10 exposes three more — `ws`, `wss`
//! and the experimental `zt` — and SP's RFCs define a UDP mapping
//! (`docs/research/nanomsg-nng.md` §0, §11); every one of them is named
//! absent with its reason rather than treated as a typo, because a caller
//! who wrote `zt://` made a supportable request that this library refuses
//! and [`Error::ENOTSUP`] is the answer NNG itself gives for a transport it
//! was not built with.
//!
//! Parsing happens **before** anything is bound or dialled, and the length
//! rules are NNG's own numbers rather than ours, so a program ported from
//! NNG meets the same refusals at the same points (§11).

use std::fmt;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::error::{Error, Result};

/// Longest endpoint URL, in bytes, scheme included.
///
/// NNG's `NNG_MAXADDRLEN`, which `nng.h` fixes at 128 and the manual calls
/// implementation-defined rather than an SP wire limit (§11). It bounds the
/// whole string, which is what NNG bounds, so an over-long URL is refused
/// here instead of being truncated somewhere inside a transport.
pub const NNG_MAXADDRLEN: usize = 128;

/// Longest `ipc://` path that a legacy nanomsg or mangos peer can also
/// address, in bytes.
///
/// "IPC path compatibility with legacy nanomsg requires at most 122 bytes
/// including NUL" (§11), so 121 bytes of path. It is a legacy URL
/// representation constraint rather than a kernel one, and it is the tighter
/// of the two budgets only on paper: the kernel's `sun_path` is
/// [`weida_core::MAX_SOCKET_PATH_BYTES`] — 107 on Linux — and is what
/// actually fails a bind. Both are checked, and the error says which.
pub const MAX_LEGACY_IPC_PATH_BYTES: usize = 121;

/// Longest `inproc://` name, in bytes.
///
/// NNG publishes no `inproc` name limit — its `inproc(7)` page says only
/// that the name is an arbitrary string (§11) — so the bound is derived from
/// the one NNG does publish: a name that fits [`NNG_MAXADDRLEN`] with
/// `inproc://` in front of it. A name is remote input the moment it comes
/// from a configuration file or a peer's address, so it has a bound
/// (`docs/INVARIANTS.md`); deriving it from `NNG_MAXADDRLEN` means the bound
/// is one number rather than two.
pub const MAX_INPROC_NAME_BYTES: usize = NNG_MAXADDRLEN - "inproc://".len();

/// Transports NNG has or SP defines and this library does not implement,
/// each with the reason its absence is a decision rather than an oversight.
const ABSENT_TRANSPORTS: &[(&str, &str)] = &[
    (
        "ws",
        "the WebSocket mapping needs an HTTP server and is out of scope until a user asks",
    ),
    (
        "wss",
        "the WebSocket mapping needs an HTTP server and is out of scope until a user asks",
    ),
    (
        "zt",
        "ZeroTier is experimental in NNG itself and its connection setup can take a minute",
    ),
    (
        "udp",
        "SP's UDP mapping is a datagram mapping with no pipe and is out of scope",
    ),
    (
        "socket",
        "NNG's inherited-file-descriptor form has no address to parse",
    ),
    (
        "abstract",
        "the Linux abstract AF_UNIX namespace has no filesystem permissions to check",
    ),
    (
        "tcp4",
        "the address-family-pinned form is absent; write an IPv4 literal, or a name whose \
         resolved addresses this library tries in order",
    ),
    (
        "tcp6",
        "the address-family-pinned form is absent; write a bracketed IPv6 literal, or a name \
         whose resolved addresses this library tries in order",
    ),
    (
        "tls+tcp4",
        "the address-family-pinned form is absent; write an IPv4 literal",
    ),
    (
        "tls+tcp6",
        "the address-family-pinned form is absent; write a bracketed IPv6 literal",
    ),
];

/// What a `tcp://` or `tls+tcp://` endpoint names as its host.
///
/// Three shapes, kept apart because they mean different things at bind and
/// dial time: a wildcard binds every interface, a literal needs no resolver,
/// and a name is resolved where it is used — and, for TLS, is also the name
/// the peer's certificate is checked against (§1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TcpHost {
    /// `*`: every interface. `INADDR_ANY`, and only meaningful for a listen.
    Any,
    /// An IPv4 or IPv6 literal. An IPv6 literal is bracketed:
    /// `tcp://[::1]:5555`.
    Ip(IpAddr),
    /// A hostname, resolved where it is used.
    Name(String),
}

impl fmt::Display for TcpHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TcpHost::Any => f.write_str("*"),
            TcpHost::Ip(ip) if ip.is_ipv6() => write!(f, "[{ip}]"),
            TcpHost::Ip(ip) => write!(f, "{ip}"),
            TcpHost::Name(name) => f.write_str(name),
        }
    }
}

/// A parsed NNG endpoint URL.
///
/// `Display` renders the canonical string again, which is what
/// `NNG_OPT_URL` reports on a dialer or listener — and the reason a wildcard
/// port has to be read back at all: a caller that listened on `tcp://*:0`
/// cannot know the port it got without asking.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Endpoint {
    /// `tcp://host:port`. Port `0` is the wildcard the OS fills in.
    Tcp {
        /// The interface, literal or name this endpoint names.
        host: TcpHost,
        /// The port, or `0` for "whatever the OS gives me".
        port: u16,
    },
    /// `tls+tcp://host:port`: the same TCP endpoint under TLS 1.2 or better,
    /// with the server name taken from the URL when it is a name (§1, §10).
    TlsTcp {
        /// The interface, literal or name this endpoint names.
        host: TcpHost,
        /// The port, or `0` for "whatever the OS gives me".
        port: u16,
    },
    /// `ipc://path`: an `AF_UNIX` socket on the filesystem.
    Ipc(PathBuf),
    /// `inproc://name`: a name in this process's own namespace, scoped to the
    /// [`Context`](crate::Context) that holds it.
    Inproc(String),
}

impl Endpoint {
    /// Parses `url`, refusing everything NNG refuses and naming why.
    ///
    /// The [`NNG_MAXADDRLEN`] budget is checked first, on the whole string,
    /// because an over-long URL is refused whatever its transport turns out
    /// to be.
    pub fn parse(url: &str) -> Result<Endpoint> {
        if url.is_empty() || url.len() > NNG_MAXADDRLEN {
            return Err(Error::EADDRINVAL(
                format!(
                    "an endpoint URL must be 1..={NNG_MAXADDRLEN} bytes (NNG_MAXADDRLEN); \
                     this one is {} bytes",
                    url.len()
                )
                .into(),
            ));
        }
        let Some((transport, address)) = url.split_once("://") else {
            return Err(Error::EADDRINVAL(
                format!("an endpoint must be transport://address: {url:?}").into(),
            ));
        };
        match transport {
            "tcp" => parse_tcp(address, false),
            "tls+tcp" => parse_tcp(address, true),
            "ipc" => parse_ipc(address),
            "inproc" => parse_inproc(address),
            other => Err(match ABSENT_TRANSPORTS.iter().find(|(name, _)| *name == other) {
                Some((_, why)) => Error::ENOTSUP(
                    format!("the {other} transport is not implemented: {why}").into(),
                ),
                None => Error::ENOTSUP(
                    format!(
                        "unknown transport {other:?}; this library speaks tcp, tls+tcp, ipc and \
                         inproc"
                    )
                    .into(),
                ),
            }),
        }
    }

    /// The transport name this endpoint would be written with.
    pub const fn transport(&self) -> &'static str {
        match self {
            Endpoint::Tcp { .. } => "tcp",
            Endpoint::TlsTcp { .. } => "tls+tcp",
            Endpoint::Ipc(_) => "ipc",
            Endpoint::Inproc(_) => "inproc",
        }
    }

    /// Whether the port is the wildcard, so that the listening endpoint has
    /// to be read back to be useful.
    pub const fn has_wildcard_port(&self) -> bool {
        matches!(
            self,
            Endpoint::Tcp { port: 0, .. } | Endpoint::TlsTcp { port: 0, .. }
        )
    }

    /// Whether this endpoint's bytes cross a kernel or a network, and
    /// therefore whether `NNG_OPT_RECVMAXSZ` means anything on it.
    ///
    /// `inproc` "accepts but deliberately ignores `RECVMAXSZ`, because peers
    /// share an address space" (§3), and that decision is a property of the
    /// transport rather than of each socket that uses one.
    pub const fn enforces_recv_max_size(&self) -> bool {
        !matches!(self, Endpoint::Inproc(_))
    }

    /// The host a TLS certificate would be checked against, for the one
    /// transport where that question has an answer.
    ///
    /// `None` for an IP literal and for the wildcard: NNG "can validate the
    /// server name from the dial URL" (§1), and a URL that names no server
    /// carries no name to validate.
    pub fn tls_server_name(&self) -> Option<&str> {
        match self {
            Endpoint::TlsTcp {
                host: TcpHost::Name(name),
                ..
            } => Some(name),
            _ => None,
        }
    }
}

fn parse_tcp(address: &str, tls: bool) -> Result<Endpoint> {
    let build = |host: TcpHost, port: u16| {
        if tls {
            Endpoint::TlsTcp { host, port }
        } else {
            Endpoint::Tcp { host, port }
        }
    };
    // An IPv6 literal is bracketed, because otherwise its own colons are
    // indistinguishable from the port separator.
    if let Some(rest) = address.strip_prefix('[') {
        let Some((literal, tail)) = rest.split_once(']') else {
            return Err(Error::EADDRINVAL(
                format!("unterminated IPv6 literal in tcp endpoint: {address:?}").into(),
            ));
        };
        let Some(port) = tail.strip_prefix(':') else {
            return Err(Error::EADDRINVAL(
                format!("a tcp endpoint needs host:port: {address:?}").into(),
            ));
        };
        let ip: IpAddr = literal
            .parse()
            .map_err(|_| Error::EADDRINVAL(format!("not an IP literal: {literal:?}").into()))?;
        return Ok(build(TcpHost::Ip(ip), parse_port(port, address)?));
    }
    let Some((host_text, port_text)) = address.rsplit_once(':') else {
        return Err(Error::EADDRINVAL(
            format!("a tcp endpoint needs host:port: {address:?}").into(),
        ));
    };
    if host_text.is_empty() {
        return Err(Error::EADDRINVAL(
            format!("a tcp endpoint needs a host: {address:?}").into(),
        ));
    }
    let port = parse_port(port_text, address)?;
    if host_text == "*" {
        return Ok(build(TcpHost::Any, port));
    }
    if let Ok(ip) = IpAddr::from_str(host_text) {
        return Ok(build(TcpHost::Ip(ip), port));
    }
    if host_text
        .bytes()
        .any(|b| b < 0x20 || b == b'/' || b == b'@')
    {
        return Err(Error::EADDRINVAL(
            format!("not a hostname: {host_text:?}").into(),
        ));
    }
    Ok(build(TcpHost::Name(host_text.to_owned()), port))
}

fn parse_port(text: &str, address: &str) -> Result<u16> {
    text.parse::<u16>().map_err(|_| {
        Error::EADDRINVAL(format!("not a port number: {text:?} in {address:?}").into())
    })
}

fn parse_ipc(address: &str) -> Result<Endpoint> {
    if address.is_empty() {
        return Err(Error::EADDRINVAL(
            "an ipc endpoint needs a path: \"ipc://\"".into(),
        ));
    }
    if address.as_bytes().contains(&0) {
        return Err(Error::EADDRINVAL(
            "an ipc path may not contain a NUL: the kernel would truncate it there".into(),
        ));
    }
    // Two budgets, and both are named where they bite. The kernel's is what
    // fails a bind; the legacy one is what a nanomsg or mangos peer can
    // address (§11).
    if address.len() > weida_core::MAX_SOCKET_PATH_BYTES {
        return Err(Error::EADDRINVAL(
            format!(
                "an ipc path is at most {} bytes on this platform (the kernel's sun_path \
                 budget); this one is {}",
                weida_core::MAX_SOCKET_PATH_BYTES,
                address.len()
            )
            .into(),
        ));
    }
    if address.len() > MAX_LEGACY_IPC_PATH_BYTES {
        return Err(Error::EADDRINVAL(
            format!(
                "an ipc path is at most {MAX_LEGACY_IPC_PATH_BYTES} bytes for legacy nanomsg \
                 compatibility (122 including NUL); this one is {}",
                address.len()
            )
            .into(),
        ));
    }
    Ok(Endpoint::Ipc(PathBuf::from(address)))
}

fn parse_inproc(address: &str) -> Result<Endpoint> {
    if address.is_empty() || address.len() > MAX_INPROC_NAME_BYTES {
        return Err(Error::EADDRINVAL(
            format!(
                "an inproc name must be 1..={MAX_INPROC_NAME_BYTES} bytes (NNG_MAXADDRLEN less \
                 the scheme): {address:?}"
            )
            .into(),
        ));
    }
    if address.bytes().any(|b| b < 0x20) {
        return Err(Error::EADDRINVAL(
            format!("invalid byte in inproc name: {address:?}").into(),
        ));
    }
    Ok(Endpoint::Inproc(address.to_owned()))
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Endpoint::Tcp { host, port } => write!(f, "tcp://{host}:{port}"),
            Endpoint::TlsTcp { host, port } => write!(f, "tls+tcp://{host}:{port}"),
            Endpoint::Ipc(path) => write!(f, "ipc://{}", path.display()),
            Endpoint::Inproc(name) => write!(f, "inproc://{name}"),
        }
    }
}

impl FromStr for Endpoint {
    type Err = Error;

    fn from_str(url: &str) -> Result<Endpoint> {
        Endpoint::parse(url)
    }
}

impl Endpoint {
    /// The filesystem path of an `ipc://` endpoint.
    pub fn ipc_path(&self) -> Option<&Path> {
        match self {
            Endpoint::Ipc(path) => Some(path),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Claim: the four transports this library implements round-trip through
    /// their canonical string, which is what `NNG_OPT_URL` reports.
    #[test]
    fn the_four_transports_round_trip() {
        for url in [
            "tcp://127.0.0.1:5555",
            "tcp://[::1]:5555",
            "tcp://*:0",
            "tcp://example.test:1234",
            "tls+tcp://example.test:443",
            "tls+tcp://[::1]:443",
            "ipc:///tmp/weida-nng.sock",
            "inproc://orders",
        ] {
            let parsed = Endpoint::parse(url).expect(url);
            assert_eq!(parsed.to_string(), url, "{url} did not round-trip");
            assert_eq!(Endpoint::from_str(url).expect(url), parsed);
        }
    }

    /// Claim: `NNG_MAXADDRLEN` bounds the whole URL, and it is checked before
    /// the transport is even looked at — an over-long `zt://` URL is refused
    /// for its length, not for its scheme.
    #[test]
    fn the_address_budget_bounds_the_whole_url() {
        let name = "a".repeat(NNG_MAXADDRLEN);
        let err = Endpoint::parse(&format!("inproc://{name}")).unwrap_err();
        assert!(matches!(err, Error::EADDRINVAL(_)), "{err:?}");
        assert!(err.cause().contains("NNG_MAXADDRLEN"));

        // Exactly at the budget is accepted.
        let fits = "a".repeat(NNG_MAXADDRLEN - "inproc://".len());
        let url = format!("inproc://{fits}");
        assert_eq!(url.len(), NNG_MAXADDRLEN);
        assert!(Endpoint::parse(&url).is_ok());
        assert_eq!(MAX_INPROC_NAME_BYTES, fits.len());
    }

    /// Claim: the two `ipc://` budgets are both enforced and the error says
    /// which one bit — the kernel's `sun_path` on this platform, and the
    /// 122-bytes-including-NUL form a legacy nanomsg peer can address.
    #[test]
    fn an_ipc_path_meets_both_of_its_budgets() {
        let kernel = weida_core::MAX_SOCKET_PATH_BYTES;
        assert!(
            kernel < MAX_LEGACY_IPC_PATH_BYTES,
            "on this platform the kernel's budget is the tighter one"
        );

        let ok = format!("/{}", "a".repeat(kernel - 1));
        assert!(Endpoint::parse(&format!("ipc://{ok}")).is_ok());

        let too_long_for_kernel = format!("/{}", "a".repeat(kernel));
        let err = Endpoint::parse(&format!("ipc://{too_long_for_kernel}")).unwrap_err();
        assert!(err.cause().contains("sun_path"), "{err}");

        // The legacy budget is not decoration: it is checked on its own, so a
        // platform whose kernel budget is the looser one still refuses a path
        // a nanomsg peer could not address.
        assert!(
            parse_ipc(&"a".repeat(MAX_LEGACY_IPC_PATH_BYTES + 1))
                .unwrap_err()
                .cause()
                .contains("sun_path")
                || parse_ipc(&"a".repeat(MAX_LEGACY_IPC_PATH_BYTES + 1))
                    .unwrap_err()
                    .cause()
                    .contains("legacy nanomsg")
        );
    }

    /// Claim: every transport NNG has or SP defines that this library does
    /// not implement is refused as `NNG_ENOTSUP` with a reason, and an
    /// unknown scheme is refused as a scheme rather than as a typo.
    #[test]
    fn absent_transports_are_named_rather_than_mistaken_for_typos() {
        for (transport, _) in ABSENT_TRANSPORTS {
            let err = Endpoint::parse(&format!("{transport}://host:1")).unwrap_err();
            assert!(matches!(err, Error::ENOTSUP(_)), "{transport}: {err:?}");
            assert!(
                err.cause().len() > transport.len() + 30,
                "{transport} is absent without a reason: {err}"
            );
        }
        let err = Endpoint::parse("carrier-pigeon://host:1").unwrap_err();
        assert!(matches!(err, Error::ENOTSUP(_)), "{err:?}");
        assert!(err.cause().contains("tls+tcp"));
    }

    /// Claim: a malformed URL is `NNG_EADDRINVAL`, which is NNG's code for an
    /// address it cannot use, and never `NNG_EINVAL`, which is for arguments.
    #[test]
    fn malformed_urls_are_address_errors() {
        for url in [
            "orders",
            "tcp://",
            "tcp://host",
            "tcp://:5555",
            "tcp://host:99999",
            "tcp://host:-1",
            "tcp://[::1:5555",
            "tcp://[::1]5555",
            "tcp://[not-an-ip]:1",
            "ipc://",
            "inproc://",
            "inproc://has\nnewline",
            "",
        ] {
            let err = Endpoint::parse(url).unwrap_err();
            assert!(
                matches!(err, Error::EADDRINVAL(_)),
                "{url:?} gave {err:?} rather than NNG_EADDRINVAL"
            );
        }
    }

    /// Claim: the wildcard port is visible in the parsed endpoint, because a
    /// caller that listens on one has to read the real endpoint back.
    #[test]
    fn a_wildcard_port_is_visible() {
        assert!(Endpoint::parse("tcp://*:0").unwrap().has_wildcard_port());
        assert!(
            Endpoint::parse("tls+tcp://*:0")
                .unwrap()
                .has_wildcard_port()
        );
        assert!(!Endpoint::parse("tcp://*:1").unwrap().has_wildcard_port());
        assert!(
            !Endpoint::parse("inproc://orders")
                .unwrap()
                .has_wildcard_port()
        );
    }

    /// Claim: `RECVMAXSZ` applies to every transport whose bytes leave the
    /// address space and to none that does not — which is NNG's own reason
    /// for `inproc` ignoring it (§3).
    #[test]
    fn inproc_is_the_transport_that_ignores_the_size_limit() {
        assert!(
            !Endpoint::parse("inproc://orders")
                .unwrap()
                .enforces_recv_max_size()
        );
        for url in ["tcp://h:1", "tls+tcp://h:1", "ipc:///tmp/s"] {
            assert!(
                Endpoint::parse(url).unwrap().enforces_recv_max_size(),
                "{url}"
            );
        }
    }

    /// Claim: only a TLS endpoint that names a host carries a name a
    /// certificate can be checked against; a literal carries none, and
    /// saying so here is what keeps a later verifier from inventing one.
    #[test]
    fn only_a_named_tls_host_has_a_server_name() {
        assert_eq!(
            Endpoint::parse("tls+tcp://peer.test:443")
                .unwrap()
                .tls_server_name(),
            Some("peer.test")
        );
        assert_eq!(
            Endpoint::parse("tls+tcp://10.0.0.1:443")
                .unwrap()
                .tls_server_name(),
            None
        );
        assert_eq!(
            Endpoint::parse("tcp://peer.test:443")
                .unwrap()
                .tls_server_name(),
            None
        );
    }
}
