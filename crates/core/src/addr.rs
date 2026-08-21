//! Endpoint addressing: `weida://host:port/path`.
//!
//! The path is an **opaque identifier** (master doc §4). Nothing in the core
//! interprets its structure: no hierarchy, no wildcards, no topic semantics.

use std::fmt;

use crate::error::Error;

/// URL scheme of the native transport.
pub const SCHEME: &str = "weida";

/// Maximum endpoint path length in bytes (also the wire cap for DATA key 0).
pub const MAX_PATH_BYTES: usize = 512;

/// Validates an endpoint path.
///
/// A path MUST start with `/`, be 1..=[`MAX_PATH_BYTES`] bytes long and contain
/// no byte below `0x20`. Every other byte sequence is accepted: the path is an
/// opaque identifier, so `*`, `..` and `%20` carry no meaning here.
pub fn validate_endpoint_path(path: &str) -> Result<(), Error> {
    if path.is_empty() || path.len() > MAX_PATH_BYTES {
        return Err(Error::InvalidEndpointPath);
    }
    if !path.starts_with('/') {
        return Err(Error::InvalidEndpointPath);
    }
    if path.bytes().any(|b| b < 0x20) {
        return Err(Error::InvalidEndpointPath);
    }
    Ok(())
}

/// A parsed `weida://host:port/path` address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointAddr {
    /// Host as written: a DNS name or an IP literal without brackets.
    pub host: String,
    /// Port; always explicit, there is no default.
    pub port: u16,
    /// Opaque endpoint identifier, starting with `/`.
    pub path: String,
}

impl EndpointAddr {
    /// Parses a `weida://host:port/path` URL.
    ///
    /// The port is mandatory (no well-known port is claimed), IPv6 literals are
    /// bracketed, and the path is validated by [`validate_endpoint_path`].
    pub fn parse(input: &str) -> Result<EndpointAddr, Error> {
        let invalid = |m: &str| Error::InvalidAddress(format!("{m}: {input:?}"));

        let rest = input
            .strip_prefix(SCHEME)
            .and_then(|r| r.strip_prefix("://"))
            .ok_or_else(|| invalid("expected scheme weida://"))?;

        let (authority, path) = match rest.find('/') {
            Some(i) => rest.split_at(i),
            None => return Err(invalid("missing endpoint path")),
        };

        let (host, port_str) = if let Some(after) = authority.strip_prefix('[') {
            let close = after
                .find(']')
                .ok_or_else(|| invalid("unterminated IPv6 literal"))?;
            let host = &after[..close];
            let port = after[close + 1..]
                .strip_prefix(':')
                .ok_or_else(|| invalid("missing port"))?;
            (host, port)
        } else {
            match authority.rsplit_once(':') {
                Some((h, p)) => (h, p),
                None => return Err(invalid("missing port")),
            }
        };

        if host.is_empty() {
            return Err(invalid("empty host"));
        }
        if host.bytes().any(|b| b < 0x20 || b == b'/' || b == b'@') {
            return Err(invalid("invalid byte in host"));
        }
        let port: u16 = port_str.parse().map_err(|_| invalid("port is not a u16"))?;
        if port == 0 {
            return Err(invalid("port 0 is not connectable"));
        }

        validate_endpoint_path(path).map_err(|_| invalid("invalid endpoint path"))?;

        Ok(EndpointAddr {
            host: host.to_owned(),
            port,
            path: path.to_owned(),
        })
    }

    /// True if the host is an IPv6 literal and must be bracketed when printed.
    fn host_needs_brackets(&self) -> bool {
        self.host.contains(':')
    }
}

impl fmt::Display for EndpointAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host_needs_brackets() {
            write!(f, "{SCHEME}://[{}]:{}{}", self.host, self.port, self.path)
        } else {
            write!(f, "{SCHEME}://{}:{}{}", self.host, self.port, self.path)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ipv4_authority() {
        let a = EndpointAddr::parse("weida://127.0.0.1:7443/transform").unwrap();
        assert_eq!(a.host, "127.0.0.1");
        assert_eq!(a.port, 7443);
        assert_eq!(a.path, "/transform");
        assert_eq!(a.to_string(), "weida://127.0.0.1:7443/transform");
    }

    #[test]
    fn parses_bracketed_ipv6_authority() {
        let a = EndpointAddr::parse("weida://[::1]:7443/x").unwrap();
        assert_eq!(a.host, "::1");
        assert_eq!(a.port, 7443);
        assert_eq!(a.path, "/x");
        assert_eq!(a.to_string(), "weida://[::1]:7443/x");
    }

    #[test]
    fn parses_dns_name_and_deep_path() {
        let a = EndpointAddr::parse("weida://broker.example.com:443/a/b/c").unwrap();
        assert_eq!(a.host, "broker.example.com");
        assert_eq!(a.path, "/a/b/c");
    }

    #[test]
    fn path_stays_opaque() {
        let a = EndpointAddr::parse("weida://h:1/important*/../%20").unwrap();
        assert_eq!(a.path, "/important*/../%20");
    }

    #[test]
    fn rejects_bad_addresses() {
        let cases = [
            "mq://127.0.0.1:7443/x",
            "weida://127.0.0.1/x",
            "weida://127.0.0.1:7443",
            "weida://:7443/x",
            "weida://127.0.0.1:0/x",
            "weida://127.0.0.1:99999/x",
            "weida://127.0.0.1:abc/x",
            "weida://[::1:7443/x",
            "weida://[::1]7443/x",
            "127.0.0.1:7443/x",
            "weida://127.0.0.1:7443/a\u{0}b",
            "weida://127.0.0.1:7443/a\u{1f}b",
            "weida://user@host:1/x",
        ];
        for c in cases {
            assert!(
                EndpointAddr::parse(c).is_err(),
                "expected rejection of {c:?}"
            );
        }
    }

    #[test]
    fn path_length_bounds() {
        let ok = format!("weida://h:1/{}", "a".repeat(MAX_PATH_BYTES - 1));
        assert!(EndpointAddr::parse(&ok).is_ok());
        let too_long = format!("weida://h:1/{}", "a".repeat(MAX_PATH_BYTES));
        assert!(EndpointAddr::parse(&too_long).is_err());
    }

    #[test]
    fn path_validation_rules() {
        assert!(validate_endpoint_path("/").is_ok());
        assert!(validate_endpoint_path("").is_err());
        assert!(validate_endpoint_path("no-leading-slash").is_err());
        assert!(validate_endpoint_path("/\t").is_err());
        assert!(validate_endpoint_path(&"/".repeat(MAX_PATH_BYTES)).is_ok());
        assert!(validate_endpoint_path(&"/".repeat(MAX_PATH_BYTES + 1)).is_err());
    }

    #[test]
    fn display_parse_roundtrip() {
        for s in [
            "weida://127.0.0.1:7443/transform",
            "weida://[::1]:7443/x",
            "weida://host:1/a",
        ] {
            let a = EndpointAddr::parse(s).unwrap();
            assert_eq!(EndpointAddr::parse(&a.to_string()).unwrap(), a);
        }
    }
}
