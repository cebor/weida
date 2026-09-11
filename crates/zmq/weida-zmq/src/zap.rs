//! ZAP: the authorization decision, delegated to a handler
//! ([27/ZAP](https://rfc.zeromq.org/spec/27/)).
//!
//! **The library authenticates; the application authorizes.** A server checks
//! that a peer's credentials are well-formed and then asks a handler whether
//! they are *acceptable*, over an in-process request-reply dialog. That is
//! ZAP's whole design: "ZAP uses an *inprocess bridge* design. That is, ZAP
//! itself requires that the handler run as a thread within the same process
//! as the servers" (`docs/research/zeromq.md` §10).
//!
//! # The dialog
//!
//! The handler binds [`ZAP_ENDPOINT`] with a REP or ROUTER socket; the server
//! dials it. 27/ZAP's frames, as this library sends and reads them:
//!
//! ```text
//! request                      reply
//! <empty delimiter>            <the envelope, put back by the REP socket>
//! version   "1.0"              version   "1.0"
//! request id                   request id (echoed)
//! domain                       status code  200 | 300 | 400 | 500
//! address                      status text
//! identity                     user id
//! mechanism                    metadata
//! credentials (0..n frames)
//! ```
//!
//! The delimiter is what makes a plain REP handler work: "a terminal handler
//! behind unknown intermediaries MUST accept an address envelope consisting
//! of N routing ID frames followed by an empty address delimiter frame… and
//! MUST send the same envelope back with the reply. This is what a REP socket
//! does." This library dials with a DEALER socket — 27/ZAP allows "REQ or
//! DEALER" — and writes that envelope itself, so the frames above are exactly
//! what goes on the wire and nothing is added behind the caller's back.
//!
//! **Credentials per mechanism**: NULL has none, and the dialog then exists
//! so that a server can "filter bogus clients on the basis of IP address";
//! PLAIN has two frames, username and password. CURVE's public-key frame
//! belongs to the mechanism that is not implemented here.
//!
//! # What this library adds, and names
//!
//! For an `ipc://` peer there is no IP address to put in the `address` frame
//! and the thing that *is* known — the kernel's `SO_PEERCRED` answer — has no
//! field in 27/ZAP. It is offered to the handler as one extra trailing frame,
//! `X-Local-Principal: uid=…, gid=…, pid=…`, after the credentials:
//!
//! - It is **additional**, so a handler that reads the fields it knows (which
//!   is what a REP handler does) is unaffected.
//! - It is **named as an extension** by its `X-` prefix, the same convention
//!   37/ZMTP reserves for application metadata.
//! - It is the kernel's statement and not an identity
//!   ([0010](../../../docs/decisions/0010-local-transport.md) §4.4): the
//!   handler decides, the peer only reports, and nothing here turns it into a
//!   credential.
//!
//! # The user id is not an identity either
//!
//! A 200 reply may carry a user id, "in case of a 200 status, for use by
//! applications. For other statuses, it SHALL be empty". It is a
//! [`ZapUserId`] and it stays one:
//!
//! - It has **no conversion to or from weida's proved identities**
//!   ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4 item
//!   6). weida's `LocalPrincipal` is a kernel fact and its `Fingerprint` is a
//!   key; a ZAP user id is whatever string a handler chose. The three are
//!   different claims about different things, and no `From`, `AsRef` or
//!   `Deref` between them exists in this workspace.
//! - It is a **per-connection** fact held by the server, not a per-message
//!   label (`docs/research/zeromq.md` §12/P14). It is readable on the peer,
//!   as [`crate::Peer::user_id`], and it never touches the routing id.
//!
//! # One handler per context
//!
//! 27/ZAP says one handler per process; libzmq scopes it per context, and so
//! does this, because the endpoint is an `inproc://` name and that namespace
//! *is* the context ([`crate::Inproc`]). The rule needs no separate check: a
//! second bind of the name is `EADDRINUSE`.
//!
//! A ZAP dialog that malfunctions — a version other than 1.0, a status code
//! 27/ZAP does not define, a reply that answers another request — is reported
//! as `ENOCOMPATPROTO`, the same errno a protocol violation by a peer gets.
//! libzmq has no errno for it either: it reports the family through
//! `zmq_socket_monitor`'s `ZMQ_PROTOCOL_ERROR_ZAP_*` values and fails the
//! handshake, which is what happens here.
//!
//! "The handler SHALL start before any server starts" is the handler's
//! responsibility and cannot be checked from here — what can be, and is, is
//! that a server which must authorize and finds no handler refuses the
//! connection instead of admitting it.

use std::sync::{Arc, Mutex};

use weida_core::LocalPrincipal;

use crate::context::Context;
use crate::dealerrouter::DealerSocket;
use crate::error::{Error, Result};
use crate::message::{Message, Multipart};
use crate::options::{Security, SocketOptions};

/// The endpoint a ZAP handler binds, fixed by 27/ZAP.
pub const ZAP_ENDPOINT: &str = "inproc://zeromq.zap.01";

/// The `inproc://` name inside [`ZAP_ENDPOINT`], for a namespace lookup.
pub const ZAP_NAME: &str = "zeromq.zap.01";

/// The only ZAP version, sent in every request and expected in every reply.
pub const ZAP_VERSION: &[u8] = b"1.0";

/// A handler's answer, as its status code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZapStatus {
    /// `200`: the peer is authorized, and a user id may accompany it.
    Allowed,
    /// `300`: a temporary failure — the handler could not decide now. The
    /// connection is refused; a client may retry.
    TemporaryError,
    /// `400`: authentication failed. The credentials were read and rejected.
    AuthenticationFailure,
    /// `500`: the handler broke. Distinct from `300` by the handler's
    /// judgement, which is where 27/ZAP leaves it.
    InternalError,
}

impl ZapStatus {
    /// The three octets on the wire.
    pub const fn code(&self) -> &'static str {
        match self {
            ZapStatus::Allowed => "200",
            ZapStatus::TemporaryError => "300",
            ZapStatus::AuthenticationFailure => "400",
            ZapStatus::InternalError => "500",
        }
    }

    /// Reads a status frame, refusing anything 27/ZAP does not define — a
    /// handler that invents a code has malfunctioned, and guessing which way
    /// it meant would be worse than failing.
    pub fn parse(code: &[u8]) -> Option<ZapStatus> {
        match code {
            b"200" => Some(ZapStatus::Allowed),
            b"300" => Some(ZapStatus::TemporaryError),
            b"400" => Some(ZapStatus::AuthenticationFailure),
            b"500" => Some(ZapStatus::InternalError),
            _ => None,
        }
    }

    /// Whether this status admits the connection.
    pub const fn is_allowed(&self) -> bool {
        matches!(self, ZapStatus::Allowed)
    }
}

impl std::fmt::Display for ZapStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

/// The user id a handler returned with a `200`.
///
/// **A string a handler chose, and nothing more.** It is deliberately not
/// convertible to any weida identity: `weida_core::Fingerprint` is a public
/// key and `weida_core::LocalPrincipal` is a kernel fact, while this is an
/// application's own name for whoever authenticated
/// ([0013](../../../docs/decisions/0013-competitor-libraries.md) §4.4 item
/// 6). Compare it with what your handler issued; do not treat it as proof of
/// anything this library checked.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ZapUserId(String);

impl ZapUserId {
    /// Wraps a user id a handler produced.
    pub fn new(id: impl Into<String>) -> ZapUserId {
        ZapUserId(id.into())
    }

    /// The id as the handler wrote it.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ZapUserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where a server records what a handler said about one connection.
///
/// Shared with the peer entry the engine holds, the way the announced routing
/// identity is: the session learns it, the socket reads it. Cloning shares the
/// slot.
#[derive(Clone, Debug, Default)]
pub struct AuthenticatedUser(Arc<Mutex<Option<ZapUserId>>>);

impl AuthenticatedUser {
    /// Records the user id of a `200` reply.
    pub fn set(&self, id: Option<ZapUserId>) {
        *self.0.lock().expect("authenticated user poisoned") = id;
    }

    /// What the handler said, if anything.
    pub fn get(&self) -> Option<ZapUserId> {
        self.0.lock().expect("authenticated user poisoned").clone()
    }
}

/// One authorization question, as 27/ZAP's frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZapRequest {
    /// The request id, echoed by the reply. Any opaque blob; this library
    /// uses a counter, which is enough because the dialog is one exchange on
    /// one socket.
    pub request_id: Vec<u8>,
    /// `ZMQ_ZAP_DOMAIN`, whose meaning is the application's.
    pub domain: String,
    /// The peer's address: an IP for `tcp://`, empty where there is none,
    /// which is every local transport.
    pub address: String,
    /// The peer's `Identity` metadata property, if it announced one. At most
    /// 255 octets, and self-asserted.
    pub identity: Vec<u8>,
    /// The mechanism whose credentials follow: `NULL`, `PLAIN` or `CURVE`.
    pub mechanism: String,
    /// The credential frames, per mechanism: none for NULL, username and
    /// password for PLAIN.
    pub credentials: Vec<Vec<u8>>,
    /// The kernel's statement about a local peer, offered as the extension
    /// frame this module documents. `None` for every transport that has no
    /// such fact.
    pub local_principal: Option<LocalPrincipal>,
}

/// The extension frame's prefix, `X-` as 37/ZMTP reserves for applications.
const LOCAL_PRINCIPAL_PREFIX: &str = "X-Local-Principal: ";

impl ZapRequest {
    /// The frames of the request, delimiter included.
    pub fn encode(&self) -> Multipart {
        let mut frames = vec![
            Message::empty(),
            Message::from(ZAP_VERSION.to_vec()),
            Message::from(self.request_id.clone()),
            Message::from(self.domain.clone()),
            Message::from(self.address.clone()),
            Message::from(self.identity.clone()),
            Message::from(self.mechanism.clone()),
        ];
        for credential in &self.credentials {
            frames.push(Message::from(credential.clone()));
        }
        if let Some(principal) = &self.local_principal {
            frames.push(Message::from(format!(
                "{LOCAL_PRINCIPAL_PREFIX}uid={}, gid={}, pid={}",
                principal.uid,
                principal.gid,
                match principal.pid {
                    Some(pid) => pid.to_string(),
                    // macOS's LOCAL_PEERCRED reports none, and a pid is an
                    // observation even where it exists.
                    None => "unknown".to_owned(),
                }
            )));
        }
        Multipart::new(frames).expect("a ZAP request has frames")
    }

    /// Reads a request a handler received, for a handler written in Rust.
    ///
    /// Accepts the address envelope 27/ZAP describes: any number of routing
    /// frames before the empty delimiter, which a REP socket has already
    /// stripped and a ROUTER socket has not.
    pub fn decode(message: &Multipart) -> Result<ZapRequest> {
        let frames: Vec<&[u8]> = message.frames().iter().map(Message::as_slice).collect();
        let body = strip_envelope(&frames);
        let field = |index: usize, what: &str| -> Result<&[u8]> {
            body.get(index).copied().ok_or_else(|| {
                Error::ENOCOMPATPROTO(format!("a ZAP request has no {what} frame").into())
            })
        };
        let version = field(0, "version")?;
        if version != ZAP_VERSION {
            return Err(Error::ENOCOMPATPROTO(
                format!(
                    "a ZAP request announced version {:?}; this library speaks 1.0 only",
                    String::from_utf8_lossy(version)
                )
                .into(),
            ));
        }
        let mut credentials = Vec::new();
        let mut local_principal = None;
        for frame in body.iter().skip(6) {
            match std::str::from_utf8(frame)
                .ok()
                .and_then(|text| text.strip_prefix(LOCAL_PRINCIPAL_PREFIX))
            {
                Some(text) => local_principal = parse_local_principal(text),
                None => credentials.push(frame.to_vec()),
            }
        }
        Ok(ZapRequest {
            request_id: field(1, "request id")?.to_vec(),
            domain: text_of(field(2, "domain")?),
            address: text_of(field(3, "address")?),
            identity: field(4, "identity")?.to_vec(),
            mechanism: text_of(field(5, "mechanism")?),
            credentials,
            local_principal,
        })
    }

    /// The PLAIN username and password, where this is a PLAIN request.
    pub fn plain_credentials(&self) -> Option<(&[u8], &[u8])> {
        if self.mechanism != "PLAIN" {
            return None;
        }
        match self.credentials.as_slice() {
            [username, password] => Some((username, password)),
            _ => None,
        }
    }
}

/// One authorization answer, as 27/ZAP's frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZapReply {
    /// The request id, echoed from the request. A reply carrying a different
    /// one is refused: it answers a question nobody asked.
    pub request_id: Vec<u8>,
    /// The decision.
    pub status: ZapStatus,
    /// A short text for a log, whose contents are the handler's.
    pub text: String,
    /// The user id, for a `200`. "For other statuses, it SHALL be empty."
    pub user_id: Option<ZapUserId>,
    /// Metadata the handler attached, unused by this library and passed
    /// nowhere: a property dictionary whose meaning is an application's.
    pub metadata: Vec<u8>,
}

impl ZapReply {
    /// A `200` with a user id.
    pub fn allowed(request_id: Vec<u8>, user_id: impl Into<String>) -> ZapReply {
        ZapReply {
            request_id,
            status: ZapStatus::Allowed,
            text: String::new(),
            user_id: Some(ZapUserId::new(user_id)),
            metadata: Vec::new(),
        }
    }

    /// A refusal with a reason for the log.
    pub fn refused(request_id: Vec<u8>, status: ZapStatus, text: impl Into<String>) -> ZapReply {
        ZapReply {
            request_id,
            status,
            text: text.into(),
            user_id: None,
            metadata: Vec::new(),
        }
    }

    /// The frames of the reply, **without** an address envelope — what a
    /// handler on a REP socket sends, because "this is what a REP socket
    /// does": it remembers the envelope of the request and puts it back. A
    /// handler on a ROUTER socket prepends the envelope itself.
    pub fn encode(&self) -> Multipart {
        Multipart::new(vec![
            Message::from(ZAP_VERSION.to_vec()),
            Message::from(self.request_id.clone()),
            Message::from(self.status.code()),
            Message::from(self.text.clone()),
            Message::from(
                self.user_id
                    .as_ref()
                    .map(|id| id.as_str().to_owned())
                    .unwrap_or_default(),
            ),
            Message::from(self.metadata.clone()),
        ])
        .expect("a ZAP reply has frames")
    }

    /// Reads a reply, refusing a malformed one rather than guessing.
    pub fn decode(message: &Multipart) -> Result<ZapReply> {
        let frames: Vec<&[u8]> = message.frames().iter().map(Message::as_slice).collect();
        let body = strip_envelope(&frames);
        let field = |index: usize, what: &str| -> Result<&[u8]> {
            body.get(index).copied().ok_or_else(|| {
                Error::ENOCOMPATPROTO(format!("a ZAP reply has no {what} frame").into())
            })
        };
        let version = field(0, "version")?;
        if version != ZAP_VERSION {
            return Err(Error::ENOCOMPATPROTO(
                format!(
                    "a ZAP handler answered version {:?}; this library speaks 1.0 only",
                    String::from_utf8_lossy(version)
                )
                .into(),
            ));
        }
        let code = field(2, "status code")?;
        let status = ZapStatus::parse(code).ok_or_else(|| {
            Error::ENOCOMPATPROTO(
                format!(
                    "a ZAP handler answered status {:?}, which 27/ZAP does not define",
                    String::from_utf8_lossy(code)
                )
                .into(),
            )
        })?;
        let user = text_of(field(4, "user id")?);
        if !status.is_allowed() && !user.is_empty() {
            // "For other statuses, it SHALL be empty." A handler that names a
            // user while refusing one is confused about which it means.
            return Err(Error::ENOCOMPATPROTO(
                format!("a ZAP handler returned a user id with status {status}").into(),
            ));
        }
        Ok(ZapReply {
            request_id: field(1, "request id")?.to_vec(),
            status,
            text: text_of(field(3, "status text")?),
            user_id: (!user.is_empty()).then(|| ZapUserId::new(user)),
            metadata: body.get(5).copied().unwrap_or_default().to_vec(),
        })
    }
}

/// Asks the context's handler about one connection.
///
/// One DEALER socket per question, which is a socket slot for the duration of
/// the handshake — libzmq spends one too, per session. The socket is closed
/// before this returns.
///
/// Fails with `ENOTSOCK` when no handler is bound: "The handler SHALL start
/// before any server starts", and a server that cannot ask must not admit.
pub async fn authorize(context: &Context, request: &ZapRequest) -> Result<ZapReply> {
    if !context.inproc().is_bound(ZAP_NAME) {
        return Err(Error::ENOTSOCK(
            format!(
                "no ZAP handler is bound at {ZAP_ENDPOINT} in this context, so this connection \
                 cannot be authorized"
            )
            .into(),
        ));
    }
    let mut dialog = DealerSocket::with_options(
        context,
        SocketOptions {
            // The handshake's own bound is ZMQ_HANDSHAKE_IVL, applied by the
            // engine to the whole session; this socket must not add a second
            // clock that could expire mid-dialog.
            send_timeout: None,
            recv_timeout: None,
            ..SocketOptions::default()
        },
    )?;
    dialog.connect(ZAP_ENDPOINT)?;
    dialog.send(request.encode()).await?;
    let reply = ZapReply::decode(&dialog.recv().await?);
    dialog.close();
    let reply = reply?;
    if reply.request_id != request.request_id {
        return Err(Error::ENOCOMPATPROTO(
            "a ZAP handler echoed a different request id, so its reply answers another question"
                .into(),
        ));
    }
    Ok(reply)
}

/// The mechanism name and credential frames a security choice sends.
pub fn credentials_of(security: Security, options: &SocketOptions) -> (String, Vec<Vec<u8>>) {
    match security {
        Security::Null => ("NULL".to_owned(), Vec::new()),
        Security::PlainServer | Security::PlainClient => (
            "PLAIN".to_owned(),
            vec![
                options
                    .plain_username
                    .clone()
                    .unwrap_or_default()
                    .into_bytes(),
                options
                    .plain_password
                    .clone()
                    .unwrap_or_default()
                    .into_bytes(),
            ],
        ),
    }
}

/// Drops the address envelope, if there is one.
///
/// 27/ZAP's envelope is "N routing ID frames followed by an empty address
/// delimiter frame", and a REP socket has already stripped it while a ROUTER
/// socket has not — so both shapes arrive here. **Scanning for the first
/// empty frame is not enough**: the `identity` field is legitimately empty
/// for a peer that announced none, so a scan would mistake a field for the
/// delimiter. The version frame is the anchor instead: where it is first,
/// there is no envelope.
fn strip_envelope<'a>(frames: &'a [&'a [u8]]) -> &'a [&'a [u8]] {
    if frames.first() == Some(&ZAP_VERSION) {
        return frames;
    }
    match frames.iter().position(|frame| frame.is_empty()) {
        Some(delimiter) => &frames[delimiter + 1..],
        None => frames,
    }
}

fn text_of(frame: &[u8]) -> String {
    String::from_utf8_lossy(frame).into_owned()
}

/// Reads back the extension frame this module writes. A frame that does not
/// parse is treated as absent rather than as an error: it is an extension,
/// and a strict handler is allowed to have rewritten it.
fn parse_local_principal(text: &str) -> Option<LocalPrincipal> {
    let mut uid = None;
    let mut gid = None;
    let mut pid = None;
    for field in text.split(',') {
        let (name, value) = field.trim().split_once('=')?;
        match name {
            "uid" => uid = value.parse().ok(),
            "gid" => gid = value.parse().ok(),
            "pid" => pid = value.parse().ok(),
            _ => {}
        }
    }
    Some(LocalPrincipal {
        uid: uid?,
        gid: gid?,
        pid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> ZapRequest {
        ZapRequest {
            request_id: b"1".to_vec(),
            domain: "global".to_owned(),
            address: "127.0.0.1".to_owned(),
            identity: b"abc".to_vec(),
            mechanism: "PLAIN".to_owned(),
            credentials: vec![b"admin".to_vec(), b"secret".to_vec()],
            local_principal: None,
        }
    }

    /// Claim: the request frames are 27/ZAP's, in its order, with the empty
    /// delimiter first so that a plain REP handler can answer.
    #[test]
    fn a_request_is_the_rfcs_frames() {
        let encoded = request().encode();
        let frames: Vec<&[u8]> = encoded.frames().iter().map(Message::as_slice).collect();
        assert_eq!(
            frames,
            vec![
                &b""[..],
                b"1.0",
                b"1",
                b"global",
                b"127.0.0.1",
                b"abc",
                b"PLAIN",
                b"admin",
                b"secret",
            ]
        );
        assert_eq!(ZapRequest::decode(&encoded).expect("decode"), request());
    }

    /// Claim: the kernel's statement about a local peer reaches the handler as
    /// a named extension frame **after** the credentials, so a handler
    /// reading 27/ZAP's fields is unaffected and one that wants it can have
    /// it.
    #[test]
    fn a_local_peer_offers_its_kernel_credentials() {
        let mut asked = request();
        asked.mechanism = "NULL".to_owned();
        asked.credentials.clear();
        asked.address = String::new();
        asked.local_principal = Some(LocalPrincipal {
            uid: 1000,
            gid: 100,
            pid: Some(4242),
        });
        let encoded = asked.encode();
        let last = encoded.frames().last().expect("frames").as_slice();
        assert_eq!(last, b"X-Local-Principal: uid=1000, gid=100, pid=4242");

        let read = ZapRequest::decode(&encoded).expect("decode");
        assert_eq!(read, asked);
        assert!(
            read.credentials.is_empty(),
            "the extension must not be mistaken for a credential"
        );
    }

    /// Claim: an empty `identity` field is not mistaken for the address
    /// delimiter — the bug a delimiter scan would have, since a peer that
    /// announced no identity sends an empty frame in the middle of the
    /// request.
    #[test]
    fn an_empty_identity_is_not_a_delimiter() {
        let mut anonymous = request();
        anonymous.identity = Vec::new();
        let read = ZapRequest::decode(&anonymous.encode()).expect("decode");
        assert_eq!(read, anonymous);
        assert_eq!(read.mechanism, "PLAIN");
        assert_eq!(read.credentials.len(), 2);

        // And a request a REP socket already unwrapped — no delimiter at all
        // — reads the same way.
        let unwrapped = Multipart::new(
            anonymous
                .encode()
                .frames()
                .iter()
                .skip(1)
                .cloned()
                .collect(),
        )
        .expect("frames");
        assert_eq!(ZapRequest::decode(&unwrapped).expect("decode"), anonymous);

        // As does one behind a ROUTER's routing frames.
        let mut routed = vec![Message::from("router-id")];
        routed.extend(anonymous.encode().frames().iter().cloned());
        let routed = Multipart::new(routed).expect("frames");
        assert_eq!(ZapRequest::decode(&routed).expect("decode"), anonymous);
    }

    /// Claim: every status 27/ZAP defines round-trips, and one it does not
    /// define is refused rather than guessed at.
    #[test]
    fn the_four_statuses_are_the_rfcs() {
        for (status, code) in [
            (ZapStatus::Allowed, "200"),
            (ZapStatus::TemporaryError, "300"),
            (ZapStatus::AuthenticationFailure, "400"),
            (ZapStatus::InternalError, "500"),
        ] {
            assert_eq!(status.code(), code);
            assert_eq!(ZapStatus::parse(code.as_bytes()), Some(status));
        }
        assert_eq!(ZapStatus::parse(b"201"), None);
        assert!(ZapStatus::Allowed.is_allowed());
        assert!(!ZapStatus::AuthenticationFailure.is_allowed());
    }

    /// Claim: a reply round-trips without an envelope of its own — a REP
    /// handler's socket puts the request's envelope back — and a refusal
    /// carrying a user id is refused: "for other statuses, it SHALL be
    /// empty".
    #[test]
    fn a_reply_round_trips_and_a_refusal_carries_no_user() {
        let allowed = ZapReply::allowed(b"7".to_vec(), "alice");
        assert_eq!(
            allowed.encode().frames()[0].as_slice(),
            ZAP_VERSION,
            "a reply starts at the version, not at a delimiter"
        );
        let read = ZapReply::decode(&allowed.encode()).expect("decode");
        assert_eq!(read, allowed);
        assert_eq!(
            read.user_id.expect("user id").as_str(),
            "alice",
            "a 200 may name the user"
        );

        let refused = ZapReply::refused(b"7".to_vec(), ZapStatus::AuthenticationFailure, "no");
        assert_eq!(
            ZapReply::decode(&refused.encode()).expect("decode"),
            refused
        );

        let mut confused = refused.clone();
        confused.user_id = Some(ZapUserId::new("alice"));
        let err = ZapReply::decode(&confused.encode()).unwrap_err();
        assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
    }

    /// Claim: a version other than 1.0 is refused in both directions, since
    /// 27/ZAP defines exactly one.
    #[test]
    fn only_version_one_zero_is_spoken() {
        let mut frames = ZapReply::allowed(b"1".to_vec(), "alice").encode();
        frames = Multipart::new(
            frames
                .frames()
                .iter()
                .enumerate()
                .map(|(index, frame)| {
                    if index == 0 {
                        Message::from("2.0")
                    } else {
                        frame.clone()
                    }
                })
                .collect(),
        )
        .expect("frames");
        let err = ZapReply::decode(&frames).unwrap_err();
        assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
    }

    /// Claim: the user id has no bridge to a weida identity — the type is a
    /// string a handler chose, and the only thing it offers is that string.
    #[test]
    fn the_user_id_is_not_an_identity() {
        let id = ZapUserId::new("alice");
        assert_eq!(id.as_str(), "alice");
        assert_eq!(id.to_string(), "alice");
        // A slot holds it per connection, which is where a per-connection
        // fact belongs.
        let slot = AuthenticatedUser::default();
        assert_eq!(slot.get(), None);
        slot.set(Some(id.clone()));
        assert_eq!(slot.get(), Some(id));
    }
}
