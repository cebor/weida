//! CURVE: the keys, the handshake and the message boxes
//! ([26/CURVEZMQ](https://rfc.zeromq.org/spec/26/),
//! [25/ZMTP-CURVE](https://rfc.zeromq.org/spec/25/)).
//!
//! The octets are [`weida_zmtp::curve`]'s and are not repeated here; this
//! module is the cryptography, the sequencing and the policy. What it adds to
//! the layouts is the four keys, the cookie and its lifetime, the nonce
//! counters, the ZAP credential and the transport that boxes every message
//! after the handshake.
//!
//! # The four keys
//!
//! Per connection, 26/CURVEZMQ names four: the client's long-term pair
//! `C`/`c` and the server's `S`/`s`, plus a **transient** pair on each side,
//! `C'`/`c'` and `S'`/`s'`, generated for this connection and thrown away
//! with it. The long-term keys authenticate; the transient keys encrypt.
//! That separation is what "perfect forward secrecy" means here: "Session
//! keys are held in memory and destroyed when the connection is closed", so
//! a long-term key stolen tomorrow does not open a conversation recorded
//! today ([`CurveTransport::destroy`]).
//!
//! `C` never travels in clear text — it is inside the `INITIATE` box, which
//! is the client-identity protection CURVE claims — and it is what reaches a
//! ZAP handler as the CURVE credential, "a 32-byte long-term public key of
//! the peer being authenticated" (27/ZAP).
//!
//! # The cookie
//!
//! A server answers a `HELLO` with a `WELCOME` and then **keeps nothing**:
//! "It's generated a keypair, sent that back to the client in a way only the
//! client can read, and thrown it away." This implementation does exactly
//! that. After [`CurveServer::welcome`] the only state left is the cookie
//! key; `C'` and `s'` come back out of the cookie the client echoes in its
//! `INITIATE`, and the cookie key is dropped the moment that `INITIATE`
//! opens — or when the interval runs out, which 26/CURVEZMQ puts at "a short
//! interval, for example 60 seconds" and this module at
//! [`COOKIE_LIFETIME`]. An expired cookie is refused rather than tolerated:
//! the cookie *is* the anti-DoS design, and a cookie key that never expired
//! would be a long-term key nobody chose.
//!
//! # The three security models
//!
//! 26/CURVEZMQ names three, and all three are reachable from this crate's
//! options without any new knob:
//!
//! 1. **"Where the server does not check client keys at all."**
//!    `ZMQ_CURVE_SERVER` with an empty `ZMQ_ZAP_DOMAIN`. Knowing `S` is the
//!    whole of the authorization, which is what a public service with
//!    confidentiality but no authentication wants.
//! 2. **"Where all clients share the same public key, that the server
//!    checks."** A `ZMQ_ZAP_DOMAIN` and a ZAP handler whose table has one
//!    entry. The handler answers 200 or 400 on the key it is given.
//! 3. **"Where each client has its own key… the server can grant access to
//!    clients according to their authenticated identity."** The same domain
//!    and a handler whose table has one entry per client, whose 200 also
//!    names a user id — which this library keeps per connection as
//!    [`crate::ZapUserId`], readable on the peer.
//!
//! Models 2 and 3 are one option surface and one wire: the difference is the
//! handler's table and whether its 200 names a user, which is where the
//! difference belongs, since "the significance of domains are an application
//! issue and not relevant to ZAP". [`SecurityModel`] is the part this library
//! can see, and it says which of the two groups a configuration selects.
//!
//! # Nonces
//!
//! Every box takes a 24-octet nonce, and 26/CURVEZMQ fixes the first 16 (or
//! 8) octets per box kind so that two boxes under the same key cannot collide
//! even at the same counter value — the prefixes are
//! [`weida_zmtp::curve`]'s. What travels is a **counter** for the short form
//! and 16 **random** octets for the long form. The counter here starts at 1
//! and is shared between one side's handshake boxes and its message boxes, as
//! libzmq's is, so a nonce never repeats within a connection; 2^64-1 is the
//! specification's own ceiling and reaching it is an error rather than a
//! wrap. On the way in, a `MESSAGE` whose counter does not exceed the highest
//! seen is refused: replay is one of the attacks 26/CURVEZMQ claims to
//! defend against, and the counter is the only thing that can defend it.
//!
//! # The one cryptographic dependency
//!
//! [`crypto_box`], from RustCrypto. 26/CURVEZMQ is specified in terms of
//! NaCl's `crypto_box`: X25519 key agreement, HSalsa20 to derive the shared
//! key, XSalsa20 for the stream and Poly1305 for the tag, boxes 16 octets
//! larger than their plaintext. `crypto_box::SalsaBox` **is** that
//! construction — not a lookalike — so a box this crate seals opens in
//! libsodium and libzmq, and it is pure Rust, so `unsafe_code = "forbid"`
//! survives it. It is the only cryptographic dependency this crate has, and
//! `weida-zmtp` still has none at all.
//!
//! One place where this implementation chooses its own construction is the
//! cookie box. 26/CURVEZMQ seals it with `crypto_secretbox` under a random
//! 32-octet cookie key; the cookie is opaque to every peer — only the server
//! that made it ever opens it — so the choice is unobservable on the wire,
//! and this module seals it with a `crypto_box` to a keypair generated for
//! the interval. The 96 octets on the wire are the same 96 octets, which is
//! the part that has to match.

use std::fmt;
use std::time::{Duration, Instant};

use crypto_box::aead::rand_core::RngCore;
use crypto_box::aead::{Aead, OsRng};
use crypto_box::{PublicKey, SalsaBox, SecretKey};
use weida_zmtp::curve as layout;
use weida_zmtp::{CurveCommand, FrameKind, InitiatePlaintext, Metadata, frame, z85};

use crate::error::{Error, Result};
use crate::options::{Security, SocketOptions};

/// A CURVE key, public or secret: 32 octets, "not configurable; they are
/// enforced by the underlying cryptography library".
pub const KEY_LEN: usize = layout::KEY_LEN;

/// A CURVE key as Z85 text: 40 characters, which is what
/// `zmq_curve_keypair` prints and `zmq_setsockopt` accepts beside the 32
/// binary octets.
pub const KEY_TEXT_LEN: usize = z85::KEY_TEXT_LEN;

/// How long a server holds the cookie key for one connection.
///
/// 26/CURVEZMQ: the server "MUST discard \[the cookie key\] after a short
/// interval, for example 60 seconds, or as soon as the client sends a valid
/// INITIATE". 60 seconds is the specification's own example and is also
/// twice `ZMQ_HANDSHAKE_IVL`'s 30 s default, so a handshake that is inside
/// its own budget is never refused by this one.
pub const COOKIE_LIFETIME: Duration = Duration::from_secs(60);

/// The `HELLO` signature box's plaintext: "Box \[64 * %x0\](C'->S)".
const SIGNATURE_PLAINTEXT: [u8; 64] = [0u8; 64];

/// Which of 26/CURVEZMQ's security models a configuration selects, as far as
/// this library can see it.
///
/// See the module documentation: the RFC names three, and the second and
/// third are the same options and the same wire, told apart by the handler's
/// table and by whether its 200 names a user id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecurityModel {
    /// Model 1: "the server does not check client keys at all". A CURVE
    /// server with no `ZMQ_ZAP_DOMAIN`; every client that knows `S` is
    /// admitted, and its key is still confidential and still authenticated
    /// as *some* key.
    AnyClient,
    /// Models 2 and 3: the client's long-term key goes to a ZAP handler,
    /// which either holds one shared key or one key per client and may name
    /// a user id for the connection.
    CheckedByHandler,
}

/// What security model a socket's options select, or `None` where the socket
/// is not a CURVE server and therefore checks nobody.
pub fn security_model(options: &SocketOptions) -> Option<SecurityModel> {
    match options.security() {
        Security::CurveServer if options.zap_domain.is_empty() => Some(SecurityModel::AnyClient),
        Security::CurveServer => Some(SecurityModel::CheckedByHandler),
        _ => None,
    }
}

/// A CURVE public key: a long-term `C`/`S` or a transient `C'`/`S'`.
///
/// Printable, because a public key is meant to be published — `Debug` and
/// `Display` both write the 40-character Z85 form, which is the form
/// `zmq_curve_keypair` prints and configuration files carry.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CurvePublicKey([u8; KEY_LEN]);

impl CurvePublicKey {
    /// A key from its 32 octets.
    pub const fn from_bytes(bytes: [u8; KEY_LEN]) -> CurvePublicKey {
        CurvePublicKey(bytes)
    }

    /// A key as `zmq_setsockopt` takes one: "32 binary bytes or 40-character
    /// Z85".
    ///
    /// # Errors
    ///
    /// `EINVAL` for any other length, or for 40 characters that are not Z85,
    /// naming both accepted forms.
    pub fn parse(option: &[u8]) -> Result<CurvePublicKey> {
        parse_key(option).map(CurvePublicKey)
    }

    /// The 32 octets.
    pub const fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }

    /// The 40-character Z85 form.
    pub fn to_z85(&self) -> String {
        z85::encode_key(&self.0)
    }
}

impl fmt::Debug for CurvePublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CurvePublicKey({})", self.to_z85())
    }
}

impl fmt::Display for CurvePublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_z85())
    }
}

/// A CURVE secret key: a long-term `c`/`s`.
///
/// Deliberately not printable and deliberately not `Copy`: `Debug` writes a
/// placeholder, because a secret key in a log is a secret key on a disk, and
/// comparison is constant-time, because `SocketOptions` derives `PartialEq`
/// and an early-exiting comparison there would be a timing oracle nobody
/// asked for. `Drop` overwrites the octets — best effort, since only a
/// volatile write is guaranteed to survive the optimizer, and the transient
/// keys that matter for forward secrecy are [`crypto_box::SecretKey`]s,
/// which zeroize on drop.
#[derive(Clone)]
pub struct CurveSecretKey([u8; KEY_LEN]);

impl CurveSecretKey {
    /// A key from its 32 octets.
    pub const fn from_bytes(bytes: [u8; KEY_LEN]) -> CurveSecretKey {
        CurveSecretKey(bytes)
    }

    /// A key as `zmq_setsockopt` takes one: 32 binary octets or 40
    /// characters of Z85.
    ///
    /// # Errors
    ///
    /// `EINVAL`, as [`CurvePublicKey::parse`].
    pub fn parse(option: &[u8]) -> Result<CurveSecretKey> {
        parse_key(option).map(CurveSecretKey)
    }

    /// The public key this secret key belongs to.
    ///
    /// Which is why `ZMQ_CURVE_PUBLICKEY` is optional on a server: libzmq's
    /// manual says a server "does not need to know its own public key", and
    /// that is because X25519 derives it.
    pub fn public_key(&self) -> CurvePublicKey {
        CurvePublicKey(*SecretKey::from(self.0).public_key().as_bytes())
    }

    /// The 40-character Z85 form. Named so that writing a secret key down is
    /// a deliberate call and never a `Display`.
    pub fn to_z85(&self) -> String {
        z85::encode_key(&self.0)
    }

    fn secret(&self) -> SecretKey {
        SecretKey::from(self.0)
    }
}

impl fmt::Debug for CurveSecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CurveSecretKey(<secret>)")
    }
}

impl PartialEq for CurveSecretKey {
    fn eq(&self, other: &CurveSecretKey) -> bool {
        let mut difference = 0u8;
        for (ours, theirs) in self.0.iter().zip(other.0.iter()) {
            difference |= ours ^ theirs;
        }
        difference == 0
    }
}

impl Eq for CurveSecretKey {}

impl Drop for CurveSecretKey {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// A fresh CURVE key pair, as `zmq_curve_keypair` returns one.
pub fn keypair() -> (CurvePublicKey, CurveSecretKey) {
    let secret = SecretKey::generate(&mut OsRng);
    (
        CurvePublicKey(*secret.public_key().as_bytes()),
        CurveSecretKey(secret.to_bytes()),
    )
}

/// 32 binary octets or 40 characters of Z85, and nothing else.
fn parse_key(option: &[u8]) -> Result<[u8; KEY_LEN]> {
    match option.len() {
        KEY_LEN => Ok(option.try_into().expect("32 octets")),
        KEY_TEXT_LEN => {
            let text = std::str::from_utf8(option).map_err(|_| {
                Error::EINVAL(
                    "a 40-octet CURVE key is Z85 text, and this one is not UTF-8 at all".into(),
                )
            })?;
            z85::decode_key(text).map_err(|e| {
                Error::EINVAL(format!("a 40-character CURVE key must be Z85: {e}").into())
            })
        }
        other => Err(Error::EINVAL(
            format!(
                "a CURVE key is {KEY_LEN} binary octets or {KEY_TEXT_LEN} characters of Z85; \
                 this one is {other} octets"
            )
            .into(),
        )),
    }
}

/// The short-nonce counter of one direction.
///
/// Starts at 1, never repeats, and reaching 2^64-1 is the specification's own
/// ceiling: "SHALL NOT send more than 2^64-1 commands in one connection".
#[derive(Debug)]
struct Counter(u64);

impl Counter {
    const fn new() -> Counter {
        Counter(1)
    }

    fn take(&mut self) -> Result<u64> {
        let value = self.0;
        self.0 = self.0.checked_add(1).ok_or_else(|| {
            Error::ENOCOMPATPROTO(
                "this connection has sent 2^64-1 CURVE commands, which is the ceiling \
                 26/CURVEZMQ sets; a further box would repeat a nonce"
                    .into(),
            )
        })?;
        Ok(value)
    }
}

/// The cookie key a server holds between its `WELCOME` and one `INITIATE`.
///
/// A keypair rather than a symmetric key, for the reason the module
/// documentation gives: the cookie is opaque to every peer, so the
/// construction is the server's own and the octet count is what matters.
struct CookieKey {
    public: PublicKey,
    secret: SecretKey,
    /// When this key stops being accepted. The other half of "or as soon as
    /// the client sends a valid INITIATE".
    expires: Instant,
}

impl CookieKey {
    fn new(lifetime: Duration) -> CookieKey {
        let secret = SecretKey::generate(&mut OsRng);
        CookieKey {
            public: secret.public_key(),
            secret,
            expires: Instant::now() + lifetime,
        }
    }

    fn boxed(&self) -> SalsaBox {
        SalsaBox::new(&self.public, &self.secret)
    }
}

/// The client half of a CURVE handshake.
///
/// One per connection, because the transient pair is one per connection.
pub struct CurveClient {
    /// `C` and `c`: the identity a ZAP handler will be told about.
    long_public: CurvePublicKey,
    long_secret: SecretKey,
    /// `S`: the server this client will talk to, and nobody else.
    server_key: PublicKey,
    /// `C'` and `c'`.
    transient_public: PublicKey,
    transient_secret: SecretKey,
    /// `S'`, learned from the `WELCOME` box.
    server_transient: Option<PublicKey>,
    /// The cookie, held only between `WELCOME` and `INITIATE`.
    cookie: Option<[u8; layout::COOKIE_LEN]>,
    counter: Counter,
}

impl CurveClient {
    /// A client with its long-term pair and the server's public key.
    pub fn new(
        long_public: CurvePublicKey,
        long_secret: &CurveSecretKey,
        server_key: CurvePublicKey,
    ) -> CurveClient {
        let transient_secret = SecretKey::generate(&mut OsRng);
        CurveClient {
            long_public,
            long_secret: long_secret.secret(),
            server_key: PublicKey::from(*server_key.as_bytes()),
            transient_public: transient_secret.public_key(),
            transient_secret,
            server_transient: None,
            cookie: None,
            counter: Counter::new(),
        }
    }

    /// The `HELLO` frame: `C'`, a counter nonce and 64 zero octets sealed to
    /// the server's long-term key, which is the proof that this client knows
    /// `S`.
    ///
    /// # Errors
    ///
    /// `ENOCOMPATPROTO` if the nonce counter is exhausted; `EIO` if the box
    /// cannot be sealed, which needs a broken cryptography library.
    pub fn hello(&mut self) -> Result<Vec<u8>> {
        let nonce = self.counter.take()?;
        let sealed = seal(
            &SalsaBox::new(&self.server_key, &self.transient_secret),
            &layout::short_nonce(layout::PREFIX_HELLO, nonce),
            &SIGNATURE_PLAINTEXT,
        )?;
        let signature_box = sized::<{ layout::SIGNATURE_BOX_LEN }>(&sealed, "HELLO signature")?;
        CurveCommand::Hello {
            client_key: self.transient_public.as_bytes(),
            nonce,
            signature_box: &signature_box,
        }
        .encode()
        .map_err(curve_error)
    }

    /// Reads a `WELCOME`: opens the box with `c'` and `S`, keeping `S'` and
    /// the cookie.
    ///
    /// # Errors
    ///
    /// `EACCES` if the box does not open — which means the peer does not hold
    /// `s`, so it is not the server this client was configured for.
    /// `ENOCOMPATPROTO` for a malformed command.
    pub fn read_welcome(&mut self, body: &[u8]) -> Result<()> {
        let CurveCommand::Welcome { nonce, welcome_box } =
            CurveCommand::decode(body).map_err(curve_error)?
        else {
            return Err(unexpected("WELCOME", body));
        };
        let plaintext = open(
            &SalsaBox::new(&self.server_key, &self.transient_secret),
            &layout::long_nonce(layout::PREFIX_WELCOME, nonce),
            welcome_box,
            "WELCOME",
        )?;
        let plaintext = sized::<{ layout::KEY_LEN + layout::COOKIE_LEN }>(&plaintext, "WELCOME")?;
        let (server_transient, cookie) = layout::split_welcome_plaintext(&plaintext);
        self.server_transient = Some(PublicKey::from(*server_transient));
        self.cookie = Some(*cookie);
        Ok(())
    }

    /// The `INITIATE` frame: the cookie echoed, and `C`, the vouch and this
    /// socket's metadata sealed to `S'`.
    ///
    /// # Errors
    ///
    /// `EFSM` if no `WELCOME` has been read; otherwise as [`Self::hello`].
    pub fn initiate(&mut self, metadata: &Metadata<'_>) -> Result<Vec<u8>> {
        let (Some(server_transient), Some(cookie)) = (&self.server_transient, self.cookie) else {
            return Err(Error::EFSM(
                "an INITIATE needs the S' and the cookie that a WELCOME carries".into(),
            ));
        };
        // `vouch = vouch-nonce vouch-box`, `Box [C',S](C->S')`: the client's
        // long-term key signs the pair, so a vouch collected by one server
        // cannot be replayed at another.
        let mut vouch_nonce = [0u8; layout::LONG_NONCE_LEN];
        OsRng.fill_bytes(&mut vouch_nonce);
        let vouch_box = seal(
            &SalsaBox::new(server_transient, &self.long_secret),
            &layout::long_nonce(layout::PREFIX_VOUCH, &vouch_nonce),
            &layout::join_key_pair(self.transient_public.as_bytes(), self.server_key.as_bytes()),
        )?;
        let vouch = layout::join_nonce_box(
            &vouch_nonce,
            &sized::<{ layout::NONCE_BOX_LEN }>(&vouch_box, "vouch")?,
        );

        let mut plaintext = Vec::new();
        InitiatePlaintext {
            client_key: self.long_public.as_bytes(),
            vouch: &vouch,
            metadata: metadata.clone(),
        }
        .encode(&mut plaintext)
        .map_err(curve_error)?;

        let nonce = self.counter.take()?;
        let initiate_box = seal(
            &SalsaBox::new(server_transient, &self.transient_secret),
            &layout::short_nonce(layout::PREFIX_INITIATE, nonce),
            &plaintext,
        )?;
        // The cookie is the server's memory, not ours: it goes back and this
        // side forgets it.
        self.cookie = None;
        CurveCommand::Initiate {
            cookie: &cookie,
            nonce,
            initiate_box: &initiate_box,
        }
        .encode()
        .map_err(curve_error)
    }

    /// Reads a `READY` and returns the server's metadata dictionary.
    ///
    /// # Errors
    ///
    /// `EACCES` if the box does not open, `ENOCOMPATPROTO` for a malformed
    /// command or a `READY` before the `WELCOME`.
    pub fn read_ready(&mut self, body: &[u8]) -> Result<Vec<u8>> {
        let Some(server_transient) = &self.server_transient else {
            return Err(unexpected("WELCOME", body));
        };
        let CurveCommand::Ready { nonce, ready_box } =
            CurveCommand::decode(body).map_err(curve_error)?
        else {
            return Err(unexpected("READY", body));
        };
        open(
            &SalsaBox::new(server_transient, &self.transient_secret),
            &layout::short_nonce(layout::PREFIX_READY, nonce),
            ready_box,
            "READY",
        )
    }

    /// The message transport this handshake agreed: `C'` and `S'`, with the
    /// client's own nonce prefix and the counter it has reached.
    ///
    /// # Errors
    ///
    /// `EFSM` if the handshake did not get as far as a `WELCOME`.
    pub fn into_transport(self) -> Result<CurveTransport> {
        let Some(server_transient) = &self.server_transient else {
            return Err(Error::EFSM(
                "a CURVE transport needs the S' a WELCOME carries".into(),
            ));
        };
        Ok(CurveTransport {
            boxed: SalsaBox::new(server_transient, &self.transient_secret),
            send_prefix: layout::PREFIX_MESSAGE_CLIENT,
            recv_prefix: layout::PREFIX_MESSAGE_SERVER,
            send: Counter(self.counter.0),
            highest_seen: 0,
            alive: true,
        })
    }
}

/// What a server learned from one `INITIATE`.
pub struct OpenedInitiate {
    /// The client's long-term public key `C` — the ZAP credential, and the
    /// only thing an authorization decision can be made on.
    pub client_key: CurvePublicKey,
    /// The opened box, whose metadata section is [`Self::metadata`].
    plaintext: Vec<u8>,
}

impl OpenedInitiate {
    /// The client's connection metadata, where NULL puts it in `READY`.
    ///
    /// # Errors
    ///
    /// `ENOCOMPATPROTO` for a malformed property dictionary.
    pub fn metadata(&self) -> Result<Metadata<'_>> {
        Ok(InitiatePlaintext::decode(&self.plaintext)
            .map_err(curve_error)?
            .metadata)
    }

    /// The metadata section of the opened box, as the property dictionary
    /// octets. The length was checked when the box was opened, so the
    /// section is present even when it is empty.
    pub fn metadata_bytes(&self) -> &[u8] {
        &self.plaintext[layout::KEY_LEN + layout::VOUCH_LEN..]
    }
}

impl fmt::Debug for OpenedInitiate {
    /// The client key and nothing else: the opened box also holds the vouch,
    /// and a vouch in a log is a box somebody kept.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenedInitiate")
            .field("client_key", &self.client_key)
            .finish_non_exhaustive()
    }
}

/// The server half of a CURVE handshake.
pub struct CurveServer {
    /// `s`, and `S` derived from it: "a server does not need to know its own
    /// public key".
    long_secret: SecretKey,
    long_public: PublicKey,
    /// How long a cookie key lives once it is made. A parameter rather than
    /// a constant so that the expiry can be tested without waiting a minute.
    cookie_lifetime: Duration,
    /// `C'`, held only between the `HELLO` and the `WELCOME`.
    client_transient: Option<PublicKey>,
    /// The cookie key, held only between the `WELCOME` and a valid
    /// `INITIATE`.
    cookie: Option<CookieKey>,
    /// `C'` and `s'`, recovered from the cookie by a valid `INITIATE`.
    session: Option<(PublicKey, SecretKey)>,
    counter: Counter,
}

impl CurveServer {
    /// A server holding its long-term secret key.
    pub fn new(long_secret: &CurveSecretKey, cookie_lifetime: Duration) -> CurveServer {
        let long_secret = long_secret.secret();
        CurveServer {
            long_public: long_secret.public_key(),
            long_secret,
            cookie_lifetime,
            client_transient: None,
            cookie: None,
            session: None,
            counter: Counter::new(),
        }
    }

    /// Reads a `HELLO`: opens the signature box with `s`, which proves the
    /// client knows `S`, and keeps `C'`.
    ///
    /// # Errors
    ///
    /// `EACCES` if the signature box does not open or does not hold the 64
    /// zero octets: the peer does not know this server's public key.
    /// `ENOCOMPATPROTO` for a malformed or mis-sized command — including the
    /// 198-octet `HELLO` that 26/CURVEZMQ's prose describes and its own
    /// grammar contradicts.
    pub fn read_hello(&mut self, body: &[u8]) -> Result<()> {
        let CurveCommand::Hello {
            client_key,
            nonce,
            signature_box,
        } = CurveCommand::decode(body).map_err(curve_error)?
        else {
            return Err(unexpected("HELLO", body));
        };
        let client_transient = PublicKey::from(*client_key);
        let signature = open(
            &SalsaBox::new(&client_transient, &self.long_secret),
            &layout::short_nonce(layout::PREFIX_HELLO, nonce),
            signature_box,
            "HELLO",
        )?;
        if signature != SIGNATURE_PLAINTEXT {
            return Err(Error::EACCES(
                "the HELLO signature box does not hold the 64 zero octets 26/CURVEZMQ \
                 specifies"
                    .into(),
            ));
        }
        self.client_transient = Some(client_transient);
        Ok(())
    }

    /// The `WELCOME` frame, and the moment this server forgets the
    /// connection: `S'` and the cookie go to the client, and all that is left
    /// here is the cookie key.
    ///
    /// # Errors
    ///
    /// `EFSM` if no `HELLO` has been read.
    pub fn welcome(&mut self) -> Result<Vec<u8>> {
        let Some(client_transient) = self.client_transient.take() else {
            return Err(Error::EFSM(
                "a WELCOME needs the C' that a HELLO carries".into(),
            ));
        };
        let transient_secret = SecretKey::generate(&mut OsRng);
        let transient_public = transient_secret.public_key();

        // `cookie = cookie-nonce cookie-box`, holding C' and s' so that the
        // server can keep neither.
        let cookie_key = CookieKey::new(self.cookie_lifetime);
        let mut cookie_nonce = [0u8; layout::LONG_NONCE_LEN];
        OsRng.fill_bytes(&mut cookie_nonce);
        let cookie_box = seal(
            &cookie_key.boxed(),
            &layout::long_nonce(layout::PREFIX_COOKIE, &cookie_nonce),
            &layout::join_key_pair(client_transient.as_bytes(), &transient_secret.to_bytes()),
        )?;
        let cookie = layout::join_nonce_box(
            &cookie_nonce,
            &sized::<{ layout::NONCE_BOX_LEN }>(&cookie_box, "cookie")?,
        );
        self.cookie = Some(cookie_key);

        let mut welcome_nonce = [0u8; layout::LONG_NONCE_LEN];
        OsRng.fill_bytes(&mut welcome_nonce);
        let welcome_box = seal(
            &SalsaBox::new(&client_transient, &self.long_secret),
            &layout::long_nonce(layout::PREFIX_WELCOME, &welcome_nonce),
            &layout::join_welcome_plaintext(transient_public.as_bytes(), &cookie),
        )?;
        let welcome_box = sized::<{ layout::WELCOME_BOX_LEN }>(&welcome_box, "WELCOME")?;
        CurveCommand::Welcome {
            nonce: &welcome_nonce,
            welcome_box: &welcome_box,
        }
        .encode()
        .map_err(curve_error)
    }

    /// Reads an `INITIATE`: opens the cookie to get `C'` and `s'` back, opens
    /// the box, checks the vouch, and **discards the cookie key**.
    ///
    /// # Errors
    ///
    /// `EACCES` for a cookie that has expired, a box that does not open, or a
    /// vouch that does not name this connection's `C'` and this server's `S`
    /// — the last one being the replay a vouch exists to prevent.
    /// `ENOCOMPATPROTO` for a malformed command.
    pub fn read_initiate(&mut self, body: &[u8]) -> Result<OpenedInitiate> {
        let CurveCommand::Initiate {
            cookie,
            nonce,
            initiate_box,
        } = CurveCommand::decode(body).map_err(curve_error)?
        else {
            return Err(unexpected("INITIATE", body));
        };
        let cookie_key = self.cookie.as_ref().ok_or_else(|| {
            Error::EACCES(
                "this INITIATE has no cookie key to open it: either no WELCOME was sent or \
                 the cookie was already spent"
                    .into(),
            )
        })?;
        if Instant::now() > cookie_key.expires {
            // Dropped here as well as on success: an expired key must not
            // survive the connection that could not use it in time.
            self.cookie = None;
            return Err(Error::EACCES(
                format!(
                    "the cookie key expired: 26/CURVEZMQ requires it to be discarded after a \
                     short interval, and this one lived {COOKIE_LIFETIME:?}"
                )
                .into(),
            ));
        }
        let (cookie_nonce, cookie_box) = layout::split_nonce_box(cookie);
        let opened = open(
            &cookie_key.boxed(),
            &layout::long_nonce(layout::PREFIX_COOKIE, cookie_nonce),
            cookie_box,
            "cookie",
        )?;
        let opened = sized::<{ 2 * layout::KEY_LEN }>(&opened, "cookie")?;
        let (client_transient, transient_secret) = layout::split_key_pair(&opened);
        let client_transient = PublicKey::from(*client_transient);
        let transient_secret = SecretKey::from(*transient_secret);

        let plaintext = open(
            &SalsaBox::new(&client_transient, &transient_secret),
            &layout::short_nonce(layout::PREFIX_INITIATE, nonce),
            initiate_box,
            "INITIATE",
        )?;
        let initiate = InitiatePlaintext::decode(&plaintext).map_err(curve_error)?;
        let client_key = PublicKey::from(*initiate.client_key);

        // `Box [C',S](C->S')`: opened with `s'` and `C`, and its plaintext
        // must name this connection's C' and this server's S.
        let (vouch_nonce, vouch_box) = layout::split_nonce_box(initiate.vouch);
        let vouch = open(
            &SalsaBox::new(&client_key, &transient_secret),
            &layout::long_nonce(layout::PREFIX_VOUCH, vouch_nonce),
            vouch_box,
            "vouch",
        )?;
        let vouch = sized::<{ 2 * layout::KEY_LEN }>(&vouch, "vouch")?;
        let (vouched_transient, vouched_server) = layout::split_key_pair(&vouch);
        if vouched_transient != client_transient.as_bytes()
            || vouched_server != self.long_public.as_bytes()
        {
            return Err(Error::EACCES(
                "the vouch box names another connection's transient key or another server, \
                 which is the replay the vouch exists to refuse"
                    .into(),
            ));
        }

        // "or as soon as the client sends a valid INITIATE": the cookie key
        // has done its work and must not open a second one.
        self.cookie = None;
        self.session = Some((client_transient, transient_secret));
        Ok(OpenedInitiate {
            client_key: CurvePublicKey(*client_key.as_bytes()),
            plaintext,
        })
    }

    /// The `READY` frame: this socket's metadata, sealed to `C'`.
    ///
    /// # Errors
    ///
    /// `EFSM` if no valid `INITIATE` has been read.
    pub fn ready(&mut self, metadata: &Metadata<'_>) -> Result<Vec<u8>> {
        let Some((client_transient, transient_secret)) = &self.session else {
            return Err(Error::EFSM(
                "a READY needs the session keys a valid INITIATE recovers".into(),
            ));
        };
        let mut plaintext = Vec::new();
        metadata.encode(&mut plaintext).map_err(metadata_error)?;
        let nonce = self.counter.take()?;
        let ready_box = seal(
            &SalsaBox::new(client_transient, transient_secret),
            &layout::short_nonce(layout::PREFIX_READY, nonce),
            &plaintext,
        )?;
        CurveCommand::Ready {
            nonce,
            ready_box: &ready_box,
        }
        .encode()
        .map_err(curve_error)
    }

    /// The message transport this handshake agreed.
    ///
    /// # Errors
    ///
    /// `EFSM` if no valid `INITIATE` has been read.
    pub fn into_transport(self) -> Result<CurveTransport> {
        let Some((client_transient, transient_secret)) = &self.session else {
            return Err(Error::EFSM(
                "a CURVE transport needs the session keys a valid INITIATE recovers".into(),
            ));
        };
        Ok(CurveTransport {
            boxed: SalsaBox::new(client_transient, transient_secret),
            send_prefix: layout::PREFIX_MESSAGE_SERVER,
            recv_prefix: layout::PREFIX_MESSAGE_CLIENT,
            send: Counter(self.counter.0),
            highest_seen: 0,
            alive: true,
        })
    }

    /// Whether the cookie key is still held. The observable half of "the
    /// server keeps no state for an unauthenticated client".
    pub const fn holds_cookie(&self) -> bool {
        self.cookie.is_some()
    }
}

/// The session keys of one established CURVE connection, and the nonce
/// counters that go with them.
///
/// Every frame after the handshake travels as a `MESSAGE` command whose box
/// holds a flags octet and the frame body — application messages and the
/// commands between them alike, because a CURVE connection has no plaintext
/// after its `READY`.
pub struct CurveTransport {
    boxed: SalsaBox,
    send_prefix: &'static [u8; layout::SHORT_NONCE_PREFIX_LEN],
    recv_prefix: &'static [u8; layout::SHORT_NONCE_PREFIX_LEN],
    send: Counter,
    /// The highest counter seen from the peer. A `MESSAGE` that does not
    /// exceed it is a replay.
    highest_seen: u64,
    alive: bool,
}

impl fmt::Debug for CurveTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CurveTransport")
            .field("alive", &self.alive)
            .field("sent", &self.send.0)
            .field("highest_seen", &self.highest_seen)
            .finish_non_exhaustive()
    }
}

impl CurveTransport {
    /// Seals one frame: the flags octet and the body, as one `MESSAGE`
    /// ready for the wire.
    ///
    /// **In a message frame, not a command frame.** 26/CURVEZMQ calls
    /// `MESSAGE` a command and the body is a command body — the name, the
    /// nonce and the box — but libzmq 4.3.5 puts it behind a **message**
    /// frame header, flags without the COMMAND bit, and refuses the
    /// command-framed form: measured in `tests/interop_libzmq.rs`, where a
    /// CURVE handshake with libzmq succeeds and the first command-framed
    /// `MESSAGE` closes the connection. The outer MORE flag stays zero
    /// because the real one is the flags octet inside the box. Reading
    /// accepts either kind ([`Self::open_message`] takes a body).
    ///
    /// # Errors
    ///
    /// `ENOTSOCK` after [`Self::destroy`], `ENOCOMPATPROTO` at the counter
    /// ceiling.
    pub fn seal_frame(&mut self, flags: u8, body: &[u8]) -> Result<Vec<u8>> {
        self.check_alive()?;
        let mut plaintext = Vec::with_capacity(1 + body.len());
        plaintext.push(flags);
        plaintext.extend_from_slice(body);
        let nonce = self.send.take()?;
        let message_box = seal(
            &self.boxed,
            &layout::short_nonce(self.send_prefix, nonce),
            &plaintext,
        )?;
        let mut framed = Vec::new();
        CurveCommand::Message {
            nonce,
            message_box: &message_box,
        }
        .encode_body(&mut framed)
        .map_err(curve_error)?;
        Ok(frame::encode(FrameKind::Message { more: false }, &framed))
    }

    /// Opens one `MESSAGE` command body, returning the flags octet and the
    /// frame body it held.
    ///
    /// # Errors
    ///
    /// `ENOCOMPATPROTO` for a command that is not a `MESSAGE` or whose
    /// counter does not exceed the highest seen — a repeated nonce is a
    /// replay, and 26/CURVEZMQ names replay among the attacks it defends
    /// against. `EACCES` if the box does not open.
    pub fn open_message(&mut self, body: &[u8]) -> Result<(u8, Vec<u8>)> {
        self.check_alive()?;
        let CurveCommand::Message { nonce, message_box } =
            CurveCommand::decode(body).map_err(curve_error)?
        else {
            return Err(Error::ENOCOMPATPROTO(
                "a CURVE connection carries MESSAGE commands after its READY, and this is \
                 something else"
                    .into(),
            ));
        };
        if nonce <= self.highest_seen {
            return Err(Error::ENOCOMPATPROTO(
                format!(
                    "the peer's MESSAGE nonce {nonce} does not exceed the {} already seen, \
                     so it is a replay or a restarted counter",
                    self.highest_seen
                )
                .into(),
            ));
        }
        let plaintext = open(
            &self.boxed,
            &layout::short_nonce(self.recv_prefix, nonce),
            message_box,
            "MESSAGE",
        )?;
        self.highest_seen = nonce;
        let (&flags, body) = plaintext
            .split_first()
            .ok_or_else(|| Error::ENOCOMPATPROTO("a MESSAGE box has no flags octet".into()))?;
        Ok((flags, body.to_vec()))
    }

    /// Destroys the session keys: "Session keys are held in memory and
    /// destroyed when the connection is closed."
    ///
    /// Called when the connection ends. Afterwards nothing can be sealed or
    /// opened on this transport, which is what makes the destruction
    /// observable rather than a claim about memory.
    pub fn destroy(&mut self) {
        self.alive = false;
        self.send = Counter(0);
        self.highest_seen = 0;
    }

    /// How many boxes this side has sealed, counting the handshake's.
    pub const fn sent(&self) -> u64 {
        self.send.0
    }

    fn check_alive(&self) -> Result<()> {
        if self.alive {
            Ok(())
        } else {
            Err(Error::ENOTSOCK(
                "this connection's CURVE session keys have been destroyed".into(),
            ))
        }
    }
}

/// A box, sealed.
fn seal(boxed: &SalsaBox, nonce: &[u8; layout::NONCE_LEN], plaintext: &[u8]) -> Result<Vec<u8>> {
    boxed
        .encrypt(crypto_box::Nonce::from_slice(nonce), plaintext)
        .map_err(|_| Error::EIO("the cryptography library refused to seal a CURVE box".into()))
}

/// A box, opened — or `EACCES`, because a box that does not open is a peer
/// that does not hold the key it claimed.
fn open(
    boxed: &SalsaBox,
    nonce: &[u8; layout::NONCE_LEN],
    sealed: &[u8],
    what: &str,
) -> Result<Vec<u8>> {
    boxed
        .decrypt(crypto_box::Nonce::from_slice(nonce), sealed)
        .map_err(|_| {
            Error::EACCES(
                format!(
                    "the {what} box does not open: the peer does not hold the key it would \
                     need, or the octets were altered"
                )
                .into(),
            )
        })
}

/// A box or plaintext of exactly the length its layout states.
fn sized<const N: usize>(bytes: &[u8], what: &str) -> Result<[u8; N]> {
    bytes.try_into().map_err(|_| {
        Error::ENOCOMPATPROTO(
            format!(
                "a CURVE {what} is {N} octets and this one is {}",
                bytes.len()
            )
            .into(),
        )
    })
}

fn unexpected(want: &str, body: &[u8]) -> Error {
    let got = CurveCommand::decode(body)
        .map(|command| command.name().to_owned())
        .unwrap_or_else(|_| "something unreadable".to_owned());
    Error::ENOCOMPATPROTO(format!("expected a CURVE {want}, got {got}").into())
}

fn curve_error(error: weida_zmtp::CurveError) -> Error {
    Error::ENOCOMPATPROTO(format!("malformed CURVE command: {error}").into())
}

/// A metadata dictionary that will not encode, which is a property this
/// socket set rather than anything a peer sent.
fn metadata_error(error: weida_zmtp::CommandError) -> Error {
    Error::ENOCOMPATPROTO(format!("malformed CURVE metadata: {error}").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use weida_zmtp::SocketType;

    /// A handshake driven to completion, as the session does it.
    fn handshake() -> (CurveTransport, CurveTransport) {
        let (server_public, server_secret) = keypair();
        let (client_public, client_secret) = keypair();
        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);

        let hello = body(&client.hello().expect("HELLO"));
        server.read_hello(&hello).expect("read HELLO");
        let welcome = body(&server.welcome().expect("WELCOME"));
        client.read_welcome(&welcome).expect("read WELCOME");
        let initiate = body(
            &client
                .initiate(&Metadata::new().with_socket_type(SocketType::Req))
                .expect("INITIATE"),
        );
        let opened = server.read_initiate(&initiate).expect("read INITIATE");
        assert_eq!(opened.client_key, client_public);
        let ready = body(
            &server
                .ready(&Metadata::new().with_socket_type(SocketType::Rep))
                .expect("READY"),
        );
        client.read_ready(&ready).expect("read READY");

        (
            client.into_transport().expect("client transport"),
            server.into_transport().expect("server transport"),
        )
    }

    /// The command body of a frame this module produced.
    fn body(frame_bytes: &[u8]) -> Vec<u8> {
        let (_, body, used) = weida_zmtp::frame::decode(frame_bytes, 1 << 20).expect("a frame");
        assert_eq!(used, frame_bytes.len());
        body.to_vec()
    }

    /// Claim: a whole handshake runs, both sides derive the same session
    /// keys, and a message sealed by one opens on the other.
    #[test]
    fn the_handshake_agrees_on_session_keys() {
        let (mut client, mut server) = handshake();

        let sealed = client.seal_frame(0, b"hello").expect("seal");
        let (flags, payload) = server.open_message(&body(&sealed)).expect("open");
        assert_eq!(flags, 0);
        assert_eq!(payload, b"hello");

        let sealed = server.seal_frame(1, b"more").expect("seal");
        let (flags, payload) = client.open_message(&body(&sealed)).expect("open");
        assert_eq!(flags, 1, "the MORE flag travels inside the box");
        assert_eq!(payload, b"more");
    }

    /// Claim: a nonce never repeats within a session — not across the
    /// handshake and the messages, and not between the two directions.
    #[test]
    fn a_nonce_never_repeats_within_a_session() {
        let (server_public, server_secret) = keypair();
        let (client_public, client_secret) = keypair();
        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);

        // Every nonce this connection puts on the wire, **prefix included**,
        // because the prefix is what keeps a HELLO and a MESSAGE apart at the
        // same counter value and the two directions apart at the same one.
        let mut seen: Vec<[u8; layout::NONCE_LEN]> = Vec::new();
        let mut record = |frame: &[u8], from_client: bool| {
            let body = body(frame);
            let nonce = match CurveCommand::decode(&body).expect("a CURVE command") {
                CurveCommand::Hello { nonce, .. } => {
                    layout::short_nonce(layout::PREFIX_HELLO, nonce)
                }
                CurveCommand::Welcome { nonce, .. } => {
                    layout::long_nonce(layout::PREFIX_WELCOME, nonce)
                }
                CurveCommand::Initiate { nonce, .. } => {
                    layout::short_nonce(layout::PREFIX_INITIATE, nonce)
                }
                CurveCommand::Ready { nonce, .. } => {
                    layout::short_nonce(layout::PREFIX_READY, nonce)
                }
                CurveCommand::Message { nonce, .. } => layout::short_nonce(
                    if from_client {
                        layout::PREFIX_MESSAGE_CLIENT
                    } else {
                        layout::PREFIX_MESSAGE_SERVER
                    },
                    nonce,
                ),
            };
            seen.push(nonce);
        };

        let hello = client.hello().expect("HELLO");
        record(&hello, true);
        server.read_hello(&body(&hello)).expect("read HELLO");
        let welcome = server.welcome().expect("WELCOME");
        record(&welcome, false);
        client.read_welcome(&body(&welcome)).expect("read WELCOME");
        let initiate = client.initiate(&Metadata::new()).expect("INITIATE");
        record(&initiate, true);
        server.read_initiate(&body(&initiate)).expect("read");
        let ready = server.ready(&Metadata::new()).expect("READY");
        record(&ready, false);
        client.read_ready(&body(&ready)).expect("read READY");

        let mut client_transport = client.into_transport().expect("client transport");
        let mut server_transport = server.into_transport().expect("server transport");
        for i in 0..64u8 {
            let from_client = client_transport.seal_frame(0, &[i]).expect("seal");
            record(&from_client, true);
            server_transport
                .open_message(&body(&from_client))
                .expect("open");
            let from_server = server_transport.seal_frame(0, &[i]).expect("seal");
            record(&from_server, false);
            client_transport
                .open_message(&body(&from_server))
                .expect("open");
        }

        let total = seen.len();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), total, "a nonce repeated within the session");
        assert_eq!(total, 4 + 2 * 64);
    }

    /// Claim: the counter is what refuses a replay, in both directions.
    #[test]
    fn a_replayed_message_is_refused() {
        let (mut client, mut server) = handshake();
        let sealed = body(&client.seal_frame(0, b"once").expect("seal"));
        server.open_message(&sealed).expect("the first arrival");
        let err = server.open_message(&sealed).unwrap_err();
        assert_eq!(err.errno(), "ENOCOMPATPROTO", "{err}");
        assert!(err.cause().contains("replay"), "{err}");
    }

    /// Claim: the cookie key is discarded by a valid `INITIATE`, and a second
    /// `INITIATE` cannot be opened.
    #[test]
    fn the_cookie_is_spent_by_a_valid_initiate() {
        let (server_public, server_secret) = keypair();
        let (client_public, client_secret) = keypair();
        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);

        assert!(!server.holds_cookie(), "no cookie before a WELCOME");
        let hello = body(&client.hello().expect("HELLO"));
        server.read_hello(&hello).expect("read HELLO");
        let welcome = body(&server.welcome().expect("WELCOME"));
        assert!(server.holds_cookie(), "the WELCOME made one");
        client.read_welcome(&welcome).expect("read WELCOME");
        let initiate = body(&client.initiate(&Metadata::new()).expect("INITIATE"));
        server.read_initiate(&initiate).expect("read INITIATE");
        assert!(
            !server.holds_cookie(),
            "a valid INITIATE spends the cookie key"
        );

        let err = server.read_initiate(&initiate).unwrap_err();
        assert_eq!(err.errno(), "EACCES", "{err}");
        assert!(err.cause().contains("already spent"), "{err}");
    }

    /// Claim: a cookie the interval has run out on is refused, which is the
    /// other half of 26/CURVEZMQ's rule.
    #[test]
    fn an_expired_cookie_is_refused() {
        let (server_public, server_secret) = keypair();
        let (client_public, client_secret) = keypair();
        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        // Zero lifetime: the cookie is stale by the time the INITIATE
        // arrives, which is the same code path as sixty seconds later.
        let mut server = CurveServer::new(&server_secret, Duration::ZERO);

        let hello = body(&client.hello().expect("HELLO"));
        server.read_hello(&hello).expect("read HELLO");
        let welcome = body(&server.welcome().expect("WELCOME"));
        client.read_welcome(&welcome).expect("read WELCOME");
        let initiate = body(&client.initiate(&Metadata::new()).expect("INITIATE"));
        let err = server.read_initiate(&initiate).unwrap_err();
        assert_eq!(err.errno(), "EACCES", "{err}");
        assert!(err.cause().contains("expired"), "{err}");
        assert!(!server.holds_cookie(), "and the stale key is gone");
    }

    /// Claim: a client that does not know `S` cannot get past `HELLO`, which
    /// is what the signature box is for.
    #[test]
    fn a_client_with_the_wrong_server_key_is_refused() {
        let (_, server_secret) = keypair();
        let (other_public, _) = keypair();
        let (client_public, client_secret) = keypair();
        let mut client = CurveClient::new(client_public, &client_secret, other_public);
        let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);

        let hello = body(&client.hello().expect("HELLO"));
        let err = server.read_hello(&hello).unwrap_err();
        assert_eq!(err.errno(), "EACCES", "{err}");
    }

    /// Claim: a vouch that names another server is refused. That is the
    /// deviation from CurveCP — `Box [C',S](C->S')` rather than
    /// `Box [C'](C->S)` — doing its job.
    #[test]
    fn a_vouch_for_another_server_is_refused() {
        let (server_public, server_secret) = keypair();
        let (client_public, client_secret) = keypair();
        let mut server = CurveServer::new(&server_secret, COOKIE_LIFETIME);

        // The client knows the real S for its HELLO, then vouches for a
        // different one: a man in the middle relaying a vouch it collected
        // elsewhere.
        let mut client = CurveClient::new(client_public, &client_secret, server_public);
        let hello = body(&client.hello().expect("HELLO"));
        server.read_hello(&hello).expect("read HELLO");
        let welcome = body(&server.welcome().expect("WELCOME"));
        client.read_welcome(&welcome).expect("read WELCOME");
        let (elsewhere, _) = keypair();
        client.server_key = PublicKey::from(*elsewhere.as_bytes());
        let initiate = body(&client.initiate(&Metadata::new()).expect("INITIATE"));

        let err = server.read_initiate(&initiate).unwrap_err();
        assert_eq!(err.errno(), "EACCES", "{err}");
        assert!(err.cause().contains("vouch"), "{err}");
    }

    /// Claim: destroyed session keys seal and open nothing.
    #[test]
    fn destroyed_session_keys_carry_no_more_messages() {
        let (mut client, mut server) = handshake();
        let sealed = body(&client.seal_frame(0, b"before").expect("seal"));
        server.open_message(&sealed).expect("open");

        client.destroy();
        let err = client.seal_frame(0, b"after").unwrap_err();
        assert_eq!(err.errno(), "ENOTSOCK", "{err}");
        server.destroy();
        let err = server.open_message(&sealed).unwrap_err();
        assert_eq!(err.errno(), "ENOTSOCK", "{err}");
    }

    /// Claim: a key is 32 binary octets or 40 characters of Z85, and
    /// anything else is refused with both forms named.
    #[test]
    fn a_key_is_binary_or_z85() {
        let (public, secret) = keypair();
        let text = public.to_z85();
        assert_eq!(text.len(), KEY_TEXT_LEN);
        assert_eq!(
            CurvePublicKey::parse(text.as_bytes()).expect("Z85"),
            public,
            "the Z85 form parses to the same key"
        );
        assert_eq!(
            CurvePublicKey::parse(public.as_bytes()).expect("binary"),
            public
        );
        assert_eq!(
            CurveSecretKey::parse(secret.to_z85().as_bytes()).expect("Z85"),
            secret
        );

        for wrong in [&b""[..], &[0u8; 31][..], &[0u8; 33][..], &[0u8; 39][..]] {
            let err = CurvePublicKey::parse(wrong).unwrap_err();
            assert_eq!(err.errno(), "EINVAL", "{err}");
            assert!(err.cause().contains("40"), "{err}");
        }
        // Forty octets that are not Z85: the length is right and the
        // alphabet is not.
        let err = CurvePublicKey::parse(&[b'"'; 40]).unwrap_err();
        assert_eq!(err.errno(), "EINVAL", "{err}");
        assert!(err.cause().contains("Z85"), "{err}");
    }

    /// Claim: a secret key does not print itself, and a public key does.
    #[test]
    fn a_secret_key_is_not_printable() {
        let (public, secret) = keypair();
        assert!(!format!("{secret:?}").contains(&secret.to_z85()));
        assert_eq!(format!("{public}"), public.to_z85());
        assert!(format!("{public:?}").contains(&public.to_z85()));
    }

    /// Claim: a server derives its own public key, since libzmq's manual
    /// says it "does not need to know" it.
    #[test]
    fn a_server_derives_its_own_public_key() {
        let (public, secret) = keypair();
        assert_eq!(secret.public_key(), public);
    }
}
