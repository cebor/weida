//! `tls+tcp://`, end to end, with certificates generated for the test.
//!
//! What these assert is the six things NNG exposes (§10) and the one thing
//! this library refuses to offer: a sender identity.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use weida_nng::{
    Admission, AuthMode, Context, ContextConfig, Error, PipeEvent, PipeInfo, RepSocket, ReqSocket,
    SocketOptions, TlsConfig,
};

/// A certificate authority and one identity signed by it.
struct Pki {
    ca_pem: Vec<u8>,
    cert_pem: Vec<u8>,
    key_pem: Vec<u8>,
}

fn pki(name: &str) -> Pki {
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "weida-nng test CA");
    let ca =
        rcgen::CertifiedIssuer::self_signed(ca_params, rcgen::KeyPair::generate().expect("ca key"))
            .expect("ca");

    let mut leaf_params =
        rcgen::CertificateParams::new(vec![name.to_owned()]).expect("leaf params");
    leaf_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, name);
    let leaf_key = rcgen::KeyPair::generate().expect("leaf key");
    let leaf = leaf_params.signed_by(&leaf_key, &ca).expect("leaf");

    Pki {
        ca_pem: ca.pem().into_bytes(),
        cert_pem: leaf.pem().into_bytes(),
        key_pem: leaf_key.serialize_pem().into_bytes(),
    }
}

fn options(tls: TlsConfig) -> SocketOptions {
    SocketOptions {
        tls: Some(tls),
        recv_timeout: Some(Duration::from_secs(5)),
        send_timeout: Some(Duration::from_secs(5)),
        handshake_timeout: Duration::from_secs(5),
        reconnect_min: Duration::from_millis(10),
        ..SocketOptions::default()
    }
}

/// Claim: a REQ/REP round trip runs over `tls+tcp://`, the peer's
/// certificate is validated against the configured CA, and the
/// verification result, the common name and the alternative names all
/// reach the pipe-add-pre hook (§10).
#[tokio::test]
async fn a_round_trip_over_tls_reports_the_peer_to_the_hook() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");
    let client = pki("client.test");

    // The listener requires a client certificate, which is what puts a
    // peer on the client side into the hook's hands.
    let rep = RepSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::Required,
            ca_pem: Some(client.ca_pem.clone()),
            cert_pem: Some(server.cert_pem.clone()),
            key_pem: Some(server.key_pem.clone()),
            server_name: None,
        }),
    )
    .expect("rep");

    let seen: Arc<std::sync::Mutex<Option<weida_nng::TlsPeer>>> =
        Arc::new(std::sync::Mutex::new(None));
    let recorded = Arc::clone(&seen);
    rep.notify(Arc::new(move |event, info: &PipeInfo| {
        if event == PipeEvent::AddPre {
            *recorded.lock().unwrap() = info.tls.clone();
        }
        Admission::Accept
    }));
    let url = rep
        .listen("tls+tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();
    // The listening URL keeps its scheme, so `NNG_OPT_URL` is dialable.
    assert!(url.starts_with("tls+tcp://"), "{url}");

    let req = ReqSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::Required,
            ca_pem: Some(server.ca_pem.clone()),
            cert_pem: Some(client.cert_pem.clone()),
            key_pem: Some(client.key_pem.clone()),
            // The URL names an address, which carries no name a
            // certificate can be checked against, so the expected name is
            // said explicitly — which is what NNG_OPT_TLS_SERVER_NAME is
            // for.
            server_name: Some("server.test".to_owned()),
        }),
    )
    .expect("req");
    req.dial(&url).await.expect("dial");

    req.send(b"ping".to_vec()).await.expect("send");
    let request = rep.recv().await.expect("recv");
    assert_eq!(request.body(), b"ping");
    rep.send(b"pong".to_vec()).await.expect("reply");
    assert_eq!(req.recv().await.expect("reply").body(), b"pong");

    let peer = seen.lock().unwrap().clone().expect("the hook saw the peer");
    assert!(peer.verified, "the client certificate validated");
    assert_eq!(peer.common_name.as_deref(), Some("client.test"));
    assert_eq!(peer.subject_alt_names, vec!["client.test".to_owned()]);

    // The dialling side knows the same kind of thing about its server.
    let server_peer = req.pipe_infos()[0]
        .tls
        .clone()
        .expect("the dialler has a TLS peer too");
    assert!(server_peer.verified);
    assert_eq!(
        server_peer.subject_alt_names,
        vec!["server.test".to_owned()]
    );
}

/// Claim: an allow-list at the hook refuses a peer on what TLS
/// established, and the refusal is a close with nothing sent back — which
/// is all SP has (§6, §10).
#[tokio::test]
async fn an_allow_list_on_the_alt_names_refuses_a_peer() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");
    let client = pki("client.test");

    let rep = RepSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::Required,
            ca_pem: Some(client.ca_pem.clone()),
            cert_pem: Some(server.cert_pem.clone()),
            key_pem: Some(server.key_pem.clone()),
            server_name: None,
        }),
    )
    .expect("rep");
    let refusals = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&refusals);
    rep.notify(Arc::new(move |event, info: &PipeInfo| {
        if event != PipeEvent::AddPre {
            return Admission::Accept;
        }
        let allowed = info.tls.as_ref().is_some_and(|peer| {
            peer.verified
                && peer
                    .subject_alt_names
                    .iter()
                    .any(|name| name == "allowed.test")
        });
        if allowed {
            Admission::Accept
        } else {
            counter.fetch_add(1, Ordering::SeqCst);
            Admission::Reject("not on the allow-list".into())
        }
    }));
    let url = rep
        .listen("tls+tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let req = ReqSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::Required,
            ca_pem: Some(server.ca_pem.clone()),
            cert_pem: Some(client.cert_pem.clone()),
            key_pem: Some(client.key_pem.clone()),
            server_name: Some("server.test".to_owned()),
        }),
    )
    .expect("req");
    // The dial may or may not return before the refusal reaches it: the
    // refusal is a close, so what the dialler observes is a pipe that
    // existed and then did not. Either way the listener admitted nothing.
    let dialled = req.dial(&url).await;
    for _ in 0..200 {
        if refusals.load(Ordering::SeqCst) >= 1 && req.pipe_count() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if let Err(error) = dialled {
        assert!(
            matches!(
                error,
                Error::ECONNRESET(_) | Error::ECONNABORTED(_) | Error::ETIMEDOUT(_)
            ),
            "{error:?}"
        );
    }
    assert_eq!(refusals.load(Ordering::SeqCst), 1);
    assert_eq!(rep.pipe_count(), 0, "the listener admitted nothing");
    assert_eq!(req.pipe_count(), 0, "and the dialler's pipe is gone");
}

/// Claim: a peer whose certificate does not validate against the
/// configured CA is refused as `NNG_EPEERAUTH`, which is the code NNG's
/// own dial reports for it (§8).
#[tokio::test]
async fn an_untrusted_certificate_is_peer_auth() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");
    let stranger = pki("stranger.test");

    let rep = RepSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::None,
            ca_pem: None,
            cert_pem: Some(server.cert_pem.clone()),
            key_pem: Some(server.key_pem.clone()),
            server_name: None,
        }),
    )
    .expect("rep");
    let url = rep
        .listen("tls+tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    // The client trusts a CA that did not sign the server's certificate.
    let req = ReqSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::Required,
            ca_pem: Some(stranger.ca_pem.clone()),
            cert_pem: None,
            key_pem: None,
            server_name: Some("server.test".to_owned()),
        }),
    )
    .expect("req");
    let err = req.dial(&url).await.unwrap_err();
    assert!(matches!(err, Error::EPEERAUTH(_)), "{err:?}");
}

/// Claim: `NNG_TLS_AUTH_MODE_NONE` encrypts and establishes nothing, and
/// says so — `verified` is false, which is the field an allow-list has to
/// read.
#[tokio::test]
async fn auth_mode_none_encrypts_and_proves_nothing() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let server = pki("server.test");

    let rep = RepSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::None,
            ca_pem: None,
            cert_pem: Some(server.cert_pem.clone()),
            key_pem: Some(server.key_pem.clone()),
            server_name: None,
        }),
    )
    .expect("rep");
    let url = rep
        .listen("tls+tcp://127.0.0.1:0")
        .await
        .expect("listen")
        .url()
        .to_string();

    let req = ReqSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::None,
            ca_pem: None,
            cert_pem: None,
            key_pem: None,
            // Any name will do: nothing is checked against it.
            server_name: Some("whatever.invalid".to_owned()),
        }),
    )
    .expect("req");
    req.dial(&url).await.expect("an unverified dial succeeds");

    req.send(b"ping".to_vec()).await.expect("send");
    assert_eq!(rep.recv().await.expect("recv").body(), b"ping");

    let peer = req.pipe_infos()[0].tls.clone().expect("a TLS peer");
    assert!(
        !peer.verified,
        "nothing was validated, and the field says so rather than implying otherwise"
    );
    // The server side of an unauthenticated client has no certificate at
    // all, so there is nothing to report and nothing to allow-list on.
    let client_side = rep.pipe_infos()[0].tls.clone().expect("a TLS peer");
    assert!(!client_side.verified);
    assert_eq!(client_side.common_name, None);
}

/// Claim: a `tls+tcp` dial whose URL names an address and whose
/// configuration names no server name is refused, because there would be
/// nothing for the certificate to be checked against.
#[tokio::test]
async fn a_tls_dial_with_no_name_to_check_is_refused() {
    let ctx = Context::new(ContextConfig::default()).expect("context");
    let req = ReqSocket::with_options(
        &ctx,
        options(TlsConfig {
            auth_mode: AuthMode::Required,
            ca_pem: Some(pki("x").ca_pem),
            cert_pem: None,
            key_pem: None,
            server_name: None,
        }),
    )
    .expect("req");
    let err = req.dial("tls+tcp://127.0.0.1:9").await.unwrap_err();
    assert!(matches!(err, Error::EADDRINVAL(_)), "{err:?}");
    assert!(err.cause().contains("NNG_OPT_TLS_SERVER_NAME"));
}
