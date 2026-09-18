//! The client, the three auths and the three sources, against a scripted
//! OpenBao ([0032](../../../docs/decisions/0032-identity-sources-and-the-handoff.md)
//! §4.3). What the real server does with the same requests is
//! `tests/bao_dev.rs`, run by hand against the local binary.

mod common;

use std::time::Duration;

use common::{Bao, Script};
use serde_json::json;
use tokio::time::timeout;
use weida::{FilesOptions, IdentityEvent, IdentitySource};
use weida_openbao::{Auth, Config, Error, HandoffSource, Kv, OpenBao, PkiAnchor, PkiSign};

async fn within<F: Future>(f: F) -> F::Output {
    timeout(Duration::from_secs(10), f)
        .await
        .expect("timed out")
}

fn private_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "weida-bao-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn auth_answer(token: &str, ttl: u64, renewable: bool) -> serde_json::Value {
    json!({
        "auth": {
            "client_token": token,
            "accessor": format!("acc-{token}"),
            "lease_duration": ttl,
            "renewable": renewable,
            "policies": ["default", "svc"],
        }
    })
}

/// Claim: a given token is looked up so the client knows its TTL and
/// accessor, and every later request carries it.
#[tokio::test]
async fn a_token_is_looked_up_and_then_used() {
    let script = Script::new();
    script.on("GET", "auth/token/lookup-self", |r| {
        assert_eq!(r.token(), Some("t-given"));
        (200, json!({ "data": { "accessor": "acc-1", "ttl": 3600, "renewable": false, "policies": ["p"] } }))
    });
    script.on("GET", "secret/data/x", |r| {
        assert_eq!(r.token(), Some("t-given"));
        (200, json!({ "data": { "data": { "v": 1 } } }))
    });
    let bao = Bao::start(script).await;
    let client = OpenBao::new(Config::new(&bao.address)).expect("client");
    let info = within(client.login(Auth::Token("t-given".into())))
        .await
        .expect("login");
    assert_eq!(info.accessor.as_deref(), Some("acc-1"));
    assert_eq!(info.ttl, Duration::from_secs(3600));
    assert!(!info.renewable);
    let v = within(client.get("secret/data/x")).await.expect("get");
    assert_eq!(v["data"]["data"]["v"], 1);
}

/// Claim: AppRole logs in without a token and comes back with one.
#[tokio::test]
async fn approle_logs_in_anonymously() {
    let script = Script::new();
    script.on("POST", "auth/approle/login", |r| {
        assert_eq!(r.token(), None, "a login carries no token");
        assert_eq!(r.body["role_id"], "r1");
        assert_eq!(r.body["secret_id"], "s1");
        (200, auth_answer("t-approle", 60, true))
    });
    let bao = Bao::start(script).await;
    let client = OpenBao::new(Config::new(&bao.address)).expect("client");
    let info = within(client.login(Auth::approle("r1", "s1")))
        .await
        .expect("login");
    assert_eq!(info.accessor.as_deref(), Some("acc-t-approle"));
    assert_eq!(client.accessor().as_deref(), Some("acc-t-approle"));
}

/// Claim: the hand-off redeems the wrapping token as its first and only
/// request, with the wrapping token as the token; what comes back is the
/// service token. A wrapping token somebody already redeemed is
/// `HandoffStolen`, not an API error among others.
#[tokio::test]
async fn the_handoff_redeems_once_and_theft_is_named() {
    let script = Script::new();
    // Single use, as the real endpoint: the first redemption of `w-fresh`
    // succeeds, every later one is the defined error.
    let redeemed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    script.on("POST", "sys/wrapping/unwrap", move |r| {
        let first = r.token() == Some("w-fresh")
            && !redeemed.swap(true, std::sync::atomic::Ordering::AcqRel);
        if first {
            (200, auth_answer("t-service", 120, true))
        } else {
            (
                400,
                json!({ "errors": ["wrapping token is not valid or does not exist"] }),
            )
        }
    });
    let bao = Bao::start(script).await;
    let dir = private_dir("handoff");
    std::fs::create_dir_all(&dir).expect("dir");
    let credential = dir.join("bao-handoff");
    std::fs::write(&credential, "w-fresh\n").expect("write credential");

    let client = OpenBao::new(Config::new(&bao.address)).expect("client");
    let info = within(client.login(Auth::Handoff(HandoffSource::File(credential.clone()))))
        .await
        .expect("redeem");
    assert_eq!(info.accessor.as_deref(), Some("acc-t-service"));
    let seen = bao.script.seen.lock().expect("seen").clone();
    assert_eq!(seen.len(), 1, "the unwrap was the first and only request");
    assert_eq!(seen[0].path, "/v1/sys/wrapping/unwrap");

    // Somebody else — or this process again — redeems the same credential.
    let again = OpenBao::new(Config::new(&bao.address)).expect("client");
    let err = within(again.login(Auth::Handoff(HandoffSource::File(credential))))
        .await
        .expect_err("already redeemed");
    assert!(matches!(err, Error::HandoffStolen), "{err:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Claim: a renewable token is renewed at half its TTL for as long as the
/// client lives, and the renewed TTL is the next pace.
#[tokio::test]
async fn a_renewable_token_is_renewed_at_half_its_ttl() {
    let script = Script::new();
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 2, true))
    });
    script.on("POST", "auth/token/renew-self", |r| {
        assert_eq!(r.token(), Some("t"));
        (200, auth_answer("t", 2, true))
    });
    let bao = Bao::start(script).await;
    let client = OpenBao::new(Config::new(&bao.address)).expect("client");
    within(client.login(Auth::approle("r", "s")))
        .await
        .expect("login");
    tokio::time::sleep(Duration::from_millis(2600)).await;
    let renewals = bao.script.seen_on("auth/token/renew-self").len();
    assert!(
        (2..=3).contains(&renewals),
        "two renewals in 2.6 s at a 1 s pace, got {renewals}"
    );
}

/// Claim: `PkiSign` sends a CSR over the key it was given and serves the
/// chain that comes back; a renewal is a `Renewed` event under the same
/// fingerprint, and a renewal the server refuses is `RenewalFailed` with
/// the certificate in service unchanged.
#[tokio::test]
async fn pki_sign_keeps_the_key_and_renews_the_certificate() {
    let dir = private_dir("pki");
    let key_source = IdentitySource::files(
        &dir,
        FilesOptions {
            names: vec!["svc.local".into()],
            ..FilesOptions::default()
        },
    )
    .expect("key source");
    let fingerprint = key_source.fingerprint().expect("fingerprint");
    let key_pem = std::fs::read_to_string(dir.join("key.pem")).expect("key.pem");

    // The "CA" signs by re-signing over the CSR's key, which this script
    // knows because the test made it; what is asserted is the plumbing, the
    // real signature is the dev server's business.
    let script = Script::new();
    let signed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    script.on("POST", "pki/sign/svc", {
        let key_pem = key_pem.clone();
        let signed = signed.clone();
        move |r| {
            assert!(r.body["csr"].as_str().is_some_and(|c| c.contains("BEGIN CERTIFICATE REQUEST")));
            assert_eq!(r.body["use_csr_sans"], true);
            assert_eq!(r.body["common_name"], "svc.local");
            let n = signed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if n == 2 {
                return (403, json!({ "errors": ["permission denied"] }));
            }
            let key = rcgen::KeyPair::from_pem(&key_pem).expect("key");
            let params = rcgen::CertificateParams::new(vec!["svc.local".to_owned()]).expect("params");
            let cert = params.self_signed(&key).expect("sign").pem();
            let expiration = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_secs()
                + 3;
            (200, json!({ "data": { "certificate": cert, "ca_chain": [], "expiration": expiration } }))
        }
    });
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 0, false))
    });
    let bao = Bao::start(script).await;
    let client = OpenBao::new(Config::new(&bao.address)).expect("client");
    within(client.login(Auth::approle("r", "s")))
        .await
        .expect("login");

    let signing = PkiSign {
        ttl: Some(Duration::from_secs(3)),
        renew_at: 0.34,
        ..PkiSign::new("svc", ["svc.local"])
    };
    let source = within(signing.start(client.clone(), &key_source))
        .await
        .expect("first signature");
    assert_eq!(
        source.fingerprint().expect("fingerprint"),
        fingerprint,
        "the key is unchanged"
    );
    let mut events = source.events();
    // About one second in: the first renewal; another second: the refused one.
    assert_eq!(
        within(events.recv()).await,
        Some(IdentityEvent::Renewed { fingerprint })
    );
    let failed = within(events.recv()).await;
    assert!(
        matches!(&failed, Some(IdentityEvent::RenewalFailed(m)) if m.contains("permission denied")),
        "{failed:?}"
    );
    assert_eq!(source.fingerprint().expect("fingerprint"), fingerprint);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Claim: `PkiAnchor` trusts the mount's chain and picks up a rotated one;
/// `Kv` reads the two fields of a secret into an identity.
#[tokio::test]
async fn pki_anchor_refreshes_and_kv_reads_an_identity() {
    let ca_a = weida::Identity::generate_for(["ca-a"])
        .expect("ca a")
        .certificate_pem()
        .expect("pem");
    let ca_b = weida::Identity::generate_for(["ca-b"])
        .expect("ca b")
        .certificate_pem()
        .expect("pem");
    let stored = weida::Identity::generate_for(["stored"]).expect("stored");
    let script = Script::new();
    let fetches = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    script.on("GET", "pki/cert/ca_chain", {
        let (ca_a, ca_b, fetches) = (ca_a.clone(), ca_b.clone(), fetches.clone());
        move |_| {
            let n = fetches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let ca = if n < 2 { &ca_a } else { &ca_b };
            (200, json!({ "data": { "certificate": ca } }))
        }
    });
    script.on("GET", "secret/data/svc/identity", {
        let cert = stored.certificate_pem().expect("pem");
        let key = stored.to_pem().expect("pem");
        move |_| {
            (
                200,
                json!({ "data": { "data": { "certificate": cert, "private_key": key } } }),
            )
        }
    });
    script.on("POST", "auth/approle/login", |_| {
        (200, auth_answer("t", 0, false))
    });
    let bao = Bao::start(script).await;
    let client = OpenBao::new(Config::new(&bao.address)).expect("client");
    within(client.login(Auth::approle("r", "s")))
        .await
        .expect("login");

    let anchor = PkiAnchor {
        refresh: Duration::from_millis(100),
        ..PkiAnchor::new()
    };
    let trust = within(anchor.start(client.clone())).await.expect("anchor");
    assert_eq!(
        trust.current().anchors,
        vec![weida::Pem::Bytes(ca_a.into_bytes())]
    );
    let mut events = trust.events();
    assert_eq!(
        within(events.recv()).await,
        Some(IdentityEvent::TrustRefreshed)
    );
    assert_eq!(
        trust.current().anchors,
        vec![weida::Pem::Bytes(ca_b.into_bytes())]
    );

    let identity = within(Kv::new("svc/identity").start(client))
        .await
        .expect("kv");
    assert_eq!(
        identity.fingerprint().expect("fingerprint"),
        stored.fingerprint().expect("fingerprint")
    );
}
