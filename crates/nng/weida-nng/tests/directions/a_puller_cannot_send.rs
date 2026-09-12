//! "PULL cannot send" (`docs/research/nanomsg-nng.md` §4).

use weida_nng::{Context, ContextConfig, PullSocket};

fn main() {
    let ctx = Context::owned(ContextConfig::default()).expect("context");
    let pull = PullSocket::new(&ctx).expect("pull");
    let _ = pull.send(b"work".to_vec());
}
