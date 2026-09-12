//! "PUSH has no receive operation" (`docs/research/nanomsg-nng.md` §4).

use weida_nng::{Context, ContextConfig, PushSocket};

fn main() {
    let ctx = Context::owned(ContextConfig::default()).expect("context");
    let push = PushSocket::new(&ctx).expect("push");
    let _ = push.recv();
}
