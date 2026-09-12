//! The other half of the rule: the directions each protocol *does* have are
//! there, so the compile-fail cases above are about direction rather than
//! about a socket that cannot do anything at all.

use weida_nng::{Context, ContextConfig, PullSocket, PushSocket};

fn main() {
    let ctx = Context::owned(ContextConfig::default()).expect("context");
    let push = PushSocket::new(&ctx).expect("push");
    let pull = PullSocket::new(&ctx).expect("pull");
    let _sending = push.send(b"work".to_vec());
    let _receiving = pull.recv();
    let _nonblocking_send = push.try_send(b"work".to_vec());
    let _nonblocking_recv = pull.try_recv();
}
