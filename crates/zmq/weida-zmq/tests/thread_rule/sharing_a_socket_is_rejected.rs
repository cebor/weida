//! Must not compile: a socket shared between threads.
//!
//! `share_between_threads` takes a reference and needs `Sync`, which is what
//! two threads holding `&socket` would need. Every socket type is asked for
//! it, and every socket type must be refused.

/// What sharing a socket between threads requires, and nothing else.
fn share_between_threads<T: Sync>(_socket: &T) {}

macro_rules! share_every_socket_type {
    ($($socket:ident),+ $(,)?) => {
        $(
            #[allow(non_snake_case)]
            mod $socket {
                pub fn shared(socket: &weida_zmq::$socket) {
                    crate::share_between_threads(socket);
                }
            }
        )+
    };
}

weida_zmq::for_each_socket_type!(share_every_socket_type);

fn main() {}
