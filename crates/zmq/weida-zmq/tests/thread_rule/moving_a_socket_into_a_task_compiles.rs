//! Must compile: a socket moved into a spawned future.
//!
//! The thread rule forbids *sharing*, not migration — "after migrating a
//! socket from one thread to another" is libzmq's own wording — and a
//! library whose sockets could not be handed to a task would be useless on
//! any executor. So every socket type is constructed, moved into a
//! `tokio::spawn`ed future, and used there.
//!
//! Compiling is the assertion. The socket types each task announces are
//! collected only so that nothing here is dead code.

use weida_zmq::{Context, ContextConfig};

macro_rules! move_every_socket_type {
    ($($socket:ident),+ $(,)?) => {
        async fn moved_into_tasks(context: &Context) -> Vec<::weida_zmtp::SocketType> {
            let mut announced = Vec::new();
            $({
                let socket = weida_zmq::$socket::new(context).expect("socket");
                announced.push(
                    tokio::spawn(async move { socket.socket_type() })
                        .await
                        .expect("the task owns the socket"),
                );
            })+
            announced
        }
    };
}

weida_zmq::for_each_socket_type!(move_every_socket_type);

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let context = Context::new(ContextConfig::default()).expect("context");
    let announced = moved_into_tasks(&context).await;
    assert!(
        announced.len() > 1,
        "every socket type reaches a task of its own"
    );
}
