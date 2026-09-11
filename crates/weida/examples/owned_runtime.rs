//! A full exchange from a plain `fn main`: no `#[tokio::main]`, no tokio at all
//! in this file.
//!
//! ```text
//! cargo run -p weida --example owned_runtime
//! ```
//!
//! `quinn` needs a Tokio reactor; nothing else in weida does. `Runtime::owned`
//! therefore *creates and owns* that reactor — one worker thread by default —
//! and every task, timer and name lookup the library performs runs there. What
//! the caller gets back are ordinary futures, driven here by
//! `futures::executor::block_on`.
//!
//! This is the shape a host that owns its own thread needs: a language binding
//! called from Python or Java, a GUI event loop, a `smol` or `async-std`
//! program. `Runtime::new` (the ambient reactor) and `Runtime::with_handle`
//! (somebody else's) are the other two constructors; see
//! `docs/ARCHITECTURE.md` §5.
//!
//! The payload is read and written through the `futures-io` traits for the same
//! reason: a caller on `futures` should not have to wrap a compatibility shim
//! around a transfer.

use std::net::SocketAddr;

use futures::io::{AsyncReadExt, AsyncWriteExt};
use weida::{Identity, Runtime, RuntimeConfig, TransferMeta, Trust};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A plain `main`. Nothing has entered a reactor, and nothing will:
    // the runtime below owns one and keeps it to itself.
    let identity = Identity::generate()?;
    let fingerprint = identity.fingerprint()?;

    let runtime = Runtime::owned(RuntimeConfig::default())?;
    let listener = runtime.listener();
    let replier = listener.replier("/shout")?;

    futures::executor::block_on(async {
        let binding = listener
            .bind_quic("127.0.0.1:0".parse::<SocketAddr>()?, identity)
            .await?;
        let url = format!(
            "weida://{fingerprint}@127.0.0.1:{}/shout",
            binding.local_addr().port()
        );
        println!("serving {url}");

        // Both halves run on this executor, which has no reactor and no
        // `spawn`: they are joined instead of spawned.
        let server = async {
            let mut request = replier.accept().await?;
            let mut body = request.take_body();
            let mut reply = request.reply(TransferMeta::default()).await?;

            let mut heard = Vec::new();
            body.read_to_end(&mut heard).await?;
            heard.make_ascii_uppercase();
            AsyncWriteExt::write_all(&mut reply, &heard).await?;
            reply.finish()?;
            Ok::<(), weida::Error>(())
        };

        let client = async {
            let requester = runtime.requester(Trust::by_address());
            requester.connect(&url).await?;

            let (mut transfer, reply) = requester.open(TransferMeta::default()).await?;
            AsyncWriteExt::write_all(&mut transfer, b"hello from a plain main").await?;
            transfer.finish()?;

            let mut answer = reply.recv().await?;
            let mut bytes = Vec::new();
            answer.read_to_end(&mut bytes).await?;
            Ok::<Vec<u8>, weida::Error>(bytes)
        };

        let (served, answered) = futures::future::join(server, client).await;
        served?;
        println!("  {}", String::from_utf8_lossy(&answered?));

        runtime.shutdown().await;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
