//! Reference prototype client (master doc §83).
//!
//! ```text
//! printf 'hello weida' | cargo run -p weida --example transform_client -- \
//!     weida://sha256:…@127.0.0.1:7443/transform
//! ```
//!
//! The address is the one `transform_server` printed. It names the server's
//! public key, so the client needs no certificate file: `Trust::by_address`
//! accepts exactly that key and nothing else. Alternatively `--ca PATH`
//! trusts a certificate as an anchor and a plain `weida://127.0.0.1:7443/…`
//! address is verified against its names.
//!
//! Streams standard input as the request payload and the reply to standard
//! output. The delivery receipt and the trace id go to standard error, so the
//! payload on stdout stays clean.
//!
//! Request and reply are the two halves of one bidirectional QUIC stream, and
//! they are drained concurrently. That is not an optimisation: a responder
//! that answers while still receiving has flow control live in both
//! directions, so a client that wrote its whole request before reading would
//! stall the responder and therefore itself.

use std::path::PathBuf;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use weida::{Error, Runtime, RuntimeConfig, TransferMeta, Trust};

const CHUNK: usize = 64 * 1024;

struct Args {
    ca: Option<PathBuf>,
    url: String,
}

fn usage() -> ! {
    eprintln!("usage: transform_client [--ca PATH] weida://[FINGERPRINT@]HOST:PORT/PATH");
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut ca = None;
    let mut url = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--ca" => ca = Some(PathBuf::from(args.next().unwrap_or_else(|| usage()))),
            "--help" | "-h" => usage(),
            other if other.starts_with("--") => {
                eprintln!("unexpected option: {other}");
                usage();
            }
            other => url = Some(other.to_owned()),
        }
    }
    Args {
        ca,
        url: url.unwrap_or_else(|| usage()),
    }
}

fn trust(ca: Option<&PathBuf>) -> Trust {
    match ca {
        Some(path) => Trust::anchor_file(path),
        None => Trust::by_address(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    let runtime = Runtime::new(RuntimeConfig::default())?;

    let requester = runtime.requester(trust(args.ca.as_ref()));
    match requester.connect(&args.url).await {
        Ok(()) => {}
        // The one failure worth a dedicated message: the operator can check
        // this fingerprint out of band and put it into the address.
        Err(Error::Untrusted(presented)) => {
            eprintln!(
                "the peer at {} presented {presented}, which is not trusted",
                args.url
            );
            std::process::exit(1);
        }
        Err(e) => return Err(e.into()),
    }

    // A trace exists because this program asked for one: weida propagates a
    // context and never mints one
    // ([0028](../../../docs/decisions/0028-trace-propagation-is-the-callers.md)),
    // so a client that wants its request traced starts the trace itself.
    let trace = weida::new_trace();
    let (mut transfer, reply) = requester
        .open(TransferMeta::default().with_trace(trace))
        .await?;
    eprintln!(
        "trace_id={} span_id={}",
        trace.trace_id_hex(),
        trace.span_id_hex()
    );

    let reader = tokio::spawn(async move {
        let mut reply = reply.recv().await?;
        let mut stdout = tokio::io::stdout();
        let mut chunk = vec![0u8; CHUNK];
        let mut total = 0u64;
        loop {
            let n = reply.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            total += n as u64;
            stdout.write_all(&chunk[..n]).await?;
        }
        stdout.flush().await?;
        Ok::<u64, Error>(total)
    });

    let mut stdin = tokio::io::stdin();
    let mut chunk = vec![0u8; CHUNK];
    let mut sent = 0u64;
    loop {
        let n = stdin.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        sent += n as u64;
        transfer.write_all(&chunk[..n]).await?;
    }
    let delivery = transfer.finish()?;

    let received = reader.await??;
    // The receipt is QUIC's transport acknowledgement, not an application ack.
    let delivered = match delivery.delivered().await {
        Ok(()) => "delivered".to_owned(),
        Err(e) => format!("undelivered: {e}"),
    };
    eprintln!("{delivered} sent={sent} received={received}");

    runtime.shutdown().await;
    Ok(())
}
