//! Reference prototype client (master doc §83).
//!
//! ```text
//! printf 'hello weida' | cargo run -p weida --example transform_client -- \
//!     --ca /tmp/weida-cert.pem --ack weida://127.0.0.1:7443/transform
//! ```
//!
//! Streams standard input as the request payload and the reply to standard
//! output. The outcome and the trace id go to standard error, so the payload on
//! stdout stays clean.
//!
//! Request and reply are drained concurrently. That is not an optimisation: a
//! responder that answers while still receiving has flow control live in both
//! directions, so a client that wrote its whole request before reading would
//! stall the responder and therefore itself.

use std::path::PathBuf;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use weida::{AckMode, ClientTls, Error, Runtime, RuntimeConfig, TransferMeta};

const CHUNK: usize = 64 * 1024;

struct Args {
    ca: PathBuf,
    ack: bool,
    url: String,
}

fn usage() -> ! {
    eprintln!("usage: transform_client --ca PATH [--ack] weida://HOST:PORT/PATH");
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut ca = None;
    let mut ack = false;
    let mut url = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--ca" => ca = Some(PathBuf::from(args.next().unwrap_or_else(|| usage()))),
            "--ack" => ack = true,
            "--help" | "-h" => usage(),
            other if other.starts_with("--") => {
                eprintln!("unexpected option: {other}");
                usage();
            }
            other => url = Some(other.to_owned()),
        }
    }
    Args {
        ca: ca.unwrap_or_else(|| usage()),
        ack,
        url: url.unwrap_or_else(|| usage()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    let runtime = Runtime::new(RuntimeConfig {
        client_tls: Some(ClientTls::from_pem_file(&args.ca)),
        ..RuntimeConfig::default()
    })?;

    let requester = runtime.requester();
    requester.connect(&args.url).await?;

    let meta = TransferMeta::default().with_ack(if args.ack {
        AckMode::Accepted
    } else {
        AckMode::None
    });
    let (mut transfer, pending) = requester.open(meta).await?;
    let trace = transfer.trace();
    eprintln!(
        "trace_id={} span_id={}",
        trace.trace_id_hex(),
        trace.span_id_hex()
    );

    let reader = tokio::spawn(async move {
        let mut reply = pending.recv().await?;
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
    let outcome = transfer.finish().await?;

    let received = reader.await??;
    eprintln!("outcome={outcome} sent={sent} received={received}");

    runtime.shutdown().await;
    Ok(())
}
