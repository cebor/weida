//! Reference prototype server (master doc §83).
//!
//! ```text
//! cargo run -p weida --example transform_server -- \
//!     --bind 127.0.0.1:7443 --cert-out /tmp/weida-cert.pem
//! ```
//!
//! Serves two endpoints:
//!
//! * `/transform` — uppercases the request payload, streaming in 64 KiB chunks
//!   and opening the reply stream as soon as the first chunk arrives, before
//!   the request has finished;
//! * `/echo` — returns the payload unchanged.
//!
//! A self-signed certificate is generated on every start, valid for
//! `localhost`, `127.0.0.1` and `::1`. The certificate is written to
//! `--cert-out` for clients to trust; the private key is written next to it
//! with a `.key.pem` suffix, because the server loads its key from a file.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use tokio::io::AsyncReadExt;
use weida::{IncomingRequest, Replier, Runtime, RuntimeConfig, ServerTls, TransferMeta};

const CHUNK: usize = 64 * 1024;

struct Args {
    bind: SocketAddr,
    cert_out: PathBuf,
}

fn usage() -> ! {
    eprintln!("usage: transform_server --bind ADDR --cert-out PATH");
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut bind = None;
    let mut cert_out = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bind" => {
                bind = Some(
                    args.next()
                        .unwrap_or_else(|| usage())
                        .parse()
                        .unwrap_or_else(|e| {
                            eprintln!("--bind: {e}");
                            usage()
                        }),
                )
            }
            "--cert-out" => cert_out = Some(PathBuf::from(args.next().unwrap_or_else(|| usage()))),
            "--help" | "-h" => usage(),
            other => {
                eprintln!("unexpected argument: {other}");
                usage();
            }
        }
    }
    Args {
        bind: bind.unwrap_or_else(|| usage()),
        cert_out: cert_out.unwrap_or_else(|| usage()),
    }
}

/// Derives the key path from the certificate path.
fn key_path(cert_out: &Path) -> PathBuf {
    let mut name = cert_out
        .file_stem()
        .unwrap_or_else(|| std::ffi::OsStr::new("weida"))
        .to_os_string();
    name.push(".key.pem");
    cert_out.with_file_name(name)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,weida=debug")),
        )
        .init();

    let args = parse_args();

    let generated = rcgen::generate_simple_self_signed(vec![
        "localhost".to_owned(),
        "127.0.0.1".to_owned(),
        "::1".to_owned(),
    ])?;
    let key_out = key_path(&args.cert_out);
    std::fs::write(&args.cert_out, generated.cert.pem())?;
    std::fs::write(&key_out, generated.signing_key.serialize_pem())?;
    tracing::info!(
        cert = %args.cert_out.display(),
        key = %key_out.display(),
        "generated a self-signed certificate"
    );

    let runtime = Runtime::new(RuntimeConfig::default())?;
    let listener = runtime
        .listener(ServerTls::new(&args.cert_out, &key_out))
        .await?;
    let binding = listener.bind_quic(args.bind).await?;

    let transform = listener.replier("/transform")?;
    let echo = listener.replier("/echo")?;
    tracing::info!(addr = %binding.local_addr(), "serving /transform and /echo");

    tokio::spawn(serve(transform, Transform::Uppercase));
    tokio::spawn(serve(echo, Transform::Identity));

    // Nothing else to do on the main task: the `signal` tokio feature is not in
    // the dependency set, so stop this process with Ctrl-C.
    std::future::pending::<()>().await;
    Ok(())
}

#[derive(Clone, Copy)]
enum Transform {
    Uppercase,
    Identity,
}

impl Transform {
    fn apply(self, chunk: &mut [u8]) {
        match self {
            Transform::Uppercase => chunk.make_ascii_uppercase(),
            Transform::Identity => {}
        }
    }
}

async fn serve(replier: Replier, transform: Transform) {
    loop {
        match replier.accept().await {
            Ok(request) => {
                tokio::spawn(async move {
                    if let Err(e) = handle(request, transform).await {
                        tracing::warn!(error = %e, "request failed");
                    }
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, path = replier.path(), "endpoint stopped accepting");
                return;
            }
        }
    }
}

async fn handle(
    mut request: IncomingRequest,
    transform: Transform,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let meta = request.meta().clone();
    let trace_id = meta
        .trace
        .map(|t| t.trace_id_hex())
        .unwrap_or_else(|| "-".to_owned());
    tracing::info!(
        endpoint = meta.endpoint.as_deref().unwrap_or("-"),
        transfer_id = %meta.transfer_id,
        ack_mode = %meta.ack_mode,
        content_len = meta.content_len.unwrap_or_default(),
        trace_id = %trace_id,
        "request accepted"
    );

    let mut reply = None;
    let mut chunk = vec![0u8; CHUNK];
    let mut total = 0u64;

    loop {
        let n = request.body().read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        total += n as u64;
        // The reply stream opens on the first chunk, long before the request
        // reaches FIN: that overlap is the point of the architecture.
        let out = match &mut reply {
            Some(out) => out,
            None => {
                let out = request.reply(TransferMeta::default()).await?;
                tracing::debug!(trace_id = %trace_id, "reply stream opened before request FIN");
                reply.insert(out)
            }
        };
        transform.apply(&mut chunk[..n]);
        out.write_all(&chunk[..n]).await?;

        if request.is_canceled() {
            tracing::info!(trace_id = %trace_id, "requester canceled; abandoning the reply");
            return Ok(());
        }
    }

    // An empty request still deserves a reply, otherwise the requester waits.
    let reply = match reply {
        Some(reply) => reply,
        None => request.reply(TransferMeta::default()).await?,
    };
    let outcome = reply.finish().await?;
    tracing::info!(
        bytes = total,
        trace_id = %trace_id,
        %outcome,
        "reply finished"
    );
    Ok(())
}
