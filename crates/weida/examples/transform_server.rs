//! Reference prototype server (master doc §83).
//!
//! ```text
//! cargo run -p weida --example transform_server -- --bind 127.0.0.1:7443
//! ```
//!
//! Serves two endpoints:
//!
//! * `/transform` — uppercases the request payload, streaming in 64 KiB
//!   chunks and opening the reply stream as soon as the first chunk arrives,
//!   before the request has finished;
//! * `/echo` — returns the payload unchanged.
//!
//! On start the server prints one line per endpoint of the form
//! `weida://sha256:…@127.0.0.1:7443/transform`. That address is the whole
//! client configuration: it says where to dial and which public key must
//! answer. No certificate file changes hands.
//!
//! Without `--identity` a fresh identity is generated on every start and the
//! address changes with it. With `--identity PATH` the identity is loaded
//! from `PATH`, or generated and written there (owner-only) on first use, so
//! the address is stable across restarts. The private key is otherwise never
//! written anywhere.
//!
//! `--cert-out PATH` additionally writes the certificate, for a client that
//! prefers to trust it as an anchor (`transform_client --ca PATH`, plain
//! address). The certificate names `localhost`, `127.0.0.1` and `::1`, which
//! is what the anchor path checks against.

use std::net::SocketAddr;
use std::path::PathBuf;

use tokio::io::AsyncReadExt;
use weida::{Identity, IncomingRequest, Replier, Runtime, RuntimeConfig, TransferMeta};

const CHUNK: usize = 64 * 1024;

struct Args {
    bind: SocketAddr,
    identity: Option<PathBuf>,
    cert_out: Option<PathBuf>,
}

fn usage() -> ! {
    eprintln!("usage: transform_server --bind ADDR [--identity PATH] [--cert-out PATH]");
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut bind = None;
    let mut identity = None;
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
            "--identity" => identity = Some(PathBuf::from(args.next().unwrap_or_else(|| usage()))),
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
        identity,
        cert_out,
    }
}

/// Loads the identity at `path`, or generates one and stores it there.
fn load_or_create_identity(path: &PathBuf) -> Result<Identity, Box<dyn std::error::Error>> {
    if path.exists() {
        let identity = Identity::from_pem_file(path);
        // Read it now so a corrupt file fails here, with the path in hand.
        identity.fingerprint()?;
        tracing::info!(path = %path.display(), "loaded identity");
        return Ok(identity);
    }
    let identity = Identity::generate_for(["localhost", "127.0.0.1", "::1"])?;
    write_private(path, identity.to_pem()?.as_bytes())?;
    tracing::info!(path = %path.display(), "generated and stored a new identity");
    Ok(identity)
}

/// Writes `bytes` to `path` readable by the owner only.
fn write_private(path: &PathBuf, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    options.open(path)?.write_all(bytes)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,weida=debug")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = parse_args();

    let identity = match &args.identity {
        Some(path) => load_or_create_identity(path)?,
        None => Identity::generate_for(["localhost", "127.0.0.1", "::1"])?,
    };
    if let Some(path) = &args.cert_out {
        // Only the certificate is published; the key stays in the process.
        std::fs::write(path, identity.certificate_pem()?)?;
        tracing::info!(cert = %path.display(), "wrote the certificate");
    }
    let fingerprint = identity.fingerprint()?;

    let runtime = Runtime::new(RuntimeConfig::default())?;
    let listener = runtime.listener();
    let binding = listener.bind_quic(args.bind, identity).await?;

    let transform = listener.replier("/transform")?;
    let echo = listener.replier("/echo")?;

    // The addresses go to stdout: they are the output of this program, in
    // the sense that a client needs nothing else to talk to it.
    let host = binding.local_addr().ip();
    let port = binding.local_addr().port();
    for path in ["/transform", "/echo"] {
        println!("weida://{fingerprint}@{host}:{port}{path}");
    }

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
        content_len = meta.content_len.unwrap_or_default(),
        trace_id = %trace_id,
        "request accepted"
    );

    // Taken before `reply` consumes the request, and independent of the stream
    // handle, so it can sit in the `select!` below beside the writes.
    let canceled = request.canceled();
    tokio::pin!(canceled);
    // Detaching the request half keeps both directions of the one
    // bidirectional stream live at once: the reply is written while the
    // request is still arriving, which is the point of the architecture.
    let mut body = request.take_body();
    let mut out = request.reply(TransferMeta::default()).await?;
    tracing::debug!(trace_id = %trace_id, "reply half opened before request FIN");

    let mut chunk = vec![0u8; CHUNK];
    let mut total = 0u64;
    loop {
        let n = body.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        total += n as u64;
        transform.apply(&mut chunk[..n]);

        tokio::select! {
            _ = &mut canceled => {
                tracing::info!(trace_id = %trace_id, "requester canceled; abandoning the reply");
                return Ok(());
            }
            written = out.write_all(&chunk[..n]) => written?,
        }
    }

    out.finish()?;
    tracing::info!(
        bytes = total,
        trace_id = %trace_id,
        "reply finished"
    );
    Ok(())
}
