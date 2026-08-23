//! Bounded-memory demonstration (master doc §83 requirement 16).
//!
//! ```text
//! cargo run --release -p weida --example large_stream -- \
//!     --ca /tmp/weida-cert.pem --gib 4 weida://127.0.0.1:7443/echo
//! ```
//!
//! Generates a multi-gigabyte payload, streams it to `/echo`, and checksums
//! both directions. Nothing is ever materialized: the payload is produced a
//! chunk at a time and the echo is folded into a checksum as it arrives, so
//! peak memory stays flat regardless of `--gib`. On Linux the peak resident set
//! is read from `/proc/self/status` and printed.

use std::path::PathBuf;

use tokio::io::AsyncReadExt;
use weida::{ClientTls, Error, Runtime, RuntimeConfig, TransferMeta};

const CHUNK: usize = 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;

struct Args {
    ca: PathBuf,
    gib: u64,
    url: String,
}

fn usage() -> ! {
    eprintln!("usage: large_stream --ca PATH --gib N weida://HOST:PORT/PATH");
    std::process::exit(2);
}

fn parse_args() -> Args {
    let mut ca = None;
    let mut gib = None;
    let mut url = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--ca" => ca = Some(PathBuf::from(args.next().unwrap_or_else(|| usage()))),
            "--gib" => {
                gib = Some(
                    args.next()
                        .unwrap_or_else(|| usage())
                        .parse()
                        .unwrap_or_else(|e| {
                            eprintln!("--gib: {e}");
                            usage()
                        }),
                )
            }
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
        gib: gib.unwrap_or_else(|| usage()),
        url: url.unwrap_or_else(|| usage()),
    }
}

/// xorshift64: deterministic payload, no dependency.
struct Xorshift(u64);

impl Xorshift {
    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            let bytes = x.to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&bytes[..n]);
        }
    }
}

/// FNV-1a over the whole stream.
struct Fnv(u64);

impl Fnv {
    fn new() -> Fnv {
        Fnv(0xcbf2_9ce4_8422_2325)
    }

    fn update(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x1000_0000_01b3);
        }
    }
}

/// Peak resident set size in bytes, on Linux.
fn peak_rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let total = args.gib * GIB;

    let runtime = Runtime::new(RuntimeConfig::default())?;
    let requester = runtime.requester(ClientTls::from_pem_file(&args.ca));
    requester.connect(&args.url).await?;

    let (mut transfer, reply) = requester
        .open(TransferMeta::default().with_content_len(total))
        .await?;

    // The echo must be folded away concurrently; buffering it would defeat the
    // whole point.
    let reader = tokio::spawn(async move {
        let mut reply = reply.recv().await?;
        let mut digest = Fnv::new();
        let mut chunk = vec![0u8; CHUNK];
        let mut received = 0u64;
        loop {
            let n = reply.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            digest.update(&chunk[..n]);
            received += n as u64;
        }
        Ok::<(u64, u64), Error>((received, digest.0))
    });

    let started = std::time::Instant::now();
    let mut rng = Xorshift(0x2545_f491_4f6c_dd1d);
    let mut sent_digest = Fnv::new();
    let mut chunk = vec![0u8; CHUNK];
    let mut sent = 0u64;
    while sent < total {
        let n = std::cmp::min(CHUNK as u64, total - sent) as usize;
        rng.fill(&mut chunk[..n]);
        sent_digest.update(&chunk[..n]);
        transfer.write_all(&chunk[..n]).await?;
        sent += n as u64;
    }
    let delivery = transfer.finish()?;
    let (received, received_digest) = reader.await??;
    let elapsed = started.elapsed();

    let mib = (sent + received) as f64 / (1024.0 * 1024.0);
    match delivery.delivered().await {
        Ok(()) => println!("delivered (transport receipt)"),
        Err(e) => println!("undelivered: {e}"),
    }
    println!("sent={sent} received={received}");
    println!("checksum_sent={:#018x}", sent_digest.0);
    println!("checksum_received={received_digest:#018x}");
    println!(
        "elapsed={:.2}s throughput={:.1} MiB/s (both directions)",
        elapsed.as_secs_f64(),
        mib / elapsed.as_secs_f64()
    );
    match peak_rss() {
        Some(bytes) => println!("peak_rss={:.1} MiB", bytes as f64 / (1024.0 * 1024.0)),
        None => println!("peak_rss=unavailable"),
    }

    if received != sent || received_digest != sent_digest.0 {
        eprintln!("MISMATCH: the echo does not match what was sent");
        std::process::exit(1);
    }
    println!("verified: {} GiB echoed byte for byte", args.gib);

    runtime.shutdown().await;
    Ok(())
}
