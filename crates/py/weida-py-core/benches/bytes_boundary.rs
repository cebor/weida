//! What a payload costs on the way across, measured rather than asserted.
//!
//! `src/bytes.rs` claims one copy per direction and no more, and names what
//! forces each one. This bench is the number behind the claim: it is the only
//! way to answer "is the boundary worth optimising?" with something other than
//! an opinion, and the numbers go into
//! [IMPLEMENTATION.md](../../../../docs/IMPLEMENTATION.md).
//!
//! Four sizes, because the answer changes shape across them: an empty frame
//! (the ZeroMQ envelope delimiter, where the whole cost is the object), 64 B (a
//! realistic control message), 1 KiB, and 1 MiB (`ZMQ_MAXMSGSIZE`'s default in
//! this workspace, where the copy is memory bandwidth and nothing else).
//!
//! The GIL is held throughout, as it is in a real binding's conversion, so
//! what is measured includes the attach that a call from Python has already
//! paid for.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use weida_py_core::bytes::{payload, payload_of, py_bytes};

const SIZES: [usize; 4] = [0, 64, 1024, 1024 * 1024];

fn bench_boundary(c: &mut Criterion) {
    Python::initialize();
    Python::attach(|py| {
        let mut group = c.benchmark_group("bytes_boundary");
        for size in SIZES {
            let owned = vec![0x5au8; size];
            let from_python = PyBytes::new(py, &owned).into_any();

            // Rust to Python: one copy into CPython's own allocation, which is
            // the only thing a `bytes` object will accept.
            group.bench_function(format!("rust_to_python/{size}"), |b| {
                b.iter(|| black_box(py_bytes(py, black_box(&owned))))
            });
            // Python to Rust, borrowed: a reference count and no bytes touched.
            group.bench_function(format!("python_to_rust_borrowed/{size}"), |b| {
                b.iter(|| black_box(payload(black_box(&from_python)).expect("bytes")))
            });
            // Python to Rust, owned: the copy an owning frame type forces.
            group.bench_function(format!("python_to_rust_owned/{size}"), |b| {
                b.iter(|| black_box(payload_of(black_box(&from_python)).expect("bytes")))
            });
        }
        group.finish();
    });
}

criterion_group!(benches, bench_boundary);
criterion_main!(benches);
