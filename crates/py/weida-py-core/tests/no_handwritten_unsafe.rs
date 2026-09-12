//! The half of `unsafe_code = "forbid"` this crate can still keep.
//!
//! Every other crate in this workspace forbids `unsafe_code` outright. A PyO3
//! crate cannot: `#[pyclass]`, `#[pymethods]` and `#[pyfunction]` expand to
//! `unsafe impl`s of PyO3's type-object traits, and the lint does not care that
//! the code came from a macro. So the manifest allows it and this test asserts
//! the property that is actually load-bearing — that no `unsafe` is written by
//! hand in this crate's own source — because "we allowed it for the macros"
//! stops being true the moment somebody writes an `unsafe` block under that
//! allowance and nothing complains.

use std::fs;
use std::path::Path;

#[test]
fn no_unsafe_is_written_by_hand_in_src() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders = Vec::new();
    let mut files = 0usize;
    for entry in fs::read_dir(&src).expect("the crate has a src directory") {
        let path = entry.expect("a readable directory entry").path();
        if path.extension().is_none_or(|extension| extension != "rs") {
            continue;
        }
        files += 1;
        let source = fs::read_to_string(&path).expect("readable Rust source");
        for (number, line) in source.lines().enumerate() {
            // The word appears in this crate's prose — the module documentation
            // explains what PyO3's macros expand to — and a doc comment is not
            // code.
            let code = line.trim_start();
            if code.starts_with("//") {
                continue;
            }
            if code.contains("unsafe ") || code.contains("unsafe{") {
                offenders.push(format!("{}:{}: {}", path.display(), number + 1, code));
            }
        }
    }
    assert!(
        files > 0,
        "no Rust source was scanned, so nothing was checked"
    );
    assert!(
        offenders.is_empty(),
        "unsafe is written by hand in this crate, which its manifest only allows for PyO3's \
         macro expansion:\n{}",
        offenders.join("\n")
    );
}
