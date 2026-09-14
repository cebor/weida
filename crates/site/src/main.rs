//! Renders the site into a directory.
//!
//! `cargo run -p weida-site` writes `target/site` and prints the path of the
//! landing page, which is openable with `file://` — there is no server to
//! start, because a site made of files does not need one until it is served
//! to somebody else.
//!
//! Two things make the build fail rather than warn: a document the manifest
//! names and the repository does not have, and a link that resolves to
//! nothing. The second one is the reason this is a tool and not a template: a
//! dead link in `docs/` is a defect in the documents, and rendering them is
//! the cheapest place to notice it.

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mut out: Option<PathBuf> = None;
    let mut root: Option<PathBuf> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => out = args.next().map(PathBuf::from),
            "--root" => root = args.next().map(PathBuf::from),
            "--help" | "-h" => {
                println!("weida-site [--root DIR] [--out DIR]");
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("weida-site: unknown argument {other}");
                return ExitCode::from(2);
            }
        }
    }
    let root = root.unwrap_or_else(weida_site::repository_root);
    let out = out.unwrap_or_else(|| root.join("target/site"));

    let site = match weida_site::Site::discover(&root) {
        Ok(site) => site,
        Err(e) => {
            eprintln!("weida-site: {e}");
            return ExitCode::FAILURE;
        }
    };
    let report = match site.render(&root, &out) {
        Ok(report) => report,
        Err(e) => {
            eprintln!("weida-site: {e}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "{} pages, {} files copied, {} off-site links",
        report.pages,
        report.assets,
        report.external.len()
    );
    if !report.dangling.is_empty() {
        eprintln!(
            "weida-site: {} links resolve to nothing:",
            report.dangling.len()
        );
        for dangling in &report.dangling {
            eprintln!("  {dangling}");
        }
        return ExitCode::FAILURE;
    }
    println!("{}", out.join("index.html").display());
    ExitCode::SUCCESS
}
