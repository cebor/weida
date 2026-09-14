//! What the site has to be true of, checked against the repository itself.
//!
//! These are not tests of a renderer — `pulldown-cmark` has its own — but of
//! the two properties that decide whether a generated site can be trusted:
//! that it covers the documents, and that every link in it goes somewhere.
//! Both fail on a change to `docs/`, not on a change to this crate, which is
//! the point: the site is the document set, so the document set is what the
//! tests are about.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use weida_site::{Group, Site, repository_root};

/// One rendered copy of the whole site, in a directory of this test's own.
struct Built {
    out: PathBuf,
    site: Site,
    report: weida_site::Report,
}

impl Built {
    fn once() -> Built {
        // One directory per instance, not one per process: these tests run
        // as threads of one binary, and a shared directory would have each
        // one's destructor deleting another's output.
        static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let serial = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root = repository_root();
        let out =
            std::env::temp_dir().join(format!("weida-site-test-{}-{serial}", std::process::id()));
        let _ = fs::remove_dir_all(&out);
        let site = Site::discover(&root).expect("discover");
        let report = site.render(&root, &out).expect("render");
        Built { out, site, report }
    }
    fn page(&self, url: &str) -> String {
        fs::read_to_string(self.out.join(url)).unwrap_or_else(|e| panic!("{url}: {e}"))
    }
}

impl Drop for Built {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.out);
    }
}

#[test]
fn every_document_is_published_or_excluded_on_purpose() {
    let root = repository_root();
    let site = Site::discover(&root).expect("discover");
    let published: BTreeSet<&str> = site.pages().iter().map(|p| p.source.as_str()).collect();

    let mut unaccounted = Vec::new();
    for document in weida_site::documents(&root).expect("walk") {
        if published.contains(document.as_str()) || weida_site::excluded(&document).is_some() {
            continue;
        }
        unaccounted.push(document);
    }
    assert!(
        unaccounted.is_empty(),
        "these documents are neither on the site nor excluded with a reason, so the site has \
         quietly fallen behind the repository: {unaccounted:?}"
    );
}

#[test]
fn an_exclusion_carries_its_reason_and_the_reason_is_a_sentence() {
    for (pattern, reason) in weida_site::EXCLUDED {
        assert!(
            reason.len() > 30,
            "{pattern} is excluded with too little said about why: {reason:?}"
        );
    }
}

#[test]
fn every_link_between_documents_resolves() {
    let built = Built::once();
    assert!(
        built.report.dangling.is_empty(),
        "links that point at nothing — a defect in the documents, which is why the build \
         fails on them: {:?}",
        built.report.dangling
    );
}

#[test]
fn every_page_carries_the_notice_that_nothing_is_released() {
    let built = Built::once();
    for page in built.site.pages() {
        let html = built.page(&page.url);
        assert!(
            html.contains(weida_site::NOTICE),
            "{} does not say that weida is unreleased, and a reader arrives on any page",
            page.url
        );
    }
}

#[test]
fn a_link_to_an_excluded_document_reaches_the_repository_instead_of_a_missing_page() {
    let built = Built::once();
    // The status page cites the backlog and the nightlog, and neither is
    // published: what a reader must get is the file in the forge, never a
    // page that is not there.
    let status = built.page("docs/STATUS.html");
    assert!(
        status.contains("git.doodleshnookie.net/tuco86/weida/src/branch/main/docs/BACKLOG.md"),
        "a link to an excluded document must become a link into the source"
    );
    assert!(
        !status.contains("BACKLOG.html"),
        "and must not become a page the site does not have"
    );
}

#[test]
fn the_landing_page_is_the_readme_and_needs_no_prose_of_its_own() {
    let built = Built::once();
    let root = repository_root();
    let index = built.page("index.html");
    let readme = fs::read_to_string(root.join("README.md")).expect("README.md");
    // A sentence from the middle of the README, so the assertion is about the
    // document's body rather than about its title.
    let claim = readme
        .lines()
        .find(|line| line.contains("messaging framework for Rust"))
        .expect("the README's opening claim");
    let words = claim
        .split_whitespace()
        .take(8)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        index.contains(&words),
        "the landing page must be the README, rendered: {words:?} is missing"
    );
}

#[test]
fn a_page_links_to_its_siblings_by_a_path_that_works_over_the_file_scheme() {
    let built = Built::once();
    // A decision note sits two directories deep, so its navigation has to
    // climb out. Nothing here is served by a web server, and a root-relative
    // link would break the moment the output is opened from disk.
    let note = built
        .site
        .pages()
        .iter()
        .find(|p| p.group == Group::Decisions && p.url.ends_with("0009-drain.html"))
        .expect("the drain note");
    let html = built.page(&note.url);
    assert!(html.contains("href=\"../../index.html\""), "{}", note.url);
    assert!(
        !html.contains("href=\"/"),
        "{} carries a root-relative link, which file:// cannot follow",
        note.url
    );
    assert!(
        built.out.join("docs/decisions/0009-drain.html").exists(),
        "the page the navigation points at must exist"
    );
}

#[test]
fn the_pictures_the_status_page_shows_are_copied_beside_it() {
    let built = Built::once();
    let status = built.page("docs/STATUS.html");
    assert!(
        status.contains("../docs/status/roadmap.svg"),
        "the image is not addressed"
    );
    assert!(
        built.out.join("docs/status/roadmap.svg").exists(),
        "and the file it addresses is not there"
    );
}

#[test]
fn a_document_that_the_manifest_names_and_the_repository_lacks_fails_the_build() {
    // The failure mode this guards is a renamed document: the navigation
    // would otherwise carry an entry that leads nowhere, which is the one
    // thing a generated site must never do.
    let empty = std::env::temp_dir().join(format!("weida-site-empty-{}", std::process::id()));
    let _ = fs::remove_dir_all(&empty);
    fs::create_dir_all(empty.join("docs/decisions")).expect("scratch");
    fs::create_dir_all(empty.join("docs/libraries")).expect("scratch");
    fs::create_dir_all(empty.join("docs/adapters")).expect("scratch");
    let outcome = Site::discover(&empty);
    let _ = fs::remove_dir_all(&empty);
    let error = outcome.expect_err("a tree with no documents cannot be a site");
    assert!(
        error.to_string().contains("README.md"),
        "the error must name the document that is missing: {error}"
    );
}

#[test]
fn the_stylesheet_is_the_only_asset_the_shell_adds() {
    let built = Built::once();
    // No JavaScript and no web font: a page that needs neither should ask
    // for neither, and a site that fetches a font leaks its readers to
    // whoever serves it.
    for page in built.site.pages() {
        let html = built.page(&page.url);
        assert!(!html.contains("<script"), "{} loads a script", page.url);
        assert!(
            !html.contains("fonts.googleapis") && !html.contains("cdn."),
            "{} fetches something from somebody else",
            page.url
        );
    }
    assert!(built.out.join("site.css").exists());
}

#[test]
fn the_only_off_site_links_are_ones_a_document_chose() {
    let built = Built::once();
    // The shell adds exactly one: the repository. Everything else in this
    // set comes from a document's own prose, and the list is worth printing
    // rather than asserting a number, because it is what an operator checks
    // before the site is served to anybody.
    let shell_only = Path::new("index.html");
    assert!(built.out.join(shell_only).exists());
    for external in &built.report.external {
        assert!(
            external.starts_with("http://")
                || external.starts_with("https://")
                || external.starts_with("mailto:"),
            "an off-site link with a scheme nobody asked for: {external}"
        );
    }
}
