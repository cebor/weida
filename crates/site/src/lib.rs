//! The website at <https://weida.doodleshnookie.net>, rendered from this
//! repository's own documents.
//!
//! **The site writes no prose of its own.** Every sentence a visitor reads
//! comes from a file under `docs/` or from `README.md`; what this crate adds
//! is a shell — navigation, a stylesheet, a heading index and the notice of
//! [`NOTICE`] — and nothing that could disagree with the documents beside it.
//! That is the whole design: a landing page with its own hand-written summary
//! of the guarantees would be a second copy of every claim, and this
//! repository has spent enough sessions removing the first kind of drift to
//! know better than to introduce it on a web page
//! ([decisions/0025](../../../docs/decisions/0025-the-website.md)).
//!
//! What follows from that:
//!
//! - A document is **published, or excluded on purpose with the reason
//!   written down** ([`EXCLUDED`]). A new file under `docs/` that is neither
//!   fails `every_document_is_published_or_excluded_on_purpose`, so the site
//!   cannot quietly fall behind the repository.
//! - A link to an excluded document becomes a link **into the repository**
//!   rather than a page that is not there, so nothing on the site 404s and
//!   nothing silently loses its reference.
//! - The output is plain files under `target/site`, openable with `file://`.
//!   There is no server, no JavaScript, no web font and no build step beyond
//!   `cargo run -p weida-site`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use pulldown_cmark::{CowStr, Event, HeadingLevel, Options, Parser, Tag, TagEnd, html};

/// What every page says about the state of the project, in one place.
///
/// There is no public release: no crate on any registry, no tag, no binary
/// anywhere. A visitor who reads a guarantee table here is reading a
/// description of work in progress, and the notice is on **every** page
/// rather than only on the landing page, because a search engine or a direct
/// link delivers readers into the middle of a document set.
pub const NOTICE: &str = "Pre-release. weida is not published: no crate on any registry, \
     no tag, no binary. These documents are rendered from the repository and describe work \
     in progress.";

/// Where the source lives, and the only external link the shell adds.
pub const REPOSITORY: &str = "https://git.doodleshnookie.net/tuco86/weida";

/// The prefix that turns a repository-relative path into a link into the
/// source, for the documents this site does not publish.
pub const BLOB: &str = "https://git.doodleshnookie.net/tuco86/weida/src/branch/main/";

/// The site's own stylesheet, compiled in so that a build needs no data
/// directory beside the binary.
pub const STYLESHEET: &str = include_str!("../assets/site.css");

/// One group in the navigation, in the order a reader should meet them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Group {
    /// The landing page.
    Start,
    /// weida itself: the normative documents and the status.
    Weida,
    /// One note per decision.
    Decisions,
    /// The standalone protocol libraries and their bindings.
    Libraries,
    /// The mapping documents for the forwarders.
    Adapters,
}

impl Group {
    /// Every group, in navigation order.
    pub const ALL: [Group; 5] = [
        Group::Start,
        Group::Weida,
        Group::Decisions,
        Group::Libraries,
        Group::Adapters,
    ];

    /// The heading this group carries in the navigation.
    pub fn label(self) -> &'static str {
        match self {
            Group::Start => "Start",
            Group::Weida => "weida",
            Group::Decisions => "Decisions",
            Group::Libraries => "Libraries",
            Group::Adapters => "Adapters",
        }
    }
}

/// One published document.
#[derive(Clone, Debug)]
pub struct Page {
    /// Repository-relative source, always a Markdown file.
    pub source: String,
    /// Site-relative output path, always ending in `.html`.
    pub url: String,
    /// Where it appears in the navigation.
    pub group: Group,
    /// The document's own first heading, or its file name if it has none.
    pub title: String,
}

/// The documents that are **not** published, each with the reason.
///
/// The reasons are three kinds, and the distinction is worth keeping: the
/// loop's own bookkeeping is not documentation, a third party's requirements
/// are not ours to publish, and the research sheets are condensed from other
/// projects' specifications and manuals — publishing somebody else's
/// documentation is not a decision a static site generator gets to take.
pub const EXCLUDED: &[(&str, &str)] = &[
    (
        "docs/BACKLOG.md",
        "the loop's own work queue, not documentation of the product",
    ),
    (
        "docs/NIGHTLOG.md",
        "the loop's own diary, and the place findings are recorded before they are documented",
    ),
    (
        "docs/LOOP.md",
        "the owner's standing instructions to the loop, which is neither a specification nor a guide",
    ),
    (
        "docs/requirements/",
        "a named third party's requirements, theirs rather than ours to publish",
    ),
    (
        "docs/research/",
        "condensed from other projects' specifications and manuals; the citations belong in the \
         repository, not on a page that looks like ours",
    ),
    (
        "Master Architecture and Implementation Plan — QUIC-native Messaging Framework.md",
        "the brief this repository is built from: normative for the loop, and a plan rather \
         than documentation of what exists",
    ),
];

/// Assets copied verbatim beside the pages.
const ASSET_DIRS: &[&str] = &["docs/status"];

/// Everything the site is, discovered from the repository.
#[derive(Debug)]
pub struct Site {
    pages: Vec<Page>,
}

/// What one build did, and what it could not resolve.
#[derive(Debug, Default)]
pub struct Report {
    /// Pages written.
    pub pages: usize,
    /// Files copied verbatim.
    pub assets: usize,
    /// Every off-site destination the documents link to, deduplicated — the
    /// list an operator checks before a site goes public.
    pub external: BTreeSet<String>,
    /// Links that resolve to nothing at all. A build with any of these is a
    /// broken document set, not a broken site, so the binary fails and names
    /// them rather than quietly writing a dead link.
    pub dangling: Vec<Dangling>,
}

/// One link that points at nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dangling {
    /// The document that carries the link.
    pub from: String,
    /// The destination exactly as written.
    pub target: String,
}

impl fmt::Display for Dangling {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} -> {}", self.from, self.target)
    }
}

impl Site {
    /// Reads the manifest against `root` and takes each document's title from
    /// its own first heading.
    ///
    /// Fails when a listed document is missing, because a navigation entry
    /// that leads nowhere is exactly what this crate exists to prevent.
    pub fn discover(root: &Path) -> io::Result<Site> {
        let mut pages = Vec::new();
        for (source, group) in manifest(root)? {
            let text = fs::read_to_string(root.join(&source))
                .map_err(|e| io::Error::other(format!("{source}: {e}")))?;
            pages.push(Page {
                url: url_of(&source),
                title: title_of(&text, &source),
                source,
                group,
            });
        }
        Ok(Site { pages })
    }

    /// The published documents, in navigation order.
    pub fn pages(&self) -> &[Page] {
        &self.pages
    }

    /// The page a repository-relative source belongs to, if it is published.
    pub fn page_of(&self, source: &str) -> Option<&Page> {
        self.pages.iter().find(|p| p.source == source)
    }

    /// Writes the whole site into `out`, which is created if it is missing.
    pub fn render(&self, root: &Path, out: &Path) -> io::Result<Report> {
        let mut report = Report::default();
        for page in &self.pages {
            let text = fs::read_to_string(root.join(&page.source))?;
            let rendered = self.page(root, page, &text, &mut report);
            let target = out.join(&page.url);
            if let Some(dir) = target.parent() {
                fs::create_dir_all(dir)?;
            }
            fs::write(&target, rendered)?;
            report.pages += 1;
        }
        fs::write(out.join("site.css"), STYLESHEET)?;
        report.assets += 1;
        for dir in ASSET_DIRS {
            report.assets += copy_dir(&root.join(dir), &out.join(dir))?;
        }
        Ok(report)
    }

    /// Renders one document into a whole HTML page.
    fn page(&self, root: &Path, page: &Page, markdown: &str, report: &mut Report) -> String {
        let (body, headings) = self.body(root, page, markdown, report);
        let depth = page.url.matches('/').count();
        let up = "../".repeat(depth);
        let mut out = String::with_capacity(body.len() + 4096);
        out.push_str("<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
        out.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
        out.push_str("<title>");
        escape_into(&mut out, &page.title);
        out.push_str(" — weida</title>\n<link rel=\"stylesheet\" href=\"");
        out.push_str(&up);
        out.push_str("site.css\">\n</head>\n<body>\n");

        out.push_str("<p class=\"notice\">");
        escape_into(&mut out, NOTICE);
        out.push_str(" <a href=\"");
        out.push_str(REPOSITORY);
        out.push_str("\">Source</a>.</p>\n");

        out.push_str("<div class=\"frame\">\n<nav>\n<a class=\"brand");
        // The brand *is* the landing page's navigation entry. Listing the
        // landing page again below it would print the word twice, which is
        // what the first render of this template did.
        if page.group == Group::Start {
            out.push_str(" here");
        }
        out.push_str("\" href=\"");
        out.push_str(&up);
        out.push_str("index.html\">weida</a>\n");
        for group in Group::ALL {
            if group == Group::Start {
                continue;
            }
            let mut listed = self.pages.iter().filter(|p| p.group == group).peekable();
            if listed.peek().is_none() {
                continue;
            }
            out.push_str("<h2>");
            out.push_str(group.label());
            out.push_str("</h2>\n<ul>\n");
            for entry in listed {
                let here = entry.url == page.url;
                out.push_str(if here {
                    "<li class=\"here\"><a href=\""
                } else {
                    "<li><a href=\""
                });
                out.push_str(&up);
                out.push_str(&entry.url);
                out.push_str("\">");
                escape_into(&mut out, &entry.title);
                out.push_str("</a></li>\n");
            }
            out.push_str("</ul>\n");
        }
        out.push_str("</nav>\n<main>\n");

        if headings.len() > 2 {
            out.push_str("<details class=\"toc\"><summary>On this page</summary>\n<ul>\n");
            for (slug, text) in &headings {
                out.push_str("<li><a href=\"#");
                escape_into(&mut out, slug);
                out.push_str("\">");
                escape_into(&mut out, text);
                out.push_str("</a></li>\n");
            }
            out.push_str("</ul>\n</details>\n");
        }
        out.push_str(&body);
        out.push_str("</main>\n</div>\n<footer><p>");
        escape_into(&mut out, &page.source);
        out.push_str(", rendered by <code>weida-site</code>. <a href=\"");
        out.push_str(BLOB);
        out.push_str(&page.source);
        out.push_str("\">This document in the repository</a>.</p></footer>\n</body>\n</html>\n");
        out
    }

    /// Renders one document's Markdown, rewriting every link it carries and
    /// collecting its second-level headings for the page index.
    fn body(
        &self,
        root: &Path,
        page: &Page,
        markdown: &str,
        report: &mut Report,
    ) -> (String, Vec<(String, String)>) {
        let mut options = Options::empty();
        options.insert(Options::ENABLE_TABLES);
        options.insert(Options::ENABLE_STRIKETHROUGH);
        options.insert(Options::ENABLE_FOOTNOTES);
        options.insert(Options::ENABLE_TASKLISTS);
        let mut events: Vec<Event> = Parser::new_ext(markdown, options).collect();

        let mut headings = Vec::new();
        let mut seen: BTreeMap<String, usize> = BTreeMap::new();
        let mut index = 0;
        while index < events.len() {
            match &events[index] {
                Event::Start(Tag::Heading { level, .. }) => {
                    let level = *level;
                    let text = heading_text(&events[index + 1..]);
                    let slug = unique(slug(&text), &mut seen);
                    if level == HeadingLevel::H2 {
                        headings.push((slug.clone(), text));
                    }
                    if let Event::Start(Tag::Heading { id, .. }) = &mut events[index] {
                        *id = Some(CowStr::from(slug));
                    }
                }
                Event::Start(Tag::Link { dest_url, .. })
                | Event::Start(Tag::Image { dest_url, .. }) => {
                    let rewritten = self.rewrite(root, page, dest_url, report);
                    match &mut events[index] {
                        Event::Start(Tag::Link { dest_url, .. })
                        | Event::Start(Tag::Image { dest_url, .. }) => {
                            *dest_url = CowStr::from(rewritten);
                        }
                        _ => unreachable!("the arm matched one of these two"),
                    }
                }
                _ => {}
            }
            index += 1;
        }

        let mut body = String::with_capacity(markdown.len() * 2);
        html::push_html(&mut body, events.into_iter());
        (body, headings)
    }

    /// Maps one link destination as written in a document onto where it goes
    /// on the site.
    ///
    /// Four answers, in this order: an off-site or in-page link is left
    /// alone; a published document becomes a relative path to its page, so
    /// the output works over `file://` as well as over HTTP; a file that
    /// exists in the repository but is not published becomes a link into the
    /// repository; and anything else is recorded as dangling, which fails the
    /// build.
    fn rewrite(&self, root: &Path, page: &Page, dest: &str, report: &mut Report) -> String {
        if dest.starts_with('#') {
            return dest.to_owned();
        }
        if let Some(scheme) = dest.split_once(':').map(|(s, _)| s)
            && !scheme.contains('/')
            && !scheme.contains('.')
        {
            report.external.insert(dest.to_owned());
            return dest.to_owned();
        }
        let (path, fragment) = match dest.split_once('#') {
            Some((path, fragment)) => (path, Some(fragment)),
            None => (dest, None),
        };
        let decoded = decode(path);
        let Some(target) = resolve(&page.source, &decoded) else {
            report.dangling.push(Dangling {
                from: page.source.clone(),
                target: dest.to_owned(),
            });
            return dest.to_owned();
        };

        let mut out = if let Some(found) = self.page_of(&target) {
            relative(&page.url, &found.url)
        } else if is_asset(&target) && root.join(&target).exists() {
            relative(&page.url, &target)
        } else if root.join(&target).exists() {
            let mut url = String::from(BLOB);
            url.push_str(&encode(&target));
            url
        } else {
            report.dangling.push(Dangling {
                from: page.source.clone(),
                target: dest.to_owned(),
            });
            return dest.to_owned();
        };
        if let Some(fragment) = fragment {
            out.push('#');
            out.push_str(fragment);
        }
        out
    }
}

/// Every published document, in navigation order.
///
/// The three groups that grow — the decision notes, the library documents and
/// the adapter mappings — are read from the directory rather than listed, so
/// a note written tomorrow is on the site without anyone remembering to add
/// it. Everything else is named here, because the order the normative
/// documents are met in is a choice.
fn manifest(root: &Path) -> io::Result<Vec<(String, Group)>> {
    let mut pages = vec![("README.md".to_owned(), Group::Start)];
    for named in [
        "docs/ARCHITECTURE.md",
        "docs/PATTERNS.md",
        "docs/PROTOCOL.md",
        "docs/GUARANTEES.md",
        "docs/FAILURE_MODEL.md",
        "docs/INVARIANTS.md",
        "docs/STORE.md",
        "docs/IMPLEMENTATION.md",
        "docs/STATUS.md",
    ] {
        pages.push((named.to_owned(), Group::Weida));
    }
    for (dir, group) in [
        ("docs/decisions", Group::Decisions),
        ("docs/libraries", Group::Libraries),
        ("docs/adapters", Group::Adapters),
    ] {
        for source in markdown_in(&root.join(dir))? {
            pages.push((format!("{dir}/{source}"), group));
        }
    }
    Ok(pages)
}

/// The Markdown files of one directory, `README.md` first and the rest in
/// name order — which for the decision notes is their number.
fn markdown_in(dir: &Path) -> io::Result<Vec<String>> {
    let mut names: Vec<String> = fs::read_dir(dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".md"))
        .collect();
    names.sort();
    if let Some(at) = names.iter().position(|n| n == "README.md") {
        let readme = names.remove(at);
        names.insert(0, readme);
    }
    Ok(names)
}

/// The output path of a source document.
fn url_of(source: &str) -> String {
    if source == "README.md" {
        return "index.html".to_owned();
    }
    format!("{}.html", source.trim_end_matches(".md"))
}

/// A document's own first heading, or its file name when it has none.
fn title_of(markdown: &str, source: &str) -> String {
    for line in markdown.lines() {
        if let Some(rest) = line.strip_prefix("# ") {
            return rest.trim().to_owned();
        }
    }
    source.rsplit('/').next().unwrap_or(source).to_owned()
}

/// The text of a heading, from the events that follow its start.
fn heading_text(after: &[Event]) -> String {
    let mut text = String::new();
    for event in after {
        match event {
            Event::Text(t) | Event::Code(t) => text.push_str(t),
            Event::End(TagEnd::Heading(_)) => break,
            _ => {}
        }
    }
    text.trim().to_owned()
}

/// The anchor form of a heading: lowercase, one hyphen per run of anything
/// that is not a letter or a digit. The same shape a forge generates, so a
/// link written for the repository's own rendering lands in the same place
/// here.
fn slug(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending = false;
    for c in text.chars() {
        if c.is_alphanumeric() {
            if pending && !out.is_empty() {
                out.push('-');
            }
            pending = false;
            out.extend(c.to_lowercase());
        } else {
            pending = true;
        }
    }
    if out.is_empty() {
        "section".to_owned()
    } else {
        out
    }
}

/// Makes a slug unique within one page, the way a forge does: a second
/// `overview` becomes `overview-1`.
fn unique(slug: String, seen: &mut BTreeMap<String, usize>) -> String {
    let count = seen.entry(slug.clone()).or_insert(0);
    *count += 1;
    if *count == 1 {
        slug
    } else {
        format!("{slug}-{}", *count - 1)
    }
}

/// Resolves `dest`, written in the document at `from`, to a
/// repository-relative path. `None` when it climbs out of the repository.
fn resolve(from: &str, dest: &str) -> Option<String> {
    let mut parts: Vec<&str> = from.split('/').collect();
    parts.pop();
    for segment in dest.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            other => parts.push(other),
        }
    }
    if parts.is_empty() {
        return None;
    }
    Some(parts.join("/"))
}

/// A site-relative path from the page at `from` to the file at `to`.
fn relative(from: &str, to: &str) -> String {
    let depth = from.matches('/').count();
    let mut out = "../".repeat(depth);
    out.push_str(to);
    out
}

/// Is this a file the site copies beside its pages?
fn is_asset(path: &str) -> bool {
    ASSET_DIRS.iter().any(|dir| path.starts_with(dir))
}

/// Percent-decoding, for the one document whose name has spaces in it.
fn decode(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| path.to_owned())
}

/// Percent-encoding of the characters a path may hold and a URL may not.
fn encode(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for c in path.chars() {
        match c {
            ' ' => out.push_str("%20"),
            '"' => out.push_str("%22"),
            '<' => out.push_str("%3C"),
            '>' => out.push_str("%3E"),
            other => out.push(other),
        }
    }
    out
}

/// HTML-escapes into an existing buffer.
fn escape_into(out: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            other => out.push(other),
        }
    }
}

/// Copies one directory's files, returning how many.
fn copy_dir(from: &Path, to: &Path) -> io::Result<usize> {
    fs::create_dir_all(to)?;
    let mut copied = 0;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            fs::copy(entry.path(), to.join(entry.file_name()))?;
            copied += 1;
        }
    }
    Ok(copied)
}

/// Every Markdown file in the repository that the site takes a position on:
/// the root documents and everything under `docs/`.
pub fn documents(root: &Path) -> io::Result<Vec<String>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type()?.is_file() && name.ends_with(".md") {
            out.push(name);
        }
    }
    walk(&root.join("docs"), "docs", &mut out)?;
    out.sort();
    Ok(out)
}

fn walk(dir: &Path, prefix: &str, out: &mut Vec<String>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = format!("{prefix}/{name}");
        if entry.file_type()?.is_dir() {
            walk(&entry.path(), &path, out)?;
        } else if name.ends_with(".md") {
            out.push(path);
        }
    }
    Ok(())
}

/// Is this document deliberately not published, and why?
pub fn excluded(source: &str) -> Option<&'static str> {
    EXCLUDED
        .iter()
        .find(|(pattern, _)| match pattern.strip_suffix('/') {
            Some(dir) => source.starts_with(dir),
            None => source == *pattern,
        })
        .map(|(_, reason)| *reason)
}

/// The repository root, from this crate's manifest directory.
pub fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the crate sits two levels below the repository root")
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_source_path_becomes_the_page_beside_it() {
        assert_eq!(url_of("README.md"), "index.html");
        assert_eq!(url_of("docs/PROTOCOL.md"), "docs/PROTOCOL.html");
        assert_eq!(
            url_of("docs/decisions/0009-drain.md"),
            "docs/decisions/0009-drain.html"
        );
    }

    #[test]
    fn a_link_resolves_against_the_document_that_carries_it() {
        assert_eq!(
            resolve("docs/STATUS.md", "decisions/0009-drain.md").as_deref(),
            Some("docs/decisions/0009-drain.md")
        );
        assert_eq!(
            resolve("docs/decisions/0009-drain.md", "../PATTERNS.md").as_deref(),
            Some("docs/PATTERNS.md")
        );
        assert_eq!(
            resolve("README.md", "docs/libraries/zmq.md").as_deref(),
            Some("docs/libraries/zmq.md")
        );
        // Out of the repository is not a link this site can express, and
        // silently clamping it at the root would invent a target.
        assert_eq!(resolve("README.md", "../../etc/passwd"), None);
    }

    #[test]
    fn a_page_reaches_another_page_by_a_path_that_works_without_a_server() {
        assert_eq!(
            relative("index.html", "docs/PROTOCOL.html"),
            "docs/PROTOCOL.html"
        );
        assert_eq!(relative("docs/STATUS.html", "index.html"), "../index.html");
        assert_eq!(
            relative("docs/decisions/0009-drain.html", "docs/PATTERNS.html"),
            "../../docs/PATTERNS.html"
        );
    }

    #[test]
    fn a_heading_becomes_the_anchor_a_forge_would_have_given_it() {
        assert_eq!(slug("4. Decision"), "4-decision");
        assert_eq!(slug("`Indeterminate` outcomes"), "indeterminate-outcomes");
        assert_eq!(slug("—"), "section");
    }

    #[test]
    fn a_repeated_heading_gets_a_counted_anchor() {
        let mut seen = BTreeMap::new();
        assert_eq!(unique(slug("Overview"), &mut seen), "overview");
        assert_eq!(unique(slug("Overview"), &mut seen), "overview-1");
        assert_eq!(unique(slug("Overview"), &mut seen), "overview-2");
    }

    #[test]
    fn the_one_document_whose_name_has_spaces_survives_the_round_trip() {
        let written = "Master%20Architecture%20and%20Implementation%20Plan.md";
        let decoded = decode(written);
        assert_eq!(decoded, "Master Architecture and Implementation Plan.md");
        assert_eq!(encode(&decoded), written);
    }
}
