use super::{Error, Result, UiPath};
use crate::agent::{AgentPath, Phase, StatusRecord};
use acyclic_stream::{StreamPath, demand::Want};
use std::{borrow::Cow, io::Write, ops::Range};

/// The borrowed page data needed by the projection, independent of cache ownership.
#[derive(Clone, Copy)]
pub struct Children<'a> {
    /// One direct children page; never a flattened subtree.
    pub children: &'a [StreamPath],
    /// Whether another provider page exists.
    pub more: bool,
}

/// Immutable provider cut. Implementations borrow cached/pinned values and do
/// no IO; `Computed` can supply a server cut without changing the view.
pub trait Cut {
    /// Agent whose stream and volume facts this cut selects.
    fn agent(&self) -> &AgentPath;
    /// Immutable role selected by this cut; unavailable backing stays pending.
    fn role(&self) -> Result<Cow<'_, str>> {
        Err(Error::Pending)
    }
    /// Latest status at this cut.
    fn status(&self) -> Result<Cow<'_, StatusRecord>>;
    /// Exclusive conversation tail, or unavailable until demanded.
    fn tail(&self) -> Option<u64>;
    /// One conversation entry's rendered text, from pinned file bytes.
    fn turn(&self, sequence: u64) -> Result<Cow<'_, str>>;
    /// Exactly one cached `children_page`, never a flattened subtree.
    fn children(&self, page: u32) -> Result<Option<Children<'_>>>;
    /// One original diagnostic interval from this selected cut.
    fn diagnostic(&self, _: &crate::agent::Diagnostic, _: u64) -> Result<crate::agent::DiagnosticRead> {
        Err(Error::Pending)
    }
}

/// One borrowed directory entry, valid only during its emission callback.
pub struct NodeRef<'a> {
    /// Name within the selected directory.
    pub name: &'a str,
    /// Directory navigation rather than a markdown file.
    pub directory: bool,
    /// Agent navigation target, represented by the backend's namespace link.
    pub agent: Option<&'a StreamPath>,
}

/// One directory entry; a child directory is an explicit navigation link.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    /// Name within the requested directory.
    pub name: String,
    /// Directory rather than markdown file.
    pub directory: bool,
    /// Agent navigation target, when this is a child.
    pub agent: Option<AgentPath>,
}
/// A bounded directory page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page {
    /// Only entries demanded by this call.
    pub nodes: Vec<Node>,
    /// Next cursor; the host retains it unchanged.
    pub next: Option<u64>,
}
/// Follow-to-invalidation mapping, independent of the following transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Affected {
    /// Conversation tail moved.
    Conversation,
    /// Status (including attention) moved.
    Status,
    /// Direct children changed.
    Children,
}

/// Stateless default projection. Page size chunks work; it never limits history
/// or child count. Construction has no per-agent store reads or background work.
#[derive(Clone, Copy)]
pub struct View {
    pub(super) page_size: u32,
}

/// One visible agent row resolved from its UI files.
pub struct AgentRow {
    /// State glyph, including an authored override.
    pub glyph: String,
    /// Authored title or canonical agent identity.
    pub title: String,
    /// Authored model label, or an em dash when unknown.
    pub model: String,
    /// Authored cost with its original currency or unit.
    pub cost: String,
    /// Authored age label, or an em dash when unknown.
    pub age: String,
}
impl AgentRow {
    /// Exact files needed by a visible row; no directory enumeration.
    pub const PATHS: [&str; 5] = ["title", "model", "cost", "age", "glyph"];
}
impl View {
    /// Resolves authored labels over the shared default projection without IO.
    pub fn row<'a>(
        &self,
        cut: &impl Cut,
        file: impl Fn(&str) -> Option<&'a str>,
    ) -> Result<AgentRow> {
        let resolve = |path: &str| {
            let path_ref = UiPath::new(path)?;
            path_ref.resolve(file(path), || self.project(cut, &path_ref))
        };
        Ok(AgentRow {
            glyph: resolve("glyph")?,
            title: resolve("title")?,
            model: resolve("model")?,
            cost: resolve("cost")?,
            age: resolve("age")?,
        })
    }
    /// Select a nonzero chunk size (the reader owns transport page sizing).
    pub fn new(page_size: u32) -> Result<Self> {
        if page_size == 0 {
            return Err(Error::Invalid("zero page size"));
        }
        Ok(Self { page_size })
    }
    /// Stable algorithm/configuration identity for content-addressed projections.
    pub fn identity(&self) -> [u8; 32] {
        let mut identity = [0; 32];
        let domain = b"acyclic:harness:ui:v1";
        identity[..domain.len()].copy_from_slice(domain);
        identity[28..].copy_from_slice(&self.page_size.to_le_bytes());
        identity
    }
    /// Explicit demands only. Callers plan these at the visible priority and
    /// follow the selected status/conversation streams using the same reader.
    /// For directory reads, pass the cursor returned by `nodes` unchanged.
    pub fn wants(
        &self,
        agent: &AgentPath,
        path: &UiPath,
        tail: Option<u64>,
        cursor: u64,
    ) -> Result<Vec<Want>> {
        if path.as_str().is_empty() {
            return Ok(Vec::new());
        }
        let mut wants = vec![Want::Last {
            stream: agent.status()?,
            count: 1,
        }];
        if path.as_str().starts_with("diagnostics/") && path.as_str().ends_with(".md") {
            let (digest, from) = diagnostic_path(path.as_str())?;
            let first = from / acyclic_stream::MAX_RECORD_BYTES as u64;
            wants.push(Want::Range {
                stream: crate::agent::Diagnostic::stream_path(agent, &digest)?,
                from: first,
                to: first.checked_add(2).ok_or(Error::Invalid("diagnostic cursor overflow"))?,
            });
            return Ok(wants);
        }
        match path.as_str() {
            "index.md" => wants.push(Want::Last {
                stream: agent.conversation()?,
                count: self.page_size,
            }),
            "turns" => wants.push(Want::Last {
                stream: agent.conversation()?,
                count: 1,
            }),
            "children" => wants.push(Want::Listing {
                parent: agent.children()?,
                index: u32::try_from(cursor >> 32).map_err(|_| Error::Invalid("child cursor"))?,
                tail: None,
                witness: Some(agent.membership()?),
            }),
            "children/pages" => wants.push(Want::Listing {
                parent: agent.children()?,
                index: u32::try_from(cursor).map_err(|_| Error::Invalid("child cursor"))?,
                tail: None,
                witness: Some(agent.membership()?),
            }),
            path if path.starts_with("children/pages/") => wants.push(Want::Listing {
                parent: agent.children()?,
                index: page_number(path, "children/pages/")?,
                tail: None,
                witness: Some(agent.membership()?),
            }),
            path if path.starts_with("turns/") => {
                let page: u64 = page_number(path, "turns/")?;
                let end = page
                    .checked_add(1)
                    .and_then(|p| p.checked_mul(u64::from(self.page_size)))
                    .ok_or(Error::Invalid("page overflow"))?;
                let from = end - u64::from(self.page_size);
                let tail = tail.ok_or(Error::Pending)?;
                if from >= tail {
                    return Err(Error::Invalid("page beyond conversation"));
                }
                wants.push(Want::Range {
                    stream: agent.conversation()?,
                    from,
                    to: end.min(tail),
                });
            }
            _ => {}
        }
        Ok(wants)
    }
    /// Directory listing at a cut. Children cursors encode (provider page,
    /// offset); requesting the continuation never visits another child subtree.
    pub fn nodes(
        &self,
        cut: &impl Cut,
        dir: &impl AsRef<str>,
        cursor: u64,
        max: u32,
    ) -> Result<Page> {
        let mut nodes = Vec::new();
        let next = self.visit_nodes(cut, dir, cursor, max, |node| {
            nodes.push(Node {
                name: node.name.to_owned(),
                directory: node.directory,
                agent: node
                    .agent
                    .map(|path| AgentPath::try_from(path.as_str().to_owned()))
                    .transpose()?,
            });
            Ok::<_, Error>(())
        })?;
        Ok(Page { nodes, next })
    }
    /// Emit the same directory page into a caller-owned sink. Names are borrowed
    /// for each callback; native callers fund their retained copies directly.
    pub fn visit_nodes<E: From<Error>>(
        &self,
        cut: &impl Cut,
        dir: &impl AsRef<str>,
        cursor: u64,
        max: u32,
        mut emit: impl FnMut(NodeRef<'_>) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<u64>, E> {
        if max == 0 {
            return Err(Error::Invalid("zero page size").into());
        }
        if dir.as_ref() == "diagnostics" || dir.as_ref().starts_with("diagnostics/") {
            let source = diagnostic_source(cut)?;
            let digest = blake3::Hash::from_bytes(source.digest).to_hex();
            let (name, directory) = if dir.as_ref() == "diagnostics" {
                (digest.as_str(), true)
            } else if dir.as_ref().strip_prefix("diagnostics/") == Some(digest.as_str()) {
                ("0.md", false)
            } else {
                return Err(Error::Invalid("unknown diagnostic directory").into());
            };
            if cursor == 0 {
                emit(NodeRef { name, directory, agent: None })?;
            }
            return Ok(None);
        }
        match dir.as_ref() {
            "" => {
                let names = [
                    ("index.md", false),
                    ("status.md", false),
                    ("children", true),
                    ("turns", true),
                    ("diagnostics", true),
                    ("title", false),
                    ("model", false),
                    ("cost", false),
                    ("age", false),
                    ("glyph", false),
                ];
                let mut end = cursor;
                for (name, directory) in names
                    .into_iter()
                    .skip(usize::try_from(cursor).unwrap_or(usize::MAX))
                    .take(max as usize)
                {
                    emit(NodeRef {
                        name,
                        directory,
                        agent: None,
                    })?;
                    end += 1;
                }
                Ok((end < names.len() as u64).then_some(end))
            }
            "children" => {
                let page =
                    u32::try_from(cursor >> 32).map_err(|_| Error::Invalid("child cursor"))?;
                let offset = (cursor & u64::from(u32::MAX)) as usize;
                let listing = cut
                    .children(page)?
                    .ok_or(Error::Invalid("unknown child page"))?;
                let mut end = offset;
                for path in listing.children.iter().skip(offset).take(max as usize) {
                    let name = AgentPath::parse_id(path.as_str()).map_err(Error::from)?;
                    emit(NodeRef {
                        name,
                        directory: true,
                        agent: Some(path),
                    })?;
                    end += 1;
                }
                if !listing.more && end == listing.children.len() && end - offset < max as usize {
                    emit(NodeRef {
                        name: "pages",
                        directory: true,
                        agent: None,
                    })?;
                    end += 1;
                }
                Ok(if end < listing.children.len() {
                    Some((u64::from(page) << 32) | end as u64)
                } else if listing.more {
                    page.checked_add(1).map(|n| u64::from(n) << 32)
                } else if end == listing.children.len() {
                    Some((u64::from(page) << 32) | end as u64)
                } else {
                    None
                })
            }
            "children/pages" => {
                let page = u32::try_from(cursor).map_err(|_| Error::Invalid("child cursor"))?;
                let Some(listing) = cut.children(page)? else {
                    return Ok(None);
                };
                let mut buffer = [0; 23];
                let mut name = std::io::Cursor::new(buffer.as_mut_slice());
                write!(name, "{page}.md").map_err(Error::from)?;
                let length = name.position() as usize;
                let name = std::str::from_utf8(&buffer[..length])
                    .map_err(|_| Error::Invalid("generated page name"))?;
                emit(NodeRef {
                    name,
                    directory: false,
                    agent: None,
                })?;
                Ok(listing.more.then_some(cursor + 1))
            }
            "turns" => {
                let tail = cut.tail().ok_or(Error::Pending)?;
                let pages = tail.div_ceil(u64::from(self.page_size));
                let end = cursor.saturating_add(u64::from(max)).min(pages);
                for n in cursor..end {
                    // All u64 decimal digits plus `.md`, without heap backing.
                    let mut buffer = [0; 23];
                    let mut name = std::io::Cursor::new(buffer.as_mut_slice());
                    write!(name, "{n}.md").map_err(Error::from)?;
                    let length = name.position() as usize;
                    let name = std::str::from_utf8(&buffer[..length])
                        .map_err(|_| Error::Invalid("generated page name"))?;
                    emit(NodeRef {
                        name,
                        directory: false,
                        agent: None,
                    })?;
                }
                Ok((end < pages).then_some(end))
            }
            _ => Err(Error::Invalid("unknown default directory").into()),
        }
    }
    /// Generate only the selected page and return the requested byte range.
    /// Range offsets are bytes (UTF-8 boundaries need not align).
    pub fn bytes(
        &self,
        cut: &impl Cut,
        path: &impl AsRef<str>,
        range: Range<usize>,
    ) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.write_range(cut, path, range, &mut bytes)?;
        Ok(bytes)
    }
    /// Pure markdown generation; authored-over-default resolution wraps this.
    pub fn project(&self, cut: &impl Cut, path: &impl AsRef<str>) -> Result<String> {
        let mut bytes = Vec::new();
        self.write(cut, path, &mut bytes)?;
        String::from_utf8(bytes).map_err(|_| Error::Invalid("non-UTF-8 projection"))
    }
    /// Emit directly into the caller's writer. Native callers can admit each
    /// output growth on their original ledger without an intermediate String.
    pub fn write(
        &self,
        cut: &impl Cut,
        path: &impl AsRef<str>,
        sink: &mut impl Write,
    ) -> Result<()> {
        if path.as_ref().starts_with("diagnostics/") {
            let (digest, from) = diagnostic_path(path.as_ref())?;
            let source = diagnostic_source(cut)?;
            if source.digest != digest {
                return Err(Error::Invalid("diagnostic differs from the selected cut"));
            }
            let page = cut.diagnostic(&source, from)?;
            sink.write_all(b"# Diagnostic\n\n")?;
            for line in page.text.split('\n') {
                writeln!(sink, "    {line}")?;
            }
            if page.end < page.total {
                writeln!(sink, "\n[Next diagnostic page]({}.md)", page.end)?;
            }
            return Ok(());
        }
        match path.as_ref() {
            "role" => Ok(sink.write_all(cut.role()?.as_bytes())?),
            "title" => Ok(sink.write_all(
                cut.agent()
                    .as_str()
                    .rsplit('/')
                    .next()
                    .unwrap_or_default()
                    .as_bytes(),
            )?),
            "model" | "cost" | "age" => Ok(sink.write_all("—".as_bytes())?),
            "glyph" => Ok(sink.write_all(
                match &cut.status()?.phase {
                    Phase::Idle => "○",
                    Phase::Running => "●",
                    Phase::WaitingApproval { .. } | Phase::WaitingUser { .. } => "?",
                    Phase::WaitingHost { .. } | Phase::Waiting { .. } => "◌",
                    Phase::Done {
                        outcome: crate::agent::AgentOutcome::Succeeded { .. },
                    } => "✓",
                    Phase::Done {
                        outcome: crate::agent::AgentOutcome::Failed { .. } | crate::agent::AgentOutcome::FailedDiagnostic { .. },
                    } => "✗",
                    Phase::Done { .. } => "■",
                }
                .as_bytes(),
            )?),
            "status.md" => status(cut, "", sink),
            "index.md" => {
                let tail = cut.tail().ok_or(Error::Pending)?;
                let page = tail.saturating_sub(1) / u64::from(self.page_size);
                self.turns(cut, page, tail, true, sink)
            }
            path if path.starts_with("turns/") => self.turns(
                cut,
                page_number(path, "turns/")?,
                cut.tail().ok_or(Error::Pending)?,
                false,
                sink,
            ),
            "children" => self.child_links(cut, 0, true, sink),
            path if path.starts_with("children/pages/") => {
                self.child_links(cut, page_number(path, "children/pages/")?, false, sink)
            }
            _ => Err(Error::Invalid("unknown default file")),
        }
    }
    /// Emit only the selected bytes, including ranges inside a UTF-8 scalar.
    /// Discarded prefixes and suffixes never allocate output backing.
    pub fn write_range(
        &self,
        cut: &impl Cut,
        path: &impl AsRef<str>,
        range: Range<usize>,
        sink: &mut impl Write,
    ) -> Result<()> {
        self.write(
            cut,
            path,
            &mut ByteRange {
                sink,
                range,
                position: 0,
            },
        )
    }
    fn turns(
        &self,
        cut: &impl Cut,
        page: u64,
        tail: u64,
        index: bool,
        sink: &mut impl Write,
    ) -> Result<()> {
        let start = page
            .checked_mul(u64::from(self.page_size))
            .ok_or(Error::Invalid("page overflow"))?;
        let end = start.saturating_add(u64::from(self.page_size)).min(tail);
        if !index && start >= tail {
            return Err(Error::Invalid("page beyond conversation"));
        }
        sink.write_all(b"# Conversation\n\n")?;
        status(cut, if index { "" } else { "../" }, sink)?;
        sink.write_all(b"\n")?;
        for n in start..end {
            writeln!(sink, "## Turn {}\n\n{}\n", n + 1, cut.turn(n)?)?;
        }
        if page > 0 {
            writeln!(
                sink,
                "[Earlier turns]({}{}.md)",
                if index { "turns/" } else { "" },
                page - 1
            )?;
        }
        Ok(())
    }
    fn child_links(
        &self,
        cut: &impl Cut,
        page: u32,
        index: bool,
        sink: &mut impl Write,
    ) -> Result<()> {
        let listing = cut
            .children(page)?
            .ok_or(Error::Invalid("unknown child page"))?;
        sink.write_all(b"# Children\n\n")?;
        for path in listing.children {
            let id = AgentPath::parse_id(path.as_str())?;
            writeln!(
                sink,
                "- [{}]({}{}/)",
                id,
                if index { "children/" } else { "../" },
                id
            )?;
        }
        if listing.more {
            writeln!(
                sink,
                "\n[More children]({}{}.md)",
                if index { "children/pages/" } else { "" },
                u64::from(page) + 1
            )?;
        }
        Ok(())
    }
    /// Paths invalidated by one followed record, with no filesystem walk.
    pub fn affected(&self, event: Affected) -> &'static [&'static str] {
        match event {
            Affected::Conversation => &["index.md", "turns"],
            Affected::Status => &["index.md", "status.md", "glyph"],
            Affected::Children => &["children"],
        }
    }
}

pub(super) fn page_number<T: std::str::FromStr>(path: &str, prefix: &str) -> Result<T> {
    path.strip_prefix(prefix)
        .and_then(|s| s.strip_suffix(".md"))
        .and_then(|s| s.parse().ok())
        .ok_or(Error::Invalid("page path"))
}
fn status(cut: &impl Cut, root: &str, sink: &mut impl Write) -> Result<()> {
    let status = cut.status()?;
    match &status.phase {
        Phase::WaitingApproval { id, .. } => writeln!(
            sink,
            "Waiting for approval\n\n```input choose id=approval approval={}\n- allow once\n- allow rule\n- deny\n```",
            id.as_str()
        ),
        Phase::WaitingUser { path } => writeln!(sink, "Waiting for your reply: [{path}]({root}{path})"),
        Phase::Idle => sink.write_all(b"Idle\n"),
        Phase::Running => sink.write_all(b"Running\n"),
        Phase::WaitingHost { .. } => sink.write_all(b"Waiting for host\n"),
        Phase::Waiting { input: true, .. } => sink.write_all(b"Waiting for input or alarm\n"),
        Phase::Waiting { .. } => sink.write_all(b"Waiting\n"),
        Phase::Done { outcome: crate::agent::AgentOutcome::FailedDiagnostic { diagnostic } } => writeln!(sink,
            "Failed\n\n[Read complete diagnostic]({root}diagnostics/{}/0.md)",
            blake3::Hash::from_bytes(diagnostic.digest).to_hex()),
        Phase::Done { .. } => sink.write_all(b"Done\n"),
    }?;
    Ok(())
}

fn diagnostic_source(cut: &impl Cut) -> Result<Cow<'_, crate::agent::Diagnostic>> {
    match cut.status()? {
        Cow::Borrowed(status) => match &status.phase {
            Phase::Done { outcome: crate::agent::AgentOutcome::FailedDiagnostic { diagnostic } }
                if diagnostic.agent == *cut.agent() => Ok(Cow::Borrowed(diagnostic)),
            _ => Err(Error::Invalid("selected agent has no published diagnostic")),
        },
        Cow::Owned(status) => match status.phase {
            Phase::Done { outcome: crate::agent::AgentOutcome::FailedDiagnostic { diagnostic } }
                if diagnostic.agent == *cut.agent() => Ok(Cow::Owned(diagnostic)),
            _ => Err(Error::Invalid("selected agent has no published diagnostic")),
        },
    }
}

struct ByteRange<'a, W> {
    sink: &'a mut W,
    range: Range<usize>,
    position: usize,
}
impl<W: Write> Write for ByteRange<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let end = self
            .position
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("projection length overflow"))?;
        let from = self
            .range
            .start
            .saturating_sub(self.position)
            .min(bytes.len());
        let to = self
            .range
            .end
            .saturating_sub(self.position)
            .min(bytes.len())
            .max(from);
        self.sink.write_all(&bytes[from..to])?;
        self.position = end;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.sink.flush()
    }
}

fn diagnostic_path(path: &str) -> Result<([u8; 32], u64)> {
    let rest = path
        .strip_prefix("diagnostics/")
        .ok_or(Error::Invalid("diagnostic path"))?;
    let (digest, page) = rest
        .split_once('/')
        .ok_or(Error::Invalid("diagnostic page"))?;
    let hash = blake3::Hash::from_hex(digest).map_err(|_| Error::Invalid("diagnostic digest"))?;
    if hash.to_hex().as_str() != digest {
        return Err(Error::Invalid("noncanonical diagnostic digest"));
    }
    let page = page
        .strip_suffix(".md")
        .ok_or(Error::Invalid("diagnostic page"))?;
    let from: u64 = page
        .parse()
        .map_err(|_| Error::Invalid("diagnostic cursor"))?;
    if from.to_string() != page {
        return Err(Error::Invalid("noncanonical diagnostic cursor"));
    }
    Ok((*hash.as_bytes(), from))
}
