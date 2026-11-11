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
    /// Latest status at this cut.
    fn status(&self) -> Result<Cow<'_, StatusRecord>>;
    /// Exclusive conversation tail, or unavailable until demanded.
    fn tail(&self) -> Option<u64>;
    /// One conversation entry's rendered text, from pinned file bytes.
    fn turn(&self, sequence: u64) -> Result<Cow<'_, str>>;
    /// Exactly one cached `children_page`, never a flattened subtree.
    fn children(&self, page: u32) -> Option<Children<'_>>;
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
    page_size: u32,
}
impl View {
    /// Select a nonzero chunk size (the reader owns transport page sizing).
    pub fn new(page_size: u32) -> Result<Self> {
        if page_size == 0 {
            return Err(Error::Invalid("zero page size"));
        }
        Ok(Self { page_size })
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
                witness: Some(agent.status()?),
            }),
            path if path.starts_with("children/pages/") => wants.push(Want::Listing {
                parent: agent.children()?,
                index: page_number(path, "children/pages/")?,
                tail: None,
                witness: Some(agent.status()?),
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
    pub fn nodes(&self, cut: &impl Cut, dir: &UiPath, cursor: u64, max: u32) -> Result<Page> {
        if max == 0 {
            return Err(Error::Invalid("zero page size"));
        }
        match dir.as_str() {
            "" => {
                let names = [
                    ("index.md", false),
                    ("status.md", false),
                    ("children", true),
                    ("turns", true),
                ];
                let nodes = names
                    .into_iter()
                    .skip(usize::try_from(cursor).unwrap_or(usize::MAX))
                    .take(max as usize)
                    .map(|(name, directory)| Node {
                        name: name.into(),
                        directory,
                        agent: None,
                    })
                    .collect::<Vec<_>>();
                let end = cursor.saturating_add(nodes.len() as u64);
                Ok(Page {
                    nodes,
                    next: (end < 4).then_some(end),
                })
            }
            "children" => {
                let page =
                    u32::try_from(cursor >> 32).map_err(|_| Error::Invalid("child cursor"))?;
                let offset = (cursor & u64::from(u32::MAX)) as usize;
                let listing = cut.children(page).ok_or(Error::Pending)?;
                let nodes = listing
                    .children
                    .iter()
                    .skip(offset)
                    .take(max as usize)
                    .map(|path| {
                        let agent = AgentPath::try_from(path.as_str().to_owned())?;
                        Ok(Node {
                            name: agent.id().to_string(),
                            directory: true,
                            agent: Some(agent),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let end = offset.saturating_add(nodes.len());
                let next = if end < listing.children.len() {
                    Some((u64::from(page) << 32) | end as u64)
                } else if listing.more {
                    page.checked_add(1).map(|n| u64::from(n) << 32)
                } else {
                    None
                };
                Ok(Page { nodes, next })
            }
            "turns" => {
                let tail = cut.tail().ok_or(Error::Pending)?;
                let pages = tail.div_ceil(u64::from(self.page_size));
                let end = cursor.saturating_add(u64::from(max)).min(pages);
                Ok(Page {
                    nodes: (cursor..end)
                        .map(|n| Node {
                            name: format!("{n}.md"),
                            directory: false,
                            agent: None,
                        })
                        .collect(),
                    next: (end < pages).then_some(end),
                })
            }
            _ => Err(Error::Invalid("unknown default directory")),
        }
    }
    /// Generate only the selected page and return the requested byte range.
    /// Range offsets are bytes (UTF-8 boundaries need not align).
    pub fn bytes(&self, cut: &impl Cut, path: &UiPath, range: Range<usize>) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.write_range(cut, path, range, &mut bytes)?;
        Ok(bytes)
    }
    /// Pure markdown generation; authored-over-default resolution wraps this.
    pub fn project(&self, cut: &impl Cut, path: &UiPath) -> Result<String> {
        let mut bytes = Vec::new();
        self.write(cut, path, &mut bytes)?;
        String::from_utf8(bytes).map_err(|_| Error::Invalid("non-UTF-8 projection"))
    }
    /// Emit directly into the caller's writer. Native callers can admit each
    /// output growth on their original ledger without an intermediate String.
    pub fn write(&self, cut: &impl Cut, path: &UiPath, sink: &mut impl Write) -> Result<()> {
        match path.as_str() {
            "status.md" => status(cut, sink),
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
        path: &UiPath,
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
        sink.write_all(b"# Conversation\n\n")?;
        status(cut, sink)?;
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
        let listing = cut.children(page).ok_or(Error::Pending)?;
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
            Affected::Status => &["index.md", "status.md"],
            Affected::Children => &["children"],
        }
    }
}

fn page_number<T: std::str::FromStr>(path: &str, prefix: &str) -> Result<T> {
    path.strip_prefix(prefix)
        .and_then(|s| s.strip_suffix(".md"))
        .and_then(|s| s.parse().ok())
        .ok_or(Error::Invalid("page path"))
}
fn status(cut: &impl Cut, sink: &mut impl Write) -> Result<()> {
    let status = cut.status()?;
    match &status.phase {
        Phase::WaitingApproval { id, .. } => writeln!(
            sink,
            "Waiting for approval\n\n```input choose id=approval approval={}\n- allow once\n- allow rule\n- deny\n```",
            id.as_str()
        ),
        Phase::WaitingUser { path } => writeln!(sink, "Waiting for your reply: [{path}]({path})"),
        Phase::Idle => sink.write_all(b"Idle\n"),
        Phase::Running => sink.write_all(b"Running\n"),
        Phase::WaitingHost { .. } => sink.write_all(b"Waiting for host\n"),
        Phase::Waiting { input: true, .. } => sink.write_all(b"Waiting for input or alarm\n"),
        Phase::Waiting { .. } => sink.write_all(b"Waiting\n"),
        Phase::Done { .. } => sink.write_all(b"Done\n"),
    }?;
    Ok(())
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
