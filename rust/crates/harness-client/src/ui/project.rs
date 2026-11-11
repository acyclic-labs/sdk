use super::{Error, Result, UiPath};
use crate::reader::{Listing, Want};
use crate::{AgentPath, AgentRecord, ConversationEntry, Phase, Reader, StatusRecord};
use acyclic_harness_core::{conversation::FileRef, executor::TurnOutput};
use std::ops::Range;

/// Immutable provider cut. Implementations borrow cached/pinned values and do
/// no IO; `Computed` can supply a server cut without changing the view.
pub trait Cut {
    /// Agent whose stream and volume facts this cut selects.
    fn agent(&self) -> &AgentPath;
    /// Immutable role selected by this cut.
    fn role(&self) -> Result<String>;
    /// Latest status at this cut.
    fn status(&self) -> Result<StatusRecord>;
    /// Exclusive conversation tail, or unavailable until demanded.
    fn tail(&self) -> Option<u64>;
    /// One conversation entry's rendered text, from pinned file bytes.
    fn turn(&self, sequence: u64) -> Result<String>;
    /// Exactly one cached `children_page`, never a flattened subtree.
    fn children(&self, page: u32) -> Option<&Listing>;
}

/// Reader-backed cut plus a lookup of immutable conversation file bytes. The
/// lookup must use the full FileRef, including its generation, and never IO.
pub struct ReaderCut<'a> {
    /// Reader's sole cache, retained by the caller for this render.
    pub reader: &'a Reader,
    /// Selected agent.
    pub agent: &'a AgentPath,
    /// Already resident immutable output text; absence asks the caller to read it.
    pub file: &'a dyn Fn(&FileRef) -> Option<String>,
}

/// A single agent row, projected from its visible UI files. An em dash means
/// the selected cut has no model, price or age fact; these are never guessed.
pub struct AgentRow {
    /// State glyph from the original phase, or an authored replacement.
    pub glyph: String,
    /// Authored title, otherwise the canonical agent identity.
    pub title: String,
    /// Authored model label, when available at the cut.
    pub model: String,
    /// Authored cost with its original currency/unit.
    pub cost: String,
    /// Authored age label.
    pub age: String,
}

impl AgentRow {
    /// Explicit UI paths needed for one visible row. Querying these paths does
    /// not list a directory or evaluate another agent's projection.
    pub const PATHS: [&str; 5] = ["title", "model", "cost", "age", "glyph"];
}
impl Cut for ReaderCut<'_> {
    fn role(&self) -> Result<String> {
        super::agent_role(self.reader, self.agent).ok_or(Error::Pending)
    }
    fn agent(&self) -> &AgentPath {
        self.agent
    }
    fn status(&self) -> Result<StatusRecord> {
        let stream = self.agent.status()?;
        let tail = self.reader.cache().tail(&stream).ok_or(Error::Pending)?;
        let bytes = tail
            .checked_sub(1)
            .and_then(|n| self.reader.cache().record(&stream, n))
            .ok_or(Error::Pending)?;
        Ok(StatusRecord::decode(bytes)?)
    }
    fn tail(&self) -> Option<u64> {
        self.agent
            .conversation()
            .ok()
            .and_then(|p| self.reader.cache().tail(&p))
    }
    fn turn(&self, sequence: u64) -> Result<String> {
        let stream = self.agent.conversation()?;
        let bytes = self
            .reader
            .cache()
            .record(&stream, sequence)
            .ok_or(Error::Pending)?;
        let entry = ConversationEntry::decode(bytes)?;
        let output = (self.file)(&entry.output).ok_or(Error::Pending)?;
        let output: TurnOutput = serde_json::from_str(&output)?;
        Ok(output.text)
    }
    fn children(&self, page: u32) -> Option<&Listing> {
        self.agent
            .children()
            .ok()
            .and_then(|p| self.reader.cache().listing(&p, page))
    }
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
pub struct View {
    page_size: u32,
}
impl View {
    /// Resolve authored UI files over the default projection, with no IO or
    /// per-agent cache. Missing model/cost/age data remains explicitly unknown.
    pub fn row<'a>(
        &self,
        cut: &impl Cut,
        file: impl Fn(&str) -> Option<&'a str>,
    ) -> Result<AgentRow> {
        let resolve = |path: &str| -> Result<String> {
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
                    ("title", false),
                    ("model", false),
                    ("cost", false),
                    ("age", false),
                    ("glyph", false),
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
                    next: (end < names.len() as u64).then_some(end),
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
                } else if listing.next.is_some() {
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
        let body = self.project(cut, path)?;
        let from = range.start.min(body.len());
        let to = range.end.min(body.len()).max(from);
        Ok(body
            .as_bytes()
            .get(from..to)
            .ok_or(Error::Invalid("byte range"))?
            .to_vec())
    }
    /// Pure markdown generation; authored-over-default resolution wraps this.
    pub fn project(&self, cut: &impl Cut, path: &UiPath) -> Result<String> {
        match path.as_str() {
            "role" => cut.role(),
            "title" => Ok(cut.agent().id().to_string()),
            "model" | "cost" | "age" => Ok("—".into()),
            "glyph" => Ok(match cut.status()?.phase {
                Phase::Idle => "○",
                Phase::Running => "●",
                Phase::WaitingApproval { .. } | Phase::WaitingUser { .. } => "?",
                Phase::WaitingHost { .. } | Phase::Waiting { .. } => "◌",
                Phase::Done {
                    outcome: acyclic_harness_core::agent::AgentOutcome::Succeeded { .. },
                } => "✓",
                Phase::Done {
                    outcome:
                        acyclic_harness_core::agent::AgentOutcome::Failed { .. }
                        | acyclic_harness_core::agent::AgentOutcome::FailedDiagnostic { .. },
                } => "✗",
                Phase::Done { .. } => "■",
            }
            .into()),
            "status.md" => status(cut),
            "index.md" => {
                let tail = cut.tail().ok_or(Error::Pending)?;
                let page = tail.saturating_sub(1) / u64::from(self.page_size);
                self.turns(cut, page, tail, true)
            }
            path if path.starts_with("turns/") => self.turns(
                cut,
                page_number(path, "turns/")?,
                cut.tail().ok_or(Error::Pending)?,
                false,
            ),
            "children" => self.child_links(cut, 0, true),
            path if path.starts_with("children/pages/") => {
                self.child_links(cut, page_number(path, "children/pages/")?, false)
            }
            _ => Err(Error::Invalid("unknown default file")),
        }
    }
    fn turns(&self, cut: &impl Cut, page: u64, tail: u64, index: bool) -> Result<String> {
        let start = page
            .checked_mul(u64::from(self.page_size))
            .ok_or(Error::Invalid("page overflow"))?;
        let end = start.saturating_add(u64::from(self.page_size)).min(tail);
        let mut text = format!("# Conversation\n\n{}\n", status(cut)?);
        for n in start..end {
            text.push_str(&format!("## Turn {}\n\n{}\n\n", n + 1, cut.turn(n)?));
        }
        if page > 0 {
            text.push_str(&format!(
                "[Earlier turns]({}{}.md)\n",
                if index { "turns/" } else { "" },
                page - 1
            ));
        }
        Ok(text)
    }
    fn child_links(&self, cut: &impl Cut, page: u32, index: bool) -> Result<String> {
        let listing = cut.children(page).ok_or(Error::Pending)?;
        let mut text = "# Children\n\n".to_owned();
        for path in &listing.children {
            let child = AgentPath::try_from(path.as_str().to_owned())?;
            text.push_str(&format!(
                "- [{}]({}{}/)\n",
                child.id(),
                if index { "children/" } else { "../" },
                child.id()
            ));
        }
        if listing.next.is_some() {
            text.push_str(&format!(
                "\n[More children]({}{}.md)\n",
                if index { "children/pages/" } else { "" },
                u64::from(page) + 1
            ));
        }
        Ok(text)
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

fn page_number<T: std::str::FromStr>(path: &str, prefix: &str) -> Result<T> {
    path.strip_prefix(prefix)
        .and_then(|s| s.strip_suffix(".md"))
        .and_then(|s| s.parse().ok())
        .ok_or(Error::Invalid("page path"))
}
fn status(cut: &impl Cut) -> Result<String> {
    let status = cut.status()?;
    Ok(match status.phase {
        Phase::WaitingApproval { id, .. } => format!(
            "Waiting for approval\n\n```input choose id=approval approval={}\n- allow once\n- allow rule\n- deny\n```\n",
            id.as_str()
        ),
        Phase::WaitingUser { path } => format!("Waiting for your reply: [{path}]({path})\n"),
        Phase::Idle => "Idle\n".into(),
        Phase::Running => "Running\n".into(),
        Phase::WaitingHost { .. } => "Waiting for host\n".into(),
        Phase::Waiting { input: true, .. } => "Waiting for input or alarm\n".into(),
        Phase::Waiting { .. } => "Waiting\n".into(),
        Phase::Done { .. } => "Done\n".into(),
    })
}
