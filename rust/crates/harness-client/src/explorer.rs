//! Agent tree and history views as pure renders over the [`Reader`].
//!
//! A swarm is a stream subtree and an agent fork is a stream fork, so this
//! layer adds no data API: it lists `harness/agents` and `<agent>/children`
//! with `children_page` (projecting each child's `conversation` head), reads
//! `harness/status/<id>` heads and conversation ranges with `read`, follows
//! only the displayed live tails. The reader learns each stream's exact
//! inherited cut from the primitive Head/origin, so opening a forked child
//! reuses cached history without reconstructing lineage from agent records.
//!
//! Per frame the work is bounded by the viewport, overscan and fixed
//! budgets; agents that are not displayed cost nothing.

use crate::reader::{Priority, Reader, ReaderPolicy, Want};
use acyclic_harness_core::agent::{AGENTS, AgentPath, AgentRecord, StatusRecord};
use acyclic_stream::{StreamError, StreamPath};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Rows of lookahead in the scroll direction, at most.
const MAX_LOOKAHEAD: usize = 256;
/// Agents remembered as recently active, for prefetch.
const MAX_RECENT: usize = 16;
/// Rows in one frame, at most.
const MAX_ROWS: usize = 512;
/// History rows in one window, at most.
const MAX_HISTORY_ROWS: usize = 1024;

/// A tree position: a listed child, or a listing page not loaded yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pos {
    /// `children[offset]` of listing page `page` of `parent`.
    Row {
        /// `children_page` parent.
        parent: StreamPath,
        /// Page index.
        page: u32,
        /// Index in the page.
        offset: u32,
    },
    /// Page `page` of `parent`, not cached: a skeleton row.
    Gap {
        /// `children_page` parent.
        parent: StreamPath,
        /// Page index.
        page: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::{ListedChild, Listing, Op, Outcome};
    use acyclic_harness_core::agent::AgentId;
    use acyclic_stream::{
        AppendRequest, ChildrenPageRequest, IdempotencyKey, MemoryStream, StreamProvider,
    };

    /// Directory data comes from the actual provider. Only the first and
    /// late pages remain cached, as after scrolling through a large tree.
    #[test]
    fn retained_late_pages_relocate_rows_and_resume_large_logical_offsets() {
        futures::executor::block_on(async {
            let provider = MemoryStream::default();
            for id in 1..=130u64 {
                let agent = AgentPath::root(&AgentId::try_from(format!("{id:032x}")).unwrap());
                provider
                    .append(AppendRequest {
                        path: StreamPath::new(agent.as_str()).unwrap(),
                        records: vec![b"directory entry".to_vec().into()],
                        if_tail: None,
                        idempotency_key: IdempotencyKey::new(id.to_be_bytes().to_vec()).unwrap(),
                    })
                    .await
                    .unwrap();
            }
            let mut explorer = Explorer::new(ReaderPolicy {
                listing_limit: 1,
                ..ReaderPolicy::default()
            })
            .unwrap();
            let parent = explorer.root.clone();
            let want = explorer.page(parent.clone(), 0);
            explorer.reader.plan([(want, Priority::Visible)]);
            let [first]: [Op; 1] = explorer.reader.poll(0).try_into().unwrap();
            let first_id = first.id();
            let mut after = None;
            let mut version = None;
            let mut target = None;
            for index in 0..=128 {
                let page = provider
                    .children_page(ChildrenPageRequest {
                        parent: Some(parent.clone()),
                        after: after.clone(),
                        hierarchy_version: version,
                        limit: 1,
                        tail_stream: None,
                    })
                    .await
                    .unwrap();
                version = Some(page.hierarchy_version);
                if index == 0 {
                    explorer.reader.complete(
                        first_id,
                        Outcome::Children {
                            children: page
                                .children
                                .iter()
                                .map(|child| ListedChild {
                                    path: child.path.clone(),
                                    origin: child.origin.clone(),
                                    tail: None,
                                })
                                .collect(),
                            next: page.next_after.clone(),
                            version: page.hierarchy_version,
                        },
                        0,
                    );
                } else if index == 128 {
                    target = Some(
                        AgentPath::try_from(page.children[0].path.as_str().to_owned()).unwrap(),
                    );
                    explorer.reader.cache_mut().insert_listing(
                        &parent,
                        index,
                        Listing {
                            after: after.clone(),
                            children: page.children.into_iter().map(|child| child.path).collect(),
                            next: page.next_after.clone(),
                            fresh: true,
                            witnessed: None,
                        },
                        None,
                    );
                }
                after = page.next_after;
            }
            let target = target.unwrap();
            assert!(matches!(
                explorer.locate(&target),
                Some(Pos::Row { page: 128, .. })
            ));
            explorer.expand(&target);
            assert_eq!(explorer.expanded.get(&target), Some(&128));
            let want = explorer.page(parent, 1_000_000);
            explorer.reader.plan([(want, Priority::Visible)]);
            assert!(matches!(
                explorer.reader.poll(1).as_slice(),
                [Op::Children { index: 129, .. }]
            ));
        });
    }
}

/// One rendered tree row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Row {
    /// A listed agent, rendered from its latest committed records.
    Agent {
        /// Agent address.
        path: AgentPath,
        /// Latest committed status, `None` until read.
        status: Option<StatusRecord>,
        /// Conversation tail, from the listing projection or a later read.
        conversation: Option<u64>,
        /// The latest committed conversation record, projected by the listing.
        latest: Option<Bytes>,
        /// Whether children are shown.
        expanded: bool,
    },
    /// Children not loaded yet; shown as a skeleton, never blank.
    Loading {
        /// Nesting depth of the missing rows.
        depth: usize,
    },
}

/// One rendered history row: a committed position and its record, if cached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryRow {
    /// Conversation position.
    pub sequence: u64,
    /// Committed record, `None` while loading.
    pub value: Option<Bytes>,
}

struct History {
    stream: StreamPath,
    from: u64,
    rows: usize,
    end: bool,
}

/// The tree explorer and the open agent's history, over one reader.
pub struct Explorer {
    reader: Reader,
    root: StreamPath,
    projection: StreamPath,
    anchor: Pos,
    height: usize,
    overscan: usize,
    velocity: i64,
    frame_ms: u64,
    /// Logical expansion intent and the page each agent was seen on. Only
    /// the viewport requests data; cache pressure never collapses intent.
    expanded: BTreeMap<AgentPath, u32>,
    hover: Option<AgentPath>,
    focus: Option<AgentPath>,
    recent: VecDeque<AgentPath>,
    seen: BTreeMap<StreamPath, (AgentPath, u64)>,
    history: Option<History>,
    rows: Vec<(Pos, Row)>,
}

fn stream(text: &str) -> Option<StreamPath> {
    StreamPath::new(text).ok()
}

fn agent_of(child: &StreamPath) -> Option<AgentPath> {
    AgentPath::try_from(child.as_str().to_owned()).ok()
}

/// The listing parent that contains `agent`.
fn parent_listing(agent: &AgentPath) -> Option<StreamPath> {
    match agent.parent() {
        Some(parent) => parent.children().ok(),
        None => stream(AGENTS),
    }
}

/// The agent whose children `parent` lists, `None` for the top level.
fn owner(parent: &StreamPath) -> Option<AgentPath> {
    parent
        .as_str()
        .strip_suffix("/children")
        .and_then(|owner| AgentPath::try_from(owner.to_owned()).ok())
}

impl Explorer {
    /// An explorer at the top of the tree.
    ///
    /// # Errors
    /// Refuses an invalid policy.
    pub fn new(policy: ReaderPolicy) -> Result<Self, StreamError> {
        let root = StreamPath::new(AGENTS)?;
        Ok(Self {
            reader: Reader::new(policy)?,
            anchor: Pos::Gap {
                parent: root.clone(),
                page: 0,
            },
            root,
            projection: StreamPath::new("conversation")?,
            height: 32,
            overscan: 16,
            velocity: 0,
            frame_ms: 16,
            expanded: BTreeMap::new(),
            hover: None,
            focus: None,
            recent: VecDeque::new(),
            seen: BTreeMap::new(),
            history: None,
            rows: Vec::new(),
        })
    }

    /// The reader the platform driver executes.
    pub fn reader(&mut self) -> &mut Reader {
        &mut self.reader
    }

    /// Borrow the reader for diagnostics.
    #[must_use]
    pub fn reader_ref(&self) -> &Reader {
        &self.reader
    }

    /// Viewport rows, overscan rows and the frame interval.
    pub fn set_viewport(&mut self, height: usize, overscan: usize, frame_ms: u64) {
        self.height = height.clamp(1, MAX_ROWS);
        self.overscan = overscan.min(MAX_ROWS);
        self.frame_ms = frame_ms.max(1);
    }

    /// Pointer intent: the hovered agent's children and history are likely next.
    pub fn hover(&mut self, agent: Option<AgentPath>) {
        self.hover = agent;
    }

    /// Keyboard intent, like [`Self::hover`].
    pub fn focus(&mut self, agent: Option<AgentPath>) {
        self.focus = agent;
    }

    /// Scroll by rows; the per-frame velocity steers lookahead.
    pub fn scroll(&mut self, rows: i64) {
        self.velocity = rows;
        let steps = rows.unsigned_abs().min(MAX_ROWS as u64 * 4);
        for _ in 0..steps {
            let next = if rows > 0 {
                self.next(&self.anchor)
            } else {
                self.prev(&self.anchor)
            };
            match next {
                Some(next) => self.anchor = next,
                None => break,
            }
        }
    }

    /// Show an agent's children.
    pub fn expand(&mut self, agent: &AgentPath) {
        let page = match self.locate(agent) {
            Some(Pos::Row { page, .. }) => page,
            _ => 0,
        };
        self.expanded.insert(agent.clone(), page);
    }

    /// Hide an agent's children; the anchor moves out of them if inside.
    pub fn collapse(&mut self, agent: &AgentPath) {
        self.expanded.remove(agent);
        if self
            .anchor_agent()
            .is_some_and(|anchor| anchor.as_str().starts_with(&format!("{agent}/")))
            && let Some(at) = self.locate(agent)
        {
            self.anchor = at;
        }
    }

    /// Open an agent's history at its end.
    pub fn open(&mut self, agent: AgentPath, rows: usize) {
        let Ok(stream) = agent.conversation() else {
            return;
        };
        self.push_recent(&agent);
        self.history = Some(History {
            stream,
            from: 0,
            rows: rows.clamp(1, MAX_HISTORY_ROWS),
            end: true,
        });
    }

    /// Close the history view.
    pub fn close(&mut self) {
        self.history = None;
    }

    /// Seek the history window to a position, O(1).
    pub fn seek(&mut self, sequence: u64) {
        if let Some(history) = &mut self.history {
            history.from = sequence;
            history.end = false;
        }
    }

    /// Jump the history window to the live end, O(1).
    pub fn jump_end(&mut self) {
        if let Some(history) = &mut self.history {
            history.end = true;
        }
    }

    fn push_recent(&mut self, agent: &AgentPath) {
        self.recent.retain(|recent| recent != agent);
        self.recent.push_front(agent.clone());
        self.recent.truncate(MAX_RECENT);
    }

    fn anchor_agent(&self) -> Option<AgentPath> {
        match &self.anchor {
            Pos::Row {
                parent,
                page,
                offset,
            } => self
                .child(parent, *page, *offset)
                .and_then(|c| agent_of(&c)),
            Pos::Gap { parent, .. } => owner(parent),
        }
    }

    fn child(&self, parent: &StreamPath, page: u32, offset: u32) -> Option<StreamPath> {
        self.reader
            .cache()
            .listing(parent, page)
            .and_then(|listing| listing.children.get(offset as usize))
            .cloned()
    }

    /// Where `agent` is listed, searching cached pages of its parent.
    fn locate(&self, agent: &AgentPath) -> Option<Pos> {
        let parent = parent_listing(agent)?;
        let hint = self.expanded.get(agent).copied();
        // The hint is fast when still cached; otherwise search only retained
        // pages, including pages beyond the former fixed scan window.
        let cache = self.reader.cache();
        let hinted =
            hint.and_then(|page| cache.listing(&parent, page).map(|listing| (page, listing)));
        for (page, listing) in hinted.into_iter().chain(cache.listings(&parent)) {
            if let Ok(offset) = listing
                .children
                .binary_search_by(|child| child.as_str().cmp(agent.as_str()))
            {
                return Some(Pos::Row {
                    parent,
                    page,
                    offset: u32::try_from(offset).ok()?,
                });
            }
        }
        None
    }

    /// The first row at or after `page`. Pages may be empty yet continue
    /// (children the credential cannot list are omitted), so empty pages
    /// with a cursor are skipped. A cache miss yields a resumable gap, so
    /// the walk is bounded by retained pages rather than a logical cutoff.
    fn first_of(&self, parent: StreamPath, mut page: u32) -> Option<Pos> {
        loop {
            match self.reader.cache().listing(&parent, page) {
                None => return Some(Pos::Gap { parent, page }),
                Some(listing) if !listing.children.is_empty() => {
                    return Some(Pos::Row {
                        parent,
                        page,
                        offset: 0,
                    });
                }
                Some(listing) if listing.next.is_none() => return None,
                Some(_) => {}
            }
            page = page.checked_add(1)?;
        }
    }

    /// The want for one listing page. An agent's children change only in a
    /// step that also writes its status, so that status witnesses the page.
    fn page(&self, parent: StreamPath, index: u32) -> Want {
        let witness = owner(&parent).and_then(|owner| owner.status().ok());
        Want::Listing {
            parent,
            index,
            tail: Some(self.projection.clone()),
            witness,
        }
    }

    fn next(&self, pos: &Pos) -> Option<Pos> {
        match pos {
            Pos::Row {
                parent,
                page,
                offset,
            } => {
                if let Some(agent) = self
                    .child(parent, *page, *offset)
                    .and_then(|c| agent_of(&c))
                    && self.expanded.contains_key(&agent)
                    && let Some(first) = agent.children().ok().and_then(|c| self.first_of(c, 0))
                {
                    return Some(first);
                }
                self.sibling(parent, *page, *offset)
            }
            Pos::Gap { parent, .. } => self.up(parent),
        }
    }

    fn sibling(&self, parent: &StreamPath, page: u32, offset: u32) -> Option<Pos> {
        let listing = self.reader.cache().listing(parent, page)?;
        if (offset as usize) + 1 < listing.children.len() {
            return Some(Pos::Row {
                parent: parent.clone(),
                page,
                offset: offset + 1,
            });
        }
        if listing.next.is_some() {
            return self
                .first_of(parent.clone(), page + 1)
                .or_else(|| self.up(parent));
        }
        self.up(parent)
    }

    fn up(&self, parent: &StreamPath) -> Option<Pos> {
        let owner = owner(parent)?;
        match self.locate(&owner)? {
            Pos::Row {
                parent,
                page,
                offset,
            } => self.sibling(&parent, page, offset),
            Pos::Gap { .. } => None,
        }
    }

    fn prev(&self, pos: &Pos) -> Option<Pos> {
        let (parent, page, offset) = match pos {
            Pos::Row {
                parent,
                page,
                offset,
            } => (parent, *page, *offset),
            Pos::Gap { parent, page } => (parent, *page, 0),
        };
        if offset > 0 {
            return Some(self.last_descendant(Pos::Row {
                parent: parent.clone(),
                page,
                offset: offset - 1,
            }));
        }
        if page > 0 {
            return Some(match self.reader.cache().listing(parent, page - 1) {
                Some(listing) if !listing.children.is_empty() => self.last_descendant(Pos::Row {
                    parent: parent.clone(),
                    page: page - 1,
                    offset: u32::try_from(listing.children.len() - 1).unwrap_or(0),
                }),
                _ => Pos::Gap {
                    parent: parent.clone(),
                    page: page - 1,
                },
            });
        }
        owner(parent).and_then(|owner| self.locate(&owner))
    }

    /// The last row shown under `pos`, through expanded descendants.
    fn last_descendant(&self, mut pos: Pos) -> Pos {
        for _ in 0..acyclic_harness_core::agent::MAX_DEPTH {
            let Pos::Row {
                parent,
                page,
                offset,
            } = &pos
            else {
                return pos;
            };
            let Some(agent) = self
                .child(parent, *page, *offset)
                .and_then(|c| agent_of(&c))
            else {
                return pos;
            };
            if !self.expanded.contains_key(&agent) {
                return pos;
            }
            let Ok(children) = agent.children() else {
                return pos;
            };
            let mut last = None;
            let mut page = 0;
            loop {
                match self.reader.cache().listing(&children, page) {
                    None => {
                        last = Some(Pos::Gap {
                            parent: children.clone(),
                            page,
                        });
                        break;
                    }
                    Some(listing) if listing.next.is_none() => {
                        last = listing
                            .children
                            .len()
                            .checked_sub(1)
                            .map(|offset| Pos::Row {
                                parent: children.clone(),
                                page,
                                offset: u32::try_from(offset).unwrap_or(0),
                            });
                        break;
                    }
                    Some(_) => {}
                }
                let Some(next) = page.checked_add(1) else {
                    break;
                };
                page = next;
            }
            match last {
                Some(next) => pos = next,
                None => return pos,
            }
        }
        pos
    }

    fn render(&self, pos: &Pos) -> Row {
        match pos {
            Pos::Gap { parent, .. } => Row::Loading {
                depth: owner(parent).map_or(0, |owner| owner.depth() + 1),
            },
            Pos::Row {
                parent,
                page,
                offset,
            } => {
                let Some(path) = self
                    .child(parent, *page, *offset)
                    .and_then(|c| agent_of(&c))
                else {
                    return Row::Loading { depth: 0 };
                };
                let cache = self.reader.cache();
                let status = path.status().ok().and_then(|stream| {
                    let tail = cache.tail(&stream)?.checked_sub(1)?;
                    StatusRecord::decode(cache.record(&stream, tail)?).ok()
                });
                let stream = path.conversation().ok();
                let conversation = stream.as_ref().and_then(|s| cache.tail(s));
                let latest = stream
                    .as_ref()
                    .zip(conversation)
                    .and_then(|(s, tail)| cache.record(s, tail.checked_sub(1)?).cloned());
                Row::Agent {
                    latest,
                    expanded: self.expanded.contains_key(&path),
                    path,
                    status,
                    conversation,
                }
            }
        }
    }

    /// The wants that keep one position renderable.
    fn wants_for(&self, pos: &Pos, priority: Priority, out: &mut Vec<(Want, Priority)>) {
        let (parent, page) = match pos {
            Pos::Row { parent, page, .. } | Pos::Gap { parent, page } => (parent, *page),
        };
        out.push((self.page(parent.clone(), page), priority));
        if let Pos::Row { page, offset, .. } = pos
            && let Some(agent) = self
                .child(parent, *page, *offset)
                .and_then(|c| agent_of(&c))
            && let Ok(status) = agent.status()
        {
            out.push((
                Want::Last {
                    stream: status,
                    count: 1,
                },
                priority,
            ));
        }
    }

    /// An agent about to be opened: the end of its history. When `deep`
    /// (pointer or keyboard intent) it may also be expanded: its first
    /// children page and those children's statuses.
    fn intent_wants(&self, agent: &AgentPath, deep: bool, out: &mut Vec<(Want, Priority)>) {
        if let Ok(conversation) = agent.conversation() {
            out.push((
                Want::Last {
                    stream: conversation,
                    count: self.reader.policy().read_limit,
                },
                Priority::Prefetch,
            ));
        }
        let Some(children) = deep.then(|| agent.children().ok()).flatten() else {
            return;
        };
        out.push((self.page(children.clone(), 0), Priority::Prefetch));
        if let Some(listing) = self.reader.cache().listing(&children, 0) {
            for child in listing.children.iter().take(self.height) {
                if let Some(status) = agent_of(child).and_then(|c| c.status().ok()) {
                    out.push((
                        Want::Last {
                            stream: status,
                            count: 1,
                        },
                        Priority::Prefetch,
                    ));
                }
            }
        }
    }

    /// Lay out one frame and hand the reader its needs. Returns the rows.
    pub fn frame(&mut self, now: u64) -> &[(Pos, Row)] {
        self.reader.begin_frame(now);
        // A loaded gap becomes its first row.
        if let Pos::Gap { parent, page } = &self.anchor
            && let Some(listing) = self.reader.cache().listing(parent, *page)
        {
            self.anchor = if listing.children.is_empty() {
                self.prev(&self.anchor).unwrap_or(self.anchor.clone())
            } else {
                Pos::Row {
                    parent: parent.clone(),
                    page: *page,
                    offset: 0,
                }
            };
        }
        // Rows re-listed out from under the anchor: snap to the first row.
        if let Pos::Row {
            parent,
            page,
            offset,
        } = &self.anchor
            && self.child(parent, *page, *offset).is_none()
        {
            self.anchor = Pos::Gap {
                parent: parent.clone(),
                page: *page,
            };
        }
        let mut wants = Vec::new();
        let mut visible = Vec::with_capacity(self.height);
        let mut pos = Some(self.anchor.clone());
        while let Some(at) = pos {
            if visible.len() >= self.height {
                break;
            }
            pos = self.next(&at);
            visible.push(at);
        }
        for at in &visible {
            self.wants_for(at, Priority::Visible, &mut wants);
        }
        // Ancestors' pages carry traversal across page and level ends.
        let ancestors = self.ancestors(&visible);
        for (parent, page) in &ancestors {
            wants.push((self.page(parent.clone(), *page), Priority::Visible));
        }
        if let Some(history) = &self.history {
            self.history_wants(history, &mut wants);
        }
        // Likely next, most likely first: pointer and keyboard intent, then
        // recently active agents, then lookahead in the scroll direction.
        for agent in [&self.focus, &self.hover].into_iter().flatten() {
            self.intent_wants(agent, true, &mut wants);
        }
        for agent in &self.recent {
            self.intent_wants(agent, false, &mut wants);
        }
        // Lookahead in the scroll direction, overscan behind.
        let rtt_rows = (self.reader.rtt_ms() / self.frame_ms) as usize;
        let ahead = (self.overscan
            + (self.velocity.unsigned_abs() as usize).saturating_mul(rtt_rows.max(1)))
        .min(MAX_LOOKAHEAD);
        let (forward, backward) = if self.velocity < 0 {
            (self.overscan, ahead)
        } else {
            (ahead, self.overscan)
        };
        let mut below = visible.last().cloned();
        for _ in 0..forward {
            below = below.and_then(|at| self.next(&at));
            match &below {
                Some(at) => self.wants_for(at, Priority::Prefetch, &mut wants),
                None => break,
            }
        }
        let mut above = Some(self.anchor.clone());
        for _ in 0..backward {
            above = above.and_then(|at| self.prev(&at));
            match &above {
                Some(at) => self.wants_for(at, Priority::Prefetch, &mut wants),
                None => break,
            }
        }
        // Any visible row may be expanded next: its first children page.
        for at in &visible {
            if let Pos::Row {
                parent,
                page,
                offset,
            } = at
                && let Some(children) = self
                    .child(parent, *page, *offset)
                    .and_then(|c| agent_of(&c))
                    .and_then(|agent| agent.children().ok())
            {
                wants.push((self.page(children, 0), Priority::Prefetch));
            }
        }
        self.velocity /= 2;
        self.reader.plan(wants);
        // Follow only displayed live tails: visible statuses, the displayed
        // pages' witnesses and the open history.
        let mut follows = Vec::new();
        // The open history is followed first, wherever its window is: the
        // follow's `follow_checked` anchor is what proves cached (possibly
        // restored) records were not replaced, so a seek must not skip it,
        // and the follow budget is spent on tree rows only after it.
        if let Some(history) = &self.history {
            follows.push(history.stream.clone());
        }
        let mut seen = BTreeMap::new();
        for at in &visible {
            if let Pos::Row {
                parent,
                page,
                offset,
            } = at
                && let Some(agent) = self
                    .child(parent, *page, *offset)
                    .and_then(|c| agent_of(&c))
                && let Ok(status) = agent.status()
            {
                let tail = self.reader.cache().tail(&status).unwrap_or(0);
                seen.insert(status.clone(), (agent, tail));
                follows.push(status);
            }
        }
        // Each displayed page's witness, so new children appear live.
        let pages = visible
            .iter()
            .map(|at| match at {
                Pos::Row { parent, .. } | Pos::Gap { parent, .. } => parent,
            })
            .chain(ancestors.iter().map(|(parent, _)| parent));
        for parent in pages {
            if let Some(status) = owner(parent).and_then(|owner| owner.status().ok())
                && !follows.contains(&status)
            {
                follows.push(status);
            }
        }
        // An advancing status marks its agent recently active.
        let mut active = Vec::new();
        for (status, (agent, tail)) in &seen {
            if self
                .seen
                .get(status)
                .is_some_and(|(_, before)| tail > before)
            {
                active.push(agent.clone());
            }
        }
        for agent in &active {
            self.push_recent(agent);
        }
        self.seen = seen;
        self.reader.set_follows(follows);
        // Mark what is displayed most recently used.
        for at in &visible {
            let (parent, page) = match at {
                Pos::Row { parent, page, .. } | Pos::Gap { parent, page } => (parent, *page),
            };
            self.reader.cache_mut().touch_listing(parent, page);
        }
        for (parent, page) in &ancestors {
            self.reader.cache_mut().touch_listing(parent, *page);
        }
        self.rows = visible
            .into_iter()
            .map(|at| {
                let row = self.render(&at);
                (at, row)
            })
            .collect();
        for (_, row) in &self.rows {
            let Row::Agent { path, .. } = row else {
                continue;
            };
            for stream in [path.status(), path.conversation()].into_iter().flatten() {
                if let Some(last) = self
                    .reader
                    .cache()
                    .tail(&stream)
                    .and_then(|t| t.checked_sub(1))
                {
                    self.reader.cache_mut().touch_record(&stream, last);
                }
            }
        }
        &self.rows
    }

    /// Listing pages of every expanded ancestor of the visible rows.
    fn ancestors(&self, visible: &[Pos]) -> BTreeSet<(StreamPath, u32)> {
        let mut out = BTreeSet::new();
        let mut owners = BTreeSet::new();
        for at in visible {
            let (Pos::Row { parent, .. } | Pos::Gap { parent, .. }) = at;
            owners.insert(parent.clone());
        }
        for parent in owners {
            let mut agent = owner(&parent);
            while let Some(current) = agent {
                let Some(page) = self.expanded.get(&current) else {
                    break;
                };
                let Some(listing) = parent_listing(&current) else {
                    break;
                };
                if !out.insert((listing, *page)) {
                    break;
                }
                agent = current.parent();
            }
        }
        out
    }

    fn history_wants(&self, history: &History, wants: &mut Vec<(Want, Priority)>) {
        let rows = history.rows as u32;
        if history.end {
            wants.push((
                Want::Last {
                    stream: history.stream.clone(),
                    count: rows,
                },
                Priority::Visible,
            ));
        } else {
            wants.push((
                Want::Range {
                    stream: history.stream.clone(),
                    from: history.from,
                    to: history.from + u64::from(rows),
                },
                Priority::Visible,
            ));
        }
        // The page before the window is the likely next scroll.
        let start = self.history_start(history);
        if let Some(start) = start
            && start > 0
        {
            wants.push((
                Want::Range {
                    stream: history.stream.clone(),
                    from: start.saturating_sub(u64::from(rows)),
                    to: start,
                },
                Priority::Prefetch,
            ));
        }
    }

    fn history_start(&self, history: &History) -> Option<u64> {
        if history.end {
            let tail = self.reader.cache().tail(&history.stream)?;
            Some(tail.saturating_sub(history.rows as u64))
        } else {
            Some(history.from)
        }
    }

    /// The open history window: cached records, or `None` placeholders.
    #[must_use]
    pub fn history(&self) -> Vec<HistoryRow> {
        let Some(history) = &self.history else {
            return Vec::new();
        };
        let Some(start) = self.history_start(history) else {
            return Vec::new();
        };
        let end = match self.reader.cache().tail(&history.stream) {
            Some(tail) => tail.min(start + history.rows as u64),
            None => start + history.rows as u64,
        };
        (start..end)
            .map(|sequence| HistoryRow {
                sequence,
                value: self
                    .reader
                    .cache()
                    .record(&history.stream, sequence)
                    .cloned(),
            })
            .collect()
    }

    /// Rows of the last frame.
    #[must_use]
    pub fn rows(&self) -> &[(Pos, Row)] {
        &self.rows
    }

    /// Parent stream of the top level.
    #[must_use]
    pub fn root(&self) -> &StreamPath {
        &self.root
    }

    /// Expanded nodes retained.
    #[must_use]
    pub fn expanded(&self) -> usize {
        self.expanded.len()
    }
}
