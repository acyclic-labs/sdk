//! Agent tree and history views as pure renders over the [`Reader`].
//!
//! A swarm is a stream subtree and an agent fork is a stream fork, so this
//! layer adds no data API: it lists `harness/agents` and `<agent>/children`
//! with `children_page` (projecting each child's `conversation` head), reads
//! `harness/status/<id>` heads and conversation ranges with `read`, follows
//! only the displayed live tails, and tells the cache that a child's
//! conversation prefix is its parent's (learned from the child's committed
//! `ForkAdopted` record), so opening a forked child reuses the parent's
//! cached history.
//!
//! Per frame the work is bounded by the viewport, overscan and fixed
//! budgets; agents that are not displayed cost nothing.

use crate::reader::{Priority, Reader, ReaderPolicy, Want};
use acyclic_harness_core::agent::{AGENTS, AgentPath, AgentRecord, InboxRecord, StatusRecord};
use acyclic_stream::{StreamError, StreamPath};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Expanded nodes retained; the least recently used collapse beyond this.
const MAX_EXPANDED: usize = 256;
/// Rows of lookahead in the scroll direction, at most.
const MAX_LOOKAHEAD: usize = 256;
/// Agents remembered as recently active, for prefetch.
const MAX_RECENT: usize = 16;
/// Cached sibling pages scanned to relocate a moved row.
const MAX_SCAN_PAGES: u32 = 64;
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
    agent: AgentPath,
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
    /// Expanded agents: last use and the listing page they were seen on.
    expanded: BTreeMap<AgentPath, (u64, u32)>,
    clock: u64,
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
            clock: 0,
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
        self.clock += 1;
        let page = match self.locate(agent) {
            Some(Pos::Row { page, .. }) => page,
            _ => 0,
        };
        self.expanded.insert(agent.clone(), (self.clock, page));
        if self.expanded.len() > MAX_EXPANDED {
            let visible: Vec<&AgentPath> = self
                .rows
                .iter()
                .filter_map(|(_, row)| match row {
                    Row::Agent { path, .. } => Some(path),
                    Row::Loading { .. } => None,
                })
                .collect();
            let anchor = self.anchor_agent();
            let victim = self
                .expanded
                .iter()
                .filter(|(path, _)| {
                    !visible.contains(path)
                        && !anchor
                            .as_ref()
                            .is_some_and(|anchor| anchor.as_str().starts_with(path.as_str()))
                })
                .min_by_key(|(_, (used, _))| *used)
                .map(|(path, _)| path.clone());
            if let Some(victim) = victim {
                self.expanded.remove(&victim);
            }
        }
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
            agent,
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
        let hint = self.expanded.get(agent).map(|(_, page)| *page);
        // Pages are sorted, so each cached page is one binary search.
        for page in hint.into_iter().chain(0..MAX_SCAN_PAGES) {
            let Some(listing) = self.reader.cache().listing(&parent, page) else {
                continue;
            };
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
    /// with a cursor are skipped, within the scan bound.
    fn first_of(&self, parent: StreamPath, page: u32) -> Option<Pos> {
        for page in page..page.saturating_add(MAX_SCAN_PAGES) {
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
        }
        None
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
            for page in 0..MAX_SCAN_PAGES {
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
        self.learn();
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
        if let Some(history) = &self.history
            && history.end
        {
            follows.push(history.stream.clone());
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
                let Some((_, page)) = self.expanded.get(&current) else {
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
        // Learn the fork origin from committed records: the first status
        // record's cursor is the child's `ForkAdopted` inbox position.
        if history.agent.depth() > 0
            && self.reader.cache().fork(&history.stream).is_none()
            && let (Ok(status), Ok(inbox)) = (history.agent.status(), history.agent.inbox())
        {
            let first = self
                .reader
                .cache()
                .record(&status, 0)
                .and_then(|bytes| StatusRecord::decode(bytes).ok());
            match first {
                None => wants.push((
                    Want::Range {
                        stream: status,
                        from: 0,
                        to: 1,
                    },
                    Priority::Visible,
                )),
                Some(first) => wants.push((
                    Want::Range {
                        stream: inbox,
                        from: first.cursor,
                        to: first.cursor + 1,
                    },
                    Priority::Visible,
                )),
            }
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

    /// Learn the open child's fork origin once its records are cached.
    fn learn(&mut self) {
        let Some(history) = &self.history else {
            return;
        };
        if history.agent.depth() == 0 || self.reader.cache().fork(&history.stream).is_some() {
            return;
        }
        let (Ok(status), Ok(inbox)) = (history.agent.status(), history.agent.inbox()) else {
            return;
        };
        let cache = self.reader.cache();
        let Some(first) = cache
            .record(&status, 0)
            .and_then(|bytes| StatusRecord::decode(bytes).ok())
        else {
            return;
        };
        let Some(InboxRecord::ForkAdopted(adopted)) = cache
            .record(&inbox, first.cursor)
            .and_then(|bytes| InboxRecord::decode(bytes).ok())
        else {
            return;
        };
        let Some(source) = adopted.parent.and_then(|parent| parent.conversation().ok()) else {
            return;
        };
        let stream = history.stream.clone();
        self.reader
            .cache_mut()
            .set_fork(&stream, source, adopted.conversation_tail);
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
