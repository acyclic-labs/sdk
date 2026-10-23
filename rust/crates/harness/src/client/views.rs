//! One disposable fold for overview status and selected full-fidelity files.
mod status;
pub use status::StatusView;

use super::{
    ClientInterests, FileQueryView, InterestTier, QueryViewLimits, SwarmUpdate, query_error,
};
use crate::{
    Error, Result,
    task_volume::{Cursor, FileKey, FileValue, HeadFile, Query},
};
use acyclic_stream::{StreamPath, wire};
use std::collections::BTreeMap;
use std::{cell::RefCell, rc::Rc};

/// Bounds depend on selected content and retained latest status, not event history.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct ClientViewLimits {
    /// Maximum retained agents, including overview-only rows.
    pub agents: usize,
    /// Maximum agents retaining full file views.
    pub selected: usize,
    /// Maximum latest-value status keys across the entire swarm.
    pub overview_values: usize,
    /// Maximum retained status key, payload and opaque cursor bytes.
    pub overview_bytes: u64,
    /// Per-agent file and segment bounds.
    pub query: QueryViewLimits,
}

/// Renderer hints contain the actual file delta, never another event schema.
pub enum SwarmChange {
    /// Membership, overview, selected content and accounting were retired together.
    Retired {
        /// Former member, whose history remains in the volume.
        agent: StreamPath,
    },
    /// A child is known, independently of its first status record.
    Agent {
        /// Canonical discovered path.
        agent: StreamPath,
    },
    /// Rendering demand changed.
    Layout,
    /// Selected content was installed at one snapshot cut.
    Snapshot {
        /// Selected agent.
        agent: StreamPath,
        /// Cut from which its full-fidelity follow continues.
        cursor: Cursor,
    },
    /// One exact platform status or file delivery was folded.
    Data {
        /// Source agent.
        agent: StreamPath,
        /// Whether this is an always-on status delivery.
        overview: bool,
        /// Decoded file delta when this is selected content.
        file: Option<FileValue>,
        /// Original platform delivery.
        data: Box<wire::SessionData>,
    },
    /// Durability advanced without consuming a file record.
    Watermark {
        /// Source agent.
        agent: StreamPath,
        /// Whether this is the always-on status follow.
        overview: bool,
        /// Exact platform watermarks.
        watermark: wire::SessionWatermark,
    },
}

/// Read-only UI-thread handle, available while the subscription awaits I/O.
/// The callback cannot return a borrow or keep a RefCell guard across an await.
#[derive(Clone)]
pub struct ClientViewHandle(Rc<RefCell<ClientViews>>);

impl ClientViewHandle {
    #[cfg(any(
        all(feature = "grpc", not(target_arch = "wasm32")),
        all(feature = "wasm", target_arch = "wasm32")
    ))]
    pub(crate) fn new(views: ClientViews) -> Self {
        Self(Rc::new(RefCell::new(views)))
    }
    /// Reads the current fold synchronously, without copying its files or text.
    pub fn read<T>(&self, render: impl FnOnce(&ClientViews) -> T) -> Result<T> {
        let views = self
            .0
            .try_borrow()
            .map_err(|error| Error::Conflict(error.to_string()))?;
        Ok(render(&views))
    }
    #[cfg(any(
        all(feature = "grpc", not(target_arch = "wasm32")),
        all(feature = "wasm", target_arch = "wasm32")
    ))]
    pub(crate) fn apply(&self, update: SwarmUpdate) -> Result<SwarmChange> {
        self.0
            .try_borrow_mut()
            .map_err(|error| Error::Conflict(error.to_string()))?
            .apply(update)
    }
}

/// Cache only. Layout, content and status remain authoritative in followed files.
#[derive(Clone, Debug, PartialEq)]
pub struct ClientViews {
    pub(super) goal_sources: BTreeMap<([u8; 32], [u8; 32]), ([u8; 32], [u8; 32])>,
    layout: ClientInterests,
    query: [u8; 32],
    agents: status::Agents,
    limits: ClientViewLimits,
}

impl ClientViews {
    /// Starts a subscription cache with overview interest for the whole root.
    pub fn new(root: StreamPath, limits: ClientViewLimits) -> Result<Self> {
        Ok(Self {
            goal_sources: BTreeMap::new(),
            layout: ClientInterests::from_layout(root, [], [], 0)?,
            query: crate::task_volume::query_id(&Query::Root).map_err(query_error)?,
            agents: status::Agents::default(),
            limits,
        })
    }

    /// Current file-derived rendering demand.
    pub fn layout(&self) -> &ClientInterests {
        &self.layout
    }
    /// Every discovered agent, including children awaiting their first status.
    pub fn agents(&self) -> impl Iterator<Item = &StreamPath> {
        self.agents.keys()
    }
    /// Latest status for one discovered agent, including agents out of view.
    pub fn status(&self, agent: &StreamPath) -> Option<&StatusView> {
        self.agents.get(agent)
    }
    /// Selected content's exact snapshot/follow cut.
    pub fn cursor(&self, agent: &StreamPath) -> Option<&Cursor> {
        self.agents
            .files(agent)?
            .values()
            .find(|file| file.cursor().query == self.query)
            .map(FileQueryView::cursor)
    }
    /// Borrows one logical file without joining or copying its segments.
    pub fn file(&self, agent: &StreamPath, key: &FileKey) -> Option<&HeadFile> {
        self.agents
            .files(agent)?
            .values()
            .find_map(|files| files.head(key))
    }
    /// Visits only the requested agent's selected content.
    pub fn files<'a>(&'a self, agent: &StreamPath) -> impl Iterator<Item = &'a HeadFile> {
        self.agents
            .files(agent)
            .into_iter()
            .flat_map(|files| files.values())
            .flat_map(FileQueryView::heads)
    }

    /// Cached plan rows for the requested agent, from its followed files.
    pub fn plans<'a>(&'a self, agent: &StreamPath) -> impl Iterator<Item = &'a str> {
        self.agents
            .files(agent)
            .into_iter()
            .flat_map(|files| files.values())
            .flat_map(FileQueryView::plan_rows)
    }
    /// Goal projections at the same cuts as the followed files.
    pub fn goals(&self) -> impl Iterator<Item = super::GoalQueryView<'_>> {
        self.selections()
            .filter_map(|(identity, files)| self.goal_selection(identity, files))
    }

    /// All followed file selections, including ordinary private-file queries.
    pub fn selections(&self) -> impl Iterator<Item = (&([u8; 32], [u8; 32]), &FileQueryView)> {
        self.agents
            .files(self.layout.root())
            .into_iter()
            .chain(
                self.layout
                    .full_fidelity()
                    .filter_map(|path| self.agents.files(path)),
            )
            .flat_map(|files| files.iter())
    }
    /// Borrows the agent-owned selection at an exact volume/query identity.
    pub fn selection(&self, identity: &([u8; 32], [u8; 32])) -> Option<&FileQueryView> {
        self.selections()
            .find_map(|(key, files)| (key == identity).then_some(files))
    }
    /// Retires a query's files and derived projections together.
    pub fn remove_selection(&mut self, identity: &([u8; 32], [u8; 32])) {
        if let Some(path) = std::iter::once(self.layout.root())
            .chain(self.layout.full_fidelity())
            .find(|path| {
                self.agents
                    .files(*path)
                    .is_some_and(|files| files.contains_key(identity))
            })
            .cloned()
        {
            if let Some(files) = self.agents.files_mut(&path) {
                files.remove(identity);
            }
            self.goal_sources
                .retain(|goal, request| goal != identity && request != identity);
        }
    }
    pub(super) fn selection_mut(
        &mut self,
        identity: &([u8; 32], [u8; 32]),
    ) -> Option<&mut FileQueryView> {
        let agent = std::iter::once(self.layout.root())
            .chain(self.layout.full_fidelity())
            .find(|path| {
                self.agents
                    .files(*path)
                    .is_some_and(|files| files.contains_key(identity))
            })?
            .clone();
        self.agents.files_mut(&agent)?.get_mut(identity)
    }
    pub(super) fn install_selection(&mut self, files: FileQueryView) {
        let cursor = files.cursor();
        self.agents
            .entry_files(self.layout.root().clone())
            .insert((cursor.volume, cursor.query), files);
    }

    fn admit_agent(&self, agent: &StreamPath) -> Result<()> {
        if agent == self.layout.root()
            || self.layout.tier(agent).is_none()
            || (!self.agents.contains(agent) && self.agents.len() >= self.limits.agents)
        {
            return Err(Error::Invalid(
                "swarm view agent outside its root or bound".into(),
            ));
        }
        Ok(())
    }

    /// Validation precedes mutation. Prefetch promotion preserves every segment
    /// and cursor; demotion releases only content while retaining overview status.
    pub fn apply(&mut self, update: SwarmUpdate) -> Result<SwarmChange> {
        match update {
            SwarmUpdate::Retired { agent } => {
                if &agent == self.layout.root() || self.layout.tier(&agent).is_none() {
                    return Err(Error::Invalid("retirement outside swarm".into()));
                }
                self.agents.retire(&agent);
                Ok(SwarmChange::Retired { agent })
            }
            SwarmUpdate::Agent { agent } => {
                self.admit_agent(&agent)?;
                self.agents.discover(agent.clone());
                Ok(SwarmChange::Agent { agent })
            }
            SwarmUpdate::Layout(layout) => {
                if layout.root() != self.layout.root()
                    || layout.full_fidelity().nth(self.limits.selected).is_some()
                {
                    return Err(Error::Invalid(
                        "swarm view layout outside its root or bound".into(),
                    ));
                }
                for removed in layout.changes(&self.layout)?.unsubscribe() {
                    if let Some(files) = self.agents.files_mut(removed) {
                        files.clear();
                    }
                }
                self.layout = layout;
                Ok(SwarmChange::Layout)
            }
            SwarmUpdate::Snapshot { agent, snapshot } => {
                self.admit_agent(&agent)?;
                if snapshot.cursor.query != self.query {
                    return Err(Error::Invalid(
                        "selected snapshot is not the full agent query".into(),
                    ));
                }
                if self.layout.tier(&agent) == Some(InterestTier::Overview) {
                    return Err(Error::Invalid("unselected agent received content".into()));
                }
                if self
                    .agents
                    .files(&agent)
                    .is_some_and(|files| !files.is_empty())
                {
                    return Err(Error::Conflict("content snapshot already installed".into()));
                }
                let files = FileQueryView::from_heads(snapshot, self.limits.query)?;
                let cursor = files.cursor().clone();
                self.agents
                    .entry_files(agent.clone())
                    .insert((cursor.volume, cursor.query), files);
                Ok(SwarmChange::Snapshot { agent, cursor })
            }
            SwarmUpdate::Data {
                agent,
                overview: false,
                data,
            } => {
                let files = self
                    .agents
                    .files_mut(&agent)
                    .and_then(|files| {
                        files
                            .values_mut()
                            .find(|files| files.cursor().query == self.query)
                    })
                    .ok_or_else(|| Error::Conflict("content arrived before its snapshot".into()))?;
                let change = files.session_change(&data)?;
                Ok(SwarmChange::Data {
                    agent,
                    overview: false,
                    file: change.file,
                    data: Box::new(data),
                })
            }
            SwarmUpdate::Data {
                agent,
                overview: true,
                data,
            } => {
                self.admit_agent(&agent)?;
                self.agents.data(&agent, &data, self.limits)?;
                Ok(SwarmChange::Data {
                    agent,
                    overview: true,
                    file: None,
                    data: Box::new(data),
                })
            }
            SwarmUpdate::Watermark {
                agent,
                overview: false,
                watermark,
            } => {
                let files = self
                    .agents
                    .files_mut(&agent)
                    .and_then(|files| {
                        files
                            .values_mut()
                            .find(|files| files.cursor().query == self.query)
                    })
                    .ok_or_else(|| {
                        Error::Conflict("content watermark arrived before its snapshot".into())
                    })?;
                files.session_watermark(&watermark)?;
                Ok(SwarmChange::Watermark {
                    agent,
                    overview: false,
                    watermark,
                })
            }
            SwarmUpdate::Watermark {
                agent,
                overview: true,
                watermark,
            } => {
                self.admit_agent(&agent)?;
                self.agents.watermark(&agent, &watermark)?;
                Ok(SwarmChange::Watermark {
                    agent,
                    overview: true,
                    watermark,
                })
            }
        }
    }
}

impl serde::Serialize for ClientViews {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut result = serializer.serialize_struct("ClientViews", 2)?;
        result.serialize_field("goals", &super::goal_view::Goals(self))?;
        result.serialize_field("models", &super::model_view::Models(self))?;
        result.end()
    }
}
