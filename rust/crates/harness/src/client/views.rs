//! One disposable fold for overview status and selected full-fidelity files.
#[cfg(all(test, not(acyclic_harness_loom)))]
mod retention;

use super::{ClientInterests, FileViews, InterestTier, QueryViewLimits, SwarmUpdate, query_error};
use crate::{
    Error, Result,
    task_volume::{Cursor, FileKey, FileValue, HeadFile, Query},
};
use acyclic_stream::{StreamPath, StreamWatermarks, wire};
use bytes::Bytes;
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

/// Exact latest values for one agent; the producer owns their schema.
#[derive(Clone, Debug, PartialEq)]
pub struct StatusView {
    after: Option<wire::SessionCursor>,
    watermarks: StreamWatermarks,
    values: BTreeMap<Bytes, StatusValue>,
}

#[derive(Clone, Debug, PartialEq)]
struct StatusValue {
    after: wire::SessionCursor,
    value: wire::SessionLatestValue,
}

impl Default for StatusView {
    fn default() -> Self {
        Self {
            after: None,
            watermarks: StreamWatermarks {
                accepted: 0,
                durable: 0,
            },
            values: BTreeMap::new(),
        }
    }
}

impl StatusView {
    /// Greatest delivered status cursor; platform I/O owns replay and fencing.
    pub fn cursor(&self) -> Option<&wire::SessionCursor> {
        self.after.as_ref()
    }
    /// Accepted and confirmed durable status prefixes.
    pub fn watermarks(&self) -> StreamWatermarks {
        self.watermarks
    }
    /// Latest values in stable key order, with producer-defined payloads.
    pub fn values(&self) -> impl Iterator<Item = &wire::SessionLatestValue> {
        self.values.values().map(|v| &v.value)
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct AgentViews {
    status: StatusView,
    files: Option<FileViews>,
}

/// Renderer hints contain the actual file delta, never another event schema.
pub enum SwarmChange {
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
        data: wire::SessionData,
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
    /// Ordinary filesystem query projections, keyed by volume and selection.
    pub queries: BTreeMap<([u8; 32], [u8; 32]), super::FileQueryView>,
    pub(super) goal_sources: BTreeMap<([u8; 32], [u8; 32]), ([u8; 32], [u8; 32])>,
    layout: ClientInterests,
    query: [u8; 32],
    agents: BTreeMap<StreamPath, AgentViews>,
    limits: ClientViewLimits,
    overview_values: usize,
    overview_bytes: u64,
}

impl ClientViews {
    /// Starts a subscription cache with overview interest for the whole root.
    pub fn new(root: StreamPath, limits: ClientViewLimits) -> Result<Self> {
        Ok(Self {
            queries: BTreeMap::new(),
            goal_sources: BTreeMap::new(),
            layout: ClientInterests::from_layout(root, [], [], 0)?,
            query: crate::task_volume::query_id(&Query::Root).map_err(query_error)?,
            agents: BTreeMap::new(),
            limits,
            overview_values: 0,
            overview_bytes: 0,
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
        self.agents.get(agent).map(|a| &a.status)
    }
    /// Selected content's exact snapshot/follow cut.
    pub fn cursor(&self, agent: &StreamPath) -> Option<&Cursor> {
        self.agents
            .get(agent)?
            .files
            .as_ref()
            .map(FileViews::cursor)
    }
    /// Borrows one logical file without joining or copying its segments.
    pub fn file(&self, agent: &StreamPath, key: &FileKey) -> Option<&HeadFile> {
        self.agents.get(agent)?.files.as_ref()?.file(key)
    }
    /// Visits only the requested agent's selected content.
    pub fn files<'a>(&'a self, agent: &StreamPath) -> impl Iterator<Item = &'a HeadFile> {
        self.agents
            .get(agent)
            .and_then(|a| a.files.as_ref())
            .into_iter()
            .flat_map(FileViews::files)
    }

    fn admit_agent(&self, agent: &StreamPath) -> Result<()> {
        if agent == self.layout.root()
            || self.layout.tier(agent).is_none()
            || (!self.agents.contains_key(agent) && self.agents.len() >= self.limits.agents)
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
            SwarmUpdate::Agent { agent } => {
                self.admit_agent(&agent)?;
                self.agents.entry(agent.clone()).or_default();
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
                    if let Some(agent) = self.agents.get_mut(removed) {
                        agent.files = None;
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
                if self.agents.get(&agent).is_some_and(|a| a.files.is_some()) {
                    return Err(Error::Conflict("content snapshot already installed".into()));
                }
                let files =
                    FileViews::snapshot(snapshot, self.limits.query).map_err(query_error)?;
                let cursor = files.cursor().clone();
                self.agents.entry(agent.clone()).or_default().files = Some(files);
                Ok(SwarmChange::Snapshot { agent, cursor })
            }
            SwarmUpdate::Data {
                agent,
                overview: false,
                data,
            } => {
                let files = self
                    .agents
                    .get_mut(&agent)
                    .and_then(|a| a.files.as_mut())
                    .ok_or_else(|| Error::Conflict("content arrived before its snapshot".into()))?;
                let change = files.session_change(&data).map_err(query_error)?;
                files.prepare(&change).map_err(query_error)?(files).map_err(query_error)?;
                Ok(SwarmChange::Data {
                    agent,
                    overview: false,
                    file: change.file,
                    data,
                })
            }
            SwarmUpdate::Data {
                agent,
                overview: true,
                data,
            } => {
                self.status_data(&agent, &data)?;
                Ok(SwarmChange::Data {
                    agent,
                    overview: true,
                    file: None,
                    data,
                })
            }
            SwarmUpdate::Watermark {
                agent,
                overview: false,
                watermark,
            } => {
                let files = self
                    .agents
                    .get_mut(&agent)
                    .and_then(|a| a.files.as_mut())
                    .ok_or_else(|| {
                        Error::Conflict("content watermark arrived before its snapshot".into())
                    })?;
                files.session_watermark(watermark).map_err(query_error)?;
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
                let current = self
                    .agents
                    .get(&agent)
                    .map(|a| a.status.watermarks)
                    .unwrap_or(StreamWatermarks {
                        accepted: 0,
                        durable: 0,
                    });
                if watermark.durable > watermark.accepted
                    || watermark.accepted < current.accepted
                    || watermark.durable < current.durable
                {
                    return Err(Error::Invalid("status watermark regressed".into()));
                }
                self.agents
                    .entry(agent.clone())
                    .or_default()
                    .status
                    .watermarks = StreamWatermarks {
                    accepted: watermark.accepted,
                    durable: watermark.durable,
                };
                Ok(SwarmChange::Watermark {
                    agent,
                    overview: true,
                    watermark,
                })
            }
        }
    }

    fn status_data(&mut self, agent: &StreamPath, data: &wire::SessionData) -> Result<()> {
        self.admit_agent(agent)?;
        let mut after = data
            .after
            .clone()
            .ok_or_else(|| Error::Invalid("status cursor missing".into()))?;
        if after.next == 0 || data.durable > data.accepted || after.next > data.accepted {
            return Err(Error::Invalid("invalid status watermarks".into()));
        }
        let mut value = match &data.item {
            Some(wire::session_data::Item::LatestValue(value)) => value.clone(),
            _ => return Err(Error::Invalid("status received a non-value frame".into())),
        };
        let previous = self.agents.get(agent).map(|a| &a.status);
        let old = previous.and_then(|s| s.values.get(&value.key));
        if old.is_some_and(|old| {
            old.after.next > after.next
                || (old.after.next == after.next && old.after.opaque != after.opaque)
        }) {
            return Err(Error::Conflict("status key cursor regressed".into()));
        }
        if old.is_some_and(|old| old.after == after && old.value != value) {
            return Err(Error::Conflict(
                "one status cut contains conflicting values".into(),
            ));
        }
        let size = |value: &wire::SessionLatestValue, cursor: &wire::SessionCursor| {
            (value.key.len() as u64)
                .checked_add(value.value.len() as u64)
                .and_then(|size| size.checked_add(cursor.opaque.len() as u64))
        };
        let retained =
            size(&value, &after).ok_or_else(|| Error::Invalid("status bytes overflow".into()))?;
        let bytes = self
            .overview_bytes
            .checked_sub(
                old.and_then(|old| size(&old.value, &old.after))
                    .unwrap_or(0),
            )
            .and_then(|bytes| bytes.checked_add(retained))
            .ok_or_else(|| Error::Invalid("status bytes overflow".into()))?;
        let count = self
            .overview_values
            .checked_add(usize::from(old.is_none()))
            .ok_or_else(|| Error::Invalid("status count overflow".into()))?;
        if bytes > self.limits.overview_bytes || count > self.limits.overview_values {
            return Err(Error::Invalid("status view bound reached".into()));
        }
        // A protobuf Bytes slice may pin a much larger transport allocation.
        // Compact once into one exact buffer, accounting key, body and cursor.
        let length = usize::try_from(retained)
            .map_err(|_| Error::Invalid("status bytes overflow".into()))?;
        let key_end = value.key.len();
        let value_end = key_end + value.value.len();
        let mut compact = Vec::with_capacity(length);
        compact.extend_from_slice(&value.key);
        compact.extend_from_slice(&value.value);
        compact.extend_from_slice(&after.opaque);
        let compact = Bytes::from(compact.into_boxed_slice());
        value.key = compact.slice(..key_end);
        value.value = compact.slice(key_end..value_end);
        after.opaque = compact.slice(value_end..);
        let status = &mut self.agents.entry(agent.clone()).or_default().status;
        if status
            .after
            .as_ref()
            .is_none_or(|old| old.next <= after.next)
        {
            status.after = Some(after.clone());
        }
        status.watermarks.accepted = status.watermarks.accepted.max(data.accepted);
        status.watermarks.durable = status.watermarks.durable.max(data.durable);
        let key = value.key.clone();
        // BTreeMap::insert preserves its existing key. That key may share the
        // old compact payload, so retire it before publishing the replacement.
        status.values.remove(&key);
        status.values.insert(key, StatusValue { after, value });
        self.overview_bytes = bytes;
        self.overview_values = count;
        Ok(())
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
