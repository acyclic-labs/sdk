use super::{event_id, invalid};
use crate::agent::{Error, InboxRecord, IntentId};
use acyclic_stream::{
    CommitId, FollowFilter, FollowRequest, FollowStream, FollowUpdate, StreamPath, StreamProvider,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Persisted filter projection; execution uses Streams' one filter implementation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorFilter {
    /// Provider-indexed half-open key ranges; empty accepts every key.
    pub keys: Vec<MonitorKeyRange>,
    /// Indexed attribute names and admitted values. All attributes must match.
    pub attributes: std::collections::BTreeMap<String, Vec<Vec<u8>>>,
}

/// One half-open source key range, interpreted by Streams.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorKeyRange {
    /// Inclusive key bytes.
    pub start: Vec<u8>,
    /// Exclusive key bytes; empty is the primitive's unbounded end.
    pub end: Vec<u8>,
}

impl MonitorFilter {
    fn compile(&self) -> Result<FollowFilter, Error> {
        FollowFilter::new(acyclic_stream::wire::SessionFilter {
            keys: self
                .keys
                .iter()
                .map(|range| acyclic_stream::wire::KeyRange {
                    start: range.start.clone().into(),
                    end: range.end.clone().into(),
                })
                .collect(),
            attributes: self
                .attributes
                .iter()
                .map(|(name, values)| acyclic_stream::wire::AttributeMatch {
                    name: name.clone(),
                    any_of: values.iter().cloned().map(Into::into).collect(),
                })
                .collect(),
            ..Default::default()
        })
        .map_err(Error::Stream)
    }
}

/// Replay fence retained with a delivered event, never a sender-side acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MonitorCursor {
    /// Exclusive source sequence.
    pub next: u64,
    /// Commit at `next - 1`; required after any progress.
    pub preceding: Option<[u8; 32]>,
}

/// `monitors/<deadline>-<id>.json`: one authorized follow, with no event-count
/// ceiling. Bounded and unbounded declarations occupy separate indexed folders.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Monitor {
    /// Persisted format; this build accepts only v1.
    pub version: u32,
    /// Definition identity, changed when replacing a monitor.
    pub id: IntentId,
    /// Original authorized stream; authority remains with its provider.
    pub source: String,
    /// Resume fence, committed with publication of the corresponding notice.
    pub cursor: MonitorCursor,
    /// Provider-side indexed predicate.
    pub filter: MonitorFilter,
    /// Whether each match requests a model turn.
    pub wake: bool,
    /// Optional absolute end, multiplexed onto the agent's actor alarm.
    pub deadline_unix_ms: Option<u64>,
    /// Only unsent quiet notices with this key may be coalesced by Notice core.
    pub coalesce_key: Option<String>,
}

/// One atomic source observation and the cursor to persist with its notice.
pub struct MonitorEvent {
    /// New resume fence; filtered progress carries no source value.
    pub cursor: MonitorCursor,
    /// A matching source reference. Progress alone never wakes a model.
    pub notice: Option<InboxRecord>,
}

/// The original follow, held only while this monitor is active.
pub struct MonitorFollow {
    definition: Monitor,
    updates: FollowStream,
}

impl Monitor {
    /// The directory index is also the deadline index. Reading the first
    /// bounded declaration never traverses the unbounded monitors.
    #[must_use]
    pub fn path(&self) -> String {
        let id = blake3::hash(self.id.as_str().as_bytes());
        match self.deadline_unix_ms {
            Some(deadline) => format!("monitors/bounded/{deadline:020}-{}.json", id.to_hex()),
            None => format!("monitors/unbounded/{}.json", id.to_hex()),
        }
    }

    /// Registers a replay-to-live follow through the admitted provider.
    /// Deadline expiry and deletion drop this follow; no timer is created here.
    pub async fn follow<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
    ) -> Result<MonitorFollow, Error> {
        if self.version != 1 || self.source.is_empty() {
            return Err(invalid("invalid monitor v1 definition"));
        }
        if (self.cursor.next == 0) != self.cursor.preceding.is_none() {
            return Err(invalid("monitor cursor requires its preceding commit"));
        }
        let mut request = FollowRequest::new(
            StreamPath::new(&self.source).map_err(Error::Stream)?,
            self.cursor.next,
        );
        request.preceding = self.cursor.preceding.map(CommitId::from_bytes);
        request.filter = self.filter.compile()?;
        request.progress = true;
        let updates = provider.follow(request).await.map_err(Error::Stream)?;
        Ok(MonitorFollow {
            definition: self.clone(),
            updates,
        })
    }
}

impl MonitorFollow {
    /// Waits on the source primitive. A clean source end ends this monitor.
    /// Publish each returned notice and cursor together before requesting more.
    pub async fn next(&mut self) -> Result<Option<MonitorEvent>, Error> {
        let Some(update) = self.updates.next().await else {
            return Ok(None);
        };
        let (next, preceding, notice) = match update.map_err(Error::Stream)? {
            FollowUpdate::Progress { next, preceding } => (next, preceding, None),
            FollowUpdate::Record(record) => {
                let next = record
                    .sequence
                    .checked_add(1)
                    .ok_or(invalid("monitor cursor overflow"))?;
                let id = event_id(
                    "monitor",
                    &[
                        self.definition.id.as_str().as_bytes(),
                        self.definition.source.as_bytes(),
                        &record.sequence.to_le_bytes(),
                        record.commit_id.as_bytes(),
                    ],
                )?;
                let notice = crate::conversation::wire::Notice {
                    source: self.definition.source.clone(),
                    kind: "monitor.match".into(),
                    body: json!({"monitor": self.definition.id, "sequence": record.sequence,
                        "commit": record.commit_id.as_bytes()}),
                    wake: self.definition.wake,
                    coalesce_key: self.definition.coalesce_key.clone(),
                };
                (
                    next,
                    record.commit_id,
                    Some(InboxRecord::Notice { id, notice }),
                )
            }
        };
        if next <= self.definition.cursor.next {
            return Err(invalid("monitor source cursor did not advance"));
        }
        let cursor = MonitorCursor {
            next,
            preceding: Some(*preceding.as_bytes()),
        };
        self.definition.cursor = cursor.clone();
        Ok(Some(MonitorEvent { cursor, notice }))
    }
}
