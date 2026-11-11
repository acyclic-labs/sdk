//! Credited, client-owned workspace queries on the existing actor socket.
//!
//! The v1 open is two bytes (version, projection), followed by the original
//! canonical Diff invocation. Frames start with one byte: 1 = immutable
//! snapshot JSON, 2 = canonical Diff reply, 3 = cumulative summary JSON,
//! 4 = filtered source work, 5 = canonical refusal. Only an ordinary socket
//! end completes the query; a transport failure never completes its totals.

use crate::{AgentCommand, AgentExchangeError, AgentPageRequest, AgentReply, CommandError};
use acyclic_harness_core::{
    agent::AgentPath,
    context::{DiffCursor, DiffFilePage},
    conversation::VolumeRef,
    resources::GenerationRef,
};
use acyclic_stream::{StreamError, session::ActorSocket};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Projection selected before opening the original query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiffProjection {
    /// File headers and bounded patch fragments.
    Patch,
    /// Cumulative counts, without transferring patch text.
    Summary,
}

impl DiffProjection {
    /// Encode the original invocation without introducing another identity or
    /// command decoder. The host admits that same invocation exactly once.
    pub fn open(self, invocation: &[u8]) -> Result<Bytes, CommandError> {
        if invocation.is_empty() || invocation.len() > acyclic_stream::MAX_COMMAND_BYTES - 2 {
            return Err(CommandError::Capacity);
        }
        let mut bytes = Vec::with_capacity(invocation.len() + 2);
        bytes.extend_from_slice(&[1, u8::from(self == Self::Summary)]);
        bytes.extend_from_slice(invocation);
        Ok(bytes.into())
    }

    /// Decode only the product framing; the owning host admits the returned
    /// original invocation through its existing canonical command decoder.
    pub fn decode_open(bytes: &[u8]) -> Result<(Self, &[u8]), CommandError> {
        if bytes.len() < 3 || bytes.len() > acyclic_stream::MAX_COMMAND_BYTES {
            return Err(CommandError::Capacity);
        }
        let projection = match &bytes[..2] {
            [1, 0] => Self::Patch,
            [1, 1] => Self::Summary,
            _ => return Err(CommandError::Invalid),
        };
        Ok((projection, &bytes[2..]))
    }
}

/// Immutable selection for every later step of one live query.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiffSnapshot {
    /// Selected agent, including an independent session root.
    pub agent: AgentPath,
    /// Actual selected project volume.
    pub volume: VolumeRef,
    /// Original fork/checkout creation baseline.
    pub base: GenerationRef,
    /// Current generation captured when this query opened.
    pub current: GenerationRef,
}

/// Totals through the consumed source frontier. They become complete only
/// after normal socket exhaustion; omissions are never counted as equality.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiffSummary {
    /// Compared changed files, including explicit content omissions.
    pub files: u64,
    /// Added text lines from completed comparisons.
    pub added: u64,
    /// Removed text lines from completed comparisons.
    pub removed: u64,
    /// Files whose content could not be compared within the admitted credit.
    pub omitted: u64,
}

/// One credited step. Empty filtered work is distinct from query completion.
#[derive(Debug)]
pub enum DiffQueryEvent {
    /// Initial immutable selection.
    Snapshot(DiffSnapshot),
    /// Visible fragments of one file; their cursors continue that file only.
    Page(Vec<DiffFilePage>),
    /// Counts through the current frontier, without patch bytes.
    Summary(DiffSummary),
    /// One source step disclosed no file.
    Progress,
}

/// One live original socket. Dropping the query releases its source cursor;
/// there is no server-wide query index or replayable continuation token.
pub struct DiffQuery {
    socket: Option<ActorSocket>,
    agent: AgentPath,
    page: AgentPageRequest,
    projection: DiffProjection,
    snapshot: bool,
    cursor: Option<DiffCursor>,
    summary: DiffSummary,
    failed: bool,
}

impl DiffQuery {
    pub(crate) fn new(
        socket: ActorSocket,
        agent: AgentPath,
        page: AgentPageRequest,
        projection: DiffProjection,
    ) -> Self {
        Self {
            socket: Some(socket),
            agent,
            page,
            projection,
            snapshot: false,
            cursor: None,
            summary: DiffSummary::default(),
            failed: false,
        }
    }

    /// Receive one bounded step from this original query. A lost connection,
    /// refusal, malformed frame or incomplete file is an error, never EOF.
    pub async fn next(&mut self) -> Result<Option<DiffQueryEvent>, AgentExchangeError> {
        let result = self.receive().await;
        if result.is_err() {
            self.socket = None;
            self.failed = true;
        }
        result
    }

    async fn receive(&mut self) -> Result<Option<DiffQueryEvent>, AgentExchangeError> {
        if self.failed {
            return Err(CommandError::Invalid.into());
        }
        let Some(socket) = &mut self.socket else {
            return Ok(None);
        };
        let Some(frame) = socket.next().await.map_err(AgentExchangeError::Session)? else {
            if !self.snapshot || self.cursor.is_some() {
                return Err(CommandError::Invalid.into());
            }
            self.socket = None;
            return Ok(None);
        };
        if frame.len() > self.page.bytes() as usize + 1 {
            return Err(CommandError::Capacity.into());
        }
        let Some((&kind, body)) = frame.split_first() else {
            return Err(CommandError::Invalid.into());
        };
        let event = match kind {
            1 if !self.snapshot => {
                let snapshot: DiffSnapshot =
                    serde_json::from_slice(body).map_err(|_| CommandError::Invalid)?;
                if snapshot.agent != self.agent
                    || snapshot.base.as_resource().provider() != snapshot.volume.provider()
                    || snapshot.current.as_resource().provider() != snapshot.volume.provider()
                {
                    return Err(CommandError::Invalid.into());
                }
                snapshot
                    .volume
                    .validate(&mut None)
                    .map_err(|_| CommandError::Invalid)?;
                snapshot
                    .base
                    .validate(&mut None)
                    .map_err(|_| CommandError::Invalid)?;
                snapshot
                    .current
                    .validate(&mut None)
                    .map_err(|_| CommandError::Invalid)?;
                self.snapshot = true;
                DiffQueryEvent::Snapshot(snapshot)
            }
            2 if self.snapshot && self.projection == DiffProjection::Patch => {
                let command = AgentCommand::Diff {
                    agent: self.agent.clone(),
                    page: AgentPageRequest::new(
                        self.page.items(),
                        self.page.bytes(),
                        self.cursor
                            .as_ref()
                            .map(serde_json::to_string)
                            .transpose()
                            .map_err(|_| CommandError::Invalid)?,
                    )?,
                };
                let AgentReply::Diff { items } = crate::decode_agent_reply(body, &command)?? else {
                    return Err(CommandError::Invalid.into());
                };
                if items.is_empty() {
                    return Err(CommandError::Invalid.into());
                }
                let first = &items[0];
                if items
                    .iter()
                    .any(|item| item.path != first.path || item.publication != first.publication)
                    || items
                        .last()
                        .and_then(|item| item.next.as_ref())
                        .is_some_and(|next| next.path != first.path || next.offset.is_none())
                {
                    return Err(CommandError::Invalid.into());
                }
                self.cursor = items.last().and_then(|item| item.next.clone());
                DiffQueryEvent::Page(items)
            }
            3 if self.snapshot && self.projection == DiffProjection::Summary => {
                let summary: DiffSummary =
                    serde_json::from_slice(body).map_err(|_| CommandError::Invalid)?;
                if summary.files < self.summary.files
                    || summary.added < self.summary.added
                    || summary.removed < self.summary.removed
                    || summary.omitted < self.summary.omitted
                    || summary.omitted > summary.files
                {
                    return Err(CommandError::Invalid.into());
                }
                self.summary = summary;
                DiffQueryEvent::Summary(summary)
            }
            4 if self.snapshot && self.cursor.is_none() && body.is_empty() => {
                DiffQueryEvent::Progress
            }
            5 => {
                let command = AgentCommand::Diff {
                    agent: self.agent.clone(),
                    page: self.page.clone(),
                };
                match crate::decode_agent_reply(body, &command)? {
                    Err(failure) => return Err(failure.into()),
                    Ok(_) => return Err(CommandError::Invalid.into()),
                }
            }
            _ => return Err(AgentExchangeError::Session(StreamError::InvalidResponse)),
        };
        Ok(Some(event))
    }
}
