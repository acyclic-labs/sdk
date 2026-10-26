//! Agent records over the one Rust session and its bounded record fold.
//!
//! Both native and browser callers supply the platform's existing authenticated
//! Streams pool. This adapter owns no transport task, queue or recovery loop.

use super::{DemandError, DemandPolicy, StreamView, WireSession, WireSessionError};
use crate::agent::{AgentPath, AgentRecord, ConversationEntry, StatusRecord};
use acyclic_stream::{StreamPath, session::Client, wire, wire_codec};
use std::task::{Context, Poll};

/// A provider refusal retains its original machine-readable classification.
#[derive(Debug, thiserror::Error)]
pub enum AgentClientError {
    /// Canonical session admission, transport or authorization failure.
    #[error(transparent)]
    Session(#[from] WireSessionError),
    /// A bounded agent record failed its canonical decoder.
    #[error(transparent)]
    Record(#[from] crate::agent::Error),
    /// A selected stream ended; the client must not invent an idle status.
    #[error("selected agent stream ended: {0}")]
    Closed(StreamPath),
}

/// One observed transition, already applied to the shared bounded Rust fold.
#[derive(Debug, serde::Serialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentEvent {
    /// A settled turn and its immutable output reference.
    Conversation {
        /// Originating selected agent, including inherited conversation records.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Sequence in the selected conversation stream.
        sequence: u64,
        /// Bounded canonical record; output bytes remain in the filesystem.
        entry: ConversationEntry,
    },
    /// A lifecycle transition from the selected agent's actual status path.
    Status {
        /// Originating selected agent; never inferred from a relative suffix.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        agent: AgentPath,
        /// Exact status published by the agent host.
        status: StatusRecord,
    },
    /// Admission or watermark progress, without a fabricated record.
    Progress,
}

/// Explicit initial cursors for one visible or focused agent. Re-selecting an
/// already selected agent preserves its current fold and transport cursor.
#[derive(Clone, Debug)]
pub struct AgentSelection {
    /// Canonical agent identity.
    pub agent: AgentPath,
    /// Exclusive conversation cursor for a newly selected agent.
    pub conversation_from: u64,
    /// Exclusive status cursor for a newly selected agent.
    pub status_from: u64,
}

/// One shared fold for the focused agent and bounded visible agents. Each
/// selected agent costs two logical follows on the caller's existing pool.
pub struct AgentClient {
    session: WireSession,
    agent: AgentPath,
    conversation: StreamPath,
    streams: Vec<(AgentPath, bool, StreamPath)>,
    next_stream: usize,
    credential: u32,
}

impl AgentClient {
    /// Follow from explicit exclusive cursors. Callers choose a bounded history
    /// page or retained cursor; opening never snapshots an entire conversation.
    /// The credential must already be registered on `client`.
    pub fn new(
        client: Client,
        agent: AgentPath,
        conversation_from: u64,
        status_from: u64,
        credential: u32,
    ) -> Result<Self, AgentClientError> {
        let conversation = agent.conversation()?;
        let mut selected = Self {
            session: WireSession::new(client, DemandPolicy::default()),
            agent: agent.clone(), conversation,
            streams: Vec::new(), next_stream: 0, credential,
        };
        selected.set_view(AgentSelection { agent, conversation_from, status_from }, std::iter::empty())?;
        Ok(selected)
    }

    /// Combined focused/visible budget. Each agent owns a conversation and a
    /// status follow, charged to the same finite WireSession stream budget.
    pub fn selected_capacity() -> usize {
        DemandPolicy::default().selected_capacity() / 2
    }

    /// Replace view demand, preserving overlapping folds and releasing removed
    /// agents before new follows open. Input consumption is bounded even for
    /// duplicate or infinite iterators. Refusal leaves the old view intact.
    pub fn set_view(
        &mut self,
        focused: AgentSelection,
        visible: impl IntoIterator<Item = AgentSelection>,
    ) -> Result<(), AgentClientError> {
        let mut selections = Vec::new();
        for (index, selection) in std::iter::once(focused.clone()).chain(visible).enumerate() {
            if index == Self::selected_capacity() {
                return Err(WireSessionError::Demand(DemandError::SelectedCapacity).into());
            }
            if selections.iter().any(|previous: &AgentSelection| previous.agent == selection.agent) {
                continue;
            }
            selections.push(selection);
        }
        let conversation = focused.agent.conversation()?;
        let mut streams = Vec::with_capacity(selections.len() * 2);
        let mut subscriptions = Vec::with_capacity(selections.len() * 2);
        for selection in selections {
            for (is_status, path, from) in [
                (false, selection.agent.conversation()?, selection.conversation_from),
                (true, selection.agent.status()?, selection.status_from),
            ] {
                subscriptions.push((path.clone(), subscription(&path, from, self.credential)));
                streams.push((selection.agent.clone(), is_status, path));
            }
        }
        self.session.replace_streams(subscriptions)?;
        self.agent = focused.agent;
        self.conversation = conversation;
        self.streams = streams;
        self.next_stream %= self.streams.len();
        Ok(())
    }

    /// Borrow just the explicit selected agents, with focus first. No all-agent
    /// membership or status map is retained.
    pub fn selected_agents(&self) -> impl Iterator<Item = &AgentPath> {
        self.streams.iter().step_by(2).map(|(agent, _, _)| agent)
    }

    /// Borrow one selected conversation without copying its retained records.
    pub fn agent_conversation(&self, agent: &AgentPath) -> Option<&StreamView> {
        self.streams.iter().find(|(selected, status, _)| selected == agent && !*status)
            .and_then(|(_, _, path)| self.session.view(path))
    }

    /// Selected agent identity; status paths come from AgentPath, never suffixes.
    pub const fn agent(&self) -> &AgentPath {
        &self.agent
    }

    /// Borrow the exact shared display window, including its eviction edge.
    pub fn conversation(&self) -> &StreamView {
        self.session.view(&self.conversation).expect("selected conversation owns a view")
    }

    /// Decode only the displayed turn requested by the renderer. Reading a
    /// visible row never clones or joins the remainder of the live window.
    pub fn conversation_entry(&self, index: usize) -> Result<Option<AgentEvent>, AgentClientError> {
        self.agent_entry(&self.agent, index)
    }

    /// Decode one visible record for a selected agent, without copying its
    /// complete window or accepting records for an unrequested authority.
    pub fn agent_entry(&self, agent: &AgentPath, index: usize) -> Result<Option<AgentEvent>, AgentClientError> {
        let view = self.agent_conversation(agent).ok_or(WireSessionError::NotRequested)?;
        view.record(index).map(|record| {
            ConversationEntry::decode(&record.value)
                .map(|entry| AgentEvent::Conversation { agent: agent.clone(), sequence: record.sequence, entry })
                .map_err(AgentClientError::Record)
        }).transpose()
    }

    /// Latest actually observed lifecycle state. Before its first delivery the
    /// result is absent; opening a follow never invents an idle/running phase.
    pub fn current_status(&self) -> Result<Option<StatusRecord>, AgentClientError> {
        self.agent_status(&self.agent)
    }

    /// Latest actually delivered status for a selected agent; no separate cache.
    pub fn agent_status(&self, agent: &AgentPath) -> Result<Option<StatusRecord>, AgentClientError> {
        let Some((_, _, path)) = self.streams.iter().find(|(selected, status, _)| selected == agent && *status) else {
            return Err(WireSessionError::NotRequested.into());
        };
        let view = self.session.view(path).expect("selected status owns a view");
        let record = view.records().len().checked_sub(1).and_then(|index| view.record(index));
        record.map(|record| StatusRecord::decode(&record.value).map_err(AgentClientError::Record))
            .transpose()
    }

    /// Borrow the same core for counters and rendering; no second ClientViews.
    pub const fn session(&self) -> &WireSession {
        &self.session
    }

    /// Fairly poll only the bounded selected streams. There is no population scan
    /// and no per-agent queue, task or unbounded status cache.
    pub fn poll_next(&mut self, context: &mut Context<'_>) -> Poll<Result<AgentEvent, AgentClientError>> {
        let mut waiting = false;
        for offset in 0..self.streams.len() {
            let index = (self.next_stream + offset) % self.streams.len();
            let (agent, status, path) = &self.streams[index];
            // WireSession releases terminal follows but retains their final
            // views. Poll only its actual live ownership, so reporting one
            // failed path cannot strand the other selected streams on NotOpen.
            if !self.session.is_following(path) {
                continue;
            }
            let outcome = self.session.poll_stream(path, context);
            if outcome.is_ready() {
                self.next_stream = (index + 1) % self.streams.len();
            }
            let frame = match outcome {
                Poll::Pending => {
                    waiting = true;
                    continue;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
                Poll::Ready(Ok(None)) => return Poll::Ready(Err(AgentClientError::Closed(path.clone()))),
                Poll::Ready(Ok(Some(frame))) => frame,
            };
            let event = match frame.body {
                Some(wire::session_frame::Body::Data(data)) => {
                    let Some(wire::session_data::Item::Record(record)) = data.item else {
                        return Poll::Ready(Err(WireSessionError::InvalidSubscription.into()));
                    };
                    if *status {
                        StatusRecord::decode(&record.value).map(|status| AgentEvent::Status { agent: agent.clone(), status })
                    } else {
                        ConversationEntry::decode(&record.value).map(|entry| AgentEvent::Conversation {
                            agent: agent.clone(),
                            sequence: record.sequence,
                            entry,
                        })
                    }
                }
                Some(wire::session_frame::Body::Closed(_)) => {
                    return Poll::Ready(Err(AgentClientError::Closed(path.clone())));
                }
                _ => Ok(AgentEvent::Progress),
            };
            return Poll::Ready(event.map_err(AgentClientError::Record));
        }
        if waiting {
            Poll::Pending
        } else {
            // No live follow can wake this task. Report exhaustion rather than
            // leaving the next read permanently pending after the last failure.
            Poll::Ready(Err(AgentClientError::Closed(self.conversation.clone())))
        }
    }

    /// Wait without adding a task, channel or independent follow loop.
    pub async fn next(&mut self) -> Result<AgentEvent, AgentClientError> {
        futures::future::poll_fn(|context| self.poll_next(context)).await
    }
}

fn subscription(path: &StreamPath, from: u64, credential: u32) -> wire::SessionSubscription {
    wire::SessionSubscription {
        credential,
        channel: None,
        filter: None,
        resume: Some(wire::SessionCursor { next: from, opaque: bytes::Bytes::new() }),
        kind: Some(wire::session_subscription::Kind::Follow(wire::SessionFollow {
            resource: wire_codec::session::stream_resource(path.as_str()),
            delivery: wire::SessionDelivery::All as i32,
            ..Default::default()
        })),
    }
}
