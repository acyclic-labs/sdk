//! Agent records over the one Rust session and its bounded record fold.
//!
//! Both native and browser callers supply the platform's existing authenticated
//! Streams pool and shared readiness owner, without a transport task or recovery loop.

use crate::{DemandPolicy, StreamView, WireSession, WireSessionError};
use acyclic_harness_core::agent::{AgentPath, AgentRecord, ConversationEntry, StatusRecord};
use acyclic_stream::{StreamPath, session::Client, wire, wire_codec};
use std::{
    collections::{BTreeMap, BTreeSet},
    task::{Context, Poll},
};

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "agents_tests.rs"]
mod tests;

/// A provider refusal retains its original machine-readable classification.
#[derive(Debug, thiserror::Error)]
pub enum AgentClientError {
    /// Canonical session admission, transport or authorization failure.
    #[error(transparent)]
    Session(#[from] WireSessionError),
    /// A bounded agent record failed its canonical decoder.
    #[error(transparent)]
    Record(#[from] acyclic_harness_core::agent::Error),
    /// A selected stream ended; the client must not invent an idle status.
    #[error("selected agent stream ended: {0}")]
    Closed(StreamPath),
    /// A selected filesystem/tool-output stream ended, without ending others.
    #[error("selected resource stream ended: {0}")]
    ResourceClosed(String),
    /// A terminal provider refusal; retained separately from stream absence.
    #[error("follow refused for {resource}: {code}: {detail}")]
    Refused {
        /// Canonical selected resource whose producer refused the follow.
        resource: String,
        /// Provider's stable refusal classification.
        code: String,
        /// Provider's original diagnostic.
        detail: String,
    },
}

impl AgentClientError {
    /// Preserve the canonical cause classification across platform adapters.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Session(cause) => cause.code(),
            Self::Record(cause) => cause.code(),
            Self::Closed(_) => "agent_stream_closed",
            Self::ResourceClosed(_) => "agent_resource_closed",
            Self::Refused { .. } => "agent_follow_refused",
        }
    }
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
    /// Tool-output/filesystem progress folded into the same bounded owner.
    /// Renderers borrow only displayed records via `resource_view`.
    Resource {
        /// Exact selected canonical filesystem resource.
        resource: String,
        /// Revision of its retained display window, with no payload copy.
        revision: u64,
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

/// One shared fold for the focused agent and explicit visible agents. Each
/// selected agent costs two logical follows on the caller's existing pool.
pub struct AgentClient {
    session: WireSession,
    agent: AgentPath,
    conversation: StreamPath,
    selected: Vec<AgentPath>,
    streams: BTreeMap<StreamPath, (AgentPath, bool)>,
    credential: u32,
    all: bool,
}

/// One canonical session delivery, before a frontend chooses its projection.
pub struct AgentDelivery {
    /// Source resource; record identity is this resource plus its sequence.
    pub resource: String,
    /// The unchanged provider frame, including its exclusive replay cursor.
    pub frame: wire::SessionFrame,
}

impl AgentClient {
    /// Bind controls to this observer's exact authenticated pool and slot.
    /// The host supplies canonical routing; no second wire owner is opened.
    pub fn controls(&self, binding: crate::AgentBinding) -> crate::AgentControls {
        crate::AgentControls::new(self.session.client().clone(), self.credential, binding)
    }

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
        Self::observing(
            client,
            agent,
            conversation_from,
            status_from,
            credential,
            false,
        )
    }

    /// Follow every committed status on the same bounded session owner.
    pub fn all(
        client: Client,
        agent: AgentPath,
        conversation_from: u64,
        status_from: u64,
        credential: u32,
    ) -> Result<Self, AgentClientError> {
        Self::observing(
            client,
            agent,
            conversation_from,
            status_from,
            credential,
            true,
        )
    }

    fn observing(
        client: Client,
        agent: AgentPath,
        conversation_from: u64,
        status_from: u64,
        credential: u32,
        all: bool,
    ) -> Result<Self, AgentClientError> {
        let conversation = agent.conversation()?;
        let mut selected = Self {
            session: WireSession::new(client, DemandPolicy::default()),
            agent: agent.clone(),
            conversation,
            selected: Vec::new(),
            streams: BTreeMap::new(),
            credential,
            all,
        };
        selected.set_view(
            AgentSelection {
                agent,
                conversation_from,
                status_from,
            },
            std::iter::empty(),
        )?;
        Ok(selected)
    }

    /// Replace view demand, preserving overlapping folds and releasing removed
    /// agents before new follows open. Refusal leaves the old view intact.
    pub fn set_view(
        &mut self,
        focused: AgentSelection,
        visible: impl IntoIterator<Item = AgentSelection>,
    ) -> Result<(), AgentClientError> {
        self.replace_view(focused, visible, None)
    }

    /// Replace all on-screen agents and tool-output panes in one transaction.
    /// Scroll-out releases subscriptions and cached records; overlapping panes
    /// retain their actual cursors. No off-screen agent state is retained.
    pub fn set_viewport(
        &mut self,
        focused: AgentSelection,
        visible: impl IntoIterator<Item = AgentSelection>,
        resources: impl IntoIterator<Item = wire::SessionSubscription>,
    ) -> Result<(), AgentClientError> {
        let mut requested = Vec::new();
        for subscription in resources {
            if subscription.credential != self.credential {
                return Err(WireSessionError::InvalidSubscription.into());
            }
            requested.push(subscription);
        }
        self.replace_view(focused, visible, Some(requested))
    }

    fn replace_view(
        &mut self,
        focused: AgentSelection,
        visible: impl IntoIterator<Item = AgentSelection>,
        resources: Option<Vec<wire::SessionSubscription>>,
    ) -> Result<(), AgentClientError> {
        let mut selections = Vec::new();
        let mut selected = BTreeSet::new();
        for selection in std::iter::once(focused.clone()).chain(visible) {
            if !selected.insert(selection.agent.clone()) {
                continue;
            }
            selections.push(selection);
        }
        let conversation = focused.agent.conversation()?;
        let mut streams = BTreeMap::new();
        let mut ordered = Vec::with_capacity(selections.len());
        let mut subscriptions = Vec::with_capacity(selections.len() * 2);
        for selection in selections {
            for (is_status, path, from) in [
                (
                    false,
                    selection.agent.conversation()?,
                    selection.conversation_from,
                ),
                (true, selection.agent.status()?, selection.status_from),
            ] {
                let mut request = subscription(&path, from, self.credential, is_status && !self.all);
                if let Some(wire::session_subscription::Kind::Follow(follow)) = &mut request.kind {
                    follow.opening_cut = self.all;
                }
                subscriptions.push((path.clone(), request));
                streams.insert(path, (selection.agent.clone(), is_status));
            }
            ordered.push(selection.agent);
        }
        if let Some(mut resources) = resources {
            for request in &mut resources {
                if let Some(wire::session_subscription::Kind::Follow(follow)) = &mut request.kind {
                    follow.opening_cut = self.all;
                }
            }
            self.session.replace_interests(subscriptions, resources)?;
        } else {
            self.session.replace_streams(subscriptions)?;
        }
        self.agent = focused.agent;
        self.conversation = conversation;
        self.streams = streams;
        self.selected = ordered;
        Ok(())
    }

    /// Read-only production residency, independent of total tree cardinality.
    pub fn residency(&self) -> (usize, usize, usize) {
        (
            self.session.followed_interests(),
            self.session.retained_views(),
            self.session.retained_bytes(),
        )
    }

    /// Borrow just the explicit selected agents, with focus first. No all-agent
    /// membership or status map is retained.
    pub fn selected_agents(&self) -> impl Iterator<Item = &AgentPath> {
        self.selected.iter()
    }

    /// Borrow one selected conversation without copying its retained records.
    pub fn agent_conversation(&self, agent: &AgentPath) -> Option<&StreamView> {
        self.session.view(&agent.conversation().ok()?)
    }

    /// Selected agent identity; status paths come from AgentPath, never suffixes.
    pub const fn agent(&self) -> &AgentPath {
        &self.agent
    }

    /// Borrow the exact shared display window, including its eviction edge.
    pub fn conversation(&self) -> &StreamView {
        self.session
            .view(&self.conversation)
            .expect("selected conversation owns a view")
    }

    /// Decode only the displayed turn requested by the renderer. Reading a
    /// visible row never clones or joins the remainder of the live window.
    pub fn conversation_entry(&self, index: usize) -> Result<Option<AgentEvent>, AgentClientError> {
        self.agent_entry(&self.agent, index)
    }

    /// Decode one visible record for a selected agent, without copying its
    /// complete window or accepting records for an unrequested authority.
    pub fn agent_entry(
        &self,
        agent: &AgentPath,
        index: usize,
    ) -> Result<Option<AgentEvent>, AgentClientError> {
        let view = self
            .agent_conversation(agent)
            .ok_or(WireSessionError::NotRequested)?;
        view.record(index)
            .map(|record| {
                ConversationEntry::decode(&record.value)
                    .map(|entry| AgentEvent::Conversation {
                        agent: agent.clone(),
                        sequence: record.sequence,
                        entry,
                    })
                    .map_err(AgentClientError::Record)
            })
            .transpose()
    }

    /// Latest actually observed lifecycle state. Before its first delivery the
    /// result is absent; opening a follow never invents an idle/running phase.
    pub fn current_status(&self) -> Result<Option<StatusRecord>, AgentClientError> {
        self.agent_status(&self.agent)
    }

    /// Latest actually delivered status for a selected agent; no separate cache.
    pub fn agent_status(
        &self,
        agent: &AgentPath,
    ) -> Result<Option<StatusRecord>, AgentClientError> {
        let path = agent.status()?;
        let view = self
            .session
            .view(&path)
            .ok_or(WireSessionError::NotRequested)?;
        view.latest_value(b"status")
            .map(|value| &value.value)
            .or_else(|| view.records().last().map(|record| &record.value))
            .map(|value| StatusRecord::decode(value).map_err(AgentClientError::Record))
            .transpose()
    }

    /// Borrow the same core for counters and rendering; no second ClientViews.
    pub const fn session(&self) -> &WireSession {
        &self.session
    }

    /// Select filtered filesystem/tool output on the same fold, credential and
    /// total interest budget as conversation/status. No second read loop.
    pub fn open_resource(
        &mut self,
        mut subscription: wire::SessionSubscription,
    ) -> Result<(), AgentClientError> {
        if subscription.credential != self.credential {
            return Err(WireSessionError::InvalidSubscription.into());
        }
        let Some(wire::session_subscription::Kind::Follow(follow)) = &mut subscription.kind else {
            return Err(WireSessionError::InvalidSubscription.into());
        };
        follow.opening_cut = self.all;
        wire_codec::session::validate_resource(&follow.resource)
            .map_err(WireSessionError::Stream)?;
        self.session.begin_resource(subscription)?;
        Ok(())
    }

    /// Borrow only the requested resource's bounded display window.
    pub fn resource_view(&self, resource: &str) -> Result<&StreamView, AgentClientError> {
        self.session
            .resource_view(resource)
            .ok_or_else(|| WireSessionError::NotRequested.into())
    }

    /// Release both live interest and retained output custody, including after
    /// a failure. The next combined read can continue with healthy selections.
    pub fn release_resource(&mut self, resource: &str) -> Result<bool, AgentClientError> {
        self.session.release_resource(resource).map_err(Into::into)
    }

    /// Deliver the original ready frame without scanning dormant subscriptions.
    pub fn poll_delivery(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<AgentDelivery, AgentClientError>> {
        use crate::session::Interest;
        let (interest, outcome) = match self.session.poll_next(context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error.into())),
            Poll::Ready(Ok(None)) => {
                return Poll::Ready(Err(AgentClientError::Closed(self.conversation.clone())));
            }
            Poll::Ready(Ok(Some(event))) => event,
        };
        let frame = match outcome {
            Err(error) => return Poll::Ready(Err(error.into())),
            Ok(Some(frame)) => frame,
            Ok(None) => {
                return Poll::Ready(Err(match interest {
                    Interest::Stream(path) => AgentClientError::Closed(path),
                    Interest::Resource(resource) => AgentClientError::ResourceClosed(resource.to_string()),
                }));
            }
        };
        let resource = match interest {
            Interest::Stream(path) => wire_codec::session::stream_resource(path.as_str()),
            Interest::Resource(resource) => resource.to_string(),
        };
        Poll::Ready(Ok(AgentDelivery { resource, frame }))
    }

    /// Decode the same delivery used by JSONL export into interactive events.
    pub fn poll_next(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<AgentEvent, AgentClientError>> {
        let delivery = match self.poll_delivery(context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(delivery)) => delivery,
        };
        Poll::Ready(self.decode_delivery(&delivery))
    }

    /// Apply a frontend projection to an already folded delivery.
    pub fn decode_delivery(
        &self,
        delivery: &AgentDelivery,
    ) -> Result<AgentEvent, AgentClientError> {
        if let Some(wire::session_frame::Body::Closed(closed)) = &delivery.frame.body
            && closed.code != "not_found"
        {
            return Err(AgentClientError::Refused {
                resource: delivery.resource.clone(),
                code: closed.code.clone(),
                detail: closed.message.clone(),
            });
        }
        let selected = if delivery.resource.starts_with("streams:/") {
            let path = wire_codec::session::stream_path(&delivery.resource)
                .map_err(WireSessionError::Stream)?;
            self.streams.get_key_value(&path)
        } else {
            None
        };
        let Some((path, (agent, status))) = selected else {
            if matches!(
                delivery.frame.body,
                Some(wire::session_frame::Body::Closed(_))
            ) {
                return Err(AgentClientError::ResourceClosed(delivery.resource.clone()));
            }
            let Some(view) = self.session.resource_view(&delivery.resource) else {
                return Err(WireSessionError::NotOpen.into());
            };
            return Ok(AgentEvent::Resource {
                resource: delivery.resource.clone(),
                revision: view.revision(),
            });
        };
        let event = match &delivery.frame.body {
            Some(wire::session_frame::Body::Data(data)) => {
                if *status {
                    let value = match &data.item {
                        Some(wire::session_data::Item::LatestValue(value)) => &value.value,
                        Some(wire::session_data::Item::Record(record)) => &record.value,
                        _ => return Err(WireSessionError::InvalidSubscription.into()),
                    };
                    return StatusRecord::decode(value)
                        .map(|status| AgentEvent::Status {
                            agent: agent.clone(),
                            status,
                        })
                        .map_err(AgentClientError::Record);
                }
                let Some(wire::session_data::Item::Record(record)) = &data.item else {
                    return Err(WireSessionError::InvalidSubscription.into());
                };
                ConversationEntry::decode(&record.value).map(|entry| AgentEvent::Conversation {
                    agent: agent.clone(),
                    sequence: record.sequence,
                    entry,
                })
            }
            Some(wire::session_frame::Body::Closed(_)) => {
                return Err(AgentClientError::Closed(path.clone()));
            }
            _ => Ok(AgentEvent::Progress),
        };
        event.map_err(AgentClientError::Record)
    }

    /// Wait without adding a task, channel or independent follow loop.
    pub async fn next(&mut self) -> Result<AgentEvent, AgentClientError> {
        futures::future::poll_fn(|context| self.poll_next(context)).await
    }
}

fn subscription(
    path: &StreamPath,
    from: u64,
    credential: u32,
    status: bool,
) -> wire::SessionSubscription {
    wire::SessionSubscription {
        credential,
        channel: None,
        filter: status.then(|| wire::SessionFilter {
            keys: vec![wire::KeyRange {
                start: bytes::Bytes::from_static(b"status"),
                end: bytes::Bytes::from_static(b"status\0"),
            }],
            ..Default::default()
        }),
        resume: Some(wire::SessionCursor {
            next: from,
            opaque: bytes::Bytes::new(),
        }),
        kind: Some(wire::session_subscription::Kind::Follow(
            wire::SessionFollow {
                resource: wire_codec::session::stream_resource(path.as_str()),
                delivery: if status {
                    wire::SessionDelivery::Latest
                } else {
                    wire::SessionDelivery::All
                } as i32,
                bootstrap_head: status,
                // Conversation streams may not exist before the first turn.
                // Their creation is a provider event, never a viewport retry.
                when_absent: if status {
                    wire::WhenAbsent::NotFound
                } else {
                    wire::WhenAbsent::Wait
                } as i32,
                ..Default::default()
            },
        )),
    }
}
