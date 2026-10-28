//! The root of one agent tree observed through its connection: its status,
//! its settled turns and its newest tool call's output, on one
//! [`AgentClient`] over the connection's session and credential.
use super::{
    AgentFailure, Connection,
    agent_conversation::Conversation,
    storage,
    tool_output::{CHUNK_BYTES, OutputTail},
};
use crate::{
    Result,
    agent::{AgentPath, ConversationEntry, StatusRecord},
};
use acyclic_actors::addressing::ActorReference;
use acyclic_harness_client::{
    AgentClient, AgentClientError, AgentEvent, AgentSelection, ReadTarget, WireSessionError,
};
use acyclic_stream::{StreamError, StreamPath, wire};
use tokio::sync::broadcast;

/// What an observer sees, in commit order per stream.
pub enum Event {
    /// The agent's phase after a transition, with its consumed inbox cursor.
    Status(StatusRecord),
    /// One settled turn's conversation entry, at the inbox sequence of the
    /// record that started it.
    Turn(ConversationEntry),
    /// New tool output was announced; [`Observer::absorb`] reads it.
    Output,
    /// The runtime failed the agent's delivery; it stays queued for a retry
    /// that may fail the same way.
    Failed(String),
}

/// A live, read-only observation of one agent. Revoking the scope it derives
/// from ends it.
pub struct Observer {
    client: AgentClient,
    connection: Connection,
    agent: AgentPath,
    conversation: StreamPath,
    status: StreamPath,
    /// The `fs:/` resource of the agent's `tools/`.
    resource: String,
    /// Next conversation sequence, where a missing conversation reopens.
    next_turn: u64,
    /// The conversation stream does not exist yet: the step that creates it
    /// also appends a status, so it reopens after the next status.
    missing: bool,
    failures: Option<(broadcast::Receiver<AgentFailure>, ActorReference)>,
    tail: OutputTail,
}

impl Observer {
    /// Observes `connection`'s root from conversation sequence `from` and
    /// status sequence `status_from` on.
    pub(super) fn open(connection: &Connection, from: u64, status_from: u64) -> Result<Self> {
        let agent = connection.root().clone();
        let mut client = connection
            .controls
            .observe(AgentSelection {
                agent: agent.clone(),
                conversation_from: from,
                status_from,
            })
            .map_err(storage)?;
        client
            .open_resource(wire::SessionSubscription {
                credential: super::endpoint::CREDENTIAL,
                filter: None,
                channel: None,
                resume: Some(wire::SessionCursor {
                    next: 0,
                    opaque: Default::default(),
                }),
                kind: Some(wire::session_subscription::Kind::Follow(
                    wire::SessionFollow {
                        resource: connection.tools.clone(),
                        delivery: wire::SessionDelivery::All as i32,
                        ..Default::default()
                    },
                )),
            })
            .map_err(storage)?;
        Ok(Self {
            client,
            conversation: agent.conversation().map_err(storage)?,
            status: agent.status().map_err(storage)?,
            resource: connection.tools.clone(),
            next_turn: from,
            missing: false,
            failures: connection
                .failures
                .as_ref()
                .map(|failures| (failures.sender.subscribe(), failures.actor.clone())),
            tail: OutputTail::default(),
            connection: connection.clone(),
            agent,
        })
    }

    /// The next event; cancel-safe.
    ///
    /// # Errors
    /// Fails when a follow is refused or ends, including on revocation.
    pub async fn next(&mut self) -> Result<Event> {
        loop {
            let next = {
                let Self {
                    client, failures, ..
                } = &mut *self;
                let failure = async {
                    match failures.as_mut() {
                        Some((failures, _)) => failures.recv().await,
                        None => futures::future::pending().await,
                    }
                };
                tokio::select! {
                    event = client.next() => Ok(event),
                    failure = failure => Err(failure),
                }
            };
            let event = match next {
                Ok(event) => event,
                Err(Ok(failure))
                    if self
                        .failures
                        .as_ref()
                        .is_some_and(|(_, actor)| failure.actor == *actor) =>
                {
                    return Ok(Event::Failed(failure.error));
                }
                Err(Ok(_) | Err(broadcast::error::RecvError::Lagged(_))) => continue,
                Err(Err(broadcast::error::RecvError::Closed)) => {
                    self.failures = None;
                    continue;
                }
            };
            match event {
                Ok(AgentEvent::Status { status, .. }) => {
                    if self.missing {
                        self.missing = false;
                        self.client
                            .set_view(
                                AgentSelection {
                                    agent: self.agent.clone(),
                                    conversation_from: self.next_turn,
                                    status_from: 0,
                                },
                                std::iter::empty(),
                            )
                            .map_err(storage)?;
                    }
                    tracing::debug!(phase = ?status.phase, cursor = status.cursor, "agent status");
                    return Ok(Event::Status(status));
                }
                Ok(AgentEvent::Conversation {
                    sequence, entry, ..
                }) => {
                    self.next_turn = sequence + 1;
                    // The turn's output is complete; the next turn starts
                    // its own display.
                    let end = self
                        .client
                        .resource_view(&self.resource)
                        .map_err(storage)?
                        .next_sequence();
                    self.tail.reset(end);
                    return Ok(Event::Turn(entry));
                }
                Ok(AgentEvent::Resource { .. }) => return Ok(Event::Output),
                Ok(AgentEvent::Progress) => {}
                Err(error) if self.conversation_missing(&error) => self.missing = true,
                Err(error) => return Err(storage(error)),
            }
        }
    }

    /// Whether `error` only reports that the conversation stream does not
    /// exist yet, while the status follow lives on to announce it.
    fn conversation_missing(&self, error: &AgentClientError) -> bool {
        let session = self.client.session();
        let conversation = match error {
            AgentClientError::Closed(path) => *path == self.conversation,
            AgentClientError::Session(WireSessionError::Stream(StreamError::NotFound)) => true,
            _ => false,
        };
        conversation
            && !session.is_following(&self.conversation)
            && session.is_following(&self.status)
    }

    /// Reads the output chunks announced since the last call. Only the newest
    /// call's chunks are read, so a reopened history costs one call's output.
    ///
    /// # Errors
    /// Fails when an announced chunk cannot be read.
    pub async fn absorb(&mut self) -> Result<()> {
        let view = self.client.resource_view(&self.resource).map_err(storage)?;
        let next = self.tail.next();
        let mut records = Vec::new();
        for record in view.records().filter(|record| record.sequence >= next) {
            // Filesystem change records carry their paths in the compact
            // change list, not as wire keys.
            let changes = acyclic_stream::decode_changes(&record.value).map_err(storage)?;
            let paths = changes
                .entries
                .iter()
                .filter_map(|entry| std::str::from_utf8(&entry.key).ok().map(str::to_owned))
                .collect();
            records.push((record.sequence, paths));
        }
        let Some(plan) = self
            .tail
            .plan(records, view.next_sequence(), view.watermarks().accepted)
        else {
            return Ok(());
        };
        let mut bytes = Vec::with_capacity(plan.reads.len());
        for (path, _) in &plan.reads {
            bytes.push(
                Conversation::read(
                    &self.connection,
                    &self.agent,
                    ReadTarget::ToolOutput { path: path.clone() },
                    CHUNK_BYTES as u64,
                )
                .await?,
            );
        }
        self.tail.apply(plan, bytes);
        Ok(())
    }

    /// The newest call's output, its last 16 KiB at most.
    #[must_use]
    pub fn output(&self) -> &str {
        self.tail.text()
    }
}
