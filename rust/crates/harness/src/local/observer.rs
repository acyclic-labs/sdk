//! One agent observed as its local user: its status, its settled turns and
//! its newest tool call's output, on one [`AgentClient`] over the harness's
//! in-process session and one derived credential.
use super::{
    LocalHarness, storage,
    tool_output::{CHUNK_BYTES, OutputTail},
};
use crate::{
    Error, Result,
    agent::{AgentPath, ConversationEntry, StatusRecord},
    conversation::{ContentAccess, ContentResidencyVerifier, VolumeRef},
    filesystem::{FilesystemContentVerifier, TOOL_OUTPUT, TurnAdmission, agent_authority},
};
use acyclic_actors::addressing::ActorReference;
use acyclic_harness_client::{
    AgentClient, AgentClientError, AgentEvent, AgentSelection, WireSessionError,
};
use acyclic_stream::{StreamError, StreamPath, StreamProvider as _, session::Client, wire};
use std::sync::Arc;
use tokio::sync::broadcast;

/// The observer's one credential slot: conversation, status and tool output.
const CREDENTIAL: u32 = 0;

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
    failures: Option<broadcast::Receiver<super::AgentFailure>>,
    actor: ActorReference,
    reader: Arc<dyn ContentResidencyVerifier>,
    volume: VolumeRef,
    tail: OutputTail,
    #[cfg(test)]
    pub(super) scope: crate::core::Scope,
}

impl LocalHarness {
    /// Observes `agent` from conversation sequence `from` and its latest
    /// status on, under a credential derived from its own scope and bound to
    /// its audience.
    ///
    /// # Errors
    /// Fails for an embedded harness, an unknown agent, or a scope that does
    /// not grant reading its tool output.
    pub async fn observe(self: &Arc<Self>, agent: &AgentPath, from: u64) -> Result<Observer> {
        let (agents, _) = self.agents().await?;
        let state = agents.state(agent).await.map_err(storage)?;
        let actor = agents.actor(agent).map_err(storage)?;
        let failures = Some(
            self.agent_failures()
                .ok_or_else(|| Error::Conflict("agents are not hosted".into()))?,
        );
        let issuer = self.root_authority.as_ref().ok_or_else(|| {
            Error::Unsupported("an embedded harness has no agent signing custody".into())
        })?;
        let stream = self
            .agent_stream
            .clone()
            .ok_or_else(|| Error::Unsupported("an embedded harness has no agent stream".into()))?;
        let authority = agent_authority(
            issuer,
            &TurnAdmission::LocalRoot,
            &state.path,
            &state.volumes,
            &[],
        )?;
        let observation = self
            .files
            .agent_observation(
                &authority,
                &state.path,
                &authority.private.volume,
                Arc::clone(&self.clock),
            )
            .await?;
        let changes = self.files.change_provider(Arc::clone(&stream))?;
        let reader = FilesystemContentVerifier::new(
            Arc::clone(&self.files),
            &authority.verifier,
            &authority.scope,
            self.limits.file_bytes,
            None,
        )?;
        let status = agent.status().map_err(storage)?;
        let latest = stream.tail(status.clone()).await.map_err(storage)?;
        let session = Client::in_process_with_resources(
            stream,
            observation.authority,
            Arc::new(changes),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
        );
        session
            .register_credential(CREDENTIAL, observation.credential.into())
            .map_err(storage)?;
        let mut client = AgentClient::new(
            session,
            agent.clone(),
            from,
            latest.saturating_sub(1),
            CREDENTIAL,
        )
        .map_err(storage)?;
        client
            .open_resource(wire::SessionSubscription {
                credential: CREDENTIAL,
                filter: None,
                channel: None,
                resume: Some(wire::SessionCursor {
                    next: 0,
                    opaque: Default::default(),
                }),
                kind: Some(wire::session_subscription::Kind::Follow(
                    wire::SessionFollow {
                        resource: observation.resource.clone(),
                        delivery: wire::SessionDelivery::All as i32,
                        ..Default::default()
                    },
                )),
            })
            .map_err(storage)?;
        Ok(Observer {
            client,
            agent: agent.clone(),
            conversation: agent.conversation().map_err(storage)?,
            status,
            resource: observation.resource,
            next_turn: from,
            missing: false,
            failures,
            actor,
            reader,
            volume: authority.private.volume.clone(),
            tail: OutputTail::default(),
            #[cfg(test)]
            scope: authority.scope.clone(),
        })
    }
}

impl Observer {
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
                        Some(failures) => failures.recv().await,
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
                Err(Ok(failure)) if failure.actor == self.actor => {
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
            let (_, read, _) = self
                .reader
                .read_private_path(
                    &self.volume,
                    TOOL_OUTPUT,
                    path,
                    None,
                    CHUNK_BYTES as u64,
                    ContentAccess::default(),
                )?
                .await?;
            bytes.push(read.to_vec());
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
