//! The root of one agent tree observed through its connection: its status,
//! its settled turns and its newest tool call's output, on one
//! [`AgentClient`] over the connection's session and credential.
use super::{
    Connection,
    agent_conversation::Conversation,
    storage,
    tool_output::{OutputTail, read_newest},
};
use crate::{
    Result,
    agent::{AgentPath, ConversationEntry, StatusRecord},
};
use acyclic_harness_client::{
    AgentClient, AgentClientError, AgentEvent, AgentSelection, ReadTarget, WireSessionError,
};
use acyclic_stream::{StreamError, StreamPath, wire};

/// What an observer sees, in commit order per stream.
pub enum Event {
    /// The agent's phase after a transition, with its consumed inbox cursor.
    Status(StatusRecord),
    /// One settled turn's conversation entry, at the inbox sequence of the
    /// record that started it.
    Turn(ConversationEntry),
    /// New tool output was announced; [`Observer::absorb`] reads it.
    Output,
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
    /// Next conversation sequence, where a refused conversation reopens.
    next_turn: u64,
    tail: OutputTail,
}

impl Observer {
    /// Observes `connection`'s root from conversation sequence `from` and
    /// status sequence `status_from` on.
    pub(super) fn open(connection: &Connection, from: u64, status_from: u64) -> Result<Self> {
        Self::opening(connection, from, status_from, None)
    }

    pub(super) fn replay(
        connection: &Connection,
        projection: &acyclic_harness_client::EventProjection,
    ) -> Result<Self> {
        let agent = connection.root();
        projection.execution_position(agent).map_err(storage)?;
        let cursor = |path: StreamPath| {
            projection
                .cursor(&acyclic_stream::wire_codec::session::stream_resource(
                    path.as_str(),
                ))
                .next
        };
        let observer = Self::opening(
            connection,
            cursor(agent.conversation().map_err(storage)?),
            cursor(agent.status().map_err(storage)?),
            Some(projection),
        )?;
        projection
            .validate_sources(&observer.sources())
            .map_err(storage)?;
        Ok(observer)
    }

    fn opening(
        connection: &Connection,
        from: u64,
        status_from: u64,
        projection: Option<&acyclic_harness_client::EventProjection>,
    ) -> Result<Self> {
        let agent = connection.root().clone();
        let selection = AgentSelection {
            agent: agent.clone(),
            conversation_from: from,
            status_from,
        };
        let mut client = match projection {
            Some(_) => connection.controls.observe_all(selection),
            None => connection.controls.observe(selection),
        }
        .map_err(storage)?;
        client
            .open_resource(wire::SessionSubscription {
                credential: super::endpoint::CREDENTIAL,
                filter: None,
                channel: None,
                resume: Some(match projection {
                    Some(projection) => projection
                        .cursor(&connection.tools)
                        .wire()
                        .map_err(storage)?,
                    None => wire::SessionCursor {
                        next: 0,
                        opaque: Default::default(),
                    },
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
            tail: OutputTail::default(),
            connection: connection.clone(),
            agent,
        })
    }

    /// One lossless delivery and its ordinary frontend event, including
    /// watermarks. The caller flushes the JSONL line before saving its cursor.
    ///
    /// # Errors
    /// The same authenticated follow and canonical record errors as `next`.
    pub async fn next_projected(
        &mut self,
    ) -> Result<(Result<Option<Event>>, acyclic_harness_client::AgentDelivery)> {
        let delivery = futures::future::poll_fn(|cx| self.client.poll_delivery(cx))
            .await
            .map_err(|error| {
                if matches!(
                    &error,
                    AgentClientError::Session(WireSessionError::Stream(StreamError::Unavailable))
                ) {
                    self.connection.stream_failure(error)
                } else {
                    storage(error)
                }
            })?;
        let event = match self.client.decode_delivery(&delivery) {
            Ok(event) => self.apply(event),
            Err(error) if self.conversation_missing(&error) => {
                let announced = self
                    .client
                    .current_status()
                    .map_err(storage)?
                    .map_or(0, |status| status.conversation);
                self.follow_announced(announced).map(|()| None)
            }
            Err(error) => Err(storage(error)),
        };
        Ok((event, delivery))
    }

    /// The bounded set of sources selected by this observer.
    pub fn sources(&self) -> [String; 3] {
        [
            acyclic_stream::wire_codec::session::stream_resource(self.status.as_str()),
            acyclic_stream::wire_codec::session::stream_resource(self.conversation.as_str()),
            self.resource.clone(),
        ]
    }

    fn apply(&mut self, event: AgentEvent) -> Result<Option<Event>> {
        Ok(match event {
            AgentEvent::Status { status, .. } => {
                self.follow_announced(status.conversation)?;
                if matches!(
                    status.phase,
                    crate::agent::Phase::Idle | crate::agent::Phase::Done { .. }
                ) {
                    self.reset_output()?;
                }
                Some(Event::Status(status))
            }
            AgentEvent::Conversation {
                sequence, entry, ..
            } => {
                self.next_turn = sequence + 1;
                self.reset_output()?;
                Some(Event::Turn(entry))
            }
            AgentEvent::Resource { .. } => Some(Event::Output),
            AgentEvent::Progress => None,
        })
    }

    /// The next event; cancel-safe.
    ///
    /// # Errors
    /// Fails when a follow is refused or ends, including on revocation.
    pub async fn next(&mut self) -> Result<Event> {
        loop {
            match self.client.next().await {
                Ok(event) => {
                    if let Some(event) = self.apply(event)? {
                        return Ok(event);
                    }
                }
                Err(error) if self.conversation_missing(&error) => {
                    // Refused before its first entry: the status committed
                    // with that entry reopens it, and one read before this
                    // refusal already did.
                    let announced = self
                        .client
                        .current_status()
                        .map_err(storage)?
                        .map_or(0, |status| status.conversation);
                    self.follow_announced(announced)?;
                }
                Err(error) => {
                    tracing::debug!(%error, "observer follow failed");
                    if matches!(
                        &error,
                        AgentClientError::Session(WireSessionError::Stream(
                            StreamError::Unavailable
                        ))
                    ) {
                        return Err(self.connection.stream_failure(error));
                    }
                    return Err(storage(error));
                }
            }
        }
    }

    /// Follows the conversation again from the next unseen turn when a
    /// committed status announced entries past it and it is not followed.
    fn follow_announced(&mut self, conversation: u64) -> Result<()> {
        if conversation <= self.next_turn || self.client.session().is_following(&self.conversation)
        {
            return Ok(());
        }
        tracing::debug!(from = self.next_turn, "conversation reopen");
        self.client
            .set_view(
                AgentSelection {
                    agent: self.agent.clone(),
                    conversation_from: self.next_turn,
                    status_from: 0,
                },
                std::iter::empty(),
            )
            .map_err(storage)
    }

    /// Ends the displayed output: records announced so far are never shown.
    fn reset_output(&mut self) -> Result<()> {
        let end = self
            .client
            .resource_view(&self.resource)
            .map_err(storage)?
            .next_sequence();
        self.tail.reset(end);
        Ok(())
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
        // Only the displayed tail is read, in bounded ranges, however large
        // the announced files are.
        let (connection, agent) = (&self.connection, &self.agent);
        let (bytes, cut) = read_newest(&plan.reads, |path, offset| {
            Conversation::read_at(
                connection,
                agent,
                ReadTarget::ToolOutput { path, at: None },
                offset,
            )
        })
        .await?;
        self.tail.apply(plan, bytes, cut);
        Ok(())
    }

    /// The next conversation sequence not yet observed: every entry before
    /// it was returned as [`Event::Turn`].
    #[must_use]
    pub const fn seen(&self) -> u64 {
        self.next_turn
    }

    /// The newest call's output, its last 16 KiB at most.
    #[must_use]
    pub fn output(&self) -> &str {
        if self.tail.reply() {
            ""
        } else {
            self.tail.text()
        }
    }

    /// The running turn's answer text while it streams, its last 16 KiB at
    /// most; empty once a tool call runs or the turn settles.
    #[must_use]
    pub fn reply(&self) -> &str {
        if self.tail.reply() {
            self.tail.text()
        } else {
            ""
        }
    }
}
