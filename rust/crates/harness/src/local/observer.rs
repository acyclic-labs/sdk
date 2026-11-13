//! The root of one agent tree observed through its connection: its status,
//! its settled turns and its newest tool call's output, on one
//! [`AgentClient`] over the connection's session and credential.
use super::{
    Connection,
    agent_conversation::Conversation,
    storage,
    tool_output::{LiveKind, OutputTail, read_newest},
};
use crate::{
    Error, Result,
    agent::{AgentPath, ConversationEntry, StatusRecord},
};
use acyclic_harness_client::{
    AgentClient, AgentClientError, AgentEvent, AgentSelection, ReadTarget, WireSessionError,
};
use acyclic_stream::{StreamPath, wire};

/// What an observer sees, in commit order per stream.
pub enum Event {
    /// The agent's phase after a transition, with its consumed inbox cursor.
    Status(StatusRecord),
    /// One settled turn's conversation entry, at the inbox sequence of the
    /// record that started it.
    Turn(ConversationEntry),
    /// A delta of live text was announced in the agent's `tools/` records:
    /// tool output, or a model dispatch's answer or reasoning while it
    /// streams. [`Observer::absorb`] reads it into [`Observer::live`]; a
    /// caller that does not display it reads nothing.
    Delta,
    /// The optional accepted-input follow advanced through this exclusive cursor.
    Input { through: u64 },
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
    input_resource: Option<String>,
    input_through: u64,
    /// Next conversation sequence not yet returned to the caller.
    next_turn: u64,
    /// A phase-aware opening supplied this checkpoint's phase. A replayed idle
    /// status must not replace accepted input that is already running.
    status_floor: Option<(u64, bool)>,
    tail: OutputTail,
}

impl Observer {
    /// Settlement needs both original required follows, including pending
    /// opens. A terminal view alone no longer owns a producing event.
    pub(super) fn belongs_to(&self, connection: &Connection) -> bool {
        std::sync::Arc::ptr_eq(&self.connection.controls, &connection.controls)
            && self.client.session().is_following(&self.conversation)
            && self.client.session().is_following(&self.status)
    }

    /// Observes `connection`'s root from conversation sequence `from` and
    /// status sequence `status_from` on.
    pub(super) fn open(connection: &Connection, from: u64, status_from: u64) -> Result<Self> {
        Self::opening(
            connection,
            AgentSelection::new(connection.root().clone(), from, status_from),
            None,
        )
    }

    pub(super) fn open_at_phase(
        connection: &Connection,
        from: u64,
        statuses: u64,
        phase: &crate::agent::Phase,
    ) -> Result<Self> {
        let mut observer = Self::open(connection, from, statuses.saturating_sub(1))?;
        observer.status_floor = Some((statuses, phase.in_turn()));
        Ok(observer)
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
                .wire()
                .map_err(storage)
        };
        let mut observer = Self::opening(
            connection,
            AgentSelection {
                agent: agent.clone(),
                conversation: cursor(agent.conversation().map_err(storage)?)?,
                status: cursor(agent.status().map_err(storage)?)?,
            },
            Some(projection),
        )?;
        let input = acyclic_stream::wire_codec::session::stream_resource(
            agent.inbox().map_err(storage)?.as_str(),
        );
        let mut sources = observer.sources();
        sources.push(input.clone());
        if projection.validate_sources(&sources).is_ok() {
            observer.watch_inputs_from(projection.cursor(&input).wire().map_err(storage)?)?;
        }
        projection
            .validate_sources(&observer.sources())
            .map_err(storage)?;
        Ok(observer)
    }

    fn opening(
        connection: &Connection,
        selection: AgentSelection,
        projection: Option<&acyclic_harness_client::EventProjection>,
    ) -> Result<Self> {
        let agent = selection.agent.clone();
        let from = selection.conversation.next;
        let mut client = match projection {
            Some(_) => connection.controls.observe_all(selection),
            None => connection.controls.observe(selection),
        }
        .map_err(storage)?;
        client
            .open_resource(wire::SessionSubscription {
                credential: connection.controls.observation_credential(),
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
            input_resource: None,
            input_through: 0,
            next_turn: from,
            status_floor: None,
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
            .map_err(|error| self.follow_error(error))?;
        let after = match &delivery.frame.body {
            Some(wire::session_frame::Body::Data(data)) => {
                data.after.as_ref().map(|cursor| cursor.next)
            }
            _ => None,
        };
        let event = self
            .client
            .decode_delivery(&delivery)
            .map_err(|error| self.follow_error(error))
            .and_then(|event| self.apply(event, after));
        Ok((event, delivery))
    }

    /// The bounded set of sources selected by this observer.
    pub fn sources(&self) -> Vec<String> {
        let mut sources = vec![
            acyclic_stream::wire_codec::session::stream_resource(self.status.as_str()),
            acyclic_stream::wire_codec::session::stream_resource(self.conversation.as_str()),
            self.resource.clone(),
        ];
        sources.extend(self.input_resource.iter().cloned());
        sources
    }

    /// Follow accepted input on the same bounded reader and credential. No
    /// message body is loaded. Other observers pay nothing unless they opt in.
    pub fn watch_inputs(&mut self) -> Result<()> {
        self.watch_inputs_from(wire::SessionCursor {
            next: 0,
            opaque: Default::default(),
        })
    }

    fn watch_inputs_from(&mut self, cursor: wire::SessionCursor) -> Result<()> {
        if self.input_resource.is_some() {
            return Ok(());
        }
        let resource = acyclic_stream::wire_codec::session::stream_resource(
            self.agent.inbox().map_err(storage)?.as_str(),
        );
        self.input_through = cursor.next;
        self.client
            .open_resource(wire::SessionSubscription {
                credential: self.connection.controls.observation_credential(),
                filter: None,
                channel: None,
                resume: Some(cursor),
                kind: Some(wire::session_subscription::Kind::Follow(
                    wire::SessionFollow {
                        resource: resource.clone(),
                        delivery: wire::SessionDelivery::Latest as i32,
                        ..Default::default()
                    },
                )),
            })
            .map_err(storage)?;
        self.input_resource = Some(resource);
        Ok(())
    }
    fn apply(&mut self, event: AgentEvent, after: Option<u64>) -> Result<Option<Event>> {
        Ok(match event {
            AgentEvent::Status { status, .. } => {
                if let Some((floor, opening_turn)) = self.status_floor.take() {
                    let after = after.ok_or_else(|| {
                        Error::Invalid("status delivery has no source cursor".into())
                    })?;
                    if after <= floor && opening_turn && !status.phase.in_turn() {
                        return Ok(None);
                    }
                }
                if !status.phase.in_turn() {
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
            AgentEvent::Resource { resource, .. }
                if self.input_resource.as_ref() == Some(&resource) =>
            {
                after
                    .filter(|through| *through > self.input_through)
                    .map(|through| {
                        self.input_through = through;
                        Event::Input { through }
                    })
            }
            AgentEvent::Resource { .. } => Some(Event::Delta),
            AgentEvent::Progress => None,
        })
    }

    /// The next event; cancel-safe.
    ///
    /// # Errors
    /// Fails when a follow is refused or ends, including on revocation.
    pub async fn next(&mut self) -> Result<Event> {
        loop {
            let (event, _) = self.next_projected().await?;
            if let Some(event) = event? {
                return Ok(event);
            }
        }
    }

    /// Creation wakes the original follow. Terminal errors retain their source
    /// classification rather than being treated as another creation wait.
    fn follow_error(&self, error: AgentClientError) -> Error {
        tracing::debug!(%error, "observer follow failed");
        if error.code() == "unavailable" {
            self.connection.stream_failure(error)
        } else {
            match error {
                AgentClientError::Subscription {
                    resource,
                    cause: WireSessionError::Stream(source),
                } => Error::StreamFollow { resource, source },
                error => storage(error),
            }
        }
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

    /// The live text absorbed so far and what it is: the newest tool call's
    /// output, or the running dispatch's answer or reasoning, its last
    /// 16 KiB at most. Empty once the turn settles: its entry is the record.
    #[must_use]
    pub fn live(&self) -> (LiveKind, &str) {
        (self.tail.kind(), self.tail.text())
    }
}
