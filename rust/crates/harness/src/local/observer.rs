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
    Error, ExchangeStep, Result,
    agent::{AgentPath, ConversationEntry, StatusRecord},
};
use acyclic_harness_client::{
    AgentClient, AgentClientError, AgentEvent, AgentSelection, EventDrain, ReadTarget,
    WireSessionError,
};
use acyclic_stream::{StreamPath, wire};

struct OutputSelection {
    conversation: u64,
    owner: crate::OperationId,
    prefix: String,
}

/// What an observer sees, in commit order per stream.
pub enum Event {
    /// The agent's phase after a transition, with its consumed inbox cursor.
    Status(StatusRecord),
    /// One settled turn's conversation entry, at the inbox sequence of the
    /// record that started it. `sequence` is its committed conversation
    /// sequence, the proof [`super::Conversation::turn`] reads its prompt with.
    Turn {
        /// Committed conversation sequence of `entry`.
        sequence: u64,
        /// The settled entry.
        entry: ConversationEntry,
    },
    /// A delta of live text was announced in the agent's `tools/` records:
    /// tool output, or a model dispatch's answer or reasoning while it
    /// streams. [`Observer::absorb`] reads it into [`Observer::live`]; a
    /// caller that does not display it reads nothing.
    Delta,
    /// The optional accepted-input follow advanced through this exclusive cursor.
    Input {
        /// Exclusive accepted inbox cursor, without loading message bodies.
        through: u64,
    },
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
    /// A phase-aware opening already supplied the state through this checkpoint.
    /// Replayed statuses must not replace that opening state.
    status_floor: Option<u64>,
    /// The original tools opening cut, retired once its backfill is consumed.
    output_opening: EventDrain,
    output_selection: Option<OutputSelection>,
    output_changed: bool,
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

    pub(super) fn open_at_phase(connection: &Connection, from: u64, statuses: u64) -> Result<Self> {
        let mut observer = Self::open(connection, from, statuses.saturating_sub(1))?;
        observer.status_floor = Some(statuses);
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
                conversation: Some(cursor(agent.conversation().map_err(storage)?)?),
                status: Some(cursor(agent.status().map_err(storage)?)?),
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
        let (Some(conversation), Some(_)) = (&selection.conversation, &selection.status) else {
            return Err(Error::Invalid(
                "an observer requires conversation and status cursors".into(),
            ));
        };
        let from = conversation.next;
        let mut client = match projection {
            Some(_) => connection.controls.observe_all(selection),
            None => connection.controls.observe(selection),
        }
        .map_err(storage)?;
        let output_from = match projection {
            Some(projection) => projection
                .cursor(&connection.tools)
                .wire()
                .map_err(storage)?,
            None => wire::SessionCursor::default(),
        };
        let output_opening = EventDrain::new([(connection.tools.clone(), output_from.next)]);
        client
            .open_resource(wire::SessionSubscription {
                credential: connection.controls.observation_credential(),
                filter: None,
                channel: None,
                resume: Some(output_from),
                kind: Some(wire::session_subscription::Kind::Follow(
                    wire::SessionFollow {
                        resource: connection.tools.clone(),
                        delivery: wire::SessionDelivery::All as i32,
                        opening_cut: true,
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
            output_opening,
            output_selection: None,
            output_changed: false,
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
            .and_then(|event| {
                let was_opening = !self.output_opening.complete();
                self.output_opening.accepted(&delivery);
                let event = self.apply(event, after)?;
                // A filtered progress delivery may finish backfill without a
                // data record. Wake the display for that original cut too.
                Ok(event.or_else(|| {
                    (was_opening && self.output_opening.complete()).then_some(Event::Delta)
                }))
            });
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
                        delivery: wire::SessionDelivery::All as i32,
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
                if status.conversation < self.next_turn {
                    return Ok(None);
                }
                if let Some(floor) = self.status_floor {
                    let after = after.ok_or_else(|| {
                        Error::Invalid("status delivery has no source cursor".into())
                    })?;
                    if after < floor {
                        return Ok(None);
                    }
                    self.select_output(&status)?;
                    if after == floor {
                        return Ok(None);
                    }
                    self.status_floor = None;
                } else {
                    self.select_output(&status)?;
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
                if self
                    .output_selection
                    .as_ref()
                    .is_none_or(|selection| selection.conversation < self.next_turn)
                {
                    self.output_selection = None;
                    self.reset_output()?;
                }
                Some(Event::Turn { sequence, entry })
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
            if std::mem::take(&mut self.output_changed) {
                return Ok(Event::Delta);
            }
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
                error => Error::exchange(ExchangeStep::Session, error),
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

    fn select_output(&mut self, status: &StatusRecord) -> Result<()> {
        let owner = status.output_owner.filter(|_| status.phase.in_turn());
        if self
            .output_selection
            .as_ref()
            .map(|selection| selection.owner)
            == owner
        {
            if let Some(selection) = &mut self.output_selection {
                selection.conversation = status.conversation;
            }
            return Ok(());
        }
        self.output_selection = owner.map(|owner| OutputSelection {
            conversation: status.conversation,
            owner,
            prefix: format!("tools/.model/{}/", hex::encode(owner.into_bytes())),
        });
        if owner.is_some() {
            // Resource delivery may precede its owning status. Re-select only
            // the bounded display window, never an unread transport cursor.
            let from = self
                .client
                .resource_view(&self.resource)
                .map_err(storage)?
                .first_sequence();
            self.tail.select(from);
        } else {
            self.reset_output()?;
        }
        // One status can change both phase and displayed output. The typed
        // event iterator publishes its display notification without another
        // source delivery; raw projections retain the original status frame.
        self.output_changed = true;
        Ok(())
    }

    /// Reads the output chunks announced since the last call. Only the newest
    /// call's chunks are read, so a reopened history costs one call's output.
    ///
    /// # Errors
    /// Fails when an announced chunk cannot be read.
    pub async fn absorb(&mut self) -> Result<()> {
        if !self.output_opening.complete() {
            return Ok(());
        }
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
        let plan = self.tail.plan(
            records,
            view.next_sequence(),
            self.output_selection
                .as_ref()
                .map(|selection| selection.prefix.as_str()),
        );
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
        self.output_changed = false;
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::{
        agent::Phase,
        model::ModelEvent,
        test_support::{ScriptedEvent, ScriptedTurn},
    };
    use acyclic_harness_client::AgentRecord as _;
    use std::sync::Arc;

    #[tokio::test]
    async fn replayed_model_text_cannot_replace_a_settled_or_newer_turn() -> Result<()> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let provider = super::super::tests::scripted([
            ScriptedTurn::Text("old answer".into()),
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::Content {
                    delta: "fresh answer".into(),
                }),
                ScriptedEvent::Pending {},
            ]),
        ]);
        let project = super::super::tests::volume(directory.path()).await?;
        let harness = Arc::new(
            super::super::tests::open_scripted(super::super::tests::volume_config(
                directory.path(),
                project,
                provider,
            )?)
            .await?,
        );
        harness.agents().await?;
        let completion = harness
            .agents
            .get()
            .expect("hosted agents")
            .runtime
            .completion();
        let run = async {
            let connection = harness.connect().await?;
            let conversation = Conversation::open(connection.clone());
            let mut original = conversation.observe(Some(0)).await?;
            let input = conversation.send("first", "first question").await?;
            eprintln!("observer regression: waiting for the original settled turn");
            let old = loop {
                if let Event::Turn { entry, .. } = original.next().await?
                    && entry.inbox == input
                {
                    break entry;
                }
            };
            let (agents, root) = harness.agents().await?;
            let state = agents.state(root).await.map_err(storage)?;
            let old_owner = crate::agent::turn_id(&state, &old.input).map_err(storage)?;
            eprintln!("observer regression: draining the original opening cuts");

            // Actual model files and actor records are replayed through the served
            // session. The fixed opening cut bounds this drain even if appends follow.
            let projection = acyclic_harness_client::EventProjection::new("observation".into());
            let mut replay = Observer::opening(
                &connection,
                AgentSelection::new(root.clone(), 0, 0),
                Some(&projection),
            )?;
            let mut drain = EventDrain::new(replay.sources().into_iter().map(|source| (source, 0)));
            replay.absorb().await?;
            while !drain.complete() {
                let (event, delivery) = replay.next_projected().await?;
                event?;
                drain.accepted(&delivery);
                replay.absorb().await?;
            }
            assert!(replay.output_selection.is_none());
            assert_eq!(replay.live().1, "");
            let stale = StatusRecord {
                output_owner: Some(old_owner),
                cancelled_input: None,
                child_settlement: None,
                child_wakes: None,
                phase: Phase::Running,
                cursor: input + 1,
                conversation: 0,
            };
            // Deterministically place the original running status after settlement,
            // then reconsider the actual cached file records as delayed backfill.
            assert!(
                replay
                    .apply(
                        AgentEvent::Status {
                            agent: root.clone(),
                            status: stale.clone(),
                        },
                        Some(1)
                    )?
                    .is_none()
            );
            replay.tail.select(0);
            replay.absorb().await?;
            assert_eq!(replay.live().1, "");

            let second = conversation.send("second", "second question").await?;
            let mut current = conversation.observe(Some(1)).await?;
            eprintln!("observer regression: waiting for the newer turn's model file");
            let old_prefix = format!("tools/.model/{}/", hex::encode(old_owner.into_bytes()));
            loop {
                current.next().await?;
                current.absorb().await?;
                assert_ne!(current.live().1, "old answer");
                let mut fresh_file = false;
                for record in current
                    .client
                    .resource_view(&current.resource)
                    .map_err(storage)?
                    .records()
                {
                    let changes = acyclic_stream::decode_changes(&record.value).map_err(storage)?;
                    fresh_file |= changes.entries.iter().any(|entry| {
                        std::str::from_utf8(&entry.key).is_ok_and(|path| {
                            path.starts_with("tools/.model/")
                                && !path.starts_with(&old_prefix)
                                && path.contains("/reply/")
                        })
                    });
                }
                if fresh_file {
                    break;
                }
            }
            let state = agents.state(root).await.map_err(storage)?;
            let owner = state
                .turn
                .as_ref()
                .and_then(|turn| turn.output_owner)
                .ok_or_else(|| {
                    Error::Invalid("running model has no retained output owner".into())
                })?;
            assert_ne!(owner, old_owner);
            // Check the actual committed status once a real model file exists. A
            // missing same-phase owner publication fails here instead of waiting
            // forever for a display notification that can never arrive.
            let status_path = root.status().map_err(storage)?;
            let mut statuses =
                acyclic_stream::Stream::new(Arc::clone(agents.provider()), status_path.as_str())
                    .map_err(storage)?
                    .replay(state.status_tail - 1)
                    .until(state.status_tail);
            let status = statuses
                .next_page()
                .await
                .map_err(storage)?
                .and_then(|page| page.into_iter().next())
                .ok_or_else(|| {
                    Error::Invalid("model output has no original status record".into())
                })?;
            assert_eq!(
                StatusRecord::decode(&status.value)
                    .map_err(storage)?
                    .output_owner,
                Some(owner)
            );
            while current.live().1 != "fresh answer" {
                current.next().await?;
                current.absorb().await?;
                assert_ne!(current.live().1, "old answer");
            }
            assert_eq!(
                current
                    .output_selection
                    .as_ref()
                    .map(|selection| selection.owner),
                Some(owner)
            );
            assert_eq!(current.live().0, LiveKind::Answer);
            // A newer owner can arrive before an older settled conversation record.
            // That older record must not clear the genuine newer display.
            current.apply(
                AgentEvent::Conversation {
                    agent: root.clone(),
                    sequence: 0,
                    entry: old,
                },
                Some(1),
            )?;
            assert_eq!(current.live().1, "fresh answer");
            assert!(
                current
                    .apply(
                        AgentEvent::Status {
                            agent: root.clone(),
                            status: stale,
                        },
                        Some(1)
                    )?
                    .is_none()
            );
            assert_eq!(current.live().1, "fresh answer");
            conversation.cancel_input(second).await?;
            eprintln!("observer regression: waiting for cancellation settlement");
            loop {
                if let Event::Turn { entry, .. } = current.next().await?
                    && entry.inbox == second
                {
                    break;
                }
            }
            current.tail.select(0);
            current.absorb().await?;
            assert_eq!(current.live().1, "");
            Ok(())
        };
        // The served stream cannot publish another turn after its original
        // runtime has ended. Retain that terminal event instead of hanging on
        // a follow whose producer has already failed.
        tokio::select! {
            result = run => result,
            failure = completion => Err(Error::Conflict(format!("observer runtime ended: {failure:?}"))),
        }
    }
}
