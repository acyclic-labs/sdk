//! The device's actor `targets/<name>`: presence from its socket, the
//! hand-over of every queued run, and the record of each run's outcome.
//!
//! The actor is the one place a run's fate is decided. It reads the queue
//! and the socket's events in one order: a run for the open session is
//! recorded `Started` and sent to the device; a run for any other session is
//! recorded `Refused` and never sent. When the session ends, every unfinished
//! run is covered by one `Lost` session marker in the same commit as the
//! offline presence. Every observer joins that one immutable session end with
//! its own operation stream; disconnect never enumerates active operations.
use super::records::{
    DeviceMessage, HandoverRecord, MessageRecord, RECORD_VERSION, Run, RunEvent, RunRecord,
    TargetPaths, TargetStatus, decode, encode,
};
use crate::{
    Error, OperationId, Result,
    agent::{TargetRecord, TargetState},
};
use acyclic_actors::{
    addressing::ActorReference,
    domain::{ActorLimits, CodeSha256},
    guest::{
        AlarmChange, Checkpoint, CodecIdentity, Composer, Composition, Delivery, Effect,
        GuestError, GuestEventResult, GuestFuture, GuestSocketEvent, Handler, Manifest,
        Participant, SocketHost, SocketPayload, Transition, WebSocketId,
    },
    local::{PreparedPublish, Store, Subscription},
    sockets::socket_reply,
};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey,
    Record, StreamError, StreamPath, StreamProvider, StreamProviderExt as _,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

const INBOX: &str = "inbox";
const QUEUE: &str = "queue";
const HANDOVER: &str = "acyclic.harness.device.handover.v1";
/// Longest session a device may open with.
const MAX_SESSION_BYTES: usize = 128;

fn codec() -> CodecIdentity {
    CodecIdentity {
        name: "acyclic.harness.devices.target".into(),
        version: "1".into(),
    }
}

fn guest(error: impl std::fmt::Display) -> GuestError {
    GuestError::Handler(error.to_string())
}

/// The actor of device `name`, `targets/<name>`.
///
/// # Errors
/// When `name` is not an actor identity.
pub fn target_actor(paths: &TargetPaths, name: &str) -> Result<ActorReference> {
    paths.actor(name)
}

/// The actor's state (v1).
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    v: u32,
    root: StreamPath,
    owner: Vec<u8>,
    target: TargetRecord,
    generation: u64,
    /// The open socket and the session it opened with.
    connection: Option<(String, String)>,
    /// Last original queue position handed over in this checkpoint.
    handover: Option<u64>,
}

impl State {
    fn paths(&self) -> Result<TargetPaths> {
        Ok(TargetPaths::new(
            self.root.clone(),
            ActorReference::decode(&self.owner)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        ))
    }

    fn checkpoint(&self) -> std::result::Result<Checkpoint, GuestError> {
        Ok(Checkpoint {
            codec: codec(),
            bytes: encode(self).map_err(guest)?.into(),
        })
    }

    /// The presence record for the current connection.
    fn status(&self) -> Result<Effect> {
        let mut target = self.target.clone();
        target.state = if self.connection.is_some() {
            TargetState::Online
        } else {
            TargetState::Offline
        };
        let status = TargetStatus {
            v: RECORD_VERSION,
            target,
            session: self.connection.as_ref().map(|(_, session)| session.clone()),
            generation: self.generation,
        };
        Ok(append(
            self.paths()?.presence()?,
            AppendRecord::latest(
                encode(&status)?,
                self.target.name.clone().into_bytes(),
                false,
            )
            .map_err(Error::from)?,
        ))
    }

    fn event(&self, op: OperationId, event: RunEvent) -> Result<Effect> {
        let record = RunRecord {
            v: RECORD_VERSION,
            event,
        };
        Ok(append(
            self.paths()?.run(&self.target.name, op)?,
            AppendRecord::from(encode(&record)?),
        ))
    }

    /// One session end covers every unfinished operation without a registry.
    fn lose(&self, reason: &str, effects: &mut Vec<Effect>) -> Result<()> {
        effects.push(append(
            self.paths()?
                .session_end(&self.target.name, self.generation)?,
            AppendRecord::from(encode(&RunRecord {
                v: RECORD_VERSION,
                event: RunEvent::Lost {
                    reason: reason.to_owned(),
                },
            })?),
        ));
        Ok(())
    }

    /// One queued run: handed to the open session it names, else refused.
    async fn queued<P: StreamProvider + ?Sized>(
        &mut self,
        provider: &P,
        record: &Record,
        effects: &mut Vec<Effect>,
    ) -> Result<()> {
        // A record that is no run is no command; dispatch writes only runs.
        let Ok(run) = decode::<Run>(&record.value) else {
            return Ok(());
        };
        let op = run.command.op;
        let path = self.paths()?.run(&self.target.name, op)?;
        match provider.head(path.clone()).await {
            Ok(head) if head.tail != 0 => return Ok(()),
            Ok(_) | Err(StreamError::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        let before = effects.len();
        let accepted = self.connection.as_ref().is_some_and(|(_, session)| {
            *session == run.command.session
                && run.command.target == self.target.name
                && run.command.generation == self.generation
        }) && run.command.actor
            == *blake3::hash(&self.paths()?.actor(&self.target.name)?.encode()).as_bytes();
        if accepted {
            self.handover = Some(record.sequence);
            effects.push(self.event(
                op,
                RunEvent::Started {
                    session: run.command.session.clone(),
                    generation: self.generation,
                },
            )?);
        } else {
            effects.push(self.event(
                op,
                RunEvent::Refused {
                    reason: "the device's session ended before the command reached it; it did not run"
                        .into(),
                },
            )?);
        }
        if let Some(effect) = effects.get_mut(before) {
            effect
                .conditions
                .push(CommitCondition::Tail { path, expected: 0 });
        }
        Ok(())
    }

    /// A device report from the current session about a run it was handed.
    async fn report<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
        message: DeviceMessage,
        effects: &mut Vec<Effect>,
    ) -> Result<()> {
        let Some((_, current)) = &self.connection else {
            return Ok(());
        };
        let (op, event) = match message {
            DeviceMessage::Output { op, stderr, data } => {
                if data.is_empty() || data.len() > super::records::OUTPUT_CHUNK_BYTES {
                    return Err(Error::Invalid(
                        "device output exceeds its protective chunk".into(),
                    ));
                }
                (
                    op,
                    RunEvent::Output {
                        session: current.clone(),
                        generation: self.generation,
                        stderr,
                        data,
                    },
                )
            }
            DeviceMessage::Lost { op, reason } => (op, RunEvent::Lost { reason }),
            DeviceMessage::Exit { op, result } => (op, RunEvent::Exit { result }),
            DeviceMessage::Refused { op, reason } => (op, RunEvent::Refused { reason }),
        };
        let path = self.paths()?.run(&self.target.name, op)?;
        let head = match provider.head(path.clone()).await {
            Ok(head) => head,
            Err(StreamError::NotFound) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if let Some(last) = head.last
            && let RunRecord {
                event:
                    RunEvent::Started {
                        session,
                        generation,
                    }
                    | RunEvent::Output {
                        session,
                        generation,
                        ..
                    },
                ..
            } = decode(&last.value)?
            && generation == self.generation
            && self
                .connection
                .as_ref()
                .is_some_and(|(_, current)| *current == session)
        {
            let mut effect = self.event(op, event)?;
            effect.conditions.push(CommitCondition::Head {
                path,
                expected: head.tail,
                commit_id: last.commit_id,
            });
            effects.push(effect);
        }
        Ok(())
    }
}

fn append(path: StreamPath, record: AppendRecord) -> Effect {
    Effect {
        conditions: Vec::new(),
        mutation: CommitMutation::Append {
            path,
            records: vec![record],
        },
    }
}

/// One handler package for every device's actor.
pub struct TargetActor<P: StreamProvider + ?Sized> {
    provider: Arc<P>,
    manifest: Manifest,
}

impl<P: StreamProvider + ?Sized> TargetActor<P> {
    /// The package. Its store uses [`TargetComposer`] to send handovers only
    /// after the actor checkpoint and `Started` event have committed.
    ///
    /// # Errors
    /// When `limits` or the package digest are refused.
    pub fn new(provider: Arc<P>, limits: ActorLimits) -> Result<Self> {
        let digest = blake3::hash(include_str!("actor.rs").as_bytes());
        Ok(Self {
            provider,
            manifest: Manifest {
                code: CodeSha256::try_from(digest.as_bytes().to_vec())
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                codec: codec(),
                subscriptions: vec![INBOX.into(), QUEUE.into()],
                interrupts: Vec::new(),
                bindings: Vec::new(),
                limits,
            },
        })
    }

    async fn transition(
        &self,
        delivery: &Delivery,
        state: &mut State,
        effects: &mut Vec<Effect>,
    ) -> Result<Option<bytes::Bytes>> {
        if let (acyclic_actors::guest::Trigger::Record { subscription, .. }, Some(record)) =
            (&delivery.trigger, &delivery.record)
            && subscription == QUEUE
        {
            state
                .queued(self.provider.as_ref(), record, effects)
                .await?;
            return Ok(None);
        }
        let Some(socket) = delivery
            .socket()
            .map_err(|error| Error::Invalid(error.to_string()))?
        else {
            // The inbox carries only socket events; anything else is no command.
            return Ok(None);
        };
        let current = state
            .connection
            .as_ref()
            .is_some_and(|(connection, _)| *connection == socket.connection_id);
        match socket.event {
            GuestSocketEvent::Open { app_channel, .. } => {
                let Some(session) = app_channel.filter(|session| {
                    !session.is_empty()
                        && session.len() <= MAX_SESSION_BYTES
                        && !session.chars().any(char::is_control)
                }) else {
                    return reply(GuestEventResult::SocketReject {
                        status: 400,
                        reason: "a device opens its socket with its session".into(),
                    });
                };
                // A reconnect supersedes the previous socket: its runs are
                // lost now, so a device never waits out a dead connection.
                if state.connection.take().is_some() {
                    state.lose("the device reconnected while the command ran", effects)?;
                }
                state.generation = state
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("device generation overflows".into()))?;
                state.connection = Some((socket.connection_id, session));
                effects.push(state.status()?);
                reply(GuestEventResult::SocketAccept {
                    subprotocol: None,
                    attachment: bytes::Bytes::new(),
                    tags: Vec::new(),
                })
            }
            GuestSocketEvent::Message {
                payload: SocketPayload::Text { text },
                ..
            } if current => {
                // A malformed report is the device's error; it changes nothing.
                if let Ok(MessageRecord { message, .. }) = decode::<MessageRecord>(text.as_bytes())
                {
                    state
                        .report(self.provider.as_ref(), message, effects)
                        .await?;
                }
                Ok(None)
            }
            GuestSocketEvent::Close { code, reason, .. } if current => {
                state.connection = None;
                state.lose(
                    &format!("the device disconnected while the command ran ({code} {reason})"),
                    effects,
                )?;
                effects.push(state.status()?);
                Ok(None)
            }
            _ => Ok(None),
        }
    }
}

fn reply(verdict: GuestEventResult) -> Result<Option<bytes::Bytes>> {
    socket_reply(&verdict)
        .map(Some)
        .map_err(|error| Error::Invalid(error.to_string()))
}

impl<P: StreamProvider + ?Sized + 'static> Handler for TargetActor<P> {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        _cancellation: acyclic_actors::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            let checkpoint = checkpoint.ok_or_else(|| guest("device actor has no state"))?;
            let mut state: State = decode(&checkpoint.bytes).map_err(guest)?;
            let mut effects = Vec::new();
            let response = self
                .transition(&delivery, &mut state, &mut effects)
                .await
                .map_err(guest)?;
            Ok(Transition {
                response,
                participant: Some(Participant {
                    kind: HANDOVER.into(),
                    plan: bytes::Bytes::new(),
                }),
                checkpoint: state.checkpoint()?,
                alarm: AlarmChange::Keep,
                effects,
            })
        })
    }
}

/// Publishes device handovers through the actor's original composition.
/// No command can reach the device before its durable `Started` event.
pub struct TargetComposer {
    host: Arc<dyn SocketHost>,
}

impl TargetComposer {
    /// The same socket host that admits the device's connection.
    #[must_use]
    pub fn new(host: Arc<dyn SocketHost>) -> Self {
        Self { host }
    }
}

impl Composer for TargetComposer {
    fn open(
        &self,
        actor: &ActorReference,
        previous: Option<&Checkpoint>,
        delivered: Option<&Record>,
        next: &Checkpoint,
        participant: &Participant,
    ) -> std::result::Result<Box<dyn Composition>, GuestError> {
        if participant.kind != HANDOVER || !participant.plan.is_empty() {
            return Err(guest("not a device handover"));
        }
        let previous: State = decode(
            &previous
                .ok_or_else(|| guest("device has no previous checkpoint"))?
                .bytes,
        )
        .map_err(guest)?;
        let next: State = decode(&next.bytes).map_err(guest)?;
        let close = previous.connection.as_ref().and_then(|(connection, _)| {
            (previous.connection != next.connection).then(|| WebSocketId(connection.clone()))
        });
        let send = match (delivered, &next.connection) {
            (Some(record), Some((connection, _))) => decode::<Run>(&record.value)
                .ok()
                .filter(|run| {
                    previous.handover != next.handover
                        && next.handover == Some(record.sequence)
                        && run.command.generation == next.generation
                })
                .map(|run| {
                    let text = String::from_utf8(
                        encode(&HandoverRecord {
                            v: RECORD_VERSION,
                            sequence: record.sequence,
                            run,
                        })
                        .map_err(guest)?,
                    )
                    .map_err(guest)?;
                    Ok::<_, GuestError>((WebSocketId(connection.clone()), text))
                })
                .transpose()?,
            _ => None,
        };
        Ok(Box::new(Handover {
            host: Arc::clone(&self.host),
            actor: actor.clone(),
            close,
            send,
        }))
    }
}

struct Handover {
    host: Arc<dyn SocketHost>,
    actor: ActorReference,
    close: Option<WebSocketId>,
    send: Option<(WebSocketId, String)>,
}

impl Composition for Handover {
    fn seal<'a>(&'a mut self, request: CommitRequest) -> GuestFuture<'a, CommitRequest> {
        Box::pin(async move { Ok(request) })
    }

    fn settle<'a>(
        self: Box<Self>,
        outcome: std::result::Result<&'a CommitOutcome, &'a StreamError>,
    ) -> GuestFuture<'a, ()> {
        Box::pin(async move {
            if !matches!(outcome, Ok(CommitOutcome::Committed(_))) {
                return Ok(());
            }
            if let Some(connection) = self.close {
                // An already closed socket is the same terminal outcome.
                let _closed = self.host.web_socket_close(
                    &self.actor,
                    &connection,
                    4000,
                    "device session superseded",
                );
            }
            if let Some((connection, text)) = self.send {
                if self
                    .host
                    .web_socket_send(&self.actor, &connection, SocketPayload::Text { text })
                    .is_err()
                {
                    // Presence can precede the socket's Open verdict. A
                    // refused handover retires that original session; the
                    // accepted operation is never sent on another connection.
                    self.host.web_socket_close(
                        &self.actor,
                        &connection,
                        1011,
                        "device handover was not accepted",
                    )?;
                }
            }
            Ok(())
        })
    }
}

/// Attaches device `target` under `paths`: creates its actor in `store`,
/// listed offline until it connects. Attaching an attached device changes
/// nothing.
///
/// # Errors
/// When the record is malformed or the commit fails.
pub async fn attach<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    paths: &TargetPaths,
    mut target: TargetRecord,
) -> Result<ActorReference> {
    crate::agent::AgentRecord::check(&target).map_err(|error| Error::Invalid(error.to_string()))?;
    target.state = TargetState::Offline;
    let actor = target_actor(paths, &target.name)?;
    let state = State {
        v: RECORD_VERSION,
        root: paths.root().clone(),
        owner: paths.owner().encode(),
        target,
        generation: 0,
        connection: None,
        handover: None,
    };
    let key = IdempotencyKey::new(
        format!("{}/attach/{}", paths.presence()?, state.target.name).into_bytes(),
    )
    .map_err(Error::from)?;
    let mut prepared = PreparedPublish::foreign(key);
    for effect in store
        .create(
            &actor,
            BTreeMap::from([
                (
                    INBOX.into(),
                    Subscription {
                        path: paths.inbox(&state.target.name)?,
                        cursor: 0,
                    },
                ),
                (
                    QUEUE.into(),
                    Subscription {
                        path: paths.queue(&state.target.name)?,
                        cursor: 0,
                    },
                ),
            ]),
            Some(
                state
                    .checkpoint()
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            ),
        )
        .map_err(actors)?
    {
        prepared.join(effect).map_err(actors)?;
    }
    prepared.join(state.status()?).map_err(actors)?;
    let outcome = store
        .provider()
        .commit(prepared.into_request().map_err(actors)?)
        .await?;
    match PreparedPublish::settle(&outcome) {
        Ok(()) => Ok(actor),
        Err(acyclic_actors::local::Error::Conflict) => {
            // A concurrent attach may only reuse this exact original binding.
            let snapshot = store.load(&actor).await.map_err(actors)?;
            let checkpoint = snapshot
                .state()
                .and_then(acyclic_actors::local::State::checkpoint)
                .ok_or_else(|| Error::Storage("attached device has no checkpoint".into()))?;
            let mut existing: State = decode(&checkpoint.bytes)?;
            existing.target.state = TargetState::Offline;
            if existing.root != state.root
                || existing.owner != state.owner
                || existing.target != state.target
            {
                return Err(Error::Unauthorized(
                    "device is attached with another binding".into(),
                ));
            }
            Ok(actor)
        }
        Err(error) => Err(actors(error)),
    }
}

fn actors(error: acyclic_actors::local::Error) -> Error {
    Error::Storage(error.to_string())
}
