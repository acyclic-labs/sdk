//! The agent as an A4 actor: one [`Handler`] package serves every agent.
//!
//! Each agent's actor subscribes to its inbox (subscription [`INBOX`]). A delivery
//! runs one [`super::step`], lowers its effects into the one commit the runtime publishes
//! with the checkpoint, and maps the step's alarm onto the actor's single alarm.
//! A spawn's commit also creates the child's actor state at the fork cut, so the
//! child's first delivery is its `ForkAdopted` record. There is no other timer,
//! poller or per-agent task.

use super::checkpoint::{checkpoint_state, codec};
use super::{
    AgentId, AgentPath, AgentRecord, AgentState, Alarm, Error, ForkAdopted, InboxRecord, IntentId,
    Out, Spawn, TurnDriver, Wake, delivery_key, lower,
};
use acyclic_actors::{
    addressing::ActorReference,
    domain::{ActorLimits, CodeSha256},
    guest::{
        AlarmChange, Checkpoint, Delivery, GuestError, GuestFuture, Handler, Manifest, Transition,
        Trigger,
    },
    local::{Creation, PreparedPublish, Store, Subscription},
};
use acyclic_stream::CommitEffect;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

/// The agent's one subscription: its inbox.
pub const INBOX: &str = "inbox";

#[cfg(test)]
#[path = "handler_tests.rs"]
mod tests;

const REPLY_SUBSCRIPTION: &str = "reply-v1:";

fn reply_subscription(watch: &super::ReplyWatch) -> Result<String, Error> {
    serde_json::to_string(watch)
        .map(|value| format!("{REPLY_SUBSCRIPTION}{value}"))
        .map_err(|_| Error::Record("reply subscription does not encode"))
}

fn guest(error: impl std::fmt::Display) -> GuestError {
    GuestError::Handler(error.to_string())
}

/// One handler package for every agent, over one turn driver.
pub struct AgentHandler<P: crate::store::JournalProvider + ?Sized, D> {
    agents: Agents<P>,
    driver: D,
    manifest: Manifest,
    volumes: Option<std::sync::Arc<dyn super::SpawnVolumes>>,
    closes: bool,
    diagnostics: Mutex<BTreeMap<AgentPath, RetainedDiagnostic>>,
}

/// A failed publication keeps the computed transition, including every byte
/// not yet staged. A later attempt at the same delivery cannot call the driver
/// again or replace its error. Successful and unrelated deliveries retain none.
struct RetainedDiagnostic {
    trigger: String,
    checkpoint: Option<Checkpoint>,
    prepared: super::machine::PreparedStep,
}

/// Return original diagnostic ownership on errors and future cancellation.
struct DiagnosticAttempt<'a> {
    owner: &'a Mutex<BTreeMap<AgentPath, RetainedDiagnostic>>,
    identity: Option<(AgentPath, String, Option<Checkpoint>)>,
    prepared: Option<super::machine::PreparedStep>,
}

impl std::ops::Deref for DiagnosticAttempt<'_> {
    type Target = super::machine::PreparedStep;
    fn deref(&self) -> &Self::Target {
        self.prepared.as_ref().expect("original diagnostic attempt")
    }
}
impl std::ops::DerefMut for DiagnosticAttempt<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.prepared.as_mut().expect("original diagnostic attempt")
    }
}
impl DiagnosticAttempt<'_> {
    fn into_checkpoint(mut self) -> Checkpoint {
        self.prepared
            .take()
            .expect("original diagnostic attempt")
            .into_checkpoint()
    }
}
impl Drop for DiagnosticAttempt<'_> {
    fn drop(&mut self) {
        if let (Some(prepared), Some((path, trigger, checkpoint))) =
            (self.prepared.take(), self.identity.take())
        {
            self.owner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .insert(
                    path,
                    RetainedDiagnostic {
                        trigger,
                        checkpoint,
                        prepared,
                    },
                );
        }
    }
}

/// The agents' actor state under one store root: creation and observation,
/// shared by a handler and by a host whose turns run elsewhere.
pub(super) struct Agents<P: crate::store::JournalProvider + ?Sized> {
    store: Store<P>,
}

impl<P: crate::store::JournalProvider + ?Sized + 'static, D: TurnDriver> AgentHandler<P, D> {
    /// Handler whose actors live under `store`'s root.
    pub fn new(store: Store<P>, driver: D, limits: ActorLimits) -> Result<Self, GuestError> {
        let digest = blake3::hash(include_str!("handler.rs").as_bytes());
        Ok(Self {
            agents: Agents::new(store),
            driver,
            manifest: Manifest {
                code: CodeSha256::try_from(digest.as_bytes().to_vec()).map_err(guest)?,
                codec: codec(),
                subscriptions: vec![INBOX.into()],
                interrupts: super::records::interrupt_prefixes()
                    .map_err(guest)?
                    .into_iter()
                    .map(|prefix| acyclic_actors::guest::Interrupt {
                        subscription: INBOX.into(),
                        value_prefix: bytes::Bytes::from(prefix),
                    })
                    .collect(),
                bindings: Vec::new(),
                limits,
            },
            volumes: None,
            closes: false,
            diagnostics: Mutex::new(BTreeMap::new()),
        })
    }

    /// Asks the host, in each agent's closing step, to release what the agent
    /// holds ([`super::lifecycle::CLOSE`]) before that step commits.
    #[must_use]
    pub fn with_close(mut self) -> Self {
        self.closes = true;
        self
    }

    /// Creates spawned children's volumes through `volumes`, in each spawn's
    /// commit; without it a child binds no volumes.
    #[must_use]
    pub fn with_spawn_volumes(mut self, volumes: std::sync::Arc<dyn super::SpawnVolumes>) -> Self {
        self.volumes = Some(volumes);
        self
    }

    /// Prepares, without IO, the one commit creating root agent
    /// `adopted.agent` as an actor in `within`'s account and namespace: its
    /// inbox with `ForkAdopted`, its actor state and its activation entry.
    /// A host may join its own contributions (the root's volumes) before it
    /// submits the request once with [`super::submit`]; on
    /// [`Error::Unknown`] it resubmits the exact returned request.
    pub fn prepare_root(
        &self,
        within: &ActorReference,
        intent: &IntentId,
        adopted: ForkAdopted,
    ) -> Result<Spawn, Error> {
        self.agents.prepare_root(within, intent, adopted)
    }

    /// The actor of `agent` in `within`'s account and namespace.
    pub fn actor(within: &ActorReference, agent: &AgentId) -> Result<ActorReference, Error> {
        Agents::<P>::actor(within, agent)
    }
}

impl<P: crate::store::JournalProvider + ?Sized + 'static> Agents<P> {
    /// Over `store`'s root.
    pub(super) const fn new(store: Store<P>) -> Self {
        Self { store }
    }

    /// The same-commit effect that wakes `actor` after a write to its inbox.
    pub(super) fn wake(&self, actor: &ActorReference) -> Result<CommitEffect, Error> {
        self.store
            .activation(actor)
            .map_err(|error| Error::Actor(Box::new(error)))
    }

    /// The effects that create `agent`'s actor, dormant until its trigger:
    /// the inbox read registered at `cursor`, where the creating commit
    /// appends `ForkAdopted`, and that trigger's activation. They ride in the
    /// commit that creates the agent: a spawn or a root creation.
    pub(super) fn create(
        &self,
        actor: &ActorReference,
        agent: &AgentPath,
        cursor: u64,
    ) -> Result<[CommitEffect; 2], Error> {
        self.create_with_pause(actor, agent, cursor, false)
    }

    fn create_with_pause(
        &self,
        actor: &ActorReference,
        agent: &AgentPath,
        cursor: u64,
        paused: bool,
    ) -> Result<[CommitEffect; 2], Error> {
        let creation = Creation {
            escrow: paused.then(|| acyclic_actors::escrow::Escrow::new(Some(0))),
            subscriptions: BTreeMap::from([(
                INBOX.to_owned(),
                Subscription {
                    path: agent.inbox()?,
                    cursor,
                },
            )]),
            ..Creation::default()
        };
        let state = if paused {
            self.store.create_paused(None, actor, creation)
        } else {
            self.store.create(None, actor, creation)
        }
        .map_err(|_| Error::Record("agent actor state cannot be created"))?;
        Ok([state, self.wake(actor)?])
    }

    /// One commit creating root agent `adopted.agent` as an actor in `within`'s
    /// account and namespace: its inbox with `ForkAdopted`, its actor state and
    /// its activation entry. The caller adds volume contributions, then submits.
    pub(super) fn prepare_root(
        &self,
        within: &ActorReference,
        intent: &IntentId,
        adopted: ForkAdopted,
    ) -> Result<Spawn, Error> {
        self.prepare_root_with_peer(within, intent, adopted, true)
    }

    pub(super) fn prepare_root_with_peer(
        &self,
        within: &ActorReference,
        intent: &IntentId,
        adopted: ForkAdopted,
        peer: bool,
    ) -> Result<Spawn, Error> {
        let created = super::create_root_with_peer(intent, adopted, peer)?;
        self.root_actor(within, created, false)
    }

    pub(super) fn prepare_root_history(
        &self,
        within: &ActorReference,
        intent: &IntentId,
        adopted: ForkAdopted,
        source: &AgentPath,
        history: &super::HistorySelection,
        peer: bool,
    ) -> Result<Spawn, Error> {
        let created =
            super::commit::create_root_at(intent, adopted, Some((source, history)), peer)?;
        self.root_actor(within, created, true)
    }

    fn root_actor(
        &self,
        within: &ActorReference,
        created: Spawn,
        paused: bool,
    ) -> Result<Spawn, Error> {
        let actor = Self::actor(within, &created.child.id())?;
        let mut publish = PreparedPublish::foreign(created.request.idempotency_key.clone());
        let effects = created
            .request
            .effects
            .into_iter()
            .chain(self.create_with_pause(&actor, &created.child, 0, paused)?);
        for effect in effects {
            publish
                .join(effect)
                .map_err(|_| Error::Record("agent creation does not compose"))?;
        }
        Ok(Spawn {
            child: created.child,
            request: publish
                .into_request()
                .map_err(|_| Error::Record("agent creation lost publication custody"))?,
        })
    }

    /// The actor of `agent` in `actor`'s account and namespace.
    pub(super) fn actor(within: &ActorReference, agent: &AgentId) -> Result<ActorReference, Error> {
        ActorReference::new(
            within.account().as_bytes(),
            within.namespace().as_str().as_bytes(),
            agent.as_str().as_bytes(),
        )
        .map_err(|_| Error::Path("agent id is not an actor id"))
    }

    /// The committed agent state and the inbox cursor its last step consumed
    /// before; before the first step, the adoption at its own cursor.
    pub(super) async fn state(
        &self,
        actor: &ActorReference,
        agent: &AgentPath,
    ) -> Result<(AgentState, u64), Error> {
        let snapshot = self
            .store
            .load(actor)
            .await
            .map_err(|error| Error::Actor(Box::new(error)))?;
        let state = snapshot.state().ok_or(Error::NoSuchAgent)?;
        let inbox = agent.inbox()?;
        let cursor = state
            .subscriptions()
            .get(INBOX)
            .filter(|subscription| subscription.path == inbox)
            .ok_or(Error::Record("agent inbox subscription differs"))?
            .cursor;
        let mut observed = if let Some(checkpoint) = state.checkpoint() {
            checkpoint_state(self.store.provider().as_ref(), checkpoint).await?
        } else {
            // A new child's inherited inbox starts with its ancestors' records.
            // Its own adoption is at the cursor installed by the spawn commit.
            let from = cursor;
            let records =
                super::input::read_back(self.store.provider().as_ref(), agent, from, 1, u64::MAX)
                    .await?;
            let [(sequence, InboxRecord::ForkAdopted(adopted))] = records.as_slice() else {
                return Err(Error::Record("agent has no adoption record"));
            };
            if *sequence != from
                || adopted.forked_at != from
                || adopted.agent != agent.id()
                || !agent.parent_is(adopted.parent.as_ref())
            {
                return Err(Error::Record("agent adoption cursor differs"));
            }
            AgentState::adopted(agent.clone(), adopted)?
        };
        if &observed.path != agent || !agent.parent_is(observed.parent.as_ref()) {
            return Err(Error::Record("agent state binding differs"));
        }
        observed
            .open_children
            .bind(Arc::clone(self.store.provider()));
        observed
            .archive_pending
            .bind(Arc::clone(self.store.provider()));
        observed.awaiting.bind(Arc::clone(self.store.provider()));
        observed
            .topology_refusals
            .bind(Arc::clone(self.store.provider()));
        observed
            .spawned_children
            .bind(Arc::clone(self.store.provider()));
        Ok((observed, cursor))
    }
}

impl<P: crate::store::JournalProvider + ?Sized + 'static, D: TurnDriver> AgentHandler<P, D> {
    async fn transition(
        &self,
        delivery: Delivery,
        checkpoint: Option<&Checkpoint>,
        cancellation: acyclic_actors::guest::CancellationToken,
        failure: Option<&str>,
    ) -> Result<Transition, Error> {
        let saved = match checkpoint {
            Some(checkpoint) => {
                Some(checkpoint_state(self.agents.store.provider().as_ref(), checkpoint).await?)
            }
            None => None,
        };
        let cut = delivery
            .subscriptions
            .get(INBOX)
            .ok_or(Error::Record("agent inbox subscription missing"))?;
        let settled_at_micros = delivery
            .record
            .as_ref()
            .map(|record| record.committed_at_micros);
        let mut completed_reply = None;
        let (mut wake, cursor, trigger) = match (&delivery.trigger, delivery.record) {
            (
                Trigger::Record {
                    subscription,
                    sequence,
                },
                Some(record),
            ) if subscription == INBOX
                && *sequence == cut.cursor
                && record.sequence == *sequence =>
            {
                (
                    Wake::Inbox {
                        sequence: *sequence,
                        record: InboxRecord::decode(&record.value)?,
                    },
                    sequence
                        .checked_add(1)
                        .ok_or(Error::Record("agent inbox cursor overflow"))?,
                    format!("record:{sequence}"),
                )
            }
            (Trigger::Alarm { generation, .. }, None) => {
                (Wake::Alarm, cut.cursor, format!("alarm:{generation}"))
            }
            (
                Trigger::Record {
                    subscription,
                    sequence,
                },
                Some(record),
            ) if subscription.starts_with(REPLY_SUBSCRIPTION) => {
                let watch: super::ReplyWatch = serde_json::from_str(
                    subscription
                        .strip_prefix(REPLY_SUBSCRIPTION)
                        .expect("reply prefix"),
                )
                .map_err(|_| Error::Record("reply subscription is malformed"))?;
                let bound = delivery
                    .subscriptions
                    .get(subscription)
                    .ok_or(Error::Record("reply subscription is absent"))?;
                if bound.path != watch.to.status()?
                    || bound.cursor != *sequence
                    || record.sequence != *sequence
                    || !saved.as_ref().is_some_and(|state| {
                        state.turn.is_some() && matches!(state.phase, super::Phase::Waiting { .. })
                    })
                {
                    return Err(Error::Record("reply delivery differs from its wait"));
                }
                super::StatusRecord::decode(&record.value)?;
                let actor = Agents::<P>::actor(&delivery.actor, &watch.to.id())?;
                let (mut state, cursor) = self.agents.state(&actor, &watch.to).await?;
                let settled = matches!(
                    super::reply(
                        self.agents.store.provider().as_ref(),
                        &mut state,
                        cursor,
                        &watch.id,
                        watch.inbox
                    )
                    .await?,
                    super::Reply::Answered { .. } | super::Reply::Dropped
                );
                if settled {
                    completed_reply = Some(subscription.clone());
                }
                (
                    Wake::Reply { settled },
                    cut.cursor,
                    format!("{subscription}:{sequence}"),
                )
            }
            _ => return Err(Error::Record("delivery does not match its trigger")),
        };
        let path = match (&saved, &wake) {
            (Some(state), _) => state.path.clone(),
            (
                None,
                Wake::Inbox {
                    record: InboxRecord::ForkAdopted(adopted),
                    ..
                },
            ) => match &adopted.parent {
                Some(parent) => parent.child(&adopted.agent)?,
                None => AgentPath::root(&adopted.agent),
            },
            (None, _) => return Err(Error::Record("first agent input is not ForkAdopted")),
        };
        if delivery.actor.id() != path.id().as_str() {
            return Err(Error::Record("agent actor identity differs"));
        }
        if cut.path != path.inbox()? {
            return Err(Error::Record("agent inbox subscription differs"));
        }
        let was_open = saved.as_ref().is_none_or(|state| !state.closed);
        let previously_steered = saved.as_ref().map_or(0, |state| state.steered_through);
        let mut state = saved;
        if let Some(state) = state.as_mut().filter(|_| failure.is_none()) {
            // Overflowed input precedes this delivery; read it back first.
            let end = match &wake {
                Wake::Inbox { sequence, .. } => *sequence,
                Wake::Alarm | Wake::Rejected | Wake::Reply { .. } => cursor,
            };
            super::input::refill(self.agents.store.provider().as_ref(), state, end).await?;
        }
        if let Wake::Inbox {
            record: InboxRecord::Cancel(cancel),
            ..
        } = &wake
            && !super::cancellation::accepted(self.agents.store.provider().as_ref(), &path, cancel)
                .await?
        {
            wake = Wake::Rejected;
        }
        if let Some(state) = state.as_mut() {
            state
                .open_children
                .bind(Arc::clone(self.agents.store.provider()));
            state
                .archive_pending
                .bind(Arc::clone(self.agents.store.provider()));
            state
                .awaiting
                .bind(Arc::clone(self.agents.store.provider()));
            state
                .topology_refusals
                .bind(Arc::clone(self.agents.store.provider()));
            state
                .spawned_children
                .bind(Arc::clone(self.agents.store.provider()));
        }
        let retained = self
            .diagnostics
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&path)
            .filter(|retained| {
                retained.trigger == trigger && retained.checkpoint.as_ref() == checkpoint
            });
        let prepared = if let Some(retained) = retained {
            retained.prepared
        } else if let Some(failure) = failure {
            super::machine::failed_step(&path, state, wake, cursor, failure).await?
        } else {
            super::machine::prepare_step(
                &super::cancellation::Driver {
                    provider: self.agents.store.provider().as_ref(),
                    inner: &self.driver,
                },
                &path,
                state,
                (cursor, delivery.now_unix_ms, settled_at_micros),
                wake,
                cancellation,
            )
            .await?
        };
        let diagnostic = prepared
            .step()
            .outs
            .iter()
            .any(|out| matches!(out, Out::Diagnostic { .. }));
        let mut prepared = DiagnosticAttempt {
            owner: &self.diagnostics,
            identity: diagnostic.then(|| (path.clone(), trigger.clone(), checkpoint.cloned())),
            prepared: Some(prepared),
        };
        for out in &prepared.step().outs {
            if let Out::Send {
                to,
                record: InboxRecord::Cancel(cancel),
            } = out
            {
                super::cancellation::validate(self.agents.store.provider().as_ref(), to, cancel)
                    .await?;
            }
            if let Out::Diagnostic { diagnostic, text } = out {
                diagnostic
                    .stage(self.agents.store.provider().as_ref(), text)
                    .await?;
            }
        }
        prepared
            .persist_indexes(Arc::clone(self.agents.store.provider()), checkpoint)
            .await?;
        let participant = match &self.volumes {
            Some(volumes) => {
                let (state, outs) = prepared.state_and_outs();
                volumes.bind(state, outs)?
            }
            None => None,
        };
        let participant = match participant {
            _ if !self.closes || !was_open || !prepared.state_and_outs().0.closed => participant,
            participant => Some(super::lifecycle::close_participant(&path, participant)?),
        };
        let step = prepared.step();
        let mut request = lower(step, cursor, delivery_key(&step.state, &trigger)?)?;
        let retired = step.state.turn.as_ref().and_then(|turn| {
            step.outs.iter().find_map(|out| match out {
                Out::Steered(through) => Some(super::BoundaryCut {
                    from: previously_steered.max(turn.inbox.saturating_add(1)),
                    through: *through,
                }),
                _ => None,
            })
        });
        let guarded = super::cancellation::complete(
            self.agents.store.provider().as_ref(),
            &path,
            &step.outs,
            retired,
            step.state.conversation_tail,
            &mut request,
        )
        .await?;
        let participant = if guarded {
            Some(super::cancellation::retirement_participant(participant)?)
        } else {
            participant
        };
        let mut effects = request.effects;
        let spawned: BTreeSet<_> = step
            .outs
            .iter()
            .filter_map(|out| match out {
                Out::Spawn { intent, .. } => Some(AgentId::derive(&path.id(), intent.as_str())),
                _ => None,
            })
            .collect();
        for child in spawned {
            let actor = Agents::<P>::actor(&delivery.actor, &child)?;
            effects.extend(self.agents.create(&actor, &path.child(&child)?, cursor)?);
        }
        // Every inbox this commit writes wakes its agent through the one
        // activation follow; no dormant agent keeps a follow of its own.
        let woken: BTreeSet<_> = step
            .outs
            .iter()
            .filter_map(|out| match out {
                Out::Send { to, .. } => Some(to.id()),
                Out::Archive { target, .. } => Some(target.id()),
                _ => None,
            })
            .collect();
        for agent in woken {
            effects.push(
                self.agents
                    .wake(&Agents::<P>::actor(&delivery.actor, &agent)?)
                    .map_err(guest)?,
            );
        }
        let alarm = step.alarm;
        let replacement = step.outs.iter().rev().find_map(|out| match out {
            Out::AwaitReplies(watches) => Some(watches),
            _ => None,
        });
        let waiting =
            step.state.turn.is_some() && matches!(step.state.phase, super::Phase::Waiting { .. });
        let mut subscriptions = Vec::new();
        for (id, _) in delivery.subscriptions.iter() {
            if id.starts_with(REPLY_SUBSCRIPTION)
                && (!waiting || replacement.is_some() || completed_reply.as_ref() == Some(id))
            {
                subscriptions
                    .push(acyclic_actors::guest::SubscriptionChange::Remove { id: id.clone() });
            }
        }
        if waiting && let Some(watches) = replacement {
            for watch in watches {
                subscriptions.push(acyclic_actors::guest::SubscriptionChange::Add {
                    id: reply_subscription(watch)?,
                    path: watch.to.status()?,
                    from: Some(watch.from),
                });
            }
        }
        Ok(Transition {
            response: None,
            participant,
            checkpoint: prepared.into_checkpoint(),
            alarm: match alarm {
                Alarm::Clear => AlarmChange::Clear,
                Alarm::At(deadline_unix_ms) => AlarmChange::Set { deadline_unix_ms },
            },
            effects,
            subscriptions,
        })
    }
}

impl<P, D> Handler for AgentHandler<P, D>
where
    P: crate::store::JournalProvider + ?Sized + 'static,
    D: TurnDriver + 'static,
{
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn accepts_interrupt<'a>(
        &'a self,
        delivery: &'a Delivery,
        checkpoint: Option<&'a Checkpoint>,
        subscription: &'a str,
        record: &'a acyclic_stream::Record,
    ) -> GuestFuture<'a, bool> {
        Box::pin(async move {
            if subscription != INBOX {
                return Ok(false);
            }
            match InboxRecord::decode(&record.value).map_err(guest)? {
                InboxRecord::Archive(_) => Ok(true),
                InboxRecord::Cancel(cancel) => {
                    let mut head = checkpoint
                        .map(super::checkpoint_head)
                        .transpose()
                        .map_err(guest)?;
                    if let Some(head) = head.as_mut()
                        && head.state.turn.is_none()
                        && head.state.queued.is_empty()
                    {
                        let end = delivery
                            .subscriptions
                            .get(INBOX)
                            .ok_or_else(|| guest("agent inbox subscription missing"))?
                            .cursor;
                        super::input::refill(
                            self.agents.store.provider().as_ref(),
                            &mut head.state,
                            end,
                        )
                        .await
                        .map_err(guest)?;
                    }
                    let state = head.as_ref().map(|head| &head.state);
                    let active = state
                        .and_then(|state| state.turn.as_ref().map(|turn| turn.inbox))
                        .or_else(|| {
                            state.and_then(|state| state.queued.front().map(|input| input.inbox))
                        })
                        .or_else(|| {
                            delivery.record.as_ref().and_then(|record| {
                                let input = InboxRecord::decode(&record.value).ok()?;
                                super::input::input(record.sequence, input).map(|input| input.inbox)
                            })
                        });
                    if active != Some(cancel.input) {
                        return Ok(false);
                    }
                    let inbox = &delivery
                        .subscriptions
                        .get(INBOX)
                        .ok_or_else(|| guest("agent inbox subscription missing"))?
                        .path;
                    let path = AgentPath::try_from(inbox.as_str().to_owned()).map_err(guest)?;
                    super::cancellation::accepted(
                        self.agents.store.provider().as_ref(),
                        &path,
                        &cancel,
                    )
                    .await
                    .map_err(guest)
                }
                _ => Ok(false),
            }
        })
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: acyclic_actors::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            self.transition(delivery, checkpoint, cancellation, None)
                .await
                .map_err(|error| match error {
                    Error::IncompatibleCheckpointCodec => GuestError::IncompatibleCodec,
                    Error::Stream(error) => guest(format!("reading agent inbox replay: {error}")),
                    error => guest(error),
                })
        })
    }

    fn on_failure<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        failure: String,
    ) -> GuestFuture<'a, Option<Transition>> {
        Box::pin(async move {
            self.transition(
                delivery,
                checkpoint,
                acyclic_actors::guest::CancellationToken::new(),
                Some(&failure),
            )
            .await
            .map(Some)
            .map_err(guest)
        })
    }
}
