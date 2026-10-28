//! The agent as an A4 actor: one [`Handler`] package serves every agent.
//!
//! Each agent's actor subscribes to its inbox (subscription [`INBOX`]). A delivery
//! runs one [`step`], lowers its effects into the one commit the runtime publishes
//! with the checkpoint, and maps the step's alarm onto the actor's single alarm.
//! A spawn's commit also creates the child's actor state at the fork cut, so the
//! child's first delivery is its `ForkAdopted` record. There is no other timer,
//! poller or per-agent task.

use super::checkpoint::{self, checkpoint_state, codec, decode};
use super::{
    AgentId, AgentPath, AgentRecord, AgentState, Alarm, Error, ForkAdopted, InboxRecord, IntentId,
    Out, Spawn, TurnDriver, Wake, create_root, delivery_key, lower, step,
};
use acyclic_actors::{
    addressing::ActorReference,
    domain::{ActorLimits, CodeSha256},
    guest::{
        AlarmChange, Checkpoint, Delivery, Effect, GuestError, GuestFuture, Handler, Manifest,
        Transition, Trigger,
    },
    local::{PreparedPublish, Store, Subscription},
};
use acyclic_stream::{CommitRequest, ReadRequest, StreamProvider};
use futures::TryStreamExt;
use std::collections::{BTreeMap, BTreeSet};

/// The agent's one subscription: its inbox.
pub const INBOX: &str = "inbox";

fn guest(error: impl std::fmt::Display) -> GuestError {
    GuestError::Handler(error.to_string())
}

/// One handler package for every agent, over one turn driver.
pub struct AgentHandler<P: StreamProvider + ?Sized, D> {
    agents: Agents<P>,
    driver: D,
    manifest: Manifest,
    volumes: Option<std::sync::Arc<dyn super::SpawnVolumes>>,
}

/// The agents' actor state under one store root: creation and observation,
/// shared by a handler and by a host whose turns run elsewhere.
pub(super) struct Agents<P: StreamProvider + ?Sized> {
    store: Store<P>,
}

impl<P: StreamProvider + ?Sized + 'static, D: TurnDriver> AgentHandler<P, D> {
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
                interrupts: [b"{\"cancel\":".as_slice(), b"{\"archive\":".as_slice()]
                    .into_iter()
                    .map(|prefix| acyclic_actors::guest::Interrupt {
                        subscription: INBOX.into(),
                        value_prefix: bytes::Bytes::copy_from_slice(prefix),
                    })
                    .collect(),
                bindings: Vec::new(),
                limits,
            },
            volumes: None,
        })
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

impl<P: StreamProvider + ?Sized + 'static> Agents<P> {
    /// Over `store`'s root.
    pub(super) const fn new(store: Store<P>) -> Self {
        Self { store }
    }

    /// The same-commit effect that wakes `actor` after a write to its inbox.
    pub(super) fn wake(&self, actor: &ActorReference) -> Effect {
        self.store.activation(actor)
    }

    /// The effects that create `agent`'s actor, first consuming inbox record `cursor`.
    /// They ride in the commit that creates the agent: a spawn or a root creation.
    pub(super) fn create(
        &self,
        actor: &ActorReference,
        agent: &AgentPath,
        cursor: u64,
    ) -> Result<[Effect; 2], Error> {
        let subscriptions = BTreeMap::from([(
            INBOX.to_owned(),
            Subscription {
                path: agent.inbox()?,
                cursor,
            },
        )]);
        self.store
            .create(actor, subscriptions, None)
            .map_err(|_| Error::Record("agent actor state cannot be created"))
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
        let created = create_root(intent, adopted)?;
        let actor = Self::actor(within, &created.child.id())?;
        let mut publish = PreparedPublish::foreign(created.request.idempotency_key.clone());
        let effects =
            effects(created.request)
                .into_iter()
                .chain(self.create(&actor, &created.child, 0)?);
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
        let observed = if let Some(checkpoint) = state.checkpoint() {
            checkpoint_state(checkpoint)?
        } else {
            // A new child's inherited inbox starts with its ancestors' records.
            // Its own adoption is at the cursor installed by the spawn commit.
            let from = cursor;
            let records = self.read_back(agent, from, 1, u64::MAX).await?;
            let [(sequence, InboxRecord::ForkAdopted(adopted))] = records.as_slice() else {
                return Err(Error::Record("agent has no adoption record"));
            };
            if *sequence != from
                || adopted.forked_at != from
                || adopted.agent != agent.id()
                || adopted.parent != agent.parent()
            {
                return Err(Error::Record("agent adoption cursor differs"));
            }
            AgentState::adopted(agent.clone(), adopted)
        };
        if &observed.path != agent || observed.parent != agent.parent() {
            return Err(Error::Record("agent state binding differs"));
        }
        Ok((observed, cursor))
    }

    /// Inbox records `[from, end)`, at most `limit`, in sequence order.
    async fn read_back(
        &self,
        agent: &AgentPath,
        from: u64,
        limit: u32,
        end: u64,
    ) -> Result<Vec<(u64, InboxRecord)>, Error> {
        let request = ReadRequest {
            key: None,
            path: agent.inbox()?,
            from,
            limit,
            at_least: Default::default(),
        };
        let mut records = self
            .store
            .provider()
            .read(request)
            .await
            .map_err(Error::Stream)?;
        let mut read = Vec::new();
        while let Some(record) = records.try_next().await.map_err(Error::Stream)? {
            if record.sequence >= end {
                break;
            }
            read.push((record.sequence, InboxRecord::decode(&record.value)?));
        }
        Ok(read)
    }
}

impl<P: StreamProvider + ?Sized + 'static, D: TurnDriver> AgentHandler<P, D> {
    async fn transition(
        &self,
        delivery: Delivery,
        checkpoint: Option<&Checkpoint>,
        cancellation: acyclic_actors::guest::CancellationToken,
    ) -> Result<Transition, Error> {
        let saved = checkpoint
            .map(|checkpoint| {
                if checkpoint.codec != codec() {
                    return Err(Error::Record("agent checkpoint codec differs"));
                }
                decode(&checkpoint.bytes)
            })
            .transpose()?;
        let cut = delivery
            .subscriptions
            .get(INBOX)
            .ok_or(Error::Record("agent inbox subscription missing"))?;
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
        if cut.path != path.inbox()? {
            return Err(Error::Record("agent inbox subscription differs"));
        }
        // A spawned child names the brief its private volume was created with.
        if let (
            None,
            Some(volumes),
            Wake::Inbox {
                record: InboxRecord::ForkAdopted(adopted),
                ..
            },
        ) = (&saved, &self.volumes, &mut wake)
            && adopted.parent.is_some()
            && adopted.brief.is_none()
        {
            let brief = volumes.brief(&path, &adopted.volumes).await?;
            adopted.brief = brief;
        }
        let mut state = saved;
        if let Some(state) = state.as_mut() {
            // Overflowed input precedes this delivery; read it back first.
            let end = match &wake {
                Wake::Inbox { sequence, .. } => *sequence,
                Wake::Alarm => cursor,
            };
            if let Some((from, limit)) = state.overflowed(end) {
                let records = self.agents.read_back(&path, from, limit, end).await?;
                state.refill(records, end);
            }
        }
        let mut step = step(&self.driver, &path, state, cursor, wake, cancellation).await?;
        let participant = match &self.volumes {
            Some(volumes) => volumes.bind(&step.state, &mut step.outs)?,
            None => None,
        };
        let request = lower(&step, cursor, delivery_key(&step.state, &trigger)?)?;
        let mut effects = effects(request);
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
                    .wake(&Agents::<P>::actor(&delivery.actor, &agent)?),
            );
        }
        Ok(Transition {
            response: None,
            participant,
            checkpoint: checkpoint::checkpoint(&step.state)?,
            alarm: match step.alarm {
                Alarm::Clear => AlarmChange::Clear,
                Alarm::At(deadline_unix_ms) => AlarmChange::Set { deadline_unix_ms },
            },
            effects,
        })
    }
}

/// The lowered commit as effects: the first carries every condition, so the
/// runtime's join keeps them sorted and unique by path.
fn effects(request: CommitRequest) -> Vec<Effect> {
    let mut conditions = Some(request.conditions);
    request
        .mutations
        .into_iter()
        .map(|mutation| Effect {
            conditions: conditions.take().unwrap_or_default(),
            mutation,
        })
        .collect()
}

impl<P, D> Handler for AgentHandler<P, D>
where
    P: StreamProvider + ?Sized + 'static,
    D: TurnDriver + 'static,
{
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: acyclic_actors::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            self.transition(delivery, checkpoint, cancellation)
                .await
                .map_err(guest)
        })
    }
}
