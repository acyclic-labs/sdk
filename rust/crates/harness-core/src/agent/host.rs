//! The one agent entry point every host shares: local, wasm, providers and native.
//!
//! An [`AgentHost`] owns one A4 [`Runtime`] for every agent in one account and
//! namespace. Every control is one agent commit: create a root agent, send a
//! record (prompt, cancel, approval, hook) or archive. Agents never run outside
//! the runtime: a commit to an inbox wakes its actor through the actor's follow.
//! Observation needs no API: an agent's status and conversation are streams
//! ([`AgentPath::status`], [`AgentPath::conversation`]) any client follows.

use super::{
    AgentHandler, AgentPath, Error, ForkAdopted, InboxRecord, IntentId, Sender, Spawn,
    SpawnVolumes, Submitted, TurnDriver, archive, handler::Agents, inbox_tail, send, submit,
};
use acyclic_actors::{
    addressing::ActorReference,
    domain::ActorLimits,
    guest::{Composer, Handler},
    local::{Clock, Runtime, Store},
};
use acyclic_stream::{
    CommitCondition, CommitRequest, CommittedMutation, StreamPath, StreamProvider,
};
use std::{convert::Infallible, sync::Arc};

/// Outcome of one control commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Delivered {
    /// Committed exactly once (an exact replay also reports this) at inbox
    /// sequence `inbox`. A message's settled turn carries the same sequence in
    /// its [`super::TurnInput::Message`]; `StatusRecord::cursor > inbox` means
    /// the agent has consumed it.
    Committed {
        /// Inbox sequence of the delivered record, identical on replay.
        inbox: u64,
    },
    /// The recipient is archived; nothing was written.
    Closed,
}

/// One runtime for every agent in one account and namespace.
pub struct AgentHost<P: StreamProvider + ?Sized + 'static> {
    provider: Arc<P>,
    agents: Agents<P>,
    runtime: Runtime<P>,
    within: ActorReference,
}

/// The agents' actor store in `provider`'s domain, under [`super::ACTORS`].
pub fn actor_store<P: StreamProvider + ?Sized + 'static>(
    provider: &Arc<P>,
) -> Result<Store<P>, Error> {
    let root = StreamPath::new(super::ACTORS).map_err(|_| Error::Path("actor root"))?;
    Ok(Store::new(provider.clone(), root))
}

impl<P: StreamProvider + ?Sized + 'static> AgentHost<P> {
    /// Host for the agents of `within`'s account and namespace, with actor state
    /// under [`super::ACTORS`] in `provider`'s consistency domain.
    pub fn new<D: TurnDriver + 'static>(
        provider: Arc<P>,
        driver: D,
        within: ActorReference,
        limits: ActorLimits,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        Self::build(provider, driver, within, limits, clock, None)
    }

    /// Host whose spawns create each child's volumes through `volumes`, in the
    /// spawn's commit; `volumes` must place them in `provider`'s domain.
    pub fn with_spawn_volumes<D: TurnDriver + 'static>(
        provider: Arc<P>,
        driver: D,
        within: ActorReference,
        limits: ActorLimits,
        clock: Arc<dyn Clock>,
        volumes: Arc<dyn SpawnVolumes>,
    ) -> Result<Self, Error> {
        Self::build(provider, driver, within, limits, clock, Some(volumes))
    }

    fn build<D: TurnDriver + 'static>(
        provider: Arc<P>,
        driver: D,
        within: ActorReference,
        limits: ActorLimits,
        clock: Arc<dyn Clock>,
        volumes: Option<Arc<dyn SpawnVolumes>>,
    ) -> Result<Self, Error> {
        let handler = AgentHandler::new(actor_store(&provider)?, driver, limits)
            .map_err(|_| Error::Record("agent handler manifest"))?;
        let handler = match &volumes {
            Some(volumes) => handler.with_spawn_volumes(volumes.clone()),
            None => handler,
        };
        let composer = volumes.map(|volumes| volumes as Arc<dyn Composer>);
        Self::hosting(provider, within, Arc::new(handler), clock, composer)
    }

    /// Host for agents whose turns run in `handler`, such as the agent guest
    /// artifact's [`AgentHandler`] reached through [`acyclic_actors::guest::wire`].
    /// This host keeps scheduling and publication; `composer` opens each
    /// transition's participant request, as [`SpawnVolumes`] does.
    pub fn hosting(
        provider: Arc<P>,
        within: ActorReference,
        handler: Arc<dyn Handler>,
        clock: Arc<dyn Clock>,
        composer: Option<Arc<dyn Composer>>,
    ) -> Result<Self, Error> {
        let published = match composer {
            Some(composer) => actor_store(&provider)?.with_composer(composer),
            None => actor_store(&provider)?,
        };
        Ok(Self {
            agents: Agents::new(actor_store(&provider)?),
            // Agents of other bindings may share this stream's actor root;
            // this host claims only its own account and namespace.
            runtime: Runtime::new(published, handler, clock).claiming(&within),
            provider,
            within,
        })
    }

    /// The shared runtime, for deterministic hosts that `drain` it.
    pub const fn runtime(&self) -> &Runtime<P> {
        &self.runtime
    }

    /// The shared provider session.
    pub const fn provider(&self) -> &Arc<P> {
        &self.provider
    }

    /// Host every agent until the runtime fails: one activation follow and one
    /// timer for all agents. Per-agent failures go to `report` and stay queued.
    pub async fn run(
        &self,
        report: impl Fn(&ActorReference, &acyclic_actors::local::Error),
    ) -> Result<Infallible, acyclic_actors::local::Error> {
        self.runtime.run(report).await
    }

    /// The actor that runs `agent`.
    pub fn actor(&self, agent: &AgentPath) -> Result<ActorReference, Error> {
        Agents::<P>::actor(&self.within, &agent.id())
    }

    /// Observes the committed agent state, or its adoption before its first turn.
    /// This is an observation; effect admission still requires its own commit fence.
    pub async fn state(&self, agent: &AgentPath) -> Result<super::AgentState, Error> {
        self.agents.state(&self.actor(agent)?, agent).await
    }

    /// Prepares, without IO, the one commit creating root agent
    /// `adopted.agent` in this host's account and namespace (see
    /// [`AgentHandler::prepare_root`](super::AgentHandler::prepare_root)).
    /// [`Self::create`] is exactly this request submitted once.
    pub fn prepare_root(&self, intent: &IntentId, adopted: ForkAdopted) -> Result<Spawn, Error> {
        self.agents.prepare_root(&self.within, intent, adopted)
    }

    /// Create root agent `adopted.agent` in one commit, and start hosting it.
    /// Retrying the same `intent` replays exactly and names the same agent.
    pub async fn create(
        &self,
        intent: &IntentId,
        adopted: ForkAdopted,
    ) -> Result<AgentPath, Error> {
        let created = self.prepare_root(intent, adopted)?;
        match submit(&*self.provider, created.request).await? {
            Submitted::Committed(_) => {}
            Submitted::Conflict(_) | Submitted::InboxMoved(_) | Submitted::RecipientClosed(_) => {
                return Err(Error::Record("agent exists with another creation"));
            }
        }
        self.host(&created.child)?;
        Ok(created.child)
    }

    /// Append `record` to `to`'s inbox in one conditional commit: a prompt
    /// (`Message`), `Cancel`, an `Approval` response or a `Hook`. The key binds
    /// recipient, sender and record intent, so resending an intent never
    /// appends it twice. Changed bytes under the same intent are a definitive
    /// idempotency mismatch. On [`Error::Unknown`], retain and retry the exact
    /// returned request with [`submit`].
    pub async fn send(
        &self,
        to: &AgentPath,
        from: &Sender,
        record: &InboxRecord,
    ) -> Result<Delivered, Error> {
        self.deliver(to, |tail| send(to, tail, from, record)).await
    }

    /// Close `target` and, through its cascade, its open descendants.
    pub async fn archive(
        &self,
        target: &AgentPath,
        by: &Sender,
        id: &IntentId,
    ) -> Result<Delivered, Error> {
        self.deliver(target, |tail| archive(target, tail, by, id))
            .await
    }

    /// Durably pause or resume one agent. A paused agent consumes nothing.
    pub async fn pause(&self, agent: &AgentPath, paused: bool) -> Result<(), Error> {
        let actor = self.actor(agent)?;
        self.runtime.track(actor.clone()).map_err(actor_error)?;
        self.runtime
            .pause(&actor, paused)
            .await
            .map_err(actor_error)
    }

    /// Host an agent created elsewhere (for example after reopening), so its
    /// pending input runs without waiting for activation discovery.
    pub fn host(&self, agent: &AgentPath) -> Result<(), Error> {
        let actor = self.actor(agent)?;
        self.runtime.track(actor.clone()).map_err(actor_error)?;
        self.runtime.wake(&actor);
        Ok(())
    }

    /// Runs one agent's committed inputs to quiescence within this event.
    /// Uses finite reads, so no subscription transport wake is required.
    pub async fn drain(&self, agent: &AgentPath) -> Result<(), Error> {
        self.host(agent)?;
        self.runtime
            .drain_actor(&self.actor(agent)?)
            .await
            .map_err(actor_error)
    }

    /// One inbox commit, independent of the observed inbox tail. The tail read
    /// rejects an absent agent; an existing inbox cannot disappear. The closed
    /// marker fences admission in the commit itself. Removing only the inbox
    /// Tail condition makes exact intent retries identical after other appends,
    /// while changed semantic bytes still fail the provider's digest check.
    async fn deliver(
        &self,
        to: &AgentPath,
        request: impl Fn(u64) -> Result<CommitRequest, Error>,
    ) -> Result<Delivered, Error> {
        loop {
            let inbox = to.inbox()?;
            let tail = inbox_tail(&*self.provider, to).await?;
            let mut request = request(tail)?;
            request.conditions.retain(|condition| {
                !matches!(condition, CommitCondition::Tail { path, .. } if path == &inbox)
            });
            match submit(&*self.provider, request).await {
                Ok(Submitted::Committed(envelope)) => {
                    return envelope
                        .mutations
                        .iter()
                        .find_map(|mutation| match mutation {
                            CommittedMutation::Append(append) if append.path == inbox => {
                                Some(Delivered::Committed {
                                    inbox: append.start,
                                })
                            }
                            _ => None,
                        })
                        .ok_or(Error::Record("committed envelope omits the inbox append"));
                }
                Err(error) => return Err(error),
                Ok(Submitted::RecipientClosed(_)) => return Ok(Delivered::Closed),
                Ok(Submitted::InboxMoved(_)) => {}
                Ok(Submitted::Conflict(_)) => return Err(Error::Record("inbox commit conflicted")),
            }
        }
    }
}

fn actor_error(error: acyclic_actors::local::Error) -> Error {
    Error::Actor(Box::new(error))
}
