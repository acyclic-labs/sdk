//! The one agent entry point every host shares: local, wasm, providers and native.
//!
//! An [`AgentHost`] owns one A4 [`Runtime`] for every agent in one account and
//! namespace. Every control is one agent commit: create a root agent, send a
//! record (prompt, cancel, approval, hook) or archive. Agents never run outside
//! the runtime: a commit to an inbox wakes its actor through the actor's follow.
//! Observation needs no API: an agent's status and conversation are streams
//! ([`AgentPath::status`], [`AgentPath::conversation`]) any client follows.

use super::{
    AgentHandler, AgentPath, Error, ForkAdopted, INBOX, InboxRecord, IntentId, Sender, Spawn,
    SpawnOutcome, SpawnVolumes, Submitted, TurnDriver, archive, handler::Agents, send, submit,
};
use acyclic_actors::{
    addressing::ActorReference,
    domain::ActorLimits,
    guest::{Composer, Handler},
    local::{Clock, Runtime, State, Store},
};
use acyclic_stream::{CommitRequest, ReadRequest, StreamError, StreamPath, StreamProvider};
use futures::{StreamExt as _, TryStreamExt as _};
use std::{convert::Infallible, sync::Arc};

/// Outcome of one control commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Delivered {
    /// Committed exactly once (an exact replay also reports this) at inbox
    /// sequence `inbox`. A message's settled turn carries the same sequence in
    /// its [`super::ConversationEntry`], and [`AgentHost::reply`] finds it;
    /// `StatusRecord::cursor > inbox` means the agent has consumed it.
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
        Ok(self.agents.state(&self.actor(agent)?, agent).await?.0)
    }

    /// The committed reply to `agent`'s message `id` at inbox sequence `inbox`
    /// (see [`super::reply`]), from its committed checkpoint.
    pub async fn reply(
        &self,
        agent: &AgentPath,
        id: &IntentId,
        inbox: u64,
    ) -> Result<super::Reply, Error> {
        let (state, cursor) = self.agents.state(&self.actor(agent)?, agent).await?;
        super::reply(&*self.provider, &state, cursor, id, inbox).await
    }

    /// What `parent`'s step did with the spawn named `intent` that its inbox
    /// record `sequence` requested: the outcome that step committed, never a
    /// guess from an earlier state, however many steps later it is asked.
    /// Waits, on the parent's actor state stream, for that step to commit.
    pub async fn spawned(
        &self,
        parent: &AgentPath,
        sequence: u64,
        intent: &IntentId,
    ) -> Result<SpawnOutcome, Error> {
        let actor = self.actor(parent)?;
        let store = self.runtime.store();
        let path = store.path(&actor).map_err(actor_error)?;
        loop {
            // Follow from the tail read before the load, so a step committing
            // after the load is never missed.
            let tail = match self.provider.tail(path.clone()).await {
                Ok(tail) => tail,
                Err(StreamError::NotFound) => 0,
                Err(error) => return Err(Error::Stream(error)),
            };
            let consumed = store
                .load(&actor)
                .await
                .map_err(actor_error)?
                .state()
                .and_then(|state| state.cursor(INBOX))
                .is_some_and(|cursor| cursor > sequence);
            if consumed {
                return self.applied(&path, sequence, intent).await;
            }
            let mut committed = acyclic_stream::records_only(
                self.provider
                    .follow(acyclic_stream::FollowRequest::new(path.clone(), tail))
                    .await
                    .map_err(Error::Stream)?,
            );
            match committed.next().await {
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(Error::Stream(error)),
                None => return Err(Error::Stream(StreamError::Unavailable)),
            }
        }
    }

    /// The outcome of the spawn the step consuming inbox record `sequence`
    /// applied, from the actor state history at `path`: the first state whose
    /// inbox cursor passes `sequence` is that step's commit. History is append-only and
    /// cursors only advance, so this is O(log history) reads.
    async fn applied(
        &self,
        path: &StreamPath,
        sequence: u64,
        intent: &IntentId,
    ) -> Result<SpawnOutcome, Error> {
        let tail = self
            .provider
            .tail(path.clone())
            .await
            .map_err(Error::Stream)?;
        let (mut low, mut high) = (0, tail);
        while low < high {
            let middle = low + (high - low) / 2;
            let passed = self
                .committed(path, middle)
                .await?
                .cursor(INBOX)
                .is_some_and(|cursor| cursor > sequence);
            if passed {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        if low == tail {
            return Err(Error::Record("no committed step consumed the spawn"));
        }
        let state = self.committed(path, low).await?;
        let checkpoint = state.checkpoint().ok_or(Error::Record(
            "the step consuming the spawn has no checkpoint",
        ))?;
        Ok(super::checkpoint_state(checkpoint)?.spawn_outcome(intent))
    }

    /// Actor state record `sequence` of the state stream at `path`.
    async fn committed(&self, path: &StreamPath, sequence: u64) -> Result<State, Error> {
        let mut records = self
            .provider
            .read(ReadRequest {
                path: path.clone(),
                from: sequence,
                limit: 1,
            })
            .await
            .map_err(Error::Stream)?;
        let record = records
            .try_next()
            .await
            .map_err(Error::Stream)?
            .filter(|record| record.sequence == sequence)
            .ok_or(Error::Record("actor state history is incomplete"))?;
        State::decode(&record.value).map_err(actor_error)
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
            Submitted::Conflict(_) | Submitted::NoSuchAgent(_) | Submitted::RecipientClosed(_) => {
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
        self.deliver(to, send(to, from, record)?).await
    }

    /// Close `target` and, through its cascade, its open descendants.
    pub async fn archive(
        &self,
        target: &AgentPath,
        by: &Sender,
        id: &IntentId,
    ) -> Result<Delivered, Error> {
        self.deliver(target, archive(target, by, id)?).await
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

    /// Event-owned delivery, returning the committed native alarm and releasing
    /// idle follow cursors. An unsettled publication keeps its runtime slot.
    pub async fn drain_event(&self, agent: &AgentPath) -> Result<Option<u64>, Error> {
        self.runtime
            .drain_event(&self.actor(agent)?)
            .await
            .map_err(actor_error)
    }

    /// One inbox commit with no prior read: `Present{inbox}` proves the agent
    /// exists and `Absent{closed}` fences admission in the commit itself. An
    /// exact intent retry is identical after other appends, while changed
    /// semantic bytes still fail the provider's digest check.
    /// Commits one inbox write with its agent's wake entry.
    async fn deliver(
        &self,
        to: &AgentPath,
        mut request: CommitRequest,
    ) -> Result<Delivered, Error> {
        let inbox = to.inbox()?;
        request
            .mutations
            .push(self.runtime.store().activation(&self.actor(to)?).mutation);
        match submit(&*self.provider, request).await? {
            Submitted::Committed(receipt) => receipt
                .appends
                .iter()
                .find(|append| append.path == inbox)
                .map(|append| Delivered::Committed {
                    inbox: append.start,
                })
                .ok_or(Error::Record("commit receipt omits the inbox append")),
            Submitted::RecipientClosed(_) => Ok(Delivered::Closed),
            Submitted::NoSuchAgent(_) => Err(Error::NoSuchAgent),
            Submitted::Conflict(_) => Err(Error::Record("inbox commit conflicted")),
        }
    }
}

fn actor_error(error: acyclic_actors::local::Error) -> Error {
    Error::Actor(Box::new(error))
}
