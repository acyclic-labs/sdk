//! The one agent entry point every host shares: local, wasm, providers and native.
//!
//! An [`AgentHost`] owns one A4 [`Runtime`] for every agent in one account and
//! namespace. Every control is one agent commit: create a root agent, send a
//! record (prompt, cancel, approval, hook) or archive. Agents never run outside
//! the runtime: a commit to an inbox wakes its actor through the actor's follow.
//! Observation needs no API: an agent's status and conversation are streams
//! ([`AgentPath::status`], [`AgentPath::conversation`]) any client follows.

use super::{
    AgentComposer, AgentHandler, AgentLifecycle, AgentPath, AgentProfilePin, Error, ForkAdopted,
    INBOX, InboxRecord, IntentId, Sender, Spawn, SpawnOutcome, SpawnProfiles, SpawnVolumes,
    Submitted, TurnDriver, archive, handler::Agents, send, submit,
};
use acyclic_actors::{
    addressing::ActorReference,
    domain::ActorLimits,
    guest::{Composer, Handler},
    local::{Clock, Runtime, Store},
};
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitRequest, FollowRequest, ReadRequest, StreamError,
    StreamPath, StreamProvider, WhenAbsent,
};
use futures::TryStreamExt as _;
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
pub struct AgentHost<P: crate::store::JournalProvider + ?Sized + 'static> {
    provider: Arc<P>,
    agents: Agents<P>,
    runtime: Runtime<P>,
    within: ActorReference,
    lifecycle: Option<Arc<dyn AgentLifecycle>>,
    /// The role profiles this host's turns run under, and the pin of every
    /// root it creates; `None` binds no roles.
    profiles: Option<(Arc<dyn SpawnProfiles>, AgentProfilePin)>,
}

/// The agents' actor store in `provider`'s domain, under [`super::ACTORS`].
pub fn actor_store<P: crate::store::JournalProvider + ?Sized + 'static>(
    provider: &Arc<P>,
) -> Result<Store<P>, Error> {
    let root = StreamPath::new(super::ACTORS).map_err(|_| Error::Path("actor root"))?;
    Ok(Store::new(provider.clone(), root))
}

impl<P: crate::store::JournalProvider + ?Sized + 'static> AgentHost<P> {
    /// Host for the agents of `within`'s account and namespace, with actor state
    /// under [`super::ACTORS`] in `provider`'s consistency domain.
    pub fn new<D: TurnDriver + 'static>(
        provider: Arc<P>,
        driver: D,
        within: ActorReference,
        limits: ActorLimits,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        Self::composing(provider, driver, within, limits, clock, None, None)
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
        Self::composing(provider, driver, within, limits, clock, Some(volumes), None)
    }

    /// Host whose spawns create children's volumes through `volumes`, and
    /// whose agents' resources `lifecycle` parks on pause and releases in each
    /// agent's closing step, before that step commits.
    pub fn composing<D: TurnDriver + 'static>(
        provider: Arc<P>,
        driver: D,
        within: ActorReference,
        limits: ActorLimits,
        clock: Arc<dyn Clock>,
        volumes: Option<Arc<dyn SpawnVolumes>>,
        lifecycle: Option<Arc<dyn AgentLifecycle>>,
    ) -> Result<Self, Error> {
        let mut handler = AgentHandler::new(actor_store(&provider)?, driver, limits)
            .map_err(|_| Error::Record("agent handler manifest"))?;
        if let Some(volumes) = &volumes {
            handler = handler.with_spawn_volumes(volumes.clone());
        }
        if lifecycle.is_some() {
            handler = handler.with_close();
        }
        let composer: Option<Arc<dyn Composer>> = match (&volumes, &lifecycle) {
            (None, None) => None,
            _ => Some(Arc::new(AgentComposer::new(volumes, lifecycle.clone()))),
        };
        let host = Self::hosting(provider, within, Arc::new(handler), clock, composer)?;
        Ok(match lifecycle {
            Some(lifecycle) => host.with_lifecycle(lifecycle),
            None => host,
        })
    }

    /// Parks what each agent holds when [`Self::pause`] pauses it. A host
    /// from [`Self::hosting`] releases the same resources on archive by
    /// passing an [`AgentComposer`] with `lifecycle` and a handler built
    /// with [`AgentHandler::with_close`].
    #[must_use]
    pub fn with_lifecycle(mut self, lifecycle: Arc<dyn AgentLifecycle>) -> Self {
        self.lifecycle = Some(lifecycle);
        self
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
            lifecycle: None,
            profiles: None,
        })
    }

    /// This host, with the same agents, role profiles and lifecycle, whose
    /// deliveries run in `handler` (such as a guest boundary wrapping this
    /// host's own handler) and whose transitions `composer` opens.
    ///
    /// # Errors
    /// As [`Self::hosting`].
    pub fn rehosted(
        &self,
        handler: Arc<dyn Handler>,
        clock: Arc<dyn Clock>,
        composer: Option<Arc<dyn Composer>>,
    ) -> Result<Self, Error> {
        let mut host = Self::hosting(
            self.provider.clone(),
            self.within.clone(),
            handler,
            clock,
            composer,
        )?;
        host.lifecycle.clone_from(&self.lifecycle);
        host.profiles.clone_from(&self.profiles);
        Ok(host)
    }

    /// Runs this host's agents under `profiles`, the same profiles its turns
    /// bind: every root it creates is pinned to their top-level role, and a
    /// host-requested spawn is admitted by its parent's role. Roots are
    /// pinned here, at the one creation entrance, so no host can create a
    /// root its turns cannot run under a role.
    ///
    /// # Errors
    /// The profiles' root role fails to resolve.
    pub fn with_profiles(mut self, profiles: Arc<dyn SpawnProfiles>) -> Result<Self, Error> {
        let root = profiles
            .root()
            .map_err(|error| Error::Turn(Box::new(error)))?;
        self.profiles = Some((profiles, root));
        Ok(self)
    }

    /// The pin of a `role` child of `parent`, admitted by `parent`'s role.
    /// A pin never changes once adopted, so this admission holds for the
    /// step that applies the spawn.
    ///
    /// # Errors
    /// [`Error::SpawnRefused`] when this host binds no roles, `parent` has no
    /// role, or its role does not admit `role`; or the state read fails.
    pub async fn child_profile(
        &self,
        parent: &AgentPath,
        role: &str,
    ) -> Result<AgentProfilePin, Error> {
        let refused = Error::SpawnRefused(super::SpawnRefusal::Role);
        let Some((profiles, _)) = &self.profiles else {
            return Err(refused);
        };
        let Some(pin) = self.state(parent).await?.profile else {
            return Err(refused);
        };
        profiles.child(&pin, role).map_err(|_| refused)
    }

    /// Admit at most `limits` deliveries at once; configure before running.
    /// Admission is backpressure: a delivery beyond it waits for a slot, and
    /// no agent holds one while it waits for another (see [`super::Wait`]).
    #[must_use]
    pub fn with_limits(mut self, limits: acyclic_actors::local::RuntimeLimits) -> Self {
        self.runtime = self.runtime.with_limits(limits);
        self
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

    /// Settlement and continuation from the same original actor observation.
    /// Consumption alone may retain a paused turn, queued or overflowed input.
    pub(crate) async fn input_progress(
        &self,
        agent: &AgentPath,
        input: u64,
    ) -> Result<(bool, acyclic_stream::FollowRequest), Error> {
        let snapshot = self
            .runtime
            .store()
            .load(&self.actor(agent)?)
            .await
            .map_err(actor_error)?;
        let state = snapshot.state().ok_or(Error::NoSuchAgent)?;
        let inbox = agent.inbox()?;
        let subscription = state
            .subscriptions()
            .get(INBOX)
            .filter(|subscription| subscription.path == inbox)
            .ok_or(Error::Record("agent inbox subscription differs"))?;
        let settled = if let Some(checkpoint) = state.checkpoint() {
            let observed = super::checkpoint_state(checkpoint)?;
            if &observed.path != agent || observed.parent != agent.parent() {
                return Err(Error::Record("agent state binding differs"));
            }
            if observed.closed && input >= subscription.cursor {
                return Err(Error::Record("usage input source is closed"));
            }
            input < observed.settled_before(subscription.cursor)
        } else if subscription.cursor > input {
            return Err(Error::Record("consumed input has no agent checkpoint"));
        } else {
            false
        };
        Ok((settled, snapshot.follow_request()))
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
    /// Follows the immutable outcome appended in that step's commit. Both
    /// replay and observation use one record, independent of actor history.
    /// An intent not named by `sequence` is a conflict, refused before IO.
    pub async fn spawned(
        &self,
        parent: &AgentPath,
        sequence: u64,
        intent: &IntentId,
    ) -> Result<SpawnOutcome, Error> {
        if super::machine::requested_intent(sequence)? != *intent {
            return Err(Error::Conflict(
                "spawn intent differs from its inbox sequence",
            ));
        }
        let mut request = FollowRequest::new(super::spawn_result::path(parent, sequence)?, 0);
        request.when_absent = WhenAbsent::Wait;
        let mut records = acyclic_stream::records_only(
            self.provider.follow(request).await.map_err(Error::Stream)?,
        );
        let record = records
            .try_next()
            .await
            .map_err(Error::Stream)?
            .ok_or(Error::Stream(StreamError::Unavailable))?;
        if record.sequence != 0 {
            return Err(Error::Record("spawn outcome is not the first record"));
        }
        super::spawn_result::decode(parent, sequence, &record.value)
    }

    /// Prepares, without IO, the one commit creating root agent
    /// `adopted.agent` in this host's account and namespace (see
    /// [`AgentHandler::prepare_root`](super::AgentHandler::prepare_root)).
    /// [`Self::create`] is exactly this request submitted once. The root's
    /// role is this host's ([`Self::with_profiles`]): `adopted.profile` must
    /// be `None`, and the host pins it.
    ///
    /// # Errors
    /// [`Error::Record`] when `adopted` names its own profile.
    pub fn prepare_root(
        &self,
        intent: &IntentId,
        mut adopted: ForkAdopted,
    ) -> Result<Spawn, Error> {
        if adopted.profile.is_some() {
            return Err(Error::Record("a root's role profile is pinned by its host"));
        }
        adopted.profile = self.profiles.as_ref().map(|(_, root)| root.clone());
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

    /// Close exactly the child's settled `status` and wait for the parent
    /// commit that releases its membership. Keeps the original output cut,
    /// so retry/resume never substitutes the subsequent archived status.
    ///
    /// One immutable status read, one archive commit and one release follow.
    /// The archive commit fences both the status tail and the inbox cursor:
    /// intervening input or progress is a conflict, never a readiness retry.
    /// An exact intent retry replays that commit and its original receipt.
    /// Roots have no parent membership and are refused before IO.
    ///
    /// The parent consuming `ChildDone` is the sole release producer. Session
    /// owners must race this operation with their runtime failure event, as
    /// they do for [`Self::spawned`]; a closed follow returns `Unavailable`.
    pub async fn release_settled(
        &self,
        child: &AgentPath,
        by: &Sender,
        id: &IntentId,
        status: u64,
    ) -> Result<(super::Settlement, super::ChildRelease), Error> {
        use super::{AgentRecord as _, Phase, Settlement, commit};
        let release_path = super::release::for_child(child)?;
        let expected = status
            .checked_add(1)
            .ok_or(Error::Conflict("settlement status sequence overflows"))?;
        let status_path = child.status()?;
        let mut records = self
            .provider
            .read(ReadRequest {
                key: None,
                path: status_path.clone(),
                from: status,
                limit: 1,
                at_least: Default::default(),
            })
            .await
            .map_err(Error::Stream)?;
        let record = records
            .try_next()
            .await
            .map_err(Error::Stream)?
            .filter(|record| record.sequence == status)
            .ok_or(Error::Conflict("settlement status does not exist"))?;
        drop(records);
        let observed = super::StatusRecord::decode(&record.value)?;
        if !matches!(observed.phase, Phase::Idle | Phase::Done { .. }) {
            return Err(Error::Conflict("selected status has not settled"));
        }
        let settlement = Settlement {
            status,
            cursor: observed.cursor,
            conversation: observed.conversation,
        };
        let retained = Settlement::path(child)?;
        let mut request = commit::archive_at(child, by, id, Some(settlement.cursor))?;
        request.conditions.extend([
            CommitCondition::Tail {
                path: status_path,
                expected,
            },
            CommitCondition::Absent {
                path: retained.clone(),
            },
        ]);
        request.mutations.push(CommitMutation::Append {
            path: retained,
            records: vec![settlement.encode()?.into()],
        });
        if self.deliver(child, request).await? == Delivered::Closed {
            return Err(Error::Conflict("child was archived by another intent"));
        }
        let mut request = FollowRequest::new(release_path, 0);
        request.when_absent = WhenAbsent::Wait;
        let mut records = acyclic_stream::records_only(
            self.provider.follow(request).await.map_err(Error::Stream)?,
        );
        let record = records
            .try_next()
            .await
            .map_err(Error::Stream)?
            .ok_or(Error::Stream(StreamError::Unavailable))?;
        if record.sequence != 0 {
            return Err(Error::Record("child release is not the original record"));
        }
        Ok((settlement, super::ChildRelease::decode(&record.value)?))
    }

    /// Durably pause or resume one agent. A paused agent consumes nothing:
    /// its resources are parked too (its Machines suspended), and its next
    /// use after resuming wakes them.
    pub async fn pause(&self, agent: &AgentPath, paused: bool) -> Result<(), Error> {
        let actor = self.actor(agent)?;
        let lifecycle = self.lifecycle.clone();
        let agent = agent.clone();
        self.runtime
            .pause_with(&actor, paused, async move {
                match lifecycle {
                    Some(lifecycle) if paused => lifecycle.pause(&agent).await,
                    _ => Ok(()),
                }
            })
            .await
            .map_err(actor_error)?
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
        let actor = self.actor(to)?;
        request
            .mutations
            .push(self.runtime.store().activation(&actor).mutation);
        match submit(&*self.provider, request).await? {
            Submitted::Committed(receipt) => {
                let append = receipt
                    .appends
                    .iter()
                    .find(|append| append.path == inbox)
                    .ok_or(Error::Record("commit receipt omits the inbox append"))?;
                // Persistent activation serves hosted dispatchers. A locally
                // driven runtime has no idle follow, so signal its ready slot
                // on this same confirmed-commit path as well.
                self.runtime.wake(&actor);
                Ok(Delivered::Committed {
                    inbox: append.start,
                })
            }
            Submitted::RecipientClosed(_) => Ok(Delivered::Closed),
            Submitted::NoSuchAgent(_) => Err(Error::NoSuchAgent),
            Submitted::Conflict(_) => Err(Error::Conflict("inbox admission changed")),
        }
    }
}

fn actor_error(error: acyclic_actors::local::Error) -> Error {
    Error::Actor(Box::new(error))
}
