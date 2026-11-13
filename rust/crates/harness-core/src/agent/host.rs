//! The one agent entry point every host shares: local, wasm, providers and native.
//!
//! An [`AgentHost`] owns one A4 [`Runtime`] for every agent in one account and
//! namespace. Every control is one agent commit: create a root agent, send a
//! record (prompt, cancel, approval, hook) or archive. Agents never run outside
//! the runtime: a commit to an inbox wakes its actor through the actor's follow.
//! Observation needs no API: an agent's status and conversation are streams
//! ([`AgentPath::status`], [`AgentPath::conversation`]) any client follows.

use super::{
    AgentComposer, AgentHandler, AgentLifecycle, AgentPath, AgentRecord as _, CreationConfig,
    Error, ForkAdopted, INBOX, InboxRecord, IntentId, Sender, Spawn, SpawnOutcome, SpawnRefusal,
    SpawnTemplates, SpawnVolumes, StatusRecord, Submitted, TurnDriver, archive, handler::Agents,
    send, submit,
};
use acyclic_actors::{
    addressing::ActorReference,
    domain::ActorLimits,
    guest::{Composer, Handler},
    local::{Clock, Runtime, Store},
};
use acyclic_stream::{
    CommitRequest, ReadRequest, StreamError, StreamPath, StreamProvider, StreamProviderExt as _,
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

/// Input settlement and producer lifetime from one original actor observation.
pub struct InputProgress {
    /// The input has settled, or archive has retired its remaining work.
    pub settled: bool,
    /// Archive closed the producer. Consuming an input never closes it.
    pub closed: bool,
    /// Direct open children in that same checkpoint. No child tree is read.
    pub open_children: u64,
    /// Wait for a change after this same observation.
    pub follow: acyclic_stream::FollowRequest,
}

/// One runtime for every agent in one account and namespace.
pub struct AgentHost<P: crate::store::JournalProvider + ?Sized + 'static> {
    provider: Arc<P>,
    agents: Agents<P>,
    runtime: Runtime<P>,
    within: ActorReference,
    lifecycle: Option<Arc<dyn AgentLifecycle>>,
    /// The creation reader shared with this host's turns; `None` disables
    /// template creation.
    templates: Option<Arc<dyn SpawnTemplates>>,
    model: Option<String>,
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
        if lifecycle.is_some() || volumes.is_some() {
            handler = handler.with_close();
        }
        let composer: Option<Arc<dyn Composer>> = match (&volumes, &lifecycle) {
            (None, None) => None,
            _ => Some(Arc::new(AgentComposer::new(
                provider.clone(),
                volumes,
                lifecycle.clone(),
            ))),
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
    /// transition's participant request, as [`SpawnVolumes`] does. The shared
    /// agent composer always owns terminal-fact revalidation, including when
    /// no external participant composer is supplied.
    pub fn hosting(
        provider: Arc<P>,
        within: ActorReference,
        handler: Arc<dyn Handler>,
        clock: Arc<dyn Clock>,
        composer: Option<Arc<dyn Composer>>,
    ) -> Result<Self, Error> {
        let published = actor_store(&provider)?.with_composer(Arc::new(AgentComposer::wrapping(
            Arc::clone(&provider),
            composer,
        )));
        Ok(Self {
            agents: Agents::new(actor_store(&provider)?),
            // Agents of other bindings may share this stream's actor root;
            // this filter grants no placement or execution authority.
            runtime: Runtime::new(published, handler, clock).within(&within),
            provider,
            within,
            lifecycle: None,
            templates: None,
            model: None,
        })
    }

    /// This host, with the same agents, creation templates and lifecycle, whose
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
        host.templates.clone_from(&self.templates);
        host.model.clone_from(&self.model);
        Ok(host)
    }

    /// Pins the root model even when the host enables no child templates.
    #[must_use]
    pub fn with_model(mut self, model: Option<String>) -> Self {
        self.model = model;
        self
    }

    /// Uses the host's original admitted template reader for child creation.
    #[must_use]
    pub fn with_templates(mut self, templates: Arc<dyn SpawnTemplates>) -> Self {
        self.templates = Some(templates);
        self
    }

    /// Admits one child template from the parent's effective tree. The exact
    /// selected file and context are committed with the child's creation.
    pub async fn child_profile(
        &self,
        parent: &AgentPath,
        template: &str,
    ) -> Result<CreationConfig, Error> {
        let refused = || Error::SpawnRefused(super::SpawnRefusal::Role);
        let templates = self.templates.as_ref().ok_or_else(refused)?;
        let state = self.state(parent).await?;
        let creation = state.profile.as_ref().ok_or_else(refused)?;
        templates
            .child(creation, &state.path, &state.volumes, template, None)
            .await
            .map_err(|_| refused())
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
        Ok(self.state_snapshot(agent).await?.0)
    }

    /// The original checkpoint and its consumed inbox cursor from one read.
    /// The state's settlement frontier includes paused, queued and overflowed
    /// input; a status phase alone cannot reconstruct pending questions.
    pub async fn state_snapshot(
        &self,
        agent: &AgentPath,
    ) -> Result<(super::AgentState, u64), Error> {
        self.agents.state(&self.actor(agent)?, agent).await
    }

    /// The original checkpoint and its settlement against the accepted inbox
    /// cut, read concurrently. Pending accepted input remains running even
    /// while its executing delivery has not committed a new checkpoint.
    pub async fn settlement_snapshot(
        &self,
        agent: &AgentPath,
    ) -> Result<(super::AgentState, super::Settlement), Error> {
        super::settled::snapshot(&self.provider, &self.within, agent).await
    }

    /// The live child currently owning a spawn name, or its free generation.
    pub async fn resolve_spawn_name(
        &self,
        parent: &AgentPath,
        name: &str,
    ) -> Result<super::SpawnResolution, Error> {
        super::resolve_spawn_name(&self.provider, &self.within, parent, name).await
    }

    /// Settlement and continuation from the same original actor observation.
    /// Consumption alone may retain a paused turn, queued or overflowed input.
    pub async fn input_progress(
        &self,
        agent: &AgentPath,
        input: u64,
    ) -> Result<InputProgress, Error> {
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
        let (settled, closed, open_children) = if let Some(checkpoint) = state.checkpoint() {
            let observed = super::checkpoint_head(checkpoint)?.state;
            if &observed.path != agent || !agent.parent_is(observed.parent.as_ref()) {
                return Err(Error::Record("agent state binding differs"));
            }
            // Archive retires the producer, not its durable observations. At
            // its tail this is an ordinary terminal frontier for every query.
            (
                observed.closed || input < observed.settled_before(subscription.cursor),
                observed.closed,
                observed.open_children.len() as u64,
            )
        } else if subscription.cursor > input {
            return Err(Error::Record("consumed input has no agent checkpoint"));
        } else {
            (false, false, 0)
        };
        Ok(InputProgress {
            settled,
            closed,
            open_children,
            follow: snapshot.follow_request(),
        })
    }

    /// Wait until every input through `input` has settled or been disposed.
    /// Queued work and a paused turn are included; an idle phase alone is not
    /// a settlement fence. Each recheck follows an original actor publication.
    pub async fn wait_for_input(&self, agent: &AgentPath, input: u64) -> Result<(), Error> {
        let failed = self
            .runtime
            .delivery_failure(&self.actor(agent)?)
            .map_err(actor_error)?;
        let failed = std::pin::pin!(failed);
        let mut failed = failed;
        loop {
            let progress = self.input_progress(agent, input).await?;
            if progress.settled {
                return Ok(());
            }
            let mut records = acyclic_stream::records_only(
                self.provider
                    .follow(progress.follow)
                    .await
                    .map_err(Error::Stream)?,
            );
            let next = std::pin::pin!(records.try_next());
            match futures::future::select(next, failed.as_mut()).await {
                futures::future::Either::Left((next, _)) => {
                    next.map_err(Error::Stream)?
                        .ok_or(Error::Stream(StreamError::Unavailable))?;
                }
                futures::future::Either::Right((cause, _)) => {
                    // A publication that settled this particular input wins
                    // over a later failed delivery of another input.
                    if self.input_progress(agent, input).await?.settled {
                        return Ok(());
                    }
                    return Err(Error::Stream(StreamError::unavailable_with_cause(&cause)));
                }
            }
        }
    }

    /// Replays and follows conversation, including before its first turn.
    /// First publication is observed through the original actor checkpoint;
    /// same-phase turns need no extra status record. A terminal checkpoint with
    /// no conversation preserves the original missing-stream refusal.
    pub async fn follow_conversation(
        &self,
        agent: &AgentPath,
        from: u64,
    ) -> Result<acyclic_stream::RecordStream, Error> {
        let conversation = agent.conversation()?;
        match self.provider.tail(conversation.clone()).await {
            Ok(_) => return self.conversation_cursor(conversation, from).await,
            Err(error) if from == 0 && error.category() == StreamError::NotFound => {}
            Err(error) => return Err(Error::Stream(error)),
        }
        let actor = self.actor(agent)?;
        loop {
            let snapshot = self
                .runtime
                .store()
                .load(&actor)
                .await
                .map_err(actor_error)?;
            let state = snapshot
                .state()
                .ok_or(Error::Stream(StreamError::NotFound))?;
            if let Some(checkpoint) = state.checkpoint() {
                let observed = super::checkpoint_head(checkpoint)?.state;
                if &observed.path != agent || !agent.parent_is(observed.parent.as_ref()) {
                    return Err(Error::Record("agent state binding differs"));
                }
                if observed.conversation_tail > 0 {
                    return self.conversation_cursor(conversation, from).await;
                }
                if matches!(observed.phase, super::Phase::Done { .. }) {
                    return Err(Error::Stream(StreamError::NotFound));
                }
            }
            let mut committed = acyclic_stream::records_only(
                self.provider
                    .follow(snapshot.follow_request())
                    .await
                    .map_err(Error::Stream)?,
            );
            committed
                .try_next()
                .await
                .map_err(Error::Stream)?
                .ok_or(Error::Stream(StreamError::Unavailable))?;
        }
    }

    async fn conversation_cursor(
        &self,
        conversation: StreamPath,
        from: u64,
    ) -> Result<acyclic_stream::RecordStream, Error> {
        self.provider
            .follow(acyclic_stream::FollowRequest::new(conversation, from))
            .await
            .map(acyclic_stream::records_only)
            .map_err(Error::Stream)
    }

    /// The committed reply to `agent`'s message `id` at inbox sequence `inbox`
    /// (see [`super::reply`]), from its committed checkpoint.
    pub async fn reply(
        &self,
        agent: &AgentPath,
        id: &IntentId,
        inbox: u64,
    ) -> Result<super::Reply, Error> {
        let (mut state, cursor) = self.agents.state(&self.actor(agent)?, agent).await?;
        super::reply(&*self.provider, &mut state, cursor, id, inbox).await
    }

    /// What `parent`'s step did with the spawn named `intent` that its inbox
    /// record `sequence` requested: the outcome that step committed, never a
    /// guess from an earlier state, however many steps later it is asked.
    /// Waits on the parent's actor state stream for that step to commit, then
    /// reads the child's original creation status. Later steps and archival
    /// retain that status, so the work is independent of history length.
    /// An intent not named by `sequence` is a conflict, refused before IO.
    /// The original inbox record must be a committed spawn before waiting.
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
        if !matches!(
            super::reply::inbox_record(&*self.provider, parent.inbox()?, sequence).await?,
            Some(InboxRecord::Spawn(_))
        ) {
            return Err(Error::Conflict("spawn wait has no committed spawn input"));
        }
        let actor = self.actor(parent)?;
        let store = self.runtime.store();
        let inbox = parent.inbox()?;
        loop {
            let snapshot = store.load(&actor).await.map_err(actor_error)?;
            let state = snapshot.state().ok_or(Error::NoSuchAgent)?;
            let subscription = state
                .subscriptions()
                .get(INBOX)
                .filter(|subscription| subscription.path == inbox)
                .ok_or(Error::Record("agent inbox subscription differs"))?;
            if subscription.cursor > sequence {
                return self
                    .spawn_result(parent, sequence, intent, state.checkpoint())
                    .await;
            }
            if let Some(checkpoint) = state.checkpoint()
                && super::checkpoint_head(checkpoint)?.state.closed
                && sequence
                    >= self
                        .provider
                        .tail(inbox.clone())
                        .await
                        .map_err(Error::Stream)?
            {
                return Err(Error::Conflict("spawn sequence follows the closed inbox"));
            }
            let mut committed = acyclic_stream::records_only(
                self.provider
                    .follow(snapshot.follow_request())
                    .await
                    .map_err(Error::Stream)?,
            );
            committed
                .try_next()
                .await
                .map_err(Error::Stream)?
                .ok_or(Error::Stream(StreamError::Unavailable))?;
        }
    }

    /// Creation and the parent's consumed cursor share one commit. The first
    /// status is outside the forked subtree and survives the child's retirement.
    async fn spawn_result(
        &self,
        parent: &AgentPath,
        sequence: u64,
        intent: &IntentId,
        checkpoint: Option<&acyclic_actors::guest::Checkpoint>,
    ) -> Result<SpawnOutcome, Error> {
        let child = parent.child(&super::AgentId::derive(&parent.id(), intent.as_str()))?;
        match self
            .provider
            .read(ReadRequest {
                key: None,
                path: child.status()?,
                from: 0,
                limit: 1,
                at_least: Default::default(),
            })
            .await
        {
            Ok(mut records) => {
                let record = records
                    .try_next()
                    .await
                    .map_err(Error::Stream)?
                    .filter(|record| record.sequence == 0)
                    .ok_or(Error::Record("spawn creation status is missing"))?;
                let status = StatusRecord::decode(&record.value)?;
                if status.phase != super::Phase::Idle
                    || sequence.checked_add(1) != Some(status.cursor)
                {
                    return Err(Error::Record("spawn creation status differs"));
                }
                return Ok(SpawnOutcome::Spawned { child });
            }
            Err(error) if error.category() == StreamError::NotFound => {}
            Err(error) => return Err(Error::Stream(error)),
        }
        let mut state = super::checkpoint_head(checkpoint.ok_or(Error::Record(
            "the step consuming the spawn has no checkpoint",
        ))?)?
        .state;
        state.topology_refusals.bind(Arc::clone(&self.provider));
        let topology_refusal = state.topology_refusals.contains(&child).await?;
        if !state.closed && !topology_refusal {
            return Err(Error::Record("consumed spawn has no creation status"));
        }
        // Absence proves a refusal only for an actual Spawn input. A caller
        // naming a different input must not receive a fabricated outcome.
        let mut records = self
            .provider
            .read(ReadRequest {
                key: None,
                path: parent.inbox()?,
                from: sequence,
                limit: 1,
                at_least: Default::default(),
            })
            .await
            .map_err(Error::Stream)?;
        let record = records
            .try_next()
            .await
            .map_err(Error::Stream)?
            .filter(|record| record.sequence == sequence)
            .ok_or(Error::Record("consumed spawn input is missing"))?;
        if !matches!(InboxRecord::decode(&record.value)?, InboxRecord::Spawn(_)) {
            return Err(Error::Conflict(
                "the inbox sequence consumed another request",
            ));
        }
        Ok(SpawnOutcome::Refused {
            reason: if topology_refusal {
                SpawnRefusal::Topology
            } else {
                SpawnRefusal::Archived
            },
        })
    }

    /// Prepares, without IO, the one commit creating root agent
    /// `adopted.agent` in this host's account and namespace (see
    /// [`AgentHandler::prepare_root`](super::AgentHandler::prepare_root)).
    /// [`Self::create`] is exactly this request submitted once. The root's
    /// creation config is this host's model and templates: `adopted.profile` must
    /// be `None`, and the host pins it.
    ///
    /// # Errors
    /// [`Error::Record`] when `adopted` names its own profile.
    pub fn prepare_root(
        &self,
        intent: &IntentId,
        adopted: ForkAdopted,
        question_plugin: bool,
    ) -> Result<Spawn, Error> {
        self.prepare_root_with_peer(intent, adopted, question_plugin, true)
    }

    /// Prepare the root with an explicit user launch choice, without a later
    /// membership update or any transient peer admission.
    pub fn prepare_root_with_peer(
        &self,
        intent: &IntentId,
        mut adopted: ForkAdopted,
        question_plugin: bool,
        peer: bool,
    ) -> Result<Spawn, Error> {
        if adopted.profile.is_some() {
            return Err(Error::Record(
                "a root's creation config is admitted by its host",
            ));
        }
        if self.templates.is_some() || self.model.is_some() {
            let mut profile = crate::templates::root_with_topology(
                self.templates
                    .as_ref()
                    .and_then(|templates| templates.topology()),
            )
            .map_err(|error| Error::Turn(Box::new(error)))?;
            profile.model =
                crate::templates::resolve_model(profile.model.as_deref(), self.model.as_deref())
                    .map(str::to_owned);
            profile.question_plugin = question_plugin;
            adopted.profile = Some(profile);
        }
        self.agents
            .prepare_root_with_peer(&self.within, intent, adopted, peer)
    }

    /// Prepare a root branch of an admitted source at its coupled history cut.
    /// Its creation pins the actor paused with independent zero escrow, so
    /// publishing a branch cannot run the model or spend source funding.
    pub(crate) fn prepare_root_history(
        &self,
        intent: &IntentId,
        source: &super::AgentState,
        mut adopted: ForkAdopted,
        at: u64,
        saved: &super::HistoryCheckpoint,
        peer: bool,
    ) -> Result<Spawn, Error> {
        if saved.conversation
            != at
                .checked_add(1)
                .ok_or(Error::Record("history cut overflow"))?
            || saved.conversation > source.conversation_tail
        {
            return Err(Error::Record("root branch requires a settled source cut"));
        }
        let mut profile = source.profile.clone().ok_or(Error::Record(
            "a historical root requires the source's admitted creation config",
        ))?;
        profile.context = super::SpawnContext::Fork;
        profile.continuation = Some(Box::new(super::Continuation::at(
            &source.path,
            at,
            saved,
            None,
        )));
        adopted.profile = Some(profile);
        adopted.conversation_tail = saved.conversation;
        adopted.checkpoint = saved.checkpoint;
        let history = super::HistorySelection {
            conversation: saved.conversation,
            checkpoint: saved.checkpoint,
        };
        self.agents.prepare_root_history(
            &self.within,
            intent,
            adopted,
            &source.path,
            &history,
            peer,
        )
    }

    /// Create root agent `adopted.agent` in one commit, and start hosting it.
    /// Retrying the same `intent` replays exactly and names the same agent.
    pub async fn create(
        &self,
        intent: &IntentId,
        adopted: ForkAdopted,
    ) -> Result<AgentPath, Error> {
        let created = self.prepare_root(intent, adopted, false)?;
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
        if let InboxRecord::Cancel(cancel) = record {
            super::cancellation::validate(self.provider.as_ref(), to, cancel).await?;
        }
        self.deliver(to, send(to, from, record)?).await
    }

    /// Publish a prepared external peer message through the ordinary inbox
    /// writer and activation. The authenticated host supplies `from`; neither
    /// control inputs nor caller-supplied attribution are accepted here.
    /// Retain `admission` and the exact message for an uncertain retry.
    pub async fn send_peer(
        &self,
        to: &AgentPath,
        from: &AgentPath,
        message: &super::Message,
        admission: &super::PeerAdmission,
    ) -> Result<Delivered, Error> {
        let sender = Sender::Agent { path: from.clone() };
        if message.from != sender {
            return Err(Error::Record(
                "peer sender differs from authenticated session",
            ));
        }
        let conditions = admission.conditions(from, to)?;
        admission.validate(&*self.provider, from, to).await?;
        let mut request = send(to, &sender, &InboxRecord::Message(message.clone()))?;
        request.effects[0].conditions.extend(conditions);
        self.deliver(to, request).await
    }

    /// Change root peer participation through the authenticated user surface.
    /// This writes no inbox input and grants no session-directory authority.
    pub async fn set_peer_membership(
        &self,
        root: &AgentPath,
        by: &Sender,
        id: &IntentId,
        enabled: bool,
    ) -> Result<(), Error> {
        let request = super::set_peer_membership(root, by, id, enabled)?;
        match submit(&*self.provider, request).await? {
            Submitted::Committed(_) => Ok(()),
            Submitted::RecipientClosed(_) => Err(Error::Record("the peer root is archived")),
            Submitted::NoSuchAgent(_) => Err(Error::NoSuchAgent),
            Submitted::Conflict(_) => Err(Error::Record("peer membership changed")),
        }
    }

    /// Close `target` and its open descendants. Closing admission precedes
    /// resuming each actor through its original runtime control owner: a
    /// paused actor must still execute its ordinary closing/cleanup step.
    /// Only currently open children are visited, never retained history.
    pub fn archive<'a>(
        &'a self,
        target: &'a AgentPath,
        by: &'a Sender,
        id: &'a IntentId,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Delivered, Error>> {
        Box::pin(async move {
            let delivered = self.deliver(target, archive(target, by, id)?).await?;
            self.pause(target, false).await?;
            let state = self.state(target).await?;
            let mut ancestry = vec![(target.clone(), state.open_children, None)];
            while let Some((parent, children, after)) = ancestry.last_mut() {
                // One child is a page credit, not a subtree-size bound. The
                // persistent cursor and ancestry are the only retained work.
                let Some(next) = children.page(after.as_ref(), 1).await?.pop() else {
                    ancestry.pop();
                    continue;
                };
                let child = parent.child(&next)?;
                *after = Some(next);
                self.deliver(&child, archive(&child, by, id)?).await?;
                self.pause(&child, false).await?;
                let state = self.state(&child).await?;
                ancestry.push((child, state.open_children, None));
            }
            Ok(delivered)
        })
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
        self.runtime.host(actor).map_err(actor_error)
    }

    /// Admit execution through the actor-local placement command. Registration
    /// and inbox delivery alone never authorize this host to claim an agent.
    pub async fn place(&self, agent: &AgentPath, index: u64) -> Result<(), Error> {
        self.runtime
            .place(self.actor(agent)?, index)
            .await
            .map_err(actor_error)
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
        request.effects.push(
            self.runtime
                .store()
                .activation(&actor)
                .map_err(actor_error)?,
        );
        match submit(&*self.provider, request).await? {
            Submitted::Committed(receipt) => {
                let append = receipt
                    .appends
                    .iter()
                    .find(|append| append.path == inbox)
                    .ok_or(Error::Record("commit receipt omits the inbox append"))?;
                // The local wake and the dispatcher's follow name the same
                // committed activation, not two independent delivery attempts.
                self.runtime
                    .wake_committed(&actor, &receipt)
                    .map_err(actor_error)?;
                Ok(Delivered::Committed {
                    inbox: append.start,
                })
            }
            Submitted::RecipientClosed(_) => Ok(Delivered::Closed),
            Submitted::NoSuchAgent(_) => Err(Error::NoSuchAgent),
            Submitted::Conflict(_) => Err(Error::Record("inbox commit conflicted")),
        }
    }
}

fn actor_error(error: acyclic_actors::local::Error) -> Error {
    Error::Actor(Box::new(error))
}
