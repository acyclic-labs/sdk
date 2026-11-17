//! Private control files are original filesystem events consumed by an actor.
//! Its checkpoint and every admission/inbox effect share one publication.
use super::*;
use crate::agent::{AgentOutcome, AgentRecord as _, Phase, StatusRecord};
use acyclic_actors::{
    guest::{
        AlarmChange, Checkpoint, CodecIdentity, Delivery, GuestError, GuestFuture, Handler,
        Manifest, Subscription, SubscriptionChange, Transition,
    },
    local::{Creation, Store},
};
use acyclic_stream::{
    CommitCondition, CommitEffect, CommitMutation, StreamPath, StreamProviderExt as _,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[cfg(not(target_arch = "wasm32"))]
mod hooks;

const PREFIX: &str = "file-controls-";
const PAUSE: &str = "file-pause";
const STOP: &str = "file-stop";
const STEER: &str = "file-steer";
const CLOSED: &str = "file-closed";
const DESCENDANT_STATUS: &str = "file-descendant-status:";
const JOBS: &str = "job-created";
/// The home volume's publications; `hooks/<name>` programs register below.
const HOOKS: &str = "file-hooks";
const VERSION: u32 = 6;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    version: u32,
    agent: AgentPath,
    private: WorkspaceEntry,
    steer: Option<crate::resources::GenerationRef>,
    source_sequence: Option<u64>,
    paused: bool,
    /// The agent's workspace table at creation: its home holds `hooks/`, and
    /// a file selector names one of these volumes.
    volumes: Vec<WorkspaceEntry>,
    /// Registered `hooks/<name>` sources, each followed as `hook/<name>`.
    hooks: std::collections::BTreeMap<String, Registered>,
}

/// One admitted source selector. Its subscription keeps its cursor while the
/// followed path is unchanged, so editing a program never replays records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registered {
    /// The followed stream: a volume's changes path or a stream path.
    path: String,
    /// `FileChanged` or `StreamRecord`.
    event: crate::config::HookEvent,
    /// A file selector's volume key prefix and the bound root that the
    /// agent sees it under; `None` for a stream.
    keys: Option<(String, String)>,
}

impl State {
    fn transition(&self, effects: Vec<CommitEffect>, retired: bool) -> Result<Transition, Error> {
        Ok(Transition {
            response: None,
            checkpoint: Checkpoint {
                codec: codec(),
                bytes: serde_json::to_vec(self)
                    .map_err(|_| Error::Record("control checkpoint"))?
                    .into(),
            },
            alarm: AlarmChange::Clear,
            pause: retired,
            effects,
            participant: None,
            subscriptions: Vec::new(),
        })
    }
}

fn codec() -> CodecIdentity {
    CodecIdentity {
        name: "harness.file-controls".into(),
        version: VERSION.to_string(),
    }
}

/// The actor reference of one descendant, in the control actor's account and
/// namespace.
fn descendant_actor(within: &ActorReference, child: &AgentPath) -> Result<ActorReference, Error> {
    ActorReference::new(
        within.account().as_bytes(),
        within.namespace().as_str().as_bytes(),
        child.id().as_str().as_bytes(),
    )
    .map_err(|_| Error::Record("descendant identity"))
}

pub(super) struct Controls<P: AuthorityProvider + ?Sized + 'static, O: 'static> {
    issuer: RuntimeAuthority,
    admission: TurnAdmission,
    volumes: Arc<Host<P, O>>,
    store: Store<P>,
    composer: std::sync::OnceLock<std::sync::Weak<dyn acyclic_actors::guest::Composer>>,
    maximum_bytes: Option<u64>,
    manifest: Manifest,
    external: Vec<String>,
    ordered: Vec<String>,
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Controls<P, O> {
    pub(super) fn new(
        issuer: RuntimeAuthority,
        admission: TurnAdmission,
        volumes: Arc<Host<P, O>>,
        maximum_bytes: Option<u64>,
        limits: ActorLimits,
    ) -> Result<Self, Error> {
        let external: Vec<String> = [
            PAUSE,
            STOP,
            STEER,
            CLOSED,
            JOBS,
            HOOKS,
            super::quiet::ACTIVITY,
            super::quiet::COMPLETED,
            super::quiet::LIFECYCLE,
            super::quiet::CLOSED,
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        let mut code = blake3::Hasher::new();
        code.update(include_bytes!("controls.rs"));
        code.update(include_bytes!("controls/hooks.rs"));
        code.update(include_bytes!("quiet.rs"));
        code.update(include_bytes!("../../supervision/tool_jobs.rs"));
        Ok(Self {
            ordered: vec![PAUSE.into(), STOP.into(), STEER.into()],
            store: crate::agent::actor_store(&volumes.filesystem.stream_provider())?,
            composer: std::sync::OnceLock::new(),
            issuer,
            admission,
            volumes,
            maximum_bytes,
            manifest: Manifest {
                code: acyclic_actors::domain::CodeSha256::try_from(
                    code.finalize().as_bytes().to_vec(),
                )
                .map_err(|_| Error::Record("control package digest"))?,
                codec: codec(),
                subscriptions: external.clone(),
                interrupts: vec![],
                bindings: vec![],
                limits,
            },
            external,
        })
    }

    pub(super) fn bind_composer(
        &self,
        composer: std::sync::Weak<dyn acyclic_actors::guest::Composer>,
    ) {
        self.composer
            .set(composer)
            .expect("control composer bound once");
    }

    fn actor(&self, within: &ActorReference, agent: &AgentPath) -> Result<ActorReference, Error> {
        ActorReference::new(
            within.account().as_bytes(),
            within.namespace().as_str().as_bytes(),
            format!("{PREFIX}{}", agent.id()).as_bytes(),
        )
        .map_err(|_| Error::Record("control actor identity"))
    }

    fn source(&self, volume: &VolumeRef) -> crate::Result<StreamPath> {
        let (name, _) = self.volumes.volume_creation(volume)?;
        let volume = self
            .volumes
            .filesystem
            .workspace_id(name)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?
            .volume_id();
        acyclic_fs::session::changes_path(&self.volumes.filesystem, volume).map_err(Into::into)
    }

    /// The home volume's changes path, followed for `hooks/<name>` programs.
    fn hook_source(&self, volumes: &[WorkspaceEntry]) -> Result<Option<StreamPath>, Error> {
        if cfg!(target_arch = "wasm32") {
            return Ok(None);
        }
        match volumes
            .iter()
            .find(|volume| volume.role == crate::agent::WorkspaceRole::Home)
        {
            Some(home) => Ok(Some(self.source(home.volume()?).map_err(turn)?)),
            None => Ok(None),
        }
    }

    async fn transition(
        &self,
        delivery: Delivery,
        checkpoint: Option<&Checkpoint>,
    ) -> Result<Transition, Error> {
        // The volume composer also owns these companions. Borrow its custody
        // only for this transition so the host does not retain an Arc cycle.
        let composer = self
            .composer
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or(Error::Record("control composer is unavailable"))?;
        let store = self.store.clone().with_composer(composer);
        if delivery.actor.id().starts_with(super::quiet::PREFIX) {
            return super::quiet::transition(&store, delivery, checkpoint).await;
        }
        let checkpoint = checkpoint
            .filter(|checkpoint| checkpoint.codec == codec())
            .ok_or(Error::Record("control checkpoint codec"))?;
        let mut state: State = serde_json::from_slice(&checkpoint.bytes)
            .map_err(|_| Error::Record("control checkpoint"))?;
        if state.version != VERSION || self.actor(&delivery.actor, &state.agent)? != delivery.actor
        {
            return Err(Error::Record("control checkpoint identity"));
        }
        let (subscription, sequence) = delivery
            .trigger
            .record_position()
            .ok_or(Error::Record("control delivery is not a record"))?;
        let record = delivery
            .record
            .as_ref()
            .filter(|record| record.sequence == sequence)
            .ok_or(Error::Record("control source record"))?;
        let mut effects = Vec::new();
        if subscription == JOBS {
            if delivery.subscriptions.get(JOBS).is_none_or(|input| {
                crate::supervision::tool_jobs::path(&state.agent)
                    .ok()
                    .as_ref()
                    != Some(&input.path)
            }) {
                return Err(Error::Record("quiet creation source differs"));
            }
            effects.extend(super::quiet::create(
                &self.store,
                &delivery.actor,
                &state.agent,
                self.source(
                    &crate::conversation::journal_volume(
                        self.volumes.provider.clone(),
                        &state.agent.id(),
                    )
                    .map_err(turn)?,
                )
                .map_err(turn)?,
                record,
            )?);
            return state.transition(effects, false);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if subscription == HOOKS {
            return self.register_hooks(state, &delivery, record).await;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(name) = subscription.strip_prefix(hooks::HOOK) {
            return self.deliver_hook(state, &delivery, name, record).await;
        }
        if let Some(suffix) = subscription.strip_prefix(DESCENDANT_STATUS) {
            let child = AgentPath::try_from(suffix.to_owned())?;
            let bound = delivery
                .subscriptions
                .get(subscription)
                .ok_or(Error::Record("descendant status subscription is absent"))?;
            if bound.path != child.status()?
                || bound.cursor != sequence
                || record.sequence != sequence
            {
                return Err(Error::Record(
                    "descendant status delivery differs from its wait",
                ));
            }
            let status = StatusRecord::decode(&record.value)?;
            let archived = matches!(
                status.phase,
                Phase::Done {
                    outcome: AgentOutcome::Archived
                }
            );
            if !archived && !matches!(status.phase, Phase::Done { .. }) {
                return state.transition(Vec::new(), false);
            }
            let mut effects = Vec::new();
            let mut added = Vec::new();
            if !archived {
                // A child that completed normally has already reported and
                // never publishes a second terminal status. Its cascade still
                // archives its open children, so wait on that subtree instead.
                // Reading it now also covers a child that spawned after the
                // stop's snapshot: a reported child cannot spawn again.
                let open = self.descendant_children(&delivery.actor, &child).await?;
                added = self
                    .descendant_status(&delivery.actor, &state.agent, &child, &open, &mut effects)
                    .await?;
                added.retain(|change| match change {
                    SubscriptionChange::Add { id, .. } => !delivery.subscriptions.contains_key(id),
                    SubscriptionChange::Remove { .. } => true,
                });
            }
            let remaining = delivery
                .subscriptions
                .iter()
                .filter(|(id, _)| id.starts_with(DESCENDANT_STATUS))
                .count();
            let mut changes = vec![SubscriptionChange::Remove {
                id: subscription.to_owned(),
            }];
            changes.extend(added);
            let mut transition = if remaining == 1 && changes.len() == 1 {
                let position = crate::supervision::Position {
                    stream: child.status()?,
                    sequence,
                };
                let effect = self
                    .lifecycle(
                        &state.agent,
                        &position,
                        vec![crate::supervision::Lifecycle::Stopped { signal: None }],
                        None,
                    )
                    .await?;
                effects.push(effect);
                state.transition(effects, true)?
            } else {
                state.transition(effects, false)?
            };
            transition.subscriptions.extend(changes);
            return Ok(transition);
        }
        let mut retired = subscription == CLOSED;
        if retired
            && delivery
                .subscriptions
                .iter()
                .any(|(id, _)| id.starts_with(DESCENDANT_STATUS))
        {
            // The stop's descendant waits own retirement; the close marker
            // only closes admission, not the settled subtree.
            return state.transition(effects, false);
        }
        let mut descendant_subscriptions = Vec::new();
        if !retired {
            let source = self.source(state.private.volume()?).map_err(turn)?;
            if record.origin != source {
                return Err(Error::Record("control record origin differs"));
            }
            let context = acyclic_stream::changes_context(&record.value, &record.headers)
                .map_err(Error::Stream)?;
            let publication = acyclic_fs::PublicationContext::from_bytes(
                context.ok_or(Error::Record("control source is not a publication"))?,
            )
            .map_err(Error::Stream)?;
            if delivery
                .subscriptions
                .get(subscription)
                .is_none_or(|cut| cut.path != source)
                || source
                    != acyclic_fs::session::changes_path(
                        &self.volumes.filesystem,
                        publication.volume,
                    )
                    .map_err(Error::Stream)?
            {
                return Err(Error::Record("control publication volume differs"));
            }
            let generation = self
                .volumes
                .generation_ref_id(publication.generation, None)
                .map_err(turn)?;
            // One publication can span several records. Deduplicate the same
            // record selected by multiple control keys, never its whole
            // authority revision: later records can carry another control.
            if state
                .source_sequence
                .is_none_or(|through| sequence > through)
            {
                let bindings = [state.private.clone()];
                let authority =
                    agent_authority(&self.issuer, &self.admission, &state.agent, &bindings, &[])
                        .await
                        .map_err(turn)?;
                let grant = crate::conversation::ContentGrant::verify_directory_read(
                    &authority.verifier,
                    &authority.scope,
                    state.private.volume()?,
                    "",
                    &mut None,
                )
                .map_err(turn)?;
                let mut present = [false; 2];
                for (index, path) in ["control/pause", "control/stop"].into_iter().enumerate() {
                    present[index] = self
                        .volumes
                        .stat_private_path(
                            state.private.volume()?,
                            &grant,
                            path,
                            Some(&generation),
                            ContentAccess {
                                ceiling: Some(authority.scope.capabilities()),
                                io: None,
                            },
                        )
                        .await
                        .map_err(turn)?
                        .is_some();
                }
                let target = ActorReference::new(
                    delivery.actor.account().as_bytes(),
                    delivery.actor.namespace().as_str().as_bytes(),
                    state.agent.id().as_str().as_bytes(),
                )
                .map_err(|_| Error::Record("control target identity"))?;
                let closed = state.agent.closed()?;
                match self.store.provider().tail(closed.clone()).await {
                    Ok(_) => return state.transition(effects, true),
                    Err(acyclic_stream::StreamError::NotFound) => {}
                    Err(error) => return Err(Error::Stream(error)),
                }
                effects.push(CommitEffect::hard(
                    vec![CommitCondition::Absent { path: closed }],
                    vec![],
                ));
                let observed = self
                    .store
                    .load(&target)
                    .await
                    .map_err(|error| Error::Actor(Box::new(error.into_cause())))?;
                let target_state = observed.state().ok_or(Error::NoSuchAgent)?;
                let by = Sender::Host {
                    host: "harness".into(),
                };
                let intent: IntentId = format!(
                    "files-{}-{sequence}",
                    hex::encode(record.commit_id.as_bytes())
                )
                .try_into()?;
                let position = crate::supervision::Position {
                    stream: source,
                    sequence,
                };
                let [paused, stopping] = present;
                let mut lifecycle = Vec::new();
                let mut lifecycle_fence = None;
                if stopping || paused != state.paused {
                    if !stopping && !paused {
                        let (run, fence) = crate::supervision::streams::paused_run(
                            self.store.provider().as_ref(),
                            &state.agent,
                        )
                        .await
                        .map_err(turn)?;
                        lifecycle_fence = Some(fence);
                        if let Some(run) = run
                            && let Some(effect) = crate::supervision::streams::Queries::new(
                                self.store.provider().as_ref(),
                                &state.agent,
                                run,
                            )
                            .resume(&position)
                            .await
                            .map_err(turn)?
                        {
                            effects.push(effect);
                        }
                    }
                    let key = crate::agent::intent_key(
                        "file-controls",
                        &[state.agent.as_str(), intent.as_str()],
                    )?;
                    let prepared = store
                        .prepare_paused(&observed, target_state.epoch(), !stopping && paused, key)
                        .map_err(|error| Error::Actor(Box::new(error.into_cause())))?;
                    let request = store
                        .export_prepared(prepared)
                        .await
                        .map_err(|error| Error::Actor(Box::new(error.into_cause())))?;
                    effects.extend(request.effects);
                    if stopping {
                        let activation = self
                            .store
                            .activation(&target)
                            .map_err(|error| Error::Actor(Box::new(error.into_cause())))?;
                        effects.extend(
                            crate::agent::archive(&state.agent, &by, &intent, activation)?.effects,
                        );
                        let root_checkpoint = target_state
                            .checkpoint()
                            .ok_or(Error::Record("root checkpoint is absent"))?;
                        let root_state =
                            crate::agent::checkpoint::decode(&root_checkpoint.bytes)?.state;
                        descendant_subscriptions = self
                            .descendant_status(
                                &delivery.actor,
                                &state.agent,
                                &state.agent,
                                &root_state.open_children,
                                &mut effects,
                            )
                            .await?;
                        if descendant_subscriptions.is_empty() {
                            lifecycle.push(crate::supervision::Lifecycle::Stopped { signal: None });
                            retired = true;
                        }
                    } else {
                        lifecycle.push(if paused {
                            crate::supervision::Lifecycle::Paused { signal: None }
                        } else {
                            crate::supervision::Lifecycle::Resumed {}
                        });
                    }
                    state.paused = paused;
                }
                let steering = async {
                    if stopping
                        || record
                            .headers
                            .keys()
                            .binary_search_by(|key| key.as_ref().cmp(b"control/steer.md"))
                            .is_err()
                    {
                        return Ok(None);
                    }
                    let Some(delta) = self
                        .volumes
                        .read_private_append(
                            state.private.volume()?,
                            &grant,
                            "control/steer.md",
                            &generation,
                            state.steer.as_ref(),
                            self.maximum_bytes,
                            ContentAccess {
                                ceiling: Some(authority.scope.capabilities()),
                                io: None,
                            },
                        )
                        .await
                        .map_err(turn)?
                    else {
                        return Ok(None);
                    };
                    let text = std::str::from_utf8(&delta).map_err(|_| {
                        turn(crate::Error::tool_rejected("steering delta is not UTF-8"))
                    })?;
                    if !text.is_empty() {
                        let content = crate::model::ModelContent::Text(text.to_owned());
                        let (journal, journal_authority) =
                            super::super::agent_turns::AdmittedAgent::verify(
                                &self.issuer,
                                &self.admission,
                                &state.agent,
                                &bindings,
                            )
                            .map_err(turn)?
                            .journal_authority(self.volumes.as_ref(), &[], &[])
                            .await
                            .map_err(turn)?;
                        let body = InputStager {
                            volumes: &self.volumes,
                            authority: &journal_authority,
                            input_authority: Some(&authority),
                            volume: &journal,
                            maximum_bytes: self.maximum_bytes,
                        }
                        .stage(
                            &state.agent,
                            "message",
                            &by,
                            &intent,
                            StagedInput::Content(&content),
                        )
                        .await?
                        .ok_or(Error::Record("steering input missing"))?;
                        effects.extend(
                            crate::agent::send(
                                &state.agent,
                                &by,
                                &crate::agent::InboxRecord::Message(crate::agent::Message {
                                    from: by.clone(),
                                    id: intent.clone(),
                                    body: Box::new(body),
                                    re: None,
                                    attachments: None,
                                }),
                            )?
                            .effects,
                        );
                        effects.push(
                            self.store
                                .activation(&target)
                                .map_err(|error| Error::Actor(Box::new(error.into_cause())))?,
                        );
                    }
                    Ok(Some(generation.clone()))
                }
                .await;
                match steering {
                    Ok(Some(cursor)) => state.steer = Some(cursor),
                    Ok(None) => {}
                    Err(Error::Turn(error))
                        if matches!(
                            error.as_ref(),
                            crate::Error::ToolRejected {
                                code: crate::wire::ErrorCode::Invalid,
                                ..
                            }
                        ) =>
                    {
                        lifecycle.push(crate::supervision::Lifecycle::ControlRejected {
                            path: "control/steer.md".into(),
                        })
                    }
                    Err(error) => return Err(error),
                }
                if !lifecycle.is_empty() {
                    effects.push(
                        self.lifecycle(&state.agent, &position, lifecycle, lifecycle_fence)
                            .await?,
                    );
                }
                state.source_sequence = Some(sequence);
            }
        }
        let mut transition = state.transition(effects, retired)?;
        if !descendant_subscriptions.is_empty() {
            // A stop is terminal: later pause, stop or steering edits must not
            // pause the control actor before the descendant waits finish.
            for id in [PAUSE, STOP, STEER, CLOSED, JOBS, HOOKS] {
                transition
                    .subscriptions
                    .push(SubscriptionChange::Remove { id: id.into() });
            }
        }
        transition.subscriptions.extend(descendant_subscriptions);
        Ok(transition)
    }

    /// One status subscription per open descendant, so a stop acknowledges
    /// only after every descendant has published its archived state. The
    /// traversal pages the persistent child index; it retains no
    /// subtree-sized list. A descendant that already reported a normal
    /// completion is terminal and never publishes `Archived`, so the wait
    /// moves to its open children instead.
    async fn descendant_status(
        &self,
        within: &ActorReference,
        root: &AgentPath,
        parent: &AgentPath,
        children: &crate::agent::Children,
        effects: &mut Vec<CommitEffect>,
    ) -> Result<Vec<SubscriptionChange>, Error> {
        let mut changes = Vec::new();
        let mut stack = Vec::new();
        let mut children = children.clone();
        children.bind(Arc::clone(self.store.provider()));
        stack.push((parent.clone(), children));
        while let Some((parent, children)) = stack.pop() {
            let mut after = None;
            while let Some(id) = children.page(after.as_ref(), 1).await?.pop() {
                after = Some(id.clone());
                let child = parent.child(&id)?;
                let actor = descendant_actor(within, &child)?;
                let observed = self
                    .store
                    .load(&actor)
                    .await
                    .map_err(|error| Error::Actor(Box::new(error.into_cause())))?;
                let actor_state = observed.state().ok_or(Error::NoSuchAgent)?;
                let (state, status) = match actor_state.checkpoint() {
                    Some(checkpoint) => {
                        let state = crate::agent::checkpoint::decode(&checkpoint.bytes)?.state;
                        let status = state.status_tail;
                        (Some(state), status)
                    }
                    // A freshly created child has not published its adopted
                    // checkpoint yet. It has no known children and its first
                    // status is still ahead.
                    None => (None, 0),
                };
                // A child that has not published `Archived` still owns its
                // subtree: its own archive report waits for every open child,
                // so waiting for this status covers descendants it spawns
                // after this read. A closed child that is still archiving is
                // waited on too; only its published `Archived` is final.
                let archived = state.as_ref().is_some_and(|state| {
                    matches!(
                        state.phase,
                        Phase::Done {
                            outcome: AgentOutcome::Archived
                        }
                    )
                });
                let reported = state.as_ref().is_some_and(|state| state.reported);
                if !archived {
                    if actor_state.paused() {
                        let key = crate::agent::intent_key(
                            "file-controls",
                            &[root.as_str(), child.as_str(), "stop"],
                        )?;
                        let prepared = self
                            .store
                            .prepare_paused(&observed, actor_state.epoch(), false, key)
                            .map_err(|error| Error::Actor(Box::new(error.into_cause())))?;
                        let request = self
                            .store
                            .export_prepared(prepared)
                            .await
                            .map_err(|error| Error::Actor(Box::new(error.into_cause())))?;
                        effects.extend(request.effects);
                    }
                    if !reported {
                        changes.push(SubscriptionChange::Add {
                            id: format!("{DESCENDANT_STATUS}{}", child.as_str()),
                            path: child.status()?,
                            from: Some(status),
                        });
                    }
                }
                if let Some(state) = state
                    && state.reported
                {
                    // A reported descendant never publishes a second terminal
                    // status. Its archive cascade still settles its open
                    // children, so walk only that reported frontier. Every
                    // other descendant's Archived status already waits for
                    // its whole subtree.
                    let mut open = state.open_children.clone();
                    open.bind(Arc::clone(self.store.provider()));
                    stack.push((child, open));
                }
            }
        }
        Ok(changes)
    }

    /// The committed open-child index of one descendant. A child without a
    /// checkpoint has not spawned anything yet.
    async fn descendant_children(
        &self,
        within: &ActorReference,
        child: &AgentPath,
    ) -> Result<crate::agent::Children, Error> {
        let actor = descendant_actor(within, child)?;
        let observed = self
            .store
            .load(&actor)
            .await
            .map_err(|error| Error::Actor(Box::new(error.into_cause())))?;
        let actor_state = observed.state().ok_or(Error::NoSuchAgent)?;
        let mut open = match actor_state.checkpoint() {
            Some(checkpoint) => {
                crate::agent::checkpoint::decode(&checkpoint.bytes)?
                    .state
                    .open_children
            }
            None => crate::agent::Children::new(),
        };
        open.bind(Arc::clone(self.store.provider()));
        Ok(open)
    }
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Controls<P, O> {
    /// One append of `events` to the agent's lifecycle stream, fenced by
    /// `fence` or else by the stream's observed tail.
    async fn lifecycle(
        &self,
        agent: &AgentPath,
        position: &crate::supervision::Position,
        events: Vec<crate::supervision::Lifecycle>,
        fence: Option<CommitCondition>,
    ) -> Result<CommitEffect, Error> {
        let path = crate::supervision::streams::path(agent).map_err(turn)?;
        let fence = match fence {
            Some(fence) => fence,
            None => match self.store.provider().tail(path.clone()).await {
                Ok(expected) => CommitCondition::Tail {
                    path: path.clone(),
                    expected,
                },
                Err(acyclic_stream::StreamError::NotFound) => {
                    CommitCondition::Absent { path: path.clone() }
                }
                Err(error) => return Err(Error::Stream(error)),
            },
        };
        let mut records = Vec::new();
        for event in events {
            let record = crate::supervision::Event {
                version: 1,
                agent: agent.clone(),
                run: None,
                source: position.clone(),
                event,
            };
            let admission = matches!(
                record.event,
                crate::supervision::Lifecycle::Paused { .. }
                    | crate::supervision::Lifecycle::Resumed { .. }
                    | crate::supervision::Lifecycle::Stopped { .. }
            );
            let bytes = serde_json::to_vec(&record).map_err(|_| Error::Record("control event"))?;
            let record = if admission {
                acyclic_stream::AppendRecord::latest(
                    bytes,
                    crate::supervision::OWNER_CONTROL_KEY,
                    false,
                )
                .map_err(Error::Stream)?
            } else {
                bytes.into()
            };
            records.push(record);
        }
        Ok(CommitEffect::hard(
            vec![fence],
            vec![CommitMutation::Append { path, records }],
        ))
    }
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Handler
    for Controls<P, O>
{
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    fn admission_class(&self, _: &ActorReference) -> acyclic_actors::guest::AdmissionClass {
        acyclic_actors::guest::AdmissionClass::Control
    }
    fn failure_owner(
        &self,
        actor: &ActorReference,
        checkpoint: Option<&Checkpoint>,
    ) -> Option<ActorReference> {
        // Both control and quiet checkpoints carry their original agent. Read
        // only ownership so an unrelated malformed field cannot hide a failure.
        #[derive(Deserialize)]
        struct Owner {
            agent: AgentPath,
        }
        let owner: Owner = serde_json::from_slice(&checkpoint?.bytes).ok()?;
        ActorReference::new(
            actor.account().as_bytes(),
            actor.namespace().as_str().as_bytes(),
            owner.agent.id().as_str().as_bytes(),
        )
        .ok()
    }
    fn external_subscriptions(&self) -> &[String] {
        &self.external
    }
    fn retain_paused(&self, _: &ActorReference, _: Option<&Checkpoint>) -> bool {
        false
    }
    fn ordered_subscriptions(&self) -> &[String] {
        &self.ordered
    }
    fn revalidate_foreign_conflicts(&self, _: &ActorReference) -> bool {
        true
    }
    fn subscription_filter(
        &self,
        id: &str,
        checkpoint: Option<&Checkpoint>,
    ) -> Result<Option<acyclic_stream::FollowFilter>, GuestError> {
        let key = if [
            super::quiet::ACTIVITY,
            super::quiet::COMPLETED,
            super::quiet::LIFECYCLE,
            super::quiet::CLOSED,
        ]
        .contains(&id)
        {
            super::quiet::key(id, checkpoint).map_err(Error::into_guest)?
        } else {
            match id {
                PAUSE => Some(Bytes::from_static(b"control/pause")),
                STOP => Some(Bytes::from_static(b"control/stop")),
                STEER => Some(Bytes::from_static(b"control/steer.md")),
                JOBS => Some(Bytes::from_static(
                    crate::supervision::tool_jobs::STARTED_KEY,
                )),
                _ => None,
            }
        };
        key.map(acyclic_stream::FollowFilter::exact_key)
            .transpose()
            .map_err(|error| Error::Stream(error).into_guest())
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        _: acyclic_actors::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            self.transition(delivery, checkpoint)
                .await
                .map_err(Error::into_guest)
        })
    }
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static>
    crate::agent::Companions for Controls<P, O>
{
    fn handles(&self, actor: &ActorReference) -> bool {
        actor.id().starts_with(PREFIX) || actor.id().starts_with(super::quiet::PREFIX)
    }
    fn create(
        &self,
        within: &ActorReference,
        agent: &AgentPath,
        volumes: &[WorkspaceEntry],
    ) -> Result<Vec<CommitEffect>, Error> {
        let Some(private) = volumes
            .iter()
            .find(|volume| volume.role == crate::agent::WorkspaceRole::Private)
        else {
            return Ok(vec![]);
        };
        let source = self.source(private.volume()?).map_err(turn)?;
        let actor = self.actor(within, agent)?;
        let state = State {
            version: VERSION,
            agent: agent.clone(),
            private: private.clone(),
            steer: Default::default(),
            source_sequence: None,
            paused: false,
            volumes: volumes.to_vec(),
            hooks: Default::default(),
        };
        let subscriptions = [PAUSE, STOP, STEER]
            .into_iter()
            .map(|id| {
                (
                    id.into(),
                    Subscription {
                        path: source.clone(),
                        cursor: 0,
                        paused: false,
                    },
                )
            })
            .chain([
                (
                    CLOSED.into(),
                    Subscription {
                        path: agent.closed()?,
                        cursor: 0,
                        paused: false,
                    },
                ),
                (
                    JOBS.into(),
                    Subscription {
                        path: crate::supervision::tool_jobs::path(agent).map_err(turn)?,
                        cursor: 0,
                        paused: false,
                    },
                ),
            ])
            .chain(self.hook_source(volumes)?.map(|path| {
                (
                    HOOKS.into(),
                    Subscription {
                        path,
                        cursor: 0,
                        paused: false,
                    },
                )
            }))
            .collect();
        Ok(vec![
            self.store
                .create(
                    None,
                    &actor,
                    Creation {
                        subscriptions,
                        checkpoint: Some(Checkpoint {
                            codec: codec(),
                            bytes: serde_json::to_vec(&state)
                                .map_err(|_| Error::Record("control checkpoint"))?
                                .into(),
                        }),
                        escrow: None,
                    },
                )
                .map_err(|error| Error::Actor(Box::new(error.into_cause())))?,
            self.store
                .activation(&actor)
                .map_err(|error| Error::Actor(Box::new(error.into_cause())))?,
        ])
    }
}
