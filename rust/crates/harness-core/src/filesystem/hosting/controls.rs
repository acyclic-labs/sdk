//! Private control files are original filesystem events consumed by an actor.
//! Its checkpoint and every admission/inbox effect share one publication.
use super::*;
use acyclic_actors::{
    guest::{
        AlarmChange, Checkpoint, CodecIdentity, Delivery, GuestError, GuestFuture, Handler,
        Manifest, Subscription, Transition,
    },
    local::{Creation, Store},
};
use acyclic_stream::{
    CommitCondition, CommitEffect, CommitMutation, StreamPath, StreamProviderExt as _,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

const PREFIX: &str = "file-controls-";
const PAUSE: &str = "file-pause";
const STOP: &str = "file-stop";
const STEER: &str = "file-steer";
const CLOSED: &str = "file-closed";
const JOBS: &str = "job-created";
const TITLE: &str = "session-title";
const GOAL: &str = "session-goal";
const PEERS: &str = "session-membership";

mod presence;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    version: u32,
    agent: AgentPath,
    sources: crate::agent::WorkspaceTable,
    steer: Option<crate::resources::GenerationRef>,
    source_sequence: Option<u64>,
    metadata_private_from: Option<u64>,
    metadata_home_from: Option<u64>,
    paused: bool,
}

impl State {
    fn private(sources: &crate::agent::WorkspaceTable) -> Result<&WorkspaceEntry, Error> {
        sources
            .entries()
            .iter()
            .find(|entry| entry.role == crate::agent::WorkspaceRole::Private)
            .ok_or(Error::Record("control private binding missing"))
    }
    fn home(sources: &crate::agent::WorkspaceTable) -> Option<&WorkspaceEntry> {
        sources
            .entries()
            .iter()
            .find(|entry| entry.role == crate::agent::WorkspaceRole::Home)
    }

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
        version: "7".into(),
    }
}
fn guest(error: impl std::fmt::Display) -> GuestError {
    GuestError::Handler(error.to_string())
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
    routing: Option<crate::agent::Routing>,
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Controls<P, O> {
    pub(super) fn new(
        issuer: RuntimeAuthority,
        admission: TurnAdmission,
        volumes: Arc<Host<P, O>>,
        maximum_bytes: Option<u64>,
        limits: ActorLimits,
        routing: Option<crate::agent::Routing>,
    ) -> Result<Self, Error> {
        let external: Vec<String> = [
            PAUSE,
            STOP,
            STEER,
            CLOSED,
            JOBS,
            TITLE,
            GOAL,
            PEERS,
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
        code.update(include_bytes!("controls/presence.rs"));
        code.update(include_bytes!("quiet.rs"));
        code.update(include_bytes!("../../supervision/tool_jobs.rs"));
        Ok(Self {
            ordered: [PAUSE, STOP, STEER, TITLE, GOAL].map(str::to_owned).into(),
            routing,
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
        if state.version != 7 || self.actor(&delivery.actor, &state.agent)? != delivery.actor {
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
        if subscription == PEERS {
            return self.membership_transition(&delivery, state).await;
        }
        let mut retired = subscription == CLOSED;
        if !retired {
            let selected = if subscription == GOAL {
                State::home(&state.sources).ok_or(Error::Record("goal has no home binding"))?
            } else {
                State::private(&state.sources)?
            };
            let private_source =
                selected.as_volume() == State::private(&state.sources)?.as_volume();
            let source = self.source(selected.volume()?).map_err(turn)?;
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
            let through = if private_source {
                state.source_sequence
            } else {
                state
                    .metadata_home_from
                    .and_then(|from| from.checked_sub(1))
            };
            if through.is_none_or(|through| sequence > through) {
                let bindings = if private_source {
                    std::slice::from_ref(State::private(&state.sources)?)
                } else {
                    state.sources.entries()
                };
                let authority =
                    agent_authority(&self.issuer, &self.admission, &state.agent, bindings, &[])
                        .await
                        .map_err(turn)?;
                let grant = crate::conversation::ContentGrant::verify_directory_read(
                    &authority.verifier,
                    &authority.scope,
                    selected.volume()?,
                    "",
                    &mut None,
                )
                .map_err(turn)?;
                let changed = |path: &[u8]| {
                    record
                        .headers
                        .keys()
                        .binary_search_by(|key| key.as_ref().cmp(path))
                        .is_ok()
                };
                let metadata_from = if private_source {
                    state.metadata_private_from
                } else {
                    state.metadata_home_from
                };
                if metadata_from.is_some_and(|from| sequence >= from)
                    && (changed(b"ui/title") || changed(b"GOAL.md"))
                {
                    effects.extend(
                        self.metadata_effects(
                            &delivery,
                            &state,
                            selected,
                            &generation,
                            &grant,
                            authority.scope.capabilities(),
                        )
                        .await?,
                    );
                    let next = Some(
                        sequence
                            .checked_add(1)
                            .ok_or(Error::Record("metadata source cursor overflow"))?,
                    );
                    if private_source {
                        state.metadata_private_from = next;
                    } else {
                        state.metadata_home_from = next;
                    }
                    if !private_source
                        || ![
                            b"control/pause".as_slice(),
                            b"control/stop",
                            b"control/steer.md",
                        ]
                        .into_iter()
                        .any(changed)
                    {
                        if private_source {
                            state.source_sequence = Some(sequence);
                        }
                        return state.transition(effects, false);
                    }
                }
                if !private_source {
                    return state.transition(effects, false);
                }
                let mut present = [false; 2];
                for (index, path) in ["control/pause", "control/stop"].into_iter().enumerate() {
                    present[index] = self
                        .volumes
                        .stat_private_path(
                            State::private(&state.sources)?.volume()?,
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
                    .map_err(|error| Error::Actor(Box::new(error)))?;
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
                        .map_err(|error| Error::Actor(Box::new(error)))?;
                    let request = store
                        .export_prepared(prepared)
                        .await
                        .map_err(|error| Error::Actor(Box::new(error)))?;
                    effects.extend(request.effects);
                    if stopping {
                        effects.extend(crate::agent::archive(&state.agent, &by, &intent)?.effects);
                        lifecycle.push(crate::supervision::Lifecycle::Stopped { signal: None });
                        retired = true;
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
                            State::private(&state.sources)?.volume()?,
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
                                bindings,
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
                                .map_err(|error| Error::Actor(Box::new(error)))?,
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
                    let path = crate::supervision::streams::path(&state.agent).map_err(turn)?;
                    let fence = match lifecycle_fence {
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
                    for event in lifecycle {
                        let record = crate::supervision::Event {
                            version: 1,
                            agent: state.agent.clone(),
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
                        let bytes = serde_json::to_vec(&record)
                            .map_err(|_| Error::Record("control event"))?;
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
                    effects.push(CommitEffect::hard(
                        vec![fence],
                        vec![CommitMutation::Append { path, records }],
                    ));
                }
                state.source_sequence = Some(sequence);
            }
        }
        state.transition(effects, retired)
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
    fn subscription_key(
        &self,
        id: &str,
        checkpoint: Option<&Checkpoint>,
    ) -> Result<Option<Bytes>, GuestError> {
        if [
            super::quiet::ACTIVITY,
            super::quiet::COMPLETED,
            super::quiet::LIFECYCLE,
            super::quiet::CLOSED,
        ]
        .contains(&id)
        {
            return super::quiet::key(id, checkpoint).map_err(guest);
        }
        Ok(match id {
            PAUSE => Some(Bytes::from_static(b"control/pause")),
            STOP => Some(Bytes::from_static(b"control/stop")),
            STEER => Some(Bytes::from_static(b"control/steer.md")),
            TITLE => Some(Bytes::from_static(b"ui/title")),
            GOAL => Some(Bytes::from_static(b"GOAL.md")),
            JOBS => Some(Bytes::from_static(
                crate::supervision::tool_jobs::STARTED_KEY,
            )),
            _ => None,
        })
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        _: acyclic_actors::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move { self.transition(delivery, checkpoint).await.map_err(guest) })
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
            version: 7,
            agent: agent.clone(),
            sources: crate::agent::WorkspaceTable::new(
                volumes
                    .iter()
                    .filter(|entry| {
                        entry.role == crate::agent::WorkspaceRole::Private
                            || (agent.depth() == 0
                                && self.routing.is_some()
                                && entry.role == crate::agent::WorkspaceRole::Home)
                    })
                    .cloned()
                    .collect(),
            )?,
            steer: Default::default(),
            source_sequence: None,
            metadata_private_from: None,
            metadata_home_from: None,
            paused: false,
        };
        let metadata = if agent.depth() == 0 && self.routing.is_some() {
            Some((PEERS, agent.peer_membership()?))
        } else {
            None
        };
        let subscriptions: std::collections::BTreeMap<String, Subscription> = [PAUSE, STOP, STEER]
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
            .chain(metadata.map(|(id, path)| {
                (
                    id.into(),
                    Subscription {
                        path,
                        cursor: 0,
                        paused: false,
                    },
                )
            }))
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
                .map_err(|error| Error::Actor(Box::new(error)))?,
            self.store
                .activation(&actor)
                .map_err(|error| Error::Actor(Box::new(error)))?,
        ])
    }
}
