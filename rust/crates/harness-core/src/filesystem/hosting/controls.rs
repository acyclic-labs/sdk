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

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    version: u32,
    agent: AgentPath,
    private: WorkspaceEntry,
    steer: Option<crate::resources::GenerationRef>,
    source_sequence: Option<u64>,
    paused: bool,
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
        version: "5".into(),
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
            super::quiet::ACTIVITY,
            super::quiet::LIFECYCLE,
            super::quiet::CLOSED,
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        let mut code = blake3::Hasher::new();
        code.update(include_bytes!("controls.rs"));
        code.update(include_bytes!("quiet.rs"));
        code.update(include_bytes!("../../supervision/tool_jobs.rs"));
        Ok(Self {
            ordered: vec![PAUSE.into(), STOP.into(), STEER.into()],
            store: crate::agent::actor_store(&volumes.filesystem.stream_provider())?,
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
        if delivery.actor.id().starts_with(super::quiet::PREFIX) {
            return super::quiet::transition(&self.store, delivery, checkpoint).await;
        }
        let checkpoint = checkpoint
            .filter(|checkpoint| checkpoint.codec == codec())
            .ok_or(Error::Record("control checkpoint codec"))?;
        let mut state: State = serde_json::from_slice(&checkpoint.bytes)
            .map_err(|_| Error::Record("control checkpoint"))?;
        if state.version != 5 || self.actor(&delivery.actor, &state.agent)? != delivery.actor {
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
        let mut retired = subscription == CLOSED;
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
                        let (run, fence) = crate::supervision::streams::active_run(
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
                    let request = self
                        .store
                        .prepare_paused(&observed, target_state.epoch(), !stopping && paused, key)
                        .and_then(acyclic_actors::local::PreparedPublish::into_request)
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
                            .journal_authority(&[])
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
                        effects.push(self.store.activation(&target));
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
            version: 5,
            agent: agent.clone(),
            private: private.clone(),
            steer: Default::default(),
            source_sequence: None,
            paused: false,
        };
        let subscriptions = [PAUSE, STOP, STEER]
            .into_iter()
            .map(|id| {
                (
                    id.into(),
                    Subscription {
                        path: source.clone(),
                        cursor: 0,
                    },
                )
            })
            .chain([
                (
                    CLOSED.into(),
                    Subscription {
                        path: agent.closed()?,
                        cursor: 0,
                    },
                ),
                (
                    JOBS.into(),
                    Subscription {
                        path: crate::supervision::tool_jobs::path(agent).map_err(turn)?,
                        cursor: 0,
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
            self.store.activation(&actor),
        ])
    }
}
