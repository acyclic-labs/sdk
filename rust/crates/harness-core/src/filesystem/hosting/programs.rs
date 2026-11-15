//! Authored hooks are ordinary actor companions over original source records.
//! Notifications select work; every executable and event is independently read
//! through its original admitted journal and authenticated session.
use super::*;
use crate::{
    agent::{AgentRecord as _, WorkspaceRole},
    config::{ProgramTrigger, Start},
};
use acyclic_actors::{
    guest::{
        AlarmChange, Checkpoint, CodecIdentity, Delivery, Subscription, SubscriptionChange,
        Transition,
    },
    local::{Creation, Store},
};
use acyclic_stream::{CommitEffect, CommitMutation, FollowFilter, Record, StreamPath};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub(super) const CATALOGUE_PREFIX: &str = "program-catalogue-";
pub(super) const HOOK_PREFIX: &str = "program-hook-";
pub(super) const BOOT: &str = "program-boot";
pub(super) const HOME: &str = "program-home";
pub(super) const REGISTRATION: &str = "program-registration";
pub(super) const SOURCE: &str = "program-source";
pub(super) const APPROVAL: &str = "program-approval";
pub(super) const GATE: &str = "program-gate";
pub(super) const CLOSED: &str = "program-closed";
pub(super) const EXTERNAL: [&str; 6] = [BOOT, HOME, SOURCE, APPROVAL, GATE, CLOSED];

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    version: u32,
    agent: AgentPath,
    kind: Kind,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Kind {
    Catalogue {
        operation: Option<OperationId>,
        cursor: Option<crate::plugins::HookCatalogueCursor>,
        installed: BTreeSet<String>,
        seen: BTreeSet<String>,
    },
    Hook {
        name: String,
        registration: Option<Registration>,
        source: Option<Source>,
        deferred: Option<Update>,
        pending: Option<IntentId>,
        blocked: bool,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    operation: OperationId,
    on: ProgramTrigger,
    cut: crate::layers::ContentCut,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Update {
    version: u32,
    agent: AgentPath,
    name: String,
    registration: Option<Registration>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    path: StreamPath,
    file: Option<(VolumeRef, String)>,
    // These are the owning reader's candidates, not a second prefix matcher.
    keys: Vec<(Vec<u8>, Vec<u8>)>,
    from: Start,
}

fn codec() -> CodecIdentity {
    CodecIdentity {
        name: "harness.program-hooks".into(),
        version: "1".into(),
    }
}

fn state(checkpoint: Option<&Checkpoint>) -> Result<State, Error> {
    let checkpoint = checkpoint
        .filter(|value| value.codec == codec())
        .ok_or(Error::Record("program checkpoint codec"))?;
    let state: State = serde_json::from_slice(&checkpoint.bytes)
        .map_err(|_| Error::Record("program checkpoint"))?;
    if state.version != 1 {
        return Err(Error::Record("program checkpoint version"));
    }
    Ok(state)
}

fn transition(
    state: &State,
    effects: Vec<CommitEffect>,
    subscriptions: Vec<SubscriptionChange>,
    alarm: AlarmChange,
    pause: bool,
) -> Result<Transition, Error> {
    Ok(Transition {
        response: None,
        checkpoint: Checkpoint {
            codec: codec(),
            bytes: serde_json::to_vec(state)
                .map_err(|_| Error::Record("program checkpoint"))?
                .into(),
        },
        alarm,
        pause,
        effects,
        subscriptions,
        participant: None,
    })
}

fn actor(
    within: &ActorReference,
    agent: &AgentPath,
    name: Option<&str>,
) -> Result<ActorReference, Error> {
    let id = if let Some(name) = name {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.hooks.actor/v1");
        hash.update(agent.as_str().as_bytes());
        hash.update(&[0]);
        hash.update(name.as_bytes());
        format!("{HOOK_PREFIX}{}", hash.finalize())
    } else {
        format!("{CATALOGUE_PREFIX}{}", agent.id())
    };
    ActorReference::new(
        within.account().as_bytes(),
        within.namespace().as_str().as_bytes(),
        id.as_bytes(),
    )
    .map_err(|_| Error::Record("program actor identity"))
}

fn registration_path(agent: &AgentPath, name: &str) -> Result<StreamPath, Error> {
    StreamPath::new(format!("{}/program-hooks/{name}", agent.as_str())).map_err(Error::Stream)
}

fn alarm(now: u64) -> Result<AlarmChange, Error> {
    Ok(AlarmChange::Set {
        deadline_unix_ms: now
            .checked_add(1)
            .ok_or(Error::Record("program continuation clock overflow"))?,
    })
}

pub(super) fn handles(actor: &ActorReference) -> bool {
    actor.id().starts_with(CATALOGUE_PREFIX) || actor.id().starts_with(HOOK_PREFIX)
}

pub(super) fn filter(
    id: &str,
    checkpoint: Option<&Checkpoint>,
) -> Result<Option<FollowFilter>, Error> {
    if id != SOURCE {
        return Ok(None);
    }
    let state = state(checkpoint)?;
    let Kind::Hook {
        source: Some(source),
        ..
    } = state.kind
    else {
        return Ok(None);
    };
    if source.keys.is_empty() {
        return Ok(None);
    }
    FollowFilter::new(acyclic_stream::wire::SessionFilter {
        keys: source
            .keys
            .into_iter()
            .map(|(start, end)| acyclic_stream::wire::KeyRange {
                start: start.into(),
                end: end.into(),
            })
            .collect(),
        ..Default::default()
    })
    .map(Some)
    .map_err(Error::Stream)
}

pub(super) fn create<P: AuthorityProvider + ?Sized>(
    store: &Store<P>,
    within: &ActorReference,
    agent: &AgentPath,
    volumes: &[WorkspaceEntry],
) -> Result<Vec<CommitEffect>, Error> {
    if !volumes
        .iter()
        .any(|volume| volume.role == WorkspaceRole::Home)
    {
        return Ok(vec![]);
    }
    let actor = actor(within, agent, None)?;
    let state = State {
        version: 1,
        agent: agent.clone(),
        kind: Kind::Catalogue {
            operation: None,
            cursor: None,
            installed: BTreeSet::new(),
            seen: BTreeSet::new(),
        },
    };
    let subscriptions = [
        (
            BOOT.into(),
            Subscription {
                path: agent.status()?,
                cursor: 0,
                paused: false,
            },
        ),
        (
            CLOSED.into(),
            Subscription {
                path: agent.closed()?,
                cursor: 0,
                paused: false,
            },
        ),
    ]
    .into();
    Ok(vec![
        store
            .create(
                None,
                &actor,
                Creation {
                    subscriptions,
                    checkpoint: Some(
                        transition(&state, vec![], vec![], AlarmChange::Clear, false)?.checkpoint,
                    ),
                    escrow: None,
                },
            )
            .map_err(|error| Error::Actor(Box::new(error)))?,
        store
            .activation(&actor)
            .map_err(|error| Error::Actor(Box::new(error)))?,
    ])
}

async fn publish_registration<P: AuthorityProvider + ?Sized>(
    store: &Store<P>,
    within: &ActorReference,
    update: Update,
) -> Result<Vec<CommitEffect>, Error> {
    let target = actor(within, &update.agent, Some(&update.name))?;
    let path = registration_path(&update.agent, &update.name)?;
    let previous = store
        .load(&target)
        .await
        .map_err(|error| Error::Actor(Box::new(error)))?;
    let mut effects = Vec::new();
    let mut resumed = false;
    match previous.state() {
        None => {
            let initial = State {
                version: 1,
                agent: update.agent.clone(),
                kind: Kind::Hook {
                    name: update.name.clone(),
                    registration: None,
                    source: None,
                    deferred: None,
                    pending: None,
                    blocked: false,
                },
            };
            effects.push(
                store
                    .create(
                        None,
                        &target,
                        Creation {
                            subscriptions: [
                                (
                                    REGISTRATION.into(),
                                    Subscription {
                                        path: path.clone(),
                                        cursor: 0,
                                        paused: false,
                                    },
                                ),
                                (
                                    CLOSED.into(),
                                    Subscription {
                                        path: update.agent.closed()?,
                                        cursor: 0,
                                        paused: false,
                                    },
                                ),
                            ]
                            .into(),
                            checkpoint: Some(
                                transition(&initial, vec![], vec![], AlarmChange::Clear, false)?
                                    .checkpoint,
                            ),
                            escrow: None,
                        },
                    )
                    .map_err(|error| Error::Actor(Box::new(error)))?,
            );
        }
        Some(state) if state.paused() => {
            let prepared = store
                .prepare_paused(
                    &previous,
                    state.epoch(),
                    false,
                    acyclic_stream::IdempotencyKey::unique(),
                )
                .map_err(|error| Error::Actor(Box::new(error)))?;
            effects.extend(
                store
                    .export_prepared(prepared)
                    .await
                    .map_err(|error| Error::Actor(Box::new(error)))?
                    .effects,
            );
            resumed = true;
        }
        Some(_) => {}
    }
    effects.push(CommitEffect::hard(
        vec![],
        vec![CommitMutation::Append {
            path,
            records: vec![
                serde_json::to_vec(&update)
                    .map_err(|_| Error::Record("program registration"))?
                    .into(),
            ],
        }],
    ));
    if !resumed {
        effects.push(
            store
                .activation(&target)
                .map_err(|error| Error::Actor(Box::new(error)))?,
        );
    }
    Ok(effects)
}

pub(super) async fn on_record<
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
>(
    store: &Store<P>,
    turns: &Turner<P, O>,
    volumes: &Host<P, O>,
    delivery: Delivery,
    checkpoint: Option<&Checkpoint>,
    stop: acyclic_actors::guest::CancellationToken,
) -> Result<Transition, Error> {
    let mut state = state(checkpoint)?;
    let name = match &state.kind {
        Kind::Catalogue { .. } => None,
        Kind::Hook { name, .. } => Some(name.as_str()),
    };
    if actor(&delivery.actor, &state.agent, name)? != delivery.actor {
        return Err(Error::Record("program checkpoint identity"));
    }
    let id = delivery.trigger.record_position().map(|(id, _)| id);
    if id == Some(CLOSED) {
        return transition(&state, vec![], vec![], AlarmChange::Clear, true);
    }
    let agent =
        crate::agent::observe_state(store.provider(), &delivery.actor, &state.agent).await?;
    match &state.kind {
        Kind::Catalogue { .. } => {
            catalogue(store, turns, volumes, &delivery, &agent, state, stop).await
        }
        Kind::Hook { .. } => hook(turns, volumes, &delivery, &agent, &mut state, stop).await,
    }
}

async fn catalogue<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static>(
    store: &Store<P>,
    turns: &Turner<P, O>,
    volumes: &Host<P, O>,
    delivery: &Delivery,
    agent: &crate::agent::AgentState,
    mut state: State,
    stop: acyclic_actors::guest::CancellationToken,
) -> Result<Transition, Error> {
    let Kind::Catalogue {
        operation,
        cursor,
        installed,
        seen,
    } = &mut state.kind
    else {
        return Err(Error::Record("program catalogue checkpoint"));
    };
    if let Some((id, _)) = delivery.trigger.record_position() {
        let record = delivery
            .record
            .as_ref()
            .ok_or(Error::Record("program catalogue source"))?;
        let source = delivery
            .subscriptions
            .get(id)
            .ok_or(Error::Record("program catalogue subscription"))?;
        if id == BOOT || id == HOME {
            *operation = Some(crate::hooks::effects::record_operation(
                delivery.actor.account(),
                &source.path,
                record,
                "hook-catalogue",
            ));
            *cursor = None;
            seen.clear();
            if id == BOOT {
                let home = agent
                    .volumes
                    .iter()
                    .find(|volume| volume.role == WorkspaceRole::Home)
                    .ok_or(Error::Record("program catalogue home"))?
                    .volume()
                    .map_err(turn)?;
                // Commit the watch before selecting a manifest or its imports.
                return transition(
                    &state,
                    vec![],
                    vec![
                        SubscriptionChange::Remove { id: BOOT.into() },
                        SubscriptionChange::Add {
                            id: HOME.into(),
                            path: volumes.volume_changes(home).map_err(turn)?,
                            from: None,
                        },
                    ],
                    alarm(delivery.now_unix_ms)?,
                    false,
                );
            }
        }
    }
    let operation = operation.ok_or(Error::Record("program catalogue operation"))?;
    let Some(catalogue) = turns
        .program_catalogue(agent, operation, stop)
        .await
        .map_err(turn)?
    else {
        return transition(&state, vec![], vec![], AlarmChange::Clear, false);
    };
    let page = catalogue
        .programs
        .catalogue_page(
            catalogue.authority.journal.as_ref(),
            operation,
            cursor.as_ref(),
            64,
            None,
        )
        .map_err(turn)?
        .await
        .map_err(turn)?;
    let next = page
        .next()
        .map(serde_json::to_vec)
        .transpose()
        .map_err(|_| Error::Record("program catalogue continuation"))?
        .map(|bytes| serde_json::from_slice(&bytes))
        .transpose()
        .map_err(|_| Error::Record("program catalogue continuation"))?;
    let mut effects = Vec::new();
    for name in page.names() {
        if !seen.insert(name.clone()) {
            continue;
        }
        let (program, cut) = catalogue
            .programs
            .hook_for_execution(catalogue.authority.journal.as_ref(), operation, name, None)
            .map_err(turn)?
            .await
            .map_err(turn)?;
        effects.extend(
            publish_registration(
                store,
                &delivery.actor,
                Update {
                    version: 1,
                    agent: state.agent.clone(),
                    name: name.clone(),
                    registration: Some(Registration {
                        operation,
                        on: program.on().clone(),
                        cut: cut.selection,
                    }),
                },
            )
            .await?,
        );
        installed.insert(name.clone());
    }
    *cursor = next;
    let continuation = if cursor.is_some() {
        alarm(delivery.now_unix_ms)?
    } else {
        for name in installed.difference(seen) {
            effects.extend(
                publish_registration(
                    store,
                    &delivery.actor,
                    Update {
                        version: 1,
                        agent: state.agent.clone(),
                        name: name.clone(),
                        registration: None,
                    },
                )
                .await?,
            );
        }
        *installed = std::mem::take(seen);
        AlarmChange::Clear
    };
    transition(&state, effects, vec![], continuation, false)
}

async fn source<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static>(
    turns: &Turner<P, O>,
    volumes: &Host<P, O>,
    agent: &crate::agent::AgentState,
    on: &ProgramTrigger,
) -> crate::Result<Option<Source>> {
    match on {
        ProgramTrigger::Lifecycle { .. } => Ok(None),
        ProgramTrigger::Stream { path, from } => {
            let path = StreamPath::new(path.clone())?;
            let client = turns.source_session(agent).await?;
            let mut check = acyclic_stream::FollowRequest::new(path.clone(), 0);
            check.until = Some(0);
            match client.follow_records(check).await {
                Ok(_) => {}
                Err(error) if error.category() == acyclic_stream::StreamError::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            Ok(Some(Source {
                path,
                file: None,
                keys: vec![],
                from: *from,
            }))
        }
        ProgramTrigger::File {
            volume,
            prefix,
            from,
        } => {
            let entry = agent
                .volumes
                .iter()
                .find(|entry| &entry.name == volume)
                .ok_or_else(|| {
                    crate::Error::Unauthorized("hook volume is not bound to its agent".into())
                })?;
            let volume = entry.volume()?;
            let prefix = if entry.root.is_empty() {
                prefix.clone()
            } else if prefix.is_empty() {
                entry.root.clone()
            } else {
                format!("{}/{prefix}", entry.root)
            };
            let (_, selection) = turns.file_source_session(agent, volume, &prefix).await?;
            let keys = selection
                .filter()
                .transport_filter()?
                .keys
                .into_iter()
                .map(|range| (range.start.to_vec(), range.end.to_vec()))
                .collect();
            Ok(Some(Source {
                path: volumes.volume_changes(volume)?,
                file: Some((volume.clone(), prefix)),
                keys,
                from: *from,
            }))
        }
    }
}

async fn event<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static>(
    turns: &Turner<P, O>,
    agent: &crate::agent::AgentState,
    source: &Source,
    wake: &Record,
) -> crate::Result<Option<Record>> {
    let until = wake
        .sequence
        .checked_add(1)
        .ok_or_else(|| crate::Error::Invalid("hook source sequence overflow".into()))?;
    let record = if let Some((volume, prefix)) = &source.file {
        let (client, selected) = turns.file_source_session(agent, volume, prefix).await?;
        let mut records = client
            .follow_data(
                acyclic_stream::wire::SessionFollow {
                    resource: selected.resource().to_owned(),
                    delivery: acyclic_stream::wire::SessionDelivery::All as i32,
                    until: Some(until),
                    ..Default::default()
                },
                wake.sequence,
                selected.filter().clone(),
            )
            .await?;
        match records.try_next().await? {
            None => None,
            Some(data) => match data.item {
                Some(acyclic_stream::wire::session_data::Item::Record(record)) => {
                    Some(acyclic_stream::wire_codec::record(record, None)?)
                }
                _ => {
                    return Err(crate::Error::Invalid(
                        "hook source returned another payload".into(),
                    ));
                }
            },
        }
    } else {
        let client = turns.source_session(agent).await?;
        let mut request = acyclic_stream::FollowRequest::new(source.path.clone(), wake.sequence);
        request.until = Some(until);
        client.follow_records(request).await?.try_next().await?
    };
    if let Some(record) = &record {
        if record.sequence != wake.sequence
            || record.commit_id != wake.commit_id
            || record.origin != wake.origin
        {
            return Err(crate::Error::Conflict(
                "hook notification differs from its original source".into(),
            ));
        }
    }
    Ok(record)
}

enum Outcome {
    Done,
    Approval(IntentId),
    Controlled(crate::supervision::Control),
}

async fn execute<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static>(
    turns: &Turner<P, O>,
    actor: &ActorReference,
    agent: &crate::agent::AgentState,
    name: &str,
    registration: &Registration,
    source: &Source,
    wake: &Record,
    stop: acyclic_actors::guest::CancellationToken,
) -> crate::Result<Outcome> {
    let Some(record) = event(turns, agent, source, wake).await? else {
        return Ok(Outcome::Done);
    };
    let operation =
        crate::hooks::effects::record_operation(actor.account(), &source.path, &record, name);
    let catalogue = turns
        .program_catalogue(agent, operation, stop.clone())
        .await?
        .ok_or_else(|| crate::Error::Unsupported("hook programs are not bound".into()))?;
    let (program, _cut) = catalogue
        .programs
        .hook_for_event(
            catalogue.authority.journal.as_ref(),
            operation,
            name,
            &registration.cut,
            None,
        )?
        .await?;
    let issued = turns.invocation(agent, operation, stop).await?;
    let call = crate::tool::ToolInvocation {
        operation_id: crate::hooks::effects::phase_operation(operation, registration.on.event()),
        call_id: name.to_owned(),
        name: program.binding().definition.name.clone(),
        arguments: serde_json::json!({"version": 1,
        "hook_event_name": registration.on.event().name(),
        "stream_path": source.path.as_str(),
        "path": source.file.as_ref().map(|(_, prefix)| prefix.as_str()),
        "record": {
            "sequence": record.sequence.to_string(),
            "origin": record.origin.as_str(),
            "commit": record.commit_id.as_bytes(),
            "committed_at_micros": record.committed_at_micros.to_string(),
            "value": record.value.as_ref(),
            "keys": record.headers.keys().iter().map(|key| key.as_ref()).collect::<Vec<_>>(),
            "attributes": record.headers.attributes().iter().map(|attribute|
                serde_json::json!({"name": attribute.name, "value": attribute.value.as_ref()})
            ).collect::<Vec<_>>(),
        }}),
    };
    let mut context = Vec::new();
    match issued
        .executor
        .settle_invocation(
            issued.authority.journal.as_ref(),
            (operation, 0),
            (call, program.binding().clone()),
            &serde_json::Value::Null,
            &mut context,
            None,
        )?
        .await?
    {
        crate::executor::ExecutionProgress::Ready(_)
        | crate::executor::ExecutionProgress::Failed(_) => Ok(Outcome::Done),
        crate::executor::ExecutionProgress::Pending(_) => {
            let (id, _, _) = issued.approvals.pending()?.ok_or_else(|| {
                crate::Error::Invalid("hook approval has no original request".into())
            })?;
            Ok(Outcome::Approval(IntentId::try_from(
                crate::approvals::agent_request_id(id),
            )?))
        }
        crate::executor::ExecutionProgress::Controlled(control) => Ok(Outcome::Controlled(control)),
        crate::executor::ExecutionProgress::PreDispatchFailed(error) => Err(error.into_error()),
    }
}

async fn hook<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static>(
    turns: &Turner<P, O>,
    volumes: &Host<P, O>,
    delivery: &Delivery,
    agent: &crate::agent::AgentState,
    state: &mut State,
    stop: acyclic_actors::guest::CancellationToken,
) -> Result<Transition, Error> {
    let Kind::Hook {
        name,
        registration,
        source: selected,
        deferred,
        pending,
        blocked,
    } = &mut state.kind
    else {
        return Err(Error::Record("hook checkpoint"));
    };
    let mut subscriptions = Vec::new();
    let mut continuation = AlarmChange::Clear;
    let mut paused = false;
    let mut update = None;
    match delivery.trigger.record_position() {
        Some((REGISTRATION, _)) => {
            let record = delivery
                .record
                .as_ref()
                .ok_or(Error::Record("hook registration source"))?;
            let next: Update = serde_json::from_slice(&record.value)
                .map_err(|_| Error::Record("hook registration"))?;
            if next.version != 1 || next.agent != state.agent || &next.name != name {
                return Err(Error::Record("hook registration identity"));
            }
            if pending.is_some() || *blocked {
                *deferred = Some(next);
            } else {
                update = Some(next);
            }
        }
        Some((APPROVAL, _)) => {
            let record = delivery
                .record
                .as_ref()
                .ok_or(Error::Record("hook approval source"))?;
            if let crate::agent::InboxRecord::Approval(approval) =
                crate::agent::InboxRecord::decode(&record.value)?
            {
                if pending.as_ref() == Some(&approval.id) {
                    subscriptions.push(SubscriptionChange::Resume { id: SOURCE.into() });
                }
            }
        }
        Some((GATE, _)) => {
            subscriptions.push(SubscriptionChange::Remove { id: GATE.into() });
            subscriptions.push(SubscriptionChange::Resume { id: SOURCE.into() });
        }
        Some((SOURCE, _)) => {
            let source = selected
                .as_ref()
                .ok_or(Error::Record("hook source is not selected"))?;
            if delivery
                .subscriptions
                .get(SOURCE)
                .is_none_or(|cut| cut.path != source.path)
            {
                return Err(Error::Record("hook source subscription differs"));
            }
            let wake = delivery
                .record
                .as_ref()
                .ok_or(Error::Record("hook source record"))?;
            let outcome = execute(
                turns,
                &delivery.actor,
                agent,
                name,
                registration
                    .as_ref()
                    .ok_or(Error::Record("hook registration absent"))?,
                source,
                wake,
                stop,
            )
            .await;
            let outcome = match outcome {
                Err(crate::Error::ExecutionControlled(control)) => Outcome::Controlled(control),
                value => value.map_err(turn)?,
            };
            match outcome {
                Outcome::Done => {
                    *pending = None;
                    *blocked = false;
                    update = deferred.take();
                }
                Outcome::Approval(id) => {
                    *pending = Some(id);
                    subscriptions.push(SubscriptionChange::Pause { id: SOURCE.into() });
                }
                Outcome::Controlled(control) => {
                    *blocked = true;
                    if control.action == crate::supervision::Action::Stop {
                        paused = true;
                    } else {
                        subscriptions.push(SubscriptionChange::Pause { id: SOURCE.into() });
                        subscriptions.push(SubscriptionChange::Add {
                            id: GATE.into(),
                            path: control.source.stream,
                            from: Some(
                                control
                                    .source
                                    .sequence
                                    .checked_add(1)
                                    .ok_or(Error::Record("hook gate cursor overflow"))?,
                            ),
                        });
                    }
                }
            }
        }
        None => {
            if let Some(source) = selected {
                subscriptions.push(SubscriptionChange::Add {
                    id: SOURCE.into(),
                    path: source.path.clone(),
                    from: match source.from {
                        Start::Start => Some(0),
                        Start::Now => None,
                    },
                });
                if delivery.subscriptions.get(APPROVAL).is_none() {
                    subscriptions.push(SubscriptionChange::Add {
                        id: APPROVAL.into(),
                        path: agent.path.inbox().map_err(Error::from)?,
                        from: None,
                    });
                }
            }
        }
        Some(_) => return Err(Error::Record("hook input is undeclared")),
    }
    if let Some(update) = update {
        let next = match update.registration.as_ref() {
            Some(registration) => source(turns, volumes, agent, &registration.on)
                .await
                .map_err(turn)?,
            None => None,
        };
        let same = next
            .as_ref()
            .zip(selected.as_ref())
            .is_some_and(|(next, old)| next.path == old.path);
        if !same {
            if selected.is_some() {
                subscriptions.push(SubscriptionChange::Remove { id: SOURCE.into() });
            }
            if next.is_some() {
                continuation = alarm(delivery.now_unix_ms)?;
            }
        }
        paused = update.registration.is_none();
        *registration = update.registration;
        *selected = next;
    }
    transition(state, vec![], subscriptions, continuation, paused)
}
