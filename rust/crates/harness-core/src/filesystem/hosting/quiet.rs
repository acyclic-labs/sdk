//! One durable alarm per admitted tool, using indexed original publications.
use super::*;
use crate::supervision::{Action, Kind, Observation, Policy, Position, tool_jobs};
use acyclic_actors::{
    guest::{AlarmChange, Checkpoint, CodecIdentity, Delivery, Subscription, Transition, Trigger},
    local::{Creation, Store},
};
use acyclic_stream::{CommitEffect, Record, StreamPath, StreamProviderExt as _};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub(super) const PREFIX: &str = "tool-quiet-";
pub(super) const ACTIVITY: &str = "job-output";
pub(super) const COMPLETED: &str = "job-completed";
pub(super) const LIFECYCLE: &str = "job-lifecycle";
pub(super) const CLOSED: &str = "job-closed";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    version: u32,
    agent: AgentPath,
    run: OperationId,
    execution: OperationId,
    call: OperationId,
    quiet_ms: u64,
    action: Action,
    activity: Position,
    at_ms: u64,
    deadline_ms: Option<u64>,
}

fn codec() -> CodecIdentity {
    CodecIdentity {
        name: tool_jobs::CODEC.into(),
        version: "2".into(),
    }
}

fn state(checkpoint: Option<&Checkpoint>) -> Result<State, Error> {
    let checkpoint = checkpoint
        .filter(|value| value.codec == codec())
        .ok_or(Error::Record("quiet checkpoint codec"))?;
    let state: State =
        serde_json::from_slice(&checkpoint.bytes).map_err(|_| Error::Record("quiet checkpoint"))?;
    if state.version != 2 {
        return Err(Error::Record("quiet checkpoint version"));
    }
    Ok(state)
}

fn checkpoint(state: &State) -> Result<Checkpoint, Error> {
    Ok(Checkpoint {
        codec: codec(),
        bytes: serde_json::to_vec(state)
            .map_err(|_| Error::Record("quiet checkpoint"))?
            .into(),
    })
}

pub(super) fn key(id: &str, checkpoint: Option<&Checkpoint>) -> Result<Option<Bytes>, Error> {
    match id {
        ACTIVITY => {
            let state = state(checkpoint)?;
            Ok(Some(
                crate::runtime::tool_output_activity_path(state.execution, state.call).into(),
            ))
        }
        COMPLETED => {
            let state = state(checkpoint)?;
            Ok(Some(
                crate::runtime::tool_output_completion_path(state.execution, state.call).into(),
            ))
        }
        LIFECYCLE => Ok(Some(Bytes::copy_from_slice(
            &state(checkpoint)?.call.into_bytes(),
        ))),
        _ => Ok(None),
    }
}

fn actor(within: &ActorReference, call: OperationId) -> Result<ActorReference, Error> {
    ActorReference::new(
        within.account().as_bytes(),
        within.namespace().as_str().as_bytes(),
        format!("{PREFIX}{call}").as_bytes(),
    )
    .map_err(|_| Error::Record("quiet actor identity"))
}

pub(super) fn create<P: AuthorityProvider + ?Sized>(
    store: &Store<P>,
    within: &ActorReference,
    agent: &AgentPath,
    output: StreamPath,
    record: &Record,
) -> Result<Vec<CommitEffect>, Error> {
    let job: tool_jobs::Record =
        serde_json::from_slice(&record.value).map_err(|_| Error::Record("quiet source record"))?;
    let tool_jobs::Event::Started {
        run,
        execution,
        quiet_ms,
        action,
        at_ms,
        ..
    } = job.event
    else {
        return Err(Error::Record("quiet source is not a start"));
    };
    if job.version != 1 || quiet_ms == 0 {
        return Err(Error::Record("quiet source version or duration"));
    }
    let lifecycle = tool_jobs::path(agent).map_err(turn)?;
    if record.origin != lifecycle {
        return Err(Error::Record("quiet source identity"));
    }
    let actor = actor(within, job.call)?;
    let state = State {
        version: 2,
        agent: agent.clone(),
        run,
        execution,
        call: job.call,
        quiet_ms,
        action,
        activity: Position {
            stream: lifecycle.clone(),
            sequence: record.sequence,
        },
        at_ms,
        deadline_ms: Some(
            at_ms
                .checked_add(quiet_ms)
                .ok_or(Error::Record("quiet deadline overflow"))?,
        ),
    };
    let subscriptions = [
        (
            ACTIVITY.into(),
            Subscription {
                path: output.clone(),
                cursor: 0,
                paused: false,
            },
        ),
        (
            COMPLETED.into(),
            Subscription {
                path: output,
                cursor: 0,
                paused: false,
            },
        ),
        (
            LIFECYCLE.into(),
            Subscription {
                path: lifecycle,
                cursor: record.sequence,
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
    .into_iter()
    .collect();
    Ok(vec![
        store
            .create(
                None,
                &actor,
                Creation {
                    subscriptions,
                    checkpoint: Some(checkpoint(&state)?),
                    escrow: None,
                },
            )
            .map_err(|error| Error::Actor(Box::new(error.into_cause())))?,
        store
            .activation(&actor)
            .map_err(|error| Error::Actor(Box::new(error.into_cause())))?,
    ])
}

// The finite point read is pinned to the captured tail. Every captured tail is
// fenced in the alarm publication, so output or completion racing an alarm
// forces recomputation rather than a stale signal.
async fn latest<P: AuthorityProvider + ?Sized>(
    store: &Store<P>,
    path: StreamPath,
    key: Option<Bytes>,
    effects: &mut Vec<CommitEffect>,
) -> Result<Option<Record>, Error> {
    let (record, condition) = store
        .provider()
        .latest_fenced(path, key.map(Into::into), Default::default())
        .await
        .map_err(Error::Stream)?;
    effects.push(CommitEffect::hard(vec![condition], vec![]));
    Ok(record)
}

pub(super) async fn transition<P: AuthorityProvider + ?Sized>(
    store: &Store<P>,
    delivery: Delivery,
    original: Option<&Checkpoint>,
) -> Result<Transition, Error> {
    let mut state = state(original)?;
    if actor(&delivery.actor, state.call)? != delivery.actor {
        return Err(Error::Record("quiet actor differs"));
    }
    let mut effects = Vec::new();
    let closed = latest(store, state.agent.closed()?, None, &mut effects)
        .await?
        .is_some();
    let jobs = tool_jobs::path(&state.agent).map_err(turn)?;
    let lifecycle = latest(
        store,
        jobs,
        Some(Bytes::copy_from_slice(&state.call.into_bytes())),
        &mut effects,
    )
    .await?
    .ok_or(Error::Record("quiet original job missing"))?;
    let job: tool_jobs::Record = serde_json::from_slice(&lifecycle.value)
        .map_err(|_| Error::Record("quiet lifecycle record"))?;
    if job.version != 1 || job.call != state.call {
        return Err(Error::Record("quiet lifecycle identity"));
    }
    let mut retired = closed || matches!(job.event, tool_jobs::Event::Finished);
    let mut output = None;
    if !retired {
        let source = delivery
            .subscriptions
            .get(COMPLETED)
            .ok_or(Error::Record("quiet completion subscription"))?
            .path
            .clone();
        if delivery
            .subscriptions
            .get(ACTIVITY)
            .map(|bound| &bound.path)
            != Some(&source)
        {
            return Err(Error::Record("quiet output sources differ"));
        }
        let ([completed, activity], condition) = store
            .provider()
            .latest_many_fenced(
                source.clone(),
                [
                    key(COMPLETED, original)?.map(Into::into),
                    key(ACTIVITY, original)?.map(Into::into),
                ],
                Default::default(),
            )
            .await
            .map_err(Error::Stream)?;
        effects.push(CommitEffect::hard(vec![condition], vec![]));
        output = activity.map(|record| (source, record));
        if let Some(record) = completed {
            let context = acyclic_stream::changes_context(&record.value, &record.headers)
                .map_err(Error::Stream)?
                .ok_or(Error::Record("quiet completion publication"))?;
            acyclic_fs::PublicationContext::from_bytes(context).map_err(Error::Stream)?;
            retired = true;
        }
    }
    if !retired {
        if let Some((source, record)) = output {
            if state.activity.stream != source || record.sequence > state.activity.sequence {
                // An exact filesystem change key identifies the output cursor's
                // original atomic publication, whose time survives replay.
                let context = acyclic_stream::changes_context(&record.value, &record.headers)
                    .map_err(Error::Stream)?
                    .ok_or(Error::Record("quiet output publication"))?;
                acyclic_fs::PublicationContext::from_bytes(context).map_err(Error::Stream)?;
                state.at_ms = state.at_ms.max(record.committed_at_micros / 1000);
                state.activity = Position {
                    stream: source,
                    sequence: record.sequence,
                };
                state.deadline_ms = Some(
                    state
                        .at_ms
                        .checked_add(state.quiet_ms)
                        .ok_or(Error::Record("quiet deadline overflow"))?,
                );
            }
        }
        if let Trigger::Alarm { now_unix_ms, .. } = delivery.trigger
            && let Some(deadline_ms) = state
                .deadline_ms
                .filter(|deadline| now_unix_ms >= *deadline)
        {
            let mut policy = Policy {
                quiet_ms: Some(state.quiet_ms),
                ..Default::default()
            };
            policy.actions.insert(Kind::Stuck, state.action);
            let mut projection = crate::supervision::streams::Queries::new(
                store.provider().as_ref(),
                &state.agent,
                state.run,
            )
            .project(
                &state.activity,
                &Observation::ToolAlarm {
                    call: *blake3::hash(&state.call.into_bytes()).as_bytes(),
                    deadline_ms,
                    last_activity_ms: state.at_ms,
                    in_flight: true,
                    now_ms: now_unix_ms,
                },
                &policy,
            )
            .await
            .map_err(turn)?;
            state.deadline_ms = None;
            if state.action != Action::Alert {
                let target = ActorReference::new(
                    delivery.actor.account().as_bytes(),
                    delivery.actor.namespace().as_str().as_bytes(),
                    state.agent.id().as_str().as_bytes(),
                )
                .map_err(|_| Error::Record("quiet target identity"))?;
                let observed = store
                    .load(&target)
                    .await
                    .map_err(|error| Error::Actor(Box::new(error.into_cause())))?;
                let target_state = observed.state().ok_or(Error::NoSuchAgent)?;
                let intent: IntentId = format!("quiet-{}-{deadline_ms}", state.call).try_into()?;
                let key = crate::agent::intent_key(
                    "tool-quiet",
                    &[state.agent.as_str(), intent.as_str()],
                )?;
                let prepared = store
                    .prepare_paused(
                        &observed,
                        target_state.epoch(),
                        state.action == Action::Pause,
                        key,
                    )
                    .map_err(|error| Error::Actor(Box::new(error.into_cause())))?;
                effects.extend(
                    store
                        .export_prepared(prepared)
                        .await
                        .map_err(|error| Error::Actor(Box::new(error.into_cause())))?
                        .effects,
                );
                if state.action == Action::Stop {
                    effects.extend(
                        crate::agent::archive(
                            &state.agent,
                            &Sender::Host {
                                host: "harness".into(),
                            },
                            &intent,
                        )?
                        .effects,
                    );
                    retired = true;
                }
                let event = crate::supervision::Event {
                    version: 1,
                    agent: state.agent.clone(),
                    run: Some(state.run),
                    source: state.activity.clone(),
                    event: if state.action == Action::Pause {
                        crate::supervision::Lifecycle::Paused {
                            signal: Some(Kind::Stuck),
                        }
                    } else {
                        crate::supervision::Lifecycle::Stopped {
                            signal: Some(Kind::Stuck),
                        }
                    },
                };
                projection.append_event(&event).map_err(turn)?;
            }
            effects.push(projection.effect);
        }
    }
    Ok(Transition {
        response: None,
        checkpoint: checkpoint(&state)?,
        alarm: if retired {
            AlarmChange::Clear
        } else {
            state
                .deadline_ms
                .map_or(AlarmChange::Clear, |deadline_unix_ms| AlarmChange::Set {
                    deadline_unix_ms,
                })
        },
        pause: retired,
        effects,
        participant: None,
        subscriptions: Vec::new(),
    })
}
