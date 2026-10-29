//! Bounded exhaustive model check of checkpoint/alarm/effect atomicity.
//!
//! A deterministic driver runs a fixed actor script (record with alarm Set,
//! the alarm firing with Keep, then a record with Clear whose effects include
//! a Fork and a foreign append) through the real `Store` over a real
//! `MemoryStream`. Every schedule of up to two commit faults (dropped before
//! apply, or applied with a lost acknowledgement) is explored. After every
//! observed cut the durable tuple must match exactly one prefix of the script.
//! The split publication shape (state, then effects) is the negative control.
use super::*;
use crate::guest::CodecIdentity;
use acyclic_stream::{
    CommitId, CommittedEnvelope, IdempotencyObservation, MemoryStream, ReadRequest, RecordStream,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use std::sync::atomic::{AtomicUsize, Ordering};

mod composition;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Fault {
    None,
    /// The commit never reaches the provider.
    Drop,
    /// The commit applies, then the acknowledgement is lost.
    LoseAck,
    /// The provider refuses this request before applying it.
    Refuse(StreamError),
}

struct Faulty {
    inner: MemoryStream,
    plan: Vec<Fault>,
    calls: AtomicUsize,
}

macro_rules! delegate {
    ($($name:ident($arg:ident: $input:ty) -> $output:ty;)*) => {
        #[async_trait::async_trait]
        impl StreamProvider for Faulty {
            fn context(&self) -> acyclic_stream::ProviderContext<'_> {
                acyclic_stream::ProviderContext::new(self.inner.context().operation_claims())
            }
            $(async fn $name(&self, $arg: $input) -> Result<$output, StreamError> {
                self.inner.$name($arg).await
            })*
            async fn follow(&self, request: acyclic_stream::FollowRequest) -> Result<acyclic_stream::FollowStream, StreamError> {
                self.inner.follow(request).await
            }
            fn write(&self, request: acyclic_stream::WriteRequest)
                -> acyclic_stream::BoxProviderFuture<'_, std::result::Result<acyclic_stream::WriteOutcome, StreamError>> {
                let acyclic_stream::WriteRequest::Commit(request) = request else {
                    return self.inner.write(request);
                };
                Box::pin(async move {
                    let call = self.calls.fetch_add(1, Ordering::SeqCst);
                    match self.plan.get(call).cloned().unwrap_or(Fault::None) {
                        Fault::None => self.inner.write(acyclic_stream::WriteRequest::Commit(request)).await,
                        Fault::Drop => Err(StreamError::Unavailable),
                        Fault::Refuse(error) => Err(error),
                        Fault::LoseAck => {
                            self.inner.write(acyclic_stream::WriteRequest::Commit(request)).await?;
                            Err(StreamError::Unavailable)
                        }
                    }
                })
            }
        }
    };
}

delegate! {
    heads(request: acyclic_stream::HeadsRequest) -> acyclic_stream::Heads;
    inspect_idempotency(key: IdempotencyKey) -> Option<IdempotencyObservation>;
    read(request: ReadRequest) -> RecordStream;
    read_commit(id: CommitId) -> CommittedEnvelope;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// The production shape: one commit.
    Atomic,
    /// The old shape: the state record commits, then the effects separately.
    Split,
}

type Failure = Box<dyn std::error::Error>;

fn path(value: &str) -> Result<StreamPath, StreamError> {
    StreamPath::new(value)
}

fn checkpoint(step: u8) -> Checkpoint {
    Checkpoint {
        codec: CodecIdentity {
            name: "model".into(),
            version: "1".into(),
        },
        bytes: Bytes::copy_from_slice(&[step]),
    }
}

fn output(published: u64, value: &'static [u8]) -> Result<Effect, StreamError> {
    let out = path("model/out")?;
    Ok(Effect {
        conditions: vec![if published == 0 {
            CommitCondition::Absent { path: out.clone() }
        } else {
            CommitCondition::Tail {
                path: out.clone(),
                expected: published,
            }
        }],
        mutation: CommitMutation::Append {
            path: out,
            records: vec![Bytes::from_static(value).into()],
        },
    })
}

/// The script: which delivery is next at this cut, and its transition.
fn script(state: &State) -> Result<Option<(Trigger, Transition)>, Failure> {
    let step = state
        .checkpoint()
        .and_then(|value| value.bytes.first().copied())
        .unwrap_or(0);
    let record = |sequence| Trigger::Record {
        subscription: "inbox".into(),
        sequence,
    };
    Ok(match step {
        0 => Some((
            record(0),
            Transition {
                response: None,
                participant: None,
                checkpoint: checkpoint(1),
                alarm: AlarmChange::Set {
                    deadline_unix_ms: 10,
                },
                effects: vec![output(0, b"e0")?],
            },
        )),
        1 => Some((
            Trigger::Alarm {
                generation: 1,
                now_unix_ms: 10,
            },
            Transition {
                response: None,
                participant: None,
                checkpoint: checkpoint(2),
                alarm: AlarmChange::Keep,
                effects: vec![output(1, b"a1")?],
            },
        )),
        2 => Some((
            record(1),
            Transition {
                response: None,
                participant: None,
                checkpoint: checkpoint(3),
                alarm: AlarmChange::Clear,
                effects: vec![
                    output(2, b"e1")?,
                    Effect {
                        conditions: vec![CommitCondition::Absent {
                            path: path("model/child")?,
                        }],
                        mutation: CommitMutation::Fork {
                            source: path("model/inbox")?,
                            destination: path("model/child")?,
                            at_tail: 2,
                            records: Vec::new(),
                            exclude: Vec::new(),
                        },
                    },
                    Effect {
                        conditions: vec![CommitCondition::Absent {
                            path: path("model/audit")?,
                        }],
                        mutation: CommitMutation::Append {
                            path: path("model/audit")?,
                            records: vec![Bytes::from_static(b"spawned").into()],
                        },
                    },
                ],
            },
        )),
        _ => None,
    })
}

async fn values(provider: &MemoryStream, stream: &str) -> Result<Vec<Bytes>, Failure> {
    let stream = path(stream)?;
    let tail = match provider.tail(stream.clone()).await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    if tail == 0 {
        return Ok(Vec::new());
    }
    let records: Vec<_> = provider
        .read(ReadRequest {
            key: None,
            path: stream,
            from: 0,
            limit: u32::try_from(tail)?,
            at_least: Default::default(),
        })
        .await?
        .try_collect()
        .await?;
    Ok(records
        .into_iter()
        .map(|record| Bytes::copy_from_slice(&record.value))
        .collect())
}

/// The durable tuple must equal exactly the script prefix its checkpoint names.
async fn check(provider: &MemoryStream, state: &State) -> Result<(), Failure> {
    let step = state
        .checkpoint()
        .and_then(|value| value.bytes.first().copied())
        .unwrap_or(0);
    let outputs: &[&[u8]] = &[b"e0", b"a1", b"e1"];
    let expected_out: Vec<Bytes> = outputs
        .iter()
        .take(usize::from(step))
        .map(|value| Bytes::from_static(value))
        .collect();
    let (cursor, generation, deadline, pending) = match step {
        0 => (0, 0, None, None),
        1 => (1, 1, Some(10), Some(10)),
        2 => (1, 1, Some(10), None),
        _ => (2, 2, None, None),
    };
    let observed_out = values(provider, "model/out").await?;
    let child = provider.tail(path("model/child")?).await.is_ok();
    let audit = !values(provider, "model/audit").await?.is_empty();
    if observed_out != expected_out
        || state.cursor("inbox") != Some(cursor)
        || state.alarm().generation() != generation
        || state.alarm().deadline_unix_ms() != deadline
        || state.alarm().pending() != pending
        || child != (step >= 3)
        || audit != (step >= 3)
    {
        return Err(format!(
            "torn tuple at step {step}: out {observed_out:?}, cursor {:?}, alarm {:?}, child {child}, audit {audit}",
            state.cursor("inbox"),
            state.alarm()
        )
        .into());
    }
    Ok(())
}

async fn run(shape: Shape, plan: Vec<Fault>) -> Result<(), Failure> {
    let provider = Arc::new(Faulty {
        inner: MemoryStream::default(),
        plan,
        calls: AtomicUsize::new(0),
    });
    let store = Store::new(Arc::clone(&provider), path("model/actors")?);
    let actor = ActorReference::new(b"account", b"model", b"actor")?;
    // Setup is fault-free: the inbox, the companion create, the placement.
    let setup = Store::new(Arc::new(provider.inner.clone()), path("model/actors")?);
    let mut create = PreparedPublish::foreign(IdempotencyKey::new(b"setup".to_vec())?);
    for effect in setup.create(
        &actor,
        BTreeMap::from([(
            "inbox".to_owned(),
            Subscription {
                path: path("model/inbox")?,
                cursor: 0,
            },
        )]),
        None,
    )? {
        create.join(effect)?;
    }
    create.join(Effect {
        conditions: vec![CommitCondition::Absent {
            path: path("model/inbox")?,
        }],
        mutation: CommitMutation::Append {
            path: path("model/inbox")?,
            records: vec![
                Bytes::from_static(b"m0").into(),
                Bytes::from_static(b"m1").into(),
            ],
        },
    })?;
    PreparedPublish::settle(&provider.inner.commit(create.into_request()?).await?)?;
    setup
        .claim(
            &setup.load(&actor).await?,
            1,
            IdempotencyKey::new(b"claim".to_vec())?,
        )
        .await?;
    for _ in 0..32 {
        let snapshot = store.load(&actor).await?;
        let state = snapshot.state().ok_or("missing state")?;
        check(&provider.inner, state).await?;
        let Some((trigger, transition)) = script(state)? else {
            return Ok(());
        };
        let key = |suffix: &str| {
            IdempotencyKey::new(
                format!("{}/{}/{suffix}", snapshot.path, snapshot.tail).into_bytes(),
            )
        };
        // Any failure is a crash: re-read the cut and continue from it.
        match shape {
            Shape::Atomic => {
                let _crash = store
                    .publish(&snapshot, 1, &trigger, None, transition, key("publish")?)
                    .await;
            }
            Shape::Split => {
                let request = store
                    .prepare_publish(&snapshot, 1, &trigger, None, transition, key("state")?)?
                    .into_request()?;
                let own = |path: &StreamPath| path == &snapshot.path;
                let (state_conditions, effect_conditions): (Vec<_>, Vec<_>) = request
                    .conditions
                    .into_iter()
                    .partition(|condition| own(condition_path(condition)));
                let (state_mutations, effect_mutations): (Vec<_>, Vec<_>) =
                    request.mutations.into_iter().partition(|mutation| {
                        matches!(mutation, CommitMutation::Append { path, .. } if own(path))
                    });
                let first = provider
                    .commit(CommitRequest {
                        deadline_unix_millis: None,
                        conditions: state_conditions,
                        mutations: state_mutations,
                        idempotency_key: key("state")?,
                    })
                    .await;
                if matches!(first, Ok(CommitOutcome::Committed(_))) && !effect_mutations.is_empty()
                {
                    let _crash = provider
                        .commit(CommitRequest {
                            deadline_unix_millis: None,
                            conditions: effect_conditions,
                            mutations: effect_mutations,
                            idempotency_key: key("effects")?,
                        })
                        .await;
                }
            }
        }
    }
    Err("script did not finish".into())
}

/// Every plan of `length` commits with at most `faults` faults.
fn plans(length: usize, faults: usize) -> Vec<Vec<Fault>> {
    let mut plans = vec![Vec::new()];
    for _ in 0..length {
        plans = plans
            .into_iter()
            .flat_map(|plan| {
                [Fault::None, Fault::Drop, Fault::LoseAck]
                    .into_iter()
                    .map(move |fault| {
                        let mut next = plan.clone();
                        next.push(fault);
                        next
                    })
            })
            .filter(|plan| plan.iter().filter(|fault| **fault != Fault::None).count() <= faults)
            .collect();
    }
    plans
}

async fn explore(shape: Shape) -> Result<usize, String> {
    let schedules = plans(8, 2);
    let count = schedules.len();
    for plan in schedules {
        if let Err(error) = run(shape, plan.clone()).await {
            return Err(format!("{plan:?}: {error}"));
        }
    }
    Ok(count)
}

#[tokio::test]
async fn every_bounded_fault_schedule_keeps_the_tuple_atomic() -> Result<(), Failure> {
    let explored = explore(Shape::Atomic).await?;
    assert_eq!(explored, plans(8, 2).len());
    Ok(())
}

#[tokio::test]
async fn negative_control_split_publication_is_caught() {
    assert!(
        explore(Shape::Split).await.is_err(),
        "the model accepted a publication that commits state before its effects"
    );
}
