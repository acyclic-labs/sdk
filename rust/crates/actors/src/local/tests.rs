use super::*;
use crate::guest::{CodecIdentity, Effect};
use acyclic_stream::{LocalStream, MemoryStream, ReadRequest};
use futures::TryStreamExt;
use bytes::Bytes;
use proptest::prelude::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn actor(id: &str) -> Result<ActorReference, Box<dyn std::error::Error>> {
    Ok(ActorReference::new(b"account", b"agents", id.as_bytes())?)
}

fn key(value: &str) -> Result<IdempotencyKey, StreamError> {
    IdempotencyKey::new(value.as_bytes().to_vec())
}

fn transition(value: u8, alarm: AlarmChange) -> Transition {
    Transition {
        participant: None,
        checkpoint: Checkpoint {
            codec: CodecIdentity {
                name: "counter".into(),
                version: "1".into(),
            },
            bytes: Bytes::copy_from_slice(&[value]),
        },
        alarm,
        effects: Vec::new(),
    }
}

fn record(sequence: u64) -> Trigger {
    Trigger::Record {
        subscription: "inbox".into(),
        sequence,
    }
}

fn state_root() -> Result<StreamPath, StreamError> {
    StreamPath::new("harness/actors")
}

fn inbox(address: &ActorReference) -> Result<BTreeMap<String, Subscription>, StreamError> {
    Ok(BTreeMap::from([(
        "inbox".into(),
        Subscription {
            path: StreamPath::new(format!("inboxes/{}", address.id()))?,
            cursor: 0,
        },
    )]))
}

/// Create through the companion mutation in a standalone commit, then place.
async fn place<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    address: &ActorReference,
) -> Result<(), Error> {
    let mut prepared = PreparedPublish::foreign(key("create")?);
    for effect in store.create(address, inbox(address)?, None)? {
        prepared.join(effect)?;
    }
    let created = store.provider().commit(prepared.into_request()?).await?;
    PreparedPublish::settle(&created)?;
    store
        .claim(&store.load(address).await?, 1, key("place")?)
        .await
}

async fn atomic_history<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    root: StreamPath,
) -> TestResult {
    let store = Store::new(Arc::clone(&provider), root);
    let address = actor("atomic")?;
    place(&store, &address).await?;
    let before = store.load(&address).await?;
    let output = StreamPath::new("outputs/atomic")?;
    let make_transition = || {
        let mut value = transition(
            7,
            AlarmChange::Set {
                deadline_unix_ms: 42,
            },
        );
        value.effects = vec![
            Effect {
                conditions: vec![CommitCondition::Absent {
                    path: output.clone(),
                }],
                mutation: CommitMutation::Append {
                    path: output.clone(),
                    records: vec![Bytes::from_static(b"a").into()],
                },
            },
            Effect {
                conditions: vec![CommitCondition::Absent {
                    path: output.clone(),
                }],
                mutation: CommitMutation::Append {
                    path: output.clone(),
                    records: vec![Bytes::from_static(b"b").into()],
                },
            },
        ];
        value
    };
    store
        .publish(
            &before,
            1,
            &record(0),
            None,
            make_transition(),
            key("commit")?,
        )
        .await?;
    // Lose the acknowledgement, then replay the exact request. No duplicate effects.
    store
        .publish(
            &before,
            1,
            &record(0),
            None,
            make_transition(),
            key("commit")?,
        )
        .await?;
    let after = store.load(&address).await?;
    let state = after.state().ok_or("missing state")?;
    assert_eq!(state.cursor("inbox"), Some(1), "atomic consumed cursor");
    assert_eq!(
        state.checkpoint().map(|v| v.bytes.as_ref()),
        Some([7].as_slice()),
        "atomic checkpoint"
    );
    assert_eq!(state.alarm().deadline_unix_ms(), Some(42), "atomic alarm");
    assert_eq!(state.alarm().generation(), 1);
    assert_eq!(
        provider.tail(output.clone()).await?,
        2,
        "exactly once committed effects"
    );
    let mut records = provider
        .read(ReadRequest {
            path: output,
            from: 0,
            limit: 2,
        })
        .await?;
    while let Some(effect) = records.try_next().await? {
        assert_eq!(
            Some(effect.commit_id),
            after.anchor,
            "one checkpoint/effect transaction"
        );
    }
    Ok(())
}

async fn fencing_history<P: StreamProvider + ?Sized>(provider: Arc<P>) -> TestResult {
    let store = Store::new(provider, state_root()?);
    let address = actor("fences")?;
    place(&store, &address).await?;
    let first = store.load(&address).await?;
    store
        .publish(
            &first,
            1,
            &record(0),
            None,
            transition(
                1,
                AlarmChange::Set {
                    deadline_unix_ms: 10,
                },
            ),
            key("set")?,
        )
        .await?;
    let old = store.load(&address).await?;
    store.claim(&old, 2, key("takeover")?).await?;
    let current = store.load(&address).await?;
    // A freshly loaded token still cannot be used by an old placement.
    assert!(
        matches!(
            store
                .publish(
                    &current,
                    1,
                    &record(1),
                    None,
                    transition(2, AlarmChange::Clear),
                    key("old-owner")?
                )
                .await,
            Err(Error::Conflict)
        ),
        "epoch fencing"
    );
    assert!(
        matches!(
            store
                .publish(
                    &old,
                    1,
                    &record(1),
                    None,
                    transition(2, AlarmChange::Clear),
                    key("old-cut")?
                )
                .await,
            Err(Error::Conflict)
        ),
        "exact cut fencing"
    );
    store
        .publish(
            &current,
            2,
            &record(1),
            None,
            transition(
                2,
                AlarmChange::Set {
                    deadline_unix_ms: 20,
                },
            ),
            key("replace")?,
        )
        .await?;
    let replaced = store.load(&address).await?;
    assert!(
        matches!(
            store
                .publish(
                    &replaced,
                    2,
                    &Trigger::Alarm {
                        generation: 1,
                        now_unix_ms: 30
                    },
                    None,
                    transition(3, AlarmChange::Keep),
                    key("old-alarm")?
                )
                .await,
            Err(Error::Conflict)
        ),
        "alarm generation fencing"
    );
    pause_and_consumed_alarm_history(&store, &address, &replaced).await
}

async fn pause_and_consumed_alarm_history<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    address: &ActorReference,
    replaced: &Snapshot,
) -> TestResult {
    store.set_paused(replaced, 2, true, key("pause")?).await?;
    let paused = store.load(address).await?;
    assert!(
        matches!(
            store
                .publish(
                    &paused,
                    2,
                    &record(2),
                    None,
                    transition(3, AlarmChange::Clear),
                    key("paused")?
                )
                .await,
            Err(Error::Paused)
        ),
        "paused work remains unconsumed"
    );
    assert!(
        matches!(
            store
                .publish(
                    replaced,
                    2,
                    &record(2),
                    None,
                    transition(3, AlarmChange::Clear),
                    key("inflight")?
                )
                .await,
            Err(Error::Conflict)
        ),
        "pause fences in-flight work"
    );
    store.set_paused(&paused, 2, false, key("resume")?).await?;
    let resumed = store.load(address).await?;
    store
        .publish(
            &resumed,
            2,
            &Trigger::Alarm {
                generation: 2,
                now_unix_ms: 20,
            },
            None,
            transition(3, AlarmChange::Keep),
            key("fire")?,
        )
        .await?;
    let fired = store.load(address).await?;
    let state = fired.state().ok_or("missing state")?;
    assert_eq!(state.cursor("inbox"), Some(2));
    assert_eq!(state.alarm().generation(), 2, "Keep preserves generation");
    assert_eq!(
        state.alarm().deadline_unix_ms(),
        Some(20),
        "Keep preserves deadline"
    );
    assert!(
        matches!(
            store
                .publish(
                    &fired,
                    2,
                    &Trigger::Alarm {
                        generation: 2,
                        now_unix_ms: 30
                    },
                    None,
                    transition(4, AlarmChange::Keep),
                    key("cleared")?
                )
                .await,
            Err(Error::Conflict)
        ),
        "a consumed alarm cannot publish twice even when Keep preserves its deadline"
    );
    Ok(())
}

#[tokio::test]
async fn memory_atomic_history() -> TestResult {
    atomic_history(Arc::new(MemoryStream::default()), state_root()?).await?;
    atomic_history(
        Arc::new(MemoryStream::default()),
        StreamPath::new("actors")?,
    )
    .await
}

#[tokio::test]
async fn memory_epoch_alarm_pause_fences() -> TestResult {
    fencing_history(Arc::new(MemoryStream::default())).await
}

#[tokio::test]
async fn local_atomic_history() -> TestResult {
    let root = tempfile::tempdir()?;
    atomic_history(
        Arc::new(LocalStream::open(root.path(), Default::default()).await?),
        state_root()?,
    )
    .await
}

#[tokio::test]
async fn local_epoch_alarm_pause_fences() -> TestResult {
    let root = tempfile::tempdir()?;
    fencing_history(Arc::new(
        LocalStream::open(root.path(), Default::default()).await?,
    ))
    .await
}

#[tokio::test]
async fn reopen_preserves_the_complete_committed_tuple() -> TestResult {
    let root = tempfile::tempdir()?;
    {
        let provider = Arc::new(LocalStream::open(root.path(), Default::default()).await?);
        atomic_history(provider, state_root()?).await?;
    }
    let store = Store::new(
        Arc::new(LocalStream::open(root.path(), Default::default()).await?),
        state_root()?,
    );
    let snapshot = store.load(&actor("atomic")?).await?;
    let state = snapshot.state().ok_or("missing recovered state")?;
    assert_eq!(state.epoch(), 1);
    assert_eq!(state.cursor("inbox"), Some(1));
    assert_eq!(state.alarm().generation(), 1);
    assert_eq!(state.alarm().deadline_unix_ms(), Some(42));
    assert_eq!(
        state.checkpoint().map(|value| value.bytes.as_ref()),
        Some([7].as_slice())
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn bounded_alarm_histories(actions in prop::collection::vec(any::<u8>(), 1..24)) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(async {
            let store = Store::new(Arc::new(MemoryStream::default()), state_root()?);
            let address = actor("history")?;
            place(&store, &address).await?;
            let mut generation = 0;
            let mut deadline = None;
            for (index, action) in actions.into_iter().enumerate() {
                let alarm = match action % 3 { 0 => AlarmChange::Keep, 1 => AlarmChange::Clear, _ => AlarmChange::Set { deadline_unix_ms: u64::from(action) + 1 } };
                match alarm { AlarmChange::Keep => {}, AlarmChange::Clear => { generation += 1; deadline = None; }, AlarmChange::Set { deadline_unix_ms } => { generation += 1; deadline = Some(deadline_unix_ms); } }
                let snapshot = store.load(&address).await?;
                store.publish(&snapshot, 1, &record(u64::try_from(index)?), None, transition(action, alarm), key(&format!("history-{index}"))?).await?;
                let after = store.load(&address).await?;
                let state = after.state().ok_or("missing state")?;
                assert_eq!(state.alarm().generation(), generation);
                assert_eq!(state.alarm().deadline_unix_ms(), deadline);
                assert_eq!(state.cursor("inbox"), Some(u64::try_from(index)? + 1));
            }
            TestResult::Ok(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
