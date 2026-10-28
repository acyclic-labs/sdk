use super::*;
use crate::guest::{CodecIdentity, Effect};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{LocalStream, MemoryStream, ReadRequest};
use bytes::Bytes;
use futures::TryStreamExt;
use proptest::prelude::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The original publication owns response, cursor, checkpoint and retry. A
/// missing/foreign response cannot consume input; an empty response is real.
#[tokio::test]
async fn invocation_response_is_one_fenced_publication_and_reconciles_after_reopen() -> TestResult {
    use crate::invocation::Invocation;
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), state_root()?);
    let address = actor("invoked")?;
    place(&store, &address).await?;
    let invocation = Invocation::new(
        address.clone(),
        "inbox".into(),
        Bytes::from_static(b"op"),
        Bytes::from_static(b"request"),
    )?;
    let ticket = store.enqueue_invoke(&invocation).await?;
    assert_eq!(store.enqueue_invoke(&invocation).await?, ticket);
    let before = store.load(&address).await?;
    assert!(matches!(
        store.prepare_publish(
            &before,
            1,
            &record(0),
            Some(ticket.record()),
            transition(7, AlarmChange::Keep),
            key("invoked/missing")?
        ),
        Err(Error::Invalid("invocation has no response"))
    ));
    let mut result = transition(7, AlarmChange::Keep);
    result.response = Some(Bytes::new());
    assert!(matches!(
        store.prepare_publish(
            &before,
            1,
            &record(0),
            None,
            result.clone(),
            key("invoked/no-input")?
        ),
        Err(Error::Invalid("response without an invocation"))
    ));
    let foreign = Invocation::new(
        actor("different")?,
        "inbox".into(),
        Bytes::from_static(b"op"),
        Bytes::from_static(b"request"),
    )?
    .record(before.path())?;
    let mut different = ticket.record().clone();
    different.value = foreign.value;
    different.headers = foreign.headers;
    assert!(matches!(
        store.prepare_publish(
            &before,
            1,
            &record(0),
            Some(&different),
            result.clone(),
            key("invoked/foreign")?
        ),
        Err(Error::Invalid(
            "invocation input targets another actor or subscription"
        ))
    ));
    let other_root = Store::new(Arc::clone(&provider), StreamPath::new("foreign/actors")?);
    let foreign = invocation.record(&other_root.path(&address)?)?;
    different.value = foreign.value;
    different.headers = foreign.headers;
    assert!(matches!(
        store.prepare_publish(
            &before,
            1,
            &record(0),
            Some(&different),
            result.clone(),
            key("invoked/foreign-root")?
        ),
        Err(Error::Invalid(
            "invocation input targets another actor or subscription"
        ))
    ));
    assert_eq!(store.load(&address).await?.tail, before.tail);
    let mut forked = result.clone();
    let reply_parent = StreamPath::new(format!("{}/replies", before.path()))?;
    let reply_fork = Effect {
        conditions: Vec::new(),
        mutation: CommitMutation::Fork {
            exclude: Vec::new(),
            source: before.path().clone(),
            destination: reply_parent,
            at_tail: 0,
            records: Vec::new(),
        },
    };
    forked.effects.push(reply_fork.clone());
    assert!(matches!(
        store.prepare_publish(
            &before,
            1,
            &record(0),
            Some(ticket.record()),
            forked,
            key("invoked/fork-parent")?
        ),
        Err(Error::Invalid(
            "guest effect targets the host's invocation reply"
        ))
    ));
    let mut publication = store.prepare_publish(
        &before,
        1,
        &record(0),
        Some(ticket.record()),
        result.clone(),
        key("invoked/publish")?,
    )?;
    let original_request = publication.request().clone();
    assert!(matches!(
        publication.join(reply_fork),
        Err(Error::Invalid("effect changes the host's invocation reply"))
    ));
    assert_eq!(publication.request(), &original_request);
    store.submit(publication).await?;
    // Lost reply and acknowledgement: replay the same original publication,
    // then reopen the Store and retry the public operation identity.
    store
        .publish(
            &before,
            1,
            &record(0),
            Some(ticket.record()),
            result,
            key("invoked/publish")?,
        )
        .await?;
    let committed = store.wait_invocation(ticket.clone()).await?;
    assert!(committed.response().is_empty());
    assert_eq!(committed.receipt().revision(), before.tail + 1);
    assert_eq!(
        committed.receipt().state().commit_id,
        committed.receipt().reply().commit_id
    );
    let reopened = Store::new(provider, state_root()?);
    assert_eq!(reopened.invoke(&invocation).await?, committed);
    let changed = Invocation::new(
        address.clone(),
        "inbox".into(),
        invocation.operation().clone(),
        Bytes::from_static(b"changed"),
    )?;
    assert!(matches!(
        reopened.enqueue_invoke(&changed).await,
        Err(Error::Stream(StreamError::IdempotencyMismatch))
    ));
    assert_eq!(
        reopened
            .load(&address)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(1)
    );
    Ok(())
}

/// Reopening the actual durable provider must recover the same publication,
/// while issuing its read floor from the provider's current incarnation.
#[tokio::test]
async fn invocation_reconciles_the_original_records_after_durable_provider_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let address = actor("durable-invoked")?;
    let invocation = crate::invocation::Invocation::new(
        address.clone(),
        "inbox".into(),
        Bytes::from_static(b"durable-op"),
        Bytes::from_static(b"durable-request"),
    )?;
    let committed = {
        let store = Store::new(
            Arc::new(LocalStream::open(directory.path(), Default::default()).await?),
            state_root()?,
        );
        place(&store, &address).await?;
        let ticket = store.enqueue_invoke(&invocation).await?;
        let snapshot = store.load(&address).await?;
        let mut result = transition(9, AlarmChange::Keep);
        result.response = Some(Bytes::from_static(b"durable-response"));
        store
            .publish(
                &snapshot,
                1,
                &record(0),
                Some(ticket.record()),
                result,
                key("durable-invoke/publish")?,
            )
            .await?;
        store.wait_invocation(ticket).await?
    };
    let store = Store::new(
        Arc::new(LocalStream::open(directory.path(), Default::default()).await?),
        state_root()?,
    );
    let reopened = store.invoke(&invocation).await?;
    assert_eq!(reopened, committed);
    let receipt = reopened.receipt();
    let floor = acyclic_stream::RevisionCut::of(receipt.ticket().storage_revision().clone());
    let applied = store.provider().wait_applied(&floor).await?;
    assert!(floor.proven_by(Some(&applied)));
    assert_eq!(
        store
            .provider()
            .tail(
                inbox(&address)?
                    .remove("inbox")
                    .ok_or("missing input")?
                    .path
            )
            .await?,
        1
    );
    Ok(())
}

/// Observation follows only committed actor state, preserving the original
/// input anchor and refusing a different subscription/path at the same cursor.
#[tokio::test]
async fn wait_applied_retains_its_cut_across_pause_and_never_consumes_input() -> TestResult {
    use acyclic_stream::AppendRequest;
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), state_root()?);
    let address = actor("applied")?;
    place(&store, &address).await?;
    let path = inbox(&address)?.remove("inbox").ok_or("inbox")?.path;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"input").into()],
            if_tail: Some(0),
            idempotency_key: key("applied/input")?,
        })
        .await?;
    let original = provider
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
        .await?
        .try_next()
        .await?
        .ok_or("original input")?;
    let zero = store
        .wait_applied(&address, AppliedCut::empty("inbox".into(), path.clone())?)
        .await?;
    assert_eq!(
        zero.snapshot()
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(0)
    );
    assert!(matches!(
        store
            .wait_applied(&address, AppliedCut::empty("missing".into(), path.clone())?)
            .await,
        Err(Error::Conflict)
    ));
    assert!(matches!(
        store
            .wait_applied(
                &address,
                AppliedCut::empty("inbox".into(), StreamPath::new("wrong/input")?)?
            )
            .await,
        Err(Error::Conflict)
    ));

    let cut = AppliedCut::through("inbox".into(), path.clone(), &original)?;
    let mut waiter = Box::pin(store.wait_applied(&address, cut.clone()));
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    let before = store.load(&address).await?;
    assert_eq!(before.tail, zero.revision(), "waiting published nothing");
    store
        .set_paused(&before, 1, true, key("applied/pause")?)
        .await?;
    assert!(
        futures::poll!(waiter.as_mut()).is_pending(),
        "pause is not input consumption"
    );
    let paused = store.load(&address).await?;
    store
        .set_paused(&paused, 1, false, key("applied/resume")?)
        .await?;
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    let resumed = store.load(&address).await?;
    store
        .publish(
            &resumed,
            1,
            &record(0),
            Some(&original),
            transition(7, AlarmChange::Keep),
            key("applied/publish")?,
        )
        .await?;
    let applied = tokio::time::timeout(std::time::Duration::from_secs(1), waiter).await??;
    assert_eq!(applied.cut(), &cut);
    assert_eq!(applied.revision(), resumed.tail + 1);
    assert_eq!(
        Some(applied.commit_id()),
        store.load(&address).await?.anchor
    );
    assert_eq!(
        applied
            .snapshot()
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(1)
    );

    // An already-consumed numerical cursor cannot authenticate a different
    // source history. Construct only the negative control from the real record.
    let mut different = original.clone();
    let mut anchor = *different.commit_id.as_bytes();
    anchor[0] ^= 1;
    different.commit_id = CommitId::from_bytes(anchor);
    assert!(matches!(
        store
            .wait_applied(
                &address,
                AppliedCut::through("inbox".into(), path, &different)?
            )
            .await,
        Err(Error::Stream(StreamError::PrefixNotRetained))
    ));
    assert_eq!(
        provider.tail(store.path(&address)?).await?,
        applied.revision()
    );
    Ok(())
}

fn actor(id: &str) -> Result<ActorReference, Box<dyn std::error::Error>> {
    Ok(ActorReference::new(b"account", b"agents", id.as_bytes())?)
}

fn key(value: &str) -> Result<IdempotencyKey, StreamError> {
    IdempotencyKey::new(value.as_bytes().to_vec())
}

fn transition(value: u8, alarm: AlarmChange) -> Transition {
    Transition {
        response: None,
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
            key: None,
            path: output,
            from: 0,
            limit: 2,
            at_least: Default::default(),
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
