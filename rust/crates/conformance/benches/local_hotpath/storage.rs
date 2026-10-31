use super::{
    actor_delivery::{Entries, EntryError},
    calibrate::Floors,
    population::{Fixture, id, key, require_root},
    report::{Measurements, Observation, Quantiles, Result, Terms, emit, ns},
};
use acyclic_actors::{
    addressing::ActorReference,
    guest::{AlarmChange, Checkpoint, CodecIdentity, Effect, Subscription, Transition, Trigger},
    local::{PreparedPublish, Snapshot, Store},
};
use acyclic_fs::{CancellationToken, WorkBudget};
use acyclic_objects::{Blobs, ByteRange, Digest, Head, Kind};
use acyclic_stream::testing::RequestClass;
use acyclic_stream::{AppendOutcome, CommitCondition, CommitMutation, Stream, StreamPath};
use acyclic_test_support::scale::Operation;
use bytes::Bytes;
use futures::{TryStreamExt as _, task::AtomicWaker};
use std::{
    collections::BTreeMap,
    future::poll_fn,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
    time::Instant,
};

#[derive(Default)]
struct DeliveryWake {
    first: Mutex<Option<Instant>>,
    task: AtomicWaker,
}

impl Wake for DeliveryWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.first
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(Instant::now);
        self.task.wake();
    }
}

macro_rules! timed {
    ($rows:expr, $name:expr, $floor:expr, $identity:expr, $future:expr) => {{ timed!($rows, $name, $floor, $identity, None, $future) }};
    ($rows:expr, $name:expr, $floor:expr, $identity:expr, $requested_query:expr, $future:expr) => {{
        // Retain original request identity outside both measured intervals.
        let identity = $identity;
        // Allocate the sampler's future before the interval; construction of
        // the operation itself remains inside it.
        let future = Box::pin(async { $future.await });
        if $rows.includes($name) {
            let before = $rows.start($requested_query);
            let start = Instant::now();
            let result = future.await;
            let elapsed = start.elapsed();
            let observed = $rows.stop(before)?;
            $rows.record($name, $floor, elapsed, observed, identity)?;
            result
        } else {
            future.await
        }
    }};
}

fn read(bytes: u32) -> Terms {
    Terms {
        calls: 1,
        copy_bytes: bytes,
        ..Terms::default()
    }
}
fn commit(bytes: u32) -> Terms {
    // One encoding transfer plus one transfer into publication custody. This
    // deliberately excludes implementation-specific hashes and extra flushes.
    Terms {
        flushes: 1,
        encode_bytes: bytes,
        copy_bytes: bytes,
        ..read(0)
    }
}

pub(super) async fn run(
    fixture: &Fixture,
    agents: usize,
    samples: usize,
    floors: &Floors,
    recovery: &str,
    operations: &[Operation],
) -> Result<(bool, acyclic_test_support::scale::Point)> {
    let mut rows = Measurements::new(
        agents,
        fixture.requests.clone(),
        fixture.durability.clone(),
        recovery,
        operations,
    );
    let any = |family: &[Operation]| {
        operations
            .iter()
            .any(|operation| family.contains(operation))
    };
    if any(&[
        Operation::ObjectPut,
        Operation::ObjectGet,
        Operation::ObjectHead,
        Operation::ObjectList,
    ]) {
        objects(fixture, samples, &mut rows).await?;
    }
    if any(&[
        Operation::StreamAppend,
        Operation::StreamRead,
        Operation::StreamFollow,
    ]) {
        streams(fixture, samples, &mut rows).await?;
    }
    if any(&[
        Operation::ActorCreate,
        Operation::ActorDurableWrite,
        Operation::ActorWarmRead,
    ]) {
        actors(fixture, samples, &mut rows).await?;
    }
    if rows.includes(Operation::ActorWake) {
        actor_delivery(fixture, agents, samples, &mut rows).await?;
    }
    if any(&[
        Operation::PinnedRead,
        Operation::StagedWrite,
        Operation::Fsync,
    ]) {
        filesystem(fixture, samples, &mut rows).await?;
    }
    if rows.includes(Operation::SessionStart) {
        sessions(fixture, agents, samples, &mut rows).await?;
    }
    rows.finish(floors)
}

async fn objects(fixture: &Fixture, samples: usize, rows: &mut Measurements) -> Result {
    let mut first = fixture
        .objects
        .list(None, 1)
        .await?
        .digests
        .first()
        .copied();
    for index in 0..samples {
        let mut body = vec![0x5a; 1024];
        body.get_mut(..8)
            .ok_or("object fixture prefix missing")?
            .copy_from_slice(&u64::try_from(index)?.to_le_bytes());
        let expected = body.clone();
        let expected_digest = Digest::of(&expected);
        let put = timed!(
            rows,
            Operation::ObjectPut,
            Terms {
                hash_bytes: 1024,
                // Blobs::put stores bytes; only Blobs::flush promises crash
                // survival. Charge no durability barrier to PUT admission.
                ..read(1024)
            },
            serde_json::json!({"digest": expected_digest.to_string()}),
            fixture.objects.put(body.into(), Vec::new())
        )?;
        let digest = put.verified().ok_or("object publication unknown")?.digest;
        if digest != expected_digest {
            return Err("object PUT returned a different content identity".into());
        }
        first = Some(first.map_or(digest, |current| current.min(digest)));
        // Settle this sample before subsequent work. This explicit commit is
        // outside the PUT interval, whose public guarantee is only storage.
        fixture.objects.flush(Some(&[digest])).await?;
        let get = timed!(
            rows,
            Operation::ObjectGet,
            read(1024),
            serde_json::json!({"digest": digest.to_string(), "range": "full", "maximum_bytes": 1024}),
            fixture.objects.get(digest, ByteRange::FULL, 1024)
        )?;
        require_bytes(get.body().as_ref(), &expected)?;
        if index == 0 {
            let mut corrupted = expected.clone();
            *corrupted.first_mut().ok_or("empty object control")? ^= 1;
            if require_bytes(get.body().as_ref(), &corrupted).is_ok() {
                return Err("payload safety oracle accepted changed bytes".into());
            }
            emit(
                serde_json::json!({"schema": 1, "kind": "negative_control", "control": "changed real object payload", "result": "rejected"}),
            )?;
        }
        let mut head = timed!(
            rows,
            Operation::ObjectHead,
            read(0),
            serde_json::json!({"digest": digest.to_string()}),
            Some(RequestClass::Head),
            fixture.objects.head(digest)
        )?;
        require_head(&head, digest)?;
        if index == 0 {
            head.digest = Digest::of(b"changed real HEAD identity");
            if require_head(&head, digest).is_ok() {
                return Err("HEAD safety oracle accepted another digest".into());
            }
            emit(
                serde_json::json!({"schema": 1, "kind": "negative_control", "control": "changed real object HEAD identity", "result": "rejected"}),
            )?;
            let before = rows.start(Some(RequestClass::Head));
            fixture.objects.head(digest).await?;
            fixture.objects.head(digest).await?;
            Measurements::require_one_control(&rows.stop(before)?)?;
            emit(serde_json::json!({
                "schema": 1, "kind": "negative_control",
                "control": "redundant real HEAD after requested data query",
                "result": "one original data dispatch and one control probe",
            }))?;
        }
        let page = timed!(
            rows,
            Operation::ObjectList,
            read(32),
            serde_json::json!({"after": null, "limit": 1, "expected_first": first.map(|digest| digest.to_string())}),
            Some(RequestClass::List),
            fixture.objects.list(None, 1)
        )?;
        if page.digests.len() != 1 || page.digests.first().copied() != first {
            return Err("object LIST page identity mismatch".into());
        }
    }
    Ok(())
}

async fn streams(fixture: &Fixture, samples: usize, rows: &mut Measurements) -> Result {
    let stream = Stream::new(fixture.stream.clone(), "benchmark/stream")?;
    let payload = Bytes::from(vec![0x5a; 1024]);
    let seed = stream.append(payload.clone(), key("stream-seed")?).await?;
    let AppendOutcome::Committed(seed) = seed else {
        return Err("stream seed conflicted".into());
    };
    let mut next = seed.start.checked_add(1).ok_or("stream cursor overflow")?;
    for index in 0..samples {
        let append_key = key(format!("append/{index}"))?;
        let append_identity = hex::encode(append_key.as_bytes());
        let mut follow = stream.follow(next, Default::default()).await?;
        let probe = Arc::new(DeliveryWake::default());
        let wake = Waker::from(probe.clone());
        // Install the actual follower waiter before its producer is admitted.
        // This is one poll, not a readiness retry loop.
        poll_fn(|context| {
            probe.task.register(context.waker());
            let parked =
                futures::Stream::poll_next(Pin::new(&mut follow), &mut Context::from_waker(&wake));
            Poll::Ready(if parked.is_pending() {
                Ok(())
            } else {
                Err("tail follower did not park")
            })
        })
        .await?;
        let append = async {
            let start = Instant::now();
            let result = stream.append(payload.clone(), append_key).await?;
            let elapsed = start.elapsed();
            let AppendOutcome::Committed(receipt) = result else {
                return Err("stream append conflicted".into());
            };
            Result::Ok((receipt, elapsed))
        };
        let delivery = async {
            let delivered = poll_fn(|context| {
                probe.task.register(context.waker());
                futures::Stream::poll_next(Pin::new(&mut follow), &mut Context::from_waker(&wake))
            })
            .await
            .ok_or("follow terminated before delivery")??;
            let start = probe
                .first
                .lock()
                .map_err(|_| "follow wake timestamp poisoned")?
                .ok_or("follow delivered without its producer wake")?;
            Result::Ok((delivered, start.elapsed()))
        };
        // Producer failure cancels the consumer, so it cannot leave an orphan
        // wait for a record that will never be published.
        let ((receipt, appended), (delivered, notified)) = futures::try_join!(append, delivery)?;
        rows.record(
            Operation::StreamAppend,
            commit(1024),
            appended,
            Observation::default(),
            serde_json::json!({"path": stream.path().as_str(), "key_hex": append_identity, "start": receipt.start, "count": 1}),
        )?;
        rows.record(
            Operation::StreamFollow,
            Terms {
                wakes: 1,
                calls: 1,
                ..Terms::default()
            },
            notified,
            Observation::default(),
            serde_json::json!({"path": stream.path().as_str(), "sequence": delivered.sequence, "commit": format!("{:?}", delivered.commit_id)}),
        )?;
        next = receipt
            .start
            .checked_add(1)
            .ok_or("stream cursor overflow")?;
        require_bytes(delivered.value.as_ref(), &payload)?;
        if delivered.sequence != receipt.start {
            return Err("follow cursor mismatch".into());
        }
        let records = timed!(
            rows,
            Operation::StreamRead,
            // Resident records share their immutable payload owner. The
            // result oracle below does not justify a timed payload copy.
            read(0),
            serde_json::json!({"path": stream.path().as_str(), "start": receipt.start, "count": 1}),
            async {
                stream
                    .read(receipt.start, 1)
                    .await?
                    .try_collect::<Vec<_>>()
                    .await
            }
        )?;
        let record = records
            .first()
            .ok_or("finite read omitted committed record")?;
        if records.len() != 1 || record.sequence != receipt.start {
            return Err("finite read range mismatch".into());
        }
        require_bytes(record.value.as_ref(), &payload)?;
    }
    Ok(())
}

async fn actors(fixture: &Fixture, samples: usize, rows: &mut Measurements) -> Result {
    let store = Store::new(fixture.stream.clone(), StreamPath::new("benchmark/actors")?);
    let mut address = None;
    let creates = if rows.includes(Operation::ActorCreate) {
        samples
    } else {
        1
    };
    for index in 0..creates {
        let actor = ActorReference::new(
            b"benchmark",
            b"measure",
            format!("actor-{index}").as_bytes(),
        )?;
        let creation_key = key(format!("actor-create/{index}"))?;
        timed!(
            rows,
            Operation::ActorCreate,
            commit(0),
            serde_json::json!({"actor": format!("{actor:?}"), "key_hex": hex::encode(creation_key.as_bytes())}),
            async {
                let mut creation = PreparedPublish::foreign(creation_key);
                for effect in store.create(&actor, BTreeMap::new(), None)? {
                    creation.join(effect)?;
                }
                store.submit(creation).await
            }
        )?;
        require_created(&store.load(&actor).await?)?;
        address = Some(actor);
    }
    let address = address.ok_or("no actor samples")?;
    let before = store.load(&address).await?;
    let claimed = store.claim(&before, 1, key("actor-claim")?).await?;
    if require_created(&claimed).is_ok() {
        return Err("actor creation oracle accepted a claimed epoch".into());
    }
    emit(
        serde_json::json!({"schema": 1, "kind": "negative_control", "control": "real actor after placement claim", "result": "rejected"}),
    )?;

    if !rows.includes(Operation::ActorDurableWrite) && !rows.includes(Operation::ActorWarmRead) {
        return Ok(());
    }
    // Creation samples retain their empty-state oracle. This separate actor
    // declares its input once and retains each confirmed publication cut.
    let address = ActorReference::new(b"benchmark", b"measure", b"durable")?;
    let inbox = Stream::new(fixture.stream.clone(), "benchmark/actor-input")?;
    let mut creation = PreparedPublish::foreign(key("durable-create")?);
    for effect in store.create(
        &address,
        BTreeMap::from([(
            "inbox".into(),
            Subscription {
                path: inbox.path().clone(),
                cursor: 0,
            },
        )]),
        None,
    )? {
        creation.join(effect)?;
    }
    store.submit(creation).await?;
    let mut cut = store
        .claim(&store.load(&address).await?, 1, key("durable-claim")?)
        .await?;
    for index in 0..samples {
        let sequence = u64::try_from(index)?;
        let payload = Bytes::from(vec![u8::try_from(index % 256)?; 128]);
        // Fresh original input plus activation share one conditional commit.
        // Input publication and its finite read are outside the durable row.
        let mut input = PreparedPublish::foreign(key(format!("actor-input/{index}"))?);
        input.join(Effect {
            conditions: vec![if index == 0 {
                CommitCondition::Absent {
                    path: inbox.path().clone(),
                }
            } else {
                CommitCondition::Tail {
                    path: inbox.path().clone(),
                    expected: sequence,
                }
            }],
            mutation: CommitMutation::Append {
                path: inbox.path().clone(),
                records: vec![payload.clone().into()],
            },
        })?;
        input.join(store.activation(&address))?;
        store.submit(input).await?;
        let mut records = inbox.read(sequence, 1).await?;
        let original = records.try_next().await?.ok_or("actor input missing")?;
        if original.sequence != sequence || records.try_next().await?.is_some() {
            return Err("actor input finite range mismatch".into());
        }
        require_bytes(original.value.as_ref(), &payload)?;
        let trigger = Trigger::Record {
            subscription: "inbox".into(),
            sequence,
        };
        let transition = || Transition {
            response: None,
            checkpoint: Checkpoint {
                codec: CodecIdentity {
                    name: "benchmark-bytes".into(),
                    version: "1".into(),
                },
                bytes: Bytes::copy_from_slice(&payload),
            },
            alarm: AlarmChange::Keep,
            effects: Vec::new(),
            participant: None,
        };
        if index == 0 {
            if require_actor_write(&cut, sequence, &payload).is_ok() {
                return Err("combined publication oracle accepted the unconsumed cut".into());
            }
            let wrong = Trigger::Record {
                subscription: "inbox".into(),
                sequence: sequence + 1,
            };
            let binding = store.bind_state(store.load(&address).await?, 1)?;
            if !matches!(
                binding.prepare_publish(
                    &wrong,
                    Some(&original),
                    transition(),
                    key("actor-wrong-input")?
                ),
                Err(acyclic_actors::local::Error::Invalid(
                    "delivered record differs from its trigger"
                ))
            ) {
                return Err("combined publication accepted another original input".into());
            }
            emit(
                serde_json::json!({"schema": 1, "kind": "negative_control", "control": "combined Actor publication with unconsumed cut and mismatched real input", "result": "rejected"}),
            )?;
        }
        let operation = key(format!("actor-write/{index}"))?;
        let mut binding = store.bind_state(cut, 1)?;
        let requests = fixture.requests.requests();
        // Both 128-byte values are transferred into the one publication.
        // Mutation, checkpoint construction and encoding are all timed.
        cut = timed!(
            rows,
            Operation::ActorDurableWrite,
            commit(256),
            serde_json::json!({"actor": format!("{address:?}"), "key_hex": hex::encode(operation.as_bytes()), "subscription": "inbox", "sequence": sequence}),
            async {
                binding.transaction(|state| {
                    state.put("value".into(), Bytes::copy_from_slice(&payload));
                    Ok(())
                })?;
                let prepared =
                    binding.prepare_publish(&trigger, Some(&original), transition(), operation)?;
                store
                    .submit_retained(prepared)
                    .await?
                    .ok_or(acyclic_actors::local::Error::Invalid(
                        "combined publication omitted retained state",
                    ))
            }
        )?;
        let work = fixture.requests.requests().since(requests);
        if work.data() != 1 || work.control() != 0 {
            return Err(
                "combined Actor publication requires one commit and no control requests".into(),
            );
        }
        require_actor_write(&cut, sequence, &payload)?;
        if store.load(&address).await?.state() != cut.state() {
            return Err("confirmed Actor tuple differs from durable readback".into());
        }
        let binding = store.bind_state(cut, 1)?;
        // Borrow the admitted value without mutation bookkeeping or a handle clone.
        let value = timed!(
            rows,
            Operation::ActorWarmRead,
            read(0),
            serde_json::json!({"actor": format!("{address:?}"), "consumed_sequence": sequence, "key": "value"}),
            async { binding.get("value") }
        )?;
        require_bytes(value.ok_or("actor metadata missing")?.as_ref(), &payload)?;
        cut = binding.finish()?;
    }
    Ok(())
}

fn require_actor_write(snapshot: &Snapshot, sequence: u64, payload: &[u8]) -> Result {
    let state = snapshot.state().ok_or("combined Actor state missing")?;
    if state.cursor("inbox") != sequence.checked_add(1)
        || state.checkpoint().map(|value| value.bytes.as_ref()) != Some(payload)
        || state
            .metadata()
            .find(|(name, _)| *name == "value")
            .map(|(_, value)| value.as_ref())
            != Some(payload)
    {
        return Err("combined Actor metadata/checkpoint/cursor tuple mismatch".into());
    }
    Ok(())
}

async fn actor_delivery(
    fixture: &Fixture,
    agents: usize,
    samples: usize,
    rows: &mut Measurements,
) -> Result {
    use acyclic_harness_core::agent::{
        AgentPath, Control, Delivered, INBOX, InboxRecord, IntentId, Phase, Sender,
    };
    use std::sync::atomic::Ordering;

    let path = AgentPath::root(&id(0)?);
    let host = fixture.bound.agents();
    let actor = host.actor(&path)?;
    let runtime = host.runtime();
    let admitted = runtime.store().load(&actor).await?;
    let epoch = admitted
        .state()
        .ok_or("resident Actor state absent")?
        .epoch();
    fixture
        .entries
        .check_dropped_producer(
            actor.clone(),
            epoch,
            Trigger::Record {
                subscription: INBOX.into(),
                sequence: 0,
            },
        )
        .await?;
    if runtime.counts() != (agents, 0) {
        return Err("handler-entry population is not resident and idle".into());
    }
    let mut completed = Vec::with_capacity(samples);
    for index in 0..samples
        .checked_add(4)
        .ok_or("delivery sample count overflow")?
    {
        let by = Sender::Host {
            host: "benchmark".into(),
        };
        let record = InboxRecord::Cancel(Control {
            by: by.clone(),
            id: IntentId::try_from(format!("delivery-{index}").as_str())?,
        });
        // AgentHost publishes the fresh input and Store::activation together
        // in one original conditional commit. Publication is outside delivery.
        let Delivered::Committed { inbox } = host.send(&path, &by, &record).await? else {
            return Err("delivery input was not committed".into());
        };
        let expected = Trigger::Record {
            subscription: if index == 1 {
                "another-inbox".into()
            } else {
                INBOX.into()
            },
            sequence: if index == 0 {
                inbox.checked_add(1).ok_or("delivery cursor overflow")?
            } else {
                inbox
            },
        };
        let expected_epoch = if index == 2 {
            epoch.checked_add(1).ok_or("delivery epoch overflow")?
        } else {
            epoch
        };
        let expected_actor = if index == 3 {
            host.actor(&AgentPath::root(&id(agents)?))?
        } else {
            actor.clone()
        };
        let (scope, receiver) =
            fixture
                .entries
                .arm(expected_actor, expected_epoch, expected.clone())?;
        let entries_before = fixture.entries.calls();
        let before = rows.start(None);
        let started = Instant::now();
        let drained = {
            // The scope closes its producer on success, error or cancellation.
            let _scope = scope;
            let result = Box::pin(runtime.drain_actor(&actor)).await;
            if index == 0 {
                fixture
                    .entries
                    .check_owned_scope(actor.clone(), expected_epoch, expected)?;
            }
            result
        };
        let elapsed = started.elapsed();
        drained?;
        if fixture.entries.calls().checked_sub(entries_before) != Some(1) {
            return Err("fresh input did not enter the handler exactly once".into());
        }
        // The only producer was on the completed drain path. This receive
        // must now contain its entry or return typed producer loss.
        let entered = Entries::receive(receiver).await;
        if index < 4 {
            if entered != Err(EntryError::UnexpectedDelivery) {
                return Err("handler-entry oracle accepted another input identity".into());
            }
            emit(serde_json::json!({
                "schema": 1, "kind": "negative_control",
                "control": match index {
                    0 => "real handler entry against another input cursor",
                    1 => "real handler entry against another subscription",
                    2 => "real handler entry against another admitted epoch",
                    _ => "real handler entry against another actor",
                },
                "result": "rejected",
            }))?;
        } else {
            let entered = entered?;
            let observed = Measurements::stop_at(before, entered.counters)?;
            if index == 4 {
                let completion = rows.stop(before)?;
                Measurements::require_entry_prefix(&observed, &completion)?;
                if Measurements::require_entry_prefix(&completion, &completion).is_ok() {
                    return Err("whole-drain accounting was accepted as handler entry".into());
                }
                emit(serde_json::json!({
                    "schema": 1, "kind": "negative_control",
                    "control": "substituted whole-drain request ledger for handler entry",
                    "result": "rejected; later original data publication excluded",
                }))?;
            }
            rows.record(
                Operation::ActorWake,
                Terms {
                    wakes: 1,
                    ..Terms::default()
                },
                entered
                    .at
                    .checked_duration_since(started)
                    .ok_or("handler entry preceded delivery")?,
                observed,
                serde_json::json!({"actor": format!("{actor:?}"), "epoch": epoch, "subscription": INBOX, "sequence": inbox}),
            )?;
            completed.push(ns(elapsed));
        }
        let cut = runtime.store().load(&actor).await?;
        let state = cut.state().ok_or("delivered actor state absent")?;
        if state.subscriptions().get(INBOX).map(|value| value.cursor) != inbox.checked_add(1)
            || state.checkpoint().is_none()
            || !matches!(host.state(&path).await?.phase, Phase::Idle)
            || fixture.model_calls.load(Ordering::Relaxed) != 0
        {
            return Err("delivery did not durably consume its original idle input".into());
        }
        // Clear the confirmed input's ready event through the same runtime.
        // This is outside timing and must not enter the handler a second time.
        if let Some((actor, error)) = Box::pin(runtime.drain()).await.into_iter().next() {
            return Err(format!("settle delivery {actor:?}: {error}").into());
        }
        if runtime.counts() != (agents, 0) {
            return Err("delivery changed the resident population or left ready work".into());
        }
        if fixture.entries.calls().checked_sub(entries_before) != Some(1) {
            return Err("settled input entered the handler again".into());
        }
    }
    emit(serde_json::json!({
        "schema": 1, "kind": "delivery_completion", "operation": Operation::ActorWake.name(),
        "agents": agents, "entry_label": "event-drain-to-handler-entry",
        "completion_label": "event-drain-completion", "input_publication_timed": false,
        "quantiles": Quantiles::new(&mut completed.clone())?, "raw_ns": completed,
    }))
}

async fn filesystem(fixture: &Fixture, samples: usize, rows: &mut Measurements) -> Result {
    let cancellation = CancellationToken::new();
    let volume = fixture.volume(&id(0)?)?;
    let name = volume.volume.storage_name(&mut None)?;
    let workspace = Box::pin(fixture.fs.open_workspace(
        &name,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .map_err(|failure| failure.error)?
    .value;
    let payload = Bytes::from(vec![0x5a; 1024]);
    Box::pin(workspace.write("/value", payload.clone())).await?;
    let generation = Box::pin(workspace.head(WorkBudget::UNBOUNDED, &cancellation))
        .await
        .map_err(|failure| failure.error)?
        .value;
    // Warm the exact immutable generation before sampling it.
    Box::pin(generation.read("/value", 1024, WorkBudget::UNBOUNDED, &cancellation))
        .await
        .map_err(|failure| failure.error)?;
    let mut visible = payload.to_vec();
    for index in 0..samples {
        let observed = timed!(
            rows,
            Operation::PinnedRead,
            read(1024),
            serde_json::json!({"workspace": format!("{:?}", generation.workspace_id()), "generation": format!("{:?}", generation.id()), "path": "/value"}),
            generation.read("/value", 1024, WorkBudget::UNBOUNDED, &cancellation)
        )
        .map_err(|failure| failure.error)?
        .value;
        require_bytes(observed.as_ref(), &payload)?;
        let operation = acyclic_fs::IdempotencyKey::from_bytes(
            (u128::MAX - u128::try_from(index)?).to_le_bytes(),
        );
        let mut transaction =
            Box::pin(workspace.begin_transaction(operation, WorkBudget::UNBOUNDED, &cancellation))
                .await
                .map_err(|failure| failure.error)?
                .value;
        let mut body = vec![0x5a; 1024];
        body.get_mut(..8)
            .ok_or("filesystem fixture prefix missing")?
            .copy_from_slice(&u64::try_from(index)?.to_le_bytes());
        let staged = Bytes::from(body.clone());
        timed!(
            rows,
            Operation::StagedWrite,
            read(1024),
            serde_json::json!({"volume": name, "transaction": format!("{operation:?}"), "path": "/value"}),
            transaction.write("/value", staged, WorkBudget::UNBOUNDED, &cancellation)
        )
        .map_err(|failure| failure.error)?;
        // Staging promises no publication. Check the live workspace outside
        // the interval before allowing the commit sample to begin.
        let unpublished =
            Box::pin(workspace.read("/value", 1024, WorkBudget::UNBOUNDED, &cancellation))
                .await
                .map_err(|failure| failure.error)?
                .value;
        require_bytes(unpublished.as_ref(), &visible)?;
        let outcome = timed!(
            rows,
            Operation::Fsync,
            commit(1024),
            serde_json::json!({"volume": name, "transaction": format!("{operation:?}")}),
            transaction.commit(
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &cancellation,
                |_| async {
                    Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                        acyclic_fs::OperationReceipt {
                            value: (),
                            work: Default::default(),
                        },
                    )
                },
            )
        )
        .map_err(|failure| failure.error)?
        .value;
        if !matches!(
            outcome,
            acyclic_fs::TransactionCommit::Committed(_)
                | acyclic_fs::TransactionCommit::AlreadyCommitted(_)
        ) {
            return Err("filesystem publication did not commit".into());
        }
        let confirmed =
            Box::pin(workspace.read("/value", 1024, WorkBudget::UNBOUNDED, &cancellation))
                .await
                .map_err(|failure| failure.error)?
                .value;
        require_bytes(confirmed.as_ref(), &body)?;
        visible = body;
    }
    Ok(())
}

async fn sessions(
    fixture: &Fixture,
    agents: usize,
    samples: usize,
    rows: &mut Measurements,
) -> Result {
    use acyclic_harness_core::agent::{AgentPath, IntentId};
    for index in 0..samples {
        let agent = id(agents + index)?;
        let expected = AgentPath::root(&agent);
        let intent = IntentId::try_from(format!("session-{index}").as_str())?;
        let request = fixture.root_request(agent)?;
        let created = timed!(
            rows,
            Operation::SessionStart,
            Terms {
                wakes: 1,
                ..commit(0)
            },
            serde_json::json!({"agent": request.agent.to_string(), "intent": format!("{intent:?}")}),
            fixture.bound.create_root(&intent, request)
        )?;
        require_root(&created, &expected)?;
        if index == 0 {
            let other = AgentPath::root(&id(agents + samples)?);
            if require_root(&created, &other).is_ok() {
                return Err("root receipt oracle accepted another requested agent".into());
            }
            emit(serde_json::json!({
                "schema": 1, "kind": "negative_control",
                "control": "real root receipt against another requested agent",
                "result": "rejected",
            }))?;
        }
    }
    Ok(())
}

fn require_bytes(observed: &[u8], expected: &[u8]) -> Result {
    if observed != expected {
        return Err("provider returned different payload bytes".into());
    }
    Ok(())
}

fn require_head(head: &Head, digest: Digest) -> Result {
    if head.digest != digest
        || head.length != 1024
        || head.kind != Kind::Blob
        || !head.references.is_empty()
    {
        return Err("object HEAD identity, kind, length or references mismatch".into());
    }
    Ok(())
}

fn require_created(snapshot: &Snapshot) -> Result {
    let state = snapshot.state().ok_or("created actor state missing")?;
    if state.epoch() != 0
        || state.checkpoint().is_some()
        || state.metadata().len() != 0
        || !state.subscriptions().is_empty()
        || state.alarm().pending().is_some()
        || state.paused()
    {
        return Err("created actor differs from its requested initial state".into());
    }
    Ok(())
}
