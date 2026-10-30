use super::{
    calibrate::Floors,
    population::{Fixture, id, key},
    report::{Measurements, Result, Terms},
};
use acyclic_actors::{
    addressing::ActorReference,
    local::{PreparedPublish, Store},
};
use acyclic_fs::{CancellationToken, WorkBudget};
use acyclic_objects::{Blobs, ByteRange, Digest};
use acyclic_stream::{AppendOutcome, Stream, StreamPath};
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
    ($rows:expr, $name:expr, $floor:expr, $future:expr) => {{
        // Allocate the sampler's future before the interval; construction of
        // the operation itself remains inside it.
        let future = Box::pin(async { $future.await });
        let start = Instant::now();
        let result = future.await;
        $rows.record($name, $floor, start.elapsed());
        result
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
) -> Result<bool> {
    let mut rows = Measurements::new(agents);
    objects(fixture, samples, &mut rows).await?;
    streams(fixture, samples, &mut rows).await?;
    actors(fixture, samples, &mut rows).await?;
    filesystem(fixture, samples, &mut rows).await?;
    sessions(fixture, agents, samples, &mut rows).await?;
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
        let put = timed!(
            rows,
            "objects.put",
            Terms {
                hash_bytes: 1024,
                // Blobs::put stores bytes; only Blobs::flush promises crash
                // survival. Charge no durability barrier to PUT admission.
                ..read(1024)
            },
            fixture.objects.put(body.into(), Vec::new())
        )?;
        let digest = put.verified().ok_or("object publication unknown")?.digest;
        if digest != Digest::of(&expected) {
            return Err("object PUT returned a different content identity".into());
        }
        first = Some(first.map_or(digest, |current| current.min(digest)));
        // Settle this sample before subsequent work. This explicit commit is
        // outside the PUT interval, whose public guarantee is only storage.
        fixture.objects.flush(Some(&[digest])).await?;
        let get = timed!(
            rows,
            "objects.get",
            read(1024),
            fixture.objects.get(digest, ByteRange::FULL, 1024)
        )?;
        require_bytes(get.body().as_ref(), &expected)?;
        let head = timed!(rows, "objects.head", read(0), fixture.objects.head(digest))?;
        if head.length != 1024 {
            return Err("object HEAD length mismatch".into());
        }
        let page = timed!(
            rows,
            "objects.list",
            read(32),
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
        rows.record("streams.append", commit(1024), appended);
        rows.record(
            "streams.follow",
            Terms {
                wakes: 1,
                calls: 1,
                ..Terms::default()
            },
            notified,
        );
        next = receipt
            .start
            .checked_add(1)
            .ok_or("stream cursor overflow")?;
        require_bytes(delivered.value.as_ref(), &payload)?;
        if delivered.sequence != receipt.start {
            return Err("follow cursor mismatch".into());
        }
        let records = timed!(rows, "streams.read", read(1024), async {
            stream
                .read(receipt.start, 1)
                .await?
                .try_collect::<Vec<_>>()
                .await
        })?;
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
    for index in 0..samples {
        let actor = ActorReference::new(
            b"benchmark",
            b"measure",
            format!("actor-{index}").as_bytes(),
        )?;
        let creation_key = key(format!("actor-create/{index}"))?;
        timed!(rows, "actors.create", commit(0), async {
            let mut creation = PreparedPublish::foreign(creation_key);
            for effect in store.create(&actor, BTreeMap::new(), None)? {
                creation.join(effect)?;
            }
            store.submit(creation).await
        })?;
        address = Some(actor);
    }
    let address = address.ok_or("no actor samples")?;
    let before = store.load(&address).await?;
    store.claim(&before, 1, key("actor-claim")?).await?;
    let mut state = store.bind_state(store.load(&address).await?, 1)?;
    for index in 0..samples {
        let payload = Bytes::from(vec![u8::try_from(index % 256)?; 128]);
        let operation = key(format!("actor-write/{index}"))?;
        timed!(rows, "actors.durable_write", commit(128), async {
            state.transaction(|transaction| {
                transaction.put("value".into(), payload.clone());
                Ok(())
            })?;
            state.flush(operation).await
        })?;
        let start = Instant::now();
        let value = state.transaction(|transaction| Ok(transaction.get("value").cloned()))?;
        rows.record("actors.warm_read", read(128), start.elapsed());
        require_bytes(value.ok_or("actor metadata missing")?.as_ref(), &payload)?;
    }
    Ok(())
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
    for index in 0..samples {
        let observed = timed!(
            rows,
            "fs.pinned_read",
            read(1024),
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
            "fs.staged_write",
            read(1024),
            transaction.write("/value", staged, WorkBudget::UNBOUNDED, &cancellation)
        )
        .map_err(|failure| failure.error)?;
        let outcome = timed!(
            rows,
            "fs.fsync",
            commit(1024),
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
    }
    Ok(())
}

async fn sessions(
    fixture: &Fixture,
    agents: usize,
    samples: usize,
    rows: &mut Measurements,
) -> Result {
    use acyclic_harness_core::agent::IntentId;
    use acyclic_harness_core::filesystem::hosting::{RootRequest, RootSubmission};
    for index in 0..samples {
        let agent = id(agents + index)?;
        let volume = fixture.volume(&agent)?;
        let intent = IntentId::try_from(format!("session-{index}").as_str())?;
        let request = RootRequest {
            agent,
            fresh: vec![volume],
            retained: Vec::new(),
            brief: None,
            policy: None,
        };
        let created = timed!(
            rows,
            "swarm.session_start",
            Terms {
                wakes: 1,
                ..commit(0)
            },
            fixture.bound.create_root(&intent, request)
        )?;
        if !matches!(created, RootSubmission::Created(_)) {
            return Err("session creation outcome unknown".into());
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
