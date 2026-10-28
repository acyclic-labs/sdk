use futures::TryFutureExt as _;
use super::*;
use crate::conformance::{self, Failure, Package, Target};
use crate::guest::{AlarmChange, Checkpoint, Effect, GuestFuture, Manifest, Transition};
use crate::local::{PreparedPublish, Subscription};
use acyclic_stream::testing::{Counted, RequestLedger};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitMutation, CommitRequest, LocalStream, MemoryStream,
    StreamPath,
};
use bytes::Bytes;
use futures::future::BoxFuture;
use std::sync::atomic::{AtomicU64, Ordering};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn root() -> Result<StreamPath, StreamError> {
    StreamPath::new("harness/actors")
}

fn key(value: &str) -> Result<IdempotencyKey, StreamError> {
    IdempotencyKey::new(value.as_bytes().to_vec())
}

fn actor(id: &str) -> Result<ActorReference, Box<dyn std::error::Error>> {
    Ok(ActorReference::new(b"account", b"agents", id.as_bytes())?)
}

#[test]
fn concurrent_sharded_tracking_keeps_one_registration_per_actor() -> TestResult {
    let runtime = Runtime::new(
        Store::new(Arc::new(MemoryStream::default()), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    let actors = (0..128)
        .map(|index| actor(&format!("shard-{index}")))
        .collect::<Result<Vec<_>, _>>()?;
    std::thread::scope(|scope| {
        let workers = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    for actor in &actors {
                        runtime.track(actor.clone())?;
                        runtime.wake(actor);
                    }
                    Ok::<_, Error>(())
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker
                .join()
                .map_err(|_| Error::Invalid("tracking worker panicked"))??;
        }
        Ok::<_, Error>(())
    })?;
    assert_eq!(runtime.counts().0, actors.len());
    for actor in &actors {
        assert!(runtime.release(actor));
    }
    assert_eq!(runtime.counts(), (0, 0));
    Ok(())
}

/// A running host and the task driving it.
type Running<P> = Option<(Arc<Runtime<P>>, tokio::task::JoinHandle<()>)>;

/// Hosts `local::Runtime` behind the conformance target surface.
struct LocalTarget<P: StreamProvider> {
    store: Store<P>,
    requests: RequestLedger,
    handler: Arc<dyn Handler>,
    running: std::sync::Mutex<Running<P>>,
}

impl<P: StreamProvider> LocalTarget<P> {
    fn start(
        provider: &Arc<P>,
        handler: Arc<dyn Handler>,
        requests: RequestLedger,
    ) -> Result<Self, StreamError> {
        let target = Self {
            store: Store::new(Arc::clone(provider), root()?),
            handler,
            requests,
            running: std::sync::Mutex::new(None),
        };
        target.launch()?;
        Ok(target)
    }

    fn launch(&self) -> Result<(), StreamError> {
        let runtime = Arc::new(Runtime::new(
            Store::new(Arc::clone(self.store.provider()), root()?),
            Arc::clone(&self.handler),
            Arc::new(SystemClock),
        ));
        let host = Arc::clone(&runtime);
        let task = tokio::spawn(async move {
            let _outcome = host.run(|_, _| {}).await;
        });
        *self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((runtime, task));
        Ok(())
    }

    fn runtime(&self) -> Result<Arc<Runtime<P>>, Error> {
        self.running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|(runtime, _)| Arc::clone(runtime))
            .ok_or(Error::Invalid("runtime stopped"))
    }
}

impl<P: StreamProvider> Target for LocalTarget<P> {
    type Provider = P;
    fn requests(&self) -> RequestLedger {
        self.requests.clone()
    }
    fn store(&self) -> &Store<P> {
        &self.store
    }
    fn clock(&self) -> &dyn Clock {
        &SystemClock
    }
    fn pause<'a>(
        &'a self,
        actor: &'a ActorReference,
        paused: bool,
    ) -> BoxFuture<'a, Result<(), Error>> {
        async move { self.runtime()?.pause(actor, paused).await }.boxed()
    }
    fn restart(&self) -> BoxFuture<'_, Result<(), Error>> {
        async move {
            let stopped = self
                .running
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some((_, task)) = stopped {
                task.abort();
                // An aborted host is the crash being modelled.
                let _aborted = task.await;
            }
            self.launch()?;
            Ok(())
        }
        .boxed()
    }
}

async fn memory_backend() -> Result<LocalTarget<Counted<MemoryStream>>, Box<dyn std::error::Error>>
{
    let provider = Counted::new(MemoryStream::default());
    let requests = provider.ledger();
    Ok(LocalTarget::start(
        &Arc::new(provider),
        Arc::new(Package::new()?),
        requests,
    )?)
}

struct DurableBackend {
    target: LocalTarget<Counted<LocalStream>>,
    // The journal directory must outlive the running host and every restart.
    _directory: tempfile::TempDir,
}

impl Target for DurableBackend {
    type Provider = Counted<LocalStream>;
    fn requests(&self) -> RequestLedger {
        self.target.requests()
    }
    fn store(&self) -> &Store<Counted<LocalStream>> {
        self.target.store()
    }
    fn clock(&self) -> &dyn Clock {
        self.target.clock()
    }
    fn pause<'a>(
        &'a self,
        actor: &'a ActorReference,
        paused: bool,
    ) -> BoxFuture<'a, Result<(), Error>> {
        self.target.pause(actor, paused)
    }
    fn restart(&self) -> BoxFuture<'_, Result<(), Error>> {
        self.target.restart()
    }
}

async fn durable_backend() -> Result<DurableBackend, Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = Counted::new(LocalStream::open(directory.path(), Default::default()).await?);
    let requests = provider.ledger();
    let provider = Arc::new(provider);
    Ok(DurableBackend {
        target: LocalTarget::start(&provider, Arc::new(Package::new()?), requests)?,
        _directory: directory,
    })
}

conformance::suite!(conformance_suite_passes_on_memory_runtime, memory_backend);
conformance::suite!(
    conformance_suite_passes_on_durable_local_runtime,
    durable_backend
);

#[tokio::test]
async fn conformance_benchmark_checks_all_deliveries() -> TestResult {
    let backend = memory_backend().await?;
    let sample = conformance::bench(&backend).await?;
    assert_eq!(sample.deliveries, 256);
    eprintln!("ACTORS_CONFORMANCE_BENCH {sample:?}");
    Ok(())
}

/// A host that loses the alarm half of a transition (the old independent
/// alarm write) is the broken behaviour the suite must reject.
struct DropsAlarmClears(Package);
impl Handler for DropsAlarmClears {
    fn manifest(&self) -> &Manifest {
        self.0.manifest()
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        async move {
            let mut transition = self.0.on_record(delivery, checkpoint, cancellation).await?;
            if transition.alarm == AlarmChange::Clear {
                transition.alarm = AlarmChange::Keep;
            }
            Ok(transition)
        }
        .boxed()
    }
}

#[tokio::test]
async fn negative_control_conformance_rejects_a_host_that_loses_alarm_changes() -> TestResult {
    let provider = Counted::new(MemoryStream::default());
    let requests = provider.ledger();
    let target = LocalTarget::start(
        &Arc::new(provider),
        Arc::new(DropsAlarmClears(Package::new()?)),
        requests,
    )?;
    match conformance::verify(&target).await {
        Err(Failure::Violation {
            case: "cleared", ..
        }) => Ok(()),
        other => {
            Err(format!("the suite accepted a host that loses alarm clears: {other:?}").into())
        }
    }
}

struct Manual(AtomicU64);
impl Clock for Manual {
    fn now_unix_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn sleep_until(&self, _deadline_unix_ms: u64) -> BoxFuture<'static, ()> {
        futures::future::pending().boxed()
    }
}

struct CapturesCuts {
    package: Package,
    deliveries: std::sync::Mutex<Vec<Delivery>>,
}

#[tokio::test]
async fn generic_event_host_runs_the_original_package_over_strict_wire() -> TestResult {
    use crate::{guest::wire, hosting::Host};
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "generic-event", &[b"emit", b"set 100"]).await?;
    let package = Arc::new(CapturesCuts {
        package: Package::new()?,
        deliveries: std::sync::Mutex::new(Vec::new()),
    });
    let remote = || -> Result<Arc<dyn Handler>, crate::guest::GuestError> {
        let manifest = wire::decode_manifest(&wire::encode_manifest(package.manifest()))?;
        let package = Arc::clone(&package);
        Ok(Arc::new(wire::Remote::new(
            manifest,
            move |bytes, cancellation| {
                let package = Arc::clone(&package);
                Box::pin(async move {
                    let (delivery, checkpoint) = wire::decode_delivery(&bytes)?;
                    let transition = package
                        .on_record(delivery, checkpoint.as_ref(), cancellation)
                        .await?;
                    wire::encode_transition(&transition)
                }) as GuestFuture<'static, Vec<u8>>
            },
        )))
    };
    let clock = Arc::new(Manual(AtomicU64::new(1)));
    let open = || -> Result<Host<MemoryStream>, Box<dyn std::error::Error>> {
        Ok(Host::new(
            Store::new(Arc::clone(&provider), root()?),
            address.clone(),
            remote()?,
            Arc::clone(&clock) as Arc<dyn Clock>,
            RuntimeLimits::new(1, 1)?,
        ))
    };
    let host = open()?;
    assert_eq!(host.actor(), &address);
    assert_eq!(host.store().root(), store.root());
    assert!(Arc::ptr_eq(host.store().provider(), &provider));
    assert_eq!(host.clock().now_unix_ms(), 1);
    assert!(
        package
            .deliveries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty(),
        "opening does not invoke the package"
    );
    assert_eq!(
        host.wake_paths().await?,
        vec![
            StreamPath::new("agents/generic-event")?,
            store.path(&address)?
        ]
    );
    host.pause(true).await?;
    assert_eq!(host.wake().await?, None);
    assert!(
        package
            .deliveries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty(),
        "a durable pause fences actual package entry"
    );
    host.pause(false).await?;
    assert_eq!(host.wake().await?, Some(101));
    let first = store.load(&address).await?;
    assert_eq!(
        first.state().and_then(|state| state.cursor("inbox")),
        Some(2)
    );
    drop(host);
    clock.0.store(101, Ordering::SeqCst);
    let reopened = open()?;
    assert_eq!(reopened.wake().await?, None);
    assert_eq!(
        reopened.wake().await?,
        None,
        "repeated wake must not repeat the alarm"
    );
    let second = store.load(&address).await?;
    assert!(second.state().map(State::epoch) > first.state().map(State::epoch));
    assert_eq!(
        records(&*provider, "out/generic-event").await?,
        vec![Bytes::from_static(b"r0"), Bytes::from_static(b"a1")]
    );
    let deliveries = package
        .deliveries
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(deliveries.len(), 3);
    let alarm = deliveries.last().ok_or("missing alarm delivery")?;
    assert!(matches!(alarm.trigger, Trigger::Alarm { .. }));
    assert_eq!(alarm.record, None);
    assert_eq!(
        alarm.subscriptions.get("inbox").map(|cut| cut.cursor),
        Some(2)
    );
    Ok(())
}

impl Handler for CapturesCuts {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        self.deliveries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(delivery.clone());
        self.package.on_record(delivery, checkpoint, cancellation)
    }
}

/// Alarm resume must carry Actors' saved input cut even without a record.
/// Resetting to zero would replay the two consumed commands after reopen.
#[tokio::test]
async fn reopened_alarm_delivery_retains_the_committed_subscription_cut() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "delivery-cut", &[b"emit", b"set 100"]).await?;
    let inbox = StreamPath::new("agents/delivery-cut")?;
    let handler = Arc::new(CapturesCuts {
        package: Package::new()?,
        deliveries: std::sync::Mutex::new(Vec::new()),
    });
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        handler.clone(),
        Arc::new(Manual(AtomicU64::new(1))),
    );
    runtime.track(address.clone())?;
    assert!(runtime.drain().await.is_empty());
    drop(runtime);
    let reopened = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        handler.clone(),
        Arc::new(Manual(AtomicU64::new(101))),
    );
    reopened.track(address.clone())?;
    assert!(reopened.drain().await.is_empty());
    let snapshot = store.load(&address).await?;
    assert_eq!(
        snapshot.state().and_then(|state| state.cursor("inbox")),
        Some(2)
    );
    assert_eq!(
        records(&*provider, "out/delivery-cut").await?,
        vec![Bytes::from_static(b"r0"), Bytes::from_static(b"a1")]
    );
    let mut history = provider
        .read(acyclic_stream::ReadRequest {
            path: store.path(&address)?,
            from: 0,
            limit: 16,
        })
        .await?;
    let mut states = BTreeMap::new();
    while let Some(record) = history.next().await {
        let record = record?;
        states.insert(record.commit_id, State::decode(&record.value)?);
    }
    let deliveries = handler
        .deliveries
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(deliveries.len(), 3);
    for (delivery, expected) in deliveries.iter().zip([0, 1, 2]) {
        let admitted = states
            .get(&delivery.subscriptions.anchor())
            .ok_or("delivery anchor is not an original committed state")?;
        assert_eq!(admitted.epoch(), delivery.epoch);
        assert_eq!(
            admitted.subscriptions().get("inbox"),
            delivery.subscriptions.get("inbox")
        );
        assert_eq!(
            delivery.subscriptions.get("inbox"),
            Some(&Subscription {
                path: inbox.clone(),
                cursor: expected,
            })
        );
    }
    assert!(matches!(
        deliveries[2].trigger,
        Trigger::Alarm { generation: 1, .. }
    ));
    assert!(deliveries[2].record.is_none());
    Ok(())
}

fn ledger(outbox: &str) -> Result<Checkpoint, Box<dyn std::error::Error>> {
    Ok(Checkpoint {
        codec: conformance::codec(),
        bytes: serde_json::to_vec(&serde_json::json!({ "outbox": outbox, "published": 0 }))?.into(),
    })
}

async fn records<P: StreamProvider + ?Sized>(
    provider: &P,
    path: &str,
) -> Result<Vec<Bytes>, Box<dyn std::error::Error>> {
    let path = StreamPath::new(path)?;
    let tail = match provider.tail(path.clone()).await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let stream = provider
        .read(acyclic_stream::ReadRequest {
            path,
            from: 0,
            limit: u32::try_from(tail)?.max(1),
        })
        .await?;
    Ok(stream
        .map(|record| record.map(|record| Bytes::copy_from_slice(&record.value)))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?)
}

/// Two actors from one manifest bind different inbox paths and first cursors
/// in a foreign commit; the runtime consumes exactly from each bound cursor.
#[tokio::test]
async fn instance_bound_subscriptions_created_in_a_foreign_commit() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let (first, second) = (actor("first")?, actor("second")?);
    // Five inherited records precede the first actor's cut.
    let prefix = vec![AppendRecord::from(Bytes::from_static(b"emit")); 5];
    PreparedPublish::settle(
        &provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                conditions: vec![CommitCondition::Absent {
                    path: StreamPath::new("agents/first")?,
                }],
                mutations: vec![CommitMutation::Append {
                    path: StreamPath::new("agents/first")?,
                    records: prefix,
                }],
                idempotency_key: key("prefix")?,
            })
            .await?,
    )?;
    let bind = |path: &str, cursor| -> Result<_, StreamError> {
        Ok(BTreeMap::from([(
            "inbox".to_owned(),
            Subscription {
                path: StreamPath::new(path)?,
                cursor,
            },
        )]))
    };
    let mut prepared = PreparedPublish::foreign(key("spawn")?);
    for (address, inbox, cursor, outbox) in [
        (&first, "agents/first", 5, "out/first"),
        (&second, "agents/second", 0, "out/second"),
    ] {
        for effect in store.create(address, bind(inbox, cursor)?, Some(ledger(outbox)?))? {
            prepared.join(effect)?;
        }
    }
    prepared.join(Effect {
        conditions: vec![CommitCondition::Absent {
            path: StreamPath::new("unrelated/log")?,
        }],
        mutation: CommitMutation::Append {
            path: StreamPath::new("unrelated/log")?,
            records: vec![Bytes::from_static(b"spawned").into()],
        },
    })?;
    PreparedPublish::settle(&provider.commit(prepared.into_request()?).await?)?;
    PreparedPublish::settle(
        &provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                conditions: vec![CommitCondition::Tail {
                    path: StreamPath::new("agents/first")?,
                    expected: 5,
                }],
                mutations: vec![CommitMutation::Append {
                    path: StreamPath::new("agents/first")?,
                    records: vec![Bytes::from_static(b"emit").into()],
                }],
                idempotency_key: key("send")?,
            })
            .await?,
    )?;
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(Manual(AtomicU64::new(1))),
    );
    runtime.track(first.clone())?;
    runtime.track(second.clone())?;
    let failures = runtime.drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    // A claim that reset the bound cursor would replay r0..r4 here.
    assert_eq!(
        records(&*provider, "out/first").await?,
        vec![Bytes::from_static(b"r5")]
    );
    assert!(records(&*provider, "out/second").await?.is_empty());
    let state = store.load(&first).await?;
    assert_eq!(
        state.state().and_then(|state| state.cursor("inbox")),
        Some(6)
    );
    assert_eq!(state.state().map(State::epoch), Some(1));
    let state = store.load(&second).await?;
    assert_eq!(
        state.state().and_then(|state| state.cursor("inbox")),
        Some(0)
    );
    assert_eq!(
        runtime.counts(),
        (2, 0),
        "dormant actors hold no ready work"
    );
    Ok(())
}

/// Waits, without polling, until `path` exists. Each wake is a record on
/// `wake`, a stream appended in every commit that can create `path`.
async fn created<P: StreamProvider + ?Sized>(
    provider: &P,
    wake: StreamPath,
    path: StreamPath,
) -> TestResult {
    let mut wakes = provider.follow(acyclic_stream::FollowRequest { path: wake, from: 0, preceding: None, filter: FollowFilter::default(), progress: false }).map_ok(acyclic_stream::records_only).await?;
    loop {
        match provider.tail(path.clone()).await {
            Ok(_) => return Ok(()),
            Err(StreamError::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        wakes.next().await.ok_or("wake stream ended")??;
    }
}

/// Activation by the root's activation log: actors created by other parties'
/// commits get their first delivery with no placement call and no scan.
async fn foreign_creates_activate(count: usize) -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let runtime = Arc::new(Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    ));
    let host = Arc::clone(&runtime);
    let task = tokio::spawn(async move {
        let _outcome = host.run(|_, _| {}).await;
    });
    for index in 0..count {
        let address = actor(&format!("a{index}"))?;
        let inbox = StreamPath::new(format!("agents/a{index}"))?;
        let mut prepared = PreparedPublish::foreign(key(&format!("create-{index}"))?);
        for effect in store.create(
            &address,
            BTreeMap::from([(
                "inbox".to_owned(),
                Subscription {
                    path: inbox.clone(),
                    cursor: 0,
                },
            )]),
            Some(ledger(&format!("out/a{index}"))?),
        )? {
            prepared.join(effect)?;
        }
        // The first input rides in the same commit as the creation.
        prepared.join(Effect {
            conditions: vec![CommitCondition::Absent {
                path: inbox.clone(),
            }],
            mutation: CommitMutation::Append {
                path: inbox,
                records: vec![Bytes::from_static(b"emit").into()],
            },
        })?;
        PreparedPublish::settle(&provider.commit(prepared.into_request()?).await?)?;
    }
    for index in 0..count {
        let address = actor(&format!("a{index}"))?;
        created(
            &*provider,
            store.path(&address)?,
            StreamPath::new(format!("out/a{index}"))?,
        )
        .await?;
    }
    task.abort();
    let _aborted = task.await;
    assert_eq!(runtime.counts().0, count, "one registration per actor");
    Ok(())
}

#[tokio::test]
async fn foreign_created_actors_activate_without_placement_or_scan() -> TestResult {
    foreign_creates_activate(1_000).await
}

#[tokio::test]
#[ignore = "stress"]
async fn ten_thousand_foreign_created_actors_activate() -> TestResult {
    foreign_creates_activate(10_000).await
}

/// Pausing an actor that no host has claimed yet takes effect at epoch 0; the
/// first delivery waits for resume. The refused epoch-0 CAS spun forever.
#[tokio::test]
async fn an_unclaimed_actor_pauses_before_its_first_delivery() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = actor("paused")?;
    let inbox = StreamPath::new("agents/paused")?;
    let mut prepared = PreparedPublish::foreign(key("create")?);
    for effect in store.create(
        &address,
        BTreeMap::from([(
            "inbox".to_owned(),
            Subscription {
                path: inbox.clone(),
                cursor: 0,
            },
        )]),
        Some(ledger("out/paused")?),
    )? {
        prepared.join(effect)?;
    }
    prepared.join(Effect {
        conditions: vec![CommitCondition::Absent {
            path: inbox.clone(),
        }],
        mutation: CommitMutation::Append {
            path: inbox,
            records: vec![Bytes::from_static(b"emit").into()],
        },
    })?;
    PreparedPublish::settle(&provider.commit(prepared.into_request()?).await?)?;
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(Manual(AtomicU64::new(1))),
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        runtime.pause(&address, true),
    )
    .await??;
    assert_eq!(
        store.load(&address).await?.state().map(State::epoch),
        Some(0)
    );
    runtime.track(address.clone())?;
    assert!(runtime.drain().await.is_empty());
    assert!(records(&*provider, "out/paused").await?.is_empty());
    runtime.pause(&address, false).await?;
    assert!(runtime.drain().await.is_empty());
    assert_eq!(
        records(&*provider, "out/paused").await?,
        vec![Bytes::from_static(b"r0")]
    );
    Ok(())
}

/// Create `name` under `store`'s root with its inbox holding `commands`, in
/// one foreign commit.
async fn spawn_with<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    name: &str,
    commands: &[&'static [u8]],
) -> Result<ActorReference, Box<dyn std::error::Error>> {
    let address = actor(name)?;
    let inbox = StreamPath::new(format!("agents/{name}"))?;
    let mut prepared = PreparedPublish::foreign(key(&format!("spawn-{name}"))?);
    for effect in store.create(
        &address,
        BTreeMap::from([(
            "inbox".to_owned(),
            Subscription {
                path: inbox.clone(),
                cursor: 0,
            },
        )]),
        Some(ledger(&format!("out/{name}"))?),
    )? {
        prepared.join(effect)?;
    }
    prepared.join(Effect {
        conditions: vec![CommitCondition::Absent {
            path: inbox.clone(),
        }],
        mutation: CommitMutation::Append {
            path: inbox,
            records: commands
                .iter()
                .map(|command| Bytes::from_static(command).into())
                .collect(),
        },
    })?;
    PreparedPublish::settle(&store.provider().commit(prepared.into_request()?).await?)?;
    Ok(address)
}

struct ActiveAttempt<'a>(&'a std::sync::atomic::AtomicUsize);
impl Drop for ActiveAttempt<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Gated {
    package: Package,
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    gate: tokio::sync::Semaphore,
}
impl Handler for Gated {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            let _attempt = ActiveAttempt(&self.active);
            self.peak.fetch_max(active, Ordering::SeqCst);
            self.gate
                .acquire()
                .await
                .map_err(|_| crate::guest::GuestError::Handler("test gate closed".into()))?
                .forget();
            self.package
                .on_record(delivery, checkpoint, cancellation)
                .await
        }
        .boxed()
    }
}

async fn bounded_admission_and_abort(event_owned: bool) -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let handler = Arc::new(Gated {
        package: Package::new()?,
        active: std::sync::atomic::AtomicUsize::new(0),
        peak: std::sync::atomic::AtomicUsize::new(0),
        gate: tokio::sync::Semaphore::new(0),
    });
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        handler.clone(),
        Arc::new(SystemClock),
    )
    .with_limits(RuntimeLimits::new(2, 2)?);
    let mut actors = Vec::new();
    for index in 0..6 {
        let address = spawn_with(&store, &format!("bounded-{index}"), &[b"emit"]).await?;
        runtime.track(address.clone())?;
        actors.push(address);
    }
    {
        let events = FuturesUnordered::new();
        for address in &actors {
            events.push(runtime.drain_actor(address));
        }
        let mut host = Box::pin(runtime.run(|_, error| panic!("host failed: {error}")));
        let mut events = events;
        poll_fn(|context| {
            if event_owned {
                assert!(events.poll_next_unpin(context).is_pending());
            } else {
                assert!(host.as_mut().poll(context).is_pending());
            }
            if handler.active.load(Ordering::SeqCst) == 2 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        assert_eq!(handler.peak.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.admission.available_permits(), 0);
        for address in &actors {
            assert_eq!(
                store
                    .load(address)
                    .await?
                    .state()
                    .and_then(|state| state.cursor("inbox")),
                Some(0)
            );
        }
        // Drop a real in-flight handler, provider follows and admission waiters.
        // Its claim may be durable; its input and effect must remain untouched.
    }
    assert_eq!(handler.active.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.admission.available_permits(), 2);
    handler.gate.add_permits(actors.len());
    for address in &actors {
        runtime.wake(address);
    }
    assert!(runtime.drain().await.is_empty());
    for index in 0..actors.len() {
        assert_eq!(
            records(&*provider, &format!("out/bounded-{index}")).await?,
            vec![Bytes::from_static(b"r0")]
        );
    }
    assert_eq!(handler.peak.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn continuous_host_bounds_admission_and_aborted_delivery_releases_it() -> TestResult {
    bounded_admission_and_abort(false).await
}

#[tokio::test]
async fn concurrent_event_hosts_share_admission_and_aborted_delivery_releases_it() -> TestResult {
    bounded_admission_and_abort(true).await
}

#[tokio::test]
async fn hot_actor_requeues_behind_other_ready_work_at_its_committed_cut() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let hot = spawn_with(&store, "hot", &[b"emit".as_slice(); 8]).await?;
    let cold = spawn_with(&store, "cold", &[b"emit"]).await?;
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    )
    .with_limits(RuntimeLimits::new(1, 2)?);
    runtime.track(hot.clone())?;
    runtime.track(cold.clone())?;
    let work = runtime.ready.take().ok_or("missing initial hot work")?;
    assert_eq!(work.key(), &hot);
    runtime
        .execute(work, Inputs::Follow)
        .await
        .map_err(|(_, error)| error)?;
    assert_eq!(
        store
            .load(&hot)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(1)
    );
    let work = runtime.ready.take().ok_or("missing cold work")?;
    assert_eq!(work.key(), &cold, "a hot actor must rejoin at the back");
    runtime
        .execute(work, Inputs::Follow)
        .await
        .map_err(|(_, error)| error)?;
    assert_eq!(
        records(&*provider, "out/cold").await?,
        vec![Bytes::from_static(b"r0")]
    );
    assert_eq!(
        records(&*provider, "out/hot").await?,
        vec![Bytes::from_static(b"r0")]
    );
    runtime.drain_actor(&hot).await?;
    assert_eq!(
        store
            .load(&hot)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(8)
    );
    assert_eq!(records(&*provider, "out/hot").await?.len(), 8);
    Ok(())
}

#[tokio::test]
async fn event_batch_yield_retains_placement_until_completion_or_abort() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "event-lease", &[b"emit"]).await?;
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    )
    .with_limits(RuntimeLimits::new(1, 1)?);
    runtime.track(address.clone())?;
    {
        let delivery = runtime.drain_actor(&address);
        futures::pin_mut!(delivery);
        assert!(
            delivery
                .as_mut()
                .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
                .is_pending()
        );
        assert_eq!(
            runtime.admission.available_permits(),
            1,
            "the next actor may enter during the yield"
        );
        assert!(
            !runtime.release(&address),
            "the unfinished event still owns placement"
        );
        assert_eq!(
            store
                .load(&address)
                .await?
                .state()
                .and_then(|state| state.cursor("inbox")),
            Some(0)
        );
    }
    assert!(
        runtime.release(&address),
        "aborting the event releases its placement guard"
    );
    runtime.track(address.clone())?;
    runtime.drain_actor(&address).await?;
    assert_eq!(
        records(&*provider, "out/event-lease").await?,
        vec![Bytes::from_static(b"r0")]
    );
    assert!(
        runtime.release(&address),
        "a completed event releases its placement guard"
    );
    Ok(())
}

/// A paused actor's overdue alarm leaves the due index (no wake loop) but
/// stays committed and fires once on resume.
#[tokio::test]
async fn a_paused_actor_withdraws_its_due_alarm_until_resume() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let clock = Arc::new(Manual(AtomicU64::new(1)));
    let shared: Arc<dyn Clock> = Arc::clone(&clock) as Arc<dyn Clock>;
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        shared,
    );
    let address = spawn_with(&store, "sleeper", &[b"set 10"]).await?;
    runtime.track(address.clone())?;
    assert!(runtime.drain().await.is_empty());
    assert_eq!(runtime.ready.next_deadline(), Some(11));
    runtime.pause(&address, true).await?;
    runtime.wake(&address);
    assert!(runtime.drain().await.is_empty());
    // Control: scheduling before the paused check keeps Some(11) here, and
    // every timer fire re-queues the paused actor.
    assert_eq!(
        runtime.ready.next_deadline(),
        None,
        "paused alarm left the due index"
    );
    clock.0.store(100, Ordering::SeqCst);
    assert!(runtime.drain().await.is_empty());
    assert_eq!(runtime.counts().1, 0, "no wake while paused");
    assert!(records(&*provider, "out/sleeper").await?.is_empty());
    runtime.pause(&address, false).await?;
    assert!(runtime.drain().await.is_empty());
    assert_eq!(
        records(&*provider, "out/sleeper").await?,
        vec![Bytes::from_static(b"a1")]
    );
    Ok(())
}

/// Two runtimes claiming one cut use distinct attempt keys, so the second
/// claim is fenced instead of replaying the first claim's saved success.
#[tokio::test]
async fn two_runtimes_claiming_one_cut_cannot_share_an_epoch() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "contested", &[b"emit"]).await?;
    let host = || -> Result<Runtime<MemoryStream>, Box<dyn std::error::Error>> {
        Ok(Runtime::new(
            Store::new(Arc::clone(&provider), root()?),
            Arc::new(Package::new()?),
            Arc::new(SystemClock),
        ))
    };
    let (first, second) = (host()?, host()?);
    let cut = store.load(&address).await?;
    let attempt = |runtime: &Runtime<MemoryStream>| {
        super::key(&cut, &format!("claim-1-{}", runtime.instance))
    };
    store.claim(&cut, 1, attempt(&first)?).await?;
    // A retry of the same attempt replays its own success.
    store.claim(&cut, 1, attempt(&first)?).await?;
    // Control: with one shared `claim-1` key this replays success as well.
    assert!(matches!(
        store.claim(&cut, 1, attempt(&second)?).await,
        Err(Error::Conflict)
    ));
    Ok(())
}

/// Count actual handler admission, including work a later CAS would refuse.
struct Invocations {
    package: Package,
    calls: Arc<AtomicU64>,
}

impl Handler for Invocations {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.package.on_record(delivery, checkpoint, cancellation)
    }
}

/// A retained cut is evidence for CAS, never permission to invoke a handler
/// after an external pause or replacement has already committed.
#[tokio::test]
async fn retained_cuts_refresh_after_pause_resume_and_foreign_claim() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "retained", &[b"emit"]).await?;
    let inbox = StreamPath::new("agents/retained")?;
    let calls = Arc::new(AtomicU64::new(0));
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Invocations {
            package: Package::new()?,
            calls: Arc::clone(&calls),
        }),
        Arc::new(Manual(AtomicU64::new(1))),
    );
    runtime.track(address.clone())?;
    assert!(runtime.drain().await.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    provider
        .append(AppendRequest {
            path: inbox.clone(),
            records: vec![Bytes::from_static(b"emit").into()],
            if_tail: Some(1),
            idempotency_key: None,
        })
        .await?;
    let cut = store.load(&address).await?;
    store
        .set_paused(&cut, 1, true, key("external-pause")?)
        .await?;
    runtime.wake(&address);
    assert!(runtime.drain().await.is_empty());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "paused actor must not invoke its handler"
    );
    assert_eq!(
        records(&*provider, "out/retained").await?,
        vec![Bytes::from_static(b"r0")]
    );
    let cut = store.load(&address).await?;
    store
        .set_paused(&cut, 1, false, key("external-resume")?)
        .await?;
    runtime.wake(&address);
    assert!(runtime.drain().await.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        records(&*provider, "out/retained").await?,
        vec![Bytes::from_static(b"r0"), Bytes::from_static(b"r1")]
    );
    let cut = store.load(&address).await?;
    store.claim(&cut, 2, key("foreign-placement")?).await?;
    provider
        .append(AppendRequest {
            path: inbox,
            records: vec![Bytes::from_static(b"emit").into()],
            if_tail: Some(2),
            idempotency_key: None,
        })
        .await?;
    runtime.wake(&address);
    let failures = runtime.drain().await;
    assert!(
        matches!(failures.as_slice(), [(_, Error::Conflict)]),
        "{failures:?}"
    );
    assert_eq!(runtime.counts(), (0, 0));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "replaced placement must not invoke its handler"
    );
    assert_eq!(records(&*provider, "out/retained").await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn retained_cut_is_invalidated_when_delivery_is_cancelled() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "cancelled", &[b"emit"]).await?;
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Limited::new(10_000, 1 << 16, true)?),
        Arc::new(Manual(AtomicU64::new(1))),
    );
    runtime.track(address.clone())?;
    let slot = runtime.slot(&address).ok_or("missing actor slot")?;
    // Poll through claim and state observation, then cancel the stalled handler.
    assert!(
        runtime
            .deliver(&address, &slot, Inputs::Event)
            .now_or_never()
            .is_none()
    );
    drop(slot);
    let cut = store.load(&address).await?;
    assert_eq!(cut.state().ok_or("missing actor state")?.epoch(), 1);
    store
        .claim(&cut, 2, key("replacement-after-cancel")?)
        .await?;
    runtime.wake(&address);
    let failures = runtime.drain().await;
    assert!(
        matches!(failures.as_slice(), [(_, Error::Conflict)]),
        "{failures:?}"
    );
    assert_eq!(runtime.counts(), (0, 0));
    assert!(records(&*provider, "out/cancelled").await?.is_empty());
    Ok(())
}

/// Generated keys stay bounded for any valid root: a 338-byte root hosts.
#[tokio::test]
async fn a_long_state_root_still_claims_and_delivers() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let long = StreamPath::new(format!("harness/{}", ["segment-xx"; 30].join("/")))?;
    let store = Store::new(Arc::clone(&provider), long.clone());
    let address = spawn_with(&store, "deep", &[b"emit"]).await?;
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), long),
        Arc::new(Package::new()?),
        Arc::new(Manual(AtomicU64::new(1))),
    );
    runtime.track(address)?;
    // Control: a key embedding the path exceeds 256 bytes, so every claim
    // fails with InvalidArgument.
    let failures = runtime.drain().await;
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(
        records(&*provider, "out/deep").await?,
        vec![Bytes::from_static(b"r0")]
    );
    Ok(())
}

/// Wraps the package with explicit manifest limits; `stall` never resolves.
struct Limited {
    package: Package,
    manifest: Manifest,
    stall: bool,
}
impl Limited {
    fn new(
        timeout_ms: u64,
        checkpoint_bytes: u64,
        stall: bool,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            package: Package::new()?,
            manifest: Manifest {
                code: crate::domain::CodeSha256::try_from(vec![0xa4; 32])?,
                codec: conformance::codec(),
                subscriptions: vec!["inbox".into()],
                interrupts: Vec::new(),
                bindings: Vec::new(),
                limits: crate::domain::ActorLimits::new(timeout_ms, 1 << 20, checkpoint_bytes)?,
            },
            stall,
        })
    }
}
impl Handler for Limited {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        if self.stall {
            return futures::future::pending().boxed();
        }
        self.package.on_record(delivery, checkpoint, cancellation)
    }
}

type Outcome = (Vec<(ActorReference, Error)>, Vec<Bytes>);

async fn run_limited(handler: Limited) -> Result<Outcome, Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let address = spawn_with(&store, "limited", &[b"emit"]).await?;
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(handler),
        Arc::new(Manual(AtomicU64::new(1))),
    );
    runtime.track(address)?;
    let failures = runtime.drain().await;
    Ok((failures, records(&*provider, "out/limited").await?))
}

fn refused(failures: &[(ActorReference, Error)], reason: &str) -> bool {
    matches!(failures, [(_, Error::Invalid(found))] if found.contains(reason))
}

/// Manifest limits bind: a stalled handler is abandoned at its timeout and an
/// oversized checkpoint is refused; neither consumes the input.
#[tokio::test]
async fn manifest_limits_bound_handler_time_and_checkpoint_size() -> TestResult {
    let stalled = run_limited(Limited::new(50, 1 << 16, true)?);
    let (failures, published) =
        tokio::time::timeout(std::time::Duration::from_secs(5), stalled).await??;
    assert!(refused(&failures, "timeout"), "{failures:?}");
    assert!(published.is_empty());
    // Control: the same package within its limits publishes.
    let (failures, published) = run_limited(Limited::new(10_000, 1 << 16, false)?).await?;
    assert!(failures.is_empty(), "{failures:?}");
    assert_eq!(published, vec![Bytes::from_static(b"r0")]);
    let (failures, published) = run_limited(Limited::new(10_000, 4, false)?).await?;
    assert!(refused(&failures, "checkpoint"), "{failures:?}");
    assert!(published.is_empty());
    Ok(())
}
