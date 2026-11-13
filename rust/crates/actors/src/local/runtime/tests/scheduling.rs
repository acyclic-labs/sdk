use super::*;

/// Shard routing and checkpoint custody survive node replacement without a
/// root broadcast, rewriting producer keys or sharing checkpoint state.
#[tokio::test]
async fn shard_assignment_isolates_wakes_and_checkpoint_progress() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let first = spawn_with(&store, "scheduled-first", &[b"emit"]).await?;
    let shard = store.scheduling_shard(&first);
    let mut candidate = 0_u64;
    let second = loop {
        let address = actor(&format!("scheduled-other-{candidate}"))?;
        if store.scheduling_shard(&address) != shard {
            break address;
        }
        candidate = candidate
            .checked_add(1)
            .ok_or("no distinct scheduling identity")?;
    };
    let mut creation = PreparedPublish::foreign(key("scheduling/second")?);
    creation.join(store.create(None, &second, crate::local::Creation::default())?)?;
    creation.join(store.activation(&second)?)?;
    store.submit(creation).await?;
    let other_shard = store.scheduling_shard(&second);
    let original_path = store.activation_record(&first)?.0;
    assert_ne!(original_path, store.activation_record(&second)?.0);
    assert!(matches!(
        provider
            .tail(root()?)
            .await
            .map_err(|error| error.category()),
        Ok(0) | Err(StreamError::NotFound)
    ));
    let host = Runtime::new(
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    )
    .with_scheduling_shards([shard])?;
    assert!(matches!(
        Runtime::new(
            Store::new(Arc::clone(&provider), root()?),
            Arc::new(Package::new()?),
            Arc::new(SystemClock),
        )
        .with_scheduling_shards([store.scheduling_shards()]),
        Err(Error::Invalid(
            "scheduling shard outside the store's partitions"
        ))
    ));
    host.place(first.clone(), 1).await?;
    assert!(matches!(
        host.place(second.clone(), 1).await,
        Err(Error::Invalid("actor is outside this runtime's scope"))
    ));
    let (checkpoint, address) = host.dispatcher_store(shard)?;
    let initial = checkpoint.load(&address).await?;
    let tail = provider.tail(original_path.clone()).await?;
    host.advance_dispatcher(&checkpoint, initial, shard, tail, 0, Vec::new())
        .await?;
    let replacement = Runtime::new(
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    )
    .with_scheduling_shards([shard])?;
    replacement.place(first.clone(), 2).await?;
    assert_eq!(
        replacement.store().activation_record(&first)?.0,
        original_path
    );
    let restored = checkpoint.load(&address).await?;
    assert_eq!(
        restored
            .state()
            .and_then(|state| state.cursor("activation")),
        Some(tail)
    );
    let (other_checkpoint, other_address) = replacement.dispatcher_store(other_shard)?;
    let other = other_checkpoint.load(&other_address).await?;
    assert!(
        other.state().is_none(),
        "an unadvanced shard owns no checkpoint"
    );
    assert_ne!(
        checkpoint.path(&address)?,
        other_checkpoint.path(&other_address)?
    );
    assert_eq!(provider.tail(store.scheduling_path(other_shard)?).await?, 1);
    Ok(())
}

/// The checkpoint of the store's only shard, once its `cursor` reaches `tail`.
async fn checkpointed<P: StreamProvider + ?Sized>(
    runtime: &Runtime<P>,
    cursor: &str,
    tail: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    use futures::StreamExt as _;
    let (checkpoints, dispatcher) = runtime.dispatcher_store(0)?;
    let mut follow = acyclic_stream::FollowRequest::new(checkpoints.path(&dispatcher)?, 0);
    follow.when_absent = acyclic_stream::WhenAbsent::Wait;
    let mut records =
        acyclic_stream::records_only(runtime.store().provider().follow(follow).await?);
    while let Some(record) = records.next().await {
        if State::decode(&record?.value)?.cursor(cursor) >= Some(tail) {
            return Ok(());
        }
    }
    Err("dispatcher checkpoint follow ended".into())
}

/// Serve `runtime` until its only shard checkpoints past the log's tail.
async fn serve_to_checkpoint<P: StreamProvider + ?Sized>(
    runtime: &Runtime<P>,
) -> Result<(), Box<dyn std::error::Error>> {
    let tail = runtime
        .store()
        .provider()
        .tail(runtime.store().scheduling_path(0)?)
        .await?;
    let mut serving = Box::pin(runtime.run(|_, error| panic!("delivery failed: {error}")));
    tokio::select! {
        stopped = &mut serving => Err(format!("host stopped: {stopped:?}").into()),
        result = checkpointed(runtime, "activation", tail) => result,
    }
}

/// Requests a restarted host issues to deliver one due actor among `dormant`
/// checkpointed ones, through its next shard checkpoint.
async fn restart_work(
    dormant: usize,
) -> Result<acyclic_stream::testing::Requests, Box<dyn std::error::Error>> {
    let one = std::num::NonZeroU16::MIN;
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?).with_scheduling_shards(one);
    let mut residents = Vec::new();
    for index in 0..dormant {
        residents.push(spawn_with(&store, &format!("dormant-{index}"), &[b"emit"]).await?);
    }
    serve_to_checkpoint(&Runtime::new(
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&provider), root()?).with_scheduling_shards(one),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    ))
    .await?;
    let due = residents.first().ok_or("no dormant actor")?;
    let mut input = PreparedPublish::foreign(key("restart/input")?);
    input.join(acyclic_stream::CommitEffect::hard(
        Vec::new(),
        vec![CommitMutation::Append {
            path: StreamPath::new("agents/dormant-0")?,
            records: vec![Bytes::from_static(b"emit").into()],
        }],
    ))?;
    input.join(store.activation(due)?)?;
    PreparedPublish::settle(&provider.commit(input.into_request()?).await?)?;
    let counted = Arc::new(Counted::new((*provider).clone()));
    serve_to_checkpoint(&Runtime::new(
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&counted), root()?).with_scheduling_shards(one),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    ))
    .await?;
    assert_eq!(records(&*provider, "out/dormant-0").await?.len(), 2);
    assert!(
        crate::local::tests::orphans(&store, &residents)
            .await?
            .is_empty()
    );
    Ok(counted.requests())
}

/// Restart work is bounded by the assigned shards and the due actors, never
/// by the dormant population: N and 16N checkpointed actors cost the same.
#[tokio::test]
async fn restart_work_is_independent_of_the_dormant_population() -> TestResult {
    let small = restart_work(8).await?;
    let large = restart_work(128).await?;
    assert!(small.data() > 0, "the counted host must do live work");
    assert_eq!(
        (small.control(), small.data()),
        (large.control(), large.data()),
        "restart work grew with dormant actors"
    );
    Ok(())
}

/// The target's epoch before and after a fresh host serves one forged
/// wake for it and one for a never-created actor, and its output count. The
/// wake is genuine when `input` joins it. A wake entry is only a hint: delivery reads the
/// actor's own committed state, and only work admits placement.
async fn after_wake(
    input: bool,
) -> Result<(Option<u64>, Option<u64>, usize), Box<dyn std::error::Error>> {
    let one = std::num::NonZeroU16::MIN;
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?).with_scheduling_shards(one);
    let target = spawn_with(&store, "forged-target", &[b"emit"]).await?;
    let host = || {
        Ok::<_, Box<dyn std::error::Error>>(Runtime::new(
            Store::new(Arc::clone(&provider), root()?).with_scheduling_shards(one),
            Arc::new(Package::new()?),
            Arc::new(SystemClock),
        ))
    };
    serve_to_checkpoint(&host()?).await?;
    let before = store.load(&target).await?.state().map(State::epoch);
    let ghost = actor("forged-ghost")?;
    let mut forged = PreparedPublish::foreign(key("forged/wake")?);
    forged.join(store.activation(&target)?)?;
    forged.join(store.activation(&ghost)?)?;
    if input {
        forged.join(acyclic_stream::CommitEffect::hard(
            Vec::new(),
            vec![CommitMutation::Append {
                path: StreamPath::new("agents/forged-target")?,
                records: vec![Bytes::from_static(b"emit").into()],
            }],
        ))?;
    }
    PreparedPublish::settle(&provider.commit(forged.into_request()?).await?)?;
    serve_to_checkpoint(&host()?).await?;
    assert!(
        store.load(&ghost).await?.state().is_none(),
        "a wake created an actor"
    );
    Ok((
        before,
        store.load(&target).await?.state().map(State::epoch),
        records(&*provider, "out/forged-target").await?.len(),
    ))
}

/// A wake entry carries no authority: without committed input it claims,
/// creates and delivers nothing. The control appends real input and must
/// deliver it through the same wake.
#[tokio::test]
async fn a_wake_without_work_claims_and_delivers_nothing() -> TestResult {
    let (before, after, output) = after_wake(false).await?;
    assert!(before.is_some());
    assert_eq!(after, before, "a forged wake moved the fence");
    assert_eq!(output, 1, "a forged wake delivered");
    let (_, _, delivered) = after_wake(true).await?;
    assert_eq!(delivered, 2, "the control's genuine wake must deliver");
    Ok(())
}

/// The first instant of deadline window 10.
const WINDOW_TEN: u64 = 600_000;

/// Complete `runtime`'s current work while it serves.
async fn completed(
    runtime: &Runtime<MemoryStream>,
    serving: std::pin::Pin<&mut impl Future<Output = Result<Infallible, Error>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    observe_while_running(serving, async {
        runtime.complete().await.map_err(Into::into)
    })
    .await
}

/// One single-shard store, and `name` spawned with `commands` and delivered
/// by an earlier host, so the next host finds its alarm window at startup.
async fn armed_before_start(
    clock: &Arc<ControlledClock>,
    name: &str,
    commands: &[&'static [u8]],
) -> Result<(Arc<MemoryStream>, Store<MemoryStream>, ActorReference), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?)
        .with_scheduling_shards(std::num::NonZeroU16::MIN);
    let actor = spawn_with(&store, name, commands).await?;
    let earlier = Runtime::new(
        crate::guest::Requests::default(),
        store.clone(),
        Arc::new(Package::new()?),
        Arc::clone(clock) as Arc<dyn Clock>,
    );
    earlier.track(actor.clone())?;
    earlier.drain_actor(&actor).await?;
    Ok((provider, store, actor))
}

/// Hibernating never drops an armed alarm, whether its window was already
/// opened (the host takes the alarm over) or was armed after the host
/// started and never discovered (the host learns the window).
#[tokio::test]
async fn hibernated_alarms_fire_in_the_running_host() -> TestResult {
    let (clock, mut time) = ControlledClock::new(WINDOW_TEN);
    let clock = Arc::new(clock);
    let (provider, store, near) =
        armed_before_start(&clock, "hibernate-near", &[b"set 30000"]).await?;
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store.clone(),
        Arc::new(Package::new()?),
        Arc::clone(&clock) as Arc<dyn Clock>,
    );
    let mut serving = Box::pin(runtime.run(|_, error| panic!("delivery failed: {error}")));
    // Window ten opens at once and wakes `near`, and no later window exists.
    completed(&runtime, serving.as_mut()).await?;
    // `far` then arms window twelve, which the host never discovered.
    let far = spawn_with(&store, "hibernate-far", &[b"set 150000"]).await?;
    completed(&runtime, serving.as_mut()).await?;
    for actor in [&near, &far] {
        assert!(runtime.release(actor), "an idle actor did not hibernate");
    }
    time.advance_to(WINDOW_TEN + 150_000)?;
    completed(&runtime, serving.as_mut()).await?;
    for name in ["hibernate-near", "hibernate-far"] {
        assert_eq!(
            records(provider.as_ref(), &format!("out/{name}")).await?,
            vec![Bytes::from_static(b"a1")],
            "{name} lost its alarm when it hibernated"
        );
    }
    Ok(())
}

/// Input a paused actor holds while its host's dispatcher passes it.
#[derive(Clone, Copy, Debug)]
enum Held {
    /// An alarm whose window the dispatcher passes.
    Alarm,
    /// A subscription record whose activation entry the dispatcher passes.
    Record,
}

/// The host pauses the actor, checkpoints past its held input and stops;
/// the resume commits and no host delivers before a restart. Returns the
/// restarted host's output. `forget` strips the resume's activation from
/// its commit, leaving only an in-memory wake: the negative control.
async fn resumed_after_checkpoint(
    held: Held,
    forget: bool,
) -> Result<Vec<Bytes>, Box<dyn std::error::Error>> {
    let (clock, mut time) = ControlledClock::new(WINDOW_TEN);
    let clock = Arc::new(clock);
    let (name, command) = match held {
        Held::Alarm => ("resumed-alarm", b"set 1000".as_slice()),
        Held::Record => ("resumed-record", b"emit".as_slice()),
    };
    let (provider, store, actor) = armed_before_start(&clock, name, &[command]).await?;
    {
        let host = Runtime::new(
            crate::guest::Requests::default(),
            store.clone(),
            Arc::new(Package::new()?),
            Arc::clone(&clock) as Arc<dyn Clock>,
        );
        let mut serving = Box::pin(host.run(|_, error| panic!("delivery failed: {error}")));
        completed(&host, serving.as_mut()).await?;
        observe_while_running(serving.as_mut(), async {
            host.pause(&actor, true).await.map_err(Into::into)
        })
        .await?;
        let passed = match held {
            Held::Alarm => {
                time.advance_to(WINDOW_TEN + 120_000)?;
                checkpointed(
                    &host,
                    "deadline",
                    crate::local::scheduling::window(WINDOW_TEN) + 1,
                )
            }
            Held::Record => {
                let mut input = PreparedPublish::foreign(key("resumed/input")?);
                input.join(acyclic_stream::CommitEffect::hard(
                    Vec::new(),
                    vec![CommitMutation::Append {
                        path: StreamPath::new(format!("agents/{name}"))?,
                        records: vec![Bytes::from_static(b"emit").into()],
                    }],
                ))?;
                input.join(store.activation(&actor)?)?;
                store.submit(input).await?;
                let tail = provider.tail(store.scheduling_path(0)?).await?;
                checkpointed(&host, "activation", tail)
            }
        };
        observe_while_running(serving.as_mut(), passed).await?;
    }
    let snapshot = store.load(&actor).await?;
    let mut state = snapshot.state().ok_or("missing actor state")?.clone();
    state.paused = false;
    let mut resume = store.prepare(&snapshot, state, Vec::new(), key("resumed/resume")?, false)?;
    if forget {
        let shard = store.scheduling_path(store.scheduling_shard(&actor))?;
        resume.request.effects[0]
            .mutations
            .retain(|mutation| CommitMutation::path(mutation) != &shard);
    }
    store.submit(resume).await?;
    let host = Runtime::new(
        crate::guest::Requests::default(),
        store,
        Arc::new(Package::new()?),
        clock,
    );
    let mut serving = Box::pin(host.run(|_, error| panic!("delivery failed: {error}")));
    completed(&host, serving.as_mut()).await?;
    Ok(records(provider.as_ref(), &format!("out/{name}")).await?)
}

/// A crash after a resume commits and before any host delivers loses no
/// held input: the resume itself carries the durable activation.
#[tokio::test]
async fn a_resume_survives_a_crash_before_delivery() -> TestResult {
    for (held, delivered) in [
        (Held::Alarm, vec![Bytes::from_static(b"a1")]),
        (
            Held::Record,
            vec![Bytes::from_static(b"r0"), Bytes::from_static(b"r1")],
        ),
    ] {
        assert_eq!(
            resumed_after_checkpoint(held, false).await?,
            delivered,
            "{held:?}: the restarted host lost the resumed input"
        );
        assert_ne!(
            resumed_after_checkpoint(held, true).await?,
            delivered,
            "{held:?}: the negative control must lose the resumed input"
        );
    }
    Ok(())
}

/// Deleting a resident actor with an armed alarm is finished work for its
/// running host: completion keeps working for every other actor.
#[tokio::test]
async fn deleting_a_resident_actor_keeps_its_host_completing() -> TestResult {
    let (clock, mut time) = ControlledClock::new(WINDOW_TEN);
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store.clone(),
        Arc::new(Package::new()?),
        Arc::new(clock),
    );
    let mut serving = Box::pin(runtime.run(|_, error| panic!("delivery failed: {error}")));
    let doomed = spawn_with(&store, "doomed", &[b"set 60000"]).await?;
    completed(&runtime, serving.as_mut()).await?;
    let snapshot = store.load(&doomed).await?;
    store
        .submit(store.delete(&snapshot, key("doomed/delete")?)?)
        .await?;
    time.advance_to(WINDOW_TEN + 60_000)?;
    spawn_with(&store, "survivor", &[b"emit"]).await?;
    completed(&runtime, serving.as_mut()).await?;
    assert!(records(provider.as_ref(), "out/doomed").await?.is_empty());
    assert_eq!(
        records(provider.as_ref(), "out/survivor").await?,
        vec![Bytes::from_static(b"r0")]
    );
    Ok(())
}

/// Pause control admits an inherited branch with the store's own
/// configuration, so its inherited alarm lands in the store's own shard.
#[tokio::test]
async fn pause_control_files_an_inherited_alarm_in_the_stores_shard() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?)
        .with_scheduling_shards(std::num::NonZeroU16::MIN);
    let unconfigured = Store::new(Arc::clone(&provider), root()?);
    let parent = spawn_with(&store, "inherit-parent", &[b"set 600000"]).await?;
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        store.clone(),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    runtime.track(parent.clone())?;
    runtime.drain_actor(&parent).await?;
    // A child that a default-partitioned store would file elsewhere.
    let mut index = 0_u32;
    let child = loop {
        let candidate = actor(&format!("inherit-child-{index}"))?;
        if unconfigured.scheduling_shard(&candidate) != 0 {
            break candidate;
        }
        index += 1;
    };
    let cut = store.load(&parent).await?;
    let mut fork = PreparedPublish::foreign(key("inherit/fork")?);
    fork.join(
        store.branch(
            &cut,
            &child,
            crate::local::Branch::unfunded(
                cut.state()
                    .ok_or("missing parent state")?
                    .subscriptions()
                    .clone(),
            ),
        )?,
    )?;
    store.submit(fork).await?;
    // The branch is born paused; resuming it is its first placement.
    runtime.pause(&child, false).await?;
    let deadline = store
        .load(&child)
        .await?
        .state()
        .and_then(|state| state.alarm().pending())
        .ok_or("the child did not inherit its parent's alarm")?;
    let (filed, _) = store
        .window_actors(0, crate::local::scheduling::window(deadline), 0)
        .await?;
    assert!(
        filed.contains(&child),
        "the inherited alarm was filed outside the store's own shard"
    );
    Ok(())
}

/// Fails every delivery to one actor, then delegates.
struct Poisons {
    package: Package,
    poisoned: ActorReference,
}

impl Handler for Poisons {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        if delivery.actor == self.poisoned {
            return futures::future::ready(Err(crate::guest::GuestError::Handler(
                "poisoned".into(),
            )))
            .boxed();
        }
        self.package.on_record(delivery, checkpoint, cancellation)
    }
}

/// An actor whose every delivery fails holds back only its own wake: real
/// dispatcher checkpoints pass the healthy actor's entries by committing one
/// carried entry for it, and a restarted host replays just that entry and
/// delivers the retained input.
///
/// Negative control: committing the checkpoint without its carried entry
/// stalled it behind the poisoned actor's missing entry, short of the
/// healthy actor's entries from the next round on.
#[tokio::test]
async fn a_poisoned_actor_is_carried_through_checkpoints_and_restart() -> TestResult {
    const ROUNDS: usize = 4;
    let one = std::num::NonZeroU16::MIN;
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?).with_scheduling_shards(one);
    let poisoned = spawn_with(&store, "poisoned", &[b"emit"]).await?;
    let healthy = spawn_with(&store, "healthy", &[b"emit"]).await?;
    let log = store.scheduling_path(0)?;
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&provider), root()?).with_scheduling_shards(one),
        Arc::new(Poisons {
            package: Package::new()?,
            poisoned: poisoned.clone(),
        }),
        Arc::new(SystemClock),
    );
    let (checkpoints, dispatcher) = runtime.dispatcher_store(0)?;
    let failures = AtomicUsize::new(0);
    let mut serving = Box::pin(runtime.run(|actor, error| {
        assert_eq!(actor, &poisoned, "{error}");
        failures.fetch_add(1, Ordering::SeqCst);
    }));
    for round in 0..=ROUNDS {
        if round > 0 {
            let mut input = PreparedPublish::foreign(key(&format!("poisoned/healthy-{round}"))?);
            input.join(CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: StreamPath::new("agents/healthy")?,
                    records: vec![Bytes::from_static(b"emit").into()],
                }],
            ))?;
            input.join(store.activation(&healthy)?)?;
            PreparedPublish::settle(&provider.commit(input.into_request()?).await?)?;
        }
        // At quiescence every healthy entry committed so far is passed.
        let tail = provider.tail(log.clone()).await?;
        let completed = observe_while_running(serving.as_mut(), async {
            Ok::<_, Box<dyn std::error::Error>>(runtime.complete().await)
        })
        .await?;
        assert!(matches!(completed, Err(Error::CompletionFailed(_))));
        let cursor = checkpoints
            .load(&dispatcher)
            .await?
            .state()
            .and_then(|state| state.cursor("activation"));
        assert!(
            cursor >= Some(tail),
            "round {round}: checkpoint {cursor:?} is short of {tail}"
        );
    }
    drop(serving);
    assert!(failures.load(Ordering::SeqCst) > 0, "no delivery failed");
    assert!(records(&*provider, "out/poisoned").await?.is_empty());
    assert_eq!(records(&*provider, "out/healthy").await?.len(), ROUNDS + 1);
    let cursors = records(&*provider, checkpoints.path(&dispatcher)?.as_str())
        .await?
        .iter()
        .map(|record| Ok(State::decode(record)?.cursor("activation")))
        .collect::<Result<Vec<_>, Error>>()?;
    let cursor = cursors.last().copied().flatten().ok_or("no checkpoint")?;
    let passing = cursors.windows(2).filter(|pair| pair[1] > pair[0]).count() + 1;
    let entries = records(&*provider, log.as_str()).await?;
    let (poisoned_entry, healthy_entry) = (poisoned.encode(), healthy.encode());
    let replayed = &entries[usize::try_from(cursor)?..];
    assert!(
        replayed.iter().any(|entry| entry[..] == poisoned_entry[..]),
        "the checkpoint passed every entry of the poisoned actor"
    );
    assert!(
        !replayed.iter().any(|entry| entry[..] == healthy_entry[..]),
        "a restart would replay the healthy actor's entries"
    );
    let carried = entries
        .iter()
        .filter(|entry| entry[..] == poisoned_entry[..])
        .count()
        - 1;
    assert!(
        (1..=passing).contains(&carried),
        "{carried} carried entries over {passing} passing checkpoints"
    );
    // A restarted host without the poison replays the carried entry alone.
    serve_to_checkpoint(&Runtime::new(
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&provider), root()?).with_scheduling_shards(one),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    ))
    .await?;
    assert_eq!(records(&*provider, "out/poisoned").await?.len(), 1);
    assert_eq!(records(&*provider, "out/healthy").await?.len(), ROUNDS + 1);
    Ok(())
}

/// Fails once `actor` has more than `bound` entries in `log` from `from`.
async fn carried_beyond<P: StreamProvider + ?Sized>(
    provider: &P,
    log: StreamPath,
    from: u64,
    actor: &[u8],
    bound: usize,
) -> Box<dyn std::error::Error> {
    use futures::StreamExt as _;
    let mut records = match provider
        .follow(acyclic_stream::FollowRequest::new(log, from))
        .await
    {
        Ok(follow) => acyclic_stream::records_only(follow),
        Err(error) => return error.into(),
    };
    let mut count = 0;
    while let Some(record) = records.next().await {
        match record {
            Ok(record) if record.value[..] == *actor => {
                count += 1;
                if count > bound {
                    return format!("{count} entries carried for a released actor").into();
                }
            }
            Ok(_) => {}
            Err(error) => return error.into(),
        }
    }
    "activation follow ended".into()
}

/// A restarted externally placed host asks its owner to place each dormant
/// actor with due work, and that placement then delivers the waiting input.
///
/// Negative control: an actor already resident is never requested, and the
/// dormant actor delivers nothing until its owner places it.
#[tokio::test]
async fn due_work_on_an_unplaced_actor_requests_its_placement() -> TestResult {
    let one = std::num::NonZeroU16::MIN;
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?).with_scheduling_shards(one);
    let dormant = spawn_with(&store, "dormant", &[b"emit"]).await?;
    let resident = spawn_with(&store, "resident", &[b"emit"]).await?;
    let (sender, mut requested) = tokio::sync::mpsc::unbounded_channel();
    let runtime = Runtime::new(
        Store::new(Arc::clone(&provider), root()?).with_scheduling_shards(one),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    )
    .with_external_placement(Arc::new(move |actor: &ActorReference| {
        sender.send(actor.clone()).ok();
    }));
    runtime.place(resident.clone(), 1).await?;
    let mut serving = Box::pin(runtime.run(|actor, error| panic!("{actor:?}: {error}")));
    let first = observe_while_running(serving.as_mut(), async {
        Ok(requested.recv().await.ok_or("placement requests closed")?)
    })
    .await?;
    assert_eq!(first, dormant);
    observe_while_running(serving.as_mut(), async { Ok(runtime.complete().await?) }).await?;
    assert!(records(&*provider, "out/dormant").await?.is_empty());
    assert_eq!(records(&*provider, "out/resident").await?.len(), 1);
    assert!(!runtime.release(&dormant), "pending placement lost custody");
    // Restart after a checkpoint can advance for the healthy actor, but before
    // the owner has committed the requested placement. No new input arrives.
    drop(serving);
    drop(runtime);
    let (sender, mut requested) = tokio::sync::mpsc::unbounded_channel();
    let replacement = Runtime::new(
        store.clone(),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    )
    .with_external_placement(Arc::new(move |actor: &ActorReference| {
        sender.send(actor.clone()).ok();
    }));
    let mut serving = Box::pin(replacement.run(|actor, error| panic!("{actor:?}: {error}")));
    let restored = observe_while_running(serving.as_mut(), async {
        tokio::select! {
            biased;
            actor = requested.recv() => Ok(actor.ok_or("placement requests closed")?),
            idle = replacement.complete() => Err(format!("pending input was forgotten: {idle:?}").into()),
        }
    })
    .await?;
    assert_eq!(restored, dormant);
    replacement.place(dormant.clone(), 1).await?;
    observe_while_running(serving.as_mut(), async {
        Ok(replacement.complete().await?)
    })
    .await?;
    assert_eq!(records(&*provider, "out/dormant").await?.len(), 1);
    while let Ok(actor) = requested.try_recv() {
        assert_eq!(actor, dormant, "a resident actor was requested");
    }
    Ok(())
}

/// A placement request cannot retire an alarm window before its owner admits
/// the actor. Restarting with no new activation must discover the same alarm.
#[tokio::test]
async fn an_unplaced_alarm_retains_its_window_across_restart() -> TestResult {
    let (clock, mut time) = ControlledClock::new(WINDOW_TEN);
    let clock = Arc::new(clock);
    let (provider, store, actor) =
        armed_before_start(&clock, "pending-alarm", &[b"set 1000"]).await?;
    time.advance_to(WINDOW_TEN + 120_000)?;
    for attempt in 0..2 {
        let (sender, mut requested) = tokio::sync::mpsc::unbounded_channel();
        let runtime = Runtime::new(
            store.clone(),
            Arc::new(Package::new()?),
            Arc::clone(&clock) as Arc<dyn Clock>,
        )
        .with_external_placement(Arc::new(move |actor: &ActorReference| {
            sender.send(actor.clone()).ok();
        }));
        let mut serving = Box::pin(runtime.run(|actor, error| panic!("{actor:?}: {error}")));
        let restored = observe_while_running(serving.as_mut(), async {
            tokio::select! {
                biased;
                actor = requested.recv() => Ok(actor.ok_or("placement requests closed")?),
                idle = runtime.complete() => Err(format!("pending alarm was forgotten: {idle:?}").into()),
            }
        })
        .await?;
        assert_eq!(restored, actor);
        completed(&runtime, serving.as_mut()).await?;
        assert!(records(&*provider, "out/pending-alarm").await?.is_empty());
        if attempt == 1 {
            runtime.place(actor.clone(), 2).await?;
            completed(&runtime, serving.as_mut()).await?;
        }
    }
    assert_eq!(records(&*provider, "out/pending-alarm").await?.len(), 1);
    Ok(())
}

/// An externally placed host releases its failed actor: it carries that
/// actor's wake exactly once, then no more however many checkpoints pass,
/// and the input stays unconsumed for the next placer, which delivers it.
///
/// Reading that handoff entry back asks the owner to place the actor and
/// recreates no slot here.
///
/// Negative controls: keeping the released actor's failure mark carried a
/// fresh entry at every checkpoint, each one's observation forcing the next;
/// taking custody of the read-back handoff recreated an unplaced slot whose
/// failure mark carried the actor again.
#[tokio::test]
async fn a_released_failed_actor_is_carried_once() -> TestResult {
    const ROUNDS: usize = 4;
    let one = std::num::NonZeroU16::MIN;
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?).with_scheduling_shards(one);
    let poisoned = spawn_with(&store, "released", &[b"emit"]).await?;
    let (sender, mut requested) = tokio::sync::mpsc::unbounded_channel();
    let healthy = spawn_with(&store, "resident", &[b"emit"]).await?;
    let log = store.scheduling_path(0)?;
    let runtime = Runtime::new(
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&provider), root()?).with_scheduling_shards(one),
        Arc::new(Poisons {
            package: Package::new()?,
            poisoned: poisoned.clone(),
        }),
        Arc::new(SystemClock),
    )
    .with_external_placement(Arc::new(move |actor: &ActorReference| {
        sender.send(actor.clone()).ok();
    }));
    runtime.place(poisoned.clone(), 1).await?;
    runtime.place(healthy.clone(), 1).await?;
    let mut serving = Box::pin(runtime.run(|actor, error| {
        assert_eq!(actor, &poisoned, "{error}");
    }));
    let poisoned_entry = poisoned.encode();
    let mut from = None;
    for round in 0..=ROUNDS {
        if round > 0 {
            let mut input = PreparedPublish::foreign(key(&format!("released/input-{round}"))?);
            input.join(CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: StreamPath::new("agents/resident")?,
                    records: vec![Bytes::from_static(b"emit").into()],
                }],
            ))?;
            input.join(store.activation(&healthy)?)?;
            PreparedPublish::settle(&provider.commit(input.into_request()?).await?)?;
        }
        let completed = observe_while_running(serving.as_mut(), async {
            match from {
                None => Ok(runtime.complete().await),
                Some(from) => tokio::select! {
                    completed = runtime.complete() => Ok(completed),
                    grown = carried_beyond(&*provider, log.clone(), from, &poisoned_entry, 1) => {
                        Err(grown)
                    }
                },
            }
        })
        .await?;
        assert!(matches!(completed, Err(Error::CompletionFailed(_))));
        if round == 0 {
            assert!(runtime.release(&poisoned), "the failed actor stays hosted");
            from = Some(provider.tail(log.clone()).await?);
        }
    }
    assert!(
        runtime.slot(&poisoned).is_none(),
        "reading the handoff entry back rehosted the released actor"
    );
    drop(serving);
    let mut requests = Vec::new();
    while let Ok(actor) = requested.try_recv() {
        requests.push(actor);
    }
    assert_eq!(
        requests,
        [poisoned.clone()],
        "the owner was not asked once to place the handoff"
    );
    let from = usize::try_from(from.ok_or("no release")?)?;
    let entries = records(&*provider, log.as_str()).await?;
    assert_eq!(
        entries[from..]
            .iter()
            .filter(|entry| entry[..] == poisoned_entry[..])
            .count(),
        1,
        "the released actor's wake was not carried exactly once"
    );
    assert_eq!(records(&*provider, "out/resident").await?.len(), ROUNDS + 1);
    assert_eq!(
        store
            .load(&poisoned)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(0)
    );
    // The next placer delivers the retained input.
    let next = Runtime::new(
        crate::guest::Requests::default(),
        Store::new(Arc::clone(&provider), root()?).with_scheduling_shards(one),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    )
    .externally_placed();
    next.place(poisoned.clone(), 2).await?;
    next.drain_actor(&poisoned).await?;
    assert_eq!(records(&*provider, "out/released").await?.len(), 1);
    Ok(())
}
