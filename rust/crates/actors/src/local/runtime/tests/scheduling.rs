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
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    )
    .with_scheduling_shards([shard]);
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
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    )
    .with_scheduling_shards([shard]);
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

/// A resumed alarm whose window passed while it was paused fires after a
/// restart that follows the resume commit, before any host delivered it.
#[tokio::test]
async fn a_resumed_alarm_survives_a_restart_past_its_window() -> TestResult {
    let (clock, mut time) = ControlledClock::new(WINDOW_TEN);
    let clock = Arc::new(clock);
    let (provider, store, actor) = armed_before_start(&clock, "resumed", &[b"set 1000"]).await?;
    {
        let host = Runtime::new(
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
        time.advance_to(WINDOW_TEN + 120_000)?;
        // The host checkpoints past the paused alarm's window, then stops.
        observe_while_running(
            serving.as_mut(),
            checkpointed(
                &host,
                "deadline",
                crate::local::scheduling::window(WINDOW_TEN) + 1,
            ),
        )
        .await?;
    }
    let snapshot = store.load(&actor).await?;
    let epoch = snapshot.state().ok_or("missing actor state")?.epoch();
    store
        .set_paused(&snapshot, epoch, false, key("resumed/resume")?)
        .await?;
    let host = Runtime::new(store, Arc::new(Package::new()?), clock);
    let mut serving = Box::pin(host.run(|_, error| panic!("delivery failed: {error}")));
    completed(&host, serving.as_mut()).await?;
    assert_eq!(
        records(provider.as_ref(), "out/resumed").await?,
        vec![Bytes::from_static(b"a1")],
        "the restarted host lost the resumed alarm"
    );
    Ok(())
}

/// Deleting a resident actor with an armed alarm is finished work for its
/// running host: completion keeps working for every other actor.
#[tokio::test]
async fn deleting_a_resident_actor_keeps_its_host_completing() -> TestResult {
    let (clock, mut time) = ControlledClock::new(WINDOW_TEN);
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let runtime = Runtime::new(store.clone(), Arc::new(Package::new()?), Arc::new(clock));
    let mut serving = Box::pin(runtime.run(|_, error| panic!("delivery failed: {error}")));
    let doomed = spawn_with(&store, "doomed", &[b"set 60000"]).await?;
    completed(&runtime, serving.as_mut()).await?;
    let snapshot = store.load(&doomed).await?;
    store
        .submit(store.delete(&snapshot, key("doomed/delete")?).await?)
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
