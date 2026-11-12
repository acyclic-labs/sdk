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
    host.advance_dispatcher(&checkpoint, initial, shard, tail, 0)
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

/// The checkpoint of the store's only shard, once it passes `tail`.
async fn checkpointed<P: StreamProvider + ?Sized>(
    runtime: &Runtime<P>,
    tail: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    use futures::StreamExt as _;
    let (checkpoints, dispatcher) = runtime.dispatcher_store(0)?;
    let mut follow = acyclic_stream::FollowRequest::new(checkpoints.path(&dispatcher)?, 0);
    follow.when_absent = acyclic_stream::WhenAbsent::Wait;
    let mut records =
        acyclic_stream::records_only(runtime.store().provider().follow(follow).await?);
    while let Some(record) = records.next().await {
        if State::decode(&record?.value)?.cursor("activation") >= Some(tail) {
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
        result = checkpointed(runtime, tail) => result,
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
