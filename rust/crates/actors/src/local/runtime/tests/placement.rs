use super::*;

/// Seeing the same namespace and durable input never authorizes a second host
/// to claim. Only explicit, per-actor placement commands change the fence.
#[tokio::test]
async fn placement_commands_fence_same_namespace_nodes_without_self_claim() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let first = spawn_with(&store, "placed-first", &[b"emit"]).await?;
    let second = spawn_with(&store, "placed-second", &[b"emit"]).await?;
    let owner = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    let other = Runtime::new(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(Package::new()?),
        Arc::new(SystemClock),
    );
    other.track(first.clone())?;
    assert!(matches!(
        other.drain_actor(&first).await,
        Err(Error::Conflict)
    ));
    assert_eq!(store.load(&first).await?.state().map(State::epoch), Some(0));

    owner.place(first.clone(), 1).await?;
    other.place(second.clone(), 1).await?;
    assert!(matches!(
        other.place(first.clone(), 1).await,
        Err(Error::Conflict)
    ));
    owner.drain_actor(&first).await?;
    other.drain_actor(&second).await?;
    owner.place(first.clone(), 1).await?;
    assert_eq!(store.load(&first).await?.state().map(State::epoch), Some(1));
    assert_eq!(
        store
            .load(&first)
            .await?
            .state()
            .and_then(|s| s.cursor("inbox")),
        Some(1)
    );

    other.place(first.clone(), 2).await?;
    assert_eq!(store.load(&first).await?.state().map(State::epoch), Some(2));
    assert!(matches!(
        owner.place(first.clone(), 1).await,
        Err(Error::Conflict)
    ));
    owner.track(first.clone())?;
    assert!(matches!(
        owner.drain_actor(&first).await,
        Err(Error::Conflict)
    ));
    other.drain_actor(&first).await?;
    assert_eq!(
        records(provider.as_ref(), "out/placed-first").await?.len(),
        1
    );
    assert_eq!(
        records(provider.as_ref(), "out/placed-second").await?.len(),
        1
    );
    Ok(())
}
