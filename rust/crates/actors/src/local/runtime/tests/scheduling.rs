use super::*;
use crate::local::scheduling_shard;

/// Shard routing and checkpoint custody survive node replacement without a
/// root broadcast, rewriting producer keys or sharing checkpoint state.
#[tokio::test]
async fn shard_assignment_isolates_wakes_and_checkpoint_progress() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), root()?);
    let first = spawn_with(&store, "scheduled-first", &[b"emit"]).await?;
    let shard = scheduling_shard(&first);
    let mut candidate = 0_u64;
    let second = loop {
        let address = actor(&format!("scheduled-other-{candidate}"))?;
        if scheduling_shard(&address) != shard {
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
    let other_shard = scheduling_shard(&second);
    let original_path = store.activation_record(&first)?.0;
    assert_ne!(original_path, store.activation_record(&second)?.0);
    assert!(matches!(
        provider.tail(root()?).await,
        Err(StreamError::NotFound)
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
        Err(Error::Invalid("invalid actor placement"))
    ));
    let (checkpoint, address) = host.dispatcher_store(shard)?;
    let initial = host.dispatcher(&checkpoint, &address, shard).await?;
    let tail = provider.tail(original_path.clone()).await?;
    host.advance_dispatcher(&checkpoint, initial, tail).await?;
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
    let restored = replacement.dispatcher(&checkpoint, &address, shard).await?;
    assert_eq!(
        restored
            .state()
            .and_then(|state| state.cursor("activation")),
        Some(tail)
    );
    let (other_checkpoint, other_address) = replacement.dispatcher_store(other_shard)?;
    let other = replacement
        .dispatcher(&other_checkpoint, &other_address, other_shard)
        .await?;
    assert_eq!(
        other.state().and_then(|state| state.cursor("activation")),
        Some(0)
    );
    assert_ne!(
        checkpoint.path(&address)?,
        other_checkpoint.path(&other_address)?
    );
    assert_eq!(provider.tail(store.scheduling_path(other_shard)?).await?, 1);
    Ok(())
}
