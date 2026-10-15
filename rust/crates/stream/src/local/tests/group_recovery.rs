use super::*;

#[test]
fn a_later_writer_makes_the_preceding_frame_durable_before_writing()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_PERSIST_BLOCKER
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((
            Arc::as_ptr(&provider.inner.journal) as usize,
            started_tx,
            release_rx,
        ));
        let commit = |path: &'static str| {
            let provider = provider.clone();
            tokio::spawn(async move { provider.commit(keyed_commit(path, 0)?).await })
        };
        let first = commit("first");
        tokio::task::spawn_blocking(move || started_rx.recv()).await??;
        let second = commit("second");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if provider
                    .inner
                    .journal
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .appended
                    == 2
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        // This also checks that synchronous progress wakes watch subscribers;
        // merely updating Journal::synced would leave this waiter stuck.
        tokio::time::timeout(std::time::Duration::from_secs(1), provider.await_durable(1))
            .await??;
        assert_eq!(
            provider
                .inner
                .journal
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .synced,
            1
        );
        release_tx.send(())?;
        first.await??;
        second.await??;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

/// Model power loss by discarding the earliest unsynchronized frame while
/// retaining later writes. Recovery must still find a repairable torn tail.
#[tokio::test]
async fn reordered_unsynced_writes_recover_without_manual_repair()
-> Result<(), Box<dyn std::error::Error>> {
    for fill in [0_u8, 0x5a] {
        let directory = tempfile::tempdir()?;
        let crashed = tempfile::tempdir()?;
        let limits = LocalStreamLimits::default();
        let provider = LocalStream::open(directory.path(), limits).await?;
        let journal_path = directory.path().join(JOURNAL_FILE);
        let mut ends = vec![u64::try_from(HEADER_BYTES)?];
        for path in ["first", "second", "third"] {
            deferring_durability(append_one(&provider, path, &journal_path)).await?;
            ends.push(std::fs::metadata(&journal_path)?.len());
        }
        let synced = usize::try_from(
            provider
                .inner
                .journal
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .synced,
        )?;
        let mut image = std::fs::read(&journal_path)?;
        let frame_start = *ends.get(synced).ok_or("missing unsynced frame")?;
        let start = usize::try_from(frame_start)?;
        let end = usize::try_from(*ends.get(synced + 1).ok_or("missing unsynced frame end")?)?;
        image
            .get_mut(start..end)
            .ok_or("invalid frame range")?
            .fill(fill);
        std::fs::write(crashed.path().join(JOURNAL_FILE), image)?;

        let recovered = LocalStream::open(crashed.path(), limits).await?;
        assert_eq!(recovered.tail(StreamPath::new("first")?).await?, 1);
        assert_eq!(recovered.tail(StreamPath::new("second")?).await?, 1);
        assert_eq!(
            recovered.tail(StreamPath::new("third")?).await,
            Err(StreamError::NotFound)
        );
        assert_eq!(
            std::fs::metadata(crashed.path().join(JOURNAL_FILE))?.len(),
            frame_start
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_ordering_barrier_uses_the_prepared_durability_budget()
-> Result<(), Box<dyn std::error::Error>> {
    for (operations, expected_syncs) in [(1_000_000, 2), (4, 4)] {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits {
            journal_operations: operations,
            ..LocalStreamLimits::default()
        };
        let provider = LocalStream::open(directory.path(), limits).await?;
        deferring_durability(provider.append(keyed_append("seed", 0)?)).await?;
        let meter = Arc::new(Meter::local());
        let outcome = provider
            .native_append(keyed_append("native", 0)?, meter.original())
            .await??;
        assert!(matches!(outcome.value(), AppendOutcome::Committed(_)));
        assert_eq!(
            meter.durability_operations.load(Ordering::SeqCst),
            expected_syncs
        );
        drop(outcome);
        drop(provider);
        meter.assert_refunded();
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.tail(StreamPath::new("seed")?).await?, 1);
        assert_eq!(reopened.tail(StreamPath::new("native")?).await?, 1);
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_at_the_ordering_barrier_finishes_the_accepted_write()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let limits = LocalStreamLimits::default();
    let provider = LocalStream::open(directory.path(), limits).await?;
    deferring_durability(provider.append(keyed_append("seed", 0)?)).await?;
    let meter = Arc::new(Meter::local());
    meter.cancel_on_durability.store(true, Ordering::SeqCst);
    let request = keyed_append("cancelled", 0)?;
    let error = provider
        .native_append(request.clone(), meter.original())
        .await
        .err()
        .ok_or("late cancellation acknowledged the write")?;
    assert_original_denial(&error, "cancelled");
    assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 2);
    drop(provider);
    meter.assert_refunded();
    let reopened = LocalStream::open(directory.path(), limits).await?;
    assert_eq!(reopened.tail(request.path.clone()).await?, 1);
    assert!(matches!(
        reopened.append(request).await?,
        AppendOutcome::Committed(_)
    ));
    Ok(())
}
