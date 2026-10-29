use super::*;
use futures::TryFutureExt as _;

#[tokio::test]
async fn immutable_commit_reads_bypass_pending_publication_but_absence_does_not()
-> Result<(), Box<dyn std::error::Error>> {
    use futures::FutureExt as _;
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let outcome = provider.commit(keyed_commit("retained", 0)?).await?;
    let CommitOutcome::Committed(receipt) = outcome else {
        return Err("seed commit refused".into());
    };
    let held = provider.inner.publication.write().await;
    let observed = provider.read_commit(receipt.commit_id).now_or_never();
    assert!(matches!(observed, Some(Ok(ref envelope)) if envelope.commit_id == receipt.commit_id));
    // The former path takes the unrelated publication lock even for a hit.
    assert!(
        provider
            .accepted(provider.inner.provider.read_commit(receipt.commit_id))
            .now_or_never()
            .is_none()
    );
    let mut missing = std::pin::pin!(provider.read_commit(crate::CommitId::from_bytes([0; 32])));
    assert!(futures::poll!(missing.as_mut()).is_pending());
    drop(held);
    assert!(matches!(missing.await, Err(StreamError::NotFound)));
    provider.inner.poison();
    assert!(matches!(
        provider.read_commit(receipt.commit_id).await,
        Err(StreamError::Unavailable)
    ));
    Ok(())
}

#[test]
fn a_flush_never_certifies_a_write_accepted_after_its_device_operation()
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
        deferring_durability(provider.commit(keyed_commit("before-device", 0)?)).await?;
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_FLUSHED_BLOCKER.lock().unwrap() = Some((
            Arc::as_ptr(&provider.inner.journal) as usize,
            started_tx,
            release_rx,
        ));
        let flushing = tokio::spawn({
            let provider = provider.clone();
            async move { provider.await_flushed(1).await }
        });
        tokio::task::spawn_blocking(move || started_rx.recv()).await??;
        deferring_durability(provider.commit(keyed_commit("after-device", 0)?)).await?;
        let late_cut = provider.inner.journal.lock().unwrap().appended;
        assert_eq!(late_cut, 2);
        release_tx.send(())?;
        flushing.await??;
        assert_eq!(
            provider.inner.journal.lock().unwrap().flushed_sequence,
            Some(1),
            "a post-I/O cut certified an unflushed write"
        );
        assert_eq!(
            provider
                .watermarks(StreamPath::new("after-device")?)
                .await?
                .durable,
            0
        );
        // The post-I/O-capture negative control would incorrectly certify it.
        assert!(
            SyncProgress {
                ordered: late_cut,
                flushed: Some(late_cut)
            }
            .completion(2, LocalDurability::FullFlush)
            .is_some()
        );
        provider.await_flushed(2).await?;
        assert_eq!(
            provider
                .watermarks(StreamPath::new("after-device")?)
                .await?
                .durable,
            1
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[test]
fn a_queued_flush_covers_the_worker_cut_instead_of_the_submitter_cut()
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
        deferring_durability(provider.commit(keyed_commit("first-cut", 0)?)).await?;
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *JOURNAL_PERSIST_BLOCKER.lock().unwrap() = Some((
            Arc::as_ptr(&provider.inner.journal) as usize,
            started_tx,
            release_rx,
        ));
        let flushing = tokio::spawn({
            let provider = provider.clone();
            async move { provider.await_flushed(1).await }
        });
        tokio::task::spawn_blocking(move || started_rx.recv()).await??;
        deferring_durability(provider.commit(keyed_commit("queued-cut", 0)?)).await?;
        assert_eq!(provider.inner.journal.lock().unwrap().appended, 2);
        assert!(!flushing.is_finished());
        release_tx.send(())?;
        flushing.await??;
        assert_eq!(
            provider.inner.journal.lock().unwrap().flushed_sequence,
            Some(2),
            "a write queued before physical I/O required a second full flush"
        );

        // Execute the former capture/I/O/publication path on a second real
        // journal. Both writes precede I/O, but the stale submitter cut forces
        // the second waiter to perform another full device operation.
        let old_directory = tempfile::tempdir()?;
        let old = LocalStream::open(old_directory.path(), LocalStreamLimits::default()).await?;
        deferring_durability(old.commit(keyed_commit("old-first", 0)?)).await?;
        let (old_target, file) = {
            let journal = old.inner.journal.lock().unwrap();
            (journal.appended, journal.file.lease()?)
        };
        deferring_durability(old.commit(keyed_commit("old-queued", 0)?)).await?;
        let old_completion = sync_file_data(file.file(), LocalDurability::FullFlush)?;
        {
            let mut journal = old.inner.journal.lock().unwrap();
            old.inner
                .publish_synced(&mut journal, old_target, old_completion);
        }
        assert_eq!(
            old.watermarks(StreamPath::new("old-queued")?)
                .await?
                .durable,
            0
        );
        old.await_flushed(2).await?;
        assert_eq!(
            old.watermarks(StreamPath::new("old-queued")?)
                .await?
                .durable,
            1
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[test]
fn follow_and_heads_observe_acceptance_before_the_durability_barrier()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir_in(std::env::current_dir()?)?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let path = StreamPath::new("accepted-events")?;
        for n in 0..2 {
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            *JOURNAL_PERSIST_BLOCKER
                .lock()
                .map_err(|_| "poisoned persist hook")? = Some((
                Arc::as_ptr(&provider.inner.journal) as usize,
                started_tx,
                release_rx,
            ));
            let appending = tokio::spawn({
                let (provider, path) = (provider.clone(), path.clone());
                async move {
                    provider
                        .append(AppendRequest {
                            path,
                            records: vec![(Bytes::from_static(b"delta")).into()],
                            if_tail: Some(n),
                            idempotency_key: crate::IdempotencyKey::unique(),
                        })
                        .await
                }
            });
            tokio::task::spawn_blocking(move || {
                started_rx.recv_timeout(std::time::Duration::from_secs(1))
            })
            .await??;
            let mut follow = provider
                .follow(crate::FollowRequest::new(path.clone(), n))
                .map_ok(crate::records_only)
                .await?;
            let record = tokio::time::timeout(std::time::Duration::from_secs(1), follow.next())
                .await?
                .ok_or("follow ended")??;
            assert_eq!(record.sequence, n);
            assert_eq!(record.value.as_slice(), b"delta");
            let checked = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                provider
                    .follow(crate::FollowRequest {
                        when_absent: Default::default(),
                        path: path.clone(),
                        from: n + 1,
                        preceding: Some(record.commit_id),
                        filter: crate::FollowFilter::default(),
                        progress: false,
                        at_least: Default::default(),
                    })
                    .map_ok(crate::records_only),
            )
            .await??;
            drop(checked);
            let lost = provider
                .follow(crate::FollowRequest {
                    when_absent: Default::default(),
                    path: path.clone(),
                    from: n + 2,
                    preceding: Some(record.commit_id),
                    filter: crate::FollowFilter::default(),
                    progress: false,
                    at_least: Default::default(),
                })
                .map_ok(crate::records_only)
                .await;
            assert!(matches!(lost, Err(StreamError::OutOfRange)));
            assert_eq!(
                provider.watermarks(path.clone()).await?,
                crate::StreamWatermarks {
                    accepted: n + 1,
                    durable: n
                }
            );
            let mut head = provider
                .read(ReadRequest {
                    key: None,
                    path: path.clone(),
                    from: n,
                    limit: 1,
                    at_least: Default::default(),
                })
                .await?;
            assert_eq!(head.next().await.ok_or("missing accepted head")??, record);
            assert!(
                !appending.is_finished(),
                "durability acknowledgement completed before the barrier"
            );
            release_tx.send(())?;
            assert!(matches!(appending.await??, AppendOutcome::Committed(_)));
            assert_eq!(
                provider.watermarks(path.clone()).await?,
                crate::StreamWatermarks {
                    accepted: n + 1,
                    durable: n + 1
                }
            );
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}
