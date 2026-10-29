use super::*;

#[test]
fn a_pending_write_releases_metadata_and_cannot_borrow_an_earlier_flush()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        for lose_completion in [false, true] {
            let root = tempfile::tempdir()?;
            let provider = LocalStream::open(root.path(), Default::default()).await?;
            let first = keyed_commit("write-cut", 0)?;
            deferring_durability(provider.commit(first)).await?;
            let before = provider.inner.journal.lock().unwrap().bytes;
            let meter = Arc::new(Meter::local());
            let work = crate::native::Work::new(meter.original())?;
            let pending = PreparedFrame::encode(
                &WriteRequest::Commit(keyed_commit("write-cut", 1)?),
                SystemUnixMillisClock.now_unix_millis(),
                Some(work.clone()),
            )?;
            let bytes = pending.encoded.len() as u64;
            let (started, entered) = tokio::sync::oneshot::channel();
            let (release, released) = std::sync::mpsc::sync_channel(0);
            *JOURNAL_WRITE_BLOCKER.lock().unwrap() = Some((
                Arc::as_ptr(&provider.inner.journal) as usize,
                started,
                released,
            ));
            let visibility = provider.inner.visibility.write().await;
            let publication = provider.inner.publication.write().await;
            let writing = tokio::spawn({
                let provider = provider.clone();
                async move { provider.persist(pending).await }
            });
            // The physical worker produces this event; dropping either side
            // closes the wait instead of requiring a readiness probe.
            entered.await?;
            let completed = {
                let journal = provider
                    .inner
                    .journal
                    .try_lock()
                    .map_err(|_| "physical write retained journal metadata")?;
                assert_eq!((journal.appended, journal.bytes), (1, before));
                journal.appended
            };
            provider.await_publication(completed).await?;
            assert_eq!(
                provider.inner.journal.lock().unwrap().flushed_sequence,
                Some(1)
            );
            assert!(!writing.is_finished());
            assert_eq!(
                meter
                    .write_operations
                    .load(std::sync::atomic::Ordering::SeqCst),
                0
            );
            if lose_completion {
                drop(release);
                assert!(matches!(writing.await?, Err(StreamError::Unavailable)));
                assert!(matches!(
                    provider.check_available(),
                    Err(StreamError::Unavailable)
                ));
                assert_eq!(
                    std::fs::metadata(root.path().join(JOURNAL_FILE))?.len(),
                    before
                );
            } else {
                release.send(())?;
                writing.await??;
                let journal = provider.inner.journal.lock().unwrap();
                assert_eq!((journal.appended, journal.bytes), (2, before + bytes));
                assert_eq!(journal.flushed_sequence, Some(1));
                drop(journal);
                provider.await_publication(2).await?;
                assert_eq!(
                    meter.write_bytes.load(std::sync::atomic::Ordering::SeqCst),
                    bytes
                );
            }
            drop(publication);
            drop(visibility);
            drop(work);
            meter.assert_refunded();
            drop(provider);
            let reopened = LocalStream::open(root.path(), Default::default()).await?;
            assert_eq!(
                reopened.tail(StreamPath::new("write-cut")?).await?,
                if lose_completion { 1 } else { 2 }
            );
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

#[tokio::test]
async fn negative_control_physical_write_under_metadata_prevents_cut_observation()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let provider = LocalStream::open(root.path(), Default::default()).await?;
    let mut frame = provider.prepare(&WriteRequest::Commit(keyed_commit("locked-cut", 0)?))?;
    // Execute the former physical path on a real journal. Its metadata cannot
    // be observed until the file operation and publication have both ended.
    {
        let mut journal = provider.inner.journal.lock().unwrap();
        let (file, offset) = journal.begin_append(true)?;
        file.write(|file| {
            assert!(provider.inner.journal.try_lock().is_err());
            Ok(frame.write(&mut acyclic_native_runtime::WriteAt::new(file, offset)))
        })??;
        journal.complete_append(frame.encoded.len() as u64, true);
    }
    provider.await_publication(1).await?;
    drop(provider);
    let reopened = LocalStream::open(root.path(), Default::default()).await?;
    assert_eq!(reopened.tail(StreamPath::new("locked-cut")?).await?, 1);
    Ok(())
}
