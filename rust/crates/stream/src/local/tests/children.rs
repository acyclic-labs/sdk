use super::*;

#[tokio::test]
async fn local_children_reports_failure_on_an_unrelated_stream()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    provider
        .append(AppendRequest {
            path: StreamPath::new("agents")?,
            records: vec![Bytes::from_static(b"parent")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let mut children = provider
        .children(ChildrenRequest {
            parent: Some(StreamPath::new("agents")?),
            limit: 1,
        })
        .await?;
    assert!(futures::poll!(children.next()).is_pending());
    // Force a real journal write error after the unrelated memory mutation.
    provider.inner.journal.lock().map_err(|_| "journal")?.file =
        File::open(directory.path().join(JOURNAL_FILE))?;
    assert_eq!(
        provider
            .append(AppendRequest {
                path: StreamPath::new("unrelated")?,
                records: vec![Bytes::from_static(b"failure")],
                if_tail: None,
                idempotency_key: None,
            })
            .await,
        Err(StreamError::Unavailable)
    );
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(1), children.next()).await?,
        Some(Err(StreamError::Unavailable))
    ));
    assert!(children.next().await.is_none());
    Ok(())
}

#[test]
fn local_children_observes_acceptance_during_a_device_flush()
-> Result<(), Box<dyn std::error::Error>> {
    let _serial = PERSIST_BLOCKER_TESTS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(async {
            let directory = tempfile::tempdir()?;
            let provider =
                LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
            let mut children = provider
                .children(ChildrenRequest {
                    parent: Some(StreamPath::new("agents")?),
                    limit: 1,
                })
                .await?;
            assert!(futures::poll!(children.next()).is_pending());
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            *JOURNAL_PERSIST_BLOCKER.lock().map_err(|_| "persist hook")? = Some((
                Arc::as_ptr(&provider.inner.journal) as usize,
                started_tx,
                release_rx,
            ));
            let writing = tokio::spawn({
                let provider = provider.clone();
                async move {
                    provider
                        .append(AppendRequest {
                            path: StreamPath::new("agents/new")?,
                            records: vec![Bytes::from_static(b"accepted")],
                            if_tail: None,
                            idempotency_key: None,
                        })
                        .await
                }
            });
            tokio::task::spawn_blocking(move || {
                started_rx.recv_timeout(std::time::Duration::from_secs(1))
            })
            .await??;
            // Always release the device gate, including when the regression fails.
            let observed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                let first = children.next().await.ok_or("children ended")??;
                let mut later = provider
                    .children(ChildrenRequest {
                        parent: Some(StreamPath::new("agents")?),
                        limit: 1,
                    })
                    .await?;
                let replay = later.next().await.ok_or("replay ended")??;
                assert_eq!(first.path.as_str(), "agents/new");
                assert_eq!(first.path, replay.path);
                assert!(!writing.is_finished());
                assert_eq!(provider.watermarks(first.path).await?.durable, 0);
                Ok::<_, Box<dyn std::error::Error>>(())
            })
            .await;
            release_tx.send(())?;
            writing.await??;
            observed??;
            Ok::<_, Box<dyn std::error::Error>>(())
        })
}

#[tokio::test]
async fn local_children_releases_visibility_while_waiting() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let mut children = provider
        .children(ChildrenRequest {
            parent: Some(StreamPath::new("agents")?),
            limit: 1,
        })
        .await?;
    for name in ["first", "second"] {
        assert!(futures::poll!(children.next()).is_pending());
        let path = StreamPath::new(format!("agents/{name}"))?;
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![Bytes::from_static(b"status")],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let child = tokio::time::timeout(std::time::Duration::from_secs(1), children.next())
            .await?
            .ok_or("children ended")??;
        assert_eq!(child.path, path);
    }
    drop(children);
    drop(provider);
    let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    assert_eq!(
        reopened
            .children_page(ChildrenPageRequest {
                parent: Some(StreamPath::new("agents")?),
                limit: 1,
                after: None,
                hierarchy_version: None,
            })
            .await?
            .children
            .len(),
        1
    );
    Ok(())
}
