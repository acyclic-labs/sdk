use super::*;

#[tokio::test]
async fn local_children_reports_failure_on_an_unrelated_stream()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    provider
        .append(AppendRequest {
            path: StreamPath::new("agents")?,
            records: vec![(Bytes::from_static(b"parent")).into()],
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
        Arc::new(File::open(directory.path().join(JOURNAL_FILE))?);
    assert_eq!(
        provider
            .append(AppendRequest {
                path: StreamPath::new("unrelated")?,
                records: vec![(Bytes::from_static(b"failure")).into()],
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
                            records: vec![(Bytes::from_static(b"accepted")).into()],
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
                #[cfg(feature = "grpc")]
                {
                    resumed_children_session(provider.clone(), &first.path).await?;
                    let relay = crate::grpc::tests::client(provider.clone())?;
                    assert_eq!(
                        relay.watermarks(first.path.clone()).await?,
                        crate::StreamWatermarks {
                            accepted: 1,
                            durable: 0
                        }
                    );
                    resumed_children_session(relay.clone(), &first.path).await?;
                    let second = crate::grpc::tests::client(relay)?;
                    assert_eq!(
                        second.watermarks(first.path.clone()).await?,
                        crate::StreamWatermarks {
                            accepted: 1,
                            durable: 0
                        }
                    );
                    resumed_children_session(second, &first.path).await?;
                }
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

#[cfg(feature = "grpc")]
async fn resumed_children_session<P: StreamProvider>(
    provider: P,
    anchor: &StreamPath,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::wire::{self, session_frame::Body, session_subscription::Kind};
    let (mut commands, input) = futures::channel::mpsc::channel(2);
    let resume = wire::SessionCursor {
        next: 1,
        opaque: anchor.as_str().as_bytes().to_vec().into(),
    };
    for body in [
        Body::Hello(wire::SessionHello {
            credentials: vec![crate::session::testing::credential()],
            version: 1,
            client_id: bytes::Bytes::from_static(b"held-flush-client"),
            destination_node: String::new(),
            receive_bytes: crate::MAX_COMMAND_BYTES as u64,
            receive_frames: u32::try_from(crate::MAX_ITEMS)?,
        }),
        Body::Interest(wire::SessionInterest {
            revision: 1,
            upsert: vec![wire::SessionSubscription {
                credential: 0,
                filter: None,
                channel: Some(wire::SessionChannel {
                    channel_id: 1,
                    generation: 1,
                }),
                resume: Some(resume.clone()),
                kind: Some(Kind::Children(wire::ChildrenRequest {
                    parent: Some("agents".into()),
                    limit: 1,
                })),
            }],
            remove: Vec::new(),
        }),
    ] {
        commands.try_send(Ok(wire::SessionFrame {
            channel_id: 0,
            generation: 0,
            body: Some(body),
        }))?;
    }
    // A fresh session resumes the accepted cursor while its flush is still held.
    let mut output = crate::session::serve(
        Arc::new(provider),
        crate::session::testing::authority(),
        "local".into(),
        crate::session::Limits::default(),
        input.boxed(),
    )?;
    while let Some(frame) = output.next().await {
        let frame = frame?;
        match frame.body {
            Some(Body::Opened(opened)) => {
                assert_eq!(opened.resume, Some(resume));
                return Ok(());
            }
            Some(Body::Closed(_)) => {
                return Err("resumed child channel closed before Opened".into());
            }
            _ => {}
        }
    }
    Err("resumed child session ended before Opened".into())
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
                records: vec![(Bytes::from_static(b"status")).into()],
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
