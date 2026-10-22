use super::*;

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
                            idempotency_key: None,
                        })
                        .await
                }
            });
            tokio::task::spawn_blocking(move || {
                started_rx.recv_timeout(std::time::Duration::from_secs(1))
            })
            .await??;
            let mut follow = provider.follow(path.clone(), n).await?;
            let record = tokio::time::timeout(std::time::Duration::from_secs(1), follow.next())
                .await?
                .ok_or("follow ended")??;
            assert_eq!(record.sequence, n);
            assert_eq!(record.value.as_slice(), b"delta");
            let checked = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                provider.follow_checked(path.clone(), n + 1, record.commit_id),
            )
            .await??;
            drop(checked);
            let lost = provider
                .follow_checked(path.clone(), n + 2, record.commit_id)
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
                    path: path.clone(),
                    from: n,
                    limit: 1,
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
