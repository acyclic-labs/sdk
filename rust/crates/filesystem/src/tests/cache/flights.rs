use super::*;

#[tokio::test]
async fn missing_scalar_and_batch_reads_release_every_flight_for_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (present_id, present_bytes) = object(b"present");
    (object_work!(
        inner,
        put(present_id, present_bytes.clone()),
        B,
        &cancellation
    ))
    .await?;
    let (missing_id, _) = object(b"absent");
    let cached = CachedObjectStore::new(inner, options(4, 64))?;

    for _ in 0..2 {
        assert!(
            (object_work!(cached, read(missing_id, 6), B, &cancellation))
                .await
                .is_err()
        );
        assert_eq!(cached.stats()?.in_flight, 0);
    }
    let requests = [
        ObjectReadRequest {
            object_id: present_id,
            maximum_bytes: 7,
        },
        ObjectReadRequest {
            object_id: missing_id,
            maximum_bytes: 6,
        },
    ];
    assert!(
        (object_work!(cached, read_many(&requests), B, &cancellation))
            .await
            .is_err()
    );
    assert_eq!(cached.stats()?.in_flight, 0);
    let retry = object_work!(cached, read(present_id, 7), B, &cancellation).await?;
    assert_eq!(retry.value.bytes, present_bytes);

    let empty = (object_work!(cached, read_many(&[]), B, &cancellation))
        .await
        .err()
        .ok_or("empty cache batch succeeded")?;
    assert!(matches!(empty.error, ObjectStoreError::Rejected(_)));
    assert_eq!(*empty.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn concurrent_reads_share_one_original_backend_request()
-> Result<(), Box<dyn std::error::Error>> {
    for capacity in [64, 1] {
        let inner = MemoryObjectStore::default();
        let cancellation = CancellationToken::new();
        let (object_id, bytes) = object(b"single-flight");
        (object_work!(inner, put(object_id, bytes.clone()), B, &cancellation)).await?;
        let (release, gate) = futures::channel::oneshot::channel();
        let gate = futures::FutureExt::shared(gate);
        let reads = Arc::new(AtomicUsize::new(0));
        let cached = CachedObjectStore::new(
            GatedStore {
                inner: Arc::new(inner),
                gate: gate.clone(),
                reads: Arc::clone(&reads),
            },
            options(4, capacity),
        )?;
        let (first, second) = async {
            let first = object_work!(cached, read(object_id, 13), B, &cancellation);
            let second = object_work!(cached, read(object_id, 13), B, &cancellation);
            tokio::pin!(first, second);
            assert!(futures::poll!(first.as_mut()).is_pending());
            assert!(futures::poll!(second.as_mut()).is_pending());
            assert_eq!(reads.load(Ordering::Acquire), 1);
            release
                .send(())
                .map_err(|_| "original read receiver closed")?;
            Ok::<_, Box<dyn std::error::Error>>(tokio::join!(first, second))
        }
        .await?;
        let first = first?;
        let second = second?;
        assert_eq!(first.value.bytes, bytes);
        assert_eq!(second.value.bytes, bytes);
        assert_eq!(reads.load(Ordering::Acquire), 1);

        assert_eq!(cached.stats()?.coalesced_reads, 1);
        assert_eq!(cached.stats()?.resident_entries, u64::from(capacity >= 13));
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_scalar_and_batch_reads_share_the_original_success_or_failure()
-> Result<(), Box<dyn std::error::Error>> {
    for scalar_leads in [true, false] {
        for (object_id, body) in [object(b"scalar-batch"), object(b"missing-batch")]
            .into_iter()
            .enumerate()
        {
            let inner = MemoryObjectStore::default();
            let cancellation = CancellationToken::new();
            let (identity, bytes) = body;
            if object_id == 0 {
                (object_work!(inner, put(identity, bytes.clone()), B, &cancellation)).await?;
            }
            let (release, gate) = futures::channel::oneshot::channel();
            let gate = futures::FutureExt::shared(gate);
            let reads = Arc::new(AtomicUsize::new(0));
            let cached = CachedObjectStore::new(
                GatedStore {
                    inner: Arc::new(inner),
                    gate: gate.clone(),
                    reads: Arc::clone(&reads),
                },
                options(4, 64),
            )?;
            let requests = [ObjectReadRequest {
                object_id: identity,
                maximum_bytes: u64::try_from(bytes.len())?,
            }];
            let (scalar, batch) = async {
                let scalar = object_work!(
                    cached,
                    read(identity, u64::try_from(bytes.len()).unwrap_or(u64::MAX)),
                    B,
                    &cancellation
                );
                let batch = object_work!(cached, read_many(&requests), B, &cancellation);
                tokio::pin!(scalar, batch);
                if scalar_leads {
                    assert!(futures::poll!(scalar.as_mut()).is_pending());
                    assert!(futures::poll!(batch.as_mut()).is_pending());
                } else {
                    assert!(futures::poll!(batch.as_mut()).is_pending());
                    assert!(futures::poll!(scalar.as_mut()).is_pending());
                }
                assert_eq!(reads.load(Ordering::Acquire), 1);
                release
                    .send(())
                    .map_err(|_| "original read receiver closed")?;
                Ok::<_, Box<dyn std::error::Error>>(tokio::join!(scalar, batch))
            }
            .await?;
            if object_id == 0 {
                assert_eq!(scalar?.value.bytes, bytes);
                assert_eq!(batch?.value[0].bytes, bytes);
                assert_eq!(reads.load(Ordering::Acquire), 1);
            } else {
                let scalar = scalar.err().ok_or("missing scalar succeeded")?.error;
                let batch = batch.err().ok_or("missing batch succeeded")?.error;
                let (leader, follower) = if scalar_leads {
                    (scalar, batch)
                } else {
                    (batch, scalar)
                };
                assert!(matches!(leader, ObjectStoreError::Missing));
                assert!(matches!(follower, ObjectStoreError::SharedReadUnavailable));
                assert_eq!(reads.load(Ordering::Acquire), 1);
            }
            assert_eq!(cached.stats()?.in_flight, 0);
            assert_eq!(cached.stats()?.coalesced_reads, 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn dropped_leaders_release_flights_and_retry_without_a_stale_identity()
-> Result<(), Box<dyn std::error::Error>> {
    struct Woken(AtomicUsize);
    impl std::task::Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Release);
        }
    }
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"drop-safe");
    (object_work!(inner, put(object_id, bytes.clone()), B, &cancellation)).await?;
    let (release, gate) = futures::channel::oneshot::channel();
    let gate = futures::FutureExt::shared(gate);
    let reads = Arc::new(AtomicUsize::new(0));
    let cached = CachedObjectStore::new(
        GatedStore {
            inner: Arc::new(inner),
            gate: gate.clone(),
            reads: Arc::clone(&reads),
        },
        options(4, 64),
    )?;
    async {
        let follower = object_work!(cached, read(object_id, 9), B, &cancellation);
        tokio::pin!(follower);
        let woken = Arc::new(Woken(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&woken));
        {
            let read = object_work!(cached, read(object_id, 9), B, &cancellation);
            tokio::pin!(read);
            assert!(futures::poll!(read.as_mut()).is_pending());
            assert!(
                follower
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            assert_eq!(woken.0.load(Ordering::Acquire), 0);
        }
        assert!(woken.0.load(Ordering::Acquire) > 0);
        let Poll::Ready(Err(failed)) = follower.as_mut().poll(&mut Context::from_waker(&waker))
        else {
            return Err("dropped leader did not end its original follower".into());
        };
        assert!(matches!(
            failed.error,
            ObjectStoreError::SharedReadUnavailable
        ));
        assert_eq!(reads.load(Ordering::Acquire), 1);
        assert_eq!(cached.stats()?.in_flight, 0);
        release
            .send(())
            .map_err(|_| "original read receiver closed")?;
        let retry = object_work!(cached, read(object_id, 9), B, &cancellation).await?;
        assert_eq!(retry.value.bytes, bytes);
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await?;
    assert_eq!(reads.load(Ordering::Acquire), 2);
    Ok(())
}
