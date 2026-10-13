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
async fn concurrent_reads_share_one_backend_request_and_dropped_leaders_recover()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"single-flight");
    (object_work!(inner, put(object_id, bytes.clone()), B, &cancellation)).await?;
    let gate = Arc::new(Gate::new());
    let reads = Arc::new(AtomicUsize::new(0));
    let cached = CachedObjectStore::new(
        GatedStore {
            inner: Arc::new(inner),
            gate: Arc::clone(&gate),
            reads: Arc::clone(&reads),
        },
        options(4, 64),
    )?;
    let (first, second) = async {
        let first = object_work!(cached, read(object_id, 13), B, &cancellation);
        let second = object_work!(cached, read(object_id, 13), B, &cancellation);
        let release = async {
            while reads.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
            gate.release();
        };
        let (first, second, ()) = tokio::join!(first, second, release);
        (first, second)
    }
    .await;
    let first = first?;
    let second = second?;
    assert_eq!(first.value.bytes, bytes);
    assert_eq!(second.value.bytes, bytes);
    assert_eq!(reads.load(Ordering::Acquire), 1);

    assert_eq!(cached.stats()?.coalesced_reads, 1);
    Ok(())
}

#[tokio::test]
async fn concurrent_scalar_and_batch_reads_share_success_and_retry_lost_results()
-> Result<(), Box<dyn std::error::Error>> {
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
        let gate = Arc::new(Gate::new());
        let reads = Arc::new(AtomicUsize::new(0));
        let cached = CachedObjectStore::new(
            GatedStore {
                inner: Arc::new(inner),
                gate: Arc::clone(&gate),
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
            let release = async {
                while reads.load(Ordering::Acquire) == 0 {
                    tokio::task::yield_now().await;
                }
                gate.release();
            };
            let (scalar, batch, ()) = tokio::join!(scalar, batch, release);
            (scalar, batch)
        }
        .await;
        if object_id == 0 {
            assert_eq!(scalar?.value.bytes, bytes);
            assert_eq!(batch?.value[0].bytes, bytes);
            assert_eq!(reads.load(Ordering::Acquire), 1);
        } else {
            assert!(scalar.is_err());
            assert!(batch.is_err());
            assert_eq!(reads.load(Ordering::Acquire), 2);
        }
        assert_eq!(cached.stats()?.in_flight, 0);
        assert_eq!(cached.stats()?.coalesced_reads, 1);
    }
    Ok(())
}

#[tokio::test]
async fn dropped_leaders_release_flights_and_retry_without_a_stale_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (object_id, bytes) = object(b"drop-safe");
    (object_work!(inner, put(object_id, bytes.clone()), B, &cancellation)).await?;
    let gate = Arc::new(Gate::new());
    let reads = Arc::new(AtomicUsize::new(0));
    let cached = CachedObjectStore::new(
        GatedStore {
            inner: Arc::new(inner),
            gate: Arc::clone(&gate),
            reads: Arc::clone(&reads),
        },
        options(4, 64),
    )?;
    async {
        {
            let read = object_work!(cached, read(object_id, 9), B, &cancellation);
            tokio::pin!(read);
            poll_fn(|context| match read.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(Ok(())),
                Poll::Ready(_) => Poll::Ready(Err(std::io::Error::other(
                    "gated leader completed before cancellation",
                ))),
            })
            .await?;
        }
        assert_eq!(cached.stats()?.in_flight, 0);
        gate.release();
        let retry = object_work!(cached, read(object_id, 9), B, &cancellation).await?;
        assert_eq!(retry.value.bytes, bytes);
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await?;
    assert_eq!(reads.load(Ordering::Acquire), 2);
    Ok(())
}
