use super::*;

#[tokio::test]
async fn missing_scalar_and_batch_reads_release_every_flight_for_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (present_id, present_bytes) = object(b"present");
    (async {
        let authority_input_1 = present_bytes.clone();

        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = inner
            .put(present_id, authority_input_1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    })
    .await?;
    let (missing_id, _) = object(b"absent");
    let cached = CachedObjectStore::new(inner, options(4, 64))?;

    for _ in 0..2 {
        assert!(
            (async {
                let mut __object_work =
                    crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
                let __object_result = cached.read(missing_id, 6, &mut __object_work).await;
                __object_work.receipt(__object_result)
            })
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
        (async {
            let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let __object_result = cached
                .read_many(&requests, &mut __object_work)
                .await
                .map(|(values, _allocation)| values);
            __object_work.receipt(__object_result)
        })
        .await
        .is_err()
    );
    assert_eq!(cached.stats()?.in_flight, 0);
    let retry = (async {
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = cached.read(present_id, 7, &mut __object_work).await;
        __object_work.receipt(__object_result)
    })
    .await?;
    assert_eq!(retry.value.bytes, present_bytes);

    let empty = (async {
        let authority_input_0 = &[];

        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = cached
            .read_many(authority_input_0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    })
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
    (async {
        let authority_input_1 = bytes.clone();

        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = inner
            .put(object_id, authority_input_1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    })
    .await?;
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
    let (first, second) = (async {
        let first = async {
            let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let __object_result = cached.read(object_id, 13, &mut __object_work).await;
            __object_work.receipt(__object_result)
        };
        let second = async {
            let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let __object_result = cached.read(object_id, 13, &mut __object_work).await;
            __object_work.receipt(__object_result)
        };
        let release = async {
            while reads.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
            gate.release();
        };
        let (first, second, ()) = tokio::join!(first, second, release);
        (first, second)
    })
    .await;
    let first = first?;
    let second = second?;
    assert_eq!(first.value.bytes, bytes);
    assert_eq!(second.value.bytes, bytes);
    assert_eq!(reads.load(Ordering::Acquire), 1);
    assert_eq!(
        first.work.backend_read_operations + second.work.backend_read_operations,
        1
    );
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
            (async {
                let authority_input_1 = bytes.clone();

                let mut __object_work =
                    crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
                let __object_result = inner
                    .put(identity, authority_input_1, &mut __object_work)
                    .await;
                __object_work.receipt(__object_result)
            })
            .await?;
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
        let (scalar, batch) = (async {
            let scalar = async {
                let authority_input_1 = u64::try_from(bytes.len()).unwrap_or(u64::MAX);

                let mut __object_work =
                    crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
                let __object_result = cached
                    .read(identity, authority_input_1, &mut __object_work)
                    .await;
                __object_work.receipt(__object_result)
            };
            let batch = async {
                let mut __object_work =
                    crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
                let __object_result = cached
                    .read_many(&requests, &mut __object_work)
                    .await
                    .map(|(values, _allocation)| values);
                __object_work.receipt(__object_result)
            };
            let release = async {
                while reads.load(Ordering::Acquire) == 0 {
                    tokio::task::yield_now().await;
                }
                gate.release();
            };
            let (scalar, batch, ()) = tokio::join!(scalar, batch, release);
            (scalar, batch)
        })
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
async fn abandoned_scalar_and_batch_followers_retry_through_one_fresh_leader()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();

    for batch in [false, true] {
        let inner = MemoryObjectStore::default();
        let (object_id, bytes) = object(if batch {
            b"batch-retry"
        } else {
            b"scalar-retry"
        });
        (async {
            let authority_input_1 = bytes.clone();

            let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let __object_result = inner
                .put(object_id, authority_input_1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        })
        .await?;
        let cached = CachedObjectStore::new(inner, options(4, 64))?;
        let abandoned = Arc::new(Flight::new(1));
        abandoned.finish(None);
        {
            let mut state = cached.lock_state()?;
            state.flights.insert(object_id, Arc::clone(&abandoned));
        }

        if batch {
            let receipt = (async {
                let authority_input_0 = &[ObjectReadRequest {
                    object_id,
                    maximum_bytes: u64::try_from(bytes.len())?,
                }];

                let mut __object_work =
                    crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
                let __object_result = cached
                    .read_many(authority_input_0, &mut __object_work)
                    .await
                    .map(|(values, _allocation)| values);
                __object_work.receipt(__object_result)
            })
            .await?;
            assert_eq!(receipt.value[0].bytes, bytes);
        } else {
            let receipt = (async {
                let authority_input_1 = u64::try_from(bytes.len())?;

                let mut __object_work =
                    crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
                let __object_result = cached
                    .read(object_id, authority_input_1, &mut __object_work)
                    .await;
                __object_work.receipt(__object_result)
            })
            .await?;
            assert_eq!(receipt.value.bytes, bytes);
        }
        assert_eq!(cached.stats()?.coalesced_reads, 1);
        assert_eq!(cached.stats()?.in_flight, 0);
    }
    Ok(())
}
