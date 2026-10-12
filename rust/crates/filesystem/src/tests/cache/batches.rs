use super::*;

#[tokio::test]
async fn malformed_backend_batch_cardinality_releases_all_flights()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = Arc::new(MemoryObjectStore::default());
    let cancellation = CancellationToken::new();
    let objects = [object(b"first-short"), object(b"second-short")];
    for (object_id, bytes) in &objects {
        (async {
            let authority_input_0 = *object_id;
            let authority_input_1 = bytes.clone();

            let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let __object_result = inner
                .put(authority_input_0, authority_input_1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        })
        .await?;
    }
    let cached = CachedObjectStore::new(ShortBatchStore { inner }, options(4, 64))?;
    let requests = objects.map(|(object_id, bytes)| ObjectReadRequest {
        object_id,
        maximum_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    });
    let failure = (async {
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = cached
            .read_many(&requests, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    })
    .await
    .err()
    .ok_or("short backend batch was accepted")?;
    assert!(matches!(failure.error, ObjectStoreError::Corrupt));
    assert_eq!(cached.stats()?.in_flight, 0);
    Ok(())
}

#[tokio::test]
async fn mixed_batches_preserve_backend_multiget_order_duplicates_and_exact_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let objects = [object(b"warm"), object(b"second"), object(b"third")];
    for (object_id, bytes) in &objects {
        (async {
            let authority_input_0 = *object_id;
            let authority_input_1 = bytes.clone();

            let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let __object_result = inner
                .put(authority_input_0, authority_input_1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        })
        .await?;
    }
    let (counting, scalar_reads, batch_reads) = counting_store(inner);
    let cached = CachedObjectStore::new(counting, options(8, 128))?;
    (async {
        let authority_input_0 = objects[0].0;

        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = cached.read(authority_input_0, 4, &mut __object_work).await;
        __object_work.receipt(__object_result)
    })
    .await?;
    let requests = [
        ObjectReadRequest {
            object_id: objects[0].0,
            maximum_bytes: 4,
        },
        ObjectReadRequest {
            object_id: objects[1].0,
            maximum_bytes: 6,
        },
        ObjectReadRequest {
            object_id: objects[1].0,
            maximum_bytes: 6,
        },
        ObjectReadRequest {
            object_id: objects[2].0,
            maximum_bytes: 5,
        },
    ];
    let receipt = (async {
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = cached
            .read_many(&requests, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    })
    .await?;
    assert_eq!(
        receipt
            .value
            .iter()
            .map(|value| value.bytes.as_ref())
            .collect::<Vec<_>>(),
        vec![b"warm".as_slice(), b"second", b"second", b"third"]
    );
    assert_eq!(receipt.work.backend_read_operations, 1);
    assert_eq!(scalar_reads.load(Ordering::Acquire), 1);
    assert_eq!(batch_reads.load(Ordering::Acquire), 1);

    let unequal = [
        ObjectReadRequest {
            object_id: objects[1].0,
            maximum_bytes: 1,
        },
        ObjectReadRequest {
            object_id: objects[1].0,
            maximum_bytes: 6,
        },
    ];
    let failure = (async {
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = cached
            .read_many(&unequal, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    })
    .await
    .err()
    .ok_or("per-request byte limit was ignored")?;
    assert!(matches!(
        failure.error,
        ObjectStoreError::TooLarge {
            observed: 6,
            maximum: 1
        }
    ));
    assert_eq!(batch_reads.load(Ordering::Acquire), 1);
    Ok(())
}

#[tokio::test]
async fn failed_batch_planning_releases_every_unstarted_flight_without_backend_io()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let objects = [object(b"one"), object(b"two")];
    for (object_id, bytes) in &objects {
        (async {
            let authority_input_0 = *object_id;
            let authority_input_1 = bytes.clone();

            let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let __object_result = inner
                .put(authority_input_0, authority_input_1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        })
        .await?;
    }
    let (counting, scalar_reads, batch_reads) = counting_store(inner);
    let cached = CachedObjectStore::new(
        counting,
        ObjectCacheOptions {
            maximum_entries: 4,
            maximum_bytes: 64,
            maximum_in_flight: 1,
            maximum_waiters_per_object: 1,
        },
    )?;
    let requests = objects.map(|(object_id, bytes)| ObjectReadRequest {
        object_id,
        maximum_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    });
    let failure = (async {
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = cached
            .read_many(&requests, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    })
    .await
    .err()
    .ok_or("over-capacity batch unexpectedly succeeded")?;
    assert!(matches!(failure.error, ObjectStoreError::Rejected(_)));
    assert_eq!(cached.stats()?.in_flight, 0);
    assert_eq!(scalar_reads.load(Ordering::Acquire), 0);
    assert_eq!(batch_reads.load(Ordering::Acquire), 0);
    Ok(())
}
