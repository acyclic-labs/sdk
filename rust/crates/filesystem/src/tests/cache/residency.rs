use super::*;

#[tokio::test]
async fn cache_is_bounded_lru_authenticated_and_physically_observable()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (first_id, first_bytes) = object(b"first");
    let (second_id, second_bytes) = object(b"second");
    for (object_id, bytes) in [(first_id, first_bytes), (second_id, second_bytes)] {
        (inner
            .put(
                object_id,
                bytes,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
            )
            .await)?;
    }
    let cached = CachedObjectStore::new(inner, options(1, 8))?;
    let first = (async {
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = cached.read(first_id, 5, &mut __object_work).await;
        __object_work.receipt(__object_result)
    })
    .await?;
    let warm = (async {
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = cached.read(first_id, 5, &mut __object_work).await;
        __object_work.receipt(__object_result)
    })
    .await?;
    let second = (cached
        .read(
            second_id,
            6,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .await)?;
    let reloaded = (cached
        .read(
            first_id,
            5,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .await)?;
    assert_eq!(first.value.bytes, warm.value.bytes);
    assert_eq!(second.bytes.as_ref(), b"second");
    assert_eq!(reloaded.bytes.as_ref(), b"first");
    assert_eq!(first.work.backend_read_operations, 1);
    assert_eq!(warm.work.backend_read_operations, 0);
    assert_eq!(warm.value.retention, ObjectReadRetention::Shared);
    assert_eq!(
        cached.stats()?,
        ObjectCacheStats {
            hits: 1,
            decoded_hits: 0,
            misses: 3,
            coalesced_reads: 0,
            evictions: 2,
            resident_entries: 1,
            resident_bytes: 5,
            resident_canonical_objects: 1,
            resident_canonical_bytes: 5,
            resident_decoded_pages: 0,
            resident_decoded_bytes: 0,
            in_flight: 0,
        }
    );
    Ok(())
}

#[tokio::test]
async fn cache_bounds_cancellation_and_clear_fail_before_hidden_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        CachedObjectStore::new(MemoryObjectStore::default(), options(0, 1)).err(),
        Some(ObjectCacheConfigError::ZeroBound)
    );
    let inner = MemoryObjectStore::default();
    let (object_id, bytes) = object(b"bounded");
    let active = CancellationToken::new();
    (inner
        .put(
            object_id,
            bytes,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &active),
        )
        .await)?;
    let cached = CachedObjectStore::new(inner, options(1, 16))?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = (async {
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancelled);
        let __object_result = cached.read(object_id, 7, &mut __object_work).await;
        __object_work.receipt(__object_result)
    })
    .await
    .err()
    .ok_or("cancelled cache read succeeded")?;
    assert!(matches!(failure.error, ObjectStoreError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    assert_eq!(cached.stats()?, ObjectCacheStats::default());
    (cached
        .read(
            object_id,
            7,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &active),
        )
        .await)?;
    cached.clear()?;
    assert_eq!(cached.stats()?.resident_entries, 0);
    assert_eq!(cached.stats()?.resident_bytes, 0);
    Ok(())
}

#[tokio::test]
async fn put_contains_and_decoded_pages_share_one_deterministic_lru()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cached = CachedObjectStore::new(inner, options(2, 64))?;
    let cancellation = CancellationToken::new();
    let (canonical_id, canonical_bytes) = object(b"canonical");
    (cached
        .put(
            canonical_id,
            canonical_bytes,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .await)?;
    assert!(
        cached
            .inner()
            .contains(
                canonical_id,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
            )
            .await?
    );
    assert!(
        (cached
            .contains(
                canonical_id,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
            )
            .await)?
    );
    let (missing_id, _) = object(b"missing");
    assert!(
        !(cached
            .contains(
                missing_id,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
            )
            .await)?
    );

    let first_key = DecodedCacheKey::new::<u64>(canonical_id, DecodeLimits::default());
    let first_value = DecodedCacheValue {
        value: Arc::new(41_u64),
        logical_bytes: 8,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
    };
    assert!(matches!(
        cached.decoded_cache_admit(first_key, first_value)?,
        DecodedCacheAdmission::Shared(_)
    ));
    let cached_value = cached
        .decoded_cache_get(first_key)?
        .ok_or("decoded cache admission was not observable")?;
    assert_eq!(cached_value.value.downcast_ref::<u64>(), Some(&41));

    let second_key = DecodedCacheKey::new::<u32>(missing_id, DecodeLimits::default());
    assert!(matches!(
        cached.decoded_cache_admit(
            second_key,
            DecodedCacheValue {
                value: Arc::new(7_u32),
                logical_bytes: 4,
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: None,
            },
        )?,
        DecodedCacheAdmission::Shared(_)
    ));
    let stats = cached.stats()?;
    assert_eq!(stats.evictions, 1);
    assert_eq!(stats.resident_canonical_objects, 0);
    assert_eq!(stats.resident_decoded_pages, 2);

    let replacement = DecodedCacheValue {
        value: Arc::new(99_u64),
        logical_bytes: 8,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
    };
    let DecodedCacheAdmission::Shared(existing) =
        cached.decoded_cache_admit(first_key, replacement)?
    else {
        return Err("existing decoded page was not shared".into());
    };
    assert_eq!(existing.value.downcast_ref::<u64>(), Some(&41));

    let oversized_key = DecodedCacheKey::new::<u16>(missing_id, DecodeLimits::default());
    assert!(matches!(
        cached.decoded_cache_admit(
            oversized_key,
            DecodedCacheValue {
                value: Arc::new(5_u16),
                logical_bytes: 65,
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: None,
            },
        )?,
        DecodedCacheAdmission::Uncached(_)
    ));
    Ok(())
}

#[tokio::test]
async fn oversized_objects_bypass_residency_while_clones_share_warm_entries()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (small_id, small_bytes) = object(b"small");
    let (large_id, large_bytes) = object(b"larger-than-cache");
    for (object_id, bytes) in [(small_id, small_bytes), (large_id, large_bytes)] {
        (inner
            .put(
                object_id,
                bytes,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
            )
            .await)?;
    }
    let (counting, scalar_reads, _) = counting_store(inner);
    let cached = CachedObjectStore::new(counting, options(4, 8))?;
    let clone = cached.clone();
    (cached
        .read(
            small_id,
            5,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .await)?;
    let warm = (async {
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = clone.read(small_id, 5, &mut __object_work).await;
        __object_work.receipt(__object_result)
    })
    .await?;
    assert_eq!(warm.work.backend_read_operations, 0);
    (cached
        .read(
            large_id,
            17,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .await)?;
    (clone
        .read(
            large_id,
            17,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .await)?;
    assert_eq!(scalar_reads.load(Ordering::Acquire), 3);
    assert_eq!(cached.stats()?.resident_entries, 1);
    assert_eq!(cached.stats()?.resident_bytes, 5);
    Ok(())
}

#[tokio::test]
async fn decoded_eviction_poison_recovery_and_failure_accounting_are_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let cached = CachedObjectStore::new(MemoryObjectStore::default(), options(1, 64))?;
    let (first_id, first_bytes) = object(b"first");
    let (second_id, _) = object(b"second");
    let first_key = DecodedCacheKey::new::<u64>(first_id, DecodeLimits::default());
    let second_key = DecodedCacheKey::new::<u32>(second_id, DecodeLimits::default());
    assert!(matches!(
        cached.decoded_cache_admit(
            first_key,
            DecodedCacheValue {
                value: Arc::new(1_u64),
                logical_bytes: 8,
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: None,
            },
        )?,
        DecodedCacheAdmission::Shared(_)
    ));
    assert!(matches!(
        cached.decoded_cache_admit(
            second_key,
            DecodedCacheValue {
                value: Arc::new(2_u32),
                logical_bytes: 4,
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: None,
            },
        )?,
        DecodedCacheAdmission::Shared(_)
    ));
    assert!(cached.decoded_cache_get(first_key)?.is_none());
    assert_eq!(
        cached
            .decoded_cache_get(second_key)?
            .and_then(|value| value.value.downcast_ref::<u32>().copied()),
        Some(2)
    );
    let stats = cached.stats()?;
    assert_eq!(stats.evictions, 1);
    assert_eq!(stats.resident_decoded_pages, 1);

    cached.clear()?;
    assert!(cached.insert(first_id, first_bytes.clone())?);
    assert!(cached.insert(first_id, first_bytes.clone())?);
    let conflicting = cached
        .insert(first_id, OwnedBytes::from_static(b"different"))
        .err()
        .ok_or("conflicting immutable cache bytes were accepted")?;
    assert!(matches!(conflicting, ObjectStoreError::Corrupt));

    let prior = WorkCounters {
        bytes_hashed: u64::MAX,
        ..WorkCounters::default()
    };
    let combined = combine_failure(
        prior,
        ObjectFailure::new(
            ObjectStoreError::Missing,
            WorkCounters {
                bytes_hashed: 1,
                ..WorkCounters::default()
            },
        ),
    );
    assert!(matches!(
        combined.error,
        ObjectStoreError::Work(crate::performance::WorkError::Overflow)
    ));
    assert_eq!(*combined.work, prior);

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = (wait_for_flight(&Flight::new(1), &cancellation))
        .await
        .err()
        .ok_or("pre-cancelled flight wait succeeded")?;
    assert!(matches!(cancelled, ObjectStoreError::Cancelled));

    let flight = Flight::new(1);
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = match flight.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        std::panic::resume_unwind(Box::new("poison flight state"));
    }));
    assert!(poisoned.is_err());
    flight.finish(Some(OwnedBytes::from_static(b"must-not-survive-poison")));
    let flight_state = match flight.state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    assert!(matches!(&*flight_state, FlightState::Complete(None)));

    let completed = Flight::new(1);
    completed.finish(Some(OwnedBytes::from_static(b"complete")));
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = match completed.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        std::panic::resume_unwind(Box::new("poison completed flight state"));
    }));
    assert!(poisoned.is_err());
    completed.finish(None);
    Ok(())
}
