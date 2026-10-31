use super::*;

#[tokio::test]
async fn cache_is_bounded_lru_authenticated_and_physically_observable()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let (first_id, first_bytes) = object(b"first");
    let (second_id, second_bytes) = object(b"second");
    for (object_id, bytes) in [(first_id, first_bytes), (second_id, second_bytes)] {
        (object_work!(inner, put(object_id, bytes), B, &cancellation)).await?;
    }
    let cached = CachedObjectStore::new(inner, options(1, 8))?;
    let first = object_work!(cached, read(first_id, 5), B, &cancellation).await?;
    let warm = object_work!(cached, read(first_id, 5), B, &cancellation).await?;
    let second = object_work!(cached, read(second_id, 6), B, &cancellation).await?;
    let reloaded = object_work!(cached, read(first_id, 5), B, &cancellation).await?;
    assert_eq!(first.value.bytes, warm.value.bytes);
    assert_eq!(second.value.bytes.as_ref(), b"second");
    assert_eq!(reloaded.value.bytes.as_ref(), b"first");

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
    (object_work!(inner, put(object_id, bytes), B, &active)).await?;
    let cached = CachedObjectStore::new(inner, options(1, 16))?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = (object_work!(cached, read(object_id, 7), B, &cancelled))
        .await
        .err()
        .ok_or("cancelled cache read succeeded")?;
    assert!(matches!(failure.error, ObjectStoreError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    assert_eq!(cached.stats()?, ObjectCacheStats::default());
    object_work!(cached, read(object_id, 7), B, &active).await?;
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
    object_work!(cached, put(canonical_id, canonical_bytes), B, &cancellation).await?;
    assert!(
        (object_work!(cached.inner(), contains(canonical_id), B, &cancellation))
            .await?
            .value
    );
    assert!(
        (object_work!(cached, contains(canonical_id), B, &cancellation))
            .await?
            .value
    );
    let (missing_id, _) = object(b"missing");
    assert!(
        !(object_work!(cached, contains(missing_id), B, &cancellation))
            .await?
            .value
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
        (object_work!(inner, put(object_id, bytes), B, &cancellation)).await?;
    }
    let (counting, scalar_reads, _) = counting_store(inner);
    let cached = CachedObjectStore::new(counting, options(4, 8))?;
    let clone = cached.clone();
    object_work!(cached, read(small_id, 5), B, &cancellation).await?;
    let _warm = object_work!(clone, read(small_id, 5), B, &cancellation).await?;

    object_work!(cached, read(large_id, 17), B, &cancellation).await?;
    object_work!(clone, read(large_id, 17), B, &cancellation).await?;
    assert_eq!(scalar_reads.load(Ordering::Acquire), 3);
    assert_eq!(cached.stats()?.resident_entries, 1);
    assert_eq!(cached.stats()?.resident_bytes, 5);
    Ok(())
}

#[tokio::test]
async fn decoded_eviction_and_conflicting_immutable_bytes_fail_closed()
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
    assert!(cached.insert(first_id, first_bytes.clone(), cached.shared.fill_epoch())?);
    assert!(cached.insert(first_id, first_bytes.clone(), cached.shared.fill_epoch())?);
    let conflicting = cached
        .insert(
            first_id,
            OwnedBytes::from_static(b"different"),
            cached.shared.fill_epoch(),
        )
        .err()
        .ok_or("conflicting immutable cache bytes were accepted")?;
    assert!(matches!(conflicting, ObjectStoreError::Corrupt));

    Ok(())
}
