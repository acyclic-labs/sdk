use super::*;

use crate::test_support::PausedStore;

#[derive(Clone, Copy, Debug)]
enum Fill {
    Read,
    ReadMany,
    Put,
    PutHashed,
    PutMany,
    PutManyHashed,
}

#[derive(Clone, Copy, Debug)]
enum Interruption {
    OwnClear,
    PeerClear,
    ProducerLoss,
    Withdrawal,
}

#[tokio::test]
async fn original_fill_epoch_survives_clear_reply_loss_and_reentrant_destruction()
-> Result<(), Box<dyn std::error::Error>> {
    for fill in [
        Fill::Read,
        Fill::ReadMany,
        Fill::Put,
        Fill::PutHashed,
        Fill::PutMany,
        Fill::PutManyHashed,
    ] {
        for interruption in [
            Interruption::OwnClear,
            Interruption::PeerClear,
            Interruption::ProducerLoss,
            Interruption::Withdrawal,
        ] {
            let inner = Arc::new(MemoryObjectStore::default());
            let cancel = CancellationToken::new();
            let objects = [object(b"first epoch"), object(b"second epoch")];
            for (id, bytes) in &objects {
                inner
                    .put(*id, bytes.clone(), &mut AuthorityWork::new(B, &cancel))
                    .await?;
            }
            let (release, receiver) = tokio::sync::oneshot::channel();
            let cache = CachedObjectStore::new(
                PausedStore {
                    inner: Arc::clone(&inner),
                    release: Mutex::new(Some(receiver)),
                },
                options(4, 128),
            )?;
            let peer = cache.share_cache_with(())?;
            let mut work = AuthorityWork::new(B, &cancel);
            let pending = async {
                match fill {
                    Fill::Read => {
                        let read = cache.read(objects[0].0, 128, &mut work).await?;
                        assert_eq!(read.bytes, objects[0].1);
                    }
                    Fill::ReadMany => {
                        let requests = objects.each_ref().map(|(object_id, _)| ObjectReadRequest {
                            object_id: *object_id,
                            maximum_bytes: 128,
                        });
                        let (reads, allocation) = cache.read_many(&requests, &mut work).await?;
                        assert_eq!(reads.len(), 2);
                        for (read, (_, bytes)) in reads.iter().zip(&objects) {
                            assert_eq!(&read.bytes, bytes);
                        }
                        drop(reads);
                        drop(allocation);
                    }
                    Fill::Put => {
                        cache
                            .put(objects[0].0, objects[0].1.clone(), &mut work)
                            .await?
                    }
                    Fill::PutHashed => {
                        cache
                            .put_hashed(
                                crate::HashedObject::new(
                                    ObjectKind::BlobChunk,
                                    objects[0].1.clone(),
                                ),
                                &mut work,
                            )
                            .await?;
                    }
                    Fill::PutMany => {
                        let writes = objects.each_ref().map(|(object_id, bytes)| {
                            crate::storage::ObjectWrite {
                                object_id: *object_id,
                                bytes: bytes.clone(),
                            }
                        });
                        cache.put_many(&writes, &mut work).await?;
                    }
                    Fill::PutManyHashed => {
                        let hashed = objects.each_ref().map(|(_, bytes)| {
                            crate::HashedObject::new(ObjectKind::BlobChunk, bytes.clone())
                        });
                        cache.put_many_hashed(&hashed, &mut work).await?;
                    }
                }
                Ok::<_, ObjectStoreError>(())
            };
            let mut pending = Box::pin(pending);
            // One deterministic drive reaches the original provider suspension.
            poll_fn(|cx| {
                assert!(
                    pending.as_mut().poll(cx).is_pending(),
                    "{fill:?}/{interruption:?}"
                );
                Poll::Ready(())
            })
            .await;
            match interruption {
                Interruption::OwnClear => {
                    cache.clear()?;
                    release
                        .send(())
                        .map_err(|_| "original provider receiver lost")?;
                }
                Interruption::PeerClear => {
                    peer.clear()?;
                    release
                        .send(())
                        .map_err(|_| "original provider receiver lost")?;
                }
                Interruption::ProducerLoss => drop(release),
                Interruption::Withdrawal => {
                    drop(pending);
                    assert!(release.send(()).is_err());
                    assert_eq!(cache.stats()?.resident_canonical_objects, 0);
                    assert_eq!(cache.stats()?.in_flight, 0);
                    continue;
                }
            }
            let result = pending.await;
            match interruption {
                Interruption::ProducerLoss => assert!(
                    matches!(result, Err(ObjectStoreError::Rejected(ref why)) if why == "cache fill producer lost")
                ),
                _ => result?,
            }
            let expected = if matches!(interruption, Interruption::PeerClear) {
                if matches!(fill, Fill::ReadMany | Fill::PutMany | Fill::PutManyHashed) {
                    2
                } else {
                    1
                }
            } else {
                0
            };
            assert_eq!(
                cache.stats()?.resident_canonical_objects,
                expected,
                "{fill:?}/{interruption:?}"
            );
            assert_eq!(cache.stats()?.in_flight, 0);
            // Refusing accelerator admission never removes the actual object.
            let read = inner
                .read(objects[0].0, 128, &mut AuthorityWork::new(B, &cancel))
                .await?;
            assert_eq!(read.bytes, objects[0].1);
            cache
                .read(objects[0].0, 128, &mut AuthorityWork::new(B, &cancel))
                .await?;
            assert!(cache.cached_contains(objects[0].0)?);
        }
    }
    reentrant_clear_refuses_the_original_fill()?;
    Ok(())
}

fn reentrant_clear_refuses_the_original_fill() -> Result<(), ObjectStoreError> {
    struct ClearOnDrop(std::sync::Weak<Namespace>);
    impl Drop for ClearOnDrop {
        fn drop(&mut self) {
            CachedObjectStore {
                inner: (),
                shared: self.0.upgrade().unwrap(),
            }
            .clear()
            .unwrap();
        }
    }
    let cache = CachedObjectStore::new((), options(1, 128)).unwrap();
    let (id, bytes) = object(b"clear during eviction");
    let value = DecodedCacheValue {
        value: Arc::new(ClearOnDrop(Arc::downgrade(&cache.shared))),
        logical_bytes: 1,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
    };
    drop(cache.decoded_admit(
        DecodedCacheKey::new::<ClearOnDrop>(id, DecodeLimits::default()),
        value,
    )?);
    let original_epoch = cache.shared.fill_epoch();
    assert!(!cache.insert(id, bytes.clone(), original_epoch)?);
    assert_eq!(cache.stats()?.resident_entries, 0);
    // Counterfactual: rediscovering an epoch after the original fill loses its
    // evidence and wrongly allows that same old result to restore residency.
    assert!(cache.insert(id, bytes, cache.shared.fill_epoch())?);
    assert_eq!(cache.stats()?.resident_entries, 1);
    Ok(())
}

#[test]
fn decoded_admission_rechecks_after_reentrant_clear() -> Result<(), ObjectStoreError> {
    use crate::async_storage::DecodedCacheLookup;
    struct ClearOnDrop(std::sync::Weak<Namespace>);
    impl Drop for ClearOnDrop {
        fn drop(&mut self) {
            CachedObjectStore {
                inner: (),
                shared: self.0.upgrade().unwrap(),
            }
            .clear()
            .unwrap();
        }
    }
    let cache = CachedObjectStore::new((), options(1, 128)).unwrap();
    let (id, _) = object(b"evicted decoded origin");
    let key = DecodedCacheKey::new::<u64>(id, DecodeLimits::default());
    drop(cache.decoded_admit(
        DecodedCacheKey::new::<ClearOnDrop>(id, DecodeLimits::default()),
        DecodedCacheValue {
            value: Arc::new(ClearOnDrop(Arc::downgrade(&cache.shared))),
            logical_bytes: 1,
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        },
    )?);
    let DecodedCacheLookup::Miss(miss) = cache.decoded_lookup(key)? else {
        return Err(ObjectStoreError::Corrupt);
    };
    let value = DecodedCacheValue {
        value: Arc::new(19_u64),
        logical_bytes: 8,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
    };
    let retained = Arc::clone(&value.value);
    let DecodedCacheAdmission::Uncached(value) = miss.admit(value)? else {
        return Err(ObjectStoreError::Corrupt);
    };
    assert!(Arc::ptr_eq(&value.value, &retained));
    assert_eq!(cache.stats()?.resident_entries, 0);
    // The old completion-time epoch lookup incorrectly restores residency.
    assert!(matches!(
        cache.decoded_admit(key, value)?,
        DecodedCacheAdmission::Shared(_)
    ));
    Ok(())
}
