use super::*;

#[tokio::test]
async fn object_store_verifies_identity_and_bounds_reads() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let object_id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"abcdef"),
    };
    store
        .put(
            object_id,
            OwnedBytes::from_static(b"abcdef"),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        store
            .read(
                object_id,
                6,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value,
        ObjectRead {
            bytes: OwnedBytes::from_static(b"abcdef"),
            retention: ObjectReadRetention::Shared,
        }
    );
    assert!(matches!(
        store
            .read(
                object_id,
                5,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await,
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge { .. },
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn object_batch_is_ordered_deduplicated_by_storage_and_one_backend_read()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"first"),
    };
    let second = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"second"),
    };
    store
        .put(
            first,
            OwnedBytes::from_static(b"first"),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    store
        .put(
            second,
            OwnedBytes::from_static(b"second"),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    let receipt = store
        .read_many(
            &[
                crate::storage::ObjectReadRequest {
                    object_id: second,
                    maximum_bytes: 6,
                },
                crate::storage::ObjectReadRequest {
                    object_id: first,
                    maximum_bytes: 5,
                },
                crate::storage::ObjectReadRequest {
                    object_id: second,
                    maximum_bytes: 6,
                },
            ],
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        receipt
            .value
            .iter()
            .map(|value| value.bytes.clone())
            .collect::<Vec<_>>(),
        vec![
            OwnedBytes::from_static(b"second"),
            OwnedBytes::from_static(b"first"),
            OwnedBytes::from_static(b"second"),
        ]
    );
    assert_eq!(receipt.work.backend_read_operations, 1);
    assert_eq!(receipt.work.object_probes, 3);
    assert_eq!(receipt.work.items_returned, 3);
    assert!(matches!(
        store
            .read_many(&[], WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await,
        Err(ObjectFailure {
            error: ObjectStoreError::EmptyReadBatch,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn object_store_reauthenticates_shared_bytes_after_memory_corruption()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let object_id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"authentic"),
    };
    store
        .put(
            object_id,
            OwnedBytes::from_static(b"authentic"),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    let mut state = store
        .objects
        .write()
        .map_err(|_| "memory object lock poisoned")?;
    let index = state
        .objects
        .binary_search_by_key(&object_id, |(id, _)| *id)
        .map_err(|_| "resident object is missing")?;
    state.objects[index].1 = OwnedBytes::from_static(b"corrupted");
    drop(state);
    assert!(matches!(
        store
            .read(
                object_id,
                64,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await,
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    assert!(matches!(
        store
            .contains(
                object_id,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await,
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    let batch_failure = store
        .read_many(
            &[crate::storage::ObjectReadRequest {
                object_id,
                maximum_bytes: 64,
            }],
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("corrupt object batch unexpectedly succeeded")?;
    assert!(matches!(batch_failure.error, ObjectStoreError::Corrupt));
    assert_eq!(batch_failure.work.object_probes, 1);
    assert_eq!(batch_failure.work.backend_read_operations, 1);
    assert_eq!(batch_failure.work.object_bytes_read, 9);
    Ok(())
}

#[tokio::test]
async fn object_admission_rejects_size_identity_absence_and_batch_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        MemoryObjectStore::new(0),
        Err(ObjectStoreError::TooLarge {
            observed: 0,
            maximum: 0,
        })
    ));
    let store = MemoryObjectStore::new(4)?;
    let object_id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"data"),
    };
    assert!(matches!(
        store
            .put(
                object_id,
                OwnedBytes::from_static(b"oversized"),
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await,
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge { .. },
            ..
        })
    ));
    assert!(matches!(
        store
            .put(
                object_id,
                OwnedBytes::from_static(b"nope"),
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await,
        Err(ObjectFailure {
            error: ObjectStoreError::DigestMismatch,
            ..
        })
    ));
    assert!(
        !store
            .contains(
                object_id,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
    );
    assert!(matches!(
        store
            .read(
                object_id,
                4,
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await,
        Err(ObjectFailure {
            error: ObjectStoreError::Missing,
            ..
        })
    ));
    store
        .put(
            object_id,
            OwnedBytes::from_static(b"data"),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    store
        .put(
            object_id,
            OwnedBytes::from_static(b"data"),
            WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?;
    assert!(matches!(
        store
            .read_many(
                &[crate::storage::ObjectReadRequest {
                    object_id,
                    maximum_bytes: 3,
                }],
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await,
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge {
                observed: 4,
                maximum: 3,
            },
            ..
        })
    ));
    let missing = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: Digest::from_bytes([31; 32]),
    };
    assert!(matches!(
        store
            .read_many(
                &[crate::storage::ObjectReadRequest {
                    object_id: missing,
                    maximum_bytes: 4,
                }],
                WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await,
        Err(ObjectFailure {
            error: ObjectStoreError::Missing,
            ..
        })
    ));
    let mut denied = WorkBudget::UNBOUNDED;
    denied.object_probes = 0;
    assert!(matches!(
        store
            .contains(object_id, denied, &crate::CancellationToken::new())
            .await,
        Err(ObjectFailure {
            error: ObjectStoreError::Work(_),
            ..
        })
    ));
    Ok(())
}
