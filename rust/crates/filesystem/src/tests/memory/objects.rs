use super::*;

#[tokio::test]
async fn object_store_verifies_identity_and_bounds_reads() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let object_id = ObjectId {
        storage: None,
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(b"abcdef"),
    };
    provider_receipt!(
        store,
        put(object_id, OwnedBytes::from_static(b"abcdef")),
        WorkBudget::UNBOUNDED
    )?;
    assert_eq!(
        provider_receipt!(store, read(object_id, 6), WorkBudget::UNBOUNDED)?.value,
        ObjectRead {
            bytes: OwnedBytes::from_static(b"abcdef"),
            retention: ObjectReadRetention::Shared,
        }
    );
    assert!(matches!(
        provider_receipt!(store, read(object_id, 5), WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge { .. },
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn object_batch_preserves_order_and_duplicates() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = ObjectId {
        storage: None,
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(b"first"),
    };
    let second = ObjectId {
        storage: None,
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(b"second"),
    };
    provider_receipt!(
        store,
        put(first, OwnedBytes::from_static(b"first")),
        WorkBudget::UNBOUNDED
    )?;
    provider_receipt!(
        store,
        put(second, OwnedBytes::from_static(b"second")),
        WorkBudget::UNBOUNDED
    )?;
    let receipt = provider_receipt!(
        store,
        read_many(&[
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
        ]),
        WorkBudget::UNBOUNDED
    )?;
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

    assert!(matches!(
        provider_receipt!(store, read_many(&[]), WorkBudget::UNBOUNDED),
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
        storage: None,
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(b"authentic"),
    };
    provider_receipt!(
        store,
        put(object_id, OwnedBytes::from_static(b"authentic")),
        WorkBudget::UNBOUNDED
    )?;
    let mut state = store
        .objects
        .write()
        .map_err(|_| "memory object lock poisoned")?;
    let index = state
        .objects
        .binary_search_by_key(&object_id, |(id, _)| *id)
        .map_err(|_| "resident object missing")?;
    state.objects[index].1 = OwnedBytes::from_static(b"corrupted");
    drop(state);
    assert!(matches!(
        provider_receipt!(store, read(object_id, 64), WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    assert!(matches!(
        provider_receipt!(store, contains(object_id), WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    let batch_failure = provider_receipt!(
        store,
        read_many(&[crate::storage::ObjectReadRequest {
            object_id,
            maximum_bytes: 64,
        }]),
        WorkBudget::UNBOUNDED
    )
    .err()
    .ok_or("corrupt object batch unexpectedly succeeded")?;
    assert!(matches!(batch_failure.error, ObjectStoreError::Corrupt));

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
    let store = MemoryObjectStore::new(Some(4))?;
    let unlimited = MemoryObjectStore::new(None)?;
    let oversized = OwnedBytes::from_static(b"oversized");
    let oversized_id = ObjectId {
        storage: None,
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(&oversized),
    };
    for store in [&unlimited, &MemoryObjectStore::default()] {
        provider_receipt!(
            store,
            put(oversized_id, oversized.clone()),
            WorkBudget::UNBOUNDED
        )?;
        assert_eq!(
            provider_receipt!(store, read(oversized_id, 9), WorkBudget::UNBOUNDED)?
                .value
                .bytes,
            oversized
        );
    }
    let object_id = ObjectId {
        storage: None,
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(b"data"),
    };
    assert!(matches!(
        provider_receipt!(
            store,
            put(object_id, OwnedBytes::from_static(b"oversized")),
            WorkBudget::UNBOUNDED
        ),
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge { .. },
            ..
        })
    ));
    assert!(matches!(
        provider_receipt!(
            store,
            put(object_id, OwnedBytes::from_static(b"nope")),
            WorkBudget::UNBOUNDED
        ),
        Err(ObjectFailure {
            error: ObjectStoreError::DigestMismatch,
            ..
        })
    ));
    assert!(!provider_receipt!(store, contains(object_id), WorkBudget::UNBOUNDED)?.value);
    assert!(matches!(
        provider_receipt!(store, read(object_id, 4), WorkBudget::UNBOUNDED),
        Err(ObjectFailure {
            error: ObjectStoreError::Missing,
            ..
        })
    ));
    provider_receipt!(
        store,
        put(object_id, OwnedBytes::from_static(b"data")),
        WorkBudget::UNBOUNDED
    )?;
    provider_receipt!(
        store,
        put(object_id, OwnedBytes::from_static(b"data")),
        WorkBudget::UNBOUNDED
    )?;
    assert!(matches!(
        provider_receipt!(
            store,
            read_many(&[crate::storage::ObjectReadRequest {
                object_id,
                maximum_bytes: 3,
            }]),
            WorkBudget::UNBOUNDED
        ),
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge {
                observed: 4,
                maximum: 3,
            },
            ..
        })
    ));
    let missing = ObjectId {
        storage: None,
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: Digest::from_bytes([31; 32]),
    };
    assert!(matches!(
        provider_receipt!(
            store,
            read_many(&[crate::storage::ObjectReadRequest {
                object_id: missing,
                maximum_bytes: 4,
            }]),
            WorkBudget::UNBOUNDED
        ),
        Err(ObjectFailure {
            error: ObjectStoreError::Missing,
            ..
        })
    ));
    let mut denied = WorkBudget::UNBOUNDED;
    denied.object_probes = 0;
    assert!(matches!(
        provider_receipt!(store, contains(object_id), denied),
        Err(ObjectFailure {
            error: ObjectStoreError::Work(_),
            ..
        })
    ));
    Ok(())
}
