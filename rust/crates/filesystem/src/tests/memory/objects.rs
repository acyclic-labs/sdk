use super::*;

#[tokio::test]
async fn object_store_verifies_identity_and_bounds_reads() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let object_id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"abcdef"),
    };
    {
        let authority_input_1 = OwnedBytes::from_static(b"abcdef");
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __object_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __object_result = store
            .put(object_id, authority_input_1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    assert_eq!(
        {
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __object_result = store.read(object_id, 6, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }?
        .value,
        ObjectRead {
            bytes: OwnedBytes::from_static(b"abcdef"),
            retention: ObjectReadRetention::Shared,
        }
    );
    assert!(matches!(
        {
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __object_result = store.read(object_id, 5, &mut __object_work).await;
            __object_work.receipt(__object_result)
        },
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
    {
        let authority_input_1 = OwnedBytes::from_static(b"first");
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __object_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __object_result = store
            .put(first, authority_input_1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    {
        let authority_input_1 = OwnedBytes::from_static(b"second");
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __object_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __object_result = store
            .put(second, authority_input_1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    let receipt = {
        let authority_input_0 = &[
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
        ];
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __object_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __object_result = store
            .read_many(authority_input_0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }?;
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
        {
            let authority_input_0 = &[];
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __object_result = store
                .read_many(authority_input_0, &mut __object_work)
                .await
                .map(|(values, _allocation)| values);
            __object_work.receipt(__object_result)
        },
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
    {
        let authority_input_1 = OwnedBytes::from_static(b"authentic");
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __object_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __object_result = store
            .put(object_id, authority_input_1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
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
        {
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __object_result = store.read(object_id, 64, &mut __object_work).await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    assert!(matches!(
        {
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __object_result = store.contains(object_id, &mut __object_work).await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::Corrupt,
            ..
        })
    ));
    let batch_failure = {
        let authority_input_0 = &[crate::storage::ObjectReadRequest {
            object_id,
            maximum_bytes: 64,
        }];
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __object_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __object_result = store
            .read_many(authority_input_0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }
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
        {
            let authority_input_1 = OwnedBytes::from_static(b"oversized");
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __object_result = store
                .put(object_id, authority_input_1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::TooLarge { .. },
            ..
        })
    ));
    assert!(matches!(
        {
            let authority_input_1 = OwnedBytes::from_static(b"nope");
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __object_result = store
                .put(object_id, authority_input_1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::DigestMismatch,
            ..
        })
    ));
    assert!(
        !{
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __object_result = store.contains(object_id, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }?
        .value
    );
    assert!(matches!(
        {
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __object_result = store.read(object_id, 4, &mut __object_work).await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::Missing,
            ..
        })
    ));
    {
        let authority_input_1 = OwnedBytes::from_static(b"data");
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __object_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __object_result = store
            .put(object_id, authority_input_1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    {
        let authority_input_1 = OwnedBytes::from_static(b"data");
        let authority_cancellation = &crate::CancellationToken::new();

        let mut __object_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __object_result = store
            .put(object_id, authority_input_1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    assert!(matches!(
        {
            let authority_input_0 = &[crate::storage::ObjectReadRequest {
                object_id,
                maximum_bytes: 3,
            }];
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __object_result = store
                .read_many(authority_input_0, &mut __object_work)
                .await
                .map(|(values, _allocation)| values);
            __object_work.receipt(__object_result)
        },
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
        {
            let authority_input_0 = &[crate::storage::ObjectReadRequest {
                object_id: missing,
                maximum_bytes: 4,
            }];
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
            let __object_result = store
                .read_many(authority_input_0, &mut __object_work)
                .await
                .map(|(values, _allocation)| values);
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::Missing,
            ..
        })
    ));
    let mut denied = WorkBudget::UNBOUNDED;
    denied.object_probes = 0;
    assert!(matches!(
        {
            let authority_cancellation = &crate::CancellationToken::new();

            let mut __object_work = crate::AuthorityWork::new(denied, authority_cancellation);
            let __object_result = store.contains(object_id, &mut __object_work).await;
            __object_work.receipt(__object_result)
        },
        Err(ObjectFailure {
            error: ObjectStoreError::Work(_),
            ..
        })
    ));
    Ok(())
}
