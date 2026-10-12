use super::*;

#[tokio::test]
async fn object_store_verifies_identity_and_bounds_reads() -> Result<(), Box<dyn std::error::Error>>
{
    let cancel = crate::CancellationToken::new();

    let store = MemoryObjectStore::default();
    let object_id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"abcdef"),
    };
    store
        .put(
            object_id,
            OwnedBytes::from_static(b"abcdef"),
            &mut crate::AuthorityWork::new(B, &cancel),
        )
        .await?;
    assert_eq!(
        store
            .read(object_id, 6, &mut crate::AuthorityWork::new(B, &cancel))
            .await?,
        ObjectRead {
            bytes: OwnedBytes::from_static(b"abcdef"),
            retention: ObjectReadRetention::Shared,
        }
    );
    assert!(matches!(
        store
            .read(object_id, 5, &mut crate::AuthorityWork::new(B, &cancel))
            .await,
        Err(ObjectStoreError::TooLarge { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn object_batch_is_ordered_deduplicated_by_storage_and_one_backend_read()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = crate::CancellationToken::new();
    let mut batch_work = crate::AuthorityWork::new(B, &cancel);

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
            &mut crate::AuthorityWork::new(B, &cancel),
        )
        .await?;
    store
        .put(
            second,
            OwnedBytes::from_static(b"second"),
            &mut crate::AuthorityWork::new(B, &cancel),
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
            &mut batch_work,
        )
        .await
        .map(|(value, _allocation)| value)?;
    assert_eq!(
        receipt
            .iter()
            .map(|value| value.bytes.clone())
            .collect::<Vec<_>>(),
        vec![
            OwnedBytes::from_static(b"second"),
            OwnedBytes::from_static(b"first"),
            OwnedBytes::from_static(b"second"),
        ]
    );
    assert_eq!(batch_work.spent().backend_read_operations, 1);
    assert_eq!(batch_work.spent().object_probes, 3);
    assert_eq!(batch_work.spent().items_returned, 3);
    assert!(matches!(
        store
            .read_many(&[], &mut batch_work)
            .await
            .map(|(value, _allocation)| value),
        Err(ObjectStoreError::EmptyReadBatch)
    ));
    Ok(())
}

#[tokio::test]
async fn object_store_reauthenticates_shared_bytes_after_memory_corruption()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = crate::CancellationToken::new();
    let mut batch_work = crate::AuthorityWork::new(B, &cancel);

    let store = MemoryObjectStore::default();
    let object_id = ObjectId {
        kind: crate::storage::ObjectKind::BlobChunk,
        digest: object_digest(crate::storage::ObjectKind::BlobChunk, b"authentic"),
    };
    store
        .put(
            object_id,
            OwnedBytes::from_static(b"authentic"),
            &mut crate::AuthorityWork::new(B, &cancel),
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
            .read(object_id, 64, &mut crate::AuthorityWork::new(B, &cancel))
            .await,
        Err(ObjectStoreError::Corrupt)
    ));
    assert!(matches!(
        store
            .contains(object_id, &mut crate::AuthorityWork::new(B, &cancel))
            .await,
        Err(ObjectStoreError::Corrupt)
    ));
    let batch_failure = store
        .read_many(
            &[crate::storage::ObjectReadRequest {
                object_id,
                maximum_bytes: 64,
            }],
            &mut batch_work,
        )
        .await
        .map(|(value, _allocation)| value)
        .err()
        .ok_or("corrupt object batch unexpectedly succeeded")?;
    assert!(matches!(batch_failure, ObjectStoreError::Corrupt));
    assert_eq!(batch_work.spent().object_probes, 1);
    assert_eq!(batch_work.spent().backend_read_operations, 1);
    assert_eq!(batch_work.spent().object_bytes_read, 9);
    Ok(())
}

#[tokio::test]
async fn object_admission_rejects_size_identity_absence_and_batch_bounds()
-> Result<(), Box<dyn std::error::Error>> {
    let cancel = crate::CancellationToken::new();

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
                &mut crate::AuthorityWork::new(B, &cancel)
            )
            .await,
        Err(ObjectStoreError::TooLarge { .. })
    ));
    assert!(matches!(
        store
            .put(
                object_id,
                OwnedBytes::from_static(b"nope"),
                &mut crate::AuthorityWork::new(B, &cancel)
            )
            .await,
        Err(ObjectStoreError::DigestMismatch)
    ));
    assert!(
        !store
            .contains(object_id, &mut crate::AuthorityWork::new(B, &cancel))
            .await?
    );
    assert!(matches!(
        store
            .read(object_id, 4, &mut crate::AuthorityWork::new(B, &cancel))
            .await,
        Err(ObjectStoreError::Missing)
    ));
    store
        .put(
            object_id,
            OwnedBytes::from_static(b"data"),
            &mut crate::AuthorityWork::new(B, &cancel),
        )
        .await?;
    store
        .put(
            object_id,
            OwnedBytes::from_static(b"data"),
            &mut crate::AuthorityWork::new(B, &cancel),
        )
        .await?;
    assert!(matches!(
        store
            .read_many(
                &[crate::storage::ObjectReadRequest {
                    object_id,
                    maximum_bytes: 3,
                }],
                &mut crate::AuthorityWork::new(B, &cancel)
            )
            .await
            .map(|(value, _allocation)| value),
        Err(ObjectStoreError::TooLarge {
            observed: 4,
            maximum: 3,
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
                &mut crate::AuthorityWork::new(B, &cancel)
            )
            .await
            .map(|(value, _allocation)| value),
        Err(ObjectStoreError::Missing)
    ));
    let mut denied = B;
    denied.object_probes = 0;
    assert!(matches!(
        store
            .contains(object_id, &mut crate::AuthorityWork::new(denied, &cancel))
            .await,
        Err(ObjectStoreError::Work(_))
    ));
    Ok(())
}
