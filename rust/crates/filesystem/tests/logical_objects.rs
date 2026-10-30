//! Immutable filesystem composition over the canonical Objects v1 provider.
#![cfg(feature = "distributed")]

#[macro_use]
#[path = "support/provider_receipt.rs"]
mod provider_receipt;

use acyclic_fs::storage::{
    ObjectId, ObjectKind, ObjectReadRequest, ObjectStoreError, ObjectWrite, object_digest,
};
use acyclic_fs::{AsyncObjectStore, CancellationToken, LogicalObjectStore, WorkBudget};
use acyclic_objects::{Blobs, Delete, MemoryObjects};
use acyclic_stream::OwnedBytes;
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
#[path = "logical_objects/sealed.rs"]
mod sealed;

fn write(bytes: &'static [u8]) -> ObjectWrite {
    let bytes = OwnedBytes::from_static(bytes);
    ObjectWrite {
        object_id: ObjectId {
            storage: None,
            kind: ObjectKind::BlobChunk,
            digest: object_digest(&bytes),
        },
        bytes,
    }
}

#[tokio::test]
async fn v2_batches_preserve_filesystem_content_identity_and_backend_accounting()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryObjects::memory());
    let store = LogicalObjectStore::new(Arc::clone(&provider)).with_object_limit(Some(8))?;
    let writes = [write(b"first"), write(b"second"), write(b"first")];
    let cancel = CancellationToken::new();
    let result = {
        let mut work = acyclic_fs::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        let result = store.put_many(&writes, &mut work).await;
        work.receipt(result)
    }
    .map_err(|failure| failure.error)?;
    assert_eq!(result.work.backend_write_operations, 1);
    let reads = writes
        .iter()
        .map(|write| ObjectReadRequest {
            object_id: write.object_id,
            maximum_bytes: 8,
        })
        .collect::<Vec<_>>();
    let result = {
        let mut work = acyclic_fs::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        let result = store
            .read_many(&reads, &mut work)
            .await
            .map(|(values, _allocation)| values);
        work.receipt(result)
    }
    .map_err(|failure| failure.error)?;
    assert_eq!(result.work.backend_read_operations, 1);
    assert_eq!(result.work.object_probes, 3);
    let denied = {
        let mut work = acyclic_fs::AuthorityWork::new(
            WorkBudget {
                backend_read_operations: 0,
                ..WorkBudget::UNBOUNDED
            },
            &cancel,
        );
        let result = store
            .read_many(&reads, &mut work)
            .await
            .map(|(values, _allocation)| values);
        work.receipt(result)
    }
    .err()
    .ok_or("zero backend budget admitted a batch")?;
    assert_eq!(denied.work.backend_read_operations, 0);
    assert!(matches!(denied.error, ObjectStoreError::Work(_)));
    assert_eq!(
        result
            .value
            .iter()
            .map(|read| &read.bytes)
            .collect::<Vec<_>>(),
        writes.iter().map(|write| &write.bytes).collect::<Vec<_>>()
    );
    assert_eq!(result.work.bytes_hashed, 16);
    let present = provider_receipt!(
        store,
        contains(writes.first().ok_or("write absent")?.object_id),
        WorkBudget::UNBOUNDED,
        &cancel
    )
    .map_err(|failure| failure.error)?;
    assert!(present.value);
    assert_eq!(
        (
            present.work.backend_read_operations,
            present.work.object_bytes_read,
            present.work.bytes_hashed
        ),
        (1, 0, 0)
    );
    let replay = {
        let mut work = acyclic_fs::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
        let result = store.put_many(&writes, &mut work).await;
        work.receipt(result)
    }
    .map_err(|failure| failure.error)?;
    assert_eq!(
        (
            replay.work.backend_write_operations,
            replay.work.backend_read_operations
        ),
        (1, 0)
    );
    assert_eq!(provider.list(None, 10).await?.digests.len(), 2);
    Ok(())
}

#[tokio::test]
async fn v2_fs_rejects_invalid_digest_oversize_and_cancellation_before_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryObjects::memory());
    let store = LogicalObjectStore::new(Arc::clone(&provider)).with_object_limit(Some(4))?;
    let cancel = CancellationToken::new();
    let oversized = write(b"oversized");
    let failure = provider_receipt!(
        store,
        put(oversized.object_id, oversized.bytes.clone()),
        WorkBudget::UNBOUNDED,
        &cancel
    )
    .err()
    .ok_or("oversized put succeeded")?;
    assert_eq!(failure.work.backend_write_operations, 0);
    assert!(matches!(
        failure.error,
        ObjectStoreError::TooLarge {
            observed: 9,
            maximum: 4,
        }
    ));
    assert!(
        provider_receipt!(
            store,
            put_many(&[oversized]),
            WorkBudget::UNBOUNDED,
            &cancel
        )
        .is_err()
    );
    let mut invalid = write(b"data");
    invalid.bytes = OwnedBytes::from_static(b"bad");
    assert!(matches!(
        provider_receipt!(store, put_many(&[invalid]), WorkBudget::UNBOUNDED, &cancel)
            .err()
            .map(|failure| failure.error),
        Some(ObjectStoreError::DigestMismatch)
    ));
    cancel.cancel();
    let valid = write(b"data");
    assert!(matches!(
        {
            let mut work = acyclic_fs::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
            let result = store.put(valid.object_id, valid.bytes, &mut work).await;
            work.receipt(result)
        }
        .err()
        .map(|failure| failure.error),
        Some(ObjectStoreError::Cancelled)
    ));
    assert!(provider.list(None, 10).await?.digests.is_empty());
    Ok(())
}

#[tokio::test]
async fn v2_fs_republishes_collected_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(MemoryObjects::memory());
    let store = LogicalObjectStore::new(Arc::clone(&provider));
    let wanted = write(b"real");
    let cancel = CancellationToken::new();
    let blob = acyclic_objects::Digest::from_bytes(*wanted.object_id.digest.as_bytes());
    for _ in 0..2 {
        provider_receipt!(
            store,
            put(wanted.object_id, wanted.bytes.clone()),
            WorkBudget::UNBOUNDED,
            &cancel
        )
        .map_err(|failure| failure.error)?;
        assert_eq!(provider.delete(blob).await?, Delete::Deleted);
        assert!(matches!(
            provider_receipt!(
                store,
                read(wanted.object_id, 4),
                WorkBudget::UNBOUNDED,
                &cancel
            )
            .err()
            .map(|failure| failure.error),
            Some(ObjectStoreError::Missing)
        ));
    }
    provider_receipt!(
        store,
        put(wanted.object_id, wanted.bytes.clone()),
        WorkBudget::UNBOUNDED,
        &cancel
    )
    .map_err(|failure| failure.error)?;
    let result = provider_receipt!(
        store,
        read(wanted.object_id, 4),
        WorkBudget::UNBOUNDED,
        &cancel
    )
    .map_err(|failure| failure.error)?;
    assert_eq!(result.value.bytes, wanted.bytes);
    Ok(())
}
