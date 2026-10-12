//! Immutable filesystem composition over the canonical Objects v1 provider.
#![cfg(feature = "distributed")]

use acyclic_fs::storage::{
    ObjectId, ObjectKind, ObjectReadRequest, ObjectStoreError, ObjectWrite, object_digest,
};
use acyclic_fs::{AsyncObjectStore, CancellationToken, LogicalObjectStore, WorkBudget};
use acyclic_objects::v1::{MemoryObjects, MemoryOptions, ObjectsProvider, wire};
use acyclic_stream::OwnedBytes;
use std::sync::Arc;

fn write(bytes: &'static [u8]) -> ObjectWrite {
    let bytes = OwnedBytes::from_static(bytes);
    ObjectWrite {
        object_id: ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &bytes),
        },
        bytes,
    }
}

#[tokio::test]
async fn v2_batches_preserve_filesystem_content_identity_and_backend_accounting()
-> Result<(), Box<dyn std::error::Error>> {
    let (provider, bucket) = MemoryObjects::with_bucket("fs-memory", MemoryOptions::default())?;
    let provider = Arc::new(provider);
    let store =
        LogicalObjectStore::new(Arc::clone(&provider), bucket.clone()).with_object_limit(8)?;
    let writes = [write(b"first"), write(b"second"), write(b"first")];
    let cancel = CancellationToken::new();
    let result = {
        let __object_args = (&writes, WorkBudget::UNBOUNDED, &cancel);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store.put_many(__object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
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
        let __object_args = (&reads, WorkBudget::UNBOUNDED, &cancel);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .read_many(__object_args.0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    }
    .map_err(|failure| failure.error)?;
    assert_eq!(result.work.backend_read_operations, 1);
    assert_eq!(result.work.object_probes, 3);
    let denied = {
        let __object_args = (
            &reads,
            WorkBudget {
                backend_read_operations: 0,
                ..WorkBudget::UNBOUNDED
            },
            &cancel,
        );
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store
            .read_many(__object_args.0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
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
    let present = {
        let __object_args = (
            writes.first().ok_or("write absent")?.object_id,
            WorkBudget::UNBOUNDED,
            &cancel,
        );
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store.contains(__object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }
    .map_err(|failure| failure.error)?;
    assert!(present.value);
    assert_eq!(
        (
            present.work.backend_read_operations,
            present.work.object_bytes_read,
            present.work.bytes_hashed
        ),
        (1, 5, 5)
    );
    let replay = {
        let __object_args = (&writes,
            WorkBudget::UNBOUNDED,
            &cancel,
        );
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
        let __object_result = store.put_many(__object_args.0, &mut __object_work).await;
        __object_work.receipt(__object_result)
    }
    .map_err(|failure| failure.error)?;
    assert_eq!(
        (
            replay.work.backend_write_operations,
            replay.work.backend_read_operations
        ),
        (1, 2)
    );
    let listed = provider
        .list(wire::ListObjectsRequest {
            bucket: Some(bucket),
            page_size: 10,
            ..Default::default()
        })
        .await?;
    assert_eq!(listed.entries.len(), 2);
    Ok(())
}

#[tokio::test]
async fn v2_fs_rejects_invalid_digest_oversize_and_cancellation_before_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let (provider, bucket) = MemoryObjects::with_bucket("fs-memory", MemoryOptions::default())?;
    let provider = Arc::new(provider);
    let store =
        LogicalObjectStore::new(Arc::clone(&provider), bucket.clone()).with_object_limit(4)?;
    let cancel = CancellationToken::new();
    let oversized = write(b"oversized");
    let failure = {
        let __object_args = (
            oversized.object_id,
            oversized.bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancel,
        );
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }
    .err()
        .ok_or("oversized put succeeded")?;
    assert_eq!(failure.work.backend_write_operations, 0);
    assert!(
        matches!(failure.error, ObjectStoreError::Rejected(ref reason)
        if reason == "Objects capacity exhausted")
    );
    assert!(
        {
            let __object_args = (&[oversized], WorkBudget::UNBOUNDED, &cancel);
            let mut __object_work =
                acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store.put_many(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }
        .is_err()
    );
    let mut invalid = write(b"data");
    invalid.bytes = OwnedBytes::from_static(b"bad");
    assert!(matches!(
        {
            let __object_args = (&[invalid], WorkBudget::UNBOUNDED, &cancel);
            let mut __object_work =
                acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store.put_many(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }
        .err()
            .map(|failure| failure.error),
        Some(ObjectStoreError::DigestMismatch)
    ));
    cancel.cancel();
    let valid = write(b"data");
    assert!(matches!(
        {
            let __object_args = (valid.object_id, valid.bytes, WorkBudget::UNBOUNDED, &cancel);
            let mut __object_work =
                acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .put(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }
        .err()
            .map(|failure| failure.error),
        Some(ObjectStoreError::Cancelled)
    ));
    let listed = provider
        .list(wire::ListObjectsRequest {
            bucket: Some(bucket),
            page_size: 10,
            ..Default::default()
        })
        .await?;
    assert!(listed.entries.is_empty());
    Ok(())
}

#[tokio::test]
async fn v2_fs_never_accepts_substituted_content_or_replays_collected_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let (provider, bucket) = MemoryObjects::with_bucket("fs-memory", MemoryOptions::default())?;
    let provider = Arc::new(provider);
    let store = LogicalObjectStore::new(Arc::clone(&provider), bucket.clone());
    let wanted = write(b"real");
    let cancel = CancellationToken::new();
    let key = format!(
        "fs/v1/{}/{}",
        wanted.object_id.kind.canonical_tag(),
        hex::encode(wanted.object_id.digest.as_bytes())
    );
    provider
        .put(
            wire::PutObjectHeader {
                bucket: Some(bucket.clone()),
                object_key: key.clone(),
                ..Default::default()
            },
            OwnedBytes::from_static(b"fake"),
        )
        .await?;
    assert!(matches!(
        {
            let __object_args = (
                wanted.object_id,
                wanted.bytes.clone(),
                WorkBudget::UNBOUNDED,
                &cancel,
            );
            let mut __object_work =
                acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .put(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }
        .err()
        .map(|failure| failure.error),
        Some(ObjectStoreError::Corrupt)
    ));
    assert!(matches!(
        {
            let __object_args = (wanted.object_id, 4, WorkBudget::UNBOUNDED, &cancel);
            let mut __object_work =
                acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
            let __object_result = store
                .read(__object_args.0, __object_args.1, &mut __object_work)
                .await;
            __object_work.receipt(__object_result)
        }
        .err()
        .map(|failure| failure.error),
        Some(ObjectStoreError::Corrupt)
    ));
    assert!(matches!(
        {
            let __object_args = (wanted.object_id,
                WorkBudget::UNBOUNDED,
                &cancel
            );
            let mut __object_work =
                acyclic_fs::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = store.contains(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }
        .err()
            .map(|failure| failure.error),
        Some(ObjectStoreError::Corrupt)
    ));
    provider
        .delete(wire::DeleteObjectRequest {
            bucket: Some(bucket.clone()),
            object_key: key.clone(),
            ..Default::default()
        })
        .await?;
    {
        let __object_args = (
            wanted.object_id,
            wanted.bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancel,
        );
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }
    .map_err(|failure| failure.error)?;
    provider
        .delete(wire::DeleteObjectRequest {
            bucket: Some(bucket),
            object_key: key,
            ..Default::default()
        })
        .await?;
    {
        let __object_args = (
            wanted.object_id,
            wanted.bytes.clone(),
            WorkBudget::UNBOUNDED,
            &cancel,
        );
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }
    .map_err(|failure| failure.error)?;
    let result = {
        let __object_args = (wanted.object_id, 4, WorkBudget::UNBOUNDED, &cancel);
        let mut __object_work = acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .read(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }
    .map_err(|failure| failure.error)?;
    assert_eq!(result.value.bytes, wanted.bytes);
    Ok(())
}
