use super::*;
use crate::foundation::{Digest, FileId, VolumeId};
use crate::memory::MemoryObjectStore;
use crate::AsyncObjectStore;
use crate::kernel::decode_generation_root;

fn table(byte: u8) -> ObjectId {
    ObjectId {
        kind: ObjectKind::FileTablePage,
        digest: Digest::from_bytes([byte; 32]),
    }
}

async fn put_root(
    store: &MemoryObjectStore,
    volume_byte: u8,
    root_file_byte: u8,
    table_byte: u8,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let encoded = encode_generation_root(&GenerationRoot {
        volume_id: VolumeId::from_bytes([volume_byte; 16]),
        root_file_id: FileId::from_bytes([root_file_byte; 16]),
        file_table: table(table_byte),
        parents: Vec::new(),
        required_features: 0,
    }, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?;
    let object = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: object_digest(ObjectKind::GenerationRoot, &encoded),
    };
    store.put(object, acyclic_stream::OwnedBytes::from(encoded), WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
    Ok(object)
}

#[tokio::test]
async fn unchanged_checkpoint_reuses_base_without_a_write() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 1, 2, 3).await?;
    let receipt = build_checkpoint(
        &store,
        CheckpointRequest {
            base,
            file_table: table(3),
            merge_parent: None,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ).await?;
    assert_eq!(receipt.value, base);
    assert_eq!(receipt.work.backend_read_operations, 1);
    assert_eq!(receipt.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn changed_checkpoint_has_exact_parent_and_one_immutable_write()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 4, 5, 6).await?;
    let receipt = build_checkpoint(
        &store,
        CheckpointRequest {
            base,
            file_table: table(7),
            merge_parent: None,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ).await?;
    assert_ne!(receipt.value, base);
    assert_eq!(receipt.work.backend_write_operations, 1);
    let bytes = store
        .read(receipt.value, u64::MAX, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?
        .value;
    let root = decode_generation_root(&bytes, DecodeLimits::default(), |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
    assert_eq!(root.file_table, table(7));
    assert_eq!(root.parents, vec![GenerationId::new(base.digest)]);
    Ok(())
}

#[tokio::test]
async fn cross_volume_merge_fails_before_any_candidate_write() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 8, 9, 10).await?;
    let other = put_root(&store, 11, 9, 12).await?;
    let failure = build_checkpoint(
        &store,
        CheckpointRequest {
            base,
            file_table: table(13),
            merge_parent: Some(other),
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("cross-volume merge unexpectedly succeeded")?;
    assert!(matches!(failure.error, CheckpointError::CrossVolumeMerge));
    assert_eq!(failure.work.backend_read_operations, 2);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_checkpoint_performs_zero_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 1, 2, 3).await?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = build_checkpoint(
        &store,
        CheckpointRequest {
            base,
            file_table: table(4),
            merge_parent: None,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await
    .err()
    .ok_or("cancelled checkpoint unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        CheckpointError::Storage(ObjectStoreError::Cancelled)
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn checkpoint_request_and_merge_identity_guards_are_total_before_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 1, 2, 3).await?;
    let wrong_generation = ObjectId {
        kind: ObjectKind::Blob,
        digest: base.digest,
    };
    for (request, expected) in [
        (
            CheckpointRequest {
                base: wrong_generation,
                file_table: table(4),
                merge_parent: None,
            },
            "generation",
        ),
        (
            CheckpointRequest {
                base,
                file_table: ObjectId {
                    kind: ObjectKind::Blob,
                    digest: Digest::ZERO,
                },
                merge_parent: None,
            },
            "table",
        ),
        (
            CheckpointRequest {
                base,
                file_table: table(4),
                merge_parent: Some(base),
            },
            "duplicate",
        ),
        (
            CheckpointRequest {
                base,
                file_table: table(4),
                merge_parent: Some(wrong_generation),
            },
            "generation",
        ),
    ] {
        let failure = build_checkpoint(
            &store,
            request,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("invalid checkpoint request succeeded")?;
        assert!(match expected {
            "generation" => matches!(failure.error, CheckpointError::WrongGenerationKind),
            "table" => matches!(failure.error, CheckpointError::WrongFileTableKind),
            "duplicate" => matches!(failure.error, CheckpointError::DuplicateParent),
            _ => false,
        });
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let other_root_identity = put_root(&store, 1, 9, 5).await?;
    let mismatch = build_checkpoint(
        &store,
        CheckpointRequest {
            base,
            file_table: table(6),
            merge_parent: Some(other_root_identity),
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("root-identity-mismatched merge succeeded")?;
    assert!(matches!(
        mismatch.error,
        CheckpointError::RootIdentityMismatch
    ));
    assert_eq!(mismatch.work.backend_read_operations, 2);
    assert_eq!(mismatch.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn valid_merge_checkpoint_preserves_both_exact_parent_identities()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 1, 2, 3).await?;
    let merge = put_root(&store, 1, 2, 4).await?;
    let receipt = build_checkpoint(
        &store,
        CheckpointRequest {
            base,
            file_table: table(5),
            merge_parent: Some(merge),
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ).await?;
    assert_ne!(receipt.value, base);
    assert_eq!(receipt.work.backend_read_operations, 2);
    assert_eq!(receipt.work.backend_write_operations, 1);
    let encoded = store
        .read(receipt.value, u64::MAX, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?
        .value;
    let root = decode_generation_root(&encoded, DecodeLimits::default(), |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
    assert_eq!(
        root.parents,
        vec![
            GenerationId::new(base.digest),
            GenerationId::new(merge.digest)
        ]
    );
    Ok(())
}
