use super::*;
use crate::foundation::{Digest, FileId, VolumeId};
use crate::memory::MemoryObjectStore;
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn run_ready<F: Future>(future: F) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => Some(value),
        Poll::Pending => None,
    }
}

fn table(byte: u8) -> ObjectId {
    ObjectId {
        kind: ObjectKind::FileTablePage,
        digest: Digest::from_bytes([byte; 32]),
    }
}

fn put_root(
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
    })?;
    let object = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: object_digest(ObjectKind::GenerationRoot, &encoded),
    };
    store.put(object, Bytes::from(encoded), WorkBudget::UNBOUNDED)?;
    Ok(object)
}

#[test]
fn unchanged_checkpoint_reuses_base_without_a_write() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 1, 2, 3)?;
    let receipt = build_checkpoint(
        &store,
        CheckpointRequest {
            base,
            file_table: table(3),
            merge_parent: None,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(receipt.reused_base);
    assert_eq!(receipt.root, base);
    assert_eq!(receipt.work.backend_read_operations, 1);
    assert_eq!(receipt.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn unchanged_async_checkpoint_reuses_base_without_a_write() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 1, 2, 3)?;
    let receipt = run_ready(build_checkpoint_async(
        &store,
        CheckpointRequest {
            base,
            file_table: table(3),
            merge_parent: None,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed asynchronous checkpoint unexpectedly blocked")??;
    assert!(receipt.reused_base);
    assert_eq!(receipt.root, base);
    assert_eq!(receipt.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn changed_checkpoint_has_exact_parent_and_one_immutable_write()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 4, 5, 6)?;
    let receipt = build_checkpoint(
        &store,
        CheckpointRequest {
            base,
            file_table: table(7),
            merge_parent: None,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(!receipt.reused_base);
    assert_ne!(receipt.root, base);
    assert_eq!(receipt.work.backend_write_operations, 1);
    let bytes = store
        .read(receipt.root, u64::MAX, WorkBudget::UNBOUNDED)?
        .value;
    let root = decode_generation_root(&bytes, DecodeLimits::default())?;
    assert_eq!(root.file_table, table(7));
    assert_eq!(root.parents, vec![GenerationId::new(base.digest)]);
    Ok(())
}

#[test]
fn cross_volume_merge_fails_before_any_candidate_write() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 8, 9, 10)?;
    let other = put_root(&store, 11, 9, 12)?;
    let failure = build_checkpoint(
        &store,
        CheckpointRequest {
            base,
            file_table: table(13),
            merge_parent: Some(other),
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("cross-volume merge unexpectedly succeeded")?;
    assert!(matches!(failure.error, CheckpointError::CrossVolumeMerge));
    assert_eq!(failure.work.backend_read_operations, 2);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn async_and_sync_checkpoint_paths_are_semantically_and_physically_identical()
-> Result<(), Box<dyn std::error::Error>> {
    let sync_store = MemoryObjectStore::default();
    let async_store = MemoryObjectStore::default();
    let sync_base = put_root(&sync_store, 1, 2, 3)?;
    let async_base = put_root(&async_store, 1, 2, 3)?;
    assert_eq!(sync_base, async_base);
    let request = CheckpointRequest {
        base: sync_base,
        file_table: table(4),
        merge_parent: None,
    };
    let sync = build_checkpoint(
        &sync_store,
        request,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let asynchronous = run_ready(build_checkpoint_async(
        &async_store,
        request,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed asynchronous checkpoint unexpectedly blocked")??;
    assert_eq!(asynchronous, sync);
    Ok(())
}

#[test]
fn pre_cancelled_async_checkpoint_performs_zero_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 1, 2, 3)?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = run_ready(build_checkpoint_async(
        &store,
        CheckpointRequest {
            base,
            file_table: table(4),
            merge_parent: None,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory-backed asynchronous checkpoint unexpectedly blocked")?
    .err()
    .ok_or("cancelled checkpoint unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        CheckpointError::Storage(ObjectStoreError::Cancelled)
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn checkpoint_request_and_merge_identity_guards_are_total_before_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 1, 2, 3)?;
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
        )
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

    let other_root_identity = put_root(&store, 1, 9, 5)?;
    let mismatch = build_checkpoint(
        &store,
        CheckpointRequest {
            base,
            file_table: table(6),
            merge_parent: Some(other_root_identity),
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
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

#[test]
fn valid_merge_checkpoint_preserves_both_exact_parent_identities()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = put_root(&store, 1, 2, 3)?;
    let merge = put_root(&store, 1, 2, 4)?;
    let receipt = build_checkpoint(
        &store,
        CheckpointRequest {
            base,
            file_table: table(5),
            merge_parent: Some(merge),
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(!receipt.reused_base);
    assert_eq!(
        receipt.generation_id,
        GenerationId::new(receipt.root.digest)
    );
    assert_eq!(receipt.work.backend_read_operations, 2);
    assert_eq!(receipt.work.backend_write_operations, 1);
    let encoded = store
        .read(receipt.root, u64::MAX, WorkBudget::UNBOUNDED)?
        .value;
    let root = decode_generation_root(&encoded, DecodeLimits::default())?;
    assert_eq!(
        root.parents,
        vec![
            GenerationId::new(base.digest),
            GenerationId::new(merge.digest)
        ]
    );
    Ok(())
}
