use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::{FileId, VolumeId};
use crate::kernel::{
    DecodeLimits, FileKind, FileMetadata, FilePayload, FileRecord, FileTablePage, GenerationRoot,
    TreePage,
};
use crate::memory::MemoryObjectStore;
use crate::model::{FilesystemProfile, Lifecycle, VolumeConfig};

#[path = "../../../tests/support/transfer_manifest.rs"]
mod transfer_manifest;
use transfer_manifest::fixture;

fn empty_metadata() -> FileMetadata {
    FileMetadata::default()
}

use crate::test_support::{
    put_file_metadata, put_file_table_page, put_generation_root, put_tree_page,
};

async fn minimal_generation(
    store: &MemoryObjectStore,
    volume_id: VolumeId,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let metadata = put_file_metadata(store, empty_metadata()).await?;
    let entries = put_tree_page(store, &TreePage::Leaf(Vec::new()), 8).await?;
    let root_file_id = FileId::from_bytes([41; 16]);
    let table = put_file_table_page(
        store,
        &FileTablePage::Leaf(vec![FileRecord {
            bindings: None,
            file_id: root_file_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata,
            payload: FilePayload::Directory { entries },
        }]),
        8,
    )
    .await?;
    put_generation_root(
        store,
        &GenerationRoot {
            volume_id,
            root_file_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        },
    )
    .await
}

fn closure_limits() -> ClosureLimits {
    ClosureLimits {
        decode: DecodeLimits::default(),
        profile: FilesystemProfile::Portable,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
    }
}

#[tokio::test]
async fn import_keeps_cumulative_work_live_backing_and_cancelled_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, bodies) = fixture();
    let cancellation = CancellationToken::new();
    let reference_store = MemoryObjectStore::default();
    let mut reference =
        crate::AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    let mut first = WorkCounters::UNCHARGED;
    for (index, (id, body)) in manifest.objects.iter().zip(&bodies).enumerate() {
        reference_store
            .put_stored(*id, body.clone(), &mut reference)
            .await?;
        if index == 0 {
            first = reference.spent().unwrap();
        }
    }
    let expected = reference.spent().unwrap();
    let live = reference.allocations.live_bytes();
    assert!(
        live > 0,
        "destination backing must retain its original issuer"
    );
    let budget = WorkBudget {
        backend_write_operations: bodies.len() as u64,
        peak_allocation_bytes: expected.peak_allocation_bytes,
        ..crate::performance::BOUNDARY_BUDGET
    };
    let imported = MemoryObjectStore::default();
    let receipt = import_generation_batch_async(
        &imported,
        &manifest,
        TransferCursor::START,
        &bodies,
        2,
        budget,
        &cancellation,
    )
    .await?;
    assert_eq!(receipt.value, TransferCursor::new(2));
    assert_eq!(
        receipt.work, expected,
        "one original frame funds the whole page"
    );
    for (id, body) in manifest.objects.iter().zip(&bodies) {
        let read = imported
            .read(*id, body.len() as u64, &mut reference)
            .await?;
        assert_eq!(read.bytes, *body);
    }
    drop(reference_store);
    assert!(reference.allocations.live_bytes() < live);

    // The deciding write is denied against the same cumulative allowance.
    for (bound, counter) in [
        (
            WorkBudget {
                backend_write_operations: 1,
                ..budget
            },
            "backend_write_operations",
        ),
        (
            WorkBudget {
                peak_allocation_bytes: expected.peak_allocation_bytes - 1,
                ..budget
            },
            "peak_allocation_bytes",
        ),
    ] {
        let limited = MemoryObjectStore::default();
        let failure = import_generation_batch_async(
            &limited,
            &manifest,
            TransferCursor::START,
            &bodies,
            2,
            bound,
            &cancellation,
        )
        .await
        .err()
        .ok_or("a second write escaped its original credit")?;
        assert!(matches!(
            failure.error,
            GenerationTransferError::Object(ObjectStoreError::Work(WorkError::BudgetExceeded {
                counter: observed, ..
            })) if observed == counter
        ));
        failure.work.as_deref().unwrap().verify(bound)?;
        assert!(matches!(
            limited
                .read(manifest.objects[1], 1024, &mut reference)
                .await,
            Err(ObjectStoreError::Missing)
        ));
    }

    // The actual first write settles before caller or completion-producer loss.
    for cancelled in [false, true] {
        let token = CancellationToken::new();
        let (producer, receiver) = tokio::sync::oneshot::channel();
        let paused = crate::test_support::PausedStore::new(
            std::sync::Arc::new(MemoryObjectStore::default()),
            Some(receiver),
        );
        let mut importing = Box::pin(import_generation_batch_async(
            &paused,
            &manifest,
            TransferCursor::START,
            &bodies,
            2,
            budget,
            &token,
        ));
        assert!(futures::poll!(importing.as_mut()).is_pending());
        if cancelled {
            token.cancel();
            producer
                .send(())
                .map_err(|_| "import lost its original completion")?;
        } else {
            drop(producer);
        }
        let failure = importing.await.err().ok_or("withdrawn import continued")?;
        if cancelled {
            assert!(matches!(
                failure.error,
                GenerationTransferError::Cancelled(_)
            ));
        } else {
            assert!(matches!(failure.error, GenerationTransferError::Object(
                ObjectStoreError::Rejected(ref cause)
            ) if cause == "cache fill producer lost"));
        }
        assert_eq!(*failure.work().unwrap(), first);
        let read = paused
            .inner
            .read(manifest.objects[0], 1024, &mut reference)
            .await?;
        assert_eq!(read.bytes, bodies[0]);
        assert!(matches!(
            paused
                .inner
                .read(manifest.objects[1], 1024, &mut reference)
                .await,
            Err(ObjectStoreError::Missing)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn manifest_construction_rejects_foreign_volume_after_complete_authentication()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let actual_volume = VolumeId::from_bytes([42; 16]);
    let generation = minimal_generation(&store, actual_volume).await?;
    let cancellation = CancellationToken::new();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);

    let mismatch = (build_generation_export_manifest_async(
        &store,
        VolumeId::from_bytes([43; 16]),
        config,
        generation,
        closure_limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("foreign-volume generation unexpectedly produced a manifest")?;
    assert!(matches!(
        mismatch.error,
        GenerationTransferError::ManifestMismatch
    ));
    assert!(mismatch.work().unwrap().backend_read_operations > 0);
    assert!(mismatch.work().unwrap().page_reads > 0);

    let manifest = (build_generation_export_manifest_async(
        &store,
        actual_volume,
        config,
        generation,
        closure_limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(manifest.value.volume_id, actual_volume);
    assert_eq!(manifest.value.generation_root, generation);
    assert_eq!(manifest.value.file_count, 1);
    assert_eq!(manifest.value.objects.last(), Some(&generation));
    assert_eq!(manifest.work.backend_read_operations, 4);
    assert_eq!(manifest.work.page_reads, 2);
    Ok(())
}
