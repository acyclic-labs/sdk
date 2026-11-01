use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::{FileId, GenerationId, VolumeId};
use crate::kernel::{
    DecodeLimits, FileKind, FileMetadata, FilePayload, FileRecord, FileTablePage, GenerationRoot,
    MetadataField, TreePage,
};
use crate::memory::MemoryObjectStore;
use crate::model::{FilesystemProfile, Lifecycle, VolumeConfig};
use crate::storage::{ObjectKind, object_digest};

fn fixture() -> (GenerationExportManifest, [acyclic_stream::OwnedBytes; 2]) {
    let chunk = acyclic_stream::OwnedBytes::from_static(b"transfer-chunk");
    let root_body = acyclic_stream::OwnedBytes::from_static(b"transfer-root");
    let chunk_id = ObjectId {
        storage: None,
        kind: ObjectKind::BlobChunk,
        digest: object_digest(&chunk),
    };
    let root_id = ObjectId {
        storage: None,
        kind: ObjectKind::GenerationRoot,
        digest: object_digest(&root_body),
    };
    (
        GenerationExportManifest {
            volume_id: VolumeId::from_bytes([7; 16]),
            config: VolumeConfig::portable(Lifecycle::Ephemeral),
            generation_root: root_id,
            generation_id: GenerationId::from_root(root_id),
            objects: vec![chunk_id, root_id],
            file_count: 1,
        },
        [chunk, root_body],
    )
}

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
        maximum_objects: 16,
        maximum_files: 8,
        maximum_object_bytes: 1024 * 1024,
        profile: FilesystemProfile::Portable,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
    }
}

#[tokio::test]
async fn resumable_batches_preserve_manifest_order_and_idempotent_import()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, bodies) = fixture();
    let source = MemoryObjectStore::default();
    let destination = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    for (object_id, body) in manifest.objects.iter().copied().zip(bodies.iter().cloned()) {
        (async {
            let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let result = AsyncObjectStore::put(&source, object_id, body, &mut work).await;
            work.receipt(result)
        })
        .await?;
    }

    let first = (export_generation_batch_async(
        &source,
        &manifest,
        TransferCursor::START,
        1,
        manifest.config.limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(first.value.first_object, TransferCursor::START);
    assert_eq!(first.value.next, Some(TransferCursor::new(1)));
    assert_eq!(first.value.objects.len(), 1);
    assert_eq!(first.value.objects[0].bytes.as_ref(), bodies[0].as_ref());
    assert_eq!(first.work.backend_read_operations, 1);
    assert!(first.work.peak_allocation_bytes > 0);

    let imported = (import_generation_batch_async(
        &destination,
        &manifest,
        TransferCursor::START,
        &[first.value.objects[0].bytes.clone()],
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(imported.value, TransferCursor::new(1));
    let repeated = (import_generation_batch_async(
        &destination,
        &manifest,
        TransferCursor::START,
        &[bodies[0].clone()],
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(repeated.value, TransferCursor::new(1));

    let second = (export_generation_batch_async(
        &source,
        &manifest,
        TransferCursor::new(1),
        8,
        manifest.config.limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(second.value.next, None);
    assert_eq!(second.value.objects[0].bytes.as_ref(), bodies[1].as_ref());
    (import_generation_batch_async(
        &destination,
        &manifest,
        TransferCursor::new(1),
        &[second.value.objects[0].bytes.clone()],
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    for (object_id, body) in manifest.objects.iter().copied().zip(bodies) {
        let read = AsyncObjectStore::read(
            &destination,
            object_id,
            manifest.config.limits.maximum_object_bytes,
            &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
        )
        .await?;
        assert_eq!(read.bytes.as_ref(), body.as_ref());
    }
    Ok(())
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
            first = reference.spent();
        }
    }
    let expected = reference.spent();
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
        failure.work.verify(bound)?;
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
        let paused = crate::test_support::PausedStore {
            inner: std::sync::Arc::new(MemoryObjectStore::default()),
            release: std::sync::Mutex::new(Some(receiver)),
        };
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
        assert_eq!(*failure.work, first);
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
async fn terminal_invalid_and_cancelled_batches_do_no_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, _) = fixture();
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let terminal = (export_generation_batch_async(
        &store,
        &manifest,
        TransferCursor::new(2),
        1,
        manifest.config.limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(terminal.value.objects.is_empty());
    assert_eq!(terminal.work, WorkCounters::default());

    let invalid = (export_generation_batch_async(
        &store,
        &manifest,
        TransferCursor::new(3),
        1,
        manifest.config.limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("invalid transfer cursor unexpectedly succeeded")?;
    assert!(matches!(
        invalid.error,
        GenerationTransferError::InvalidCursor
    ));
    assert_eq!(*invalid.work, WorkCounters::default());

    cancellation.cancel();
    let cancelled = (import_generation_batch_async(
        &store,
        &manifest,
        TransferCursor::START,
        &[acyclic_stream::OwnedBytes::from_static(b"ignored")],
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("cancelled transfer unexpectedly succeeded")?;
    assert!(matches!(
        cancelled.error,
        GenerationTransferError::Cancelled(_)
    ));
    assert_eq!(*cancelled.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn transfer_page_bounds_are_total_and_terminal_import_is_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, _) = fixture();
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();

    for (maximum_objects, maximum_object_bytes) in
        [(0, manifest.config.limits.maximum_object_bytes), (1, 0)]
    {
        let failure = (export_generation_batch_async(
            &store,
            &manifest,
            TransferCursor::START,
            maximum_objects,
            maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("zero export bound was accepted")?;
        assert!(matches!(failure.error, GenerationTransferError::EmptyBatch));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let terminal = (import_generation_batch_async(
        &store,
        &manifest,
        TransferCursor::new(2),
        &[],
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(terminal.value, TransferCursor::new(2));
    assert_eq!(terminal.work, WorkCounters::default());

    let tiny_budget = WorkBudget {
        bytes_copied: 0,
        ..WorkBudget::UNBOUNDED
    };
    let budget_failure = (export_generation_batch_async(
        &store,
        &manifest,
        TransferCursor::START,
        1,
        manifest.config.limits.maximum_object_bytes,
        tiny_budget,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("transfer request-vector copy escaped its work budget")?;
    assert!(matches!(
        budget_failure.error,
        GenerationTransferError::Work(_)
    ));
    assert_eq!(*budget_failure.work, WorkCounters::default());

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancellation_failure = (export_generation_batch_async(
        &store,
        &manifest,
        TransferCursor::START,
        1,
        manifest.config.limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancelled,
    ))
    .await
    .err()
    .ok_or("cancelled export succeeded")?;
    assert!(matches!(
        cancellation_failure.error,
        GenerationTransferError::Cancelled(_)
    ));
    assert_eq!(*cancellation_failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn import_rejects_every_noncanonical_page_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, fixture_bodies) = fixture();
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    for (cursor, bodies, maximum_objects, expected) in [
        (
            TransferCursor::START,
            Vec::<acyclic_stream::OwnedBytes>::new(),
            1,
            GenerationTransferError::EmptyBatch,
        ),
        (
            TransferCursor::START,
            vec![fixture_bodies[0].clone()],
            0,
            GenerationTransferError::EmptyBatch,
        ),
        (
            TransferCursor::new(3),
            vec![fixture_bodies[0].clone()],
            1,
            GenerationTransferError::InvalidCursor,
        ),
        (
            TransferCursor::new(1),
            vec![fixture_bodies[0].clone(), fixture_bodies[1].clone()],
            2,
            GenerationTransferError::TooManyObjects,
        ),
        (
            TransferCursor::START,
            vec![fixture_bodies[0].clone(), fixture_bodies[1].clone()],
            1,
            GenerationTransferError::TooManyObjects,
        ),
    ] {
        let failure = (import_generation_batch_async(
            &store,
            &manifest,
            cursor,
            &bodies,
            maximum_objects,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .await
        .err()
        .ok_or("invalid import page was accepted")?;
        assert_eq!(
            std::mem::discriminant(&failure.error),
            std::mem::discriminant(&expected)
        );
        assert_eq!(*failure.work, WorkCounters::default());
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
    assert!(mismatch.work.backend_read_operations > 0);
    assert!(mismatch.work.page_reads > 0);

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
