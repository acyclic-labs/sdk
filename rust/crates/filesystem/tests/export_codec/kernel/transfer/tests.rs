use acyclic_fs::kernel::{
    ClosureLimits, DecodeLimits, FileKind, FilePayload, FileRecord, FileTablePage, GenerationRoot,
    GenerationTransferError, TransferCursor, TreePage, build_generation_export_manifest_async,
    export_generation_batch_async, import_generation_batch_async,
};
use acyclic_fs::{
    AsyncObjectStore, AuthorityWork, CancellationToken, FilesystemProfile, Lifecycle,
    MemoryObjectStore, VolumeConfig,
};
use acyclic_fs_format::{
    identity::{FileId, VolumeId},
    metadata::FileMetadata,
    object_reference::ObjectId,
};
use acyclic_fs_work::performance::{WorkBudget, WorkCounters};

use super::super::memory::memory_objects;
use super::super::tree_objects::put_tree_page;

#[path = "../../../support/transfer_manifest.rs"]
mod transfer_manifest;
use transfer_manifest::fixture;

use super::super::generation_objects::{
    put_file_metadata, put_file_table_page, put_generation_root,
};

#[tokio::test]
async fn terminal_invalid_and_cancelled_batches_do_no_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, _) = fixture();
    let store = memory_objects();
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
    assert_eq!(*invalid.work()?, WorkCounters::default());

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
    assert_eq!(*cancelled.work()?, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn transfer_page_bounds_are_total_and_terminal_import_is_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, _) = fixture();
    let store = memory_objects();
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
        assert_eq!(*failure.work()?, WorkCounters::default());
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
    assert_eq!(*budget_failure.work()?, WorkCounters::default());

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
    assert_eq!(*cancellation_failure.work()?, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn import_rejects_every_noncanonical_page_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, [chunk, root]) = fixture();
    let store = memory_objects();
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
            vec![chunk.clone()],
            0,
            GenerationTransferError::EmptyBatch,
        ),
        (
            TransferCursor::new(3),
            vec![chunk.clone()],
            1,
            GenerationTransferError::InvalidCursor,
        ),
        (
            TransferCursor::new(1),
            vec![chunk.clone(), root.clone()],
            2,
            GenerationTransferError::TooManyObjects,
        ),
        (
            TransferCursor::START,
            vec![chunk.clone(), root.clone()],
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
        assert_eq!(*failure.work()?, WorkCounters::default());
    }
    Ok(())
}

#[tokio::test]
async fn resumable_batches_preserve_manifest_order_and_idempotent_import()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, bodies) = fixture();
    let source = memory_objects();
    let destination = memory_objects();
    let cancellation = CancellationToken::new();
    for (object_id, body) in manifest.objects.iter().copied().zip(bodies.iter().cloned()) {
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        source.put(object_id, body, &mut work).await?;
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
    let first_object = first
        .value
        .objects
        .first()
        .ok_or("missing first transfer object")?;
    let [chunk, root] = &bodies;
    assert_eq!(first_object.bytes.as_ref(), chunk.as_ref());
    assert_eq!(first.work.backend_read_operations, 1);
    assert!(first.work.peak_allocation_bytes > 0);

    let imported = (import_generation_batch_async(
        &destination,
        &manifest,
        TransferCursor::START,
        std::slice::from_ref(&first_object.bytes),
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
        std::slice::from_ref(chunk),
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
    let second_object = second
        .value
        .objects
        .first()
        .ok_or("missing second transfer object")?;
    assert_eq!(second_object.bytes.as_ref(), root.as_ref());
    (import_generation_batch_async(
        &destination,
        &manifest,
        TransferCursor::new(1),
        std::slice::from_ref(&second_object.bytes),
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
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .await?;
        assert_eq!(read.bytes.as_ref(), body.as_ref());
    }
    Ok(())
}

async fn minimal_generation(
    store: &MemoryObjectStore,
    volume_id: VolumeId,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let metadata = put_file_metadata(store, FileMetadata::default()).await?;
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
async fn manifest_construction_rejects_foreign_volume_after_complete_authentication()
-> Result<(), Box<dyn std::error::Error>> {
    let store = memory_objects();
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
    assert!(mismatch.work()?.backend_read_operations > 0);
    assert!(mismatch.work()?.page_reads > 0);

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
