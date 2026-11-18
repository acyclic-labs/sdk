use acyclic_fs::kernel::{
    GenerationTransferError, TransferCursor, export_generation_batch_async,
    import_generation_batch_async,
};
use acyclic_fs::{CancellationToken, MemoryObjectStore};
use acyclic_fs_work::performance::{WorkBudget, WorkCounters};

#[path = "../../../support/transfer_manifest.rs"]
mod transfer_manifest;
use transfer_manifest::fixture;

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
