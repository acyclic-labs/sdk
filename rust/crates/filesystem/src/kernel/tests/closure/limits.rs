#[tokio::test]
async fn closure_limits_and_typed_root_are_enforced_at_the_exact_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, _, metadata) = minimal_generation(&store, None).await?;
    let wrong_kind = prove_generation_closure(
        &store,
        metadata,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("metadata object was admitted as a generation")?;
    assert!(matches!(wrong_kind.error, ClosureError::WrongObjectKind));
    assert_eq!(*wrong_kind.work, WorkCounters::default());

    let mut object_limited = limits();
    object_limited.maximum_objects = 0;
    let too_many_objects = prove_generation_closure(
        &store,
        generation,
        object_limited,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("zero-object closure limit succeeded")?;
    assert!(matches!(
        too_many_objects.error,
        ClosureError::TooManyObjects {
            observed: 1,
            maximum: 0,
        }
    ));
    assert_eq!(*too_many_objects.work, WorkCounters::default());

    let mut byte_limited = limits();
    byte_limited.maximum_object_bytes = 0;
    let too_many_bytes = prove_generation_closure(
        &store,
        generation,
        byte_limited,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("zero-byte closure limit succeeded")?;
    assert!(matches!(
        too_many_bytes.error,
        ClosureError::ClosureBytesExceeded { maximum: 0, .. }
    ));
    assert_eq!(too_many_bytes.work.object_probes, 1);

    let mut file_limited = limits();
    file_limited.maximum_files = 0;
    let too_many_files = prove_generation_closure(
        &store,
        generation,
        file_limited,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("zero-file closure limit succeeded")?;
    assert!(matches!(
        too_many_files.error,
        ClosureError::TooManyFiles {
            observed: 1,
            maximum: 0,
        }
    ));
    assert_eq!(too_many_files.work.page_reads, 1);
    Ok(())
}

#[tokio::test]
async fn extent_closure_rejects_missing_out_of_bounds_and_unavailable_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let missing_store = MemoryObjectStore::default();
    let missing_blob = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([9; 32]),
    };
    let missing_generation = generation_with_regular_extent(
        &missing_store,
        ExtentKind::Content {
            object: missing_blob,
            object_offset: 0,
        },
        4,
    )
    .await?;
    let missing = prove_generation_closure(
        &missing_store,
        missing_generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("missing extent blob unexpectedly proved")?;
    assert!(matches!(
        missing.error,
        ClosureError::Storage(ObjectStoreError::Missing)
    ));
    assert!(missing.work.object_probes > 0);

    let outside_store = MemoryObjectStore::default();
    let four_bytes = blob_with_bytes(&outside_store, b"data").await?;
    let outside_generation = generation_with_regular_extent(
        &outside_store,
        ExtentKind::Content {
            object: four_bytes,
            object_offset: 3,
        },
        2,
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &outside_store,
            outside_generation,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await,
        Err(OperationFailure {
            error: ClosureError::BlobSpanOutsideObject,
            ..
        })
    ));

    for sparse_kind in [ExtentKind::Hole, ExtentKind::AllocatedZero] {
        let sparse_store = MemoryObjectStore::default();
        let sparse_generation =
            generation_with_regular_extent(&sparse_store, sparse_kind, 4).await?;
        let mut dense_limits = limits();
        dense_limits.sparse_files = false;
        assert!(matches!(
            prove_generation_closure(
                &sparse_store,
                sparse_generation,
                dense_limits,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await,
            Err(OperationFailure {
                error: ClosureError::UnsupportedVolumeSemantics,
                ..
            })
        ));
    }
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_closure_does_no_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, _, _) = minimal_generation(&store, None).await?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = (prove_generation_closure(
        &store,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("cancelled asynchronous closure unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ClosureError::Storage(ObjectStoreError::Cancelled)
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn owned_generation_closure_reads_release_all_retained_backend_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let (generation, _, _) = minimal_generation(&store.inner, None).await?;
    let proof = (prove_generation_closure(
        &store,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(proof.file_count, 1);
    assert_eq!(u64::try_from(proof.objects.len()).unwrap_or(u64::MAX), 4);
    assert!(proof.work.bytes_copied > 0);
    assert!(proof.work.peak_allocation_bytes > 0);
    Ok(())
}

#[tokio::test]
async fn missing_metadata_preserves_backend_failure_work() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let missing = ObjectId {
        kind: ObjectKind::Metadata,
        digest: Digest::from_bytes([7; 32]),
    };
    let tree = put_tree_page(
        &store,
        &TreePage::Leaf(Vec::new()),
        8,
    )
    .await?;
    let table = put_file_table_page(
        &store,
        &FileTablePage::Leaf(vec![FileRecord {
            file_id: FileId::from_bytes([1; 16]),
            kind: FileKind::Directory,
            link_count: 1,
            metadata: missing,
            payload: FilePayload::Directory { entries: tree },
        }]),
        8,
    )
    .await?;
    let (generation, _, _) = minimal_generation(&store, Some(table)).await?;
    let failure = prove_generation_closure(
        &store,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("missing metadata unexpectedly proved")?;
    assert!(matches!(
        failure.error,
        ClosureError::Storage(ObjectStoreError::Missing)
    ));
    assert!(failure.work.object_probes >= 3);
    Ok(())
}
