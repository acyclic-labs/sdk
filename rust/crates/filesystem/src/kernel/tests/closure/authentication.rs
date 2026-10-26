#[tokio::test]
async fn minimal_generation_has_a_complete_authenticated_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, _, _) = minimal_generation(&store, None).await?;
    let proof = prove_generation_closure(
        &store,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(proof.file_count, 1);
    assert_eq!(proof.logical_file_bytes, 0);
    assert_eq!(u64::try_from(proof.objects.len()).unwrap_or(u64::MAX), 4);
    assert_eq!(proof.root.root_file_id, FileId::from_bytes([1; 16]));
    Ok(())
}

#[tokio::test]
async fn authenticated_generation_counts_sparse_logical_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let generation = generation_with_regular_extent(&store, ExtentKind::Hole, 4096).await?;
    let proof = prove_generation_closure(
        &store,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(proof.file_count, 2);
    assert_eq!(proof.logical_file_bytes, 4096);
    Ok(())
}

#[tokio::test]
async fn authenticated_generation_counts_hardlinked_inline_file_once()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata_id = put_file_metadata(
        &store,
        metadata(),
    )
    .await?;
    let root_id = FileId::from_bytes([1; 16]);
    let file_id = FileId::from_bytes([2; 16]);
    let entries = put_tree_page(
        &store,
        &TreePage::Leaf(vec![
            TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"first".to_vec(), 255)?,
                file_id,
                kind: FileKind::Regular,
            },
            TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"second".to_vec(), 255)?,
                file_id,
                kind: FileKind::Regular,
            },
        ]),
        8,
    )
    .await?;
    let generation = generation_with_records(
        &store,
        root_id,
        vec![
            directory_record(root_id, 1, metadata_id, entries),
            FileRecord {
                file_id,
                kind: FileKind::Regular,
                link_count: 2,
                metadata: metadata_id,
                payload: FilePayload::InlineRegular(InlineFileData::new(b"hello")?),
            },
        ],
    )
    .await?;
    let proof = prove_generation_closure(
        &store,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(proof.logical_file_bytes, 5);
    Ok(())
}

#[tokio::test]
async fn metadata_attributes_and_internal_blobs_are_fully_authenticated()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let value = internal_blob(&store).await?;
    let first_leaf = put_attribute_page(
        &store,
        &AttributePage::Leaf(vec![AttributeEntry {
            name: attribute_name("a")?,
            value_bytes: 4,
            value,
        }]),
        8,
    )
    .await?;
    let second_leaf = put_attribute_page(
        &store,
        &AttributePage::Leaf(vec![AttributeEntry {
            name: attribute_name("z")?,
            value_bytes: 4,
            value,
        }]),
        8,
    )
    .await?;
    let attributes = put_attribute_page(
        &store,
        &AttributePage::Internal(vec![
            Child {
                first: attribute_name("a")?,
                page: first_leaf,
            },
            Child {
                first: attribute_name("z")?,
                page: second_leaf,
            },
        ]),
        8,
    )
    .await?;
    let mut complete_metadata = metadata();
    complete_metadata.named_attributes = MetadataField::Value(attributes);
    complete_metadata.acl = MetadataField::Value(value);
    complete_metadata.security_descriptor = MetadataField::Value(value);
    let metadata_id = put_file_metadata(
        &store,
        complete_metadata,
    )
    .await?;
    let tree = put_tree_page(
        &store,
        &TreePage::Leaf(Vec::new()),
        8,
    )
    .await?;
    let root_id = FileId::from_bytes([1; 16]);
    let generation = generation_with_records(
        &store,
        root_id,
        vec![directory_record(root_id, 1, metadata_id, tree)],
    )
    .await?;
    let proof = prove_generation_closure(
        &store,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(proof.file_count, 1);
    assert!(u64::try_from(proof.objects.len()).unwrap_or(u64::MAX) >= 12);
    assert_eq!(
        proof
            .objects
            .iter()
            .filter(|object| **object == value)
            .count(),
        1
    );
    let cached = CachedObjectStore::new(
        store,
        ObjectCacheOptions {
            maximum_entries: 64,
            maximum_bytes: 1024 * 1024,
            maximum_in_flight: 4,
            maximum_waiters_per_object: 4,
        },
    )?;
    let cancellation = CancellationToken::new();
    let cached_proof = (prove_generation_closure(
        &cached,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(cached_proof.objects, proof.objects);
    let warm = (prove_generation_closure(
        &cached,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(warm.objects, proof.objects);
    assert_eq!(warm.work.backend_read_operations, 0);
    Ok(())
}

#[tokio::test]
async fn cached_closure_proves_sparse_extent_and_blob_without_duplicate_backend_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::default();
    let blob = blob_with_bytes(&backend, b"data").await?;
    let generation = generation_with_regular_extent(
        &backend,
        ExtentKind::Content {
            object: blob,
            object_offset: 0,
        },
        4,
    )
    .await?;
    let cached = CachedObjectStore::new(
        backend,
        ObjectCacheOptions {
            maximum_entries: 32,
            maximum_bytes: 1024 * 1024,
            maximum_in_flight: 4,
            maximum_waiters_per_object: 4,
        },
    )?;
    let cancellation = CancellationToken::new();
    let cold = (prove_generation_closure(
        &cached,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(cold.file_count, 2);
    assert!(cold.work.backend_read_operations > 0);
    let warm = (prove_generation_closure(
        &cached,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(warm.objects, cold.objects);
    assert_eq!(warm.work.backend_read_operations, 0);
    assert_eq!(warm.work.object_bytes_read, 0);
    Ok(())
}

#[tokio::test]
async fn attribute_value_length_mismatch_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let value = blob_with_bytes(&store, b"data").await?;
    let attributes = put_attribute_page(
        &store,
        &AttributePage::Leaf(vec![AttributeEntry {
            name: attribute_name("wrong-length")?,
            value_bytes: 5,
            value,
        }]),
        8,
    )
    .await?;
    let mut complete_metadata = metadata();
    complete_metadata.named_attributes = MetadataField::Value(attributes);
    let metadata_id = put_file_metadata(
        &store,
        complete_metadata,
    )
    .await?;
    let tree = put_tree_page(
        &store,
        &TreePage::Leaf(Vec::new()),
        8,
    )
    .await?;
    let root_id = FileId::from_bytes([1; 16]);
    let generation = generation_with_records(
        &store,
        root_id,
        vec![directory_record(root_id, 1, metadata_id, tree)],
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &store,
            generation,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await,
        Err(OperationFailure {
            error: ClosureError::BlobLengthMismatch,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn symbolic_link_and_reparse_payloads_are_fully_proved_and_length_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata_id = put_file_metadata(
        &store,
        metadata(),
    )
    .await?;
    let payload = blob_with_bytes(&store, b"data").await?;
    let root_id = FileId::from_bytes([1; 16]);
    let symlink_id = FileId::from_bytes([2; 16]);
    let reparse_id = FileId::from_bytes([3; 16]);
    let tree = put_tree_page(
        &store,
        &TreePage::Leaf(vec![
            TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"link".to_vec(), 255)?,
                file_id: symlink_id,
                kind: FileKind::SymbolicLink,
            },
            TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"reparse".to_vec(), 255)?,
                file_id: reparse_id,
                kind: FileKind::ReparsePoint,
            },
        ]),
        8,
    )
    .await?;
    let records = |symlink_bytes, reparse_bytes| {
        vec![
            directory_record(root_id, 1, metadata_id, tree),
            FileRecord {
                file_id: symlink_id,
                kind: FileKind::SymbolicLink,
                link_count: 1,
                metadata: metadata_id,
                payload: FilePayload::SymbolicLink {
                    target_bytes: symlink_bytes,
                    target: payload,
                },
            },
            FileRecord {
                file_id: reparse_id,
                kind: FileKind::ReparsePoint,
                link_count: 1,
                metadata: metadata_id,
                payload: FilePayload::ReparsePoint {
                    payload_bytes: reparse_bytes,
                    payload,
                },
            },
        ]
    };
    let generation = generation_with_records(&store, root_id, records(4, 4)).await?;
    let mut windows = limits();
    windows.profile = FilesystemProfile::Windows;
    let proof = prove_generation_closure(
        &store,
        generation,
        windows,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(proof.file_count, 3);
    assert_eq!(
        proof
            .objects
            .iter()
            .filter(|object| **object == payload)
            .count(),
        1
    );

    let mismatch = generation_with_records(&store, root_id, records(5, 4)).await?;
    let failure = prove_generation_closure(
        &store,
        mismatch,
        windows,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("wrong-length symbolic link closure succeeded")?;
    assert!(matches!(failure.error, ClosureError::BlobLengthMismatch));

    let mismatch = generation_with_records(&store, root_id, records(4, 5)).await?;
    let failure = prove_generation_closure(
        &store,
        mismatch,
        windows,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("wrong-length reparse-point closure succeeded")?;
    assert!(matches!(failure.error, ClosureError::BlobLengthMismatch));
    Ok(())
}

#[tokio::test]
async fn shared_regular_data_and_metadata_are_authenticated_once()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata_id = put_file_metadata(
        &store,
        metadata(),
    )
    .await?;
    let blob = blob_with_bytes(&store, b"body").await?;
    let extents = put_extent_page(
        &store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 4,
            kind: ExtentKind::Content {
                object: blob,
                object_offset: 0,
            },
        }]),
        8,
    )
    .await?;
    let root_id = FileId::from_bytes([1; 16]);
    let child_ids = [FileId::from_bytes([2; 16]), FileId::from_bytes([3; 16])];
    let tree = put_tree_page(
        &store,
        &TreePage::Leaf(vec![
            TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"first".to_vec(), 255)?,
                file_id: child_ids[0],
                kind: FileKind::Regular,
            },
            TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"second".to_vec(), 255)?,
                file_id: child_ids[1],
                kind: FileKind::Regular,
            },
        ]),
        8,
    )
    .await?;
    let mut records = vec![directory_record(root_id, 1, metadata_id, tree)];
    for file_id in child_ids {
        records.push(FileRecord {
            file_id,
            kind: FileKind::Regular,
            link_count: 1,
            metadata: metadata_id,
            payload: FilePayload::Regular {
                logical_bytes: 4,
                extents,
            },
        });
    }
    let generation = generation_with_records(&store, root_id, records.clone()).await?;
    let proof = prove_generation_closure(
        &store,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(proof.file_count, 3);
    assert_eq!(u64::try_from(proof.objects.len()).unwrap_or(u64::MAX), 7);
    assert_eq!(proof.work.object_probes, 7);
    assert_eq!(proof.work.backend_read_operations, 7);

    // A previously proven extent root must still reject a conflicting file size.
    records[2].payload = FilePayload::Regular {
        logical_bytes: 5,
        extents,
    };
    let conflicting = generation_with_records(&store, root_id, records).await?;
    assert!(matches!(
        prove_generation_closure(&store, conflicting, limits(), WorkBudget::UNBOUNDED, &CancellationToken::new()).await,
        Err(failure) if matches!(failure.error, ClosureError::RoutingBoundsMismatch)
    ));
    Ok(())
}
