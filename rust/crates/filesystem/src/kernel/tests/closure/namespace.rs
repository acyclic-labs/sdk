#[tokio::test]
async fn namespace_proof_rejects_missing_mismatched_and_multiply_linked_identities()
-> Result<(), Box<dyn std::error::Error>> {
    let root_file_id = FileId::from_bytes([1; 16]);
    let child_file_id = FileId::from_bytes([2; 16]);

    let missing_root_store = MemoryObjectStore::default();
    let metadata_id = put(
        &missing_root_store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let empty_tree = put(
        &missing_root_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(Vec::new()),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let missing_root = generation_with_records(
        &missing_root_store,
        root_file_id,
        vec![FileRecord {
            file_id: child_file_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata: metadata_id,
            payload: FilePayload::Directory {
                entries: empty_tree,
            },
        }],
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &missing_root_store,
            missing_root,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await,
        Err(OperationFailure {
            error: ClosureError::MissingRootFile,
            ..
        })
    ));

    let non_directory_store = MemoryObjectStore::default();
    let metadata_id = put(
        &non_directory_store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let non_directory = generation_with_records(
        &non_directory_store,
        root_file_id,
        vec![FileRecord {
            file_id: root_file_id,
            kind: FileKind::Regular,
            link_count: 1,
            metadata: metadata_id,
            payload: FilePayload::InlineRegular(InlineFileData::new(b"root")?),
        }],
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &non_directory_store,
            non_directory,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await,
        Err(OperationFailure {
            error: ClosureError::RootIsNotDirectory,
            ..
        })
    ));

    let absent_child_store = MemoryObjectStore::default();
    let metadata_id = put(
        &absent_child_store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let child_name = LogicalName::new(NameEncoding::Utf8, b"child".to_vec(), 255)?;
    let dangling_tree = put(
        &absent_child_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: child_name.clone(),
                file_id: child_file_id,
                kind: FileKind::Regular,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let absent_child = generation_with_records(
        &absent_child_store,
        root_file_id,
        vec![FileRecord {
            file_id: root_file_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata: metadata_id,
            payload: FilePayload::Directory {
                entries: dangling_tree,
            },
        }],
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &absent_child_store,
            absent_child,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ).await,
        Err(OperationFailure {
            error: ClosureError::MissingFileRecord(actual),
            ..
        }) if actual == child_file_id
    ));

    let mismatched_store = MemoryObjectStore::default();
    let metadata_id = put(
        &mismatched_store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let mismatched_tree = put(
        &mismatched_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: child_name,
                file_id: child_file_id,
                kind: FileKind::Directory,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let mismatched = generation_with_records(
        &mismatched_store,
        root_file_id,
        vec![
            FileRecord {
                file_id: root_file_id,
                kind: FileKind::Directory,
                link_count: 1,
                metadata: metadata_id,
                payload: FilePayload::Directory {
                    entries: mismatched_tree,
                },
            },
            FileRecord {
                file_id: child_file_id,
                kind: FileKind::Regular,
                link_count: 1,
                metadata: metadata_id,
                payload: FilePayload::InlineRegular(InlineFileData::new(b"child")?),
            },
        ],
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &mismatched_store,
            mismatched,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ).await,
        Err(OperationFailure {
            error: ClosureError::NamespaceKindMismatch(actual),
            ..
        }) if actual == child_file_id
    ));
    Ok(())
}

#[tokio::test]
async fn closure_rejects_kinds_outside_the_declared_volume_profile()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let root_file_id = FileId::from_bytes([1; 16]);
    let fifo_file_id = FileId::from_bytes([2; 16]);
    let tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"fifo".to_vec(), 255)?,
                file_id: fifo_file_id,
                kind: FileKind::Fifo,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let table = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![
                FileRecord {
                    file_id: root_file_id,
                    kind: FileKind::Directory,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Directory { entries: tree },
                },
                FileRecord {
                    file_id: fifo_file_id,
                    kind: FileKind::Fifo,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Empty,
                },
            ]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let generation = put(
        &store,
        ObjectKind::GenerationRoot,
        encode_generation_root(
            &GenerationRoot {
                volume_id: VolumeId::from_bytes([2; 16]),
                root_file_id,
                file_table: table,
                parents: Vec::new(),
                required_features: 0,
            },
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let failure = prove_generation_closure(
        &store,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("portable FIFO closure unexpectedly proved")?;
    assert!(matches!(
        failure.error,
        ClosureError::UnsupportedVolumeSemantics
    ));
    Ok(())
}

#[tokio::test]
async fn namespace_proof_rejects_link_counts_directory_links_and_disconnected_cycles()
-> Result<(), Box<dyn std::error::Error>> {
    let root_id = FileId::from_bytes([1; 16]);
    let child_id = FileId::from_bytes([2; 16]);

    let mismatch_store = MemoryObjectStore::default();
    let metadata_id = put(
        &mismatch_store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let root_tree = put(
        &mismatch_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"child".to_vec(), 255)?,
                file_id: child_id,
                kind: FileKind::Regular,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let mismatch = generation_with_records(
        &mismatch_store,
        root_id,
        vec![
            directory_record(root_id, 1, metadata_id, root_tree),
            FileRecord {
                file_id: child_id,
                kind: FileKind::Regular,
                link_count: 2,
                metadata: metadata_id,
                payload: FilePayload::InlineRegular(InlineFileData::new(b"child")?),
            },
        ],
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &mismatch_store,
            mismatch,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ).await,
        Err(OperationFailure {
            error: ClosureError::LinkCountMismatch(actual),
            ..
        }) if actual == child_id
    ));

    let hard_link_store = MemoryObjectStore::default();
    let metadata_id = put(
        &hard_link_store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let empty_tree = put(
        &hard_link_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(Vec::new()),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let root_tree = put(
        &hard_link_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![
                TreeEntry {
                    name: LogicalName::new(NameEncoding::Utf8, b"first".to_vec(), 255)?,
                    file_id: child_id,
                    kind: FileKind::Directory,
                },
                TreeEntry {
                    name: LogicalName::new(NameEncoding::Utf8, b"second".to_vec(), 255)?,
                    file_id: child_id,
                    kind: FileKind::Directory,
                },
            ]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let hard_link = generation_with_records(
        &hard_link_store,
        root_id,
        vec![
            directory_record(root_id, 1, metadata_id, root_tree),
            directory_record(child_id, 2, metadata_id, empty_tree),
        ],
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &hard_link_store,
            hard_link,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ).await,
        Err(OperationFailure {
            error: ClosureError::DirectoryHardLink(actual),
            ..
        }) if actual == child_id
    ));

    let disconnected_store = MemoryObjectStore::default();
    let metadata_id = put(
        &disconnected_store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let root_tree = put(
        &disconnected_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(Vec::new()),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let first_id = FileId::from_bytes([2; 16]);
    let second_id = FileId::from_bytes([3; 16]);
    let first_tree = put(
        &disconnected_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"second".to_vec(), 255)?,
                file_id: second_id,
                kind: FileKind::Directory,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let second_tree = put(
        &disconnected_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"first".to_vec(), 255)?,
                file_id: first_id,
                kind: FileKind::Directory,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let disconnected = generation_with_records(
        &disconnected_store,
        root_id,
        vec![
            directory_record(root_id, 1, metadata_id, root_tree),
            directory_record(first_id, 1, metadata_id, first_tree),
            directory_record(second_id, 1, metadata_id, second_tree),
        ],
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &disconnected_store,
            disconnected,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await,
        Err(OperationFailure {
            error: ClosureError::UnreachableFileRecords,
            ..
        })
    ));
    Ok(())
}
