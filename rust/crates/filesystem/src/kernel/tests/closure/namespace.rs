#[tokio::test]
async fn namespace_proof_rejects_missing_mismatched_and_multiply_linked_identities()
-> Result<(), Box<dyn std::error::Error>> {
    let root_file_id = FileId::from_bytes([1; 16]);
    let child_file_id = FileId::from_bytes([2; 16]);

    let missing_root_store = MemoryObjectStore::default();
    let metadata_id = put_file_metadata(
        &missing_root_store,
        metadata(),
    )
    .await?;
    let empty_tree = put_tree_page(
        &missing_root_store,
        &TreePage::Leaf(Vec::new()),
        8,
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
    let metadata_id = put_file_metadata(
        &non_directory_store,
        metadata(),
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
    let metadata_id = put_file_metadata(
        &absent_child_store,
        metadata(),
    )
    .await?;
    let child_name = LogicalName::new(NameEncoding::Utf8, b"child".to_vec(), 255)?;
    let dangling_tree = put_tree_page(
        &absent_child_store,
        &TreePage::Leaf(vec![TreeEntry {
            name: child_name.clone(),
            file_id: child_file_id,
            kind: FileKind::Regular,
        }]),
        8,
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
    let metadata_id = put_file_metadata(
        &mismatched_store,
        metadata(),
    )
    .await?;
    let mismatched_tree = put_tree_page(
        &mismatched_store,
        &TreePage::Leaf(vec![TreeEntry {
            name: child_name,
            file_id: child_file_id,
            kind: FileKind::Directory,
        }]),
        8,
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
    let metadata = put_file_metadata(
        &store,
        metadata(),
    )
    .await?;
    let root_file_id = FileId::from_bytes([1; 16]);
    let fifo_file_id = FileId::from_bytes([2; 16]);
    let tree = put_tree_page(
        &store,
        &TreePage::Leaf(vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"fifo".to_vec(), 255)?,
            file_id: fifo_file_id,
            kind: FileKind::Fifo,
        }]),
        8,
    )
    .await?;
    let table = put_file_table_page(
        &store,
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
    )
    .await?;
    let generation = put_generation_root(
        &store,
        &GenerationRoot {
            volume_id: VolumeId::from_bytes([2; 16]),
            root_file_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        },
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
    let metadata_id = put_file_metadata(
        &mismatch_store,
        metadata(),
    )
    .await?;
    let root_tree = put_tree_page(
        &mismatch_store,
        &TreePage::Leaf(vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"child".to_vec(), 255)?,
            file_id: child_id,
            kind: FileKind::Regular,
        }]),
        8,
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
    let metadata_id = put_file_metadata(
        &hard_link_store,
        metadata(),
    )
    .await?;
    let empty_tree = put_tree_page(
        &hard_link_store,
        &TreePage::Leaf(Vec::new()),
        8,
    )
    .await?;
    let root_tree = put_tree_page(
        &hard_link_store,
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
    let metadata_id = put_file_metadata(
        &disconnected_store,
        metadata(),
    )
    .await?;
    let root_tree = put_tree_page(
        &disconnected_store,
        &TreePage::Leaf(Vec::new()),
        8,
    )
    .await?;
    let first_id = FileId::from_bytes([2; 16]);
    let second_id = FileId::from_bytes([3; 16]);
    let first_tree = put_tree_page(
        &disconnected_store,
        &TreePage::Leaf(vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"second".to_vec(), 255)?,
            file_id: second_id,
            kind: FileKind::Directory,
        }]),
        8,
    )
    .await?;
    let second_tree = put_tree_page(
        &disconnected_store,
        &TreePage::Leaf(vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"first".to_vec(), 255)?,
            file_id: first_id,
            kind: FileKind::Directory,
        }]),
        8,
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
