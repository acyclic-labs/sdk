#[tokio::test]
async fn blob_page_aliases_cannot_enter_a_generation_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let blob_store = MemoryObjectStore::default();
    let chunk = put(&blob_store, ObjectKind::BlobChunk, b"ab".to_vec()).await?;
    let leaf = put(
        &blob_store,
        ObjectKind::Blob,
        encode_blob_page(
            &BlobPage {
                first_offset: 0,
                end_offset: 2,
                node: BlobNode::Leaf(vec![BlobChunkRef {
                    first_offset: 0,
                    end_offset: 2,
                    chunk,
                }]),
            },
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let aliased_blob = put(
        &blob_store,
        ObjectKind::Blob,
        encode_blob_page(
            &BlobPage {
                first_offset: 0,
                end_offset: 4,
                node: BlobNode::Internal(vec![
                    BlobChild {
                        first_offset: 0,
                        end_offset: 2,
                        page: leaf,
                    },
                    BlobChild {
                        first_offset: 2,
                        end_offset: 4,
                        page: leaf,
                    },
                ]),
            },
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let mut blob_metadata = metadata();
    blob_metadata.acl = MetadataField::Value(aliased_blob);
    let metadata_id = put(
        &blob_store,
        ObjectKind::Metadata,
        encode_file_metadata(
            blob_metadata,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let empty_tree = put(
        &blob_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(Vec::new()),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let root_id = FileId::from_bytes([1; 16]);
    let generation = generation_with_records(
        &blob_store,
        root_id,
        vec![directory_record(root_id, 1, metadata_id, empty_tree)],
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &blob_store,
            generation,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await,
        Err(OperationFailure {
            error: ClosureError::RoutingAlias,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn attribute_page_aliases_cannot_enter_a_generation_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let attribute_store = MemoryObjectStore::default();
    let value = blob_with_bytes(&attribute_store, b"x").await?;
    let attribute_leaf = put(
        &attribute_store,
        ObjectKind::AttributePage,
        encode_attribute_page(
            &AttributePage::Leaf(vec![AttributeEntry {
                name: attribute_name("a")?,
                value_bytes: 1,
                value,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let aliased_attributes = put(
        &attribute_store,
        ObjectKind::AttributePage,
        encode_attribute_page(
            &AttributePage::Internal(vec![
                Child {
                    first: attribute_name("a")?,
                    page: attribute_leaf,
                },
                Child {
                    first: attribute_name("z")?,
                    page: attribute_leaf,
                },
            ]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let mut attribute_metadata = metadata();
    attribute_metadata.named_attributes = MetadataField::Value(aliased_attributes);
    let metadata_id = put(
        &attribute_store,
        ObjectKind::Metadata,
        encode_file_metadata(
            attribute_metadata,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let empty_tree = put(
        &attribute_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(Vec::new()),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let root_id = FileId::from_bytes([1; 16]);
    let generation = generation_with_records(
        &attribute_store,
        root_id,
        vec![directory_record(root_id, 1, metadata_id, empty_tree)],
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &attribute_store,
            generation,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await,
        Err(OperationFailure {
            error: ClosureError::RoutingAlias,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn file_table_page_aliases_cannot_enter_a_generation_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata_id = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let empty_tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(Vec::new()),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let root_id = FileId::from_bytes([1; 16]);
    let leaf = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![directory_record(root_id, 1, metadata_id, empty_tree)]),
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
            &FileTablePage::Internal(vec![
                Child {
                    first: root_id,
                    page: leaf,
                },
                Child {
                    first: FileId::from_bytes([2; 16]),
                    page: leaf,
                },
            ]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let generation = minimal_generation(&store, Some(table)).await?.0;
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
            error: ClosureError::RoutingAlias,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn directory_page_aliases_cannot_enter_a_generation_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let tree_store = MemoryObjectStore::default();
    let metadata_id = put(
        &tree_store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let root_id = FileId::from_bytes([1; 16]);
    let child_id = FileId::from_bytes([2; 16]);
    let tree_leaf = put(
        &tree_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 255)?,
                file_id: child_id,
                kind: FileKind::Regular,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let aliased_tree = put(
        &tree_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Internal(vec![
                Child {
                    first: LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 255)?,
                    page: tree_leaf,
                },
                Child {
                    first: LogicalName::new(NameEncoding::Utf8, b"z".to_vec(), 255)?,
                    page: tree_leaf,
                },
            ]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let generation = generation_with_records(
        &tree_store,
        root_id,
        vec![
            directory_record(root_id, 1, metadata_id, aliased_tree),
            FileRecord {
                file_id: child_id,
                kind: FileKind::Regular,
                link_count: 1,
                metadata: metadata_id,
                payload: FilePayload::InlineRegular(InlineFileData::new(b"x")?),
            },
        ],
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &tree_store,
            generation,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await,
        Err(OperationFailure {
            error: ClosureError::RoutingAlias,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn extent_page_aliases_cannot_enter_a_generation_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let extent_store = MemoryObjectStore::default();
    let metadata_id = put(
        &extent_store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let root_id = FileId::from_bytes([1; 16]);
    let child_id = FileId::from_bytes([2; 16]);
    let namespace = put(
        &extent_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"file".to_vec(), 255)?,
                file_id: child_id,
                kind: FileKind::Regular,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let extent_leaf = put(
        &extent_store,
        ObjectKind::ExtentPage,
        encode_extent_page(
            &ExtentPage::Leaf(vec![Extent {
                offset: 0,
                length: 1,
                kind: ExtentKind::Hole,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let aliased_extents = put(
        &extent_store,
        ObjectKind::ExtentPage,
        encode_extent_page(
            &ExtentPage::Internal(vec![
                ExtentChild {
                    first_offset: 0,
                    end_offset: 1,
                    page: extent_leaf,
                },
                ExtentChild {
                    first_offset: 1,
                    end_offset: 2,
                    page: extent_leaf,
                },
            ]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let generation = generation_with_records(
        &extent_store,
        root_id,
        vec![
            directory_record(root_id, 1, metadata_id, namespace),
            FileRecord {
                file_id: child_id,
                kind: FileKind::Regular,
                link_count: 1,
                metadata: metadata_id,
                payload: FilePayload::Regular {
                    logical_bytes: 2,
                    extents: aliased_extents,
                },
            },
        ],
    )
    .await?;
    assert!(matches!(
        prove_generation_closure(
            &extent_store,
            generation,
            limits(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await,
        Err(OperationFailure {
            error: ClosureError::RoutingAlias,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn internal_namespace_file_table_and_extent_pages_prove_without_scanning_unrelated_data()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata_id = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let root_id = FileId::from_bytes([1; 16]);
    let first_id = FileId::from_bytes([2; 16]);
    let second_id = FileId::from_bytes([3; 16]);
    let first_tree_leaf = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 255)?,
                file_id: first_id,
                kind: FileKind::Regular,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let second_tree_leaf = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"z".to_vec(), 255)?,
                file_id: second_id,
                kind: FileKind::Regular,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Internal(vec![
                Child {
                    first: LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 255)?,
                    page: first_tree_leaf,
                },
                Child {
                    first: LogicalName::new(NameEncoding::Utf8, b"z".to_vec(), 255)?,
                    page: second_tree_leaf,
                },
            ]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let first_extent_leaf = put(
        &store,
        ObjectKind::ExtentPage,
        encode_extent_page(
            &ExtentPage::Leaf(vec![Extent {
                offset: 0,
                length: 2,
                kind: ExtentKind::Hole,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let second_extent_leaf = put(
        &store,
        ObjectKind::ExtentPage,
        encode_extent_page(
            &ExtentPage::Leaf(vec![Extent {
                offset: 2,
                length: 2,
                kind: ExtentKind::AllocatedZero,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let extents = put(
        &store,
        ObjectKind::ExtentPage,
        encode_extent_page(
            &ExtentPage::Internal(vec![
                ExtentChild {
                    first_offset: 0,
                    end_offset: 2,
                    page: first_extent_leaf,
                },
                ExtentChild {
                    first_offset: 2,
                    end_offset: 4,
                    page: second_extent_leaf,
                },
            ]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let records = [
        directory_record(root_id, 1, metadata_id, tree),
        FileRecord {
            file_id: first_id,
            kind: FileKind::Regular,
            link_count: 1,
            metadata: metadata_id,
            payload: FilePayload::Regular {
                logical_bytes: 4,
                extents,
            },
        },
        FileRecord {
            file_id: second_id,
            kind: FileKind::Regular,
            link_count: 1,
            metadata: metadata_id,
            payload: FilePayload::InlineRegular(InlineFileData::new(b"inline")?),
        },
    ];
    let first_table_leaf = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(records[..1].to_vec()),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let second_table_leaf = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(records[1..].to_vec()),
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
            &FileTablePage::Internal(vec![
                Child {
                    first: root_id,
                    page: first_table_leaf,
                },
                Child {
                    first: first_id,
                    page: second_table_leaf,
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
                volume_id: VolumeId::from_bytes([4; 16]),
                root_file_id: root_id,
                file_table: table,
                parents: Vec::new(),
                required_features: 0,
            },
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
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
    assert_eq!(proof.file_count, 3);
    assert!(proof.work.page_reads >= 8);
    Ok(())
}

#[tokio::test]
async fn forged_file_table_lower_bound_fails_before_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata_id = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(Vec::new()),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let leaf = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![FileRecord {
                file_id: FileId::from_bytes([1; 16]),
                kind: FileKind::Directory,
                link_count: 1,
                metadata: metadata_id,
                payload: FilePayload::Directory { entries: tree },
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let forged = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Internal(vec![Child {
                first: FileId::from_bytes([0; 16]),
                page: leaf,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let (generation, _, _) = minimal_generation(&store, Some(forged)).await?;
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
            error: ClosureError::RoutingBoundsMismatch,
            ..
        })
    ));
    Ok(())
}
