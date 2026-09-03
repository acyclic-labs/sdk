use super::*;
use crate::foundation::{Digest, VolumeId};
use crate::kernel::{
    AttributeChild, AttributeClass, AttributeEntry, BlobChild, BlobChunkRef, BlobPage, Extent,
    ExtentChild, FileKind, FileMetadata, FileTableChild, InlineFileData, NameEncoding, TreeChild,
    encode_attribute_page, encode_blob_page, encode_extent_page, encode_file_metadata,
    encode_file_table_page, encode_generation_root, encode_tree_page,
};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectStore, object_digest};
use crate::test_support::OwnedReadObjectStore;
use crate::{CachedObjectStore, ObjectCacheOptions};

fn limits() -> ClosureLimits {
    ClosureLimits {
        decode: DecodeLimits::default(),
        maximum_objects: 100,
        maximum_files: 100,
        maximum_object_bytes: 1024 * 1024,
        profile: FilesystemProfile::Portable,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
    }
}

fn metadata() -> FileMetadata {
    FileMetadata {
        posix_mode: MetadataField::Unavailable,
        posix_uid: MetadataField::Unavailable,
        posix_gid: MetadataField::Unavailable,
        posix_flags: MetadataField::Unavailable,
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Unavailable,
        modified_ns: MetadataField::Unavailable,
        accessed_ns: MetadataField::Unavailable,
        changed_ns: MetadataField::Unavailable,
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    }
}

fn put(
    store: &MemoryObjectStore,
    kind: ObjectKind,
    bytes: Vec<u8>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let object = ObjectId {
        kind,
        digest: object_digest(kind, &bytes),
    };
    store.put(object, Bytes::from(bytes), WorkBudget::UNBOUNDED)?;
    Ok(object)
}

fn minimal_generation(
    store: &MemoryObjectStore,
    file_table: Option<ObjectId>,
) -> Result<(ObjectId, FileId, ObjectId), Box<dyn std::error::Error>> {
    let metadata = put(
        store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
    let tree = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
    let root_file_id = FileId::from_bytes([1; 16]);
    let table = match file_table {
        Some(table) => table,
        None => put(
            store,
            ObjectKind::FileTablePage,
            encode_file_table_page(
                &FileTablePage::Leaf(vec![FileRecord {
                    file_id: root_file_id,
                    kind: FileKind::Directory,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Directory { entries: tree },
                }]),
                8,
            )?,
        )?,
    };
    let generation = put(
        store,
        ObjectKind::GenerationRoot,
        encode_generation_root(&GenerationRoot {
            volume_id: VolumeId::from_bytes([2; 16]),
            root_file_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        })?,
    )?;
    Ok((generation, root_file_id, metadata))
}

fn generation_with_records(
    store: &MemoryObjectStore,
    root_file_id: FileId,
    records: Vec<FileRecord>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let table = put(
        store,
        ObjectKind::FileTablePage,
        encode_file_table_page(&FileTablePage::Leaf(records), 16)?,
    )?;
    put(
        store,
        ObjectKind::GenerationRoot,
        encode_generation_root(&GenerationRoot {
            volume_id: VolumeId::from_bytes([3; 16]),
            root_file_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        })?,
    )
}

fn directory_record(
    file_id: FileId,
    link_count: u64,
    metadata: ObjectId,
    entries: ObjectId,
) -> FileRecord {
    FileRecord {
        file_id,
        kind: FileKind::Directory,
        link_count,
        metadata,
        payload: FilePayload::Directory { entries },
    }
}

fn generation_with_regular_extent(
    store: &MemoryObjectStore,
    extent_kind: ExtentKind,
    logical_bytes: u64,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let metadata_id = put(
        store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
    let root_id = FileId::from_bytes([1; 16]);
    let child_id = FileId::from_bytes([2; 16]);
    let entries = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"file".to_vec(), 255)?,
                file_id: child_id,
                kind: FileKind::Regular,
            }]),
            8,
        )?,
    )?;
    let extents = put(
        store,
        ObjectKind::ExtentPage,
        encode_extent_page(
            &ExtentPage::Leaf(vec![Extent {
                offset: 0,
                length: logical_bytes,
                kind: extent_kind,
            }]),
            8,
        )?,
    )?;
    generation_with_records(
        store,
        root_id,
        vec![
            directory_record(root_id, 1, metadata_id, entries),
            FileRecord {
                file_id: child_id,
                kind: FileKind::Regular,
                link_count: 1,
                metadata: metadata_id,
                payload: FilePayload::Regular {
                    logical_bytes,
                    extents,
                },
            },
        ],
    )
}

fn blob_with_bytes(
    store: &MemoryObjectStore,
    bytes: &'static [u8],
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let chunk = put(store, ObjectKind::BlobChunk, bytes.to_vec())?;
    put(
        store,
        ObjectKind::Blob,
        encode_blob_page(
            &BlobPage {
                first_offset: 0,
                end_offset: u64::try_from(bytes.len())?,
                node: BlobNode::Leaf(vec![BlobChunkRef {
                    first_offset: 0,
                    end_offset: u64::try_from(bytes.len())?,
                    chunk,
                }]),
            },
            8,
        )?,
    )
}

fn internal_blob(store: &MemoryObjectStore) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let first_chunk = put(store, ObjectKind::BlobChunk, b"ab".to_vec())?;
    let second_chunk = put(store, ObjectKind::BlobChunk, b"cd".to_vec())?;
    let first = put(
        store,
        ObjectKind::Blob,
        encode_blob_page(
            &BlobPage {
                first_offset: 0,
                end_offset: 2,
                node: BlobNode::Leaf(vec![BlobChunkRef {
                    first_offset: 0,
                    end_offset: 2,
                    chunk: first_chunk,
                }]),
            },
            8,
        )?,
    )?;
    let second = put(
        store,
        ObjectKind::Blob,
        encode_blob_page(
            &BlobPage {
                first_offset: 2,
                end_offset: 4,
                node: BlobNode::Leaf(vec![BlobChunkRef {
                    first_offset: 2,
                    end_offset: 4,
                    chunk: second_chunk,
                }]),
            },
            8,
        )?,
    )?;
    put(
        store,
        ObjectKind::Blob,
        encode_blob_page(
            &BlobPage {
                first_offset: 0,
                end_offset: 4,
                node: BlobNode::Internal(vec![
                    BlobChild {
                        first_offset: 0,
                        end_offset: 2,
                        page: first,
                    },
                    BlobChild {
                        first_offset: 2,
                        end_offset: 4,
                        page: second,
                    },
                ]),
            },
            8,
        )?,
    )
}

fn attribute_name(value: &str) -> Result<AttributeName, Box<dyn std::error::Error>> {
    Ok(AttributeName::new(
        AttributeClass::PosixXattr,
        value.as_bytes().to_vec(),
        255,
    )?)
}

#[test]
fn minimal_generation_has_a_complete_authenticated_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, _, _) = minimal_generation(&store, None)?;
    let proof = prove_generation_closure(&store, generation, limits(), WorkBudget::UNBOUNDED)?;
    assert_eq!(proof.file_count, 1);
    assert_eq!(proof.object_count, 4);
    assert_eq!(proof.root.root_file_id, FileId::from_bytes([1; 16]));
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn metadata_attributes_and_internal_blobs_are_fully_authenticated()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let value = internal_blob(&store)?;
    let first_leaf = put(
        &store,
        ObjectKind::AttributePage,
        encode_attribute_page(
            &AttributePage::Leaf(vec![AttributeEntry {
                name: attribute_name("a")?,
                value_bytes: 4,
                value,
            }]),
            8,
        )?,
    )?;
    let second_leaf = put(
        &store,
        ObjectKind::AttributePage,
        encode_attribute_page(
            &AttributePage::Leaf(vec![AttributeEntry {
                name: attribute_name("z")?,
                value_bytes: 4,
                value,
            }]),
            8,
        )?,
    )?;
    let attributes = put(
        &store,
        ObjectKind::AttributePage,
        encode_attribute_page(
            &AttributePage::Internal(vec![
                AttributeChild {
                    first_name: attribute_name("a")?,
                    page: first_leaf,
                },
                AttributeChild {
                    first_name: attribute_name("z")?,
                    page: second_leaf,
                },
            ]),
            8,
        )?,
    )?;
    let mut complete_metadata = metadata();
    complete_metadata.named_attributes = MetadataField::Value(attributes);
    complete_metadata.acl = MetadataField::Value(value);
    complete_metadata.security_descriptor = MetadataField::Value(value);
    let metadata_id = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(complete_metadata)?,
    )?;
    let tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
    let root_id = FileId::from_bytes([1; 16]);
    let generation = generation_with_records(
        &store,
        root_id,
        vec![directory_record(root_id, 1, metadata_id, tree)],
    )?;
    let proof = prove_generation_closure(&store, generation, limits(), WorkBudget::UNBOUNDED)?;
    assert_eq!(proof.file_count, 1);
    assert!(proof.object_count >= 12);
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
    let cached_proof = async_storage::poll_ready(prove_generation_closure_async(
        &cached,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cache-backed closure proof suspended")??;
    assert_eq!(cached_proof.objects, proof.objects);
    let warm = async_storage::poll_ready(prove_generation_closure_async(
        &cached,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("warm cache-backed closure proof suspended")??;
    assert_eq!(warm.objects, proof.objects);
    assert_eq!(warm.work.backend_read_operations, 0);
    Ok(())
}

#[test]
fn cached_closure_proves_sparse_extent_and_blob_without_duplicate_backend_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::default();
    let blob = blob_with_bytes(&backend, b"data")?;
    let generation = generation_with_regular_extent(
        &backend,
        ExtentKind::Content {
            object: blob,
            object_offset: 0,
        },
        4,
    )?;
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
    let cold = async_storage::poll_ready(prove_generation_closure_async(
        &cached,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cache-backed sparse closure proof suspended")??;
    assert_eq!(cold.file_count, 2);
    assert!(cold.work.backend_read_operations > 0);
    let warm = async_storage::poll_ready(prove_generation_closure_async(
        &cached,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("warm cache-backed sparse closure proof suspended")??;
    assert_eq!(warm.objects, cold.objects);
    assert_eq!(warm.work.backend_read_operations, 0);
    assert_eq!(warm.work.object_bytes_read, 0);
    Ok(())
}

#[test]
fn attribute_value_length_mismatch_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let value = blob_with_bytes(&store, b"data")?;
    let attributes = put(
        &store,
        ObjectKind::AttributePage,
        encode_attribute_page(
            &AttributePage::Leaf(vec![AttributeEntry {
                name: attribute_name("wrong-length")?,
                value_bytes: 5,
                value,
            }]),
            8,
        )?,
    )?;
    let mut complete_metadata = metadata();
    complete_metadata.named_attributes = MetadataField::Value(attributes);
    let metadata_id = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(complete_metadata)?,
    )?;
    let tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
    let root_id = FileId::from_bytes([1; 16]);
    let generation = generation_with_records(
        &store,
        root_id,
        vec![directory_record(root_id, 1, metadata_id, tree)],
    )?;
    assert!(matches!(
        prove_generation_closure(&store, generation, limits(), WorkBudget::UNBOUNDED),
        Err(OperationFailure {
            error: ClosureError::BlobLengthMismatch,
            ..
        })
    ));
    Ok(())
}

#[test]
fn symbolic_link_and_reparse_payloads_are_fully_proved_and_length_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata_id = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
    let payload = blob_with_bytes(&store, b"data")?;
    let root_id = FileId::from_bytes([1; 16]);
    let symlink_id = FileId::from_bytes([2; 16]);
    let reparse_id = FileId::from_bytes([3; 16]);
    let tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
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
        )?,
    )?;
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
    let generation = generation_with_records(&store, root_id, records(4, 4))?;
    let mut windows = limits();
    windows.profile = FilesystemProfile::Windows;
    let proof = prove_generation_closure(&store, generation, windows, WorkBudget::UNBOUNDED)?;
    assert_eq!(proof.file_count, 3);
    assert_eq!(
        proof
            .objects
            .iter()
            .filter(|object| **object == payload)
            .count(),
        1
    );

    let mismatch = generation_with_records(&store, root_id, records(5, 4))?;
    let failure = prove_generation_closure(&store, mismatch, windows, WorkBudget::UNBOUNDED)
        .err()
        .ok_or("wrong-length symbolic link closure succeeded")?;
    assert!(matches!(failure.error, ClosureError::BlobLengthMismatch));

    let mismatch = generation_with_records(&store, root_id, records(4, 5))?;
    let failure = prove_generation_closure(&store, mismatch, windows, WorkBudget::UNBOUNDED)
        .err()
        .ok_or("wrong-length reparse-point closure succeeded")?;
    assert!(matches!(failure.error, ClosureError::BlobLengthMismatch));
    Ok(())
}

#[test]
fn blob_page_aliases_cannot_enter_a_generation_closure() -> Result<(), Box<dyn std::error::Error>> {
    let blob_store = MemoryObjectStore::default();
    let chunk = put(&blob_store, ObjectKind::BlobChunk, b"ab".to_vec())?;
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
        )?,
    )?;
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
        )?,
    )?;
    let mut blob_metadata = metadata();
    blob_metadata.acl = MetadataField::Value(aliased_blob);
    let metadata_id = put(
        &blob_store,
        ObjectKind::Metadata,
        encode_file_metadata(blob_metadata)?,
    )?;
    let empty_tree = put(
        &blob_store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
    let root_id = FileId::from_bytes([1; 16]);
    let generation = generation_with_records(
        &blob_store,
        root_id,
        vec![directory_record(root_id, 1, metadata_id, empty_tree)],
    )?;
    assert!(matches!(
        prove_generation_closure(&blob_store, generation, limits(), WorkBudget::UNBOUNDED,),
        Err(OperationFailure {
            error: ClosureError::RoutingAlias,
            ..
        })
    ));
    Ok(())
}

#[test]
fn attribute_page_aliases_cannot_enter_a_generation_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let attribute_store = MemoryObjectStore::default();
    let value = blob_with_bytes(&attribute_store, b"x")?;
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
        )?,
    )?;
    let aliased_attributes = put(
        &attribute_store,
        ObjectKind::AttributePage,
        encode_attribute_page(
            &AttributePage::Internal(vec![
                AttributeChild {
                    first_name: attribute_name("a")?,
                    page: attribute_leaf,
                },
                AttributeChild {
                    first_name: attribute_name("z")?,
                    page: attribute_leaf,
                },
            ]),
            8,
        )?,
    )?;
    let mut attribute_metadata = metadata();
    attribute_metadata.named_attributes = MetadataField::Value(aliased_attributes);
    let metadata_id = put(
        &attribute_store,
        ObjectKind::Metadata,
        encode_file_metadata(attribute_metadata)?,
    )?;
    let empty_tree = put(
        &attribute_store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
    let root_id = FileId::from_bytes([1; 16]);
    let generation = generation_with_records(
        &attribute_store,
        root_id,
        vec![directory_record(root_id, 1, metadata_id, empty_tree)],
    )?;
    assert!(matches!(
        prove_generation_closure(
            &attribute_store,
            generation,
            limits(),
            WorkBudget::UNBOUNDED,
        ),
        Err(OperationFailure {
            error: ClosureError::RoutingAlias,
            ..
        })
    ));
    Ok(())
}

#[test]
fn file_table_page_aliases_cannot_enter_a_generation_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata_id = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
    let empty_tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
    let root_id = FileId::from_bytes([1; 16]);
    let leaf = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![directory_record(root_id, 1, metadata_id, empty_tree)]),
            8,
        )?,
    )?;
    let table = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Internal(vec![
                FileTableChild {
                    first_file_id: root_id,
                    page: leaf,
                },
                FileTableChild {
                    first_file_id: FileId::from_bytes([2; 16]),
                    page: leaf,
                },
            ]),
            8,
        )?,
    )?;
    let generation = minimal_generation(&store, Some(table))?.0;
    assert!(matches!(
        prove_generation_closure(&store, generation, limits(), WorkBudget::UNBOUNDED),
        Err(OperationFailure {
            error: ClosureError::RoutingAlias,
            ..
        })
    ));
    Ok(())
}

#[test]
fn directory_page_aliases_cannot_enter_a_generation_closure()
-> Result<(), Box<dyn std::error::Error>> {
    let tree_store = MemoryObjectStore::default();
    let metadata_id = put(
        &tree_store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
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
        )?,
    )?;
    let aliased_tree = put(
        &tree_store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Internal(vec![
                TreeChild {
                    first_name: LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 255)?,
                    page: tree_leaf,
                },
                TreeChild {
                    first_name: LogicalName::new(NameEncoding::Utf8, b"z".to_vec(), 255)?,
                    page: tree_leaf,
                },
            ]),
            8,
        )?,
    )?;
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
    )?;
    assert!(matches!(
        prove_generation_closure(&tree_store, generation, limits(), WorkBudget::UNBOUNDED),
        Err(OperationFailure {
            error: ClosureError::RoutingAlias,
            ..
        })
    ));
    Ok(())
}

#[test]
fn extent_page_aliases_cannot_enter_a_generation_closure() -> Result<(), Box<dyn std::error::Error>>
{
    let extent_store = MemoryObjectStore::default();
    let metadata_id = put(
        &extent_store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
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
        )?,
    )?;
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
        )?,
    )?;
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
        )?,
    )?;
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
    )?;
    assert!(matches!(
        prove_generation_closure(&extent_store, generation, limits(), WorkBudget::UNBOUNDED,),
        Err(OperationFailure {
            error: ClosureError::RoutingAlias,
            ..
        })
    ));
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn internal_namespace_file_table_and_extent_pages_prove_without_scanning_unrelated_data()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata_id = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
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
        )?,
    )?;
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
        )?,
    )?;
    let tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Internal(vec![
                TreeChild {
                    first_name: LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 255)?,
                    page: first_tree_leaf,
                },
                TreeChild {
                    first_name: LogicalName::new(NameEncoding::Utf8, b"z".to_vec(), 255)?,
                    page: second_tree_leaf,
                },
            ]),
            8,
        )?,
    )?;
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
        )?,
    )?;
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
        )?,
    )?;
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
        )?,
    )?;
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
        encode_file_table_page(&FileTablePage::Leaf(records[..1].to_vec()), 8)?,
    )?;
    let second_table_leaf = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(&FileTablePage::Leaf(records[1..].to_vec()), 8)?,
    )?;
    let table = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Internal(vec![
                FileTableChild {
                    first_file_id: root_id,
                    page: first_table_leaf,
                },
                FileTableChild {
                    first_file_id: first_id,
                    page: second_table_leaf,
                },
            ]),
            8,
        )?,
    )?;
    let generation = put(
        &store,
        ObjectKind::GenerationRoot,
        encode_generation_root(&GenerationRoot {
            volume_id: VolumeId::from_bytes([4; 16]),
            root_file_id: root_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        })?,
    )?;
    let proof = prove_generation_closure(&store, generation, limits(), WorkBudget::UNBOUNDED)?;
    assert_eq!(proof.file_count, 3);
    assert!(proof.work.page_reads >= 8);
    Ok(())
}

#[test]
fn closure_limits_and_typed_root_are_enforced_at_the_exact_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, _, metadata) = minimal_generation(&store, None)?;
    let wrong_kind = prove_generation_closure(&store, metadata, limits(), WorkBudget::UNBOUNDED)
        .err()
        .ok_or("metadata object was admitted as a generation")?;
    assert!(matches!(wrong_kind.error, ClosureError::WrongObjectKind));
    assert_eq!(*wrong_kind.work, WorkCounters::default());

    let mut object_limited = limits();
    object_limited.maximum_objects = 0;
    let too_many_objects =
        prove_generation_closure(&store, generation, object_limited, WorkBudget::UNBOUNDED)
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
    let too_many_bytes =
        prove_generation_closure(&store, generation, byte_limited, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("zero-byte closure limit succeeded")?;
    assert!(matches!(
        too_many_bytes.error,
        ClosureError::ClosureBytesExceeded { maximum: 0, .. }
    ));
    assert_eq!(too_many_bytes.work.object_probes, 1);

    let mut file_limited = limits();
    file_limited.maximum_files = 0;
    let too_many_files =
        prove_generation_closure(&store, generation, file_limited, WorkBudget::UNBOUNDED)
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

#[test]
#[allow(clippy::too_many_lines)]
fn namespace_proof_rejects_missing_mismatched_and_multiply_linked_identities()
-> Result<(), Box<dyn std::error::Error>> {
    let root_file_id = FileId::from_bytes([1; 16]);
    let child_file_id = FileId::from_bytes([2; 16]);

    let missing_root_store = MemoryObjectStore::default();
    let metadata_id = put(
        &missing_root_store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
    let empty_tree = put(
        &missing_root_store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
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
    )?;
    assert!(matches!(
        prove_generation_closure(
            &missing_root_store,
            missing_root,
            limits(),
            WorkBudget::UNBOUNDED,
        ),
        Err(OperationFailure {
            error: ClosureError::MissingRootFile,
            ..
        })
    ));

    let non_directory_store = MemoryObjectStore::default();
    let metadata_id = put(
        &non_directory_store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
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
    )?;
    assert!(matches!(
        prove_generation_closure(
            &non_directory_store,
            non_directory,
            limits(),
            WorkBudget::UNBOUNDED,
        ),
        Err(OperationFailure {
            error: ClosureError::RootIsNotDirectory,
            ..
        })
    ));

    let absent_child_store = MemoryObjectStore::default();
    let metadata_id = put(
        &absent_child_store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
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
        )?,
    )?;
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
    )?;
    assert!(matches!(
        prove_generation_closure(
            &absent_child_store,
            absent_child,
            limits(),
            WorkBudget::UNBOUNDED,
        ),
        Err(OperationFailure {
            error: ClosureError::MissingFileRecord(actual),
            ..
        }) if actual == child_file_id
    ));

    let mismatched_store = MemoryObjectStore::default();
    let metadata_id = put(
        &mismatched_store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
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
        )?,
    )?;
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
    )?;
    assert!(matches!(
        prove_generation_closure(
            &mismatched_store,
            mismatched,
            limits(),
            WorkBudget::UNBOUNDED,
        ),
        Err(OperationFailure {
            error: ClosureError::NamespaceKindMismatch(actual),
            ..
        }) if actual == child_file_id
    ));
    Ok(())
}

#[test]
fn closure_rejects_kinds_outside_the_declared_volume_profile()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
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
        )?,
    )?;
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
        )?,
    )?;
    let generation = put(
        &store,
        ObjectKind::GenerationRoot,
        encode_generation_root(&GenerationRoot {
            volume_id: VolumeId::from_bytes([2; 16]),
            root_file_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        })?,
    )?;
    let failure = prove_generation_closure(&store, generation, limits(), WorkBudget::UNBOUNDED)
        .err()
        .ok_or("portable FIFO closure unexpectedly proved")?;
    assert!(matches!(
        failure.error,
        ClosureError::UnsupportedVolumeSemantics
    ));
    Ok(())
}

#[test]
fn extent_closure_rejects_missing_out_of_bounds_and_unavailable_storage()
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
    )?;
    let missing = prove_generation_closure(
        &missing_store,
        missing_generation,
        limits(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("missing extent blob unexpectedly proved")?;
    assert!(matches!(
        missing.error,
        ClosureError::Storage(ObjectStoreError::Missing)
    ));
    assert!(missing.work.object_probes > 0);

    let outside_store = MemoryObjectStore::default();
    let four_bytes = blob_with_bytes(&outside_store, b"data")?;
    let outside_generation = generation_with_regular_extent(
        &outside_store,
        ExtentKind::Content {
            object: four_bytes,
            object_offset: 3,
        },
        2,
    )?;
    assert!(matches!(
        prove_generation_closure(
            &outside_store,
            outside_generation,
            limits(),
            WorkBudget::UNBOUNDED,
        ),
        Err(OperationFailure {
            error: ClosureError::BlobSpanOutsideObject,
            ..
        })
    ));

    for sparse_kind in [ExtentKind::Hole, ExtentKind::AllocatedZero] {
        let sparse_store = MemoryObjectStore::default();
        let sparse_generation = generation_with_regular_extent(&sparse_store, sparse_kind, 4)?;
        let mut dense_limits = limits();
        dense_limits.sparse_files = false;
        assert!(matches!(
            prove_generation_closure(
                &sparse_store,
                sparse_generation,
                dense_limits,
                WorkBudget::UNBOUNDED,
            ),
            Err(OperationFailure {
                error: ClosureError::UnsupportedVolumeSemantics,
                ..
            })
        ));
    }
    Ok(())
}

#[test]
fn async_and_sync_closure_proofs_are_identical_and_cancellable()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, _, _) = minimal_generation(&store, None)?;
    let synchronous =
        prove_generation_closure(&store, generation, limits(), WorkBudget::UNBOUNDED)?;
    let asynchronous = async_storage::poll_ready(prove_generation_closure_async(
        &store,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed asynchronous closure unexpectedly blocked")??;
    assert_eq!(asynchronous, synchronous);

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = async_storage::poll_ready(prove_generation_closure_async(
        &store,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cancelled asynchronous closure unexpectedly blocked")?
    .err()
    .ok_or("cancelled asynchronous closure unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ClosureError::Storage(ObjectStoreError::Cancelled)
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn owned_generation_closure_reads_release_all_retained_backend_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let (generation, _, _) = minimal_generation(&store.inner, None)?;
    let proof = async_storage::poll_ready(prove_generation_closure_async(
        &store,
        generation,
        limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("owned closure proof blocked")??;
    assert_eq!(proof.file_count, 1);
    assert_eq!(proof.object_count, 4);
    assert!(proof.work.bytes_copied > 0);
    assert!(proof.work.peak_allocation_bytes > 0);
    Ok(())
}

#[test]
fn forged_file_table_lower_bound_fails_before_publication() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let metadata_id = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
    let tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
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
        )?,
    )?;
    let forged = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Internal(vec![FileTableChild {
                first_file_id: FileId::from_bytes([0; 16]),
                page: leaf,
            }]),
            8,
        )?,
    )?;
    let (generation, _, _) = minimal_generation(&store, Some(forged))?;
    assert!(matches!(
        prove_generation_closure(&store, generation, limits(), WorkBudget::UNBOUNDED),
        Err(OperationFailure {
            error: ClosureError::RoutingBoundsMismatch,
            ..
        })
    ));
    Ok(())
}

#[test]
fn missing_metadata_preserves_backend_failure_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let missing = ObjectId {
        kind: ObjectKind::Metadata,
        digest: Digest::from_bytes([7; 32]),
    };
    let tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
    let table = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![FileRecord {
                file_id: FileId::from_bytes([1; 16]),
                kind: FileKind::Directory,
                link_count: 1,
                metadata: missing,
                payload: FilePayload::Directory { entries: tree },
            }]),
            8,
        )?,
    )?;
    let (generation, _, _) = minimal_generation(&store, Some(table))?;
    let failure = prove_generation_closure(&store, generation, limits(), WorkBudget::UNBOUNDED)
        .err()
        .ok_or("missing metadata unexpectedly proved")?;
    assert!(matches!(
        failure.error,
        ClosureError::Storage(ObjectStoreError::Missing)
    ));
    assert!(failure.work.object_probes >= 3);
    Ok(())
}

#[test]
fn shared_metadata_is_authenticated_once() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata_id = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
    let root_id = FileId::from_bytes([1; 16]);
    let child_id = FileId::from_bytes([2; 16]);
    let tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"child".to_vec(), 255)?,
                file_id: child_id,
                kind: FileKind::Regular,
            }]),
            8,
        )?,
    )?;
    let generation = generation_with_records(
        &store,
        root_id,
        vec![
            directory_record(root_id, 1, metadata_id, tree),
            FileRecord {
                file_id: child_id,
                kind: FileKind::Regular,
                link_count: 1,
                metadata: metadata_id,
                payload: FilePayload::InlineRegular(InlineFileData::new(b"body")?),
            },
        ],
    )?;

    let proof = prove_generation_closure(&store, generation, limits(), WorkBudget::UNBOUNDED)?;
    assert_eq!(proof.file_count, 2);
    assert_eq!(proof.object_count, 4);
    assert_eq!(proof.work.object_probes, 4);
    assert_eq!(proof.work.backend_read_operations, 4);
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn namespace_proof_rejects_link_counts_directory_links_and_disconnected_cycles()
-> Result<(), Box<dyn std::error::Error>> {
    let root_id = FileId::from_bytes([1; 16]);
    let child_id = FileId::from_bytes([2; 16]);

    let mismatch_store = MemoryObjectStore::default();
    let metadata_id = put(
        &mismatch_store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
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
        )?,
    )?;
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
    )?;
    assert!(matches!(
        prove_generation_closure(
            &mismatch_store,
            mismatch,
            limits(),
            WorkBudget::UNBOUNDED,
        ),
        Err(OperationFailure {
            error: ClosureError::LinkCountMismatch(actual),
            ..
        }) if actual == child_id
    ));

    let hard_link_store = MemoryObjectStore::default();
    let metadata_id = put(
        &hard_link_store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
    let empty_tree = put(
        &hard_link_store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
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
        )?,
    )?;
    let hard_link = generation_with_records(
        &hard_link_store,
        root_id,
        vec![
            directory_record(root_id, 1, metadata_id, root_tree),
            directory_record(child_id, 2, metadata_id, empty_tree),
        ],
    )?;
    assert!(matches!(
        prove_generation_closure(
            &hard_link_store,
            hard_link,
            limits(),
            WorkBudget::UNBOUNDED,
        ),
        Err(OperationFailure {
            error: ClosureError::DirectoryHardLink(actual),
            ..
        }) if actual == child_id
    ));

    let disconnected_store = MemoryObjectStore::default();
    let metadata_id = put(
        &disconnected_store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
    let root_tree = put(
        &disconnected_store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
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
        )?,
    )?;
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
        )?,
    )?;
    let disconnected = generation_with_records(
        &disconnected_store,
        root_id,
        vec![
            directory_record(root_id, 1, metadata_id, root_tree),
            directory_record(first_id, 1, metadata_id, first_tree),
            directory_record(second_id, 1, metadata_id, second_tree),
        ],
    )?;
    assert!(matches!(
        prove_generation_closure(
            &disconnected_store,
            disconnected,
            limits(),
            WorkBudget::UNBOUNDED,
        ),
        Err(OperationFailure {
            error: ClosureError::UnreachableFileRecords,
            ..
        })
    ));
    Ok(())
}

#[test]
fn authenticated_routing_bound_helpers_are_total() -> Result<(), Box<dyn std::error::Error>> {
    let file_a = FileId::from_bytes([1; 16]);
    let file_b = FileId::from_bytes([2; 16]);
    let file_children = vec![FileTableChild {
        first_file_id: file_b,
        page: ObjectId {
            kind: ObjectKind::FileTablePage,
            digest: Digest::ZERO,
        },
    }];
    assert!(validate_file_child_bounds(&file_children, Some(file_b), None).is_ok());
    assert!(matches!(
        validate_file_child_bounds(&file_children, Some(file_a), None),
        Err(ClosureError::RoutingBoundsMismatch)
    ));
    assert!(matches!(
        validate_file_child_bounds(&file_children, None, Some(file_b)),
        Err(ClosureError::RoutingBoundsMismatch)
    ));

    let a = LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 1)?;
    let b = LogicalName::new(NameEncoding::Utf8, b"b".to_vec(), 1)?;
    let tree_entries = vec![TreeEntry {
        name: b.clone(),
        file_id: file_b,
        kind: FileKind::Regular,
    }];
    assert!(validate_tree_bounds(&tree_entries, Some(&b), None).is_ok());
    assert!(matches!(
        validate_tree_bounds(&tree_entries, Some(&a), None),
        Err(ClosureError::RoutingBoundsMismatch)
    ));
    assert!(matches!(
        validate_tree_bounds(&tree_entries, None, Some(&b)),
        Err(ClosureError::RoutingBoundsMismatch)
    ));
    let tree_children = vec![TreeChild {
        first_name: b.clone(),
        page: ObjectId {
            kind: ObjectKind::TreePage,
            digest: Digest::ZERO,
        },
    }];
    assert!(validate_tree_child_bounds(&tree_children, Some(&b), None).is_ok());
    assert!(matches!(
        validate_tree_child_bounds(&tree_children, Some(&a), None),
        Err(ClosureError::RoutingBoundsMismatch)
    ));
    assert!(matches!(
        validate_tree_child_bounds(&tree_children, None, Some(&b)),
        Err(ClosureError::RoutingBoundsMismatch)
    ));

    assert!(validate_extent_bounds(&[], 0, 0).is_ok());
    let extents = vec![Extent {
        offset: 2,
        length: 3,
        kind: ExtentKind::Hole,
    }];
    assert!(validate_extent_bounds(&extents, 2, 5).is_ok());
    assert!(matches!(
        validate_extent_bounds(&extents, 1, 5),
        Err(ClosureError::RoutingBoundsMismatch)
    ));

    let attr_a = AttributeName::new(AttributeClass::PosixXattr, b"a".to_vec(), 1)?;
    let attr_b = AttributeName::new(AttributeClass::PosixXattr, b"b".to_vec(), 1)?;
    let attribute_entries = vec![AttributeEntry {
        name: attr_b.clone(),
        value_bytes: 0,
        value: ObjectId {
            kind: ObjectKind::Blob,
            digest: Digest::ZERO,
        },
    }];
    assert!(validate_attribute_bounds(&attribute_entries, Some(&attr_b), None).is_ok());
    assert!(matches!(
        validate_attribute_bounds(&attribute_entries, Some(&attr_a), None),
        Err(ClosureError::RoutingBoundsMismatch)
    ));
    assert!(matches!(
        validate_attribute_bounds(&attribute_entries, None, Some(&attr_b)),
        Err(ClosureError::RoutingBoundsMismatch)
    ));
    let attribute_children = vec![AttributeChild {
        first_name: attr_b.clone(),
        page: ObjectId {
            kind: ObjectKind::AttributePage,
            digest: Digest::ZERO,
        },
    }];
    assert!(validate_attribute_child_bounds(&attribute_children, Some(&attr_b), None).is_ok());
    assert!(matches!(
        validate_attribute_child_bounds(&attribute_children, Some(&attr_a), None),
        Err(ClosureError::RoutingBoundsMismatch)
    ));
    assert!(matches!(
        validate_attribute_child_bounds(&attribute_children, None, Some(&attr_b)),
        Err(ClosureError::RoutingBoundsMismatch)
    ));
    Ok(())
}
