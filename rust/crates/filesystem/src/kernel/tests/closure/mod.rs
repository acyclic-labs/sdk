use super::*;
use crate::foundation::{Digest, VolumeId};
use crate::kernel::{
    AttributeClass, AttributeEntry, BlobChild, BlobChunkRef, BlobPage, Extent, ExtentChild,
    FileKind, FileMetadata, InlineFileData, NameEncoding,
};
use crate::memory::MemoryObjectStore;
use crate::test_support::OwnedReadObjectStore;
use crate::{CachedObjectStore, ObjectCacheOptions};

use crate::kernel::Child;
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
    FileMetadata::default()
}

use crate::test_support::{
    put_attribute_page, put_blob_page, put_extent_page,
    put_file_metadata, put_file_table_page, put_generation_root,
    put_tree_page,
};
use crate::test_support::put_object as put;

async fn minimal_generation(
    store: &MemoryObjectStore,
    file_table: Option<ObjectId>,
) -> Result<(ObjectId, FileId, ObjectId), Box<dyn std::error::Error>> {
    let metadata = put_file_metadata(
        store,
        metadata(),
    )
    .await?;
    let tree = put_tree_page(
        store,
        &TreePage::Leaf(Vec::new()),
        8,
    )
    .await?;
    let root_file_id = FileId::from_bytes([1; 16]);
    let table = match file_table {
        Some(table) => table,
        None => {
            put_file_table_page(
                store,
                &FileTablePage::Leaf(vec![FileRecord {
                    file_id: root_file_id,
                    kind: FileKind::Directory,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Directory { entries: tree },
                }]),
                8,
            )
            .await?
        }
    };
    let generation = put_generation_root(
        store,
        &GenerationRoot {
            volume_id: VolumeId::from_bytes([2; 16]),
            root_file_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        },
    )
    .await?;
    Ok((generation, root_file_id, metadata))
}

async fn generation_with_records(
    store: &MemoryObjectStore,
    root_file_id: FileId,
    records: Vec<FileRecord>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let table = put_file_table_page(
        store,
        &FileTablePage::Leaf(records),
        16,
    )
    .await?;
    put_generation_root(
        store,
        &GenerationRoot {
            volume_id: VolumeId::from_bytes([3; 16]),
            root_file_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        },
    )
    .await
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

async fn generation_with_regular_extent(
    store: &MemoryObjectStore,
    extent_kind: ExtentKind,
    logical_bytes: u64,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let metadata_id = put_file_metadata(
        store,
        metadata(),
    )
    .await?;
    let root_id = FileId::from_bytes([1; 16]);
    let child_id = FileId::from_bytes([2; 16]);
    let entries = put_tree_page(
        store,
        &TreePage::Leaf(vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"file".to_vec(), 255)?,
            file_id: child_id,
            kind: FileKind::Regular,
        }]),
        8,
    )
    .await?;
    let extents = put_extent_page(
        store,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: logical_bytes,
            kind: extent_kind,
        }]),
        8,
    )
    .await?;
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
    .await
}

async fn blob_with_bytes(
    store: &MemoryObjectStore,
    bytes: &'static [u8],
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let chunk = put(store, ObjectKind::BlobChunk, bytes.to_vec()).await?;
    put_blob_page(
        store,
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
    )
    .await
}

async fn internal_blob(store: &MemoryObjectStore) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let first_chunk = put(store, ObjectKind::BlobChunk, b"ab".to_vec()).await?;
    let second_chunk = put(store, ObjectKind::BlobChunk, b"cd".to_vec()).await?;
    let first = put_blob_page(
        store,
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
    )
    .await?;
    let second = put_blob_page(
        store,
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
    )
    .await?;
    put_blob_page(
        store,
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
    )
    .await
}

fn attribute_name(value: &str) -> Result<AttributeName, Box<dyn std::error::Error>> {
    Ok(AttributeName::new(
        AttributeClass::PosixXattr,
        value.as_bytes().to_vec(),
        255,
    )?)
}

include!("authentication.rs");
include!("routing.rs");
include!("namespace.rs");
include!("limits.rs");
