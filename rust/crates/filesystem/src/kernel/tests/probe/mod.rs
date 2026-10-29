use super::*;
use crate::ObjectKind;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::VolumeId;
use crate::kernel::{
    BlobChunkRef, BlobNode, BlobPage, CheckoutDependencies, Dependency, Extent, ExtentPage,
    FileKind, FileMetadata, FileTablePage, GenerationRoot, InlineFileData, LogicalName,
    MetadataField, NameEncoding, RebaseDecision, TreeEntry, TreePage, classify_rebase_async,
    encode_extent_page,
};
use crate::memory::MemoryObjectStore;
use acyclic_stream::OwnedBytes;
use std::future::Future;
use std::task::{Context, Poll, Waker};

use crate::test_support::put_object as put;
use crate::test_support::{
    put_blob_page, put_extent_page, put_file_metadata, put_file_table_page, put_generation_root,
    put_tree_page,
};

async fn fixture(
    store: &MemoryObjectStore,
    inserted_name: Option<&str>,
) -> Result<(GenerationId, FileId, ObjectId), Box<dyn std::error::Error>> {
    let metadata = put_file_metadata(store, FileMetadata::default()).await?;
    let file_id = FileId::from_bytes([1; 16]);
    let child_id = FileId::from_bytes([3; 16]);
    let tree_entries = inserted_name
        .map(|value| {
            Ok(TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, value.as_bytes().to_vec(), 255)?,
                file_id: child_id,
                kind: FileKind::Fifo,
            })
        })
        .into_iter()
        .collect::<Result<Vec<_>, super::super::TreePageError>>()?;
    let entries = put_tree_page(store, &TreePage::Leaf(tree_entries), 8).await?;
    let mut records = vec![FileRecord {
        file_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata,
        payload: FilePayload::Directory { entries },
    }];
    if inserted_name.is_some() {
        records.push(FileRecord {
            file_id: child_id,
            kind: FileKind::Fifo,
            link_count: 1,
            metadata,
            payload: FilePayload::Empty,
        });
    }
    let table = put_file_table_page(store, &FileTablePage::Leaf(records), 8).await?;
    let root = put_generation_root(
        store,
        &GenerationRoot {
            volume_id: VolumeId::from_bytes([2; 16]),
            root_file_id: file_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        },
    )
    .await?;
    Ok((GenerationId::from_root(root), file_id, metadata))
}

async fn non_regular_fixture(
    store: &MemoryObjectStore,
) -> Result<(GenerationId, Vec<FileId>), Box<dyn std::error::Error>> {
    let metadata = put_file_metadata(store, FileMetadata::default()).await?;
    let root_id = FileId::from_bytes([11; 16]);
    let symbolic_link_id = FileId::from_bytes([12; 16]);
    let empty_id = FileId::from_bytes([13; 16]);
    let device_id = FileId::from_bytes([14; 16]);
    let reparse_id = FileId::from_bytes([15; 16]);
    let empty_tree = put_tree_page(store, &TreePage::Leaf(Vec::new()), 8).await?;
    let absent_blob = ObjectId {
        storage: None,
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([16; 32]),
    };
    let records = vec![
        FileRecord {
            file_id: root_id,
            kind: FileKind::Directory,
            link_count: 1,
            metadata,
            payload: FilePayload::Directory {
                entries: empty_tree,
            },
        },
        FileRecord {
            file_id: symbolic_link_id,
            kind: FileKind::SymbolicLink,
            link_count: 1,
            metadata,
            payload: FilePayload::SymbolicLink {
                target_bytes: 3,
                target: absent_blob,
            },
        },
        FileRecord {
            file_id: empty_id,
            kind: FileKind::Fifo,
            link_count: 1,
            metadata,
            payload: FilePayload::Empty,
        },
        FileRecord {
            file_id: device_id,
            kind: FileKind::CharacterDevice,
            link_count: 1,
            metadata,
            payload: FilePayload::Device { major: 1, minor: 3 },
        },
        FileRecord {
            file_id: reparse_id,
            kind: FileKind::ReparsePoint,
            link_count: 1,
            metadata,
            payload: FilePayload::ReparsePoint {
                payload_bytes: 3,
                payload: absent_blob,
            },
        },
    ];
    let table = put_file_table_page(store, &FileTablePage::Leaf(records), 8).await?;
    let root = put_generation_root(
        store,
        &GenerationRoot {
            volume_id: VolumeId::from_bytes([17; 16]),
            root_file_id: root_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        },
    )
    .await?;
    Ok((
        GenerationId::from_root(root),
        vec![symbolic_link_id, empty_id, device_id, reparse_id],
    ))
}

struct SparseProbeFixture {
    generation: GenerationId,
    root_id: FileId,
    inline_id: FileId,
    regular_id: FileId,
    regular_name: LogicalName,
}

async fn sparse_probe_fixture(
    store: &MemoryObjectStore,
) -> Result<SparseProbeFixture, Box<dyn std::error::Error>> {
    let metadata = put_file_metadata(store, FileMetadata::default()).await?;
    let chunk = put(store, ObjectKind::BlobChunk, b"DATA".to_vec()).await?;
    let blob = put_blob_page(
        store,
        &BlobPage {
            first_offset: 0,
            end_offset: 4,
            node: BlobNode::Leaf(vec![BlobChunkRef {
                first_offset: 0,
                end_offset: 4,
                chunk,
            }]),
        },
        8,
    )
    .await?;
    let extents = put_extent_page(
        store,
        &ExtentPage::Leaf(vec![
            Extent {
                offset: 0,
                length: 4,
                kind: ExtentKind::Hole,
            },
            Extent {
                offset: 4,
                length: 4,
                kind: ExtentKind::AllocatedZero,
            },
            Extent {
                offset: 8,
                length: 4,
                kind: ExtentKind::Content {
                    object: blob,
                    object_offset: 0,
                },
            },
            Extent {
                offset: 12,
                length: 4,
                kind: ExtentKind::Hole,
            },
        ]),
        8,
    )
    .await?;
    let root_id = FileId::from_bytes([21; 16]);
    let inline_id = FileId::from_bytes([22; 16]);
    let regular_id = FileId::from_bytes([23; 16]);
    let inline_name = LogicalName::new(NameEncoding::Utf8, b"inline".to_vec(), 255)?;
    let regular_name = LogicalName::new(NameEncoding::Utf8, b"regular".to_vec(), 255)?;
    let tree = put_tree_page(
        store,
        &TreePage::Leaf(vec![
            TreeEntry {
                name: inline_name,
                file_id: inline_id,
                kind: FileKind::Regular,
            },
            TreeEntry {
                name: regular_name.clone(),
                file_id: regular_id,
                kind: FileKind::Regular,
            },
        ]),
        8,
    )
    .await?;
    let table = put_file_table_page(
        store,
        &FileTablePage::Leaf(vec![
            FileRecord {
                file_id: root_id,
                kind: FileKind::Directory,
                link_count: 1,
                metadata,
                payload: FilePayload::Directory { entries: tree },
            },
            FileRecord {
                file_id: inline_id,
                kind: FileKind::Regular,
                link_count: 1,
                metadata,
                payload: FilePayload::InlineRegular(InlineFileData::new(b"abcdefgh")?),
            },
            FileRecord {
                file_id: regular_id,
                kind: FileKind::Regular,
                link_count: 1,
                metadata,
                payload: FilePayload::Regular {
                    logical_bytes: 16,
                    extents,
                },
            },
        ]),
        8,
    )
    .await?;
    let root = put_generation_root(
        store,
        &GenerationRoot {
            volume_id: VolumeId::from_bytes([24; 16]),
            root_file_id: root_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        },
    )
    .await?;
    Ok(SparseProbeFixture {
        generation: GenerationId::from_root(root),
        root_id,
        inline_id,
        regular_id,
        regular_name,
    })
}

mod content;
mod limits;
mod summaries;
