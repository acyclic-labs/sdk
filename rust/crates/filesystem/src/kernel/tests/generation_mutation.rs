use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::{Digest, VolumeId};
use crate::kernel::{
    BlobBuildOptions, BlobNode, ExtentKind, ExtentMutationError, ExtentPage, FileMetadata,
    FileTablePage, InlineFileData, MAXIMUM_INLINE_FILE_BYTES, NameEncoding, TreePage,
    TreeReadError, build_blob, decode_blob_page, decode_extent_page, decode_file_table_page,
    lookup_file_record,
    lookup_path,
};
use crate::memory::MemoryObjectStore;
use crate::model::Lifecycle;
use crate::storage::{ObjectId, ObjectKind};
use crate::tests::support::{put_file_metadata, put_tree_page, put_file_table_page};
use acyclic_stream::OwnedBytes;
use std::collections::{BTreeMap, BTreeSet};

struct Fixture {
    generation: GenerationRoot,
    metadata: ObjectId,
    changed_metadata: ObjectId,
    empty_tree: ObjectId,
}

fn config() -> VolumeConfig {
    VolumeConfig::portable(Lifecycle::Ephemeral)
}

fn metadata_value(modified: MetadataField<i64>) -> FileMetadata {
    FileMetadata { modified_ns: modified, ..FileMetadata::default() }
}

fn name(value: &str) -> Result<LogicalName, Box<dyn std::error::Error>> {
    Ok(LogicalName::new(
        NameEncoding::Utf8,
        value.as_bytes().to_vec(),
        config().limits.maximum_component_bytes,
    )?)
}

fn path(values: &[&str]) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    Ok(NamespacePath::new(
        values
            .iter()
            .map(|value| name(value))
            .collect::<Result<Vec<_>, _>>()?,
        config().limits,
    )?)
}

fn regular(file_id: FileId, metadata: ObjectId) -> Result<FileRecord, Box<dyn std::error::Error>> {
    Ok(FileRecord {
        file_id,
        kind: FileKind::Regular,
        link_count: 1,
        metadata,
        payload: FilePayload::InlineRegular(InlineFileData::new(b"payload")?),
    })
}

async fn blob(
    store: &MemoryObjectStore,
    bytes: &[u8],
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let mut source = std::io::Cursor::new(bytes);
    Ok(build_blob(
        store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 64,
            page_items: 8,
            page_bytes: 4096,
            maximum_blob_bytes: u64::try_from(bytes.len())?,
        },
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .root)
}

async fn inline_at(
    store: &MemoryObjectStore,
    generation: &GenerationRoot,
    value: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let record = lookup_path(
        store,
        generation,
        &path(&[value])?,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .record
    .ok_or("expected regular-file path is absent")?;
    let FilePayload::InlineRegular(data) = record.payload else {
        return Err("expected inline regular-file payload".into());
    };
    Ok(data.as_bytes().to_vec())
}

async fn fixture(store: &MemoryObjectStore) -> Result<Fixture, Box<dyn std::error::Error>> {
    let limits = config().limits;
    let metadata = put_file_metadata(store, metadata_value(MetadataField::Value(1)))
    .await?;
    let changed_metadata = put_file_metadata(store, metadata_value(MetadataField::Value(2)))
    .await?;
    let empty_tree = put_tree_page(store, &TreePage::Leaf(Vec::new()), limits.maximum_directory_page_entries)
    .await?;
    let root_id = FileId::from_bytes([1; 16]);
    let file_id = FileId::from_bytes([2; 16]);
    let directory_id = FileId::from_bytes([3; 16]);
    let root_tree = put_tree_page(store, &TreePage::Leaf(vec![
                TreeEntry {
                    name: name("a")?,
                    file_id,
                    kind: FileKind::Regular,
                },
                TreeEntry {
                    name: name("dir")?,
                    file_id: directory_id,
                    kind: FileKind::Directory,
                },
            ]), limits.maximum_directory_page_entries)
    .await?;
    let file_table = put_file_table_page(store, &FileTablePage::Leaf(vec![
                FileRecord {
                    file_id: root_id,
                    kind: FileKind::Directory,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Directory { entries: root_tree },
                },
                regular(file_id, metadata)?,
                FileRecord {
                    file_id: directory_id,
                    kind: FileKind::Directory,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Directory {
                        entries: empty_tree,
                    },
                },
            ]), limits.maximum_directory_page_entries)
    .await?;
    Ok(Fixture {
        generation: GenerationRoot {
            volume_id: VolumeId::from_bytes([9; 16]),
            root_file_id: root_id,
            file_table,
            parents: Vec::new(),
            required_features: 0,
        },
        metadata,
        changed_metadata,
        empty_tree,
    })
}

fn missing_file_table_generation(fixture: &Fixture) -> GenerationRoot {
    GenerationRoot {
        file_table: ObjectId {
            kind: ObjectKind::FileTablePage,
            digest: Digest::from_bytes([0xff; 32]),
        },
        ..fixture.generation.clone()
    }
}

#[derive(Clone, Debug)]
struct GenerationModel {
    bindings: BTreeMap<String, FileId>,
    bytes: BTreeMap<FileId, Vec<u8>>,
    identities_seen: BTreeSet<FileId>,
    metadata: ObjectId,
}

impl GenerationModel {
    fn fixture(metadata: ObjectId) -> Self {
        Self {
            bindings: BTreeMap::from([("a".to_owned(), FileId::from_bytes([2; 16]))]),
            bytes: BTreeMap::from([(FileId::from_bytes([2; 16]), b"payload".to_vec())]),
            identities_seen: BTreeSet::from([FileId::from_bytes([2; 16])]),
            metadata,
        }
    }

    fn prune(&mut self, file_id: FileId) {
        if !self
            .bindings
            .values()
            .any(|candidate| *candidate == file_id)
        {
            self.bytes.remove(&file_id);
        }
    }

    fn link_count(&self, file_id: FileId) -> u64 {
        u64::try_from(
            self.bindings
                .values()
                .filter(|candidate| **candidate == file_id)
                .count(),
        )
        .unwrap_or(u64::MAX)
    }
}

fn generated_next(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(2_862_933_555_777_941_757)
        .wrapping_add(3_037_000_493);
    *seed
}

fn selected_name<'a>(names: &'a [String], seed: &mut u64) -> &'a str {
    let index = usize::try_from(generated_next(seed) % u64::try_from(names.len()).unwrap_or(1))
        .unwrap_or(0);
    &names[index]
}

async fn append_blob_bytes(
    store: &MemoryObjectStore,
    root: ObjectId,
    output: &mut Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let encoded = AsyncObjectStore::read(store, root, u64::MAX, &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))
                .await?;
    let page = decode_blob_page(&encoded, DecodeLimits::default(), &mut |_| {
        Ok::<_, crate::kernel::CanonicalDecodeError>(())
    })?;
    match page.node {
        BlobNode::Leaf(chunks) => {
            for chunk in chunks {
                let bytes = AsyncObjectStore::read(
                        store,
                        chunk.chunk,
                        u64::MAX,
                        &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()),
                    )
                    .await?;
                output.extend_from_slice(&bytes);
            }
        }
        BlobNode::Internal(children) => {
            for child in children {
                Box::pin(append_blob_bytes(store, child.page, output)).await?;
            }
        }
    }
    Ok(())
}

async fn collect_extents(
    store: &MemoryObjectStore,
    root: ObjectId,
    output: &mut Vec<crate::kernel::Extent>,
) -> Result<(), Box<dyn std::error::Error>> {
    let encoded = AsyncObjectStore::read(store, root, u64::MAX, &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))
                .await?;
    match decode_extent_page(&encoded, DecodeLimits::default(), &mut |_| {
        Ok::<_, crate::kernel::CanonicalDecodeError>(())
    })? {
        ExtentPage::Leaf(extents) => output.extend(extents),
        ExtentPage::Internal(children) => {
            for child in children {
                Box::pin(collect_extents(store, child.page, output)).await?;
            }
        }
    }
    Ok(())
}

async fn record_bytes(
    store: &MemoryObjectStore,
    record: FileRecord,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    match record.payload {
        FilePayload::InlineRegular(data) => Ok(data.as_bytes().to_vec()),
        FilePayload::Regular {
            logical_bytes,
            extents,
        } => {
            let mut output = vec![0; usize::try_from(logical_bytes)?];
            let mut decoded = Vec::new();
            collect_extents(store, extents, &mut decoded).await?;
            for extent in decoded {
                if let ExtentKind::Content {
                    object,
                    object_offset,
                } = extent.kind
                {
                    let mut blob = Vec::new();
                    append_blob_bytes(store, object, &mut blob).await?;
                    let source_start = usize::try_from(object_offset)?;
                    let source_end = source_start
                        .checked_add(usize::try_from(extent.length)?)
                        .ok_or("modeled extent source overflow")?;
                    let destination_start = usize::try_from(extent.offset)?;
                    let destination_end = destination_start
                        .checked_add(usize::try_from(extent.length)?)
                        .ok_or("modeled extent destination overflow")?;
                    output[destination_start..destination_end]
                        .copy_from_slice(&blob[source_start..source_end]);
                }
            }
            Ok(output)
        }
        _ => Err("modeled identity stopped being a regular file".into()),
    }
}

async fn collect_file_table_records(
    store: &MemoryObjectStore,
    root: ObjectId,
    records: &mut BTreeMap<FileId, FileRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    let encoded = AsyncObjectStore::read(store, root, u64::MAX, &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new()))
                .await?;
    match decode_file_table_page(&encoded, DecodeLimits::default(), &mut |_| {
        Ok::<_, crate::kernel::CanonicalDecodeError>(())
    })? {
        FileTablePage::Leaf(values) => {
            for record in values {
                if records.insert(record.file_id, record).is_some() {
                    return Err("file table repeated one stable identity".into());
                }
            }
        }
        FileTablePage::Internal(children) => {
            for child in children {
                Box::pin(collect_file_table_records(store, child.page, records)).await?;
            }
        }
    }
    Ok(())
}

async fn assert_generation_matches_model(
    store: &MemoryObjectStore,
    generation: &GenerationRoot,
    model: &GenerationModel,
    names: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(generation.root_file_id, FileId::from_bytes([1; 16]));
    for candidate in names {
        let found = lookup_path(
            store,
            generation,
            &path(&[candidate])?,
            config(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .record;
        match (model.bindings.get(candidate), found) {
            (None, None) => {}
            (Some(expected), Some(record)) => {
                assert_eq!(record.file_id, *expected, "binding {candidate}");
                assert_eq!(record.kind, FileKind::Regular);
                assert_eq!(record.metadata, model.metadata);
                assert_eq!(record.link_count, model.link_count(*expected));
                if model.bytes[expected].len() > MAXIMUM_INLINE_FILE_BYTES {
                    assert!(matches!(record.payload, FilePayload::Regular { .. }));
                }
                assert_eq!(record_bytes(store, record).await?, model.bytes[expected]);
            }
            state => return Err(format!("binding {candidate} diverged: {state:?}").into()),
        }
    }
    for file_id in &model.identities_seen {
        let found = lookup_file_record(
            store,
            generation.file_table,
            *file_id,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
        match (model.bytes.get(file_id), found) {
            (None, None) => {}
            (Some(expected), Some(record)) => {
                assert_eq!(record.kind, FileKind::Regular);
                assert_eq!(record.metadata, model.metadata);
                assert_eq!(record.link_count, model.link_count(*file_id));
                if expected.len() > MAXIMUM_INLINE_FILE_BYTES {
                    assert!(matches!(record.payload, FilePayload::Regular { .. }));
                }
                assert_eq!(record_bytes(store, record).await?, *expected);
            }
            state => return Err(format!("identity {file_id:?} diverged: {state:?}").into()),
        }
    }
    let mut actual_records = BTreeMap::new();
    collect_file_table_records(store, generation.file_table, &mut actual_records).await?;
    let expected_ids = model
        .bytes
        .keys()
        .copied()
        .chain([FileId::from_bytes([1; 16]), FileId::from_bytes([3; 16])])
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual_records.keys().copied().collect::<BTreeSet<_>>(),
        expected_ids
    );
    Ok(())
}

mod mutations;
mod preconditions;
mod identity;
mod histories;
mod construction;
mod native_and_rename;
