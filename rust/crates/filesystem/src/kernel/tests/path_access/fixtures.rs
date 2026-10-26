use super::super::allocation::AllocationLedger;
use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::{FileId, VolumeId};
use crate::kernel::{
    FileMetadata, FileTablePage, GenerationRoot, InlineFileData, LogicalName, MetadataField,
    NameEncoding, TreeEntry, TreePage, encode_file_metadata, encode_file_table_page,
    encode_tree_page,
};
use crate::memory::MemoryObjectStore;
use crate::model::{CaseSensitivity, ConcurrencyMode, Lifecycle, UnicodePolicy, VolumeLimits};
use crate::storage::{
    ObjectId, ObjectKind, ObjectRead, ObjectReadRequest, ObjectStoreError
};
use acyclic_stream::OwnedBytes;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::kernel::Child;
fn setup_cache<'a, S>(
    store: &'a S,
    entries: usize,
    budget: WorkBudget,
    custody: Option<&'a super::super::ProofDataCustody>,
) -> Result<(OperationReadCache<'a, S>, WorkCounters), PathLookupFailure> {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(budget, &cancellation);
    let result = OperationReadCache::new(store, entries, &mut accounting, custody);
    let work = accounting.spent();
    let mut cache = result.map_err(|error| OperationFailure::new(path_error(error), work))?;
    let state = cache
        .state
        .get_mut()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.accounted_bytes = 0;
    state.accounted_slot_bytes = 0;
    Ok((cache, work))
}

fn cache_resident<S>(cache: &OperationReadCache<'_, S>) -> Result<u64, WorkError> {
    let state = cache.lock_state();
    OperationReadCache::<S>::slot_bytes(&state)?
        .checked_add(state.retained_owned_bytes)
        .ok_or(WorkError::Overflow)
}

fn config() -> VolumeConfig {
    VolumeConfig::portable(Lifecycle::Ephemeral)
}

fn metadata() -> FileMetadata {
    FileMetadata::default()
}

use crate::test_support::put_object as put;

struct CancellingStore {
    inner: MemoryObjectStore,
    reads: AtomicUsize,
}

impl AsyncObjectStore for CancellingStore {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        self.inner.put(id, bytes, context).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        let read = self.inner.read(id, maximum, context).await?;
        if self.reads.fetch_add(1, Ordering::Relaxed) == 0 {
            context.cancellation.cancel();
        }
        Ok(read)
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        self.inner.read_many(requests, context).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        self.inner.contains(id, context).await
    }
}

async fn fixture(
    store: &MemoryObjectStore,
) -> Result<(GenerationRoot, NamespacePath), Box<dyn std::error::Error>> {
    let root_id = FileId::from_bytes([1; 16]);
    let src_id = FileId::from_bytes([2; 16]);
    let file_id = FileId::from_bytes([3; 16]);
    let metadata = put(
        store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let main_name = LogicalName::new(NameEncoding::Utf8, b"main.rs".to_vec(), 255)?;
    let src_tree = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: main_name.clone(),
                file_id,
                kind: FileKind::Regular,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let src_name = LogicalName::new(NameEncoding::Utf8, b"src".to_vec(), 255)?;
    let alias_name = LogicalName::new(NameEncoding::Utf8, b"alias".to_vec(), 255)?;
    let root_tree = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![
                TreeEntry {
                    name: alias_name,
                    file_id: src_id,
                    kind: FileKind::Directory,
                },
                TreeEntry {
                    name: src_name.clone(),
                    file_id: src_id,
                    kind: FileKind::Directory,
                },
            ]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let file_table = put(
        store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![
                FileRecord {
                    file_id: root_id,
                    kind: FileKind::Directory,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Directory { entries: root_tree },
                },
                FileRecord {
                    file_id: src_id,
                    kind: FileKind::Directory,
                    link_count: 2,
                    metadata,
                    payload: FilePayload::Directory { entries: src_tree },
                },
                FileRecord {
                    file_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::InlineRegular(InlineFileData::new(b"fn main() {}")?),
                },
            ]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    Ok((
        GenerationRoot {
            volume_id: VolumeId::from_bytes([4; 16]),
            root_file_id: root_id,
            file_table,
            parents: Vec::new(),
            required_features: 0,
        },
        NamespacePath::new(vec![src_name, main_name], config().limits)?,
    ))
}

async fn binding_fixture(
    store: &MemoryObjectStore,
    declared_kind: FileKind,
    recorded_kind: Option<FileKind>,
) -> Result<(GenerationRoot, NamespacePath), Box<dyn std::error::Error>> {
    let root_id = FileId::from_bytes([21; 16]);
    let child_id = FileId::from_bytes([22; 16]);
    let metadata = put(
        store,
        ObjectKind::Metadata,
        encode_file_metadata(
            metadata(),
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let child_name = LogicalName::new(NameEncoding::Utf8, b"child".to_vec(), 255)?;
    let root_tree = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: child_name.clone(),
                file_id: child_id,
                kind: declared_kind,
            }]),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    let mut records = vec![FileRecord {
        file_id: root_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata,
        payload: FilePayload::Directory { entries: root_tree },
    }];
    if let Some(kind) = recorded_kind {
        let payload = match kind {
            FileKind::Directory => {
                let empty_tree = put(
                    store,
                    ObjectKind::TreePage,
                    encode_tree_page(
                        &TreePage::Leaf(Vec::new()),
                        8,
                        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
                        |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
                    )?,
                )
                .await?;
                FilePayload::Directory {
                    entries: empty_tree,
                }
            }
            FileKind::Regular => FilePayload::InlineRegular(InlineFileData::new(b"contents")?),
            _ => return Err("test fixture supports directory or regular records".into()),
        };
        records.push(FileRecord {
            file_id: child_id,
            kind,
            link_count: 1,
            metadata,
            payload,
        });
    }
    let file_table = put(
        store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(records),
            8,
            |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
            |_| crate::kernel::CanonicalDecodeError::AllocationFailed,
        )?,
    )
    .await?;
    Ok((
        GenerationRoot {
            volume_id: VolumeId::from_bytes([23; 16]),
            root_file_id: root_id,
            file_table,
            parents: Vec::new(),
            required_features: 0,
        },
        NamespacePath::new(vec![child_name], config().limits)?,
    ))
}
