use super::*;
use crate::foundation::{Digest, FileId, VolumeId};
use crate::kernel::{
    FileMetadata, FileTablePage, GenerationRoot, InlineFileData, LogicalName, MetadataField,
    NameEncoding, TreeChild, TreeEntry, TreePage, encode_file_metadata, encode_file_table_page,
    encode_tree_page,
};
use crate::memory::MemoryObjectStore;
use crate::model::{CaseSensitivity, ConcurrencyMode, Lifecycle, UnicodePolicy, VolumeLimits};
use crate::storage::{
    ObjectFailure, ObjectId, ObjectKind, ObjectRead, ObjectReadRequest, ObjectResult, ObjectStore,
    ObjectStoreError, object_digest,
};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

fn config() -> VolumeConfig {
    VolumeConfig {
        profile: FilesystemProfile::Portable,
        concurrency: ConcurrencyMode::Optimistic,
        lifecycle: Lifecycle::Ephemeral,
        case_sensitivity: CaseSensitivity::Sensitive,
        unicode: UnicodePolicy::Preserve,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
        limits: VolumeLimits::default(),
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
    ObjectStore::put(store, object, Bytes::from(bytes), WorkBudget::UNBOUNDED)?;
    Ok(object)
}

struct CancellingStore {
    inner: MemoryObjectStore,
    reads: AtomicUsize,
}

impl AsyncObjectStore for CancellingStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::put(&self.inner, object_id, bytes, budget)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        let receipt = ObjectStore::read(&self.inner, object_id, maximum_bytes, budget)?;
        if self.reads.fetch_add(1, Ordering::Relaxed) == 0 {
            cancellation.cancel();
        }
        Ok(receipt)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::read_many(&self.inner, requests, budget)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::contains(&self.inner, object_id, budget)
    }
}

struct OwnedRetentionStore {
    inner: MemoryObjectStore,
    fail: AtomicBool,
}

impl AsyncObjectStore for OwnedRetentionStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.fail.load(Ordering::Relaxed) {
            return Err(ObjectFailure::new(
                ObjectStoreError::Corrupt,
                WorkCounters {
                    backend_write_operations: 1,
                    peak_allocation_bytes: 17,
                    ..WorkCounters::default()
                },
            ));
        }
        ObjectStore::put(&self.inner, object_id, bytes, budget)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.fail.load(Ordering::Relaxed) {
            return Err(ObjectFailure::new(
                ObjectStoreError::Corrupt,
                WorkCounters {
                    backend_read_operations: 1,
                    peak_allocation_bytes: 17,
                    ..WorkCounters::default()
                },
            ));
        }
        let receipt = ObjectStore::read(&self.inner, object_id, maximum_bytes, budget)?;
        let logical_bytes = u64::try_from(receipt.value.bytes.len()).map_err(|_| {
            ObjectFailure::new(ObjectStoreError::Work(WorkError::Overflow), receipt.work)
        })?;
        let mut work = receipt
            .work
            .checked_add(WorkCounters {
                bytes_copied: logical_bytes,
                allocation_operations: 1,
                ..WorkCounters::default()
            })
            .map_err(|error| ObjectFailure::new(error.into(), receipt.work))?;
        work.peak_allocation_bytes = work.peak_allocation_bytes.max(logical_bytes);
        work.verify(budget)
            .map_err(|error| ObjectFailure::new(error.into(), receipt.work))?;
        Ok(ObjectReceipt {
            value: ObjectRead {
                bytes: Bytes::copy_from_slice(&receipt.value.bytes),
                retention: ObjectReadRetention::Owned { logical_bytes },
            },
            work,
        })
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        ObjectStore::read_many(&self.inner, requests, budget)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
        if self.fail.load(Ordering::Relaxed) {
            return Err(ObjectFailure::new(
                ObjectStoreError::Corrupt,
                WorkCounters {
                    backend_read_operations: 1,
                    peak_allocation_bytes: 17,
                    ..WorkCounters::default()
                },
            ));
        }
        ObjectStore::contains(&self.inner, object_id, budget)
    }
}

fn fixture(
    store: &MemoryObjectStore,
) -> Result<(GenerationRoot, NamespacePath), Box<dyn std::error::Error>> {
    let root_id = FileId::from_bytes([1; 16]);
    let src_id = FileId::from_bytes([2; 16]);
    let file_id = FileId::from_bytes([3; 16]);
    let metadata = put(
        store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
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
        )?,
    )?;
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
        )?,
    )?;
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
        )?,
    )?;
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

fn binding_fixture(
    store: &MemoryObjectStore,
    declared_kind: FileKind,
    recorded_kind: Option<FileKind>,
) -> Result<(GenerationRoot, NamespacePath), Box<dyn std::error::Error>> {
    let root_id = FileId::from_bytes([21; 16]);
    let child_id = FileId::from_bytes([22; 16]);
    let metadata = put(
        store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
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
        )?,
    )?;
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
                    encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
                )?;
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
        encode_file_table_page(&FileTablePage::Leaf(records), 8)?,
    )?;
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

#[test]
fn exact_path_reads_only_required_frontiers_and_authenticates_absence()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store)?;
    let found = lookup_path(&store, &generation, &path, config(), WorkBudget::UNBOUNDED)?;
    assert_eq!(
        found.record.map(|record| record.kind),
        Some(FileKind::Regular)
    );
    assert_eq!(
        found.parent.map(|record| record.kind),
        Some(FileKind::Directory)
    );
    assert_eq!(found.resolved_components, 2);
    assert_eq!(found.work.page_reads, 5);
    assert_eq!(found.work.backend_read_operations, 3);
    let observed = async_storage::poll_ready(observe_path_async(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory observation remained pending")??;
    assert_eq!(observed.lookup.record, found.record);
    assert_eq!(observed.dependencies.len(), 3);
    assert!(matches!(
        observed.dependencies.last(),
        Some(Dependency {
            region: DependencyRegion::FileRecord(_),
            expected: DependencyState::Present(_),
        })
    ));

    let missing = NamespacePath::new(
        vec![
            LogicalName::new(NameEncoding::Utf8, b"src".to_vec(), 255)?,
            LogicalName::new(NameEncoding::Utf8, b"missing".to_vec(), 255)?,
        ],
        config().limits,
    )?;
    let absent = lookup_path(
        &store,
        &generation,
        &missing,
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(absent.record, None);
    assert_eq!(
        absent.parent.map(|record| record.kind),
        Some(FileKind::Directory)
    );
    assert_eq!(absent.resolved_components, 1);
    let observed_absent = async_storage::poll_ready(observe_path_async(
        &store,
        &generation,
        &missing,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory negative observation remained pending")??;
    assert_eq!(observed_absent.lookup.record, None);
    assert_eq!(observed_absent.dependencies.len(), 2);
    assert!(matches!(
        observed_absent.dependencies.last(),
        Some(Dependency {
            region: DependencyRegion::DirectoryName { .. },
            expected: DependencyState::Absent,
        })
    ));
    Ok(())
}

#[test]
fn observation_dependency_storage_is_admitted_with_cache_residency()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store)?;
    let cache_entries = maximum_cache_entries(&path, config())?;
    let (cache, _) = OperationReadCache::new(&store, cache_entries, WorkBudget::UNBOUNDED)?;
    let dependency_bytes = u64::try_from(path.depth().saturating_add(1))?
        .checked_mul(u64::try_from(size_of::<Dependency>())?)
        .ok_or("dependency byte count overflowed")?;
    let required_peak = cache
        .metadata_bytes
        .checked_add(dependency_bytes)
        .ok_or("combined resident byte count overflowed")?;
    drop(cache);

    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = required_peak - 1;
    let failure = async_storage::poll_ready(observe_path_async(
        &store,
        &generation,
        &path,
        config(),
        budget,
        &CancellationToken::new(),
    ))
    .ok_or("budgeted observation remained pending")?
    .err()
    .ok_or("unadmitted dependency allocation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        PathLookupError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed,
            maximum,
        }) if observed == required_peak && maximum == required_peak - 1
    ));
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.allocation_operations, 2);

    let first_name_bytes = u64::try_from(path.components()[0].as_bytes().len())?;
    let required_name_peak = required_peak
        .checked_add(first_name_bytes)
        .ok_or("dependency name byte count overflowed")?;
    let mut name_budget = WorkBudget::UNBOUNDED;
    name_budget.peak_allocation_bytes = required_name_peak - 1;
    let failure = async_storage::poll_ready(observe_path_async(
        &store,
        &generation,
        &path,
        config(),
        name_budget,
        &CancellationToken::new(),
    ))
    .ok_or("name-budgeted observation remained pending")?
    .err()
    .ok_or("unadmitted dependency name unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        PathLookupError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed,
            maximum,
        }) if observed == required_name_peak && maximum == required_name_peak - 1
    ));
    assert!(failure.work.backend_read_operations > 0);
    Ok(())
}

#[test]
fn owned_backend_reads_and_observation_names_share_one_exact_peak()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedRetentionStore {
        inner: MemoryObjectStore::default(),
        fail: AtomicBool::new(false),
    };
    let (generation, path) = fixture(&store.inner)?;
    let cancellation = CancellationToken::new();
    let shared = async_storage::poll_ready(observe_path_async(
        &store.inner,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("shared observation remained pending")??;
    let owned = async_storage::poll_ready(observe_path_async(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("owned observation remained pending")??;
    assert!(owned.lookup.record.is_some());
    assert!(owned.lookup.work.peak_allocation_bytes > shared.lookup.work.peak_allocation_bytes);

    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = owned.lookup.work.peak_allocation_bytes - 1;
    let failure = async_storage::poll_ready(observe_path_async(
        &store,
        &generation,
        &path,
        config(),
        budget,
        &cancellation,
    ))
    .ok_or("peak-limited owned observation remained pending")?
    .err()
    .ok_or("peak-limited owned observation unexpectedly succeeded")?;
    assert!(
        matches!(
            &failure.error,
            PathLookupError::Tree(TreeReadError::Storage(ObjectStoreError::Work(
                WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }
            )))
        ),
        "unexpected owned peak failure: {:?}",
        failure.error
    );
    assert!(failure.work.backend_read_operations > 0);
    assert!(failure.work.peak_allocation_bytes > shared.lookup.work.peak_allocation_bytes);
    assert!(failure.work.peak_allocation_bytes <= budget.peak_allocation_bytes);
    Ok(())
}

#[test]
fn authenticated_bindings_fail_closed_on_kind_or_record_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    for (declared, recorded) in [
        (FileKind::Regular, Some(FileKind::Directory)),
        (FileKind::Directory, Some(FileKind::Regular)),
        (FileKind::Regular, None),
    ] {
        let store = MemoryObjectStore::default();
        let (generation, path) = binding_fixture(&store, declared, recorded)?;
        let failure = lookup_path(&store, &generation, &path, config(), WorkBudget::UNBOUNDED)
            .err()
            .ok_or("inconsistent authenticated binding resolved")?;
        assert!(matches!(failure.error, PathLookupError::KindMismatch));
        assert!(failure.work.backend_read_operations > 0);
        assert!(failure.work.page_reads > 0);

        let batch_failure = lookup_paths(
            &store,
            &generation,
            std::slice::from_ref(&path),
            config(),
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("batch accepted inconsistent authenticated binding")?;
        assert!(matches!(batch_failure.error, PathLookupError::KindMismatch));
        assert!(batch_failure.work.backend_read_operations > 0);

        let observed_failure = async_storage::poll_ready(observe_path_async(
            &store,
            &generation,
            &path,
            config(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ))
        .ok_or("inconsistent observation remained pending")?
        .err()
        .ok_or("observation accepted inconsistent binding")?;
        assert!(matches!(
            observed_failure.error,
            PathLookupError::KindMismatch
        ));
        assert!(observed_failure.work.backend_read_operations > 0);
    }
    Ok(())
}

#[test]
fn batch_rejects_authenticated_child_bounds_and_preserves_failure_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store)?;
    let root_id = FileId::from_bytes([1; 16]);
    let src_id = FileId::from_bytes([2; 16]);
    let metadata = put(
        &store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata())?,
    )?;
    let leaf = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![TreeEntry {
                name: LogicalName::new(NameEncoding::Utf8, b"src".to_vec(), 255)?,
                file_id: src_id,
                kind: FileKind::Directory,
            }]),
            8,
        )?,
    )?;
    let malformed_root_tree = put(
        &store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Internal(vec![TreeChild {
                first_name: LogicalName::new(NameEncoding::Utf8, b"zzz".to_vec(), 255)?,
                page: leaf,
            }]),
            8,
        )?,
    )?;
    let file_table = put(
        &store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![FileRecord {
                file_id: root_id,
                kind: FileKind::Directory,
                link_count: 1,
                metadata,
                payload: FilePayload::Directory {
                    entries: malformed_root_tree,
                },
            }]),
            8,
        )?,
    )?;
    let malformed_generation = GenerationRoot {
        volume_id: generation.volume_id,
        root_file_id: root_id,
        file_table,
        parents: Vec::new(),
        required_features: 0,
    };
    let src = NamespacePath::new(
        vec![LogicalName::new(NameEncoding::Utf8, b"src".to_vec(), 255)?],
        config().limits,
    )?;
    let failure = lookup_paths(
        &store,
        &malformed_generation,
        &[main, src],
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("malformed routing unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        PathLookupError::Tree(TreeReadError::ChildBoundsMismatch)
    ));
    assert!(failure.work.backend_read_operations > 0);
    assert!(failure.work.page_reads > 0);
    assert!(failure.work.items_examined > 0);
    assert!(failure.work.peak_allocation_bytes > 0);
    Ok(())
}

#[test]
fn batch_cancellation_after_authenticated_progress_preserves_prior_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = CancellingStore {
        inner: MemoryObjectStore::default(),
        reads: AtomicUsize::new(0),
    };
    let (generation, path) = fixture(&store.inner)?;
    let cancellation = CancellationToken::new();
    let failure = async_storage::poll_ready(lookup_paths_async(
        &store,
        &generation,
        std::slice::from_ref(&path),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cancelling store remained pending")?
    .err()
    .ok_or("cancelled batch unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        PathLookupError::Tree(TreeReadError::Cancelled)
    ));
    assert!(failure.work.backend_read_operations >= 1);
    assert!(failure.work.page_reads >= 1);
    assert!(failure.work.peak_allocation_bytes > 0);
    Ok(())
}

#[test]
fn batch_paths_share_prefix_and_file_table_frontiers_and_preserve_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store)?;
    let missing = NamespacePath::new(
        vec![
            LogicalName::new(NameEncoding::Utf8, b"src".to_vec(), 255)?,
            LogicalName::new(NameEncoding::Utf8, b"missing".to_vec(), 255)?,
        ],
        config().limits,
    )?;
    let root = NamespacePath::new(Vec::new(), config().limits)?;
    let batch = lookup_paths(
        &store,
        &generation,
        &[main.clone(), missing, main.clone(), root.clone()],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(batch.entries.len(), 4);
    assert_eq!(
        batch.entries[0].record.map(|record| record.kind),
        Some(FileKind::Regular)
    );
    assert_eq!(batch.entries[1].record, None);
    assert_eq!(batch.entries[1].resolved_components, 1);
    assert_eq!(
        batch.entries[0].parent.map(|record| record.file_id),
        Some(FileId::from_bytes([2; 16]))
    );
    assert_eq!(batch.entries[1].parent, batch.entries[0].parent);
    assert_eq!(batch.entries[0], batch.entries[2]);
    assert_eq!(
        batch.entries[3].record.map(|record| record.kind),
        Some(FileKind::Directory)
    );
    assert_eq!(batch.entries[3].parent, None);
    assert_eq!(batch.work.page_reads, 5);
    assert_eq!(batch.work.backend_read_operations, 3);
    let borrowed_paths = [&main, &root];
    let borrowed = async_storage::poll_ready(lookup_path_refs_async(
        &store,
        &generation,
        &borrowed_paths,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("borrowed path batch blocked")??;
    let owned = lookup_paths(
        &store,
        &generation,
        &[main, root],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(borrowed, owned);
    Ok(())
}

#[test]
fn hardlinked_directory_aliases_share_one_directory_frontier()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store)?;
    let alias = NamespacePath::new(
        vec![
            LogicalName::new(NameEncoding::Utf8, b"alias".to_vec(), 255)?,
            LogicalName::new(NameEncoding::Utf8, b"main.rs".to_vec(), 255)?,
        ],
        config().limits,
    )?;
    let batch = lookup_paths(
        &store,
        &generation,
        &[main, alias],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(batch.entries[0], batch.entries[1]);
    assert_eq!(batch.work.page_reads, 5);
    assert_eq!(batch.work.backend_read_operations, 3);
    Ok(())
}

#[test]
fn copied_byte_budget_rejects_before_directory_frontier_read()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store)?;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_copied = u64::try_from(main.components()[0].as_bytes().len())
        .unwrap_or(u64::MAX)
        .saturating_sub(1);
    let failure = lookup_paths(
        &store,
        &generation,
        std::slice::from_ref(&main),
        config(),
        budget,
    )
    .err()
    .ok_or("copy budget unexpectedly admitted")?;
    assert!(matches!(
        failure.error,
        PathLookupError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied",
            ..
        })
    ));
    assert_eq!(failure.work.backend_read_operations, 1);
    assert_eq!(failure.work.page_reads, 1);
    assert!(failure.work.bytes_copied <= budget.bytes_copied);
    Ok(())
}

#[test]
fn failed_cached_backend_work_includes_simultaneous_resident_peak() {
    let prior = WorkCounters {
        items_examined: 3,
        peak_allocation_bytes: 29,
        ..WorkCounters::default()
    };
    let backend = WorkCounters {
        backend_read_operations: 1,
        peak_allocation_bytes: 17,
        ..WorkCounters::default()
    };
    let failure = merge_backend_failure(prior, backend, 101, ObjectStoreError::Corrupt);
    assert!(matches!(failure.error, ObjectStoreError::Corrupt));
    assert_eq!(failure.work.items_examined, 3);
    assert_eq!(failure.work.backend_read_operations, 1);
    assert_eq!(failure.work.peak_allocation_bytes, 118);
}

#[test]
fn operation_cache_preserves_backend_failures_resident_peak_and_poison_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let cached_bytes = Bytes::from_static(b"retained-owned-cache-entry");
    let cached = ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &cached_bytes),
    };
    ObjectStore::put(&inner, cached, cached_bytes.clone(), WorkBudget::UNBOUNDED)?;
    let store = OwnedRetentionStore {
        inner,
        fail: AtomicBool::new(false),
    };
    let (cache, _) = OperationReadCache::new(&store, 2, WorkBudget::UNBOUNDED)?;
    let cancellation = CancellationToken::new();
    let cold = async_storage::poll_ready(AsyncObjectStore::read(
        &cache,
        cached,
        u64::MAX,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("owned cache fill remained pending")??;
    assert!(matches!(
        cold.value.retention,
        ObjectReadRetention::Owned { .. }
    ));
    let resident = cache.resident_bytes()?;
    assert!(resident > u64::try_from(cached_bytes.len())?);

    store.fail.store(true, Ordering::Relaxed);
    let absent = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([252; 32]),
    };
    let write_bytes = Bytes::from_static(b"rejected-write");
    let write_id = ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &write_bytes),
    };
    let put_failure = async_storage::poll_ready(AsyncObjectStore::put(
        &cache,
        write_id,
        write_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("failing cache write remained pending")?
    .err()
    .ok_or("failing cache write unexpectedly succeeded")?;
    assert!(matches!(put_failure.error, ObjectStoreError::Corrupt));
    assert_eq!(put_failure.work.backend_write_operations, 1);
    assert_eq!(put_failure.work.peak_allocation_bytes, resident + 17);

    let read_failure = async_storage::poll_ready(AsyncObjectStore::read(
        &cache,
        absent,
        u64::MAX,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("failing cache read remained pending")?
    .err()
    .ok_or("failing cache read unexpectedly succeeded")?;
    assert!(matches!(read_failure.error, ObjectStoreError::Corrupt));
    assert_eq!(read_failure.work.backend_read_operations, 1);
    assert!(read_failure.work.items_examined > 0);
    assert_eq!(read_failure.work.peak_allocation_bytes, resident + 17);

    let contains_failure = async_storage::poll_ready(AsyncObjectStore::contains(
        &cache,
        absent,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("failing cache presence probe remained pending")?
    .err()
    .ok_or("failing cache presence probe unexpectedly succeeded")?;
    assert!(matches!(contains_failure.error, ObjectStoreError::Corrupt));
    assert_eq!(contains_failure.work.backend_read_operations, 1);
    assert!(contains_failure.work.items_examined > 0);
    assert_eq!(contains_failure.work.peak_allocation_bytes, resident + 17);

    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _state = match cache.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        std::panic::resume_unwind(Box::new("deliberate operation-cache poison"));
    }));
    assert!(poisoned.is_err());
    store.fail.store(false, Ordering::Relaxed);
    let recovered = async_storage::poll_ready(AsyncObjectStore::contains(
        &cache,
        cached,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("poison recovery presence probe remained pending")??;
    assert!(recovered.value);
    assert_eq!(recovered.work.backend_read_operations, 0);
    Ok(())
}

#[test]
fn operation_cache_reuses_authenticated_reads_and_preserves_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let bytes = Bytes::from_static(b"authenticated cache payload");
    let object_id = ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &bytes),
    };
    ObjectStore::put(&store, object_id, bytes.clone(), WorkBudget::UNBOUNDED)?;
    let (cache, setup) = OperationReadCache::new(&store, 2, WorkBudget::UNBOUNDED)?;
    assert_eq!(setup.allocation_operations, 1);
    assert!(setup.peak_allocation_bytes > 0);
    let cancellation = CancellationToken::new();

    let cold = async_storage::poll_ready(AsyncObjectStore::read(
        &cache,
        object_id,
        u64::try_from(bytes.len())?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cold cache read remained pending")??;
    assert_eq!(cold.value.bytes, bytes);
    assert_eq!(cold.work.backend_read_operations, 1);

    let warm = async_storage::poll_ready(AsyncObjectStore::read(
        &cache,
        object_id,
        u64::try_from(bytes.len())?,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("warm cache read remained pending")??;
    assert_eq!(warm.value.bytes, bytes);
    assert_eq!(warm.value.retention, ObjectReadRetention::Shared);
    assert_eq!(warm.work.backend_read_operations, 0);
    assert!(warm.work.items_examined > 0);

    let present = async_storage::poll_ready(AsyncObjectStore::contains(
        &cache,
        object_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("warm cache contains remained pending")??;
    assert!(present.value);
    assert_eq!(present.work.backend_read_operations, 0);

    let too_small = async_storage::poll_ready(AsyncObjectStore::read(
        &cache,
        object_id,
        u64::try_from(bytes.len())?.saturating_sub(1),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("bounded cache read remained pending")?
    .err()
    .ok_or("cache hit ignored maximum bytes")?;
    assert!(matches!(too_small.error, ObjectStoreError::TooLarge { .. }));
    assert_eq!(too_small.work.backend_read_operations, 0);
    assert!(too_small.work.items_examined > 0);

    let mut zero_items = WorkBudget::UNBOUNDED;
    zero_items.items_examined = 0;
    let unadmitted = async_storage::poll_ready(AsyncObjectStore::read(
        &cache,
        object_id,
        u64::MAX,
        zero_items,
        &cancellation,
    ))
    .ok_or("unadmitted cache read remained pending")?
    .err()
    .ok_or("cache hit exceeded work budget")?;
    assert!(matches!(
        unadmitted.error,
        ObjectStoreError::Work(WorkError::BudgetExceeded {
            counter: "items_examined",
            ..
        })
    ));
    assert!(unadmitted.work.items_examined > 0);
    assert_eq!(unadmitted.work.backend_read_operations, 0);
    Ok(())
}

#[test]
fn operation_cache_forwards_writes_and_uncached_presence_probes_exactly()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (cache, setup) = OperationReadCache::new(&store, 2, WorkBudget::UNBOUNDED)?;
    let cancellation = CancellationToken::new();
    let bytes = Bytes::from_static(b"cache-forwarded-object");
    let object_id = ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &bytes),
    };
    let written = async_storage::poll_ready(AsyncObjectStore::put(
        &cache,
        object_id,
        bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cache write remained pending")??;
    assert_eq!(written.work.backend_write_operations, 1);
    assert!(written.work.peak_allocation_bytes >= setup.peak_allocation_bytes);

    let present = async_storage::poll_ready(AsyncObjectStore::contains(
        &cache,
        object_id,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("uncached presence probe remained pending")??;
    assert!(present.value);
    assert_eq!(present.work.backend_read_operations, 1);
    assert!(present.work.items_examined > 0);

    let absent = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([253; 32]),
    };
    let missing = async_storage::poll_ready(AsyncObjectStore::contains(
        &cache,
        absent,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("absent presence probe remained pending")??;
    assert!(!missing.value);
    assert_eq!(missing.work.backend_read_operations, 1);
    assert!(missing.work.items_examined > 0);
    Ok(())
}

#[test]
fn pre_cancelled_single_path_operations_do_no_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let inner = MemoryObjectStore::default();
    let (generation, path) = fixture(&inner)?;
    let store = CancellingStore {
        inner,
        reads: AtomicUsize::new(0),
    };
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let lookup = async_storage::poll_ready(lookup_path_async(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("pre-cancelled path lookup blocked")?
    .err()
    .ok_or("pre-cancelled path lookup unexpectedly succeeded")?;
    assert!(matches!(
        lookup.error,
        PathLookupError::Tree(TreeReadError::Cancelled)
    ));
    assert_eq!(*lookup.work, WorkCounters::default());

    let observed = async_storage::poll_ready(observe_path_async(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("pre-cancelled path observation blocked")?
    .err()
    .ok_or("pre-cancelled path observation unexpectedly succeeded")?;
    assert!(matches!(
        observed.error,
        PathLookupError::Tree(TreeReadError::Cancelled)
    ));
    assert_eq!(*observed.work, WorkCounters::default());
    assert_eq!(store.reads.load(Ordering::Relaxed), 0);
    Ok(())
}

#[test]
fn borrowed_sync_paths_and_private_accounting_helpers_are_total()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store)?;
    let root = NamespacePath::new(Vec::new(), config().limits)?;
    let borrowed = lookup_path_refs(
        &store,
        &generation,
        &[&main, &root],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    let owned = lookup_paths(
        &store,
        &generation,
        &[main.clone(), root.clone()],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(borrowed.entries, owned.entries);
    for paths in [Vec::<&NamespacePath>::new(), vec![&main, &root]] {
        let mut limited = config();
        limited.limits.maximum_paths_per_batch = 1;
        let failure = lookup_path_refs(&store, &generation, &paths, limited, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("invalid borrowed batch unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            PathLookupError::EmptyBatch | PathLookupError::TooManyPaths
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let prior = WorkCounters {
        items_examined: 7,
        ..WorkCounters::default()
    };
    for allocation in [
        AllocationError::Overflow,
        AllocationError::ReleaseInvariant,
        AllocationError::InvalidCapacity,
        AllocationError::CapacityExceeded,
        AllocationError::AllocationFailed,
    ] {
        let failure = allocation_failure(allocation, prior);
        assert_eq!(failure.work.items_examined, 7);
        assert!(matches!(
            failure.error,
            PathLookupError::Work(WorkError::Overflow) | PathLookupError::AllocationFailed
        ));
    }
    let mapped = allocation_failure(AllocationError::Work(WorkError::Overflow), prior);
    assert!(matches!(
        mapped.error,
        PathLookupError::Work(WorkError::Overflow)
    ));
    assert_eq!(*mapped.work, prior);

    let overflow = OperationReadCache::new(&store, usize::MAX, WorkBudget::UNBOUNDED)
        .err()
        .ok_or("overflowing cache size unexpectedly succeeded")?;
    assert!(matches!(
        overflow.error,
        PathLookupError::Work(WorkError::Overflow)
    ));
    let overflow = maximum_cache_entries_for_components(usize::MAX, config())
        .err()
        .ok_or("overflowing component frontier unexpectedly succeeded")?;
    assert!(matches!(
        overflow.error,
        PathLookupError::Work(WorkError::Overflow)
    ));
    assert!(matches!(
        map_cache_error(ObjectStoreError::Work(WorkError::Overflow)),
        PathLookupError::Work(WorkError::Overflow)
    ));
    assert!(matches!(
        map_cache_error(ObjectStoreError::Corrupt),
        PathLookupError::Tree(TreeReadError::Storage(ObjectStoreError::Corrupt))
    ));
    let reduced = batch_sub_budget(prior, WorkBudget::UNBOUNDED, 17)?;
    assert_eq!(
        reduced.peak_allocation_bytes,
        WorkBudget::UNBOUNDED.peak_allocation_bytes - 17
    );
    let mut insufficient = WorkBudget::UNBOUNDED;
    insufficient.peak_allocation_bytes = 16;
    let failure = batch_sub_budget(prior, insufficient, 17)
        .err()
        .ok_or("orchestration peak underflow unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        PathLookupError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed: 17,
            maximum: 16,
        })
    ));
    assert_eq!(*failure.work, prior);
    Ok(())
}

#[test]
fn operation_cache_capacity_never_evicts_or_misattributes_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first_bytes = Bytes::from_static(b"first");
    let second_bytes = Bytes::from_static(b"second");
    let first = ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &first_bytes),
    };
    let second = ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &second_bytes),
    };
    ObjectStore::put(&store, first, first_bytes, WorkBudget::UNBOUNDED)?;
    ObjectStore::put(&store, second, second_bytes, WorkBudget::UNBOUNDED)?;
    let (cache, _) = OperationReadCache::new(&store, 1, WorkBudget::UNBOUNDED)?;
    let cancellation = CancellationToken::new();

    for object_id in [first, second, second] {
        let receipt = async_storage::poll_ready(AsyncObjectStore::read(
            &cache,
            object_id,
            u64::MAX,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))
        .ok_or("capacity cache read remained pending")??;
        assert_eq!(receipt.work.backend_read_operations, 1);
    }
    let warm_first = async_storage::poll_ready(AsyncObjectStore::read(
        &cache,
        first,
        u64::MAX,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("retained cache read remained pending")??;
    assert_eq!(warm_first.work.backend_read_operations, 0);
    Ok(())
}

#[test]
fn empty_and_excessive_path_batches_reject_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store)?;
    let empty = lookup_paths(&store, &generation, &[], config(), WorkBudget::UNBOUNDED)
        .err()
        .ok_or("empty path batch succeeded")?;
    assert!(matches!(empty.error, PathLookupError::EmptyBatch));
    assert_eq!(*empty.work, WorkCounters::default());

    let mut bounded = config();
    bounded.limits.maximum_paths_per_batch = 1;
    let excessive = lookup_paths(
        &store,
        &generation,
        &[path.clone(), path],
        bounded,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("excessive path batch succeeded")?;
    assert!(matches!(excessive.error, PathLookupError::TooManyPaths));
    assert_eq!(*excessive.work, WorkCounters::default());
    Ok(())
}

#[test]
fn cancelled_and_unadmitted_batches_perform_no_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store)?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = async_storage::poll_ready(lookup_paths_async(
        &store,
        &generation,
        std::slice::from_ref(&path),
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cancelled memory batch blocked")?
    .err()
    .ok_or("cancelled path batch succeeded")?;
    assert!(matches!(
        cancelled.error,
        PathLookupError::Tree(TreeReadError::Cancelled)
    ));
    assert_eq!(*cancelled.work, WorkCounters::default());

    let mut budget = WorkBudget::UNBOUNDED;
    budget.peak_allocation_bytes = 0;
    let unadmitted = lookup_paths(&store, &generation, &[path], config(), budget)
        .err()
        .ok_or("zero-allocation path batch succeeded")?;
    assert!(matches!(unadmitted.error, PathLookupError::Work(_)));
    assert_eq!(unadmitted.work.backend_read_operations, 0);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn scalar_and_batch_peak_accounting_fail_closed_at_every_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let prior = WorkCounters {
        items_examined: 3,
        peak_allocation_bytes: 11,
        ..WorkCounters::default()
    };
    let nested = WorkCounters {
        backend_read_operations: 1,
        peak_allocation_bytes: 17,
        ..WorkCounters::default()
    };
    let reduced = budget_after_resident(WorkBudget::UNBOUNDED, 29)?;
    assert_eq!(
        reduced.peak_allocation_bytes,
        WorkBudget::UNBOUNDED.peak_allocation_bytes - 29
    );
    let rejected = budget_after_resident(
        WorkBudget {
            peak_allocation_bytes: 28,
            ..WorkBudget::UNBOUNDED
        },
        29,
    )
    .err()
    .ok_or("resident bytes exceeded the admitted peak")?;
    assert!(matches!(
        rejected.error,
        ObjectStoreError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed: 29,
            maximum: 28
        })
    ));

    let scalar = merge_backend_peak(prior, nested, 101, WorkBudget::UNBOUNDED)?;
    assert_eq!(scalar.items_examined, 3);
    assert_eq!(scalar.backend_read_operations, 1);
    assert_eq!(scalar.peak_allocation_bytes, 118);
    let scalar_peak = merge_backend_peak(
        prior,
        WorkCounters {
            peak_allocation_bytes: u64::MAX,
            ..WorkCounters::default()
        },
        1,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("scalar simultaneous peak overflow succeeded")?;
    assert!(matches!(
        scalar_peak.error,
        ObjectStoreError::Work(WorkError::Overflow)
    ));
    let scalar_counter = merge_backend_peak(
        WorkCounters {
            bytes_hashed: u64::MAX,
            ..WorkCounters::default()
        },
        WorkCounters {
            bytes_hashed: 1,
            ..WorkCounters::default()
        },
        0,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("scalar counter overflow succeeded")?;
    assert!(matches!(
        scalar_counter.error,
        ObjectStoreError::Work(WorkError::Overflow)
    ));
    let scalar_failure_peak = merge_backend_failure(
        prior,
        WorkCounters {
            peak_allocation_bytes: u64::MAX,
            ..WorkCounters::default()
        },
        1,
        ObjectStoreError::Corrupt,
    );
    assert!(matches!(
        scalar_failure_peak.error,
        ObjectStoreError::Work(WorkError::Overflow)
    ));
    let scalar_failure_counter = merge_backend_failure(
        WorkCounters {
            bytes_hashed: u64::MAX,
            ..WorkCounters::default()
        },
        WorkCounters {
            bytes_hashed: 1,
            ..WorkCounters::default()
        },
        0,
        ObjectStoreError::Corrupt,
    );
    assert!(matches!(
        scalar_failure_counter.error,
        ObjectStoreError::Work(WorkError::Overflow)
    ));

    let batch = merge_batch_work(prior, nested, 101, WorkBudget::UNBOUNDED)?;
    assert_eq!(batch.items_examined, 3);
    assert_eq!(batch.backend_read_operations, 1);
    assert_eq!(batch.peak_allocation_bytes, 118);
    let batch_peak = merge_batch_work(
        prior,
        WorkCounters {
            peak_allocation_bytes: u64::MAX,
            ..WorkCounters::default()
        },
        1,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("batch simultaneous peak overflow succeeded")?;
    assert!(matches!(
        batch_peak.error,
        PathLookupError::Work(WorkError::Overflow)
    ));
    let batch_counter = merge_batch_work(
        WorkCounters {
            bytes_hashed: u64::MAX,
            ..WorkCounters::default()
        },
        WorkCounters {
            bytes_hashed: 1,
            ..WorkCounters::default()
        },
        0,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("batch counter overflow succeeded")?;
    assert!(matches!(
        batch_counter.error,
        PathLookupError::Work(WorkError::Overflow)
    ));
    let batch_failure = merge_batch_failure(prior, nested, 101, PathLookupError::MissingRootRecord);
    assert!(matches!(
        batch_failure.error,
        PathLookupError::MissingRootRecord
    ));
    assert_eq!(batch_failure.work.peak_allocation_bytes, 118);
    let batch_failure_peak = merge_batch_failure(
        prior,
        WorkCounters {
            peak_allocation_bytes: u64::MAX,
            ..WorkCounters::default()
        },
        1,
        PathLookupError::MissingRootRecord,
    );
    assert!(matches!(
        batch_failure_peak.error,
        PathLookupError::Work(WorkError::Overflow)
    ));
    let batch_failure_counter = merge_batch_failure(
        WorkCounters {
            bytes_hashed: u64::MAX,
            ..WorkCounters::default()
        },
        WorkCounters {
            bytes_hashed: 1,
            ..WorkCounters::default()
        },
        0,
        PathLookupError::MissingRootRecord,
    );
    assert!(matches!(
        batch_failure_counter.error,
        PathLookupError::Work(WorkError::Overflow)
    ));
    Ok(())
}

#[test]
fn mixed_depth_batches_finish_prefixes_and_reject_non_directory_descent()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, main) = fixture(&store)?;
    let src_name = LogicalName::new(NameEncoding::Utf8, b"src".to_vec(), 255)?;
    let src = NamespacePath::new(vec![src_name.clone()], config().limits)?;
    let root = NamespacePath::new(Vec::new(), config().limits)?;
    let mixed = lookup_paths(
        &store,
        &generation,
        &[main.clone(), src, root],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(
        mixed.entries[0].record.map(|record| record.kind),
        Some(FileKind::Regular)
    );
    assert_eq!(
        mixed.entries[1].record.map(|record| record.kind),
        Some(FileKind::Directory)
    );
    assert_eq!(
        mixed.entries[2].record.map(|record| record.kind),
        Some(FileKind::Directory)
    );

    let mut invalid_components = main.components().to_vec();
    invalid_components.push(LogicalName::new(
        NameEncoding::Utf8,
        b"child".to_vec(),
        255,
    )?);
    let invalid = NamespacePath::new(invalid_components, config().limits)?;
    let failure = lookup_paths(
        &store,
        &generation,
        &[invalid],
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("non-directory descent succeeded")?;
    assert!(matches!(failure.error, PathLookupError::NotDirectory));
    assert!(failure.work.backend_read_operations > 0);
    Ok(())
}

#[test]
fn unsupported_name_encoding_rejects_before_backend_work() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let (generation, _) = fixture(&store)?;
    let path = NamespacePath::new(
        vec![LogicalName::new(NameEncoding::PosixBytes, vec![0xff], 255)?],
        config().limits,
    )?;
    let failure = lookup_path(&store, &generation, &path, config(), WorkBudget::UNBOUNDED)
        .err()
        .ok_or("unsupported encoding succeeded")?;
    assert!(matches!(
        failure.error,
        PathLookupError::UnsupportedNameEncoding
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn profile_specific_name_encodings_reach_authenticated_absence()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, _) = fixture(&store)?;
    for (profile, encoding, bytes) in [
        (
            FilesystemProfile::Posix,
            NameEncoding::PosixBytes,
            vec![0xff],
        ),
        (
            FilesystemProfile::Windows,
            NameEncoding::WindowsUtf16Le,
            vec![b'x', 0],
        ),
    ] {
        let mut profile_config = config();
        profile_config.profile = profile;
        let candidate = NamespacePath::new(
            vec![LogicalName::new(encoding, bytes, 255)?],
            profile_config.limits,
        )?;
        let lookup = lookup_path(
            &store,
            &generation,
            &candidate,
            profile_config,
            WorkBudget::UNBOUNDED,
        )?;
        assert!(lookup.record.is_none());
        assert_eq!(lookup.resolved_components, 0);
        assert!(lookup.work.backend_read_operations > 0);
        assert!(lookup.work.page_reads > 0);
    }
    Ok(())
}

#[test]
fn single_path_cancellation_after_authenticated_progress_preserves_work()
-> Result<(), Box<dyn std::error::Error>> {
    for observe in [false, true] {
        let inner = MemoryObjectStore::default();
        let (generation, path) = fixture(&inner)?;
        let store = CancellingStore {
            inner,
            reads: AtomicUsize::new(0),
        };
        let cancellation = CancellationToken::new();
        let failure = if observe {
            async_storage::poll_ready(observe_path_async(
                &store,
                &generation,
                &path,
                config(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("cancelling path observation blocked")?
            .err()
            .ok_or("cancelled path observation unexpectedly succeeded")?
        } else {
            async_storage::poll_ready(lookup_path_async(
                &store,
                &generation,
                &path,
                config(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .ok_or("cancelling path lookup blocked")?
            .err()
            .ok_or("cancelled path lookup unexpectedly succeeded")?
        };
        assert!(matches!(
            failure.error,
            PathLookupError::Tree(
                TreeReadError::Cancelled | TreeReadError::Storage(ObjectStoreError::Cancelled)
            )
        ));
        assert!(failure.work.backend_read_operations >= 1);
        assert!(failure.work.page_reads >= 1);
        assert!(failure.work.peak_allocation_bytes > 0);
    }
    Ok(())
}

#[test]
fn root_observation_and_missing_root_identity_are_exact() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store)?;
    let root = NamespacePath::new(Vec::new(), config().limits)?;
    let lookup = lookup_path(&store, &generation, &root, config(), WorkBudget::UNBOUNDED)?;
    assert_eq!(lookup.parent, None);
    assert_eq!(lookup.resolved_components, 0);
    assert_eq!(
        lookup.record.map(|record| record.file_id),
        Some(generation.root_file_id)
    );

    let cancellation = CancellationToken::new();
    let observed_root = async_storage::poll_ready(observe_path_async(
        &store,
        &generation,
        &root,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("root observation blocked")??;
    let root_edges = async_storage::poll_ready(observe_path_edges_async(
        &store,
        &generation,
        &root,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("root edge observation blocked")??;
    assert_eq!(observed_root.lookup.record, root_edges.lookup.record);
    assert_eq!(observed_root.lookup.parent, root_edges.lookup.parent);
    assert_eq!(
        observed_root.lookup.resolved_components,
        root_edges.lookup.resolved_components
    );
    assert_eq!(observed_root.dependencies.len(), 1);
    assert!(root_edges.dependencies.is_empty());

    let observed = async_storage::poll_ready(observe_path_async(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("full observation blocked")??;
    let edges = async_storage::poll_ready(observe_path_edges_async(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("edge observation blocked")??;
    assert_eq!(observed.lookup.record, edges.lookup.record);
    assert_eq!(observed.lookup.parent, edges.lookup.parent);
    assert_eq!(
        observed.lookup.resolved_components,
        edges.lookup.resolved_components
    );
    assert_eq!(observed.dependencies.len(), edges.dependencies.len() + 1);
    assert!(matches!(
        observed.dependencies.last(),
        Some(Dependency {
            region: DependencyRegion::FileRecord(_),
            expected: DependencyState::Present(_),
        })
    ));

    let mut missing = generation.clone();
    missing.root_file_id = FileId::from_bytes([99; 16]);
    let failure = lookup_path(&store, &missing, &root, config(), WorkBudget::UNBOUNDED)
        .err()
        .ok_or("missing root record unexpectedly succeeded")?;
    assert!(matches!(failure.error, PathLookupError::MissingRootRecord));
    assert!(failure.work.page_reads > 0);
    Ok(())
}

#[test]
fn path_bound_violations_reject_before_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store)?;
    let mut shallow = config();
    shallow.limits.maximum_path_depth = 1;
    let mut small_component = config();
    small_component.limits.maximum_component_bytes = 3;
    for candidate in [shallow, small_component] {
        let failure = lookup_path(&store, &generation, &path, candidate, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("unsupported path semantics unexpectedly succeeded")?;
        assert!(matches!(failure.error, PathLookupError::PathBounds));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let large_component = LogicalName::new(NameEncoding::Utf8, vec![b'x'; 200], 255)?;
    let long_path = NamespacePath::new(
        vec![large_component.clone(), large_component],
        config().limits,
    )?;
    let mut short_path = config();
    short_path.limits.maximum_path_bytes = 255;
    let failure = lookup_path(
        &store,
        &generation,
        &long_path,
        short_path,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("oversized encoded path unexpectedly succeeded")?;
    assert!(matches!(failure.error, PathLookupError::PathBounds));
    assert_eq!(*failure.work, WorkCounters::default());

    let windows_name = LogicalName::new(NameEncoding::WindowsUtf16Le, vec![b'a', 0], 255)?;
    let windows_path = NamespacePath::new(vec![windows_name], config().limits)?;
    let failure = lookup_path(
        &store,
        &generation,
        &windows_path,
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("Windows name in portable profile unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        PathLookupError::UnsupportedNameEncoding
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn exact_lookup_succeeds_regardless_of_case_and_unicode_policy()
-> Result<(), Box<dyn std::error::Error>> {
    // `case_sensitivity`/`unicode` are folding/normalization policy for the
    // facade layer above this one; an exact-key lookup at this level must not
    // reject a config merely for selecting `ProfileFolded`/`RequireNfc`.
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store)?;
    let mut folded = config();
    folded.case_sensitivity = CaseSensitivity::ProfileFolded;
    let mut normalized = config();
    normalized.unicode = UnicodePolicy::RequireNfc;
    for candidate in [folded, normalized] {
        let lookup = lookup_path(&store, &generation, &path, candidate, WorkBudget::UNBOUNDED)?;
        assert!(lookup.record.is_some());
    }
    Ok(())
}

#[test]
#[cfg(target_pointer_width = "64")]
fn cache_and_fixed_vector_admission_fail_before_impossible_allocations()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cache = OperationReadCache::new(&store, 1_usize << 61, WorkBudget::UNBOUNDED)
        .err()
        .ok_or("cache admitted an overflowing slot table")?;
    assert!(matches!(
        cache.error,
        PathLookupError::Work(WorkError::Overflow)
    ));

    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let fixed = reserve_fixed::<u8>(
        usize::MAX,
        &mut allocations,
        &mut work,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("impossible fixed vector allocation succeeded")?;
    assert!(matches!(fixed.error, PathLookupError::AllocationFailed));
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}
