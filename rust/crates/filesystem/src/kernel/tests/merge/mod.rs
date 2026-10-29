use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::{Digest, VolumeId};
use crate::kernel::{
    FileKind, FileMetadata, MetadataField, NameEncoding, TreeEntry, TreePage, file_metadata_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::ObjectKind;
use crate::test_support::{
    put_file_metadata as put_metadata, put_generation_root as put_generation,
};

fn object(kind: ObjectKind, byte: u8) -> ObjectId {
    ObjectId {
        storage: None,
        kind,
        digest: Digest::from_bytes([byte; 32]),
    }
}

fn root(volume: u8, root_file: u8, table: u8) -> GenerationRoot {
    GenerationRoot {
        volume_id: VolumeId::from_bytes([volume; 16]),
        root_file_id: FileId::from_bytes([root_file; 16]),
        file_table: object(ObjectKind::FileTablePage, table),
        parents: Vec::new(),
        required_features: 0,
    }
}

fn request(
    base: GenerationRoot,
    ours: GenerationRoot,
    theirs: GenerationRoot,
) -> MergeGenerationRequest {
    MergeGenerationRequest {
        base_generation: object(ObjectKind::GenerationRoot, 10),
        base,
        ours_generation: None,
        ours,
        theirs_generation: object(ObjectKind::GenerationRoot, 11),
        theirs,
        retain_theirs_parent: true,
        maximum_changes: 8,
        maximum_conflicts: 8,
        resolutions: BTreeMap::new(),
    }
}

fn record(
    file: u8,
    metadata: u8,
    link_count: u64,
    payload: FilePayload,
    kind: FileKind,
) -> FileRecord {
    FileRecord {
        file_id: FileId::from_bytes([file; 16]),
        kind,
        link_count,
        metadata: object(ObjectKind::Metadata, metadata),
        payload,
    }
}

fn directory(file: u8, metadata: u8, link_count: u64, entries: u8) -> FileRecord {
    record(
        file,
        metadata,
        link_count,
        FilePayload::Directory {
            entries: object(ObjectKind::TreePage, entries),
        },
        FileKind::Directory,
    )
}

fn timed(modified_ns: i64, posix_mode: u32) -> FileMetadata {
    FileMetadata {
        posix_mode: MetadataField::Value(posix_mode),
        modified_ns: MetadataField::Value(modified_ns),
        ..FileMetadata::default()
    }
}

async fn put_tree(
    store: &MemoryObjectStore,
    entries: Vec<TreeEntry>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    crate::test_support::put_tree_page(store, &TreePage::Leaf(entries), 8).await
}

async fn put_table(
    store: &MemoryObjectStore,
    records: Vec<FileRecord>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    crate::test_support::put_file_table_page(store, &crate::kernel::FileTablePage::Leaf(records), 8)
        .await
}

async fn stored_request(
    store: &MemoryObjectStore,
    base: GenerationRoot,
    ours: GenerationRoot,
    theirs: GenerationRoot,
) -> Result<MergeGenerationRequest, Box<dyn std::error::Error>> {
    Ok(MergeGenerationRequest {
        base_generation: put_generation(store, &base).await?,
        base,
        ours_generation: None,
        ours,
        theirs_generation: put_generation(store, &theirs).await?,
        theirs,
        retain_theirs_parent: true,
        maximum_changes: 8,
        maximum_conflicts: 8,
        resolutions: BTreeMap::new(),
    })
}

async fn merge_error(
    request: MergeGenerationRequest,
    cancellation: &CancellationToken,
) -> Result<OperationFailure<MergeGenerationError>, std::io::Error> {
    let result = (merge_generation_async(
        &MemoryObjectStore::default(),
        request,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        cancellation,
    ))
    .await;
    result
        .err()
        .ok_or_else(|| std::io::Error::other("request guard admitted an invalid request"))
}

async fn adjust_test_link_counts(
    store: &MemoryObjectStore,
    records: &mut BTreeMap<FileId, RecordChange>,
    remaining_changes: &mut u32,
    limits: DecodeLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    work: &mut WorkCounters,
) -> Result<(), OperationFailure<MergeGenerationError>> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = async {
        let mut state = MergeState::new(*remaining_changes, &mut accounting)?;
        for (file, value) in records.iter() {
            state.records.ensure_for_push(usize::MAX, &mut accounting)?;
            state.records.value.push((*file, *value));
        }
        let result =
            adjust_link_counts(store, limits, &mut state, &mut accounting, cancellation).await;
        for (file, value) in &state.records.value {
            *records.get_mut(file).ok_or(FsError::InvalidDiff)? = *value;
        }
        *remaining_changes = state.remaining_changes;
        state.records.retire(&mut accounting)?;
        state.conflicts.retire(&mut accounting)?;
        state.folds.retire(&mut accounting)?;
        state.file_folds.retire(&mut accounting)?;
        result
    }
    .await;
    *work = accounting.spent();
    result.map_err(|error| OperationFailure::new(project_merge_error(error), *work))
}

mod directories;
mod generations;
