use crate::async_storage::AsyncObjectStore;
use super::*;
use crate::foundation::{Digest, VolumeId};
use crate::kernel::{
    FileKind, FileMetadata, MetadataField, NameEncoding, TreeEntry, TreePage, encode_file_metadata,
    encode_tree_page, file_metadata_id, tree_page_id,
};
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectKind, };
use acyclic_stream::OwnedBytes;

fn object(kind: ObjectKind, byte: u8) -> ObjectId {
    ObjectId {
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

async fn put_metadata(
    store: &MemoryObjectStore,
    metadata: FileMetadata,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let object = file_metadata_id(metadata)?;
    AsyncObjectStore::put(
        store,
        object,
        OwnedBytes::from(encode_file_metadata(metadata, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?),
        WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?;
    Ok(object)
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
    let page = TreePage::Leaf(entries);
    let object = tree_page_id(&page, 8)?;
    AsyncObjectStore::put(
        store,
        object,
        OwnedBytes::from(encode_tree_page(&page, 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?),
        WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?;
    Ok(object)
}

async fn put_table(
    store: &MemoryObjectStore,
    records: Vec<FileRecord>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let page = super::super::FileTablePage::Leaf(records);
    let object = super::super::file_table_page_id(&page, 8)?;
    AsyncObjectStore::put(
        store,
        object,
        OwnedBytes::from(super::super::encode_file_table_page(&page, 8, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?),
        WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?;
    Ok(object)
}

async fn put_generation(
    store: &MemoryObjectStore,
    generation: &GenerationRoot,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let encoded = super::super::encode_generation_root(generation, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?;
    let object = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: crate::storage::object_digest(ObjectKind::GenerationRoot, &encoded),
    };
    AsyncObjectStore::put(store, object, OwnedBytes::from(encoded), WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?;
    Ok(object)
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
    )).await;
    result
        .err()
        .ok_or_else(|| std::io::Error::other("request guard admitted an invalid request"))
}

#[tokio::test]
async fn merge_request_guards_fail_before_backend_work() -> Result<(), Box<dyn std::error::Error>> {
    let valid = root(1, 2, 3);
    let cancellation = CancellationToken::new();

    let mut zero_changes = request(valid.clone(), valid.clone(), valid.clone());
    zero_changes.maximum_changes = 0;
    let failure = merge_error(zero_changes, &cancellation).await?;
    assert!(matches!(failure.error, MergeGenerationError::ChangeLimit));
    assert_eq!(*failure.work, WorkCounters::default());

    let mut zero_conflicts = request(valid.clone(), valid.clone(), valid.clone());
    zero_conflicts.maximum_conflicts = 0;
    assert!(matches!(
        merge_error(zero_conflicts, &cancellation).await?.error,
        MergeGenerationError::ChangeLimit
    ));

    assert!(matches!(
        merge_error(
            request(valid.clone(), root(9, 2, 3), valid.clone()),
            &cancellation
        ).await?
        .error,
        MergeGenerationError::InvalidDiff
    ));
    assert!(matches!(
        merge_error(
            request(valid.clone(), valid.clone(), root(1, 9, 3)),
            &cancellation
        ).await?
        .error,
        MergeGenerationError::InvalidDiff
    ));

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failure = merge_error(request(valid.clone(), valid.clone(), valid), &cancelled).await?;
    assert!(matches!(failure.error, MergeGenerationError::Cancelled(_)));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn directory_scalar_conflicts_are_exact_and_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let directory_id = FileId::from_bytes([7; 16]);

    // Authored metadata (the mode) changed differently on each side: a real conflict.
    let with_metadata = |metadata| FileRecord {
        metadata,
        ..directory(7, 1, 1, 8)
    };
    let metadata_conflict = (merge_directory_record_async(
        &store,
        directory_id,
        with_metadata(put_metadata(&store, timed(1, 0o755)).await?),
        with_metadata(put_metadata(&store, timed(1, 0o700)).await?),
        with_metadata(put_metadata(&store, timed(1, 0o770)).await?),
        1,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(metadata_conflict.value.record.is_none());
    assert_eq!(
        metadata_conflict.value.conflicts,
        vec![MergeConflict::File(directory_id)]
    );
    assert!(!metadata_conflict.value.truncated);
    assert_eq!(metadata_conflict.value.examined_changes, 0);

    let bounded_link_conflict = (merge_directory_record_async(
        &store,
        directory_id,
        directory(7, 1, 1, 8),
        directory(7, 1, 2, 8),
        directory(7, 1, 3, 8),
        1,
        0,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(bounded_link_conflict.value.record.is_none());
    assert!(bounded_link_conflict.value.conflicts.is_empty());
    assert!(bounded_link_conflict.value.truncated);

    let non_directory = record(7, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let result = (merge_directory_record_async(
        &store,
        directory_id,
        non_directory,
        non_directory,
        non_directory,
        1,
        1,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await;
    let failure = result
        .err()
        .ok_or_else(|| std::io::Error::other("directory merge accepted a non-directory record"))?;
    assert!(matches!(failure.error, MergeGenerationError::InvalidDiff));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn tree_mutation_preserves_exact_names_and_file_identity() -> Result<(), Box<dyn std::error::Error>>
{
    let name = LogicalName::new(NameEncoding::Utf8, b"entry".to_vec(), 16)?;
    let before = TreeEntry {
        name: name.clone(),
        file_id: FileId::from_bytes([1; 16]),
        kind: FileKind::Regular,
    };
    let after = TreeEntry {
        name: name.clone(),
        file_id: FileId::from_bytes([2; 16]),
        kind: FileKind::Directory,
    };
    assert!(matches!(
        tree_mutation(name.clone(), None, Some(after.clone())),
        Some(TreeMutation::Insert(value)) if value == after
    ));
    assert!(matches!(
        tree_mutation(name.clone(), Some(before.clone()), None),
        Some(TreeMutation::Remove { name: value, expected_file_id: Some(file_id) })
            if value == name && file_id == before.file_id
    ));
    assert!(matches!(
        tree_mutation(name.clone(), Some(before.clone()), Some(after.clone())),
        Some(TreeMutation::Replace { entry, expected_file_id })
            if entry == after && expected_file_id == before.file_id
    ));
    assert!(tree_mutation(name, None, None).is_none());
    Ok(())
}

async fn adjust_test_link_counts(
    store: &MemoryObjectStore, records: &mut BTreeMap<FileId, RecordChange>, remaining_changes: &mut u32,
    limits: DecodeLimits, budget: WorkBudget, cancellation: &CancellationToken, work: &mut WorkCounters,
) -> Result<(), OperationFailure<MergeGenerationError>> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = async {
        let mut state = MergeState::new(*remaining_changes, &mut accounting)?;
        for (file, value) in records.iter() {
            state.records.ensure_for_push(usize::MAX, &mut accounting)?;
            state.records.value.push((*file, *value));
        }
        let result = adjust_link_counts(store, limits, &mut state, &mut accounting, cancellation).await;
        for (file, value) in &state.records.value {
            *records.get_mut(file).ok_or(FsError::InvalidDiff)? = *value;
        }
        *remaining_changes = state.remaining_changes;
        state.records.retire(&mut accounting)?;
        state.conflicts.retire(&mut accounting)?;
        state.folds.retire(&mut accounting)?;
        state.file_folds.retire(&mut accounting)?;
        result
    }.await;
    *work = accounting.spent();
    result.map_err(|error| OperationFailure::new(project_merge_error(error), *work))
}

#[tokio::test]
async fn link_count_adjustment_is_bounded_exact_and_rename_neutral()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let target_id = FileId::from_bytes([31; 16]);
    let directory_id = FileId::from_bytes([32; 16]);
    let target = record(31, 1, 2, FilePayload::Empty, FileKind::Fifo);
    let a = LogicalName::new(NameEncoding::Utf8, b"a".to_vec(), 8)?;
    let b = LogicalName::new(NameEncoding::Utf8, b"b".to_vec(), 8)?;
    let entry = |name: LogicalName| TreeEntry {
        name,
        file_id: target_id,
        kind: FileKind::Fifo,
    };
    let base_tree = put_tree(&store, vec![entry(a.clone())]).await?;
    let empty_tree = put_tree(&store, Vec::new()).await?;
    let renamed_tree = put_tree(&store, vec![entry(b)]).await?;
    let base_directory = FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata: object(ObjectKind::Metadata, 1),
        payload: FilePayload::Directory { entries: base_tree },
    };
    let removed_directory = FileRecord {
        payload: FilePayload::Directory {
            entries: empty_tree,
        },
        ..base_directory
    };
    let mut resolutions = BTreeMap::from([
        (
            directory_id,
            (Some(base_directory), Some(removed_directory)),
        ),
        (target_id, (Some(target), Some(target))),
    ]);
    let mut remaining_changes = 8;
    let mut work = WorkCounters::default();
    (adjust_test_link_counts(&store,
    &mut resolutions,
    &mut remaining_changes,
    DecodeLimits::default(),
    WorkBudget::UNBOUNDED,
    &cancellation,
    &mut work,)).await?;
    assert_eq!(
        resolutions
            .get(&target_id)
            .and_then(|(_, record)| *record)
            .ok_or("adjusted target record missing")?
            .link_count,
        1
    );

    // A file introduced on one side may lose its directory binding when the
    // other side wins a same-name replacement. It must not remain orphaned in
    // the merged file table merely because no binding changed relative to ours.
    let introduced = record(33, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let introduced_id = introduced.file_id;
    let mut resolutions = BTreeMap::from([(introduced_id, (None, Some(introduced)))]);
    (adjust_test_link_counts(&store,
    &mut resolutions,
    &mut 8,
    DecodeLimits::default(),
    WorkBudget::UNBOUNDED,
    &cancellation,
    &mut WorkCounters::default(),)).await?;
    assert_eq!(
        resolutions.get(&introduced_id).map(|(_, record)| *record),
        Some(None)
    );

    let renamed_directory = FileRecord {
        payload: FilePayload::Directory {
            entries: renamed_tree,
        },
        ..base_directory
    };
    let mut resolutions = BTreeMap::from([(
        directory_id,
        (Some(base_directory), Some(renamed_directory)),
    )]);
    let mut remaining_changes = 8;
    (adjust_test_link_counts(&store,
    &mut resolutions,
    &mut remaining_changes,
    DecodeLimits::default(),
    WorkBudget::UNBOUNDED,
    &cancellation,
    &mut WorkCounters::default(),)).await?;
    assert!(!resolutions.contains_key(&target_id));

    let mut resolutions = BTreeMap::from([(
        directory_id,
        (Some(base_directory), Some(removed_directory)),
    )]);
    let limit = (adjust_test_link_counts(&store,
    &mut resolutions,
    &mut 0,
    DecodeLimits::default(),
    WorkBudget::UNBOUNDED,
    &cancellation,
    &mut WorkCounters::default(),)).await
    .err()
    .ok_or("zero link frontier unexpectedly succeeded")?;
    assert!(matches!(limit.error, MergeGenerationError::ChangeLimit));
    Ok(())
}

#[tokio::test]
async fn pruning_an_unbound_directory_recomputes_descendant_links()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let directory_id = FileId::from_bytes([90; 16]);
    let child = record(91, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let child_id = child.file_id;
    let entries = put_tree(
        &store,
        vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"child".to_vec(), 8)?,
            file_id: child_id,
            kind: FileKind::Fifo,
        }],
    ).await?;
    let directory = FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata: object(ObjectKind::Metadata, 1),
        payload: FilePayload::Directory { entries },
    };
    let mut resolutions = BTreeMap::from([
        (directory_id, (None, Some(directory))),
        (child_id, (None, Some(child))),
    ]);
    (adjust_test_link_counts(&store,
    &mut resolutions,
    &mut 8,
    DecodeLimits::default(),
    WorkBudget::UNBOUNDED,
    &CancellationToken::new(),
    &mut WorkCounters::default(),)).await?;
    assert!(resolutions.values().all(|(_, resolved)| resolved.is_none()));
    Ok(())
}

#[tokio::test]
async fn deleting_a_record_with_remaining_links_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let linked = record(92, 1, 2, FilePayload::Empty, FileKind::Fifo);
    let mut resolutions = BTreeMap::from([(linked.file_id, (Some(linked), None))]);
    let failure = (adjust_test_link_counts(&store,
    &mut resolutions,
    &mut 8,
    DecodeLimits::default(),
    WorkBudget::UNBOUNDED,
    &CancellationToken::new(),
    &mut WorkCounters::default(),)).await
    .err()
    .ok_or("remaining-link deletion unexpectedly succeeded")?;
    assert!(matches!(failure.error, MergeGenerationError::InvalidDiff));
    Ok(())
}

#[tokio::test]
async fn generation_merge_covers_union_directory_and_field_resolution_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let base_first = record(1, 1, 1, FilePayload::Empty, FileKind::Fifo);
    let base_second = record(2, 2, 1, FilePayload::Empty, FileKind::Fifo);
    let ours_first = record(1, 3, 1, FilePayload::Empty, FileKind::Fifo);
    let theirs_second = record(2, 4, 1, FilePayload::Empty, FileKind::Fifo);
    let base_table = put_table(&store, vec![base_first, base_second]).await?;
    let ours_table = put_table(&store, vec![ours_first, base_second]).await?;
    let theirs_table = put_table(&store, vec![base_first, theirs_second]).await?;
    let generation = |table| GenerationRoot {
        file_table: table,
        ..root(1, 9, 1)
    };
    let union_limit = (merge_generation_async(
        &store,
        MergeGenerationRequest {
            maximum_changes: 1,
            ..request(
                generation(base_table),
                generation(ours_table),
                generation(theirs_table),
            )
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("union-limited generation merge succeeded")?;
    assert!(matches!(
        union_limit.error,
        MergeGenerationError::ChangeLimit
    ));

    let base_third = record(3, 3, 1, FilePayload::Empty, FileKind::Fifo);
    let ours_second = record(2, 5, 1, FilePayload::Empty, FileKind::Fifo);
    let theirs_second_overlap = record(2, 6, 1, FilePayload::Empty, FileKind::Fifo);
    let theirs_third = record(3, 7, 1, FilePayload::Empty, FileKind::Fifo);
    let three_way_union_limit = (merge_generation_async(
        &store,
        MergeGenerationRequest {
            maximum_changes: 2,
            ..request(
                generation(put_table(
                    &store,
                    vec![base_first, base_second, base_third],
                ).await?),
                generation(put_table(
                    &store,
                    vec![ours_first, ours_second, base_third],
                ).await?),
                generation(put_table(
                    &store,
                    vec![base_first, theirs_second_overlap, theirs_third],
                ).await?),
            )
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("three-file union-limited generation merge succeeded")?;
    assert!(matches!(
        three_way_union_limit.error,
        MergeGenerationError::ChangeLimit
    ));

    let shared_change_table = put_table(&store, vec![ours_first, base_second]).await?;
    let unchanged = (merge_generation_async(
        &store,
        stored_request(
            &store,
            generation(base_table),
            generation(shared_change_table),
            generation(shared_change_table),
        ).await?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(matches!(
        unchanged.value,
        MergeGenerationOutcome::Prepared { root, .. } if root.file_table == shared_change_table
    ));

    let ours_field = record(1, 5, 1, FilePayload::Empty, FileKind::Fifo);
    let theirs_field = record(1, 1, 2, FilePayload::Empty, FileKind::Fifo);
    let base_field_table = put_table(&store, vec![base_first]).await?;
    let ours_field_table = put_table(&store, vec![ours_field]).await?;
    let theirs_field_table = put_table(&store, vec![theirs_field]).await?;
    let combined = (merge_generation_async(
        &store,
        stored_request(
            &store,
            generation(base_field_table),
            generation(ours_field_table),
            generation(theirs_field_table),
        ).await?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(matches!(
        combined.value,
        MergeGenerationOutcome::Prepared { .. }
    ));

    let mut linear_request = stored_request(
        &store,
        generation(base_field_table),
        generation(ours_field_table),
        generation(theirs_field_table),
    ).await?;
    linear_request.retain_theirs_parent = false;
    let linear = (merge_generation_async(
        &store,
        linear_request,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(matches!(
        linear.value,
        MergeGenerationOutcome::Prepared { root, .. } if root.parents.len() == 1
    ));

    let empty = put_tree(&store, Vec::new()).await?;
    let ours_tree = put_tree(
        &store,
        vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"ours".to_vec(), 16)?,
            file_id: FileId::from_bytes([7; 16]),
            kind: FileKind::Fifo,
        }],
    ).await?;
    let theirs_tree = put_tree(
        &store,
        vec![TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, b"theirs".to_vec(), 16)?,
            file_id: FileId::from_bytes([8; 16]),
            kind: FileKind::Fifo,
        }],
    ).await?;
    let base_directory = directory(3, 1, 1, 1);
    let directory_with = |entries| FileRecord {
        payload: FilePayload::Directory { entries },
        ..base_directory
    };
    let directory_limit = (merge_generation_async(
        &store,
        MergeGenerationRequest {
            maximum_changes: 1,
            ..request(
                generation(put_table(&store, vec![directory_with(empty)]).await?),
                generation(put_table(&store, vec![directory_with(ours_tree)]).await?),
                generation(put_table(&store, vec![directory_with(theirs_tree)]).await?),
            )
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("directory-limited generation merge succeeded")?;
    assert!(matches!(
        directory_limit.error,
        MergeGenerationError::ChangeLimit
    ));

    let shared_name = LogicalName::new(NameEncoding::Utf8, b"shared".to_vec(), 16)?;
    let tree_for = async |file: u8| {
        put_tree(
            &store,
            vec![TreeEntry {
                name: shared_name.clone(),
                file_id: FileId::from_bytes([file; 16]),
                kind: FileKind::Fifo,
            }],
        ).await
    };
    let conflicted = (merge_generation_async(
        &store,
        stored_request(
            &store,
            generation(put_table(&store, vec![directory_with(tree_for(10).await?)]).await?),
            generation(put_table(&store, vec![directory_with(tree_for(11).await?)]).await?),
            generation(put_table(&store, vec![directory_with(tree_for(12).await?)]).await?),
        ).await?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(matches!(
        conflicted.value,
        MergeGenerationOutcome::Conflicted { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn directory_merge_and_link_recount_fail_at_each_distinct_change_frontier()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let directory_id = FileId::from_bytes([41; 16]);
    let entry = |name: &'static [u8], file: u8| -> Result<TreeEntry, Box<dyn std::error::Error>> {
        Ok(TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, name.to_vec(), 16)?,
            file_id: FileId::from_bytes([file; 16]),
            kind: FileKind::Fifo,
        })
    };
    let empty = put_tree(&store, Vec::new()).await?;
    let a = entry(b"a", 42)?;
    let b = entry(b"b", 43)?;
    let both = put_tree(&store, vec![a.clone(), b.clone()]).await?;
    let only_a = put_tree(&store, vec![a]).await?;
    let only_b = put_tree(&store, vec![b]).await?;
    let base = FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata: object(ObjectKind::Metadata, 1),
        payload: FilePayload::Directory { entries: empty },
    };
    let with = |entries| FileRecord {
        payload: FilePayload::Directory { entries },
        ..base
    };

    let truncated_side = (merge_directory_record_async(
        &store,
        directory_id,
        base,
        with(both),
        base,
        1,
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("side-truncated directory merge succeeded")?;
    assert!(matches!(
        truncated_side.error,
        MergeGenerationError::ChangeLimit
    ));

    let union_limit = (merge_directory_record_async(
        &store,
        directory_id,
        base,
        with(only_a),
        with(only_b),
        1,
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await
    .err()
    .ok_or("union-limited directory merge succeeded")?;
    assert!(matches!(
        union_limit.error,
        MergeGenerationError::ChangeLimit
    ));

    let mut resolutions = BTreeMap::from([(directory_id, (Some(base), Some(with(both))))]);
    let mut remaining_changes = 1;
    let failure = (adjust_test_link_counts(&store,
    &mut resolutions,
    &mut remaining_changes,
    DecodeLimits::default(),
    WorkBudget::UNBOUNDED,
    &cancellation,
    &mut WorkCounters::default(),)).await
    .err()
    .ok_or("truncated link recount succeeded")?;
    assert!(matches!(failure.error, MergeGenerationError::ChangeLimit));
    Ok(())
}

#[test]
fn metadata_timestamps_reconcile_and_authored_fields_conflict() {
    let base = timed(10, 0o644);
    // Both sides touched the record: the later time wins, the mode is unchanged.
    let merged = merge_metadata_fields(base, timed(30, 0o644), timed(20, 0o644))
        .expect("divergent timestamps reconcile");
    assert_eq!(merged.modified_ns, MetadataField::Value(30));
    assert_eq!(merged.posix_mode, MetadataField::Value(0o644));
    // One side changed the mode, the other only its time: both land.
    let merged = merge_metadata_fields(base, timed(10, 0o600), timed(50, 0o644))
        .expect("one-sided mode change reconciles");
    assert_eq!(merged.posix_mode, MetadataField::Value(0o600));
    assert_eq!(merged.modified_ns, MetadataField::Value(50));
    // A time only one side can represent is kept.
    let mut unavailable = timed(10, 0o644);
    unavailable.modified_ns = MetadataField::Unavailable;
    let merged = merge_metadata_fields(base, unavailable, timed(40, 0o644))
        .expect("unavailable versus value reconciles");
    assert_eq!(merged.modified_ns, MetadataField::Value(40));
    // Authored state changed differently on each side conflicts.
    assert!(merge_metadata_fields(base, timed(10, 0o600), timed(10, 0o700)).is_none());
    let mut ours = base;
    ours.posix_uid = MetadataField::Value(1);
    let mut theirs = base;
    theirs.posix_uid = MetadataField::Value(2);
    assert!(merge_metadata_fields(base, ours, theirs).is_none());
}

#[test]
fn one_file_added_on_both_sides_keeps_the_later_times() {
    let merged = merge_added_metadata_fields(timed(30, 0o644), timed(20, 0o644))
        .expect("the same file added twice reconciles");
    assert_eq!(merged.modified_ns, MetadataField::Value(30));
    assert_eq!(merged.posix_mode, MetadataField::Value(0o644));
    // Without a base, authored state must agree.
    assert!(merge_added_metadata_fields(timed(30, 0o600), timed(20, 0o644)).is_none());
}

#[tokio::test]
async fn sibling_additions_merge_entries_and_reconcile_directory_times()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let directory_id = FileId::from_bytes([51; 16]);
    let entry = |name: &'static [u8], file: u8| -> Result<TreeEntry, Box<dyn std::error::Error>> {
        Ok(TreeEntry {
            name: LogicalName::new(NameEncoding::Utf8, name.to_vec(), 16)?,
            file_id: FileId::from_bytes([file; 16]),
            kind: FileKind::Fifo,
        })
    };
    let a = entry(b"a", 52)?;
    let b = entry(b"b", 53)?;
    let empty = put_tree(&store, Vec::new()).await?;
    let only_a = put_tree(&store, vec![a.clone()]).await?;
    let only_b = put_tree(&store, vec![b.clone()]).await?;
    let both = put_tree(&store, vec![a, b]).await?;
    let dir = |metadata, entries| FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata,
        payload: FilePayload::Directory { entries },
    };
    let base = dir(put_metadata(&store, timed(10, 0o755)).await?, empty);
    let ours = dir(put_metadata(&store, timed(20, 0o755)).await?, only_a);
    let theirs = dir(put_metadata(&store, timed(30, 0o755)).await?, only_b);

    let merged = (merge_directory_record_async(
        &store,
        directory_id,
        base,
        ours,
        theirs,
        8,
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert!(merged.value.conflicts.is_empty());
    let record = merged
        .value
        .record
        .ok_or("sibling directory merge produced no record")?;
    assert_eq!(record.payload, FilePayload::Directory { entries: both });
    assert_eq!(record.metadata, file_metadata_id(timed(30, 0o755))?);

    // A genuine metadata conflict (mode changed both ways) resolved by a driver
    // still merges the entries; the resolution contributes metadata only.
    let ours = dir(put_metadata(&store, timed(20, 0o700)).await?, only_a);
    let theirs = dir(put_metadata(&store, timed(30, 0o770)).await?, only_b);
    let unresolved = (merge_directory_record_async(
        &store,
        directory_id,
        base,
        ours,
        theirs,
        8,
        8,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )).await?;
    assert_eq!(
        unresolved.value.conflicts,
        vec![MergeConflict::File(directory_id)]
    );
    let chosen = put_metadata(&store, timed(30, 0o770)).await?;
    let resolutions = BTreeMap::from([(
        MergeConflict::File(directory_id),
        MergeConflictResolution::File(Some(dir(chosen, only_b))),
    )]);
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut state = MergeState::new(8, &mut accounting)?;
    let record = (merge_directory_record_with_resolutions_async(
        &store, directory_id, base, ours, theirs, ResolutionView::Map(&resolutions), 8,
        DecodeLimits::default(), &mut state, &mut accounting, &cancellation,
    )).await?.ok_or("resolved directory merge produced no record")?;
    state.records.retire(&mut accounting)?;
    state.conflicts.retire(&mut accounting)?;
    state.folds.retire(&mut accounting)?;
    state.file_folds.retire(&mut accounting)?;
    assert_eq!(record.metadata, chosen);
    assert_eq!(record.payload, FilePayload::Directory { entries: both });
    Ok(())
}

mod laws {
    use super::{FileMetadata, MetadataField, merge_metadata_fields, resolve_three};
    use crate::foundation::Digest;
    use crate::storage::{ObjectId, ObjectKind};
    use proptest::prelude::*;

    // Tiny domains so sides often agree with the base or each other.
    fn field_of<T: Clone + std::fmt::Debug>(
        value: impl Strategy<Value = T>,
    ) -> impl Strategy<Value = MetadataField<T>> {
        prop::option::of(value)
            .prop_map(|value| value.map_or(MetadataField::Unavailable, MetadataField::Value))
    }

    fn object_of(kind: ObjectKind) -> impl Strategy<Value = MetadataField<ObjectId>> {
        field_of((0_u8..2).prop_map(move |byte| ObjectId {
            kind,
            digest: Digest::from_bytes([byte; 32]),
        }))
    }

    fn metadata() -> impl Strategy<Value = FileMetadata> {
        let small = || field_of(0_u32..2);
        let times = prop::array::uniform4(field_of(0_i64..3));
        let objects = (
            object_of(ObjectKind::AttributePage),
            object_of(ObjectKind::Blob),
            object_of(ObjectKind::Blob),
        );
        (
            (small(), small(), small(), field_of(0_u64..2), small()),
            times,
            objects,
        )
            .prop_map(
                |(
                    (posix_mode, posix_uid, posix_gid, posix_flags, windows_attributes),
                    [created_ns, modified_ns, accessed_ns, changed_ns],
                    (named_attributes, acl, security_descriptor),
                )| FileMetadata {
                    posix_mode,
                    posix_uid,
                    posix_gid,
                    posix_flags,
                    windows_attributes,
                    created_ns,
                    modified_ns,
                    accessed_ns,
                    changed_ns,
                    named_attributes,
                    acl,
                    security_descriptor,
                },
            )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn resolve_three_keeps_one_sided_and_identical_changes(
            base in 0_u8..3,
            ours in 0_u8..3,
            theirs in 0_u8..3,
        ) {
            prop_assert_eq!(resolve_three(&base, &base, &theirs), Some(theirs));
            prop_assert_eq!(resolve_three(&base, &ours, &base), Some(ours));
            prop_assert_eq!(resolve_three(&base, &ours, &ours), Some(ours));
            prop_assert_eq!(resolve_three(&base, &ours, &theirs), resolve_three(&base, &theirs, &ours));
        }

        #[test]
        fn metadata_merge_keeps_one_sided_and_identical_changes(
            base in metadata(),
            ours in metadata(),
            theirs in metadata(),
        ) {
            prop_assert_eq!(merge_metadata_fields(base, base, theirs), Some(theirs));
            prop_assert_eq!(merge_metadata_fields(base, ours, base), Some(ours));
            prop_assert_eq!(merge_metadata_fields(base, ours, ours), Some(ours));
            prop_assert_eq!(
                merge_metadata_fields(base, ours, theirs),
                merge_metadata_fields(base, theirs, ours)
            );
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_merge_preserves_concurrent_bindings_hardlinks_and_parent_lineage()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let metadata = put_metadata(&store, FileMetadata::default()).await?;
    let a = FileId::from_bytes([2; 16]);
    let b = FileId::from_bytes([3; 16]);
    let entry = |name: &str, file_id| -> Result<TreeEntry, Box<dyn std::error::Error>> {
        Ok(TreeEntry { name: LogicalName::new(NameEncoding::Utf8, name.as_bytes().to_vec(), 16)?,
            file_id, kind: FileKind::Fifo })
    };
    let base_entries = put_tree(&store, vec![entry("alpha", a)?, entry("beta", b)?]).await?;
    let ours_entries = put_tree(&store, vec![entry("alpha", a)?, entry("beta", b)?, entry("left", a)?]).await?;
    let theirs_entries = put_tree(&store, vec![entry("alpha", a)?, entry("beta", b)?, entry("right", b)?]).await?;
    let directory = |entries| FileRecord { metadata, payload: FilePayload::Directory { entries }, ..directory(1, 0, 1, 0) };
    let first = FileRecord { metadata, ..record(2, 0, 1, FilePayload::Empty, FileKind::Fifo) };
    let second = FileRecord { metadata, ..record(3, 0, 1, FilePayload::Empty, FileKind::Fifo) };
    let base = GenerationRoot { file_table: put_table(&store, vec![directory(base_entries), first, second]).await?, ..root(1, 1, 0) };
    let ours = GenerationRoot { file_table: put_table(&store,
        vec![directory(ours_entries), FileRecord { link_count: 2, ..first }, second]).await?, ..root(1, 1, 0) };
    let ours_table = ours.file_table;
    let theirs = GenerationRoot { file_table: put_table(&store,
        vec![directory(theirs_entries), first, FileRecord { link_count: 2, ..second }]).await?, ..root(1, 1, 0) };
    let request = stored_request(&store, base, ours, theirs).await?;
    let second_parent = GenerationId::new(request.theirs_generation.digest);
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).map_err(|(error, _)| error)?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let (outcome, allocation) = (merge_generation_with_accounting(
        &store, request, DecodeLimits::default(), &mut accounting, &cancellation)).await?;
    let MergeGenerationOutcome::Prepared { generation_root, root, generation_id } = outcome
        else { return Err("independent native additions conflicted".into()); };
    assert_eq!(generation_id, GenerationId::new(generation_root.digest));
    assert_eq!(root.parents.len(), 2);
    assert_eq!(root.parents.get(1), Some(&second_parent));
    let KernelOwnedAllocation::NativeOriginal(guard) = allocation else { return Err("native merge lost output custody".into()); };
    assert_eq!(guard.bytes(), (root.parents.capacity() * std::mem::size_of::<GenerationId>()) as u64);
    let changes = (super::super::diff_file_records_with_accounting(&store, Some(ours_table), Some(root.file_table),
        8, DecodeLimits::default(), &mut super::super::allocation::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation), &cancellation)).await?.0;
    let merged_second = changes.changes.iter().find(|change| change.key == b).and_then(|change| change.after).ok_or("second hardlink delta missing")?;
    assert_eq!(merged_second.link_count, 2);
    let merged_directory = changes.changes.iter().find(|change| change.key == FileId::from_bytes([1; 16]))
        .and_then(|change| change.after).ok_or("merged directory missing")?;
    let entries = directory_entries(Some(merged_directory)).ok_or("merged directory payload missing")?;
    let read = AsyncObjectStore::read(&store, entries, DecodeLimits::default().maximum_object_bytes, WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?;
    let TreePage::Leaf(entries) = super::super::decode_tree_page(&read.value.bytes, DecodeLimits::default(), &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?
        else { return Err("small merged directory was not a leaf".into()); };
    assert_eq!(entries.len(), 4);
    assert!(entries.iter().any(|entry| entry.name.as_bytes() == b"left" && entry.file_id == a));
    assert!(entries.iter().any(|entry| entry.name.as_bytes() == b"right" && entry.file_id == b));
    let stored = AsyncObjectStore::read(&store, generation_root, DecodeLimits::default().maximum_object_bytes, WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await?;
    assert_eq!(super::super::decode_generation_root(&stored.value.bytes, DecodeLimits::default(), |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?, root);
    assert!(original.receipt().page_reads > 0);
    assert!(original.receipt().page_writes > 0);
    drop(root);
    drop(guard);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_merge_original_allocation_denial_precedes_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let base = GenerationRoot { file_table: put_table(&store,
        vec![record(1, 1, 1, FilePayload::Empty, FileKind::Fifo)]).await?, ..root(1, 1, 0) };
    let ours = GenerationRoot { file_table: put_table(&store,
        vec![record(1, 2, 1, FilePayload::Empty, FileKind::Fifo)]).await?, ..root(1, 1, 0) };
    let request = request(base.clone(), ours, base);
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        WorkBudget { allocation_operations: 1, ..WorkBudget::UNBOUNDED }, &cancellation).map_err(|(error, _)| error)?;
    let before = original.receipt();
    let baseline = original.allocations(|ledger, _, _, _| Ok::<_, FsError>(ledger.live_bytes())).map(|(value, _allocation)| value)?;
    let mut accounting = KernelAccounting::native(&original, &cancellation);
    let result = (merge_generation_with_accounting(
        &store, request, DecodeLimits::default(), &mut accounting, &cancellation)).await;
    assert!(matches!(result, Err(FsError::Work(WorkError::BudgetExceeded { counter: "allocation_operations", .. }))));
    assert_eq!(original.receipt(), before);
    assert_eq!(original.receipt().backend_read_operations, 0);
    assert_eq!(original.receipt().backend_write_operations, 0);
    assert_eq!(original.allocations(|ledger, _, _, _| Ok::<_, FsError>(ledger.live_bytes())).map(|(value, _allocation)| value)?, baseline);
    Ok(())
}
